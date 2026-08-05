// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Vision2Seq document reading pipeline.
//
// Architecture: image preprocessing → vision encoder → text decoder.
// Processes document images and extracts text/structured content.
//
// Supported model layouts:
//   - vision_encoder.onnx (or encoder_model.onnx)
//   - decoder_model.onnx
//   - or a native Florence safetensors/GGUF directory
//   - preprocessor_config.json (image size, normalization)
//   - tokenizer.json
//   - config.json (model_type: florence2, etc.)

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const session_factory = @import("../architectures/session_factory.zig");
const florence_arch = @import("../architectures/florence.zig");
const backends = @import("../backends/backends.zig");
const ops = @import("../ops/ops.zig");
const tokenizer_mod = @import("inference_tokenizer");
const image = @import("image.zig");

const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

pub const ReadTelemetry = struct {
    resident_decoder: bool = false,
    kv_cache: bool = false,
    kv_cache_mode: ?[]const u8 = null,
    lm_head_path: ?[]const u8 = null,
    cuda_graph_replay: bool = false,
    cuda_graph_fallback_reason: ?[]const u8 = null,
    cuda_graph_capture_steps: usize = 0,
    cuda_graph_replay_steps: usize = 0,
    generated_tokens: ?usize = null,
};

var last_read_telemetry: ReadTelemetry = .{};

pub fn resetLastReadTelemetry() void {
    last_read_telemetry = .{};
}

pub fn lastReadTelemetry() ReadTelemetry {
    return last_read_telemetry;
}

pub const ReadConfig = struct {
    max_length: usize = 1024,
    image_size: usize = 384,
    image_seq_length: usize = 0,
    resample: image.Resample = .bilinear,
    decoder_start_token_id: i32 = 2,
    eos_token_id: i32 = 2,
    pad_token_id: i32 = 1,
    forced_bos_token_id: ?i32 = null,
    no_repeat_ngram_size: usize = 0,
    image_mean: [3]f32 = .{ 0.5, 0.5, 0.5 },
    image_std: [3]f32 = .{ 0.5, 0.5, 0.5 },
    pix2struct_max_patches: usize = 0,
    pix2struct_patch_height: usize = 0,
    pix2struct_patch_width: usize = 0,
    pix2struct_do_normalize: bool = false,
    prompt: ?[]const u8 = null,
};

pub const ReadResult = struct {
    text: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ReadResult) void {
        self.allocator.free(self.text);
    }
};

pub const ReadingPipeline = struct {
    allocator: std.mem.Allocator,
    vision_encoder: backends.Session,
    decoder: backends.Session,
    tokenizer: tokenizer_mod.Tokenizer,
    config: ReadConfig,
    florence_final_logits_bias_zero_cache: ?*?bool = null,

    pub fn init(
        allocator: std.mem.Allocator,
        vision_encoder: backends.Session,
        decoder: backends.Session,
        tokenizer: tokenizer_mod.Tokenizer,
        config: ReadConfig,
        florence_final_logits_bias_zero_cache: ?*?bool,
    ) ReadingPipeline {
        return .{
            .allocator = allocator,
            .vision_encoder = vision_encoder,
            .decoder = decoder,
            .tokenizer = tokenizer,
            .config = config,
            .florence_final_logits_bias_zero_cache = florence_final_logits_bias_zero_cache,
        };
    }

    /// Read text from an image. image_data is raw JPEG/PNG bytes.
    pub fn read(self: *ReadingPipeline, image_data: []const u8) !ReadResult {
        resetLastReadTelemetry();
        const allocator = self.allocator;
        const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
        if (debug_cuda_session) std.log.info("reading: decode start bytes={d}", .{image_data.len});
        const decode_start = nowNs();
        const decoded = try image.decode(allocator, image_data);
        logReadProfile("decode", decode_start);
        if (debug_cuda_session) std.log.info("reading: decode done", .{});
        defer decoded.deinit(allocator);

        if (expectsFlattenedPatches(self.vision_encoder)) {
            if (debug_cuda_session) std.log.info("reading: pix2struct path", .{});
            return self.readPix2StructDecoded(decoded);
        }

        const img_size: u32 = @intCast(self.config.image_size);
        if (debug_cuda_session) std.log.info("reading: preprocess start image_size={d}", .{img_size});
        const preprocess_start = nowNs();
        const pixel_values = try image.preprocessDecodedWithResample(
            allocator,
            decoded,
            img_size,
            self.config.image_mean,
            self.config.image_std,
            self.config.resample,
        );
        logReadProfile("preprocess", preprocess_start);
        if (debug_cuda_session) std.log.info("reading: preprocess done pixels={d}", .{pixel_values.len});
        defer allocator.free(pixel_values);
        return self.readPixelValues(pixel_values);
    }

    /// Read text from a homogeneous image batch. Unsupported model families fall
    /// back to the existing serial path; native Florence uses a batched encoder
    /// and KV-decoder path where the selected backend supports it.
    pub fn readBatch(self: *ReadingPipeline, image_datas: []const []const u8) ![]ReadResult {
        const allocator = self.allocator;
        if (image_datas.len == 0) return try allocator.alloc(ReadResult, 0);
        if (image_datas.len == 1) {
            const out = try allocator.alloc(ReadResult, 1);
            errdefer allocator.free(out);
            out[0] = try self.read(image_datas[0]);
            return out;
        }
        if (expectsFlattenedPatches(self.vision_encoder)) return self.readBatchSerial(image_datas);

        if (session_factory.getFlorenceConfig(self.vision_encoder) != null) {
            return self.readBatchNativeFlorenceChunked(image_datas) catch |err| switch (err) {
                error.UnsupportedShape,
                error.UnsupportedOperation,
                error.UnsupportedFlorence2ResidentMetal,
                => self.readBatchSerial(image_datas),
                else => return err,
            };
        }

        return self.readBatchSerial(image_datas);
    }

    fn readBatchSerial(self: *ReadingPipeline, image_datas: []const []const u8) ![]ReadResult {
        const allocator = self.allocator;
        const out = try allocator.alloc(ReadResult, image_datas.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*result| result.deinit();
            allocator.free(out);
        }

        for (image_datas, 0..) |image_data, i| {
            out[i] = try self.read(image_data);
            filled += 1;
        }
        return out;
    }

    fn readBatchNativeFlorenceChunked(self: *ReadingPipeline, image_datas: []const []const u8) ![]ReadResult {
        const allocator = self.allocator;
        const max_batch = nativeFlorenceReadBatchSize();
        if (max_batch <= 1) return self.readBatchSerial(image_datas);

        const out = try allocator.alloc(ReadResult, image_datas.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*result| result.deinit();
            allocator.free(out);
        }

        var offset: usize = 0;
        while (offset < image_datas.len) {
            const chunk_len = @min(max_batch, image_datas.len - offset);
            const chunk = image_datas[offset .. offset + chunk_len];
            const chunk_results = try self.readBatchNativeFlorence(chunk);
            if (chunk_results.len != chunk_len) {
                for (chunk_results) |*result| result.deinit();
                allocator.free(chunk_results);
                return error.InvalidReadResultCount;
            }
            {
                defer allocator.free(chunk_results);
                for (chunk_results, 0..) |result, i| {
                    out[offset + i] = result;
                }
            }
            filled += chunk_results.len;
            offset += chunk_len;
        }

        return out;
    }

    fn readBatchNativeFlorence(self: *ReadingPipeline, image_datas: []const []const u8) ![]ReadResult {
        const allocator = self.allocator;
        const florence_cfg = session_factory.getFlorenceConfig(self.vision_encoder) orelse return error.InvalidModelForReading;
        const batch = image_datas.len;
        if (batch == 0) return try allocator.alloc(ReadResult, 0);

        resetLastReadTelemetry();
        const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
        const img_size: u32 = @intCast(self.config.image_size);
        const ts: usize = @intCast(img_size);
        const per_image_side = std.math.mul(usize, ts, ts) catch return error.InvalidInputShape;
        const per_image = std.math.mul(usize, 3, per_image_side) catch return error.InvalidInputShape;
        const pixel_count = std.math.mul(usize, batch, per_image) catch return error.InvalidInputShape;
        const pixel_values = try allocator.alloc(f32, pixel_count);
        defer allocator.free(pixel_values);

        for (image_datas, 0..) |image_data, i| {
            {
                const decoded = try image.decode(allocator, image_data);
                defer decoded.deinit(allocator);
                const single = try image.preprocessDecodedWithResample(
                    allocator,
                    decoded,
                    img_size,
                    self.config.image_mean,
                    self.config.image_std,
                    self.config.resample,
                );
                defer allocator.free(single);
                @memcpy(pixel_values[i * per_image ..][0..per_image], single);
            }
        }

        const prompt_text = self.config.prompt orelse "<OCR>";
        const prompt_i32 = try buildFlorencePromptIds(
            allocator,
            self.tokenizer,
            florence_cfg,
            prompt_text,
        );
        defer allocator.free(prompt_i32);

        const prompt_len = prompt_i32.len;
        const prompt_total = std.math.mul(usize, batch, prompt_len) catch return error.InvalidInputShape;
        const prompt_i64 = try allocator.alloc(i64, prompt_total);
        defer allocator.free(prompt_i64);
        for (0..batch) |b| {
            for (prompt_i32, 0..) |id, i| prompt_i64[b * prompt_len + i] = id;
        }

        var cb = try session_factory.getComputeBackend(self.vision_encoder, allocator);
        defer cb.deinit();

        if (debug_cuda_session) std.log.info("reading: native florence batch encoder start batch={d}", .{batch});
        const encoder_start = nowNs();
        const encoder = try florence_arch.encoderForwardTensor(
            &cb,
            allocator,
            florence_cfg,
            pixel_values,
            batch,
            prompt_i64,
            prompt_len,
        );
        logReadProfile("batch_encoder", encoder_start);
        defer cb.free(encoder.hidden);

        const decode_start = nowNs();
        const backend = self.vision_encoder.backend();
        if ((backend == .cuda or backend == .metal) and !florenceKvCacheDisabled()) {
            last_read_telemetry.resident_decoder = true;
            last_read_telemetry.kv_cache = true;
            last_read_telemetry.cuda_graph_replay = false;
            last_read_telemetry.cuda_graph_fallback_reason = "batched_florence_kv_decode";
            const kv_result = self.decodeNativeFlorenceBatchIncrementalFromEncoder(&cb, florence_cfg, encoder.hidden, batch, encoder.seq_len) catch |err| fallback: {
                if (!shouldFallbackFlorenceIncremental(err)) return err;
                markFlorenceIncrementalFallback();
                break :fallback null;
            };
            if (kv_result) |result| {
                logReadProfile("batch_kv_decode_from_encoder", decode_start);
                return result;
            }
        }
        const result = try self.decodeNativeFlorenceBatchFromEncoder(&cb, florence_cfg, encoder.hidden, batch, encoder.seq_len);
        logReadProfile("batch_decode_from_encoder", decode_start);
        return result;
    }

    fn decodeNativeFlorenceBatchIncrementalFromEncoder(
        self: *ReadingPipeline,
        cb: *const ComputeBackend,
        florence_cfg: florence_arch.Config,
        encoder_hidden: CT,
        batch: usize,
        enc_seq_len: usize,
    ) ![]ReadResult {
        const allocator = self.allocator;
        const max_len = self.config.max_length;
        const out = try allocator.alloc(ReadResult, batch);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*result| result.deinit();
            allocator.free(out);
        }
        if (max_len == 0) {
            for (out) |*result| {
                result.* = .{ .text = try allocator.dupe(u8, ""), .allocator = allocator };
                filled += 1;
            }
            return out;
        }

        const dec_id_count = std.math.mul(usize, batch, max_len) catch return error.InvalidInputShape;
        const dec_ids = try allocator.alloc(i64, dec_id_count);
        defer allocator.free(dec_ids);
        @memset(dec_ids, @as(i64, self.config.pad_token_id));

        var lengths = try allocator.alloc(usize, batch);
        defer allocator.free(lengths);
        var finished = try allocator.alloc(bool, batch);
        defer allocator.free(finished);
        @memset(finished, false);

        var dec_len: usize = 1;
        for (0..batch) |b| {
            dec_ids[b * max_len] = @as(i64, self.config.decoder_start_token_id);
            lengths[b] = 1;
            if (self.config.forced_bos_token_id) |forced_bos| {
                if (max_len > 1) {
                    dec_ids[b * max_len + 1] = @as(i64, forced_bos);
                    lengths[b] = 2;
                    dec_len = 2;
                }
            }
        }

        const cache_start = nowNs();
        var cache = try florence_arch.buildDecoderIncrementalCache(cb, allocator, florence_cfg, encoder_hidden, batch, enc_seq_len, max_len);
        defer cache.deinit(cb, allocator);
        last_read_telemetry.kv_cache_mode = if (cache.self.preallocated) "batched_preallocated" else "batched_concat";
        logReadProfile("batch_florence_decoder_kv_cache_build", cache_start);

        var hidden_opt: ?CT = null;
        defer if (hidden_opt) |hidden| cb.free(hidden);

        const step_tokens = try allocator.alloc(i64, batch);
        defer allocator.free(step_tokens);
        for (0..dec_len) |idx| {
            for (0..batch) |b| step_tokens[b] = dec_ids[b * max_len + idx];
            const prefix_step_start = nowNs();
            const hidden = try florence_arch.decoderForwardIncrementalBatchStepFinalHiddenTensor(
                cb,
                allocator,
                florence_cfg,
                step_tokens,
                &cache,
            );
            if (idx + 1 == dec_len) {
                hidden_opt = hidden;
            } else {
                cb.free(hidden);
            }
            logReadProfileStep("batch_florence_decoder_kv_prefix_step", idx + 1, idx + 1, nowNs() - prefix_step_start);
        }

        var finished_count: usize = 0;
        var decoder_run_total_ns: u64 = 0;
        var decoder_steps: usize = 0;
        while (dec_len < max_len and finished_count < batch) {
            {
                const hidden = hidden_opt orelse return error.InvalidInputShape;
                hidden_opt = null;
                var hidden_live = true;
                errdefer if (hidden_live) cb.free(hidden);

                const logits_tensor = try florence_arch.decoderLmHeadLogitsRowsFromFinalHiddenTensor(
                    cb,
                    allocator,
                    florence_cfg,
                    hidden,
                    batch,
                );
                hidden_live = false;
                defer cb.free(logits_tensor);
                const logits = try cb.toFloat32(logits_tensor, allocator);
                defer allocator.free(logits);

                const vocab_size = florence_cfg.vocab_size;
                for (0..batch) |b| {
                    if (finished[b]) {
                        step_tokens[b] = @as(i64, self.config.pad_token_id);
                        continue;
                    }
                    const row = dec_ids[b * max_len ..][0..max_len];
                    const logits_offset = std.math.mul(usize, b, vocab_size) catch return error.InvalidInputShape;
                    const last_logits = logits[logits_offset..][0..vocab_size];
                    const best_id = selectGreedyToken(last_logits, row[0..dec_len], self.config.no_repeat_ngram_size);
                    if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) {
                        finished[b] = true;
                        finished_count += 1;
                        lengths[b] = dec_len;
                        step_tokens[b] = @as(i64, self.config.pad_token_id);
                    } else {
                        const token: i64 = @intCast(best_id);
                        row[dec_len] = token;
                        step_tokens[b] = token;
                        lengths[b] = dec_len + 1;
                    }
                }
            }

            dec_len += 1;
            decoder_steps += 1;
            if (dec_len < max_len and finished_count < batch) {
                const decoder_run_start = nowNs();
                hidden_opt = try florence_arch.decoderForwardIncrementalBatchStepFinalHiddenTensor(
                    cb,
                    allocator,
                    florence_cfg,
                    step_tokens,
                    &cache,
                );
                const decoder_run_ns = nowNs() - decoder_run_start;
                decoder_run_total_ns += decoder_run_ns;
                logReadProfileStep("batch_florence_decoder_kv_step", decoder_steps, dec_len, decoder_run_ns);
            }
        }
        logReadProfileStep("batch_florence_decoder_kv_run_total", decoder_steps, dec_len, decoder_run_total_ns);

        for (0..batch) |b| {
            const row = dec_ids[b * max_len ..][0..max_len];
            out[b] = try self.decodeGeneratedIds(row[0..lengths[b]], lengths[b]);
            filled += 1;
        }
        return out;
    }

    fn decodeNativeFlorenceBatchFromEncoder(
        self: *ReadingPipeline,
        cb: *const ComputeBackend,
        florence_cfg: florence_arch.Config,
        encoder_hidden: CT,
        batch: usize,
        enc_seq_len: usize,
    ) ![]ReadResult {
        const allocator = self.allocator;
        const max_len = self.config.max_length;
        const out = try allocator.alloc(ReadResult, batch);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*result| result.deinit();
            allocator.free(out);
        }
        if (max_len == 0) {
            for (out, 0..) |*result, i| {
                _ = i;
                result.* = .{ .text = try allocator.dupe(u8, ""), .allocator = allocator };
                filled += 1;
            }
            return out;
        }

        const encoder_mask_len = std.math.mul(usize, batch, enc_seq_len) catch return error.InvalidInputShape;
        const encoder_attention_mask = try allocator.alloc(i64, encoder_mask_len);
        defer allocator.free(encoder_attention_mask);
        @memset(encoder_attention_mask, 1);

        const dec_id_count = std.math.mul(usize, batch, max_len) catch return error.InvalidInputShape;
        const dec_ids = try allocator.alloc(i64, dec_id_count);
        defer allocator.free(dec_ids);
        @memset(dec_ids, @as(i64, self.config.pad_token_id));

        var lengths = try allocator.alloc(usize, batch);
        defer allocator.free(lengths);
        var finished = try allocator.alloc(bool, batch);
        defer allocator.free(finished);
        @memset(finished, false);

        var dec_len: usize = 1;
        for (0..batch) |b| {
            dec_ids[b * max_len] = @as(i64, self.config.decoder_start_token_id);
            lengths[b] = 1;
            if (self.config.forced_bos_token_id) |forced_bos| {
                if (max_len > 1) {
                    dec_ids[b * max_len + 1] = @as(i64, forced_bos);
                    lengths[b] = 2;
                    dec_len = 2;
                }
            }
        }

        const decode_loop_start = nowNs();
        var decoder_run_total_ns: u64 = 0;
        var decoder_steps: usize = 0;
        var finished_count: usize = 0;
        while (dec_len < max_len and finished_count < batch) {
            {
                const decoder_input_ids = try compactDecoderInputIds(allocator, dec_ids, batch, max_len, dec_len);
                defer allocator.free(decoder_input_ids);

                const decoder_run_start = nowNs();
                const logits = try florence_arch.decoderForward(
                    cb,
                    allocator,
                    florence_cfg,
                    decoder_input_ids,
                    encoder_hidden,
                    encoder_attention_mask,
                    batch,
                    dec_len,
                    enc_seq_len,
                );
                const decoder_run_ns = nowNs() - decoder_run_start;
                decoder_run_total_ns += decoder_run_ns;
                decoder_steps += 1;
                logReadProfileStep("batch_decoder_run", decoder_steps, dec_len, decoder_run_ns);
                defer allocator.free(logits);

                const vocab_size = florence_cfg.vocab_size;
                for (0..batch) |b| {
                    if (finished[b]) {
                        dec_ids[b * max_len + dec_len] = @as(i64, self.config.pad_token_id);
                        continue;
                    }
                    const row = dec_ids[b * max_len ..][0..max_len];
                    const batch_decode_offset = std.math.mul(usize, b, dec_len) catch return error.InvalidInputShape;
                    const token_offset = std.math.add(usize, batch_decode_offset, dec_len - 1) catch return error.InvalidInputShape;
                    const logits_offset = std.math.mul(usize, token_offset, vocab_size) catch return error.InvalidInputShape;
                    const last_logits = logits[logits_offset..][0..vocab_size];
                    const best_id = selectGreedyToken(last_logits, row[0..dec_len], self.config.no_repeat_ngram_size);
                    if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) {
                        finished[b] = true;
                        finished_count += 1;
                        lengths[b] = dec_len;
                        dec_ids[b * max_len + dec_len] = @as(i64, self.config.pad_token_id);
                    } else {
                        dec_ids[b * max_len + dec_len] = @intCast(best_id);
                        lengths[b] = dec_len + 1;
                    }
                }
            }
            dec_len += 1;
        }
        logReadProfileStep("batch_decoder_total", decoder_steps, dec_len, nowNs() - decode_loop_start);
        logReadProfileStep("batch_decoder_run_total", decoder_steps, dec_len, decoder_run_total_ns);

        for (0..batch) |b| {
            const row = dec_ids[b * max_len ..][0..max_len];
            out[b] = try self.decodeGeneratedIds(row[0..lengths[b]], lengths[b]);
            filled += 1;
        }
        return out;
    }

    /// Read text from an already-decoded image crop.
    pub fn readDecoded(self: *ReadingPipeline, img: image.Image) !ReadResult {
        resetLastReadTelemetry();
        if (expectsFlattenedPatches(self.vision_encoder)) {
            return self.readPix2StructDecoded(img);
        }

        const allocator = self.allocator;
        const img_size: u32 = @intCast(self.config.image_size);

        const pixel_values = try image.preprocessDecodedWithResample(
            allocator,
            img,
            img_size,
            self.config.image_mean,
            self.config.image_std,
            self.config.resample,
        );
        defer allocator.free(pixel_values);
        return self.readPixelValues(pixel_values);
    }

    fn readPixelValues(self: *ReadingPipeline, pixel_values: []const f32) !ReadResult {
        const allocator = self.allocator;
        const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
        const img_size: u32 = @intCast(self.config.image_size);

        // 1. Preprocess image: decode/resize/normalize → [1, 3, H, W] f32
        // 2. Run vision encoder
        const img_sz: i64 = @intCast(img_size);
        if (try self.readPixelValuesFlorenceResident(pixel_values)) |result| return result;

        const pv_shape = [_]i64{ 1, 3, img_sz, img_sz };
        if (debug_cuda_session) std.log.info("reading: pixel tensor init start", .{});
        var pv_tensor = try backends.Tensor.initFloat32(allocator, "pixel_values", &pv_shape, pixel_values);
        if (debug_cuda_session) std.log.info("reading: pixel tensor init done", .{});
        defer pv_tensor.deinit();

        const is_native_florence = session_factory.getFlorenceConfig(self.vision_encoder) != null;
        var prompt_ids_i64: ?[]i64 = null;
        defer if (prompt_ids_i64) |ids| allocator.free(ids);
        var prompt_tensor: ?backends.Tensor = null;
        defer if (prompt_tensor) |*t| t.deinit();

        const encoder_outputs = if (is_native_florence) blk: {
            const florence_cfg = session_factory.getFlorenceConfig(self.vision_encoder).?;
            const prompt_text = self.config.prompt orelse "<OCR>";
            const prompt_i32 = try buildFlorencePromptIds(
                allocator,
                self.tokenizer,
                florence_cfg,
                prompt_text,
            );
            defer allocator.free(prompt_i32);

            const prompt_len = prompt_i32.len;
            const prompt_i64 = try allocator.alloc(i64, prompt_len);
            errdefer allocator.free(prompt_i64);
            for (prompt_i32, 0..) |id, i| prompt_i64[i] = id;
            prompt_ids_i64 = prompt_i64;

            const prompt_shape = [_]i64{ 1, @intCast(prompt_len) };
            var pt = try backends.Tensor.initInt64(allocator, "input_ids", &prompt_shape, prompt_i64);
            errdefer pt.deinit();
            prompt_tensor = pt;

            break :blk try self.vision_encoder.run(&.{ pv_tensor, prompt_tensor.? }, allocator);
        } else try self.vision_encoder.run(&.{pv_tensor}, allocator);
        if (debug_cuda_session) std.log.info("reading: vision encoder run done outputs={d}", .{encoder_outputs.len});
        defer {
            for (encoder_outputs) |*t| {
                var mt = t.*;
                mt.deinit();
            }
            allocator.free(encoder_outputs);
        }

        if (encoder_outputs.len == 0) return error.NoEncoderOutput;
        logTensorStatsF32("encoder_hidden(session)", encoder_outputs[0].asFloat32());
        if (debug_cuda_session) std.log.info("reading: decode from encoder outputs start", .{});
        const decode_start = nowNs();
        const result = try self.decodeFromEncoderOutputs(encoder_outputs, null);
        logReadProfile("decode_from_encoder", decode_start);
        return result;
    }

    fn readNativeFlorencePixelValues(self: *ReadingPipeline, pixel_values: []const f32, florence_cfg: florence_arch.Config) !ReadResult {
        const allocator = self.allocator;
        const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
        const prompt_text = self.config.prompt orelse "<OCR>";
        if (debug_cuda_session) std.log.info("reading: native florence prompt ids start prompt={s}", .{prompt_text});
        const prompt_i32 = try buildFlorencePromptIds(
            allocator,
            self.tokenizer,
            florence_cfg,
            prompt_text,
        );
        if (debug_cuda_session) std.log.info("reading: native florence prompt ids done len={d}", .{prompt_i32.len});
        defer allocator.free(prompt_i32);

        const prompt_i64 = try allocator.alloc(i64, prompt_i32.len);
        defer allocator.free(prompt_i64);
        for (prompt_i32, 0..) |id, i| prompt_i64[i] = id;

        var cb = try session_factory.getComputeBackend(self.vision_encoder, allocator);
        defer cb.deinit();

        if (debug_cuda_session) std.log.info("reading: native florence encoder tensor run start", .{});
        const encoder_start = nowNs();
        const encoder = try florence_arch.encoderForwardTensor(
            &cb,
            allocator,
            florence_cfg,
            pixel_values,
            1,
            prompt_i64,
            prompt_i64.len,
        );
        logReadProfile("encoder", encoder_start);
        if (debug_cuda_session) std.log.info("reading: native florence encoder tensor run done seq={d}", .{encoder.seq_len});
        defer cb.free(encoder.hidden);

        const decode_start = nowNs();
        const result = try self.decodeNativeFlorenceFromEncoder(&cb, florence_cfg, encoder.hidden, encoder.seq_len);
        logReadProfile("decode_from_encoder", decode_start);
        return result;
    }

    fn decodeNativeFlorenceFromEncoder(
        self: *ReadingPipeline,
        cb: *const ComputeBackend,
        florence_cfg: florence_arch.Config,
        encoder_hidden: CT,
        enc_seq_len: usize,
    ) !ReadResult {
        const allocator = self.allocator;
        const encoder_attention_mask = try allocator.alloc(i64, enc_seq_len);
        defer allocator.free(encoder_attention_mask);
        @memset(encoder_attention_mask, 1);

        const max_len = self.config.max_length;
        if (max_len == 0) return .{ .text = try allocator.dupe(u8, ""), .allocator = allocator };
        var dec_ids = try allocator.alloc(i64, max_len);
        defer allocator.free(dec_ids);
        dec_ids[0] = self.config.decoder_start_token_id;
        var dec_len: usize = 1;
        if (self.config.forced_bos_token_id) |forced_bos| {
            if (max_len > 1) {
                dec_ids[1] = forced_bos;
                dec_len = 2;
            }
        }

        var cross_cache: ?florence_arch.DecoderCrossCache = null;
        defer {
            if (cross_cache) |*cache| cache.deinit(cb, allocator);
        }
        if (platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_CROSS_KV_CACHE")) {
            const cache_start = nowNs();
            cross_cache = try florence_arch.buildDecoderCrossCache(cb, allocator, florence_cfg, encoder_hidden, 1, enc_seq_len);
            logReadProfile("decoder_cross_cache", cache_start);
        }

        const decode_loop_start = nowNs();
        var decoder_run_total_ns: u64 = 0;
        var decoder_steps: usize = 0;
        while (dec_len < max_len) {
            const decoder_run_start = nowNs();
            const logits = if (cross_cache) |*cache|
                try florence_arch.decoderForwardCached(
                    cb,
                    allocator,
                    florence_cfg,
                    dec_ids[0..dec_len],
                    encoder_attention_mask,
                    1,
                    dec_len,
                    enc_seq_len,
                    cache,
                )
            else
                try florence_arch.decoderForward(
                    cb,
                    allocator,
                    florence_cfg,
                    dec_ids[0..dec_len],
                    encoder_hidden,
                    encoder_attention_mask,
                    1,
                    dec_len,
                    enc_seq_len,
                );
            const decoder_run_ns = nowNs() - decoder_run_start;
            decoder_run_total_ns += decoder_run_ns;
            decoder_steps += 1;
            logReadProfileStep("decoder_run", decoder_steps, dec_len, decoder_run_ns);
            defer allocator.free(logits);

            const vocab_size = florence_cfg.vocab_size;
            const last_logits = logits[(dec_len - 1) * vocab_size ..][0..vocab_size];
            const best_id = selectGreedyToken(last_logits, dec_ids[0..dec_len], self.config.no_repeat_ngram_size);
            if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) break;

            dec_ids[dec_len] = @intCast(best_id);
            dec_len += 1;
        }
        logReadProfileStep("decoder_total", decoder_steps, dec_len, nowNs() - decode_loop_start);
        logReadProfileStep("decoder_run_total", decoder_steps, dec_len, decoder_run_total_ns);

        const prefix_len: usize = if (self.config.forced_bos_token_id != null and dec_len > 1) 2 else 1;
        const text_len = if (dec_len > prefix_len) dec_len - prefix_len else 0;
        const token_ids = try allocator.alloc(i32, text_len);
        defer allocator.free(token_ids);
        for (0..text_len) |i| token_ids[i] = @intCast(dec_ids[prefix_len + i]);

        const text = try self.tokenizer.decode(allocator, token_ids);
        const cleaned = try cleanupPureText(allocator, text);
        allocator.free(text);
        return .{ .text = cleaned, .allocator = allocator };
    }

    fn readPixelValuesFlorenceResident(self: *ReadingPipeline, pixel_values: []const f32) !?ReadResult {
        const backend = self.vision_encoder.backend();
        if (backend != .metal and backend != .cuda) return null;
        if (self.vision_encoder.vtable != self.decoder.vtable or self.vision_encoder.ptr != self.decoder.ptr) return null;

        const florence_cfg = session_factory.getFlorenceConfig(self.vision_encoder) orelse return null;
        last_read_telemetry.resident_decoder = true;
        const allocator = self.allocator;
        var cb = try session_factory.getComputeBackend(self.vision_encoder, allocator);
        defer cb.deinit();

        const prompt_text = self.config.prompt orelse "<OCR>";
        const prompt_i32 = try buildFlorencePromptIds(
            allocator,
            self.tokenizer,
            florence_cfg,
            prompt_text,
        );
        defer allocator.free(prompt_i32);

        const prompt_i64 = try allocator.alloc(i64, prompt_i32.len);
        defer allocator.free(prompt_i64);
        for (prompt_i32, 0..) |id, i| prompt_i64[i] = id;

        const encoder = (try session_factory.runFlorenceEncoderResident(
            self.vision_encoder,
            &cb,
            allocator,
            pixel_values,
            1,
            prompt_i64,
            prompt_i64.len,
        )) orelse return null;
        defer cb.free(encoder.hidden);

        if (florenceDebugStats()) {
            const hidden_host = try cb.toFloat32(encoder.hidden, allocator);
            defer allocator.free(hidden_host);
            logTensorStatsF32("encoder_hidden(resident)", hidden_host);
        }

        const encoder_attention_mask = try allocator.alloc(i64, encoder.seq_len);
        defer allocator.free(encoder_attention_mask);
        @memset(encoder_attention_mask, 1);

        const max_len = self.config.max_length;
        if (max_len == 0) return .{ .text = try allocator.dupe(u8, ""), .allocator = allocator };
        var dec_ids = try allocator.alloc(i64, max_len);
        defer allocator.free(dec_ids);
        dec_ids[0] = self.config.decoder_start_token_id;
        var dec_len: usize = 1;
        if (self.config.forced_bos_token_id) |forced_bos| {
            if (max_len > 1) {
                dec_ids[1] = forced_bos;
                dec_len = 2;
            }
        }

        // Incremental KV-cache decode is default-on for both device backends;
        // ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE is the kill switch.
        const kv_cache_backend_ok = backend == .cuda or backend == .metal;
        if (kv_cache_backend_ok and !florenceKvCacheDisabled()) {
            last_read_telemetry.kv_cache = true;
            last_read_telemetry.cuda_graph_replay = false;
            last_read_telemetry.cuda_graph_fallback_reason = if (florenceCudaGraphEnabled()) null else "florence_graph_disabled";
            const decode_start = nowNs();
            const result = self.decodeFlorenceResidentIncremental(
                &cb,
                florence_cfg,
                encoder.hidden,
                encoder.seq_len,
                dec_ids,
                dec_len,
            ) catch |err| fallback: {
                if (!shouldFallbackFlorenceIncremental(err)) return err;
                markFlorenceIncrementalFallback();
                break :fallback null;
            };
            if (result) |decoded| {
                logReadProfile("florence_resident_kv_decode", decode_start);
                return decoded;
            }
        }

        while (dec_len < max_len) {
            const logits = (try session_factory.runFlorenceDecoderResident(
                self.decoder,
                &cb,
                allocator,
                dec_ids[0..dec_len],
                encoder.hidden,
                encoder_attention_mask,
                1,
                dec_len,
                encoder.seq_len,
            )) orelse return null;
            defer cb.free(logits);

            if (florenceDebugStats() and dec_len <= 5) {
                const logits_host = try cb.toFloat32(logits, allocator);
                defer allocator.free(logits_host);
                const last_row = logits_host[(dec_len - 1) * florence_cfg.vocab_size ..][0..florence_cfg.vocab_size];
                logTopLogits("logits(resident)", dec_len, last_row);
            }

            const suppress_tokens = try buildNoRepeatSuppressTokens(
                allocator,
                dec_ids[0..dec_len],
                self.config.no_repeat_ngram_size,
            );
            defer allocator.free(suppress_tokens);

            const best_id = if (suppress_tokens.len == 0) blk: {
                break :blk (try cb.argmaxLastRow(logits, dec_len, florence_cfg.vocab_size)) orelse return error.UnsupportedOperation;
            } else blk: {
                if (try cb.argmaxRowsSuppress(logits, dec_len - 1, 1, florence_cfg.vocab_size, suppress_tokens, allocator)) |tokens| {
                    defer allocator.free(tokens);
                    if (tokens.len != 1) return error.InvalidTensorShape;
                    break :blk tokens[0];
                }

                if (try cb.argmaxLastRowSuppressTensor(logits, dec_len, florence_cfg.vocab_size, suppress_tokens)) |token_tensor| {
                    defer cb.free(token_tensor);
                    const token_ids = try cb.toFloat32(token_tensor, allocator);
                    defer allocator.free(token_ids);
                    if (token_ids.len != 1 or token_ids[0] < 0) return error.InvalidTensorShape;
                    break :blk @as(u32, @intFromFloat(token_ids[0]));
                }

                return error.UnsupportedFlorence2NoRepeatMetal;
            };
            if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) break;

            dec_ids[dec_len] = @intCast(best_id);
            dec_len += 1;
        }

        return try self.decodeGeneratedIds(dec_ids[0..dec_len], dec_len);
    }

    fn decodeFlorenceResidentIncremental(
        self: *ReadingPipeline,
        cb: *const ComputeBackend,
        florence_cfg: florence_arch.Config,
        encoder_hidden: CT,
        enc_seq_len: usize,
        dec_ids: []i64,
        initial_len: usize,
    ) !ReadResult {
        const allocator = self.allocator;
        const max_len = self.config.max_length;
        if (initial_len == 0 or initial_len > max_len or dec_ids.len < max_len) return error.InvalidInputShape;

        const cache_start = nowNs();
        var cache = try florence_arch.buildDecoderIncrementalCache(cb, allocator, florence_cfg, encoder_hidden, 1, enc_seq_len, max_len);
        defer cache.deinit(cb, allocator);
        last_read_telemetry.kv_cache_mode = if (cache.self.preallocated) "preallocated" else "concat";
        logReadProfile("florence_decoder_kv_cache_build", cache_start);

        const fused_lm_head_allowed = if (self.florence_final_logits_bias_zero_cache) |cache_ptr| blk: {
            if (cache_ptr.*) |cached| break :blk cached;
            const bias_check_start = nowNs();
            const value = try florence_arch.decoderFinalLogitsBiasIsZero(cb, allocator, florence_cfg.vocab_size);
            logReadProfile("florence_decoder_final_logits_bias_check", bias_check_start);
            cache_ptr.* = value;
            break :blk value;
        } else blk: {
            const bias_check_start = nowNs();
            const value = try florence_arch.decoderFinalLogitsBiasIsZero(cb, allocator, florence_cfg.vocab_size);
            logReadProfile("florence_decoder_final_logits_bias_check", bias_check_start);
            break :blk value;
        };
        if (!fused_lm_head_allowed and last_read_telemetry.lm_head_path == null) {
            last_read_telemetry.lm_head_path = "full_logits_bias";
        }

        var hidden_opt: ?CT = null;
        defer if (hidden_opt) |hidden| cb.free(hidden);

        var dec_len = initial_len;
        for (0..dec_len) |idx| {
            const prefix_step_start = nowNs();
            const hidden = try florence_arch.decoderForwardIncrementalStepFinalHiddenTensor(
                cb,
                allocator,
                florence_cfg,
                dec_ids[idx],
                &cache,
            );
            if (idx + 1 == dec_len) {
                hidden_opt = hidden;
            } else {
                cb.free(hidden);
            }
            logReadProfileStep("florence_decoder_kv_prefix_step", idx + 1, idx + 1, nowNs() - prefix_step_start);
        }

        var decoder_run_total_ns: u64 = 0;
        var decoder_steps: usize = 0;
        while (dec_len < max_len) {
            var hidden = hidden_opt orelse return error.InvalidInputShape;
            hidden_opt = null;
            var hidden_live = true;
            errdefer if (hidden_live) cb.free(hidden);
            decoder_steps += 1;

            const suppress_tokens = try buildNoRepeatSuppressTokens(
                allocator,
                dec_ids[0..dec_len],
                self.config.no_repeat_ngram_size,
            );
            var suppress_tokens_live = true;
            errdefer if (suppress_tokens_live) allocator.free(suppress_tokens);

            const best_id = try self.selectFlorenceTokenFromFinalHidden(
                cb,
                florence_cfg,
                &hidden,
                &hidden_live,
                suppress_tokens,
                fused_lm_head_allowed,
            );
            allocator.free(suppress_tokens);
            suppress_tokens_live = false;

            if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) break;

            dec_ids[dec_len] = @intCast(best_id);
            dec_len += 1;
            if (dec_len < max_len) {
                const decoder_run_start = nowNs();
                hidden_opt = try florence_arch.decoderForwardIncrementalStepFinalHiddenTensor(
                    cb,
                    allocator,
                    florence_cfg,
                    dec_ids[dec_len - 1],
                    &cache,
                );
                const decoder_run_ns = nowNs() - decoder_run_start;
                decoder_run_total_ns += decoder_run_ns;
                logReadProfileStep("florence_decoder_kv_step", decoder_steps, dec_len, decoder_run_ns);
            }
        }
        logReadProfileStep("florence_decoder_kv_run_total", decoder_steps, dec_len, decoder_run_total_ns);

        const decode_ids_start = nowNs();
        const result = try self.decodeGeneratedIds(dec_ids[0..dec_len], dec_len);
        logReadProfile("florence_decoder_decode_ids", decode_ids_start);
        return result;
    }

    fn selectFlorenceTokenFromFinalHidden(
        self: *ReadingPipeline,
        cb: *const ComputeBackend,
        florence_cfg: florence_arch.Config,
        hidden: *CT,
        hidden_live: *bool,
        suppress_tokens: []const i32,
        fused_lm_head_allowed: bool,
    ) !u32 {
        const allocator = self.allocator;

        if (fused_lm_head_allowed) {
            var capture_graph = false;
            errdefer if (capture_graph) cb.debugCudaGraphCaptureEnd(false) catch {};
            if (florenceCudaGraphEnabled()) {
                if (suppress_tokens.len == 0) {
                    if (try cb.debugCudaGraphPrepareGreedyTokenReplayInput("florence.lm_head_argmax", hidden.*, 0)) |prepared| {
                        cb.free(hidden.*);
                        hidden.* = prepared;
                        hidden_live.* = true;
                        _ = try cb.debugCudaGraphPrepareDecodeScalars(0, 0, 0, 0, 0);
                        if (try cb.debugCudaGraphReplayFinalHidden(hidden.*)) |token_tensor| {
                            defer cb.free(token_tensor);
                            cb.free(hidden.*);
                            hidden_live.* = false;
                            last_read_telemetry.cuda_graph_replay = true;
                            last_read_telemetry.cuda_graph_replay_steps += 1;
                            last_read_telemetry.lm_head_path = "fused_argmax_graph";
                            return try tokenTensorToU32(cb, allocator, token_tensor);
                        }
                        capture_graph = try cb.debugCudaGraphCaptureBegin("florence.lm_head_argmax");
                        if (capture_graph) {
                            try cb.debugCudaTraceTensor("florence.lm_head_input", hidden.*);
                            try cb.debugCudaGraphRegisterFinalHiddenReplayInput(hidden.*);
                        } else if (last_read_telemetry.cuda_graph_fallback_reason == null) {
                            last_read_telemetry.cuda_graph_fallback_reason = "florence_graph_capture_unavailable";
                        }
                    } else if (last_read_telemetry.cuda_graph_fallback_reason == null) {
                        last_read_telemetry.cuda_graph_fallback_reason = "florence_graph_prepare_unavailable";
                    }
                } else if (last_read_telemetry.cuda_graph_fallback_reason == null) {
                    last_read_telemetry.cuda_graph_fallback_reason = "florence_graph_suppress_tokens_dynamic";
                }
            }
            const fused_start = nowNs();
            if (try florence_arch.decoderFusedTokenFromFinalHiddenTensor(cb, florence_cfg, hidden.*, suppress_tokens)) |token_tensor| {
                logReadProfile("florence_decoder_lm_head_fused_argmax", fused_start);
                defer cb.free(token_tensor);
                if (capture_graph) {
                    try cb.debugCudaGraphRegisterFinalHiddenReplayBoundary(hidden.*, token_tensor);
                    try cb.debugCudaGraphCaptureEnd(true);
                    last_read_telemetry.cuda_graph_capture_steps += 1;
                    capture_graph = false;
                }
                cb.free(hidden.*);
                hidden_live.* = false;
                if (last_read_telemetry.lm_head_path == null) last_read_telemetry.lm_head_path = "fused_argmax";
                return try tokenTensorToU32(cb, allocator, token_tensor);
            }
            logReadProfile("florence_decoder_lm_head_fused_unavailable", fused_start);
            if (capture_graph) {
                try cb.debugCudaGraphCaptureEnd(false);
                capture_graph = false;
            }
            if (last_read_telemetry.lm_head_path == null) last_read_telemetry.lm_head_path = "full_logits_fused_unavailable";
        }

        const logits = try florence_arch.decoderLmHeadLogitsFromFinalHiddenTensor(cb, allocator, florence_cfg, hidden.*);
        hidden_live.* = false;
        defer cb.free(logits);

        if (suppress_tokens.len == 0) {
            if (last_read_telemetry.lm_head_path == null) last_read_telemetry.lm_head_path = "full_logits";
            const token = (try cb.argmaxLastRow(logits, 1, florence_cfg.vocab_size)) orelse return error.UnsupportedOperation;
            return token;
        }

        if (try cb.argmaxRowsSuppress(logits, 0, 1, florence_cfg.vocab_size, suppress_tokens, allocator)) |tokens| {
            defer allocator.free(tokens);
            if (tokens.len != 1) return error.InvalidTensorShape;
            if (last_read_telemetry.lm_head_path == null) last_read_telemetry.lm_head_path = "full_logits";
            return tokens[0];
        }

        if (try cb.argmaxLastRowSuppressTensor(logits, 1, florence_cfg.vocab_size, suppress_tokens)) |token_tensor| {
            defer cb.free(token_tensor);
            if (last_read_telemetry.lm_head_path == null) last_read_telemetry.lm_head_path = "full_logits";
            return try tokenTensorToU32(cb, allocator, token_tensor);
        }

        return error.UnsupportedFlorence2NoRepeatMetal;
    }

    fn readPix2StructDecoded(self: *ReadingPipeline, img: image.Image) !ReadResult {
        const allocator = self.allocator;
        const patch_height = if (self.config.pix2struct_patch_height > 0) self.config.pix2struct_patch_height else 16;
        const patch_width = if (self.config.pix2struct_patch_width > 0) self.config.pix2struct_patch_width else 16;
        const max_patches = if (self.config.pix2struct_max_patches > 0) self.config.pix2struct_max_patches else 2048;

        var patches = try image.preprocessDecodedPix2Struct(
            allocator,
            img,
            patch_height,
            patch_width,
            max_patches,
            self.config.pix2struct_do_normalize,
            self.config.resample,
        );
        defer patches.deinit();

        const feature_depth = 2 + patch_height * patch_width * 3;
        const patch_shape = [_]i64{ 1, @intCast(max_patches), @intCast(feature_depth) };
        var patch_tensor = try backends.Tensor.initFloat32(allocator, "flattened_patches", &patch_shape, patches.flattened_patches);
        defer patch_tensor.deinit();

        const mask_shape = [_]i64{ 1, @intCast(max_patches) };
        var mask_tensor = try backends.Tensor.initInt64(allocator, "attention_mask", &mask_shape, patches.attention_mask);
        defer mask_tensor.deinit();

        const inputs = if (hasInput(self.vision_encoder, "attention_mask"))
            &[_]backends.Tensor{ patch_tensor, mask_tensor }
        else
            &[_]backends.Tensor{patch_tensor};

        const encoder_outputs = try self.vision_encoder.run(inputs, allocator);
        defer {
            for (encoder_outputs) |*t| {
                var mt = t.*;
                mt.deinit();
            }
            allocator.free(encoder_outputs);
        }
        if (encoder_outputs.len == 0) return error.NoEncoderOutput;

        return self.decodeFromEncoderOutputs(encoder_outputs, patches.attention_mask);
    }

    fn decodeFromEncoderOutputs(
        self: *ReadingPipeline,
        encoder_outputs: []const backends.Tensor,
        encoder_attention_mask_opt: ?[]const i64,
    ) !ReadResult {
        const allocator = self.allocator;
        const encoder_hidden = &encoder_outputs[0];

        const enc_seq_len: usize = if (encoder_hidden.shape.len >= 2) @intCast(encoder_hidden.shape[1]) else 1;
        const encoder_attention_mask = if (encoder_attention_mask_opt) |mask|
            mask
        else blk: {
            const all_ones = try allocator.alloc(i64, enc_seq_len);
            @memset(all_ones, 1);
            break :blk all_ones;
        };
        defer if (encoder_attention_mask_opt == null) allocator.free(encoder_attention_mask);

        // 3. Autoregressive decode
        const max_len = self.config.max_length;
        if (max_len == 0) return .{ .text = try allocator.dupe(u8, ""), .allocator = allocator };
        var dec_ids = try allocator.alloc(i64, max_len);
        defer allocator.free(dec_ids);
        dec_ids[0] = self.config.decoder_start_token_id;
        var dec_len: usize = 1;
        if (self.config.forced_bos_token_id) |forced_bos| {
            if (max_len > 1) {
                dec_ids[1] = forced_bos;
                dec_len = 2;
            }
        }

        const decode_loop_start = nowNs();
        var decoder_run_total_ns: u64 = 0;
        var decoder_steps: usize = 0;
        while (dec_len < max_len) {
            // Build decoder input tensors
            const dec_seq: i64 = @intCast(dec_len);
            const dec_shape = [_]i64{ 1, dec_seq };

            var decoder_inputs = std.ArrayListUnmanaged(backends.Tensor).empty;
            defer {
                for (decoder_inputs.items) |*tensor| tensor.deinit();
                decoder_inputs.deinit(allocator);
            }

            try decoder_inputs.append(
                allocator,
                try backends.Tensor.initInt64(allocator, "input_ids", &dec_shape, dec_ids[0..dec_len]),
            );

            if (hasInput(self.decoder, "encoder_attention_mask")) {
                const enc_mask_shape = [_]i64{ 1, @intCast(enc_seq_len) };
                try decoder_inputs.append(
                    allocator,
                    try backends.Tensor.initInt64(allocator, "encoder_attention_mask", &enc_mask_shape, encoder_attention_mask),
                );
            }
            if (hasInput(self.decoder, "decoder_attention_mask")) {
                const dec_mask = try allocator.alloc(i64, dec_len);
                defer allocator.free(dec_mask);
                @memset(dec_mask, 1);
                try decoder_inputs.append(
                    allocator,
                    try backends.Tensor.initInt64(allocator, "decoder_attention_mask", &dec_shape, dec_mask),
                );
            }

            const enc_hidden = encoder_outputs[0].borrowedView("encoder_hidden_states");
            try decoder_inputs.append(allocator, enc_hidden);

            const decoder_run_start = nowNs();
            const dec_outputs = try self.decoder.run(decoder_inputs.items, allocator);
            const decoder_run_ns = nowNs() - decoder_run_start;
            decoder_run_total_ns += decoder_run_ns;
            decoder_steps += 1;
            logReadProfileStep("decoder_run", decoder_steps, dec_len, decoder_run_ns);
            defer {
                for (dec_outputs) |*t| {
                    var mt = t.*;
                    mt.deinit();
                }
                allocator.free(dec_outputs);
            }

            if (dec_outputs.len == 0) return error.NoDecoderOutput;

            // Get logits for last position
            const logits = dec_outputs[0].asFloat32();
            const vocab_size = if (dec_outputs[0].shape.len >= 3)
                @as(usize, @intCast(dec_outputs[0].shape[2]))
            else
                return error.InvalidLogitsShape;

            // Last position logits
            const last_logits = logits[(dec_len - 1) * vocab_size ..][0..vocab_size];
            if (decoder_steps <= 4) logTopLogits("logits(session)", decoder_steps, last_logits);

            // Greedy: argmax
            const best_id = selectGreedyToken(last_logits, dec_ids[0..dec_len], self.config.no_repeat_ngram_size);

            // Check for EOS
            if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) break;

            dec_ids[dec_len] = @intCast(best_id);
            dec_len += 1;
        }
        logReadProfileStep("decoder_total", decoder_steps, dec_len, nowNs() - decode_loop_start);
        logReadProfileStep("decoder_run_total", decoder_steps, dec_len, decoder_run_total_ns);

        return try self.decodeGeneratedIds(dec_ids[0..dec_len], dec_len);
    }

    fn decodeGeneratedIds(self: *ReadingPipeline, dec_ids: []const i64, dec_len: usize) !ReadResult {
        const allocator = self.allocator;
        const prefix_len: usize = if (self.config.forced_bos_token_id != null and dec_len > 1) 2 else 1;
        const text_len = if (dec_len > prefix_len) dec_len - prefix_len else 0;
        last_read_telemetry.generated_tokens = text_len;
        const token_ids = try allocator.alloc(i32, text_len);
        defer allocator.free(token_ids);
        for (0..text_len) |i| token_ids[i] = @intCast(dec_ids[prefix_len + i]);

        const text = try self.tokenizer.decode(allocator, token_ids);
        const cleaned = try cleanupPureText(allocator, text);
        allocator.free(text);
        return .{ .text = cleaned, .allocator = allocator };
    }

    pub fn deinit(_: *ReadingPipeline) void {
        // Sessions and tokenizer are borrowed — caller manages their lifetime.
    }
};

fn compactDecoderInputIds(
    allocator: std.mem.Allocator,
    dec_ids: []const i64,
    batch: usize,
    max_len: usize,
    dec_len: usize,
) ![]i64 {
    const total = std.math.mul(usize, batch, dec_len) catch return error.InvalidInputShape;
    const out = try allocator.alloc(i64, total);
    errdefer allocator.free(out);
    for (0..batch) |b| {
        const dst = std.math.mul(usize, b, dec_len) catch return error.InvalidInputShape;
        const src = std.math.mul(usize, b, max_len) catch return error.InvalidInputShape;
        @memcpy(out[dst..][0..dec_len], dec_ids[src..][0..dec_len]);
    }
    return out;
}

fn nativeFlorenceReadBatchSize() usize {
    const raw = platform.env.getenv("ANTFLY_INFERENCE_READ_BATCH_SIZE") orelse return 8;
    const parsed = std.fmt.parseInt(usize, raw, 10) catch return 8;
    return std.math.clamp(parsed, 1, 64);
}

fn shouldFallbackFlorenceIncremental(err: anyerror) bool {
    return switch (err) {
        error.InvalidInputShape, error.UnsupportedOperation, error.UnsupportedShape => true,
        else => false,
    };
}

fn markFlorenceIncrementalFallback() void {
    last_read_telemetry.kv_cache = false;
    last_read_telemetry.kv_cache_mode = null;
    last_read_telemetry.lm_head_path = null;
    last_read_telemetry.cuda_graph_replay = false;
    last_read_telemetry.cuda_graph_capture_steps = 0;
    last_read_telemetry.cuda_graph_replay_steps = 0;
    last_read_telemetry.cuda_graph_fallback_reason = "florence_incremental_decode_unsupported";
}

fn readProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_READ_PROFILE");
}

fn florenceKvCacheDisabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE");
}

fn florenceDebugStats() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_DEBUG_STATS");
}

fn logTensorStatsF32(label: []const u8, values: []const f32) void {
    if (!florenceDebugStats()) return;
    if (values.len == 0) {
        std.log.info("florence-debug {s}: empty", .{label});
        return;
    }
    var sum: f64 = 0;
    var absmax: f32 = 0;
    for (values) |v| {
        sum += v;
        const a = @abs(v);
        if (a > absmax) absmax = a;
    }
    const mean = sum / @as(f64, @floatFromInt(values.len));
    const n_first = @min(values.len, 4);
    std.log.info("florence-debug {s}: n={d} mean={d:.6} absmax={d:.4} first={any}", .{
        label, values.len, mean, absmax, values[0..n_first],
    });
}

fn logTopLogits(label: []const u8, step: usize, logits: []const f32) void {
    if (!florenceDebugStats()) return;
    var top_ids: [3]usize = .{ 0, 0, 0 };
    var top_vals: [3]f32 = .{ -std.math.inf(f32), -std.math.inf(f32), -std.math.inf(f32) };
    for (logits, 0..) |v, i| {
        if (v > top_vals[0]) {
            top_vals[2] = top_vals[1];
            top_ids[2] = top_ids[1];
            top_vals[1] = top_vals[0];
            top_ids[1] = top_ids[0];
            top_vals[0] = v;
            top_ids[0] = i;
        } else if (v > top_vals[1]) {
            top_vals[2] = top_vals[1];
            top_ids[2] = top_ids[1];
            top_vals[1] = v;
            top_ids[1] = i;
        } else if (v > top_vals[2]) {
            top_vals[2] = v;
            top_ids[2] = i;
        }
    }
    std.log.info("florence-debug {s} step={d}: top=[{d}:{d:.4}, {d}:{d:.4}, {d}:{d:.4}]", .{
        label, step, top_ids[0], top_vals[0], top_ids[1], top_vals[1], top_ids[2], top_vals[2],
    });
}

fn florenceCudaGraphEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_CUDA_GRAPH");
}

fn tokenTensorToU32(cb: *const ComputeBackend, allocator: std.mem.Allocator, token_tensor: CT) !u32 {
    const download_start = nowNs();
    const token_ids = try cb.toFloat32(token_tensor, allocator);
    logReadProfile("florence_decoder_token_download", download_start);
    defer allocator.free(token_ids);
    if (token_ids.len != 1 or token_ids[0] < 0) return error.InvalidTensorShape;
    return @as(u32, @intFromFloat(token_ids[0]));
}

fn logReadProfile(phase: []const u8, start_ns: u64) void {
    if (!readProfileEnabled()) return;
    std.log.info("read-profile phase={s} elapsed_ms={d:.3}", .{ phase, nsToMs(nowNs() - start_ns) });
}

fn logReadProfileStep(phase: []const u8, step: usize, seq_len: usize, elapsed_ns: u64) void {
    if (!readProfileEnabled()) return;
    std.log.info("read-profile phase={s} step={d} seq_len={d} elapsed_ms={d:.3}", .{ phase, step, seq_len, nsToMs(elapsed_ns) });
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn hasInput(session: backends.Session, name: []const u8) bool {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return true;
    }
    return false;
}

fn expectsFlattenedPatches(session: backends.Session) bool {
    return hasInput(session, "flattened_patches");
}

fn buildFlorencePromptIds(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    cfg: anytype,
    prompt: []const u8,
) ![]i32 {
    const normalized = normalizeFlorencePrompt(prompt);
    const prompt_ids = try tokenizer.encode(allocator, normalized);
    defer allocator.free(prompt_ids);

    const total = 2 + prompt_ids.len;
    const ids = try allocator.alloc(i32, total);
    ids[0] = cfg.bos_token_id;
    @memcpy(ids[1..][0..prompt_ids.len], prompt_ids);
    ids[1 + prompt_ids.len] = cfg.eos_token_id;
    return ids;
}

fn normalizeFlorencePrompt(prompt: []const u8) []const u8 {
    if (std.mem.eql(u8, prompt, "<OCR>")) return "What is the text in the image?";
    if (std.mem.eql(u8, prompt, "<OCR_WITH_REGION>")) return "What is the text in the image, with regions?";
    if (std.mem.eql(u8, prompt, "<CAPTION>")) return "What does the image describe?";
    if (std.mem.eql(u8, prompt, "<DETAILED_CAPTION>")) return "Describe in detail what is shown in the image.";
    if (std.mem.eql(u8, prompt, "<MORE_DETAILED_CAPTION>")) return "Describe with a paragraph what is shown in the image.";
    if (std.mem.eql(u8, prompt, "<OD>")) return "Locate the objects with category name in the image.";
    if (std.mem.eql(u8, prompt, "<DENSE_REGION_CAPTION>")) return "Locate the objects in the image, with their descriptions.";
    if (std.mem.eql(u8, prompt, "<REGION_PROPOSAL>")) return "Locate the region proposals in the image.";
    return prompt;
}

fn cleanupPureText(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var cleaned = std.ArrayListUnmanaged(u8).empty;
    defer cleaned.deinit(allocator);

    var i: usize = 0;
    while (i < text.len) {
        if (std.mem.startsWith(u8, text[i..], "<s>")) {
            i += 3;
            continue;
        }
        if (std.mem.startsWith(u8, text[i..], "</s>")) {
            i += 4;
            continue;
        }
        try cleaned.append(allocator, text[i]);
        i += 1;
    }

    return allocator.dupe(u8, std.mem.trim(u8, cleaned.items, " \t\r\n"));
}

fn selectGreedyToken(logits: []const f32, prefix: []const i64, no_repeat_ngram_size: usize) usize {
    var best_id: usize = 0;
    var best_val: f32 = -std.math.inf(f32);

    for (0..logits.len) |i| {
        if (no_repeat_ngram_size > 0 and wouldRepeatNgram(prefix, @intCast(i), no_repeat_ngram_size)) continue;
        if (logits[i] > best_val) {
            best_val = logits[i];
            best_id = i;
        }
    }

    if (best_val != -std.math.inf(f32)) return best_id;

    best_id = 0;
    best_val = logits[0];
    for (1..logits.len) |i| {
        if (logits[i] > best_val) {
            best_val = logits[i];
            best_id = i;
        }
    }
    return best_id;
}

fn buildNoRepeatSuppressTokens(
    allocator: std.mem.Allocator,
    prefix: []const i64,
    no_repeat_ngram_size: usize,
) ![]i32 {
    if (no_repeat_ngram_size <= 1 or prefix.len < no_repeat_ngram_size) {
        return allocator.alloc(i32, 0);
    }

    const context_len = no_repeat_ngram_size - 1;
    const context_start = prefix.len - context_len;
    const context = prefix[context_start..];
    const search_end = prefix.len - no_repeat_ngram_size + 1;

    var suppress_tokens = std.ArrayListUnmanaged(i32).empty;
    errdefer suppress_tokens.deinit(allocator);

    for (0..search_end) |start| {
        if (!std.mem.eql(i64, prefix[start .. start + context_len], context)) continue;
        const candidate = prefix[start + context_len];
        if (candidate < 0 or candidate > std.math.maxInt(i32)) continue;
        try appendUniqueSuppressToken(allocator, &suppress_tokens, @intCast(candidate));
    }

    return suppress_tokens.toOwnedSlice(allocator);
}

fn appendUniqueSuppressToken(
    allocator: std.mem.Allocator,
    suppress_tokens: *std.ArrayListUnmanaged(i32),
    token_id: i32,
) !void {
    for (suppress_tokens.items) |existing| {
        if (existing == token_id) return;
    }
    try suppress_tokens.append(allocator, token_id);
}

fn wouldRepeatNgram(prefix: []const i64, candidate: i64, no_repeat_ngram_size: usize) bool {
    if (no_repeat_ngram_size <= 1) return false;
    if (prefix.len < no_repeat_ngram_size) return false;

    const context_len = no_repeat_ngram_size - 1;
    const context_start = prefix.len - context_len;
    const context = prefix[context_start..];
    const search_end = prefix.len - no_repeat_ngram_size + 1;

    for (0..search_end) |start| {
        if (!std.mem.eql(i64, prefix[start .. start + context_len], context)) continue;
        if (prefix[start + context_len] == candidate) return true;
    }
    return false;
}

test "wouldRepeatNgram detects repeated trigram continuation" {
    const prefix = [_]i64{ 2, 0, 42, 77, 9, 42, 77 };
    try std.testing.expect(wouldRepeatNgram(&prefix, 9, 3));
    try std.testing.expect(!wouldRepeatNgram(&prefix, 10, 3));
}

test "selectGreedyToken skips repeated ngrams when configured" {
    const logits = [_]f32{ 0.1, 0.2, 0.3, 0.7, 0.9 };
    const prefix = [_]i64{ 2, 0, 1, 3, 4, 1, 3 };
    try std.testing.expectEqual(@as(usize, 3), selectGreedyToken(&logits, &prefix, 3));
    try std.testing.expectEqual(@as(usize, 4), selectGreedyToken(&logits, &prefix, 0));
}

test "buildNoRepeatSuppressTokens returns repeated continuations" {
    const allocator = std.testing.allocator;
    const prefix = [_]i64{ 2, 0, 42, 77, 9, 42, 77 };
    const suppress_tokens = try buildNoRepeatSuppressTokens(allocator, &prefix, 3);
    defer allocator.free(suppress_tokens);
    try std.testing.expectEqualSlices(i32, &.{9}, suppress_tokens);
}

test "buildNoRepeatSuppressTokens deduplicates continuations" {
    const allocator = std.testing.allocator;
    const prefix = [_]i64{ 2, 4, 5, 9, 4, 5, 9, 4, 5 };
    const suppress_tokens = try buildNoRepeatSuppressTokens(allocator, &prefix, 3);
    defer allocator.free(suppress_tokens);
    try std.testing.expectEqualSlices(i32, &.{9}, suppress_tokens);
}

test "Florence incremental decode falls back only for unsupported paths" {
    try std.testing.expect(shouldFallbackFlorenceIncremental(error.InvalidInputShape));
    try std.testing.expect(shouldFallbackFlorenceIncremental(error.UnsupportedOperation));
    try std.testing.expect(shouldFallbackFlorenceIncremental(error.UnsupportedShape));
    try std.testing.expect(!shouldFallbackFlorenceIncremental(error.OutOfMemory));

    last_read_telemetry = .{
        .resident_decoder = true,
        .kv_cache = true,
        .kv_cache_mode = "preallocated",
        .lm_head_path = "fused_argmax",
        .cuda_graph_replay = true,
        .cuda_graph_capture_steps = 2,
        .cuda_graph_replay_steps = 1,
    };
    defer resetLastReadTelemetry();
    markFlorenceIncrementalFallback();
    try std.testing.expect(last_read_telemetry.resident_decoder);
    try std.testing.expect(!last_read_telemetry.kv_cache);
    try std.testing.expectEqual(@as(?[]const u8, null), last_read_telemetry.kv_cache_mode);
    try std.testing.expectEqual(@as(?[]const u8, null), last_read_telemetry.lm_head_path);
    try std.testing.expect(!last_read_telemetry.cuda_graph_replay);
    try std.testing.expectEqual(@as(usize, 0), last_read_telemetry.cuda_graph_capture_steps);
    try std.testing.expectEqual(@as(usize, 0), last_read_telemetry.cuda_graph_replay_steps);
    try std.testing.expectEqualStrings("florence_incremental_decode_unsupported", last_read_telemetry.cuda_graph_fallback_reason.?);
}
