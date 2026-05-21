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

// Embedding pipeline: tokenize -> encode -> pool -> normalize.
//
// Accepts a batch of text strings and returns [batch][hidden_dim] f32 embeddings.
// Uses the unified Tokenizer interface (HuggingFace or SentencePiece) and any
// backend Session (ONNX, native, MLX).

const std = @import("std");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const linalg = @import("termite_linalg");
const tokenizer_mod = @import("termite_tokenizer");
const Tokenizer = tokenizer_mod.Tokenizer;
const EncodeResult = tokenizer_mod.EncodeResult;
const Tensor = backends.Tensor;
const session_mod = @import("../backends/session.zig");
const ops_mod = @import("../ops/ops.zig");
const image = @import("image.zig");
const audio = @import("audio.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const resident_ops = @import("../graph/resident_ops.zig");

pub const PoolingStrategy = enum {
    mean,
    cls,
    max,
    last,
};

pub const EmbeddingConfig = struct {
    normalize: bool = true,
    pooling: PoolingStrategy = .mean,
    max_length: usize = 512,
    batch_size: usize = 32,
    /// When true, resident encoder/projection paths fail instead of silently
    /// falling back to host-session projection.
    resident_projection_required: bool = false,
    /// Optional text prefix applied before tokenization. Jina v5 text models
    /// default to document embeddings by encoding "Document: " + text.
    text_prefix: []const u8 = "",
    /// Trim padded text batches to the longest active sequence in the batch
    /// before encoder execution. Useful for decoder-style embedders where
    /// padding to the architectural context window is prohibitively expensive.
    trim_padding_to_batch_max: bool = false,
    /// Enable the direct resident Qwen3/Jina embedding encoder. This is set
    /// from Jina/Qwen3 embedding manifests, not merely from the backbone family.
    resident_qwen3_embedding: bool = false,
    /// For CLIP/SigLIP multimodal models: image size for vision encoder.
    image_size: u32 = 224,
    /// For CLAP audio models: mel spectrogram configuration.
    audio_config: audio.AudioConfig = audio.CLAP_CONFIG,
};

pub const ResidentProjectionModality = enum { text, image, audio };
pub const ResidentProjectionOutcome = enum { success, fallback };

pub const ResidentProjectionStats = struct {
    text_success: u64 = 0,
    text_fallback: u64 = 0,
    image_success: u64 = 0,
    image_fallback: u64 = 0,
    audio_success: u64 = 0,
    audio_fallback: u64 = 0,

    pub fn add(self: *ResidentProjectionStats, other: ResidentProjectionStats) void {
        self.text_success += other.text_success;
        self.text_fallback += other.text_fallback;
        self.image_success += other.image_success;
        self.image_fallback += other.image_fallback;
        self.audio_success += other.audio_success;
        self.audio_fallback += other.audio_fallback;
    }
};

pub const AtomicResidentProjectionStats = struct {
    const AtomicU64 = std.atomic.Value(u64);

    text_success: AtomicU64 = .init(0),
    text_fallback: AtomicU64 = .init(0),
    image_success: AtomicU64 = .init(0),
    image_fallback: AtomicU64 = .init(0),
    audio_success: AtomicU64 = .init(0),
    audio_fallback: AtomicU64 = .init(0),

    pub fn record(
        self: *AtomicResidentProjectionStats,
        modality: ResidentProjectionModality,
        outcome: ResidentProjectionOutcome,
    ) void {
        switch (modality) {
            .text => switch (outcome) {
                .success => _ = self.text_success.fetchAdd(1, .monotonic),
                .fallback => _ = self.text_fallback.fetchAdd(1, .monotonic),
            },
            .image => switch (outcome) {
                .success => _ = self.image_success.fetchAdd(1, .monotonic),
                .fallback => _ = self.image_fallback.fetchAdd(1, .monotonic),
            },
            .audio => switch (outcome) {
                .success => _ = self.audio_success.fetchAdd(1, .monotonic),
                .fallback => _ = self.audio_fallback.fetchAdd(1, .monotonic),
            },
        }
    }

    pub fn snapshot(self: *const AtomicResidentProjectionStats) ResidentProjectionStats {
        return .{
            .text_success = self.text_success.load(.monotonic),
            .text_fallback = self.text_fallback.load(.monotonic),
            .image_success = self.image_success.load(.monotonic),
            .image_fallback = self.image_fallback.load(.monotonic),
            .audio_success = self.audio_success.load(.monotonic),
            .audio_fallback = self.audio_fallback.load(.monotonic),
        };
    }
};

pub const EncodedAudioClip = struct {
    bytes: []const u8,
    decode_options: audio.DecodeOptions = .{},
};

pub const EmbeddingPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: EmbeddingConfig,
    /// Optional vision encoder session for CLIP/SigLIP multimodal embedding.
    vision_session: ?backends.Session = null,
    /// Optional audio encoder session for CLAP multimodal embedding.
    audio_session: ?backends.Session = null,
    /// Optional projection session for text embeddings.
    text_projection: ?backends.Session = null,
    /// Optional projection session for image embeddings.
    visual_projection: ?backends.Session = null,
    /// Optional projection session (e.g. audio_projection.onnx for CLIPCLAP).
    audio_projection: ?backends.Session = null,
    /// Optional caller-owned resident path counters for benchmark/service use.
    resident_projection_stats: ?*AtomicResidentProjectionStats = null,
    /// Print phase timings for CLI/debug callers. TERMITE_EMBED_TIMING still
    /// enables the same logs for server and legacy workflows.
    print_timing: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        session: backends.Session,
        tok: Tokenizer,
        config: EmbeddingConfig,
    ) EmbeddingPipeline {
        return .{
            .allocator = allocator,
            .session = session,
            .tok = tok,
            .config = config,
        };
    }

    pub fn initMultimodal(
        allocator: std.mem.Allocator,
        text_session: backends.Session,
        vision_session: backends.Session,
        tok: Tokenizer,
        config: EmbeddingConfig,
    ) EmbeddingPipeline {
        return .{
            .allocator = allocator,
            .session = text_session,
            .tok = tok,
            .config = config,
            .vision_session = vision_session,
        };
    }

    /// Embed a batch of texts, returning [batch_size][hidden_dim] embeddings.
    /// Caller owns the returned slices and must free them with the allocator.
    pub fn embed(self: *EmbeddingPipeline, texts: []const []const u8) ![][]f32 {
        if (texts.len == 0) return try self.allocator.alloc([]f32, 0);

        const alloc = self.allocator;
        const max_len = self.config.max_length;
        const batch = texts.len;

        const encoded = try alloc.alloc(EncodeResult, batch);
        defer alloc.free(encoded);
        var encoded_count: usize = 0;
        defer {
            for (encoded[0..encoded_count]) |*result| result.deinit();
        }

        var effective_len: usize = if (self.config.trim_padding_to_batch_max) 1 else max_len;
        for (texts, 0..) |text, i| {
            const token_text = if (self.config.text_prefix.len > 0)
                try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.config.text_prefix, text })
            else
                text;
            defer if (self.config.text_prefix.len > 0) alloc.free(token_text);

            encoded[i] = try self.tok.encodeForModel(alloc, token_text, max_len);
            encoded_count += 1;
            if (self.config.trim_padding_to_batch_max) {
                effective_len = @max(effective_len, activeTokenLength(encoded[i].attention_mask));
            }
        }

        const all_ids = try alloc.alloc(i32, batch * effective_len);
        defer alloc.free(all_ids);
        const all_mask = try alloc.alloc(i32, batch * effective_len);
        defer alloc.free(all_mask);

        for (encoded[0..batch], 0..) |result, i| {
            @memcpy(all_ids[i * effective_len .. (i + 1) * effective_len], result.ids[0..effective_len]);
            @memcpy(all_mask[i * effective_len .. (i + 1) * effective_len], result.attention_mask[0..effective_len]);
        }

        // Convert i32 token IDs to i64 for ONNX Runtime (expects int64 tensors)
        const ids_i64 = try alloc.alloc(i64, batch * effective_len);
        defer alloc.free(ids_i64);
        const mask_i64 = try alloc.alloc(i64, batch * effective_len);
        defer alloc.free(mask_i64);

        for (0..batch * effective_len) |j| {
            ids_i64[j] = @intCast(all_ids[j]);
            mask_i64[j] = @intCast(all_mask[j]);
        }

        // Build input tensors
        const shape = [_]i64{ @intCast(batch), @intCast(effective_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, ids_i64);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, mask_i64);
        defer attention_mask_tensor.deinit();

        // Check if model expects token_type_ids
        var token_type_tensor: ?Tensor = null;
        defer if (token_type_tensor) |*t| t.deinit();

        const input_info = self.session.inputInfo();
        var needs_token_type = false;
        for (input_info) |info| {
            if (std.mem.eql(u8, info.name, "token_type_ids")) {
                needs_token_type = true;
                break;
            }
        }

        const inputs = if (needs_token_type) blk: {
            // Create zeros tensor for token_type_ids
            const zeros = try alloc.alloc(i64, batch * effective_len);
            defer alloc.free(zeros);
            @memset(zeros, 0);
            token_type_tensor = try Tensor.initInt64(alloc, "token_type_ids", &shape, zeros);
            break :blk &[_]Tensor{ input_ids_tensor, attention_mask_tensor, token_type_tensor.? };
        } else &[_]Tensor{ input_ids_tensor, attention_mask_tensor };

        if (self.text_projection) |proj| {
            if (try self.tryEmbedTextResidentProjection(inputs, all_mask, batch, effective_len, proj)) |resident_embeddings| {
                return resident_embeddings;
            }
        } else if (try self.tryEmbedTextResidentQwen3(inputs, all_mask, ids_i64, batch, effective_len)) |resident_embeddings| {
            return resident_embeddings;
        }

        // Run inference
        const encoder_start = embedTimingStart(self.print_timing);
        var outputs = try self.session.run(inputs, alloc);
        logEmbedTiming("text.encoder", batch, encoder_start);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        // Get the first output tensor (last_hidden_state for encoder models)
        const output = &outputs[0];
        const output_shape = output.shape;

        const embeddings = switch (output_shape.len) {
            3 => try self.pool3D(output, all_mask, batch, effective_len, self.text_projection == null),
            2 => try self.extract2D(output, batch, self.text_projection == null),
            else => return error.UnexpectedOutputShape,
        };
        errdefer freeEmbeddingSlices(alloc, embeddings);

        if (self.text_projection) |proj| {
            try self.projectEmbeddings(embeddings, proj);
        }

        return embeddings;
    }

    fn activeTokenLength(mask: []const i32) usize {
        var last_active: usize = 0;
        var found = false;
        for (mask, 0..) |value, idx| {
            if (value > 0) {
                last_active = idx;
                found = true;
            }
        }
        return if (found) last_active + 1 else 1;
    }

    /// Pool 3D output [batch, seq, hidden] -> [batch][hidden]
    fn pool3D(self: *EmbeddingPipeline, output: *const Tensor, mask: []const i32, batch: usize, seq_len: usize, normalize: bool) ![][]f32 {
        const hidden: usize = @intCast(output.shape[2]);
        const data = output.asFloat32();

        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |e| alloc.free(e);
            alloc.free(embeddings);
        }

        for (0..batch) |b| {
            const emb = try alloc.alloc(f32, hidden);
            @memset(emb, 0.0);

            switch (self.config.pooling) {
                .mean => {
                    var count: f32 = 0.0;
                    for (0..seq_len) |s| {
                        if (mask[b * seq_len + s] > 0) {
                            const offset = (b * seq_len + s) * hidden;
                            for (0..hidden) |h| {
                                emb[h] += data[offset + h];
                            }
                            count += 1.0;
                        }
                    }
                    if (count > 0.0) {
                        for (0..hidden) |h| emb[h] /= count;
                    }
                },
                .cls => {
                    const offset = b * seq_len * hidden;
                    @memcpy(emb, data[offset .. offset + hidden]);
                },
                .max => {
                    @memset(emb, -std.math.inf(f32));
                    for (0..seq_len) |s| {
                        if (mask[b * seq_len + s] > 0) {
                            const offset = (b * seq_len + s) * hidden;
                            for (0..hidden) |h| {
                                if (data[offset + h] > emb[h]) emb[h] = data[offset + h];
                            }
                        }
                    }
                },
                .last => {
                    var last_idx: usize = 0;
                    for (0..seq_len) |s| {
                        if (mask[b * seq_len + s] > 0) last_idx = s;
                    }
                    const offset = (b * seq_len + last_idx) * hidden;
                    @memcpy(emb, data[offset .. offset + hidden]);
                },
            }

            if (normalize and self.config.normalize) {
                linalg.l2Normalize(emb, hidden);
            }

            embeddings[b] = emb;
            initialized += 1;
        }

        return embeddings;
    }

    /// Extract 2D output [batch, hidden] -> [batch][hidden]
    fn extract2D(self: *EmbeddingPipeline, output: *const Tensor, batch: usize, normalize: bool) ![][]f32 {
        if (output.shape.len != 2 or output.shape[1] <= 0) return error.UnexpectedOutputShape;
        const hidden: usize = @intCast(output.shape[1]);
        const data = output.asFloat32();
        const rows = if (hidden == 0) 0 else data.len / hidden;
        if (hidden == 0 or rows < batch or rows * hidden != data.len) {
            std.log.warn("extract2D shape/data mismatch shape={any} batch={d} hidden={d} rows={d} data_len={d}", .{
                output.shape,
                batch,
                hidden,
                rows,
                data.len,
            });
            return error.ShapeMismatch;
        }

        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |e| alloc.free(e);
            alloc.free(embeddings);
        }

        for (0..batch) |b| {
            const emb = try alloc.alloc(f32, hidden);
            @memcpy(emb, data[b * hidden .. (b + 1) * hidden]);

            if (normalize and self.config.normalize) {
                linalg.l2Normalize(emb, hidden);
            }

            embeddings[b] = emb;
            initialized += 1;
        }

        return embeddings;
    }

    /// Embed a batch of images (raw JPEG/PNG bytes), returning [batch][projection_dim] embeddings.
    /// Requires a vision_session (CLIP/SigLIP model).
    pub fn embedImages(self: *EmbeddingPipeline, images: []const []const u8) anyerror![][]f32 {
        const vs = self.vision_session orelse if (sessionHasInput(self.session, "pixel_values")) self.session else return error.NoVisionSession;
        if (images.len == 0) return try self.allocator.alloc([]f32, 0);

        const alloc = self.allocator;
        const img_size = self.config.image_size;
        const batch = images.len;

        // Preprocess all images to [batch, 3, H, W]
        const preprocess_start = embedTimingStart(self.print_timing);
        const pixel_values = try image.preprocessBatch(
            alloc,
            images,
            img_size,
            image.IMAGENET_MEAN,
            image.IMAGENET_STD,
        );
        defer alloc.free(pixel_values);
        logEmbedTiming("image.preprocess", batch, preprocess_start);

        // Build input tensor
        const sz: i64 = @intCast(img_size);
        const pv_shape = [_]i64{ @intCast(batch), 3, sz, sz };
        var pv_tensor = try Tensor.initFloat32(alloc, "pixel_values", &pv_shape, pixel_values);
        defer pv_tensor.deinit();

        if (self.visual_projection) |proj| {
            const resident = self.tryEmbedResidentProjection(
                vs,
                &.{pv_tensor},
                proj,
                .image,
                batch,
                "image.encoder.resident",
                "image.projection.resident",
            ) catch |err| {
                if (self.residentProjectionRequired()) return err;
                if (batch > 1 and shouldFallbackBatchedImageError(err)) {
                    return self.embedImagesIndividually(images);
                }
                return err;
            };
            if (resident) |embeddings| return embeddings;
        }

        // Run vision encoder
        const encoder_start = embedTimingStart(self.print_timing);
        const outputs = vs.run(&.{pv_tensor}, alloc) catch |err| {
            if (batch > 1 and shouldFallbackBatchedImageError(err)) {
                return self.embedImagesIndividually(images);
            }
            return err;
        };
        logEmbedTiming("image.encoder", batch, encoder_start);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        const embeddings = self.imageEmbeddingsFromOutputs(outputs, batch) catch |err| {
            if (batch > 1 and shouldFallbackBatchedImageError(err)) {
                return self.embedImagesIndividually(images);
            }
            return err;
        };
        return embeddings;
    }

    fn imageEmbeddingsFromOutputs(self: *EmbeddingPipeline, outputs: []Tensor, batch: usize) ![][]f32 {
        const alloc = self.allocator;
        if (outputs.len == 0) return error.NoOutputTensors;
        logEmbedTensorShapes(self.print_timing, "image.outputs", outputs);

        if (self.visual_projection) |proj| {
            if (projectionInputDim(proj)) |expected_dim| {
                if (selectProjectedOutput(outputs, expected_dim, batch)) |selection| {
                    logEmbedSelectedTensor(self.print_timing, "image.project_input", selection.tensor);
                    const projected_embeddings = if (selection.use_cls_pool)
                        try self.extractVision3D(selection.tensor, batch, false)
                    else
                        try self.extract2D(selection.tensor, batch, false);
                    errdefer freeEmbeddingSlices(alloc, projected_embeddings);
                    try self.projectEmbeddings(projected_embeddings, proj);
                    return projected_embeddings;
                }
            }
        }

        if (!tensorHasBatchRows(&outputs[0], batch)) return error.ShapeMismatch;
        const extracted = switch (outputs[0].shape.len) {
            3 => try self.extractVision3D(&outputs[0], batch, self.visual_projection == null),
            2 => try self.extract2D(&outputs[0], batch, self.visual_projection == null),
            else => return error.UnexpectedOutputShape,
        };
        errdefer freeEmbeddingSlices(alloc, extracted);

        if (self.visual_projection) |proj| {
            try self.projectEmbeddings(extracted, proj);
        }

        return extracted;
    }

    fn embedImagesIndividually(self: *EmbeddingPipeline, images: []const []const u8) anyerror![][]f32 {
        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, images.len);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |emb| alloc.free(emb);
            alloc.free(embeddings);
        }
        for (images, 0..) |img, i| {
            const single = try self.embedImages(&.{img});
            defer alloc.free(single);
            embeddings[i] = single[0];
            initialized += 1;
        }
        return embeddings;
    }

    /// Embed a batch of supported encoded audio clips, returning [batch][embed_dim] embeddings.
    /// Requires an audio_session (CLAP model).
    pub fn embedAudio(self: *EmbeddingPipeline, audio_clips: []const []const u8) ![][]f32 {
        const alloc = self.allocator;
        const clips = try alloc.alloc(EncodedAudioClip, audio_clips.len);
        defer alloc.free(clips);
        for (audio_clips, 0..) |clip_bytes, i| {
            clips[i] = .{ .bytes = clip_bytes };
        }
        return self.embedEncodedAudio(clips);
    }

    /// Embed a batch of supported encoded audio clips, allowing per-clip
    /// decode hints such as MIME type when byte sniffing is ambiguous.
    pub fn embedEncodedAudio(
        self: *EmbeddingPipeline,
        audio_clips: []const EncodedAudioClip,
    ) ![][]f32 {
        if (audio_clips.len == 0) return try self.allocator.alloc([]f32, 0);

        const alloc = self.allocator;
        const decoded = try alloc.alloc(audio.Audio, audio_clips.len);
        var initialized: usize = 0;
        defer {
            for (decoded[0..initialized]) |*clip| clip.deinit();
            alloc.free(decoded);
        }

        var pcm_inputs = try alloc.alloc(audio.PcmAudio, audio_clips.len);
        defer alloc.free(pcm_inputs);

        const decode_start = embedTimingStart(self.print_timing);
        for (audio_clips, 0..) |clip, i| {
            decoded[i] = try audio.decode(alloc, clip.bytes, clip.decode_options);
            initialized += 1;
            pcm_inputs[i] = .{
                .samples = decoded[i].samples,
                .sample_rate = decoded[i].sample_rate,
            };
        }
        logEmbedTiming("audio.decode", audio_clips.len, decode_start);

        return self.embedAudioPcm(pcm_inputs);
    }

    /// Embed a batch of interleaved PCM audio clips, explicitly downmixing to
    /// mono before CLAP preprocessing.
    pub fn embedAudioInterleavedPcm(
        self: *EmbeddingPipeline,
        audio_clips: []const audio.PcmAudioInterleaved,
    ) ![][]f32 {
        if (audio_clips.len == 0) return try self.allocator.alloc([]f32, 0);

        const alloc = self.allocator;
        var mono_storage = try alloc.alloc([]f32, audio_clips.len);
        defer alloc.free(mono_storage);

        var mono_inputs = try alloc.alloc(audio.PcmAudio, audio_clips.len);
        defer alloc.free(mono_inputs);

        var initialized: usize = 0;
        errdefer {
            for (mono_storage[0..initialized]) |samples| alloc.free(samples);
        }

        for (audio_clips, 0..) |clip, i| {
            mono_storage[i] = try audio.downmixToMono(alloc, clip.samples, clip.channels);
            initialized += 1;
            mono_inputs[i] = .{
                .samples = mono_storage[i],
                .sample_rate = clip.sample_rate,
            };
        }
        defer {
            for (mono_storage[0..initialized]) |samples| alloc.free(samples);
        }

        return self.embedAudioPcm(mono_inputs);
    }

    /// Embed a batch of PCM audio clips, returning [batch][embed_dim] embeddings.
    /// Requires an audio_session (CLAP model).
    pub fn embedAudioPcm(self: *EmbeddingPipeline, audio_clips: []const audio.PcmAudio) ![][]f32 {
        const as = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        if (audio_clips.len == 0) return try self.allocator.alloc([]f32, 0);

        const alloc = self.allocator;
        const batch = audio_clips.len;
        var clap_channels: usize = 1;
        const clap_fusion_enabled = if (session_factory.getClapConfig(as)) |clap_cfg| blk: {
            clap_channels = if (clap_cfg.audio_config.enable_fusion and audio_clips.len == 1) 4 else 1;
            break :blk clap_cfg.audio_config.enable_fusion;
        } else false;
        var is_longer = try alloc.alloc(u8, batch);
        defer alloc.free(is_longer);

        // Process each audio clip to official CLAP input features and concatenate.
        const default_frames = audio.CLAP_CONFIG.chunk_length_s * audio.CLAP_CONFIG.sample_rate / audio.CLAP_CONFIG.hop_length + 1;
        const n_frames = clapInputFeatureFrames(as, default_frames);
        const n_mels = audio.CLAP_CONFIG.n_mels;
        const all_mels = try alloc.alloc(f32, batch * clap_channels * n_frames * n_mels);
        @memset(all_mels, 0.0);
        defer alloc.free(all_mels);

        const features_start = embedTimingStart(self.print_timing);
        for (0..batch) |b| {
            var features = try audio.clapFeaturesFromPcm(
                alloc,
                audio_clips[b].samples,
                audio_clips[b].sample_rate,
                clap_channels,
            );
            defer features.deinit();

            const dst = all_mels[b * clap_channels * n_frames * n_mels ..][0 .. clap_channels * n_frames * n_mels];
            const src_frames = @min(features.time_frames, n_frames);
            const src_channels = @min(features.channels, clap_channels);
            const src_plane = features.time_frames * features.mel_bins;
            const dst_plane = n_frames * n_mels;
            for (0..src_channels) |ch| {
                for (0..src_frames) |frame| {
                    const src_off = ch * src_plane + frame * features.mel_bins;
                    const dst_off = ch * dst_plane + frame * n_mels;
                    @memcpy(dst[dst_off..][0..n_mels], features.data[src_off..][0..n_mels]);
                }
            }
            is_longer[b] = if (features.is_longer) 1 else 0;
        }
        logEmbedTiming("audio.features", batch, features_start);

        if (clap_fusion_enabled) {
            var any_long = false;
            for (is_longer) |flag| {
                if (flag != 0) {
                    any_long = true;
                    break;
                }
            }
            if (!any_long and is_longer.len == 1) {
                // Some exported CLAP graphs expect the fusion branch to be
                // exercised for single short clips. Do not fake a long item for
                // multi-item batches: the fusion path contains concrete
                // singleton-batch reshapes in common ONNX exports.
                is_longer[0] = 1;
            }
        }

        // Build input tensor: [batch, channels, n_frames, n_mels]
        const mel_shape = [_]i64{ @intCast(batch), @intCast(clap_channels), @intCast(n_frames), @intCast(n_mels) };
        var mel_tensor = try Tensor.initFloat32(alloc, "input_features", &mel_shape, all_mels);
        defer mel_tensor.deinit();
        const longer_shape = [_]i64{ @intCast(batch), 1 };
        var is_longer_tensor = try Tensor.initBool(alloc, "is_longer", &longer_shape, is_longer);
        defer is_longer_tensor.deinit();

        // Run audio encoder
        const run_inputs = if (sessionHasInput(as, "is_longer"))
            &[_]Tensor{ mel_tensor, is_longer_tensor }
        else
            &[_]Tensor{mel_tensor};

        if (self.audio_projection) |proj| {
            if (try self.tryEmbedResidentProjection(
                as,
                run_inputs,
                proj,
                .audio,
                batch,
                "audio.encoder.resident",
                "audio.projection.resident",
            )) |embeddings| return embeddings;
        }

        const encoder_start = embedTimingStart(self.print_timing);
        var outputs = try as.run(run_inputs, alloc);
        logEmbedTiming("audio.encoder", batch, encoder_start);
        logEmbedTensorShapes(self.print_timing, "audio.outputs", outputs);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        if (self.audio_projection) |proj| {
            if (projectionInputDim(proj)) |expected_dim| {
                if (selectProjectedOutput(outputs, expected_dim, batch)) |selection| {
                    const projected_embeddings = if (selection.use_cls_pool)
                        try self.extractVision3D(selection.tensor, batch, false)
                    else
                        try self.extract2D(selection.tensor, batch, false);
                    errdefer freeEmbeddingSlices(alloc, projected_embeddings);
                    try self.projectEmbeddings(projected_embeddings, proj);
                    return projected_embeddings;
                }
            }
        }

        if (batch > 1 and !outputsContainBatchRows(outputs, batch)) {
            logEmbedFallback(self.print_timing, "audio.individual", batch);
            return self.embedAudioPcmIndividually(audio_clips);
        }

        const embeddings = switch (outputs[0].shape.len) {
            3 => try self.extractVision3D(&outputs[0], batch, self.audio_projection == null),
            2 => try self.extract2D(&outputs[0], batch, self.audio_projection == null),
            else => return error.UnexpectedOutputShape,
        };
        errdefer freeEmbeddingSlices(alloc, embeddings);

        // Apply audio projection if present (CLIPCLAP: CLAP→CLIP space)
        if (self.audio_projection) |proj| {
            try self.projectEmbeddings(embeddings, proj);
        }

        return embeddings;
    }

    fn embedAudioPcmIndividually(self: *EmbeddingPipeline, audio_clips: []const audio.PcmAudio) ![][]f32 {
        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, audio_clips.len);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |embedding| alloc.free(embedding);
            alloc.free(embeddings);
        }

        for (audio_clips, 0..) |_, i| {
            const one = try self.embedAudioPcm(audio_clips[i .. i + 1]);
            defer alloc.free(one);
            if (one.len != 1) return error.UnexpectedOutputShape;
            embeddings[i] = one[0];
            initialized += 1;
        }

        return embeddings;
    }

    pub fn deinit(self: *EmbeddingPipeline) void {
        self.session.close();
        if (self.vision_session) |vs| vs.close();
        if (self.audio_session) |as_| as_.close();
        if (self.text_projection) |tp| tp.close();
        if (self.visual_projection) |vp| vp.close();
        if (self.audio_projection) |ap| ap.close();
        self.tok.deinitTokenizer();
    }

    fn projectEmbeddings(self: *EmbeddingPipeline, embeddings: [][]f32, proj: backends.Session) !void {
        const alloc = self.allocator;
        if (embeddings.len == 0) return;

        const batch = embeddings.len;
        const in_dim = embeddings[0].len;
        for (embeddings) |embedding| {
            if (embedding.len != in_dim) return error.ShapeMismatch;
        }

        const packed_embeddings = try alloc.alloc(f32, batch * in_dim);
        defer alloc.free(packed_embeddings);
        for (embeddings, 0..) |embedding, b| {
            @memcpy(packed_embeddings[b * in_dim ..][0..in_dim], embedding);
        }

        const proj_shape = [_]i64{ @intCast(batch), @intCast(in_dim) };
        var proj_input = try Tensor.initFloat32(alloc, "input", &proj_shape, packed_embeddings);
        defer proj_input.deinit();

        const projection_start = embedTimingStart(self.print_timing);
        var proj_outputs = try proj.run(&.{proj_input}, alloc);
        logEmbedTiming("projection", batch, projection_start);
        defer {
            for (proj_outputs) |*o| o.deinit();
            alloc.free(proj_outputs);
        }

        if (proj_outputs.len == 0) return error.NoOutputTensors;

        const proj_data = proj_outputs[0].asFloat32();
        if (proj_outputs[0].shape.len == 0) return error.UnexpectedOutputShape;
        const proj_dim: usize = @intCast(proj_outputs[0].shape[proj_outputs[0].shape.len - 1]);
        if (proj_dim == 0 or proj_data.len != batch * proj_dim) return error.ShapeMismatch;

        for (0..batch) |b| {
            if (proj_dim != in_dim) {
                const projected_embedding = try alloc.alloc(f32, proj_dim);
                alloc.free(embeddings[b]);
                embeddings[b] = projected_embedding;
            }
            @memcpy(embeddings[b], proj_data[b * proj_dim ..][0..proj_dim]);
            if (self.config.normalize) {
                linalg.l2Normalize(embeddings[b], embeddings[b].len);
            }
        }
    }

    const ResidentPooled = struct {
        value: ops_mod.CT,
        backend: *const ops_mod.ComputeBackend,
        owns_value: bool,

        fn deinit(self: ResidentPooled) void {
            if (self.owns_value) self.backend.free(self.value);
        }
    };

    fn recordResidentProjection(
        self: *EmbeddingPipeline,
        modality: ResidentProjectionModality,
        outcome: ResidentProjectionOutcome,
        phase: []const u8,
        count: usize,
        reason: ?[]const u8,
    ) void {
        if (self.resident_projection_stats) |stats| {
            stats.record(modality, outcome);
        }

        if (outcome == .success) {
            logEmbedResidentSuccess(modality, phase, count);
        } else {
            logEmbedResidentFallback(modality, phase, count, reason orelse "unknown");
        }
    }

    fn residentProjectionFallback(
        self: *EmbeddingPipeline,
        modality: ResidentProjectionModality,
        phase: []const u8,
        count: usize,
        reason: []const u8,
    ) !?[][]f32 {
        self.recordResidentProjection(modality, .fallback, phase, count, reason);
        if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
        return null;
    }

    fn residentProjectionRequired(self: *const EmbeddingPipeline) bool {
        return self.config.resident_projection_required or embedResidentFailClosedEnabled();
    }

    fn tryEmbedTextResidentQwen3(
        self: *EmbeddingPipeline,
        inputs: []const Tensor,
        mask: []const i32,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
    ) !?[][]f32 {
        _ = inputs;
        if (residentQwen3EmbeddingDisabled()) {
            if (residentQwen3EmbeddingRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "disabled");
            }
            return null;
        }
        const cfg = session_factory.getGptConfig(self.session) orelse {
            if (residentQwen3EmbeddingRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "not_gpt_session");
            }
            return null;
        };
        if (!residentQwen3EmbeddingEligible(self.session, cfg, self.config)) {
            if (residentQwen3EmbeddingRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "ineligible");
            }
            return null;
        }

        var cb = try session_factory.getComputeBackend(self.session, self.allocator);
        defer cb.deinit();
        if (cb.kind() != .metal) {
            return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "not_metal_backend");
        }

        const prepare = try cb.decoderRuntimePrepareOrReuseFamily(
            self.allocator,
            cfg,
            0,
            cfg.num_hidden_layers,
        );
        if (!prepare.prepared) {
            return self.residentProjectionFallback(.text, "text.prepare.qwen3.resident", batch, "prepare_failed");
        }

        if (try self.tryEmbedTextResidentQwen3Graph(&cb, cfg, mask, input_ids, batch, seq_len)) |graph_embeddings| {
            return graph_embeddings;
        }
        if (residentQwen3EmbeddingGraphRequired()) {
            return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, "graph_fallback");
        }

        const overrides = decoder_gated_runtime.buildOverridesWithLevel(
            cfg,
            cfg.num_hidden_layers,
            residentQwen3EmbeddingOverrideLevel(),
        );
        const encoder_start = embedTimingStart(self.print_timing);
        const hidden = try gpt_arch.hiddenForwardResidentWithOverrides(
            &cb,
            self.allocator,
            cfg,
            input_ids,
            batch,
            seq_len,
            null,
            overrides,
        );
        logEmbedTiming("text.encoder.qwen3.resident", batch, encoder_start);

        const output_storage = try self.allocator.alloc(ops_mod.CT, 1);
        defer self.allocator.free(output_storage);
        output_storage[0] = hidden;
        var encoder_outputs = session_mod.ResidentOutputs{
            .outputs = output_storage,
            .backend = &cb,
            .allocator = self.allocator,
        };
        defer encoder_outputs.deinit();

        var pooled = self.residentPoolTextOutput(&encoder_outputs, mask, batch, seq_len) catch |err| switch (err) {
            error.UnsupportedResidentTextPooling,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedOperation,
            error.UnsupportedShape,
            => return self.residentProjectionFallback(.text, "text.pool.qwen3.resident", batch, @errorName(err)),
            else => return err,
        };
        defer pooled.deinit();

        const pooled_storage = try self.allocator.alloc(ops_mod.CT, 1);
        defer self.allocator.free(pooled_storage);
        pooled_storage[0] = pooled.value;
        var pooled_outputs = session_mod.ResidentOutputs{
            .outputs = pooled_storage,
            .backend = pooled.backend,
            .allocator = self.allocator,
        };
        const embeddings = try self.resident2DToEmbeddings(&pooled_outputs, batch);
        self.recordResidentProjection(.text, .success, "text.encoder.qwen3.resident", batch, null);
        return embeddings;
    }

    fn tryEmbedTextResidentQwen3Graph(
        self: *EmbeddingPipeline,
        cb: *const ops_mod.ComputeBackend,
        cfg: gpt_arch.Config,
        mask: []const i32,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
    ) !?[][]f32 {
        if (residentQwen3EmbeddingGraphDisabled()) {
            if (residentQwen3EmbeddingGraphRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, "disabled");
            }
            return null;
        }
        if (cfg.num_hidden_layers == 0 or cfg.num_hidden_layers > 256) {
            if (residentQwen3EmbeddingGraphRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, "unsupported_layer_count");
            }
            return null;
        }
        if (input_ids.len != batch * seq_len) return error.ShapeMismatch;

        var layer_storage: [256]ops_mod.DecoderRuntimeLayerSpec = undefined;
        const layers = decoder_gated_runtime.fillDenseQwen3LayerSpecs(
            cfg,
            cfg.num_hidden_layers,
            &layer_storage,
        ) catch |err| {
            if (residentQwen3EmbeddingGraphRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, @errorName(err));
            }
            return null;
        };

        const planned = try cb.decoderRuntimePlanPrefillFrame(&.{
            .contract = .qwen3_dense_text_embedding,
            .layer_count = layers.len,
            .rows = batch * seq_len,
            .batch = batch,
            .seq_len = seq_len,
            .hidden_size = cfg.hidden_size,
            .vocab_size = cfg.vocab_size,
            .num_attention_heads = cfg.num_attention_heads,
            .global_head_dim = cfg.global_head_dim,
            .ple_hidden_size = 0,
            .final_norm_slot = decoder_gated_runtime.finalNormSlot(cfg.num_hidden_layers),
            .final_lm_head_slot = 0,
            .include_tail = false,
            .layers = layers,
        });
        if (!planned) {
            if (residentQwen3EmbeddingGraphRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, "plan_failed");
            }
            return null;
        }

        const embed_w = try gpt_arch.getEmbeddingWeight(cb, cfg);
        defer cb.free(embed_w);
        const hidden = try cb.embeddingLookup(embed_w, input_ids, batch * seq_len, cfg.hidden_size);
        defer cb.free(hidden);

        var active = try cb.decoderRuntimeBeginFrame();
        if (!active) {
            if (residentQwen3EmbeddingGraphRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, "begin_frame_failed");
            }
            return null;
        }
        errdefer if (active) cb.decoderRuntimeCancelFrame() catch {};

        var graph_hidden: ?ops_mod.CT = null;
        const attention = ops_mod.AttentionContext{
            .mode = .dense_causal,
            .total_sequence_len = seq_len,
            .query_sequence_len = seq_len,
            .kv_sequence_len = seq_len,
        };
        const encoder_start = embedTimingStart(self.print_timing);
        const executed = try cb.decoderRuntimeExecuteGraphCommandPlanFrame(&.{
            .contract = .qwen3_dense_text_embedding,
            .layer_count = layers.len,
            .rows = batch * seq_len,
            .batch = batch,
            .seq_len = seq_len,
            .hidden_size = cfg.hidden_size,
            .vocab_size = cfg.vocab_size,
            .num_attention_heads = cfg.num_attention_heads,
            .global_head_dim = cfg.global_head_dim,
            .ple_hidden_size = 0,
            .final_norm_slot = decoder_gated_runtime.finalNormSlot(cfg.num_hidden_layers),
            .norm_eps = cfg.norm_eps,
            .rope_freq_scale = cfg.rope_freq_scale,
            .rope_consecutive_pairs = cfg.rope_layout == .consecutive_pairs,
            .activation = gpt_arch.decoderRuntimeActivationKind(cfg.activation),
            .attention = attention,
            .hidden = hidden,
            .ple_vectors = null,
            .layers = layers,
            .output_hidden = &graph_hidden,
        });
        if (!executed) {
            try cb.decoderRuntimeCancelFrame();
            active = false;
            if (residentQwen3EmbeddingGraphRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.graph", batch, "execute_failed");
            }
            return null;
        }
        try cb.decoderRuntimeSubmitAndWaitFrame();
        active = false;
        logEmbedTiming("text.encoder.qwen3.graph", batch, encoder_start);

        const output = graph_hidden orelse return error.NoOutputTensors;
        const output_storage = try self.allocator.alloc(ops_mod.CT, 1);
        defer self.allocator.free(output_storage);
        output_storage[0] = output;
        var encoder_outputs = session_mod.ResidentOutputs{
            .outputs = output_storage,
            .backend = cb,
            .allocator = self.allocator,
        };
        defer encoder_outputs.deinit();

        var pooled = self.residentPoolTextOutput(&encoder_outputs, mask, batch, seq_len) catch |err| switch (err) {
            error.UnsupportedResidentTextPooling,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedOperation,
            error.UnsupportedShape,
            => {
                if (residentQwen3EmbeddingGraphRequired()) {
                    return self.residentProjectionFallback(.text, "text.pool.qwen3.graph", batch, @errorName(err));
                }
                return null;
            },
            else => return err,
        };
        defer pooled.deinit();

        const pooled_storage = try self.allocator.alloc(ops_mod.CT, 1);
        defer self.allocator.free(pooled_storage);
        pooled_storage[0] = pooled.value;
        var pooled_outputs = session_mod.ResidentOutputs{
            .outputs = pooled_storage,
            .backend = pooled.backend,
            .allocator = self.allocator,
        };
        const embeddings = try self.resident2DToEmbeddings(&pooled_outputs, batch);
        self.recordResidentProjection(.text, .success, "text.encoder.qwen3.graph", batch, null);
        return embeddings;
    }

    fn tryEmbedTextResidentProjection(
        self: *EmbeddingPipeline,
        inputs: []const Tensor,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
        proj: backends.Session,
    ) !?[][]f32 {
        const encoder_start = embedTimingStart(self.print_timing);
        var encoder_outputs = (try self.session.runResident(inputs, self.allocator)) orelse
            return self.residentProjectionFallback(.text, "text.encoder.resident", batch, "unsupported");
        logEmbedTiming("text.encoder.resident", batch, encoder_start);
        defer encoder_outputs.deinit();
        if (encoder_outputs.outputs.len == 0) return error.NoOutputTensors;

        var pooled = self.residentPoolTextOutput(&encoder_outputs, mask, batch, seq_len) catch |err| switch (err) {
            error.UnsupportedResidentTextPooling,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedOperation,
            error.UnsupportedShape,
            => return self.residentProjectionFallback(.text, "text.pool.resident", batch, @errorName(err)),
            else => return err,
        };
        defer pooled.deinit();

        const resident_input = [_]session_mod.ResidentInput{.{
            .value = pooled.value,
            .backend = pooled.backend,
        }};
        const projection_start = embedTimingStart(self.print_timing);
        var proj_outputs = (proj.runResidentInputs(&resident_input, self.allocator) catch |err| switch (err) {
            error.UnsupportedResidentInputBackend => return self.residentProjectionFallback(.text, "text.projection.resident", batch, @errorName(err)),
            else => return err,
        }) orelse return self.residentProjectionFallback(.text, "text.projection.resident", batch, "unsupported");
        logEmbedTiming("text.projection.resident", batch, projection_start);
        defer proj_outputs.deinit();

        const embeddings = try self.resident2DToEmbeddings(&proj_outputs, batch);
        self.recordResidentProjection(.text, .success, "text.projection.resident", batch, null);
        return embeddings;
    }

    fn tryEmbedResidentProjection(
        self: *EmbeddingPipeline,
        encoder: backends.Session,
        inputs: []const Tensor,
        proj: backends.Session,
        modality: ResidentProjectionModality,
        batch: usize,
        encoder_phase: []const u8,
        projection_phase: []const u8,
    ) !?[][]f32 {
        const expected_dim = projectionInputDim(proj) orelse
            return self.residentProjectionFallback(modality, projection_phase, batch, "unknown_projection_input_dim");

        const encoder_start = embedTimingStart(self.print_timing);
        var encoder_outputs = (try encoder.runResident(inputs, self.allocator)) orelse
            return self.residentProjectionFallback(modality, encoder_phase, batch, "unsupported");
        logEmbedTiming(encoder_phase, batch, encoder_start);
        defer encoder_outputs.deinit();
        if (encoder_outputs.outputs.len == 0) return error.NoOutputTensors;

        var selected = self.selectResidentProjectedInput(&encoder_outputs, expected_dim, batch) catch |err| switch (err) {
            error.UnsupportedResidentProjectionInput,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedOperation,
            error.UnsupportedShape,
            => return self.residentProjectionFallback(modality, encoder_phase, batch, @errorName(err)),
            else => return err,
        };
        defer selected.deinit();

        const resident_input = [_]session_mod.ResidentInput{.{
            .value = selected.value,
            .backend = selected.backend,
        }};
        const projection_start = embedTimingStart(self.print_timing);
        var proj_outputs = (proj.runResidentInputs(&resident_input, self.allocator) catch |err| switch (err) {
            error.UnsupportedResidentInputBackend => return self.residentProjectionFallback(modality, projection_phase, batch, @errorName(err)),
            else => return err,
        }) orelse return self.residentProjectionFallback(modality, projection_phase, batch, "unsupported");
        logEmbedTiming(projection_phase, batch, projection_start);
        defer proj_outputs.deinit();

        const embeddings = try self.resident2DToEmbeddings(&proj_outputs, batch);
        self.recordResidentProjection(modality, .success, projection_phase, batch, null);
        return embeddings;
    }

    fn selectResidentProjectedInput(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        expected_dim: usize,
        expected_batch: usize,
    ) !ResidentPooled {
        _ = self;
        for (outputs.outputs) |output| {
            const shape = try outputs.backend.tensorShape(output, outputs.allocator);
            defer outputs.allocator.free(shape);
            if (shape.len == 2 and
                shape[0] == @as(i64, @intCast(expected_batch)) and
                shape[1] == @as(i64, @intCast(expected_dim)))
            {
                return .{ .value = output, .backend = outputs.backend, .owns_value = false };
            }
        }
        for (outputs.outputs) |output| {
            const shape = try outputs.backend.tensorShape(output, outputs.allocator);
            defer outputs.allocator.free(shape);
            if (shape.len == 3 and
                shape[0] == @as(i64, @intCast(expected_batch)) and
                shape[2] == @as(i64, @intCast(expected_dim)))
            {
                return try residentClsPool(outputs.backend, output, expected_batch, expected_dim);
            }
        }
        return error.UnsupportedResidentProjectionInput;
    }

    fn residentPoolTextOutput(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
    ) !ResidentPooled {
        const output = outputs.outputs[0];
        const backend = outputs.backend;
        const shape = try backend.tensorShape(output, outputs.allocator);
        defer outputs.allocator.free(shape);

        if (shape.len == 2) {
            if (shape[0] == @as(i64, @intCast(batch)) and shape[1] > 0) {
                return .{ .value = output, .backend = backend, .owns_value = false };
            }
            if (shape[0] == @as(i64, @intCast(batch * seq_len)) and shape[1] > 0) {
                const hidden: usize = @intCast(shape[1]);
                const output_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(hidden) };
                const reshaped = try backend.primReshape(output, &output_shape);
                defer backend.free(reshaped);
                return switch (self.config.pooling) {
                    .mean => try residentMaskedMeanPool(outputs.allocator, backend, reshaped, mask, batch, seq_len, hidden),
                    .cls => try residentClsPool(backend, reshaped, batch, hidden),
                    .last => try residentLastTokenPool(outputs.allocator, backend, reshaped, mask, batch, seq_len, hidden),
                    .max => error.UnsupportedResidentTextPooling,
                };
            }
            return error.ShapeMismatch;
        }
        if (shape.len != 3) return error.UnexpectedOutputShape;
        if (shape[0] != @as(i64, @intCast(batch)) or shape[1] != @as(i64, @intCast(seq_len)) or shape[2] <= 0) {
            return error.ShapeMismatch;
        }

        return switch (self.config.pooling) {
            .mean => try residentMaskedMeanPool(outputs.allocator, backend, output, mask, batch, seq_len, @intCast(shape[2])),
            .cls => try residentClsPool(backend, output, batch, @intCast(shape[2])),
            .last => try residentLastTokenPool(outputs.allocator, backend, output, mask, batch, seq_len, @intCast(shape[2])),
            .max => error.UnsupportedResidentTextPooling,
        };
    }

    fn residentMaskedMeanPool(
        allocator: std.mem.Allocator,
        backend: *const ops_mod.ComputeBackend,
        output: ops_mod.CT,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
        hidden: usize,
    ) !ResidentPooled {
        if (mask.len != batch * seq_len) return error.ShapeMismatch;

        const mask_values = try allocator.alloc(f32, batch * seq_len);
        defer allocator.free(mask_values);
        for (mask, 0..) |flag, i| mask_values[i] = if (flag > 0) 1.0 else 0.0;

        const mask_shape_i32 = [_]i32{ @intCast(batch), @intCast(seq_len), 1 };
        const mask_shape = [_]i64{ @intCast(batch), @intCast(seq_len), 1 };
        const output_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(hidden) };
        const sum_shape = [_]i64{ @intCast(batch), 1, @intCast(hidden) };
        const count_shape = [_]i64{ @intCast(batch), 1, 1 };
        const pooled_shape = [_]i64{ @intCast(batch), @intCast(hidden) };

        const mask_ct = try backend.fromFloat32Shape(mask_values, &mask_shape_i32);
        defer backend.free(mask_ct);
        const mask_broadcast = try backend.primBroadcastInDim(mask_ct, &output_shape, &.{ 0, 1, 2 }, &mask_shape);
        defer backend.free(mask_broadcast);
        const masked_output = try backend.multiply(output, mask_broadcast);
        defer backend.free(masked_output);
        const summed = try backend.primReduceSum(masked_output, &.{1}, &output_shape);
        defer backend.free(summed);
        const counts = try backend.primReduceSum(mask_ct, &.{1}, &mask_shape);
        defer backend.free(counts);
        const count_broadcast = try backend.primBroadcastInDim(counts, &sum_shape, &.{ 0, 1, 2 }, &count_shape);
        defer backend.free(count_broadcast);
        const mean3d = try backend.primDivide(summed, count_broadcast);
        defer backend.free(mean3d);
        const mean2d = try backend.primReshape(mean3d, &pooled_shape);

        return .{ .value = mean2d, .backend = backend, .owns_value = true };
    }

    fn residentClsPool(
        backend: *const ops_mod.ComputeBackend,
        output: ops_mod.CT,
        batch: usize,
        hidden: usize,
    ) !ResidentPooled {
        const input_shape = [_]i64{ @intCast(batch), -1, @intCast(hidden) };
        const starts = [_]i64{ 0, 0, 0 };
        const limits = [_]i64{ @intCast(batch), 1, @intCast(hidden) };
        const strides = [_]i64{ 1, 1, 1 };
        const pooled_shape = [_]i64{ @intCast(batch), @intCast(hidden) };
        const sliced = try backend.primSlice(output, &starts, &limits, &strides, &input_shape);
        defer backend.free(sliced);
        const pooled = try backend.primReshape(sliced, &pooled_shape);
        return .{ .value = pooled, .backend = backend, .owns_value = true };
    }

    fn residentLastTokenPool(
        allocator: std.mem.Allocator,
        backend: *const ops_mod.ComputeBackend,
        output: ops_mod.CT,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
        hidden: usize,
    ) !ResidentPooled {
        if (mask.len != batch * seq_len) return error.ShapeMismatch;

        const row_ids = try allocator.alloc(u32, batch);
        defer allocator.free(row_ids);
        for (0..batch) |b| {
            var last_idx: usize = 0;
            for (0..seq_len) |s| {
                if (mask[b * seq_len + s] > 0) last_idx = s;
            }
            row_ids[b] = @intCast(b * seq_len + last_idx);
        }

        const flat_shape = [_]i64{ @intCast(batch * seq_len), @intCast(hidden) };
        const flat = backend.primReshape(output, &flat_shape) catch null;
        if (flat) |flat_output| {
            defer backend.free(flat_output);
            if (try backend.takeRows(flat_output, row_ids, batch, hidden)) |pooled| {
                return .{ .value = pooled, .backend = backend, .owns_value = true };
            }
        }

        const host = try backend.toFloat32(output, allocator);
        defer allocator.free(host);
        if (host.len != batch * seq_len * hidden) return error.ShapeMismatch;

        const pooled_values = try allocator.alloc(f32, batch * hidden);
        errdefer allocator.free(pooled_values);
        for (0..batch) |b| {
            const src_offset = @as(usize, row_ids[b]) * hidden;
            const dst_offset = b * hidden;
            @memcpy(pooled_values[dst_offset .. dst_offset + hidden], host[src_offset .. src_offset + hidden]);
        }

        const pooled = try backend.fromFloat32Shape(pooled_values, &.{ @intCast(batch), @intCast(hidden) });
        allocator.free(pooled_values);
        return .{ .value = pooled, .backend = backend, .owns_value = true };
    }

    fn resident2DToEmbeddings(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        batch: usize,
    ) ![][]f32 {
        if (outputs.outputs.len == 0) return error.NoOutputTensors;
        const shape = try outputs.backend.tensorShape(outputs.outputs[0], self.allocator);
        defer self.allocator.free(shape);
        if (shape.len == 0) return error.UnexpectedOutputShape;

        const proj_dim: usize = @intCast(shape[shape.len - 1]);
        if (proj_dim == 0) return error.ShapeMismatch;
        const resident_output = if (self.config.normalize)
            try resident_ops.l2NormalizeLastDim(self.allocator, outputs.backend, outputs.outputs[0], shape)
        else
            outputs.outputs[0];
        defer if (self.config.normalize) outputs.backend.free(resident_output);

        const data = try outputs.backend.toFloat32(resident_output, self.allocator);
        defer self.allocator.free(data);
        if (data.len != batch * proj_dim) return error.ShapeMismatch;

        const embeddings = try self.allocator.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |embedding| self.allocator.free(embedding);
            self.allocator.free(embeddings);
        }

        for (0..batch) |b| {
            const embedding = try self.allocator.alloc(f32, proj_dim);
            @memcpy(embedding, data[b * proj_dim ..][0..proj_dim]);
            embeddings[b] = embedding;
            initialized += 1;
        }

        return embeddings;
    }

    fn extractVision3D(self: *EmbeddingPipeline, output: *const Tensor, batch: usize, normalize: bool) ![][]f32 {
        const hidden: usize = @intCast(output.shape[2]);
        const seq_len: usize = @intCast(output.shape[1]);
        const data = output.asFloat32();

        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |e| alloc.free(e);
            alloc.free(embeddings);
        }

        for (0..batch) |b| {
            const emb = try alloc.alloc(f32, hidden);
            const offset = b * seq_len * hidden;
            @memcpy(emb, data[offset .. offset + hidden]);
            if (normalize and self.config.normalize) {
                linalg.l2Normalize(emb, hidden);
            }
            embeddings[b] = emb;
            initialized += 1;
        }

        return embeddings;
    }
};

fn sessionHasInput(session: backends.Session, name: []const u8) bool {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return true;
    }
    return false;
}

fn shouldFallbackBatchedImageError(err: anyerror) bool {
    return switch (err) {
        error.ShapeMismatch,
        error.UnexpectedOutputShape,
        error.UnsupportedShape,
        error.InvalidInputShape,
        error.InvalidTensorShape,
        => true,
        else => false,
    };
}

fn tensorHasBatchRows(tensor: *const Tensor, expected_batch: usize) bool {
    if (tensor.shape.len < 2 or tensor.shape[0] <= 0) return true;
    return @as(usize, @intCast(tensor.shape[0])) >= expected_batch;
}

fn outputsContainBatchRows(outputs: []const Tensor, expected_batch: usize) bool {
    for (outputs) |*output| {
        if (output.shape.len >= 2 and tensorHasBatchRows(output, expected_batch)) return true;
    }
    return false;
}

fn embedTimingEnabled(explicit: bool) bool {
    if (explicit) return true;
    return platform.env.getenvBoolDefault("TERMITE_EMBED_TIMING", false);
}

fn embedTimingNowNs() u128 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => 0,
    };
}

fn embedTimingStart(explicit: bool) u128 {
    if (!embedTimingEnabled(explicit)) return 0;
    return embedTimingNowNs();
}

fn logEmbedTiming(phase: []const u8, count: usize, start_ns: u128) void {
    if (start_ns == 0) return;
    const now = embedTimingNowNs();
    const elapsed_us = if (now > start_ns) @divTrunc(now - start_ns, 1000) else 0;
    std.log.info("termite embed timing phase={s} count={d} elapsed_us={d}", .{ phase, count, elapsed_us });
}

fn logEmbedFallback(explicit: bool, phase: []const u8, count: usize) void {
    if (!embedTimingEnabled(explicit)) return;
    std.log.info("termite embed fallback phase={s} count={d}", .{ phase, count });
}

fn logEmbedResidentSuccess(modality: ResidentProjectionModality, phase: []const u8, count: usize) void {
    if (!embedTimingEnabled(false) and !embedResidentFailClosedEnabled()) return;
    std.log.info("termite embed resident outcome=success modality={s} phase={s} count={d}", .{
        @tagName(modality),
        phase,
        count,
    });
}

fn logEmbedResidentFallback(modality: ResidentProjectionModality, phase: []const u8, count: usize, reason: []const u8) void {
    if (!embedTimingEnabled(false) and !embedResidentFailClosedEnabled()) return;
    std.log.warn("termite embed resident outcome=fallback modality={s} phase={s} count={d} reason={s}", .{
        @tagName(modality),
        phase,
        count,
        reason,
    });
}

fn embedResidentFailClosedEnabled() bool {
    if (platform.env.getenvSlice("TERMITE_EMBED_RESIDENT_FAIL_CLOSED")) |value| {
        if (envFlagEnabled(value)) return true;
    }
    if (platform.env.getenvSlice("TERMITE_EMBED_RESIDENT_REQUIRED")) |value| {
        if (envFlagEnabled(value)) return true;
    }
    if (platform.env.getenvSlice("TERMITE_REQUIRE_RESIDENT_QWEN3_EMBED")) |value| {
        if (envFlagEnabled(value)) return true;
    }
    if (platform.env.getenvSlice("TERMITE_REQUIRE_QWEN3_EMBED_GRAPH")) |value| {
        if (envFlagEnabled(value)) return true;
    }
    return false;
}

fn residentQwen3EmbeddingDisabled() bool {
    if (platform.env.getenvSlice("TERMITE_DISABLE_RESIDENT_QWEN3_EMBED")) |value| {
        return envFlagEnabled(value);
    }
    return false;
}

fn residentQwen3EmbeddingRequired() bool {
    if (platform.env.getenvSlice("TERMITE_REQUIRE_RESIDENT_QWEN3_EMBED")) |value| {
        return envFlagEnabled(value);
    }
    return false;
}

fn residentQwen3EmbeddingGraphDisabled() bool {
    if (platform.env.getenvSlice("TERMITE_DISABLE_QWEN3_EMBED_GRAPH")) |value| {
        return envFlagEnabled(value);
    }
    return false;
}

fn residentQwen3EmbeddingGraphRequired() bool {
    if (platform.env.getenvSlice("TERMITE_REQUIRE_QWEN3_EMBED_GRAPH")) |value| {
        return envFlagEnabled(value);
    }
    return false;
}

fn residentQwen3EmbeddingOverrideLevel() usize {
    return 4;
}

fn residentQwen3EmbeddingEligible(session: backends.Session, cfg: gpt_arch.Config, embedding_config: EmbeddingConfig) bool {
    return residentQwen3EmbeddingEligibleForBackend(session.backend(), cfg, embedding_config);
}

fn residentQwen3EmbeddingEligibleForBackend(backend: backends.BackendType, cfg: gpt_arch.Config, embedding_config: EmbeddingConfig) bool {
    if (!embedding_config.resident_qwen3_embedding) return false;
    if (backend != .metal) return false;
    if (cfg.family != .qwen3) return false;
    if (cfg.usesMoe() or cfg.hasPle() or cfg.isMultimodal()) return false;
    if (cfg.num_kv_shared_layers != 0) return false;
    if (cfg.global_head_dim != 0 or cfg.num_global_key_value_heads != 0) return false;
    if (cfg.sliding_window != 0) return false;
    return true;
}

fn envFlagEnabled(value: []const u8) bool {
    return value.len > 0 and
        !std.mem.eql(u8, value, "0") and
        !std.ascii.eqlIgnoreCase(value, "false") and
        !std.ascii.eqlIgnoreCase(value, "off") and
        !std.ascii.eqlIgnoreCase(value, "no");
}

fn logEmbedTensorShapes(explicit: bool, label: []const u8, tensors: []const Tensor) void {
    if (!embedTimingEnabled(explicit)) return;
    for (tensors, 0..) |tensor, i| {
        std.log.info("termite embed tensor label={s} index={d} name={s} shape={any} data_len={d}", .{
            label,
            i,
            tensor.name,
            tensor.shape,
            tensor.asFloat32().len,
        });
    }
}

fn logEmbedSelectedTensor(explicit: bool, label: []const u8, tensor: *const Tensor) void {
    if (!embedTimingEnabled(explicit)) return;
    std.log.info("termite embed selected label={s} name={s} shape={any} data_len={d}", .{
        label,
        tensor.name,
        tensor.shape,
        tensor.asFloat32().len,
    });
}

fn clapInputFeatureFrames(session: backends.Session, default_frames: usize) usize {
    for (session.inputInfo()) |info| {
        if (!std.mem.eql(u8, info.name, "input_features")) continue;
        if (info.shape.len >= 3 and info.shape[2] > 0) {
            return @max(default_frames, @as(usize, @intCast(info.shape[2])));
        }
    }

    for (session.outputInfo()) |info| {
        if (info.shape.len < 2 or info.shape[1] <= 0) continue;
        const dim = @as(usize, @intCast(info.shape[1]));
        if (dim > default_frames and dim <= 4096 and dim % 32 == 0) {
            return dim;
        }
    }

    return default_frames;
}

fn freeEmbeddingSlices(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |emb| allocator.free(emb);
    allocator.free(embeddings);
}

const ProjectionSelection = struct {
    tensor: *const Tensor,
    use_cls_pool: bool,
};

fn projectionInputDim(session: backends.Session) ?usize {
    const inputs = session.inputInfo();
    if (inputs.len == 0) return null;
    const shape = inputs[0].shape;
    if (shape.len == 0) return null;
    const dim = shape[shape.len - 1];
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn selectProjectedOutput(outputs: []Tensor, expected_dim: usize, expected_batch: usize) ?ProjectionSelection {
    for (outputs) |*output| {
        if (output.shape.len == 2 and output.shape[1] > 0 and @as(usize, @intCast(output.shape[1])) == expected_dim and tensorHasBatchRows(output, expected_batch)) {
            return .{ .tensor = output, .use_cls_pool = false };
        }
    }
    for (outputs) |*output| {
        if (output.shape.len == 3 and output.shape[2] > 0 and @as(usize, @intCast(output.shape[2])) == expected_dim and tensorHasBatchRows(output, expected_batch)) {
            return .{ .tensor = output, .use_cls_pool = true };
        }
    }
    return null;
}

test "projectEmbeddings runs projection as a single batch" {
    const allocator = std.testing.allocator;

    const first = try allocator.dupe(f32, &.{ 1.0, 2.0 });
    errdefer allocator.free(first);
    const second = try allocator.dupe(f32, &.{ 3.0, 4.0 });
    errdefer allocator.free(second);
    const third = try allocator.dupe(f32, &.{ 5.0, 6.0 });
    errdefer allocator.free(third);

    var embeddings = try allocator.alloc([]f32, 3);
    defer allocator.free(embeddings);
    embeddings[0] = first;
    embeddings[1] = second;
    embeddings[2] = third;
    defer {
        for (embeddings) |embedding| allocator.free(embedding);
    }

    var fake = FakeProjectionSession{};
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .normalize = false },
    };

    try pipeline.projectEmbeddings(embeddings, fake.session());

    try std.testing.expectEqual(@as(usize, 1), fake.run_count);
    try std.testing.expectEqual(@as(usize, 3), fake.last_batch);
    try std.testing.expectEqual(@as(usize, 2), fake.last_dim);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0, 7.0 }, embeddings[1]);
    try std.testing.expectEqualSlices(f32, &.{ 5.0, 6.0, 11.0 }, embeddings[2]);
}

test "resident fail closed env flag parser accepts explicit opt in only" {
    try std.testing.expect(envFlagEnabled("1"));
    try std.testing.expect(envFlagEnabled("true"));
    try std.testing.expect(envFlagEnabled("yes"));
    try std.testing.expect(!envFlagEnabled(""));
    try std.testing.expect(!envFlagEnabled("0"));
    try std.testing.expect(!envFlagEnabled("false"));
    try std.testing.expect(!envFlagEnabled("off"));
    try std.testing.expect(!envFlagEnabled("no"));
}

test "resident projection stats records success fallback and fail closed" {
    var stats = AtomicResidentProjectionStats{};
    var pipeline = EmbeddingPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .resident_projection_required = true },
        .resident_projection_stats = &stats,
    };

    pipeline.recordResidentProjection(.text, .success, "text.projection.resident", 2, null);
    var snapshot = stats.snapshot();
    try std.testing.expectEqual(@as(u64, 1), snapshot.text_success);
    try std.testing.expectEqual(@as(u64, 0), snapshot.text_fallback);

    try std.testing.expectError(
        error.ResidentEmbeddingFallback,
        pipeline.residentProjectionFallback(.image, "image.encoder.resident", 2, "unsupported"),
    );
    snapshot = stats.snapshot();
    try std.testing.expectEqual(@as(u64, 1), snapshot.image_fallback);
}

test "resident masked mean pooling uses backend primitives" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer {
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
        native_mod.deinitPrefetchQueue(&weight_store);
    }
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const values = [_]f32{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    };
    const input = try cb.fromFloat32Shape(&values, &.{ 2, 3, 2 });
    defer cb.free(input);

    const mask = [_]i32{ 1, 1, 0, 1, 0, 0 };
    const pooled = try EmbeddingPipeline.residentMaskedMeanPool(allocator, &cb, input, &mask, 2, 3, 2);
    defer pooled.deinit();

    const shape = try cb.tensorShape(pooled.value, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);

    const host = try cb.toFloat32(pooled.value, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 2.0, 3.0, 7.0, 8.0 }, host);
}

test "resident text pooling handles flattened batch sequence hidden states" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer {
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
        native_mod.deinitPrefetchQueue(&weight_store);
    }
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const values = [_]f32{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    };
    const output_ct = try cb.fromFloat32Shape(&values, &.{ 6, 2 });
    const output_storage = try allocator.alloc(ops_mod.CT, 1);
    output_storage[0] = output_ct;
    var resident_outputs = session_mod.ResidentOutputs{
        .outputs = output_storage,
        .backend = &cb,
        .allocator = allocator,
    };
    defer resident_outputs.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .pooling = .last, .normalize = false },
    };

    const mask = [_]i32{ 1, 1, 0, 1, 0, 0 };
    const pooled = try pipeline.residentPoolTextOutput(&resident_outputs, &mask, 2, 3);
    defer pooled.deinit();

    const shape = try cb.tensorShape(pooled.value, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);

    const host = try cb.toFloat32(pooled.value, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0, 7.0, 8.0 }, host);
}

test "pool3D last uses final non-padding token and normalizes" {
    const allocator = std.testing.allocator;

    const shape = [_]i64{ 2, 3, 2 };
    const values = [_]f32{
        1.0, 0.0,
        3.0, 4.0,
        9.0, 9.0,
        0.0, 2.0,
        8.0, 8.0,
        0.0, 5.0,
    };
    var output = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, &values);
    defer output.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .pooling = .last, .normalize = true },
    };

    const mask = [_]i32{ 1, 1, 0, 1, 0, 1 };
    const embeddings = try pipeline.pool3D(&output, &mask, 2, 3, true);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectApproxEqAbs(@as(f32, 0.6), embeddings[0][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), embeddings[0][1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), embeddings[1][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), embeddings[1][1], 1e-6);
}

test "active token length trims trailing padding but keeps at least one token" {
    try std.testing.expectEqual(@as(usize, 3), EmbeddingPipeline.activeTokenLength(&.{ 1, 1, 1, 0, 0 }));
    try std.testing.expectEqual(@as(usize, 4), EmbeddingPipeline.activeTokenLength(&.{ 1, 0, 0, 1, 0 }));
    try std.testing.expectEqual(@as(usize, 1), EmbeddingPipeline.activeTokenLength(&.{ 0, 0, 0 }));
}

test "resident qwen3 embedding eligibility accepts dense metal qwen3 only" {
    var cfg = gpt_arch.Config{
        .family = .qwen3,
        .hidden_size = 1024,
        .num_hidden_layers = 28,
        .num_attention_heads = 16,
        .num_key_value_heads = 8,
        .intermediate_size = 3072,
    };

    const embedding_cfg = EmbeddingConfig{
        .pooling = .last,
        .text_prefix = "Document: ",
        .trim_padding_to_batch_max = true,
        .resident_qwen3_embedding = true,
    };

    try std.testing.expect(residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, .{}));
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.native, cfg, embedding_cfg));

    cfg.family = .qwen3_5;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.family = .qwen3;

    cfg.num_local_experts = 4;
    cfg.num_experts_per_tok = 2;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.num_local_experts = 0;
    cfg.num_experts_per_tok = 0;

    cfg.image_token_index = 151655;
    cfg.mm_tokens_per_image = 256;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.image_token_index = -1;
    cfg.mm_tokens_per_image = 0;

    cfg.ple_hidden_size = 1024;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.ple_hidden_size = 0;

    cfg.sliding_window = 4096;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
}

test "resident projected input selection supports 3d cls pooling" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer {
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
        native_mod.deinitPrefetchQueue(&weight_store);
    }
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const values = [_]f32{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    };
    const output_ct = try cb.fromFloat32Shape(&values, &.{ 2, 3, 2 });
    const output_storage = try allocator.alloc(ops_mod.CT, 1);
    output_storage[0] = output_ct;
    var resident_outputs = session_mod.ResidentOutputs{
        .outputs = output_storage,
        .backend = &cb,
        .allocator = allocator,
    };
    defer resident_outputs.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .normalize = false },
    };

    const selected = try pipeline.selectResidentProjectedInput(&resident_outputs, 2, 2);
    defer selected.deinit();

    const shape = try cb.tensorShape(selected.value, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);

    const host = try cb.toFloat32(selected.value, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 7.0, 8.0 }, host);
}

test "resident 2d embedding extraction normalizes before host readback" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer {
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
        native_mod.deinitPrefetchQueue(&weight_store);
    }
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const values = [_]f32{ 3.0, 4.0, 0.0, 0.0 };
    const output_ct = try cb.fromFloat32Shape(&values, &.{ 2, 2 });
    const output_storage = try allocator.alloc(ops_mod.CT, 1);
    output_storage[0] = output_ct;
    var resident_outputs = session_mod.ResidentOutputs{
        .outputs = output_storage,
        .backend = &cb,
        .allocator = allocator,
    };
    defer resident_outputs.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .normalize = true },
    };

    const embeddings = try pipeline.resident2DToEmbeddings(&resident_outputs, 2);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectApproxEqAbs(@as(f32, 0.6), embeddings[0][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), embeddings[0][1], 1e-6);
    try std.testing.expectEqualSlices(f32, &.{ 0.0, 0.0 }, embeddings[1]);
}

test "embedImages uses one vision session run for an image batch" {
    const allocator = std.testing.allocator;

    var fake = FakeVisionBatchSession{};
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = fake.session(),
        .tok = undefined,
        .config = .{ .normalize = false, .image_size = 2 },
        .vision_session = fake.session(),
    };

    const images = [_][]const u8{ red_png_2x2[0..], red_png_2x2[0..] };
    const embeddings = try pipeline.embedImages(&images);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectEqual(@as(usize, 1), fake.run_count);
    try std.testing.expectEqual(@as(usize, 2), fake.last_batch);
    try std.testing.expectEqual(@as(usize, 2), embeddings.len);
    try std.testing.expectEqualSlices(f32, &.{ 0.0, 1.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[1]);
}

test "embedImages falls back to per-image runs when batched image shape collapses" {
    const allocator = std.testing.allocator;

    var fake = FakeCollapsingVisionSession{};
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = fake.session(),
        .tok = undefined,
        .config = .{ .normalize = false, .image_size = 2 },
        .vision_session = fake.session(),
    };

    const images = [_][]const u8{ red_png_2x2[0..], red_png_2x2[0..] };
    const embeddings = try pipeline.embedImages(&images);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectEqual(@as(usize, 3), fake.run_count);
    try std.testing.expectEqual(@as(usize, 2), fake.collapsed_batch_attempts);
    try std.testing.expectEqual(@as(usize, 2), embeddings.len);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[1]);
}

test "selectProjectedOutput skips collapsed pooled output for image batch" {
    const allocator = std.testing.allocator;

    var outputs = try allocator.alloc(Tensor, 2);
    defer allocator.free(outputs);
    outputs[0] = try Tensor.initFloat32(allocator, "pooled", &.{ 1, 2 }, &.{ 9.0, 9.0 });
    defer outputs[0].deinit();
    outputs[1] = try Tensor.initFloat32(allocator, "hidden", &.{ 2, 3, 2 }, &.{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    });
    defer outputs[1].deinit();

    const selection = selectProjectedOutput(outputs, 2, 2) orelse return error.TestUnexpectedResult;
    try std.testing.expect(selection.use_cls_pool);
    try std.testing.expectEqualStrings("hidden", selection.tensor.name);
}

const red_png_2x2 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02,
    0x08, 0x02, 0x00, 0x00, 0x00, 0xfd, 0xd4, 0x9a, 0x73, 0x00, 0x00, 0x00,
    0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x4f, 0x25, 0xc4, 0xd6, 0x00, 0x00, 0x00, 0x10, 0x49, 0x44,
    0x41, 0x54, 0x78, 0x9c, 0x63, 0xfc, 0xc3, 0x00, 0x02, 0x2c, 0x60, 0x92,
    0x01, 0x00, 0x0d, 0x04, 0x01, 0x02, 0xbf, 0x50, 0x15, 0xb3, 0x00, 0x00,
    0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
};

const FakeVisionBatchSession = struct {
    run_count: usize = 0,
    last_batch: usize = 0,

    fn session(self: *FakeVisionBatchSession) backends.Session {
        return fakeVisionSession(self, run);
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeVisionBatchSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        const batch: usize = @intCast(inputs[0].shape[0]);
        self.run_count += 1;
        self.last_batch = batch;

        const data = try allocator.alloc(f32, batch * 2);
        defer allocator.free(data);
        for (0..batch) |b| {
            data[b * 2 + 0] = @floatFromInt(b);
            data[b * 2 + 1] = @floatFromInt(b + 1);
        }

        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "image_embeds", &.{ @intCast(batch), 2 }, data);
        return out;
    }
};

const FakeCollapsingVisionSession = struct {
    run_count: usize = 0,
    collapsed_batch_attempts: usize = 0,

    fn session(self: *FakeCollapsingVisionSession) backends.Session {
        return fakeVisionSession(self, run);
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeCollapsingVisionSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        const requested_batch: usize = @intCast(inputs[0].shape[0]);
        self.run_count += 1;
        if (requested_batch > 1) self.collapsed_batch_attempts = requested_batch;

        const actual_batch: usize = if (requested_batch > 1) 1 else requested_batch;
        const data = [_]f32{ 1.0, 2.0 };
        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "image_embeds", &.{ @intCast(actual_batch), 2 }, data[0 .. actual_batch * 2]);
        return out;
    }
};

fn fakeVisionSession(ptr: anytype, comptime runFn: anytype) backends.Session {
    const VTable = struct {
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, 2, 2 } }};
        }

        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "image_embeds", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }

        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }

        fn close(_: *anyopaque) void {}
    };
    return .{
        .ptr = @ptrCast(ptr),
        .vtable = &.{
            .run = runFn,
            .inputInfo = VTable.inputInfo,
            .outputInfo = VTable.outputInfo,
            .backend = VTable.backend,
            .close = VTable.close,
        },
    };
}

const FakeProjectionSession = struct {
    run_count: usize = 0,
    last_batch: usize = 0,
    last_dim: usize = 0,

    fn session(self: *FakeProjectionSession) backends.Session {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = run,
                .inputInfo = inputInfo,
                .outputInfo = outputInfo,
                .backend = backend,
                .close = close,
            },
        };
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeProjectionSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        try std.testing.expectEqual(@as(usize, 2), inputs[0].shape.len);
        const batch: usize = @intCast(inputs[0].shape[0]);
        const dim: usize = @intCast(inputs[0].shape[1]);
        self.run_count += 1;
        self.last_batch = batch;
        self.last_dim = dim;

        const input = inputs[0].asFloat32();
        const projected = try allocator.alloc(f32, batch * 3);
        defer allocator.free(projected);
        for (0..batch) |b| {
            const row = input[b * dim ..][0..dim];
            projected[b * 3 + 0] = row[0];
            projected[b * 3 + 1] = row[1];
            projected[b * 3 + 2] = row[0] + row[1];
        }

        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "projected", &.{ @intCast(batch), 3 }, projected);
        return out;
    }

    fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "input", .dtype = .f32, .shape = &.{ -1, 2 } }};
    }

    fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "output", .dtype = .f32, .shape = &.{ -1, 3 } }};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}
};
