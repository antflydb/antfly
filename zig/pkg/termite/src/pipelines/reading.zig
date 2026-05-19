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
//   - or a native Florence safetensors directory
//   - preprocessor_config.json (image size, normalization)
//   - tokenizer.json
//   - config.json (model_type: florence2, etc.)

const std = @import("std");
const build_options = @import("build_options");
const session_factory = @import("../architectures/session_factory.zig");
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("termite_tokenizer");
const image = @import("image.zig");

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

    pub fn init(
        allocator: std.mem.Allocator,
        vision_encoder: backends.Session,
        decoder: backends.Session,
        tokenizer: tokenizer_mod.Tokenizer,
        config: ReadConfig,
    ) ReadingPipeline {
        return .{
            .allocator = allocator,
            .vision_encoder = vision_encoder,
            .decoder = decoder,
            .tokenizer = tokenizer,
            .config = config,
        };
    }

    /// Read text from an image. image_data is raw JPEG/PNG bytes.
    pub fn read(self: *ReadingPipeline, image_data: []const u8) !ReadResult {
        const allocator = self.allocator;
        const decoded = try image.decode(allocator, image_data);
        defer decoded.deinit(allocator);

        if (expectsFlattenedPatches(self.vision_encoder)) {
            return self.readPix2StructDecoded(decoded);
        }

        const img_size: u32 = @intCast(self.config.image_size);
        const pixel_values = try image.preprocessDecodedWithResample(
            allocator,
            decoded,
            img_size,
            self.config.image_mean,
            self.config.image_std,
            self.config.resample,
        );
        defer allocator.free(pixel_values);
        return self.readPixelValues(pixel_values);
    }

    /// Read text from an already-decoded image crop.
    pub fn readDecoded(self: *ReadingPipeline, img: image.Image) !ReadResult {
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
        const img_size: u32 = @intCast(self.config.image_size);

        // 1. Preprocess image: decode/resize/normalize → [1, 3, H, W] f32
        // 2. Run vision encoder
        const img_sz: i64 = @intCast(img_size);
        const pv_shape = [_]i64{ 1, 3, img_sz, img_sz };
        var pv_tensor = try backends.Tensor.initFloat32(allocator, "pixel_values", &pv_shape, pixel_values);
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
        defer {
            for (encoder_outputs) |*t| {
                var mt = t.*;
                mt.deinit();
            }
            allocator.free(encoder_outputs);
        }

        if (encoder_outputs.len == 0) return error.NoEncoderOutput;
        return self.decodeFromEncoderOutputs(encoder_outputs, null);
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

            var enc_hidden = encoder_outputs[0];
            enc_hidden.name = "encoder_hidden_states";
            enc_hidden.owns_data = false;
            enc_hidden.owns_shape = false;
            try decoder_inputs.append(allocator, enc_hidden);

            const dec_outputs = try self.decoder.run(decoder_inputs.items, allocator);
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

            // Greedy: argmax
            const best_id = selectGreedyToken(last_logits, dec_ids[0..dec_len], self.config.no_repeat_ngram_size);

            // Check for EOS
            if (@as(i32, @intCast(best_id)) == self.config.eos_token_id) break;

            dec_ids[dec_len] = @intCast(best_id);
            dec_len += 1;
        }

        // 4. Decode token IDs to text
        // Convert i64 to i32 for tokenizer
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

    pub fn deinit(_: *ReadingPipeline) void {
        // Sessions and tokenizer are borrowed — caller manages their lifetime.
    }
};

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
