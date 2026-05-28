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

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const c_file = @import("../util/c_file.zig");
const manifest_mod = @import("../models/manifest.zig");
const gpt_mod = @import("../models/gpt.zig");
const generation = @import("generation.zig");
const gemma3_mm = @import("gemma3_multimodal.zig");
const hf_tokenizer = @import("termite_hf_tokenizer");
const tokenizer_mod = @import("termite_tokenizer");
const activations = @import("../backends/activations.zig");
const onnx_kv_cache = @import("../graph/onnx_kv_cache.zig");

const Tensor = backends.Tensor;
const Session = backends.Session;
const KvCache = onnx_kv_cache.KvCache;

const decoder_candidates = [_][]const u8{
    "decoder_model_merged.onnx",
    "decoder_model_merged_fp16.onnx",
    "decoder_model_merged_quantized.onnx",
    "decoder_model_merged_q4.onnx",
    "decoder_model_merged_q4f16.onnx",
};

const embed_candidates = [_][]const u8{
    "embed_tokens.onnx",
    "embed_tokens_fp16.onnx",
    "embed_tokens_quantized.onnx",
    "embed_tokens_q4.onnx",
    "embed_tokens_q4f16.onnx",
};

const vision_candidates = [_][]const u8{
    "vision_encoder.onnx",
    "vision_encoder_fp16.onnx",
    "vision_encoder_quantized.onnx",
    "vision_encoder_q4.onnx",
    "vision_encoder_q4f16.onnx",
};

pub fn isSupportedModelDir(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    if (!build_options.enable_onnx) return false;
    if (!c_file.fileExistsInDir(allocator, model_dir, "config.json")) return false;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return false;
    const decoder_path = findOnnxFile(allocator, model_dir, &decoder_candidates) catch return false;
    if (decoder_path) |path| allocator.free(path) else return false;
    const embed_path = findOnnxFile(allocator, model_dir, &embed_candidates) catch return false;
    if (embed_path) |path| allocator.free(path) else return false;
    return true;
}

const SharedDebugResources = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    hf_tok: *hf_tokenizer.HfTokenizer,
    gpt_config: gpt_mod.Config,
    chat_tmpl: ?*generation.ChatTemplate = null,

    fn load(allocator: std.mem.Allocator, model_dir: []const u8) !SharedDebugResources {
        var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
        errdefer manifest.deinit();

        const tok_bytes = try c_file.readFileFromDir(allocator, model_dir, "tokenizer.json");
        defer allocator.free(tok_bytes);
        const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
        errdefer tok.deinitSelf();

        const cfg_bytes = try c_file.readFileFromDir(allocator, model_dir, "config.json");
        defer allocator.free(cfg_bytes);
        const gpt_config = try gpt_mod.parseConfig(allocator, cfg_bytes);

        const chat_tmpl: ?*generation.ChatTemplate = if (manifest.chat_template) |ct_source| blk: {
            const ct = try allocator.create(generation.ChatTemplate);
            ct.* = try generation.ChatTemplate.init(
                allocator,
                ct_source,
                manifest.bos_token,
                manifest.eos_token,
                manifest.unk_token,
                manifest.pad_token,
            );
            break :blk ct;
        } else null;
        errdefer if (chat_tmpl) |ct| {
            var ct_mut = ct;
            ct_mut.deinit();
            allocator.destroy(ct_mut);
        };

        return .{
            .allocator = allocator,
            .model_dir = try allocator.dupe(u8, model_dir),
            .manifest = manifest,
            .hf_tok = tok,
            .gpt_config = gpt_config,
            .chat_tmpl = chat_tmpl,
        };
    }

    fn deinit(self: *SharedDebugResources) void {
        if (self.chat_tmpl) |ct| {
            var ct_mut = ct;
            ct_mut.deinit();
            self.allocator.destroy(ct_mut);
        }
        self.hf_tok.deinitSelf();
        self.allocator.free(self.model_dir);
        self.manifest.deinit();
    }
};

pub fn debugImageFeaturesFromDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    messages: []const generation.Message,
) ![]f32 {
    if (!build_options.enable_onnx) return error.OnnxNotEnabled;
    if (!generation.messagesHaveImages(messages)) return error.NoImages;

    var shared = try SharedDebugResources.load(allocator, model_dir);
    defer shared.deinit();

    const vision_path = (try findOnnxFile(allocator, model_dir, &vision_candidates)) orelse return error.MissingVisionEncoder;
    defer allocator.free(vision_path);
    const vision = try backends.onnx.createSession(allocator, vision_path);
    defer vision.close();

    const images = try collectImagesInPromptOrder(allocator, messages);
    defer allocator.free(images);
    const image_features = try encodeImages(allocator, model_dir, vision, shared.gpt_config, images);
    return image_features.data;
}

pub fn debugPromptEmbeddingsFromDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    messages: []const generation.Message,
    prompt_override: ?[]const u8,
) !struct { token_ids: []i64, embeds: []f32 } {
    if (!build_options.enable_onnx) return error.OnnxNotEnabled;

    std.debug.print("onnx-debug: load shared resources\n", .{});
    var shared = try SharedDebugResources.load(allocator, model_dir);
    defer shared.deinit();

    std.debug.print("onnx-debug: create embed session\n", .{});
    const embed_path = (try findOnnxFile(allocator, model_dir, &embed_candidates)) orelse return error.EmbedTokensModelNotFound;
    defer allocator.free(embed_path);
    const embed_tokens = try backends.onnx.createSession(allocator, embed_path);
    defer embed_tokens.close();

    std.debug.print("onnx-debug: create vision session\n", .{});
    const vision_path = try findOnnxFile(allocator, model_dir, &vision_candidates);
    defer if (vision_path) |path| allocator.free(path);
    const vision_encoder = if (vision_path) |path|
        try backends.onnx.createSession(allocator, path)
    else
        null;
    defer if (vision_encoder) |session| session.close();

    std.debug.print("onnx-debug: render prompt\n", .{});
    const prompt = if (prompt_override) |override|
        try allocator.dupe(u8, override)
    else if (shared.chat_tmpl) |ct|
        try ct.apply(allocator, messages, true)
    else
        try generation.formatMessages(allocator, messages);
    defer allocator.free(prompt);

    const tok = shared.hf_tok.tokenizer();
    const has_images = generation.messagesHaveImages(messages);
    std.debug.print("onnx-debug: collect images has_images={}\n", .{has_images});
    const image_bytes = if (has_images)
        try collectImagesInPromptOrder(allocator, messages)
    else
        &[_][]const u8{};
    defer if (has_images) allocator.free(image_bytes);

    std.debug.print("onnx-debug: encode prompt tokens\n", .{});
    const token_ids = blk: {
        var encoded = try generation.encodePromptForGeneration(
            tok,
            allocator,
            prompt,
            4096,
            shared.manifest.add_bos_token,
            shared.manifest.bos_token,
        );
        defer encoded.deinit();

        const prompt_tokens = countPromptTokens(encoded.attention_mask);
        if (prompt_tokens == 0) return error.EmptyPrompt;

        const ids = try allocator.alloc(i64, prompt_tokens);
        for (0..prompt_tokens) |idx| ids[idx] = encoded.ids[idx];
        break :blk ids;
    };
    errdefer allocator.free(token_ids);

    std.debug.print("onnx-debug: run embed session tokens={d}\n", .{token_ids.len});
    const input_shape = [_]i64{ 1, @intCast(token_ids.len) };
    var input_tensor = try Tensor.initInt64(allocator, "input_ids", &input_shape, token_ids);
    defer input_tensor.deinit();

    var embed_outputs = try embed_tokens.run(&.{input_tensor}, allocator);
    defer freeTensorSlice(allocator, embed_outputs);
    if (embed_outputs.len == 0) return error.NoEmbedOutput;

    const embeds = try tensorToOwnedF32(allocator, &embed_outputs[0]);
    if (image_bytes.len > 0) {
        std.debug.print("onnx-debug: encode images count={d}\n", .{image_bytes.len});
        const vision = vision_encoder orelse return error.MissingVisionEncoder;
        const image_features = try encodeImages(allocator, model_dir, vision, shared.gpt_config, image_bytes);
        defer allocator.free(image_features.data);
        const hidden_size = if (embed_outputs[0].shape.len >= 3) @as(usize, @intCast(embed_outputs[0].shape[2])) else return error.InvalidEmbeddingShape;
        std.debug.print("onnx-debug: concat image features hidden={d}\n", .{hidden_size});
        const combined = try concatImageAndTextEmbeddings(allocator, image_features.data, embeds, hidden_size);
        allocator.free(embeds);
        return .{
            .token_ids = token_ids,
            .embeds = combined,
        };
    }

    std.debug.print("onnx-debug: done prompt embeddings\n", .{});
    return .{
        .token_ids = token_ids,
        .embeds = embeds,
    };
}

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    hf_tok: *hf_tokenizer.HfTokenizer,
    decoder: Session,
    embed_tokens: Session,
    vision_encoder: ?Session = null,
    gpt_config: gpt_mod.Config,
    eos_token_ids: []i64,
    chat_tmpl: ?*generation.ChatTemplate = null,
    prompt_override: ?[]const u8 = null,

    pub fn load(allocator: std.mem.Allocator, model_dir: []const u8) !Pipeline {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;

        var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
        errdefer manifest.deinit();

        const tok_bytes = try c_file.readFileFromDir(allocator, model_dir, "tokenizer.json");
        defer allocator.free(tok_bytes);
        const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
        errdefer tok.deinitSelf();

        const cfg_bytes = try c_file.readFileFromDir(allocator, model_dir, "config.json");
        defer allocator.free(cfg_bytes);
        const gpt_config = try gpt_mod.parseConfig(allocator, cfg_bytes);

        const eos_token_ids = try loadEosTokenIds(allocator, model_dir, &manifest, gpt_config);
        errdefer allocator.free(eos_token_ids);

        const decoder_path = (try findOnnxFile(allocator, model_dir, &decoder_candidates)) orelse return error.DecoderModelNotFound;
        defer allocator.free(decoder_path);
        const embed_path = (try findOnnxFile(allocator, model_dir, &embed_candidates)) orelse return error.EmbedTokensModelNotFound;
        defer allocator.free(embed_path);
        const vision_path = try findOnnxFile(allocator, model_dir, &vision_candidates);
        defer if (vision_path) |path| allocator.free(path);

        const decoder = try backends.onnx.createSession(allocator, decoder_path);
        errdefer decoder.close();
        const embed_tokens = try backends.onnx.createSession(allocator, embed_path);
        errdefer embed_tokens.close();
        const vision_encoder = if (vision_path) |path|
            try backends.onnx.createSession(allocator, path)
        else
            null;
        errdefer if (vision_encoder) |session| session.close();

        if (platform.env.getenv("TERMITE_DEBUG_ONNX_INPUTS") != null) {
            std.debug.print("onnx-decoder-inputs:\n", .{});
            for (decoder.inputInfo()) |info| {
                std.debug.print("  {s} dtype={s} shape=", .{ info.name, @tagName(info.dtype) });
                for (info.shape, 0..) |dim, idx| {
                    if (idx > 0) std.debug.print("x", .{});
                    std.debug.print("{d}", .{dim});
                }
                std.debug.print("\n", .{});
            }
            std.debug.print("onnx-decoder-outputs:\n", .{});
            for (decoder.outputInfo()) |info| {
                std.debug.print("  {s} dtype={s} shape=", .{ info.name, @tagName(info.dtype) });
                for (info.shape, 0..) |dim, idx| {
                    if (idx > 0) std.debug.print("x", .{});
                    std.debug.print("{d}", .{dim});
                }
                std.debug.print("\n", .{});
            }
        }

        const chat_tmpl: ?*generation.ChatTemplate = if (manifest.chat_template) |ct_source| blk: {
            const ct = try allocator.create(generation.ChatTemplate);
            ct.* = try generation.ChatTemplate.init(
                allocator,
                ct_source,
                manifest.bos_token,
                manifest.eos_token,
                manifest.unk_token,
                manifest.pad_token,
            );
            break :blk ct;
        } else null;
        errdefer if (chat_tmpl) |ct| {
            var ct_mut = ct;
            ct_mut.deinit();
            allocator.destroy(ct_mut);
        };

        return .{
            .allocator = allocator,
            .model_dir = try allocator.dupe(u8, model_dir),
            .manifest = manifest,
            .hf_tok = tok,
            .decoder = decoder,
            .embed_tokens = embed_tokens,
            .vision_encoder = vision_encoder,
            .gpt_config = gpt_config,
            .eos_token_ids = eos_token_ids,
            .chat_tmpl = chat_tmpl,
        };
    }

    pub fn deinit(self: *Pipeline) void {
        self.decoder.close();
        self.embed_tokens.close();
        if (self.vision_encoder) |session| session.close();
        if (self.chat_tmpl) |ct| {
            var ct_mut = ct;
            ct_mut.deinit();
            self.allocator.destroy(ct_mut);
        }
        self.hf_tok.deinitSelf();
        self.allocator.free(self.eos_token_ids);
        self.allocator.free(self.model_dir);
        self.manifest.deinit();
    }

    pub fn generate(self: *Pipeline, messages: []const generation.Message, config: generation.GenerationConfig) !generation.GenerationResult {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;

        const prompt = if (self.prompt_override) |override|
            try self.allocator.dupe(u8, override)
        else if (self.chat_tmpl) |ct|
            try ct.apply(self.allocator, messages, true)
        else
            try generation.formatMessages(self.allocator, messages);
        defer self.allocator.free(prompt);

        const has_images = generation.messagesHaveImages(messages);

        const image_bytes = if (has_images)
            try collectImagesInPromptOrder(self.allocator, messages)
        else
            &[_][]const u8{};
        defer if (has_images) self.allocator.free(image_bytes);

        return self.generatePrompt(prompt, image_bytes, config);
    }

    pub fn generatePrompt(
        self: *Pipeline,
        prompt: []const u8,
        images: []const []const u8,
        config: generation.GenerationConfig,
    ) !generation.GenerationResult {
        const tok = self.hf_tok.tokenizer();

        const token_ids = blk: {
            var encoded = try generation.encodePromptForGeneration(
                tok,
                self.allocator,
                prompt,
                4096,
                self.manifest.add_bos_token,
                self.manifest.bos_token,
            );
            defer encoded.deinit();

            const prompt_tokens = countPromptTokens(encoded.attention_mask);
            if (prompt_tokens == 0) return error.EmptyPrompt;

            const ids = try self.allocator.alloc(i64, prompt_tokens);
            for (0..prompt_tokens) |idx| ids[idx] = encoded.ids[idx];
            break :blk ids;
        };
        defer self.allocator.free(token_ids);

        var generation_ids = std.ArrayListUnmanaged(i32).empty;
        defer generation_ids.deinit(self.allocator);

        var penalty_state = SamplingPenaltyState{};
        defer penalty_state.deinit(self.allocator);
        try penalty_state.seedFromHistory(self.allocator, token_ids);

        var kv_cache = KvCache.init(self.allocator);
        defer kv_cache.deinit();

        var working_token_ids = token_ids;
        var next_token_buf: [1]i64 = undefined;
        var first_step = true;
        var finish_reason: []const u8 = "length";

        const max_tokens: usize = @intCast(@max(config.max_tokens, 1));
        for (0..max_tokens) |_| {
            const logits = if (first_step)
                try self.firstStep(working_token_ids, images, &kv_cache)
            else
                try self.subsequentStep(working_token_ids, &kv_cache);
            defer self.allocator.free(logits);

            first_step = false;
            try kv_cache.replace(self.allocator, self.decoder.outputInfo(), kv_cache.pending_outputs);

            const next_token: usize = sample(logits, config, &penalty_state, self.allocator);
            const next_token_i64: i64 = @intCast(next_token);
            const next_token_i32: i32 = @intCast(next_token);
            try generation_ids.append(self.allocator, next_token_i32);
            try penalty_state.noteToken(self.allocator, next_token_i64);

            if (isEosToken(self.eos_token_ids, next_token_i64)) {
                finish_reason = "stop";
                break;
            }

            next_token_buf[0] = next_token_i64;
            working_token_ids = next_token_buf[0..];
        }

        const text = try tok.decode(self.allocator, generation_ids.items);
        return .{
            .text = text,
            .token_ids = try self.allocator.dupe(i32, generation_ids.items),
            .prompt_tokens = token_ids.len,
            .tokens_used = generation_ids.items.len,
            .finish_reason = finish_reason,
            .allocator = self.allocator,
        };
    }

    pub fn generateStreaming(
        self: *Pipeline,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
        on_token_ctx: *anyopaque,
        on_token: generation.TokenCallback,
    ) !generation.GenerationResult {
        var result = try self.generate(messages, config);
        errdefer result.deinit();
        if (result.text.len > 0 and !on_token(on_token_ctx, result.text)) {
            result.finish_reason = "stop";
        }
        return result;
    }

    pub fn firstTokenDebug(self: *Pipeline, prompt: []const u8) !struct { token_id: i32, text: []u8 } {
        const tok = self.hf_tok.tokenizer();
        var encoded = try generation.encodePromptForGeneration(
            tok,
            self.allocator,
            prompt,
            4096,
            self.manifest.add_bos_token,
            self.manifest.bos_token,
        );
        defer encoded.deinit();
        const prompt_tokens = countPromptTokens(encoded.attention_mask);
        if (prompt_tokens == 0) return error.EmptyPrompt;
        const ids = try self.allocator.alloc(i64, prompt_tokens);
        defer self.allocator.free(ids);
        for (0..prompt_tokens) |idx| ids[idx] = encoded.ids[idx];

        var kv_cache = KvCache.init(self.allocator);
        defer kv_cache.deinit();
        const logits = try self.firstStep(ids, &.{}, &kv_cache);
        defer self.allocator.free(logits);
        const token_id: i32 = @intCast(activations.argmax(logits));
        const token_text = try tok.decode(self.allocator, &.{token_id});
        return .{ .token_id = token_id, .text = token_text };
    }

    pub fn debugImageFeatures(self: *Pipeline, messages: []const generation.Message) ![]f32 {
        const has_images = generation.messagesHaveImages(messages);
        if (!has_images) return error.NoImages;
        const images = try collectImagesInPromptOrder(self.allocator, messages);
        defer self.allocator.free(images);
        const vision = self.vision_encoder orelse return error.MissingVisionEncoder;
        const image_features = try encodeImages(self.allocator, self.model_dir, vision, self.gpt_config, images);
        return image_features.data;
    }

    pub fn debugPromptEmbeddings(self: *Pipeline, messages: []const generation.Message) !struct { token_ids: []i64, embeds: []f32 } {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;

        const prompt = if (self.prompt_override) |override|
            try self.allocator.dupe(u8, override)
        else if (self.chat_tmpl) |ct|
            try ct.apply(self.allocator, messages, true)
        else
            try generation.formatMessages(self.allocator, messages);
        defer self.allocator.free(prompt);

        const tok = self.hf_tok.tokenizer();
        const has_images = generation.messagesHaveImages(messages);

        const image_bytes = if (has_images)
            try collectImagesInPromptOrder(self.allocator, messages)
        else
            &[_][]const u8{};
        defer if (has_images) self.allocator.free(image_bytes);

        const token_ids = blk: {
            var encoded = try generation.encodePromptForGeneration(
                tok,
                self.allocator,
                prompt,
                4096,
                self.manifest.add_bos_token,
                self.manifest.bos_token,
            );
            defer encoded.deinit();

            const prompt_tokens = countPromptTokens(encoded.attention_mask);
            if (prompt_tokens == 0) return error.EmptyPrompt;

            const ids = try self.allocator.alloc(i64, prompt_tokens);
            for (0..prompt_tokens) |idx| ids[idx] = encoded.ids[idx];
            break :blk ids;
        };

        errdefer self.allocator.free(token_ids);

        const input_shape = [_]i64{ 1, @intCast(token_ids.len) };
        var input_tensor = try Tensor.initInt64(self.allocator, "input_ids", &input_shape, token_ids);
        defer input_tensor.deinit();

        var embed_outputs = try self.embed_tokens.run(&.{input_tensor}, self.allocator);
        defer freeTensorSlice(self.allocator, embed_outputs);
        if (embed_outputs.len == 0) return error.NoEmbedOutput;

        const embeds = try tensorToOwnedF32(self.allocator, &embed_outputs[0]);
        if (image_bytes.len > 0) {
            const vision = self.vision_encoder orelse return error.MissingVisionEncoder;
            const image_features = try encodeImages(self.allocator, self.model_dir, vision, self.gpt_config, image_bytes);
            defer self.allocator.free(image_features.data);
            const hidden_size = if (embed_outputs[0].shape.len >= 3) @as(usize, @intCast(embed_outputs[0].shape[2])) else return error.InvalidEmbeddingShape;
            const combined = try concatImageAndTextEmbeddings(self.allocator, image_features.data, embeds, hidden_size);
            self.allocator.free(embeds);
            return .{
                .token_ids = token_ids,
                .embeds = combined,
            };
        }

        return .{
            .token_ids = token_ids,
            .embeds = embeds,
        };
    }

    fn firstStep(self: *Pipeline, input_ids: []const i64, images: []const []const u8, kv_cache: *KvCache) ![]f32 {
        var decoder_inputs = std.ArrayListUnmanaged(Tensor).empty;
        defer {
            for (decoder_inputs.items) |*tensor| tensor.deinit();
            decoder_inputs.deinit(self.allocator);
        }

        const seq_len = input_ids.len;
        const input_shape = [_]i64{ 1, @intCast(seq_len) };
        var input_tensor = try Tensor.initInt64(self.allocator, "input_ids", &input_shape, input_ids);
        defer input_tensor.deinit();

        var embed_outputs = try self.embed_tokens.run(&.{input_tensor}, self.allocator);
        defer freeTensorSlice(self.allocator, embed_outputs);
        if (embed_outputs.len == 0) return error.NoEmbedOutput;

        var embeds = try tensorToOwnedF32(self.allocator, &embed_outputs[0]);
        defer self.allocator.free(embeds);
        const hidden_size = if (embed_outputs[0].shape.len >= 3) @as(usize, @intCast(embed_outputs[0].shape[2])) else return error.InvalidEmbeddingShape;
        var total_seq_len = seq_len;

        if (images.len > 0) {
            const vision = self.vision_encoder orelse return error.MissingVisionEncoder;
            const image_features = try encodeImages(self.allocator, self.model_dir, vision, self.gpt_config, images);
            defer self.allocator.free(image_features.data);
            const combined = try concatImageAndTextEmbeddings(self.allocator, image_features.data, embeds, hidden_size);
            self.allocator.free(embeds);
            embeds = combined;
            total_seq_len = combined.len / hidden_size;
        }

        const embed_shape = [_]i64{ 1, @intCast(total_seq_len), @intCast(hidden_size) };
        try decoder_inputs.append(
            self.allocator,
            try initSessionFloatInput(self.allocator, self.decoder, "inputs_embeds", &embed_shape, embeds),
        );

        if (hasInput(self.decoder, "attention_mask")) {
            const attention_mask = try allocOnesI64(self.allocator, total_seq_len);
            defer self.allocator.free(attention_mask);
            const mask_shape = [_]i64{ 1, @intCast(total_seq_len) };
            try decoder_inputs.append(self.allocator, try Tensor.initInt64(self.allocator, "attention_mask", &mask_shape, attention_mask));
        }

        try appendPositionIdsIfNeeded(self.allocator, self.decoder, &decoder_inputs, 0, total_seq_len);
        try appendUseCacheBranchIfNeeded(self.allocator, self.decoder, &decoder_inputs, false);

        if (hasInput(self.decoder, "num_logits_to_keep")) {
            const scalar_shape = [_]i64{};
            const one = [_]i64{1};
            try decoder_inputs.append(self.allocator, try Tensor.initInt64(self.allocator, "num_logits_to_keep", &scalar_shape, &one));
        }

        try appendZeroPastKvInputs(self.allocator, self.decoder, &decoder_inputs, self.gpt_config);

        const outputs = try self.decoder.run(decoder_inputs.items, self.allocator);
        return extractLogitsAndMoveKv(self.allocator, outputs, kv_cache);
    }

    fn subsequentStep(self: *Pipeline, input_ids: []const i64, kv_cache: *KvCache) ![]f32 {
        var decoder_inputs = std.ArrayListUnmanaged(Tensor).empty;
        defer {
            for (decoder_inputs.items) |*tensor| tensor.deinit();
            decoder_inputs.deinit(self.allocator);
        }

        const seq_len = input_ids.len;
        const input_shape = [_]i64{ 1, @intCast(seq_len) };
        var input_tensor = try Tensor.initInt64(self.allocator, "input_ids", &input_shape, input_ids);
        defer input_tensor.deinit();

        var embed_outputs = try self.embed_tokens.run(&.{input_tensor}, self.allocator);
        defer freeTensorSlice(self.allocator, embed_outputs);
        if (embed_outputs.len == 0) return error.NoEmbedOutput;

        const embeds = try tensorToOwnedF32(self.allocator, &embed_outputs[0]);
        defer self.allocator.free(embeds);
        const hidden_size = if (embed_outputs[0].shape.len >= 3) @as(usize, @intCast(embed_outputs[0].shape[2])) else return error.InvalidEmbeddingShape;

        const embed_shape = [_]i64{ 1, @intCast(seq_len), @intCast(hidden_size) };
        try decoder_inputs.append(
            self.allocator,
            try initSessionFloatInput(self.allocator, self.decoder, "inputs_embeds", &embed_shape, embeds),
        );

        if (hasInput(self.decoder, "attention_mask")) {
            const past_seq_len = kv_cache.seqLen();
            const total_len = past_seq_len + seq_len;
            const attention_mask = try allocOnesI64(self.allocator, total_len);
            defer self.allocator.free(attention_mask);
            const mask_shape = [_]i64{ 1, @intCast(total_len) };
            try decoder_inputs.append(self.allocator, try Tensor.initInt64(self.allocator, "attention_mask", &mask_shape, attention_mask));
        }

        try appendPositionIdsIfNeeded(self.allocator, self.decoder, &decoder_inputs, kv_cache.seqLen(), seq_len);
        try appendUseCacheBranchIfNeeded(self.allocator, self.decoder, &decoder_inputs, true);

        if (hasInput(self.decoder, "num_logits_to_keep")) {
            const scalar_shape = [_]i64{};
            const one = [_]i64{1};
            try decoder_inputs.append(self.allocator, try Tensor.initInt64(self.allocator, "num_logits_to_keep", &scalar_shape, &one));
        }

        try onnx_kv_cache.appendPastInputs(self.allocator, self.decoder.inputInfo(), kv_cache, &decoder_inputs);

        const outputs = try self.decoder.run(decoder_inputs.items, self.allocator);
        return extractLogitsAndMoveKv(self.allocator, outputs, kv_cache);
    }
};

const ImageFeatures = struct {
    data: []f32,
};

const SamplingPenaltyState = struct {
    counts: std.AutoHashMapUnmanaged(u32, u32) = .empty,

    fn deinit(self: *SamplingPenaltyState, allocator: std.mem.Allocator) void {
        self.counts.deinit(allocator);
        self.* = .{};
    }

    fn seedFromHistory(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_history: []const i64) !void {
        for (token_history) |token_id| try self.noteToken(allocator, token_id);
    }

    fn noteToken(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_id: i64) !void {
        if (token_id < 0) return;
        const entry = try self.counts.getOrPut(allocator, @intCast(token_id));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }

    fn isEmpty(self: *const SamplingPenaltyState) bool {
        return self.counts.count() == 0;
    }
};

fn findOnnxFile(allocator: std.mem.Allocator, model_dir: []const u8, candidates: []const []const u8) !?[]u8 {
    const subdirs = [_][]const u8{ "", "onnx" };
    for (subdirs) |subdir| {
        for (candidates) |candidate| {
            const path = if (subdir.len > 0)
                try std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ model_dir, subdir, candidate })
            else
                try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, candidate });
            errdefer allocator.free(path);
            if (c_file.fileExists(allocator, path)) return path;
            allocator.free(path);
        }
    }
    return null;
}

fn loadEosTokenIds(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: *const manifest_mod.ModelManifest,
    gpt_config: gpt_mod.Config,
) ![]i64 {
    _ = manifest;
    if (c_file.readFileFromDir(allocator, model_dir, "generation_config.json")) |gen_cfg_bytes| {
        defer allocator.free(gen_cfg_bytes);
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, gen_cfg_bytes, .{});
        defer parsed.deinit();
        if (parsed.value == .object) {
            if (parsed.value.object.get("eos_token_id")) |value| {
                if (try jsonValueToIntArray(allocator, value)) |ids| return ids;
            }
        }
    } else |_| {}

    const eos = if (gpt_config.eos_token_id >= 0) gpt_config.eos_token_id else 1;
    const ids = try allocator.alloc(i64, 1);
    ids[0] = eos;
    return ids;
}

fn jsonValueToIntArray(allocator: std.mem.Allocator, value: std.json.Value) !?[]i64 {
    return switch (value) {
        .integer => blk: {
            const ids = try allocator.alloc(i64, 1);
            ids[0] = value.integer;
            break :blk ids;
        },
        .array => blk: {
            const ids = try allocator.alloc(i64, value.array.items.len);
            var count: usize = 0;
            for (value.array.items) |item| {
                switch (item) {
                    .integer => {
                        ids[count] = item.integer;
                        count += 1;
                    },
                    else => {},
                }
            }
            if (count == 0) {
                allocator.free(ids);
                break :blk null;
            }
            if (count < ids.len) {
                break :blk try allocator.realloc(ids, count);
            }
            break :blk ids;
        },
        else => null,
    };
}

fn countPromptTokens(attention_mask: []const i32) usize {
    var count: usize = 0;
    while (count < attention_mask.len and attention_mask[count] != 0) : (count += 1) {}
    return count;
}

fn allocOnesI64(allocator: std.mem.Allocator, len: usize) ![]i64 {
    const values = try allocator.alloc(i64, len);
    @memset(values, 1);
    return values;
}

fn growAttentionMask(allocator: std.mem.Allocator, old: []i64) ![]i64 {
    const next = try allocator.realloc(old, old.len + 1);
    next[next.len - 1] = 1;
    return next;
}

fn hasInput(session: Session, name: []const u8) bool {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return true;
    }
    return false;
}

fn freeTensorSlice(allocator: std.mem.Allocator, tensors: []Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

fn tensorToOwnedF32(allocator: std.mem.Allocator, tensor: *const Tensor) ![]f32 {
    return switch (tensor.dtype) {
        .f32 => allocator.dupe(f32, tensor.asFloat32()),
        .f16 => convertF16ToF32(allocator, tensor.data),
        .bf16 => convertBf16ToF32(allocator, tensor.data),
        else => error.UnsupportedTensorType,
    };
}

fn convertF16ToF32(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    const count = data.len / 2;
    const out = try allocator.alloc(f32, count);
    const aligned: []align(@alignOf(u16)) const u8 = @alignCast(data);
    const src = std.mem.bytesAsSlice(u16, aligned);
    for (src, 0..) |bits, idx| out[idx] = @floatCast(@as(f16, @bitCast(bits)));
    return out;
}

fn convertBf16ToF32(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    const count = data.len / 2;
    const out = try allocator.alloc(f32, count);
    const aligned: []align(@alignOf(u16)) const u8 = @alignCast(data);
    const src = std.mem.bytesAsSlice(u16, aligned);
    for (src, 0..) |bits, idx| {
        const wide: u32 = @as(u32, bits) << 16;
        out[idx] = @bitCast(wide);
    }
    return out;
}

fn encodeImages(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    vision_session: Session,
    gpt_config: gpt_mod.Config,
    images: []const []const u8,
) !ImageFeatures {
    const pre_cfg = try gemma3_mm.loadPreprocessorConfig(allocator, model_dir);
    _ = gpt_config;
    const image_size = pre_cfg.image_size;
    const pixels_per_image = 3 * image_size * image_size;
    const pixel_values = try allocator.alloc(f32, images.len * pixels_per_image);
    defer allocator.free(pixel_values);

    for (images, 0..) |image_bytes, idx| {
        const processed = try gemma3_mm.preprocessImage(allocator, image_bytes, pre_cfg);
        defer allocator.free(processed);
        @memcpy(pixel_values[idx * pixels_per_image ..][0..pixels_per_image], processed);
    }

    const shape = [_]i64{ @intCast(images.len), 3, @intCast(image_size), @intCast(image_size) };
    var pv = try initSessionFloatInput(allocator, vision_session, "pixel_values", &shape, pixel_values);
    defer pv.deinit();

    var outputs = try vision_session.run(&.{pv}, allocator);
    defer freeTensorSlice(allocator, outputs);
    if (outputs.len == 0) return error.NoVisionOutput;
    return .{ .data = try tensorToOwnedF32(allocator, &outputs[0]) };
}

fn concatImageAndTextEmbeddings(
    allocator: std.mem.Allocator,
    image_features: []const f32,
    text_embeds: []const f32,
    hidden_size: usize,
) ![]f32 {
    if (hidden_size == 0) return error.InvalidEmbeddingShape;
    if (image_features.len % hidden_size != 0) return error.ImageFeatureCountMismatch;
    if (text_embeds.len % hidden_size != 0) return error.InvalidEmbeddingShape;
    const combined = try allocator.alloc(f32, image_features.len + text_embeds.len);
    @memcpy(combined[0..image_features.len], image_features);
    @memcpy(combined[image_features.len..], text_embeds);
    return combined;
}

fn appendZeroPastKvInputs(
    allocator: std.mem.Allocator,
    decoder: Session,
    decoder_inputs: *std.ArrayListUnmanaged(Tensor),
    gpt_config: gpt_mod.Config,
) !void {
    const head_dim = gpt_config.headDim();
    const kv_heads = gpt_config.effectiveKVHeads();
    const empty = [_]f32{};
    const shape = [_]i64{ 1, @intCast(kv_heads), 0, @intCast(head_dim) };

    for (decoder.inputInfo()) |info| {
        if (!std.mem.startsWith(u8, info.name, "past_key_values.")) continue;
        try decoder_inputs.append(allocator, try initFloatTensorForDType(allocator, info.name, &shape, &empty, info.dtype));
    }
}

fn appendPositionIdsIfNeeded(
    allocator: std.mem.Allocator,
    decoder: Session,
    decoder_inputs: *std.ArrayListUnmanaged(Tensor),
    start_pos: usize,
    seq_len: usize,
) !void {
    if (!hasInput(decoder, "position_ids")) return;
    const pos_ids = try allocator.alloc(i64, seq_len);
    defer allocator.free(pos_ids);
    for (0..seq_len) |idx| pos_ids[idx] = @intCast(start_pos + idx);
    const pos_shape = [_]i64{ 1, @intCast(seq_len) };
    try decoder_inputs.append(allocator, try Tensor.initInt64(allocator, "position_ids", &pos_shape, pos_ids));
}

fn appendUseCacheBranchIfNeeded(
    allocator: std.mem.Allocator,
    decoder: Session,
    decoder_inputs: *std.ArrayListUnmanaged(Tensor),
    enabled: bool,
) !void {
    const dtype = tensorInputDType(decoder, "use_cache_branch") orelse return;
    switch (dtype) {
        .bool_ => {
            const scalar_shape = [_]i64{1};
            const value = [_]u8{if (enabled) 1 else 0};
            try decoder_inputs.append(allocator, try Tensor.initBool(allocator, "use_cache_branch", &scalar_shape, &value));
        },
        .f32 => {
            const scalar_shape = [_]i64{1};
            const value = [_]f32{if (enabled) 1.0 else 0.0};
            try decoder_inputs.append(allocator, try Tensor.initFloat32(allocator, "use_cache_branch", &scalar_shape, &value));
        },
        else => {},
    }
}

fn tensorInputDType(session: Session, name: []const u8) ?backends.DType {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return info.dtype;
    }
    return null;
}

fn initSessionFloatInput(
    allocator: std.mem.Allocator,
    session: Session,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !Tensor {
    return initFloatTensorForDType(
        allocator,
        name,
        shape,
        data,
        tensorInputDType(session, name) orelse .f32,
    );
}

fn initFloatTensorForDType(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
    dtype: backends.DType,
) !Tensor {
    return switch (dtype) {
        .f32 => Tensor.initFloat32(allocator, name, shape, data),
        .f16 => initFloat16Tensor(allocator, name, shape, data),
        .bf16 => initBFloat16Tensor(allocator, name, shape, data),
        else => error.UnsupportedTensorType,
    };
}

fn initFloat16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !Tensor {
    const owned_bytes = try allocator.alloc(u8, data.len * 2);
    errdefer allocator.free(owned_bytes);
    const aligned: []align(@alignOf(u16)) u8 = @alignCast(owned_bytes);
    const dst = std.mem.bytesAsSlice(u16, aligned);
    for (data, 0..) |value, idx| {
        const half: f16 = @floatCast(value);
        dst[idx] = @bitCast(half);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = owned_bytes,
        .dtype = .f16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initBFloat16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !Tensor {
    const owned_bytes = try allocator.alloc(u8, data.len * 2);
    errdefer allocator.free(owned_bytes);
    const aligned: []align(@alignOf(u16)) u8 = @alignCast(owned_bytes);
    const dst = std.mem.bytesAsSlice(u16, aligned);
    for (data, 0..) |value, idx| {
        const wide: u32 = @bitCast(value);
        dst[idx] = @intCast(wide >> 16);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = owned_bytes,
        .dtype = .bf16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn extractLogitsAndMoveKv(allocator: std.mem.Allocator, outputs: []Tensor, kv_cache: ?*KvCache) ![]f32 {
    if (outputs.len == 0) {
        allocator.free(outputs);
        return error.NoDecoderOutput;
    }

    const logits_tensor = outputs[0];
    defer {
        var mutable = logits_tensor;
        mutable.deinit();
    }
    const full_logits = try tensorToOwnedF32(allocator, &logits_tensor);
    defer allocator.free(full_logits);
    const vocab_size: usize = @intCast(logits_tensor.shape[logits_tensor.shape.len - 1]);
    const logits = try allocator.dupe(f32, full_logits[full_logits.len - vocab_size ..]);

    var moved_outputs = try allocator.alloc(Tensor, outputs.len);
    @memcpy(moved_outputs, outputs);
    moved_outputs[0].owns_data = false;
    moved_outputs[0].owns_shape = false;
    allocator.free(outputs);

    if (kv_cache) |cache| {
        if (cache.pending_outputs) |old| freeTensorSlice(allocator, old);
        cache.pending_outputs = moved_outputs;
    } else {
        freeTensorSlice(allocator, moved_outputs);
    }

    return logits[(logits.len - @as(usize, @intCast(logits_tensor.shape[logits_tensor.shape.len - 1])))..];
}

fn collectImagesInPromptOrder(allocator: std.mem.Allocator, messages: []const generation.Message) ![]const []const u8 {
    var images = std.ArrayListUnmanaged([]const u8).empty;
    errdefer images.deinit(allocator);

    for (messages) |msg| {
        if (msg.content_parts) |parts| {
            const msg_images = msg.image_bytes orelse &.{};
            for (parts) |part| {
                switch (part) {
                    .text => {},
                    .image => |image_idx| {
                        if (image_idx >= msg_images.len) return error.InvalidMessageImageIndex;
                        try images.append(allocator, msg_images[image_idx]);
                    },
                    .audio => {},
                }
            }
        } else if (msg.image_bytes) |msg_images| {
            for (msg_images) |image_bytes| try images.append(allocator, image_bytes);
        }
    }

    return try images.toOwnedSlice(allocator);
}

fn isEosToken(eos_ids: []const i64, token_id: i64) bool {
    for (eos_ids) |eos| {
        if (eos == token_id) return true;
    }
    return false;
}

fn sample(logits: []const f32, config: generation.GenerationConfig, penalty_state: *const SamplingPenaltyState, allocator: std.mem.Allocator) usize {
    const has_penalties = config.repetition_penalty != 1.0 or config.frequency_penalty != 0 or config.presence_penalty != 0;
    if (config.temperature <= 0 and !has_penalties) {
        return activations.argmax(logits);
    }

    const working = allocator.alloc(f32, logits.len) catch return activations.argmax(logits);
    defer allocator.free(working);
    @memcpy(working, logits);

    if (has_penalties and !penalty_state.isEmpty()) {
        applyRepetitionPenalties(working, penalty_state, config);
    }
    if (config.temperature <= 0) return activations.argmax(working);

    const inv_temp = 1.0 / config.temperature;
    for (working) |*value| value.* *= inv_temp;
    activations.softmax(working, working.len);
    if (config.top_k > 0 and @as(usize, @intCast(config.top_k)) < working.len) {
        activations.topK(working, @intCast(config.top_k), allocator);
    }
    if (config.top_p > 0 and config.top_p < 1.0) {
        activations.topP(working, config.top_p, allocator);
    }
    if (config.min_p > 0 and config.min_p < 1.0) {
        applyMinP(working, config.min_p);
    }
    return activations.sampleFromProbs(working);
}

fn applyRepetitionPenalties(logits: []f32, penalty_state: *const SamplingPenaltyState, config: generation.GenerationConfig) void {
    var it = penalty_state.counts.iterator();
    while (it.next()) |entry| {
        const token_id = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (token_id >= logits.len) continue;
        if (config.repetition_penalty != 1.0) {
            const logit = logits[token_id];
            if (logit > 0) {
                logits[token_id] = logit / config.repetition_penalty;
            } else {
                logits[token_id] = logit * config.repetition_penalty;
            }
        }
        if (config.frequency_penalty != 0) {
            logits[token_id] -= config.frequency_penalty * @as(f32, @floatFromInt(count));
        }
        if (config.presence_penalty != 0) {
            logits[token_id] -= config.presence_penalty;
        }
    }
}

fn applyMinP(probs: []f32, min_p: f32) void {
    var max_prob: f32 = 0;
    for (probs) |p| {
        if (p > max_prob) max_prob = p;
    }
    const threshold = min_p * max_prob;
    for (probs) |*p| {
        if (p.* < threshold) p.* = 0;
    }
}

test "initFloatTensorForDType converts float32 data to float16 tensor bytes" {
    const allocator = std.testing.allocator;
    var tensor = try initFloatTensorForDType(allocator, "inputs_embeds", &.{ 1, 2 }, &.{ 1.5, -2.0 }, .f16);
    defer tensor.deinit();

    try std.testing.expectEqual(backends.DType.f16, tensor.dtype);
    const roundtrip = try tensorToOwnedF32(allocator, &tensor);
    defer allocator.free(roundtrip);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), roundtrip[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, -2.0), roundtrip[1], 1e-3);
}

test "initFloatTensorForDType converts float32 data to bfloat16 tensor bytes" {
    const allocator = std.testing.allocator;
    var tensor = try initFloatTensorForDType(allocator, "pixel_values", &.{ 1, 2 }, &.{ 1.25, -0.75 }, .bf16);
    defer tensor.deinit();

    try std.testing.expectEqual(backends.DType.bf16, tensor.dtype);
    const roundtrip = try tensorToOwnedF32(allocator, &tensor);
    defer allocator.free(roundtrip);
    try std.testing.expectApproxEqAbs(@as(f32, 1.25), roundtrip[0], 1e-2);
    try std.testing.expectApproxEqAbs(@as(f32, -0.75), roundtrip[1], 1e-2);
}
