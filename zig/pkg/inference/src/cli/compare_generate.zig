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
const ortgenai = if (build_options.enable_onnx) @import("../backends/ortgenai.zig") else struct {};
const activations = @import("../backends/activations.zig");
const ops = @import("../ops/ops.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const generation = @import("../pipelines/generation.zig");
const gemma3_mm = @import("../pipelines/gemma3_multimodal.zig");
const onnx_decoder_only_vlm = @import("../pipelines/onnx_decoder_only_vlm.zig");
const gemma3_vision = @import("../architectures/gemma3_vision.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const c_file = @import("../util/c_file.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    mlx,
};

const Options = struct {
    native_model_dir: []const u8,
    reference_model_dir: []const u8,
    prompt: []const u8,
    image_paths: [8][]const u8 = .{""} ** 8,
    image_count: usize = 0,
    backend: BackendChoice = .auto,
    top_k: usize = 8,
    no_chat_template: bool = false,
    image_features_only: bool = false,
    onnx_prompt_embeddings_only: bool = false,
};

const PreparedMessages = struct {
    allocator: std.mem.Allocator,
    loaded_images: std.ArrayListUnmanaged([]u8) = .empty,
    message_image_slice: ?[]const []const u8 = null,
    content_part_slice: ?[]const generation.Message.ContentPart = null,
    messages_buf: [1]generation.Message = undefined,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        prompt: []const u8,
        image_paths: []const []const u8,
    ) !PreparedMessages {
        var prepared = PreparedMessages{
            .allocator = allocator,
        };
        errdefer prepared.deinit();

        var message_images = std.ArrayListUnmanaged([]const u8).empty;
        defer message_images.deinit(allocator);
        var content_parts = std.ArrayListUnmanaged(generation.Message.ContentPart).empty;
        defer content_parts.deinit(allocator);

        for (image_paths, 0..) |path, idx| {
            const image_bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
            try prepared.loaded_images.append(allocator, image_bytes);
            try message_images.append(allocator, image_bytes);
            try content_parts.append(allocator, .{ .image = idx });
        }
        if (image_paths.len > 0 and prompt.len > 0) {
            try content_parts.append(allocator, .{ .text = prompt });
        }

        prepared.message_image_slice = if (message_images.items.len > 0)
            try allocator.dupe([]const u8, message_images.items)
        else
            null;
        prepared.content_part_slice = if (content_parts.items.len > 0)
            try allocator.dupe(generation.Message.ContentPart, content_parts.items)
        else
            null;
        prepared.messages_buf = .{.{
            .role = "user",
            .content = prompt,
            .image_bytes = prepared.message_image_slice,
            .content_parts = prepared.content_part_slice,
        }};
        return prepared;
    }

    fn messages(self: *const PreparedMessages) []const generation.Message {
        return self.messages_buf[0..];
    }

    fn deinit(self: *PreparedMessages) void {
        if (self.content_part_slice) |slice| self.allocator.free(slice);
        if (self.message_image_slice) |slice| self.allocator.free(slice);
        for (self.loaded_images.items) |image_bytes| self.allocator.free(image_bytes);
        self.loaded_images.deinit(self.allocator);
        self.* = undefined;
    }
};

const FirstTokenResult = struct {
    backend_name: []const u8,
    rendered_prompt: []u8,
    token_id: i32,
    token_text: []u8,
    finish_reason: []const u8,

    fn deinit(self: *FirstTokenResult, allocator: std.mem.Allocator) void {
        allocator.free(self.rendered_prompt);
        allocator.free(self.token_text);
    }
};

const ExpandedPromptInfo = struct {
    allocator: std.mem.Allocator,
    token_ids: []i64,
    image_offsets: []usize,

    fn deinit(self: *ExpandedPromptInfo) void {
        self.allocator.free(self.token_ids);
        self.allocator.free(self.image_offsets);
    }
};

const NativeAnalysis = struct {
    backend_name: []const u8,
    prompt: []u8,
    prompt_token_ids: []i64,
    rope_layout: gpt_mod.RopeLayout,
    position_encoding: gpt_mod.PositionEncoding,
    rope_theta: f32,
    rope_local_theta: f32,
    rope_freq_scale: f32,
    sliding_window: u32,
    sliding_window_pattern: u32,
    norm_eps: f32,
    norm_weight_offset: f32,
    final_logit_softcapping: f32,
    has_lm_head: bool,
    input_norm_sample: [4]f32,
    q_norm_sample: [4]f32,
    k_norm_sample: [4]f32,
    pre_ffn_norm_sample: [4]f32,
    post_attn_norm_sample: [4]f32,
    post_ffn_norm_sample: [4]f32,
    top1: i32,
    top_logits: []TopLogit,

    fn deinit(self: *NativeAnalysis, allocator: std.mem.Allocator) void {
        allocator.free(self.prompt);
        allocator.free(self.prompt_token_ids);
        allocator.free(self.top_logits);
    }
};

const TopLogit = struct {
    id: i32,
    logit: f32,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);

    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const native_model = try model_manager.loadFromDir(opts.native_model_dir);
    print("native_tokenizer={s}\n", .{
        if (native_model.sp_tok != null) "sentencepiece" else "hf",
    });
    if (opts.image_count > 0) {
        var prepared_messages = try PreparedMessages.init(allocator, io, opts.prompt, opts.image_paths[0..opts.image_count]);
        defer prepared_messages.deinit();
        const rendered_prompt = try renderPromptFromMessages(allocator, native_model, prepared_messages.messages(), opts.no_chat_template);
        defer allocator.free(rendered_prompt);

        print("native_model={s}\n", .{opts.native_model_dir});
        print("reference_model={s}\n", .{opts.reference_model_dir});
        print("native_backend={s}\n", .{@tagName(native_model.session.backend())});
        print("native_prompt:\n{s}\n", .{rendered_prompt});

        if (!build_options.enable_onnx) {
            print("onnx_first_token=unavailable (onnx disabled at build)\n", .{});
            return;
        } else if (!c_file.fileExistsInDir(allocator, opts.reference_model_dir, "genai_config.json") and
            onnx_decoder_only_vlm.isSupportedModelDir(allocator, opts.reference_model_dir))
        {
            const stage_override = platform.env.getenv("TERMITE_COMPARE_NATIVE_VISION_STAGE");
            if (opts.image_features_only and stage_override != null) {
                const native_image_features = try collectNativeVisionStageFeatures(
                    allocator,
                    native_model,
                    prepared_messages.loaded_images.items,
                    stage_override.?,
                );
                defer allocator.free(native_image_features);
                const onnx_image_features = try onnx_decoder_only_vlm.debugImageFeaturesFromDir(
                    allocator,
                    opts.reference_model_dir,
                    prepared_messages.messages(),
                );
                defer allocator.free(onnx_image_features);
                printImageFeatureSummary(native_image_features, onnx_image_features);
                return;
            }
            if (opts.image_features_only) {
                std.debug.print("compare-debug: collect native image features\n", .{});
                const native_image_features = try collectNativeImageFeatures(
                    allocator,
                    native_model,
                    prepared_messages.loaded_images.items,
                );
                defer allocator.free(native_image_features);
                std.debug.print("compare-debug: collect onnx image features\n", .{});
                const onnx_image_features = try onnx_decoder_only_vlm.debugImageFeaturesFromDir(
                    allocator,
                    opts.reference_model_dir,
                    prepared_messages.messages(),
                );
                defer allocator.free(onnx_image_features);
                printImageFeatureSummary(native_image_features, onnx_image_features);
                return;
            }
            std.debug.print("compare-debug: collect onnx prompt embeddings\n", .{});
            const onnx_prompt_embeddings = try onnx_decoder_only_vlm.debugPromptEmbeddingsFromDir(
                allocator,
                opts.reference_model_dir,
                prepared_messages.messages(),
                rendered_prompt,
            );
            defer allocator.free(onnx_prompt_embeddings.token_ids);
            defer allocator.free(onnx_prompt_embeddings.embeds);
            try printPromptTokenIdParity(allocator, native_model, rendered_prompt, prepared_messages.loaded_images.items.len, onnx_prompt_embeddings.token_ids);
            try printPromptEmbeddingSummary(allocator, native_model, onnx_prompt_embeddings.token_ids, onnx_prompt_embeddings.embeds);
            if (opts.onnx_prompt_embeddings_only) {
                if (prepared_messages.loaded_images.items.len > 0) {
                    const onnx_image_features_only = try onnx_decoder_only_vlm.debugImageFeaturesFromDir(
                        allocator,
                        opts.reference_model_dir,
                        prepared_messages.messages(),
                    );
                    defer allocator.free(onnx_image_features_only);
                    var native_from_onnx_image_features_only = try analyzeNativeFirstTokenWithProjectedFeatures(
                        allocator,
                        native_model,
                        rendered_prompt,
                        prepared_messages.loaded_images.items.len,
                        onnx_image_features_only,
                    );
                    defer native_from_onnx_image_features_only.deinit(allocator);
                    print("native_first_token_with_onnx_image_features: id={d} text={s}\n", .{
                        native_from_onnx_image_features_only.token_id,
                        native_from_onnx_image_features_only.token_text,
                    });
                }
                var native_from_onnx_embeds_only_no_mask = try analyzeNativeFirstTokenFromEmbeddingsWithMaskMode(
                    allocator,
                    native_model,
                    rendered_prompt,
                    onnx_prompt_embeddings.token_ids,
                    onnx_prompt_embeddings.embeds,
                    false,
                );
                defer native_from_onnx_embeds_only_no_mask.deinit(allocator);
                print("native_first_token_with_onnx_prompt_embeddings_no_mask: id={d} text={s}\n", .{
                    native_from_onnx_embeds_only_no_mask.token_id,
                    native_from_onnx_embeds_only_no_mask.token_text,
                });
                const native_from_onnx_embed_logits_no_mask = try computeNativeLastLogitsFromEmbeddingsWithMaskMode(
                    allocator,
                    native_model,
                    rendered_prompt,
                    onnx_prompt_embeddings.token_ids,
                    onnx_prompt_embeddings.embeds,
                    false,
                );
                defer allocator.free(native_from_onnx_embed_logits_no_mask);
                print("native_top_logits_with_onnx_prompt_embeddings_no_mask:\n", .{});
                try printTopLogits(allocator, native_model.getTokenizer(), native_from_onnx_embed_logits_no_mask, opts.top_k);
                var onnx_pipeline_only = try onnx_decoder_only_vlm.Pipeline.load(allocator, opts.reference_model_dir);
                defer onnx_pipeline_only.deinit();
                onnx_pipeline_only.prompt_override = rendered_prompt;
                var onnx_result_only = try onnx_pipeline_only.generate(prepared_messages.messages(), .{
                    .max_tokens = 1,
                    .temperature = 0,
                    .top_p = 0,
                    .top_k = 1,
                });
                defer onnx_result_only.deinit();
                const onnx_token_id_only = if (onnx_result_only.token_ids) |ids|
                    if (ids.len > 0) ids[0] else return error.EmptyGeneration
                else
                    return error.MissingTokenIds;
                print("native_prompt == onnx_prompt: {}\n", .{true});
                print("onnx_first_token: id={d} text={s} finish_reason={s}\n", .{
                    onnx_token_id_only,
                    onnx_result_only.text,
                    onnx_result_only.finish_reason,
                });
                return;
            }

            std.debug.print("compare-debug: collect native image features\n", .{});
            const native_image_features = if (stage_override) |stage|
                try collectNativeVisionStageFeatures(allocator, native_model, prepared_messages.loaded_images.items, stage)
            else
                try collectNativeImageFeatures(allocator, native_model, prepared_messages.loaded_images.items);
            defer allocator.free(native_image_features);
            std.debug.print("compare-debug: collect onnx image features\n", .{});
            const onnx_image_features = try onnx_decoder_only_vlm.debugImageFeaturesFromDir(
                allocator,
                opts.reference_model_dir,
                prepared_messages.messages(),
            );
            defer allocator.free(onnx_image_features);
            printImageFeatureSummary(native_image_features, onnx_image_features);
            if (opts.image_features_only) return;
            var native_first = try analyzeNativeFirstTokenMultimodalWithPrompt(allocator, native_model, try allocator.dupe(u8, rendered_prompt), prepared_messages.messages());
            defer native_first.deinit(allocator);
            print("native_first_token: id={d} text={s} finish_reason={s}\n", .{
                native_first.token_id,
                native_first.token_text,
                native_first.finish_reason,
            });
            var native_from_native = try analyzeNativeFirstTokenWithProjectedFeatures(
                allocator,
                native_model,
                rendered_prompt,
                prepared_messages.loaded_images.items.len,
                native_image_features,
            );
            defer native_from_native.deinit(allocator);
            print("native_first_token_with_native_image_features: id={d} text={s}\n", .{
                native_from_native.token_id,
                native_from_native.token_text,
            });
            var native_from_onnx = try analyzeNativeFirstTokenWithProjectedFeatures(
                allocator,
                native_model,
                rendered_prompt,
                prepared_messages.loaded_images.items.len,
                onnx_image_features,
            );
            defer native_from_onnx.deinit(allocator);
            print("native_first_token_with_onnx_image_features: id={d} text={s}\n", .{
                native_from_onnx.token_id,
                native_from_onnx.token_text,
            });
            var native_from_onnx_embeds = try analyzeNativeFirstTokenFromEmbeddings(
                allocator,
                native_model,
                rendered_prompt,
                onnx_prompt_embeddings.token_ids,
                onnx_prompt_embeddings.embeds,
            );
            defer native_from_onnx_embeds.deinit(allocator);
            print("native_first_token_with_onnx_prompt_embeddings: id={d} text={s}\n", .{
                native_from_onnx_embeds.token_id,
                native_from_onnx_embeds.token_text,
            });
            var onnx_pipeline = try onnx_decoder_only_vlm.Pipeline.load(allocator, opts.reference_model_dir);
            defer onnx_pipeline.deinit();
            onnx_pipeline.prompt_override = rendered_prompt;
            var onnx_result = try onnx_pipeline.generate(prepared_messages.messages(), .{
                .max_tokens = 1,
                .temperature = 0,
                .top_p = 0,
                .top_k = 1,
            });
            defer onnx_result.deinit();
            const onnx_token_id = if (onnx_result.token_ids) |ids|
                if (ids.len > 0) ids[0] else return error.EmptyGeneration
            else
                return error.MissingTokenIds;
            print("native_prompt == onnx_prompt: {}\n", .{true});
            print("onnx_first_token: id={d} text={s} finish_reason={s}\n", .{
                onnx_token_id,
                onnx_result.text,
                onnx_result.finish_reason,
            });
            return;
        }

        const reference_model = try model_manager.loadFromDir(opts.reference_model_dir);
        var native_first = try analyzeNativeFirstTokenMultimodalWithPrompt(allocator, native_model, try allocator.dupe(u8, rendered_prompt), prepared_messages.messages());
        defer native_first.deinit(allocator);
        print("native_first_token: id={d} text={s} finish_reason={s}\n", .{
            native_first.token_id,
            native_first.token_text,
            native_first.finish_reason,
        });
        var reference_first = try analyzeNativeFirstTokenMultimodalWithPrompt(allocator, reference_model, try allocator.dupe(u8, rendered_prompt), prepared_messages.messages());
        defer reference_first.deinit(allocator);
        print("native_prompt == reference_prompt: {}\n", .{std.mem.eql(u8, rendered_prompt, reference_first.rendered_prompt)});
        print("reference_backend={s}\n", .{reference_first.backend_name});
        print("reference_first_token: id={d} text={s} finish_reason={s}\n", .{
            reference_first.token_id,
            reference_first.token_text,
            reference_first.finish_reason,
        });
        return;
    }

    var native = try analyzeNativeModel(allocator, native_model, opts.prompt, opts.no_chat_template, opts.top_k);
    defer native.deinit(allocator);

    print("native_model={s}\n", .{opts.native_model_dir});
    print("reference_model={s}\n", .{opts.reference_model_dir});
    print("native_backend={s}\n", .{native.backend_name});
    print("native_rope_layout={s}\n", .{@tagName(native.rope_layout)});
    print("native_position_encoding={s}\n", .{@tagName(native.position_encoding)});
    print("native_rope_theta={d:.6} local={d:.6} freq_scale={d:.6}\n", .{ native.rope_theta, native.rope_local_theta, native.rope_freq_scale });
    print("native_sliding_window={d} pattern={d} norm_eps={d:.8} norm_offset={d:.6} softcap={d:.6}\n", .{
        native.sliding_window,
        native.sliding_window_pattern,
        native.norm_eps,
        native.norm_weight_offset,
        native.final_logit_softcapping,
    });
    print("native_has_lm_head={}\n", .{native.has_lm_head});
    printWeightSamples("native", native);
    print("native_prompt_token_ids:", .{});
    for (native.prompt_token_ids) |id| print(" {d}", .{id});
    print("\n", .{});
    print("native_top_logits:\n", .{});
    try printTopLogitsFromEntries(allocator, native_model.getTokenizer(), native.top_logits);
    try printSingleToken(allocator, "native_top1", native_model.getTokenizer(), native.top1);

    if (!build_options.enable_onnx) {
        print("onnx_first_token=unavailable (onnx disabled at build)\n", .{});
    } else if (!c_file.fileExistsInDir(allocator, opts.reference_model_dir, "genai_config.json") and
        onnx_decoder_only_vlm.isSupportedModelDir(allocator, opts.reference_model_dir))
    {
        var onnx_pipeline = try onnx_decoder_only_vlm.Pipeline.load(allocator, opts.reference_model_dir);
        defer onnx_pipeline.deinit();
        const onnx_prompt = try alignOnnxPromptForCompare(
            allocator,
            native_model,
            onnx_pipeline.manifest.bos_token,
            onnx_pipeline.manifest.add_bos_token,
            native.prompt,
        );
        defer allocator.free(onnx_prompt);
        const onnx_messages = [_]generation.Message{
            .{ .role = "user", .content = opts.prompt },
        };
        const onnx_prompt_embeddings = try onnx_decoder_only_vlm.debugPromptEmbeddingsFromDir(
            allocator,
            opts.reference_model_dir,
            &onnx_messages,
            onnx_prompt,
        );
        defer allocator.free(onnx_prompt_embeddings.token_ids);
        defer allocator.free(onnx_prompt_embeddings.embeds);
        try printPromptTokenIdParity(allocator, native_model, onnx_prompt, 0, onnx_prompt_embeddings.token_ids);
        try printPromptEmbeddingSummary(allocator, native_model, onnx_prompt_embeddings.token_ids, onnx_prompt_embeddings.embeds);
        print("native_prompt == onnx_prompt: {}\n", .{std.mem.eql(u8, native.prompt, onnx_prompt)});
        const onnx_first = try onnx_pipeline.firstTokenDebug(onnx_prompt);
        defer allocator.free(onnx_first.text);
        print("onnx_first_token: id={d} text={s}\n", .{ onnx_first.token_id, onnx_first.text });
        return;
    } else if (try ortgenai.prepareGenerativeModelPackage(allocator, opts.reference_model_dir)) |onnx_model_dir| {
        defer allocator.free(onnx_model_dir);
        const onnx_prompt = try allocator.dupe(u8, native.prompt);
        defer allocator.free(onnx_prompt);
        print("native_prompt == onnx_prompt: {}\n", .{true});

        var onnx_gen = try ortgenai.GenAiModel.load(allocator, onnx_model_dir);
        defer onnx_gen.deinit();
        var onnx_first = try ortgenai.generateFirstTokenDebug(allocator, &onnx_gen, onnx_prompt, .{});
        defer onnx_first.deinit();
        print("onnx_first_token: id={d} text={s}\n", .{ onnx_first.token_id, onnx_first.text });
        return;
    }

    const reference_model = try model_manager.loadFromDir(opts.reference_model_dir);
    var reference = try analyzeNativeModel(allocator, reference_model, opts.prompt, opts.no_chat_template, opts.top_k);
    defer reference.deinit(allocator);

    print("reference_backend={s}\n", .{reference.backend_name});
    print("reference_rope_layout={s}\n", .{@tagName(reference.rope_layout)});
    print("reference_position_encoding={s}\n", .{@tagName(reference.position_encoding)});
    print("reference_rope_theta={d:.6} local={d:.6} freq_scale={d:.6}\n", .{ reference.rope_theta, reference.rope_local_theta, reference.rope_freq_scale });
    print("reference_sliding_window={d} pattern={d} norm_eps={d:.8} norm_offset={d:.6} softcap={d:.6}\n", .{
        reference.sliding_window,
        reference.sliding_window_pattern,
        reference.norm_eps,
        reference.norm_weight_offset,
        reference.final_logit_softcapping,
    });
    print("reference_has_lm_head={}\n", .{reference.has_lm_head});
    printWeightSamples("reference", reference);
    print("native_prompt == reference_prompt: {}\n", .{std.mem.eql(u8, native.prompt, reference.prompt)});
    if (!std.mem.eql(u8, native.prompt, reference.prompt)) {
        print("native_prompt:\n{s}\n", .{native.prompt});
        print("reference_prompt:\n{s}\n", .{reference.prompt});
    }
    print("reference_prompt_token_ids:", .{});
    for (reference.prompt_token_ids) |id| print(" {d}", .{id});
    print("\n", .{});
    print("reference_top_logits:\n", .{});
    try printTopLogitsFromEntries(allocator, reference_model.getTokenizer(), reference.top_logits);
    try printSingleToken(allocator, "reference_top1", reference_model.getTokenizer(), reference.top1);
}

fn hasOnnxPayload(io: std.Io, model_dir: []const u8) !bool {
    const Dir = std.Io.Dir;
    var dir = Dir.cwd().openDir(io, model_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer dir.close(io);
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind == .file) {
            if (std.mem.eql(u8, entry.name, "genai_config.json")) return true;
            if (std.mem.endsWith(u8, entry.name, ".onnx")) return true;
            continue;
        }
        if (entry.kind != .directory) continue;
        if (!std.mem.eql(u8, entry.name, "onnx")) continue;
        var sub = try dir.openDir(io, entry.name, .{ .iterate = true });
        defer sub.close(io);
        var sub_it = sub.iterate();
        while (try sub_it.next(io)) |sub_entry| {
            if (sub_entry.kind != .file) continue;
            if (std.mem.endsWith(u8, sub_entry.name, ".onnx")) return true;
        }
    }
    return false;
}

fn renderPrompt(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    prompt: []const u8,
    no_chat_template: bool,
) ![]u8 {
    if (no_chat_template) {
        return allocator.dupe(u8, prompt);
    }
    const messages = [_]generation.Message{
        .{ .role = "user", .content = prompt },
    };
    if (model.chat_tmpl != null) {
        return model.chat_tmpl.?.apply(allocator, &messages, true);
    }
    return generation.formatMessages(allocator, &messages);
}

fn renderPromptFromMessages(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    messages: []const generation.Message,
    no_chat_template: bool,
) ![]u8 {
    if (!no_chat_template and model.chat_tmpl != null) {
        return model.chat_tmpl.?.apply(allocator, messages, true);
    }
    return generation.formatMessages(allocator, messages);
}

fn alignOnnxPromptForCompare(
    allocator: std.mem.Allocator,
    native_model: *model_manager_mod.LoadedModel,
    onnx_bos_token: []const u8,
    onnx_add_bos_token: bool,
    prompt: []const u8,
) ![]u8 {
    if (!native_model.manifest.add_bos_token or native_model.manifest.bos_token.len == 0) {
        return allocator.dupe(u8, prompt);
    }
    if (onnx_add_bos_token) {
        return allocator.dupe(u8, prompt);
    }
    if (std.mem.startsWith(u8, prompt, native_model.manifest.bos_token)) {
        return allocator.dupe(u8, prompt);
    }
    if (onnx_bos_token.len > 0 and std.mem.startsWith(u8, prompt, onnx_bos_token)) {
        return allocator.dupe(u8, prompt);
    }
    return std.fmt.allocPrint(allocator, "{s}{s}", .{ native_model.manifest.bos_token, prompt });
}

fn printTokenizationSummary(
    allocator: std.mem.Allocator,
    label: []const u8,
    tok: @import("inference_tokenizer").Tokenizer,
    add_bos: bool,
    bos_token: []const u8,
    prompt: []const u8,
) !void {
    var encoded = try generation.encodePromptForGeneration(tok, allocator, prompt, 4096, add_bos, bos_token);
    defer encoded.deinit();
    const prompt_tokens = countPromptTokens(encoded.attention_mask);
    print("{s}_prompt_tokens={d}\n", .{ label, prompt_tokens });
    if (prompt_tokens > 0) {
        const limit = @min(prompt_tokens, 24);
        print("{s}_prompt_token_ids:", .{label});
        for (encoded.ids[0..limit]) |id| print(" {d}", .{id});
        print("\n", .{});
    }
}

fn analyzeNativeModel(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    prompt: []const u8,
    no_chat_template: bool,
    top_k: usize,
) !NativeAnalysis {
    const rendered_prompt = try renderPrompt(allocator, model, prompt, no_chat_template);
    errdefer allocator.free(rendered_prompt);

    const tok = model.getTokenizer();
    const cfg = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();
    const has_lm_head = blk: {
        const lm = cb.getWeight("lm_head.weight") catch break :blk false;
        cb.free(lm);
        break :blk true;
    };
    const input_norm_sample = try loadWeightPrefix(&cb, allocator, "model.layers.0.input_layernorm.weight");
    const q_norm_sample = try loadWeightPrefix(&cb, allocator, "model.layers.0.self_attn.q_norm.weight");
    const k_norm_sample = try loadWeightPrefix(&cb, allocator, "model.layers.0.self_attn.k_norm.weight");
    const pre_ffn_norm_sample = try loadWeightPrefix(&cb, allocator, "model.layers.0.pre_feedforward_layernorm.weight");
    const post_attn_norm_sample = try loadWeightPrefix(&cb, allocator, "model.layers.0.post_attention_layernorm.weight");
    const post_ffn_norm_sample = try loadWeightPrefix(&cb, allocator, "model.layers.0.post_feedforward_layernorm.weight");

    var encoded = try generation.encodePromptForGeneration(
        tok,
        allocator,
        rendered_prompt,
        4096,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer encoded.deinit();
    const prompt_tokens = countPromptTokens(encoded.attention_mask);
    if (prompt_tokens == 0) return error.EmptyPrompt;

    var input_ids = try allocator.alloc(i64, prompt_tokens);
    errdefer allocator.free(input_ids);
    for (0..prompt_tokens) |i| input_ids[i] = encoded.ids[i];

    const logits = try gpt_arch.forward(&cb, allocator, cfg, input_ids, 1, prompt_tokens, null);
    defer allocator.free(logits);
    const vocab = cfg.vocab_size;
    const last_logits = logits[(prompt_tokens - 1) * vocab ..][0..vocab];
    const top1: i32 = @intCast(activations.argmax(last_logits));
    const top_logits = try collectTopLogits(allocator, last_logits, top_k);

    return .{
        .backend_name = @tagName(model.session.backend()),
        .prompt = rendered_prompt,
        .prompt_token_ids = input_ids,
        .rope_layout = cfg.rope_layout,
        .position_encoding = cfg.position_encoding,
        .rope_theta = cfg.rope_theta,
        .rope_local_theta = cfg.rope_local_theta,
        .rope_freq_scale = cfg.rope_freq_scale,
        .sliding_window = cfg.sliding_window,
        .sliding_window_pattern = cfg.sliding_window_pattern,
        .norm_eps = cfg.norm_eps,
        .norm_weight_offset = cfg.norm_weight_offset,
        .final_logit_softcapping = cfg.final_logit_softcapping,
        .has_lm_head = has_lm_head,
        .input_norm_sample = input_norm_sample,
        .q_norm_sample = q_norm_sample,
        .k_norm_sample = k_norm_sample,
        .pre_ffn_norm_sample = pre_ffn_norm_sample,
        .post_attn_norm_sample = post_attn_norm_sample,
        .post_ffn_norm_sample = post_ffn_norm_sample,
        .top1 = top1,
        .top_logits = top_logits,
    };
}

fn analyzeNativeFirstTokenMultimodal(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    no_chat_template: bool,
    messages: []const generation.Message,
) !FirstTokenResult {
    const rendered_prompt = try renderPromptFromMessages(allocator, model, messages, no_chat_template);
    return analyzeNativeFirstTokenMultimodalWithPrompt(allocator, model, rendered_prompt, messages);
}

fn analyzeNativeFirstTokenMultimodalWithPrompt(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []u8,
    messages: []const generation.Message,
) !FirstTokenResult {
    errdefer allocator.free(rendered_prompt);

    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = allocator,
        .cb = cb,
        .gpt_config = gpt_config,
        .tokenizer = model.getTokenizer(),
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .prompt_override = rendered_prompt,
        .model_dir = model.model_dir,
        .gguf_projector_path = model.manifest.gguf_projector_path,
    };
    var result = try pipeline.generate(messages, .{
        .max_tokens = 1,
        .temperature = 0,
        .top_p = 0,
        .top_k = 1,
    });
    defer result.deinit();

    const token_id = if (result.token_ids) |ids|
        if (ids.len > 0) ids[0] else return error.EmptyGeneration
    else
        return error.MissingTokenIds;
    const token_text = try allocator.dupe(u8, result.text);
    return .{
        .backend_name = @tagName(model.session.backend()),
        .rendered_prompt = rendered_prompt,
        .token_id = token_id,
        .token_text = token_text,
        .finish_reason = result.finish_reason,
    };
}

fn collectNativeImageFeatures(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    images: []const []const u8,
) ![]f32 {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();

    const pre_cfg = try gemma3_mm.loadPreprocessorConfig(allocator, model.model_dir);
    const pixels_per_image = 3 * pre_cfg.image_size * pre_cfg.image_size;
    const pixel_values = try allocator.alloc(f32, images.len * pixels_per_image);
    defer allocator.free(pixel_values);

    for (images, 0..) |image_bytes, idx| {
        const processed = try gemma3_mm.preprocessImage(allocator, image_bytes, pre_cfg);
        defer allocator.free(processed);
        @memcpy(pixel_values[idx * pixels_per_image ..][0..pixels_per_image], processed);
    }

    return gemma3_vision.encodeProjectedImageTokens(&cb, allocator, gpt_config, pixel_values, images.len);
}

fn collectNativeVisionStageFeatures(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    images: []const []const u8,
    stage: []const u8,
) ![]f32 {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();

    const pre_cfg = try gemma3_mm.loadPreprocessorConfig(allocator, model.model_dir);
    const pixels_per_image = 3 * pre_cfg.image_size * pre_cfg.image_size;
    const pixel_values = try allocator.alloc(f32, images.len * pixels_per_image);
    defer allocator.free(pixel_values);

    for (images, 0..) |image_bytes, idx| {
        const processed = try gemma3_mm.preprocessImage(allocator, image_bytes, pre_cfg);
        defer allocator.free(processed);
        @memcpy(pixel_values[idx * pixels_per_image ..][0..pixels_per_image], processed);
    }

    var debug_outputs = try gemma3_vision.encodeProjectedImageTokensDebug(&cb, allocator, gpt_config, pixel_values, images.len);
    defer debug_outputs.deinit();

    if (std.mem.eql(u8, stage, "patch")) return allocator.dupe(f32, debug_outputs.patch_tokens);
    if (std.mem.eql(u8, stage, "positioned")) return allocator.dupe(f32, debug_outputs.positioned_tokens);
    if (std.mem.eql(u8, stage, "pooled")) return allocator.dupe(f32, debug_outputs.pooled_tokens);
    if (std.mem.eql(u8, stage, "softnorm")) return allocator.dupe(f32, debug_outputs.soft_normed_tokens);
    if (std.mem.eql(u8, stage, "projected")) return allocator.dupe(f32, debug_outputs.projected_tokens);
    return error.InvalidDebugStage;
}

fn analyzeNativeFirstTokenWithProjectedFeatures(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    image_count: usize,
    projected_features: []const f32,
) !FirstTokenResult {
    const last_logits = try computeNativeLastLogitsWithProjectedFeatures(
        allocator,
        model,
        rendered_prompt,
        image_count,
        projected_features,
    );
    defer allocator.free(last_logits);
    const token_id: i32 = @intCast(activations.argmax(last_logits));
    const one = [_]i32{token_id};
    const token_text = try model.getTokenizer().decode(allocator, &one);
    return .{
        .backend_name = @tagName(model.session.backend()),
        .rendered_prompt = try allocator.dupe(u8, rendered_prompt),
        .token_id = token_id,
        .token_text = token_text,
        .finish_reason = "length",
    };
}

fn computeNativeLastLogitsWithProjectedFeatures(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    image_count: usize,
    projected_features: []const f32,
) ![]f32 {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();

    var expanded = try buildExpandedPromptInfo(
        allocator,
        model.getTokenizer(),
        model.manifest.add_bos_token,
        model.manifest.bos_token,
        rendered_prompt,
        gpt_config,
        image_count,
    );
    defer expanded.deinit();

    const hidden_size = gpt_config.hidden_size;
    const expected_features = image_count * @as(usize, gpt_config.mm_tokens_per_image) * hidden_size;
    if (projected_features.len != expected_features) return error.ImageFeatureCountMismatch;

    const embed_w = try switch (gpt_config.family) {
        .gpt2 => cb.getWeight("wte.weight"),
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
        else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
    defer cb.free(embed_w);
    const base_embeddings = try cb.embeddingLookup(embed_w, expanded.token_ids, expanded.token_ids.len, hidden_size);
    defer cb.free(base_embeddings);
    const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
    defer allocator.free(prompt_embeddings);

    const embedding_scale = gpt_config.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6)) {
        for (prompt_embeddings) |*value| value.* *= embedding_scale;
    }

    const tokens_per_image: usize = gpt_config.mm_tokens_per_image;
    for (expanded.image_offsets, 0..) |offset, idx| {
        const dst = offset * hidden_size;
        const src = idx * tokens_per_image * hidden_size;
        @memcpy(
            prompt_embeddings[dst..][0 .. tokens_per_image * hidden_size],
            projected_features[src..][0 .. tokens_per_image * hidden_size],
        );
    }

    const embedding_shape = [_]i32{ @intCast(expanded.token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);

    const logits = try gpt_arch.forwardFromEmbeddings(
        &cb,
        allocator,
        gpt_config,
        input_embeddings,
        1,
        expanded.token_ids.len,
        null,
        null, // PLE vectors (multimodal path, not yet supported)
    );
    defer allocator.free(logits);

    const vocab = gpt_config.vocab_size;
    return try allocator.dupe(f32, logits[(expanded.token_ids.len - 1) * vocab ..][0..vocab]);
}

fn analyzeNativeFirstTokenFromEmbeddings(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    token_ids: []const i64,
    prompt_embeddings: []const f32,
) !FirstTokenResult {
    const last_logits = try computeNativeLastLogitsFromEmbeddings(
        allocator,
        model,
        rendered_prompt,
        token_ids,
        prompt_embeddings,
    );
    defer allocator.free(last_logits);
    const token_id: i32 = @intCast(activations.argmax(last_logits));
    const one = [_]i32{token_id};
    const token_text = try model.getTokenizer().decode(allocator, &one);
    return .{
        .backend_name = @tagName(model.session.backend()),
        .rendered_prompt = try allocator.dupe(u8, rendered_prompt),
        .token_id = token_id,
        .token_text = token_text,
        .finish_reason = "length",
    };
}

fn analyzeNativeFirstTokenFromEmbeddingsWithMaskMode(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    token_ids: []const i64,
    prompt_embeddings: []const f32,
    use_multimodal_mask: bool,
) !FirstTokenResult {
    const last_logits = try computeNativeLastLogitsFromEmbeddingsWithMaskMode(
        allocator,
        model,
        rendered_prompt,
        token_ids,
        prompt_embeddings,
        use_multimodal_mask,
    );
    defer allocator.free(last_logits);
    const token_id: i32 = @intCast(activations.argmax(last_logits));
    const one = [_]i32{token_id};
    const token_text = try model.getTokenizer().decode(allocator, &one);
    return .{
        .backend_name = @tagName(model.session.backend()),
        .rendered_prompt = try allocator.dupe(u8, rendered_prompt),
        .token_id = token_id,
        .token_text = token_text,
        .finish_reason = "length",
    };
}

fn computeNativeLastLogitsFromEmbeddings(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    token_ids: []const i64,
    prompt_embeddings: []const f32,
) ![]f32 {
    return computeNativeLastLogitsFromEmbeddingsWithMaskMode(
        allocator,
        model,
        rendered_prompt,
        token_ids,
        prompt_embeddings,
        true,
    );
}

fn computeNativeLastLogitsFromEmbeddingsWithMaskMode(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    token_ids: []const i64,
    prompt_embeddings: []const f32,
    use_multimodal_mask: bool,
) ![]f32 {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var effective_config = gpt_config;
    if (platform.env.getenv("TERMITE_COMPARE_ROPE_FREQ_SCALE")) |value| {
        effective_config.rope_freq_scale = std.fmt.parseFloat(f32, value) catch effective_config.rope_freq_scale;
        std.debug.print("compare-debug: override rope_freq_scale={d:.6}\n", .{effective_config.rope_freq_scale});
    }
    const hidden_size = effective_config.hidden_size;
    if (token_ids.len == 0) return error.EmptyPrompt;
    if (prompt_embeddings.len != token_ids.len * hidden_size) return error.InvalidEmbeddingShape;

    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();
    var decode_state = generation.NativeDecodeState.initContiguous(allocator);
    defer decode_state.deinit();
    try decode_state.notePrefill(token_ids.len);

    const embedding_shape = [_]i32{ @intCast(token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    var decode_context = decode_state.gptDecodeContext(token_ids.len, token_ids.len);
    if (use_multimodal_mask) {
        decode_context.attn_or_mask = try gemma3_mm.buildImageAttentionOrMaskFromExpandedTokens(allocator, token_ids, gpt_config);
        defer if (decode_context.attn_or_mask) |mask| allocator.free(mask);
    } else {
        decode_context.attn_or_mask = null;
    }
    const logits = try gpt_arch.forwardFromEmbeddings(
        &cb,
        allocator,
        effective_config,
        input_embeddings,
        1,
        token_ids.len,
        &decode_context,
        null, // PLE vectors (multimodal path, not yet supported)
    );
    defer allocator.free(logits);

    const vocab = effective_config.vocab_size;
    _ = rendered_prompt;
    return try allocator.dupe(f32, logits[(token_ids.len - 1) * vocab ..][0..vocab]);
}

fn buildExpandedPromptInfo(
    allocator: std.mem.Allocator,
    tok: @import("inference_tokenizer").Tokenizer,
    add_bos_token: bool,
    bos_token: []const u8,
    rendered_prompt: []const u8,
    config: gpt_mod.Config,
    image_count: usize,
) !ExpandedPromptInfo {
    const expanded_prompt = try gemma3_mm.expandPromptText(allocator, rendered_prompt, config, image_count);
    defer allocator.free(expanded_prompt);
    var encoded = try generation.encodePromptForGeneration(
        tok,
        allocator,
        expanded_prompt,
        4096,
        add_bos_token,
        bos_token,
    );
    defer encoded.deinit();

    var prompt_tokens: usize = 0;
    while (prompt_tokens < encoded.attention_mask.len and encoded.attention_mask[prompt_tokens] != 0) : (prompt_tokens += 1) {}
    if (prompt_tokens == 0) return error.EmptyPrompt;

    var token_ids = try allocator.alloc(i64, prompt_tokens);
    errdefer allocator.free(token_ids);
    for (0..prompt_tokens) |idx| token_ids[idx] = encoded.ids[idx];

    var image_offsets = try allocator.alloc(usize, image_count);
    errdefer allocator.free(image_offsets);

    var soft_token_count: usize = 0;
    var image_idx: usize = 0;
    var run_start: ?usize = null;
    for (token_ids, 0..) |token_id, idx| {
        if (token_id == config.image_token_index) {
            soft_token_count += 1;
            if (run_start == null) run_start = idx;
        } else if (run_start) |start| {
            if (idx - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
            if (image_idx >= image_count) return error.ImagePlaceholderCountMismatch;
            image_offsets[image_idx] = start;
            image_idx += 1;
            run_start = null;
        }
    }
    if (run_start) |start| {
        if (token_ids.len - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
        if (image_idx >= image_count) return error.ImagePlaceholderCountMismatch;
        image_offsets[image_idx] = start;
        image_idx += 1;
    }
    if (soft_token_count != image_count * config.mm_tokens_per_image or image_idx != image_count) {
        return error.ImagePlaceholderCountMismatch;
    }

    return .{
        .allocator = allocator,
        .token_ids = token_ids,
        .image_offsets = image_offsets,
    };
}

fn printImageFeatureSummary(native_features: []const f32, onnx_features: []const f32) void {
    const sample = @min(@min(native_features.len, onnx_features.len), 8);
    print("native_image_features_sample:", .{});
    for (native_features[0..sample]) |value| print(" {d:.6}", .{value});
    print("\n", .{});
    print("onnx_image_features_sample:", .{});
    for (onnx_features[0..sample]) |value| print(" {d:.6}", .{value});
    print("\n", .{});

    const count = @min(native_features.len, onnx_features.len);
    if (count == 0) {
        print("image_feature_mean_abs_diff=nan native_count={d} onnx_count={d}\n", .{ native_features.len, onnx_features.len });
        return;
    }
    var total_abs_diff: f64 = 0;
    for (native_features[0..count], onnx_features[0..count]) |lhs, rhs| {
        total_abs_diff += @abs(@as(f64, lhs) - @as(f64, rhs));
    }
    const mean_abs_diff = total_abs_diff / @as(f64, @floatFromInt(count));
    print("image_feature_mean_abs_diff={d:.6} native_count={d} onnx_count={d}\n", .{ mean_abs_diff, native_features.len, onnx_features.len });

    if (native_features.len == onnx_features.len and native_features.len > 0) {
        const tokens = 256;
        if (native_features.len % tokens == 0) {
            const hidden = native_features.len / tokens;
            const side = 16;
            if (hidden > 0 and side * side == tokens) {
                var transpose_abs_diff: f64 = 0;
                for (0..side) |y| {
                    for (0..side) |x| {
                        const native_token = y * side + x;
                        const transposed_token = x * side + y;
                        const native_base = native_token * hidden;
                        const onnx_base = transposed_token * hidden;
                        for (0..hidden) |i| {
                            transpose_abs_diff += @abs(@as(f64, native_features[native_base + i]) - @as(f64, onnx_features[onnx_base + i]));
                        }
                    }
                }
                const transpose_mean_abs_diff = transpose_abs_diff / @as(f64, @floatFromInt(native_features.len));
                print("image_feature_mean_abs_diff_grid_transpose={d:.6}\n", .{transpose_mean_abs_diff});
            }
        }
    }
}

fn printPromptEmbeddingSummary(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    token_ids: []const i64,
    onnx_embeddings: []const f32,
) !void {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const hidden_size = gpt_config.hidden_size;
    if (token_ids.len == 0 or onnx_embeddings.len != token_ids.len * hidden_size) return;

    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();

    const embed_w = try switch (gpt_config.family) {
        .gpt2 => cb.getWeight("wte.weight"),
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
        else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
    defer cb.free(embed_w);

    const base_embeddings = try cb.embeddingLookup(embed_w, token_ids, token_ids.len, hidden_size);
    defer cb.free(base_embeddings);
    const native_raw = try cb.toFloat32(base_embeddings, allocator);
    defer allocator.free(native_raw);

    const embedding_scale = gpt_config.tokenEmbeddingScale();
    var raw_accum: f64 = 0;
    var scaled_accum: f64 = 0;
    var compare_count: usize = 0;
    for (token_ids, 0..) |token_id, pos| {
        if (token_id == gpt_config.image_token_index) continue;
        const row_start = pos * hidden_size;
        for (0..hidden_size) |col| {
            const idx = row_start + col;
            const onnx_value = onnx_embeddings[idx];
            const raw_value = native_raw[idx];
            raw_accum += @abs(@as(f64, raw_value) - @as(f64, onnx_value));
            scaled_accum += @abs(@as(f64, raw_value * embedding_scale) - @as(f64, onnx_value));
        }
        compare_count += hidden_size;
    }
    if (compare_count == 0) return;
    print("text_prompt_embedding_mean_abs_diff_raw={d:.6} scaled={d:.6} count={d}\n", .{
        raw_accum / @as(f64, @floatFromInt(compare_count)),
        scaled_accum / @as(f64, @floatFromInt(compare_count)),
        compare_count,
    });
}

fn printPromptTokenIdParity(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    image_count: usize,
    onnx_token_ids: []const i64,
) !void {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var expanded = buildExpandedPromptInfo(
        allocator,
        model.getTokenizer(),
        model.manifest.add_bos_token,
        model.manifest.bos_token,
        rendered_prompt,
        gpt_config,
        image_count,
    ) catch |err| {
        if (err == error.ImagePlaceholderCountMismatch) {
            try printExpandedPromptDebug(allocator, model, rendered_prompt, image_count, gpt_config);
        }
        return err;
    };
    defer expanded.deinit();

    const same_len = expanded.token_ids.len == onnx_token_ids.len;
    var mismatch_idx: ?usize = null;
    const compare_len = @min(expanded.token_ids.len, onnx_token_ids.len);
    for (0..compare_len) |idx| {
        if (expanded.token_ids[idx] != onnx_token_ids[idx]) {
            mismatch_idx = idx;
            break;
        }
    }
    print("prompt_token_ids_match: {} native_count={d} onnx_count={d}", .{ same_len and mismatch_idx == null, expanded.token_ids.len, onnx_token_ids.len });
    if (mismatch_idx) |idx| {
        print(" first_mismatch={d} native={d} onnx={d}", .{ idx, expanded.token_ids[idx], onnx_token_ids[idx] });
    } else if (!same_len) {
        print(" first_mismatch={d} native={s} onnx={s}", .{
            compare_len,
            if (expanded.token_ids.len > compare_len) "extra" else "eof",
            if (onnx_token_ids.len > compare_len) "extra" else "eof",
        });
    }
    print("\n", .{});
    if (mismatch_idx) |idx| {
        try printTokenIdWindow(allocator, "native_prompt_mismatch_window", model.getTokenizer(), expanded.token_ids, idx);
        try printTokenIdWindow(allocator, "onnx_prompt_mismatch_window", model.getTokenizer(), onnx_token_ids, idx);
    }

    const native_counts = countSpecialPromptTokens(expanded.token_ids, gpt_config);
    const onnx_counts = countSpecialPromptTokens(onnx_token_ids, gpt_config);
    print("prompt_special_counts native(boi={d} image={d} eoi={d}) onnx(boi={d} image={d} eoi={d})\n", .{
        native_counts.boi,
        native_counts.image,
        native_counts.eoi,
        onnx_counts.boi,
        onnx_counts.image,
        onnx_counts.eoi,
    });
}

fn printExpandedPromptDebug(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
    image_count: usize,
    config: gpt_mod.Config,
) !void {
    const expanded_prompt = try gemma3_mm.expandPromptText(allocator, rendered_prompt, config, image_count);
    defer allocator.free(expanded_prompt);
    print("expanded_prompt_debug:\n{s}\n", .{expanded_prompt});

    var encoded = try generation.encodePromptForGeneration(
        model.getTokenizer(),
        allocator,
        expanded_prompt,
        4096,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer encoded.deinit();

    var prompt_tokens: usize = 0;
    while (prompt_tokens < encoded.attention_mask.len and encoded.attention_mask[prompt_tokens] != 0) : (prompt_tokens += 1) {}
    print("expanded_prompt_token_count={d}\n", .{prompt_tokens});

    var boi: usize = 0;
    var image: usize = 0;
    var eoi: usize = 0;
    print("expanded_prompt_token_ids:", .{});
    const limit = @min(prompt_tokens, 64);
    for (0..limit) |idx| {
        const id = encoded.ids[idx];
        if (id == config.boi_token_index) boi += 1;
        if (id == config.image_token_index) image += 1;
        if (id == config.eoi_token_index) eoi += 1;
        print(" {d}", .{id});
    }
    print("\n", .{});
    for (limit..prompt_tokens) |idx| {
        const id = encoded.ids[idx];
        if (id == config.boi_token_index) boi += 1;
        if (id == config.image_token_index) image += 1;
        if (id == config.eoi_token_index) eoi += 1;
    }
    print("expanded_prompt_special_counts boi={d} image={d} eoi={d}\n", .{ boi, image, eoi });
}

fn printTokenIdWindow(
    allocator: std.mem.Allocator,
    label: []const u8,
    tok: @import("inference_tokenizer").Tokenizer,
    token_ids: []const i64,
    center: usize,
) !void {
    const start = center -| 4;
    const end = @min(token_ids.len, center + 5);
    print("{s}:\n", .{label});
    for (start..end) |idx| {
        const token_id: i32 = @intCast(token_ids[idx]);
        const one = [_]i32{token_id};
        const piece = tok.decode(allocator, &one) catch try allocator.dupe(u8, "");
        defer allocator.free(piece);
        print("  [{d}] id={d} text={s}\n", .{ idx, token_id, piece });
    }
}

const PromptSpecialCounts = struct { boi: usize, image: usize, eoi: usize };

fn countSpecialPromptTokens(token_ids: []const i64, config: gpt_mod.Config) PromptSpecialCounts {
    var counts: PromptSpecialCounts = .{ .boi = 0, .image = 0, .eoi = 0 };
    for (token_ids) |token_id| {
        if (token_id == config.boi_token_index) counts.boi += 1;
        if (token_id == config.image_token_index) counts.image += 1;
        if (token_id == config.eoi_token_index) counts.eoi += 1;
    }
    return counts;
}

fn loadWeightPrefix(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, name: []const u8) ![4]f32 {
    const weight = cb.getWeight(name) catch return .{ 0, 0, 0, 0 };
    defer cb.free(weight);
    const dense = try cb.toFloat32(weight, allocator);
    defer allocator.free(dense);
    var out = [_]f32{ 0, 0, 0, 0 };
    const n = @min(out.len, dense.len);
    @memcpy(out[0..n], dense[0..n]);
    return out;
}

fn printWeightSamples(label: []const u8, analysis: NativeAnalysis) void {
    print(
        "{s}_layer0_norm_samples input={d:.6},{d:.6},{d:.6},{d:.6} q={d:.6},{d:.6},{d:.6},{d:.6} k={d:.6},{d:.6},{d:.6},{d:.6}\n",
        .{
            label,
            analysis.input_norm_sample[0],
            analysis.input_norm_sample[1],
            analysis.input_norm_sample[2],
            analysis.input_norm_sample[3],
            analysis.q_norm_sample[0],
            analysis.q_norm_sample[1],
            analysis.q_norm_sample[2],
            analysis.q_norm_sample[3],
            analysis.k_norm_sample[0],
            analysis.k_norm_sample[1],
            analysis.k_norm_sample[2],
            analysis.k_norm_sample[3],
        },
    );
    print(
        "{s}_layer0_ffn_norm_samples pre={d:.6},{d:.6},{d:.6},{d:.6} post_attn={d:.6},{d:.6},{d:.6},{d:.6} post_ffn={d:.6},{d:.6},{d:.6},{d:.6}\n",
        .{
            label,
            analysis.pre_ffn_norm_sample[0],
            analysis.pre_ffn_norm_sample[1],
            analysis.pre_ffn_norm_sample[2],
            analysis.pre_ffn_norm_sample[3],
            analysis.post_attn_norm_sample[0],
            analysis.post_attn_norm_sample[1],
            analysis.post_attn_norm_sample[2],
            analysis.post_attn_norm_sample[3],
            analysis.post_ffn_norm_sample[0],
            analysis.post_ffn_norm_sample[1],
            analysis.post_ffn_norm_sample[2],
            analysis.post_ffn_norm_sample[3],
        },
    );
}

fn countPromptTokens(attention_mask: []const i32) usize {
    var count: usize = 0;
    while (count < attention_mask.len and attention_mask[count] != 0) : (count += 1) {}
    return count;
}

fn collectTopLogits(allocator: std.mem.Allocator, logits: []const f32, top_k: usize) ![]TopLogit {
    var entries = try allocator.alloc(TopLogit, logits.len);
    defer allocator.free(entries);
    for (logits, 0..) |logit, idx| {
        entries[idx] = .{ .id = @intCast(idx), .logit = logit };
    }
    std.mem.sort(TopLogit, entries, {}, struct {
        fn lessThan(_: void, a: TopLogit, b: TopLogit) bool {
            return a.logit > b.logit;
        }
    }.lessThan);
    const limit = @min(top_k, entries.len);
    const out = try allocator.alloc(TopLogit, limit);
    @memcpy(out, entries[0..limit]);
    return out;
}

fn printTopLogits(allocator: std.mem.Allocator, tok: @import("inference_tokenizer").Tokenizer, logits: []const f32, top_k: usize) !void {
    const entries = try collectTopLogits(allocator, logits, top_k);
    defer allocator.free(entries);
    try printTopLogitsFromEntries(allocator, tok, entries);
}

fn printTopLogitsFromEntries(allocator: std.mem.Allocator, tok: @import("inference_tokenizer").Tokenizer, entries: []const TopLogit) !void {
    for (entries, 0..) |entry, rank| {
        const one = [_]i32{entry.id};
        const piece = tok.decode(allocator, &one) catch try allocator.dupe(u8, "");
        defer allocator.free(piece);
        print("  {d}. id={d} logit={d:.6} text={s}\n", .{ rank + 1, entry.id, entry.logit, piece });
    }
}

fn printSingleToken(allocator: std.mem.Allocator, label: []const u8, tok: @import("inference_tokenizer").Tokenizer, token_id: i32) !void {
    const one = [_]i32{token_id};
    const piece = tok.decode(allocator, &one) catch try allocator.dupe(u8, "");
    defer allocator.free(piece);
    print("{s}: id={d} text={s}\n", .{ label, token_id, piece });
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 3) {
        printUsage();
        return error.InvalidArguments;
    }
    var opts = Options{
        .native_model_dir = args[0],
        .reference_model_dir = args[1],
        .prompt = args[2],
    };
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--image")) {
            i += 1;
            if (i >= args.len) return error.MissingImagePath;
            if (opts.image_count >= opts.image_paths.len) return error.TooManyImages;
            opts.image_paths[opts.image_count] = args[i];
            opts.image_count += 1;
        } else if (std.mem.eql(u8, arg, "--top-k")) {
            i += 1;
            if (i >= args.len) return error.MissingTopK;
            opts.top_k = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--no-chat-template")) {
            opts.no_chat_template = true;
        } else if (std.mem.eql(u8, arg, "--image-features-only")) {
            opts.image_features_only = true;
        } else if (std.mem.eql(u8, arg, "--onnx-prompt-embeddings-only")) {
            opts.onnx_prompt_embeddings_only = true;
        } else {
            return error.UnknownArgument;
        }
    }
    return opts;
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => &.{ .onnx, .mlx, .native },
        .native => &.{ .native, .onnx, .mlx },
        .mlx => &.{ .mlx, .onnx, .native },
    };
}

fn printUsage() void {
    print("usage: termite compare <native-model-dir> <reference-model-dir> <prompt> [--image path] [--image-features-only] [--onnx-prompt-embeddings-only] [--backend auto|native|mlx] [--top-k N] [--no-chat-template]\n", .{});
}
