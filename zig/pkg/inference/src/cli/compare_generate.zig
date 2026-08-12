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
const runtime = @import("../runtime/root.zig");
const gemma3_mm = @import("../pipelines/gemma3_multimodal.zig");
const onnx_decoder_only_vlm = @import("../pipelines/onnx_decoder_only_vlm.zig");
const gemma3_vision = @import("../architectures/gemma3_vision.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const c_file = @import("../util/c_file.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const compat = @import("../io/compat.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    cuda,
    metal,
};

const Options = struct {
    native_model_dir: []const u8,
    reference_model_dir: []const u8,
    prompt: []const u8,
    image_paths: [8][]const u8 = .{""} ** 8,
    image_count: usize = 0,
    backend: BackendChoice = .auto,
    native_backend: ?BackendChoice = null,
    reference_backend: ?BackendChoice = null,
    top_k: usize = 8,
    no_chat_template: bool = false,
    raw_prompt: bool = false,
    image_features_only: bool = false,
    onnx_prompt_embeddings_only: bool = false,
    runtime_parity: bool = false,
    sequential_compare: bool = false,
    quality_eval: bool = false,
    weight_binding_audit: bool = false,
    activation_trace: bool = false,
    prompt_file: ?[]const u8 = null,
    max_prompts: usize = 0,
    json_out_path: ?[]const u8 = null,
    binding_audit_layer_limit: usize = 0,
    activation_trace_layer_limit: usize = 8,
    activation_trace_layer: ?usize = null,
    activation_trace_row: ?usize = null,
    activation_trace_all_rows: bool = false,
    host_budget_mb: usize = 0,
    backend_budget_mb: usize = 0,
    combined_budget_mb: usize = 0,
    kv_budget_mb: usize = 0,
    scratch_budget_mb: usize = 0,
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
    last_logits: []f32,
    top_logits: []TopLogit,
    elapsed_ms: u64 = 0,

    fn deinit(self: *NativeAnalysis, allocator: std.mem.Allocator) void {
        allocator.free(self.prompt);
        allocator.free(self.prompt_token_ids);
        allocator.free(self.last_logits);
        allocator.free(self.top_logits);
    }
};

const TopLogit = struct {
    id: i32,
    logit: f32,
};

const RuntimeParityCapture = struct {
    allocator: std.mem.Allocator,
    backend_name: []const u8,
    phase: []const u8,
    total_rows: usize,
    vocab_size: usize,
    token_id: i32,
    token_text: []u8,
    final_hidden: []f32,
    pre_norm_hidden: []f32,
    logits: []f32,
    top_logits: []TopLogit,

    fn deinit(self: *RuntimeParityCapture) void {
        self.allocator.free(self.token_text);
        self.allocator.free(self.final_hidden);
        self.allocator.free(self.pre_norm_hidden);
        self.allocator.free(self.logits);
        self.allocator.free(self.top_logits);
        self.* = undefined;
    }
};

const RuntimeParityDiff = struct {
    max_abs: f32 = 0,
    mean_abs: f64 = 0,
    max_index: usize = 0,
};

const weight_binding_sample_count = 8;
const weight_binding_full_stats_element_limit: usize = 80_000_000;

const WeightBindingSlotStat = struct {
    label: []u8,
    used_name: []u8 = "",
    used_name_owned: bool = false,
    present: bool = false,
    optional: bool = false,
    values_ok: bool = false,
    values_skipped: bool = false,
    error_name: []const u8 = "",
    shape: []i64 = &.{},
    shape_owned: bool = false,
    element_count: usize = 0,
    l2: f64 = 0,
    rms: f64 = 0,
    mean_abs: f64 = 0,
    max_abs: f32 = 0,
    first: [weight_binding_sample_count]f32 = [_]f32{0} ** weight_binding_sample_count,
    first_count: usize = 0,

    fn deinit(self: *WeightBindingSlotStat, allocator: std.mem.Allocator) void {
        allocator.free(self.label);
        if (self.used_name_owned) allocator.free(self.used_name);
        if (self.shape_owned) allocator.free(self.shape);
        self.* = undefined;
    }
};

const WeightBindingAnalysis = struct {
    side: []const u8,
    model_dir: []u8,
    backend_name: []const u8,
    weight_prefix: []u8,
    stats: []WeightBindingSlotStat,

    fn deinit(self: *WeightBindingAnalysis, allocator: std.mem.Allocator) void {
        allocator.free(self.model_dir);
        allocator.free(self.weight_prefix);
        for (self.stats) |*stat| stat.deinit(allocator);
        allocator.free(self.stats);
        self.* = undefined;
    }
};

const ActivationTracePoint = struct {
    label: []u8,
    layer: ?usize,
    row_dim: usize,
    row_count: usize,
    row_start: usize,
    all_rows: bool,
    values: []f32,

    fn deinit(self: *ActivationTracePoint, allocator: std.mem.Allocator) void {
        allocator.free(self.label);
        allocator.free(self.values);
        self.* = undefined;
    }
};

const ActivationTraceAnalysis = struct {
    side: []const u8,
    model_dir: []u8,
    backend_name: []const u8,
    prompt: []u8,
    prompt_token_ids: []i64,
    top1: i32,
    points: []ActivationTracePoint,

    fn deinit(self: *ActivationTraceAnalysis, allocator: std.mem.Allocator) void {
        allocator.free(self.model_dir);
        allocator.free(self.prompt);
        allocator.free(self.prompt_token_ids);
        for (self.points) |*point| point.deinit(allocator);
        allocator.free(self.points);
        self.* = undefined;
    }
};

const ActivationTraceCollector = struct {
    allocator: std.mem.Allocator,
    points: std.ArrayListUnmanaged(ActivationTracePoint) = .empty,
    layer_limit: usize,
    target_layer: ?usize,
    target_row: ?usize,
    all_rows: bool,

    fn init(allocator: std.mem.Allocator, layer_limit: usize, target_layer: ?usize, target_row: ?usize, all_rows: bool) ActivationTraceCollector {
        return .{
            .allocator = allocator,
            .layer_limit = layer_limit,
            .target_layer = target_layer,
            .target_row = target_row,
            .all_rows = all_rows,
        };
    }

    fn deinit(self: *ActivationTraceCollector) void {
        for (self.points.items) |*point| point.deinit(self.allocator);
        self.points.deinit(self.allocator);
        self.* = undefined;
    }

    fn sink(self: *ActivationTraceCollector) gpt_arch.ActivationTraceSink {
        return .{
            .ptr = self,
            .captureFn = captureThunk,
        };
    }

    fn captureThunk(
        ptr: *anyopaque,
        cb: *const ops.ComputeBackend,
        allocator: std.mem.Allocator,
        label: []const u8,
        layer: ?usize,
        tensor: ops.CT,
        row_dim: usize,
    ) !void {
        const self: *ActivationTraceCollector = @ptrCast(@alignCast(ptr));
        try self.capture(cb, allocator, label, layer, tensor, row_dim);
    }

    fn capture(
        self: *ActivationTraceCollector,
        cb: *const ops.ComputeBackend,
        allocator: std.mem.Allocator,
        label: []const u8,
        layer: ?usize,
        tensor: ops.CT,
        row_dim: usize,
    ) !void {
        if (!self.shouldCapture(label, layer)) return;
        if (row_dim == 0) return;
        const values = try cb.toFloat32(tensor, allocator);
        defer allocator.free(values);
        if (values.len < row_dim) return;
        const row_count = values.len / row_dim;
        if (row_count == 0) return;
        const row_start = if (self.all_rows)
            @as(usize, 0)
        else if (self.target_row) |target|
            if (target < row_count) target else return
        else
            row_count - 1;
        const captured_values = if (self.all_rows)
            values[0 .. row_count * row_dim]
        else
            values[row_start * row_dim ..][0..row_dim];
        const copied = try self.allocator.dupe(f32, captured_values);
        errdefer self.allocator.free(copied);
        const label_copy = try self.allocator.dupe(u8, label);
        errdefer self.allocator.free(label_copy);
        try self.points.append(self.allocator, .{
            .label = label_copy,
            .layer = layer,
            .row_dim = row_dim,
            .row_count = if (self.all_rows) row_count else 1,
            .row_start = row_start,
            .all_rows = self.all_rows,
            .values = copied,
        });
    }

    fn shouldCapture(self: *const ActivationTraceCollector, label: []const u8, layer: ?usize) bool {
        const layer_index = layer orelse return true;
        if (std.mem.eql(u8, label, "out")) {
            return self.layer_limit == 0 or layer_index < self.layer_limit;
        }
        return if (self.target_layer) |target| layer_index == target else false;
    }
};

const ActivationTraceDiff = struct {
    max_abs: f32 = 0,
    mean_abs: f64 = 0,
    rmse: f64 = 0,
    rel_rmse: f64 = 0,
    cosine: f64 = 0,
    actual_rms: f64 = 0,
    expected_rms: f64 = 0,
    max_index: usize = 0,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);

    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    if (opts.runtime_parity) {
        try runRuntimeParity(allocator, &model_manager, opts);
        return;
    }
    if (opts.weight_binding_audit) {
        try runWeightBindingAudit(allocator, opts);
        return;
    }
    if (opts.activation_trace) {
        try runActivationTraceCompare(allocator, opts);
        return;
    }
    if (opts.quality_eval) {
        try runQualityEval(allocator, io, opts);
        return;
    }
    if (opts.sequential_compare) {
        try runSequentialTextCompare(allocator, opts);
        return;
    }

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

    var native = try analyzeNativeModel(allocator, native_model, opts);
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
    var reference = try analyzeNativeModel(allocator, reference_model, opts);
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
    try printNativeReferenceTopComparison(allocator, native_model, native, reference, opts.top_k);
}

fn printNativeReferenceTopComparison(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    native: NativeAnalysis,
    reference: NativeAnalysis,
    top_k: usize,
) !void {
    if (native.last_logits.len != reference.last_logits.len) {
        print("first_token_compare: skipped shape_mismatch native_vocab={d} reference_vocab={d}\n", .{ native.last_logits.len, reference.last_logits.len });
        return;
    }

    const overlap = topLogitOverlap(native.top_logits, reference.top_logits);
    const limit = @min(top_k, @min(native.top_logits.len, reference.top_logits.len));
    const native_rank_in_reference = rankOfToken(reference.last_logits, native.top1) orelse 0;
    const reference_rank_in_native = rankOfToken(native.last_logits, reference.top1) orelse 0;
    const native_logit_in_reference = logitForToken(reference.last_logits, native.top1) orelse std.math.nan(f32);
    const reference_logit_in_native = logitForToken(native.last_logits, reference.top1) orelse std.math.nan(f32);
    const native_gap_in_reference = reference.top_logits[0].logit - native_logit_in_reference;
    const reference_gap_in_native = native.top_logits[0].logit - reference_logit_in_native;

    const native_text = try decodeTokenText(allocator, model.getTokenizer(), native.top1);
    defer allocator.free(native_text);
    const reference_text = try decodeTokenText(allocator, model.getTokenizer(), reference.top1);
    defer allocator.free(reference_text);

    print("first_token_compare: vocab={d} top_k={d} topk_overlap={d}/{d}\n", .{ native.last_logits.len, top_k, overlap, limit });
    print(
        "first_token_compare: native_top1 id={d} text={s} reference_rank={d} native_logit={d:.6} reference_logit={d:.6} reference_gap_to_top={d:.6} empty_text={} manifest_special={}\n",
        .{
            native.top1,
            native_text,
            native_rank_in_reference,
            native.top_logits[0].logit,
            native_logit_in_reference,
            native_gap_in_reference,
            native_text.len == 0,
            isManifestSpecialText(model, native_text),
        },
    );
    print(
        "first_token_compare: reference_top1 id={d} text={s} native_rank={d} reference_logit={d:.6} native_logit={d:.6} native_gap_to_top={d:.6} empty_text={} manifest_special={}\n",
        .{
            reference.top1,
            reference_text,
            reference_rank_in_native,
            reference.top_logits[0].logit,
            reference_logit_in_native,
            reference_gap_in_native,
            reference_text.len == 0,
            isManifestSpecialText(model, reference_text),
        },
    );
    print("first_token_compare: native_top1_margin={d:.6} reference_top1_margin={d:.6}\n", .{ topLogitMargin(native.top_logits), topLogitMargin(reference.top_logits) });
}

fn runSequentialTextCompare(allocator: std.mem.Allocator, opts: Options) !void {
    if (opts.image_count > 0) return error.SequentialCompareTextOnly;

    var native = blk: {
        var session_manager = backends.SessionManager.init(allocator);
        configureBackendPreference(&session_manager, opts.native_backend orelse opts.backend);
        var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
        defer model_manager.deinit();

        const model = try model_manager.loadFromDir(opts.native_model_dir);
        var analysis = try analyzeNativeModel(allocator, model, opts);
        errdefer analysis.deinit(allocator);

        print("native_model={s}\n", .{opts.native_model_dir});
        print("reference_model={s}\n", .{opts.reference_model_dir});
        try printAnalysisSummary(allocator, "native", model, analysis);
        break :blk analysis;
    };
    defer native.deinit(allocator);

    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, opts.reference_backend orelse opts.backend);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const reference_model = try model_manager.loadFromDir(opts.reference_model_dir);
    var reference = try analyzeNativeModel(allocator, reference_model, opts);
    defer reference.deinit(allocator);

    try printAnalysisSummary(allocator, "reference", reference_model, reference);
    print("native_prompt == reference_prompt: {}\n", .{std.mem.eql(u8, native.prompt, reference.prompt)});
    if (!std.mem.eql(u8, native.prompt, reference.prompt)) {
        print("native_prompt:\n{s}\n", .{native.prompt});
        print("reference_prompt:\n{s}\n", .{reference.prompt});
    }
    try printNativeReferenceTopComparison(allocator, reference_model, native, reference, opts.top_k);
}

const QualityEvalItem = struct {
    prompt: []u8,
    native_top1: i32,
    reference_top1: i32,
    native_text: []u8,
    reference_text: []u8,
    native_elapsed_ms: u64,
    reference_elapsed_ms: u64,
    top1_match: bool,
    topk_overlap: usize,
    topk_limit: usize,
    native_rank_in_reference: usize,
    reference_rank_in_native: usize,
    native_empty_or_special: bool,
    reference_empty_or_special: bool,

    fn deinit(self: *QualityEvalItem, allocator: std.mem.Allocator) void {
        allocator.free(self.prompt);
        allocator.free(self.native_text);
        allocator.free(self.reference_text);
        self.* = undefined;
    }
};

fn runQualityEval(allocator: std.mem.Allocator, io: std.Io, opts: Options) !void {
    if (opts.image_count > 0) return error.QualityEvalTextOnly;

    var prompts = try loadQualityEvalPrompts(allocator, io, opts);
    defer {
        for (prompts.items) |prompt| allocator.free(prompt);
        prompts.deinit(allocator);
    }
    if (prompts.items.len == 0) return error.EmptyPromptFile;

    var items = std.ArrayListUnmanaged(QualityEvalItem).empty;
    defer {
        for (items.items) |*item| item.deinit(allocator);
        items.deinit(allocator);
    }

    var top1_matches: usize = 0;
    var empty_or_special_failures: usize = 0;
    var overlap_sum: usize = 0;
    var reference_rank_sum: usize = 0;
    var candidate_rank_sum: usize = 0;

    var native_results = std.ArrayListUnmanaged(NativeAnalysis).empty;
    defer {
        for (native_results.items) |*analysis| analysis.deinit(allocator);
        native_results.deinit(allocator);
    }
    {
        var session_manager = backends.SessionManager.init(allocator);
        configureBackendPreference(&session_manager, opts.native_backend orelse opts.backend);
        var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
        defer model_manager.deinit();

        const native_model = try model_manager.loadFromDir(opts.native_model_dir);
        print("quality_eval_phase: side=native prompts={d}\n", .{prompts.items.len});
        for (prompts.items) |prompt| {
            var prompt_opts = opts;
            prompt_opts.prompt = prompt;
            prompt_opts.image_count = 0;

            const started_ns = platform.time.monotonicNs();
            var analysis = try analyzeNativeModel(allocator, native_model, prompt_opts);
            analysis.elapsed_ms = elapsedMillisSince(started_ns);
            native_results.append(allocator, analysis) catch |err| {
                analysis.deinit(allocator);
                return err;
            };
        }
    }

    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, opts.reference_backend orelse opts.backend);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const reference_model = try model_manager.loadFromDir(opts.reference_model_dir);
    print("quality_eval_phase: side=reference prompts={d}\n", .{prompts.items.len});
    for (prompts.items, 0..) |prompt, idx| {
        var prompt_opts = opts;
        prompt_opts.prompt = prompt;
        prompt_opts.image_count = 0;

        const native = &native_results.items[idx];
        const reference_started_ns = platform.time.monotonicNs();
        var reference = try analyzeNativeModel(allocator, reference_model, prompt_opts);
        const reference_elapsed_ms = elapsedMillisSince(reference_started_ns);
        defer reference.deinit(allocator);

        if (native.last_logits.len != reference.last_logits.len) return error.QualityEvalVocabMismatch;
        if (!std.mem.eql(u8, native.prompt, reference.prompt)) return error.QualityEvalPromptMismatch;

        const top1_match = native.top1 == reference.top1;
        if (top1_match) top1_matches += 1;
        const overlap = topLogitOverlap(native.top_logits, reference.top_logits);
        overlap_sum += overlap;
        const topk_limit = @min(opts.top_k, @min(native.top_logits.len, reference.top_logits.len));
        const native_rank_in_reference = rankOfToken(reference.last_logits, native.top1) orelse 0;
        const reference_rank_in_native = rankOfToken(native.last_logits, reference.top1) orelse 0;
        reference_rank_sum += reference_rank_in_native;
        candidate_rank_sum += native_rank_in_reference;
        const native_text = try decodeTokenText(allocator, reference_model.getTokenizer(), native.top1);
        errdefer allocator.free(native_text);
        const reference_text = try decodeTokenText(allocator, reference_model.getTokenizer(), reference.top1);
        errdefer allocator.free(reference_text);
        const native_empty_or_special = native_text.len == 0 or isManifestSpecialText(reference_model, native_text);
        const reference_empty_or_special = reference_text.len == 0 or isManifestSpecialText(reference_model, reference_text);
        if (native_empty_or_special) empty_or_special_failures += 1;

        print(
            "quality_eval_prompt: index={d} native_top1={d} reference_top1={d} match={} topk_overlap={d}/{d} native_rank_in_reference={d} reference_rank_in_native={d} native_empty_or_special={} reference_empty_or_special={} native_ms={d} reference_ms={d}\n",
            .{
                idx,
                native.top1,
                reference.top1,
                top1_match,
                overlap,
                topk_limit,
                native_rank_in_reference,
                reference_rank_in_native,
                native_empty_or_special,
                reference_empty_or_special,
                native.elapsed_ms,
                reference_elapsed_ms,
            },
        );

        const prompt_copy = try allocator.dupe(u8, prompt);
        errdefer allocator.free(prompt_copy);
        try items.append(allocator, .{
            .prompt = prompt_copy,
            .native_top1 = native.top1,
            .reference_top1 = reference.top1,
            .native_text = native_text,
            .reference_text = reference_text,
            .native_elapsed_ms = native.elapsed_ms,
            .reference_elapsed_ms = reference_elapsed_ms,
            .top1_match = top1_match,
            .topk_overlap = overlap,
            .topk_limit = topk_limit,
            .native_rank_in_reference = native_rank_in_reference,
            .reference_rank_in_native = reference_rank_in_native,
            .native_empty_or_special = native_empty_or_special,
            .reference_empty_or_special = reference_empty_or_special,
        });
    }

    const prompt_count = items.items.len;
    const top1_pct = percent(top1_matches, prompt_count);
    const overlap_avg = if (prompt_count == 0) 0.0 else @as(f64, @floatFromInt(overlap_sum)) / @as(f64, @floatFromInt(prompt_count));
    const candidate_rank_avg = if (prompt_count == 0) 0.0 else @as(f64, @floatFromInt(candidate_rank_sum)) / @as(f64, @floatFromInt(prompt_count));
    const reference_rank_avg = if (prompt_count == 0) 0.0 else @as(f64, @floatFromInt(reference_rank_sum)) / @as(f64, @floatFromInt(prompt_count));
    const min_top1_pct = inferredQualityTop1Threshold(opts.native_model_dir);
    const passed = empty_or_special_failures == 0 and top1_pct >= min_top1_pct;

    print(
        "quality_eval_summary: prompts={d} top1_matches={d} top1_pct={d:.2} min_top1_pct={d:.2} empty_or_special_failures={d} avg_topk_overlap={d:.3} avg_native_rank_in_reference={d:.3} avg_reference_rank_in_native={d:.3} passed={}\n",
        .{
            prompt_count,
            top1_matches,
            top1_pct,
            min_top1_pct,
            empty_or_special_failures,
            overlap_avg,
            candidate_rank_avg,
            reference_rank_avg,
            passed,
        },
    );

    if (opts.json_out_path) |path| {
        try writeQualityEvalJson(allocator, io, path, opts, items.items, top1_matches, top1_pct, min_top1_pct, empty_or_special_failures, overlap_avg, candidate_rank_avg, reference_rank_avg, passed);
    }
    if (!passed) return error.QualityEvalGateFailed;
}

fn loadQualityEvalPrompts(allocator: std.mem.Allocator, io: std.Io, opts: Options) !std.ArrayListUnmanaged([]u8) {
    var prompts = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (prompts.items) |prompt| allocator.free(prompt);
        prompts.deinit(allocator);
    }
    if (opts.prompt_file) |path| {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
        defer allocator.free(bytes);
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |line_raw| {
            if (opts.max_prompts > 0 and prompts.items.len >= opts.max_prompts) break;
            const line = std.mem.trim(u8, line_raw, " \t\r");
            if (line.len == 0) continue;
            if (line[0] == '#') continue;
            try prompts.append(allocator, try allocator.dupe(u8, line));
        }
    } else {
        try prompts.append(allocator, try allocator.dupe(u8, opts.prompt));
    }
    return prompts;
}

fn percent(count: usize, total: usize) f64 {
    if (total == 0) return 0.0;
    return @as(f64, @floatFromInt(count)) * 100.0 / @as(f64, @floatFromInt(total));
}

fn elapsedMillisSince(start_ns: u64) u64 {
    const end_ns = platform.time.monotonicNs();
    if (end_ns <= start_ns) return 0;
    return (end_ns - start_ns) / std.time.ns_per_ms;
}

fn inferredQualityTop1Threshold(model_dir: []const u8) f64 {
    if (std.mem.indexOf(u8, model_dir, "q4") != null or std.mem.indexOf(u8, model_dir, "Q4") != null) return 75.0;
    if (std.mem.indexOf(u8, model_dir, "q8") != null or std.mem.indexOf(u8, model_dir, "Q8") != null) return 90.0;
    return 0.0;
}

fn writeQualityEvalJson(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    opts: Options,
    items: []const QualityEvalItem,
    top1_matches: usize,
    top1_pct: f64,
    min_top1_pct: f64,
    empty_or_special_failures: usize,
    overlap_avg: f64,
    candidate_rank_avg: f64,
    reference_rank_avg: f64,
    passed: bool,
) !void {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    const header = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\"native_model":{f},
        \\"reference_model":{f},
        \\"prompt_count":{d},
        \\"top1_matches":{d},
        \\"top1_pct":{d:.6},
        \\"min_top1_pct":{d:.6},
        \\"empty_or_special_failures":{d},
        \\"avg_topk_overlap":{d:.6},
        \\"avg_native_rank_in_reference":{d:.6},
        \\"avg_reference_rank_in_native":{d:.6},
        \\"passed":{},
        \\"items":[
        \\
    ,
        .{
            std.json.fmt(opts.native_model_dir, .{}),
            std.json.fmt(opts.reference_model_dir, .{}),
            items.len,
            top1_matches,
            top1_pct,
            min_top1_pct,
            empty_or_special_failures,
            overlap_avg,
            candidate_rank_avg,
            reference_rank_avg,
            passed,
        },
    );
    defer allocator.free(header);
    try out.appendSlice(allocator, header);
    for (items, 0..) |item, idx| {
        const row = try std.fmt.allocPrint(
            allocator,
            \\{s}{{
            \\"prompt":{f},
            \\"native_top1":{d},
            \\"reference_top1":{d},
            \\"native_text":{f},
            \\"reference_text":{f},
            \\"native_elapsed_ms":{d},
            \\"reference_elapsed_ms":{d},
            \\"top1_match":{},
            \\"topk_overlap":{d},
            \\"topk_limit":{d},
            \\"native_rank_in_reference":{d},
            \\"reference_rank_in_native":{d},
            \\"native_empty_or_special":{},
            \\"reference_empty_or_special":{}
            \\}}
        ,
            .{
                if (idx == 0) "" else ",",
                std.json.fmt(item.prompt, .{}),
                item.native_top1,
                item.reference_top1,
                std.json.fmt(item.native_text, .{}),
                std.json.fmt(item.reference_text, .{}),
                item.native_elapsed_ms,
                item.reference_elapsed_ms,
                item.top1_match,
                item.topk_overlap,
                item.topk_limit,
                item.native_rank_in_reference,
                item.reference_rank_in_native,
                item.native_empty_or_special,
                item.reference_empty_or_special,
            },
        );
        defer allocator.free(row);
        try out.appendSlice(allocator, row);
    }
    try out.appendSlice(allocator, "\n]}\n");
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
}

fn runActivationTraceCompare(allocator: std.mem.Allocator, opts: Options) !void {
    if (opts.image_count > 0) return error.ActivationTraceTextOnly;

    var native = try analyzeActivationTrace(
        allocator,
        "native",
        opts.native_model_dir,
        opts.native_backend orelse opts.backend,
        opts,
    );
    defer native.deinit(allocator);

    var reference = try analyzeActivationTrace(
        allocator,
        "reference",
        opts.reference_model_dir,
        opts.reference_backend orelse opts.backend,
        opts,
    );
    defer reference.deinit(allocator);

    print("activation_trace_compare: native_prompt == reference_prompt: {}\n", .{std.mem.eql(u8, native.prompt, reference.prompt)});
    print("activation_trace_compare: native_tokens={d} reference_tokens={d}\n", .{ native.prompt_token_ids.len, reference.prompt_token_ids.len });
    if (!std.mem.eql(u8, native.prompt, reference.prompt) or native.prompt_token_ids.len != reference.prompt_token_ids.len) return error.ActivationTracePromptMismatch;
    for (native.prompt_token_ids, reference.prompt_token_ids, 0..) |lhs, rhs, idx| {
        if (lhs != rhs) {
            print("activation_trace_compare: token_mismatch index={d} native={d} reference={d}\n", .{ idx, lhs, rhs });
            return error.ActivationTracePromptMismatch;
        }
    }
    print("activation_trace_compare: native_top1={d} reference_top1={d}\n", .{ native.top1, reference.top1 });
    try printActivationTraceComparison(&native, &reference);
}

fn analyzeActivationTrace(
    allocator: std.mem.Allocator,
    side: []const u8,
    model_dir: []const u8,
    backend_choice: BackendChoice,
    opts: Options,
) !ActivationTraceAnalysis {
    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, backend_choice);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(model_dir);
    const cfg = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const rendered_prompt = try renderPrompt(allocator, model, opts.prompt, opts.no_chat_template, opts.raw_prompt);
    errdefer allocator.free(rendered_prompt);
    const input_ids = try encodeRuntimeParityPrompt(allocator, model, rendered_prompt);
    errdefer allocator.free(input_ids);

    var run_budget = try makeAnalyzeRunBudget(allocator, model, cfg, input_ids.len, opts);
    var cb = try session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget);
    defer cb.deinit();

    var collector = ActivationTraceCollector.init(allocator, opts.activation_trace_layer_limit, opts.activation_trace_layer, opts.activation_trace_row, opts.activation_trace_all_rows);
    errdefer collector.deinit();
    var sink = collector.sink();

    const hidden_size: usize = @intCast(cfg.hidden_size);
    const embed_w = try gpt_arch.getEmbeddingWeight(&cb, cfg);
    defer cb.free(embed_w);
    const embedded = try cb.embeddingLookup(embed_w, input_ids, input_ids.len, hidden_size);
    const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(&cb, allocator, cfg, embedded, input_ids.len, hidden_size);
    const ple_vectors = try gpt_arch.computePleVectors(&cb, allocator, cfg, input_ids, hidden_input, input_ids.len);
    defer if (ple_vectors) |pv| cb.free(pv);

    const hidden_result = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithTrace(
        &cb,
        allocator,
        cfg,
        hidden_input,
        1,
        input_ids.len,
        null,
        ple_vectors,
        &sink,
    );
    defer cb.free(hidden_result.hidden);

    const lm_w = try gpt_arch.getLmHeadWeight(&cb, cfg);
    defer cb.free(lm_w);
    const logits_ct = try cb.linearNoBias(hidden_result.hidden, lm_w, hidden_result.total_rows, cfg.hidden_size, cfg.vocab_size);
    defer cb.free(logits_ct);
    const logits = try cb.toFloat32(logits_ct, allocator);
    defer allocator.free(logits);
    gpt_arch.applyFinalLogitSoftcapInPlace(cfg, logits);
    const vocab_size: usize = @intCast(cfg.vocab_size);
    const last_logits = logits[(hidden_result.total_rows - 1) * vocab_size ..][0..vocab_size];
    const top1: i32 = @intCast(activations.argmax(last_logits));

    const model_dir_copy = try allocator.dupe(u8, model_dir);
    errdefer allocator.free(model_dir_copy);
    const points = try collector.points.toOwnedSlice(allocator);
    collector.points = .empty;
    errdefer {
        for (points) |*point| point.deinit(allocator);
        allocator.free(points);
    }

    print(
        "activation_trace: side={s} model={s} backend={s} prompt_tokens={d} top1={d} points={d} layer_limit={d} detail_layer={any} row={any} all_rows={}\n",
        .{
            side,
            model_dir,
            @tagName(model.session.backend()),
            input_ids.len,
            top1,
            points.len,
            opts.activation_trace_layer_limit,
            opts.activation_trace_layer,
            opts.activation_trace_row,
            opts.activation_trace_all_rows,
        },
    );

    return .{
        .side = side,
        .model_dir = model_dir_copy,
        .backend_name = @tagName(model.session.backend()),
        .prompt = rendered_prompt,
        .prompt_token_ids = input_ids,
        .top1 = top1,
        .points = points,
    };
}

fn printActivationTraceComparison(native: *const ActivationTraceAnalysis, reference: *const ActivationTraceAnalysis) !void {
    print(
        "activation_trace_compare: native_model={s} native_backend={s} reference_model={s} reference_backend={s}\n",
        .{ native.model_dir, native.backend_name, reference.model_dir, reference.backend_name },
    );
    var missing: usize = 0;
    var compared: usize = 0;
    var first_suspect_seen = false;
    for (native.points) |native_point| {
        const reference_point = findActivationTracePoint(reference.points, native_point.layer, native_point.label) orelse {
            missing += 1;
            printActivationTraceMissing(native_point, "reference_missing");
            continue;
        };
        if (native_point.values.len != reference_point.values.len) {
            missing += 1;
            printActivationTraceShapeMismatch(native_point, reference_point.*);
            continue;
        }
        const stats = activationTraceDiffStats(native_point.values, reference_point.values);
        const suspect = activationTraceSuspect(stats);
        compared += 1;
        printActivationTraceDiff(native_point, stats, suspect);
        if (suspect and !first_suspect_seen) {
            printActivationTraceFirstSuspect(native_point, stats);
            first_suspect_seen = true;
        }
    }
    print("activation_trace_compare_totals: compared={d} missing_or_mismatch={d} first_suspect_found={}\n", .{ compared, missing, first_suspect_seen });
    printActivationTraceInvariants(native.side, native.points);
    printActivationTraceInvariants(reference.side, reference.points);
}

fn findActivationTracePoint(points: []const ActivationTracePoint, layer: ?usize, label: []const u8) ?*const ActivationTracePoint {
    for (points) |*point| {
        if (!optionalUsizeEql(point.layer, layer)) continue;
        if (!std.mem.eql(u8, point.label, label)) continue;
        return point;
    }
    return null;
}

fn optionalUsizeEql(lhs: ?usize, rhs: ?usize) bool {
    if (lhs == null and rhs == null) return true;
    if (lhs == null or rhs == null) return false;
    return lhs.? == rhs.?;
}

fn activationTraceDiffStats(actual: []const f32, expected: []const f32) ActivationTraceDiff {
    var stats: ActivationTraceDiff = .{};
    var sum_abs: f64 = 0;
    var sum_sq: f64 = 0;
    var actual_sq: f64 = 0;
    var expected_sq: f64 = 0;
    var dot: f64 = 0;
    for (actual, expected, 0..) |got, want, idx| {
        const diff_f32 = got - want;
        const diff = @abs(diff_f32);
        const diff64: f64 = @floatCast(diff_f32);
        const got64: f64 = @floatCast(got);
        const want64: f64 = @floatCast(want);
        sum_abs += diff;
        sum_sq += diff64 * diff64;
        actual_sq += got64 * got64;
        expected_sq += want64 * want64;
        dot += got64 * want64;
        if (diff > stats.max_abs) {
            stats.max_abs = diff;
            stats.max_index = idx;
        }
    }
    const count: f64 = @floatFromInt(actual.len);
    stats.mean_abs = sum_abs / count;
    stats.rmse = @sqrt(sum_sq / count);
    stats.actual_rms = @sqrt(actual_sq / count);
    stats.expected_rms = @sqrt(expected_sq / count);
    const expected_l2 = @sqrt(expected_sq);
    const denom = @sqrt(actual_sq) * expected_l2;
    stats.rel_rmse = if (expected_l2 > 1e-12) @sqrt(sum_sq) / expected_l2 else 0;
    stats.cosine = if (denom > 1e-12) dot / denom else 0;
    return stats;
}

fn activationTraceSuspect(stats: ActivationTraceDiff) bool {
    return stats.rel_rmse > 0.05 or stats.cosine < 0.995;
}

fn printActivationTraceInvariants(side: []const u8, points: []const ActivationTracePoint) void {
    for (points) |point| {
        if (!std.mem.eql(u8, point.label, "v_norm")) continue;
        const v_attn = findActivationTracePoint(points, point.layer, "v_attn") orelse continue;
        printActivationTraceInvariant(side, point, v_attn.*, "v_norm_eq_v_attn");
    }
}

fn printActivationTraceInvariant(side: []const u8, lhs: ActivationTracePoint, rhs: ActivationTracePoint, name: []const u8) void {
    if (lhs.values.len != rhs.values.len) {
        if (lhs.layer) |layer| {
            print("activation_trace_invariant: side={s} layer={d} name={s} shape_mismatch lhs={s}:{d} rhs={s}:{d}\n", .{ side, layer, name, lhs.label, lhs.values.len, rhs.label, rhs.values.len });
        } else {
            print("activation_trace_invariant: side={s} name={s} shape_mismatch lhs={s}:{d} rhs={s}:{d}\n", .{ side, name, lhs.label, lhs.values.len, rhs.label, rhs.values.len });
        }
        return;
    }
    const stats = activationTraceDiffStats(lhs.values, rhs.values);
    const suspect = activationTraceSuspect(stats);
    if (lhs.layer) |layer| {
        print(
            "activation_trace_invariant: side={s} layer={d} name={s} lhs={s} rhs={s} row_start={d} rows={d} cosine={d:.9} rel_rmse={d:.6} mean_abs={d:.6} max_abs={d:.6}@{d} suspect={}\n",
            .{ side, layer, name, lhs.label, rhs.label, lhs.row_start, lhs.row_count, stats.cosine, stats.rel_rmse, stats.mean_abs, stats.max_abs, stats.max_index, suspect },
        );
    } else {
        print(
            "activation_trace_invariant: side={s} name={s} lhs={s} rhs={s} row_start={d} rows={d} cosine={d:.9} rel_rmse={d:.6} mean_abs={d:.6} max_abs={d:.6}@{d} suspect={}\n",
            .{ side, name, lhs.label, rhs.label, lhs.row_start, lhs.row_count, stats.cosine, stats.rel_rmse, stats.mean_abs, stats.max_abs, stats.max_index, suspect },
        );
    }
}

fn printActivationTraceDiff(point: ActivationTracePoint, stats: ActivationTraceDiff, suspect: bool) void {
    if (point.layer) |layer| {
        print(
            "activation_trace_compare: point=layer{d}.{s} row_dim={d} row_start={d} rows={d} all_rows={} cosine={d:.9} rel_rmse={d:.6} rmse={d:.6} mean_abs={d:.6} max_abs={d:.6}@{d} actual_rms={d:.6} reference_rms={d:.6} suspect={}\n",
            .{ layer, point.label, point.row_dim, point.row_start, point.row_count, point.all_rows, stats.cosine, stats.rel_rmse, stats.rmse, stats.mean_abs, stats.max_abs, stats.max_index, stats.actual_rms, stats.expected_rms, suspect },
        );
    } else {
        print(
            "activation_trace_compare: point={s} row_dim={d} row_start={d} rows={d} all_rows={} cosine={d:.9} rel_rmse={d:.6} rmse={d:.6} mean_abs={d:.6} max_abs={d:.6}@{d} actual_rms={d:.6} reference_rms={d:.6} suspect={}\n",
            .{ point.label, point.row_dim, point.row_start, point.row_count, point.all_rows, stats.cosine, stats.rel_rmse, stats.rmse, stats.mean_abs, stats.max_abs, stats.max_index, stats.actual_rms, stats.expected_rms, suspect },
        );
    }
}

fn printActivationTraceFirstSuspect(point: ActivationTracePoint, stats: ActivationTraceDiff) void {
    if (point.layer) |layer| {
        print("activation_trace_compare: first_suspect=layer{d}.{s} cosine={d:.9} rel_rmse={d:.6}\n", .{ layer, point.label, stats.cosine, stats.rel_rmse });
    } else {
        print("activation_trace_compare: first_suspect={s} cosine={d:.9} rel_rmse={d:.6}\n", .{ point.label, stats.cosine, stats.rel_rmse });
    }
}

fn printActivationTraceMissing(point: ActivationTracePoint, reason: []const u8) void {
    if (point.layer) |layer| {
        print("activation_trace_compare: point=layer{d}.{s} {s}\n", .{ layer, point.label, reason });
    } else {
        print("activation_trace_compare: point={s} {s}\n", .{ point.label, reason });
    }
}

fn printActivationTraceShapeMismatch(native: ActivationTracePoint, reference: ActivationTracePoint) void {
    if (native.layer) |layer| {
        print("activation_trace_compare: point=layer{d}.{s} shape_mismatch native={d} reference={d}\n", .{ layer, native.label, native.values.len, reference.values.len });
    } else {
        print("activation_trace_compare: point={s} shape_mismatch native={d} reference={d}\n", .{ native.label, native.values.len, reference.values.len });
    }
}

fn runWeightBindingAudit(allocator: std.mem.Allocator, opts: Options) !void {
    if (opts.image_count > 0) return error.WeightBindingAuditTextOnly;

    var native = try analyzeWeightBindings(
        allocator,
        "native",
        opts.native_model_dir,
        opts.native_backend orelse opts.backend,
        opts,
    );
    defer native.deinit(allocator);

    var reference = try analyzeWeightBindings(
        allocator,
        "reference",
        opts.reference_model_dir,
        opts.reference_backend orelse opts.backend,
        opts,
    );
    defer reference.deinit(allocator);

    printWeightBindingComparison(&native, &reference);
}

fn analyzeWeightBindings(
    allocator: std.mem.Allocator,
    side: []const u8,
    model_dir: []const u8,
    backend_choice: BackendChoice,
    opts: Options,
) !WeightBindingAnalysis {
    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, backend_choice);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(model_dir);
    const cfg = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var run_budget = try makeAnalyzeRunBudget(allocator, model, cfg, 1, opts);
    var cb = try session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget);
    defer cb.deinit();

    print("weight_binding_audit: side={s} model={s} backend={s} layers={d} weight_prefix={s}\n", .{
        side,
        model_dir,
        @tagName(model.session.backend()),
        cfg.num_hidden_layers,
        cfg.weight_prefix,
    });

    var stats = std.ArrayListUnmanaged(WeightBindingSlotStat).empty;
    errdefer {
        for (stats.items) |*stat| stat.deinit(allocator);
        stats.deinit(allocator);
    }

    try collectWeightBindingStats(allocator, &cb, cfg, opts.binding_audit_layer_limit, &stats);

    return .{
        .side = side,
        .model_dir = try allocator.dupe(u8, model_dir),
        .backend_name = @tagName(model.session.backend()),
        .weight_prefix = try allocator.dupe(u8, cfg.weight_prefix),
        .stats = try stats.toOwnedSlice(allocator),
    };
}

fn collectWeightBindingStats(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    layer_limit_opt: usize,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
) !void {
    try appendAuditedStaticSlot(allocator, cb, cfg, stats, "model.embed_tokens.weight", true, false);
    try appendAuditedStaticSlot(allocator, cb, cfg, stats, "model.norm.weight", false, false);
    if (cfg.weight_tying) {
        try appendAuditedStaticSlotWithFallback(
            allocator,
            cb,
            cfg,
            stats,
            "lm_head.weight",
            "lm_head.weight",
            "model.embed_tokens.weight",
            true,
            false,
        );
    } else {
        try appendAuditedStaticSlotWithFallback(
            allocator,
            cb,
            cfg,
            stats,
            "lm_head.weight",
            "lm_head.weight",
            "model.embed_tokens.weight",
            true,
            true,
        );
    }

    if (cfg.hasPle()) {
        try appendAuditedStaticSlotWithFallback(
            allocator,
            cb,
            cfg,
            stats,
            "model.per_layer_input.per_layer_token_embd.weight",
            "model.per_layer_input.per_layer_token_embd.weight",
            "model.embed_tokens_per_layer.weight",
            true,
            false,
        );
        try appendAuditedStaticSlotWithFallback(
            allocator,
            cb,
            cfg,
            stats,
            "model.per_layer_input.per_layer_model_proj.weight",
            "model.per_layer_input.per_layer_model_proj.weight",
            "model.per_layer_model_projection.weight",
            false,
            false,
        );
        try appendAuditedStaticSlotWithFallback(
            allocator,
            cb,
            cfg,
            stats,
            "model.per_layer_input.per_layer_proj_norm.weight",
            "model.per_layer_input.per_layer_proj_norm.weight",
            "model.per_layer_projection_norm.weight",
            false,
            false,
        );
    }

    const configured_layers: usize = @intCast(cfg.num_hidden_layers);
    const layer_count = if (layer_limit_opt > 0) @min(layer_limit_opt, configured_layers) else configured_layers;
    for (0..layer_count) |layer| {
        try appendAuditedLayerSlot(allocator, cb, cfg, stats, layer, "input_layernorm.weight", false, false);
        try appendAuditedLayerAttnNormSlot(allocator, cb, cfg, stats, layer, "q", true);
        try appendAuditedLayerAttnNormSlot(allocator, cb, cfg, stats, layer, "k", true);
        try appendAuditedLayerAttnProjSlot(allocator, cb, cfg, stats, layer, "q", false);
        try appendAuditedLayerAttnProjSlot(allocator, cb, cfg, stats, layer, "k", false);
        if (!cfg.layerOmitsVProj(layer)) {
            try appendAuditedLayerAttnProjSlot(allocator, cb, cfg, stats, layer, "v", false);
        }
        try appendAuditedLayerAttnProjSlot(allocator, cb, cfg, stats, layer, "o", false);
        try appendAuditedLayerSlot(allocator, cb, cfg, stats, layer, "post_attention_layernorm.weight", false, true);
        try appendAuditedLayerSlotWithFallback(
            allocator,
            cb,
            cfg,
            stats,
            layer,
            "pre_feedforward_layernorm.weight",
            "pre_feedforward_layernorm.weight",
            "post_attention_layernorm.weight",
            false,
            false,
        );
        try appendAuditedLayerMlpSlot(allocator, cb, cfg, stats, layer, "gate", false);
        try appendAuditedLayerMlpSlot(allocator, cb, cfg, stats, layer, "up", false);
        try appendAuditedLayerMlpSlot(allocator, cb, cfg, stats, layer, "down", false);
        try appendAuditedLayerSlot(allocator, cb, cfg, stats, layer, "post_feedforward_layernorm.weight", false, true);
        try appendAuditedLayerOutputScaleSlot(allocator, cb, cfg, stats, layer);

        if (cfg.hasPle()) {
            try appendAuditedPleLayerSlot(allocator, cb, cfg, stats, layer, "inp_gate.weight", "per_layer_input_gate.weight", false);
            try appendAuditedPleLayerSlot(allocator, cb, cfg, stats, layer, "proj.weight", "per_layer_projection.weight", false);
            try appendAuditedPleLayerSlot(allocator, cb, cfg, stats, layer, "post_norm.weight", "post_per_layer_input_norm.weight", false);
        }
    }
}

fn appendAuditedStaticSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    name: []const u8,
    shape_only: bool,
    optional: bool,
) !void {
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, name, &.{name}, shape_only, optional);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedStaticSlotWithFallback(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    label: []const u8,
    primary: []const u8,
    fallback: []const u8,
    shape_only: bool,
    optional: bool,
) !void {
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, label, &.{ primary, fallback }, shape_only, optional);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedLayerSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
    suffix: []const u8,
    shape_only: bool,
    optional: bool,
) !void {
    const name = try std.fmt.allocPrint(allocator, "model.layers.{d}.{s}", .{ layer, suffix });
    defer allocator.free(name);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, name, &.{name}, shape_only, optional);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedLayerSlotWithFallback(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
    label_suffix: []const u8,
    primary_suffix: []const u8,
    fallback_suffix: []const u8,
    shape_only: bool,
    optional: bool,
) !void {
    const label = try std.fmt.allocPrint(allocator, "model.layers.{d}.{s}", .{ layer, label_suffix });
    defer allocator.free(label);
    const primary = try std.fmt.allocPrint(allocator, "model.layers.{d}.{s}", .{ layer, primary_suffix });
    defer allocator.free(primary);
    const fallback = try std.fmt.allocPrint(allocator, "model.layers.{d}.{s}", .{ layer, fallback_suffix });
    defer allocator.free(fallback);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, label, &.{ primary, fallback }, shape_only, optional);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedLayerAttnNormSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
    proj: []const u8,
    optional: bool,
) !void {
    const name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.{s}_norm.weight", .{ layer, proj });
    defer allocator.free(name);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, name, &.{name}, false, optional);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedLayerAttnProjSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
    proj: []const u8,
    shape_only: bool,
) !void {
    const name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj });
    defer allocator.free(name);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, name, &.{name}, shape_only, false);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedLayerMlpSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
    proj: []const u8,
    shape_only: bool,
) !void {
    const name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.{s}_proj.weight", .{ layer, proj });
    defer allocator.free(name);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, name, &.{name}, shape_only, false);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedLayerOutputScaleSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
) !void {
    const label = try std.fmt.allocPrint(allocator, "model.layers.{d}.per_layer_input.layer_output_scale.weight", .{layer});
    defer allocator.free(label);
    const primary = try std.fmt.allocPrint(allocator, "model.layers.{d}.per_layer_input.layer_output_scale.weight", .{layer});
    defer allocator.free(primary);
    const fallback = try std.fmt.allocPrint(allocator, "model.layers.{d}.layer_scalar", .{layer});
    defer allocator.free(fallback);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, label, &.{ primary, fallback }, false, true);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAuditedPleLayerSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    layer: usize,
    primary_suffix: []const u8,
    fallback_suffix: []const u8,
    shape_only: bool,
) !void {
    const label = try std.fmt.allocPrint(allocator, "model.layers.{d}.per_layer_input.{s}", .{ layer, primary_suffix });
    defer allocator.free(label);
    const primary = try std.fmt.allocPrint(allocator, "model.layers.{d}.per_layer_input.{s}", .{ layer, primary_suffix });
    defer allocator.free(primary);
    const fallback = try std.fmt.allocPrint(allocator, "model.layers.{d}.{s}", .{ layer, fallback_suffix });
    defer allocator.free(fallback);
    const stat = try auditWeightBindingSlot(allocator, cb, cfg, label, &.{ primary, fallback }, shape_only, false);
    try appendAndPrintWeightBindingStat(allocator, stats, stat);
}

fn appendAndPrintWeightBindingStat(
    allocator: std.mem.Allocator,
    stats: *std.ArrayListUnmanaged(WeightBindingSlotStat),
    stat: WeightBindingSlotStat,
) !void {
    try printWeightBindingSlotStat(stat);
    errdefer {
        var owned = stat;
        owned.deinit(allocator);
    }
    try stats.append(allocator, stat);
}

fn auditWeightBindingSlot(
    allocator: std.mem.Allocator,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    label: []const u8,
    candidates: []const []const u8,
    shape_only: bool,
    optional: bool,
) !WeightBindingSlotStat {
    var stat = WeightBindingSlotStat{
        .label = try allocator.dupe(u8, label),
        .optional = optional,
    };
    errdefer stat.deinit(allocator);

    const resolved = resolveWeightBindingCandidate(cb, cfg, candidates) catch |err| {
        stat.error_name = @errorName(err);
        return stat;
    };
    defer cb.free(resolved.tensor);
    stat.present = true;
    stat.used_name = try allocator.dupe(u8, resolved.name);
    stat.used_name_owned = true;

    if (cb.tensorShape(resolved.tensor, allocator)) |shape| {
        stat.shape = shape;
        stat.shape_owned = true;
        stat.element_count = elementCountFromShape(shape) orelse 0;
    } else |err| {
        stat.error_name = @errorName(err);
    }

    if (shape_only) {
        stat.values_skipped = true;
        return stat;
    }
    if (stat.element_count > weight_binding_full_stats_element_limit) {
        stat.values_skipped = true;
        return stat;
    }

    const values = materializeAuditTensorF32(cb, resolved.tensor, allocator) catch |err| {
        stat.error_name = @errorName(err);
        return stat;
    };
    defer allocator.free(values);
    stat.values_ok = true;
    if (stat.element_count == 0 or (stat.shape.len == 0 and values.len > stat.element_count)) stat.element_count = values.len;
    stat.first_count = @min(values.len, stat.first.len);
    @memcpy(stat.first[0..stat.first_count], values[0..stat.first_count]);

    if (values.len == 0) return stat;
    var sum_sq: f64 = 0;
    var sum_abs: f64 = 0;
    var max_abs: f32 = 0;
    for (values) |value| {
        const abs_value = @abs(value);
        sum_abs += @as(f64, abs_value);
        sum_sq += @as(f64, value) * @as(f64, value);
        if (abs_value > max_abs) max_abs = abs_value;
    }
    const denom = @as(f64, @floatFromInt(values.len));
    stat.l2 = @sqrt(sum_sq);
    stat.rms = @sqrt(sum_sq / denom);
    stat.mean_abs = sum_abs / denom;
    stat.max_abs = max_abs;
    return stat;
}

fn materializeAuditTensorF32(cb: *ops.ComputeBackend, tensor: ops.CT, allocator: std.mem.Allocator) ![]f32 {
    return cb.toFloat32(tensor, allocator) catch |to_float_err| switch (to_float_err) {
        error.UnsupportedTensorType => {
            const exported = (try cb.exportTensorData(tensor, allocator)) orelse return to_float_err;
            return materializeAuditExportedTensorF32(allocator, exported);
        },
        else => return to_float_err,
    };
}

fn materializeAuditExportedTensorF32(allocator: std.mem.Allocator, exported: ops.ExportTensorData) ![]f32 {
    switch (exported.payload) {
        .bytes => |bytes| {
            defer allocator.free(bytes);
            return try denseExportBytesToF32(allocator, exported.dtype, bytes);
        },
        .quantized_f32 => |quantized| {
            defer allocator.free(quantized.raw_bytes);
            defer allocator.free(quantized.shape);
            const element_count = elementCountFromShape(quantized.shape) orelse return error.InvalidTensorShape;
            const out = try allocator.alloc(f32, element_count);
            errdefer allocator.free(out);
            try quant_codec.dequantizeToFloat32(quantized.tensor_type, quantized.raw_bytes, out);
            return out;
        },
    }
}

fn denseExportBytesToF32(allocator: std.mem.Allocator, dtype: @TypeOf(@as(ops.ExportTensorData, undefined).dtype), bytes: []const u8) ![]f32 {
    if (bytes.len % dtype.byteSize() != 0) return error.InvalidTensorShape;
    const count = bytes.len / dtype.byteSize();
    const out = try allocator.alloc(f32, count);
    errdefer allocator.free(out);
    switch (dtype) {
        .f32 => {
            for (out, 0..) |*dst, idx| {
                const bits = std.mem.readInt(u32, bytes[idx * 4 ..][0..4], .little);
                dst.* = @bitCast(bits);
            }
        },
        .f16 => {
            for (out, 0..) |*dst, idx| {
                const bits = std.mem.readInt(u16, bytes[idx * 2 ..][0..2], .little);
                dst.* = @floatCast(@as(f16, @bitCast(bits)));
            }
        },
        .bf16 => {
            for (out, 0..) |*dst, idx| {
                const bits = std.mem.readInt(u16, bytes[idx * 2 ..][0..2], .little);
                dst.* = bf16BitsToF32(bits);
            }
        },
        .f64 => {
            for (out, 0..) |*dst, idx| {
                const bits = std.mem.readInt(u64, bytes[idx * 8 ..][0..8], .little);
                dst.* = @floatCast(@as(f64, @bitCast(bits)));
            }
        },
        .i8 => {
            for (bytes, out) |value, *dst| dst.* = @floatFromInt(@as(i8, @bitCast(value)));
        },
        .i16 => {
            for (out, 0..) |*dst, idx| {
                const value = std.mem.readInt(i16, bytes[idx * 2 ..][0..2], .little);
                dst.* = @floatFromInt(value);
            }
        },
        .i32 => {
            for (out, 0..) |*dst, idx| {
                const value = std.mem.readInt(i32, bytes[idx * 4 ..][0..4], .little);
                dst.* = @floatFromInt(value);
            }
        },
        .i64 => {
            for (out, 0..) |*dst, idx| {
                const value = std.mem.readInt(i64, bytes[idx * 8 ..][0..8], .little);
                dst.* = @floatFromInt(value);
            }
        },
        .u8, .bool_ => {
            for (bytes, out) |value, *dst| dst.* = @floatFromInt(value);
        },
    }
    return out;
}

fn bf16BitsToF32(bits: u16) f32 {
    const as_u32: u32 = @as(u32, bits) << 16;
    return @bitCast(as_u32);
}

const ResolvedWeightBindingCandidate = struct {
    tensor: ops.CT,
    name: []const u8,
};

fn resolveWeightBindingCandidate(
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    candidates: []const []const u8,
) !ResolvedWeightBindingCandidate {
    var last_missing: anyerror = error.MissingWeight;
    for (candidates) |candidate| {
        const tensor = gpt_arch.getModelWeight(cb, cfg, candidate) catch |err| switch (err) {
            error.MissingWeight, error.WeightNotFound => {
                last_missing = err;
                continue;
            },
            else => return err,
        };
        return .{ .tensor = tensor, .name = candidate };
    }
    return last_missing;
}

fn elementCountFromShape(shape: []const i64) ?usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return null;
        count = std.math.mul(usize, count, @intCast(dim)) catch return null;
    }
    return count;
}

fn printWeightBindingSlotStat(stat: WeightBindingSlotStat) !void {
    const status = if (!stat.present)
        "missing"
    else if (stat.values_ok)
        "ok"
    else if (stat.values_skipped)
        "shape_only"
    else
        "value_error";
    print("weight_binding_slot: name={s} status={s} optional={} used={s} shape=", .{
        stat.label,
        status,
        stat.optional,
        if (stat.used_name.len > 0) stat.used_name else "-",
    });
    printShape(stat.shape);
    print(" count={d}", .{stat.element_count});
    if (stat.values_ok) {
        print(" l2={d:.6} rms={d:.9} mean_abs={d:.9} max_abs={d:.9} first=", .{
            stat.l2,
            stat.rms,
            stat.mean_abs,
            stat.max_abs,
        });
        printSample(stat.first[0..stat.first_count]);
    } else if (stat.error_name.len > 0) {
        print(" error={s}", .{stat.error_name});
    }
    print("\n", .{});
}

fn printWeightBindingComparison(native: *const WeightBindingAnalysis, reference: *const WeightBindingAnalysis) void {
    print("weight_binding_compare_summary: native_model={s} native_backend={s} native_prefix={s} reference_model={s} reference_backend={s} reference_prefix={s}\n", .{
        native.model_dir,
        native.backend_name,
        native.weight_prefix,
        reference.model_dir,
        reference.backend_name,
        reference.weight_prefix,
    });
    var ok_count: usize = 0;
    var suspect_count: usize = 0;
    var skipped_count: usize = 0;
    var missing_optional_count: usize = 0;

    for (native.stats) |lhs| {
        const rhs = findWeightBindingStat(reference.stats, lhs.label);
        if (rhs == null) {
            suspect_count += 1;
            print("weight_binding_compare: name={s} status=suspect reason=missing_reference_slot\n", .{lhs.label});
            continue;
        }
        const result = compareWeightBindingSlot(lhs, rhs.?);
        switch (result.status) {
            .ok => ok_count += 1,
            .suspect => suspect_count += 1,
            .skipped => skipped_count += 1,
            .missing_optional => missing_optional_count += 1,
        }
        print("weight_binding_compare: name={s} status={s} shape_match={} count_match={} values={s} rms_rel={d:.6} mean_abs_rel={d:.6} max_abs_rel={d:.6} sample_mean_abs={d:.9} native_used={s} reference_used={s}\n", .{
            lhs.label,
            @tagName(result.status),
            result.shape_match,
            result.count_match,
            result.value_status,
            result.rms_rel,
            result.mean_abs_rel,
            result.max_abs_rel,
            result.sample_mean_abs,
            if (lhs.used_name.len > 0) lhs.used_name else "-",
            if (rhs.?.used_name.len > 0) rhs.?.used_name else "-",
        });
    }

    for (reference.stats) |rhs| {
        if (findWeightBindingStat(native.stats, rhs.label) == null) {
            suspect_count += 1;
            print("weight_binding_compare: name={s} status=suspect reason=missing_native_slot reference_used={s}\n", .{
                rhs.label,
                if (rhs.used_name.len > 0) rhs.used_name else "-",
            });
        }
    }

    print("weight_binding_compare_totals: ok={d} suspect={d} skipped={d} both_missing_optional={d}\n", .{
        ok_count,
        suspect_count,
        skipped_count,
        missing_optional_count,
    });
}

const WeightBindingCompareStatus = enum {
    ok,
    suspect,
    skipped,
    missing_optional,
};

const WeightBindingCompareResult = struct {
    status: WeightBindingCompareStatus,
    value_status: []const u8,
    shape_match: bool,
    count_match: bool,
    rms_rel: f64 = 0,
    mean_abs_rel: f64 = 0,
    max_abs_rel: f64 = 0,
    sample_mean_abs: f64 = 0,
};

fn compareWeightBindingSlot(lhs: WeightBindingSlotStat, rhs: WeightBindingSlotStat) WeightBindingCompareResult {
    const shape_match = shapeEqual(lhs.shape, rhs.shape) or lhs.shape.len == 0 or rhs.shape.len == 0;
    const count_match = lhs.element_count == rhs.element_count or
        ((!lhs.values_ok or !rhs.values_ok) and (lhs.shape.len == 0 or rhs.shape.len == 0));
    var result = WeightBindingCompareResult{
        .status = .ok,
        .value_status = "ok",
        .shape_match = shape_match,
        .count_match = count_match,
    };

    if (!lhs.present or !rhs.present) {
        if (!lhs.present and !rhs.present and lhs.optional and rhs.optional) {
            result.status = .missing_optional;
            result.value_status = "both_missing_optional";
        } else {
            result.status = .suspect;
            result.value_status = "missing";
        }
        return result;
    }

    if (!shape_match or !count_match) {
        result.status = .suspect;
    }

    if (!lhs.values_ok or !rhs.values_ok) {
        result.value_status = "skipped";
        if (result.status == .ok) result.status = .skipped;
        return result;
    }

    result.rms_rel = relativeDiff(lhs.rms, rhs.rms);
    result.mean_abs_rel = relativeDiff(lhs.mean_abs, rhs.mean_abs);
    result.max_abs_rel = relativeDiff(@as(f64, lhs.max_abs), @as(f64, rhs.max_abs));
    result.sample_mean_abs = sampleMeanAbsDiff(lhs, rhs);
    if (result.rms_rel > 0.05 or result.mean_abs_rel > 0.05) {
        result.status = .suspect;
    }
    return result;
}

fn findWeightBindingStat(stats: []const WeightBindingSlotStat, label: []const u8) ?WeightBindingSlotStat {
    for (stats) |stat| {
        if (std.mem.eql(u8, stat.label, label)) return stat;
    }
    return null;
}

fn shapeEqual(lhs: []const i64, rhs: []const i64) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |a, b| {
        if (a != b) return false;
    }
    return true;
}

fn relativeDiff(lhs: f64, rhs: f64) f64 {
    const denom = @max(@max(@abs(lhs), @abs(rhs)), 1.0e-12);
    return @abs(lhs - rhs) / denom;
}

fn sampleMeanAbsDiff(lhs: WeightBindingSlotStat, rhs: WeightBindingSlotStat) f64 {
    const count = @min(lhs.first_count, rhs.first_count);
    if (count == 0) return 0;
    var total: f64 = 0;
    for (0..count) |idx| {
        total += @abs(@as(f64, lhs.first[idx]) - @as(f64, rhs.first[idx]));
    }
    return total / @as(f64, @floatFromInt(count));
}

fn printShape(shape: []const i64) void {
    print("[", .{});
    for (shape, 0..) |dim, idx| {
        if (idx > 0) print(",", .{});
        print("{d}", .{dim});
    }
    print("]", .{});
}

fn printSample(values: []const f32) void {
    print("[", .{});
    for (values, 0..) |value, idx| {
        if (idx > 0) print(",", .{});
        print("{d:.6}", .{value});
    }
    print("]", .{});
}

fn printAnalysisSummary(
    allocator: std.mem.Allocator,
    label: []const u8,
    model: *model_manager_mod.LoadedModel,
    analysis: NativeAnalysis,
) !void {
    print("{s}_backend={s}\n", .{ label, analysis.backend_name });
    print("{s}_rope_layout={s}\n", .{ label, @tagName(analysis.rope_layout) });
    print("{s}_position_encoding={s}\n", .{ label, @tagName(analysis.position_encoding) });
    print("{s}_rope_theta={d:.6} local={d:.6} freq_scale={d:.6}\n", .{ label, analysis.rope_theta, analysis.rope_local_theta, analysis.rope_freq_scale });
    print("{s}_sliding_window={d} pattern={d} norm_eps={d:.8} norm_offset={d:.6} softcap={d:.6}\n", .{
        label,
        analysis.sliding_window,
        analysis.sliding_window_pattern,
        analysis.norm_eps,
        analysis.norm_weight_offset,
        analysis.final_logit_softcapping,
    });
    print("{s}_has_lm_head={}\n", .{ label, analysis.has_lm_head });
    printWeightSamples(label, analysis);
    print("{s}_prompt_token_ids:", .{label});
    for (analysis.prompt_token_ids) |id| print(" {d}", .{id});
    print("\n", .{});
    print("{s}_top_logits:\n", .{label});
    try printTopLogitsFromEntries(allocator, model.getTokenizer(), analysis.top_logits);
    const top1_label = try std.fmt.allocPrint(allocator, "{s}_top1", .{label});
    defer allocator.free(top1_label);
    try printSingleToken(allocator, top1_label, model.getTokenizer(), analysis.top1);
}

fn topLogitOverlap(a: []const TopLogit, b: []const TopLogit) usize {
    var count: usize = 0;
    for (a) |entry| {
        if (topLogitsContain(b, entry.id)) count += 1;
    }
    return count;
}

fn topLogitsContain(entries: []const TopLogit, id: i32) bool {
    for (entries) |entry| {
        if (entry.id == id) return true;
    }
    return false;
}

fn rankOfToken(logits: []const f32, token_id: i32) ?usize {
    if (token_id < 0) return null;
    const idx: usize = @intCast(token_id);
    if (idx >= logits.len) return null;
    const value = logits[idx];
    var rank: usize = 1;
    for (logits) |logit| {
        if (logit > value) rank += 1;
    }
    return rank;
}

fn logitForToken(logits: []const f32, token_id: i32) ?f32 {
    if (token_id < 0) return null;
    const idx: usize = @intCast(token_id);
    if (idx >= logits.len) return null;
    return logits[idx];
}

fn topLogitMargin(entries: []const TopLogit) f32 {
    if (entries.len < 2) return 0;
    return entries[0].logit - entries[1].logit;
}

fn decodeTokenText(allocator: std.mem.Allocator, tok: @import("inference_tokenizer").Tokenizer, token_id: i32) ![]u8 {
    const one = [_]i32{token_id};
    return tok.decode(allocator, &one) catch try allocator.dupe(u8, "");
}

fn isManifestSpecialText(model: *model_manager_mod.LoadedModel, text: []const u8) bool {
    if (text.len == 0) return false;
    return std.mem.eql(u8, text, model.manifest.bos_token) or
        std.mem.eql(u8, text, model.manifest.eos_token) or
        std.mem.eql(u8, text, model.manifest.unk_token) or
        std.mem.eql(u8, text, model.manifest.pad_token);
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

fn runRuntimeParity(
    allocator: std.mem.Allocator,
    model_manager: *model_manager_mod.ModelManager,
    opts: Options,
) !void {
    if (opts.image_count > 0) return error.RuntimeParityTextOnly;
    const native_pref = [_]backends.BackendType{.native};
    const cuda_pref = [_]backends.BackendType{.cuda};
    const native_model = try model_manager.loadFromDirWithPreferredBackends(opts.native_model_dir, native_pref[0..], false);
    const cuda_model = try model_manager.loadFromDirWithPreferredBackends(opts.native_model_dir, cuda_pref[0..], false);
    const native_cfg = session_factory.getGptConfig(native_model.session) orelse return error.InvalidModelForGeneration;
    const cuda_cfg = session_factory.getGptConfig(cuda_model.session) orelse return error.InvalidModelForGeneration;
    if (native_cfg.vocab_size != cuda_cfg.vocab_size or native_cfg.hidden_size != cuda_cfg.hidden_size) return error.IncompatibleRuntimeParityModels;

    const rendered_prompt = try renderPrompt(allocator, native_model, opts.prompt, opts.no_chat_template, opts.raw_prompt);
    defer allocator.free(rendered_prompt);
    const rendered_cuda = try renderPrompt(allocator, cuda_model, opts.prompt, opts.no_chat_template, opts.raw_prompt);
    defer allocator.free(rendered_cuda);
    print("runtime_parity: native_model={s} cuda_model={s}\n", .{ opts.native_model_dir, opts.native_model_dir });
    print("runtime_parity: native_backend={s} cuda_backend={s}\n", .{ @tagName(native_model.session.backend()), @tagName(cuda_model.session.backend()) });
    print("runtime_parity: prompt_match={}\n", .{std.mem.eql(u8, rendered_prompt, rendered_cuda)});
    if (!std.mem.eql(u8, rendered_prompt, rendered_cuda)) return error.RuntimeParityPromptMismatch;

    const token_ids = try encodeRuntimeParityPrompt(allocator, native_model, rendered_prompt);
    defer allocator.free(token_ids);
    const cuda_token_ids = try encodeRuntimeParityPrompt(allocator, cuda_model, rendered_cuda);
    defer allocator.free(cuda_token_ids);
    try compareTokenIds(token_ids, cuda_token_ids);
    print("runtime_parity: prompt_tokens={d}\n", .{token_ids.len});
    print("runtime_parity: prompt_token_ids:", .{});
    for (token_ids[0..@min(token_ids.len, 32)]) |id| print(" {d}", .{id});
    print("\n", .{});

    var native_cb = try session_factory.getComputeBackend(native_model.session, allocator);
    defer native_cb.deinit();
    var cuda_cb = try session_factory.getComputeBackend(cuda_model.session, allocator);
    defer cuda_cb.deinit();

    var native_state = try initRuntimeParityDecodeState(allocator, native_model, native_cfg, &native_cb);
    defer native_state.deinit();
    var cuda_state = try initRuntimeParityDecodeState(allocator, cuda_model, cuda_cfg, &cuda_cb);
    defer cuda_state.deinit();

    try native_state.decode_state.notePrefill(token_ids.len);
    try cuda_state.decode_state.notePrefill(token_ids.len);
    var native_prefill_ctx = native_state.decode_state.gptDecodeContext(token_ids.len, token_ids.len);
    var cuda_prefill_ctx = cuda_state.decode_state.gptDecodeContext(token_ids.len, token_ids.len);
    var native_prefill = try captureRuntimeParityForward(
        allocator,
        native_model,
        &native_cb,
        native_cfg,
        token_ids,
        1,
        token_ids.len,
        &native_prefill_ctx,
        "prefill",
        opts.top_k,
    );
    defer native_prefill.deinit();
    var cuda_prefill = try captureRuntimeParityForward(
        allocator,
        cuda_model,
        &cuda_cb,
        cuda_cfg,
        token_ids,
        1,
        token_ids.len,
        &cuda_prefill_ctx,
        "prefill",
        opts.top_k,
    );
    defer cuda_prefill.deinit();
    try compareRuntimeParityCapture(allocator, "prefill", native_model, &native_prefill, &cuda_prefill);
    print("runtime_parity: prefill ok\n", .{});

    const next_token: i64 = native_prefill.token_id;
    try native_state.decode_state.appendGeneratedToken();
    try cuda_state.decode_state.appendGeneratedToken();
    const decode_seq_len = token_ids.len + 1;
    const decode_input = [_]i64{next_token};
    var native_decode_ctx = native_state.decode_state.gptDecodeContext(decode_seq_len, 1);
    var cuda_decode_ctx = cuda_state.decode_state.gptDecodeContext(decode_seq_len, 1);
    var native_decode = try captureRuntimeParityForward(
        allocator,
        native_model,
        &native_cb,
        native_cfg,
        &decode_input,
        1,
        decode_seq_len,
        &native_decode_ctx,
        "decode",
        opts.top_k,
    );
    defer native_decode.deinit();
    var cuda_decode = try captureRuntimeParityForward(
        allocator,
        cuda_model,
        &cuda_cb,
        cuda_cfg,
        &decode_input,
        1,
        decode_seq_len,
        &cuda_decode_ctx,
        "decode",
        opts.top_k,
    );
    defer cuda_decode.deinit();
    try compareRuntimeParityCapture(allocator, "decode_step", native_model, &native_decode, &cuda_decode);
    print("runtime_parity: decode_step ok\n", .{});
    print("runtime_parity: selected_token_match=true prefill={d} decode={d}\n", .{ native_prefill.token_id, native_decode.token_id });
    printRuntimeParityCudaStats(cuda_model.session);
}

const RuntimeParityDecodeState = struct {
    allocator: std.mem.Allocator,
    kv_manager: *runtime.kv.manager.KvManager,
    kv_storage: *runtime.kv.storage_runtime.KvStorageRuntime,
    decode_state: generation.NativeDecodeState,

    fn deinit(self: *RuntimeParityDecodeState) void {
        self.decode_state.deinit();
        self.kv_storage.deinit();
        self.allocator.destroy(self.kv_storage);
        self.kv_manager.deinit();
        self.allocator.destroy(self.kv_manager);
        self.* = undefined;
    }
};

fn initRuntimeParityDecodeState(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    cfg: gpt_mod.Config,
    cb: *ops.ComputeBackend,
) !RuntimeParityDecodeState {
    const kv_manager = try allocator.create(runtime.kv.manager.KvManager);
    errdefer allocator.destroy(kv_manager);
    kv_manager.* = runtime.kv.manager.KvManager.init(allocator);
    errdefer kv_manager.deinit();
    const backend_kind = try kvBackendKindFromOps(cb.kind());
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const sliding_window_size: ?u32 = if (cfg.position_encoding == .absolute)
        null
    else if (cfg.sliding_window > 0)
        cfg.sliding_window
    else if (cfg.max_position_embeddings > 0)
        cfg.max_position_embeddings
    else
        null;
    const pool_config = runtime.kv.pool.KvPoolConfig{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(cfg.num_hidden_layers),
        .num_kv_heads = cfg.maxKvHeads(),
        .head_dim = cfg.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    };
    const pool_id = try kv_manager.addPool(pool_config);
    const kv_storage = try allocator.create(runtime.kv.storage_runtime.KvStorageRuntime);
    errdefer allocator.destroy(kv_storage);
    kv_storage.* = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, pool_config);
    errdefer kv_storage.deinit();
    try cb.provisionKvDeviceWriteHook(kv_storage);
    var decode_state = generation.NativeDecodeState.initPaged(allocator, kv_manager, pool_id, model.shared_moe_cache);
    decode_state.kv_storage = kv_storage;
    return .{
        .allocator = allocator,
        .kv_manager = kv_manager,
        .kv_storage = kv_storage,
        .decode_state = decode_state,
    };
}

fn kvBackendKindFromOps(kind: ops.BackendKind) !runtime.kv.pool.BackendKind {
    return switch (kind) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        else => error.UnsupportedRuntimeParityBackend,
    };
}

fn encodeRuntimeParityPrompt(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    rendered_prompt: []const u8,
) ![]i64 {
    var encoded = try generation.encodePromptForGeneration(
        model.getTokenizer(),
        allocator,
        rendered_prompt,
        4096,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer encoded.deinit();
    const prompt_tokens = countPromptTokens(encoded.attention_mask);
    if (prompt_tokens == 0) return error.EmptyPrompt;
    const token_ids = try allocator.alloc(i64, prompt_tokens);
    for (0..prompt_tokens) |idx| token_ids[idx] = encoded.ids[idx];
    return token_ids;
}

fn compareTokenIds(native: []const i64, cuda: []const i64) !void {
    if (native.len != cuda.len) {
        print("runtime_parity: prompt_token_ids_match=false native_count={d} cuda_count={d}\n", .{ native.len, cuda.len });
        return error.RuntimeParityTokenizationMismatch;
    }
    for (native, cuda, 0..) |lhs, rhs, idx| {
        if (lhs != rhs) {
            print("runtime_parity: prompt_token_ids_match=false first_mismatch={d} native={d} cuda={d}\n", .{ idx, lhs, rhs });
            return error.RuntimeParityTokenizationMismatch;
        }
    }
    print("runtime_parity: prompt_token_ids_match=true\n", .{});
}

fn captureRuntimeParityForward(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    cb: *ops.ComputeBackend,
    cfg: gpt_mod.Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    phase: []const u8,
    top_k: usize,
) !RuntimeParityCapture {
    const query_seq_len = decode_context.query_sequence_len;
    const total = batch * query_seq_len;
    const hidden_size: usize = @intCast(cfg.hidden_size);
    if (input_ids.len != total) return error.InvalidTensorShape;

    const embed_w = try gpt_arch.getEmbeddingWeight(cb, cfg);
    defer cb.free(embed_w);
    const embedded = try cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
    const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(cb, allocator, cfg, embedded, total, hidden_size);

    const ple_vectors = try gpt_arch.computePleVectors(cb, allocator, cfg, input_ids, hidden_input, total);
    defer if (ple_vectors) |pv| cb.free(pv);

    const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        cfg,
        hidden_input,
        .{},
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    defer cb.free(hidden_result.final_hidden);
    defer cb.free(hidden_result.pre_norm_hidden);

    const lm_w = try gpt_arch.getLmHeadWeight(cb, cfg);
    defer cb.free(lm_w);

    const logits_ct = try cb.linearNoBias(hidden_result.final_hidden, lm_w, hidden_result.total_rows, cfg.hidden_size, cfg.vocab_size);
    defer cb.free(logits_ct);
    const logits_host = try cb.toFloat32(logits_ct, allocator);
    errdefer allocator.free(logits_host);
    gpt_arch.applyFinalLogitSoftcapInPlace(cfg, logits_host);
    const final_hidden = try cb.toFloat32(hidden_result.final_hidden, allocator);
    errdefer allocator.free(final_hidden);
    const pre_norm_hidden = try cb.toFloat32(hidden_result.pre_norm_hidden, allocator);
    errdefer allocator.free(pre_norm_hidden);
    const vocab_size: usize = @intCast(cfg.vocab_size);
    const last_logits = logits_host[(hidden_result.total_rows - 1) * vocab_size ..][0..vocab_size];
    const token_id: i32 = @intCast(activations.argmax(last_logits));
    const token_arr = [_]i32{token_id};
    const token_text = try model.getTokenizer().decode(allocator, &token_arr);
    errdefer allocator.free(token_text);
    const top_logits = try collectTopLogits(allocator, last_logits, top_k);
    errdefer allocator.free(top_logits);
    return .{
        .allocator = allocator,
        .backend_name = @tagName(model.session.backend()),
        .phase = phase,
        .total_rows = hidden_result.total_rows,
        .vocab_size = vocab_size,
        .token_id = token_id,
        .token_text = token_text,
        .final_hidden = final_hidden,
        .pre_norm_hidden = pre_norm_hidden,
        .logits = logits_host,
        .top_logits = top_logits,
    };
}

fn compareRuntimeParityCapture(
    allocator: std.mem.Allocator,
    phase: []const u8,
    model: *model_manager_mod.LoadedModel,
    native: *const RuntimeParityCapture,
    cuda: *const RuntimeParityCapture,
) !void {
    print("runtime_parity: {s} native_token={d} cuda_token={d}\n", .{ phase, native.token_id, cuda.token_id });
    try printSingleToken(allocator, "runtime_parity_native_top1", model.getTokenizer(), native.token_id);
    try printSingleToken(allocator, "runtime_parity_cuda_top1", model.getTokenizer(), cuda.token_id);
    if (native.total_rows != cuda.total_rows or native.vocab_size != cuda.vocab_size) return error.RuntimeParityShapeMismatch;
    try compareRuntimeTensor(phase, "pre_norm_hidden", cuda.pre_norm_hidden, native.pre_norm_hidden, 0.01);
    try compareRuntimeTensor(phase, "final_hidden", cuda.final_hidden, native.final_hidden, 0.01);
    try compareRuntimeTensor(phase, "logits", cuda.logits, native.logits, 0.1);
    compareRuntimeTopLogits(phase, native.top_logits, cuda.top_logits);
    if (native.token_id != cuda.token_id) {
        print("runtime_parity: first_divergence phase={s} surface=selected_token native={d} cuda={d}\n", .{ phase, native.token_id, cuda.token_id });
        return error.RuntimeParityTokenMismatch;
    }
}

fn compareRuntimeTensor(phase: []const u8, surface: []const u8, actual: []const f32, expected: []const f32, tolerance: f32) !void {
    if (actual.len != expected.len) return error.RuntimeParityShapeMismatch;
    const stats = runtimeDiffStats(actual, expected);
    print("runtime_parity: {s}.{s} max_abs={d:.6} mean_abs={d:.6} max_index={d}\n", .{ phase, surface, stats.max_abs, stats.mean_abs, stats.max_index });
    if (stats.max_abs > tolerance) {
        print(
            "runtime_parity: first_divergence phase={s} surface={s} native={d:.6} cuda={d:.6}\n",
            .{ phase, surface, expected[stats.max_index], actual[stats.max_index] },
        );
        return error.RuntimeParityTensorMismatch;
    }
}

fn runtimeDiffStats(actual: []const f32, expected: []const f32) RuntimeParityDiff {
    var stats: RuntimeParityDiff = .{};
    var sum_abs: f64 = 0;
    for (actual, expected, 0..) |got, want, idx| {
        const diff = @abs(got - want);
        sum_abs += diff;
        if (diff > stats.max_abs) {
            stats.max_abs = diff;
            stats.max_index = idx;
        }
    }
    stats.mean_abs = sum_abs / @as(f64, @floatFromInt(actual.len));
    return stats;
}

fn compareRuntimeTopLogits(phase: []const u8, native: []const TopLogit, cuda: []const TopLogit) void {
    const count = @min(native.len, cuda.len);
    print("runtime_parity: {s}.top_logits\n", .{phase});
    for (0..count) |idx| {
        print("  rank={d} native={d}:{d:.6} cuda={d}:{d:.6}\n", .{ idx + 1, native[idx].id, native[idx].logit, cuda[idx].id, cuda[idx].logit });
    }
}

fn printRuntimeParityCudaStats(session: backends.Session) void {
    if (comptime !build_options.enable_cuda) return;
    if (session_factory.getCudaRuntimeStats(session)) |stats| {
        print(
            "runtime_parity_cuda_stats: launches={d} syncs={d} upload_syncs={d} download_syncs={d} linear={d} attention={d} h2d={d} d2h={d} device_kv_attempts={d} device_kv_successes={d} host_attention_fallbacks={d}\n",
            .{
                stats.kernel_launches,
                stats.stream_syncs,
                stats.upload_syncs,
                stats.download_syncs,
                stats.launch_linear,
                stats.launch_attention,
                stats.h2d_bytes,
                stats.d2h_bytes,
                stats.device_kv_attempts,
                stats.device_kv_successes,
                stats.host_attention_fallbacks,
            },
        );
    }
}

fn renderPrompt(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
) ![]u8 {
    if (raw_prompt or no_chat_template) {
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

fn makeAnalyzeRunBudget(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    cfg: gpt_mod.Config,
    prompt_tokens: usize,
    opts: Options,
) !runtime.tier.memory.RunBudget {
    const backend_kind = backendKindForSession(model.session.backend()) orelse return error.UnsupportedBudgetBackend;
    const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
        .native => .cpu,
        else => .gpu,
    };
    var budget_limits = runtime.tier.memory.defaultLimitsForBackend(budget_backend_class);
    budget_limits = session_factory.widenBudgetLimitsForSession(model.session, budget_limits);
    budget_limits = try applyBudgetOverrides(budget_limits, opts);

    var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    run_budget.reserveEstimate(try runtime.tier.memory.estimateGptGeneration(
        backend_kind,
        kv_dtype,
        cfg,
        prompt_tokens,
        1,
        256,
    )) catch |err| {
        if (err == error.MemoryBudgetExceeded) printCompareBudgetExceeded(model.session, &run_budget);
        return err;
    };

    _ = allocator;
    print("budget: host={d}MiB backend={d}MiB combined={d}MiB kv={d}MiB scratch={d}MiB\n", .{
        budget_limits.host_limit_bytes / (1024 * 1024),
        budget_limits.backend_limit_bytes / (1024 * 1024),
        budget_limits.combined_limit_bytes / (1024 * 1024),
        budget_limits.kv_limit_bytes / (1024 * 1024),
        budget_limits.scratch_limit_bytes / (1024 * 1024),
    });
    return run_budget;
}

fn backendKindForSession(backend: backends.BackendType) ?runtime.kv.pool.BackendKind {
    return switch (backend) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        else => null,
    };
}

fn applyBudgetOverrides(defaults: runtime.tier.memory.Limits, opts: Options) !runtime.tier.memory.Limits {
    return (runtime.tier.memory.BudgetOverridesMib{
        .host = opts.host_budget_mb,
        .backend = opts.backend_budget_mb,
        .combined = opts.combined_budget_mb,
        .kv = opts.kv_budget_mb,
        .scratch = opts.scratch_budget_mb,
    }).apply(defaults);
}

fn printCompareBudgetExceeded(
    session: backends.Session,
    run_budget: *const runtime.tier.memory.RunBudget,
) void {
    var buf: [512]u8 = undefined;
    const msg = session_factory.memoryBudgetExceededDetail(session, run_budget, &buf) catch "memory budget exceeded";
    print("{s}\n", .{msg});
}

fn analyzeNativeModel(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    opts: Options,
) !NativeAnalysis {
    const rendered_prompt = try renderPrompt(allocator, model, opts.prompt, opts.no_chat_template, opts.raw_prompt);
    errdefer allocator.free(rendered_prompt);

    const tok = model.getTokenizer();
    const cfg = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
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

    var run_budget = try makeAnalyzeRunBudget(allocator, model, cfg, prompt_tokens, opts);
    var cb = try session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget);
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

    const logits = try gpt_arch.forward(&cb, allocator, cfg, input_ids, 1, prompt_tokens, null);
    defer allocator.free(logits);
    const vocab = cfg.vocab_size;
    const last_logits = logits[(prompt_tokens - 1) * vocab ..][0..vocab];
    const top1: i32 = @intCast(activations.argmax(last_logits));
    const top_logits = try collectTopLogits(allocator, last_logits, opts.top_k);
    errdefer allocator.free(top_logits);
    const last_logits_copy = try allocator.dupe(f32, last_logits);
    errdefer allocator.free(last_logits_copy);

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
        .last_logits = last_logits_copy,
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
        } else if (std.mem.eql(u8, arg, "--native-backend")) {
            i += 1;
            if (i >= args.len) return error.MissingNativeBackendValue;
            opts.native_backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--reference-backend")) {
            i += 1;
            if (i >= args.len) return error.MissingReferenceBackendValue;
            opts.reference_backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
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
        } else if (std.mem.eql(u8, arg, "--raw-prompt")) {
            opts.raw_prompt = true;
        } else if (std.mem.eql(u8, arg, "--image-features-only")) {
            opts.image_features_only = true;
        } else if (std.mem.eql(u8, arg, "--onnx-prompt-embeddings-only")) {
            opts.onnx_prompt_embeddings_only = true;
        } else if (std.mem.eql(u8, arg, "--runtime-parity")) {
            opts.runtime_parity = true;
        } else if (std.mem.eql(u8, arg, "--sequential")) {
            opts.sequential_compare = true;
        } else if (std.mem.eql(u8, arg, "--quality-eval")) {
            opts.quality_eval = true;
        } else if (std.mem.eql(u8, arg, "--weight-binding-audit")) {
            opts.weight_binding_audit = true;
        } else if (std.mem.eql(u8, arg, "--activation-trace")) {
            opts.activation_trace = true;
        } else if (std.mem.eql(u8, arg, "--prompt-file")) {
            i += 1;
            if (i >= args.len) return error.MissingPromptFile;
            opts.prompt_file = args[i];
        } else if (std.mem.eql(u8, arg, "--max-prompts")) {
            i += 1;
            if (i >= args.len) return error.MissingMaxPrompts;
            opts.max_prompts = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--json-out")) {
            i += 1;
            if (i >= args.len) return error.MissingJsonOutPath;
            opts.json_out_path = args[i];
        } else if (std.mem.eql(u8, arg, "--activation-trace-all-rows")) {
            opts.activation_trace_all_rows = true;
        } else if (std.mem.eql(u8, arg, "--binding-audit-layer-limit")) {
            i += 1;
            if (i >= args.len) return error.MissingBindingAuditLayerLimit;
            opts.binding_audit_layer_limit = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--activation-trace-layer-limit")) {
            i += 1;
            if (i >= args.len) return error.MissingActivationTraceLayerLimit;
            opts.activation_trace_layer_limit = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--activation-trace-layer")) {
            i += 1;
            if (i >= args.len) return error.MissingActivationTraceLayer;
            opts.activation_trace_layer = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--activation-trace-row")) {
            i += 1;
            if (i >= args.len) return error.MissingActivationTraceRow;
            opts.activation_trace_row = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--host-budget-mb")) {
            i += 1;
            if (i >= args.len) return error.MissingHostBudget;
            opts.host_budget_mb = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--backend-budget-mb")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendBudget;
            opts.backend_budget_mb = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--combined-budget-mb")) {
            i += 1;
            if (i >= args.len) return error.MissingCombinedBudget;
            opts.combined_budget_mb = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--kv-budget-mb")) {
            i += 1;
            if (i >= args.len) return error.MissingKvBudget;
            opts.kv_budget_mb = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--scratch-budget-mb")) {
            i += 1;
            if (i >= args.len) return error.MissingScratchBudget;
            opts.scratch_budget_mb = try std.fmt.parseInt(usize, args[i], 10);
        } else {
            return error.UnknownArgument;
        }
    }
    return opts;
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => &.{ .onnx, .cuda, .metal, .native },
        .native => &.{ .native, .onnx, .metal },
        .cuda => &.{ .cuda, .native },
        .metal => &.{ .metal, .onnx, .native },
    };
}

fn printUsage() void {
    print("usage: antfly inference compare <native-model-dir> <reference-model-dir> <prompt> [--image path] [--image-features-only] [--onnx-prompt-embeddings-only] [--runtime-parity] [--sequential] [--quality-eval] [--prompt-file PATH] [--max-prompts N] [--json-out PATH] [--weight-binding-audit] [--binding-audit-layer-limit N] [--activation-trace] [--activation-trace-all-rows] [--activation-trace-layer-limit N] [--activation-trace-layer N] [--activation-trace-row N] [--backend auto|native|cuda|metal] [--native-backend auto|native|cuda|metal] [--reference-backend auto|native|cuda|metal] [--top-k N] [--host-budget-mb N] [--backend-budget-mb N] [--combined-budget-mb N] [--kv-budget-mb N] [--scratch-budget-mb N] [--no-chat-template] [--raw-prompt]\n", .{});
}
