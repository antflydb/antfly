// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Public generation qualification. Whole-model attachment makes a declined
//! compiled graph an error, rather than reporting native fallback as PJRT.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const generation = inference.pipelines.generation;

pub fn run(init: std.process.Init, model: *inference.server.model_manager.LoadedModel, a: std.mem.Allocator, options: anytype) !void {
    const config = factory.getGptConfig(model.session) orelse return error.MissingDecoderConfig;
    const is_pjrt = options.backend == .xla;
    const client = factory.getPjrtClientPtr(model.session);
    if (is_pjrt and (model.session.backend() != .pjrt or client == null)) return error.GenerationBackendSelectionMismatch;
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    var cache = inference.graph.cache.GraphCache.init(a);
    defer cache.deinitWithBackend(&cb);
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = a,
        .io = init.io,
        .cb = cb,
        .gpt_config = config,
        .session = model.session,
        .tokenizer = model.getTokenizer(),
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .chat_template = model.chat_tmpl,
        .return_raw_token_ids = true,
        .model_dir = options.model_dir,
        .graph_cache = if (is_pjrt) &cache else null,
        .compiled_partition_backend = if (is_pjrt) .pjrt else null,
        .compiled_attachment_target = .whole_model,
        .pjrt_client = client,
    };
    defer pipeline.deinit();
    const messages = [_]generation.Message{.{ .role = "user", .content = options.generate_text.? }};
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    var reference: ?generation.GenerationResult = null;
    defer if (reference) |*value| value.deinit();
    var cold_ns: u64 = 0;
    for (0..options.warmup + options.iterations) |i| {
        const start = inference.platform.time.awakeNs(init.io);
        var result = try pipeline.generate(&messages, .{ .max_tokens = @intCast(options.generate_tokens), .temperature = 0, .enable_thinking = false });
        const elapsed = inference.platform.time.awakeNs(init.io) - start;
        if (i == 0) cold_ns = elapsed;
        if (i >= options.warmup) samples[i - options.warmup] = elapsed;
        if (reference) |expected| {
            defer result.deinit();
            if (!std.mem.eql(u8, expected.text, result.text) or expected.tokens_used != result.tokens_used) return error.GenerationResetOutputChanged;
            if (expected.token_ids) |tokens| {
                if (!std.mem.eql(i32, tokens, result.token_ids orelse return error.GenerationResetOutputChanged)) return error.GenerationResetOutputChanged;
            }
        } else reference = result;
    }
    const held = reference orelse return error.NoGenerationOutput;
    const report = try std.json.Stringify.valueAlloc(a, .{
        .task = "public_generation",
        .model_dir = options.model_dir,
        .backend = @tagName(model.session.backend()),
        .pjrt_device = if (is_pjrt) @tagName(options.pjrt.device) else null,
        .pjrt_precision = if (is_pjrt) @tagName(options.pjrt.precision) else null,
        .whole_model_required = is_pjrt,
        .decode_execution_scope = "offline_full_prefix_recomputation",
        .cached_serving_performance_qualified = false,
        .model_load_in_timing = false,
        .model_chat_template = model.chat_tmpl != null,
        .runtime_capabilities = if (cache.compiled_model_runtime) |*runtime| runtime.capabilities() else null,
        .cached_shapes = cache.specializations.entries.items.len,
        .cold_request_ns = cold_ns,
        .samples_ns = samples,
        .warmup = options.warmup,
        .text = held.text,
        .token_ids = held.token_ids,
        .tokens_used = held.tokens_used,
        .prompt_tokens = held.prompt_tokens,
        .finish_reason = held.finish_reason,
    }, .{});
    defer a.free(report);
    try std.Io.File.stdout().writeStreamingAll(init.io, report);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}
