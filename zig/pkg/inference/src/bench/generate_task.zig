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
    cb.execution_control = .{ .native_call_policy = .in_process, .io = init.io };
    var cache = inference.graph.cache.GraphCache.init(a);
    defer cache.deinitWithBackend(&cb);
    var kv = inference.runtime.kv.manager.KvManager.init(a);
    defer kv.deinit();
    var storage: ?inference.runtime.kv.storage_runtime.KvStorageRuntime = null;
    defer if (storage) |*value| value.deinit();
    var request_pool: ?inference.runtime.kv.block.KvPoolId = null;
    if (!is_pjrt) {
        const kind: inference.runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            else => return error.GenerationBackendSelectionMismatch,
        };
        const dtype = factory.recommendedKvDTypeForSession(model.session, kind);
        const pool_config = generation.kvPoolConfig(kind, dtype, config, false);
        const pool_id = try kv.addPool(generation.requestKvManagerPoolConfig(pool_config));
        storage = try inference.runtime.kv.storage_runtime.KvStorageRuntime.init(a, pool_config);
        try cb.provisionKvDeviceWriteHook(&storage.?);
        request_pool = pool_id;
    }
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = a,
        .io = init.io,
        .cb = cb,
        .execution_control = cb.execution_control,
        .gpt_config = config,
        .session = model.session,
        .tokenizer = model.getTokenizer(),
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .chat_template = model.chat_tmpl,
        .return_raw_token_ids = true,
        .model_dir = options.model_dir,
        .gguf_projector_path = model.manifest.gguf_projector_path,
        .graph_cache = if (is_pjrt) &cache else null,
        .compiled_partition_backend = if (is_pjrt) .pjrt else null,
        .compiled_attachment_target = .whole_model,
        .pjrt_client = client,
        .pjrt_media_bucket_rows = options.pjrt.media_bucket_rows,
    };
    defer pipeline.deinit();
    const image_bytes = if (options.generate_image) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024)) else null;
    defer if (image_bytes) |bytes| a.free(bytes);
    const audio_bytes = if (options.generate_audio) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024)) else null;
    defer if (audio_bytes) |bytes| a.free(bytes);
    const video_bytes = if (options.generate_video) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(256 * 1024 * 1024)) else null;
    defer if (video_bytes) |bytes| a.free(bytes);
    const image_inputs = [_][]const u8{image_bytes orelse &.{}};
    const audio_inputs = [_][]const u8{audio_bytes orelse &.{}};
    const messages = [_]generation.Message{.{ .role = "user", .content = options.generate_text.?, .image_bytes = if (image_bytes != null) &image_inputs else null, .audio_bytes = if (audio_bytes != null) &audio_inputs else null, .video_bytes = video_bytes }};
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    var reference: ?generation.GenerationResult = null;
    defer if (reference) |*value| value.deinit();
    var cold_ns: u64 = 0;
    for (0..options.warmup + options.iterations) |i| {
        const start = inference.platform.time.awakeNs(init.io);
        var result = request: {
            var decode: ?generation.NativeDecodeState = if (request_pool) |pool| generation.NativeDecodeState.initPaged(a, &kv, pool, null) else null;
            defer if (decode) |*state| state.deinit();
            if (decode) |*state| {
                state.kv_storage = &storage.?;
                state.configureForGptConfig(config);
                pipeline.decode_state = state;
            }
            defer pipeline.decode_state = null;
            break :request try pipeline.generate(&messages, .{ .max_tokens = @intCast(options.generate_tokens), .temperature = 0, .enable_thinking = false });
        };
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
        .pjrt_execution_profile = factory.getPjrtExecutionProfile(model.session),
        .pjrt_device = if (is_pjrt) @tagName(options.pjrt.device) else null,
        .pjrt_media_bucket_rows = options.pjrt.media_bucket_rows,
        .pjrt_gemm = if (is_pjrt) @tagName(options.pjrt.gemm) else null,
        .pjrt_precision = if (is_pjrt) @tagName(options.pjrt.precision) else null,
        .whole_model_required = is_pjrt,
        .modalities = .{ .image = image_bytes != null, .audio = audio_bytes != null, .video = video_bytes != null },
        .decode_execution_scope = if (is_pjrt) "offline_full_prefix_recomputation" else "native_paged_kv",
        .cached_serving_performance_qualified = false,
        .kv_cached = !is_pjrt,
        .model_load_in_timing = false,
        .model_chat_template = model.chat_tmpl != null,
        .runtime_capabilities = if (cache.compiled_model_runtime) |*runtime| runtime.capabilities() else null,
        .compiled_stage_shapes = factory.getPjrtSpecializationCount(model.session),
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
