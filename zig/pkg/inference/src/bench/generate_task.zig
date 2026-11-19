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
    const comparison = options.generate_cache_comparison;
    if (comparison and (!is_pjrt or !try generation.selectPjrtDecoderCache(model.session, true))) return error.UnsupportedResidentDecode;
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
    const has_media = options.generate_image != null or options.generate_audio != null or options.generate_video != null;
    const projector = if (has_media) try model.ensureProjectorStoreWithControl(cb.execution_control) else null;
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = a,
        .io = init.io,
        .cb = cb,
        .execution_control = cb.execution_control,
        .gpt_config = config,
        .kv_dtype = if (storage) |*owned| owned.storage.config.dtype else null,
        .session = model.session,
        .tokenizer = model.getTokenizer(),
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .chat_template = model.chat_tmpl,
        .return_raw_token_ids = true,
        .model_dir = options.model_dir,
        .gguf_projector_path = model.manifest.gguf_projector_path,
        .projector_store = projector,
        .graph_cache = if (is_pjrt) &cache else null,
        .compiled_partition_backend = if (is_pjrt) .pjrt else null,
        .compiled_attachment_target = .whole_model,
        .pjrt_client = client,
        .pjrt_media_bucket_rows = options.pjrt.media_bucket_rows,
        .pjrt_decoder_cache = options.pjrt.decoder_cache,
    };
    defer pipeline.deinit();
    const text_bytes = if (options.generate_text_file) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(16 * 1024 * 1024)) else null;
    defer if (text_bytes) |bytes| a.free(bytes);
    const image_bytes = if (options.generate_image) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024)) else null;
    defer if (image_bytes) |bytes| a.free(bytes);
    const audio_bytes = if (options.generate_audio) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024)) else null;
    defer if (audio_bytes) |bytes| a.free(bytes);
    const video_bytes = if (options.generate_video) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(256 * 1024 * 1024)) else null;
    defer if (video_bytes) |bytes| a.free(bytes);
    const image_inputs = [_][]const u8{image_bytes orelse &.{}};
    const audio_inputs = [_][]const u8{audio_bytes orelse &.{}};
    const messages = [_]generation.Message{.{ .role = "user", .content = text_bytes orelse options.generate_text.?, .image_bytes = if (image_bytes != null) &image_inputs else null, .audio_bytes = if (audio_bytes != null) &audio_inputs else null, .video_bytes = video_bytes }};
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    const stage_samples = try a.alloc(?generation.GenerationTimingMs, options.iterations);
    defer a.free(stage_samples);
    const cached_stage_samples = try a.alloc(?generation.GenerationTimingMs, if (comparison) options.iterations else 0);
    defer a.free(cached_stage_samples);
    var reference: ?generation.GenerationResult = null;
    defer if (reference) |*value| value.deinit();
    const cached_samples = try a.alloc(u64, if (comparison) options.iterations else 0);
    defer a.free(cached_samples);
    var counts: [2]usize = @splat(0);
    var cold: [2]u64 = @splat(0);
    const requests = try std.math.mul(usize, try std.math.add(usize, options.warmup, options.iterations), if (comparison) @as(usize, 2) else 1);
    const cuda_before = factory.getCudaRuntimeStats(model.session);
    var cuda_before_measured: @TypeOf(cuda_before) = null;
    // Cooperative stepping keeps two warmed providers resident while a
    // controller alternates requests. Waits and protocol I/O stay outside the
    // timer; ordinary invocation and cleanup are identical to ungated runs.
    var control_buffer: [64]u8 = undefined;
    var control = std.Io.File.stdin().readerStreaming(init.io, &control_buffer);
    for (0..requests) |i| {
        const mode: usize = if (comparison and (i % 4 == 1 or i % 4 == 2)) 1 else 0;
        if (comparison) pipeline.pjrt_decoder_cache = mode == 1;
        // Native requests have one contiguous measured cohort. Snapshot after
        // warmup, outside request timing: cold weight conversion and program
        // construction must not be attributed to steady-state GPU work.
        // The interleaved comparison is PJRT-only and has no CUDA counters.
        if (!comparison and i == options.warmup) cuda_before_measured = factory.getCudaRuntimeStats(model.session);
        if (options.generate_control_stdin and i >= options.warmup) {
            if (i == options.warmup) try std.Io.File.stderr().writeStreamingAll(init.io, "GENERATION_MEASURE_READY\n");
            const command = (try control.interface.takeDelimiter('\n')) orelse return error.MeasurementControlClosed;
            if (!std.mem.eql(u8, command, "run")) return error.InvalidMeasurementControl;
        }
        const start = inference.platform.time.awakeNs(init.io);
        // This benchmark reuses one backend, whereas serving acquires it for
        // each request. Reset captured temporaries before media/prompt tensors
        // are created, while keeping immutable model caches and PJRT programs.
        if (!is_pjrt) try pipeline.cb.beginRequest();
        var result = request: {
            var decode: ?generation.NativeDecodeState = if (request_pool) |pool| generation.NativeDecodeState.initPaged(a, &kv, pool, null) else null;
            defer if (decode) |*state| state.deinit();
            if (decode) |*state| {
                state.kv_storage = &storage.?;
                state.configureForGptConfig(config);
                pipeline.decode_state = state;
            }
            defer pipeline.decode_state = null;
            break :request try pipeline.generate(&messages, .{ .max_tokens = @intCast(options.generate_tokens), .temperature = 0, .enable_thinking = false, .prefill_chunk_size = options.generate_prefill_chunk_rows });
        };
        const elapsed = inference.platform.time.awakeNs(init.io) - start;
        if (counts[mode] == 0) cold[mode] = elapsed;
        if (counts[mode] >= options.warmup) {
            const destination = if (mode == 1) cached_samples else samples;
            const index = counts[mode] - options.warmup;
            destination[index] = elapsed;
            const stages = if (mode == 1) cached_stage_samples else stage_samples;
            stages[index] = result.timing_ms;
        }
        counts[mode] += 1;
        if (reference) |expected| {
            defer result.deinit();
            if (!std.mem.eql(u8, expected.text, result.text) or expected.tokens_used != result.tokens_used) return error.GenerationResetOutputChanged;
            if (expected.token_ids) |tokens| {
                if (!std.mem.eql(i32, tokens, result.token_ids orelse return error.GenerationResetOutputChanged)) return error.GenerationResetOutputChanged;
            }
        } else reference = result;
        if (options.generate_control_stdin and i >= options.warmup) {
            var response_buffer: [128]u8 = undefined;
            const response = try std.fmt.bufPrint(&response_buffer, "GENERATION_MEASURE_SAMPLE {d} {d}\n", .{ i - options.warmup, elapsed });
            try std.Io.File.stderr().writeStreamingAll(init.io, response);
        }
    }
    if (options.generate_control_stdin) {
        const command = (try control.interface.takeDelimiter('\n')) orelse return error.MeasurementControlClosed;
        if (!std.mem.eql(u8, command, "finish")) return error.InvalidMeasurementControl;
    }
    const cuda_after = factory.getCudaRuntimeStats(model.session);
    const cuda_counts = if (cuda_before != null and cuda_after != null) factory.cudaStatsDelta(cuda_after.?, cuda_before.?) else null;
    const held = reference orelse return error.NoGenerationOutput;
    const report = try std.json.Stringify.valueAlloc(a, .{
        .task = "public_generation",
        .model_dir = options.model_dir,
        .backend = @tagName(model.session.backend()),
        .pjrt_execution_profile = factory.getPjrtExecutionProfile(model.session),
        .pjrt_device = if (is_pjrt) @tagName(options.pjrt.device) else null,
        .pjrt_media_bucket_rows = options.pjrt.media_bucket_rows,
        .pjrt_decoder_cache_requested = options.pjrt.decoder_cache,
        .pjrt_decoder_cache = pipeline.pjrt_decoder_cache_used,
        .pjrt_gemm = if (is_pjrt) @tagName(options.pjrt.gemm) else null,
        .pjrt_precision = if (is_pjrt) @tagName(options.pjrt.precision) else null,
        .whole_model_required = is_pjrt,
        .modalities = .{ .image = image_bytes != null, .audio = audio_bytes != null, .video = video_bytes != null },
        .decode_execution_scope = if (comparison) "full_prefix_vs_resident_ABBA" else if (is_pjrt) (if (pipeline.pjrt_decoder_cache_used) "request_resident_state" else "offline_full_prefix_recomputation") else "native_paged_kv",
        .cached_serving_performance_qualified = false,
        .kv_cached = !is_pjrt or pipeline.pjrt_decoder_cache_used,
        .native_kv_dtype = pipeline.kv_dtype,
        .model_load_in_timing = false,
        .model_context_tokens = config.max_position_embeddings,
        .max_output_tokens = options.generate_tokens,
        .requested_prefill_chunk_rows = options.generate_prefill_chunk_rows,
        .measurement_control = if (options.generate_control_stdin) "stdin_after_warmup" else "uncontrolled",
        .model_chat_template = model.chat_tmpl != null,
        .runtime_capabilities = if (cache.compiled_model_runtime) |*runtime| runtime.capabilities() else null,
        .compiled_stage_shapes = factory.getPjrtSpecializationCount(model.session),
        .cached_shapes = cache.specializations.entries.items.len,
        .cache_comparison = if (comparison) .{ .order = "ABBA", .same_tokens = true, .baseline_samples_ns = samples, .cached_samples_ns = cached_samples, .cached_cold_request_ns = cold[1] } else null,
        .cold_request_ns = cold[0],
        .samples_ns = samples,
        .stage_samples_ms = stage_samples,
        .cuda_request_counters = cuda_counts,
        .cuda_measured_counters = if (cuda_before_measured != null and cuda_after != null) factory.cudaStatsDelta(cuda_after.?, cuda_before_measured.?) else null,
        .cuda_profile_intervals = "nested; category durations are not additive",
        .cached_stage_samples_ms = if (comparison) cached_stage_samples else null,
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
