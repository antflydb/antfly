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
const backends = @import("backends/backends.zig");
const decoder_gated_runtime = @import("backends/decoder_gated_runtime.zig");
const debug_timing = @import("debug_timing.zig");
const ops = @import("ops/ops.zig");
const gpt_arch = @import("architectures/gpt.zig");
const session_factory = @import("architectures/session_factory.zig");
const generation = @import("pipelines/generation.zig");
const graph_mod = @import("graph/root.zig");
const onnx_decoder_only_vlm = @import("pipelines/onnx_decoder_only_vlm.zig");
const model_manager_mod = @import("server/model_manager.zig");
const manifest_mod = @import("models/manifest.zig");
const runtime = @import("runtime/root.zig");
const c_file = @import("util/c_file.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const native_run_artifact = @import("native_run_artifact.zig");
const compiled_artifact = @import("compiled_artifact.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const tokenizer_mod = @import("inference_tokenizer");
const pjrt_lib = if (build_options.enable_pjrt) @import("pjrt") else struct {
    pub const pjrt = struct {
        pub const Client = struct {
            pub fn init(_: [:0]const u8) !@This() {
                return error.PjrtNotEnabled;
            }
            pub fn deinit(_: *@This()) void {}
        };
    };
};

const print = std.debug.print;
const BackendChoice = native_backend_choice.Choice;

const ExecutionMode = enum {
    eager,
    compiled,
};

const CompiledTarget = graph_mod.compiled_backend.AttachmentTarget;

const Options = struct {
    model_dir: []const u8,
    prompt: []const u8,
    image_paths: [8][]const u8 = @splat(""),
    image_count: usize = 0,
    audio_paths: [8][]const u8 = @splat(""),
    audio_count: usize = 0,
    backend: BackendChoice = .auto,
    max_tokens: i32 = 128,
    temperature: f32 = 0,
    top_p: f32 = 0,
    top_k: i32 = 0,
    repetition_penalty: f32 = 1.0,
    prefill_chunk_size: usize = 0,
    draft_model: ?[]const u8 = null,
    speculative_k: u32 = 4,
    no_chat_template: bool = false,
    print_finish_reason: bool = false,
    print_token_count: bool = false,
    print_token_ids: bool = false,
    print_prompt_token_ids: bool = false,
    print_prompt: bool = false,
    print_chat_template_status: bool = false,
    print_timing: bool = false,
    host_budget_mb: usize = 0,
    backend_budget_mb: usize = 0,
    combined_budget_mb: usize = 0,
    kv_budget_mb: usize = 0,
    scratch_budget_mb: usize = 0,
    raw_prompt: bool = false,
    no_bos: bool = false,
    cache_dtype: ?[]const u8 = null,
    cache_compaction_ratio: ?f32 = null,
    mode: ?ExecutionMode = null,
    compiled_target: ?CompiledTarget = null,
    artifact_dir: ?[]const u8 = null,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try native_backend_choice.validate(opts.backend);
    if (opts.draft_model != null and opts.backend == .onnx) return error.SpeculativeDecodingRequiresNativeBackend;
    const started_at = std.Io.Timestamp.now(io, .awake);

    var preflight_manifest = try manifest_mod.loadFromDir(allocator, opts.model_dir);
    defer preflight_manifest.deinit();
    try preflightModelLoadBudget(allocator, &preflight_manifest, opts);

    var loaded_images = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (loaded_images.items) |image_bytes| allocator.free(image_bytes);
        loaded_images.deinit(allocator);
    }
    var loaded_audio = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (loaded_audio.items) |audio_bytes| allocator.free(audio_bytes);
        loaded_audio.deinit(allocator);
    }
    var message_images = std.ArrayListUnmanaged([]const u8).empty;
    defer message_images.deinit(allocator);
    var message_audio = std.ArrayListUnmanaged([]const u8).empty;
    defer message_audio.deinit(allocator);
    var content_parts = std.ArrayListUnmanaged(generation.Message.ContentPart).empty;
    defer content_parts.deinit(allocator);

    for (0..opts.image_count) |idx| {
        const image_bytes = try std.Io.Dir.cwd().readFileAlloc(io, opts.image_paths[idx], allocator, .limited(std.math.maxInt(usize)));
        try loaded_images.append(allocator, image_bytes);
        try message_images.append(allocator, image_bytes);
        try content_parts.append(allocator, .{ .image = idx });
    }
    for (0..opts.audio_count) |idx| {
        const audio_bytes = try std.Io.Dir.cwd().readFileAlloc(io, opts.audio_paths[idx], allocator, .limited(std.math.maxInt(usize)));
        try loaded_audio.append(allocator, audio_bytes);
        try message_audio.append(allocator, audio_bytes);
        try content_parts.append(allocator, .{ .audio = idx });
    }
    if ((opts.image_count > 0 or opts.audio_count > 0) and opts.prompt.len > 0) {
        try content_parts.append(allocator, .{ .text = opts.prompt });
    }

    const message_image_slice: ?[]const []const u8 = if (message_images.items.len > 0)
        try allocator.dupe([]const u8, message_images.items)
    else
        null;
    defer if (message_image_slice) |slice| allocator.free(slice);
    const message_audio_slice: ?[]const []const u8 = if (message_audio.items.len > 0)
        try allocator.dupe([]const u8, message_audio.items)
    else
        null;
    defer if (message_audio_slice) |slice| allocator.free(slice);
    const content_part_slice: ?[]const generation.Message.ContentPart = if (content_parts.items.len > 0)
        try allocator.dupe(generation.Message.ContentPart, content_parts.items)
    else
        null;
    defer if (content_part_slice) |slice| allocator.free(slice);

    const messages = [_]generation.Message{
        .{
            .role = "user",
            .content = opts.prompt,
            .image_bytes = message_image_slice,
            .audio_bytes = message_audio_slice,
            .content_parts = content_part_slice,
        },
    };

    var config = generation.GenerationConfig{
        .max_tokens = opts.max_tokens,
        .temperature = opts.temperature,
        .top_p = opts.top_p,
        .top_k = opts.top_k,
        .repetition_penalty = opts.repetition_penalty,
        .prefill_chunk_size = opts.prefill_chunk_size,
        .draft_model = opts.draft_model,
        .speculative_k = opts.speculative_k,
        .cache_compaction_ratio = opts.cache_compaction_ratio,
    };

    const artifact_backend = switch (opts.backend) {
        .onnx => "onnx",
        .xla => "xla",
        else => null,
    };
    const resolved_artifact_dir = if (artifact_backend != null)
        if (opts.artifact_dir) |artifact_dir|
            try allocator.dupe(u8, artifact_dir)
        else
            try compiled_artifact.defaultArtifactDirForModel(allocator, opts.model_dir, artifact_backend.?)
    else
        null;
    defer if (resolved_artifact_dir) |path| allocator.free(path);
    const route_onnx_whole_model_graph = opts.backend == .onnx and opts.compiled_target == .whole_model;

    const allow_direct_onnx = opts.backend == .auto or opts.backend == .onnx;
    if (allow_direct_onnx and opts.draft_model == null and build_options.enable_onnx and
        !route_onnx_whole_model_graph and
        !c_file.fileExistsInDir(allocator, opts.model_dir, "genai_config.json") and
        onnx_decoder_only_vlm.isSupportedModelDir(allocator, opts.model_dir))
    {
        var pipeline = try onnx_decoder_only_vlm.Pipeline.load(allocator, opts.model_dir);
        defer pipeline.deinit();
        const loaded_model_at = std.Io.Timestamp.now(io, .awake);

        const apply_chat_template = !opts.raw_prompt and !opts.no_chat_template and pipeline.chat_tmpl != null;
        const rendered_prompt = if (opts.raw_prompt)
            try allocator.dupe(u8, opts.prompt)
        else if (apply_chat_template)
            try pipeline.chat_tmpl.?.apply(allocator, &messages, true)
        else
            try generation.formatMessages(allocator, &messages);
        defer allocator.free(rendered_prompt);
        pipeline.prompt_override = rendered_prompt;

        var prompt_encoded = try generation.encodePromptForGeneration(
            pipeline.hf_tok.tokenizer(),
            allocator,
            rendered_prompt,
            4096,
            !opts.no_bos and pipeline.manifest.add_bos_token,
            pipeline.manifest.bos_token,
        );
        defer prompt_encoded.deinit();
        const encoded_prompt_at = std.Io.Timestamp.now(io, .awake);

        if (opts.print_chat_template_status) {
            print("chat_template={}\n", .{apply_chat_template});
        }
        if (opts.print_prompt) {
            print("prompt:\n{s}\n", .{rendered_prompt});
        }
        if (opts.print_prompt_token_ids) {
            print("prompt_token_ids:", .{});
            for (prompt_encoded.ids[0..countPromptTokens(prompt_encoded.attention_mask)]) |id| {
                print(" {d}", .{id});
            }
            print("\n", .{});
        }

        if (artifact_backend != null and opts.draft_model == null and !route_onnx_whole_model_graph and opts.max_tokens == 1 and opts.image_count == 0 and opts.audio_count == 0) {
            if (try tryRunArtifactForPromptShape(
                allocator,
                io,
                &opts,
                resolved_artifact_dir.?,
                artifact_backend.?,
                countPromptTokens(prompt_encoded.attention_mask),
                countPromptTokens(prompt_encoded.attention_mask),
                "paged_prefill",
            )) return;
        }

        var result = try pipeline.generate(&messages, config);
        defer result.deinit();
        const finished_generate_at = std.Io.Timestamp.now(io, .awake);

        print("{s}\n", .{result.text});
        if (opts.print_token_ids) {
            if (result.token_ids) |ids| {
                print("token_ids:", .{});
                for (ids) |id| print(" {d}", .{id});
                print("\n", .{});
            } else {
                print("token_ids=unavailable\n", .{});
            }
        }
        if (opts.print_finish_reason or opts.print_token_count) {
            if (opts.print_finish_reason and opts.print_token_count) {
                print("finish_reason={s} tokens={d}\n", .{ result.finish_reason, result.tokens_used });
            } else if (opts.print_finish_reason) {
                print("finish_reason={s}\n", .{result.finish_reason});
            } else {
                print("tokens={d}\n", .{result.tokens_used});
            }
        }
        if (opts.print_timing) {
            print(
                "timing_ms: load_model={d} prompt_prep={d} scheduler=0 backend_setup=0 decode_setup=0 generate={d} total={d}\n",
                .{
                    durationMillis(started_at, loaded_model_at),
                    durationMillis(loaded_model_at, encoded_prompt_at),
                    durationMillis(encoded_prompt_at, finished_generate_at),
                    durationMillis(started_at, finished_generate_at),
                },
            );
        }
        return;
    }

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, if (route_onnx_whole_model_graph) .native else opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    if (artifact_backend != null and opts.draft_model == null and !route_onnx_whole_model_graph and opts.max_tokens == 1 and opts.image_count == 0 and opts.audio_count == 0) {
        var artifact_arena = std.heap.ArenaAllocator.init(allocator);
        defer artifact_arena.deinit();
        const artifact_allocator = artifact_arena.allocator();
        if (try native_run_artifact.tryRunMatchingArtifact(
            artifact_allocator,
            io,
            resolved_artifact_dir.?,
            artifact_backend.?,
            opts.model_dir,
            opts.prompt,
            opts.no_chat_template,
            opts.raw_prompt,
        )) |artifact_result_const| {
            var artifact_result = artifact_result_const;
            defer artifact_result.deinit(artifact_allocator);
            const finished_generate_at = std.Io.Timestamp.now(io, .awake);
            emitArtifactResultAndExit(&artifact_result, &opts, started_at, finished_generate_at);
        }
    }

    if (route_onnx_whole_model_graph) {
        try runOnnxWholeModelGraphGenerate(
            allocator,
            io,
            &opts,
            messages[0..],
            config,
            resolved_artifact_dir.?,
            started_at,
        );
        return;
    }

    const model = try model_manager.loadFromDir(opts.model_dir);
    const loaded_model_at = std.Io.Timestamp.now(io, .awake);
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const tokenizer = model.getTokenizer();
    if (opts.draft_model != null and (opts.image_count > 0 or opts.audio_count > 0)) {
        return error.MultimodalSpeculativeDecodingNotSupported;
    }
    const draft_model = if (opts.draft_model) |draft_model_dir| blk: {
        const loaded = try model_manager.loadFromDir(draft_model_dir);
        const draft_cfg = session_factory.getGptConfig(loaded.session) orelse return error.InvalidDraftModelForGeneration;
        try validateDraftTokenizerCompatibility(tokenizer, loaded.getTokenizer(), gpt_config, draft_cfg);
        break :blk loaded;
    } else null;
    const draft_gpt_config: ?@import("models/gpt.zig").Config = if (draft_model) |loaded|
        session_factory.getGptConfig(loaded.session).?
    else
        null;

    const apply_chat_template = !opts.raw_prompt and !opts.no_chat_template and model.chat_tmpl != null;
    const rendered_prompt = if (opts.raw_prompt)
        try allocator.dupe(u8, opts.prompt)
    else if (apply_chat_template)
        try model.chat_tmpl.?.apply(allocator, &messages, true)
    else
        try generation.formatMessages(allocator, &messages);
    defer allocator.free(rendered_prompt);
    var prompt_encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        rendered_prompt,
        2048,
        !opts.no_bos and model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    const encoded_prompt_at = std.Io.Timestamp.now(io, .awake);
    defer prompt_encoded.deinit();
    const prompt_tokens = countPromptTokens(prompt_encoded.attention_mask) +
        opts.image_count * (@as(usize, gpt_config.mm_tokens_per_image) + 1);

    if (artifact_backend != null and opts.draft_model == null and !route_onnx_whole_model_graph and opts.max_tokens == 1 and opts.image_count == 0 and opts.audio_count == 0) {
        if (try tryRunArtifactForPromptShape(
            allocator,
            io,
            &opts,
            resolved_artifact_dir.?,
            artifact_backend.?,
            prompt_tokens,
            prompt_tokens,
            "paged_prefill",
        )) return;
    }

    if (opts.print_chat_template_status) {
        print("chat_template={}\n", .{apply_chat_template});
    }
    if (opts.print_prompt) {
        print("prompt:\n{s}\n", .{rendered_prompt});
    }
    if (opts.print_prompt_token_ids) {
        print("prompt_token_ids:", .{});
        for (prompt_encoded.ids[0..countPromptTokens(prompt_encoded.attention_mask)]) |id| {
            print(" {d}", .{id});
        }
        print("\n", .{});
    }

    // Explicit compiled partition backends always use graph mode; otherwise
    // keep eager as the default and preserve TERMITE_GRAPH_MODE as the
    // compatibility opt-in when --mode is omitted.
    const compiled_mode_requested = if (opts.mode) |mode| mode == .compiled else false;
    const explicit_partition_backend = blk: {
        const requested = native_backend_choice.compiledPartitionBackendForMode(
            opts.backend,
            compiled_mode_requested,
        );
        if (requested) |backend| break :blk backend;
        if (compiled_mode_requested and opts.backend == .auto and build_options.enable_metal and model.session.backend() == .metal) {
            break :blk ops.BackendKind.metal;
        }
        break :blk @as(?ops.BackendKind, null);
    };
    const compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = opts.compiled_target orelse blk: {
        if (compiled_mode_requested and explicit_partition_backend == .metal) break :blk .whole_model;
        break :blk .partitioned;
    };
    const graph_mode = native_backend_choice.forcesGraphMode(opts.backend) or
        compiled_mode_requested or graphModeEnabled();
    if (graph_mode and generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config)) {
        return error.DeepSeekV4CompressedGraphModeNotSupported;
    }

    if (build_options.enable_metal and
        graph_mode and
        explicit_partition_backend == .metal and
        compiled_attachment_target == .whole_model and
        opts.draft_model == null and
        opts.image_count == 0 and
        opts.audio_count == 0 and
        graph_mod.metal_executor.supportsSession(model.session))
    {
        _ = graph_mod.metal_executor.prewarmSharedDecoderRuntime(allocator, model.session, gpt_config) catch |err| {
            std.log.warn("metal decoder-runtime prewarm failed for {s}: {s}", .{ opts.model_dir, @errorName(err) });
        };
    }

    if (try tryRunLiveWholeModelExecutorGenerate(
        allocator,
        io,
        &opts,
        model,
        gpt_config,
        tokenizer,
        config,
        prompt_encoded.ids[0..countPromptTokens(prompt_encoded.attention_mask)],
        prompt_tokens,
        started_at,
        loaded_model_at,
        encoded_prompt_at,
    )) return;

    if (!graph_mode) {
        graph_mod.executor_stats.printBypass("inference.generate", "native_generation_direct_decoder_runtime");
    }

    const decoder_runtime_scheduler_override = model.session.backend().usesGpuHostedSession() and enableMlxRawMetalWholeTokenDebug();
    var native_generate_lease: ?runtime.scheduler.native_generate.Lease = null;
    defer if (native_generate_lease) |lease| {
        if (model.native_generate_coordinator) |coordinator| coordinator.release(lease);
    };
    if (!decoder_runtime_scheduler_override) {
        if (model.native_generate_coordinator) |coordinator| {
            native_generate_lease = try coordinator.acquire(.{
                .requested_units = 1,
                .prompt_bytes = rendered_prompt.len,
                .max_tokens = opts.max_tokens,
            });
        }
    }
    const acquired_scheduler_at = std.Io.Timestamp.now(io, .awake);

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .mlx => .mlx,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = if (opts.cache_dtype) |name|
        runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDtype
    else
        session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
        .native => .cpu,
        .metal, .mlx, .cuda => .gpu,
    };
    var budget_limits = runtime.tier.memory.defaultLimitsForBackend(budget_backend_class);
    budget_limits = session_factory.widenBudgetLimitsForSession(model.session, budget_limits);
    budget_limits = applyBudgetOverrides(budget_limits, opts);
    var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
    print("budget: host={d}MB backend={d}MB combined={d}MB\n", .{
        budget_limits.host_limit_bytes / (1024 * 1024),
        budget_limits.backend_limit_bytes / (1024 * 1024),
        budget_limits.combined_limit_bytes / (1024 * 1024),
    });
    const admission_prefill_chunk = if (opts.prefill_chunk_size > 0) opts.prefill_chunk_size else 256;
    run_budget.reserveEstimate(runtime.tier.memory.estimateGptGeneration(
        backend_kind,
        kv_dtype,
        gpt_config,
        prompt_tokens,
        @intCast(@max(opts.max_tokens, 1)),
        admission_prefill_chunk,
    )) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    const draft_kv_dtype = if (draft_model) |loaded| blk: {
        break :blk if (opts.cache_dtype) |name|
            runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDtype
        else
            session_factory.recommendedKvDTypeForSession(loaded.session, backend_kind);
    } else null;
    if (draft_gpt_config) |draft_cfg| {
        run_budget.reserveEstimate(runtime.tier.memory.estimateGptGeneration(
            backend_kind,
            draft_kv_dtype.?,
            draft_cfg,
            prompt_tokens,
            @intCast(@max(opts.max_tokens, 1)),
            admission_prefill_chunk,
        )) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                printBudgetExceeded(draft_model.?.session, &run_budget);
            }
            return err;
        };
    }
    var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    const created_backend_at = std.Io.Timestamp.now(io, .awake);
    defer cb.deinit();
    var draft_cb: ?ops.ComputeBackend = if (draft_model) |loaded|
        session_factory.getComputeBackendWithBudget(loaded.session, allocator, &run_budget) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                printBudgetExceeded(loaded.session, &run_budget);
            }
            return err;
        }
    else
        null;
    defer if (draft_cb) |*backend| backend.deinit();

    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;

    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    const created_decode_state_at = std.Io.Timestamp.now(io, .awake);
    defer decode_state.deinit();
    var draft_kv_manager: ?runtime.kv.manager.KvManager = null;
    defer if (draft_kv_manager) |*manager| manager.deinit();
    var draft_decode_state: ?generation.NativeDecodeState = null;
    defer if (draft_decode_state) |*state| state.deinit();
    if (draft_gpt_config) |draft_cfg| {
        draft_kv_manager = runtime.kv.manager.KvManager.init(allocator);
        const draft_sliding_window_size: ?u32 = if (draft_cfg.position_encoding == .absolute)
            null
        else if (draft_cfg.sliding_window > 0)
            draft_cfg.sliding_window
        else if (draft_cfg.max_position_embeddings > 0)
            draft_cfg.max_position_embeddings
        else
            null;
        const draft_pool_id = try draft_kv_manager.?.addPool(.{
            .backend = backend_kind,
            .dtype = draft_kv_dtype.?,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(draft_cfg.num_hidden_layers),
            .num_kv_heads = draft_cfg.maxKvHeads(),
            .head_dim = draft_cfg.maxHeadDim(),
            .sliding_window_size = draft_sliding_window_size,
        });
        draft_decode_state = generation.NativeDecodeState.initPaged(allocator, &draft_kv_manager.?, draft_pool_id, null);
    }
    if (native_generate_lease) |lease| {
        if (config.prefill_chunk_size == 0) {
            config.prefill_chunk_size = lease.prefill_chunk_size;
        }
    }

    const use_scheduler = !generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config) and !graph_mode and !decoder_runtime_scheduler_override and nativeGenerateSchedulerEnabled();
    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer graph_cache.deinit();
    var pjrt_client: ?pjrt_lib.pjrt.Client = null;
    defer if (pjrt_client) |*client| client.deinit();
    var pjrt_plugin_path: ?[:0]u8 = null;
    defer if (pjrt_plugin_path) |path| allocator.free(path);
    if (explicit_partition_backend == .pjrt) {
        pjrt_plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator);
        const plugin_path = pjrt_plugin_path orelse return error.MissingPjrtPluginPath;
        pjrt_client = try pjrt_lib.pjrt.Client.init(plugin_path);
    }

    var pipeline = generation.NativeGenerationPipeline{
        .allocator = allocator,
        .io = io,
        .cb = cb,
        .session = model.session,
        .gpt_config = gpt_config,
        .kv_dtype = kv_dtype,
        .shared_moe_cache = model.shared_moe_cache,
        .tokenizer = tokenizer,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .chat_template = if (opts.no_chat_template) null else model.chat_tmpl,
        .prompt_override = if (opts.raw_prompt) rendered_prompt else null,
        .print_timing = opts.print_timing,
        .model_dir = opts.model_dir,
        .artifact_dir = resolved_artifact_dir,
        .gguf_projector_path = model.manifest.gguf_projector_path,
        .decode_state = &decode_state,
        .scheduler = if (use_scheduler) model.native_generate_coordinator else null,
        .scheduler_lease = if (use_scheduler) if (native_generate_lease) |*lease| lease else null else null,
        .draft_cb = if (draft_cb) |draft_backend| draft_backend else null,
        .draft_gpt_config = draft_gpt_config,
        .draft_decode_state = if (draft_decode_state) |*state| state else null,
        .graph_cache = if (graph_mode) &graph_cache else null,
        .compiled_partition_backend = explicit_partition_backend,
        .compiled_attachment_target = compiled_attachment_target,
        .pjrt_client = if (pjrt_client) |*client| client else null,
    };

    if ((build_options.enable_mlx or build_options.enable_metal) and opts.print_timing and model.session.backend().usesGpuHostedSession()) {
        debug_timing.resetLiveMlxTimingStats(&cb);
        generation.resetDecoderRuntimeDebugStats();
    }
    gpt_arch.resetDebugTimingStats();

    var result = pipeline.generate(&messages, config) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    const finished_generate_at = std.Io.Timestamp.now(io, .awake);
    defer result.deinit();

    print("{s}\n", .{result.text});
    if (opts.print_token_ids) {
        if (result.token_ids) |ids| {
            print("token_ids:", .{});
            for (ids) |id| print(" {d}", .{id});
            print("\n", .{});
        } else {
            print("token_ids=unavailable\n", .{});
        }
    }
    if (opts.print_finish_reason or opts.print_token_count) {
        if (opts.print_finish_reason and opts.print_token_count) {
            print("finish_reason={s} tokens={d}\n", .{ result.finish_reason, result.tokens_used });
        } else if (opts.print_finish_reason) {
            print("finish_reason={s}\n", .{result.finish_reason});
        } else {
            print("tokens={d}\n", .{result.tokens_used});
        }
    }
    if (opts.print_timing) printSpeculativeStats(&result);
    if (opts.print_timing) {
        if (build_options.enable_metal and explicit_partition_backend == .metal and compiled_attachment_target == .whole_model) {
            printLiveWholeModelExecutorDetails(graph_cache.getSessionCompiledModelRuntime(.metal, .whole_model));
        }
        print(
            "timing_ms: load_model={d} prompt_prep={d} scheduler={d} backend_setup={d} decode_setup={d} generate={d} total={d}\n",
            .{
                durationMillis(started_at, loaded_model_at),
                durationMillis(loaded_model_at, encoded_prompt_at),
                durationMillis(encoded_prompt_at, acquired_scheduler_at),
                durationMillis(acquired_scheduler_at, created_backend_at),
                durationMillis(created_backend_at, created_decode_state_at),
                durationMillis(created_decode_state_at, finished_generate_at),
                durationMillis(started_at, finished_generate_at),
            },
        );
        if ((build_options.enable_mlx or build_options.enable_metal) and model.session.backend().usesGpuHostedSession() and detailedGpuTimingEnabled()) {
            printGpuHostedTimingDetails(&cb);
            const quant_stats = cb.debugTimingSnapshot().quant;
            const decoder_runtime_stats = generation.getDecoderRuntimeDebugStats();
            if (build_options.enable_mlx) {
                print(
                    "mlx_decoder_runtime: forward_attempts={d} flag_disabled={d} backend_not_mlx={d} scheduler_blocked={d} graph_blocked={d} first_token_blocked={d} kv_missing={d} non_greedy={d} grammar_blocked={d} prepare_attempts={d} prepare_calls={d} prepare_flag_disabled={d} prepare_backend_not_mlx={d} prepare_kv_missing={d} prepare_scheduler_blocked={d} prepare_graph_blocked={d} prepare_arch_blocked={d} prepare_model_blocked={d} input_attempts={d} input_successes={d} input_flag_disabled={d} input_backend_not_mlx={d} input_kv_missing={d} input_arch_blocked={d} input_model_blocked={d} input_seq_empty={d}\n",
                    .{
                        decoder_runtime_stats.forward_attempts,
                        decoder_runtime_stats.flag_disabled,
                        decoder_runtime_stats.backend_not_mlx,
                        decoder_runtime_stats.scheduler_blocked,
                        decoder_runtime_stats.graph_blocked,
                        decoder_runtime_stats.first_token_blocked,
                        decoder_runtime_stats.kv_missing,
                        decoder_runtime_stats.non_greedy,
                        decoder_runtime_stats.grammar_blocked,
                        decoder_runtime_stats.prepare_attempts,
                        decoder_runtime_stats.prepare_calls,
                        decoder_runtime_stats.prepare_flag_disabled,
                        decoder_runtime_stats.prepare_backend_not_mlx,
                        decoder_runtime_stats.prepare_kv_missing,
                        decoder_runtime_stats.prepare_scheduler_blocked,
                        decoder_runtime_stats.prepare_graph_blocked,
                        decoder_runtime_stats.prepare_arch_blocked,
                        decoder_runtime_stats.prepare_model_blocked,
                        decoder_runtime_stats.input_attempts,
                        decoder_runtime_stats.input_successes,
                        decoder_runtime_stats.input_flag_disabled,
                        decoder_runtime_stats.input_backend_not_mlx,
                        decoder_runtime_stats.input_kv_missing,
                        decoder_runtime_stats.input_arch_blocked,
                        decoder_runtime_stats.input_model_blocked,
                        decoder_runtime_stats.input_seq_empty,
                    },
                );
                print(
                    "mlx_moe_grouped_failures: recovered_packed_metadata={d} not_device_native={d} missing_quant_storage={d} not_packed={d} provider_null={d}\n",
                    .{
                        quant_stats.moe_grouped_recovered_packed_metadata,
                        quant_stats.moe_grouped_fail_not_device_native,
                        quant_stats.moe_grouped_fail_missing_quant_storage,
                        quant_stats.moe_grouped_fail_not_packed,
                        quant_stats.moe_grouped_fail_provider_null,
                    },
                );
                print(
                    "mlx_moe_grouped_staging: calls={d} bytes={d} experts={d} ms={d}\n",
                    .{
                        quant_stats.moe_grouped_stage_calls,
                        quant_stats.moe_grouped_stage_bytes,
                        quant_stats.moe_grouped_stage_experts,
                        @divTrunc(quant_stats.moe_grouped_stage_nanos, std.time.ns_per_ms),
                    },
                );
            }
        }
        if (build_options.enable_metal and cb.kind() == .metal) {
            const metal_snapshot = cb.debugTimingSnapshot();
            print(
                "metal_direct_paths: gated_direct_ok={d} gated_direct_fail={d} gated_direct_fail_replace={d} gated_direct_fail_attn={d} gated_direct_fail_prefix={d} gated_direct_fail_ffn={d} dense_fast_attempts={d} gated_fast_attempts={d}\n",
                .{
                    metal_snapshot.provider.compressed_block_gated_direct_successes,
                    metal_snapshot.provider.compressed_block_gated_direct_runtime_failures,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_replace_span,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_attention_span,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_attention_prefix,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_gated_ffn,
                    metal_snapshot.quant.dense_block_fast_attempts,
                    metal_snapshot.quant.gated_block_fast_attempts,
                },
            );
            print(
                "metal_gated_quantized_block: calls={d} quantized_branch={d} attn_calls={d} attn_nulls={d} attn_prefill_nulls={d} attn_decode_nulls={d} norm_nulls={d} f32_kv_calls={d} f32_kv_ok={d} f32_kv_nulls={d} f32_quant_direct_ok={d} f32_quant_direct_fail={d} compressed_f32_reroutes={d} active_bootstrap_misses={d}\n",
                .{
                    metal_snapshot.provider.compressed_block_gated_calls,
                    metal_snapshot.provider.compressed_block_gated_quantized_branch_calls,
                    metal_snapshot.provider.compressed_block_quantized_attention_calls,
                    metal_snapshot.provider.compressed_block_gated_quantized_attention_nulls,
                    metal_snapshot.provider.compressed_block_gated_quantized_attention_prefill_nulls,
                    metal_snapshot.provider.compressed_block_gated_quantized_attention_decode_nulls,
                    metal_snapshot.provider.compressed_block_gated_quantized_norm_nulls,
                    metal_snapshot.provider.f32_kv_gated_block_calls,
                    metal_snapshot.provider.f32_kv_gated_block_successes,
                    metal_snapshot.provider.f32_kv_gated_block_nulls,
                    metal_snapshot.provider.f32_kv_quant_direct_block_successes,
                    metal_snapshot.provider.f32_kv_quant_direct_block_failures,
                    metal_snapshot.provider.compressed_block_active_frame_f32_reroutes,
                    metal_snapshot.provider.compressed_block_active_frame_bootstrap_misses,
                },
            );
            print(
                "metal_decoder_frame: begins={d} submits={d} wait_ms={d} gpu_ms={d} last_compute_encoders={d} last_blit_encoders={d} total_compute_encoders={d} total_blit_encoders={d}\n",
                .{
                    metal_snapshot.provider.decoder_runtime_frame_begins,
                    metal_snapshot.provider.decoder_runtime_frame_submits,
                    @divTrunc(metal_snapshot.provider.decoder_runtime_frame_wait_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.decoder_runtime_frame_gpu_nanos, std.time.ns_per_ms),
                    metal_snapshot.provider.metal_runtime_last_frame_compute_encoder_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_encoder_count,
                    metal_snapshot.provider.metal_runtime_compute_encoder_count,
                    metal_snapshot.provider.metal_runtime_blit_encoder_count,
                },
            );
            print(
                "metal_decoder_frame_blits: upload={d} copy={d} slice={d} attention_span={d} ffn_copy={d} embedding={d} other={d}\n",
                .{
                    metal_snapshot.provider.metal_runtime_last_frame_blit_buffer_upload_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_buffer_copy_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_buffer_slice_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_attention_span_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_ffn_copy_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_embedding_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_other_count,
                },
            );
            print(
                "metal_decoder_frame_compute_sources: quant_linear={d} quant_qkv={d} quant_pair_act={d} attention={d} rms_norm={d} head_rope={d} ffn={d} ple={d} tail={d} embedding={d} dense_linear={d} layer={d} other={d}\n",
                .{
                    metal_snapshot.provider.metal_runtime_last_frame_compute_quant_linear_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_quant_qkv_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_quant_pair_act_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_attention_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_rms_norm_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_head_rope_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_ffn_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_ple_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_tail_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_embedding_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_dense_linear_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_layer_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_other_count,
                },
            );
            print(
                "metal_decoder_frame_compute_regions: attention={d} attention_project={d} ffn_norm={d} ffn={d} ple={d} tail={d} embedding={d} layer={d} other={d}\n",
                .{
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_attention_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_attention_project_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_ffn_norm_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_ffn_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_ple_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_tail_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_embedding_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_layer_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_other_count,
                },
            );
            print(
                "metal_active_decode_kernels: attention_f32={d} q8_0_linear={d} q8_0_attn_linear={d} q8_0_ffn_down={d} q8_0_ple={d} q8_0_pair_activation={d} rms_norm={d} rms_norm_add={d} layer_norm={d} add={d} head_norm_rope_fused={d} blit={d}\n",
                .{
                    metal_snapshot.provider.active_decode_attention_f32_kernels,
                    metal_snapshot.provider.active_decode_q8_0_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_attention_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_ffn_down_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_ple_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_pair_activation_kernels,
                    metal_snapshot.provider.active_decode_rms_norm_kernels,
                    metal_snapshot.provider.active_decode_rms_norm_add_kernels,
                    metal_snapshot.provider.active_decode_layer_norm_kernels,
                    metal_snapshot.provider.active_decode_add_kernels,
                    metal_snapshot.provider.active_decode_head_norm_rope_fused_kernels,
                    metal_snapshot.provider.active_decode_blit_copies,
                },
            );
            print(
                "metal_active_decode_ops: layers={d} layer_input_direct={d}/{d} attn_norm={d} q_linear={d} qkv={d} head_norm={d} rope={d} head_norm_rope_fused={d} ple={d} final_fused_argmax={d} final_split_argmax={d}\n",
                .{
                    metal_snapshot.provider.active_decode_layers,
                    metal_snapshot.provider.active_decode_layer_input_direct_hits,
                    metal_snapshot.provider.active_decode_layer_input_direct_attempts,
                    metal_snapshot.provider.active_decode_attn_norm_ops,
                    metal_snapshot.provider.active_decode_q_linear_ops,
                    metal_snapshot.provider.active_decode_qkv_ops,
                    metal_snapshot.provider.active_decode_head_norm_ops,
                    metal_snapshot.provider.active_decode_rope_ops,
                    metal_snapshot.provider.active_decode_head_norm_rope_fused_ops,
                    metal_snapshot.provider.active_decode_ple_ops,
                    metal_snapshot.provider.active_decode_final_fused_argmax_ops,
                    metal_snapshot.provider.active_decode_final_split_argmax_ops,
                },
            );
            print(
                "metal_frame_fallbacks: decode_attempts={d} decode_success={d} decode_disabled={d} decode_scratch_fail={d} decode_fallback={d} decode_batch={d} decode_initial={d} decode_layer={d} decode_tail={d} prefill_plan={d}/{d} prefill_plan_fail={d} prefill_execute={d}/{d} prefill_execute_fail={d} prefill_missing_ple={d}\n",
                .{
                    metal_snapshot.provider.active_decode_frame_attempts,
                    metal_snapshot.provider.active_decode_frame_successes,
                    metal_snapshot.provider.active_decode_frame_disabled,
                    metal_snapshot.provider.active_decode_frame_scratch_failures,
                    metal_snapshot.provider.active_decode_frame_fallbacks,
                    metal_snapshot.provider.active_decode_frame_batch_fallbacks,
                    metal_snapshot.provider.active_decode_frame_initial_tensor_fallbacks,
                    metal_snapshot.provider.active_decode_frame_layer_fallbacks,
                    metal_snapshot.provider.active_decode_frame_tail_fallbacks,
                    metal_snapshot.provider.prefill_frame_plan_successes,
                    metal_snapshot.provider.prefill_frame_plan_attempts,
                    metal_snapshot.provider.prefill_frame_plan_failures,
                    metal_snapshot.provider.prefill_frame_execute_successes,
                    metal_snapshot.provider.prefill_frame_execute_attempts,
                    metal_snapshot.provider.prefill_frame_execute_failures,
                    metal_snapshot.provider.prefill_frame_execute_missing_ple,
                },
            );
            print(
                "metal_frame_contract: ops={d} scopes={d} barriers={d} windows={d} full_frames={d} layer_contracts={d} tail_contracts={d} local_plan_bypass={d} scope_links={d} layer_runtime={d}/{d} layer_runtime_fail={d} layer_staged_path={d} tail_hits={d} tail_misses={d} no_runtime={d} no_active={d} invalid_contract={d} invalid_shape={d} missing_plan={d} plan_mismatch={d} output_hidden_set={d}\n",
                .{
                    metal_snapshot.provider.prefill_frame_contract_ops,
                    metal_snapshot.provider.prefill_frame_contract_scopes,
                    metal_snapshot.provider.prefill_frame_contract_barriers,
                    metal_snapshot.provider.prefill_frame_contract_windows,
                    metal_snapshot.provider.prefill_frame_contract_full_frames,
                    metal_snapshot.provider.prefill_frame_executor_layer_contracts,
                    metal_snapshot.provider.prefill_frame_executor_tail_contracts,
                    metal_snapshot.provider.prefill_frame_executor_local_plan_bypasses,
                    metal_snapshot.provider.prefill_frame_executor_scope_links,
                    metal_snapshot.provider.prefill_frame_executor_layer_runtime_successes,
                    metal_snapshot.provider.prefill_frame_executor_layer_runtime_calls,
                    metal_snapshot.provider.prefill_frame_executor_layer_runtime_failures,
                    metal_snapshot.provider.prefill_frame_executor_layer_staged_paths,
                    metal_snapshot.provider.prefill_frame_tail_contract_hits,
                    metal_snapshot.provider.prefill_frame_tail_contract_misses,
                    metal_snapshot.provider.prefill_frame_execute_no_runtime,
                    metal_snapshot.provider.prefill_frame_execute_no_active_frame,
                    metal_snapshot.provider.prefill_frame_execute_invalid_contract,
                    metal_snapshot.provider.prefill_frame_execute_invalid_shape,
                    metal_snapshot.provider.prefill_frame_execute_missing_plan,
                    metal_snapshot.provider.prefill_frame_execute_plan_mismatch,
                    metal_snapshot.provider.prefill_frame_execute_output_hidden_set,
                },
            );
            print(
                "metal_quant_block_apply_ms: total={d} replace_span={d} attention_span={d} attention_prefix={d} gated_ffn={d} command_wait={d} gpu={d}\n",
                .{
                    @divTrunc(metal_snapshot.provider.compressed_block_apply_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_replace_span_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_attention_span_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_attention_prefix_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_gated_ffn_residual_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_command_wait_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_gpu_nanos, std.time.ns_per_ms),
                },
            );
            print(
                "metal_gated_quantized_failures: span_update={d} span_attn={d} post_linear_fail={d} ffn_direct_ok={d} ffn_direct_fallback={d} ffn_backend_fallback={d} ffn_runtime_fail={d}\n",
                .{
                    metal_snapshot.provider.compressed_attention_residual_update_span_failures,
                    metal_snapshot.provider.compressed_attention_residual_attention_span_failures,
                    metal_snapshot.provider.compressed_attention_residual_post_linear_failures,
                    metal_snapshot.provider.quantized_gated_ffn_direct_successes,
                    metal_snapshot.provider.quantized_gated_ffn_direct_fallbacks,
                    metal_snapshot.provider.quantized_gated_ffn_backend_fallbacks,
                    metal_snapshot.provider.quantized_gated_ffn_runtime_failures,
                },
            );
        }
        const gpt_stats = gpt_arch.getDebugTimingStats();
        print(
            "gpt_timing_ms: attention={d} attn_norm={d} attn_qkv={d} attn_core={d} attn_rope={d} attn_gqa={d} attn_out_proj={d} ffn={d} moe_router_weight_fetch={d} moe_router_proj={d} moe_route_select={d} moe_router_download={d} moe_expert_scale_download={d} moe_expert_weight_fetch={d} moe_input_download={d} moe_prepare_layer={d} moe_append_route={d} moe_finalize_layer={d} moe_prefetch_hint={d}\n",
            .{
                @divTrunc(gpt_stats.attention_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.attention_norm_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.attention_qkv_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.attention_core_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.attention_rope_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.attention_gqa_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.attention_out_proj_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.ffn_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_router_weight_fetch_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_router_proj_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_route_select_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_router_download_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_expert_scale_download_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_expert_weight_fetch_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_input_download_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_prepare_layer_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_append_route_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_finalize_layer_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_prefetch_hint_nanos, std.time.ns_per_ms),
            },
        );
        print(
            "gpt_moe_timing_ms: grouped_attempts={d} grouped_successes={d} moe_grouped={d} moe_fallback={d} moe_grouped_input_copy={d} moe_grouped_input_upload={d} moe_grouped_ops={d} moe_grouped_sync_w1={d} moe_grouped_sync_w3={d} moe_grouped_sync_gate={d} moe_grouped_sync_w2={d} moe_grouped_sync_ops={d} moe_grouped_output_download={d} moe_grouped_scatter={d} moe_grouped_sync_scatter={d} moe_grouped_cleanup={d}\n",
            .{
                gpt_stats.moe_grouped_attempts,
                gpt_stats.moe_grouped_successes,
                @divTrunc(gpt_stats.moe_grouped_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_fallback_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_input_copy_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_input_upload_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_ops_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_sync_w1_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_sync_w3_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_sync_gate_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_sync_w2_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_sync_ops_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_output_download_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_scatter_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_sync_scatter_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.moe_grouped_cleanup_nanos, std.time.ns_per_ms),
            },
        );
        print(
            "gpt_overhead_ms: eval={d} eval_count={d} shared_expert_ffn={d} norm={d}\n",
            .{
                @divTrunc(gpt_stats.eval_nanos, std.time.ns_per_ms),
                gpt_stats.eval_count,
                @divTrunc(gpt_stats.shared_expert_ffn_nanos, std.time.ns_per_ms),
                @divTrunc(gpt_stats.norm_nanos, std.time.ns_per_ms),
            },
        );
        print(
            "gpt_block_counts: dense_attempts={d} dense_successes={d} gated_attempts={d} gated_successes={d} gated_input_attempts={d} gated_input_successes={d} gated_qkv_attempts={d} gated_qkv_successes={d}\n",
            .{
                gpt_stats.dense_block_attempts,
                gpt_stats.dense_block_successes,
                gpt_stats.gated_block_attempts,
                gpt_stats.gated_block_successes,
                gpt_stats.gated_block_input_attempts,
                gpt_stats.gated_block_input_successes,
                gpt_stats.gated_block_qkv_attempts,
                gpt_stats.gated_block_qkv_successes,
            },
        );
    }
}

fn nanosToMillis(nanos: i128) u64 {
    return @intCast(@divTrunc(nanos, std.time.ns_per_ms));
}

fn durationMillis(from: std.Io.Timestamp, to: std.Io.Timestamp) u64 {
    return nanosToMillis(std.Io.Timestamp.durationTo(from, to).nanoseconds);
}

fn printGpuHostedTimingDetails(cb_opt: ?*const ops.ComputeBackend) void {
    if (!build_options.enable_mlx and !build_options.enable_metal) {
        return;
    }
    const backend_stats = if (cb_opt) |cb| cb.debugTimingSnapshot() else debug_timing.fallbackMlxTimingSnapshot();
    const backend_kind: ops.BackendKind = if (cb_opt) |cb| cb.kind() else .mlx;
    const decoder_runtime_runtime_ready = if (cb_opt) |cb| cb.decoderRuntimeReady() else false;
    const decoder_runtime_embeddings_prepared = if (cb_opt) |cb| cb.decoderRuntimeAbsoluteEmbeddingsPrepared() else false;
    debug_timing.printBackendTimingDetails(
        backend_kind,
        backend_stats,
        decoder_runtime_runtime_ready,
        decoder_runtime_embeddings_prepared,
    );
}

fn envFlagEnabled(name: [:0]const u8) bool {
    return platform.env.getenvBool(name.ptr);
}

fn detailedGpuTimingEnabled() bool {
    return envFlagEnabled("TERMITE_DEBUG_METAL_TIMING") or envFlagEnabled("TERMITE_DEBUG_GPT_STATS");
}

fn metalExecutorReuseProbeEnabled() bool {
    return envFlagEnabled("TERMITE_METAL_EXECUTOR_REUSE_PROBE");
}

fn printLiveWholeModelExecutorDetails(runtime_opt: ?*const graph_mod.model_runtime.ModelRuntime) void {
    if (runtime_opt) |runtime_model| {
        runtime_model.printDebugTiming();
    }
}

fn isPureGreedyConfig(config: generation.GenerationConfig) bool {
    return config.temperature <= 0 and
        config.repetition_penalty == 1.0 and
        config.frequency_penalty == 0 and
        config.presence_penalty == 0;
}

fn liveWholeModelExecutorRequested(opts: *const Options) bool {
    const explicit_whole_model = opts.mode != null and opts.mode.? == .compiled and
        opts.compiled_target != null and opts.compiled_target.? == .whole_model;
    if (explicit_whole_model) return false;
    if (!explicit_whole_model and opts.backend != .metal and !enableMlxRawMetalWholeTokenDebug()) return false;
    return switch (opts.backend) {
        .auto, .native, .metal, .mlx => true,
        else => false,
    };
}

fn runLiveWholeModelExecutorReuseProbe(
    allocator: std.mem.Allocator,
    io: std.Io,
    model: *model_manager_mod.LoadedModel,
    prompt_ids: []const i64,
    prefill_chunk_size: usize,
    kv_dtype: runtime.kv.pool.KvDType,
) !void {
    if (!model.session.backend().usesGpuHostedSession()) return;

    gpt_arch.resetDebugTimingStats();

    const started_at = std.Io.Timestamp.now(io, .awake);
    var executor = (try model.wholeModelExecutor(allocator, kv_dtype)) orelse return;
    defer executor.deinit();
    var runtime_model = try executor.createRuntime(allocator);
    defer runtime_model.deinit();
    runtime_model.resetDebugTimingStats();
    const created_runtime_at = std.Io.Timestamp.now(io, .awake);

    var processed: usize = 0;
    var output_accum: ?graph_mod.model_runtime.ModelOutput = null;
    errdefer if (output_accum) |*owned| owned.deinit(allocator);
    while (processed < prompt_ids.len) {
        const chunk_end = @min(prompt_ids.len, processed + @max(prefill_chunk_size, 1));
        if (output_accum) |*owned| owned.deinit(allocator);
        output_accum = try runtime_model.prefill(allocator, .{
            .input_ids = prompt_ids[processed..chunk_end],
            .seq_len = chunk_end,
            .query_seq_len = chunk_end - processed,
            .attention_mode = .paged_prefill,
        });
        processed = chunk_end;
    }
    if (output_accum) |*owned| owned.deinit(allocator);
    const finished_at = std.Io.Timestamp.now(io, .awake);

    print(
        "metal_executor_reuse_ms: backend_setup={d} prefill={d} total={d}\n",
        .{
            durationMillis(started_at, created_runtime_at),
            durationMillis(created_runtime_at, finished_at),
            durationMillis(started_at, finished_at),
        },
    );
    printLiveWholeModelExecutorDetails(&runtime_model);
}

fn tryRunLiveWholeModelExecutorGenerate(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: *const Options,
    model: *model_manager_mod.LoadedModel,
    gpt_config: @import("models/gpt.zig").Config,
    tokenizer: tokenizer_mod.Tokenizer,
    config: generation.GenerationConfig,
    prompt_token_ids: []const i32,
    prompt_tokens: usize,
    started_at: std.Io.Timestamp,
    loaded_model_at: std.Io.Timestamp,
    encoded_prompt_at: std.Io.Timestamp,
) !bool {
    if (!liveWholeModelExecutorRequested(opts)) return false;
    if (generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config)) return false;
    if (opts.draft_model != null) return false;
    if (opts.image_count > 0 or opts.audio_count > 0) return false;

    gpt_arch.resetDebugTimingStats();

    const kv_backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .metal => .metal,
        .mlx => .mlx,
        .native => .native,
        else => .native,
    };
    const kv_dtype = if (opts.cache_dtype) |name|
        runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDType
    else
        session_factory.recommendedKvDTypeForSession(model.session, kv_backend_kind);
    var executor = (try model.wholeModelExecutor(allocator, kv_dtype)) orelse return false;
    defer executor.deinit();

    var runtime_model = try executor.createRuntime(allocator);
    defer runtime_model.deinit();
    if (opts.print_timing and model.session.backend().usesGpuHostedSession()) {
        runtime_model.resetDebugTimingStats();
    }
    const runtime_caps = runtime_model.capabilities();
    const created_runtime_at = std.Io.Timestamp.now(io, .awake);

    const prompt_ids = try allocator.alloc(i64, prompt_tokens);
    defer allocator.free(prompt_ids);
    for (prompt_token_ids, 0..) |token_id, idx| prompt_ids[idx] = token_id;

    var all_token_ids = std.ArrayListUnmanaged(i64).empty;
    defer all_token_ids.deinit(allocator);
    try all_token_ids.appendSlice(allocator, prompt_ids);

    var generated_token_ids = std.ArrayListUnmanaged(i32).empty;
    defer generated_token_ids.deinit(allocator);

    var finish_reason: []const u8 = "length";
    const max_tokens: usize = if (opts.max_tokens > 0) @intCast(opts.max_tokens) else 0;
    const sampling_config: graph_mod.model_runtime.SamplingConfig = .{
        .temperature = config.temperature,
        .top_p = config.top_p,
        .top_k = config.top_k,
        .min_p = config.min_p,
        .repetition_penalty = config.repetition_penalty,
        .frequency_penalty = config.frequency_penalty,
        .presence_penalty = config.presence_penalty,
    };
    const use_greedy_decode = runtime_caps.supports_greedy_decode and isPureGreedyConfig(config);
    const use_sample_decode = runtime_caps.supports_sample_decode and !use_greedy_decode;
    var prefill_chunk_size = if (config.prefill_chunk_size > 0) config.prefill_chunk_size else prompt_ids.len;
    prefill_chunk_size = @max(@min(prefill_chunk_size, prompt_ids.len), 1);
    var output = blk: {
        if (prompt_ids.len == 0) return error.EmptyPrompt;

        var processed: usize = 0;
        var output_accum: ?graph_mod.model_runtime.ModelOutput = null;
        errdefer if (output_accum) |*owned| owned.deinit(allocator);

        while (processed < prompt_ids.len) {
            const chunk_end = @min(prompt_ids.len, processed + prefill_chunk_size);
            if (output_accum) |*owned| owned.deinit(allocator);
            output_accum = try runtime_model.prefill(allocator, .{
                .input_ids = prompt_ids[processed..chunk_end],
                .seq_len = chunk_end,
                .query_seq_len = chunk_end - processed,
                .attention_mode = .paged_prefill,
            });
            processed = chunk_end;
        }
        break :blk output_accum.?;
    };
    defer output.deinit(allocator);

    var generated: usize = 0;
    while (generated < max_tokens) {
        const next_token_i32: i32 = if (generated == 0) blk: {
            if (use_greedy_decode) {
                break :blk @intCast(try output.greedyToken(allocator, gpt_config.vocab_size));
            }
            const output_logits = try output.hostLogits(allocator);
            break :blk @intCast(generation.sampleTokenFromLogits(
                allocator,
                output_logits,
                config,
                all_token_ids.items,
            ));
        } else if (use_greedy_decode) blk: {
            const greedy = try runtime_model.decodeGreedy(allocator, .{
                .token_id = all_token_ids.items[all_token_ids.items.len - 1],
                .position = all_token_ids.items.len - 1,
                .attention_mode = .paged_decode,
            });
            break :blk @intCast(greedy.token_id);
        } else if (use_sample_decode) blk: {
            const sampled = try runtime_model.decodeSample(allocator, .{
                .decode = .{
                    .token_id = all_token_ids.items[all_token_ids.items.len - 1],
                    .position = all_token_ids.items.len - 1,
                    .attention_mode = .paged_decode,
                },
                .sampling = sampling_config,
                .token_history = all_token_ids.items,
            });
            break :blk @intCast(sampled.token_id);
        } else blk: {
            const output_logits = try output.hostLogits(allocator);
            break :blk @intCast(generation.sampleTokenFromLogits(
                allocator,
                output_logits,
                config,
                all_token_ids.items,
            ));
        };
        const next_token_i64: i64 = next_token_i32;
        try generated_token_ids.append(allocator, next_token_i32);
        try all_token_ids.append(allocator, next_token_i64);
        generated += 1;

        if (gpt_config.eos_token_id >= 0 and next_token_i32 == gpt_config.eos_token_id) {
            finish_reason = "stop";
            break;
        }
        if (generated >= max_tokens) break;
        if (use_greedy_decode or use_sample_decode) continue;

        const next_output = try runtime_model.decode(allocator, .{
            .token_id = next_token_i64,
            .position = all_token_ids.items.len - 1,
            .attention_mode = .paged_decode,
        });
        output.deinit(allocator);
        output = next_output;
    }

    const result_token_ids = try allocator.dupe(i32, generated_token_ids.items);
    defer allocator.free(result_token_ids);
    const result_text = if (result_token_ids.len > 0)
        try tokenizer.decode(allocator, result_token_ids)
    else
        try allocator.dupe(u8, "");
    defer allocator.free(result_text);

    const finished_generate_at = std.Io.Timestamp.now(io, .awake);
    print("{s}\n", .{result_text});
    if (opts.print_token_ids) {
        print("token_ids:", .{});
        for (result_token_ids) |id| print(" {d}", .{id});
        print("\n", .{});
    }
    if (opts.print_finish_reason or opts.print_token_count) {
        if (opts.print_finish_reason and opts.print_token_count) {
            print("finish_reason={s} tokens={d}\n", .{ finish_reason, result_token_ids.len });
        } else if (opts.print_finish_reason) {
            print("finish_reason={s}\n", .{finish_reason});
        } else {
            print("tokens={d}\n", .{result_token_ids.len});
        }
    }
    if (opts.print_timing) {
        print(
            "timing_ms: load_model={d} prompt_prep={d} scheduler=0 backend_setup={d} decode_setup=0 generate={d} total={d}\n",
            .{
                durationMillis(started_at, loaded_model_at),
                durationMillis(loaded_model_at, encoded_prompt_at),
                durationMillis(encoded_prompt_at, created_runtime_at),
                durationMillis(created_runtime_at, finished_generate_at),
                durationMillis(started_at, finished_generate_at),
            },
        );
        if (model.session.backend().usesGpuHostedSession()) {
            printLiveWholeModelExecutorDetails(&runtime_model);
            if (metalExecutorReuseProbeEnabled()) {
                try runLiveWholeModelExecutorReuseProbe(
                    allocator,
                    io,
                    model,
                    prompt_ids,
                    prefill_chunk_size,
                    kv_dtype,
                );
            }
        }
        if (build_options.enable_metal and opts.backend == .metal) {
            const metal_snapshot = runtime_model.debugTimingStats().backend;
            const gated_stats = decoder_gated_runtime.getTimingStats();
            print(
                "metal_direct_paths: gated_direct_ok={d} gated_direct_fail={d} gated_direct_fail_replace={d} gated_direct_fail_attn={d} gated_direct_fail_prefix={d} gated_direct_fail_ffn={d} dense_fast_attempts={d} gated_fast_attempts={d}\n",
                .{
                    metal_snapshot.provider.compressed_block_gated_direct_successes,
                    metal_snapshot.provider.compressed_block_gated_direct_runtime_failures,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_replace_span,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_attention_span,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_attention_prefix,
                    metal_snapshot.provider.compressed_block_gated_direct_fail_gated_ffn,
                    metal_snapshot.quant.dense_block_fast_attempts,
                    metal_snapshot.quant.gated_block_fast_attempts,
                },
            );
            print(
                "metal_gated_quantized_block: calls={d} quantized_branch={d} attn_calls={d} attn_nulls={d} attn_prefill_nulls={d} attn_decode_nulls={d} norm_nulls={d} f32_kv_calls={d} f32_kv_ok={d} f32_kv_nulls={d} f32_quant_direct_ok={d} f32_quant_direct_fail={d} compressed_f32_reroutes={d} active_bootstrap_misses={d}\n",
                .{
                    metal_snapshot.provider.compressed_block_gated_calls,
                    metal_snapshot.provider.compressed_block_gated_quantized_branch_calls,
                    metal_snapshot.provider.compressed_block_quantized_attention_calls,
                    metal_snapshot.provider.compressed_block_gated_quantized_attention_nulls,
                    metal_snapshot.provider.compressed_block_gated_quantized_attention_prefill_nulls,
                    metal_snapshot.provider.compressed_block_gated_quantized_attention_decode_nulls,
                    metal_snapshot.provider.compressed_block_gated_quantized_norm_nulls,
                    metal_snapshot.provider.f32_kv_gated_block_calls,
                    metal_snapshot.provider.f32_kv_gated_block_successes,
                    metal_snapshot.provider.f32_kv_gated_block_nulls,
                    metal_snapshot.provider.f32_kv_quant_direct_block_successes,
                    metal_snapshot.provider.f32_kv_quant_direct_block_failures,
                    metal_snapshot.provider.compressed_block_active_frame_f32_reroutes,
                    metal_snapshot.provider.compressed_block_active_frame_bootstrap_misses,
                },
            );
            print(
                "metal_decoder_frame: begins={d} submits={d} wait_ms={d} gpu_ms={d} last_compute_encoders={d} last_blit_encoders={d} total_compute_encoders={d} total_blit_encoders={d}\n",
                .{
                    metal_snapshot.provider.decoder_runtime_frame_begins,
                    metal_snapshot.provider.decoder_runtime_frame_submits,
                    @divTrunc(metal_snapshot.provider.decoder_runtime_frame_wait_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.decoder_runtime_frame_gpu_nanos, std.time.ns_per_ms),
                    metal_snapshot.provider.metal_runtime_last_frame_compute_encoder_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_encoder_count,
                    metal_snapshot.provider.metal_runtime_compute_encoder_count,
                    metal_snapshot.provider.metal_runtime_blit_encoder_count,
                },
            );
            print(
                "metal_decoder_frame_blits: upload={d} copy={d} slice={d} attention_span={d} ffn_copy={d} embedding={d} other={d}\n",
                .{
                    metal_snapshot.provider.metal_runtime_last_frame_blit_buffer_upload_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_buffer_copy_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_buffer_slice_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_attention_span_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_ffn_copy_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_embedding_count,
                    metal_snapshot.provider.metal_runtime_last_frame_blit_other_count,
                },
            );
            print(
                "metal_decoder_frame_compute_sources: quant_linear={d} quant_qkv={d} quant_pair_act={d} attention={d} rms_norm={d} head_rope={d} ffn={d} ple={d} tail={d} embedding={d} dense_linear={d} layer={d} other={d}\n",
                .{
                    metal_snapshot.provider.metal_runtime_last_frame_compute_quant_linear_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_quant_qkv_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_quant_pair_act_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_attention_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_rms_norm_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_head_rope_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_ffn_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_ple_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_tail_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_embedding_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_dense_linear_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_layer_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_other_count,
                },
            );
            print(
                "metal_decoder_frame_compute_regions: attention={d} attention_project={d} ffn_norm={d} ffn={d} ple={d} tail={d} embedding={d} layer={d} other={d}\n",
                .{
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_attention_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_attention_project_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_ffn_norm_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_ffn_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_ple_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_tail_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_embedding_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_layer_count,
                    metal_snapshot.provider.metal_runtime_last_frame_compute_region_other_count,
                },
            );
            print(
                "metal_active_decode_kernels: attention_f32={d} q8_0_linear={d} q8_0_attn_linear={d} q8_0_ffn_down={d} q8_0_ple={d} q8_0_pair_activation={d} rms_norm={d} rms_norm_add={d} layer_norm={d} add={d} head_norm_rope_fused={d} blit={d}\n",
                .{
                    metal_snapshot.provider.active_decode_attention_f32_kernels,
                    metal_snapshot.provider.active_decode_q8_0_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_attention_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_ffn_down_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_ple_linear_kernels,
                    metal_snapshot.provider.active_decode_q8_0_pair_activation_kernels,
                    metal_snapshot.provider.active_decode_rms_norm_kernels,
                    metal_snapshot.provider.active_decode_rms_norm_add_kernels,
                    metal_snapshot.provider.active_decode_layer_norm_kernels,
                    metal_snapshot.provider.active_decode_add_kernels,
                    metal_snapshot.provider.active_decode_head_norm_rope_fused_kernels,
                    metal_snapshot.provider.active_decode_blit_copies,
                },
            );
            print(
                "metal_active_decode_ops: layers={d} layer_input_direct={d}/{d} attn_norm={d} q_linear={d} qkv={d} head_norm={d} rope={d} head_norm_rope_fused={d} ple={d} final_fused_argmax={d} final_split_argmax={d}\n",
                .{
                    metal_snapshot.provider.active_decode_layers,
                    metal_snapshot.provider.active_decode_layer_input_direct_hits,
                    metal_snapshot.provider.active_decode_layer_input_direct_attempts,
                    metal_snapshot.provider.active_decode_attn_norm_ops,
                    metal_snapshot.provider.active_decode_q_linear_ops,
                    metal_snapshot.provider.active_decode_qkv_ops,
                    metal_snapshot.provider.active_decode_head_norm_ops,
                    metal_snapshot.provider.active_decode_rope_ops,
                    metal_snapshot.provider.active_decode_head_norm_rope_fused_ops,
                    metal_snapshot.provider.active_decode_ple_ops,
                    metal_snapshot.provider.active_decode_final_fused_argmax_ops,
                    metal_snapshot.provider.active_decode_final_split_argmax_ops,
                },
            );
            print(
                "metal_frame_fallbacks: decode_attempts={d} decode_success={d} decode_disabled={d} decode_scratch_fail={d} decode_fallback={d} decode_batch={d} decode_initial={d} decode_layer={d} decode_tail={d} prefill_plan={d}/{d} prefill_plan_fail={d} prefill_execute={d}/{d} prefill_execute_fail={d} prefill_missing_ple={d}\n",
                .{
                    metal_snapshot.provider.active_decode_frame_attempts,
                    metal_snapshot.provider.active_decode_frame_successes,
                    metal_snapshot.provider.active_decode_frame_disabled,
                    metal_snapshot.provider.active_decode_frame_scratch_failures,
                    metal_snapshot.provider.active_decode_frame_fallbacks,
                    metal_snapshot.provider.active_decode_frame_batch_fallbacks,
                    metal_snapshot.provider.active_decode_frame_initial_tensor_fallbacks,
                    metal_snapshot.provider.active_decode_frame_layer_fallbacks,
                    metal_snapshot.provider.active_decode_frame_tail_fallbacks,
                    metal_snapshot.provider.prefill_frame_plan_successes,
                    metal_snapshot.provider.prefill_frame_plan_attempts,
                    metal_snapshot.provider.prefill_frame_plan_failures,
                    metal_snapshot.provider.prefill_frame_execute_successes,
                    metal_snapshot.provider.prefill_frame_execute_attempts,
                    metal_snapshot.provider.prefill_frame_execute_failures,
                    metal_snapshot.provider.prefill_frame_execute_missing_ple,
                },
            );
            print(
                "metal_frame_contract: ops={d} scopes={d} barriers={d} windows={d} full_frames={d} layer_contracts={d} tail_contracts={d} local_plan_bypass={d} scope_links={d} layer_runtime={d}/{d} layer_runtime_fail={d} layer_staged_path={d} tail_hits={d} tail_misses={d} no_runtime={d} no_active={d} invalid_contract={d} invalid_shape={d} missing_plan={d} plan_mismatch={d} output_hidden_set={d}\n",
                .{
                    metal_snapshot.provider.prefill_frame_contract_ops,
                    metal_snapshot.provider.prefill_frame_contract_scopes,
                    metal_snapshot.provider.prefill_frame_contract_barriers,
                    metal_snapshot.provider.prefill_frame_contract_windows,
                    metal_snapshot.provider.prefill_frame_contract_full_frames,
                    metal_snapshot.provider.prefill_frame_executor_layer_contracts,
                    metal_snapshot.provider.prefill_frame_executor_tail_contracts,
                    metal_snapshot.provider.prefill_frame_executor_local_plan_bypasses,
                    metal_snapshot.provider.prefill_frame_executor_scope_links,
                    metal_snapshot.provider.prefill_frame_executor_layer_runtime_successes,
                    metal_snapshot.provider.prefill_frame_executor_layer_runtime_calls,
                    metal_snapshot.provider.prefill_frame_executor_layer_runtime_failures,
                    metal_snapshot.provider.prefill_frame_executor_layer_staged_paths,
                    metal_snapshot.provider.prefill_frame_tail_contract_hits,
                    metal_snapshot.provider.prefill_frame_tail_contract_misses,
                    metal_snapshot.provider.prefill_frame_execute_no_runtime,
                    metal_snapshot.provider.prefill_frame_execute_no_active_frame,
                    metal_snapshot.provider.prefill_frame_execute_invalid_contract,
                    metal_snapshot.provider.prefill_frame_execute_invalid_shape,
                    metal_snapshot.provider.prefill_frame_execute_missing_plan,
                    metal_snapshot.provider.prefill_frame_execute_plan_mismatch,
                    metal_snapshot.provider.prefill_frame_execute_output_hidden_set,
                },
            );
            print(
                "metal_quant_block_apply_ms: total={d} replace_span={d} attention_span={d} attention_prefix={d} gated_ffn={d} command_wait={d} gpu={d}\n",
                .{
                    @divTrunc(metal_snapshot.provider.compressed_block_apply_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_replace_span_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_attention_span_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_attention_prefix_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_gated_ffn_residual_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_command_wait_nanos, std.time.ns_per_ms),
                    @divTrunc(metal_snapshot.provider.compressed_block_gpu_nanos, std.time.ns_per_ms),
                },
            );
            print(
                "metal_gated_quantized_failures: span_update={d} span_attn={d} post_linear_fail={d} ffn_direct_ok={d} ffn_direct_fallback={d} ffn_backend_fallback={d} ffn_runtime_fail={d}\n",
                .{
                    metal_snapshot.provider.compressed_attention_residual_update_span_failures,
                    metal_snapshot.provider.compressed_attention_residual_attention_span_failures,
                    metal_snapshot.provider.compressed_attention_residual_post_linear_failures,
                    metal_snapshot.provider.quantized_gated_ffn_direct_successes,
                    metal_snapshot.provider.quantized_gated_ffn_direct_fallbacks,
                    metal_snapshot.provider.quantized_gated_ffn_backend_fallbacks,
                    metal_snapshot.provider.quantized_gated_ffn_runtime_failures,
                },
            );
            print(
                "metal_gemma_family: qkv_hits={d} qkv_fallbacks={d} attn_hits={d} attn_fallbacks={d} ffn_hits={d} ffn_fallbacks={d}\n",
                .{
                    gated_stats.gemma_fused_qkv_hits,
                    gated_stats.gemma_fused_qkv_fallbacks,
                    gated_stats.gemma_fused_attn_residual_hits,
                    gated_stats.gemma_fused_attn_residual_fallbacks,
                    gated_stats.gemma_fused_ffn_hits,
                    gated_stats.gemma_fused_ffn_fallbacks,
                },
            );
            print(
                "metal_gemma_runtime_residency: qkv_hits={d} qkv_fallbacks={d} o_proj_hits={d} o_proj_fallbacks={d} mlp_proj_hits={d} mlp_proj_fallbacks={d} attention_matmul_hits={d} attention_matmul_fallbacks={d} rms_norm_hits={d} rms_norm_fallbacks={d} softmax_hits={d} softmax_fallbacks={d} residual_add_hits={d} residual_add_fallbacks={d} elementwise_mul_hits={d} elementwise_mul_fallbacks={d}\n",
                .{
                    gated_stats.gemma_qkv_hits,
                    gated_stats.gemma_qkv_fallbacks,
                    gated_stats.gemma_o_proj_hits,
                    gated_stats.gemma_o_proj_fallbacks,
                    gated_stats.gemma_mlp_proj_hits,
                    gated_stats.gemma_mlp_proj_fallbacks,
                    gated_stats.gemma_attention_matmul_hits,
                    gated_stats.gemma_attention_matmul_fallbacks,
                    gated_stats.gemma_rms_norm_hits,
                    gated_stats.gemma_rms_norm_fallbacks,
                    gated_stats.gemma_softmax_hits,
                    gated_stats.gemma_softmax_fallbacks,
                    gated_stats.gemma_residual_add_hits,
                    gated_stats.gemma_residual_add_fallbacks,
                    gated_stats.gemma_elementwise_mul_hits,
                    gated_stats.gemma_elementwise_mul_fallbacks,
                },
            );
        }
    }
    return true;
}

fn runOnnxWholeModelGraphGenerate(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: *const Options,
    messages: []const generation.Message,
    config: generation.GenerationConfig,
    artifact_dir: []const u8,
    started_at: std.Io.Timestamp,
) !void {
    if (!build_options.enable_onnx) return error.BackendUnavailable;
    if (opts.image_count > 0 or opts.audio_count > 0) return error.UnsupportedMultimodalWholeModelArtifact;

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, .native);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const loaded_model_at = std.Io.Timestamp.now(io, .awake);
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config)) {
        return error.DeepSeekV4CompressedGraphModeNotSupported;
    }
    const tokenizer = model.getTokenizer();
    const apply_chat_template = !opts.raw_prompt and !opts.no_chat_template and model.chat_tmpl != null;
    const rendered_prompt = if (opts.raw_prompt)
        try allocator.dupe(u8, opts.prompt)
    else if (apply_chat_template)
        try model.chat_tmpl.?.apply(allocator, messages, true)
    else
        try generation.formatMessages(allocator, messages);
    defer allocator.free(rendered_prompt);
    var prompt_encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        rendered_prompt,
        2048,
        !opts.no_bos and model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    const encoded_prompt_at = std.Io.Timestamp.now(io, .awake);
    defer prompt_encoded.deinit();
    const prompt_tokens = countPromptTokens(prompt_encoded.attention_mask);

    if (opts.print_chat_template_status) {
        print("chat_template={}\n", .{apply_chat_template});
    }
    if (opts.print_prompt) {
        print("prompt:\n{s}\n", .{rendered_prompt});
    }
    if (opts.print_prompt_token_ids) {
        print("prompt_token_ids:", .{});
        for (prompt_encoded.ids[0..prompt_tokens]) |id| {
            print(" {d}", .{id});
        }
        print("\n", .{});
    }

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();
    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .mlx => .mlx,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = if (opts.cache_dtype) |name|
        runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDtype
    else
        session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
        .native => .cpu,
        .metal, .mlx, .cuda => .gpu,
    };
    var budget_limits = runtime.tier.memory.defaultLimitsForBackend(budget_backend_class);
    budget_limits = session_factory.widenBudgetLimitsForSession(model.session, budget_limits);
    budget_limits = applyBudgetOverrides(budget_limits, opts.*);
    var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
    print("budget: host={d}MB backend={d}MB combined={d}MB\n", .{
        budget_limits.host_limit_bytes / (1024 * 1024),
        budget_limits.backend_limit_bytes / (1024 * 1024),
        budget_limits.combined_limit_bytes / (1024 * 1024),
    });
    run_budget.reserveEstimate(runtime.tier.memory.estimateGptGeneration(
        backend_kind,
        kv_dtype,
        gpt_config,
        prompt_tokens,
        @intCast(@max(opts.max_tokens, 1)),
        256,
    )) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    const created_backend_at = std.Io.Timestamp.now(io, .awake);
    defer cb.deinit();

    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;
    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });
    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    const created_decode_state_at = std.Io.Timestamp.now(io, .awake);
    defer decode_state.deinit();

    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer graph_cache.deinit();
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = allocator,
        .io = io,
        .cb = cb,
        .gpt_config = gpt_config,
        .tokenizer = tokenizer,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .chat_template = if (opts.no_chat_template) null else model.chat_tmpl,
        .prompt_override = if (opts.raw_prompt) rendered_prompt else null,
        .print_timing = opts.print_timing,
        .model_dir = opts.model_dir,
        .artifact_dir = artifact_dir,
        .gguf_projector_path = model.manifest.gguf_projector_path,
        .decode_state = &decode_state,
        .scheduler = null,
        .scheduler_lease = null,
        .graph_cache = &graph_cache,
        .compiled_partition_backend = .onnx,
        .compiled_attachment_target = .whole_model,
        .pjrt_client = null,
    };

    gpt_arch.resetDebugTimingStats();
    var result = try pipeline.generate(messages, config);
    const finished_generate_at = std.Io.Timestamp.now(io, .awake);
    defer result.deinit();

    print("{s}\n", .{result.text});
    if (opts.print_token_ids) {
        if (result.token_ids) |ids| {
            print("token_ids:", .{});
            for (ids) |id| print(" {d}", .{id});
            print("\n", .{});
        } else {
            print("token_ids=unavailable\n", .{});
        }
    }
    if (opts.print_finish_reason or opts.print_token_count) {
        if (opts.print_finish_reason and opts.print_token_count) {
            print("finish_reason={s} tokens={d}\n", .{ result.finish_reason, result.tokens_used });
        } else if (opts.print_finish_reason) {
            print("finish_reason={s}\n", .{result.finish_reason});
        } else {
            print("tokens={d}\n", .{result.tokens_used});
        }
    }
    if (opts.print_timing) printSpeculativeStats(&result);
    if (opts.print_timing) {
        print(
            "timing_ms: load_model={d} prompt_prep={d} scheduler=0 backend_setup={d} decode_setup={d} generate={d} total={d}\n",
            .{
                durationMillis(started_at, loaded_model_at),
                durationMillis(loaded_model_at, encoded_prompt_at),
                durationMillis(encoded_prompt_at, created_backend_at),
                durationMillis(created_backend_at, created_decode_state_at),
                durationMillis(created_decode_state_at, finished_generate_at),
                durationMillis(started_at, finished_generate_at),
            },
        );
    }
}

fn tryRunArtifactForPromptShape(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: *const Options,
    artifact_dir: []const u8,
    artifact_backend: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
) !bool {
    const artifact_started_at = std.Io.Timestamp.now(io, .awake);
    var artifact_arena = std.heap.ArenaAllocator.init(allocator);
    defer artifact_arena.deinit();
    const artifact_allocator = artifact_arena.allocator();
    var found = (try compiled_artifact.findMatchingArtifactPath(allocator, io, artifact_dir, .{
        .backend = artifact_backend,
        .kind = if (std.mem.eql(u8, artifact_backend, "onnx")) "onnx_graph" else null,
        .model_dir = opts.model_dir,
        .seq_len = seq_len,
        .query_seq_len = query_seq_len,
        .attention_mode = attention_mode,
    })) orelse return false;
    defer found.deinit(allocator);

    var artifact_result = try native_run_artifact.runArtifactPrompt(
        artifact_allocator,
        io,
        found.manifest_path,
        opts.prompt,
        false,
        opts.no_chat_template,
        opts.raw_prompt,
    );
    defer artifact_result.deinit(artifact_allocator);
    const finished_generate_at = std.Io.Timestamp.now(io, .awake);
    emitArtifactResultAndExit(&artifact_result, opts, artifact_started_at, finished_generate_at);
    return true;
}

fn emitArtifactResultAndExit(
    artifact_result: *const native_run_artifact.RunResult,
    opts: *const Options,
    started_at: std.Io.Timestamp,
    finished_at: std.Io.Timestamp,
) noreturn {
    print(
        "using offline artifact backend={s} manifest={s}\n",
        .{ artifact_result.backend, artifact_result.manifest_path },
    );
    if (!artifact_result.has_token) std.process.exit(1);
    print("{s}\n", .{artifact_result.token_text});
    if (opts.print_token_ids) {
        print("token_ids: {d}\n", .{artifact_result.token_id});
    }
    if (opts.print_finish_reason or opts.print_token_count) {
        if (opts.print_finish_reason and opts.print_token_count) {
            print("finish_reason=max_tokens tokens=1\n", .{});
        } else if (opts.print_finish_reason) {
            print("finish_reason=max_tokens\n", .{});
        } else {
            print("tokens=1\n", .{});
        }
    }
    if (opts.print_timing) {
        const total_ms = durationMillis(started_at, finished_at);
        print(
            "timing_ms: load_model=0 prompt_prep=0 scheduler=0 backend_setup=0 decode_setup=0 generate={d} total={d}\n",
            .{ total_ms, total_ms },
        );
    }
    std.process.exit(0);
}

fn validateDraftTokenizerCompatibility(
    target_tokenizer: tokenizer_mod.Tokenizer,
    draft_tokenizer: tokenizer_mod.Tokenizer,
    target_cfg: @import("models/gpt.zig").Config,
    draft_cfg: @import("models/gpt.zig").Config,
) !void {
    const target_special = target_tokenizer.specialTokens();
    const draft_special = draft_tokenizer.specialTokens();
    if (draft_tokenizer.vocabSize() != target_tokenizer.vocabSize() or
        draft_cfg.vocab_size != target_cfg.vocab_size or
        draft_special.cls_id != target_special.cls_id or
        draft_special.sep_id != target_special.sep_id or
        draft_special.pad_id != target_special.pad_id or
        draft_special.unk_id != target_special.unk_id)
    {
        return error.IncompatibleDraftTokenizer;
    }
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
        .prompt = args[1],
    };

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--artifact-dir")) {
            i += 1;
            if (i >= args.len) return error.MissingArtifactDir;
            opts.artifact_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i >= args.len) return error.MissingMode;
            opts.mode = parseExecutionMode(args[i]) orelse return error.InvalidMode;
        } else if (std.mem.startsWith(u8, arg, "--mode=")) {
            opts.mode = parseExecutionMode(arg["--mode=".len..]) orelse return error.InvalidMode;
        } else if (std.mem.eql(u8, arg, "--compiled-target")) {
            i += 1;
            if (i >= args.len) return error.MissingCompiledTarget;
            opts.compiled_target = parseCompiledTarget(args[i]) orelse return error.InvalidCompiledTarget;
        } else if (std.mem.startsWith(u8, arg, "--compiled-target=")) {
            opts.compiled_target = parseCompiledTarget(arg["--compiled-target=".len..]) orelse return error.InvalidCompiledTarget;
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            i += 1;
            if (i >= args.len) return error.MissingMaxTokens;
            opts.max_tokens = try std.fmt.parseInt(i32, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--temperature")) {
            i += 1;
            if (i >= args.len) return error.MissingTemperature;
            opts.temperature = try std.fmt.parseFloat(f32, args[i]);
        } else if (std.mem.eql(u8, arg, "--top-p")) {
            i += 1;
            if (i >= args.len) return error.MissingTopP;
            opts.top_p = try std.fmt.parseFloat(f32, args[i]);
        } else if (std.mem.eql(u8, arg, "--top-k")) {
            i += 1;
            if (i >= args.len) return error.MissingTopK;
            opts.top_k = try std.fmt.parseInt(i32, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--repetition-penalty")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.repetition_penalty = try std.fmt.parseFloat(f32, args[i]);
        } else if (std.mem.eql(u8, arg, "--prefill-chunk-size")) {
            i += 1;
            if (i >= args.len) return error.MissingPrefillChunkSize;
            opts.prefill_chunk_size = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--draft-model")) {
            i += 1;
            if (i >= args.len) return error.MissingDraftModel;
            opts.draft_model = args[i];
        } else if (std.mem.eql(u8, arg, "--speculative-k")) {
            i += 1;
            if (i >= args.len) return error.MissingSpeculativeK;
            opts.speculative_k = try std.fmt.parseInt(u32, args[i], 10);
            if (opts.speculative_k == 0) return error.InvalidSpeculativeK;
        } else if (std.mem.eql(u8, arg, "--image")) {
            i += 1;
            if (i >= args.len) return error.MissingImagePath;
            if (opts.image_count >= opts.image_paths.len) return error.TooManyImages;
            opts.image_paths[opts.image_count] = args[i];
            opts.image_count += 1;
        } else if (std.mem.eql(u8, arg, "--audio")) {
            i += 1;
            if (i >= args.len) return error.MissingAudioPath;
            if (opts.audio_count >= opts.audio_paths.len) return error.TooManyAudioInputs;
            opts.audio_paths[opts.audio_count] = args[i];
            opts.audio_count += 1;
        } else if (std.mem.eql(u8, arg, "--no-chat-template")) {
            opts.no_chat_template = true;
        } else if (std.mem.eql(u8, arg, "--print-finish-reason")) {
            opts.print_finish_reason = true;
        } else if (std.mem.eql(u8, arg, "--print-token-count")) {
            opts.print_token_count = true;
        } else if (std.mem.eql(u8, arg, "--print-token-ids")) {
            opts.print_token_ids = true;
        } else if (std.mem.eql(u8, arg, "--print-prompt-token-ids")) {
            opts.print_prompt_token_ids = true;
        } else if (std.mem.eql(u8, arg, "--print-prompt")) {
            opts.print_prompt = true;
        } else if (std.mem.eql(u8, arg, "--print-chat-template-status")) {
            opts.print_chat_template_status = true;
        } else if (std.mem.eql(u8, arg, "--print-timing")) {
            opts.print_timing = true;
        } else if (std.mem.eql(u8, arg, "--raw-prompt")) {
            opts.raw_prompt = true;
        } else if (std.mem.eql(u8, arg, "--no-bos")) {
            opts.no_bos = true;
        } else if (std.mem.eql(u8, arg, "--cache-dtype")) {
            i += 1;
            if (i >= args.len) return error.MissingCacheDtype;
            opts.cache_dtype = args[i];
        } else if (std.mem.eql(u8, arg, "--cache-compaction-ratio")) {
            i += 1;
            if (i >= args.len) return error.MissingCacheCompactionRatio;
            opts.cache_compaction_ratio = try std.fmt.parseFloat(f32, args[i]);
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
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    return native_backend_choice.parse(value);
}

fn parseExecutionMode(value: []const u8) ?ExecutionMode {
    if (std.mem.eql(u8, value, "eager")) return .eager;
    if (std.mem.eql(u8, value, "compiled")) return .compiled;
    return null;
}

fn parseCompiledTarget(value: []const u8) ?CompiledTarget {
    if (std.mem.eql(u8, value, "partitioned")) return .partitioned;
    if (std.mem.eql(u8, value, "whole-model")) return .whole_model;
    if (std.mem.eql(u8, value, "whole_model")) return .whole_model;
    return null;
}

fn printBudgetExceeded(
    session: backends.Session,
    run_budget: *const runtime.tier.memory.RunBudget,
) void {
    var buf: [512]u8 = undefined;
    const msg = session_factory.memoryBudgetExceededDetail(session, run_budget, &buf) catch {
        print("memory budget exceeded\n", .{});
        return;
    };
    print("{s}\n", .{msg});
}

fn printSpeculativeStats(result: *const generation.GenerationResult) void {
    const stats = result.speculative orelse return;
    print(
        "speculative: rounds={d} drafted={d} matched={d} rejected={d} accepted={d} corrections={d} bonus={d}\n",
        .{
            stats.rounds,
            stats.drafted_tokens,
            stats.matched_draft_tokens,
            stats.rejectedDraftTokens(),
            stats.accepted_tokens,
            stats.correction_tokens,
            stats.bonus_tokens,
        },
    );
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    native_backend_choice.configureSessionPreference(session_manager, choice);
}

fn preflightModelLoadBudget(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    opts: Options,
) !void {
    const reservation_tier = predictedWeightTier(allocator, manifest, opts.backend) orelse return;
    const weight_bytes = estimateModelArtifactBytes(allocator, manifest) catch 0;
    if (weight_bytes == 0) return;

    var limits = runtime.tier.memory.defaultLimitsForBackend(switch (reservation_tier) {
        .host => .cpu,
        .backend => .gpu,
        .disk => return,
    });
    const predicted_backend_type: backends.BackendType = switch (reservation_tier) {
        .host => .native,
        .backend => if (opts.backend == .metal) .metal else .mlx,
        .disk => unreachable,
    };
    limits = try session_factory.widenBudgetLimitsForModelPath(
        allocator,
        opts.model_dir,
        limits,
        predicted_backend_type,
    );
    limits = applyBudgetOverrides(limits, opts);
    var run_budget = runtime.tier.memory.RunBudget.init(limits);
    _ = run_budget.tryReserveWeight(reservation_tier, weight_bytes) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            var buf: [512]u8 = undefined;
            const msg = run_budget.lastDenialString(&buf) catch "memory budget exceeded before model load";
            print("{s}; model artifact requires ~{d} MB before prompt/KV/scratch\n", .{
                msg,
                weight_bytes / (1024 * 1024),
            });
        }
        return err;
    };
}

fn predictedWeightTier(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    choice: BackendChoice,
) ?runtime.tier.memory.ResidencyTier {
    switch (choice) {
        .native => return .host,
        .metal => {
            if (!build_options.enable_metal) return .host;
            return .backend;
        },
        .mlx => {
            if (!build_options.enable_mlx) return .host;
            return .backend;
        },
        .cuda => {
            if (!build_options.enable_cuda) return .host;
            return .backend;
        },
        .auto => {
            if (build_options.enable_mlx and !shouldPreferNativeAheadOfMlx(allocator, manifest)) return .backend;
            return .host;
        },
        .onnx, .xla, .webgpu => return .host,
    }
}

fn shouldPreferNativeAheadOfMlx(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
) bool {
    const total_bytes = estimateModelArtifactBytes(allocator, manifest) catch return true;
    return total_bytes == 0 or total_bytes > mlxEagerDenseMaxBytes();
}

fn estimateModelArtifactBytes(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
) !usize {
    if (manifest.gguf_path) |path| return @intCast(try c_file.fileSize(allocator, path));
    if (manifest.safetensors_path) |path| return @intCast(try c_file.fileSize(allocator, path));
    return 0;
}

fn mlxEagerDenseMaxBytes() u64 {
    const mb = platform.env.getenvUsize("TERMITE_MLX_EAGER_DENSE_MAX_MB") orelse return 1024 * 1024 * 1024;
    return mb * 1024 * 1024;
}

fn enableMlxRawMetalWholeTokenDebug() bool {
    return platform.env.getenvBool("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN");
}

fn printUsage() void {
    print(
        \\usage: antfly inference generate <model-dir> <prompt> [--image path] [--audio path] [--backend auto|onnx|native|metal|mlx|xla|webgpu] [--mode eager|compiled] [--compiled-target partitioned|whole-model] [--max-tokens N] [--temperature V] [--top-p V] [--top-k N] [--repetition-penalty V] [--prefill-chunk-size N] [--draft-model path] [--speculative-k N] [--cache-dtype f16|f32|int8|fp8|int4|polar4|turbo3] [--host-budget-mb N] [--backend-budget-mb N] [--combined-budget-mb N] [--kv-budget-mb N] [--scratch-budget-mb N] [--artifact-dir <path>] [--no-chat-template] [--raw-prompt] [--no-bos] [--print-finish-reason] [--print-token-count] [--print-token-ids] [--print-prompt-token-ids] [--print-prompt] [--print-chat-template-status] [--print-timing]
        \\  Loads a native GGUF/SafeTensors model and prints generated text to stdout.
        \\  draft-model enables native speculative decoding with a tokenizer-compatible drafter such as a Gemma 4 *-assistant model.
        \\  Explicit compiled backends consult ~/.antfly/inference/artifacts/<owner>/<model>/<backend>/... by default.
        \\  artifact-dir overrides that lookup root.
        \\  whole-model compiled generate prefers package manifests before raw sidecar scanning.
        \\  compiled-target=whole-model requests a compiled backend only when it can own the full traced graph shape.
        \\
    , .{});
}

fn applyBudgetOverrides(defaults: runtime.tier.memory.Limits, opts: Options) runtime.tier.memory.Limits {
    var limits = defaults;
    if (opts.host_budget_mb > 0) limits.host_limit_bytes = opts.host_budget_mb * 1024 * 1024;
    if (opts.backend_budget_mb > 0) limits.backend_limit_bytes = opts.backend_budget_mb * 1024 * 1024;
    if (opts.combined_budget_mb > 0) limits.combined_limit_bytes = opts.combined_budget_mb * 1024 * 1024;
    if (opts.kv_budget_mb > 0) limits.kv_limit_bytes = opts.kv_budget_mb * 1024 * 1024;
    if (opts.scratch_budget_mb > 0) limits.scratch_limit_bytes = opts.scratch_budget_mb * 1024 * 1024;
    return limits;
}

fn graphModeEnabled() bool {
    return getenvBool("TERMITE_GRAPH_MODE");
}

fn nativeGenerateSchedulerEnabled() bool {
    return !getenvBool("TERMITE_DISABLE_NATIVE_GENERATE_SCHEDULER");
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
}

fn countPromptTokens(attention_mask: anytype) usize {
    var count: usize = 0;
    while (count < attention_mask.len and attention_mask[count] != 0) : (count += 1) {}
    return count;
}

test "parseArgs accepts artifact dir" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "hello",
        "--backend",
        "xla",
        "--artifact-dir",
        "/tmp/artifacts",
        "--max-tokens",
        "1",
        "--raw-prompt",
    });
    try std.testing.expectEqual(BackendChoice.xla, opts.backend);
    try std.testing.expectEqualStrings("/tmp/artifacts", opts.artifact_dir.?);
    try std.testing.expectEqual(@as(i32, 1), opts.max_tokens);
    try std.testing.expect(opts.raw_prompt);
}

test "parseArgs accepts compiled target" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "hello",
        "--backend",
        "xla",
        "--mode",
        "compiled",
        "--compiled-target",
        "whole-model",
    });
    try std.testing.expectEqual(BackendChoice.xla, opts.backend);
    try std.testing.expectEqual(ExecutionMode.compiled, opts.mode.?);
    try std.testing.expectEqual(CompiledTarget.whole_model, opts.compiled_target.?);
}

test "explicit compiled whole model does not route through live executor" {
    const opts = Options{
        .model_dir = "/tmp/model",
        .prompt = "hello",
        .backend = .metal,
        .mode = .compiled,
        .compiled_target = .whole_model,
    };
    try std.testing.expect(!liveWholeModelExecutorRequested(&opts));
}
