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
const httpx = @import("httpx");
const api = @import("inference_api");
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
const gpt_mod = @import("models/gpt.zig");
const runtime = @import("runtime/root.zig");
const c_file = @import("util/c_file.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const native_run_artifact = @import("native_run_artifact.zig");
const compiled_artifact = @import("compiled_artifact.zig");
const compat = @import("io/compat.zig");
const cuda_context = if (build_options.enable_cuda) @import("ops/cuda/context.zig") else struct {};
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

fn debugGenerateSetup(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBool("TERMITE_GEN_STAGE_DEBUG")) return;
    std.debug.print("generate-setup: " ++ fmt ++ "\n", args);
}

fn shouldSkipAutoMtpDraftLoad(opts: Options, draft_cfg: gpt_mod.Config) bool {
    if (opts.speculation_policy != .auto) return false;
    if (!draft_cfg.gemma4_mtp_assistant) return false;
    if (opts.speculation_calibration == .none) return true;
    const requested_max_tokens: usize = @intCast(@max(opts.max_tokens, 1));
    return requested_max_tokens < generation.gemma4MtpAutoMinGenerationTokens();
}

const Options = struct {
    model_dir: []const u8,
    prompt: []const u8,
    image_paths: [8][]const u8 = .{""} ** 8,
    image_count: usize = 0,
    audio_paths: [8][]const u8 = .{""} ** 8,
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
    speculation_policy: generation.SpeculationPolicy = .auto,
    speculation_calibration: generation.SpeculationCalibration = .none,
    no_chat_template: bool = false,
    print_finish_reason: bool = false,
    print_token_count: bool = false,
    print_token_ids: bool = false,
    print_prompt_token_ids: bool = false,
    print_prompt: bool = false,
    print_chat_template_status: bool = false,
    print_timing: bool = false,
    debug_mtp: bool = false,
    debug_gemma4_target: bool = false,
    disable_gemma_embedding_scale: bool = false,
    host_budget_mb: usize = 0,
    backend_budget_mb: usize = 0,
    combined_budget_mb: usize = 0,
    kv_budget_mb: usize = 0,
    scratch_budget_mb: usize = 0,
    raw_prompt: bool = false,
    no_bos: bool = false,
    raw_decode_bench: bool = false,
    ignore_eos: bool = false,
    cache_dtype: ?[]const u8 = null,
    cache_compaction_ratio: ?f32 = null,
    mode: ?ExecutionMode = null,
    compiled_target: ?CompiledTarget = null,
    artifact_dir: ?[]const u8 = null,
    server_url: ?[]const u8 = null,
    require_server: bool = false,
    stream: bool = false,
    json_timing_path: ?[]const u8 = null,
};

fn shouldDisableMetalAutoDraft(opts: Options) bool {
    return opts.backend == .metal and
        opts.speculation_policy == .auto and
        !platform.env.getenvBool("ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO");
}

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    const effective_draft_model = if (opts.speculation_policy == .off or shouldDisableMetalAutoDraft(opts)) null else opts.draft_model;
    if (opts.raw_decode_bench and (opts.image_count > 0 or opts.audio_count > 0)) return error.RawDecodeBenchRequiresTextOnly;
    if (opts.raw_decode_bench and effective_draft_model != null) return error.RawDecodeBenchSpeculationUnsupported;
    if (opts.raw_decode_bench and opts.stream) return error.RawDecodeBenchStreamingUnsupported;
    try native_backend_choice.validate(opts.backend);
    const require_server = requireWarmServer(opts);
    if (effective_draft_model != null and opts.backend == .onnx) return error.SpeculativeDecodingRequiresNativeBackend;
    if (opts.server_url orelse platform.env.getenv("ANTFLY_INFERENCE_SERVER_URL")) |server_url| {
        if (opts.raw_decode_bench) return error.RawDecodeBenchRequiresLocalBackend;
        var server_opts = opts;
        server_opts.server_url = server_url;
        if (defaultServerModelName(opts.model_dir)) |model_name| server_opts.model_dir = model_name;
        return try runServerGenerate(allocator, io, server_opts, false);
    }
    if (require_server) {
        if (!serverGenerateSupportsOptions(opts)) return error.UnsupportedServerGenerateOption;
        return error.WarmInferenceServerUnavailable;
    }
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
        .draft_model = effective_draft_model,
        .speculative_k = opts.speculative_k,
        .speculation_requested = effective_draft_model != null,
        .speculation_policy = opts.speculation_policy,
        .speculation_calibration = opts.speculation_calibration,
        .cache_compaction_ratio = opts.cache_compaction_ratio,
        .ignore_eos = opts.ignore_eos,
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
    if (allow_direct_onnx and effective_draft_model == null and build_options.enable_onnx and
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

        if (artifact_backend != null and effective_draft_model == null and !route_onnx_whole_model_graph and opts.max_tokens == 1 and opts.image_count == 0 and opts.audio_count == 0) {
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

        var result = try generateWithOptionalStreaming(&pipeline, &messages, config, opts.stream);
        defer result.deinit();
        const finished_generate_at = std.Io.Timestamp.now(io, .awake);

        if (!opts.stream) print("{s}\n", .{result.text});
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

    if (artifact_backend != null and effective_draft_model == null and !route_onnx_whole_model_graph and opts.max_tokens == 1 and opts.image_count == 0 and opts.audio_count == 0) {
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

    generation.gemma4_mtp_debug_override = opts.debug_mtp;
    try warmInitCudaBeforeLargeModelScan(opts.backend);
    debugGenerateSetup("load model begin dir={s}", .{opts.model_dir});
    const model = model_manager.loadFromDir(opts.model_dir) catch |err| {
        debugGenerateSetup("load model failed err={s}", .{@errorName(err)});
        return err;
    };
    debugGenerateSetup("load model done backend={s}", .{@tagName(model.session.backend())});
    const loaded_model_at = std.Io.Timestamp.now(io, .awake);
    var gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (opts.disable_gemma_embedding_scale and gpt_config.family == .gemma) {
        gpt_config.disable_token_embedding_scale = true;
    }
    const tokenizer = model.getTokenizer();
    if (effective_draft_model != null and (opts.image_count > 0 or opts.audio_count > 0)) {
        return error.MultimodalSpeculativeDecodingNotSupported;
    }
    var draft_gpt_config: ?gpt_mod.Config = null;
    const draft_model = if (effective_draft_model) |draft_model_dir| blk: {
        if (opts.speculation_policy == .auto) {
            var draft_manifest = try manifest_mod.loadFromDir(allocator, draft_model_dir);
            defer draft_manifest.deinit();
            const draft_cfg = try session_factory.loadGptConfigFromModelDir(allocator, draft_model_dir, draft_manifest);
            if (shouldSkipAutoMtpDraftLoad(opts, draft_cfg)) {
                draft_gpt_config = draft_cfg;
                break :blk null;
            }
        }
        debugGenerateSetup("load draft model begin dir={s}", .{draft_model_dir});
        const loaded = model_manager.loadFromDir(draft_model_dir) catch |err| {
            debugGenerateSetup("load draft model failed err={s}", .{@errorName(err)});
            return err;
        };
        debugGenerateSetup("load draft model done backend={s}", .{@tagName(loaded.session.backend())});
        const draft_cfg = session_factory.getGptConfig(loaded.session) orelse return error.InvalidDraftModelForGeneration;
        try validateDraftTokenizerCompatibility(tokenizer, loaded.getTokenizer(), gpt_config, draft_cfg);
        draft_gpt_config = draft_cfg;
        break :blk loaded;
    } else null;

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

    if (artifact_backend != null and effective_draft_model == null and !route_onnx_whole_model_graph and opts.max_tokens == 1 and opts.image_count == 0 and opts.audio_count == 0) {
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
        effective_draft_model == null and
        opts.image_count == 0 and
        opts.audio_count == 0 and
        graph_mod.metal_executor.supportsSession(model.session))
    {
        _ = graph_mod.metal_executor.prewarmSharedDecoderRuntime(allocator, model.session, gpt_config) catch |err| {
            std.log.warn("metal decoder-runtime prewarm failed for {s}: {s}", .{ opts.model_dir, @errorName(err) });
        };
    }

    debugGenerateSetup("live whole-model executor probe begin", .{});
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
    )) {
        debugGenerateSetup("live whole-model executor handled request", .{});
        return;
    }
    debugGenerateSetup("live whole-model executor skipped", .{});

    if (!graph_mode) {
        graph_mod.executor_stats.printBypass("inference.generate", "native_generation_direct_decoder_runtime");
    }

    const decoder_runtime_scheduler_override = false;
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
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const requested_kv_dtype = if (opts.cache_dtype) |name|
        runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDtype
    else
        session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const kv_dtype = effectiveGenerationKvDType(
        requested_kv_dtype,
        backend_kind,
        gpt_config,
        prompt_tokens,
        @intCast(@max(opts.max_tokens, 1)),
    );
    if (opts.print_timing and kv_dtype != requested_kv_dtype) {
        print("cache_dtype_effective: requested={s} effective={s}\n", .{ @tagName(requested_kv_dtype), @tagName(kv_dtype) });
    }
    const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
        .native => .cpu,
        else => .gpu,
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
        const requested_draft_kv_dtype = if (opts.cache_dtype) |name|
            runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDtype
        else
            session_factory.recommendedKvDTypeForSession(loaded.session, backend_kind);
        break :blk if (draft_gpt_config) |draft_cfg|
            effectiveGenerationKvDType(
                requested_draft_kv_dtype,
                backend_kind,
                draft_cfg,
                prompt_tokens,
                @intCast(@max(opts.max_tokens, 1)),
            )
        else
            requested_draft_kv_dtype;
    } else null;
    if (draft_model != null) {
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
    }
    debugGenerateSetup("compute backend begin", .{});
    var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
        debugGenerateSetup("compute backend failed err={s}", .{@errorName(err)});
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    debugGenerateSetup("compute backend done", .{});
    const created_backend_at = std.Io.Timestamp.now(io, .awake);
    defer cb.deinit();
    if (opts.debug_gemma4_target) {
        try printGemma4TargetDebug(&cb, tokenizer, model.manifest, gpt_config, prompt_encoded.ids[0..prompt_tokens]);
    }
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
    else if (gpt_config.sliding_window > 0 and gpt_config.hasGlobalAttentionLayers() and !kvSlidingTrimForced())
        // Mixed attention (iSWA-style models like Gemma): global layers need
        // the full KV history, and the pool packs every layer's KV into
        // shared blocks, so window-trimming the pool silently truncates the
        // global layers' context. Retain everything; sliding-window layers
        // still apply their exact window inside the attention kernels, so
        // their compute stays bounded — only KV memory grows with context.
        // ANTFLY_INFERENCE_KV_SLIDING_TRIM=1 restores the old
        // trim-to-window behavior (lower memory, truncated global context).
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
    var kv_storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, .{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });
    defer kv_storage.deinit();
    try cb.provisionKvDeviceWriteHook(&kv_storage);

    var cuda_gemma_prefill_prewarm_ms: u64 = 0;
    if (cudaGemmaPrefillPrewarmEnabled()) {
        const prewarm_started_at = std.Io.Timestamp.now(io, .awake);
        // Prewarm is a pure residency optimization; a failure must not
        // abort the generation the real prefill could still serve.
        const prewarmed = prewarmCudaGemmaPrefillResidency(
            allocator,
            &cb,
            gpt_config,
            prompt_tokens,
        ) catch |err| blk: {
            std.log.warn("cuda_gemma_prefill_prewarm_failed: err={s}", .{@errorName(err)});
            break :blk false;
        };
        const prewarm_finished_at = std.Io.Timestamp.now(io, .awake);
        if (prewarmed) {
            cuda_gemma_prefill_prewarm_ms = durationMillis(prewarm_started_at, prewarm_finished_at);
        }
    }

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    decode_state.kv_storage = &kv_storage;
    const created_decode_state_at = std.Io.Timestamp.now(io, .awake);
    defer decode_state.deinit();
    var draft_kv_manager: ?runtime.kv.manager.KvManager = null;
    defer if (draft_kv_manager) |*manager| manager.deinit();
    var draft_decode_state: ?generation.NativeDecodeState = null;
    defer if (draft_decode_state) |*state| state.deinit();
    if (draft_model != null) {
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

    if (build_options.enable_metal and opts.print_timing and model.session.backend().usesGpuHostedSession()) {
        debug_timing.resetLiveGpuTimingStats(&cb);
        generation.resetDecoderRuntimeDebugStats();
    }
    gpt_arch.resetDebugTimingStats();

    const cuda_stats_before_generate = if (comptime build_options.enable_cuda)
        session_factory.getCudaRuntimeStats(model.session)
    else
        null;
    const draft_cuda_stats_before_generate = if (comptime build_options.enable_cuda)
        if (draft_model) |loaded_draft| session_factory.getCudaRuntimeStats(loaded_draft.session) else null
    else
        null;
    if (opts.raw_decode_bench) {
        const bench_result = runRawDecodeBench(
            allocator,
            io,
            &cb,
            gpt_config,
            prompt_encoded.ids[0..countPromptTokens(prompt_encoded.attention_mask)],
            @intCast(@max(opts.max_tokens, 0)),
            config.prefill_chunk_size,
            &decode_state,
        ) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                printBudgetExceeded(model.session, &run_budget);
            }
            return err;
        };
        const finished_generate_at = std.Io.Timestamp.now(io, .awake);
        const cuda_stats_after_generate = if (comptime build_options.enable_cuda)
            session_factory.getCudaRuntimeStats(model.session)
        else
            null;
        const cuda_generate_stats = if (comptime build_options.enable_cuda)
            if (cuda_stats_after_generate) |after|
                if (cuda_stats_before_generate) |before| cudaStatsDelta(after, before) else null
            else
                null
        else
            null;

        if (opts.print_token_count) print("tokens={d}\n", .{bench_result.tokens});
        if (opts.print_timing) {
            print(
                "timing_ms: load_model={d} prompt_prep={d} scheduler={d} backend_setup={d} decode_setup={d} prefill={d} device_warmup={d} warmup={d} decode={d} total={d}\n",
                .{
                    durationMillis(started_at, loaded_model_at),
                    durationMillis(loaded_model_at, encoded_prompt_at),
                    durationMillis(encoded_prompt_at, acquired_scheduler_at),
                    durationMillis(acquired_scheduler_at, created_backend_at),
                    durationMillis(created_backend_at, created_decode_state_at),
                    bench_result.prefill_ms,
                    bench_result.device_warmup_ms,
                    bench_result.warmup_ms,
                    bench_result.decode_ms,
                    durationMillis(started_at, finished_generate_at),
                },
            );
            print("raw_decode_tok_per_s={d:.3}\n", .{tokensPerSecond(bench_result.tokens, bench_result.decode_ms)});
            print("raw_decode_scope={s}\n", .{bench_result.scope});
        }
        if (opts.json_timing_path) |path| {
            try writeRawDecodeBenchJson(
                allocator,
                io,
                path,
                opts.model_dir,
                @tagName(model.session.backend()),
                bench_result,
                durationMillis(started_at, loaded_model_at),
                durationMillis(loaded_model_at, encoded_prompt_at),
                durationMillis(encoded_prompt_at, acquired_scheduler_at),
                durationMillis(acquired_scheduler_at, created_backend_at),
                durationMillis(created_backend_at, created_decode_state_at),
                durationMillis(started_at, finished_generate_at),
                cuda_stats_after_generate,
                cuda_generate_stats,
            );
        }
        return;
    }
    var result = generateWithOptionalStreaming(&pipeline, &messages, config, opts.stream) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        } else if (err == error.AudioInputTooLong) {
            print("error: {s}\n", .{generation.userFacingErrorMessage(err)});
        }
        return err;
    };
    const finished_generate_at = std.Io.Timestamp.now(io, .awake);
    const cuda_stats_after_generate = if (comptime build_options.enable_cuda)
        session_factory.getCudaRuntimeStats(model.session)
    else
        null;
    const draft_cuda_stats_after_generate = if (comptime build_options.enable_cuda)
        if (draft_model) |loaded_draft| session_factory.getCudaRuntimeStats(loaded_draft.session) else null
    else
        null;
    const cuda_generate_stats = if (comptime build_options.enable_cuda)
        if (cuda_stats_after_generate) |after|
            if (cuda_stats_before_generate) |before| cudaStatsDelta(after, before) else null
        else
            null
    else
        null;
    const draft_cuda_generate_stats = if (comptime build_options.enable_cuda)
        if (draft_cuda_stats_after_generate) |after|
            if (draft_cuda_stats_before_generate) |before| cudaStatsDelta(after, before) else null
        else
            null
    else
        null;
    defer result.deinit();

    if (!opts.stream) print("{s}\n", .{result.text});
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
        if (cuda_gemma_prefill_prewarm_ms != 0) {
            print("cuda_gemma_prefill_prewarm_ms: runtime_prepare={d}\n", .{cuda_gemma_prefill_prewarm_ms});
        }
        const decode_ms = if (result.timing_ms) |timing| timing.decode else durationMillis(created_decode_state_at, finished_generate_at);
        print("decode_tok_per_s={d:.3}\n", .{tokensPerSecond(result.tokens_used, decode_ms)});
        if (build_options.enable_metal and model.session.backend().usesGpuHostedSession() and detailedGpuTimingEnabled()) {
            printGpuHostedTimingDetails(&cb);
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
            printMetalQuantDispatchSummary(metal_snapshot);
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
                "metal_active_decode_kernels: attention_f32={d} quant_linear={d} quant_attn_linear={d} quant_ffn_down={d} quant_ple={d} quant_gate_up_pair={d} rms_norm={d} rms_norm_add={d} layer_norm={d} add={d} head_norm_rope_fused={d} blit={d}\n",
                .{
                    metal_snapshot.provider.active_decode_attention_f32_kernels,
                    metal_snapshot.provider.active_decode_quant_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_attention_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_ffn_down_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_ple_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_gate_up_pair_kernels,
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
        if (comptime build_options.enable_cuda) {
            if (cuda_stats_after_generate) |cuda_stats| {
                print(
                    "cuda_runtime_counts: launches={d} syncs={d} upload_syncs={d} download_syncs={d} eval_syncs={d} alloc_calls={d} free_calls={d} temp_hits={d} temp_misses={d} temp_releases={d} temp_evictions={d} temp_cached_mb={d} deferred_free_queued={d} deferred_free_drains={d} deferred_free_forced_drains={d} deferred_free_pending_mb={d} deferred_free_reclaimed_mb={d}\n",
                    .{
                        cuda_stats.kernel_launches,
                        cuda_stats.stream_syncs,
                        cuda_stats.upload_syncs,
                        cuda_stats.download_syncs,
                        cuda_stats.eval_syncs,
                        cuda_stats.device_alloc_calls,
                        cuda_stats.device_free_calls,
                        cuda_stats.temp_buffer_hits,
                        cuda_stats.temp_buffer_misses,
                        cuda_stats.temp_buffer_releases,
                        cuda_stats.temp_buffer_evictions,
                        cuda_stats.temp_buffer_cached_bytes / (1024 * 1024),
                        cuda_stats.deferred_free_queued,
                        cuda_stats.deferred_free_drains,
                        cuda_stats.deferred_free_forced_drains,
                        cuda_stats.deferred_free_pending_bytes / (1024 * 1024),
                        cuda_stats.deferred_free_reclaimed_bytes / (1024 * 1024),
                    },
                );
                print(
                    "cuda_runtime_rates: launches_per_token={d:.3} syncs_per_token={d:.3}\n",
                    .{
                        perToken(cuda_stats.kernel_launches, result.tokens_used),
                        perToken(cuda_stats.stream_syncs, result.tokens_used),
                    },
                );
                if (cuda_generate_stats) |generate_stats| {
                    print(
                        "cuda_generate_counts: launches={d} syncs={d} upload_syncs={d} download_syncs={d} eval_syncs={d} alloc_calls={d} free_calls={d} temp_hits={d} temp_misses={d} temp_releases={d} temp_evictions={d} h2d={d} d2h={d} to_f32_calls={d} to_f32_bytes={d} argmax={d} norm={d} elementwise={d} scalar={d} qkv={d} linear={d} attention={d} deferred_free_queued={d} deferred_free_drains={d} deferred_free_forced_drains={d}\n",
                        .{
                            generate_stats.kernel_launches,
                            generate_stats.stream_syncs,
                            generate_stats.upload_syncs,
                            generate_stats.download_syncs,
                            generate_stats.eval_syncs,
                            generate_stats.device_alloc_calls,
                            generate_stats.device_free_calls,
                            generate_stats.temp_buffer_hits,
                            generate_stats.temp_buffer_misses,
                            generate_stats.temp_buffer_releases,
                            generate_stats.temp_buffer_evictions,
                            generate_stats.h2d_bytes,
                            generate_stats.d2h_bytes,
                            generate_stats.to_float32_calls,
                            generate_stats.to_float32_bytes,
                            generate_stats.launch_argmax,
                            generate_stats.launch_norm,
                            generate_stats.launch_elementwise,
                            generate_stats.launch_scalar,
                            generate_stats.launch_linear_qkv,
                            generate_stats.launch_linear,
                            generate_stats.launch_attention,
                            generate_stats.deferred_free_queued,
                            generate_stats.deferred_free_drains,
                            generate_stats.deferred_free_forced_drains,
                        },
                    );
                    print(
                        "cuda_generate_cross_backend_copy: copies={d} bytes={d} event_records={d} event_waits={d} sync_fallbacks={d}\n",
                        .{
                            generate_stats.cross_backend_copies,
                            generate_stats.cross_backend_copy_bytes,
                            generate_stats.cross_backend_event_records,
                            generate_stats.cross_backend_event_waits,
                            generate_stats.cross_backend_sync_fallbacks,
                        },
                    );
                    print(
                        "cuda_generate_pinned_scalar_transfers: uploads={d} upload_bytes={d} upload_fallbacks={d} upload_wrap_syncs={d} downloads={d} download_bytes={d} download_fallbacks={d}\n",
                        .{
                            generate_stats.pinned_scalar_uploads,
                            generate_stats.pinned_scalar_upload_bytes,
                            generate_stats.pinned_scalar_upload_fallbacks,
                            generate_stats.pinned_scalar_upload_wrap_syncs,
                            generate_stats.pinned_scalar_downloads,
                            generate_stats.pinned_scalar_download_bytes,
                            generate_stats.pinned_scalar_download_fallbacks,
                        },
                    );
                    print(
                        "cuda_generate_norm_launch_breakdown: layer={d} add_layer={d} rms={d} rms_add={d} rms_add_mul_scalar={d} rms_add_output_scale={d} rms_bare={d} head_rope={d}\n",
                        .{
                            generate_stats.launch_norm_layer,
                            generate_stats.launch_norm_add_layer,
                            generate_stats.launch_norm_rms,
                            generate_stats.launch_norm_rms_add,
                            generate_stats.launch_norm_rms_add_mul_scalar,
                            generate_stats.launch_norm_rms_add_output_scale,
                            generate_stats.launch_norm_rms_bare,
                            generate_stats.launch_norm_head_rope,
                        },
                    );
                    print(
                        "cuda_generate_attention_launch_breakdown: gqa_decode={d} gqa_fast={d} gqa_fast_fallbacks={d} gqa_prefill_fast={d} gqa_prefill_tiled={d} gqa_prefill_mma={d} gqa_prefill_mma_m32={d} gqa_scalar={d}\n",
                        .{
                            generate_stats.launch_attention_gqa_decode,
                            generate_stats.launch_attention_gqa_decode_fast,
                            generate_stats.launch_attention_gqa_decode_fast_fallbacks,
                            generate_stats.launch_attention_gqa_prefill_fast,
                            generate_stats.launch_attention_gqa_prefill_tiled,
                            generate_stats.launch_attention_gqa_prefill_mma,
                            generate_stats.launch_attention_gqa_prefill_mma_m32,
                            generate_stats.launch_attention_gqa_scalar,
                        },
                    );
                    print(
                        "cuda_generate_rates: launches_per_token={d:.3} syncs_per_token={d:.3}\n",
                        .{
                            perToken(generate_stats.kernel_launches, result.tokens_used),
                            perToken(generate_stats.stream_syncs, result.tokens_used),
                        },
                    );
                    print(
                        "cuda_generate_graph_capture_counts: begins={d} replays={d} discards={d} instantiates={d} update_successes={d} update_failures={d} update_unavailable={d} scalar_updates={d} persistent_replays={d} capacity_skips={d}\n",
                        .{
                            generate_stats.cuda_graph_capture_begins,
                            generate_stats.cuda_graph_capture_replays,
                            generate_stats.cuda_graph_capture_discards,
                            generate_stats.cuda_graph_capture_instantiates,
                            generate_stats.cuda_graph_capture_update_successes,
                            generate_stats.cuda_graph_capture_update_failures,
                            generate_stats.cuda_graph_capture_update_unavailable,
                            generate_stats.cuda_graph_capture_scalar_updates,
                            generate_stats.cuda_graph_capture_persistent_replays,
                            generate_stats.cuda_graph_capture_capacity_skips,
                        },
                    );
                    print(
                        "cuda_generate_lm_head_argmax_counts: fused_q8={d} fused_q4_0={d} fused_q4={d} fused_q6={d} fallbacks={d}\n",
                        .{
                            generate_stats.lm_head_argmax_fused_q8,
                            generate_stats.lm_head_argmax_fused_q4_0,
                            generate_stats.lm_head_argmax_fused_q4,
                            generate_stats.lm_head_argmax_fused_q6,
                            generate_stats.lm_head_argmax_fallbacks,
                        },
                    );
                    print(
                        "cuda_generate_epilogue_fusion_counts: activation_multiply={d} linear_activation_slice_q4_0={d} add_mul_scalar={d} rms_norm_add={d} rms_norm_add_output_scale={d} rms_norm_add_weighted_embedding_q6_k={d} rms_norm_add_output_scale_fallbacks={d} gated_down_q8={d} gated_down_q4_0={d} gated_down_q4_0_precompute={d} gated_down_q4_0_tile4={d} gated_down_q4_0_tile8={d} gated_down_q4_0_tile16={d} gated_down_q4={d} gated_down_fallbacks={d}\n",
                        .{
                            generate_stats.activation_multiply_fused,
                            generate_stats.linear_activation_slice_fused_q4_0,
                            generate_stats.add_mul_scalar_fused,
                            generate_stats.rms_norm_add_fused,
                            generate_stats.rms_norm_add_output_scale_fused,
                            generate_stats.rms_norm_add_weighted_embedding_fused_q6_k,
                            generate_stats.rms_norm_add_output_scale_fallbacks,
                            generate_stats.gated_down_fused_q8,
                            generate_stats.gated_down_fused_q4_0,
                            generate_stats.gated_down_fused_q4_0_precompute,
                            generate_stats.gated_down_fused_q4_0_tile4,
                            generate_stats.gated_down_fused_q4_0_tile8,
                            generate_stats.gated_down_fused_q4_0_tile16,
                            generate_stats.gated_down_fused_q4,
                            generate_stats.gated_down_fallbacks,
                        },
                    );
                    print(
                        "cuda_generate_decoder_runtime_counts: linear_prepares={d} linear_prepare_misses={d} rms_prepares={d} rms_prepare_misses={d} linear_apply_hits={d} linear_apply_misses={d} linear_pair_apply_hits={d} linear_qkv_apply_hits={d} rms_apply_hits={d} rms_apply_misses={d} attention_residual_attempts={d} attention_residual_hits={d} attention_residual_misses={d} gated_ffn_attempts={d} gated_ffn_hits={d} gated_ffn_misses={d} pinned_eviction_skips={d}\n",
                        .{
                            generate_stats.decoder_runtime_linear_slot_prepares,
                            generate_stats.decoder_runtime_linear_slot_prepare_misses,
                            generate_stats.decoder_runtime_rms_norm_slot_prepares,
                            generate_stats.decoder_runtime_rms_norm_slot_prepare_misses,
                            generate_stats.decoder_runtime_linear_apply_hits,
                            generate_stats.decoder_runtime_linear_apply_misses,
                            generate_stats.decoder_runtime_linear_pair_apply_hits,
                            generate_stats.decoder_runtime_linear_qkv_apply_hits,
                            generate_stats.decoder_runtime_rms_norm_apply_hits,
                            generate_stats.decoder_runtime_rms_norm_apply_misses,
                            generate_stats.decoder_runtime_attention_residual_attempts,
                            generate_stats.decoder_runtime_attention_residual_hits,
                            generate_stats.decoder_runtime_attention_residual_misses,
                            generate_stats.decoder_runtime_gated_ffn_attempts,
                            generate_stats.decoder_runtime_gated_ffn_hits,
                            generate_stats.decoder_runtime_gated_ffn_misses,
                            generate_stats.decoder_runtime_pinned_eviction_skips,
                        },
                    );
                    print(
                        "cuda_decode_profile_us: events={d} qkv={d} gqa_attention={d} attention_output={d} attention_norm_residual={d} ffn_gate_up={d} ffn_gated_down={d} ffn_post_norm={d} lm_head_argmax={d} graph_replay={d}\n",
                        .{
                            generate_stats.decode_profile_events,
                            generate_stats.decode_profile_qkv_us,
                            generate_stats.decode_profile_gqa_attention_us,
                            generate_stats.decode_profile_attention_output_us,
                            generate_stats.decode_profile_attention_norm_residual_us,
                            generate_stats.decode_profile_ffn_gate_up_us,
                            generate_stats.decode_profile_ffn_gated_down_us,
                            generate_stats.decode_profile_ffn_post_norm_us,
                            generate_stats.decode_profile_lm_head_argmax_us,
                            generate_stats.decode_profile_graph_replay_us,
                        },
                    );
                    print(
                        "cuda_prefill_profile_us: events={d} q4_linear={d} q4_qkv={d} q4_pair={d} q4_gated_down={d} bf16_linear={d} bf16_qkv={d} bf16_pair={d} attention={d} ple_dense={d} staging={d} norm={d}\n",
                        .{
                            generate_stats.prefill_profile_events,
                            generate_stats.prefill_profile_q4_linear_us,
                            generate_stats.prefill_profile_q4_qkv_us,
                            generate_stats.prefill_profile_q4_pair_us,
                            generate_stats.prefill_profile_q4_gated_down_us,
                            generate_stats.prefill_profile_bf16_linear_us,
                            generate_stats.prefill_profile_bf16_qkv_us,
                            generate_stats.prefill_profile_bf16_pair_us,
                            generate_stats.prefill_profile_attention_us,
                            generate_stats.prefill_profile_ple_dense_us,
                            generate_stats.prefill_profile_staging_us,
                            generate_stats.prefill_profile_norm_us,
                        },
                    );
                }
                print(
                    "cuda_eval_breakdown: requests={d} skipped_eager={d} forced_syncs={d}\n",
                    .{
                        cuda_stats.eval_requests,
                        cuda_stats.eval_skipped_eager,
                        cuda_stats.eval_forced_syncs,
                    },
                );
                print(
                    "cuda_runtime_bytes: h2d={d} d2h={d} d2d={d} resident_weights={d} device_allocated={d}\n",
                    .{
                        cuda_stats.h2d_bytes,
                        cuda_stats.d2h_bytes,
                        cuda_stats.d2d_bytes,
                        cuda_stats.resident_weight_bytes,
                        cuda_stats.device_allocated_bytes,
                    },
                );
                print(
                    "cuda_cross_backend_copy: copies={d} bytes={d} event_records={d} event_waits={d} sync_fallbacks={d}\n",
                    .{
                        cuda_stats.cross_backend_copies,
                        cuda_stats.cross_backend_copy_bytes,
                        cuda_stats.cross_backend_event_records,
                        cuda_stats.cross_backend_event_waits,
                        cuda_stats.cross_backend_sync_fallbacks,
                    },
                );
                print(
                    "cuda_lazy_profile: prefetch_enqueues={d} prefetch_duplicates={d} prefetch_missing={d} prefetch_cancelled_for_demand={d} drain_calls={d} demand_loads={d} host_prefetch_hits={d} host_load_ms={d} page_touch_ms={d} upload_ms={d} uploaded_mb={d}\n",
                    .{
                        cuda_stats.lazy_prefetch_enqueues,
                        cuda_stats.lazy_prefetch_duplicates,
                        cuda_stats.lazy_prefetch_missing,
                        cuda_stats.lazy_prefetch_cancelled_for_demand,
                        cuda_stats.lazy_prefetch_drain_calls,
                        cuda_stats.lazy_demand_loads,
                        cuda_stats.lazy_host_prefetch_hits,
                        cuda_stats.lazy_host_load_ns / 1_000_000,
                        cuda_stats.lazy_host_page_touch_ns / 1_000_000,
                        cuda_stats.lazy_upload_ns / 1_000_000,
                        cuda_stats.lazy_uploaded_bytes / (1024 * 1024),
                    },
                );
                print(
                    "cuda_ffn_stream_profile: requests={d} hits={d} misses={d} fallbacks={d} evictions={d} read_ms={d} h2d_ms={d} read_mb={d} uploaded_mb={d} resident_mb={d} fadvise_calls={d}\n",
                    .{
                        cuda_stats.ffn_stream_requests,
                        cuda_stats.ffn_stream_hits,
                        cuda_stats.ffn_stream_misses,
                        cuda_stats.ffn_stream_fallbacks,
                        cuda_stats.ffn_stream_evictions,
                        cuda_stats.ffn_stream_read_ns / 1_000_000,
                        cuda_stats.ffn_stream_h2d_ns / 1_000_000,
                        cuda_stats.ffn_stream_read_bytes / (1024 * 1024),
                        cuda_stats.ffn_stream_uploaded_bytes / (1024 * 1024),
                        cuda_stats.ffn_stream_resident_bytes / (1024 * 1024),
                        cuda_stats.ffn_stream_fadvise_calls,
                    },
                );
                print(
                    "cuda_dense_stream_profile: requests={d} hits={d} misses={d} fallbacks={d} evictions={d} read_ms={d} h2d_ms={d} read_mb={d} uploaded_mb={d} resident_mb={d} fadvise_calls={d} attention_loads={d} mlp_loads={d}\n",
                    .{
                        cuda_stats.dense_stream_requests,
                        cuda_stats.dense_stream_hits,
                        cuda_stats.dense_stream_misses,
                        cuda_stats.dense_stream_fallbacks,
                        cuda_stats.dense_stream_evictions,
                        cuda_stats.dense_stream_read_ns / 1_000_000,
                        cuda_stats.dense_stream_h2d_ns / 1_000_000,
                        cuda_stats.dense_stream_read_bytes / (1024 * 1024),
                        cuda_stats.dense_stream_uploaded_bytes / (1024 * 1024),
                        cuda_stats.dense_stream_resident_bytes / (1024 * 1024),
                        cuda_stats.dense_stream_fadvise_calls,
                        cuda_stats.dense_stream_attention_loads,
                        cuda_stats.dense_stream_mlp_loads,
                    },
                );
                print(
                    "cuda_dense_prefetch_profile: enqueues={d} duplicates={d} ready_hits={d} inflight_steals={d} sync_reads={d} evictions={d} failures={d} host_read_ms={d} demand_wait_ms={d} upload_ms={d} resident_mb={d} read_mb={d}\n",
                    .{
                        cuda_stats.dense_prefetch_enqueues,
                        cuda_stats.dense_prefetch_duplicates,
                        cuda_stats.dense_prefetch_ready_hits,
                        cuda_stats.dense_prefetch_inflight_steals,
                        cuda_stats.dense_prefetch_sync_reads,
                        cuda_stats.dense_prefetch_evictions,
                        cuda_stats.dense_prefetch_failures,
                        cuda_stats.dense_prefetch_host_read_ns / 1_000_000,
                        cuda_stats.dense_prefetch_demand_wait_ns / 1_000_000,
                        cuda_stats.dense_prefetch_upload_ns / 1_000_000,
                        cuda_stats.dense_prefetch_resident_bytes / (1024 * 1024),
                        cuda_stats.dense_prefetch_read_bytes / (1024 * 1024),
                    },
                );
                const attributed_launches =
                    cuda_stats.launch_embedding +
                    cuda_stats.launch_linear +
                    cuda_stats.launch_linear_qkv +
                    cuda_stats.launch_norm +
                    cuda_stats.launch_rope +
                    cuda_stats.launch_attention +
                    cuda_stats.launch_elementwise +
                    cuda_stats.launch_scalar +
                    cuda_stats.launch_argmax +
                    cuda_stats.launch_other;
                const unattributed_launches = if (cuda_stats.kernel_launches >= attributed_launches)
                    cuda_stats.kernel_launches - attributed_launches
                else
                    0;
                print(
                    "cuda_launch_breakdown: embedding={d} linear={d} qkv={d} norm={d} rope={d} attention={d} elementwise={d} scalar={d} argmax={d} other={d} unattributed={d}\n",
                    .{
                        cuda_stats.launch_embedding,
                        cuda_stats.launch_linear,
                        cuda_stats.launch_linear_qkv,
                        cuda_stats.launch_norm,
                        cuda_stats.launch_rope,
                        cuda_stats.launch_attention,
                        cuda_stats.launch_elementwise,
                        cuda_stats.launch_scalar,
                        cuda_stats.launch_argmax,
                        cuda_stats.launch_other,
                        unattributed_launches,
                    },
                );
                print(
                    "cuda_norm_launch_breakdown: layer={d} add_layer={d} rms={d} rms_add={d} rms_add_mul_scalar={d} rms_add_output_scale={d} rms_bare={d} head_rope={d}\n",
                    .{
                        cuda_stats.launch_norm_layer,
                        cuda_stats.launch_norm_add_layer,
                        cuda_stats.launch_norm_rms,
                        cuda_stats.launch_norm_rms_add,
                        cuda_stats.launch_norm_rms_add_mul_scalar,
                        cuda_stats.launch_norm_rms_add_output_scale,
                        cuda_stats.launch_norm_rms_bare,
                        cuda_stats.launch_norm_head_rope,
                    },
                );
                print(
                    "cuda_attention_launch_breakdown: gqa_decode={d} gqa_fast={d} gqa_fast_fallbacks={d} gqa_prefill_fast={d} gqa_prefill_tiled={d} gqa_prefill_mma={d} gqa_prefill_mma_m32={d} gqa_scalar={d}\n",
                    .{
                        cuda_stats.launch_attention_gqa_decode,
                        cuda_stats.launch_attention_gqa_decode_fast,
                        cuda_stats.launch_attention_gqa_decode_fast_fallbacks,
                        cuda_stats.launch_attention_gqa_prefill_fast,
                        cuda_stats.launch_attention_gqa_prefill_tiled,
                        cuda_stats.launch_attention_gqa_prefill_mma,
                        cuda_stats.launch_attention_gqa_prefill_mma_m32,
                        cuda_stats.launch_attention_gqa_scalar,
                    },
                );
                print(
                    "cuda_scalar_launch_breakdown: multiply_immediate={d} add_immediate={d} device_broadcast={d}\n",
                    .{
                        cuda_stats.launch_scalar_multiply_immediate,
                        cuda_stats.launch_scalar_add_immediate,
                        cuda_stats.launch_scalar_device_broadcast,
                    },
                );
                print(
                    "cuda_transfer_breakdown: from_f32_calls={d} from_f32_bytes={d} to_f32_calls={d} to_f32_bytes={d} upload_owned_calls={d} upload_owned_bytes={d} download_alloc_calls={d} download_alloc_bytes={d}\n",
                    .{
                        cuda_stats.from_float32_calls,
                        cuda_stats.from_float32_bytes,
                        cuda_stats.to_float32_calls,
                        cuda_stats.to_float32_bytes,
                        cuda_stats.upload_owned_host_calls,
                        cuda_stats.upload_owned_host_bytes,
                        cuda_stats.download_alloc_calls,
                        cuda_stats.download_alloc_bytes,
                    },
                );
                print(
                    "cuda_pinned_scalar_transfers: uploads={d} upload_bytes={d} upload_fallbacks={d} upload_wrap_syncs={d} downloads={d} download_bytes={d} download_fallbacks={d}\n",
                    .{
                        cuda_stats.pinned_scalar_uploads,
                        cuda_stats.pinned_scalar_upload_bytes,
                        cuda_stats.pinned_scalar_upload_fallbacks,
                        cuda_stats.pinned_scalar_upload_wrap_syncs,
                        cuda_stats.pinned_scalar_downloads,
                        cuda_stats.pinned_scalar_download_bytes,
                        cuda_stats.pinned_scalar_download_fallbacks,
                    },
                );
                print(
                    "cuda_transfer_buckets: upload_le16={d} upload_le1k={d} upload_le8k={d} upload_le32k={d} upload_le256k={d} upload_gt256k={d} download_le16={d} download_le1k={d} download_le8k={d} download_le32k={d} download_le256k={d} download_gt256k={d}\n",
                    .{
                        cuda_stats.upload_bucket_le_16,
                        cuda_stats.upload_bucket_le_1k,
                        cuda_stats.upload_bucket_le_8k,
                        cuda_stats.upload_bucket_le_32k,
                        cuda_stats.upload_bucket_le_256k,
                        cuda_stats.upload_bucket_gt_256k,
                        cuda_stats.download_bucket_le_16,
                        cuda_stats.download_bucket_le_1k,
                        cuda_stats.download_bucket_le_8k,
                        cuda_stats.download_bucket_le_32k,
                        cuda_stats.download_bucket_le_256k,
                        cuda_stats.download_bucket_gt_256k,
                    },
                );
                print(
                    "cuda_device_op_counts: add_scalar={d} rms_norm_bare={d}\n",
                    .{
                        cuda_stats.add_scalar_calls,
                        cuda_stats.rms_norm_bare_calls,
                    },
                );
                print("cuda_upload_top_sizes:", .{});
                for (cuda_stats.upload_top_sizes, cuda_stats.upload_top_counts) |size, count| {
                    if (size != 0 and count != 0) print(" {d}x{d}", .{ count, size });
                }
                print("\n", .{});
                print("cuda_download_top_sizes:", .{});
                for (cuda_stats.download_top_sizes, cuda_stats.download_top_counts) |size, count| {
                    if (size != 0 and count != 0) print(" {d}x{d}", .{ count, size });
                }
                print("\n", .{});
                print(
                    "cuda_fallback_counts: host_attention={d} rope={d} rope_per_item={d} gqa_dense={d} paged_attention={d}\n",
                    .{
                        cuda_stats.host_attention_fallbacks,
                        cuda_stats.rope_host_fallbacks,
                        cuda_stats.rope_per_item_host_fallbacks,
                        cuda_stats.gqa_dense_host_fallbacks,
                        cuda_stats.paged_attention_host_fallbacks,
                    },
                );
                print(
                    "cuda_qkv_counts: fused_q8={d} fused_q4_0={d} fused_q4_0_tile4={d} fused_q4_0_tile8={d} fused_q4={d} fused_q4_q4_f32={d} fused_f32={d} fallback_unsupported={d} kernel_unavailable={d}\n",
                    .{
                        cuda_stats.qkv_fused_q8,
                        cuda_stats.qkv_fused_q4_0,
                        cuda_stats.qkv_fused_q4_0_tile4,
                        cuda_stats.qkv_fused_q4_0_tile8,
                        cuda_stats.qkv_fused_q4,
                        cuda_stats.qkv_fused_q4_q4_f32,
                        cuda_stats.qkv_fused_f32,
                        cuda_stats.qkv_fallback_unsupported,
                        cuda_stats.qkv_kernel_unavailable,
                    },
                );
                print(
                    "cuda_q4_0_q8_1_prefill_counts: linear={d} linear_rows2={d} linear_rows4={d} linear_rows8_c4={d} linear_e4b_down_rows={d} linear_generic_rows={d} linear_tile8_rows={d} qkv={d} qkv_rows4={d} qkv_tile8_rows={d} qkv_tile8_w8_rows={d} pair={d} pair_rows2={d} pair_rows4={d} pair_rows8_c2={d} pair_rows16_c1={d} pair_generic_rows={d} pair_tile8_rows={d} gated_down={d} gated_down_rows2={d} gated_down_rows4={d} gated_down_rows8_c4={d} gated_down_e4b_down_rows={d} gated_down_generic_rows={d} gated_down_tile8_rows={d}\n",
                    .{
                        cuda_stats.q4_0_q8_1_prefill_linear_hits,
                        cuda_stats.q4_0_q8_1_prefill_linear_rows2_hits,
                        cuda_stats.q4_0_q8_1_prefill_linear_rows4_hits,
                        cuda_stats.q4_0_q8_1_prefill_linear_rows8_c4_hits,
                        cuda_stats.q4_0_q8_1_prefill_linear_e4b_down_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_linear_generic_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_linear_tile8_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_qkv_hits,
                        cuda_stats.q4_0_q8_1_prefill_qkv_rows4_hits,
                        cuda_stats.q4_0_q8_1_prefill_qkv_tile8_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_qkv_tile8_w8_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_rows2_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_rows4_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_rows8_c2_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_rows16_c1_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_generic_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_pair_tile8_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_rows2_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_rows4_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_rows8_c4_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_e4b_down_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_generic_rows_hits,
                        cuda_stats.q4_0_q8_1_prefill_gated_down_tile8_rows_hits,
                    },
                );
                print(
                    "cuda_linear_pair_counts: fused_q8={d} fused_q4_0={d} fused_q4_0_activation={d} fused_q4_0_tile4={d} fused_q4_0_tile8={d} fused_q4={d} fallbacks={d}\n",
                    .{
                        cuda_stats.linear_pair_fused_q8,
                        cuda_stats.linear_pair_fused_q4_0,
                        cuda_stats.linear_pair_fused_q4_0_activation,
                        cuda_stats.linear_pair_fused_q4_0_tile4,
                        cuda_stats.linear_pair_fused_q4_0_tile8,
                        cuda_stats.linear_pair_fused_q4,
                        cuda_stats.linear_pair_fallbacks,
                    },
                );
                print(
                    "cuda_lm_head_argmax_counts: fused_q8={d} fused_q4_0={d} fused_q4={d} fused_q6={d} fallbacks={d}\n",
                    .{
                        cuda_stats.lm_head_argmax_fused_q8,
                        cuda_stats.lm_head_argmax_fused_q4_0,
                        cuda_stats.lm_head_argmax_fused_q4,
                        cuda_stats.lm_head_argmax_fused_q6,
                        cuda_stats.lm_head_argmax_fallbacks,
                    },
                );
                print(
                    "cuda_epilogue_fusion_counts: activation_multiply={d} linear_activation_slice_q4_0={d} add_mul_scalar={d} rms_norm_add={d} rms_norm_add_output_scale={d} rms_norm_add_weighted_embedding_q6_k={d} rms_norm_add_output_scale_fallbacks={d} gated_down_q8={d} gated_down_q4_0={d} gated_down_q4_0_precompute={d} gated_down_q4_0_tile4={d} gated_down_q4_0_tile8={d} gated_down_q4_0_tile16={d} gated_down_q4={d} gated_down_fallbacks={d}\n",
                    .{
                        cuda_stats.activation_multiply_fused,
                        cuda_stats.linear_activation_slice_fused_q4_0,
                        cuda_stats.add_mul_scalar_fused,
                        cuda_stats.rms_norm_add_fused,
                        cuda_stats.rms_norm_add_output_scale_fused,
                        cuda_stats.rms_norm_add_weighted_embedding_fused_q6_k,
                        cuda_stats.rms_norm_add_output_scale_fallbacks,
                        cuda_stats.gated_down_fused_q8,
                        cuda_stats.gated_down_fused_q4_0,
                        cuda_stats.gated_down_fused_q4_0_precompute,
                        cuda_stats.gated_down_fused_q4_0_tile4,
                        cuda_stats.gated_down_fused_q4_0_tile8,
                        cuda_stats.gated_down_fused_q4_0_tile16,
                        cuda_stats.gated_down_fused_q4,
                        cuda_stats.gated_down_fallbacks,
                    },
                );
                print(
                    "cuda_decoder_runtime_counts: linear_prepares={d} linear_prepare_misses={d} rms_prepares={d} rms_prepare_misses={d} linear_apply_hits={d} linear_apply_misses={d} linear_pair_apply_hits={d} linear_qkv_apply_hits={d} rms_apply_hits={d} rms_apply_misses={d} attention_residual_attempts={d} attention_residual_hits={d} attention_residual_misses={d} gated_ffn_attempts={d} gated_ffn_hits={d} gated_ffn_misses={d} pinned_eviction_skips={d}\n",
                    .{
                        cuda_stats.decoder_runtime_linear_slot_prepares,
                        cuda_stats.decoder_runtime_linear_slot_prepare_misses,
                        cuda_stats.decoder_runtime_rms_norm_slot_prepares,
                        cuda_stats.decoder_runtime_rms_norm_slot_prepare_misses,
                        cuda_stats.decoder_runtime_linear_apply_hits,
                        cuda_stats.decoder_runtime_linear_apply_misses,
                        cuda_stats.decoder_runtime_linear_pair_apply_hits,
                        cuda_stats.decoder_runtime_linear_qkv_apply_hits,
                        cuda_stats.decoder_runtime_rms_norm_apply_hits,
                        cuda_stats.decoder_runtime_rms_norm_apply_misses,
                        cuda_stats.decoder_runtime_attention_residual_attempts,
                        cuda_stats.decoder_runtime_attention_residual_hits,
                        cuda_stats.decoder_runtime_attention_residual_misses,
                        cuda_stats.decoder_runtime_gated_ffn_attempts,
                        cuda_stats.decoder_runtime_gated_ffn_hits,
                        cuda_stats.decoder_runtime_gated_ffn_misses,
                        cuda_stats.decoder_runtime_pinned_eviction_skips,
                    },
                );
                print(
                    "cuda_bf16_counts: cublaslt_linear={d} cublaslt_qkv={d} cublaslt_activation_staging={d} cublaslt_activation_mirror={d} cublaslt_fallbacks={d} scalar_linear={d} scalar_qkv={d} rms_norm_bf16_mirror={d}\n",
                    .{
                        cuda_stats.bf16_cublaslt_linear_calls,
                        cuda_stats.bf16_cublaslt_qkv_calls,
                        cuda_stats.bf16_cublaslt_activation_staging_calls,
                        cuda_stats.bf16_cublaslt_activation_mirror_hits,
                        cuda_stats.bf16_cublaslt_fallbacks,
                        cuda_stats.bf16_scalar_linear_calls,
                        cuda_stats.bf16_scalar_qkv_calls,
                        cuda_stats.rms_norm_bf16_mirror_hits,
                    },
                );
                print(
                    "cuda_q4k_fast_counts: decode_hits={d} decode_fallbacks={d}\n",
                    .{
                        cuda_stats.q4k_decode_fast_hits,
                        cuda_stats.q4k_decode_fast_fallbacks,
                    },
                );
                print(
                    "cuda_head_norm_rope_counts: fused_hits={d} fused_fallbacks={d}\n",
                    .{
                        cuda_stats.head_norm_rope_fused_hits,
                        cuda_stats.head_norm_rope_fused_fallbacks,
                    },
                );
                print(
                    "cuda_mtp_counts: preproject_fused_hits={d} preproject_fused_f32_weight_hits={d} preproject_fused_bf16_weight_hits={d} preproject_fused_f16_weight_hits={d} preproject_fused_fallbacks={d} masked_select_fused_hits={d} masked_select_fused_f32_weight_hits={d} masked_select_fused_bf16_weight_hits={d} masked_select_fused_f16_weight_hits={d} masked_select_fused_fallbacks={d} masked_select_hidden_fused_hits={d} masked_select_hidden_fused_bf16_hits={d} masked_select_hidden_multiblock_hits={d} masked_select_hidden_fused_fallbacks={d} masked_argmax_hits={d} masked_argmax_fallbacks={d} verify_device_hits={d} verify_device_fallbacks={d} verify_result_downloads={d} verify_choice_downloads={d}\n",
                    .{
                        cuda_stats.mtp_preproject_fused_hits,
                        cuda_stats.mtp_preproject_fused_f32_weight_hits,
                        cuda_stats.mtp_preproject_fused_bf16_weight_hits,
                        cuda_stats.mtp_preproject_fused_f16_weight_hits,
                        cuda_stats.mtp_preproject_fused_fallbacks,
                        cuda_stats.mtp_masked_select_fused_hits,
                        cuda_stats.mtp_masked_select_fused_f32_weight_hits,
                        cuda_stats.mtp_masked_select_fused_bf16_weight_hits,
                        cuda_stats.mtp_masked_select_fused_f16_weight_hits,
                        cuda_stats.mtp_masked_select_fused_fallbacks,
                        cuda_stats.mtp_masked_select_hidden_fused_hits,
                        cuda_stats.mtp_masked_select_hidden_fused_bf16_hits,
                        cuda_stats.mtp_masked_select_hidden_multiblock_hits,
                        cuda_stats.mtp_masked_select_hidden_fused_fallbacks,
                        cuda_stats.mtp_masked_argmax_hits,
                        cuda_stats.mtp_masked_argmax_fallbacks,
                        cuda_stats.mtp_verify_commit_device_hits,
                        cuda_stats.mtp_verify_commit_device_fallbacks,
                        cuda_stats.mtp_verify_commit_result_downloads,
                        cuda_stats.mtp_verify_commit_choice_downloads,
                    },
                );
                print(
                    "cuda_device_kv_counts: attempts={d} successes={d} writes={d} reads={d} compressed_v_writes={d} compressed_v_reads={d} compressed_v_bytes={d} block_table_uploads={d} block_table_bytes={d} identity_attention_reads={d} fail_batch={d} fail_no_cache={d} fail_no_storage={d} fail_no_hook={d} fail_write={d} fail_read={d} fail_shape={d}\n",
                    .{
                        cuda_stats.device_kv_attempts,
                        cuda_stats.device_kv_successes,
                        cuda_stats.device_kv_writes,
                        cuda_stats.device_kv_reads,
                        cuda_stats.device_kv_compressed_v_writes,
                        cuda_stats.device_kv_compressed_v_reads,
                        cuda_stats.device_kv_compressed_v_bytes,
                        cuda_stats.device_kv_paged_block_table_uploads,
                        cuda_stats.device_kv_paged_block_table_bytes,
                        cuda_stats.device_kv_paged_identity_attention_reads,
                        cuda_stats.device_kv_fail_batch,
                        cuda_stats.device_kv_fail_no_cache,
                        cuda_stats.device_kv_fail_no_storage,
                        cuda_stats.device_kv_fail_no_hook,
                        cuda_stats.device_kv_fail_write,
                        cuda_stats.device_kv_fail_read,
                        cuda_stats.device_kv_fail_shape,
                    },
                );
            }
            if (draft_model) |loaded_draft| {
                if (session_factory.getCudaRuntimeStats(loaded_draft.session)) |draft_cuda_stats| {
                    print(
                        "draft_cuda_counts: launches={d} syncs={d} upload_syncs={d} download_syncs={d} linear={d} argmax={d} h2d={d} d2h={d} cross_backend_copies={d} cross_backend_bytes={d} cross_backend_event_records={d} cross_backend_event_waits={d} cross_backend_sync_fallbacks={d} mtp_preproject_fused_hits={d} mtp_preproject_fused_fallbacks={d} mtp_masked_select_fused_hits={d} mtp_masked_select_fused_fallbacks={d} mtp_masked_select_hidden_fused_hits={d} mtp_masked_select_hidden_multiblock_hits={d} mtp_masked_select_hidden_fused_fallbacks={d} mtp_masked_argmax_hits={d} mtp_masked_argmax_fallbacks={d} device_kv_attempts={d} device_kv_successes={d} device_kv_reads={d} device_kv_writes={d} device_kv_fail_read={d} device_kv_fail_shape={d}\n",
                        .{
                            draft_cuda_stats.kernel_launches,
                            draft_cuda_stats.stream_syncs,
                            draft_cuda_stats.upload_syncs,
                            draft_cuda_stats.download_syncs,
                            draft_cuda_stats.launch_linear,
                            draft_cuda_stats.launch_argmax,
                            draft_cuda_stats.h2d_bytes,
                            draft_cuda_stats.d2h_bytes,
                            draft_cuda_stats.cross_backend_copies,
                            draft_cuda_stats.cross_backend_copy_bytes,
                            draft_cuda_stats.cross_backend_event_records,
                            draft_cuda_stats.cross_backend_event_waits,
                            draft_cuda_stats.cross_backend_sync_fallbacks,
                            draft_cuda_stats.mtp_preproject_fused_hits,
                            draft_cuda_stats.mtp_preproject_fused_fallbacks,
                            draft_cuda_stats.mtp_masked_select_fused_hits,
                            draft_cuda_stats.mtp_masked_select_fused_fallbacks,
                            draft_cuda_stats.mtp_masked_select_hidden_fused_hits,
                            draft_cuda_stats.mtp_masked_select_hidden_multiblock_hits,
                            draft_cuda_stats.mtp_masked_select_hidden_fused_fallbacks,
                            draft_cuda_stats.mtp_masked_argmax_hits,
                            draft_cuda_stats.mtp_masked_argmax_fallbacks,
                            draft_cuda_stats.device_kv_attempts,
                            draft_cuda_stats.device_kv_successes,
                            draft_cuda_stats.device_kv_reads,
                            draft_cuda_stats.device_kv_writes,
                            draft_cuda_stats.device_kv_fail_read,
                            draft_cuda_stats.device_kv_fail_shape,
                        },
                    );
                }
            }
        }
    }
    if (opts.json_timing_path) |path| {
        try writeJsonTiming(
            allocator,
            io,
            path,
            opts.model_dir,
            @tagName(model.session.backend()),
            &result,
            durationMillis(started_at, loaded_model_at),
            durationMillis(loaded_model_at, encoded_prompt_at),
            durationMillis(encoded_prompt_at, acquired_scheduler_at),
            durationMillis(acquired_scheduler_at, created_backend_at),
            durationMillis(created_backend_at, created_decode_state_at),
            durationMillis(created_decode_state_at, finished_generate_at),
            durationMillis(started_at, finished_generate_at),
            cuda_stats_after_generate,
            cuda_generate_stats,
            draft_cuda_stats_after_generate,
            draft_cuda_generate_stats,
        );
    }
}

fn warmInitCudaBeforeLargeModelScan(backend: BackendChoice) !void {
    if (backend != .cuda) return;
    if (!build_options.enable_cuda) return;
    var ctx = cuda_context.CudaContext.initDefault() catch |err| {
        std.log.err("CUDA warm init before model load failed: {s}", .{@errorName(err)});
        return;
    };
    ctx.deinit();
}

fn nanosToMillis(nanos: i128) u64 {
    return @intCast(@divTrunc(nanos, std.time.ns_per_ms));
}

fn durationMillis(from: std.Io.Timestamp, to: std.Io.Timestamp) u64 {
    return nanosToMillis(std.Io.Timestamp.durationTo(from, to).nanoseconds);
}

fn tokensPerSecond(tokens: usize, millis: u64) f64 {
    if (tokens == 0 or millis == 0) return 0.0;
    return @as(f64, @floatFromInt(tokens)) * 1000.0 / @as(f64, @floatFromInt(millis));
}

const RawDecodeBenchResult = struct {
    tokens: usize,
    warmup_tokens: usize,
    prompt_tokens: usize,
    scope: []const u8,
    device_warmup_ms: u64,
    prefill_ms: u64,
    warmup_ms: u64,
    decode_ms: u64,
};

fn runRawDecodeBench(
    allocator: std.mem.Allocator,
    io: std.Io,
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    prompt_ids_i32: []const i32,
    tokens: usize,
    prefill_chunk_size: usize,
    decode_state: *generation.NativeDecodeState,
) !RawDecodeBenchResult {
    const prompt_ids = try allocator.alloc(i64, prompt_ids_i32.len);
    defer allocator.free(prompt_ids);
    for (prompt_ids_i32, 0..) |id, idx| prompt_ids[idx] = id;

    var last_hidden: ?ops.CT = null;
    defer if (last_hidden) |hidden| cb.free(hidden);
    var last_token_tensor: ?ops.CT = null;
    defer if (last_token_tensor) |token| cb.free(token);
    const include_greedy_token = rawDecodeBenchGreedyTokenTensorEnabled();
    const scope = if (include_greedy_token)
        "greedy_token_tensor_no_host_resolution"
    else
        "hidden_decode_no_lm_head_sampler";

    const prefill_started_at = std.Io.Timestamp.now(io, .awake);
    var offset: usize = 0;
    const chunk_size = if (prefill_chunk_size > 0) prefill_chunk_size else prompt_ids.len;
    while (offset < prompt_ids.len) {
        const remaining = prompt_ids.len - offset;
        const query_len = @min(remaining, chunk_size);
        try decode_state.appendPrefillChunk(query_len);
        const seq_len = decode_state.total_tokens;
        var decode_context = decode_state.gptDecodeContext(seq_len, query_len);
        if (last_hidden) |hidden| {
            cb.free(hidden);
            last_hidden = null;
        }
        last_hidden = try gpt_arch.forwardHiddenTensorWithCudaReplay(
            cb,
            allocator,
            gpt_config,
            prompt_ids[offset..][0..query_len],
            1,
            seq_len,
            &decode_context,
            "gpt.raw_hidden_decode",
        );
        offset += query_len;
    }
    const finished_prefill_at = std.Io.Timestamp.now(io, .awake);

    var token_buf: [1]i64 = undefined;
    var token_buf_i32: [1]i32 = undefined;
    const token_shape = [_]i32{1};
    const warmup_tokens = rawDecodeBenchWarmupTokens();
    const total_decode_steps = try std.math.add(usize, warmup_tokens, tokens);
    var resident_token_tensors = std.ArrayListUnmanaged(ops.CT).empty;
    defer {
        for (resident_token_tensors.items) |token_tensor| cb.free(token_tensor);
        resident_token_tensors.deinit(allocator);
    }
    if (rawDecodeBenchResidentTokenInputsEnabled()) {
        var idx: usize = 0;
        while (idx < total_decode_steps) : (idx += 1) {
            const token_id = rawDecodeBenchToken(idx, gpt_config.vocab_size);
            const token_id_i32 = std.math.cast(i32, token_id) orelse return error.InvalidTokenId;
            token_buf_i32[0] = token_id_i32;
            const token_tensor = (try cb.fromInt32Shape(token_buf_i32[0..], &token_shape)) orelse break;
            try resident_token_tensors.append(allocator, token_tensor);
        }
        if (resident_token_tensors.items.len != total_decode_steps) {
            for (resident_token_tensors.items) |token_tensor| cb.free(token_tensor);
            resident_token_tensors.clearRetainingCapacity();
        }
    }
    const use_resident_token_tensors = resident_token_tensors.items.len == total_decode_steps;
    const device_warmup_ms = try runRawDecodeDeviceWarmup(allocator, io, cb);

    const warmup_started_at = std.Io.Timestamp.now(io, .awake);
    for (0..warmup_tokens) |idx| {
        try decode_state.appendGeneratedToken();
        const seq_len = decode_state.total_tokens;
        var decode_context = decode_state.gptDecodeContext(seq_len, 1);
        const need_hidden = idx + 1 == warmup_tokens;
        token_buf[0] = rawDecodeBenchToken(idx, gpt_config.vocab_size);
        if (last_hidden) |hidden| {
            cb.free(hidden);
            last_hidden = null;
        }
        if (last_token_tensor) |token| {
            cb.free(token);
            last_token_tensor = null;
        }
        token_buf_i32[0] = std.math.cast(i32, token_buf[0]) orelse return error.InvalidTokenId;
        const token_tensor_opt: ?ops.CT = if (use_resident_token_tensors)
            resident_token_tensors.items[idx]
        else
            try cb.fromInt32Shape(token_buf_i32[0..], &token_shape);
        if (token_tensor_opt) |token_tensor| {
            defer if (!use_resident_token_tensors) cb.free(token_tensor);
            if (include_greedy_token) {
                last_token_tensor = (try gpt_arch.forwardGreedyLastTokenTensorOnlyFromTokenTensor(
                    cb,
                    allocator,
                    gpt_config,
                    token_tensor,
                    1,
                    seq_len,
                    &decode_context,
                )) orelse return error.RawDecodeGreedyTokenTensorUnavailable;
                continue;
            }
            if (!need_hidden and try gpt_arch.forwardHiddenOnlyFromTokenTensorWithCudaReplayDiscard(
                cb,
                allocator,
                gpt_config,
                token_tensor,
                1,
                seq_len,
                &decode_context,
                "gpt.raw_hidden_decode",
            )) {
                continue;
            }
            if (try gpt_arch.forwardHiddenTensorFromTokenTensorWithCudaReplay(
                cb,
                allocator,
                gpt_config,
                token_tensor,
                1,
                seq_len,
                &decode_context,
                "gpt.raw_hidden_decode",
            )) |hidden| {
                last_hidden = hidden;
                continue;
            }
        }
        last_hidden = try gpt_arch.forwardHiddenTensorWithCudaReplay(cb, allocator, gpt_config, token_buf[0..], 1, seq_len, &decode_context, "gpt.raw_hidden_decode");
    }
    if (last_hidden) |hidden| try cb.evalTensor(hidden);
    if (last_token_tensor) |token| try cb.evalTensor(token);
    const finished_warmup_at = std.Io.Timestamp.now(io, .awake);

    const decode_started_at = std.Io.Timestamp.now(io, .awake);
    for (0..tokens) |idx| {
        const token_index = warmup_tokens + idx;
        try decode_state.appendGeneratedToken();
        const seq_len = decode_state.total_tokens;
        var decode_context = decode_state.gptDecodeContext(seq_len, 1);
        const need_hidden = idx + 1 == tokens;
        token_buf[0] = rawDecodeBenchToken(token_index, gpt_config.vocab_size);
        if (last_hidden) |hidden| {
            cb.free(hidden);
            last_hidden = null;
        }
        if (last_token_tensor) |token| {
            cb.free(token);
            last_token_tensor = null;
        }
        token_buf_i32[0] = std.math.cast(i32, token_buf[0]) orelse return error.InvalidTokenId;
        const token_tensor_opt: ?ops.CT = if (use_resident_token_tensors)
            resident_token_tensors.items[token_index]
        else
            try cb.fromInt32Shape(token_buf_i32[0..], &token_shape);
        if (token_tensor_opt) |token_tensor| {
            defer if (!use_resident_token_tensors) cb.free(token_tensor);
            if (include_greedy_token) {
                last_token_tensor = (try gpt_arch.forwardGreedyLastTokenTensorOnlyFromTokenTensor(
                    cb,
                    allocator,
                    gpt_config,
                    token_tensor,
                    1,
                    seq_len,
                    &decode_context,
                )) orelse return error.RawDecodeGreedyTokenTensorUnavailable;
                continue;
            }
            if (!need_hidden and try gpt_arch.forwardHiddenOnlyFromTokenTensorWithCudaReplayDiscard(
                cb,
                allocator,
                gpt_config,
                token_tensor,
                1,
                seq_len,
                &decode_context,
                "gpt.raw_hidden_decode",
            )) {
                continue;
            }
            if (try gpt_arch.forwardHiddenTensorFromTokenTensorWithCudaReplay(
                cb,
                allocator,
                gpt_config,
                token_tensor,
                1,
                seq_len,
                &decode_context,
                "gpt.raw_hidden_decode",
            )) |hidden| {
                last_hidden = hidden;
                continue;
            }
        }
        last_hidden = try gpt_arch.forwardHiddenTensorWithCudaReplay(cb, allocator, gpt_config, token_buf[0..], 1, seq_len, &decode_context, "gpt.raw_hidden_decode");
    }
    if (last_hidden) |hidden| try cb.evalTensor(hidden);
    if (last_token_tensor) |token| try cb.evalTensor(token);
    const finished_decode_at = std.Io.Timestamp.now(io, .awake);

    return .{
        .tokens = tokens,
        .warmup_tokens = warmup_tokens,
        .prompt_tokens = prompt_ids.len,
        .scope = scope,
        .device_warmup_ms = device_warmup_ms,
        .prefill_ms = durationMillis(prefill_started_at, finished_prefill_at),
        .warmup_ms = durationMillis(warmup_started_at, finished_warmup_at),
        .decode_ms = durationMillis(decode_started_at, finished_decode_at),
    };
}

fn runRawDecodeDeviceWarmup(
    allocator: std.mem.Allocator,
    io: std.Io,
    cb: *const ops.ComputeBackend,
) !u64 {
    _ = allocator;
    const target_ms = platform.env.getenvUsize("ANTFLY_INFERENCE_RAW_DECODE_DEVICE_WARMUP_MS") orelse 0;
    const fixed_iters = platform.env.getenvUsize("ANTFLY_INFERENCE_RAW_DECODE_DEVICE_WARMUP_ITERS") orelse 0;
    if (target_ms == 0 and fixed_iters == 0) return 0;

    const warmup_mb = platform.env.getenvUsize("ANTFLY_INFERENCE_RAW_DECODE_DEVICE_WARMUP_MB") orelse 256;
    const warmup_bytes = try std.math.mul(usize, warmup_mb, 1024 * 1024);
    const iterations = if (fixed_iters != 0) fixed_iters else @max(target_ms, 1);
    const started_at = std.Io.Timestamp.now(io, .awake);
    if (!(try cb.debugCudaDeviceWarmup(warmup_bytes, iterations))) return 0;
    return durationMillis(started_at, std.Io.Timestamp.now(io, .awake));
}

fn rawDecodeBenchWarmupTokens() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_RAW_DECODE_WARMUP_TOKENS") orelse 0;
}

fn rawDecodeBenchResidentTokenInputsEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_RAW_DECODE_RESIDENT_TOKEN_INPUTS", false);
}

fn rawDecodeBenchGreedyTokenTensorEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_RAW_DECODE_GREEDY_TOKEN_TENSOR", false);
}

fn rawDecodeBenchToken(index: usize, vocab_size: u32) i64 {
    if (vocab_size <= 1) return 0;
    var x: u64 = @as(u64, @intCast(index)) +% 1;
    x = x *% 6364136223846793005 +% 1442695040888963407;
    return @intCast((x % @as(u64, vocab_size - 1)) + 1);
}

fn writeRawDecodeBenchJson(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    model_dir: []const u8,
    backend_name: []const u8,
    result: RawDecodeBenchResult,
    load_model_ms: u64,
    prompt_prep_ms: u64,
    scheduler_ms: u64,
    backend_setup_ms: u64,
    decode_setup_ms: u64,
    total_ms: u64,
    cuda_stats_opt: ?session_factory.CudaRuntimeStats,
    cuda_generate_stats_opt: ?session_factory.CudaRuntimeStats,
) !void {
    const cuda_json = if (comptime build_options.enable_cuda)
        try cudaStatsCompactJson(allocator, cuda_stats_opt, result.tokens)
    else
        try allocator.dupe(u8, "null");
    defer allocator.free(cuda_json);

    const cuda_generate_json = if (comptime build_options.enable_cuda)
        try cudaStatsCompactJson(allocator, cuda_generate_stats_opt, result.tokens)
    else
        try allocator.dupe(u8, "null");
    defer allocator.free(cuda_generate_json);

    const json = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\"model_dir":{f},
        \\"backend":{f},
        \\"tokens":{d},
        \\"warmup_tokens":{d},
        \\"prompt_tokens":{d},
        \\"benchmark_type":"raw_decode",
        \\"benchmark_scope":{f},
        \\"raw_decode_tok_per_s":{d:.6},
        \\"timing_ms":{{
        \\"load_model":{d},
        \\"prompt_prep":{d},
        \\"scheduler":{d},
        \\"backend_setup":{d},
        \\"decode_setup":{d},
        \\"prefill":{d},
        \\"device_warmup":{d},
        \\"warmup":{d},
        \\"decode":{d},
        \\"total":{d}
        \\}},
        \\"cuda":{s},
        \\"cuda_generate":{s}
        \\}}
        \\
    ,
        .{
            std.json.fmt(model_dir, .{}),
            std.json.fmt(backend_name, .{}),
            result.tokens,
            result.warmup_tokens,
            result.prompt_tokens,
            std.json.fmt(result.scope, .{}),
            tokensPerSecond(result.tokens, result.decode_ms),
            load_model_ms,
            prompt_prep_ms,
            scheduler_ms,
            backend_setup_ms,
            decode_setup_ms,
            result.prefill_ms,
            result.device_warmup_ms,
            result.warmup_ms,
            result.decode_ms,
            total_ms,
            cuda_json,
            cuda_generate_json,
        },
    );
    defer allocator.free(json);
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = json });
}

fn perToken(count: usize, tokens: usize) f64 {
    if (tokens == 0) return 0.0;
    return @as(f64, @floatFromInt(count)) / @as(f64, @floatFromInt(tokens));
}

fn cudaStatsDelta(after: session_factory.CudaRuntimeStats, before: session_factory.CudaRuntimeStats) session_factory.CudaRuntimeStats {
    if (comptime !build_options.enable_cuda) return;
    var delta = after;
    inline for (std.meta.fields(session_factory.CudaRuntimeStats)) |field| {
        switch (@typeInfo(field.type)) {
            .int => @field(delta, field.name) = @field(after, field.name) -| @field(before, field.name),
            else => {},
        }
    }
    return delta;
}

fn appendFmt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const chunk = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

fn tokenIdsJson(allocator: std.mem.Allocator, ids_opt: ?[]i32) ![]u8 {
    if (!platform.env.getenvBool("ANTFLY_INFERENCE_JSON_TOKEN_IDS")) return allocator.dupe(u8, "null");
    const ids = ids_opt orelse return allocator.dupe(u8, "null");
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, '[');
    for (ids, 0..) |id, idx| {
        if (idx > 0) try out.append(allocator, ',');
        try appendFmt(allocator, &out, "{d}", .{id});
    }
    try out.append(allocator, ']');
    return out.toOwnedSlice(allocator);
}

fn cudaStatsCompactJson(
    allocator: std.mem.Allocator,
    stats_opt: ?session_factory.CudaRuntimeStats,
    tokens: usize,
) ![]u8 {
    const stats = stats_opt orelse return allocator.dupe(u8, "null");
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try appendFmt(
        allocator,
        &out,
        \\{{
        \\"kernel_launches":{d},
        \\"launches_per_token":{d:.6},
        \\"stream_syncs":{d},
        \\"syncs_per_token":{d:.6},
        \\"upload_syncs":{d},
        \\"pinned_scalar_uploads":{d},
        \\"pinned_scalar_upload_bytes":{d},
        \\"pinned_scalar_upload_fallbacks":{d},
        \\"pinned_scalar_upload_wrap_syncs":{d},
        \\"pinned_scalar_downloads":{d},
        \\"pinned_scalar_download_bytes":{d},
        \\"pinned_scalar_download_fallbacks":{d},
        \\"download_syncs":{d},
        \\"eval_syncs":{d},
        \\"h2d_bytes":{d},
        \\"d2h_bytes":{d},
        \\"d2d_bytes":{d},
        \\"cross_backend_copies":{d},
        \\"cross_backend_copy_bytes":{d},
        \\"cross_backend_event_records":{d},
        \\"cross_backend_event_waits":{d},
        \\"cross_backend_sync_fallbacks":{d},
        \\
    ,
        .{
            stats.kernel_launches,
            perToken(stats.kernel_launches, tokens),
            stats.stream_syncs,
            perToken(stats.stream_syncs, tokens),
            stats.upload_syncs,
            stats.pinned_scalar_uploads,
            stats.pinned_scalar_upload_bytes,
            stats.pinned_scalar_upload_fallbacks,
            stats.pinned_scalar_upload_wrap_syncs,
            stats.pinned_scalar_downloads,
            stats.pinned_scalar_download_bytes,
            stats.pinned_scalar_download_fallbacks,
            stats.download_syncs,
            stats.eval_syncs,
            stats.h2d_bytes,
            stats.d2h_bytes,
            stats.d2d_bytes,
            stats.cross_backend_copies,
            stats.cross_backend_copy_bytes,
            stats.cross_backend_event_records,
            stats.cross_backend_event_waits,
            stats.cross_backend_sync_fallbacks,
        },
    );
    try appendFmt(
        allocator,
        &out,
        \\"graph_capture_begins":{d},
        \\"graph_capture_replays":{d},
        \\"graph_capture_discards":{d},
        \\"graph_capture_instantiates":{d},
        \\"graph_capture_update_successes":{d},
        \\"graph_capture_update_failures":{d},
        \\"graph_capture_update_unavailable":{d},
        \\"graph_capture_scalar_updates":{d},
        \\"graph_capture_persistent_replays":{d},
        \\"graph_capture_capacity_skips":{d},
        \\"launch_linear":{d},
        \\"launch_linear_qkv":{d},
        \\"launch_norm":{d},
        \\"launch_attention":{d},
        \\"launch_elementwise":{d},
        \\"launch_scalar":{d},
        \\"launch_argmax":{d},
        \\"decoder_runtime_linear_apply_hits":{d},
        \\"decoder_runtime_linear_pair_apply_hits":{d},
        \\"decoder_runtime_linear_qkv_apply_hits":{d},
        \\"decoder_runtime_rms_norm_apply_hits":{d},
        \\"decoder_runtime_attention_residual_hits":{d},
        \\"decoder_runtime_gated_ffn_hits":{d},
        \\
    ,
        .{
            stats.cuda_graph_capture_begins,
            stats.cuda_graph_capture_replays,
            stats.cuda_graph_capture_discards,
            stats.cuda_graph_capture_instantiates,
            stats.cuda_graph_capture_update_successes,
            stats.cuda_graph_capture_update_failures,
            stats.cuda_graph_capture_update_unavailable,
            stats.cuda_graph_capture_scalar_updates,
            stats.cuda_graph_capture_persistent_replays,
            stats.cuda_graph_capture_capacity_skips,
            stats.launch_linear,
            stats.launch_linear_qkv,
            stats.launch_norm,
            stats.launch_attention,
            stats.launch_elementwise,
            stats.launch_scalar,
            stats.launch_argmax,
            stats.decoder_runtime_linear_apply_hits,
            stats.decoder_runtime_linear_pair_apply_hits,
            stats.decoder_runtime_linear_qkv_apply_hits,
            stats.decoder_runtime_rms_norm_apply_hits,
            stats.decoder_runtime_attention_residual_hits,
            stats.decoder_runtime_gated_ffn_hits,
        },
    );
    try appendFmt(
        allocator,
        &out,
        \\"decode_profile_events":{d},
        \\"decode_profile_qkv_us":{d},
        \\"decode_profile_gqa_attention_us":{d},
        \\"decode_profile_attention_output_us":{d},
        \\"decode_profile_attention_norm_residual_us":{d},
        \\"decode_profile_ffn_gate_up_us":{d},
        \\"decode_profile_ffn_gated_down_us":{d},
        \\"decode_profile_ffn_post_norm_us":{d},
        \\"decode_profile_lm_head_argmax_us":{d},
        \\"decode_profile_graph_replay_us":{d},
        \\"lm_head_argmax_fused_q8":{d},
        \\"lm_head_argmax_fused_q4_0":{d},
        \\"lm_head_argmax_fused_q4":{d},
        \\"lm_head_argmax_fused_q6":{d},
        \\"lm_head_argmax_fallbacks":{d},
        \\
    ,
        .{
            stats.decode_profile_events,
            stats.decode_profile_qkv_us,
            stats.decode_profile_gqa_attention_us,
            stats.decode_profile_attention_output_us,
            stats.decode_profile_attention_norm_residual_us,
            stats.decode_profile_ffn_gate_up_us,
            stats.decode_profile_ffn_gated_down_us,
            stats.decode_profile_ffn_post_norm_us,
            stats.decode_profile_lm_head_argmax_us,
            stats.decode_profile_graph_replay_us,
            stats.lm_head_argmax_fused_q8,
            stats.lm_head_argmax_fused_q4_0,
            stats.lm_head_argmax_fused_q4,
            stats.lm_head_argmax_fused_q6,
            stats.lm_head_argmax_fallbacks,
        },
    );
    try appendFmt(
        allocator,
        &out,
        \\"prefill_profile_events":{d},
        \\"prefill_profile_q4_linear_us":{d},
        \\"prefill_profile_q4_qkv_us":{d},
        \\"prefill_profile_q4_pair_us":{d},
        \\"prefill_profile_q4_gated_down_us":{d},
        \\"prefill_profile_bf16_linear_us":{d},
        \\"prefill_profile_bf16_qkv_us":{d},
        \\"prefill_profile_bf16_pair_us":{d},
        \\"prefill_profile_attention_us":{d},
        \\"prefill_profile_ple_dense_us":{d},
        \\"prefill_profile_staging_us":{d},
        \\"prefill_profile_norm_us":{d},
        \\
    ,
        .{
            stats.prefill_profile_events,
            stats.prefill_profile_q4_linear_us,
            stats.prefill_profile_q4_qkv_us,
            stats.prefill_profile_q4_pair_us,
            stats.prefill_profile_q4_gated_down_us,
            stats.prefill_profile_bf16_linear_us,
            stats.prefill_profile_bf16_qkv_us,
            stats.prefill_profile_bf16_pair_us,
            stats.prefill_profile_attention_us,
            stats.prefill_profile_ple_dense_us,
            stats.prefill_profile_staging_us,
            stats.prefill_profile_norm_us,
        },
    );
    try appendFmt(
        allocator,
        &out,
        \\"mtp_preproject_fused_hits":{d},
        \\"mtp_preproject_fused_f32_weight_hits":{d},
        \\"mtp_preproject_fused_bf16_weight_hits":{d},
        \\"mtp_preproject_fused_f16_weight_hits":{d},
        \\"mtp_preproject_fused_fallbacks":{d},
        \\"mtp_masked_select_fused_hits":{d},
        \\"mtp_masked_select_fused_f32_weight_hits":{d},
        \\"mtp_masked_select_fused_bf16_weight_hits":{d},
        \\"mtp_masked_select_fused_f16_weight_hits":{d},
        \\"mtp_masked_select_fused_fallbacks":{d},
        \\"mtp_masked_select_hidden_fused_hits":{d},
        \\"mtp_masked_select_hidden_fused_bf16_hits":{d},
        \\"mtp_masked_select_hidden_multiblock_hits":{d},
        \\"mtp_masked_select_hidden_fused_fallbacks":{d},
        \\"mtp_masked_argmax_hits":{d},
        \\"mtp_masked_argmax_fallbacks":{d},
        \\"mtp_verify_commit_device_hits":{d},
        \\"mtp_verify_commit_device_fallbacks":{d},
        \\"mtp_verify_commit_result_downloads":{d},
        \\"mtp_verify_commit_choice_downloads":{d},
        \\
    ,
        .{
            stats.mtp_preproject_fused_hits,
            stats.mtp_preproject_fused_f32_weight_hits,
            stats.mtp_preproject_fused_bf16_weight_hits,
            stats.mtp_preproject_fused_f16_weight_hits,
            stats.mtp_preproject_fused_fallbacks,
            stats.mtp_masked_select_fused_hits,
            stats.mtp_masked_select_fused_f32_weight_hits,
            stats.mtp_masked_select_fused_bf16_weight_hits,
            stats.mtp_masked_select_fused_f16_weight_hits,
            stats.mtp_masked_select_fused_fallbacks,
            stats.mtp_masked_select_hidden_fused_hits,
            stats.mtp_masked_select_hidden_fused_bf16_hits,
            stats.mtp_masked_select_hidden_multiblock_hits,
            stats.mtp_masked_select_hidden_fused_fallbacks,
            stats.mtp_masked_argmax_hits,
            stats.mtp_masked_argmax_fallbacks,
            stats.mtp_verify_commit_device_hits,
            stats.mtp_verify_commit_device_fallbacks,
            stats.mtp_verify_commit_result_downloads,
            stats.mtp_verify_commit_choice_downloads,
        },
    );
    try appendFmt(
        allocator,
        &out,
        \\"device_kv_attempts":{d},
        \\"device_kv_successes":{d},
        \\"device_kv_reads":{d},
        \\"device_kv_writes":{d},
        \\"device_kv_compressed_v_reads":{d},
        \\"device_kv_compressed_v_writes":{d},
        \\"device_kv_compressed_v_bytes":{d},
        \\"device_kv_paged_block_table_uploads":{d},
        \\"device_kv_paged_block_table_bytes":{d},
        \\"device_kv_paged_identity_attention_reads":{d},
        \\"device_kv_fail_read":{d},
        \\"device_kv_fail_shape":{d}
        \\}}
    ,
        .{
            stats.device_kv_attempts,
            stats.device_kv_successes,
            stats.device_kv_reads,
            stats.device_kv_writes,
            stats.device_kv_compressed_v_reads,
            stats.device_kv_compressed_v_writes,
            stats.device_kv_compressed_v_bytes,
            stats.device_kv_paged_block_table_uploads,
            stats.device_kv_paged_block_table_bytes,
            stats.device_kv_paged_identity_attention_reads,
            stats.device_kv_fail_read,
            stats.device_kv_fail_shape,
        },
    );
    return try out.toOwnedSlice(allocator);
}

fn writeJsonTiming(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    model_dir: []const u8,
    backend_name: []const u8,
    result: *const generation.GenerationResult,
    load_model_ms: u64,
    prompt_prep_ms: u64,
    scheduler_ms: u64,
    backend_setup_ms: u64,
    decode_setup_ms: u64,
    generate_ms: u64,
    total_ms: u64,
    cuda_stats_opt: ?session_factory.CudaRuntimeStats,
    cuda_generate_stats_opt: ?session_factory.CudaRuntimeStats,
    draft_cuda_stats_opt: ?session_factory.CudaRuntimeStats,
    draft_cuda_generate_stats_opt: ?session_factory.CudaRuntimeStats,
) !void {
    const inner_timing = result.timing_ms orelse generation.GenerationTimingMs{};
    const runtime_stats = graph_mod.metal_executor.getTimingStats();
    const decoder_debug_stats = generation.getDecoderRuntimeDebugStats();
    const decode_ms = if (inner_timing.decode != 0) inner_timing.decode else generate_ms;
    const speculative_json = if (result.speculative) |stats| blk: {
        const quality = stats.mtp_quality;
        const profile = stats.mtp_profile;
        var profile_out = std.ArrayListUnmanaged(u8).empty;
        errdefer profile_out.deinit(allocator);
        try appendFmt(
            allocator,
            &profile_out,
            \\{{
            \\"enabled":{},
            \\"sync_enabled":{},
            \\"draft_steps":{d},
            \\"resident_draft_steps":{d},
            \\"host_draft_steps":{d},
            \\"target_verify_calls":{d},
            \\"target_verify_rows":{d},
            \\"target_verify_argmax_calls":{d},
            \\"target_verify_argmax_rows":{d},
            \\"target_verify_argmax_batched_calls":{d},
            \\"target_verify_argmax_syncs":{d},
            \\"dedicated_runtime_hits":{d},
            \\"dedicated_runtime_fallbacks":{d},
            \\"device_verify_commit_hits":{d},
            \\"device_verify_commit_fallbacks":{d},
            \\"device_verify_commit_result_downloads":{d},
            \\"target_choice_downloads":{d},
            \\
        ,
            .{
                profile.enabled,
                profile.sync_enabled,
                profile.draft_steps,
                profile.resident_draft_steps,
                profile.host_draft_steps,
                profile.target_verify_calls,
                profile.target_verify_rows,
                profile.target_verify_argmax_calls,
                profile.target_verify_argmax_rows,
                profile.target_verify_argmax_batched_calls,
                profile.target_verify_argmax_syncs,
                profile.dedicated_runtime_hits,
                profile.dedicated_runtime_fallbacks,
                profile.device_verify_commit_hits,
                profile.device_verify_commit_fallbacks,
                profile.device_verify_commit_result_downloads,
                profile.target_choice_downloads,
            },
        );
        try appendFmt(
            allocator,
            &profile_out,
            \\"commit_forwards_required":{d},
            \\"commit_forwards_avoided":{d},
            \\"accepted_hidden_reuse_rows":{d},
            \\"activation_copies":{d},
            \\"materializations":{d},
            \\"materialization_hidden_only_hits":{d},
            \\"materialization_hidden_only_fallbacks":{d},
            \\"correction_materializations":{d},
            \\"bonus_materializations":{d},
            \\"bonus_skips":{d},
            \\"fallback_calls":{d},
            \\"draft_embedding_cache_hits":{d},
            \\"draft_embedding_cache_misses":{d},
            \\"draft_embedding_cache_inserts":{d},
            \\"draft_embedding_cache_evictions":{d},
            \\"draft_embedding_cache_disabled":{d},
            \\"draft_target_embedding_cross_copies":{d},
            \\"draft_token_ns":{d},
            \\"draft_target_embedding_ns":{d},
            \\"draft_concat_ns":{d},
            \\"draft_preprojection_ns":{d},
            \\"draft_assistant_ns":{d},
            \\"draft_postprojection_ns":{d},
            \\"draft_argmax_ns":{d},
            \\"draft_lm_head_ns":{d},
            \\"draft_selection_ns":{d},
            \\"target_verify_ns":{d},
            \\"activation_copy_ns":{d},
            \\"materialization_ns":{d},
            \\"fallback_ns":{d}
            \\}}
        ,
            .{
                profile.commit_forwards_required,
                profile.commit_forwards_avoided,
                profile.accepted_hidden_reuse_rows,
                profile.activation_copies,
                profile.materializations,
                profile.materialization_hidden_only_hits,
                profile.materialization_hidden_only_fallbacks,
                profile.correction_materializations,
                profile.bonus_materializations,
                profile.bonus_skips,
                profile.fallback_calls,
                profile.draft_embedding_cache_hits,
                profile.draft_embedding_cache_misses,
                profile.draft_embedding_cache_inserts,
                profile.draft_embedding_cache_evictions,
                profile.draft_embedding_cache_disabled,
                profile.draft_target_embedding_cross_copies,
                profile.draft_token_ns,
                profile.draft_target_embedding_ns,
                profile.draft_concat_ns,
                profile.draft_preprojection_ns,
                profile.draft_assistant_ns,
                profile.draft_postprojection_ns,
                profile.draft_argmax_ns,
                profile.draft_lm_head_ns,
                profile.draft_selection_ns,
                profile.target_verify_ns,
                profile.activation_copy_ns,
                profile.materialization_ns,
                profile.fallback_ns,
            },
        );
        const profile_json = try profile_out.toOwnedSlice(allocator);
        defer allocator.free(profile_json);
        break :blk try std.fmt.allocPrint(
            allocator,
            \\{{
            \\"speculation_policy":"{s}",
            \\"speculation_calibration":"{s}",
            \\"speculation_policy_decision":"{s}",
            \\"rounds":{d},
            \\"drafted":{d},
            \\"matched":{d},
            \\"rejected":{d},
            \\"accepted":{d},
            \\"corrections":{d},
            \\"bonus":{d},
            \\"adaptive_fallbacks":{d},
            \\"mtp_enabled":{},
            \\"mtp_graph_replay":"{s}",
            \\"mtp_acceptance_permille":{d},
            \\"mtp_acceptance_gate_fallbacks":{d},
            \\"mtp_profile":{s},
            \\"mtp_quality":{{
            \\"mismatches":{d},
            \\"mismatches_with_assistant_logits":{d},
            \\"target_in_assistant_top2":{d},
            \\"target_in_assistant_top4":{d},
            \\"target_in_assistant_top8":{d},
            \\"draft_in_target_top2":{d},
            \\"format_or_control_misses":{d},
            \\"near_tie_misses":{d},
            \\"confident_misses":{d},
            \\"avg_assistant_target_margin":{d:.6}
            \\}}
            \\}}
        ,
            .{
                stats.speculation_policy.name(),
                stats.speculation_calibration.name(),
                stats.speculation_policy_decision.name(),
                stats.rounds,
                stats.drafted_tokens,
                stats.matched_draft_tokens,
                stats.rejectedDraftTokens(),
                stats.accepted_tokens,
                stats.correction_tokens,
                stats.bonus_tokens,
                stats.adaptive_fallbacks,
                stats.mtp_enabled,
                stats.mtp_graph_replay_status,
                stats.acceptancePermille(),
                stats.mtp_acceptance_gate_fallbacks,
                profile_json,
                quality.mismatches,
                quality.mismatches_with_assistant_logits,
                quality.target_in_assistant_top2,
                quality.target_in_assistant_top4,
                quality.target_in_assistant_top8,
                quality.draft_in_target_top2,
                quality.format_or_control_misses,
                quality.near_tie_misses,
                quality.confident_misses,
                quality.averageAssistantTargetMargin(),
            },
        );
    } else try allocator.dupe(u8, "null");
    defer allocator.free(speculative_json);
    const cuda_json = if (comptime build_options.enable_cuda) blk: {
        if (cuda_stats_opt) |cuda_stats| {
            var cuda_out = std.ArrayListUnmanaged(u8).empty;
            errdefer cuda_out.deinit(allocator);
            try appendFmt(
                allocator,
                &cuda_out,
                \\{{
                \\"kernel_launches":{d},
                \\"launches_per_token":{d:.6},
                \\"stream_syncs":{d},
                \\"syncs_per_token":{d:.6},
                \\"upload_syncs":{d},
                \\"pinned_scalar_uploads":{d},
                \\"pinned_scalar_upload_bytes":{d},
                \\"pinned_scalar_upload_fallbacks":{d},
                \\"pinned_scalar_upload_wrap_syncs":{d},
                \\"pinned_scalar_downloads":{d},
                \\"pinned_scalar_download_bytes":{d},
                \\"pinned_scalar_download_fallbacks":{d},
                \\"download_syncs":{d},
                \\"eval_syncs":{d},
                \\"h2d_bytes":{d},
                \\"d2h_bytes":{d},
                \\"d2d_bytes":{d},
                \\"cross_backend_copies":{d},
                \\"cross_backend_copy_bytes":{d},
                \\"cross_backend_event_records":{d},
                \\"cross_backend_event_waits":{d},
                \\"cross_backend_sync_fallbacks":{d},
                \\
            ,
                .{
                    cuda_stats.kernel_launches,
                    perToken(cuda_stats.kernel_launches, result.tokens_used),
                    cuda_stats.stream_syncs,
                    perToken(cuda_stats.stream_syncs, result.tokens_used),
                    cuda_stats.upload_syncs,
                    cuda_stats.pinned_scalar_uploads,
                    cuda_stats.pinned_scalar_upload_bytes,
                    cuda_stats.pinned_scalar_upload_fallbacks,
                    cuda_stats.pinned_scalar_upload_wrap_syncs,
                    cuda_stats.pinned_scalar_downloads,
                    cuda_stats.pinned_scalar_download_bytes,
                    cuda_stats.pinned_scalar_download_fallbacks,
                    cuda_stats.download_syncs,
                    cuda_stats.eval_syncs,
                    cuda_stats.h2d_bytes,
                    cuda_stats.d2h_bytes,
                    cuda_stats.d2d_bytes,
                    cuda_stats.cross_backend_copies,
                    cuda_stats.cross_backend_copy_bytes,
                    cuda_stats.cross_backend_event_records,
                    cuda_stats.cross_backend_event_waits,
                    cuda_stats.cross_backend_sync_fallbacks,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"temp_buffer_hits":{d},
                \\"temp_buffer_misses":{d},
                \\"temp_buffer_releases":{d},
                \\"temp_buffer_evictions":{d},
                \\"temp_buffer_cached_bytes":{d},
                \\"deferred_free_queued":{d},
                \\"deferred_free_drains":{d},
                \\"deferred_free_forced_drains":{d},
                \\"deferred_free_pending_bytes":{d},
                \\"deferred_free_reclaimed_bytes":{d},
                \\"graph_capture_begins":{d},
                \\"graph_capture_replays":{d},
                \\"graph_capture_discards":{d},
                \\"graph_capture_instantiates":{d},
                \\"graph_capture_update_successes":{d},
                \\"graph_capture_update_failures":{d},
                \\"graph_capture_update_unavailable":{d},
                \\"graph_capture_scalar_updates":{d},
                \\"graph_capture_persistent_replays":{d},
                \\"graph_capture_capacity_skips":{d},
                \\
            ,
                .{
                    cuda_stats.temp_buffer_hits,
                    cuda_stats.temp_buffer_misses,
                    cuda_stats.temp_buffer_releases,
                    cuda_stats.temp_buffer_evictions,
                    cuda_stats.temp_buffer_cached_bytes,
                    cuda_stats.deferred_free_queued,
                    cuda_stats.deferred_free_drains,
                    cuda_stats.deferred_free_forced_drains,
                    cuda_stats.deferred_free_pending_bytes,
                    cuda_stats.deferred_free_reclaimed_bytes,
                    cuda_stats.cuda_graph_capture_begins,
                    cuda_stats.cuda_graph_capture_replays,
                    cuda_stats.cuda_graph_capture_discards,
                    cuda_stats.cuda_graph_capture_instantiates,
                    cuda_stats.cuda_graph_capture_update_successes,
                    cuda_stats.cuda_graph_capture_update_failures,
                    cuda_stats.cuda_graph_capture_update_unavailable,
                    cuda_stats.cuda_graph_capture_scalar_updates,
                    cuda_stats.cuda_graph_capture_persistent_replays,
                    cuda_stats.cuda_graph_capture_capacity_skips,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"launch_embedding":{d},
                \\"launch_linear":{d},
                \\"launch_linear_qkv":{d},
                \\"launch_norm":{d},
                \\"launch_rope":{d},
                \\"launch_attention":{d},
                \\"launch_attention_gqa_decode":{d},
                \\"launch_attention_gqa_decode_fast":{d},
                \\"launch_attention_gqa_decode_fast_fallbacks":{d},
                \\"launch_attention_gqa_prefill_fast":{d},
                \\"launch_attention_gqa_prefill_tiled":{d},
                \\"launch_attention_gqa_prefill_mma":{d},
                \\"launch_attention_gqa_prefill_mma_m32":{d},
                \\"launch_attention_gqa_scalar":{d},
                \\"launch_elementwise":{d},
                \\"launch_scalar":{d},
                \\"launch_argmax":{d},
                \\
            ,
                .{
                    cuda_stats.launch_embedding,
                    cuda_stats.launch_linear,
                    cuda_stats.launch_linear_qkv,
                    cuda_stats.launch_norm,
                    cuda_stats.launch_rope,
                    cuda_stats.launch_attention,
                    cuda_stats.launch_attention_gqa_decode,
                    cuda_stats.launch_attention_gqa_decode_fast,
                    cuda_stats.launch_attention_gqa_decode_fast_fallbacks,
                    cuda_stats.launch_attention_gqa_prefill_fast,
                    cuda_stats.launch_attention_gqa_prefill_tiled,
                    cuda_stats.launch_attention_gqa_prefill_mma,
                    cuda_stats.launch_attention_gqa_prefill_mma_m32,
                    cuda_stats.launch_attention_gqa_scalar,
                    cuda_stats.launch_elementwise,
                    cuda_stats.launch_scalar,
                    cuda_stats.launch_argmax,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"activation_multiply_fused":{d},
                \\"linear_activation_slice_fused_q4_0":{d},
                \\"add_mul_scalar_fused":{d},
                \\"rms_norm_add_output_scale_fused":{d},
                \\"rms_norm_add_weighted_embedding_fused_q6_k":{d},
                \\"rms_norm_add_output_scale_fallbacks":{d},
                \\"gated_down_fused_q8":{d},
                \\"gated_down_fused_q4_0":{d},
                \\"gated_down_fused_q4_0_precompute":{d},
                \\"gated_down_fused_q4_0_tile4":{d},
                \\"gated_down_fused_q4_0_tile8":{d},
                \\"gated_down_fused_q4_0_tile16":{d},
                \\"gated_down_fused_q4":{d},
                \\"gated_down_fallbacks":{d},
                \\"qkv_fused_q8":{d},
                \\"qkv_fused_q4_0":{d},
                \\"qkv_fused_q4_0_tile4":{d},
                \\"qkv_fused_q4_0_tile8":{d},
                \\"qkv_fused_q4":{d},
                \\"qkv_fused_q4_q4_f32":{d},
                \\"qkv_fused_f32":{d},
                \\"qkv_fallback_unsupported":{d},
                \\"qkv_kernel_unavailable":{d},
                \\"linear_pair_fused_q8":{d},
                \\"linear_pair_fused_q4_0":{d},
                \\"linear_pair_fused_q4_0_activation":{d},
                \\"linear_pair_fused_q4_0_tile4":{d},
                \\"linear_pair_fused_q4_0_tile8":{d},
                \\"linear_pair_fused_q4":{d},
                \\"linear_pair_fallbacks":{d},
                \\
            ,
                .{
                    cuda_stats.activation_multiply_fused,
                    cuda_stats.linear_activation_slice_fused_q4_0,
                    cuda_stats.add_mul_scalar_fused,
                    cuda_stats.rms_norm_add_output_scale_fused,
                    cuda_stats.rms_norm_add_weighted_embedding_fused_q6_k,
                    cuda_stats.rms_norm_add_output_scale_fallbacks,
                    cuda_stats.gated_down_fused_q8,
                    cuda_stats.gated_down_fused_q4_0,
                    cuda_stats.gated_down_fused_q4_0_precompute,
                    cuda_stats.gated_down_fused_q4_0_tile4,
                    cuda_stats.gated_down_fused_q4_0_tile8,
                    cuda_stats.gated_down_fused_q4_0_tile16,
                    cuda_stats.gated_down_fused_q4,
                    cuda_stats.gated_down_fallbacks,
                    cuda_stats.qkv_fused_q8,
                    cuda_stats.qkv_fused_q4_0,
                    cuda_stats.qkv_fused_q4_0_tile4,
                    cuda_stats.qkv_fused_q4_0_tile8,
                    cuda_stats.qkv_fused_q4,
                    cuda_stats.qkv_fused_q4_q4_f32,
                    cuda_stats.qkv_fused_f32,
                    cuda_stats.qkv_fallback_unsupported,
                    cuda_stats.qkv_kernel_unavailable,
                    cuda_stats.linear_pair_fused_q8,
                    cuda_stats.linear_pair_fused_q4_0,
                    cuda_stats.linear_pair_fused_q4_0_activation,
                    cuda_stats.linear_pair_fused_q4_0_tile4,
                    cuda_stats.linear_pair_fused_q4_0_tile8,
                    cuda_stats.linear_pair_fused_q4,
                    cuda_stats.linear_pair_fallbacks,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"q4_0_q8_1_prefill_linear_hits":{d},
                \\"q4_0_q8_1_prefill_linear_rows2_hits":{d},
                \\"q4_0_q8_1_prefill_linear_rows4_hits":{d},
                \\"q4_0_q8_1_prefill_linear_rows8_c4_hits":{d},
                \\"q4_0_q8_1_prefill_linear_e4b_down_rows_hits":{d},
                \\"q4_0_q8_1_prefill_linear_generic_rows_hits":{d},
                \\"q4_0_q8_1_prefill_linear_tile8_rows_hits":{d},
                \\"q4_0_q8_1_prefill_qkv_hits":{d},
                \\"q4_0_q8_1_prefill_qkv_rows4_hits":{d},
                \\"q4_0_q8_1_prefill_qkv_tile8_rows_hits":{d},
                \\"q4_0_q8_1_prefill_qkv_tile8_w8_rows_hits":{d},
                \\"q4_0_q8_1_prefill_pair_hits":{d},
                \\"q4_0_q8_1_prefill_pair_rows2_hits":{d},
                \\"q4_0_q8_1_prefill_pair_rows4_hits":{d},
                \\"q4_0_q8_1_prefill_pair_rows8_c2_hits":{d},
                \\"q4_0_q8_1_prefill_pair_rows16_c1_hits":{d},
                \\"q4_0_q8_1_prefill_pair_generic_rows_hits":{d},
                \\"q4_0_q8_1_prefill_pair_tile8_rows_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_rows2_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_rows4_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_rows8_c4_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_e4b_down_rows_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_generic_rows_hits":{d},
                \\"q4_0_q8_1_prefill_gated_down_tile8_rows_hits":{d},
                \\
            ,
                .{
                    cuda_stats.q4_0_q8_1_prefill_linear_hits,
                    cuda_stats.q4_0_q8_1_prefill_linear_rows2_hits,
                    cuda_stats.q4_0_q8_1_prefill_linear_rows4_hits,
                    cuda_stats.q4_0_q8_1_prefill_linear_rows8_c4_hits,
                    cuda_stats.q4_0_q8_1_prefill_linear_e4b_down_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_linear_generic_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_linear_tile8_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_qkv_hits,
                    cuda_stats.q4_0_q8_1_prefill_qkv_rows4_hits,
                    cuda_stats.q4_0_q8_1_prefill_qkv_tile8_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_qkv_tile8_w8_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_rows2_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_rows4_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_rows8_c2_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_rows16_c1_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_generic_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_pair_tile8_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_rows2_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_rows4_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_rows8_c4_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_e4b_down_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_generic_rows_hits,
                    cuda_stats.q4_0_q8_1_prefill_gated_down_tile8_rows_hits,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"lm_head_argmax_fused_q8":{d},
                \\"lm_head_argmax_fused_q4_0":{d},
                \\"lm_head_argmax_fused_q4":{d},
                \\"lm_head_argmax_fused_q6":{d},
                \\"lm_head_argmax_fallbacks":{d},
                \\"q4k_decode_fast_hits":{d},
                \\"q4k_decode_fast_fallbacks":{d},
                \\"bf16_cublaslt_linear_calls":{d},
                \\"bf16_cublaslt_qkv_calls":{d},
                \\"bf16_cublaslt_activation_staging_calls":{d},
                \\"bf16_cublaslt_activation_mirror_hits":{d},
                \\"bf16_cublaslt_fallbacks":{d},
                \\"bf16_scalar_linear_calls":{d},
                \\"bf16_scalar_qkv_calls":{d},
                \\"rms_norm_bf16_mirror_hits":{d},
                \\"mtp_verify_commit_device_hits":{d},
                \\"mtp_verify_commit_device_fallbacks":{d},
                \\"mtp_verify_commit_result_downloads":{d},
                \\"mtp_verify_commit_choice_downloads":{d},
                \\
            ,
                .{
                    cuda_stats.lm_head_argmax_fused_q8,
                    cuda_stats.lm_head_argmax_fused_q4_0,
                    cuda_stats.lm_head_argmax_fused_q4,
                    cuda_stats.lm_head_argmax_fused_q6,
                    cuda_stats.lm_head_argmax_fallbacks,
                    cuda_stats.q4k_decode_fast_hits,
                    cuda_stats.q4k_decode_fast_fallbacks,
                    cuda_stats.bf16_cublaslt_linear_calls,
                    cuda_stats.bf16_cublaslt_qkv_calls,
                    cuda_stats.bf16_cublaslt_activation_staging_calls,
                    cuda_stats.bf16_cublaslt_activation_mirror_hits,
                    cuda_stats.bf16_cublaslt_fallbacks,
                    cuda_stats.bf16_scalar_linear_calls,
                    cuda_stats.bf16_scalar_qkv_calls,
                    cuda_stats.rms_norm_bf16_mirror_hits,
                    cuda_stats.mtp_verify_commit_device_hits,
                    cuda_stats.mtp_verify_commit_device_fallbacks,
                    cuda_stats.mtp_verify_commit_result_downloads,
                    cuda_stats.mtp_verify_commit_choice_downloads,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"decoder_runtime_linear_slot_prepares":{d},
                \\"decoder_runtime_linear_slot_prepare_misses":{d},
                \\"decoder_runtime_rms_norm_slot_prepares":{d},
                \\"decoder_runtime_rms_norm_slot_prepare_misses":{d},
                \\"decoder_runtime_linear_apply_hits":{d},
                \\"decoder_runtime_linear_apply_misses":{d},
                \\"decoder_runtime_linear_pair_apply_hits":{d},
                \\"decoder_runtime_linear_qkv_apply_hits":{d},
                \\"decoder_runtime_rms_norm_apply_hits":{d},
                \\"decoder_runtime_rms_norm_apply_misses":{d},
                \\"decoder_runtime_attention_residual_attempts":{d},
                \\"decoder_runtime_attention_residual_hits":{d},
                \\"decoder_runtime_attention_residual_misses":{d},
                \\"decoder_runtime_gated_ffn_attempts":{d},
                \\"decoder_runtime_gated_ffn_hits":{d},
                \\"decoder_runtime_gated_ffn_misses":{d},
                \\"decoder_runtime_pinned_eviction_skips":{d},
                \\"decode_profile_events":{d},
                \\"decode_profile_qkv_us":{d},
                \\"decode_profile_gqa_attention_us":{d},
                \\"decode_profile_attention_output_us":{d},
                \\"decode_profile_attention_norm_residual_us":{d},
                \\"decode_profile_ffn_gate_up_us":{d},
                \\"decode_profile_ffn_gated_down_us":{d},
                \\"decode_profile_ffn_post_norm_us":{d},
                \\"decode_profile_lm_head_argmax_us":{d},
                \\"decode_profile_graph_replay_us":{d},
                \\
            ,
                .{
                    cuda_stats.decoder_runtime_linear_slot_prepares,
                    cuda_stats.decoder_runtime_linear_slot_prepare_misses,
                    cuda_stats.decoder_runtime_rms_norm_slot_prepares,
                    cuda_stats.decoder_runtime_rms_norm_slot_prepare_misses,
                    cuda_stats.decoder_runtime_linear_apply_hits,
                    cuda_stats.decoder_runtime_linear_apply_misses,
                    cuda_stats.decoder_runtime_linear_pair_apply_hits,
                    cuda_stats.decoder_runtime_linear_qkv_apply_hits,
                    cuda_stats.decoder_runtime_rms_norm_apply_hits,
                    cuda_stats.decoder_runtime_rms_norm_apply_misses,
                    cuda_stats.decoder_runtime_attention_residual_attempts,
                    cuda_stats.decoder_runtime_attention_residual_hits,
                    cuda_stats.decoder_runtime_attention_residual_misses,
                    cuda_stats.decoder_runtime_gated_ffn_attempts,
                    cuda_stats.decoder_runtime_gated_ffn_hits,
                    cuda_stats.decoder_runtime_gated_ffn_misses,
                    cuda_stats.decoder_runtime_pinned_eviction_skips,
                    cuda_stats.decode_profile_events,
                    cuda_stats.decode_profile_qkv_us,
                    cuda_stats.decode_profile_gqa_attention_us,
                    cuda_stats.decode_profile_attention_output_us,
                    cuda_stats.decode_profile_attention_norm_residual_us,
                    cuda_stats.decode_profile_ffn_gate_up_us,
                    cuda_stats.decode_profile_ffn_gated_down_us,
                    cuda_stats.decode_profile_ffn_post_norm_us,
                    cuda_stats.decode_profile_lm_head_argmax_us,
                    cuda_stats.decode_profile_graph_replay_us,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"prefill_profile_events":{d},
                \\"prefill_profile_q4_linear_us":{d},
                \\"prefill_profile_q4_qkv_us":{d},
                \\"prefill_profile_q4_pair_us":{d},
                \\"prefill_profile_q4_gated_down_us":{d},
                \\"prefill_profile_bf16_linear_us":{d},
                \\"prefill_profile_bf16_qkv_us":{d},
                \\"prefill_profile_bf16_pair_us":{d},
                \\"prefill_profile_attention_us":{d},
                \\"prefill_profile_ple_dense_us":{d},
                \\"prefill_profile_staging_us":{d},
                \\"prefill_profile_norm_us":{d},
                \\
            ,
                .{
                    cuda_stats.prefill_profile_events,
                    cuda_stats.prefill_profile_q4_linear_us,
                    cuda_stats.prefill_profile_q4_qkv_us,
                    cuda_stats.prefill_profile_q4_pair_us,
                    cuda_stats.prefill_profile_q4_gated_down_us,
                    cuda_stats.prefill_profile_bf16_linear_us,
                    cuda_stats.prefill_profile_bf16_qkv_us,
                    cuda_stats.prefill_profile_bf16_pair_us,
                    cuda_stats.prefill_profile_attention_us,
                    cuda_stats.prefill_profile_ple_dense_us,
                    cuda_stats.prefill_profile_staging_us,
                    cuda_stats.prefill_profile_norm_us,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"launch_norm_layer":{d},
                \\"launch_norm_add_layer":{d},
                \\"launch_norm_rms":{d},
                \\"launch_norm_rms_add":{d},
                \\"launch_norm_rms_add_mul_scalar":{d},
                \\"launch_norm_rms_add_output_scale":{d},
                \\"launch_norm_rms_bare":{d},
                \\"launch_norm_head_rope":{d},
                \\"rms_norm_add_fused":{d},
                \\"rms_norm_add_output_scale_fused":{d},
                \\"rms_norm_add_output_scale_fallbacks":{d},
                \\
            ,
                .{
                    cuda_stats.launch_norm_layer,
                    cuda_stats.launch_norm_add_layer,
                    cuda_stats.launch_norm_rms,
                    cuda_stats.launch_norm_rms_add,
                    cuda_stats.launch_norm_rms_add_mul_scalar,
                    cuda_stats.launch_norm_rms_add_output_scale,
                    cuda_stats.launch_norm_rms_bare,
                    cuda_stats.launch_norm_head_rope,
                    cuda_stats.rms_norm_add_fused,
                    cuda_stats.rms_norm_add_output_scale_fused,
                    cuda_stats.rms_norm_add_output_scale_fallbacks,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"dense_stream_requests":{d},
                \\"dense_stream_hits":{d},
                \\"dense_stream_misses":{d},
                \\"dense_stream_fallbacks":{d},
                \\"dense_stream_evictions":{d},
                \\"dense_stream_read_ms":{d},
                \\"dense_stream_h2d_ms":{d},
                \\"dense_stream_read_bytes":{d},
                \\"dense_stream_uploaded_bytes":{d},
                \\"dense_stream_resident_bytes":{d},
                \\"dense_prefetch_enqueues":{d},
                \\"dense_prefetch_ready_hits":{d},
                \\"dense_prefetch_inflight_steals":{d},
                \\"dense_prefetch_sync_reads":{d},
                \\"dense_prefetch_host_read_ms":{d},
                \\"dense_prefetch_upload_ms":{d},
                \\
            ,
                .{
                    cuda_stats.dense_stream_requests,
                    cuda_stats.dense_stream_hits,
                    cuda_stats.dense_stream_misses,
                    cuda_stats.dense_stream_fallbacks,
                    cuda_stats.dense_stream_evictions,
                    cuda_stats.dense_stream_read_ns / std.time.ns_per_ms,
                    cuda_stats.dense_stream_h2d_ns / std.time.ns_per_ms,
                    cuda_stats.dense_stream_read_bytes,
                    cuda_stats.dense_stream_uploaded_bytes,
                    cuda_stats.dense_stream_resident_bytes,
                    cuda_stats.dense_prefetch_enqueues,
                    cuda_stats.dense_prefetch_ready_hits,
                    cuda_stats.dense_prefetch_inflight_steals,
                    cuda_stats.dense_prefetch_sync_reads,
                    cuda_stats.dense_prefetch_host_read_ns / std.time.ns_per_ms,
                    cuda_stats.dense_prefetch_upload_ns / std.time.ns_per_ms,
                },
            );
            try appendFmt(
                allocator,
                &cuda_out,
                \\"device_kv_attempts":{d},
                \\"device_kv_successes":{d},
                \\"device_kv_writes":{d},
                \\"device_kv_reads":{d},
                \\"device_kv_compressed_v_writes":{d},
                \\"device_kv_compressed_v_reads":{d},
                \\"device_kv_compressed_v_bytes":{d},
                \\"device_kv_paged_block_table_uploads":{d},
                \\"device_kv_paged_block_table_bytes":{d},
                \\"device_kv_paged_identity_attention_reads":{d},
                \\"device_kv_fail_batch":{d},
                \\"device_kv_fail_no_cache":{d},
                \\"device_kv_fail_no_storage":{d},
                \\"device_kv_fail_no_hook":{d},
                \\"device_kv_fail_write":{d},
                \\"device_kv_fail_read":{d},
                \\"device_kv_fail_shape":{d}
                \\}}
                \\
            ,
                .{
                    cuda_stats.device_kv_attempts,
                    cuda_stats.device_kv_successes,
                    cuda_stats.device_kv_writes,
                    cuda_stats.device_kv_reads,
                    cuda_stats.device_kv_compressed_v_writes,
                    cuda_stats.device_kv_compressed_v_reads,
                    cuda_stats.device_kv_compressed_v_bytes,
                    cuda_stats.device_kv_paged_block_table_uploads,
                    cuda_stats.device_kv_paged_block_table_bytes,
                    cuda_stats.device_kv_paged_identity_attention_reads,
                    cuda_stats.device_kv_fail_batch,
                    cuda_stats.device_kv_fail_no_cache,
                    cuda_stats.device_kv_fail_no_storage,
                    cuda_stats.device_kv_fail_no_hook,
                    cuda_stats.device_kv_fail_write,
                    cuda_stats.device_kv_fail_read,
                    cuda_stats.device_kv_fail_shape,
                },
            );
            break :blk try cuda_out.toOwnedSlice(allocator);
        }
        break :blk try allocator.dupe(u8, "null");
    } else try allocator.dupe(u8, "null");
    defer allocator.free(cuda_json);

    const cuda_generate_json = if (comptime build_options.enable_cuda) blk: {
        if (cuda_generate_stats_opt) |cuda_stats| {
            var cuda_generate_out = std.ArrayListUnmanaged(u8).empty;
            errdefer cuda_generate_out.deinit(allocator);
            try appendFmt(
                allocator,
                &cuda_generate_out,
                \\{{
                \\"kernel_launches":{d},
                \\"launches_per_token":{d:.6},
                \\"stream_syncs":{d},
                \\"syncs_per_token":{d:.6},
                \\"upload_syncs":{d},
                \\"pinned_scalar_uploads":{d},
                \\"pinned_scalar_upload_bytes":{d},
                \\"pinned_scalar_upload_fallbacks":{d},
                \\"pinned_scalar_upload_wrap_syncs":{d},
                \\"pinned_scalar_downloads":{d},
                \\"pinned_scalar_download_bytes":{d},
                \\"pinned_scalar_download_fallbacks":{d},
                \\"download_syncs":{d},
                \\"eval_syncs":{d},
                \\"device_alloc_calls":{d},
                \\"device_free_calls":{d},
                \\"temp_buffer_hits":{d},
                \\"temp_buffer_misses":{d},
                \\"temp_buffer_releases":{d},
                \\"temp_buffer_evictions":{d},
                \\"h2d_bytes":{d},
                \\"d2h_bytes":{d},
                \\"d2d_bytes":{d},
                \\"cross_backend_copies":{d},
                \\"cross_backend_copy_bytes":{d},
                \\"cross_backend_event_records":{d},
                \\"cross_backend_event_waits":{d},
                \\"cross_backend_sync_fallbacks":{d},
                \\"to_float32_calls":{d},
                \\"to_float32_bytes":{d},
                \\
            ,
                .{
                    cuda_stats.kernel_launches,
                    perToken(cuda_stats.kernel_launches, result.tokens_used),
                    cuda_stats.stream_syncs,
                    perToken(cuda_stats.stream_syncs, result.tokens_used),
                    cuda_stats.upload_syncs,
                    cuda_stats.pinned_scalar_uploads,
                    cuda_stats.pinned_scalar_upload_bytes,
                    cuda_stats.pinned_scalar_upload_fallbacks,
                    cuda_stats.pinned_scalar_upload_wrap_syncs,
                    cuda_stats.pinned_scalar_downloads,
                    cuda_stats.pinned_scalar_download_bytes,
                    cuda_stats.pinned_scalar_download_fallbacks,
                    cuda_stats.download_syncs,
                    cuda_stats.eval_syncs,
                    cuda_stats.device_alloc_calls,
                    cuda_stats.device_free_calls,
                    cuda_stats.temp_buffer_hits,
                    cuda_stats.temp_buffer_misses,
                    cuda_stats.temp_buffer_releases,
                    cuda_stats.temp_buffer_evictions,
                    cuda_stats.h2d_bytes,
                    cuda_stats.d2h_bytes,
                    cuda_stats.d2d_bytes,
                    cuda_stats.cross_backend_copies,
                    cuda_stats.cross_backend_copy_bytes,
                    cuda_stats.cross_backend_event_records,
                    cuda_stats.cross_backend_event_waits,
                    cuda_stats.cross_backend_sync_fallbacks,
                    cuda_stats.to_float32_calls,
                    cuda_stats.to_float32_bytes,
                },
            );
            try appendFmt(
                allocator,
                &cuda_generate_out,
                \\"graph_capture_begins":{d},
                \\"graph_capture_replays":{d},
                \\"graph_capture_discards":{d},
                \\"graph_capture_instantiates":{d},
                \\"graph_capture_update_successes":{d},
                \\"graph_capture_update_failures":{d},
                \\"graph_capture_update_unavailable":{d},
                \\"graph_capture_scalar_updates":{d},
                \\"graph_capture_persistent_replays":{d},
                \\"graph_capture_capacity_skips":{d},
                \\"launch_linear":{d},
                \\"launch_linear_qkv":{d},
                \\
            ,
                .{
                    cuda_stats.cuda_graph_capture_begins,
                    cuda_stats.cuda_graph_capture_replays,
                    cuda_stats.cuda_graph_capture_discards,
                    cuda_stats.cuda_graph_capture_instantiates,
                    cuda_stats.cuda_graph_capture_update_successes,
                    cuda_stats.cuda_graph_capture_update_failures,
                    cuda_stats.cuda_graph_capture_update_unavailable,
                    cuda_stats.cuda_graph_capture_scalar_updates,
                    cuda_stats.cuda_graph_capture_persistent_replays,
                    cuda_stats.cuda_graph_capture_capacity_skips,
                    cuda_stats.launch_linear,
                    cuda_stats.launch_linear_qkv,
                },
            );
            try appendFmt(
                allocator,
                &cuda_generate_out,
                \\"decoder_runtime_linear_slot_prepares":{d},
                \\"decoder_runtime_linear_slot_prepare_misses":{d},
                \\"decoder_runtime_rms_norm_slot_prepares":{d},
                \\"decoder_runtime_rms_norm_slot_prepare_misses":{d},
                \\"decoder_runtime_linear_apply_hits":{d},
                \\"decoder_runtime_linear_apply_misses":{d},
                \\"decoder_runtime_linear_pair_apply_hits":{d},
                \\"decoder_runtime_linear_qkv_apply_hits":{d},
                \\"decoder_runtime_rms_norm_apply_hits":{d},
                \\"decoder_runtime_rms_norm_apply_misses":{d},
                \\"decoder_runtime_attention_residual_attempts":{d},
                \\"decoder_runtime_attention_residual_hits":{d},
                \\"decoder_runtime_attention_residual_misses":{d},
                \\"decoder_runtime_gated_ffn_attempts":{d},
                \\"decoder_runtime_gated_ffn_hits":{d},
                \\"decoder_runtime_gated_ffn_misses":{d},
                \\"decoder_runtime_pinned_eviction_skips":{d},
                \\"decode_profile_events":{d},
                \\"decode_profile_qkv_us":{d},
                \\"decode_profile_gqa_attention_us":{d},
                \\"decode_profile_attention_output_us":{d},
                \\"decode_profile_attention_norm_residual_us":{d},
                \\"decode_profile_ffn_gate_up_us":{d},
                \\"decode_profile_ffn_gated_down_us":{d},
                \\"decode_profile_ffn_post_norm_us":{d},
                \\"decode_profile_lm_head_argmax_us":{d},
                \\"decode_profile_graph_replay_us":{d},
                \\
            ,
                .{
                    cuda_stats.decoder_runtime_linear_slot_prepares,
                    cuda_stats.decoder_runtime_linear_slot_prepare_misses,
                    cuda_stats.decoder_runtime_rms_norm_slot_prepares,
                    cuda_stats.decoder_runtime_rms_norm_slot_prepare_misses,
                    cuda_stats.decoder_runtime_linear_apply_hits,
                    cuda_stats.decoder_runtime_linear_apply_misses,
                    cuda_stats.decoder_runtime_linear_pair_apply_hits,
                    cuda_stats.decoder_runtime_linear_qkv_apply_hits,
                    cuda_stats.decoder_runtime_rms_norm_apply_hits,
                    cuda_stats.decoder_runtime_rms_norm_apply_misses,
                    cuda_stats.decoder_runtime_attention_residual_attempts,
                    cuda_stats.decoder_runtime_attention_residual_hits,
                    cuda_stats.decoder_runtime_attention_residual_misses,
                    cuda_stats.decoder_runtime_gated_ffn_attempts,
                    cuda_stats.decoder_runtime_gated_ffn_hits,
                    cuda_stats.decoder_runtime_gated_ffn_misses,
                    cuda_stats.decoder_runtime_pinned_eviction_skips,
                    cuda_stats.decode_profile_events,
                    cuda_stats.decode_profile_qkv_us,
                    cuda_stats.decode_profile_gqa_attention_us,
                    cuda_stats.decode_profile_attention_output_us,
                    cuda_stats.decode_profile_attention_norm_residual_us,
                    cuda_stats.decode_profile_ffn_gate_up_us,
                    cuda_stats.decode_profile_ffn_gated_down_us,
                    cuda_stats.decode_profile_ffn_post_norm_us,
                    cuda_stats.decode_profile_lm_head_argmax_us,
                    cuda_stats.decode_profile_graph_replay_us,
                },
            );
            try appendFmt(
                allocator,
                &cuda_generate_out,
                \\"prefill_profile_events":{d},
                \\"prefill_profile_q4_linear_us":{d},
                \\"prefill_profile_q4_qkv_us":{d},
                \\"prefill_profile_q4_pair_us":{d},
                \\"prefill_profile_q4_gated_down_us":{d},
                \\"prefill_profile_bf16_linear_us":{d},
                \\"prefill_profile_bf16_qkv_us":{d},
                \\"prefill_profile_bf16_pair_us":{d},
                \\"prefill_profile_attention_us":{d},
                \\"prefill_profile_ple_dense_us":{d},
                \\"prefill_profile_staging_us":{d},
                \\"prefill_profile_norm_us":{d},
                \\
            ,
                .{
                    cuda_stats.prefill_profile_events,
                    cuda_stats.prefill_profile_q4_linear_us,
                    cuda_stats.prefill_profile_q4_qkv_us,
                    cuda_stats.prefill_profile_q4_pair_us,
                    cuda_stats.prefill_profile_q4_gated_down_us,
                    cuda_stats.prefill_profile_bf16_linear_us,
                    cuda_stats.prefill_profile_bf16_qkv_us,
                    cuda_stats.prefill_profile_bf16_pair_us,
                    cuda_stats.prefill_profile_attention_us,
                    cuda_stats.prefill_profile_ple_dense_us,
                    cuda_stats.prefill_profile_staging_us,
                    cuda_stats.prefill_profile_norm_us,
                },
            );
            try appendFmt(
                allocator,
                &cuda_generate_out,
                \\"launch_norm":{d},
                \\"launch_norm_layer":{d},
                \\"launch_norm_add_layer":{d},
                \\"launch_norm_rms":{d},
                \\"launch_norm_rms_add":{d},
                \\"launch_norm_rms_add_mul_scalar":{d},
                \\"launch_norm_rms_add_output_scale":{d},
                \\"launch_norm_rms_bare":{d},
                \\"launch_norm_head_rope":{d},
                \\"launch_attention":{d},
                \\"launch_attention_gqa_decode":{d},
                \\"launch_attention_gqa_decode_fast":{d},
                \\"launch_attention_gqa_decode_fast_fallbacks":{d},
                \\"launch_attention_gqa_prefill_fast":{d},
                \\"launch_attention_gqa_prefill_tiled":{d},
                \\"launch_attention_gqa_prefill_mma":{d},
                \\"launch_attention_gqa_prefill_mma_m32":{d},
                \\"launch_attention_gqa_scalar":{d},
                \\"launch_elementwise":{d},
                \\"launch_scalar":{d},
                \\"launch_argmax":{d},
                \\"lm_head_argmax_fused_q8":{d},
                \\"lm_head_argmax_fused_q4_0":{d},
                \\"lm_head_argmax_fused_q4":{d},
                \\"lm_head_argmax_fused_q6":{d},
                \\"lm_head_argmax_fallbacks":{d},
                \\"mtp_verify_commit_device_hits":{d},
                \\"mtp_verify_commit_device_fallbacks":{d},
                \\"mtp_verify_commit_result_downloads":{d},
                \\"mtp_verify_commit_choice_downloads":{d},
                \\
            ,
                .{
                    cuda_stats.launch_norm,
                    cuda_stats.launch_norm_layer,
                    cuda_stats.launch_norm_add_layer,
                    cuda_stats.launch_norm_rms,
                    cuda_stats.launch_norm_rms_add,
                    cuda_stats.launch_norm_rms_add_mul_scalar,
                    cuda_stats.launch_norm_rms_add_output_scale,
                    cuda_stats.launch_norm_rms_bare,
                    cuda_stats.launch_norm_head_rope,
                    cuda_stats.launch_attention,
                    cuda_stats.launch_attention_gqa_decode,
                    cuda_stats.launch_attention_gqa_decode_fast,
                    cuda_stats.launch_attention_gqa_decode_fast_fallbacks,
                    cuda_stats.launch_attention_gqa_prefill_fast,
                    cuda_stats.launch_attention_gqa_prefill_tiled,
                    cuda_stats.launch_attention_gqa_prefill_mma,
                    cuda_stats.launch_attention_gqa_prefill_mma_m32,
                    cuda_stats.launch_attention_gqa_scalar,
                    cuda_stats.launch_elementwise,
                    cuda_stats.launch_scalar,
                    cuda_stats.launch_argmax,
                    cuda_stats.lm_head_argmax_fused_q8,
                    cuda_stats.lm_head_argmax_fused_q4_0,
                    cuda_stats.lm_head_argmax_fused_q4,
                    cuda_stats.lm_head_argmax_fused_q6,
                    cuda_stats.lm_head_argmax_fallbacks,
                    cuda_stats.mtp_verify_commit_device_hits,
                    cuda_stats.mtp_verify_commit_device_fallbacks,
                    cuda_stats.mtp_verify_commit_result_downloads,
                    cuda_stats.mtp_verify_commit_choice_downloads,
                },
            );
            try appendFmt(
                allocator,
                &cuda_generate_out,
                \\"activation_multiply_fused":{d},
                \\"linear_activation_slice_fused_q4_0":{d},
                \\"add_mul_scalar_fused":{d},
                \\"rms_norm_add_fused":{d},
                \\"rms_norm_add_output_scale_fused":{d},
                \\"rms_norm_add_weighted_embedding_fused_q6_k":{d},
                \\"rms_norm_add_output_scale_fallbacks":{d},
                \\"gated_down_fused_q8":{d},
                \\"gated_down_fused_q4_0":{d},
                \\"gated_down_fused_q4_0_precompute":{d},
                \\"gated_down_fused_q4_0_tile4":{d},
                \\"gated_down_fused_q4_0_tile8":{d},
                \\"gated_down_fused_q4_0_tile16":{d},
                \\"gated_down_fused_q4":{d},
                \\"gated_down_fallbacks":{d},
                \\"deferred_free_queued":{d},
                \\"deferred_free_drains":{d},
                \\"deferred_free_forced_drains":{d},
                \\"deferred_free_reclaimed_bytes":{d}
                \\}}
            ,
                .{
                    cuda_stats.activation_multiply_fused,
                    cuda_stats.linear_activation_slice_fused_q4_0,
                    cuda_stats.add_mul_scalar_fused,
                    cuda_stats.rms_norm_add_fused,
                    cuda_stats.rms_norm_add_output_scale_fused,
                    cuda_stats.rms_norm_add_weighted_embedding_fused_q6_k,
                    cuda_stats.rms_norm_add_output_scale_fallbacks,
                    cuda_stats.gated_down_fused_q8,
                    cuda_stats.gated_down_fused_q4_0,
                    cuda_stats.gated_down_fused_q4_0_precompute,
                    cuda_stats.gated_down_fused_q4_0_tile4,
                    cuda_stats.gated_down_fused_q4_0_tile8,
                    cuda_stats.gated_down_fused_q4_0_tile16,
                    cuda_stats.gated_down_fused_q4,
                    cuda_stats.gated_down_fallbacks,
                    cuda_stats.deferred_free_queued,
                    cuda_stats.deferred_free_drains,
                    cuda_stats.deferred_free_forced_drains,
                    cuda_stats.deferred_free_reclaimed_bytes,
                },
            );
            break :blk try cuda_generate_out.toOwnedSlice(allocator);
        }
        break :blk try allocator.dupe(u8, "null");
    } else try allocator.dupe(u8, "null");
    defer allocator.free(cuda_generate_json);

    const draft_cuda_json = if (comptime build_options.enable_cuda)
        try cudaStatsCompactJson(allocator, draft_cuda_stats_opt, result.tokens_used)
    else
        try allocator.dupe(u8, "null");
    defer allocator.free(draft_cuda_json);

    const draft_cuda_generate_json = if (comptime build_options.enable_cuda)
        try cudaStatsCompactJson(allocator, draft_cuda_generate_stats_opt, result.tokens_used)
    else
        try allocator.dupe(u8, "null");
    defer allocator.free(draft_cuda_generate_json);

    const runtime_json = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\"decode_greedy_calls":{d},
        \\"decode_greedy_device_token_handoff_attempts":{d},
        \\"decode_greedy_device_token_handoff_hits":{d},
        \\"decode_greedy_device_token_handoff_fallbacks":{d},
        \\"decode_greedy_device_token_seeds":{d}
        \\}}
    ,
        .{
            runtime_stats.decode_greedy_calls,
            runtime_stats.decode_greedy_device_token_handoff_attempts,
            runtime_stats.decode_greedy_device_token_handoff_hits,
            runtime_stats.decode_greedy_device_token_handoff_fallbacks,
            runtime_stats.decode_greedy_device_token_seeds,
        },
    );
    defer allocator.free(runtime_json);

    const generation_decoder_runtime_json = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\"forward_attempts":{d},
        \\"backend_not_device_decode":{d},
        \\"graph_blocked":{d},
        \\"kv_missing":{d},
        \\"non_greedy":{d},
        \\"grammar_blocked":{d},
        \\"device_token_handoff_attempts":{d},
        \\"device_token_handoff_hits":{d},
        \\"device_token_handoff_fallbacks":{d},
        \\"device_token_handoff_seeds":{d}
        \\}}
    ,
        .{
            decoder_debug_stats.forward_attempts,
            decoder_debug_stats.backend_not_device_decode,
            decoder_debug_stats.graph_blocked,
            decoder_debug_stats.kv_missing,
            decoder_debug_stats.non_greedy,
            decoder_debug_stats.grammar_blocked,
            decoder_debug_stats.device_token_handoff_attempts,
            decoder_debug_stats.device_token_handoff_hits,
            decoder_debug_stats.device_token_handoff_fallbacks,
            decoder_debug_stats.device_token_handoff_seeds,
        },
    );
    defer allocator.free(generation_decoder_runtime_json);

    const token_ids_json = try tokenIdsJson(allocator, result.token_ids);
    defer allocator.free(token_ids_json);

    const json = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\"model_dir":{f},
        \\"backend":{f},
        \\"tokens":{d},
        \\"token_ids":{s},
        \\"finish_reason":{f},
        \\"decode_tok_per_s":{d:.6},
        \\"timing_ms":{{
        \\"load_model":{d},
        \\"prompt_prep":{d},
        \\"scheduler":{d},
        \\"backend_setup":{d},
        \\"decode_setup":{d},
        \\"generate":{d},
        \\"total":{d},
        \\"prompt_format_inner":{d},
        \\"tokenize_inner":{d},
        \\"runtime_prepare_inner":{d},
        \\"prefill_inner":{d},
        \\"decode_inner":{d},
        \\"text_decode_inner":{d},
        \\"total_inner":{d}
        \\}},
        \\"runtime":{s},
        \\"generation_decoder_runtime":{s},
        \\"speculative":{s},
        \\"cuda":{s},
        \\"cuda_generate":{s},
        \\"draft_cuda":{s},
        \\"draft_cuda_generate":{s}
        \\}}
        \\
    ,
        .{
            std.json.fmt(model_dir, .{}),
            std.json.fmt(backend_name, .{}),
            result.tokens_used,
            token_ids_json,
            std.json.fmt(result.finish_reason, .{}),
            tokensPerSecond(result.tokens_used, decode_ms),
            load_model_ms,
            prompt_prep_ms,
            scheduler_ms,
            backend_setup_ms,
            decode_setup_ms,
            generate_ms,
            total_ms,
            inner_timing.prompt_format,
            inner_timing.tokenize,
            inner_timing.runtime_prepare,
            inner_timing.prefill,
            inner_timing.decode,
            inner_timing.text_decode,
            inner_timing.total,
            runtime_json,
            generation_decoder_runtime_json,
            speculative_json,
            cuda_json,
            cuda_generate_json,
            draft_cuda_json,
            draft_cuda_generate_json,
        },
    );
    defer allocator.free(json);
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = json });
}

fn writeLiveWholeModelJsonTiming(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    model_dir: []const u8,
    backend_name: []const u8,
    tokens: usize,
    finish_reason: []const u8,
    load_model_ms: u64,
    prompt_prep_ms: u64,
    backend_setup_ms: u64,
    runtime_prewarm_ms: u64,
    generate_ms: u64,
    total_ms: u64,
    prefill_ms: u64,
    decode_ms: u64,
) !void {
    const json = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\"model_dir":{f},
        \\"backend":{f},
        \\"tokens":{d},
        \\"finish_reason":{f},
        \\"decode_tok_per_s":{d:.6},
        \\"timing_ms":{{
        \\"load_model":{d},
        \\"prompt_prep":{d},
        \\"scheduler":0,
        \\"backend_setup":{d},
        \\"runtime_prewarm":{d},
        \\"decode_setup":0,
        \\"generate":{d},
        \\"total":{d},
        \\"prefill_inner":{d},
        \\"decode_inner":{d},
        \\"total_inner":{d}
        \\}}
        \\}}
        \\
    ,
        .{
            std.json.fmt(model_dir, .{}),
            std.json.fmt(backend_name, .{}),
            tokens,
            std.json.fmt(finish_reason, .{}),
            tokensPerSecond(tokens, decode_ms),
            load_model_ms,
            prompt_prep_ms,
            backend_setup_ms,
            runtime_prewarm_ms,
            generate_ms,
            total_ms,
            prefill_ms,
            decode_ms,
            prefill_ms + decode_ms,
        },
    );
    defer allocator.free(json);
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = json });
}

fn printGpuHostedTimingDetails(cb_opt: ?*const ops.ComputeBackend) void {
    if (!build_options.enable_metal) {
        return;
    }
    const backend_stats = if (cb_opt) |cb| cb.debugTimingSnapshot() else debug_timing.fallbackGpuTimingSnapshot();
    const backend_kind: ops.BackendKind = if (cb_opt) |cb| cb.kind() else .metal;
    const decoder_runtime_runtime_ready = if (cb_opt) |cb| cb.decoderRuntimeReady() else false;
    const decoder_runtime_embeddings_prepared = if (cb_opt) |cb| cb.decoderRuntimeAbsoluteEmbeddingsPrepared() else false;
    debug_timing.printBackendTimingDetails(
        backend_kind,
        backend_stats,
        decoder_runtime_runtime_ready,
        decoder_runtime_embeddings_prepared,
    );
}

fn printMetalQuantDispatchSummary(metal_snapshot: ops.BackendDebugTimingSnapshot) void {
    print(
        "metal_q8_0_dispatch: scalar={d} mmv={d} small_batch={d} mm={d} rows_1={d} rows_2_8={d} rows_9_64={d} rows_65_plus={d} pair_act_mm_out_f16={d} linear_mm_in_f16={d} pair_act_rms_mmv_out_f16={d} linear_mmv_in_f16={d}\n",
        .{
            metal_snapshot.provider.metal_runtime_q8_0_linear_dispatch_scalar,
            metal_snapshot.provider.metal_runtime_q8_0_linear_dispatch_mmv,
            metal_snapshot.provider.metal_runtime_q8_0_linear_dispatch_small_batch,
            metal_snapshot.provider.metal_runtime_q8_0_linear_dispatch_mm,
            metal_snapshot.provider.metal_runtime_q8_0_linear_rows_1,
            metal_snapshot.provider.metal_runtime_q8_0_linear_rows_2_8,
            metal_snapshot.provider.metal_runtime_q8_0_linear_rows_9_64,
            metal_snapshot.provider.metal_runtime_q8_0_linear_rows_65_plus,
            metal_snapshot.provider.metal_runtime_q8_0_pair_activation_mm_f16_output,
            metal_snapshot.provider.metal_runtime_q8_0_linear_mm_f16_input,
            metal_snapshot.provider.metal_runtime_q8_0_pair_activation_rms_scale_mmv_f16_output,
            metal_snapshot.provider.metal_runtime_q8_0_linear_mmv_f16_input,
        },
    );
    print(
        "metal_attention_dispatch: paged_1x={d}\n",
        .{metal_snapshot.provider.metal_runtime_paged_attention_1x_calls},
    );
    print(
        "metal_q4_0_dispatch: linear_reduce={d} linear_reduce_in_f16={d} linear_reduce_out_f16={d} linear_reduce_in_f16_out_f16={d} linear_reduce_sumsq={d} pair_act_reduce={d} pair_act_reduce_out_f16={d} pair_act_rms_scale_reduce_out_f16={d} activation_rhs_reduce={d} activation_rhs_reduce_out_f16={d} rms_norm_add_sumsq={d} pair_reduce={d} pair={d}\n",
        .{
            metal_snapshot.provider.metal_runtime_q4_0_linear_reduce,
            metal_snapshot.provider.metal_runtime_q4_0_linear_reduce_f16_input,
            metal_snapshot.provider.metal_runtime_q4_0_linear_reduce_f16_output,
            metal_snapshot.provider.metal_runtime_q4_0_linear_reduce_f16_input_f16_output,
            metal_snapshot.provider.metal_runtime_q4_0_linear_reduce_sumsq,
            metal_snapshot.provider.metal_runtime_q4_0_pair_activation_reduce,
            metal_snapshot.provider.metal_runtime_q4_0_pair_activation_reduce_f16_output,
            metal_snapshot.provider.metal_runtime_q4_0_pair_activation_rms_scale_reduce_f16_output,
            metal_snapshot.provider.metal_runtime_q4_0_activation_rhs_reduce,
            metal_snapshot.provider.metal_runtime_q4_0_activation_rhs_reduce_f16_output,
            metal_snapshot.provider.metal_runtime_rms_norm_add_sumsq,
            metal_snapshot.provider.metal_runtime_q4_0_pair_reduce,
            metal_snapshot.provider.metal_runtime_q4_0_pair,
        },
    );
    print(
        "metal_q4_q6_k_dispatch: q4_linear_reduce={d} q4_pair_reduce={d} q4_pair_act_reduce={d} q4_pair_act_reduce_out_f16={d} q4_activation_rhs_reduce={d} q6_linear_reduce={d} q6_linear_reduce_in_f16={d}\n",
        .{
            metal_snapshot.provider.metal_runtime_q4_k_linear_reduce,
            metal_snapshot.provider.metal_runtime_q4_k_pair_reduce,
            metal_snapshot.provider.metal_runtime_q4_k_pair_activation_reduce,
            metal_snapshot.provider.metal_runtime_q4_k_pair_activation_reduce_f16_output,
            metal_snapshot.provider.metal_runtime_q4_k_activation_rhs_reduce,
            metal_snapshot.provider.metal_runtime_q6_k_linear_reduce,
            metal_snapshot.provider.metal_runtime_q6_k_linear_reduce_f16_input,
        },
    );
    print(
        "metal_q4_0_ple_dispatch: activation_rhs_reduce_out_f16={d} linear_reduce_in_f16={d}\n",
        .{
            metal_snapshot.provider.metal_runtime_q4_0_ple_activation_rhs_reduce_f16_output,
            metal_snapshot.provider.metal_runtime_q4_0_ple_linear_reduce_f16_input,
        },
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

fn gemmaPrefillPrewarmEnabled() bool {
    if (envFlagEnabled("TERMITE_METAL_DISABLE_GEMMA_PREFILL_PREWARM")) return false;
    return envFlagEnabled("TERMITE_METAL_ENABLE_GEMMA_PREFILL_PREWARM");
}

fn prefillGreedyTokenEnabled() bool {
    return !envFlagEnabled("TERMITE_METAL_DISABLE_PREFILL_GREEDY_TOKEN");
}

fn runtimeTokenDecodeEnabled() bool {
    return !envFlagEnabled("TERMITE_METAL_DISABLE_RUNTIME_TOKEN_DECODE");
}

fn forcePrefillHostLogits() bool {
    return envFlagEnabled("TERMITE_METAL_FORCE_PREFILL_HOST_LOGITS");
}

fn traceGenerateTopLogitsEnabled() bool {
    return envFlagEnabled("TERMITE_METAL_TRACE_GENERATE_TOP_LOGITS");
}

fn traceGenerateTopLogits(label: []const u8, step: usize, logits: []const f32) void {
    if (!traceGenerateTopLogitsEnabled()) return;
    var top_ids = [_]usize{0} ** 8;
    var top_vals = [_]f32{-std.math.inf(f32)} ** 8;
    for (logits, 0..) |logit, idx| {
        var insert_at: ?usize = null;
        for (top_vals, 0..) |current, slot| {
            if (logit > current) {
                insert_at = slot;
                break;
            }
        }
        if (insert_at) |slot| {
            var i: usize = top_vals.len - 1;
            while (i > slot) : (i -= 1) {
                top_vals[i] = top_vals[i - 1];
                top_ids[i] = top_ids[i - 1];
            }
            top_vals[slot] = logit;
            top_ids[slot] = idx;
        }
    }
    std.debug.print("generate_top_logits step={d} label={s}:", .{ step, label });
    for (top_ids, top_vals) |id, value| {
        std.debug.print(" {d}:{d:.6}", .{ id, value });
    }
    std.debug.print("\n", .{});
}

fn kvSlidingTrimForced() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_KV_SLIDING_TRIM", false);
}

fn cudaGemmaPrefillPrewarmEnabled() bool {
    if (envFlagEnabled("ANTFLY_INFERENCE_DISABLE_GEMMA_PREFILL_PREWARM")) return false;
    if (envFlagEnabled("ANTFLY_INFERENCE_CUDA_DISABLE_GEMMA_PREFILL_PREWARM")) return false;
    return envFlagEnabled("ANTFLY_INFERENCE_GEMMA_PREFILL_PREWARM") or
        envFlagEnabled("ANTFLY_INFERENCE_CUDA_GEMMA_PREFILL_PREWARM");
}

fn prewarmCudaGemmaPrefillResidency(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    prompt_tokens: usize,
) !bool {
    if (cb.kind() != .cuda or gpt_config.family != .gemma or gpt_config.usesMoe()) return false;
    const configured_layer_count: usize = @intCast(gpt_config.num_hidden_layers);
    const prepare = try cb.decoderRuntimePrepareOrReuseFamily(
        allocator,
        gpt_config,
        prompt_tokens,
        configured_layer_count,
    );
    if (!prepare.prepared) return false;

    const embedding = try gpt_arch.getEmbeddingWeight(cb, gpt_config);
    defer cb.free(embedding);

    const final_norm = gpt_arch.getModelWeight(cb, gpt_config, "model.norm.weight") catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => null,
        else => return err,
    };
    defer if (final_norm) |weight| cb.free(weight);

    const lm_head = try gpt_arch.getLmHeadWeight(cb, gpt_config);
    defer cb.free(lm_head);
    return true;
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

fn liveWholeModelDeclineError(err: anyerror) bool {
    return switch (err) {
        error.UnsupportedOperation,
        error.UnsupportedTensorType,
        error.UnsupportedShape,
        error.UnsupportedPrimitiveOp,
        error.UnsupportedBackend,
        error.UnsupportedCompileBackend,
        => true,
        else => false,
    };
}

fn liveWholeModelExecutorRequested(opts: *const Options) bool {
    if (envFlagEnabled("TERMITE_METAL_DISABLE_LIVE_WHOLE_MODEL_EXECUTOR")) return false;
    const explicit_whole_model = opts.mode != null and opts.mode.? == .compiled and
        opts.compiled_target != null and opts.compiled_target.? == .whole_model;
    if (explicit_whole_model) return false;
    if (!explicit_whole_model and opts.backend != .metal) return false;
    return switch (opts.backend) {
        .auto, .native, .metal => true,
        else => false,
    };
}

fn runLiveWholeModelExecutorReuseProbe(
    allocator: std.mem.Allocator,
    io: std.Io,
    model: *model_manager_mod.LoadedModel,
    gpt_config: @import("models/gpt.zig").Config,
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
            .force_host_logits = forcePrefillHostLogits(),
            .prefer_greedy_token = prefillGreedyTokenEnabled() and chunk_end == prompt_ids.len,
        });
        processed = chunk_end;
    }
    const finished_prefill_at = std.Io.Timestamp.now(io, .awake);
    var first_token_at = finished_prefill_at;
    if (runtime_model.capabilities().supports_greedy_decode) {
        _ = try output_accum.?.greedyToken(allocator, gpt_config.vocab_size);
        first_token_at = std.Io.Timestamp.now(io, .awake);
    }
    if (output_accum) |*owned| owned.deinit(allocator);
    const finished_at = first_token_at;

    print(
        "metal_executor_reuse_ms: backend_setup={d} prefill={d} first_token={d} total={d}\n",
        .{
            durationMillis(started_at, created_runtime_at),
            durationMillis(created_runtime_at, finished_prefill_at),
            durationMillis(finished_prefill_at, first_token_at),
            durationMillis(started_at, finished_at),
        },
    );
    print(
        "metal_executor_reuse_first_token_ms: service={d} prefill={d} sample={d}\n",
        .{
            durationMillis(created_runtime_at, first_token_at),
            durationMillis(created_runtime_at, finished_prefill_at),
            durationMillis(finished_prefill_at, first_token_at),
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
    if (config.draft_model != null) return false;
    if (opts.image_count > 0 or opts.audio_count > 0) return false;

    gpt_arch.resetDebugTimingStats();

    const kv_backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .metal => .metal,
        .native => .native,
        else => .native,
    };
    const kv_dtype = if (opts.cache_dtype) |name|
        runtime.kv.pool.parseKvDType(name) orelse return error.InvalidCacheDType
    else
        session_factory.recommendedKvDTypeForSession(model.session, kv_backend_kind);
    var executor = (model.wholeModelExecutor(allocator, kv_dtype) catch |err| {
        debugGenerateSetup("live whole-model executor unavailable err={s}", .{@errorName(err)});
        if (liveWholeModelDeclineError(err)) return false;
        return err;
    }) orelse return false;
    defer executor.deinit();

    var runtime_model = executor.createRuntime(allocator) catch |err| {
        debugGenerateSetup("live whole-model runtime unavailable err={s}", .{@errorName(err)});
        if (liveWholeModelDeclineError(err)) return false;
        return err;
    };
    defer runtime_model.deinit();
    if (opts.print_timing and model.session.backend().usesGpuHostedSession()) {
        runtime_model.resetDebugTimingStats();
    }
    const created_runtime_at = std.Io.Timestamp.now(io, .awake);

    const prompt_ids = try allocator.alloc(i64, prompt_tokens);
    defer allocator.free(prompt_ids);
    for (prompt_token_ids, 0..) |token_id, idx| prompt_ids[idx] = token_id;
    if (prompt_ids.len == 0) return error.EmptyPrompt;

    var warmed_runtime_at = created_runtime_at;
    var runtime_prewarm_ms: u64 = 0;
    if (build_options.enable_metal and
        gpt_config.family == .gemma and
        model.session.backend().usesGpuHostedSession() and
        gemmaPrefillPrewarmEnabled())
    {
        const prewarm_started_at = std.Io.Timestamp.now(io, .awake);
        const prewarm_ok = runtime_model.prepare(allocator, .{
            .kv_tokens_hint = prompt_ids.len,
        }) catch |err| blk: {
            std.log.warn("Gemma4 Metal runtime prewarm failed for {s}: {s}", .{ opts.model_dir, @errorName(err) });
            break :blk false;
        };
        warmed_runtime_at = std.Io.Timestamp.now(io, .awake);
        runtime_prewarm_ms = durationMillis(prewarm_started_at, warmed_runtime_at);
        if (!prewarm_ok) {
            std.log.warn("Gemma4 Metal runtime prewarm declined for {s}", .{opts.model_dir});
        }
        if (opts.print_timing) {
            runtime_model.resetDebugTimingStats();
        }
    }
    const runtime_caps = runtime_model.capabilities();

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
    const use_runtime_token_decode = runtimeTokenDecodeEnabled();
    const use_greedy_decode = use_runtime_token_decode and runtime_caps.supports_greedy_decode and isPureGreedyConfig(config);
    const use_sample_decode = use_runtime_token_decode and runtime_caps.supports_sample_decode and !use_greedy_decode;
    const prefer_prefill_greedy_token = prefillGreedyTokenEnabled() and use_greedy_decode;
    var prefill_chunk_size = if (config.prefill_chunk_size > 0) config.prefill_chunk_size else prompt_ids.len;
    prefill_chunk_size = @max(@min(prefill_chunk_size, prompt_ids.len), 1);
    const prefill_started_at = std.Io.Timestamp.now(io, .awake);
    var output = blk: {
        var processed: usize = 0;
        var output_accum: ?graph_mod.model_runtime.ModelOutput = null;
        errdefer if (output_accum) |*owned| owned.deinit(allocator);

        while (processed < prompt_ids.len) {
            const chunk_end = @min(prompt_ids.len, processed + prefill_chunk_size);
            if (output_accum) |*owned| owned.deinit(allocator);
            output_accum = runtime_model.prefill(allocator, .{
                .input_ids = prompt_ids[processed..chunk_end],
                .seq_len = chunk_end,
                .query_seq_len = chunk_end - processed,
                .attention_mode = .paged_prefill,
                .force_host_logits = forcePrefillHostLogits(),
                .prefer_greedy_token = prefer_prefill_greedy_token and chunk_end == prompt_ids.len,
            }) catch |err| {
                debugGenerateSetup("live whole-model prefill unavailable err={s}", .{@errorName(err)});
                if (liveWholeModelDeclineError(err)) return false;
                return err;
            };
            processed = chunk_end;
        }
        break :blk output_accum.?;
    };
    const finished_prefill_at = std.Io.Timestamp.now(io, .awake);
    defer output.deinit(allocator);

    var generated: usize = 0;
    var first_token_at: ?std.Io.Timestamp = null;
    while (generated < max_tokens) {
        const next_token_i32: i32 = if (generated == 0) blk: {
            if (use_greedy_decode) {
                break :blk @intCast(try output.greedyToken(allocator, gpt_config.vocab_size));
            }
            const output_logits = try output.hostLogits(allocator);
            traceGenerateTopLogits("prefill", generated, output_logits);
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
            traceGenerateTopLogits("decode", generated, output_logits);
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
        if (generated == 1) first_token_at = std.Io.Timestamp.now(io, .awake);

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
    const load_model_ms = durationMillis(started_at, loaded_model_at);
    const prompt_prep_ms = durationMillis(loaded_model_at, encoded_prompt_at);
    const backend_setup_ms = durationMillis(encoded_prompt_at, created_runtime_at);
    const generate_ms = durationMillis(warmed_runtime_at, finished_generate_at);
    const total_ms = durationMillis(started_at, finished_generate_at);
    const prefill_ms = durationMillis(prefill_started_at, finished_prefill_at);
    const decode_ms = durationMillis(finished_prefill_at, finished_generate_at);
    const first_token_value_at = first_token_at orelse finished_generate_at;
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
            "timing_ms: load_model={d} prompt_prep={d} scheduler=0 backend_setup={d} runtime_prewarm={d} decode_setup=0 generate={d} total={d}\n",
            .{
                load_model_ms,
                prompt_prep_ms,
                backend_setup_ms,
                runtime_prewarm_ms,
                generate_ms,
                total_ms,
            },
        );
        print(
            "first_token_ms: request={d} service={d} prefill={d} sample={d}\n",
            .{
                durationMillis(started_at, first_token_value_at),
                durationMillis(warmed_runtime_at, first_token_value_at),
                durationMillis(prefill_started_at, finished_prefill_at),
                durationMillis(finished_prefill_at, first_token_value_at),
            },
        );
        print("decode_tok_per_s={d:.3}\n", .{tokensPerSecond(result_token_ids.len, decode_ms)});
        if (model.session.backend().usesGpuHostedSession()) {
            printLiveWholeModelExecutorDetails(&runtime_model);
            if (metalExecutorReuseProbeEnabled()) {
                try runLiveWholeModelExecutorReuseProbe(
                    allocator,
                    io,
                    model,
                    gpt_config,
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
            printMetalQuantDispatchSummary(metal_snapshot);
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
                "metal_active_decode_kernels: attention_f32={d} quant_linear={d} quant_attn_linear={d} quant_ffn_down={d} quant_ple={d} quant_gate_up_pair={d} rms_norm={d} rms_norm_add={d} layer_norm={d} add={d} head_norm_rope_fused={d} blit={d}\n",
                .{
                    metal_snapshot.provider.active_decode_attention_f32_kernels,
                    metal_snapshot.provider.active_decode_quant_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_attention_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_ffn_down_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_ple_linear_kernels,
                    metal_snapshot.provider.active_decode_quant_gate_up_pair_kernels,
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
    if (opts.json_timing_path) |path| {
        try writeLiveWholeModelJsonTiming(
            allocator,
            io,
            path,
            opts.model_dir,
            @tagName(model.session.backend()),
            result_token_ids.len,
            finish_reason,
            load_model_ms,
            prompt_prep_ms,
            backend_setup_ms,
            runtime_prewarm_ms,
            generate_ms,
            total_ms,
            prefill_ms,
            decode_ms,
        );
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
        else => .gpu,
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
    else if (gpt_config.sliding_window > 0 and gpt_config.hasGlobalAttentionLayers() and !kvSlidingTrimForced())
        // Mixed attention (iSWA-style models like Gemma): global layers need
        // the full KV history, and the pool packs every layer's KV into
        // shared blocks, so window-trimming the pool silently truncates the
        // global layers' context. Retain everything; sliding-window layers
        // still apply their exact window inside the attention kernels, so
        // their compute stays bounded — only KV memory grows with context.
        // ANTFLY_INFERENCE_KV_SLIDING_TRIM=1 restores the old
        // trim-to-window behavior (lower memory, truncated global context).
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
    var kv_storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, .{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });
    defer kv_storage.deinit();
    try cb.provisionKvDeviceWriteHook(&kv_storage);
    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    decode_state.kv_storage = &kv_storage;
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
    var result = try generateWithOptionalStreaming(&pipeline, messages, config, opts.stream);
    const finished_generate_at = std.Io.Timestamp.now(io, .awake);
    defer result.deinit();

    if (!opts.stream) print("{s}\n", .{result.text});
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

const CliStreamPrinter = struct {
    wrote_text: bool = false,

    fn onToken(raw_ctx: *anyopaque, token_text: []const u8) bool {
        const self: *@This() = @ptrCast(@alignCast(raw_ctx));
        if (token_text.len > 0) {
            print("{s}", .{token_text});
            self.wrote_text = true;
        }
        return true;
    }
};

fn generateWithOptionalStreaming(
    pipeline: anytype,
    messages: []const generation.Message,
    config: generation.GenerationConfig,
    stream: bool,
) !generation.GenerationResult {
    if (!stream) return pipeline.generate(messages, config);

    var stream_printer = CliStreamPrinter{};
    var result = try pipeline.generateStreaming(
        messages,
        config,
        @ptrCast(&stream_printer),
        CliStreamPrinter.onToken,
    );
    errdefer result.deinit();
    if (!stream_printer.wrote_text and result.text.len > 0) {
        print("{s}", .{result.text});
    }
    print("\n", .{});
    return result;
}

fn runServerGenerate(allocator: std.mem.Allocator, io: std.Io, opts: Options, quiet_errors: bool) !void {
    if (!serverGenerateSupportsOptions(opts)) {
        return error.UnsupportedServerGenerateOption;
    }

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http = httpx.Client.init(allocator, io_impl.io());
    defer http.deinit();

    const url = try generateEndpointUrl(allocator, opts.server_url.?);
    defer allocator.free(url);

    const messages = [_]api.ChatMessage{.{
        .role = .user,
        .content = .{ .string = opts.prompt },
    }};
    const request = api.GenerateRequest{
        .model = opts.model_dir,
        .messages = &messages,
        .max_tokens = opts.max_tokens,
        .temperature = opts.temperature,
        .top_p = opts.top_p,
        .top_k = opts.top_k,
        .repetition_penalty = opts.repetition_penalty,
        .stream = if (opts.stream) true else null,
        .cache_dtype = opts.cache_dtype,
        .cache_compaction_ratio = opts.cache_compaction_ratio,
        .backend = generateBackendOverrideForChoice(opts.backend),
        .mode = serverGenerateModeName(opts),
        .compiled_target = serverGenerateCompiledTargetName(opts),
    };
    const body = try httpx.json.Json.stringify(allocator, request);
    defer allocator.free(body);

    if (opts.stream) {
        return try runServerGenerateStream(allocator, io, &http, url, body, opts, quiet_errors);
    }

    const started_at = std.Io.Timestamp.now(io, .awake);
    var resp = try http.post(url, .{ .json = body, .timeout_ms = 300_000 });
    defer resp.deinit();
    const finished_at = std.Io.Timestamp.now(io, .awake);
    if (!resp.ok()) {
        if (!quiet_errors) {
            if (resp.body) |payload| {
                print("server_error status={d} body={s}\n", .{ resp.status.code, payload });
            } else {
                print("server_error status={d}\n", .{resp.status.code});
            }
        }
        return error.GenerateRequestFailed;
    }
    const payload = resp.body orelse return error.EmptyResponse;
    var parsed = try std.json.parseFromSlice(api.GenerateResponse, allocator, payload, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const choice = if (parsed.value.choices.len > 0) parsed.value.choices[0] else return error.EmptyResponse;

    print("{s}\n", .{choice.message.content orelse ""});
    if (opts.print_finish_reason or opts.print_token_count) {
        if (opts.print_finish_reason and opts.print_token_count) {
            print("finish_reason={s} tokens={d}\n", .{ @tagName(choice.finish_reason), parsed.value.usage.completion_tokens });
        } else if (opts.print_finish_reason) {
            print("finish_reason={s}\n", .{@tagName(choice.finish_reason)});
        } else {
            print("tokens={d}\n", .{parsed.value.usage.completion_tokens});
        }
    }
    if (opts.print_timing) {
        const total_ms = durationMillis(started_at, finished_at);
        const tokens_per_sec: f64 = if (total_ms > 0)
            @as(f64, @floatFromInt(parsed.value.usage.completion_tokens)) * 1000.0 / @as(f64, @floatFromInt(total_ms))
        else
            0;
        print("timing_ms: server_request={d} total={d} tokens_per_sec={d:.2}\n", .{ total_ms, total_ms, tokens_per_sec });
    }
}

fn generateBackendOverrideForChoice(choice: BackendChoice) ?api.ModelBackend {
    return switch (choice) {
        .auto => null,
        .onnx => .onnx,
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .xla => .xla,
        .webgpu => .webgpu,
    };
}

const SseEventBoundary = struct {
    end: usize,
    delimiter_len: usize,
};

const ServerGenerateSseWriter = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    finish_reason: ?api.FinishReason = null,
    stream_error: bool = false,

    fn deinit(self: *@This()) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn writeAll(self: *@This(), data: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, data);
        try self.processCompleteEvents();
    }

    fn finish(self: *@This()) !void {
        try self.processCompleteEvents();
        if (std.mem.trim(u8, self.buffer.items, " \t\r\n").len != 0) return error.InvalidResponse;
    }

    fn processCompleteEvents(self: *@This()) !void {
        while (findSseEventBoundary(self.buffer.items)) |boundary| {
            try self.handleEvent(self.buffer.items[0..boundary.end]);
            const consumed = boundary.end + boundary.delimiter_len;
            const remaining = self.buffer.items[consumed..];
            std.mem.copyForwards(u8, self.buffer.items[0..remaining.len], remaining);
            self.buffer.shrinkRetainingCapacity(remaining.len);
        }
    }

    fn handleEvent(self: *@This(), raw_event: []const u8) !void {
        var event_name: ?[]const u8 = null;
        var data = std.ArrayListUnmanaged(u8).empty;
        defer data.deinit(self.allocator);

        var lines = std.mem.splitScalar(u8, raw_event, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, "\r");
            if (line.len == 0 or line[0] == ':') continue;
            if (std.mem.startsWith(u8, line, "event:")) {
                event_name = std.mem.trim(u8, line["event:".len..], " ");
            } else if (std.mem.startsWith(u8, line, "data:")) {
                var value = line["data:".len..];
                if (std.mem.startsWith(u8, value, " ")) value = value[1..];
                if (data.items.len > 0) try data.append(self.allocator, '\n');
                try data.appendSlice(self.allocator, value);
            }
        }

        if (data.items.len == 0) return;
        if (event_name) |name| {
            if (std.mem.eql(u8, name, "error")) {
                self.stream_error = true;
                print("server_stream_error={s}\n", .{data.items});
                return;
            }
        }
        if (std.mem.eql(u8, data.items, "[DONE]")) return;

        var parsed = try std.json.parseFromSlice(api.GenerateChunk, self.allocator, data.items, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        for (parsed.value.choices) |choice| {
            if (choice.delta.content) |content| {
                print("{s}", .{content});
            }
            if (choice.finish_reason) |finish_reason| {
                self.finish_reason = finish_reason;
            }
        }
    }
};

fn findSseEventBoundary(data: []const u8) ?SseEventBoundary {
    if (std.mem.indexOf(u8, data, "\n\n")) |idx| {
        return .{ .end = idx, .delimiter_len = 2 };
    }
    if (std.mem.indexOf(u8, data, "\r\n\r\n")) |idx| {
        return .{ .end = idx, .delimiter_len = 4 };
    }
    return null;
}

fn runServerGenerateStream(
    allocator: std.mem.Allocator,
    io: std.Io,
    http: *httpx.Client,
    url: []const u8,
    body: []const u8,
    opts: Options,
    quiet_errors: bool,
) !void {
    var stream_writer = ServerGenerateSseWriter{ .allocator = allocator };
    defer stream_writer.deinit();

    const headers = [_][2][]const u8{
        .{ "Accept", "text/event-stream" },
    };

    const started_at = std.Io.Timestamp.now(io, .awake);
    var resp = try http.requestToWriter(.POST, url, .{
        .json = body,
        .headers = &headers,
        .timeout_ms = 300_000,
    }, &stream_writer, null, null);
    defer resp.deinit();
    const finished_at = std.Io.Timestamp.now(io, .awake);

    if (!resp.ok()) {
        if (!quiet_errors) {
            if (stream_writer.buffer.items.len > 0) {
                print("server_error status={d} body={s}\n", .{ resp.status.code, stream_writer.buffer.items });
            } else {
                print("server_error status={d}\n", .{resp.status.code});
            }
        }
        return error.GenerateRequestFailed;
    }
    try stream_writer.finish();
    if (stream_writer.stream_error) return error.GenerateRequestFailed;

    print("\n", .{});
    if (opts.print_finish_reason or opts.print_token_count) {
        const finish_reason = if (stream_writer.finish_reason) |reason| @tagName(reason) else "unknown";
        if (opts.print_finish_reason and opts.print_token_count) {
            print("finish_reason={s} tokens=unavailable\n", .{finish_reason});
        } else if (opts.print_finish_reason) {
            print("finish_reason={s}\n", .{finish_reason});
        } else {
            print("tokens=unavailable\n", .{});
        }
    }
    if (opts.print_timing) {
        const total_ms = durationMillis(started_at, finished_at);
        print("timing_ms: server_request={d} total={d}\n", .{ total_ms, total_ms });
    }
}

fn requireWarmServer(opts: Options) bool {
    return opts.require_server or platform.env.getenvBool("ANTFLY_INFERENCE_REQUIRE_WARM_SERVER");
}

fn defaultServerModelName(model_dir: []const u8) ?[]const u8 {
    const home = platform.env.getenv("HOME") orelse return null;
    return stripDefaultModelsDir(home, model_dir);
}

fn stripDefaultModelsDir(home: []const u8, model_dir: []const u8) ?[]const u8 {
    const marker = "/.antfly/inference/models/";
    if (!std.mem.startsWith(u8, model_dir, home)) return null;
    const rest = model_dir[home.len..];
    if (!std.mem.startsWith(u8, rest, marker)) return null;
    const model_name = rest[marker.len..];
    if (model_name.len == 0) return null;
    return model_name;
}

fn serverGenerateSupportsOptions(opts: Options) bool {
    return opts.image_count == 0 and
        opts.audio_count == 0 and
        !opts.raw_prompt and
        !opts.no_bos and
        !opts.raw_decode_bench and
        !opts.ignore_eos and
        !opts.no_chat_template and
        opts.draft_model == null and
        opts.speculation_policy == .auto and
        opts.speculation_calibration == .none and
        !opts.debug_mtp and
        !opts.debug_gemma4_target and
        !opts.disable_gemma_embedding_scale and
        opts.prefill_chunk_size == 0 and
        opts.host_budget_mb == 0 and
        opts.backend_budget_mb == 0 and
        opts.combined_budget_mb == 0 and
        opts.kv_budget_mb == 0 and
        opts.scratch_budget_mb == 0 and
        opts.artifact_dir == null and
        opts.json_timing_path == null and
        !opts.print_token_ids and
        !opts.print_prompt_token_ids and
        !opts.print_prompt and
        !opts.print_chat_template_status;
}

fn compiledTargetName(target: CompiledTarget) []const u8 {
    return switch (target) {
        .partitioned => "partitioned",
        .whole_model => "whole-model",
    };
}

fn serverGenerateModeName(opts: Options) ?[]const u8 {
    if (opts.mode) |mode| return @tagName(mode);
    if (opts.backend == .metal) return @tagName(ExecutionMode.compiled);
    return null;
}

fn serverGenerateCompiledTargetName(opts: Options) ?[]const u8 {
    if (opts.compiled_target) |target| return compiledTargetName(target);
    if (opts.backend == .metal and (opts.mode == null or opts.mode.? != .eager)) return compiledTargetName(.whole_model);
    return null;
}

fn generateEndpointUrl(allocator: std.mem.Allocator, server_url: []const u8) ![]u8 {
    const root = trimRightSlash(server_url);
    if (std.mem.endsWith(u8, root, "/ai/v1")) {
        return try std.fmt.allocPrint(allocator, "{s}/generate", .{root});
    }
    return try std.fmt.allocPrint(allocator, "{s}/ai/v1/generate", .{root});
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn validateDraftTokenizerCompatibility(
    target_tokenizer: tokenizer_mod.Tokenizer,
    draft_tokenizer: tokenizer_mod.Tokenizer,
    target_cfg: @import("models/gpt.zig").Config,
    draft_cfg: @import("models/gpt.zig").Config,
) !void {
    if (draft_cfg.gemma4_mtp_assistant) {
        if (draft_tokenizer.vocabSize() != target_tokenizer.vocabSize() or
            draft_cfg.vocab_size != target_cfg.vocab_size)
        {
            return error.IncompatibleDraftTokenizer;
        }
        return;
    }
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
        } else if (std.mem.eql(u8, arg, "--speculation-policy")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.speculation_policy = generation.parseSpeculationPolicy(args[i]) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--speculation-calibration")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.speculation_calibration = generation.parseSpeculationCalibration(args[i]) orelse return error.InvalidArguments;
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
        } else if (std.mem.eql(u8, arg, "--stream")) {
            opts.stream = true;
        } else if (std.mem.eql(u8, arg, "--json-timing")) {
            i += 1;
            if (i >= args.len) return error.MissingJsonTimingPath;
            opts.json_timing_path = args[i];
        } else if (std.mem.eql(u8, arg, "--debug-mtp")) {
            opts.debug_mtp = true;
        } else if (std.mem.eql(u8, arg, "--debug-gemma4-target")) {
            opts.debug_gemma4_target = true;
        } else if (std.mem.eql(u8, arg, "--disable-gemma-embedding-scale")) {
            opts.disable_gemma_embedding_scale = true;
        } else if (std.mem.eql(u8, arg, "--raw-prompt")) {
            opts.raw_prompt = true;
        } else if (std.mem.eql(u8, arg, "--no-bos")) {
            opts.no_bos = true;
        } else if (std.mem.eql(u8, arg, "--raw-decode-bench")) {
            opts.raw_decode_bench = true;
        } else if (std.mem.eql(u8, arg, "--ignore-eos")) {
            opts.ignore_eos = true;
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
        } else if (std.mem.eql(u8, arg, "--server")) {
            i += 1;
            if (i >= args.len) return error.MissingServerUrl;
            opts.server_url = args[i];
        } else if (std.mem.eql(u8, arg, "--require-server")) {
            opts.require_server = true;
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
        "speculative: policy={s} calibration={s} decision={s} rounds={d} drafted={d} matched={d} rejected={d} accepted={d} corrections={d} bonus={d} adaptive_fallbacks={d} mtp_enabled={} mtp_graph_replay={s} mtp_acceptance_permille={d} mtp_acceptance_gate_fallbacks={d}",
        .{
            stats.speculation_policy.name(),
            stats.speculation_calibration.name(),
            stats.speculation_policy_decision.name(),
            stats.rounds,
            stats.drafted_tokens,
            stats.matched_draft_tokens,
            stats.rejectedDraftTokens(),
            stats.accepted_tokens,
            stats.correction_tokens,
            stats.bonus_tokens,
            stats.adaptive_fallbacks,
            stats.mtp_enabled,
            stats.mtp_graph_replay_status,
            stats.acceptancePermille(),
            stats.mtp_acceptance_gate_fallbacks,
        },
    );
    if (stats.mtp_disabled_reason) |reason| {
        print(" mtp_disabled_reason={s}", .{reason});
    }
    print("\n", .{});
    const profile = stats.mtp_profile;
    if (profile.enabled) {
        print(
            "mtp_profile: sync={} draft_steps={d} resident_draft_steps={d} host_draft_steps={d} target_verify_calls={d} target_verify_rows={d} target_verify_argmax_calls={d} target_verify_argmax_rows={d} target_verify_argmax_batched_calls={d} target_verify_argmax_syncs={d} dedicated_runtime_hits={d} dedicated_runtime_fallbacks={d} device_verify_commit_hits={d} device_verify_commit_fallbacks={d} device_verify_commit_result_downloads={d} target_choice_downloads={d} commit_forwards_required={d} commit_forwards_avoided={d} accepted_hidden_reuse_rows={d} activation_copies={d} materializations={d} hidden_only_materializations={d} hidden_only_fallbacks={d} correction_materializations={d} bonus_materializations={d} bonus_skips={d} fallback_calls={d}",
            .{
                profile.sync_enabled,
                profile.draft_steps,
                profile.resident_draft_steps,
                profile.host_draft_steps,
                profile.target_verify_calls,
                profile.target_verify_rows,
                profile.target_verify_argmax_calls,
                profile.target_verify_argmax_rows,
                profile.target_verify_argmax_batched_calls,
                profile.target_verify_argmax_syncs,
                profile.dedicated_runtime_hits,
                profile.dedicated_runtime_fallbacks,
                profile.device_verify_commit_hits,
                profile.device_verify_commit_fallbacks,
                profile.device_verify_commit_result_downloads,
                profile.target_choice_downloads,
                profile.commit_forwards_required,
                profile.commit_forwards_avoided,
                profile.accepted_hidden_reuse_rows,
                profile.activation_copies,
                profile.materializations,
                profile.materialization_hidden_only_hits,
                profile.materialization_hidden_only_fallbacks,
                profile.correction_materializations,
                profile.bonus_materializations,
                profile.bonus_skips,
                profile.fallback_calls,
            },
        );
        print(
            " draft_embed_cache_hits={d} draft_embed_cache_misses={d} draft_embed_cache_inserts={d} draft_embed_cache_evictions={d} draft_embed_cache_disabled={d} draft_embedding_cross_copies={d} draft_ms={d} draft_embedding_ms={d} draft_concat_ms={d} draft_preprojection_ms={d} draft_assistant_ms={d} draft_postprojection_ms={d} draft_argmax_ms={d} draft_lm_head_ms={d} draft_selection_ms={d} verify_ms={d} activation_copy_ms={d} materialization_ms={d} fallback_ms={d}\n",
            .{
                profile.draft_embedding_cache_hits,
                profile.draft_embedding_cache_misses,
                profile.draft_embedding_cache_inserts,
                profile.draft_embedding_cache_evictions,
                profile.draft_embedding_cache_disabled,
                profile.draft_target_embedding_cross_copies,
                profile.draft_token_ns / std.time.ns_per_ms,
                profile.draft_target_embedding_ns / std.time.ns_per_ms,
                profile.draft_concat_ns / std.time.ns_per_ms,
                profile.draft_preprojection_ns / std.time.ns_per_ms,
                profile.draft_assistant_ns / std.time.ns_per_ms,
                profile.draft_postprojection_ns / std.time.ns_per_ms,
                profile.draft_argmax_ns / std.time.ns_per_ms,
                profile.draft_lm_head_ns / std.time.ns_per_ms,
                profile.draft_selection_ns / std.time.ns_per_ms,
                profile.target_verify_ns / std.time.ns_per_ms,
                profile.activation_copy_ns / std.time.ns_per_ms,
                profile.materialization_ns / std.time.ns_per_ms,
                profile.fallback_ns / std.time.ns_per_ms,
            },
        );
    }
    const quality = stats.mtp_quality;
    if (stats.mtp_enabled and quality.mismatches > 0) {
        print(
            "mtp_quality: mismatches={d} with_assistant_logits={d} target_in_assistant_top2={d} target_in_assistant_top4={d} target_in_assistant_top8={d} draft_in_target_top2={d} format_or_control_misses={d} near_tie_misses={d} confident_misses={d} avg_assistant_target_margin={d:.6}\n",
            .{
                quality.mismatches,
                quality.mismatches_with_assistant_logits,
                quality.target_in_assistant_top2,
                quality.target_in_assistant_top4,
                quality.target_in_assistant_top8,
                quality.draft_in_target_top2,
                quality.format_or_control_misses,
                quality.near_tie_misses,
                quality.confident_misses,
                quality.averageAssistantTargetMargin(),
            },
        );
    }
}

fn printGemma4TargetDebug(
    cb: *const ops.ComputeBackend,
    tokenizer: tokenizer_mod.Tokenizer,
    manifest: manifest_mod.ModelManifest,
    cfg: gpt_mod.Config,
    prompt_ids: []const i32,
) !void {
    const special = tokenizer.specialTokens();
    print(
        "gemma4_target_debug: family={s} hidden={d} layers={d} heads={d} kv_heads={d} global_kv_heads={d} head_dim={d} global_head_dim={d} sliding_window={d} sliding_pattern={d} kv_shared_layers={d} ple_hidden={d} attention_k_eq_v={} rope_theta={d:.6} rope_local_theta={d:.6} rope_partial_factor={d:.6} rope_dim_override={d} final_logit_softcap={d:.6} embedding_scale={d:.6} weight_tying={} norm_offset={d:.6}\n",
        .{
            @tagName(cfg.family),
            cfg.hidden_size,
            cfg.num_hidden_layers,
            cfg.num_attention_heads,
            cfg.num_key_value_heads,
            cfg.num_global_key_value_heads,
            cfg.attention_head_dim,
            cfg.global_head_dim,
            cfg.sliding_window,
            cfg.sliding_window_pattern,
            cfg.num_kv_shared_layers,
            cfg.ple_hidden_size,
            cfg.attention_k_eq_v,
            cfg.rope_theta,
            cfg.rope_local_theta,
            cfg.rope_partial_factor,
            cfg.rope_dim_override,
            cfg.final_logit_softcapping,
            cfg.tokenEmbeddingScale(),
            cfg.weight_tying,
            cfg.norm_weight_offset,
        },
    );
    print(
        "gemma4_tokenizer_debug: vocab={d} bos={d} eos={d} pad={d} unk={d} manifest_bos={s} manifest_eos={s} manifest_pad={s} add_bos={} add_eos={}\n",
        .{
            tokenizer.vocabSize(),
            special.cls_id,
            special.sep_id,
            special.pad_id,
            special.unk_id,
            manifest.bos_token,
            manifest.eos_token,
            manifest.pad_token,
            manifest.add_bos_token,
            manifest.add_eos_token,
        },
    );
    print("gemma4_prompt_debug: tokens={d} ids", .{prompt_ids.len});
    for (prompt_ids[0..@min(prompt_ids.len, 48)]) |id| print(" {d}", .{id});
    if (prompt_ids.len > 48) print(" ...", .{});
    print("\n", .{});

    print(
        "gemma4_weight_debug: raw_token_embd={} model_token_embd={} raw_output={} lm_head={} raw_output_norm={} model_norm={}\n",
        .{
            try backendHasWeight(cb, "token_embd.weight"),
            try backendHasWeight(cb, "model.embed_tokens.weight"),
            try backendHasWeight(cb, "output.weight"),
            try backendHasWeight(cb, "lm_head.weight"),
            try backendHasWeight(cb, "output_norm.weight"),
            try backendHasWeight(cb, "model.norm.weight"),
        },
    );
    try printDebugWeightSample(cb, cfg, "model.norm.weight");
    try printDebugWeightSample(cb, cfg, "model.layers.0.input_layernorm.weight");
    try printDebugWeightSample(cb, cfg, "model.layers.0.post_attention_layernorm.weight");
    try printDebugWeightSample(cb, cfg, "model.layers.0.pre_feedforward_layernorm.weight");
    try printDebugWeightSample(cb, cfg, "model.layers.0.post_feedforward_layernorm.weight");
    try printDebugWeightSample(cb, cfg, "model.layers.0.self_attn.q_norm.weight");
    try printDebugWeightSample(cb, cfg, "model.layers.0.self_attn.k_norm.weight");

    const layer_samples = [_]usize{ 0, 1, 4, 5, 6, 47 };
    for (layer_samples) |layer| {
        if (layer >= cfg.num_hidden_layers) continue;
        print(
            "gemma4_layer_debug: layer={d} sliding={} shared_tail={} donor={?} kv_heads={d} head_dim={d} rope_dim={d} rope_freq_dim={d} omits_v={}\n",
            .{
                layer,
                cfg.layerUsesSlidingAttention(layer),
                cfg.layerSharesKv(layer),
                cfg.kvDonorLayerIndex(layer),
                cfg.effectiveKVHeadsForLayer(layer),
                cfg.effectiveHeadDimForLayer(layer),
                cfg.layerRopeDim(layer),
                cfg.layerRopeFrequencyDim(layer),
                cfg.layerOmitsVProj(layer),
            },
        );
    }
}

fn backendHasWeight(cb: *const ops.ComputeBackend, name: []const u8) !bool {
    const weight = cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => return false,
        else => return err,
    };
    cb.free(weight);
    return true;
}

fn printDebugWeightSample(cb: *const ops.ComputeBackend, cfg: gpt_mod.Config, name: []const u8) !void {
    const weight = gpt_arch.getModelWeight(cb, cfg, name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => {
            print("gemma4_weight_sample: {s} missing\n", .{name});
            return;
        },
        else => return err,
    };
    defer cb.free(weight);
    const values = try cb.toFloat32(weight, std.heap.page_allocator);
    defer std.heap.page_allocator.free(values);
    print("gemma4_weight_sample: {s} len={d} first", .{ name, values.len });
    for (values[0..@min(values.len, 8)]) |value| print(" {d:.6}", .{value});
    print("\n", .{});
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
    const weight_bytes = estimatePreflightWeightBytes(allocator, manifest, opts) catch 0;
    if (weight_bytes == 0) return;

    var limits = runtime.tier.memory.defaultLimitsForBackend(switch (reservation_tier) {
        .host => .cpu,
        .backend => .gpu,
        .disk => return,
    });
    const predicted_backend_type = predictedBackendType(opts.backend, reservation_tier);
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
        .cuda => {
            if (!build_options.enable_cuda) return .host;
            return .backend;
        },
        .auto => {
            if (build_options.enable_metal and !shouldPreferNativeAheadOfMetal(allocator, manifest)) return .backend;
            return .host;
        },
        .onnx, .xla, .webgpu => return .host,
    }
}

fn predictedBackendType(choice: BackendChoice, tier: runtime.tier.memory.ResidencyTier) backends.BackendType {
    if (tier != .backend) return .native;
    return switch (choice) {
        .metal => .metal,
        .cuda => .cuda,
        .auto => .metal,
        .onnx, .native, .xla, .webgpu => .native,
    };
}

fn shouldPreferNativeAheadOfMetal(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
) bool {
    const total_bytes = estimateModelArtifactBytes(allocator, manifest) catch return true;
    return total_bytes == 0 or total_bytes > metalEagerDenseMaxBytes();
}

fn estimateModelArtifactBytes(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
) !usize {
    if (manifest.gguf_path) |path| return @intCast(try c_file.fileSize(allocator, path));
    if (manifest.safetensors_path) |path| return @intCast(try c_file.fileSize(allocator, path));
    return 0;
}

fn estimatePreflightWeightBytes(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    opts: Options,
) !usize {
    const artifact_bytes = try estimateModelArtifactBytes(allocator, manifest);
    if (opts.backend != .cuda or manifest.safetensors_path == null or manifest.config_path == null) return artifact_bytes;

    const config_bytes = try c_file.readFile(allocator, manifest.config_path.?);
    defer allocator.free(config_bytes);
    const cfg = gpt_mod.parseConfig(allocator, config_bytes) catch return artifact_bytes;
    if (cfg.family != .gemma) return artifact_bytes;

    const embed_elements = std.math.mul(usize, @intCast(cfg.vocab_size), @intCast(cfg.hidden_size)) catch return artifact_bytes;
    const embed_bytes = std.math.mul(usize, embed_elements, @sizeOf(f32)) catch return artifact_bytes;
    const overhead_bytes: usize = 512 * 1024 * 1024;
    return @min(artifact_bytes, embed_bytes + overhead_bytes);
}

fn metalEagerDenseMaxBytes() u64 {
    const mb = platform.env.getenvUsize("TERMITE_METAL_EAGER_DENSE_MAX_MB") orelse return 1024 * 1024 * 1024;
    return mb * 1024 * 1024;
}

fn printUsage() void {
    print(
        \\usage: antfly inference generate <model-dir|model> <prompt> [--server http://host:port] [--require-server] [--stream] [--image path] [--audio path] [--backend auto|onnx|native|metal|xla|webgpu] [--mode eager|compiled] [--compiled-target partitioned|whole-model] [--max-tokens N] [--temperature V] [--top-p V] [--top-k N] [--repetition-penalty V] [--prefill-chunk-size N] [--draft-model path] [--speculative-k N] [--speculation-policy auto|force|off] [--speculation-calibration none|probe|positive] [--cache-dtype f16|f32|int8|fp8|int4|polar4|turbo3] [--host-budget-mb N] [--backend-budget-mb N] [--combined-budget-mb N] [--kv-budget-mb N] [--scratch-budget-mb N] [--artifact-dir <path>] [--no-chat-template] [--raw-prompt] [--no-bos] [--raw-decode-bench] [--ignore-eos] [--debug-mtp] [--debug-gemma4-target] [--disable-gemma-embedding-scale] [--print-finish-reason] [--print-token-count] [--print-token-ids] [--print-prompt-token-ids] [--print-prompt] [--print-chat-template-status] [--print-timing] [--json-timing path]
        \\  Loads a native GGUF/SafeTensors model and prints generated text to stdout.
        \\  With --server or ANTFLY_INFERENCE_SERVER_URL, sends the request to an already-running inference server.
        \\  --stream prints generated text incrementally as token deltas arrive.
        \\  --require-server or ANTFLY_INFERENCE_REQUIRE_WARM_SERVER=1 fails unless a server URL is configured.
        \\  draft-model enables native speculative decoding with a tokenizer-compatible drafter such as a Gemma 4 *-assistant model.
        \\  speculation-calibration defaults to none; Gemma4 MTP auto mode requires probe or positive to run the drafter.
        \\  Explicit compiled backends consult ~/.antfly/inference/artifacts/<owner>/<model>/<backend>/... by default.
        \\  artifact-dir overrides that lookup root.
        \\  whole-model compiled generate prefers package manifests before raw sidecar scanning.
        \\  compiled-target=whole-model requests a compiled backend only when it can own the full traced graph shape.
        \\  raw-decode-bench runs transformer-body decode without logits/sampling for llama-bench-style baselines.
        \\  ignore-eos keeps generating after EOS for benchmark compatibility with engines that do not stop on the same EOG token set.
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

fn effectiveGenerationKvDType(
    requested: runtime.kv.pool.KvDType,
    backend_kind: runtime.kv.pool.BackendKind,
    config: gpt_mod.Config,
    prompt_tokens: usize,
    max_tokens: usize,
) runtime.kv.pool.KvDType {
    if (backend_kind != .cuda or config.family != .gemma) return requested;
    switch (requested) {
        .polar4, .turbo3 => {},
        else => return requested,
    }
    const min_tokens = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TURBOQUANT_MIN_TOKENS") orelse 256;
    if (min_tokens == 0) return requested;
    const total_tokens = prompt_tokens + max_tokens;
    if (total_tokens < min_tokens) return .f32;
    if (requested == .turbo3 and !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_ENABLE_TURBO3_KV")) {
        return .polar4;
    }
    return requested;
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

test "parseArgs accepts server URL" {
    const opts = try parseArgs(&.{
        "gemma-e2b",
        "hello",
        "--server",
        "http://127.0.0.1:8090",
        "--require-server",
        "--stream",
        "--max-tokens",
        "4",
    });
    try std.testing.expectEqualStrings("gemma-e2b", opts.model_dir);
    try std.testing.expectEqualStrings("http://127.0.0.1:8090", opts.server_url.?);
    try std.testing.expect(opts.require_server);
    try std.testing.expect(opts.stream);
    try std.testing.expectEqual(@as(i32, 4), opts.max_tokens);
}

test "server generate routes metal requests to whole model" {
    const opts = Options{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
    };
    try std.testing.expectEqualStrings("compiled", serverGenerateModeName(opts).?);
    try std.testing.expectEqualStrings("whole-model", serverGenerateCompiledTargetName(opts).?);
}

test "server generate rejects unsupported server options" {
    try std.testing.expect(serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
        .print_token_ids = true,
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
        .prefill_chunk_size = 64,
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
        .backend_budget_mb = 4096,
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
        .artifact_dir = "/tmp/artifacts",
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
        .speculation_policy = .force,
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .metal,
        .json_timing_path = "/tmp/timing.json",
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .cuda,
        .raw_decode_bench = true,
    }));
    try std.testing.expect(!serverGenerateSupportsOptions(.{
        .model_dir = "gemma-e2b",
        .prompt = "hello",
        .backend = .cuda,
        .ignore_eos = true,
    }));
}

test "server model name strips local models dir prefix" {
    try std.testing.expectEqualStrings(
        "ggml-org/gemma-4-e2b-it-gguf",
        stripDefaultModelsDir(
            "/Users/alice",
            "/Users/alice/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf",
        ).?,
    );
    try std.testing.expect(stripDefaultModelsDir(
        "/Users/alice",
        "/tmp/models/ggml-org/gemma-4-e2b-it-gguf",
    ) == null);
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
