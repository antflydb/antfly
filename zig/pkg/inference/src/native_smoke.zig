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
const session_factory = @import("architectures/session_factory.zig");
const manifest_mod = @import("models/manifest.zig");
const tensor_store_mod = @import("models/tensor_store.zig");
const generation = @import("pipelines/generation.zig");
const model_manager_mod = @import("server/model_manager.zig");
const runtime = @import("runtime/root.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const graph_mod = @import("graph/root.zig");
const c_file = @import("util/c_file.zig");
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

const Options = struct {
    model_dir: []const u8,
    prompt: []const u8,
    backend: BackendChoice = .auto,
    max_tokens: i32 = 32,
    temperature: f32 = 0,
    top_p: f32 = 0,
    top_k: i32 = 0,
    prefill_chunk_size: usize = 0,
    inspect_only: bool = false,
    no_chat_template: bool = false,
    host_budget_mb: usize = 0,
    backend_budget_mb: usize = 0,
    combined_budget_mb: usize = 0,
    kv_budget_mb: usize = 0,
    scratch_budget_mb: usize = 0,
    cache_dtype: ?[]const u8 = null,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try native_backend_choice.validate(opts.backend);

    var manifest = try manifest_mod.loadFromDir(allocator, opts.model_dir);
    defer manifest.deinit();

    printManifestSummary(&manifest, opts.backend);
    var gguf_report = try session_factory.inspectGgufModel(allocator, opts.model_dir);
    defer if (gguf_report) |*report| report.deinit();
    try printGgufSummary(&manifest, gguf_report);

    if (opts.inspect_only) return;

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    try preflightModelLoadBudget(allocator, &manifest, opts);

    const model = try model_manager.loadFromDir(opts.model_dir);
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const tokenizer = model.getTokenizer();
    const messages = [_]generation.Message{
        .{ .role = "user", .content = opts.prompt },
    };
    const apply_chat_template = !opts.no_chat_template and model.chat_tmpl != null;
    const rendered_prompt = if (apply_chat_template)
        try model.chat_tmpl.?.apply(allocator, &messages, true)
    else
        try generation.formatMessages(allocator, &messages);
    defer allocator.free(rendered_prompt);
    var prompt_encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        rendered_prompt,
        2048,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer prompt_encoded.deinit();
    const prompt_tokens = countPromptTokens(prompt_encoded.attention_mask);

    print(
        "native backend={s} layers={d} heads={d} kv_heads={d} hidden={d} moe={} sliding_window={d}\n",
        .{
            @tagName(model.session.backend()),
            gpt_config.num_hidden_layers,
            gpt_config.num_attention_heads,
            gpt_config.effectiveKVHeads(),
            gpt_config.hidden_size,
            gpt_config.usesMoe(),
            gpt_config.sliding_window,
        },
    );

    const special = tokenizer.specialTokens();
    const raw_ids = try tokenizer.encode(allocator, opts.prompt);
    defer allocator.free(raw_ids);
    print(
        "tokenizer vocab={d} cls={d} sep={d} pad={d} unk={d} raw_prompt_tokens={d} chat_template={}\n",
        .{
            tokenizer.vocabSize(),
            special.cls_id,
            special.sep_id,
            special.pad_id,
            special.unk_id,
            raw_ids.len,
            apply_chat_template,
        },
    );
    if (raw_ids.len > 0) {
        const limit = @min(raw_ids.len, 16);
        print("  raw_token_ids:", .{});
        for (raw_ids[0..limit]) |token_id| print(" {d}", .{token_id});
        print("\n", .{});
        if (raw_ids.len > limit) print("  ... and {d} more\n", .{raw_ids.len - limit});
    }

    var native_generate_lease: ?runtime.scheduler.native_generate.Lease = null;
    defer if (native_generate_lease) |lease| {
        if (model.native_generate_coordinator) |coordinator| coordinator.release(lease);
    };
    if (model.native_generate_coordinator) |coordinator| {
        native_generate_lease = try coordinator.acquire(.{
            .requested_units = 1,
            .prompt_bytes = rendered_prompt.len,
            .max_tokens = opts.max_tokens,
        });
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
    budget_limits = applyBudgetOverrides(budget_limits, opts);
    var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
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
    var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
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
    defer decode_state.deinit();
    const explicit_partition_backend = native_backend_choice.compiledPartitionBackend(opts.backend);
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

    var config = generation.GenerationConfig{
        .max_tokens = opts.max_tokens,
        .temperature = opts.temperature,
        .top_p = opts.top_p,
        .top_k = opts.top_k,
        .prefill_chunk_size = opts.prefill_chunk_size,
    };
    if (native_generate_lease) |lease| {
        if (config.prefill_chunk_size == 0) {
            config.prefill_chunk_size = lease.prefill_chunk_size;
        }
    }

    var pipeline = generation.NativeGenerationPipeline{
        .allocator = allocator,
        .io = io,
        .cb = cb,
        .gpt_config = gpt_config,
        .tokenizer = tokenizer,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .chat_template = if (opts.no_chat_template) null else model.chat_tmpl,
        .model_dir = opts.model_dir,
        .gguf_projector_path = model.manifest.gguf_projector_path,
        .decode_state = &decode_state,
        .scheduler = if (nativeGenerateSchedulerEnabled()) model.native_generate_coordinator else null,
        .scheduler_lease = if (nativeGenerateSchedulerEnabled()) if (native_generate_lease) |*lease| lease else null else null,
        .graph_cache = if (native_backend_choice.forcesGraphMode(opts.backend)) &graph_cache else null,
        .compiled_partition_backend = explicit_partition_backend,
        .pjrt_client = if (pjrt_client) |*client| client else null,
    };

    var result = pipeline.generate(&messages, config) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            printBudgetExceeded(model.session, &run_budget);
        }
        return err;
    };
    defer result.deinit();

    print("finish_reason={s} tokens={d}\n", .{ result.finish_reason, result.tokens_used });
    print("{s}\n", .{result.text});
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
        } else if (std.mem.eql(u8, arg, "--prefill-chunk-size")) {
            i += 1;
            if (i >= args.len) return error.MissingPrefillChunkSize;
            opts.prefill_chunk_size = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--inspect-only")) {
            opts.inspect_only = true;
        } else if (std.mem.eql(u8, arg, "--no-chat-template")) {
            opts.no_chat_template = true;
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
        } else if (std.mem.eql(u8, arg, "--cache-dtype")) {
            i += 1;
            if (i >= args.len) return error.MissingCacheDtype;
            opts.cache_dtype = args[i];
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

fn nativeGenerateSchedulerEnabled() bool {
    return !getenvBool("TERMITE_DISABLE_NATIVE_GENERATE_SCHEDULER");
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
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
        .mlx => {
            if (!build_options.enable_mlx) return .host;
            return .backend;
        },
        .cuda => {
            if (!build_options.enable_cuda) return .host;
            return .backend;
        },
        .auto => {
            if ((build_options.enable_metal or build_options.enable_mlx) and !shouldPreferNativeAheadOfMlx(allocator, manifest)) return .backend;
            return .host;
        },
        .onnx, .xla, .webgpu => return .host,
    }
}

fn predictedBackendType(choice: BackendChoice, tier: runtime.tier.memory.ResidencyTier) backends.BackendType {
    if (tier != .backend) return .native;
    return switch (choice) {
        .metal => .metal,
        .mlx => .mlx,
        .cuda => .cuda,
        .auto => if (build_options.enable_metal) .metal else .mlx,
        .onnx, .native, .xla, .webgpu => .native,
    };
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

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    if (choice == .onnx) {
        session_manager.preferred_backends = if (build_options.enable_native)
            &.{backends.BackendType.native}
        else if (build_options.enable_mlx)
            &.{backends.BackendType.mlx}
        else
            &.{};
        return;
    }
    native_backend_choice.configureSessionPreference(session_manager, choice);
}

fn printManifestSummary(manifest: *const manifest_mod.ModelManifest, backend: BackendChoice) void {
    print(
        "model={s} gguf={} mmproj={} safetensors={} tokenizer={s} requested_backend={s}\n",
        .{
            @tagName(manifest.model_type),
            manifest.gguf_path != null,
            manifest.gguf_projector_path != null,
            manifest.safetensors_path != null,
            if (manifest.tokenizer_type) |tok| @tagName(tok) else "none",
            @tagName(backend),
        },
    );
    if (manifest.gguf_projector_path) |path| {
        print("mmproj path={s}\n", .{path});
    }
}

fn printGgufSummary(
    manifest: *const manifest_mod.ModelManifest,
    report: ?session_factory.GgufInspectionReport,
) !void {
    if (manifest.gguf_path == null) return;
    const gguf_report = report orelse {
        print("gguf path={s} inspection=unavailable\n", .{manifest.gguf_path.?});
        return;
    };

    print(
        "gguf path={s} architecture={s} tensors={d} metadata={d}\n",
        .{ manifest.gguf_path.?, gguf_report.architecture, gguf_report.tensor_count, gguf_report.metadata_count },
    );

    if (gguf_report.gpt_config) |cfg| {
        print(
            "  gpt family={s} layers={d} heads={d} kv_heads={d} hidden={d} sliding_window={d} moe={} experts={d} experts_per_tok={d}\n",
            .{
                @tagName(cfg.family),
                cfg.num_hidden_layers,
                cfg.num_attention_heads,
                cfg.effectiveKVHeads(),
                cfg.hidden_size,
                cfg.sliding_window,
                cfg.usesMoe(),
                cfg.num_local_experts,
                cfg.num_experts_per_tok,
            },
        );
    }

    if (gguf_report.all_tensor_types.len == 0) {
        print("  tensor_types=none\n", .{});
    } else {
        print("  tensor_types={d}\n", .{gguf_report.all_tensor_types.len});
        for (gguf_report.all_tensor_types) |entry| {
            print("    {s}: {d}\n", .{ entry.tensor_type.name(), entry.count });
        }
    }

    if (gguf_report.unsupported_tensor_types.len == 0) {
        print("  unsupported_tensor_types=none\n", .{});
    } else {
        print("  unsupported_tensor_types={d}\n", .{gguf_report.unsupported_tensor_types.len});
        for (gguf_report.unsupported_tensor_types) |entry| {
            print("    {s}: {d}\n", .{ entry.tensor_type.name(), entry.count });
        }
    }

    if (gguf_report.quantized_tensor_samples.len > 0) {
        print("  quantized_tensor_samples={d}\n", .{gguf_report.quantized_tensor_samples.len});
        for (gguf_report.quantized_tensor_samples) |name| {
            print("    {s}\n", .{name});
        }
    }

    if (gguf_report.dense_tensor_samples.len > 0) {
        print("  largest_dense_tensor_samples={d}\n", .{gguf_report.dense_tensor_samples.len});
        for (gguf_report.dense_tensor_samples) |sample| {
            print("    {s}: {s} {d} bytes\n", .{ sample.name, sample.tensor_type.name(), sample.byte_len });
        }
    }

    if (gguf_report.missing_required_tensors.len == 0) {
        print("  missing_required_tensors=none\n", .{});
    } else {
        print("  missing_required_tensors={d}\n", .{gguf_report.missing_required_tensors.len});
        const limit = @min(gguf_report.missing_required_tensors.len, 24);
        for (gguf_report.missing_required_tensors[0..limit]) |name| {
            print("    {s}\n", .{name});
        }
        if (gguf_report.missing_required_tensors.len > limit) {
            print("    ... and {d} more\n", .{gguf_report.missing_required_tensors.len - limit});
        }
    }

    if (gguf_report.packed_moe_expert_tensors.len > 0) {
        print("  packed_moe_expert_tensors={d}\n", .{gguf_report.packed_moe_expert_tensors.len});
        for (gguf_report.packed_moe_expert_tensors) |name| {
            print("    {s}\n", .{name});
        }
    }

    if (gguf_report.unmapped_tensor_names.len > 0) {
        print("  unmapped_gguf_tensor_names={d}\n", .{gguf_report.unmapped_tensor_names.len});
        for (gguf_report.unmapped_tensor_names) |name| {
            print("    {s}\n", .{name});
        }
    }
}

fn printUsage() void {
    print(
        \\usage: antfly inference smoke <model-dir> <prompt> [--backend auto|onnx|native|metal|mlx|xla] [--max-tokens N] [--temperature V] [--top-p V] [--top-k N] [--prefill-chunk-size N] [--cache-dtype f16|f32|int8|fp8|int4|polar4|turbo3] [--host-budget-mb N] [--backend-budget-mb N] [--combined-budget-mb N] [--kv-budget-mb N] [--scratch-budget-mb N] [--inspect-only] [--no-chat-template]
        \\  Loads a native GGUF/SafeTensors model, prints GGUF tensor coverage, and runs one native generation pass.
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

fn countPromptTokens(attention_mask: anytype) usize {
    var count: usize = 0;
    while (count < attention_mask.len and attention_mask[count] != 0) : (count += 1) {}
    return count;
}
