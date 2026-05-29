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
const gpt_arch = @import("../architectures/gpt.zig");
const contracts = @import("backend_contracts.zig");
const ops = @import("../ops/ops.zig");
const tracing_compute = @import("tracing_compute.zig");
const cache_mod = @import("cache.zig");
const compiled_backend = @import("compiled_backend.zig");
const compiled_registry = @import("compiled_registry.zig");
const interpreter = @import("interpreter.zig");
const model_runtime = @import("model_runtime.zig");
const partition_mod = @import("partition.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const webgpu_capabilities = @import("webgpu_capabilities.zig");
const device_mesh = @import("device_mesh.zig");
const multi_executor = @import("multi_executor.zig");
const parallel_strategy = @import("parallel_strategy.zig");
const graph_passes = @import("ml").graph.passes;
const native_compute = @import("../ops/native_compute.zig");
const executor_stats = @import("executor_stats.zig");
const metal_compute = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};

fn compiledBackendDefinition(kind: contracts.BackendKind) ?compiled_backend.Definition {
    return compiled_registry.find(kind);
}

fn compiledModelForwardRequest(
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    attention_mode: cache_mod.AttentionMode,
) !model_runtime.ForwardRequest {
    return if (attention_mode == .paged_decode) blk: {
        if (input_ids.len != 1 or seq_len == 0) return error.UnsupportedShape;
        break :blk .{ .decode = .{
            .token_id = input_ids[0],
            .position = seq_len - 1,
            .attention_mode = attention_mode,
        } };
    } else .{ .prefill = .{
        .input_ids = input_ids,
        .seq_len = seq_len,
        .query_seq_len = decode_context.query_sequence_len,
        .attention_mode = attention_mode,
    } };
}

fn compiledBackendAttachContext(
    pipeline: anytype,
    requested_backend: ?contracts.BackendKind,
    attention_mode: ?cache_mod.AttentionMode,
) compiled_backend.AttachContext {
    return .{
        .cb = &pipeline.cb,
        .requested_backend = requested_backend,
        .model_dir = pipeline.model_dir,
        .artifact_dir = pipeline.artifact_dir,
        .pjrt_client = pipeline.pjrt_client,
        .session = pipeline.session,
        .gpt_config = pipeline.gpt_config,
        .kv_dtype = pipeline.kv_dtype,
        .shared_moe_cache = pipeline.shared_moe_cache,
        .attention_mode = attention_mode,
        .attachment_target = pipeline.compiled_attachment_target,
    };
}

fn attachEligibleCompiledBackends(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    dpp: *multi_executor.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    mode: compiled_backend.CompileMode,
) !void {
    for (compiled_registry.all()) |backend_def| {
        if (!backend_def.should_attach(context, mode)) continue;
        try backend_def.attach_executors(allocator, entry, graph, dpp, context, mode);
    }
}

pub fn shouldUsePartitionedGraphExecution(
    mesh: *const device_mesh.DeviceMesh,
    maybe_config: ?parallel_strategy.ParallelConfig,
) bool {
    if (mesh.deviceCount() <= 1) return false;
    const config = maybe_config orelse return false;
    if (config.num_devices <= 1) return false;
    return config.strategy != .single;
}

pub fn graphForwardAll(
    pipeline: anytype,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) ![]f32 {
    const allocator = pipeline.allocator;
    const attn_mode = cacheAttentionMode(decode_context);
    const entry = try ensureGraphEntry(pipeline, cache, input_ids, batch, seq_len, decode_context);
    const graph = &entry.graph;

    if (entry.weight_inputs == null) {
        const params = graph.parameters.items;
        const wc = try allocator.alloc(interpreter.RuntimeInput, params.len);
        for (params, 0..) |param_id, idx| {
            const name = graph.parameterName(graph.node(param_id));
            const value = pipeline.graphWeight(name) catch |err| {
                std.log.err("graph mode missing parameter: {s}", .{name});
                return err;
            };
            wc[idx] = .{
                .node_id = param_id,
                .value = value,
            };
        }
        entry.weight_inputs = wc;
    }
    if (entry.cached_analysis == null) {
        entry.cached_analysis = try interpreter.CachedAnalysis.compute(allocator, graph);
    }

    const exec_options = interpreter.ExecuteOptions{
        .attention = gpt_arch.attentionContextFromDecode(decode_context),
        .embedding_ids = input_ids,
        .runtime_inputs = entry.weight_inputs,
        .cached_analysis = entry.cached_analysis,
    };

    if (pipeline.device_mesh) |mesh| {
        if (shouldUsePartitionedGraphExecution(mesh, pipeline.parallel_config)) {
            var config = pipeline.parallel_config.?;
            const mesh_devices: u16 = @intCast(mesh.deviceCount());
            if (config.num_devices > mesh_devices) config.num_devices = mesh_devices;
            config.backend = pipeline.cb.kind();

            var dpp = try parallel_strategy.planParallel(allocator, graph, config);
            defer dpp.deinit();

            try attachEligibleCompiledBackends(
                allocator,
                entry,
                graph,
                &dpp,
                compiledBackendAttachContext(pipeline, pipeline.compiled_partition_backend, attn_mode),
                .sharded,
            );

            var multi_result = try multi_executor.executeMultiDevice(allocator, graph, &dpp, mesh, exec_options);
            defer multi_result.deinit(mesh);

            return try multiResultLastOutputToFloat32(allocator, mesh, &multi_result);
        }
    }

    if (pipeline.compiled_partition_backend) |backend| {
        if (try executeExplicitCompiledPartitionBackend(pipeline, allocator, entry, graph, exec_options, backend, attn_mode)) |logits| {
            return logits;
        }
    }

    return try executeSingleDeviceGraphExecutor(pipeline, allocator, graph, exec_options);
}

fn executeSingleDeviceGraphExecutor(
    pipeline: anytype,
    allocator: std.mem.Allocator,
    graph: *const @import("ml").graph.Graph,
    exec_options: interpreter.ExecuteOptions,
) ![]f32 {
    const target_kind = backendKindForGraphPartition(pipeline.cb.kind());
    const use_native_fallback = target_kind != .native and @import("build_options").enable_native;

    var fallback_storage: ?FallbackNativeBackend = null;
    if (use_native_fallback) {
        fallback_storage = try FallbackNativeBackend.init(allocator);
    }
    defer {
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_fallback_begin\n", .{});
        if (fallback_storage) |*fallback| fallback.deinit(allocator);
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_fallback_end\n", .{});
    }

    var devices_buf: [2]device_mesh.DeviceEntry = undefined;
    devices_buf[0] = .{
        .id = 0,
        .backend = &pipeline.cb,
        .kind = target_kind,
    };
    const device_count: usize = if (use_native_fallback) blk: {
        const fallback = &fallback_storage.?;
        devices_buf[1] = .{
            .id = 1,
            .backend = &fallback.backend,
            .kind = .native,
        };
        break :blk 2;
    } else 1;

    var mesh = try device_mesh.DeviceMesh.init(allocator, devices_buf[0..device_count]);
    defer {
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_mesh_begin\n", .{});
        mesh.deinit();
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_mesh_end\n", .{});
    }

    var capabilities_buf: [2]partition_mod.Capability = undefined;
    capabilities_buf[0] = .{
        .backend = target_kind,
        .priority = 10,
        .supports = supportsForGraphBackend(target_kind),
        .decide = decisionForGraphBackend(target_kind),
    };
    const capability_count: usize = if (use_native_fallback) blk: {
        capabilities_buf[1] = .{
            .backend = .native,
            .priority = 0,
            .supports = &partition_mod.supportsAll,
            .decide = &partition_mod.decideNative,
        };
        break :blk 2;
    } else 1;

    const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(descriptor_seeds);
    try partition_mod.seedAllUploadableResidency(descriptor_seeds, graph, target_kind, 0);
    try seedRuntimeQuantizedInputDescriptors(descriptor_seeds, graph, exec_options.runtime_inputs, &pipeline.cb, target_kind);
    _ = try partition_mod.seedAttentionKvDescriptorsFromContext(descriptor_seeds, graph, exec_options.attention, target_kind, 0);

    var diagnostics = partition_mod.CapabilityDiagnostics{};
    var base_plan = try partition_mod.partitionWithOptions(allocator, graph, capabilities_buf[0..capability_count], .{
        .tensor_descs = descriptor_seeds,
        .diagnostics = &diagnostics,
    });
    var owns_base_plan = true;
    errdefer if (owns_base_plan) base_plan.deinit();
    executor_stats.printPartitionFallbackOps(graph, &base_plan, target_kind);

    const assignments = try allocator.alloc(device_mesh.DeviceId, base_plan.partitions.len);
    var owns_assignments = true;
    errdefer if (owns_assignments) allocator.free(assignments);
    for (base_plan.partitions, 0..) |part, idx| {
        assignments[idx] = if (use_native_fallback and part.backend == .native) 1 else 0;
    }

    var dpp = multi_executor.DevicePartitionPlan{
        .base = base_plan,
        .device_assignment = assignments,
        .allocator = allocator,
    };
    owns_base_plan = false;
    owns_assignments = false;
    defer {
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_dpp_begin\n", .{});
        dpp.deinit();
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_dpp_end\n", .{});
    }

    var multi_result = try multi_executor.executeMultiDevice(allocator, graph, &dpp, &mesh, exec_options);
    defer {
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_multi_result_begin\n", .{});
        multi_result.deinit(&mesh);
        if (traceGraphExecutorOutputs()) std.debug.print("graph_executor_output_trace: cleanup_multi_result_end\n", .{});
    }

    if (traceGraphExecutorOutputs()) std.debug.print(
        "graph_executor_output_trace: single_device outputs={d} output_devices={d}\n",
        .{ multi_result.outputs.len, multi_result.output_devices.len },
    );
    if (multi_result.outputs.len == 0 or multi_result.output_devices.len != multi_result.outputs.len) return error.MissingValue;
    const output_index = multi_result.outputs.len - 1;
    const output_device = mesh.device(multi_result.output_devices[output_index]) orelse return error.DeviceNotFound;
    if (traceGraphExecutorOutputs()) std.debug.print(
        "graph_executor_output_trace: single_device output_index={d} output_device={d} backend={s}\n",
        .{ output_index, multi_result.output_devices[output_index], @tagName(output_device.backend.kind()) },
    );
    const logits = try output_device.backend.toFloat32(multi_result.outputs[output_index], allocator);
    if (traceGraphExecutorOutputs()) std.debug.print(
        "graph_executor_output_trace: single_device to_float32_done len={d}\n",
        .{logits.len},
    );
    return logits;
}

fn multiResultLastOutputToFloat32(
    allocator: std.mem.Allocator,
    mesh: *const device_mesh.DeviceMesh,
    multi_result: *const multi_executor.MultiExecutionResult,
) ![]f32 {
    if (multi_result.outputs.len == 0 or multi_result.output_devices.len != multi_result.outputs.len) return error.MissingValue;
    const output_index = multi_result.outputs.len - 1;
    const output_device = mesh.device(multi_result.output_devices[output_index]) orelse return error.DeviceNotFound;
    return try output_device.backend.toFloat32(multi_result.outputs[output_index], allocator);
}

fn traceGraphExecutorOutputs() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_OUTPUTS", false);
}

const FallbackNativeBackend = struct {
    weight_store: *native_compute.WeightStore,
    compute: *native_compute.NativeCompute,
    backend: ops.ComputeBackend,

    fn init(allocator: std.mem.Allocator) !FallbackNativeBackend {
        const weight_store = try allocator.create(native_compute.WeightStore);
        errdefer allocator.destroy(weight_store);
        weight_store.* = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .empty,
        };

        const compute = try allocator.create(native_compute.NativeCompute);
        errdefer allocator.destroy(compute);
        compute.* = native_compute.NativeCompute.init(allocator, weight_store, null);

        return .{
            .weight_store = weight_store,
            .compute = compute,
            .backend = compute.computeBackend(),
        };
    }

    fn deinit(self: *FallbackNativeBackend, allocator: std.mem.Allocator) void {
        self.backend.deinit();
        native_compute.deinitPrefetchQueue(self.weight_store);
        self.weight_store.resident_weights.deinit(allocator);
        self.weight_store.lazy_weights.deinit(allocator);
        allocator.destroy(self.weight_store);
    }
};

fn backendKindForGraphPartition(kind: contracts.BackendKind) contracts.BackendKind {
    return switch (kind) {
        .metal => .metal,
        .mlx => .mlx,
        .onnx => .onnx,
        .pjrt => .pjrt,
        .wasm => .wasm,
        .webgpu => .webgpu,
        else => .native,
    };
}

fn supportsForGraphBackend(kind: contracts.BackendKind) *const fn (@import("ml").graph.OpCode) bool {
    return switch (kind) {
        .metal => &metal_capabilities.supportsMetalEagerGraph,
        .webgpu => &webgpu_capabilities.supportsWebGpuGraph,
        else => &partition_mod.supportsAll,
    };
}

fn decisionForGraphBackend(kind: contracts.BackendKind) ?*const fn (partition_mod.CapabilityQuery) partition_mod.CapabilityDecision {
    return switch (kind) {
        .metal => &metal_capabilities.decideMetalEagerGraph,
        .webgpu => &webgpu_capabilities.decideWebGpuGraph,
        .native => &partition_mod.decideNative,
        else => null,
    };
}

fn cacheAttentionMode(decode_context: *const gpt_arch.DecodeContext) cache_mod.AttentionMode {
    return switch (decode_context.attention_mode) {
        .full_recompute => .full_recompute,
        .paged_prefill => .paged_prefill,
        .paged_decode => .paged_decode,
    };
}

fn seedRuntimeQuantizedInputDescriptors(
    seeds: []?contracts.TensorDesc,
    graph: *const @import("ml").graph.Graph,
    runtime_inputs: ?[]const interpreter.RuntimeInput,
    cb: *const ops.ComputeBackend,
    backend: contracts.BackendKind,
) !void {
    if (comptime !build_options.enable_metal) return;
    if (backend != .metal or cb.kind() != .metal) return;
    const inputs = runtime_inputs orelse return;
    for (inputs) |input| {
        const storage = metal_compute.MetalCompute.getQuantizedStorage(cb, input.value) orelse continue;
        const format = contracts.quantFormatFromGgufTensorType(storage.tensor_type) orelse continue;
        try partition_mod.seedTensorDescriptor(
            seeds,
            graph,
            input.node_id,
            contracts.TensorDesc.packedQuant(graph.node(input.node_id).output_shape, format),
        );
    }
}

fn backendTensorDTypeToGraphDType(dtype: @import("../backends/tensor.zig").DType) @import("ml").graph.DType {
    return switch (dtype) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
        .f64 => .f64,
        .i8 => .i8,
        .i16 => .i16,
        .i32 => .i32,
        .i64 => .i64,
        .u8 => .u8,
        .bool_ => .bool_,
    };
}

fn graphCacheKey(
    pipeline: anytype,
    batch: usize,
    decode_context: *const gpt_arch.DecodeContext,
) cache_mod.CacheKey {
    const attn_mode = cacheAttentionMode(decode_context);
    const query_seq_len = decode_context.query_sequence_len;
    return .{
        .config_hash = cache_mod.hashConfigBytes(std.mem.asBytes(&pipeline.gpt_config)),
        .batch = @intCast(batch),
        .seq_len = if (attn_mode == .paged_prefill) cache_mod.bucketSeqLen(@intCast(query_seq_len)) else @intCast(query_seq_len),
        .attention_mode = attn_mode,
    };
}

pub fn ensureGraphEntry(
    pipeline: anytype,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !*cache_mod.CacheEntry {
    const allocator = pipeline.allocator;
    const key = graphCacheKey(pipeline, batch, decode_context);

    if (cache.get(key) == null) {
        const PipelineType = @TypeOf(pipeline.*);
        const Resolver = struct {
            fn resolve(raw_context: ?*anyopaque, resolve_allocator: std.mem.Allocator, name: []const u8) anyerror!?@import("ml").graph.Shape {
                const context = raw_context orelse return null;
                const typed_pipeline: *PipelineType = @ptrCast(@alignCast(context));
                const backend = if (@hasField(PipelineType, "cb")) typed_pipeline.cb else return null;
                const resolved_weight = if (@hasDecl(PipelineType, "graphWeight"))
                    typed_pipeline.graphWeight(name)
                else if (@hasField(PipelineType, "cb"))
                    backend.getWeight(name)
                else
                    return null;
                const weight = resolved_weight catch |err| switch (err) {
                    error.MissingWeight, error.WeightNotFound => return null,
                    else => return err,
                };
                defer backend.free(weight);

                const dims = backend.tensorShape(weight, resolve_allocator) catch |err| switch (err) {
                    error.UnsupportedShape => return null,
                    else => return err,
                };
                defer resolve_allocator.free(dims);

                const dtype = backend.tensorDType(weight) catch .f32;
                return @import("ml").graph.Shape.init(backendTensorDTypeToGraphDType(dtype), dims);
            }
        };

        var tc = tracing_compute.TracingCompute.initWithWeightResolver(allocator, .{
            .context = @ptrCast(@constCast(pipeline)),
            .resolve = &Resolver.resolve,
        });
        tc.setRuntimeEmbeddingIds(input_ids);
        var tc_cb = tc.backend();

        var trace_dc = decode_context.*;
        trace_dc.moe_runtime = null;
        const dummy_logits = try gpt_arch.forward(&tc_cb, allocator, pipeline.gpt_config, input_ids, batch, seq_len, &trace_dc);
        allocator.free(dummy_logits);

        var raw_graph = tc.extractGraph();
        tc.deinit();

        const optimized = try graph_passes.pipeline.Pipeline.default.run(allocator, &raw_graph);
        raw_graph.deinit();
        try cache.put(key, optimized.graph);
    }

    return cache.getEntry(key).?;
}

pub fn executeExplicitCompiledPartitionBackend(
    pipeline: anytype,
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    exec_options: interpreter.ExecuteOptions,
    backend: contracts.BackendKind,
    attention_mode: cache_mod.AttentionMode,
) !?[]f32 {
    const backend_def = compiledBackendDefinition(backend) orelse return null;
    const attach_context = compiledBackendAttachContext(pipeline, backend, attention_mode);
    if (!backend_def.should_attach(attach_context, .single_device)) return null;
    const host_kind = pipeline.cb.kind();
    const use_native_fallback = backend == host_kind and backend != .native and @import("build_options").enable_native;
    const fallback_kind: contracts.BackendKind = if (use_native_fallback) .native else host_kind;
    var fallback_storage: ?FallbackNativeBackend = null;
    if (use_native_fallback) {
        fallback_storage = try FallbackNativeBackend.init(allocator);
    }
    defer if (fallback_storage) |*fallback| fallback.deinit(allocator);

    const capabilities = [_]partition_mod.Capability{
        backend_def.capability(attach_context, .single_device),
        .{ .backend = fallback_kind, .priority = 1, .supports = &partition_mod.supportsAll },
    };

    const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(descriptor_seeds);
    try partition_mod.seedAllUploadableResidency(descriptor_seeds, graph, backend, 0);
    try seedRuntimeQuantizedInputDescriptors(descriptor_seeds, graph, exec_options.runtime_inputs, &pipeline.cb, backend);
    _ = try partition_mod.seedAttentionKvDescriptorsFromContext(descriptor_seeds, graph, exec_options.attention, backend, 0);

    const plan = try partition_mod.partitionWithOptions(allocator, graph, &capabilities, .{
        .tensor_descs = descriptor_seeds,
    });
    executor_stats.printPartitionFallbackOps(graph, &plan, backend);
    const dev_assign = try allocator.alloc(device_mesh.DeviceId, plan.partitions.len);
    for (plan.partitions, 0..) |part, idx| {
        dev_assign[idx] = if (use_native_fallback and part.backend == .native) 1 else 0;
    }

    var dpp = multi_executor.DevicePartitionPlan{
        .base = plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    if (!backend_def.has_compilable_partition(graph, &dpp, attach_context, .single_device)) return null;

    try backend_def.attach_executors(
        allocator,
        entry,
        graph,
        &dpp,
        attach_context,
        .single_device,
    );

    var devices_buf: [2]device_mesh.DeviceEntry = undefined;
    devices_buf[0] = .{ .id = 0, .backend = &pipeline.cb, .kind = host_kind };
    const device_count: usize = if (use_native_fallback) blk: {
        const fallback = &fallback_storage.?;
        devices_buf[1] = .{ .id = 1, .backend = &fallback.backend, .kind = .native };
        break :blk 2;
    } else 1;

    var mesh = try device_mesh.DeviceMesh.init(allocator, devices_buf[0..device_count]);
    defer mesh.deinit();

    var multi_result = try multi_executor.executeMultiDevice(allocator, graph, &dpp, &mesh, exec_options);
    defer multi_result.deinit(&mesh);

    return try multiResultLastOutputToFloat32(allocator, &mesh, &multi_result);
}

pub fn graphForwardCompiledModelLast(
    pipeline: anytype,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    const backend = pipeline.compiled_partition_backend orelse return null;
    if (pipeline.compiled_attachment_target != .whole_model) return null;
    const backend_def = compiledBackendDefinition(backend) orelse {
        std.log.warn(
            "compiled whole-model runtime unavailable: backend={s} is not a registered compiled backend",
            .{@tagName(backend)},
        );
        return null;
    };
    if (backend_def.model_runtime_strategy == .none) {
        std.log.warn(
            "compiled whole-model runtime unavailable: backend={s} declares model_runtime_strategy=none",
            .{@tagName(backend)},
        );
        return null;
    }
    const allocator = pipeline.allocator;
    const attention_mode = cacheAttentionMode(decode_context);
    const attach_context = compiledBackendAttachContext(pipeline, backend, attention_mode);
    const request = try compiledModelForwardRequest(input_ids, seq_len, decode_context, attention_mode);
    if (backend_def.execute_model_forward_direct) |execute_direct| {
        if (try execute_direct(
            allocator,
            cache,
            attach_context,
            .single_device,
            request,
        )) |last_logits| return last_logits;
    }

    const execute_model_forward = backend_def.execute_model_forward orelse {
        std.log.warn(
            "compiled whole-model runtime unavailable: backend={s} strategy={s} has no execute_model_forward hook",
            .{ @tagName(backend), compiled_backend.modelRuntimeStrategyLabel(backend_def.model_runtime_strategy) },
        );
        return null;
    };

    const entry = try ensureGraphEntry(pipeline, cache, input_ids, batch, seq_len, decode_context);
    const graph = &entry.graph;
    if (!backend_def.should_attach(attach_context, .single_device)) {
        std.log.warn(
            "compiled whole-model runtime unavailable: backend={s} declined attach for attention_mode={s}",
            .{ @tagName(backend), @tagName(attention_mode) },
        );
        return null;
    }

    const host_kind = pipeline.cb.kind();
    const capabilities = [_]partition_mod.Capability{
        backend_def.capability(attach_context, .single_device),
        .{ .backend = host_kind, .priority = 1, .supports = &partition_mod.supportsAll },
    };

    const plan = try partition_mod.partition(allocator, graph, &capabilities);
    const dev_assign = try allocator.alloc(device_mesh.DeviceId, plan.partitions.len);
    @memset(dev_assign, 0);

    var dpp = multi_executor.DevicePartitionPlan{
        .base = plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    if (!backend_def.has_compilable_partition(graph, &dpp, attach_context, .single_device)) {
        std.log.warn(
            "compiled whole-model runtime unavailable: backend={s} strategy={s} cannot own traced graph shape attention_mode={s} seq_len={d} query_seq_len={d}",
            .{
                @tagName(backend),
                compiled_backend.modelRuntimeStrategyLabel(backend_def.model_runtime_strategy),
                @tagName(attention_mode),
                seq_len,
                decode_context.query_sequence_len,
            },
        );
        return null;
    }

    const maybe_logits = try execute_model_forward(
        allocator,
        cache,
        entry,
        graph,
        &dpp,
        attach_context,
        .single_device,
        request,
    );
    if (maybe_logits == null) {
        std.log.warn(
            "compiled whole-model runtime unavailable: backend={s} strategy={s} did not produce logits for attention_mode={s} seq_len={d} query_seq_len={d}",
            .{
                @tagName(backend),
                compiled_backend.modelRuntimeStrategyLabel(backend_def.model_runtime_strategy),
                @tagName(attention_mode),
                seq_len,
                decode_context.query_sequence_len,
            },
        );
    }
    return maybe_logits;
}

pub fn prepareCompiledModelRuntime(
    pipeline: anytype,
    cache: *cache_mod.GraphCache,
    kv_tokens_hint: usize,
) !bool {
    const backend = pipeline.compiled_partition_backend orelse return false;
    if (pipeline.compiled_attachment_target != .whole_model) return false;
    const backend_def = compiledBackendDefinition(backend) orelse return false;
    if (backend_def.model_runtime_strategy == .none) return false;
    const prepare_direct = backend_def.prepare_model_runtime_direct orelse return false;

    const attach_context = compiledBackendAttachContext(pipeline, backend, null);
    return try prepare_direct(
        pipeline.allocator,
        cache,
        attach_context,
        .single_device,
        .{ .kv_tokens_hint = kv_tokens_hint },
    );
}

pub fn graphForwardCompiledModelOutput(
    pipeline: anytype,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?model_runtime.ModelOutput {
    _ = batch;
    const backend = pipeline.compiled_partition_backend orelse return null;
    if (pipeline.compiled_attachment_target != .whole_model) return null;
    const backend_def = compiledBackendDefinition(backend) orelse return null;
    if (backend_def.model_runtime_strategy == .none) return null;
    const execute_direct = backend_def.execute_model_forward_output_direct orelse return null;

    const attention_mode = cacheAttentionMode(decode_context);
    const attach_context = compiledBackendAttachContext(pipeline, backend, attention_mode);
    const request = try compiledModelForwardRequest(input_ids, seq_len, decode_context, attention_mode);
    return try execute_direct(
        pipeline.allocator,
        cache,
        attach_context,
        .single_device,
        request,
    );
}

pub fn graphForwardCompiledModelGreedyToken(
    pipeline: anytype,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    vocab_size: usize,
) !?i64 {
    _ = batch;
    const backend = pipeline.compiled_partition_backend orelse return null;
    if (pipeline.compiled_attachment_target != .whole_model) return null;
    const backend_def = compiledBackendDefinition(backend) orelse return null;
    if (backend_def.model_runtime_strategy == .none) return null;
    const execute_direct = backend_def.execute_model_greedy_direct orelse return null;

    const attention_mode = cacheAttentionMode(decode_context);
    const attach_context = compiledBackendAttachContext(pipeline, backend, attention_mode);
    const request = try compiledModelForwardRequest(input_ids, seq_len, decode_context, attention_mode);
    return try execute_direct(
        pipeline.allocator,
        cache,
        attach_context,
        .single_device,
        request,
        vocab_size,
    );
}

test "shouldUsePartitionedGraphExecution requires explicit sharding config" {
    const allocator = std.testing.allocator;
    const fake_cb_a = @as(*const ops.ComputeBackend, @ptrFromInt(0x1000));
    const fake_cb_b = @as(*const ops.ComputeBackend, @ptrFromInt(0x2000));

    var mesh = try device_mesh.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = fake_cb_a, .kind = .mlx },
        .{ .id = 1, .backend = fake_cb_b, .kind = .mlx },
    });
    defer mesh.deinit();

    try std.testing.expect(!shouldUsePartitionedGraphExecution(&mesh, null));
    try std.testing.expect(!shouldUsePartitionedGraphExecution(&mesh, .{
        .strategy = .single,
        .num_devices = 2,
    }));
    try std.testing.expect(!shouldUsePartitionedGraphExecution(&mesh, .{
        .strategy = .pipeline,
        .num_devices = 1,
    }));
    try std.testing.expect(shouldUsePartitionedGraphExecution(&mesh, .{
        .strategy = .pipeline,
        .num_devices = 2,
    }));
}

test "shouldUsePartitionedGraphExecution requires multiple mesh devices" {
    const allocator = std.testing.allocator;
    const fake_cb = @as(*const ops.ComputeBackend, @ptrFromInt(0x3000));

    var mesh = try device_mesh.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = fake_cb, .kind = .mlx },
    });
    defer mesh.deinit();

    try std.testing.expect(!shouldUsePartitionedGraphExecution(&mesh, .{
        .strategy = .tensor,
        .num_devices = 2,
    }));
}

test "multiResultLastOutputToFloat32 reads from output owning device" {
    const allocator = std.testing.allocator;

    var backend_a = try FallbackNativeBackend.init(allocator);
    defer backend_a.deinit(allocator);
    var backend_b = try FallbackNativeBackend.init(allocator);
    defer backend_b.deinit(allocator);

    var mesh = try device_mesh.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &backend_a.backend, .kind = .native },
        .{ .id = 1, .backend = &backend_b.backend, .kind = .native },
    });
    defer mesh.deinit();

    const first = try backend_a.backend.fromFloat32(&.{ 1.0, 2.0 });
    defer backend_a.backend.free(first);
    const last = try backend_b.backend.fromFloat32(&.{ 3.0, 4.0 });
    defer backend_b.backend.free(last);

    const outputs = try allocator.alloc(ops.CT, 2);
    defer allocator.free(outputs);
    outputs[0] = first;
    outputs[1] = last;

    const output_devices = try allocator.alloc(device_mesh.DeviceId, 2);
    defer allocator.free(output_devices);
    output_devices[0] = 0;
    output_devices[1] = 1;

    const result = multi_executor.MultiExecutionResult{
        .outputs = outputs,
        .output_devices = output_devices,
        .allocator = allocator,
    };

    const host = try multiResultLastOutputToFloat32(allocator, &mesh, &result);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, host);
}
