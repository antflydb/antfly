// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
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
const device_mesh = @import("device_mesh.zig");
const multi_executor = @import("multi_executor.zig");
const parallel_strategy = @import("parallel_strategy.zig");
const graph_passes = @import("ml").graph.passes;
const native_compute = @import("../ops/native_compute.zig");
const executor_stats = @import("executor_stats.zig");
const metal_compute = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};

/// Synchronous model-weight adapter. Returned handles transfer to execution;
/// checkpoint naming stays with the adapter, never with generic scheduling.
pub const Weights = struct {
    ptr: *anyopaque,
    get_fn: *const fn (*anyopaque, []const u8) anyerror!ops.CT,
};

/// Explicit borrowed execution dependencies shared by serving and offline
/// compilation. Backend/weight adapters are explicit borrows; task publication
/// is outside this interface. The backend owner must outlive cache bindings
/// and resident outputs.
pub const Context = struct {
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    gpt_config: @import("../models/gpt.zig").Config,
    weights: ?Weights = null,
    execution_control: ?InferenceExecutionControl = null,
    model_dir: ?[]const u8 = null,
    artifact_dir: ?[]const u8 = null,
    pjrt_client: ?*anyopaque = null,
    session: ?@import("../backends/session.zig").Session = null,
    kv_dtype: ?@import("../runtime/root.zig").kv.pool.KvDType = null,
    shared_moe_cache: ?*@import("../runtime/root.zig").moe.shared.SharedExpertCache = null,
    device_mesh: ?*device_mesh.DeviceMesh = null,
    parallel_config: ?parallel_strategy.ParallelConfig = null,
    compiled_partition_backend: ?contracts.BackendKind = null,
    compiled_attachment_target: compiled_backend.AttachmentTarget = .partitioned,

    pub fn graphWeight(self: *const Context, name: []const u8) !ops.CT {
        if (self.weights) |source| return source.get_fn(source.ptr, name);
        return self.cb.getWeight(name);
    }
};

fn compiledBackendDefinition(kind: contracts.BackendKind) ?compiled_backend.Definition {
    return compiled_registry.find(kind);
}

fn compiledModelForwardRequest(
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    attention_mode: cache_mod.AttentionMode,
    execution_control: ?InferenceExecutionControl,
) !model_runtime.ForwardRequest {
    return if (attention_mode == .paged_decode) blk: {
        if (input_ids.len != 1 or seq_len == 0) return error.UnsupportedShape;
        break :blk .{ .decode = .{
            .execution_control = execution_control,
            .token_id = input_ids[0],
            .position = seq_len - 1,
            .attention_mode = attention_mode,
        } };
    } else .{ .prefill = .{
        .execution_control = execution_control,
        .input_ids = input_ids,
        .seq_len = seq_len,
        .query_seq_len = decode_context.query_sequence_len,
        .attention_mode = attention_mode,
        .max_inflight_tokens = if (decode_context.kv_cache) |kv| kv.max_inflight_tokens else 0,
        .allow_swa_ring = if (decode_context.kv_cache) |kv| kv.allow_swa_ring else false,
    } };
}

test "compiled prefill request preserves KV ring policy" {
    const decode_context = gpt_arch.DecodeContext{
        .attention_mode = .paged_prefill,
        .total_sequence_len = 64,
        .query_sequence_len = 64,
        .kv_sequence_len = 64,
        .kv_cache = .{
            .sequence_id = 1,
            .pool_id = 2,
            .logical_block_count = 4,
            .tail_tokens = 16,
            .max_inflight_tokens = 128,
            .allow_swa_ring = true,
        },
    };

    const request = try compiledModelForwardRequest(&.{ 1, 2 }, 64, &decode_context, .paged_prefill, null);
    switch (request) {
        .prefill => |prefill| {
            try std.testing.expectEqual(@as(usize, 128), prefill.max_inflight_tokens);
            try std.testing.expect(prefill.allow_swa_ring);
        },
        .decode => return error.TestExpectedEqual,
    }
}

test "compiled model request preserves execution control" {
    const decode_context = gpt_arch.DecodeContext{
        .attention_mode = .paged_decode,
        .total_sequence_len = 2,
        .query_sequence_len = 1,
        .kv_sequence_len = 2,
    };
    const control = InferenceExecutionControl{ .deadline_ns = 0 };
    const request = try compiledModelForwardRequest(&.{7}, 2, &decode_context, .paged_decode, control);
    switch (request) {
        .decode => |decode| try std.testing.expectError(error.Timeout, decode.check()),
        .prefill => return error.TestExpectedEqual,
    }
}

fn compiledBackendAttachContext(
    pipeline: *const Context,
    requested_backend: ?contracts.BackendKind,
    attention_mode: ?cache_mod.AttentionMode,
) compiled_backend.AttachContext {
    return .{
        .cb = pipeline.cb,
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
    pipeline: *const Context,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) ![]f32 {
    const allocator = pipeline.allocator;
    const attn_mode = cacheAttentionMode(decode_context);
    const cache_before = cache.specializations.stats;
    const entry = try ensureGraphEntry(pipeline, cache, input_ids, batch, seq_len, decode_context);
    const graph = &entry.graph;

    if (entry.weight_inputs == null) {
        const params = graph.parameters.items;
        const wc = try entry.resourceAllocator().alloc(interpreter.RuntimeInput, params.len);
        var initialized: usize = 0;
        errdefer {
            for (wc[0..initialized]) |input| pipeline.cb.free(input.value);
            entry.resourceAllocator().free(wc);
        }

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
            initialized += 1;
        }
        entry.weight_inputs = wc;
    }
    if (entry.cached_analysis == null) {
        entry.cached_analysis = try interpreter.CachedAnalysis.compute(entry.resourceAllocator(), graph);
    }

    const exec_options = interpreter.ExecuteOptions{
        .execution_control = pipeline.execution_control,
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

    const output = try executeSingleDeviceGraphExecutor(pipeline, allocator, entry, graph, exec_options);
    if (entry.single_device_executable) |prepared| prepared.diagnostics.recordSpecializations(cache.specializations.stats.since(cache_before));
    return output;
}

fn executeSingleDeviceGraphExecutor(
    pipeline: *const Context,
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    exec_options: interpreter.ExecuteOptions,
) ![]f32 {
    const executable = @import("../execution/executable.zig");
    if (entry.single_device_executable != null and
        (entry.single_device_backend.ptr != pipeline.cb.ptr or entry.single_device_backend.vtable != pipeline.cb.vtable)) entry.resetSingleDeviceExecutable();
    // The cache entry is heap-stable across insertion, LRU eviction and array
    // growth. Executors borrow this backend copy rather than a pipeline stack.
    entry.single_device_backend = pipeline.cb.*;
    if (entry.single_device_executable == null) {
        const target_kind = pipeline.cb.kind();
        const seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, graph);
        defer allocator.free(seeds);
        try partition_mod.seedAllUploadableResidency(seeds, graph, target_kind, 0);
        try seedRuntimeQuantizedInputDescriptors(seeds, graph, exec_options.runtime_inputs, pipeline.cb, target_kind);
        _ = try partition_mod.seedAttentionKvDescriptorsFromContext(seeds, graph, exec_options.attention, target_kind, 0);
        const prepared = try allocator.create(executable.Executable);
        errdefer allocator.destroy(prepared);
        prepared.* = try executable.prepareAnalyzedGraph(allocator, graph, entry.cached_analysis.?, &entry.single_device_backend, .partitioned, .{ .tensor_descs = seeds, .input_bindings = exec_options.runtime_inputs });
        entry.resource_allocator = allocator;
        entry.single_device_executable = prepared;
    }
    const prepared = entry.single_device_executable.?;
    var options = exec_options;
    options.output_placement = .producer;
    var result = try prepared.run(allocator, options);
    defer result.deinit(&prepared.runtime);
    if (result.outputs.len == 0) return error.MissingValue;
    const index = result.outputs.len - 1;
    return result.backendFor(&prepared.runtime, index).toFloat32(result.outputs[index], allocator);
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

    pub fn deinit(self: *FallbackNativeBackend, allocator: std.mem.Allocator) void {
        self.backend.deinit();
        native_compute.deinitPrefetchQueue(self.weight_store);
        self.weight_store.resident_weights.deinit(allocator);
        self.weight_store.lazy_weights.deinit(allocator);
        allocator.destroy(self.weight_store);
    }
};

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
    pipeline: *const Context,
    batch: usize,
    decode_context: *const gpt_arch.DecodeContext,
) cache_mod.CacheKey {
    const attn_mode = cacheAttentionMode(decode_context);
    return .{
        .config_hash = cache_mod.hashConfigBytes(std.mem.asBytes(&pipeline.gpt_config)),
        .batch = @intCast(batch),
        // Key the shape actually traced. Bucketing is safe only when the
        // caller has padded the physical inputs to that bucket first.
        .seq_len = @intCast(decode_context.query_sequence_len),
        .attention_mode = attn_mode,
    };
}

pub fn ensureGraphEntry(
    pipeline: *const Context,
    cache: *cache_mod.GraphCache,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !*cache_mod.CacheEntry {
    const allocator = cache.allocator;
    cache.selectBackend(pipeline.cb);
    const key = graphCacheKey(pipeline, batch, decode_context);

    if (cache.get(key) == null) {
        const Resolver = struct {
            fn resolve(raw_context: ?*anyopaque, resolve_allocator: std.mem.Allocator, name: []const u8) anyerror!?@import("ml").graph.Shape {
                const context = raw_context orelse return null;
                const typed_context: *const Context = @ptrCast(@alignCast(context));
                const backend = typed_context.cb;
                const resolved_weight = typed_context.graphWeight(name);
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

        var raw_graph = trace: {
            var tc = tracing_compute.TracingCompute.initWithWeightResolver(allocator, .{
                .context = @ptrCast(@constCast(pipeline)),
                .resolve = &Resolver.resolve,
            });
            defer tc.deinit();
            tc.setRuntimeEmbeddingIds(input_ids);
            var tc_cb = tc.backend();
            var trace_dc = decode_context.*;
            trace_dc.moe_runtime = null;
            const dummy_logits = try gpt_arch.forward(&tc_cb, allocator, pipeline.gpt_config, input_ids, batch, seq_len, &trace_dc);
            defer allocator.free(dummy_logits);
            break :trace tc.extractGraph();
        };

        const optimized = graph_passes.pipeline.Pipeline.default.run(allocator, &raw_graph) catch |err| {
            raw_graph.deinit();
            return err;
        };
        defer allocator.free(optimized.id_map);
        raw_graph.deinit();
        errdefer {
            var graph = optimized.graph;
            graph.deinit();
        }
        try cache.put(key, optimized.graph);
    }

    return cache.getEntry(key).?;
}

pub fn executeExplicitCompiledPartitionBackend(
    pipeline: *const Context,
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
    try seedRuntimeQuantizedInputDescriptors(descriptor_seeds, graph, exec_options.runtime_inputs, pipeline.cb, backend);
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
    devices_buf[0] = .{ .id = 0, .backend = pipeline.cb, .kind = host_kind };
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
    pipeline: *const Context,
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
    const request = try compiledModelForwardRequest(input_ids, seq_len, decode_context, attention_mode, pipeline.execution_control);
    if (try backend_def.candidate(allocator, cache, attach_context, .single_device)) |candidate| {
        if (try candidate.forward(allocator, request)) |last_logits| return last_logits;
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
    pipeline: *const Context,
    cache: *cache_mod.GraphCache,
    kv_tokens_hint: usize,
) !bool {
    const backend = pipeline.compiled_partition_backend orelse return false;
    if (pipeline.compiled_attachment_target != .whole_model) return false;
    const backend_def = compiledBackendDefinition(backend) orelse return false;
    if (backend_def.model_runtime_strategy == .none) return false;

    const attach_context = compiledBackendAttachContext(pipeline, backend, null);
    const candidate = (try backend_def.candidate(pipeline.allocator, cache, attach_context, .single_device)) orelse return false;
    return candidate.prepare(pipeline.allocator, .{ .execution_control = pipeline.execution_control, .kv_tokens_hint = kv_tokens_hint });
}

pub fn graphForwardCompiledModelOutput(
    pipeline: *const Context,
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

    const attention_mode = cacheAttentionMode(decode_context);
    const attach_context = compiledBackendAttachContext(pipeline, backend, attention_mode);
    const request = try compiledModelForwardRequest(input_ids, seq_len, decode_context, attention_mode, pipeline.execution_control);
    const candidate = (try backend_def.candidate(pipeline.allocator, cache, attach_context, .single_device)) orelse return null;
    return candidate.output(pipeline.allocator, request);
}

pub fn graphForwardCompiledModelGreedyToken(
    pipeline: *const Context,
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

    const attention_mode = cacheAttentionMode(decode_context);
    const attach_context = compiledBackendAttachContext(pipeline, backend, attention_mode);
    const request = try compiledModelForwardRequest(input_ids, seq_len, decode_context, attention_mode, pipeline.execution_control);
    const candidate = (try backend_def.candidate(pipeline.allocator, cache, attach_context, .single_device)) orelse return null;
    return candidate.greedy(pipeline.allocator, request, vocab_size);
}

/// Executes a capability-qualified sampled decode through the compiled
/// whole-model runtime. A null result means the request was rejected before
/// any model frame was submitted. Errors after submission propagate to the
/// caller; this function never replays the forward through another path.
pub fn graphForwardCompiledModelSampledToken(
    pipeline: *const Context,
    cache: *cache_mod.GraphCache,
    request: model_runtime.SampledDecodeRequest,
) !?i64 {
    const backend = pipeline.compiled_partition_backend orelse return null;
    if (pipeline.compiled_attachment_target != .whole_model) return null;
    const backend_def = compiledBackendDefinition(backend) orelse return null;
    if (backend_def.model_runtime_strategy == .none) return null;

    const attach_context = compiledBackendAttachContext(
        pipeline,
        backend,
        request.decode.attention_mode,
    );
    const candidate = (try backend_def.candidate(pipeline.allocator, cache, attach_context, .single_device)) orelse return null;
    return candidate.sample(pipeline.allocator, request);
}

test "shouldUsePartitionedGraphExecution requires explicit sharding config" {
    const allocator = std.testing.allocator;
    const fake_cb_a = @as(*const ops.ComputeBackend, @ptrFromInt(0x1000));
    const fake_cb_b = @as(*const ops.ComputeBackend, @ptrFromInt(0x2000));

    var mesh = try device_mesh.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = fake_cb_a, .kind = .metal },
        .{ .id = 1, .backend = fake_cb_b, .kind = .metal },
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
        .{ .id = 0, .backend = fake_cb, .kind = .metal },
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

test "cached decoder graph preparation reuses native and Metal executables across cache growth" {
    const a = std.testing.allocator;
    const contexts = @import("../backends/compute_context.zig");
    const Backend = @import("../backends/backends.zig").BackendType;
    for ([_]Backend{ .native, .metal }) |kind| {
        if (kind == .metal) {
            if (comptime !build_options.enable_metal) continue;
            if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) continue;
        }
        var context = try contexts.BackendContext.init(a, kind, null);
        defer context.deinit(a);
        const cb = context.computeBackend();
        const pipeline = Context{ .allocator = a, .cb = &cb, .gpt_config = .{} };
        var cache = cache_mod.GraphCache.init(a);
        defer cache.deinitWithBackend(pipeline.cb);
        var graph = @import("ml").graph.Graph.init(a);
        var owned = true;
        defer if (owned) graph.deinit();
        var b = @import("ml").graph.Builder.init(&graph);
        const x = try b.parameter("x", @import("ml").graph.Shape.init(.f32, &.{ 2, 4 }));
        const output = try b.neg(try b.relu(x));
        try graph.markOutput(output);
        const key = cache_mod.CacheKey{ .config_hash = 0, .batch = 2, .seq_len = 1, .attention_mode = .full_recompute };
        try cache.put(key, graph);
        owned = false;
        const entry = cache.getEntry(key).?;
        entry.cached_analysis = try interpreter.CachedAnalysis.compute(a, &entry.graph);
        const bindings = try a.alloc(interpreter.RuntimeInput, 1);
        var bound = false;
        defer if (!bound) a.free(bindings);
        bindings[0] = .{ .node_id = x, .value = try pipeline.cb.fromFloat32Shape(&.{ -4, -3, -2, -1, 0, 1, 2, 3 }, &.{ 2, 4 }) };
        entry.weight_inputs = bindings;
        bound = true;
        var identity: ?*@import("../execution/executable.zig").Executable = null;
        for (0..3) |iteration| {
            const values = try executeSingleDeviceGraphExecutor(&pipeline, a, entry, &entry.graph, .{ .runtime_inputs = bindings });
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, -1, -2, -3 }, values);
            const prepared = entry.single_device_executable.?;
            if (identity) |prior| try std.testing.expectEqual(prior, prepared) else identity = prepared;
            if (iteration == 0) {
                // Keep this entry hot while the pointer array grows and other
                // entries are evicted. Prepared graph/backend pointers survive.
                for (1..cache_mod.max_entries + 8) |index| {
                    _ = cache.getEntry(key);
                    var other_key = key;
                    other_key.config_hash = index;
                    try cache.put(other_key, @import("ml").graph.Graph.init(a));
                }
                prepared.diagnostics.configure(true);
            } else if (kind == .native) {
                const stats = prepared.diagnostics.snapshot().?.execution;
                try std.testing.expectEqual(@as(u64, 0), stats.physical_storage_allocations);
                try std.testing.expect(stats.physical_storage_reuses > 0);
            }
        }
    }
}

test "decoder graph cache owns tracing allocations and failed traces remain retryable" {
    const a = std.testing.allocator;
    const Backend = @import("../backends/backends.zig").BackendType;
    for ([_]Backend{ .native, .metal, .cuda }) |kind| {
        if (kind == .metal and !build_options.enable_metal) continue;
        if (kind == .cuda and !build_options.enable_cuda) continue;
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, kind, null);
        defer context.deinit(a);
        const cb = context.computeBackend();
        var request = std.heap.FixedBufferAllocator.init(&.{});
        const Fixture = struct {
            cb: *const ops.ComputeBackend,
            missing_norm: bool = true,
            fn get(raw: *anyopaque, name: []const u8) !ops.CT {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (std.mem.endsWith(u8, name, "norm.weight")) {
                    if (self.missing_norm) return error.MissingWeight;
                    return self.cb.fromFloat32Shape(&.{ 1, 1, 1, 1 }, &.{4});
                }
                if (std.mem.endsWith(u8, name, "embed_tokens.weight")) {
                    var values: [32]f32 = @splat(1);
                    @memset(values[5..8], 0);
                    return self.cb.fromFloat32Shape(&values, &.{ 8, 4 });
                }
                if (std.mem.eql(u8, name, "lm_head.weight"))
                    return self.cb.fromFloat32Shape(&@as([32]f32, @splat(1)), &.{ 8, 4 });
                return error.MissingWeight;
            }
        };
        var fixture = Fixture{ .cb = &cb };
        const pipeline = Context{ .allocator = request.allocator(), .cb = &cb, .weights = .{ .ptr = &fixture, .get_fn = Fixture.get }, .gpt_config = .{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .num_key_value_heads = 1, .intermediate_size = 8, .vocab_size = 8, .position_encoding = .rope, .norm_type = .rms_norm, .activation = .silu } };
        var cache = cache_mod.GraphCache.init(a);
        defer cache.deinit();
        const decode_context = gpt_arch.DecodeContext{ .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1 };
        try std.testing.expectError(error.MissingWeight, ensureGraphEntry(&pipeline, &cache, &.{0}, 1, 1, &decode_context));
        try std.testing.expectEqual(@as(usize, 0), cache.count());
        fixture.missing_norm = false;
        // Serving and offline preparation share this exact typed context. Execute
        // through it too, so backend-pointer contracts cannot hide in uninstantiated
        // generic branches. The adapter supplies all-one embeddings/head weights.
        var execution = pipeline;
        execution.allocator = a;
        const logits = try graphForwardAll(&execution, &cache, &.{0}, 1, 1, &decode_context);
        defer a.free(logits);
        try std.testing.expectEqual(@as(usize, 8), logits.len);
        for (logits) |value| try std.testing.expectApproxEqAbs(@as(f32, 4) / @sqrt(1 + execution.gpt_config.norm_eps), value, 0.00001);
        const changed = try graphForwardAll(&execution, &cache, &.{1}, 1, 1, &decode_context);
        defer a.free(changed);
        for (changed) |value| try std.testing.expectApproxEqAbs(@as(f32, 1) / @sqrt(0.25 + execution.gpt_config.norm_eps), value, 0.00001);
        const entry = try ensureGraphEntry(&pipeline, &cache, &.{0}, 1, 1, &decode_context);
        // The request allocator has zero capacity; persistent graph creation and
        // a subsequent cache hit must use the cache's owner and remain usable.
        try std.testing.expect(entry.graph.nodeCount() > 0);
        try std.testing.expect(entry == try ensureGraphEntry(&pipeline, &cache, &.{0}, 1, 1, &decode_context));
        try std.testing.expectEqual(@as(usize, 1), cache.count());
        const first_ids: [12]i64 = @splat(0);
        const second_ids: [13]i64 = @splat(0);
        var prefill = gpt_arch.DecodeContext{ .attention_mode = .paged_prefill, .total_sequence_len = 12, .query_sequence_len = 12, .kv_sequence_len = 12 };
        const first = try ensureGraphEntry(&pipeline, &cache, &first_ids, 1, 12, &prefill);
        prefill.total_sequence_len = 13;
        prefill.query_sequence_len = 13;
        prefill.kv_sequence_len = 13;
        const second = try ensureGraphEntry(&pipeline, &cache, &second_ids, 1, 13, &prefill);
        const last = second.graph.outputs.items[second.graph.outputs.items.len - 1];
        try std.testing.expectEqual(@as(i64, 13 * 8), second.graph.node(last).output_shape.numElements().?);
        try std.testing.expect(first != second);
        var decode = gpt_arch.DecodeContext{ .attention_mode = .paged_decode, .total_sequence_len = 14, .query_sequence_len = 1, .kv_sequence_len = 14 };
        const decode_entry = try ensureGraphEntry(&pipeline, &cache, &.{0}, 1, 14, &decode);
        decode.total_sequence_len = 15;
        decode.kv_sequence_len = 15;
        // Total KV length changes each step; the physical query remains one row.
        try std.testing.expect(decode_entry == try ensureGraphEntry(&pipeline, &cache, &.{0}, 1, 15, &decode));
    }
}
