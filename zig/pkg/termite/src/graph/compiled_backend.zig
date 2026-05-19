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
const ml = @import("ml");
const contracts = @import("backend_contracts.zig");
const ops = @import("../ops/ops.zig");
const backends = @import("../backends/backends.zig");
const cache_mod = @import("cache.zig");
const gpt_mod = @import("../models/gpt.zig");
const model_runtime = @import("model_runtime.zig");
const partition_mod = @import("partition.zig");
const runtime_mod = @import("../runtime/root.zig");
const multi_executor = @import("multi_executor.zig");

pub const CompileMode = enum {
    single_device,
    sharded,
};

pub const AttachmentTarget = cache_mod.CompiledAttachmentTarget;

pub const ModelRuntimeStrategy = enum {
    /// Backend has no whole-model ModelRuntime path yet.
    none,
    /// Backend owns the model directly from the loaded session/config and does
    /// not need a traced Graph shape before creating its runtime.
    direct_session,
    /// Backend creates a ModelRuntime by compiling the traced graph at attach time.
    inline_compiled_graph,
    /// Backend creates a ModelRuntime by loading offline compiled artifacts.
    offline_artifact,
};

pub fn modelRuntimeStrategyLabel(strategy: ModelRuntimeStrategy) []const u8 {
    return switch (strategy) {
        .none => "none",
        .direct_session => "direct_session",
        .inline_compiled_graph => "inline_compiled_graph",
        .offline_artifact => "offline_artifact",
    };
}

pub const AttachContext = struct {
    cb: *const ops.ComputeBackend,
    requested_backend: ?contracts.BackendKind = null,
    model_dir: ?[]const u8 = null,
    artifact_dir: ?[]const u8 = null,
    pjrt_client: ?*anyopaque = null,
    session: ?backends.Session = null,
    gpt_config: ?gpt_mod.Config = null,
    kv_dtype: ?runtime_mod.kv.pool.KvDType = null,
    shared_moe_cache: ?*runtime_mod.moe.shared.SharedExpertCache = null,
    attention_mode: ?cache_mod.AttentionMode = null,
    attachment_target: AttachmentTarget = .partitioned,
};

pub const Definition = struct {
    kind: contracts.BackendKind,
    priority: u8 = 2,
    model_runtime_strategy: ModelRuntimeStrategy = .none,
    supports_for_mode: *const fn (
        context: AttachContext,
        mode: CompileMode,
    ) *const fn (op: ml.graph.OpCode) bool,
    decide_for_mode: ?*const fn (
        context: AttachContext,
        mode: CompileMode,
    ) ?*const fn (query: partition_mod.CapabilityQuery) partition_mod.CapabilityDecision = null,
    should_attach: *const fn (
        context: AttachContext,
        mode: CompileMode,
    ) bool,
    has_compilable_partition: *const fn (
        graph: *const ml.graph.Graph,
        dpp: *const multi_executor.DevicePartitionPlan,
        context: AttachContext,
        mode: CompileMode,
    ) bool,
    attach_executors: *const fn (
        allocator: std.mem.Allocator,
        entry: *cache_mod.CacheEntry,
        graph: *const ml.graph.Graph,
        dpp: *multi_executor.DevicePartitionPlan,
        context: AttachContext,
        mode: CompileMode,
    ) anyerror!void,
    execute_model_forward: ?*const fn (
        allocator: std.mem.Allocator,
        cache: *cache_mod.GraphCache,
        entry: *cache_mod.CacheEntry,
        graph: *const ml.graph.Graph,
        dpp: *multi_executor.DevicePartitionPlan,
        context: AttachContext,
        mode: CompileMode,
        request: model_runtime.ForwardRequest,
    ) anyerror!?[]f32 = null,
    execute_model_forward_direct: ?*const fn (
        allocator: std.mem.Allocator,
        cache: *cache_mod.GraphCache,
        context: AttachContext,
        mode: CompileMode,
        request: model_runtime.ForwardRequest,
    ) anyerror!?[]f32 = null,
    execute_model_forward_output_direct: ?*const fn (
        allocator: std.mem.Allocator,
        cache: *cache_mod.GraphCache,
        context: AttachContext,
        mode: CompileMode,
        request: model_runtime.ForwardRequest,
    ) anyerror!?model_runtime.ModelOutput = null,
    execute_model_greedy_direct: ?*const fn (
        allocator: std.mem.Allocator,
        cache: *cache_mod.GraphCache,
        context: AttachContext,
        mode: CompileMode,
        request: model_runtime.ForwardRequest,
        vocab_size: usize,
    ) anyerror!?i64 = null,
    prepare_model_runtime_direct: ?*const fn (
        allocator: std.mem.Allocator,
        cache: *cache_mod.GraphCache,
        context: AttachContext,
        mode: CompileMode,
        request: model_runtime.PrepareRequest,
    ) anyerror!bool = null,

    pub fn capability(
        self: Definition,
        context: AttachContext,
        mode: CompileMode,
    ) partition_mod.Capability {
        return .{
            .backend = self.kind,
            .priority = self.priority,
            .supports = self.supports_for_mode(context, mode),
            .decide = if (self.decide_for_mode) |decide_for_mode| decide_for_mode(context, mode) else null,
        };
    }
};

pub fn modelRuntimeForSessionExecutor(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    backend_kind: contracts.BackendKind,
    attachment_target: cache_mod.CompiledAttachmentTarget,
    model_executor: *model_runtime.ModelExecutor,
) !*model_runtime.ModelRuntime {
    if (cache.getSessionCompiledModelRuntime(backend_kind, attachment_target)) |runtime_value| return runtime_value;

    var runtime_value = try model_executor.createRuntime(allocator);
    errdefer runtime_value.deinit();
    cache.putSessionCompiledModelRuntime(backend_kind, attachment_target, runtime_value);
    return cache.getSessionCompiledModelRuntime(backend_kind, attachment_target) orelse error.MissingCompiledModelRuntime;
}

pub fn modelRuntimeForExecutor(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    entry: *cache_mod.CacheEntry,
    backend_kind: contracts.BackendKind,
    attachment_target: cache_mod.CompiledAttachmentTarget,
    model_executor: *const model_runtime.ModelExecutor,
) !*model_runtime.ModelRuntime {
    if (entry.compiled_model_runtime) |*runtime_value| return runtime_value;
    if (cache.getSessionCompiledModelRuntime(backend_kind, attachment_target)) |runtime_value| return runtime_value;

    var runtime_value = try model_executor.createRuntime(allocator);
    errdefer runtime_value.deinit();
    const caps = runtime_value.capabilities();
    if (caps.state_ownership == .host_assisted_inputs) {
        entry.compiled_model_runtime = runtime_value;
        return if (entry.compiled_model_runtime) |*runtime| runtime else unreachable;
    }

    cache.putSessionCompiledModelRuntime(backend_kind, attachment_target, runtime_value);
    return cache.getSessionCompiledModelRuntime(backend_kind, attachment_target) orelse error.MissingCompiledModelRuntime;
}

fn partitionHasCompute(
    graph: *const ml.graph.Graph,
    part: partition_mod.Partition,
) bool {
    for (part.node_ids) |node_id| {
        const op = graph.node(node_id).op;
        if (op != .parameter and op != .constant) return true;
    }
    return false;
}

pub fn planOwnsAllCompute(
    graph: *const ml.graph.Graph,
    dpp: *const multi_executor.DevicePartitionPlan,
    backend: contracts.BackendKind,
) bool {
    var saw_compute = false;
    for (dpp.base.partitions) |part| {
        if (!partitionHasCompute(graph, part)) continue;
        saw_compute = true;
        if (part.backend != backend) return false;
    }
    return saw_compute;
}

pub fn planHasSingleBackendComputePartition(
    graph: *const ml.graph.Graph,
    dpp: *const multi_executor.DevicePartitionPlan,
    backend: contracts.BackendKind,
) bool {
    var owner_count: usize = 0;
    for (dpp.base.partitions) |part| {
        if (!partitionHasCompute(graph, part)) continue;
        if (part.backend != backend) return false;
        owner_count += 1;
    }
    return owner_count == 1;
}

pub fn attachmentTargetAllowsPlan(
    graph: *const ml.graph.Graph,
    dpp: *const multi_executor.DevicePartitionPlan,
    backend: contracts.BackendKind,
    target: AttachmentTarget,
) bool {
    return switch (target) {
        .partitioned => true,
        .whole_model => planOwnsAllCompute(graph, dpp, backend),
    };
}

fn testSupportsOnlyGelu(op: ml.graph.OpCode) bool {
    return op == .fused_gelu;
}

test "planOwnsAllCompute ignores parameter-only fallback partitions" {
    const allocator = std.testing.allocator;

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 4 });

    const x = try builder.parameter("x", shape);
    const y = try graph.addNode(.{
        .op = .{ .fused_gelu = {} },
        .output_shape = shape,
        .inputs = .{ x, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    try graph.markOutput(y);

    const capabilities = [_]partition_mod.Capability{
        .{ .backend = .onnx, .priority = 10, .supports = &testSupportsOnlyGelu },
        .{ .backend = .native, .priority = 1, .supports = &partition_mod.supportsAll },
    };
    const plan = try partition_mod.partition(allocator, &graph, &capabilities);
    const assignments = try allocator.alloc(@import("device_mesh.zig").DeviceId, plan.partitions.len);
    @memset(assignments, 0);
    var dpp = multi_executor.DevicePartitionPlan{
        .base = plan,
        .device_assignment = assignments,
        .allocator = allocator,
    };
    defer dpp.deinit();

    try std.testing.expect(planOwnsAllCompute(&graph, &dpp, .onnx));
    try std.testing.expect(planHasSingleBackendComputePartition(&graph, &dpp, .onnx));
    try std.testing.expect(attachmentTargetAllowsPlan(&graph, &dpp, .onnx, .whole_model));
}

test "planOwnsAllCompute rejects host compute fallback" {
    const allocator = std.testing.allocator;

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 4 });

    const x = try builder.parameter("x", shape);
    const y = try graph.addNode(.{
        .op = .{ .fused_gelu = {} },
        .output_shape = shape,
        .inputs = .{ x, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    const z = try graph.addNode(.{
        .op = .{ .fused_silu = {} },
        .output_shape = shape,
        .inputs = .{ y, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    try graph.markOutput(z);

    const capabilities = [_]partition_mod.Capability{
        .{ .backend = .onnx, .priority = 10, .supports = &testSupportsOnlyGelu },
        .{ .backend = .native, .priority = 1, .supports = &partition_mod.supportsAll },
    };
    const plan = try partition_mod.partition(allocator, &graph, &capabilities);
    const assignments = try allocator.alloc(@import("device_mesh.zig").DeviceId, plan.partitions.len);
    @memset(assignments, 0);
    var dpp = multi_executor.DevicePartitionPlan{
        .base = plan,
        .device_assignment = assignments,
        .allocator = allocator,
    };
    defer dpp.deinit();

    try std.testing.expect(!planOwnsAllCompute(&graph, &dpp, .onnx));
    try std.testing.expect(!attachmentTargetAllowsPlan(&graph, &dpp, .onnx, .whole_model));
    try std.testing.expect(attachmentTargetAllowsPlan(&graph, &dpp, .onnx, .partitioned));
}

test "planHasSingleBackendComputePartition rejects backend islands" {
    const allocator = std.testing.allocator;

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 4 });

    const x = try builder.parameter("x", shape);
    const y = try graph.addNode(.{
        .op = .{ .fused_gelu = {} },
        .output_shape = shape,
        .inputs = .{ x, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    const z = try graph.addNode(.{
        .op = .{ .fused_relu = {} },
        .output_shape = shape,
        .inputs = .{ y, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    try graph.markOutput(z);

    const first_nodes = [_]ml.graph.NodeId{y};
    const second_nodes = [_]ml.graph.NodeId{z};
    const ext = [_]partition_mod.ExternalInput{.{ .node_id = y, .source_partition = 0 }};
    var parts = [_]partition_mod.Partition{
        .{ .backend = .pjrt, .node_ids = first_nodes[0..], .external_inputs = &.{} },
        .{ .backend = .pjrt, .node_ids = second_nodes[0..], .external_inputs = ext[0..] },
    };
    const assignments = try allocator.alloc(@import("device_mesh.zig").DeviceId, parts.len);
    defer allocator.free(assignments);
    @memset(assignments, 0);
    const dpp = multi_executor.DevicePartitionPlan{
        .base = .{
            .partitions = parts[0..],
            .node_assignment = &.{},
            .node_operator_plans = &.{},
            .allocator = allocator,
        },
        .device_assignment = assignments,
        .allocator = allocator,
    };

    try std.testing.expect(planOwnsAllCompute(&graph, &dpp, .pjrt));
    try std.testing.expect(!planHasSingleBackendComputePartition(&graph, &dpp, .pjrt));
}

test "modelRuntimeStrategyLabel is stable for diagnostics" {
    try std.testing.expectEqualStrings("none", modelRuntimeStrategyLabel(.none));
    try std.testing.expectEqualStrings("inline_compiled_graph", modelRuntimeStrategyLabel(.inline_compiled_graph));
    try std.testing.expectEqualStrings("offline_artifact", modelRuntimeStrategyLabel(.offline_artifact));
}

const RuntimeCachingMock = struct {
    ownership: model_runtime.RuntimeStateOwnership,
    created: usize = 0,
    runtime_deinits: usize = 0,
    executor_deinits: usize = 0,

    const runtime_vtable = model_runtime.ModelRuntime.VTable{
        .capabilities = runtimeCapabilities,
        .prefill = runtimePrefill,
        .deinit = runtimeDeinit,
    };

    const executor_vtable = model_runtime.ModelExecutor.VTable{
        .create_runtime = createRuntime,
        .deinit = executorDeinit,
    };

    fn executor(self: *RuntimeCachingMock) model_runtime.ModelExecutor {
        return .{ .ptr = self, .vtable = &executor_vtable };
    }

    fn runtimeCapabilities(ctx: *anyopaque) model_runtime.RuntimeCapabilities {
        const self: *RuntimeCachingMock = @ptrCast(@alignCast(ctx));
        return .{ .state_ownership = self.ownership };
    }

    fn runtimePrefill(
        _: *anyopaque,
        allocator: std.mem.Allocator,
        _: model_runtime.PrefillRequest,
    ) !model_runtime.ModelOutput {
        return .{ .logits = try allocator.dupe(f32, &.{0}) };
    }

    fn runtimeDeinit(ctx: *anyopaque) void {
        const self: *RuntimeCachingMock = @ptrCast(@alignCast(ctx));
        self.runtime_deinits += 1;
    }

    fn createRuntime(ctx: *anyopaque, _: std.mem.Allocator) !model_runtime.ModelRuntime {
        const self: *RuntimeCachingMock = @ptrCast(@alignCast(ctx));
        self.created += 1;
        return .{ .ptr = self, .vtable = &runtime_vtable };
    }

    fn executorDeinit(ctx: *anyopaque) void {
        const self: *RuntimeCachingMock = @ptrCast(@alignCast(ctx));
        self.executor_deinits += 1;
    }
};

fn testCacheEntry(key_seed: u64) cache_mod.CacheEntry {
    return .{
        .key = .{
            .config_hash = key_seed,
            .batch = 1,
            .seq_len = 1,
            .attention_mode = .paged_prefill,
        },
        .graph = undefined,
        .last_used = 0,
    };
}

test "modelRuntimeForExecutor keeps host-assisted runtimes entry scoped" {
    const allocator = std.testing.allocator;
    var cache = cache_mod.GraphCache.init(allocator);
    defer cache.deinit();

    var entry = testCacheEntry(1);
    defer if (entry.compiled_model_runtime) |*runtime| runtime.deinit();

    var mock = RuntimeCachingMock{ .ownership = .host_assisted_inputs };
    var executor = mock.executor();
    defer executor.deinit();

    const first = try modelRuntimeForExecutor(allocator, &cache, &entry, .pjrt, .whole_model, &executor);
    const second = try modelRuntimeForExecutor(allocator, &cache, &entry, .pjrt, .whole_model, &executor);

    try std.testing.expect(first == second);
    try std.testing.expect(cache.getSessionCompiledModelRuntime(.pjrt, .whole_model) == null);
    try std.testing.expectEqual(@as(usize, 1), mock.created);
}

test "modelRuntimeForExecutor promotes backend-owned runtimes to session scope" {
    const allocator = std.testing.allocator;
    var cache = cache_mod.GraphCache.init(allocator);
    defer cache.deinit();

    var first_entry = testCacheEntry(1);
    var second_entry = testCacheEntry(2);

    var mock = RuntimeCachingMock{ .ownership = .backend_owned };
    var executor = mock.executor();
    defer executor.deinit();

    const first = try modelRuntimeForExecutor(allocator, &cache, &first_entry, .onnx, .whole_model, &executor);
    const second = try modelRuntimeForExecutor(allocator, &cache, &second_entry, .onnx, .whole_model, &executor);

    try std.testing.expect(first == second);
    try std.testing.expect(cache.getSessionCompiledModelRuntime(.onnx, .whole_model) == first);
    try std.testing.expectEqual(@as(usize, 1), mock.created);
}
