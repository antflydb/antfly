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

// Runtime owner for static Antfly inference graphs.
//
// Frontends such as ONNX import and traced model capture produce `ml.graph.Graph`.
// This runtime owns the execution strategy for that graph: eager interpreter,
// partitioned execution, and eventually compiled-preferred/compiled-required
// backend executors.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const native_mod = @import("../ops/native_compute.zig");
const interpreter = @import("interpreter.zig");
const partition_mod = @import("partition.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const webgpu_capabilities = @import("webgpu_capabilities.zig");
const multi_executor = @import("multi_executor.zig");
const device_mesh_mod = @import("device_mesh.zig");
const native_partition_executor = @import("native_partition_executor.zig");

const Graph = ml.graph.Graph;
const ComputeBackend = ops_mod.ComputeBackend;
const BackendKind = contracts.BackendKind;
const NativeCompute = native_mod.NativeCompute;
const WeightStore = native_mod.WeightStore;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const NodeId = ml.graph.NodeId;
const operator_plan = @import("operator_plan.zig");

pub const Strategy = enum {
    interpreter,
    partitioned,
    compiled_preferred,
    compiled_required,
};

pub const Result = struct {
    outputs: []contracts.CT,
    output_devices: ?[]device_mesh_mod.DeviceId = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Result, runtime: *const Runtime) void {
        if (self.output_devices) |devices| {
            if (runtime.mesh) |*mesh| {
                for (self.outputs, devices) |ct, dev_id| {
                    if (mesh.device(dev_id)) |entry| entry.backend.free(ct);
                }
            }
            self.allocator.free(devices);
        } else {
            for (self.outputs) |ct| runtime.default_backend.free(ct);
        }
        self.allocator.free(self.outputs);
        self.outputs = &.{};
        self.output_devices = null;
    }
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    strategy: Strategy,
    default_backend: *const ComputeBackend,
    mesh: ?device_mesh_mod.DeviceMesh = null,
    plan: ?multi_executor.DevicePartitionPlan = null,
    fallback_native: ?FallbackNativeBackend = null,

    pub fn init(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        backend: *const ComputeBackend,
        requested_strategy: Strategy,
    ) !Runtime {
        var runtime = Runtime{
            .allocator = allocator,
            .strategy = requested_strategy,
            .default_backend = backend,
        };
        errdefer runtime.deinit();

        if (requested_strategy != .interpreter) {
            try runtime.initSingleDevicePlan(graph, backend);
            if (requested_strategy == .compiled_required and !runtime.allPartitionsHaveAttachedExecutors()) {
                return error.UnsupportedCompiledGraphRuntime;
            }
        }

        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        if (self.plan) |*plan| {
            plan.deinit();
            self.plan = null;
        }
        if (self.mesh) |*mesh| {
            mesh.deinit();
            self.mesh = null;
        }
        if (self.fallback_native) |*fallback| {
            fallback.deinit(self.allocator);
            self.fallback_native = null;
        }
    }

    pub fn execute(
        self: *Runtime,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        options: interpreter.ExecuteOptions,
    ) !Result {
        switch (self.strategy) {
            .interpreter => {
                var result = try interpreter.execute(allocator, graph, self.default_backend, options);
                errdefer result.deinit(self.default_backend);
                const outputs = result.outputs;
                result.outputs = &.{};
                return .{
                    .outputs = outputs,
                    .allocator = result.allocator,
                };
            },
            .partitioned, .compiled_preferred, .compiled_required => {
                const plan = self.plan orelse return error.MissingGraphRuntimePlan;
                const mesh = self.mesh orelse return error.MissingGraphRuntimeMesh;
                var result = try multi_executor.executeMultiDevice(allocator, graph, &plan, &mesh, options);
                errdefer result.deinit(&mesh);
                try self.normalizeOutputsToDefaultDevice(allocator, &result);
                const outputs = result.outputs;
                result.outputs = &.{};
                allocator.free(result.output_devices);
                result.output_devices = &.{};
                return .{
                    .outputs = outputs,
                    .allocator = result.allocator,
                };
            },
        }
    }

    fn initSingleDevicePlan(
        self: *Runtime,
        graph: *const Graph,
        backend: *const ComputeBackend,
    ) !void {
        const target_kind = backendKindForPartition(backend.kind());
        const use_native_fallback = target_kind != .native and build_options.enable_native;

        if (use_native_fallback) {
            try self.initFallbackNativeBackend();
        }

        var devices_buf: [2]device_mesh_mod.DeviceEntry = undefined;
        const device_count: usize = if (use_native_fallback) blk: {
            const fallback = self.fallback_native orelse return error.MissingNativeFallback;
            devices_buf[0] = .{
                .id = 0,
                .backend = backend,
                .kind = target_kind,
            };
            devices_buf[1] = .{
                .id = 1,
                .backend = &fallback.backend,
                .kind = .native,
            };
            break :blk 2;
        } else blk: {
            devices_buf[0] = .{
                .id = 0,
                .backend = backend,
                .kind = target_kind,
            };
            break :blk 1;
        };
        self.mesh = try device_mesh_mod.DeviceMesh.init(self.allocator, devices_buf[0..device_count]);

        var capabilities_buf: [2]partition_mod.Capability = undefined;
        const capability_count: usize = if (use_native_fallback) blk: {
            capabilities_buf[0] = .{
                .backend = target_kind,
                .priority = 10,
                .supports = supportsForBackend(target_kind),
                .decide = decisionForBackend(target_kind),
            };
            capabilities_buf[1] = .{
                .backend = .native,
                .priority = 0,
                .supports = &partition_mod.supportsAll,
                .decide = &partition_mod.decideNative,
            };
            break :blk 2;
        } else blk: {
            capabilities_buf[0] = .{
                .backend = target_kind,
                .priority = 0,
                .supports = supportsForBackend(target_kind),
                .decide = if (target_kind == .native) &partition_mod.decideNative else null,
            };
            break :blk 1;
        };
        const report_target_override = graphPartitionReportTargetOverride();
        if (graphPartitionReportRequested()) {
            if (report_target_override) |report_target| {
                if (report_target != target_kind) {
                    try dumpPartitionReportForTarget(self.allocator, graph, report_target);
                }
            }
        }

        const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(self.allocator, graph);
        defer self.allocator.free(descriptor_seeds);
        try partition_mod.seedAllUploadableResidency(descriptor_seeds, graph, target_kind, 0);

        var diagnostics = partition_mod.CapabilityDiagnostics{};
        var base = try partition_mod.partitionWithOptions(self.allocator, graph, capabilities_buf[0..capability_count], .{
            .tensor_descs = descriptor_seeds,
            .diagnostics = &diagnostics,
        });
        errdefer base.deinit();
        if (graphPartitionReportRequested()) {
            dumpPartitionReport(graph, &base, target_kind, use_native_fallback, &diagnostics);
        }
        try enforceGraphRuntimePartitionGates(graph, &base, target_kind, .{
            .require_no_fallback = graphRuntimeRequireNoFallbackRequested(),
            .require_no_host_assisted = graphRuntimeRequireNoHostAssistedRequested(),
        });

        const assignments = try self.allocator.alloc(device_mesh_mod.DeviceId, base.partitions.len);
        errdefer self.allocator.free(assignments);
        for (base.partitions, 0..) |part, i| {
            assignments[i] = if (use_native_fallback and part.backend == .native) 1 else 0;
        }
        try self.attachNativePartitionExecutors(graph, &base, assignments);

        self.plan = .{
            .base = base,
            .device_assignment = assignments,
            .allocator = self.allocator,
        };
    }

    fn allPartitionsHaveAttachedExecutors(self: *const Runtime) bool {
        const plan = self.plan orelse return false;
        for (plan.base.partitions) |part| {
            if (part.executor == null) return false;
        }
        return plan.base.partitions.len > 0;
    }

    fn attachNativePartitionExecutors(
        self: *Runtime,
        graph: *const Graph,
        base: *partition_mod.PartitionPlan,
        assignments: []const device_mesh_mod.DeviceId,
    ) !void {
        const mesh = self.mesh orelse return error.MissingGraphRuntimeMesh;
        for (base.partitions, 0..) |*part, idx| {
            if (part.backend != .native and part.backend != .graph) continue;
            if (part.executor != null) continue;
            const dev_id = assignments[idx];
            const dev_entry = mesh.device(dev_id) orelse return error.DeviceNotFound;
            const exec = try native_partition_executor.NativePartitionExecutor.create(
                self.allocator,
                graph,
                dev_entry.backend,
            );
            part.executor = exec.partitionExecutor();
        }
    }

    fn initFallbackNativeBackend(self: *Runtime) !void {
        if (self.fallback_native != null) return;

        const weight_store = try self.allocator.create(WeightStore);
        errdefer self.allocator.destroy(weight_store);
        weight_store.* = .{
            .allocator = self.allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        };

        const compute = try self.allocator.create(NativeCompute);
        errdefer self.allocator.destroy(compute);
        compute.* = NativeCompute.init(self.allocator, weight_store, null);
        self.fallback_native = .{
            .weight_store = weight_store,
            .compute = compute,
            .backend = compute.computeBackend(),
        };
    }

    fn normalizeOutputsToDefaultDevice(
        self: *Runtime,
        allocator: std.mem.Allocator,
        result: *multi_executor.MultiExecutionResult,
    ) !void {
        const mesh = self.mesh orelse return;
        const default_entry = mesh.device(0) orelse return error.DeviceNotFound;
        for (result.outputs, result.output_devices) |*ct, *dev_id| {
            if (dev_id.* == 0) continue;
            const src_entry = mesh.device(dev_id.*) orelse return error.DeviceNotFound;
            const transferred = try multi_executor.transferTensor(allocator, ct.*, src_entry.backend, default_entry.backend);
            src_entry.backend.free(ct.*);
            ct.* = transferred;
            dev_id.* = 0;
        }
    }
};

const FallbackNativeBackend = struct {
    weight_store: *WeightStore,
    compute: *NativeCompute,
    backend: ComputeBackend,

    fn deinit(self: *FallbackNativeBackend, allocator: std.mem.Allocator) void {
        self.backend.deinit();
        native_mod.deinitPrefetchQueue(self.weight_store);
        self.weight_store.resident_weights.deinit(allocator);
        self.weight_store.lazy_weights.deinit(allocator);
        allocator.destroy(self.weight_store);
    }
};

fn graphPartitionReportRequested() bool {
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    return platform.env.getenvBool("TERMITE_GRAPH_PARTITION_REPORT");
}

fn graphPartitionReportPartsRequested() bool {
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    return platform.env.getenvBool("TERMITE_GRAPH_PARTITION_REPORT_PARTS");
}

fn parseEnvBool(value: []const u8) bool {
    return platform.env.truthy(value);
}

fn graphPartitionReportTargetOverride() ?BackendKind {
    if (comptime @import("builtin").os.tag == .freestanding) return null;
    const value = platform.env.getenv("TERMITE_GRAPH_PARTITION_REPORT_TARGET") orelse return null;
    return parsePartitionReportTarget(value);
}

fn graphRuntimeFailClosedRequested() bool {
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    const value = std.c.getenv("TERMITE_GRAPH_RUNTIME_FAIL_CLOSED") orelse
        std.c.getenv("TERMITE_GRAPH_PARTITION_FAIL_CLOSED") orelse
        return false;
    return parseEnvBool(std.mem.span(value));
}

fn graphRuntimeRequireNoFallbackRequested() bool {
    if (graphRuntimeFailClosedRequested()) return true;
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    const value = std.c.getenv("TERMITE_GRAPH_RUNTIME_REQUIRE_NO_FALLBACK") orelse
        std.c.getenv("TERMITE_GRAPH_RUNTIME_FAIL_ON_FALLBACK") orelse
        std.c.getenv("TERMITE_GRAPH_PARTITION_TARGET_REQUIRED") orelse
        return false;
    return parseEnvBool(std.mem.span(value));
}

fn graphRuntimeRequireNoHostAssistedRequested() bool {
    if (graphRuntimeFailClosedRequested()) return true;
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    const value = std.c.getenv("TERMITE_GRAPH_RUNTIME_REQUIRE_NO_HOST_ASSISTED") orelse
        std.c.getenv("TERMITE_GRAPH_RUNTIME_FAIL_ON_HOST_ASSISTED") orelse
        return false;
    return parseEnvBool(std.mem.span(value));
}

fn parsePartitionReportTarget(value: []const u8) ?BackendKind {
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "mlx")) return .mlx;
    if (std.ascii.eqlIgnoreCase(value, "onnx")) return .onnx;
    if (std.ascii.eqlIgnoreCase(value, "pjrt")) return .pjrt;
    if (std.ascii.eqlIgnoreCase(value, "cuda")) return .cuda;
    if (std.ascii.eqlIgnoreCase(value, "wasm")) return .wasm;
    if (std.ascii.eqlIgnoreCase(value, "webgpu")) return .webgpu;
    return null;
}

const PartitionGateSummary = struct {
    fallback_nodes: usize = 0,
    host_assisted_target_nodes: usize = 0,
    first_fallback_op: ?[]const u8 = null,
    first_host_assisted_op: ?[]const u8 = null,
};

fn graphPartitionNodeHasCompute(graph: *const Graph, node_id: NodeId) bool {
    const op = graph.node(node_id).op;
    return op != .parameter and op != .constant;
}

fn summarizePartitionGates(
    graph: *const Graph,
    plan: *const partition_mod.PartitionPlan,
    target_kind: BackendKind,
) PartitionGateSummary {
    var summary = PartitionGateSummary{};
    for (plan.partitions) |part| {
        if (part.backend == target_kind) {
            if (target_kind == .metal) {
                for (part.node_ids) |node_id| {
                    const op = graph.node(node_id).op;
                    if (metal_capabilities.metalEagerGraphNodeIsHostAssisted(.{
                        .graph = graph,
                        .node_id = node_id,
                        .op = op,
                    })) {
                        summary.host_assisted_target_nodes += 1;
                        if (summary.first_host_assisted_op == null) summary.first_host_assisted_op = @tagName(op);
                    }
                }
            }
            continue;
        }

        for (part.node_ids) |node_id| {
            if (!graphPartitionNodeHasCompute(graph, node_id)) continue;
            summary.fallback_nodes += 1;
            if (summary.first_fallback_op == null) {
                summary.first_fallback_op = @tagName(graph.node(node_id).op);
            }
        }
    }
    return summary;
}

const GraphRuntimePartitionGateOptions = struct {
    require_no_fallback: bool = false,
    require_no_host_assisted: bool = false,
};

fn enforceGraphRuntimePartitionGates(
    graph: *const Graph,
    plan: *const partition_mod.PartitionPlan,
    target_kind: BackendKind,
    options: GraphRuntimePartitionGateOptions,
) !void {
    if (!options.require_no_fallback and !options.require_no_host_assisted) return;

    const summary = summarizePartitionGates(graph, plan, target_kind);
    if (options.require_no_fallback and summary.fallback_nodes > 0) {
        std.log.warn(
            "graph runtime fallback rejected target={s} fallback_nodes={d} first_fallback_op={s}",
            .{
                @tagName(target_kind),
                summary.fallback_nodes,
                summary.first_fallback_op orelse "none",
            },
        );
        return error.GraphRuntimeFallbackPartition;
    }
    if (options.require_no_host_assisted and summary.host_assisted_target_nodes > 0) {
        std.log.warn(
            "graph runtime host-assisted target nodes rejected target={s} host_assisted_nodes={d} first_host_assisted_op={s}",
            .{
                @tagName(target_kind),
                summary.host_assisted_target_nodes,
                summary.first_host_assisted_op orelse "none",
            },
        );
        return error.GraphRuntimeHostAssistedOp;
    }
}

fn dumpPartitionReportForTarget(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    target_kind: BackendKind,
) !void {
    const capabilities = [_]partition_mod.Capability{
        .{
            .backend = target_kind,
            .priority = 10,
            .supports = supportsForBackend(target_kind),
            .decide = decisionForBackend(target_kind),
        },
        .{
            .backend = .native,
            .priority = 0,
            .supports = &partition_mod.supportsAll,
            .decide = &partition_mod.decideNative,
        },
    };
    const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(descriptor_seeds);
    try partition_mod.seedAllUploadableResidency(descriptor_seeds, graph, target_kind, 0);

    var diagnostics = partition_mod.CapabilityDiagnostics{};
    var report_plan = try partition_mod.partitionWithOptions(allocator, graph, &capabilities, .{
        .tensor_descs = descriptor_seeds,
        .diagnostics = &diagnostics,
    });
    defer report_plan.deinit();
    dumpPartitionReport(graph, &report_plan, target_kind, target_kind != .native, &diagnostics);
}

const OpCount = struct {
    name: []const u8 = "",
    count: usize = 0,
};

fn addOpCount(counts: []OpCount, used: *usize, name: []const u8) void {
    for (counts[0..used.*]) |*entry| {
        if (std.mem.eql(u8, entry.name, name)) {
            entry.count += 1;
            return;
        }
    }
    if (used.* >= counts.len) return;
    counts[used.*] = .{ .name = name, .count = 1 };
    used.* += 1;
}

fn dumpOpCounts(prefix: []const u8, counts: []const OpCount, used: usize) void {
    if (used == 0) {
        std.debug.print("graph-partition-report {s}=none\n", .{prefix});
        return;
    }
    std.debug.print("graph-partition-report {s}=", .{prefix});
    for (counts[0..used], 0..) |entry, i| {
        if (i > 0) std.debug.print(",", .{});
        std.debug.print("{s}:{d}", .{ entry.name, entry.count });
    }
    if (used == counts.len) std.debug.print(",...", .{});
    std.debug.print("\n", .{});
}

fn dumpPartitionReport(
    graph: *const Graph,
    plan: *const partition_mod.PartitionPlan,
    target_kind: BackendKind,
    has_native_fallback: bool,
    diagnostics: ?*const partition_mod.CapabilityDiagnostics,
) void {
    std.debug.print(
        "graph-partition-report target={s} nodes={d} partitions={d} native_fallback={}\n",
        .{ @tagName(target_kind), graph.nodeCount(), plan.partitions.len, has_native_fallback },
    );

    var target_nodes: usize = 0;
    var fallback_nodes: usize = 0;
    var host_assisted_target_nodes: usize = 0;
    var first_fallback_op: ?[]const u8 = null;
    var first_host_assisted_op: ?[]const u8 = null;
    var fallback_counts = @as([64]OpCount, @splat(.{}));
    var host_assisted_counts = @as([64]OpCount, @splat(.{}));
    var fallback_used: usize = 0;
    var host_assisted_used: usize = 0;
    const include_parts = graphPartitionReportPartsRequested();

    for (plan.partitions, 0..) |part, part_idx| {
        if (include_parts) {
            const first_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[0]).op) else "none";
            const last_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[part.node_ids.len - 1]).op) else "none";
            std.debug.print(
                "graph-partition-report part={d} backend={s} nodes={d} external_inputs={d} first={s} last={s}\n",
                .{ part_idx, @tagName(part.backend), part.node_ids.len, part.external_inputs.len, first_op, last_op },
            );
        }

        if (part.backend == target_kind) {
            target_nodes += part.node_ids.len;
            if (target_kind == .metal) {
                for (part.node_ids) |node_id| {
                    const op = graph.node(node_id).op;
                    if (metal_capabilities.metalEagerGraphNodeIsHostAssisted(.{
                        .graph = graph,
                        .node_id = node_id,
                        .op = op,
                    })) {
                        host_assisted_target_nodes += 1;
                        if (first_host_assisted_op == null) first_host_assisted_op = @tagName(op);
                        addOpCount(&host_assisted_counts, &host_assisted_used, @tagName(op));
                    }
                }
            }
        } else {
            fallback_nodes += part.node_ids.len;
            if (first_fallback_op == null and part.node_ids.len > 0) {
                first_fallback_op = @tagName(graph.node(part.node_ids[0]).op);
            }
            for (part.node_ids) |node_id| {
                addOpCount(&fallback_counts, &fallback_used, @tagName(graph.node(node_id).op));
            }
        }
    }

    std.debug.print(
        "graph-partition-report summary target_nodes={d} fallback_nodes={d} host_assisted_target_nodes={d} first_fallback_op={s} first_host_assisted_op={s}\n",
        .{
            target_nodes,
            fallback_nodes,
            host_assisted_target_nodes,
            first_fallback_op orelse "none",
            first_host_assisted_op orelse "none",
        },
    );
    dumpOpCounts("fallback_ops", &fallback_counts, fallback_used);
    if (target_kind == .metal) {
        dumpOpCounts("metal_host_assisted_ops", &host_assisted_counts, host_assisted_used);
    }
    if (diagnostics) |d| {
        std.debug.print(
            "graph-partition-report capability_rejections unsupported={d} unprofitable={d} wrong_storage={d} missing_quant={d} backend_disabled={d}\n",
            .{
                d.count(.unsupported_op),
                d.count(.unprofitable_shape),
                d.count(.wrong_storage),
                d.count(.missing_quant_kernel),
                d.count(.backend_disabled),
            },
        );
    }
}

fn backendKindForPartition(kind: BackendKind) BackendKind {
    return switch (kind) {
        .metal => .metal,
        .mlx => .mlx,
        .onnx => .onnx,
        .pjrt => .pjrt,
        .cuda => .cuda,
        .wasm => .wasm,
        .webgpu => .webgpu,
        else => .native,
    };
}

fn supportsForBackend(kind: BackendKind) *const fn (ml.graph.OpCode) bool {
    return switch (kind) {
        .metal => &metal_capabilities.supportsMetalEagerGraph,
        .webgpu => &webgpu_capabilities.supportsWebGpuGraph,
        else => &partition_mod.supportsAll,
    };
}

fn decisionForBackend(kind: BackendKind) ?*const fn (partition_mod.CapabilityQuery) partition_mod.CapabilityDecision {
    return switch (kind) {
        .metal => &metal_capabilities.decideMetalEagerGraph,
        .webgpu => &webgpu_capabilities.decideWebGpuGraph,
        .native => &partition_mod.decideNative,
        else => null,
    };
}

pub fn strategyFromEnv() Strategy {
    const value = platform.env.getenv("TERMITE_GRAPH_RUNTIME") orelse
        platform.env.getenv("TERMITE_ONNX_GRAPH_RUNTIME") orelse
        return .interpreter;
    return parseStrategy(value) orelse .interpreter;
}

pub fn parseStrategy(value: []const u8) ?Strategy {
    if (std.ascii.eqlIgnoreCase(value, "interpreter")) return .interpreter;
    if (std.ascii.eqlIgnoreCase(value, "interpreted")) return .interpreter;
    if (std.ascii.eqlIgnoreCase(value, "eager")) return .interpreter;
    if (std.ascii.eqlIgnoreCase(value, "partitioned")) return .partitioned;
    if (std.ascii.eqlIgnoreCase(value, "compiled")) return .compiled_preferred;
    if (std.ascii.eqlIgnoreCase(value, "compiled-preferred")) return .compiled_preferred;
    if (std.ascii.eqlIgnoreCase(value, "compiled_preferred")) return .compiled_preferred;
    if (std.ascii.eqlIgnoreCase(value, "compiled-required")) return .compiled_required;
    if (std.ascii.eqlIgnoreCase(value, "compiled_required")) return .compiled_required;
    return null;
}

test "graph runtime strategy parses environment values" {
    try std.testing.expectEqual(@as(?Strategy, null), parseStrategy(""));
    try std.testing.expectEqual(Strategy.interpreter, parseStrategy("interpreter").?);
    try std.testing.expectEqual(Strategy.interpreter, parseStrategy("interpreted").?);
    try std.testing.expectEqual(Strategy.interpreter, parseStrategy("eager").?);
    try std.testing.expectEqual(Strategy.partitioned, parseStrategy("partitioned").?);
    try std.testing.expectEqual(Strategy.compiled_preferred, parseStrategy("compiled").?);
    try std.testing.expectEqual(Strategy.compiled_preferred, parseStrategy("compiled-preferred").?);
    try std.testing.expectEqual(Strategy.compiled_preferred, parseStrategy("compiled_preferred").?);
    try std.testing.expectEqual(Strategy.compiled_required, parseStrategy("compiled-required").?);
    try std.testing.expectEqual(Strategy.compiled_required, parseStrategy("compiled_required").?);
}

test "graph partition report env parsers accept explicit values" {
    try std.testing.expect(parseEnvBool("1"));
    try std.testing.expect(parseEnvBool("true"));
    try std.testing.expect(parseEnvBool("YES"));
    try std.testing.expect(parseEnvBool("on"));
    try std.testing.expect(!parseEnvBool(""));
    try std.testing.expect(!parseEnvBool("0"));

    try std.testing.expectEqual(@as(?BackendKind, .metal), parsePartitionReportTarget("metal"));
    try std.testing.expectEqual(@as(?BackendKind, .native), parsePartitionReportTarget("NATIVE"));
    try std.testing.expectEqual(@as(?BackendKind, null), parsePartitionReportTarget("gpu"));
}

fn buildTwoPartitionGateTestPlan(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    first_node: NodeId,
    second_node: NodeId,
    first_backend: BackendKind,
    second_backend: BackendKind,
) !partition_mod.PartitionPlan {
    const node_count: usize = @intCast(graph.nodeCount());
    const partitions = try allocator.alloc(partition_mod.Partition, 2);
    errdefer allocator.free(partitions);

    const assignment = try allocator.alloc(u32, node_count);
    errdefer allocator.free(assignment);
    @memset(assignment, 0);
    assignment[@intCast(second_node)] = 1;

    const node_operator_plans = try allocator.alloc(?operator_plan.OperatorPlan, node_count);
    errdefer allocator.free(node_operator_plans);
    @memset(node_operator_plans, null);

    const first_nodes = try allocator.dupe(NodeId, &.{first_node});
    errdefer allocator.free(first_nodes);
    const second_nodes = try allocator.dupe(NodeId, &.{second_node});
    errdefer allocator.free(second_nodes);
    const first_external_inputs = try allocator.alloc(partition_mod.ExternalInput, 0);
    errdefer allocator.free(first_external_inputs);
    const second_external_inputs = try allocator.alloc(partition_mod.ExternalInput, 0);
    errdefer allocator.free(second_external_inputs);

    partitions[0] = .{
        .backend = first_backend,
        .node_ids = first_nodes,
        .external_inputs = first_external_inputs,
    };
    partitions[1] = .{
        .backend = second_backend,
        .node_ids = second_nodes,
        .external_inputs = second_external_inputs,
    };
    return .{
        .partitions = partitions,
        .node_assignment = assignment,
        .node_operator_plans = node_operator_plans,
        .allocator = allocator,
    };
}

test "graph runtime partition gate summary counts fallback partitions" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    const out = try builder.neg(x);
    try graph.markOutput(out);

    var plan = try buildTwoPartitionGateTestPlan(allocator, &graph, x, out, .metal, .native);
    defer plan.deinit();

    const summary = summarizePartitionGates(&graph, &plan, .metal);
    try std.testing.expectEqual(@as(usize, 1), summary.fallback_nodes);
    try std.testing.expectEqualStrings("neg", summary.first_fallback_op.?);
    try std.testing.expectEqual(@as(usize, 0), summary.host_assisted_target_nodes);
}

test "graph runtime partition gates fail closed on fallback partitions" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    const out = try builder.neg(x);
    try graph.markOutput(out);

    var fallback_plan = try buildTwoPartitionGateTestPlan(allocator, &graph, x, out, .metal, .native);
    defer fallback_plan.deinit();
    try std.testing.expectError(
        error.GraphRuntimeFallbackPartition,
        enforceGraphRuntimePartitionGates(&graph, &fallback_plan, .metal, .{ .require_no_fallback = true }),
    );

    var resident_plan = try buildTwoPartitionGateTestPlan(allocator, &graph, x, out, .metal, .metal);
    defer resident_plan.deinit();
    try enforceGraphRuntimePartitionGates(&graph, &resident_plan, .metal, .{ .require_no_fallback = true });

    var parameter_only_fallback_plan = try buildTwoPartitionGateTestPlan(allocator, &graph, x, out, .native, .metal);
    defer parameter_only_fallback_plan.deinit();
    try enforceGraphRuntimePartitionGates(&graph, &parameter_only_fallback_plan, .metal, .{ .require_no_fallback = true });
}

test "native graph runtime attaches native partition executors" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    const out = try builder.neg(x);
    try graph.markOutput(out);

    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer {
        native_mod.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    var runtime = try Runtime.init(allocator, &graph, &cb, .partitioned);
    defer runtime.deinit();

    const plan = runtime.plan orelse return error.MissingGraphRuntimePlan;
    try std.testing.expect(plan.base.partitions.len > 0);
    for (plan.base.partitions) |part| {
        try std.testing.expect(part.executor != null);
    }
}
