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
const cache_mod = @import("cache.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const compiled_backend = @import("compiled_backend.zig");
const compiled_registry = @import("compiled_registry.zig");
const interpreter = @import("interpreter.zig");
const partition_mod = @import("partition.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const backend_policy = @import("backend_policy.zig");
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

pub const Strategy = @import("../execution/strategy.zig").Strategy;

pub const Result = struct {
    stats: partition_mod.PartitionExecutor.ExecutionStats = .{},
    outputs: []contracts.CT,
    output_devices: ?[]device_mesh_mod.DeviceId = null,
    owner_backend: ?ComputeBackend = null,
    output_backends: ?[]ComputeBackend = null,
    fallback_owner: ?*FallbackNativeBackend = null,
    allocator: std.mem.Allocator,

    pub fn backendFor(self: *const Result, runtime: *const Runtime, index: usize) *const ComputeBackend {
        if (self.output_backends) |backends| return &backends[index];
        if (self.owner_backend) |*backend| return backend;
        if (self.output_devices) |devices| {
            const mesh = &(runtime.mesh orelse unreachable);
            return mesh.device(devices[index]).?.backend;
        }
        return runtime.default_backend;
    }

    pub fn deinit(self: *Result, runtime: *const Runtime) void {
        for (self.outputs, 0..) |ct, i| {
            const backend = self.backendFor(runtime, i);
            var duplicate = false;
            for (self.outputs[0..i], 0..) |previous, j| {
                const owner = self.backendFor(runtime, j);
                if (previous == ct and owner.ptr == backend.ptr and owner.vtable == backend.vtable) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) backend.free(ct);
        }
        if (self.output_devices) |devices| self.allocator.free(devices);
        if (self.output_backends) |backends| self.allocator.free(backends);
        self.allocator.free(self.outputs);
        if (self.fallback_owner) |owner| owner.release();
        self.outputs = &.{};
        self.output_devices = null;
        self.output_backends = null;
        self.fallback_owner = null;
    }
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    strategy: Strategy,
    default_backend: *const ComputeBackend,
    mesh: ?device_mesh_mod.DeviceMesh = null,
    plan: ?multi_executor.DevicePartitionPlan = null,
    buffer_plan: ?buffer_plan_mod.BufferPlan = null,
    partition_last_use: ?[]u32 = null,
    physical_pool: ?*@import("../execution/physical_buffers.zig").Pool = null,
    runtime_cache_entry: ?cache_mod.CacheEntry = null,
    native_partition_executors: std.ArrayListUnmanaged(*native_partition_executor.NativePartitionExecutor) = .empty,
    // DeviceMesh borrows this backend across moves of Runtime itself.
    fallback_native: ?*FallbackNativeBackend = null,

    pub const InitOptions = struct {
        tensor_descs: ?[]const ?@import("backend_contracts.zig").TensorDesc = null,
        analysis: ?interpreter.CachedAnalysis = null,
        region_costs: ?@import("../execution/placement.zig").RegionCosts = .{},
    };

    pub fn init(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        backend: *const ComputeBackend,
        requested_strategy: Strategy,
    ) !Runtime {
        return initWithOptions(allocator, graph, backend, requested_strategy, .{});
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, graph: *const Graph, backend: *const ComputeBackend, requested_strategy: Strategy, options: InitOptions) !Runtime {
        var runtime = Runtime{
            .allocator = allocator,
            .strategy = requested_strategy,
            .default_backend = backend,
        };
        errdefer runtime.deinit();

        if (requested_strategy != .interpreter) {
            try runtime.initSingleDevicePlan(graph, backend, options);
            if (options.analysis) |analysis| runtime.partition_last_use = try multi_executor.partitionLastUses(allocator, graph, &runtime.plan.?.base, analysis.reachable, analysis.last_use);
            if (requested_strategy == .compiled_required and !runtime.allComputePartitionsHaveAttachedExecutors(graph)) {
                return error.UnsupportedCompiledGraphRuntime;
            }
        }

        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        if (self.partition_last_use) |uses| self.allocator.free(uses);
        self.partition_last_use = null;
        if (self.physical_pool) |pool| pool.deinit();
        self.physical_pool = null;
        if (self.buffer_plan) |*plan| plan.deinit();
        self.buffer_plan = null;
        if (self.plan) |*plan| {
            plan.deinit();
            self.plan = null;
        }
        for (self.native_partition_executors.items) |exec| {
            exec.partitionExecutor().deinitExecutor();
        }
        self.native_partition_executors.deinit(self.allocator);
        if (self.runtime_cache_entry) |*entry| {
            entry.resetCompiledModelExecutor();
            entry.resetCompiledPartitions();
            self.runtime_cache_entry = null;
        }
        if (self.mesh) |*mesh| {
            mesh.deinit();
            self.mesh = null;
        }
        if (self.fallback_native) |fallback| {
            fallback.release();
            self.fallback_native = null;
        }
    }

    pub fn execute(
        self: *Runtime,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        options: interpreter.ExecuteOptions,
    ) !Result {
        const transfer_audit = @import("../execution/transfer_audit.zig");
        var audit = transfer_audit.Audit{ .forbid_device_host_access = options.activation_readbacks == .forbidden };
        const auditing = options.collect_partition_stats or audit.forbid_device_host_access;
        if (audit.forbid_device_host_access) try transfer_audit.validateBackend(self.default_backend.kind());
        if (auditing) try transfer_audit.begin(&audit);
        defer if (auditing) transfer_audit.end();
        var result = try self.executeImpl(allocator, graph, options);
        audit.recordStats(&result.stats);
        return result;
    }

    fn addPhysicalStats(stats: *partition_mod.PartitionExecutor.ExecutionStats, physical: @import("../execution/physical_buffers.zig").Stats) void {
        stats.physical_replay = physical.replay_decision;
        stats.assigned_kernel_dispatches += physical.dispatches;
        stats.physical_transfer_dispatches += physical.transfers;
        stats.physical_transfer_bytes += physical.transfer_bytes;
        stats.physical_storage_allocations += physical.allocations;
        stats.physical_storage_bytes += physical.allocated_bytes;
        stats.physical_storage_reuses += physical.reused_allocations;
        stats.physical_graph_captures += physical.graph_captures;
        stats.physical_capture_failures += physical.capture_failures;
        stats.physical_replay_error = physical.capture_failure;
        stats.physical_graph_replays += physical.graph_replays;
        stats.physical_input_copies += physical.input_copies;
        stats.physical_input_copy_bytes += physical.input_copy_bytes;
    }

    fn submitPreparedFrame(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        plan: *const multi_executor.DevicePartitionPlan,
        mesh: *const device_mesh_mod.DeviceMesh,
        options: interpreter.ExecuteOptions,
        frame: ?*@import("../execution/physical_buffers.zig").Frame,
        capturing: *bool,
    ) !multi_executor.MultiExecutionResult {
        var result = multi_executor.executeMultiDevice(allocator, graph, plan, mesh, options) catch |err| recover: {
            if (!capturing.*) return err;
            frame.?.abortReplayCapture(err);
            capturing.* = false;
            break :recover try multi_executor.executeMultiDevice(allocator, graph, plan, mesh, options);
        };
        if (!capturing.*) return result;
        const active = frame.?;
        active.commitReplayCapture() catch |err| {
            // Recording/instantiation has not submitted compute. Preserve the
            // ordinary execution path when optional capture cannot be prepared.
            active.abortReplayCapture(err);
            capturing.* = false;
            result.deinit(mesh);
            return multi_executor.executeMultiDevice(allocator, graph, plan, mesh, options);
        };
        capturing.* = false;
        errdefer result.deinit(mesh);
        // A submission error is an execution error and must propagate.
        try active.replay.?.launch();
        return result;
    }

    fn executeImpl(self: *Runtime, allocator: std.mem.Allocator, graph: *const Graph, options: interpreter.ExecuteOptions) !Result {
        switch (self.strategy) {
            .interpreter => {
                var result = try interpreter.execute(allocator, graph, self.default_backend, options);
                errdefer result.deinit(self.default_backend);
                const outputs = result.outputs;
                result.outputs = &.{};
                return .{
                    .outputs = outputs,
                    .owner_backend = durableBackend(self.default_backend.*),
                    .allocator = result.allocator,
                };
            },
            .partitioned, .compiled_preferred, .compiled_required => {
                const plan = self.plan orelse return error.MissingGraphRuntimePlan;
                const mesh = self.mesh orelse return error.MissingGraphRuntimeMesh;
                const frame = if (self.physical_pool) |pool| try pool.acquire() else null;
                var frame_finished = false;
                defer if (frame) |active| {
                    if (!frame_finished) {
                        if (active.finish()) |_| {} else |err| std.log.warn("physical frame completion failed during cleanup: {s}", .{@errorName(err)});
                    }
                };
                if (frame) |active| try active.begin();
                var planned_options = options;
                planned_options.physical_frame = frame;
                const replay_inputs = if (frame) |active| try active.bindReplayInputs(options) else null;
                defer if (replay_inputs != null) frame.?.releaseReplayInputs();
                if (replay_inputs) |inputs| planned_options.runtime_inputs = inputs;
                if (frame) |active| if (replay_inputs != null and active.replay_ready) {
                    var replayed = Result{
                        .outputs = try active.replayOutputs(allocator),
                        .owner_backend = durableBackend(self.default_backend.*),
                        .allocator = allocator,
                    };
                    errdefer replayed.deinit(self);
                    frame_finished = true;
                    addPhysicalStats(&replayed.stats, try active.finish());
                    return replayed;
                };
                var capturing = if (frame) |active| if (replay_inputs != null) active.beginReplayCapture() catch |err| recover: {
                    active.abortReplayCapture(err);
                    break :recover false;
                } else false else false;
                defer if (capturing) frame.?.replay.?.abort();

                if (planned_options.cached_partition_last_use == null) planned_options.cached_partition_last_use = self.partition_last_use;
                if (planned_options.cached_buffer_plan == null) {
                    if (self.buffer_plan) |*buffers| {
                        planned_options.cached_buffer_plan = buffers;
                        planned_options.cached_buffer_plan_validated = true;
                    }
                }
                var result = try submitPreparedFrame(allocator, graph, &plan, &mesh, planned_options, frame, &capturing);
                errdefer result.deinit(&mesh);
                if (frame) |active| if (replay_inputs != null) {
                    active.warmed = active.stats.dispatches == active.pool.replay_kernel_count;
                };
                if (options.output_placement == .default_backend) try self.normalizeOutputsToDefaultDevice(allocator, &result);
                if (frame) |active| {
                    frame_finished = true;
                    const physical_stats = try active.finish();
                    addPhysicalStats(&result.stats, physical_stats);
                } else if (self.physical_pool != null) {
                    result.stats.physical_frame_busy_fallbacks += 1;
                    result.stats.physical_replay = .busy_frame;
                }
                // Copy representation owners before detaching outputs. Result
                // cleanup must not depend on an evicted executable's mesh.
                const devices = if (options.output_placement == .producer) result.output_devices else null;
                var output_backends: ?[]ComputeBackend = null;
                var fallback_owner: ?*FallbackNativeBackend = null;
                if (devices) |ids| {
                    var foreign = false;
                    for (ids) |device| if (device != 0) {
                        foreign = true;
                    };
                    if (foreign) {
                        const owners = try allocator.alloc(ComputeBackend, ids.len);
                        errdefer allocator.free(owners);
                        for (ids, owners) |device, *owner| owner.* = durableBackend((mesh.device(device) orelse return error.DeviceNotFound).backend.*);
                        output_backends = owners;
                        if (self.fallback_native) |fallback| for (owners) |owner| {
                            if (owner.ptr == fallback.backend.ptr and owner.vtable == fallback.backend.vtable) {
                                fallback.retain();
                                fallback_owner = fallback;
                                break;
                            }
                        };
                    }
                }
                const outputs = result.outputs;
                result.outputs = &.{};
                if (devices == null) allocator.free(result.output_devices);
                result.output_devices = &.{};
                return .{
                    .outputs = outputs,
                    .output_devices = devices,
                    .owner_backend = durableBackend(self.default_backend.*),
                    .output_backends = output_backends,
                    .fallback_owner = fallback_owner,
                    .stats = result.stats,
                    .allocator = result.allocator,
                };
            },
        }
    }

    fn initSingleDevicePlan(
        self: *Runtime,
        graph: *const Graph,
        backend: *const ComputeBackend,
        options: InitOptions,
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
                .supports = backend_policy.supports(target_kind),
                .decide = backend_policy.decide(target_kind),
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
                .supports = backend_policy.supports(target_kind),
                .decide = backend_policy.decide(target_kind),
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
        if (options.tensor_descs) |provided| {
            if (provided.len != descriptor_seeds.len) return error.InvalidTensorDescriptorTable;
            for (provided, descriptor_seeds) |desc, *seed| if (desc) |value| {
                seed.* = value;
            };
        }

        var diagnostics = partition_mod.CapabilityDiagnostics{};
        var base = try partition_mod.partitionWithOptions(self.allocator, graph, capabilities_buf[0..capability_count], .{
            .tensor_descs = descriptor_seeds,
            .diagnostics = &diagnostics,
            .region_costs = if (use_native_fallback and (target_kind == .metal or target_kind == .cuda)) options.region_costs else null,
        });
        // Once self.plan takes ownership of base/assignments (below), plan.deinit
        // (run via the Runtime.init errdefer) is the sole owner. Guard the local
        // ownership errdefers with this flag so a failure in the compiled-executor
        // attach after installation does not free base/assignments a second time.
        var plan_installed = false;
        errdefer if (!plan_installed) base.deinit();
        if (graphPartitionReportRequested()) {
            dumpPartitionReport(graph, &base, target_kind, use_native_fallback, &diagnostics);
        }
        try enforceGraphRuntimePartitionGates(graph, &base, target_kind, .{
            .require_no_fallback = graphRuntimeRequireNoFallbackRequested(),
            .require_no_host_assisted = graphRuntimeRequireNoHostAssistedRequested(),
        });

        const assignments = try self.allocator.alloc(device_mesh_mod.DeviceId, base.partitions.len);
        errdefer if (!plan_installed) self.allocator.free(assignments);
        for (base.partitions, 0..) |part, i| {
            assignments[i] = if (use_native_fallback and part.backend == .native) 1 else 0;
        }
        try self.attachNativePartitionExecutors(graph, &base, assignments);

        self.plan = .{
            .base = base,
            .device_assignment = assignments,
            .allocator = self.allocator,
        };
        plan_installed = true;
        try self.attachCompiledPartitionExecutors(graph, backend, target_kind);
        self.buffer_plan = try buffer_plan_mod.build(self.allocator, graph, &self.plan.?.base, .{});
        var physical_backends: [2]*const ComputeBackend = undefined;
        for (devices_buf[0..device_count], 0..) |device, i| physical_backends[i] = device.backend;
        self.physical_pool = try @import("../execution/physical_buffers.zig").Pool.create(self.allocator, graph, &self.buffer_plan.?, physical_backends[0..device_count], self.plan.?.base.node_operator_plans);
        try self.buffer_plan.?.validate(graph, &self.plan.?.base);
    }

    fn allComputePartitionsHaveAttachedExecutors(self: *const Runtime, graph: *const Graph) bool {
        const plan = self.plan orelse return false;
        var compute_partitions: usize = 0;
        for (plan.base.partitions) |part| {
            if (!partitionHasCompute(graph, part)) continue;
            compute_partitions += 1;
            if (part.executor == null) return false;
        }
        return compute_partitions > 0;
    }

    fn ensureRuntimeCacheEntry(self: *Runtime, graph: *const Graph) void {
        if (self.runtime_cache_entry != null) return;
        self.runtime_cache_entry = .{
            .key = .{
                .config_hash = 0,
                .batch = 0,
                .seq_len = 0,
                .attention_mode = .full_recompute,
            },
            .graph = graph.*,
            .resource_allocator = self.allocator,
        };
    }

    fn attachCompiledPartitionExecutors(
        self: *Runtime,
        graph: *const Graph,
        backend: *const ComputeBackend,
        target_kind: BackendKind,
    ) !void {
        const backend_def = compiled_registry.find(target_kind) orelse return;
        const plan = if (self.plan) |*p| p else return error.MissingGraphRuntimePlan;
        const context = compiled_backend.AttachContext{
            .cb = backend,
            .requested_backend = target_kind,
            .attachment_target = .partitioned,
        };
        if (!backend_def.should_attach(context, .single_device)) return;
        if (!backend_def.has_compilable_partition(graph, plan, context, .single_device)) return;
        self.ensureRuntimeCacheEntry(graph);
        try backend_def.attach_executors(
            self.allocator,
            &self.runtime_cache_entry.?,
            graph,
            plan,
            context,
            .single_device,
        );
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
            errdefer exec.partitionExecutor().deinitExecutor();
            try self.native_partition_executors.append(self.allocator, exec);
            // Ownership transfers to self.native_partition_executors immediately.
            // Clear base.owns_executors before assigning part.executor so a later
            // mid-loop error cannot make base.deinit double-free the executors that
            // are already tracked in self.native_partition_executors.
            base.owns_executors = false;
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
        errdefer compute.deinit();
        const fallback = try self.allocator.create(FallbackNativeBackend);
        fallback.* = .{
            .allocator = self.allocator,
            .weight_store = weight_store,
            .compute = compute,
            .backend = compute.computeBackend(),
        };
        self.fallback_native = fallback;
    }

    fn normalizeOutputsToDefaultDevice(
        self: *Runtime,
        allocator: std.mem.Allocator,
        result: *multi_executor.MultiExecutionResult,
    ) !void {
        const mesh = self.mesh orelse return;
        const default_entry = mesh.device(0) orelse return error.DeviceNotFound;
        for (result.outputs, result.output_devices, 0..) |ct, dev_id, index| {
            if (dev_id == 0) continue;
            const src_entry = mesh.device(dev_id) orelse return error.DeviceNotFound;
            const transferred = try multi_executor.transferTensor(allocator, ct, src_entry.backend, default_entry.backend);
            src_entry.backend.free(ct);
            // Repeated graph outputs may alias the same handle. Transfer and
            // release it once, then update all matching bindings together.
            for (result.outputs[index..], result.output_devices[index..]) |*alias, *owner| {
                if (alias.* == ct and owner.* == dev_id) {
                    alias.* = transferred;
                    owner.* = 0;
                }
            }
        }
    }
};

fn durableBackend(backend: ComputeBackend) ComputeBackend {
    var retained = backend;
    retained.execution_control = null;
    retained.embedding_stage = null;
    return retained;
}

const FallbackNativeBackend = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    weight_store: *WeightStore,
    compute: *NativeCompute,
    backend: ComputeBackend,

    fn retain(self: *FallbackNativeBackend) void {
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0);
    }
    fn release(self: *FallbackNativeBackend) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        const allocator = self.allocator;
        if (comptime @import("builtin").os.tag == .freestanding) unreachable;
        self.backend.deinit();
        native_mod.deinitPrefetchQueue(self.weight_store);
        self.weight_store.resident_weights.deinit(allocator);
        self.weight_store.lazy_weights.deinit(allocator);
        allocator.destroy(self.weight_store);
        allocator.destroy(self);
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

fn partitionHasCompute(graph: *const Graph, part: partition_mod.Partition) bool {
    for (part.node_ids) |node_id| {
        if (graphPartitionNodeHasCompute(graph, node_id)) return true;
    }
    return false;
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
            .supports = backend_policy.supports(target_kind),
            .decide = backend_policy.decide(target_kind),
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

    if (diagnostics) |d| {
        if (d.coalesced_regions > 0) std.debug.print(
            "graph-partition-report native_regions_coalesced={d} nodes_coalesced={d} estimated_work_units_saved={d} reason=avoid_device_roundtrip\n",
            .{ d.coalesced_regions, d.coalesced_nodes, d.estimated_region_cost_saved },
        );
    }

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
                if (include_parts) {
                    const node = graph.node(node_id);
                    std.debug.print("graph-partition-report fallback node={d} op={s} shape={any} attrs={any}\n", .{ node_id, @tagName(node.op), node.output_shape, node.op });
                    for (node.getInputs()) |input| if (input != ml.graph.null_node) {
                        std.debug.print("graph-partition-report fallback input={d} shape={any}\n", .{ input, graph.node(input).output_shape });
                    };
                }
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
        .pjrt => .pjrt,
        .cuda => .cuda,
        .wasm => .wasm,
        .webgpu => .webgpu,
        else => .native,
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
    defer compute.deinit();
    var cb = compute.computeBackend();

    var runtime = try Runtime.init(allocator, &graph, &cb, .partitioned);
    defer runtime.deinit();

    const plan = runtime.plan orelse return error.MissingGraphRuntimePlan;
    try std.testing.expect(plan.base.partitions.len > 0);
    for (plan.base.partitions) |part| {
        try std.testing.expect(part.executor != null);
    }
}

test "native partitioned runtimes restore request dimensions after flattened projection" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = Builder.init(&graph);

    // Mirrors the dynamic sequence-classifier topology used by imported ONNX
    // rerankers: [batch, sequence, hidden] is flattened for a projection and
    // then restored from symbolic graph dimensions.
    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, -1, 4 }));
    const weight = try builder.parameter("weight", Shape.init(.f32, &.{ 4, 5 }));
    const flat = try builder.reshape(x, Shape.init(.f32, &.{ -1, 4 }));
    const projected = try builder.matmul(flat, weight);
    const restored = try builder.reshape(projected, Shape.init(.f32, &.{ -1, -1, 5 }));
    try graph.markOutput(restored);

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
    defer compute.deinit();
    var cb = compute.computeBackend();

    const x_data = [_]f32{
        1,  2,  3,  4,
        5,  6,  7,  8,
        9,  10, 11, 12,
        13, 14, 15, 16,
        17, 18, 19, 20,
        21, 22, 23, 24,
    };
    const weight_data = [_]f32{
        1, 0, 0, 0, 1,
        0, 1, 0, 0, 1,
        0, 0, 1, 0, 1,
        0, 0, 0, 1, 1,
    };
    const x_value = try cb.fromFloat32Shape(&x_data, &.{ 2, 3, 4 });
    defer cb.free(x_value);
    const weight_value = try cb.fromFloat32Shape(&weight_data, &.{ 4, 5 });
    defer cb.free(weight_value);
    const runtime_inputs = [_]interpreter.RuntimeInput{
        .{ .node_id = x, .value = x_value },
        .{ .node_id = weight, .value = weight_value },
    };
    var analysis = try interpreter.CachedAnalysis.compute(allocator, &graph);
    defer analysis.deinit(allocator);

    for ([_]Strategy{ .partitioned, .compiled_preferred }) |strategy| {
        var runtime = try Runtime.init(allocator, &graph, &cb, strategy);
        defer runtime.deinit();

        var result = try runtime.execute(allocator, &graph, .{
            .runtime_inputs = &runtime_inputs,
            .cached_analysis = analysis,
        });
        defer result.deinit(&runtime);

        const actual_shape = try cb.tensorShape(result.outputs[0], allocator);
        defer allocator.free(actual_shape);
        try std.testing.expectEqualSlices(i64, &.{ 2, 3, 5 }, actual_shape);

        const actual = try cb.toFloat32(result.outputs[0], allocator);
        defer allocator.free(actual);
        try std.testing.expectEqual(@as(usize, 30), actual.len);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 10 }, actual[0..5]);
        try std.testing.expectEqualSlices(f32, &.{ 21, 22, 23, 24, 90 }, actual[25..30]);
    }
}

test "graph runtime producer outputs retain fallback ownership and repeated bindings" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var b = Builder.init(&graph);
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const indices = try b.parameter("indices", Shape.init(.i32, &.{2}));
    // LayerNorm now has a qualified general CUDA route. Use an unsupported
    // non-row gather to force the ownership boundary this regression needs.
    const out = try b.gather(x, indices, Shape.init(.f32, &.{ 2, 2 }));
    graph.nodeMut(out).op.gather.axis = 1;
    try graph.markOutput(out);
    try graph.markOutput(out);
    var weights = WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.resident_weights.deinit(a);
    defer weights.lazy_weights.deinit(a);
    var compute = NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const Probe = struct {
        fn kind(_: *anyopaque) BackendKind {
            return .cuda;
        }
    };
    var cb = compute.computeBackend();
    var vtable = cb.vtable.*;
    vtable.backendKind = Probe.kind;
    cb.vtable = &vtable;
    var runtime = try Runtime.init(a, &graph, &cb, .partitioned);
    var runtime_live = true;
    defer if (runtime_live) runtime.deinit();
    const value = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6 }, &.{ 2, 3 });
    defer cb.free(value);
    const index_values = (try cb.fromInt32Shape(&.{ 1, 0 }, &.{2})).?;
    defer cb.free(index_values);
    const inputs = [_]interpreter.RuntimeInput{ .{ .node_id = x, .value = value }, .{ .node_id = indices, .value = index_values } };
    var resident = try runtime.execute(a, &graph, .{ .runtime_inputs = &inputs, .output_placement = .producer });
    defer resident.deinit(&runtime);
    try std.testing.expectEqual(BackendKind.native, resident.backendFor(&runtime, 0).kind());
    const expected = try resident.backendFor(&runtime, 0).toFloat32(resident.outputs[0], a);
    defer a.free(expected);
    var normalized = try runtime.execute(a, &graph, .{ .runtime_inputs = &inputs });
    defer normalized.deinit(&runtime);
    try std.testing.expectEqual(BackendKind.cuda, normalized.backendFor(&runtime, 0).kind());
    for (normalized.outputs) |output| {
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    runtime.deinit();
    runtime_live = false;
    const after_eviction = try resident.backendFor(&runtime, 0).toFloat32(resident.outputs[0], a);
    defer a.free(after_eviction);
    try std.testing.expectEqualSlices(f32, expected, after_eviction);
}
