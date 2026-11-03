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

//! Physical-plan storage shared by native, Metal and CUDA assigned-output kernels.
//! Frames are reusable only after their values have released every lease, and
//! device submission dependencies are established before an address is reused.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const buffers = @import("../graph/buffer_plan.zig");
const storage = ops.physical_storage;
const accounting = @import("storage_accounting.zig");
const kernels = ops.kernel_descriptor;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const CT = ops.CT;
const interpreter = @import("../graph/interpreter.zig");
const Binding = @import("program.zig").Binding;
const admission = @import("storage_admission.zig");
const route = @import("kernel_route.zig");
const prepared_routes = @import("prepared_routes.zig");
const RouteSelection = @import("route_selection.zig").Selection;
pub const SharedLiteralStore = @import("immutable_literals.zig").SharedLiteralStore;
const ReplayDecision = @import("stats.zig").PhysicalReplayDecision;

pub const Stats = struct {
    storage_usage: ?accounting.Snapshot = null,
    replay_decision: ReplayDecision = .not_requested,
    dispatches: u64 = 0,
    transfers: u64 = 0,
    output_copies: u64 = 0,
    transfer_bytes: u64 = 0,
    allocations: u64 = 0,
    allocated_bytes: u64 = 0,
    reused_allocations: u64 = 0,
    input_copies: u64 = 0,
    input_copy_bytes: u64 = 0,
    graph_replays: u64 = 0,
    graph_captures: u64 = 0,
    capture_failures: u64 = 0,
    capture_failure: ?[]const u8 = null,
};

const AuxiliaryPlan = struct { requirement: kernels.Auxiliary, allocation: buffers.AllocationId };

pub const KernelPlan = struct {
    descriptor: kernels.Descriptor,
    operator_plan: ?ops.OperatorPlan = null,
    inputs: [3]NodeId,
    backend_index: usize,
    output_allocation: buffers.AllocationId,
    workspace_bytes: usize,
    host_workspace_bytes: usize = 0,
    auxiliaries: [kernels.max_auxiliaries]?AuxiliaryPlan = @splat(null),
};

const host_ingress = @import("host_ingress.zig");
const HostInputPlan = struct {
    borrowed: bool = false,
    index_bounds: ?host_ingress.IndexBounds = null,
    node: NodeId,
    shape: ml.graph.Shape,
    backend_index: usize,
    allocation: buffers.AllocationId,
    requirements: host_ingress.Requirements,
    epoch: u64,
    fn outputSlot(self: @This()) route.OutputSlot {
        return .{ .allocation = self.allocation, .node = self.node, .epoch = self.epoch, .logical = self.shape, .writable_bytes = self.requirements.bytes };
    }
};
const HostInputStorage = struct { resource: host_ingress.Upload, admission: ?*admission.Reservation, retirement_owner: ?*@import("backing_retirement.zig").BackingOwner = null, private_view: ?CT = null };

const ReplayInput = struct { node: NodeId, shape: ml.graph.Shape, allocation: ?buffers.AllocationId = null, constant: ?[]const f32 = null, constant_index: ?usize = null };
const ReplayOutput = struct { node: NodeId, allocation: buffers.AllocationId, shape: ml.graph.Shape };
const ReplayBindingState = struct { weight: ?CT = null, supplied: bool = false, host_direct: bool = false };

const TransferPlan = struct { shape: ml.graph.Shape, backend_index: usize, allocation: buffers.AllocationId };

/// Move-only exclusive claim. The specialization/cache owner must outlive this
/// token. Claiming performs no GPU work or admission; consuming it happens only
/// after request admission, against the exact identity whose quota was credited.
pub const FrameClaim = struct {
    frame: ?*Frame,
    identity: admission.Identity,
    credit: @import("../runtime/tier/memory.zig").AdmissionAmounts,
    /// Refine already-materialized spans to measured bytes; missing banks keep
    /// their cached cold upper bound. Null means capacity metadata is not ready.
    requirement: ?@import("../runtime/tier/memory.zig").AdmissionAmounts = null,
    nonce: u64,
    route_owner: ?*prepared_routes.PreparedRoutes = null,

    pub fn release(self: *FrameClaim) void {
        self.releaseWithCleanup(.{});
    }
    pub fn releaseWithCleanup(self: *FrameClaim, cleanup: @import("cleanup_context.zig").Context) void {
        const frame = self.frame orelse return;
        self.frame = null;
        const owner = self.route_owner;
        self.route_owner = null;
        defer if (owner) |value| value.releaseWithCleanup(cleanup);
        const pool = frame.pool;
        @import("antfly_platform").sync.lockYielding(&pool.mutex);
        defer pool.mutex.unlock();
        if (!frame.claimed or frame.claim_nonce != self.nonce) {
            std.debug.assert(false); // copied/stale ownership token
            return;
        }
        frame.claimed = false;
        frame.in_use = false;
    }

    fn consume(self: *FrameClaim, pool: *Pool, capability: ?admission.Capability) !*Frame {
        const frame = self.frame orelse return error.PhysicalFrameClaimConsumed;
        if (frame.pool != pool) return error.InvalidPhysicalFrameClaim;
        const cap = capability orelse return error.InvalidPhysicalFrameClaim;
        if (!self.identity.eql(cap.identity)) return error.PhysicalStorageAdmissionMismatch;
        @import("antfly_platform").sync.lockYielding(&pool.mutex);
        if (!frame.claimed or !frame.in_use or frame.claim_nonce != self.nonce) {
            pool.mutex.unlock();
            return error.InvalidPhysicalFrameClaim;
        }
        std.debug.assert(frame.owner_pin == null);
        self.frame = null;
        // Move the claim's existing route pin into its exclusively held frame.
        // No callback may invalidate the pool while preparation is unlocked.
        frame.owner_pin = self.route_owner orelse if (pool.route_owner) |owner| owner.retain() else null;
        self.route_owner = null;
        frame.claimed = false;
        pool.mutex.unlock();
        return pool.prepareAcquired(frame, cap);
    }
};

pub const Pool = struct {
    prepared_backings: std.atomic.Value(bool) = .init(false),
    allocator: std.mem.Allocator,
    accounting: ?*accounting.Ledger = null,
    owns_plan: bool = false,
    immutable_bindings: ?*prepared_routes.ImmutableBindings = null,
    variant_cache: ?*prepared_routes.Cache = null,
    selection: ?RouteSelection = null,
    coverage_graph: ?*const Graph = null,
    coverage_reachable: ?[]const bool = null,
    native_frame_dispatch: bool = false,
    coverage_backend: ?ops.ComputeBackend = null,
    payload_coverage: @import("owned_coverage.zig").Coverage = .{ .compatibility = .no_selected_routes },
    route_owner: ?*prepared_routes.PreparedRoutes = null,
    root_pool: ?*Pool = null,
    region_policy: @import("region_route.zig").SelectionPolicy = .select_regions,
    region_execution_profile: @import("region_route.zig").ExecutionProfile = .unknown,
    region_owner: ?@import("region_preparation.zig").Selected = null,
    regions: []@import("region_binding.zig").Installed = &.{},
    region_by_node: []u32 = &.{},
    region_runtime_last_use: []u32 = &.{},
    host_alignment: std.mem.Alignment = .@"1",
    reshape_sources: []NodeId = &.{},
    metadata_aliases: []@import("metadata_alias.zig").Plan = &.{},
    /// Borrow the plan's heap-backed slices, never its movable struct address.
    plan: buffers.BufferPlan,
    kernels: []?KernelPlan,
    transfers: []?TransferPlan,
    backends: []ops.ComputeBackend,
    workspace_allocations: []buffers.AllocationId,
    host_workspace_allocation: buffers.AllocationId = buffers.invalid_allocation,
    frames: std.ArrayListUnmanaged(*Frame) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    max_frames: usize = 4,
    /// Cold constructions reserve cache slots before releasing the mutex.
    pending_frames: usize = 0,
    host_input_plans: []HostInputPlan = &.{},
    replay_inputs: []ReplayInput = &.{},
    replay_input_indices: []u32 = &.{},
    literal_store: ?*SharedLiteralStore = null,
    replay_outputs: []ReplayOutput = &.{},
    replay_kernel_count: usize = 0,
    replay_config: ?struct { graph: *const Graph, bindings: []const Binding, costs: @import("placement.zig").RegionCosts } = null,
    replay_backend: ?usize = null,
    replay_decision: ReplayDecision = .unsupported_backend,
    storage_requirement: ?@import("../runtime/tier/memory.zig").AdmissionAmounts = null,
    storage_requirement_bounds: []usize = &.{},

    pub fn create(allocator: std.mem.Allocator, graph: *const Graph, plan: *buffers.BufferPlan, backends: []const *const ops.ComputeBackend, operator_plans: []const ?ops.OperatorPlan) !?*Pool {
        const prepared = try allocator.alloc(?KernelPlan, graph.nodeCount());
        errdefer allocator.free(prepared);
        @memset(prepared, null);
        const scratch_requirements = try allocator.alloc(kernels.WorkspaceRequirement, backends.len);
        defer allocator.free(scratch_requirements);
        @memset(scratch_requirements, .{});
        var count: usize = 0;
        var host_workspace_bytes: usize = 0;
        for (prepared, 0..) |*entry, id| {
            const slot = plan.slotForNode(@intCast(id)) orelse continue;
            if (slot.kind != .allocation or slot.allocation == buffers.invalid_allocation or (slot.backend != .native and slot.backend != .cuda and slot.backend != .metal)) continue;
            const descriptor = kernels.lower(graph, @intCast(id)) orelse continue;
            for (backends, 0..) |backend, backend_index| {
                if (backend.kind() != slot.backend) continue;
                const functions = backend.vtable.kernels orelse continue;
                if (!functions.accepts(backend.vtable)) continue;
                if (functions.runAssignedKernel == null or functions.physicalTensorView == null) continue;
                // New physical dtypes require an explicit provider contract;
                // legacy optional hooks only imply the original F32 surface.
                if (descriptor.output_shape.dtype != .f32 and functions.assignedKernelSupported == null) continue;
                if (functions.assignedKernelSupported) |supported| if (!supported(backend.ptr, &descriptor)) continue;
                const workspace = if (backend.vtable.kernels.?.assignedKernelWorkspace) |requirement| try requirement(backend.ptr, &descriptor) else kernels.WorkspaceRequirement{};
                if (workspace.byte_size % @sizeOf(f32) != 0) return error.InvalidWorkspaceSize;
                scratch_requirements[backend_index].byte_size = @max(scratch_requirements[backend_index].byte_size, workspace.byte_size);
                scratch_requirements[backend_index].alignment = @max(scratch_requirements[backend_index].alignment, workspace.alignment);
                host_workspace_bytes = @max(host_workspace_bytes, workspace.host_byte_size);
                var inputs: [3]NodeId = @splat(ml.graph.null_node);
                @memcpy(inputs[0..descriptor.input_count], graph.node(@intCast(id)).getInputs());
                entry.* = .{ .descriptor = descriptor, .operator_plan = if (id < operator_plans.len) operator_plans[id] else null, .inputs = inputs, .backend_index = backend_index, .output_allocation = slot.allocation, .workspace_bytes = workspace.byte_size, .host_workspace_bytes = workspace.host_byte_size };
                count += 1;
                break;
            }
        }
        // Sharing a representation plane by primary allocation preserves the
        // primary reuse schedule instead of allocating one mirror per node.
        var planes = std.AutoHashMap(u64, buffers.AllocationId).init(allocator);
        defer planes.deinit();
        for (prepared) |*maybe_kernel| {
            const kernel = if (maybe_kernel.*) |*value| value else continue;
            const backend = backends[kernel.backend_index];
            const get_auxiliaries = backend.vtable.kernels.?.assignedKernelAuxiliaries orelse continue;
            const requirements = try get_auxiliaries(backend.ptr, &kernel.descriptor);
            if (requirements.count > kernels.max_auxiliaries) return error.InvalidAuxiliaryRequirements;
            var roles: u32 = 0;
            for (requirements.items(), 0..) |requirement, auxiliary_index| {
                const role_bit = @as(u32, 1) << @intCast(@backingInt(requirement.role));
                if (roles & role_bit != 0) return error.InvalidAuxiliaryRequirements;
                roles |= role_bit;
                const elements = requirement.shape.numElements() orelse return error.InvalidAuxiliaryRequirements;
                if (elements <= 0 or elements > std.math.maxInt(i32)) return error.InvalidAuxiliaryRequirements;
                const bytes = try std.math.mul(usize, @intCast(elements), requirement.shape.dtype.byteSize());
                const key = @as(u64, kernel.output_allocation) << 32 | @backingInt(requirement.role);
                const entry = try planes.getOrPut(key);
                if (!entry.found_existing) entry.value_ptr.* = try plan.addAuxiliaryPlane(kernel.output_allocation, bytes);
                const allocation = entry.value_ptr.*;
                const padded = try std.math.add(usize, bytes, @sizeOf(f32) - 1);
                plan.allocations[allocation].byte_size = @max(plan.allocations[allocation].byte_size, padded / @sizeOf(f32) * @sizeOf(f32));
                kernel.auxiliaries[auxiliary_index] = .{ .requirement = requirement, .allocation = allocation };
            }
        }
        const transfers = try allocator.alloc(?TransferPlan, plan.transfers.len);
        errdefer allocator.free(transfers);
        @memset(transfers, null);
        for (plan.transfers, 0..) |edge, edge_index| {
            const download = edge.target_backend == .native and edge.source_backend != .native;
            const upload = edge.target_backend == .cuda and edge.source_backend == .native;
            if (!download and !upload) continue;
            const shape = graph.node(edge.source_node).output_shape;
            const elements = shape.numElements() orelse continue;
            if (shape.dtype != .f32 or elements <= 0 or elements > std.math.maxInt(i32)) continue;
            for (backends, 0..) |backend, index| {
                if (backend.kind() != edge.target_backend) continue;
                const functions = backend.vtable.kernels orelse continue;
                if (functions.physicalTensorView == null) continue;
                if (download and backend.vtable.hostFloat32Write == null) continue;
                if (upload and backend.vtable.kernels.?.writePhysicalFloat32 == null) continue;
                transfers[edge_index] = .{ .shape = shape, .backend_index = index, .allocation = edge.allocation };
                count += 1;
                break;
            }
        }
        const owned_backends = try allocator.alloc(ops.ComputeBackend, backends.len);
        errdefer allocator.free(owned_backends);
        for (backends, owned_backends) |backend, *owned| owned.* = backend.*;
        const literal_store = try SharedLiteralStore.create(allocator, graph, plan, owned_backends);
        errdefer if (literal_store) |store| store.release();
        if (count == 0 and literal_store == null) {
            allocator.free(prepared);
            allocator.free(transfers);
            allocator.free(owned_backends);
            return null;
        }
        const workspace_ids = try allocator.alloc(buffers.AllocationId, backends.len);
        errdefer allocator.free(workspace_ids);
        @memset(workspace_ids, buffers.invalid_allocation);
        for (owned_backends, scratch_requirements, 0..) |backend, requirement, index| {
            if (requirement.byte_size != 0) workspace_ids[index] = try plan.addWorkspace(backend.kind(), requirement.byte_size, requirement.alignment);
        }
        const host_workspace_allocation = if (host_workspace_bytes != 0) try plan.addHostWorkspace(host_workspace_bytes) else buffers.invalid_allocation;
        const reshape_sources = try allocator.alloc(NodeId, graph.nodeCount());
        errdefer allocator.free(reshape_sources);
        @memset(reshape_sources, ml.graph.null_node);
        for (graph.nodes.items, 0..) |node, node_id| {
            const attrs = switch (node.op) {
                .reshape => |value| value,
                else => continue,
            };
            if (attrs.runtime_shape or node.output_shape.dtype != .f32) continue;
            const source_node = node.inputs[0];
            const source_shape = graph.node(source_node).output_shape;
            const elements = source_shape.numElements() orelse continue;
            if (source_shape.dtype != .f32 or elements <= 0 or node.output_shape.numElements() != elements) continue;
            const slot = plan.slotForNode(@intCast(node_id)) orelse continue;
            const source = plan.slotForNode(source_node) orelse continue;
            if (slot.kind != .view or slot.source_slot != source.id or slot.backend != source.backend or slot.desc.quant_format != null) continue;
            reshape_sources[node_id] = source_node;
        }
        var metadata_aliases = std.array_list.Managed(@import("metadata_alias.zig").Plan).init(allocator);
        defer metadata_aliases.deinit();
        for (graph.nodes.items, 0..) |_, node_id| {
            const slot = plan.slotForNode(@intCast(node_id)) orelse continue;
            if (slot.kind != .view or slot.backend != .native or slot.desc.quant_format != null) continue;
            const alias = @import("metadata_alias.zig").Plan.lower(graph, @intCast(node_id), .native) orelse continue;
            const source = plan.slotForNode(alias.source) orelse continue;
            if (slot.source_slot != source.id or source.backend != .native) continue;
            for (owned_backends) |backend| {
                if (backend.kind() != .native) continue;
                const functions = backend.vtable.kernels orelse continue;
                if (!functions.accepts(backend.vtable) or functions.createMetadataAlias == null) continue;
                try metadata_aliases.append(alias);
                break;
            }
        }
        const owned_aliases = try metadata_aliases.toOwnedSlice();
        errdefer allocator.free(owned_aliases);
        const ledger = try accounting.Ledger.create(allocator);
        errdefer ledger.release();
        const self = try allocator.create(Pool);
        self.* = .{ .metadata_aliases = owned_aliases, .reshape_sources = reshape_sources, .literal_store = literal_store, .allocator = allocator, .accounting = ledger, .host_workspace_allocation = host_workspace_allocation, .plan = plan.*, .kernels = prepared, .transfers = transfers, .backends = owned_backends, .workspace_allocations = workspace_ids };
        return self;
    }

    /// Admit complete assigned-output CUDA graphs with explicit parameter
    /// roles. Graphs with unknown bindings, unsupported literals or partial
    /// descriptor coverage keep their existing execution path.
    pub fn prepareReplay(self: *Pool, graph: *const Graph, plan: *buffers.BufferPlan, bindings: []const Binding, costs: @import("placement.zig").RegionCosts) !void {
        std.debug.assert(self.frames.items.len == 0);
        if (self.root_pool == null) self.replay_config = .{ .graph = graph, .bindings = bindings, .costs = costs };
        self.storage_requirement = null;
        self.allocator.free(self.storage_requirement_bounds);
        self.storage_requirement_bounds = &.{};
        // Reuse the executable's cached reachability; dead bindings must not
        // introduce uploads, kernel costs, or capture dependencies.
        if (self.coverage_reachable) |reachable| if (reachable.len != graph.nodeCount()) return error.InvalidReachabilityTable;
        var backend_index: ?usize = null;
        for (self.backends, 0..) |backend, index| {
            const functions = backend.vtable.kernels orelse continue;
            if (backend.kind() == .cuda and functions.createPhysicalReplay != null and functions.copyPhysicalInput != null and functions.retainImmutableTensor != null and functions.physicalReplayTensorSupported != null and functions.physicalReplayKernelSupported != null) backend_index = index;
        }
        const index = backend_index orelse return;
        self.replay_decision = .unsupported_graph;
        if (costs.transfer_bytes_per_unit == 0) return error.InvalidRegionCosts;
        var input_count: u64 = 0;
        var input_bytes: u64 = 0;
        var count: usize = 0;
        var binding_count: usize = 0;
        for (graph.nodes.items, 0..) |node, id| {
            if (self.coverage_reachable) |reachable| if (!reachable[id]) continue;
            if (node.op == .constant) {
                const attrs = node.op.constant;
                const elements = node.output_shape.numElements() orelse return;
                if (node.output_shape.dtype != .f32 or elements <= 0 or attrs.data_len != elements or self.backends[index].vtable.kernels.?.writePhysicalFloat32 == null) return;
                const bytes = try std.math.mul(usize, attrs.data_len, @sizeOf(f32));
                if (attrs.data_offset > graph.constant_pool.items.len or bytes > graph.constant_pool.items.len - attrs.data_offset) return error.InvalidShape;
                binding_count += 1;
                continue;
            }
            if (node.op == .parameter) {
                binding_count += 1;
                if (node.output_shape.dtype != .f32) {
                    const host_index = self.hostInputIndex(@intCast(id)) orelse return;
                    if (self.host_input_plans[host_index].index_bounds == null) return;
                }
                var mutable = false;
                var immutable = false;
                for (bindings) |binding| if (binding.node_id == id) {
                    switch (binding.kind) {
                        .input, .state_input => mutable = true,
                        .weight => immutable = true,
                        else => {},
                    }
                };
                if (mutable == immutable) {
                    self.replay_decision = .incomplete_bindings;
                    return;
                }
                if (mutable) {
                    const elements = node.output_shape.numElements() orelse return;
                    if (elements <= 0) return;
                    if (self.hostInputIndex(@intCast(id)) == null) {
                        input_count += 1;
                        input_bytes = try std.math.add(u64, input_bytes, try std.math.mul(u64, @intCast(elements), node.output_shape.dtype.byteSize()));
                    }
                }
                continue;
            }
            if (node.op == .reshape and self.backends[index].vtable.kernels.?.physicalReplayReshapeViews) {
                const slot = plan.slotForNode(@intCast(id)) orelse return;
                if (slot.backend != .cuda or node.num_inputs != 1 or node.output_shape.dtype != .f32 or
                    node.output_shape.numElements() == null or node.output_shape.numElements() != graph.node(node.inputs[0]).output_shape.numElements()) return;
                continue;
            }
            const kernel = self.kernels[id] orelse return;
            if (kernel.backend_index != index) return;
            if (self.preparedKernelRoute(@intCast(id))) |owned| {
                if (owned.requirements.capture != .supported) return;
            } else if (!self.backends[index].vtable.kernels.?.physicalReplayKernelSupported.?(self.backends[index].ptr, &kernel.descriptor)) return;
            count += 1;
        }
        if (count == 0 or graph.outputs.items.len == 0) return;
        // Legality does not imply profitability. Charge the graph launch and
        // every bank copy (including read/write traffic), with a conservative
        // 2x margin. Single-kernel heads keep their existing direct dispatch.
        const cost = @import("../graph/cost.zig");
        const direct_cost = cost.checkedMul(@intCast(count), costs.launch);
        const copy_cost = cost.checkedAdd(cost.checkedMul(input_count, costs.transfer_latency), cost.checkedMul(input_bytes, 2) / costs.transfer_bytes_per_unit);
        if (direct_cost <= cost.checkedMul(cost.checkedAdd(costs.launch, copy_cost), 2)) {
            self.replay_decision = .unprofitable;
            return;
        }
        for (graph.outputs.items) |id| {
            const source = replayOutputSource(graph, id) orelse return;
            if (self.kernels[source] != null or graph.node(source).op == .constant) continue;
            var mutable = false;
            for (bindings) |binding| if (binding.node_id == source and (binding.kind == .input or binding.kind == .state_input)) {
                mutable = true;
                break;
            };
            if (!mutable) return;
        }
        const inputs = try self.allocator.alloc(ReplayInput, binding_count);
        errdefer self.allocator.free(inputs);
        const outputs = try self.allocator.alloc(ReplayOutput, graph.outputs.items.len);
        errdefer self.allocator.free(outputs);
        const input_indices = try self.allocator.alloc(u32, graph.nodeCount());
        errdefer self.allocator.free(input_indices);
        @memset(input_indices, ml.graph.null_node);
        // Allocation IDs are appended before any frame borrows the plan.
        // Refresh the borrowed slice even on an allocation failure.
        defer self.plan = plan.*;
        var input_index: usize = 0;
        for (graph.nodes.items, 0..) |node, id| {
            if (self.coverage_reachable) |reachable| if (!reachable[id]) continue;
            if (node.op != .parameter and node.op != .constant) continue;
            const input = &inputs[input_index];
            input_indices[id] = @intCast(input_index);
            input_index += 1;
            input.* = .{ .node = @intCast(id), .shape = node.output_shape };
            if (node.op == .constant) {
                const attrs = node.op.constant;
                input.constant = graph.constantData(attrs.data_offset, attrs.data_len);
                const store = self.literal_store orelse return error.InvalidLiteralPlan;
                const literal_index = store.node_indices[id];
                if (literal_index == ml.graph.null_node or store.literals[literal_index].backend_index != index) return error.InvalidLiteralPlan;
                input.constant_index = literal_index;
                input.allocation = store.literals[literal_index].allocation;
                continue;
            }
            for (bindings) |binding| if (binding.node_id == id and (binding.kind == .input or binding.kind == .state_input)) {
                input.allocation = if (self.hostInputIndex(@intCast(id))) |host| self.host_input_plans[host].allocation else try plan.addInputBinding(.cuda, try std.math.mul(usize, @intCast(input.shape.numElements().?), input.shape.dtype.byteSize()));
                break;
            };
        }
        for (graph.outputs.items, outputs) |id, *output| {
            const source = replayOutputSource(graph, id).?;
            const allocation = if (self.kernels[source]) |kernel| kernel.output_allocation else inputs[input_indices[source]].allocation orelse return error.InvalidShape;
            output.* = .{ .node = id, .allocation = allocation, .shape = graph.node(id).output_shape };
        }
        self.replay_inputs = inputs;
        self.replay_input_indices = input_indices;
        self.replay_outputs = outputs;
        self.replay_kernel_count = count;
        self.replay_backend = index;
        self.replay_decision = .warming;
    }

    fn replayOutputSource(graph: *const Graph, node_id: NodeId) ?NodeId {
        var id = node_id;
        for (0..graph.nodeCount()) |_| {
            const node = graph.node(id);
            if (node.op != .reshape) return id;
            if (node.num_inputs != 1 or node.inputs[0] >= graph.nodeCount()) return null;
            id = node.inputs[0];
        }
        return null;
    }

    /// Collect only unowned spans from the frozen actual plan. The warm frame
    /// is exclusively claimed and pinned before its mutable fields are read;
    /// the program's immutable store provides its own short metadata snapshot.
    /// No source capability/SDK/producer callback runs under either mutex.
    pub fn collectAdmissionRequirements(self: *Pool, builder: *@import("prepared_storage_admission.zig").Builder, warm: ?*const FrameClaim, identity: admission.Identity) !void {
        if (self.storage_requirement == null or self.storage_requirement_bounds.len != self.plan.allocations.len) return error.MissingPreparedStorageRequirement;
        var frame: ?*Frame = null;
        if (warm) |claim| {
            const current = claim.frame orelse return error.PhysicalFrameClaimConsumed;
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            const valid = current.pool == self and current.in_use and current.claimed and current.claim_nonce == claim.nonce and claim.identity.eql(identity);
            self.mutex.unlock();
            if (!valid) return error.InvalidPhysicalFrameClaim;
            frame = current;
        }
        for (self.storage_requirement_bounds, 0..) |bound, index| {
            if (bound == 0) continue;
            const allocation = self.plan.allocationById(@intCast(index)) orelse return error.InactivePhysicalAllocation;
            if (index == self.host_workspace_allocation) {
                if (frame) |current| if (current.host_workspace_admission) |reserved| {
                    if (!reserved.identity.eql(identity)) return error.PhysicalStorageAdmissionMismatch;
                    continue;
                };
                try builder.append(.{ .backend = .native, .class = .host_workspace, .bytes = if (frame) |current| current.host_workspace.len else bound });
                continue;
            }
            if (allocation.kind == .constant_binding) {
                const store = self.literal_store orelse return error.InvalidImmutableStorageClaim;
                var found = false;
                for (store.literals, 0..) |literal, literal_index| {
                    if (literal.allocation != index) continue;
                    if (self.selection != null and !self.literalEnabled(literal_index)) return error.InvalidImmutableStorageClaim;
                    const required = try store.unadmittedSpan(literal_index, identity);
                    if (required) |value| try builder.append(.{ .backend = value.backend, .class = .constant, .bytes = value.bytes });
                    found = true;
                    break;
                }
                if (!found) return error.InvalidImmutableStorageClaim;
                continue;
            }
            var bytes = bound;
            if (frame) |current| {
                if (index >= current.storages.len) return error.InvalidPhysicalAllocation;
                if (current.storages[index]) |lease| {
                    if (lease.admission) |reserved| {
                        if (!reserved.identity.eql(identity)) return error.PhysicalStorageAdmissionMismatch;
                        continue;
                    }
                    bytes = lease.backing_byte_size orelse lease.byte_size;
                    if (bytes > bound) return error.InvalidPhysicalStorageReservation;
                }
            }
            try builder.append(.{ .backend = allocation.backend, .class = if (allocation.kind == .workspace) .workspace else .activation, .bytes = bytes });
        }
        for (self.host_input_plans, 0..) |input, index| {
            if (input.requirements.host_bytes == 0) continue;
            if (frame) |current| {
                if (index >= current.host_input_resources.len) return error.InvalidPhysicalAllocation;
                if (current.host_input_resources[index]) |resource| if (resource.admission) |reserved| {
                    if (!reserved.identity.eql(identity)) return error.PhysicalStorageAdmissionMismatch;
                    continue;
                };
            }
            try builder.append(.{ .backend = .native, .class = .host_workspace, .bytes = input.requirements.host_bytes });
        }
    }

    pub fn requiresCleanupContext(self: *Pool) bool {
        if (self.prepared_backings.load(.acquire)) return true;
        if (self.literal_store) |store| if (store.prepared_backings.load(.acquire)) return true;
        if (self.variant_cache) |cache| {
            @import("antfly_platform").sync.lockYielding(&cache.mutex);
            defer cache.mutex.unlock();
            for (cache.entries.items) |variant| {
                if (variant.pool.prepared_backings.load(.acquire)) return true;
            }
        }
        return false;
    }
    pub fn deinit(self: *Pool) void {
        self.deinitWithCleanup(.{});
    }
    pub fn deinitWithCleanup(self: *Pool, cleanup: @import("cleanup_context.zig").Context) void {
        std.debug.assert(self.pending_frames == 0);
        if (self.variant_cache) |cache| {
            cache.deinitWithCleanup(cleanup);
            self.allocator.destroy(cache);
        }
        for (self.frames.items) |frame| frame.deinitWithCleanup(cleanup);
        self.deinitRegions();
        if (self.selection) |*selected| selected.deinitWithCleanup(cleanup);
        if (self.immutable_bindings) |immutable| immutable.releaseWithCleanup(cleanup);
        if (self.literal_store) |store| store.releaseWithCleanup(cleanup);
        if (self.owns_plan) self.plan.deinit();
        self.frames.deinit(self.allocator);
        self.allocator.free(self.host_input_plans);
        self.allocator.free(self.replay_inputs);
        self.allocator.free(self.replay_input_indices);
        self.allocator.free(self.replay_outputs);
        self.allocator.free(self.reshape_sources);
        self.allocator.free(self.metadata_aliases);
        self.allocator.free(self.kernels);
        self.allocator.free(self.transfers);
        self.allocator.free(self.workspace_allocations);
        self.allocator.free(self.backends);
        self.allocator.free(self.storage_requirement_bounds);
        if (self.accounting) |ledger| ledger.release();
        self.allocator.destroy(self);
    }

    /// Busy published outputs pin their frame. Bound retained storage; callers
    /// may use the observable compatibility fallback when all frames are pinned.
    /// Inspect retained quota only. No allocation, adoption, device wait or
    /// descriptor preparation may occur before the caller's admission decision.
    /// Capacity of the actual prepared output/auxiliary/workspace/input and
    /// transfer bindings. Aliased allocation IDs are counted once. This is an
    /// upper bound for selected physical storage, not a claim that legacy or
    /// input-dependent routes no longer need their compatibility workspace.
    pub fn storageRequirement(self: *Pool) !@import("../runtime/tier/memory.zig").AdmissionAmounts {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.storage_requirement) |cached| return cached;
        const seen = try self.allocator.alloc(usize, self.plan.allocations.len);
        errdefer self.allocator.free(seen);
        @memset(seen, 0);
        var amounts = @import("../runtime/tier/memory.zig").AdmissionAmounts{};
        if (self.host_workspace_allocation != buffers.invalid_allocation) {
            const id = self.host_workspace_allocation;
            amounts.host_scratch_bytes = @intCast(self.plan.allocations[id].byte_size);
            seen[id] = amounts.host_scratch_bytes;
        }
        for (self.kernels) |maybe| if (maybe) |kernel| {
            try self.addStorageRequirement(seen, kernel.backend_index, kernel.output_allocation, &amounts);
            if (kernel.workspace_bytes != 0) try self.addStorageRequirement(seen, kernel.backend_index, self.workspace_allocations[kernel.backend_index], &amounts);
            for (kernel.auxiliaries) |maybe_auxiliary| if (maybe_auxiliary) |auxiliary| {
                try self.addStorageRequirement(seen, kernel.backend_index, auxiliary.allocation, &amounts);
            };
        };
        for (self.transfers) |maybe| if (maybe) |transfer| try self.addStorageRequirement(seen, transfer.backend_index, transfer.allocation, &amounts);
        if (self.literal_store) |store| {
            for (store.literals, 0..) |literal, literal_index| if (self.selection == null or self.literalEnabled(literal_index)) try self.addStorageRequirement(seen, literal.backend_index, literal.allocation, &amounts);
            for (store.publications) |maybe| if (maybe) |publication| {
                if (self.selection != null and !self.literalEnabled(publication.literal)) continue;
                const literal = store.literals[publication.literal];
                try self.addStorageRequirement(seen, literal.backend_index, publication.allocation, &amounts);
            };
        }
        if (self.replay_backend) |index| for (self.replay_inputs) |input| {
            if (input.constant != null) continue;
            if (input.allocation) |id| try self.addStorageRequirement(seen, index, id, &amounts);
        };
        for (self.host_input_plans) |input| {
            if (!input.borrowed) try self.addStorageRequirement(seen, input.backend_index, input.allocation, &amounts);
            amounts.host_scratch_bytes = try std.math.add(usize, amounts.host_scratch_bytes, input.requirements.host_bytes);
        }
        self.storage_requirement_bounds = seen;
        self.storage_requirement = amounts;
        return amounts;
    }

    fn addStorageRequirement(self: *Pool, seen: []usize, index: usize, id: buffers.AllocationId, amounts: *@import("../runtime/tier/memory.zig").AdmissionAmounts) !void {
        if (id == buffers.invalid_allocation or id >= seen.len) return error.InvalidPhysicalAllocation;
        if (seen[id] != 0) return;
        const allocation = (self.plan.allocationById(id) orelse return error.InactivePhysicalAllocation).*;
        if (index >= self.backends.len) return error.InvalidPhysicalAllocation;
        const backend = self.backends[index];
        const bytes: usize = @intCast(allocation.byte_size);
        const functions = backend.vtable.kernels orelse return error.InvalidPhysicalAllocation;
        const bound = if (functions.physicalStorageReservationBytes) |query| try query(backend.ptr, bytes) else bytes;
        if (bound < bytes) return error.InvalidPhysicalStorageReservation;
        seen[id] = bound;
        switch (backend.kind()) {
            .native => amounts.host_scratch_bytes = try std.math.add(usize, amounts.host_scratch_bytes, bound),
            .cuda, .metal => amounts.backend_scratch_bytes = try std.math.add(usize, amounts.backend_scratch_bytes, bound),
            else => return error.InvalidPhysicalAllocation,
        }
    }

    pub fn configureRoutes(self: *Pool, inputs: []const route.InputBinding) !void {
        std.debug.assert(self.variant_cache == null and self.immutable_bindings == null);
        const immutable = try prepared_routes.ImmutableBindings.create(self.allocator, inputs);
        errdefer immutable.release();
        const cache = try self.allocator.create(prepared_routes.Cache);
        cache.* = .{ .allocator = self.allocator };
        self.immutable_bindings = immutable;
        self.variant_cache = cache;
    }

    pub fn prepareRoutes(self: *Pool, inputs: []const route.InputBinding) !?*prepared_routes.PreparedRoutes {
        return self.prepareRoutesWithPolicy(inputs, .select_regions);
    }
    const RegionPrepareContext = struct { root: *Pool, policy: @import("region_route.zig").SelectionPolicy, profile: @import("region_route.zig").ExecutionProfile };
    const RegionFilter = struct {
        policy: @import("region_route.zig").SelectionPolicy,
        profile: @import("region_route.zig").ExecutionProfile,
        pub fn accept(self: @This(), owner: *prepared_routes.PreparedRoutes) bool {
            return owner.pool.region_policy == self.policy and owner.pool.region_execution_profile == self.profile;
        }
    };
    fn prepareRegionVariant(context: RegionPrepareContext, inputs: []const route.InputBinding) !?*prepared_routes.PreparedRoutes {
        return prepareVariant(context.root, inputs, context.policy, context.profile);
    }
    pub fn prepareRoutesWithPolicy(self: *Pool, inputs: []const route.InputBinding, policy: @import("region_route.zig").SelectionPolicy) !?*prepared_routes.PreparedRoutes {
        return self.prepareRoutesWithProfile(inputs, policy, .unknown);
    }
    pub fn prepareRoutesWithProfile(self: *Pool, inputs: []const route.InputBinding, policy: @import("region_route.zig").SelectionPolicy, profile: @import("region_route.zig").ExecutionProfile) !?*prepared_routes.PreparedRoutes {
        return self.prepareRoutesWithProfileAndCleanup(inputs, policy, profile, .{});
    }
    pub fn prepareRoutesWithProfileAndCleanup(self: *Pool, inputs: []const route.InputBinding, policy: @import("region_route.zig").SelectionPolicy, profile: @import("region_route.zig").ExecutionProfile, cleanup: @import("cleanup_context.zig").Context) !?*prepared_routes.PreparedRoutes {
        const cache = self.variant_cache orelse return null;
        return cache.prepareFilteredWithCleanup(RegionPrepareContext{ .root = self, .policy = policy, .profile = profile }, inputs, prepareRegionVariant, RegionFilter{ .policy = policy, .profile = profile }, cleanup);
    }

    pub fn matchesRoutes(self: *const Pool, inputs: []const route.InputBinding) bool {
        const selected = if (self.selection) |*value| value else return false;
        return selected.matches(self, inputs) and self.matchesRegions(inputs);
    }
    pub fn validateRoutes(self: *const Pool, inputs: []const route.InputBinding) !void {
        const selected = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        try selected.validate(self, inputs);
        try self.validateRegions(inputs);
    }
    pub fn preparedKernelRoute(self: *const Pool, node: NodeId) ?*const route.Owned {
        const selected = if (self.selection) |*value| value else return null;
        if (node >= selected.routes.len or self.plan.slotForNode(node) == null) return null;
        return if (selected.routes[node]) |*value| value else null;
    }
    pub fn outputSlot(self: *const Pool, node: NodeId, bytes: usize) route.OutputSlot {
        if (self.selection == null or node >= self.kernels.len or self.kernels[node] == null or self.plan.slotForNode(node) == null) return .{ .node = node, .allocation = buffers.invalid_allocation, .epoch = if (self.selection) |selected| selected.epoch else 0, .logical = .{}, .writable_bytes = 0 };
        return self.selection.?.output(self, node, bytes);
    }
    pub fn allocationForNode(self: *const Pool, node: NodeId) ?buffers.AllocationId {
        const slot = self.plan.slotForNode(node) orelse return null;
        return if (self.plan.allocationForSlot(slot.id)) |allocation| allocation.id else null;
    }
    pub fn completeOwnedPayload(self: *const Pool) ?@import("owned_coverage.zig").Complete {
        const selected = if (self.selection) |*value| value else return null;
        const coverage = switch (self.payload_coverage) {
            .complete => |value| value,
            else => return null,
        };
        if (coverage.epoch != selected.epoch or !self.native_frame_dispatch) return null;
        return coverage;
    }

    pub fn preparedAlignment(self: *const Pool, node: NodeId) ?u32 {
        const selected = self.preparedKernelRoute(node) orelse return null;
        return selected.requirements.primary_alignment;
    }

    fn prepareHostIngress(self: *Pool) !void {
        const selected = if (self.selection) |*value| value else return;
        var plans = std.ArrayList(HostInputPlan).empty;
        errdefer plans.deinit(self.allocator);
        for (selected.inputs.entries) |input| {
            const host = switch (input.value) {
                .imported_host => |host| host,
                else => continue,
            };
            const slot = self.plan.slotForNode(input.node_id) orelse return error.InvalidHostIngressBinding;
            if (!host.owned_ingress) continue;
            if (!host_ingress.rawDType(host.logical.dtype) or !host.logical.eq(slot.desc.shape)) return error.PreparedHostIngressBindingChanged;
            var backend_index: ?usize = null;
            var requirements: ?host_ingress.Requirements = null;
            for (self.backends, 0..) |backend, index| {
                if (backend.kind() != host.target) continue;
                const functions = backend.vtable.kernels orelse continue;
                if (!functions.accepts(backend.vtable) or functions.createHostInput == null or functions.hostInputRequirements == null) continue;
                requirements = try functions.hostInputRequirements.?(backend.ptr, host.logical);
                if (requirements != null) {
                    backend_index = index;
                    break;
                }
            }
            const required = requirements orelse return error.PreparedHostIngressBindingChanged;
            const index = backend_index.?;
            const count = host.logical.numElements() orelse return error.InvalidHostIngressBinding;
            if (count <= 0 or required.bytes != try std.math.mul(usize, @intCast(count), host.logical.dtype.byteSize()) or required.alignment == 0 or !std.math.isPowerOfTwo(required.alignment)) return error.InvalidHostIngressBinding;
            var allocation: ?buffers.AllocationId = null;
            if (input.node_id < self.replay_input_indices.len) {
                const i = self.replay_input_indices[input.node_id];
                if (i != ml.graph.null_node and self.replay_inputs[i].constant == null) {
                    if (self.replay_inputs[i].allocation) |candidate| if (self.plan.allocations[candidate].backend == host.target) {
                        allocation = candidate;
                    };
                }
            }
            if (allocation == null) allocation = try self.plan.addInputBinding(host.target, required.bytes);
            try plans.append(self.allocator, .{ .node = input.node_id, .shape = host.logical, .backend_index = index, .allocation = allocation.?, .requirements = required, .epoch = selected.epoch });
        }
        self.host_input_plans = try plans.toOwnedSlice(self.allocator);
        for (selected.routes, 0..) |maybe, node| if (maybe) |owned| if (owned.vtable.host_input_constraint) |query| {
            const kernel = self.kernels[node] orelse return error.InvalidHostIndexBounds;
            for (0..kernel.descriptor.input_count) |input_index| if (query(owned.context, input_index)) |bounds| {
                if (bounds.extent == 0) return error.InvalidHostIndexBounds;
                const index = self.hostInputIndex(kernel.inputs[input_index]) orelse return error.MissingPreparedHostIndices;
                const target = &self.host_input_plans[index];
                target.index_bounds = if (target.index_bounds) |prior| prior.intersect(bounds) else bounds;
            };
        };
    }

    /// Cold proof uses the existing complete payload witness before omitting
    /// any input-bank charge. No alias, regional, opaque, asynchronous or mutable
    /// numerical provider participates in this initially narrow borrow mode.
    fn selectNativeBorrowedHostInputs(self: *Pool) void {
        if (self.host_input_plans.len == 0 or self.regions.len != 0 or @import("owned_coverage.zig").assess(self) != .complete) return;
        const selected = if (self.selection) |*value| value else return;
        const reachable = self.coverage_reachable orelse return;
        for (self.plan.slots) |slot| {
            if (slot.node_id >= reachable.len or !reachable[slot.node_id]) continue;
            if (slot.kind == .view) return;
            if (slot.kind == .allocation) {
                const owned = self.preparedKernelRoute(slot.node_id) orelse return;
                if (owned.validation_scope != .prepared_program) return;
            }
        }
        for (self.host_input_plans) |input| {
            const fact = selected.inputs.find(input.node) orelse return;
            if (input.shape.dtype != .f32 or fact != .imported_host or !fact.imported_host.borrowable_host) return;
        }
        for (self.host_input_plans) |*input| input.borrowed = true;
    }

    pub fn hostIngressBackingKnown(self: *const Pool) bool {
        for (self.host_input_plans) |plan| if (plan.requirements.host_bytes != 0 and plan.requirements.host_backing_bytes == null) return false;
        return true;
    }

    fn hostInputIndex(self: *const Pool, node: NodeId) ?usize {
        for (self.host_input_plans, 0..) |plan, index| if (plan.node == node) return index;
        return null;
    }

    /// Validate the entire source set before admission/SDK access. All public
    /// inputs must qualify; callers retain the existing import path otherwise.
    pub fn validateBorrowedHostOptions(self: *const Pool, options: *const interpreter.ExecuteOptions) !void {
        var borrowed = false;
        for (self.host_input_plans) |input| borrowed = borrowed or input.borrowed;
        if (!borrowed) return;
        if (options.attention != null or options.embedding_ids != null or options.sdpa_mask != null or options.cross_attention_mask != null or options.donate != null or options.deferred_input != null) return error.BorrowedHostInputSideChannels;
    }

    pub fn supportsHostInputs(self: *const Pool, inputs: []const host_ingress.Input) bool {
        if (inputs.len == 0 or inputs.len != self.host_input_plans.len) return false;
        for (inputs, 0..) |input, i| {
            const index = self.hostInputIndex(input.node_id) orelse return false;
            const plan = self.host_input_plans[index];
            const backend = self.backends[plan.backend_index];
            const functions = backend.vtable.kernels orelse return false;
            if (!functions.accepts(backend.vtable) or functions.hostInputRequirements == null or functions.createHostInput == null) return false;
            if (plan.requirements.validation_scope == .invocation) {
                const current = (functions.hostInputRequirements.?(backend.ptr, plan.shape) catch return false) orelse return false;
                if (!current.eql(plan.requirements)) return false;
            }
            if (plan.borrowed and @intFromPtr(input.value.data.ptr) % @alignOf(f32) != 0) return false;
            if (!std.mem.eql(u8, @tagName(input.value.dtype), @tagName(plan.shape.dtype)) or input.value.shape.len != plan.shape.rank() or input.value.data.len != plan.requirements.bytes) return false;
            for (input.value.shape, plan.shape.dims[0..plan.shape.rank()]) |actual, expected| if (actual != expected) return false;
            for (inputs[0..i]) |previous| if (previous.node_id == input.node_id) return false;
        }
        return true;
    }

    fn recomputeHostWorkspace(self: *Pool) !void {
        std.debug.assert(self.owns_plan and self.frames.items.len == 0);
        var bytes = self.regionHostWorkspaceBytes();
        for (self.kernels, 0..) |maybe, node| if (maybe) |kernel| {
            if (self.regionForNode(@intCast(node)) == null) bytes = @max(bytes, kernel.host_workspace_bytes);
        };
        const rounded = (try std.math.add(usize, bytes, 3)) / 4 * 4;
        if (self.host_workspace_allocation != buffers.invalid_allocation) {
            self.plan.allocations[self.host_workspace_allocation].byte_size = rounded;
            if (rounded == 0) self.host_workspace_allocation = buffers.invalid_allocation;
        } else if (rounded != 0) self.host_workspace_allocation = try self.plan.addHostWorkspace(rounded);
    }

    pub fn validateHostInputs(self: *const Pool, inputs: []const host_ingress.Input) !void {
        if (!self.supportsHostInputs(inputs)) return error.InvalidHostIngressBinding;
        for (inputs) |input| {
            const index = self.hostInputIndex(input.node_id).?;
            if (self.host_input_plans[index].index_bounds) |bounds| try bounds.validate(input.value.dtype, input.value.data);
        }
    }

    fn clonePlan(allocator: std.mem.Allocator, source: buffers.BufferPlan) !buffers.BufferPlan {
        const slots = try allocator.dupe(buffers.LogicalSlot, source.slots);
        errdefer allocator.free(slots);
        const node_slots = try allocator.dupe(buffers.SlotId, source.node_slots);
        errdefer allocator.free(node_slots);
        const positions = try allocator.dupe(u32, source.node_positions);
        errdefer allocator.free(positions);
        const enabled = try allocator.dupe(bool, source.allocation_enabled);
        errdefer allocator.free(enabled);
        const allocations = try allocator.dupe(buffers.PhysicalAllocation, source.allocations);
        errdefer allocator.free(allocations);
        const transfers = try allocator.dupe(buffers.TransferEdge, source.transfers);
        errdefer allocator.free(transfers);
        const index = try source.transfer_index.clone(allocator);
        return .{ .execution_reachable = source.execution_reachable, .allocation_enabled = enabled, .allocator = allocator, .slots = slots, .node_slots = node_slots, .node_positions = positions, .allocations = allocations, .transfers = transfers, .transfer_index = index };
    }

    fn cloneVariant(self: *Pool) !*Pool {
        var plan = try clonePlan(self.allocator, self.plan);
        errdefer plan.deinit();
        const selected = try self.allocator.dupe(?KernelPlan, self.kernels);
        errdefer self.allocator.free(selected);
        const transfers = try self.allocator.dupe(?TransferPlan, self.transfers);
        errdefer self.allocator.free(transfers);
        const backends = try self.allocator.dupe(ops.ComputeBackend, self.backends);
        errdefer self.allocator.free(backends);
        const workspaces = try self.allocator.dupe(buffers.AllocationId, self.workspace_allocations);
        errdefer self.allocator.free(workspaces);
        const replay_inputs = try self.allocator.dupe(ReplayInput, self.replay_inputs);
        errdefer self.allocator.free(replay_inputs);
        const replay_indices = try self.allocator.dupe(u32, self.replay_input_indices);
        errdefer self.allocator.free(replay_indices);
        const replay_outputs = try self.allocator.dupe(ReplayOutput, self.replay_outputs);
        errdefer self.allocator.free(replay_outputs);
        const metadata_aliases = try self.allocator.dupe(@import("metadata_alias.zig").Plan, self.metadata_aliases);
        errdefer self.allocator.free(metadata_aliases);
        const reshapes = try self.allocator.dupe(NodeId, self.reshape_sources);
        errdefer self.allocator.free(reshapes);
        const ledger = try accounting.Ledger.create(self.allocator);
        errdefer ledger.release();
        const child = try self.allocator.create(Pool);
        child.* = .{ .metadata_aliases = metadata_aliases, .replay_config = self.replay_config, .coverage_graph = self.coverage_graph, .coverage_reachable = self.coverage_reachable, .native_frame_dispatch = self.native_frame_dispatch, .coverage_backend = self.coverage_backend, .reshape_sources = reshapes, .allocator = self.allocator, .accounting = ledger, .plan = plan, .owns_plan = true, .kernels = selected, .transfers = transfers, .backends = backends, .workspace_allocations = workspaces, .host_workspace_allocation = self.host_workspace_allocation, .max_frames = self.max_frames, .replay_inputs = replay_inputs, .replay_input_indices = replay_indices, .replay_outputs = replay_outputs, .replay_kernel_count = self.replay_kernel_count, .replay_backend = self.replay_backend, .replay_decision = self.replay_decision, .host_alignment = self.host_alignment, .root_pool = self };
        if (self.literal_store) |store| {
            store.retain();
            child.literal_store = store;
        }
        if (self.immutable_bindings) |immutable| child.immutable_bindings = immutable.retain();
        return child;
    }

    fn prepareVariant(root: *Pool, inputs: []const route.InputBinding, policy: @import("region_route.zig").SelectionPolicy, profile: @import("region_route.zig").ExecutionProfile) !?*prepared_routes.PreparedRoutes {
        const child = try root.cloneVariant();
        errdefer child.deinit();
        child.region_policy = policy;
        child.region_execution_profile = profile;
        try @import("reachable_projection.zig").deactivate(child);
        child.selection = try RouteSelection.create(child, inputs);
        if (policy == .select_regions) try child.prepareRegions();
        try child.applyRouteRequirements();
        try child.recomputeHostWorkspace();
        try child.prepareHostIngress();
        child.selectNativeBorrowedHostInputs();
        if (child.replay_backend == null) if (child.replay_config) |config| {
            var qualified = false;
            for (child.host_input_plans) |input| qualified = qualified or input.index_bounds != null;
            if (qualified) {
                // Root templates are immutable. This private variant owns its
                // copied plan and publishes fresh templates only on success.
                if (child.replay_inputs.len != 0 or child.replay_outputs.len != 0 or child.replay_input_indices.len != 0) return error.InvalidReplayTemplate;
                try child.prepareReplay(config.graph, &child.plan, config.bindings, config.costs);
            }
        };
        try @import("reachable_projection.zig").refine(child);
        child.payload_coverage = @import("owned_coverage.zig").assess(child);
        const immutable = child.immutable_bindings orelse return error.MissingImmutableKernelBindings;
        // Epoch is identity, not a hash of equivalent metadata. Every cache hit
        // still calls full backend matches and validates its concrete policy.
        return try prepared_routes.PreparedRoutes.create(root.allocator, child, immutable, child.selection.?.epoch);
    }

    fn currentRegionPolicy(self: *const Pool, backend_index: usize) !u64 {
        const backend = self.backends[backend_index];
        const functions = backend.vtable.kernels orelse return error.PreparedRegionPolicyChanged;
        if (!functions.accepts(backend.vtable)) return error.PreparedRegionPolicyChanged;
        const query = functions.regionPolicyKey orelse return error.PreparedRegionPolicyChanged;
        const value = (try query(backend.ptr)) orelse return error.PreparedRegionPolicyChanged;
        if (value == 0) return error.PreparedRegionPolicyChanged;
        return value;
    }

    /// Cold authority for shared arena recomputation. Ordinary KernelPlans alone
    /// cannot erase preparation/dispatch scratch reserved by frozen regions.
    pub fn regionHostWorkspaceBytes(self: *const Pool) usize {
        var bytes: usize = 0;
        for (self.regions) |installed| bytes = @max(bytes, installed.host_workspace_bytes);
        return bytes;
    }

    fn deinitRegions(self: *Pool) void {
        for (self.regions) |installed| self.allocator.free(installed.outputs);
        self.allocator.free(self.regions);
        self.allocator.free(self.region_by_node);
        self.allocator.free(self.region_runtime_last_use);
        if (self.region_owner) |*owner| owner.deinit();
    }

    pub fn regionForNode(self: *const Pool, node: NodeId) ?*const @import("region_binding.zig").Installed {
        if (node >= self.region_by_node.len) return null;
        const index = self.region_by_node[node];
        if (index == std.math.maxInt(u32)) return null;
        return &self.regions[index];
    }
    pub fn regionAtEntry(self: *const Pool, node: NodeId) ?*const @import("region_binding.zig").Installed {
        const installed = self.regionForNode(node) orelse return null;
        return if (installed.owner.entry_node == node) installed else null;
    }

    pub fn regionExecutionFacts(self: *const Pool, owner: *const @import("region_route.zig").Owned, result: []route.Binding) !void {
        const selected = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        var installed = false;
        for (self.regions) |entry| if (entry.owner == owner) {
            installed = true;
            break;
        };
        if (!installed) return error.PreparedRegionOwnerChanged;
        try self.regionFacts(owner, selected.inputs.entries, result);
    }

    fn regionFacts(self: *const Pool, owner: *const @import("region_route.zig").Owned, runtime: []const route.InputBinding, result: []route.Binding) !void {
        if (owner.input_nodes.len != result.len) return error.PreparedRegionBindingChanged;
        const selected = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        for (owner.input_nodes, result) |node, *fact| fact.* = selected.binding(self, node, runtime);
    }
    fn matchesRegions(self: *const Pool, runtime: []const route.InputBinding) bool {
        if (self.region_owner) |*owner| if (!owner.matchesReadiness()) return false;
        const limits = @import("region_binding.zig");
        for (self.regions) |installed| {
            const owner = installed.owner;
            if (owner.input_nodes.len > limits.max_inputs or installed.outputs.len > limits.max_outputs) return false;
            var input_facts: [limits.max_inputs]route.Binding = undefined;
            var output_slots: [limits.max_outputs]route.OutputSlot = undefined;
            self.regionFacts(owner, runtime, input_facts[0..owner.input_nodes.len]) catch return false;
            for (installed.outputs, output_slots[0..installed.outputs.len]) |output, *slot| slot.* = output.slot;
            const backend = self.backends[installed.backend_index];
            if (!owner.matches(.{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .policy_key = self.currentRegionPolicy(installed.backend_index) catch return false, .inputs = input_facts[0..owner.input_nodes.len], .outputs = output_slots[0..installed.outputs.len] })) return false;
        }
        return true;
    }
    fn validateRegions(self: *const Pool, runtime: []const route.InputBinding) !void {
        if (!self.matchesRegions(runtime)) return error.PreparedRegionBindingChanged;
        if (self.region_owner) |*owner| try owner.validateReadiness();
        const limits = @import("region_binding.zig");
        for (self.regions) |installed| {
            var input_facts: [limits.max_inputs]route.Binding = undefined;
            var output_slots: [limits.max_outputs]route.OutputSlot = undefined;
            try self.regionFacts(installed.owner, runtime, input_facts[0..installed.owner.input_nodes.len]);
            for (installed.outputs, output_slots[0..installed.outputs.len]) |output, *slot| slot.* = output.slot;
            const backend = self.backends[installed.backend_index];
            try installed.owner.validate(.{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .policy_key = try self.currentRegionPolicy(installed.backend_index), .inputs = input_facts[0..installed.owner.input_nodes.len], .outputs = output_slots[0..installed.outputs.len] });
        }
    }

    fn rebindRegionKernelSlots(self: *Pool) !void {
        var auxiliaries = std.AutoHashMap(u64, buffers.AllocationId).init(self.allocator);
        defer auxiliaries.deinit();
        for (self.kernels, 0..) |*maybe, node| {
            const kernel = if (maybe.*) |*value| value else continue;
            kernel.output_allocation = (self.plan.slotForNode(@intCast(node)) orelse return error.InvalidPhysicalAllocation).allocation;
            for (&kernel.auxiliaries) |*maybe_auxiliary| {
                const auxiliary = if (maybe_auxiliary.*) |*value| value else continue;
                const count = auxiliary.requirement.shape.numElements() orelse return error.InvalidAuxiliaryRequirements;
                if (count <= 0) return error.InvalidAuxiliaryRequirements;
                const bytes = try std.math.mul(usize, @intCast(count), auxiliary.requirement.shape.dtype.byteSize());
                const key = @as(u64, kernel.output_allocation) << 32 | @backingInt(auxiliary.requirement.role);
                const entry = try auxiliaries.getOrPut(key);
                if (!entry.found_existing) entry.value_ptr.* = try self.plan.addAuxiliaryPlane(kernel.output_allocation, bytes);
                auxiliary.allocation = entry.value_ptr.*;
                try self.growAllocation(auxiliary.allocation, bytes);
            }
        }
        for (self.replay_outputs) |*output| output.allocation = (self.plan.slotForNode(output.node) orelse return error.InvalidPhysicalAllocation).allocation;
    }

    fn prepareRegions(self: *Pool) !void {
        if (self.region_execution_profile == .unknown) return;
        const region_binding = @import("region_binding.zig");
        const preparation = @import("region_preparation.zig");
        const graph = self.coverage_graph orelse return;
        const reachable = self.coverage_reachable orelse return;
        var enabled = false;
        for (self.backends) |backend| if (backend.vtable.kernels) |functions| {
            enabled = enabled or (functions.accepts(backend.vtable) and functions.prepareRegionRoutes != null);
        };
        if (!enabled) return;
        const selected = if (self.selection) |*value| value else return;
        const facts = try self.allocator.alloc(route.Binding, graph.nodeCount());
        defer self.allocator.free(facts);
        for (facts, 0..) |*fact, node| fact.* = selected.binding(self, @intCast(node), selected.inputs.entries);
        var owned = (try preparation.select(.{ .allocator = self.allocator, .graph = graph, .reachable = reachable, .plan = &self.plan, .backends = self.backends, .node_facts = facts, .execution_profile = self.region_execution_profile })) orelse return;
        errdefer owned.deinit();
        if (owned.owners.len == 0) {
            // Keep cold readiness watches so legacy slot warming forces a new
            // selection. This installs no numerical region or coverage proof.
            self.region_owner = owned;
            return;
        }
        for (owned.owners) |owner| {
            if (owner.input_nodes.len > region_binding.max_inputs or owner.requirements.outputs.len > region_binding.max_outputs) return error.UnsupportedPreparedRegionBoundary;
        }
        var lifetimes = try @import("region_lifetimes.zig").create(self.allocator, graph, reachable, &self.plan, owned.owners);
        defer lifetimes.deinit();
        try @import("region_recolor.zig").applyWithLastUse(&self.plan, lifetimes.first_use, lifetimes.input_last_use);
        try self.rebindRegionKernelSlots();
        // Per-node owners capture allocation IDs. Rebuild them against the
        // final coloring; input representations/profile policies remain exact.
        try selected.refresh(self);
        const installed = try self.allocator.alloc(region_binding.Installed, owned.owners.len);
        errdefer self.allocator.free(installed);
        var initialized: usize = 0;
        errdefer for (installed[0..initialized]) |value| self.allocator.free(value.outputs);
        const region_by_node = try self.allocator.alloc(u32, graph.nodeCount());
        errdefer self.allocator.free(region_by_node);
        @memset(region_by_node, std.math.maxInt(u32));
        for (owned.owners, owned.backend_indices, 0..) |owner, backend_index, index| {
            const outputs = try self.allocator.alloc(region_binding.Installed.Output, owner.requirements.outputs.len);
            errdefer self.allocator.free(outputs);
            for (owner.requirements.outputs, outputs, 0..) |output, *target, output_index| {
                const slot = self.plan.slotForNode(output.node) orelse return error.PreparedRegionOutputMismatch;
                if (!slot.desc.shape.eq(output.logical) or slot.allocation == buffers.invalid_allocation) return error.PreparedRegionOutputMismatch;
                if (output.bytes == 0 or output.logical.dtype != .f32 or output.alignment == 0 or !std.math.isPowerOfTwo(output.alignment)) return error.PreparedRegionOutputMismatch;
                var alias_index: ?usize = null;
                if (output.alias_of) |parent| {
                    for (owner.requirements.outputs[0..output_index], 0..) |prior, prior_index| if (prior.node == parent and prior.alias_of == null) {
                        alias_index = prior_index;
                        break;
                    };
                    const parent_index = alias_index orelse return error.PreparedRegionOutputMismatch;
                    const prior = outputs[parent_index];
                    const dense = @import("../graph/backend_contracts.zig").TensorStrides.dense(output.logical);
                    if (slot.kind != .view or graph.node(output.node).op != .reshape or !std.meta.eql(slot.desc.strides, dense) or slot.allocation != prior.slot.allocation or output.logical.numElements() != prior.slot.logical.numElements()) return error.UnsupportedPreparedRegionAlias;
                } else {
                    if (slot.kind != .allocation) return error.PreparedRegionOutputMismatch;
                    for (outputs[0..output_index]) |prior| if (prior.alias_index == null and prior.slot.allocation == slot.allocation) return error.OverlappingRegionOutputs;
                    try self.growAllocation(slot.allocation, output.bytes);
                }
                target.* = .{ .slot = .{ .allocation = slot.allocation, .node = output.node, .epoch = selected.epoch, .logical = output.logical, .writable_bytes = if (alias_index == null) output.bytes else 0 }, .alias_index = alias_index };
            }
            const workspace = @max(owner.requirements.dispatch_workspace.byte_size, owner.requirements.preparation_workspace.byte_size);
            const host = @max(owner.requirements.dispatch_workspace.host_byte_size, owner.requirements.preparation_workspace.host_byte_size);
            const alignment = @max(owner.requirements.dispatch_workspace.alignment, owner.requirements.preparation_workspace.alignment);
            try validateAlignment(alignment);
            if (workspace % 4 != 0) return error.InvalidWorkspaceSize;
            if (workspace != 0) {
                const allocation = &self.workspace_allocations[backend_index];
                if (allocation.* == buffers.invalid_allocation) allocation.* = try self.plan.addWorkspace(self.backends[backend_index].kind(), workspace, @min(alignment, @alignOf(f32))) else try self.growAllocation(allocation.*, workspace);
            }
            const host_alignment = @max(owner.requirements.host_alignment, owner.requirements.preparation_host_alignment);
            try validateAlignment(host_alignment);
            if (host != 0) {
                const needed = std.mem.Alignment.fromByteUnits(host_alignment);
                if (@backingInt(needed) > @backingInt(self.host_alignment)) self.host_alignment = needed;
                if (self.host_workspace_allocation == buffers.invalid_allocation) {
                    const rounded = try std.math.add(usize, host, 3);
                    self.host_workspace_allocation = try self.plan.addHostWorkspace(rounded / 4 * 4);
                } else try self.growAllocation(self.host_workspace_allocation, host);
            }
            installed[index] = .{ .owner = owner, .key = owner.key, .backend_index = backend_index, .epoch = selected.epoch, .outputs = outputs, .workspace_allocation = self.workspace_allocations[backend_index], .workspace_bytes = workspace, .host_workspace_bytes = host };
            for (owner.covered_nodes) |node| region_by_node[node] = @intCast(index);
            for (owner.requirements.literals) |node| {
                const store = self.literal_store orelse return error.PreparedRegionLiteralUnavailable;
                if (node >= store.node_indices.len) return error.PreparedRegionLiteralUnavailable;
                const literal = store.node_indices[node];
                if (literal == ml.graph.null_node or !self.literalEnabled(literal)) return error.PreparedRegionLiteralUnavailable;
            }
            var input_facts: [region_binding.max_inputs]route.Binding = undefined;
            var output_slots: [region_binding.max_outputs]route.OutputSlot = undefined;
            try self.regionFacts(owner, selected.inputs.entries, input_facts[0..owner.input_nodes.len]);
            for (outputs, output_slots[0..outputs.len]) |output, *slot| slot.* = output.slot;
            const backend = self.backends[backend_index];
            try owner.validate(.{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .policy_key = try self.currentRegionPolicy(backend_index), .inputs = input_facts[0..owner.input_nodes.len], .outputs = output_slots[0..outputs.len] });
            initialized += 1;
        }
        self.regions = installed;
        self.region_by_node = region_by_node;
        self.region_owner = owned;
        self.region_runtime_last_use = lifetimes.runtime_last_use;
        lifetimes.runtime_last_use = &.{};
        self.native_frame_dispatch = false;
        // Existing per-node captures do not encode a region's tuned dispatch.
        // Region capture needs its own qualified assigned-resource owner.
        self.replay_backend = null;
        self.replay_kernel_count = 0;
        self.replay_decision = .unsupported_graph;
    }

    /// Called BEFORE fitting/admission. The warm FrameClaim must remain held until
    /// the returned Set has moved into that Frame or has released its pins. This
    /// call acquires no Pool lock while consulting provider readiness/slot owners.
    pub fn claimRegions(self: *Pool, allocator: std.mem.Allocator, warm: ?*const FrameClaim, runtime: []const route.InputBinding) !@import("region_claims.zig").Acquisition {
        const claims = @import("region_claims.zig");
        const limits = @import("region_binding.zig");
        if (self.regions.len == 0) return .no_regions;
        const selection = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        const identity = @intFromPtr(self);
        var pending: claims.Set = undefined;
        if (warm) |token| {
            const frame = token.frame orelse return error.PhysicalFrameClaimConsumed;
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (frame.pool != self or !frame.claimed or !frame.in_use or frame.claim_nonce != token.nonce) return error.InvalidPhysicalFrameClaim;
            pending = try claims.Set.borrowed(identity, selection.epoch, frame.region_claims);
        } else pending = try claims.Set.create(allocator, identity, selection.epoch, self.regions.len);
        errdefer pending.releaseUnsubmitted();
        // Cache dependencies decide whether to select again; after successful pin
        // acquisition, a subsequent eviction request does not revoke these pins.
        if (self.region_owner) |*owner| if (!owner.matchesReadiness()) {
            pending.releaseUnsubmitted();
            return .reprepare;
        };
        for (self.regions, 0..) |installed, index| {
            var facts: [limits.max_inputs]route.Binding = undefined;
            var slots: [limits.max_outputs]route.OutputSlot = undefined;
            try self.regionFacts(installed.owner, runtime, facts[0..installed.owner.input_nodes.len]);
            for (installed.outputs, slots[0..installed.outputs.len]) |output, *slot| slot.* = output.slot;
            const backend = self.backends[installed.backend_index];
            const validation: @import("region_route.zig").Validate = .{
                .implementation = .{ .context = backend.ptr, .table = backend.vtable },
                .policy_key = try self.currentRegionPolicy(installed.backend_index),
                .inputs = facts[0..installed.owner.input_nodes.len],
                .outputs = slots[0..installed.outputs.len],
            };
            pending.claims[index] = try installed.owner.claimBeforeAdmission(.{ .allocator = allocator, .validation = validation });
            if (installed.owner.requirements.payload_claim_required and pending.claims[index] == null) {
                pending.releaseUnsubmitted();
                return .reprepare;
            }
        }
        if (warm) |token| {
            const frame = token.frame orelse return error.PhysicalFrameClaimConsumed;
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (frame.pool != self or !frame.claimed or !frame.in_use or frame.claim_nonce != token.nonce) return error.InvalidPhysicalFrameClaim;
        }
        return .{ .ready = pending };
    }

    /// Executing an admitted invocation uses its already-pinned payload authority,
    /// never a mutable cache-readiness epoch. Cache lookups still check readiness.
    fn validateClaimedRegions(self: *const Pool, runtime: []const route.InputBinding, pending: *const @import("region_claims.zig").Set) !void {
        const limits = @import("region_binding.zig");
        const selection = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        if (pending.closed or pending.owner_identity != @intFromPtr(self) or pending.epoch != selection.epoch or pending.claims.len != self.regions.len) return error.InvalidPreparedRegionClaim;
        for (self.regions, pending.claims) |installed, *maybe| {
            var facts: [limits.max_inputs]route.Binding = undefined;
            var slots: [limits.max_outputs]route.OutputSlot = undefined;
            try self.regionFacts(installed.owner, runtime, facts[0..installed.owner.input_nodes.len]);
            for (installed.outputs, slots[0..installed.outputs.len]) |output, *slot| slot.* = output.slot;
            const backend = self.backends[installed.backend_index];
            if (installed.owner.requirements.payload_claim_required and maybe.* == null) return error.PreparedRegionClaimRequired;
            try installed.owner.validate(.{
                .implementation = .{ .context = backend.ptr, .table = backend.vtable },
                .policy_key = try self.currentRegionPolicy(installed.backend_index),
                .inputs = facts[0..installed.owner.input_nodes.len],
                .outputs = slots[0..installed.outputs.len],
                .payload_claim = if (maybe.*) |*claim| claim else null,
            });
        }
    }

    pub fn validateRoutesWithClaims(self: *const Pool, inputs: []const route.InputBinding, pending: ?*const @import("region_claims.zig").Set) !void {
        const selected = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        try selected.validate(self, inputs);
        if (pending) |claims| return self.validateClaimedRegions(inputs, claims);
        if (self.regions.len != 0) return error.PreparedRegionClaimRequired;
        // No selected numerical owner exists. Readiness changes after quota cannot
        // change this invocation into a newly selected region.
    }

    /// Exact admitted handoff. Legacy acquisition is forbidden for variants with
    /// selected regions: it cannot silently omit the pre-admission payload pins.
    pub fn acquirePreparedWithRegions(self: *Pool, capability: ?admission.Capability, warm: ?*FrameClaim, pending: ?*@import("region_claims.zig").Set) !?*Frame {
        if (self.regions.len == 0) {
            if (pending != null) return error.InvalidPreparedRegionClaim;
            return self.acquirePrepared(capability, warm);
        }
        // consume() may drop the warm token's last route-owner reference on an
        // error. Keep this Pool and Frame alive through wrapper cleanup afterward.
        const route_lifetime = if (self.route_owner) |owner| owner.retain() else null;
        defer if (route_lifetime) |owner| owner.release();
        const claims = pending orelse return error.PreparedRegionClaimRequired;
        const selection = if (self.selection) |*value| value else return error.MissingPreparedKernelRoute;
        if (claims.closed or claims.owner_identity != @intFromPtr(self) or claims.epoch != selection.epoch or claims.claims.len != self.regions.len) return error.InvalidPreparedRegionClaim;
        if (warm) |token| {
            const frame = token.frame orelse return error.PhysicalFrameClaimConsumed;
            const cap = capability orelse return error.InvalidPhysicalFrameClaim;
            if (!token.identity.eql(cap.identity)) return error.PhysicalStorageAdmissionMismatch;
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            if (frame.pool != self or !frame.claimed or !frame.in_use or frame.claim_nonce != token.nonce) {
                self.mutex.unlock();
                return error.InvalidPhysicalFrameClaim;
            }
            const metadata = claims.moveToFrame(@intFromPtr(self), selection.epoch, frame.region_claims) catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.mutex.unlock();
            if (metadata) |retired| retired.deinit();
            // Source storage cannot be exposed for reuse if prepareAcquired fails:
            // reusableCredit rejects still-held claims even before this catch.
            return token.consume(self, capability) catch |err| {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                frame.retired = true;
                self.mutex.unlock();
                try frame.releaseRegionClaimsChecked();
                return err;
            };
        }
        const frame = (try self.acquireWithAdmission(capability)) orelse return error.PreparedRegionStorageUnavailable;
        errdefer {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            frame.retired = true;
            self.mutex.unlock();
            _ = frame.finish() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        const metadata = claims.moveToFrame(@intFromPtr(self), selection.epoch, frame.region_claims) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.mutex.unlock();
        if (metadata) |retired| retired.deinit();
        return frame;
    }

    fn growAllocation(self: *Pool, id: buffers.AllocationId, bytes: usize) !void {
        if (id == buffers.invalid_allocation or id >= self.plan.allocations.len) return error.InvalidPhysicalAllocation;
        const rounded = try std.math.add(usize, bytes, 3);
        self.plan.allocations[id].byte_size = @max(self.plan.allocations[id].byte_size, rounded / 4 * 4);
    }

    fn validateAlignment(value: u32) !void {
        if (value == 0 or !std.math.isPowerOfTwo(value)) return error.InvalidPreparedKernelAlignment;
    }

    fn applyRouteRequirements(self: *Pool) !void {
        const selected = if (self.selection) |*value| value else return;
        // Existing independently qualified compatibility routes retain their
        // original storage. Selected exact routes replace their own scratch,
        // never silently selecting a new profile after admission.
        for (selected.routes, 0..) |maybe, node| {
            const owned = maybe orelse continue;
            const required = owned.requirements;
            const kernel = &self.kernels[node].?;
            const kind = self.backends[kernel.backend_index].kind();
            try validateAlignment(required.primary_alignment);
            try validateAlignment(required.host_alignment);
            try validateAlignment(required.probe_host_alignment);
            try validateAlignment(required.workspace.alignment);
            try validateAlignment(required.probe_workspace.alignment);
            if (kind == .native and (required.primary_alignment > @alignOf(u64) or required.workspace.alignment > @alignOf(f32) or required.probe_workspace.alignment > @alignOf(f32))) return error.UnsupportedPhysicalWorkspaceAlignment;
            try self.growAllocation(kernel.output_allocation, required.primary_bytes);
            const workspace_bytes = @max(required.workspace.byte_size, required.probe_workspace.byte_size);
            const workspace_alignment = @max(required.workspace.alignment, required.probe_workspace.alignment);
            if (workspace_bytes % 4 != 0) return error.InvalidWorkspaceSize;
            kernel.workspace_bytes = workspace_bytes;
            if (workspace_bytes != 0) {
                const id = &self.workspace_allocations[kernel.backend_index];
                if (id.* == buffers.invalid_allocation) {
                    // Provider-owned device banks establish their actual
                    // alignment; Assigned.validate checks exact addresses.
                    id.* = try self.plan.addWorkspace(kind, workspace_bytes, @min(workspace_alignment, @alignOf(f32)));
                } else try self.growAllocation(id.*, workspace_bytes);
            }
            kernel.host_workspace_bytes = @max(required.workspace.host_byte_size, required.probe_workspace.host_byte_size);
            if (kernel.host_workspace_bytes != 0) {
                const alignment = std.mem.Alignment.fromByteUnits(@max(required.host_alignment, required.probe_host_alignment));
                if (@backingInt(alignment) > @backingInt(self.host_alignment)) self.host_alignment = alignment;
                if (self.host_workspace_allocation == buffers.invalid_allocation) {
                    const rounded = try std.math.add(usize, kernel.host_workspace_bytes, 3);
                    self.host_workspace_allocation = try self.plan.addHostWorkspace(rounded / 4 * 4);
                } else try self.growAllocation(self.host_workspace_allocation, kernel.host_workspace_bytes);
            }
            // Reuse existing role slots only for the same primary allocation.
            // The shared primary lifetime already governs alias protection.
            var auxiliaries: [kernels.max_auxiliaries]?AuxiliaryPlan = @splat(null);
            var roles: u32 = 0;
            if (required.auxiliaries.count > kernels.max_auxiliaries) return error.InvalidAuxiliaryRequirements;
            for (required.auxiliaries.items(), 0..) |auxiliary, index| {
                const bit: u32 = @as(u32, 1) << @intCast(@backingInt(auxiliary.role));
                if (roles & bit != 0) return error.InvalidAuxiliaryRequirements;
                roles |= bit;
                const elements = auxiliary.shape.numElements() orelse return error.InvalidAuxiliaryRequirements;
                if (elements <= 0) return error.InvalidAuxiliaryRequirements;
                const bytes = try std.math.mul(usize, @intCast(elements), auxiliary.shape.dtype.byteSize());
                var allocation: ?buffers.AllocationId = null;
                for (self.kernels) |peer| if (peer) |other| {
                    if (other.output_allocation != kernel.output_allocation) continue;
                    for (other.auxiliaries) |plane| if (plane) |value| {
                        if (value.requirement.role == auxiliary.role) allocation = value.allocation;
                    };
                };
                const id = allocation orelse try self.plan.addAuxiliaryPlane(kernel.output_allocation, bytes);
                try self.growAllocation(id, bytes);
                auxiliaries[index] = .{ .requirement = auxiliary, .allocation = id };
            }
            kernel.auxiliaries = auxiliaries;
            if (required.capture != .supported) {
                self.replay_backend = null;
                self.replay_decision = .unsupported_representation;
            }
        }
    }

    pub fn literalEnabled(self: *const Pool, index: usize) bool {
        const store = self.literal_store orelse return false;
        if (index >= store.literals.len) return false;
        if (self.selection) |*selected| return selected.literal_enabled[index];
        return store.backends[store.literals[index].backend_index].kind() == .native;
    }

    pub fn claimWarm(self: *Pool, identity: admission.Identity) !?FrameClaim {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.frames.items) |frame| {
            var requirement = self.storage_requirement;
            const credit = (try frame.reusableCredit(identity, if (requirement) |*value| value else null)) orelse continue;
            frame.claim_nonce = std.math.add(u64, frame.claim_nonce, 1) catch {
                frame.retired = true;
                continue;
            };
            frame.in_use = true;
            frame.claimed = true;
            return .{ .frame = frame, .identity = identity, .credit = credit, .requirement = requirement, .nonce = frame.claim_nonce, .route_owner = if (self.route_owner) |owner| owner.retain() else null };
        }
        return null;
    }

    /// Cold pressure cleanup. Keep a listed owner marked busy while checking
    /// completion outside the mutex; on failure its storage and quota remain.
    pub fn reclaimRetiredFrames(self: *Pool) !bool {
        return self.reclaimRetiredFramesWithCleanup(.{});
    }
    pub fn reclaimRetiredFramesWithCleanup(self: *Pool, cleanup: @import("cleanup_context.zig").Context) !bool {
        var reclaimed = false;
        while (true) {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            var candidate: ?*Frame = null;
            for (self.frames.items) |frame| if (!frame.in_use and frame.retired and !frame.claimed and frame.owner_pin == null) {
                frame.in_use = true;
                candidate = frame;
                break;
            };
            self.mutex.unlock();
            const frame = candidate orelse return reclaimed;
            frame.waitForRetirementChecked() catch |err| {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                frame.in_use = false;
                self.mutex.unlock();
                return err;
            };
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            var found = false;
            for (self.frames.items, 0..) |entry, index| if (entry == frame) {
                _ = self.frames.orderedRemove(index);
                found = true;
                break;
            };
            self.mutex.unlock();
            std.debug.assert(found);
            // All queue uses are complete. Captures release only their own
            // pins; externally published output Leases remain independent.
            frame.deinitAfterWaitWithCleanup(cleanup);
            reclaimed = true;
        }
    }

    pub fn acquirePrepared(self: *Pool, capability: ?admission.Capability, claim: ?*FrameClaim) !?*Frame {
        if (self.regions.len != 0) return error.PreparedRegionClaimRequired;
        if (claim) |prepared| return try prepared.consume(self, capability);
        return self.acquireWithAdmission(capability);
    }

    /// Caller reserved the frame and its route pin under mutex. Completion,
    /// admission and driver callbacks run only after that mutex is released.
    fn prepareAcquired(self: *Pool, frame: *Frame, capability: ?admission.Capability) !*Frame {
        if (capability) |cap| if (cap.services.takeBacking != null) self.prepared_backings.store(true, .release);
        std.debug.assert(frame.in_use and !frame.claimed);
        errdefer {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            frame.storage_admission = null;
            const owner = frame.owner_pin;
            frame.owner_pin = null;
            frame.in_use = false;
            self.mutex.unlock();
            if (owner) |value| value.release();
        }
        frame.storage_admission = capability;
        frame.storage_admission_failed = false;
        for (frame.completions) |completion| if (completion) |value| value.beforeReuse() catch |err| {
            frame.retired = true;
            return err;
        };
        if (capability) |cap| try frame.adoptAdmission(cap);
        frame.stats = .{ .replay_decision = self.replay_decision };
        return frame;
    }

    pub fn acquire(self: *Pool) !?*Frame {
        return self.acquireWithAdmission(null);
    }
    pub fn acquireWithAdmission(self: *Pool, capability: ?admission.Capability) !?*Frame {
        if (capability) |cap| if (cap.services.takeBacking != null) self.prepared_backings.store(true, .release);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        for (self.frames.items) |frame| {
            const credit = frame.reusableCredit(null, null) catch |err| {
                self.mutex.unlock();
                return err;
            };
            if (credit == null) continue;
            frame.in_use = true;
            std.debug.assert(frame.owner_pin == null);
            frame.owner_pin = if (self.route_owner) |owner| owner.retain() else null;
            self.mutex.unlock();
            return try self.prepareAcquired(frame, capability);
        }
        const full = self.frames.items.len >= self.max_frames or self.pending_frames >= self.max_frames - self.frames.items.len;
        if (full and self.route_owner == null) {
            self.mutex.unlock();
            return null;
        }
        var owner = if (self.route_owner) |value| value.retain() else null;
        if (!full) self.pending_frames += 1;
        self.mutex.unlock();
        defer if (owner) |value| value.release();
        const frame = Frame.createWithAdmission(self, capability) catch |err| {
            if (!full) {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                self.pending_frames -= 1;
                self.mutex.unlock();
            }
            return err;
        };
        if (full) {
            // Cache size is not a concurrency/admission limit. The transient
            // owns its full cold admission and leaves cached slots unchanged.
            frame.transient = true;
            frame.replay_disabled = .busy_frame;
            frame.stats.replay_decision = .busy_frame;
            frame.owner_pin = owner;
            owner = null;
            return frame;
        }
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.pending_frames -= 1;
        self.frames.append(self.allocator, frame) catch |err| {
            self.mutex.unlock();
            frame.deinitWithCleanup(.{ .boundary = if (capability) |cap| cap.constructionEpoch() else null });
            return err;
        };
        frame.owner_pin = owner;
        owner = null;
        self.mutex.unlock();
        return frame;
    }
};

pub const Frame = struct {
    pool: *Pool,
    owner_pin: ?*prepared_routes.PreparedRoutes = null,
    transient: bool = false,
    assigned: []?route.Assigned = &.{},
    region_claims: []?@import("region_route.zig").PayloadClaim = &.{},
    region_started: []bool = &.{},
    region_assigned: []?@import("region_route.zig").AssignedResources = &.{},
    storage_admission: ?admission.Capability = null,
    storage_admission_failed: bool = false,
    host_workspace_admission: ?*admission.Reservation = null,
    host_workspace_retirement_owner: ?*@import("backing_retirement.zig").BackingOwner = null,
    storages: []?*storage.Lease,
    completions: []?storage.Completion,
    host_workspace: []u8 = &.{},
    host_workspace_fresh: bool = true,
    collect_storage_stats: bool = false,
    in_use: bool = true,
    claimed: bool = false,
    claim_nonce: u64 = 0,
    retired: bool = false,
    stats: Stats = .{},
    scope_backends: usize = 0,
    replay: ?storage.Replay = null,
    replay_ready: bool = false,
    capture_pins: []bool = &.{},
    replay_disabled: ?ReplayDecision = null,
    replay_failure: ?[]const u8 = null,
    warmed: bool = false,
    bound_inputs: []interpreter.RuntimeInput = &.{},
    binding_states: []ReplayBindingState = &.{},
    bound_input_count: usize = 0,
    host_input_resources: []?HostInputStorage = &.{},
    host_bound_inputs: []interpreter.RuntimeInput = &.{},
    bound_host_input_count: usize = 0,
    host_input_generation: u64 = 0,

    fn create(pool: *Pool) !*Frame {
        return createWithAdmission(pool, null);
    }
    fn createWithAdmission(pool: *Pool, capability: ?admission.Capability) !*Frame {
        const leases = try pool.allocator.alloc(?*storage.Lease, pool.plan.allocations.len);
        errdefer pool.allocator.free(leases);
        @memset(leases, null);
        const completions = try pool.allocator.alloc(?storage.Completion, pool.backends.len);
        errdefer pool.allocator.free(completions);
        @memset(completions, null);
        const host_resources = try pool.allocator.alloc(?HostInputStorage, pool.host_input_plans.len);
        errdefer pool.allocator.free(host_resources);
        @memset(host_resources, null);
        const host_inputs = try pool.allocator.alloc(interpreter.RuntimeInput, pool.host_input_plans.len);
        errdefer pool.allocator.free(host_inputs);
        const bound_inputs = try pool.allocator.alloc(interpreter.RuntimeInput, pool.replay_inputs.len);
        errdefer pool.allocator.free(bound_inputs);
        const binding_states = try pool.allocator.alloc(ReplayBindingState, pool.replay_inputs.len);
        errdefer pool.allocator.free(binding_states);
        @memset(binding_states, .{});
        const capture_pins = try pool.allocator.alloc(bool, if (pool.replay_backend != null) leases.len else 0);
        errdefer pool.allocator.free(capture_pins);
        @memset(capture_pins, false);
        const host_bytes: usize = if (pool.host_workspace_allocation == buffers.invalid_allocation) 0 else @intCast(pool.plan.allocations[pool.host_workspace_allocation].byte_size);
        const host_binding = if (capability) |cap| try reserveHost(pool, cap, host_bytes) else admission.Binding{};
        errdefer host_binding.rollback(pool.allocator);
        const host_workspace: []u8 = if (host_bytes == 0) &.{} else (pool.allocator.rawAlloc(host_bytes, pool.host_alignment, @returnAddress()) orelse return error.OutOfMemory)[0..host_bytes];
        errdefer if (host_bytes != 0) pool.allocator.rawFree(host_workspace, pool.host_alignment, @returnAddress());
        const assigned = try pool.allocator.alloc(?route.Assigned, if (pool.selection != null) pool.kernels.len else 0);
        errdefer pool.allocator.free(assigned);
        @memset(assigned, null);
        const region_claims = try pool.allocator.alloc(?@import("region_route.zig").PayloadClaim, pool.regions.len);
        errdefer pool.allocator.free(region_claims);
        @memset(region_claims, null);
        const region_started = try pool.allocator.alloc(bool, pool.regions.len);
        errdefer pool.allocator.free(region_started);
        @memset(region_started, false);
        const region_assigned = try pool.allocator.alloc(?@import("region_route.zig").AssignedResources, pool.regions.len);
        errdefer pool.allocator.free(region_assigned);
        @memset(region_assigned, null);
        const self = try pool.allocator.create(Frame);
        self.* = .{ .region_claims = region_claims, .region_started = region_started, .region_assigned = region_assigned, .host_input_resources = host_resources, .host_bound_inputs = host_inputs, .assigned = assigned, .storage_admission = capability, .host_workspace_admission = host_binding.reservation, .host_workspace_retirement_owner = if (host_binding.backing) |value| value.owner else null, .host_workspace = host_workspace, .capture_pins = capture_pins, .pool = pool, .storages = leases, .completions = completions, .bound_inputs = bound_inputs, .binding_states = binding_states, .stats = .{ .replay_decision = pool.replay_decision } };
        if (host_bytes != 0) {
            if (pool.accounting) |ledger| ledger.allocated(.host_workspace, host_bytes, host_bytes);
            self.stats.allocations += 1;
            self.stats.allocated_bytes += host_bytes;
        }
        return self;
    }
    fn reusableCredit(self: *const Frame, identity: ?admission.Identity, requirement: ?*@import("../runtime/tier/memory.zig").AdmissionAmounts) !?@import("../runtime/tier/memory.zig").AdmissionAmounts {
        if (self.in_use or self.retired) return null;
        for (self.region_claims) |maybe| if (maybe) |claim| if (claim.held) return null;
        var credit = @import("../runtime/tier/memory.zig").AdmissionAmounts{};
        if (identity) |expected| if (self.host_workspace_admission) |reserved| {
            if (!reserved.identity.eql(expected)) return null;
            credit.host_scratch_bytes = hostOwnedAmounts(reserved, self.host_workspace_retirement_owner, self.host_workspace.len).host_scratch_bytes;
        };
        for (self.storages, 0..) |maybe, id| if (maybe) |lease| {
            const shared_literal = self.pool.plan.allocations[id].kind == .constant_binding;
            if (!shared_literal) {
                const capture_pin: usize = if (self.replay_ready) @intFromBool(self.capture_pins[id]) else 0;
                var private_pins: usize = 0;
                for (self.pool.host_input_plans, self.host_input_resources) |input, resource| {
                    if (input.allocation != id or resource == null or resource.?.private_view == null) continue;
                    const backend = self.pool.backends[input.backend_index];
                    const functions = backend.vtable.kernels orelse return null;
                    const exclusive = functions.hostInputViewExclusive orelse return null;
                    if (!functions.accepts(backend.vtable) or !exclusive(backend.ptr, resource.?.private_view.?)) return null;
                    private_pins += 1;
                }
                if (lease.references.load(.acquire) != 1 + capture_pin + private_pins) return null;
            }
            if (identity) |expected| {
                if (requirement) |refined| {
                    if (id >= self.pool.storage_requirement_bounds.len) return error.InvalidPhysicalAllocation;
                    const bound = self.pool.storage_requirement_bounds[id];
                    if (!shared_literal and bound != 0) {
                        const retained = lease.backing_byte_size orelse lease.byte_size;
                        const domain = if (lease.backend.kind() == .native) &refined.host_scratch_bytes else &refined.backend_scratch_bytes;
                        if (domain.* < bound) return error.InvalidPhysicalStorageReservation;
                        domain.* = try std.math.add(usize, domain.* - bound, retained);
                    }
                }
                if (lease.admission) |reserved| {
                    if (!reserved.identity.eql(expected)) return null;
                    // Shared literal ownership cannot be lent by an exclusive frame.
                    if (shared_literal) continue;
                    const owned = lease.admittedAmounts();
                    credit.host_scratch_bytes = try std.math.add(usize, credit.host_scratch_bytes, owned.host_scratch_bytes);
                    credit.backend_scratch_bytes = try std.math.add(usize, credit.backend_scratch_bytes, owned.backend_scratch_bytes);
                }
            }
        };
        if (identity) |expected| for (self.host_input_resources) |maybe| if (maybe) |resource| if (resource.admission) |reserved| {
            if (!reserved.identity.eql(expected)) return null;
            credit.host_scratch_bytes = try std.math.add(usize, credit.host_scratch_bytes, hostOwnedAmounts(reserved, resource.retirement_owner, resource.resource.requirements.host_bytes).host_scratch_bytes);
        };
        return credit;
    }

    fn reserveHost(pool: *Pool, capability: admission.Capability, bytes: usize) !admission.Binding {
        if (bytes == 0) return .{};
        return capability.reserveOwned(pool.allocator, .native, .host_workspace, bytes);
    }
    fn hostOwnedAmounts(reserved: *const admission.Reservation, owner: ?*const @import("backing_retirement.zig").BackingOwner, actual_bytes: usize) @import("../runtime/tier/memory.zig").AdmissionAmounts {
        if (owner) |value| return value.charge.storageAmountsForBytes(@min(actual_bytes, value.admittedBytes()));
        return reserved.amounts();
    }
    fn adoptAdmission(self: *Frame, capability: admission.Capability) !void {
        if (self.host_workspace_admission) |existing| {
            if (!existing.identity.eql(capability.identity)) return error.PhysicalStorageAdmissionMismatch;
        } else {
            const binding = try reserveHost(self.pool, capability, self.host_workspace.len);
            self.host_workspace_admission = binding.reservation;
            self.host_workspace_retirement_owner = if (binding.backing) |value| value.owner else null;
        }
        for (self.storages) |maybe| if (maybe) |lease| try lease.ensureAdmission(capability);
        for (self.host_input_resources) |*maybe| if (maybe.*) |*resource| {
            if (resource.admission) |existing| {
                if (!existing.identity.eql(capability.identity)) return error.PhysicalStorageAdmissionMismatch;
            } else {
                const binding = try reserveHost(self.pool, capability, resource.resource.requirements.host_bytes);
                resource.admission = binding.reservation;
                resource.retirement_owner = if (binding.backing) |value| value.owner else null;
            }
        };
    }
    fn allocateStorage(self: *Frame, cb: ops.ComputeBackend, bytes: usize, class: accounting.Class) !?*storage.Lease {
        return storage.Lease.createAdmitted(self.pool.allocator, cb, bytes, self.pool.accounting, class, self.storage_admission) catch |err| {
            if (self.storage_admission != null) self.storage_admission_failed = true;
            return err;
        };
    }
    fn deinit(self: *Frame) void {
        self.deinitWithBoundary(null);
    }
    fn deinitWithBoundary(self: *Frame, boundary: ?*const @import("retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(.{ .boundary = boundary });
    }
    fn deinitWithCleanup(self: *Frame, cleanup: @import("cleanup_context.zig").Context) void {
        self.deinitCheckedWithCleanup(cleanup) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    fn deinitChecked(self: *Frame) !void {
        return self.deinitCheckedWithBoundary(null);
    }
    fn deinitCheckedWithBoundary(self: *Frame, boundary: ?*const @import("retirement_sink.zig").Epoch) !void {
        return self.deinitCheckedWithCleanup(.{ .boundary = boundary });
    }
    fn deinitCheckedWithCleanup(self: *Frame, cleanup: @import("cleanup_context.zig").Context) !void {
        try self.waitForRetirementChecked();
        self.deinitAfterWaitWithCleanup(cleanup);
    }
    fn waitForRetirementChecked(self: *Frame) !void {
        try self.releaseRegionClaimsChecked();
        if (self.retired) for (self.storages, 0..) |maybe, id| if (maybe != null and self.pool.plan.allocations[id].kind != .constant_binding) {
            const kind = self.pool.plan.allocations[id].backend;
            if (kind == .native) continue;
            var proven = false;
            for (self.pool.backends, self.completions) |backend, completion| if (backend.kind() == kind) {
                if (completion) |fence| if (fence.vtable.wait != null) {
                    proven = true;
                };
            };
            if (!proven) return error.PhysicalCompletionUnavailable;
        };
        for (self.completions) |completion| if (completion) |value| try value.wait();
    }
    fn deinitAfterWait(self: *Frame) void {
        self.deinitAfterWaitWithBoundary(null);
    }
    fn deinitAfterWaitWithBoundary(self: *Frame, boundary: ?*const @import("retirement_sink.zig").Epoch) void {
        self.deinitAfterWaitWithCleanup(.{ .boundary = boundary });
    }
    fn deinitAfterWaitWithCleanup(self: *Frame, cleanup: @import("cleanup_context.zig").Context) void {
        if (cleanup.boundary == null) {
            if (self.host_workspace_retirement_owner != null) @import("antfly_platform").inference_process_supervisor.restartWorker();
            for (self.host_input_resources) |maybe| if (maybe) |resource| if (resource.retirement_owner != null) @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        // Eviction is a cold boundary. Complete submissions before retiring
        // frame addresses, including views which outlive this frame owner.

        for (self.completions) |completion| if (completion) |value| value.deinit();
        self.releaseHostInputs();
        for (self.host_input_resources, 0..) |maybe, index| if (maybe) |resource| {
            if (resource.private_view) |value| self.pool.backends[self.pool.host_input_plans[index].backend_index].freeWithCleanup(value, cleanup);
            const bytes = resource.resource.requirements.host_bytes;
            resource.resource.deinit();
            if (resource.retirement_owner) |owned| {
                owned.release(cleanup.boundary orelse @import("antfly_platform").inference_process_supervisor.restartWorker());
            } else if (resource.admission) |reserved| {
                reserved.release();
                self.pool.allocator.destroy(reserved);
            }
            if (bytes != 0) if (self.pool.accounting) |ledger| ledger.freed(.host_workspace, bytes, resource.resource.requirements.host_backing_bytes);
        };
        self.pool.allocator.free(self.host_input_resources);
        self.pool.allocator.free(self.host_bound_inputs);
        if (self.replay) |replay| replay.deinit();
        for (self.assigned) |maybe| if (maybe) |resources| resources.deinit(resources.context);
        self.pool.allocator.free(self.assigned);
        self.deinitRegionResources();
        if (self.pool.replay_backend) |index| for (self.binding_states) |state| if (state.weight) |value| self.pool.backends[index].free(value);
        self.pool.allocator.free(self.binding_states);
        self.pool.allocator.free(self.capture_pins);
        self.pool.allocator.free(self.bound_inputs);
        for (self.storages, 0..) |lease, id| if (lease) |value| {
            // Literal addresses are borrowed from the program-level owner.
            if (self.pool.plan.allocations[id].kind != .constant_binding) value.releaseWithCleanup(cleanup);
        };
        const host_bytes = self.host_workspace.len;
        if (host_bytes != 0) self.pool.allocator.rawFree(self.host_workspace, self.pool.host_alignment, @returnAddress());
        if (self.host_workspace_retirement_owner) |owned| {
            owned.release(cleanup.boundary orelse @import("antfly_platform").inference_process_supervisor.restartWorker());
        } else if (self.host_workspace_admission) |value| {
            value.release();
            self.pool.allocator.destroy(value);
        }
        if (host_bytes != 0) if (self.pool.accounting) |ledger| ledger.freed(.host_workspace, host_bytes, host_bytes);
        self.pool.allocator.free(self.completions);
        self.pool.allocator.free(self.storages);
        self.pool.allocator.destroy(self);
    }
    pub fn begin(self: *Frame) !void {
        for (self.pool.regions, self.region_claims) |installed, maybe| if (installed.owner.requirements.payload_claim_required and maybe == null) return error.PreparedRegionClaimRequired;
        std.debug.assert(self.scope_backends == 0);
        // Replays skip runAssignedKernel: validate mutable code policy at the
        // shared frame boundary before provider scope or any input upload.
        if (self.pool.selection) |*selected| selected.validateRetainedPolicies(self.pool) catch |err| {
            self.retired = true;
            return err;
        };
        if (self.pool.host_input_plans.len != 0) {
            for (self.pool.host_input_plans) |input| if (self.storages[input.allocation]) |storage_owner| storage_owner.invalidateHostIndices();
            self.host_input_generation = std.math.add(u64, self.host_input_generation, 1) catch {
                self.retired = true;
                return error.HostInputGenerationExhausted;
            };
        }
        errdefer self.endScope();
        for (self.pool.backends) |backend| {
            if (backend.vtable.kernels) |functions| if (functions.physicalExecutionScope) |enter| try enter(backend.ptr, self.storages);
            self.scope_backends += 1;
        }
    }
    fn endScope(self: *Frame) void {
        while (self.scope_backends != 0) {
            self.scope_backends -= 1;
            const backend = self.pool.backends[self.scope_backends];
            if (backend.vtable.kernels) |functions| if (functions.physicalExecutionScope) |leave| leave(backend.ptr, null) catch {
                self.retired = true;
            };
        }
    }
    pub fn finish(self: *Frame) !Stats {
        return self.finishWithCleanup(.{});
    }
    pub fn finishWithCleanup(self: *Frame, cleanup: @import("cleanup_context.zig").Context) !Stats {
        try self.releaseRegionClaimsChecked();
        self.releaseHostInputs();
        const owner = self.owner_pin;
        self.owner_pin = null;
        // Exact variants remain alive through resource retirement. A transient
        // Frame is destroyed after unlocking, while output Leases survive it.
        defer if (owner) |value| value.releaseWithCleanup(cleanup);
        if (!self.transient) return self.finishRecorded();
        const pool = self.pool;
        const collect = self.collect_storage_stats;
        var result = self.finishRecorded() catch |err| {
            self.deinitWithCleanup(cleanup);
            return err;
        };
        self.deinitWithCleanup(cleanup);
        // Sample after transient scratch/resources retire, so the reported
        // live span includes retained outputs rather than a dead frame arena.
        if (collect) if (pool.accounting) |ledger| {
            result.storage_usage = ledger.snapshot();
        };
        return result;
    }

    fn finishRecorded(self: *Frame) !Stats {
        @import("antfly_platform").sync.lockYielding(&self.pool.mutex);
        defer self.pool.mutex.unlock();
        defer self.in_use = false;
        defer self.storage_admission = null;
        defer self.endScope();
        for (self.completions) |completion| if (completion) |value| value.record() catch |err| {
            // An uncertain completion must never authorize address reuse.
            self.retired = true;
            return err;
        };
        if (self.collect_storage_stats) if (self.pool.accounting) |ledger| {
            self.stats.storage_usage = ledger.snapshot();
        };
        return self.stats;
    }

    fn hostInputResource(self: *Frame, index: usize) !*HostInputStorage {
        const plan = self.pool.host_input_plans[index];
        const backend = self.pool.backends[plan.backend_index];
        const functions = backend.vtable.kernels orelse return error.PreparedHostIngressUnavailable;
        if (!functions.accepts(backend.vtable)) return error.PreparedHostIngressUnavailable;
        const required = if (plan.requirements.validation_scope == .prepared_program) plan.requirements else (try functions.hostInputRequirements.?(backend.ptr, plan.shape)) orelse return error.PreparedHostIngressUnavailable;
        if (!required.eql(plan.requirements)) return error.PreparedHostIngressChanged;
        if (self.host_input_resources[index]) |*resource| return resource;
        const host_binding = if (self.storage_admission) |cap| try reserveHost(self.pool, cap, required.host_bytes) else admission.Binding{};
        errdefer host_binding.rollback(self.pool.allocator);
        const resource = (try functions.createHostInput.?(backend.ptr, .{
            .allocator = self.pool.allocator,
            .borrowed = plan.borrowed,
            .implementation = .{ .context = backend.ptr, .table = backend.vtable },
            .requirements = required,
            .slot = plan.outputSlot(),
            .index_bounds = plan.index_bounds,
        })) orelse return error.PreparedHostIngressUnavailable;
        errdefer resource.deinit();
        if (resource.borrowed != plan.borrowed or (plan.borrowed and (resource.vtable.bind_borrowed == null or resource.vtable.unbind_borrowed == null))) return error.PreparedHostIngressChanged;
        if (!std.meta.eql(resource.index_bounds, plan.index_bounds) or !resource.matchesSlot(plan.outputSlot()) or !resource.requirements.eql(required) or resource.implementation.context != backend.ptr or resource.implementation.table != @as(*const anyopaque, @ptrCast(backend.vtable))) return error.PreparedHostIngressChanged;
        self.host_input_resources[index] = .{ .resource = resource, .admission = host_binding.reservation, .retirement_owner = if (host_binding.backing) |value| value.owner else null };
        if (required.host_bytes != 0) {
            if (self.pool.accounting) |ledger| ledger.allocated(.host_workspace, required.host_bytes, required.host_backing_bytes);
            self.stats.allocations += 1;
            self.stats.allocated_bytes += required.host_bytes;
        }
        return &self.host_input_resources[index].?;
    }

    /// Upload directly to the admitted input slot before graph capture starts.
    /// The returned metadata belongs to this frame until releaseHostInputs.
    pub fn bindHostInputs(self: *Frame, inputs: []const host_ingress.Input) ![]const interpreter.RuntimeInput {
        return self.bindHostInputsWithControl(inputs, null);
    }
    pub fn bindHostInputsWithControl(self: *Frame, inputs: []const host_ingress.Input, control: ?@import("../execution_control.zig").InferenceExecutionControl) ![]const interpreter.RuntimeInput {
        if (self.retired or self.bound_host_input_count != 0) return error.InvalidHostIngressBinding;
        try self.pool.validateHostInputs(inputs);
        errdefer self.releaseHostInputs();
        for (inputs) |input| {
            if (control) |active| try active.check();
            const index = self.pool.hostInputIndex(input.node_id).?;
            const plan = self.pool.host_input_plans[index];
            const cb = &self.pool.backends[plan.backend_index];
            const resource = try self.hostInputResource(index);
            if (plan.borrowed) {
                const value = try resource.resource.vtable.bind_borrowed.?(resource.resource.context, input.value.data);
                self.host_bound_inputs[self.bound_host_input_count] = .{ .node_id = input.node_id, .value = value, .source_backend = cb };
                self.bound_host_input_count += 1;
                continue;
            }
            const lease = (try self.storageFor(cb, plan.backend_index, plan.allocation)) orelse return error.PreparedHostIngressUnavailable;
            try lease.bindAllocationSlot(plan.allocation, plan.epoch, @intCast(self.pool.plan.allocations[plan.allocation].byte_size));
            const assigned = route.AssignedOutput{ .input_generation = self.host_input_generation, .slot = plan.outputSlot(), .region = try lease.writableRegion(plan.allocation, plan.epoch, plan.requirements.bytes) };
            // Complete Native programs never publish a public-input alias. Keep
            // their private metadata with the frame, including its exact Lease
            // pin, rather than allocating/refcounting it on every warm call.
            const functions = cb.vtable.kernels.?;
            const cache_view = self.pool.payload_coverage == .complete and cb.kind() == .native and functions.hostInputViewExclusive != null and functions.implementation_table == @as(*const anyopaque, @ptrCast(cb.vtable));
            const value = if (resource.private_view) |view| view else create: {
                const view = try self.viewForShape(cb, lease, plan.shape);
                if (cache_view) resource.private_view = view;
                break :create view;
            };
            errdefer if (!cache_view) cb.free(value);
            resource.resource.write(value, assigned, input.value.data) catch |err| {
                self.retired = true;
                return err;
            };
            self.host_bound_inputs[self.bound_host_input_count] = .{ .node_id = input.node_id, .value = value, .source_backend = cb };
            self.bound_host_input_count += 1;
        }
        return self.host_bound_inputs[0..self.bound_host_input_count];
    }

    fn hostProofsCurrent(self: *const Frame) bool {
        if (self.pool.host_input_plans.len == 0) return true;
        if (self.bound_host_input_count != self.pool.host_input_plans.len or self.host_input_generation == 0) return false;
        for (self.pool.host_input_plans) |input| if (input.index_bounds) |bounds| {
            const owner = self.storages[input.allocation] orelse return false;
            if (!owner.hostIndicesCertified(bounds.extent, self.host_input_generation, input.requirements.bytes)) return false;
        };
        return true;
    }
    pub fn releaseHostInputs(self: *Frame) void {
        for (self.host_bound_inputs[0..self.bound_host_input_count]) |input| {
            const index = self.pool.hostInputIndex(input.node_id).?;
            if (self.host_input_resources[index]) |resource| if (resource.resource.borrowed) {
                resource.resource.vtable.unbind_borrowed.?(resource.resource.context) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                continue;
            };
            const cached = if (self.host_input_resources[index]) |resource| resource.private_view else null;
            if (cached == null or cached.? != input.value) input.source_backend.?.free(input.value);
        }
        self.bound_host_input_count = 0;
    }

    fn ownsHostInput(self: *const Frame, node: NodeId, value: CT) bool {
        for (self.host_bound_inputs[0..self.bound_host_input_count]) |input| if (input.node_id == node and input.value == value) return true;
        return false;
    }

    /// Bind mutable values by device copy before recording or replaying.
    /// Immutable handles are retained separately and must keep their identity.
    pub fn bindReplayInputs(self: *Frame, options: *const interpreter.ExecuteOptions) !?[]const interpreter.RuntimeInput {
        if (!self.hostProofsCurrent()) return self.declineReplay(.incomplete_bindings);

        const index = self.pool.replay_backend orelse return null;
        if (self.replay_disabled) |reason| return self.declineReplay(reason);
        if (options.execution_control != null) return self.declineReplay(.controlled_request);
        if (options.attention != null or options.embedding_ids != null or options.sdpa_mask != null or options.cross_attention_mask != null or options.donate != null or options.deferred_input != null) return self.declineReplay(.side_channels);
        const inputs = options.runtime_inputs orelse return self.declineReplay(.incomplete_bindings);
        if (inputs.len > self.bound_inputs.len) return self.declineReplay(.incomplete_bindings);
        const cb = &self.pool.backends[index];
        // A prepared node->binding table keeps matching linear in the number
        // of public inputs, independent of initializer/constant count.
        for (self.binding_states) |*state| {
            state.supplied = false;
            state.host_direct = false;
        }
        for (inputs) |input| {
            if (input.node_id >= self.pool.replay_input_indices.len) return self.declineReplay(.incomplete_bindings);
            const i = self.pool.replay_input_indices[input.node_id];
            if (i == ml.graph.null_node or self.binding_states[i].supplied) return self.declineReplay(.incomplete_bindings);
            if (input.source_backend) |owner| if (owner.ptr != cb.ptr or owner.vtable != cb.vtable) return self.declineReplay(.foreign_inputs);
            const binding = self.pool.replay_inputs[i];
            // Per-node literal overrides retain their ordinary semantics;
            // they must never mutate a shared immutable captured address.
            if (binding.constant != null) return self.declineReplay(.literal_override);
            if (!cb.vtable.kernels.?.physicalReplayTensorSupported.?(cb.ptr, input.value)) {
                if (binding.allocation == null) self.replay_disabled = .unsupported_representation;
                return self.declineReplay(.unsupported_representation);
            }
            if (binding.allocation == null) if (self.binding_states[i].weight) |weight| {
                if (weight != input.value) return self.declineReplay(.changed_weights);
            };
            self.binding_states[i].supplied = true;
            self.bound_inputs[i] = input;
        }
        for (self.pool.replay_inputs, self.binding_states, self.bound_inputs) |binding, state, *input| {
            if (state.supplied) continue;
            if (binding.constant == null) return self.declineReplay(.incomplete_bindings);
            input.* = .{ .node_id = binding.node, .value = undefined };
        }
        // Reject unsupported immutable ownership before allocating/copying
        // mutable banks. A permanent decline must not tax every invocation.
        for (self.pool.replay_inputs, self.bound_inputs, 0..) |binding, input, i| {
            if (binding.allocation == null and self.binding_states[i].weight == null) {
                self.binding_states[i].weight = (try cb.vtable.kernels.?.retainImmutableTensor.?(cb.ptr, input.value)) orelse {
                    self.replay_disabled = .unsupported_owner;
                    return self.declineReplay(.unsupported_owner);
                };
            }
        }
        var complete = false;
        defer if (!complete) self.releaseReplayInputs();
        for (self.pool.replay_inputs, self.bound_inputs, 0..) |binding, *input, i| {
            if (binding.constant != null) {
                input.value = try self.valueForLiteral(cb, index, binding);
                input.source_backend = cb;
            } else if (binding.allocation) |allocation_id| {
                if (self.ownsHostInput(binding.node, input.value)) {
                    self.binding_states[i].host_direct = true;
                    input.source_backend = cb;
                    self.bound_input_count = i + 1;
                    continue;
                }
                const allocation = (try self.storageFor(cb, index, allocation_id)) orelse return null;
                const target = try self.viewForShape(cb, allocation, binding.shape);
                cb.vtable.kernels.?.copyPhysicalInput.?(cb.ptr, target, input.value) catch |err| {
                    cb.free(target);
                    return err;
                };
                // Borrow one invocation handle; the allocation stays fixed.
                input.value = target;
                input.source_backend = cb;
                self.stats.input_copies += 1;
                self.stats.input_copy_bytes += @intCast(binding.shape.numElements().? * @sizeOf(f32));
            }
            self.bound_input_count = i + 1;
        }
        complete = true;
        return self.bound_inputs;
    }

    fn declineReplay(self: *Frame, reason: ReplayDecision) ?[]const interpreter.RuntimeInput {
        self.stats.replay_decision = reason;
        self.stats.capture_failure = self.replay_failure;
        return null;
    }

    pub fn releaseReplayInputs(self: *Frame) void {
        const index = self.pool.replay_backend orelse return;
        const cb = self.pool.backends[index];
        for (self.pool.replay_inputs[0..self.bound_input_count], self.bound_inputs[0..self.bound_input_count], self.binding_states[0..self.bound_input_count]) |binding, input, state| if (binding.allocation != null and binding.constant == null and !state.host_direct) cb.free(input.value);
        self.bound_input_count = 0;
    }

    pub fn beginReplayCapture(self: *Frame) !bool {
        if (!self.hostProofsCurrent()) return error.MissingCurrentHostIndexProof;
        if (!self.warmed or self.replay_ready) return false;
        const index = self.pool.replay_backend orelse return false;
        const backend = self.pool.backends[index];
        if (self.pool.coverage_reachable) |reachable| if (reachable.len != self.pool.kernels.len) return error.InvalidReachabilityTable;
        for (self.pool.kernels, 0..) |maybe, node| if (maybe) |kernel| {
            if (self.pool.coverage_reachable) |reachable| if (!reachable[node]) continue;
            const allowed = if (self.pool.preparedKernelRoute(@intCast(node))) |owned| owned.requirements.capture == .supported else backend.vtable.kernels.?.physicalReplayKernelSupported.?(backend.ptr, &kernel.descriptor);
            if (!allowed) {
                self.stats.replay_decision = .unsupported_graph;
                self.replay_disabled = .unsupported_graph;
                return false;
            }
        };
        if (self.replay == null) self.replay = (try self.pool.backends[index].vtable.kernels.?.createPhysicalReplay.?(self.pool.backends[index].ptr)) orelse return false;
        const started = try self.replay.?.begin(self.storages);
        if (!started) self.stats.replay_decision = .capture_busy;
        return started;
    }

    pub fn abortReplayCapture(self: *Frame, err: anyerror) void {
        if (self.replay) |replay| replay.abort();
        self.replay_disabled = .capture_failed;
        self.replay_failure = @errorName(err);
        self.stats.replay_decision = .capture_failed;
        self.stats.capture_failure = self.replay_failure;
        self.stats.capture_failures += 1;
        self.warmed = false;
    }

    pub fn commitReplayCapture(self: *Frame) !void {
        try self.replay.?.commit();
        // Deduplication belongs to the cold capture boundary. Reuse performs
        // one reference-count comparison per allocation, not a pins scan.
        for (self.storages, self.capture_pins) |lease, *pin| pin.* = if (lease) |value| self.replay.?.retains(value) else false;
        self.replay_ready = true;
        self.stats.graph_captures += 1;
        self.stats.replay_decision = .captured;
    }

    pub fn replayOutputs(self: *Frame, allocator: std.mem.Allocator) ![]CT {
        if (!self.hostProofsCurrent()) return error.MissingCurrentHostIndexProof;
        const index = self.pool.replay_backend orelse return error.InvalidShape;
        const cb = &self.pool.backends[index];
        const outputs = try allocator.alloc(CT, self.pool.replay_outputs.len);
        errdefer allocator.free(outputs);
        var initialized: usize = 0;
        errdefer for (outputs[0..initialized]) |output| cb.free(output);
        for (self.pool.replay_outputs, outputs) |binding, *output| {
            const allocation = self.storages[binding.allocation] orelse return error.InvalidShape;
            output.* = try self.viewForShape(cb, allocation, binding.shape);
            if (try self.publishLiteralOutput(cb, binding.node, output.*)) |independent| {
                cb.free(output.*);
                output.* = independent;
            }
            initialized += 1;
        }
        try self.replay.?.launch();
        self.stats.graph_replays += 1;
        self.stats.replay_decision = .replayed;
        return outputs;
    }

    fn valueForLiteral(self: *Frame, _: *const ops.ComputeBackend, backend_index: usize, binding: ReplayInput) !CT {
        const store = self.pool.literal_store orelse return error.InvalidLiteralPlan;
        const literal = store.literals[binding.constant_index.?];
        if (literal.backend_index != backend_index or literal.allocation != binding.allocation.?) return error.InvalidLiteralPlan;
        return self.literalValue(binding.constant_index.?);
    }
    fn literalValue(self: *Frame, index: usize) !CT {
        const store = self.pool.literal_store orelse return error.InvalidLiteralPlan;
        const literal = store.literals[index];
        const cb = store.backends[literal.backend_index];
        if (self.completions[literal.backend_index] == null) {
            if (cb.vtable.kernels.?.createPhysicalCompletion) |make| self.completions[literal.backend_index] = try make(cb.ptr);
        }
        const result = store.value(index, self.storage_admission, self.pool.accounting) catch |err| {
            if (self.storage_admission != null) self.storage_admission_failed = true;
            return err;
        };
        self.storages[literal.allocation] = result.lease; // Store owns it.
        if (result.fresh) {
            self.stats.allocations += 1;
            self.stats.allocated_bytes += literal.data.len * 4;
        } else self.stats.reused_allocations += 1;
        return result.value;
    }
    /// Materialize at the original constant-node checkpoint. This returns
    /// an owned metadata reference; the immutable Store keeps its own owner.
    pub fn materializeLiteral(self: *Frame, cb: *const ops.ComputeBackend, node: NodeId) !?CT {
        const store = self.pool.literal_store orelse return null;
        if (node >= store.node_indices.len) return null;
        const index = store.node_indices[node];
        if (index == ml.graph.null_node or !self.pool.literalEnabled(index)) return null;
        const literal = store.literals[index];
        const owner = store.backends[literal.backend_index];
        if (cb.ptr != owner.ptr or cb.vtable != owner.vtable) return null;
        const functions = cb.vtable.kernels.?;
        if (functions.physicalLiteralBindingSupported) |eligible| if (!eligible(cb.ptr)) return null;
        const value = try self.literalValue(index);
        const lease = self.storages[literal.allocation].?;
        var dimensions: [8]i32 = undefined;
        for (literal.shape.dims[0..literal.shape.rank()], 0..) |dimension, axis| dimensions[axis] = @intCast(dimension);
        return (try functions.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = dimensions[0..literal.shape.rank()], .layout_source = value })) orelse error.UnsupportedPhysicalTensorView;
    }
    pub fn publishLiteralOutput(self: *Frame, cb: *const ops.ComputeBackend, node: NodeId, value: CT) !?CT {
        const store = self.pool.literal_store orelse return null;
        if (node >= store.publications.len) return null;
        const publication = store.publications[node] orelse return null;
        const literal = store.literals[publication.literal];
        const owner = store.backends[literal.backend_index];
        if (cb.ptr != owner.ptr or cb.vtable != owner.vtable) return null;
        var access = @import("storage_ownership.zig").ReadOnly{};
        const query = cb.vtable.kernels.?.storageOwnership orelse return null;
        _ = try query(cb.ptr, value, access.visitor());
        if (!access.value or access.largest_requested > self.pool.plan.allocations[publication.allocation].byte_size) return null;
        const backing = (try self.storageFor(cb, literal.backend_index, publication.allocation)) orelse return null;
        var dimensions: [8]i32 = undefined;
        for (publication.shape.dims[0..publication.shape.rank()], 0..) |dimension, axis| dimensions[axis] = @intCast(dimension);
        const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = backing, .shape = dimensions[0..publication.shape.rank()], .layout_source = value })) orelse return error.UnsupportedPhysicalTensorView;
        errdefer cb.free(output);
        try cb.vtable.kernels.?.copyPhysicalInput.?(cb.ptr, output, value);
        self.stats.output_copies += 1;
        return output;
    }

    fn storageFor(self: *Frame, cb: *const ops.ComputeBackend, backend_index: usize, allocation_id: buffers.AllocationId) !?*storage.Lease {
        if (allocation_id >= self.storages.len or backend_index >= self.pool.backends.len or self.pool.plan.allocationById(allocation_id) == null) return error.InactivePhysicalAllocation;
        if (self.storages[allocation_id] == null) {
            if (self.completions[backend_index] == null) {
                if (cb.vtable.kernels.?.createPhysicalCompletion) |make_completion| self.completions[backend_index] = try make_completion(cb.ptr);
            }
            const bytes: usize = @intCast(self.pool.plan.allocations[allocation_id].byte_size);
            self.storages[allocation_id] = (try self.allocateStorage(cb.*, bytes, if (self.pool.plan.allocations[allocation_id].kind == .workspace) .workspace else .activation)) orelse return null;
            self.stats.allocations += 1;
            self.stats.allocated_bytes += bytes;
        } else self.stats.reused_allocations += 1;
        return self.storages[allocation_id];
    }

    pub fn assignedOutput(self: *Frame, node: NodeId, lease: *storage.Lease) !route.AssignedOutput {
        const selected = self.pool.preparedKernelRoute(node) orelse return error.MissingPreparedKernelRoute;
        const slot = self.pool.outputSlot(node, selected.requirements.primary_bytes);
        try lease.bindAllocationSlot(slot.allocation, slot.epoch, @intCast(self.pool.plan.allocations[slot.allocation].byte_size));
        return .{ .slot = slot, .region = try lease.writableRegion(slot.allocation, slot.epoch, slot.writable_bytes) };
    }

    fn regionIndex(self: *const Frame, installed: *const @import("region_binding.zig").Installed) !usize {
        for (self.pool.regions, 0..) |*candidate, index| if (candidate == installed) return index;
        return error.PreparedRegionOwnerChanged;
    }

    /// Every selected region consumes its already-admitted, exact Frame resources.
    /// No runtime pattern selection or numerical fallback occurs on this path.
    pub fn executeRegion(self: *Frame, installed: *const @import("region_binding.zig").Installed, facts: []const route.Binding, inputs: []const CT) !@import("region_binding.zig").Bound {
        const region = @import("region_route.zig");
        const index = try self.regionIndex(installed);
        const backend = self.pool.backends[installed.backend_index];
        if (!self.in_use or self.claimed or index >= self.region_claims.len) return error.InvalidPreparedRegionClaim;
        const claim: ?*const region.PayloadClaim = if (self.region_claims[index]) |*value| value else null;
        if (installed.owner.requirements.payload_claim_required and claim == null) return error.PreparedRegionClaimRequired;
        const policy = try self.pool.currentRegionPolicy(installed.backend_index);
        var bound = try self.acquireRegionOutputs(&backend, installed, facts, policy);
        errdefer bound.deinit();
        var invocation = bound.invocation(.{ .context = backend.ptr, .table = backend.vtable }, policy, facts, inputs);
        invocation.validation.payload_claim = claim;
        // Preparing resources can itself submit packing/probe work. A thrown
        // error from here therefore requires checked drain before releasing pins.
        self.region_started[index] = true;
        errdefer self.retired = true;
        if (self.region_assigned[index] == null) self.region_assigned[index] = try installed.owner.prepareAssigned(invocation);
        invocation.prepared_resources = if (self.region_assigned[index]) |*resources| resources else null;
        const outcome = try installed.owner.dispatch(invocation);
        if (outcome == .declined_before_submission) return error.PreparedRegionDeclined;
        self.stats.dispatches += 1;
        if (self.region_claims[index]) |*owned| {
            // Concrete numerical callbacks established independent tickets before
            // encoding on EVERY actual command buffer, including nested flushes.
            // This confirms that handoff; attaching only to the current CB here
            // would be too late and is forbidden by the backend contract.
            try owned.transferToSubmission(.{ .context = backend.ptr, .table = backend.vtable }, .completed_or_enqueued, null);
            self.region_claims[index] = null;
        }
        return bound;
    }

    /// Successful encoding already handed its roots to real completion tickets.
    /// Untouched regions release metadata-only pins. Uncertain numerical failures
    /// explicitly drain before release, outside the Pool mutex; no reuse/fence
    /// enqueue is treated as terminal completion proof.
    fn releaseRegionClaimsChecked(self: *Frame) !void {
        for (self.region_claims, self.region_started) |*maybe, started| {
            if (maybe.*) |*claim| {
                if (started) try claim.drain();
                claim.release();
                maybe.* = null;
            }
        }
        @memset(self.region_started, false);
    }

    fn deinitRegionResources(self: *Frame) void {
        for (self.region_assigned) |maybe| if (maybe) |resources| resources.deinit(resources.context);
        self.pool.allocator.free(self.region_assigned);
        self.pool.allocator.free(self.region_started);
        self.pool.allocator.free(self.region_claims);
    }

    /// Cold installation stores `Installed` inside this exact variant Pool. A
    /// pointer/key from another pool/frame/backend cannot acquire its ranges.
    pub fn acquireRegionOutputs(self: *Frame, cb: *const ops.ComputeBackend, installed: *const @import("region_binding.zig").Installed, facts: []const route.Binding, policy_key: u64) !@import("region_binding.zig").Bound {
        const binding = @import("region_binding.zig");
        const region_index = try self.regionIndex(installed);
        if (installed.key != installed.owner.key or installed.backend_index >= self.pool.backends.len or installed.outputs.len > binding.max_outputs) return error.PreparedRegionOwnerChanged;
        const backend = self.pool.backends[installed.backend_index];
        if (backend.ptr != cb.ptr or backend.vtable != cb.vtable or installed.owner.implementation.context != cb.ptr or installed.owner.implementation.table != @as(*const anyopaque, cb.vtable)) return error.PreparedRegionOwnerChanged;
        var result = binding.Bound{ .backend = cb.* };
        errdefer result.deinit();
        for (installed.outputs, 0..) |output, index| {
            if (output.slot.epoch != installed.epoch or output.slot.allocation >= self.pool.plan.allocations.len) return error.PreparedRegionOutputMismatch;
            if (output.slot.writable_bytes > self.pool.plan.allocations[output.slot.allocation].byte_size) return error.PreparedRegionOutputMismatch;
            if (output.alias_index) |parent| {
                if (parent >= index or installed.outputs[parent].alias_index != null or installed.outputs[parent].slot.allocation != output.slot.allocation or output.slot.writable_bytes != 0) return error.PreparedRegionOutputMismatch;
            }
            result.slots[index] = output.slot;
        }
        if (installed.host_workspace_bytes > self.host_workspace.len) return error.PreparedRegionWorkspaceMismatch;
        if (installed.workspace_bytes != 0 and (installed.workspace_allocation == buffers.invalid_allocation or installed.workspace_allocation >= self.pool.plan.allocations.len or installed.workspace_bytes % @sizeOf(f32) != 0 or installed.workspace_bytes > self.pool.plan.allocations[installed.workspace_allocation].byte_size)) return error.PreparedRegionWorkspaceMismatch;
        // No backing allocation, metadata CT, command encoder or driver call may
        // happen until all selected semantic/input/output facts have validated.
        try installed.owner.validate(.{ .implementation = .{ .context = cb.ptr, .table = cb.vtable }, .policy_key = policy_key, .inputs = facts, .outputs = result.slots[0..installed.outputs.len], .payload_claim = if (self.region_claims[region_index]) |*claim| claim else null });
        for (installed.outputs, 0..) |output, index| {
            const lease = (try self.storageFor(cb, installed.backend_index, output.slot.allocation)) orelse return error.PreparedRegionStorageUnavailable;
            var authority: ?route.AssignedOutput = null;
            if (output.alias_index == null) {
                try lease.bindAllocationSlot(output.slot.allocation, output.slot.epoch, @intCast(self.pool.plan.allocations[output.slot.allocation].byte_size));
                authority = .{ .slot = output.slot, .region = try lease.writableRegion(output.slot.allocation, output.slot.epoch, output.slot.writable_bytes) };
            }
            const tensor = try self.viewForShape(cb, lease, output.slot.logical);
            result.outputs[index] = .{ .tensor = tensor, .authority = authority };
            result.owned[index] = true;
            result.output_count += 1;
        }
        if (installed.workspace_bytes != 0) {
            if (installed.workspace_allocation == buffers.invalid_allocation or installed.workspace_bytes % @sizeOf(f32) != 0) return error.PreparedRegionWorkspaceMismatch;
            const lease = (try self.storageFor(cb, installed.backend_index, installed.workspace_allocation)) orelse return error.PreparedRegionStorageUnavailable;
            result.workspace = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = &.{@intCast(installed.workspace_bytes / @sizeOf(f32))} })) orelse return error.UnsupportedPhysicalTensorView;
        }
        if (installed.host_workspace_bytes > self.host_workspace.len) return error.PreparedRegionWorkspaceMismatch;
        result.host_workspace = self.host_workspace[0..installed.host_workspace_bytes];
        return result;
    }

    fn viewForShape(_: *Frame, target: *const ops.ComputeBackend, allocation: *storage.Lease, logical: ml.graph.Shape) !CT {
        var shape: [ml.graph.shape.max_rank]i32 = undefined;
        for (logical.dims[0..logical.rank()], 0..) |dim, axis| shape[axis] = @intCast(dim);
        return (try target.vtable.kernels.?.physicalTensorView.?(target.ptr, .{ .storage = allocation, .shape = shape[0..logical.rank()], .dtype = logical.dtype })) orelse error.UnsupportedPhysicalTensorView;
    }

    pub fn transfer(self: *Frame, source: *const ops.ComputeBackend, target: *const ops.ComputeBackend, value: CT, node: NodeId, partition: u32) !?CT {
        const index = self.pool.plan.transfer_index.get(buffers.transferKey(node, partition)) orelse return null;
        const binding = self.pool.transfers[index] orelse return null;
        if (try source.tensorDType(value) != .f32) return null;
        const selected = self.pool.backends[binding.backend_index];
        if (selected.ptr != target.ptr or selected.vtable != target.vtable) return null;
        const count: usize = @intCast(binding.shape.numElements().?);
        const result = if (target.kind() == .cuda) upload: {
            if (source.kind() != .native) return null;
            const host = (try source.hostFloat32Read(value)) orelse return null;
            if (host.len != count) return error.InvalidPhysicalTransferSize;
            const allocation = (try self.storageFor(target, binding.backend_index, binding.allocation)) orelse return null;
            const output = try self.viewForShape(target, allocation, binding.shape);
            errdefer target.free(output);
            try target.vtable.kernels.?.writePhysicalFloat32.?(target.ptr, output, host);
            break :upload output;
        } else download: {
            const functions = source.vtable.kernels orelse return null;
            if (functions.readFloat32Into == null) return null;
            const allocation = (try self.storageFor(target, binding.backend_index, binding.allocation)) orelse return null;
            const host = (try target.hostFloat32Write(allocation.tensor)) orelse return null;
            if (host.len < count) return error.InvalidPhysicalTransferSize;
            if (!try source.readFloat32Into(value, host[0..count])) return null;
            break :download try self.viewForShape(target, allocation, binding.shape);
        };
        self.stats.transfers += 1;
        self.stats.transfer_bytes += count * @sizeOf(f32);
        return result;
    }

    fn executeMetadataAlias(self: *Frame, cb: *const ops.ComputeBackend, values: []const ?CT, id: NodeId) !?CT {
        const selection = if (self.pool.selection) |*value| value else return null;
        for (self.pool.metadata_aliases, 0..) |alias, alias_index| {
            if (alias.node != id or !selection.alias_enabled[alias_index]) continue;
            if (id >= selection.plane_counts.len or selection.plane_counts[id] != 1) return null;
            const functions = cb.vtable.kernels orelse return error.PreparedMetadataAliasChanged;
            if (cb.kind() != .native or !functions.accepts(cb.vtable)) return error.PreparedMetadataAliasChanged;
            var registered = false;
            for (self.pool.backends) |backend| if (backend.ptr == cb.ptr and backend.vtable == cb.vtable) {
                registered = true;
                break;
            };
            if (!registered) return error.PreparedMetadataAliasChanged;
            const make_alias = functions.createMetadataAlias orelse return error.PreparedMetadataAliasChanged;
            const source = if (alias.source < values.len) values[alias.source] orelse return error.MissingValue else return error.MissingValue;
            const incoming = selection.binding(self.pool, alias.source, selection.inputs.entries);
            const primary = (try @import("dense_alias.zig").primary(incoming, cb.*, alias.input)) orelse return error.PreparedMetadataAliasChanged;
            const request = @import("metadata_alias.zig").request(primary, selection.planes[id][0]);
            return (try make_alias(cb.ptr, source, &request)) orelse error.PreparedMetadataAliasChanged;
        }
        return null;
    }

    pub fn execute(self: *Frame, cb: *const ops.ComputeBackend, values: []const ?CT, id: NodeId, rope_position: ?usize) !?CT {
        if (self.pool.plan.slotForNode(id) == null) return error.InactivePhysicalNode;
        if (try self.materializeLiteral(cb, id)) |literal| return literal;
        if (try self.executeMetadataAlias(cb, values, id)) |alias| return alias;
        if (id >= self.pool.kernels.len) return null;
        const kernel = if (self.pool.kernels[id]) |*value| value else return null;
        var positioned: kernels.Descriptor = undefined;
        const descriptor = if (kernel.descriptor.operation == .rope and rope_position != null) blk: {
            positioned = kernel.descriptor;
            positioned.operation.rope = try positioned.operation.rope.withPosition(rope_position.?);
            break :blk &positioned;
        } else &kernel.descriptor;
        const index = kernel.backend_index;
        const selected = self.pool.backends[index];
        if (selected.ptr != cb.ptr or selected.vtable != cb.vtable) return null;
        const functions = cb.vtable.kernels orelse return null;
        const run = functions.runAssignedKernel orelse return null;
        const view = functions.physicalTensorView orelse return null;
        var inputs: [3]CT = undefined;
        for (kernel.inputs[0..descriptor.input_count], 0..) |input, input_index| inputs[input_index] = values[input] orelse return error.MissingValue;
        const selected_route = self.pool.preparedKernelRoute(id);
        var issued_inputs: [3]?route.AssignedInput = @splat(null);
        const issue_inputs = if (selected_route) |owned| owned.vtable.host_input_constraint != null else false;
        if (issue_inputs) for (kernel.inputs[0..descriptor.input_count], 0..) |input_node, position| {
            if (self.pool.hostInputIndex(input_node)) |host_index| {
                const plan = self.pool.host_input_plans[host_index];
                const owner = self.storages[plan.allocation] orelse return error.MissingCurrentHostIndexProof;
                const authority = if (owner.slot_authority) |*slot| slot.token() else return error.MissingCurrentHostIndexProof;
                issued_inputs[position] = .{ .node = input_node, .lease_identity = @intFromPtr(owner), .authority = authority, .generation = self.host_input_generation };
            }
        };
        const request = kernels.Invocation{ .input_generation = self.host_input_generation, .assigned_inputs = if (issue_inputs) &issued_inputs else null, .prepared_route = selected_route, .descriptor = descriptor, .operator_plan = if (kernel.operator_plan) |*plan| plan else null, .inputs = inputs[0..descriptor.input_count] };
        if (!try run(cb.ptr, request)) {
            if (selected_route != null) return error.PreparedKernelBindingChanged;
            return null;
        }
        const output_storage = (try self.storageFor(cb, index, kernel.output_allocation)) orelse {
            if (selected_route != null) return error.PreparedStorageUnavailable;
            return null;
        };
        const output = try self.viewForShape(cb, output_storage, descriptor.output_shape);
        errdefer cb.free(output);
        var bound = request;
        bound.output = output;
        if (kernel.host_workspace_bytes != 0) {
            if (kernel.host_workspace_bytes > self.host_workspace.len) return error.MissingAssignedHostWorkspace;
            bound.host_workspace = self.host_workspace[0..kernel.host_workspace_bytes];
            if (!self.host_workspace_fresh) self.stats.reused_allocations += 1;
            self.host_workspace_fresh = false;
        }
        if (kernel.workspace_bytes != 0) {
            const scratch = (try self.storageFor(cb, index, self.pool.workspace_allocations[index])) orelse {
                if (selected_route != null) return error.PreparedStorageUnavailable;
                cb.free(output);
                return null;
            };
            bound.workspace = (try view(cb.ptr, .{ .storage = scratch, .shape = &.{@intCast(kernel.workspace_bytes / @sizeOf(f32))} })) orelse return error.UnsupportedPhysicalTensorView;
        }
        defer if (bound.workspace) |workspace| cb.free(workspace);
        var auxiliary_bindings: [kernels.max_auxiliaries]kernels.AuxiliaryBinding = undefined;
        var auxiliary_count: usize = 0;
        defer for (auxiliary_bindings[0..auxiliary_count]) |binding| cb.free(binding.tensor);
        for (kernel.auxiliaries) |maybe_auxiliary| {
            const auxiliary = maybe_auxiliary orelse break;
            const allocation = (try self.storageFor(cb, index, auxiliary.allocation)) orelse return error.AuxiliaryStorageUnavailable;
            const tensor = try self.viewForShape(cb, allocation, auxiliary.requirement.shape);
            auxiliary_bindings[auxiliary_count] = .{ .role = auxiliary.requirement.role, .tensor = tensor };
            auxiliary_count += 1;
        }
        bound.auxiliaries = auxiliary_bindings[0..auxiliary_count];
        var output_authority: route.AssignedOutput = undefined;
        if (selected_route) |selected_owner| {
            output_authority = try self.assignedOutput(id, output_storage);
            bound.assigned_output = &output_authority;
            const assignment = route.PrepareAssigned{ .allocator = self.pool.allocator, .invocation = &bound, .output = output_authority.slot };
            if (self.assigned[id]) |resources| {
                try resources.validate(resources.context, assignment);
            } else self.assigned[id] = try selected_owner.prepareAssigned(assignment);
            if (self.assigned[id]) |*resources| bound.prepared_resources = resources;
        }
        if (!try run(cb.ptr, bound)) return error.AssignedKernelDeclinedAfterBinding;
        self.stats.dispatches += 1;
        return output;
    }
};

test "prepared physical frame metadata allocation failures release every owner" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const allocations = [_]buffers.PhysicalAllocation{
                .{ .id = 0, .kind = .tensor, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = true },
                .{ .id = 1, .kind = .workspace, .backend = .native, .storage = .host_dense, .byte_size = 24, .first_use = 0, .last_use = 1, .reusable = true },
            };
            const ledger = try accounting.Ledger.create(allocator);
            defer ledger.release();
            const inputs = [_]ReplayInput{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}) }};
            // Metadata construction never calls this borrowed backend: leases,
            // submissions and immutable handles are all initially empty.
            const backends = [_]ops.ComputeBackend{.{ .ptr = @ptrFromInt(8), .vtable = &@import("../ops/native_compute.zig").vtable_impl }};
            var pool = Pool{
                .allocator = allocator,
                .accounting = ledger,
                .host_workspace_allocation = 1,
                .plan = .{ .allocator = allocator, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = @constCast(&allocations), .transfers = &.{}, .transfer_index = .empty },
                .kernels = &.{},
                .transfers = &.{},
                .backends = @constCast(&backends),
                .workspace_allocations = &.{},
                .replay_backend = 0,
                .replay_inputs = @constCast(&inputs),
            };
            const frame = try Frame.create(&pool);
            frame.deinit();
            const usage = ledger.snapshot();
            try std.testing.expectEqual(@as(?u64, 0), usage.host_workspace.backing_live_bytes);
            try std.testing.expectEqual(@as(?u64, 24), usage.host_workspace.backing_peak_bytes);
            try std.testing.expectEqual(@as(u64, 1), usage.host_workspace.leases_released);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

const TestHostIngress = struct {
    const native = @import("../ops/native_compute.zig");
    var fail_write: bool = false;
    allocator: std.mem.Allocator,
    compute: *anyopaque,
    slot: route.OutputSlot,
    staging: []u8,
    const test_kernels: ops.KernelBackend = blk: {
        var value = native.vtable_impl.kernels.?.*;
        value.implementation_table = null;
        value.acceptsImplementation = null;
        value.hostInputRequirements = requirement;
        value.createHostInput = create;
        break :blk value;
    };
    const table: ops.ComputeBackend.VTable = blk: {
        var value = native.vtable_impl;
        value.kernels = &test_kernels;
        break :blk value;
    };
    fn requirement(_: *anyopaque, shape: ml.graph.Shape) !?host_ingress.Requirements {
        if (shape.dtype != .f32) return null;
        const n = shape.numElements() orelse return null;
        if (n <= 0) return null;
        const bytes = try std.math.mul(usize, @intCast(n), 4);
        return .{ .bytes = bytes, .host_bytes = bytes, .host_backing_bytes = bytes, .profile = 9 };
    }
    fn create(ctx: *anyopaque, request: host_ingress.Create) !?host_ingress.Upload {
        const self = try request.allocator.create(TestHostIngress);
        errdefer request.allocator.destroy(self);
        const staging = try request.allocator.alloc(u8, request.requirements.host_bytes);
        self.* = .{ .allocator = request.allocator, .compute = ctx, .slot = request.slot, .staging = staging };
        return .{ .context = self, .implementation = request.implementation, .requirements = request.requirements, .slot = request.slot, .vtable = &upload_table };
    }
    const upload_table = host_ingress.Upload.VTable{ .write = write, .deinit = deinit };
    fn write(ctx: *anyopaque, output: CT, assigned: route.AssignedOutput, bytes: []const u8) !void {
        const self: *TestHostIngress = @ptrCast(@alignCast(ctx));
        if (self.slot.node != assigned.slot.node or self.slot.allocation != assigned.slot.allocation or self.slot.epoch != assigned.slot.epoch) return error.InvalidHostIngressAuthority;
        @memcpy(self.staging, bytes);
        if (fail_write) return error.TestHostUploadFailed;
        const backend = ops.ComputeBackend{ .ptr = self.compute, .vtable = &table };
        if (self.staging.len != 16) return error.InvalidHostIngressBinding;
        var values: [4]f32 = undefined;
        for (&values, 0..) |*value, i| value.* = std.mem.bytesToValue(f32, self.staging[i * 4 ..][0..4]);
        try backend.vtable.kernels.?.writePhysicalFloat32.?(backend.ptr, output, &values);
    }
    fn deinit(ctx: *anyopaque) void {
        const self: *TestHostIngress = @ptrCast(@alignCast(ctx));
        self.allocator.free(self.staging);
        self.allocator.destroy(self);
    }
};

test "prepared host ingress frame owner validates metadata retains outputs and unwinds every allocation failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{ .host_scratch_bytes = 32 },
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: ops.BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.current, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 32, .combined_limit_bytes = 32 }, .check_live_memory = false, .credit = &self.credit }, kind, class, bytes);
        }
    };
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            var cb = context.computeBackend();
            cb.vtable = &TestHostIngress.table;
            const backends = [_]ops.ComputeBackend{cb};
            const allocations = [_]buffers.PhysicalAllocation{.{ .id = 0, .kind = .input_binding, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = false }};
            const input_plans = [_]HostInputPlan{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}), .backend_index = 0, .allocation = 0, .requirements = .{ .bytes = 16, .host_bytes = 16, .host_backing_bytes = 16, .profile = 9 }, .epoch = 42 }};
            const ledger = try accounting.Ledger.create(a);
            defer ledger.release();
            var pool = Pool{ .allocator = a, .accounting = ledger, .plan = .{ .allocator = a, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = @constCast(&allocations), .transfers = &.{}, .transfer_index = .empty }, .kernels = &.{}, .transfers = &.{}, .backends = @constCast(&backends), .workspace_allocations = &.{}, .host_input_plans = @constCast(&input_plans) };
            var tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{4}, &.{ 1, 2, 3, 4 });
            defer tensor.deinit();
            const inputs = [_]host_ingress.Input{.{ .node_id = 0, .value = &tensor }};
            try std.testing.expect(pool.supportsHostInputs(&inputs));
            const original_dtype = tensor.dtype;
            tensor.dtype = .i32;
            try std.testing.expect(!pool.supportsHostInputs(&inputs));
            tensor.dtype = original_dtype;
            const required = try pool.storageRequirement();
            defer a.free(pool.storage_requirement_bounds);
            try std.testing.expectEqual(@as(usize, 32), required.host_scratch_bytes);
            var owner = Owner{};
            defer owner.controller.deinit();
            owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 32, .combined_limit_bytes = 32 }, required, false);
            defer {
                owner.current.release();
                std.debug.assert(owner.controller.snapshot().host_scratch_bytes == 0);
            }
            const cap = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
            const frame = try Frame.createWithAdmission(&pool, cap);
            defer frame.deinit();
            try frame.begin();
            var finished = false;
            defer if (!finished) {
                _ = frame.finish() catch {};
            };
            const bound = try frame.bindHostInputs(&inputs);
            try std.testing.expectEqual(@as(usize, 1), bound.len);
            const held = try cb.primReshape(bound[0].value, &.{ 2, 2 });
            var held_live = true;
            defer if (held_live) cb.free(held);
            frame.releaseHostInputs();
            _ = try frame.finish();
            finished = true;
            try std.testing.expect((try frame.reusableCredit(null, null)) == null);
            const values = try cb.toFloat32(held, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
            try std.testing.expectEqual(@as(?u64, 16), ledger.snapshot().host_workspace.backing_live_bytes);
            try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().host_scratch_bytes);
            try std.testing.expectEqual(@as(usize, 0), owner.current.amounts.host_scratch_bytes);
            cb.free(held);
            held_live = false;
            var refined = required;
            const warm = (try frame.reusableCredit(cap.identity, &refined)) orelse return error.MissingWarmIngressCredit;
            try std.testing.expectEqual(@as(usize, 32), warm.host_scratch_bytes);
            try std.testing.expectEqual(@as(usize, 32), refined.host_scratch_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prepared ingress cancellation and confirmed faults reuse or reclaim exact quota before retry" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const memory = @import("../runtime/tier/memory.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{},
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: ops.BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.current, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .check_live_memory = false, .credit = &self.credit }, kind, class, bytes);
        }
    };
    const Fence = struct {
        fail_wait: bool = false,
        fail_record: bool = false,
        fn record(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_record) return error.FenceRecordFailed;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_wait) return error.FenceWaitFailed;
        }
        fn before(_: *anyopaque) !void {}
        fn deinit(_: *anyopaque) void {}
        const table = storage.Completion.VTable{ .record = record, .before_reuse = before, .wait = wait, .deinit = deinit };
    };
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) return error.Cancelled;
        }
    };
    for ([_]bool{ false, true }) |fault| {
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
        defer context.deinit(a);
        var cb = context.computeBackend();
        cb.vtable = &TestHostIngress.table;
        const backends = [_]ops.ComputeBackend{cb};
        const allocations = [_]buffers.PhysicalAllocation{
            .{ .id = 0, .kind = .input_binding, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = false },
            .{ .id = 1, .kind = .input_binding, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = false },
        };
        const plans = [_]HostInputPlan{
            .{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}), .backend_index = 0, .allocation = 0, .requirements = .{ .bytes = 16, .host_bytes = 16, .host_backing_bytes = 16, .profile = 9 }, .epoch = 42 },
            .{ .node = 1, .shape = ml.graph.Shape.init(.f32, &.{4}), .backend_index = 0, .allocation = 1, .requirements = .{ .bytes = 16, .host_bytes = 16, .host_backing_bytes = 16, .profile = 9 }, .epoch = 42 },
        };
        var owner = Owner{};
        defer owner.controller.deinit();
        var lease_live = false;
        defer if (lease_live) owner.current.release();
        var fence = Fence{};
        var pool = Pool{ .allocator = a, .plan = .{ .allocator = a, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = @constCast(&allocations), .transfers = &.{}, .transfer_index = .empty }, .kernels = &.{}, .transfers = &.{}, .backends = @constCast(&backends), .workspace_allocations = &.{}, .host_input_plans = @constCast(&plans) };
        defer {
            TestHostIngress.fail_write = false;
            fence.fail_wait = false;
            fence.fail_record = false;
            for (pool.frames.items) |frame| {
                if (frame.scope_backends != 0) {
                    _ = frame.finish() catch {};
                }
                frame.deinit();
            }
            pool.frames.deinit(a);
            a.free(pool.storage_requirement_bounds);
        }
        const required = try pool.storageRequirement();
        try std.testing.expectEqual(@as(usize, 64), required.host_scratch_bytes);
        var tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{4}, &.{ 1, 2, 3, 4 });
        defer tensor.deinit();
        const inputs = [_]host_ingress.Input{ .{ .node_id = 0, .value = &tensor }, .{ .node_id = 1, .value = &tensor } };
        const cap = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
        owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, required, false);
        lease_live = true;
        owner.credit = required;
        const first = (try pool.acquireWithAdmission(cap)).?;
        try first.begin();
        if (fault) {
            TestHostIngress.fail_write = true;
            try std.testing.expectError(error.TestHostUploadFailed, first.bindHostInputs(&inputs));
            TestHostIngress.fail_write = false;
        } else {
            var cancelled = Cancel{};
            const control = @import("../execution_control.zig").InferenceExecutionControl{ .ptr = &cancelled, .check_fn = Cancel.check };
            try std.testing.expectError(error.Cancelled, first.bindHostInputsWithControl(&inputs, control));
        }
        first.completions[0] = .{ .ptr = &fence, .vtable = &Fence.table };
        _ = try first.finish();
        owner.current.release();
        lease_live = false;
        try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().host_scratch_bytes);
        var claim: ?FrameClaim = null;
        defer if (claim) |*held| held.release();
        if (fault) {
            try std.testing.expectError(error.ResourceTemporarilyUnavailable, owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, required, false));
            fence.fail_wait = true;
            try std.testing.expectError(error.FenceWaitFailed, pool.reclaimRetiredFrames());
            try std.testing.expectEqual(@as(usize, 1), pool.frames.items.len);
            try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().host_scratch_bytes);
            fence.fail_wait = false;
            try std.testing.expect(try pool.reclaimRetiredFrames());
            try std.testing.expectEqual(@as(usize, 0), owner.controller.snapshot().host_scratch_bytes);
        } else {
            try std.testing.expect(!try pool.reclaimRetiredFrames());
            claim = try pool.claimWarm(cap.identity);
            try std.testing.expectEqual(@as(usize, 32), claim.?.credit.host_scratch_bytes);
        }
        const increment = required.host_scratch_bytes - (if (claim) |held| held.credit.host_scratch_bytes else 0);
        owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .{ .host_scratch_bytes = increment }, false);
        lease_live = true;
        owner.credit = .{ .host_scratch_bytes = increment };
        const retry = (try pool.acquirePrepared(cap, if (claim) |*held| held else null)).?;
        try retry.begin();
        const bound = try retry.bindHostInputs(&inputs);
        const held_output = try cb.primReshape(bound[0].value, &.{ 2, 2 });
        var held_live = true;
        defer if (held_live) cb.free(held_output);
        retry.completions[0] = .{ .ptr = &fence, .vtable = &Fence.table };
        fence.fail_record = true;
        try std.testing.expectError(error.FenceRecordFailed, retry.finish());
        fence.fail_record = false;
        owner.current.release();
        lease_live = false;
        try std.testing.expectEqual(@as(usize, 64), owner.controller.snapshot().host_scratch_bytes);
        try std.testing.expect(try pool.reclaimRetiredFrames());
        try std.testing.expectEqual(@as(usize, 16), owner.controller.snapshot().host_scratch_bytes);
        const actual = try cb.toFloat32(held_output, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, actual);
        cb.free(held_output);
        held_live = false;
        try std.testing.expectEqual(@as(usize, 0), owner.controller.snapshot().host_scratch_bytes);
    }
}

const RegionFrameFixture = struct {
    const native = @import("../ops/native_compute.zig");
    fn policy(_: *anyopaque) !?u64 {
        return 11;
    }
    const test_kernels = blk: {
        var value = native.vtable_impl.kernels.?.*;
        value.implementation_table = null;
        value.acceptsImplementation = null;
        value.regionPolicyKey = policy;
        break :blk value;
    };
    const test_backend_table: ops.ComputeBackend.VTable = blk: {
        var value = native.vtable_impl;
        value.kernels = &test_kernels;
        break :blk value;
    };
    backend: ops.ComputeBackend,
    references: usize = 0,
    tickets: usize = 0,
    drains: usize = 0,
    dispatches: usize = 0,
    ready: bool = true,
    fail_encode: bool = false,
    fn retain(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.references += 1;
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.references != 0);
        self.references -= 1;
    }
    fn claimValidate(raw: *anyopaque, request: @import("region_route.zig").Validate) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (request.payload_claim == null or request.payload_claim.?.resource.context != @as(*anyopaque, @ptrCast(self)) or self.references == 0) return error.InvalidPreparedRegionClaim;
    }
    fn transfer(raw: *anyopaque, request: @import("region_route.zig").Submission) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (request.outcome != .completed_or_enqueued) return error.PreparedRegionCompletionUncertain;
        // Simulate the independent ticket already installed by the numerical
        // callback before its actual command-buffer submission.
        if (self.tickets == 0) return error.MissingRegionSubmissionTicket;
    }
    fn drain(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.drains += 1;
        self.complete();
    }
    fn complete(self: *@This()) void {
        while (self.tickets != 0) {
            self.tickets -= 1;
            release(self);
        }
    }
    const claim_table = @import("region_route.zig").PayloadClaim.VTable{ .validate = claimValidate, .transfer_to_submission = transfer, .drain = drain };
    fn claim(self: *@This()) @import("region_route.zig").PayloadClaim {
        retain(self);
        return .{ .key = 7, .resource = .{ .context = self, .retain_fn = retain, .release_fn = release }, .vtable = &claim_table };
    }
    fn matches(raw: *anyopaque, request: @import("region_route.zig").Validate) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (request.payload_claim) |held| return held.resource.context == @as(*anyopaque, @ptrCast(self)) and held.held;
        return self.ready;
    }
    fn validate(_: *anyopaque, _: @import("region_route.zig").Validate) !void {}
    fn dispatch(raw: *anyopaque, request: @import("region_route.zig").Invocation) !@import("region_route.zig").Outcome {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // The ticket precedes numerical work, including a deliberately failing
        // encoder. A post-dispatch transfer cannot manufacture this proof.
        retain(self);
        self.tickets += 1;
        self.dispatches += 1;
        try self.backend.vtable.kernels.?.writePhysicalFloat32.?(self.backend.ptr, request.outputs[0].tensor, &.{ 1, 2, 3, 4 });
        if (self.fail_encode) return error.TestRegionEncodeFailed;
        return .completed;
    }
    fn deinit(_: *anyopaque) void {}
    const table = @import("region_route.zig").Owned.VTable{ .matches = matches, .validate = validate, .dispatch = dispatch, .deinit = deinit };
    fn run(a: std.mem.Allocator) !void {
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
        defer context.deinit(a);
        var cb = context.computeBackend();
        cb.vtable = &test_backend_table;
        var fixture = RegionFrameFixture{ .backend = cb };
        defer {
            fixture.complete();
            std.debug.assert(fixture.references == 0);
        }
        const shape = ml.graph.Shape.init(.f32, &.{4});
        const outputs = [_]@import("region_route.zig").Output{.{ .node = 0, .logical = shape, .bytes = 16 }};
        const owner = @import("region_route.zig").Owned{ .context = &fixture, .implementation = .{ .context = cb.ptr, .table = cb.vtable }, .key = 7, .policy_key = 11, .covered_nodes = &.{0}, .input_nodes = &.{}, .entry_node = 0, .requirements = .{ .outputs = &outputs, .payload_claim_required = true, .uncovered = .{} }, .vtable = &table };
        const mappings = [_]@import("region_binding.zig").Installed.Output{.{ .slot = .{ .node = 0, .allocation = 0, .epoch = 42, .logical = shape, .writable_bytes = 16 } }};
        const regions = [_]@import("region_binding.zig").Installed{.{ .owner = &owner, .key = 7, .backend_index = 0, .epoch = 42, .outputs = &mappings, .workspace_allocation = buffers.invalid_allocation, .workspace_bytes = 0, .host_workspace_bytes = 0 }};
        const allocations = [_]buffers.PhysicalAllocation{.{ .id = 0, .kind = .tensor, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 0, .reusable = true }};
        const backends = [_]ops.ComputeBackend{cb};
        var pool = Pool{ .allocator = a, .regions = @constCast(&regions), .plan = .{ .allocator = a, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = @constCast(&allocations), .transfers = &.{}, .transfer_index = .empty }, .kernels = &.{}, .transfers = &.{}, .backends = @constCast(&backends), .workspace_allocations = &.{} };
        const frame = try Frame.create(&pool);
        defer frame.deinit();
        frame.region_claims[0] = fixture.claim();
        fixture.ready = false; // queued eviction cannot revoke admitted pins
        var bound = try frame.executeRegion(&regions[0], &.{}, &.{});
        defer bound.deinit();
        const held = try bound.take(0);
        var held_live = true;
        defer if (held_live) cb.free(held);
        _ = try frame.finish();
        try std.testing.expectEqual(@as(usize, 1), fixture.references);
        try std.testing.expectEqual(@as(usize, 0), fixture.drains);
        try std.testing.expect(frame.region_claims[0] == null);
        fixture.complete();
        const values = try cb.toFloat32(held, a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
        cb.free(held);
        held_live = false;
        // Reuse the same admitted addresses for a committed numerical failure.
        frame.in_use = true;
        frame.region_claims[0] = fixture.claim();
        fixture.fail_encode = true;
        if (frame.executeRegion(&regions[0], &.{}, &.{})) |unexpected| {
            var value = unexpected;
            value.deinit();
            return error.TestUnexpectedResult;
        } else |err| switch (err) {
            error.TestRegionEncodeFailed => {},
            else => return err,
        }
        try std.testing.expect(frame.retired and frame.region_claims[0] != null);
        _ = try frame.finish();
        try std.testing.expectEqual(@as(usize, 1), fixture.drains);
        try std.testing.expectEqual(@as(usize, 0), fixture.references);
        try std.testing.expectEqual(@as(usize, 2), fixture.dispatches);
    }
};

test "frozen region frame pins transfer to completion and uncertain errors drain without replay" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try RegionFrameFixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, RegionFrameFixture.run, .{});
}

test "shared host arena shrinks obsolete kernels while preserving real region preparation scratch" {
    const a = std.testing.allocator;
    const region = @import("region_route.zig");
    const binding = @import("region_binding.zig");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    var cb = context.computeBackend();
    cb.vtable = &RegionFrameFixture.test_backend_table;
    const Consumer = struct {
        backend: ops.ComputeBackend,
        fn matches(_: *anyopaque, _: region.Validate) bool {
            return true;
        }
        fn validate(_: *anyopaque, _: region.Validate) !void {}
        fn prepare(_: *anyopaque, invocation: region.Invocation) !?region.AssignedResources {
            try std.testing.expectEqual(@as(usize, 24), invocation.host_workspace.len);
            invocation.host_workspace[23] = 42;
            invocation.host_workspace[15] = invocation.host_workspace[23];
            return null;
        }
        fn dispatch(raw: *anyopaque, invocation: region.Invocation) !region.Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u8, 42), invocation.host_workspace[15]);
            const seed: f32 = @floatFromInt(invocation.host_workspace[15]);
            try self.backend.vtable.kernels.?.writePhysicalFloat32.?(self.backend.ptr, invocation.outputs[0].tensor, &.{ seed, seed + 1, seed + 2, seed + 3 });
            return .completed;
        }
        fn deinit(_: *anyopaque) void {}
        const functions = region.Owned.VTable{ .matches = matches, .validate = validate, .prepare_assigned = prepare, .dispatch = dispatch, .deinit = deinit };
    };
    var consumer = Consumer{ .backend = cb };
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const outputs = [_]region.Output{.{ .node = 0, .logical = shape, .bytes = 16 }};
    const owner = region.Owned{ .context = &consumer, .implementation = .{ .context = cb.ptr, .table = cb.vtable }, .key = 7, .policy_key = 11, .covered_nodes = &.{0}, .input_nodes = &.{}, .entry_node = 0, .requirements = .{ .outputs = &outputs, .dispatch_workspace = .{ .host_byte_size = 16 }, .preparation_workspace = .{ .host_byte_size = 24 }, .uncovered = .{} }, .vtable = &Consumer.functions };
    const mapping = [_]binding.Installed.Output{.{ .slot = .{ .node = 0, .allocation = 0, .epoch = 42, .logical = shape, .writable_bytes = 16 } }};
    var regions = [_]binding.Installed{.{ .owner = &owner, .key = 7, .backend_index = 0, .epoch = 42, .outputs = &mapping, .workspace_allocation = buffers.invalid_allocation, .workspace_bytes = 0, .host_workspace_bytes = 24 }};
    var allocations = [_]buffers.PhysicalAllocation{ .{ .id = 0, .kind = .tensor, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = false }, .{ .id = 1, .kind = .workspace, .backend = .native, .storage = .host_f32, .byte_size = 128, .first_use = 0, .last_use = 1, .reusable = false } };
    const descriptor = kernels.Descriptor{ .operation = .{ .unary = .negate }, .output_shape = shape, .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 };
    var plans = [_]?KernelPlan{ .{ .descriptor = descriptor, .inputs = @splat(ml.graph.null_node), .backend_index = 0, .output_allocation = 0, .workspace_bytes = 0, .host_workspace_bytes = 80 }, .{ .descriptor = descriptor, .inputs = @splat(ml.graph.null_node), .backend_index = 0, .output_allocation = 0, .workspace_bytes = 0, .host_workspace_bytes = 16 } };
    var nodes = [_]u32{ 0, std.math.maxInt(u32) };
    var backends = [_]ops.ComputeBackend{cb};
    var pool = Pool{ .allocator = a, .owns_plan = true, .regions = &regions, .region_by_node = &nodes, .plan = .{ .allocator = a, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = &allocations, .transfers = &.{}, .transfer_index = .empty }, .kernels = &plans, .transfers = &.{}, .backends = &backends, .workspace_allocations = &.{}, .host_workspace_allocation = 1 };
    try pool.recomputeHostWorkspace();
    try std.testing.expectEqual(@as(u64, 24), pool.plan.allocations[1].byte_size);
    const frame = try Frame.create(&pool);
    defer frame.deinit();
    try std.testing.expectEqual(@as(usize, 24), frame.host_workspace.len);
    var result = try frame.executeRegion(&regions[0], &.{}, &.{});
    defer result.deinit();
    const values = try cb.toFloat32(result.outputs[0].tensor, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 42, 43, 44, 45 }, values);
    _ = try frame.finish();
}

test "Native frame private input metadata survives warm release and blocks same-header escape" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const required = (try backend.vtable.kernels.?.hostInputRequirements.?(backend.ptr, shape)).?;
    const allocations = [_]buffers.PhysicalAllocation{.{ .id = 0, .kind = .input_binding, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = false }};
    var backends = [_]ops.ComputeBackend{backend};
    const plans = [_]HostInputPlan{.{ .node = 0, .shape = shape, .backend_index = 0, .allocation = 0, .requirements = required, .epoch = 41 }};
    var pool = Pool{ .allocator = a, .plan = .{ .allocator = a, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = @constCast(&allocations), .transfers = &.{}, .transfer_index = .empty }, .kernels = &.{}, .transfers = &.{}, .backends = &backends, .workspace_allocations = &.{}, .host_input_plans = @constCast(&plans), .payload_coverage = .{ .complete = .{ .epoch = 41 } } };
    const frame = try Frame.create(&pool);
    defer frame.deinit();
    var data = [_]f32{ 1, 2, 3, 4 };
    const tensor = @import("../backends/tensor.zig").Tensor{ .data = std.mem.sliceAsBytes(&data), .dtype = .f32, .shape = &.{4}, .name = "", .allocator = a, .owns_data = false, .owns_shape = false };
    const inputs = [_]host_ingress.Input{.{ .node_id = 0, .value = &tensor }};
    try frame.begin();
    var active = true;
    defer if (active) {
        _ = frame.finish() catch unreachable;
    };
    const first = (try frame.bindHostInputs(&inputs))[0].value;
    _ = try frame.finish();
    active = false;
    try std.testing.expect(frame.host_input_resources[0].?.private_view.? == first);
    try std.testing.expect((try frame.reusableCredit(null, null)) != null);
    const exclusive = backend.vtable.kernels.?.hostInputViewExclusive.?;
    try std.testing.expect(exclusive(backend.ptr, first));
    // A metadata alias retains the private header, adding no Lease reference: the private-header
    // ownership query, not merely the bank count, must reject this escape.
    const escaped = try backend.primReshape(first, &.{ 2, 2 });
    try std.testing.expect(!exclusive(backend.ptr, first));
    try std.testing.expect((try frame.reusableCredit(null, null)) == null);
    backend.free(escaped);
    // Model the pool's successful exclusive acquisition before entering the
    // provider scope again. Reuse checks above run only after real completion.
    frame.in_use = true;
    try frame.begin();
    active = true;
    data[0] = 9;
    const second = (try frame.bindHostInputs(&inputs))[0].value;
    try std.testing.expect(first == second);
    _ = try frame.finish();
    active = false;
    try std.testing.expect((try frame.reusableCredit(null, null)) != null);
}

test "Native borrowing freezes only complete non-alias programs and rejects direct side channels before frame creation" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    inline for (.{ false, true }) |alias_output| {
        var graph = Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const shape = ml.graph.Shape.init(.f32, &.{4});
        const input_node = try builder.parameter("x", shape);
        const negative = try builder.neg(input_node);
        const output_node = if (alias_output) try builder.reshape(negative, ml.graph.Shape.init(.f32, &.{ 2, 2 })) else negative;
        try graph.markOutput(output_node);
        var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
        defer analysis.deinit(a);
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
        defer context.deinit(a);
        const backend = context.computeBackend();
        var executable = try @import("executable.zig").prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{});
        defer executable.deinit();
        const root = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
        const facts = [_]route.InputBinding{.{ .node_id = input_node, .value = .{ .imported_host = .{ .logical = shape, .target = .native, .owned_ingress = true, .borrowable_host = true } } }};
        const selected = (try root.prepareRoutes(&facts)) orelse return error.MissingPreparedRoutes;
        defer selected.release();
        try std.testing.expect(selected.pool.completeOwnedPayload() != null);
        try std.testing.expectEqual(!alias_output, selected.pool.host_input_plans[0].borrowed);
        const requirement = try selected.pool.storageRequirement();
        try std.testing.expectEqual(@as(usize, if (alias_output) 32 else 16), requirement.host_scratch_bytes);
        var values = [_]f32{ 1, 2, 3, 4 };
        const input = @import("../backends/tensor.zig").Tensor{ .data = std.mem.sliceAsBytes(&values), .shape = &.{4}, .dtype = .f32, .name = "", .allocator = a, .owns_data = false, .owns_shape = false };
        const inputs = [_]host_ingress.Input{.{ .node_id = input_node, .value = &input }};
        if (!alias_output) {
            try std.testing.expectError(error.InvalidHostIngressBinding, executable.run(a, .{ .prepared_routes = selected, .host_inputs = &inputs, .donate = &.{false} }));
            try std.testing.expectError(error.BorrowedHostInputSideChannels, executable.run(a, .{ .prepared_routes = selected, .host_inputs = &inputs, .embedding_ids = &.{0} }));
            try std.testing.expectEqual(@as(usize, 0), selected.pool.frames.items.len);
        }
        var output = try executable.run(a, .{ .prepared_routes = selected, .host_inputs = &inputs });
        defer output.deinit(&executable.runtime);
        const actual = try backend.toFloat32(output.outputs[0], a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, actual);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, &values);
    }
}

fn frameAdmissionCallbackCase(allocator: std.mem.Allocator, comptime inject_failure: bool) !void {
    const memory = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        pool: *Pool,
        source: *memory.AdmissionLease,
        controller: *memory.AdmissionController,
        credit: memory.AdmissionAmounts = .{ .host_scratch_bytes = 8 },
        calls: usize = 0,
        fail: bool = false,
        fn reserve(raw: *anyopaque, a: std.mem.Allocator, kind: ops.BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.pool.mutex.tryLock()) return error.AdmissionCallbackUnderPoolMutex;
            self.pool.mutex.unlock();
            // The outer frame or its pending cold slot is already exclusive.
            // Reentry must terminate without stealing that frame/cache slot.
            if (try self.pool.acquire()) |_| return error.UnreservedFramePublication;
            self.calls += 1;
            if (self.fail) return error.InjectedFrameAdmissionFailure;
            return admission.fromRunLease(a, self.source, .{ .controller = self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 8 }, .check_live_memory = false, .credit = &self.credit }, kind, class, bytes);
        }
    };
    for ([_]bool{ false, true }) |warm| {
        var controller = memory.AdmissionController{};
        defer controller.deinit();
        var source = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 8 }, false);
        defer source.release();
        {
            const allocations = [_]buffers.PhysicalAllocation{.{ .id = 0, .kind = .workspace, .backend = .native, .storage = .host_f32, .byte_size = 8, .first_use = 0, .last_use = 1, .reusable = false }};
            var pool = Pool{ .allocator = allocator, .plan = .{ .allocator = allocator, .slots = &.{}, .node_slots = &.{}, .node_positions = &.{}, .allocations = @constCast(&allocations), .transfers = &.{}, .transfer_index = .empty }, .kernels = &.{}, .transfers = &.{}, .backends = &.{}, .workspace_allocations = &.{}, .host_workspace_allocation = 0, .max_frames = 1 };
            defer {
                for (pool.frames.items) |frame| frame.deinit();
                pool.frames.deinit(allocator);
            }
            if (warm) {
                const initial = (try pool.acquire()) orelse return error.MissingInitialFrame;
                _ = try initial.finish();
            }
            var probe = Probe{ .pool = &pool, .source = &source, .controller = &controller, .fail = inject_failure };
            const capability = admission.Capability{ .identity = .{ .controller = &controller, .backend_class = .cpu }, .context = &probe, .services = &.{ .reserve = Probe.reserve } };
            if (inject_failure) {
                try std.testing.expectError(error.InjectedFrameAdmissionFailure, pool.acquireWithAdmission(capability));
                try std.testing.expectEqual(@as(usize, 0), pool.pending_frames);
                try std.testing.expectEqual(@as(usize, @intFromBool(warm)), pool.frames.items.len);
                if (warm) try std.testing.expect(!pool.frames.items[0].in_use and pool.frames.items[0].owner_pin == null);
            }
            probe.fail = false;
            const frame = (try pool.acquireWithAdmission(capability)) orelse return error.MissingAdmittedFrame;
            try std.testing.expect(frame.in_use);
            try std.testing.expectEqual(@as(usize, 0), pool.pending_frames);
            try std.testing.expectEqual(@as(usize, if (inject_failure) 2 else 1), probe.calls);
            _ = try frame.finish();
        }
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    }
}

fn frameAdmissionCallbackOomCase(allocator: std.mem.Allocator) !void {
    try frameAdmissionCallbackCase(allocator, false);
}

test "physical frame cold and warm admission callbacks run unlocked with exclusive slots and failure atomic rollback" {
    try frameAdmissionCallbackCase(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, frameAdmissionCallbackOomCase, .{});
}
