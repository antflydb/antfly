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

//! Physical-plan storage shared by native and CUDA assigned-output kernels.
//! Frames are reusable only after their values have released every lease, and
//! device submission dependencies are established before an address is reused.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const buffers = @import("../graph/buffer_plan.zig");
const storage = ops.physical_storage;
const kernels = ops.kernel_descriptor;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const CT = ops.CT;
const interpreter = @import("../graph/interpreter.zig");
const Binding = @import("program.zig").Binding;
const ReplayDecision = @import("stats.zig").PhysicalReplayDecision;

pub const Stats = struct {
    replay_decision: ReplayDecision = .not_requested,
    dispatches: u64 = 0,
    transfers: u64 = 0,
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
    auxiliaries: [kernels.max_auxiliaries]?AuxiliaryPlan = @splat(null),
};

const ReplayInput = struct { node: NodeId, shape: ml.graph.Shape, allocation: ?buffers.AllocationId = null, constant: ?[]const f32 = null, constant_index: ?usize = null };
const ReplayOutput = struct { allocation: buffers.AllocationId, shape: ml.graph.Shape };
const SharedLiteral = struct { lease: ?*storage.Lease = null, value: ?CT = null };
const ReplayBindingState = struct { weight: ?CT = null, supplied: bool = false };

const TransferPlan = struct { shape: ml.graph.Shape, backend_index: usize, allocation: buffers.AllocationId };

pub const Pool = struct {
    allocator: std.mem.Allocator,
    /// Borrow the plan's heap-backed slices, never its movable struct address.
    plan: buffers.BufferPlan,
    kernels: []?KernelPlan,
    transfers: []?TransferPlan,
    backends: []ops.ComputeBackend,
    workspace_allocations: []buffers.AllocationId,
    frames: std.ArrayListUnmanaged(*Frame) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    max_frames: usize = 4,
    replay_inputs: []ReplayInput = &.{},
    replay_input_indices: []u32 = &.{},
    shared_constants: []SharedLiteral = &.{},
    shared_constant_completion: ?storage.Completion = null,
    replay_outputs: []ReplayOutput = &.{},
    replay_kernel_count: usize = 0,
    replay_backend: ?usize = null,
    replay_decision: ReplayDecision = .unsupported_backend,

    pub fn create(allocator: std.mem.Allocator, graph: *const Graph, plan: *buffers.BufferPlan, backends: []const *const ops.ComputeBackend, operator_plans: []const ?ops.OperatorPlan) !?*Pool {
        const prepared = try allocator.alloc(?KernelPlan, graph.nodeCount());
        errdefer allocator.free(prepared);
        @memset(prepared, null);
        const scratch_requirements = try allocator.alloc(kernels.WorkspaceRequirement, backends.len);
        defer allocator.free(scratch_requirements);
        @memset(scratch_requirements, .{});
        var count: usize = 0;
        for (prepared, 0..) |*entry, id| {
            const slot = plan.slotForNode(@intCast(id)) orelse continue;
            if (slot.kind != .allocation or slot.allocation == buffers.invalid_allocation or (slot.backend != .native and slot.backend != .cuda)) continue;
            const descriptor = kernels.lower(graph, @intCast(id)) orelse continue;
            for (backends, 0..) |backend, backend_index| {
                if (backend.kind() != slot.backend) continue;
                const functions = backend.vtable.kernels orelse continue;
                if (functions.runAssignedKernel == null or functions.physicalTensorView == null) continue;
                const workspace = if (backend.vtable.kernels.?.assignedKernelWorkspace) |requirement| try requirement(backend.ptr, &descriptor) else kernels.WorkspaceRequirement{};
                if (workspace.byte_size % @sizeOf(f32) != 0) return error.InvalidWorkspaceSize;
                scratch_requirements[backend_index].byte_size = @max(scratch_requirements[backend_index].byte_size, workspace.byte_size);
                scratch_requirements[backend_index].alignment = @max(scratch_requirements[backend_index].alignment, workspace.alignment);
                var inputs: [3]NodeId = @splat(ml.graph.null_node);
                @memcpy(inputs[0..descriptor.input_count], graph.node(@intCast(id)).getInputs());
                entry.* = .{ .descriptor = descriptor, .operator_plan = if (id < operator_plans.len) operator_plans[id] else null, .inputs = inputs, .backend_index = backend_index, .output_allocation = slot.allocation, .workspace_bytes = workspace.byte_size };
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
        if (count == 0) {
            allocator.free(prepared);
            allocator.free(transfers);
            return null;
        }
        const owned_backends = try allocator.alloc(ops.ComputeBackend, backends.len);
        errdefer allocator.free(owned_backends);
        for (backends, owned_backends) |backend, *owned| owned.* = backend.*;
        const workspace_ids = try allocator.alloc(buffers.AllocationId, backends.len);
        errdefer allocator.free(workspace_ids);
        @memset(workspace_ids, buffers.invalid_allocation);
        for (owned_backends, scratch_requirements, 0..) |backend, requirement, index| {
            if (requirement.byte_size != 0) workspace_ids[index] = try plan.addWorkspace(backend.kind(), requirement.byte_size, requirement.alignment);
        }
        const self = try allocator.create(Pool);
        self.* = .{ .allocator = allocator, .plan = plan.*, .kernels = prepared, .transfers = transfers, .backends = owned_backends, .workspace_allocations = workspace_ids };
        return self;
    }

    /// Admit complete assigned-output CUDA graphs with explicit parameter
    /// roles. Graphs with unknown bindings, unsupported literals or partial
    /// descriptor coverage keep their existing execution path.
    pub fn prepareReplay(self: *Pool, graph: *const Graph, plan: *buffers.BufferPlan, bindings: []const Binding, costs: @import("placement.zig").RegionCosts) !void {
        std.debug.assert(self.frames.items.len == 0);
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
        var constant_count: usize = 0;
        for (graph.nodes.items, 0..) |node, id| {
            if (node.op == .constant) {
                const attrs = node.op.constant;
                const elements = node.output_shape.numElements() orelse return;
                if (node.output_shape.dtype != .f32 or elements <= 0 or attrs.data_len != elements or self.backends[index].vtable.kernels.?.writePhysicalFloat32 == null) return;
                const bytes = try std.math.mul(usize, attrs.data_len, @sizeOf(f32));
                if (attrs.data_offset > graph.constant_pool.items.len or bytes > graph.constant_pool.items.len - attrs.data_offset) return error.InvalidShape;
                binding_count += 1;
                constant_count += 1;
                continue;
            }
            if (node.op == .parameter) {
                binding_count += 1;
                if (node.output_shape.dtype != .f32) return;
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
                    input_count += 1;
                    input_bytes = try std.math.add(u64, input_bytes, try std.math.mul(u64, @intCast(elements), @sizeOf(f32)));
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
            if (kernel.backend_index != index or !self.backends[index].vtable.kernels.?.physicalReplayKernelSupported.?(self.backends[index].ptr, &kernel.descriptor)) return;
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
        const constants = try self.allocator.alloc(SharedLiteral, constant_count);
        errdefer self.allocator.free(constants);
        @memset(constants, .{});
        // Allocation IDs are appended before any frame borrows the plan.
        // Refresh the borrowed slice even on an allocation failure.
        defer self.plan = plan.*;
        var input_index: usize = 0;
        var constant_index: usize = 0;
        for (graph.nodes.items, 0..) |node, id| {
            if (node.op != .parameter and node.op != .constant) continue;
            const input = &inputs[input_index];
            input_indices[id] = @intCast(input_index);
            input_index += 1;
            input.* = .{ .node = @intCast(id), .shape = node.output_shape };
            if (node.op == .constant) {
                const attrs = node.op.constant;
                input.constant = graph.constantData(attrs.data_offset, attrs.data_len);
                input.constant_index = constant_index;
                constant_index += 1;
                input.allocation = try plan.addConstantBinding(.cuda, try std.math.mul(usize, attrs.data_len, @sizeOf(f32)));
                continue;
            }
            for (bindings) |binding| if (binding.node_id == id and (binding.kind == .input or binding.kind == .state_input)) {
                input.allocation = try plan.addInputBinding(.cuda, try std.math.mul(usize, @intCast(input.shape.numElements().?), @sizeOf(f32)));
                break;
            };
        }
        for (graph.outputs.items, outputs) |id, *output| {
            const source = replayOutputSource(graph, id).?;
            const allocation = if (self.kernels[source]) |kernel| kernel.output_allocation else inputs[input_indices[source]].allocation orelse return error.InvalidShape;
            output.* = .{ .allocation = allocation, .shape = graph.node(id).output_shape };
        }
        self.replay_inputs = inputs;
        self.replay_input_indices = input_indices;
        self.shared_constants = constants;
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

    pub fn deinit(self: *Pool) void {
        for (self.frames.items) |frame| frame.deinit();
        if (self.shared_constant_completion) |completion| {
            completion.wait() catch {};
            completion.deinit();
        }
        for (self.shared_constants) |literal| if (literal.lease) |lease| {
            if (literal.value) |value| lease.backend.free(value);
            lease.release();
        };
        self.allocator.free(self.shared_constants);
        self.frames.deinit(self.allocator);
        self.allocator.free(self.replay_inputs);
        self.allocator.free(self.replay_input_indices);
        self.allocator.free(self.replay_outputs);
        self.allocator.free(self.kernels);
        self.allocator.free(self.transfers);
        self.allocator.free(self.workspace_allocations);
        self.allocator.free(self.backends);
        self.allocator.destroy(self);
    }

    /// Busy published outputs pin their frame. Bound retained storage; callers
    /// may use the observable compatibility fallback when all frames are pinned.
    pub fn acquire(self: *Pool) !?*Frame {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.frames.items) |frame| {
            if (frame.in_use or frame.retired) continue;
            var unique = true;
            for (frame.storages, 0..) |lease, id| if (lease) |value| {
                if (self.plan.allocations[id].kind == .constant_binding) continue;
                const capture_pin: usize = if (frame.replay_ready) @intFromBool(frame.capture_pins[id]) else 0;
                if (value.references.load(.acquire) != 1 + capture_pin) {
                    unique = false;
                    break;
                }
            };
            if (!unique) continue;
            for (frame.completions) |completion| if (completion) |value| value.beforeReuse() catch |err| {
                frame.retired = true;
                return err;
            };
            frame.in_use = true;
            frame.stats = .{ .replay_decision = self.replay_decision };
            return frame;
        }
        if (self.frames.items.len >= self.max_frames) return null;
        const frame = try Frame.create(self);
        errdefer frame.deinit();
        try self.frames.append(self.allocator, frame);
        return frame;
    }
};

pub const Frame = struct {
    pool: *Pool,
    storages: []?*storage.Lease,
    completions: []?storage.Completion,
    in_use: bool = true,
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

    fn create(pool: *Pool) !*Frame {
        const leases = try pool.allocator.alloc(?*storage.Lease, pool.plan.allocations.len);
        errdefer pool.allocator.free(leases);
        @memset(leases, null);
        const completions = try pool.allocator.alloc(?storage.Completion, pool.backends.len);
        errdefer pool.allocator.free(completions);
        @memset(completions, null);
        const bound_inputs = try pool.allocator.alloc(interpreter.RuntimeInput, pool.replay_inputs.len);
        errdefer pool.allocator.free(bound_inputs);
        const binding_states = try pool.allocator.alloc(ReplayBindingState, pool.replay_inputs.len);
        errdefer pool.allocator.free(binding_states);
        @memset(binding_states, .{});
        const capture_pins = try pool.allocator.alloc(bool, if (pool.replay_backend != null) leases.len else 0);
        errdefer pool.allocator.free(capture_pins);
        @memset(capture_pins, false);
        const self = try pool.allocator.create(Frame);
        self.* = .{ .capture_pins = capture_pins, .pool = pool, .storages = leases, .completions = completions, .bound_inputs = bound_inputs, .binding_states = binding_states, .stats = .{ .replay_decision = pool.replay_decision } };
        return self;
    }
    fn deinit(self: *Frame) void {
        // Eviction is a cold boundary. Complete submissions before retiring
        // frame addresses, including views which outlive this frame owner.
        for (self.completions) |completion| if (completion) |value| value.wait() catch {};
        for (self.completions) |completion| if (completion) |value| value.deinit();
        if (self.replay) |replay| replay.deinit();
        if (self.pool.replay_backend) |index| for (self.binding_states) |state| if (state.weight) |value| self.pool.backends[index].free(value);
        self.pool.allocator.free(self.binding_states);
        self.pool.allocator.free(self.capture_pins);
        self.pool.allocator.free(self.bound_inputs);
        for (self.storages, 0..) |lease, id| if (lease) |value| {
            // Literal addresses are borrowed from the program-level owner.
            if (self.pool.plan.allocations[id].kind != .constant_binding) value.release();
        };
        self.pool.allocator.free(self.completions);
        self.pool.allocator.free(self.storages);
        self.pool.allocator.destroy(self);
    }
    pub fn begin(self: *Frame) !void {
        std.debug.assert(self.scope_backends == 0);
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
        @import("antfly_platform").sync.lockYielding(&self.pool.mutex);
        defer self.pool.mutex.unlock();
        defer self.in_use = false;
        defer self.endScope();
        for (self.completions) |completion| if (completion) |value| value.record() catch |err| {
            // An uncertain completion must never authorize address reuse.
            self.retired = true;
            return err;
        };
        return self.stats;
    }

    /// Bind mutable values by device copy before recording or replaying.
    /// Immutable handles are retained separately and must keep their identity.
    pub fn bindReplayInputs(self: *Frame, options: interpreter.ExecuteOptions) !?[]const interpreter.RuntimeInput {
        const index = self.pool.replay_backend orelse return null;
        if (self.replay_disabled) |reason| return self.declineReplay(reason);
        if (options.execution_control != null) return self.declineReplay(.controlled_request);
        if (options.attention != null or options.embedding_ids != null or options.sdpa_mask != null or options.cross_attention_mask != null or options.donate != null or options.deferred_input != null) return self.declineReplay(.side_channels);
        const inputs = options.runtime_inputs orelse return self.declineReplay(.incomplete_bindings);
        if (inputs.len > self.bound_inputs.len) return self.declineReplay(.incomplete_bindings);
        const cb = &self.pool.backends[index];
        // A prepared node->binding table keeps matching linear in the number
        // of public inputs, independent of initializer/constant count.
        for (self.binding_states) |*state| state.supplied = false;
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
        for (self.pool.replay_inputs[0..self.bound_input_count], self.bound_inputs[0..self.bound_input_count]) |binding, input| if (binding.allocation != null and binding.constant == null) cb.free(input.value);
        self.bound_input_count = 0;
    }

    pub fn beginReplayCapture(self: *Frame) !bool {
        if (!self.warmed or self.replay_ready) return false;
        const index = self.pool.replay_backend orelse return false;
        const backend = self.pool.backends[index];
        for (self.pool.kernels) |maybe| if (maybe) |kernel| {
            if (!backend.vtable.kernels.?.physicalReplayKernelSupported.?(backend.ptr, &kernel.descriptor)) {
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
        const index = self.pool.replay_backend orelse return error.InvalidShape;
        const cb = &self.pool.backends[index];
        const outputs = try allocator.alloc(CT, self.pool.replay_outputs.len);
        errdefer allocator.free(outputs);
        var initialized: usize = 0;
        errdefer for (outputs[0..initialized]) |output| cb.free(output);
        for (self.pool.replay_outputs, outputs) |binding, *output| {
            const allocation = self.storages[binding.allocation] orelse return error.InvalidShape;
            output.* = try self.viewForShape(cb, allocation, binding.shape);
            initialized += 1;
        }
        try self.replay.?.launch();
        self.stats.graph_replays += 1;
        self.stats.replay_decision = .replayed;
        return outputs;
    }

    fn valueForLiteral(self: *Frame, cb: *const ops.ComputeBackend, backend_index: usize, binding: ReplayInput) !CT {
        const id = binding.allocation.?;
        if (self.storages[id] != null) {
            self.stats.reused_allocations += 1;
            return self.pool.shared_constants[binding.constant_index.?].value.?;
        }
        @import("antfly_platform").sync.lockYielding(&self.pool.mutex);
        defer self.pool.mutex.unlock();
        const functions = cb.vtable.kernels.?;
        if (self.pool.shared_constant_completion == null) {
            if (functions.createPhysicalCompletion) |make_completion| self.pool.shared_constant_completion = try make_completion(cb.ptr);
        }
        if (self.pool.shared_constant_completion) |completion| try completion.beforeReuse();
        const owner = &self.pool.shared_constants[binding.constant_index.?];
        if (owner.lease == null) {
            const bytes: usize = @intCast(self.pool.plan.allocations[id].byte_size);
            const lease = (try storage.Lease.create(self.pool.allocator, cb.*, bytes)) orelse return error.InvalidShape;
            errdefer lease.release();
            const view = try self.viewForShape(cb, lease, binding.shape);
            errdefer cb.free(view);
            try functions.writePhysicalFloat32.?(cb.ptr, view, binding.constant.?);
            if (self.pool.shared_constant_completion) |completion| try completion.record();
            owner.* = .{ .lease = lease, .value = view };
            self.stats.allocations += 1;
            self.stats.allocated_bytes += bytes;
        } else self.stats.reused_allocations += 1;
        if (self.completions[backend_index] == null) {
            if (functions.createPhysicalCompletion) |make_completion| self.completions[backend_index] = try make_completion(cb.ptr);
        }
        // Frames borrow this immutable address. Pool retirement destroys all
        // captures before releasing its sole allocation owner.
        self.storages[id] = owner.lease;
        return owner.value.?;
    }

    fn storageFor(self: *Frame, cb: *const ops.ComputeBackend, backend_index: usize, allocation_id: buffers.AllocationId) !?*storage.Lease {
        if (self.storages[allocation_id] == null) {
            if (self.completions[backend_index] == null) {
                if (cb.vtable.kernels.?.createPhysicalCompletion) |make_completion| self.completions[backend_index] = try make_completion(cb.ptr);
            }
            const bytes: usize = @intCast(self.pool.plan.allocations[allocation_id].byte_size);
            self.storages[allocation_id] = (try storage.Lease.create(self.pool.allocator, cb.*, bytes)) orelse return null;
            self.stats.allocations += 1;
            self.stats.allocated_bytes += bytes;
        } else self.stats.reused_allocations += 1;
        return self.storages[allocation_id];
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

    pub fn execute(self: *Frame, cb: *const ops.ComputeBackend, values: []const ?CT, id: NodeId, rope_position: ?usize) !?CT {
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
        const request = kernels.Invocation{ .descriptor = descriptor, .operator_plan = if (kernel.operator_plan) |*plan| plan else null, .inputs = inputs[0..descriptor.input_count] };
        if (!try run(cb.ptr, request)) return null;
        const output_storage = (try self.storageFor(cb, index, kernel.output_allocation)) orelse return null;
        var shape: [ml.graph.shape.max_rank]i32 = undefined;
        for (descriptor.output_shape.dims[0..descriptor.output_shape.rank()], 0..) |dim, axis| shape[axis] = @intCast(dim);
        const output = (try view(cb.ptr, .{ .storage = output_storage, .shape = shape[0..descriptor.output_shape.rank()] })) orelse return error.UnsupportedPhysicalTensorView;
        errdefer cb.free(output);
        var bound = request;
        bound.output = output;
        if (kernel.workspace_bytes != 0) {
            const scratch = (try self.storageFor(cb, index, self.pool.workspace_allocations[index])) orelse {
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
        if (!try run(cb.ptr, bound)) return error.AssignedKernelDeclinedAfterBinding;
        self.stats.dispatches += 1;
        return output;
    }
};

test "prepared physical frame metadata allocation failures release every owner" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const allocations = [_]buffers.PhysicalAllocation{.{ .id = 0, .kind = .tensor, .backend = .native, .storage = .host_f32, .byte_size = 16, .first_use = 0, .last_use = 1, .reusable = true }};
            const inputs = [_]ReplayInput{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}) }};
            // Metadata construction never calls this borrowed backend: leases,
            // submissions and immutable handles are all initially empty.
            const backends = [_]ops.ComputeBackend{.{ .ptr = @ptrFromInt(8), .vtable = &@import("../ops/native_compute.zig").vtable_impl }};
            var pool = Pool{
                .allocator = allocator,
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
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
