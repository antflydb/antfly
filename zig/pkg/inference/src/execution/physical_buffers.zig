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

pub const Stats = struct {
    dispatches: u64 = 0,
    transfers: u64 = 0,
    transfer_bytes: u64 = 0,
    allocations: u64 = 0,
    allocated_bytes: u64 = 0,
    reused_allocations: u64 = 0,
};

pub const KernelPlan = struct {
    descriptor: kernels.Descriptor,
    inputs: [3]NodeId,
    backend_index: usize,
    output_allocation: buffers.AllocationId,
    workspace_bytes: usize,
};

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

    pub fn create(allocator: std.mem.Allocator, graph: *const Graph, plan: *buffers.BufferPlan, backends: []const *const ops.ComputeBackend) !?*Pool {
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
                entry.* = .{ .descriptor = descriptor, .inputs = inputs, .backend_index = backend_index, .output_allocation = slot.allocation, .workspace_bytes = workspace.byte_size };
                count += 1;
                break;
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

    pub fn deinit(self: *Pool) void {
        for (self.frames.items) |frame| frame.deinit();
        self.frames.deinit(self.allocator);
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
            for (frame.storages) |lease| if (lease) |value| {
                if (!value.unique()) {
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
            frame.stats = .{};
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

    fn create(pool: *Pool) !*Frame {
        const leases = try pool.allocator.alloc(?*storage.Lease, pool.plan.allocations.len);
        errdefer pool.allocator.free(leases);
        @memset(leases, null);
        const completions = try pool.allocator.alloc(?storage.Completion, pool.backends.len);
        errdefer pool.allocator.free(completions);
        @memset(completions, null);
        const self = try pool.allocator.create(Frame);
        self.* = .{ .pool = pool, .storages = leases, .completions = completions };
        return self;
    }
    fn deinit(self: *Frame) void {
        // Eviction is a cold boundary. Complete submissions before retiring
        // frame addresses, including views which outlive this frame owner.
        for (self.completions) |completion| if (completion) |value| value.wait() catch {};
        for (self.completions) |completion| if (completion) |value| value.deinit();
        for (self.storages) |lease| if (lease) |value| value.release();
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
        return (try target.vtable.kernels.?.physicalTensorView.?(target.ptr, .{ .storage = allocation, .shape = shape[0..logical.rank()] })) orelse error.UnsupportedPhysicalTensorView;
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

    pub fn execute(self: *Frame, cb: *const ops.ComputeBackend, values: []const ?CT, id: NodeId) !?CT {
        if (id >= self.pool.kernels.len) return null;
        const kernel = if (self.pool.kernels[id]) |*value| value else return null;
        const descriptor = &kernel.descriptor;
        const index = kernel.backend_index;
        const selected = self.pool.backends[index];
        if (selected.ptr != cb.ptr or selected.vtable != cb.vtable) return null;
        const functions = cb.vtable.kernels orelse return null;
        const run = functions.runAssignedKernel orelse return null;
        const view = functions.physicalTensorView orelse return null;
        var inputs: [3]CT = undefined;
        for (kernel.inputs[0..descriptor.input_count], 0..) |input, input_index| inputs[input_index] = values[input] orelse return error.MissingValue;
        const request = kernels.Invocation{ .descriptor = descriptor, .inputs = inputs[0..descriptor.input_count] };
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
        if (!try run(cb.ptr, bound)) return error.AssignedKernelDeclinedAfterBinding;
        self.stats.dispatches += 1;
        return output;
    }
};
