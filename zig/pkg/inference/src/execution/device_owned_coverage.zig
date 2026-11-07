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

//! Device program payload proof for the actual attested assigned Frame path.
//! Driver-private memory, model-owned inputs and protected request IO retain
//! their independent envelopes. Capture readiness alone grants no credit.
const std = @import("std");
const physical = @import("physical_buffers.zig");
const coverage = @import("owned_coverage.zig");
const buffers = @import("../graph/buffer_plan.zig");
const route = @import("kernel_route.zig");
const ops = @import("../ops/ops.zig");
const ml = @import("ml");
const services_type = @import("../ops/kernel_backend.zig").OwnedPayloadServices;

pub fn services(backend: ops.ComputeBackend) ?*const services_type {
    const functions = backend.vtable.kernels orelse return null;
    const value = functions.ownedPayloadServices orelse return null;
    if (!value.accepts(backend.vtable)) return null;
    if (!value.available(backend.ptr)) return null;
    return value;
}
fn bytes(shape: ml.graph.Shape) ?usize {
    if (shape.dtype != .f32 and shape.dtype != .i32 and shape.dtype != .i64) return null;
    const count = shape.numElements() orelse return null;
    if (count <= 0) return null;
    return std.math.mul(usize, @intCast(count), shape.dtype.byteSize()) catch null;
}
// Reduced auxiliary payload is a separate physical representation, never a
// logical graph input/output permission. The caller already attests the exact
// provider route and matches this role/shape against its declared auxiliaries.
fn auxiliaryBytes(auxiliary: ops.kernel_descriptor.Auxiliary) ?usize {
    if (auxiliary.shape.dtype != .bf16) return bytes(auxiliary.shape);
    if (auxiliary.role != .bf16_mirror) return null;
    const count = auxiliary.shape.numElements() orelse return null;
    if (count <= 0) return null;
    return std.math.mul(usize, @intCast(count), @sizeOf(u16)) catch null;
}
fn allocation(pool: *const physical.Pool, id: buffers.AllocationId, size: usize, kind: buffers.AllocationKind, host: bool) bool {
    const value = pool.plan.allocationById(id) orelse return false;
    const target = pool.coverage_backend orelse return false;
    const selected_backend: @TypeOf(value.backend) = if (host) .native else target.kind();
    return id != buffers.invalid_allocation and value.id == id and value.backend == selected_backend and value.kind == kind and value.byte_size >= size;
}
fn ingress(pool: *const physical.Pool, node: ml.graph.NodeId, logical: ml.graph.Shape, target: ops.ComputeBackend) bool {
    const selected = if (pool.selection) |*value| value else return false;
    const fact = selected.inputs.find(node) orelse return false;
    if (fact != .imported_host or !fact.imported_host.owned_ingress or fact.imported_host.target != target.kind() or !fact.imported_host.logical.eq(logical)) return false;
    var found = false;
    for (pool.host_input_plans) |plan| if (plan.node == node) {
        if (found or plan.borrowed or plan.backend_index >= pool.backends.len or plan.epoch != selected.epoch or !plan.shape.eq(logical)) return false;
        const backend = pool.backends[plan.backend_index];
        if (backend.ptr != target.ptr or backend.vtable != target.vtable or services(backend) == null) return false;
        const size = bytes(logical) orelse return false;
        if (plan.requirements.bytes != size or plan.requirements.host_bytes != (if (target.kind() == .cuda) size else @as(usize, 0)) or (target.kind() == .metal and (!plan.requirements.requires_managed_operation or plan.requirements.host_backing_bytes != 0)) or !allocation(pool, plan.allocation, size, .input_binding, false)) return false;
        const functions = backend.vtable.kernels.?;
        const query = functions.hostInputRequirements orelse return false;
        if (functions.createHostInput == null) return false;
        const current = (query(backend.ptr, logical) catch return false) orelse return false;
        if (!current.eql(plan.requirements)) return false;
        // Pool.collectAdmissionRequirements includes this exact host staging
        // owner separately, including SDK-private backing uncertainty.
        found = true;
    };
    return found;
}
fn literal(pool: *const physical.Pool, slot: buffers.LogicalSlot, target: ops.ComputeBackend) ?usize {
    const store = pool.literal_store orelse return null;
    const selected = if (pool.selection) |*value| value else return null;
    if (slot.node_id >= store.node_indices.len) return null;
    const index = store.node_indices[slot.node_id];
    if (index == ml.graph.null_node or index >= store.literals.len or index >= selected.literal_enabled.len or !selected.literal_enabled[index]) return null;
    const value = store.literals[index];
    const size = bytes(value.shape) orelse return null;
    if (value.shape.dtype != .f32 or value.node != slot.node_id or !value.shape.eq(slot.desc.shape) or value.data.len != size / 4 or value.backend_index >= pool.backends.len or value.backend_index >= store.backends.len) return null;
    const backend = pool.backends[value.backend_index];
    const owner = store.backends[value.backend_index];
    if (backend.ptr != target.ptr or backend.vtable != target.vtable or owner.ptr != backend.ptr or owner.vtable != backend.vtable or services(backend) == null or !allocation(pool, value.allocation, size, .constant_binding, false)) return null;
    if (target.kind() == .metal) {
        if (value.staging_bound == 0) return null;
        const functions = target.vtable.kernels.?;
        const eligible = functions.physicalLiteralBindingSupported orelse return null;
        if (!eligible(target.ptr)) return null;
        const query = functions.physicalLiteralInitializer orelse return null;
        const initializer = (query(target.ptr, size) catch return null) orelse return null;
        if (initializer.staging.byte_size != size or initializer.staging.usage != .host_input or initializer.staging.admitted) return null;
        const bound = ops.physical_storage.reservationBytes(target, initializer.staging) catch return null;
        if (bound != value.staging_bound) return null;
    }
    return index;
}
fn publication(pool: *const physical.Pool, slot: buffers.LogicalSlot, index: usize) bool {
    const store = pool.literal_store orelse return false;
    if (slot.node_id >= store.publications.len) return false;
    const copy = store.publications[slot.node_id] orelse return false;
    const value = store.literals[index];
    const size = bytes(value.shape) orelse return false;
    return copy.node == slot.node_id and copy.literal == index and copy.shape.eq(slot.desc.shape) and copy.allocation != value.allocation and allocation(pool, copy.allocation, size, .tensor, false) and !pool.plan.allocations[copy.allocation].reusable;
}
fn resources(pool: *const physical.Pool, kernel: physical.KernelPlan, owned: route.Owned) bool {
    const needed = owned.requirements;
    if (!allocation(pool, kernel.output_allocation, needed.primary_bytes, .tensor, false)) return false;
    const device = @max(needed.workspace.byte_size, needed.probe_workspace.byte_size);
    const host = @max(needed.workspace.host_byte_size, needed.probe_workspace.host_byte_size);
    if (kernel.workspace_bytes != device or kernel.host_workspace_bytes != host) return false;
    if (device != 0 and (kernel.backend_index >= pool.workspace_allocations.len or !allocation(pool, pool.workspace_allocations[kernel.backend_index], device, .workspace, false))) return false;
    if (host != 0 and (!allocation(pool, pool.host_workspace_allocation, host, .workspace, true) or pool.host_alignment.toByteUnits() < @max(needed.host_alignment, needed.probe_host_alignment))) return false;
    var count: usize = 0;
    for (kernel.auxiliaries) |maybe| if (maybe) |aux| {
        if (count >= needed.auxiliaries.count) return false;
        const declared = needed.auxiliaries.items()[count];
        const size = auxiliaryBytes(aux.requirement) orelse return false;
        if (aux.requirement.role != declared.role or !aux.requirement.shape.eq(declared.shape) or !allocation(pool, aux.allocation, size, .auxiliary, false)) return false;
        count += 1;
    };
    return count == needed.auxiliaries.count;
}
pub fn assess(pool: *const physical.Pool, target: ops.ComputeBackend, metadata: ?@import("backend_operation.zig").Borrowed) coverage.Coverage {
    const service = services(target) orelse return .{ .compatibility = .execution_path };
    const authority = metadata orelse return .{ .compatibility = .execution_path };
    authority.validate(.{ .implementation = .{ .context = target.ptr, .table = target.vtable }, .program = pool.coverage_graph, .mode = .metadata }) catch return .{ .compatibility = .execution_path };
    if (target.kind() == .metal and !@import("../graph/metal_partition_executor.zig").ownedFramePolicyAvailable()) return .{ .compatibility = .execution_path };
    const selected = if (pool.selection) |*value| value else return .{ .compatibility = .no_selected_routes };
    const graph = pool.coverage_graph orelse return .{ .compatibility = .execution_path };
    const reachable = pool.coverage_reachable orelse return .{ .compatibility = .execution_path };
    if (!(switch (target.kind()) {
        .cuda => pool.cuda_frame_dispatch,
        .metal => pool.metal_frame_dispatch,
        else => false,
    }) or reachable.len != graph.nodeCount() or pool.plan.node_slots.len != graph.nodeCount() or graph.outputs.items.len == 0) return .{ .compatibility = .execution_path };
    if (pool.regions.len != 0) return .{ .compatibility = .backend_region };
    if (pool.plan.transfers.len != 0) return .{ .compatibility = .transfer };
    for (pool.host_input_plans) |plan| if (plan.node >= reachable.len or !reachable[plan.node] or !ingress(pool, plan.node, plan.shape, target)) return .{ .compatibility = .input_representation };
    var result = coverage.Complete{ .epoch = selected.epoch, .accounting_class = .device };
    for (reachable, 0..) |active, id| {
        if (!active) continue;
        const slot = (pool.plan.slotForNode(@intCast(id)) orelse return .{ .compatibility = .operation }).*;
        const node = graph.node(@intCast(id));
        if (slot.node_id != id or slot.id >= pool.plan.slots.len or pool.plan.node_slots[id] != slot.id or slot.backend != target.kind() or !slot.desc.shape.eq(node.output_shape)) return .{ .compatibility = .backend_region };
        const size = bytes(slot.desc.shape) orelse return .{ .compatibility = .input_representation };
        switch (slot.kind) {
            .runtime_input => {
                if (node.op != .parameter or slot.roles.graph_output) return .{ .compatibility = .input_representation };
                if (selected.inputs.find(@intCast(id))) |binding| {
                    if (binding != .imported_host or !ingress(pool, @intCast(id), slot.desc.shape, target)) return .{ .compatibility = .input_representation };
                } else {
                    const immutable = pool.immutable_bindings orelse return .{ .compatibility = .input_representation };
                    const binding = immutable.find(@intCast(id)) orelse return .{ .compatibility = .input_representation };
                    if (!(service.input(target.ptr, binding, slot.desc.shape) catch false)) return .{ .compatibility = .input_representation };
                }
            },
            .constant => {
                const index = literal(pool, slot, target) orelse return .{ .compatibility = .literal };
                if (target.kind() == .metal and pool.literal_store.?.literals[index].staging_bound == 0)
                    return .{ .compatibility = .literal };
                if (node.op != .constant or (slot.roles.graph_output and !publication(pool, slot, index))) return .{ .compatibility = .output_publication };
                result.literals += 1;
            },
            .view => {
                if (!@import("owned_alias.zig").fullSpanActivation(pool, slot, target)) return .{ .compatibility = .alias };
                result.aliases += 1;
            },
            .allocation => {
                if (id >= selected.routes.len or id >= pool.kernels.len) return .{ .compatibility = .operation };
                const owned = selected.routes[id] orelse return .{ .compatibility = .operation };
                const kernel = pool.kernels[id] orelse return .{ .compatibility = .operation };
                if (kernel.backend_index >= pool.backends.len) return .{ .compatibility = .backend_region };
                const backend = pool.backends[kernel.backend_index];
                if (backend.ptr != target.ptr or backend.vtable != target.vtable or !service.route(target.ptr, &owned) or owned.implementation.context != target.ptr or owned.implementation.table != @as(*const anyopaque, @ptrCast(target.vtable)) or kernel.output_allocation != slot.allocation or !kernel.descriptor.output_shape.eq(slot.desc.shape) or owned.requirements.primary_bytes < size or kernel.descriptor.input_count != node.num_inputs) return .{ .compatibility = .operation };
                for (kernel.inputs[0..kernel.descriptor.input_count], node.getInputs(), 0..) |actual, expected, index| {
                    if (actual != expected or expected >= reachable.len or !reachable[expected] or !kernel.descriptor.input_shapes[index].eq(graph.node(expected).output_shape)) return .{ .compatibility = .operation };
                }
                if (!resources(pool, kernel, owned)) return .{ .compatibility = .workspace };
                result.operations += 1;
            },
        }
    }
    for (graph.outputs.items) |id| {
        if (id >= reachable.len or !reachable[id]) return .{ .compatibility = .output_publication };
        const slot = pool.plan.slotForNode(id) orelse return .{ .compatibility = .output_publication };
        if (!slot.roles.graph_output) return .{ .compatibility = .output_publication };
    }
    return .{ .complete = result };
}
