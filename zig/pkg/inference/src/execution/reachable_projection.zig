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

//! Cold storage projection for a private prepared variant. The executable's
//! cached reachability is the only authority; root compatibility plans persist.
const std = @import("std");
const buffers = @import("../graph/buffer_plan.zig");

pub fn deactivate(pool: anytype) !void {
    const graph = pool.coverage_graph orelse return;
    const reachable = pool.coverage_reachable orelse return;
    if (!pool.owns_plan or pool.frames.items.len != 0) return error.InvalidPhysicalProjectionState;
    if (reachable.len != graph.nodeCount() or reachable.len != pool.plan.node_slots.len) return error.InvalidReachabilityTable;
    if (pool.kernels.len != reachable.len) return error.InvalidPhysicalProjectionState;
    for (pool.plan.slots) |slot| if (slot.node_id >= reachable.len) return error.InvalidBufferPlan;
    // Whole-entry unused-input and cross-backend contracts remain unchanged.
    // Their separate transfer/ingress owners require an explicit projection.
    if (pool.plan.transfers.len != 0) return;
    for (pool.plan.slots) |slot| if (slot.kind == .runtime_input and !reachable[slot.node_id]) return;
    for (pool.plan.slots) |slot| if (reachable[slot.node_id]) {
        if (slot.source_slot) |source| {
            if (source >= pool.plan.slots.len or pool.plan.slots[source].node_id >= reachable.len) return error.InvalidBufferPlan;
            if (!reachable[pool.plan.slots[source].node_id]) return;
        }
    };
    try pool.plan.projectReachable(graph, reachable);
    for (pool.kernels, 0..) |*kernel, node| if (!reachable[node]) {
        kernel.* = null;
    };
}

fn mark(required: []usize, id: buffers.AllocationId, bytes: usize) !void {
    if (id == buffers.invalid_allocation or id >= required.len or bytes == 0) return error.InvalidPhysicalProjectionResource;
    required[id] = @max(required[id], bytes);
}
fn original(pool: anytype, required: []usize, id: buffers.AllocationId) !void {
    if (id >= pool.plan.allocations.len) return error.InvalidPhysicalProjectionResource;
    try mark(required, id, @intCast(pool.plan.allocations[id].byte_size));
}

pub fn refine(pool: anytype) !void {
    if (pool.plan.execution_reachable == null) return;
    if (!pool.owns_plan or pool.frames.items.len != 0 or pool.selection == null) return error.InvalidPhysicalProjectionState;
    const required = try pool.allocator.alloc(usize, pool.plan.allocations.len);
    defer pool.allocator.free(required);
    @memset(required, 0);
    // Every live unknown or unsupported output protects the original bank.
    // Known routes can shrink a bank formerly enlarged by an inactive slot.
    for (pool.plan.slots) |slot| {
        if (pool.plan.slotForNode(slot.node_id) == null or slot.kind != .allocation) continue;
        if (pool.preparedKernelRoute(slot.node_id)) |owned| {
            try mark(required, slot.allocation, owned.requirements.primary_bytes);
        } else try original(pool, required, slot.allocation);
    }
    for (pool.kernels) |maybe| if (maybe) |kernel| {
        if (kernel.workspace_bytes != 0) try mark(required, pool.workspace_allocations[kernel.backend_index], kernel.workspace_bytes);
        for (kernel.auxiliaries) |maybe_aux| if (maybe_aux) |aux| {
            const count = aux.requirement.shape.numElements() orelse return error.InvalidPhysicalProjectionResource;
            if (count <= 0) return error.InvalidPhysicalProjectionResource;
            const payload = try std.math.mul(usize, @intCast(count), aux.requirement.shape.dtype.byteSize());
            const rounded = (try std.math.add(usize, payload, 3)) / 4 * 4;
            try mark(required, aux.allocation, rounded);
        };
    };
    for (pool.transfers) |maybe| if (maybe) |transfer| try original(pool, required, transfer.allocation);
    // Region outputs and preparation/dispatch workspace remain represented
    // independently of the retained conservative ordinary-kernel resources.
    for (pool.regions) |region| {
        for (region.outputs) |output| if (output.alias_index == null) try mark(required, output.slot.allocation, output.slot.writable_bytes);
        if (region.workspace_bytes != 0) try mark(required, region.workspace_allocation, region.workspace_bytes);
    }
    if (pool.host_workspace_allocation != buffers.invalid_allocation) try original(pool, required, pool.host_workspace_allocation);
    if (pool.literal_store) |store| {
        for (store.literals, 0..) |literal, index| if (pool.literalEnabled(index)) try original(pool, required, literal.allocation);
        for (store.publications) |maybe| if (maybe) |publication| if (pool.literalEnabled(publication.literal)) try original(pool, required, publication.allocation);
    }
    for (pool.replay_inputs) |input| if (input.allocation) |id| try original(pool, required, id);
    for (pool.host_input_plans) |input| try mark(required, input.allocation, input.requirements.bytes);

    // Do not invalidate alias extents when their provider-specific layout has
    // no exact replacement proof. Its backing remains conservatively original.
    for (pool.plan.slots) |slot| {
        if (pool.plan.slotForNode(slot.node_id) == null or slot.kind != .view or slot.allocation == buffers.invalid_allocation) continue;
        const selected_alias = if (pool.selection) |*selection| slot.node_id < selection.plane_counts.len and selection.plane_counts[slot.node_id] == 1 else false;
        if (!selected_alias) try original(pool, required, slot.allocation);
    }
    const enabled = try pool.allocator.alloc(bool, required.len);
    errdefer pool.allocator.free(enabled);
    for (required, enabled, pool.plan.allocations) |bytes, *active, *allocation| {
        active.* = bytes != 0;
        if (bytes != 0) {
            if (bytes > allocation.byte_size) return error.InvalidPhysicalProjectionResource;
            allocation.byte_size = bytes;
        }
    }
    pool.allocator.free(pool.plan.allocation_enabled);
    pool.plan.allocation_enabled = enabled;
}
