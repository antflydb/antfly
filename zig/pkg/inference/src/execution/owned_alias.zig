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

//! Exact full-span aliases of selected activation banks. Model/runtime inputs,
//! literals, affine/partial views and opaque roots retain compatibility floors.
const std = @import("std");
const physical = @import("physical_buffers.zig");
const buffers = @import("../graph/buffer_plan.zig");
const metadata = @import("metadata_alias.zig");
const ops = @import("../ops/ops.zig");
const ml = @import("ml");

pub fn fullSpanActivation(pool: *const physical.Pool, slot: buffers.LogicalSlot, target: ops.ComputeBackend) bool {
    if (target.kind() != .cuda and target.kind() != .metal) return false;
    const functions = target.vtable.kernels orelse return false;
    if (!functions.accepts(target.vtable) or functions.createMetadataAlias == null) return false;
    const graph = pool.coverage_graph orelse return false;
    const selected = if (pool.selection) |*value| value else return false;
    const reachable = pool.coverage_reachable orelse return false;
    if (slot.kind != .view or slot.backend != target.kind() or slot.node_id >= graph.nodeCount() or slot.node_id >= reachable.len or !reachable[slot.node_id]) return false;
    const node = graph.node(slot.node_id);
    if (node.num_inputs != 1 or node.inputs[0] >= reachable.len or !reachable[node.inputs[0]]) return false;
    const root = metadata.activationRoot(&pool.plan, slot.node_id, slot.backend) orelse return false;
    if (root.node_id >= selected.planes_witnessed.len or !selected.planes_witnessed[root.node_id]) return false;
    const allocating = pool.plan.allocationById(root.allocation) orelse return false;
    if (allocating.backend != slot.backend or allocating.kind != .tensor) return false;
    var current = slot;
    var depth: usize = 0;
    while (current.kind == .view) : (depth += 1) {
        if (depth >= pool.plan.slots.len or current.node_id >= selected.plane_counts.len or selected.plane_counts[current.node_id] != 1 or !selected.planes_witnessed[current.node_id]) return false;
        if (current.node_id >= graph.nodeCount() or current.node_id >= reachable.len or !reachable[current.node_id]) return false;
        const current_node = graph.node(current.node_id);
        if (current_node.num_inputs != 1) return false;
        const parent_id = current.source_slot orelse return false;
        if (parent_id >= pool.plan.slots.len) return false;
        const parent = pool.plan.slots[parent_id];
        if (parent.node_id != current_node.inputs[0] or parent.backend != current.backend) return false;
        if (parent.node_id >= selected.plane_counts.len or selected.plane_counts[parent.node_id] != 1) return false;
        var expected: ?@import("storage_ownership.zig").Plane = null;
        for (pool.metadata_aliases, 0..) |alias, index| {
            if (index >= selected.alias_enabled.len) return false;
            if (alias.node != current.node_id or alias.source != parent.node_id or !selected.alias_enabled[index]) continue;
            if (alias.kind != .reshape and alias.kind != .full_slice) return false;
            if (!alias.input.eq(parent.desc.shape) or !alias.output.eq(current.desc.shape) or metadata.logicalBytes(alias.input) != metadata.logicalBytes(alias.output)) return false;
            expected = alias.apply(selected.planes[parent.node_id][0]) orelse return false;
            break;
        }
        const planned = expected orelse return false;
        const plane = selected.planes[current.node_id][0];
        const bytes = metadata.logicalBytes(current.desc.shape) orelse return false;
        const storage: @import("../graph/backend_contracts.zig").TensorStorageClass = switch (target.kind()) {
            .cuda => .cuda_buffer,
            .metal => .metal_buffer,
            else => unreachable,
        };
        if (!std.meta.eql(plane.backing, planned.backing) or plane.byte_offset != planned.byte_offset) return false;
        if (plane.role != .primary or plane.descriptor.current_backend != target.kind() or plane.descriptor.storage != storage or plane.descriptor.quant_format != null or plane.descriptor.attention_storage != null or plane.descriptor.attention_kv_format != null or !plane.descriptor.shape.eq(current.desc.shape) or plane.descriptor.view_source != parent.node_id or !metadata.dense(current.desc.shape, plane.descriptor.strides) or plane.logical_bytes != bytes or plane.readable_bytes != bytes or plane.writable != null or plane.layout_key != null or plane.storage_epoch != selected.epoch or allocating.byte_size < bytes) return false;
        current = parent;
    }
    return current.id == root.id;
}
