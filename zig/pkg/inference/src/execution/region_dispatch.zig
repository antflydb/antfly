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

//! Ordinary region submission and atomic publication into a current values table.
//! Covered intermediates have no CT; callers skip them and release external
//! operands at the region's already-validated runtime last-use boundary.
const ops = @import("../ops/ops.zig");
const buffers = @import("physical_buffers.zig");
const binding = @import("region_binding.zig");
const kernel = @import("kernel_route.zig");
const ml = @import("ml");

pub fn execute(frame: *buffers.Frame, backend: *const ops.ComputeBackend, node: ml.graph.NodeId, values: []?ops.CT) !?*const binding.Installed {
    const installed = frame.pool.regionForNode(node) orelse return null;
    const owner_backend = frame.pool.backends[installed.backend_index];
    if (owner_backend.ptr != backend.ptr or owner_backend.vtable != backend.vtable) return error.PreparedRegionOwnerChanged;
    if (installed.owner.entry_node != node) return installed;
    const count = installed.owner.input_nodes.len;
    if (count > binding.max_inputs) return error.PreparedRegionBindingChanged;
    var facts: [binding.max_inputs]kernel.Binding = undefined;
    var inputs: [binding.max_inputs]ops.CT = undefined;
    try frame.pool.regionExecutionFacts(installed.owner, facts[0..count]);
    for (installed.owner.input_nodes, facts[0..count], inputs[0..count]) |input, fact, *value| {
        if (input >= values.len) return error.PreparedRegionBindingChanged;
        value.* = if (fact == .tensor) fact.tensor.value else values[input] orelse return error.MissingValue;
    }
    for (installed.outputs) |output| if (output.slot.node >= values.len or values[output.slot.node] != null) return error.PreparedRegionOutputMismatch;
    var produced = try frame.executeRegion(installed, facts[0..count], inputs[0..count]);
    defer produced.deinit();
    for (installed.outputs, 0..) |output, index| values[output.slot.node] = try produced.take(index);
    return installed;
}
