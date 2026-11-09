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

//! Conservative transfer-aware refinement of already legal placement. Only
//! pure regions enclosed by native fallback are considered; resident device
//! graphs and specialized/stateful regions retain their selected implementations.
const std = @import("std");
const ml = @import("ml");
const contracts = @import("../graph/backend_contracts.zig");
const cost = @import("../graph/cost.zig");
const BackendKind = contracts.BackendKind;

/// Relative work units, not elapsed nanoseconds. These deliberately charge
/// only one minimum launch per region (backend fusion may reduce node launches).
pub const RegionCosts = struct {
    device_throughput: u64 = 16,
    launch: u64 = 4096,
    transfer_latency: u64 = 4096,
    transfer_bytes_per_unit: u64 = 16,
    /// Qualify larger islands against hardware measurements before raising this.
    max_cpu_work: u64 = 4096,
};

pub const Refinement = struct {
    regions: usize = 0,
    nodes: usize = 0,
    estimated_saved_cost: u64 = 0,
};

pub fn refine(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, assignments: []BackendKind, operator_plans: anytype, capabilities: anytype, tensor_descs: anytype, model: RegionCosts) !Refinement {
    if (model.device_throughput == 0 or model.transfer_bytes_per_unit == 0) return error.InvalidRegionCosts;
    var result = Refinement{};
    if (assignments.len < 3) return result;
    var has_candidate = false;
    for (1..assignments.len) |i| {
        if (assignments[i - 1] == .native and (assignments[i] == .metal or assignments[i] == .cuda)) {
            has_candidate = true;
            break;
        }
    }
    if (!has_candidate) return result;
    // Build a compact consumer index once: refinement remains linear in graph
    // edges instead of scanning the rest of the graph for every fallback island.
    const offsets = try allocator.alloc(usize, assignments.len + 1);
    defer allocator.free(offsets);
    @memset(offsets, 0);
    for (graph.nodes.items) |node| for (node.getInputs()) |input| {
        if (input < assignments.len) offsets[input + 1] += 1;
    };
    for (1..offsets.len) |i| offsets[i] += offsets[i - 1];
    const consumers = try allocator.alloc(usize, offsets[offsets.len - 1]);
    defer allocator.free(consumers);
    // Reuse cursors as generation stamps after building the index.
    const seen = try allocator.dupe(usize, offsets[0..assignments.len]);
    defer allocator.free(seen);
    for (graph.nodes.items, 0..) |node, consumer| for (node.getInputs()) |input| {
        if (input >= assignments.len) continue;
        consumers[seen[input]] = consumer;
        seen[input] += 1;
    };
    @memset(seen, 0);
    const outputs = try allocator.alloc(bool, assignments.len);
    defer allocator.free(outputs);
    @memset(outputs, false);
    for (graph.outputs.items) |id| if (id < outputs.len) {
        outputs[id] = true;
    };
    var start: usize = 0;
    while (start < assignments.len) {
        const backend = assignments[start];
        var end = start + 1;
        while (end < assignments.len and assignments[end] == backend) : (end += 1) {}
        defer start = end;
        if ((backend != .metal and backend != .cuda) or start == 0 or end == assignments.len or assignments[start - 1] != .native or assignments[end] != .native) continue;
        var cpu_work: u64 = 0;
        var legal = true;
        for (start..end) |index| {
            const id: ml.graph.NodeId = @intCast(index);
            const node = graph.node(id);
            if (!pure(node.op) or node.output_shape.dtype != .f32 or node.output_shape.numElements() == null or
                outputs[id] or operator_plans[index] != null)
            {
                legal = false;
                break;
            }
            var native_legal = false;
            for (capabilities) |cap| {
                if (cap.backend != .native) continue;
                if (cap.decision(.{ .graph = graph, .node_id = id, .op = node.op, .tensor_descs = tensor_descs }).can_execute) native_legal = true;
            }
            if (!native_legal) {
                legal = false;
                break;
            }
            cpu_work = cost.checkedAdd(cpu_work, cost.nodeComputeCost(graph, id));
            if (cpu_work > model.max_cpu_work) {
                legal = false;
                break;
            }
        }
        if (!legal) continue;
        var removed_transfer_cost: u64 = 0;
        var added_transfer_cost: u64 = 0;
        // External operands: moving to native either removes an existing
        // native->device copy or adds a device->native copy.
        for (start..end) |index| {
            for (graph.node(@intCast(index)).getInputs()) |input| {
                if (input == ml.graph.null_node or input >= assignments.len or (input >= start and input < end) or seen[input] == start + 1) continue;
                seen[input] = start + 1;
                const source = assignments[input];
                if (source != .native and source != backend) {
                    legal = false;
                    break;
                }
                const transfer = transferCost(graph, input, model);
                if (source == .native) removed_transfer_cost = cost.checkedAdd(removed_transfer_cost, transfer) else added_transfer_cost = cost.checkedAdd(added_transfer_cost, transfer);
            }
        }
        if (!legal) continue;
        // Charge each unique boundary tensor once per destination. A small
        // region can serve native and device consumers profitably; fan-out
        // requires an upload rather than disqualifying the entire region.
        for (start..end) |input| {
            var native_consumer = false;
            var device_consumer = false;
            for (consumers[offsets[input]..offsets[input + 1]]) |consumer| {
                if (consumer >= start and consumer < end) continue;
                if (assignments[consumer] == .native) native_consumer = true else if (assignments[consumer] == backend) device_consumer = true else {
                    legal = false;
                    break;
                }
            }
            const transfer = transferCost(graph, @intCast(input), model);
            if (native_consumer) removed_transfer_cost = cost.checkedAdd(removed_transfer_cost, transfer);
            if (device_consumer) added_transfer_cost = cost.checkedAdd(added_transfer_cost, transfer);
        }
        if (!legal) continue;
        const old_cost = cost.checkedAdd(cost.checkedAdd(cpu_work / model.device_throughput, model.launch), removed_transfer_cost);
        const new_cost = cost.checkedAdd(cpu_work, added_transfer_cost);
        if (new_cost >= old_cost) continue;
        @memset(assignments[start..end], .native);
        result.regions += 1;
        result.nodes += end - start;
        result.estimated_saved_cost = cost.checkedAdd(result.estimated_saved_cost, old_cost - new_cost);
    }
    return result;
}

fn transferCost(graph: *const ml.graph.Graph, id: ml.graph.NodeId, model: RegionCosts) u64 {
    return cost.checkedAdd(model.transfer_latency, cost.tensorByteSize(graph.node(id).output_shape) / model.transfer_bytes_per_unit);
}

fn pure(op: ml.graph.OpCode) bool {
    return switch (op) {
        .neg,
        .sqrt,
        .rsqrt,
        .exp,
        .log,
        .sin,
        .cos,
        .tanh,
        .erf,
        .abs,
        .add,
        .sub,
        .mul,
        .div,
        .fused_gelu,
        .fused_gelu_exact,
        .fused_relu,
        .fused_silu,
        => true,
        else => false,
    };
}
