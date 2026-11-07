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

//! Shared shape/operation cost arithmetic. Backend policy supplies relative
//! throughput and transfer assumptions without duplicating semantic formulas.
const std = @import("std");
const ml = @import("ml");
const contracts = @import("backend_contracts.zig");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;

pub fn nodeElementCost(graph: *const Graph, node_id: NodeId) u64 {
    const shape = graph.node(node_id).output_shape;
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return 1;
    return @intCast(elems);
}

pub fn tensorByteSize(shape: ml.graph.Shape) u64 {
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return @intCast(shape.dtype.byteSize());
    return checkedMul(@intCast(elems), @intCast(shape.dtype.byteSize()));
}

pub fn descByteSize(desc: contracts.TensorDesc) u64 {
    return tensorByteSize(desc.shape);
}

pub fn nodeComputeCost(graph: *const Graph, node_id: NodeId) u64 {
    const n = graph.node(node_id);
    return switch (n.op) {
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair => |attrs| mul3Cost(attrs.rows, attrs.in_dim, attrs.out_dim),
        .dot_general => |attrs| dotGeneralCost(graph, node_id, attrs) orelse nodeElementCost(graph, node_id),
        else => nodeElementCost(graph, node_id),
    };
}

pub fn dotGeneralCost(graph: *const Graph, node_id: NodeId, attrs: ml.graph.node.DotGeneralAttrs) ?u64 {
    if (attrs.num_contracting != 1) return null;
    const n = graph.node(node_id);
    if (n.num_inputs < 2) return null;
    const lhs = graph.node(n.inputs[0]).output_shape;
    const lhs_axis = attrs.lhs_contracting[0];
    if (lhs_axis >= lhs.rank()) return null;
    const k = lhs.dims[lhs_axis];
    if (k <= 0) return null;
    return checkedMul(nodeElementCost(graph, node_id), @intCast(k));
}

pub fn mul3Cost(a: u32, b: u32, c: u32) u64 {
    return checkedMul(checkedMul(a, b), c);
}

pub fn checkedMul(a: u64, b: u64) u64 {
    return std.math.mul(u64, a, b) catch std.math.maxInt(u64);
}

pub fn checkedAdd(a: u64, b: u64) u64 {
    return std.math.add(u64, a, b) catch std.math.maxInt(u64);
}

pub fn nodeInputBytes(query: anytype) u64 {
    const descs = query.tensor_descs orelse return 0;
    const n = query.graph.node(query.node_id);
    var total: u64 = 0;
    for (n.getInputs()) |input_id| {
        if (input_id < descs.len) {
            if (descs[input_id]) |desc| total = checkedAdd(total, descByteSize(desc));
        }
    }
    return total;
}
