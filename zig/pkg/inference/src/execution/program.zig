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

//! Owned, optimized semantic program and stable external bindings. No device
//! selection, tensor storage, serving policy, or weight materialization here.
const std = @import("std");
const ml = @import("ml");
const interpreter = @import("../graph/interpreter.zig");
const NodeId = ml.graph.NodeId;

pub const Binding = struct {
    name: []const u8,
    node_id: NodeId,
    kind: enum { input, weight, output, state_input, state_output },
};

pub const ModelProgram = struct {
    graph: ml.graph.Graph,
    id_map: []NodeId,
    bindings: []Binding,
    binding_names: []u8,
    analysis: interpreter.CachedAnalysis,
    optimization_stats: ml.graph.passes.pipeline.Stats,

    /// Borrows source graph/bindings; the returned program owns its graph and
    /// names. Node IDs in returned bindings always refer to the optimized IR.
    pub fn init(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, bindings: []const Binding) !ModelProgram {
        var name_bytes: usize = 0;
        for (bindings) |binding| {
            if (binding.node_id >= graph.nodeCount()) return error.InvalidProgramBinding;
            switch (binding.kind) {
                .input, .weight, .state_input => if (graph.node(binding.node_id).op != .parameter) return error.InvalidProgramBinding,
                .output, .state_output => {
                    if (std.mem.indexOfScalar(NodeId, graph.outputs.items, binding.node_id) == null) return error.UnrootedProgramOutput;
                },
            }
            name_bytes = try std.math.add(usize, name_bytes, binding.name.len);
        }
        var optimized = try ml.graph.passes.pipeline.Pipeline.default.run(allocator, graph);
        errdefer optimized.deinit();
        const mapped_bindings = try allocator.alloc(Binding, bindings.len);
        errdefer allocator.free(mapped_bindings);
        const names = try allocator.alloc(u8, name_bytes);
        errdefer allocator.free(names);
        var offset: usize = 0;
        for (bindings, mapped_bindings) |binding, *mapped| {
            const owned_name = names[offset..][0..binding.name.len];
            @memcpy(owned_name, binding.name);
            offset += binding.name.len;
            mapped.* = binding;
            mapped.name = owned_name;
            mapped.node_id = optimized.id_map[binding.node_id];
        }
        const analysis = try interpreter.CachedAnalysis.compute(allocator, &optimized.graph);
        return .{ .graph = optimized.graph, .id_map = optimized.id_map, .bindings = mapped_bindings, .binding_names = names, .analysis = analysis, .optimization_stats = optimized.stats };
    }

    pub fn deinit(self: *ModelProgram) void {
        const allocator = self.graph.allocator;
        self.analysis.deinit(allocator);
        allocator.free(self.binding_names);
        allocator.free(self.bindings);
        allocator.free(self.id_map);
        self.graph.deinit();
    }

    /// Bind already-resident values once at preparation. Eliminated public
    /// inputs keep their ABI binding but need no physical runtime value.
    pub fn remapInputs(self: *const ModelProgram, inputs: []interpreter.RuntimeInput) ![]interpreter.RuntimeInput {
        var count: usize = 0;
        for (inputs) |input| {
            if (input.node_id >= self.id_map.len) return error.InvalidProgramBinding;
            const mapped = self.id_map[input.node_id];
            if (mapped == ml.graph.null_node) continue;
            inputs[count] = input;
            inputs[count].node_id = mapped;
            count += 1;
        }
        return inputs[0..count];
    }
};

test "ModelProgram preserves public and state bindings through optimization" {
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const unused = try b.parameter("unused", ml.graph.Shape.scalar(.f32));
    const state = try b.parameter("state", ml.graph.Shape.scalar(.f32));
    const one = try b.scalarConst(.f32, 1);
    const next = try b.mul(state, one);
    try graph.markOutput(next);
    var name = [_]u8{ 'k', 'v' };
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "unused", .node_id = unused, .kind = .input }, .{ .name = &name, .node_id = state, .kind = .state_input }, .{ .name = "next", .node_id = next, .kind = .state_output } });
    defer program.deinit();
    name[0] = 'x';
    try std.testing.expectEqualStrings("kv", program.bindings[1].name);
    try std.testing.expectEqual(ml.graph.null_node, program.bindings[0].node_id);
    try std.testing.expectEqual(program.graph.outputs.items[0], program.bindings[2].node_id);
    try std.testing.expectEqual(program.bindings[1].node_id, program.bindings[2].node_id);
    const owner: *const @import("../ops/ops.zig").ComputeBackend = @ptrFromInt(4096);
    var values = [_]interpreter.RuntimeInput{
        .{ .node_id = unused, .value = @ptrFromInt(8) },
        .{ .node_id = state, .value = @ptrFromInt(16), .source_backend = owner },
    };
    const mapped = try program.remapInputs(&values);
    try std.testing.expectEqual(@as(usize, 1), mapped.len);
    try std.testing.expectEqual(owner, mapped[0].source_backend.?);
    try std.testing.expectEqual(program.bindings[1].node_id, mapped[0].node_id);
    try std.testing.expectError(error.UnrootedProgramOutput, ModelProgram.init(a, &graph, &.{.{ .name = "unrooted", .node_id = unused, .kind = .state_output }}));
}
