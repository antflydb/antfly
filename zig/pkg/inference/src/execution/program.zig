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

/// Known process-stable stateless implementations. Unknown vtables may also
/// legitimately ignore an undefined context: absence here never proves state.
pub fn processMetadataAllocator(allocator: std.mem.Allocator) bool {
    if (allocator.vtable == &std.heap.SmpAllocator.vtable or allocator.vtable == &std.heap.PageAllocator.vtable) return true;
    if (comptime @import("builtin").link_libc) {
        if (allocator.vtable == std.heap.c_allocator.vtable) return true;
        if (comptime @hasDecl(std.heap, "raw_c_allocator")) {
            if (allocator.vtable == std.heap.raw_c_allocator.vtable) return true;
        }
    }
    return false;
}
/// Conservative shared-owner identity, not arbitrary Allocator equality.
/// Unknown implementations always clone; NEVER inspect their context field.
pub fn knownSameMetadataAllocator(a: std.mem.Allocator, b: std.mem.Allocator) bool {
    if (a.vtable != b.vtable) return false;
    if (processMetadataAllocator(a)) return true;
    if (comptime @import("builtin").is_test) {
        const allocator = std.testing.allocator;
        if (a.vtable == allocator.vtable) return a.ptr == allocator.ptr and b.ptr == allocator.ptr;
    }
    return false;
}

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
    /// Prepared once; external/state parameters must never become cached weights.
    mutable_parameters: ?[]bool = null,
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
        var mutable_parameters: ?[]bool = null;
        errdefer if (mutable_parameters) |flags| allocator.free(flags);
        for (mapped_bindings) |binding| {
            if ((binding.kind != .input and binding.kind != .state_input) or binding.node_id == ml.graph.null_node) continue;
            if (mutable_parameters == null) {
                const flags = try allocator.alloc(bool, optimized.graph.nodeCount());
                @memset(flags, false);
                mutable_parameters = flags;
            }
            mutable_parameters.?[binding.node_id] = true;
        }
        const analysis = try interpreter.CachedAnalysis.compute(allocator, &optimized.graph);
        return .{ .graph = optimized.graph, .id_map = optimized.id_map, .bindings = mapped_bindings, .binding_names = names, .mutable_parameters = mutable_parameters, .analysis = analysis, .optimization_stats = optimized.stats };
    }

    /// Take an already normalized semantic graph without running another
    /// optimizer. Native original-reduction normalization must remain final.
    /// Failure leaves graph ownership with the caller.
    pub fn takeOptimized(graph: *ml.graph.Graph, input: NodeId) !ModelProgram {
        const allocator = graph.allocator;
        if (input >= graph.nodeCount() or graph.node(input).op != .parameter) return error.InvalidProgramBinding;
        const id_map = try allocator.alloc(NodeId, graph.nodeCount());
        errdefer allocator.free(id_map);
        for (id_map, 0..) |*id, index| id.* = @intCast(index);
        const bindings = try allocator.alloc(Binding, 1);
        errdefer allocator.free(bindings);
        const names = try allocator.dupe(u8, graph.parameterName(graph.node(input)));
        errdefer allocator.free(names);
        bindings[0] = .{ .name = names, .node_id = input, .kind = .input };
        const mutable_parameters = try allocator.alloc(bool, graph.nodeCount());
        errdefer allocator.free(mutable_parameters);
        @memset(mutable_parameters, false);
        mutable_parameters[input] = true;
        const analysis = try interpreter.CachedAnalysis.compute(allocator, graph);
        const owned = graph.*;
        graph.* = ml.graph.Graph.init(allocator);
        return .{ .graph = owned, .id_map = id_map, .bindings = bindings, .binding_names = names, .mutable_parameters = mutable_parameters, .analysis = analysis, .optimization_stats = .{} };
    }

    pub fn deinit(self: *ModelProgram) void {
        const allocator = self.graph.allocator;
        self.analysis.deinit(allocator);
        if (self.mutable_parameters) |flags| allocator.free(flags);
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
    try std.testing.expect(program.mutable_parameters.?[program.bindings[1].node_id]);
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

test "ModelProgram mutable binding roles clean up every preparation allocation failure" {
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    try graph.markOutput(try b.mul(x, try b.scalarConst(.f32, 2)));
    const Prepare = struct {
        fn run(allocator: std.mem.Allocator, source: *const ml.graph.Graph, input: NodeId) !void {
            var program = try ModelProgram.init(allocator, source, &.{.{ .name = "x", .node_id = input, .kind = .state_input }});
            defer program.deinit();
            try std.testing.expect(program.mutable_parameters.?[program.bindings[0].node_id]);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Prepare.run, .{ &graph, x });
}

test "ModelProgram normalized ownership preserves original nodes without another optimizer and all OOM" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var graph = ml.graph.Graph.init(a);
            defer graph.deinit();
            var builder = ml.graph.Builder.init(&graph);
            const input = try builder.parameter("token", ml.graph.Shape.scalar(.f32));
            const output = try builder.mul(input, try builder.scalarConst(.f32, 1));
            try graph.markOutput(output);
            const count = graph.nodeCount();
            var program = try ModelProgram.takeOptimized(&graph, input);
            defer program.deinit();
            try std.testing.expectEqual(@as(usize, 0), graph.nodeCount());
            try std.testing.expectEqual(count, program.graph.nodeCount());
            try std.testing.expect(program.graph.node(output).op == .mul);
            for (program.id_map, 0..) |id, index| try std.testing.expectEqual(@as(NodeId, @intCast(index)), id);
            try std.testing.expect(program.mutable_parameters.?[input]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "ModelProgram metadata identity never reads stateless allocator context" {
    var first = std.heap.smp_allocator;
    var second = std.heap.smp_allocator;
    first.ptr = @ptrFromInt(4096);
    second.ptr = @ptrFromInt(8192);
    try std.testing.expect(knownSameMetadataAllocator(first, second));
    first = std.heap.page_allocator;
    second = std.heap.page_allocator;
    first.ptr = @ptrFromInt(4096);
    second.ptr = @ptrFromInt(8192);
    if (processMetadataAllocator(first)) try std.testing.expect(knownSameMetadataAllocator(first, second));
    var arena_a = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_a.deinit();
    var arena_b = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_b.deinit();
    try std.testing.expect(!processMetadataAllocator(arena_a.allocator()));
    try std.testing.expect(!knownSameMetadataAllocator(arena_a.allocator(), arena_a.allocator()));
    try std.testing.expect(!knownSameMetadataAllocator(arena_a.allocator(), arena_b.allocator()));
}

/// Test-only genuinely stateless custom allocator. Its distinct table forwards
/// to the global testing owner without reading its own undefined context.
pub const Testing = if (@import("builtin").is_test) struct {
    fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        return std.testing.allocator.rawAlloc(len, alignment, ret);
    }
    fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
        return std.testing.allocator.rawResize(memory, alignment, len, ret);
    }
    fn remap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        return std.testing.allocator.rawRemap(memory, alignment, len, ret);
    }
    fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        std.testing.allocator.rawFree(memory, alignment, ret);
    }
    const vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    pub fn statelessAllocator() std.mem.Allocator {
        return .{ .ptr = undefined, .vtable = &vtable };
    }
} else struct {};

test "ModelProgram normalized take derives exact custom stateless graph owner without context reads" {
    const allocator = Testing.statelessAllocator();
    try std.testing.expect(!processMetadataAllocator(allocator));
    try std.testing.expect(!knownSameMetadataAllocator(allocator, allocator));
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("token", ml.graph.Shape.scalar(.f32));
    try graph.markOutput(try builder.mul(input, try builder.scalarConst(.f32, 1)));
    var owned = try ModelProgram.takeOptimized(&graph, input);
    defer owned.deinit();
    try std.testing.expect(owned.graph.allocator.vtable == allocator.vtable);
    try std.testing.expectEqual(@as(usize, 0), graph.nodeCount());
}
