// Copyright 2026 Antfly, Inc.
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

// Bridge between a ComputeBackend's WeightStore and ml.graph.Graph parameter
// nodes.  Produces a RuntimeInput array suitable for interpreter.execute.
//
// The level-3 training pipeline builds the forward pass as a Graph where every
// model weight is a `bld.parameter("name", shape)` node.  At execution time
// the interpreter needs RuntimeInput entries that bind each parameter node to
// an actual tensor handle (CT).  This module reads the graph's parameter list,
// looks up each name in the ComputeBackend's weight store, and builds the
// runtime-input array.

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const Builder = ml.graph.Builder;
const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;
const interpreter = @import("../graph/interpreter.zig");
const RuntimeInput = interpreter.RuntimeInput;

pub const BindError = error{
    WeightNotFound,
    OutOfMemory,
};

pub const BindResult = struct {
    allocator: std.mem.Allocator,
    /// Owned array of RuntimeInput entries ready to pass to
    /// `interpreter.ExecuteOptions.runtime_inputs`.
    inputs: []RuntimeInput,
    /// Which parameter names were skipped (prefixed with `__` = internal
    /// placeholder, not a model weight).  Callers must bind these separately.
    skipped_count: usize,

    pub fn deinit(self: *BindResult) void {
        self.allocator.free(self.inputs);
        self.* = undefined;
    }
};

/// Walk `graph.parameters`, look up each weight by name in the compute
/// backend, and build a RuntimeInput array.
///
/// Parameters whose name starts with `__` are SKIPPED -- these are internal
/// placeholders (input_ids, attention_mask, targets, rope_cos/sin, etc.)
/// that the training harness binds from per-step data.  The caller must bind
/// them separately via `addBinding`.
///
/// For LoRA parameters (names containing `.lora_A` or `.lora_B`), these are
/// NOT in the WeightStore -- they are managed by `RealAutodiffTrainer` and
/// bound via its own `lora_params` array.  This function skips them too.
///
/// `cb` must have all model weights loaded before this call.
pub fn bindWeightsToGraph(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
) BindError!BindResult {
    const param_count = graph.parameters.items.len;

    // Over-allocate to the total parameter count; we will shrink at the end.
    const buf = allocator.alloc(RuntimeInput, param_count) catch return BindError.OutOfMemory;
    errdefer allocator.free(buf);

    var bound: usize = 0;
    var skipped: usize = 0;

    for (graph.parameters.items) |param_id| {
        const n = graph.node(param_id);

        // Safety: only process parameter nodes.
        switch (n.op) {
            .parameter => {},
            else => continue,
        }

        const name = graph.parameterName(n);

        // Skip internal placeholders (prefixed with "__").
        if (std.mem.startsWith(u8, name, "__")) {
            skipped += 1;
            continue;
        }

        // Skip LoRA parameters -- managed externally by the trainer.
        if (std.mem.indexOf(u8, name, ".lora_A") != null or
            std.mem.indexOf(u8, name, ".lora_B") != null)
        {
            skipped += 1;
            continue;
        }

        const ct = cb.getWeight(name) catch |err| {
            std.log.err("graph_weight_bridge: weight not found: {s} (err={any})", .{ name, err });
            return BindError.WeightNotFound;
        };

        buf[bound] = .{ .node_id = param_id, .value = ct };
        bound += 1;
    }

    // Shrink to actual bound count.
    const inputs = if (bound < param_count)
        allocator.realloc(buf, bound) catch buf[0..bound]
    else
        buf;

    return .{
        .allocator = allocator,
        .inputs = inputs,
        .skipped_count = skipped,
    };
}

/// Convenience: add a single placeholder binding to an existing RuntimeInput
/// array (by reallocating).  Used by trainers to bind `__input_ids` etc.
pub fn addBinding(
    allocator: std.mem.Allocator,
    existing: []RuntimeInput,
    node_id: NodeId,
    value: CT,
) ![]RuntimeInput {
    const new_len = existing.len + 1;
    const new_buf = try allocator.realloc(existing, new_len);
    new_buf[new_len - 1] = .{ .node_id = node_id, .value = value };
    return new_buf;
}

/// Find a parameter node by name in the graph.  Returns null if not found.
pub fn findParameterByName(
    graph: *const Graph,
    name: []const u8,
) ?NodeId {
    for (graph.parameters.items) |param_id| {
        const n = graph.node(param_id);
        switch (n.op) {
            .parameter => {},
            else => continue,
        }
        const pname = graph.parameterName(n);
        if (std.mem.eql(u8, pname, name)) {
            return param_id;
        }
    }
    return null;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "findParameterByName — hit and miss" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const s = Shape.init(.f32, &.{ 4, 3 });
    const p0 = try b.parameter("alpha", s);
    const p1 = try b.parameter("beta", s);
    const p2 = try b.parameter("gamma", s);
    _ = p2;

    // Find existing parameters.
    try std.testing.expectEqual(p0, findParameterByName(&g, "alpha").?);
    try std.testing.expectEqual(p1, findParameterByName(&g, "beta").?);

    // Non-existent returns null.
    try std.testing.expectEqual(@as(?NodeId, null), findParameterByName(&g, "delta"));
}

test "skip logic — internal placeholders and LoRA params" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const s = Shape.init(.f32, &.{ 4, 3 });

    // Internal placeholder (should be skipped).
    _ = try b.parameter("__input_ids", s);
    // Regular model weight.
    _ = try b.parameter("embeddings.word_embeddings.weight", s);
    // LoRA param (should be skipped).
    _ = try b.parameter("layers.0.q_proj.weight.lora_A", s);
    // LoRA param (should be skipped).
    _ = try b.parameter("layers.0.q_proj.weight.lora_B", s);
    // Another regular weight.
    _ = try b.parameter("layers.0.q_proj.weight", s);
    // Another placeholder.
    _ = try b.parameter("__targets", s);

    // Total parameters: 6
    try std.testing.expectEqual(@as(usize, 6), g.parameters.items.len);

    // Count how many would be skipped vs bound by walking the same logic
    // as bindWeightsToGraph (without the actual ComputeBackend call).
    var skipped: usize = 0;
    var would_bind: usize = 0;
    for (g.parameters.items) |param_id| {
        const n = g.node(param_id);
        switch (n.op) {
            .parameter => {},
            else => continue,
        }
        const name = g.parameterName(n);
        if (std.mem.startsWith(u8, name, "__")) {
            skipped += 1;
            continue;
        }
        if (std.mem.indexOf(u8, name, ".lora_A") != null or
            std.mem.indexOf(u8, name, ".lora_B") != null)
        {
            skipped += 1;
            continue;
        }
        would_bind += 1;
    }

    // 2 placeholders + 2 LoRA = 4 skipped, 2 regular weights bound.
    try std.testing.expectEqual(@as(usize, 4), skipped);
    try std.testing.expectEqual(@as(usize, 2), would_bind);

    // Verify findParameterByName still works for all of them.
    try std.testing.expect(findParameterByName(&g, "__input_ids") != null);
    try std.testing.expect(findParameterByName(&g, "layers.0.q_proj.weight.lora_A") != null);
    try std.testing.expect(findParameterByName(&g, "layers.0.q_proj.weight") != null);
}

test "addBinding — grow from empty and from existing" {
    const allocator = std.testing.allocator;

    // Sentinel opaque pointers for CT values.
    const sentinel1: CT = @ptrFromInt(0xDEAD_0001);
    const sentinel2: CT = @ptrFromInt(0xDEAD_0002);

    // Start from an allocator-owned empty slice.
    const empty = try allocator.alloc(RuntimeInput, 0);
    var inputs = try addBinding(allocator, empty, 10, sentinel1);
    try std.testing.expectEqual(@as(usize, 1), inputs.len);
    try std.testing.expectEqual(@as(NodeId, 10), inputs[0].node_id);
    try std.testing.expectEqual(sentinel1, inputs[0].value);

    // Grow by one.
    inputs = try addBinding(allocator, inputs, 20, sentinel2);
    try std.testing.expectEqual(@as(usize, 2), inputs.len);
    try std.testing.expectEqual(@as(NodeId, 20), inputs[1].node_id);
    try std.testing.expectEqual(sentinel2, inputs[1].value);

    allocator.free(inputs);
}
