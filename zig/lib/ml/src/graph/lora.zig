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

// LoRA: Low-Rank Adaptation of Large Language Models.
//
// Injects trainable low-rank decomposition alongside frozen linear layers:
//   output = frozen_linear(x) + scale * (x @ A^T @ B^T)
//
// A is initialized from small random values; B is zero-initialized so the
// LoRA path produces zero output at init (preserving the original model).
//
// Usage:
//   var result = try injectLoRA(allocator, &graph, .{ .rank = 8, .target_patterns = &.{"q_proj", "v_proj"} });
//   defer result.deinit();
//   // result.graph contains the modified graph
//   // result.adapter has the adapter parameter names

const std = @import("std");
const graph_mod = @import("graph.zig");
const node_mod = @import("node.zig");
const builder_mod = @import("builder.zig");
const shape_mod = @import("shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Builder = builder_mod.Builder;
const Shape = shape_mod.Shape;

pub const LoRAConfig = struct {
    rank: u32,
    alpha: f32 = 1.0,
    target_patterns: []const []const u8,
    sharing: SharingMode = .by_weight,
};

pub const SharingMode = enum {
    /// One adapter pair per base weight parameter. This is standard LoRA and
    /// preserves existing weight-sharing semantics.
    by_weight,
    /// One adapter pair per linear use site. This is intended for recursive
    /// models where the same physical weight is reused at multiple logical
    /// depths and each loop needs a distinct low-rank delta.
    by_use,
};

pub const AdapterInfo = struct {
    /// Original weight parameter name (e.g. "layers.0.attention.q_proj.weight").
    base_name: []const u8,
    /// Name of lora_A parameter (rank x in_dim).
    lora_a_name: []const u8,
    /// Name of lora_B parameter (out_dim x rank).
    lora_b_name: []const u8,
    /// NodeId of lora_A parameter in the modified graph.
    lora_a_id: NodeId,
    /// NodeId of lora_B parameter in the modified graph.
    lora_b_id: NodeId,
    /// Monotonic use-site index among adapted linear ops. Zero for the first
    /// match. In `.by_weight` mode this is informational; in `.by_use` mode it
    /// is encoded into adapter parameter names.
    use_site_index: u32 = 0,
};

pub const LoRAAdapter = struct {
    adapters: std.ArrayListUnmanaged(AdapterInfo),
    /// Owned name strings.
    name_arena: std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,

    pub fn deinit(self: *LoRAAdapter) void {
        self.adapters.deinit(self.allocator);
        self.name_arena.deinit(self.allocator);
    }

    pub fn adapterNames(self: *const LoRAAdapter) []const AdapterInfo {
        return self.adapters.items;
    }
};

pub const LoRAResult = struct {
    graph: Graph,
    adapter: LoRAAdapter,

    pub fn deinit(self: *LoRAResult) void {
        self.graph.deinit();
        self.adapter.deinit();
    }
};

/// Inject LoRA adapters into a graph. Returns a new graph with additional
/// parameters for each matching linear layer plus an adapter descriptor.
///
/// The original graph is not modified.
pub fn injectLoRA(
    allocator: std.mem.Allocator,
    src: *const Graph,
    config: LoRAConfig,
) !LoRAResult {
    // Clone the source graph.
    var g = try cloneGraph(allocator, src);
    errdefer g.deinit();

    var adapter = LoRAAdapter{
        .adapters = .empty,
        .name_arena = .empty,
        .allocator = allocator,
    };
    errdefer adapter.deinit();

    var bld = Builder.init(&g);

    // Scan all nodes for matching linear ops.
    const node_count = g.nodeCount();

    // Pre-allocate the name arena so that slices returned by arenaFmt
    // remain stable across iterations (no reallocation during the loop).
    try adapter.name_arena.ensureTotalCapacity(allocator, node_count * 128);

    // Track which weight parameters already have LoRA adapters. When the
    // same weight is used in multiple fused_linear ops (e.g. DeBERTa's
    // shared Q/K projections for content and relative position), we reuse
    // the same lora_A/B parameters but create separate matmul paths with
    // the correct per-site `rows`. This avoids duplicate adapter names
    // (which would cause gradient overwrites) and preserves weight-sharing
    // semantics.
    var weight_lora_map = std.StringHashMapUnmanaged(void){};
    defer weight_lora_map.deinit(allocator);
    var weight_lora_keys: std.ArrayListUnmanaged([]u8) = .empty;
    defer freeOwnedStrings(allocator, &weight_lora_keys);
    var weight_use_counts = std.StringHashMapUnmanaged(u32){};
    defer weight_use_counts.deinit(allocator);
    var weight_use_count_keys: std.ArrayListUnmanaged([]u8) = .empty;
    defer freeOwnedStrings(allocator, &weight_use_count_keys);

    for (0..node_count) |i| {
        const id: NodeId = @intCast(i);
        const n = g.node(id);

        // Match fused_linear or fused_linear_no_bias.
        const attrs = switch (n.op) {
            .fused_linear => |a| a,
            .fused_linear_no_bias => |a| a,
            else => continue,
        };

        // Get the weight parameter name.
        const weight_id = n.inputs[1]; // input[0] = x, input[1] = weight
        const weight_node = g.node(weight_id);
        if (weight_node.op != .parameter) continue;
        const weight_name = g.parameterName(weight_node);

        // Check if this weight matches any target pattern.
        if (!matchesPattern(weight_name, config.target_patterns)) continue;

        // Found a match. Inject LoRA path:
        //   lora_out = linearNoBias(x, A, rows, in_dim, rank)
        //   lora_out = linearNoBias(lora_out, B, rows, rank, out_dim)
        //   lora_scaled = lora_out * scale
        //   combined = original + lora_scaled
        const input_id = n.inputs[0];
        const rows = attrs.rows;
        const in_dim = attrs.in_dim;
        const out_dim = attrs.out_dim;
        const rank = config.rank;
        const scale = config.alpha / @as(f32, @floatFromInt(rank));

        // Skip if this weight was already LoRA-adapted via a different
        // fused_linear. When the same weight is used in multiple linears
        // (e.g. DeBERTa's shared Q/K projections for content and relative
        // position), the LoRA perturbation on the first use already affects
        // the shared weight for all uses. Injecting a separate matmul path
        // for the second use creates gradient graph shape mismatches.
        const current_use_site = switch (config.sharing) {
            .by_weight => blk: {
                if (weight_lora_map.contains(weight_name)) continue;
                const key = try allocator.dupe(u8, weight_name);
                errdefer allocator.free(key);
                try weight_lora_map.put(allocator, key, {});
                try weight_lora_keys.append(allocator, key);
                break :blk 0;
            },
            .by_use => blk: {
                if (weight_use_counts.getPtr(weight_name)) |count| {
                    const index = count.*;
                    count.* = index + 1;
                    break :blk index;
                }
                const key = try allocator.dupe(u8, weight_name);
                errdefer allocator.free(key);
                try weight_use_counts.put(allocator, key, 1);
                try weight_use_count_keys.append(allocator, key);
                break :blk 0;
            },
        };

        // Create LoRA parameter names.
        const a_name = switch (config.sharing) {
            .by_weight => try arenaFmt(&adapter.name_arena, allocator, "{s}.lora_A", .{weight_name}),
            .by_use => try arenaFmt(&adapter.name_arena, allocator, "{s}.loop_{d}.lora_A", .{ weight_name, current_use_site }),
        };
        const b_name = switch (config.sharing) {
            .by_weight => try arenaFmt(&adapter.name_arena, allocator, "{s}.lora_B", .{weight_name}),
            .by_use => try arenaFmt(&adapter.name_arena, allocator, "{s}.loop_{d}.lora_B", .{ weight_name, current_use_site }),
        };

        // A: [rank, in_dim], B: [out_dim, rank]
        const lora_a = try bld.parameter(a_name, Shape.init(.f32, &.{ @intCast(rank), @intCast(in_dim) }));
        const lora_b = try bld.parameter(b_name, Shape.init(.f32, &.{ @intCast(out_dim), @intCast(rank) }));

        // x @ A^T → [rows, rank]
        const after_a = try bld.linearNoBias(input_id, lora_a, rows, in_dim, rank);
        // [rows, rank] @ B^T → [rows, out_dim]
        const after_b = try bld.linearNoBias(after_a, lora_b, rows, rank, out_dim);

        // Scale.
        const scale_const = try bld.scalarConst(.f32, scale);
        const scaled = try bld.mul(after_b, scale_const);

        // Add to original linear output.
        const combined = try bld.add(id, scaled);

        // Redirect all consumers of the original linear to the combined output.
        redirectConsumers(&g, id, combined, @intCast(i + 1), node_count);

        // Also update graph outputs if they reference this node.
        for (g.outputs.items) |*out| {
            if (out.* == id) out.* = combined;
        }

        try adapter.adapters.append(allocator, .{
            .base_name = weight_name,
            .lora_a_name = a_name,
            .lora_b_name = b_name,
            .lora_a_id = lora_a,
            .lora_b_id = lora_b,
            .use_site_index = current_use_site,
        });
    }

    return .{ .graph = g, .adapter = adapter };
}

test "injectLoRA by_use creates separate adapters for shared weight uses" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x0 = try bld.parameter("x0", Shape.init(.f32, &.{ 2, 4 }));
    const x1 = try bld.parameter("x1", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("shared.q_proj.weight", Shape.init(.f32, &.{ 3, 4 }));
    const y0 = try bld.linearNoBias(x0, w, 2, 4, 3);
    const y1 = try bld.linearNoBias(x1, w, 2, 4, 3);
    const y = try bld.add(y0, y1);
    try g.markOutput(y);

    var result = try injectLoRA(allocator, &g, .{
        .rank = 2,
        .alpha = 2.0,
        .target_patterns = &.{"q_proj"},
        .sharing = .by_use,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.adapter.adapters.items.len);
    try std.testing.expectEqualStrings("shared.q_proj.weight.loop_0.lora_A", result.adapter.adapters.items[0].lora_a_name);
    try std.testing.expectEqualStrings("shared.q_proj.weight.loop_1.lora_A", result.adapter.adapters.items[1].lora_a_name);
    try std.testing.expectEqual(@as(u32, 0), result.adapter.adapters.items[0].use_site_index);
    try std.testing.expectEqual(@as(u32, 1), result.adapter.adapters.items[1].use_site_index);
}

test "injectLoRA by_weight keeps one adapter for shared weight uses" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x0 = try bld.parameter("x0", Shape.init(.f32, &.{ 2, 4 }));
    const x1 = try bld.parameter("x1", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("shared.q_proj.weight", Shape.init(.f32, &.{ 3, 4 }));
    const y0 = try bld.linearNoBias(x0, w, 2, 4, 3);
    const y1 = try bld.linearNoBias(x1, w, 2, 4, 3);
    const y = try bld.add(y0, y1);
    try g.markOutput(y);

    var result = try injectLoRA(allocator, &g, .{
        .rank = 2,
        .alpha = 2.0,
        .target_patterns = &.{"q_proj"},
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.adapter.adapters.items.len);
    try std.testing.expectEqualStrings("shared.q_proj.weight.lora_A", result.adapter.adapters.items[0].lora_a_name);
}

/// Merge LoRA weights back into base weights: W' = W + scale * B @ A.
/// Operates on f32 slices. After merging, the LoRA parameters can be discarded.
pub fn mergeLoRA(
    base_weight: []f32,
    lora_a: []const f32, // [rank, in_dim]
    lora_b: []const f32, // [out_dim, rank]
    config: LoRAConfig,
    in_dim: u32,
    out_dim: u32,
) void {
    const rank = config.rank;
    const scale = config.alpha / @as(f32, @floatFromInt(rank));

    // base_weight is [out_dim, in_dim] in row-major.
    // B @ A = [out_dim, rank] @ [rank, in_dim] = [out_dim, in_dim]
    for (0..out_dim) |o| {
        for (0..in_dim) |k| {
            var dot: f32 = 0.0;
            for (0..rank) |r| {
                // B[o, r] * A[r, k]
                dot += lora_b[o * rank + r] * lora_a[r * in_dim + k];
            }
            base_weight[o * in_dim + k] += scale * dot;
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn matchesPattern(name: []const u8, patterns: []const []const u8) bool {
    for (patterns) |pattern| {
        if (std.mem.indexOf(u8, name, pattern) != null) return true;
    }
    return false;
}

fn redirectConsumers(g: *Graph, old_id: NodeId, new_id: NodeId, start: u32, end: u32) void {
    for (start..end) |i| {
        const n = g.nodeMut(@intCast(i));
        for (&n.inputs) |*inp| {
            if (inp.* == old_id) inp.* = new_id;
        }
        if (n.vjp_alternate == old_id) n.vjp_alternate = new_id;
    }
}

fn cloneGraph(allocator: std.mem.Allocator, src: *const Graph) !Graph {
    var g = Graph.init(allocator);
    errdefer g.deinit();

    // Clone nodes.
    try g.nodes.appendSlice(allocator, src.nodes.items);

    // Clone constant pool.
    try g.constant_pool.appendSlice(allocator, src.constant_pool.items);

    // Clone string table.
    try g.string_table.appendSlice(allocator, src.string_table.items);

    // Clone outputs.
    try g.outputs.appendSlice(allocator, src.outputs.items);

    // Clone parameters.
    try g.parameters.appendSlice(allocator, src.parameters.items);

    return g;
}

fn arenaFmt(
    arena: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    comptime fmt: []const u8,
    args: anytype,
) ![]const u8 {
    const start = arena.items.len;
    const rendered = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(rendered);
    try arena.appendSlice(allocator, rendered);
    return arena.items[start..];
}

fn freeOwnedStrings(allocator: std.mem.Allocator, strings: *std.ArrayListUnmanaged([]u8)) void {
    for (strings.items) |item| allocator.free(item);
    strings.deinit(allocator);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "injectLoRA adds adapter parameters" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // x:[2,4], w:[3,4] -> linear_no_bias -> output:[2,3]
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("attn.q_proj.weight", Shape.init(.f32, &.{ 3, 4 }));
    const y = try bld.linearNoBias(x, w, 2, 4, 3);
    try g.markOutput(y);

    var result = try injectLoRA(allocator, &g, .{
        .rank = 2,
        .alpha = 2.0,
        .target_patterns = &.{"q_proj"},
    });
    defer result.deinit();

    // Should have one adapter.
    try std.testing.expectEqual(@as(usize, 1), result.adapter.adapters.items.len);

    const info = result.adapter.adapters.items[0];
    try std.testing.expectEqualStrings("attn.q_proj.weight", info.base_name);
    try std.testing.expectEqualStrings("attn.q_proj.weight.lora_A", info.lora_a_name);
    try std.testing.expectEqualStrings("attn.q_proj.weight.lora_B", info.lora_b_name);

    // lora_A shape should be [rank=2, in_dim=4]
    const a_node = result.graph.node(info.lora_a_id);
    try std.testing.expectEqual(@as(i32, 2), a_node.output_shape.dims[0]);
    try std.testing.expectEqual(@as(i32, 4), a_node.output_shape.dims[1]);

    // lora_B shape should be [out_dim=3, rank=2]
    const b_node = result.graph.node(info.lora_b_id);
    try std.testing.expectEqual(@as(i32, 3), b_node.output_shape.dims[0]);
    try std.testing.expectEqual(@as(i32, 2), b_node.output_shape.dims[1]);

    // Graph output should no longer be the original linear.
    try std.testing.expect(result.graph.outputs.items[0] != y);
}

test "zero-B LoRA preserves output via mergeLoRA" {
    // When B=0, merging produces W' = W + 0 = W.
    var base = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 }; // [2, 3]
    const original = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const a = [_]f32{ 0.5, 0.3, 0.1, 0.2, 0.4, 0.6 }; // [rank=2, in_dim=3] — nonzero
    const b_zeros = [_]f32{ 0, 0, 0, 0 }; // [out_dim=2, rank=2] — all zero

    mergeLoRA(&base, &a, &b_zeros, .{ .rank = 2, .alpha = 1.0, .target_patterns = &.{} }, 3, 2);

    // Should be unchanged.
    for (0..6) |i| {
        try std.testing.expectApproxEqAbs(original[i], base[i], 1e-6);
    }
}

test "mergeLoRA correctness" {
    // W' = W + (alpha/rank) * B @ A
    var base = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 }; // [2, 3]
    const a = [_]f32{ 1.0, 0.0, 0.0, 0.0, 1.0, 0.0 }; // [rank=2, in_dim=3] — picks cols 0,1
    const b = [_]f32{ 1.0, 0.0, 0.0, 1.0 }; // [out_dim=2, rank=2] — identity

    // B @ A = [[1,0,0],[0,1,0]], scale = alpha/rank = 2.0/2 = 1.0
    // W' = W + 1.0 * [[1,0,0],[0,1,0]] = [[2,2,3],[4,6,6]]
    mergeLoRA(&base, &a, &b, .{ .rank = 2, .alpha = 2.0, .target_patterns = &.{} }, 3, 2);

    try std.testing.expectApproxEqAbs(@as(f32, 2.0), base[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), base[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), base[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), base[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), base[4], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), base[5], 1e-6);
}
