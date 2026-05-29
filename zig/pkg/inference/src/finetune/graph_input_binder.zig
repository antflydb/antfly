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

// Helpers for binding ALL input placeholders (standard + architecture-specific)
// to runtime data before graph execution.
//
// The RealAutodiffTrainer harness creates 3 standard placeholders:
//   __input_ids, __attention_mask, __targets
// Each architecture's buildForward callback creates ADDITIONAL internal
// placeholders (e.g. BERT: __bert_position_ids, __bert_token_type_ids,
// __bert_attn_bias; Qwen2: __qwen_rope_cos, __qwen_rope_sin).
//
// At runtime, interpreter.execute needs RuntimeInput entries for ALL of them.
// This module lets the harness discover and bind those extra placeholders.

const std = @import("std");
const math = std.math;
const ml = @import("ml");
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const ops_mod = @import("../ops/ops.zig");
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;

/// Metadata about an input placeholder the architecture created.
pub const PlaceholderInfo = struct {
    node_id: NodeId,
    name: []const u8,
    shape: Shape,
};

/// Scan a graph for all parameter nodes whose name starts with `__` (the
/// convention for input placeholders). Returns them sorted by name.
/// Caller owns the returned slice (allocated with `allocator`).
pub fn listPlaceholders(
    allocator: std.mem.Allocator,
    graph: *const Graph,
) ![]PlaceholderInfo {
    var list: std.ArrayList(PlaceholderInfo) = .empty;
    errdefer list.deinit(allocator);

    for (graph.parameters.items) |param_id| {
        const n = graph.node(param_id);
        if (n.op != .parameter) continue;
        const name = graph.parameterName(n);
        if (name.len >= 2 and name[0] == '_' and name[1] == '_') {
            try list.append(allocator, .{
                .node_id = param_id,
                .name = name,
                .shape = n.output_shape,
            });
        }
    }

    // Sort by name for deterministic ordering.
    std.mem.sort(PlaceholderInfo, list.items, {}, struct {
        fn lessThan(_: void, a: PlaceholderInfo, b: PlaceholderInfo) bool {
            return std.mem.order(u8, a.name, b.name) == .lt;
        }
    }.lessThan);

    return list.toOwnedSlice(allocator);
}

/// Given a list of placeholder infos and a name, find the matching entry.
/// Returns null if no placeholder with that name exists.
pub fn findPlaceholder(
    placeholders: []const PlaceholderInfo,
    name: []const u8,
) ?PlaceholderInfo {
    for (placeholders) |p| {
        if (std.mem.eql(u8, p.name, name)) return p;
    }
    return null;
}

/// Bind a placeholder to a concrete value by converting an f32 slice to a
/// backend tensor. This is the helper trainers call for each placeholder.
pub fn bindF32(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    placeholder: PlaceholderInfo,
    data: []const f32,
) !CT {
    const dims = try shapeToDims(allocator, placeholder.shape);
    defer allocator.free(dims);
    return cb.fromFloat32Shape(data, dims);
}

/// Bind a placeholder to a concrete i64 value (for token IDs, position IDs).
/// Converts i64 -> f32 slice (via simple cast), then calls fromFloat32Shape.
/// This is a lossy conversion but matches the current harness convention.
/// A proper i64 binding path is a TODO.
pub fn bindI64(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    placeholder: PlaceholderInfo,
    data: []const i64,
) !CT {
    const f32_buf = try allocator.alloc(f32, data.len);
    defer allocator.free(f32_buf);
    for (data, 0..) |v, i| {
        f32_buf[i] = @floatFromInt(v);
    }
    return bindF32(cb, allocator, placeholder, f32_buf);
}

/// Convert a Shape to the i32 dims slice expected by ComputeBackend.fromFloat32Shape.
fn shapeToDims(allocator: std.mem.Allocator, shape: Shape) ![]i32 {
    const r = shape.rank();
    const dims = try allocator.alloc(i32, r);
    for (0..r) |i| dims[i] = @intCast(shape.dim(@intCast(i)));
    return dims;
}

// ── Architecture-specific placeholder prep ────────────────────────────────

/// Architecture-specific placeholder prep: for BERT, build position_ids
/// and attn_bias from basic inputs.
pub const BertPlaceholderPrep = struct {
    /// Derive position_ids: [0, 1, ..., seq_len-1] repeated batch times.
    /// Returns a [batch * seq_len] i64 slice. Caller owns the memory.
    pub fn buildPositionIds(
        allocator: std.mem.Allocator,
        batch: u32,
        seq_len: u32,
    ) ![]i64 {
        const total: usize = @as(usize, batch) * @as(usize, seq_len);
        const ids = try allocator.alloc(i64, total);
        for (0..total) |i| {
            ids[i] = @intCast(i % seq_len);
        }
        return ids;
    }

    /// Build additive attention bias from a flat f32 mask [batch * seq_len]:
    /// returns [batch * num_heads * seq_len * seq_len] where padded positions
    /// have -1e9 and valid positions have 0.0.
    /// Caller owns the returned slice.
    pub fn buildAttnBias(
        allocator: std.mem.Allocator,
        attention_mask: []const f32,
        batch: u32,
        seq_len: u32,
        num_heads: u32,
    ) ![]f32 {
        const sl: usize = @intCast(seq_len);
        const b: usize = @intCast(batch);
        const nh: usize = @intCast(num_heads);
        const total = b * nh * sl * sl;
        const bias = try allocator.alloc(f32, total);

        // Layout: [batch, num_heads, seq_len, seq_len]
        // For each (b, h, i, j): if mask[b * seq_len + j] < 0.5 then -1e9 else 0.0
        var idx: usize = 0;
        for (0..b) |bi| {
            for (0..nh) |_| {
                for (0..sl) |_| {
                    for (0..sl) |j| {
                        const mask_idx = bi * sl + j;
                        bias[idx] = if (attention_mask[mask_idx] < 0.5) -1.0e9 else 0.0;
                        idx += 1;
                    }
                }
            }
        }

        return bias;
    }
};

/// Qwen2-specific: build RoPE cos/sin tables.
pub const QwenPlaceholderPrep = struct {
    pub const RopeCosSin = struct {
        cos: []f32,
        sin: []f32,
    };

    /// Build RoPE cos/sin tables for positional encoding.
    ///
    /// Allocates [seq_len * head_dim] f32 for cos, same for sin.
    /// For each (pos, i) where i < head_dim/2:
    ///   inv_freq = 1.0 / pow(theta, 2*i / head_dim)
    ///   cos[pos * head_dim + i] = @cos(pos * inv_freq)
    ///   sin[pos * head_dim + i] = @sin(pos * inv_freq)
    /// Then duplicates the first head_dim/2 values to the second half
    /// (matching the split convention used by Builder.rope).
    /// Caller owns both returned slices.
    pub fn buildRopeCosSin(
        allocator: std.mem.Allocator,
        seq_len: u32,
        head_dim: u32,
        theta: f32,
    ) !RopeCosSin {
        const sl: usize = @intCast(seq_len);
        const hd: usize = @intCast(head_dim);
        const half = hd / 2;
        const total = sl * hd;

        const cos_buf = try allocator.alloc(f32, total);
        errdefer allocator.free(cos_buf);
        const sin_buf = try allocator.alloc(f32, total);

        for (0..sl) |pos| {
            for (0..half) |i| {
                const fi: f32 = @floatFromInt(i);
                const fhd: f32 = @floatFromInt(hd);
                const fpos: f32 = @floatFromInt(pos);
                const inv_freq = 1.0 / math.pow(f32, theta, 2.0 * fi / fhd);
                const angle = fpos * inv_freq;
                const c = @cos(angle);
                const s = @sin(angle);

                // First half
                cos_buf[pos * hd + i] = c;
                sin_buf[pos * hd + i] = s;
                // Duplicate to second half (split convention)
                cos_buf[pos * hd + half + i] = c;
                sin_buf[pos * hd + half + i] = s;
            }
        }

        return .{ .cos = cos_buf, .sin = sin_buf };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────

const testing = std.testing;

test "listPlaceholders filters and sorts by name" {
    const allocator = testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var b = Builder.init(&graph);

    // Create mixed parameters: two placeholders + one regular weight.
    _ = try b.parameter("__input_ids", Shape.init(.f32, &.{ 2, 8 }));
    _ = try b.parameter("weight.param", Shape.init(.f32, &.{ 64, 64 }));
    _ = try b.parameter("__attn_bias", Shape.init(.f32, &.{ 2, 12, 8, 8 }));

    const placeholders = try listPlaceholders(allocator, &graph);
    defer allocator.free(placeholders);

    // Should find exactly 2 placeholders (not weight.param).
    try testing.expectEqual(@as(usize, 2), placeholders.len);

    // Sorted by name: __attn_bias < __input_ids
    try testing.expectEqualStrings("__attn_bias", placeholders[0].name);
    try testing.expectEqualStrings("__input_ids", placeholders[1].name);
}

test "findPlaceholder returns match or null" {
    const allocator = testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var b = Builder.init(&graph);

    _ = try b.parameter("__input_ids", Shape.init(.f32, &.{ 2, 8 }));
    _ = try b.parameter("__attn_bias", Shape.init(.f32, &.{ 2, 12, 8, 8 }));

    const placeholders = try listPlaceholders(allocator, &graph);
    defer allocator.free(placeholders);

    const found = findPlaceholder(placeholders, "__attn_bias");
    try testing.expect(found != null);
    try testing.expectEqualStrings("__attn_bias", found.?.name);

    const missing = findPlaceholder(placeholders, "__nonexistent");
    try testing.expect(missing == null);
}

test "BertPlaceholderPrep.buildPositionIds shape and values" {
    const allocator = testing.allocator;
    const batch: u32 = 2;
    const seq_len: u32 = 4;

    const ids = try BertPlaceholderPrep.buildPositionIds(allocator, batch, seq_len);
    defer allocator.free(ids);

    // Total length = batch * seq_len = 8
    try testing.expectEqual(@as(usize, 8), ids.len);

    // First batch: 0,1,2,3
    try testing.expectEqual(@as(i64, 0), ids[0]);
    try testing.expectEqual(@as(i64, 1), ids[1]);
    try testing.expectEqual(@as(i64, 2), ids[2]);
    try testing.expectEqual(@as(i64, 3), ids[3]);

    // Second batch: 0,1,2,3 (repeated)
    try testing.expectEqual(@as(i64, 0), ids[4]);
    try testing.expectEqual(@as(i64, 1), ids[5]);
    try testing.expectEqual(@as(i64, 2), ids[6]);
    try testing.expectEqual(@as(i64, 3), ids[7]);
}

test "BertPlaceholderPrep.buildAttnBias masks padded positions" {
    const allocator = testing.allocator;
    const batch: u32 = 1;
    const seq_len: u32 = 3;
    const num_heads: u32 = 2;

    // Mask: first two tokens valid, third is padding.
    const mask = [_]f32{ 1.0, 1.0, 0.0 };

    const bias = try BertPlaceholderPrep.buildAttnBias(
        allocator,
        &mask,
        batch,
        seq_len,
        num_heads,
    );
    defer allocator.free(bias);

    // Total: 1 * 2 * 3 * 3 = 18
    try testing.expectEqual(@as(usize, 18), bias.len);

    // Check head 0 (same pattern for head 1):
    // Row 0: [0.0, 0.0, -1e9] (j=0 valid, j=1 valid, j=2 padded)
    // Row 1: [0.0, 0.0, -1e9]
    // Row 2: [0.0, 0.0, -1e9]
    // Head 0 starts at index 0.
    try testing.expectEqual(@as(f32, 0.0), bias[0]); // (0,0,0,0)
    try testing.expectEqual(@as(f32, 0.0), bias[1]); // (0,0,0,1)
    try testing.expectEqual(@as(f32, -1.0e9), bias[2]); // (0,0,0,2) padded
    try testing.expectEqual(@as(f32, 0.0), bias[3]); // (0,0,1,0)
    try testing.expectEqual(@as(f32, 0.0), bias[4]); // (0,0,1,1)
    try testing.expectEqual(@as(f32, -1.0e9), bias[5]); // (0,0,1,2) padded

    // Head 1 should have the same pattern (starts at index 9).
    try testing.expectEqual(@as(f32, 0.0), bias[9]); // (0,1,0,0)
    try testing.expectEqual(@as(f32, -1.0e9), bias[11]); // (0,1,0,2) padded
}

test "QwenPlaceholderPrep.buildRopeCosSin at pos=0" {
    const allocator = testing.allocator;
    const seq_len: u32 = 2;
    const head_dim: u32 = 4;
    const theta: f32 = 10000.0;

    const result = try QwenPlaceholderPrep.buildRopeCosSin(
        allocator,
        seq_len,
        head_dim,
        theta,
    );
    defer allocator.free(result.cos);
    defer allocator.free(result.sin);

    // Total: seq_len * head_dim = 8
    try testing.expectEqual(@as(usize, 8), result.cos.len);
    try testing.expectEqual(@as(usize, 8), result.sin.len);

    // At pos=0, angle = 0 * inv_freq = 0 for all i.
    // cos(0) = 1.0, sin(0) = 0.0
    try testing.expectApproxEqAbs(@as(f32, 1.0), result.cos[0], 1e-6); // pos=0, i=0
    try testing.expectApproxEqAbs(@as(f32, 0.0), result.sin[0], 1e-6);
    try testing.expectApproxEqAbs(@as(f32, 1.0), result.cos[1], 1e-6); // pos=0, i=1
    try testing.expectApproxEqAbs(@as(f32, 0.0), result.sin[1], 1e-6);

    // Second half duplicates: cos[2] == cos[0], cos[3] == cos[1]
    try testing.expectApproxEqAbs(result.cos[0], result.cos[2], 1e-6);
    try testing.expectApproxEqAbs(result.cos[1], result.cos[3], 1e-6);

    // At pos=1, angle = 1 * inv_freq != 0 for i=0 at least.
    // inv_freq for i=0: 1.0 / pow(10000, 0/4) = 1.0 / 1.0 = 1.0
    // So angle = 1.0, cos(1) ~ 0.5403, sin(1) ~ 0.8414
    try testing.expectApproxEqAbs(@as(f32, @cos(@as(f32, 1.0))), result.cos[4], 1e-5);
    try testing.expectApproxEqAbs(@as(f32, @sin(@as(f32, 1.0))), result.sin[4], 1e-5);
}
