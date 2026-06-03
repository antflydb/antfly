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

//! Tree ensemble inference. The hot path.
//!
//! On-disk thresholds are f64; at load time we pre-bake an f32 parallel array
//! to keep tree comparisons cache-friendly and to enable @Vector SIMD batch
//! traversal. The scalar walk handles single-sample and multiclass paths.
//!
//! NaN handling: per-node `default_left` is consulted whenever the input
//! feature is NaN. We use the IEEE-754 property `nan != nan` for the fast
//! check, avoiding a function call per node.

const std = @import("std");
const ir = @import("ir.zig");

const MAX_DEPTH_SAFETY: u32 = 256; // bounded safety net; real trees stay << this

inline fn simdLaneCount() comptime_int {
    return std.simd.suggestVectorLength(f32) orelse 1;
}

pub const Error = error{
    OutOfMemory,
    FeatureCountMismatch,
    EmptyEnsemble,
};

pub const TreeEngine = struct {
    alloc: std.mem.Allocator,
    objective: []const u8,
    base_score: f64,
    num_trees: u32,
    num_features: u32,
    num_outputs: u32, // 1 for binary/regression/ranking; num_classes for multiclass

    // Mirror of TreeNodes but with thresholds pre-converted to f32.
    feature_index: []const i32,
    threshold32: []const f32,
    left_child: []const i32,
    right_child: []const i32,
    leaf_value: []const f64,
    default_left: []const bool,
    tree_starts: []const i32,

    pub fn init(alloc: std.mem.Allocator, te: ir.TreeEnsemble, num_outputs_hint: u32) Error!TreeEngine {
        if (te.num_trees == 0 or te.nodes.feature_index.len == 0) return Error.EmptyEnsemble;

        const len = te.nodes.threshold.len;
        const thr32 = try alloc.alloc(f32, len);
        for (te.nodes.threshold, 0..) |t, i| thr32[i] = @floatCast(t);

        return .{
            .alloc = alloc,
            .objective = te.objective,
            .base_score = te.base_score,
            .num_trees = te.num_trees,
            .num_features = te.num_features,
            .num_outputs = if (num_outputs_hint > 0) num_outputs_hint else 1,
            .feature_index = te.nodes.feature_index,
            .threshold32 = thr32,
            .left_child = te.nodes.left_child,
            .right_child = te.nodes.right_child,
            .leaf_value = te.nodes.leaf_value,
            .default_left = te.nodes.default_left,
            .tree_starts = te.nodes.tree_starts,
        };
    }

    pub fn deinit(self: *TreeEngine) void {
        self.alloc.free(self.threshold32);
    }

    /// Single-sample, single-output: tree-walk and sum leaf values.
    pub fn predictSingleOne(self: *const TreeEngine, feats: []const f32) f32 {
        if (feats.len != self.num_features) return 0;
        var acc: f64 = self.base_score;
        var t: u32 = 0;
        while (t < self.num_trees) : (t += 1) {
            acc += self.walkOne(t, feats);
        }
        return @floatCast(acc);
    }

    /// Single-sample, multi-output (multiclass): trees come in groups of
    /// `num_outputs` — tree i contributes to class `i % num_outputs`.
    /// Matches the standard XGBoost / LightGBM softprob layout.
    pub fn predictSingleMany(self: *const TreeEngine, feats: []const f32, out: []f32) void {
        if (feats.len != self.num_features) return;
        for (out) |*v| v.* = @floatCast(self.base_score);
        if (self.num_outputs == 0) return;
        var t: u32 = 0;
        while (t < self.num_trees) : (t += 1) {
            const class_idx = t % self.num_outputs;
            out[class_idx] += @floatCast(self.walkOne(t, feats));
        }
    }

    /// Batched, single-output prediction. Walks `L` samples per chunk with
    /// `@Vector(L, f32)` features so threshold comparisons amortise. Falls
    /// back to the scalar walk for the trailing partial chunk.
    pub fn predictBatchOne(self: *const TreeEngine, batch: []const []const f32, out: []f32) void {
        const L = comptime simdLaneCount();
        if (L <= 1 or batch.len < L) {
            for (batch, 0..) |x, i| out[i] = self.predictSingleOne(x);
            return;
        }

        var i: usize = 0;
        while (i + L <= batch.len) : (i += L) {
            self.walkChunk(L, batch[i .. i + L], out[i .. i + L]);
        }
        // Trailing scalar tail.
        while (i < batch.len) : (i += 1) out[i] = self.predictSingleOne(batch[i]);
    }

    fn walkChunk(self: *const TreeEngine, comptime L: comptime_int, rows: []const []const f32, out: []f32) void {
        const VecF = @Vector(L, f32);
        const VecI = @Vector(L, i32);

        // Bounded fallback: if SIMD lanes detect ANY malformed node in this
        // chunk, drop to the scalar walker for the affected rows. Scalar
        // walkOne has defensive guards against -1 / OOB indices.
        const node_count: i32 = @intCast(self.feature_index.len);

        var acc: VecF = @splat(@as(f32, @floatCast(self.base_score)));
        var t: u32 = 0;
        while (t < self.num_trees) : (t += 1) {
            // Per-lane node-index walk for the current tree. Lanes diverge
            // independently; loop until every lane reaches a leaf.
            var node: VecI = @splat(self.tree_starts[t]);
            var leaf: @Vector(L, bool) = @splat(false);
            var safety: u32 = 0;
            var fell_back = false;
            while (safety < MAX_DEPTH_SAFETY) : (safety += 1) {
                // Detect any out-of-range lane: a -1 / overflow has crept in
                // from a malformed converter. Fall back to scalar for the
                // whole chunk in that case.
                inline for (0..L) |k| {
                    if (!leaf[k] and (node[k] < 0 or node[k] >= node_count)) fell_back = true;
                }
                if (fell_back) break;

                // Gather per-lane node attributes.
                var fi_v: VecI = undefined;
                var thr_v: VecF = undefined;
                var lc_v: VecI = undefined;
                var rc_v: VecI = undefined;
                var feat_v: VecF = undefined;
                inline for (0..L) |k| {
                    const idx: usize = @intCast(node[k]);
                    fi_v[k] = self.feature_index[idx];
                    thr_v[k] = self.threshold32[idx];
                    lc_v[k] = self.left_child[idx];
                    rc_v[k] = self.right_child[idx];
                    const fidx = fi_v[k];
                    feat_v[k] = if (fidx >= 0 and fidx < @as(i32, @intCast(rows[k].len))) rows[k][@intCast(fidx)] else 0;
                }

                // NaN follows default_left; non-NaN compares against threshold.
                const is_leaf_now = fi_v < @as(VecI, @splat(0));
                leaf = leaf | is_leaf_now;
                if (@reduce(.And, leaf)) break;

                // Compute go-left per lane (scalar — branch on NaN is cheaper
                // than a vector NaN dance for typical L≤16).
                var next: VecI = node;
                inline for (0..L) |k| {
                    if (!leaf[k]) {
                        const fv = feat_v[k];
                        const idx: usize = @intCast(node[k]);
                        const go_left = if (fv != fv) self.default_left[idx] else fv < thr_v[k];
                        next[k] = if (go_left) lc_v[k] else rc_v[k];
                    }
                }
                node = next;
            }

            if (fell_back) {
                // Restart the entire chunk via scalar walker. Cheap relative
                // to the typical hot path; only triggered by malformed trees.
                inline for (0..L) |k| out[k] = self.predictSingleOne(rows[k]);
                return;
            }

            // Add leaf values to the accumulator.
            var leaf_v: VecF = @splat(0);
            inline for (0..L) |k| {
                const idx: usize = @intCast(node[k]);
                leaf_v[k] = @floatCast(self.leaf_value[idx]);
            }
            acc += leaf_v;
        }

        inline for (0..L) |k| out[k] = acc[k];
    }

    inline fn walkOne(self: *const TreeEngine, tree_idx: u32, feats: []const f32) f64 {
        var i: i32 = self.tree_starts[tree_idx];
        var safety: u32 = 0;
        while (safety < MAX_DEPTH_SAFETY) : (safety += 1) {
            // Defensive against malformed trees produced by buggy converters
            // (validate() catches well-formed IR, but the converters bypass
            // it). A negative index here would otherwise panic on @intCast.
            if (i < 0 or @as(usize, @intCast(i)) >= self.feature_index.len) return 0;
            const idx: usize = @intCast(i);
            const fi = self.feature_index[idx];
            if (fi < 0) return self.leaf_value[idx];
            if (fi >= @as(i32, @intCast(self.num_features))) return 0;
            const fv = feats[@intCast(fi)];
            // `fv != fv` is the IEEE-754 NaN test (fast, no math.h call).
            const go_left = if (fv != fv) self.default_left[idx] else fv < self.threshold32[idx];
            i = if (go_left) self.left_child[idx] else self.right_child[idx];
        }
        // Malformed tree: would loop forever. Return 0 contribution.
        return 0;
    }
};

// ---------------------------------------------------------------------------
// Tests — fixture is a one-tree, single-split stump on feature 0:
//   if x[0] < 0.5 -> -1.0, else -> +1.0
// Tree structure (node order):
//   0: split on f0, thr=0.5, L=1, R=2
//   1: leaf -1
//   2: leaf +1
// ---------------------------------------------------------------------------

fn makeStump() ir.TreeEnsemble {
    return .{
        .objective = "reg:squarederror",
        .base_score = 0,
        .num_trees = 1,
        .num_features = 1,
        .max_depth = 1,
        .nodes = .{
            .feature_index = @constCast(&[_]i32{ 0, -1, -1 }),
            .threshold = @constCast(&[_]f64{ 0.5, 0, 0 }),
            .left_child = @constCast(&[_]i32{ 1, -1, -1 }),
            .right_child = @constCast(&[_]i32{ 2, -1, -1 }),
            .leaf_value = @constCast(&[_]f64{ 0, -1, 1 }),
            .default_left = @constCast(&[_]bool{ true, false, false }),
            .tree_starts = @constCast(&[_]i32{0}),
        },
    };
}

test "single-output stump" {
    var eng = try TreeEngine.init(std.testing.allocator, makeStump(), 1);
    defer eng.deinit();

    try std.testing.expectApproxEqAbs(@as(f32, -1), eng.predictSingleOne(&[_]f32{0.0}), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1), eng.predictSingleOne(&[_]f32{0.7}), 1e-6);
    // Strict `<` comparison: 0.5 is NOT less than 0.5, so we go right (+1).
    try std.testing.expectApproxEqAbs(@as(f32, 1), eng.predictSingleOne(&[_]f32{0.5}), 1e-6);
}

test "NaN follows default_left" {
    var eng = try TreeEngine.init(std.testing.allocator, makeStump(), 1);
    defer eng.deinit();
    const nan = std.math.nan(f32);
    try std.testing.expectApproxEqAbs(@as(f32, -1), eng.predictSingleOne(&[_]f32{nan}), 1e-6);
}

test "batched scalar matches single" {
    var eng = try TreeEngine.init(std.testing.allocator, makeStump(), 1);
    defer eng.deinit();

    const rows = [_][]const f32{
        &[_]f32{0.1},
        &[_]f32{0.9},
        &[_]f32{0.3},
    };
    var out: [3]f32 = undefined;
    eng.predictBatchOne(&rows, &out);
    try std.testing.expectApproxEqAbs(@as(f32, -1), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1), out[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -1), out[2], 1e-6);
}

test "SIMD batch path matches scalar across a 64-row fuzz" {
    var eng = try TreeEngine.init(std.testing.allocator, makeStump(), 1);
    defer eng.deinit();

    var rng = std.Random.DefaultPrng.init(0xc0ffee);
    var random = rng.random();

    const N = 64;
    var rows_buf: [N][1]f32 = undefined;
    var rows: [N][]const f32 = undefined;
    for (0..N) |i| {
        rows_buf[i][0] = random.float(f32);
        rows[i] = &rows_buf[i];
    }

    var simd_out: [N]f32 = undefined;
    var scalar_out: [N]f32 = undefined;
    eng.predictBatchOne(&rows, &simd_out);
    for (0..N) |i| scalar_out[i] = eng.predictSingleOne(rows[i]);

    for (0..N) |i| try std.testing.expectApproxEqAbs(scalar_out[i], simd_out[i], 1e-6);
}
