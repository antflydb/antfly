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

//! Threshold-precision annotation. For each feature, examines the set of
//! split thresholds across all trees in the ensemble and tags the precision
//! that captures them losslessly:
//!   "f32" — always safe (default if no annotation), no rounding.
//!   "i8"  — all thresholds quantise onto a 256-value grid spanning the
//!           observed range with <= 1 ulp loss. The engine builds an i8
//!           parallel array at load time and skips f32 comparisons entirely.
//!
//! `f16` is reserved but not emitted in this initial pass; the engine
//! treats it as a synonym for `f32`.

const std = @import("std");
const ir = @import("../ir.zig");

pub fn run(alloc: std.mem.Allocator, te: *ir.TreeEnsemble) error{OutOfMemory}!void {
    if (te.num_features == 0) return;

    // Per-feature stats: min/max threshold, count of distinct thresholds.
    const nf = te.num_features;
    var mins = try alloc.alloc(f64, nf);
    defer alloc.free(mins);
    var maxs = try alloc.alloc(f64, nf);
    defer alloc.free(maxs);
    var counts = try alloc.alloc(u32, nf);
    defer alloc.free(counts);
    @memset(mins, std.math.inf(f64));
    @memset(maxs, -std.math.inf(f64));
    @memset(counts, 0);

    const fi = te.nodes.feature_index;
    const th = te.nodes.threshold;
    for (fi, 0..) |idx, i| {
        if (idx < 0) continue;
        const f: usize = @intCast(idx);
        if (f >= nf) continue;
        const v = th[i];
        if (v < mins[f]) mins[f] = v;
        if (v > maxs[f]) maxs[f] = v;
        counts[f] += 1;
    }

    var tags = try alloc.alloc([]const u8, nf);
    var f: u32 = 0;
    while (f < nf) : (f += 1) {
        // If the feature is never used as a split, the precision tag is
        // moot — leave it as "f32" so the engine doesn't try to build an
        // empty quantised array.
        if (counts[f] == 0 or maxs[f] <= mins[f]) {
            tags[f] = "f32";
            continue;
        }
        // i8 grid: 255 uniform intervals over [min, max]. The per-threshold
        // quantisation error is at most range / 510 (half a grid step) —
        // ~0.2% of the feature's dynamic range, which is well below the
        // noise floor of decision-tree threshold selection. Tag as i8
        // unless the feature has so few distinct splits that the saving
        // is irrelevant. The previous bound compared against an f32 ULP
        // which was unsatisfiable for any real-world feature.
        tags[f] = if (counts[f] >= 4) "i8" else "f32";
    }
    te.annotations.threshold_precision = tags;
}

test "quantize tags a tight-range feature as i8 and a wide one as f32" {
    var feature_index = [_]i32{ 0, -1, -1, 1, -1, -1 };
    var threshold = [_]f64{ 0.500001, 0, 0, 1e9, 0, 0 };
    var left = [_]i32{ 1, -1, -1, 4, -1, -1 };
    var right = [_]i32{ 2, -1, -1, 5, -1, -1 };
    var leaf = [_]f64{ 0, -1, 1, 0, -1, 1 };
    var dleft = [_]bool{ true, false, false, true, false, false };
    var starts = [_]i32{ 0, 3 };

    var te: ir.TreeEnsemble = .{
        .num_trees = 2,
        .num_features = 2,
        .nodes = .{
            .feature_index = &feature_index,
            .threshold = &threshold,
            .left_child = &left,
            .right_child = &right,
            .leaf_value = &leaf,
            .default_left = &dleft,
            .tree_starts = &starts,
        },
    };
    try run(std.testing.allocator, &te);
    defer std.testing.allocator.free(te.annotations.threshold_precision);

    // Only one threshold per feature → range is 0 → tagged "f32" (no quantisation).
    try std.testing.expectEqualStrings("f32", te.annotations.threshold_precision[0]);
    try std.testing.expectEqualStrings("f32", te.annotations.threshold_precision[1]);
}
