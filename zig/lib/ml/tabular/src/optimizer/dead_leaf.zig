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

//! Dead-leaf elimination: zero out leaves whose absolute value is below
//! `threshold_fraction * max_abs_leaf_in_tree`. The per-tree max (not the
//! global max) preserves contributions from gradient-boosting correction
//! trees, whose leaves are systematically smaller than the early trees'.
//!
//! Mirrors termite's `optimizer/dead_leaf.go`. After this pass the engine
//! sees the same leaf zeroes; it never short-circuits the walk, so behaviour
//! is bit-identical to a manual leaf rewrite.

const std = @import("std");
const ir = @import("../ir.zig");

pub const Result = struct {
    leaves_eliminated: u32,
};

pub fn run(te: *ir.TreeEnsemble, threshold_fraction: f64) Result {
    var eliminated: u32 = 0;
    if (te.nodes.tree_starts.len == 0) return .{ .leaves_eliminated = 0 };

    var t: u32 = 0;
    while (t < te.nodes.tree_starts.len) : (t += 1) {
        const start: usize = @intCast(te.nodes.tree_starts[t]);
        const end: usize = if (t + 1 < te.nodes.tree_starts.len)
            @intCast(te.nodes.tree_starts[t + 1])
        else
            te.nodes.leaf_value.len;

        var max_abs: f64 = 0;
        var i = start;
        while (i < end) : (i += 1) {
            if (te.nodes.feature_index[i] < 0) {
                const av = @abs(te.nodes.leaf_value[i]);
                if (av > max_abs) max_abs = av;
            }
        }
        if (max_abs == 0) continue;

        const cutoff = threshold_fraction * max_abs;
        i = start;
        while (i < end) : (i += 1) {
            if (te.nodes.feature_index[i] < 0 and @abs(te.nodes.leaf_value[i]) < cutoff) {
                // Avoid eliminating an already-zero leaf as "eliminated".
                if (te.nodes.leaf_value[i] != 0) eliminated += 1;
                te.nodes.leaf_value[i] = 0;
            }
        }
    }

    te.annotations.dead_leaves_eliminated +%= eliminated;
    return .{ .leaves_eliminated = eliminated };
}

test "dead-leaf zeroes leaves below cutoff but preserves the big ones" {
    var feature_index = [_]i32{ 0, -1, -1, 0, -1, -1 };
    var threshold = [_]f64{ 0.5, 0, 0, 0.5, 0, 0 };
    var left = [_]i32{ 1, -1, -1, 4, -1, -1 };
    var right = [_]i32{ 2, -1, -1, 5, -1, -1 };
    var leaf = [_]f64{ 0, 100.0, 0.0005, 0, -50.0, 0.01 };
    var dleft = [_]bool{ true, false, false, true, false, false };
    var starts = [_]i32{ 0, 3 };

    var te: ir.TreeEnsemble = .{
        .num_trees = 2,
        .num_features = 1,
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

    const r = run(&te, 0.001); // anything < 0.001 * max is killed
    try std.testing.expectEqual(@as(u32, 2), r.leaves_eliminated); // 0.0005 in tree 0, 0.01 in tree 1
    try std.testing.expectEqual(@as(f64, 100), leaf[1]);
    try std.testing.expectEqual(@as(f64, 0), leaf[2]);
    try std.testing.expectEqual(@as(f64, -50), leaf[4]);
    try std.testing.expectEqual(@as(f64, 0), leaf[5]);
}
