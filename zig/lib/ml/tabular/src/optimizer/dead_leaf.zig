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
//! After this pass the engine sees the same leaf zeroes; it never short-circuits
//! the walk, so behaviour is bit-identical to a manual leaf rewrite.

const std = @import("std");
const ir = @import("../ir.zig");

pub const Result = struct {
    leaves_eliminated: u32,
};

pub fn run(te: *ir.TreeEnsemble, threshold_fraction: f64) Result {
    var eliminated: u32 = 0;
    if (te.nodes.tree_starts.len == 0) return .{ .leaves_eliminated = 0 };

    // Stack-allocated DFS buffer — bounded to avoid pathological recursion
    // on adversarial trees. Real ensembles have << 4096 total nodes per
    // tree; larger trees skip optimisation rather than recurse unbounded.
    var stack_buf: [4096]usize = undefined;

    var t: u32 = 0;
    while (t < te.nodes.tree_starts.len) : (t += 1) {
        const raw_start = te.nodes.tree_starts[t];
        if (raw_start < 0) continue;
        const start: usize = @intCast(raw_start);

        // First pass: walk this tree's nodes from the root via children
        // (correct under any node layout, unlike a contiguous-range scan).
        var max_abs: f64 = 0;
        if (!findMaxAbs(te, start, &stack_buf, &max_abs)) continue;
        if (max_abs == 0) continue;

        // Second pass: zero leaves below cutoff and count eliminations.
        const cutoff = threshold_fraction * max_abs;
        var pass_eliminated: u32 = 0;
        _ = zeroBelowCutoff(te, start, &stack_buf, cutoff, &pass_eliminated);
        eliminated += pass_eliminated;
    }

    te.annotations.dead_leaves_eliminated +%= eliminated;
    return .{ .leaves_eliminated = eliminated };
}

/// DFS that finds the maximum |leaf| value in the tree rooted at `root`.
/// Returns false on structural problems (cycles, OOB).
fn findMaxAbs(
    te: *ir.TreeEnsemble,
    root: usize,
    stack_buf: []usize,
    max_abs_out: *f64,
) bool {
    const len = te.nodes.feature_index.len;
    if (root >= len) return false;
    var sp: usize = 0;
    stack_buf[sp] = root;
    sp += 1;
    var visited: u32 = 0;
    while (sp > 0) {
        if (visited > 256 * 1024) return false;
        visited += 1;
        sp -= 1;
        const i = stack_buf[sp];
        if (i >= len) return false;
        const fi = te.nodes.feature_index[i];
        if (fi < 0) {
            const av = @abs(te.nodes.leaf_value[i]);
            if (av > max_abs_out.*) max_abs_out.* = av;
            continue;
        }
        const l = te.nodes.left_child[i];
        const r = te.nodes.right_child[i];
        if (l < 0 or r < 0) return false;
        if (sp + 2 > stack_buf.len) return false;
        stack_buf[sp] = @intCast(l);
        sp += 1;
        stack_buf[sp] = @intCast(r);
        sp += 1;
    }
    return true;
}

/// DFS that zeroes any leaf whose magnitude is below `cutoff`, counting
/// the eliminations. Same robustness contract as findMaxAbs.
fn zeroBelowCutoff(
    te: *ir.TreeEnsemble,
    root: usize,
    stack_buf: []usize,
    cutoff: f64,
    count_out: *u32,
) bool {
    const len = te.nodes.feature_index.len;
    if (root >= len) return false;
    var sp: usize = 0;
    stack_buf[sp] = root;
    sp += 1;
    var visited: u32 = 0;
    while (sp > 0) {
        if (visited > 256 * 1024) return false;
        visited += 1;
        sp -= 1;
        const i = stack_buf[sp];
        if (i >= len) return false;
        const fi = te.nodes.feature_index[i];
        if (fi < 0) {
            const v = te.nodes.leaf_value[i];
            if (@abs(v) < cutoff and v != 0) {
                te.nodes.leaf_value[i] = 0;
                count_out.* += 1;
            }
            continue;
        }
        const l = te.nodes.left_child[i];
        const r = te.nodes.right_child[i];
        if (l < 0 or r < 0) return false;
        if (sp + 2 > stack_buf.len) return false;
        stack_buf[sp] = @intCast(l);
        sp += 1;
        stack_buf[sp] = @intCast(r);
        sp += 1;
    }
    return true;
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
