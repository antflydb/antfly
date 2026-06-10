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

//! LightGBM text-format parser. Handles the canonical output of
//! `booster.save_model(filename)` — a header section followed by repeated
//! `tree=N ... \n\n` blocks.
//!
//! Each tree section has key=value entries; we care about:
//!   num_leaves=N
//!   split_feature=...    (length = num internal nodes)
//!   threshold=...
//!   decision_type=...    (bit 0: default-left, bit 1: missing-as-zero)
//!   left_child=...       (negative = leaf at `~val`)
//!   right_child=...
//!   leaf_value=...       (length = num_leaves)

const std = @import("std");
const ir = @import("../ir.zig");

pub const Error = error{
    OutOfMemory,
    NotLightgbm,
    UnsupportedObjective,
    MalformedTree,
};

pub const Parsed = struct {
    arena: *std.heap.ArenaAllocator,
    model: ir.TabularModel,

    pub fn deinit(self: *Parsed) void {
        const child = self.arena.child_allocator;
        self.arena.deinit();
        child.destroy(self.arena);
    }
};

pub fn parse(parent: std.mem.Allocator, bytes: []const u8) Error!Parsed {
    var arena = try parent.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(parent);
    errdefer {
        arena.deinit();
        parent.destroy(arena);
    }
    const alloc = arena.allocator();

    // Header section runs until the first `Tree=` block.
    const header_end = std.mem.indexOf(u8, bytes, "\nTree=") orelse std.mem.indexOf(u8, bytes, "Tree=") orelse return Error.NotLightgbm;
    const header = bytes[0..header_end];

    // `objective=multiclass num_class:3` — take the first token only.
    const objective_raw = headerValue(header, "objective") orelse return Error.NotLightgbm;
    const objective = firstToken(objective_raw);
    // `max_feature_idx` is 0-based; num_features = max_feature_idx + 1.
    const mfi_str = headerValue(header, "max_feature_idx") orelse "-1";
    const mfi: i64 = std.fmt.parseInt(i64, firstToken(mfi_str), 10) catch return Error.NotLightgbm;
    const num_features_real: u32 = if (mfi < 0) 1 else try i64PlusOneToU32(mfi);
    const num_class_str = headerValue(header, "num_class") orelse "1";
    const num_class_raw: i64 = std.fmt.parseInt(i64, firstToken(num_class_str), 10) catch 1;
    const num_class = try i64ToU32(num_class_raw);

    const map = try mapObjective(objective, num_class);

    // Collect per-tree slices via simple linear split on "\nTree=".
    var trees = std.array_list.Managed([]const u8).init(alloc);
    var it = std.mem.splitSequence(u8, bytes, "\nTree=");
    _ = it.next(); // discard pre-header chunk
    while (it.next()) |chunk| try trees.append(chunk);
    if (trees.items.len == 0) return Error.NotLightgbm;

    // First pass: count nodes (internal nodes + leaves) across all trees.
    var per_tree_node_counts = try alloc.alloc(usize, trees.items.len);
    var total: usize = 0;
    for (trees.items, 0..) |chunk, i| {
        const split_feat = treeValue(chunk, "split_feature") orelse return Error.MalformedTree;
        const left_child_s = treeValue(chunk, "left_child") orelse return Error.MalformedTree;
        const internal = countSpace(split_feat);
        const leaves = countSpace(left_child_s) + 1 - internal; // leaves = #left_child entries that point to a leaf
        // LightGBM: internal nodes have left_child entries; num_leaves = internal + 1.
        _ = leaves;
        const num_leaves_str = treeValue(chunk, "num_leaves") orelse return Error.MalformedTree;
        const num_leaves = std.fmt.parseInt(usize, num_leaves_str, 10) catch return Error.MalformedTree;
        per_tree_node_counts[i] = internal + num_leaves;
        total += per_tree_node_counts[i];
    }

    var feat = try alloc.alloc(i32, total);
    var thr = try alloc.alloc(f64, total);
    var lc = try alloc.alloc(i32, total);
    var rc = try alloc.alloc(i32, total);
    var lv = try alloc.alloc(f64, total);
    var dl = try alloc.alloc(bool, total);
    var starts = try alloc.alloc(i32, trees.items.len);

    var off: usize = 0;
    for (trees.items, 0..) |chunk, ti| {
        starts[ti] = try usizeToI32(off);
        const num_leaves_str = treeValue(chunk, "num_leaves") orelse return Error.MalformedTree;
        const num_leaves = std.fmt.parseInt(usize, num_leaves_str, 10) catch return Error.MalformedTree;

        const split_feat_s = treeValue(chunk, "split_feature") orelse return Error.MalformedTree;
        const threshold_s = treeValue(chunk, "threshold") orelse return Error.MalformedTree;
        const decision_s = treeValue(chunk, "decision_type") orelse return Error.MalformedTree;
        const left_child_s = treeValue(chunk, "left_child") orelse return Error.MalformedTree;
        const right_child_s = treeValue(chunk, "right_child") orelse return Error.MalformedTree;
        const leaf_value_s = treeValue(chunk, "leaf_value") orelse return Error.MalformedTree;

        const num_internal = per_tree_node_counts[ti] - num_leaves;
        const split_feat = try parseI32Tokens(alloc, split_feat_s, num_internal);
        const threshold = try parseF64Tokens(alloc, threshold_s, num_internal);
        const decision = try parseI32Tokens(alloc, decision_s, num_internal);
        const left_child = try parseI32Tokens(alloc, left_child_s, num_internal);
        const right_child = try parseI32Tokens(alloc, right_child_s, num_internal);
        const leaf_value = try parseF64Tokens(alloc, leaf_value_s, num_leaves);

        // Layout: internal nodes [0..num_internal), then leaves [num_internal..total_tree).
        var i: usize = 0;
        while (i < num_internal) : (i += 1) {
            if (split_feat[i] < 0 or @as(u32, @intCast(split_feat[i])) >= num_features_real) return Error.MalformedTree;
            feat[off + i] = split_feat[i];
            thr[off + i] = threshold[i];
            dl[off + i] = (decision[i] & 0x2) != 0; // bit 1 = "default left"
            lc[off + i] = try resolveChild(left_child[i], off, num_internal, num_leaves);
            rc[off + i] = try resolveChild(right_child[i], off, num_internal, num_leaves);
            lv[off + i] = 0;
        }
        var j: usize = 0;
        while (j < num_leaves) : (j += 1) {
            const k = off + num_internal + j;
            feat[k] = -1;
            thr[k] = 0;
            lc[k] = -1;
            rc[k] = -1;
            dl[k] = false;
            lv[k] = leaf_value[j];
        }
        off += per_tree_node_counts[ti];
    }

    const te_ptr = try alloc.create(ir.TreeEnsemble);
    te_ptr.* = .{
        .objective = try alloc.dupe(u8, objective),
        .base_score = 0,
        .num_trees = try usizeToU32(trees.items.len),
        .num_features = num_features_real,
        .max_depth = 0,
        .nodes = .{
            .feature_index = feat,
            .threshold = thr,
            .left_child = lc,
            .right_child = rc,
            .leaf_value = lv,
            .default_left = dl,
            .tree_starts = starts,
        },
    };
    const stages = try alloc.alloc(ir.Stage, 1);
    stages[0] = .{ .type = .tree_ensemble, .tree_ensemble = te_ptr };

    const num_outputs: u32 = if (map.task == .multiclass) num_class else 1;
    return .{
        .arena = arena,
        .model = .{
            .schema_version = ir.schema_version,
            .metadata = .{
                .source_framework = try alloc.dupe(u8, "lightgbm"),
                .task = map.task,
                .num_features = num_features_real,
                .num_classes = if (map.task == .multiclass) num_class else 0,
            },
            .pipeline = stages,
            .output = .{ .activation = map.activation, .num_outputs = num_outputs },
        },
    };
}

const ObjectiveMap = struct { task: ir.TaskType, activation: ir.Activation };

fn mapObjective(name: []const u8, num_class: u32) Error!ObjectiveMap {
    if (std.mem.eql(u8, name, "binary"))
        return .{ .task = .binary_classification, .activation = .sigmoid };
    if (std.mem.eql(u8, name, "regression") or std.mem.eql(u8, name, "regression_l1") or
        std.mem.eql(u8, name, "huber") or std.mem.eql(u8, name, "fair") or
        std.mem.eql(u8, name, "quantile") or std.mem.eql(u8, name, "mape") or
        std.mem.eql(u8, name, "gamma") or std.mem.eql(u8, name, "tweedie"))
        return .{ .task = .regression, .activation = .identity };
    if (std.mem.eql(u8, name, "multiclass") or std.mem.eql(u8, name, "softmax")) {
        if (num_class < 2) return Error.UnsupportedObjective;
        return .{ .task = .multiclass, .activation = .softmax };
    }
    if (std.mem.eql(u8, name, "multiclassova"))
        return .{ .task = .multiclass, .activation = .sigmoid };
    if (std.mem.startsWith(u8, name, "lambdarank") or std.mem.startsWith(u8, name, "rank"))
        return .{ .task = .ranking, .activation = .identity };
    if (std.mem.eql(u8, name, "poisson"))
        return .{ .task = .regression, .activation = .exp };
    return Error.UnsupportedObjective;
}

fn resolveChild(value: i32, off: usize, num_internal: usize, num_leaves: usize) Error!i32 {
    if (value < 0) {
        // LightGBM encodes leaf-references as ~leaf_idx (i.e. -leaf_idx - 1).
        const leaf_idx: usize = @intCast(-(value + 1));
        if (leaf_idx >= num_leaves) return Error.MalformedTree;
        return checkedOffset(off, num_internal + leaf_idx);
    }
    const internal_idx: usize = @intCast(value);
    if (internal_idx >= num_internal) return Error.MalformedTree;
    return checkedOffset(off, internal_idx);
}

fn checkedOffset(off: usize, local: usize) Error!i32 {
    const global = std.math.add(usize, off, local) catch return Error.MalformedTree;
    return std.math.cast(i32, global) orelse Error.MalformedTree;
}

fn i64ToU32(v: i64) Error!u32 {
    return std.math.cast(u32, v) orelse Error.MalformedTree;
}

fn i64PlusOneToU32(v: i64) Error!u32 {
    const plus_one = std.math.add(i64, v, 1) catch return Error.MalformedTree;
    return i64ToU32(plus_one);
}

fn usizeToU32(v: usize) Error!u32 {
    return std.math.cast(u32, v) orelse Error.MalformedTree;
}

fn usizeToI32(v: usize) Error!i32 {
    return std.math.cast(i32, v) orelse Error.MalformedTree;
}

fn headerValue(header: []const u8, key: []const u8) ?[]const u8 {
    var it = std.mem.tokenizeAny(u8, header, "\n");
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, key) and line.len > key.len and line[key.len] == '=') {
            return line[key.len + 1 ..];
        }
    }
    return null;
}

fn treeValue(chunk: []const u8, key: []const u8) ?[]const u8 {
    return headerValue(chunk, key);
}

fn countSpace(s: []const u8) usize {
    var n: usize = 0;
    var prev_space = true;
    for (s) |c| {
        const is_space = (c == ' ' or c == '\t');
        if (prev_space and !is_space) n += 1;
        prev_space = is_space;
    }
    return n;
}

fn parseI32Tokens(alloc: std.mem.Allocator, s: []const u8, count: usize) Error![]i32 {
    var out = try alloc.alloc(i32, count);
    var it = std.mem.tokenizeAny(u8, s, " \t");
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const tok = it.next() orelse return Error.MalformedTree;
        out[i] = std.fmt.parseInt(i32, tok, 10) catch return Error.MalformedTree;
    }
    return out;
}

fn firstToken(s: []const u8) []const u8 {
    var it = std.mem.tokenizeAny(u8, s, " \t");
    return it.next() orelse s;
}

fn parseF64Tokens(alloc: std.mem.Allocator, s: []const u8, count: usize) Error![]f64 {
    var out = try alloc.alloc(f64, count);
    var it = std.mem.tokenizeAny(u8, s, " \t");
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const tok = it.next() orelse return Error.MalformedTree;
        out[i] = std.fmt.parseFloat(f64, tok) catch return Error.MalformedTree;
    }
    return out;
}

test "objective mapping" {
    try std.testing.expectEqual(ir.TaskType.binary_classification, (try mapObjective("binary", 0)).task);
    try std.testing.expectEqual(ir.TaskType.regression, (try mapObjective("regression", 0)).task);
    try std.testing.expectEqual(ir.TaskType.multiclass, (try mapObjective("multiclass", 3)).task);
    try std.testing.expectEqual(ir.TaskType.ranking, (try mapObjective("lambdarank", 0)).task);
}

test "resolveChild rejects malformed child references without narrowing panic" {
    try std.testing.expectError(Error.MalformedTree, resolveChild(2, 0, 2, 1));
    try std.testing.expectError(Error.MalformedTree, resolveChild(-3, 0, 1, 1));
}
