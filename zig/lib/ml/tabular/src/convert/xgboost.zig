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

//! XGBoost JSON model parser. Consumes the output of `booster.save_model(*.json)`
//! and produces an IR TreeEnsemble.
//!
//! XGBoost JSON layout (relevant fields):
//!   learner:
//!     learner_model_param: { base_score, num_feature, num_class }
//!     objective: { name: "binary:logistic" | "reg:squarederror" | "multi:softprob" | "rank:..." }
//!     gradient_booster:
//!       name: "gbtree"
//!       model:
//!         trees: [{
//!           split_indices:    [...]  per-node feature
//!           split_conditions: [...]  per-node threshold
//!           left_children:    [...]  -1 = leaf
//!           right_children:   [...]
//!           base_weights:     [...]  per-node value (leaf only meaningful)
//!           default_left:     [...]  bool per node
//!         }, ...]

const std = @import("std");
const ir = @import("../ir.zig");

pub const Error = error{
    OutOfMemory,
    NotXgboost,
    UnsupportedObjective,
    MalformedTree,
    InvalidJson,
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

    var root = std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{}) catch return Error.InvalidJson;
    if (root != .object) return Error.NotXgboost;
    const learner_v = root.object.get("learner") orelse return Error.NotXgboost;
    if (learner_v != .object) return Error.NotXgboost;

    const lmp = getObject(learner_v, "learner_model_param") orelse return Error.NotXgboost;
    const objective_obj = getObject(learner_v, "objective") orelse return Error.NotXgboost;
    const objective = (objective_obj.get("name") orelse return Error.NotXgboost);
    if (objective != .string) return Error.InvalidJson;

    const num_feature = try parseU32Like(lmp.get("num_feature") orelse return Error.NotXgboost);
    const num_class_raw = try parseU32Like(lmp.get("num_class") orelse std.json.Value{ .string = "0" });
    const base_score = try parseBaseScore(lmp.get("base_score") orelse std.json.Value{ .string = "0.5" });

    const map = try mapObjective(objective.string, num_class_raw);

    const gb = getObject(learner_v, "gradient_booster") orelse return Error.NotXgboost;
    const model_obj = getObject(.{ .object = gb.* }, "model") orelse return Error.NotXgboost;
    _ = model_obj;
    const gb_model_v = gb.get("model") orelse return Error.NotXgboost;
    if (gb_model_v != .object) return Error.NotXgboost;
    const trees_v = gb_model_v.object.get("trees") orelse return Error.NotXgboost;
    if (trees_v != .array) return Error.NotXgboost;
    if (trees_v.array.items.len == 0) return Error.MalformedTree;

    var ensemble = try buildEnsemble(alloc, trees_v.array.items, num_feature, base_score);
    ensemble.objective = try alloc.dupe(u8, objective.string);

    const te_ptr = try alloc.create(ir.TreeEnsemble);
    te_ptr.* = ensemble;
    const stages = try alloc.alloc(ir.Stage, 1);
    stages[0] = .{ .type = .tree_ensemble, .tree_ensemble = te_ptr };

    const num_outputs: u32 = if (map.task == .multiclass) num_class_raw else 1;
    return .{
        .arena = arena,
        .model = .{
            .schema_version = ir.schema_version,
            .metadata = .{
                .name = "",
                .source_framework = try alloc.dupe(u8, "xgboost"),
                .task = map.task,
                .num_features = num_feature,
                .num_classes = if (map.task == .multiclass) num_class_raw else 0,
            },
            .pipeline = stages,
            .output = .{ .activation = map.activation, .num_outputs = num_outputs },
        },
    };
}

const ObjectiveMap = struct { task: ir.TaskType, activation: ir.Activation };

fn mapObjective(name: []const u8, num_class: u32) Error!ObjectiveMap {
    if (std.mem.eql(u8, name, "binary:logistic") or std.mem.eql(u8, name, "binary:logitraw"))
        return .{ .task = .binary_classification, .activation = .sigmoid };
    if (std.mem.eql(u8, name, "reg:squarederror") or std.mem.eql(u8, name, "reg:linear") or
        std.mem.eql(u8, name, "reg:absoluteerror") or std.mem.eql(u8, name, "reg:pseudohubererror"))
        return .{ .task = .regression, .activation = .identity };
    if (std.mem.eql(u8, name, "multi:softprob") or std.mem.eql(u8, name, "multi:softmax")) {
        if (num_class < 2) return Error.UnsupportedObjective;
        return .{ .task = .multiclass, .activation = .softmax };
    }
    if (std.mem.startsWith(u8, name, "rank:"))
        return .{ .task = .ranking, .activation = .identity };
    if (std.mem.eql(u8, name, "count:poisson"))
        return .{ .task = .regression, .activation = .exp };
    return Error.UnsupportedObjective;
}

fn buildEnsemble(
    alloc: std.mem.Allocator,
    trees: []std.json.Value,
    num_features: u32,
    base_score: f64,
) Error!ir.TreeEnsemble {
    // First pass: count total nodes across all trees.
    var total: usize = 0;
    for (trees) |t| {
        if (t != .object) return Error.MalformedTree;
        const si = t.object.get("split_indices") orelse return Error.MalformedTree;
        if (si != .array) return Error.MalformedTree;
        total += si.array.items.len;
    }

    var feat = try alloc.alloc(i32, total);
    var thr = try alloc.alloc(f64, total);
    var lc = try alloc.alloc(i32, total);
    var rc = try alloc.alloc(i32, total);
    var lv = try alloc.alloc(f64, total);
    var dl = try alloc.alloc(bool, total);
    var starts = try alloc.alloc(i32, trees.len);

    var off: usize = 0;
    for (trees, 0..) |t, ti| {
        starts[ti] = try usizeToI32(off);
        const obj = t.object;
        const si = (obj.get("split_indices") orelse return Error.MalformedTree).array.items;
        const sc = (obj.get("split_conditions") orelse return Error.MalformedTree).array.items;
        const li = (obj.get("left_children") orelse return Error.MalformedTree).array.items;
        const ri = (obj.get("right_children") orelse return Error.MalformedTree).array.items;
        const bw = (obj.get("base_weights") orelse return Error.MalformedTree).array.items;
        // default_left is optional in some dumps; default to true.
        const dl_arr: ?[]std.json.Value = if (obj.get("default_left")) |v| (if (v == .array) v.array.items else null) else null;

        if (si.len != sc.len or si.len != li.len or si.len != ri.len or si.len != bw.len) return Error.MalformedTree;

        var i: usize = 0;
        while (i < si.len) : (i += 1) {
            const left_local = try jsonAsI64(li[i]);
            const right_local = try jsonAsI64(ri[i]);
            const is_leaf = left_local < 0 and right_local < 0;
            const f_idx = try jsonAsI64(si[i]);
            if (is_leaf) {
                feat[off + i] = -1;
                lv[off + i] = try jsonAsF64(bw[i]);
                thr[off + i] = 0;
                lc[off + i] = -1;
                rc[off + i] = -1;
            } else {
                const feature_index = try i64ToU32(f_idx);
                if (feature_index >= num_features) return Error.MalformedTree;
                feat[off + i] = try u32ToI32(feature_index);
                thr[off + i] = try jsonAsF64(sc[i]);
                lc[off + i] = try resolveChild(off, left_local);
                rc[off + i] = try resolveChild(off, right_local);
                lv[off + i] = 0;
            }
            dl[off + i] = if (dl_arr) |arr| (if (i < arr.len) jsonAsBool(arr[i]) else true) else true;
        }
        off += si.len;
    }

    return .{
        .objective = "",
        .base_score = base_score,
        .num_trees = try usizeToU32(trees.len),
        .num_features = num_features,
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
}

// ---------------------------------------------------------------------------
// XGBoost stores numeric model params as JSON strings (e.g. "27", "0.5"); fall
// back to numeric values too for forward compatibility.
// ---------------------------------------------------------------------------

fn parseI64Like(v: std.json.Value) Error!i64 {
    return switch (v) {
        .string => |s| std.fmt.parseInt(i64, s, 10) catch return Error.MalformedTree,
        .integer => |i| i,
        .float => |f| {
            if (!std.math.isFinite(f)) return Error.MalformedTree;
            if (f < @as(f64, @floatFromInt(std.math.minInt(i64))) or
                f > @as(f64, @floatFromInt(std.math.maxInt(i64))))
                return Error.MalformedTree;
            return @intFromFloat(f);
        },
        else => Error.MalformedTree,
    };
}

fn parseU32Like(v: std.json.Value) Error!u32 {
    return i64ToU32(try parseI64Like(v));
}

fn parseF64Like(v: std.json.Value) Error!f64 {
    const out: f64 = switch (v) {
        .string => |s| std.fmt.parseFloat(f64, s) catch return Error.MalformedTree,
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => return Error.MalformedTree,
    };
    if (!std.math.isFinite(out)) return Error.MalformedTree;
    return out;
}

fn parseBaseScore(v: std.json.Value) Error!f64 {
    if (v == .string) {
        const s = std.mem.trim(u8, v.string, " \t\r\n");
        if (std.mem.startsWith(u8, s, "[")) return parseUniformBaseScoreVector(s);
    }
    return parseF64Like(v);
}

fn parseUniformBaseScoreVector(s: []const u8) Error!f64 {
    if (s.len < 2 or s[0] != '[' or s[s.len - 1] != ']') return Error.MalformedTree;
    const inner = s[1 .. s.len - 1];
    var it = std.mem.splitScalar(u8, inner, ',');
    var first: ?f64 = null;
    while (it.next()) |raw| {
        const tok = std.mem.trim(u8, raw, " \t\r\n");
        if (tok.len == 0) return Error.MalformedTree;
        const v = std.fmt.parseFloat(f64, tok) catch return Error.MalformedTree;
        if (!std.math.isFinite(v)) return Error.MalformedTree;
        if (first) |f| {
            if (@abs(v - f) > 1e-12) return Error.UnsupportedObjective;
        } else {
            first = v;
        }
    }
    return first orelse Error.MalformedTree;
}

fn jsonAsI64(v: std.json.Value) Error!i64 {
    return parseI64Like(v);
}
fn jsonAsF64(v: std.json.Value) Error!f64 {
    return parseF64Like(v);
}
fn jsonAsBool(v: std.json.Value) bool {
    return switch (v) {
        .bool => |b| b,
        .integer => |i| i != 0,
        else => true,
    };
}

fn getObject(v: std.json.Value, key: []const u8) ?*std.json.ObjectMap {
    if (v != .object) return null;
    const child = v.object.getPtr(key) orelse return null;
    if (child.* != .object) return null;
    return &child.object;
}

fn i64ToU32(v: i64) Error!u32 {
    return std.math.cast(u32, v) orelse Error.MalformedTree;
}

fn u32ToI32(v: u32) Error!i32 {
    return std.math.cast(i32, v) orelse Error.MalformedTree;
}

fn usizeToU32(v: usize) Error!u32 {
    return std.math.cast(u32, v) orelse Error.MalformedTree;
}

fn usizeToI32(v: usize) Error!i32 {
    return std.math.cast(i32, v) orelse Error.MalformedTree;
}

fn resolveChild(off: usize, local: i64) Error!i32 {
    if (local < 0) return -1;
    const off_i64 = std.math.cast(i64, off) orelse return Error.MalformedTree;
    const global = std.math.add(i64, off_i64, local) catch return Error.MalformedTree;
    return std.math.cast(i32, global) orelse Error.MalformedTree;
}

test "objective mapping covers core cases" {
    try std.testing.expectEqual(ir.TaskType.binary_classification, (try mapObjective("binary:logistic", 0)).task);
    try std.testing.expectEqual(ir.TaskType.regression, (try mapObjective("reg:squarederror", 0)).task);
    try std.testing.expectEqual(ir.TaskType.multiclass, (try mapObjective("multi:softprob", 3)).task);
    try std.testing.expectEqual(ir.TaskType.ranking, (try mapObjective("rank:ndcg", 0)).task);
    try std.testing.expectError(Error.UnsupportedObjective, mapObjective("survival:cox", 0));
    try std.testing.expectError(Error.UnsupportedObjective, mapObjective("multi:softprob", 1));
}

test "hostile xgboost integer metadata is rejected without narrowing panic" {
    const bad =
        \\{"learner":{
        \\  "learner_model_param":{"num_feature":"4294967296","num_class":"0","base_score":"0.5"},
        \\  "objective":{"name":"reg:squarederror"},
        \\  "gradient_booster":{"model":{"trees":[]}}
        \\}}
    ;
    try std.testing.expectError(Error.MalformedTree, parse(std.testing.allocator, bad));
}

test "xgboost vector base score accepts uniform class bias" {
    try std.testing.expectApproxEqAbs(@as(f64, 0), try parseBaseScore(.{ .string = "[0E0,0E0,0E0]" }), 1e-12);
    try std.testing.expectError(Error.UnsupportedObjective, parseBaseScore(.{ .string = "[0,1,0]" }));
}
