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

//! tabular_model.json reader. Parses the dynamic `std.json.Value` tree
//! produced by `std.json.parseFromSlice` and projects it into our typed IR,
//! validating along the way.

const std = @import("std");
const ir = @import("ir.zig");

pub const filename = "tabular_model.json";

pub const LoadError = error{
    InvalidJson,
    UnsupportedSchemaVersion,
    MissingMetadata,
    MissingPipeline,
    EmptyPipeline,
    MultipleModelStages,
    NoModelStage,
    UnknownStageType,
    UnknownTaskType,
    UnknownActivation,
    UnknownScalerMethod,
    UnknownImputerStrategy,
    UnknownSvmKernel,
    MalformedTreeEnsemble,
    InconsistentTreeNodeArrays,
    OutOfBoundsTreeIndex,
    FeatureIndexOutOfRange,
    MalformedLinearModel,
    MalformedSvmModel,
    MalformedScaler,
    MalformedImputer,
    NumFeaturesMustBePositive,
    NumOutputsMustBePositive,
    OutOfMemory,
};

/// Caller owns the returned `Loaded.arena` and must call `Loaded.deinit()`.
/// After `takeArena` the arena is null and `deinit` is a safe no-op.
pub const Loaded = struct {
    arena: ?*std.heap.ArenaAllocator,
    model: ir.TabularModel,

    pub fn deinit(self: *Loaded) void {
        if (self.arena) |a| {
            const child_alloc = a.child_allocator;
            a.deinit();
            child_alloc.destroy(a);
            self.arena = null;
        }
    }

    /// Hand off ownership of the arena to a caller (e.g. the registry's
    /// `Predictor.initOwned`). After this returns the Loaded is "empty":
    /// `model` is still readable but its backing memory is owned by the new
    /// arena holder, and `deinit` is a safe no-op.
    pub fn takeArena(self: *Loaded) *std.heap.ArenaAllocator {
        const a = self.arena.?;
        self.arena = null;
        return a;
    }
};

pub fn parseFromSlice(parent_alloc: std.mem.Allocator, bytes: []const u8) LoadError!Loaded {
    var arena = try parent_alloc.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(parent_alloc);
    errdefer {
        arena.deinit();
        parent_alloc.destroy(arena);
    }
    const alloc = arena.allocator();

    var parsed = std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{}) catch return LoadError.InvalidJson;
    const model = try projectModel(alloc, &parsed);
    try validate(model);
    return .{ .arena = arena, .model = model };
}

// ---------------------------------------------------------------------------
// Projection from std.json.Value → typed IR. We do the projection by hand
// (rather than std.json.parseFromValue with a struct schema) because the
// schema is permissive: optional fields, default values, the tagged-union
// Stage representation, etc.
// ---------------------------------------------------------------------------

fn projectModel(alloc: std.mem.Allocator, v: *std.json.Value) LoadError!ir.TabularModel {
    if (v.* != .object) return LoadError.InvalidJson;
    const root = v.object;

    var m: ir.TabularModel = .{};

    if (root.get("schema_version")) |sv| {
        m.schema_version = try castInt(u32, try expectInt(sv));
    }
    if (m.schema_version != ir.schema_version) return LoadError.UnsupportedSchemaVersion;

    if (root.get("metadata")) |md| {
        m.metadata = try projectMetadata(alloc, md);
    } else return LoadError.MissingMetadata;

    if (root.get("output")) |o| {
        m.output = try projectOutput(o);
    } else {
        m.output = .{ .activation = .identity, .num_outputs = 1 };
    }

    if (root.get("pipeline")) |p| {
        m.pipeline = try projectPipeline(alloc, p);
    } else return LoadError.MissingPipeline;

    return m;
}

fn projectMetadata(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.Metadata {
    if (v != .object) return LoadError.InvalidJson;
    const o = v.object;

    var md: ir.Metadata = .{};
    if (o.get("name")) |x| md.name = try expectString(x);
    if (o.get("source_framework")) |x| md.source_framework = try expectString(x);
    if (o.get("source_version")) |x| md.source_version = try expectString(x);
    if (o.get("task")) |x| {
        md.task = ir.taskTypeFromString(try expectString(x)) orelse return LoadError.UnknownTaskType;
    }
    if (o.get("num_features")) |x| md.num_features = try castInt(u32, try expectInt(x));
    if (o.get("num_classes")) |x| md.num_classes = try castInt(u32, try expectInt(x));
    if (o.get("created_at")) |x| md.created_at = try expectString(x);

    if (o.get("feature_names")) |x| {
        if (x != .array) return LoadError.InvalidJson;
        var out = try alloc.alloc([]const u8, x.array.items.len);
        for (x.array.items, 0..) |item, i| out[i] = try expectString(item);
        md.feature_names = out;
    }
    if (o.get("feature_types")) |x| {
        if (x != .array) return LoadError.InvalidJson;
        var out = try alloc.alloc([]const u8, x.array.items.len);
        for (x.array.items, 0..) |item, i| out[i] = try expectString(item);
        md.feature_types = out;
    }

    if (md.num_features == 0) return LoadError.NumFeaturesMustBePositive;
    return md;
}

fn projectOutput(v: std.json.Value) LoadError!ir.OutputCfg {
    if (v != .object) return LoadError.InvalidJson;
    const o = v.object;
    var out: ir.OutputCfg = .{};
    if (o.get("activation")) |x| {
        out.activation = ir.activationFromString(try expectString(x)) orelse return LoadError.UnknownActivation;
    }
    if (o.get("num_outputs")) |x| out.num_outputs = try castInt(u32, try expectInt(x));
    if (out.num_outputs == 0) return LoadError.NumOutputsMustBePositive;
    return out;
}

fn projectPipeline(alloc: std.mem.Allocator, v: std.json.Value) LoadError![]const ir.Stage {
    if (v != .array) return LoadError.InvalidJson;
    if (v.array.items.len == 0) return LoadError.EmptyPipeline;

    var out = try alloc.alloc(ir.Stage, v.array.items.len);
    for (v.array.items, 0..) |s, i| {
        out[i] = try projectStage(alloc, s);
    }
    return out;
}

fn projectStage(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.Stage {
    if (v != .object) return LoadError.InvalidJson;
    const o = v.object;

    const type_v = o.get("type") orelse return LoadError.UnknownStageType;
    const tag = ir.stageTagFromString(try expectString(type_v)) orelse return LoadError.UnknownStageType;

    var stage: ir.Stage = .{ .type = tag };
    switch (tag) {
        .tree_ensemble => {
            const t = o.get("tree_ensemble") orelse return LoadError.MalformedTreeEnsemble;
            const te = try alloc.create(ir.TreeEnsemble);
            te.* = try projectTreeEnsemble(alloc, t);
            stage.tree_ensemble = te;
        },
        .linear => {
            const t = o.get("linear") orelse return LoadError.MalformedLinearModel;
            const lm = try alloc.create(ir.LinearModel);
            lm.* = try projectLinear(alloc, t);
            stage.linear = lm;
        },
        .svm => {
            const t = o.get("svm") orelse return LoadError.MalformedSvmModel;
            const sm = try alloc.create(ir.SVMModel);
            sm.* = try projectSvm(alloc, t);
            stage.svm = sm;
        },
        .scaler => {
            const t = o.get("scaler") orelse return LoadError.MalformedScaler;
            const sc = try alloc.create(ir.Scaler);
            sc.* = try projectScaler(alloc, t);
            stage.scaler = sc;
        },
        .imputer => {
            const t = o.get("imputer") orelse return LoadError.MalformedImputer;
            const im = try alloc.create(ir.Imputer);
            im.* = try projectImputer(alloc, t);
            stage.imputer = im;
        },
    }
    return stage;
}

fn projectTreeEnsemble(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.TreeEnsemble {
    if (v != .object) return LoadError.MalformedTreeEnsemble;
    const o = v.object;

    var te: ir.TreeEnsemble = .{};
    if (o.get("objective")) |x| te.objective = try expectString(x);
    if (o.get("base_score")) |x| te.base_score = try expectFloat(x);
    if (o.get("num_trees")) |x| te.num_trees = try castInt(u32, try expectInt(x));
    if (o.get("num_features")) |x| te.num_features = try castInt(u32, try expectInt(x));
    if (o.get("max_depth")) |x| te.max_depth = try castInt(u32, try expectInt(x));

    const nodes_v = o.get("nodes") orelse return LoadError.MalformedTreeEnsemble;
    te.nodes = try projectTreeNodes(alloc, nodes_v);

    if (o.get("annotations")) |x| te.annotations = try projectTreeAnnotations(alloc, x);

    return te;
}

fn projectTreeNodes(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.TreeNodes {
    if (v != .object) return LoadError.MalformedTreeEnsemble;
    const o = v.object;

    var n: ir.TreeNodes = .{};
    if (o.get("feature_index")) |x| n.feature_index = try toI32Slice(alloc, x);
    if (o.get("threshold")) |x| n.threshold = try toF64Slice(alloc, x);
    if (o.get("left_child")) |x| n.left_child = try toI32Slice(alloc, x);
    if (o.get("right_child")) |x| n.right_child = try toI32Slice(alloc, x);
    if (o.get("leaf_value")) |x| n.leaf_value = try toF64Slice(alloc, x);
    if (o.get("default_left")) |x| n.default_left = try toBoolSlice(alloc, x);
    if (o.get("tree_starts")) |x| n.tree_starts = try toI32Slice(alloc, x);
    return n;
}

fn projectTreeAnnotations(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.TreeAnnotations {
    if (v != .object) return .{};
    const o = v.object;
    var a: ir.TreeAnnotations = .{};
    if (o.get("threshold_precision")) |x| {
        if (x != .array) return LoadError.InvalidJson;
        var out = try alloc.alloc([]const u8, x.array.items.len);
        for (x.array.items, 0..) |item, i| out[i] = try expectString(item);
        a.threshold_precision = out;
    }
    if (o.get("dead_leaves_eliminated")) |x| a.dead_leaves_eliminated = try castInt(u32, try expectInt(x));
    if (o.get("branch_order")) |x| a.branch_order = try expectString(x);
    return a;
}

fn projectLinear(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.LinearModel {
    if (v != .object) return LoadError.MalformedLinearModel;
    const o = v.object;
    var lm: ir.LinearModel = .{};
    if (o.get("weights")) |x| lm.weights = try toF64Matrix(alloc, x);
    if (o.get("biases")) |x| lm.biases = try toF64Slice(alloc, x);
    return lm;
}

fn projectSvm(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.SVMModel {
    if (v != .object) return LoadError.MalformedSvmModel;
    const o = v.object;
    var sm: ir.SVMModel = .{};
    if (o.get("kernel")) |x| {
        sm.kernel = ir.svmKernelFromString(try expectString(x)) orelse return LoadError.UnknownSvmKernel;
    }
    if (o.get("support_vectors")) |x| sm.support_vectors = try toF64Matrix(alloc, x);
    if (o.get("dual_coefficients")) |x| sm.dual_coefficients = try toF64Matrix(alloc, x);
    if (o.get("intercept")) |x| sm.intercept = try toF64Slice(alloc, x);
    if (o.get("gamma")) |x| sm.gamma = try expectFloat(x);
    if (o.get("coef0")) |x| sm.coef0 = try expectFloat(x);
    if (o.get("degree")) |x| sm.degree = try castInt(u32, try expectInt(x));
    return sm;
}

fn projectScaler(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.Scaler {
    if (v != .object) return LoadError.MalformedScaler;
    const o = v.object;
    var sc: ir.Scaler = .{};
    if (o.get("method")) |x| {
        sc.method = ir.scalerMethodFromString(try expectString(x)) orelse return LoadError.UnknownScalerMethod;
    }
    if (o.get("center")) |x| sc.center = try toF64Slice(alloc, x);
    if (o.get("scale")) |x| sc.scale = try toF64Slice(alloc, x);
    if (o.get("min")) |x| sc.min = try toF64Slice(alloc, x);
    if (o.get("max")) |x| sc.max = try toF64Slice(alloc, x);
    return sc;
}

fn projectImputer(alloc: std.mem.Allocator, v: std.json.Value) LoadError!ir.Imputer {
    if (v != .object) return LoadError.MalformedImputer;
    const o = v.object;
    var im: ir.Imputer = .{};
    if (o.get("strategy")) |x| {
        im.strategy = ir.imputerStrategyFromString(try expectString(x)) orelse return LoadError.UnknownImputerStrategy;
    }
    if (o.get("fill_values")) |x| im.fill_values = try toF64Slice(alloc, x);
    return im;
}

// ---------------------------------------------------------------------------
// Primitive coercions
// ---------------------------------------------------------------------------

fn expectString(v: std.json.Value) LoadError![]const u8 {
    return switch (v) {
        .string => |s| s,
        else => LoadError.InvalidJson,
    };
}

fn expectInt(v: std.json.Value) LoadError!i64 {
    return switch (v) {
        .integer => |i| i,
        .float => |f| blk: {
            // Reject NaN/Inf and out-of-range floats — @intFromFloat is
            // undefined behaviour otherwise.
            if (!std.math.isFinite(f)) return LoadError.InvalidJson;
            if (f < @as(f64, @floatFromInt(std.math.minInt(i64))) or
                f > @as(f64, @floatFromInt(std.math.maxInt(i64))))
                return LoadError.InvalidJson;
            break :blk @intFromFloat(f);
        },
        else => LoadError.InvalidJson,
    };
}

fn expectFloat(v: std.json.Value) LoadError!f64 {
    return switch (v) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => LoadError.InvalidJson,
    };
}

/// Safely narrow an i64 to a target signed/unsigned int type, returning a
/// LoadError on overflow instead of panicking. Used pervasively to reject
/// hostile input that would otherwise abort the inference process.
fn castInt(comptime T: type, v: i64) LoadError!T {
    return std.math.cast(T, v) orelse LoadError.InvalidJson;
}

fn toI32Slice(alloc: std.mem.Allocator, v: std.json.Value) LoadError![]i32 {
    if (v != .array) return LoadError.InvalidJson;
    var out = try alloc.alloc(i32, v.array.items.len);
    for (v.array.items, 0..) |x, i| out[i] = try castInt(i32, try expectInt(x));
    return out;
}

fn toF64Slice(alloc: std.mem.Allocator, v: std.json.Value) LoadError![]f64 {
    if (v != .array) return LoadError.InvalidJson;
    var out = try alloc.alloc(f64, v.array.items.len);
    for (v.array.items, 0..) |x, i| out[i] = try expectFloat(x);
    return out;
}

fn toBoolSlice(alloc: std.mem.Allocator, v: std.json.Value) LoadError![]bool {
    if (v != .array) return LoadError.InvalidJson;
    var out = try alloc.alloc(bool, v.array.items.len);
    for (v.array.items, 0..) |x, i| {
        out[i] = switch (x) {
            .bool => |b| b,
            .integer => |n| n != 0,
            else => return LoadError.InvalidJson,
        };
    }
    return out;
}

fn toF64Matrix(alloc: std.mem.Allocator, v: std.json.Value) LoadError![]const []const f64 {
    if (v != .array) return LoadError.InvalidJson;
    var out = try alloc.alloc([]const f64, v.array.items.len);
    for (v.array.items, 0..) |row, i| out[i] = try toF64Slice(alloc, row);
    return out;
}

// ---------------------------------------------------------------------------
// Validation for Antfly Inference tabular models.
// ---------------------------------------------------------------------------

pub fn validate(m: ir.TabularModel) LoadError!void {
    if (m.schema_version != ir.schema_version) return LoadError.UnsupportedSchemaVersion;
    if (m.metadata.num_features == 0) return LoadError.NumFeaturesMustBePositive;
    if (m.output.num_outputs == 0) return LoadError.NumOutputsMustBePositive;
    if (m.pipeline.len == 0) return LoadError.EmptyPipeline;

    var model_stages: u32 = 0;
    for (m.pipeline) |s| {
        switch (s.type) {
            .tree_ensemble => {
                model_stages += 1;
                const te = s.tree_ensemble orelse return LoadError.MalformedTreeEnsemble;
                try validateTreeEnsemble(te.*, m.metadata.num_features);
            },
            .linear => {
                model_stages += 1;
                const lm = s.linear orelse return LoadError.MalformedLinearModel;
                try validateLinear(lm.*, m.metadata.num_features, m.output.num_outputs);
            },
            .svm => {
                model_stages += 1;
                const sm = s.svm orelse return LoadError.MalformedSvmModel;
                try validateSvm(sm.*, m.metadata.num_features);
            },
            .scaler => {
                const sc = s.scaler orelse return LoadError.MalformedScaler;
                try validateScaler(sc.*, m.metadata.num_features);
            },
            .imputer => {
                const im = s.imputer orelse return LoadError.MalformedImputer;
                try validateImputer(im.*, m.metadata.num_features);
            },
        }
    }
    if (model_stages == 0) return LoadError.NoModelStage;
    if (model_stages > 1) return LoadError.MultipleModelStages;
}

fn validateTreeEnsemble(te: ir.TreeEnsemble, num_features: u32) LoadError!void {
    const n = te.nodes;
    const len = n.feature_index.len;
    if (n.threshold.len != len or n.left_child.len != len or n.right_child.len != len or
        n.leaf_value.len != len or n.default_left.len != len)
    {
        return LoadError.InconsistentTreeNodeArrays;
    }
    if (te.num_trees != 0 and n.tree_starts.len != te.num_trees) return LoadError.MalformedTreeEnsemble;
    if (len == 0) return LoadError.MalformedTreeEnsemble;

    // Tree starts must be in range. We do NOT require ascending order or
    // contiguous tree ranges — ONNX-ML exporters sometimes interleave
    // nodes. dead_leaf and other passes walk trees via children rather
    // than via index ranges to stay correct under any layout.
    for (n.tree_starts) |ts| {
        if (ts < 0 or @as(usize, @intCast(ts)) >= len) return LoadError.OutOfBoundsTreeIndex;
    }
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const fi = n.feature_index[i];
        if (fi >= 0 and @as(u32, @intCast(fi)) >= num_features) return LoadError.FeatureIndexOutOfRange;
        const l = n.left_child[i];
        const r = n.right_child[i];
        if (l != -1 and (l < 0 or @as(usize, @intCast(l)) >= len)) return LoadError.OutOfBoundsTreeIndex;
        if (r != -1 and (r < 0 or @as(usize, @intCast(r)) >= len)) return LoadError.OutOfBoundsTreeIndex;
        // An internal node (feature_index >= 0) must have BOTH children
        // resolved. Letting one child be -1 means the tree walk steps to
        // index -1 and crashes — see tree_engine.walkOne/walkChunk.
        if (fi >= 0 and (l < 0 or r < 0)) return LoadError.OutOfBoundsTreeIndex;
    }
}

fn validateLinear(lm: ir.LinearModel, num_features: u32, num_outputs: u32) LoadError!void {
    if (lm.weights.len == 0 or lm.weights.len != num_outputs) return LoadError.MalformedLinearModel;
    if (lm.biases.len != num_outputs) return LoadError.MalformedLinearModel;
    for (lm.weights) |row| if (row.len != num_features) return LoadError.MalformedLinearModel;
}

fn validateSvm(sm: ir.SVMModel, num_features: u32) LoadError!void {
    if (sm.support_vectors.len == 0) return LoadError.MalformedSvmModel;
    for (sm.support_vectors) |sv| if (sv.len != num_features) return LoadError.MalformedSvmModel;
    if (sm.dual_coefficients.len == 0) return LoadError.MalformedSvmModel;
    for (sm.dual_coefficients) |row| if (row.len != sm.support_vectors.len) return LoadError.MalformedSvmModel;
    if (sm.intercept.len != sm.dual_coefficients.len) return LoadError.MalformedSvmModel;
}

fn validateScaler(sc: ir.Scaler, num_features: u32) LoadError!void {
    const nf: usize = @intCast(num_features);
    switch (sc.method) {
        .standard, .robust => if (sc.center.len != nf or sc.scale.len != nf) return LoadError.MalformedScaler,
        .minmax => if (sc.min.len != nf or sc.max.len != nf) return LoadError.MalformedScaler,
        .maxabs => if (sc.scale.len != nf) return LoadError.MalformedScaler,
    }
}

fn validateImputer(im: ir.Imputer, num_features: u32) LoadError!void {
    if (im.fill_values.len != @as(usize, @intCast(num_features))) return LoadError.MalformedImputer;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const minimal_tree_json =
    \\{
    \\  "schema_version": 1,
    \\  "metadata": { "name": "t", "source_framework": "test", "task": "regression", "num_features": 2 },
    \\  "output": { "activation": "identity", "num_outputs": 1 },
    \\  "pipeline": [
    \\    {
    \\      "type": "tree_ensemble",
    \\      "tree_ensemble": {
    \\        "objective": "reg:squarederror",
    \\        "base_score": 0.0,
    \\        "num_trees": 1,
    \\        "num_features": 2,
    \\        "max_depth": 1,
    \\        "nodes": {
    \\          "feature_index": [0, -1, -1],
    \\          "threshold": [0.5, 0.0, 0.0],
    \\          "left_child": [1, -1, -1],
    \\          "right_child": [2, -1, -1],
    \\          "leaf_value": [0.0, -1.0, 1.0],
    \\          "default_left": [true, false, false],
    \\          "tree_starts": [0]
    \\        }
    \\      }
    \\    }
    \\  ]
    \\}
;

test "minimal tree IR parses and validates" {
    var loaded = try parseFromSlice(std.testing.allocator, minimal_tree_json);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(u32, 1), loaded.model.schema_version);
    try std.testing.expectEqual(@as(u32, 2), loaded.model.metadata.num_features);
    try std.testing.expectEqual(ir.TaskType.regression, loaded.model.metadata.task);
    try std.testing.expectEqual(@as(usize, 1), loaded.model.pipeline.len);
    const s = loaded.model.modelStage().?;
    try std.testing.expectEqual(ir.StageTag.tree_ensemble, s.type);
    try std.testing.expectEqual(@as(u32, 1), s.tree_ensemble.?.num_trees);
}

test "missing pipeline rejected" {
    const bad =
        \\{"schema_version":1,"metadata":{"num_features":1,"task":"regression"},"output":{"activation":"identity","num_outputs":1}}
    ;
    try std.testing.expectError(LoadError.MissingPipeline, parseFromSlice(std.testing.allocator, bad));
}

test "out-of-range feature index rejected" {
    const bad =
        \\{
        \\  "schema_version": 1,
        \\  "metadata": { "num_features": 1, "task": "regression" },
        \\  "output": { "activation": "identity", "num_outputs": 1 },
        \\  "pipeline": [{
        \\    "type": "tree_ensemble",
        \\    "tree_ensemble": {
        \\      "objective": "reg:squarederror", "num_trees": 1, "num_features": 1, "max_depth": 1,
        \\      "nodes": {
        \\        "feature_index": [5, -1, -1], "threshold": [0,0,0],
        \\        "left_child": [1,-1,-1], "right_child": [2,-1,-1],
        \\        "leaf_value": [0,0,0], "default_left": [true,false,false],
        \\        "tree_starts":[0]
        \\      }
        \\    }
        \\  }]
        \\}
    ;
    try std.testing.expectError(LoadError.FeatureIndexOutOfRange, parseFromSlice(std.testing.allocator, bad));
}

test "wrong schema version rejected" {
    const bad =
        \\{"schema_version":99,"metadata":{"num_features":1,"task":"regression"},"output":{"activation":"identity","num_outputs":1},"pipeline":[]}
    ;
    try std.testing.expectError(LoadError.UnsupportedSchemaVersion, parseFromSlice(std.testing.allocator, bad));
}
