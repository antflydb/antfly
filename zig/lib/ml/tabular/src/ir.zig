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

//! Intermediate representation for traditional ML models (tree ensembles,
//! linear / logistic regression, SVMs) plus preprocessing (Scaler, Imputer).
//!
//! The IR is framework-agnostic and serialised to `tabular_model.json`.
//! Antfly Inference consumes the format verbatim after conversion or pull.

const std = @import("std");

pub const schema_version: u32 = 1;

pub const TaskType = enum {
    regression,
    binary_classification,
    multiclass,
    ranking,
};

pub const Activation = enum {
    identity,
    sigmoid,
    softmax,
    exp,
};

pub const StageTag = enum {
    tree_ensemble,
    linear,
    svm,
    scaler,
    imputer,
};

pub const ScalerMethod = enum { standard, minmax, robust, maxabs };

pub const ImputerStrategy = enum { mean, median, most_frequent, constant };

pub const SvmKernel = enum { rbf, linear, poly, sigmoid };

pub const Metadata = struct {
    name: []const u8 = "",
    source_framework: []const u8 = "",
    source_version: []const u8 = "",
    task: TaskType = .regression,
    num_features: u32 = 0,
    num_classes: u32 = 0,
    feature_names: []const []const u8 = &.{},
    feature_types: []const []const u8 = &.{},
    created_at: []const u8 = "",
};

pub const OutputCfg = struct {
    activation: Activation = .identity,
    num_outputs: u32 = 1,
};

/// Struct-of-arrays node storage. All slices except `tree_starts` have
/// the same length — the total node count across all trees.
pub const TreeNodes = struct {
    feature_index: []i32 = &.{}, // -1 = leaf
    threshold: []f64 = &.{},
    left_child: []i32 = &.{}, // -1 = none
    right_child: []i32 = &.{}, // -1 = none
    leaf_value: []f64 = &.{}, // meaningful only for leaves
    default_left: []bool = &.{},
    tree_starts: []i32 = &.{}, // index of each tree's root node
};

pub const TreeAnnotations = struct {
    /// Per-feature precision hint produced by the quantize pass.
    /// Recognised values: "f32", "f16", "i8". Default = "f32".
    threshold_precision: []const []const u8 = &.{},
    dead_leaves_eliminated: u32 = 0,
    branch_order: []const u8 = "",
};

pub const TreeEnsemble = struct {
    objective: []const u8 = "",
    base_score: f64 = 0,
    num_trees: u32 = 0,
    num_features: u32 = 0,
    max_depth: u32 = 0,
    nodes: TreeNodes = .{},
    annotations: TreeAnnotations = .{},
};

pub const LinearModel = struct {
    /// [num_outputs][num_features]
    weights: []const []const f64 = &.{},
    /// [num_outputs]
    biases: []const f64 = &.{},
};

pub const SVMModel = struct {
    kernel: SvmKernel = .linear,
    /// [num_vectors][num_features]
    support_vectors: []const []const f64 = &.{},
    /// [num_outputs][num_vectors]
    dual_coefficients: []const []const f64 = &.{},
    /// [num_outputs]
    intercept: []const f64 = &.{},
    gamma: f64 = 0,
    coef0: f64 = 0,
    degree: u32 = 3,
};

pub const Scaler = struct {
    method: ScalerMethod = .standard,
    center: []const f64 = &.{},
    scale: []const f64 = &.{},
    min: []const f64 = &.{},
    max: []const f64 = &.{},
};

pub const Imputer = struct {
    strategy: ImputerStrategy = .mean,
    fill_values: []const f64 = &.{},
};

/// Pipeline stage. Exactly one of the typed pointer fields is non-null,
/// chosen by `type`.
pub const Stage = struct {
    type: StageTag,
    tree_ensemble: ?*const TreeEnsemble = null,
    linear: ?*const LinearModel = null,
    svm: ?*const SVMModel = null,
    scaler: ?*const Scaler = null,
    imputer: ?*const Imputer = null,
};

pub const TabularModel = struct {
    schema_version: u32 = schema_version,
    metadata: Metadata = .{},
    pipeline: []const Stage = &.{},
    output: OutputCfg = .{},

    /// Find the (single) model stage in the pipeline. Returns null if
    /// the pipeline contains only preprocessing (which is invalid).
    pub fn modelStage(self: TabularModel) ?Stage {
        for (self.pipeline) |s| {
            switch (s.type) {
                .tree_ensemble, .linear, .svm => return s,
                else => continue,
            }
        }
        return null;
    }
};

// ---------------------------------------------------------------------------
// Convenience helpers for parsing string -> enum used by the loader and the
// converters. Centralised here so all consumers stay in sync.
// ---------------------------------------------------------------------------

pub fn taskTypeFromString(s: []const u8) ?TaskType {
    if (std.mem.eql(u8, s, "regression")) return .regression;
    if (std.mem.eql(u8, s, "binary_classification")) return .binary_classification;
    if (std.mem.eql(u8, s, "multiclass")) return .multiclass;
    if (std.mem.eql(u8, s, "ranking")) return .ranking;
    return null;
}

pub fn taskTypeToString(t: TaskType) []const u8 {
    return switch (t) {
        .regression => "regression",
        .binary_classification => "binary_classification",
        .multiclass => "multiclass",
        .ranking => "ranking",
    };
}

pub fn activationFromString(s: []const u8) ?Activation {
    if (std.mem.eql(u8, s, "identity")) return .identity;
    if (std.mem.eql(u8, s, "sigmoid")) return .sigmoid;
    if (std.mem.eql(u8, s, "softmax")) return .softmax;
    if (std.mem.eql(u8, s, "exp")) return .exp;
    return null;
}

pub fn activationToString(a: Activation) []const u8 {
    return switch (a) {
        .identity => "identity",
        .sigmoid => "sigmoid",
        .softmax => "softmax",
        .exp => "exp",
    };
}

pub fn stageTagFromString(s: []const u8) ?StageTag {
    if (std.mem.eql(u8, s, "tree_ensemble")) return .tree_ensemble;
    if (std.mem.eql(u8, s, "linear")) return .linear;
    if (std.mem.eql(u8, s, "svm")) return .svm;
    if (std.mem.eql(u8, s, "scaler")) return .scaler;
    if (std.mem.eql(u8, s, "imputer")) return .imputer;
    return null;
}

pub fn scalerMethodFromString(s: []const u8) ?ScalerMethod {
    if (std.mem.eql(u8, s, "standard")) return .standard;
    if (std.mem.eql(u8, s, "minmax")) return .minmax;
    if (std.mem.eql(u8, s, "robust")) return .robust;
    if (std.mem.eql(u8, s, "maxabs")) return .maxabs;
    return null;
}

pub fn imputerStrategyFromString(s: []const u8) ?ImputerStrategy {
    if (std.mem.eql(u8, s, "mean")) return .mean;
    if (std.mem.eql(u8, s, "median")) return .median;
    if (std.mem.eql(u8, s, "most_frequent")) return .most_frequent;
    if (std.mem.eql(u8, s, "constant")) return .constant;
    return null;
}

pub fn svmKernelFromString(s: []const u8) ?SvmKernel {
    if (std.mem.eql(u8, s, "rbf")) return .rbf;
    if (std.mem.eql(u8, s, "linear")) return .linear;
    if (std.mem.eql(u8, s, "poly")) return .poly;
    if (std.mem.eql(u8, s, "sigmoid")) return .sigmoid;
    return null;
}

test "stage tag round-trip" {
    try std.testing.expectEqual(@as(?StageTag, .tree_ensemble), stageTagFromString("tree_ensemble"));
    try std.testing.expectEqual(@as(?StageTag, .linear), stageTagFromString("linear"));
    try std.testing.expectEqual(@as(?StageTag, .svm), stageTagFromString("svm"));
    try std.testing.expectEqual(@as(?StageTag, null), stageTagFromString("nope"));
}

test "modelStage finds first model after preprocessing" {
    const tree: TreeEnsemble = .{ .num_trees = 0 };
    const lin: LinearModel = .{};
    const scaler: Scaler = .{};
    const stages = [_]Stage{
        .{ .type = .scaler, .scaler = &scaler },
        .{ .type = .tree_ensemble, .tree_ensemble = &tree },
        .{ .type = .linear, .linear = &lin },
    };
    const m = TabularModel{ .pipeline = &stages };
    const got = m.modelStage();
    try std.testing.expect(got != null);
    try std.testing.expectEqual(StageTag.tree_ensemble, got.?.type);
}

test "modelStage returns null for preprocessing-only pipeline" {
    const scaler: Scaler = .{};
    const stages = [_]Stage{.{ .type = .scaler, .scaler = &scaler }};
    const m = TabularModel{ .pipeline = &stages };
    try std.testing.expectEqual(@as(?Stage, null), m.modelStage());
}
