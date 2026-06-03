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

//! Pipeline driver: walks the IR's `pipeline` stages in order, dispatching
//! to the per-stage engines. The pure-tree, single-output, no-preprocessing
//! fast path bypasses intermediate buffers entirely.

const std = @import("std");
const ir = @import("ir.zig");
const activation = @import("activation.zig");
const tree = @import("tree_engine.zig");
const linear = @import("linear_engine.zig");
const svm = @import("svm_engine.zig");
const preprocess = @import("preprocess.zig");

pub const Error = error{
    OutOfMemory,
    FeatureCountMismatch,
    OutputMismatch,
    NoModelStage,
    EmptyEnsemble,
    UnsupportedKernel,
};

const PreprocessStage = union(enum) {
    scaler: preprocess.ScalerEngine,
    imputer: preprocess.ImputerEngine,

    fn apply(self: *PreprocessStage, x: []f32) void {
        switch (self.*) {
            .scaler => |*s| s.apply(x),
            .imputer => |*i| i.apply(x),
        }
    }

    fn deinit(self: *PreprocessStage) void {
        switch (self.*) {
            .scaler => |*s| s.deinit(),
            .imputer => |*i| i.deinit(),
        }
    }
};

const ModelStage = union(enum) {
    tree: tree.TreeEngine,
    linear: linear.LinearEngine,
    svm: svm.SvmEngine,

    fn deinit(self: *ModelStage) void {
        switch (self.*) {
            .tree => |*t| t.deinit(),
            .linear => |*l| l.deinit(),
            .svm => |*s| s.deinit(),
        }
    }
};

pub const Predictor = struct {
    alloc: std.mem.Allocator,
    metadata: ir.Metadata,
    output: ir.OutputCfg,
    num_features: u32,
    num_outputs: u32,
    pre: []PreprocessStage,
    model: ModelStage,
    /// Cached fast-path flag: pre.len == 0 && model is tree && num_outputs == 1.
    fast_tree_path: bool,
    /// Optional arena owning the IR strings/slices the predictor's engines
    /// reference (feature names, threshold arrays before conversion, etc.).
    /// When non-null, deinit() releases it on the predictor's own allocator.
    /// Use `Predictor.initOwned` to attach one.
    owned_arena: ?*std.heap.ArenaAllocator = null,

    pub fn init(alloc: std.mem.Allocator, m: ir.TabularModel) Error!Predictor {
        var pre = std.array_list.Managed(PreprocessStage).init(alloc);
        errdefer {
            for (pre.items) |*p| p.deinit();
            pre.deinit();
        }
        var model_opt: ?ModelStage = null;
        errdefer if (model_opt) |*ms| ms.deinit();

        for (m.pipeline) |s| {
            switch (s.type) {
                .scaler => {
                    const sc = s.scaler orelse return Error.NoModelStage;
                    try pre.append(.{ .scaler = try preprocess.ScalerEngine.init(alloc, sc.*) });
                },
                .imputer => {
                    const im = s.imputer orelse return Error.NoModelStage;
                    try pre.append(.{ .imputer = try preprocess.ImputerEngine.init(alloc, im.*) });
                },
                .tree_ensemble => {
                    const te = s.tree_ensemble orelse return Error.NoModelStage;
                    model_opt = .{ .tree = try tree.TreeEngine.init(alloc, te.*, m.output.num_outputs) };
                },
                .linear => {
                    const lm = s.linear orelse return Error.NoModelStage;
                    model_opt = .{ .linear = try linear.LinearEngine.init(alloc, lm.*) };
                },
                .svm => {
                    const sm = s.svm orelse return Error.NoModelStage;
                    model_opt = .{ .svm = try svm.SvmEngine.init(alloc, sm.*) };
                },
            }
        }

        const model = model_opt orelse return Error.NoModelStage;
        const fast = pre.items.len == 0 and switch (model) {
            .tree => m.output.num_outputs == 1,
            else => false,
        };

        return .{
            .alloc = alloc,
            .metadata = m.metadata,
            .output = m.output,
            .num_features = m.metadata.num_features,
            .num_outputs = m.output.num_outputs,
            .pre = try pre.toOwnedSlice(),
            .model = model,
            .fast_tree_path = fast,
        };
    }

    pub fn deinit(self: *Predictor) void {
        for (self.pre) |*p| p.deinit();
        self.alloc.free(self.pre);
        self.model.deinit();
        if (self.owned_arena) |a| {
            const parent = a.child_allocator;
            a.deinit();
            parent.destroy(a);
        }
    }

    /// Construct a Predictor that takes ownership of the IR's arena. The
    /// arena is released alongside the predictor in `deinit`. This is the
    /// common case for HTTP / file-loaded models: `parseFromSlice` returns
    /// an arena-rooted IR, and we want the predictor to own that lifetime.
    pub fn initOwned(
        alloc: std.mem.Allocator,
        m: ir.TabularModel,
        arena: *std.heap.ArenaAllocator,
    ) Error!Predictor {
        var p = try init(alloc, m);
        p.owned_arena = arena;
        return p;
    }

    pub fn numFeatures(self: *const Predictor) u32 {
        return self.num_features;
    }
    pub fn numOutputs(self: *const Predictor) u32 {
        return self.num_outputs;
    }

    /// Single-sample prediction. `out.len` must equal `numOutputs()`.
    pub fn predictSingle(self: *Predictor, x: []const f32, out: []f32) Error!void {
        if (x.len != self.num_features) return Error.FeatureCountMismatch;
        if (out.len != self.num_outputs) return Error.OutputMismatch;

        if (self.fast_tree_path) {
            out[0] = self.model.tree.predictSingleOne(x);
            activation.apply(self.output.activation, out);
            return;
        }

        // General path: copy → preprocess → model → activation.
        var scratch_buf: [256]f32 = undefined;
        var scratch_dyn: ?[]f32 = null;
        defer if (scratch_dyn) |s| self.alloc.free(s);

        const scratch: []f32 = blk: {
            if (x.len <= scratch_buf.len) break :blk scratch_buf[0..x.len];
            const s = try self.alloc.alloc(f32, x.len);
            scratch_dyn = s;
            break :blk s;
        };
        @memcpy(scratch, x);

        for (self.pre) |*p| p.apply(scratch);

        switch (self.model) {
            .tree => |*t| {
                if (self.num_outputs == 1) {
                    out[0] = t.predictSingleOne(scratch);
                } else {
                    t.predictSingleMany(scratch, out);
                }
            },
            .linear => |*l| l.predictSingle(scratch, out),
            .svm => |*s| s.predictSingle(scratch, out),
        }

        activation.apply(self.output.activation, out);
    }

    /// Batched prediction. Out is [batch][num_outputs] — caller-allocated.
    pub fn predict(self: *Predictor, batch: []const []const f32, out: [][]f32) Error!void {
        if (batch.len != out.len) return Error.OutputMismatch;
        for (batch, 0..) |x, i| try self.predictSingle(x, out[i]);
    }
};

// ---------------------------------------------------------------------------
// Tests — end-to-end pipeline including preprocessing + activation.
// ---------------------------------------------------------------------------

test "scaler + tree + sigmoid pipeline" {
    // Scaler: standardise feature 0 by (val - 5) / 2.
    const sc = ir.Scaler{
        .method = .standard,
        .center = &[_]f64{5.0},
        .scale = &[_]f64{2.0},
    };
    // Stump split at 0.5 returning -1 or +1.
    const te = ir.TreeEnsemble{
        .objective = "binary:logistic",
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
    const stages = [_]ir.Stage{
        .{ .type = .scaler, .scaler = &sc },
        .{ .type = .tree_ensemble, .tree_ensemble = &te },
    };
    const model = ir.TabularModel{
        .metadata = .{ .num_features = 1, .task = .binary_classification },
        .pipeline = &stages,
        .output = .{ .activation = .sigmoid, .num_outputs = 1 },
    };

    var p = try Predictor.init(std.testing.allocator, model);
    defer p.deinit();
    try std.testing.expectEqual(false, p.fast_tree_path); // has scaler

    var out: [1]f32 = undefined;
    // Raw 8 → scaled (8-5)/2 = 1.5 → > 0.5 → tree returns +1 → sigmoid(1) ≈ 0.731.
    try p.predictSingle(&[_]f32{8.0}, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 0.7310586), out[0], 1e-5);

    // Raw 4 → scaled (4-5)/2 = -0.5 → < 0.5 → tree returns -1 → sigmoid(-1) ≈ 0.269.
    try p.predictSingle(&[_]f32{4.0}, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 0.26894143), out[0], 1e-5);
}

test "pure tree single-output uses fast path" {
    const te = ir.TreeEnsemble{
        .num_trees = 1,
        .num_features = 1,
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
    const stages = [_]ir.Stage{.{ .type = .tree_ensemble, .tree_ensemble = &te }};
    const model = ir.TabularModel{
        .metadata = .{ .num_features = 1, .task = .regression },
        .pipeline = &stages,
        .output = .{ .activation = .identity, .num_outputs = 1 },
    };
    var p = try Predictor.init(std.testing.allocator, model);
    defer p.deinit();
    try std.testing.expectEqual(true, p.fast_tree_path);

    var out: [1]f32 = undefined;
    try p.predictSingle(&[_]f32{0.9}, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 1), out[0], 1e-6);
}
