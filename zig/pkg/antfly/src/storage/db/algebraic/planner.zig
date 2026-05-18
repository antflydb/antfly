// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const algebra = @import("algebra.zig");
const fact_mod = @import("fact.zig");
const index_mod = @import("index.zig");
const ir = @import("ir.zig");
const join_mod = @import("join.zig");
const law_mod = @import("law.zig");
const pathfact_mod = @import("pathfact.zig");
const value_mod = @import("value.zig");

pub const UnsupportedAggregationPlan = error{UnsupportedAggregationPlan};

pub const MetricPlan = struct {
    materialization: index_mod.MaterializationConfig,
    op: algebra.Op,
};

pub const TensorPlan = struct {
    materialization: index_mod.MaterializationConfig,
    access_path: ir.PhysicalAccessPath,
};

pub const TensorProgramQueryPlan = struct {
    program_id: []u8,
    inputs: []ir.TensorExpr = &.{},
    access_paths: []ir.PhysicalAccessPath,
    steps: []ir.TensorProgramStep,
    output: ir.TensorProgramRef,
    outputs: []ir.TensorProgramRef = &.{},
    owned_metadata: []?[]u8 = &.{},

    pub fn deinit(self: *TensorProgramQueryPlan, alloc: std.mem.Allocator) void {
        alloc.free(self.program_id);
        if (self.inputs.len > 0) alloc.free(self.inputs);
        if (self.access_paths.len > 0) alloc.free(self.access_paths);
        for (self.owned_metadata) |metadata| {
            if (metadata) |bytes| alloc.free(bytes);
        }
        if (self.owned_metadata.len > 0) alloc.free(self.owned_metadata);
        if (self.steps.len > 0) alloc.free(self.steps);
        if (self.outputs.len > 0) alloc.free(self.outputs);
        self.* = undefined;
    }

    pub fn asProgram(self: *const TensorProgramQueryPlan) ir.TensorProgram {
        return .{
            .inputs = self.inputs,
            .steps = self.steps,
            .output = self.output,
            .outputs = self.outputs,
        };
    }
};

pub const BucketTensorProgramQueryPlan = struct {
    count: TensorProgramQueryPlan,
    child_metrics: []TensorProgramQueryPlan = &.{},

    pub fn deinit(self: *BucketTensorProgramQueryPlan, alloc: std.mem.Allocator) void {
        self.count.deinit(alloc);
        for (self.child_metrics) |*child| child.deinit(alloc);
        if (self.child_metrics.len > 0) alloc.free(self.child_metrics);
        self.* = undefined;
    }
};

pub const DerivedJoinFoldOutput = struct {
    semantic_id: []const u8,
    fold: index_mod.DerivedJoinFoldRequest,
};

pub const DocFactBucketFoldOutput = struct {
    semantic_id: []const u8,
    fold: index_mod.DocFactBucketFoldRequest,
};

pub const PathFactBucketFoldOutput = struct {
    semantic_id: []const u8,
    fold: index_mod.PathFactBucketFoldRequest,
};

pub const RangeBound = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

pub const DerivedJoinRangeField = struct {
    name: []const u8,
    role: fact_mod.Role,
    kind: index_mod.DerivedJoinRangeKind,
};

pub const PlanKind = enum {
    exact_materialized,
    rollup_materialized,
    derived_join_fold,
    lazy_join_possible,
    unsupported,
};

pub const JoinPlanKind = enum {
    equi,
    temporal_bucket,
    temporal_window,
    temporal_bucket_window,
};

pub const AlgebraicPlan = struct {
    kind: PlanKind,
    metric: ?MetricPlan = null,
    join_kind: ?JoinPlanKind = null,
};

pub const FallbackReason = enum {
    unsupported_type,
    no_materialization,
    ambiguous_materialization,
    unsupported_field,
    unsupported_join,
    child_metric_unsupported,
    schema_lifecycle_not_ready,
};

pub const PlanResult = struct {
    kind: PlanKind,
    metric: ?MetricPlan = null,
    count_metric: ?MetricPlan = null,
    child_metrics: []const MetricPlan = &.{},
    derived_join_fold: ?index_mod.DerivedJoinFoldRequest = null,
    derived_child_join_folds: []const index_mod.DerivedJoinFoldRequest = &.{},
    derived_join_group_by_owned: bool = false,
    join_kind: ?JoinPlanKind = null,
    fallback_reason: ?FallbackReason = null,
    estimated_scan_rows: ?usize = null,
    estimated_result_buckets: ?usize = null,

    pub fn deinit(self: *PlanResult, alloc: std.mem.Allocator) void {
        if (self.child_metrics.len > 0) alloc.free(self.child_metrics);
        if (self.derived_join_group_by_owned) {
            if (self.derived_join_fold) |derived| {
                if (derived.group_by.len > 0) alloc.free(@constCast(derived.group_by));
            }
        }
        if (self.derived_child_join_folds.len > 0) alloc.free(self.derived_child_join_folds);
        self.* = undefined;
    }
};

const materialized_tensor_fragments = [_]ir.TensorFragment{ .slice, .reduce, .merge };
const materialized_join_tensor_fragments = [_]ir.TensorFragment{ .slice, .join, .reduce, .merge };
const join_fact_fragments = [_]ir.TensorFragment{.slice};
const doc_input_dims = [_]ir.Dimension{.doc};
const doc_scalar_input_dims = [_]ir.Dimension{ .doc, .scalar };
const doc_time_input_dims = [_]ir.Dimension{ .doc, .time };
const scalar_output_dims = [_]ir.Dimension{.scalar};
const bucket_scalar_output_dims = [_]ir.Dimension{ .bucket, .scalar };
const time_bucket_scalar_output_dims = [_]ir.Dimension{ .time, .bucket, .scalar };
const join_fact_output_dims = [_]ir.Dimension{ .src, .dst, .scalar, .time, .bucket };
const derived_join_step_inputs = [_]ir.TensorProgramRef{.{ .step = 0 }};
const doc_fact_output_dims = [_]ir.Dimension{ .doc, .field, .scalar };
const doc_fact_reduce_inputs = [_]ir.TensorProgramRef{.{ .step = 0 }};
const path_fact_output_dims = [_]ir.Dimension{ .doc, .path, .kind, .scalar };
const path_fact_reduce_inputs = [_]ir.TensorProgramRef{.{ .step = 0 }};
const native_doc_id_constraint_dims = [_]ir.Dimension{.doc};
const vector_search_output_dims = [_]ir.Dimension{ .doc, .score };
const vector_doc_id_constraint_inputs = [_]ir.TensorProgramRef{.{ .input = 0 }};
const graph_traverse_output_dims = [_]ir.Dimension{.doc};
const graph_edge_output_dims = [_]ir.Dimension{ .src, .dst };
const graph_target_constraint_inputs = [_]ir.TensorProgramRef{.{ .input = 0 }};
const count_laws = [_]law_mod.Id{.count};
const sum_laws = [_]law_mod.Id{.sum};
const sumsquares_laws = [_]law_mod.Id{.sumsquares};
const avg_laws = [_]law_mod.Id{.avg};
const min_laws = [_]law_mod.Id{.min};
const max_laws = [_]law_mod.Id{.max};

pub fn materializationAccessPath(mat: index_mod.MaterializationConfig) ?ir.PhysicalAccessPath {
    const op = algebra.Op.parse(mat.op) orelse return null;
    return .{
        .owner = mat.name,
        .layout = .materialized_tensor,
        .fragments = if (mat.join != null) &materialized_join_tensor_fragments else &materialized_tensor_fragments,
        .output_dims = materializationOutputDims(mat),
        .law_ids = materializationLawIds(op),
    };
}

pub fn materializationTensorExpression(mat: index_mod.MaterializationConfig) ?ir.TensorExpr {
    const op = algebra.Op.parse(mat.op) orelse return null;
    return .{
        .fragment = .reduce,
        .input_dims = if (mat.time != null)
            &doc_time_input_dims
        else if (mat.measure != null)
            &doc_scalar_input_dims
        else
            &doc_input_dims,
        .output_dims = materializationOutputDims(mat),
        .semantic_id = mat.name,
        .layout = .materialized_expr,
        .law_id = law_mod.fromOp(op),
    };
}

pub fn materializationCanSatisfy(mat: index_mod.MaterializationConfig, expr: ir.TensorExpr) ir.AccessPathProof {
    const path = materializationAccessPath(mat) orelse return .{ .rejected = .law_required };
    return ir.accessPathCanSatisfy(path, expr);
}

pub fn planTensorExpr(index: *const index_mod.Index, expr: ir.TensorExpr) ?TensorPlan {
    if (!index.plannerLifecycleReady()) return null;
    var selected: ?TensorPlan = null;
    var selected_count: usize = 0;
    for (index.config().materializations) |mat| {
        const access_path = materializationAccessPath(mat) orelse continue;
        if (!ir.accessPathCanSatisfy(access_path, expr).safe()) continue;
        selected = .{ .materialization = mat, .access_path = access_path };
        selected_count += 1;
    }
    if (selected_count == 1) return selected;
    return null;
}

pub fn planMetricTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    query: ir.Query,
) !?TensorProgramQueryPlan {
    const result = planMetricQuery(index, query);
    if (result.kind == .derived_join_fold) {
        const fold = result.derived_join_fold orelse return null;
        return try planDerivedJoinFoldTensorProgramAlloc(alloc, fold, query.aggregation_name);
    }
    if (result.kind == .exact_materialized or result.kind == .rollup_materialized) {
        const metric_plan = result.metric orelse return null;
        return try tensorProgramForMetricPlanAlloc(alloc, metric_plan);
    }
    return null;
}

pub fn planBucketCountTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    query: ir.Query,
) !?TensorProgramQueryPlan {
    const result = planBucketCountQuery(index, query);
    if (result.kind != .exact_materialized and result.kind != .rollup_materialized) return null;
    const count_plan = result.count_metric orelse return null;
    return try tensorProgramForMetricPlanAlloc(alloc, count_plan);
}

pub fn planBucketQueryTensorProgramsAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    query: ir.Query,
) !?BucketTensorProgramQueryPlan {
    var result = try planBucketQueryAlloc(alloc, index, query);
    defer result.deinit(alloc);
    if (result.kind == .derived_join_fold) {
        const count_fold = result.derived_join_fold orelse return null;
        var count_program = (try planDerivedJoinFoldTensorProgramAlloc(alloc, count_fold, query.aggregation_name)) orelse return null;
        errdefer count_program.deinit(alloc);

        const children = try alloc.alloc(TensorProgramQueryPlan, result.derived_child_join_folds.len);
        var initialized: usize = 0;
        errdefer {
            for (children[0..initialized]) |*child| child.deinit(alloc);
            if (children.len > 0) alloc.free(children);
        }
        for (result.derived_child_join_folds, 0..) |child_fold, i| {
            const semantic_id = if (i < query.child_metrics.len) query.child_metrics[i].name else query.aggregation_name;
            children[i] = (try planDerivedJoinFoldTensorProgramAlloc(alloc, child_fold, semantic_id)) orelse {
                for (children[0..initialized]) |*child| child.deinit(alloc);
                if (children.len > 0) alloc.free(children);
                count_program.deinit(alloc);
                return null;
            };
            initialized += 1;
        }
        return .{
            .count = count_program,
            .child_metrics = children,
        };
    }
    if (result.kind != .exact_materialized and result.kind != .rollup_materialized) return null;
    const count_plan = result.count_metric orelse return null;
    var count_program = (try tensorProgramForMetricPlanAlloc(alloc, count_plan)) orelse return null;
    errdefer count_program.deinit(alloc);

    const children = try alloc.alloc(TensorProgramQueryPlan, result.child_metrics.len);
    var initialized: usize = 0;
    errdefer {
        for (children[0..initialized]) |*child| child.deinit(alloc);
        if (children.len > 0) alloc.free(children);
    }
    for (result.child_metrics, 0..) |child_plan, i| {
        children[i] = (try tensorProgramForMetricPlanAlloc(alloc, child_plan)) orelse {
            for (children[0..initialized]) |*child| child.deinit(alloc);
            if (children.len > 0) alloc.free(children);
            count_program.deinit(alloc);
            return null;
        };
        initialized += 1;
    }
    return .{
        .count = count_program,
        .child_metrics = children,
    };
}

pub fn planBucketQueryMultiOutputTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    query: ir.Query,
) !?TensorProgramQueryPlan {
    var result = try planBucketQueryAlloc(alloc, index, query);
    defer result.deinit(alloc);
    if (result.kind == .derived_join_fold) {
        const count_fold = result.derived_join_fold orelse return null;
        return try multiOutputProgramForDerivedJoinBucketAlloc(alloc, count_fold, result.derived_child_join_folds, query);
    }
    if (result.kind != .exact_materialized and result.kind != .rollup_materialized) return null;
    const count_plan = result.count_metric orelse return null;
    return try multiOutputProgramForMetricPlansAlloc(alloc, count_plan, result.child_metrics);
}

pub fn planMaterializationPartialsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    materializations: []const []const u8,
) !?TensorProgramQueryPlan {
    if (!index.plannerLifecycleReady() or materializations.len == 0) return null;

    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, materializations.len);
    errdefer alloc.free(access_paths);
    const steps = try alloc.alloc(ir.TensorProgramStep, materializations.len);
    errdefer alloc.free(steps);
    const outputs = try alloc.alloc(ir.TensorProgramRef, materializations.len);
    errdefer alloc.free(outputs);

    for (materializations, 0..) |name, i| {
        const mat = namedMaterialization(index, name) orelse {
            alloc.free(outputs);
            alloc.free(steps);
            alloc.free(access_paths);
            return null;
        };
        const op = algebra.Op.parse(mat.op) orelse {
            alloc.free(outputs);
            alloc.free(steps);
            alloc.free(access_paths);
            return null;
        };
        const access_path = materializationAccessPath(mat) orelse {
            alloc.free(outputs);
            alloc.free(steps);
            alloc.free(access_paths);
            return null;
        };
        access_paths[i] = access_path;
        steps[i] = .{ .expr = tensorExprForMetricPlan(.{ .materialization = mat, .op = op }, access_path) };
        outputs[i] = .{ .step = i };
    }

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 0 },
        .outputs = outputs,
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
        .outputs = outputs,
    };
}

fn multiOutputProgramForMetricPlansAlloc(
    alloc: std.mem.Allocator,
    count_plan: MetricPlan,
    child_plans: []const MetricPlan,
) !?TensorProgramQueryPlan {
    const total_steps = 1 + child_plans.len;
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, total_steps);
    errdefer alloc.free(access_paths);
    const steps = try alloc.alloc(ir.TensorProgramStep, total_steps);
    errdefer alloc.free(steps);
    const outputs = try alloc.alloc(ir.TensorProgramRef, total_steps);
    errdefer alloc.free(outputs);

    const count_access_path = materializationAccessPath(count_plan.materialization) orelse {
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    };
    access_paths[0] = count_access_path;
    steps[0] = .{ .expr = tensorExprForMetricPlan(count_plan, count_access_path) };
    outputs[0] = .{ .step = 0 };
    for (child_plans, 0..) |child_plan, i| {
        const step_idx = i + 1;
        const access_path = materializationAccessPath(child_plan.materialization) orelse {
            alloc.free(outputs);
            alloc.free(steps);
            alloc.free(access_paths);
            return null;
        };
        access_paths[step_idx] = access_path;
        steps[step_idx] = .{ .expr = tensorExprForMetricPlan(child_plan, access_path) };
        outputs[step_idx] = .{ .step = step_idx };
    }

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 0 },
        .outputs = outputs,
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
        .outputs = outputs,
    };
}

fn multiOutputProgramForDerivedJoinBucketAlloc(
    alloc: std.mem.Allocator,
    count_fold: index_mod.DerivedJoinFoldRequest,
    child_folds: []const index_mod.DerivedJoinFoldRequest,
    query: ir.Query,
) !?TensorProgramQueryPlan {
    const access_path = derivedJoinAccessPath(count_fold.join);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    const total_outputs = 1 + child_folds.len;
    const total_steps = 1 + total_outputs;
    const steps = try alloc.alloc(ir.TensorProgramStep, total_steps);
    errdefer alloc.free(steps);
    const outputs = try alloc.alloc(ir.TensorProgramRef, total_outputs);
    errdefer alloc.free(outputs);
    const owned_metadata = try alloc.alloc(?[]u8, total_outputs);
    errdefer alloc.free(owned_metadata);
    @memset(owned_metadata, null);
    errdefer for (owned_metadata) |metadata| {
        if (metadata) |bytes| alloc.free(bytes);
    };

    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = &join_fact_output_dims,
        .semantic_id = count_fold.join.name,
        .owner = access_path.owner,
        .layout = access_path.layout,
    } };
    owned_metadata[0] = try index_mod.derivedJoinFoldMetadataAlloc(alloc, count_fold);
    steps[1] = derivedJoinStepForFold(count_fold, query.aggregation_name, owned_metadata[0].?);
    outputs[0] = .{ .step = 1 };
    for (child_folds, 0..) |child_fold, i| {
        const step_idx = i + 2;
        const semantic_id = if (i < query.child_metrics.len) query.child_metrics[i].name else query.aggregation_name;
        owned_metadata[i + 1] = try index_mod.derivedJoinFoldMetadataAlloc(alloc, child_fold);
        steps[step_idx] = derivedJoinStepForFold(child_fold, semantic_id, owned_metadata[i + 1].?);
        outputs[i + 1] = .{ .step = step_idx };
    }

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 1 },
        .outputs = outputs,
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        for (owned_metadata) |metadata| {
            if (metadata) |bytes| alloc.free(bytes);
        }
        alloc.free(owned_metadata);
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 1 },
        .outputs = outputs,
        .owned_metadata = owned_metadata,
    };
}

pub fn planDerivedJoinFoldTensorProgramAlloc(
    alloc: std.mem.Allocator,
    fold: index_mod.DerivedJoinFoldRequest,
    semantic_id: []const u8,
) !?TensorProgramQueryPlan {
    const outputs = [_]DerivedJoinFoldOutput{.{ .semantic_id = semantic_id, .fold = fold }};
    return try planDerivedJoinFoldOutputsTensorProgramAlloc(alloc, &outputs);
}

pub fn planDerivedJoinFoldOutputsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    fold_outputs: []const DerivedJoinFoldOutput,
) !?TensorProgramQueryPlan {
    if (fold_outputs.len == 0) return null;
    const join_ref = fold_outputs[0].fold.join;
    for (fold_outputs[1..]) |fold_output| {
        if (!std.mem.eql(u8, fold_output.fold.join.name, join_ref.name) or
            !optionalStringMatches(fold_output.fold.join.group_side, join_ref.group_side) or
            !optionalStringMatches(fold_output.fold.join.measure_side, join_ref.measure_side))
        {
            return error.InvalidAlgebraicConfig;
        }
    }

    const access_path = derivedJoinAccessPath(join_ref);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    const steps = try alloc.alloc(ir.TensorProgramStep, fold_outputs.len + 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = &join_fact_output_dims,
        .semantic_id = join_ref.name,
        .owner = access_path.owner,
        .layout = access_path.layout,
    } };

    const outputs = try alloc.alloc(ir.TensorProgramRef, fold_outputs.len);
    errdefer if (outputs.len > 0) alloc.free(outputs);
    const owned_metadata = try alloc.alloc(?[]u8, fold_outputs.len);
    errdefer alloc.free(owned_metadata);
    @memset(owned_metadata, null);
    errdefer for (owned_metadata) |metadata| {
        if (metadata) |bytes| alloc.free(bytes);
    };
    for (fold_outputs, 0..) |fold_output, i| {
        owned_metadata[i] = try index_mod.derivedJoinFoldMetadataAlloc(alloc, fold_output.fold);
        const step_idx = i + 1;
        steps[step_idx] = derivedJoinStepForFold(fold_output.fold, fold_output.semantic_id, owned_metadata[i].?);
        outputs[i] = .{ .step = step_idx };
    }

    const program = ir.TensorProgram{
        .steps = steps,
        .output = outputs[0],
        .outputs = outputs,
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        for (owned_metadata) |metadata| {
            if (metadata) |bytes| alloc.free(bytes);
        }
        alloc.free(owned_metadata);
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = outputs[0],
        .outputs = outputs,
        .owned_metadata = owned_metadata,
    };
}

pub fn planDocFactBucketFoldTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    fold: index_mod.DocFactBucketFoldRequest,
    semantic_id: []const u8,
) !?TensorProgramQueryPlan {
    const outputs = [_]DocFactBucketFoldOutput{.{ .semantic_id = semantic_id, .fold = fold }};
    return try planDocFactBucketFoldOutputsTensorProgramAlloc(alloc, index, &outputs);
}

pub fn planDocFactBucketFoldOutputsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    fold_outputs: []const DocFactBucketFoldOutput,
) !?TensorProgramQueryPlan {
    if (fold_outputs.len == 0) return null;
    const access_path = ir.docFactAccessPath(index.name);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    const steps = try alloc.alloc(ir.TensorProgramStep, fold_outputs.len + 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = &doc_fact_output_dims,
        .owner = access_path.owner,
        .layout = access_path.layout,
    } };

    const outputs = try alloc.alloc(ir.TensorProgramRef, fold_outputs.len);
    errdefer if (outputs.len > 0) alloc.free(outputs);
    const owned_metadata = try alloc.alloc(?[]u8, fold_outputs.len);
    errdefer alloc.free(owned_metadata);
    @memset(owned_metadata, null);
    errdefer for (owned_metadata) |metadata| {
        if (metadata) |bytes| alloc.free(bytes);
    };
    var output_idx: usize = 0;
    for (fold_outputs) |fold_output| {
        try appendDocFactBucketFoldOutput(alloc, fold_output.semantic_id, fold_output.fold, steps, outputs, owned_metadata, &output_idx);
    }

    const program = ir.TensorProgram{
        .steps = steps,
        .output = outputs[0],
        .outputs = outputs,
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        for (owned_metadata) |metadata| {
            if (metadata) |bytes| alloc.free(bytes);
        }
        alloc.free(owned_metadata);
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = outputs[0],
        .outputs = outputs,
        .owned_metadata = owned_metadata,
    };
}

fn appendDocFactBucketFoldOutput(
    alloc: std.mem.Allocator,
    semantic_id: []const u8,
    fold: index_mod.DocFactBucketFoldRequest,
    steps: []ir.TensorProgramStep,
    outputs: []ir.TensorProgramRef,
    owned_metadata: []?[]u8,
    output_idx: *usize,
) !void {
    const idx = output_idx.*;
    if (idx >= outputs.len or idx >= owned_metadata.len or idx + 1 >= steps.len) return error.InvalidAlgebraicConfig;
    owned_metadata[idx] = try index_mod.docFactBucketFoldMetadataAlloc(alloc, fold);
    const step_idx = idx + 1;
    steps[step_idx] = .{
        .expr = .{
            .fragment = .reduce,
            .input_dims = &doc_fact_output_dims,
            .output_dims = &bucket_scalar_output_dims,
            .semantic_id = semantic_id,
            .law_id = law_mod.fromOp(fold.op),
            .metadata = owned_metadata[idx].?,
        },
        .inputs = &doc_fact_reduce_inputs,
    };
    outputs[idx] = .{ .step = step_idx };
    output_idx.* += 1;
}

pub fn planPathFactBucketFoldTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    fold: index_mod.PathFactBucketFoldRequest,
    semantic_id: []const u8,
) !?TensorProgramQueryPlan {
    const outputs = [_]PathFactBucketFoldOutput{.{ .semantic_id = semantic_id, .fold = fold }};
    return try planPathFactBucketFoldOutputsTensorProgramAlloc(alloc, index, &outputs);
}

pub fn planPathFactBucketFoldOutputsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    fold_outputs: []const PathFactBucketFoldOutput,
) !?TensorProgramQueryPlan {
    if (fold_outputs.len == 0) return null;
    const access_path = ir.pathFactAccessPath(index.name);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    const steps = try alloc.alloc(ir.TensorProgramStep, fold_outputs.len + 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = &path_fact_output_dims,
        .owner = access_path.owner,
        .layout = access_path.layout,
    } };

    const outputs = try alloc.alloc(ir.TensorProgramRef, fold_outputs.len);
    errdefer if (outputs.len > 0) alloc.free(outputs);
    const owned_metadata = try alloc.alloc(?[]u8, fold_outputs.len);
    errdefer alloc.free(owned_metadata);
    @memset(owned_metadata, null);
    errdefer for (owned_metadata) |metadata| {
        if (metadata) |bytes| alloc.free(bytes);
    };
    var output_idx: usize = 0;
    for (fold_outputs) |fold_output| {
        try appendPathFactBucketFoldOutput(alloc, fold_output.semantic_id, fold_output.fold, steps, outputs, owned_metadata, &output_idx);
    }

    const program = ir.TensorProgram{
        .steps = steps,
        .output = outputs[0],
        .outputs = outputs,
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        for (owned_metadata) |metadata| {
            if (metadata) |bytes| alloc.free(bytes);
        }
        alloc.free(owned_metadata);
        alloc.free(outputs);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = outputs[0],
        .outputs = outputs,
        .owned_metadata = owned_metadata,
    };
}

fn appendPathFactBucketFoldOutput(
    alloc: std.mem.Allocator,
    semantic_id: []const u8,
    fold: index_mod.PathFactBucketFoldRequest,
    steps: []ir.TensorProgramStep,
    outputs: []ir.TensorProgramRef,
    owned_metadata: []?[]u8,
    output_idx: *usize,
) !void {
    const idx = output_idx.*;
    if (idx >= outputs.len or idx >= owned_metadata.len or idx + 1 >= steps.len) return error.InvalidAlgebraicConfig;
    owned_metadata[idx] = try index_mod.pathFactBucketFoldMetadataAlloc(alloc, fold);
    const step_idx = idx + 1;
    steps[step_idx] = .{
        .expr = .{
            .fragment = .reduce,
            .input_dims = &path_fact_output_dims,
            .output_dims = &bucket_scalar_output_dims,
            .semantic_id = semantic_id,
            .law_id = law_mod.fromOp(fold.op),
            .metadata = owned_metadata[idx].?,
        },
        .inputs = &path_fact_reduce_inputs,
    };
    outputs[idx] = .{ .step = step_idx };
    output_idx.* += 1;
}

pub fn planCardinalityPartialsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    field_or_path: []const u8,
    constraints: []const ir.Constraint,
) !?TensorProgramQueryPlan {
    const uses_pathfact = index_mod.isExplicitJsonPointerPath(field_or_path);
    if (!uses_pathfact and !cardinalityDocFactFieldSupported(index, field_or_path)) return null;
    const access_path = if (uses_pathfact) ir.pathFactAccessPath(index.name) else ir.docFactAccessPath(index.name);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    const steps = try alloc.alloc(ir.TensorProgramStep, 2);
    errdefer alloc.free(steps);
    const input_dims = if (uses_pathfact) &path_fact_output_dims else &doc_fact_output_dims;
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = input_dims,
        .owner = access_path.owner,
        .layout = access_path.layout,
    } };

    const owned_metadata = try alloc.alloc(?[]u8, 1);
    errdefer alloc.free(owned_metadata);
    owned_metadata[0] = try index_mod.cardinalityPartialsMetadataAlloc(alloc, .{
        .aggregation_name = aggregation_name,
        .field_or_path = field_or_path,
        .constraints = constraints,
    });
    errdefer if (owned_metadata[0]) |metadata| alloc.free(metadata);

    steps[1] = .{
        .expr = .{
            .fragment = .reduce,
            .input_dims = input_dims,
            .output_dims = &scalar_output_dims,
            .semantic_id = aggregation_name,
            .law_id = .count,
            .metadata = owned_metadata[0].?,
        },
        .inputs = if (uses_pathfact) &path_fact_reduce_inputs else &doc_fact_reduce_inputs,
    };

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 1 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        if (owned_metadata[0]) |metadata| alloc.free(metadata);
        alloc.free(owned_metadata);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 1 },
        .owned_metadata = owned_metadata,
    };
}

pub fn planTermsCardinalityPartialsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    bucket_field_or_path: []const u8,
    children: []const index_mod.CardinalityChildRequest,
    constraints: []const ir.Constraint,
) !?TensorProgramQueryPlan {
    if (children.len == 0) return null;
    const uses_pathfact = index_mod.isExplicitJsonPointerPath(bucket_field_or_path);
    if (!uses_pathfact and index.fieldConfig(bucket_field_or_path, .group) == null) return null;
    var needs_docfact = !uses_pathfact;
    var needs_pathfact = uses_pathfact;
    for (children) |child| {
        if (index_mod.isExplicitJsonPointerPath(child.field)) {
            needs_pathfact = true;
        } else {
            if (!cardinalityDocFactFieldSupported(index, child.field)) return null;
            needs_docfact = true;
        }
    }
    for (constraints) |constraint| {
        if (index_mod.isExplicitJsonPointerPath(constraint.field)) {
            needs_pathfact = true;
        } else {
            if (index.fieldConfig(constraint.field, .group) == null) return null;
            needs_docfact = true;
        }
    }

    const access_path_count: usize = @as(usize, @intFromBool(needs_docfact)) + @as(usize, @intFromBool(needs_pathfact));
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, access_path_count);
    errdefer alloc.free(access_paths);
    var access_idx: usize = 0;
    if (needs_docfact) {
        access_paths[access_idx] = ir.docFactAccessPath(index.name);
        access_idx += 1;
    }
    if (needs_pathfact) {
        access_paths[access_idx] = ir.pathFactAccessPath(index.name);
        access_idx += 1;
    }

    const primary_access_path = if (uses_pathfact) ir.pathFactAccessPath(index.name) else ir.docFactAccessPath(index.name);
    const input_dims = if (uses_pathfact) &path_fact_output_dims else &doc_fact_output_dims;
    const steps = try alloc.alloc(ir.TensorProgramStep, 2);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = input_dims,
        .owner = primary_access_path.owner,
        .layout = primary_access_path.layout,
    } };

    const owned_metadata = try alloc.alloc(?[]u8, 1);
    errdefer alloc.free(owned_metadata);
    owned_metadata[0] = try index_mod.termsCardinalityPartialsMetadataAlloc(alloc, .{
        .aggregation_name = aggregation_name,
        .bucket_field_or_path = bucket_field_or_path,
        .children = children,
        .constraints = constraints,
    });
    errdefer if (owned_metadata[0]) |metadata| alloc.free(metadata);

    steps[1] = .{
        .expr = .{
            .fragment = .reduce,
            .input_dims = input_dims,
            .output_dims = &bucket_scalar_output_dims,
            .semantic_id = aggregation_name,
            .law_id = .count,
            .metadata = owned_metadata[0].?,
        },
        .inputs = if (uses_pathfact) &path_fact_reduce_inputs else &doc_fact_reduce_inputs,
    };

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 1 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        if (owned_metadata[0]) |metadata| alloc.free(metadata);
        alloc.free(owned_metadata);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 1 },
        .owned_metadata = owned_metadata,
    };
}

pub fn planRangeCardinalityPartialsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    field_or_path: []const u8,
    kind: index_mod.CardinalityRangeKind,
    ranges: []const index_mod.CardinalityRangeRequest,
    children: []const index_mod.CardinalityChildRequest,
    constraints: []const ir.Constraint,
) !?TensorProgramQueryPlan {
    if (ranges.len == 0 or children.len == 0) return null;
    const uses_pathfact = index_mod.isExplicitJsonPointerPath(field_or_path);
    if (!uses_pathfact and !rangeCardinalityDocFactFieldSupported(index, field_or_path, kind)) return null;
    var needs_docfact = !uses_pathfact;
    var needs_pathfact = uses_pathfact;
    for (children) |child| {
        if (index_mod.isExplicitJsonPointerPath(child.field)) {
            needs_pathfact = true;
        } else {
            if (!cardinalityDocFactFieldSupported(index, child.field)) return null;
            needs_docfact = true;
        }
    }
    for (constraints) |constraint| {
        if (index_mod.isExplicitJsonPointerPath(constraint.field)) {
            needs_pathfact = true;
        } else {
            if (index.fieldConfig(constraint.field, .group) == null) return null;
            needs_docfact = true;
        }
    }

    const access_path_count: usize = @as(usize, @intFromBool(needs_docfact)) + @as(usize, @intFromBool(needs_pathfact));
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, access_path_count);
    errdefer alloc.free(access_paths);
    var access_idx: usize = 0;
    if (needs_docfact) {
        access_paths[access_idx] = ir.docFactAccessPath(index.name);
        access_idx += 1;
    }
    if (needs_pathfact) {
        access_paths[access_idx] = ir.pathFactAccessPath(index.name);
        access_idx += 1;
    }

    const primary_access_path = if (uses_pathfact) ir.pathFactAccessPath(index.name) else ir.docFactAccessPath(index.name);
    const input_dims = if (uses_pathfact) &path_fact_output_dims else &doc_fact_output_dims;
    const steps = try alloc.alloc(ir.TensorProgramStep, 2);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = input_dims,
        .owner = primary_access_path.owner,
        .layout = primary_access_path.layout,
    } };

    const owned_metadata = try alloc.alloc(?[]u8, 1);
    errdefer alloc.free(owned_metadata);
    owned_metadata[0] = try index_mod.rangeCardinalityPartialsMetadataAlloc(alloc, .{
        .aggregation_name = aggregation_name,
        .field_or_path = field_or_path,
        .kind = kind,
        .ranges = ranges,
        .children = children,
        .constraints = constraints,
    });
    errdefer if (owned_metadata[0]) |metadata| alloc.free(metadata);

    steps[1] = .{
        .expr = .{
            .fragment = .reduce,
            .input_dims = input_dims,
            .output_dims = &bucket_scalar_output_dims,
            .semantic_id = aggregation_name,
            .law_id = .count,
            .metadata = owned_metadata[0].?,
        },
        .inputs = if (uses_pathfact) &path_fact_reduce_inputs else &doc_fact_reduce_inputs,
    };

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 1 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        if (owned_metadata[0]) |metadata| alloc.free(metadata);
        alloc.free(owned_metadata);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 1 },
        .owned_metadata = owned_metadata,
    };
}

pub fn planHistogramCardinalityPartialsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    field_or_path: []const u8,
    kind: index_mod.CardinalityHistogramKind,
    numeric_interval: f64,
    date_bucket_name: []const u8,
    children: []const index_mod.CardinalityChildRequest,
    constraints: []const ir.Constraint,
) !?TensorProgramQueryPlan {
    if (field_or_path.len == 0 or children.len == 0) return null;
    if (kind == .numeric and numeric_interval <= 0) return null;
    const uses_pathfact = index_mod.isExplicitJsonPointerPath(field_or_path);
    if (!uses_pathfact and !histogramCardinalityDocFactFieldSupported(index, field_or_path, kind)) return null;
    var needs_docfact = !uses_pathfact;
    var needs_pathfact = uses_pathfact;
    for (children) |child| {
        if (index_mod.isExplicitJsonPointerPath(child.field)) {
            needs_pathfact = true;
        } else {
            if (!cardinalityDocFactFieldSupported(index, child.field)) return null;
            needs_docfact = true;
        }
    }
    for (constraints) |constraint| {
        if (index_mod.isExplicitJsonPointerPath(constraint.field)) {
            needs_pathfact = true;
        } else {
            if (index.fieldConfig(constraint.field, .group) == null) return null;
            needs_docfact = true;
        }
    }

    const access_path_count: usize = @as(usize, @intFromBool(needs_docfact)) + @as(usize, @intFromBool(needs_pathfact));
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, access_path_count);
    errdefer alloc.free(access_paths);
    var access_idx: usize = 0;
    if (needs_docfact) {
        access_paths[access_idx] = ir.docFactAccessPath(index.name);
        access_idx += 1;
    }
    if (needs_pathfact) {
        access_paths[access_idx] = ir.pathFactAccessPath(index.name);
        access_idx += 1;
    }

    const primary_access_path = if (uses_pathfact) ir.pathFactAccessPath(index.name) else ir.docFactAccessPath(index.name);
    const input_dims = if (uses_pathfact) &path_fact_output_dims else &doc_fact_output_dims;
    const steps = try alloc.alloc(ir.TensorProgramStep, 2);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .slice,
        .output_dims = input_dims,
        .owner = primary_access_path.owner,
        .layout = primary_access_path.layout,
    } };

    const owned_metadata = try alloc.alloc(?[]u8, 1);
    errdefer alloc.free(owned_metadata);
    owned_metadata[0] = try index_mod.histogramCardinalityPartialsMetadataAlloc(alloc, .{
        .aggregation_name = aggregation_name,
        .field_or_path = field_or_path,
        .kind = kind,
        .numeric_interval = numeric_interval,
        .date_bucket_name = date_bucket_name,
        .children = children,
        .constraints = constraints,
    });
    errdefer if (owned_metadata[0]) |metadata| alloc.free(metadata);

    steps[1] = .{
        .expr = .{
            .fragment = .reduce,
            .input_dims = input_dims,
            .output_dims = &bucket_scalar_output_dims,
            .semantic_id = aggregation_name,
            .law_id = .count,
            .metadata = owned_metadata[0].?,
        },
        .inputs = if (uses_pathfact) &path_fact_reduce_inputs else &doc_fact_reduce_inputs,
    };

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 1 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        if (owned_metadata[0]) |metadata| alloc.free(metadata);
        alloc.free(owned_metadata);
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 1 },
        .owned_metadata = owned_metadata,
    };
}

fn cardinalityDocFactFieldSupported(index: *const index_mod.Index, field_or_path: []const u8) bool {
    var count: usize = 0;
    if (index.fieldConfig(field_or_path, .group) != null) count += 1;
    if (index.fieldConfig(field_or_path, .measure) != null) count += 1;
    if (index.fieldConfig(field_or_path, .time) != null) count += 1;
    return count == 1;
}

fn rangeCardinalityDocFactFieldSupported(index: *const index_mod.Index, field_or_path: []const u8, kind: index_mod.CardinalityRangeKind) bool {
    return switch (kind) {
        .numeric => blk: {
            if (index.fieldConfig(field_or_path, .measure) != null) break :blk true;
            const group_field = index.fieldConfig(field_or_path, .group) orelse break :blk false;
            const bucket_kind = value_mod.kindFromFieldType(group_field.type);
            break :blk bucket_kind == .number or bucket_kind == .integer;
        },
        .date => index.fieldConfig(field_or_path, .time) != null,
    };
}

fn histogramCardinalityDocFactFieldSupported(index: *const index_mod.Index, field_or_path: []const u8, kind: index_mod.CardinalityHistogramKind) bool {
    return switch (kind) {
        .numeric => blk: {
            if (index.fieldConfig(field_or_path, .measure) != null) break :blk true;
            const group_field = index.fieldConfig(field_or_path, .group) orelse break :blk false;
            const bucket_kind = value_mod.kindFromFieldType(group_field.type);
            break :blk bucket_kind == .number or bucket_kind == .integer;
        },
        .date => index.fieldConfig(field_or_path, .time) != null,
    };
}

pub fn planDerivedJoinRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    join_ref: ir.JoinRef,
    range_field: DerivedJoinRangeField,
    ranges: []const RangeBound,
    child_metrics: []const ir.Metric,
    constraints: []const ir.Constraint,
) !?TensorProgramQueryPlan {
    if (ranges.len == 0) return null;
    const join_cfg = joinConfigByName(index, join_ref.name) orelse return null;
    if (!derivedJoinConstraintsSupported(index, constraints)) return null;
    const outputs_per_range = 1 + child_metrics.len;
    const fold_outputs = try alloc.alloc(DerivedJoinFoldOutput, ranges.len * outputs_per_range);
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    for (ranges) |range| {
        try appendDerivedJoinRangeFoldOutput(index, join_cfg, join_ref, range_field, aggregation_name, .{ .name = aggregation_name, .op = .count }, constraints, range.start, range.end, fold_outputs, &output_idx);
        for (child_metrics) |metric| {
            try appendDerivedJoinRangeFoldOutput(index, join_cfg, join_ref, range_field, metric.name, metric, constraints, range.start, range.end, fold_outputs, &output_idx);
        }
    }
    return try planDerivedJoinFoldOutputsTensorProgramAlloc(alloc, fold_outputs);
}

fn appendDerivedJoinRangeFoldOutput(
    index: *const index_mod.Index,
    join_cfg: index_mod.JoinConfig,
    join_ref: ir.JoinRef,
    range_field: DerivedJoinRangeField,
    semantic_id: []const u8,
    metric: ir.Metric,
    constraints: []const ir.Constraint,
    range_start: ?[]const u8,
    range_end: ?[]const u8,
    outputs: []DerivedJoinFoldOutput,
    output_idx: *usize,
) !void {
    if (output_idx.* >= outputs.len) return error.InvalidAlgebraicConfig;
    const law_id = law_mod.fromOp(metric.op);
    if (!join_mod.queryRewriteProof(join_cfg, join_ref, .{
        .kind = .derived_distributive_fold,
        .law_id = law_id,
        .bounded_fanout = join_cfg.max_fanout != null,
    }).safe()) return error.UnsupportedQueryRequest;
    const measure = switch (metric.op) {
        .count => null,
        .sum, .sumsquares, .min, .max, .avg => blk: {
            const field = index.fieldConfig(metric.field, .measure) orelse return error.UnsupportedQueryRequest;
            break :blk field.name;
        },
    };
    outputs[output_idx.*] = .{
        .semantic_id = semantic_id,
        .fold = .{
            .join = join_ref,
            .op = metric.op,
            .range_field = range_field.name,
            .range_role = range_field.role,
            .range_kind = range_field.kind,
            .range_start = range_start,
            .range_end = range_end,
            .measure = measure,
            .constraints = constraints,
        },
    };
    output_idx.* += 1;
}

pub fn planDocFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    measure_field: []const u8,
    interval: f64,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    if (interval <= 0) return null;
    const total_outputs = 1 + child_metrics.len;
    const fold_outputs = try alloc.alloc(DocFactBucketFoldOutput, total_outputs);
    defer alloc.free(fold_outputs);
    fold_outputs[0] = .{
        .semantic_id = aggregation_name,
        .fold = .{
            .kind = .histogram,
            .op = .count,
            .bucket_field = measure_field,
            .bucket_role = .measure,
            .histogram_interval = interval,
            .constraints = constraints,
        },
    };
    for (child_metrics, 0..) |metric, i| {
        fold_outputs[i + 1] = .{
            .semantic_id = metric.name,
            .fold = .{
                .kind = .histogram,
                .op = metric.op,
                .bucket_field = measure_field,
                .bucket_role = .measure,
                .histogram_interval = interval,
                .measure = if (metric.op == .count) null else metric.field,
                .constraints = constraints,
            },
        };
    }
    return try planDocFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs);
}

pub fn planDocFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    measure_field: []const u8,
    ranges: []const RangeBound,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    if (ranges.len == 0) return null;
    const outputs_per_range = 1 + child_metrics.len;
    const fold_outputs = try alloc.alloc(DocFactBucketFoldOutput, ranges.len * outputs_per_range);
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    for (ranges) |range| {
        fold_outputs[output_idx] = .{
            .semantic_id = aggregation_name,
            .fold = .{
                .kind = .range,
                .op = .count,
                .bucket_field = measure_field,
                .bucket_role = .measure,
                .range_start = range.start,
                .range_end = range.end,
                .constraints = constraints,
            },
        };
        output_idx += 1;
        for (child_metrics) |metric| {
            fold_outputs[output_idx] = .{
                .semantic_id = metric.name,
                .fold = .{
                    .kind = .range,
                    .op = metric.op,
                    .bucket_field = measure_field,
                    .bucket_role = .measure,
                    .range_start = range.start,
                    .range_end = range.end,
                    .measure = if (metric.op == .count) null else metric.field,
                    .constraints = constraints,
                },
            };
            output_idx += 1;
        }
    }
    return try planDocFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs);
}

pub fn planDocFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    time_field: []const u8,
    ranges: []const RangeBound,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    if (ranges.len == 0) return null;
    const outputs_per_range = 1 + child_metrics.len;
    const fold_outputs = try alloc.alloc(DocFactBucketFoldOutput, ranges.len * outputs_per_range);
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    for (ranges) |range| {
        fold_outputs[output_idx] = .{
            .semantic_id = aggregation_name,
            .fold = .{
                .kind = .date_range,
                .op = .count,
                .bucket_field = time_field,
                .bucket_role = .time,
                .range_start = range.start,
                .range_end = range.end,
                .constraints = constraints,
            },
        };
        output_idx += 1;
        for (child_metrics) |metric| {
            fold_outputs[output_idx] = .{
                .semantic_id = metric.name,
                .fold = .{
                    .kind = .date_range,
                    .op = metric.op,
                    .bucket_field = time_field,
                    .bucket_role = .time,
                    .range_start = range.start,
                    .range_end = range.end,
                    .measure = if (metric.op == .count) null else metric.field,
                    .constraints = constraints,
                },
            };
            output_idx += 1;
        }
    }
    return try planDocFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs);
}

pub fn planPathFactTermsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    bucket_path: []const u8,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    var outputs_per_kind: usize = 1;
    for (child_metrics) |metric| outputs_per_kind += pathFactMeasureKindOutputCount(index, metric.op);
    const bucket_kinds = [_]pathfact_mod.Kind{ .string, .number, .bool, .null, .object, .array };
    const fold_outputs = try alloc.alloc(PathFactBucketFoldOutput, bucket_kinds.len * outputs_per_kind);
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    for (bucket_kinds) |kind| {
        fold_outputs[output_idx] = .{
            .semantic_id = aggregation_name,
            .fold = .{
                .kind = .terms,
                .op = .count,
                .bucket_path = bucket_path,
                .bucket_kind = kind,
                .constraints = constraints,
            },
        };
        output_idx += 1;
        try appendPathFactMetricOutputs(index, bucket_path, kind, null, 0, null, null, constraints, child_metrics, fold_outputs, &output_idx);
    }
    return try planPathFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs[0..output_idx]);
}

pub fn planPathFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    bucket_path: []const u8,
    interval: f64,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    if (interval <= 0) return null;
    const total_outputs = pathFactNumericKindOutputCount(index) * pathFactBucketOutputsPerKind(index, child_metrics);
    const fold_outputs = try alloc.alloc(PathFactBucketFoldOutput, total_outputs);
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    const kinds = [_]pathfact_mod.Kind{ .number, .string };
    for (kinds) |kind| {
        if (kind == .string and !pathFactPlannerAllowsNumericString(index)) continue;
        fold_outputs[output_idx] = .{
            .semantic_id = aggregation_name,
            .fold = .{
                .kind = .histogram,
                .op = .count,
                .bucket_path = bucket_path,
                .bucket_kind = kind,
                .histogram_interval = interval,
                .constraints = constraints,
            },
        };
        output_idx += 1;
        try appendPathFactMetricOutputs(index, bucket_path, kind, .histogram, interval, null, null, constraints, child_metrics, fold_outputs, &output_idx);
    }
    return try planPathFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs[0..output_idx]);
}

pub fn planPathFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    bucket_path: []const u8,
    ranges: []const RangeBound,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    if (ranges.len == 0) return null;
    const fold_outputs = try alloc.alloc(PathFactBucketFoldOutput, ranges.len * pathFactNumericKindOutputCount(index) * pathFactBucketOutputsPerKind(index, child_metrics));
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    const kinds = [_]pathfact_mod.Kind{ .number, .string };
    for (ranges) |range| {
        for (kinds) |kind| {
            if (kind == .string and !pathFactPlannerAllowsNumericString(index)) continue;
            fold_outputs[output_idx] = .{
                .semantic_id = aggregation_name,
                .fold = .{
                    .kind = .range,
                    .op = .count,
                    .bucket_path = bucket_path,
                    .bucket_kind = kind,
                    .range_start = range.start,
                    .range_end = range.end,
                    .constraints = constraints,
                },
            };
            output_idx += 1;
            try appendPathFactMetricOutputs(index, bucket_path, kind, .range, 0, range.start, range.end, constraints, child_metrics, fold_outputs, &output_idx);
        }
    }
    return try planPathFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs[0..output_idx]);
}

pub fn planPathFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    bucket_path: []const u8,
    ranges: []const RangeBound,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
) !?TensorProgramQueryPlan {
    if (ranges.len == 0 or !pathFactPlannerAllowsDatetimeString(index)) return null;
    const fold_outputs = try alloc.alloc(PathFactBucketFoldOutput, ranges.len * pathFactDateRangeOutputsPerRange(index, child_metrics));
    defer alloc.free(fold_outputs);
    var output_idx: usize = 0;
    for (ranges) |range| {
        fold_outputs[output_idx] = .{
            .semantic_id = aggregation_name,
            .fold = .{
                .kind = .date_range,
                .op = .count,
                .bucket_path = bucket_path,
                .bucket_kind = .string,
                .range_start = range.start,
                .range_end = range.end,
                .constraints = constraints,
            },
        };
        output_idx += 1;
        try appendPathFactMetricOutputs(index, bucket_path, .string, .date_range, 0, range.start, range.end, constraints, child_metrics, fold_outputs, &output_idx);
    }
    return try planPathFactBucketFoldOutputsTensorProgramAlloc(alloc, index, fold_outputs[0..output_idx]);
}

fn appendPathFactMetricOutputs(
    index: *const index_mod.Index,
    bucket_path: []const u8,
    bucket_kind: pathfact_mod.Kind,
    fold_kind: ?index_mod.DocFactBucketFoldKind,
    histogram_interval: f64,
    range_start: ?[]const u8,
    range_end: ?[]const u8,
    constraints: []const ir.Constraint,
    child_metrics: []const ir.Metric,
    fold_outputs: []PathFactBucketFoldOutput,
    output_idx: *usize,
) !void {
    for (child_metrics) |metric| {
        if (output_idx.* >= fold_outputs.len) return error.InvalidAlgebraicConfig;
        if (metric.op == .count) {
            fold_outputs[output_idx.*] = .{
                .semantic_id = metric.name,
                .fold = .{
                    .kind = fold_kind orelse .terms,
                    .op = metric.op,
                    .bucket_path = bucket_path,
                    .bucket_kind = bucket_kind,
                    .histogram_interval = histogram_interval,
                    .range_start = range_start,
                    .range_end = range_end,
                    .constraints = constraints,
                },
            };
            output_idx.* += 1;
            continue;
        }
        const measure_kinds = [_]pathfact_mod.Kind{ .number, .string };
        for (measure_kinds) |measure_kind| {
            if (measure_kind == .string and !pathFactPlannerAllowsNumericString(index)) continue;
            if (output_idx.* >= fold_outputs.len) return error.InvalidAlgebraicConfig;
            fold_outputs[output_idx.*] = .{
                .semantic_id = metric.name,
                .fold = .{
                    .kind = fold_kind orelse .terms,
                    .op = metric.op,
                    .bucket_path = bucket_path,
                    .bucket_kind = bucket_kind,
                    .histogram_interval = histogram_interval,
                    .range_start = range_start,
                    .range_end = range_end,
                    .measure_path = metric.field,
                    .measure_kind = measure_kind,
                    .constraints = constraints,
                },
            };
            output_idx.* += 1;
        }
    }
}

fn pathFactDateRangeOutputsPerRange(index: *const index_mod.Index, child_metrics: []const ir.Metric) usize {
    return pathFactBucketOutputsPerKind(index, child_metrics);
}

fn pathFactBucketOutputsPerKind(index: *const index_mod.Index, child_metrics: []const ir.Metric) usize {
    var count: usize = 1;
    for (child_metrics) |metric| count += pathFactMeasureKindOutputCount(index, metric.op);
    return count;
}

fn pathFactNumericKindOutputCount(index: *const index_mod.Index) usize {
    return if (pathFactPlannerAllowsNumericString(index)) 2 else 1;
}

fn pathFactMeasureKindOutputCount(index: *const index_mod.Index, op: algebra.Op) usize {
    return if (op == .count) 1 else pathFactNumericKindOutputCount(index);
}

fn pathFactPlannerAllowsNumericString(index: *const index_mod.Index) bool {
    return index.parsed.value.pathfact_policy.allow_numeric_string_coercion;
}

fn pathFactPlannerAllowsDatetimeString(index: *const index_mod.Index) bool {
    return index.parsed.value.pathfact_policy.allow_datetime_string_coercion;
}

pub fn planVectorSearchTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    layout: ir.PhysicalLayout,
    constrained: bool,
) !?TensorProgramQueryPlan {
    if (layout != .dense_vector and layout != .sparse_vector) return null;
    const access_path = ir.vectorAccessPath(index_name, layout);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    var inputs: []ir.TensorExpr = if (constrained) try alloc.alloc(ir.TensorExpr, 1) else &.{};
    errdefer if (inputs.len > 0) alloc.free(inputs);
    if (constrained) {
        inputs[0] = .{
            .fragment = .slice,
            .output_dims = &native_doc_id_constraint_dims,
            .semantic_id = "native_doc_id_constraints",
        };
    }

    const steps = try alloc.alloc(ir.TensorProgramStep, 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .vector_search,
        .output_dims = &vector_search_output_dims,
        .owner = access_path.owner,
        .layout = access_path.layout,
    }, .inputs = if (constrained) &vector_doc_id_constraint_inputs else &.{} };

    const program = ir.TensorProgram{
        .inputs = inputs,
        .steps = steps,
        .output = .{ .step = 0 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(steps);
        if (inputs.len > 0) alloc.free(inputs);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .inputs = inputs,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
    };
}

pub fn planGraphTraversalTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    constrained_targets: bool,
) !?TensorProgramQueryPlan {
    const access_path = ir.graphReachabilityAccessPath(index_name);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    var inputs: []ir.TensorExpr = if (constrained_targets) try alloc.alloc(ir.TensorExpr, 1) else &.{};
    errdefer if (inputs.len > 0) alloc.free(inputs);
    if (constrained_targets) {
        inputs[0] = .{
            .fragment = .slice,
            .output_dims = &native_doc_id_constraint_dims,
            .semantic_id = "graph_target_constraints",
        };
    }

    const steps = try alloc.alloc(ir.TensorProgramStep, 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .graph_traverse,
        .layout = .graph_edges,
        .output_dims = &graph_traverse_output_dims,
        .owner = access_path.owner,
        .law_id = .provenance_semiring,
    }, .inputs = if (constrained_targets) &graph_target_constraint_inputs else &.{} };

    const program = ir.TensorProgram{
        .inputs = inputs,
        .steps = steps,
        .output = .{ .step = 0 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(steps);
        if (inputs.len > 0) alloc.free(inputs);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .inputs = inputs,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
    };
}

pub fn planGraphEdgesTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
) !?TensorProgramQueryPlan {
    const access_path = ir.graphEdgeAccessPath(index_name);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    const steps = try alloc.alloc(ir.TensorProgramStep, 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .graph_traverse,
        .layout = .graph_edges,
        .output_dims = &graph_edge_output_dims,
        .owner = access_path.owner,
        .law_id = .provenance_semiring,
    } };

    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 0 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
    };
}

fn tensorProgramForMetricPlanAlloc(
    alloc: std.mem.Allocator,
    metric_plan: MetricPlan,
) !?TensorProgramQueryPlan {
    const access_path = materializationAccessPath(metric_plan.materialization) orelse return null;
    const expr = tensorExprForMetricPlan(metric_plan, access_path);
    if (!ir.accessPathCanSatisfy(access_path, expr).safe()) return null;

    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;
    const steps = try alloc.alloc(ir.TensorProgramStep, 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = expr };
    const program = ir.TensorProgram{
        .steps = steps,
        .output = .{ .step = 0 },
    };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(steps);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
    };
}

fn tensorExprForMetricPlan(metric_plan: MetricPlan, access_path: ir.PhysicalAccessPath) ir.TensorExpr {
    return .{
        .fragment = if (metric_plan.materialization.join != null) .join else .reduce,
        .output_dims = access_path.output_dims,
        .semantic_id = metric_plan.materialization.name,
        .owner = access_path.owner,
        .layout = access_path.layout,
        .law_id = law_mod.fromOp(metric_plan.op),
    };
}

fn namedMaterialization(index: *const index_mod.Index, name: []const u8) ?index_mod.MaterializationConfig {
    for (index.config().materializations) |mat| {
        if (std.mem.eql(u8, mat.name, name)) return mat;
    }
    return null;
}

fn derivedJoinStepForFold(fold: index_mod.DerivedJoinFoldRequest, semantic_id: []const u8, metadata: []const u8) ir.TensorProgramStep {
    return .{
        .expr = .{
            .fragment = .join,
            .input_dims = &join_fact_output_dims,
            .output_dims = derivedJoinFoldOutputDims(fold),
            .semantic_id = semantic_id,
            .law_id = law_mod.fromOp(fold.op),
            .metadata = metadata,
        },
        .inputs = &derived_join_step_inputs,
    };
}

fn derivedJoinAccessPath(join_ref: ir.JoinRef) ir.PhysicalAccessPath {
    return .{
        .owner = join_ref.name,
        .layout = .join_fact_rows,
        .fragments = &join_fact_fragments,
        .output_dims = &join_fact_output_dims,
    };
}

fn derivedJoinFoldOutputDims(fold: index_mod.DerivedJoinFoldRequest) []const ir.Dimension {
    if (fold.time_field != null) return &time_bucket_scalar_output_dims;
    if (fold.group_by.len > 0 or fold.histogram_field != null or fold.range_field != null) return &bucket_scalar_output_dims;
    return &scalar_output_dims;
}

fn materializationOutputDims(mat: index_mod.MaterializationConfig) []const ir.Dimension {
    if (mat.time != null) return &time_bucket_scalar_output_dims;
    if (mat.group_by.len > 0 or mat.axes.len > 0) return &bucket_scalar_output_dims;
    return &scalar_output_dims;
}

fn materializationLawIds(op: algebra.Op) []const law_mod.Id {
    return switch (op) {
        .count => &count_laws,
        .sum => &sum_laws,
        .sumsquares => &sumsquares_laws,
        .avg => &avg_laws,
        .min => &min_laws,
        .max => &max_laws,
    };
}

fn reduceExpr(op: algebra.Op, output_dims: []const ir.Dimension) ir.TensorExpr {
    return .{
        .fragment = .reduce,
        .output_dims = output_dims,
        .law_id = law_mod.fromOp(op),
    };
}

fn joinExpr(op: algebra.Op, output_dims: []const ir.Dimension) ir.TensorExpr {
    return .{
        .fragment = .join,
        .output_dims = output_dims,
        .law_id = law_mod.fromOp(op),
    };
}

fn exprWithOwner(expr: ir.TensorExpr, owner: []const u8) ir.TensorExpr {
    var out = expr;
    out.owner = owner;
    return out;
}

fn queryOutputDims(has_bucket_output: bool, has_time_output: bool) []const ir.Dimension {
    if (has_time_output) return &time_bucket_scalar_output_dims;
    if (has_bucket_output) return &bucket_scalar_output_dims;
    return &scalar_output_dims;
}

fn queryBucketFieldCount(query: ir.Query) usize {
    return if (query.bucket_fields.len > 0) query.bucket_fields.len else @intFromBool(query.bucket_field != null);
}

pub fn planMetric(
    index: *const index_mod.Index,
    aggregation_name: []const u8,
    op_text: []const u8,
    query_measure_field: []const u8,
    query_group_fields: []const []const u8,
    query_time_field: ?[]const u8,
    query_bucket: ?[]const u8,
) ?MetricPlan {
    const op = algebra.Op.parse(op_text) orelse return null;
    const expr = reduceExpr(op, queryOutputDims(query_group_fields.len > 0, query_time_field != null));
    if (planTensorExpr(index, exprWithOwner(expr, aggregation_name))) |typed_plan| {
        const mat = typed_plan.materialization;
        if ((mat.join == null or std.mem.eql(u8, mat.name, aggregation_name)) and
            materializationMatches(index, mat, op, query_measure_field, query_group_fields, query_time_field, query_bucket))
        {
            return .{ .materialization = mat, .op = op };
        }
    }
    var exact: ?MetricPlan = null;
    var compatible: ?MetricPlan = null;
    var compatible_count: usize = 0;
    for (index.config().materializations) |mat| {
        if (mat.join != null and !std.mem.eql(u8, mat.name, aggregation_name)) continue;
        if (!materializationMatches(index, mat, op, query_measure_field, query_group_fields, query_time_field, query_bucket)) continue;
        if (!materializationCanSatisfy(mat, expr).safe()) continue;
        const plan: MetricPlan = .{ .materialization = mat, .op = op };
        if (std.mem.eql(u8, mat.name, aggregation_name)) exact = plan;
        compatible = plan;
        compatible_count += 1;
    }
    if (exact) |plan| return plan;
    if (compatible_count == 1) return compatible;
    return null;
}

pub fn planBucketCount(
    index: *const index_mod.Index,
    bucket_name: []const u8,
    query_group_fields: []const []const u8,
    query_time_field: ?[]const u8,
    query_bucket: ?[]const u8,
) ?MetricPlan {
    return planMetric(index, bucket_name, "count", "", query_group_fields, query_time_field, query_bucket);
}

pub fn planMetricQuery(
    index: *const index_mod.Index,
    query: ir.Query,
) PlanResult {
    if (!index.plannerLifecycleReady()) return .{ .kind = .unsupported, .fallback_reason = .schema_lifecycle_not_ready };
    const metric = query.metric orelse return .{ .kind = .unsupported, .fallback_reason = .unsupported_type };
    if (!joinSupportedByConfig(index, query.join)) return .{ .kind = .unsupported, .fallback_reason = .unsupported_join };

    const expr = reduceExpr(metric.op, &scalar_output_dims);
    if (planTensorExpr(index, exprWithOwner(expr, metric.name))) |typed_plan| {
        const mat = typed_plan.materialization;
        if (materializationMatchesMetricQuery(index, mat, query, metric) and
            (mat.join == null or materializationCanSatisfy(mat, joinExpr(metric.op, &scalar_output_dims)).safe()))
        {
            return .{
                .kind = if (mat.group_by.len == query.constraints.len) .exact_materialized else .rollup_materialized,
                .metric = .{ .materialization = mat, .op = metric.op },
                .join_kind = joinKindForMaterialization(index, mat),
            };
        }
    }

    var exact: ?MetricPlan = null;
    var compatible: ?MetricPlan = null;
    var compatible_count: usize = 0;
    for (index.config().materializations) |mat| {
        if (!materializationMatchesMetricQuery(index, mat, query, metric)) continue;
        if (!materializationCanSatisfy(mat, expr).safe()) continue;
        if (mat.join != null and !materializationCanSatisfy(mat, joinExpr(metric.op, &scalar_output_dims)).safe()) continue;
        const plan: MetricPlan = .{ .materialization = mat, .op = metric.op };
        if (std.mem.eql(u8, mat.name, metric.name)) exact = plan;
        compatible = plan;
        compatible_count += 1;
    }
    const selected = exact orelse if (compatible_count == 1) compatible else null;
    if (selected) |metric_plan| {
        return .{
            .kind = if (metric_plan.materialization.group_by.len == query.constraints.len) .exact_materialized else .rollup_materialized,
            .metric = metric_plan,
            .join_kind = joinKindForMaterialization(index, metric_plan.materialization),
        };
    }
    if (planDerivedJoinFoldQuery(index, query)) |derived| {
        return .{
            .kind = .derived_join_fold,
            .derived_join_fold = derived,
            .join_kind = joinKindForJoinRef(index, derived.join),
        };
    }
    return .{ .kind = .unsupported, .fallback_reason = if (compatible_count > 1) .ambiguous_materialization else .no_materialization };
}

pub fn planBucketCountQuery(
    index: *const index_mod.Index,
    query: ir.Query,
) PlanResult {
    if (!index.plannerLifecycleReady()) return .{ .kind = .unsupported, .fallback_reason = .schema_lifecycle_not_ready };
    if (!joinSupportedByConfig(index, query.join)) return .{ .kind = .unsupported, .fallback_reason = .unsupported_join };

    const bucket_field_count = queryBucketFieldCount(query);
    const expr = reduceExpr(.count, queryOutputDims(bucket_field_count > 0, query.time_field != null));
    const join_expr = joinExpr(.count, queryOutputDims(bucket_field_count > 0, query.time_field != null));
    if (planTensorExpr(index, exprWithOwner(expr, query.aggregation_name))) |typed_plan| {
        const mat = typed_plan.materialization;
        if (materializationMatchesBucketCountQuery(index, mat, query) and
            (mat.join == null or materializationCanSatisfy(mat, join_expr).safe()))
        {
            return .{
                .kind = if (mat.group_by.len == query.constraints.len + bucket_field_count) .exact_materialized else .rollup_materialized,
                .count_metric = .{ .materialization = mat, .op = .count },
                .join_kind = joinKindForMaterialization(index, mat),
            };
        }
    }

    var exact: ?MetricPlan = null;
    var compatible: ?MetricPlan = null;
    var compatible_count: usize = 0;
    for (index.config().materializations) |mat| {
        if (!materializationMatchesBucketCountQuery(index, mat, query)) continue;
        if (!materializationCanSatisfy(mat, expr).safe()) continue;
        if (mat.join != null and !materializationCanSatisfy(mat, join_expr).safe()) continue;
        const plan: MetricPlan = .{ .materialization = mat, .op = .count };
        if (std.mem.eql(u8, mat.name, query.aggregation_name)) exact = plan;
        compatible = plan;
        compatible_count += 1;
    }
    const selected = exact orelse if (compatible_count == 1) compatible else null;
    if (selected) |count_plan| {
        return .{
            .kind = if (count_plan.materialization.group_by.len == query.constraints.len + bucket_field_count) .exact_materialized else .rollup_materialized,
            .count_metric = count_plan,
            .join_kind = joinKindForMaterialization(index, count_plan.materialization),
        };
    }
    return .{ .kind = .unsupported, .fallback_reason = if (compatible_count > 1) .ambiguous_materialization else .no_materialization };
}

pub fn planDerivedJoinFoldQuery(index: *const index_mod.Index, query: ir.Query) ?index_mod.DerivedJoinFoldRequest {
    if (!index.plannerLifecycleReady()) return null;
    if (query.kind != .metric) return null;
    if (!derivedJoinConstraintsSupported(index, query.constraints)) return null;
    if (query.time_field != null or query.time_bucket != null) return null;
    const join_ref = query.join orelse return null;
    const join_cfg = joinConfigByName(index, join_ref.name) orelse return null;
    const metric = query.metric orelse return null;
    const law_id = law_mod.fromOp(metric.op);
    if (!join_mod.queryRewriteProof(join_cfg, join_ref, .{
        .kind = .derived_distributive_fold,
        .law_id = law_id,
        .bounded_fanout = join_cfg.max_fanout != null,
    }).safe()) return null;
    const measure = switch (metric.op) {
        .count => null,
        .sum, .sumsquares, .min, .max, .avg => blk: {
            const field = index.fieldConfig(metric.field, .measure) orelse return null;
            break :blk field.name;
        },
    };
    return .{
        .join = join_ref,
        .op = metric.op,
        .group_by = &.{},
        .measure = measure,
        .constraints = query.constraints,
    };
}

pub fn planBucketQueryAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    query: ir.Query,
) !PlanResult {
    var result = planBucketCountQuery(index, query);
    if (result.kind == .unsupported) {
        if (try planDerivedJoinBucketCountQueryAlloc(alloc, index, query)) |derived| return derived;
        return result;
    }
    const count_plan = result.count_metric orelse return .{ .kind = .unsupported, .fallback_reason = .no_materialization };

    const child_metrics = try alloc.alloc(MetricPlan, query.child_metrics.len);
    errdefer if (child_metrics.len > 0) alloc.free(child_metrics);
    for (query.child_metrics, 0..) |metric, i| {
        child_metrics[i] = planMetricForGroupLayout(
            index,
            metric,
            count_plan.materialization.group_by,
            query.time_field,
            query.time_bucket,
            query.join,
        ) orelse {
            if (child_metrics.len > 0) alloc.free(child_metrics);
            result.kind = .unsupported;
            result.count_metric = null;
            result.fallback_reason = .child_metric_unsupported;
            return result;
        };
    }
    result.child_metrics = child_metrics;
    return result;
}

fn planDerivedJoinBucketCountQueryAlloc(
    alloc: std.mem.Allocator,
    index: *const index_mod.Index,
    query: ir.Query,
) !?PlanResult {
    if (!index.plannerLifecycleReady()) return null;
    if (!derivedJoinConstraintsSupported(index, query.constraints)) return null;
    const join_ref = query.join orelse return null;
    const join_cfg = joinConfigByName(index, join_ref.name) orelse return null;
    if (!join_mod.queryRewriteProof(join_cfg, join_ref, .{
        .kind = .derived_distributive_fold,
        .law_id = .count,
        .bounded_fanout = join_cfg.max_fanout != null,
    }).safe()) return null;
    var group_by_owned = false;
    var group_by: []const []const u8 = &.{};
    var histogram_field: ?[]const u8 = null;
    var histogram_role: ?fact_mod.Role = null;
    var histogram_interval: f64 = 0;
    var time_field: ?[]const u8 = null;
    var time_bucket: ?[]const u8 = null;
    switch (query.kind) {
        .terms => {
            if (query.time_field != null or query.time_bucket != null) return null;
            if (query.bucket_fields.len > 0) return null;
            const bucket_field_name = query.bucket_field orelse return null;
            const bucket_field = index.fieldConfig(bucket_field_name, .group) orelse return null;
            const owned = try alloc.alloc([]const u8, 1);
            owned[0] = bucket_field.name;
            group_by = owned;
            group_by_owned = true;
        },
        .histogram => {
            if (query.time_field != null or query.time_bucket != null) return null;
            if (query.bucket_interval <= 0) return null;
            const histogram = derivedJoinHistogramField(index, query.bucket_field orelse return null) orelse return null;
            histogram_field = histogram.name;
            histogram_role = histogram.role;
            histogram_interval = query.bucket_interval;
        },
        .date_histogram => {
            if (query.bucket_field != null or query.bucket_fields.len > 0) return null;
            const time_field_name = query.time_field orelse return null;
            const field = index.fieldConfig(time_field_name, .time) orelse return null;
            time_field = field.name;
            time_bucket = query.time_bucket orelse return null;
        },
        .metric => return null,
    }
    errdefer if (group_by_owned and group_by.len > 0) alloc.free(@constCast(group_by));
    const child_folds = try alloc.alloc(index_mod.DerivedJoinFoldRequest, query.child_metrics.len);
    errdefer if (child_folds.len > 0) alloc.free(child_folds);
    for (query.child_metrics, 0..) |metric, i| {
        child_folds[i] = derivedJoinFoldRequestForMetric(index, join_cfg, join_ref, metric, group_by, histogram_field, histogram_role, histogram_interval, time_field, time_bucket, query.constraints) orelse {
            if (child_folds.len > 0) alloc.free(child_folds);
            if (group_by_owned and group_by.len > 0) alloc.free(@constCast(group_by));
            return .{ .kind = .unsupported, .fallback_reason = .child_metric_unsupported };
        };
    }
    return .{
        .kind = .derived_join_fold,
        .derived_join_fold = .{
            .join = join_ref,
            .op = .count,
            .group_by = group_by,
            .histogram_field = histogram_field,
            .histogram_role = histogram_role,
            .histogram_interval = histogram_interval,
            .time_field = time_field,
            .time_bucket = time_bucket,
            .measure = null,
            .constraints = query.constraints,
        },
        .derived_child_join_folds = child_folds,
        .derived_join_group_by_owned = group_by_owned,
        .join_kind = joinKindForJoinRef(index, join_ref),
    };
}

fn derivedJoinFoldRequestForMetric(
    index: *const index_mod.Index,
    join_cfg: index_mod.JoinConfig,
    join_ref: ir.JoinRef,
    metric: ir.Metric,
    group_by: []const []const u8,
    histogram_field: ?[]const u8,
    histogram_role: ?fact_mod.Role,
    histogram_interval: f64,
    time_field: ?[]const u8,
    time_bucket: ?[]const u8,
    constraints: []const ir.Constraint,
) ?index_mod.DerivedJoinFoldRequest {
    const law_id = law_mod.fromOp(metric.op);
    if (!join_mod.queryRewriteProof(join_cfg, join_ref, .{
        .kind = .derived_distributive_fold,
        .law_id = law_id,
        .bounded_fanout = join_cfg.max_fanout != null,
    }).safe()) return null;
    const measure = switch (metric.op) {
        .count => null,
        .sum, .sumsquares, .min, .max, .avg => blk: {
            const field = index.fieldConfig(metric.field, .measure) orelse return null;
            break :blk field.name;
        },
    };
    return .{
        .join = join_ref,
        .op = metric.op,
        .group_by = group_by,
        .histogram_field = histogram_field,
        .histogram_role = histogram_role,
        .histogram_interval = histogram_interval,
        .time_field = time_field,
        .time_bucket = time_bucket,
        .measure = measure,
        .constraints = constraints,
    };
}

const DerivedHistogramField = struct {
    name: []const u8,
    role: fact_mod.Role,
};

fn derivedJoinHistogramField(index: *const index_mod.Index, field_name: []const u8) ?DerivedHistogramField {
    var found: ?DerivedHistogramField = null;
    if (index.fieldConfig(field_name, .measure)) |field| {
        const kind = value_mod.kindFromFieldType(field.type);
        if (kind == .number or kind == .integer) found = .{ .name = field.name, .role = .measure };
    }
    if (index.fieldConfig(field_name, .group)) |field| {
        const kind = value_mod.kindFromFieldType(field.type);
        if (kind == .number or kind == .integer) {
            if (found != null) return null;
            found = .{ .name = field.name, .role = .group };
        }
    }
    return found;
}

fn derivedJoinConstraintsSupported(index: *const index_mod.Index, constraints: []const ir.Constraint) bool {
    for (constraints) |constraint| {
        _ = index.fieldConfig(constraint.field, .group) orelse return false;
    }
    return true;
}

pub fn planMetricForGroupLayout(
    index: *const index_mod.Index,
    metric: ir.Metric,
    group_layout: []const []const u8,
    time_field: ?[]const u8,
    bucket: ?[]const u8,
    join_ref: ?ir.JoinRef,
) ?MetricPlan {
    if (!index.plannerLifecycleReady()) return null;
    if (!joinSupportedByConfig(index, join_ref)) return null;
    var exact: ?MetricPlan = null;
    var compatible: ?MetricPlan = null;
    var compatible_count: usize = 0;
    const expr = reduceExpr(metric.op, queryOutputDims(group_layout.len > 0, time_field != null));
    const join_expr = joinExpr(metric.op, queryOutputDims(group_layout.len > 0, time_field != null));
    if (planTensorExpr(index, exprWithOwner(expr, metric.name))) |typed_plan| {
        const mat = typed_plan.materialization;
        if (materializationMatchesMetric(index, mat, metric.op, metric.field, time_field, bucket) and
            sameGroupLayout(mat.group_by, group_layout) and
            materializationJoinMatchesNamed(index, mat, join_ref, metric.name) and
            (mat.join == null or materializationCanSatisfy(mat, join_expr).safe()))
        {
            return .{ .materialization = mat, .op = metric.op };
        }
    }
    for (index.config().materializations) |mat| {
        if (!materializationMatchesMetric(index, mat, metric.op, metric.field, time_field, bucket)) continue;
        if (!sameGroupLayout(mat.group_by, group_layout)) continue;
        if (!materializationJoinMatchesNamed(index, mat, join_ref, metric.name)) continue;
        if (!materializationCanSatisfy(mat, expr).safe()) continue;
        if (mat.join != null and !materializationCanSatisfy(mat, join_expr).safe()) continue;
        const plan: MetricPlan = .{ .materialization = mat, .op = metric.op };
        if (std.mem.eql(u8, mat.name, metric.name)) exact = plan;
        compatible = plan;
        compatible_count += 1;
    }
    if (exact) |plan| return plan;
    if (compatible_count == 1) return compatible;
    return null;
}

fn materializationMatches(
    index: *const index_mod.Index,
    mat: index_mod.MaterializationConfig,
    op: algebra.Op,
    query_measure_field: []const u8,
    query_group_fields: []const []const u8,
    query_time_field: ?[]const u8,
    query_bucket: ?[]const u8,
) bool {
    const mat_op = algebra.Op.parse(mat.op) orelse return false;
    if (mat_op != op) return false;
    if (!measureMatches(index, mat, op, query_measure_field)) return false;
    if (!groupFieldsMatch(index, mat.group_by, query_group_fields)) return false;
    if (!timeMatches(index, mat.time, query_time_field)) return false;
    if (!optionalStringMatches(mat.bucket, query_bucket)) return false;
    return true;
}

fn materializationMatchesMetricQuery(index: *const index_mod.Index, mat: index_mod.MaterializationConfig, query: ir.Query, metric: ir.Metric) bool {
    if (!materializationMatchesMetric(index, mat, metric.op, metric.field, query.time_field, query.time_bucket)) return false;
    if (!materializationGroupCoversConstraints(index, mat, query.constraints, null, &.{})) return false;
    if (!materializationJoinMatchesNamed(index, mat, query.join, metric.name)) return false;
    return true;
}

fn materializationMatchesBucketCountQuery(index: *const index_mod.Index, mat: index_mod.MaterializationConfig, query: ir.Query) bool {
    if (!materializationMatchesMetric(index, mat, .count, "", query.time_field, query.time_bucket)) return false;
    if (!materializationGroupCoversConstraints(index, mat, query.constraints, query.bucket_field, query.bucket_fields)) return false;
    if (!materializationJoinMatchesNamed(index, mat, query.join, query.aggregation_name)) return false;
    return true;
}

fn materializationMatchesMetric(
    index: *const index_mod.Index,
    mat: index_mod.MaterializationConfig,
    op: algebra.Op,
    query_measure_field: []const u8,
    query_time_field: ?[]const u8,
    query_bucket: ?[]const u8,
) bool {
    const mat_op = algebra.Op.parse(mat.op) orelse return false;
    if (mat_op != op) return false;
    if (!measureMatches(index, mat, op, query_measure_field)) return false;
    if (!timeMatches(index, mat.time, query_time_field)) return false;
    if (!optionalStringMatches(mat.bucket, query_bucket)) return false;
    return true;
}

fn materializationGroupCoversConstraints(
    index: *const index_mod.Index,
    mat: index_mod.MaterializationConfig,
    constraints: []const ir.Constraint,
    bucket_group_field: ?[]const u8,
    bucket_group_fields: []const []const u8,
) bool {
    const bucket_count = if (bucket_group_fields.len > 0) bucket_group_fields.len else @intFromBool(bucket_group_field != null);
    if (mat.group_by.len < constraints.len + bucket_count) return false;
    for (constraints) |constraint| {
        const field = index.fieldConfig(constraint.field, .group) orelse return false;
        if (fieldPosition(mat.group_by, field.name) == null) return false;
    }
    if (bucket_group_fields.len > 0) {
        for (bucket_group_fields) |bucket_field_name| {
            const field = index.fieldConfig(bucket_field_name, .group) orelse return false;
            if (fieldPosition(mat.group_by, field.name) == null) return false;
        }
    } else if (bucket_group_field) |bucket_field| {
        if (fieldPosition(mat.group_by, bucket_field) == null) return false;
    }
    return true;
}

fn materializationJoinMatchesNamed(index: *const index_mod.Index, mat: index_mod.MaterializationConfig, join_ref: ?ir.JoinRef, exact_name: []const u8) bool {
    if (join_ref) |join_value| {
        const mat_join = mat.join orelse return false;
        if (!std.mem.eql(u8, mat_join, join_value.name)) return false;
        const join_cfg = joinConfigByName(index, join_value.name) orelse return false;
        return join_mod.explicitQueryMaterializationProof(join_cfg, mat, join_value).safe();
    }
    if (mat.join == null) return true;
    if (std.mem.eql(u8, mat.name, exact_name)) return join_mod.namedQueryMaterializationProof(mat).safe();
    return mat.implicit_query and implicitJoinMaterializationSafe(index, mat);
}

fn implicitJoinMaterializationSafe(index: *const index_mod.Index, mat: index_mod.MaterializationConfig) bool {
    const join_name = mat.join orelse return false;
    const join_cfg = joinConfigByName(index, join_name) orelse return false;
    if (!join_mod.implicitQueryMaterializationProof(join_cfg, mat).safe()) return false;
    _ = algebra.Op.parse(mat.op) orelse return false;
    return true;
}

fn joinSupportedByConfig(index: *const index_mod.Index, join_ref: ?ir.JoinRef) bool {
    const join_value = join_ref orelse return true;
    const join_cfg = joinConfigByName(index, join_value.name) orelse return false;
    return join_mod.queryRewriteProof(join_cfg, join_value, .{ .kind = .predeclared_materialization }).safe();
}

fn joinConfigByName(index: *const index_mod.Index, name: []const u8) ?index_mod.JoinConfig {
    for (index.config().joins) |join_cfg| {
        if (std.mem.eql(u8, join_cfg.name, name)) return join_cfg;
    }
    return null;
}

fn joinKindForMaterialization(index: *const index_mod.Index, mat: index_mod.MaterializationConfig) ?JoinPlanKind {
    const join_name = mat.join orelse return null;
    return joinKindByName(index, join_name);
}

fn joinKindForJoinRef(index: *const index_mod.Index, join_ref: ir.JoinRef) ?JoinPlanKind {
    return joinKindByName(index, join_ref.name);
}

fn joinKindByName(index: *const index_mod.Index, join_name: []const u8) ?JoinPlanKind {
    for (index.config().joins) |join_cfg| {
        if (!std.mem.eql(u8, join_cfg.name, join_name)) continue;
        return switch (join_mod.temporalMode(join_cfg.temporal_bucket, join_cfg.temporal_window_seconds)) {
            .none => .equi,
            .bucket => .temporal_bucket,
            .window => .temporal_window,
            .bucket_window => .temporal_bucket_window,
        };
    }
    return null;
}

fn measureMatches(index: *const index_mod.Index, mat: index_mod.MaterializationConfig, op: algebra.Op, query_measure_field: []const u8) bool {
    if (op == .count) return mat.measure == null;
    const measure = mat.measure orelse return false;
    const field = index.fieldConfig(query_measure_field, .measure) orelse return false;
    return std.mem.eql(u8, measure, field.name);
}

fn groupFieldsMatch(index: *const index_mod.Index, mat_fields: []const []const u8, query_fields: []const []const u8) bool {
    if (mat_fields.len != query_fields.len) return false;
    for (mat_fields, query_fields) |mat_field, query_field| {
        const field = index.fieldConfig(query_field, .group) orelse return false;
        if (!std.mem.eql(u8, mat_field, field.name)) return false;
    }
    return true;
}

fn sameGroupLayout(lhs: []const []const u8, rhs: []const []const u8) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn fieldPosition(fields: []const []const u8, field_name: []const u8) ?usize {
    for (fields, 0..) |field, i| {
        if (std.mem.eql(u8, field, field_name)) return i;
    }
    return null;
}

fn timeMatches(index: *const index_mod.Index, mat_time: ?[]const u8, query_time: ?[]const u8) bool {
    if (query_time) |query| {
        const mat = mat_time orelse return false;
        const field = index.fieldConfig(query, .time) orelse return false;
        return std.mem.eql(u8, mat, field.name);
    }
    return mat_time == null;
}

fn optionalStringMatches(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs) |left| {
        const right = rhs orelse return false;
        return std.mem.eql(u8, left, right);
    }
    return rhs == null;
}

pub fn unsupported() UnsupportedAggregationPlan!void {
    return error.UnsupportedAggregationPlan;
}

test "planner lowers materialized metric query to tensor program" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"total_amount","op":"sum","measure":"amount"}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_program", cfg);
    defer index.close();

    var program_plan = (try planMetricTensorProgramAlloc(alloc, &index, .{
        .kind = .metric,
        .aggregation_name = "total_amount",
        .metric = .{ .name = "total_amount", .op = .sum, .field = "amount" },
    })).?;
    defer program_plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(@as(usize, 1), program_plan.steps.len);
    try std.testing.expectEqualStrings("total_amount", program_plan.access_paths[0].owner);
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_tensor, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(law_mod.Id.sum, program_plan.steps[0].expr.law_id.?);
    const program = program_plan.asProgram();
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.access_paths, program)).safe());
    const expected_id = try ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(expected_id);
    try std.testing.expectEqualStrings(expected_id, program_plan.program_id);
}

test "planner lowers materialized bucket count query to tensor program" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"segment","path":"segment","type":"string"}],
        \\  "materializations": [
        \\    {"name":"by_segment","op":"count","group_by":["segment"]}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_bucket_program", cfg);
    defer index.close();

    var program_plan = (try planBucketCountTensorProgramAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "by_segment",
        .bucket_field = "segment",
    })).?;
    defer program_plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(@as(usize, 1), program_plan.steps.len);
    try std.testing.expectEqualStrings("by_segment", program_plan.access_paths[0].owner);
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_tensor, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(law_mod.Id.count, program_plan.steps[0].expr.law_id.?);
    try std.testing.expectEqualSlices(ir.Dimension, &.{ .bucket, .scalar }, program_plan.steps[0].expr.output_dims);
    const program = program_plan.asProgram();
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.access_paths, program)).safe());
    const expected_id = try ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(expected_id);
    try std.testing.expectEqualStrings(expected_id, program_plan.program_id);
}

test "planner lowers materialized bucket query with child metrics to tensor programs" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"segment","path":"segment","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"by_segment","op":"count","group_by":["segment"]},
        \\    {"name":"amount_by_segment","op":"sum","group_by":["segment"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_bucket_child_program", cfg);
    defer index.close();

    const child_metrics = [_]ir.Metric{.{ .name = "amount_by_segment", .op = .sum, .field = "amount" }};
    var program_plan = (try planBucketQueryTensorProgramsAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "by_segment",
        .bucket_field = "segment",
        .child_metrics = child_metrics[0..],
    })).?;
    defer program_plan.deinit(alloc);

    try std.testing.expectEqualStrings("by_segment", program_plan.count.access_paths[0].owner);
    try std.testing.expectEqual(law_mod.Id.count, program_plan.count.steps[0].expr.law_id.?);
    try std.testing.expectEqual(@as(usize, 1), program_plan.child_metrics.len);
    try std.testing.expectEqualStrings("amount_by_segment", program_plan.child_metrics[0].access_paths[0].owner);
    try std.testing.expectEqual(law_mod.Id.sum, program_plan.child_metrics[0].steps[0].expr.law_id.?);
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.count.access_paths, program_plan.count.asProgram())).safe());
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.child_metrics[0].access_paths, program_plan.child_metrics[0].asProgram())).safe());

    var multi_output = (try planBucketQueryMultiOutputTensorProgramAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "by_segment",
        .bucket_field = "segment",
        .child_metrics = child_metrics[0..],
    })).?;
    defer multi_output.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), multi_output.access_paths.len);
    try std.testing.expectEqual(@as(usize, 2), multi_output.steps.len);
    try std.testing.expectEqual(@as(usize, 2), multi_output.outputs.len);
    try std.testing.expectEqual(@as(?usize, 0), multi_output.output.step);
    try std.testing.expectEqual(@as(?usize, 0), multi_output.outputs[0].step);
    try std.testing.expectEqual(@as(?usize, 1), multi_output.outputs[1].step);
    try std.testing.expectEqualStrings("by_segment", multi_output.access_paths[0].owner);
    try std.testing.expectEqualStrings("amount_by_segment", multi_output.access_paths[1].owner);
    try std.testing.expect((try ir.tensorProgramProof(alloc, multi_output.access_paths, multi_output.asProgram())).safe());
    const multi_output_id = try ir.tensorProgramIdAlloc(alloc, multi_output.asProgram());
    defer alloc.free(multi_output_id);
    try std.testing.expectEqualStrings(multi_output_id, multi_output.program_id);
}

test "planner matches configured join materialization through algebraic IR" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"integer"},
        \\    {"name":"segment","path":"segment","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer"}
        \\  ],
        \\  "materializations": [
        \\    {"name":"amount_by_segment","op":"sum","join":"orders_customers","group_by":["segment"],"measure":"amount","group_side":"right","measure_side":"left"}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg", cfg);
    defer index.close();

    const query = ir.Query{
        .kind = .metric,
        .aggregation_name = "amount_by_segment",
        .metric = .{ .name = "amount_by_segment", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    const result = planMetricQuery(&index, query);

    try std.testing.expectEqual(PlanKind.rollup_materialized, result.kind);
    try std.testing.expectEqual(JoinPlanKind.equi, result.join_kind.?);
    try std.testing.expectEqualStrings("amount_by_segment", result.metric.?.materialization.name);

    var program_plan = (try planMetricTensorProgramAlloc(alloc, &index, query)).?;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(ir.TensorFragment.join, program_plan.steps[0].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.sum, program_plan.steps[0].expr.law_id.?);
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.access_paths, program_plan.asProgram())).safe());

    const missing_sides = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "amount_by_segment",
        .metric = .{ .name = "amount_by_segment", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers" },
    });
    try std.testing.expectEqual(PlanKind.unsupported, missing_sides.kind);
    try std.testing.expectEqual(FallbackReason.unsupported_join, missing_sides.fallback_reason.?);

    const invalid_side = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "amount_by_segment",
        .metric = .{ .name = "amount_by_segment", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "middle", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.unsupported, invalid_side.kind);
    try std.testing.expectEqual(FallbackReason.unsupported_join, invalid_side.fallback_reason.?);

    const wrong_sides = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "amount_by_segment",
        .metric = .{ .name = "amount_by_segment", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "left", .measure_side = "right" },
    });
    try std.testing.expectEqual(PlanKind.unsupported, wrong_sides.kind);
    try std.testing.expectEqual(FallbackReason.no_materialization, wrong_sides.fallback_reason.?);
}

test "planner only matches implicit join materializations when configured safe" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"integer"},
        \\    {"name":"segment","path":"segment","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer"}
        \\  ],
        \\  "materializations": [
        \\    {"name":"count_by_segment_join","op":"count","join":"orders_customers","group_by":["segment"],"group_side":"right","measure_side":"left"},
        \\    {"name":"amount_by_segment_join","op":"sum","join":"orders_customers","group_by":["segment"],"measure":"amount","group_side":"right","measure_side":"left","implicit_query":true}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg", cfg);
    defer index.close();

    const generic = planBucketCountQuery(&index, .{
        .kind = .terms,
        .aggregation_name = "count_by_segment",
        .bucket_field = "segment",
    });
    try std.testing.expectEqual(PlanKind.unsupported, generic.kind);
    try std.testing.expectEqual(FallbackReason.no_materialization, generic.fallback_reason.?);

    const exact = planBucketCountQuery(&index, .{
        .kind = .terms,
        .aggregation_name = "count_by_segment_join",
        .bucket_field = "segment",
    });
    try std.testing.expectEqual(PlanKind.exact_materialized, exact.kind);
    try std.testing.expectEqualStrings("count_by_segment_join", exact.count_metric.?.materialization.name);

    const implicit = planMetricForGroupLayout(&index, .{
        .name = "amount",
        .op = .sum,
        .field = "amount",
    }, &.{"segment"}, null, null, null);
    try std.testing.expect(implicit != null);
    try std.testing.expectEqualStrings("amount_by_segment_join", implicit.?.materialization.name);
}

test "planner gates temporal join materializations by declared query mode" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"integer"},
        \\    {"name":"segment","path":"segment","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [
        \\    {"name":"order_time","path":"order_time","type":"datetime"},
        \\    {"name":"profile_time","path":"profile_time","type":"datetime"}
        \\  ],
        \\  "joins": [
        \\    {"name":"orders_profiles","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"profile","left_time_field":"order_time","right_time_field":"profile_time","temporal_bucket":"hour"}
        \\  ],
        \\  "materializations": [
        \\    {"name":"amount_by_segment_temporal","op":"sum","join":"orders_profiles","group_by":["segment"],"measure":"amount","group_side":"right","measure_side":"left"}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_temporal_join", cfg);
    defer index.close();

    const accepted = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "amount_by_segment_temporal",
        .metric = .{ .name = "amount_by_segment_temporal", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_profiles", .kind = .bucket, .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.rollup_materialized, accepted.kind);
    try std.testing.expectEqual(JoinPlanKind.temporal_bucket, accepted.join_kind.?);

    const wrong_mode = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "amount_by_segment_temporal",
        .metric = .{ .name = "amount_by_segment_temporal", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_profiles", .kind = .none, .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.unsupported, wrong_mode.kind);
    try std.testing.expectEqual(FallbackReason.unsupported_join, wrong_mode.fallback_reason.?);

    const wrong_side = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "amount_by_segment_temporal",
        .metric = .{ .name = "amount_by_segment_temporal", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_profiles", .kind = .bucket, .group_side = "left", .measure_side = "right" },
    });
    try std.testing.expectEqual(PlanKind.unsupported, wrong_side.kind);
    try std.testing.expectEqual(FallbackReason.no_materialization, wrong_side.fallback_reason.?);
}

test "planner returns derived join fold plan for bounded distributive metric query" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_derived_join_plan", cfg);
    defer index.close();

    const result = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "sum_orders_by_customer_region",
        .metric = .{ .name = "sum_orders_by_customer_region", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.derived_join_fold, result.kind);
    try std.testing.expectEqual(JoinPlanKind.equi, result.join_kind.?);
    try std.testing.expectEqual(algebra.Op.sum, result.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("amount", result.derived_join_fold.?.measure.?);

    var program_plan = (try planMetricTensorProgramAlloc(alloc, &index, .{
        .kind = .metric,
        .aggregation_name = "sum_orders_by_customer_region",
        .metric = .{ .name = "sum_orders_by_customer_region", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    })).?;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqualStrings("orders_customers", program_plan.access_paths[0].owner);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(ir.TensorFragment.slice, program_plan.steps[0].expr.fragment);
    try std.testing.expectEqual(ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.sum, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualSlices(ir.Dimension, &.{.scalar}, program_plan.steps[1].expr.output_dims);
    const expected_metadata = try index_mod.derivedJoinFoldMetadataAlloc(alloc, result.derived_join_fold.?);
    defer alloc.free(expected_metadata);
    try std.testing.expectEqualStrings(expected_metadata, program_plan.steps[1].expr.metadata.?);
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.access_paths, program_plan.asProgram())).safe());

    const avg = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "avg_orders_by_customer_region",
        .metric = .{ .name = "avg_orders_by_customer_region", .op = .avg, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.derived_join_fold, avg.kind);
    try std.testing.expectEqual(algebra.Op.avg, avg.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("amount", avg.derived_join_fold.?.measure.?);

    const min = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "min_orders_by_customer_region",
        .metric = .{ .name = "min_orders_by_customer_region", .op = .min, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.derived_join_fold, min.kind);
    try std.testing.expectEqual(algebra.Op.min, min.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("amount", min.derived_join_fold.?.measure.?);

    const max = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "max_orders_by_customer_region",
        .metric = .{ .name = "max_orders_by_customer_region", .op = .max, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.derived_join_fold, max.kind);
    try std.testing.expectEqual(algebra.Op.max, max.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("amount", max.derived_join_fold.?.measure.?);

    const constraints = [_]ir.Constraint{.{ .field = "region", .value = "west" }};
    const constrained = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "sum_orders_by_customer_region",
        .constraints = constraints[0..],
        .metric = .{ .name = "sum_orders_by_customer_region", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.derived_join_fold, constrained.kind);
    try std.testing.expectEqual(@as(usize, 1), constrained.derived_join_fold.?.constraints.len);
    try std.testing.expectEqualStrings("region", constrained.derived_join_fold.?.constraints[0].field);

    const unsupported_constraints = [_]ir.Constraint{.{ .field = "missing", .value = "west" }};
    const unsupported_constraint_plan = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "sum_orders_by_customer_region",
        .constraints = unsupported_constraints[0..],
        .metric = .{ .name = "sum_orders_by_customer_region", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(PlanKind.unsupported, unsupported_constraint_plan.kind);
}

test "planner returns derived join fold plan for bounded terms query" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_derived_terms_plan", cfg);
    defer index.close();

    var result = try planBucketQueryAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer result.deinit(alloc);
    try std.testing.expectEqual(PlanKind.derived_join_fold, result.kind);
    try std.testing.expectEqual(JoinPlanKind.equi, result.join_kind.?);
    try std.testing.expectEqual(algebra.Op.count, result.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("region", result.derived_join_fold.?.group_by[0]);

    var with_child = try planBucketQueryAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .child_metrics = &.{.{ .name = "amount", .op = .sum, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer with_child.deinit(alloc);
    try std.testing.expectEqual(PlanKind.derived_join_fold, with_child.kind);
    try std.testing.expectEqual(@as(usize, 1), with_child.derived_child_join_folds.len);
    try std.testing.expectEqual(algebra.Op.sum, with_child.derived_child_join_folds[0].op);
    try std.testing.expectEqualStrings("amount", with_child.derived_child_join_folds[0].measure.?);

    var program_plan = (try planBucketQueryTensorProgramsAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .child_metrics = &.{.{ .name = "amount", .op = .sum, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    })).?;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(ir.PhysicalLayout.join_fact_rows, program_plan.count.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.join, program_plan.count.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, program_plan.count.steps[1].expr.law_id.?);
    try std.testing.expectEqualSlices(ir.Dimension, &.{ .bucket, .scalar }, program_plan.count.steps[1].expr.output_dims);
    try std.testing.expectEqual(@as(usize, 1), program_plan.child_metrics.len);
    try std.testing.expectEqual(ir.PhysicalLayout.join_fact_rows, program_plan.child_metrics[0].access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.join, program_plan.child_metrics[0].steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.sum, program_plan.child_metrics[0].steps[1].expr.law_id.?);
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.count.access_paths, program_plan.count.asProgram())).safe());
    try std.testing.expect((try ir.tensorProgramProof(alloc, program_plan.child_metrics[0].access_paths, program_plan.child_metrics[0].asProgram())).safe());

    var multi_output_program = (try planBucketQueryMultiOutputTensorProgramAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .child_metrics = &.{.{ .name = "amount", .op = .sum, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    })).?;
    defer multi_output_program.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), multi_output_program.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.join_fact_rows, multi_output_program.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), multi_output_program.steps.len);
    try std.testing.expectEqual(@as(usize, 2), multi_output_program.outputs.len);
    try std.testing.expectEqual(ir.TensorFragment.slice, multi_output_program.steps[0].expr.fragment);
    try std.testing.expectEqual(ir.TensorFragment.join, multi_output_program.steps[1].expr.fragment);
    try std.testing.expectEqual(ir.TensorFragment.join, multi_output_program.steps[2].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, multi_output_program.steps[1].expr.law_id.?);
    try std.testing.expectEqual(law_mod.Id.sum, multi_output_program.steps[2].expr.law_id.?);
    try std.testing.expectEqual(@as(?usize, 1), multi_output_program.output.step);
    try std.testing.expectEqual(@as(?usize, 1), multi_output_program.outputs[0].step);
    try std.testing.expectEqual(@as(?usize, 2), multi_output_program.outputs[1].step);
    try std.testing.expect((try ir.tensorProgramProof(alloc, multi_output_program.access_paths, multi_output_program.asProgram())).safe());

    var with_avg_child = try planBucketQueryAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .child_metrics = &.{.{ .name = "amount", .op = .avg, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer with_avg_child.deinit(alloc);
    try std.testing.expectEqual(PlanKind.derived_join_fold, with_avg_child.kind);
    try std.testing.expectEqual(@as(usize, 1), with_avg_child.derived_child_join_folds.len);
    try std.testing.expectEqual(algebra.Op.avg, with_avg_child.derived_child_join_folds[0].op);
    try std.testing.expectEqualStrings("amount", with_avg_child.derived_child_join_folds[0].measure.?);

    const constraints = [_]ir.Constraint{.{ .field = "customer", .value = "c1" }};
    var constrained = try planBucketQueryAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .constraints = constraints[0..],
        .child_metrics = &.{.{ .name = "amount", .op = .sum, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer constrained.deinit(alloc);
    try std.testing.expectEqual(PlanKind.derived_join_fold, constrained.kind);
    try std.testing.expectEqual(@as(usize, 1), constrained.derived_join_fold.?.constraints.len);
    try std.testing.expectEqual(@as(usize, 1), constrained.derived_child_join_folds[0].constraints.len);
}

test "planner returns derived join fold plan for bounded date histogram query" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_derived_date_plan", cfg);
    defer index.close();

    var result = try planBucketQueryAlloc(alloc, &index, .{
        .kind = .date_histogram,
        .aggregation_name = "orders_by_day",
        .time_field = "created_at",
        .time_bucket = "day",
        .child_metrics = &.{.{ .name = "amount", .op = .sum, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer result.deinit(alloc);
    try std.testing.expectEqual(PlanKind.derived_join_fold, result.kind);
    try std.testing.expectEqual(algebra.Op.count, result.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("created_at", result.derived_join_fold.?.time_field.?);
    try std.testing.expectEqualStrings("day", result.derived_join_fold.?.time_bucket.?);
    try std.testing.expectEqual(@as(usize, 1), result.derived_child_join_folds.len);
    try std.testing.expectEqual(algebra.Op.sum, result.derived_child_join_folds[0].op);
}

test "planner returns derived join fold plan for bounded histogram query" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_derived_histogram_plan", cfg);
    defer index.close();

    var result = try planBucketQueryAlloc(alloc, &index, .{
        .kind = .histogram,
        .aggregation_name = "amount_histogram",
        .bucket_field = "amount",
        .bucket_interval = 20,
        .child_metrics = &.{.{ .name = "sum_amount", .op = .sum, .field = "amount" }},
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer result.deinit(alloc);
    try std.testing.expectEqual(PlanKind.derived_join_fold, result.kind);
    try std.testing.expectEqual(algebra.Op.count, result.derived_join_fold.?.op);
    try std.testing.expectEqualStrings("amount", result.derived_join_fold.?.histogram_field.?);
    try std.testing.expectEqual(fact_mod.Role.measure, result.derived_join_fold.?.histogram_role.?);
    try std.testing.expectEqual(@as(f64, 20), result.derived_join_fold.?.histogram_interval);
    try std.testing.expectEqual(@as(usize, 1), result.derived_child_join_folds.len);
    try std.testing.expectEqual(algebra.Op.sum, result.derived_child_join_folds[0].op);
    try std.testing.expectEqualStrings("amount", result.derived_child_join_folds[0].histogram_field.?);

    const ambiguous_cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"amount","path":"amount_bucket","type":"number"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var ambiguous = try index_mod.Index.open(alloc, "alg_derived_histogram_ambiguous", ambiguous_cfg);
    defer ambiguous.close();
    var rejected = try planBucketQueryAlloc(alloc, &ambiguous, .{
        .kind = .histogram,
        .aggregation_name = "amount_histogram",
        .bucket_field = "amount",
        .bucket_interval = 20,
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer rejected.deinit(alloc);
    try std.testing.expectEqual(PlanKind.unsupported, rejected.kind);
}

test "planner builds pathfact bucket fold tensor program" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "materializations": []
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_pathfact_plan", cfg);
    defer index.close();

    const fold = index_mod.PathFactBucketFoldRequest{
        .kind = .terms,
        .op = .count,
        .bucket_path = "/tenant",
        .bucket_kind = .string,
    };
    var plan = (try planPathFactBucketFoldTensorProgramAlloc(alloc, &index, fold, "tenant_terms")) orelse return error.TestUnexpectedResult;
    defer plan.deinit(alloc);

    try std.testing.expect(plan.program_id.len > 0);
    try std.testing.expectEqual(@as(usize, 1), plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.pathfact_rows, plan.access_paths[0].layout);
    try std.testing.expectEqualStrings(index.name, plan.access_paths[0].owner);
    try std.testing.expectEqual(@as(usize, 2), plan.steps.len);
    try std.testing.expectEqual(ir.TensorFragment.slice, plan.steps[0].expr.fragment);
    try std.testing.expectEqual(ir.TensorFragment.reduce, plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("tenant_terms", plan.steps[1].expr.semantic_id.?);
    try std.testing.expect(plan.steps[1].expr.metadata != null);
    try std.testing.expectEqual(ir.TensorProgramRef{ .step = 1 }, plan.output);

    const proof = try ir.tensorProgramProof(alloc, plan.access_paths, plan.asProgram());
    try std.testing.expect(proof.safe());
}

test "planner builds cardinality partial tensor programs for docfact and pathfact" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"tenant","path":"tenant","type":"string"},{"name":"product","path":"product","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_cardinality_plan", cfg);
    defer index.close();

    const constraints = [_]ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    var doc_plan = (try planCardinalityPartialsTensorProgramAlloc(alloc, &index, "product_cardinality", "product", constraints[0..])) orelse return error.TestUnexpectedResult;
    defer doc_plan.deinit(alloc);

    try std.testing.expect(doc_plan.program_id.len > 0);
    try std.testing.expectEqual(@as(usize, 1), doc_plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.docfact_rows, doc_plan.access_paths[0].layout);
    try std.testing.expectEqualStrings(index.name, doc_plan.access_paths[0].owner);
    try std.testing.expectEqual(@as(usize, 2), doc_plan.steps.len);
    try std.testing.expectEqual(ir.TensorFragment.slice, doc_plan.steps[0].expr.fragment);
    try std.testing.expectEqual(ir.TensorFragment.reduce, doc_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, doc_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("product_cardinality", doc_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqual(@as(usize, 1), doc_plan.owned_metadata.len);
    var doc_request = try index_mod.decodeCardinalityPartialsMetadataAlloc(alloc, doc_plan.steps[1].expr.metadata.?);
    defer doc_request.deinit(alloc);
    try std.testing.expectEqualStrings("product_cardinality", doc_request.request.aggregation_name);
    try std.testing.expectEqualStrings("product", doc_request.request.field_or_path);
    try std.testing.expectEqual(@as(usize, 1), doc_request.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", doc_request.request.constraints[0].field);
    try std.testing.expect((try ir.tensorProgramProof(alloc, doc_plan.access_paths, doc_plan.asProgram())).safe());
    try std.testing.expect((try planCardinalityPartialsTensorProgramAlloc(alloc, &index, "missing_cardinality", "missing", constraints[0..])) == null);

    var path_plan = (try planCardinalityPartialsTensorProgramAlloc(alloc, &index, "tier_cardinality", "/meta/tier", constraints[0..])) orelse return error.TestUnexpectedResult;
    defer path_plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), path_plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.pathfact_rows, path_plan.access_paths[0].layout);
    try std.testing.expectEqualStrings(index.name, path_plan.access_paths[0].owner);
    try std.testing.expectEqual(ir.TensorFragment.reduce, path_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, path_plan.steps[1].expr.law_id.?);
    var path_request = try index_mod.decodeCardinalityPartialsMetadataAlloc(alloc, path_plan.steps[1].expr.metadata.?);
    defer path_request.deinit(alloc);
    try std.testing.expectEqualStrings("tier_cardinality", path_request.request.aggregation_name);
    try std.testing.expectEqualStrings("/meta/tier", path_request.request.field_or_path);
    try std.testing.expect((try ir.tensorProgramProof(alloc, path_plan.access_paths, path_plan.asProgram())).safe());

    const children = [_]index_mod.CardinalityChildRequest{.{ .name = "product_cardinality", .field = "product" }};
    var terms_plan = (try planTermsCardinalityPartialsTensorProgramAlloc(alloc, &index, "by_tier", "/meta/tier", children[0..], constraints[0..])) orelse return error.TestUnexpectedResult;
    defer terms_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), terms_plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.docfact_rows, terms_plan.access_paths[0].layout);
    try std.testing.expectEqual(ir.PhysicalLayout.pathfact_rows, terms_plan.access_paths[1].layout);
    try std.testing.expectEqual(ir.TensorFragment.reduce, terms_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, terms_plan.steps[1].expr.law_id.?);
    var terms_request = try index_mod.decodeTermsCardinalityPartialsMetadataAlloc(alloc, terms_plan.steps[1].expr.metadata.?);
    defer terms_request.deinit(alloc);
    try std.testing.expectEqualStrings("by_tier", terms_request.request.aggregation_name);
    try std.testing.expectEqualStrings("/meta/tier", terms_request.request.bucket_field_or_path);
    try std.testing.expectEqual(@as(usize, 1), terms_request.request.children.len);
    try std.testing.expectEqualStrings("product_cardinality", terms_request.request.children[0].name);
    try std.testing.expectEqualStrings("product", terms_request.request.children[0].field);
    try std.testing.expect((try ir.tensorProgramProof(alloc, terms_plan.access_paths, terms_plan.asProgram())).safe());

    const ranges = [_]index_mod.CardinalityRangeRequest{.{ .name = "low", .start = "0", .end = "20" }};
    var range_plan = (try planRangeCardinalityPartialsTensorProgramAlloc(alloc, &index, "amount_ranges", "amount", .numeric, ranges[0..], children[0..], constraints[0..])) orelse return error.TestUnexpectedResult;
    defer range_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.docfact_rows, range_plan.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.reduce, range_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, range_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_ranges", range_plan.steps[1].expr.semantic_id.?);
    var range_request = try index_mod.decodeRangeCardinalityPartialsMetadataAlloc(alloc, range_plan.steps[1].expr.metadata.?);
    defer range_request.deinit(alloc);
    try std.testing.expectEqualStrings("amount_ranges", range_request.request.aggregation_name);
    try std.testing.expectEqualStrings("amount", range_request.request.field_or_path);
    try std.testing.expectEqual(index_mod.CardinalityRangeKind.numeric, range_request.request.kind);
    try std.testing.expectEqual(@as(usize, 1), range_request.request.ranges.len);
    try std.testing.expectEqualStrings("low", range_request.request.ranges[0].name);
    try std.testing.expectEqualStrings("0", range_request.request.ranges[0].start.?);
    try std.testing.expectEqualStrings("20", range_request.request.ranges[0].end.?);
    try std.testing.expect((try ir.tensorProgramProof(alloc, range_plan.access_paths, range_plan.asProgram())).safe());

    var histogram_plan = (try planHistogramCardinalityPartialsTensorProgramAlloc(alloc, &index, "amount_histogram", "amount", .numeric, 10, "", children[0..], constraints[0..])) orelse return error.TestUnexpectedResult;
    defer histogram_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), histogram_plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.docfact_rows, histogram_plan.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.reduce, histogram_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.count, histogram_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", histogram_plan.steps[1].expr.semantic_id.?);
    var histogram_request = try index_mod.decodeHistogramCardinalityPartialsMetadataAlloc(alloc, histogram_plan.steps[1].expr.metadata.?);
    defer histogram_request.deinit(alloc);
    try std.testing.expectEqualStrings("amount_histogram", histogram_request.request.aggregation_name);
    try std.testing.expectEqualStrings("amount", histogram_request.request.field_or_path);
    try std.testing.expectEqual(index_mod.CardinalityHistogramKind.numeric, histogram_request.request.kind);
    try std.testing.expectEqual(@as(f64, 10), histogram_request.request.numeric_interval);
    try std.testing.expectEqual(@as(usize, 1), histogram_request.request.children.len);
    try std.testing.expect((try ir.tensorProgramProof(alloc, histogram_plan.access_paths, histogram_plan.asProgram())).safe());
}

test "planner builds vector search tensor programs with optional doc constraints" {
    const alloc = std.testing.allocator;

    var dense = (try planVectorSearchTensorProgramAlloc(alloc, "dense_v1", .dense_vector, false)) orelse return error.TestUnexpectedResult;
    defer dense.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), dense.inputs.len);
    try std.testing.expectEqual(@as(usize, 1), dense.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.dense_vector, dense.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.vector_search, dense.steps[0].expr.fragment);
    try std.testing.expect(ir.vectorSearchProgramMatchesTarget(dense.asProgram(), "dense_v1", .dense_vector, false));
    try std.testing.expect((try ir.tensorProgramProof(alloc, dense.access_paths, dense.asProgram())).safe());

    var sparse_constrained = (try planVectorSearchTensorProgramAlloc(alloc, "sparse_v1", .sparse_vector, true)) orelse return error.TestUnexpectedResult;
    defer sparse_constrained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), sparse_constrained.inputs.len);
    try std.testing.expectEqualStrings("native_doc_id_constraints", sparse_constrained.inputs[0].semantic_id.?);
    try std.testing.expectEqual(ir.PhysicalLayout.sparse_vector, sparse_constrained.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorProgramRef{ .input = 0 }, sparse_constrained.steps[0].inputs[0]);
    try std.testing.expect(ir.vectorSearchProgramMatchesTarget(sparse_constrained.asProgram(), "sparse_v1", .sparse_vector, true));
    try std.testing.expect((try ir.tensorProgramProof(alloc, sparse_constrained.access_paths, sparse_constrained.asProgram())).safe());

    try std.testing.expect((try planVectorSearchTensorProgramAlloc(alloc, "graph_v1", .graph_edges, false)) == null);
}

test "planner builds graph traversal tensor programs with optional target constraints" {
    const alloc = std.testing.allocator;

    var unconstrained = (try planGraphTraversalTensorProgramAlloc(alloc, "graph_v1", false)) orelse return error.TestUnexpectedResult;
    defer unconstrained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), unconstrained.inputs.len);
    try std.testing.expectEqual(@as(usize, 1), unconstrained.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.graph_edges, unconstrained.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.graph_traverse, unconstrained.steps[0].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.provenance_semiring, unconstrained.steps[0].expr.law_id.?);
    try std.testing.expect(ir.graphTraversalProgramMatchesTarget(unconstrained.asProgram(), "graph_v1", false));
    try std.testing.expect((try ir.tensorProgramProof(alloc, unconstrained.access_paths, unconstrained.asProgram())).safe());

    var constrained = (try planGraphTraversalTensorProgramAlloc(alloc, "graph_v1", true)) orelse return error.TestUnexpectedResult;
    defer constrained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), constrained.inputs.len);
    try std.testing.expectEqualStrings("graph_target_constraints", constrained.inputs[0].semantic_id.?);
    try std.testing.expectEqual(ir.TensorProgramRef{ .input = 0 }, constrained.steps[0].inputs[0]);
    try std.testing.expect(ir.graphTraversalProgramMatchesTarget(constrained.asProgram(), "graph_v1", true));
    try std.testing.expect((try ir.tensorProgramProof(alloc, constrained.access_paths, constrained.asProgram())).safe());
}

test "planner builds graph edge tensor programs" {
    const alloc = std.testing.allocator;

    var plan = (try planGraphEdgesTensorProgramAlloc(alloc, "graph_v1")) orelse return error.TestUnexpectedResult;
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), plan.inputs.len);
    try std.testing.expectEqual(@as(usize, 1), plan.access_paths.len);
    try std.testing.expectEqual(ir.PhysicalLayout.graph_edges, plan.access_paths[0].layout);
    try std.testing.expectEqual(ir.TensorFragment.graph_traverse, plan.steps[0].expr.fragment);
    try std.testing.expectEqual(law_mod.Id.provenance_semiring, plan.steps[0].expr.law_id.?);
    try std.testing.expect(ir.graphEdgesProgramMatchesTarget(plan.asProgram(), "graph_v1"));
    try std.testing.expect((try ir.tensorProgramProof(alloc, plan.access_paths, plan.asProgram())).safe());
}

test "planner advertises materializations as typed tensor access paths" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "materializations": [
        \\    {"name":"revenue_by_day_customer","op":"sum","group_by":["customer"],"measure":"amount","time":"created_at","bucket":"day"}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_tensor_access", cfg);
    defer index.close();

    const mat = index_mod.MaterializationConfig{
        .name = "revenue_by_day_customer",
        .op = "sum",
        .group_by = &.{"customer"},
        .measure = "amount",
        .time = "created_at",
        .bucket = "day",
    };

    const access_path = materializationAccessPath(mat).?;
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_tensor, access_path.layout);
    try std.testing.expectEqualStrings("revenue_by_day_customer", access_path.owner);
    try std.testing.expect(ir.accessPathCanSatisfy(access_path, .{
        .fragment = .reduce,
        .output_dims = &.{ .time, .bucket },
        .law_id = .sum,
    }).safe());
    try std.testing.expectEqual(
        ir.AccessPathRejectReason.law_required,
        materializationCanSatisfy(mat, .{
            .fragment = .reduce,
            .output_dims = &.{.bucket},
            .law_id = .max,
        }).rejected,
    );
    try std.testing.expectEqual(
        ir.AccessPathRejectReason.missing_fragment,
        materializationCanSatisfy(mat, .{
            .fragment = .automaton_select,
            .output_dims = &.{.doc},
        }).rejected,
    );

    const planned = planTensorExpr(&index, .{
        .fragment = .reduce,
        .output_dims = &.{ .time, .bucket },
        .law_id = .sum,
    }).?;
    try std.testing.expectEqualStrings("revenue_by_day_customer", planned.materialization.name);
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_tensor, planned.access_path.layout);
    const materializations = [_][]const u8{"revenue_by_day_customer"};
    var partial_plan = (try planMaterializationPartialsTensorProgramAlloc(alloc, &index, materializations[0..])) orelse return error.TestUnexpectedResult;
    defer partial_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), partial_plan.access_paths.len);
    try std.testing.expectEqual(@as(usize, 1), partial_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 1), partial_plan.outputs.len);
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_tensor, partial_plan.access_paths[0].layout);
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_tensor, partial_plan.steps[0].expr.layout.?);
    try std.testing.expectEqual(law_mod.Id.sum, partial_plan.steps[0].expr.law_id.?);
    try std.testing.expect((try ir.tensorProgramProof(alloc, partial_plan.access_paths, partial_plan.asProgram())).safe());
    const output_expr = materializationTensorExpression(index.config().materializations[0]) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(ir.PhysicalLayout.materialized_expr, output_expr.layout.?);
    try std.testing.expectEqual(law_mod.Id.sum, output_expr.law_id.?);
    try std.testing.expect(planTensorExpr(&index, .{
        .fragment = .reduce,
        .output_dims = &.{.bucket},
        .law_id = .max,
    }) == null);

    const join_mat = index_mod.MaterializationConfig{
        .name = "revenue_by_segment_join",
        .op = "sum",
        .group_by = &.{"segment"},
        .measure = "amount",
        .join = "orders_customers",
        .group_side = "right",
        .measure_side = "left",
    };
    const join_path = materializationAccessPath(join_mat).?;
    try std.testing.expect(ir.accessPathCanSatisfy(join_path, .{
        .fragment = .join,
        .output_dims = &.{.bucket},
        .law_id = .sum,
    }).safe());
    try std.testing.expectEqual(
        ir.AccessPathRejectReason.missing_fragment,
        materializationCanSatisfy(mat, .{
            .fragment = .join,
            .output_dims = &.{.bucket},
            .law_id = .sum,
        }).rejected,
    );
}

test "planner uses typed tensor owner selection for exact named materializations" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"sum_by_customer_a","op":"sum","group_by":["customer"],"measure":"amount"},
        \\    {"name":"sum_by_customer_b","op":"sum","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_tensor_owner", cfg);
    defer index.close();

    try std.testing.expect(planTensorExpr(&index, .{
        .fragment = .reduce,
        .output_dims = &.{.bucket},
        .law_id = .sum,
    }) == null);

    const typed = planTensorExpr(&index, .{
        .fragment = .reduce,
        .output_dims = &.{.bucket},
        .owner = "sum_by_customer_b",
        .law_id = .sum,
    }).?;
    try std.testing.expectEqualStrings("sum_by_customer_b", typed.materialization.name);

    const exact = planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "sum_by_customer_b",
        .metric = .{ .name = "sum_by_customer_b", .op = .sum, .field = "amount" },
    });
    try std.testing.expectEqual(PlanKind.rollup_materialized, exact.kind);
    try std.testing.expectEqualStrings("sum_by_customer_b", exact.metric.?.materialization.name);

    const group_layout_plan = planMetricForGroupLayout(
        &index,
        .{ .name = "sum_by_customer_b", .op = .sum, .field = "amount" },
        &.{"customer"},
        null,
        null,
        null,
    ).?;
    try std.testing.expectEqualStrings("sum_by_customer_b", group_layout_plan.materialization.name);
}

test "planner rejects rebuild-required schema lifecycle state" {
    const alloc = std.testing.allocator;
    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "capability_lifecycle_status": "rebuild_required",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "materializations": [
        \\    {"name":"count_by_customer","op":"count","group_by":["customer"]}
        \\  ]
        \\}
    ;
    var index = try index_mod.Index.open(alloc, "alg_stale", cfg);
    defer index.close();

    const result = planBucketCountQuery(&index, .{
        .kind = .terms,
        .aggregation_name = "count_by_customer",
        .bucket_field = "customer",
    });
    try std.testing.expectEqual(PlanKind.unsupported, result.kind);
    try std.testing.expectEqual(FallbackReason.schema_lifecycle_not_ready, result.fallback_reason.?);
}
