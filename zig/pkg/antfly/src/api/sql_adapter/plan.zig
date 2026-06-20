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

const ast = @import("ast.zig");
const db_mod = @import("../../storage/db/mod.zig");
const grammar = @import("grammar.zig");
const relational_rows = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");

pub const RelationLifetimeKind = grammar.RelationLifetimeKind;
pub const RelationPopulationMode = grammar.RelationPopulationMode;
pub const SelectOutputRef = ast.SelectOutputRef;
pub const SelectSetOperation = ast.SelectSetOperation;

pub const LoweredSelect = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    query: db_mod.types.RelationalRowsQueryRequest,
    select_outputs: []const SelectOutputRef = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        self.query.deinit(alloc);
        self.clearSelectOutputs(alloc);
        self.* = undefined;
    }

    pub fn clearSelectOutputs(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.select_outputs.len > 0) alloc.free(self.select_outputs);
        self.select_outputs = &.{};
    }
};

pub const LoweredQueryPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsQueryPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredSetOperationPlan = struct {
    operation: SelectSetOperation,
    left: LoweredQueryPlan,
    right: LoweredQueryPlan,
    output_columns: []const runtime_schema.RelationalColumn = &.{},
    order_by: []const db_mod.types.RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.left.deinit(alloc);
        self.right.deinit(alloc);
        freeSetOperationOutputColumns(alloc, self.output_columns);
        var order_query: db_mod.types.RelationalRowsQueryRequest = .{ .order_by = self.order_by };
        order_query.deinit(alloc);
        self.* = undefined;
    }
};

fn freeSetOperationOutputColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    for (columns) |column| {
        if (column.name.len > 0) alloc.free(column.name);
        if (column.path.len > 0) alloc.free(column.path);
    }
    if (columns.len > 0) alloc.free(columns);
}

pub const LoweredWindowPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsWindowPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredInsert = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    returning_expression_count: usize = 0,
    returning_all: bool = false,
    conflict_where: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredInsertSource = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    insert_source: relational_rows.OwnedRowsInsertSourceRequest,
    returning_expression_count: usize = 0,
    returning_all: bool = false,
    conflict_where: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        self.insert_source.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutation = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    returning_expression_count: usize = 0,
    returning_all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutationSource = struct {
    table_name: []const u8,
    mutation: relational_rows.OwnedRowsMutationSourceRequest,
    restart_identity: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredJoinedMutationSource = struct {
    target_table_name: []const u8,
    source_table_name: []const u8,
    mutation: relational_rows.OwnedRowsJoinedMutationSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_table_name);
        alloc.free(self.source_table_name);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LowerWritePlanOptions = struct {
    unique_resolver: ?relational_rows.UniqueSelectorResolver = null,
    row_claim: ?db_mod.types.RowClaimRequest = null,
    joined_source_schema: ?runtime_schema.TableSchema = null,
    insert_source_schema: ?runtime_schema.TableSchema = null,
};

pub const ReturningProjection = struct {
    fields: []const []const u8 = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionProjection = &.{},

    pub fn hasProjection(self: ReturningProjection) bool {
        return self.fields.len > 0 or self.expressions.len > 0;
    }

    pub fn returnsAll(self: ReturningProjection) bool {
        return self.fields.len == 1 and std.mem.eql(u8, self.fields[0], "*");
    }

    pub fn deinit(self: ReturningProjection, alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.fields);
        freeExpressionProjections(alloc, self.expressions);
    }
};

pub const MergeFieldMapping = struct {
    target_field: []const u8,
    source_field: []const u8,
};

pub const MergeExpressionAssignment = struct {
    target_field: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
};

pub const MergePredicateSide = enum {
    target,
    source,
};

pub const MergeArmPredicate = struct {
    side: MergePredicateSide,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value_json: ?[]const u8 = null,
};

pub const MergeMatchedArm = struct {
    predicates: []const MergeArmPredicate = &.{},
    expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    expression_or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    expression_not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    update: []const MergeFieldMapping = &.{},
    update_expressions: []const MergeExpressionAssignment = &.{},
    delete: bool = false,
    do_nothing: bool = false,
};

pub const MergeNotMatchedArm = struct {
    predicates: []const MergeArmPredicate = &.{},
    expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    expression_or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    expression_not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    insert: []const MergeFieldMapping = &.{},
    insert_expressions: []const MergeExpressionAssignment = &.{},
    do_nothing: bool = false,
};

pub const LoweredMergeMutationPlan = struct {
    target_table_name: []const u8,
    source_table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    source: db_mod.types.RelationalRowsQueryRequest = .{},
    match_fields: []const MergeFieldMapping = &.{},
    matched_arms: []const MergeMatchedArm = &.{},
    not_matched_arms: []const MergeNotMatchedArm = &.{},
    returning: ReturningProjection = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_table_name);
        alloc.free(self.source_table_name);
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        self.source.deinit(alloc);
        freeMergeFieldMappings(alloc, self.match_fields);
        freeMergeMatchedArms(alloc, self.matched_arms);
        freeMergeNotMatchedArms(alloc, self.not_matched_arms);
        self.returning.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredWritePlan = union(enum) {
    insert: LoweredInsert,
    insert_source: LoweredInsertSource,
    update: LoweredMutation,
    delete: LoweredMutation,
    update_source: LoweredMutationSource,
    delete_source: LoweredMutationSource,
    truncate_source: LoweredMutationSource,
    update_joined_source: LoweredJoinedMutationSource,
    delete_joined_source: LoweredJoinedMutationSource,
    merge_mutation: LoweredMergeMutationPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .insert => |*insert| insert.deinit(alloc),
            .insert_source => |*insert_source| insert_source.deinit(alloc),
            .update => |*update| update.deinit(alloc),
            .delete => |*delete| delete.deinit(alloc),
            .update_source => |*update_source| update_source.deinit(alloc),
            .delete_source => |*delete_source| delete_source.deinit(alloc),
            .truncate_source => |*truncate_source| truncate_source.deinit(alloc),
            .update_joined_source => |*update_joined_source| update_joined_source.deinit(alloc),
            .delete_joined_source => |*delete_joined_source| delete_joined_source.deinit(alloc),
            .merge_mutation => |*merge_mutation| merge_mutation.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const LoweredAggregate = struct {
    table_name: []const u8,
    aggregate: db_mod.types.RelationalRowsAggregateRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.aggregate.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredAggregatePlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsAggregatePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredJoin = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange = &.{},
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange = &.{},
    join: db_mod.types.RelationalRowsJoinRequest,

    pub fn asPlan(self: @This()) db_mod.types.RelationalRowsJoinPlan {
        return .{
            .ctes = self.ctes,
            .left_table = self.left_table_name,
            .right_table = self.right_table_name,
            .left_ranges = self.left_ranges,
            .right_ranges = self.right_ranges,
            .join = self.join,
        };
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        var owned = self.asPlan();
        owned.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredLateralPlan = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    plan: db_mod.types.RelationalRowsLateralPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.left_table_name);
        alloc.free(self.right_table_name);
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredReadPlan = union(enum) {
    query: LoweredQueryPlan,
    set_operation: LoweredSetOperationPlan,
    aggregate: LoweredAggregatePlan,
    join: LoweredJoin,
    lateral: LoweredLateralPlan,
    window: LoweredWindowPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query => |*query| query.deinit(alloc),
            .set_operation => |*set_operation| set_operation.deinit(alloc),
            .aggregate => |*aggregate| aggregate.deinit(alloc),
            .join => |*join| join.deinit(alloc),
            .lateral => |*lateral| lateral.deinit(alloc),
            .window => |*window| window.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const LoweredRelationPopulationPlan = struct {
    mode: RelationPopulationMode,
    target_table_name: []const u8,
    target_lifetime: ?RelationLifetimeKind = null,
    if_not_exists: bool = false,
    populate: bool = true,
    source: LoweredReadPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_table_name);
        self.source.deinit(alloc);
        self.* = undefined;
    }
};

pub const ExplainFormat = ast.SqlExplainFormat;

pub const LoweredExplainPlan = struct {
    analyze: bool = false,
    format: ExplainFormat = .text,
    verbose: bool = false,
    costs: bool = true,
    buffers: bool = false,
    timing: bool = true,
    summary: bool = true,
    settings: bool = false,
    wal: bool = false,
    subject: LoweredExplainSubject,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.subject.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredExplainSubject = union(enum) {
    read: LoweredReadPlan,
    write: LoweredWritePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .read => |*read| read.deinit(alloc),
            .write => |*write| write.deinit(alloc),
        }
        self.* = undefined;
    }
};

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

pub fn cloneExpressionAlloc(
    alloc: std.mem.Allocator,
    value: db_mod.types.RelationalRowsExpression,
) anyerror!db_mod.types.RelationalRowsExpression {
    var cloned: db_mod.types.RelationalRowsExpression = .{
        .kind = value.kind,
        .field_source = value.field_source,
        .cast_type = value.cast_type,
        .json_as_text = value.json_as_text,
    };
    errdefer freeExpression(alloc, cloned);

    if (value.field.len > 0) cloned.field = try alloc.dupe(u8, value.field);
    if (value.value_json.len > 0) cloned.value_json = try alloc.dupe(u8, value.value_json);
    if (value.json_path.len > 0) cloned.json_path = try alloc.dupe(u8, value.json_path);

    if (value.operands.len > 0) {
        const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, value.operands.len);
        var initialized: usize = 0;
        errdefer {
            for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
            alloc.free(operands);
        }
        for (value.operands, 0..) |operand, i| {
            operands[i] = try cloneExpressionAlloc(alloc, operand);
            initialized += 1;
        }
        cloned.operands = operands;
    }

    if (value.case_branches.len > 0) {
        const branches = try alloc.alloc(db_mod.types.RelationalRowsExpressionCaseBranch, value.case_branches.len);
        var initialized: usize = 0;
        errdefer {
            for (branches[0..initialized]) |branch| freeExpressionCaseBranch(alloc, branch);
            alloc.free(branches);
        }
        for (value.case_branches, 0..) |branch, i| {
            branches[i] = try cloneExpressionCaseBranchAlloc(alloc, branch);
            initialized += 1;
        }
        cloned.case_branches = branches;
    }

    if (value.case_else.len > 0) {
        const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, value.case_else.len);
        var initialized: usize = 0;
        errdefer {
            for (fallback[0..initialized]) |expression| freeExpression(alloc, expression);
            alloc.free(fallback);
        }
        for (value.case_else, 0..) |expression, i| {
            fallback[i] = try cloneExpressionAlloc(alloc, expression);
            initialized += 1;
        }
        cloned.case_else = fallback;
    }

    return cloned;
}

pub fn rewriteExpressionFieldsToSource(value: *db_mod.types.RelationalRowsExpression) void {
    if (value.field.len > 0) value.field_source = .source;
    for (@constCast(value.operands)) |*operand| rewriteExpressionFieldsToSource(operand);
    for (@constCast(value.case_branches)) |*branch| {
        rewriteExpressionConditionFieldsToSource(&branch.when);
        rewriteExpressionFieldsToSource(&branch.then);
    }
    for (@constCast(value.case_else)) |*fallback| rewriteExpressionFieldsToSource(fallback);
}

pub fn rewriteExpressionConditionFieldsToSource(value: *db_mod.types.RelationalRowsExpressionCondition) void {
    rewriteExpressionFieldsToSource(&value.lhs);
    for (@constCast(value.rhs)) |*rhs| rewriteExpressionFieldsToSource(rhs);
}

pub fn cloneSelectOutputsAlloc(
    alloc: std.mem.Allocator,
    values: []const SelectOutputRef,
) ![]const SelectOutputRef {
    if (values.len == 0) return &.{};
    return try alloc.dupe(SelectOutputRef, values);
}

pub fn cloneExpressionProjection(
    alloc: std.mem.Allocator,
    value: db_mod.types.RelationalRowsExpressionProjection,
) !db_mod.types.RelationalRowsExpressionProjection {
    const output = try alloc.dupe(u8, value.output);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    const expression = try cloneExpressionAlloc(alloc, value.expression);
    output_transferred = true;
    return .{
        .output = output,
        .expression = expression,
    };
}

pub fn cloneExpressionCaseBranchAlloc(
    alloc: std.mem.Allocator,
    value: db_mod.types.RelationalRowsExpressionCaseBranch,
) anyerror!db_mod.types.RelationalRowsExpressionCaseBranch {
    const when = try cloneExpressionConditionAlloc(alloc, value.when);
    var when_transferred = false;
    errdefer if (!when_transferred) freeExpressionCondition(alloc, when);
    const then_expression = try cloneExpressionAlloc(alloc, value.then);
    when_transferred = true;
    return .{
        .when = when,
        .then = then_expression,
    };
}

pub fn cloneExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    value: db_mod.types.RelationalRowsExpressionCondition,
) anyerror!db_mod.types.RelationalRowsExpressionCondition {
    const lhs = try cloneExpressionAlloc(alloc, value.lhs);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const rhs = if (value.rhs.len > 0) blk: {
        const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, value.rhs.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |expression| freeExpression(alloc, expression);
            alloc.free(out);
        }
        for (value.rhs, 0..) |expression, i| {
            out[i] = try cloneExpressionAlloc(alloc, expression);
            initialized += 1;
        }
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = value.op,
        .rhs = rhs,
    };
}

pub fn cloneExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    values: []const db_mod.types.RelationalRowsExpressionCondition,
) anyerror![]db_mod.types.RelationalRowsExpressionCondition {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |condition| freeExpressionCondition(alloc, condition);
        alloc.free(out);
    }
    for (values) |condition| {
        out[initialized] = try cloneExpressionConditionAlloc(alloc, condition);
        initialized += 1;
    }
    return out;
}

pub fn cloneExpressionPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    values: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) anyerror![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(out);
    }
    for (values) |group| {
        out[initialized] = .{ .conditions = try cloneExpressionConditionsAlloc(alloc, group.conditions) };
        initialized += 1;
    }
    return out;
}

pub fn cloneExpressionConditionsConcatAlloc(
    alloc: std.mem.Allocator,
    lhs: []const db_mod.types.RelationalRowsExpressionCondition,
    rhs: []const db_mod.types.RelationalRowsExpressionCondition,
) anyerror![]db_mod.types.RelationalRowsExpressionCondition {
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, lhs.len + rhs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |condition| freeExpressionCondition(alloc, condition);
        alloc.free(out);
    }
    for (lhs) |condition| {
        out[initialized] = try cloneExpressionConditionAlloc(alloc, condition);
        initialized += 1;
    }
    for (rhs) |condition| {
        out[initialized] = try cloneExpressionConditionAlloc(alloc, condition);
        initialized += 1;
    }
    return out;
}

pub fn cloneOrderByAlloc(
    alloc: std.mem.Allocator,
    values: []const db_mod.types.RelationalRowsQueryOrder,
) ![]const db_mod.types.RelationalRowsQueryOrder {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsQueryOrder, values.len);
    var initialized: usize = 0;
    errdefer {
        freeOrderBy(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = .{
            .direction = value.direction,
            .null_test = value.null_test,
        };
        if (value.field.len > 0) out[i].field = try alloc.dupe(u8, value.field);
        if (value.expression) |expression| out[i].expression = try cloneExpressionAlloc(alloc, expression);
        initialized += 1;
    }
    return out;
}

pub fn freeExpression(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpression) void {
    if (value.field.len > 0) alloc.free(value.field);
    if (value.value_json.len > 0) alloc.free(value.value_json);
    if (value.json_path.len > 0) alloc.free(value.json_path);
    for (value.operands) |operand| freeExpression(alloc, operand);
    if (value.operands.len > 0) alloc.free(value.operands);
    for (value.case_branches) |branch| freeExpressionCaseBranch(alloc, branch);
    if (value.case_branches.len > 0) alloc.free(value.case_branches);
    for (value.case_else) |fallback| freeExpression(alloc, fallback);
    if (value.case_else.len > 0) alloc.free(value.case_else);
}

pub fn freeExpressionSlice(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpression) void {
    for (values) |value| freeExpression(alloc, value);
    if (values.len > 0) alloc.free(values);
}

pub fn freeExpressionCaseBranch(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionCaseBranch) void {
    freeExpressionCondition(alloc, value.when);
    freeExpression(alloc, value.then);
}

pub fn freeExpressionCondition(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionCondition) void {
    freeExpression(alloc, value.lhs);
    for (value.rhs) |rhs| freeExpression(alloc, rhs);
    if (value.rhs.len > 0) alloc.free(value.rhs);
}

pub fn freeExpressionConditions(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (values) |value| freeExpressionCondition(alloc, value);
}

pub fn freeExpressionPredicateGroup(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionPredicateGroup) void {
    freeExpressionConditions(alloc, value.conditions);
    if (value.conditions.len > 0) alloc.free(value.conditions);
}

pub fn freeExpressionPredicateGroups(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionPredicateGroup) void {
    for (values) |group| freeExpressionPredicateGroup(alloc, group);
}

pub fn freeExpressionProjection(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionProjection) void {
    alloc.free(value.output);
    freeExpression(alloc, value.expression);
}

pub fn freeExpressionProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionProjection) void {
    for (values) |value| freeExpressionProjection(alloc, value);
    if (values.len > 0) alloc.free(values);
}

pub fn freeOrderBy(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsQueryOrder) void {
    for (values) |value| {
        if (value.field.len > 0) alloc.free(value.field);
        if (value.expression) |expression| freeExpression(alloc, expression);
    }
}

pub fn freeRelationalCheck(alloc: std.mem.Allocator, value: runtime_schema.RelationalCheck) void {
    std.debug.assert(value.name.len == 0);
    alloc.free(value.field);
    if (value.value_json) |json| alloc.free(json);
    if (value.expression) |expression| freeExpressionCondition(alloc, expression);
}

pub fn freeRelationalChecks(alloc: std.mem.Allocator, values: []const runtime_schema.RelationalCheck) void {
    for (values) |value| freeRelationalCheck(alloc, value);
}

pub fn freeArrayContains(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayContainsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

pub fn freeArrayAny(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayAnyPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

pub fn freeArrayEq(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayEqPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

pub fn freeExpressionArrayContainsOne(
    alloc: std.mem.Allocator,
    value: db_mod.types.RelationalRowsExpressionArrayContainsPredicate,
) void {
    freeExpression(alloc, value.expression);
    alloc.free(value.value_json);
}

pub fn freeExpressionArrayContains(
    alloc: std.mem.Allocator,
    values: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate,
) void {
    for (values) |value| freeExpressionArrayContainsOne(alloc, value);
}

pub fn freeInPredicates(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsInPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.values_json);
    }
}

pub fn freeTextPatterns(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsTextPatternPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.pattern);
    }
}

pub fn freeJsonContains(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonContainsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

pub fn freeJsonPathEq(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonPathEqPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.path);
        alloc.free(value.value_json);
    }
}

pub fn freeJsonPathExists(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonPathExistsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.path);
    }
}

pub fn freePredicateGroup(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsPredicateGroup) void {
    freeRelationalChecks(alloc, value.predicates);
    if (value.predicates.len > 0) alloc.free(value.predicates);
}

pub fn freePredicateGroups(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsPredicateGroup) void {
    for (values) |value| freePredicateGroup(alloc, value);
}

pub fn freeAccessPredicateGroup(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsAccessPredicateGroup) void {
    freeRelationalChecks(alloc, value.predicates);
    if (value.predicates.len > 0) alloc.free(value.predicates);
    freeArrayAny(alloc, value.array_any);
    if (value.array_any.len > 0) alloc.free(value.array_any);
    freeArrayContains(alloc, value.array_contains);
    if (value.array_contains.len > 0) alloc.free(value.array_contains);
    freeArrayEq(alloc, value.array_eq);
    if (value.array_eq.len > 0) alloc.free(value.array_eq);
    freeInPredicates(alloc, value.in_predicates);
    if (value.in_predicates.len > 0) alloc.free(value.in_predicates);
    freeJsonContains(alloc, value.json_contains);
    if (value.json_contains.len > 0) alloc.free(value.json_contains);
    freeJsonPathEq(alloc, value.json_path_eq);
    if (value.json_path_eq.len > 0) alloc.free(value.json_path_eq);
    freeJsonPathExists(alloc, value.json_path_exists);
    if (value.json_path_exists.len > 0) alloc.free(value.json_path_exists);
    freeTextPatterns(alloc, value.text_patterns);
    if (value.text_patterns.len > 0) alloc.free(value.text_patterns);
}

pub fn freeAccessPredicateGroups(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsAccessPredicateGroup) void {
    for (values) |value| freeAccessPredicateGroup(alloc, value);
}

pub fn freeWindowSpec(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsWindowSpec) void {
    alloc.free(value.output);
    freeStringSlice(alloc, value.partition_by);
    freeOrderBy(alloc, value.order_by);
    if (value.order_by.len > 0) alloc.free(value.order_by);
    if (value.value_expression) |expression| freeExpression(alloc, expression);
    if (value.default_json.len > 0) alloc.free(value.default_json);
    freeRelationalChecks(alloc, value.filter_predicates);
    if (value.filter_predicates.len > 0) alloc.free(value.filter_predicates);
    freeArrayAny(alloc, value.filter_array_any);
    if (value.filter_array_any.len > 0) alloc.free(value.filter_array_any);
    freeArrayContains(alloc, value.filter_array_contains);
    if (value.filter_array_contains.len > 0) alloc.free(value.filter_array_contains);
    freeArrayEq(alloc, value.filter_array_eq);
    if (value.filter_array_eq.len > 0) alloc.free(value.filter_array_eq);
    freeInPredicates(alloc, value.filter_in_predicates);
    if (value.filter_in_predicates.len > 0) alloc.free(value.filter_in_predicates);
    freeJsonContains(alloc, value.filter_json_contains);
    if (value.filter_json_contains.len > 0) alloc.free(value.filter_json_contains);
    freeJsonPathEq(alloc, value.filter_json_path_eq);
    if (value.filter_json_path_eq.len > 0) alloc.free(value.filter_json_path_eq);
    freeJsonPathExists(alloc, value.filter_json_path_exists);
    if (value.filter_json_path_exists.len > 0) alloc.free(value.filter_json_path_exists);
    freeTextPatterns(alloc, value.filter_text_patterns);
    if (value.filter_text_patterns.len > 0) alloc.free(value.filter_text_patterns);
    freeExpressionConditions(alloc, value.filter_expressions);
    if (value.filter_expressions.len > 0) alloc.free(value.filter_expressions);
    freeExpressionArrayContains(alloc, value.filter_expression_array_contains);
    if (value.filter_expression_array_contains.len > 0) alloc.free(value.filter_expression_array_contains);
    freeExpressionPredicateGroups(alloc, value.filter_any);
    if (value.filter_any.len > 0) alloc.free(value.filter_any);
    freeExpressionPredicateGroups(alloc, value.filter_not);
    if (value.filter_not.len > 0) alloc.free(value.filter_not);
}

pub fn freeWindowSpecs(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsWindowSpec) void {
    for (values) |value| freeWindowSpec(alloc, value);
}

pub fn cloneQueryRelationalCheckAlloc(alloc: std.mem.Allocator, value: runtime_schema.RelationalCheck) !runtime_schema.RelationalCheck {
    const field = try alloc.dupe(u8, value.field);
    errdefer alloc.free(field);
    const value_json = if (value.value_json) |json| try alloc.dupe(u8, json) else null;
    errdefer if (value_json) |json| alloc.free(json);
    return .{
        .name = "",
        .field = field,
        .op = value.op,
        .value_json = value_json,
        .validation_state = value.validation_state,
        .expression = if (value.expression) |expression| try cloneExpressionConditionAlloc(alloc, expression) else null,
    };
}

pub fn cloneQueryRelationalChecksAlloc(
    alloc: std.mem.Allocator,
    values: []const runtime_schema.RelationalCheck,
) ![]const runtime_schema.RelationalCheck {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalCheck, values.len);
    var initialized: usize = 0;
    errdefer {
        freeRelationalChecks(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try cloneQueryRelationalCheckAlloc(alloc, value);
        initialized += 1;
    }
    return out;
}

pub fn cloneQueryRelationalChecksConcatAlloc(
    alloc: std.mem.Allocator,
    lhs: []const runtime_schema.RelationalCheck,
    rhs: []const runtime_schema.RelationalCheck,
) ![]const runtime_schema.RelationalCheck {
    const out = try alloc.alloc(runtime_schema.RelationalCheck, lhs.len + rhs.len);
    var initialized: usize = 0;
    errdefer {
        freeRelationalChecks(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (lhs) |value| {
        out[initialized] = try cloneQueryRelationalCheckAlloc(alloc, value);
        initialized += 1;
    }
    for (rhs) |value| {
        out[initialized] = try cloneQueryRelationalCheckAlloc(alloc, value);
        initialized += 1;
    }
    return out;
}

pub fn cloneInPredicateAlloc(
    alloc: std.mem.Allocator,
    value: db_mod.types.RelationalRowsInPredicate,
) !db_mod.types.RelationalRowsInPredicate {
    const field = try alloc.dupe(u8, value.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const values_json = try alloc.dupe(u8, value.values_json);
    var values_transferred = false;
    errdefer if (!values_transferred) alloc.free(values_json);
    field_transferred = true;
    values_transferred = true;
    return .{
        .field = field,
        .values_json = values_json,
        .negated = value.negated,
    };
}

pub fn cloneInPredicatesAlloc(
    alloc: std.mem.Allocator,
    values: []const db_mod.types.RelationalRowsInPredicate,
) ![]const db_mod.types.RelationalRowsInPredicate {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsInPredicate, values.len);
    var initialized: usize = 0;
    errdefer {
        freeInPredicates(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try cloneInPredicateAlloc(alloc, value);
        initialized += 1;
    }
    return out;
}

pub fn cloneInPredicatesConcatAlloc(
    alloc: std.mem.Allocator,
    lhs: []const db_mod.types.RelationalRowsInPredicate,
    rhs: []const db_mod.types.RelationalRowsInPredicate,
) ![]const db_mod.types.RelationalRowsInPredicate {
    const out = try alloc.alloc(db_mod.types.RelationalRowsInPredicate, lhs.len + rhs.len);
    var initialized: usize = 0;
    errdefer {
        freeInPredicates(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (lhs) |value| {
        out[initialized] = try cloneInPredicateAlloc(alloc, value);
        initialized += 1;
    }
    for (rhs) |value| {
        out[initialized] = try cloneInPredicateAlloc(alloc, value);
        initialized += 1;
    }
    return out;
}

pub fn freeMergeFieldMappingValues(alloc: std.mem.Allocator, values: []const MergeFieldMapping) void {
    for (values) |value| {
        alloc.free(value.target_field);
        alloc.free(value.source_field);
    }
}

fn freeMergeFieldMappings(alloc: std.mem.Allocator, values: []const MergeFieldMapping) void {
    freeMergeFieldMappingValues(alloc, values);
    if (values.len > 0) alloc.free(values);
}

pub fn freeMergeExpressionAssignmentValues(alloc: std.mem.Allocator, values: []const MergeExpressionAssignment) void {
    for (values) |value| {
        alloc.free(value.target_field);
        freeExpression(alloc, value.expression);
    }
}

fn freeMergeExpressionAssignments(alloc: std.mem.Allocator, values: []const MergeExpressionAssignment) void {
    freeMergeExpressionAssignmentValues(alloc, values);
    if (values.len > 0) alloc.free(values);
}

pub fn freeMergeMatchedArmValue(alloc: std.mem.Allocator, value: MergeMatchedArm) void {
    freeMergeArmPredicates(alloc, value.predicates);
    freeExpressionConditions(alloc, value.expression_predicates);
    if (value.expression_predicates.len > 0) alloc.free(value.expression_predicates);
    freeExpressionPredicateGroups(alloc, value.expression_or_predicates);
    if (value.expression_or_predicates.len > 0) alloc.free(value.expression_or_predicates);
    freeExpressionPredicateGroups(alloc, value.expression_not_predicates);
    if (value.expression_not_predicates.len > 0) alloc.free(value.expression_not_predicates);
    freeMergeFieldMappings(alloc, value.update);
    freeMergeExpressionAssignments(alloc, value.update_expressions);
}

pub fn freeMergeMatchedArmValues(alloc: std.mem.Allocator, values: []const MergeMatchedArm) void {
    for (values) |value| freeMergeMatchedArmValue(alloc, value);
}

fn freeMergeMatchedArms(alloc: std.mem.Allocator, values: []const MergeMatchedArm) void {
    freeMergeMatchedArmValues(alloc, values);
    if (values.len > 0) alloc.free(values);
}

pub fn freeMergeNotMatchedArmValue(alloc: std.mem.Allocator, value: MergeNotMatchedArm) void {
    freeMergeArmPredicates(alloc, value.predicates);
    freeExpressionConditions(alloc, value.expression_predicates);
    if (value.expression_predicates.len > 0) alloc.free(value.expression_predicates);
    freeExpressionPredicateGroups(alloc, value.expression_or_predicates);
    if (value.expression_or_predicates.len > 0) alloc.free(value.expression_or_predicates);
    freeExpressionPredicateGroups(alloc, value.expression_not_predicates);
    if (value.expression_not_predicates.len > 0) alloc.free(value.expression_not_predicates);
    freeMergeFieldMappings(alloc, value.insert);
    freeMergeExpressionAssignments(alloc, value.insert_expressions);
}

pub fn freeMergeNotMatchedArmValues(alloc: std.mem.Allocator, values: []const MergeNotMatchedArm) void {
    for (values) |value| freeMergeNotMatchedArmValue(alloc, value);
}

fn freeMergeNotMatchedArms(alloc: std.mem.Allocator, values: []const MergeNotMatchedArm) void {
    freeMergeNotMatchedArmValues(alloc, values);
    if (values.len > 0) alloc.free(values);
}

pub fn freeMergeArmPredicateValues(alloc: std.mem.Allocator, values: []const MergeArmPredicate) void {
    for (values) |value| freeMergeArmPredicateValue(alloc, value);
}

pub fn freeMergeArmPredicateValue(alloc: std.mem.Allocator, value: MergeArmPredicate) void {
    alloc.free(value.field);
    if (value.value_json) |json| alloc.free(json);
}

fn freeMergeArmPredicates(alloc: std.mem.Allocator, values: []const MergeArmPredicate) void {
    freeMergeArmPredicateValues(alloc, values);
    if (values.len > 0) alloc.free(values);
}

test "sql adapter plan clones and frees row expressions" {
    const alloc = std.testing.allocator;

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    operands[0] = .{
        .kind = .field,
        .field = try alloc.dupe(u8, "tenant_id"),
        .field_source = .row,
    };
    operands[1] = .{
        .kind = .value,
        .value_json = try alloc.dupe(u8, "\"tenant-a\""),
    };

    const branch_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    branch_rhs[0] = .{
        .kind = .value,
        .value_json = try alloc.dupe(u8, "\"active\""),
    };

    const branches = try alloc.alloc(db_mod.types.RelationalRowsExpressionCaseBranch, 1);
    branches[0] = .{
        .when = .{
            .lhs = .{
                .kind = .field,
                .field = try alloc.dupe(u8, "status"),
                .field_source = .row,
            },
            .op = .eq,
            .rhs = branch_rhs,
        },
        .then = .{
            .kind = .field,
            .field = try alloc.dupe(u8, "payload"),
            .json_path = try alloc.dupe(u8, "$.tier"),
            .json_as_text = true,
        },
    };

    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    fallback[0] = .{
        .kind = .value,
        .value_json = try alloc.dupe(u8, "\"unknown\""),
    };

    const expression: db_mod.types.RelationalRowsExpression = .{
        .kind = .case,
        .operands = operands,
        .case_branches = branches,
        .case_else = fallback,
    };
    defer freeExpression(alloc, expression);

    var cloned = try cloneExpressionAlloc(alloc, expression);
    defer freeExpression(alloc, cloned);

    try std.testing.expect(cloned.operands.ptr != expression.operands.ptr);
    try std.testing.expect(cloned.case_branches.ptr != expression.case_branches.ptr);
    try std.testing.expect(cloned.case_else.ptr != expression.case_else.ptr);
    try std.testing.expectEqualStrings("tenant_id", cloned.operands[0].field);
    try std.testing.expectEqualStrings("$.tier", cloned.case_branches[0].then.json_path);

    rewriteExpressionFieldsToSource(&cloned);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, cloned.operands[0].field_source);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, cloned.case_branches[0].when.lhs.field_source);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, cloned.case_branches[0].then.field_source);

    const order_by = try alloc.alloc(db_mod.types.RelationalRowsQueryOrder, 2);
    defer {
        freeOrderBy(alloc, order_by);
        alloc.free(order_by);
    }
    order_by[0] = .{
        .field = try alloc.dupe(u8, "created_at"),
        .direction = .desc,
        .null_test = .is_not_null,
    };
    order_by[1] = .{
        .expression = .{
            .kind = .lower,
            .operands = try alloc.dupe(db_mod.types.RelationalRowsExpression, &[_]db_mod.types.RelationalRowsExpression{.{
                .kind = .field,
                .field = try alloc.dupe(u8, "email"),
            }}),
        },
    };

    const cloned_order_by = try cloneOrderByAlloc(alloc, order_by);
    defer {
        freeOrderBy(alloc, cloned_order_by);
        alloc.free(cloned_order_by);
    }

    try std.testing.expect(cloned_order_by.ptr != order_by.ptr);
    try std.testing.expectEqualStrings("created_at", cloned_order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, cloned_order_by[0].direction);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderNullTest.is_not_null, cloned_order_by[0].null_test.?);
    try std.testing.expect(cloned_order_by[1].expression != null);
    try std.testing.expect(cloned_order_by[1].expression.?.operands.ptr != order_by[1].expression.?.operands.ptr);
    try std.testing.expectEqualStrings("email", cloned_order_by[1].expression.?.operands[0].field);
}

test "sql adapter plan frees predicate and window ownership containers" {
    const alloc = std.testing.allocator;

    const access_group = db_mod.types.RelationalRowsAccessPredicateGroup{
        .predicates = try alloc.dupe(runtime_schema.RelationalCheck, &[_]runtime_schema.RelationalCheck{.{
            .name = "",
            .field = try alloc.dupe(u8, "tenant_id"),
            .op = .eq,
            .value_json = try alloc.dupe(u8, "\"tenant-a\""),
        }}),
        .array_contains = try alloc.dupe(db_mod.types.RelationalRowsArrayContainsPredicate, &[_]db_mod.types.RelationalRowsArrayContainsPredicate{.{
            .field = try alloc.dupe(u8, "tags"),
            .value_json = try alloc.dupe(u8, "[\"vip\"]"),
        }}),
        .in_predicates = try alloc.dupe(db_mod.types.RelationalRowsInPredicate, &[_]db_mod.types.RelationalRowsInPredicate{.{
            .field = try alloc.dupe(u8, "status"),
            .values_json = try alloc.dupe(u8, "[\"active\",\"trial\"]"),
        }}),
        .json_path_eq = try alloc.dupe(db_mod.types.RelationalRowsJsonPathEqPredicate, &[_]db_mod.types.RelationalRowsJsonPathEqPredicate{.{
            .field = try alloc.dupe(u8, "payload"),
            .path = try alloc.dupe(u8, "$.region"),
            .value_json = try alloc.dupe(u8, "\"us\""),
        }}),
        .text_patterns = try alloc.dupe(db_mod.types.RelationalRowsTextPatternPredicate, &[_]db_mod.types.RelationalRowsTextPatternPredicate{.{
            .field = try alloc.dupe(u8, "email"),
            .pattern = try alloc.dupe(u8, "%@example.com"),
            .case_insensitive = true,
        }}),
    };
    freeAccessPredicateGroup(alloc, access_group);

    const window_order = try alloc.dupe(db_mod.types.RelationalRowsQueryOrder, &[_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = try alloc.dupe(u8, "created_at"),
        .direction = .desc,
    }});

    const window = db_mod.types.RelationalRowsWindowSpec{
        .output = try alloc.dupe(u8, "ranked"),
        .function = .row_number,
        .partition_by = try alloc.dupe([]const u8, &[_][]const u8{try alloc.dupe(u8, "tenant_id")}),
        .order_by = window_order,
        .value_expression = .{
            .kind = .field,
            .field = try alloc.dupe(u8, "amount"),
        },
        .default_json = try alloc.dupe(u8, "0"),
        .filter_predicates = try alloc.dupe(runtime_schema.RelationalCheck, &[_]runtime_schema.RelationalCheck{.{
            .name = "",
            .field = try alloc.dupe(u8, "amount"),
            .op = .gte,
            .value_json = try alloc.dupe(u8, "0"),
        }}),
        .filter_json_contains = try alloc.dupe(db_mod.types.RelationalRowsJsonContainsPredicate, &[_]db_mod.types.RelationalRowsJsonContainsPredicate{.{
            .field = try alloc.dupe(u8, "payload"),
            .value_json = try alloc.dupe(u8, "{\"kind\":\"invoice\"}"),
        }}),
        .filter_expression_array_contains = try alloc.dupe(db_mod.types.RelationalRowsExpressionArrayContainsPredicate, &[_]db_mod.types.RelationalRowsExpressionArrayContainsPredicate{.{
            .expression = .{
                .kind = .field,
                .field = try alloc.dupe(u8, "tags"),
            },
            .value_json = try alloc.dupe(u8, "\"paid\""),
        }}),
        .filter_expressions = try alloc.dupe(db_mod.types.RelationalRowsExpressionCondition, &[_]db_mod.types.RelationalRowsExpressionCondition{.{
            .lhs = .{
                .kind = .field,
                .field = try alloc.dupe(u8, "status"),
            },
            .op = .eq,
            .rhs = try alloc.dupe(db_mod.types.RelationalRowsExpression, &[_]db_mod.types.RelationalRowsExpression{.{
                .kind = .value,
                .value_json = try alloc.dupe(u8, "\"active\""),
            }}),
        }}),
    };
    freeWindowSpec(alloc, window);
}

test "sql adapter plan clones query predicates" {
    const alloc = std.testing.allocator;

    const lhs_checks = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "tenant_id",
        .op = .eq,
        .value_json = "\"tenant-a\"",
        .validation_state = .enforced,
    }};
    const rhs_expression_rhs = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "10",
    }};
    const rhs_checks = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .op = .gte,
        .expression = .{
            .lhs = .{
                .kind = .field,
                .field = "amount",
            },
            .op = .gte,
            .rhs = &rhs_expression_rhs,
        },
    }};

    const cloned_checks = try cloneQueryRelationalChecksConcatAlloc(alloc, &lhs_checks, &rhs_checks);
    defer {
        freeRelationalChecks(alloc, cloned_checks);
        alloc.free(cloned_checks);
    }
    try std.testing.expectEqual(@as(usize, 2), cloned_checks.len);
    try std.testing.expectEqualStrings("tenant_id", cloned_checks[0].field);
    try std.testing.expect(cloned_checks[0].field.ptr != lhs_checks[0].field.ptr);
    try std.testing.expect(cloned_checks[0].value_json != null);
    try std.testing.expect(cloned_checks[0].value_json.?.ptr != lhs_checks[0].value_json.?.ptr);
    try std.testing.expect(cloned_checks[1].expression != null);
    try std.testing.expectEqualStrings("amount", cloned_checks[1].expression.?.lhs.field);
    try std.testing.expect(cloned_checks[1].expression.?.lhs.field.ptr != rhs_checks[0].expression.?.lhs.field.ptr);

    const lhs_in = [_]db_mod.types.RelationalRowsInPredicate{.{
        .field = "status",
        .values_json = "[\"active\"]",
    }};
    const rhs_in = [_]db_mod.types.RelationalRowsInPredicate{.{
        .field = "region",
        .values_json = "[\"us\",\"eu\"]",
        .negated = true,
    }};

    const cloned_in = try cloneInPredicatesConcatAlloc(alloc, &lhs_in, &rhs_in);
    defer {
        freeInPredicates(alloc, cloned_in);
        alloc.free(cloned_in);
    }
    try std.testing.expectEqual(@as(usize, 2), cloned_in.len);
    try std.testing.expectEqualStrings("status", cloned_in[0].field);
    try std.testing.expect(cloned_in[0].field.ptr != lhs_in[0].field.ptr);
    try std.testing.expectEqualStrings("[\"us\",\"eu\"]", cloned_in[1].values_json);
    try std.testing.expect(cloned_in[1].values_json.ptr != rhs_in[0].values_json.ptr);
    try std.testing.expect(cloned_in[1].negated);
}

test "sql adapter lowered read plans own nested storage plan memory" {
    const alloc = std.testing.allocator;

    var lowered = LoweredReadPlan{
        .join = .{
            .left_table_name = try alloc.dupe(u8, "left_table"),
            .right_table_name = try alloc.dupe(u8, "right_table"),
            .join = .{
                .on = try alloc.dupe(db_mod.types.RelationalRowsJoinOn, &[_]db_mod.types.RelationalRowsJoinOn{.{
                    .left_field = try alloc.dupe(u8, "left_id"),
                    .right_field = try alloc.dupe(u8, "right_id"),
                }}),
            },
        },
    };
    lowered.deinit(alloc);
}

test "sql adapter lowered write containers own nested request memory" {
    const alloc = std.testing.allocator;

    var insert = LoweredInsert{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .batch = .{
            .writes = try alloc.dupe(db_mod.types.BatchWrite, &[_]db_mod.types.BatchWrite{.{
                .key = try alloc.dupe(u8, "pk"),
                .value = try alloc.dupe(u8, "{\"id\":\"u1\"}"),
            }}),
        },
    };
    insert.deinit(alloc);

    var mutation_source = LoweredMutationSource{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .mutation = .{ .req = .{ .kind = .delete } },
    };
    mutation_source.deinit(alloc);
}

test "sql adapter lowered merge and explain plans own nested memory" {
    const alloc = std.testing.allocator;

    var merge = LoweredWritePlan{
        .merge_mutation = .{
            .target_table_name = try alloc.dupe(u8, "target_records"),
            .source_table_name = try alloc.dupe(u8, "source_records"),
            .match_fields = try alloc.dupe(MergeFieldMapping, &[_]MergeFieldMapping{.{
                .target_field = try alloc.dupe(u8, "id"),
                .source_field = try alloc.dupe(u8, "id"),
            }}),
            .matched_arms = try alloc.dupe(MergeMatchedArm, &[_]MergeMatchedArm{.{
                .update = try alloc.dupe(MergeFieldMapping, &[_]MergeFieldMapping{.{
                    .target_field = try alloc.dupe(u8, "status"),
                    .source_field = try alloc.dupe(u8, "status"),
                }}),
                .update_expressions = try alloc.dupe(MergeExpressionAssignment, &[_]MergeExpressionAssignment{.{
                    .target_field = try alloc.dupe(u8, "amount"),
                    .expression = .{
                        .kind = .value,
                        .value_json = try alloc.dupe(u8, "1"),
                    },
                }}),
            }}),
            .returning = .{
                .fields = try alloc.dupe([]const u8, &[_][]const u8{try alloc.dupe(u8, "id")}),
                .expressions = try alloc.dupe(db_mod.types.RelationalRowsExpressionProjection, &[_]db_mod.types.RelationalRowsExpressionProjection{.{
                    .output = try alloc.dupe(u8, "one"),
                    .expression = .{
                        .kind = .value,
                        .value_json = try alloc.dupe(u8, "1"),
                    },
                }}),
            },
        },
    };
    merge.deinit(alloc);

    var explain = LoweredExplainPlan{
        .subject = .{
            .read = .{
                .query = .{
                    .table_name = try alloc.dupe(u8, "usage_records"),
                    .plan = .{},
                },
            },
        },
    };
    explain.deinit(alloc);
}
