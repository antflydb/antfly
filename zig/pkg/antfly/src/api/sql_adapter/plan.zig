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

fn freeExpression(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpression) void {
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

fn freeExpressionCaseBranch(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionCaseBranch) void {
    freeExpressionCondition(alloc, value.when);
    freeExpression(alloc, value.then);
}

pub fn freeExpressionCondition(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionCondition) void {
    freeExpression(alloc, value.lhs);
    for (value.rhs) |rhs| freeExpression(alloc, rhs);
    if (value.rhs.len > 0) alloc.free(value.rhs);
}

fn freeExpressionConditions(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (values) |value| freeExpressionCondition(alloc, value);
}

fn freeExpressionPredicateGroup(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionPredicateGroup) void {
    freeExpressionConditions(alloc, value.conditions);
    if (value.conditions.len > 0) alloc.free(value.conditions);
}

fn freeExpressionPredicateGroups(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionPredicateGroup) void {
    for (values) |group| freeExpressionPredicateGroup(alloc, group);
}

fn freeExpressionProjection(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionProjection) void {
    alloc.free(value.output);
    freeExpression(alloc, value.expression);
}

fn freeExpressionProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionProjection) void {
    for (values) |value| freeExpressionProjection(alloc, value);
    if (values.len > 0) alloc.free(values);
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
