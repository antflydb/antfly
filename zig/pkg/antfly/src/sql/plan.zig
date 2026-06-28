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
const sql_statement_kind = @import("statement_kind.zig");
const db_mod = @import("../storage/db/mod.zig");
const ddl_plan = @import("ddl.zig");
const grammar = @import("grammar.zig");
const generated_parser = @import("generated_parser.zig");
const parser = @import("parser.zig");
const query_function = @import("query_function.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const strings = @import("strings.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");
const document_plan = @import("document_plan.zig");

pub const RelationLifetimeKind = grammar.RelationLifetimeKind;
pub const RelationPopulationMode = grammar.RelationPopulationMode;
pub const SelectOutputRef = ast.SelectOutputRef;
pub const SelectSetOperation = ast.SelectSetOperation;
pub const Token = parser.Token;

pub const NamedWindowSpec = struct {
    name: []const u8,
    partition_by: []const []const u8 = &.{},
    order_by: []const db_mod.types.RelationalRowsQueryOrder = &.{},
    frame: ?db_mod.types.RelationalRowsWindowFrame = null,
};

pub const NamedWindowDefinition = struct {
    partition_by: []const []const u8 = &.{},
    order_by: []const db_mod.types.RelationalRowsQueryOrder = &.{},
    frame: ?db_mod.types.RelationalRowsWindowFrame = null,
};

pub const TableAlias = struct {
    name: []const u8,
    alias: []const u8,
};

pub const QualifiedField = struct {
    qualifier: []const u8,
    field: []const u8,
};

pub const QualifiedProjection = struct {
    source: QualifiedField,
    output: []const u8,
};

pub const SetOperationResultTail = struct {
    order_by: []const db_mod.types.RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,
};

const GeneratedLimitValue = struct {
    value: ?u32,
};

fn generatedPaginationExpressionRange(expression: generated_parser.GeneratedSqlExpressionAst) !generated_parser.GeneratedSqlTokenRange {
    if (expression.kind != .token_range) return error.UnsupportedSqlShape;
    return expression.tokens orelse error.UnsupportedSqlShape;
}

fn generatedTokenRangeEqual(
    a: generated_parser.GeneratedSqlTokenRange,
    b: generated_parser.GeneratedSqlTokenRange,
) bool {
    return a.start == b.start and a.end == b.end;
}

fn validateGeneratedOrderListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0 or list.items.len != list.count or list.expression_items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != list.count or list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != list.count or list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != list.count or list.nulls_order_items.len != list.count or list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;
    if (list.first_tokens == null or !generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start >= item.end or item.start < range.start or item.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (item.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = list.items[index - 1];
            if (previous.end + 1 != item.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == list.count and item.end != range.end) return error.UnsupportedSqlShape;
        if (list.alias_items[index] != null or list.alias_name_items[index] != null) return error.UnsupportedSqlShape;

        const expression_range = list.expression_items[index];
        if (expression_range.start < item.start or expression_range.end > item.end or expression_range.start >= expression_range.end) return error.UnsupportedSqlShape;
        if (!generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;

        const direction_end = if (list.direction_items[index]) |direction_range| blk: {
            if (direction_range.start != expression_range.end or direction_range.end > item.end or direction_range.start >= direction_range.end) return error.UnsupportedSqlShape;
            if (tokens[direction_range.start].matchesKeywordTag(.using)) {
                const using_operator = list.order_using_operator_items[index] orelse return error.UnsupportedSqlShape;
                if (list.directions[index] != null) return error.UnsupportedSqlShape;
                if (using_operator.start != direction_range.start + 1 or using_operator.end != direction_range.end) return error.UnsupportedSqlShape;
                if (using_operator.end != using_operator.start + 1) return error.UnsupportedSqlShape;
                switch (tokens[using_operator.start].kind) {
                    .lt, .lte, .gt, .gte => {},
                    else => return error.UnsupportedSqlShape,
                }
            } else {
                if (list.order_using_operator_items[index] != null) return error.UnsupportedSqlShape;
                const direction = list.directions[index] orelse return error.UnsupportedSqlShape;
                if (direction_range.end != direction_range.start + 1) return error.UnsupportedSqlShape;
                switch (direction) {
                    .asc => if (!tokens[direction_range.start].matchesKeywordTag(.asc)) return error.UnsupportedSqlShape,
                    .desc => if (!tokens[direction_range.start].matchesKeywordTag(.desc)) return error.UnsupportedSqlShape,
                }
            }
            break :blk direction_range.end;
        } else blk: {
            if (list.directions[index] != null or list.order_using_operator_items[index] != null) return error.UnsupportedSqlShape;
            break :blk expression_range.end;
        };

        const item_end = if (list.nulls_order_items[index]) |nulls_range| blk: {
            if (nulls_range.start != direction_end or nulls_range.end != nulls_range.start + 2 or nulls_range.end > item.end) return error.UnsupportedSqlShape;
            if (!tokens[nulls_range.start].matchesKeywordTag(.nulls)) return error.UnsupportedSqlShape;
            const nulls_order = list.nulls_orders[index] orelse return error.UnsupportedSqlShape;
            switch (nulls_order) {
                .first => if (!tokens[nulls_range.start + 1].matchesKeywordTag(.first)) return error.UnsupportedSqlShape,
                .last => if (!tokens[nulls_range.start + 1].matchesKeywordTag(.last)) return error.UnsupportedSqlShape,
            }
            break :blk nulls_range.end;
        } else blk: {
            if (list.nulls_orders[index] != null) return error.UnsupportedSqlShape;
            break :blk direction_end;
        };
        if (item_end != item.end) return error.UnsupportedSqlShape;
    }
}

fn generatedOrderClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index + 1 >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.order) or !tokens[keyword_index + 1].matchesKeywordTag(.by)) return null;
    const range = read.order_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos or range.end > tokens.len) return error.UnsupportedSqlShape;
    try validateGeneratedOrderListForClause(tokens, range, read.order_items);
    return range.end;
}

pub const SelectList = struct {
    fields: []const []const u8 = &.{},
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection = &.{},
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection = &.{},
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection = &.{},
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionProjection = &.{},
    outputs: []const SelectOutputRef = &.{},
    select_all: bool = false,
};

pub const SelectItem = union(enum) {
    field: []const u8,
    json_extract: db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: db_mod.types.RelationalRowsCoalesceProjection,
    expression: db_mod.types.RelationalRowsExpressionProjection,
    field_alias: db_mod.types.RelationalRowsFieldAliasProjection,
};

pub fn selectOutputName(select: SelectList, output: SelectOutputRef) ?[]const u8 {
    return switch (output.kind) {
        .field => if (output.index < select.fields.len) select.fields[output.index] else null,
        .json_extract => if (output.index < select.json_extract.len) select.json_extract[output.index].output else null,
        .array_length => if (output.index < select.array_length.len) select.array_length[output.index].output else null,
        .coalesce => if (output.index < select.coalesce.len) select.coalesce[output.index].output else null,
        .field_alias => if (output.index < select.field_aliases.len) select.field_aliases[output.index].output else null,
        .expression => if (output.index < select.expressions.len) select.expressions[output.index].output else null,
    };
}

pub fn selectOutputByName(name: []const u8, select: SelectList) !?SelectOutputRef {
    var found: ?SelectOutputRef = null;
    for (select.outputs) |output| {
        const output_name = selectOutputName(select, output) orelse continue;
        if (!std.ascii.eqlIgnoreCase(output_name, name)) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = output;
    }
    return found;
}

pub const WindowSelectList = struct {
    fields: []const []const u8 = &.{},
    windows: []const db_mod.types.RelationalRowsWindowSpec = &.{},
    outputs: []const WindowSelectOutputRef = &.{},
    select_all: bool = false,
};

pub const WindowSelectOutputKind = enum {
    field,
    window,
};

pub const WindowSelectOutputRef = struct {
    kind: WindowSelectOutputKind,
    index: usize,
};

pub const ParsedWindowFrameBound = struct {
    bound: db_mod.types.RelationalRowsWindowFrameBound,
    offset: u32 = 0,
};

pub const AggregateSelectList = struct {
    group_fields: []const []const u8 = &.{},
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection = &.{},
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec = &.{},
    outputs: []const AggregateSelectOutputRef = &.{},
};

pub const AggregateSelectOutputKind = enum {
    group_field,
    group_expression,
    aggregation,
};

pub const AggregateSelectOutputRef = struct {
    kind: AggregateSelectOutputKind,
    index: usize,
};

pub const LoweredSelect = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    query: db_mod.types.RelationalRowsQueryRequest,
    select_outputs: []const SelectOutputRef = &.{},
    system_time_as_of_sequence: ?u64 = null,

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

pub fn applyCteColumnAliasesAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredSelect,
    aliases: []const []const u8,
) !void {
    if (aliases.len == 0) return;
    if (lowered.query.select_all) return error.UnsupportedSqlShape;
    if (aliases.len != lowered.select_outputs.len) return error.UnsupportedSqlShape;

    var direct_field_outputs: usize = 0;
    for (lowered.select_outputs) |output| {
        if (output.kind == .field) direct_field_outputs += 1;
    }
    if (direct_field_outputs != lowered.query.select.len) return error.UnsupportedSqlShape;

    for (lowered.select_outputs, aliases) |output, alias| {
        if (alias.len == 0) return error.UnsupportedSqlShape;
        try renameCteSelectOutputAlloc(alloc, lowered, output, alias);
    }

    if (direct_field_outputs == 0) return;

    const old_field_aliases = lowered.query.field_aliases;
    const new_field_aliases = try alloc.alloc(db_mod.types.RelationalRowsFieldAliasProjection, old_field_aliases.len + direct_field_outputs);
    var initialized: usize = old_field_aliases.len;
    errdefer {
        for (new_field_aliases[old_field_aliases.len..initialized]) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        alloc.free(new_field_aliases);
    }
    for (old_field_aliases, 0..) |projection, i| new_field_aliases[i] = projection;

    for (lowered.select_outputs, aliases) |output, alias| {
        if (output.kind != .field) continue;
        if (output.index >= lowered.query.select.len) return error.UnsupportedSqlShape;
        const projection = db_mod.types.RelationalRowsFieldAliasProjection{
            .output = try alloc.dupe(u8, alias),
            .field = try alloc.dupe(u8, lowered.query.select[output.index]),
        };
        new_field_aliases[initialized] = projection;
        initialized += 1;
    }

    if (old_field_aliases.len > 0) alloc.free(old_field_aliases);
    freeStringSlice(alloc, lowered.query.select);
    lowered.query.select = &.{};
    lowered.query.field_aliases = new_field_aliases;
}

fn renameCteSelectOutputAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredSelect,
    output: SelectOutputRef,
    alias: []const u8,
) !void {
    switch (output.kind) {
        .field => {},
        .json_extract => {
            if (output.index >= lowered.query.json_extract.len) return error.UnsupportedSqlShape;
            try replaceOwnedStringAlloc(alloc, &lowered.query.json_extract[output.index].output, alias);
        },
        .array_length => {
            if (output.index >= lowered.query.array_length.len) return error.UnsupportedSqlShape;
            try replaceOwnedStringAlloc(alloc, &lowered.query.array_length[output.index].output, alias);
        },
        .coalesce => {
            if (output.index >= lowered.query.coalesce.len) return error.UnsupportedSqlShape;
            const old_output = lowered.query.coalesce[output.index].output;
            try replaceOwnedStringAlloc(alloc, &lowered.query.coalesce[output.index].output, alias);
            for (lowered.query.expressions) |*projection_const| {
                const projection = @constCast(projection_const);
                if (projection.expression.kind != .coalesce) continue;
                if (!std.mem.eql(u8, projection.output, old_output)) continue;
                try replaceOwnedStringAlloc(alloc, &projection.output, alias);
                break;
            }
        },
        .field_alias => {
            if (output.index >= lowered.query.field_aliases.len) return error.UnsupportedSqlShape;
            try replaceOwnedStringAlloc(alloc, &lowered.query.field_aliases[output.index].output, alias);
        },
        .expression => {
            if (output.index >= lowered.query.expressions.len) return error.UnsupportedSqlShape;
            try replaceOwnedStringAlloc(alloc, &lowered.query.expressions[output.index].output, alias);
        },
    }
}

fn replaceOwnedStringAlloc(alloc: std.mem.Allocator, target_const: *const []const u8, replacement: []const u8) !void {
    const target = @constCast(target_const);
    const next = try alloc.dupe(u8, replacement);
    alloc.free(target.*);
    target.* = next;
}

pub const LoweredQueryPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsQueryPlan,
    system_time_as_of_sequence: ?u64 = null,

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
    max_rows: ?u32 = db_mod.types.default_relational_rows_cte_max_rows,
    max_bytes: ?u64 = db_mod.types.default_relational_rows_cte_max_bytes,
    spill_after_bytes: ?u64 = db_mod.types.default_relational_rows_cte_spill_after_bytes,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.left.deinit(alloc);
        self.right.deinit(alloc);
        freeSetOperationOutputColumns(alloc, self.output_columns);
        var order_query: db_mod.types.RelationalRowsQueryRequest = .{ .order_by = self.order_by };
        order_query.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredRecursiveCtePlan = struct {
    cte_name: []const u8,
    operation: SelectSetOperation,
    anchor: LoweredQueryPlan,
    recursive_member: LoweredRecursiveCteMemberPlan,
    final_query: db_mod.types.RelationalRowsQueryRequest = .{},
    output_columns: []const runtime_schema.RelationalColumn = &.{},
    recursive_member_references_cte: bool = false,
    max_rows: ?u32 = db_mod.types.default_relational_rows_cte_max_rows,
    max_bytes: ?u64 = db_mod.types.default_relational_rows_cte_max_bytes,
    spill_after_bytes: ?u64 = db_mod.types.default_relational_rows_cte_spill_after_bytes,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.cte_name);
        self.anchor.deinit(alloc);
        self.recursive_member.deinit(alloc);
        self.final_query.deinit(alloc);
        freeSetOperationOutputColumns(alloc, self.output_columns);
        self.* = undefined;
    }
};

pub const LoweredRecursiveCteMemberPlan = union(enum) {
    join: LoweredRecursiveCteJoinMemberPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .join => |*join| join.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const LoweredRecursiveCteJoinMemberPlan = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    join_type: db_mod.types.RelationalRowsJoinType,
    on: []const db_mod.types.RelationalRowsJoinOn = &.{},
    projections: []const db_mod.types.RelationalRowsExpressionProjection = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.left_table_name);
        alloc.free(self.right_table_name);
        freeJoinOn(alloc, self.on);
        if (self.on.len > 0) alloc.free(self.on);
        freeExpressionProjections(alloc, self.projections);
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
    sync_level: db_mod.types.SyncLevel = .write,
    returning: ReturningProjection = .{},
    returning_expression_count: usize = 0,
    returning_all: bool = false,
    conflict_where: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.returning.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredInsertSource = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    insert_source: relational_rows.OwnedRowsInsertSourceRequest,
    sync_level: db_mod.types.SyncLevel = .write,
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

pub const LoweredRecursiveInsertSource = struct {
    recursive: LoweredRecursiveCtePlan,
    insert_source: LoweredInsertSource,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.recursive.deinit(alloc);
        self.insert_source.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredRecursiveJoinedMutationSource = struct {
    recursive: LoweredRecursiveCtePlan,
    mutation: LoweredJoinedMutationSource,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.recursive.deinit(alloc);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutation = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    sync_level: db_mod.types.SyncLevel = .write,
    returning: ReturningProjection = .{},
    returning_expression_count: usize = 0,
    returning_all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.returning.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutationSource = struct {
    table_name: []const u8,
    additional_table_names: []const []const u8 = &.{},
    mutation: relational_rows.OwnedRowsMutationSourceRequest,
    sync_level: db_mod.types.SyncLevel = .write,
    restart_identity: bool = false,
    truncate_cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        freeStringSlice(alloc, self.additional_table_names);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredJoinedMutationSource = struct {
    target_table_name: []const u8,
    source_table_name: []const u8,
    mutation: relational_rows.OwnedRowsJoinedMutationSourceRequest,
    sync_level: db_mod.types.SyncLevel = .write,

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
    sync_level: db_mod.types.SyncLevel = .write,
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

pub const LoweredDataModifyingCte = struct {
    name: []const u8,
    table_name: []const u8,
    mutation: relational_rows.OwnedRowsMutationSourceRequest,
    claim_placeholder: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.table_name);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMergeMutationPlan = struct {
    target_table_name: []const u8,
    source_table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    data_modifying_ctes: []const LoweredDataModifyingCte = &.{},
    source: db_mod.types.RelationalRowsQueryRequest = .{},
    match_fields: []const MergeFieldMapping = &.{},
    matched_arms: []const MergeMatchedArm = &.{},
    not_matched_arms: []const MergeNotMatchedArm = &.{},
    returning: ReturningProjection = .{},
    sync_level: db_mod.types.SyncLevel = .write,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_table_name);
        alloc.free(self.source_table_name);
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        for (self.data_modifying_ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.data_modifying_ctes.len > 0) alloc.free(self.data_modifying_ctes);
        self.source.deinit(alloc);
        freeMergeFieldMappings(alloc, self.match_fields);
        freeMergeMatchedArms(alloc, self.matched_arms);
        freeMergeNotMatchedArms(alloc, self.not_matched_arms);
        self.returning.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredRecursiveMergeMutation = struct {
    recursive: LoweredRecursiveCtePlan,
    merge: LoweredMergeMutationPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.recursive.deinit(alloc);
        self.merge.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredWritePlan = union(enum) {
    insert: LoweredInsert,
    insert_source: LoweredInsertSource,
    recursive_insert_source: LoweredRecursiveInsertSource,
    update: LoweredMutation,
    delete: LoweredMutation,
    update_source: LoweredMutationSource,
    delete_source: LoweredMutationSource,
    truncate_source: LoweredMutationSource,
    update_joined_source: LoweredJoinedMutationSource,
    delete_joined_source: LoweredJoinedMutationSource,
    recursive_update_joined_source: LoweredRecursiveJoinedMutationSource,
    recursive_delete_joined_source: LoweredRecursiveJoinedMutationSource,
    merge_mutation: LoweredMergeMutationPlan,
    recursive_merge_mutation: LoweredRecursiveMergeMutation,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .insert => |*insert| insert.deinit(alloc),
            .insert_source => |*insert_source| insert_source.deinit(alloc),
            .recursive_insert_source => |*recursive_insert_source| recursive_insert_source.deinit(alloc),
            .update => |*update| update.deinit(alloc),
            .delete => |*delete| delete.deinit(alloc),
            .update_source => |*update_source| update_source.deinit(alloc),
            .delete_source => |*delete_source| delete_source.deinit(alloc),
            .truncate_source => |*truncate_source| truncate_source.deinit(alloc),
            .update_joined_source => |*update_joined_source| update_joined_source.deinit(alloc),
            .delete_joined_source => |*delete_joined_source| delete_joined_source.deinit(alloc),
            .recursive_update_joined_source => |*recursive_update_joined_source| recursive_update_joined_source.deinit(alloc),
            .recursive_delete_joined_source => |*recursive_delete_joined_source| recursive_delete_joined_source.deinit(alloc),
            .merge_mutation => |*merge_mutation| merge_mutation.deinit(alloc),
            .recursive_merge_mutation => |*recursive_merge_mutation| recursive_merge_mutation.deinit(alloc),
        }
        self.* = undefined;
    }
};

fn applyRowsBatchSyncLevel(batch: *relational_rows.OwnedRowsBatchRequest, sync_level: db_mod.types.SyncLevel) void {
    batch.req.sync_level = sync_level;
}

fn applyLoweredWritePlanSyncLevel(plan: *LoweredWritePlan, sync_level: db_mod.types.SyncLevel) void {
    switch (plan.*) {
        .insert => |*insert| {
            insert.sync_level = sync_level;
            applyRowsBatchSyncLevel(&insert.batch, sync_level);
        },
        .insert_source => |*insert_source| insert_source.sync_level = sync_level,
        .recursive_insert_source => |*recursive_insert_source| recursive_insert_source.insert_source.sync_level = sync_level,
        .update => |*update| {
            update.sync_level = sync_level;
            applyRowsBatchSyncLevel(&update.batch, sync_level);
        },
        .delete => |*delete| {
            delete.sync_level = sync_level;
            applyRowsBatchSyncLevel(&delete.batch, sync_level);
        },
        .update_source => |*update_source| update_source.sync_level = sync_level,
        .delete_source => |*delete_source| delete_source.sync_level = sync_level,
        .truncate_source => |*truncate_source| truncate_source.sync_level = sync_level,
        .update_joined_source => |*update_joined_source| update_joined_source.sync_level = sync_level,
        .delete_joined_source => |*delete_joined_source| delete_joined_source.sync_level = sync_level,
        .recursive_update_joined_source => |*recursive_update_joined_source| recursive_update_joined_source.mutation.sync_level = sync_level,
        .recursive_delete_joined_source => |*recursive_delete_joined_source| recursive_delete_joined_source.mutation.sync_level = sync_level,
        .merge_mutation => |*merge_mutation| merge_mutation.sync_level = sync_level,
        .recursive_merge_mutation => |*recursive_merge_mutation| recursive_merge_mutation.merge.sync_level = sync_level,
    }
}

fn loweredWritePlanWithSyncLevel(plan: LoweredWritePlan, sync_level: db_mod.types.SyncLevel) LoweredWritePlan {
    var owned = plan;
    applyLoweredWritePlanSyncLevel(&owned, sync_level);
    return owned;
}

pub const MutationSelectorKind = enum {
    point,
    source,
};

pub const WritePlanLoweringHooks = struct {
    ptr: *anyopaque,
    lower_recursive_insert_source: *const fn (*anyopaque, runtime_schema.TableSchema, relational_rows.UniqueSelectorResolver) anyerror!LoweredRecursiveInsertSource,
    lower_recursive_update_joined_source: *const fn (*anyopaque, runtime_schema.TableSchema, db_mod.types.RowClaimRequest) anyerror!LoweredRecursiveJoinedMutationSource,
    lower_recursive_delete_joined_source: *const fn (*anyopaque, runtime_schema.TableSchema, db_mod.types.RowClaimRequest) anyerror!LoweredRecursiveJoinedMutationSource,
    lower_recursive_merge_mutation: *const fn (*anyopaque, runtime_schema.TableSchema) anyerror!LoweredRecursiveMergeMutation,
    lower_insert: *const fn (*anyopaque, relational_rows.UniqueSelectorResolver) anyerror!LoweredInsert,
    lower_insert_source: *const fn (*anyopaque, relational_rows.UniqueSelectorResolver) anyerror!LoweredInsertSource,
    lower_insert_source_with_schema: *const fn (*anyopaque, runtime_schema.TableSchema, relational_rows.UniqueSelectorResolver) anyerror!LoweredInsertSource,
    lower_update_joined_source: *const fn (*anyopaque, runtime_schema.TableSchema, db_mod.types.RowClaimRequest) anyerror!LoweredJoinedMutationSource,
    classify_update_selector: *const fn (*anyopaque, ?relational_rows.UniqueSelectorResolver) anyerror!MutationSelectorKind,
    lower_update: *const fn (*anyopaque, relational_rows.UniqueSelectorResolver) anyerror!LoweredMutation,
    lower_update_source: *const fn (*anyopaque, db_mod.types.RowClaimRequest) anyerror!LoweredMutationSource,
    lower_delete_joined_source: *const fn (*anyopaque, runtime_schema.TableSchema, db_mod.types.RowClaimRequest) anyerror!LoweredJoinedMutationSource,
    classify_delete_selector: *const fn (*anyopaque, ?relational_rows.UniqueSelectorResolver) anyerror!MutationSelectorKind,
    lower_delete: *const fn (*anyopaque, relational_rows.UniqueSelectorResolver) anyerror!LoweredMutation,
    lower_delete_source: *const fn (*anyopaque, db_mod.types.RowClaimRequest) anyerror!LoweredMutationSource,
    lower_truncate_source: *const fn (*anyopaque, db_mod.types.RowClaimRequest) anyerror!LoweredMutationSource,
    lower_merge_mutation: *const fn (*anyopaque, runtime_schema.TableSchema) anyerror!LoweredMergeMutationPlan,
};

fn lowerRecursiveWritePlanWithHooksAlloc(
    recursive_write_kind: sql_statement_kind.SqlWriteStatementKind,
    schema: runtime_schema.TableSchema,
    options: LowerWritePlanOptions,
    hooks: WritePlanLoweringHooks,
) !LoweredWritePlan {
    switch (recursive_write_kind) {
        .insert_source => {
            const resolver = options.unique_resolver orelse return error.UnsupportedRowsSelector;
            const source_schema = options.insert_source_schema orelse schema;
            return loweredWritePlanWithSyncLevel(.{ .recursive_insert_source = try hooks.lower_recursive_insert_source(hooks.ptr, source_schema, resolver) }, options.sync_level);
        },
        .update,
        .update_source,
        .update_joined_source,
        => {
            const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
            const source_schema = options.joined_source_schema orelse schema;
            const lowered = try hooks.lower_recursive_update_joined_source(hooks.ptr, source_schema, row_claim);
            return loweredWritePlanWithSyncLevel(.{ .recursive_update_joined_source = lowered }, options.sync_level);
        },
        .delete,
        .delete_source,
        .delete_joined_source,
        => {
            const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
            const source_schema = options.joined_source_schema orelse schema;
            const lowered = try hooks.lower_recursive_delete_joined_source(hooks.ptr, source_schema, row_claim);
            return loweredWritePlanWithSyncLevel(.{ .recursive_delete_joined_source = lowered }, options.sync_level);
        },
        .merge => {
            const source_schema = options.joined_source_schema orelse schema;
            const lowered = try hooks.lower_recursive_merge_mutation(hooks.ptr, source_schema);
            return loweredWritePlanWithSyncLevel(.{ .recursive_merge_mutation = lowered }, options.sync_level);
        },
        .insert,
        .truncate,
        => return error.UnsupportedSqlShape,
    }
}

pub fn lowerWritePlanWithParsedSqlAlloc(
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    options: LowerWritePlanOptions,
    hooks: WritePlanLoweringHooks,
) !LoweredWritePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;

    const write_statement = parsed_sql.writeStatementIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    const write_kind = write_statement.kind;
    if (write_statement.recursive) {
        return try lowerRecursiveWritePlanWithHooksAlloc(write_statement.kind, schema, options, hooks);
    }

    if (write_kind == .insert) {
        const resolver = options.unique_resolver orelse return error.UnsupportedRowsSelector;
        return loweredWritePlanWithSyncLevel(.{ .insert = try hooks.lower_insert(hooks.ptr, resolver) }, options.sync_level);
    }

    if (write_kind == .insert_source) {
        const resolver = options.unique_resolver orelse return error.UnsupportedRowsSelector;
        if (options.insert_source_schema) |source_schema| {
            return loweredWritePlanWithSyncLevel(.{ .insert_source = try hooks.lower_insert_source_with_schema(hooks.ptr, source_schema, resolver) }, options.sync_level);
        }
        return loweredWritePlanWithSyncLevel(.{ .insert_source = try hooks.lower_insert_source(hooks.ptr, resolver) }, options.sync_level);
    }

    if (write_kind == .update) {
        switch (try hooks.classify_update_selector(hooks.ptr, options.unique_resolver)) {
            .point => {
                const resolver = options.unique_resolver orelse return error.UnsupportedRowsSelector;
                return loweredWritePlanWithSyncLevel(.{ .update = try hooks.lower_update(hooks.ptr, resolver) }, options.sync_level);
            },
            .source => {
                const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
                return loweredWritePlanWithSyncLevel(.{ .update_source = try hooks.lower_update_source(hooks.ptr, row_claim) }, options.sync_level);
            },
        }
    }

    if (write_kind == .update_source) {
        const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
        return loweredWritePlanWithSyncLevel(.{ .update_source = try hooks.lower_update_source(hooks.ptr, row_claim) }, options.sync_level);
    }

    if (write_kind == .update_joined_source) {
        const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
        const source_schema = options.joined_source_schema orelse schema;
        return loweredWritePlanWithSyncLevel(.{ .update_joined_source = try hooks.lower_update_joined_source(hooks.ptr, source_schema, row_claim) }, options.sync_level);
    }

    if (write_kind == .delete) {
        switch (try hooks.classify_delete_selector(hooks.ptr, options.unique_resolver)) {
            .point => {
                const resolver = options.unique_resolver orelse return error.UnsupportedRowsSelector;
                return loweredWritePlanWithSyncLevel(.{ .delete = try hooks.lower_delete(hooks.ptr, resolver) }, options.sync_level);
            },
            .source => {
                const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
                return loweredWritePlanWithSyncLevel(.{ .delete_source = try hooks.lower_delete_source(hooks.ptr, row_claim) }, options.sync_level);
            },
        }
    }

    if (write_kind == .delete_source) {
        const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
        return loweredWritePlanWithSyncLevel(.{ .delete_source = try hooks.lower_delete_source(hooks.ptr, row_claim) }, options.sync_level);
    }

    if (write_kind == .delete_joined_source) {
        const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
        const source_schema = options.joined_source_schema orelse schema;
        return loweredWritePlanWithSyncLevel(.{ .delete_joined_source = try hooks.lower_delete_joined_source(hooks.ptr, source_schema, row_claim) }, options.sync_level);
    }

    if (write_kind == .truncate) {
        const row_claim = options.row_claim orelse return error.UnsupportedRowsQuery;
        return loweredWritePlanWithSyncLevel(.{ .truncate_source = try hooks.lower_truncate_source(hooks.ptr, row_claim) }, options.sync_level);
    }

    if (write_kind == .merge) {
        const source_schema = options.joined_source_schema orelse schema;
        return loweredWritePlanWithSyncLevel(.{ .merge_mutation = try hooks.lower_merge_mutation(hooks.ptr, source_schema) }, options.sync_level);
    }

    return error.UnsupportedSqlShape;
}

pub fn mergeMatchedPredicateCount(arms: []const MergeMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.predicates.len;
    return total;
}

pub fn mergeMatchedUpdateCount(arms: []const MergeMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.update.len;
    return total;
}

pub fn mergeMatchedUpdateExpressionCount(arms: []const MergeMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.update_expressions.len;
    return total;
}

pub fn mergeMatchedExpressionPredicateCount(arms: []const MergeMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.expression_predicates.len;
    return total;
}

pub fn mergeMatchedExpressionOrPredicateCount(arms: []const MergeMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.expression_or_predicates.len;
    return total;
}

pub fn mergeMatchedExpressionNotPredicateCount(arms: []const MergeMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.expression_not_predicates.len;
    return total;
}

pub fn mergeMatchedHasDelete(arms: []const MergeMatchedArm) bool {
    for (arms) |arm| if (arm.delete) return true;
    return false;
}

pub fn mergeMatchedHasDoNothing(arms: []const MergeMatchedArm) bool {
    for (arms) |arm| if (arm.do_nothing) return true;
    return false;
}

pub fn mergeNotMatchedPredicateCount(arms: []const MergeNotMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.predicates.len;
    return total;
}

pub fn mergeNotMatchedInsertCount(arms: []const MergeNotMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.insert.len;
    return total;
}

pub fn mergeNotMatchedInsertExpressionCount(arms: []const MergeNotMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.insert_expressions.len;
    return total;
}

pub fn mergeNotMatchedExpressionPredicateCount(arms: []const MergeNotMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.expression_predicates.len;
    return total;
}

pub fn mergeNotMatchedExpressionOrPredicateCount(arms: []const MergeNotMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.expression_or_predicates.len;
    return total;
}

pub fn mergeNotMatchedExpressionNotPredicateCount(arms: []const MergeNotMatchedArm) usize {
    var total: usize = 0;
    for (arms) |arm| total += arm.expression_not_predicates.len;
    return total;
}

pub fn mergeNotMatchedHasDoNothing(arms: []const MergeNotMatchedArm) bool {
    for (arms) |arm| if (arm.do_nothing) return true;
    return false;
}

pub const LoweredAggregate = struct {
    table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte = &.{},
    aggregate: db_mod.types.RelationalRowsAggregateRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        freePlanCtes(alloc, self.ctes);
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

pub fn findCteByName(ctes: []const db_mod.types.RelationalRowsCte, name: []const u8) ?db_mod.types.RelationalRowsCte {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

pub const CteSelectParserHooks = struct {
    ptr: *anyopaque,
    parse_select: *const fn (
        *anyopaque,
        []const Token,
        []const db_mod.types.RelationalRowsCte,
    ) anyerror!LoweredSelect,
};

pub const ReadPlanParserContext = struct {
    schema: runtime_schema.TableSchema,
    available_ctes: []const db_mod.types.RelationalRowsCte = &.{},
    allow_select_set_boundary: bool = false,
    allow_select_set_result_tail_boundary: bool = false,
};

pub const ReadPlanParserContextHooks = struct {
    ptr: *anyopaque,
    get_context: *const fn (*anyopaque) ReadPlanParserContext,
    set_context: *const fn (*anyopaque, ReadPlanParserContext) void,
};

pub const WindowPlanParserHooks = struct {
    ptr: *anyopaque,
    context_hooks: ReadPlanParserContextHooks,
    parse_window: *const fn (*anyopaque) anyerror!LoweredWindowPlan,
};

pub const AggregatePlanParserHooks = struct {
    ptr: *anyopaque,
    context_hooks: ReadPlanParserContextHooks,
    parse_aggregate: *const fn (*anyopaque) anyerror!LoweredAggregate,
};

pub const JoinPlanParserHooks = struct {
    ptr: *anyopaque,
    context_hooks: ReadPlanParserContextHooks,
    parse_join: *const fn (*anyopaque) anyerror!LoweredJoin,
};

pub const LateralPlanParserHooks = struct {
    ptr: *anyopaque,
    context_hooks: ReadPlanParserContextHooks,
    parse_lateral: *const fn (*anyopaque) anyerror!LoweredLateralPlan,
};

pub const RecursiveCteParserHooks = struct {
    ptr: *anyopaque,
    parse_select_with_set_boundary: *const fn (
        *anyopaque,
        []const Token,
        *usize,
        []const db_mod.types.RelationalRowsCte,
    ) anyerror!LoweredSelect,
    select_output_columns: *const fn (
        *anyopaque,
        LoweredSelect,
    ) anyerror![]runtime_schema.RelationalColumn,
    parse_recursive_member: *const fn (
        *anyopaque,
        []const Token,
        *usize,
        []const db_mod.types.RelationalRowsCte,
        []const u8,
    ) anyerror!LoweredRecursiveCteMemberPlan,
};

pub const RecursiveCteMemberParserHooks = struct {
    ptr: *anyopaque,
    parse_projection_expression: *const fn (
        *anyopaque,
        []const Token,
        *usize,
        runtime_schema.TableSchema,
        []const []const u8,
        []const []const u8,
    ) anyerror!RecursiveCteMemberProjectionExpression,
    parse_join_on: *const fn (
        *anyopaque,
        []const Token,
        *usize,
        runtime_schema.TableSchema,
        bool,
        bool,
        db_mod.types.RelationalRowsJoinType,
        []const u8,
        []const u8,
    ) anyerror![]const db_mod.types.RelationalRowsJoinOn,
};

pub const RecursiveCteMemberProjectionExpression = struct {
    expression: db_mod.types.RelationalRowsExpression,
    default_output: []const u8,
};

pub const SetOperationParserHooks = struct {
    ptr: *anyopaque,
    context_hooks: ReadPlanParserContextHooks,
    parse_select: *const fn (*anyopaque) anyerror!LoweredSelect,
    select_output_columns: *const fn (
        *anyopaque,
        LoweredSelect,
    ) anyerror![]runtime_schema.RelationalColumn,
    parse_result_tail: *const fn (
        *anyopaque,
        LoweredSelect,
    ) anyerror!SetOperationResultTail,
};

pub const SimpleSelectSetTailHooks = struct {
    ptr: *anyopaque,
    parse_order_by: *const fn (
        *anyopaque,
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
        SelectList,
    ) anyerror!void,
};

fn parseGeneratedLimitValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedLimitValue {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.limit)) return null;
    const range = read.limit_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (read.limit_all) {
        if (read.limit_expression.tokens != null) return error.UnsupportedSqlShape;
        pos.* = range.end;
        return .{ .value = null };
    }
    const expression_range = try generatedPaginationExpressionRange(read.limit_expression);
    if (expression_range.start != range.start or expression_range.end != range.end) return error.UnsupportedSqlShape;
    var generated_pos = expression_range.start;
    const limit = try value_mod.parseLimitValue(tokens, &generated_pos, params);
    if (generated_pos != expression_range.end) return error.UnsupportedSqlShape;
    pos.* = range.end;
    return .{ .value = limit };
}

fn parseGeneratedOffsetValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?u32 {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.offset)) return null;
    const range = read.offset_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;
    const expression_range = try generatedPaginationExpressionRange(read.offset_expression);
    if (expression_range.start != range.start or expression_range.end > range.end) return error.UnsupportedSqlShape;
    if (expression_range.end != range.end) {
        if (expression_range.end + 1 != range.end) return error.UnsupportedSqlShape;
        if (!tokens[expression_range.end].matchesKeywordTag(.row) and !tokens[expression_range.end].matchesKeywordTag(.rows)) return error.UnsupportedSqlShape;
    }
    var generated_pos = expression_range.start;
    const offset = (try value_mod.parseNullableSqlU32Value(tokens, &generated_pos, params)) orelse 0;
    if (generated_pos != expression_range.end) return error.UnsupportedSqlShape;
    pos.* = range.end;
    return offset;
}

fn parseGeneratedFetchLimitValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedLimitValue {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.fetch)) return null;
    const range = read.fetch_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;
    var clause_pos = range.start;
    const clause_limit = try value_mod.parseFetchLimitValue(tokens, &clause_pos, params);
    if (clause_pos != range.end) return error.UnsupportedSqlShape;
    const limit = if (read.fetch_count_tokens) |count_range| blk: {
        const expression_range = try generatedPaginationExpressionRange(read.fetch_count_expression);
        if (expression_range.start != count_range.start or expression_range.end != count_range.end) return error.UnsupportedSqlShape;
        var generated_pos = expression_range.start;
        const generated_limit = try value_mod.parseLimitValue(tokens, &generated_pos, params);
        if (generated_pos != expression_range.end) return error.UnsupportedSqlShape;
        if (generated_limit != clause_limit) return error.UnsupportedSqlShape;
        break :blk generated_limit;
    } else blk: {
        if (read.fetch_count_expression.tokens != null) return error.UnsupportedSqlShape;
        break :blk clause_limit;
    };
    pos.* = range.end;
    return .{ .value = limit };
}

test "sql adapter plan parses generated pagination from expression AST ranges" {
    const alloc = std.testing.allocator;

    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records LIMIT 5 OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY",
    );
    defer parsed_sql.deinit(alloc);
    const generated_raw = parsed_sql.generated_statement orelse return error.TestUnexpectedResult;
    const read_ast = switch (generated_raw.ast orelse return error.TestUnexpectedResult) {
        .read => |read| read,
        else => return error.TestUnexpectedResult,
    };
    const tokens = parsed_sql.items();

    var limit_pos: usize = 5;
    const limit = (try parseGeneratedLimitValueForClause(tokens, 4, &limit_pos, &.{}, read_ast)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(?u32, 5), limit.value);
    try std.testing.expectEqual(@as(usize, 6), limit_pos);

    var offset_pos: usize = 7;
    const offset = (try parseGeneratedOffsetValueForClause(tokens, 6, &offset_pos, &.{}, read_ast)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 2), offset);
    try std.testing.expectEqual(@as(usize, 9), offset_pos);

    var fetch_pos: usize = 10;
    const fetch = (try parseGeneratedFetchLimitValueForClause(tokens, 9, &fetch_pos, &.{}, read_ast)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(?u32, 3), fetch.value);
    try std.testing.expectEqual(@as(usize, tokens.len), fetch_pos);

    var malformed_limit_ast = read_ast.*;
    malformed_limit_ast.limit_expression.tokens = malformed_limit_ast.projection_items.expression_items[0];
    limit_pos = 5;
    try std.testing.expectError(error.UnsupportedSqlShape, parseGeneratedLimitValueForClause(tokens, 4, &limit_pos, &.{}, &malformed_limit_ast));

    var malformed_offset_ast = read_ast.*;
    malformed_offset_ast.offset_expression.tokens = malformed_offset_ast.projection_items.expression_items[0];
    offset_pos = 7;
    try std.testing.expectError(error.UnsupportedSqlShape, parseGeneratedOffsetValueForClause(tokens, 6, &offset_pos, &.{}, &malformed_offset_ast));

    var malformed_fetch_ast = read_ast.*;
    malformed_fetch_ast.fetch_count_expression.tokens = malformed_fetch_ast.projection_items.expression_items[0];
    fetch_pos = 10;
    try std.testing.expectError(error.UnsupportedSqlShape, parseGeneratedFetchLimitValueForClause(tokens, 9, &fetch_pos, &.{}, &malformed_fetch_ast));
}

pub fn parseCtesForPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    base_table_name: *?[]const u8,
    hooks: CteSelectParserHooks,
) ![]const db_mod.types.RelationalRowsCte {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeywordTag(.with);
    var ctes = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCte).empty;
    errdefer {
        for (ctes.items) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        ctes.deinit(alloc);
    }

    while (true) {
        const cte_name = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (findCteByName(ctes.items, cte_name) != null) return error.UnsupportedSqlShape;
        const cte_column_aliases = try grammar.parseOptionalCteColumnAliasesAlloc(alloc, tokens, pos);
        defer strings.freeStringSlice(alloc, cte_column_aliases);
        try cursor.expectKeywordTag(.as);
        try parser.consumeCteMaterializationHint(tokens, pos);
        try cursor.expectToken(.lparen);
        const close_index = (parser.findMatchingRParenAfterOpenIndex(tokens, pos.*) orelse return error.UnsupportedSqlShape);
        if (lowerAntflyGraphTableFunctionCteAlloc(alloc, tokens[pos.*..close_index])) |table_function| {
            pos.* = close_index + 1;
            try resolveTableFunctionBaseSourceTableAlloc(alloc, table_function, base_table_name);
            try ctes.append(alloc, .{
                .name = cte_name,
                .table_function = table_function,
            });
            cte_name_transferred = true;
            if (cursor.matchToken(.comma) == null) break;
            continue;
        } else |err| switch (err) {
            error.UnsupportedSqlShape => {},
            else => return err,
        }
        var lowered = try hooks.parse_select(hooks.ptr, tokens[pos.*..close_index], ctes.items);
        errdefer lowered.deinit(alloc);
        pos.* = close_index + 1;
        try applyCteColumnAliasesAlloc(alloc, &lowered, cte_column_aliases);
        try resolveSelectSourceForPlanAlloc(alloc, &lowered, ctes.items, base_table_name);
        try ctes.append(alloc, .{
            .name = cte_name,
            .query = lowered.query,
        });
        lowered.query = .{};
        lowered.clearSelectOutputs(alloc);
        alloc.free(lowered.table_name);
        lowered.table_name = "";
        cte_name_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }

    return try ctes.toOwnedSlice(alloc);
}

pub fn lowerAntflyGraphTableFunctionCteAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !db_mod.types.RelationalRowsTableFunction {
    return try query_function.lowerAntflyGraphTableFunctionTokensAlloc(alloc, tokens);
}

pub fn resolveTableFunctionBaseSourceTableAlloc(
    alloc: std.mem.Allocator,
    table_function: db_mod.types.RelationalRowsTableFunction,
    base_table_name: *?[]const u8,
) !void {
    const table_name = switch (table_function) {
        .graph_query => |graph_query| graph_query.table_name,
        .graph_metric_query => |graph_metric_query| graph_metric_query.table_name,
        .graph_metric_rerank_query => |graph_metric_rerank_query| graph_metric_rerank_query.table_name,
    };
    if (base_table_name.*) |base| {
        if (!std.mem.eql(u8, base, table_name)) return error.UnsupportedSqlShape;
        return;
    }
    base_table_name.* = try alloc.dupe(u8, table_name);
}

pub fn freePlanCtes(alloc: std.mem.Allocator, ctes: []const db_mod.types.RelationalRowsCte) void {
    for (ctes) |cte| {
        var owned = cte;
        owned.deinit(alloc);
    }
    if (ctes.len > 0) alloc.free(ctes);
}

fn setReadPlanAvailableCtes(
    context_hooks: ReadPlanParserContextHooks,
    available_ctes: []const db_mod.types.RelationalRowsCte,
) ReadPlanParserContext {
    const previous_context = context_hooks.get_context(context_hooks.ptr);
    var context = previous_context;
    context.available_ctes = available_ctes;
    context_hooks.set_context(context_hooks.ptr, context);
    return previous_context;
}

fn parseWindowWithCtes(hooks: WindowPlanParserHooks, ctes: []const db_mod.types.RelationalRowsCte) !LoweredWindowPlan {
    const previous_context = setReadPlanAvailableCtes(hooks.context_hooks, ctes);
    defer hooks.context_hooks.set_context(hooks.context_hooks.ptr, previous_context);
    return try hooks.parse_window(hooks.ptr);
}

fn parseAggregateWithCtes(hooks: AggregatePlanParserHooks, ctes: []const db_mod.types.RelationalRowsCte) !LoweredAggregate {
    const previous_context = setReadPlanAvailableCtes(hooks.context_hooks, ctes);
    defer hooks.context_hooks.set_context(hooks.context_hooks.ptr, previous_context);
    return try hooks.parse_aggregate(hooks.ptr);
}

fn parseLateralWithCtes(hooks: LateralPlanParserHooks, ctes: []const db_mod.types.RelationalRowsCte) !LoweredLateralPlan {
    const previous_context = setReadPlanAvailableCtes(hooks.context_hooks, ctes);
    defer hooks.context_hooks.set_context(hooks.context_hooks.ptr, previous_context);
    return try hooks.parse_lateral(hooks.ptr);
}

fn parseJoinWithCtes(hooks: JoinPlanParserHooks, ctes: []const db_mod.types.RelationalRowsCte) !LoweredJoin {
    const previous_context = setReadPlanAvailableCtes(hooks.context_hooks, ctes);
    defer hooks.context_hooks.set_context(hooks.context_hooks.ptr, previous_context);
    return try hooks.parse_join(hooks.ptr);
}

pub fn parseWindowPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: CteSelectParserHooks,
    hooks: WindowPlanParserHooks,
) !LoweredWindowPlan {
    if (!parser.peekKeywordTag(tokens, pos.*, .with)) {
        var lowered = try parseWindowWithCtes(hooks, &.{});
        errdefer lowered.deinit(alloc);
        var base_table_name: ?[]const u8 = null;
        defer if (base_table_name) |table| alloc.free(table);
        if (lowered.plan.ctes.len != 0) try resolveWindowSourceForPlanAlloc(alloc, &lowered, lowered.plan.ctes, &base_table_name);
        if (lowered.plan.ctes.len != 0) {
            const table_name = base_table_name orelse return error.UnsupportedSqlShape;
            base_table_name = null;
            alloc.free(lowered.table_name);
            lowered.table_name = table_name;
        }
        return lowered;
    }

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer freePlanCtes(alloc, ctes);

    var final = try parseWindowWithCtes(hooks, ctes);
    errdefer final.deinit(alloc);
    try resolveWindowSourceForPlanAlloc(alloc, &final, ctes, &base_table_name);
    if (parser.matchToken(tokens, pos, .semicolon) != null and !parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;

    const table_name = base_table_name orelse return error.UnsupportedSqlShape;
    base_table_name = null;
    alloc.free(final.table_name);
    final.table_name = table_name;
    final.plan.ctes = ctes;
    ctes = &.{};
    return final;
}

pub fn parseAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: CteSelectParserHooks,
    hooks: AggregatePlanParserHooks,
) !LoweredAggregatePlan {
    if (!parser.peekKeywordTag(tokens, pos.*, .with)) {
        var lowered = try parseAggregateWithCtes(hooks, &.{});
        errdefer lowered.deinit(alloc);
        var base_table_name: ?[]const u8 = null;
        defer if (base_table_name) |table| alloc.free(table);
        if (lowered.ctes.len != 0) try resolveAggregateSourceForPlanAlloc(alloc, &lowered, lowered.ctes, &base_table_name);
        const table_name = if (lowered.ctes.len != 0) blk: {
            const base = base_table_name orelse return error.UnsupportedSqlShape;
            base_table_name = null;
            alloc.free(lowered.table_name);
            lowered.table_name = "";
            break :blk base;
        } else blk: {
            const table = lowered.table_name;
            lowered.table_name = "";
            break :blk table;
        };
        const ctes = lowered.ctes;
        lowered.ctes = &.{};
        return .{
            .table_name = table_name,
            .plan = .{ .ctes = ctes, .aggregate = lowered.aggregate },
        };
    }

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer freePlanCtes(alloc, ctes);

    var final = try parseAggregateWithCtes(hooks, ctes);
    errdefer final.deinit(alloc);
    try resolveAggregateSourceForPlanAlloc(alloc, &final, ctes, &base_table_name);
    if (parser.matchToken(tokens, pos, .semicolon) != null and !parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;

    const table_name = base_table_name orelse return error.UnsupportedSqlShape;
    base_table_name = null;
    alloc.free(final.table_name);
    final.table_name = "";
    const owned_ctes = ctes;
    ctes = &.{};
    return .{
        .table_name = table_name,
        .plan = .{
            .ctes = owned_ctes,
            .aggregate = final.aggregate,
        },
    };
}

pub fn parseLateralPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: CteSelectParserHooks,
    hooks: LateralPlanParserHooks,
) !LoweredLateralPlan {
    if (!parser.peekKeywordTag(tokens, pos.*, .with)) return try parseLateralWithCtes(hooks, &.{});

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer freePlanCtes(alloc, ctes);

    var final = try parseLateralWithCtes(hooks, ctes);
    errdefer final.deinit(alloc);
    try resolveLateralSourcesForPlanAlloc(alloc, &final, ctes, &base_table_name);
    if (parser.matchToken(tokens, pos, .semicolon) != null and !parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    _ = base_table_name orelse return error.UnsupportedSqlShape;

    final.plan.ctes = ctes;
    ctes = &.{};
    return final;
}

pub fn parseJoinPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: CteSelectParserHooks,
    hooks: JoinPlanParserHooks,
) !LoweredJoin {
    if (!parser.peekKeywordTag(tokens, pos.*, .with)) return try parseJoinWithCtes(hooks, &.{});

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer freePlanCtes(alloc, ctes);

    var final = try parseJoinWithCtes(hooks, ctes);
    errdefer final.deinit(alloc);
    try resolveJoinSourcesForPlanAlloc(alloc, &final, ctes, &base_table_name);
    if (parser.matchToken(tokens, pos, .semicolon) != null and !parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    _ = base_table_name orelse return error.UnsupportedSqlShape;

    final.ctes = ctes;
    ctes = &.{};
    return final;
}

pub fn parseSimpleSelectSetResultTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    lowered: *LoweredSelect,
    hooks: SimpleSelectSetTailHooks,
) !void {
    var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    errdefer {
        freeOrderBy(alloc, order_by.items);
        order_by.deinit(alloc);
    }
    const select = SelectList{
        .fields = lowered.query.select,
        .json_extract = lowered.query.json_extract,
        .array_length = lowered.query.array_length,
        .coalesce = lowered.query.coalesce,
        .field_aliases = lowered.query.field_aliases,
        .expressions = lowered.query.expressions,
        .outputs = lowered.select_outputs,
        .select_all = lowered.query.select_all,
    };

    while (!parser.atEnd(tokens, pos.*)) {
        if (parser.matchKeywordTag(tokens, pos, .order)) {
            if (order_by.items.len > 0) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            try parser.expectKeywordTag(tokens, pos, .by);
            const generated_order_end = try generatedOrderClauseEnd(tokens, keyword_index, pos.*, generated_read_ast);
            try hooks.parse_order_by(hooks.ptr, &order_by, select);
            if (generated_order_end) |end| {
                if (pos.* != end) return error.UnsupportedSqlShape;
            }
        } else if (parser.matchKeywordTag(tokens, pos, .limit)) {
            if (lowered.query.limit != null) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            if (try parseGeneratedLimitValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_limit| {
                lowered.query.limit = generated_limit.value;
            } else {
                lowered.query.limit = try value_mod.parseLimitValue(tokens, pos, params);
            }
        } else if (parser.matchKeywordTag(tokens, pos, .offset)) {
            if (lowered.query.offset != 0) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            lowered.query.offset = if (try parseGeneratedOffsetValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_offset|
                generated_offset
            else
                try value_mod.parseOffsetValue(tokens, pos, params);
        } else if (parser.matchKeywordTag(tokens, pos, .fetch)) {
            if (lowered.query.limit != null) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            if (try parseGeneratedFetchLimitValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_limit| {
                lowered.query.limit = generated_limit.value;
            } else {
                lowered.query.limit = try value_mod.parseFetchLimitValue(tokens, pos, params);
            }
        } else if (parser.matchToken(tokens, pos, .semicolon) != null) {
            if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    if (order_by.items.len > 0) {
        lowered.query.order_by = try order_by.toOwnedSlice(alloc);
    }
}

pub fn parseSetOperationResultTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    lowered: LoweredSelect,
    hooks: SimpleSelectSetTailHooks,
) !SetOperationResultTail {
    var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    errdefer {
        freeOrderBy(alloc, order_by.items);
        order_by.deinit(alloc);
    }
    const select = SelectList{
        .fields = lowered.query.select,
        .json_extract = lowered.query.json_extract,
        .array_length = lowered.query.array_length,
        .coalesce = lowered.query.coalesce,
        .field_aliases = lowered.query.field_aliases,
        .expressions = lowered.query.expressions,
        .outputs = lowered.select_outputs,
        .select_all = lowered.query.select_all,
    };
    var limit: ?u32 = null;
    var offset: u32 = 0;

    while (!parser.atEnd(tokens, pos.*)) {
        if (parser.matchKeywordTag(tokens, pos, .order)) {
            if (order_by.items.len > 0) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            try parser.expectKeywordTag(tokens, pos, .by);
            const generated_order_end = try generatedOrderClauseEnd(tokens, keyword_index, pos.*, generated_read_ast);
            try hooks.parse_order_by(hooks.ptr, &order_by, select);
            if (generated_order_end) |end| {
                if (pos.* != end) return error.UnsupportedSqlShape;
            }
            for (order_by.items) |order| {
                if (order.expression != null) return error.UnsupportedSqlShape;
            }
        } else if (parser.matchKeywordTag(tokens, pos, .limit)) {
            if (limit != null) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            if (try parseGeneratedLimitValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_limit| {
                limit = generated_limit.value;
            } else {
                limit = try value_mod.parseLimitValue(tokens, pos, params);
            }
        } else if (parser.matchKeywordTag(tokens, pos, .offset)) {
            if (offset != 0) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            offset = if (try parseGeneratedOffsetValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_offset|
                generated_offset
            else
                try value_mod.parseOffsetValue(tokens, pos, params);
        } else if (parser.matchKeywordTag(tokens, pos, .fetch)) {
            if (limit != null) return error.UnsupportedSqlShape;
            const keyword_index = pos.* - 1;
            if (try parseGeneratedFetchLimitValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_limit| {
                limit = generated_limit.value;
            } else {
                limit = try value_mod.parseFetchLimitValue(tokens, pos, params);
            }
        } else if (parser.matchToken(tokens, pos, .semicolon) != null) {
            if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    return .{
        .order_by = try order_by.toOwnedSlice(alloc),
        .limit = limit,
        .offset = offset,
    };
}

pub fn parseSetOperationPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    right_schema: runtime_schema.TableSchema,
    allow_distinct_table_names: bool,
    hooks: SetOperationParserHooks,
) !LoweredSetOperationPlan {
    if (parser.peekKeywordTag(tokens, pos.*, .with)) return error.UnsupportedSqlShape;

    var left = try parseSetOperationSelectWithContext(hooks, null, true, null);
    errdefer left.deinit(alloc);
    const left_columns = try hooks.select_output_columns(hooks.ptr, left);
    var left_columns_transferred = false;
    errdefer if (!left_columns_transferred) freeSetOperationOutputColumns(alloc, left_columns);
    if (parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;

    const op = try grammar.parseSelectSetOperation(tokens, pos);
    var right = try parseSetOperationSelectWithContext(hooks, right_schema, null, true);
    errdefer right.deinit(alloc);
    const right_columns = try hooks.select_output_columns(hooks.ptr, right);
    defer freeSetOperationOutputColumns(alloc, right_columns);

    const tail = try hooks.parse_result_tail(hooks.ptr, left);
    var tail_transferred = false;
    errdefer if (!tail_transferred) {
        freeOrderBy(alloc, tail.order_by);
        if (tail.order_by.len > 0) alloc.free(tail.order_by);
    };
    if (!std.mem.eql(u8, left.table_name, right.table_name) and !allow_distinct_table_names) return error.UnsupportedSqlShape;
    if (!reconcileSetOperationOutputColumns(left_columns, right_columns)) return error.UnsupportedSqlShape;

    const left_table_name = left.table_name;
    left.table_name = "";
    const right_table_name = right.table_name;
    right.table_name = "";
    left.clearSelectOutputs(alloc);
    right.clearSelectOutputs(alloc);

    var left_plan = LoweredQueryPlan{
        .table_name = left_table_name,
        .plan = .{ .query = left.query },
    };
    left.query = .{};
    errdefer left_plan.deinit(alloc);

    var right_plan = LoweredQueryPlan{
        .table_name = right_table_name,
        .plan = .{ .query = right.query },
    };
    right.query = .{};
    errdefer right_plan.deinit(alloc);

    left_columns_transferred = true;
    tail_transferred = true;
    return .{
        .operation = op,
        .left = left_plan,
        .right = right_plan,
        .output_columns = left_columns,
        .order_by = tail.order_by,
        .limit = tail.limit,
        .offset = tail.offset,
        .max_rows = db_mod.types.default_relational_rows_cte_max_rows,
        .max_bytes = db_mod.types.default_relational_rows_cte_max_bytes,
        .spill_after_bytes = db_mod.types.default_relational_rows_cte_spill_after_bytes,
    };
}

fn parseSetOperationSelectWithContext(
    hooks: SetOperationParserHooks,
    schema: ?runtime_schema.TableSchema,
    allow_select_set_boundary: ?bool,
    allow_select_set_result_tail_boundary: ?bool,
) !LoweredSelect {
    const previous_context = hooks.context_hooks.get_context(hooks.context_hooks.ptr);
    var context = previous_context;
    if (schema) |value| context.schema = value;
    if (allow_select_set_boundary) |value| context.allow_select_set_boundary = value;
    if (allow_select_set_result_tail_boundary) |value| context.allow_select_set_result_tail_boundary = value;
    hooks.context_hooks.set_context(hooks.context_hooks.ptr, context);
    defer hooks.context_hooks.set_context(hooks.context_hooks.ptr, previous_context);

    return try hooks.parse_select(hooks.ptr);
}

fn reconcileSetOperationOutputColumns(
    lhs: []runtime_schema.RelationalColumn,
    rhs: []const runtime_schema.RelationalColumn,
) bool {
    if (lhs.len == 0 or lhs.len != rhs.len) return false;
    for (lhs, rhs) |*left, right| {
        const common = setOperationCommonColumnType(left.*, right) orelse return false;
        left.field_type = common.field_type;
        left.array_item_type = common.array_item_type;
        left.nullable = left.nullable or right.nullable;
    }
    return true;
}

const SetOperationCommonColumnType = struct {
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
};

fn setOperationCommonColumnType(
    left: runtime_schema.RelationalColumn,
    right: runtime_schema.RelationalColumn,
) ?SetOperationCommonColumnType {
    if (left.field_type == .array or right.field_type == .array) {
        if (left.field_type != .array or right.field_type != .array) return null;
        const left_item = left.array_item_type orelse .json;
        const right_item = right.array_item_type orelse .json;
        if (!setOperationScalarTypesCompatible(left_item, right_item)) return null;
        return .{ .field_type = .array, .array_item_type = left.array_item_type orelse right.array_item_type };
    }
    if (!setOperationScalarTypesCompatible(left.field_type, right.field_type)) return null;
    return .{ .field_type = left.field_type };
}

fn setOperationScalarTypesCompatible(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    if (setOperationTypeIsTextLike(lhs) and setOperationTypeIsTextLike(rhs)) return true;
    if ((lhs == .datetime and rhs == .numeric) or (lhs == .numeric and rhs == .datetime)) return true;
    return lhs == rhs;
}

fn setOperationTypeIsTextLike(field_type: runtime_schema.AntflyType) bool {
    return switch (field_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
        else => false,
    };
}

pub fn parseRecursiveCteMemberPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    available_ctes: []const db_mod.types.RelationalRowsCte,
    cte_name: []const u8,
    hooks: RecursiveCteMemberParserHooks,
) !LoweredRecursiveCteMemberPlan {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeywordTag(.select);
    const projection_start = pos.*;
    const from_index = parser.findTopLevelKeywordTagFromIndex(tokens, projection_start, .from) orelse return error.UnsupportedSqlShape;
    pos.* = from_index;

    try cursor.expectKeywordTag(.from);
    const left_table = try parseTableAliasAlloc(alloc, tokens, pos);
    defer freeTableAlias(alloc, left_table);

    const join_type: db_mod.types.RelationalRowsJoinType = if (parser.matchKeywordTag(tokens, pos, .left)) blk: {
        _ = parser.matchKeywordTag(tokens, pos, .outer);
        try cursor.expectKeywordTag(.join);
        break :blk .left;
    } else blk: {
        _ = parser.matchKeywordTag(tokens, pos, .inner);
        try cursor.expectKeywordTag(.join);
        break :blk .inner;
    };
    const right_table = try parseTableAliasAlloc(alloc, tokens, pos);
    defer freeTableAlias(alloc, right_table);
    if (std.mem.eql(u8, left_table.alias, right_table.alias)) return error.UnsupportedSqlShape;

    const left_is_cte = std.mem.eql(u8, left_table.name, cte_name);
    const right_is_cte = std.mem.eql(u8, right_table.name, cte_name);
    if (left_is_cte == right_is_cte) return error.UnsupportedSqlShape;
    if (join_type != .inner) return error.UnsupportedSqlShape;
    const base_table = if (left_is_cte) right_table else left_table;
    const recursive_table = if (left_is_cte) left_table else right_table;

    const planned_ctes = try relational_rows.planRowsCteOutputsAlloc(alloc, schema, available_ctes);
    defer relational_rows.freeRowsPlannedCtes(alloc, planned_ctes);
    const cte_schema = relational_rows.rowsPlannedCteSchema(planned_ctes, cte_name) orelse return error.UnsupportedSqlShape;
    const target_qualifiers = [_][]const u8{ base_table.name, base_table.alias };
    const source_qualifiers = [_][]const u8{ recursive_table.name, recursive_table.alias };
    const projections = try parseRecursiveCteMemberProjectionsAlloc(
        alloc,
        tokens[projection_start..from_index],
        cte_schema,
        target_qualifiers[0..],
        source_qualifiers[0..],
        hooks,
    );
    errdefer freeExpressionProjections(alloc, projections);

    try cursor.expectKeywordTag(.on);
    const on = try hooks.parse_join_on(
        hooks.ptr,
        tokens,
        pos,
        cte_schema,
        left_is_cte,
        right_is_cte,
        join_type,
        left_table.alias,
        right_table.alias,
    );
    errdefer {
        freeJoinOn(alloc, on);
        if (on.len > 0) alloc.free(on);
    }

    const left_table_name = try alloc.dupe(u8, left_table.name);
    errdefer alloc.free(left_table_name);
    const right_table_name = try alloc.dupe(u8, right_table.name);
    errdefer alloc.free(right_table_name);
    return .{ .join = .{
        .left_table_name = left_table_name,
        .right_table_name = right_table_name,
        .join_type = join_type,
        .on = on,
        .projections = projections,
    } };
}

pub fn parseRecursiveCteMemberProjectionsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    cte_schema: runtime_schema.TableSchema,
    target_qualifiers: []const []const u8,
    source_qualifiers: []const []const u8,
    hooks: RecursiveCteMemberParserHooks,
) ![]const db_mod.types.RelationalRowsExpressionProjection {
    var pos: usize = 0;
    var projections = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    errdefer {
        freeExpressionProjections(alloc, projections.items);
        projections.deinit(alloc);
    }
    while (true) {
        const parsed = try hooks.parse_projection_expression(
            hooks.ptr,
            tokens,
            &pos,
            cte_schema,
            target_qualifiers,
            source_qualifiers,
        );
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, parsed.expression);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, &pos, parsed.default_output);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        try projections.append(alloc, .{ .output = output, .expression = parsed.expression });
        expression_transferred = true;
        output_transferred = true;
        if (parser.matchToken(tokens, &pos, .comma) == null) break;
    }
    if (!parser.atEnd(tokens, pos)) return error.UnsupportedSqlShape;
    return try projections.toOwnedSlice(alloc);
}

pub fn parseRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: RecursiveCteParserHooks,
) !LoweredRecursiveCtePlan {
    var recursive = try parseRecursiveCteProducerAlloc(alloc, tokens, pos, hooks);
    errdefer recursive.deinit(alloc);

    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;

    if (!cursor.peekKeywordTag(.select)) return error.UnsupportedSqlShape;
    if (!tokensContainTopLevelSourceName(tokens[pos.*..], recursive.cte_name)) return error.UnsupportedSqlShape;

    const final_ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = recursive.cte_name,
        .query = recursive.anchor.plan.query,
    }};
    var final = try hooks.parse_select_with_set_boundary(hooks.ptr, tokens, pos, final_ctes[0..]);
    errdefer final.deinit(alloc);
    var final_base_table_name: ?[]const u8 = null;
    defer if (final_base_table_name) |name| alloc.free(name);
    try resolveSelectSourceForPlanAlloc(alloc, &final, final_ctes[0..], &final_base_table_name);
    if (!std.mem.eql(u8, final.query.source_cte, recursive.cte_name)) return error.UnsupportedSqlShape;
    if (cursor.matchToken(.semicolon) != null and pos.* != tokens.len) return error.UnsupportedSqlShape;
    if (pos.* != tokens.len) return error.UnsupportedSqlShape;

    var final_query = final.query;
    final.query = .{};
    final.clearSelectOutputs(alloc);
    alloc.free(final.table_name);
    final.table_name = "";
    errdefer final_query.deinit(alloc);

    recursive.final_query = final_query;
    return recursive;
}

pub fn parseRecursiveCteProducerAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: RecursiveCteParserHooks,
) !LoweredRecursiveCtePlan {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeywordTag(.with);
    try cursor.expectKeywordTag(.recursive);

    const cte_name = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var cte_name_transferred = false;
    errdefer if (!cte_name_transferred) alloc.free(cte_name);

    const cte_column_aliases = try grammar.parseOptionalCteColumnAliasesAlloc(alloc, tokens, pos);
    defer strings.freeStringSlice(alloc, cte_column_aliases);
    try cursor.expectKeywordTag(.as);
    try parser.consumeCteMaterializationHint(tokens, pos);
    try cursor.expectToken(.lparen);

    const close_index = (parser.findMatchingRParenAfterOpenIndex(tokens, pos.*) orelse return error.UnsupportedSqlShape);
    const body_tokens = tokens[pos.*..close_index];
    var body_pos: usize = 0;
    var anchor = try hooks.parse_select_with_set_boundary(hooks.ptr, body_tokens, &body_pos, &.{});
    errdefer anchor.deinit(alloc);
    const output_columns = try hooks.select_output_columns(hooks.ptr, anchor);
    var output_columns_transferred = false;
    errdefer if (!output_columns_transferred) freeSetOperationOutputColumns(alloc, output_columns);
    try applyRecursiveCteOutputColumnAliasesAlloc(alloc, output_columns, cte_column_aliases);
    try applyCteColumnAliasesAlloc(alloc, &anchor, cte_column_aliases);
    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |name| alloc.free(name);
    try resolveSelectSourceForPlanAlloc(alloc, &anchor, &.{}, &base_table_name);
    if (body_pos >= body_tokens.len) return error.UnsupportedSqlShape;
    const operation = try grammar.parseSelectSetOperation(body_tokens, &body_pos);
    switch (operation) {
        .union_all, .union_distinct => {},
        .intersect, .except => return error.UnsupportedSqlShape,
    }
    const recursive_member_references_cte = tokensContainTopLevelSourceName(body_tokens[body_pos..], cte_name);
    if (!recursive_member_references_cte) return error.UnsupportedSqlShape;

    const recursive_member_ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = cte_name,
        .query = anchor.query,
    }};
    var recursive_member = try hooks.parse_recursive_member(hooks.ptr, body_tokens, &body_pos, recursive_member_ctes[0..], cte_name);
    errdefer recursive_member.deinit(alloc);
    try applyRecursiveCteMemberOutputColumnsAlloc(alloc, &recursive_member, output_columns);
    if (body_pos != body_tokens.len) return error.UnsupportedSqlShape;

    pos.* = close_index + 1;

    const anchor_table_name = anchor.table_name;
    anchor.table_name = "";
    anchor.clearSelectOutputs(alloc);
    var anchor_plan = LoweredQueryPlan{
        .table_name = anchor_table_name,
        .plan = .{ .query = anchor.query },
    };
    anchor.query = .{};
    errdefer anchor_plan.deinit(alloc);

    cte_name_transferred = true;
    output_columns_transferred = true;
    return .{
        .cte_name = cte_name,
        .operation = operation,
        .anchor = anchor_plan,
        .recursive_member = recursive_member,
        .final_query = .{},
        .output_columns = output_columns,
        .recursive_member_references_cte = recursive_member_references_cte,
        .max_rows = db_mod.types.default_relational_rows_cte_max_rows,
        .max_bytes = db_mod.types.default_relational_rows_cte_max_bytes,
        .spill_after_bytes = db_mod.types.default_relational_rows_cte_spill_after_bytes,
    };
}

fn applyRecursiveCteOutputColumnAliasesAlloc(
    alloc: std.mem.Allocator,
    columns: []const runtime_schema.RelationalColumn,
    aliases: []const []const u8,
) !void {
    if (aliases.len == 0) return;
    if (aliases.len != columns.len) return error.UnsupportedSqlShape;
    for (columns, aliases) |*column_const, alias| {
        const column: *runtime_schema.RelationalColumn = @constCast(column_const);
        const owned_alias = try alloc.dupe(u8, alias);
        errdefer alloc.free(owned_alias);
        alloc.free(column.name);
        column.name = owned_alias;
    }
}

fn applyRecursiveCteMemberOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    member: *LoweredRecursiveCteMemberPlan,
    output_columns: []const runtime_schema.RelationalColumn,
) !void {
    switch (member.*) {
        .join => |*join| {
            if (join.projections.len != output_columns.len) return error.UnsupportedSqlShape;
            const projections = @constCast(join.projections);
            for (projections, output_columns) |*projection, column| {
                const output = try alloc.dupe(u8, column.name);
                alloc.free(@constCast(projection.output));
                projection.output = output;
            }
        },
    }
}

fn tokensContainTopLevelSourceName(tokens: []const Token, name: []const u8) bool {
    var depth: usize = 0;
    var previous_source_keyword = false;
    for (tokens) |token| {
        switch (token.kind) {
            .lparen => {
                depth += 1;
                previous_source_keyword = false;
            },
            .rparen => {
                if (depth > 0) depth -= 1;
                previous_source_keyword = false;
            },
            .identifier => {
                if (depth == 0 and previous_source_keyword and std.ascii.eqlIgnoreCase(token.text, name)) return true;
                previous_source_keyword = depth == 0 and (token.matchesKeywordTag(.from) or token.matchesKeywordTag(.join));
            },
            else => previous_source_keyword = false,
        }
    }
    return false;
}

pub fn resolveSelectSourceForPlanAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredSelect,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: *?[]const u8,
) !void {
    if (findCteByName(ctes, lowered.table_name)) |cte| {
        lowered.query.source_cte = try alloc.dupe(u8, lowered.table_name);
        if (cte.table_function) |table_function| try resolveTableFunctionBaseSourceTableAlloc(alloc, table_function, base_table_name);
        return;
    }
    try resolveBaseSourceTableAlloc(alloc, lowered.table_name, base_table_name);
}

pub fn resolveAggregateSourceForPlanAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredAggregate,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: *?[]const u8,
) !void {
    if (findCteByName(ctes, lowered.table_name)) |cte| {
        lowered.aggregate.source.source_cte = try alloc.dupe(u8, lowered.table_name);
        if (cte.table_function) |table_function| try resolveTableFunctionBaseSourceTableAlloc(alloc, table_function, base_table_name);
        return;
    }
    try resolveBaseSourceTableAlloc(alloc, lowered.table_name, base_table_name);
}

pub fn resolveWindowSourceForPlanAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredWindowPlan,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: *?[]const u8,
) !void {
    if (findCteByName(ctes, lowered.table_name)) |cte| {
        lowered.plan.window.source.source_cte = try alloc.dupe(u8, lowered.table_name);
        if (cte.table_function) |table_function| try resolveTableFunctionBaseSourceTableAlloc(alloc, table_function, base_table_name);
        return;
    }
    try resolveBaseSourceTableAlloc(alloc, lowered.table_name, base_table_name);
}

pub fn resolveJoinSourcesForPlanAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredJoin,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: *?[]const u8,
) !void {
    try resolveJoinSideSourceForPlanAlloc(alloc, &lowered.join.left, &lowered.left_table_name, ctes, base_table_name);
    try resolveJoinSideSourceForPlanAlloc(alloc, &lowered.join.right, &lowered.right_table_name, ctes, base_table_name);
}

pub fn resolveLateralSourcesForPlanAlloc(
    alloc: std.mem.Allocator,
    lowered: *LoweredLateralPlan,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: *?[]const u8,
) !void {
    try resolveJoinSideSourceForPlanAlloc(alloc, &lowered.plan.lateral.left, &lowered.left_table_name, ctes, base_table_name);
    try resolveJoinSideSourceForPlanAlloc(alloc, &lowered.plan.lateral.right, &lowered.right_table_name, ctes, base_table_name);
    if (lowered.plan.left_table.len > 0) alloc.free(lowered.plan.left_table);
    lowered.plan.left_table = try alloc.dupe(u8, lowered.left_table_name);
    if (lowered.plan.right_table.len > 0) alloc.free(lowered.plan.right_table);
    lowered.plan.right_table = try alloc.dupe(u8, lowered.right_table_name);
}

fn resolveJoinSideSourceForPlanAlloc(
    alloc: std.mem.Allocator,
    source: *db_mod.types.RelationalRowsQueryRequest,
    table_name: *[]const u8,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: *?[]const u8,
) !void {
    if (findCteByName(ctes, table_name.*)) |cte| {
        source.source_cte = try alloc.dupe(u8, table_name.*);
        if (cte.table_function) |table_function| try resolveTableFunctionBaseSourceTableAlloc(alloc, table_function, base_table_name);
        const base = base_table_name.* orelse return error.UnsupportedSqlShape;
        const physical_table_name = try alloc.dupe(u8, base);
        alloc.free(table_name.*);
        table_name.* = physical_table_name;
        return;
    }
    try resolveBaseSourceTableAlloc(alloc, table_name.*, base_table_name);
}

fn resolveBaseSourceTableAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    base_table_name: *?[]const u8,
) !void {
    if (base_table_name.*) |base| {
        if (!std.mem.eql(u8, base, table_name)) return error.UnsupportedSqlShape;
    } else {
        base_table_name.* = try alloc.dupe(u8, table_name);
    }
}

pub const LateralSubquery = struct {
    table: TableAlias,
    output_columns: []const runtime_schema.RelationalColumn = &.{},
    predicates: []const runtime_schema.RelationalCheck = &.{},
    json_contains: []const db_mod.types.RelationalRowsJsonContainsPredicate = &.{},
    json_path_exists: []const db_mod.types.RelationalRowsJsonPathExistsPredicate = &.{},
    array_contains: []const db_mod.types.RelationalRowsArrayContainsPredicate = &.{},
    array_eq: []const db_mod.types.RelationalRowsArrayEqPredicate = &.{},
    in_predicates: []const db_mod.types.RelationalRowsInPredicate = &.{},
    text_patterns: []const db_mod.types.RelationalRowsTextPatternPredicate = &.{},
    expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    expression_or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    expression_not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},
    match_expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    match_expression_or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},
    correlations: []const db_mod.types.RelationalRowsLateralCorrelation = &.{},
    order_by: []const db_mod.types.RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,
};

pub const LoweredReadPlan = union(enum) {
    query: LoweredQueryPlan,
    document_query: document_plan.DocumentReadPlan,
    document_aggregate: document_plan.DocumentAlgebraicAggregatePlan,
    set_operation: LoweredSetOperationPlan,
    recursive_cte: LoweredRecursiveCtePlan,
    aggregate: LoweredAggregatePlan,
    join: LoweredJoin,
    lateral: LoweredLateralPlan,
    window: LoweredWindowPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query => |*query| query.deinit(alloc),
            .document_query => |*document_query| document_query.deinit(alloc),
            .document_aggregate => |*document_aggregate| document_aggregate.deinit(alloc),
            .set_operation => |*set_operation| set_operation.deinit(alloc),
            .recursive_cte => |*recursive_cte| recursive_cte.deinit(alloc),
            .aggregate => |*aggregate| aggregate.deinit(alloc),
            .join => |*join| join.deinit(alloc),
            .lateral => |*lateral| lateral.deinit(alloc),
            .window => |*window| window.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const ReadPlanLoweringHooks = struct {
    ptr: *anyopaque,
    statement_kind: ?sql_statement_kind.SqlReadStatementKind = null,
    lower_lateral: *const fn (*anyopaque) anyerror!LoweredLateralPlan,
    lower_window: *const fn (*anyopaque) anyerror!LoweredWindowPlan,
    lower_aggregate: *const fn (*anyopaque) anyerror!LoweredAggregatePlan,
    lower_recursive_cte: *const fn (*anyopaque) anyerror!LoweredRecursiveCtePlan,
    lower_join: *const fn (*anyopaque) anyerror!LoweredJoin,
    lower_query: *const fn (*anyopaque) anyerror!LoweredQueryPlan,
    lower_set_operation: *const fn (*anyopaque) anyerror!LoweredSetOperationPlan,
};

fn lowerReadPlanKindWithHooks(hooks: ReadPlanLoweringHooks, kind: sql_statement_kind.SqlReadStatementKind) !LoweredReadPlan {
    return switch (kind) {
        .lateral => .{ .lateral = try hooks.lower_lateral(hooks.ptr) },
        .window => .{ .window = try hooks.lower_window(hooks.ptr) },
        .aggregate => .{ .aggregate = try hooks.lower_aggregate(hooks.ptr) },
        .recursive_cte => .{ .recursive_cte = try hooks.lower_recursive_cte(hooks.ptr) },
        .join => .{ .join = try hooks.lower_join(hooks.ptr) },
        .query => .{ .query = try hooks.lower_query(hooks.ptr) },
        .set_operation => blk: {
            // Some set-operation syntax has a proven equivalent row-query plan
            // (for example disjoint UNION ALL predicates). Keep that optimizer
            // scoped to the parsed set-operation variant; unproved shapes use
            // the native set-operation plan below.
            if (hooks.lower_query(hooks.ptr)) |lowered| {
                break :blk .{ .query = lowered };
            } else |err| switch (err) {
                error.UnsupportedSqlShape => {},
                else => return err,
            }
            break :blk .{ .set_operation = try hooks.lower_set_operation(hooks.ptr) };
        },
    };
}

pub fn lowerReadPlanWithHooks(hooks: ReadPlanLoweringHooks) !LoweredReadPlan {
    if (hooks.statement_kind) |kind| {
        return try lowerReadPlanKindWithHooks(hooks, kind);
    }

    return error.UnsupportedSqlShape;
}

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

pub fn relationPopulationPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.RelationPopulationSyntax,
    source: *LoweredReadPlan,
) !LoweredRelationPopulationPlan {
    const target = try grammar.normalizeSqlObjectIdentifierAlloc(alloc, syntax.target_identifier);
    var target_transferred = false;
    errdefer if (!target_transferred) alloc.free(target);

    target_transferred = true;
    const out = LoweredRelationPopulationPlan{
        .mode = syntax.mode,
        .target_table_name = target,
        .target_lifetime = syntax.target_lifetime,
        .if_not_exists = syntax.if_not_exists,
        .populate = syntax.populate,
        .source = source.*,
    };
    source.* = undefined;
    return out;
}

pub const RelationPopulationLoweringHooks = struct {
    ptr: *anyopaque,
    lower_read: *const fn (*anyopaque, *const tokenized.ParsedSql) anyerror!LoweredReadPlan,
};

pub fn lowerRelationPopulationPlanWithParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    hooks: RelationPopulationLoweringHooks,
) !LoweredRelationPopulationPlan {
    var parsed = try grammar.parseRelationPopulationParsedSqlAlloc(alloc, parsed_sql);
    defer parsed.deinit(alloc);
    var parsed_source = try relationPopulationSourceParsedSqlAlloc(alloc, parsed_sql, parsed);
    defer parsed_source.deinit(alloc);
    var source = try hooks.lower_read(hooks.ptr, &parsed_source.parsed);
    errdefer source.deinit(alloc);
    return try relationPopulationPlanFromSyntaxAlloc(alloc, parsed, &source);
}

fn relationPopulationSourceParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parent_sql: *const tokenized.ParsedSql,
    parsed: grammar.RelationPopulationSyntax,
) !tokenized.OwnedParsedSql {
    if (parsed.source_token_start) |start| {
        const end = parsed.source_token_end orelse return error.UnsupportedSqlShape;
        if (parsed.source_suffix_token_start) |suffix_start| {
            const suffix_end = parsed.source_suffix_token_end orelse return error.UnsupportedSqlShape;
            const ranges = [_]tokenized.TokenRange{
                .{ .start = start, .end = end },
                .{ .start = suffix_start, .end = suffix_end },
            };
            return try tokenized.OwnedParsedSql.initChildStatementFromTokenRangesAlloc(alloc, parent_sql, &ranges);
        }
        return try tokenized.OwnedParsedSql.initChildStatementAlloc(alloc, parent_sql, start, end);
    }
    return error.UnsupportedSqlShape;
}

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

pub const ExplainPlanLoweringHooks = struct {
    ptr: *anyopaque,
    lower_read: *const fn (*anyopaque, *const tokenized.ParsedSql) anyerror!LoweredReadPlan,
    lower_write: *const fn (*anyopaque, *const tokenized.ParsedSql) anyerror!LoweredWritePlan,
};

pub fn lowerExplainPlanWithParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    hooks: ExplainPlanLoweringHooks,
) !LoweredExplainPlan {
    const parsed = switch (parsed_sql.statement) {
        .explain => |statement| statement,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedExplainUnsupportedBoundary(parsed_sql, parsed);
    return try lowerExplainPlanWithParsedPrefixAlloc(alloc, parsed_sql, parsed, hooks);
}

fn generatedStatementTokenEnd(tokens: []const Token) usize {
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) : (end -= 1) {}
    return end;
}

fn validateGeneratedExplainUnsupportedBoundary(
    parsed_sql: *const tokenized.ParsedSql,
    parsed: tokenized.ParsedExplainStatement,
) !void {
    const generated_statement = parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const generated_ast = generated_statement.ast orelse return error.UnsupportedSqlShape;
    const unsupported = switch (generated_ast) {
        .unsupported => |unsupported| unsupported,
        else => return error.UnsupportedSqlShape,
    };
    switch (generated_statement.statement) {
        .unsupported => |kind| if (kind != .explain) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
    if (unsupported.kind != .explain or unsupported.reason != .explain_not_planned_by_generated_parser) {
        return error.UnsupportedSqlShape;
    }
    const tokens = parsed_sql.items();
    const end = generatedStatementTokenEnd(tokens);
    if (end == 0 or !tokens[0].matchesKeyword("explain")) return error.UnsupportedSqlShape;
    if (unsupported.statement_span.start != tokens[0].source_start or unsupported.statement_span.end != tokens[end - 1].source_end) {
        return error.UnsupportedSqlShape;
    }
    if (unsupported.command_span.start != tokens[0].source_start or unsupported.command_span.end != tokens[0].source_end) {
        return error.UnsupportedSqlShape;
    }
    const inner_start = parsed.inner_token_start orelse return error.UnsupportedSqlShape;
    const inner_end = parsed.inner_token_end orelse return error.UnsupportedSqlShape;
    if (!std.meta.eql(unsupported.subject_tokens orelse return error.UnsupportedSqlShape, generated_parser.GeneratedSqlTokenRange{ .start = inner_start, .end = inner_end })) {
        return error.UnsupportedSqlShape;
    }
}

fn lowerExplainPlanWithParsedPrefixAlloc(
    alloc: std.mem.Allocator,
    parent_sql: *const tokenized.ParsedSql,
    parsed: tokenized.ParsedExplainStatement,
    hooks: ExplainPlanLoweringHooks,
) !LoweredExplainPlan {
    const inner_start = parsed.inner_token_start orelse return error.UnsupportedSqlShape;
    const inner_end = parsed.inner_token_end orelse return error.UnsupportedSqlShape;
    var inner_sql = try tokenized.OwnedParsedSql.initChildStatementAlloc(
        alloc,
        parent_sql,
        inner_start,
        inner_end,
    );
    defer inner_sql.deinit(alloc);

    const subject: LoweredExplainSubject = switch (inner_sql.parsed.statement) {
        .read => .{ .read = try hooks.lower_read(hooks.ptr, &inner_sql.parsed) },
        .write => .{ .write = try hooks.lower_write(hooks.ptr, &inner_sql.parsed) },
        else => return error.UnsupportedSqlShape,
    };
    return .{
        .analyze = parsed.analyze,
        .format = parsed.format,
        .verbose = parsed.verbose,
        .costs = parsed.costs,
        .buffers = parsed.buffers,
        .timing = parsed.timing,
        .summary = parsed.summary,
        .settings = parsed.settings,
        .wal = parsed.wal,
        .subject = subject,
    };
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn lowerExplainReadForTest(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!LoweredReadPlan {
    const alloc: *std.mem.Allocator = @ptrCast(@alignCast(ptr));
    switch (parsed_sql.statement) {
        .read => {},
        else => return error.UnsupportedSqlShape,
    }
    return .{ .query = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .plan = .{},
    } };
}

fn lowerExplainWriteForTest(_: *anyopaque, _: *const tokenized.ParsedSql) anyerror!LoweredWritePlan {
    return error.UnsupportedSqlShape;
}

fn explainPlanLoweringHooksForTest(alloc: *std.mem.Allocator) ExplainPlanLoweringHooks {
    return .{
        .ptr = alloc,
        .lower_read = lowerExplainReadForTest,
        .lower_write = lowerExplainWriteForTest,
    };
}

test "sql adapter explain validates generated unsupported boundary before lowering subject" {
    const alloc = std.testing.allocator;

    var parsed = try tokenized.ParsedSql.initAlloc(
        alloc,
        "EXPLAIN SELECT id FROM usage_records;",
    );
    defer parsed.deinit(alloc);
    var mutable_alloc = alloc;
    var lowered = try lowerExplainPlanWithParsedSqlAlloc(alloc, &parsed, explainPlanLoweringHooksForTest(&mutable_alloc));
    defer lowered.deinit(alloc);
    try std.testing.expect(!lowered.analyze);
    switch (lowered.subject) {
        .read => |read| switch (read) {
            .query => |query| try std.testing.expectEqualStrings("usage_records", query.table_name),
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }

    var malformed_kind = try tokenized.ParsedSql.initAlloc(
        alloc,
        "EXPLAIN SELECT id FROM usage_records;",
    );
    defer malformed_kind.deinit(alloc);
    if (malformed_kind.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            switch (generated_ast.*) {
                .unsupported => |*unsupported| unsupported.kind = .copy,
                else => return error.TestUnexpectedResult,
            }
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, lowerExplainPlanWithParsedSqlAlloc(alloc, &malformed_kind, explainPlanLoweringHooksForTest(&mutable_alloc)));

    var malformed_subject = try tokenized.ParsedSql.initAlloc(
        alloc,
        "EXPLAIN SELECT id FROM usage_records;",
    );
    defer malformed_subject.deinit(alloc);
    if (malformed_subject.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            switch (generated_ast.*) {
                .unsupported => |*unsupported| unsupported.subject_tokens = .{ .start = 0, .end = malformed_subject.items().len },
                else => return error.TestUnexpectedResult,
            }
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, lowerExplainPlanWithParsedSqlAlloc(alloc, &malformed_subject, explainPlanLoweringHooksForTest(&mutable_alloc)));
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

pub fn cloneNamedWindowDefinitionAlloc(
    alloc: std.mem.Allocator,
    value: NamedWindowSpec,
) !NamedWindowDefinition {
    const partition_by = try strings.cloneStringSlice(alloc, value.partition_by);
    var partition_transferred = false;
    errdefer if (!partition_transferred) strings.freeStringSlice(alloc, partition_by);
    const order_by = try cloneOrderByAlloc(alloc, value.order_by);
    var order_transferred = false;
    errdefer if (!order_transferred) {
        freeOrderBy(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    };

    partition_transferred = true;
    order_transferred = true;
    return .{
        .partition_by = partition_by,
        .order_by = order_by,
        .frame = value.frame,
    };
}

pub fn freeNamedWindowDefinition(alloc: std.mem.Allocator, value: NamedWindowDefinition) void {
    strings.freeStringSlice(alloc, value.partition_by);
    freeOrderBy(alloc, value.order_by);
    if (value.order_by.len > 0) alloc.free(value.order_by);
}

pub fn freeNamedWindowSpec(alloc: std.mem.Allocator, value: NamedWindowSpec) void {
    alloc.free(value.name);
    strings.freeStringSlice(alloc, value.partition_by);
    freeOrderBy(alloc, value.order_by);
    if (value.order_by.len > 0) alloc.free(value.order_by);
}

pub fn freeNamedWindowSpecs(alloc: std.mem.Allocator, values: []const NamedWindowSpec) void {
    for (values) |value| freeNamedWindowSpec(alloc, value);
    if (values.len > 0) alloc.free(values);
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

pub fn freeExpressionAssignments(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionAssignment) void {
    for (values) |value| {
        alloc.free(@constCast(value.field));
        freeExpression(alloc, value.expression);
    }
    if (values.len > 0) alloc.free(values);
}

pub fn freeRowsJsonSetExpressionAssignments(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonSetExpressionAssignment) void {
    for (values) |value| {
        alloc.free(@constCast(value.field));
        for (value.path) |segment| alloc.free(@constCast(segment));
        if (value.path.len > 0) alloc.free(value.path);
        freeExpression(alloc, value.expression);
    }
    if (values.len > 0) alloc.free(values);
}

pub fn freeJsonExtract(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonExtractProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
        alloc.free(value.path);
    }
    if (values.len > 0) alloc.free(values);
}

pub fn freeArrayLengthProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayLengthProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
    if (values.len > 0) alloc.free(values);
}

pub fn freeFieldAliasProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsFieldAliasProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
    if (values.len > 0) alloc.free(values);
}

pub fn freeCoalesceOperand(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsCoalesceOperand) void {
    switch (value.kind) {
        .field => if (value.field.len > 0) alloc.free(value.field),
        .value => if (value.value_json.len > 0) alloc.free(value.value_json),
    }
}

pub fn freeCoalesceProjection(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsCoalesceProjection) void {
    alloc.free(value.output);
    for (value.operands) |operand| freeCoalesceOperand(alloc, operand);
    if (value.operands.len > 0) alloc.free(value.operands);
}

pub fn freeCoalesceProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsCoalesceProjection) void {
    for (values) |value| freeCoalesceProjection(alloc, value);
    if (values.len > 0) alloc.free(values);
}

pub fn buildCoalesceProjectionFromOperandListAlloc(
    alloc: std.mem.Allocator,
    output: []const u8,
    operands: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceOperand),
) !db_mod.types.RelationalRowsCoalesceProjection {
    const owned_operands = try operands.toOwnedSlice(alloc);
    operands.* = .empty;
    return .{
        .output = output,
        .operands = owned_operands,
    };
}

pub fn expressionProjectionFromCoalesceAlloc(
    alloc: std.mem.Allocator,
    projection: db_mod.types.RelationalRowsCoalesceProjection,
) !db_mod.types.RelationalRowsExpressionProjection {
    const output = try alloc.dupe(u8, projection.output);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, projection.operands.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }
    for (projection.operands) |operand| {
        operands[initialized] = switch (operand.kind) {
            .field => .{
                .kind = .field,
                .field = try alloc.dupe(u8, operand.field),
            },
            .value => .{
                .kind = .value,
                .value_json = try alloc.dupe(u8, operand.value_json),
            },
        };
        initialized += 1;
    }

    output_transferred = true;
    return .{
        .output = output,
        .expression = .{
            .kind = .coalesce,
            .operands = operands,
        },
    };
}

pub fn freeJoinOn(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJoinOn) void {
    for (values) |value| {
        alloc.free(value.left_field);
        alloc.free(value.right_field);
    }
}

pub fn freeJoinProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJoinProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
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

pub fn freeAggregateSpec(alloc: std.mem.Allocator, spec: db_mod.types.RelationalRowsAggregateSpec) void {
    alloc.free(spec.name);
    if (spec.field) |field| alloc.free(field);
    if (spec.expression) |expression| freeExpression(alloc, expression);
    if (spec.percentiles.len > 0) alloc.free(spec.percentiles);
    if (spec.string_delimiter) |delimiter| alloc.free(delimiter);
    freeOrderBy(alloc, spec.array_order_by);
    if (spec.array_order_by.len > 0) alloc.free(spec.array_order_by);
    freeRelationalChecks(alloc, spec.filter_predicates);
    if (spec.filter_predicates.len > 0) alloc.free(spec.filter_predicates);
    freeArrayAny(alloc, spec.filter_array_any);
    if (spec.filter_array_any.len > 0) alloc.free(spec.filter_array_any);
    freeArrayContains(alloc, spec.filter_array_contains);
    if (spec.filter_array_contains.len > 0) alloc.free(spec.filter_array_contains);
    freeArrayEq(alloc, spec.filter_array_eq);
    if (spec.filter_array_eq.len > 0) alloc.free(spec.filter_array_eq);
    freeInPredicates(alloc, spec.filter_in_predicates);
    if (spec.filter_in_predicates.len > 0) alloc.free(spec.filter_in_predicates);
    freeJsonContains(alloc, spec.filter_json_contains);
    if (spec.filter_json_contains.len > 0) alloc.free(spec.filter_json_contains);
    freeJsonPathEq(alloc, spec.filter_json_path_eq);
    if (spec.filter_json_path_eq.len > 0) alloc.free(spec.filter_json_path_eq);
    freeJsonPathExists(alloc, spec.filter_json_path_exists);
    if (spec.filter_json_path_exists.len > 0) alloc.free(spec.filter_json_path_exists);
    freeTextPatterns(alloc, spec.filter_text_patterns);
    if (spec.filter_text_patterns.len > 0) alloc.free(spec.filter_text_patterns);
    freeExpressionConditions(alloc, spec.filter_expressions);
    if (spec.filter_expressions.len > 0) alloc.free(spec.filter_expressions);
    freeExpressionArrayContains(alloc, spec.filter_expression_array_contains);
    if (spec.filter_expression_array_contains.len > 0) alloc.free(spec.filter_expression_array_contains);
    freeExpressionPredicateGroups(alloc, spec.filter_any);
    if (spec.filter_any.len > 0) alloc.free(spec.filter_any);
    freeExpressionPredicateGroups(alloc, spec.filter_not);
    if (spec.filter_not.len > 0) alloc.free(spec.filter_not);
}

pub fn freeAggregateSpecs(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsAggregateSpec) void {
    for (values) |value| freeAggregateSpec(alloc, value);
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

pub fn freeTableAlias(alloc: std.mem.Allocator, value: TableAlias) void {
    alloc.free(value.name);
    alloc.free(value.alias);
}

pub fn freeQualifiedField(alloc: std.mem.Allocator, value: QualifiedField) void {
    alloc.free(value.qualifier);
    alloc.free(value.field);
}

pub fn freeQualifiedProjections(alloc: std.mem.Allocator, values: []const QualifiedProjection) void {
    for (values) |value| {
        freeQualifiedField(alloc, value.source);
        alloc.free(value.output);
    }
    if (values.len > 0) alloc.free(values);
}

pub fn parseTableAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TableAlias {
    const name = try grammar.parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    const alias = if (parser.matchKeywordTag(tokens, pos, .as))
        try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else if (parser.peekKind(tokens, pos.*, .identifier) and !nextIsJoinClauseKeyword(tokens, pos.*))
        try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else
        try alloc.dupe(u8, name);
    var alias_transferred = false;
    errdefer if (!alias_transferred) alloc.free(alias);
    name_transferred = true;
    alias_transferred = true;
    return .{ .name = name, .alias = alias };
}

pub fn inferSelectSourceAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
) !?TableAlias {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => {
                if (depth != 0 or !token.matchesKeywordTag(.from)) continue;
                return try inferSelectSourceAliasAtAlloc(alloc, tokens, i + 1);
            },
            else => {},
        }
    }
    return null;
}

pub fn inferSelectSourceAliasAtAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    from_index: usize,
) !?TableAlias {
    var name_index = from_index;
    if (name_index >= tokens.len) return null;
    if (tokens[name_index].matchesKeywordTag(.only)) {
        name_index += 1;
    }
    if (name_index >= tokens.len or tokens[name_index].kind != .identifier) return null;
    const name = try grammar.normalizeSqlObjectIdentifierAlloc(alloc, tokens[name_index].text);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);

    var alias_index = name_index + 1;
    const alias = alias: {
        if (alias_index < tokens.len and tokens[alias_index].matchesKeywordTag(.as)) {
            alias_index += 1;
            if (alias_index >= tokens.len or tokens[alias_index].kind != .identifier) return error.UnsupportedSqlShape;
            break :alias try alloc.dupe(u8, tokens[alias_index].text);
        }
        if (alias_index < tokens.len and tokens[alias_index].kind == .identifier and !selectSourceAliasTailKeyword(tokens[alias_index])) {
            break :alias try alloc.dupe(u8, tokens[alias_index].text);
        }
        break :alias try alloc.dupe(u8, name);
    };
    var alias_transferred = false;
    errdefer if (!alias_transferred) alloc.free(alias);

    name_transferred = true;
    alias_transferred = true;
    return .{ .name = name, .alias = alias };
}

pub fn selectSourceAliasTailKeyword(token: Token) bool {
    return token.matchesKeywordTag(.where) or
        token.matchesKeywordTag(.group) or
        token.matchesKeywordTag(.having) or
        token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.fetch) or
        token.matchesKeywordTag(.@"for") or
        token.matchesKeywordTag(.left) or
        token.matchesKeywordTag(.outer) or
        token.matchesKeywordTag(.inner) or
        token.matchesKeywordTag(.join) or
        token.matchesKeywordTag(.on) or
        token.matchesKeywordTag(.using) or
        token.matchesKeywordTag(.returning) or
        token.matchesKeywordTag(.set) or
        token.matchesKeywordTag(.@"union") or
        token.matchesKeywordTag(.intersect) or
        token.matchesKeywordTag(.except);
}

pub fn parseDmlTargetAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TableAlias {
    _ = parser.matchKeywordTag(tokens, pos, .only);
    const name = try grammar.parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    const alias = if (parser.matchKeywordTag(tokens, pos, .as))
        try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else if (parser.peekKind(tokens, pos.*, .identifier) and !nextIsDmlTargetTailKeyword(tokens, pos.*))
        try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else
        try alloc.dupe(u8, name);
    var alias_transferred = false;
    errdefer if (!alias_transferred) alloc.free(alias);
    name_transferred = true;
    alias_transferred = true;
    return .{ .name = name, .alias = alias };
}

pub fn nextIsDmlTargetTailKeyword(tokens: []const Token, pos: usize) bool {
    if (nextIsJoinClauseKeyword(tokens, pos)) return true;
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.matchesKeywordTag(.default) or
        token.matchesKeywordTag(.values) or
        token.matchesKeywordTag(.conflict) or
        token.matchesKeywordTag(.set) or
        token.matchesKeywordTag(.where) or
        token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.fetch) or
        token.matchesKeywordTag(.@"for") or
        token.matchesKeywordTag(.returning);
}

pub fn nextIsJoinClauseKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.matchesKeywordTag(.left) or
        token.matchesKeywordTag(.outer) or
        token.matchesKeywordTag(.inner) or
        token.matchesKeywordTag(.join) or
        token.matchesKeywordTag(.on) or
        token.matchesKeywordTag(.where) or
        token.matchesKeywordTag(.set) or
        token.matchesKeywordTag(.from) or
        token.matchesKeywordTag(.using) or
        token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.returning) or
        token.matchesKeywordTag(.@"for") or
        token.matchesKeywordTag(.group) or
        token.matchesKeywordTag(.@"union") or
        token.matchesKeywordTag(.intersect) or
        token.matchesKeywordTag(.except);
}

pub fn parseQualifiedFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !QualifiedField {
    const identifier = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
    defer alloc.free(identifier);
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return error.UnsupportedSqlShape;
    if (dot == 0 or dot + 1 >= identifier.len) return error.UnsupportedSqlShape;
    const qualifier = try alloc.dupe(u8, identifier[0..dot]);
    var qualifier_transferred = false;
    errdefer if (!qualifier_transferred) alloc.free(qualifier);
    const field = try alloc.dupe(u8, identifier[dot + 1 ..]);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    if (std.mem.indexOfScalar(u8, field, '.') != null) return error.UnsupportedSqlShape;
    qualifier_transferred = true;
    field_transferred = true;
    return .{ .qualifier = qualifier, .field = field };
}

pub fn parseJoinProjectionListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const QualifiedProjection {
    var projections = std.ArrayListUnmanaged(QualifiedProjection).empty;
    errdefer {
        for (projections.items) |projection| {
            freeQualifiedField(alloc, projection.source);
            alloc.free(projection.output);
        }
        projections.deinit(alloc);
    }
    while (true) {
        const source = try parseQualifiedFieldAlloc(alloc, tokens, pos);
        var source_transferred = false;
        errdefer if (!source_transferred) freeQualifiedField(alloc, source);
        const output = if (parser.matchKeywordTag(tokens, pos, .as))
            try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos)
        else
            try alloc.dupe(u8, source.field);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        try projections.append(alloc, .{ .source = source, .output = output });
        source_transferred = true;
        output_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try projections.toOwnedSlice(alloc);
}

pub fn freeLateralCorrelations(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsLateralCorrelation) void {
    for (values) |value| {
        alloc.free(value.left_field);
        alloc.free(value.right_field);
    }
}

pub fn freeLateralSubquery(alloc: std.mem.Allocator, value: LateralSubquery) void {
    if (value.table.name.len > 0 or value.table.alias.len > 0) freeTableAlias(alloc, value.table);
    ddl_plan.freeDdlRelationalColumns(alloc, value.output_columns);
    freeRelationalChecks(alloc, value.predicates);
    if (value.predicates.len > 0) alloc.free(value.predicates);
    freeJsonContains(alloc, value.json_contains);
    if (value.json_contains.len > 0) alloc.free(value.json_contains);
    freeJsonPathExists(alloc, value.json_path_exists);
    if (value.json_path_exists.len > 0) alloc.free(value.json_path_exists);
    freeArrayContains(alloc, value.array_contains);
    if (value.array_contains.len > 0) alloc.free(value.array_contains);
    freeArrayEq(alloc, value.array_eq);
    if (value.array_eq.len > 0) alloc.free(value.array_eq);
    freeInPredicates(alloc, value.in_predicates);
    if (value.in_predicates.len > 0) alloc.free(value.in_predicates);
    freeTextPatterns(alloc, value.text_patterns);
    if (value.text_patterns.len > 0) alloc.free(value.text_patterns);
    freeExpressionConditions(alloc, value.expression_predicates);
    if (value.expression_predicates.len > 0) alloc.free(value.expression_predicates);
    freeExpressionPredicateGroups(alloc, value.expression_or_predicates);
    if (value.expression_or_predicates.len > 0) alloc.free(value.expression_or_predicates);
    freeExpressionPredicateGroups(alloc, value.expression_not_predicates);
    if (value.expression_not_predicates.len > 0) alloc.free(value.expression_not_predicates);
    freeExpressionArrayContains(alloc, value.expression_array_contains);
    if (value.expression_array_contains.len > 0) alloc.free(value.expression_array_contains);
    freeExpressionConditions(alloc, value.match_expression_predicates);
    if (value.match_expression_predicates.len > 0) alloc.free(value.match_expression_predicates);
    freeExpressionPredicateGroups(alloc, value.match_expression_or_predicates);
    if (value.match_expression_or_predicates.len > 0) alloc.free(value.match_expression_or_predicates);
    freeExpressionPredicateGroups(alloc, value.match_expression_not_predicates);
    if (value.match_expression_not_predicates.len > 0) alloc.free(value.match_expression_not_predicates);
    freeExpressionArrayContains(alloc, value.match_expression_array_contains);
    if (value.match_expression_array_contains.len > 0) alloc.free(value.match_expression_array_contains);
    freeLateralCorrelations(alloc, value.correlations);
    if (value.correlations.len > 0) alloc.free(value.correlations);
    freeOrderBy(alloc, value.order_by);
    if (value.order_by.len > 0) alloc.free(value.order_by);
}

pub fn freeSelectItem(alloc: std.mem.Allocator, item: SelectItem) void {
    switch (item) {
        .field => |field| alloc.free(field),
        .json_extract => |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
            alloc.free(projection.path);
        },
        .array_length => |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        },
        .coalesce => |projection| freeCoalesceProjection(alloc, projection),
        .expression => |projection| freeExpressionProjection(alloc, projection),
        .field_alias => |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        },
    }
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

    const expression_assignments = try alloc.dupe(db_mod.types.RelationalRowsExpressionAssignment, &[_]db_mod.types.RelationalRowsExpressionAssignment{.{
        .field = try alloc.dupe(u8, "status"),
        .expression = .{
            .kind = .lower,
            .operands = try alloc.dupe(db_mod.types.RelationalRowsExpression, &[_]db_mod.types.RelationalRowsExpression{.{
                .kind = .field,
                .field = try alloc.dupe(u8, "source_status"),
            }}),
        },
    }});
    freeExpressionAssignments(alloc, expression_assignments);

    const json_set_assignments = try alloc.dupe(db_mod.types.RelationalRowsJsonSetExpressionAssignment, &[_]db_mod.types.RelationalRowsJsonSetExpressionAssignment{.{
        .field = try alloc.dupe(u8, "payload"),
        .path = try alloc.dupe([]const u8, &[_][]const u8{ try alloc.dupe(u8, "audit"), try alloc.dupe(u8, "state") }),
        .expression = .{
            .kind = .field,
            .field = try alloc.dupe(u8, "status"),
        },
    }});
    freeRowsJsonSetExpressionAssignments(alloc, json_set_assignments);

    const aggregate = db_mod.types.RelationalRowsAggregateSpec{
        .name = try alloc.dupe(u8, "total_amount"),
        .op = .sum,
        .expression = .{
            .kind = .field,
            .field = try alloc.dupe(u8, "amount"),
        },
        .array_order_by = try alloc.dupe(db_mod.types.RelationalRowsQueryOrder, &[_]db_mod.types.RelationalRowsQueryOrder{.{
            .field = try alloc.dupe(u8, "created_at"),
        }}),
        .filter_predicates = try alloc.dupe(runtime_schema.RelationalCheck, &[_]runtime_schema.RelationalCheck{.{
            .name = "",
            .field = try alloc.dupe(u8, "tenant_id"),
            .op = .eq,
            .value_json = try alloc.dupe(u8, "\"tenant-a\""),
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
    freeAggregateSpec(alloc, aggregate);
}

test "sql adapter plan counts merge arm surfaces" {
    const predicate = MergeArmPredicate{
        .side = .target,
        .field = "status",
        .op = .eq,
        .value_json = "\"open\"",
    };
    const expression_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .field, .field = "status" },
        .op = .eq,
        .rhs = &.{.{ .kind = .value, .value_json = "\"open\"" }},
    };
    const expression_group = db_mod.types.RelationalRowsExpressionPredicateGroup{
        .conditions = &.{expression_condition},
    };
    const mapping = MergeFieldMapping{
        .target_field = "status",
        .source_field = "source_status",
    };
    const expression_assignment = MergeExpressionAssignment{
        .target_field = "status_lower",
        .expression = .{ .kind = .lower, .operands = &.{.{ .kind = .field, .field = "source_status" }} },
    };
    const matched = [_]MergeMatchedArm{
        .{
            .predicates = &.{predicate},
            .expression_predicates = &.{expression_condition},
            .expression_or_predicates = &.{expression_group},
            .update = &.{mapping},
            .update_expressions = &.{expression_assignment},
            .delete = true,
        },
        .{
            .expression_not_predicates = &.{expression_group},
            .do_nothing = true,
        },
    };
    const not_matched = [_]MergeNotMatchedArm{
        .{
            .predicates = &.{predicate},
            .expression_predicates = &.{expression_condition},
            .expression_or_predicates = &.{expression_group},
            .insert = &.{mapping},
            .insert_expressions = &.{expression_assignment},
        },
        .{
            .expression_not_predicates = &.{expression_group},
            .do_nothing = true,
        },
    };

    try std.testing.expectEqual(@as(usize, 1), mergeMatchedPredicateCount(&matched));
    try std.testing.expectEqual(@as(usize, 1), mergeMatchedUpdateCount(&matched));
    try std.testing.expectEqual(@as(usize, 1), mergeMatchedUpdateExpressionCount(&matched));
    try std.testing.expectEqual(@as(usize, 1), mergeMatchedExpressionPredicateCount(&matched));
    try std.testing.expectEqual(@as(usize, 1), mergeMatchedExpressionOrPredicateCount(&matched));
    try std.testing.expectEqual(@as(usize, 1), mergeMatchedExpressionNotPredicateCount(&matched));
    try std.testing.expect(mergeMatchedHasDelete(&matched));
    try std.testing.expect(mergeMatchedHasDoNothing(&matched));
    try std.testing.expectEqual(@as(usize, 1), mergeNotMatchedPredicateCount(&not_matched));
    try std.testing.expectEqual(@as(usize, 1), mergeNotMatchedInsertCount(&not_matched));
    try std.testing.expectEqual(@as(usize, 1), mergeNotMatchedInsertExpressionCount(&not_matched));
    try std.testing.expectEqual(@as(usize, 1), mergeNotMatchedExpressionPredicateCount(&not_matched));
    try std.testing.expectEqual(@as(usize, 1), mergeNotMatchedExpressionOrPredicateCount(&not_matched));
    try std.testing.expectEqual(@as(usize, 1), mergeNotMatchedExpressionNotPredicateCount(&not_matched));
    try std.testing.expect(mergeNotMatchedHasDoNothing(&not_matched));
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

test "sql adapter plan owns projection helpers" {
    const alloc = std.testing.allocator;

    const select_list: SelectList = .{
        .fields = &.{"id"},
        .json_extract = &.{.{ .output = "tier", .field = "payload", .path = "$.tier", .as_text = true }},
        .array_length = &.{.{ .output = "tag_count", .field = "tags" }},
        .coalesce = &.{.{ .output = "display_name", .operands = &.{} }},
        .field_aliases = &.{.{ .field = "tenant_id", .output = "tenant" }},
        .expressions = &.{.{ .output = "is_active", .expression = .{ .kind = .value, .value_json = "true" } }},
        .outputs = &.{
            .{ .kind = .field, .index = 0 },
            .{ .kind = .json_extract, .index = 0 },
            .{ .kind = .array_length, .index = 0 },
            .{ .kind = .coalesce, .index = 0 },
            .{ .kind = .field_alias, .index = 0 },
            .{ .kind = .expression, .index = 0 },
        },
    };
    try std.testing.expectEqualStrings("id", selectOutputName(select_list, .{ .kind = .field, .index = 0 }).?);
    try std.testing.expectEqualStrings("tier", selectOutputName(select_list, .{ .kind = .json_extract, .index = 0 }).?);
    try std.testing.expectEqualStrings("tag_count", selectOutputName(select_list, .{ .kind = .array_length, .index = 0 }).?);
    try std.testing.expectEqualStrings("display_name", selectOutputName(select_list, .{ .kind = .coalesce, .index = 0 }).?);
    try std.testing.expectEqualStrings("tenant", selectOutputName(select_list, .{ .kind = .field_alias, .index = 0 }).?);
    try std.testing.expectEqualStrings("is_active", selectOutputName(select_list, .{ .kind = .expression, .index = 0 }).?);
    try std.testing.expect(selectOutputName(select_list, .{ .kind = .field, .index = 1 }) == null);
    try std.testing.expectEqual(ast.SelectOutputKind.json_extract, (try selectOutputByName("TIER", select_list)).?.kind);
    try std.testing.expect((try selectOutputByName("missing", select_list)) == null);

    var lowered = LoweredSelect{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .query = .{
            .select_all = false,
            .select = try strings.cloneStringSlice(alloc, &.{"id"}),
            .json_extract = try alloc.dupe(db_mod.types.RelationalRowsJsonExtractProjection, &[_]db_mod.types.RelationalRowsJsonExtractProjection{.{
                .output = try alloc.dupe(u8, "tier"),
                .field = try alloc.dupe(u8, "payload"),
                .path = try alloc.dupe(u8, "$.tier"),
                .as_text = true,
            }}),
        },
        .select_outputs = try alloc.dupe(SelectOutputRef, &[_]SelectOutputRef{
            .{ .kind = .field, .index = 0 },
            .{ .kind = .json_extract, .index = 0 },
        }),
    };
    defer lowered.deinit(alloc);
    try applyCteColumnAliasesAlloc(alloc, &lowered, &.{ "usage_id", "account_tier" });
    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.field_aliases.len);
    try std.testing.expectEqualStrings("usage_id", lowered.query.field_aliases[0].output);
    try std.testing.expectEqualStrings("id", lowered.query.field_aliases[0].field);
    try std.testing.expectEqualStrings("account_tier", lowered.query.json_extract[0].output);

    const json_extract = try alloc.dupe(db_mod.types.RelationalRowsJsonExtractProjection, &[_]db_mod.types.RelationalRowsJsonExtractProjection{.{
        .output = try alloc.dupe(u8, "tier"),
        .field = try alloc.dupe(u8, "payload"),
        .path = try alloc.dupe(u8, "$.tier"),
        .as_text = true,
    }});
    freeJsonExtract(alloc, json_extract);

    const array_lengths = try alloc.dupe(db_mod.types.RelationalRowsArrayLengthProjection, &[_]db_mod.types.RelationalRowsArrayLengthProjection{.{
        .output = try alloc.dupe(u8, "tag_count"),
        .field = try alloc.dupe(u8, "tags"),
    }});
    freeArrayLengthProjections(alloc, array_lengths);

    const aliases = try alloc.dupe(db_mod.types.RelationalRowsFieldAliasProjection, &[_]db_mod.types.RelationalRowsFieldAliasProjection{.{
        .output = try alloc.dupe(u8, "tenant"),
        .field = try alloc.dupe(u8, "tenant_id"),
    }});
    freeFieldAliasProjections(alloc, aliases);

    const join_on = try alloc.dupe(db_mod.types.RelationalRowsJoinOn, &[_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = try alloc.dupe(u8, "id"),
        .right_field = try alloc.dupe(u8, "tenant_id"),
    }});
    freeJoinOn(alloc, join_on);
    alloc.free(join_on);

    const join_select = try alloc.dupe(db_mod.types.RelationalRowsJoinProjection, &[_]db_mod.types.RelationalRowsJoinProjection{.{
        .output = try alloc.dupe(u8, "tenant_status"),
        .side = .right,
        .field = try alloc.dupe(u8, "status"),
    }});
    freeJoinProjections(alloc, join_select);
    alloc.free(join_select);

    const coalesce_operands = try alloc.dupe(db_mod.types.RelationalRowsCoalesceOperand, &[_]db_mod.types.RelationalRowsCoalesceOperand{
        .{
            .kind = .field,
            .field = try alloc.dupe(u8, "nickname"),
        },
        .{
            .kind = .value,
            .value_json = try alloc.dupe(u8, "\"anonymous\""),
        },
    });
    const coalesce_projection: db_mod.types.RelationalRowsCoalesceProjection = .{
        .output = try alloc.dupe(u8, "display_name"),
        .operands = coalesce_operands,
    };

    const expression_projection = try expressionProjectionFromCoalesceAlloc(alloc, coalesce_projection);
    defer freeExpressionProjection(alloc, expression_projection);
    try std.testing.expectEqualStrings("display_name", expression_projection.output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, expression_projection.expression.kind);
    try std.testing.expectEqual(@as(usize, 2), expression_projection.expression.operands.len);
    try std.testing.expectEqualStrings("nickname", expression_projection.expression.operands[0].field);
    try std.testing.expect(@intFromPtr(expression_projection.expression.operands.ptr) != @intFromPtr(coalesce_projection.operands.ptr));

    freeCoalesceProjection(alloc, coalesce_projection);
}

test "sql adapter plan parses relation aliases and qualified projections" {
    const alloc = std.testing.allocator;

    const explicit_alias_tokens = [_]Token{
        .{ .kind = .identifier, .text = "public.usage_records" },
        .{ .kind = .identifier, .text = "as" },
        .{ .kind = .identifier, .text = "u" },
    };
    var explicit_alias_pos: usize = 0;
    const explicit_alias = try parseTableAliasAlloc(alloc, &explicit_alias_tokens, &explicit_alias_pos);
    defer freeTableAlias(alloc, explicit_alias);
    try std.testing.expectEqual(@as(usize, 3), explicit_alias_pos);
    try std.testing.expectEqualStrings("usage_records", explicit_alias.name);
    try std.testing.expectEqualStrings("u", explicit_alias.alias);

    const implicit_alias_tokens = [_]Token{
        .{ .kind = .identifier, .text = "usage_records" },
        .{ .kind = .identifier, .text = "u" },
        .{ .kind = .identifier, .text = "where" },
    };
    var implicit_alias_pos: usize = 0;
    const implicit_alias = try parseTableAliasAlloc(alloc, &implicit_alias_tokens, &implicit_alias_pos);
    defer freeTableAlias(alloc, implicit_alias);
    try std.testing.expectEqual(@as(usize, 2), implicit_alias_pos);
    try std.testing.expectEqualStrings("usage_records", implicit_alias.name);
    try std.testing.expectEqualStrings("u", implicit_alias.alias);

    const no_alias_tokens = [_]Token{
        .{ .kind = .identifier, .text = "usage_records" },
        .{ .kind = .identifier, .text = "join" },
    };
    var no_alias_pos: usize = 0;
    const no_alias = try parseTableAliasAlloc(alloc, &no_alias_tokens, &no_alias_pos);
    defer freeTableAlias(alloc, no_alias);
    try std.testing.expectEqual(@as(usize, 1), no_alias_pos);
    try std.testing.expectEqualStrings("usage_records", no_alias.alias);

    const dml_target_tokens = [_]Token{
        .{ .kind = .identifier, .text = "usage_records" },
        .{ .kind = .identifier, .text = "set" },
    };
    var dml_target_pos: usize = 0;
    const dml_target = try parseDmlTargetAliasAlloc(alloc, &dml_target_tokens, &dml_target_pos);
    defer freeTableAlias(alloc, dml_target);
    try std.testing.expectEqual(@as(usize, 1), dml_target_pos);
    try std.testing.expectEqualStrings("usage_records", dml_target.alias);

    const select_tokens = [_]Token{
        .{ .kind = .identifier, .text = "select" },
        .{ .kind = .identifier, .text = "id" },
        .{ .kind = .identifier, .text = "from" },
        .{ .kind = .identifier, .text = "public.usage_records" },
        .{ .kind = .identifier, .text = "as" },
        .{ .kind = .identifier, .text = "u" },
        .{ .kind = .identifier, .text = "where" },
    };
    const inferred = (try inferSelectSourceAliasAlloc(alloc, &select_tokens, 0)) orelse return error.TestUnexpectedResult;
    defer freeTableAlias(alloc, inferred);
    try std.testing.expectEqualStrings("usage_records", inferred.name);
    try std.testing.expectEqualStrings("u", inferred.alias);

    const projection_tokens = [_]Token{
        .{ .kind = .identifier, .text = "u.id" },
        .{ .kind = .identifier, .text = "as" },
        .{ .kind = .identifier, .text = "usage_id" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .identifier, .text = "c.status" },
    };
    var projection_pos: usize = 0;
    const projections = try parseJoinProjectionListAlloc(alloc, &projection_tokens, &projection_pos);
    defer freeQualifiedProjections(alloc, projections);
    try std.testing.expectEqual(@as(usize, projection_tokens.len), projection_pos);
    try std.testing.expectEqual(@as(usize, 2), projections.len);
    try std.testing.expectEqualStrings("u", projections[0].source.qualifier);
    try std.testing.expectEqualStrings("id", projections[0].source.field);
    try std.testing.expectEqualStrings("usage_id", projections[0].output);
    try std.testing.expectEqualStrings("c", projections[1].source.qualifier);
    try std.testing.expectEqualStrings("status", projections[1].source.field);
    try std.testing.expectEqualStrings("status", projections[1].output);
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

test "sql adapter plan resolves CTE and base table sources" {
    const alloc = std.testing.allocator;
    const ctes = [_]db_mod.types.RelationalRowsCte{.{ .name = "recent_usage" }};

    var cte_select = LoweredSelect{
        .table_name = try alloc.dupe(u8, "recent_usage"),
        .query = .{},
    };
    defer cte_select.deinit(alloc);
    var no_base: ?[]const u8 = null;
    try resolveSelectSourceForPlanAlloc(alloc, &cte_select, &ctes, &no_base);
    try std.testing.expect(no_base == null);
    try std.testing.expectEqualStrings("recent_usage", cte_select.query.source_cte);

    var base_select = LoweredSelect{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .query = .{},
    };
    defer base_select.deinit(alloc);
    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    try resolveSelectSourceForPlanAlloc(alloc, &base_select, &ctes, &base_table_name);
    try std.testing.expectEqualStrings("usage_records", base_table_name.?);
    try std.testing.expectEqualStrings("", base_select.query.source_cte);

    var mismatch_select = LoweredSelect{
        .table_name = try alloc.dupe(u8, "other_records"),
        .query = .{},
    };
    defer mismatch_select.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, resolveSelectSourceForPlanAlloc(alloc, &mismatch_select, &ctes, &base_table_name));
}

test "sql adapter plan resolves join CTE sides to physical base table" {
    const alloc = std.testing.allocator;
    const ctes = [_]db_mod.types.RelationalRowsCte{.{ .name = "recent_usage" }};

    var base_table_name: ?[]const u8 = try alloc.dupe(u8, "usage_records");
    defer if (base_table_name) |table| alloc.free(table);

    var lowered = LoweredJoin{
        .left_table_name = try alloc.dupe(u8, "usage_records"),
        .right_table_name = try alloc.dupe(u8, "recent_usage"),
        .join = .{},
    };
    defer lowered.deinit(alloc);

    try resolveJoinSourcesForPlanAlloc(alloc, &lowered, &ctes, &base_table_name);
    try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
    try std.testing.expectEqualStrings("usage_records", lowered.right_table_name);
    try std.testing.expectEqualStrings("", lowered.join.left.source_cte);
    try std.testing.expectEqualStrings("recent_usage", lowered.join.right.source_cte);
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

test "sql adapter plan clones query check without catalog name" {
    const alloc = std.testing.allocator;
    const rhs = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "\"deleted\"",
    }};
    const source: runtime_schema.RelationalCheck = .{
        .name = "catalog_status_check",
        .field = "status",
        .op = .ne,
        .value_json = "\"ready\"",
        .expression = .{
            .lhs = .{
                .kind = .field,
                .field = "status",
            },
            .op = .ne,
            .rhs = rhs[0..],
        },
    };

    const cloned = try cloneQueryRelationalCheckAlloc(alloc, source);
    defer freeRelationalCheck(alloc, cloned);

    try std.testing.expectEqual(@as(usize, 0), cloned.name.len);
    try std.testing.expectEqualStrings("status", cloned.field);
    try std.testing.expect(cloned.field.ptr != source.field.ptr);
    try std.testing.expectEqualStrings("\"ready\"", cloned.value_json.?);
    try std.testing.expect(cloned.value_json.?.ptr != source.value_json.?.ptr);
    try std.testing.expect(cloned.expression != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, cloned.expression.?.lhs.kind);
    try std.testing.expectEqualStrings("status", cloned.expression.?.lhs.field);
    try std.testing.expect(cloned.expression.?.lhs.field.ptr != source.expression.?.lhs.field.ptr);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.ne, cloned.expression.?.op);
    try std.testing.expectEqual(@as(usize, 1), cloned.expression.?.rhs.len);
    try std.testing.expectEqualStrings("\"deleted\"", cloned.expression.?.rhs[0].value_json);
    try std.testing.expect(cloned.expression.?.rhs[0].value_json.ptr != source.expression.?.rhs[0].value_json.ptr);
}
