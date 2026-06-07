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

const db_mod = @import("../storage/db/mod.zig");
const platform_time = @import("../platform/time.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");

pub const default_array_agg_max_items: u32 = 1024;

pub const SqlValue = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    json: []const u8,

    fn jsonAlloc(self: SqlValue, alloc: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .null => try alloc.dupe(u8, "null"),
            .bool => |value| try alloc.dupe(u8, if (value) "true" else "false"),
            .integer => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .float => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .string => |value| try std.json.Stringify.valueAlloc(alloc, value, .{}),
            .json => |value| try alloc.dupe(u8, value),
        };
    }

    fn asU32(self: SqlValue) !u32 {
        return switch (self) {
            .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @intCast(value) else error.UnsupportedSqlShape,
            else => error.UnsupportedSqlShape,
        };
    }
};

pub const LoweredSelect = struct {
    table_name: []const u8,
    query: db_mod.types.RelationalRowsQueryRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.query.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredQueryPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsQueryPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.plan.ctes) |cte| {
            alloc.free(cte.name);
            var query = cte.query;
            query.deinit(alloc);
        }
        if (self.plan.ctes.len > 0) alloc.free(self.plan.ctes);
        self.plan.query.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredInsert = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutation = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredAggregate = struct {
    table_name: []const u8,
    aggregate: db_mod.types.RelationalRowsAggregateRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.aggregate.source.deinit(alloc);
        freeStringSlice(alloc, self.aggregate.group_by);
        freeAggregateSpecs(alloc, self.aggregate.aggregations);
        if (self.aggregate.aggregations.len > 0) alloc.free(self.aggregate.aggregations);
        freeRelationalChecks(alloc, self.aggregate.having_predicates);
        if (self.aggregate.having_predicates.len > 0) alloc.free(self.aggregate.having_predicates);
        freeOrderBy(alloc, self.aggregate.order_by);
        if (self.aggregate.order_by.len > 0) alloc.free(self.aggregate.order_by);
        self.* = undefined;
    }
};

pub const LoweredJoin = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    join: db_mod.types.RelationalRowsJoinRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.left_table_name);
        alloc.free(self.right_table_name);
        self.join.left.deinit(alloc);
        self.join.right.deinit(alloc);
        freeJoinOn(alloc, self.join.on);
        if (self.join.on.len > 0) alloc.free(self.join.on);
        freeJoinProjections(alloc, self.join.select);
        if (self.join.select.len > 0) alloc.free(self.join.select);
        freeOrderBy(alloc, self.join.order_by);
        if (self.join.order_by.len > 0) alloc.free(self.join.order_by);
        self.* = undefined;
    }
};

const TokenKind = enum {
    identifier,
    string,
    number,
    placeholder,
    comma,
    star,
    eq,
    neq,
    gt,
    gte,
    lt,
    lte,
    plus,
    minus,
    lparen,
    rparen,
    at_contains,
    pipe_concat,
    question,
    arrow_json,
    arrow_text,
    semicolon,
};

const Token = struct {
    kind: TokenKind,
    text: []const u8,
};

pub fn lowerSelectAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSelect {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseSelect();
}

pub fn lowerQueryPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredQueryPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseQueryPlan();
}

pub fn lowerInsertAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseInsert();
}

pub fn lowerInsertWithResolverAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try parser.parseInsert();
}

pub fn lowerUpdateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try parser.parseUpdate();
}

pub fn lowerDeleteAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try parser.parseDelete();
}

pub fn lowerAggregateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregate {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseAggregate();
}

pub fn lowerJoinAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseJoin();
}

const Parser = struct {
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize = 0,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: ?relational_rows.UniqueSelectorResolver = null,

    fn parseQueryPlan(self: *@This()) !LoweredQueryPlan {
        if (!self.peekKeyword("with")) {
            var lowered = try self.parseSelect();
            errdefer lowered.deinit(self.alloc);
            const table_name = lowered.table_name;
            lowered.table_name = "";
            return .{
                .table_name = table_name,
                .plan = .{ .query = lowered.query },
            };
        }

        try self.expectKeyword("with");
        var ctes = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCte).empty;
        errdefer {
            for (ctes.items) |cte| {
                self.alloc.free(cte.name);
                var query = cte.query;
                query.deinit(self.alloc);
            }
            ctes.deinit(self.alloc);
        }
        var base_table_name: ?[]const u8 = null;
        errdefer if (base_table_name) |table| self.alloc.free(table);

        while (true) {
            const cte_name = try self.parseIdentifierOwned();
            var cte_name_transferred = false;
            errdefer if (!cte_name_transferred) self.alloc.free(cte_name);
            if (findCteByName(ctes.items, cte_name) != null) return error.UnsupportedSqlShape;
            try self.expectKeyword("as");
            try self.expect(.lparen);
            const close_index = try self.findMatchingRParen();
            var sub = Parser{
                .alloc = self.alloc,
                .tokens = self.tokens[self.pos..close_index],
                .schema = self.schema,
                .params = self.params,
                .unique_resolver = self.unique_resolver,
            };
            var lowered = try sub.parseSelect();
            errdefer lowered.deinit(self.alloc);
            self.pos = close_index + 1;
            try self.resolveSelectSourceForPlan(&lowered, ctes.items, &base_table_name);
            try ctes.append(self.alloc, .{
                .name = cte_name,
                .query = lowered.query,
            });
            lowered.query = .{};
            self.alloc.free(lowered.table_name);
            lowered.table_name = "";
            cte_name_transferred = true;
            if (self.match(.comma) == null) break;
        }

        var final = try self.parseSelect();
        errdefer final.deinit(self.alloc);
        try self.resolveSelectSourceForPlan(&final, ctes.items, &base_table_name);
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const table_name = base_table_name orelse return error.UnsupportedSqlShape;
        base_table_name = null;
        const owned_ctes = try ctes.toOwnedSlice(self.alloc);
        self.alloc.free(final.table_name);
        final.table_name = "";
        return .{
            .table_name = table_name,
            .plan = .{
                .ctes = owned_ctes,
                .query = final.query,
            },
        };
    }

    fn resolveSelectSourceForPlan(
        self: *@This(),
        lowered: *LoweredSelect,
        ctes: []const db_mod.types.RelationalRowsCte,
        base_table_name: *?[]const u8,
    ) !void {
        if (findCteByName(ctes, lowered.table_name) != null) {
            lowered.query.source_cte = try self.alloc.dupe(u8, lowered.table_name);
            return;
        }
        if (base_table_name.*) |base| {
            if (!std.mem.eql(u8, base, lowered.table_name)) return error.UnsupportedSqlShape;
        } else {
            base_table_name.* = try self.alloc.dupe(u8, lowered.table_name);
        }
    }

    fn findMatchingRParen(self: *@This()) !usize {
        var depth: usize = 1;
        var i = self.pos;
        while (i < self.tokens.len) : (i += 1) {
            switch (self.tokens[i].kind) {
                .lparen => depth += 1,
                .rparen => {
                    depth -= 1;
                    if (depth == 0) return i;
                },
                else => {},
            }
        }
        return error.UnsupportedSqlShape;
    }

    fn parseSelect(self: *@This()) !LoweredSelect {
        try self.expectKeyword("select");

        const select = try self.parseSelectList();
        errdefer freeStringSlice(self.alloc, select.fields);
        errdefer freeJsonExtract(self.alloc, select.json_extract);
        errdefer freeArrayLengthProjections(self.alloc, select.array_length);
        errdefer freeCoalesceProjections(self.alloc, select.coalesce);

        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
        errdefer {
            freeJsonPathEq(self.alloc, json_path_eq.items);
            json_path_eq.deinit(self.alloc);
        }
        var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
        errdefer {
            freeJsonContains(self.alloc, json_contains.items);
            json_contains.deinit(self.alloc);
        }
        var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
        errdefer {
            freeJsonPathExists(self.alloc, json_path_exists.items);
            json_path_exists.deinit(self.alloc);
        }
        var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
        errdefer {
            freeArrayContains(self.alloc, array_contains.items);
            array_contains.deinit(self.alloc);
        }
        var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
        errdefer {
            freeArrayEq(self.alloc, array_eq.items);
            array_eq.deinit(self.alloc);
        }
        var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
        errdefer {
            freeInPredicates(self.alloc, in_predicates.items);
            in_predicates.deinit(self.alloc);
        }
        var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, or_predicates.items);
            or_predicates.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var row_claim: ?db_mod.types.RowClaimRequest = null;
        var limit: ?u32 = null;
        var offset: u32 = 0;

        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseWhere(&predicates, &json_contains, &json_path_eq, &json_path_exists, &array_contains, &array_eq, &in_predicates, &or_predicates);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseOrderBy(&order_by);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.matchKeyword("for")) {
                try self.expectKeyword("update");
                const skip_locked = if (self.matchKeyword("skip")) blk: {
                    try self.expectKeyword("locked");
                    break :blk true;
                } else false;
                row_claim = .{ .mode = .for_update, .skip_locked = skip_locked };
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        return .{
            .table_name = table_name,
            .query = .{
                .predicates = try predicates.toOwnedSlice(self.alloc),
                .array_contains = try array_contains.toOwnedSlice(self.alloc),
                .array_eq = try array_eq.toOwnedSlice(self.alloc),
                .in_predicates = try in_predicates.toOwnedSlice(self.alloc),
                .json_contains = try json_contains.toOwnedSlice(self.alloc),
                .json_path_eq = try json_path_eq.toOwnedSlice(self.alloc),
                .json_path_exists = try json_path_exists.toOwnedSlice(self.alloc),
                .or_predicates = try or_predicates.toOwnedSlice(self.alloc),
                .select = select.fields,
                .json_extract = select.json_extract,
                .array_length = select.array_length,
                .coalesce = select.coalesce,
                .select_all = select.select_all,
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .row_claim = row_claim,
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseAggregate(self: *@This()) !LoweredAggregate {
        try self.expectKeyword("select");

        const select = try self.parseAggregateSelectList();
        defer freeStringSlice(self.alloc, select.group_fields);
        errdefer {
            freeAggregateSpecs(self.alloc, select.aggregations);
            if (select.aggregations.len > 0) self.alloc.free(select.aggregations);
        }
        if (select.aggregations.len == 0) return error.UnsupportedSqlShape;

        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
        errdefer {
            freeJsonPathEq(self.alloc, json_path_eq.items);
            json_path_eq.deinit(self.alloc);
        }
        var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
        errdefer {
            freeJsonContains(self.alloc, json_contains.items);
            json_contains.deinit(self.alloc);
        }
        var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
        errdefer {
            freeJsonPathExists(self.alloc, json_path_exists.items);
            json_path_exists.deinit(self.alloc);
        }
        var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
        errdefer {
            freeArrayContains(self.alloc, array_contains.items);
            array_contains.deinit(self.alloc);
        }
        var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
        errdefer {
            freeArrayEq(self.alloc, array_eq.items);
            array_eq.deinit(self.alloc);
        }
        var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
        errdefer {
            freeInPredicates(self.alloc, in_predicates.items);
            in_predicates.deinit(self.alloc);
        }
        var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, or_predicates.items);
            or_predicates.deinit(self.alloc);
        }
        var group_by = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (group_by.items) |field| self.alloc.free(field);
            group_by.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }
        var having_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, having_predicates.items);
            having_predicates.deinit(self.alloc);
        }

        var limit: ?u32 = null;
        var offset: u32 = 0;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseWhere(&predicates, &json_contains, &json_path_eq, &json_path_exists, &array_contains, &array_eq, &in_predicates, &or_predicates);
            } else if (self.matchKeyword("group")) {
                try self.expectKeyword("by");
                try self.parseGroupBy(&group_by);
            } else if (self.matchKeyword("having")) {
                try self.parseAggregateHaving(&having_predicates, select.group_fields, select.aggregations);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseAggregateOrderBy(&order_by, select.group_fields, select.aggregations);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        try validateAggregateGroupBy(select.group_fields, group_by.items);

        return .{
            .table_name = table_name,
            .aggregate = .{
                .source = .{
                    .predicates = try predicates.toOwnedSlice(self.alloc),
                    .array_contains = try array_contains.toOwnedSlice(self.alloc),
                    .array_eq = try array_eq.toOwnedSlice(self.alloc),
                    .in_predicates = try in_predicates.toOwnedSlice(self.alloc),
                    .json_contains = try json_contains.toOwnedSlice(self.alloc),
                    .json_path_eq = try json_path_eq.toOwnedSlice(self.alloc),
                    .json_path_exists = try json_path_exists.toOwnedSlice(self.alloc),
                    .or_predicates = try or_predicates.toOwnedSlice(self.alloc),
                    .select_all = true,
                },
                .group_by = try group_by.toOwnedSlice(self.alloc),
                .aggregations = select.aggregations,
                .having_predicates = try having_predicates.toOwnedSlice(self.alloc),
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseJoin(self: *@This()) !LoweredJoin {
        try self.expectKeyword("select");

        const raw_select = try self.parseJoinProjectionListAlloc();
        defer freeQualifiedProjections(self.alloc, raw_select);

        try self.expectKeyword("from");
        const left_table = try self.parseTableAliasAlloc();
        defer freeTableAlias(self.alloc, left_table);

        const join_type: db_mod.types.RelationalRowsJoinType = if (self.matchKeyword("left")) blk: {
            _ = self.matchKeyword("outer");
            try self.expectKeyword("join");
            break :blk .left;
        } else blk: {
            _ = self.matchKeyword("inner");
            try self.expectKeyword("join");
            break :blk .inner;
        };

        const right_table = try self.parseTableAliasAlloc();
        defer freeTableAlias(self.alloc, right_table);
        if (std.mem.eql(u8, left_table.alias, right_table.alias)) return error.UnsupportedSqlShape;

        try self.expectKeyword("on");
        var on = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinOn).empty;
        errdefer {
            freeJoinOn(self.alloc, on.items);
            on.deinit(self.alloc);
        }
        try self.parseJoinOn(&on, left_table.alias, right_table.alias);

        var left_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, left_predicates.items);
            left_predicates.deinit(self.alloc);
        }
        var right_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, right_predicates.items);
            right_predicates.deinit(self.alloc);
        }

        var select = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection).empty;
        errdefer {
            freeJoinProjections(self.alloc, select.items);
            select.deinit(self.alloc);
        }
        try self.resolveJoinProjectionsAlloc(raw_select, left_table.alias, right_table.alias, &select);

        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var limit: ?u32 = null;
        var offset: u32 = 0;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseJoinWhere(&left_predicates, &right_predicates, left_table.alias, right_table.alias);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseJoinOrderBy(&order_by, select.items);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        return .{
            .left_table_name = try self.alloc.dupe(u8, left_table.name),
            .right_table_name = try self.alloc.dupe(u8, right_table.name),
            .join = .{
                .left = .{
                    .predicates = try left_predicates.toOwnedSlice(self.alloc),
                    .select_all = true,
                },
                .right = .{
                    .predicates = try right_predicates.toOwnedSlice(self.alloc),
                    .select_all = true,
                },
                .on = try on.toOwnedSlice(self.alloc),
                .join_type = join_type,
                .select = try select.toOwnedSlice(self.alloc),
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseInsert(self: *@This()) !LoweredInsert {
        try self.expectKeyword("insert");
        try self.expectKeyword("into");

        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expect(.lparen);
        var columns = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
        }
        while (true) {
            const column = try self.parseIdentifierOwned();
            var column_transferred = false;
            errdefer if (!column_transferred) self.alloc.free(column);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, column, null) == null) return error.InvalidSqlCatalog;
            try columns.append(self.alloc, column);
            column_transferred = true;
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);

        try self.expectKeyword("values");
        try self.expect(.lparen);

        var row_values = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (row_values.items) |value| self.alloc.free(value);
            row_values.deinit(self.alloc);
        }
        for (columns.items, 0..) |column_name, i| {
            const column = relationalColumnForField(self.schema, column_name, null) orelse return error.InvalidSqlCatalog;
            const value_json = try self.parseSqlColumnValueAlloc(column);
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try row_values.append(self.alloc, value_json);
            value_transferred = true;
            if (i + 1 < columns.items.len) {
                try self.expect(.comma);
            }
        }
        try self.expect(.rparen);

        var conflict: ?ConflictClause = null;
        errdefer if (conflict) |value| freeConflictClause(self.alloc, value);
        if (self.matchKeyword("on")) {
            conflict = try self.parseConflictClause(columns.items, row_values.items);
        }

        var returning_fields: []const []const u8 = &.{};
        errdefer freeStringSlice(self.alloc, returning_fields);
        if (self.matchKeyword("returning")) {
            returning_fields = try self.parseReturningListAlloc();
        }

        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const body_json = try self.insertBodyJsonAlloc(columns.items, row_values.items, conflict, returning_fields);
        defer self.alloc.free(body_json);
        var batch = if (conflict != null)
            try relational_rows.parseRowsBatchRequestWithResolver(self.alloc, table_name, body_json, self.schema, self.unique_resolver orelse return error.UnsupportedRowsSelector)
        else
            try relational_rows.parseRowsBatchRequest(self.alloc, body_json, self.schema);
        errdefer batch.deinit(self.alloc);

        for (columns.items) |column| self.alloc.free(column);
        columns.deinit(self.alloc);
        for (row_values.items) |value| self.alloc.free(value);
        row_values.deinit(self.alloc);
        if (conflict) |value| freeConflictClause(self.alloc, value);
        conflict = null;
        freeStringSlice(self.alloc, returning_fields);

        return .{
            .table_name = table_name,
            .batch = batch,
        };
    }

    fn parseUpdate(self: *@This()) !LoweredMutation {
        try self.expectKeyword("update");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expectKeyword("set");
        var patch = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, patch.items);
            patch.deinit(self.alloc);
        }
        var increment = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, increment.items);
            increment.deinit(self.alloc);
        }
        var json_set = std.ArrayListUnmanaged(JsonSetValue).empty;
        errdefer {
            freeJsonSetValues(self.alloc, json_set.items);
            json_set.deinit(self.alloc);
        }
        while (true) {
            try self.parseUpdateAssignment(&patch, &increment, &json_set);
            if (self.match(.comma) == null) break;
        }
        if (patch.items.len == 0 and increment.items.len == 0 and json_set.items.len == 0) return error.UnsupportedSqlShape;

        try self.expectKeyword("where");
        const where_json = try self.parsePrimaryWhereJsonAlloc();
        defer self.alloc.free(where_json);

        var returning_fields: []const []const u8 = &.{};
        errdefer freeStringSlice(self.alloc, returning_fields);
        if (self.matchKeyword("returning")) {
            returning_fields = try self.parseReturningListAlloc();
        }

        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const explicit_expected_version = if (updateWillLookupExistingRow(self.schema, returning_fields))
            null
        else
            try self.expectedVersionForWhereAlloc(table_name, where_json);

        const body_json = try self.updateBodyJsonAlloc(where_json, patch.items, increment.items, json_set.items, returning_fields, explicit_expected_version);
        defer self.alloc.free(body_json);
        var batch = try relational_rows.parseRowsBatchRequestWithResolver(self.alloc, table_name, body_json, self.schema, self.unique_resolver orelse return error.UnsupportedRowsSelector);
        errdefer batch.deinit(self.alloc);

        freeFieldJsonValues(self.alloc, patch.items);
        patch.deinit(self.alloc);
        freeFieldJsonValues(self.alloc, increment.items);
        increment.deinit(self.alloc);
        freeJsonSetValues(self.alloc, json_set.items);
        json_set.deinit(self.alloc);
        freeStringSlice(self.alloc, returning_fields);

        return .{
            .table_name = table_name,
            .batch = batch,
        };
    }

    fn parseDelete(self: *@This()) !LoweredMutation {
        try self.expectKeyword("delete");
        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expectKeyword("where");
        const where_json = try self.parsePrimaryWhereJsonAlloc();
        defer self.alloc.free(where_json);

        var returning_fields: []const []const u8 = &.{};
        errdefer freeStringSlice(self.alloc, returning_fields);
        if (self.matchKeyword("returning")) {
            returning_fields = try self.parseReturningListAlloc();
        }

        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const explicit_expected_version = if (returning_fields.len > 0)
            null
        else
            try self.expectedVersionForWhereAlloc(table_name, where_json);

        const body_json = try self.deleteBodyJsonAlloc(where_json, returning_fields, explicit_expected_version);
        defer self.alloc.free(body_json);
        var batch = try relational_rows.parseRowsBatchRequestWithResolver(self.alloc, table_name, body_json, self.schema, self.unique_resolver orelse return error.UnsupportedRowsSelector);
        errdefer batch.deinit(self.alloc);

        freeStringSlice(self.alloc, returning_fields);

        return .{
            .table_name = table_name,
            .batch = batch,
        };
    }

    const SelectList = struct {
        fields: []const []const u8 = &.{},
        json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection = &.{},
        array_length: []const db_mod.types.RelationalRowsArrayLengthProjection = &.{},
        coalesce: []const db_mod.types.RelationalRowsCoalesceProjection = &.{},
        select_all: bool = false,
    };

    fn parseSelectList(self: *@This()) !SelectList {
        if (self.match(.star) != null) return .{ .select_all = true };

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(field);
            fields.deinit(self.alloc);
        }
        var json_extract = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonExtractProjection).empty;
        errdefer {
            for (json_extract.items) |projection| {
                self.alloc.free(projection.output);
                self.alloc.free(projection.field);
                self.alloc.free(projection.path);
            }
            json_extract.deinit(self.alloc);
        }
        var array_length = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayLengthProjection).empty;
        errdefer {
            for (array_length.items) |projection| {
                self.alloc.free(projection.output);
                self.alloc.free(projection.field);
            }
            array_length.deinit(self.alloc);
        }
        var coalesce = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceProjection).empty;
        errdefer {
            for (coalesce.items) |projection| {
                self.alloc.free(projection.output);
                for (projection.operands) |operand| {
                    switch (operand.kind) {
                        .field => if (operand.field.len > 0) self.alloc.free(operand.field),
                        .value => if (operand.value_json.len > 0) self.alloc.free(operand.value_json),
                    }
                }
                if (projection.operands.len > 0) self.alloc.free(projection.operands);
            }
            coalesce.deinit(self.alloc);
        }
        while (true) {
            const item = try self.parseSelectItem();
            var item_transferred = false;
            errdefer if (!item_transferred) freeSelectItem(self.alloc, item);
            switch (item) {
                .field => |field| try fields.append(self.alloc, field),
                .json_extract => |projection| try json_extract.append(self.alloc, projection),
                .array_length => |projection| try array_length.append(self.alloc, projection),
                .coalesce => |projection| try coalesce.append(self.alloc, projection),
            }
            item_transferred = true;
            if (self.match(.comma) == null) break;
        }
        return .{
            .fields = try fields.toOwnedSlice(self.alloc),
            .json_extract = try json_extract.toOwnedSlice(self.alloc),
            .array_length = try array_length.toOwnedSlice(self.alloc),
            .coalesce = try coalesce.toOwnedSlice(self.alloc),
            .select_all = false,
        };
    }

    const AggregateSelectList = struct {
        group_fields: []const []const u8 = &.{},
        aggregations: []const db_mod.types.RelationalRowsAggregateSpec = &.{},
    };

    fn parseAggregateSelectList(self: *@This()) !AggregateSelectList {
        var group_fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (group_fields.items) |field| self.alloc.free(field);
            group_fields.deinit(self.alloc);
        }
        var aggregations = std.ArrayListUnmanaged(db_mod.types.RelationalRowsAggregateSpec).empty;
        errdefer {
            freeAggregateSpecs(self.alloc, aggregations.items);
            aggregations.deinit(self.alloc);
        }
        while (true) {
            if (self.nextIsAggregateFunction()) {
                const spec = try self.parseAggregateSpecAlloc();
                var spec_transferred = false;
                errdefer if (!spec_transferred) freeAggregateSpec(self.alloc, spec);
                try aggregations.append(self.alloc, spec);
                spec_transferred = true;
            } else {
                const field = try self.parseFieldExpressionOwned();
                var field_transferred = false;
                errdefer if (!field_transferred) self.alloc.free(field);
                if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
                if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
                try self.consumeCompatibleProjectionAlias(field);
                try group_fields.append(self.alloc, field);
                field_transferred = true;
            }
            if (self.match(.comma) == null) break;
        }
        return .{
            .group_fields = try group_fields.toOwnedSlice(self.alloc),
            .aggregations = try aggregations.toOwnedSlice(self.alloc),
        };
    }

    fn nextIsAggregateFunction(self: *@This()) bool {
        if (self.pos + 1 >= self.tokens.len) return false;
        if (self.tokens[self.pos].kind != .identifier or self.tokens[self.pos + 1].kind != .lparen) return false;
        const name = self.tokens[self.pos].text;
        return std.ascii.eqlIgnoreCase(name, "count") or
            std.ascii.eqlIgnoreCase(name, "sum") or
            std.ascii.eqlIgnoreCase(name, "min") or
            std.ascii.eqlIgnoreCase(name, "max") or
            std.ascii.eqlIgnoreCase(name, "avg") or
            std.ascii.eqlIgnoreCase(name, "array_agg");
    }

    fn parseAggregateSpecAlloc(self: *@This()) !db_mod.types.RelationalRowsAggregateSpec {
        const function_name = self.match(.identifier) orelse return error.UnsupportedSqlShape;
        const op = aggregateOpForName(function_name.text) orelse return error.UnsupportedSqlShape;
        try self.expect(.lparen);
        const distinct = self.matchKeyword("distinct");
        var field: ?[]const u8 = null;
        var field_transferred = false;
        errdefer if (!field_transferred) if (field) |owned| self.alloc.free(owned);
        var array_order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, array_order_by.items);
            array_order_by.deinit(self.alloc);
        }
        if (op == .count and self.match(.star) != null) {
            if (distinct) return error.UnsupportedSqlShape;
            field = null;
        } else {
            const parsed_field = try self.parseFieldExpressionOwned();
            if (relationalColumnForField(self.schema, parsed_field, null)) |column| {
                if (op == .count) {
                    if (column.field_type == .array or column.field_type == .json) {
                        self.alloc.free(parsed_field);
                        return error.InvalidSqlCatalog;
                    }
                } else if (op == .array_agg) {
                    if (column.field_type == .array or column.field_type == .json) {
                        self.alloc.free(parsed_field);
                        return error.InvalidSqlCatalog;
                    }
                } else if (column.field_type != .numeric) {
                    self.alloc.free(parsed_field);
                    return error.InvalidSqlCatalog;
                }
            } else {
                self.alloc.free(parsed_field);
                return error.InvalidSqlCatalog;
            }
            field = parsed_field;
        }
        if (op == .array_agg and self.matchKeyword("order")) {
            try self.expectKeyword("by");
            try self.parseOrderBy(&array_order_by);
        }
        try self.expect(.rparen);
        const filter_predicates = try self.parseAggregateFilterPredicatesAlloc();
        var filter_transferred = false;
        errdefer if (!filter_transferred) {
            freeRelationalChecks(self.alloc, filter_predicates);
            if (filter_predicates.len > 0) self.alloc.free(filter_predicates);
        };
        const name = try self.parseAggregateAliasOrDefaultAlloc(op, field);
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);
        field_transferred = true;
        filter_transferred = true;
        name_transferred = true;
        return .{
            .name = name,
            .op = op,
            .field = field,
            .distinct = distinct,
            .distinct_max_items = if (distinct) db_mod.types.default_relational_rows_aggregate_distinct_max_items else 0,
            .array_max_items = if (op == .array_agg) default_array_agg_max_items else 0,
            .array_order_by = try array_order_by.toOwnedSlice(self.alloc),
            .filter_predicates = filter_predicates,
        };
    }

    fn parseAggregateFilterPredicatesAlloc(self: *@This()) ![]const runtime_schema.RelationalCheck {
        if (!self.matchKeyword("filter")) return &.{};
        try self.expect(.lparen);
        try self.expectKeyword("where");

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }

        while (true) {
            const predicate = try self.parseScalarWherePredicateAlloc();
            var predicate_transferred = false;
            errdefer if (!predicate_transferred) freeRelationalCheck(self.alloc, predicate);
            try predicates.append(self.alloc, predicate);
            predicate_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
        if (predicates.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        return try predicates.toOwnedSlice(self.alloc);
    }

    fn parseAggregateAliasOrDefaultAlloc(
        self: *@This(),
        op: db_mod.types.RelationalRowsAggregateOp,
        field: ?[]const u8,
    ) ![]const u8 {
        if (self.matchKeyword("as")) return try self.parseIdentifierOwned();
        if (field) |field_name| return try std.fmt.allocPrint(self.alloc, "{s}_{s}", .{ aggregateOpName(op), field_name });
        return try self.alloc.dupe(u8, aggregateOpName(op));
    }

    const TableAlias = struct {
        name: []const u8,
        alias: []const u8,
    };

    const QualifiedField = struct {
        qualifier: []const u8,
        field: []const u8,
    };

    const QualifiedProjection = struct {
        source: QualifiedField,
        output: []const u8,
    };

    fn parseTableAliasAlloc(self: *@This()) !TableAlias {
        const name = try self.parseIdentifierOwned();
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);
        const alias = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else if (self.peekKind(.identifier) and !self.nextIsJoinClauseKeyword())
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, name);
        var alias_transferred = false;
        errdefer if (!alias_transferred) self.alloc.free(alias);
        name_transferred = true;
        alias_transferred = true;
        return .{ .name = name, .alias = alias };
    }

    fn nextIsJoinClauseKeyword(self: *@This()) bool {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].kind != .identifier) return false;
        const token = self.tokens[self.pos].text;
        return std.ascii.eqlIgnoreCase(token, "left") or
            std.ascii.eqlIgnoreCase(token, "outer") or
            std.ascii.eqlIgnoreCase(token, "inner") or
            std.ascii.eqlIgnoreCase(token, "join") or
            std.ascii.eqlIgnoreCase(token, "on") or
            std.ascii.eqlIgnoreCase(token, "where") or
            std.ascii.eqlIgnoreCase(token, "order") or
            std.ascii.eqlIgnoreCase(token, "limit") or
            std.ascii.eqlIgnoreCase(token, "offset") or
            std.ascii.eqlIgnoreCase(token, "group");
    }

    fn parseQualifiedFieldAlloc(self: *@This()) !QualifiedField {
        const identifier = try self.parseIdentifierOwned();
        defer self.alloc.free(identifier);
        const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return error.UnsupportedSqlShape;
        if (dot == 0 or dot + 1 >= identifier.len) return error.UnsupportedSqlShape;
        const qualifier = try self.alloc.dupe(u8, identifier[0..dot]);
        var qualifier_transferred = false;
        errdefer if (!qualifier_transferred) self.alloc.free(qualifier);
        const field = try self.alloc.dupe(u8, identifier[dot + 1 ..]);
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        if (std.mem.indexOfScalar(u8, field, '.') != null) return error.UnsupportedSqlShape;
        qualifier_transferred = true;
        field_transferred = true;
        return .{ .qualifier = qualifier, .field = field };
    }

    fn parseJoinProjectionListAlloc(self: *@This()) ![]const QualifiedProjection {
        var projections = std.ArrayListUnmanaged(QualifiedProjection).empty;
        errdefer {
            freeQualifiedProjections(self.alloc, projections.items);
            projections.deinit(self.alloc);
        }
        while (true) {
            const source = try self.parseQualifiedFieldAlloc();
            var source_transferred = false;
            errdefer if (!source_transferred) freeQualifiedField(self.alloc, source);
            const output = if (self.matchKeyword("as"))
                try self.parseIdentifierOwned()
            else
                try self.alloc.dupe(u8, source.field);
            var output_transferred = false;
            errdefer if (!output_transferred) self.alloc.free(output);
            try projections.append(self.alloc, .{ .source = source, .output = output });
            source_transferred = true;
            output_transferred = true;
            if (self.match(.comma) == null) break;
        }
        return try projections.toOwnedSlice(self.alloc);
    }

    fn resolveJoinProjectionsAlloc(
        self: *@This(),
        raw_select: []const QualifiedProjection,
        left_alias: []const u8,
        right_alias: []const u8,
        select: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection),
    ) !void {
        for (raw_select) |projection| {
            const side = try joinSideForQualifier(projection.source.qualifier, left_alias, right_alias);
            if (relationalColumnForField(self.schema, projection.source.field, null) == null) return error.InvalidSqlCatalog;
            const output = try self.alloc.dupe(u8, projection.output);
            var output_transferred = false;
            errdefer if (!output_transferred) self.alloc.free(output);
            const field = try self.alloc.dupe(u8, projection.source.field);
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            try select.append(self.alloc, .{ .output = output, .side = side, .field = field });
            output_transferred = true;
            field_transferred = true;
        }
    }

    fn parseJoinOn(
        self: *@This(),
        on: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinOn),
        left_alias: []const u8,
        right_alias: []const u8,
    ) !void {
        while (true) {
            const lhs = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, lhs);
            try self.expect(.eq);
            const rhs = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, rhs);
            const lhs_side = try joinSideForQualifier(lhs.qualifier, left_alias, right_alias);
            const rhs_side = try joinSideForQualifier(rhs.qualifier, left_alias, right_alias);
            if (lhs_side == rhs_side) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, lhs.field, null) == null or relationalColumnForField(self.schema, rhs.field, null) == null) return error.InvalidSqlCatalog;
            const left_field_source = if (lhs_side == .left) lhs.field else rhs.field;
            const right_field_source = if (lhs_side == .right) lhs.field else rhs.field;
            const left_field = try self.alloc.dupe(u8, left_field_source);
            var left_transferred = false;
            errdefer if (!left_transferred) self.alloc.free(left_field);
            const right_field = try self.alloc.dupe(u8, right_field_source);
            var right_transferred = false;
            errdefer if (!right_transferred) self.alloc.free(right_field);
            try on.append(self.alloc, .{ .left_field = left_field, .right_field = right_field });
            left_transferred = true;
            right_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseJoinWhere(
        self: *@This(),
        left_predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        right_predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        left_alias: []const u8,
        right_alias: []const u8,
    ) !void {
        while (true) {
            const source = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, source);
            const side = try joinSideForQualifier(source.qualifier, left_alias, right_alias);
            const column = relationalColumnForField(self.schema, source.field, null) orelse return error.InvalidSqlCatalog;
            const target = if (side == .left) left_predicates else right_predicates;
            const op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("is")) blk: {
                if (self.matchKeyword("not")) {
                    try self.expectKeyword("null");
                    break :blk .is_not_null;
                }
                try self.expectKeyword("null");
                break :blk .is_null;
            } else try self.parseComparisonOp();
            const value_json = if (op == .is_null or op == .is_not_null)
                null
            else
                try self.parseSqlColumnValueAlloc(column);
            var value_transferred = false;
            errdefer if (!value_transferred) if (value_json) |json| self.alloc.free(json);
            const field = try self.alloc.dupe(u8, source.field);
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            try target.append(self.alloc, .{ .name = "", .field = field, .op = op, .value_json = value_json });
            field_transferred = true;
            value_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseJoinOrderBy(
        self: *@This(),
        order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
        select: []const db_mod.types.RelationalRowsJoinProjection,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!joinProjectionContainsOutput(select, field)) return error.UnsupportedSqlShape;
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            try order_by.append(self.alloc, .{ .field = field, .direction = direction });
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    const SelectItem = union(enum) {
        field: []const u8,
        json_extract: db_mod.types.RelationalRowsJsonExtractProjection,
        array_length: db_mod.types.RelationalRowsArrayLengthProjection,
        coalesce: db_mod.types.RelationalRowsCoalesceProjection,
    };

    const FieldJsonValue = struct {
        field: []const u8,
        value_json: []const u8,
    };

    const FieldPredicate = struct {
        field: []const u8,
        op: runtime_schema.UniquePredicateOp,
        value_json: ?[]const u8 = null,
    };

    const JsonSetValue = struct {
        field: []const u8,
        path: []const []const u8,
        value_json: []const u8,
    };

    const ConflictAction = enum {
        nothing,
        update,
    };

    const ConflictTarget = union(enum) {
        primary,
        unique: UniqueConflictTarget,
    };

    const UniqueConflictTarget = struct {
        name: []const u8,
        where_json: []const u8 = "",
    };

    const ConflictClause = struct {
        target: ConflictTarget,
        action: ConflictAction,
        patch: []const FieldJsonValue = &.{},
        increment: []const FieldJsonValue = &.{},
        json_set: []const JsonSetValue = &.{},
    };

    fn parseConflictClause(self: *@This(), insert_columns: []const []const u8, insert_values: []const []const u8) !ConflictClause {
        try self.expectKeyword("conflict");
        const target = try self.parseConflictTarget();
        errdefer freeConflictTarget(self.alloc, target);
        try self.expectKeyword("do");

        if (self.matchKeyword("nothing")) {
            return .{
                .target = target,
                .action = .nothing,
            };
        }

        try self.expectKeyword("update");
        try self.expectKeyword("set");
        var patch = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, patch.items);
            patch.deinit(self.alloc);
        }
        var increment = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, increment.items);
            increment.deinit(self.alloc);
        }
        var json_set = std.ArrayListUnmanaged(JsonSetValue).empty;
        errdefer {
            freeJsonSetValues(self.alloc, json_set.items);
            json_set.deinit(self.alloc);
        }
        while (true) {
            try self.parseConflictUpdateAssignment(insert_columns, insert_values, &patch, &increment, &json_set);
            if (self.match(.comma) == null) break;
        }
        if (patch.items.len == 0 and increment.items.len == 0 and json_set.items.len == 0) return error.UnsupportedSqlShape;

        return .{
            .target = target,
            .action = .update,
            .patch = try patch.toOwnedSlice(self.alloc),
            .increment = try increment.toOwnedSlice(self.alloc),
            .json_set = try json_set.toOwnedSlice(self.alloc),
        };
    }

    fn parseConflictTarget(self: *@This()) !ConflictTarget {
        try self.expect(.lparen);
        if (self.matchKeyword("lower")) {
            try self.expect(.lparen);
            const field = try self.parseIdentifierOwned();
            defer self.alloc.free(field);
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try self.expect(.rparen);
            try self.expect(.rparen);
            const constraint = findUniqueConstraintByLowerExpression(self.schema, field) orelse return error.InvalidSqlCatalog;
            if (constraint.where.len != 0) return error.UnsupportedSqlShape;
            const name = try self.alloc.dupe(u8, constraint.name);
            errdefer self.alloc.free(name);
            return .{ .unique = .{ .name = name } };
        }

        var columns = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
        }
        while (true) {
            const column = try self.parseIdentifierOwned();
            var column_transferred = false;
            errdefer if (!column_transferred) self.alloc.free(column);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, column, null) == null) return error.InvalidSqlCatalog;
            try columns.append(self.alloc, column);
            column_transferred = true;
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);

        var where_json: []const u8 = "";
        errdefer if (where_json.len > 0) self.alloc.free(where_json);
        if (self.matchKeyword("where")) {
            where_json = try self.parseUniquePredicateWhereJsonAlloc();
        }

        if (columnsMatchPrimaryKey(self.schema.primary_key.?, columns.items)) {
            if (where_json.len > 0) return error.UnsupportedSqlShape;
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
            return .primary;
        }

        const constraint = findUniqueConstraintByColumns(self.schema, columns.items, where_json.len > 0) orelse return error.InvalidSqlCatalog;
        if (constraint.expressions.len != 0) return error.UnsupportedSqlShape;
        if (where_json.len == 0 and constraint.where.len != 0) return error.UnsupportedSqlShape;
        if (where_json.len > 0 and constraint.where.len == 0) return error.UnsupportedSqlShape;
        if (where_json.len > 0) try validateUniqueWhereJsonMatches(self.alloc, where_json, constraint.where);

        const name = try self.alloc.dupe(u8, constraint.name);
        errdefer self.alloc.free(name);
        for (columns.items) |column| self.alloc.free(column);
        columns.deinit(self.alloc);
        const out_where = where_json;
        where_json = "";
        return .{ .unique = .{
            .name = name,
            .where_json = out_where,
        } };
    }

    fn parseConflictUpdateAssignment(
        self: *@This(),
        insert_columns: []const []const u8,
        insert_values: []const []const u8,
        patch: *std.ArrayListUnmanaged(FieldJsonValue),
        increment: *std.ArrayListUnmanaged(FieldJsonValue),
        json_set: *std.ArrayListUnmanaged(JsonSetValue),
    ) !void {
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        defer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (primaryKeyContains(self.schema.primary_key.?, field)) return error.UnsupportedSqlShape;
        try self.expect(.eq);

        if (self.matchKeyword("jsonb_set")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const json_field = try self.parseIdentifierOwned();
            defer self.alloc.free(json_field);
            if (!std.mem.eql(u8, json_field, field)) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const path = try self.parsePostgresJsonPathAlloc();
            var path_transferred = false;
            errdefer if (!path_transferred) freeStringSlice(self.alloc, path);
            try self.expect(.comma);
            const value_json = try self.parseConflictValueJsonAlloc(column, insert_columns, insert_values);
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (self.match(.comma) != null) {
                if (!self.matchKeyword("true") and !self.matchKeyword("false")) return error.UnsupportedSqlShape;
            }
            try self.expect(.rparen);
            try json_set.append(self.alloc, .{
                .field = field,
                .path = path,
                .value_json = value_json,
            });
            field_transferred = true;
            path_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .pipe_concat) {
            const json_field_token = self.match(.identifier).?;
            if (!std.mem.eql(u8, json_field_token.text, field)) return error.UnsupportedSqlShape;
            try self.expect(.pipe_concat);
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.appendJsonObjectConcatSetValues(field, json_set);
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].kind == .plus or self.tokens[self.pos + 1].kind == .minus)) {
            try self.parseIncrementAssignment(field, column, increment);
            return;
        }

        const value_json = try self.parseConflictValueJsonAlloc(column, insert_columns, insert_values);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        try patch.append(self.alloc, .{
            .field = field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn parseConflictValueJsonAlloc(
        self: *@This(),
        column: runtime_schema.RelationalColumn,
        insert_columns: []const []const u8,
        insert_values: []const []const u8,
    ) ![]const u8 {
        if (self.peekKind(.identifier) and self.pos < self.tokens.len) {
            const token = self.tokens[self.pos];
            if (std.mem.startsWith(u8, token.text, "excluded.")) {
                self.pos += 1;
                const source = token.text["excluded.".len..];
                if (!std.mem.eql(u8, source, column.name)) return error.UnsupportedSqlShape;
                for (insert_columns, insert_values) |insert_column, insert_value| {
                    if (std.mem.eql(u8, insert_column, source)) return try self.alloc.dupe(u8, insert_value);
                }
                return error.UnsupportedSqlShape;
            }
        }
        return try self.parseSqlColumnValueAlloc(column);
    }

    fn parseIncrementAssignment(
        self: *@This(),
        field: []const u8,
        column: runtime_schema.RelationalColumn,
        increment: *std.ArrayListUnmanaged(FieldJsonValue),
    ) !void {
        if (column.field_type != .numeric) return error.InvalidSqlCatalog;
        const source = try self.parseIdentifierOwned();
        defer self.alloc.free(source);
        if (!std.mem.eql(u8, source, field)) return error.UnsupportedSqlShape;
        const negated = if (self.match(.plus) != null)
            false
        else if (self.match(.minus) != null)
            true
        else
            return error.UnsupportedSqlShape;
        const raw_value_json = try self.parseSqlColumnValueAlloc(column);
        defer self.alloc.free(raw_value_json);
        const value_json = try self.normalizedIncrementJsonAlloc(raw_value_json, negated);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        const owned_field = try self.alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(owned_field);
        try increment.append(self.alloc, .{
            .field = owned_field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn normalizedIncrementJsonAlloc(self: *@This(), value_json: []const u8, negated: bool) ![]const u8 {
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (!negated) {
            switch (parsed.value) {
                .integer, .float, .number_string => return try self.alloc.dupe(u8, value_json),
                else => return error.UnsupportedSqlShape,
            }
        }
        return switch (parsed.value) {
            .integer => |value| if (value == std.math.minInt(i64))
                error.UnsupportedSqlShape
            else
                try std.fmt.allocPrint(self.alloc, "{d}", .{-value}),
            .float => |value| try std.fmt.allocPrint(self.alloc, "{d}", .{-value}),
            .number_string => |text| blk: {
                const value = std.fmt.parseFloat(f64, text) catch return error.UnsupportedSqlShape;
                break :blk try std.fmt.allocPrint(self.alloc, "{d}", .{-value});
            },
            else => error.UnsupportedSqlShape,
        };
    }

    fn parseUniquePredicateWhereJsonAlloc(self: *@This()) ![]const u8 {
        var predicates = std.ArrayListUnmanaged(FieldPredicate).empty;
        defer {
            freeFieldPredicates(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            _ = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
            if (self.matchKeyword("is")) {
                const op: runtime_schema.UniquePredicateOp = if (self.matchKeyword("not")) blk: {
                    try self.expectKeyword("null");
                    break :blk .is_not_null;
                } else blk: {
                    try self.expectKeyword("null");
                    break :blk .is_null;
                };
                try predicates.append(self.alloc, .{ .field = field, .op = op });
                field_transferred = true;
            } else {
                const op: runtime_schema.UniquePredicateOp = if (self.match(.eq) != null) .eq else if (self.match(.neq) != null) .ne else return error.UnsupportedSqlShape;
                const value_json = try self.parseJsonValueAlloc();
                var value_transferred = false;
                errdefer if (!value_transferred) self.alloc.free(value_json);
                try predicates.append(self.alloc, .{ .field = field, .op = op, .value_json = value_json });
                field_transferred = true;
                value_transferred = true;
            }
            if (!self.matchKeyword("and")) break;
        }

        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"all\":[");
        for (predicates.items, 0..) |predicate, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"field\":{f},\"op\":{f}", .{ std.json.fmt(predicate.field, .{}), std.json.fmt(uniquePredicateOpToken(predicate.op), .{}) });
            if (predicate.value_json) |value_json| {
                try writer.writeAll(",\"value\":");
                try writer.writeAll(value_json);
            }
            try writer.writeByte('}');
        }
        try writer.writeAll("]}");
        return try out.toOwnedSlice();
    }

    fn parseUpdateAssignment(
        self: *@This(),
        patch: *std.ArrayListUnmanaged(FieldJsonValue),
        increment: *std.ArrayListUnmanaged(FieldJsonValue),
        json_set: *std.ArrayListUnmanaged(JsonSetValue),
    ) !void {
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        defer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (primaryKeyContains(self.schema.primary_key.?, field)) return error.UnsupportedSqlShape;
        try self.expect(.eq);

        if (self.matchKeyword("jsonb_set")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const json_field = try self.parseIdentifierOwned();
            defer self.alloc.free(json_field);
            if (!std.mem.eql(u8, json_field, field)) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const path = try self.parsePostgresJsonPathAlloc();
            var path_transferred = false;
            errdefer if (!path_transferred) freeStringSlice(self.alloc, path);
            try self.expect(.comma);
            const value_json = try self.parseJsonValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (self.match(.comma) != null) {
                if (!self.matchKeyword("true") and !self.matchKeyword("false")) return error.UnsupportedSqlShape;
            }
            try self.expect(.rparen);
            try json_set.append(self.alloc, .{
                .field = field,
                .path = path,
                .value_json = value_json,
            });
            field_transferred = true;
            path_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .pipe_concat) {
            const json_field_token = self.match(.identifier).?;
            if (!std.mem.eql(u8, json_field_token.text, field)) return error.UnsupportedSqlShape;
            try self.expect(.pipe_concat);
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.appendJsonObjectConcatSetValues(field, json_set);
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].kind == .plus or self.tokens[self.pos + 1].kind == .minus)) {
            try self.parseIncrementAssignment(field, column, increment);
            return;
        }

        const value_json = try self.parseSqlColumnValueAlloc(column);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        try patch.append(self.alloc, .{
            .field = field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn parsePrimaryWhereJsonAlloc(self: *@This()) ![]u8 {
        const primary_key = self.schema.primary_key orelse return error.InvalidSqlCatalog;
        var values = std.ArrayListUnmanaged(FieldJsonValue).empty;
        defer {
            freeFieldJsonValues(self.alloc, values.items);
            values.deinit(self.alloc);
        }

        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
            if (!primaryKeyContains(primary_key, field)) return error.UnsupportedSqlShape;
            try self.expect(.eq);
            const value_json = try self.parseSqlColumnValueAlloc(column);
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try values.append(self.alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            if (!self.matchKeyword("and")) break;
        }

        if (values.items.len != primary_key.columns.len) return error.UnsupportedSqlShape;
        for (primary_key.columns) |column_name| {
            var found = false;
            for (values.items) |value| {
                if (std.mem.eql(u8, value.field, column_name)) {
                    found = true;
                    break;
                }
            }
            if (!found) return error.UnsupportedSqlShape;
        }

        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"primary\":{");
        for (values.items, 0..) |value, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(value.field, .{})});
            try writer.writeAll(value.value_json);
        }
        try writer.writeAll("}}");
        return try out.toOwnedSlice();
    }

    fn expectedVersionForWhereAlloc(self: *@This(), table_name: []const u8, where_json: []const u8) !u64 {
        const resolver = self.unique_resolver orelse return error.UnsupportedRowsSelector;
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        const key = (try relational_rows.physicalPrimaryKeyFromWhereAlloc(self.alloc, table_name, self.schema, parsed.value, resolver, false)) orelse return error.RowSelectorNotFound;
        defer self.alloc.free(key);
        var row = (try resolver.lookupPrimary(self.alloc, table_name, key)) orelse return error.RowSelectorNotFound;
        defer row.deinit(self.alloc);
        return row.version;
    }

    fn updateBodyJsonAlloc(
        self: *@This(),
        where_json: []const u8,
        patch: []const FieldJsonValue,
        increment: []const FieldJsonValue,
        json_set: []const JsonSetValue,
        returning_fields: []const []const u8,
        expected_version: ?u64,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"operations\":[{\"op\":\"update\",\"where\":");
        try writer.writeAll(where_json);
        if (expected_version) |version| try writer.print(",\"expected_version\":{d}", .{version});
        if (patch.len > 0) {
            try writer.writeAll(",\"patch\":{");
            for (patch, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                try writer.writeAll(item.value_json);
            }
            try writer.writeByte('}');
        }
        if (increment.len > 0) {
            try writer.writeAll(",\"increment\":{");
            for (increment, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                try writer.writeAll(item.value_json);
            }
            try writer.writeByte('}');
        }
        if (json_set.len > 0) {
            try writer.writeAll(",\"json_set\":[");
            for (json_set, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{{\"field\":{f},\"path\":[", .{std.json.fmt(item.field, .{})});
                for (item.path, 0..) |segment, segment_i| {
                    if (segment_i != 0) try writer.writeByte(',');
                    try writer.print("{f}", .{std.json.fmt(segment, .{})});
                }
                try writer.writeAll("],\"value\":");
                try writer.writeAll(item.value_json);
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
        }
        try self.writeReturningJson(writer, returning_fields);
        try writer.writeAll("}]}");
        return try out.toOwnedSlice();
    }

    fn deleteBodyJsonAlloc(
        self: *@This(),
        where_json: []const u8,
        returning_fields: []const []const u8,
        expected_version: ?u64,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"operations\":[{\"op\":\"delete\",\"where\":");
        try writer.writeAll(where_json);
        if (expected_version) |version| try writer.print(",\"expected_version\":{d}", .{version});
        try self.writeReturningJson(writer, returning_fields);
        try writer.writeAll("}]}");
        return try out.toOwnedSlice();
    }

    fn writeReturningJson(self: *@This(), writer: *std.Io.Writer, returning_fields: []const []const u8) !void {
        _ = self;
        if (returning_fields.len == 0) return;
        try writer.writeAll(",\"returning\":[");
        for (returning_fields, 0..) |field, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}", .{std.json.fmt(field, .{})});
        }
        try writer.writeByte(']');
    }

    fn parseWhere(
        self: *@This(),
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
        json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
        json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
        array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
        array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
        in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
        or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    ) !void {
        if (self.whereHasTopLevelOr()) {
            try self.parseScalarOrWhere(or_predicates);
            return;
        }
        while (true) {
            try self.parseWhereAtom(predicates, json_contains, json_path_eq, json_path_exists, array_contains, array_eq, in_predicates, false);
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseScalarOrWhere(
        self: *@This(),
        or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    ) !void {
        while (true) {
            var branch = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
            errdefer {
                freeRelationalChecks(self.alloc, branch.items);
                branch.deinit(self.alloc);
            }

            while (true) {
                const predicate = try self.parseScalarWherePredicateAlloc();
                var predicate_transferred = false;
                errdefer if (!predicate_transferred) freeRelationalCheck(self.alloc, predicate);
                try branch.append(self.alloc, predicate);
                predicate_transferred = true;
                if (!self.matchKeyword("and")) break;
            }

            if (branch.items.len == 0) return error.UnsupportedSqlShape;
            const predicates = try branch.toOwnedSlice(self.alloc);
            var predicates_transferred = false;
            errdefer if (!predicates_transferred) {
                freeRelationalChecks(self.alloc, predicates);
                if (predicates.len > 0) self.alloc.free(predicates);
            };
            try or_predicates.append(self.alloc, .{ .predicates = predicates });
            predicates_transferred = true;

            if (!self.matchKeyword("or")) break;
        }
    }

    fn parseScalarWherePredicateAlloc(self: *@This()) !runtime_schema.RelationalCheck {
        const field = try self.parseFieldExpressionOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        if (self.peekKind(.arrow_text) or self.peekKind(.arrow_json) or self.peekKind(.at_contains) or self.peekKind(.question)) return error.UnsupportedSqlShape;
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (column.field_type == .array or column.field_type == .json) return error.UnsupportedSqlShape;

        if (self.matchKeyword("is")) {
            const op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("not")) blk: {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            } else blk: {
                try self.expectKeyword("null");
                break :blk .is_null;
            };
            field_transferred = true;
            return .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = null,
            };
        }

        if (self.peekKeyword("in") or self.peekKeyword("not")) return error.UnsupportedSqlShape;
        const op = try self.parseComparisonOp();
        if (self.peekKeyword("any")) return error.UnsupportedSqlShape;
        const value_json = try self.parseSqlColumnValueAlloc(column);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        field_transferred = true;
        value_transferred = true;
        return .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = value_json,
        };
    }

    fn parseWhereAtom(
        self: *@This(),
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
        json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
        json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
        array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
        array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
        in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
        negated: bool,
    ) !void {
        if (!negated and self.matchKeyword("not")) {
            try self.expect(.lparen);
            try self.parseWhereAtom(predicates, json_contains, json_path_eq, json_path_exists, array_contains, array_eq, in_predicates, true);
            try self.expect(.rparen);
            return;
        }
        const field = try self.parseFieldExpressionOwned();
        var field_transferred = false;
        defer if (!field_transferred) self.alloc.free(field);
        const maybe_column = relationalColumnForField(self.schema, field, null);

        if (self.match(.arrow_text) != null) {
            const path = try self.parseJsonPathOwned();
            var path_transferred = false;
            errdefer if (!path_transferred) self.alloc.free(path);
            try self.expect(.eq);
            const value_json = try self.parseJsonValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (relationalColumnForField(self.schema, field, .json) == null) return error.InvalidSqlCatalog;
            const predicate_field = try self.alloc.dupe(u8, field);
            var predicate_field_transferred = false;
            errdefer if (!predicate_field_transferred) self.alloc.free(predicate_field);
            try json_path_eq.append(self.alloc, .{
                .field = predicate_field,
                .path = path,
                .value_json = value_json,
            });
            predicate_field_transferred = true;
            path_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.match(.at_contains) != null) {
            const column = maybe_column orelse return error.InvalidSqlCatalog;
            const value_json = try self.parseJsonDocumentValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (column.field_type == .array) {
                try validateJsonArray(self.alloc, value_json);
                try array_contains.append(self.alloc, .{
                    .field = field,
                    .value_json = value_json,
                });
                field_transferred = true;
                value_transferred = true;
                return;
            }
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try json_contains.append(self.alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.match(.question) != null) {
            if (relationalColumnForField(self.schema, field, .json) == null) return error.InvalidSqlCatalog;
            const path = try self.parseJsonPathOwned();
            var path_transferred = false;
            errdefer if (!path_transferred) self.alloc.free(path);
            try json_path_exists.append(self.alloc, .{
                .field = field,
                .path = path,
            });
            field_transferred = true;
            path_transferred = true;
            return;
        }
        if (self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;

        const column = maybe_column orelse return error.InvalidSqlCatalog;
        if (self.matchKeyword("is")) {
            const op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("not")) blk: {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            } else blk: {
                try self.expectKeyword("null");
                break :blk .is_null;
            };
            try predicates.append(self.alloc, .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = null,
            });
            field_transferred = true;
            return;
        }
        if (self.matchKeyword("not")) {
            try self.expectKeyword("in");
            if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
            const values_json = try self.parseSqlInValuesJsonAlloc();
            var values_transferred = false;
            errdefer if (!values_transferred) self.alloc.free(values_json);
            try in_predicates.append(self.alloc, .{
                .field = field,
                .values_json = values_json,
                .negated = true,
            });
            field_transferred = true;
            values_transferred = true;
            return;
        }
        if (self.matchKeyword("in")) {
            if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
            const values_json = try self.parseSqlInValuesJsonAlloc();
            var values_transferred = false;
            errdefer if (!values_transferred) self.alloc.free(values_json);
            try in_predicates.append(self.alloc, .{
                .field = field,
                .values_json = values_json,
                .negated = false,
            });
            field_transferred = true;
            values_transferred = true;
            return;
        }

        const op = try self.parseComparisonOp();
        if (op == .eq and self.matchKeyword("any")) {
            if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const values_json = try self.parseJsonArrayValueAlloc();
            var values_transferred = false;
            errdefer if (!values_transferred) self.alloc.free(values_json);
            try self.expect(.rparen);
            try in_predicates.append(self.alloc, .{
                .field = field,
                .values_json = values_json,
                .negated = negated,
            });
            field_transferred = true;
            values_transferred = true;
            return;
        }
        if (negated) return error.UnsupportedSqlShape;
        const value_json = try self.parseSqlColumnValueAlloc(column);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        if (column.field_type == .array) {
            if (op != .eq) return error.UnsupportedSqlShape;
            try validateJsonArray(self.alloc, value_json);
            try array_eq.append(self.alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        try predicates.append(self.alloc, .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn parseOrderBy(self: *@This(), order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder)) !void {
        while (true) {
            var order = try self.parseOrderExpressionAlloc();
            var order_transferred = false;
            errdefer if (!order_transferred) self.alloc.free(order.field);
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            order.direction = direction;
            try order_by.append(self.alloc, order);
            order_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseOrderExpressionAlloc(self: *@This()) !db_mod.types.RelationalRowsQueryOrder {
        if (self.match(.lparen) != null) {
            const field = try self.parseFieldExpressionOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try self.expectKeyword("is");
            const null_test: db_mod.types.RelationalRowsQueryOrderNullTest = if (self.matchKeyword("not")) blk: {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            } else blk: {
                try self.expectKeyword("null");
                break :blk .is_null;
            };
            try self.expect(.rparen);
            field_transferred = true;
            return .{ .field = field, .null_test = null_test };
        }

        const field = try self.parseFieldExpressionOwned();
        errdefer self.alloc.free(field);
        if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
        return .{ .field = field };
    }

    fn parseGroupBy(self: *@This(), group_by: *std.ArrayListUnmanaged([]const u8)) !void {
        while (true) {
            const field = try self.parseFieldExpressionOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try group_by.append(self.alloc, field);
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseAggregateOrderBy(
        self: *@This(),
        order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
        group_fields: []const []const u8,
        aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!aggregateOutputContainsField(group_fields, aggregations, field)) return error.UnsupportedSqlShape;
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            try order_by.append(self.alloc, .{ .field = field, .direction = direction });
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseAggregateHaving(
        self: *@This(),
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        group_fields: []const []const u8,
        aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!aggregateOutputContainsField(group_fields, aggregations, field)) return error.UnsupportedSqlShape;
            const op = if (self.matchKeyword("is")) blk: {
                const null_op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("not")) op_blk: {
                    try self.expectKeyword("null");
                    break :op_blk .is_not_null;
                } else op_blk: {
                    try self.expectKeyword("null");
                    break :op_blk .is_null;
                };
                break :blk null_op;
            } else try self.parseComparisonOp();
            const value_json = switch (op) {
                .is_null, .is_not_null => null,
                else => try self.parseJsonValueAlloc(),
            };
            var value_transferred = false;
            errdefer if (!value_transferred) if (value_json) |json| self.alloc.free(json);
            try predicates.append(self.alloc, .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseComparisonOp(self: *@This()) !runtime_schema.RelationalCheckOp {
        if (self.match(.eq) != null) return .eq;
        if (self.match(.neq) != null) return .ne;
        if (self.match(.gt) != null) return .gt;
        if (self.match(.gte) != null) return .gte;
        if (self.match(.lt) != null) return .lt;
        if (self.match(.lte) != null) return .lte;
        return error.UnsupportedSqlShape;
    }

    fn parseJsonValueAlloc(self: *@This()) ![]const u8 {
        if (self.matchKeyword("null")) return try self.alloc.dupe(u8, "null");
        if (self.matchKeyword("true")) return try self.alloc.dupe(u8, "true");
        if (self.matchKeyword("false")) return try self.alloc.dupe(u8, "false");
        if (self.match(.string)) |token| return try std.json.Stringify.valueAlloc(self.alloc, token.text, .{});
        if (self.match(.number)) |token| return try self.alloc.dupe(u8, token.text);
        if (self.match(.placeholder)) |token| return try self.boundValueJsonAlloc(token);
        return error.UnsupportedSqlShape;
    }

    fn parseJsonDocumentValueAlloc(self: *@This()) ![]const u8 {
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            return switch (value) {
                .json => |json| blk: {
                    try validateJsonDocument(self.alloc, json);
                    break :blk try self.alloc.dupe(u8, json);
                },
                else => error.UnsupportedSqlShape,
            };
        }
        if (self.match(.string)) |token| {
            try validateJsonDocument(self.alloc, token.text);
            return try self.alloc.dupe(u8, token.text);
        }
        return error.UnsupportedSqlShape;
    }

    fn appendJsonObjectConcatSetValues(
        self: *@This(),
        field: []const u8,
        json_set: *std.ArrayListUnmanaged(JsonSetValue),
    ) !void {
        const object_json = try self.parseJsonDocumentValueAlloc();
        defer self.alloc.free(object_json);
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, object_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (parsed.value != .object) return error.UnsupportedSqlShape;
        if (parsed.value.object.count() == 0) return error.UnsupportedSqlShape;

        var it = parsed.value.object.iterator();
        while (it.next()) |entry| {
            if (std.mem.indexOfScalar(u8, entry.key_ptr.*, '.') != null) return error.UnsupportedSqlShape;
            const owned_field = try self.alloc.dupe(u8, field);
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(owned_field);
            const path = try self.alloc.alloc([]const u8, 1);
            var path_transferred = false;
            errdefer if (!path_transferred) self.alloc.free(path);
            path[0] = try self.alloc.dupe(u8, entry.key_ptr.*);
            var path_item_transferred = false;
            errdefer if (!path_item_transferred) self.alloc.free(path[0]);
            const value_json = try std.json.Stringify.valueAlloc(self.alloc, entry.value_ptr.*, .{});
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try json_set.append(self.alloc, .{
                .field = owned_field,
                .path = path,
                .value_json = value_json,
            });
            field_transferred = true;
            path_transferred = true;
            path_item_transferred = true;
            value_transferred = true;
        }
    }

    fn parseJsonArrayValueAlloc(self: *@This()) ![]const u8 {
        const value_json = try self.parseJsonDocumentValueAlloc();
        errdefer self.alloc.free(value_json);
        try validateJsonArray(self.alloc, value_json);
        return value_json;
    }

    fn parseSqlInValuesJsonAlloc(self: *@This()) ![]const u8 {
        try self.expect(.lparen);
        if (self.peekKind(.rparen)) return error.UnsupportedSqlShape;

        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            if (self.match(.rparen) != null) {
                return switch (value) {
                    .json => |json| blk: {
                        try validateJsonArray(self.alloc, json);
                        break :blk try self.alloc.dupe(u8, json);
                    },
                    else => try self.singleValueJsonArrayAlloc(value),
                };
            }
            const first_json = try value.jsonAlloc(self.alloc);
            defer self.alloc.free(first_json);
            return try self.parseSqlInRemainingValuesJsonAlloc(first_json);
        }

        const first_json = try self.parseJsonValueAlloc();
        defer self.alloc.free(first_json);
        return try self.parseSqlInRemainingValuesJsonAlloc(first_json);
    }

    fn parseSqlInRemainingValuesJsonAlloc(self: *@This(), first_json: []const u8) ![]const u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('[');
        try writer.writeAll(first_json);
        while (self.match(.comma) != null) {
            const value_json = try self.parseJsonValueAlloc();
            defer self.alloc.free(value_json);
            try writer.writeByte(',');
            try writer.writeAll(value_json);
        }
        try self.expect(.rparen);
        try writer.writeByte(']');
        return try out.toOwnedSlice();
    }

    fn singleValueJsonArrayAlloc(self: *@This(), value: SqlValue) ![]const u8 {
        const value_json = try value.jsonAlloc(self.alloc);
        defer self.alloc.free(value_json);
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('[');
        try writer.writeAll(value_json);
        try writer.writeByte(']');
        return try out.toOwnedSlice();
    }

    fn parseSqlColumnValueAlloc(self: *@This(), column: runtime_schema.RelationalColumn) ![]const u8 {
        if (self.matchKeyword("default")) {
            const default_value = column.default_value orelse return error.UnsupportedSqlShape;
            return try relational_rows.relationalDefaultValueJsonAlloc(self.alloc, default_value);
        }
        if (self.peekKeyword("now")) {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
            return try self.parseNowValueJsonAlloc();
        }
        if (self.peekKeyword("convert_from")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            return try self.parseConvertFromJsonAlloc();
        }
        if (self.peekKeyword("jsonb_build_object")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            return try self.parseJsonbBuildObjectAlloc();
        }
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            if (column.field_type == .json) {
                return switch (value) {
                    .json => |json| try self.alloc.dupe(u8, json),
                    else => try value.jsonAlloc(self.alloc),
                };
            }
            return try value.jsonAlloc(self.alloc);
        }
        if (self.match(.string)) |token| {
            if (column.field_type == .json) {
                if (jsonValueIsValid(self.alloc, token.text)) return try self.alloc.dupe(u8, token.text);
            }
            return try std.json.Stringify.valueAlloc(self.alloc, token.text, .{});
        }
        if (self.matchKeyword("null")) return try self.alloc.dupe(u8, "null");
        if (self.matchKeyword("true")) return try self.alloc.dupe(u8, "true");
        if (self.matchKeyword("false")) return try self.alloc.dupe(u8, "false");
        if (self.match(.number)) |token| return try self.alloc.dupe(u8, token.text);
        return error.UnsupportedSqlShape;
    }

    fn parseNowValueJsonAlloc(self: *@This()) ![]const u8 {
        try self.expectKeyword("now");
        try self.expect(.lparen);
        try self.expect(.rparen);
        return try std.fmt.allocPrint(self.alloc, "{d}", .{platform_time.realtimeNs()});
    }

    fn parseConvertFromJsonAlloc(self: *@This()) ![]const u8 {
        try self.expectKeyword("convert_from");
        try self.expect(.lparen);
        const decoded = try self.parseConvertFromInputAlloc();
        defer self.alloc.free(decoded);
        try self.expect(.comma);
        const encoding = self.match(.string) orelse return error.UnsupportedSqlShape;
        if (!std.ascii.eqlIgnoreCase(encoding.text, "UTF8") and !std.ascii.eqlIgnoreCase(encoding.text, "UTF-8")) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        if (!jsonValueIsValid(self.alloc, decoded)) return error.UnsupportedSqlShape;
        return try self.alloc.dupe(u8, decoded);
    }

    fn parseConvertFromInputAlloc(self: *@This()) ![]const u8 {
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            return switch (value) {
                .string => |text| try self.alloc.dupe(u8, text),
                .json => |json| try self.alloc.dupe(u8, json),
                else => error.UnsupportedSqlShape,
            };
        }
        if (self.match(.string)) |token| return try self.alloc.dupe(u8, token.text);
        return error.UnsupportedSqlShape;
    }

    fn parseJsonbBuildObjectAlloc(self: *@This()) ![]const u8 {
        try self.expectKeyword("jsonb_build_object");
        try self.expect(.lparen);
        if (self.match(.rparen) != null) return try self.alloc.dupe(u8, "{}");

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(self.alloc);
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('{');
        var first = true;
        while (true) {
            const key_token = self.match(.string) orelse return error.UnsupportedSqlShape;
            const entry = try seen.getOrPut(self.alloc, key_token.text);
            if (entry.found_existing) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const value_json = try self.parseJsonValueAlloc();
            defer self.alloc.free(value_json);
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{f}:", .{std.json.fmt(key_token.text, .{})});
            try writer.writeAll(value_json);
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);
        try writer.writeByte('}');
        return try out.toOwnedSlice();
    }

    fn parseReturningListAlloc(self: *@This()) ![]const []const u8 {
        if (self.match(.star) != null) {
            const fields = try self.alloc.alloc([]const u8, 1);
            errdefer self.alloc.free(fields);
            fields[0] = try self.alloc.dupe(u8, "*");
            return fields;
        }

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(field);
            fields.deinit(self.alloc);
        }
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
            if (relationalColumnForReturningField(self.schema, field) == null) return error.InvalidSqlCatalog;
            try fields.append(self.alloc, field);
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
        return try fields.toOwnedSlice(self.alloc);
    }

    fn insertBodyJsonAlloc(self: *@This(), columns: []const []const u8, values: []const []const u8, conflict: ?ConflictClause, returning_fields: []const []const u8) ![]u8 {
        if (columns.len == 0 or columns.len != values.len) return error.UnsupportedSqlShape;
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"operations\":[{\"op\":\"insert\",\"row\":{");
        for (columns, values, 0..) |column, value_json, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(column, .{})});
            try writer.writeAll(value_json);
        }
        try writer.writeByte('}');
        if (conflict) |clause| {
            try writer.writeAll(",\"on_conflict\":{\"target\":");
            try self.writeConflictTargetJson(writer, clause.target);
            try writer.print(",\"action\":{f}", .{std.json.fmt(conflictActionToken(clause.action), .{})});
            if (clause.patch.len > 0) {
                try writer.writeAll(",\"patch\":{");
                for (clause.patch, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                    try writer.writeAll(item.value_json);
                }
                try writer.writeByte('}');
            }
            if (clause.increment.len > 0) {
                try writer.writeAll(",\"increment\":{");
                for (clause.increment, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                    try writer.writeAll(item.value_json);
                }
                try writer.writeByte('}');
            }
            if (clause.json_set.len > 0) {
                try writer.writeAll(",\"json_set\":[");
                for (clause.json_set, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{{\"field\":{f},\"path\":[", .{std.json.fmt(item.field, .{})});
                    for (item.path, 0..) |segment, segment_i| {
                        if (segment_i != 0) try writer.writeByte(',');
                        try writer.print("{f}", .{std.json.fmt(segment, .{})});
                    }
                    try writer.writeAll("],\"value\":");
                    try writer.writeAll(item.value_json);
                    try writer.writeByte('}');
                }
                try writer.writeByte(']');
            }
            try writer.writeByte('}');
        }
        try self.writeReturningJson(writer, returning_fields);
        try writer.writeAll("}]}");
        return try out.toOwnedSlice();
    }

    fn writeConflictTargetJson(self: *@This(), writer: *std.Io.Writer, target: ConflictTarget) !void {
        _ = self;
        switch (target) {
            .primary => try writer.writeAll("{\"primary\":true}"),
            .unique => |unique| {
                try writer.print("{{\"unique\":{{\"name\":{f}", .{std.json.fmt(unique.name, .{})});
                if (unique.where_json.len > 0) {
                    try writer.writeAll(",\"where\":");
                    try writer.writeAll(unique.where_json);
                }
                try writer.writeAll("}}");
            },
        }
    }

    fn parseU32Value(self: *@This()) !u32 {
        if (self.match(.number)) |token| {
            return try std.fmt.parseInt(u32, token.text, 10);
        }
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            return try value.asU32();
        }
        return error.UnsupportedSqlShape;
    }

    fn parseSelectItem(self: *@This()) !SelectItem {
        if (self.peekKeyword("array_length")) return .{ .array_length = try self.parseArrayLengthProjectionAlloc() };
        if (self.peekKeyword("coalesce")) return .{ .coalesce = try self.parseCoalesceProjectionAlloc() };

        const field = try self.parseFieldExpressionOwned();
        errdefer self.alloc.free(field);
        if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
        if (self.match(.arrow_text) != null) {
            if (relationalColumnForField(self.schema, field, .json) == null) return error.InvalidSqlCatalog;
            const path = try self.parseJsonPathOwned();
            errdefer self.alloc.free(path);
            const output = if (self.matchKeyword("as"))
                try self.parseIdentifierOwned()
            else
                try self.alloc.dupe(u8, path);
            errdefer self.alloc.free(output);
            return .{ .json_extract = .{
                .output = output,
                .field = field,
                .path = path,
                .as_text = true,
            } };
        }
        if (self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
        if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
        try self.consumeCompatibleProjectionAlias(field);
        return .{ .field = field };
    }

    fn parseArrayLengthProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsArrayLengthProjection {
        try self.expectKeyword("array_length");
        try self.expect(.lparen);
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, .array) orelse return error.InvalidSqlCatalog;
        _ = column;
        try self.expect(.comma);
        const dimension = try self.parseU32Value();
        if (dimension != 1) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "array_length");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);
        field_transferred = true;
        output_transferred = true;
        return .{ .output = output, .field = field };
    }

    fn parseCoalesceProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsCoalesceProjection {
        try self.expectKeyword("coalesce");
        try self.expect(.lparen);
        var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceOperand).empty;
        errdefer {
            for (operands.items) |operand| {
                switch (operand.kind) {
                    .field => if (operand.field.len > 0) self.alloc.free(operand.field),
                    .value => if (operand.value_json.len > 0) self.alloc.free(operand.value_json),
                }
            }
            operands.deinit(self.alloc);
        }
        while (true) {
            const operand = try self.parseCoalesceOperandAlloc();
            var operand_transferred = false;
            errdefer if (!operand_transferred) freeCoalesceOperand(self.alloc, operand);
            try operands.append(self.alloc, operand);
            operand_transferred = true;
            if (self.match(.comma) == null) break;
        }
        if (operands.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "coalesce");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);
        const owned_operands = try operands.toOwnedSlice(self.alloc);
        output_transferred = true;
        return .{ .output = output, .operands = owned_operands };
    }

    fn parseCoalesceOperandAlloc(self: *@This()) !db_mod.types.RelationalRowsCoalesceOperand {
        if (self.peekKind(.identifier) and
            !self.peekKeyword("null") and
            !self.peekKeyword("true") and
            !self.peekKeyword("false"))
        {
            const field = try self.parseFieldExpressionOwned();
            errdefer self.alloc.free(field);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            return .{ .kind = .field, .field = field };
        }

        const value_json = try self.parseJsonValueAlloc();
        errdefer self.alloc.free(value_json);
        return .{ .kind = .value, .value_json = value_json };
    }

    fn parseFieldExpressionOwned(self: *@This()) ![]const u8 {
        if (self.peekKeyword("lower") and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .lparen) {
            self.pos += 1;
            try self.expect(.lparen);
            const source = try self.parseIdentifierOwned();
            defer self.alloc.free(source);
            if (relationalColumnForField(self.schema, source, null) == null) return error.InvalidSqlCatalog;
            try self.expect(.rparen);
            const generated = generatedLowerColumnForField(self.schema, source) orelse return error.UnsupportedSqlShape;
            return try self.alloc.dupe(u8, generated.name);
        }
        return try self.parseIdentifierOwned();
    }

    fn consumeCompatibleProjectionAlias(self: *@This(), field: []const u8) !void {
        if (!self.matchKeyword("as")) return;
        const alias = try self.parseIdentifierOwned();
        defer self.alloc.free(alias);
        if (!std.mem.eql(u8, alias, field)) return error.UnsupportedSqlShape;
    }

    fn parseIdentifierOwned(self: *@This()) ![]const u8 {
        const token = self.match(.identifier) orelse return error.UnsupportedSqlShape;
        return try self.alloc.dupe(u8, token.text);
    }

    fn parseJsonPathOwned(self: *@This()) ![]const u8 {
        const token = self.match(.string) orelse return error.UnsupportedSqlShape;
        if (token.text.len == 0) return error.UnsupportedSqlShape;
        return try self.alloc.dupe(u8, token.text);
    }

    fn parsePostgresJsonPathAlloc(self: *@This()) ![]const []const u8 {
        const token = self.match(.string) orelse return error.UnsupportedSqlShape;
        if (token.text.len < 3 or token.text[0] != '{' or token.text[token.text.len - 1] != '}') return error.UnsupportedSqlShape;
        const inner = token.text[1 .. token.text.len - 1];
        var out = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (out.items) |segment| self.alloc.free(segment);
            out.deinit(self.alloc);
        }
        var parts = std.mem.splitScalar(u8, inner, ',');
        while (parts.next()) |part| {
            if (part.len == 0 or std.mem.indexOfScalar(u8, part, '.') != null) return error.UnsupportedSqlShape;
            const segment = try self.alloc.dupe(u8, part);
            var segment_transferred = false;
            errdefer if (!segment_transferred) self.alloc.free(segment);
            try out.append(self.alloc, segment);
            segment_transferred = true;
        }
        if (out.items.len == 0) return error.UnsupportedSqlShape;
        return try out.toOwnedSlice(self.alloc);
    }

    fn boundValueJsonAlloc(self: *@This(), token: Token) ![]const u8 {
        const value = try self.boundValue(token);
        return try value.jsonAlloc(self.alloc);
    }

    fn boundValue(self: *@This(), token: Token) !SqlValue {
        if (token.text.len < 2 or token.text[0] != '$') return error.UnsupportedSqlShape;
        var end: usize = 1;
        while (end < token.text.len and std.ascii.isDigit(token.text[end])) end += 1;
        if (end == 1) return error.UnsupportedSqlShape;
        const index = try std.fmt.parseInt(usize, token.text[1..end], 10);
        if (index == 0 or index > self.params.len) return error.MissingSqlParameter;
        return self.params[index - 1];
    }

    fn expectKeyword(self: *@This(), keyword: []const u8) !void {
        if (!self.matchKeyword(keyword)) return error.UnsupportedSqlShape;
    }

    fn expect(self: *@This(), kind: TokenKind) !void {
        if (self.match(kind) == null) return error.UnsupportedSqlShape;
    }

    fn matchKeyword(self: *@This(), keyword: []const u8) bool {
        if (self.pos >= self.tokens.len) return false;
        const token = self.tokens[self.pos];
        if (token.kind != .identifier) return false;
        if (!std.ascii.eqlIgnoreCase(token.text, keyword)) return false;
        self.pos += 1;
        return true;
    }

    fn peekKeyword(self: *@This(), keyword: []const u8) bool {
        if (self.pos >= self.tokens.len) return false;
        const token = self.tokens[self.pos];
        return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, keyword);
    }

    fn match(self: *@This(), kind: TokenKind) ?Token {
        if (self.pos >= self.tokens.len) return null;
        const token = self.tokens[self.pos];
        if (token.kind != kind) return null;
        self.pos += 1;
        return token;
    }

    fn peekKind(self: *@This(), kind: TokenKind) bool {
        return self.pos < self.tokens.len and self.tokens[self.pos].kind == kind;
    }

    fn atEnd(self: *@This()) bool {
        return self.pos >= self.tokens.len;
    }

    fn nextIsUnsupportedQueryKeyword(self: *@This()) bool {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].kind != .identifier) return false;
        const token = self.tokens[self.pos].text;
        return std.ascii.eqlIgnoreCase(token, "join") or
            std.ascii.eqlIgnoreCase(token, "left") or
            std.ascii.eqlIgnoreCase(token, "inner") or
            std.ascii.eqlIgnoreCase(token, "group") or
            std.ascii.eqlIgnoreCase(token, "with") or
            std.ascii.eqlIgnoreCase(token, "over") or
            std.ascii.eqlIgnoreCase(token, "lateral");
    }

    fn whereHasTopLevelOr(self: *@This()) bool {
        var depth: usize = 0;
        var i = self.pos;
        while (i < self.tokens.len) : (i += 1) {
            const token = self.tokens[i];
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => if (depth > 0) {
                    depth -= 1;
                },
                .semicolon => if (depth == 0) return false,
                .identifier => if (depth == 0) {
                    if (std.ascii.eqlIgnoreCase(token.text, "or")) return true;
                    if (tokenStartsWhereTailClause(token.text)) return false;
                },
                else => {},
            }
        }
        return false;
    }
};

fn tokenStartsWhereTailClause(token: []const u8) bool {
    return std.ascii.eqlIgnoreCase(token, "order") or
        std.ascii.eqlIgnoreCase(token, "limit") or
        std.ascii.eqlIgnoreCase(token, "offset") or
        std.ascii.eqlIgnoreCase(token, "for") or
        std.ascii.eqlIgnoreCase(token, "group") or
        std.ascii.eqlIgnoreCase(token, "having") or
        std.ascii.eqlIgnoreCase(token, "join") or
        std.ascii.eqlIgnoreCase(token, "left") or
        std.ascii.eqlIgnoreCase(token, "inner") or
        std.ascii.eqlIgnoreCase(token, "with") or
        std.ascii.eqlIgnoreCase(token, "over") or
        std.ascii.eqlIgnoreCase(token, "lateral");
}

fn findCteByName(ctes: []const db_mod.types.RelationalRowsCte, name: []const u8) ?db_mod.types.RelationalRowsCte {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

fn tokenizeAlloc(alloc: std.mem.Allocator, sql: []const u8) !std.ArrayListUnmanaged(Token) {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    errdefer freeTokens(alloc, &tokens);

    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (std.ascii.isWhitespace(ch)) {
            i += 1;
            continue;
        }
        if (std.ascii.isAlphabetic(ch) or ch == '_') {
            const start = i;
            i += 1;
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '.')) i += 1;
            const end = i;
            i = skipSqlCast(sql, i);
            try tokens.append(alloc, .{ .kind = .identifier, .text = sql[start..end] });
            continue;
        }
        if (ch == '"') {
            const start = i + 1;
            i += 1;
            while (i < sql.len and sql[i] != '"') i += 1;
            if (i >= sql.len) return error.UnsupportedSqlShape;
            try tokens.append(alloc, .{ .kind = .identifier, .text = sql[start..i] });
            i += 1;
            i = skipSqlCast(sql, i);
            continue;
        }
        if (ch == '\'') {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        try out.append(alloc, '\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                try out.append(alloc, sql[i]);
                i += 1;
            }
            if (i >= sql.len) return error.UnsupportedSqlShape;
            const owned = try out.toOwnedSlice(alloc);
            errdefer alloc.free(owned);
            i += 1;
            i = skipSqlCast(sql, i);
            try tokens.append(alloc, .{ .kind = .string, .text = owned });
            continue;
        }
        if (std.ascii.isDigit(ch)) {
            const start = i;
            i += 1;
            while (i < sql.len and (std.ascii.isDigit(sql[i]) or sql[i] == '.')) i += 1;
            try tokens.append(alloc, .{ .kind = .number, .text = sql[start..i] });
            continue;
        }
        if (ch == '$') {
            const start = i;
            i += 1;
            while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
            if (i == start + 1) return error.UnsupportedSqlShape;
            if (i + 1 < sql.len and sql[i] == ':' and sql[i + 1] == ':') {
                i += 2;
                while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '[' or sql[i] == ']')) i += 1;
            }
            try tokens.append(alloc, .{ .kind = .placeholder, .text = sql[start..i] });
            continue;
        }
        switch (ch) {
            ',' => {
                try tokens.append(alloc, .{ .kind = .comma, .text = sql[i .. i + 1] });
                i += 1;
            },
            '*' => {
                try tokens.append(alloc, .{ .kind = .star, .text = sql[i .. i + 1] });
                i += 1;
            },
            '+' => {
                try tokens.append(alloc, .{ .kind = .plus, .text = sql[i .. i + 1] });
                i += 1;
            },
            '(' => {
                try tokens.append(alloc, .{ .kind = .lparen, .text = sql[i .. i + 1] });
                i += 1;
            },
            ')' => {
                try tokens.append(alloc, .{ .kind = .rparen, .text = sql[i .. i + 1] });
                i += 1;
                i = skipSqlCast(sql, i);
            },
            '@' => {
                if (i + 1 >= sql.len or sql[i + 1] != '>') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .at_contains, .text = sql[i .. i + 2] });
                i += 2;
            },
            '|' => {
                if (i + 1 >= sql.len or sql[i + 1] != '|') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .pipe_concat, .text = sql[i .. i + 2] });
                i += 2;
            },
            '?' => {
                try tokens.append(alloc, .{ .kind = .question, .text = sql[i .. i + 1] });
                i += 1;
            },
            ';' => {
                try tokens.append(alloc, .{ .kind = .semicolon, .text = sql[i .. i + 1] });
                i += 1;
            },
            '=' => {
                try tokens.append(alloc, .{ .kind = .eq, .text = sql[i .. i + 1] });
                i += 1;
            },
            '!' => {
                if (i + 1 >= sql.len or sql[i + 1] != '=') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                i += 2;
            },
            '<' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .lte, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .lt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '>' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .gte, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .gt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '-' => {
                if (i + 1 < sql.len and sql[i + 1] == '>' and i + 2 < sql.len and sql[i + 2] == '>') {
                    try tokens.append(alloc, .{ .kind = .arrow_text, .text = sql[i .. i + 3] });
                    i += 3;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .arrow_json, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .minus, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            else => return error.UnsupportedSqlShape,
        }
    }
    return tokens;
}

fn skipSqlCast(sql: []const u8, start: usize) usize {
    var i = start;
    if (i + 1 >= sql.len or sql[i] != ':' or sql[i + 1] != ':') return i;
    i += 2;
    while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '[' or sql[i] == ']')) i += 1;
    return i;
}

fn freeTokens(alloc: std.mem.Allocator, tokens: *std.ArrayListUnmanaged(Token)) void {
    for (tokens.items) |token| {
        if (token.kind == .string) alloc.free(token.text);
    }
    tokens.deinit(alloc);
}

fn relationalColumnForField(schema: runtime_schema.TableSchema, field: []const u8, expected_type: ?runtime_schema.AntflyType) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (!std.mem.eql(u8, column.name, field)) continue;
        if (expected_type) |field_type| {
            if (column.field_type != field_type) return null;
        }
        return column;
    }
    return null;
}

fn generatedLowerColumnForField(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        const generated = column.generated orelse continue;
        if (generated.op != .lower) continue;
        const generated_field = generated.field orelse continue;
        if (std.mem.eql(u8, generated_field, field)) return column;
    }
    return null;
}

fn relationalColumnForReturningField(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    if (relationalColumnForField(schema, field, null)) |column| return column;
    const dot_index = std.mem.indexOfScalar(u8, field, '.') orelse return null;
    if (dot_index == 0 or dot_index + 1 >= field.len) return null;
    return relationalColumnForField(schema, field[0..dot_index], .json);
}

fn findUniqueConstraintByColumns(schema: runtime_schema.TableSchema, columns: []const []const u8, require_partial: bool) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.expressions.len != 0) continue;
        if (require_partial and constraint.where.len == 0) continue;
        if (!require_partial and constraint.where.len != 0) continue;
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn findUniqueConstraintByLowerExpression(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.columns.len != 0 or constraint.expressions.len != 1) continue;
        const expression = constraint.expressions[0];
        if (expression.op == .lower and std.mem.eql(u8, expression.field, field)) return constraint;
    }
    return null;
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn columnsMatchPrimaryKey(primary_key: runtime_schema.PrimaryKey, columns: []const []const u8) bool {
    return stringSlicesEqual(primary_key.columns, columns);
}

fn primaryKeyContains(primary_key: runtime_schema.PrimaryKey, field: []const u8) bool {
    for (primary_key.columns) |column| {
        if (std.mem.eql(u8, column, field)) return true;
    }
    return false;
}

fn aggregateOpForName(name: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "array_agg")) return .array_agg;
    return null;
}

fn aggregateOpName(op: db_mod.types.RelationalRowsAggregateOp) []const u8 {
    return switch (op) {
        .count => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .array_agg => "array_agg",
    };
}

fn validateAggregateGroupBy(group_fields: []const []const u8, group_by: []const []const u8) !void {
    if (!stringSlicesEqual(group_fields, group_by)) return error.UnsupportedSqlShape;
}

fn aggregateOutputContainsField(
    group_fields: []const []const u8,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) bool {
    for (group_fields) |group_field| {
        if (std.mem.eql(u8, group_field, field)) return true;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, field)) return true;
    }
    return false;
}

fn joinSideForQualifier(
    qualifier: []const u8,
    left_alias: []const u8,
    right_alias: []const u8,
) !db_mod.types.RelationalRowsJoinProjectionSide {
    if (std.mem.eql(u8, qualifier, left_alias)) return .left;
    if (std.mem.eql(u8, qualifier, right_alias)) return .right;
    return error.UnsupportedSqlShape;
}

fn joinProjectionContainsOutput(select: []const db_mod.types.RelationalRowsJoinProjection, field: []const u8) bool {
    for (select) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return true;
    }
    return false;
}

fn conflictActionToken(action: Parser.ConflictAction) []const u8 {
    return switch (action) {
        .nothing => "nothing",
        .update => "update",
    };
}

fn uniquePredicateOpToken(op: runtime_schema.UniquePredicateOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
    };
}

fn validateUniqueWhereJsonMatches(alloc: std.mem.Allocator, where_json: []const u8, predicates: []const runtime_schema.UniquePredicate) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const all_value = parsed.value.object.get("all") orelse return error.UnsupportedSqlShape;
    if (all_value != .array or all_value.array.items.len != predicates.len) return error.UnsupportedSqlShape;
    for (all_value.array.items, predicates) |item, predicate| {
        if (item != .object) return error.UnsupportedSqlShape;
        const field_value = item.object.get("field") orelse return error.UnsupportedSqlShape;
        const op_value = item.object.get("op") orelse return error.UnsupportedSqlShape;
        if (field_value != .string or !std.mem.eql(u8, field_value.string, predicate.field)) return error.UnsupportedSqlShape;
        if (op_value != .string or !std.mem.eql(u8, op_value.string, uniquePredicateOpToken(predicate.op))) return error.UnsupportedSqlShape;
        const supplied_value = item.object.get("value");
        if (predicate.value_json) |expected_json| {
            const supplied = supplied_value orelse return error.UnsupportedSqlShape;
            const supplied_json = try std.json.Stringify.valueAlloc(alloc, supplied, .{});
            defer alloc.free(supplied_json);
            if (!std.mem.eql(u8, supplied_json, expected_json)) return error.UnsupportedSqlShape;
        } else if (supplied_value != null) {
            return error.UnsupportedSqlShape;
        }
    }
}

fn updateWillLookupExistingRow(schema: runtime_schema.TableSchema, returning_fields: []const []const u8) bool {
    if (returning_fields.len > 0 or schema.checks.len > 0) return true;
    for (schema.relational_columns) |column| {
        if (column.generated != null) return true;
    }
    return false;
}

fn validateJsonDocument(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    switch (parsed.value) {
        .object, .array => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn validateJsonArray(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
}

fn jsonValueIsValid(alloc: std.mem.Allocator, value: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return false;
    parsed.deinit();
    return true;
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn freeFieldJsonValues(alloc: std.mem.Allocator, values: []const Parser.FieldJsonValue) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeFieldPredicates(alloc: std.mem.Allocator, values: []const Parser.FieldPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        if (value.value_json) |json| alloc.free(json);
    }
}

fn freeTableAlias(alloc: std.mem.Allocator, value: Parser.TableAlias) void {
    alloc.free(value.name);
    alloc.free(value.alias);
}

fn freeQualifiedField(alloc: std.mem.Allocator, value: Parser.QualifiedField) void {
    alloc.free(value.qualifier);
    alloc.free(value.field);
}

fn freeQualifiedProjections(alloc: std.mem.Allocator, values: []const Parser.QualifiedProjection) void {
    for (values) |value| {
        freeQualifiedField(alloc, value.source);
        alloc.free(value.output);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeAggregateSpec(alloc: std.mem.Allocator, spec: db_mod.types.RelationalRowsAggregateSpec) void {
    alloc.free(spec.name);
    if (spec.field) |field| alloc.free(field);
    freeOrderBy(alloc, spec.array_order_by);
    if (spec.array_order_by.len > 0) alloc.free(spec.array_order_by);
    freeRelationalChecks(alloc, spec.filter_predicates);
    if (spec.filter_predicates.len > 0) alloc.free(spec.filter_predicates);
}

fn freeAggregateSpecs(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsAggregateSpec) void {
    for (values) |value| freeAggregateSpec(alloc, value);
}

fn freeJsonSetValues(alloc: std.mem.Allocator, values: []const Parser.JsonSetValue) void {
    for (values) |value| {
        alloc.free(value.field);
        freeStringSlice(alloc, value.path);
        alloc.free(value.value_json);
    }
}

fn freeArrayContains(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayContainsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeArrayEq(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayEqPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeInPredicates(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsInPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.values_json);
    }
}

fn freeConflictTarget(alloc: std.mem.Allocator, target: Parser.ConflictTarget) void {
    switch (target) {
        .primary => {},
        .unique => |unique| {
            alloc.free(unique.name);
            if (unique.where_json.len > 0) alloc.free(unique.where_json);
        },
    }
}

fn freeConflictClause(alloc: std.mem.Allocator, clause: Parser.ConflictClause) void {
    freeConflictTarget(alloc, clause.target);
    freeFieldJsonValues(alloc, clause.patch);
    if (clause.patch.len > 0) alloc.free(clause.patch);
    freeFieldJsonValues(alloc, clause.increment);
    if (clause.increment.len > 0) alloc.free(clause.increment);
    freeJsonSetValues(alloc, clause.json_set);
    if (clause.json_set.len > 0) alloc.free(clause.json_set);
}

fn freeJoinOn(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJoinOn) void {
    for (values) |value| {
        alloc.free(value.left_field);
        alloc.free(value.right_field);
    }
}

fn freeJoinProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJoinProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
}

fn freeSelectItem(alloc: std.mem.Allocator, item: Parser.SelectItem) void {
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
    }
}

fn freeRelationalCheck(alloc: std.mem.Allocator, value: runtime_schema.RelationalCheck) void {
    alloc.free(value.field);
    if (value.value_json) |json| alloc.free(json);
}

fn freeRelationalChecks(alloc: std.mem.Allocator, values: []const runtime_schema.RelationalCheck) void {
    for (values) |value| freeRelationalCheck(alloc, value);
}

fn freePredicateGroups(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsPredicateGroup) void {
    for (values) |value| {
        freeRelationalChecks(alloc, value.predicates);
        if (value.predicates.len > 0) alloc.free(value.predicates);
    }
}

fn freeJsonContains(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonContainsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeJsonPathEq(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonPathEqPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.path);
        alloc.free(value.value_json);
    }
}

fn freeJsonPathExists(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonPathExistsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.path);
    }
}

fn freeJsonExtract(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonExtractProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
        alloc.free(value.path);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeArrayLengthProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayLengthProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeCoalesceOperand(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsCoalesceOperand) void {
    switch (value.kind) {
        .field => if (value.field.len > 0) alloc.free(value.field),
        .value => if (value.value_json.len > 0) alloc.free(value.value_json),
    }
}

fn freeCoalesceProjection(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsCoalesceProjection) void {
    alloc.free(value.output);
    for (value.operands) |operand| freeCoalesceOperand(alloc, operand);
    if (value.operands.len > 0) alloc.free(value.operands);
}

fn freeCoalesceProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsCoalesceProjection) void {
    for (values) |value| freeCoalesceProjection(alloc, value);
    if (values.len > 0) alloc.free(values);
}

fn freeOrderBy(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsQueryOrder) void {
    for (values) |value| alloc.free(value.field);
}

test "postgres sql adapter lowers colony queue select into row claim query" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"billing_cycle_start":{"type":"datetime"},"metric_type":{"type":"keyword"},"bucket_start":{"type":"datetime"},"created_at":{"type":"datetime"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id, metric_type FROM usage_records WHERE status = $1 ORDER BY billing_cycle_start ASC, metric_type ASC, bucket_start ASC, id ASC LIMIT $2 FOR UPDATE SKIP LOCKED",
        schema,
        &.{ .{ .string = "unrated" }, .{ .integer = 100 } },
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.predicates[0].op);
    try std.testing.expectEqualStrings("\"unrated\"", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 4), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("billing_cycle_start", lowered.query.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 100), lowered.query.limit.?);
    try std.testing.expect(lowered.query.row_claim != null);
    try std.testing.expect(lowered.query.row_claim.?.skip_locked);
}

test "postgres sql adapter lowers json text extraction predicate" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"},"created_at":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE metadata->>'source' = 'autoscale_delta' ORDER BY created_at DESC LIMIT 1",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_path_eq.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_path_eq[0].field);
    try std.testing.expectEqualStrings("source", lowered.query.json_path_eq[0].path);
    try std.testing.expectEqualStrings("\"autoscale_delta\"", lowered.query.json_path_eq[0].value_json);
    try std.testing.expectEqualStrings("created_at", lowered.query.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.query.order_by[0].direction);
}

test "postgres sql adapter lowers jsonb containment existence and extraction projection" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"},"created_at":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT metadata->>'source' AS source FROM usage_records WHERE metadata @> $1::jsonb AND metadata ? 'flags' ORDER BY created_at DESC LIMIT 5",
        schema,
        &.{.{ .json = "{\"billing\":{\"plan\":\"pro\"}}" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_extract.len);
    try std.testing.expectEqualStrings("source", lowered.query.json_extract[0].output);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_extract[0].field);
    try std.testing.expectEqualStrings("source", lowered.query.json_extract[0].path);
    try std.testing.expect(lowered.query.json_extract[0].as_text);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_contains.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_contains[0].field);
    try std.testing.expectEqualStrings("{\"billing\":{\"plan\":\"pro\"}}", lowered.query.json_contains[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_path_exists.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_path_exists[0].field);
    try std.testing.expectEqualStrings("flags", lowered.query.json_path_exists[0].path);
    try std.testing.expectEqual(@as(u32, 5), lowered.query.limit.?);
}

test "postgres sql adapter accepts casted jsonb document literals" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE metadata @> '{\"source\":\"autoscale_delta\"}'::jsonb",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_contains.len);
    try std.testing.expectEqualStrings("{\"source\":\"autoscale_delta\"}", lowered.query.json_contains[0].value_json);
}

test "postgres sql adapter lowers array containment and equality predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var contains = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE tags @> $1::text[]",
        schema,
        &.{.{ .json = "[\"hot\",\"new\"]" }},
    );
    defer contains.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), contains.query.array_contains.len);
    try std.testing.expectEqualStrings("tags", contains.query.array_contains[0].field);
    try std.testing.expectEqualStrings("[\"hot\",\"new\"]", contains.query.array_contains[0].value_json);

    var eq = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE tags = $1::text[]",
        schema,
        &.{.{ .json = "[\"hot\"]" }},
    );
    defer eq.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), eq.query.array_eq.len);
    try std.testing.expectEqualStrings("tags", eq.query.array_eq[0].field);
    try std.testing.expectEqualStrings("[\"hot\"]", eq.query.array_eq[0].value_json);
}

test "postgres sql adapter lowers array_length projection" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id, array_length(tags, 1) AS tag_count FROM usage_records ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.array_length.len);
    try std.testing.expectEqualStrings("tag_count", lowered.query.array_length[0].output);
    try std.testing.expectEqualStrings("tags", lowered.query.array_length[0].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("id", lowered.query.order_by[0].field);
}

test "postgres sql adapter lowers coalesce projection" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"display_name":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id, COALESCE(display_name, email, 'unknown') AS name_or_email FROM users ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.coalesce.len);
    try std.testing.expectEqualStrings("name_or_email", lowered.query.coalesce[0].output);
    try std.testing.expectEqual(@as(usize, 3), lowered.query.coalesce[0].operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsCoalesceOperandKind.field, lowered.query.coalesce[0].operands[0].kind);
    try std.testing.expectEqualStrings("display_name", lowered.query.coalesce[0].operands[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsCoalesceOperandKind.field, lowered.query.coalesce[0].operands[1].kind);
    try std.testing.expectEqualStrings("email", lowered.query.coalesce[0].operands[1].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsCoalesceOperandKind.value, lowered.query.coalesce[0].operands[2].kind);
    try std.testing.expectEqualStrings("\"unknown\"", lowered.query.coalesce[0].operands[2].value_json);

    try std.testing.expectError(error.InvalidSqlCatalog, lowerSelectAlloc(
        alloc,
        "SELECT COALESCE(missing, 'unknown') AS display FROM users",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers scalar any predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = ANY($1::text[])",
        schema,
        &.{.{ .json = "[\"active\",\"pending\"]" }},
    );
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.in_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"active\",\"pending\"]", lowered.query.in_predicates[0].values_json);
    try std.testing.expect(!lowered.query.in_predicates[0].negated);

    var negated = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE NOT (status = ANY($1::text[]))",
        schema,
        &.{.{ .json = "[\"disabled\"]" }},
    );
    defer negated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), negated.query.in_predicates.len);
    try std.testing.expectEqualStrings("status", negated.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"disabled\"]", negated.query.in_predicates[0].values_json);
    try std.testing.expect(negated.query.in_predicates[0].negated);
}

test "postgres sql adapter lowers scalar in predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"priority":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var literal = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN ('active', 'pending')",
        schema,
        &.{},
    );
    defer literal.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), literal.query.in_predicates.len);
    try std.testing.expectEqualStrings("status", literal.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"active\",\"pending\"]", literal.query.in_predicates[0].values_json);
    try std.testing.expect(!literal.query.in_predicates[0].negated);

    var array_param = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN ($1::text[])",
        schema,
        &.{.{ .json = "[\"active\",\"pending\"]" }},
    );
    defer array_param.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), array_param.query.in_predicates.len);
    try std.testing.expectEqualStrings("[\"active\",\"pending\"]", array_param.query.in_predicates[0].values_json);

    var negated = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE priority NOT IN (1, 2, $1)",
        schema,
        &.{.{ .integer = 3 }},
    );
    defer negated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), negated.query.in_predicates.len);
    try std.testing.expectEqualStrings("priority", negated.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[1,2,3]", negated.query.in_predicates[0].values_json);
    try std.testing.expect(negated.query.in_predicates[0].negated);
}

test "postgres sql adapter lowers scalar or predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'closed' OR status = 'open' AND amount > 20 ORDER BY created_at DESC",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.predicates.len);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.or_predicates[0].predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.or_predicates[0].predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.or_predicates[0].predicates[0].op);
    try std.testing.expectEqualStrings("\"closed\"", lowered.query.or_predicates[0].predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.or_predicates[1].predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.or_predicates[1].predicates[0].field);
    try std.testing.expectEqualStrings("amount", lowered.query.or_predicates[1].predicates[1].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.query.or_predicates[1].predicates[1].op);
    try std.testing.expectEqualStrings("20", lowered.query.or_predicates[1].predicates[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN ('closed') OR status = 'open'",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers null-test order expressions" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"expires_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM jobs ORDER BY (expires_at IS NULL), expires_at ASC, id ASC LIMIT 5",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("expires_at", lowered.query.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderNullTest.is_null, lowered.query.order_by[0].null_test.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.asc, lowered.query.order_by[0].direction);
    try std.testing.expectEqualStrings("expires_at", lowered.query.order_by[1].field);
    try std.testing.expect(lowered.query.order_by[1].null_test == null);
    try std.testing.expectEqualStrings("id", lowered.query.order_by[2].field);
    try std.testing.expectEqual(@as(u32, 5), lowered.query.limit.?);
}

test "postgres sql adapter lowers now in scalar predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"created_at_ns":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE created_at_ns <= NOW() ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("created_at_ns", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.lte, lowered.query.predicates[0].op);
    const now_value = try std.fmt.parseInt(u64, lowered.query.predicates[0].value_json.?, 10);
    try std.testing.expect(now_value > 0);
}

test "postgres sql adapter lowers grouped aggregate queries" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(*) AS order_count, SUM(amount) AS amount_sum, AVG(amount) AS amount_avg FROM usage_records WHERE status = $1 GROUP BY customer HAVING amount_sum > 10 ORDER BY amount_sum DESC LIMIT 10",
        schema,
        &.{.{ .string = "open" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.group_by.len);
    try std.testing.expectEqualStrings("customer", lowered.aggregate.group_by[0]);
    try std.testing.expectEqual(@as(usize, 3), lowered.aggregate.aggregations.len);
    try std.testing.expectEqualStrings("order_count", lowered.aggregate.aggregations[0].name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].field == null);
    try std.testing.expectEqualStrings("amount_sum", lowered.aggregate.aggregations[1].name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.avg, lowered.aggregate.aggregations[2].op);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.source.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.source.predicates[0].field);
    try std.testing.expectEqualStrings("\"open\"", lowered.aggregate.source.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.having_predicates.len);
    try std.testing.expectEqualStrings("amount_sum", lowered.aggregate.having_predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.aggregate.having_predicates[0].op);
    try std.testing.expectEqualStrings("10", lowered.aggregate.having_predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.order_by.len);
    try std.testing.expectEqualStrings("amount_sum", lowered.aggregate.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.aggregate.order_by[0].direction);
    try std.testing.expectEqual(@as(u32, 10), lowered.aggregate.limit.?);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(*) AS order_count FROM usage_records GROUP BY customer HAVING missing_alias > 0",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers filtered aggregate predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count, SUM(amount) FILTER (WHERE status = 'open' AND amount > 10) AS open_amount_sum FROM usage_records GROUP BY customer ORDER BY open_amount_sum DESC LIMIT 10",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[0].filter_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[0].filter_predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.aggregate.aggregations[0].filter_predicates[0].op);
    try std.testing.expectEqualStrings("\"open\"", lowered.aggregate.aggregations[0].filter_predicates[0].value_json.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.aggregate.aggregations[1].filter_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[1].filter_predicates[0].field);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].filter_predicates[1].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.aggregate.aggregations[1].filter_predicates[1].op);
    try std.testing.expectEqualStrings("10", lowered.aggregate.aggregations[1].filter_predicates[1].value_json.?);
}

test "postgres sql adapter lowers distinct aggregate specs" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(DISTINCT status) AS status_count, SUM(DISTINCT amount) FILTER (WHERE status = 'open') AS open_amount_sum FROM usage_records GROUP BY customer",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].distinct);
    try std.testing.expectEqual(db_mod.types.default_relational_rows_aggregate_distinct_max_items, lowered.aggregate.aggregations[0].distinct_max_items);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[0].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expect(lowered.aggregate.aggregations[1].distinct);
    try std.testing.expectEqual(db_mod.types.default_relational_rows_aggregate_distinct_max_items, lowered.aggregate.aggregations[1].distinct_max_items);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[1].filter_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[1].filter_predicates[0].field);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateAlloc(
        alloc,
        "SELECT COUNT(DISTINCT *) AS row_count FROM usage_records",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers bounded array aggregate specs" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"},"metadata":{"type":"json"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) FILTER (WHERE amount > 10) AS statuses FROM usage_records GROUP BY customer",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].distinct);
    try std.testing.expectEqual(db_mod.types.default_relational_rows_aggregate_distinct_max_items, lowered.aggregate.aggregations[0].distinct_max_items);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[0].field.?);
    try std.testing.expectEqual(default_array_agg_max_items, lowered.aggregate.aggregations[0].array_max_items);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[0].array_order_by.len);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[0].array_order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.aggregate.aggregations[0].array_order_by[0].direction);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[0].filter_predicates.len);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[0].filter_predicates[0].field);

    try std.testing.expectError(error.InvalidSqlCatalog, lowerAggregateAlloc(
        alloc,
        "SELECT customer, ARRAY_AGG(metadata) AS metadata_values FROM usage_records GROUP BY customer",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers global aggregate queries" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT COUNT(*) AS row_count, MIN(amount) AS min_amount, MAX(amount) AS max_amount FROM usage_records",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.aggregate.group_by.len);
    try std.testing.expectEqual(@as(usize, 3), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.min, lowered.aggregate.aggregations[1].op);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.max, lowered.aggregate.aggregations[2].op);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateAlloc(
        alloc,
        "SELECT id, COUNT(*) AS row_count FROM usage_records",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers equality join queries" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerJoinAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name, o.amount AS amount FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' ORDER BY amount DESC LIMIT 5",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
    try std.testing.expectEqualStrings("usage_records", lowered.right_table_name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinType.left, lowered.join.join_type);
    try std.testing.expectEqual(@as(usize, 2), lowered.join.on.len);
    try std.testing.expectEqualStrings("tenant", lowered.join.on[0].left_field);
    try std.testing.expectEqualStrings("tenant", lowered.join.on[0].right_field);
    try std.testing.expectEqualStrings("customer_id", lowered.join.on[1].left_field);
    try std.testing.expectEqualStrings("id", lowered.join.on[1].right_field);
    try std.testing.expectEqual(@as(usize, 1), lowered.join.left.predicates.len);
    try std.testing.expectEqualStrings("kind", lowered.join.left.predicates[0].field);
    try std.testing.expectEqualStrings("\"order\"", lowered.join.left.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.join.right.predicates.len);
    try std.testing.expectEqualStrings("kind", lowered.join.right.predicates[0].field);
    try std.testing.expectEqualStrings("\"customer\"", lowered.join.right.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 3), lowered.join.select.len);
    try std.testing.expectEqualStrings("order_id", lowered.join.select[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, lowered.join.select[0].side);
    try std.testing.expectEqualStrings("id", lowered.join.select[0].field);
    try std.testing.expectEqualStrings("customer_name", lowered.join.select[1].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.right, lowered.join.select[1].side);
    try std.testing.expectEqualStrings("name", lowered.join.select[1].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.join.order_by.len);
    try std.testing.expectEqualStrings("amount", lowered.join.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.join.order_by[0].direction);
    try std.testing.expectEqual(@as(u32, 5), lowered.join.limit.?);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerJoinAlloc(
        alloc,
        "SELECT o.id AS order_id FROM usage_records AS o JOIN usage_records AS c ON o.tenant = o.id",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers generated lower expressions for query pushdown" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_key":{"type":"keyword","generated":{"op":"lower","field":"email"}}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT lower(email) AS email_key FROM users WHERE lower(email) = $1 ORDER BY lower(email) ASC",
        schema,
        &.{.{ .string = "ada@example.test" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.select.len);
    try std.testing.expectEqualStrings("email_key", lowered.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("email_key", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.predicates[0].op);
    try std.testing.expectEqualStrings("\"ada@example.test\"", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("email_key", lowered.query.order_by[0].field);
}

test "postgres sql adapter ignores harmless identifier casts" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT \"id\"::text FROM users WHERE id::text = $1 ORDER BY \"status\"::text DESC",
        schema,
        &.{.{ .string = "u1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("id", lowered.query.predicates[0].field);
    try std.testing.expectEqualStrings("\"u1\"", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("status", lowered.query.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.query.order_by[0].direction);
}

test "postgres sql adapter rejects lower predicate without generated column" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM users WHERE lower(email) = $1",
        schema,
        &.{.{ .string = "ada@example.test" }},
    ));
}

test "postgres sql adapter lowers insert values returning into row batch" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, metadata) VALUES ($1, $2, $3::jsonb) RETURNING id, status, metadata",
        schema,
        &.{
            .{ .string = "u1" },
            .{ .string = "pending" },
            .{ .json = "{\"source\":\"autoscale_delta\"}" },
        },
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);

    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("u1", returned.value.object.get("id").?.string);
    try std.testing.expectEqualStrings("pending", returned.value.object.get("status").?.string);
    try std.testing.expectEqualStrings("autoscale_delta", returned.value.object.get("metadata").?.object.get("source").?.string);
}

test "postgres sql adapter lowers insert jsonb literal" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', '{\"source\":\"literal\"}'::jsonb) RETURNING *",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("literal", returned.value.object.get("metadata").?.object.get("source").?.string);
}

test "postgres sql adapter lowers jsonb_build_object insert values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', jsonb_build_object('source', $1, 'count', 3, 'active', true)) RETURNING metadata",
        schema,
        &.{.{ .string = "builder" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    const metadata = returned.value.object.get("metadata").?.object;
    try std.testing.expectEqualStrings("builder", metadata.get("source").?.string);
    try std.testing.expectEqual(@as(i64, 3), metadata.get("count").?.integer);
    try std.testing.expect(metadata.get("active").?.bool);
}

test "postgres sql adapter lowers convert_from jsonb insert values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', convert_from($1, 'UTF8')::jsonb) RETURNING metadata",
        schema,
        &.{.{ .string = "{\"source\":\"converted\",\"count\":4}" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"converted\",\"count\":4}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers now insert values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"created_at_ns":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, created_at_ns) VALUES ('u1', NOW()) RETURNING created_at_ns",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("created_at_ns").?) {
        .integer => |value| try std.testing.expect(value > 0),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers explicit default insert values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"},"created_at_ns":{"type":"numeric","x-antfly-default":{"op":"now_ns"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, created_at_ns) VALUES ('u1', DEFAULT, DEFAULT) RETURNING status, created_at_ns",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("active", returned.value.object.get("status").?.string);
    switch (returned.value.object.get("created_at_ns").?) {
        .integer => |value| try std.testing.expect(value > 0),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter rejects default without column default" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', DEFAULT)",
        schema,
        &.{},
    ));
}

const TestPrimaryResolver = struct {
    row_json: []const u8,
    version: u64,
    exists: bool = true,

    fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolve,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolve(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8 {
        _ = table_name;
        _ = constraint_name;
        _ = encoded_value;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.exists) return null;
        return try alloc.dupe(u8, "test-existing-primary");
    }

    fn primaryExists(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!bool {
        _ = alloc;
        _ = table_name;
        _ = physical_key;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.exists;
    }

    fn lookupPrimary(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!?relational_rows.ResolvedPrimaryRow {
        _ = table_name;
        if (physical_key.len == 0) return null;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .json = try alloc.dupe(u8, self.row_json),
            .version = self.version,
        };
    }
};

test "postgres sql adapter lowers update patch with explicit version predicate" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"pending\",\"metadata\":{\"billing\":{\"plan\":\"free\"}}}", .version = 42 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET status = $1 WHERE id = $2",
        schema,
        &.{ .{ .string = "active" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 42), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers arithmetic updates into typed increments" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"amount\":5}", .version = 21 };

    var plus = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET amount = amount + $1 WHERE id = $2 RETURNING amount",
        schema,
        &.{ .{ .integer = 2 }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer plus.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), plus.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), plus.batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, plus.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", plus.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("2", plus.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 21), plus.batch.predicates[0].expected_version);
    var plus_returned = try std.json.parseFromSlice(std.json.Value, alloc, plus.batch.returning_rows[0], .{});
    defer plus_returned.deinit();
    switch (plus_returned.value.object.get("amount").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 7), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 7), value),
        else => return error.TestUnexpectedResult,
    }

    var minus = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET amount = amount - 1 WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer minus.deinit(alloc);

    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, minus.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", minus.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("-1", minus.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 21), minus.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers now update values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"updated_at_ns":{"type":"numeric"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"name\":\"old\",\"updated_at_ns\":1}", .version = 22 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE users SET updated_at_ns = NOW() WHERE id = $1 RETURNING updated_at_ns",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("updated_at_ns", lowered.batch.transforms[0].operations[0].path);
    const planned_now = try std.fmt.parseInt(u64, lowered.batch.transforms[0].operations[0].value_json.?, 10);
    try std.testing.expect(planned_now > 0);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("updated_at_ns").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, @intCast(planned_now)), value),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(u64, 22), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers explicit default update values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"disabled\"}", .version = 23 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE users SET status = DEFAULT WHERE id = $1 RETURNING status",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"status\":\"active\"}", lowered.batch.returning_rows[0]);
    try std.testing.expectEqual(@as(u64, 23), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers jsonb_build_object update value" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"source\":\"old\"}}", .version = 6 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = jsonb_build_object('source', $1, 'nested', $2::jsonb) WHERE id = $3 RETURNING metadata",
        schema,
        &.{ .{ .string = "builder" }, .{ .json = "{\"plan\":\"pro\"}" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("metadata", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"source\":\"builder\",\"nested\":{\"plan\":\"pro\"}}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"builder\",\"nested\":{\"plan\":\"pro\"}}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers convert_from jsonb update value" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"source\":\"old\"}}", .version = 6 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = convert_from($1, 'UTF-8')::jsonb WHERE id = $2 RETURNING metadata",
        schema,
        &.{ .{ .string = "{\"source\":\"converted\",\"active\":true}" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("metadata", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"source\":\"converted\",\"active\":true}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"converted\",\"active\":true}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers update jsonb_set returning through row batch" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"billing\":{\"plan\":\"free\"}}}", .version = 9 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = jsonb_set(metadata, '{billing,plan}', $1, true) WHERE id = $2 RETURNING metadata.billing.plan",
        schema,
        &.{ .{ .string = "pro" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("metadata.billing.plan", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pro\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 9), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"metadata.billing.plan\":\"pro\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers update jsonb concat into json set operations" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"billing\":{\"plan\":\"free\"},\"source\":\"old\"}}", .version = 10 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = metadata || '{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"rated\"]}'::jsonb WHERE id = $1 RETURNING metadata.billing.plan, metadata.flags",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 2), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("metadata.billing", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"plan\":\"pro\"}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("metadata.flags", lowered.batch.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("[\"rated\"]", lowered.batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"metadata.billing.plan\":\"pro\",\"metadata.flags\":[\"rated\"]}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers delete with explicit version predicate" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"disabled\"}", .version = 7 };

    var lowered = try lowerDeleteAlloc(
        alloc,
        "DELETE FROM usage_records WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.deleted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.deletes.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 7), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers on conflict primary do nothing" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 12 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING *",
        schema,
        &.{ .{ .string = "u1" }, .{ .string = "pending" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 0), lowered.batch.writes.len);
    try std.testing.expectEqual(@as(usize, 0), lowered.batch.returning_rows.len);
}

test "postgres sql adapter lowers on conflict primary do update with excluded values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 12 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', 'pending') ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms.len);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pending\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 12), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"status\":\"pending\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers excluded explicit default values" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 14 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', DEFAULT) ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 14), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"status\":\"active\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers on conflict unique do update" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"existing\"}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, status) VALUES ('u2', 'a@example.test', 'pending') ON CONFLICT (email) DO UPDATE SET status = excluded.status RETURNING id, status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pending\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 8), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"pending\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers on conflict arithmetic update" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"amount\":5}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, amount) VALUES ('u2', 'a@example.test', 1) ON CONFLICT (email) DO UPDATE SET amount = amount + 3 RETURNING amount",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, lowered.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("3", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 8), lowered.batch.predicates[0].expected_version);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("amount").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 8), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 8), value),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers on conflict jsonb concat update" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"metadata\":{\"source\":\"old\"}}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, metadata) VALUES ('u2', 'a@example.test', '{\"source\":\"insert\"}'::jsonb) ON CONFLICT (email) DO UPDATE SET metadata = metadata || '{\"source\":\"conflict\",\"flags\":[\"seen\"]}'::jsonb RETURNING metadata.source, metadata.flags",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 2), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("metadata.source", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"conflict\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("metadata.flags", lowered.batch.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("[\"seen\"]", lowered.batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata.source\":\"conflict\",\"metadata.flags\":[\"seen\"]}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers on conflict jsonb_build_object update" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"metadata\":{\"source\":\"old\"}}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, metadata) VALUES ('u2', 'a@example.test', '{\"source\":\"insert\"}'::jsonb) ON CONFLICT (email) DO UPDATE SET metadata = jsonb_build_object('source', 'conflict', 'count', $1) RETURNING metadata",
        schema,
        &.{.{ .integer = 2 }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("metadata", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"source\":\"conflict\",\"count\":2}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"conflict\",\"count\":2}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers partial unique conflict target predicates" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"active\",\"name\":\"old\"}", .version = 11 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, status, name) VALUES ('u2', 'a@example.test', 'active', 'new') ON CONFLICT (email) WHERE status = 'active' DO UPDATE SET name = excluded.name RETURNING id, name",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("name", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"new\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 11), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"name\":\"new\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers lower expression unique conflict target" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_lower_email_key","expressions":[{"op":"lower","field":"email"}]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"name\":\"old\"}", .version = 13 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO users (id, email, name) VALUES ('u2', 'A@EXAMPLE.TEST', 'new') ON CONFLICT (lower(email)) DO UPDATE SET name = excluded.name RETURNING id, name",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("name", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"new\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 13), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"name\":\"new\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers non recursive cte query plans" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerQueryPlanAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, status, amount, created_at FROM orders WHERE status = 'open'), expensive_open_orders AS (SELECT id, amount, created_at FROM open_orders WHERE amount > 10) SELECT id FROM expensive_open_orders ORDER BY created_at DESC LIMIT 2",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("orders", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.plan.ctes.len);
    try std.testing.expectEqualStrings("open_orders", lowered.plan.ctes[0].name);
    try std.testing.expectEqualStrings("", lowered.plan.ctes[0].query.source_cte);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes[0].query.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.plan.ctes[0].query.predicates[0].field);
    try std.testing.expectEqualStrings("\"open\"", lowered.plan.ctes[0].query.predicates[0].value_json.?);
    try std.testing.expectEqualStrings("expensive_open_orders", lowered.plan.ctes[1].name);
    try std.testing.expectEqualStrings("open_orders", lowered.plan.ctes[1].query.source_cte);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes[1].query.predicates.len);
    try std.testing.expectEqualStrings("amount", lowered.plan.ctes[1].query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.plan.ctes[1].query.predicates[0].op);
    try std.testing.expectEqualStrings("10", lowered.plan.ctes[1].query.predicates[0].value_json.?);
    try std.testing.expectEqualStrings("expensive_open_orders", lowered.plan.query.source_cte);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.plan.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.order_by.len);
    try std.testing.expectEqualStrings("created_at", lowered.plan.query.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 2), lowered.plan.query.limit.?);

    var plain = try lowerQueryPlanAlloc(
        alloc,
        "SELECT id FROM orders WHERE status = 'open'",
        schema,
        &.{},
    );
    defer plain.deinit(alloc);
    try std.testing.expectEqualStrings("orders", plain.table_name);
    try std.testing.expectEqual(@as(usize, 0), plain.plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), plain.plan.query.predicates.len);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerQueryPlanAlloc(
        alloc,
        "WITH early AS (SELECT id FROM later), later AS (SELECT id FROM orders) SELECT id FROM early",
        schema,
        &.{},
    ));
}

test "postgres sql adapter rejects unsupported colony shapes explicitly" {
    const schema_api = @import("../schema/mod.zig");
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"organization_id\":\"o1\"}", .version = 3 };

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "WITH membership AS (SELECT id FROM users) SELECT id FROM membership",
        schema,
        &.{},
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT row_number() OVER (ORDER BY id) FROM users",
        schema,
        &.{},
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM users LEFT JOIN organizations ON users.organization_id = organizations.id",
        schema,
        &.{},
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerUpdateAlloc(
        alloc,
        "UPDATE users SET id = 'u2' WHERE id = 'u1'",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerUpdateAlloc(
        alloc,
        "UPDATE users SET organization_id = 'o2' WHERE organization_id = 'o1'",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDeleteAlloc(
        alloc,
        "DELETE FROM users WHERE organization_id = 'o1'",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO users (id, organization_id) VALUES ('u1', 'o1') ON CONFLICT (upper(organization_id)) DO NOTHING",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
}
