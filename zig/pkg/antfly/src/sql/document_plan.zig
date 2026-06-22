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

const runtime_schema = @import("../storage/schema.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");

const Token = token_mod.Token;

const ParsedDocumentWhere = struct {
    ids: std.ArrayListUnmanaged([]const u8) = .empty,
    filter_clauses: std.ArrayListUnmanaged([]const u8) = .empty,
    full_text_query: ?[]const u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ids.items) |id| alloc.free(@constCast(id));
        self.ids.deinit(alloc);
        for (self.filter_clauses.items) |clause| alloc.free(@constCast(clause));
        self.filter_clauses.deinit(alloc);
        if (self.full_text_query) |query| alloc.free(@constCast(query));
        self.* = undefined;
    }
};

pub const DocumentProjectionKind = enum {
    id,
    doc,
    field,
};

pub const DocumentProjection = struct {
    kind: DocumentProjectionKind,
    field: []const u8 = "",
    output: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        alloc.free(@constCast(self.output));
        self.* = undefined;
    }
};

pub const DocumentIndexQuery = struct {
    full_text_query: ?[]const u8 = null,
    filter_query_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.full_text_query) |query| alloc.free(@constCast(query));
        if (self.filter_query_json) |query| alloc.free(@constCast(query));
        self.* = undefined;
    }
};

pub const DocumentAggregateOp = enum {
    count,
};

pub const DocumentAggregateGroupBy = struct {
    field: []const u8,
    field_type: runtime_schema.AntflyType,
    output: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        if (self.output.len > 0) alloc.free(@constCast(self.output));
        self.* = undefined;
    }
};

pub const DocumentAggregateSpec = struct {
    op: DocumentAggregateOp,
    output: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.output.len > 0) alloc.free(@constCast(self.output));
        self.* = undefined;
    }
};

pub const DocumentAlgebraicAggregatePlan = struct {
    table_name: []const u8,
    filter_query_json: ?[]const u8 = null,
    group_by: ?DocumentAggregateGroupBy = null,
    aggregate: DocumentAggregateSpec,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.filter_query_json) |filter| alloc.free(@constCast(filter));
        if (self.group_by) |*group_by| group_by.deinit(alloc);
        self.aggregate.deinit(alloc);
        self.* = undefined;
    }
};

pub const DocumentOrderDirection = enum {
    asc,
    desc,
};

pub const DocumentOrderBy = struct {
    field: []const u8,
    field_type: runtime_schema.AntflyType,
    direction: DocumentOrderDirection = .asc,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        self.* = undefined;
    }
};

pub const DocumentProducer = union(enum) {
    id_lookup: []const []const u8,
    indexed_query: DocumentIndexQuery,
    bounded_scan: u32,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .id_lookup => |ids| {
                for (ids) |id| alloc.free(@constCast(id));
                if (ids.len > 0) alloc.free(ids);
            },
            .indexed_query => |*query| query.deinit(alloc),
            .bounded_scan => {},
        }
        self.* = undefined;
    }
};

pub const DocumentReadPlan = struct {
    table_name: []const u8,
    projection: []DocumentProjection,
    producer: DocumentProducer,
    order_by: ?DocumentOrderBy = null,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.projection) |*projection| projection.deinit(alloc);
        if (self.projection.len > 0) alloc.free(self.projection);
        self.producer.deinit(alloc);
        if (self.order_by) |*order_by| order_by.deinit(alloc);
        self.* = undefined;
    }
};

pub fn lowerDocumentReadPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
) !DocumentReadPlan {
    if (schema.storage_mode != .document) return error.InvalidSqlCatalog;
    if (parsed_sql.statement.readKind() != .query) return error.UnsupportedSqlShape;

    const tokens = parsed_sql.items();
    if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;

    const from_index = findTopLevelKeyword(tokens, .from) orelse return error.UnsupportedSqlShape;
    const where_index = findTopLevelKeyword(tokens, .where);
    const order_index = findTopLevelKeyword(tokens, .order);
    const limit_index = findTopLevelKeyword(tokens, .limit);
    if (findTopLevelKeyword(tokens, .join) != null or
        findTopLevelKeyword(tokens, .group) != null or
        findTopLevelKeyword(tokens, .having) != null or
        findTopLevelKeyword(tokens, .window) != null)
    {
        return error.UnsupportedSqlShape;
    }

    if (from_index + 1 >= tokens.len or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    errdefer alloc.free(table_name);

    const tail_start = minOptionalIndex(where_index, order_index, limit_index) orelse tokens.len;
    try validateFromTail(tokens[from_index + 2 .. tail_start]);

    const projection = try parseProjectionAlloc(alloc, tokens[1..from_index], schema);
    errdefer freeProjection(alloc, projection);

    const limit = if (limit_index) |idx| try parseLimit(tokens, idx) else null;
    const order_by = if (order_index) |idx|
        try parseOrderByAlloc(alloc, tokens, idx, limit_index orelse tokens.len, schema)
    else
        null;
    errdefer if (order_by) |*order| {
        var mutable = order.*;
        mutable.deinit(alloc);
    };
    const producer = if (where_index) |idx|
        try parseWhereProducerAlloc(alloc, tokens, idx, order_index orelse limit_index orelse tokens.len, schema)
    else blk: {
        const bounded = limit orelse return error.DocumentSqlRequiresBoundedScan;
        break :blk DocumentProducer{ .bounded_scan = bounded };
    };
    errdefer {
        var mutable = producer;
        mutable.deinit(alloc);
    }
    if (order_by != null) switch (producer) {
        .indexed_query => return error.UnsupportedSqlShape,
        else => {},
    };

    return .{
        .table_name = table_name,
        .projection = projection,
        .producer = producer,
        .order_by = order_by,
        .limit = limit,
    };
}

pub fn lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
) !DocumentAlgebraicAggregatePlan {
    if (schema.storage_mode != .document) return error.InvalidSqlCatalog;
    if (parsed_sql.statement.readKind() != .aggregate) return error.UnsupportedSqlShape;

    const tokens = parsed_sql.items();
    if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;

    const from_index = findTopLevelKeyword(tokens, .from) orelse return error.UnsupportedSqlShape;
    const where_index = findTopLevelKeyword(tokens, .where);
    const group_index = findTopLevelKeyword(tokens, .group);
    const having_index = findTopLevelKeyword(tokens, .having);
    const order_index = findTopLevelKeyword(tokens, .order);
    const limit_index = findTopLevelKeyword(tokens, .limit);
    if (having_index != null or order_index != null or
        findTopLevelKeyword(tokens, .join) != null or
        findTopLevelKeyword(tokens, .window) != null)
    {
        return error.UnsupportedSqlShape;
    }

    if (from_index + 1 >= tokens.len or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    errdefer alloc.free(table_name);

    const tail_start = minOptionalIndex(where_index, group_index, limit_index) orelse tokens.len;
    try validateFromTail(tokens[from_index + 2 .. tail_start]);

    var aggregate = try parseDocumentAggregateSpecAlloc(alloc, tokens[1..from_index]);
    errdefer aggregate.deinit(alloc);

    const limit = if (limit_index) |idx| try parseLimit(tokens, idx) else null;
    var group_by = if (group_index) |idx|
        try parseDocumentAggregateGroupByAlloc(alloc, tokens, idx, limit_index orelse tokens.len, schema)
    else
        null;
    errdefer if (group_by) |*group| group.deinit(alloc);

    const filter_query_json: ?[]const u8 = if (where_index) |idx| blk: {
        var producer = try parseWhereProducerAlloc(alloc, tokens, idx, group_index orelse limit_index orelse tokens.len, schema);
        defer producer.deinit(alloc);
        break :blk switch (producer) {
            .indexed_query => |query| try documentAggregateFilterFromIndexQueryAlloc(alloc, query),
            else => return error.UnsupportedSqlShape,
        };
    } else null;
    errdefer if (filter_query_json) |filter| alloc.free(@constCast(filter));

    return .{
        .table_name = table_name,
        .filter_query_json = filter_query_json,
        .group_by = group_by,
        .aggregate = aggregate,
        .limit = limit,
    };
}

fn parseProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) ![]DocumentProjection {
    if (tokens.len == 0) return error.UnsupportedSqlShape;
    if (tokens.len == 1 and tokens[0].kind == .star) return try selectAllProjectionAlloc(alloc, schema);

    var out = std.ArrayListUnmanaged(DocumentProjection).empty;
    errdefer {
        for (out.items) |*projection| projection.deinit(alloc);
        out.deinit(alloc);
    }

    var start: usize = 0;
    while (start < tokens.len) {
        const comma = findComma(tokens, start) orelse tokens.len;
        if (comma == start) return error.UnsupportedSqlShape;
        try out.append(alloc, try parseProjectionItemAlloc(alloc, tokens[start..comma], schema));
        start = comma + 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn selectAllProjectionAlloc(alloc: std.mem.Allocator, schema: runtime_schema.TableSchema) ![]DocumentProjection {
    var out = try alloc.alloc(DocumentProjection, schema.relational_columns.len + 1);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*projection| projection.deinit(alloc);
        alloc.free(out);
    }
    out[initialized] = .{
        .kind = .id,
        .output = try alloc.dupe(u8, "_id"),
    };
    initialized += 1;
    for (schema.relational_columns) |column| {
        out[initialized] = .{
            .kind = .field,
            .field = try alloc.dupe(u8, column.name),
            .output = try alloc.dupe(u8, column.name),
        };
        initialized += 1;
    }
    return out;
}

fn parseProjectionItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) !DocumentProjection {
    if (tokens.len == 0) return error.UnsupportedSqlShape;
    const aliased = try splitProjectionAlias(tokens);
    const expression = aliased.expression;
    if (expression.len == 0 or expression[0].kind != .identifier) return error.UnsupportedSqlShape;

    if (try parseJsonPathProjectionItemAlloc(alloc, expression, aliased.output, schema)) |projection| return projection;

    var output: ?[]const u8 = null;
    if (expression.len != 1) return error.UnsupportedSqlShape;
    output = aliased.output;

    const field = expression[0].text;
    if (std.mem.eql(u8, field, "_id")) {
        return .{ .kind = .id, .output = try alloc.dupe(u8, output orelse "_id") };
    }
    if (std.mem.eql(u8, field, "_doc")) {
        return .{ .kind = .doc, .output = try alloc.dupe(u8, output orelse "_doc") };
    }
    const column = documentFieldColumn(schema, field) orelse return error.InvalidSqlCatalog;
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, column.name),
        .output = try alloc.dupe(u8, output orelse field),
    };
}

const ProjectionAliasSplit = struct {
    expression: []const Token,
    output: ?[]const u8 = null,
};

fn splitProjectionAlias(tokens: []const Token) !ProjectionAliasSplit {
    if (tokens.len >= 3 and tokens[tokens.len - 2].matchesKeywordTag(.as) and tokens[tokens.len - 1].kind == .identifier) {
        return .{ .expression = tokens[0 .. tokens.len - 2], .output = tokens[tokens.len - 1].text };
    }
    if (tokens.len == 2 and tokens[0].kind == .identifier and tokens[1].kind == .identifier) {
        return .{ .expression = tokens[0..1], .output = tokens[1].text };
    }
    if (tokens.len >= 4 and tokens[tokens.len - 1].kind == .identifier and !documentJsonArrowKind(tokens[tokens.len - 2].kind)) {
        return .{ .expression = tokens[0 .. tokens.len - 1], .output = tokens[tokens.len - 1].text };
    }
    return .{ .expression = tokens };
}

fn parseJsonPathProjectionItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    output: ?[]const u8,
    schema: runtime_schema.TableSchema,
) !?DocumentProjection {
    var expression = (try parseDocumentJsonPathExpressionAlloc(alloc, tokens, schema)) orelse return null;
    errdefer expression.deinit(alloc);
    const owned_output = try alloc.dupe(u8, output orelse expression.last_segment);
    errdefer alloc.free(owned_output);
    return .{
        .kind = .field,
        .field = expression.takePath(),
        .output = owned_output,
    };
}

fn documentJsonArrowKind(kind: token_mod.TokenKind) bool {
    return kind == .arrow_json or kind == .arrow_text or kind == .path_arrow_json or kind == .path_arrow_text;
}

fn documentJsonPathArrowKind(kind: token_mod.TokenKind) bool {
    return kind == .path_arrow_json or kind == .path_arrow_text;
}

const DocumentJsonPathExpression = struct {
    root_column: runtime_schema.RelationalColumn,
    path: []u8,
    last_segment: []const u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }

    fn takePath(self: *@This()) []u8 {
        const path = self.path;
        self.path = "";
        return path;
    }
};

fn parseDocumentJsonPathExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) !?DocumentJsonPathExpression {
    if (tokens.len < 3 or tokens[0].kind != .identifier or !documentJsonArrowKind(tokens[1].kind)) return null;
    const root = tokens[0].text;
    if (std.mem.eql(u8, root, "_id") or std.mem.eql(u8, root, "_doc")) return error.UnsupportedSqlShape;
    const column = documentFieldColumn(schema, root) orelse return error.InvalidSqlCatalog;
    var path = try documentFilterPathAlloc(alloc, column.path);
    errdefer alloc.free(path);

    var last_segment: []const u8 = root;
    var pos: usize = 1;
    while (pos < tokens.len) {
        if (pos + 1 >= tokens.len or !documentJsonArrowKind(tokens[pos].kind)) return error.UnsupportedSqlShape;
        const segment_token = tokens[pos + 1];
        if (segment_token.kind != .string and segment_token.kind != .identifier) return error.UnsupportedSqlShape;
        if (documentJsonPathArrowKind(tokens[pos].kind)) {
            last_segment = try appendDocumentJsonPathLiteralAlloc(alloc, &path, segment_token.text);
        } else {
            try appendDocumentJsonPathSegmentAlloc(alloc, &path, segment_token.text);
            last_segment = segment_token.text;
        }
        pos += 2;
    }

    return .{
        .root_column = column,
        .path = path,
        .last_segment = last_segment,
    };
}

fn appendDocumentJsonPathLiteralAlloc(alloc: std.mem.Allocator, path: *[]u8, literal: []const u8) ![]const u8 {
    if (literal.len == 0) return error.UnsupportedSqlShape;
    const body = if (literal.len >= 2 and literal[0] == '{' and literal[literal.len - 1] == '}')
        literal[1 .. literal.len - 1]
    else
        literal;
    var parts = std.mem.splitScalar(u8, body, ',');
    var count: usize = 0;
    var last_segment: []const u8 = "";
    while (parts.next()) |part| {
        try appendDocumentJsonPathSegmentAlloc(alloc, path, part);
        last_segment = part;
        count += 1;
    }
    if (count == 0) return error.UnsupportedSqlShape;
    return last_segment;
}

fn appendDocumentJsonPathSegmentAlloc(alloc: std.mem.Allocator, path: *[]u8, segment: []const u8) !void {
    if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '/') != null) return error.UnsupportedSqlShape;
    const next = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ path.*, segment });
    alloc.free(path.*);
    path.* = next;
}

fn parseWhereProducerAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    where_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
) !DocumentProducer {
    const where_tokens = tokens[where_index + 1 .. end_index];
    if (where_tokens.len == 0) return error.UnsupportedSqlShape;

    var parsed = ParsedDocumentWhere{};
    errdefer parsed.deinit(alloc);

    var start: usize = 0;
    while (start < where_tokens.len) {
        const end = findTopLevelAnd(where_tokens, start) orelse where_tokens.len;
        if (end == start) return error.UnsupportedSqlShape;
        try parseWhereClauseIntoAlloc(alloc, where_tokens[start..end], schema, &parsed);
        start = end + 1;
    }

    if (parsed.ids.items.len > 0 and (parsed.full_text_query != null or parsed.filter_clauses.items.len > 0)) {
        return error.UnsupportedSqlShape;
    }
    if (parsed.ids.items.len > 0) {
        const ids = try parsed.ids.toOwnedSlice(alloc);
        parsed.ids = .empty;
        return .{ .id_lookup = ids };
    }
    if (parsed.full_text_query != null or parsed.filter_clauses.items.len > 0) {
        const filter_query_json = try buildConjunctiveFilterJsonAlloc(alloc, parsed.filter_clauses.items);
        const full_text_query = parsed.full_text_query;
        parsed.full_text_query = null;
        parsed.deinit(alloc);
        return .{ .indexed_query = .{
            .full_text_query = full_text_query,
            .filter_query_json = filter_query_json,
        } };
    }

    return error.UnsupportedSqlShape;
}

fn parseOrderByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    order_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
) !DocumentOrderBy {
    if (order_index + 2 >= end_index) return error.UnsupportedSqlShape;
    if (!tokens[order_index + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
    const order_tokens = tokens[order_index + 2 .. end_index];
    if (order_tokens.len == 0) return error.UnsupportedSqlShape;
    if (findComma(order_tokens, 0) != null) return error.UnsupportedSqlShape;

    const direction: DocumentOrderDirection = if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.desc))
        .desc
    else if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.asc))
        .asc
    else
        .asc;
    const expression = if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.desc) or order_tokens[order_tokens.len - 1].matchesKeywordTag(.asc))
        order_tokens[0 .. order_tokens.len - 1]
    else
        order_tokens;
    if (expression.len == 0) return error.UnsupportedSqlShape;
    if (expression.len == 1 and expression[0].kind == .identifier and std.mem.eql(u8, expression[0].text, "_id")) {
        return .{
            .field = try alloc.dupe(u8, "_id"),
            .field_type = .keyword,
            .direction = direction,
        };
    }

    var field = (try documentOrderFieldForExpressionAlloc(alloc, expression, schema)) orelse return error.UnsupportedSqlShape;
    errdefer field.deinit(alloc);
    return .{
        .field = field.takePath(),
        .field_type = field.field_type,
        .direction = direction,
    };
}

fn parseDocumentAggregateSpecAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !DocumentAggregateSpec {
    const aliased = try splitProjectionAlias(tokens);
    const expression = aliased.expression;
    if (expression.len != 4 or
        !expression[0].matchesKeywordTag(.count) or
        expression[1].kind != .lparen or
        expression[2].kind != .star or
        expression[3].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    return .{
        .op = .count,
        .output = try alloc.dupe(u8, aliased.output orelse "count"),
    };
}

fn parseDocumentAggregateGroupByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    group_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
) !DocumentAggregateGroupBy {
    if (group_index + 2 >= end_index) return error.UnsupportedSqlShape;
    if (!tokens[group_index + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
    const group_tokens = tokens[group_index + 2 .. end_index];
    if (group_tokens.len == 0) return error.UnsupportedSqlShape;
    if (findComma(group_tokens, 0) != null) return error.UnsupportedSqlShape;

    var field = (try documentAggregateFieldForExpressionAlloc(alloc, group_tokens, schema)) orelse return error.UnsupportedSqlShape;
    errdefer field.deinit(alloc);
    const output = try alloc.dupe(u8, documentAggregateOutputName(group_tokens));
    errdefer alloc.free(output);
    return .{
        .field = field.takePath(),
        .field_type = field.field_type,
        .output = output,
    };
}

fn documentAggregateOutputName(tokens: []const Token) []const u8 {
    if (tokens.len == 1 and tokens[0].kind == .identifier) return tokens[0].text;
    if (tokens.len > 0 and tokens[tokens.len - 1].kind == .string) return tokens[tokens.len - 1].text;
    return "group";
}

fn documentAggregateFilterFromIndexQueryAlloc(alloc: std.mem.Allocator, query: DocumentIndexQuery) !?[]const u8 {
    if (query.full_text_query != null) return error.UnsupportedSqlShape;
    if (query.filter_query_json) |filter| return try alloc.dupe(u8, filter);
    return null;
}

fn parseWhereClauseIntoAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    out: *ParsedDocumentWhere,
) !void {
    if (try parseFullTextQueryAlloc(alloc, tokens)) |query| {
        if (out.full_text_query != null) {
            alloc.free(query);
            return error.UnsupportedSqlShape;
        }
        out.full_text_query = query;
        return;
    }
    if (try parseScalarFilterClauseAlloc(alloc, tokens, schema)) |clause| {
        errdefer alloc.free(clause);
        try out.filter_clauses.append(alloc, clause);
        return;
    }
    if (tokens.len == 3 and tokens[0].kind == .identifier and std.mem.eql(u8, tokens[0].text, "_id") and tokens[1].kind == .eq) {
        const id = try documentIdLiteralAlloc(alloc, tokens[2]);
        errdefer alloc.free(id);
        try out.ids.append(alloc, id);
        return;
    }
    if (tokens.len >= 5 and tokens[0].kind == .identifier and std.mem.eql(u8, tokens[0].text, "_id") and tokens[1].matchesKeywordTag(.in)) {
        try parseDocumentIdInListIntoAlloc(alloc, tokens[2..], out);
        return;
    }
    return error.UnsupportedSqlShape;
}

fn parseFullTextQueryAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?[]const u8 {
    if (tokens.len != 4) return null;
    if (!tokens[0].matchesQualifiedKeywordTag("antfly", .full_text_search)) return null;
    if (tokens[1].kind != .lparen or tokens[2].kind != .string or tokens[3].kind != .rparen) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, tokens[2].text);
}

fn parseScalarFilterClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) !?[]const u8 {
    if (tokens.len < 3) return null;
    const op_index = findTopLevelScalarFilterOperator(tokens) orelse return null;
    if (op_index == 0) return null;
    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens[0..op_index], schema)) orelse return null;
    defer field.deinit(alloc);
    if (tokens.len == op_index + 2 and tokens[op_index].kind == .eq) {
        const value_json = try tokenLiteralJsonAlloc(alloc, tokens[op_index + 1]);
        defer alloc.free(value_json);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"term\":{{\"path\":{f},\"value\":{s}}}}}",
            .{ std.json.fmt(field.path, .{}), value_json },
        );
    }
    if (tokens.len >= op_index + 4 and tokens[op_index].matchesKeywordTag(.in)) {
        const values_json = try tokenLiteralListJsonAlloc(alloc, tokens[op_index + 1 ..]);
        defer alloc.free(values_json);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"terms\":{{\"path\":{f},\"values\":{s}}}}}",
            .{ std.json.fmt(field.path, .{}), values_json },
        );
    }
    if (tokens.len == op_index + 2) {
        return try buildRangeFilterClauseAlloc(alloc, field, tokens[op_index], tokens[op_index + 1]);
    }
    return null;
}

const DocumentRangeBound = struct {
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

fn buildRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    field: DocumentFilterField,
    operator: Token,
    value: Token,
) !?[]const u8 {
    if (!field.exact_declared_path) return error.DocumentSqlIndexUnavailable;
    const bound = documentRangeBound(operator.kind, value.text) orelse return null;
    return switch (field.field_type) {
        .numeric => try buildNumericRangeFilterClauseAlloc(alloc, field.path, bound, value),
        .datetime => try buildDateRangeFilterClauseAlloc(alloc, field.path, bound, value),
        .keyword, .text, .search_as_you_type => try buildTermRangeFilterClauseAlloc(alloc, field.path, bound, value),
        else => error.UnsupportedSqlShape,
    };
}

fn documentRangeBound(kind: token_mod.TokenKind, value: []const u8) ?DocumentRangeBound {
    return switch (kind) {
        .gt => .{ .min = value, .inclusive_min = false },
        .gte => .{ .min = value, .inclusive_min = true },
        .lt => .{ .max = value, .inclusive_max = false },
        .lte => .{ .max = value, .inclusive_max = true },
        else => null,
    };
}

fn buildNumericRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .number) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"numeric_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"min\":{s}", .{min});
    if (bound.max) |max| try writer.print(",\"max\":{s}", .{max});
    if (bound.min != null) try writer.print(",\"inclusive_min\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_max\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

fn buildDateRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .string) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"date_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"start\":{f}", .{std.json.fmt(min, .{})});
    if (bound.max) |max| try writer.print(",\"end\":{f}", .{std.json.fmt(max, .{})});
    if (bound.min != null) try writer.print(",\"inclusive_start\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_end\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

fn buildTermRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .string and value.kind != .identifier) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"term_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"min\":{f}", .{std.json.fmt(min, .{})});
    if (bound.max) |max| try writer.print(",\"max\":{f}", .{std.json.fmt(max, .{})});
    if (bound.min != null) try writer.print(",\"inclusive_min\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_max\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

const DocumentFilterField = struct {
    path: []u8,
    field_type: runtime_schema.AntflyType,
    exact_declared_path: bool = true,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }

    fn takePath(self: *@This()) []u8 {
        const path = self.path;
        self.path = "";
        return path;
    }
};

fn documentFilterFieldForExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) !?DocumentFilterField {
    if (tokens.len == 1 and tokens[0].kind == .identifier) {
        if (std.mem.eql(u8, tokens[0].text, "_id")) return null;
        const column = documentFieldColumn(schema, tokens[0].text) orelse return error.InvalidSqlCatalog;
        if (!documentColumnIndexReady(column)) return error.DocumentSqlIndexUnavailable;
        return .{
            .path = try documentFilterPathAlloc(alloc, column.path),
            .field_type = column.field_type,
        };
    }

    var expression = (try parseDocumentJsonPathExpressionAlloc(alloc, tokens, schema)) orelse return null;
    defer expression.deinit(alloc);
    const exact_column = documentColumnForPath(schema, expression.path);
    if (exact_column) |column| {
        if (!documentColumnIndexReady(column)) return error.DocumentSqlIndexUnavailable;
        return .{
            .path = try alloc.dupe(u8, expression.path),
            .field_type = column.field_type,
        };
    }
    if (!documentFilterPathIndexReady(schema, expression.path, expression.root_column)) return error.DocumentSqlIndexUnavailable;
    return .{
        .path = try alloc.dupe(u8, expression.path),
        .field_type = expression.root_column.field_type,
        .exact_declared_path = false,
    };
}

fn documentOrderFieldForExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) !?DocumentFilterField {
    if (tokens.len == 1 and tokens[0].kind == .identifier) {
        const column = documentFieldColumn(schema, tokens[0].text) orelse return error.InvalidSqlCatalog;
        return .{
            .path = try documentFilterPathAlloc(alloc, column.path),
            .field_type = column.field_type,
        };
    }

    var expression = (try parseDocumentJsonPathExpressionAlloc(alloc, tokens, schema)) orelse return null;
    defer expression.deinit(alloc);
    const exact_column = documentColumnForPath(schema, expression.path) orelse return error.UnsupportedSqlShape;
    return .{
        .path = try alloc.dupe(u8, expression.path),
        .field_type = exact_column.field_type,
    };
}

fn documentAggregateFieldForExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
) !?DocumentFilterField {
    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens, schema)) orelse return null;
    errdefer field.deinit(alloc);
    return switch (field.field_type) {
        .keyword, .numeric, .boolean, .datetime, .geopoint, .geoshape => field,
        else => error.UnsupportedSqlShape,
    };
}

fn documentColumnIndexReady(column: runtime_schema.RelationalColumn) bool {
    return column.indexed and column.index_lifecycle == .ready;
}

fn documentFilterPathIndexReady(
    schema: runtime_schema.TableSchema,
    path: []const u8,
    root_column: runtime_schema.RelationalColumn,
) bool {
    if (documentColumnForPath(schema, path)) |column| {
        if (documentColumnIndexReady(column)) return true;
    }
    return root_column.field_type == .json and
        documentColumnIndexReady(root_column) and
        documentPathContainsPath(root_column.path, path);
}

fn documentColumnForPath(schema: runtime_schema.TableSchema, path: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (documentPathEquals(column.path, path)) return column;
    }
    return null;
}

fn documentPathEquals(column_path: []const u8, filter_path: []const u8) bool {
    const normalized_column = if (column_path.len > 0 and column_path[0] == '/') column_path[1..] else column_path;
    const normalized_filter = if (filter_path.len > 0 and filter_path[0] == '/') filter_path[1..] else filter_path;
    return std.mem.eql(u8, normalized_column, normalized_filter);
}

fn documentPathContainsPath(parent_path: []const u8, child_path: []const u8) bool {
    const parent = if (parent_path.len > 0 and parent_path[0] == '/') parent_path[1..] else parent_path;
    const child = if (child_path.len > 0 and child_path[0] == '/') child_path[1..] else child_path;
    return std.mem.eql(u8, parent, child) or
        (child.len > parent.len and std.mem.startsWith(u8, child, parent) and child[parent.len] == '/');
}

fn findTopLevelScalarFilterOperator(tokens: []const Token) ?usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .eq, .gt, .gte, .lt, .lte => if (depth == 0) return i,
            .identifier => if (depth == 0 and token.matchesKeywordTag(.in)) return i,
            else => {},
        }
    }
    return null;
}

fn tokenLiteralJsonAlloc(alloc: std.mem.Allocator, token: Token) ![]u8 {
    return switch (token.kind) {
        .string => try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(token.text, .{})}),
        .number => try alloc.dupe(u8, token.text),
        .identifier => blk: {
            if (token.matchesKeywordTag(.true)) break :blk try alloc.dupe(u8, "true");
            if (token.matchesKeywordTag(.false)) break :blk try alloc.dupe(u8, "false");
            if (token.matchesKeywordTag(.null)) break :blk try alloc.dupe(u8, "null");
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(token.text, .{})});
        },
        else => error.UnsupportedSqlShape,
    };
}

fn tokenLiteralListJsonAlloc(alloc: std.mem.Allocator, tokens: []const Token) ![]u8 {
    if (tokens.len < 3 or tokens[0].kind != .lparen or tokens[tokens.len - 1].kind != .rparen) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var pos: usize = 1;
    var count: usize = 0;
    while (pos + 1 < tokens.len) {
        if (count > 0) {
            if (tokens[pos].kind != .comma) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos + 1 >= tokens.len) return error.UnsupportedSqlShape;
            try writer.writeByte(',');
        }
        const value_json = try tokenLiteralJsonAlloc(alloc, tokens[pos]);
        defer alloc.free(value_json);
        try writer.writeAll(value_json);
        count += 1;
        pos += 1;
    }
    if (count == 0 or pos != tokens.len - 1) return error.UnsupportedSqlShape;
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn documentIdLiteralAlloc(alloc: std.mem.Allocator, token: Token) ![]const u8 {
    if (token.kind != .string and token.kind != .identifier and token.kind != .number) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn parseDocumentIdInListIntoAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    out: *ParsedDocumentWhere,
) !void {
    if (tokens.len < 3 or tokens[0].kind != .lparen or tokens[tokens.len - 1].kind != .rparen) return error.UnsupportedSqlShape;
    var pos: usize = 1;
    var count: usize = 0;
    while (pos + 1 < tokens.len) {
        if (count > 0) {
            if (tokens[pos].kind != .comma) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos + 1 >= tokens.len) return error.UnsupportedSqlShape;
        }
        const id = try documentIdLiteralAlloc(alloc, tokens[pos]);
        errdefer alloc.free(id);
        try out.ids.append(alloc, id);
        count += 1;
        pos += 1;
    }
    if (count == 0 or pos != tokens.len - 1) return error.UnsupportedSqlShape;
}

fn buildConjunctiveFilterJsonAlloc(alloc: std.mem.Allocator, clauses: []const []const u8) !?[]const u8 {
    if (clauses.len == 0) return null;
    if (clauses.len == 1) return try alloc.dupe(u8, clauses[0]);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"bool\":{\"filter\":[");
    for (clauses, 0..) |clause, i| {
        if (i > 0) try writer.writeByte(',');
        try writer.writeAll(clause);
    }
    try writer.writeAll("]}}");
    return try out.toOwnedSlice();
}

fn documentFilterPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (path.len == 0) return error.InvalidSqlCatalog;
    if (path[0] == '/') return try alloc.dupe(u8, path);
    return try std.fmt.allocPrint(alloc, "/{s}", .{path});
}

fn parseLimit(tokens: []const Token, limit_index: usize) !u32 {
    if (limit_index + 1 >= tokens.len or tokens[limit_index + 1].kind != .number) return error.UnsupportedSqlShape;
    if (limit_index + 2 < tokens.len and tokens[limit_index + 2].kind != .semicolon) return error.UnsupportedSqlShape;
    const value = try std.fmt.parseUnsigned(u32, tokens[limit_index + 1].text, 10);
    if (value == 0) return error.UnsupportedSqlShape;
    return value;
}

fn validateFromTail(tokens: []const Token) !void {
    if (tokens.len == 0) return;
    if (tokens.len == 1 and tokens[0].kind == .identifier) return;
    if (tokens.len == 2 and tokens[0].matchesKeywordTag(.as) and tokens[1].kind == .identifier) return;
    return error.UnsupportedSqlShape;
}

fn minOptionalIndex(a: ?usize, b: ?usize, c: ?usize) ?usize {
    var out: ?usize = null;
    inline for (.{ a, b, c }) |maybe| {
        if (maybe) |value| {
            out = if (out) |current| @min(current, value) else value;
        }
    }
    return out;
}

fn documentFieldExists(schema: runtime_schema.TableSchema, field: []const u8) bool {
    return documentFieldColumn(schema, field) != null;
}

fn documentFieldColumn(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, field)) return column;
    }
    return null;
}

fn freeProjection(alloc: std.mem.Allocator, projection: []DocumentProjection) void {
    for (projection) |*item| item.deinit(alloc);
    if (projection.len > 0) alloc.free(projection);
}

fn findComma(tokens: []const Token, start: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < tokens.len) : (i += 1) {
        switch (tokens[i].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .comma => if (depth == 0) return i,
            else => {},
        }
    }
    return null;
}

fn findTopLevelKeyword(tokens: []const Token, keyword: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0 and token.matchesKeywordTag(keyword)) return i,
            else => {},
        }
    }
    return null;
}

fn findTopLevelAnd(tokens: []const Token, start: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < tokens.len) : (i += 1) {
        switch (tokens[i].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0 and tokens[i].matchesKeywordTag(.@"and")) return i,
            else => {},
        }
    }
    return null;
}

test "document SQL lowers id lookup projection" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id = 'doc:a'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("docs", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, lowered.projection[0].kind);
    try std.testing.expectEqualStrings("title", lowered.projection[1].field);
    try std.testing.expectEqualStrings("doc:a", lowered.producer.id_lookup[0]);
}

test "document SQL lowers id in lookup projection" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id IN ('doc:a', 'doc:b')");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lowered.producer.id_lookup.len);
    try std.testing.expectEqualStrings("doc:a", lowered.producer.id_lookup[0]);
    try std.testing.expectEqualStrings("doc:b", lowered.producer.id_lookup[1]);
}

test "document SQL lowers bounded order by over id lookup" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id IN ('doc:a', 'doc:b') ORDER BY title DESC LIMIT 1");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lowered.producer.id_lookup.len);
    try std.testing.expectEqualStrings("/title", lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.text, lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, lowered.order_by.?.direction);
    try std.testing.expectEqual(@as(?u32, 1), lowered.limit);
}

test "document SQL rejects order by over indexed query until native ordering exists" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' ORDER BY status ASC LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL lowers algebraic grouped count over indexed facts" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("docs", lowered.table_name);
    try std.testing.expectEqual(DocumentAggregateOp.count, lowered.aggregate.op);
    try std.testing.expectEqualStrings("row_count", lowered.aggregate.output);
    try std.testing.expectEqualStrings("/metadata/plan", lowered.group_by.?.field);
    try std.testing.expectEqualStrings("plan", lowered.group_by.?.output);
    try std.testing.expect(lowered.filter_query_json != null);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL rejects algebraic group by without indexed facts" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL lowers json path projection" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'status' AS status, metadata#>>'{billing,plan}' AS plan FROM docs WHERE _id = 'doc:a'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), lowered.projection.len);
    try std.testing.expectEqualStrings("/metadata/status", lowered.projection[1].field);
    try std.testing.expectEqualStrings("status", lowered.projection[1].output);
    try std.testing.expectEqualStrings("/metadata/billing/plan", lowered.projection[2].field);
    try std.testing.expectEqualStrings("plan", lowered.projection[2].output);
}

test "document SQL lowers full text producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE full_text_search('title:alpha') LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL lowers qualified full text producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE antfly.full_text_search('title:alpha') LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL lowers scalar equality to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 10), lowered.limit);
}

test "document SQL lowers json path equality to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'status' AS status FROM docs WHERE metadata->>'status' = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/status", lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL lowers scalar range predicates to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "published_at", .path = "published_at", .field_type = .datetime, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount >= 10 AND status < 'closed' AND published_at <= '2026-01-03T00:00:00Z' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"inclusive_min\":true}},{\"term_range\":{\"path\":\"/status\",\"max\":\"closed\",\"inclusive_max\":false}},{\"date_range\":{\"path\":\"/published_at\",\"end\":\"2026-01-03T00:00:00Z\",\"inclusive_end\":true}}]}}",
        lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers declared json path range predicates to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json },
            .{ .name = "metadata_score", .path = "metadata/score", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'score' AS score FROM docs WHERE metadata->>'score' > 7 LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/score", lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metadata/score\",\"min\":7,\"inclusive_min\":false}}", lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL rejects untyped json subtree range predicates" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE metadata->>'score' > 7 LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL lowers scalar in and conjunction to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "published", .path = "published", .field_type = .boolean },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IN ('active', 'pending') AND published = true LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"terms\":{\"path\":\"/status\",\"values\":[\"active\",\"pending\"]}},{\"term\":{\"path\":\"/published\",\"value\":true}}]}}",
        lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers full text and scalar conjunction to indexed producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE full_text_search('title:alpha') AND status = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL requires bounded scan without id predicate" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{ .storage_mode = .document };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}
