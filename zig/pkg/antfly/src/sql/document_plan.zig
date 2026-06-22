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

pub const DocumentProducer = union(enum) {
    id_lookup: []const []const u8,
    full_text: []const u8,
    bounded_scan: u32,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .id_lookup => |ids| {
                for (ids) |id| alloc.free(@constCast(id));
                if (ids.len > 0) alloc.free(ids);
            },
            .full_text => |query| alloc.free(@constCast(query)),
            .bounded_scan => {},
        }
        self.* = undefined;
    }
};

pub const DocumentReadPlan = struct {
    table_name: []const u8,
    projection: []DocumentProjection,
    producer: DocumentProducer,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.projection) |*projection| projection.deinit(alloc);
        if (self.projection.len > 0) alloc.free(self.projection);
        self.producer.deinit(alloc);
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
    const limit_index = findTopLevelKeyword(tokens, .limit);
    if (findTopLevelKeyword(tokens, .join) != null or
        findTopLevelKeyword(tokens, .group) != null or
        findTopLevelKeyword(tokens, .having) != null or
        findTopLevelKeyword(tokens, .window) != null or
        findTopLevelKeyword(tokens, .order) != null)
    {
        return error.UnsupportedSqlShape;
    }

    if (from_index + 1 >= tokens.len or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    errdefer alloc.free(table_name);

    const tail_start = where_index orelse limit_index orelse tokens.len;
    try validateFromTail(tokens[from_index + 2 .. tail_start]);

    const projection = try parseProjectionAlloc(alloc, tokens[1..from_index], schema);
    errdefer freeProjection(alloc, projection);

    const limit = if (limit_index) |idx| try parseLimit(tokens, idx) else null;
    const producer = if (where_index) |idx|
        try parseWhereProducerAlloc(alloc, tokens, idx, limit_index orelse tokens.len)
    else blk: {
        const bounded = limit orelse return error.DocumentSqlRequiresBoundedScan;
        break :blk DocumentProducer{ .bounded_scan = bounded };
    };
    errdefer {
        var mutable = producer;
        mutable.deinit(alloc);
    }

    return .{
        .table_name = table_name,
        .projection = projection,
        .producer = producer,
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
    if (tokens.len == 0 or tokens[0].kind != .identifier) return error.UnsupportedSqlShape;
    var output: ?[]const u8 = null;
    if (tokens.len > 1) {
        if (tokens.len == 3 and tokens[1].matchesKeywordTag(.as) and tokens[2].kind == .identifier) {
            output = tokens[2].text;
        } else if (tokens.len == 2 and tokens[1].kind == .identifier) {
            output = tokens[1].text;
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    const field = tokens[0].text;
    if (std.mem.eql(u8, field, "_id")) {
        return .{ .kind = .id, .output = try alloc.dupe(u8, output orelse "_id") };
    }
    if (std.mem.eql(u8, field, "_doc")) {
        return .{ .kind = .doc, .output = try alloc.dupe(u8, output orelse "_doc") };
    }
    if (!documentFieldExists(schema, field)) return error.InvalidSqlCatalog;
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, field),
        .output = try alloc.dupe(u8, output orelse field),
    };
}

fn parseWhereProducerAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    where_index: usize,
    end_index: usize,
) !DocumentProducer {
    const where_tokens = tokens[where_index + 1 .. end_index];
    if (try parseFullTextProducerAlloc(alloc, where_tokens)) |producer| return producer;
    if (where_tokens.len != 3) return error.UnsupportedSqlShape;
    if (where_tokens[0].kind != .identifier or !std.mem.eql(u8, where_tokens[0].text, "_id")) return error.UnsupportedSqlShape;
    if (where_tokens[1].kind != .eq) return error.UnsupportedSqlShape;
    if (where_tokens[2].kind != .string and where_tokens[2].kind != .identifier and where_tokens[2].kind != .number) return error.UnsupportedSqlShape;

    const ids = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(ids);
    ids[0] = try alloc.dupe(u8, where_tokens[2].text);
    return .{ .id_lookup = ids };
}

fn parseFullTextProducerAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?DocumentProducer {
    if (tokens.len != 4) return null;
    if (tokens[0].kind != .identifier or
        (!std.ascii.eqlIgnoreCase(tokens[0].text, "full_text_search") and
            !std.ascii.eqlIgnoreCase(tokens[0].text, "antfly.full_text_search")))
    {
        return null;
    }
    if (tokens[1].kind != .lparen or tokens[2].kind != .string or tokens[3].kind != .rparen) return error.UnsupportedSqlShape;
    return .{ .full_text = try alloc.dupe(u8, tokens[2].text) };
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

fn documentFieldExists(schema: runtime_schema.TableSchema, field: []const u8) bool {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, field)) return true;
    }
    return false;
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
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.full_text);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL requires bounded scan without id predicate" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{ .storage_mode = .document };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}
