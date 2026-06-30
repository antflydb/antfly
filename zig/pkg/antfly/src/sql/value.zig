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

const lower_expr = @import("lower_expr.zig");
const parser = @import("parser.zig");
const platform_time = @import("../platform/time.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;
const ns_per_day: u64 = 86_400 * std.time.ns_per_s;

pub const SqlValue = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    json: []const u8,

    pub fn jsonAlloc(self: SqlValue, alloc: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .null => try alloc.dupe(u8, "null"),
            .bool => |value| try alloc.dupe(u8, if (value) "true" else "false"),
            .integer => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .float => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .string => |value| try std.json.Stringify.valueAlloc(alloc, value, .{}),
            .json => |value| try alloc.dupe(u8, value),
        };
    }

    pub fn asU32(self: SqlValue) !u32 {
        return switch (self) {
            .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @intCast(value) else error.UnsupportedSqlShape,
            else => error.UnsupportedSqlShape,
        };
    }
};

pub fn deinitSqlValue(alloc: std.mem.Allocator, value: SqlValue) void {
    switch (value) {
        .string => |text| alloc.free(@constCast(text)),
        .json => |json| alloc.free(@constCast(json)),
        .null, .bool, .integer, .float => {},
    }
}

pub fn cloneSqlValueAlloc(alloc: std.mem.Allocator, value: SqlValue) !SqlValue {
    return switch (value) {
        .null => .null,
        .bool => |flag| .{ .bool = flag },
        .integer => |number| .{ .integer = number },
        .float => |number| .{ .float = number },
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .json => |json| .{ .json = try alloc.dupe(u8, json) },
    };
}

pub fn boundSqlValue(token: Token, params: []const SqlValue) !SqlValue {
    if (token.text.len < 2 or token.text[0] != '$') return error.UnsupportedSqlShape;
    var end: usize = 1;
    while (end < token.text.len and std.ascii.isDigit(token.text[end])) end += 1;
    if (end == 1) return error.UnsupportedSqlShape;
    const index = try std.fmt.parseInt(usize, token.text[1..end], 10);
    if (index == 0 or index > params.len) return error.MissingSqlParameter;
    return params[index - 1];
}

pub fn boundSqlValueJsonAlloc(alloc: std.mem.Allocator, token: Token, params: []const SqlValue) ![]const u8 {
    const value = try boundSqlValue(token, params);
    return try value.jsonAlloc(alloc);
}

pub fn parseJsonScalarValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) !?[]const u8 {
    if (parser.matchKeywordTag(tokens, pos, .null)) return try alloc.dupe(u8, "null");
    if (parser.matchKeywordTag(tokens, pos, .true)) return try alloc.dupe(u8, "true");
    if (parser.matchKeywordTag(tokens, pos, .false)) return try alloc.dupe(u8, "false");
    if (parser.matchToken(tokens, pos, .string)) |token| return try std.json.Stringify.valueAlloc(alloc, token.text, .{});
    if (parser.matchToken(tokens, pos, .number)) |token| return try alloc.dupe(u8, token.text);
    if (parser.matchToken(tokens, pos, .minus) != null) return try parseSqlNegativeNumberJsonAfterMinusAlloc(alloc, tokens, pos);
    if (parser.matchToken(tokens, pos, .placeholder)) |token| return try boundSqlValueJsonAlloc(alloc, token, params);
    return null;
}

pub fn parseJsonValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) anyerror![]const u8 {
    if (peekConvertFromFunctionCall(tokens, pos.*)) return try parseConvertFromJsonAlloc(alloc, tokens, pos, params);
    if (peekToJsonbFunctionCall(tokens, pos.*)) return try parseToJsonbValueJsonAlloc(alloc, tokens, pos, params);
    return (try parseJsonScalarValueAlloc(alloc, tokens, pos, params)) orelse error.UnsupportedSqlShape;
}

pub fn parseToJsonbValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) anyerror![]const u8 {
    try parseToJsonbFunctionCallStart(tokens, pos);
    const value_json = try parseJsonValueAlloc(alloc, tokens, pos, params);
    errdefer alloc.free(value_json);
    try parser.expectToken(tokens, pos, .rparen);
    return value_json;
}

pub fn parseJsonDocumentValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) !?[]const u8 {
    if (parser.matchToken(tokens, pos, .placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .json => |json| blk: {
                try validateJsonDocument(alloc, json);
                break :blk try alloc.dupe(u8, json);
            },
            else => error.UnsupportedSqlShape,
        };
    }
    if (parser.matchToken(tokens, pos, .string)) |token| {
        try validateJsonDocument(alloc, token.text);
        return try alloc.dupe(u8, token.text);
    }
    return null;
}

pub fn parseRequiredJsonDocumentValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    return (try parseJsonDocumentValueAlloc(alloc, tokens, pos, params)) orelse error.UnsupportedSqlShape;
}

pub fn parseJsonArrayValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    const value_json = if (parser.peekKeywordTag(tokens, pos.*, .array))
        try parseSqlArrayConstructorJsonAlloc(alloc, tokens, pos, params)
    else
        try parseRequiredJsonDocumentValueAlloc(alloc, tokens, pos, params);
    errdefer alloc.free(value_json);
    try validateJsonArray(alloc, value_json);
    return value_json;
}

pub fn parseStructuredPredicateValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
    column: runtime_schema.RelationalColumn,
) ![]const u8 {
    return switch (column.field_type) {
        .array => try parseArrayPredicateValueAlloc(alloc, tokens, pos, params),
        .json => try parseRequiredJsonDocumentValueAlloc(alloc, tokens, pos, params),
        else => error.InvalidSqlCatalog,
    };
}

pub fn parseArrayPredicateValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    return if (parser.peekKeywordTag(tokens, pos.*, .array))
        try parseSqlArrayConstructorJsonAlloc(alloc, tokens, pos, params)
    else
        try parseRequiredJsonDocumentValueAlloc(alloc, tokens, pos, params);
}

pub fn parseSqlArrayConstructorJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    try parser.expectKeywordTag(tokens, pos, .array);
    try parser.expectToken(tokens, pos, .lbracket);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    if (parser.matchToken(tokens, pos, .rbracket) == null) {
        var first = true;
        while (true) {
            const value_json = try parseJsonValueAlloc(alloc, tokens, pos, params);
            defer alloc.free(value_json);
            if (!first) try writer.writeByte(',');
            try writer.writeAll(value_json);
            first = false;
            if (parser.matchToken(tokens, pos, .comma) == null) break;
        }
        try parser.expectToken(tokens, pos, .rbracket);
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

pub fn parseSqlInValuesJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    try parser.expectToken(tokens, pos, .lparen);
    if (parser.peekKind(tokens, pos.*, .rparen)) return error.UnsupportedSqlShape;

    if (parser.matchToken(tokens, pos, .placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        if (parser.matchToken(tokens, pos, .rparen) != null) {
            return switch (value) {
                .json => |json| blk: {
                    try validateJsonArray(alloc, json);
                    break :blk try alloc.dupe(u8, json);
                },
                else => try singleValueJsonArrayAlloc(alloc, value),
            };
        }
        const first_json = try value.jsonAlloc(alloc);
        defer alloc.free(first_json);
        return try parseSqlInRemainingValuesJsonAlloc(alloc, tokens, pos, params, first_json);
    }

    const first_json = try parseJsonValueAlloc(alloc, tokens, pos, params);
    defer alloc.free(first_json);
    return try parseSqlInRemainingValuesJsonAlloc(alloc, tokens, pos, params, first_json);
}

fn parseSqlInRemainingValuesJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
    first_json: []const u8,
) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    try writer.writeAll(first_json);
    while (parser.matchToken(tokens, pos, .comma) != null) {
        const value_json = try parseJsonValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(value_json);
        try writer.writeByte(',');
        try writer.writeAll(value_json);
    }
    try parser.expectToken(tokens, pos, .rparen);
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

pub fn parseSqlColumnValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
    column: runtime_schema.RelationalColumn,
    realtime_ns: u64,
) ![]const u8 {
    if (parser.matchKeywordTag(tokens, pos, .default)) {
        const default_value = column.default_value orelse return error.UnsupportedSqlShape;
        return try relational_rows.relationalDefaultValueJsonAlloc(alloc, default_value);
    }
    if (lower_expr.peekSqlNowExpressionSyntax(tokens, pos.*)) {
        if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        const now_ns = try checkedRealtimeNsU64(realtime_ns);
        return try parseSqlNowValueJsonAlloc(alloc, tokens, pos, now_ns);
    }
    if (lower_expr.peekSqlCurrentDateExpressionSyntax(tokens, pos.*)) {
        if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        const now_ns = try checkedRealtimeNsU64(realtime_ns);
        return try parseSqlCurrentDateValueJsonAlloc(alloc, tokens, pos, sqlCurrentUtcDateStartNs(now_ns));
    }
    if (column.field_type == .datetime and lower_expr.peekSqlTypedDatetimeLiteral(tokens, pos.*)) {
        return try parseSqlTypedDatetimeLiteralValueJsonAlloc(alloc, tokens, pos);
    }
    if (lower_expr.peekFunctionCallTokenIf(tokens, pos.*, lower_expr.sqlTokenIsUuidV4Function)) {
        if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
        return try parseUuidV4ValueJsonAlloc(alloc, tokens, pos);
    }
    if (peekConvertFromFunctionCall(tokens, pos.*)) {
        if (column.field_type != .json) return error.InvalidSqlCatalog;
        return try parseConvertFromJsonAlloc(alloc, tokens, pos, params);
    }
    if (peekJsonbBuildObjectFunctionCall(tokens, pos.*)) {
        if (column.field_type != .json) return error.InvalidSqlCatalog;
        return try parseJsonbBuildObjectAlloc(alloc, tokens, pos, params);
    }
    if (peekToJsonbFunctionCall(tokens, pos.*)) {
        if (column.field_type != .json) return error.InvalidSqlCatalog;
        return try parseToJsonbValueJsonAlloc(alloc, tokens, pos, params);
    }
    if (parser.matchToken(tokens, pos, .placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return try boundSqlColumnValueJsonAlloc(alloc, value, column);
    }
    if (parser.matchToken(tokens, pos, .string)) |token| {
        if (column.field_type == .json) {
            if (jsonValueIsValid(alloc, token.text)) return try alloc.dupe(u8, token.text);
        }
        return try std.json.Stringify.valueAlloc(alloc, token.text, .{});
    }
    if (parser.matchKeywordTag(tokens, pos, .null)) return try alloc.dupe(u8, "null");
    if (parser.matchKeywordTag(tokens, pos, .true)) return try alloc.dupe(u8, "true");
    if (parser.matchKeywordTag(tokens, pos, .false)) return try alloc.dupe(u8, "false");
    if (parser.matchToken(tokens, pos, .number)) |token| return try alloc.dupe(u8, token.text);
    if (parser.matchToken(tokens, pos, .minus) != null) return try parseSqlNegativeNumberJsonAfterMinusAlloc(alloc, tokens, pos);
    return error.UnsupportedSqlShape;
}

fn boundSqlColumnValueJsonAlloc(
    alloc: std.mem.Allocator,
    value: SqlValue,
    column: runtime_schema.RelationalColumn,
) ![]const u8 {
    if (column.field_type == .json) {
        return switch (value) {
            .json => |json| try alloc.dupe(u8, json),
            else => try value.jsonAlloc(alloc),
        };
    }
    if (value == .string) {
        const text = value.string;
        switch (column.field_type) {
            .numeric, .datetime => {
                if (sqlStringIsJsonNumber(alloc, text)) return try alloc.dupe(u8, text);
            },
            .boolean => {
                if (std.ascii.eqlIgnoreCase(text, "t") or
                    std.ascii.eqlIgnoreCase(text, "true") or
                    std.mem.eql(u8, text, "1"))
                {
                    return try alloc.dupe(u8, "true");
                }
                if (std.ascii.eqlIgnoreCase(text, "f") or
                    std.ascii.eqlIgnoreCase(text, "false") or
                    std.mem.eql(u8, text, "0"))
                {
                    return try alloc.dupe(u8, "false");
                }
            },
            else => {},
        }
    }
    return try value.jsonAlloc(alloc);
}

pub fn parseUuidV4ValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    try parseSqlUuidV4Call(tokens, pos);
    return try relational_rows.relationalDefaultValueJsonAlloc(alloc, .{ .kind = .uuid_v4, .value_json = "" });
}

pub fn parseConvertFromJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    try parseConvertFromFunctionCallStart(tokens, pos);
    const decoded = (try parseConvertFromInputAlloc(alloc, tokens, pos, params)) orelse return error.UnsupportedSqlShape;
    defer alloc.free(decoded);
    try parser.expectToken(tokens, pos, .comma);
    const encoding = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(encoding.text, "UTF8") and !std.ascii.eqlIgnoreCase(encoding.text, "UTF-8")) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);
    if (!jsonValueIsValid(alloc, decoded)) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, decoded);
}

pub fn parseJsonbBuildObjectAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    try parseJsonbBuildObjectFunctionCallStart(tokens, pos);
    if (parser.matchToken(tokens, pos, .rparen) != null) return try alloc.dupe(u8, "{}");

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    while (true) {
        const key = try parseJsonbBuildObjectKey(tokens, pos, params);
        const entry = try seen.getOrPut(alloc, key);
        if (entry.found_existing) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .comma);
        const value_json = try parseJsonValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(value_json);
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(key, .{})});
        try writer.writeAll(value_json);
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    try parser.expectToken(tokens, pos, .rparen);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn parseConvertFromInputAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) !?[]const u8 {
    if (parser.matchToken(tokens, pos, .placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .string => |text| try alloc.dupe(u8, text),
            .json => |json| try alloc.dupe(u8, json),
            else => error.UnsupportedSqlShape,
        };
    }
    if (parser.matchToken(tokens, pos, .string)) |token| return try alloc.dupe(u8, token.text);
    return null;
}

pub fn parseJsonbBuildObjectKey(
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.string)) |token| return token.text;
    if (cursor.matchToken(.placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .string => |key| key,
            else => error.UnsupportedSqlShape,
        };
    }
    return error.UnsupportedSqlShape;
}

pub fn peekConvertFromFunctionCall(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .convert_from) and
        pos + 1 < tokens.len and
        tokens[pos + 1].kind == .lparen;
}

pub fn parseConvertFromFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeywordTag(tokens, pos, .convert_from);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn peekToJsonbFunctionCall(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .to_jsonb) and
        pos + 1 < tokens.len and
        tokens[pos + 1].kind == .lparen;
}

pub fn parseToJsonbFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeywordTag(tokens, pos, .to_jsonb);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn peekJsonbBuildObjectFunctionCall(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .jsonb_build_object) and
        pos + 1 < tokens.len and
        tokens[pos + 1].kind == .lparen;
}

pub fn parseJsonbBuildObjectFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeywordTag(tokens, pos, .jsonb_build_object);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn singleValueJsonArrayAlloc(alloc: std.mem.Allocator, value: SqlValue) ![]const u8 {
    const value_json = try value.jsonAlloc(alloc);
    defer alloc.free(value_json);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    try writer.writeAll(value_json);
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

pub fn booleanJson(value: bool) []const u8 {
    return if (value) "true" else "false";
}

pub fn parseSqlBooleanIsValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    not: bool,
) !?[]const u8 {
    if (!(parser.peekKeywordTag(tokens, pos.*, .true) or parser.peekKeywordTag(tokens, pos.*, .false))) return null;
    if (not) return error.UnsupportedSqlShape;
    if (column.field_type != .boolean) return error.InvalidSqlCatalog;
    if (parser.matchKeywordTag(tokens, pos, .true)) return try alloc.dupe(u8, "true");
    try parser.expectKeywordTag(tokens, pos, .false);
    return try alloc.dupe(u8, "false");
}

pub fn parseSqlBooleanIsValue(
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
) !?bool {
    if (!(parser.peekKeywordTag(tokens, pos.*, .true) or parser.peekKeywordTag(tokens, pos.*, .false))) return null;
    if (column.field_type != .boolean) return error.InvalidSqlCatalog;
    if (parser.matchKeywordTag(tokens, pos, .true)) return true;
    try parser.expectKeywordTag(tokens, pos, .false);
    return false;
}

pub fn parseSqlBooleanIsUnknown(
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
) !bool {
    if (!parser.matchKeywordTag(tokens, pos, .unknown)) return false;
    if (column.field_type != .boolean) return error.InvalidSqlCatalog;
    return true;
}

pub fn parseJsonPathOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.string)) |token| {
        if (token.text.len == 0) return error.UnsupportedSqlShape;
        return try alloc.dupe(u8, token.text);
    }
    if (cursor.matchToken(.placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .string => |path| if (path.len == 0) error.UnsupportedSqlShape else try alloc.dupe(u8, path),
            else => error.UnsupportedSqlShape,
        };
    }
    return error.UnsupportedSqlShape;
}

pub fn parseJsonExtractOperatorPathOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
    operator: TokenKind,
) ![]const u8 {
    if (!lower_expr.tokenKindIsJsonExtractPathOperator(operator)) {
        return try parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
    }

    const segments = try parsePostgresJsonPathAlloc(alloc, tokens, pos, params);
    defer {
        for (segments) |segment| alloc.free(segment);
        alloc.free(segments);
    }
    return try lower_expr.jsonPathSegmentsToDottedPathAlloc(alloc, segments);
}

pub fn parseJsonExtractPathSegmentsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    var path = std.ArrayListUnmanaged(u8).empty;
    errdefer path.deinit(alloc);

    var segments: usize = 0;
    while (true) {
        if (segments == 0) {
            try cursor.expectToken(.comma);
        } else if (cursor.matchToken(.comma) == null) {
            break;
        }

        const segment = try parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
        defer alloc.free(segment);
        if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '.') != null) return error.UnsupportedSqlShape;
        if (segments > 0) try path.append(alloc, '.');
        try path.appendSlice(alloc, segment);
        segments += 1;
    }

    if (segments == 0) return error.UnsupportedSqlShape;
    return try path.toOwnedSlice(alloc);
}

pub fn parsePostgresJsonPathAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const []const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.string)) |token| {
        return try lower_expr.parsePostgresJsonPathTextAlloc(alloc, token.text);
    }
    if (cursor.matchToken(.placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .string => |path| try lower_expr.parsePostgresJsonPathTextAlloc(alloc, path),
            .json => |path_json| try lower_expr.parsePostgresJsonPathJsonArrayAlloc(alloc, path_json),
            else => error.UnsupportedSqlShape,
        };
    }
    return error.UnsupportedSqlShape;
}

pub fn parseSqlU32Value(tokens: []const Token, pos: *usize, params: []const SqlValue) !u32 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.number)) |token| {
        return try std.fmt.parseInt(u32, token.text, 10);
    }
    if (cursor.matchToken(.placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return try value.asU32();
    }
    return error.UnsupportedSqlShape;
}

pub fn parseArrayLengthFunctionTail(tokens: []const Token, pos: *usize, params: []const SqlValue, keyword: []const u8) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!lower_expr.sqlKeywordIsCardinalityFunction(keyword)) {
        try cursor.expectToken(.comma);
        const dimension = try parseSqlU32Value(tokens, pos, params);
        if (dimension != 1) return error.UnsupportedSqlShape;
    }
    try cursor.expectToken(.rparen);
}

pub fn parseSqlStringValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const SqlValue,
) ![]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .string => |text| try alloc.dupe(u8, text),
            else => error.UnsupportedSqlShape,
        };
    }
    if (cursor.matchToken(.string)) |token| return try alloc.dupe(u8, token.text);
    return error.UnsupportedSqlShape;
}

pub fn peekStandaloneSqlBooleanLiteral(tokens: []const Token, pos: usize) ?bool {
    const token = if (pos < tokens.len) tokens[pos] else return null;
    if (token.kind != .identifier) return null;
    const enabled = if (token.matchesKeywordTag(.true))
        true
    else if (token.matchesKeywordTag(.false))
        false
    else
        return null;
    if (pos + 1 >= tokens.len) return enabled;
    const next = tokens[pos + 1];
    return switch (next.kind) {
        .semicolon, .rparen => enabled,
        .identifier => if (next.matchesKeywordTag(.@"and") or
            next.matchesKeywordTag(.@"or") or
            lower_expr.sqlWhereTailClauseKeywordToken(next))
            enabled
        else
            null,
        else => null,
    };
}

pub fn matchStandaloneSqlBooleanLiteral(tokens: []const Token, pos: *usize) ?bool {
    const enabled = peekStandaloneSqlBooleanLiteral(tokens, pos.*) orelse return null;
    pos.* += 1;
    return enabled;
}

pub fn parseNullableSqlU32Value(tokens: []const Token, pos: *usize, params: []const SqlValue) !?u32 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeywordTag(.null)) return null;
    _ = cursor.matchToken(.plus);
    if (cursor.matchToken(.number)) |token| {
        return try std.fmt.parseInt(u32, token.text, 10);
    }
    if (cursor.matchToken(.placeholder)) |token| {
        const value = try boundSqlValue(token, params);
        return switch (value) {
            .null => null,
            else => try value.asU32(),
        };
    }
    return error.UnsupportedSqlShape;
}

pub fn parseLimitValue(tokens: []const Token, pos: *usize, params: []const SqlValue) !?u32 {
    if (parser.matchKeywordTag(tokens, pos, .all)) return null;
    return try parseNullableSqlU32Value(tokens, pos, params);
}

pub fn parseOffsetValue(tokens: []const Token, pos: *usize, params: []const SqlValue) !u32 {
    const offset = (try parseNullableSqlU32Value(tokens, pos, params)) orelse 0;
    _ = parser.matchKeywordTag(tokens, pos, .row) or parser.matchKeywordTag(tokens, pos, .rows);
    return offset;
}

pub fn parseFetchLimitValue(tokens: []const Token, pos: *usize, params: []const SqlValue) !?u32 {
    if (!(parser.matchKeywordTag(tokens, pos, .first) or parser.matchKeywordTag(tokens, pos, .next))) return error.UnsupportedSqlShape;
    const limit: ?u32 = if (parser.peekKeywordTag(tokens, pos.*, .row) or parser.peekKeywordTag(tokens, pos.*, .rows))
        1
    else
        try parseLimitValue(tokens, pos, params);
    if (!(parser.matchKeywordTag(tokens, pos, .row) or parser.matchKeywordTag(tokens, pos, .rows))) return error.UnsupportedSqlShape;
    try parser.expectKeywordTag(tokens, pos, .only);
    return limit;
}

pub fn sqlStringIsJsonNumber(alloc: std.mem.Allocator, text: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, text, .{}) catch return false;
    defer parsed.deinit();
    return parsed.value == .integer or parsed.value == .float;
}

pub fn canonicalizeDiscreteDateRangeFiniteBoundAlloc(alloc: std.mem.Allocator, bound_json: []const u8) ![]const u8 {
    if (std.mem.eql(u8, bound_json, "null")) return error.UnsupportedSqlShape;
    const bound_ns = std.fmt.parseInt(u64, bound_json, 10) catch return error.UnsupportedSqlShape;
    if (std.math.maxInt(u64) - bound_ns < ns_per_day) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "{d}", .{bound_ns + ns_per_day});
}

pub fn parseSqlRangeEndpointJsonAlloc(
    alloc: std.mem.Allocator,
    endpoint: []const u8,
    field_type: runtime_schema.AntflyType,
) ![]const u8 {
    if (endpoint.len == 0) return try alloc.dupe(u8, "null");
    return switch (field_type) {
        .numeric => blk: {
            if (!sqlStringIsJsonNumber(alloc, endpoint)) return error.UnsupportedSqlShape;
            break :blk try alloc.dupe(u8, endpoint);
        },
        .datetime => blk: {
            if (sqlStringIsJsonNumber(alloc, endpoint)) return try alloc.dupe(u8, endpoint);
            const timestamp_ns = try parseSqlTimestampLiteralNs(endpoint);
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{timestamp_ns});
        },
        else => error.InvalidSqlCatalog,
    };
}

pub fn parseSqlTypedDatetimeLiteralValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    if (!parser.matchKeywordTag(tokens, pos, .date) and
        !parser.matchKeywordTag(tokens, pos, .timestamp) and
        !parser.matchKeywordTag(tokens, pos, .timestamptz)) return error.UnsupportedSqlShape;
    const token = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
    return try parseSqlRangeEndpointJsonAlloc(alloc, token.text, .datetime);
}

pub fn sqlCurrentUtcDateStartNs(now_ns: u64) u64 {
    return now_ns - (now_ns % ns_per_day);
}

pub fn currentRealtimeNs() u64 {
    return platform_time.realtimeNs();
}

fn checkedRealtimeNsU64(value: i128) !u64 {
    if (value < 0 or value > std.math.maxInt(u64)) return error.UnsupportedSqlShape;
    return @intCast(value);
}

pub fn parseSqlNowValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    realtime_ns: u64,
) ![]const u8 {
    try parseSqlNowCall(tokens, pos);
    return try std.fmt.allocPrint(alloc, "{d}", .{realtime_ns});
}

pub fn parseSqlCurrentDateValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    current_date_start_ns: u64,
) ![]const u8 {
    try parseSqlCurrentDateKeyword(tokens, pos);
    return try std.fmt.allocPrint(alloc, "{d}", .{current_date_start_ns});
}

pub fn parseSqlNowCall(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeywordTag(.now)) {
        try cursor.expectToken(.lparen);
        try cursor.expectToken(.rparen);
    } else if (cursor.matchKeywordTag(.current_timestamp)) {
        try parseOptionalCurrentTimestampPrecision(tokens, pos);
    } else {
        return error.UnsupportedSqlShape;
    }
}

pub fn parseSqlCurrentDateKeyword(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeywordTag(.current_date);
}

pub fn parseSqlUuidV4Call(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchIdentifierTokenIf(lower_expr.sqlTokenIsUuidV4Function) orelse return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    try cursor.expectToken(.rparen);
}

fn parseOptionalCurrentTimestampPrecision(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.lparen) == null) return;
    const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    const precision = std.fmt.parseUnsigned(u8, token.text, 10) catch return error.UnsupportedSqlShape;
    if (precision > 6) return error.UnsupportedSqlShape;
    try cursor.expectToken(.rparen);
}

pub fn parseSqlCanonicalRangeLiteralValuePairAlloc(
    alloc: std.mem.Allocator,
    literal: []const u8,
    field_type: runtime_schema.AntflyType,
    range_type: ?runtime_schema.RelationalPeriodRangeType,
) !lower_expr.PeriodRangeValuePair {
    if (literal.len < 3) return error.UnsupportedSqlShape;
    const upper_bound = literal[literal.len - 1];
    const lower_bound = literal[0];
    if (lower_bound != '[' and lower_bound != '(') return error.UnsupportedSqlShape;
    if (upper_bound != ')' and upper_bound != ']') return error.UnsupportedSqlShape;
    const body = literal[1 .. literal.len - 1];
    const comma = std.mem.indexOfScalar(u8, body, ',') orelse return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, body[comma + 1 ..], ',') != null) return error.UnsupportedSqlShape;
    const start_text = std.mem.trim(u8, body[0..comma], " \t\r\n");
    const end_text = std.mem.trim(u8, body[comma + 1 ..], " \t\r\n");
    if (start_text.len != 0 and lower_bound == '(' and range_type != .daterange) return error.UnsupportedSqlShape;
    if (upper_bound == ']' and (range_type != .daterange or end_text.len == 0)) return error.UnsupportedSqlShape;

    var start_json = try parseSqlRangeEndpointJsonAlloc(alloc, start_text, field_type);
    var start_transferred = false;
    errdefer if (!start_transferred) alloc.free(start_json);
    var end_json = try parseSqlRangeEndpointJsonAlloc(alloc, end_text, field_type);
    var end_transferred = false;
    errdefer if (!end_transferred) alloc.free(end_json);
    if (lower_bound == '(' and start_text.len != 0) {
        const canonical_start_json = try canonicalizeDiscreteDateRangeFiniteBoundAlloc(alloc, start_json);
        alloc.free(start_json);
        start_json = canonical_start_json;
    }
    if (upper_bound == ']') {
        const canonical_end_json = try canonicalizeDiscreteDateRangeFiniteBoundAlloc(alloc, end_json);
        alloc.free(end_json);
        end_json = canonical_end_json;
    }

    start_transferred = true;
    end_transferred = true;
    return .{
        .start_json = start_json,
        .end_json = end_json,
    };
}

pub fn sqlArrayItemValueMatches(item_type: runtime_schema.AntflyType, value: std.json.Value) bool {
    return switch (item_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => value == .string,
        .numeric => value == .integer or value == .float or value == .number_string,
        .datetime => value == .integer or value == .float or value == .number_string,
        .boolean => value == .bool,
        .geopoint => value == .array or value == .object,
        .json => true,
        .array => value == .array,
        .embedding => false,
    };
}

pub fn sqlScalarValueMatches(field_type: runtime_schema.AntflyType, value: std.json.Value) bool {
    return switch (field_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => value == .string,
        .numeric => value == .integer or value == .float or value == .number_string,
        .datetime => value == .integer or value == .float or value == .number_string,
        .boolean => value == .bool,
        .geopoint => value == .array or value == .object,
        .json, .array, .embedding => false,
    };
}

pub fn validateSqlArrayElementValueJson(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    value_json: []const u8,
) !void {
    const item_type = column.array_item_type orelse return error.InvalidSqlCatalog;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (!sqlArrayItemValueMatches(item_type, parsed.value)) return error.UnsupportedSqlShape;
}

pub fn validateSqlArrayValueJson(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    value_json: []const u8,
) !void {
    const item_type = column.array_item_type orelse return error.InvalidSqlCatalog;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
    for (parsed.value.array.items) |item| {
        if (!sqlArrayItemValueMatches(item_type, item)) return error.UnsupportedSqlShape;
    }
}

pub fn validateSqlScalarValuesJson(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    values_json: []const u8,
) !void {
    if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
    for (parsed.value.array.items) |item| {
        if (!sqlScalarValueMatches(column.field_type, item)) return error.UnsupportedSqlShape;
    }
}

pub fn validateDefaultValueForColumnAlloc(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    default_value: runtime_schema.RelationalDefaultValue,
) !void {
    switch (default_value.kind) {
        .uuid_v4 => {
            if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.UnsupportedSqlShape;
        },
        .now_ns => {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.UnsupportedSqlShape;
        },
        .current_date_ns => {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.UnsupportedSqlShape;
        },
        .sequence_next => {
            if (column.field_type != .numeric) return error.UnsupportedSqlShape;
        },
        .literal => try validateLiteralDefaultForColumnAlloc(alloc, column, default_value.value_json),
    }
}

fn validateLiteralDefaultForColumnAlloc(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    value_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value == .null) return;
    switch (column.field_type) {
        .keyword, .text, .link, .blob, .datetime => {
            if (parsed.value != .string) return error.UnsupportedSqlShape;
        },
        .numeric => switch (parsed.value) {
            .integer, .float => {},
            else => return error.UnsupportedSqlShape,
        },
        .boolean => {
            if (parsed.value != .bool) return error.UnsupportedSqlShape;
        },
        .json => {},
        .array => {
            if (parsed.value != .array) return error.UnsupportedSqlShape;
        },
        else => return error.UnsupportedSqlShape,
    }
}

pub fn validateJsonDocument(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    switch (parsed.value) {
        .object, .array => {},
        else => return error.UnsupportedSqlShape,
    }
}

pub fn validateJsonArray(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
}

pub fn validateJsonStringArray(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
    for (parsed.value.array.items) |item| {
        if (item != .string) return error.UnsupportedSqlShape;
    }
}

pub fn jsonValueIsValid(alloc: std.mem.Allocator, value: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return false;
    parsed.deinit();
    return true;
}

pub fn encodeSqlTxnIdHex(txn_id: [16]u8) [32]u8 {
    var out: [32]u8 = undefined;
    const hex = "0123456789abcdef";
    for (txn_id, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

pub fn parseSqlUntypedValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeywordTag(.true)) return try alloc.dupe(u8, "true");
    if (cursor.matchKeywordTag(.false)) return try alloc.dupe(u8, "false");
    if (cursor.matchKeywordTag(.null)) return try alloc.dupe(u8, "null");
    if (cursor.matchToken(.string)) |token| return try std.json.Stringify.valueAlloc(alloc, token.text, .{});
    if (cursor.matchToken(.number)) |token| return try alloc.dupe(u8, token.text);
    if (cursor.matchToken(.minus) != null) {
        return try parseSqlNegativeNumberJsonAfterMinusAlloc(alloc, tokens, pos);
    }
    return error.UnsupportedSqlShape;
}

pub fn parseSqlUntypedValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !SqlValue {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeywordTag(.true)) return .{ .bool = true };
    if (cursor.matchKeywordTag(.false)) return .{ .bool = false };
    if (cursor.matchKeywordTag(.null)) return .null;
    if (cursor.matchToken(.string)) |token| return .{ .string = try alloc.dupe(u8, token.text) };
    if (cursor.matchToken(.number)) |token| return try parseSqlNumberValue(token.text);
    if (cursor.matchToken(.minus) != null) {
        const token = parser.matchToken(tokens, pos, .number) orelse return error.UnsupportedSqlShape;
        return try parseSqlNegativeNumberValue(token.text);
    }
    return error.UnsupportedSqlShape;
}

fn parseSqlNumberValue(text: []const u8) !SqlValue {
    if (std.mem.indexOfAny(u8, text, ".eE") == null) {
        if (std.fmt.parseInt(i64, text, 10)) |integer| return .{ .integer = integer } else |_| {}
    }
    return .{ .float = try std.fmt.parseFloat(f64, text) };
}

fn parseSqlNegativeNumberValue(text: []const u8) !SqlValue {
    if (std.mem.indexOfAny(u8, text, ".eE") == null) {
        if (std.fmt.parseInt(i64, text, 10)) |integer| return .{ .integer = -integer } else |_| {}
    }
    const positive = try std.fmt.parseFloat(f64, text);
    return .{ .float = -positive };
}

pub fn parseSqlNegativeNumberJsonAfterMinusAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .number) orelse return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "-{s}", .{token.text});
}

pub fn parseSqlTimestampLiteralNs(raw: []const u8) !i64 {
    const text = std.mem.trim(u8, trimSqlRangeEndpointQuotes(raw), " \t\r\n");
    if (text.len < "0000-00-00".len) return error.UnsupportedSqlShape;
    if (text[4] != '-' or text[7] != '-') return error.UnsupportedSqlShape;
    const year = try parseSqlFixedDigits(i64, text[0..4]);
    const month = try parseSqlFixedDigits(u8, text[5..7]);
    const day = try parseSqlFixedDigits(u8, text[8..10]);
    if (month < 1 or month > 12) return error.UnsupportedSqlShape;
    const max_day = sqlDaysInMonth(year, month);
    if (day < 1 or day > max_day) return error.UnsupportedSqlShape;

    var i: usize = 10;
    var hour: u8 = 0;
    var minute: u8 = 0;
    var second: u8 = 0;
    var fractional_ns: i64 = 0;
    var offset_seconds: i64 = 0;

    if (i < text.len) {
        if (text[i] != 'T' and text[i] != 't' and text[i] != ' ') return error.UnsupportedSqlShape;
        i += 1;
        if (i + 8 > text.len or text[i + 2] != ':' or text[i + 5] != ':') return error.UnsupportedSqlShape;
        hour = try parseSqlFixedDigits(u8, text[i .. i + 2]);
        minute = try parseSqlFixedDigits(u8, text[i + 3 .. i + 5]);
        second = try parseSqlFixedDigits(u8, text[i + 6 .. i + 8]);
        if (hour > 23 or minute > 59 or second > 59) return error.UnsupportedSqlShape;
        i += 8;
        if (i < text.len and text[i] == '.') {
            i += 1;
            const fraction_start = i;
            var multiplier: i64 = 100_000_000;
            while (i < text.len and std.ascii.isDigit(text[i])) : (i += 1) {
                if (i - fraction_start < 9) {
                    fractional_ns += @as(i64, text[i] - '0') * multiplier;
                    multiplier = @divExact(multiplier, 10);
                }
            }
            if (i == fraction_start) return error.UnsupportedSqlShape;
        }
        if (i < text.len) {
            offset_seconds = try parseSqlTimestampOffsetSeconds(text[i..]);
            i = text.len;
        }
    }

    if (i != text.len) return error.UnsupportedSqlShape;
    const day_ns = (@as(i64, hour) * 60 * 60 + @as(i64, minute) * 60 + @as(i64, second)) * std.time.ns_per_s + fractional_ns;
    const days = sqlDaysFromCivil(year, month, day);
    const total_ns = @as(i128, days) * @as(i128, std.time.ns_per_day) + @as(i128, day_ns) - @as(i128, offset_seconds) * @as(i128, std.time.ns_per_s);
    if (total_ns < std.math.minInt(i64) or total_ns > std.math.maxInt(i64)) return error.UnsupportedSqlShape;
    return @intCast(total_ns);
}

fn trimSqlRangeEndpointQuotes(text: []const u8) []const u8 {
    if (text.len >= 2 and ((text[0] == '"' and text[text.len - 1] == '"') or (text[0] == '\'' and text[text.len - 1] == '\''))) {
        return text[1 .. text.len - 1];
    }
    return text;
}

fn parseSqlFixedDigits(comptime T: type, text: []const u8) !T {
    if (text.len == 0) return error.UnsupportedSqlShape;
    var value: T = 0;
    for (text) |ch| {
        if (!std.ascii.isDigit(ch)) return error.UnsupportedSqlShape;
        value = value * 10 + @as(T, @intCast(ch - '0'));
    }
    return value;
}

fn parseSqlTimestampOffsetSeconds(text: []const u8) !i64 {
    if (text.len == 0) return 0;
    if (std.ascii.eqlIgnoreCase(text, "Z")) return 0;
    const sign: i64 = if (text[0] == '+') 1 else if (text[0] == '-') -1 else return error.UnsupportedSqlShape;
    if (text.len != 3 and text.len != 5 and text.len != 6) return error.UnsupportedSqlShape;
    const hour = try parseSqlFixedDigits(i64, text[1..3]);
    var minute: i64 = 0;
    if (text.len == 5) {
        minute = try parseSqlFixedDigits(i64, text[3..5]);
    } else if (text.len == 6) {
        if (text[3] != ':') return error.UnsupportedSqlShape;
        minute = try parseSqlFixedDigits(i64, text[4..6]);
    }
    if (hour > 23 or minute > 59) return error.UnsupportedSqlShape;
    return sign * (hour * 60 * 60 + minute * 60);
}

fn sqlDaysFromCivil(year_value: i64, month_value: u8, day_value: u8) i64 {
    var year = year_value;
    const month: i64 = month_value;
    const day: i64 = day_value;
    year -= if (month <= 2) 1 else 0;
    const era = @divFloor(year, 400);
    const yoe = year - era * 400;
    const month_prime = month + if (month > 2) @as(i64, -3) else @as(i64, 9);
    const doy = @divFloor(153 * month_prime + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn sqlDaysInMonth(year: i64, month: u8) u8 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (sqlIsLeapYear(year)) 29 else 28,
        else => 0,
    };
}

fn sqlIsLeapYear(year: i64) bool {
    return @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
}

pub const SqlIntervalLiteral = struct {
    fixed_ns: u64,
    calendar_months: u64,
    saw_fixed: bool,
    saw_calendar: bool,
};

pub fn sqlIntervalLiteral(text: []const u8) !SqlIntervalLiteral {
    var parts = std.mem.tokenizeAny(u8, text, " \t\r\n");
    var fixed_total: u128 = 0;
    var calendar_total: u128 = 0;
    var saw_pair = false;
    var saw_fixed = false;
    var saw_calendar = false;
    while (parts.next()) |amount_text| {
        const unit_text = parts.next() orelse return error.UnsupportedSqlShape;
        const amount = std.fmt.parseInt(u64, amount_text, 10) catch return error.UnsupportedSqlShape;
        if (sqlIntervalUnitNs(unit_text)) |multiplier| {
            saw_fixed = true;
            fixed_total += @as(u128, amount) * @as(u128, multiplier);
            if (fixed_total > std.math.maxInt(u64)) return error.UnsupportedSqlShape;
        } else if (sqlIntervalUnitMonths(unit_text)) |months| {
            saw_calendar = true;
            calendar_total += @as(u128, amount) * @as(u128, months);
            if (calendar_total > std.math.maxInt(u64)) return error.UnsupportedSqlShape;
        } else {
            return error.UnsupportedSqlShape;
        }
        saw_pair = true;
    }
    if (!saw_pair) return error.UnsupportedSqlShape;
    return .{
        .fixed_ns = @intCast(fixed_total),
        .calendar_months = @intCast(calendar_total),
        .saw_fixed = saw_fixed,
        .saw_calendar = saw_calendar,
    };
}

pub fn parseSqlIntervalLiteral(tokens: []const Token, pos: *usize) !SqlIntervalLiteral {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeywordTag(.interval);
    const token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    return try sqlIntervalLiteral(token.text);
}

fn sqlIntervalUnitNs(unit: []const u8) ?u64 {
    if (std.ascii.eqlIgnoreCase(unit, "ns") or
        std.ascii.eqlIgnoreCase(unit, "nanosecond") or
        std.ascii.eqlIgnoreCase(unit, "nanoseconds"))
        return 1;
    if (std.ascii.eqlIgnoreCase(unit, "us") or
        std.ascii.eqlIgnoreCase(unit, "microsecond") or
        std.ascii.eqlIgnoreCase(unit, "microseconds"))
        return 1_000;
    if (std.ascii.eqlIgnoreCase(unit, "ms") or
        std.ascii.eqlIgnoreCase(unit, "millisecond") or
        std.ascii.eqlIgnoreCase(unit, "milliseconds"))
        return 1_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "s") or
        std.ascii.eqlIgnoreCase(unit, "sec") or
        std.ascii.eqlIgnoreCase(unit, "second") or
        std.ascii.eqlIgnoreCase(unit, "seconds"))
        return 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "m") or
        std.ascii.eqlIgnoreCase(unit, "min") or
        std.ascii.eqlIgnoreCase(unit, "minute") or
        std.ascii.eqlIgnoreCase(unit, "minutes"))
        return 60 * 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "h") or
        std.ascii.eqlIgnoreCase(unit, "hr") or
        std.ascii.eqlIgnoreCase(unit, "hour") or
        std.ascii.eqlIgnoreCase(unit, "hours"))
        return 60 * 60 * 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "d") or
        std.ascii.eqlIgnoreCase(unit, "day") or
        std.ascii.eqlIgnoreCase(unit, "days"))
        return 24 * 60 * 60 * 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "w") or
        std.ascii.eqlIgnoreCase(unit, "week") or
        std.ascii.eqlIgnoreCase(unit, "weeks"))
        return 7 * 24 * 60 * 60 * 1_000_000_000;
    return null;
}

fn sqlIntervalUnitMonths(unit: []const u8) ?u64 {
    if (std.ascii.eqlIgnoreCase(unit, "month") or
        std.ascii.eqlIgnoreCase(unit, "months"))
        return 1;
    if (std.ascii.eqlIgnoreCase(unit, "year") or
        std.ascii.eqlIgnoreCase(unit, "years"))
        return 12;
    return null;
}

test "sql adapter value parses timestamp literals" {
    const alloc = std.testing.allocator;

    try std.testing.expectEqual(@as(i64, 0), try parseSqlTimestampLiteralNs("1970-01-01"));
    try std.testing.expectEqual(@as(i64, 1_000_000_000), try parseSqlTimestampLiteralNs("'1970-01-01T00:00:01Z'"));
    try std.testing.expectEqual(@as(i64, 0), try parseSqlTimestampLiteralNs("1970-01-01 01:00:00+01:00"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlTimestampLiteralNs("2026-02-29"));

    const numeric_endpoint = try parseSqlRangeEndpointJsonAlloc(alloc, "42.5", .numeric);
    defer alloc.free(numeric_endpoint);
    try std.testing.expectEqualStrings("42.5", numeric_endpoint);

    const date_endpoint = try parseSqlRangeEndpointJsonAlloc(alloc, "1970-01-02", .datetime);
    defer alloc.free(date_endpoint);
    try std.testing.expectEqualStrings("86400000000000", date_endpoint);

    const typed_datetime_tokens = [_]Token{
        .{ .kind = .identifier, .text = "timestamp" },
        .{ .kind = .string, .text = "1970-01-01T00:00:01Z" },
    };
    var typed_datetime_pos: usize = 0;
    const typed_datetime = try parseSqlTypedDatetimeLiteralValueJsonAlloc(alloc, typed_datetime_tokens[0..], &typed_datetime_pos);
    defer alloc.free(typed_datetime);
    try std.testing.expectEqualStrings("1000000000", typed_datetime);
    try std.testing.expectEqual(@as(usize, 2), typed_datetime_pos);

    const now_tokens = [_]Token{
        .{ .kind = .identifier, .text = "current_timestamp" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .number, .text = "6" },
        .{ .kind = .rparen, .text = ")" },
    };
    var now_pos: usize = 0;
    const now_json = try parseSqlNowValueJsonAlloc(alloc, now_tokens[0..], &now_pos, 123);
    defer alloc.free(now_json);
    try std.testing.expectEqualStrings("123", now_json);
    try std.testing.expectEqual(@as(usize, 4), now_pos);

    var now_call_pos: usize = 0;
    try parseSqlNowCall(now_tokens[0..], &now_call_pos);
    try std.testing.expectEqual(@as(usize, 4), now_call_pos);

    const current_date_tokens = [_]Token{
        .{ .kind = .identifier, .text = "current_date" },
    };
    var current_date_pos: usize = 0;
    const current_date_json = try parseSqlCurrentDateValueJsonAlloc(alloc, current_date_tokens[0..], &current_date_pos, 86_400_000_000_000);
    defer alloc.free(current_date_json);
    try std.testing.expectEqualStrings("86400000000000", current_date_json);
    try std.testing.expectEqual(@as(usize, 1), current_date_pos);
    try std.testing.expectEqual(@as(u64, 86_400_000_000_000), sqlCurrentUtcDateStartNs(86_400_000_000_123));

    var current_date_call_pos: usize = 0;
    try parseSqlCurrentDateKeyword(current_date_tokens[0..], &current_date_call_pos);
    try std.testing.expectEqual(@as(usize, 1), current_date_call_pos);

    const uuid_tokens = [_]Token{
        .{ .kind = .identifier, .text = "gen_random_uuid" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .rparen, .text = ")" },
    };
    var uuid_pos: usize = 0;
    try parseSqlUuidV4Call(uuid_tokens[0..], &uuid_pos);
    try std.testing.expectEqual(@as(usize, 3), uuid_pos);

    const canonical = try canonicalizeDiscreteDateRangeFiniteBoundAlloc(alloc, "0");
    defer alloc.free(canonical);
    try std.testing.expectEqualStrings("86400000000000", canonical);

    const pair = try parseSqlCanonicalRangeLiteralValuePairAlloc(alloc, "(1970-01-01,1970-01-02]", .datetime, .daterange);
    defer lower_expr.freePeriodRangeValuePair(alloc, pair);
    try std.testing.expectEqualStrings("86400000000000", pair.start_json);
    try std.testing.expectEqualStrings("172800000000000", pair.end_json);
}

test "sql adapter value parses scalar json literals" {
    const alloc = std.testing.allocator;
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "true" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .minus, .text = "-" },
        .{ .kind = .number, .text = "42" },
    };

    var bool_pos: usize = 0;
    const bool_json = try parseSqlUntypedValueJsonAlloc(alloc, tokens[0..1], &bool_pos);
    defer alloc.free(bool_json);
    try std.testing.expectEqualStrings("true", bool_json);
    try std.testing.expectEqual(@as(usize, 1), bool_pos);

    var string_pos: usize = 0;
    const string_json = try parseSqlUntypedValueJsonAlloc(alloc, tokens[1..2], &string_pos);
    defer alloc.free(string_json);
    try std.testing.expectEqualStrings("\"active\"", string_json);
    try std.testing.expectEqual(@as(usize, 1), string_pos);

    var negative_pos: usize = 0;
    const negative_json = try parseSqlUntypedValueJsonAlloc(alloc, tokens[2..], &negative_pos);
    defer alloc.free(negative_json);
    try std.testing.expectEqualStrings("-42", negative_json);
    try std.testing.expectEqual(@as(usize, 2), negative_pos);

    var scalar_string_pos: usize = 0;
    const scalar_string_json = (try parseJsonScalarValueAlloc(alloc, tokens[1..2], &scalar_string_pos, &.{})) orelse return error.TestUnexpectedResult;
    defer alloc.free(scalar_string_json);
    try std.testing.expectEqualStrings("\"active\"", scalar_string_json);
    try std.testing.expectEqual(@as(usize, 1), scalar_string_pos);

    const params = [_]SqlValue{.{ .string = "literal" }};
    const string_value_tokens = [_]Token{
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .identifier, .text = "true" },
        .{ .kind = .identifier, .text = "and" },
        .{ .kind = .identifier, .text = "false" },
        .{ .kind = .rparen, .text = ")" },
    };

    var sql_string_pos: usize = 0;
    const parsed = try parseSqlStringValueAlloc(alloc, string_value_tokens[0..1], &sql_string_pos, params[0..]);
    defer alloc.free(parsed);
    try std.testing.expectEqualStrings("literal", parsed);
    try std.testing.expectEqual(@as(usize, 1), sql_string_pos);

    const json_params = [_]SqlValue{ .{ .json = "{\"ok\":true}" }, .{ .string = "{\"from\":\"text\"}" } };
    var document_pos: usize = 0;
    const document_json = (try parseJsonDocumentValueAlloc(alloc, string_value_tokens[0..1], &document_pos, json_params[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(document_json);
    try std.testing.expectEqualStrings("{\"ok\":true}", document_json);
    try std.testing.expectEqual(@as(usize, 1), document_pos);

    var convert_pos: usize = 0;
    const convert_input = (try parseConvertFromInputAlloc(alloc, string_value_tokens[0..1], &convert_pos, json_params[1..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(convert_input);
    try std.testing.expectEqualStrings("{\"from\":\"text\"}", convert_input);
    try std.testing.expectEqual(@as(usize, 1), convert_pos);

    var build_key_string_pos: usize = 0;
    const build_key_string = try parseJsonbBuildObjectKey(tokens[1..2], &build_key_string_pos, &.{});
    try std.testing.expectEqualStrings("active", build_key_string);
    try std.testing.expectEqual(@as(usize, 1), build_key_string_pos);

    var build_key_param_pos: usize = 0;
    const build_key_param = try parseJsonbBuildObjectKey(string_value_tokens[0..1], &build_key_param_pos, params[0..]);
    try std.testing.expectEqualStrings("literal", build_key_param);
    try std.testing.expectEqual(@as(usize, 1), build_key_param_pos);

    const convert_from_tokens = [_]Token{
        .{ .kind = .identifier, .text = "convert_from" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekConvertFromFunctionCall(convert_from_tokens[0..], 0));
    var convert_from_call_pos: usize = 0;
    try parseConvertFromFunctionCallStart(convert_from_tokens[0..], &convert_from_call_pos);
    try std.testing.expectEqual(@as(usize, 2), convert_from_call_pos);

    const to_jsonb_tokens = [_]Token{
        .{ .kind = .identifier, .text = "to_jsonb" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekToJsonbFunctionCall(to_jsonb_tokens[0..], 0));
    var to_jsonb_pos: usize = 0;
    try parseToJsonbFunctionCallStart(to_jsonb_tokens[0..], &to_jsonb_pos);
    try std.testing.expectEqual(@as(usize, 2), to_jsonb_pos);

    const jsonb_build_object_tokens = [_]Token{
        .{ .kind = .identifier, .text = "jsonb_build_object" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekJsonbBuildObjectFunctionCall(jsonb_build_object_tokens[0..], 0));
    var jsonb_build_object_pos: usize = 0;
    try parseJsonbBuildObjectFunctionCallStart(jsonb_build_object_tokens[0..], &jsonb_build_object_pos);
    try std.testing.expectEqual(@as(usize, 2), jsonb_build_object_pos);

    const invalid_key_params = [_]SqlValue{.{ .integer = 42 }};
    var invalid_build_key_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseJsonbBuildObjectKey(string_value_tokens[0..1], &invalid_build_key_pos, invalid_key_params[0..]));

    const single_array_json = try singleValueJsonArrayAlloc(alloc, .{ .string = "literal" });
    defer alloc.free(single_array_json);
    try std.testing.expectEqualStrings("[\"literal\"]", single_array_json);

    var true_pos: usize = 1;
    try std.testing.expectEqual(true, matchStandaloneSqlBooleanLiteral(string_value_tokens[0..], &true_pos).?);
    try std.testing.expectEqual(@as(usize, 2), true_pos);
    try std.testing.expectEqual(false, peekStandaloneSqlBooleanLiteral(string_value_tokens[0..], 3).?);
    try std.testing.expect(peekStandaloneSqlBooleanLiteral(string_value_tokens[0..], 2) == null);

    const bool_column: runtime_schema.RelationalColumn = .{ .name = "enabled", .path = "enabled", .field_type = .boolean };
    const keyword_column: runtime_schema.RelationalColumn = .{ .name = "status", .path = "status", .field_type = .keyword };
    const bool_is_tokens = [_]Token{
        .{ .kind = .identifier, .text = "true" },
        .{ .kind = .identifier, .text = "false" },
        .{ .kind = .identifier, .text = "unknown" },
    };

    var bool_is_pos: usize = 0;
    const bool_is_json = try parseSqlBooleanIsValueAlloc(alloc, bool_is_tokens[0..], &bool_is_pos, bool_column, false);
    defer alloc.free(bool_is_json.?);
    try std.testing.expectEqualStrings("true", bool_is_json.?);
    try std.testing.expectEqual(@as(usize, 1), bool_is_pos);

    var bool_not_pos: usize = 1;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlBooleanIsValueAlloc(alloc, bool_is_tokens[0..], &bool_not_pos, bool_column, true));
    try std.testing.expectEqual(@as(usize, 1), bool_not_pos);

    var non_bool_pos: usize = 0;
    try std.testing.expectError(error.InvalidSqlCatalog, parseSqlBooleanIsValue(bool_is_tokens[0..], &non_bool_pos, keyword_column));

    var unknown_pos: usize = 2;
    try std.testing.expect(try parseSqlBooleanIsUnknown(bool_is_tokens[0..], &unknown_pos, bool_column));
    try std.testing.expectEqual(@as(usize, 3), unknown_pos);
}

test "sql adapter value parses limit offset and fetch values" {
    const params = [_]SqlValue{ .{ .integer = 7 }, .{ .null = {} } };
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "all" },
        .{ .kind = .number, .text = "42" },
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .placeholder, .text = "$2" },
        .{ .kind = .identifier, .text = "rows" },
        .{ .kind = .identifier, .text = "first" },
        .{ .kind = .identifier, .text = "row" },
        .{ .kind = .identifier, .text = "only" },
        .{ .kind = .identifier, .text = "next" },
        .{ .kind = .number, .text = "5" },
        .{ .kind = .identifier, .text = "rows" },
        .{ .kind = .identifier, .text = "only" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .number, .text = "9" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .placeholder, .text = "$1" },
    };

    var all_pos: usize = 0;
    try std.testing.expect((try parseLimitValue(tokens[0..], &all_pos, params[0..])) == null);
    try std.testing.expectEqual(@as(usize, 1), all_pos);

    var limit_pos: usize = 1;
    try std.testing.expectEqual(@as(?u32, 42), try parseLimitValue(tokens[0..], &limit_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 2), limit_pos);

    var param_limit_pos: usize = 2;
    try std.testing.expectEqual(@as(?u32, 7), try parseLimitValue(tokens[0..], &param_limit_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 3), param_limit_pos);

    var offset_pos: usize = 3;
    try std.testing.expectEqual(@as(u32, 0), try parseOffsetValue(tokens[0..], &offset_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 5), offset_pos);

    var fetch_default_pos: usize = 5;
    try std.testing.expectEqual(@as(?u32, 1), try parseFetchLimitValue(tokens[0..], &fetch_default_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 8), fetch_default_pos);

    var fetch_number_pos: usize = 8;
    try std.testing.expectEqual(@as(?u32, 5), try parseFetchLimitValue(tokens[0..], &fetch_number_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 12), fetch_number_pos);

    var plus_limit_pos: usize = 12;
    try std.testing.expectEqual(@as(?u32, 9), try parseLimitValue(tokens[0..], &plus_limit_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 14), plus_limit_pos);

    var plus_param_pos: usize = 14;
    try std.testing.expectEqual(@as(u32, 7), try parseOffsetValue(tokens[0..], &plus_param_pos, params[0..]));
    try std.testing.expectEqual(@as(usize, 16), plus_param_pos);
}

test "sql adapter value coerces text placeholders through column type" {
    const alloc = std.testing.allocator;
    const tokens = [_]Token{
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .placeholder, .text = "$2" },
        .{ .kind = .placeholder, .text = "$3" },
    };
    const params = [_]SqlValue{
        .{ .string = "42" },
        .{ .string = "true" },
        .{ .string = "42" },
    };
    const numeric_column: runtime_schema.RelationalColumn = .{ .name = "amount", .path = "amount", .field_type = .numeric };
    const bool_column: runtime_schema.RelationalColumn = .{ .name = "active", .path = "active", .field_type = .boolean };
    const text_column: runtime_schema.RelationalColumn = .{ .name = "label", .path = "label", .field_type = .text };

    var numeric_pos: usize = 0;
    const numeric_json = try parseSqlColumnValueAlloc(alloc, tokens[0..], &numeric_pos, params[0..], numeric_column, currentRealtimeNs());
    defer alloc.free(numeric_json);
    try std.testing.expectEqualStrings("42", numeric_json);

    var bool_pos: usize = 1;
    const bool_json = try parseSqlColumnValueAlloc(alloc, tokens[0..], &bool_pos, params[0..], bool_column, currentRealtimeNs());
    defer alloc.free(bool_json);
    try std.testing.expectEqualStrings("true", bool_json);

    var text_pos: usize = 2;
    const text_json = try parseSqlColumnValueAlloc(alloc, tokens[0..], &text_pos, params[0..], text_column, currentRealtimeNs());
    defer alloc.free(text_json);
    try std.testing.expectEqualStrings("\"42\"", text_json);
}

test "sql adapter value validates json values and defaults" {
    const alloc = std.testing.allocator;
    const keyword_column: runtime_schema.RelationalColumn = .{ .name = "status", .path = "status", .field_type = .keyword };
    const numeric_column: runtime_schema.RelationalColumn = .{ .name = "amount", .path = "amount", .field_type = .numeric };
    const array_column: runtime_schema.RelationalColumn = .{ .name = "tags", .path = "tags", .field_type = .array };
    const text_array_column: runtime_schema.RelationalColumn = .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .text };

    try validateDefaultValueForColumnAlloc(alloc, keyword_column, .{ .kind = .literal, .value_json = "\"open\"" });
    try validateDefaultValueForColumnAlloc(alloc, numeric_column, .{ .kind = .now_ns, .value_json = "" });
    try validateDefaultValueForColumnAlloc(alloc, numeric_column, .{ .kind = .sequence_next, .value_json = "{\"sequence\":\"usage_id_seq\"}" });
    try std.testing.expectError(error.UnsupportedSqlShape, validateDefaultValueForColumnAlloc(alloc, numeric_column, .{ .kind = .literal, .value_json = "\"bad\"" }));
    try std.testing.expectError(error.UnsupportedSqlShape, validateDefaultValueForColumnAlloc(alloc, array_column, .{ .kind = .uuid_v4, .value_json = "" }));
    try std.testing.expectError(error.UnsupportedSqlShape, validateDefaultValueForColumnAlloc(alloc, keyword_column, .{ .kind = .sequence_next, .value_json = "{\"sequence\":\"usage_id_seq\"}" }));
    try std.testing.expectError(error.UnsupportedSqlShape, relational_rows.relationalDefaultValueJsonAlloc(alloc, .{ .kind = .sequence_next, .value_json = "{\"sequence\":\"usage_id_seq\"}" }));

    try validateSqlArrayElementValueJson(alloc, text_array_column, "\"blue\"");
    try validateSqlArrayValueJson(alloc, text_array_column, "[\"blue\",\"green\"]");
    try validateSqlScalarValuesJson(alloc, keyword_column, "[\"open\",\"closed\"]");
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlArrayElementValueJson(alloc, text_array_column, "42"));
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlArrayValueJson(alloc, text_array_column, "[\"blue\",42]"));
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlScalarValuesJson(alloc, keyword_column, "[\"open\",42]"));
    try std.testing.expectError(error.InvalidSqlCatalog, validateSqlScalarValuesJson(alloc, array_column, "[\"open\"]"));

    try validateJsonDocument(alloc, "{\"a\":1}");
    try validateJsonArray(alloc, "[1,2]");
    try validateJsonStringArray(alloc, "[\"a\",\"b\"]");
    try std.testing.expectError(error.UnsupportedSqlShape, validateJsonDocument(alloc, "\"not-doc\""));
    try std.testing.expectError(error.UnsupportedSqlShape, validateJsonStringArray(alloc, "[\"a\",2]"));

    var parsed_number = try std.json.parseFromSlice(std.json.Value, alloc, "42", .{});
    defer parsed_number.deinit();
    var parsed_string = try std.json.parseFromSlice(std.json.Value, alloc, "\"x\"", .{});
    defer parsed_string.deinit();
    try std.testing.expect(sqlStringIsJsonNumber(alloc, "42"));
    try std.testing.expect(sqlArrayItemValueMatches(.numeric, parsed_number.value));
    try std.testing.expect(sqlScalarValueMatches(.keyword, parsed_string.value));
    try std.testing.expect(jsonValueIsValid(alloc, "{\"ok\":true}"));
    try std.testing.expect(!jsonValueIsValid(alloc, "{bad"));
    const txn_id = [_]u8{ 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10 };
    const txn_hex = encodeSqlTxnIdHex(txn_id);
    try std.testing.expectEqualStrings("0123456789abcdeffedcba9876543210", &txn_hex);
}

test "sql adapter value parses interval literals" {
    const fixed = try sqlIntervalLiteral("1 day 2 hours 3 minutes");
    try std.testing.expectEqual(@as(u64, 93_780_000_000_000), fixed.fixed_ns);
    try std.testing.expectEqual(@as(u64, 0), fixed.calendar_months);
    try std.testing.expect(fixed.saw_fixed);
    try std.testing.expect(!fixed.saw_calendar);

    const mixed = try sqlIntervalLiteral("1 year 2 months 3 days");
    try std.testing.expectEqual(@as(u64, 14), mixed.calendar_months);
    try std.testing.expectEqual(@as(u64, 259_200_000_000_000), mixed.fixed_ns);
    try std.testing.expect(mixed.saw_fixed);
    try std.testing.expect(mixed.saw_calendar);

    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "interval" },
        .{ .kind = .string, .text = "1 hour" },
    };
    var pos: usize = 0;
    const parsed = try parseSqlIntervalLiteral(tokens[0..], &pos);
    try std.testing.expectEqual(@as(u64, 3_600_000_000_000), parsed.fixed_ns);
    try std.testing.expectEqual(@as(u64, 0), parsed.calendar_months);
    try std.testing.expectEqual(@as(usize, 2), pos);

    try std.testing.expectError(error.UnsupportedSqlShape, sqlIntervalLiteral("1 parsec"));
}
