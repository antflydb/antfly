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
const source = @import("source.zig");
const sql = @import("sql.zig");

const Allocator = std.mem.Allocator;
const TranslationError = anyerror;

pub const Translation = struct {
    where_sql: []u8,
    args: []sql.ParameterValue,

    pub fn deinit(self: *Translation, alloc: Allocator) void {
        alloc.free(self.where_sql);
        for (self.args) |*arg| arg.deinit(alloc);
        if (self.args.len > 0) alloc.free(self.args);
        self.* = undefined;
    }
};

pub fn translateAlloc(
    alloc: Allocator,
    dialect: sql.Dialect,
    filter_query_json: []const u8,
    known_columns: []const source.Column,
) !Translation {
    if (filter_query_json.len == 0) {
        return .{
            .where_sql = try alloc.dupe(u8, ""),
            .args = &.{},
        };
    }

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{});
    defer parsed.deinit();

    var generator = SqlGenerator{
        .alloc = alloc,
        .dialect = dialect,
        .known_columns = known_columns,
    };
    const where_sql = try generator.valueToSqlAlloc(parsed.value);
    return .{
        .where_sql = where_sql,
        .args = try generator.args.toOwnedSlice(alloc),
    };
}

const SqlGenerator = struct {
    alloc: Allocator,
    dialect: sql.Dialect,
    known_columns: []const source.Column,
    args: std.ArrayListUnmanaged(sql.ParameterValue) = .empty,

    fn valueToSqlAlloc(self: *SqlGenerator, value: std.json.Value) TranslationError![]u8 {
        if (value != .object) return error.InvalidQueryRequest;
        return try self.objectToSqlAlloc(value.object);
    }

    fn objectToSqlAlloc(self: *SqlGenerator, obj: std.json.ObjectMap) TranslationError![]u8 {
        if (obj.get("match_all") != null) return try self.alloc.dupe(u8, "");
        if (obj.get("match_none") != null) return try self.alloc.dupe(u8, "FALSE");

        if (obj.get("query")) |query| {
            const text = expectString(query) orelse return error.InvalidQueryRequest;
            return try self.queryStringToSqlAlloc(text);
        }
        if (obj.get("conjuncts") != null or obj.get("disjuncts") != null or obj.get("must_not") != null) {
            return try self.boolObjectToSqlAlloc(obj);
        }
        if (obj.get("term")) |term| return try self.directEqualityToSqlAlloc(obj, "term", term);
        if (obj.get("match")) |match| return try self.directEqualityToSqlAlloc(obj, "match", match);
        if (obj.get("bool")) |boolean| return try self.booleanEqualityToSqlAlloc(obj, boolean);
        if (obj.get("prefix")) |prefix| return try self.patternToSqlAlloc(obj, "prefix", prefix);
        if (obj.get("wildcard")) |wildcard| return try self.patternToSqlAlloc(obj, "wildcard", wildcard);
        if (obj.get("field") != null and (obj.get("min") != null or obj.get("max") != null)) {
            return try self.rangeToSqlAlloc(obj);
        }
        return error.UnsupportedQueryRequest;
    }

    fn directEqualityToSqlAlloc(
        self: *SqlGenerator,
        parent: std.json.ObjectMap,
        comptime key: []const u8,
        raw_value: std.json.Value,
    ) TranslationError![]u8 {
        const field_name = fieldFromParentOrSingleObject(parent, raw_value) orelse return error.InvalidQueryRequest;
        try self.validateField(field_name);
        const value = scalarFromParentOrSingleObject(key, raw_value) orelse return error.InvalidQueryRequest;
        const field_sql = try self.dialect.quote_identifier(self.alloc, field_name);
        defer self.alloc.free(field_sql);
        const placeholder = try self.appendPlaceholderAlloc(value);
        defer self.alloc.free(placeholder);
        return try std.fmt.allocPrint(self.alloc, "{s} = {s}", .{ field_sql, placeholder });
    }

    fn booleanEqualityToSqlAlloc(self: *SqlGenerator, parent: std.json.ObjectMap, raw_value: std.json.Value) TranslationError![]u8 {
        const field_name = expectField(parent) orelse return error.InvalidQueryRequest;
        try self.validateField(field_name);
        if (raw_value != .bool) return error.InvalidQueryRequest;
        const field_sql = try self.dialect.quote_identifier(self.alloc, field_name);
        defer self.alloc.free(field_sql);
        const placeholder = try self.appendPlaceholderAlloc(raw_value);
        defer self.alloc.free(placeholder);
        return try std.fmt.allocPrint(self.alloc, "{s} = {s}", .{ field_sql, placeholder });
    }

    fn patternToSqlAlloc(
        self: *SqlGenerator,
        parent: std.json.ObjectMap,
        comptime key: []const u8,
        raw_value: std.json.Value,
    ) TranslationError![]u8 {
        const field_name = fieldFromParentOrSingleObject(parent, raw_value) orelse return error.InvalidQueryRequest;
        try self.validateField(field_name);
        const raw_pattern = switch (scalarFromParentOrSingleObject(key, raw_value) orelse return error.InvalidQueryRequest) {
            .string => |text| text,
            else => return error.InvalidQueryRequest,
        };
        const field_sql = try self.dialect.quote_identifier(self.alloc, field_name);
        defer self.alloc.free(field_sql);
        const pattern = if (std.mem.eql(u8, key, "prefix")) blk: {
            const escaped = try escapeLikeAlloc(self.alloc, raw_pattern);
            defer self.alloc.free(escaped);
            break :blk try std.fmt.allocPrint(self.alloc, "{s}%", .{escaped});
        } else blk: {
            break :blk try wildcardToLikeAlloc(self.alloc, raw_pattern);
        };
        defer self.alloc.free(pattern);
        const placeholder = try self.appendPlaceholderValueAlloc(.{ .string = try self.alloc.dupe(u8, pattern) });
        defer self.alloc.free(placeholder);
        return try std.fmt.allocPrint(self.alloc, "{s} LIKE {s} ESCAPE '\\'", .{ field_sql, placeholder });
    }

    fn rangeToSqlAlloc(self: *SqlGenerator, obj: std.json.ObjectMap) TranslationError![]u8 {
        const field_name = expectField(obj) orelse return error.InvalidQueryRequest;
        try self.validateField(field_name);
        const field_sql = try self.dialect.quote_identifier(self.alloc, field_name);
        defer self.alloc.free(field_sql);

        var parts = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (parts.items) |part| self.alloc.free(part);
            parts.deinit(self.alloc);
        }

        if (obj.get("min")) |min| {
            const inclusive = jsonBoolOrDefault(obj.get("inclusive_min"), true);
            const placeholder = try self.appendPlaceholderAlloc(min);
            defer self.alloc.free(placeholder);
            try parts.append(self.alloc, try std.fmt.allocPrint(
                self.alloc,
                "{s} {s} {s}",
                .{ field_sql, if (inclusive) ">=" else ">", placeholder },
            ));
        }
        if (obj.get("max")) |max| {
            const inclusive = jsonBoolOrDefault(obj.get("inclusive_max"), true);
            const placeholder = try self.appendPlaceholderAlloc(max);
            defer self.alloc.free(placeholder);
            try parts.append(self.alloc, try std.fmt.allocPrint(
                self.alloc,
                "{s} {s} {s}",
                .{ field_sql, if (inclusive) "<=" else "<", placeholder },
            ));
        }

        return try std.mem.join(self.alloc, " AND ", parts.items);
    }

    fn boolObjectToSqlAlloc(self: *SqlGenerator, obj: std.json.ObjectMap) TranslationError![]u8 {
        var parts = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (parts.items) |part| self.alloc.free(part);
            parts.deinit(self.alloc);
        }

        if (obj.get("conjuncts")) |conjuncts| {
            const rendered = try self.renderJoinedClausesAlloc(conjuncts, " AND ");
            defer self.alloc.free(rendered);
            if (rendered.len > 0) try parts.append(self.alloc, try std.fmt.allocPrint(self.alloc, "{s}", .{rendered}));
        }
        if (obj.get("disjuncts")) |disjuncts| {
            const rendered = try self.renderJoinedClausesAlloc(disjuncts, " OR ");
            defer self.alloc.free(rendered);
            if (rendered.len > 0) try parts.append(self.alloc, try std.fmt.allocPrint(self.alloc, "({s})", .{rendered}));
        }
        if (obj.get("must_not")) |must_not| {
            const clause = try self.valueToSqlAlloc(must_not);
            defer self.alloc.free(clause);
            if (clause.len > 0) try parts.append(self.alloc, try std.fmt.allocPrint(self.alloc, "NOT ({s})", .{clause}));
        }

        return try std.mem.join(self.alloc, " AND ", parts.items);
    }

    fn renderJoinedClausesAlloc(self: *SqlGenerator, value: std.json.Value, joiner: []const u8) TranslationError![]u8 {
        if (value != .array) return error.InvalidQueryRequest;
        var rendered = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (rendered.items) |item| self.alloc.free(item);
            rendered.deinit(self.alloc);
        }
        for (value.array.items) |item| {
            const clause = try self.valueToSqlAlloc(item);
            errdefer self.alloc.free(clause);
            if (clause.len == 0) {
                self.alloc.free(clause);
                continue;
            }
            try rendered.append(self.alloc, try std.fmt.allocPrint(self.alloc, "({s})", .{clause}));
            self.alloc.free(clause);
        }
        return try std.mem.join(self.alloc, joiner, rendered.items);
    }

    fn queryStringToSqlAlloc(self: *SqlGenerator, query_text: []const u8) TranslationError![]u8 {
        const trimmed = std.mem.trim(u8, query_text, &std.ascii.whitespace);
        if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "*")) return try self.alloc.dupe(u8, "");

        var terms = std.ArrayListUnmanaged(QueryStringTerm).empty;
        defer {
            for (terms.items) |*term| term.deinit(self.alloc);
            terms.deinit(self.alloc);
        }

        var it = std.mem.splitSequence(u8, trimmed, " OR ");
        while (it.next()) |piece| {
            try terms.append(self.alloc, try parseQueryStringTermAlloc(self.alloc, std.mem.trim(u8, piece, &std.ascii.whitespace)));
        }
        if (terms.items.len == 0) return try self.alloc.dupe(u8, "");

        if (allSameQueryStringField(terms.items)) |field_name| {
            try self.validateField(field_name);
            const field_sql = try self.dialect.quote_identifier(self.alloc, field_name);
            defer self.alloc.free(field_sql);
            if (terms.items.len == 1) return try self.queryStringTermToSqlAlloc(field_name, terms.items[0]);

            var placeholders = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (placeholders.items) |placeholder| self.alloc.free(placeholder);
                placeholders.deinit(self.alloc);
            }
            for (terms.items) |term| {
                if (term.kind != .equal) return error.UnsupportedQueryRequest;
                try placeholders.append(self.alloc, try self.appendPlaceholderValueAlloc(.{ .string = try self.alloc.dupe(u8, term.value) }));
            }
            const joined = try std.mem.join(self.alloc, ", ", placeholders.items);
            defer self.alloc.free(joined);
            return try std.fmt.allocPrint(self.alloc, "{s} IN ({s})", .{ field_sql, joined });
        }

        var parts = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (parts.items) |part| self.alloc.free(part);
            parts.deinit(self.alloc);
        }
        for (terms.items) |term| {
            const clause = try self.queryStringTermToSqlAlloc(term.field, term);
            defer self.alloc.free(clause);
            try parts.append(self.alloc, try std.fmt.allocPrint(self.alloc, "({s})", .{clause}));
        }
        return try std.mem.join(self.alloc, " OR ", parts.items);
    }

    fn queryStringTermToSqlAlloc(self: *SqlGenerator, field_name: []const u8, term: QueryStringTerm) TranslationError![]u8 {
        try self.validateField(field_name);
        const field_sql = try self.dialect.quote_identifier(self.alloc, field_name);
        defer self.alloc.free(field_sql);
        const value = switch (term.kind) {
            .equal => try self.alloc.dupe(u8, term.value),
            .prefix => blk: {
                const escaped = try escapeLikeAlloc(self.alloc, term.value);
                defer self.alloc.free(escaped);
                break :blk try std.fmt.allocPrint(self.alloc, "{s}%", .{escaped});
            },
            .wildcard => try wildcardToLikeAlloc(self.alloc, term.value),
        };
        defer self.alloc.free(value);
        const placeholder = try self.appendPlaceholderValueAlloc(.{ .string = try self.alloc.dupe(u8, value) });
        defer self.alloc.free(placeholder);
        return switch (term.kind) {
            .equal => try std.fmt.allocPrint(self.alloc, "{s} = {s}", .{ field_sql, placeholder }),
            .prefix, .wildcard => try std.fmt.allocPrint(self.alloc, "{s} LIKE {s} ESCAPE '\\'", .{ field_sql, placeholder }),
        };
    }

    fn appendPlaceholderAlloc(self: *SqlGenerator, value: std.json.Value) ![]u8 {
        return try self.appendPlaceholderValueAlloc(try parameterValueFromJsonAlloc(self.alloc, value));
    }

    fn appendPlaceholderValueAlloc(self: *SqlGenerator, value: sql.ParameterValue) ![]u8 {
        try self.args.append(self.alloc, value);
        return try sql.placeholderAlloc(self.alloc, self.dialect.placeholder_style, self.args.items.len);
    }

    fn validateField(self: *SqlGenerator, field_name: []const u8) !void {
        if (self.known_columns.len == 0) return;
        for (self.known_columns) |column| {
            if (std.mem.eql(u8, column.name, field_name)) return;
        }
        return error.UnknownColumn;
    }
};

fn fieldFromParentOrSingleObject(parent: std.json.ObjectMap, raw_value: std.json.Value) ?[]const u8 {
    if (expectField(parent)) |field| return field;
    if (raw_value != .object or raw_value.object.count() != 1) return null;
    var it = raw_value.object.iterator();
    const entry = it.next() orelse return null;
    return entry.key_ptr.*;
}

fn scalarFromParentOrSingleObject(comptime key: []const u8, raw_value: std.json.Value) ?std.json.Value {
    _ = key;
    if (raw_value != .object) return raw_value;
    if (raw_value.object.get("text")) |value| return value;
    if (raw_value.object.get("term")) |value| return value;
    if (raw_value.object.get("match")) |value| return value;
    if (raw_value.object.get("prefix")) |value| return value;
    if (raw_value.object.get("wildcard")) |value| return value;
    if (raw_value.object.count() == 1) {
        var it = raw_value.object.iterator();
        const entry = it.next() orelse return null;
        return entry.value_ptr.*;
    }
    return null;
}

fn parameterValueFromJsonAlloc(alloc: Allocator, value: std.json.Value) !sql.ParameterValue {
    return switch (value) {
        .null => .null,
        .bool => |flag| .{ .bool = flag },
        .integer => |number| .{ .integer = number },
        .float => |number| .{ .float = number },
        .number_string => |text| .{ .string = try alloc.dupe(u8, text) },
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        else => error.UnsupportedQueryRequest,
    };
}

fn expectField(obj: std.json.ObjectMap) ?[]const u8 {
    const value = obj.get("field") orelse return null;
    return expectString(value);
}

fn expectString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => value.string,
        else => null,
    };
}

fn jsonBoolOrDefault(value: ?std.json.Value, default: bool) bool {
    const inner = value orelse return default;
    return switch (inner) {
        .bool => inner.bool,
        else => default,
    };
}

fn escapeLikeAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (input) |ch| {
        if (ch == '\\' or ch == '%' or ch == '_') try out.append(alloc, '\\');
        try out.append(alloc, ch);
    }
    return try out.toOwnedSlice(alloc);
}

fn wildcardToLikeAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (input) |ch| switch (ch) {
        '*' => try out.append(alloc, '%'),
        '?' => try out.append(alloc, '_'),
        '\\', '%', '_' => {
            try out.append(alloc, '\\');
            try out.append(alloc, ch);
        },
        else => try out.append(alloc, ch),
    };
    return try out.toOwnedSlice(alloc);
}

const QueryStringKind = enum {
    equal,
    prefix,
    wildcard,
};

const QueryStringTerm = struct {
    field: []u8,
    value: []u8,
    kind: QueryStringKind,

    fn deinit(self: *QueryStringTerm, alloc: Allocator) void {
        alloc.free(self.field);
        alloc.free(self.value);
        self.* = undefined;
    }
};

fn parseQueryStringTermAlloc(alloc: Allocator, input: []const u8) !QueryStringTerm {
    const split = splitFieldValue(input) orelse return error.UnsupportedQueryRequest;
    const field = try unescapeQueryStringAlloc(alloc, split.field);
    errdefer alloc.free(field);
    const value = try unescapeQueryStringAlloc(alloc, split.value);
    errdefer alloc.free(value);
    const kind: QueryStringKind = if (value.len > 0 and std.mem.indexOfScalar(u8, value, '?') != null) .wildcard else if (value.len > 0 and value[value.len - 1] == '*')
        .prefix
    else if (std.mem.indexOfScalar(u8, value, '*') != null)
        .wildcard
    else
        .equal;

    if (kind == .prefix) {
        alloc.free(value);
        return .{
            .field = field,
            .value = try alloc.dupe(u8, value[0 .. value.len - 1]),
            .kind = .prefix,
        };
    }
    return .{
        .field = field,
        .value = value,
        .kind = kind,
    };
}

fn splitFieldValue(input: []const u8) ?struct { field: []const u8, value: []const u8 } {
    var escaped = false;
    for (input, 0..) |ch, idx| {
        if (escaped) {
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == ':') {
            if (idx == 0 or idx + 1 >= input.len) return null;
            return .{
                .field = input[0..idx],
                .value = input[idx + 1 ..],
            };
        }
    }
    return null;
}

fn unescapeQueryStringAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var escaped = false;
    for (input) |ch| {
        if (escaped) {
            try out.append(alloc, ch);
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        try out.append(alloc, ch);
    }
    if (escaped) try out.append(alloc, '\\');
    return try out.toOwnedSlice(alloc);
}

fn allSameQueryStringField(items: []const QueryStringTerm) ?[]const u8 {
    if (items.len == 0) return null;
    const first_field = items[0].field;
    for (items[1..]) |item| {
        if (!std.mem.eql(u8, item.field, first_field)) return null;
    }
    return first_field;
}

test "translate filter term and range to parameterized sql" {
    const alloc = std.testing.allocator;
    const columns = [_]source.Column{
        .{ .name = @constCast("status"), .data_type = @constCast("text"), .nullable = false },
        .{ .name = @constCast("age"), .data_type = @constCast("integer"), .nullable = false },
    };

    var translated = try translateAlloc(
        alloc,
        sql.postgresDialect(),
        "{\"conjuncts\":[{\"term\":\"active\",\"field\":\"status\"},{\"field\":\"age\",\"min\":21}]}",
        &columns,
    );
    defer translated.deinit(alloc);

    try std.testing.expectEqualStrings("(\"status\" = $1) AND (\"age\" >= $2)", translated.where_sql);
    try std.testing.expectEqual(@as(usize, 2), translated.args.len);
    try std.testing.expectEqualStrings("active", translated.args[0].string);
    try std.testing.expectEqual(@as(i64, 21), translated.args[1].integer);
}

test "translate filter supports prefix wildcard and query string" {
    const alloc = std.testing.allocator;
    const columns = [_]source.Column{
        .{ .name = @constCast("name"), .data_type = @constCast("text"), .nullable = false },
        .{ .name = @constCast("id"), .data_type = @constCast("uuid"), .nullable = false },
    };

    var prefix_translated = try translateAlloc(
        alloc,
        sql.postgresDialect(),
        "{\"prefix\":\"100%_done\",\"field\":\"name\"}",
        &columns,
    );
    defer prefix_translated.deinit(alloc);
    try std.testing.expectEqualStrings("\"name\" LIKE $1 ESCAPE '\\'", prefix_translated.where_sql);
    try std.testing.expectEqualStrings("100\\%\\_done%", prefix_translated.args[0].string);

    var qs_translated = try translateAlloc(
        alloc,
        sql.postgresDialect(),
        "{\"query\":\"id:1 OR id:2 OR id:3\"}",
        &columns,
    );
    defer qs_translated.deinit(alloc);
    try std.testing.expectEqualStrings("\"id\" IN ($1, $2, $3)", qs_translated.where_sql);
    try std.testing.expectEqual(@as(usize, 3), qs_translated.args.len);
}

test "translate filter rejects unknown fields when columns are known" {
    const alloc = std.testing.allocator;
    const columns = [_]source.Column{
        .{ .name = @constCast("status"), .data_type = @constCast("text"), .nullable = false },
    };

    try std.testing.expectError(
        error.UnknownColumn,
        translateAlloc(
            alloc,
            sql.postgresDialect(),
            "{\"term\":\"active\",\"field\":\"missing\"}",
            &columns,
        ),
    );
}
