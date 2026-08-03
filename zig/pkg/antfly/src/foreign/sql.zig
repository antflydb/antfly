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

pub const PlaceholderStyle = enum {
    dollar_numbered,
    question_mark,
};

pub const Dialect = struct {
    name: []const u8,
    placeholder_style: PlaceholderStyle,
    quote_identifier: *const fn (alloc: std.mem.Allocator, name: []const u8) anyerror![]u8,
    quote_relation: *const fn (alloc: std.mem.Allocator, name: []const u8) anyerror![]u8,
};

pub const SqlSourceConfig = struct {
    kind: source.SourceKind,
    dsn: []u8,
};

pub const SelectStatementOptions = struct {
    table: []const u8,
    fields: []const []const u8 = &.{},
    where_sql: ?[]const u8 = null,
    order_by: []const source.SortField = &.{},
    limit: ?usize = null,
    offset: usize = 0,
};

pub const ParameterValue = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []u8,

    pub fn deinit(self: *ParameterValue, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .string => |text| alloc.free(text),
            else => {},
        }
        self.* = undefined;
    }
};

pub const PreparedQuery = struct {
    sql_text: []u8,
    args: []ParameterValue = &.{},

    pub fn deinit(self: *PreparedQuery, alloc: std.mem.Allocator) void {
        alloc.free(self.sql_text);
        for (self.args) |*arg| arg.deinit(alloc);
        if (self.args.len > 0) alloc.free(self.args);
        self.* = undefined;
    }
};

pub fn postgresDialect() Dialect {
    return .{
        .name = "postgres",
        .placeholder_style = .dollar_numbered,
        .quote_identifier = quotePostgresIdentifierAlloc,
        .quote_relation = quotePostgresRelationAlloc,
    };
}

pub fn placeholderAlloc(
    alloc: std.mem.Allocator,
    style: PlaceholderStyle,
    index: usize,
) ![]u8 {
    return switch (style) {
        .dollar_numbered => std.fmt.allocPrint(alloc, "${d}", .{index}),
        .question_mark => alloc.dupe(u8, "?"),
    };
}

pub fn buildSelectStatementAlloc(
    alloc: std.mem.Allocator,
    dialect: Dialect,
    options: SelectStatementOptions,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "SELECT ");
    if (options.fields.len == 0) {
        try out.append(alloc, '*');
    } else {
        for (options.fields, 0..) |field, i| {
            if (i != 0) try out.appendSlice(alloc, ", ");
            const quoted = try dialect.quote_identifier(alloc, field);
            defer alloc.free(quoted);
            try out.appendSlice(alloc, quoted);
        }
    }

    try out.appendSlice(alloc, " FROM ");
    const quoted_table = try dialect.quote_relation(alloc, options.table);
    defer alloc.free(quoted_table);
    try out.appendSlice(alloc, quoted_table);

    if (options.where_sql) |where_sql| {
        try out.appendSlice(alloc, " WHERE ");
        try out.appendSlice(alloc, where_sql);
    }

    if (options.order_by.len > 0) {
        try out.appendSlice(alloc, " ORDER BY ");
        for (options.order_by, 0..) |sort_field, i| {
            if (i != 0) try out.appendSlice(alloc, ", ");
            const quoted_field = try dialect.quote_identifier(alloc, sort_field.field);
            defer alloc.free(quoted_field);
            try out.appendSlice(alloc, quoted_field);
            try out.appendSlice(alloc, if (sort_field.desc) " DESC" else " ASC");
        }
    }

    if (options.limit) |limit| {
        const limit_sql = try std.fmt.allocPrint(alloc, " LIMIT {d}", .{limit});
        defer alloc.free(limit_sql);
        try out.appendSlice(alloc, limit_sql);
    }
    if (options.offset != 0) {
        const offset_sql = try std.fmt.allocPrint(alloc, " OFFSET {d}", .{options.offset});
        defer alloc.free(offset_sql);
        try out.appendSlice(alloc, offset_sql);
    }

    return try out.toOwnedSlice(alloc);
}

fn quotePostgresIdentifierAlloc(alloc: std.mem.Allocator, name: []const u8) ![]u8 {
    if (name.len == 0 or std.mem.indexOfScalar(u8, name, 0) != null)
        return error.InvalidQueryRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '"');
    for (name) |ch| {
        if (ch == '"') try out.append(alloc, '"');
        try out.append(alloc, ch);
    }
    try out.append(alloc, '"');
    return try out.toOwnedSlice(alloc);
}

/// Canonicalizes the public PostgreSQL relation syntax used by foreign-source
/// configuration. A dot separates schema and relation names; double quotes
/// allow dots inside a component and use PostgreSQL's doubled-quote escaping.
/// Every component is emitted quoted so query, statistics, and discovery all
/// resolve exactly the same relation.
pub fn quotePostgresRelationAlloc(alloc: std.mem.Allocator, name: []const u8) ![]u8 {
    if (name.len == 0 or std.mem.indexOfScalar(u8, name, 0) != null)
        return error.InvalidQueryRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pos: usize = 0;
    var component_count: u8 = 0;
    while (pos < name.len) {
        if (component_count == 2) return error.InvalidQueryRequest;
        if (component_count != 0) try out.append(alloc, '.');
        try out.append(alloc, '"');

        var component_len: usize = 0;
        if (name[pos] == '"') {
            pos += 1;
            var closed = false;
            while (pos < name.len) {
                if (name[pos] != '"') {
                    try out.append(alloc, name[pos]);
                    component_len += 1;
                    pos += 1;
                    continue;
                }
                if (pos + 1 < name.len and name[pos + 1] == '"') {
                    try out.appendSlice(alloc, "\"\"");
                    component_len += 1;
                    pos += 2;
                    continue;
                }
                pos += 1;
                closed = true;
                break;
            }
            if (!closed or component_len == 0) return error.InvalidQueryRequest;
            if (pos < name.len and name[pos] != '.') return error.InvalidQueryRequest;
        } else {
            const start = pos;
            while (pos < name.len and name[pos] != '.') : (pos += 1) {
                if (name[pos] == '"') return error.InvalidQueryRequest;
            }
            if (pos == start) return error.InvalidQueryRequest;
            component_len = pos - start;
            for (name[start..pos]) |ch| {
                if (ch == '"') try out.append(alloc, '"');
                try out.append(alloc, ch);
            }
        }
        try out.append(alloc, '"');
        component_count += 1;

        if (pos == name.len) break;
        pos += 1;
        if (pos == name.len) return error.InvalidQueryRequest;
    }
    if (component_count == 0) return error.InvalidQueryRequest;
    return try out.toOwnedSlice(alloc);
}

test "postgres dialect quotes identifiers" {
    const alloc = std.testing.allocator;
    const quoted = try postgresDialect().quote_identifier(alloc, "customer\"name");
    defer alloc.free(quoted);
    try std.testing.expectEqualStrings("\"customer\"\"name\"", quoted);
}

test "postgres relation quoting canonicalizes qualified and quoted names" {
    const alloc = std.testing.allocator;
    const qualified = try quotePostgresRelationAlloc(alloc, "public.users");
    defer alloc.free(qualified);
    try std.testing.expectEqualStrings("\"public\".\"users\"", qualified);

    const quoted = try quotePostgresRelationAlloc(alloc, "\"tenant.data\".\"User\"\"Events\"");
    defer alloc.free(quoted);
    try std.testing.expectEqualStrings("\"tenant.data\".\"User\"\"Events\"", quoted);

    const exact = try quotePostgresRelationAlloc(alloc, "Users-Prod");
    defer alloc.free(exact);
    try std.testing.expectEqualStrings("\"Users-Prod\"", exact);

    try std.testing.expectError(error.InvalidQueryRequest, quotePostgresRelationAlloc(alloc, "public."));
    try std.testing.expectError(error.InvalidQueryRequest, quotePostgresRelationAlloc(alloc, "one.two.three"));
    try std.testing.expectError(error.InvalidQueryRequest, quotePostgresRelationAlloc(alloc, "\"unterminated"));
}

test "placeholder helper supports postgres style" {
    const alloc = std.testing.allocator;
    const placeholder = try placeholderAlloc(alloc, .dollar_numbered, 3);
    defer alloc.free(placeholder);
    try std.testing.expectEqualStrings("$3", placeholder);
}

test "select builder emits postgres-compatible query shape" {
    const alloc = std.testing.allocator;
    const order_by = [_]source.SortField{
        .{ .field = @constCast("last_name"), .desc = false },
        .{ .field = @constCast("created_at"), .desc = true },
    };
    const sql = try buildSelectStatementAlloc(alloc, postgresDialect(), .{
        .table = "customers",
        .fields = &.{ "id", "first_name" },
        .where_sql = "\"id\" = $1",
        .order_by = &order_by,
        .limit = 10,
        .offset = 20,
    });
    defer alloc.free(sql);
    try std.testing.expectEqualStrings(
        "SELECT \"id\", \"first_name\" FROM \"customers\" WHERE \"id\" = $1 ORDER BY \"last_name\" ASC, \"created_at\" DESC LIMIT 10 OFFSET 20",
        sql,
    );
}

test "select builder emits a schema-qualified PostgreSQL relation" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidQueryRequest,
        buildSelectStatementAlloc(alloc, postgresDialect(), .{ .table = "audit.customer.events" }),
    );

    const quoted = try buildSelectStatementAlloc(alloc, postgresDialect(), .{
        .table = "\"audit.data\".events",
    });
    defer alloc.free(quoted);
    try std.testing.expectEqualStrings("SELECT * FROM \"audit.data\".\"events\"", quoted);
}
