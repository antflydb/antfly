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

const Allocator = std.mem.Allocator;

pub const Config = struct {
    dsn: []u8,
    postgres_table: []u8,
    columns: []source.Column = &.{},

    pub fn deinit(self: *Config, alloc: Allocator) void {
        alloc.free(self.dsn);
        alloc.free(self.postgres_table);
        for (self.columns) |*column| column.deinit(alloc);
        if (self.columns.len > 0) alloc.free(self.columns);
        self.* = undefined;
    }

    pub fn toSourceConfig(self: Config, alloc: Allocator) !source.Config {
        return .{
            .kind = .postgres,
            .dsn = try alloc.dupe(u8, self.dsn),
        };
    }

    pub fn toQueryParams(
        self: Config,
        alloc: Allocator,
        options: QueryParamOptions,
    ) !source.QueryParams {
        return .{
            .table = try alloc.dupe(u8, self.postgres_table),
            .fields = try cloneStringSliceAlloc(alloc, options.fields),
            .filter_query_json = if (options.filter_query_json) |query| try alloc.dupe(u8, query) else null,
            .columns = try cloneColumnsAlloc(alloc, self.columns),
            .limit = options.limit,
            .offset = options.offset,
            .order_by = try cloneSortFieldsAlloc(alloc, options.order_by),
        };
    }
};

pub const NamedConfig = struct {
    name: []u8,
    config: Config,

    pub fn deinit(self: *NamedConfig, alloc: Allocator) void {
        alloc.free(self.name);
        self.config.deinit(alloc);
        self.* = undefined;
    }
};

pub const SourceMap = struct {
    entries: []NamedConfig = &.{},

    pub fn deinit(self: *SourceMap, alloc: Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        if (self.entries.len > 0) alloc.free(self.entries);
        self.* = undefined;
    }

    pub fn get(self: SourceMap, name: []const u8) ?Config {
        for (self.entries) |entry| {
            if (std.mem.eql(u8, entry.name, name)) return entry.config;
        }
        return null;
    }

    pub fn contains(self: SourceMap, name: []const u8) bool {
        return self.get(name) != null;
    }

    pub fn isEmpty(self: SourceMap) bool {
        return self.entries.len == 0;
    }
};

pub const QueryParamOptions = struct {
    fields: []const []const u8 = &.{},
    filter_query_json: ?[]const u8 = null,
    limit: ?usize = null,
    offset: usize = 0,
    order_by: []const source.SortField = &.{},
};

pub fn mapFromPublicOpenApi(alloc: Allocator, foreign_sources: anytype) !SourceMap {
    return try mapFromAnyOpenApi(alloc, foreign_sources);
}

pub fn mapFromMetadataOpenApi(alloc: Allocator, foreign_sources: anytype) !SourceMap {
    return try mapFromAnyOpenApi(alloc, foreign_sources);
}

pub fn fromPublicOpenApi(alloc: Allocator, foreign_source: anytype) !Config {
    return try fromAnyOpenApiForeignSource(alloc, foreign_source.type, foreign_source.dsn, foreign_source.postgres_table, foreign_source.columns);
}

pub fn fromMetadataOpenApi(alloc: Allocator, foreign_source: anytype) !Config {
    return try fromAnyOpenApiForeignSource(alloc, foreign_source.type, foreign_source.dsn, foreign_source.postgres_table, foreign_source.columns);
}

fn fromAnyOpenApiForeignSource(
    alloc: Allocator,
    source_type: []const u8,
    dsn: []const u8,
    postgres_table: []const u8,
    columns: anytype,
) !Config {
    if (!std.mem.eql(u8, source_type, "postgres")) return error.UnsupportedSourceKind;
    return .{
        .dsn = try alloc.dupe(u8, dsn),
        .postgres_table = try alloc.dupe(u8, postgres_table),
        .columns = try cloneOpenApiColumnsAlloc(alloc, columns),
    };
}

fn cloneOpenApiColumnsAlloc(alloc: Allocator, columns: anytype) ![]source.Column {
    const openapi_columns = switch (@typeInfo(@TypeOf(columns))) {
        .optional => columns orelse return &.{},
        else => columns,
    };
    const out = try alloc.alloc(source.Column, openapi_columns.len);
    errdefer {
        for (out[0..]) |*column| column.deinit(alloc);
        alloc.free(out);
    }
    for (openapi_columns, 0..) |column, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, column.name),
            .data_type = try alloc.dupe(u8, column.type),
            .nullable = column.nullable orelse false,
        };
    }
    return out;
}

fn cloneColumnsAlloc(alloc: Allocator, columns: []const source.Column) ![]source.Column {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(source.Column, columns.len);
    errdefer {
        for (out[0..]) |*column| column.deinit(alloc);
        alloc.free(out);
    }
    for (columns, 0..) |column, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, column.name),
            .data_type = try alloc.dupe(u8, column.data_type),
            .nullable = column.nullable,
        };
    }
    return out;
}

fn cloneStringSliceAlloc(alloc: Allocator, values: []const []const u8) ![][]u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]u8, values.len);
    errdefer {
        for (out[0..]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
    }
    return out;
}

fn cloneSortFieldsAlloc(alloc: Allocator, values: []const source.SortField) ![]source.SortField {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(source.SortField, values.len);
    errdefer {
        for (out[0..]) |*value| value.deinit(alloc);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = .{
            .field = try alloc.dupe(u8, value.field),
            .desc = value.desc,
        };
    }
    return out;
}

fn mapFromAnyOpenApi(alloc: Allocator, foreign_sources: anytype) !SourceMap {
    const source_map = switch (@typeInfo(@TypeOf(foreign_sources))) {
        .optional => foreign_sources orelse return .{},
        else => foreign_sources,
    };
    const count = source_map.map.count();
    if (count == 0) return .{};

    const entries = try alloc.alloc(NamedConfig, count);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    var it = source_map.map.iterator();
    while (it.next()) |entry| {
        entries[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .config = try fromPublicOpenApi(alloc, entry.value_ptr.*),
        };
        initialized += 1;
    }
    return .{ .entries = entries };
}

test "postgres config adapts public openapi foreign source" {
    const alloc = std.testing.allocator;
    const openapi_columns = [_]struct {
        name: []const u8,
        type: []const u8,
        nullable: ?bool,
    }{
        .{ .name = "id", .type = "uuid", .nullable = false },
        .{ .name = "name", .type = "text", .nullable = true },
    };
    var config = try fromPublicOpenApi(alloc, .{
        .type = "postgres",
        .dsn = "postgres://db",
        .postgres_table = "customers",
        .columns = openapi_columns[0..],
    });
    defer config.deinit(alloc);

    try std.testing.expectEqualStrings("postgres://db", config.dsn);
    try std.testing.expectEqualStrings("customers", config.postgres_table);
    try std.testing.expectEqual(@as(usize, 2), config.columns.len);
    try std.testing.expectEqualStrings("id", config.columns[0].name);
    try std.testing.expectEqualStrings("uuid", config.columns[0].data_type);
    try std.testing.expectEqual(false, config.columns[0].nullable);
}

test "postgres config builds source and query params" {
    const alloc = std.testing.allocator;
    var config = Config{
        .dsn = try alloc.dupe(u8, "postgres://db"),
        .postgres_table = try alloc.dupe(u8, "customers"),
        .columns = try alloc.alloc(source.Column, 1),
    };
    config.columns[0] = .{
        .name = try alloc.dupe(u8, "id"),
        .data_type = try alloc.dupe(u8, "uuid"),
        .nullable = false,
    };
    defer config.deinit(alloc);

    var source_config = try config.toSourceConfig(alloc);
    defer source_config.deinit(alloc);
    try std.testing.expectEqual(source.SourceKind.postgres, source_config.kind);

    const order_by = [_]source.SortField{
        .{ .field = @constCast("created_at"), .desc = true },
    };
    var params = try config.toQueryParams(alloc, .{
        .fields = &.{ "id", "name" },
        .filter_query_json = "{\"term\":{\"field\":\"id\",\"term\":\"cust:a\"}}",
        .limit = 5,
        .offset = 10,
        .order_by = &order_by,
    });
    defer params.deinit(alloc);

    try std.testing.expectEqualStrings("customers", params.table);
    try std.testing.expectEqual(@as(usize, 2), params.fields.len);
    try std.testing.expectEqual(@as(usize, 1), params.columns.len);
    try std.testing.expectEqual(@as(?usize, 5), params.limit);
    try std.testing.expectEqual(@as(usize, 10), params.offset);
    try std.testing.expectEqual(@as(usize, 1), params.order_by.len);
}

test "postgres source map adapts foreign_sources map" {
    const alloc = std.testing.allocator;
    const Sources = std.json.ArrayHashMap(struct {
        type: []const u8,
        dsn: []const u8,
        postgres_table: []const u8,
        columns: ?[]const struct {
            name: []const u8,
            type: []const u8,
            nullable: ?bool,
        } = null,
    });
    var parsed = try std.json.parseFromSlice(Sources, alloc,
        \\{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers"}}
    , .{});
    defer parsed.deinit();

    var source_map = try mapFromPublicOpenApi(alloc, parsed.value);
    defer source_map.deinit(alloc);

    try std.testing.expect(source_map.contains("pg_customers"));
    try std.testing.expect(!source_map.contains("missing"));
    try std.testing.expectEqualStrings("customers", source_map.get("pg_customers").?.postgres_table);
}
