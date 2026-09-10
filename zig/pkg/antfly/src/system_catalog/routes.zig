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
const domain = @import("domain.zig");
const helpers = @import("../api/http_route_helpers.zig");

pub const Route = struct {
    kind: domain.Kind,
    database: []const u8 = domain.default_database_name,
    namespace: []const u8 = domain.default_namespace_name,
    name: ?[]const u8 = null,
    suffix: []const u8 = "",

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        if (self.kind == .namespace or self.kind == .table) alloc.free(self.database);
        if (self.kind == .table) alloc.free(self.namespace);
        if (self.name) |name| alloc.free(name);
    }

    pub fn target(self: @This()) !domain.Target {
        if (self.kind != .table) return error.InvalidCatalogName;
        return .{ .database = self.database, .namespace = self.namespace, .table = self.name orelse return error.InvalidCatalogName };
    }
};

/// Decode each component once. Escaped delimiters in literal table names
/// remain part of that component and cannot change the selected scope.
pub fn parseAlloc(alloc: std.mem.Allocator, path: []const u8) !?Route {
    var parts = std.mem.splitScalar(u8, std.mem.trimStart(u8, path, "/"), '/');
    const first = parts.next() orelse return null;
    if (std.mem.eql(u8, first, "tablespaces")) {
        const raw = parts.next() orelse return .{ .kind = .tablespace };
        return .{ .kind = .tablespace, .name = try nameAlloc(alloc, raw), .suffix = parts.rest() };
    }
    if (!std.mem.eql(u8, first, "databases")) return null;
    const db_raw = parts.next() orelse return .{ .kind = .database };
    const db = try nameAlloc(alloc, db_raw);
    errdefer alloc.free(db);
    const db_suffix = parts.rest();
    const second = parts.next() orelse return .{ .kind = .database, .name = db, .database = db };
    if (!std.mem.eql(u8, second, "namespaces")) return .{ .kind = .database, .name = db, .database = db, .suffix = db_suffix };
    const ns_raw = parts.next() orelse return .{ .kind = .namespace, .database = db };
    const ns = try nameAlloc(alloc, ns_raw);
    errdefer alloc.free(ns);
    const ns_suffix = parts.rest();
    const third = parts.next() orelse return .{ .kind = .namespace, .name = ns, .database = db, .namespace = ns };
    if (!std.mem.eql(u8, third, "tables")) return .{ .kind = .namespace, .name = ns, .database = db, .namespace = ns, .suffix = ns_suffix };
    const table_raw = parts.next() orelse return .{ .kind = .table, .database = db, .namespace = ns };
    return .{ .kind = .table, .name = try tableNameAlloc(alloc, table_raw), .database = db, .namespace = ns, .suffix = parts.rest() };
}

fn nameAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    const value = try helpers.decodePercentEncodedPathComponentAlloc(alloc, raw);
    errdefer alloc.free(value);
    domain.validateName(value) catch return error.InvalidArgument;
    return value;
}

fn tableNameAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    const value = try helpers.decodePercentEncodedPathComponentAlloc(alloc, raw);
    errdefer alloc.free(value);
    domain.validateTableName(value) catch return error.InvalidArgument;
    return value;
}

pub fn resourceNameAlloc(alloc: std.mem.Allocator, route: Route) ![]u8 {
    const name = route.name orelse "*";
    return switch (route.kind) {
        .database, .tablespace => alloc.dupe(u8, name),
        .namespace => std.fmt.allocPrint(alloc, "{s}.{s}", .{ route.database, name }),
        .table => (domain.TableScope{ .database = route.database, .namespace = route.namespace, .table = route.name }).keyAlloc(alloc),
    };
}

pub const tableResourceMatches = domain.tableResourceMatches;

test "system catalog routes decode each scope once and retain literal delimiters" {
    const alloc = std.testing.allocator;
    const route = (try parseAlloc(alloc, "/databases/namespaces/namespaces/serving/tables/ev%65nts/query")).?;
    defer route.deinit(alloc);
    try std.testing.expectEqualStrings("namespaces", route.database);
    try std.testing.expectEqualStrings("events", route.name.?);
    try std.testing.expectEqualStrings("query", route.suffix);
    const literal = (try parseAlloc(alloc, "/databases/db/namespaces/ns/tables/a%2Fb")).?;
    defer literal.deinit(alloc);
    try std.testing.expectEqualStrings("a/b", literal.name.?);
    try std.testing.expectError(error.InvalidArgument, parseAlloc(alloc, "/databases/db%2Fx/namespaces/ns/tables/a"));
    const key = try (try literal.target()).resourceNameAlloc(alloc);
    defer alloc.free(key);
    const scope = try (domain.TableScope{ .database = "db", .namespace = "ns" }).keyAlloc(alloc);
    defer alloc.free(scope);
    try std.testing.expect(tableResourceMatches(scope, key));
    try std.testing.expect(!tableResourceMatches("db.ns.*", key));
    try std.testing.expect(!tableResourceMatches("a/b", key));
}
