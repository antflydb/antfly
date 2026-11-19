// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Read-only PostgreSQL catalog projections over Antfly's type registry.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_ast;
const Element = local.sql_array_value.ElementType;

const pg_type_name = "pg_type";
const pg_type_storage = "\x00pg_catalog.pg_type";
const pg_catalog_oid = 11;
const pg_auth_members_storage = "\x00pg_catalog.pg_auth_members";
const pg_roles_storage = "\x00pg_catalog.pg_roles";
pub const Role = struct {
    name: []const u8,
    replicate: bool,
};

pub fn resolve(name: ast.Name, action: catalog.Action, database: []const u8) !?catalog.Table {
    const roles = std.mem.eql(u8, name.table, "pg_roles");
    const members = std.mem.eql(u8, name.table, "pg_auth_members");
    if (!roles and !members and !std.mem.eql(u8, name.table, pg_type_name)) return null;
    if (name.namespace) |namespace| if (!std.mem.eql(u8, namespace, "pg_catalog")) return null;
    if (name.database) |qualified| if (!std.mem.eql(u8, qualified, database)) return error.Forbidden;
    if (action != .read) return error.SqlReadOnlyTransaction;
    if (members) return .{
        .id = 1261,
        .physical_name = pg_auth_members_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_auth_members", .revision = 1 },
        .columns = &.{
            .{ .name = "roleid", .path = "roleid", .type = .integer, .nullable = false },
            .{ .name = "member", .path = "member", .type = .integer, .nullable = false },
            .{ .name = "grantor", .path = "grantor", .type = .integer, .nullable = false },
            .{ .name = "admin_option", .path = "admin_option", .type = .boolean, .nullable = false },
            .{ .name = "inherit_option", .path = "inherit_option", .type = .boolean, .nullable = false },
            .{ .name = "set_option", .path = "set_option", .type = .boolean, .nullable = false },
        },
    };
    if (roles) return .{
        .id = 12000,
        .physical_name = pg_roles_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_roles", .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "rolname", .path = "rolname", .type = .string, .nullable = false },
            .{ .name = "rolsuper", .path = "rolsuper", .type = .boolean, .nullable = false },
            .{ .name = "rolinherit", .path = "rolinherit", .type = .boolean, .nullable = false },
            .{ .name = "rolcreaterole", .path = "rolcreaterole", .type = .boolean, .nullable = false },
            .{ .name = "rolcreatedb", .path = "rolcreatedb", .type = .boolean, .nullable = false },
            .{ .name = "rolcanlogin", .path = "rolcanlogin", .type = .boolean, .nullable = false },
            .{ .name = "rolreplication", .path = "rolreplication", .type = .boolean, .nullable = false },
        },
    };
    return .{
        .id = 1247, // PostgreSQL's pg_type relation OID.
        .physical_name = pg_type_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = pg_type_name, .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "typname", .path = "typname", .type = .string, .nullable = false },
            .{ .name = "typnamespace", .path = "typnamespace", .type = .integer, .nullable = false },
            .{ .name = "typlen", .path = "typlen", .type = .integer, .element_type = .int16, .nullable = false },
            .{ .name = "typtype", .path = "typtype", .type = .string, .nullable = false },
            .{ .name = "typcategory", .path = "typcategory", .type = .string, .nullable = false },
            .{ .name = "typnotnull", .path = "typnotnull", .type = .boolean, .nullable = false },
            .{ .name = "typinput", .path = "typinput", .type = .string, .nullable = false },
            .{ .name = "typelem", .path = "typelem", .type = .integer, .nullable = false },
            .{ .name = "typarray", .path = "typarray", .type = .integer, .nullable = false },
            .{ .name = "typbasetype", .path = "typbasetype", .type = .integer, .nullable = false },
            .{ .name = "typtypmod", .path = "typtypmod", .type = .integer, .element_type = .int32, .nullable = false },
        },
    };
}

pub fn owns(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_type_storage) or isRoles(table) or isMemberships(table);
}

pub fn isMemberships(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_auth_members_storage);
}

/// The session role projects effective Antfly grants. There are no
/// independently assumable PostgreSQL roles in the projected catalog.
pub fn openMemberships(context: *anyopaque) catalog.Cursor {
    const Empty = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn close(_: *anyopaque) void {}
    };
    return .{ .ptr = context, .next = Empty.next, .close = Empty.close, .estimated_rows = 0 };
}

pub fn isRoles(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_roles_storage);
}

const roleOid = local.sql_scalar.postgresRoleOid;

fn rolePage(a: std.mem.Allocator, role: Role, done: bool) !catalog.Page {
    if (done) return .{ .rows = &.{} };
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var value: std.json.ObjectMap = .empty;
    try value.put(owned, "oid", .{ .integer = roleOid(role.name) });
    try value.put(owned, "rolname", .{ .string = try owned.dupe(u8, role.name) });
    inline for (.{ "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb", "rolcanlogin" }) |flag| {
        try value.put(owned, flag, .{ .bool = std.mem.eql(u8, flag, "rolcanlogin") });
    }
    try value.put(owned, "rolreplication", .{ .bool = role.replicate });
    const rows = try owned.dupe(catalog.Row, &.{.{ .id = try std.fmt.allocPrint(owned, "{d}", .{roleOid(role.name)}), .version = 1, .value = .{ .object = value } }});
    return .{ .rows = rows, .owned_arena = arena };
}

pub fn scanRole(a: std.mem.Allocator, request: catalog.Scan, role: Role) !catalog.Page {
    if (request.search != null or request.after != null) return error.UnsupportedSqlShape;
    return rolePage(a, role, false);
}

pub fn openRole(a: std.mem.Allocator, request: catalog.Scan, role: Role) !catalog.Cursor {
    if (request.search != null or request.after != null) return error.UnsupportedSqlShape;
    const RoleCursor = struct {
        allocator: std.mem.Allocator,
        role: Role,
        done: bool = false,
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = try rolePage(alloc, self.role, self.done);
            self.done = true;
            return result;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.allocator.free(self.role.name);
            self.allocator.destroy(self);
        }
    };
    const cursor = try a.create(RoleCursor);
    errdefer a.destroy(cursor);
    cursor.* = .{ .allocator = a, .role = .{ .name = try a.dupe(u8, role.name), .replicate = role.replicate } };
    return .{ .ptr = cursor, .next = RoleCursor.next, .close = RoleCursor.close, .estimated_rows = 1 };
}

const Type = struct {
    oid: u32,
    name: []const u8,
    category: []const u8,
    length: i16,
    input: []const u8,
    element: u32 = 0,
    array: u32 = 0,

    fn builtin(element: Element, name: []const u8, category: []const u8, length: i16, input: []const u8) Type {
        return .{ .oid = element.oid(), .name = name, .category = category, .length = length, .input = input, .array = element.arrayOid() };
    }

    fn arrayType(item: Type, comptime name: []const u8) Type {
        return .{ .oid = item.array, .name = name, .category = "A", .length = -1, .input = "array_in", .element = item.oid };
    }
};

const base_types = [_]Type{
    .builtin(.boolean, "bool", "B", 1, "boolin"),
    .builtin(.int64, "int8", "N", 8, "int8in"),
    .builtin(.int16, "int2", "N", 2, "int2in"),
    .builtin(.int32, "int4", "N", 4, "int4in"),
    .builtin(.text, "text", "S", -1, "textin"),
    .builtin(.float32, "float4", "N", 4, "float4in"),
    .builtin(.float64, "float8", "N", 8, "float8in"),
    .{ .oid = 1043, .name = "varchar", .category = "S", .length = -1, .input = "varcharin" },
    .{ .oid = 1184, .name = "timestamptz", .category = "D", .length = 8, .input = "timestamptz_in" },
    .builtin(.numeric, "numeric", "N", -1, "numeric_in"),
    .builtin(.uuid, "uuid", "U", 16, "uuid_in"),
    .builtin(.jsonb, "jsonb", "U", -1, "jsonb_in"),
};

const types = blk: {
    @setEvalBranchQuota(10_000);
    var all: [base_types.len + @typeInfo(Element).@"enum".field_names.len]Type = undefined;
    var count: usize = 0;
    for (base_types) |item| {
        all[count] = item;
        count += 1;
        if (item.array != 0) {
            all[count] = item.arrayType("_" ++ item.name);
            count += 1;
        }
    }
    if (count != all.len) @compileError("pg_type registry omits a supported array type");
    std.mem.sort(Type, &all, {}, struct {
        fn less(_: void, left: Type, right: Type) bool {
            return left.oid < right.oid;
        }
    }.less);
    break :blk all;
};

fn row(alloc: std.mem.Allocator, item: Type) !catalog.Row {
    var value: std.json.ObjectMap = .empty;
    try value.put(alloc, "oid", .{ .integer = item.oid });
    try value.put(alloc, "typname", .{ .string = item.name });
    try value.put(alloc, "typnamespace", .{ .integer = pg_catalog_oid });
    try value.put(alloc, "typlen", .{ .integer = item.length });
    try value.put(alloc, "typtype", .{ .string = "b" });
    try value.put(alloc, "typcategory", .{ .string = item.category });
    try value.put(alloc, "typnotnull", .{ .bool = false });
    try value.put(alloc, "typinput", .{ .string = item.input });
    try value.put(alloc, "typelem", .{ .integer = item.element });
    try value.put(alloc, "typarray", .{ .integer = item.array });
    try value.put(alloc, "typbasetype", .{ .integer = 0 });
    try value.put(alloc, "typtypmod", .{ .integer = -1 });
    return .{ .id = try std.fmt.allocPrint(alloc, "{d:0>10}", .{item.oid}), .version = 1, .value = .{ .object = value } };
}

fn startOf(after: ?[]const u8) !usize {
    const token = after orelse return 0;
    const index = std.fmt.parseInt(usize, token, 10) catch return error.InvalidSqlBackendResponse;
    if (index > types.len) return error.InvalidSqlBackendResponse;
    return index;
}

fn page(a: std.mem.Allocator, start: usize, limit: u32) !catalog.Page {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const end = @min(types.len, start + limit);
    const rows = try owned.alloc(catalog.Row, end - start);
    for (types[start..end], rows) |item, *result| result.* = try row(owned, item);
    return .{ .rows = rows, .owned_arena = arena, .after = if (end < types.len) try std.fmt.allocPrint(owned, "{d}", .{end}) else null };
}

pub fn scan(a: std.mem.Allocator, request: catalog.Scan) !catalog.Page {
    if (request.search != null) return error.UnsupportedSqlShape;
    return page(a, try startOf(request.after), request.limit);
}

const Cursor = struct {
    alloc: std.mem.Allocator,
    index: usize,

    fn next(raw: *anyopaque, a: std.mem.Allocator, limit: u32) !catalog.Page {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        const result = try page(a, self.index, limit);
        self.index += result.rows.len;
        return result;
    }

    fn close(raw: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        self.alloc.destroy(self);
    }
};

pub fn open(a: std.mem.Allocator, request: catalog.Scan) !catalog.Cursor {
    if (request.search != null) return error.UnsupportedSqlShape;
    const index = try startOf(request.after);
    const cursor = try a.create(Cursor);
    cursor.* = .{ .alloc = a, .index = index };
    return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close, .estimated_rows = types.len - index };
}

test "pgwire pg_type reflects the scalar and array wire OIDs" {
    const alloc = std.testing.allocator;
    try std.testing.expect((try resolve(.{ .namespace = "public", .table = "pg_type" }, .read, "default")) == null);
    try std.testing.expectError(error.SqlReadOnlyTransaction, resolve(.{ .table = "pg_type" }, .write, "default"));
    const projected = (try resolve(.{ .namespace = "pg_catalog", .table = "pg_type" }, .read, "default")).?;
    try std.testing.expect(owns(projected));
    var rows = try scan(alloc, .{ .fields = &.{}, .limit = 64 });
    defer rows.deinit();
    try std.testing.expectEqual(types.len, rows.rows.len);
    const expected = Element.int32;
    for (rows.rows) |item| {
        const fields = item.value.object;
        if (fields.get("oid").?.integer == expected.arrayOid()) {
            try std.testing.expectEqualStrings("_int4", fields.get("typname").?.string);
            try std.testing.expectEqual(@as(i64, expected.oid()), fields.get("typelem").?.integer);
            return;
        }
    }
    return error.TestExpectedArrayType;
}

test "pgwire pg_roles describes only the supplied authenticated role" {
    const a = std.testing.allocator;
    const projected = (try resolve(.{ .namespace = "pg_catalog", .table = "pg_roles" }, .read, "default")).?;
    try std.testing.expect(isRoles(projected));
    try std.testing.expectError(error.SqlReadOnlyTransaction, resolve(.{ .table = "pg_roles" }, .write, "default"));
    const request: catalog.Scan = .{ .fields = &.{}, .limit = 1 };
    var cursor = try openRole(a, request, .{ .name = "reader", .replicate = false });
    defer cursor.close(cursor.ptr);
    var first = try cursor.next(cursor.ptr, a, 1);
    defer first.deinit();
    const fields = first.rows[0].value.object;
    try std.testing.expectEqualStrings("reader", fields.get("rolname").?.string);
    try std.testing.expectEqual(@as(i64, roleOid("reader")), fields.get("oid").?.integer);
    try std.testing.expect(!fields.get("rolreplication").?.bool);
    try std.testing.expect(!fields.get("rolsuper").?.bool);
    var exhausted = try cursor.next(cursor.ptr, a, 1);
    defer exhausted.deinit();
    try std.testing.expectEqual(@as(usize, 0), exhausted.rows.len);
}
