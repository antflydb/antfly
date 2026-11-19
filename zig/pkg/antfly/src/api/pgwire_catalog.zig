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
const RelationOid = local.sql_compiler.PostgresCatalogRelation;
const Element = local.sql_array_value.ElementType;

const pg_type_name = "pg_type";
const pg_type_storage = "\x00pg_catalog.pg_type";
const pg_catalog_oid = 11;
const pg_namespace_storage = "\x00pg_catalog.pg_namespace";
const pg_auth_members_storage = "\x00pg_catalog.pg_auth_members";
const pg_publication_storage = "\x00pg_catalog.pg_publication";
const pg_replication_slots_storage = "\x00pg_catalog.pg_replication_slots";
const pg_attribute_storage = "\x00pg_catalog.pg_attribute";
const pg_attrdef_storage = "\x00pg_catalog.pg_attrdef";
const pg_class_storage = "\x00pg_catalog.pg_class";
const pg_description_storage = "\x00pg_catalog.pg_description";
const pg_enum_storage = "\x00pg_catalog.pg_enum";
const pg_roles_storage = "\x00pg_catalog.pg_roles";
pub const Role = struct {
    name: []const u8,
    replicate: bool,
};

pub fn resolve(name: ast.Name, action: catalog.Action, database: []const u8) !?catalog.Table {
    const roles = std.mem.eql(u8, name.table, "pg_roles");
    const members = std.mem.eql(u8, name.table, "pg_auth_members");
    const namespaces = std.mem.eql(u8, name.table, "pg_namespace");
    const enums = std.mem.eql(u8, name.table, "pg_enum");
    const slots = std.mem.eql(u8, name.table, "pg_replication_slots");
    const publications = std.mem.eql(u8, name.table, "pg_publication");
    const descriptions = std.mem.eql(u8, name.table, "pg_description");
    const classes = std.mem.eql(u8, name.table, "pg_class");
    const attributes = std.mem.eql(u8, name.table, "pg_attribute");
    const defaults = std.mem.eql(u8, name.table, "pg_attrdef");
    if (!attributes and !defaults and !classes and !descriptions and !roles and !members and !namespaces and !enums and !slots and !publications and !std.mem.eql(u8, name.table, pg_type_name)) return null;
    if (name.namespace) |namespace| if (!std.mem.eql(u8, namespace, "pg_catalog")) return null;
    if (name.database) |qualified| if (!std.mem.eql(u8, qualified, database)) return error.Forbidden;
    if (action != .read) return error.SqlReadOnlyTransaction;
    if (namespaces) return .{
        .id = @backingInt(RelationOid.pg_namespace),
        .physical_name = pg_namespace_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_namespace", .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "nspname", .path = "nspname", .type = .string, .nullable = false },
        },
    };
    if (publications) return .{
        .id = @backingInt(RelationOid.pg_publication),
        .physical_name = pg_publication_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_publication", .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "pubname", .path = "pubname", .type = .string, .nullable = false },
            .{ .name = "pubowner", .path = "pubowner", .type = .integer, .nullable = false },
            .{ .name = "puballtables", .path = "puballtables", .type = .boolean, .nullable = false },
            .{ .name = "pubinsert", .path = "pubinsert", .type = .boolean, .nullable = false },
            .{ .name = "pubupdate", .path = "pubupdate", .type = .boolean, .nullable = false },
            .{ .name = "pubdelete", .path = "pubdelete", .type = .boolean, .nullable = false },
            .{ .name = "pubtruncate", .path = "pubtruncate", .type = .boolean, .nullable = false },
            .{ .name = "pubviaroot", .path = "pubviaroot", .type = .boolean, .nullable = false },
        },
    };
    if (slots) return .{
        .id = @backingInt(RelationOid.pg_replication_slots),
        .physical_name = pg_replication_slots_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_replication_slots", .revision = 1 },
        .columns = &.{
            .{ .name = "slot_name", .path = "slot_name", .type = .string, .nullable = false },
            .{ .name = "plugin", .path = "plugin", .type = .string, .nullable = false },
            .{ .name = "slot_type", .path = "slot_type", .type = .string, .nullable = false },
            .{ .name = "datoid", .path = "datoid", .type = .integer, .nullable = true },
            .{ .name = "database", .path = "database", .type = .string, .nullable = false },
            .{ .name = "temporary", .path = "temporary", .type = .boolean, .nullable = false },
            .{ .name = "active", .path = "active", .type = .boolean, .nullable = false },
            .{ .name = "active_pid", .path = "active_pid", .type = .integer, .nullable = true },
            .{ .name = "xmin", .path = "xmin", .type = .integer, .nullable = true },
            .{ .name = "catalog_xmin", .path = "catalog_xmin", .type = .integer, .nullable = true },
            .{ .name = "restart_lsn", .path = "restart_lsn", .type = .string, .nullable = true },
            .{ .name = "confirmed_flush_lsn", .path = "confirmed_flush_lsn", .type = .string, .nullable = true },
            .{ .name = "wal_status", .path = "wal_status", .type = .string, .nullable = false },
            .{ .name = "safe_wal_size", .path = "safe_wal_size", .type = .integer, .nullable = true },
            .{ .name = "two_phase", .path = "two_phase", .type = .boolean, .nullable = false },
            .{ .name = "conflicting", .path = "conflicting", .type = .boolean, .nullable = false },
        },
    };
    if (defaults) return .{
        .id = @backingInt(RelationOid.pg_attrdef),
        .physical_name = pg_attrdef_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_attrdef", .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "adrelid", .path = "adrelid", .type = .integer, .nullable = false },
            .{ .name = "adnum", .path = "adnum", .type = .integer, .element_type = .int16, .nullable = false },
            .{ .name = "adbin", .path = "adbin", .type = .string, .nullable = false },
        },
    };
    if (attributes) return .{
        .id = @backingInt(RelationOid.pg_attribute),
        .physical_name = pg_attribute_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_attribute", .revision = 1 },
        .columns = &.{
            .{ .name = "attrelid", .path = "attrelid", .type = .integer, .nullable = false },
            .{ .name = "attname", .path = "attname", .type = .string, .nullable = false },
            .{ .name = "atttypid", .path = "atttypid", .type = .integer, .nullable = false },
            .{ .name = "attlen", .path = "attlen", .type = .integer, .element_type = .int16, .nullable = false },
            .{ .name = "attnum", .path = "attnum", .type = .integer, .element_type = .int16, .nullable = false },
            .{ .name = "atttypmod", .path = "atttypmod", .type = .integer, .element_type = .int32, .nullable = false },
            .{ .name = "attnotnull", .path = "attnotnull", .type = .boolean, .nullable = false },
            .{ .name = "atthasdef", .path = "atthasdef", .type = .boolean, .nullable = false },
            .{ .name = "attisdropped", .path = "attisdropped", .type = .boolean, .nullable = false },
            .{ .name = "attidentity", .path = "attidentity", .type = .string, .nullable = false },
            .{ .name = "attgenerated", .path = "attgenerated", .type = .string, .nullable = false },
        },
    };
    if (classes) return .{
        .id = @backingInt(RelationOid.pg_class),
        .physical_name = pg_class_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_class", .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "relname", .path = "relname", .type = .string, .nullable = false },
            .{ .name = "relnamespace", .path = "relnamespace", .type = .integer, .nullable = false },
            .{ .name = "relkind", .path = "relkind", .type = .string, .nullable = false },
        },
    };
    if (descriptions) return .{
        .id = @backingInt(RelationOid.pg_description),
        .physical_name = pg_description_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_description", .revision = 1 },
        .columns = &.{
            .{ .name = "objoid", .path = "objoid", .type = .integer, .nullable = false },
            .{ .name = "classoid", .path = "classoid", .type = .integer, .nullable = false },
            .{ .name = "objsubid", .path = "objsubid", .type = .integer, .element_type = .int32, .nullable = false },
            .{ .name = "description", .path = "description", .type = .string, .nullable = false },
        },
    };
    if (enums) return .{
        .id = @backingInt(RelationOid.pg_enum),
        .physical_name = pg_enum_storage,
        .schema_version = 1,
        .scope = .{ .database = database, .namespace = "pg_catalog", .name = "pg_enum", .revision = 1 },
        .columns = &.{
            .{ .name = "oid", .path = "oid", .type = .integer, .nullable = false },
            .{ .name = "enumtypid", .path = "enumtypid", .type = .integer, .nullable = false },
            .{ .name = "enumsortorder", .path = "enumsortorder", .type = .number, .element_type = .float32, .nullable = false },
            .{ .name = "enumlabel", .path = "enumlabel", .type = .string, .nullable = false },
        },
    };
    if (members) return .{
        .id = @backingInt(RelationOid.pg_auth_members),
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
        .id = @backingInt(RelationOid.pg_roles),
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
        .id = @backingInt(RelationOid.pg_type), // PostgreSQL's pg_type relation OID.
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
    return std.mem.eql(u8, table.physical_name, pg_type_storage) or isRoles(table) or isEmpty(table) or isNamespaces(table) or isReplicationSlots(table) or isPublications(table) or isClasses(table) or isAttributes(table);
}

pub fn isClasses(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_class_storage);
}

pub const Class = struct {
    oid: u32,
    name: []const u8,
    namespace: u32,
    kind: []const u8 = "r",
    logical: ?struct { namespace: []const u8, id: u64, revision: u64 } = null,

    pub fn matches(self: Class, conditions: []const catalog.Condition) !bool {
        for (conditions) |condition| {
            const field = std.meta.stringToEnum(enum { oid, relname, relnamespace, relkind }, condition.column) orelse return error.InvalidSqlBackendResponse;
            const value: std.json.Value = switch (field) {
                .oid => .{ .integer = self.oid },
                .relname => .{ .string = self.name },
                .relnamespace => .{ .integer = self.namespace },
                .relkind => .{ .string = self.kind },
            };
            if (!try conditionMatches(value, condition)) return false;
        }
        return true;
    }

    fn page(a: std.mem.Allocator, items: []const Class, start: usize, limit: u32) !catalog.Page {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const end = @min(items.len, start +| limit);
        const rows = try alloc.alloc(catalog.Row, end - start);
        for (items[start..end], rows) |item, *output| {
            var attrs: std.json.ObjectMap = .empty;
            try attrs.put(alloc, "oid", .{ .integer = item.oid });
            try attrs.put(alloc, "relname", .{ .string = try alloc.dupe(u8, item.name) });
            try attrs.put(alloc, "relnamespace", .{ .integer = item.namespace });
            try attrs.put(alloc, "relkind", .{ .string = try alloc.dupe(u8, item.kind) });
            output.* = .{ .id = try std.fmt.allocPrint(alloc, "{d:0>10}", .{item.oid}), .version = 1, .value = .{ .object = attrs } };
        }
        return .{ .rows = rows, .owned_arena = arena, .after = if (end < items.len) try std.fmt.allocPrint(alloc, "{d}", .{end}) else null };
    }
};

pub fn scanClasses(a: std.mem.Allocator, request: catalog.Scan, items: []const Class) !catalog.Page {
    if (request.search != null) return error.UnsupportedSqlShape;
    return Class.page(a, items, try startOf(request.after, items.len), request.limit);
}

pub fn openRows(comptime RowType: type, request: catalog.Scan, arena: std.heap.ArenaAllocator, items: []const RowType) !catalog.Cursor {
    var owned = arena;
    errdefer owned.deinit();
    if (request.search != null) return error.UnsupportedSqlShape;
    const start = try startOf(request.after, items.len);
    const Classes = struct {
        arena: std.heap.ArenaAllocator,
        items: []const RowType,
        index: usize,
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = try RowType.page(alloc, self.items, self.index, limit);
            self.index += result.rows.len;
            return result;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var resources = self.arena;
            resources.deinit();
        }
    };
    const state = try owned.allocator().create(Classes);
    state.* = .{ .arena = owned, .items = items, .index = start };
    return .{ .ptr = state, .next = Classes.next, .close = Classes.close, .estimated_rows = items.len - start };
}

pub fn isAttributes(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_attribute_storage);
}

pub const Attribute = struct {
    relation: u32,
    ordinal: u16,
    column: catalog.Column,

    fn attrs(self: Attribute, alloc: std.mem.Allocator) !std.json.ObjectMap {
        const column = self.column;
        const element: Element = column.element_type orelse switch (column.type) {
            .integer => .int64,
            .number => .float64,
            .boolean => .boolean,
            .json => .jsonb,
            .uuid => .uuid,
            .string, .datetime => .text,
            .array => return error.InvalidSqlBackendResponse,
        };
        const oid: u32 = if (column.type == .array) element.arrayOid() else if (column.type == .datetime) 1184 else element.oid();
        const length: i64 = for (types) |item| {
            if (item.oid == oid) break item.length;
        } else return error.InvalidSqlBackendResponse;
        const modifier: i64 = if (column.numeric_modifier) |m|
            try m.postgres()
        else
            -1;
        var values: std.json.ObjectMap = .empty;
        try values.put(alloc, "attrelid", .{ .integer = self.relation });
        try values.put(alloc, "attname", .{ .string = try alloc.dupe(u8, column.name) });
        try values.put(alloc, "atttypid", .{ .integer = oid });
        try values.put(alloc, "attlen", .{ .integer = length });
        try values.put(alloc, "attnum", .{ .integer = self.ordinal });
        try values.put(alloc, "atttypmod", .{ .integer = modifier });
        try values.put(alloc, "attnotnull", .{ .bool = !column.nullable });
        try values.put(alloc, "atthasdef", .{ .bool = column.defaulted or column.generated });
        try values.put(alloc, "attisdropped", .{ .bool = false });
        try values.put(alloc, "attidentity", .{ .string = "" });
        try values.put(alloc, "attgenerated", .{ .string = if (column.generated) "s" else "" });
        return values;
    }

    pub fn matches(self: Attribute, a: std.mem.Allocator, conditions: []const catalog.Condition) !bool {
        if (conditions.len == 0) return true;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const values = try self.attrs(arena.allocator());
        for (conditions) |condition|
            if (!try conditionMatches(values.get(condition.column) orelse return error.InvalidSqlBackendResponse, condition)) return false;
        return true;
    }

    fn page(a: std.mem.Allocator, items: []const Attribute, start: usize, limit: u32) !catalog.Page {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const end = @min(items.len, start +| limit);
        const rows = try alloc.alloc(catalog.Row, end - start);
        for (items[start..end], rows) |item, *output| output.* = .{
            .id = try std.fmt.allocPrint(alloc, "{d:0>10}:{d:0>5}", .{ item.relation, item.ordinal }),
            .version = 1,
            .value = .{ .object = try item.attrs(alloc) },
        };
        return .{ .rows = rows, .owned_arena = arena, .after = if (end < items.len) try std.fmt.allocPrint(alloc, "{d}", .{end}) else null };
    }
};

pub fn scanAttributes(a: std.mem.Allocator, request: catalog.Scan, items: []const Attribute) !catalog.Page {
    if (request.search != null) return error.UnsupportedSqlShape;
    return Attribute.page(a, items, try startOf(request.after, items.len), request.limit);
}

pub fn isPublications(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_publication_storage);
}

pub fn publicationPage(a: std.mem.Allocator, request: catalog.Scan, name: []const u8, oid: u32, owner: u32) !catalog.Page {
    if (request.after != null or request.limit == 0) return .{ .rows = &.{} };
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    var attrs: std.json.ObjectMap = .empty;
    const owned_name = try alloc.dupe(u8, name);
    try attrs.put(alloc, "oid", .{ .integer = oid });
    try attrs.put(alloc, "pubname", .{ .string = owned_name });
    try attrs.put(alloc, "pubowner", .{ .integer = owner });
    inline for (.{ "pubinsert", "pubupdate", "pubdelete" }) |flag| try attrs.put(alloc, flag, .{ .bool = true });
    inline for (.{ "puballtables", "pubtruncate", "pubviaroot" }) |flag| try attrs.put(alloc, flag, .{ .bool = false });
    for (request.conditions) |condition| {
        if (try conditionMatches(attrs.get(condition.column) orelse return error.InvalidSqlBackendResponse, condition)) continue;
        arena.deinit();
        return .{ .rows = &.{} };
    }
    const rows = try alloc.dupe(catalog.Row, &.{.{ .id = owned_name, .version = 1, .value = .{ .object = attrs } }});
    return .{ .rows = rows, .owned_arena = arena };
}

pub fn isReplicationSlots(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_replication_slots_storage);
}

pub const ReplicationSlot = struct {
    name: []const u8,
    database: []const u8,
    database_oid: ?u32,
    confirmed_lsn: u64,
    active: bool,
    lost: bool,
};

pub fn keyedName(request: catalog.Scan, field: []const u8) !?[]const u8 {
    if (request.search != null) return error.UnsupportedSqlShape;
    var name: ?[]const u8 = null;
    for (request.conditions) |condition| {
        if (condition.op != .eq or !std.mem.eql(u8, condition.column, field)) continue;
        if (condition.value == .null) return null;
        if (condition.value != .string) return error.SqlTypeMismatch;
        if (name) |prior| if (!std.mem.eql(u8, prior, condition.value.string)) return null;
        name = condition.value.string;
    }
    return name orelse error.UnsupportedSqlShape;
}

pub fn replicationSlotPage(a: std.mem.Allocator, request: catalog.Scan, item: ?ReplicationSlot) !catalog.Page {
    const slot = item orelse return .{ .rows = &.{} };
    if (try startOf(request.after, 1) != 0 or request.limit == 0) return .{ .rows = &.{} };
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    const formatLsn = @import("../pgwire/replication_commands.zig").formatLsn;
    const confirmed = try formatLsn(alloc, @fromBackingInt(@intCast(slot.confirmed_lsn)));
    var row_value: std.json.ObjectMap = .empty;
    try row_value.put(alloc, "slot_name", .{ .string = try alloc.dupe(u8, slot.name) });
    try row_value.put(alloc, "plugin", .{ .string = "pgoutput" });
    try row_value.put(alloc, "slot_type", .{ .string = "logical" });
    try row_value.put(alloc, "database", .{ .string = try alloc.dupe(u8, slot.database) });
    try row_value.put(alloc, "active", .{ .bool = slot.active });
    try row_value.put(alloc, "wal_status", .{ .string = if (slot.lost) "lost" else "reserved" });
    try row_value.put(alloc, "restart_lsn", if (slot.lost) .null else .{ .string = confirmed });
    try row_value.put(alloc, "confirmed_flush_lsn", .{ .string = confirmed });
    inline for (.{ "temporary", "two_phase", "conflicting" }) |flag| try row_value.put(alloc, flag, .{ .bool = false });
    try row_value.put(alloc, "datoid", if (slot.database_oid) |oid| .{ .integer = oid } else .null);
    inline for (.{ "active_pid", "xmin", "catalog_xmin", "safe_wal_size" }) |field| try row_value.put(alloc, field, .null);
    for (request.conditions) |condition| {
        if (try conditionMatches(row_value.get(condition.column) orelse return error.InvalidSqlBackendResponse, condition)) continue;
        arena.deinit();
        return .{ .rows = &.{} };
    }
    const rows = try alloc.alloc(catalog.Row, 1);
    rows[0] = .{ .id = row_value.get("slot_name").?.string, .version = 1, .value = .{ .object = row_value } };
    return .{ .rows = rows, .owned_arena = arena };
}

pub fn openSingleRow(a: std.mem.Allocator, snapshot: catalog.Page) !catalog.Cursor {
    const Single = struct {
        allocator: std.mem.Allocator,
        page: catalog.Page,
        fn next(raw: *anyopaque, _: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (limit == 0) return .{ .rows = &.{} };
            const result = self.page;
            self.page = .{ .rows = &.{} };
            return result;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.page.deinit();
            self.allocator.destroy(self);
        }
    };
    const owned = snapshot;
    errdefer owned.deinit();
    const cursor = try a.create(Single);
    cursor.* = .{ .allocator = a, .page = owned };
    return .{ .ptr = cursor, .next = Single.next, .close = Single.close, .estimated_rows = @intCast(snapshot.rows.len) };
}

pub fn isNamespaces(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_namespace_storage);
}

pub const Namespace = struct { oid: u32, name: []const u8 };
pub const builtin_namespace: Namespace = .{ .oid = pg_catalog_oid, .name = "pg_catalog" };

pub fn namespaceMatches(item: Namespace, conditions: []const catalog.Condition) !bool {
    for (conditions) |condition| {
        const value: std.json.Value = if (std.mem.eql(u8, condition.column, "oid"))
            .{ .integer = item.oid }
        else if (std.mem.eql(u8, condition.column, "nspname"))
            .{ .string = item.name }
        else
            return error.InvalidSqlBackendResponse;
        if (!try conditionMatches(value, condition)) return false;
    }
    return true;
}

fn conditionMatches(value: std.json.Value, condition: catalog.Condition) !bool {
    if (condition.op == .is_null) return value == .null;
    if (condition.op == .is_not_null) return value != .null;
    if (value == .null or condition.value == .null) return false;
    const order = try local.sql_scalar.compare(value, condition.value);
    return switch (condition.op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .lte => order != .gt,
        .gt => order == .gt,
        .gte => order != .lt,
        .is_null, .is_not_null => unreachable,
    };
}

pub fn namespaceOid(id: u64) u32 {
    if (id == local.system_catalog_domain.default_namespace_id) return 2200;
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, id, .big);
    return local.sql_scalar.postgresRoleOid(&encoded);
}

fn namespacePage(a: std.mem.Allocator, items: []const Namespace, start: usize, limit: u32) !catalog.Page {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const end = @min(items.len, start +| limit);
    const rows = try scratch.alloc(catalog.Row, end - start);
    for (items[start..end], rows) |item, *entry| {
        var value: std.json.ObjectMap = .empty;
        try value.put(scratch, "oid", .{ .integer = item.oid });
        try value.put(scratch, "nspname", .{ .string = try scratch.dupe(u8, item.name) });
        entry.* = .{ .id = try std.fmt.allocPrint(scratch, "{d:0>10}", .{item.oid}), .version = 1, .value = .{ .object = value } };
    }
    return .{ .rows = rows, .owned_arena = arena, .after = if (end < items.len) try std.fmt.allocPrint(scratch, "{d}", .{end}) else null };
}

pub fn scanNamespaces(a: std.mem.Allocator, request: catalog.Scan, items: []const Namespace) !catalog.Page {
    if (request.search != null) return error.UnsupportedSqlShape;
    return namespacePage(a, items, try startOf(request.after, items.len), request.limit);
}

pub fn openNamespaces(a: std.mem.Allocator, request: catalog.Scan, items: []const Namespace) !catalog.Cursor {
    if (request.search != null) return error.UnsupportedSqlShape;
    const start = try startOf(request.after, items.len);
    const Namespaces = struct {
        arena: std.heap.ArenaAllocator,
        items: []Namespace,
        index: usize,
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = try namespacePage(alloc, self.items, self.index, limit);
            self.index += result.rows.len;
            return result;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var arena = self.arena;
            arena.deinit();
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const entries = try scratch.dupe(Namespace, items);
    for (entries) |*entry| entry.name = try scratch.dupe(u8, entry.name);
    const cursor = try scratch.create(Namespaces);
    cursor.* = .{ .arena = arena, .items = entries, .index = start };
    return .{ .ptr = cursor, .next = Namespaces.next, .close = Namespaces.close, .estimated_rows = items.len - start };
}

/// Antfly's PostgreSQL catalog exposes built-in types and the session's
/// effective role. SQL enum declarations, comments and separate role grants
/// are absent from this catalog. Stored default expressions currently lack
/// a PostgreSQL SQL renderer.
pub fn isEmpty(table: catalog.Table) bool {
    return std.mem.eql(u8, table.physical_name, pg_auth_members_storage) or std.mem.eql(u8, table.physical_name, pg_enum_storage) or
        std.mem.eql(u8, table.physical_name, pg_description_storage) or std.mem.eql(u8, table.physical_name, pg_attrdef_storage);
}

pub fn openEmpty(context: *anyopaque) catalog.Cursor {
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

const PostgresInputProcedure = local.sql_compiler.PostgresInputProcedure;

const Type = struct {
    oid: u32,
    name: []const u8,
    category: []const u8,
    length: i16,
    input: PostgresInputProcedure,
    element: u32 = 0,
    array: u32 = 0,

    fn builtin(element: Element, name: []const u8, category: []const u8, length: i16, input: PostgresInputProcedure) Type {
        return .{ .oid = element.oid(), .name = name, .category = category, .length = length, .input = input, .array = element.arrayOid() };
    }

    fn arrayType(item: Type, comptime name: []const u8) Type {
        return .{ .oid = item.array, .name = name, .category = "A", .length = -1, .input = .array_in, .element = item.oid };
    }
};

const base_types = [_]Type{
    .builtin(.boolean, "bool", "B", 1, .boolin),
    .builtin(.int64, "int8", "N", 8, .int8in),
    .builtin(.int16, "int2", "N", 2, .int2in),
    .builtin(.int32, "int4", "N", 4, .int4in),
    .builtin(.text, "text", "S", -1, .textin),
    .builtin(.float32, "float4", "N", 4, .float4in),
    .builtin(.float64, "float8", "N", 8, .float8in),
    .{ .oid = 1043, .name = "varchar", .category = "S", .length = -1, .input = .varcharin },
    .{ .oid = 1184, .name = "timestamptz", .category = "D", .length = 8, .input = .timestamptz_in },
    .builtin(.numeric, "numeric", "N", -1, .numeric_in),
    .builtin(.uuid, "uuid", "U", 16, .uuid_in),
    .builtin(.jsonb, "jsonb", "U", -1, .jsonb_in),
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
    try value.put(alloc, "typinput", .{ .string = @tagName(item.input) });
    try value.put(alloc, "typelem", .{ .integer = item.element });
    try value.put(alloc, "typarray", .{ .integer = item.array });
    try value.put(alloc, "typbasetype", .{ .integer = 0 });
    try value.put(alloc, "typtypmod", .{ .integer = -1 });
    return .{ .id = try std.fmt.allocPrint(alloc, "{d:0>10}", .{item.oid}), .version = 1, .value = .{ .object = value } };
}

fn startOf(after: ?[]const u8, length: usize) !usize {
    const token = after orelse return 0;
    const index = std.fmt.parseInt(usize, token, 10) catch return error.InvalidSqlBackendResponse;
    if (index > length) return error.InvalidSqlBackendResponse;
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
    return page(a, try startOf(request.after, types.len), request.limit);
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
    const index = try startOf(request.after, types.len);
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

test "pgwire pg_attribute preserves array OIDs and numeric modifiers" {
    const a = std.testing.allocator;
    const attribute: Attribute = .{ .relation = 17000, .ordinal = 2, .column = .{
        .name = "amount",
        .path = "amount",
        .type = .array,
        .element_type = .numeric,
        .numeric_modifier = .{ .precision = 12, .scale = -2 },
        .nullable = false,
        .generated = true,
    } };
    const captured = try scanAttributes(a, .{ .fields = &.{}, .limit = 1 }, &.{attribute});
    defer captured.deinit();
    const attrs = captured.rows[0].value.object;
    try std.testing.expectEqual(@as(i64, 1231), attrs.get("atttypid").?.integer);
    try std.testing.expectEqual(@as(i64, (12 << 16) + 2046 + 4), attrs.get("atttypmod").?.integer);
    try std.testing.expectEqual(@as(i64, 2), attrs.get("attnum").?.integer);
    try std.testing.expect(attrs.get("attnotnull").?.bool);
    try std.testing.expectEqualStrings("s", attrs.get("attgenerated").?.string);
}

test "pgwire pg_class exposes the regclass identity and paginates the captured rows" {
    const a = std.testing.allocator;
    const relation = (try resolve(.{ .table = "pg_class" }, .read, "default")).?;
    try std.testing.expect(isClasses(relation));
    try std.testing.expectEqual(@as(u64, @backingInt(RelationOid.pg_class)), relation.id);
    try std.testing.expect(isEmpty((try resolve(.{ .table = "pg_description" }, .read, "default")).?));
    var storage = std.heap.ArenaAllocator.init(a);
    const items = storage.allocator().dupe(Class, &.{
        .{ .oid = @backingInt(RelationOid.pg_class), .name = "pg_class", .namespace = builtin_namespace.oid },
        .{ .oid = 18000, .name = "events", .namespace = 2200 },
    }) catch |err| {
        storage.deinit();
        return err;
    };
    const cursor = try openRows(Class, .{ .fields = &.{}, .limit = 1 }, storage, items);
    defer cursor.close(cursor.ptr);
    const first = try cursor.next(cursor.ptr, a, 1);
    defer first.deinit();
    const second = try cursor.next(cursor.ptr, a, 1);
    defer second.deinit();
    try std.testing.expectEqualStrings("pg_class", first.rows[0].value.object.get("relname").?.string);
    try std.testing.expectEqual(@as(i64, 2200), second.rows[0].value.object.get("relnamespace").?.integer);
    try std.testing.expectEqual(@as(usize, 0), (try cursor.next(cursor.ptr, a, 1)).rows.len);
}

test "pgwire pg_publication projects explicit publications and handles predicates" {
    const a = std.testing.allocator;
    const relation = (try resolve(.{ .table = "pg_publication" }, .read, "default")).?;
    try std.testing.expect(owns(relation));
    try std.testing.expect(isPublications(relation));
    const request: catalog.Scan = .{ .fields = &.{}, .limit = 10, .conditions = &.{
        .{ .column = "pubname", .op = .eq, .value = .{ .string = "changes" } },
    } };
    try std.testing.expectEqualStrings("changes", (try keyedName(request, "pubname")).?);
    const result = try publicationPage(a, request, "changes", 16384, 16385);
    defer result.deinit();
    const attrs = result.rows[0].value.object;
    try std.testing.expect(!attrs.get("puballtables").?.bool);
    try std.testing.expect(attrs.get("pubinsert").?.bool);
    try std.testing.expect(attrs.get("pubupdate").?.bool);
    try std.testing.expect(attrs.get("pubdelete").?.bool);
    try std.testing.expect(!attrs.get("pubtruncate").?.bool);
    try std.testing.expectEqual(@as(usize, 0), (try publicationPage(a, request, "different", 16384, 16385)).rows.len);
}

test "pgwire pg_replication_slots keyed catalog applies every pushed condition" {
    const a = std.testing.allocator;
    const name: ast.Name = .{ .namespace = "pg_catalog", .table = "pg_replication_slots" };
    const table = (try resolve(name, .read, "default")).?;
    try std.testing.expect(owns(table));
    try std.testing.expect(isReplicationSlots(table));
    try std.testing.expectError(error.SqlReadOnlyTransaction, resolve(name, .write, "default"));
    const lookup: catalog.Scan = .{ .fields = &.{}, .limit = 10, .conditions = &.{
        .{ .column = "slot_name", .op = .eq, .value = .{ .string = "docs" } },
        .{ .column = "database", .op = .eq, .value = .{ .string = "default" } },
        .{ .column = "plugin", .op = .eq, .value = .{ .string = "pgoutput" } },
    } };
    try std.testing.expectEqualStrings("docs", (try keyedName(lookup, "slot_name")).?);
    const saved: ReplicationSlot = .{ .name = "docs", .database = "default", .database_oid = 1, .confirmed_lsn = 0x1_00000004, .active = true, .lost = false };
    const cursor = try openSingleRow(a, try replicationSlotPage(a, lookup, saved));
    defer cursor.close(cursor.ptr);
    const first = try cursor.next(cursor.ptr, a, 1);
    defer first.deinit();
    const attrs = first.rows[0].value.object;
    try std.testing.expect(attrs.get("active").?.bool);
    try std.testing.expectEqualStrings("1/4", attrs.get("confirmed_flush_lsn").?.string);
    try std.testing.expectEqualStrings("reserved", attrs.get("wal_status").?.string);
    try std.testing.expectEqual(@as(usize, 0), (try cursor.next(cursor.ptr, a, 1)).rows.len);
    var other_database = saved;
    other_database.database = "other";
    try std.testing.expectEqual(@as(usize, 0), (try replicationSlotPage(a, lookup, other_database)).rows.len);
}

test "pgwire pg_enum resolves as a read-only catalog with an empty cursor" {
    const name: ast.Name = .{ .namespace = "pg_catalog", .table = "pg_enum" };
    const table = (try resolve(name, .read, "default")).?;
    try std.testing.expectEqual(@as(u64, 3501), table.id);
    try std.testing.expect(owns(table));
    try std.testing.expect(isEmpty(table));
    try std.testing.expectEqualStrings("enumtypid", table.columns[1].name);
    try std.testing.expectEqual(Element.float32, table.columns[2].element_type.?);
    try std.testing.expectEqualStrings("enumlabel", table.columns[3].name);
    try std.testing.expectError(error.SqlReadOnlyTransaction, resolve(name, .write, "default"));
    var context: u8 = 0;
    const cursor = openEmpty(&context);
    defer cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(usize, 0), (try cursor.next(cursor.ptr, std.testing.allocator, 20)).rows.len);
}

test "pgwire pg_namespace OIDs join type rows and cursors own the captured names" {
    const a = std.testing.allocator;
    const table = (try resolve(.{ .namespace = "pg_catalog", .table = "pg_namespace" }, .read, "default")).?;
    try std.testing.expect(isNamespaces(table));
    const lookup: []const catalog.Condition = &.{.{ .column = "nspname", .op = .eq, .value = .{ .string = "public" } }};
    try std.testing.expect(!try namespaceMatches(builtin_namespace, lookup));
    try std.testing.expect(try namespaceMatches(.{ .oid = 2200, .name = "public" }, lookup));
    const request: catalog.Scan = .{ .fields = &.{}, .limit = 1 };
    var public_name = "public".*;
    var cursor = try openNamespaces(a, request, &.{ builtin_namespace, .{ .oid = namespaceOid(local.system_catalog_domain.default_namespace_id), .name = &public_name } });
    defer cursor.close(cursor.ptr);
    @memset(&public_name, 'x');
    var first = try cursor.next(cursor.ptr, a, 1);
    defer first.deinit();
    var type_rows = try scan(a, request);
    defer type_rows.deinit();
    try std.testing.expectEqual(first.rows[0].value.object.get("oid").?.integer, type_rows.rows[0].value.object.get("typnamespace").?.integer);
    try std.testing.expectEqualStrings("pg_catalog", first.rows[0].value.object.get("nspname").?.string);
    var second = try cursor.next(cursor.ptr, a, 1);
    defer second.deinit();
    try std.testing.expectEqual(@as(i64, 2200), second.rows[0].value.object.get("oid").?.integer);
    try std.testing.expectEqualStrings("public", second.rows[0].value.object.get("nspname").?.string);
}
