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
const relational_sql = @import("relational_sql.zig");
const tables_api = @import("tables.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

pub fn executeRelationalSqlDdlOnUserManager(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    sql: []const u8,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    var plan = try relational_sql.lowerDdlPlanAlloc(alloc, sql);
    defer plan.deinit(alloc);

    switch (plan) {
        .authorization_catalog => |authorization_plan| switch (authorization_plan) {
            .create_role => |create| return try executeCreateRole(manager, alloc, create),
            .alter_role => return error.UnsupportedSqlShape,
            .drop_role => |drop| return try executeDropRole(manager, alloc, drop),
            .grant_privilege => |grant| return try executePrivilegeChange(manager, alloc, grant, .grant),
            .revoke_privilege => |revoke| return try executePrivilegeChange(manager, alloc, revoke, .revoke),
        },
        else => return null,
    }
}

fn executeCreateRole(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: relational_sql.CreateRolePlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const subject = try sqlRoleSubjectAlloc(alloc, plan.role_name);
    defer alloc.free(subject);
    try manager.createRoleSubject(subject);
    return try changedRecordAlloc(alloc);
}

fn executeDropRole(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: relational_sql.DropRolePlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const subject = try sqlRoleSubjectAlloc(alloc, plan.role_name);
    defer alloc.free(subject);
    if (!(try manager.roleSubjectExists(subject))) {
        if (plan.if_exists) return try noopRecordAlloc(alloc);
        return error.RoleNotFound;
    }
    try manager.dropRoleSubject(subject);
    return try changedRecordAlloc(alloc);
}

const PrivilegeChangeKind = enum {
    grant,
    revoke,
};

fn executePrivilegeChange(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: relational_sql.PrivilegeChangePlan,
    kind: PrivilegeChangeKind,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const resource_type = try resourceTypeForSqlObjectKind(plan.object_kind);
    var mapped_privileges: usize = 0;
    for (plan.privileges) |privilege_name| {
        const privilege_count = sqlPrivilegeMappingCount(privilege_name);
        if (privilege_count == 0) return error.UnsupportedSqlShape;
        mapped_privileges += privilege_count;
    }
    if (mapped_privileges == 0) return error.UnsupportedSqlShape;

    const subject = try principalSubjectAlloc(manager, alloc, plan.principal_name);
    defer alloc.free(subject);

    for (plan.privileges) |privilege_name| {
        try executeSqlPrivilegeMapping(manager, alloc, subject, resource_type, plan.object_name, privilege_name, kind);
    }
    return try changedRecordAlloc(alloc);
}

fn resourceTypeForSqlObjectKind(object_kind: []const u8) !usermgr.ResourceType {
    if (std.ascii.eqlIgnoreCase(object_kind, "table")) return .table;
    if (std.ascii.eqlIgnoreCase(object_kind, "user")) return .user;
    if (std.mem.eql(u8, object_kind, "*")) return .@"*";
    return error.UnsupportedSqlShape;
}

fn executeSqlPrivilegeMapping(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    subject: []const u8,
    resource_type: usermgr.ResourceType,
    resource_name: []const u8,
    privilege_name: []const u8,
    kind: PrivilegeChangeKind,
) !void {
    if (std.ascii.eqlIgnoreCase(privilege_name, "select")) {
        try executePermissionChange(manager, alloc, subject, resource_type, resource_name, .read, kind);
        return;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "insert") or
        std.ascii.eqlIgnoreCase(privilege_name, "update") or
        std.ascii.eqlIgnoreCase(privilege_name, "delete") or
        std.ascii.eqlIgnoreCase(privilege_name, "truncate"))
    {
        try executePermissionChange(manager, alloc, subject, resource_type, resource_name, .write, kind);
        return;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "all")) {
        try executePermissionChange(manager, alloc, subject, resource_type, resource_name, .read, kind);
        try executePermissionChange(manager, alloc, subject, resource_type, resource_name, .write, kind);
        try executePermissionChange(manager, alloc, subject, resource_type, resource_name, .admin, kind);
        return;
    }
}

fn sqlPrivilegeMappingCount(privilege_name: []const u8) usize {
    if (std.ascii.eqlIgnoreCase(privilege_name, "select")) return 1;
    if (std.ascii.eqlIgnoreCase(privilege_name, "insert") or
        std.ascii.eqlIgnoreCase(privilege_name, "update") or
        std.ascii.eqlIgnoreCase(privilege_name, "delete") or
        std.ascii.eqlIgnoreCase(privilege_name, "truncate"))
    {
        return 1;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "all")) return 3;
    return 0;
}

fn executePermissionChange(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    subject: []const u8,
    resource_type: usermgr.ResourceType,
    resource_name: []const u8,
    permission_type: usermgr.PermissionType,
    kind: PrivilegeChangeKind,
) !void {
    var permission = try usermgr.Permission.initOwned(alloc, resource_type, resource_name, permission_type);
    defer permission.deinit(alloc);
    switch (kind) {
        .grant => try manager.addPermissionToSubject(subject, permission),
        .revoke => try manager.removePermissionFromSubjectExact(subject, permission),
    }
}

fn principalSubjectAlloc(
    manager: *const usermgr.UserManager,
    alloc: std.mem.Allocator,
    principal_name: []const u8,
) ![]u8 {
    if (manager.hasUser(principal_name)) return try alloc.dupe(u8, principal_name);
    const role_subject = try sqlRoleSubjectAlloc(alloc, principal_name);
    errdefer alloc.free(role_subject);
    if (!(try manager.roleSubjectExists(role_subject))) return error.RoleNotFound;
    return role_subject;
}

fn sqlRoleSubjectAlloc(alloc: std.mem.Allocator, role_name: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, role_name, "role:") or std.mem.startsWith(u8, role_name, "group:")) {
        return try alloc.dupe(u8, role_name);
    }
    return try std.fmt.allocPrint(alloc, "role:{s}", .{role_name});
}

fn changedRecordAlloc(alloc: std.mem.Allocator) !tables_api.AppliedRelationalSqlDdlRecord {
    return .{ .table = try emptyTableRecordAlloc(alloc) };
}

fn noopRecordAlloc(alloc: std.mem.Allocator) !tables_api.AppliedRelationalSqlDdlRecord {
    return .{
        .table = try emptyTableRecordAlloc(alloc),
        .noop = true,
    };
}

fn emptyTableRecordAlloc(alloc: std.mem.Allocator) !metadata_table_manager.TableRecord {
    return try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 0,
        .name = "",
        .description = "",
        .schema_json = "",
        .read_schema_json = "",
        .foreign_key_validation_json = "{}",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "",
        .restore_backup_id = "",
        .restore_location = "",
    });
}

test "sql auth adapter creates roles and applies table grants through user manager" {
    const alloc = std.testing.allocator;

    var store = usermgr.MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();
    var manager = try usermgr.UserManager.init(
        alloc,
        store.iface(),
        try usermgr.initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    var user = try manager.createUser("alice", "secret", &.{});
    defer user.deinit(alloc);

    try std.testing.expectError(error.RoleNotFound, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON TABLE usage_records TO app_writer;"));

    var created = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE ROLE app_writer;")).?;
    defer created.deinit(alloc);

    const subjects_after_create = try manager.listAuthSubjects();
    defer {
        for (subjects_after_create) |*entry| entry.deinit(alloc);
        alloc.free(subjects_after_create);
    }
    var found_role = false;
    for (subjects_after_create) |entry| {
        if (std.mem.eql(u8, entry.subject, "role:app_writer") and entry.kind == .role) found_role = true;
        try std.testing.expect(!std.mem.eql(u8, entry.subject, "__antfly_sql_role_catalog__"));
    }
    try std.testing.expect(found_role);

    var granted = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT, INSERT ON TABLE usage_records TO app_writer;")).?;
    defer granted.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_writer");
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .write));

    var revoked = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE INSERT ON TABLE usage_records FROM app_writer;")).?;
    defer revoked.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .read));
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .write)));

    var all_granted = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT ALL PRIVILEGES ON TABLE usage_records TO app_writer;")).?;
    defer all_granted.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .write));
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .admin));

    var all_revoked = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE ALL PRIVILEGES ON TABLE usage_records FROM app_writer;")).?;
    defer all_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .read)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .write)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .admin)));

    var dropped = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;")).?;
    defer dropped.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .read)));

    var missing = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE IF EXISTS app_writer;")).?;
    defer missing.deinit(alloc);
    try std.testing.expect(missing.noop);

    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT USAGE ON TABLE usage_records TO app_writer;"));
    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT, USAGE ON TABLE usage_records TO app_writer;"));
}

test "sql auth adapter grants directly to existing antfly users" {
    const alloc = std.testing.allocator;

    var store = usermgr.MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();
    var manager = try usermgr.UserManager.init(
        alloc,
        store.iface(),
        try usermgr.initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    var user = try manager.createUser("alice", "secret", &.{});
    defer user.deinit(alloc);

    var granted = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON TABLE docs TO alice;")).?;
    defer granted.deinit(alloc);

    try std.testing.expect(try manager.enforce("alice", .table, "docs", .read));
    try std.testing.expect(!(try manager.roleSubjectExists("role:alice")));
}
