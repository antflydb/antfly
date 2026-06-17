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
            .alter_role => |alter| return try executeAlterRole(manager, alloc, alter),
            .drop_role => |drop| return try executeDropRole(manager, alloc, drop),
            .grant_privilege => |grant| return try executePrivilegeChange(manager, alloc, grant, .grant),
            .revoke_privilege => |revoke| return try executePrivilegeChange(manager, alloc, revoke, .revoke),
        },
        .row_security_catalog => |row_security_plan| switch (row_security_plan) {
            .alter_table => |alter| return try executeAlterRowSecurity(alloc, alter),
            .create_policy => |create| return try executeCreateRowSecurityPolicy(manager, alloc, create),
            .drop_policy => |drop| return try executeDropRowSecurityPolicy(manager, alloc, drop),
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

fn executeAlterRole(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: relational_sql.AlterRolePlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const subject = try principalSubjectAlloc(manager, alloc, plan.role_name);
    defer alloc.free(subject);
    try manager.setRoleSetting(subject, plan.setting_name, plan.setting_value);
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
    const resource_target = try resourceTargetForSqlPrivilegeObject(plan.object_kind, plan.object_name);
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
        try executeSqlPrivilegeMapping(manager, alloc, subject, resource_target.resource_type, resource_target.resource_name, privilege_name, kind);
    }
    return try changedRecordAlloc(alloc);
}

fn executeAlterRowSecurity(
    alloc: std.mem.Allocator,
    plan: relational_sql.AlterRowSecurityPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    if (!plan.enabled) return error.UnsupportedSqlShape;
    return try changedRecordAlloc(alloc);
}

fn executeCreateRowSecurityPolicy(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: relational_sql.CreateRowSecurityPolicyPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const filter_json = try rowSecurityFilterJsonAlloc(alloc, plan.predicate);
    defer alloc.free(filter_json);
    try manager.createSqlRowSecurityPolicy(plan.policy_name, plan.table_name, filter_json);
    return try changedRecordAlloc(alloc);
}

fn executeDropRowSecurityPolicy(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: relational_sql.DropRowSecurityPolicyPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    manager.dropSqlRowSecurityPolicy(plan.policy_name, plan.table_name) catch |err| switch (err) {
        error.RowFilterNotFound => {
            if (plan.if_exists) return try noopRecordAlloc(alloc);
            return err;
        },
        else => return err,
    };
    return try changedRecordAlloc(alloc);
}

const SqlPrivilegeResourceTarget = struct {
    resource_type: usermgr.ResourceType,
    resource_name: []const u8,
};

fn resourceTargetForSqlPrivilegeObject(object_kind: []const u8, object_name: []const u8) !SqlPrivilegeResourceTarget {
    if (std.ascii.eqlIgnoreCase(object_kind, "table")) return .{ .resource_type = .table, .resource_name = object_name };
    if (std.ascii.eqlIgnoreCase(object_kind, "user")) return .{ .resource_type = .user, .resource_name = object_name };
    if (std.mem.eql(u8, object_kind, "*")) return .{ .resource_type = .@"*", .resource_name = object_name };
    if (std.ascii.eqlIgnoreCase(object_kind, "all_tables_in_schema")) {
        if (!std.ascii.eqlIgnoreCase(object_name, "public")) return error.UnsupportedSqlShape;
        return .{ .resource_type = .table, .resource_name = "*" };
    }
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

fn rowSecurityFilterJsonAlloc(
    alloc: std.mem.Allocator,
    predicate: relational_sql.RowSecurityPolicyPredicate,
) ![]u8 {
    return switch (predicate) {
        .current_setting_equals => |current_setting| try currentSettingRowFilterJsonAlloc(alloc, current_setting),
    };
}

fn currentSettingRowFilterJsonAlloc(
    alloc: std.mem.Allocator,
    predicate: relational_sql.RowSecurityCurrentSettingPredicate,
) ![]u8 {
    const auth_metadata_key = authMetadataKeyForSqlSetting(predicate.setting_name);
    if (auth_metadata_key.len == 0) return error.UnsupportedSqlShape;
    const field_json = try std.json.Stringify.valueAlloc(alloc, predicate.field, .{});
    defer alloc.free(field_json);
    const auth_path = try std.fmt.allocPrint(alloc, "metadata.{s}", .{auth_metadata_key});
    defer alloc.free(auth_path);
    const auth_path_json = try std.json.Stringify.valueAlloc(alloc, auth_path, .{});
    defer alloc.free(auth_path_json);
    return try std.fmt.allocPrint(alloc, "{{\"term\":{{{s}:{{\"$auth\":{s}}}}}}}", .{ field_json, auth_path_json });
}

fn authMetadataKeyForSqlSetting(setting_name: []const u8) []const u8 {
    if (std.mem.startsWith(u8, setting_name, "app.") and setting_name.len > "app.".len) {
        return setting_name["app.".len..];
    }
    return setting_name;
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

    var schema_granted = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_writer;")).?;
    defer schema_granted.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_writer");
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "docs", .read));
    var schema_revoked = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM app_writer;")).?;
    defer schema_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "docs", .read)));
    try manager.removeRoleFromUser("alice", "role:app_writer");
    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON ALL TABLES IN SCHEMA private TO app_writer;"));

    var granted = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT, INSERT ON TABLE usage_records TO app_writer;")).?;
    defer granted.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_writer");
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .write));

    var revoked = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE INSERT ON TABLE usage_records FROM app_writer;")).?;
    defer revoked.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .table, "usage_records", .read));
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .write)));
    try std.testing.expectError(error.RoleInUse, executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;"));

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
    try std.testing.expectError(error.RoleInUse, executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;"));

    try manager.removeRoleFromUser("alice", "role:app_writer");
    var dropped = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;")).?;
    defer dropped.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .read)));

    var missing = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE IF EXISTS app_writer;")).?;
    defer missing.deinit(alloc);
    try std.testing.expect(missing.noop);

    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT USAGE ON TABLE usage_records TO app_writer;"));
    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT, USAGE ON TABLE usage_records TO app_writer;"));
    try std.testing.expectError(error.RoleNotFound, executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET statement_timeout = '5s';"));

    var recreated = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE ROLE app_writer;")).?;
    defer recreated.deinit(alloc);
    var altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET statement_timeout = '5s';")).?;
    defer altered.deinit(alloc);
    const statement_timeout = try manager.getRoleSetting("role:app_writer", "statement_timeout");
    defer alloc.free(statement_timeout);
    try std.testing.expectEqualStrings("5s", statement_timeout);
    var dropped_with_setting = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;")).?;
    defer dropped_with_setting.deinit(alloc);
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleSetting("role:app_writer", "statement_timeout"));
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
    var altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE alice SET statement_timeout = '3s';")).?;
    defer altered.deinit(alloc);
    const setting = try manager.getRoleSetting("alice", "statement_timeout");
    defer alloc.free(setting);
    try std.testing.expectEqualStrings("3s", setting);
}

test "sql auth adapter applies row security policies through user manager" {
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

    var user = try manager.createUserWithMetadata("alice", "secret", &.{}, "{\"tenant_id\":\"acme\"}");
    defer user.deinit(alloc);

    var enabled = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;")).?;
    defer enabled.deinit(alloc);

    var created = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = current_setting('app.tenant_id'));")).?;
    defer created.deinit(alloc);

    const stored = try manager.getSqlRowSecurityPolicy("usage_records_tenant_policy", "usage_records");
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"tenant_id\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"$auth\":\"metadata.tenant_id\"") != null);

    const filters = try manager.getRowFilters("alice");
    defer {
        for (filters) |*entry| entry.deinit(alloc);
        alloc.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 1), filters.len);
    try std.testing.expectEqualStrings("usage_records", filters[0].table);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"$auth\":\"metadata.tenant_id\"") != null);

    try std.testing.expectError(error.PolicyExists, executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = current_setting('app.tenant_id'));"));
    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY;"));

    var dropped = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP POLICY usage_records_tenant_policy ON usage_records;")).?;
    defer dropped.deinit(alloc);
    try std.testing.expectError(error.RowFilterNotFound, manager.getSqlRowSecurityPolicy("usage_records_tenant_policy", "usage_records"));

    var missing = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP POLICY IF EXISTS usage_records_tenant_policy ON usage_records;")).?;
    defer missing.deinit(alloc);
    try std.testing.expect(missing.noop);
}
