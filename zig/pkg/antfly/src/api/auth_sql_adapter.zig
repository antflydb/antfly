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
const sql_adapter = @import("../sql/mod.zig");
const tables_api = @import("tables.zig");
const catalog_resources = @import("catalog_resources.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const runtime_schema = @import("../storage/schema.zig");
const table_catalog = @import("table_catalog.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

pub fn executeRelationalSqlDdlOnUserManager(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    sql: []const u8,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    return try executeRelationalSqlDdlOnUserManagerWithCatalog(manager, alloc, sql, .{});
}

pub const SqlAuthTableRef = struct {
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
    schema_json: []const u8 = "",
};

pub const SqlAuthCatalog = struct {
    database_name: []const u8 = tables_api.default_database_name,
    search_path: []const []const u8 = &.{tables_api.default_namespace_name},
    settings: []const catalog_resources.SqlSessionSetting = &.{},
    public_table_names: ?[]const []const u8 = null,
    tables: []const SqlAuthTableRef = &.{},

    fn session(self: SqlAuthCatalog) catalog_resources.SqlCatalogSession {
        return .{
            .current_database_name = self.database_name,
            .search_path = self.search_path,
            .settings = self.settings,
        };
    }
};

pub const OwnedSqlAuthCatalog = struct {
    value: SqlAuthCatalog = .{},
    public_table_names: []const []const u8 = &.{},
    tables: []const SqlAuthTableRef = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.public_table_names) |name| alloc.free(@constCast(name));
        if (self.public_table_names.len > 0) alloc.free(self.public_table_names);
        for (self.tables) |table| {
            alloc.free(@constCast(table.database_name));
            alloc.free(@constCast(table.namespace_name));
            alloc.free(@constCast(table.table_name));
            alloc.free(@constCast(table.schema_json));
        }
        if (self.tables.len > 0) alloc.free(self.tables);
        self.* = undefined;
    }
};

pub fn authCatalogForPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    plan: sql_adapter.AuthorizationLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !OwnedSqlAuthCatalog {
    const CatalogRequirement = enum { none, optional, required };
    const catalog_requirement: CatalogRequirement = switch (plan) {
        .authorization_catalog => |authorization_plan| switch (authorization_plan) {
            .grant_privilege => |grant| if (std.ascii.eqlIgnoreCase(grant.object_kind, "all_tables_in_schema"))
                .required
            else if (std.ascii.eqlIgnoreCase(grant.object_kind, "table"))
                .optional
            else
                .none,
            .revoke_privilege => |revoke| if (std.ascii.eqlIgnoreCase(revoke.object_kind, "all_tables_in_schema"))
                .required
            else if (std.ascii.eqlIgnoreCase(revoke.object_kind, "table"))
                .optional
            else
                .none,
            else => .none,
        },
        .row_security_catalog => .required,
    };
    if (catalog_requirement == .none) return .{
        .value = .{
            .database_name = session.currentDatabase(),
            .search_path = session.search_path,
            .settings = session.settings,
        },
    };

    var snapshot = catalog.adminSnapshot() catch |err| switch (err) {
        error.UnsupportedOperation => if (catalog_requirement == .optional) return .{
            .value = .{
                .database_name = session.currentDatabase(),
                .search_path = session.search_path,
                .settings = session.settings,
            },
        } else return err,
        else => return err,
    };
    defer catalog.freeAdminSnapshot(&snapshot);
    var names = std.ArrayListUnmanaged([]const u8).empty;
    var table_refs = std.ArrayListUnmanaged(SqlAuthTableRef).empty;
    errdefer {
        for (names.items) |name| alloc.free(@constCast(name));
        names.deinit(alloc);
        for (table_refs.items) |table| {
            alloc.free(@constCast(table.database_name));
            alloc.free(@constCast(table.namespace_name));
            alloc.free(@constCast(table.table_name));
            alloc.free(@constCast(table.schema_json));
        }
        table_refs.deinit(alloc);
    }
    try names.ensureTotalCapacity(alloc, snapshot.tables.len);
    try table_refs.ensureTotalCapacity(alloc, snapshot.tables.len);
    for (snapshot.tables) |table| {
        const name = try alloc.dupe(u8, table.name);
        errdefer alloc.free(name);
        const database_name = try alloc.dupe(u8, table.database_name);
        errdefer alloc.free(database_name);
        const namespace_name = try alloc.dupe(u8, table.namespace_name);
        errdefer alloc.free(namespace_name);
        const table_name = try alloc.dupe(u8, table.name);
        errdefer alloc.free(table_name);
        const schema_json = try alloc.dupe(u8, table.schema_json);
        errdefer alloc.free(schema_json);

        names.appendAssumeCapacity(name);
        table_refs.appendAssumeCapacity(.{
            .database_name = database_name,
            .namespace_name = namespace_name,
            .table_name = table_name,
            .schema_json = schema_json,
        });
    }
    const owned_names = try names.toOwnedSlice(alloc);
    errdefer {
        for (owned_names) |name| alloc.free(@constCast(name));
        if (owned_names.len > 0) alloc.free(owned_names);
    }
    const owned_tables = try table_refs.toOwnedSlice(alloc);
    return .{
        .value = .{
            .database_name = session.currentDatabase(),
            .search_path = session.search_path,
            .settings = session.settings,
            .public_table_names = owned_names,
            .tables = owned_tables,
        },
        .public_table_names = owned_names,
        .tables = owned_tables,
    };
}

const SqlAuthCatalogSource = struct {
    tables: []metadata_table_manager.TableRecord = &.{},

    fn initAlloc(alloc: std.mem.Allocator, catalog: SqlAuthCatalog) !SqlAuthCatalogSource {
        var records = std.ArrayListUnmanaged(metadata_table_manager.TableRecord).empty;
        var transferred = false;
        errdefer if (!transferred) records.deinit(alloc);

        if (catalog.tables.len > 0) {
            try records.ensureTotalCapacity(alloc, catalog.tables.len);
            for (catalog.tables) |table_ref| {
                records.appendAssumeCapacity(.{
                    .table_id = tables_api.deriveQualifiedTableId(table_ref.database_name, table_ref.namespace_name, table_ref.table_name),
                    .name = table_ref.table_name,
                    .database_name = table_ref.database_name,
                    .namespace_name = table_ref.namespace_name,
                    .schema_json = table_ref.schema_json,
                });
            }
        } else if (catalog.public_table_names) |public_table_names| {
            try records.ensureTotalCapacity(alloc, public_table_names.len);
            for (public_table_names) |table_name| {
                records.appendAssumeCapacity(.{
                    .table_id = tables_api.deriveQualifiedTableId(catalog_resources.default_database_name, catalog_resources.default_namespace_name, table_name),
                    .name = table_name,
                    .database_name = catalog_resources.default_database_name,
                    .namespace_name = catalog_resources.default_namespace_name,
                });
            }
        }

        const tables = try records.toOwnedSlice(alloc);
        transferred = true;
        return .{
            .tables = tables,
        };
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.tables.len > 0) alloc.free(self.tables);
        self.* = undefined;
    }

    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = self.tables,
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

pub fn executeRelationalSqlDdlOnUserManagerWithCatalog(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    sql: []const u8,
    catalog: SqlAuthCatalog,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    return try executeRelationalSqlDdlOnUserManagerWithCatalogAndFunctionBindings(manager, alloc, sql, catalog, .{});
}

pub fn executeRelationalSqlDdlOnUserManagerWithCatalogAndFunctionBindings(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    sql: []const u8,
    catalog: SqlAuthCatalog,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try executeRelationalSqlDdlParsedSqlOnUserManagerWithCatalogAndFunctionBindings(manager, alloc, &parsed_sql, catalog, function_bindings);
}

pub fn executeRelationalSqlDdlParsedSqlOnUserManagerWithCatalogAndFunctionBindings(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    catalog: SqlAuthCatalog,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    var catalog_source_storage: SqlAuthCatalogSource = undefined;
    var catalog_source_initialized = false;
    defer if (catalog_source_initialized) catalog_source_storage.deinit(alloc);
    const catalog_source = if (catalog.tables.len > 0 or catalog.public_table_names != null) blk: {
        catalog_source_storage = try SqlAuthCatalogSource.initAlloc(alloc, catalog);
        catalog_source_initialized = true;
        break :blk catalog_source_storage.iface();
    } else table_catalog.unavailableCatalogSource();

    var durable_plan = try sql_adapter.planDurableSqlPlanParsedSqlWithCatalogSessionFunctionBindingsAlloc(
        alloc,
        parsed_sql,
        catalog_source,
        catalog.session(),
        function_bindings,
    );
    defer durable_plan.deinit(alloc);
    return try executeRelationalSqlDurablePlanOnUserManagerWithCatalog(manager, alloc, &durable_plan, catalog);
}

pub fn executeRelationalSqlDurablePlanOnUserManagerWithCatalog(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    durable_plan: *sql_adapter.DurableSqlPlan,
    catalog: SqlAuthCatalog,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    return switch (durable_plan.*) {
        .auth => |auth_plan| try executeRelationalSqlAuthPlanOnUserManagerWithCatalog(manager, alloc, auth_plan, catalog),
        else => null,
    };
}

pub fn executeRelationalSqlAuthPlanOnUserManagerWithCatalog(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.AuthorizationLogicalPlan,
    catalog: SqlAuthCatalog,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    return switch (plan) {
        .authorization_catalog => |authorization_plan| switch (authorization_plan) {
            .create_role => |create| return try executeCreateRole(manager, alloc, create),
            .alter_role => |alter| return try executeAlterRole(manager, alloc, alter, catalog),
            .drop_role => |drop| return try executeDropRole(manager, alloc, drop),
            .grant_privilege => |grant| return try executePrivilegeChange(manager, alloc, grant, .grant, catalog),
            .revoke_privilege => |revoke| return try executePrivilegeChange(manager, alloc, revoke, .revoke, catalog),
        },
        .row_security_catalog => |row_security_plan| switch (row_security_plan) {
            .alter_table => |alter| return try executeAlterRowSecurity(manager, alloc, alter, catalog),
            .create_policy => |create| return try executeCreateRowSecurityPolicy(manager, alloc, create, catalog),
            .alter_policy => |alter| return try executeAlterRowSecurityPolicy(manager, alloc, alter, catalog),
            .drop_policy => |drop| return try executeDropRowSecurityPolicy(manager, alloc, drop, catalog),
        },
    };
}

fn executeCreateRole(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.CreateRolePlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const subject = try sqlRoleSubjectAlloc(alloc, plan.role_name);
    defer alloc.free(subject);
    try manager.createRoleSubject(subject);
    return try changedRecordAlloc(alloc);
}

fn executeAlterRole(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.AlterRolePlan,
    catalog: SqlAuthCatalog,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const subject = try principalSubjectAlloc(manager, alloc, plan.role_name);
    defer alloc.free(subject);
    switch (plan.setting_kind) {
        .app => switch (plan.operation) {
            .set => if (plan.database_name) |database_name| {
                try manager.setRoleDatabaseSetting(subject, database_name, plan.setting_name, try alterRoleSettingValue(catalog, plan));
            } else {
                try manager.setRoleSetting(subject, plan.setting_name, try alterRoleSettingValue(catalog, plan));
            },
            .reset => if (plan.database_name) |database_name| {
                manager.removeRoleDatabaseSetting(subject, database_name, plan.setting_name) catch |err| switch (err) {
                    error.RoleSettingNotFound => {},
                    else => return err,
                };
            } else {
                manager.removeRoleSetting(subject, plan.setting_name) catch |err| switch (err) {
                    error.RoleSettingNotFound => {},
                    else => return err,
                };
            },
        },
        .runtime => switch (plan.operation) {
            .set => try manager.setRoleRuntimeSetting(subject, plan.database_name, plan.setting_name, try alterRoleSettingValue(catalog, plan)),
            .reset => manager.removeRoleRuntimeSetting(subject, plan.database_name, plan.setting_name) catch |err| switch (err) {
                error.RoleSettingNotFound => {},
                else => return err,
            },
        },
    }
    return try changedRecordAlloc(alloc);
}

fn alterRoleSettingValue(catalog: SqlAuthCatalog, plan: sql_adapter.AlterRolePlan) ![]const u8 {
    const value = plan.setting_value orelse return error.UnsupportedSqlShape;
    return switch (value) {
        .literal => |literal| literal,
        .current_setting => |name| catalog.session().settingValue(name) orelse return error.UnsupportedSqlShape,
    };
}

fn executeDropRole(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.DropRolePlan,
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

const SqlPermissionChangeList = std.ArrayList(usermgr.PermissionChange);

fn executePrivilegeChange(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.PrivilegeChangePlan,
    kind: PrivilegeChangeKind,
    catalog: SqlAuthCatalog,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var mapped_privileges: usize = 0;
    for (plan.privileges) |privilege_name| {
        const privilege_count = sqlPrivilegeMappingCount(privilege_name);
        if (privilege_count == 0) return error.UnsupportedSqlShape;
        mapped_privileges += privilege_count;
    }
    if (mapped_privileges == 0) return error.UnsupportedSqlShape;

    const subject = try principalSubjectAlloc(manager, alloc, plan.principal_name);
    defer alloc.free(subject);
    var changes = SqlPermissionChangeList.empty;
    defer freeSqlPermissionChanges(alloc, &changes);

    if (std.ascii.eqlIgnoreCase(plan.object_kind, "all_tables_in_schema")) {
        try appendAllTablesInSchemaPrivilegeChanges(alloc, &changes, subject, plan.object_name, plan.privileges, kind, catalog);
    } else {
        const resource_target = try resourceTargetForSqlPrivilegeObject(alloc, plan.object_kind, plan.object_name, catalog);
        defer resource_target.deinit(alloc);
        for (plan.privileges) |privilege_name| {
            try appendSqlPrivilegeMapping(alloc, &changes, subject, resource_target.resource_type, resource_target.resource_name, privilege_name, kind);
        }
    }
    try manager.applyPermissionChangesAtomically(changes.items);
    return try changedRecordAlloc(alloc);
}

fn executeAlterRowSecurity(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.AlterRowSecurityPlan,
    catalog: SqlAuthCatalog,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const table_resource = try sqlAuthTableResourceNameAlloc(alloc, plan.table_name, catalog);
    defer alloc.free(table_resource);
    if (plan.enabled) {
        if (try manager.sqlRowSecurityEnabled(table_resource)) return try noopRecordAlloc(alloc);
        try manager.enableSqlRowSecurity(table_resource);
    } else {
        if (!(try manager.sqlRowSecurityEnabled(table_resource))) return try noopRecordAlloc(alloc);
        try manager.disableSqlRowSecurity(table_resource);
    }
    return try changedRecordAlloc(alloc);
}

fn executeCreateRowSecurityPolicy(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.CreateRowSecurityPolicyPlan,
    catalog: SqlAuthCatalog,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const table_resource = try sqlAuthTableResourceNameAlloc(alloc, plan.table_name, catalog);
    defer alloc.free(table_resource);
    try validateRowSecurityPredicateForCatalogTableSchema(alloc, plan.table_name, catalog, plan.predicate);
    if (plan.check_predicate) |check_predicate| {
        try validateRowSecurityPredicateForCatalogTableSchema(alloc, plan.table_name, catalog, check_predicate);
    }
    const filter_json = try rowSecurityFilterJsonAlloc(alloc, plan.predicate);
    defer alloc.free(filter_json);
    const check_filter_json = if (plan.check_predicate) |check_predicate|
        try rowSecurityFilterJsonAlloc(alloc, check_predicate)
    else
        try alloc.dupe(u8, filter_json);
    defer alloc.free(check_filter_json);
    const role_targets = try rowSecurityPolicyTargetSubjectsAlloc(manager, alloc, plan.role_targets);
    defer freeStringSlice(alloc, role_targets);
    try manager.createSqlRowSecurityPolicyWithTargetsAndCheck(plan.policy_name, table_resource, filter_json, check_filter_json, role_targets);
    return try changedRecordAlloc(alloc);
}

fn executeAlterRowSecurityPolicy(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.AlterRowSecurityPolicyPlan,
    catalog: SqlAuthCatalog,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const table_resource = try sqlAuthTableResourceNameAlloc(alloc, plan.table_name, catalog);
    defer alloc.free(table_resource);
    const existing_filter_json = try manager.getSqlRowSecurityPolicy(plan.policy_name, table_resource);
    defer alloc.free(existing_filter_json);
    const existing_check_filter_json = try manager.getSqlRowSecurityPolicyCheck(plan.policy_name, table_resource);
    defer alloc.free(existing_check_filter_json);
    const existing_role_targets = try manager.getSqlRowSecurityPolicyTargets(plan.policy_name, table_resource);
    defer freeStringSlice(alloc, existing_role_targets);
    if (plan.predicate) |predicate| {
        try validateRowSecurityPredicateForCatalogTableSchema(alloc, plan.table_name, catalog, predicate);
    }
    if (plan.check_predicate) |check_predicate| {
        try validateRowSecurityPredicateForCatalogTableSchema(alloc, plan.table_name, catalog, check_predicate);
    }
    const filter_json = if (plan.predicate) |predicate|
        try rowSecurityFilterJsonAlloc(alloc, predicate)
    else
        try alloc.dupe(u8, existing_filter_json);
    defer alloc.free(filter_json);
    const check_filter_json = if (plan.check_predicate) |check_predicate|
        try rowSecurityFilterJsonAlloc(alloc, check_predicate)
    else
        try alloc.dupe(u8, existing_check_filter_json);
    defer alloc.free(check_filter_json);
    const role_targets = if (plan.role_targets_present)
        try rowSecurityPolicyTargetSubjectsAlloc(manager, alloc, plan.role_targets)
    else
        try cloneStringSliceAlloc(alloc, existing_role_targets);
    defer freeStringSlice(alloc, role_targets);
    try manager.replaceSqlRowSecurityPolicyWithTargetsAndCheck(plan.policy_name, table_resource, filter_json, check_filter_json, role_targets);
    return try changedRecordAlloc(alloc);
}

fn executeDropRowSecurityPolicy(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    plan: sql_adapter.DropRowSecurityPolicyPlan,
    catalog: SqlAuthCatalog,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const table_resource = try sqlAuthTableResourceNameAlloc(alloc, plan.table_name, catalog);
    defer alloc.free(table_resource);
    manager.dropSqlRowSecurityPolicy(plan.policy_name, table_resource) catch |err| switch (err) {
        error.RowFilterNotFound => {
            if (plan.if_exists) return try noopRecordAlloc(alloc);
            return err;
        },
        else => return err,
    };
    return try changedRecordAlloc(alloc);
}

fn sqlAuthTableResourceNameAlloc(
    alloc: std.mem.Allocator,
    sql_object_name: []const u8,
    catalog: SqlAuthCatalog,
) ![]u8 {
    const target = try catalog.session().tableTargetFromObjectName(sql_object_name);
    if (catalog.tables.len > 0) {
        for (catalog.tables) |table_ref| {
            if (std.mem.eql(u8, table_ref.database_name, target.database_name) and
                std.mem.eql(u8, table_ref.namespace_name, target.namespace_name) and
                std.mem.eql(u8, table_ref.table_name, target.table_name))
            {
                return try catalog_resources.tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            }
        }
        return error.TableNotFound;
    }
    if (catalog.public_table_names) |public_table_names| {
        if (std.mem.eql(u8, target.database_name, catalog_resources.default_database_name) and
            std.mem.eql(u8, target.namespace_name, catalog_resources.default_namespace_name))
        {
            for (public_table_names) |table_name| {
                if (std.mem.eql(u8, table_name, target.table_name)) {
                    return try catalog_resources.tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
                }
            }
            return error.TableNotFound;
        }
    }
    return try catalog_resources.tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
}

fn sqlAuthTableRefForObject(sql_object_name: []const u8, catalog: SqlAuthCatalog) !?SqlAuthTableRef {
    const target = try catalog.session().tableTargetFromObjectName(sql_object_name);
    if (catalog.tables.len > 0) {
        for (catalog.tables) |table_ref| {
            if (std.mem.eql(u8, table_ref.database_name, target.database_name) and
                std.mem.eql(u8, table_ref.namespace_name, target.namespace_name) and
                std.mem.eql(u8, table_ref.table_name, target.table_name))
            {
                return table_ref;
            }
        }
        return error.TableNotFound;
    }
    if (catalog.public_table_names) |public_table_names| {
        if (std.mem.eql(u8, target.database_name, catalog_resources.default_database_name) and
            std.mem.eql(u8, target.namespace_name, catalog_resources.default_namespace_name))
        {
            for (public_table_names) |table_name| {
                if (std.mem.eql(u8, table_name, target.table_name)) return null;
            }
            return error.TableNotFound;
        }
    }
    return null;
}

fn validateRowSecurityPredicateForCatalogTableSchema(
    alloc: std.mem.Allocator,
    sql_object_name: []const u8,
    catalog: SqlAuthCatalog,
    predicate: sql_adapter.RowSecurityPolicyPredicate,
) !void {
    const table_ref = (try sqlAuthTableRefForObject(sql_object_name, catalog)) orelse return;
    if (table_ref.schema_json.len == 0) return;
    var parsed = try tables_api.parseValidatedTableSchema(alloc, table_ref.schema_json);
    defer parsed.deinit(alloc);
    const schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    try validateRowSecurityPredicateForSchema(alloc, schema, predicate);
}

fn validateRowSecurityPredicateForSchema(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    predicate: sql_adapter.RowSecurityPolicyPredicate,
) !void {
    switch (predicate) {
        .current_setting_equals => |current_setting| {
            _ = rowSecurityPolicyColumn(schema, current_setting.field) orelse return error.UnsupportedSqlShape;
            usermgr.validateRoleSettingName(current_setting.setting_name) catch return error.UnsupportedSqlShape;
        },
        .literal_equals => |literal| {
            const column = rowSecurityPolicyColumn(schema, literal.field) orelse return error.UnsupportedSqlShape;
            try validateRowSecurityLiteralForColumn(alloc, column, literal.value_json);
        },
        .expression => |expression| {
            try sql_adapter.validateCheckExpressionConditionForColumns(schema.relational_columns, expression);
        },
        .conjunction => |conjunction| {
            for (conjunction.predicates) |term| try validateRowSecurityPredicateForSchema(alloc, schema, term);
        },
        .disjunction => |disjunction| {
            for (disjunction.predicates) |term| try validateRowSecurityPredicateForSchema(alloc, schema, term);
        },
    }
}

fn rowSecurityPolicyColumn(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, field) or std.mem.eql(u8, column.path, field)) return column;
    }
    return null;
}

fn validateRowSecurityLiteralForColumn(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    value_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    switch (column.field_type) {
        .keyword, .text, .link, .html, .search_as_you_type, .blob => {
            if (parsed.value != .string) return error.UnsupportedSqlShape;
        },
        .boolean => {
            if (parsed.value != .bool) return error.UnsupportedSqlShape;
        },
        .numeric, .datetime => switch (parsed.value) {
            .integer, .float, .number_string => {},
            else => return error.UnsupportedSqlShape,
        },
        else => return error.UnsupportedSqlShape,
    }
}

const SqlPrivilegeResourceTarget = struct {
    resource_type: usermgr.ResourceType,
    resource_name: []u8,

    fn deinit(self: SqlPrivilegeResourceTarget, alloc: std.mem.Allocator) void {
        alloc.free(self.resource_name);
    }
};

fn resourceTargetForSqlPrivilegeObject(
    alloc: std.mem.Allocator,
    object_kind: []const u8,
    object_name: []const u8,
    catalog: SqlAuthCatalog,
) !SqlPrivilegeResourceTarget {
    if (std.ascii.eqlIgnoreCase(object_kind, "database")) {
        return .{
            .resource_type = .database,
            .resource_name = try catalog_resources.databaseResourceNameAlloc(alloc, object_name),
        };
    }
    if (std.ascii.eqlIgnoreCase(object_kind, "schema") or std.ascii.eqlIgnoreCase(object_kind, "namespace")) {
        const target = try catalog.session().namespaceTargetFromSchemaName(object_name);
        return .{
            .resource_type = .namespace,
            .resource_name = try catalog_resources.namespaceResourceNameAlloc(alloc, target.database_name, target.namespace_name),
        };
    }
    if (std.ascii.eqlIgnoreCase(object_kind, "table")) {
        return .{
            .resource_type = .table,
            .resource_name = try sqlAuthTableResourceNameAlloc(alloc, object_name, catalog),
        };
    }
    if (std.ascii.eqlIgnoreCase(object_kind, "user")) return .{ .resource_type = .user, .resource_name = try alloc.dupe(u8, object_name) };
    if (std.mem.eql(u8, object_kind, "*")) return .{ .resource_type = .@"*", .resource_name = try alloc.dupe(u8, object_name) };
    return error.UnsupportedSqlShape;
}

fn appendAllTablesInSchemaPrivilegeChanges(
    alloc: std.mem.Allocator,
    changes: *SqlPermissionChangeList,
    subject: []const u8,
    schema_name: []const u8,
    privilege_names: []const []const u8,
    kind: PrivilegeChangeKind,
    catalog: SqlAuthCatalog,
) !void {
    const schema_target = try catalog.session().namespaceTargetFromSchemaName(schema_name);
    var matched: usize = 0;
    for (catalog.tables) |table_ref| {
        if (!std.mem.eql(u8, table_ref.database_name, schema_target.database_name)) continue;
        if (!std.mem.eql(u8, table_ref.namespace_name, schema_target.namespace_name)) continue;
        const resource_name = try catalog_resources.tableResourceNameAlloc(alloc, table_ref.database_name, table_ref.namespace_name, table_ref.table_name);
        defer alloc.free(resource_name);
        for (privilege_names) |privilege_name| {
            try appendSqlPrivilegeMapping(alloc, changes, subject, .table, resource_name, privilege_name, kind);
        }
        matched += 1;
    }
    if (matched > 0) return;

    if (catalog.tables.len == 0 and
        std.mem.eql(u8, schema_target.database_name, catalog_resources.default_database_name) and
        std.ascii.eqlIgnoreCase(schema_target.namespace_name, catalog_resources.default_namespace_name))
    {
        const table_names = catalog.public_table_names orelse return error.UnsupportedSqlShape;
        for (table_names) |table_name| {
            const resource_name = try catalog_resources.defaultPublicTableResourceNameAlloc(alloc, table_name);
            defer alloc.free(resource_name);
            for (privilege_names) |privilege_name| {
                try appendSqlPrivilegeMapping(alloc, changes, subject, .table, resource_name, privilege_name, kind);
            }
            matched += 1;
        }
        if (matched > 0) return;
    }

    return error.UnsupportedSqlShape;
}

fn appendSqlPrivilegeMapping(
    alloc: std.mem.Allocator,
    changes: *SqlPermissionChangeList,
    subject: []const u8,
    resource_type: usermgr.ResourceType,
    resource_name: []const u8,
    privilege_name: []const u8,
    kind: PrivilegeChangeKind,
) !void {
    if (std.ascii.eqlIgnoreCase(privilege_name, "select")) {
        if (resource_type != .table and resource_type != .@"*") return error.UnsupportedSqlShape;
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .read, kind);
        return;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "insert") or
        std.ascii.eqlIgnoreCase(privilege_name, "update") or
        std.ascii.eqlIgnoreCase(privilege_name, "delete") or
        std.ascii.eqlIgnoreCase(privilege_name, "truncate"))
    {
        if (resource_type != .table and resource_type != .@"*") return error.UnsupportedSqlShape;
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .write, kind);
        return;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "usage")) {
        if (resource_type != .database and resource_type != .namespace and resource_type != .@"*") return error.UnsupportedSqlShape;
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .read, kind);
        return;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "create")) {
        if (resource_type != .database and resource_type != .namespace and resource_type != .@"*") return error.UnsupportedSqlShape;
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .write, kind);
        return;
    }
    if (std.ascii.eqlIgnoreCase(privilege_name, "all")) {
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .read, kind);
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .write, kind);
        try appendPermissionChange(alloc, changes, subject, resource_type, resource_name, .admin, kind);
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
    if (std.ascii.eqlIgnoreCase(privilege_name, "usage")) return 1;
    if (std.ascii.eqlIgnoreCase(privilege_name, "create")) return 1;
    if (std.ascii.eqlIgnoreCase(privilege_name, "all")) return 3;
    return 0;
}

fn rowSecurityFilterJsonAlloc(
    alloc: std.mem.Allocator,
    predicate: sql_adapter.RowSecurityPolicyPredicate,
) anyerror![]u8 {
    return switch (predicate) {
        .current_setting_equals => |current_setting| try currentSettingRowFilterJsonAlloc(alloc, current_setting),
        .literal_equals => |literal| try literalRowFilterJsonAlloc(alloc, literal),
        .expression => |expression| try expressionRowFilterJsonAlloc(alloc, expression),
        .conjunction => |conjunction| try boolRowFilterJsonAlloc(alloc, "conjuncts", conjunction),
        .disjunction => |disjunction| try boolRowFilterJsonAlloc(alloc, "disjuncts", disjunction),
    };
}

fn currentSettingRowFilterJsonAlloc(
    alloc: std.mem.Allocator,
    predicate: sql_adapter.RowSecurityCurrentSettingPredicate,
) ![]u8 {
    usermgr.validateRoleSettingName(predicate.setting_name) catch return error.UnsupportedSqlShape;
    const field_json = try std.json.Stringify.valueAlloc(alloc, predicate.field, .{});
    defer alloc.free(field_json);
    const auth_path = try std.fmt.allocPrint(alloc, "settings.{s}", .{predicate.setting_name});
    defer alloc.free(auth_path);
    const auth_path_json = try std.json.Stringify.valueAlloc(alloc, auth_path, .{});
    defer alloc.free(auth_path_json);
    return try std.fmt.allocPrint(alloc, "{{\"term\":{{{s}:{{\"$auth\":{s}}}}}}}", .{ field_json, auth_path_json });
}

fn literalRowFilterJsonAlloc(
    alloc: std.mem.Allocator,
    predicate: sql_adapter.RowSecurityLiteralPredicate,
) ![]u8 {
    const field_json = try std.json.Stringify.valueAlloc(alloc, predicate.field, .{});
    defer alloc.free(field_json);
    return try std.fmt.allocPrint(alloc, "{{\"term\":{{{s}:{s}}}}}", .{ field_json, predicate.value_json });
}

fn expressionRowFilterJsonAlloc(
    alloc: std.mem.Allocator,
    predicate: db_mod.types.RelationalRowsExpressionCondition,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"expression_where\":[");
    try sql_adapter.writeRowExpressionConditionJson(writer, predicate);
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

fn boolRowFilterJsonAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    predicate: sql_adapter.RowSecurityConjunctionPredicate,
) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"");
    try out.appendSlice(alloc, key);
    try out.appendSlice(alloc, "\":[");
    for (predicate.predicates, 0..) |term, i| {
        const term_json = try rowSecurityFilterJsonAlloc(alloc, term);
        defer alloc.free(term_json);
        if (i != 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, term_json);
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn appendPermissionChange(
    alloc: std.mem.Allocator,
    changes: *SqlPermissionChangeList,
    subject: []const u8,
    resource_type: usermgr.ResourceType,
    resource_name: []const u8,
    permission_type: usermgr.PermissionType,
    kind: PrivilegeChangeKind,
) !void {
    const change_kind: usermgr.PermissionChangeKind = switch (kind) {
        .grant => .grant,
        .revoke => .revoke,
    };
    for (changes.items) |change| {
        if (change.kind == change_kind and
            std.mem.eql(u8, change.subject, subject) and
            change.permission.resource_type == resource_type and
            change.permission.type == permission_type and
            std.mem.eql(u8, change.permission.resource, resource_name))
        {
            return;
        }
    }
    const owned_subject = try alloc.dupe(u8, subject);
    errdefer alloc.free(owned_subject);
    const permission = try usermgr.Permission.initOwned(alloc, resource_type, resource_name, permission_type);
    errdefer {
        var mutable_permission = permission;
        mutable_permission.deinit(alloc);
    }
    try changes.append(alloc, .{
        .subject = owned_subject,
        .permission = permission,
        .kind = change_kind,
    });
}

fn freeSqlPermissionChanges(alloc: std.mem.Allocator, changes: *SqlPermissionChangeList) void {
    for (changes.items) |*change| {
        alloc.free(@constCast(change.subject));
        change.permission.deinit(alloc);
    }
    changes.deinit(alloc);
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

fn rowSecurityPolicyTargetSubjectsAlloc(
    manager: *const usermgr.UserManager,
    alloc: std.mem.Allocator,
    role_targets: []const []const u8,
) ![]const []const u8 {
    for (role_targets) |target| {
        if (std.ascii.eqlIgnoreCase(target, "public")) return try alloc.alloc([]const u8, 0);
    }
    const out = try alloc.alloc([]const u8, role_targets.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |subject| alloc.free(@constCast(subject));
        alloc.free(out);
    }
    for (role_targets, 0..) |target, i| {
        out[i] = try principalSubjectAlloc(manager, alloc, target);
        initialized += 1;
    }
    return out;
}

fn sqlRoleSubjectAlloc(alloc: std.mem.Allocator, role_name: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, role_name, "role:") or std.mem.startsWith(u8, role_name, "group:")) {
        return try alloc.dupe(u8, role_name);
    }
    return try std.fmt.allocPrint(alloc, "role:{s}", .{role_name});
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(values);
}

fn cloneStringSliceAlloc(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(@constCast(value));
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
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

const auth_test_usage_schema_json =
    \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"status":{"type":"keyword"},"region":{"type":"keyword"},"visibility":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
;

const auth_test_catalog_tables = [_]SqlAuthTableRef{
    .{ .database_name = "default", .namespace_name = "public", .table_name = "usage_records", .schema_json = auth_test_usage_schema_json },
    .{ .database_name = "default", .namespace_name = "public", .table_name = "docs", .schema_json = auth_test_usage_schema_json },
    .{ .database_name = "default", .namespace_name = "analytics", .table_name = "events", .schema_json = auth_test_usage_schema_json },
    .{ .database_name = "tenant_ops", .namespace_name = "analytics", .table_name = "usage_records", .schema_json = auth_test_usage_schema_json },
};

fn authTestCatalog() SqlAuthCatalog {
    return .{ .tables = auth_test_catalog_tables[0..] };
}

fn executeTestRelationalSqlDdlOnUserManager(
    manager: *usermgr.UserManager,
    alloc: std.mem.Allocator,
    sql: []const u8,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    return try executeRelationalSqlDdlOnUserManagerWithCatalog(manager, alloc, sql, authTestCatalog());
}

test "sql auth catalog builder shares live catalog tables for table-aware auth plans" {
    const alloc = std.testing.allocator;
    const search_path = [_][]const u8{"analytics"};
    const settings = [_]catalog_resources.SqlSessionSetting{.{
        .name = "application_name",
        .value = "auth-catalog-test",
    }};
    const session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = search_path[0..],
        .settings = settings[0..],
    };

    const FailingCatalogSource = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedAdminSnapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var failing_catalog = FailingCatalogSource{};
    var create_role_plan: sql_adapter.AuthorizationLogicalPlan = .{ .authorization_catalog = .{ .create_role = .{
        .role_name = try alloc.dupe(u8, "app_writer"),
    } } };
    defer create_role_plan.deinit(alloc);
    var role_catalog = try authCatalogForPlanWithSessionAlloc(alloc, failing_catalog.iface(), create_role_plan, session);
    defer role_catalog.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_ops", role_catalog.value.database_name);
    try std.testing.expectEqual(@as(usize, 1), role_catalog.value.search_path.len);
    try std.testing.expectEqualStrings("analytics", role_catalog.value.search_path[0]);
    try std.testing.expectEqual(@as(usize, 1), role_catalog.value.settings.len);
    try std.testing.expect(role_catalog.value.public_table_names == null);
    try std.testing.expectEqual(@as(usize, 0), role_catalog.value.tables.len);

    const catalog_tables = [_]SqlAuthTableRef{
        .{
            .database_name = "tenant_ops",
            .namespace_name = "analytics",
            .table_name = "events",
            .schema_json = "{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"string\"}}}",
        },
        .{
            .database_name = "tenant_ops",
            .namespace_name = "analytics",
            .table_name = "rollups",
            .schema_json = "{\"type\":\"object\",\"properties\":{\"bucket\":{\"type\":\"string\"}}}",
        },
    };
    var source = try SqlAuthCatalogSource.initAlloc(alloc, .{
        .database_name = "tenant_ops",
        .search_path = search_path[0..],
        .settings = settings[0..],
        .tables = catalog_tables[0..],
    });
    defer source.deinit(alloc);

    var grant_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO app_writer;");
    defer grant_sql.deinit(alloc);
    var grant_plan = try sql_adapter.planDurableSqlPlanParsedSqlWithCatalogSessionFunctionBindingsAlloc(
        alloc,
        &grant_sql,
        source.iface(),
        session,
        .{},
    );
    defer grant_plan.deinit(alloc);
    switch (grant_plan) {
        .auth => |auth_plan| {
            var grant_catalog = try authCatalogForPlanWithSessionAlloc(alloc, source.iface(), auth_plan, session);
            defer grant_catalog.deinit(alloc);
            const public_names = grant_catalog.value.public_table_names orelse return error.ExpectedPublicTableNames;
            try std.testing.expectEqual(@as(usize, 2), public_names.len);
            try std.testing.expectEqualStrings("events", public_names[0]);
            try std.testing.expectEqualStrings("rollups", public_names[1]);
            try std.testing.expectEqual(@as(usize, 2), grant_catalog.value.tables.len);
            try std.testing.expectEqualStrings("tenant_ops", grant_catalog.value.tables[0].database_name);
            try std.testing.expectEqualStrings("analytics", grant_catalog.value.tables[0].namespace_name);
            try std.testing.expectEqualStrings("events", grant_catalog.value.tables[0].table_name);
            try std.testing.expectEqualStrings(catalog_tables[0].schema_json, grant_catalog.value.tables[0].schema_json);
        },
        else => return error.ExpectedAuthPlan,
    }

    var row_security_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "ALTER TABLE events ENABLE ROW LEVEL SECURITY;");
    defer row_security_sql.deinit(alloc);
    var row_security_plan = try sql_adapter.planDurableSqlPlanParsedSqlWithCatalogSessionFunctionBindingsAlloc(
        alloc,
        &row_security_sql,
        source.iface(),
        session,
        .{},
    );
    defer row_security_plan.deinit(alloc);
    switch (row_security_plan) {
        .auth => |auth_plan| {
            var row_security_catalog = try authCatalogForPlanWithSessionAlloc(alloc, source.iface(), auth_plan, session);
            defer row_security_catalog.deinit(alloc);
            try std.testing.expect(row_security_catalog.value.public_table_names != null);
            try std.testing.expectEqual(@as(usize, 2), row_security_catalog.value.tables.len);
            try std.testing.expectEqualStrings("rollups", row_security_catalog.value.tables[1].table_name);
        },
        else => return error.ExpectedAuthPlan,
    }
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

    try std.testing.expectError(error.RoleNotFound, executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON TABLE usage_records TO app_writer;"));

    var create_role_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "CREATE ROLE app_writer;");
    defer create_role_sql.deinit(alloc);
    var created = (try executeRelationalSqlDdlParsedSqlOnUserManagerWithCatalogAndFunctionBindings(&manager, alloc, &create_role_sql, .{}, .{})).?;
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

    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_writer;"));
    const public_tables = [_][]const u8{ "usage_records", "docs" };
    var schema_granted = (try executeRelationalSqlDdlOnUserManagerWithCatalog(
        &manager,
        alloc,
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_writer;",
        .{ .public_table_names = public_tables[0..] },
    )).?;
    defer schema_granted.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_writer");
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.docs", .read));
    try std.testing.expect(!(try manager.enforce("alice", .table, "usage_records", .read)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "future_table", .read)));
    var schema_revoked = (try executeRelationalSqlDdlOnUserManagerWithCatalog(
        &manager,
        alloc,
        "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM app_writer;",
        .{ .public_table_names = public_tables[0..] },
    )).?;
    defer schema_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.docs", .read)));
    try manager.removeRoleFromUser("alice", "role:app_writer");
    try std.testing.expectError(error.UnsupportedSqlShape, executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON ALL TABLES IN SCHEMA private TO app_writer;"));

    const catalog_tables = [_]SqlAuthTableRef{
        .{ .database_name = "default", .namespace_name = "analytics", .table_name = "events" },
        .{ .database_name = "default", .namespace_name = "analytics", .table_name = "rollups" },
        .{ .database_name = "default", .namespace_name = "public", .table_name = "events" },
        .{ .database_name = "tenant_ops", .namespace_name = "analytics", .table_name = "events" },
    };
    var analytics_granted = (try executeRelationalSqlDdlOnUserManagerWithCatalog(
        &manager,
        alloc,
        "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO app_writer;",
        .{ .database_name = "default", .tables = catalog_tables[0..] },
    )).?;
    defer analytics_granted.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_writer");
    try std.testing.expect(try manager.enforce("alice", .table, "default.analytics.events", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "default.analytics.rollups", .read));
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.events", .read)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "tenant_ops.analytics.events", .read)));
    var analytics_revoked = (try executeRelationalSqlDdlOnUserManagerWithCatalog(
        &manager,
        alloc,
        "REVOKE SELECT ON ALL TABLES IN SCHEMA analytics FROM app_writer;",
        .{ .database_name = "default", .tables = catalog_tables[0..] },
    )).?;
    defer analytics_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.analytics.events", .read)));
    try manager.removeRoleFromUser("alice", "role:app_writer");

    var granted = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT, INSERT ON TABLE usage_records TO app_writer;")).?;
    defer granted.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_writer");
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .write));

    var revoked = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE INSERT ON TABLE usage_records FROM app_writer;")).?;
    defer revoked.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .read));
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.usage_records", .write)));
    try std.testing.expectError(error.RoleInUse, executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;"));

    var qualified_granted = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON TABLE analytics.events TO app_writer;")).?;
    defer qualified_granted.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .table, "default.analytics.events", .read));
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.events", .read)));
    var qualified_revoked = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE SELECT ON TABLE analytics.events FROM app_writer;")).?;
    defer qualified_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.analytics.events", .read)));

    var database_usage = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT USAGE ON DATABASE tenant_ops TO app_writer;")).?;
    defer database_usage.deinit(alloc);
    var schema_usage = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT USAGE ON SCHEMA analytics TO app_writer;")).?;
    defer schema_usage.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .database, "tenant_ops", .read));
    try std.testing.expect(try manager.enforce("alice", .namespace, "default.analytics", .read));

    var all_granted = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT ALL PRIVILEGES ON TABLE usage_records TO app_writer;")).?;
    defer all_granted.deinit(alloc);
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .write));
    try std.testing.expect(try manager.enforce("alice", .table, "default.public.usage_records", .admin));

    var all_revoked = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE ALL PRIVILEGES ON TABLE usage_records FROM app_writer;")).?;
    defer all_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.usage_records", .read)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.usage_records", .write)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.usage_records", .admin)));
    var database_usage_revoked = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE USAGE ON DATABASE tenant_ops FROM app_writer;")).?;
    defer database_usage_revoked.deinit(alloc);
    var schema_usage_revoked = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "REVOKE USAGE ON SCHEMA analytics FROM app_writer;")).?;
    defer schema_usage_revoked.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .database, "tenant_ops", .read)));
    try std.testing.expect(!(try manager.enforce("alice", .namespace, "default.analytics", .read)));
    try std.testing.expectError(error.UnsupportedSqlShape, executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT USAGE ON TABLE usage_records TO app_writer;"));
    try std.testing.expectError(error.UnsupportedSqlShape, executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT, USAGE ON TABLE usage_records TO app_writer;"));
    try std.testing.expectError(error.RoleInUse, executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;"));

    try manager.removeRoleFromUser("alice", "role:app_writer");
    var dropped = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;")).?;
    defer dropped.deinit(alloc);
    try std.testing.expect(!(try manager.enforce("alice", .table, "default.public.usage_records", .read)));

    var missing = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE IF EXISTS app_writer;")).?;
    defer missing.deinit(alloc);
    try std.testing.expect(missing.noop);

    try std.testing.expectError(error.RoleNotFound, executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET app.tenant_id = 'acme';"));
    try std.testing.expectError(error.RoleNotFound, executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET statement_timeout = '5s';"));

    var recreated = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE ROLE app_writer;")).?;
    defer recreated.deinit(alloc);
    var runtime_altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET statement_timeout = '5s';")).?;
    defer runtime_altered.deinit(alloc);
    const runtime_setting = try manager.getRoleRuntimeSetting("role:app_writer", null, "statement_timeout");
    defer alloc.free(runtime_setting);
    try std.testing.expectEqualStrings("5s", runtime_setting);
    var runtime_database_altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer IN DATABASE appdb SET statement_timeout = '1ms';")).?;
    defer runtime_database_altered.deinit(alloc);
    const scoped_runtime_setting = try manager.getRoleRuntimeSetting("role:app_writer", "appdb", "statement_timeout");
    defer alloc.free(scoped_runtime_setting);
    try std.testing.expectEqualStrings("1ms", scoped_runtime_setting);

    var altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET app.tenant_id = 'acme';")).?;
    defer altered.deinit(alloc);
    const tenant_setting = try manager.getRoleSetting("role:app_writer", "app.tenant_id");
    defer alloc.free(tenant_setting);
    try std.testing.expectEqualStrings("acme", tenant_setting);
    var scoped_app_altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id = 'scoped-acme';")).?;
    defer scoped_app_altered.deinit(alloc);
    const scoped_tenant_setting = try manager.getRoleDatabaseSetting("role:app_writer", "appdb", "app.tenant_id");
    defer alloc.free(scoped_tenant_setting);
    try std.testing.expectEqualStrings("scoped-acme", scoped_tenant_setting);
    try manager.addRoleToUser("alice", "role:app_writer");
    const effective_settings = try manager.getEffectiveRoleSettings("alice");
    defer {
        for (effective_settings) |*setting| setting.deinit(alloc);
        alloc.free(effective_settings);
    }
    try std.testing.expectEqual(@as(usize, 1), effective_settings.len);
    try std.testing.expectEqualStrings("app.tenant_id", effective_settings[0].name);
    try std.testing.expectEqualStrings("acme", effective_settings[0].value);
    const scoped_effective_settings = try manager.getEffectiveRoleSettingsForDatabase("alice", "appdb");
    defer {
        for (scoped_effective_settings) |*setting| setting.deinit(alloc);
        alloc.free(scoped_effective_settings);
    }
    try std.testing.expectEqual(@as(usize, 1), scoped_effective_settings.len);
    try std.testing.expectEqualStrings("app.tenant_id", scoped_effective_settings[0].name);
    try std.testing.expectEqualStrings("scoped-acme", scoped_effective_settings[0].value);
    var scoped_reset = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer IN DATABASE appdb RESET app.tenant_id;")).?;
    defer scoped_reset.deinit(alloc);
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleDatabaseSetting("role:app_writer", "appdb", "app.tenant_id"));
    var reset = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer RESET app.tenant_id;")).?;
    defer reset.deinit(alloc);
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleSetting("role:app_writer", "app.tenant_id"));
    var reset_missing = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer RESET app.tenant_id;")).?;
    defer reset_missing.deinit(alloc);
    var runtime_reset = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer RESET statement_timeout;")).?;
    defer runtime_reset.deinit(alloc);
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleRuntimeSetting("role:app_writer", null, "statement_timeout"));
    var runtime_reset_missing = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer RESET statement_timeout;")).?;
    defer runtime_reset_missing.deinit(alloc);
    var runtime_database_reset = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer IN DATABASE appdb RESET statement_timeout;")).?;
    defer runtime_database_reset.deinit(alloc);
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleRuntimeSetting("role:app_writer", "appdb", "statement_timeout"));
    var altered_again = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET app.tenant_id = 'acme';")).?;
    defer altered_again.deinit(alloc);
    var runtime_altered_again = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE app_writer SET statement_timeout = '10s';")).?;
    defer runtime_altered_again.deinit(alloc);
    try manager.removeRoleFromUser("alice", "role:app_writer");
    var dropped_with_setting = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;")).?;
    defer dropped_with_setting.deinit(alloc);
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleSetting("role:app_writer", "app.tenant_id"));
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleRuntimeSetting("role:app_writer", null, "statement_timeout"));
}

test "sql auth adapter resolves role setting conflicts deterministically" {
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

    try manager.createRoleSubject("role:fast");
    try manager.createRoleSubject("role:slow");
    try manager.setRoleSetting("role:fast", "app.tenant_id", "acme");
    try manager.setRoleSetting("role:slow", "app.tenant_id", "other");
    try manager.addRoleToUser("alice", "role:fast");
    try manager.addRoleToUser("alice", "role:slow");

    try std.testing.expectError(error.RoleSettingConflict, manager.getEffectiveRoleSettings("alice"));

    var altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE alice SET app.tenant_id = 'direct';")).?;
    defer altered.deinit(alloc);
    const effective_settings = try manager.getEffectiveRoleSettings("alice");
    defer {
        for (effective_settings) |*setting| setting.deinit(alloc);
        alloc.free(effective_settings);
    }
    try std.testing.expectEqual(@as(usize, 1), effective_settings.len);
    try std.testing.expectEqualStrings("app.tenant_id", effective_settings[0].name);
    try std.testing.expectEqualStrings("direct", effective_settings[0].value);
}

test "user manager applies permission change batches atomically" {
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
    try manager.createRoleSubject("role:app_writer");
    try manager.addRoleToUser("alice", "role:app_writer");

    var read_docs = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    defer read_docs.deinit(alloc);
    var write_docs = try usermgr.Permission.initOwned(alloc, .table, "docs", .write);
    defer write_docs.deinit(alloc);
    const grants = [_]usermgr.PermissionChange{
        .{ .subject = "role:app_writer", .permission = read_docs, .kind = .grant },
        .{ .subject = "role:app_writer", .permission = write_docs, .kind = .grant },
    };
    try manager.applyPermissionChangesAtomically(grants[0..]);
    try std.testing.expect(try manager.enforce("alice", .table, "docs", .read));
    try std.testing.expect(try manager.enforce("alice", .table, "docs", .write));

    const revokes = [_]usermgr.PermissionChange{
        .{ .subject = "role:app_writer", .permission = read_docs, .kind = .revoke },
        .{ .subject = "role:app_writer", .permission = write_docs, .kind = .revoke },
    };
    try manager.applyPermissionChangesAtomically(revokes[0..]);
    try std.testing.expect(!(try manager.enforce("alice", .table, "docs", .read)));
    try std.testing.expect(!(try manager.enforce("alice", .table, "docs", .write)));
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

    var granted = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "GRANT SELECT ON TABLE docs TO alice;")).?;
    defer granted.deinit(alloc);

    try std.testing.expect(try manager.enforce("alice", .table, "default.public.docs", .read));
    try std.testing.expect(!(try manager.roleSubjectExists("role:alice")));
    var altered = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER ROLE alice SET app.tenant_id = 'direct';")).?;
    defer altered.deinit(alloc);
    const setting = try manager.getRoleSetting("alice", "app.tenant_id");
    defer alloc.free(setting);
    try std.testing.expectEqualStrings("direct", setting);
    const effective_settings = try manager.getEffectiveRoleSettings("alice");
    defer {
        for (effective_settings) |*entry| entry.deinit(alloc);
        alloc.free(effective_settings);
    }
    try std.testing.expectEqual(@as(usize, 1), effective_settings.len);
    try std.testing.expectEqualStrings("direct", effective_settings[0].value);
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

    const usage_records_resource = "default.public.usage_records";

    var enabled = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;")).?;
    defer enabled.deinit(alloc);
    try std.testing.expect(try manager.sqlRowSecurityEnabled(usage_records_resource));
    try std.testing.expect(!(try manager.sqlRowSecurityEnabled("usage_records")));

    var created = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = current_setting('app.tenant_id'));")).?;
    defer created.deinit(alloc);

    const stored = try manager.getSqlRowSecurityPolicy("usage_records_tenant_policy", usage_records_resource);
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"tenant_id\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"$auth\":\"settings.app.tenant_id\"") != null);

    var literal_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_literal_policy ON usage_records USING (status = 'active');")).?;
    defer literal_policy.deinit(alloc);
    const stored_literal = try manager.getSqlRowSecurityPolicy("usage_records_literal_policy", usage_records_resource);
    defer alloc.free(stored_literal);
    try std.testing.expectEqualStrings("{\"term\":{\"status\":\"active\"}}", stored_literal);

    var check_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_check_policy ON usage_records USING (tenant_id = 'tenant-a') WITH CHECK (status = 'active');")).?;
    defer check_policy.deinit(alloc);
    const stored_check_read = try manager.getSqlRowSecurityPolicy("usage_records_check_policy", usage_records_resource);
    defer alloc.free(stored_check_read);
    try std.testing.expectEqualStrings("{\"term\":{\"tenant_id\":\"tenant-a\"}}", stored_check_read);
    const stored_check_write = try manager.getSqlRowSecurityPolicyCheck("usage_records_check_policy", usage_records_resource);
    defer alloc.free(stored_check_write);
    try std.testing.expectEqualStrings("{\"term\":{\"status\":\"active\"}}", stored_check_write);

    var expression_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_lower_policy ON usage_records USING (lower(status) = 'active');")).?;
    defer expression_policy.deinit(alloc);
    const stored_expression = try manager.getSqlRowSecurityPolicy("usage_records_lower_policy", usage_records_resource);
    defer alloc.free(stored_expression);
    try std.testing.expect(std.mem.indexOf(u8, stored_expression, "\"expression_where\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_expression, "\"op\":\"lower\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_expression, "\"field\":\"status\"") != null);

    var compound_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_compound_policy ON usage_records USING (tenant_id = 'tenant-a' AND status = 'active');")).?;
    defer compound_policy.deinit(alloc);
    const stored_compound = try manager.getSqlRowSecurityPolicy("usage_records_compound_policy", usage_records_resource);
    defer alloc.free(stored_compound);
    try std.testing.expectEqualStrings("{\"conjuncts\":[{\"term\":{\"tenant_id\":\"tenant-a\"}},{\"term\":{\"status\":\"active\"}}]}", stored_compound);

    var disjunction_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_or_policy ON usage_records USING (tenant_id = 'tenant-a' OR status = 'active');")).?;
    defer disjunction_policy.deinit(alloc);
    const stored_disjunction = try manager.getSqlRowSecurityPolicy("usage_records_or_policy", usage_records_resource);
    defer alloc.free(stored_disjunction);
    try std.testing.expectEqualStrings("{\"disjuncts\":[{\"term\":{\"tenant_id\":\"tenant-a\"}},{\"term\":{\"status\":\"active\"}}]}", stored_disjunction);
    var mixed_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_mixed_policy ON usage_records USING (tenant_id = 'tenant-a' OR status = 'active' AND region = 'us');")).?;
    defer mixed_policy.deinit(alloc);
    const stored_mixed = try manager.getSqlRowSecurityPolicy("usage_records_mixed_policy", usage_records_resource);
    defer alloc.free(stored_mixed);
    try std.testing.expectEqualStrings("{\"disjuncts\":[{\"term\":{\"tenant_id\":\"tenant-a\"}},{\"conjuncts\":[{\"term\":{\"status\":\"active\"}},{\"term\":{\"region\":\"us\"}}]}]}", stored_mixed);

    var altered_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER POLICY usage_records_literal_policy ON usage_records USING (status = 'archived');")).?;
    defer altered_policy.deinit(alloc);
    const altered_literal = try manager.getSqlRowSecurityPolicy("usage_records_literal_policy", usage_records_resource);
    defer alloc.free(altered_literal);
    try std.testing.expectEqualStrings("{\"term\":{\"status\":\"archived\"}}", altered_literal);
    const altered_literal_check = try manager.getSqlRowSecurityPolicyCheck("usage_records_literal_policy", usage_records_resource);
    defer alloc.free(altered_literal_check);
    try std.testing.expectEqualStrings("{\"term\":{\"status\":\"active\"}}", altered_literal_check);

    var altered_check_only_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER POLICY usage_records_check_policy ON usage_records WITH CHECK (status = 'ready');")).?;
    defer altered_check_only_policy.deinit(alloc);
    const preserved_check_read = try manager.getSqlRowSecurityPolicy("usage_records_check_policy", usage_records_resource);
    defer alloc.free(preserved_check_read);
    try std.testing.expectEqualStrings("{\"term\":{\"tenant_id\":\"tenant-a\"}}", preserved_check_read);
    const altered_check_only = try manager.getSqlRowSecurityPolicyCheck("usage_records_check_policy", usage_records_resource);
    defer alloc.free(altered_check_only);
    try std.testing.expectEqualStrings("{\"term\":{\"status\":\"ready\"}}", altered_check_only);
    try std.testing.expectError(error.RowFilterNotFound, executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER POLICY missing_policy ON usage_records USING (status = 'active');"));

    const filters = try manager.getRowFilters("alice");
    defer {
        for (filters) |*entry| entry.deinit(alloc);
        alloc.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 1), filters.len);
    try std.testing.expectEqualStrings(usage_records_resource, filters[0].table);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"disjuncts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"$auth\":\"settings.app.tenant_id\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"status\":\"archived\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"expression_where\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"field\":\"status\"") != null);

    const write_filters = try manager.getWriteRowFilters("alice");
    defer {
        for (write_filters) |*entry| entry.deinit(alloc);
        alloc.free(write_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), write_filters.len);
    try std.testing.expectEqualStrings(usage_records_resource, write_filters[0].table);
    try std.testing.expect(std.mem.indexOf(u8, write_filters[0].filter, "\"status\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_filters[0].filter, "\"status\":\"ready\"") != null);

    try std.testing.expectError(error.PolicyExists, executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = current_setting('app.tenant_id'));"));
    var disabled = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY;")).?;
    defer disabled.deinit(alloc);
    try std.testing.expect(!disabled.noop);
    try std.testing.expect(!(try manager.sqlRowSecurityEnabled(usage_records_resource)));
    const filters_after_disable = try manager.getRowFilters("alice");
    defer {
        for (filters_after_disable) |*entry| entry.deinit(alloc);
        alloc.free(filters_after_disable);
    }
    try std.testing.expectEqual(@as(usize, 0), filters_after_disable.len);
    var disabled_again = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY;")).?;
    defer disabled_again.deinit(alloc);
    try std.testing.expect(disabled_again.noop);

    var dropped = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "DROP POLICY usage_records_tenant_policy ON usage_records;")).?;
    defer dropped.deinit(alloc);
    try std.testing.expectError(error.RowFilterNotFound, manager.getSqlRowSecurityPolicy("usage_records_tenant_policy", usage_records_resource));

    var missing = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "DROP POLICY IF EXISTS usage_records_tenant_policy ON usage_records;")).?;
    defer missing.deinit(alloc);
    try std.testing.expect(missing.noop);
}

test "sql auth adapter maps row security policy targets to Antfly role subjects" {
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

    var alice = try manager.createUser("alice", "secret", &.{});
    defer alice.deinit(alloc);
    var bob = try manager.createUser("bob", "secret", &.{});
    defer bob.deinit(alloc);

    var reader = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE ROLE app_reader;")).?;
    defer reader.deinit(alloc);
    var writer = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE ROLE app_writer;")).?;
    defer writer.deinit(alloc);
    try manager.addRoleToUser("alice", "role:app_reader");

    var enabled = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY;")).?;
    defer enabled.deinit(alloc);
    try std.testing.expectError(
        error.RoleNotFound,
        executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY unknown_target ON docs TO missing_role USING (tenant_id = 'missing');"),
    );

    var targeted = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY docs_reader_policy ON docs TO app_reader USING (tenant_id = 'reader');")).?;
    defer targeted.deinit(alloc);

    const alice_reader_filters = try manager.getRowFilters("alice");
    defer {
        for (alice_reader_filters) |*entry| entry.deinit(alloc);
        alloc.free(alice_reader_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), alice_reader_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, alice_reader_filters[0].filter, "\"tenant_id\":\"reader\"") != null);

    const bob_reader_filters = try manager.getRowFilters("bob");
    defer {
        for (bob_reader_filters) |*entry| entry.deinit(alloc);
        alloc.free(bob_reader_filters);
    }
    try std.testing.expectEqual(@as(usize, 0), bob_reader_filters.len);

    var public_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY docs_public_policy ON docs TO PUBLIC USING (visibility = 'public');")).?;
    defer public_policy.deinit(alloc);
    const bob_public_filters = try manager.getRowFilters("bob");
    defer {
        for (bob_public_filters) |*entry| entry.deinit(alloc);
        alloc.free(bob_public_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), bob_public_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, bob_public_filters[0].filter, "\"visibility\":\"public\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bob_public_filters[0].filter, "\"tenant_id\":\"reader\"") == null);

    try manager.addRoleToUser("bob", "role:app_writer");
    var altered_target_only = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER POLICY docs_reader_policy ON docs TO app_writer;")).?;
    defer altered_target_only.deinit(alloc);
    const docs_resource = try sqlAuthTableResourceNameAlloc(alloc, "docs", .{});
    defer alloc.free(docs_resource);
    const altered_targets = try manager.getSqlRowSecurityPolicyTargets("docs_reader_policy", docs_resource);
    defer freeStringSlice(alloc, altered_targets);
    try std.testing.expectEqual(@as(usize, 1), altered_targets.len);
    try std.testing.expectEqualStrings("role:app_writer", altered_targets[0]);
    const bob_writer_reader_filters = try manager.getRowFilters("bob");
    defer {
        for (bob_writer_reader_filters) |*entry| entry.deinit(alloc);
        alloc.free(bob_writer_reader_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), bob_writer_reader_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, bob_writer_reader_filters[0].filter, "\"tenant_id\":\"reader\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bob_writer_reader_filters[0].filter, "\"visibility\":\"public\"") != null);

    var altered = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER POLICY docs_reader_policy ON docs TO app_writer USING (tenant_id = 'writer');")).?;
    defer altered.deinit(alloc);

    const alice_writer_filters = try manager.getRowFilters("alice");
    defer {
        for (alice_writer_filters) |*entry| entry.deinit(alloc);
        alloc.free(alice_writer_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), alice_writer_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, alice_writer_filters[0].filter, "\"tenant_id\":\"writer\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, alice_writer_filters[0].filter, "\"visibility\":\"public\"") != null);

    const bob_writer_filters = try manager.getRowFilters("bob");
    defer {
        for (bob_writer_filters) |*entry| entry.deinit(alloc);
        alloc.free(bob_writer_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), bob_writer_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, bob_writer_filters[0].filter, "\"tenant_id\":\"writer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bob_writer_filters[0].filter, "\"visibility\":\"public\"") != null);

    try std.testing.expectError(error.RoleInUse, executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;"));
    var dropped_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "DROP POLICY docs_reader_policy ON docs;")).?;
    defer dropped_policy.deinit(alloc);
    try manager.removeRoleFromUser("bob", "role:app_writer");
    var dropped_writer = (try executeRelationalSqlDdlOnUserManager(&manager, alloc, "DROP ROLE app_writer;")).?;
    defer dropped_writer.deinit(alloc);
}

test "sql auth adapter row security uses qualified catalog table resources" {
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

    const default_resource = "default.public.usage_records";
    const tenant_resource = "tenant_ops.analytics.usage_records";

    var default_enabled = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;")).?;
    defer default_enabled.deinit(alloc);
    var tenant_enabled = (try executeRelationalSqlDdlOnUserManagerWithCatalog(
        &manager,
        alloc,
        "ALTER TABLE analytics.usage_records ENABLE ROW LEVEL SECURITY;",
        .{ .database_name = "tenant_ops", .tables = auth_test_catalog_tables[0..] },
    )).?;
    defer tenant_enabled.deinit(alloc);

    try std.testing.expect(try manager.sqlRowSecurityEnabled(default_resource));
    try std.testing.expect(try manager.sqlRowSecurityEnabled(tenant_resource));
    try std.testing.expect(!(try manager.sqlRowSecurityEnabled("usage_records")));

    var default_policy = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_default_policy ON usage_records USING (tenant_id = 'default');")).?;
    defer default_policy.deinit(alloc);
    var tenant_policy = (try executeRelationalSqlDdlOnUserManagerWithCatalog(
        &manager,
        alloc,
        "CREATE POLICY usage_tenant_policy ON analytics.usage_records USING (tenant_id = 'tenant_ops');",
        .{ .database_name = "tenant_ops", .tables = auth_test_catalog_tables[0..] },
    )).?;
    defer tenant_policy.deinit(alloc);

    const default_stored = try manager.getSqlRowSecurityPolicy("usage_default_policy", default_resource);
    defer alloc.free(default_stored);
    try std.testing.expectEqualStrings("{\"term\":{\"tenant_id\":\"default\"}}", default_stored);
    const tenant_stored = try manager.getSqlRowSecurityPolicy("usage_tenant_policy", tenant_resource);
    defer alloc.free(tenant_stored);
    try std.testing.expectEqualStrings("{\"term\":{\"tenant_id\":\"tenant_ops\"}}", tenant_stored);
    try std.testing.expectError(error.RowFilterNotFound, manager.getSqlRowSecurityPolicy("usage_tenant_policy", default_resource));

    const filters = try manager.getRowFilters("alice");
    defer {
        for (filters) |*entry| entry.deinit(alloc);
        alloc.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 2), filters.len);
    var saw_default = false;
    var saw_tenant = false;
    for (filters) |entry| {
        if (std.mem.eql(u8, entry.table, default_resource)) saw_default = true;
        if (std.mem.eql(u8, entry.table, tenant_resource)) saw_tenant = true;
    }
    try std.testing.expect(saw_default);
    try std.testing.expect(saw_tenant);
}

test "sql row security policies are inert until row security is enabled" {
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

    const usage_records_resource = "default.public.usage_records";

    var created = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = current_setting('app.tenant_id'));")).?;
    defer created.deinit(alloc);
    const filters_before_enable = try manager.getRowFilters("alice");
    defer {
        for (filters_before_enable) |*entry| entry.deinit(alloc);
        alloc.free(filters_before_enable);
    }
    try std.testing.expectEqual(@as(usize, 0), filters_before_enable.len);

    var disabled = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY;")).?;
    defer disabled.deinit(alloc);
    try std.testing.expect(!(try manager.sqlRowSecurityEnabled(usage_records_resource)));

    var enabled = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;")).?;
    defer enabled.deinit(alloc);
    var enabled_again = (try executeTestRelationalSqlDdlOnUserManager(&manager, alloc, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;")).?;
    defer enabled_again.deinit(alloc);
    try std.testing.expect(enabled_again.noop);
    try std.testing.expect(try manager.sqlRowSecurityEnabled(usage_records_resource));
    const filters_after_enable = try manager.getRowFilters("alice");
    defer {
        for (filters_after_enable) |*entry| entry.deinit(alloc);
        alloc.free(filters_after_enable);
    }
    try std.testing.expectEqual(@as(usize, 1), filters_after_enable.len);
}
