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
const extension_domain = @import("mod.zig");
const sql_adapter = @import("../sql/mod.zig");
const tables_api = @import("../api/tables.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_storage = @import("../metadata/storage/raft_apply_store.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");

pub fn executeRelationalSqlDdlOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    sql: []const u8,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    var plan = try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed_sql, .{});
    defer plan.deinit(alloc);
    return try executeRelationalSqlLogicalPlanOnService(service, alloc, plan);
}

pub fn executeRelationalSqlLogicalPlanOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: sql_adapter.LogicalSqlPlan,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    switch (plan) {
        .other_ddl => |other| switch (other) {
            .adapter_noop => |noop| {
                if (noop.reason != .extension) return null;
                return try noopRecordAlloc(alloc);
            },
            .moved => return null,
        },
        .extension => |extension_plan| return try executeRelationalSqlExtensionPlanOnService(service, alloc, extension_plan),
        else => return null,
    }
}

pub fn executeRelationalSqlOtherDdlPlanOnService(
    alloc: std.mem.Allocator,
    plan: sql_adapter.OtherDdlLogicalPlan,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    switch (plan) {
        .adapter_noop => |noop| {
            if (noop.reason != .extension) return null;
            return try noopRecordAlloc(alloc);
        },
        .moved => return null,
    }
}

pub fn executeRelationalSqlExtensionLogicalPlanOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: sql_adapter.LogicalSqlPlan,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    switch (plan) {
        .extension => |extension_plan| return try executeRelationalSqlExtensionPlanOnService(service, alloc, extension_plan),
        else => return null,
    }
}

pub fn executeRelationalSqlExtensionPlanOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: sql_adapter.ExtensionCatalogPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    switch (plan) {
        .create => |create| return try executeCreate(service, alloc, create),
        .update => |update| return try executeUpdate(service, alloc, update),
        .drop => |drop| return try executeDrop(service, alloc, drop),
    }
}

pub fn executeRelationalSqlExtensionNoopAlloc(
    alloc: std.mem.Allocator,
) !tables_api.AppliedRelationalSqlDdlRecord {
    return try noopRecordAlloc(alloc);
}

fn executeCreate(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: sql_adapter.CreateExtensionPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    if (findInstalledExtension(&snapshot, plan.extension_name)) |installed| {
        if (!plan.if_not_exists) return error.ExtensionAlreadyInstalled;
        if (plan.version) |version| {
            if (!std.mem.eql(u8, installed.package_version, version)) return error.ExtensionVersionMismatch;
        }
        return try noopRecordAlloc(alloc);
    }

    const package_name = try packageNameForSqlExtension(&snapshot, plan.extension_name, plan.version);
    var installed = try extension_domain.lifecycle.installPackageOnService(service, alloc, plan.extension_name, package_name, .{
        .version = plan.version orelse "",
        .scope = .{ .kind = .cluster },
        .config_json = "{}",
    });
    defer installed.deinitOwned(alloc);
    return try changedRecordAlloc(alloc);
}

fn executeUpdate(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: sql_adapter.UpdateExtensionPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var installed = try extension_domain.lifecycle.updateOnService(service, alloc, plan.extension_name, .{
        .target_version = plan.target_version orelse "",
    });
    defer installed.deinitOwned(alloc);
    return try changedRecordAlloc(alloc);
}

fn executeDrop(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: sql_adapter.DropExtensionPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);
        if (findInstalledExtension(&snapshot, plan.extension_name) == null and plan.if_exists) {
            return try noopRecordAlloc(alloc);
        }
    }

    try extension_domain.lifecycle.dropOnService(service, alloc, plan.extension_name, .{
        .mode = if (plan.cascade) .cascade else .restrict,
    });
    return try changedRecordAlloc(alloc);
}

fn packageNameForSqlExtension(
    snapshot: *const metadata_api.AdminSnapshot,
    extension_name: []const u8,
    version: ?[]const u8,
) ![]const u8 {
    if (try findPackageForSqlName(snapshot.extension_packages, extension_name, version)) |package| return package.name;
    return extension_name;
}

fn findPackageForSqlName(
    packages: []const extension_domain.PackageManifest,
    sql_name: []const u8,
    version: ?[]const u8,
) !?*const extension_domain.PackageManifest {
    var found: ?*const extension_domain.PackageManifest = null;
    for (packages) |*package| {
        if (version) |target_version| {
            if (!std.mem.eql(u8, package.version, target_version)) continue;
        }
        if (!packageMatchesSqlName(package.*, sql_name)) continue;
        if (found) |existing| {
            if (!std.mem.eql(u8, existing.name, package.name)) return error.AmbiguousExtensionSqlName;
            if (version == null and extension_domain.packageVersionLess(existing.version, package.version)) {
                found = package;
            }
            continue;
        }
        found = package;
    }
    return found;
}

fn packageMatchesSqlName(package: extension_domain.PackageManifest, sql_name: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(package.name, sql_name)) return true;
    for (package.sql_names) |alias| {
        if (std.ascii.eqlIgnoreCase(alias, sql_name)) return true;
    }
    return false;
}

fn findInstalledExtension(snapshot: *const metadata_api.AdminSnapshot, name: []const u8) ?*const extension_domain.InstalledExtension {
    for (snapshot.installed_extensions) |*installed| {
        if (std.mem.eql(u8, installed.name, name)) return installed;
    }
    return null;
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

const TestService = struct {
    packages: []const extension_domain.PackageManifest = &.{},
    installed: []const extension_domain.InstalledExtension = &.{},
    proposed: usize = 0,
    installed_upserts: usize = 0,
    extension_member_upserts: usize = 0,
    installed_removes: usize = 0,
    saw_uuid_ossp_package: bool = false,
    saw_gen_random_uuid_function: bool = false,

    pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = &.{},
            .ranges = &.{},
            .stores = &.{},
            .placement_intents = &.{},
            .extension_packages = @constCast(self.packages),
            .installed_extensions = @constCast(self.installed),
            .split_transitions = &.{},
            .merge_transitions = &.{},
        };
    }

    pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

    pub fn proposeTransitionCommand(self: *@This(), command: metadata_storage.TransitionCommand) !void {
        self.proposed += 1;
        const delta = command.apply_extension_lifecycle;
        const Delta = @TypeOf(delta);
        if (@hasField(Delta, "upsert_installed_extensions")) {
            self.installed_upserts += delta.upsert_installed_extensions.len;
            for (delta.upsert_installed_extensions) |installed| {
                if (std.mem.eql(u8, installed.package_name, "uuid_ossp")) self.saw_uuid_ossp_package = true;
            }
        }
        if (@hasField(Delta, "upsert_extension_members")) {
            self.extension_member_upserts += delta.upsert_extension_members.len;
            for (delta.upsert_extension_members) |member| {
                if (member.object_kind == .query_function and std.mem.eql(u8, member.object_name, "gen_random_uuid")) {
                    self.saw_gen_random_uuid_function = true;
                }
            }
        }
        if (@hasField(Delta, "remove_installed_extensions")) {
            self.installed_removes += delta.remove_installed_extensions.len;
        }
    }
};

const test_postgis_package = extension_domain.PackageManifest{
    .name = "postgis",
    .version = "3.4.0",
    .digest = "sha256:postgis-3.4.0",
    .install = .{ .scopes_supported = &.{.cluster} },
};

const test_postgis_35_package = extension_domain.PackageManifest{
    .name = "postgis",
    .version = "3.5.0",
    .digest = "sha256:postgis-3.5.0",
    .install = .{ .scopes_supported = &.{.cluster} },
    .updates = &.{.{
        .from_version = "3.4.0",
        .to_version = "3.5.0",
        .path = "updates/3.4.0--3.5.0.json",
    }},
};

const test_uuid_ossp_package = extension_domain.PackageManifest{
    .name = "uuid_ossp",
    .version = "1.0.0",
    .digest = "sha256:uuid",
    .sql_names = &.{"uuid-ossp"},
    .install = .{ .scopes_supported = &.{.cluster} },
};

const test_pgcrypto_package = extension_domain.PackageManifest{
    .name = "pgcrypto",
    .version = "1.0.0",
    .digest = "sha256:pgcrypto",
    .sql_names = &.{"pgcrypto"},
    .install = .{
        .scopes_supported = &.{.cluster},
        .objects = &.{.{
            .kind = .query_function,
            .name = "gen_random_uuid",
            .config_json = "{\"native_expression\":\"uuid_v4\",\"arity\":0,\"sql_names\":[\"gen_random_uuid\"]}",
        }},
    },
};

const test_postgis_installed = extension_domain.InstalledExtension{
    .name = "postgis",
    .package_name = "postgis",
    .package_version = "3.4.0",
    .package_digest = "sha256:postgis-3.4.0",
    .scope = .{ .kind = .cluster },
    .status = .ready,
};

test "sql extension adapter installs create extension through lifecycle" {
    const alloc = std.testing.allocator;
    var service = TestService{ .packages = &.{test_postgis_package} };
    var applied = (try executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION postgis VERSION '3.4.0';")).?;
    defer applied.deinit(alloc);

    try std.testing.expect(!applied.noop);
    try std.testing.expectEqual(@as(usize, 1), service.proposed);
    try std.testing.expectEqual(@as(usize, 1), service.installed_upserts);
}

test "sql extension adapter resolves package from manifest sql name" {
    const alloc = std.testing.allocator;
    var service = TestService{ .packages = &.{test_uuid_ossp_package} };
    var applied = (try executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION \"uuid-ossp\" VERSION '1.0.0';")).?;
    defer applied.deinit(alloc);

    try std.testing.expect(!applied.noop);
    try std.testing.expect(service.saw_uuid_ossp_package);
}

test "sql extension adapter installs query function members through lifecycle" {
    const alloc = std.testing.allocator;
    var service = TestService{ .packages = &.{test_pgcrypto_package} };
    var applied = (try executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION pgcrypto;")).?;
    defer applied.deinit(alloc);

    try std.testing.expect(!applied.noop);
    try std.testing.expectEqual(@as(usize, 1), service.proposed);
    try std.testing.expectEqual(@as(usize, 1), service.installed_upserts);
    try std.testing.expectEqual(@as(usize, 1), service.extension_member_upserts);
    try std.testing.expect(service.saw_gen_random_uuid_function);
}

test "sql extension adapter requires manifest sql name aliases" {
    const alloc = std.testing.allocator;
    const uuid_without_sql_alias = extension_domain.PackageManifest{
        .name = "uuid_ossp",
        .version = "1.0.0",
        .digest = "sha256:uuid",
        .install = .{ .scopes_supported = &.{.cluster} },
    };
    var service = TestService{ .packages = &.{uuid_without_sql_alias} };

    try std.testing.expectError(
        error.PackageNotFound,
        executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION \"uuid-ossp\" VERSION '1.0.0';"),
    );
}

test "sql extension adapter rejects ambiguous manifest sql names" {
    const alloc = std.testing.allocator;
    const other_uuid_package = extension_domain.PackageManifest{
        .name = "other_uuid",
        .version = "1.0.0",
        .digest = "sha256:other-uuid",
        .sql_names = &.{"uuid-ossp"},
        .install = .{ .scopes_supported = &.{.cluster} },
    };
    var service = TestService{ .packages = &.{ test_uuid_ossp_package, other_uuid_package } };

    try std.testing.expectError(
        error.AmbiguousExtensionSqlName,
        executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION \"uuid-ossp\" VERSION '1.0.0';"),
    );
}

test "sql extension adapter treats matching create if not exists as no-op" {
    const alloc = std.testing.allocator;
    var service = TestService{
        .packages = &.{test_postgis_package},
        .installed = &.{test_postgis_installed},
    };
    var applied = (try executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.4.0';")).?;
    defer applied.deinit(alloc);

    try std.testing.expect(applied.noop);
    try std.testing.expectEqual(@as(usize, 0), service.proposed);
    try std.testing.expectError(error.ExtensionVersionMismatch, executeRelationalSqlDdlOnService(&service, alloc, "CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.5.0';"));
}

test "sql extension adapter updates extension through lifecycle" {
    const alloc = std.testing.allocator;
    var service = TestService{
        .packages = &.{ test_postgis_package, test_postgis_35_package },
        .installed = &.{test_postgis_installed},
    };
    var applied = (try executeRelationalSqlDdlOnService(&service, alloc, "ALTER EXTENSION postgis UPDATE TO '3.5.0';")).?;
    defer applied.deinit(alloc);

    try std.testing.expect(!applied.noop);
    try std.testing.expectEqual(@as(usize, 1), service.proposed);
    try std.testing.expectEqual(@as(usize, 1), service.installed_upserts);
    try std.testing.expectEqual(@as(usize, 0), service.installed_removes);
}

test "sql extension adapter drops extension through lifecycle and no-ops missing if exists" {
    const alloc = std.testing.allocator;
    var installed_service = TestService{
        .packages = &.{test_postgis_package},
        .installed = &.{test_postgis_installed},
    };
    var dropped = (try executeRelationalSqlDdlOnService(&installed_service, alloc, "DROP EXTENSION postgis RESTRICT;")).?;
    defer dropped.deinit(alloc);
    try std.testing.expect(!dropped.noop);
    try std.testing.expectEqual(@as(usize, 1), installed_service.proposed);
    try std.testing.expectEqual(@as(usize, 1), installed_service.installed_removes);

    var missing_service = TestService{ .packages = &.{test_postgis_package} };
    var missing = (try executeRelationalSqlDdlOnService(&missing_service, alloc, "DROP EXTENSION IF EXISTS postgis;")).?;
    defer missing.deinit(alloc);
    try std.testing.expect(missing.noop);
    try std.testing.expectEqual(@as(usize, 0), missing_service.proposed);
}

test "sql extension adapter ignores non-extension ddl" {
    const alloc = std.testing.allocator;
    var service = TestService{};
    try std.testing.expect((try executeRelationalSqlDdlOnService(&service, alloc, "CREATE TABLE users (id text PRIMARY KEY);")) == null);
}
