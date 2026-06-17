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
const relational_sql = @import("../api/relational_sql.zig");
const tables_api = @import("../api/tables.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");

pub fn executeRelationalSqlDdlOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    sql: []const u8,
) !?tables_api.AppliedRelationalSqlDdlRecord {
    var plan = try relational_sql.lowerDdlPlanAlloc(alloc, sql);
    defer plan.deinit(alloc);

    switch (plan) {
        .adapter_noop => |noop| {
            if (noop.reason != .extension) return null;
            return try noopRecordAlloc(alloc);
        },
        .extension_catalog => |extension_plan| {
            switch (extension_plan) {
                .create => |create| return try executeCreate(service, alloc, create),
                .update => |update| return try executeUpdate(service, alloc, update),
                .drop => |drop| return try executeDrop(service, alloc, drop),
            }
        },
        else => return null,
    }
}

fn executeCreate(
    service: anytype,
    alloc: std.mem.Allocator,
    plan: relational_sql.CreateExtensionPlan,
) !tables_api.AppliedRelationalSqlDdlRecord {
    {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);
        if (findInstalledExtension(&snapshot, plan.extension_name)) |installed| {
            if (!plan.if_not_exists) return error.ExtensionAlreadyInstalled;
            if (plan.version) |version| {
                if (!std.mem.eql(u8, installed.package_version, version)) return error.ExtensionVersionMismatch;
            }
            return try noopRecordAlloc(alloc);
        }
    }

    const package_name = packageNameForSqlExtension(plan.extension_name);
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
    plan: relational_sql.UpdateExtensionPlan,
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
    plan: relational_sql.DropExtensionPlan,
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

fn packageNameForSqlExtension(extension_name: []const u8) []const u8 {
    if (std.ascii.eqlIgnoreCase(extension_name, "uuid-ossp")) return "uuid_ossp";
    return extension_name;
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
    installed_removes: usize = 0,

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

    pub fn proposeTransitionCommand(self: *@This(), command: anytype) !void {
        self.proposed += 1;
        const delta = command.apply_extension_lifecycle;
        const Delta = @TypeOf(delta);
        if (@hasField(Delta, "upsert_installed_extensions")) {
            self.installed_upserts += delta.upsert_installed_extensions.len;
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
