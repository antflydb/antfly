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
const indexes_api = @import("../api/indexes.zig");
const tables_api = @import("../api/tables.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_storage = @import("../metadata/storage/mod.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const platform_time = @import("antfly_platform").time;

fn lockCatalogMutation(service: anytype) bool {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (!@hasDecl(ServiceDeclType, "lockCatalogMutation")) return false;
    service.lockCatalogMutation();
    return true;
}

fn unlockCatalogMutation(service: anytype, locked: bool) void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "unlockCatalogMutation") and locked)
        service.unlockCatalogMutation();
}

fn proposeCatalogMutation(service: anytype, command: metadata_storage.TransitionCommand) !void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "proposeTransitionCommandWithReceipt") and
        @hasDecl(ServiceDeclType, "waitForTransitionApplied"))
    {
        const receipt = try service.proposeTransitionCommandWithReceipt(command);
        service.waitForTransitionApplied(receipt) catch
            return error.MetadataMutationOutcomeUnknown;
        return;
    }
    try service.proposeTransitionCommand(command);
}

test "extension lifecycle proposal holds serialization through exact apply receipt" {
    const FakeService = struct {
        const Receipt = struct { term: u64, index: u64 };
        proposed: bool = false,
        waited: bool = false,

        pub fn proposeTransitionCommandWithReceipt(
            self: *@This(),
            _: metadata_storage.TransitionCommand,
        ) !Receipt {
            self.proposed = true;
            return .{ .term = 3, .index = 9 };
        }

        pub fn waitForTransitionApplied(
            self: *@This(),
            receipt: Receipt,
        ) !void {
            try std.testing.expect(self.proposed);
            try std.testing.expectEqual(@as(u64, 3), receipt.term);
            try std.testing.expectEqual(@as(u64, 9), receipt.index);
            self.waited = true;
        }
    };

    var service = FakeService{};
    try proposeCatalogMutation(&service, .{ .remove_reconcile_lease = .{} });
    try std.testing.expect(service.proposed);
    try std.testing.expect(service.waited);
}

test "extension lifecycle proposal preserves post-admission ambiguity" {
    const FakeService = struct {
        const Receipt = struct { term: u64, index: u64 };
        pub fn proposeTransitionCommandWithReceipt(
            _: *@This(),
            _: metadata_storage.TransitionCommand,
        ) !Receipt {
            return .{ .term = 3, .index = 9 };
        }

        pub fn waitForTransitionApplied(
            _: *@This(),
            _: Receipt,
        ) !void {
            return error.NotLeader;
        }
    };

    var service = FakeService{};
    try std.testing.expectError(
        error.MetadataMutationOutcomeUnknown,
        proposeCatalogMutation(&service, .{ .remove_reconcile_lease = .{} }),
    );
}

pub fn installOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.InstallExtensionRequest,
) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);

    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);

    var persisted_request = request;
    persisted_request.dry_run = false;
    const installed_at_ms: i64 = @intCast(@divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms));
    var installed = try catalog.installManifestOnly(extension_name, extension_name, persisted_request, installed_at_ms);
    errdefer installed.deinitOwned(alloc);

    if (!request.dry_run) {
        const members = try catalog.listMembersForExtension(alloc, extension_name);
        defer catalog.freeMembers(alloc, members);
        const dependencies = try catalog.listDependenciesForExtension(alloc, extension_name);
        defer catalog.freeDependencies(alloc, dependencies);
        const table_upserts = try planStorageMemberDeltaAlloc(alloc, &snapshot, &.{}, members);
        defer freeLifecycleTables(alloc, table_upserts);

        try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
            .upsert_tables = table_upserts,
            .upsert_installed_extensions = &.{installed},
            .upsert_extension_dependencies = dependencies,
            .upsert_extension_members = members,
        } });
    }

    return installed;
}

pub fn updateOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.UpdateExtensionRequest,
) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);

    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);

    var persisted_request = request;
    persisted_request.dry_run = false;
    var installed = try catalog.updateManifestOnly(extension_name, persisted_request);
    errdefer installed.deinitOwned(alloc);

    if (!request.dry_run) {
        const old_members = try membersForName(alloc, snapshot.extension_members, extension_name);
        defer if (old_members.len > 0) alloc.free(old_members);
        const old_dependencies = try dependenciesForName(alloc, snapshot.extension_dependencies, extension_name);
        defer if (old_dependencies.len > 0) alloc.free(old_dependencies);
        const new_members = try catalog.listMembersForExtension(alloc, extension_name);
        defer catalog.freeMembers(alloc, new_members);
        const new_dependencies = try catalog.listDependenciesForExtension(alloc, extension_name);
        defer catalog.freeDependencies(alloc, new_dependencies);
        const table_upserts = try planStorageMemberDeltaAlloc(alloc, &snapshot, old_members, new_members);
        defer freeLifecycleTables(alloc, table_upserts);
        const remove_dependency_keys = try dependencyRemoveKeysAlloc(alloc, old_dependencies);
        defer freeDependencyRemoveKeys(alloc, remove_dependency_keys);
        const remove_member_keys = try memberRemoveKeysAlloc(alloc, old_members);
        defer freeMemberRemoveKeys(alloc, remove_member_keys);

        try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
            .upsert_tables = table_upserts,
            .remove_extension_dependencies = remove_dependency_keys,
            .remove_extension_members = remove_member_keys,
            .upsert_installed_extensions = &.{installed},
            .upsert_extension_dependencies = new_dependencies,
            .upsert_extension_members = new_members,
        } });
    }

    return installed;
}

pub fn dropOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.DropExtensionRequest,
) !void {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);

    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);

    var persisted_request = request;
    persisted_request.dry_run = false;
    try catalog.dropInstalledWithMode(extension_name, persisted_request);
    if (request.dry_run) return;

    const remaining_installed = try catalog.listInstalled(alloc);
    defer catalog.freeInstalled(alloc, remaining_installed);
    const remaining_members = try catalog.listMembers(alloc);
    defer catalog.freeMembers(alloc, remaining_members);
    const remaining_dependencies = try catalog.listDependencies(alloc);
    defer catalog.freeDependencies(alloc, remaining_dependencies);
    const table_upserts = try planRemovedStorageMembersAlloc(alloc, &snapshot, remaining_members);
    defer freeLifecycleTables(alloc, table_upserts);
    const remove_dependency_keys = try missingDependencyKeysAlloc(alloc, snapshot.extension_dependencies, remaining_dependencies);
    defer freeDependencyRemoveKeys(alloc, remove_dependency_keys);
    const remove_member_keys = try missingMemberKeysAlloc(alloc, snapshot.extension_members, remaining_members);
    defer freeMemberRemoveKeys(alloc, remove_member_keys);
    const remove_installed_names = try missingInstalledNamesAlloc(alloc, snapshot.installed_extensions, remaining_installed);
    defer freeInstalledRemoveNames(alloc, remove_installed_names);

    try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
        .upsert_tables = table_upserts,
        .remove_extension_dependencies = remove_dependency_keys,
        .remove_extension_members = remove_member_keys,
        .remove_installed_extensions = remove_installed_names,
    } });
}

pub fn enableOnService(service: anytype, alloc: std.mem.Allocator, extension_name: []const u8) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);
    try catalog.enableInstalled(extension_name);
    var installed = try catalog.getInstalledAlloc(alloc, extension_name);
    errdefer installed.deinitOwned(alloc);
    try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
        .upsert_installed_extensions = &.{installed},
    } });
    return installed;
}

pub fn disableOnService(service: anytype, alloc: std.mem.Allocator, extension_name: []const u8) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);
    try catalog.disableInstalled(extension_name);
    var installed = try catalog.getInstalledAlloc(alloc, extension_name);
    errdefer installed.deinitOwned(alloc);
    try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
        .upsert_installed_extensions = &.{installed},
    } });
    return installed;
}

pub fn configureOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.ConfigureExtensionRequest,
) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);
    try catalog.configureInstalled(extension_name, request);
    var installed = try catalog.getInstalledAlloc(alloc, extension_name);
    errdefer installed.deinitOwned(alloc);
    try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
        .upsert_installed_extensions = &.{installed},
    } });
    return installed;
}

pub fn restoreOnService(
    service: anytype,
    installed: []const extension_domain.InstalledExtension,
    members: []const extension_domain.ExtensionMember,
    dependencies: []const extension_domain.ExtensionDependency,
) !void {
    if (installed.len == 0 and members.len == 0 and dependencies.len == 0) return;
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    try proposeCatalogMutation(service, .{ .apply_extension_lifecycle = .{
        .upsert_installed_extensions = installed,
        .upsert_extension_dependencies = dependencies,
        .upsert_extension_members = members,
    } });
}

fn extensionMemberTableName(member: extension_domain.ExtensionMember) ?[]const u8 {
    if (member.table_name.len != 0) return member.table_name;
    if (member.scope.kind == .table) return member.scope.table_name;
    return null;
}

fn extensionIndexMemberTableName(member: extension_domain.ExtensionMember) ?[]const u8 {
    if (member.object_kind != .index) return null;
    return extensionMemberTableName(member);
}

fn extensionEnrichmentMemberTableName(member: extension_domain.ExtensionMember) ?[]const u8 {
    if (member.object_kind != .enrichment) return null;
    return extensionMemberTableName(member);
}

fn validateNewStorageMembers(snapshot: *const metadata_api.AdminSnapshot, new_members: []const extension_domain.ExtensionMember) !void {
    for (new_members) |member| {
        if (member.object_kind != .index and member.object_kind != .enrichment) continue;
        const table_name = extensionMemberTableName(member) orelse return error.UnsupportedExtensionScope;
        if (tables_api.findTableByName(snapshot, table_name) == null) return error.TableNotFound;
    }
}

fn planStorageMemberDeltaAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    old_members: []const extension_domain.ExtensionMember,
    new_members: []const extension_domain.ExtensionMember,
) ![]metadata_table_manager.TableRecord {
    try validateNewStorageMembers(snapshot, new_members);

    var out = std.ArrayListUnmanaged(metadata_table_manager.TableRecord).empty;
    errdefer {
        for (out.items) |record| metadata_table_manager.freeTable(alloc, record);
        out.deinit(alloc);
    }

    for (snapshot.tables) |table| {
        var owned_indexes_json: ?[]u8 = null;
        defer if (owned_indexes_json) |indexes_json| alloc.free(indexes_json);
        var changed = false;

        for (old_members) |member| {
            const table_name = extensionIndexMemberTableName(member) orelse continue;
            if (!std.mem.eql(u8, table_name, table.name)) continue;
            const current = owned_indexes_json orelse table.indexes_json;
            const next = (try indexes_api.removeIndexFromTableIndexesJson(alloc, current, member.object_name)) orelse continue;
            if (owned_indexes_json) |indexes_json| alloc.free(indexes_json);
            owned_indexes_json = next;
            changed = true;
        }
        for (old_members) |member| {
            const table_name = extensionEnrichmentMemberTableName(member) orelse continue;
            if (!std.mem.eql(u8, table_name, table.name)) continue;
            const current = owned_indexes_json orelse table.indexes_json;
            const next = (try indexes_api.removeEnrichmentFromTableIndexesJson(alloc, current, member.object_name)) orelse continue;
            if (owned_indexes_json) |indexes_json| alloc.free(indexes_json);
            owned_indexes_json = next;
            changed = true;
        }

        for (new_members) |member| {
            const table_name = extensionIndexMemberTableName(member) orelse continue;
            if (!std.mem.eql(u8, table_name, table.name)) continue;
            const expanded_index_json = try tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table.name, member.owner_metadata_json, table.schema_json);
            defer alloc.free(expanded_index_json);
            const current = owned_indexes_json orelse table.indexes_json;
            const next = try indexes_api.addIndexToTableIndexesJson(alloc, current, member.object_name, expanded_index_json);
            if (owned_indexes_json) |indexes_json| alloc.free(indexes_json);
            owned_indexes_json = next;
            changed = true;
        }
        for (new_members) |member| {
            const table_name = extensionEnrichmentMemberTableName(member) orelse continue;
            if (!std.mem.eql(u8, table_name, table.name)) continue;
            const current = owned_indexes_json orelse table.indexes_json;
            const next = try indexes_api.addEnrichmentToTableIndexesJson(alloc, current, member.object_name, member.owner_metadata_json);
            if (owned_indexes_json) |indexes_json| alloc.free(indexes_json);
            owned_indexes_json = next;
            changed = true;
        }

        if (!changed) continue;
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, owned_indexes_json.?);
        var updated_record = try metadata_table_manager.cloneTable(alloc, table);
        errdefer metadata_table_manager.freeTable(alloc, updated_record);
        alloc.free(@constCast(updated_record.indexes_json));
        updated_record.indexes_json = owned_indexes_json.?;
        owned_indexes_json = null;
        try out.append(alloc, updated_record);
    }
    return try out.toOwnedSlice(alloc);
}

fn planRemovedStorageMembersAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    remaining_members: []const extension_domain.ExtensionMember,
) ![]metadata_table_manager.TableRecord {
    var removed = std.ArrayListUnmanaged(extension_domain.ExtensionMember).empty;
    defer removed.deinit(alloc);
    for (snapshot.extension_members) |member| {
        if (memberExists(remaining_members, member)) continue;
        try removed.append(alloc, member);
    }
    return try planStorageMemberDeltaAlloc(alloc, snapshot, removed.items, &.{});
}

fn freeLifecycleTables(alloc: std.mem.Allocator, tables: []metadata_table_manager.TableRecord) void {
    for (tables) |record| metadata_table_manager.freeTable(alloc, record);
    if (tables.len > 0) alloc.free(tables);
}

fn memberRemoveKeysAlloc(
    alloc: std.mem.Allocator,
    members: []const extension_domain.ExtensionMember,
) ![]metadata_storage.ExtensionMemberKey {
    const out = try alloc.alloc(metadata_storage.ExtensionMemberKey, members.len);
    errdefer alloc.free(out);
    for (members, 0..) |member, i| {
        out[i] = .{
            .extension_name = member.extension_name,
            .object_kind = member.object_kind,
            .object_name = member.object_name,
        };
    }
    return out;
}

fn missingMemberKeysAlloc(
    alloc: std.mem.Allocator,
    members: []const extension_domain.ExtensionMember,
    remaining_members: []const extension_domain.ExtensionMember,
) ![]metadata_storage.ExtensionMemberKey {
    var out = std.ArrayListUnmanaged(metadata_storage.ExtensionMemberKey).empty;
    errdefer out.deinit(alloc);
    for (members) |member| {
        if (memberExists(remaining_members, member)) continue;
        try out.append(alloc, .{
            .extension_name = member.extension_name,
            .object_kind = member.object_kind,
            .object_name = member.object_name,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeMemberRemoveKeys(alloc: std.mem.Allocator, keys: []metadata_storage.ExtensionMemberKey) void {
    if (keys.len > 0) alloc.free(keys);
}

fn dependencyRemoveKeysAlloc(
    alloc: std.mem.Allocator,
    dependencies: []const extension_domain.ExtensionDependency,
) ![]metadata_storage.ExtensionDependencyKey {
    const out = try alloc.alloc(metadata_storage.ExtensionDependencyKey, dependencies.len);
    errdefer alloc.free(out);
    for (dependencies, 0..) |dependency, i| {
        out[i] = .{
            .extension_name = dependency.extension_name,
            .required_extension_name = dependency.required_extension_name,
            .package_name = dependency.package_name,
        };
    }
    return out;
}

fn missingDependencyKeysAlloc(
    alloc: std.mem.Allocator,
    dependencies: []const extension_domain.ExtensionDependency,
    remaining_dependencies: []const extension_domain.ExtensionDependency,
) ![]metadata_storage.ExtensionDependencyKey {
    var out = std.ArrayListUnmanaged(metadata_storage.ExtensionDependencyKey).empty;
    errdefer out.deinit(alloc);
    for (dependencies) |dependency| {
        if (dependencyExists(remaining_dependencies, dependency)) continue;
        try out.append(alloc, .{
            .extension_name = dependency.extension_name,
            .required_extension_name = dependency.required_extension_name,
            .package_name = dependency.package_name,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeDependencyRemoveKeys(alloc: std.mem.Allocator, keys: []metadata_storage.ExtensionDependencyKey) void {
    if (keys.len > 0) alloc.free(keys);
}

fn missingInstalledNamesAlloc(
    alloc: std.mem.Allocator,
    installed_extensions: []const extension_domain.InstalledExtension,
    remaining_installed: []const extension_domain.InstalledExtension,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);
    for (installed_extensions) |installed| {
        if (installedExists(remaining_installed, installed.name)) continue;
        try out.append(alloc, installed.name);
    }
    return try out.toOwnedSlice(alloc);
}

fn freeInstalledRemoveNames(alloc: std.mem.Allocator, names: []const []const u8) void {
    if (names.len > 0) alloc.free(@constCast(names));
}

fn membersForName(
    alloc: std.mem.Allocator,
    members: []const extension_domain.ExtensionMember,
    extension_name: []const u8,
) ![]extension_domain.ExtensionMember {
    var count: usize = 0;
    for (members) |member| {
        if (std.mem.eql(u8, member.extension_name, extension_name)) count += 1;
    }
    const out = try alloc.alloc(extension_domain.ExtensionMember, count);
    var i: usize = 0;
    for (members) |member| {
        if (!std.mem.eql(u8, member.extension_name, extension_name)) continue;
        out[i] = member;
        i += 1;
    }
    return out;
}

fn dependenciesForName(
    alloc: std.mem.Allocator,
    dependencies: []const extension_domain.ExtensionDependency,
    extension_name: []const u8,
) ![]extension_domain.ExtensionDependency {
    var count: usize = 0;
    for (dependencies) |dependency| {
        if (std.mem.eql(u8, dependency.extension_name, extension_name)) count += 1;
    }
    const out = try alloc.alloc(extension_domain.ExtensionDependency, count);
    var i: usize = 0;
    for (dependencies) |dependency| {
        if (!std.mem.eql(u8, dependency.extension_name, extension_name)) continue;
        out[i] = dependency;
        i += 1;
    }
    return out;
}

fn memberExists(members: []const extension_domain.ExtensionMember, needle: extension_domain.ExtensionMember) bool {
    for (members) |member| {
        if (std.mem.eql(u8, member.extension_name, needle.extension_name) and
            member.object_kind == needle.object_kind and
            std.mem.eql(u8, member.object_name, needle.object_name))
        {
            return true;
        }
    }
    return false;
}

fn dependencyExists(dependencies: []const extension_domain.ExtensionDependency, needle: extension_domain.ExtensionDependency) bool {
    for (dependencies) |dependency| {
        if (std.mem.eql(u8, dependency.extension_name, needle.extension_name) and
            std.mem.eql(u8, dependency.required_extension_name, needle.required_extension_name) and
            std.mem.eql(u8, dependency.package_name, needle.package_name))
        {
            return true;
        }
    }
    return false;
}

fn installedExists(installed_extensions: []const extension_domain.InstalledExtension, name: []const u8) bool {
    for (installed_extensions) |installed| {
        if (std.mem.eql(u8, installed.name, name)) return true;
    }
    return false;
}

test "extension lifecycle rejects removing referenced artifact enrichment" {
    var tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .indexes_json = "{\"enrichments\":[{\"name\":\"document_units_v1\",\"kind\":\"asset\",\"field\":\"url\"},{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"text\",\"source_artifact_name\":\"document_units_v1\",\"chunk_size\":512}]}",
        .placement_role = "data",
    }};
    var snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    const old_members = [_]extension_domain.ExtensionMember{.{
        .extension_name = "docaf",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .object_kind = .enrichment,
        .object_name = "document_units_v1",
        .table_name = "docs",
    }};

    try std.testing.expectError(
        error.InvalidEnrichmentConfig,
        planStorageMemberDeltaAlloc(std.testing.allocator, &snapshot, old_members[0..], &.{}),
    );
}

test "extension lifecycle allows cascading artifact enrichment removal in one delta" {
    var tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .indexes_json = "{\"enrichments\":[{\"name\":\"document_units_v1\",\"kind\":\"asset\",\"field\":\"url\"},{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"text\",\"source_artifact_name\":\"document_units_v1\",\"chunk_size\":512}]}",
        .placement_role = "data",
    }};
    var snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    const old_members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "docaf",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .enrichment,
            .object_name = "document_units_v1",
            .table_name = "docs",
        },
        .{
            .extension_name = "docaf",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .enrichment,
            .object_name = "document_chunks_v1",
            .table_name = "docs",
        },
    };

    const updates = try planStorageMemberDeltaAlloc(std.testing.allocator, &snapshot, old_members[0..], &.{});
    defer freeLifecycleTables(std.testing.allocator, updates);
    try std.testing.expectEqual(@as(usize, 1), updates.len);
    try std.testing.expect(std.mem.indexOf(u8, updates[0].indexes_json, "document_units_v1") == null);
    try std.testing.expect(std.mem.indexOf(u8, updates[0].indexes_json, "document_chunks_v1") == null);
}
