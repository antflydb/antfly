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
const managed_embedder = @import("../inference/managed_embedder.zig");
const tables_api = @import("../api/tables.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_storage = @import("../metadata/storage/mod.zig");
const metadata_service = @import("../metadata/service.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_topology_protocol = @import("../metadata/topology_protocol.zig");
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

fn probeLifecycleProtocolReadiness(
    service: anytype,
    required: bool,
) !?metadata_service.TableTopologyProtocolReadiness {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    // Lifecycle table preconditions change replicated apply semantics. Reject
    // incomplete or transitional memberships before admitting a v2 command.
    if (required and @hasDecl(ServiceDeclType, "ensureTableTopologyProtocolReadyWithContext"))
        return try service.ensureTableTopologyProtocolReadyWithContext(
            .{},
            metadata_topology_protocol.extension_lifecycle_table_cas_version,
        );
    return null;
}

fn validateLifecycleProtocolReadiness(
    service: anytype,
    readiness: ?metadata_service.TableTopologyProtocolReadiness,
) !void {
    const expected = readiness orelse return;
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "validateTableTopologyProtocolReadinessWithContext"))
        try service.validateTableTopologyProtocolReadinessWithContext(.{}, expected);
}

fn lifecycleMutationReadiness(
    service: anytype,
    table_precondition_count: usize,
    readiness: ?metadata_service.TableTopologyProtocolReadiness,
) !?metadata_service.TableTopologyProtocolReadiness {
    if (table_precondition_count == 0) return null;
    if (readiness) |ready| return ready;
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "ensureTableTopologyProtocolReadyWithContext"))
        return error.ExtensionLifecycleProtocolReadinessRequired;
    return null;
}

test "extension lifecycle protocol readiness is required for table CAS" {
    const LegacyService = struct {};
    const ProtocolService = struct {
        pub fn ensureTableTopologyProtocolReadyWithContext(
            _: *@This(),
            _: anytype,
            _: u16,
        ) !metadata_service.TableTopologyProtocolReadiness {
            unreachable;
        }
    };
    var legacy = LegacyService{};
    var protocol = ProtocolService{};
    try std.testing.expect((try lifecycleMutationReadiness(&protocol, 0, null)) == null);
    try std.testing.expect((try lifecycleMutationReadiness(&legacy, 1, null)) == null);
    try std.testing.expectError(
        error.ExtensionLifecycleProtocolReadinessRequired,
        lifecycleMutationReadiness(&protocol, 1, null),
    );
}

fn captureLifecycleSnapshot(service: anytype) !metadata_api.AdminSnapshot {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "ensureLinearizableReadWithContext") and
        @hasDecl(ServiceDeclType, "adminSnapshotFence"))
    {
        return try metadata_service.coherentLinearizableAdminSnapshot(ServiceDeclType, service, .{});
    }
    return try service.adminSnapshot();
}

fn lifecycleDeltaApplied(snapshot: *const metadata_api.AdminSnapshot, delta: metadata_storage.ExtensionLifecycleDelta) bool {
    for (delta.upsert_tables) |expected| {
        var found = false;
        for (snapshot.tables) |actual| {
            if (actual.table_id != expected.table_id) continue;
            found = metadata_table_manager.tableDefinitionsEqual(actual, expected);
            break;
        }
        if (!found) return false;
    }
    for (delta.upsert_installed_extensions) |expected| {
        var found = false;
        for (snapshot.installed_extensions) |actual| {
            if (!std.mem.eql(u8, actual.name, expected.name)) continue;
            found = extension_domain.installedExtensionsEqual(actual, expected);
            break;
        }
        if (!found) return false;
    }
    for (delta.remove_installed_extensions) |name| {
        const replaced = for (delta.upsert_installed_extensions) |upsert| {
            if (std.mem.eql(u8, upsert.name, name)) break true;
        } else false;
        if (replaced) continue;
        for (snapshot.installed_extensions) |actual| {
            if (std.mem.eql(u8, actual.name, name)) return false;
        }
    }
    for (delta.upsert_extension_members) |expected| {
        var found = false;
        for (snapshot.extension_members) |actual| {
            if (extension_domain.extensionMembersEqual(actual, expected)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    for (delta.remove_extension_members) |key| {
        const replaced = for (delta.upsert_extension_members) |upsert| {
            if (std.mem.eql(u8, upsert.extension_name, key.extension_name) and
                upsert.object_kind == key.object_kind and
                std.mem.eql(u8, upsert.object_name, key.object_name)) break true;
        } else false;
        if (replaced) continue;
        for (snapshot.extension_members) |actual| {
            if (std.mem.eql(u8, actual.extension_name, key.extension_name) and
                actual.object_kind == key.object_kind and
                std.mem.eql(u8, actual.object_name, key.object_name)) return false;
        }
    }
    for (delta.upsert_extension_dependencies) |expected| {
        var found = false;
        for (snapshot.extension_dependencies) |actual| {
            if (extension_domain.extensionDependenciesEqual(actual, expected)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    for (delta.remove_extension_dependencies) |key| {
        const replaced = for (delta.upsert_extension_dependencies) |upsert| {
            if (std.mem.eql(u8, upsert.extension_name, key.extension_name) and
                std.mem.eql(u8, upsert.required_extension_name, key.required_extension_name) and
                std.mem.eql(u8, upsert.package_name, key.package_name)) break true;
        } else false;
        if (replaced) continue;
        for (snapshot.extension_dependencies) |actual| {
            if (std.mem.eql(u8, actual.extension_name, key.extension_name) and
                std.mem.eql(u8, actual.required_extension_name, key.required_extension_name) and
                std.mem.eql(u8, actual.package_name, key.package_name)) return false;
        }
    }
    return true;
}

fn verifyLifecycleProjection(
    service: anytype,
    delta: metadata_storage.ExtensionLifecycleDelta,
) !bool {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "verifyExtensionLifecycleProjection"))
        return try service.verifyExtensionLifecycleProjection(delta);

    // Lightweight test and embedding services may not expose a projected
    // store. Preserve compatibility for those adapters only; production
    // services use the bounded point-read verifier above.
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    return lifecycleDeltaApplied(&snapshot, delta);
}

fn proposeLifecycleMutation(
    service: anytype,
    readiness: ?metadata_service.TableTopologyProtocolReadiness,
    delta: metadata_storage.ExtensionLifecycleDelta,
) !void {
    // The remote probe intentionally runs outside the catalog lane. Fence its
    // term and exact membership under the lane immediately before admission.
    try validateLifecycleProtocolReadiness(service, readiness);
    const command: metadata_storage.TransitionCommand = if (delta.expected_tables.len == 0)
        .{ .apply_extension_lifecycle = delta }
    else
        .{ .apply_extension_lifecycle_v2 = delta };
    try proposeCatalogMutation(service, command);
    const applied = verifyLifecycleProjection(service, delta) catch
        return error.MetadataMutationOutcomeUnknown;
    if (!applied) return error.ExtensionLifecycleConflict;
}

fn lifecycleTablePreconditionsAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    replacements: []const metadata_table_manager.TableRecord,
) ![]metadata_storage.ExtensionLifecycleTablePrecondition {
    const out = try alloc.alloc(metadata_storage.ExtensionLifecycleTablePrecondition, replacements.len);
    errdefer alloc.free(out);
    for (replacements, out) |replacement, *expected| {
        const current = for (snapshot.tables) |table| {
            if (table.table_id == replacement.table_id) break table;
        } else return error.TableNotFound;
        expected.* = .{
            .table_id = current.table_id,
            .definition_fingerprint = metadata_table_manager.tableDefinitionFingerprint(current),
        };
    }
    return out;
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

test "extension lifecycle verification distinguishes conflicts from unknown outcomes" {
    const FakeService = struct {
        const Receipt = struct { term: u64, index: u64 };
        verification_error: bool = false,

        pub fn proposeTransitionCommandWithReceipt(
            _: *@This(),
            _: metadata_storage.TransitionCommand,
        ) !Receipt {
            return .{ .term = 3, .index = 9 };
        }

        pub fn waitForTransitionApplied(_: *@This(), _: Receipt) !void {}

        pub fn verifyExtensionLifecycleProjection(self: *@This(), _: metadata_storage.ExtensionLifecycleDelta) !bool {
            if (self.verification_error) return error.StorageUnavailable;
            return false;
        }
    };

    var conflict = FakeService{};
    try std.testing.expectError(
        error.ExtensionLifecycleConflict,
        proposeLifecycleMutation(&conflict, null, .{}),
    );

    var unknown = FakeService{ .verification_error = true };
    try std.testing.expectError(
        error.MetadataMutationOutcomeUnknown,
        proposeLifecycleMutation(&unknown, null, .{}),
    );
}

test "extension lifecycle proposal emits v2 with table CAS" {
    const FakeService = struct {
        const Receipt = struct { term: u64, index: u64 };
        proposed_v2: bool = false,
        verified_with_cas: bool = false,

        pub fn proposeTransitionCommandWithReceipt(
            self: *@This(),
            command: metadata_storage.TransitionCommand,
        ) !Receipt {
            try std.testing.expect(command == .apply_extension_lifecycle_v2);
            try std.testing.expectEqual(
                @as(usize, 1),
                command.apply_extension_lifecycle_v2.expected_tables.len,
            );
            self.proposed_v2 = true;
            return .{ .term = 3, .index = 9 };
        }

        pub fn waitForTransitionApplied(_: *@This(), _: Receipt) !void {}

        pub fn verifyExtensionLifecycleProjection(
            self: *@This(),
            delta: metadata_storage.ExtensionLifecycleDelta,
        ) !bool {
            try std.testing.expectEqual(@as(usize, 1), delta.expected_tables.len);
            self.verified_with_cas = true;
            return true;
        }
    };

    const table = metadata_table_manager.TableRecord{ .table_id = 7, .name = "docs" };
    var service = FakeService{};
    try proposeLifecycleMutation(&service, null, .{
        .expected_tables = &.{.{
            .table_id = table.table_id,
            .definition_fingerprint = metadata_table_manager.tableDefinitionFingerprint(table),
        }},
        .upsert_tables = &.{table},
    });
    try std.testing.expect(service.proposed_v2);
    try std.testing.expect(service.verified_with_cas);
}

pub fn installOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.InstallExtensionRequest,
) !extension_domain.InstalledExtension {
    var protocol_readiness: ?metadata_service.TableTopologyProtocolReadiness = null;
    while (true) {
        return installOnServiceAttempt(
            service,
            alloc,
            extension_name,
            request,
            protocol_readiness,
        ) catch |err| switch (err) {
            error.ExtensionLifecycleProtocolReadinessRequired => {
                // The attempt has released the catalog lane and all projected
                // state. Probe remotely, then re-plan from a fresh snapshot;
                // no network fanout runs while unrelated catalog work waits.
                protocol_readiness = try probeLifecycleProtocolReadiness(service, true);
                continue;
            },
            else => return err,
        };
    }
}

fn installOnServiceAttempt(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.InstallExtensionRequest,
    protocol_readiness: ?metadata_service.TableTopologyProtocolReadiness,
) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try captureLifecycleSnapshot(service);
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
        const expected_tables = try lifecycleTablePreconditionsAlloc(alloc, &snapshot, table_upserts);
        defer alloc.free(expected_tables);
        const mutation_readiness = try lifecycleMutationReadiness(service, expected_tables.len, protocol_readiness);

        try proposeLifecycleMutation(service, mutation_readiness, .{
            .expected_tables = expected_tables,
            .upsert_tables = table_upserts,
            .upsert_installed_extensions = &.{installed},
            .upsert_extension_dependencies = dependencies,
            .upsert_extension_members = members,
        });
    }

    return installed;
}

pub fn updateOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.UpdateExtensionRequest,
) !extension_domain.InstalledExtension {
    var protocol_readiness: ?metadata_service.TableTopologyProtocolReadiness = null;
    while (true) {
        return updateOnServiceAttempt(
            service,
            alloc,
            extension_name,
            request,
            protocol_readiness,
        ) catch |err| switch (err) {
            error.ExtensionLifecycleProtocolReadinessRequired => {
                protocol_readiness = try probeLifecycleProtocolReadiness(service, true);
                continue;
            },
            else => return err,
        };
    }
}

fn updateOnServiceAttempt(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.UpdateExtensionRequest,
    protocol_readiness: ?metadata_service.TableTopologyProtocolReadiness,
) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try captureLifecycleSnapshot(service);
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
        const expected_tables = try lifecycleTablePreconditionsAlloc(alloc, &snapshot, table_upserts);
        defer alloc.free(expected_tables);
        const mutation_readiness = try lifecycleMutationReadiness(service, expected_tables.len, protocol_readiness);
        const remove_dependency_keys = try dependencyRemoveKeysAlloc(alloc, old_dependencies);
        defer freeDependencyRemoveKeys(alloc, remove_dependency_keys);
        const remove_member_keys = try memberRemoveKeysAlloc(alloc, old_members);
        defer freeMemberRemoveKeys(alloc, remove_member_keys);

        try proposeLifecycleMutation(service, mutation_readiness, .{
            .expected_tables = expected_tables,
            .upsert_tables = table_upserts,
            .remove_extension_dependencies = remove_dependency_keys,
            .remove_extension_members = remove_member_keys,
            .upsert_installed_extensions = &.{installed},
            .upsert_extension_dependencies = new_dependencies,
            .upsert_extension_members = new_members,
        });
    }

    return installed;
}

pub fn dropOnService(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.DropExtensionRequest,
) !void {
    var protocol_readiness: ?metadata_service.TableTopologyProtocolReadiness = null;
    while (true) {
        return dropOnServiceAttempt(
            service,
            alloc,
            extension_name,
            request,
            protocol_readiness,
        ) catch |err| switch (err) {
            error.ExtensionLifecycleProtocolReadinessRequired => {
                protocol_readiness = try probeLifecycleProtocolReadiness(service, true);
                continue;
            },
            else => return err,
        };
    }
}

fn dropOnServiceAttempt(
    service: anytype,
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    request: extension_domain.DropExtensionRequest,
    protocol_readiness: ?metadata_service.TableTopologyProtocolReadiness,
) !void {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try captureLifecycleSnapshot(service);
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
    const expected_tables = try lifecycleTablePreconditionsAlloc(alloc, &snapshot, table_upserts);
    defer alloc.free(expected_tables);
    const mutation_readiness = try lifecycleMutationReadiness(service, expected_tables.len, protocol_readiness);
    const remove_dependency_keys = try missingDependencyKeysAlloc(alloc, snapshot.extension_dependencies, remaining_dependencies);
    defer freeDependencyRemoveKeys(alloc, remove_dependency_keys);
    const remove_member_keys = try missingMemberKeysAlloc(alloc, snapshot.extension_members, remaining_members);
    defer freeMemberRemoveKeys(alloc, remove_member_keys);
    const remove_installed_names = try missingInstalledNamesAlloc(alloc, snapshot.installed_extensions, remaining_installed);
    defer freeInstalledRemoveNames(alloc, remove_installed_names);

    try proposeLifecycleMutation(service, mutation_readiness, .{
        .expected_tables = expected_tables,
        .upsert_tables = table_upserts,
        .remove_extension_dependencies = remove_dependency_keys,
        .remove_extension_members = remove_member_keys,
        .remove_installed_extensions = remove_installed_names,
    });
}

pub fn enableOnService(service: anytype, alloc: std.mem.Allocator, extension_name: []const u8) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try captureLifecycleSnapshot(service);
    defer service.freeAdminSnapshot(&snapshot);
    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);
    try catalog.enableInstalled(extension_name);
    var installed = try catalog.getInstalledAlloc(alloc, extension_name);
    errdefer installed.deinitOwned(alloc);
    try proposeLifecycleMutation(service, null, .{
        .upsert_installed_extensions = &.{installed},
    });
    return installed;
}

pub fn disableOnService(service: anytype, alloc: std.mem.Allocator, extension_name: []const u8) !extension_domain.InstalledExtension {
    const catalog_locked = lockCatalogMutation(service);
    defer unlockCatalogMutation(service, catalog_locked);
    var snapshot = try captureLifecycleSnapshot(service);
    defer service.freeAdminSnapshot(&snapshot);
    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);
    try catalog.disableInstalled(extension_name);
    var installed = try catalog.getInstalledAlloc(alloc, extension_name);
    errdefer installed.deinitOwned(alloc);
    try proposeLifecycleMutation(service, null, .{
        .upsert_installed_extensions = &.{installed},
    });
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
    var snapshot = try captureLifecycleSnapshot(service);
    defer service.freeAdminSnapshot(&snapshot);
    var catalog = extension_domain.ExtensionCatalog.init(alloc);
    defer catalog.deinit();
    try catalog.loadProjectedRows(snapshot.extension_packages, snapshot.installed_extensions, snapshot.extension_members, snapshot.extension_dependencies);
    try catalog.configureInstalled(extension_name, request);
    var installed = try catalog.getInstalledAlloc(alloc, extension_name);
    errdefer installed.deinitOwned(alloc);
    try proposeLifecycleMutation(service, null, .{
        .upsert_installed_extensions = &.{installed},
    });
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
        try managed_embedder.validateEmbeddingProducerOwnershipJsonWithOptions(
            alloc,
            owned_indexes_json.?,
            .{
                .require_owner_for_missing_producer = true,
                .require_stable_owner_identity = true,
            },
        );
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

test "extension lifecycle rejects artifact embedding consumers without executable producers" {
    var tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .indexes_json = "{}",
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
    const new_members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .index,
            .object_name = "document_vectors",
            .table_name = "docs",
            .owner_metadata_json = "{\"type\":\"embeddings\",\"dimension\":3,\"sources\":[{\"artifact\":\"document_dense_v1\"}]}",
        },
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .enrichment,
            .object_name = "document_dense_v1",
            .table_name = "docs",
            .owner_metadata_json = "{\"name\":\"document_dense_v1\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":3}",
        },
    };

    try std.testing.expectError(
        error.MissingEmbeddingArtifactProducer,
        planStorageMemberDeltaAlloc(std.testing.allocator, &snapshot, &.{}, new_members[0..]),
    );
}

test "extension lifecycle rejects duplicate executable artifact owners" {
    var tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .indexes_json = "{}",
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
    const new_members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .index,
            .object_name = "owner_a",
            .table_name = "docs",
            .owner_metadata_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedding_name\":\"dense_v1\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"model-a\"},\"semantic_producer\":\"{\\\"version\\\":2,\\\"provider\\\":\\\"antfly\\\",\\\"model\\\":\\\"model-a\\\",\\\"endpoint\\\":\\\"antfly:embedded\\\",\\\"region\\\":\\\"\\\",\\\"request_format\\\":\\\"\\\",\\\"sparse\\\":false,\\\"multimodal\\\":false,\\\"input_type\\\":\\\"\\\",\\\"truncate\\\":\\\"\\\"}\"}",
        },
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .index,
            .object_name = "owner_b",
            .table_name = "docs",
            .owner_metadata_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedding_name\":\"dense_v1\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"model-b\"},\"semantic_producer\":\"{\\\"version\\\":2,\\\"provider\\\":\\\"antfly\\\",\\\"model\\\":\\\"model-b\\\",\\\"endpoint\\\":\\\"antfly:embedded\\\",\\\"region\\\":\\\"\\\",\\\"request_format\\\":\\\"\\\",\\\"sparse\\\":false,\\\"multimodal\\\":false,\\\"input_type\\\":\\\"\\\",\\\"truncate\\\":\\\"\\\"}\"}",
        },
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .enrichment,
            .object_name = "dense_v1",
            .table_name = "docs",
            .owner_metadata_json = "{\"name\":\"dense_v1\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":3}",
        },
    };

    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        planStorageMemberDeltaAlloc(std.testing.allocator, &snapshot, &.{}, new_members[0..]),
    );
}

test "extension lifecycle requires stable identity for executable artifact owners" {
    var tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .indexes_json = "{}",
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
    const new_members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .index,
            .object_name = "owner",
            .table_name = "docs",
            .owner_metadata_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedding_name\":\"dense_v1\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"model-a\"}}",
        },
        .{
            .extension_name = "semantic",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .enrichment,
            .object_name = "dense_v1",
            .table_name = "docs",
            .owner_metadata_json = "{\"name\":\"dense_v1\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":3}",
        },
    };

    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        planStorageMemberDeltaAlloc(std.testing.allocator, &snapshot, &.{}, new_members[0..]),
    );
}

test "extension lifecycle verification rejects a committed no-op and accepts the exact projection" {
    const installed = extension_domain.InstalledExtension{
        .name = "docaf",
        .package_name = "docaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:abc",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .status = .ready,
    };
    const delta = metadata_storage.ExtensionLifecycleDelta{
        .upsert_installed_extensions = &.{installed},
    };
    var missing = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    try std.testing.expect(!lifecycleDeltaApplied(&missing, delta));

    var projected = [_]extension_domain.InstalledExtension{installed};
    var exact = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .installed_extensions = projected[0..],
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    try std.testing.expect(lifecycleDeltaApplied(&exact, delta));

    projected[0].package_version = "2.0.0";
    try std.testing.expect(!lifecycleDeltaApplied(&exact, delta));

    const member = extension_domain.ExtensionMember{
        .extension_name = "docaf",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .object_kind = .index,
        .object_name = "search",
        .table_name = "docs",
    };
    const dependency = extension_domain.ExtensionDependency{
        .extension_name = "docaf",
        .required_extension_name = "core",
        .package_name = "core",
        .version_requirement = "^1",
    };
    const replacement_delta = metadata_storage.ExtensionLifecycleDelta{
        .upsert_extension_members = &.{member},
        .remove_extension_members = &.{.{
            .extension_name = member.extension_name,
            .object_kind = member.object_kind,
            .object_name = member.object_name,
        }},
        .upsert_extension_dependencies = &.{dependency},
        .remove_extension_dependencies = &.{.{
            .extension_name = dependency.extension_name,
            .required_extension_name = dependency.required_extension_name,
            .package_name = dependency.package_name,
        }},
    };
    var members = [_]extension_domain.ExtensionMember{member};
    var dependencies = [_]extension_domain.ExtensionDependency{dependency};
    var replaced = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .extension_members = members[0..],
        .extension_dependencies = dependencies[0..],
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    try std.testing.expect(lifecycleDeltaApplied(&replaced, replacement_delta));
}
