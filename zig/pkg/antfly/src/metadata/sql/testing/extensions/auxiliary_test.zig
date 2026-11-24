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

//! Real standalone auxiliary registry authority; no physical package rows.
const std = @import("std");
const native_store = @import("../../../storage/raft_apply_store.zig");
const Store = native_store.RaftApplyStore;
const lifecycle = @import("../../../../system_catalog/sql_extensions.zig");
const packages = @import("../../../../extensions/mod.zig");
const routines = @import("../../../../system_catalog/routines.zig");
const protocol = @import("../../../topology_protocol.zig");
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
const A = std.mem.Allocator;

fn package(name: []const u8, version: []const u8, digest: []const u8, bootstrap_default: []const u8) packages.PackageManifest {
    return .{
        .name = name,
        .version = version,
        .digest = digest,
        .relocatable = true,
        .artifacts = &.{.{ .kind = .wasm, .path = "module.wasm", .digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }},
        .install = .{ .scopes_supported = &.{.embedded_db}, .runtimes = &.{.{ .name = "sql", .mode = .wasm, .artifact = "module.wasm" }} },
        .sql = .{ .default_version = bootstrap_default, .exports = &.{.{
            .name = "control_marker",
            .runtime = "sql",
            .export_name = "control_marker",
            .arguments = &.{.{ .type = .{ .scalar = .int64 } }},
            .result = .{ .scalar = .int64 },
            .strict = true,
            .volatility = .immutable,
        }} },
    };
}

fn physicalPackageCount(store: *Store, a: A) !usize {
    var buffer: [128]u8 = undefined;
    return (try store.store.scanPrefix(a, try native_store.extensionPackagePrefixForGroup(&buffer, group))).len;
}

fn snapshot(store: *Store, a: A) !lifecycle.Snapshot {
    const request: lifecycle.SnapshotRequest = .{ .database = "default", .namespace = "public", .name = "vector" };
    const value = try std.json.parseFromSliceLeaky(lifecycle.Snapshot, a, try store.sqlExtensionSnapshot(a, group, request, .{}), .{});
    try value.validate(request);
    return value;
}
fn mutation(view: lifecycle.Snapshot, a: A, nonce: u8, operation: @FieldType(lifecycle.Mutation, "operation")) !lifecycle.Mutation {
    return .{ .admission = .{ .nonce = @splat(nonce), .head = view.head }, .incarnation = view.incarnation, .database = "default", .database_id = view.database_id, .registry_digest = try lifecycle.packageRegistryDigest(a, view.packages, view.controls), .name = "vector", .operation = operation };
}
fn apply(store: *Store, a: A, request: lifecycle.Mutation) !lifecycle.Identity {
    const command = lifecycle.command(request);
    const predicted = try std.json.parseFromSliceLeaky(lifecycle.Outcome, a, try store.prepareSystemCatalogResult(a, group, command), .{});
    try std.testing.expect(predicted == .success);
    try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, command, .{}) });
    const actual = (try std.json.parseFromSliceLeaky(?lifecycle.Outcome, a, try store.sqlExtensionOutcome(a, group, try lifecycle.observation(a, request)), .{})) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualDeep(predicted, actual);
    return actual.success.identity.?;
}
fn verify(store: *Store, a: A, expected: lifecycle.Identity, description: ?[]const u8) !void {
    const view = try snapshot(store, a);
    const installed = view.installed orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(expected.id, installed.record.id);
    try std.testing.expectEqual(expected.generation, installed.record.generation);
    try std.testing.expectEqualStrings("2", installed.record.package_version);
    try std.testing.expectEqual(.auth_disabled, std.meta.activeTag(installed.record.owner));
    if (description) |text| try std.testing.expectEqualStrings(text, installed.description.?) else try std.testing.expect(installed.description == null);
    try std.testing.expectEqual(@as(usize, 1), installed.record.members.len);
    const callable = try std.json.parseFromSliceLeaky(routines.Snapshot, a, try store.sqlRoutineSnapshot(a, group, .{ .targets = &.{.{ .table = "control_marker" }}, .arguments = &.{.{ .type = .{ .scalar = .int64 } }} }, .{}), .{});
    const record = callable.found.?.record;
    try std.testing.expectEqual(installed.record.members[0].id, record.id);
    try std.testing.expectEqual(installed.record.members[0].generation, record.generation);
    try std.testing.expectEqualDeep(installed.record.owner, record.definition.owner);
    try std.testing.expectEqual(.wasm, record.definition.language);
    try std.testing.expectEqualStrings("2", record.definition.wasm.?.package_version);
    try std.testing.expectEqual(@as(usize, 0), try physicalPackageCount(store, a));
}

test "SQL extension standalone auxiliary registry owns actual native creation comments and cold membership" {
    const backing = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/extension-auxiliary", .{tmp.sub_path});
    var identity: lifecycle.Identity = undefined;
    for (0..3) |pass| {
        var store = try Store.init(backing, .{ .root_dir = root });
        defer store.deinit();
        if (pass == 0) {
            var catalog = packages.ExtensionCatalog.init(backing);
            defer catalog.deinit();
            const one = package("vector", "1", "1111111111111111111111111111111111111111111111111111111111111111", "1");
            const two = package("vector", "2", "2222222222222222222222222222222222222222222222222222222222222222", "2");
            try catalog.registerPackage(one);
            try catalog.registerPackage(two);
            const initial = catalog.sql_controls.items[0];
            try catalog.promoteSqlDefault(.{ .name = "vector", .identity = initial.identity, .expected_generation = initial.generation, .version = "2", .package_digest = try packages.sql_package.digest(two.digest), .nonce = @splat(9) });
            const aux = try std.json.Stringify.valueAlloc(a, .{ .extension_packages = catalog.packages.items, .sql_extension_controls = catalog.sql_controls.items }, .{});
            const incarnation = "01010101010101010101010101010101".*;
            try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = incarnation });
            try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = protocol.current_version, .incarnation = incarnation, .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
            const bootstrap_revision = try store.standaloneRevision();
            try store.replaceStandaloneCatalog(group, bootstrap_revision, &.{}, &.{}, aux);
            const before = try snapshot(&store, a);
            try std.testing.expect(before.installed == null);
            try std.testing.expectEqual(@as(usize, 2), before.packages.len);
            try std.testing.expectEqual(@as(usize, 1), before.controls.len);
            const registry: packages.sql_package.Registry = .{ .packages = before.packages, .controls = before.controls, .control_receipts = false };
            try std.testing.expectEqualStrings("2", (try registry.select("vector", null)).version);
            try std.testing.expectEqualStrings("1", (try registry.select("vector", "1")).version);
            try std.testing.expectEqual(@as(usize, 0), try physicalPackageCount(&store, a));

            const original_revision = try store.standaloneRevision();
            const meta = try store.systemCatalogMeta(a, group);
            const stored_control = (try store.getSqlExtensionControl(a, group, "vector")).?;
            try std.testing.expectEqualDeep(catalog.sql_controls.items[0], stored_control);
            const stored_controls = try store.listSqlExtensionControls(a, group);
            try std.testing.expectEqual(@as(usize, 1), stored_controls.len);
            try std.testing.expectEqualDeep(stored_control, stored_controls[0]);
            // The standalone auxiliary owns this registry. A physical native
            // producer cannot create a second registry or shadow its default.
            const shadow = package("vector", "3", "3333333333333333333333333333333333333333333333333333333333333333", "3");
            try std.testing.expectError(error.UnsupportedOperation, store.prepareExtensionPackageRegistration(a, group, shadow));
            try store.applyStandaloneCommand(group, .{ .upsert_extension_package = shadow });
            const shadow_promotion: packages.sql_package.Promotion = .{ .name = "vector", .identity = stored_control.identity, .expected_generation = stored_control.generation, .version = "1", .package_digest = try packages.sql_package.digest(one.digest), .nonce = @splat(10) };
            try std.testing.expectError(error.UnsupportedOperation, store.prepareSqlExtensionDefault(a, group, shadow_promotion));
            try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = shadow_promotion });
            try store.applyStandaloneCommand(group, .{ .remove_extension_package = .{ .name = "vector", .version = "1" } });
            try std.testing.expectEqualDeep(stored_control, (try store.getSqlExtensionControl(a, group, "vector")).?);
            try std.testing.expectEqual(@as(usize, 0), try physicalPackageCount(&store, a));
            const revision = try store.standaloneRevision();
            try std.testing.expectEqual(original_revision + 3, revision);
            try std.testing.expectEqualDeep(meta, try store.systemCatalogMeta(a, group));
            try std.testing.expectEqualDeep(before, try snapshot(&store, a));

            const missing = try std.json.Stringify.valueAlloc(a, .{ .extension_packages = catalog.packages.items }, .{});
            try std.testing.expectError(error.InvalidSqlExtensionControl, store.replaceStandaloneCatalog(group, revision, &.{}, &.{}, missing));
            var bad = catalog.sql_controls.items[0];
            bad.identity[0] ^= 1;
            const malformed = try std.json.Stringify.valueAlloc(a, .{ .extension_packages = catalog.packages.items, .sql_extension_controls = &.{bad} }, .{});
            try std.testing.expectError(error.InvalidSqlExtensionControl, store.replaceStandaloneCatalog(group, revision, &.{}, &.{}, malformed));
            try std.testing.expectEqual(revision, try store.standaloneRevision());
            try std.testing.expectEqualDeep(meta, try store.systemCatalogMeta(a, group));
            const unchanged = try snapshot(&store, a);
            try std.testing.expectEqualDeep(before, unchanged);
            const request = try mutation(unchanged, a, 1, .{ .install = .{ .namespace = "public", .schema_explicit = false, .version = null, .if_not_exists = false, .cascade = false, .owner = .auth_disabled } });
            identity = try apply(&store, a, request);
            try verify(&store, a, identity, null);
            identity = try apply(&store, a, try mutation(try snapshot(&store, a), a, 2, .{ .comment = .{ .expected = identity, .description = "standalone owned comment" } }));
            try verify(&store, a, identity, "standalone owned comment");
        } else if (pass == 1) {
            try verify(&store, a, identity, "standalone owned comment");
            identity = try apply(&store, a, try mutation(try snapshot(&store, a), a, 3, .{ .comment = .{ .expected = identity, .description = null } }));
            try verify(&store, a, identity, null);
        } else {
            try verify(&store, a, identity, null);
        }
    }
}
