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

//! Native package-control custody, independent of callable execution.
const std = @import("std");
const Store = @import("../../../storage/raft_apply_store.zig").RaftApplyStore;
const lifecycle = @import("../../../../system_catalog/sql_extensions.zig");
const packages = @import("../../../../extensions/mod.zig");
const sql_package = packages.sql_package;
const protocol = @import("../../../topology_protocol.zig");
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
const A = std.mem.Allocator;

fn initialize(store: *Store, a: A) !void {
    const incarnation = "01010101010101010101010101010101".*;
    try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = incarnation });
    try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, protocol.Activation{
        .version = protocol.current_version,
        .incarnation = incarnation,
        .member_count = 1,
        .membership_fingerprint = @splat(7),
    }, .{}) });
}

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

fn snapshot(store: *Store, a: A, name: []const u8) !lifecycle.Snapshot {
    const bytes = try store.sqlExtensionSnapshot(a, group, .{ .database = "default", .namespace = "public", .name = name }, .{});
    return std.json.parseFromSliceLeaky(lifecycle.Snapshot, a, bytes, .{});
}

fn control(store: *Store, a: A, name: []const u8) !sql_package.PackageControl {
    return (try store.getSqlExtensionControl(a, group, name)) orelse error.TestUnexpectedResult;
}

fn register(store: *Store, a: A, value: packages.PackageManifest) !void {
    try value.validate();
    try store.prepareExtensionPackageRegistration(a, group, value);
    try store.applyStandaloneCommand(group, .{ .upsert_extension_package = value });
}

fn promote(store: *Store, a: A, value: sql_package.Promotion) !void {
    const predicted = try store.prepareSqlExtensionDefault(a, group, value);
    try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = value });
    try std.testing.expectEqualDeep(predicted, try receipt(try control(store, a, value.name), value));
}

fn promotion(current: sql_package.PackageControl, version: []const u8, digest: []const u8, nonce: u8) !sql_package.Promotion {
    return .{
        .name = current.name,
        .identity = current.identity,
        .expected_generation = current.generation,
        .version = version,
        .package_digest = try sql_package.digest(digest),
        .nonce = @splat(nonce),
    };
}

fn receipt(current: sql_package.PackageControl, request: sql_package.Promotion) !sql_package.PromotionReceipt {
    for (current.promotions) |value| if (std.mem.eql(u8, &value.nonce, &request.nonce)) return value;
    return error.TestUnexpectedResult;
}

fn expectControl(store: *Store, a: A, expected: sql_package.PackageControl) !void {
    try std.testing.expectEqualDeep(expected, try control(store, a, expected.name));
}

fn packageBytes(store: *Store, a: A) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, try store.listExtensionPackages(a, group), .{});
}

fn selected(store: *Store, a: A, name: []const u8, version: ?[]const u8) !packages.PackageManifest {
    const view = try snapshot(store, a, name);
    const registry: sql_package.Registry = .{ .packages = view.packages, .controls = view.controls, .control_receipts = false };
    try registry.validateControls();
    return registry.select(name, version);
}

test "SQL extension native package controls preserve explicit defaults and retained promotions across cold backup" {
    const backing = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/package-controls", .{tmp.sub_path});
    const checkpoint_root = try std.fmt.allocPrint(a, "{s}-checkpoint", .{root});
    const replica_root = try std.fmt.allocPrint(a, "{s}-snapshot", .{root});
    const checkpoint_path = try std.fmt.allocPrint(a, "{s}/controls.chk", .{root});
    const one = package("vector", "1", "1111111111111111111111111111111111111111111111111111111111111111", "1");
    // Deliberately different immutable bootstrap hints do not constitute
    // competing package-level defaults after the first control exists.
    const two = package("vector", "2", "2222222222222222222222222222222222222222222222222222222222222222", "2");
    const three = package("vector", "3", "3333333333333333333333333333333333333333333333333333333333333333", "3");
    const pending_one = package("pending", "1", "4444444444444444444444444444444444444444444444444444444444444444", "2");
    const pending_two = package("pending", "2", "5555555555555555555555555555555555555555555555555555555555555555", "2");
    const future = package("future", "1", "6666666666666666666666666666666666666666666666666666666666666666", "unavailable");
    var expected_pending: sql_package.PackageControl = undefined;
    var expected: sql_package.PackageControl = undefined;
    var old_request: sql_package.Promotion = undefined;
    var old_receipt: sql_package.PromotionReceipt = undefined;
    var final_receipt: sql_package.PromotionReceipt = undefined;
    var final_request: sql_package.Promotion = undefined;
    var snapshot_bytes: []const u8 = undefined;
    var checkpoint: @import("../../../../storage/hot_standby/metadata_effects.zig").CheckpointArtifact = undefined;
    var expected_catalog: []const u8 = undefined;
    {
        var store = try Store.init(backing, .{ .root_dir = root });
        defer store.deinit();
        try initialize(&store, a);
        try register(&store, a, one);
        const initial = try control(&store, a, "vector");
        try initial.validate();
        try std.testing.expectEqualDeep(sql_package.controlIdentity("vector"), initial.identity);
        try std.testing.expectEqualStrings("1", initial.default_version);
        try std.testing.expectEqualDeep(try sql_package.digest(one.digest), initial.default_digest.?);
        try register(&store, a, one);
        try expectControl(&store, a, initial);
        const immutable_before = try packageBytes(&store, a);
        var changed = one;
        changed.digest = two.digest;
        try std.testing.expectError(error.InvalidSqlExtensionPackageMutation, register(&store, a, changed));
        try store.applyStandaloneCommand(group, .{ .upsert_extension_package = changed });
        try expectControl(&store, a, initial);
        try std.testing.expectEqualStrings(immutable_before, try packageBytes(&store, a));
        changed = one;
        changed.description = "different immutable declaration";
        try std.testing.expectError(error.InvalidSqlExtensionPackageMutation, register(&store, a, changed));
        try store.applyStandaloneCommand(group, .{ .upsert_extension_package = changed });
        try expectControl(&store, a, initial);
        try std.testing.expectEqualStrings(immutable_before, try packageBytes(&store, a));
        changed = one;
        changed.sql = null;
        try std.testing.expectError(error.InvalidSqlExtensionPackageMutation, register(&store, a, changed));
        try store.applyStandaloneCommand(group, .{ .upsert_extension_package = changed });
        try expectControl(&store, a, initial);
        try std.testing.expectEqualStrings(immutable_before, try packageBytes(&store, a));
        try store.applyStandaloneCommand(group, .{ .remove_extension_package = .{ .name = "vector", .version = "1" } });
        try expectControl(&store, a, initial);
        try std.testing.expectEqualDeep(one, try selected(&store, a, "vector", "1"));
        try std.testing.expectEqualStrings(immutable_before, try packageBytes(&store, a));

        // A valid pending package does not poison unrelated registry discovery.
        try register(&store, a, pending_one);
        const pending = try control(&store, a, "pending");
        try std.testing.expect(pending.default_digest == null);
        try std.testing.expectEqualStrings("1", (try selected(&store, a, "vector", null)).version);
        try std.testing.expectEqualStrings("1", (try selected(&store, a, "pending", "1")).version);
        try std.testing.expectError(error.SqlExtensionVersionUnavailable, selected(&store, a, "pending", null));
        try register(&store, a, pending_two);
        const ready = try control(&store, a, "pending");
        try std.testing.expectEqualDeep(pending.identity, ready.identity);
        try std.testing.expectEqual(pending.generation + 1, ready.generation);
        try std.testing.expectEqualStrings("2", (try selected(&store, a, "pending", null)).version);
        try register(&store, a, pending_one);
        try expectControl(&store, a, ready);

        try register(&store, a, two);
        try register(&store, a, three);
        try register(&store, a, one);
        try expectControl(&store, a, initial);
        try std.testing.expectEqualStrings("1", (try selected(&store, a, "vector", null)).version);
        try std.testing.expectEqualStrings("2", (try selected(&store, a, "vector", "2")).version);
        old_request = try promotion(initial, "2", two.digest, 1);
        try promote(&store, a, old_request);
        const promoted = try control(&store, a, "vector");
        try std.testing.expectEqual(initial.generation + 1, promoted.generation);
        try std.testing.expectEqualStrings("2", promoted.default_version);
        old_receipt = try receipt(promoted, old_request);
        try std.testing.expectEqualDeep(try old_request.digest(a), old_receipt.request_digest);
        try std.testing.expectEqual(promoted.generation, old_receipt.generation);
        try promote(&store, a, old_request);
        try expectControl(&store, a, promoted);

        const promotion_versions_before = try packageBytes(&store, a);
        var invalid = try promotion(initial, "3", three.digest, 2);
        try std.testing.expectError(error.SqlExtensionControlConflict, promote(&store, a, invalid));
        try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = invalid });
        try expectControl(&store, a, promoted);
        try std.testing.expectEqualStrings(promotion_versions_before, try packageBytes(&store, a));
        invalid = try promotion(promoted, "3", three.digest, 2);
        invalid.identity[0] ^= 1;
        try std.testing.expectError(error.InvalidSqlExtensionControl, promote(&store, a, invalid));
        try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = invalid });
        try expectControl(&store, a, promoted);
        try std.testing.expectEqualStrings(promotion_versions_before, try packageBytes(&store, a));
        invalid = try promotion(promoted, "missing", three.digest, 2);
        try std.testing.expectError(error.SqlExtensionVersionUnavailable, promote(&store, a, invalid));
        try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = invalid });
        try expectControl(&store, a, promoted);
        try std.testing.expectEqualStrings(promotion_versions_before, try packageBytes(&store, a));
        invalid = try promotion(promoted, "3", two.digest, 2);
        try std.testing.expectError(error.InvalidPackageDigest, promote(&store, a, invalid));
        try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = invalid });
        try expectControl(&store, a, promoted);
        try std.testing.expectEqualStrings(promotion_versions_before, try packageBytes(&store, a));
        invalid = old_request;
        invalid.version = "3";
        invalid.package_digest = try sql_package.digest(three.digest);
        try std.testing.expectError(error.SqlExtensionPromotionNonceConflict, promote(&store, a, invalid));
        try store.applyStandaloneCommand(group, .{ .promote_sql_extension_default = invalid });
        try expectControl(&store, a, promoted);
        try std.testing.expectEqualStrings(promotion_versions_before, try packageBytes(&store, a));
        try expectControl(&store, a, promoted);

        try std.testing.expectEqualStrings(promotion_versions_before, try packageBytes(&store, a));
        final_request = try promotion(promoted, "3", three.digest, 3);
        try promote(&store, a, final_request);
        expected = try control(&store, a, "vector");
        final_receipt = try receipt(expected, final_request);
        try std.testing.expectEqual(promoted.generation + 1, expected.generation);
        try std.testing.expectEqualStrings("3", expected.default_version);
        // Lost old reply is observed from retained custody before the CAS;
        // replay cannot rewind the newer successfully selected default.
        try promote(&store, a, old_request);
        try expectControl(&store, a, expected);
        try std.testing.expectEqualDeep(old_receipt, try receipt(try control(&store, a, "vector"), old_request));
        try std.testing.expectEqualStrings("3", (try selected(&store, a, "vector", null)).version);
        try register(&store, a, future);
        expected_pending = try control(&store, a, "future");
        try std.testing.expect(expected_pending.default_digest == null);
        snapshot_bytes = try store.snapshotBuilder().buildSnapshot(a, group);
        const captured = try store.captureHotStandbyCatalogCheckpoint(a, store.io_impl.io(), group, checkpoint_path);
        defer captured.deinit(a);
        checkpoint = captured.checkpoint;
        expected_catalog = try a.dupe(u8, captured.catalog_json);
    }
    // Ordinary restart and both real backup owners must retain the same
    // control pointer and nonce results, not reconstruct from version order.
    for (0..2) |pass| {
        var reopened = try Store.init(backing, .{ .root_dir = root });
        defer reopened.deinit();
        try expectControl(&reopened, a, expected);
        try expectControl(&reopened, a, expected_pending);
        try promote(&reopened, a, old_request);
        try promote(&reopened, a, final_request);
        try expectControl(&reopened, a, expected);
        try std.testing.expectEqualDeep(old_receipt, try receipt(try control(&reopened, a, "vector"), old_request));
        try std.testing.expectEqualDeep(final_receipt, try receipt(try control(&reopened, a, "vector"), final_request));
        if (pass == 0) {
            var replica = try Store.init(backing, .{ .root_dir = replica_root });
            defer replica.deinit();
            try std.testing.expect(try replica.snapshotBuilder().installSnapshot(a, group, 100, snapshot_bytes));
            try expectControl(&replica, a, expected);
            try expectControl(&replica, a, expected_pending);
            var restored = try Store.init(backing, .{ .root_dir = checkpoint_root });
            defer restored.deinit();
            try restored.importHotStandbyCheckpoint(reopened.io_impl.io(), checkpoint_path, checkpoint.size_bytes);
            try expectControl(&restored, a, expected);
            try expectControl(&restored, a, expected_pending);
            const exported = try restored.projectHotStandbyCheckpointCatalog(a, group, checkpoint);
            try std.testing.expectEqualStrings(expected_catalog, exported);
        }
    }
    for ([_][]const u8{ replica_root, checkpoint_root }) |restored_root| {
        var restored = try Store.init(backing, .{ .root_dir = restored_root });
        defer restored.deinit();
        try expectControl(&restored, a, expected);
        try promote(&restored, a, old_request);
        try promote(&restored, a, final_request);
        try expectControl(&restored, a, expected);
        try std.testing.expectEqualDeep(old_receipt, try receipt(try control(&restored, a, "vector"), old_request));
        try std.testing.expectEqualDeep(final_receipt, try receipt(try control(&restored, a, "vector"), final_request));
        try expectControl(&restored, a, expected_pending);
        try std.testing.expectError(error.SqlExtensionVersionUnavailable, selected(&restored, a, "future", null));
        try std.testing.expectEqualStrings("3", (try selected(&restored, a, "vector", null)).version);
        try std.testing.expectEqualStrings("2", (try selected(&restored, a, "pending", null)).version);
        try std.testing.expectEqualStrings("1", (try selected(&restored, a, "vector", "1")).version);
    }
}
