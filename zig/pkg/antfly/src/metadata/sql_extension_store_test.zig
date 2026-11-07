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

//! Actual native journal atomicity. Public sibling executes real WASM members.
const std = @import("std");
const Store = @import("storage/raft_apply_store.zig").RaftApplyStore;
const lifecycle = @import("../system_catalog/sql_extensions.zig");
const packages = @import("../extensions/mod.zig");
const routines = @import("../system_catalog/routines.zig");
const protocol = @import("topology_protocol.zig");
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
fn snapshot(store: *Store, a: std.mem.Allocator) !lifecycle.Snapshot {
    const bytes = try store.sqlExtensionSnapshot(a, group, .{ .database = "default", .namespace = "public", .name = "vector" }, .{});
    return std.json.parseFromSliceLeaky(lifecycle.Snapshot, a, bytes, .{});
}
fn function(store: *Store, a: std.mem.Allocator) !routines.Snapshot {
    const bytes = try store.sqlRoutineSnapshot(a, group, .{ .targets = &.{.{ .table = "vector_version_marker" }}, .arguments = &.{.{ .type = .{ .scalar = .int64 } }} }, .{});
    return std.json.parseFromSliceLeaky(routines.Snapshot, a, bytes, .{});
}
fn outcome(store: *Store, a: std.mem.Allocator, request: lifecycle.Mutation) !?lifecycle.Outcome {
    return std.json.parseFromSliceLeaky(?lifecycle.Outcome, a, try store.sqlExtensionOutcome(a, group, try lifecycle.observation(a, request)), .{});
}

test "SQL extension native outcomes abort staged effects and preserve exact membership across cold reopen" {
    const backing = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/extension-journal", .{tmp.sub_path});
    const module = try @import("../extensions/wasmtime_runtime.zig").test_support.markerModule(a, 1);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(module, &digest, .{});
    const digest_text = try std.fmt.allocPrint(a, "sha256:{s}", .{std.fmt.bytesToHex(digest, .lower)});
    const package: packages.PackageManifest = .{
        .name = "vector",
        .version = "1.0.0",
        .digest = digest_text,
        .relocatable = true,
        .artifacts = &.{.{ .kind = .wasm, .path = "module.wasm", .digest = digest_text }},
        .install = .{ .scopes_supported = &.{.embedded_db}, .runtimes = &.{.{ .name = "sql", .mode = .wasm, .artifact = "module.wasm" }} },
        .sql = .{ .default_version = "1.0.0", .exports = &.{.{ .name = "vector_version_marker", .runtime = "sql", .export_name = "vector_version_marker", .arguments = &.{.{ .type = .{ .scalar = .int64 } }}, .result = .{ .scalar = .int64 }, .strict = true, .volatility = .immutable }} },
    };
    try package.validate();
    var first: lifecycle.Mutation = undefined;
    var expected_meta: @import("antfly_local_sources").system_catalog_domain.Meta = undefined;
    var identity: lifecycle.Identity = undefined;
    // Actual failed native transaction, then cold replay of the same immutable
    // nonce/body, then another cold read of its retained successful outcome.
    for (0..3) |pass| {
        var store = try Store.init(backing, .{ .root_dir = path });
        defer store.deinit();
        if (pass == 0) {
            const incarnation = "01010101010101010101010101010101".*;
            try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = incarnation });
            try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = protocol.current_version, .incarnation = incarnation, .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
            try store.applyStandaloneCommand(group, .{ .upsert_extension_package = package });
            const before = try snapshot(&store, a);
            try std.testing.expect(before.installed == null);
            first = .{ .admission = .{ .nonce = @splat(1), .head = before.head }, .incarnation = before.incarnation, .database = "default", .database_id = before.database_id, .registry_digest = try lifecycle.packageRegistryDigest(a, before.packages, before.controls), .name = "vector", .operation = .{ .install = .{ .namespace = "public", .schema_explicit = false, .version = null, .if_not_exists = false, .cascade = false, .owner = .auth_disabled } } };
            const command = lifecycle.command(first);
            const prepared = try store.prepareSystemCatalogResult(a, group, command);
            const preview = try std.json.parseFromSliceLeaky(lifecycle.Outcome, a, prepared, .{});
            try std.testing.expect(preview == .success);
            expected_meta = try store.systemCatalogMeta(a, group);
            store.sql_catalog_post_effect_error = error.CatalogGenerationChanged;
            try std.testing.expectError(error.SqlExtensionEffectFailure, store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, command, .{}) }));
            try std.testing.expectEqualDeep(expected_meta, try store.systemCatalogMeta(a, group));
            const after = try snapshot(&store, a);
            try std.testing.expect(after.installed == null);
            try std.testing.expectEqual(before.head, after.head);
            try std.testing.expect((try function(&store, a)).found == null);
            try std.testing.expect((try outcome(&store, a, first)) == null);
        } else if (pass == 1) {
            const absent = try snapshot(&store, a);
            try std.testing.expect(absent.installed == null);
            try std.testing.expectEqualDeep(expected_meta, try store.systemCatalogMeta(a, group));
            try std.testing.expect((try function(&store, a)).found == null);
            try std.testing.expect((try outcome(&store, a, first)) == null);
            const bytes = try std.json.Stringify.valueAlloc(a, lifecycle.command(first), .{});
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = bytes });
            const accepted = (try outcome(&store, a, first)) orelse return error.TestUnexpectedResult;
            try std.testing.expect(accepted == .success);
            identity = accepted.success.identity.?;
            const installed = (try snapshot(&store, a)).installed.?;
            try std.testing.expectEqual(identity.id, installed.record.id);
            try std.testing.expectEqual(identity.generation, installed.record.generation);
            try std.testing.expectEqual(@as(usize, 1), installed.record.members.len);
            const callable = (try function(&store, a)).found.?.record;
            try std.testing.expectEqual(installed.record.members[0].id, callable.id);
            try std.testing.expectEqual(installed.record.members[0].generation, callable.generation);
            try std.testing.expectEqual(.wasm, callable.definition.language);
            try std.testing.expectEqualDeep(digest, callable.definition.wasm.?.artifact_digest);
            expected_meta = try store.systemCatalogMeta(a, group);
            // Accepted delivery with lost reply repeats no catalog/member effect.
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = bytes });
            try std.testing.expectEqualDeep(expected_meta, try store.systemCatalogMeta(a, group));
            try std.testing.expectEqualDeep(accepted, (try outcome(&store, a, first)).?);
            var substituted = try lifecycle.observation(a, first);
            substituted.digest[0] ^= 1;
            try std.testing.expectError(error.SequenceAdmissionConflict, store.sqlExtensionOutcome(a, group, substituted));
        } else {
            try std.testing.expectEqualDeep(expected_meta, try store.systemCatalogMeta(a, group));
            const installed = (try snapshot(&store, a)).installed.?;
            try std.testing.expectEqual(identity.id, installed.record.id);
            try std.testing.expectEqual(identity.generation, installed.record.generation);
            try std.testing.expectEqualDeep(digest, installed.record.package_digest);
            try std.testing.expectEqual(@as(usize, 1), installed.record.members.len);
            try std.testing.expectEqual(installed.record.members[0].id, (try function(&store, a)).found.?.record.id);
            const retained = (try outcome(&store, a, first)).?;
            try std.testing.expect(retained == .success);
            try std.testing.expectEqualDeep(identity, retained.success.identity.?);
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, lifecycle.command(first), .{}) });
            try std.testing.expectEqualDeep(expected_meta, try store.systemCatalogMeta(a, group));
        }
    }
}
