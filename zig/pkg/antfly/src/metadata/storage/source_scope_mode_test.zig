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

//! Persisted source intake closure and lifecycle work-counter qualification.
const std = @import("std");
const store_mod = @import("raft_apply_store.zig");
const Store = store_mod.RaftApplyStore;
const metadata = @import("../table_manager.zig");
const control = @import("../source_control.zig");
const groups = @import("antfly_local_sources").common_group_ids;
const A = std.mem.Allocator;
const Actor = struct {
    a: A,
    store: *Store,
    scope: control.Scope,
    catalog: control.Digest,
    fn read(self: Actor) !std.json.Parsed(control.State) {
        const bytes = try self.store.getSourceControl(self.a, groups.main_metadata_group_id, self.scope.table_id, self.scope.source_id);
        defer self.a.free(bytes);
        return control.parse(self.a, bytes);
    }
    fn apply(self: Actor, mutation: control.Mutation) !void {
        var state = try self.read();
        defer state.deinit();
        const bytes = try control.encodeWrite(self.a, .{ .table_id = self.scope.table_id, .source_id = self.scope.source_id, .expected_revision = state.value.revision, .expected_binding = self.scope.identity(), .expected_catalog_digest = self.catalog, .mutation = mutation });
        defer self.a.free(bytes);
        try self.store.applyStandaloneCommand(groups.main_metadata_group_id, .{ .apply_source_control = bytes });
    }
    fn expectWork(self: Actor, value: u64) !void {
        var txn = try self.store.store.beginReadTxn();
        defer txn.abort();
        var buffer: [256]u8 = undefined;
        const raw = try txn.get(try control.workKey(&buffer, groups.main_metadata_group_id, self.scope.table_id));
        try std.testing.expectEqual(value, std.mem.readInt(u64, raw[0..8], .little));
    }
};
pub fn run(a: A) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-scope-mode", .{tmp.sub_path});
    defer a.free(path);
    const target_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-scope-mode-target", .{tmp.sub_path});
    defer a.free(target_path);
    var store = try Store.init(a, .{ .root_dir = path });
    var opened = true;
    defer if (opened) store.deinit();
    const group = groups.main_metadata_group_id;
    const incarnation = "11111111111111111111111111111111".*;
    try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = incarnation });
    const table: metadata.TableRecord = .{ .table_id = 7, .name = "docs", .schema_json = "{}", .replication_sources_json = "[{\"type\":\"postgres\",\"postgres_table\":\"users\"}]" };
    try store.applyStandaloneCommand(group, .{ .upsert_table = table });
    const source: control.Scope = .{ .table_id = table.table_id, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = try control.definitionDigest(a, table.replication_sources_json, 0), .provider = @splat(2), .destination = .native, .destination_binding = @splat(3) };
    const actor: Actor = .{ .a = a, .store = &store, .scope = source, .catalog = control.digest(table.replication_sources_json) };
    try std.testing.expect(try store.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 0, source.definition));
    try actor.apply(.{ .configure = source });
    try std.testing.expect(!try store.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 0, source.definition));
    try std.testing.expect(!try store.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 1, source.definition));
    try std.testing.expect(try store.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 1, @splat(99)));

    const authority: control.checkpoint_authority.Spec = .{ .binding = source.identity(), .table_name = "checkpoint", .owner = .{ .metadata_group_id = group, .metadata_incarnation = incarnation, .namespace = @splat(1), .table_binding = @splat(4), .root_generation = 0, .group_id = 17, .topology_epoch = 1 } };
    try actor.apply(.{ .bind_checkpoint = authority });
    try actor.apply(.{ .enable_scope_fencing = {} });
    try actor.expectWork(1);
    try actor.apply(.{ .enable_scope_fencing = {} });
    try actor.expectWork(1);
    store.deinit();
    opened = false;
    store = try Store.init(a, .{ .root_dir = path });
    opened = true;
    var restored = try actor.read();
    defer restored.deinit();
    try std.testing.expect(restored.value.scope_fencing and !restored.value.closing);
    try actor.expectWork(1);
    try std.testing.expect(!try store.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 0, source.definition));
    try actor.apply(.{ .close = {} });
    try actor.expectWork(1);
    const snapshot = try store.snapshotBuilder().buildSnapshot(a, group);
    defer a.free(snapshot);
    var target = try Store.init(a, .{ .root_dir = target_path });
    defer target.deinit();
    try std.testing.expect(try target.snapshotBuilder().installSnapshot(a, group, 100, snapshot));
    const installed: Actor = .{ .a = a, .store = &target, .scope = source, .catalog = actor.catalog };
    var closing = try installed.read();
    defer closing.deinit();
    try std.testing.expect(closing.value.scope_fencing and closing.value.closing);
    try installed.expectWork(1);
    try std.testing.expectError(error.TableTransitionActive, target.captureTableDropProjection(a, group, "docs"));
    const fence = try target.getTableTransitionFence(group, source.table_id);
    try target.applyStandaloneCommand(group, .{ .apply_table_topology = .{ .drop = .{
        .table_id = source.table_id,
        .expected_name = "docs",
        .expected_transition_generation = fence.generation,
        .range_contract = .{ .membership = fence.membership(source.table_id) },
    } } });
    try target.applyStandaloneCommand(group, .{ .remove_table = .{ .table_id = source.table_id, .expected_transition_generation = 0 } });
    const retained_table = (try target.getTable(a, group, source.table_id)) orelse return error.TestUnexpectedTableRemoval;
    defer metadata.freeTable(a, retained_table);
    try installed.apply(.{ .retire = .{ .receipt = "native-cohort-closed" } });
    try installed.expectWork(1);
    try installed.apply(.{ .fence_checkpoint = .{ .authority_digest = authority.digest(), .proof = @splat(5) } });
    try installed.expectWork(0);
    try std.testing.expect(!try target.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 0, source.definition));
    // A bounded census must never turn an incomplete/corrupt scan into intake
    // permission. Exercise the real cursor and persisted actor prefix.
    const eligibility = @import("../source_legacy_intake.zig");
    {
        var txn = try target.store.beginWriteTxn();
        errdefer txn.abort();
        const empty = try control.encode(a, .{});
        defer a.free(empty);
        for (0..eligibility.max_records + 1) |index| {
            var id: control.Digest = @splat(0);
            std.mem.writeInt(u64, id[24..32], @intCast(index + 2), .big);
            var key_buffer: [256]u8 = undefined;
            try txn.put(try control.keyForGroup(&key_buffer, group, source.table_id, id), empty);
        }
        try txn.commit();
    }
    try std.testing.expectError(error.SourceAdmissionBackpressure, target.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 9, @splat(99)));
    {
        var txn = try target.store.beginWriteTxn();
        errdefer txn.abort();
        var key_buffer: [256]u8 = undefined;
        try txn.put(try control.keyForGroup(&key_buffer, group, source.table_id, @splat(0)), "!");
        try txn.commit();
    }
    try std.testing.expectError(error.SourceControlCorrupt, target.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 9, @splat(99)));
    {
        const oversized = try a.alloc(u8, eligibility.max_bytes + 1);
        defer a.free(oversized);
        @memset(oversized, ' ');
        var txn = try target.store.beginWriteTxn();
        errdefer txn.abort();
        var key_buffer: [256]u8 = undefined;
        try txn.put(try control.keyForGroup(&key_buffer, group, source.table_id, @splat(0)), oversized);
        try txn.commit();
    }
    try std.testing.expectError(error.SourceAdmissionBackpressure, target.legacyReplicationSourceIntakeAllowed(a, group, source.table_id, 9, @splat(99)));
}
