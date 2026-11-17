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

//! Metadata capture uses real committed-entry projection and actual cuts.
const std = @import("std");
const store_mod = @import("raft_apply_store.zig");
const Store = store_mod.RaftApplyStore;
const control = @import("../source_control.zig");
const capture = @import("../source_scope_capture.zig");
const topology = @import("../topology_protocol.zig");
const groups = @import("antfly_local_sources").common_group_ids;
const raft = @import("../../raft/state_machine/mod.zig");
const A = std.mem.Allocator;
const Driver = struct {
    a: A,
    store: *Store,
    index: u64 = 0,
    scope: control.Scope,
    catalog: control.Digest,
    fn apply(self: *Driver, command: store_mod.TransitionCommand) !void {
        const data = try store_mod.encodeTransitionCommand(self.a, command);
        defer self.a.free(data);
        const index = self.index + 1;
        const entries = try raft.encodeCommittedEntries(self.a, &.{.{ .term = 1, .index = index, .entry_type = .normal, .data = data }});
        defer self.a.free(entries);
        try self.store.snapshotBuilder().applyBatch(.{ .group_id = groups.main_metadata_group_id, .commit_index = index, .entries_bytes = entries });
        self.index = index;
    }
    fn mutate(self: *Driver, mutation: control.Mutation) !void {
        const raw = try self.store.getSourceControl(self.a, groups.main_metadata_group_id, self.scope.table_id, self.scope.source_id);
        defer self.a.free(raw);
        var state = try control.parse(self.a, raw);
        defer state.deinit();
        const bytes = try control.encodeWrite(self.a, .{ .table_id = self.scope.table_id, .source_id = self.scope.source_id, .expected_revision = state.value.revision, .expected_binding = self.scope.identity(), .expected_catalog_digest = self.catalog, .mutation = mutation });
        defer self.a.free(bytes);
        try self.apply(.{ .apply_source_control = bytes });
    }
};
pub fn run(a: A) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-scope-capture", .{tmp.sub_path});
    defer a.free(path);
    const target_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-scope-capture-target", .{tmp.sub_path});
    defer a.free(target_path);
    var store = try Store.init(a, .{ .root_dir = path });
    defer store.deinit();
    const group = groups.main_metadata_group_id;
    const incarnation = "11111111111111111111111111111111".*;
    const sources = "[{\"type\":\"postgres\",\"postgres_table\":\"users\"}]";
    const source: control.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = try control.definitionDigest(a, sources, 0), .provider = @splat(2), .destination = .native, .destination_binding = @splat(3) };
    var driver: Driver = .{ .a = a, .store = &store, .scope = source, .catalog = control.digest(sources) };
    const query: capture.Query = .{ .metadata_group_id = group, .metadata_incarnation = incarnation, .actor_table_id = source.table_id, .source_id = source.source_id, .minimum_applied_index = 1 };
    try std.testing.expectError(error.ReadUnavailable, store.captureSourceScope(a, query));
    try driver.apply(.{ .initialize_metadata_incarnation = incarnation });
    try driver.apply(.{ .upsert_table = .{ .table_id = 7, .name = "docs", .schema_json = "{}", .replication_sources_json = sources } });
    try driver.mutate(.{ .configure = source });
    const authority: control.checkpoint_authority.Spec = .{ .binding = source.identity(), .table_name = "checkpoint", .owner = .{ .metadata_group_id = group, .metadata_incarnation = incarnation, .namespace = @splat(1), .table_binding = @splat(4), .root_generation = 0, .group_id = 17, .topology_epoch = 1 } };
    try driver.mutate(.{ .bind_checkpoint = authority });
    try driver.mutate(.{ .enable_scope_fencing = {} });
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, store.captureSourceScope(a, query));
    const activation = try std.json.Stringify.valueAlloc(a, topology.Activation{ .version = control.closing_protocol_version, .incarnation = incarnation, .member_count = 1, .membership_fingerprint = @splat(5) }, .{});
    defer a.free(activation);
    try driver.apply(.{ .activate_topology_protocol = activation });
    var active = try store.captureSourceScope(a, query);
    defer active.deinit(a);
    try active.requireBind();
    try std.testing.expectEqual(driver.index, active.cut.index);
    try std.testing.expectEqual(capture.Phase.active, active.phase);
    try std.testing.expectEqualStrings("checkpoint", active.checkpoint_table);
    try std.testing.expectEqual(source.identity(), active.spec.binding);
    var changed = query;
    changed.minimum_applied_index = driver.index + 1;
    try std.testing.expectError(error.ReadUnavailable, store.captureSourceScope(a, changed));
    changed = query;
    changed.metadata_incarnation[0] = '2';
    try std.testing.expectError(error.InvalidMetadataIncarnation, store.captureSourceScope(a, changed));
    changed = query;
    changed.source_id[0] ^= 1;
    try std.testing.expectError(error.SourceBindingConflict, store.captureSourceScope(a, changed));
    var attempt: [16]u8 = @splat(6);
    std.mem.writeInt(u64, attempt[0..8], 1, .big);
    const pending: control.Reservation = .{ .sequence = 1, .batch_id = "original", .checkpoint = "one", .accepted_at_ns = 10, .payload_digest = @splat(7), .plan_digest = @splat(8), .transaction_id = @splat(9), .destination = .native, .delivery_manifest = "original", .artifact = .{ .namespace = @splat(10), .binding = source.identity(), .domain = control.artifact_control.domainFor(source.identity()), .attempt = attempt, .bytes = 128, .checksum = @splat(11), .plan_digest = @splat(12) } };
    try driver.mutate(.{ .reserve = pending });
    try driver.mutate(.{ .close = {} });
    var closing = try store.captureSourceScope(a, query);
    defer closing.deinit(a);
    try std.testing.expectEqual(driver.index, closing.cut.index);
    try std.testing.expect(closing.cut.index > active.cut.index);
    try std.testing.expectEqual(capture.Phase.closing, closing.phase);
    try closing.requireBinding(active.spec);
    try closing.requireClose();
    try std.testing.expectError(error.SourceBindingConflict, closing.requireBind());
    try std.testing.expectError(error.SourcePendingWork, closing.requireDrop());
    try std.testing.expectEqual(pending.transaction_id, closing.pending.?.transaction_id);
    try std.testing.expectEqual(try pending.inputFingerprint(source), closing.pending.?.purpose);
    try std.testing.expectEqual(@as(u64, 1), closing.pending.?.reference.token);
    try std.testing.expectEqual(pending.artifact.?.plan_digest, closing.pending.?.artifact_plan_digest);
    const rejection = "original-fenced";
    try driver.mutate(.{ .retire_artifact = .{ .reservation_digest = pending.identity(source), .proof = control.digest(rejection), .reason = .unsealed } });
    try driver.mutate(.{ .reclaim_artifact = .{ .reservation_digest = pending.identity(source), .proof = @splat(13) } });
    try driver.mutate(.{ .reject_and_retire = .{ .reservation_digest = pending.identity(source), .receipt = rejection } });
    var retired = try store.captureSourceScope(a, query);
    defer retired.deinit(a);
    try retired.requireDrop();
    try std.testing.expectEqual(capture.Phase.retired, retired.phase);
    try std.testing.expect(retired.pending == null);
    const snapshot = try store.snapshotBuilder().buildSnapshot(a, group);
    defer a.free(snapshot);
    var target = try Store.init(a, .{ .root_dir = target_path });
    defer target.deinit();
    try std.testing.expect(try target.snapshotBuilder().installSnapshot(a, group, driver.index, snapshot));
    var restored = try target.captureSourceScope(a, query);
    defer restored.deinit(a);
    try std.testing.expectEqualDeep(retired.spec, restored.spec);
    try std.testing.expectEqualDeep(retired.cut, restored.cut);
    try std.testing.expectEqual(retired.phase, restored.phase);
    const Context = struct {
        cancelled: *bool,
        pub fn ensureActive(self: @This()) !void {
            if (self.cancelled.*) return error.Canceled;
        }
    };
    const Service = struct {
        metadata_group_id: u64,
        store: *Store,
        cancelled: *bool,
        reads: usize = 0,
        cancel_at_projection: bool = false,
        pub fn ensureLinearizableReadWithContext(self: *@This(), request: Context) !void {
            try request.ensureActive();
            self.reads += 1;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            if (self.cancel_at_projection) self.cancelled.* = true;
            return self.store;
        }
    };
    var cancelled = false;
    var service: Service = .{ .metadata_group_id = group, .store = &target, .cancelled = &cancelled };
    const context: Context = .{ .cancelled = &cancelled };
    var served = try capture.readOnService(&service, a, query, context);
    defer served.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), service.reads);
    try std.testing.expectEqualDeep(restored.cut, served.cut);
    var foreign = query;
    foreign.metadata_group_id += 1;
    try std.testing.expectError(error.SourceBindingConflict, capture.readOnService(&service, a, foreign, context));
    try std.testing.expectEqual(@as(usize, 1), service.reads);
    const native_control = @import("antfly_local_sources").storage_source_scope_control;
    var intent: native_control.Intent = .{ .version = 1, .owner = authority.owner, .spec = restored.spec, .minimum_metadata_index = 1, .action = .bind };
    // The request cannot bind a retired actor even with an old minimum index.
    try std.testing.expectError(error.SourceBindingConflict, capture.authorizeOnService(&service, a, intent, context));
    intent.action = .close;
    const close_command = try capture.authorizeOnService(&service, a, intent, context);
    try std.testing.expectEqualDeep(restored.cut, close_command.metadata_cut);
    try std.testing.expectEqualDeep(intent, close_command.intent);
    intent.action = .{ .drop = .{ .term = 1, .index = 20 } };
    const drop_command = try capture.authorizeOnService(&service, a, intent, context);
    try std.testing.expectEqualDeep(restored.cut, drop_command.metadata_cut);
    intent.spec.generation += 1;
    try std.testing.expectError(error.SourceBindingConflict, capture.authorizeOnService(&service, a, intent, context));
    intent.spec = restored.spec;
    intent.minimum_metadata_index = restored.cut.index + 1;
    try std.testing.expectError(error.ReadUnavailable, capture.authorizeOnService(&service, a, intent, context));
    service.cancel_at_projection = true;
    try std.testing.expectError(error.Canceled, capture.readOnService(&service, a, query, context));
    const read_count = service.reads;
    try std.testing.expectError(error.Canceled, capture.readOnService(&service, a, query, context));
    try std.testing.expectEqual(read_count, service.reads);
    var stale = active;
    stale.live_definition = false;
    try std.testing.expectError(error.SourceBindingConflict, stale.requireBind());
    var wrong_binding = active.spec;
    wrong_binding.generation += 1;
    try std.testing.expectError(error.SourceBindingConflict, active.requireBinding(wrong_binding));
}
