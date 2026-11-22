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
    const cutover: metadata.ReplicationSourceStatusRecord = .{
        .table_id = table.table_id,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .cutover_mode = "exported_snapshot_pending",
        .phase = "cutover_preparing",
        .slot_name = "slot_first",
        .publication_name = "pub_first",
        .cutover_intent_id = 1,
        .cutover_authority_id = 2,
        .cutover_config_fingerprint = @splat(8),
        .cutover_provider_identity = @splat(9),
    };
    const definition = try control.definitionDigest(a, table.replication_sources_json, 0);
    const source: control.Scope = .{ .table_id = table.table_id, .source_id = control.nativeBootstrapSourceId(table.table_id, 0, definition), .source_ordinal = 0, .generation = 1, .definition = definition, .provider = try control.nativeSourceProviderBinding(cutover), .destination = .native, .destination_binding = @splat(3) };
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
    try store.applyStandaloneCommand(group, .{ .upsert_replication_source_status = .{
        .table_id = table.table_id,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .phase = control.native_bootstrap_prepared_phase,
        .prepared_checkpoint = "lsn:0/1234",
    } });
    const forged = try store.getReplicationSourceStatus(a, group, table.table_id, 0);
    defer if (forged) |record| metadata.freeReplicationSourceStatus(a, record);
    try std.testing.expect(forged == null);
    try actor.apply(.{ .certify_native_bootstrap = "lsn:0/1234" });
    try std.testing.expect(!try store.nativeRootProducerCohortReady(a, group, table.name));
    const bootstrap_digest = control.nativeBootstrapCatalogDigest(table);
    var original = cutover;
    original.phase = control.native_bootstrap_preparing_phase;
    original.cutover_authority_id = 1;
    original.slot_name = "slot_old";
    original.publication_name = "pub_old";
    var previous_schema = table;
    previous_schema.schema_json = "{\"version\":1}";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = control.nativeBootstrapCatalogDigest(previous_schema),
        .record = original,
    } });
    {
        const stale = try store.getReplicationSourceStatus(a, group, table.table_id, 0);
        defer if (stale) |record| metadata.freeReplicationSourceStatus(a, record);
        try std.testing.expect(stale == null);
    }
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = original,
    } });
    original.phase = control.native_bootstrap_snapshot_phase;
    original.cutover_mode = "exported_snapshot";
    original.prepared_checkpoint = "lsn:0/1000";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = original,
    } });
    var unarmed = original;
    unarmed.phase = control.native_bootstrap_applying_phase;
    unarmed.updated_at_ms = 90;
    const captured_rows = "[{\"id\":1}]";
    unarmed.pending_snapshot_page = .{
        .end_offset = 1,
        .checkpoint = "snapshot_keyset:[1]",
        .payload_digest = try control.nativeSnapshotPageDigest(unarmed, 0, 1, false, "snapshot_keyset:[1]", captured_rows),
        .managed_dispatch = true,
    };
    const captured_command = try store_mod.encodeTransitionCommand(a, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = unarmed,
        .captured_rows = captured_rows,
    } });
    defer a.free(captured_command);
    var decoded_capture = (try store_mod.decodeTransitionCommand(a, captured_command)) orelse return error.TestUnexpectedResult;
    defer decoded_capture.deinit(a);
    try store.applyStandaloneCommand(group, decoded_capture);
    store.deinit();
    opened = false;
    store = try Store.init(a, .{ .root_dir = path });
    opened = true;
    const reopened_rows = (try store.getNativeSnapshotRows(a, group, unarmed.table_id, unarmed.source_ordinal)) orelse return error.TestUnexpectedResult;
    defer a.free(reopened_rows);
    try std.testing.expectEqualStrings(captured_rows, reopened_rows);
    const aborted_job: @import("antfly_local_sources").storage_history_recovery_wire.Request = .{
        .owner = authority.owner,
        .transaction_id = try control.nativeSnapshotPageId(unarmed, 0),
        .admission_digest = @splat(82),
        .coordinator_locator = "checkpoint",
        .resolution_timestamp_ns = unarmed.updated_at_ms * std.time.ns_per_ms + 1,
    };
    const aborted_job_bytes = try std.json.Stringify.valueAlloc(a, aborted_job, .{});
    defer a.free(aborted_job_bytes);
    var retained_first = unarmed;
    retained_first.pending_snapshot_page.?.recovery_job = aborted_job_bytes;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = retained_first,
    } });
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = original,
        .completed_page = unarmed.pending_snapshot_page.?.payload_digest,
    } });
    {
        const retained = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, retained);
        try std.testing.expectEqualStrings(control.native_bootstrap_applying_phase, retained.phase);
        try std.testing.expectEqualStrings(aborted_job_bytes, retained.pending_snapshot_page.?.recovery_job);
    }
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = original,
        .completed_page = control.digest(aborted_job_bytes),
    } });
    {
        const reset = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, reset);
        try std.testing.expectEqualStrings(control.native_bootstrap_snapshot_phase, reset.phase);
        try std.testing.expectEqualStrings(aborted_job_bytes, reset.snapshot_cleanup_job);
        try std.testing.expectEqual(@as(u64, 0), reset.snapshot_offset);
    }
    try std.testing.expect(try store.getNativeSnapshotRows(a, group, unarmed.table_id, unarmed.source_ordinal) == null);

    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = original,
        .released_page = control.digest(aborted_job_bytes),
    } });
    var successor = cutover;
    successor.phase = control.native_bootstrap_preparing_phase;
    successor.retired_cutover_authority_id = original.cutover_authority_id;
    successor.retired_slot_name = original.slot_name;
    successor.retired_publication_name = original.publication_name;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = successor,
    } });
    {
        const claimed = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, claimed);
        try std.testing.expectEqual(successor.cutover_authority_id, claimed.cutover_authority_id);
        try std.testing.expect(claimed.pending_snapshot_page == null);
    }
    try std.testing.expect(try store.readNativeSnapshotCapture(a, group, original, 0) == null);
    try store.applyStandaloneCommand(group, .{ .complete_replication_source_retirement = successor });
    var snapshot_status = cutover;
    snapshot_status.cutover_mode = "exported_snapshot";
    snapshot_status.phase = control.native_bootstrap_snapshot_phase;
    snapshot_status.prepared_checkpoint = "lsn:0/1234";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = snapshot_status,
    } });
    var capture_first = snapshot_status;
    capture_first.phase = control.native_bootstrap_applying_phase;
    capture_first.updated_at_ms = unarmed.updated_at_ms;
    capture_first.pending_snapshot_page = unarmed.pending_snapshot_page;
    capture_first.pending_snapshot_page.?.payload_digest = try control.nativeSnapshotPageDigest(snapshot_status, 0, 1, false, "snapshot_keyset:[1]", captured_rows);
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = capture_first,
        .captured_rows = captured_rows,
        .capture_only = true,
    } });
    try std.testing.expect(try store.readNativeSnapshotCapture(a, group, snapshot_status, 0) == null);
    var false_empty = snapshot_status;
    false_empty.phase = control.native_bootstrap_prepared_phase;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = false_empty,
    } });
    {
        const raw = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, raw);
        try std.testing.expectEqualStrings(control.native_bootstrap_snapshot_phase, raw.phase);
    }
    const certify_pages: control.Mutation = .{ .certify_native_snapshot_pages = .{
        .provider_checkpoint = snapshot_status.prepared_checkpoint,
        .first_page = capture_first.pending_snapshot_page.?.payload_digest,
    } };
    try actor.apply(certify_pages);
    {
        var actor_state = try actor.read();
        defer actor_state.deinit();
        try std.testing.expect(actor_state.value.native_bootstrap_first_page == null);
    }
    var last_capture = capture_first;
    last_capture.snapshot_offset = 1;
    last_capture.pending_snapshot_page.?.end_offset = 2;
    last_capture.pending_snapshot_page.?.checkpoint = "snapshot_keyset:[2]";
    last_capture.pending_snapshot_page.?.final = true;
    const last_rows = "[{\"id\":2}]";
    last_capture.pending_snapshot_page.?.payload_digest = try control.nativeSnapshotPageDigest(snapshot_status, 1, 2, true, "snapshot_keyset:[2]", last_rows);
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = last_capture,
        .captured_rows = last_rows,
        .capture_only = true,
    } });
    {
        var first_capture = (try store.readNativeSnapshotCapture(a, group, snapshot_status, 0)) orelse return error.TestUnexpectedResult;
        defer first_capture.deinit();
        try std.testing.expectEqualStrings(captured_rows, first_capture.value.rows);
        var final_capture = (try store.readNativeSnapshotCapture(a, group, snapshot_status, 1)) orelse return error.TestUnexpectedResult;
        defer final_capture.deinit();
        try std.testing.expectEqualStrings(last_rows, final_capture.value.rows);
        try std.testing.expect(final_capture.value.page.final);
        const saved = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, saved);
        try std.testing.expectEqual(@as(u64, 0), saved.snapshot_offset);
    }
    {
        const original_actor = try store.getSourceControl(a, group, source.table_id, source.source_id);
        defer a.free(original_actor);
        try actor.apply(certify_pages);
        var actor_state = try actor.read();
        defer actor_state.deinit();
        try std.testing.expectEqual(certify_pages.certify_native_snapshot_pages.first_page, actor_state.value.native_bootstrap_first_page.?);
        var key_buffer: [256]u8 = undefined;
        const key = try control.keyForGroup(&key_buffer, group, source.table_id, source.source_id);
        try store.store.put(key, original_actor);
    }
    var publication_status = snapshot_status;
    publication_status.phase = control.native_bootstrap_prepared_phase;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = @splat(90),
        .record = publication_status,
    } });
    try actor.apply(.{ .certify_native_bootstrap = "lsn:0/1234" });
    {
        var stale = try actor.read();
        defer stale.deinit();
        try std.testing.expect(stale.value.native_bootstrap_checkpoint == null);
    }
    var ahead = publication_status;
    ahead.snapshot_offset += 1;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = ahead,
    } });
    var changed_cursor = publication_status;
    changed_cursor.checkpoint = "snapshot_keyset:[42]";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = changed_cursor,
    } });
    {
        const protected = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, protected);
        try std.testing.expectEqualStrings(control.native_bootstrap_snapshot_phase, protected.phase);
        try std.testing.expectEqual(snapshot_status.snapshot_offset, protected.snapshot_offset);
        try std.testing.expectEqualStrings(snapshot_status.checkpoint, protected.checkpoint);
    }
    var applying = snapshot_status;
    applying.phase = control.native_bootstrap_applying_phase;
    applying.updated_at_ms = 100;
    applying.pending_snapshot_page = .{ .end_offset = 1, .checkpoint = "snapshot_keyset:[42]", .payload_digest = @splat(55), .final = true };
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = applying,
    } });
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = snapshot_status,
    } });
    {
        const protected = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, protected);
        try std.testing.expectEqualStrings(control.native_bootstrap_applying_phase, protected.phase);
    }
    var invalid_replacement = successor;
    invalid_replacement.cutover_authority_id += 1;
    invalid_replacement.slot_name = "slot_later";
    invalid_replacement.publication_name = "pub_later";
    invalid_replacement.retired_cutover_authority_id = applying.cutover_authority_id;
    invalid_replacement.retired_slot_name = applying.slot_name;
    invalid_replacement.retired_publication_name = applying.publication_name;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = invalid_replacement,
    } });
    var reissued = applying;
    reissued.updated_at_ms = 200;
    reissued.pending_snapshot_page.?.payload_digest = @splat(56);
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = reissued,
    } });
    {
        const saved = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, saved);
        try std.testing.expectEqual(applying.updated_at_ms, saved.updated_at_ms);
        try std.testing.expectEqual(applying.pending_snapshot_page.?.payload_digest, saved.pending_snapshot_page.?.payload_digest);
    }
    const history_job: @import("antfly_local_sources").storage_history_recovery_wire.Request = .{
        .owner = authority.owner,
        .transaction_id = try control.nativeSnapshotPageId(applying, applying.snapshot_offset),
        .admission_digest = @splat(91),
        .coordinator_locator = "checkpoint",
        .resolution_timestamp_ns = applying.updated_at_ms * std.time.ns_per_ms + 1,
    };
    const job_bytes = try std.json.Stringify.valueAlloc(a, history_job, .{});
    defer a.free(job_bytes);
    var armed = applying;
    armed.pending_snapshot_page.?.recovery_job = job_bytes;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = armed,
    } });
    {
        const saved = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, saved);
        try std.testing.expectEqualStrings(job_bytes, saved.pending_snapshot_page.?.recovery_job);
    }
    var wrong_page = applying;
    wrong_page.pending_snapshot_page = null;
    wrong_page.phase = control.native_bootstrap_prepared_phase;
    wrong_page.snapshot_offset = 1;
    wrong_page.checkpoint = "snapshot_keyset:[43]";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = wrong_page,
        .completed_page = @splat(55),
    } });
    wrong_page.checkpoint = "snapshot_keyset:[42]";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = wrong_page,
        .completed_page = @splat(56),
    } });
    {
        const saved = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, saved);
        try std.testing.expectEqual(@as(u64, 0), saved.snapshot_offset);
        try std.testing.expectEqualStrings("snapshot_keyset:[42]", saved.pending_snapshot_page.?.checkpoint);
    }
    applying.snapshot_offset = 1;
    applying.phase = control.native_bootstrap_prepared_phase;
    applying.pending_snapshot_page = null;
    applying.checkpoint = "snapshot_keyset:[42]";
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = applying,
        .completed_page = @splat(55),
    } });
    publication_status.snapshot_offset = applying.snapshot_offset;
    publication_status.checkpoint = applying.checkpoint;
    const bytes = try store_mod.encodeTransitionCommand(a, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = publication_status,
    } });
    defer a.free(bytes);
    var decoded = (try store_mod.decodeTransitionCommand(a, bytes)) orelse return error.TestUnexpectedResult;
    defer decoded.deinit(a);
    try store.applyStandaloneCommand(group, decoded);
    snapshot_status.phase = "cutover_prepared";
    try store.applyStandaloneCommand(group, .{ .upsert_replication_source_status = snapshot_status });
    try actor.apply(.{ .certify_native_bootstrap = "lsn:0/1234" });
    {
        var pending_cleanup = try actor.read();
        defer pending_cleanup.deinit();
        try std.testing.expect(pending_cleanup.value.native_bootstrap_checkpoint == null);
        const saved = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, saved);
        try std.testing.expect(!try store.nativeRootProducerCohortReady(a, group, table.name));
        try std.testing.expectEqualStrings(job_bytes, saved.snapshot_cleanup_job);
    }
    store.deinit();
    opened = false;
    store = try Store.init(a, .{ .root_dir = path });
    opened = true;
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = publication_status,
        .released_page = @splat(98),
    } });
    {
        const saved = (try store.getReplicationSourceStatus(a, group, table.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, saved);
        try std.testing.expectEqualStrings(job_bytes, saved.snapshot_cleanup_job);
    }
    try store.applyStandaloneCommand(group, .{ .publish_native_replication_bootstrap = .{
        .catalog_digest = bootstrap_digest,
        .record = publication_status,
        .released_page = control.digest(job_bytes),
    } });
    try std.testing.expect(try store.nativeRootProducerCohortReady(a, group, table.name));
    const routed_source: metadata.TableRecord = .{
        .name = "external-source",
        .table_id = 8,
        .replication_sources_json = "[{\"type\":\"postgres\",\"routes\":[{\"target_table\":\"docs\"}]}]",
    };
    try store.applyStandaloneCommand(group, .{ .upsert_table = routed_source });
    try std.testing.expect(!try store.nativeRootProducerCohortReady(a, group, table.name));
    const table_binding = @import("../changefeed_control.zig").tableBinding(table);
    const seal: store_mod.TransitionCommand = .{ .seal_native_root_producer_cohort = .{ .table_id = table.table_id, .table_binding = table_binding } };
    try store.applyStandaloneCommand(group, seal);
    try std.testing.expect(!try store.nativeRootProducerCohortSealed(group, table.name, table_binding));
    const routed_fence = try store.getTableTransitionFence(group, routed_source.table_id);
    try store.applyStandaloneCommand(group, .{ .remove_table = .{ .table_id = routed_source.table_id, .expected_transition_generation = routed_fence.generation } });
    try store.applyStandaloneCommand(group, seal);
    try std.testing.expect(try store.nativeRootProducerCohortSealed(group, table.name, table_binding));
    try store.applyStandaloneCommand(group, .{ .upsert_table = routed_source });
    const rejected_source = try store.getTable(a, group, routed_source.table_id);
    defer if (rejected_source) |record| metadata.freeTable(a, record);
    try std.testing.expect(rejected_source == null);
    var scoped_source = routed_source;
    scoped_source.schema_json = "{}";
    scoped_source.replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"require_exact_cutover\":true,\"routes\":[{\"target_table\":\"docs\"}]}]";
    try store.applyStandaloneCommand(group, .{ .upsert_table = scoped_source });
    const before_activation = try store.getTable(a, group, scoped_source.table_id);
    defer if (before_activation) |record| metadata.freeTable(a, record);
    try std.testing.expect(before_activation == null);
    const topology = @import("../topology_protocol.zig");
    const activation = try std.json.Stringify.valueAlloc(a, topology.Activation{
        .version = control.scoped_snapshot_protocol_version,
        .incarnation = incarnation,
        .member_count = 1,
        .membership_fingerprint = @splat(9),
    }, .{});
    defer a.free(activation);
    try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = activation });
    try store.applyStandaloneCommand(group, .{ .upsert_table = scoped_source });
    const admitted = (try store.getTable(a, group, scoped_source.table_id)) orelse return error.TestUnexpectedResult;
    defer metadata.freeTable(a, admitted);
    try std.testing.expectEqualStrings(scoped_source.replication_sources_json, admitted.replication_sources_json);
    const cleanup_fence = try store.getTableTransitionFence(group, scoped_source.table_id);
    try store.applyStandaloneCommand(group, .{ .remove_table = .{ .table_id = scoped_source.table_id, .expected_transition_generation = cleanup_fence.generation } });
    try actor.apply(.{ .certify_native_bootstrap = "lsn:0/1234" });
    store.deinit();
    opened = false;
    store = try Store.init(a, .{ .root_dir = path });
    opened = true;
    var restored = try actor.read();
    defer restored.deinit();
    try std.testing.expect(restored.value.scope_fencing and !restored.value.closing);
    try std.testing.expectEqualStrings("lsn:0/1234", restored.value.native_bootstrap_checkpoint.?);
    {
        const before = try store.listReplicationSourceStatuses(a, group);
        defer store.freeReplicationSourceStatuses(a, before);
        try std.testing.expectEqual(@as(usize, 1), before.len);
        try std.testing.expectEqualStrings(control.native_bootstrap_prepared_phase, before[0].phase);
        var key_buffer: [256]u8 = undefined;
        const key = try control.keyForGroup(&key_buffer, group, source.table_id, source.source_id);
        var state = restored.value;
        state.accepted = .{
            .sequence = 1,
            .checkpoint = "lsn:0/1234",
            .reservation_digest = @splat(5),
            .receipt_digest = control.digest("bootstrap-receipt"),
            .reference = "bootstrap-receipt",
        };
        state.outstanding = &.{.{
            .sequence = state.accepted.sequence,
            .checkpoint = state.accepted.checkpoint,
            .reservation_digest = state.accepted.reservation_digest,
            .payload_digest = @splat(9),
            .receipt = state.accepted.reference,
        }};
        const accepted = try control.encode(a, state);
        defer a.free(accepted);
        try store.store.put(key, accepted);
        const uncommitted = try store.listReplicationSourceStatuses(a, group);
        defer store.freeReplicationSourceStatuses(a, uncommitted);
        try std.testing.expectEqualStrings(control.native_bootstrap_prepared_phase, uncommitted[0].phase);
        state.outstanding = &.{};
        state.committed = .{
            .sequence = 2,
            .checkpoint = "lsn:0/2345",
            .reservation_digest = @splat(6),
            .receipt_digest = control.digest("receipt"),
            .reference = "receipt",
        };
        state.accepted = state.committed;
        const progressed = try control.encode(a, state);
        defer a.free(progressed);
        try store.store.put(key, progressed);
        const statuses = try store.listReplicationSourceStatuses(a, group);
        defer store.freeReplicationSourceStatuses(a, statuses);
        try std.testing.expectEqual(@as(usize, 1), statuses.len);
        try std.testing.expectEqualStrings("streaming", statuses[0].phase);
        try std.testing.expectEqualStrings("lsn:0/2345", statuses[0].checkpoint);
        try std.testing.expectEqualStrings("lsn:0/2345", statuses[0].stream_checkpoint);
        try std.testing.expectEqualStrings("lsn:0/1234", statuses[0].prepared_checkpoint);
        const raw = (try store.getReplicationSourceStatus(a, group, source.table_id, 0)) orelse return error.TestUnexpectedResult;
        defer metadata.freeReplicationSourceStatus(a, raw);
        try std.testing.expectEqualStrings(control.native_bootstrap_prepared_phase, raw.phase);
        try std.testing.expectEqualStrings("", raw.stream_checkpoint);
        const original_state = try control.encode(a, restored.value);
        defer a.free(original_state);
        try store.store.put(key, original_state);
    }
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
    try std.testing.expectEqualDeep(@as(?control.Scope, source), try target.nativeSourceRecoveryScope(a, group, source.table_id, 0, source.definition));
    var replacement = retained_table;
    replacement.replication_sources_json = "[{\"type\":\"postgres\",\"postgres_table\":\"users\",\"slot_name\":\"reseed_slot\",\"publication_name\":\"reseed_pub\",\"require_exact_cutover\":true}]";
    const replace: store_mod.TransitionCommand = .{ .compare_and_replace_table = .{ .expected = retained_table, .replacement = replacement } };
    try target.applyStandaloneCommand(group, replace);
    const before_fence = (try target.getTable(a, group, source.table_id)) orelse return error.TestUnexpectedResult;
    defer metadata.freeTable(a, before_fence);
    try std.testing.expectEqualStrings(table.replication_sources_json, before_fence.replication_sources_json);

    try installed.apply(.{ .fence_checkpoint = .{ .authority_digest = authority.digest(), .proof = @splat(5) } });
    try installed.expectWork(0);
    try target.applyStandaloneCommand(group, replace);
    const after_fence = (try target.getTable(a, group, source.table_id)) orelse return error.TestUnexpectedResult;
    defer metadata.freeTable(a, after_fence);
    try std.testing.expectEqualStrings(replacement.replication_sources_json, after_fence.replication_sources_json);
    const reseed = (try target.getReplicationSourceStatus(a, group, source.table_id, 0)) orelse return error.TestUnexpectedResult;
    defer metadata.freeReplicationSourceStatus(a, reseed);
    try std.testing.expectEqualStrings(control.native_bootstrap_reseed_phase, reseed.phase);
    try std.testing.expectEqual(publication_status.cutover_authority_id, reseed.cutover_authority_id);
    try std.testing.expectEqualStrings(publication_status.slot_name, reseed.slot_name);
    try std.testing.expectEqual(control.nativeBootstrapCatalogDigest(replacement), reseed.native_bootstrap_catalog_digest);
    try std.testing.expect(try target.nativeSourceRecoveryScope(a, group, source.table_id, 0, source.definition) == null);
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
