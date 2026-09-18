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
const store_mod = @import("raft_apply_store.zig");
const shard = @import("shard_state_store.zig");
const batch = @import("../raft_batch.zig");
const types = @import("../../storage/db/types.zig");
const pages = @import("../../storage/db/merge_page_contract.zig");
const raft = @import("../../raft/state_machine/mod.zig");
const alloc = std.testing.allocator;
const group: u64 = 502;

fn payload(store: *store_mod.RaftApplyStore, index: u64, bytes: []const u8) !void {
    const entries = try raft.encodeCommittedEntries(alloc, &.{.{ .term = 1, .index = index, .entry_type = .normal, .data = @constCast(bytes) }});
    defer alloc.free(entries);
    try store.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = index, .entries_bytes = entries });
}

fn command(store: *store_mod.RaftApplyStore, index: u64, request: types.BatchRequest) !void {
    const bytes = try batch.encode(alloc, "docs", request);
    defer alloc.free(bytes);
    try payload(store, index, bytes);
}

fn seal(input: types.BatchRequest) types.BatchRequest {
    var request = input;
    request.merge_page.?.digest = pages.commandDigest(request);
    return request;
}

test "data raft merge pages chunk spool survives snapshot without exposing incomplete rows" {
    var first_dir = try @import("../../common/test_directory.zig").TestDirectory.init("merge-chunk-projection");
    defer first_dir.cleanup();
    var next_dir = try @import("../../common/test_directory.zig").TestDirectory.init("merge-chunk-projection-restored");
    defer next_dir.cleanup();
    var source = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = first_dir.path() });
    defer source.deinit();
    try std.testing.expect(try source.seedGroupSnapshotIfAbsent(alloc, group, 1, .{ .start = "m", .end = "z" }, &.{}));
    const barrier = try batch.encodeProtocolBarrier(alloc, "docs", @import("../../common/data_raft_protocol.zig").batch_merge_chunk_protocol_version);
    defer alloc.free(barrier);
    try payload(&source, 1, barrier);
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    try command(&source, 2, .{ .merge_checkpoint = checkpoint });
    const namespace: @import("../../storage/db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = group, .range_id = group };
    const pin: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 501, .range_id = 501 }, .pin_digest = @splat(7), .applied_index = 100 };
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
    checkpoint.page_source = pin;
    checkpoint.page_receiver_namespace = namespace;
    try command(&source, 3, .{ .merge_checkpoint = checkpoint });
    const context: types.MergeReplicationContext = .{ .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .identity_namespace = namespace, .copy_attempt = checkpoint.copy_attempt };
    try command(&source, 4, seal(.{ .merge_replication = context, .merge_page = .{ .source = pin, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) } }));
    const data = try alloc.alloc(u8, pages.chunk_bytes + 101);
    defer alloc.free(data);
    @memset(data, 'x');
    const value = try std.json.Stringify.valueAlloc(alloc, .{ .payload = data }, .{});
    defer alloc.free(value);
    const base = seal(.{ .merge_replication = context, .merge_page = .{ .source = pin, .sequence = 2, .phase = .rows, .next = "b", .exhausted = true, .timestamps = &.{777}, .digest = @splat(0) }, .writes = &.{.{ .key = "b", .value = value }} });
    const chunks = try pages.RowChunks(types.BatchRequest).init(base);
    const first = try chunks.requestAt(0);
    const last = try chunks.requestAt(pages.chunk_bytes);
    try command(&source, 5, first);
    const before = try source.groupState(alloc, group);
    defer shard.freeGroupStateEntries(alloc, before);
    try std.testing.expectEqual(@as(usize, 0), before.len);
    const snapshot = try source.snapshotBuilder().buildSnapshot(alloc, group);
    defer alloc.free(snapshot);
    var restored = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = next_dir.path() });
    defer restored.deinit();
    try restored.installSnapshot(alloc, group, 5, snapshot);
    try command(&restored, 6, first);
    try command(&restored, 7, last);
    try command(&restored, 8, first);
    const after = try restored.groupState(alloc, group);
    defer shard.freeGroupStateEntries(alloc, after);
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqualStrings("b", after[0].key);
    try std.testing.expectEqualStrings(value, after[0].value);
    const completed = try restored.snapshotBuilder().buildSnapshot(alloc, group);
    defer alloc.free(completed);
    try std.testing.expect(std.mem.indexOf(u8, completed, "data_group_merge_spool:") == null);
}

test "data raft merge pages persist atomic cursor through snapshot retry and protocol fences" {
    var first_dir = try @import("../../common/test_directory.zig").TestDirectory.init("merge-pages-projection");
    defer first_dir.cleanup();
    var second_dir = try @import("../../common/test_directory.zig").TestDirectory.init("merge-pages-projection-reopened");
    defer second_dir.cleanup();
    var source = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = first_dir.path() });
    defer source.deinit();
    try std.testing.expect(try source.seedGroupSnapshotIfAbsent(alloc, group, 1, .{ .start = "m", .end = "z" }, &.{.{ .key = "x", .value = "{\"id\":9}" }}));
    const barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.merge_copy_attempt_protocol_version);
    defer alloc.free(barrier);
    try payload(&source, 1, barrier);
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    try command(&source, 2, .{ .merge_checkpoint = checkpoint });
    const namespace: @import("../../storage/db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = group, .range_id = group };
    const pin: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 501, .range_id = 501 }, .pin_digest = @splat(7), .applied_index = 100 };
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
    checkpoint.page_source = pin;
    checkpoint.page_receiver_namespace = namespace;
    try std.testing.expectError(error.RaftBatchMergeProtocolNotActivated, command(&source, 3, .{ .merge_checkpoint = checkpoint }));
    const active = try batch.encodeProtocolBarrier(alloc, "docs", batch.merge_page_protocol_version);
    defer alloc.free(active);
    try payload(&source, 3, active);
    try command(&source, 4, .{ .merge_checkpoint = checkpoint });
    const context: types.MergeReplicationContext = .{ .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .identity_namespace = namespace, .copy_attempt = checkpoint.copy_attempt };
    var page: pages.Command = .{ .source = pin, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
    const cleanup = seal(.{ .merge_replication = context, .merge_page = page });
    try command(&source, 5, cleanup);
    page.sequence = 2;
    page.phase = .rows;
    page.next = "b\x00";
    page.exhausted = false;
    page.timestamps = &.{123};
    const row = seal(.{ .merge_replication = context, .merge_page = page, .writes = &.{.{ .key = "b\x00", .value = "{ \"id\": 9007199254740993 }" }} });
    try command(&source, 6, row);
    const snapshot = try source.snapshotBuilder().buildSnapshot(alloc, group);
    defer alloc.free(snapshot);
    var restored = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = second_dir.path() });
    defer restored.deinit();
    try restored.installSnapshot(alloc, group, 6, snapshot);
    try command(&restored, 7, row);
    var changed = row;
    changed.writes = &.{.{ .key = "b\x00", .value = "{\"id\":7}" }};
    changed = seal(changed);
    try std.testing.expectError(error.InvalidMergePage, command(&restored, 8, changed));
    try std.testing.expectError(error.MergePageRequired, command(&restored, 8, .{ .merge_replication = context, .writes = row.writes }));
    page.sequence = 3;
    page.after = page.next;
    page.next = "";
    page.exhausted = true;
    page.timestamps = &.{};
    try command(&restored, 8, seal(.{ .merge_replication = context, .merge_page = page }));
    page.sequence = 4;
    page.phase = .artifacts;
    page.after = "";
    try command(&restored, 9, seal(.{ .merge_replication = context, .merge_page = page }));
    checkpoint.kind = .bootstrap_complete;
    checkpoint.page_source = null;
    checkpoint.page_receiver_namespace = null;
    checkpoint.bootstrap_applied_index = pin.applied_index;
    try command(&restored, 10, .{ .merge_checkpoint = checkpoint });
    const rows = try restored.groupState(alloc, group);
    defer shard.freeGroupStateEntries(alloc, rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings(row.writes[0].key, rows[0].key);
    try std.testing.expectEqualStrings(row.writes[0].value, rows[0].value);
    checkpoint.kind = .rollback;
    checkpoint.bootstrap_applied_index = 0;
    try command(&restored, 11, .{ .merge_checkpoint = checkpoint });
    try command(&restored, 12, row);
    var final = (try restored.currentMergeReceiverState(alloc, group)).?;
    defer final.deinit(alloc);
    try std.testing.expectEqual(.rolled_back, final.phase);
}

test "data raft merge pages snapshot locator requires protocol12 and fences cursor replay" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-locator-projection");
    defer directory.cleanup();
    var store = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = directory.path() });
    defer store.deinit();
    try std.testing.expect(try store.seedGroupSnapshotIfAbsent(alloc, group, 1, .{ .start = "m", .end = "z" }, &.{}));
    const old_barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.relational_transfer_protocol_version);
    defer alloc.free(old_barrier);
    try payload(&store, 1, old_barrier);
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    try command(&store, 2, .{ .merge_checkpoint = checkpoint });
    const namespace: @import("../../storage/db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = group, .range_id = group };
    const pin: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 501, .range_id = 501 }, .pin_digest = @splat(7), .applied_index = 100, .retention = .{ .epoch = 3, .after_sequence = 10 } };
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
    checkpoint.page_source = pin;
    checkpoint.page_receiver_namespace = namespace;
    try std.testing.expectError(error.RaftBatchMergeProtocolNotActivated, command(&store, 3, .{ .merge_checkpoint = checkpoint }));
    const barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.source_scope_protocol_version);
    defer alloc.free(barrier);
    try payload(&store, 3, barrier);
    try command(&store, 4, .{ .merge_checkpoint = checkpoint });
    const context: types.MergeReplicationContext = .{ .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .identity_namespace = namespace, .copy_attempt = checkpoint.copy_attempt };
    var page: pages.Command = .{ .source = pin, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
    try command(&store, 5, seal(.{ .merge_replication = context, .merge_page = page }));
    page.sequence = 2;
    page.phase = .rows;
    page.exhausted = false;
    page.next = "b";
    page.next_snapshot_position = .{ .object = 4, .offset = 64, .remaining = 2 };
    page.timestamps = &.{123};
    const first = seal(.{ .merge_replication = context, .merge_page = page, .writes = &.{.{ .key = "b", .value = "{}" }} });
    try command(&store, 6, first);
    try command(&store, 7, first);
    var changed = first;
    changed.merge_page.?.next_snapshot_position.?.offset += 1;
    try std.testing.expectError(error.InvalidMergePage, command(&store, 8, seal(changed)));
    changed.merge_page.?.sequence = 3;
    changed.merge_page.?.after = "b";
    changed.merge_page.?.next = "c";
    changed.writes = &.{.{ .key = "c", .value = "{}" }};
    changed.merge_page.?.next_snapshot_position.?.offset = 63;
    try std.testing.expectError(error.InvalidMergePage, command(&store, 8, seal(changed)));
    changed.merge_page.?.next_snapshot_position.?.offset = 128;
    changed.merge_page.?.after = "a";
    try std.testing.expectError(error.MergePageSequenceGap, command(&store, 8, seal(changed)));
    changed.merge_page.?.after = "b";
    try command(&store, 8, seal(changed));
}

test "data raft merge pages tail requires v12 and resumes fragments through snapshots" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-tail-projection");
    defer directory.cleanup();
    var recovered_directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-tail-projection-recovered");
    defer recovered_directory.cleanup();
    var store = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = directory.path() });
    defer store.deinit();
    try std.testing.expect(try store.seedGroupSnapshotIfAbsent(alloc, group, 1, .{ .start = "m", .end = "z" }, &.{}));
    const old_barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.relational_transfer_protocol_version);
    defer alloc.free(old_barrier);
    try payload(&store, 1, old_barrier);
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    try command(&store, 2, .{ .merge_checkpoint = checkpoint });
    const namespace: @import("../../storage/db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = group, .range_id = group };
    const pin: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 501, .range_id = 501 }, .pin_digest = @splat(7), .applied_index = 100, .retention = .{ .epoch = 3, .after_sequence = 10 } };
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
    checkpoint.page_source = pin;
    checkpoint.page_receiver_namespace = namespace;
    try std.testing.expectError(error.RaftBatchMergeProtocolNotActivated, command(&store, 3, .{ .merge_checkpoint = checkpoint }));
    const barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.source_scope_protocol_version);
    defer alloc.free(barrier);
    try payload(&store, 3, barrier);
    try command(&store, 4, .{ .merge_checkpoint = checkpoint });
    const context: types.MergeReplicationContext = .{ .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .identity_namespace = namespace, .copy_attempt = checkpoint.copy_attempt };
    var page: pages.Command = .{ .source = pin, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
    try command(&store, 5, seal(.{ .merge_replication = context, .merge_page = page }));
    page.sequence = 2;
    page.phase = .rows;
    try command(&store, 6, seal(.{ .merge_replication = context, .merge_page = page }));
    page.sequence = 3;
    page.phase = .artifacts;
    try command(&store, 7, seal(.{ .merge_replication = context, .merge_page = page }));
    page.sequence = 4;
    page.phase = .tail;
    page.exhausted = false;
    page.tail = .{ .fragment = .{ .sequence = 11, .offset = 0, .total_effects = 2, .frame_digest = @splat(8) } };
    page.timestamps = &.{901};
    const first = seal(.{ .merge_replication = context, .merge_page = page, .writes = &.{.{ .key = "b", .value = "{ \"id\": 9007199254740993 }" }} });
    try command(&store, 8, first);
    // Scope-v2 groups require an actual native primary alongside the control
    // projection. Never downgrade the barrier just to exercise snapshot replay.
    try std.testing.expectError(error.NativeSnapshotRequired, store.snapshotBuilder().buildSnapshot(alloc, group));
    const prepared = (try store.prepareSnapshotHandle(group, 8)).?;
    defer prepared.destroy();
    var snapshot = std.Io.Writer.Allocating.init(alloc);
    defer snapshot.deinit();
    try shard.writeNativeSnapshotPrefixTxn(&prepared.txn, alloc, group, &snapshot.writer, 1, null);
    try snapshot.writer.writeByte(0);
    const primary_path = try std.fmt.allocPrintSentinel(alloc, "{s}/native", .{recovered_directory.path()}, 0);
    defer alloc.free(primary_path);
    var primary = try @import("../../storage/docstore.zig").DocStore.open(alloc, primary_path.ptr, .{});
    defer primary.close();
    const primary_key = try @import("../../storage/internal_keys.zig").documentKeyAlloc(alloc, first.writes[0].key);
    defer alloc.free(primary_key);
    try primary.put(primary_key, first.writes[0].value);
    var resumed = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = recovered_directory.path() });
    defer resumed.deinit();
    try resumed.installSnapshotWithNativeSource(alloc, group, 8, snapshot.written(), &primary);
    try command(&resumed, 9, first);
    page.sequence = 5;
    page.tail.?.fragment.offset = 1;
    page.timestamps = &.{};
    var second = seal(.{ .merge_replication = context, .merge_page = page, .deletes = &.{"c"} });
    var changed = second;
    changed.merge_page.?.tail.?.fragment.frame_digest = @splat(9);
    changed = seal(changed);
    try std.testing.expectError(error.InvalidMergePage, command(&resumed, 10, changed));
    try command(&resumed, 10, second);
    page.sequence = 6;
    page.exhausted = true;
    page.tail = .{ .finish = .{ .through_sequence = 11, .applied_index = 150, .cut_digest = @splat(10) } };
    try command(&resumed, 11, seal(.{ .merge_replication = context, .merge_page = page }));
    checkpoint.kind = .bootstrap_complete;
    checkpoint.page_source = null;
    checkpoint.page_receiver_namespace = null;
    checkpoint.bootstrap_applied_index = 100;
    try std.testing.expectError(error.MergePageIncomplete, command(&resumed, 12, .{ .merge_checkpoint = checkpoint }));
    second.merge_page.?.source.retention.?.epoch += 1;
    second = seal(second);
    try std.testing.expectError(error.MergeCopyFenced, command(&resumed, 12, second));
    checkpoint.bootstrap_applied_index = 150;
    try command(&resumed, 12, .{ .merge_checkpoint = checkpoint });
    const rows = try resumed.groupState(alloc, group);
    defer shard.freeGroupStateEntries(alloc, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings(first.writes[0].value, rows[0].value);
    // After the receiver closes admission, obsolete copies remain no-ops.
    try command(&resumed, 13, second);
}

test "data raft merge pages projection cannot acknowledge native source retention controls" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-source-projection-reject");
    defer directory.cleanup();
    var store = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = directory.path() });
    defer store.deinit();
    const barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.online_source_protocol_version);
    defer alloc.free(barrier);
    try payload(&store, 1, barrier);
    const source: @import("../../storage/db/online_source_contract.zig").Scope = .{
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
        .receiver_namespace = .{ .table_id = 11, .shard_id = 503, .range_id = 503 },
        .fence = .{
            .role = .merge_source,
            .transition_id = 10,
            .attempt = 1,
            .peer_group_id = 503,
            .owner_group_id = group,
            .namespace = .{ .table_id = 11, .shard_id = group, .range_id = group },
            .admission_epoch = 1,
            .catalog_digest = @splat(0),
        },
    };
    try std.testing.expectError(error.StorageKernelOwnerUnavailable, command(&store, 2, .{ .online_source = .{ .admit = .{ .scope = source } } }));
    // The failed control did not advance the applied slot or claim retention.
    try command(&store, 2, .{ .writes = &.{.{ .key = "b", .value = "{}" }} });
    const rows = try store.groupState(alloc, group);
    defer shard.freeGroupStateEntries(alloc, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
}

test "data raft merge pages native source projection requires trusted delegate and retries before shared applied publication" {
    const engine = @import("raft_engine");
    const Sink = struct {
        index: u64 = 0,
        calls: usize = 0,
        fn set(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.index = index;
        }
        fn apply(ptr: *anyopaque, _: u64, _: ?engine.core.types.Snapshot, _: []const engine.core.Entry, _: []const engine.core.ReadState) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) return error.RaftApplyWriterUnavailable;
        }
    };
    for ([_]bool{ false, true }) |trusted| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-source-native-projection");
        defer directory.cleanup();
        var store = try store_mod.RaftApplyStore.init(alloc, .{ .root_dir = directory.path(), .native_source_delegate = trusted });
        defer store.deinit();
        const source: @import("../../storage/db/online_source_contract.zig").Scope = .{
            .consumer_epoch = 1,
            .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
            .receiver_namespace = .{ .table_id = 11, .shard_id = 503, .range_id = 503 },
            .fence = .{ .role = .merge_source, .transition_id = 10, .attempt = 1, .peer_group_id = 503, .owner_group_id = group, .namespace = .{ .table_id = 11, .shard_id = group, .range_id = group }, .admission_epoch = 1, .catalog_digest = @splat(0) },
        };
        const bytes = try batch.encode(alloc, "docs", .{ .online_source = .{ .admit = .{ .scope = source } } });
        defer alloc.free(bytes);
        if (trusted) {
            try std.testing.expectError(error.UnsupportedBatchProtocolVersion, payload(&store, 1, bytes));
            const barrier = try batch.encodeProtocolBarrier(alloc, "docs", batch.source_pin_protocol_version);
            defer alloc.free(barrier);
            try payload(&store, 1, barrier);
        }
        var sink = Sink{};
        var state_machine: raft.data.DataStateMachine = .{
            .alloc = alloc,
            .applied_sink = .{ .ptr = &sink, .vtable = &.{ .set_applied_index = Sink.set } },
            .snapshot_builder = store.snapshotBuilder(),
            .delegate = .{ .ptr = &sink, .vtable = &.{ .apply_ready = Sink.apply } },
        };
        const entries = [_]engine.core.Entry{.{ .term = 1, .index = if (trusted) 2 else 1, .data = bytes }};
        try std.testing.expectError(if (trusted) error.RaftApplyWriterUnavailable else error.StorageKernelOwnerUnavailable, state_machine.stateMachine().applyReady(group, null, &entries, &.{}));
        try std.testing.expectEqual(@as(u64, 0), sink.index);
        try std.testing.expectEqual(@as(usize, if (trusted) 1 else 0), sink.calls);
        if (trusted) {
            // Projection is already durable, but a failed native effect cannot
            // advance the shared watermark. Exact replay must call it again.
            try std.testing.expectEqual(@as(u64, 2), (try store.latestBatch(group)).?.commit_index);
            try state_machine.stateMachine().applyReady(group, null, &entries, &.{});
            try std.testing.expectEqual(@as(usize, 2), sink.calls);
            try std.testing.expectEqual(@as(u64, 2), sink.index);
        } else try std.testing.expect(try store.latestBatch(group) == null);
    }
}

test "data raft merge pages cleanup verifies pending put delete overlay and exact EOF" {
    const docstore = @import("../../storage/docstore.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-page-overlay-proof");
    defer directory.cleanup();
    var store = try docstore.DocStore.open(alloc, directory.path().ptr, .{});
    defer store.close();
    const namespace: @import("../../storage/db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = group, .range_id = group };
    const pin: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 501, .range_id = 501 }, .pin_digest = @splat(7), .applied_index = 100 };
    const accept: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    var begin = accept;
    begin.kind = .begin_copy;
    begin.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
    begin.page_source = pin;
    begin.page_receiver_namespace = namespace;
    const context: types.MergeReplicationContext = .{ .transition_id = 500, .donor_group_id = 501, .receiver_group_id = group, .identity_namespace = namespace, .copy_attempt = begin.copy_attempt };
    for (0..3) |trial| {
        var request: types.BatchRequest = .{ .merge_replication = context, .merge_page = .{ .source = pin, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) } };
        if (trial == 1) {
            request.deletes = &.{"d"};
            request.merge_page.?.next = "d";
        } else if (trial == 2) {
            request.deletes = &.{ "c", "d" };
            request.merge_page.?.next = "d";
        }
        request = seal(request);
        const encoded = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(encoded);
        const operations = [_]shard.DataOperation{
            .{ .set_raft_batch_protocol = batch.merge_page_protocol_version },
            .{ .set_range = .{ .start = @constCast("m"), .end = @constCast("z") } },
            .{ .put = .{ .key = @constCast("m"), .value = @constCast("base") } },
            .{ .merge_receiver_checkpoint = .{ .checkpoint = accept } },
            .{ .put = .{ .key = @constCast("b"), .value = @constCast("deleted-before-cleanup") } },
            .{ .put = .{ .key = @constCast("c"), .value = @constCast("pending-source-row") } },
            .{ .put = .{ .key = @constCast("d"), .value = @constCast("pending-source-row") } },
            .{ .delete = @constCast("b") },
            .{ .merge_receiver_checkpoint = .{ .checkpoint = begin } },
            .{ .merge_page_fence = encoded },
            .{ .delete = @constCast("c") },
            .{ .delete = @constCast("d") },
            .{ .merge_copy_fence = null },
        };
        var writes: std.ArrayListUnmanaged(docstore.OwnedKVPair) = .empty;
        defer shard.freeOwnedWrites(alloc, &writes);
        var deletes: std.ArrayListUnmanaged([]u8) = .empty;
        defer {
            for (deletes.items) |key| alloc.free(key);
            deletes.deinit(alloc);
        }
        if (trial < 2) {
            try std.testing.expectError(error.InvalidMergePage, shard.appendOperationEffects(&store, alloc, group, &operations, &writes, &deletes));
        } else {
            try shard.appendOperationEffects(&store, alloc, group, &operations, &writes, &deletes);
            try shard.putOwnedBatch(&store, alloc, writes.items, deletes.items);
        }
    }
    var result = try shard.groupStateKeysPageInRange(&store, alloc, group, .{ .start = "a", .end = "z" }, null, 128, 1024 * 1024);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.entries.len);
    try std.testing.expectEqualStrings("m", result.entries[0].key);
}
