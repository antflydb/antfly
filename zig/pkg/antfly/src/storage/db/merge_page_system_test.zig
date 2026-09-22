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
const db_mod = @import("mod.zig");
const pages = @import("merge_page_contract.zig");
const types = db_mod.types;
const alloc = std.testing.allocator;

const schema =
    \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"later","expression":{"op":"literal","type":"integer","value":"9"}}],"generated_columns":[{"column":"doubled","expression":{"op":"multiply","args":[{"op":"column","column":"id"},{"op":"literal","type":"integer","value":"2"}]}}],"relational_indexes":[{"name":"by_id","keys":[{"column":"id"}],"include_columns":["doubled"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"doubled":{"type":"integer"},"later":{"type":"integer"}},"additionalProperties":false}}}}
;

fn seal(input: types.BatchRequest) types.BatchRequest {
    var request = input;
    request.merge_page.?.digest = pages.commandDigest(request);
    return request;
}

fn apply(db: *db_mod.DB, index: *u64, request: types.BatchRequest) !void {
    try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = index.* + 1 });
    index.* += 1;
}

fn readCopyReceipt(db: *db_mod.DB) !std.json.Parsed(@import("merge_contract.zig").CopyReceipt) {
    var result = (try db.lookup(alloc, "", .{ .relational_topology_json = "{\"mode\":\"merge_copy_receipt\"}" })) orelse return error.TestUnexpectedResult;
    defer result.deinit(alloc);
    return std.json.parseFromSlice(@import("merge_contract.zig").CopyReceipt, alloc, result.json, .{ .allocate = .alloc_always });
}

fn applyChunk(db: *db_mod.DB, index: *u64, lsn: *u64, standby: bool, request: types.BatchRequest) !void {
    if (!standby) return apply(db, index, request);
    const encoded = try @import("../hot_standby/effects.zig").encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(encoded);
    try db.applyHAReplicationRecord(.{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = lsn.* + 1, .previous_lsn = lsn.*, .payload = encoded });
    lsn.* += 1;
}

test "relational index system merge chunks keep rows invisible until durable verified completion" {
    const chunks = @import("merge_page_chunks.zig");
    for ([_]bool{ false, true }) |relational| for ([_]bool{ false, true }) |tail| for ([_]bool{ false, true }) |rollback_pending| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-chunk-receiver");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 11, .shard_id = 102, .range_id = 102 }, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        if (relational) try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"payload":{"type":"string"}},"additionalProperties":false}}}}
        );
        try db.updateRange(.{ .start = "m", .end = "z" });
        var index: u64 = 0;
        var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        const source: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 }, .pin_digest = @splat(7), .applied_index = 20, .retention = if (tail) .{ .epoch = 3, .after_sequence = 40 } else null };
        checkpoint.kind = .begin_copy;
        checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
        checkpoint.page_source = source;
        checkpoint.page_receiver_namespace = options.identity_namespace.?;
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        const context: types.MergeReplicationContext = .{ .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .identity_namespace = options.identity_namespace.?, .copy_attempt = checkpoint.copy_attempt };
        var page: pages.Command = .{ .source = source, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
        try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = page }));
        page.phase = .rows;
        page.sequence = 2;
        if (tail) {
            try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = page }));
            page.phase = .artifacts;
            page.sequence = 3;
            try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = page }));
            page.phase = .tail;
            page.sequence = 4;
            page.exhausted = false;
            page.tail = .{ .fragment = .{ .sequence = 41, .offset = 0, .total_effects = 1, .frame_digest = @splat(8) } };
        } else page.next = "b";
        const payload = try alloc.alloc(u8, pages.chunk_bytes + 123);
        defer alloc.free(payload);
        @memset(payload, 'x');
        const logical = try std.json.Stringify.valueAlloc(alloc, .{ .id = @as(u32, 7), .payload = payload }, .{});
        defer alloc.free(logical);
        page.timestamps = &.{555};
        const row_request = seal(.{ .merge_replication = context, .merge_page = page, .writes = &.{.{ .key = "b", .value = logical }} });
        const transfer = try pages.RowChunks(types.BatchRequest).init(row_request);
        const first = try transfer.requestAt(0);
        const last = try transfer.requestAt(pages.chunk_bytes);
        try std.testing.expectError(error.MergePageSequenceGap, db.batch(last));
        {
            var txn = try db.core.store.beginReadTxn();
            defer txn.abort();
            var reader: chunks.NativeReader = .{ .txn = &txn };
            var canceled = std.atomic.Value(bool).init(true);
            try std.testing.expectError(error.Canceled, chunks.prepare(alloc, &reader, first, types.CancellationToken.fromAtomic(&canceled)));
        }
        var lsn: u64 = 0;
        try applyChunk(&db, &index, &lsn, relational, first);
        try std.testing.expect((try db.lookup(alloc, "b", .{})) == null);
        {
            var pending = (try db.mergeCopyPageStatus(alloc)).?;
            defer pending.deinit();
            try std.testing.expectEqual(page.sequence - 1, pending.value.sequence);
            try std.testing.expectEqual(@as(u64, pages.chunk_bytes), pending.value.assembly.?.next_offset);
            try std.testing.expectEqual(@as(u32, 0), pending.value.tail_offset);
        }
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        try applyChunk(&db, &index, &lsn, relational, first);
        if (rollback_pending) {
            var rollback = checkpoint;
            rollback.kind = .rollback;
            rollback.page_source = null;
            rollback.page_receiver_namespace = null;
            try apply(&db, &index, .{ .merge_checkpoint = rollback });
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), options);
            // A delayed sender can retry both sides of a fragmented page
            // after losing the rollback response. Neither stale chunk may
            // resurrect a row, the assembly, or the old receiver authority.
            try applyChunk(&db, &index, &lsn, relational, first);
            try applyChunk(&db, &index, &lsn, relational, last);
            try std.testing.expect((try db.lookup(alloc, "b", .{})) == null);
            try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunks.manifest_key));
            try std.testing.expectEqualStrings("m", db.core.byteRange().start);
            try std.testing.expectEqualStrings("z", db.core.byteRange().end);
            try std.testing.expectEqual(@as(?u64, 0), try @import("range_cardinality.zig").load(alloc, db.core.store));
            continue;
        }
        {
            var txn = try db.core.store.beginReadTxn();
            defer txn.abort();
            var reader: chunks.NativeReader = .{ .txn = &txn };
            var buffer: [16 * 1024]u8 = undefined;
            var bounded = std.heap.FixedBufferAllocator.init(&buffer);
            try std.testing.expectError(error.OutOfMemory, chunks.prepare(bounded.allocator(), &reader, last, .none));
        }
        const corrupt_data = try alloc.dupe(u8, last.merge_page.?.chunk.?.data);
        defer alloc.free(corrupt_data);
        corrupt_data[0] ^= 1;
        var corrupt = last;
        corrupt.merge_page.?.chunk.?.data = corrupt_data;
        corrupt.merge_page.?.chunk.?.chunk_digest = chunks.checksum(corrupt_data);
        corrupt = seal(corrupt);
        try std.testing.expectError(error.InvalidMergePage, db.batch(corrupt));
        try std.testing.expect((try db.lookup(alloc, "b", .{})) == null);
        try applyChunk(&db, &index, &lsn, relational, last);
        try std.testing.expectEqual(@as(u64, 555), try db.getTimestamp(alloc, "b"));
        var row = (try db.lookup(alloc, "b", .{})).?;
        defer row.deinit(alloc);
        var decoded = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{});
        defer decoded.deinit();
        try std.testing.expectEqualStrings(payload, decoded.value.object.get("payload").?.string);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunks.manifest_key));
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        try applyChunk(&db, &index, &lsn, relational, first);
        try applyChunk(&db, &index, &lsn, relational, last);
        var complete = (try db.mergeCopyPageStatus(alloc)).?;
        defer complete.deinit();
        try std.testing.expectEqual(page.sequence, complete.value.sequence);
        try std.testing.expect(complete.value.assembly == null);
        if (tail) try std.testing.expectEqual(@as(u64, 41), complete.value.tail_sequence);
    };
}

test "relational index system retained merge tail fragments preserve deletes timestamps and exact final cut" {
    for ([_]bool{ false, true }) |relational| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-tail-receiver");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 11, .shard_id = 102, .range_id = 102 }, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        if (relational) try db.setSchemaJson(alloc, schema);
        try db.updateRange(.{ .start = "m", .end = "z" });
        var index: u64 = 0;
        var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        const source: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 }, .pin_digest = @splat(7), .applied_index = 20, .retention = .{ .epoch = 3, .after_sequence = 40 } };
        checkpoint.kind = .begin_copy;
        checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
        checkpoint.page_source = source;
        checkpoint.page_receiver_namespace = options.identity_namespace.?;
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        const context: types.MergeReplicationContext = .{ .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .identity_namespace = options.identity_namespace.?, .copy_attempt = checkpoint.copy_attempt };
        var page: pages.Command = .{ .source = source, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
        try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = page }));
        page.sequence = 2;
        page.phase = .rows;
        page.next = "b";
        page.timestamps = &.{123};
        try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = page, .writes = &.{.{ .key = "b", .value = "{\"id\":1,\"doubled\":2}" }} }));
        page.sequence = 3;
        page.phase = .artifacts;
        page.next = "";
        page.timestamps = &.{};
        try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = page }));
        var completed = checkpoint;
        completed.kind = .bootstrap_complete;
        completed.page_source = null;
        completed.page_receiver_namespace = null;
        completed.bootstrap_applied_index = 20;
        try std.testing.expectError(error.MergePageIncomplete, db.batch(.{ .merge_checkpoint = completed }));
        page.sequence = 4;
        page.phase = .tail;
        page.exhausted = false;
        page.tail = .{ .fragment = .{ .sequence = 41, .offset = 0, .total_effects = 2, .frame_digest = @splat(8) } };
        const first = seal(.{ .merge_replication = context, .merge_page = page, .deletes = &.{"b"} });
        try apply(&db, &index, first);
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        try apply(&db, &index, first); // lost page acknowledgement across reopen
        var finish = page;
        finish.sequence = 5;
        finish.exhausted = true;
        finish.tail = .{ .finish = .{ .through_sequence = 41, .applied_index = 50, .cut_digest = @splat(9) } };
        try std.testing.expectError(error.MergePageSequenceGap, db.batch(seal(.{ .merge_replication = context, .merge_page = finish })));
        page.sequence = 5;
        page.tail.?.fragment.offset = 1;
        page.timestamps = &.{555};
        const second = seal(.{ .merge_replication = context, .merge_page = page, .writes = &.{.{ .key = "c", .value = "{\"id\":2,\"doubled\":4}" }} });
        var different_frame = second;
        different_frame.merge_page.?.tail.?.fragment.frame_digest = @splat(10);
        different_frame = seal(different_frame);
        try std.testing.expectError(error.InvalidMergePage, db.batch(different_frame));
        if (relational) {
            var invalid = second;
            invalid.writes = &.{.{ .key = "c", .value = "{\"id\":2,\"doubled\":5}" }};
            invalid = seal(invalid);
            try std.testing.expectError(error.InvalidRelationalGeneratedValue, db.batch(invalid));
            var unchanged = (try db.mergeCopyPageStatus(alloc)).?;
            defer unchanged.deinit();
            try std.testing.expectEqual(@as(u32, 1), unchanged.value.tail_offset);
            try std.testing.expectEqual(@as(u64, 40), unchanged.value.tail_sequence);
        }
        // The native standby envelope is independently fail-closed and must
        // preserve the same tail fragment and its original row timestamp.
        const standby_payload = try @import("../hot_standby/effects.zig").encodeBatchMutationRequestAlloc(alloc, second);
        defer alloc.free(standby_payload);
        const standby_record: @import("../hot_standby/replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = standby_payload };
        try db.applyHAReplicationRecord(standby_record);
        try db.applyHAReplicationRecord(standby_record);
        try std.testing.expectEqual(@as(u64, 555), try db.getTimestamp(alloc, "c"));
        var progress = (try db.mergeCopyPageStatus(alloc)).?;
        defer progress.deinit();
        try std.testing.expectEqual(@as(u64, 41), progress.value.tail_sequence);
        try std.testing.expectEqual(@as(u32, 0), progress.value.tail_offset);
        finish.sequence = 6;
        var skipped = finish;
        skipped.tail.?.finish.through_sequence = 42;
        try std.testing.expectError(error.MergePageSequenceGap, db.batch(seal(.{ .merge_replication = context, .merge_page = skipped })));
        try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = finish }));
        try std.testing.expectError(error.MergePageIncomplete, db.batch(.{ .merge_checkpoint = completed }));
        completed.bootstrap_applied_index = 50;
        try apply(&db, &index, .{ .merge_checkpoint = completed });
        try apply(&db, &index, first); // stale delete cannot roll progress back
        var done = (try db.mergeCopyPageStatus(alloc)).?;
        defer done.deinit();
        try std.testing.expectEqual(pages.Phase.complete, done.value.phase);
        try std.testing.expectEqual(@as(u64, 50), done.value.final_applied_index);
        try std.testing.expectEqual(@as(u64, 555), try db.getTimestamp(alloc, "c"));
    }
}

test "relational index system retained afterimages own original row timestamps after overwrite and reopen" {
    const retained = @import("../retained_effects.zig");
    for ([_]bool{ false, true }) |relational| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-retained-timestamp");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 }, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        if (relational) try db.setSchemaJson(alloc, schema);
        // Establish durable identity before admitting the source consumer.
        try db.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "b", .value = "{\"id\":0}" }} });
        const namespace = @import("online_source_contract.zig").namespaceBytes(db.core.identity_namespace);
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            _ = try retained.admit(&txn, namespace, 1, @splat(7), retained.default_limit);
            try txn.commit();
        }
        try db.batch(.{ .timestamp_ns = 111, .writes = &.{.{ .key = "b", .value = "{\"id\":1}" }} });
        try db.batch(.{ .timestamp_ns = 222, .writes = &.{.{ .key = "b", .value = "{\"id\":2}" }} });
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        try std.testing.expectEqual(@as(u64, 222), try db.getTimestamp(alloc, "b"));
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        for ([_]u64{ 111, 222 }, 0..) |timestamp, after| {
            var reader = (try retained.read(&txn, namespace, 1, @splat(7), after)).?;
            const effect = (try reader.next()).?;
            try std.testing.expectEqual(timestamp, effect.timestamp);
            if (relational) try std.testing.expectEqual(timestamp, try @import("algebraic/relational_row_codec.zig").rowWriteTimestampNs(effect.value.?));
            try std.testing.expect(try reader.next() == null);
        }
    }
}

test "relational index system merge tail sessions decode bounded immutable fragments without live timestamp reads" {
    for ([_]bool{ false, true }) |relational| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-tail-reader");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 }, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc, if (relational) schema else "{}");
        const identity = try db.relationalTopologyIdentity();
        const scope: @import("online_source_contract.zig").Scope = .{
            .consumer_epoch = 1,
            .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
            .receiver_namespace = .{ .table_id = 11, .shard_id = 102, .range_id = 102 },
            .fence = .{ .role = .merge_source, .transition_id = 100, .attempt = 1, .peer_group_id = 102, .owner_group_id = 101, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest, .admission_epoch = identity.next_epoch },
        };
        var wrong_attempt = scope;
        wrong_attempt.copy_attempt.donor_term += 1;
        try std.testing.expect(!std.mem.eql(u8, &scope.pin(), &wrong_attempt.pin()));
        wrong_attempt.copy_attempt = .{ .donor_term = 1, .sequence = 0 };
        try std.testing.expectError(error.InvalidOnlineSourceCommand, wrong_attempt.validate());
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
        try db.batchRaftReplicatedApply(.{ .timestamp_ns = 111, .writes = &.{ .{ .key = "a", .value = "{\"id\":1}" }, .{ .key = "b", .value = "{\"id\":2}" } }, .deletes = &.{"c"} }, .{ .term = 1, .index = 2 });
        var session = (try db.beginMergeTailRead(scope, 0)).?;
        defer session.deinit();
        const frame_digest = session.reader.frame_digest;
        var canceled = std.atomic.Value(bool).init(true);
        try std.testing.expectError(error.Canceled, session.next(alloc, 1, 1024, .fromAtomic(&canceled)));
        try std.testing.expectEqual(@as(u32, 0), session.offset);
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, session.next(failing.allocator(), 1, 1024, .none));
        try std.testing.expectEqual(@as(u32, 0), session.offset);
        try std.testing.expectError(error.InvalidMergePage, session.next(alloc, pages.max_rows + 1, 1024, .none));
        // One oversized row is admitted for progress, never a second row.
        var first = (try session.next(alloc, 1, 1, .none)).?;
        defer first.deinit();
        try std.testing.expectEqualStrings("a", first.writes[0].key);
        try std.testing.expectEqual(@as(u64, 111), first.timestamps[0]);
        try std.testing.expect(!first.frame_complete);
        // Mutating the source after opening does not affect the pinned frame.
        try db.batchRaftReplicatedApply(.{ .timestamp_ns = 222, .writes = &.{.{ .key = "b", .value = "{\"id\":3}" }} }, .{ .term = 1, .index = 3 });
        var second = (try session.next(alloc, 1, 1024, .none)).?;
        defer second.deinit();
        try std.testing.expectEqualStrings("b", second.writes[0].key);
        try std.testing.expectEqual(@as(u64, 111), second.timestamps[0]);
        var second_json = try std.json.parseFromSlice(std.json.Value, alloc, second.writes[0].value, .{});
        defer second_json.deinit();
        try std.testing.expectEqual(@as(i64, 2), second_json.value.object.get("id").?.integer);
        try std.testing.expectEqual(frame_digest, second.tail.fragment.frame_digest);
        try std.testing.expectEqual(@as(u32, 1), second.tail.fragment.offset);
        var third = (try session.next(alloc, 1, 1024, .none)).?;
        defer third.deinit();
        try std.testing.expectEqualStrings("c", third.deletes[0]);
        try std.testing.expect(third.frame_complete);
        try std.testing.expect(try session.next(alloc, 1, 1024, .none) == null);
        // Scope identity alone cannot manufacture the transferable source cut.
        try std.testing.expectError(error.SourceSnapshotCutMismatch, first.request(.{
            .namespace = identity.namespace,
            .pin_digest = scope.pin(),
            .applied_index = 1,
            .retention = .{ .epoch = 1, .after_sequence = 0 },
        }, .{ .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .identity_namespace = scope.receiver_namespace, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } }, 1));
        {
            // Document TTL-only changes are valid. Typed rows must update
            // packed metadata atomically or capture rejects the transaction.
            const internal = @import("../internal_keys.zig");
            const ttl_key = try internal.ttlKeyAlloc(alloc, "b");
            defer alloc.free(ttl_key);
            var timestamp: [8]u8 = undefined;
            std.mem.writeInt(u64, &timestamp, 333, .little);
            var txn = try db.core.store.beginWriteTxn();
            var transaction_finished = false;
            defer if (!transaction_finished) txn.abort();
            try txn.put(ttl_key, &timestamp);
            if (relational) {
                try std.testing.expectError(error.RetainedEffectsCorrupt, txn.commit());
                txn.abort();
                transaction_finished = true;
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqual(@as(u64, 222), std.mem.readInt(u64, (try read.get(ttl_key))[0..8], .little));
                try std.testing.expect(try db.beginMergeTailRead(scope, 2) == null);
                continue;
            }
            try txn.commit();
            transaction_finished = true;
            var refreshed = (try db.beginMergeTailRead(scope, 2)).?;
            defer refreshed.deinit();
            var fragment = (try refreshed.next(alloc, 128, pages.max_bytes, .none)).?;
            defer fragment.deinit();
            try std.testing.expectEqual(@as(u64, 333), fragment.timestamps[0]);
            try std.testing.expectEqualStrings("b", fragment.writes[0].key);
            var json = try std.json.parseFromSlice(std.json.Value, alloc, fragment.writes[0].value, .{});
            defer json.deinit();
            try std.testing.expectEqual(@as(i64, 3), json.value.object.get("id").?.integer);
        }
    }
}

test "relational index system merge page admission bounds work and authenticates metadata" {
    const context: types.MergeReplicationContext = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    const source: pages.Source = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .pin_digest = @splat(7), .applied_index = 10 };
    var request = seal(.{ .merge_replication = context, .merge_page = .{ .source = source, .sequence = 1, .phase = .rows, .next = "b", .exhausted = false, .digest = @splat(0), .timestamps = &.{123} }, .writes = &.{.{ .key = "b", .value = "{}" }} });
    try pages.validateRequest(request);
    const digest = request.merge_page.?.digest;
    request.merge_page.?.timestamps = &.{124};
    try std.testing.expectError(error.InvalidMergePage, pages.validateRequest(request));
    request = seal(request);
    try std.testing.expect(!std.mem.eql(u8, &digest, &request.merge_page.?.digest));
    const too_many = [_]types.BatchWrite{.{ .key = "b", .value = "{}" }} ** (pages.max_rows + 1);
    const times = [_]u64{123} ** (pages.max_rows + 1);
    request.writes = &too_many;
    request.merge_page.?.timestamps = &times;
    request = seal(request);
    try std.testing.expectError(error.InvalidMergePage, pages.validateRequest(request));
    const wide = try alloc.alloc(u8, pages.max_bytes + 1);
    defer alloc.free(wide);
    @memset(wide, 'x');
    request.writes = &.{.{ .key = "b", .value = wide }};
    request.merge_page.?.timestamps = &.{123};
    request = seal(request);
    try pages.validateRequest(request); // one indivisible effect makes progress
    request.writes = &.{ .{ .key = "b", .value = "{}" }, .{ .key = "c", .value = wide } };
    request.merge_page.?.next = "c";
    request.merge_page.?.timestamps = &.{ 123, 124 };
    request = seal(request);
    try std.testing.expectError(error.InvalidMergePage, pages.validateRequest(request));
}

test "relational index system merge pages commit effects and cursors with reopen retry cancellation fences" {
    for ([_]bool{ false, true }) |relational| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("merge-page-receiver");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 11, .shard_id = 102, .range_id = 102 }, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        if (relational) try db.setSchemaJson(alloc, schema);
        try db.updateRange(.{ .start = "m", .end = "z" });
        try db.batch(.{ .writes = &.{.{ .key = "m", .value = "{\"id\":9,\"doubled\":18}" }} });
        var index: u64 = 0;
        var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        var context: types.MergeReplicationContext = .{ .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .identity_namespace = db.core.identity_namespace };
        // Retained data from an earlier partial attempt must be cleaned before
        // the immutable pinned source is copied; base-range rows stay live.
        try apply(&db, &index, .{ .merge_replication = context, .writes = &.{.{ .key = "b", .value = "{\"id\":3,\"doubled\":6}" }} });
        const source: pages.Source = .{ .namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 }, .pin_digest = @splat(7), .applied_index = 20 };
        checkpoint.kind = .begin_copy;
        checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
        checkpoint.page_source = source;
        checkpoint.page_receiver_namespace = context.identity_namespace;
        context.copy_attempt = checkpoint.copy_attempt;
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        try std.testing.expectError(error.MergePageRequired, db.batch(.{ .merge_replication = context, .deletes = &.{"b"} }));
        var command: pages.Command = .{ .source = source, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
        // Sender cannot claim cleanup EOF while retained primary data remains.
        try std.testing.expectError(error.InvalidMergePage, db.batch(seal(.{ .merge_replication = context, .merge_page = command })));
        command.next = "b";
        const cleanup = seal(.{ .merge_replication = context, .merge_page = command, .deletes = &.{"b"} });
        try apply(&db, &index, cleanup);
        try apply(&db, &index, cleanup); // lost acknowledgement, new Raft entry
        var changed = cleanup;
        changed.merge_page.?.exhausted = false;
        changed = seal(changed);
        try std.testing.expectError(error.InvalidMergePage, db.batch(changed));
        var progress = (try db.mergeCopyPageStatus(alloc)).?;
        defer progress.deinit();
        try std.testing.expectEqual(pages.Phase.rows, progress.value.phase);
        try std.testing.expectEqual(@as(u64, 1), progress.value.sequence);
        command = .{ .source = source, .sequence = 2, .phase = .rows, .next = "b", .exhausted = false, .digest = @splat(0), .timestamps = &.{123} };
        var row = seal(.{ .merge_replication = context, .merge_page = command, .writes = &.{.{ .key = "b", .value = "{\"id\":7,\"doubled\":14}" }} });
        if (relational) {
            var bad = row;
            bad.writes = &.{.{ .key = "b", .value = "{\"id\":7,\"doubled\":15}" }};
            bad = seal(bad);
            try std.testing.expectError(error.InvalidRelationalGeneratedValue, db.batch(bad));
            var unchanged = (try db.mergeCopyPageStatus(alloc)).?;
            defer unchanged.deinit();
            try std.testing.expectEqual(@as(u64, 1), unchanged.value.sequence);
        }
        var wrong_pin = row;
        wrong_pin.merge_page.?.source.pin_digest = @splat(8);
        wrong_pin = seal(wrong_pin);
        try std.testing.expectError(error.MergeCopyFenced, db.batch(wrong_pin));
        var base_row = row;
        base_row.writes = &.{.{ .key = "m", .value = "{\"id\":7,\"doubled\":14}" }};
        base_row.merge_page.?.next = "m";
        base_row = seal(base_row);
        try std.testing.expectError(error.KeyOutOfRange, db.batch(base_row));
        // Exercise the existing flush path that drops the apply lock after
        // page admission. The page must restart preparation before committing
        // its cursor, while the receiver base write remains independent.
        db.bulk_ingest_coalescer.active = true;
        {
            const pending_key = try alloc.dupe(u8, "m");
            errdefer alloc.free(pending_key);
            const pending_value = try alloc.dupe(u8, "{\"id\":9,\"doubled\":18}");
            errdefer alloc.free(pending_value);
            try db.bulk_ingest_coalescer.entries.append(alloc, .{ .key = pending_key, .value = pending_value, .kind = .write });
        }
        try apply(&db, &index, row);
        try std.testing.expectEqual(@as(u64, 1), db.bulk_ingest_coalescer.stats.flush_calls.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 0), db.bulk_ingest_coalescer.entries.items.len);
        db.bulk_ingest_coalescer.active = false;
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        try apply(&db, &index, cleanup); // old cleanup cannot delete copied b
        try apply(&db, &index, row);
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint }); // must not reset cursor
        var resumed = (try db.mergeCopyPageStatus(alloc)).?;
        defer resumed.deinit();
        try std.testing.expectEqual(@as(u64, 2), resumed.value.sequence);
        try std.testing.expectEqualStrings("b", resumed.value.cursor);
        if (relational) {
            const key = try @import("../internal_keys.zig").relationalRowKeyAlloc(alloc, "b");
            defer alloc.free(key);
            const packed_row = try db.core.store.get(alloc, key);
            defer alloc.free(packed_row);
            var view = db.core.acquireSchemaView().?;
            defer view.release();
            const typed = try @import("algebraic/relational_row_codec.zig").ordinalRowView(packed_row, view.tableSchema().*, view.physicalLayout());
            try std.testing.expectEqual(@as(u64, 123), typed.writeTimestampNs());
            try std.testing.expect((try typed.findCell(typed.ordinalForName("later").?)) == null);
        }
        // Missing a page or skipping a key is a retryable coordinator bug,
        // never an authorization to declare a complete source copy.
        var skipped = row;
        skipped.merge_page.?.sequence = 4;
        skipped = seal(skipped);
        try std.testing.expectError(error.MergePageSequenceGap, db.batch(skipped));
        command = .{ .source = source, .sequence = 3, .phase = .rows, .after = "b", .next = "c", .exhausted = true, .digest = @splat(0), .timestamps = &.{456} };
        const standby_request = seal(.{ .merge_replication = context, .merge_page = command, .writes = &.{.{ .key = "c", .value = "{\"id\":8,\"doubled\":16}" }} });
        const standby_payload = try @import("../hot_standby/effects.zig").encodeBatchMutationRequestAlloc(alloc, standby_request);
        defer alloc.free(standby_payload);
        const standby_record: @import("../hot_standby/replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = standby_payload };
        try db.applyHAReplicationRecord(standby_record);
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        try db.applyHAReplicationRecord(standby_record);
        if (relational) {
            const key = try @import("../internal_keys.zig").relationalRowKeyAlloc(alloc, "c");
            defer alloc.free(key);
            const packed_row = try db.core.store.get(alloc, key);
            defer alloc.free(packed_row);
            var view = db.core.acquireSchemaView().?;
            defer view.release();
            const typed = try @import("algebraic/relational_row_codec.zig").ordinalRowView(packed_row, view.tableSchema().*, view.physicalLayout());
            try std.testing.expectEqual(@as(u64, 456), typed.writeTimestampNs());
            try std.testing.expect((try typed.findCell(typed.ordinalForName("later").?)) == null);
        }
        var complete = checkpoint;
        complete.kind = .bootstrap_complete;
        complete.page_source = null;
        complete.page_receiver_namespace = null;
        complete.bootstrap_applied_index = source.applied_index;
        try std.testing.expectError(error.MergePageIncomplete, db.batch(.{ .merge_checkpoint = complete }));
        command.sequence = 4;
        command.phase = .artifacts;
        command.after = "";
        command.next = "";
        command.timestamps = &.{};
        try apply(&db, &index, seal(.{ .merge_replication = context, .merge_page = command }));
        try apply(&db, &index, .{ .merge_checkpoint = complete });
        {
            var receipt = try readCopyReceipt(&db);
            defer receipt.deinit();
            const observed = receipt.value.state.?;
            try std.testing.expectEqual(!relational, receipt.value.row_derived_document);
            try std.testing.expect(receipt.value.namespace.eql(options.identity_namespace.?));
            try std.testing.expectEqualStrings("a", receipt.value.range.start);
            try std.testing.expectEqualStrings("z", receipt.value.range.end);
            try std.testing.expectEqual(@as(u64, 100), observed.transition_id);
            try std.testing.expectEqual(@as(u64, 101), observed.donor_group_id);
            try std.testing.expectEqual(@as(u64, 102), observed.receiver_group_id);
            try std.testing.expectEqual(@import("merge_contract.zig").Phase.accepting, observed.phase);
            try std.testing.expect(observed.bootstrap_complete);
            try std.testing.expectEqual(source.applied_index, observed.bootstrap_applied_index);
            try std.testing.expectEqualDeep(checkpoint.copy_attempt, observed.copy_attempt);
            try std.testing.expectEqualStrings("m", observed.receiver_base_range.start);
            try std.testing.expectEqualStrings("z", observed.receiver_base_range.end);
            try std.testing.expectEqualStrings("a", observed.merged_range.?.start);
            try std.testing.expectEqualStrings("z", observed.merged_range.?.end);
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), options);
            var reopened = try readCopyReceipt(&db);
            defer reopened.deinit();
            try std.testing.expectEqualDeep(receipt.value, reopened.value);
        }
        if (relational) {
            for (0..128) |_| {
                if ((try db.relationalIndexBuildStatus("by_id")).state == .ready) break;
                _ = try db.runRelationalIndexMaintenancePass();
            } else return error.IndexBuildDidNotConverge;
            var reader = try db.beginRelationalRows(alloc, .{ .index = "by_id", .fields = &.{ "id", "doubled" } });
            defer reader.deinit();
            var covered = try reader.nextPage(alloc, std.testing.io, .{ .rows = 10, .records = 100, .time_ns = std.time.ns_per_s });
            defer covered.deinit();
            try std.testing.expectEqual(@as(usize, 3), covered.rows.len);
            try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
        }
        // New attempt starts at cleanup. An old attempt's page and delayed
        // completion can no longer mutate or certify that replacement.
        checkpoint.copy_attempt.sequence += 1;
        try apply(&db, &index, .{ .merge_checkpoint = checkpoint });
        {
            var replaced_receipt = try readCopyReceipt(&db);
            defer replaced_receipt.deinit();
            try std.testing.expect(!replaced_receipt.value.state.?.bootstrap_complete);
            try std.testing.expectEqual(@as(u64, 0), replaced_receipt.value.state.?.bootstrap_applied_index);
            try std.testing.expectEqualDeep(checkpoint.copy_attempt, replaced_receipt.value.state.?.copy_attempt);
        }
        try std.testing.expectEqual(@as(?u64, 1), try @import("range_cardinality.zig").load(alloc, db.core.store));
        try apply(&db, &index, row);
        var replaced = (try db.mergeCopyPageStatus(alloc)).?;
        defer replaced.deinit();
        try std.testing.expectEqual(@as(u64, 0), replaced.value.sequence);
        try apply(&db, &index, .{ .merge_checkpoint = complete });
        complete.kind = .rollback;
        complete.copy_attempt = checkpoint.copy_attempt;
        complete.bootstrap_applied_index = 0;
        try apply(&db, &index, .{ .merge_checkpoint = complete });
        row.merge_replication.?.copy_attempt = checkpoint.copy_attempt;
        row = seal(row);
        try apply(&db, &index, row); // cancellation remains terminal on replay
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        {
            var aborted_receipt = try readCopyReceipt(&db);
            defer aborted_receipt.deinit();
            const observed = aborted_receipt.value.state.?;
            try std.testing.expectEqual(@import("merge_contract.zig").Phase.rolled_back, observed.phase);
            try std.testing.expect(!observed.bootstrap_complete);
            try std.testing.expectEqual(@as(u64, 0), observed.bootstrap_applied_index);
            try std.testing.expectEqualDeep(checkpoint.copy_attempt, observed.copy_attempt);
            try std.testing.expect(aborted_receipt.value.namespace.eql(options.identity_namespace.?));
            try std.testing.expectEqualStrings("m", aborted_receipt.value.range.start);
            try std.testing.expectEqualStrings("z", aborted_receipt.value.range.end);
        }
        try std.testing.expectEqualStrings("m", db.core.byteRange().start);
        try std.testing.expectEqual(@as(?u64, 1), try @import("range_cardinality.zig").load(alloc, db.core.store));
    }
}
