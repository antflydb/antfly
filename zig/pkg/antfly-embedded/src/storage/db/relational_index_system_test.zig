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

//! End-to-end LSM lifecycle and standby contracts, plus reproducible work counts.
const server_test_adapter = if (builtin.is_test) @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter else struct {};
const builtin = @import("builtin");
const hot_standby_publisher_adapter = @import("antfly_server_test_sources").local_test_sources.storage_hot_standby_db_commit;
const replication_ingress = @import("replication_ingress.zig");
const std = @import("std");
const db_mod = @import("mod.zig");
const rows = @import("relational_rows.zig");
const records = @import("relational_index_records.zig");
const internal = @import("../internal_keys.zig");
const primary_mod = @import("antfly_server_test_sources").local_test_sources.storage_hot_standby_primary;
const time = @import("antfly_platform").time;
const alloc = std.testing.allocator;
const docstore = @import("../docstore.zig");

const NullabilityJobFixture = struct {
    const job = @import("relational_nullability_job.zig");
    const topo = @import("relational_integrity_topology.zig");
    const source = @import("../source_authority.zig");
    const namespace = @import("doc_identity_namespace.zig").Namespace{ .table_id = 900, .shard_id = 901, .range_id = 902 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .online_source_authority = .native, .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } };
    const before =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const after =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":false}},"required":["n"],"additionalProperties":false}}}}
    ;
    fn candidate(db: *db_mod.DB) !job.Candidate {
        const predecessor = try db.core.store.get(alloc, @import("relational_integrity_catalog.zig").key);
        defer alloc.free(predecessor);
        return job.Candidate.init(alloc, namespace.table_id, before, after, predecessor, &.{"n"}, .{});
    }
    fn binding(db: *db_mod.DB, prepared: *const job.Candidate, id: u8) !job.Binding {
        const identity = try db.relationalTopologyIdentity();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        return job.Binding.prepare(&read, prepared, @splat(id), @splat(83), .{
            .admission_epoch = identity.next_epoch,
            .transition_id = id,
            .attempt = 1,
            .owner_group_id = namespace.shard_id,
            .peer_group_id = namespace.shard_id,
            .namespace = namespace,
            .catalog_digest = identity.catalog_digest,
            .role = .schema_validation,
        }, .native);
    }
    const Action = enum { begin, freeze, cancel };
    fn apply(db: *db_mod.DB, prepared: *const job.Candidate, expected: job.Binding, action: Action) !job.Header {
        db.core.lockApply();
        defer db.core.unlockApply();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var manager = try db.core.initTxnManager();
        defer manager.deinit();
        const header = switch (action) {
            .begin => try job.stageBegin(alloc, &txn, expected, prepared, null),
            .freeze => try job.stageFrozen(alloc, &txn, &manager, expected, null),
            .cancel => try job.stageCancel(alloc, &txn, &manager, expected, null),
        };
        try txn.commit();
        return header;
    }
    fn page(db: *db_mod.DB, prepared: *const job.PreparedPage) !job.Header {
        db.core.lockApply();
        defer db.core.unlockApply();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var manager = try db.core.initTxnManager();
        defer manager.deinit();
        const header = try job.stagePage(alloc, &txn, &manager, prepared, null);
        try txn.commit();
        return header;
    }
    fn current(db: *db_mod.DB) !job.Header {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        return (try job.load(&read)) orelse error.MissingValidationJob;
    }
    fn clock(db: *db_mod.DB) !u64 {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        return (try source.load(&read)).?.sequence;
    }
};

test "relational index system nullability job drains persists resumes and cancels without publishing" {
    const F = NullabilityJobFixture;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-job-resume");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), F.options);
    var live = true;
    defer if (live) database.close();
    try database.setSchemaJson(alloc, F.before);
    try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
    const old = try database.beginTransactionWithId(@splat(71), 100);
    try database.writeTransaction(old, .{ .writes = &.{.{ .key = "b", .value = "{\"n\":2}" }} });
    var prepared = try F.candidate(&database);
    defer prepared.deinit();
    const expected = try F.binding(&database, &prepared, 17);
    try std.testing.expectError(error.SchemaValidationJobRequired, database.applyRelationalTopologyControl(.{ .fence = expected.fence, .action = .begin }, null));
    const admitted = try F.apply(&database, &prepared, expected, .begin);
    try std.testing.expect(admitted.updated == .native);
    const duplicate = try F.apply(&database, &prepared, expected, .begin);
    try std.testing.expectEqualDeep(admitted, duplicate);
    try std.testing.expectError(error.TransactionTopologyBusy, F.apply(&database, &prepared, expected, .freeze));
    try std.testing.expectEqualDeep(admitted, try F.current(&database));
    try std.testing.expectError(error.IntegrityTopologyBusy, database.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":null}" }} }));
    try database.commitTransaction(old, 220);
    const frozen = try F.apply(&database, &prepared, expected, .freeze);
    try std.testing.expectEqual(F.job.Phase.scanning, frozen.phase);
    try std.testing.expect(frozen.frozen.?.native.sequence > admitted.updated.native.sequence);
    try std.testing.expectEqualDeep(frozen, try F.apply(&database, &prepared, expected, .freeze));
    {
        var first = try F.job.PreparedPage.prepare(alloc, database.core, expected, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
        defer first.deinit();
        var stale = try F.job.PreparedPage.prepare(alloc, database.core, expected, .{ .records = 2, .time_ns = std.time.ns_per_s }, .{});
        defer stale.deinit();
        const advanced = try F.page(&database, &first);
        try std.testing.expectEqual(@as(u64, 1), advanced.rows_scanned);
        const clock = try F.clock(&database);
        try std.testing.expectEqualDeep(advanced, try F.page(&database, &first));
        try std.testing.expectEqual(clock, try F.clock(&database));
        try std.testing.expectError(error.SchemaValidationJobChanged, F.page(&database, &stale));
        try std.testing.expectEqual(clock, try F.clock(&database));
    }
    database.close();
    live = false;
    database = try db_mod.DB.open(alloc, directory.path(), F.options);
    live = true;
    try std.testing.expectEqual(@as(u64, 1), (try F.current(&database)).rows_scanned);
    for (0..8) |_| {
        var next = try F.job.PreparedPage.prepare(alloc, database.core, expected, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
        defer next.deinit();
        const result = try F.page(&database, &next);
        if (result.phase == .ready) break;
    } else return error.ValidationJobDidNotComplete;
    const validated = try F.current(&database);
    try std.testing.expectEqual(@as(u64, 2), validated.rows_scanned);
    try std.testing.expectEqual(F.job.Phase.ready, validated.phase);
    try std.testing.expectError(error.IntegrityTopologyBusy, database.setSchemaJson(alloc,
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":true},"extra":{"type":"string","nullable":true}},"additionalProperties":false}}}}
    ));
    try std.testing.expectError(error.SchemaValidationJobChanged, F.job.PreparedPage.prepare(alloc, database.core, expected, .{}, .{}));
    {
        var read = try database.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 1), database.core.schema.?.version);
        const bytes = try validated.encode(alloc);
        defer alloc.free(bytes);
        try std.testing.expectEqualDeep(validated, try F.job.Header.decode(bytes));
        // Structural codec coverage only: changing the tagged authority is
        // not permission to submit a native job through a Raft producer.
        var raft_header = validated;
        raft_header.binding.authority = .raft;
        raft_header.admitted = .{ .raft = .{ .term = 1, .index = 10 } };
        raft_header.frozen = .{ .raft = .{ .term = 1, .index = 11 } };
        raft_header.updated = .{ .raft = .{ .term = 1, .index = 12 } };
        const raft_bytes = try raft_header.encode(alloc);
        defer alloc.free(raft_bytes);
        try std.testing.expectEqualDeep(raft_header, try F.job.Header.decode(raft_bytes));
        raft_header.updated = validated.updated;
        try std.testing.expectError(error.InvalidControlReceiptPosition, raft_header.encode(alloc));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, F.topo.stageRelease(&read, expected.fence));
        try std.testing.expectError(error.SchemaValidationCancellationRequired, F.topo.stageCancel(&read, expected.fence));
    }
    const canceled = try F.apply(&database, &prepared, expected, .cancel);
    try std.testing.expectEqual(F.job.Phase.canceled, canceled.phase);
    try std.testing.expect((try database.relationalTopologyStatus()).fence == null);
    try std.testing.expectEqualDeep(canceled, try F.apply(&database, &prepared, expected, .cancel));
    try std.testing.expectError(error.SchemaValidationCanceled, F.apply(&database, &prepared, expected, .begin));
    try database.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":null}" }} });

    const never_visited = try F.binding(&database, &prepared, 18);
    const terminal = try F.apply(&database, &prepared, never_visited, .cancel);
    try std.testing.expect(terminal.admitted == null);
    try std.testing.expectError(error.SchemaValidationCanceled, F.apply(&database, &prepared, never_visited, .begin));
    try std.testing.expectError(error.SchemaValidationJobChanged, F.apply(&database, &prepared, expected, .begin));
}

test "relational index system nullability job failure scope corruption and transaction rollback stay fenced" {
    const F = NullabilityJobFixture;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-job-faults");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), F.options);
    defer database.close();
    try database.setSchemaJson(alloc, F.before);
    try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":null}" }} });
    var prepared = try F.candidate(&database);
    defer prepared.deinit();
    const expected = try F.binding(&database, &prepared, 21);
    var foreign = expected;
    foreign.fence.namespace.range_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, F.apply(&database, &prepared, foreign, .begin));
    _ = try F.apply(&database, &prepared, expected, .begin);
    _ = try F.apply(&database, &prepared, expected, .freeze);
    {
        const original = try F.current(&database);
        const clock_before = try F.clock(&database);
        database.core.lockApply();
        defer database.core.unlockApply();
        var txn = try database.core.store.beginWriteTxn();
        defer txn.abort();
        var manager = try database.core.initTxnManager();
        defer manager.deinit();
        var future = original;
        future.updated.native.sequence = clock_before + 1;
        const bytes = try future.encode(alloc);
        defer alloc.free(bytes);
        try txn.put(F.job.key, bytes);
        // A structurally valid copied header is not evidence that its cut
        // committed on this owner. No mutation may silently adopt it.
        try std.testing.expectError(error.InvalidControlReceiptPosition, F.job.stageBegin(alloc, &txn, expected, &prepared, null));
        try std.testing.expectError(error.InvalidControlReceiptPosition, F.job.stageFrozen(alloc, &txn, &manager, expected, null));
        try std.testing.expectError(error.InvalidControlReceiptPosition, F.job.stageCancel(alloc, &txn, &manager, expected, null));
        try std.testing.expectEqual(clock_before, try F.clock(&database));
        try std.testing.expectEqualDeep(original, try F.current(&database));
    }
    var failed = try F.job.PreparedPage.prepare(alloc, database.core, expected, .{}, .{});
    defer failed.deinit();
    try std.testing.expectEqual(@as(?usize, 0), failed.page.violation_column);
    const failure_wire = try (try failed.command()).encode(alloc);
    defer alloc.free(failure_wire);
    const failure_command = try F.job.PageCommand.decode(failure_wire);
    var concealed_null = failure_command;
    concealed_null.boundary.violation_column = null;
    try std.testing.expectError(error.InvalidNullabilityReplay, F.job.PreparedPage.replay(alloc, database.core, concealed_null, .{}));
    var replayed_failure = try F.job.PreparedPage.replay(alloc, database.core, failure_command, .{});
    defer replayed_failure.deinit();
    const result = try F.page(&database, &failed);
    try std.testing.expectEqual(F.job.Phase.failed, result.phase);
    try std.testing.expectEqual(@as(?u16, 0), result.failure_column);
    const failed_clock = try F.clock(&database);
    try std.testing.expectEqualDeep(result, try F.page(&database, &replayed_failure));
    try std.testing.expectEqual(failed_clock, try F.clock(&database));
    try std.testing.expectError(error.IntegrityTopologyBusy, database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} }));
    const encoded = try result.encode(alloc);
    defer alloc.free(encoded);
    encoded[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidSchemaValidationJob, F.job.Header.decode(encoded));
    encoded[encoded.len - 1] ^= 1;
    var invalid = result;
    invalid.rows_scanned = invalid.records + 1;
    try std.testing.expectError(error.InvalidSchemaValidationJob, invalid.encode(alloc));
    _ = try F.apply(&database, &prepared, expected, .cancel);
    const next_binding = try F.binding(&database, &prepared, 22);
    const Rollback = struct {
        fn run(a: std.mem.Allocator, db: *db_mod.DB, binding: F.job.Binding, candidate: *const F.job.Candidate) !void {
            const clock = try F.clock(db);
            const old = try F.current(db);
            db.core.lockApply();
            defer db.core.unlockApply();
            var txn = try db.core.store.beginWriteTxn();
            var active = true;
            defer if (active) txn.abort();
            _ = F.job.stageBegin(a, &txn, binding, candidate, null) catch |err| {
                txn.abort();
                active = false;
                try std.testing.expectEqual(clock, try F.clock(db));
                try std.testing.expectEqualDeep(old, try F.current(db));
                return err;
            };
            // Abort even the successful attempt so every allocation cut
            // starts from the same committed owner record and native clock.
            txn.abort();
            active = false;
            try std.testing.expectEqual(clock, try F.clock(db));
            try std.testing.expectEqualDeep(old, try F.current(db));
        }
    };
    try Rollback.run(alloc, &database, next_binding, &prepared);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Rollback.run, .{ &database, next_binding, &prepared });
}

test "relational index system nullability job reserves its entire metadata family" {
    const F = NullabilityJobFixture;
    const controller = @import("relational_index_catalog.zig").Controller;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-job-reserved");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), F.options);
    defer database.close();
    try database.setSchemaJson(alloc, F.before);
    for ([_][]const u8{ F.job.key, F.job.inputs_key, F.job.cursor_key, F.job.key ++ ":future-receipt" }) |reserved_key| {
        const before_rejected_mutations = try F.clock(&database);
        try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, controller.validateExtraMetadata(&.{.{ .key = reserved_key, .value = "forged" }}, &.{}));
        try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, controller.validateExtraMetadata(&.{}, &.{reserved_key}));
        try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, database.batch(.{ .writes = &.{.{ .key = reserved_key, .value = "forged" }} }));
        try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, database.batch(.{ .deletes = &.{reserved_key} }));
        try std.testing.expectEqual(before_rejected_mutations, try F.clock(&database));
        const transaction = try database.beginTransaction(100);
        // Transaction ingress already rejects the complete protected internal
        // family, before the ordinary mutation reservation check.
        try std.testing.expectError(error.InvalidIntegrityOperation, database.writeTransaction(transaction, .{ .writes = &.{.{ .key = reserved_key, .value = "forged" }} }));
        try database.abortTransaction(transaction, 200);
        try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, database.core.prepareSchemaMetadata(database.core.schema.?, &.{.{ .key = reserved_key, .value = "forged" }}));
    }
    var read = try database.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try F.job.load(&read) == null);
    try std.testing.expect((try database.relationalTopologyStatus()).fence == null);
}

test "relational index system nullability job prepares a compatible epoch without index or row replacement" {
    const F = NullabilityJobFixture;
    const api = @import("../../schema/mod.zig");
    for ([_]bool{ false, true }) |indexed| {
        const prefix = "{{\"relational_indexes\":[{{\"name\":\"by_n\",\"keys\":[{{\"column\":\"n\"}}]}}],{s}";
        const before = if (indexed) try std.fmt.allocPrint(alloc, prefix, .{F.before[1..]}) else try alloc.dupe(u8, F.before);
        defer alloc.free(before);
        const after = if (indexed) try std.fmt.allocPrint(alloc, prefix, .{F.after[1..]}) else try alloc.dupe(u8, F.after);
        defer alloc.free(after);
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-compatible-epoch");
        defer directory.cleanup();
        var database = try db_mod.DB.open(alloc, directory.path(), F.options);
        defer database.close();
        try database.setSchemaJson(alloc, before);
        try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
        const primary = try internal.relationalRowKeyAlloc(alloc, "a");
        defer alloc.free(primary);
        const original = try database.core.store.get(alloc, primary);
        defer alloc.free(original);
        const predecessor = try database.core.store.get(alloc, @import("relational_integrity_catalog.zig").key);
        defer alloc.free(predecessor);
        var candidate = try F.job.Candidate.init(alloc, F.namespace.table_id, before, after, predecessor, &.{"n"}, .{});
        defer candidate.deinit();
        const binding = try F.binding(&database, &candidate, 31);
        _ = try F.apply(&database, &candidate, binding, .begin);
        var definition = try api.PreparedSchemaDefinition.init(alloc, after);
        defer definition.deinit(alloc);
        const versioned = try api.versionedSchemaKeyAlloc(alloc, binding.after_version);
        defer alloc.free(versioned);
        const writes = [_]docstore.KVPair{ .{ .key = "\x00\x00__metadata__:schema_json", .value = after }, .{ .key = versioned, .value = after } };
        try std.testing.expectError(error.SchemaValidationNotReady, database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &writes, binding, &candidate));
        _ = try F.apply(&database, &candidate, binding, .freeze);
        var page = try F.job.PreparedPage.prepare(alloc, database.core, binding, .{}, .{});
        defer page.deinit();
        const validated = try F.page(&database, &page);
        try std.testing.expectEqual(F.job.Phase.ready, validated.phase);
        try std.testing.expectError(error.SchemaValidationJobChanged, database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, writes[0..1], binding, &candidate));
        const duplicate_writes = [_]docstore.KVPair{ writes[0], writes[0], writes[1] };
        try std.testing.expectError(error.SchemaValidationJobChanged, database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &duplicate_writes, binding, &candidate));
        const wrong_public = [_]docstore.KVPair{ writes[0], .{ .key = versioned, .value = before } };
        try std.testing.expectError(error.SchemaValidationJobChanged, database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &wrong_public, binding, &candidate));
        var prepared = try database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &writes, binding, &candidate);
        defer prepared.deinit();
        try std.testing.expectEqualDeep(binding, prepared.validated_nullability.?.binding);
        try std.testing.expectEqualDeep(try validated.digest(), prepared.validated_nullability.?.ready_digest);
        if (indexed) {
            const previous_index = prepared.base_relational_indexes.?.plan.boundIndexes()[0];
            const next_index = prepared.relational_indexes.?.plan.boundIndexes()[0];
            try std.testing.expectEqual(previous_index.id(), next_index.id());
            try std.testing.expectEqual(@as(usize, 0), prepared.relational_indexes.?.metadata.retired.len);
            try std.testing.expectEqual(@as(u32, 2), prepared.relational_indexes.?.plan.schemaView().version());
        } else {
            try std.testing.expect(prepared.base_relational_indexes == null);
            try std.testing.expect(prepared.relational_indexes == null);
        }
        try std.testing.expectError(error.SchemaValidationPublicationRequired, database.core.commitPreparedSchemaMetadata(&prepared, &writes, &.{}, null));
        _ = try F.apply(&database, &candidate, binding, .cancel);
        try std.testing.expectError(error.SchemaValidationPublicationRequired, database.core.commitPreparedSchemaMetadata(&prepared, &writes, &.{}, null));
        const actual = try database.core.store.get(alloc, primary);
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, original, actual);
        try std.testing.expectEqual(@as(u32, 1), database.core.schema.?.version);
    }
}

test "relational index system nullability job installation joins schema catalog clock and outbox without releasing admission" {
    const F = NullabilityJobFixture;
    const api = @import("../../schema/mod.zig");
    const Participant = struct {
        fail: bool,
        const outbox_key = "\x00\x00__metadata__:test_nullability_install_outbox";
        pub fn validateBefore(_: @This(), _: anytype) !void {}
        pub fn stage(self: @This(), txn: anytype, installed: F.job.Header) !void {
            const value = try installed.encode(alloc);
            defer alloc.free(value);
            try txn.put(outbox_key, value);
            if (self.fail) return error.InjectedSchemaInstallFailure;
        }
    };
    for ([_]bool{ false, true }) |indexed| {
        const prefix = "{{\"relational_indexes\":[{{\"name\":\"by_n\",\"keys\":[{{\"column\":\"n\"}}]}}],{s}";
        const before = if (indexed) try std.fmt.allocPrint(alloc, prefix, .{F.before[1..]}) else try alloc.dupe(u8, F.before);
        defer alloc.free(before);
        const after = if (indexed) try std.fmt.allocPrint(alloc, prefix, .{F.after[1..]}) else try alloc.dupe(u8, F.after);
        defer alloc.free(after);
        const checked_before = try std.fmt.allocPrint(alloc, "{{\"checks\":[{{\"name\":\"positive\",\"column\":\"n\",\"op\":\"gt\",\"value\":0}}],{s}", .{before[1..]});
        defer alloc.free(checked_before);
        const checked_after = try std.fmt.allocPrint(alloc, "{{\"checks\":[{{\"name\":\"positive\",\"column\":\"n\",\"op\":\"gt\",\"value\":0}}],{s}", .{after[1..]});
        defer alloc.free(checked_after);
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-installed-cold");
        defer directory.cleanup();
        var database = try db_mod.DB.open(alloc, directory.path(), F.options);
        var live = true;
        defer if (live) database.close();
        try database.setSchemaJson(alloc, checked_before);
        try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
        if (indexed) {
            for (0..8) |_| {
                if ((try database.relationalIndexBuildStatus("by_n")).state == .ready) break;
                try database.buildRelationalIndexStep("by_n", .{ .time_ns = std.time.ns_per_s });
            } else return error.ExistingIndexDidNotBecomeReady;
        }
        const primary = try internal.relationalRowKeyAlloc(alloc, "a");
        defer alloc.free(primary);
        const original = try database.core.store.get(alloc, primary);
        defer alloc.free(original);
        const predecessor = try database.core.store.get(alloc, @import("relational_integrity_catalog.zig").key);
        defer alloc.free(predecessor);
        var candidate = try F.job.Candidate.init(alloc, F.namespace.table_id, checked_before, checked_after, predecessor, &.{"n"}, .{});
        defer candidate.deinit();
        const binding = try F.binding(&database, &candidate, 41);
        // Controlled pre-existing incomplete CHECK coverage. A nullability
        // install must carry it, not claim the other constraint was validated.
        const coverage = @import("relational_integrity_activation.zig");
        {
            var txn = try database.core.store.beginWriteTxn();
            errdefer txn.abort();
            var catalog = try @import("relational_integrity_catalog.zig").decode(alloc, predecessor);
            defer catalog.deinit();
            var progress = try coverage.status(&txn, catalog);
            progress.state = .validating;
            progress.phase = .check;
            progress.rows_scanned = 7;
            progress.cursor = "retained-check-prefix";
            const encoded = try progress.encode(alloc);
            defer alloc.free(encoded);
            try txn.put(coverage.key, encoded);
            try txn.commit();
        }
        _ = try F.apply(&database, &candidate, binding, .begin);
        _ = try F.apply(&database, &candidate, binding, .freeze);
        var page = try F.job.PreparedPage.prepare(alloc, database.core, binding, .{ .time_ns = std.time.ns_per_s }, .{});
        defer page.deinit();
        const validated = try F.page(&database, &page);
        try std.testing.expectEqual(F.job.Phase.ready, validated.phase);
        var definition = try api.PreparedSchemaDefinition.init(alloc, checked_after);
        defer definition.deinit(alloc);
        const versioned = try api.versionedSchemaKeyAlloc(alloc, binding.after_version);
        defer alloc.free(versioned);
        const writes = [_]docstore.KVPair{ .{ .key = "\x00\x00__metadata__:schema_json", .value = checked_after }, .{ .key = versioned, .value = checked_after } };
        const clock_before = try F.clock(&database);
        const index_state = database.core.table_catalog.index_state;
        const physical_before = if (database.core.relational_indexes.acquire()) |snapshot| blk: {
            var retained = snapshot;
            defer retained.deinit();
            break :blk retained.plan.boundIndexes()[0].id();
        } else null;
        {
            var prepared = try database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &writes, binding, &candidate);
            defer prepared.deinit();
            database.core.lockApply();
            defer database.core.unlockApply();
            try std.testing.expectError(error.InjectedSchemaInstallFailure, database.core.commitPreparedSchemaMetadataValidatedNullability(&prepared, &writes, @splat(91), null, Participant{ .fail = true }));
        }
        try std.testing.expectEqualDeep(validated, try F.current(&database));
        try std.testing.expectEqual(clock_before, try F.clock(&database));
        try std.testing.expectEqual(@as(u32, 1), database.core.schema.?.version);
        try std.testing.expectError(error.NotFound, database.core.store.get(alloc, Participant.outbox_key));
        try std.testing.expectError(error.NotFound, database.core.store.get(alloc, versioned));
        const installed = blk: {
            var prepared = try database.core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &writes, binding, &candidate);
            defer prepared.deinit();
            database.core.lockApply();
            defer database.core.unlockApply();
            break :blk try database.core.commitPreparedSchemaMetadataValidatedNullability(&prepared, &writes, @splat(91), null, Participant{ .fail = false });
        };
        try std.testing.expectEqual(F.job.Phase.installed, installed.phase);
        try std.testing.expectEqualDeep(validated.updated, installed.installation.?.ready_position);
        var damaged_predecessor = installed;
        damaged_predecessor.records += 1;
        try std.testing.expectError(error.InvalidSchemaValidationJob, damaged_predecessor.encode(alloc));
        try std.testing.expect(installed.updated.native.sequence > clock_before);
        try std.testing.expectEqual(index_state, database.core.table_catalog.index_state);
        try std.testing.expectEqual(@as(u32, 2), database.core.schema.?.version);
        if (indexed) {
            var selected = database.core.relational_indexes.acquire().?;
            defer selected.deinit();
            try std.testing.expectEqual(physical_before.?, selected.plan.boundIndexes()[0].id());
            try std.testing.expectEqual(@as(u32, 2), selected.plan.schemaView().version());
        }
        const outbox = try database.core.store.get(alloc, Participant.outbox_key);
        defer alloc.free(outbox);
        try std.testing.expectEqualDeep(installed, try F.job.Header.decode(outbox));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, F.apply(&database, &candidate, binding, .cancel));
        try std.testing.expectError(error.IntegrityTopologyBusy, database.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"n\":2}" }} }));
        database.close();
        live = false;
        database = try db_mod.DB.open(alloc, directory.path(), F.options);
        live = true;
        var read = try database.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(installed, try F.job.requireInstalled(alloc, &read, binding, @splat(91)));
        try std.testing.expectError(error.SchemaValidationJobChanged, F.job.requireInstalled(alloc, &read, binding, @splat(92)));
        const carried = try coverage.Progress.decode(try read.get(coverage.key));
        try std.testing.expectEqual(coverage.State.validating, carried.state);
        try std.testing.expectEqual(@as(u32, 2), carried.schema_version);
        try std.testing.expectEqual(@as(u64, 7), carried.rows_scanned);
        try std.testing.expectEqualStrings("retained-check-prefix", carried.cursor);
        {
            var reader = try NullabilityScanFixture.open(&database, alloc, .{ .fields = &.{"n"} }, 0);
            defer reader.deinit();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const decoded = (try reader.lookupTypedRow(arena.allocator(), "a")).?;
            try std.testing.expectEqual(@as(u32, 2), reader.active.version());
            try std.testing.expectEqual(@as(u32, 1), decoded.schema_version);
            try std.testing.expectEqual(@as(i64, 1), decoded.typed.?.object.get("n").?.integer);
        }
        const actual = try database.core.store.get(alloc, primary);
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, original, actual);
        try std.testing.expectEqual(@as(u32, 1), try @import("algebraic/relational_row_codec.zig").rowSchemaVersion(actual));
        try std.testing.expectEqual(@as(u32, 2), database.core.schema.?.version);
        try std.testing.expect((try database.relationalTopologyStatus()).fence.?.eql(binding.fence));
        if (indexed) {
            try std.testing.expectEqual(@import("relational_index_jobs.zig").State.ready, (try database.relationalIndexBuildStatus("by_n")).state);
            var index_reader = blk: {
                database.core.lockApplyShared();
                defer database.core.unlockApplyShared();
                var view = database.core.acquireSchemaView().?;
                defer view.release();
                var selected = database.core.relational_indexes.acquire().?;
                defer selected.deinit();
                break :blk try rows.Reader.open(alloc, database.core.store, view, selected, .{ .index = "by_n", .fields = &.{"n"} }, 0);
            };
            defer index_reader.deinit();
            var indexed_page = try index_reader.nextTypedPage(alloc, null, .{ .time_ns = std.time.ns_per_s });
            defer indexed_page.deinit();
            try std.testing.expectEqual(@as(usize, 1), indexed_page.rows.len);
            try std.testing.expectEqual(@as(i64, 1), indexed_page.rows[0].typed.?.object.get("n").?.integer);
        }
        try std.testing.expectError(error.IntegrityTopologyBusy, database.batch(.{ .writes = &.{.{ .key = "c", .value = "{\"n\":3}" }} }));
    }
}

test "relational index system nullability job replay independently verifies bounded binary prefixes across distinct LSM roots" {
    const F = NullabilityJobFixture;
    var first_dir = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-page-origin");
    defer first_dir.cleanup();
    var second_dir = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-page-peer");
    defer second_dir.cleanup();
    var primary_db = try db_mod.DB.open(alloc, first_dir.path(), F.options);
    defer primary_db.close();
    var peer_options = F.options;
    peer_options.primary_backend = .{ .lsm = .{ .flush_threshold = 4096 } };
    var peer_db = try db_mod.DB.open(alloc, second_dir.path(), peer_options);
    defer peer_db.close();
    try primary_db.setSchemaJson(alloc, F.before);
    try peer_db.setSchemaJson(alloc, F.before);
    const writes = [_]db_mod.types.BatchWrite{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "b", .value = "{\"n\":2}" }, .{ .key = "c", .value = "{\"n\":3}" } };
    var reversed = writes;
    std.mem.reverse(db_mod.types.BatchWrite, &reversed);
    try primary_db.batch(.{ .timestamp_ns = 100, .writes = &writes });
    try peer_db.batch(.{ .timestamp_ns = 100, .writes = &reversed });
    // Binary physical cursor and a bounded empty family. The replica has a
    // different flush threshold and reverse input insertion order.
    const orphan = try internal.ttlKeyAlloc(alloc, "orphan-\xff");
    defer alloc.free(orphan);
    const stamp: [8]u8 = @splat(0);
    try primary_db.core.store.put(orphan, &stamp);
    try peer_db.core.store.put(orphan, &stamp);
    var first_candidate = try F.candidate(&primary_db);
    defer first_candidate.deinit();
    var second_candidate = try F.candidate(&peer_db);
    defer second_candidate.deinit();
    const binding = try F.binding(&primary_db, &first_candidate, 51);
    try std.testing.expectEqualDeep(binding, try F.binding(&peer_db, &second_candidate, 51));
    _ = try F.apply(&primary_db, &first_candidate, binding, .begin);
    _ = try F.apply(&peer_db, &second_candidate, binding, .begin);
    const frozen = try F.apply(&primary_db, &first_candidate, binding, .freeze);
    try std.testing.expectEqualDeep(frozen, try F.apply(&peer_db, &second_candidate, binding, .freeze));
    var binary_cursor_seen = false;
    var eof_page_seen = false;
    for (0..8) |turn| {
        var prepared = try F.job.PreparedPage.prepare(alloc, primary_db.core, binding, .{ .records = 2, .time_ns = std.time.ns_per_s }, .{});
        var prepared_live = true;
        defer if (prepared_live) prepared.deinit();
        const encoded_command = try (try prepared.command()).encode(alloc);
        defer alloc.free(encoded_command);
        const decoded_command = try F.job.PageCommand.decode(encoded_command);
        if (std.mem.indexOfScalar(u8, decoded_command.boundary.after, 255) != null) binary_cursor_seen = true;
        if (!decoded_command.boundary.more and decoded_command.boundary.records_examined == 0) eof_page_seen = true;
        encoded_command[encoded_command.len - 1] ^= 1;
        try std.testing.expectError(error.InvalidSchemaValidationPageCommand, F.job.PageCommand.decode(encoded_command));
        encoded_command[encoded_command.len - 1] ^= 1;
        if (turn == 0) {
            const old = try F.current(&peer_db);
            const clock_before = try F.clock(&peer_db);
            var forged_eof = decoded_command;
            forged_eof.boundary.more = false;
            try std.testing.expectError(error.InvalidNullabilityReplay, F.job.PreparedPage.replay(alloc, peer_db.core, forged_eof, .{}));
            var forged_count = decoded_command;
            forged_count.boundary.rows_examined = 0;
            try std.testing.expectError(error.InvalidNullabilityReplay, F.job.PreparedPage.replay(alloc, peer_db.core, forged_count, .{}));
            var forged_violation = decoded_command;
            forged_violation.boundary.violation_column = 0;
            try std.testing.expectError(error.InvalidNullabilityReplay, F.job.PreparedPage.replay(alloc, peer_db.core, forged_violation, .{}));
            var stale = decoded_command;
            stale.expected[0] ^= 1;
            try std.testing.expectError(error.SchemaValidationJobChanged, F.job.PreparedPage.replay(alloc, peer_db.core, stale, .{}));
            try std.testing.expectEqualDeep(old, try F.current(&peer_db));
            try std.testing.expectEqual(clock_before, try F.clock(&peer_db));
            {
                var reader = try NullabilityScanFixture.open(&peer_db, alloc, .{}, 0);
                defer reader.deinit();
                var plan = try NullabilityScanFixture.projection(&reader, &.{"n"});
                defer plan.deinit();
                const Cancel = struct {
                    calls: usize = 0,
                    fn check(raw: ?*anyopaque) !void {
                        const self: *@This() = @ptrCast(@alignCast(raw.?));
                        self.calls += 1;
                        if (self.calls == 9) return error.Canceled;
                    }
                };
                var cancellation: Cancel = .{};
                try std.testing.expectError(error.Canceled, reader.nextNullabilityReplayPage(alloc, null, decoded_command.boundary, &plan, .{ .context = &cancellation, .checkpoint = Cancel.check }));
                try std.testing.expectError(error.InvalidNullabilityReplay, reader.nextNullabilityReplayPage(alloc, null, forged_eof.boundary, &plan, .{}));
                try std.testing.expectEqual(@as(usize, 0), reader.after.items.len);
                try std.testing.expect(!reader.done and !reader.nullability_failed and reader.nullability_projection == null);
                var verified = try reader.nextNullabilityReplayPage(alloc, null, decoded_command.boundary, &plan, .{});
                defer verified.deinit();
                // Replay's normal time budget is deliberately 1ns; it must
                // still execute this whole recorded two-family prefix.
                try std.testing.expectEqual(@as(usize, 2), verified.rows_examined);
                try std.testing.expectEqualSlices(u8, decoded_command.boundary.after, verified.after);
            }
            const ReplayFaults = struct {
                fn run(a: std.mem.Allocator, database: *db_mod.DB, command_value: F.job.PageCommand) !void {
                    var vtable = a.vtable.*;
                    vtable.resize = std.mem.Allocator.noResize;
                    vtable.remap = std.mem.Allocator.noRemap;
                    const deterministic: std.mem.Allocator = .{ .ptr = a.ptr, .vtable = &vtable };
                    var replayed = try F.job.PreparedPage.replay(deterministic, database.core, command_value, .{});
                    defer replayed.deinit();
                    try std.testing.expectEqual(command_value.boundary.rows_examined, replayed.page.rows_examined);
                }
            };
            try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, ReplayFaults.run, .{ &peer_db, decoded_command });
            try std.testing.expectEqualDeep(old, try F.current(&peer_db));
            try std.testing.expectEqual(clock_before, try F.clock(&peer_db));
        }
        const advanced = try F.page(&primary_db, &prepared);
        prepared.deinit();
        prepared_live = false;
        // The transport bytes own the cursor; no original page or parser
        // arena survives to accidentally substitute for replica verification.
        var replayed = try F.job.PreparedPage.replay(alloc, peer_db.core, decoded_command, .{});
        defer replayed.deinit();
        try std.testing.expectEqualDeep(advanced, try F.page(&peer_db, &replayed));
        if (advanced.phase == .ready) {
            try std.testing.expectEqual(@as(u64, 3), advanced.rows_scanned);
            try std.testing.expectEqual(@as(u64, 4), advanced.records);
            break;
        }
    } else return error.ReplayedValidationDidNotComplete;
    try std.testing.expect(binary_cursor_seen and eof_page_seen);
}

test "relational index system nullability job native metadata journal preserves owner cuts through recovery and standby replay" {
    const F = NullabilityJobFixture;
    const owner = @import("relational_schema_owner_control.zig");
    const contract = @import("replication_contract.zig");
    const outboxes = @import("durable_outbox.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Publisher = struct {
        base: contract.AsyncEffectMirror,
        barrier: *Barrier,
        fail_before: bool = true,
        fail_after: bool = false,
        async_only: bool = false,
        completions: usize = 0,
        reject_admission: bool = false,
        fn selfOf(mirror: contract.AsyncEffectMirror) *@This() {
            return @ptrCast(@alignCast(mirror.publisher.ptr));
        }
        fn next(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.base.publisher.nextLsn();
        }
        fn identity(ptr: *anyopaque) contract.Publisher.Identity {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.base.publisher.identity();
        }
        fn publish(mirror: contract.AsyncEffectMirror, kind: outboxes.Kind, payload: []const u8, namespace: @FieldType(owner.Native, "namespace")) !u64 {
            const self = selfOf(mirror);
            return self.base.publisher.publish(self.base, kind, payload, namespace);
        }
        fn recover(mirror: contract.AsyncEffectMirror, kind: outboxes.Kind, pending: outboxes.DurableReplicationOutbox, namespace: @FieldType(owner.Native, "namespace")) !u64 {
            const self = selfOf(mirror);
            if (self.fail_before) return error.InjectedSchemaJournalUnavailable;
            const result = try self.base.publisher.recover(self.base, kind, pending, namespace);
            if (self.fail_after) {
                self.fail_after = false;
                return error.InjectedSchemaJournalAcknowledgement;
            }
            return result;
        }
        fn preflight(mirror: contract.AsyncEffectMirror, _: bool) !void {
            const self = selfOf(mirror);
            try self.base.publisher.preflight(self.base);
        }
        fn complete(mirror: contract.AsyncEffectMirror, lsn: u64) !void {
            const self = selfOf(mirror);
            var capture = self.barrier.tryAcquireExclusive() orelse return error.CheckpointCaptureBlockedByCompletionWait;
            capture.release();
            try self.base.publisher.complete(self.base, lsn);
            self.completions += 1;
        }
        fn admit(mirror: contract.AsyncEffectMirror, request: contract.Publisher.RecordAdmission) !void {
            const self = selfOf(mirror);
            if (self.reject_admission) return error.WalRecordTooLarge;
            try self.base.publisher.admitRecord(self.base, request);
        }
        fn asMirror(self: *@This()) contract.AsyncEffectMirror {
            return .{ .publisher = .{ .ptr = self, .vtable = &.{ .next_lsn = next, .identity = identity, .publish = publish, .recover = recover, .preflight = preflight, .complete = complete, .admit_record = admit } }, .mutation_barrier = self.barrier, .requirements = .{ .synchronous = !self.async_only, .durable_outbox = !self.async_only, .preflight = !self.async_only } };
        }
    };
    const Driver = struct {
        fn command(database: *db_mod.DB, value: owner.Command) !owner.Outcome {
            var prepared = try database.prepareNativeSchemaOwnerControl(value, .{});
            defer prepared.deinit();
            return database.applyPreparedNativeSchemaOwnerControl(&prepared, null);
        }
        fn replayTo(journal: *primary_mod.Primary, database: *db_mod.DB, next_lsn: *u64) !void {
            while (next_lsn.* <= journal.lastLsn()) : (next_lsn.* += 1) {
                var entry = (try journal.log.entryAt(alloc, next_lsn.*)) orelse return error.MissingReplicationRecord;
                defer entry.deinit(alloc);
                if (entry.record.kind == .metadata_mutation) {
                    try std.testing.expectEqual(@import("replication_record.zig").PayloadCodec.binary, entry.record.payload_codec);
                    const frame = try owner.Frame.decode(entry.record.payload);
                    try std.testing.expectEqualDeep(F.namespace, frame.position.namespace);
                }
                try replication_ingress.applyRecord(database, entry.record);
                const clock = try F.clock(database);
                try replication_ingress.applyRecord(database, entry.record);
                try std.testing.expectEqual(clock, try F.clock(database));
            }
        }
    };
    for ([_]bool{ false, true }) |cancel_only| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("native-schema-journal");
        defer directory.cleanup();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const owned = arena.allocator();
        const source_path = try std.fmt.allocPrint(owned, "{s}/source", .{directory.path()});
        const peer_path = try std.fmt.allocPrint(owned, "{s}/peer", .{directory.path()});
        var journal = try primary_mod.Primary.open(alloc, try std.fmt.allocPrintSentinel(owned, "{s}/log", .{directory.path()}, 0), try std.fmt.allocPrintSentinel(owned, "{s}/slots", .{directory.path()}, 0), .{ .cluster_id = 71, .table_id = F.namespace.table_id, .shard_id = F.namespace.shard_id, .timeline_id = 1, .epoch = 1 }, .{});
        defer journal.close();
        // A real non-owner record separates journal ordering from the native
        // authority clock; equality by coincidence is not domain identity.
        _ = try journal.append(.{ .kind = .checkpoint, .payload_codec = .raw, .payload = "" });
        var barrier: Barrier = .{};
        var publisher: Publisher = .{ .base = hot_standby_publisher_adapter.bindMirror(&journal, .{}), .barrier = &barrier, .async_only = cancel_only };
        var primary_db = try db_mod.DB.open(alloc, source_path, F.options);
        var primary_live = true;
        defer if (primary_live) primary_db.close();
        var peer_db = try db_mod.DB.open(alloc, peer_path, F.options);
        var peer_live = true;
        defer if (peer_live) peer_db.close();
        // Same pre-control snapshot. This qualifies owner-control replication,
        // not uncoordinated bootstrap/adoption of independently written roots.
        for ([_]*db_mod.DB{ &primary_db, &peer_db }) |database| {
            try database.setSchemaJson(alloc, F.before);
            try database.batch(.{ .timestamp_ns = 100, .writes = &.{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "b", .value = "{\"n\":2}" }, .{ .key = "c", .value = "{\"n\":3}" } } });
            const orphan = try internal.ttlKeyAlloc(alloc, "orphan-\xff");
            defer alloc.free(orphan);
            const stamp: [8]u8 = @splat(0);
            try database.core.store.put(orphan, &stamp);
        }
        var candidate = try F.candidate(&primary_db);
        defer candidate.deinit();
        const binding = try F.binding(&primary_db, &candidate, 61);
        var mirrored = F.options;
        mirrored.replication_async_metadata_mirror = publisher.asMirror();
        mirrored.replication_async_batch_mirror = publisher.asMirror();
        primary_db.local_execution.replication_async_metadata_mirror = publisher.asMirror();
        primary_db.local_execution.replication_async_batch_mirror = publisher.asMirror();
        const begin: owner.Command = .{ .binding = binding, .action = .begin, .payload = candidate.inputs };
        const before_admission_clock = try F.clock(&primary_db);
        {
            var legacy = publisher.asMirror();
            var legacy_vtable = legacy.publisher.vtable.*;
            legacy_vtable.admit_record = null;
            legacy.publisher.vtable = &legacy_vtable;
            primary_db.local_execution.replication_async_metadata_mirror = legacy;
            primary_db.local_execution.replication_async_batch_mirror = legacy;
            try std.testing.expectError(error.UnsupportedReplicationRecordAdmission, Driver.command(&primary_db, begin));
        }
        primary_db.local_execution.replication_async_metadata_mirror = publisher.asMirror();
        primary_db.local_execution.replication_async_batch_mirror = publisher.asMirror();
        publisher.reject_admission = true;
        try std.testing.expectError(error.WalRecordTooLarge, Driver.command(&primary_db, begin));
        publisher.reject_admission = false;
        try std.testing.expectEqual(before_admission_clock, try F.clock(&primary_db));
        {
            var read = try primary_db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try F.job.load(&read) == null);
            try std.testing.expect(!try @import("durable_outbox_store.zig").hasPendingTxn(&read));
        }
        const admitted = try Driver.command(&primary_db, begin);
        try std.testing.expect(admitted.replication_pending and !admitted.duplicate);
        try std.testing.expectEqualStrings("InjectedSchemaJournalUnavailable", admitted.error_name.?);
        try std.testing.expectEqual(@as(u64, 1), journal.lastLsn());
        primary_db.local_execution.durable_replication_outbox_maybe.store(false, .release);
        primary_db.async_context.primary_replication_outbox_pending.store(false, .release);
        const pending_observation = try primary_db.observeNativeSchemaOwner(binding, .{});
        try pending_observation.validateFor(binding);
        try std.testing.expect(pending_observation.replication_pending);
        try std.testing.expectEqual(primary_db.root_incarnation, pending_observation.physical_root_incarnation);
        try std.testing.expectEqualDeep(admitted.header.?, pending_observation.header.?);
        try std.testing.expectEqual(@as(u64, 1), journal.lastLsn());
        const original_cut = admitted.position;
        primary_db.close();
        primary_live = false;
        primary_db = try db_mod.DB.open(alloc, source_path, mirrored);
        primary_live = true;
        publisher.fail_before = false;
        try primary_db.flushDurableReplicationOutboxes();
        try std.testing.expectEqual(@as(u64, 2), journal.lastLsn());
        const duplicate = try Driver.command(&primary_db, begin);
        try std.testing.expect(duplicate.duplicate and !duplicate.replication_pending);
        try std.testing.expectEqualDeep(original_cut, duplicate.position);
        try std.testing.expectEqual(@as(u64, 2), journal.lastLsn());
        var next_lsn: u64 = 1;
        try Driver.replayTo(&journal, &peer_db, &next_lsn);
        try std.testing.expectEqualDeep(try F.current(&primary_db), try F.current(&peer_db));
        try std.testing.expect(original_cut.sequence != journal.lastLsn());
        const frozen = try Driver.command(&primary_db, .{ .binding = binding, .action = .freeze });
        try std.testing.expect(!frozen.duplicate and !frozen.replication_pending);
        // A committed frame cannot skip a native clock cut or change routes.
        var original_entry = (try journal.log.entryAt(alloc, journal.lastLsn())).?;
        defer original_entry.deinit(alloc);
        const original_frame = try owner.Frame.decode(original_entry.record.payload);
        try std.testing.expect(try journal.findMatchingRecordFromWithCodec(1, .metadata_mutation, .json, original_entry.record.payload, F.namespace.shard_id, F.namespace.table_id) == null);
        try std.testing.expectEqual(@as(?u64, original_entry.record.lsn), try journal.findMatchingRecordFromWithCodec(1, .metadata_mutation, .binary, original_entry.record.payload, F.namespace.shard_id, F.namespace.table_id));
        var gap = original_frame;
        gap.position.sequence += 7;
        const gap_bytes = try gap.encode(alloc);
        defer alloc.free(gap_bytes);
        var bad_record = original_entry.record;
        bad_record.payload = gap_bytes;
        const prior_clock = try F.clock(&peer_db);
        try std.testing.expectError(error.InvalidControlReceiptPosition, peer_db.applyReplicatedSchemaOwnerControl(bad_record));
        try std.testing.expectEqual(prior_clock, try F.clock(&peer_db));
        bad_record = original_entry.record;
        bad_record.table_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, peer_db.applyReplicatedSchemaOwnerControl(bad_record));
        try Driver.replayTo(&journal, &peer_db, &next_lsn);
        for (0..8) |turn| {
            var page = try primary_db.prepareNativeSchemaOwnerPage(binding, .{ .records = 2, .time_ns = std.time.ns_per_s }, .{});
            defer page.deinit();
            publisher.fail_after = turn == 0;
            const advanced = try primary_db.applyPreparedNativeSchemaOwnerControl(&page, null);
            if (turn == 0) {
                try std.testing.expect(advanced.replication_pending);
                const recorded = journal.lastLsn();
                const clock = try F.clock(&primary_db);
                try primary_db.flushDurableReplicationOutboxes();
                try std.testing.expectEqual(recorded, journal.lastLsn());
                try std.testing.expectEqual(clock, try F.clock(&primary_db));
            } else try std.testing.expect(!advanced.replication_pending);
            try Driver.replayTo(&journal, &peer_db, &next_lsn);
            try std.testing.expectEqualDeep(try F.current(&primary_db), try F.current(&peer_db));
            if (cancel_only or advanced.header.?.phase == .ready) break;
        } else return error.NativeSchemaValidationDidNotFinish;
        if (cancel_only) {
            publisher.fail_before = true;
            const canceled = try Driver.command(&primary_db, .{ .binding = binding, .action = .cancel });
            try std.testing.expectEqual(F.job.Phase.canceled, canceled.header.?.phase);
            try std.testing.expect(canceled.replication_pending);
            // Local cancellation released the table fence, but an unappended
            // control must still close foreground writes until journal order
            // is durable. No later row may overtake this cancellation.
            var blocked = false;
            primary_db.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":null}" }} }) catch |err| {
                try std.testing.expect(err == error.InjectedSchemaJournalUnavailable or err == error.ReplicationPublisherUnavailable);
                blocked = true;
            };
            try std.testing.expect(blocked);
            const absent = try internal.relationalRowKeyAlloc(alloc, "z");
            defer alloc.free(absent);
            try std.testing.expectError(error.NotFound, primary_db.core.store.get(alloc, absent));
            publisher.fail_before = false;
            // Ordinary row admission must recover before taking its shared
            // mutation lease: completion above needs checkpoint-exclusive.
            try primary_db.batch(.{ .timestamp_ns = 200, .writes = &.{.{ .key = "z", .value = "{\"n\":null}" }} });
            const retried_cancel = try Driver.command(&primary_db, .{ .binding = binding, .action = .cancel });
            try std.testing.expect(retried_cancel.duplicate and !retried_cancel.replication_pending);
            try std.testing.expectEqualDeep(canceled.position, retried_cancel.position);
            try Driver.replayTo(&journal, &peer_db, &next_lsn);
            try std.testing.expectError(error.SchemaValidationCanceled, Driver.command(&primary_db, begin));
        } else {
            const validated = try F.current(&primary_db);
            var install_body: [64]u8 = undefined;
            @memset(install_body[0..32], 91);
            @memcpy(install_body[32..64], &try validated.digest());
            const install_command: owner.Command = .{ .binding = binding, .action = .install, .payload = &install_body };
            const installed = try Driver.command(&primary_db, install_command);
            try std.testing.expectEqual(F.job.Phase.installed, installed.header.?.phase);
            try Driver.replayTo(&journal, &peer_db, &next_lsn);
            const end_lsn = journal.lastLsn();
            const repeated = try Driver.command(&primary_db, install_command);
            try std.testing.expect(repeated.duplicate and !repeated.replication_pending);
            try std.testing.expectEqualDeep(installed.position, repeated.position);
            try std.testing.expectEqual(end_lsn, journal.lastLsn());
            try std.testing.expectError(error.SchemaValidationPublicationRequired, Driver.command(&primary_db, .{ .binding = binding, .action = .cancel }));
            try std.testing.expectEqual(@as(u32, 2), peer_db.core.schema.?.version);
            const AllocationProbe = struct {
                parent: std.mem.Allocator,
                database: *db_mod.DB,
                checks: usize = 0,
                blocked: usize = 0,
                fn check(raw: *anyopaque) *@This() {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.checks += 1;
                    if (self.database.core.tryLockApplyExclusive()) self.database.core.unlockApply() else self.blocked += 1;
                    return self;
                }
                fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, return_address: usize) ?[*]u8 {
                    return check(raw).parent.rawAlloc(len, alignment, return_address);
                }
                fn resizeMemory(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, return_address: usize) bool {
                    return check(raw).parent.rawResize(memory, alignment, len, return_address);
                }
                fn remapMemory(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, return_address: usize) ?[*]u8 {
                    return check(raw).parent.rawRemap(memory, alignment, len, return_address);
                }
                fn freeMemory(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, return_address: usize) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.parent.rawFree(memory, alignment, return_address);
                }
                fn allocator(self: *@This()) std.mem.Allocator {
                    return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resizeMemory, .remap = remapMemory, .free = freeMemory } };
                }
            };
            var probe: AllocationProbe = .{ .parent = primary_db.alloc, .database = &primary_db };
            const installed_observation = blk: {
                const prior_allocator = primary_db.alloc;
                primary_db.alloc = probe.allocator();
                defer primary_db.alloc = prior_allocator;
                break :blk try primary_db.observeNativeSchemaOwner(binding, .{});
            };
            try std.testing.expectEqualDeep(installed.header.?, installed_observation.header.?);
            try std.testing.expect(probe.checks > 0);
            try std.testing.expectEqual(@as(usize, 0), probe.blocked);
            try std.testing.expectEqual(end_lsn, journal.lastLsn());
        }
        try std.testing.expectEqualDeep(try F.current(&primary_db), try F.current(&peer_db));
        try std.testing.expectEqual(try F.clock(&primary_db), try F.clock(&peer_db));
        try std.testing.expect(try primary_db.orderedApplyReceipt() == null);
        try std.testing.expect(try peer_db.orderedApplyReceipt() == null);
        peer_db.close();
        peer_live = false;
        peer_db = try db_mod.DB.open(alloc, peer_path, F.options);
        peer_live = true;
        try std.testing.expectEqualDeep(try F.current(&primary_db), try F.current(&peer_db));
        try std.testing.expectEqual(try F.clock(&primary_db), try F.clock(&peer_db));
        try std.testing.expect(publisher.completions > 0);
    }
}

test "relational index system nullability job publisher admission uses the actual WAL static limit" {
    const contract = @import("replication_contract.zig");
    const record = @import("replication_record.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("schema-journal-admission");
    defer directory.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(alloc, "{s}/log", .{directory.path()}, 0);
    defer alloc.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(alloc, "{s}/slots", .{directory.path()}, 0);
    defer alloc.free(slots_path);
    var journal = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 71, .table_id = 72, .shard_id = 73, .timeline_id = 1, .epoch = 1 }, .{
        .replication_log_options = .{ .wal_options = .{ .lsm_options = .{ .wal_hard_limit_bytes = 4096 } } },
    });
    defer journal.close();
    const mirror = hot_standby_publisher_adapter.bindMirror(&journal, .{});
    const request: contract.Publisher.RecordAdmission = .{
        .kind = .metadata_control,
        .codec = .binary,
        .payload_bytes = 100,
        .namespace = .{ .table_id = 72, .shard_id = 73, .range_id = 74 },
    };
    try mirror.publisher.admitRecord(mirror, request);
    var large = request;
    large.payload_bytes = 4096;
    try std.testing.expectError(error.WalRecordTooLarge, mirror.publisher.admitRecord(mirror, large));
    large.payload_bytes = std.math.maxInt(usize);
    try std.testing.expectError(error.UnsupportedReplicationRecordAdmission, mirror.publisher.admitRecord(mirror, large));
    var wrong = request;
    wrong.codec = .json;
    try std.testing.expectError(error.UnsupportedReplicationRecordAdmission, mirror.publisher.admitRecord(mirror, wrong));
    wrong = request;
    wrong.kind = .schema;
    try std.testing.expectError(error.UnsupportedReplicationRecordAdmission, mirror.publisher.admitRecord(mirror, wrong));
    wrong = request;
    wrong.namespace.table_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, mirror.publisher.admitRecord(mirror, wrong));
    try std.testing.expectEqual(@as(u64, 0), journal.lastLsn());
    // The size-only admission actually agrees with a physical append, not an
    // unrelated guessed frame cap. Its exact boundary includes both WAL keys,
    // next-LSN metadata and the physical state codec's entry framing.
    const overhead = 77 + record.header_size;
    var boundary = request;
    boundary.payload_bytes = 4096 - overhead;
    try mirror.publisher.admitRecord(mirror, boundary);
    boundary.payload_bytes += 1;
    try std.testing.expectError(error.WalRecordTooLarge, mirror.publisher.admitRecord(mirror, boundary));
    const body = try alloc.alloc(u8, 4096 - overhead);
    defer alloc.free(body);
    @memset(body, 7);
    _ = try journal.append(.{ .kind = .checkpoint, .payload_codec = .binary, .payload = body });
    try std.testing.expectEqual(@as(u64, 1), journal.lastLsn());
    try std.testing.expectError(error.WalRecordTooLarge, journal.log.wal.admitAppendBytes(std.math.maxInt(usize)));
}

test "relational index system nullability job observation binds physical root debt and native authority without mutation" {
    const F = NullabilityJobFixture;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("schema-owner-observation");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), F.options);
    defer database.close();
    try database.setSchemaJson(alloc, F.before);
    var candidate = try F.candidate(&database);
    defer candidate.deinit();
    const binding = try F.binding(&database, &candidate, 62);
    const before = try database.observeNativeSchemaOwner(binding, .{});
    try before.validateFor(binding);
    try std.testing.expect(before.header == null and !before.replication_pending);
    try std.testing.expectEqual(@as(u64, 0), before.sequence);
    try std.testing.expectEqual(database.root_incarnation, before.physical_root_incarnation);
    _ = try F.apply(&database, &candidate, binding, .begin);
    const admitted = try database.observeNativeSchemaOwner(binding, .{});
    try std.testing.expectEqual(F.job.Phase.fencing, admitted.header.?.phase);
    try std.testing.expectEqual(try F.clock(&database), admitted.sequence);
    const decoded = blk: {
        const encoded = try std.json.Stringify.valueAlloc(alloc, admitted, .{});
        defer alloc.free(encoded);
        var parsed = try std.json.parseFromSlice(@TypeOf(admitted), alloc, encoded, .{});
        defer parsed.deinit();
        break :blk parsed.value;
    };
    try decoded.validateFor(binding);
    try std.testing.expectEqualDeep(admitted, decoded);
    var changed = binding;
    changed.plan_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, database.observeNativeSchemaOwner(changed, .{}));
    changed = binding;
    changed.fence.namespace.range_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, database.observeNativeSchemaOwner(changed, .{}));
    changed = binding;
    changed.authority = .raft;
    try std.testing.expectError(error.InvalidControlReceiptPosition, database.observeNativeSchemaOwner(changed, .{}));
    try std.testing.expectError(error.DeadlineExceeded, database.observeNativeSchemaOwner(binding, .{ .deadline_ns = 0 }));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, database.observeNativeSchemaOwner(binding, .{ .cancellation = @import("../../api/operation.zig").CancellationToken.fromAtomic(&canceled) }));
    try std.testing.expectEqual(admitted.sequence, try F.clock(&database));
    var forged = admitted;
    forged.sequence = 0;
    try std.testing.expectError(error.SchemaValidationJobChanged, forged.validateFor(binding));
    forged = admitted;
    forged.physical_root_incarnation = 0;
    try std.testing.expectError(error.SchemaValidationJobChanged, forged.validateFor(binding));
    const marker = @import("../internal_keys.zig").ordered_document_applied_entry_key;
    // The storage participant itself refuses a Raft marker in this native
    // domain; do not bypass that earlier authority guard to set up a fixture.
    try std.testing.expectError(error.OnlineSourceScopeChanged, database.core.store.put(&marker, ""));
    try std.testing.expectEqualDeep(admitted, try database.observeNativeSchemaOwner(binding, .{}));
}

const NullabilityScanFixture = struct {
    const validation = @import("relational_nullability.zig");
    fn open(database: *db_mod.DB, a: std.mem.Allocator, request: rows.Request, now_ns: u64) !rows.Reader {
        database.core.lockApplyShared();
        defer database.core.unlockApplyShared();
        var view = database.core.acquireSchemaView() orelse return error.RelationalTableRequired;
        defer view.release();
        return rows.Reader.open(a, database.core.store, view, null, request, now_ns);
    }
    fn projection(reader: *rows.Reader, names: []const []const u8) !validation.Plan {
        return validation.Plan.init(alloc, reader.active.tableSchema().*, reader.active.physicalLayout(), names, .{});
    }
    fn finish(reader: *rows.Reader, plan: *const validation.Plan) !usize {
        var count: usize = 0;
        for (0..32) |_| {
            const previous = try alloc.dupe(u8, reader.after.items);
            defer alloc.free(previous);
            var page = try reader.nextNullabilityPage(alloc, null, .{ .records = 1, .time_ns = std.time.ns_per_s }, plan, .{});
            defer page.deinit();
            try std.testing.expectEqual(@as(?usize, null), page.violation_column);
            try std.testing.expectEqual(@as(usize, 0), page.page.rows.len);
            try std.testing.expect(page.page.records_examined <= 1);
            count += page.rows_examined;
            if (!page.page.more) return count;
            try std.testing.expect(std.mem.order(u8, page.after, previous) == .gt);
        }
        return error.NullabilityScanDidNotFinish;
    }
};

test "relational index system nullability scan bounds physical pages retains snapshots and resumes exact projections" {
    const Check = NullabilityScanFixture;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-pages");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } };
    {
        var database = try db_mod.DB.open(alloc, directory.path(), options);
        defer database.close();
        try database.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":true},"j":{"type":"json","nullable":true},"body":{"type":"string"}},"additionalProperties":false}}}}
        );
        const payload = try alloc.alloc(u8, 128 * 1024);
        defer alloc.free(payload);
        @memset(payload, 'x');
        const large = try std.fmt.allocPrint(alloc, "{{\"n\":1,\"j\":null,\"body\":\"{s}\"}}", .{payload});
        defer alloc.free(large);
        try database.batch(.{ .timestamp_ns = 100, .writes = &.{
            .{ .key = "a", .value = large, .json_null_fields = &.{"j"} },
            .{ .key = "b", .value = "{\"n\":2,\"j\":{}}" },
            .{ .key = "c", .value = "{\"n\":3,\"j\":[]}" },
            .{ .key = "d", .value = "{\"n\":4,\"j\":false}" },
        } });
        for ([_][]const u8{ "0-orphan", "aa-orphan", "z-orphan" }) |name| {
            const key = try internal.ttlKeyAlloc(alloc, name);
            defer alloc.free(key);
            const stamp: [8]u8 = @splat(0);
            try database.core.store.put(key, &stamp);
        }
        try database.setSchemaJson(alloc,
            \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"ahead":{"type":"string","nullable":true},"n":{"type":"integer","nullable":true},"j":{"type":"json","nullable":true},"body":{"type":"string"}},"additionalProperties":false}}}}
        );
    }
    // The source rows are now cold persisted LSM data.
    var database = try db_mod.DB.open(alloc, directory.path(), options);
    defer database.close();
    var reader = try Check.open(&database, alloc, .{}, 1000);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), reader.active.version());
    var twin = try rows.Reader.openSnapshot(alloc, try reader.read.forkRead(), reader.authenticated, reader.active, null, .{}, 1000);
    defer twin.deinit();
    var plan = try Check.projection(&reader, &.{ "n", "j" });
    defer plan.deinit();
    const primary = try internal.relationalRowKeyAlloc(alloc, "a");
    defer alloc.free(primary);
    const before = try database.core.store.get(alloc, primary);
    defer alloc.free(before);
    try std.testing.expectEqual(@as(u32, 1), try @import("algebraic/relational_row_codec.zig").rowSchemaVersion(before));
    {
        var first = try reader.nextNullabilityPage(alloc, null, .{ .records = 1, .time_ns = std.time.ns_per_s }, &plan, .{});
        defer first.deinit();
        try std.testing.expect(first.page.more);
        try std.testing.expectEqual(@as(usize, 0), first.rows_examined);
        try std.testing.expectEqual(@as(usize, 0), first.page.rows.len);
        try std.testing.expect(first.after.len != 0);
        try twin.resumeNullabilityScan(&plan, .{ .projection_identity = plan.identity, .after = first.after }, .{});
    }
    // Live writes must not alter either retained source cut. This is a read
    // isolation test, not a claim that a snapshot alone fences publication.
    try database.batch(.{ .timestamp_ns = 200, .writes = &.{
        .{ .key = "b", .value = "{\"n\":null,\"j\":{}}" },
        .{ .key = "e", .value = "{\"n\":null,\"j\":{}}" },
    } });
    var different = try Check.projection(&reader, &.{ "j", "n" });
    defer different.deinit();
    try std.testing.expectError(error.InvalidNullabilityProjection, reader.nextNullabilityPage(alloc, null, .{}, &different, .{}));
    try std.testing.expectEqual(@as(usize, 4), try Check.finish(&reader, &plan));
    try std.testing.expectEqual(@as(usize, 4), try Check.finish(&twin, &plan));
    try std.testing.expectError(error.InvalidNullabilityProjection, reader.nextTypedPage(alloc, null, .{}));
    try std.testing.expectError(error.InvalidNullabilityProjection, reader.lookupTypedRow(alloc, "a"));
    const after = try database.core.store.get(alloc, primary);
    defer alloc.free(after);
    try std.testing.expectEqualSlices(u8, before, after);

    // A column added in the active epoch is absent in historical frames, not
    // the value of whichever old column happened to occupy its new ordinal.
    var historical = try Check.open(&database, alloc, .{}, 1000);
    defer historical.deinit();
    var added = try Check.projection(&historical, &.{"ahead"});
    defer added.deinit();
    var absent = try historical.nextNullabilityPage(alloc, null, .{ .records = 32, .time_ns = std.time.ns_per_s }, &added, .{});
    defer absent.deinit();
    try std.testing.expectEqual(@as(?usize, 0), absent.violation_column);
    try std.testing.expectEqualStrings("a", absent.page.rows[0].key);
    try std.testing.expectEqual(@as(u32, 1), absent.page.rows[0].schema_version);

    var current = try Check.open(&database, alloc, .{}, 1000);
    defer current.deinit();
    var failed = try current.nextNullabilityPage(alloc, null, .{ .records = 32, .time_ns = std.time.ns_per_s }, &plan, .{});
    defer failed.deinit();
    try std.testing.expectEqual(@as(?usize, 0), failed.violation_column);
    try std.testing.expectEqual(@as(usize, 2), failed.rows_examined);
    try std.testing.expectEqual(@as(usize, 1), failed.page.rows.len);
    try std.testing.expectEqualStrings("b", failed.page.rows[0].key);
    try std.testing.expectEqual(@as(usize, 0), failed.page.rows[0].typed.?.object.count());
    try std.testing.expectEqual(@as(usize, 0), failed.page.rows[0].json.len);
    try std.testing.expectError(error.NullabilityValidationFailed, current.nextNullabilityPage(alloc, null, .{}, &plan, .{}));

    var fresh = try Check.open(&database, alloc, .{}, 1000);
    defer fresh.deinit();
    var forged = plan.identity;
    forged[0] ^= 1;
    try std.testing.expectError(error.InvalidNullabilityProjection, fresh.resumeNullabilityScan(&plan, .{ .projection_identity = forged, .after = failed.after }, .{}));
    try std.testing.expectError(error.InvalidNullabilityProjection, fresh.resumeNullabilityScan(&plan, .{ .projection_identity = plan.identity, .after = "\xff" }, .{}));
    try std.testing.expectEqual(@as(usize, 0), fresh.after.items.len);
}

test "relational index system nullability scan rejects filtered coverage includes expired rows and rolls back failed pages" {
    const Check = NullabilityScanFixture;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("nullability-coverage");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } });
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","ttl_duration_ns":1,"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":true},"_timestamp":{"type":"datetime"}},"additionalProperties":false}}}}
    );
    try database.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"n\":null}" }} });
    var normal = try Check.open(&database, alloc, .{}, 1000);
    defer normal.deinit();
    var hidden = try normal.nextTypedPage(alloc, null, .{});
    defer hidden.deinit();
    try std.testing.expectEqual(@as(usize, 0), hidden.rows.len);
    var reader = try Check.open(&database, alloc, .{}, 1000);
    defer reader.deinit();
    var plan = try Check.projection(&reader, &.{"n"});
    defer plan.deinit();
    try std.testing.expectError(error.InvalidNullabilityProjection, normal.nextNullabilityPage(alloc, null, .{}, &plan, .{}));
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 6) return error.Canceled;
        }
    };
    var cancellation: Cancel = .{};
    try std.testing.expectError(error.Canceled, reader.nextNullabilityPage(alloc, null, .{}, &plan, .{ .context = &cancellation, .checkpoint = Cancel.check }));
    try std.testing.expect(reader.source != null);
    try std.testing.expectEqual(@as(usize, 0), reader.after.items.len);
    try std.testing.expect(reader.nullability_projection == null);
    try std.testing.expect(!reader.nullability_failed);
    var found = try reader.nextNullabilityPage(alloc, null, .{}, &plan, .{});
    defer found.deinit();
    try std.testing.expectEqual(@as(?usize, 0), found.violation_column);
    try std.testing.expectEqualStrings("a", found.page.rows[0].key);
    try std.testing.expectEqual(@as(usize, 1), found.rows_examined);

    for ([_]rows.Request{
        .{ .fields = &.{"n"} },
        .{ .conditions = &.{.{ .column = "n", .op = .is_not_null }} },
        .{ .primary_lower = .{ .key = "b" } },
    }) |request| {
        var restricted = try Check.open(&database, alloc, request, 1000);
        defer restricted.deinit();
        try std.testing.expectError(error.InvalidNullabilityProjection, restricted.nextNullabilityPage(alloc, null, .{}, &plan, .{}));
    }
    var public = try database.beginRelationalRows(alloc, .{});
    defer public.deinit();
    try std.testing.expectError(error.InvalidNullabilityProjection, public.nextNullabilityPage(alloc, null, .{}, &plan, .{}));
    const Allocation = struct {
        fn run(a: std.mem.Allocator, db: *db_mod.DB, projection: *const Check.validation.Plan) !void {
            var validation_reader = try Check.open(db, a, .{}, 1000);
            defer validation_reader.deinit();
            var page = validation_reader.nextNullabilityPage(a, null, .{}, projection, .{}) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), validation_reader.after.items.len);
                try std.testing.expect(validation_reader.nullability_projection == null);
                try std.testing.expect(!validation_reader.nullability_failed);
                return err;
            };
            defer page.deinit();
            try std.testing.expectEqualStrings("a", page.page.rows[0].key);
        }
    };
    try Allocation.run(alloc, &database, &plan);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Allocation.run, .{ &database, &plan });
}

test "relational index system Raft owner controls commit authentic phase cuts atomically and recover" {
    const api = @import("../../schema/mod.zig");
    const compiler = @import("../../schema/relational_declarations.zig");
    const catalog = @import("relational_integrity_catalog.zig");
    const candidates = @import("relational_index_candidate.zig");
    const jobs = @import("relational_index_owner_job.zig");
    const raft = @import("relational_index_raft_control.zig");
    const receipts = @import("apply_receipts.zig");
    const before =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const after =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}},"relational_indexes":[{"name":"email_idx","keys":[{"column":"email"}]}],"unique_constraints":[{"name":"email_idx","origin":"index","columns":["email"]}]}
    ;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("raft-private-index-cuts");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{
        .start_optional_runtimes = false,
        .primary_backend = .{ .lsm = .{} },
        .online_source_authority = .raft,
        .identity_namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 },
    };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    var db_live = true;
    defer if (db_live) db.close();
    try db.setSchemaJson(alloc, before);
    var parsed = try api.parseValidatedTableSchema(alloc, before);
    defer parsed.deinit(alloc);
    var prior = try compiler.compileCatalog(alloc, parsed, 101, null);
    defer prior.deinit();
    try db.core.store.putBatch(&.{.{ .key = catalog.key, .value = prior.value }}, &.{});
    try db.applyOrderedCommittedMutation(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":1,\"email\":\"a@example.org\"}" }} }, .{ .term = 1, .index = 1 }, false);
    var candidate = try candidates.Candidate.prepare(alloc, 101, before, after, prior.value, "email_idx");
    defer candidate.deinit();
    const header: raft.Admission = .{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = options.identity_namespace.?, .catalog_digest = @import("relational_index_inputs.zig").digest(prior.value) },
        .candidate_digest = @import("relational_index_inputs.zig").digest(candidate.next.value),
        .reservation_digest = @import("relational_index_inputs.zig").digest(candidate.reservation.value),
    };
    try std.testing.expectError(error.OnlineSourceScopeChanged, db.prepareNativeIndexOwnerAdmission(header, candidate.inputs.bytes));
    var admission = try db.prepareRaftIndexOwnerAdmission(header, candidate.inputs.bytes);
    defer admission.deinit();
    const binding = admission.command.binding;
    const preparation = try db.raftIndexOwnerAdmissionPreparation(11, header, candidate.inputs.bytes);
    try std.testing.expectEqualDeep(binding, preparation.binding);
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, db.raftIndexOwnerAdmissionPreparation(99, header, candidate.inputs.bytes));
    const identity: raft.Identity = .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = binding.fence.owner_group_id };
    try std.testing.expect(try db.raftIndexOwnerState(11, identity) == null);
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, db.raftIndexOwnerState(99, identity));
    var foreign_identity = identity;
    foreign_identity.namespace.range_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, db.raftIndexOwnerState(11, foreign_identity));
    try std.testing.expectError(error.OnlineSourceScopeChanged, db.nativeIndexOwnerState(identity));
    const Wire = struct {
        fn roundtrip(backing: std.mem.Allocator, command: raft.Command) !void {
            const codec = @import("../../api/batch.zig");
            const request: db_mod.types.BatchRequest = .{ .relational_index_owner_control = command, .timestamp_ns = 18446744073709551600 };
            const encoded = try codec.encodeBatchRequest(backing, request);
            defer backing.free(encoded);
            var decoded = try codec.parseInternalBatchRequest(backing, encoded);
            defer decoded.deinit(backing);
            try std.testing.expectEqualDeep(command, decoded.req.relational_index_owner_control.?);
            try std.testing.expectEqual(request.timestamp_ns, decoded.req.timestamp_ns);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Wire.roundtrip, .{admission.command});
    {
        const codec = @import("../../api/batch.zig");
        const request: db_mod.types.BatchRequest = .{ .relational_index_owner_control = admission.command };
        const encoded = try codec.encodeBatchRequest(alloc, request);
        defer alloc.free(encoded);
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseBatchRequest(alloc, encoded));
        const wrapped = try std.fmt.allocPrint(alloc, "[\"row-semantics-batch-v1\",{s}]", .{encoded});
        defer alloc.free(wrapped);
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseInternalBatchRequest(alloc, wrapped));
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(request));
        try std.testing.expectError(error.InvalidBatchRequest, db.applyOrderedCommittedMutation(request, .{ .term = 1, .index = 10 }, false));
        try std.testing.expectError(error.InvalidBatchRequest, server_test_adapter.applyOrdered(&db, request, .{ .term = 1, .index = 10 }));
        try std.testing.expectError(error.InvalidBatchRequest, db.applyReplicatedBatch(.{ .request = request, .receipt = .{ .ordered = .{ .term = 1, .index = 10 } } }, 1));
        for (0..7) |variant| {
            var mixed = request;
            switch (variant) {
                0 => mixed.writes = &.{.{ .key = "smuggled", .value = "{}" }},
                1 => mixed.deletes = &.{"smuggled"},
                2 => mixed.restore_staging_scope = @splat(1),
                3 => mixed.activate_range_tracking = true,
                4 => mixed.schema_version = 1,
                5 => mixed.row_policy_principal_proof = "smuggled",
                6 => mixed.sync_level = .full_index,
                else => unreachable,
            }
            try std.testing.expectError(error.InvalidBatchRequest, codec.encodeBatchRequest(alloc, mixed));
        }
        var parsed_wire = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{ .parse_numbers = false });
        defer parsed_wire.deinit();
        const owned = parsed_wire.arena.allocator();
        const inserts = parsed_wire.value.object.getPtr("inserts").?;
        try inserts.object.put(owned, "row", .{ .string = "{}" });
        var no_allocations: std.heap.FixedBufferAllocator = .init(&.{});
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseInternalBatchValue(no_allocations.allocator(), parsed_wire.value));
        _ = inserts.object.swapRemove("row");
        try parsed_wire.value.object.put(owned, "future_control", .{ .bool = true });
        const unknown = try std.json.Stringify.valueAlloc(alloc, parsed_wire.value, .{});
        defer alloc.free(unknown);
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseInternalBatchRequest(alloc, unknown));
        _ = parsed_wire.value.object.swapRemove("future_control");
        const control = parsed_wire.value.object.getPtr("_relational_index_owner_control").?;
        const original_inputs = control.object.get("inputs_b64").?;
        const oversized = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(@import("relational_index_inputs.zig").max_bytes) + 1);
        defer alloc.free(oversized);
        @memset(oversized, 'A');
        try control.object.put(owned, "inputs_b64", .{ .string = oversized });
        // No derived manifest allocation may occur before rejecting the bound.
        var exhausted: std.heap.FixedBufferAllocator = .init(&.{});
        try std.testing.expectError(error.InvalidPrivateIndexInputs, codec.parseInternalBatchValue(exhausted.allocator(), parsed_wire.value));
        try control.object.put(owned, "inputs_b64", original_inputs);
        try control.object.put(owned, "position", .{ .integer = 999 });
        const caller_cut = try std.json.Stringify.valueAlloc(alloc, parsed_wire.value, .{});
        defer alloc.free(caller_cut);
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseInternalBatchRequest(alloc, caller_cut));
        const alias = control.*;
        _ = parsed_wire.value.object.swapRemove("_relational_index_owner_control");
        try parsed_wire.value.object.put(owned, "relational_index_owner_control", alias);
        const public_alias = try std.json.Stringify.valueAlloc(alloc, parsed_wire.value, .{});
        defer alloc.free(public_alias);
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseBatchRequest(alloc, public_alias));
        try std.testing.expectError(error.InvalidBatchRequest, codec.parseInternalBatchRequest(alloc, public_alias));
        try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    }
    {
        db.local_execution.restore_staging_required.store(true, .release);
        defer db.local_execution.restore_staging_required.store(false, .release);
        try std.testing.expectError(error.RestoreStagingScopeChanged, db.raftIndexOwnerState(11, identity));
        try std.testing.expectError(error.RestoreStagingScopeChanged, db.applyCommittedRaftIndexOwnerControl(&admission, 11, .{ .term = 1, .index = 10 }));
        try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    }
    const PrepareFaults = struct {
        fn run(a: std.mem.Allocator, database: *db_mod.DB, command: raft.Command) !void {
            var vtable = a.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const deterministic: std.mem.Allocator = .{ .ptr = a.ptr, .vtable = &vtable };
            var view = database.core.acquireSchemaView().?;
            defer view.release();
            var read = try database.core.store.beginReadTxn();
            defer read.abort();
            var prepared = try raft.Prepared.control(deterministic, view, &read, command);
            defer prepared.deinit();
            try std.testing.expectEqualDeep(command.binding, prepared.command.binding);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, PrepareFaults.run, .{ &db, admission.command });
    const AdmissionFaults = struct {
        fn run(a: std.mem.Allocator, database: *db_mod.DB, intent: raft.Admission, manifest: []const u8, expected: jobs.Binding) !void {
            var vtable = a.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const deterministic: std.mem.Allocator = .{ .ptr = a.ptr, .vtable = &vtable };
            var view = database.core.acquireSchemaView().?;
            defer view.release();
            var read = try database.core.store.beginReadTxn();
            defer read.abort();
            var prepared = try raft.PreparedAdmission.init(deterministic, view, &read, intent, manifest);
            defer prepared.deinit();
            try std.testing.expectEqualDeep(expected, prepared.binding);
            try std.testing.expect(try jobs.load(&read) == null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, AdmissionFaults.run, .{ &db, header, candidate.inputs.bytes, binding });
    {
        // Measure the same compiler against the same immutable snapshot. The
        // advisory bridge eliminates exactly the base64 transport allocation,
        // rather than hiding a second manifest encoding behind its API.
        var counters: [2]std.testing.FailingAllocator = .{ .init(alloc, .{}), .init(alloc, .{}) };
        var vtables = .{ counters[0].allocator().vtable.*, counters[1].allocator().vtable.* };
        inline for (0..2) |i| {
            vtables[i].resize = std.mem.Allocator.noResize;
            vtables[i].remap = std.mem.Allocator.noRemap;
        }
        const measured: [2]std.mem.Allocator = .{
            .{ .ptr = &counters[0], .vtable = &vtables[0] },
            .{ .ptr = &counters[1], .vtable = &vtables[1] },
        };
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var advisory = try raft.PreparedAdmission.init(measured[0], view, &read, header, candidate.inputs.bytes);
        advisory.deinit();
        var transport = try raft.Prepared.admission(measured[1], view, &read, header, candidate.inputs.bytes);
        const transport_bytes = transport.command.inputs_b64.?.len;
        transport.deinit();
        try std.testing.expectEqual(counters[0].allocated_bytes + transport_bytes, counters[1].allocated_bytes);
        for (counters) |counter| try std.testing.expectEqual(counter.allocated_bytes, counter.freed_bytes);
        std.debug.print("Raft admission preparation: manifest_bytes={d} transport_allocation_saved={d} advisory_allocated={d} transport_allocated={d}\n", .{ candidate.inputs.bytes.len, transport_bytes, counters[0].allocated_bytes, counters[1].allocated_bytes });
    }
    {
        var barrier: @import("antfly_runtime_abi").mutation_barrier.MutationBarrier = .{};
        var publisher: TestNativeIndexPublisher = .{ .barrier = &barrier };
        defer if (publisher.payload) |bytes| alloc.free(bytes);
        db.local_execution.replication_async_batch_mirror = publisher.asMirror();
        defer db.local_execution.replication_async_batch_mirror = null;
        try std.testing.expectError(error.ReplicationPublisherUnavailable, db.applyCommittedRaftIndexOwnerControl(&admission, 11, .{ .term = 1, .index = 10 }));
        try std.testing.expectEqual(@as(usize, 0), publisher.attempts);
        try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    }
    var wrong = admission.command;
    wrong.binding.authority = .native;
    try std.testing.expectError(error.InvalidPrivateIndexOwnerJob, wrong.validate());
    var early_fence = try db.prepareRaftIndexOwnerControl(.{ .effect = .fenced, .binding = binding });
    defer early_fence.deinit();
    try std.testing.expectError(error.PrivateIndexJobRequired, db.applyCommittedRaftIndexOwnerControl(&early_fence, 11, .{ .term = 1, .index = 3 }));
    try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    try std.testing.expectError(error.InvalidOrderedApplyReceipt, db.applyCommittedRaftIndexOwnerControl(&admission, 11, .{ .term = 1, .index = 0 }));
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, db.applyCommittedRaftIndexOwnerControl(&admission, 12, .{ .term = 1, .index = 10 }));

    const Fault = struct {
        child: *docstore.DocStore.Txn,
        at: usize,
        writes: usize = 0,
        pub fn get(self: *@This(), name: []const u8) ![]const u8 {
            return self.child.get(name);
        }
        pub fn openCursor(self: *@This()) !docstore.DocStore.Txn.CursorAdapter {
            return self.child.openCursor();
        }
        fn write(self: *@This()) !void {
            self.writes += 1;
            if (self.writes == self.at) return error.InjectedPhaseWriteFailure;
        }
        pub fn put(self: *@This(), name: []const u8, value: []const u8) !void {
            try self.write();
            try self.child.put(name, value);
        }
        pub fn delete(self: *@This(), name: []const u8) !void {
            try self.write();
            try self.child.delete(name);
        }
        fn probe(database: *db_mod.DB, prepared: *raft.Prepared, entry: receipts.OrderedApplyReceipt) !void {
            var saved_job: ?jobs.Binding = null;
            var saved_marker: ?receipts.OrderedApplyReceipt = null;
            var saved_catalog: [32]u8 = undefined;
            {
                var read = try database.core.store.beginReadTxn();
                defer read.abort();
                saved_job = try jobs.load(&read);
                saved_marker = try receipts.readOrderedApplyReceiptTxn(&read);
                saved_catalog = @import("relational_index_inputs.zig").digest(try read.get(catalog.key));
            }
            var completed = false;
            for (1..64) |at| {
                const succeeded = attempt: {
                    var txn = try database.core.store.beginWriteTxn();
                    defer txn.abort();
                    var fault: @This() = .{ .child = &txn, .at = at };
                    const outcome = prepared.stage(database.core, &fault, 11, entry) catch |err| {
                        try std.testing.expectEqual(error.InjectedPhaseWriteFailure, err);
                        break :attempt false;
                    };
                    try std.testing.expect(!outcome.duplicate);
                    var bytes: [receipts.ordered_apply_receipt_value_len]u8 = undefined;
                    const marker_write = receipts.orderedApplyReceiptWrite(entry, &bytes);
                    fault.put(marker_write.key, marker_write.value) catch |err| {
                        try std.testing.expectEqual(error.InjectedPhaseWriteFailure, err);
                        break :attempt false;
                    };
                    break :attempt true;
                };
                {
                    var read = try database.core.store.beginReadTxn();
                    defer read.abort();
                    try std.testing.expectEqualDeep(saved_job, try jobs.load(&read));
                    try std.testing.expectEqualDeep(saved_marker, try receipts.readOrderedApplyReceiptTxn(&read));
                    try std.testing.expectEqual(saved_catalog, @import("relational_index_inputs.zig").digest(try read.get(catalog.key)));
                    if (saved_job) |job| {
                        try raft.requireCuts(&read, job);
                        try @import("relational_index_inputs.zig").require(&read, job.inputs_digest);
                    } else {
                        try std.testing.expect(try @import("relational_integrity_topology.zig").current(&read) == null);
                        try std.testing.expectError(error.NotFound, read.get(@import("relational_index_inputs.zig").key));
                    }
                }
                if (succeeded) {
                    completed = true;
                    break;
                }
            }
            try std.testing.expect(completed);
        }
    };
    try Fault.probe(&db, &admission, .{ .term = 1, .index = 10 });

    // A real durable predecessor must resolve before a drained-fence receipt.
    const transaction = try db.beginTransaction(1);
    {
        var pinned = try db.core.store.beginReadTxn();
        defer pinned.abort();
        try db.applyCommittedRaftIndexOwnerControl(&admission, 11, .{ .term = 1, .index = 10 });
        const old = try raft.observe(&pinned, identity);
        try std.testing.expect(old.binding == null and old.fenced == null and old.reserved == null);
        try std.testing.expectEqual(@as(u64, 1), old.applied.?.raft.index);
        const current = try db.raftIndexOwnerObservation(11, identity);
        try std.testing.expectEqualDeep(binding, current.binding.?);
        try std.testing.expectEqual(@as(u64, 10), current.applied.?.raft.index);
    }
    try std.testing.expectEqualDeep(binding, (try db.raftIndexOwnerState(11, identity)).?);
    foreign_identity = identity;
    foreign_identity.plan_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, db.raftIndexOwnerState(11, foreign_identity));
    try db.applyCommittedRaftIndexOwnerControl(&admission, 11, .{ .term = 1, .index = 10 });
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, db.applyCommittedRaftIndexOwnerControl(&admission, 11, .{ .term = 2, .index = 10 }));
    try std.testing.expectError(error.TransactionTopologyBusy, db.applyCommittedRaftIndexOwnerControl(&early_fence, 11, .{ .term = 2, .index = 20 }));
    try std.testing.expectEqual(@as(u64, 10), (try db.orderedApplyReceipt()).?.index);
    try db.abortTransaction(transaction, 2);
    try Fault.probe(&db, &early_fence, .{ .term = 2, .index = 20 });
    try db.applyCommittedRaftIndexOwnerControl(&early_fence, 11, .{ .term = 2, .index = 20 });
    const fenced = (try db.raftIndexOwnerReceipt(11, binding, .fenced)).?;
    try std.testing.expectEqualDeep(binding, (try db.raftIndexOwnerState(11, identity)).?);
    try std.testing.expect(fenced.position == .raft);
    try std.testing.expectEqual(@as(u64, 20), fenced.position.raft.index);
    try std.testing.expectEqual(@as(u64, 2), fenced.position.raft.term);
    try db.applyCommittedRaftIndexOwnerControl(&early_fence, 11, .{ .term = 3, .index = 25 });
    try std.testing.expectEqualDeep(fenced, (try db.raftIndexOwnerReceipt(11, binding, .fenced)).?);
    try std.testing.expectEqual(@as(u64, 25), (try db.orderedApplyReceipt()).?.index);
    try std.testing.expectError(error.InvalidControlReceiptPosition, db.applyCommittedRaftIndexOwnerControl(&early_fence, 11, .{ .term = 1, .index = 26 }));
    var reserve = try db.prepareRaftIndexOwnerControl(.{ .effect = .reserved, .binding = binding });
    defer reserve.deinit();
    try Fault.probe(&db, &reserve, .{ .term = 3, .index = 30 });
    try db.applyCommittedRaftIndexOwnerControl(&reserve, 11, .{ .term = 3, .index = 30 });
    const reserved = (try db.raftIndexOwnerReceipt(11, binding, .reserved)).?;
    var reserved_binding = binding;
    reserved_binding.phase = .reserved;
    try std.testing.expectEqualDeep(reserved_binding, (try db.raftIndexOwnerState(11, identity)).?);
    try std.testing.expectEqual(@as(u64, 30), reserved.position.raft.index);
    try std.testing.expectEqualDeep(fenced, (try db.raftIndexOwnerReceipt(11, binding, .fenced)).?);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const state = (try @import("../source_authority.zig").load(&read)).?;
        try std.testing.expectEqual(.raft, state.kind);
        try std.testing.expectEqual(@as(u64, 0), state.sequence);
        try std.testing.expectError(error.NotFound, read.get(&jobs.nativeEffectKey(.admitted)));
        try std.testing.expectEqual(.reserved, (try jobs.load(&read)).?.phase);
    }
    db.close();
    db_live = false;
    db = try db_mod.DB.open(alloc, directory.path(), options);
    db_live = true;
    try std.testing.expectEqualDeep(reserved, (try db.raftIndexOwnerReceipt(11, binding, .reserved)).?);
    try std.testing.expectEqualDeep(reserved_binding, (try db.raftIndexOwnerState(11, identity)).?);
    var retry = try db.prepareRaftIndexOwnerControl(.{ .effect = .reserved, .binding = binding });
    defer retry.deinit();
    try db.applyCommittedRaftIndexOwnerControl(&retry, 11, .{ .term = 4, .index = 40 });
    try std.testing.expectEqualDeep(reserved, (try db.raftIndexOwnerReceipt(11, binding, .reserved)).?);
    try std.testing.expectEqual(@as(u64, 40), (try db.orderedApplyReceipt()).?.index);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, PrepareFaults.run, .{ &db, raft.Command{ .effect = .reserved, .binding = binding } });
    {
        var cut_key: [raft.prefix.len + 1]u8 = undefined;
        @memcpy(cut_key[0..raft.prefix.len], raft.prefix);
        cut_key[raft.prefix.len] = @backingInt(raft.Effect.reserved);
        const saved = try db.core.store.get(alloc, &cut_key);
        defer alloc.free(saved);
        const corrupt = try alloc.dupe(u8, saved);
        defer alloc.free(corrupt);
        corrupt[56] ^= 1;
        try db.core.store.putBatch(&.{.{ .key = &cut_key, .value = corrupt }}, &.{});
        try std.testing.expectError(error.InvalidControlReceiptPosition, db.raftIndexOwnerReceipt(11, binding, .reserved));
        try std.testing.expectError(error.InvalidControlReceiptPosition, db.raftIndexOwnerState(11, identity));
        try std.testing.expectError(error.InvalidControlReceiptPosition, db.raftIndexOwnerObservation(11, identity));
        try db.core.store.putBatch(&.{.{ .key = &cut_key, .value = saved }}, &.{});
        try std.testing.expectEqualDeep(reserved, (try db.raftIndexOwnerReceipt(11, binding, .reserved)).?);
    }
    try @import("antfly_server_test_sources").local_test_sources.storage_relational_index_raft_apply.run(admission.command);
}

test {
    _ = @import("relational_index_release.zig");
}

/// Synthetic metadata decision for storage-engine qualification only. Public
/// receivers must fetch real durable acceptance from independent metadata.
fn sourceDecision(binding: @import("relational_index_owner_job.zig").Binding, header: @import("relational_index_private_claims.zig").SourceStream.Header, previous: ?@import("relational_index_private_claims.zig").SourceStream.Accepted, logical_digest: [32]u8) !@import("relational_index_private_claims.zig").SourceStream.Accepted {
    const Source = @import("relational_index_private_claims.zig").SourceStream;
    var binding_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(&try binding.encode(), &binding_digest, .{});
    const count = std.mem.readInt(u32, header.bytes[96..100], .little);
    var result: Source.Accepted = .{
        .metadata_incarnation = @splat('1'),
        .binding_digest = binding_digest,
        .scope_digest = header.bytes[40..72].*,
        .ordinal = std.mem.readInt(u64, header.bytes[104..112], .little),
        .rows = (if (previous) |value| value.rows else @as(u64, 0)) + count,
        .batch_rows = count,
        .source_sequence = (try header.position()).sequence,
        .batch_digest = header.digest(),
        .logical_digest = logical_digest,
        .previous_decision = if (previous) |value| value.decision_digest else @splat(0),
        .decision_digest = @splat(0),
        .state = @fromBackingInt(@intCast(header.bytes[184])),
    };
    result.decision_digest = result.computedDigest();
    try result.requireFor(binding, header);
    return result;
}

test {
    _ = @import("antfly_server_test_sources").local_test_sources.storage_relational_read_set;
    _ = @import("antfly_server_test_sources").local_test_sources.storage_retained_read_registry;
}

const TestNativeIndexPublisher = struct {
    const contract = @import("replication_contract.zig");
    const outboxes = @import("durable_outbox.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    fail: bool = true,
    payload: ?[]u8 = null,
    attempts: usize = 0,
    completed: usize = 0,
    barrier: *Barrier,
    lsn: u64 = 2,

    fn next(ptr: *anyopaque) u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.lsn;
    }
    fn identity(_: *anyopaque) contract.Publisher.Identity {
        return .{ .table_id = 101, .shard_id = 11, .timeline_id = 1, .epoch = 1 };
    }
    fn publish(_: contract.AsyncEffectMirror, _: outboxes.Kind, _: []const u8, _: @import("doc_identity.zig").Namespace) !u64 {
        return error.UnexpectedNonDurablePublication;
    }
    fn recover(mirror: contract.AsyncEffectMirror, kind: outboxes.Kind, pending: outboxes.DurableReplicationOutbox, _: @import("doc_identity.zig").Namespace) !u64 {
        const self: *@This() = @ptrCast(@alignCast(mirror.publisher.ptr));
        try std.testing.expectEqual(outboxes.Kind.restore_batch, kind);
        try std.testing.expectEqual(self.lsn, pending.from_lsn);
        if (self.payload) |previous| {
            try std.testing.expectEqualSlices(u8, previous, pending.payload);
        } else self.payload = try alloc.dupe(u8, pending.payload);
        self.attempts += 1;
        if (self.fail) return error.TestPublicationUnavailable;
        return pending.from_lsn;
    }
    fn preflight(_: contract.AsyncEffectMirror, _: bool) !void {}
    fn complete(mirror: contract.AsyncEffectMirror, _: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(mirror.publisher.ptr));
        // Checkpoint capture must not be blocked by a lease held while
        // waiting for standby acknowledgement.
        var capture = self.barrier.tryAcquireExclusive() orelse return error.CheckpointCaptureBlockedByCompletionWait;
        capture.release();
        self.completed += 1;
    }
    fn asMirror(self: *@This()) contract.AsyncBatchMirror {
        return .{
            .publisher = .{ .ptr = self, .vtable = &.{ .next_lsn = next, .identity = identity, .publish = publish, .recover = recover, .preflight = preflight, .complete = complete } },
            .mutation_barrier = self.barrier,
            .requirements = .{ .synchronous = true, .durable_outbox = true, .preflight = true },
        };
    }
};

/// Test-only physical fixture seeding. Never use application mutation capture
/// to mix restored rows and source-control records; no identity/audit files copy.
fn seedNativeIndexFixture(source: *db_mod.DB, destination: *db_mod.DB) !void {
    var read = try source.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var txn = try destination.core.store.runtime_store.beginWrite();
    errdefer txn.abort();
    var item = try cursor.first();
    while (item) |entry| : (item = try cursor.next()) try txn.put(entry.key, entry.value);
    try txn.commit();
}

test "relational index build pages native owner control preserves committed cuts through standby replay and reopen" {
    const api = @import("../../schema/mod.zig");
    const compiler = @import("../../schema/relational_declarations.zig");
    const catalog = @import("relational_integrity_catalog.zig");
    const candidates = @import("relational_index_candidate.zig");
    const jobs = @import("relational_index_owner_job.zig");
    const effects = @import("replication_effects.zig");
    const authority = @import("../source_authority.zig");
    const outbox_store = @import("durable_outbox_store.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Publisher = TestNativeIndexPublisher;
    const before =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const after =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}},"relational_indexes":[{"name":"email_idx","keys":[{"column":"email"}]}],"unique_constraints":[{"name":"email_idx","origin":"index","columns":["email"]}]}
    ;
    var primary_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-primary");
    defer primary_dir.cleanup();
    var standby_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-standby");
    defer standby_dir.cleanup();
    const options: db_mod.OpenOptions = .{
        .start_optional_runtimes = false,
        .primary_backend = .{ .lsm = .{} },
        .online_source_authority = .native,
        .identity_namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 },
    };
    var primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
    defer primary.close();
    var barrier: Barrier = .{};
    var publisher: Publisher = .{ .barrier = &barrier };
    defer if (publisher.payload) |bytes| alloc.free(bytes);
    // The borrowed publisher must be detached before its fixture is destroyed.
    defer primary.local_execution.replication_async_batch_mirror = null;
    var standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    defer standby.close();
    var parsed = try api.parseValidatedTableSchema(alloc, before);
    defer parsed.deinit(alloc);
    var prior = try compiler.compileCatalog(alloc, parsed, 101, null);
    defer prior.deinit();
    for ([_]*db_mod.DB{ &primary, &standby }) |db| {
        try db.setSchemaJson(alloc, before);
        try db.core.store.putBatch(&.{.{ .key = catalog.key, .value = prior.value }}, &.{});
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"id\":1,\"email\":\"a@example.com\"}" },
            .{ .key = "b", .value = "{\"id\":2,\"email\":\"b@example.com\"}" },
            .{ .key = "c", .value = "{\"id\":3,\"email\":\"c@example.com\"}" },
        } });
    }
    // Standby starts from the same physical source snapshot, not independent
    // mutations whose stored row metadata can differ despite equal JSON.
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        for ([_][]const u8{ "a", "b", "c" }) |document| {
            const key = try internal.relationalRowKeyAlloc(alloc, document);
            defer alloc.free(key);
            try standby.core.store.putBatch(&.{.{ .key = key, .value = try read.get(key) }}, &.{});
        }
    }
    var candidate = try candidates.Candidate.prepare(alloc, 101, before, after, prior.value, "email_idx");
    defer candidate.deinit();
    var prepared = try primary.prepareNativeIndexOwnerAdmission(.{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = options.identity_namespace.?, .catalog_digest = @import("relational_index_inputs.zig").digest(prior.value) },
        .candidate_digest = @import("relational_index_inputs.zig").digest(candidate.next.value),
        .reservation_digest = @import("relational_index_inputs.zig").digest(candidate.reservation.value),
    }, candidate.inputs.bytes);
    defer prepared.deinit();
    const binding = prepared.command.binding;
    const receipt_identity: db_mod.DB.NativeIndexOwnerIdentity = .{
        .id = binding.id,
        .plan_digest = binding.plan_digest,
        .owner_digest = binding.owner_digest,
        .ordinal = binding.ordinal,
        .namespace = binding.fence.namespace,
        .owner_group_id = binding.fence.owner_group_id,
    };
    try std.testing.expectError(error.PrivateIndexCancellationRequired, primary.prepareNativeIndexOwnerCancellationAdmission(db_mod.DB.NativeIndexOwnerAdmission.fromBinding(binding), candidate.inputs.bytes));
    try std.testing.expectError(error.PrivateIndexCancellationRequired, primary.prepareNativeIndexOwnerCancellationControl(.{ .effect = .fenced, .binding = binding }));
    const untouched = try primary.nativeIndexOwnerCancellationState(receipt_identity);
    try std.testing.expectEqual(.native, untouched.source.kind);
    try std.testing.expect(untouched.intent == null and untouched.binding == null and untouched.cleanup == null);
    for ([_]jobs.NativeEffect{ .admitted, .fenced, .reserved }, 0..) |effect, ordinal| {
        var control = if (effect == .admitted)
            try primary.prepareNativeIndexOwnerControl(prepared.command)
        else
            try primary.prepareNativeIndexOwnerControl(.{ .effect = effect, .binding = binding });
        defer control.deinit();
        try control.preparePayload();
        if (effect == .fenced) {
            const input_key = @import("relational_index_inputs.zig").key;
            const saved = try primary.core.store.get(alloc, input_key);
            defer alloc.free(saved);
            try primary.core.store.putBatch(&.{}, &.{input_key});
            try std.testing.expectError(error.InvalidPrivateIndexInputs, primary.applyPreparedNativeIndexOwnerControl(&control, null));
            var txn = try primary.core.store.beginReadTxn();
            defer txn.abort();
            try std.testing.expectEqual(@as(u64, 1), (try authority.load(&txn)).?.sequence);
            try std.testing.expect(try jobs.nativeEffectPosition(&txn, binding, .fenced) == null);
            try primary.core.store.putBatch(&.{.{ .key = input_key, .value = saved }}, &.{});
        }
        if (effect == .reserved) {
            // A distinct effect has its own transport LSN and payload. Its
            // successful foreground ACK exercises lease release in apply,
            // rather than only during the explicit recovery call below.
            alloc.free(publisher.payload.?);
            publisher.payload = null;
            publisher.lsn = 3;
        }
        if (effect != .admitted) primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
        const outcome = try primary.applyPreparedNativeIndexOwnerControl(&control, null);
        try outcome.validateFor(control.command);
        try std.testing.expect(!outcome.duplicate);
        if (effect == .fenced) {
            try std.testing.expect(outcome.replication_pending);
            try std.testing.expectEqualStrings("TestPublicationUnavailable", outcome.error_name.?);
            try std.testing.expect(outcome.receipt == null);
            try std.testing.expectEqual(@as(usize, 1), publisher.attempts);
            try std.testing.expectEqual(@as(usize, 0), publisher.completed);
            var pending = try outbox_store.readPending(alloc, primary.core.store);
            defer pending.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), pending.entries.len);
            {
                // A stale clear hint (or a commit racing the flush) cannot
                // certify an effect whose outbox is present in its snapshot.
                const maybe = primary.local_execution.durable_replication_outbox_maybe.swap(false, .acq_rel);
                defer primary.local_execution.durable_replication_outbox_maybe.store(maybe, .release);
                const primary_pending = primary.async_context.primary_replication_outbox_pending.swap(false, .acq_rel);
                defer primary.async_context.primary_replication_outbox_pending.store(primary_pending, .release);
                const proof = (try primary.nativeIndexOwnerReceipt(.{ .identity = receipt_identity, .effect = .fenced })).?;
                try std.testing.expect(proof.replication_pending);
                try std.testing.expect(proof.receipt == null);
                try std.testing.expectEqualStrings("ReplicationOutboxPending", proof.error_name.?);
                try std.testing.expectEqual(outcome.position, proof.position);
                try std.testing.expectEqual(@as(usize, 1), publisher.attempts);
                try std.testing.expect(primary.local_execution.durable_replication_outbox_maybe.load(.acquire));
            }
            var txn = try primary.core.store.beginReadTxn();
            defer txn.abort();
            try std.testing.expectEqual(outcome.position, (try jobs.nativeEffectPosition(&txn, binding, effect)).?);
        } else {
            try std.testing.expect(!outcome.replication_pending);
            try std.testing.expect(outcome.error_name == null);
            try std.testing.expectEqual(effect != .admitted, outcome.receipt != null);
            if (effect == .reserved) {
                try std.testing.expectEqual(@as(usize, 3), publisher.attempts);
                try std.testing.expectEqual(@as(usize, 2), publisher.completed);
            }
        }
        const payload = try control.finalizePayload(outcome.position);
        if (effect == .reserved) try std.testing.expectEqualSlices(u8, payload, publisher.payload.?);
        if (effect == .fenced) {
            try std.testing.expectEqualSlices(u8, payload, publisher.payload.?);
            primary.local_execution.replication_async_batch_mirror = null;
            primary.close();
            primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
            publisher.fail = false;
            primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
            try primary.flushDurableReplicationOutboxes();
            try std.testing.expectEqual(@as(usize, 2), publisher.attempts);
            try std.testing.expectEqual(@as(usize, 1), publisher.completed);
            var pending = try outbox_store.readPending(alloc, primary.core.store);
            defer pending.deinit(alloc);
            try std.testing.expect(pending.isEmpty());
            primary.local_execution.replication_async_batch_mirror = null;
            var txn = try primary.core.store.beginReadTxn();
            defer txn.abort();
            try std.testing.expectEqual(outcome.position.sequence, (try authority.load(&txn)).?.sequence);
            try std.testing.expectEqual(outcome.position, (try jobs.nativeEffectPosition(&txn, binding, effect)).?);
        }
        const record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = ordinal + 1, .previous_lsn = ordinal, .payload = payload };
        var decoded = try effects.decodeBatchMutationRequest(alloc, record);
        defer decoded.deinit();
        var mixed = decoded.value;
        mixed.request.writes = &.{.{ .key = "smuggled", .value = "{}" }};
        try std.testing.expectError(error.InvalidBatchRequest, effects.validateIndexOwnerPayload(mixed));
        mixed = decoded.value;
        mixed.ordinary_raft_entry = .{ .term = 1, .index = 1 };
        try std.testing.expectError(error.InvalidBatchRequest, effects.validateIndexOwnerPayload(mixed));
        const unknown = try std.fmt.allocPrint(alloc, "{s},\"unknown_control\":true}}", .{payload[0 .. payload.len - 1]});
        defer alloc.free(unknown);
        var invalid_record = record;
        invalid_record.payload = unknown;
        try std.testing.expectError(error.InvalidBatchRequest, effects.decodeBatchMutationRequest(alloc, invalid_record));
        var gap = try replication_ingress.normalizeBatch(decoded.value);
        gap.index_owner_control.?.position.sequence += 1;
        gap.receipt = .{ .native = gap.index_owner_control.?.position };
        try std.testing.expectError(error.OnlineSourceScopeChanged, standby.applyReplicatedBatch(gap, record.lsn));
        try std.testing.expectEqual(@as(u64, ordinal), try standby.replicationAppliedSequence());
        try replication_ingress.applyRecord(&standby, record);
        try replication_ingress.applyRecord(&standby, record);
        const primary_retry = try primary.applyPreparedNativeIndexOwnerControl(&control, null);
        try primary_retry.validateFor(control.command);
        try std.testing.expect(primary_retry.duplicate);
        try std.testing.expectEqual(outcome.position, primary_retry.position);
        try std.testing.expectEqual(effect != .admitted, primary_retry.receipt != null);
        if (effect != .admitted) {
            const recovered = (try primary.nativeIndexOwnerReceipt(.{ .identity = receipt_identity, .effect = if (effect == .fenced) .fenced else .reserved })).?;
            const receipt_request: @import("relational_index_owner_control.zig").ReceiptRequest = .{ .identity = receipt_identity, .effect = if (effect == .fenced) .fenced else .reserved };
            try recovered.validateRecovery(receipt_request);
            try recovered.validateFor(control.command);
            var corrupt = recovered;
            corrupt.position.sequence += 1;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateRecovery(receipt_request));
            corrupt = recovered;
            corrupt.receipt.?.evidence_digest[0] ^= 1;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateFor(control.command));
            corrupt = recovered;
            corrupt.receipt.?.owner.ordinal += 1;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateFor(control.command));
            corrupt = recovered;
            corrupt.receipt.?.candidate_digest[0] ^= 1;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateRecovery(receipt_request));
            corrupt = recovered;
            corrupt.replication_pending = true;
            corrupt.error_name = "ReplicationOutboxPending";
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateFor(control.command));
            corrupt = recovered;
            corrupt.error_name = "OutOfMemory";
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateFor(control.command));
            corrupt.receipt = null;
            // A committed effect with failed proof lookup remains recoverable
            // progress; it must not be turned into a rollback-looking error.
            try corrupt.validateFor(control.command);
            corrupt = recovered;
            corrupt.binding.inputs_digest[0] ^= 1;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, corrupt.validateFor(control.command));
            var foreign = receipt_request;
            foreign.identity.owner_digest[0] ^= 1;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, recovered.validateRecovery(foreign));
            foreign = receipt_request;
            foreign.effect = if (effect == .fenced) .reserved else .fenced;
            try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, recovered.validateRecovery(foreign));
            try std.testing.expect(!recovered.replication_pending);
            try std.testing.expect(recovered.duplicate);
            try std.testing.expectEqual(outcome.position, recovered.position);
            try std.testing.expectEqual(try primary_retry.receipt.?.digest(), try recovered.receipt.?.digest());
        }
        if (effect == .reserved) {
            try std.testing.expectEqual(@as(usize, 3), publisher.attempts);
            try std.testing.expectEqual(@as(usize, 2), publisher.completed);
        }
        try std.testing.expectEqual(@as(u64, ordinal + 1), try standby.replicationAppliedSequence());
        var txn = try standby.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(outcome.position, (try jobs.nativeEffectPosition(&txn, binding, effect)).?);
        try std.testing.expectEqual(outcome.position.sequence, (try authority.load(&txn)).?.sequence);
        try std.testing.expectError(error.NotFound, txn.get(&internal.ordered_document_applied_entry_key));
    }
    // Cold schema installation preparation must retain the built generations,
    // not run ordinary DDL allocation against the reserved catalogs. This is
    // deliberately not installation authority or a metadata publish decision.
    {
        var reserved = binding;
        reserved.phase = .reserved;
        var after_schema = try api.parseValidatedTableSchema(alloc, after);
        defer after_schema.deinit(alloc);
        const runtime = try api.deriveRuntimeTableSchema(alloc, after_schema);
        defer @import("../schema.zig").freeSchema(alloc, runtime);
        const writes = [_]docstore.KVPair{.{ .key = "\x00\x00__metadata__:schema_json", .value = after }};
        var prepared_schema = try primary.core.prepareSchemaMetadataReservedIndex(runtime, &writes, .{ .binding = reserved, .candidate = &candidate });
        defer prepared_schema.deinit();
        try std.testing.expect(prepared_schema.reserved_activation == null);
        const indexes = prepared_schema.relational_indexes.?;
        try std.testing.expect(indexes.metadata.head.eql(binding.physical_candidate));
        try std.testing.expect(indexes.metadata.expected.?.eql(binding.physical_reserved));
        try std.testing.expectEqual(binding.index.mapKey(), indexes.plan.boundIndexes()[0].id().mapKey());
        try std.testing.expectEqual(prepared_schema.epoch.?, indexes.plan.schemaView().epoch);
        try std.testing.expect(indexes.plan.schemaView().validator() != null);
        try std.testing.expectEqualSlices(u8, candidate.next.value, prepared_schema.integrity_catalog.?.value);
        try std.testing.expectEqualSlices(u8, candidate.reservation.value, prepared_schema.integrity_catalog.?.expected.?);
        // Even a fully prepared candidate cannot bypass native installation
        // by using ordinary, child-generation or rehydration schema commit.
        try std.testing.expectError(error.PrivateIndexPublicationRequired, primary.core.commitPreparedSchemaMetadata(&prepared_schema, &writes, &.{}, null));
        try std.testing.expectError(error.PrivateIndexPublicationRequired, primary.core.rehydratePreparedSchemaMetadata(&prepared_schema, &writes));
        var child_fence = binding.fence;
        child_fence.role = .child_generation_source;
        try std.testing.expectError(error.PrivateIndexPublicationRequired, primary.core.commitPreparedSchemaMetadataPublishedChild(&prepared_schema, &writes, &.{}, null, child_fence));
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        var still_active = primary.core.schema_registry.acquire().?;
        defer still_active.release();
        try std.testing.expectEqual(binding.physical_reserved.schema_version, still_active.version());
        try jobs.requireReserved(&read, reserved);
        try std.testing.expectEqualSlices(u8, candidate.reservation.value, try read.get(catalog.key));
        try std.testing.expectEqual(@as(u64, 3), (try authority.load(&read)).?.sequence);
    }
    {
        var reserved = binding;
        reserved.phase = .reserved;
        var after_schema = try api.parseValidatedTableSchema(alloc, after);
        defer after_schema.deinit(alloc);
        const runtime = try api.deriveRuntimeTableSchema(alloc, after_schema);
        defer @import("../schema.zig").freeSchema(alloc, runtime);
        // The schema validator is part of the immutable candidate, not a
        // caller-selected same-version public payload.
        const wrong = [_]docstore.KVPair{.{ .key = "\x00\x00__metadata__:schema_json", .value = before }};
        try std.testing.expectError(error.InvalidPrivateIndexInputs, primary.core.prepareSchemaMetadataReservedIndex(runtime, &wrong, .{ .binding = reserved, .candidate = &candidate }));
        try std.testing.expectError(error.InvalidPrivateIndexInputs, primary.core.prepareSchemaMetadataReservedIndex(runtime, &.{}, .{ .binding = reserved, .candidate = &candidate }));
        var stale = reserved;
        stale.owner_digest[0] ^= 1;
        const writes = [_]docstore.KVPair{.{ .key = "\x00\x00__metadata__:schema_json", .value = after }};
        const versioned = try api.versionedSchemaKeyAlloc(alloc, runtime.version);
        defer alloc.free(versioned);
        const inconsistent = [_]docstore.KVPair{ writes[0], .{ .key = versioned, .value = before } };
        try std.testing.expectError(error.InvalidPrivateIndexInputs, primary.core.prepareSchemaMetadataReservedIndex(runtime, &inconsistent, .{ .binding = reserved, .candidate = &candidate }));
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, primary.core.prepareSchemaMetadataReservedIndex(runtime, &writes, .{ .binding = stale, .candidate = &candidate }));
        // Ordinary schema metadata must not delete its own admission fence
        // before the transaction participant checks the durable predecessor.
        var ordinary = try primary.core.prepareSchemaMetadata(runtime, &writes);
        defer ordinary.deinit();
        try std.testing.expectError(error.IntegrityTopologyBusy, primary.core.commitPreparedSchemaMetadata(&ordinary, &writes, &.{@import("relational_integrity_topology.zig").fence_key}, null));
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try jobs.requireReserved(&read, reserved);
        try std.testing.expectEqualSlices(u8, candidate.reservation.value, try read.get(catalog.key));
        try std.testing.expectEqual(@as(u64, 3), (try authority.load(&read)).?.sequence);
    }
    // Every bounded page (including EOF) is ordered progress, not UNIQUE
    // validation. Replay the prepared boundary, never another timed scan.
    {
        var reservation = try primary.prepareNativeIndexOwnerControl(.{ .effect = .reserved, .binding = binding });
        defer reservation.deinit();
        var page_count: u64 = 0;
        var examined: usize = 0;
        while (try primary.prepareNativeIndexOwnerBuild("items", &reservation, .{ .records = 1, .time_ns = std.time.ns_per_s })) |owned| {
            var build = owned;
            defer build.deinit();
            examined += build.command.records_examined;
            page_count += 1;
            try std.testing.expect(page_count <= 4);
            publisher.lsn = 3 + page_count;
            publisher.fail = page_count == 1;
            if (publisher.payload) |bytes| alloc.free(bytes);
            publisher.payload = null;
            primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
            const outcome = try primary.applyPreparedNativeIndexOwnerBuild(&build, null);
            try std.testing.expectEqual(page_count == 1, outcome.replication_pending);
            if (page_count == 1) {
                const source_identity = try @import("relational_index_owner_control.zig").BuildRequest.init(.{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = binding.fence.owner_group_id }, build.command.binding, .{});
                var blocked_source = try primary.readNativeIndexClaimSource(alloc, source_identity.identity, .{});
                defer blocked_source.deinit();
                try std.testing.expect(blocked_source.replication_pending and blocked_source.batch == null);
                try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, blocked_source.digest());
            }
            try outcome.validateFor(build.command);
            try std.testing.expect(!@hasField(@TypeOf(outcome), "receipt"));
            const payload = try build.finalizePayload(outcome.position);
            try std.testing.expectEqualSlices(u8, payload, publisher.payload.?);
            primary.local_execution.replication_async_batch_mirror = null;
            if (page_count == 1) {
                primary.close();
                primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
                publisher.fail = false;
                primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
                try primary.flushDurableReplicationOutboxes();
                primary.local_execution.replication_async_batch_mirror = null;
            }
            const retry = try primary.applyPreparedNativeIndexOwnerBuild(&build, null);
            try retry.validateFor(build.command);
            try std.testing.expect(retry.duplicate);
            try std.testing.expectEqual(outcome.position, retry.position);
            if (page_count == 1) {
                // Both values can be well formed with matching physical
                // checksums yet disagree with this prepared page's next cut.
                primary.core.lockApply();
                defer primary.core.unlockApply();
                var fault_txn = try primary.core.store.beginWriteTxn();
                defer fault_txn.abort();
                var bytes = (try fault_txn.get(db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_key))[0..db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_len].*;
                var changed = build.command.next;
                changed.rows_scanned += 1;
                const encoded = try changed.encode(alloc);
                defer alloc.free(encoded);
                try fault_txn.put(&@import("relational_index_jobs.zig").progressKey(binding.index), encoded);
                std.crypto.hash.Blake3.hash(encoded, bytes[72..104], .{});
                std.crypto.hash.Blake3.hash(bytes[0..176], bytes[176..208], .{});
                try fault_txn.put(db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_key, &bytes);
                try std.testing.expectError(error.InvalidRelationalIndexProgress, build.stage(primary.core, &fault_txn, null));
            }
            const record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 3 + page_count, .previous_lsn = 2 + page_count, .payload = payload };
            {
                var decoded = try effects.decodeBatchMutationRequest(alloc, record);
                defer decoded.deinit();
                try std.testing.expectEqual(build.command_digest, try decoded.value.index_owner_control.?.effect.build.digest());
                var gap = try replication_ingress.normalizeBatch(decoded.value);
                gap.index_owner_control.?.position.sequence += 1;
                gap.receipt = .{ .native = gap.index_owner_control.?.position };
                try std.testing.expectError(error.OnlineSourceScopeChanged, standby.applyReplicatedBatch(gap, record.lsn));
                try std.testing.expectEqual(record.previous_lsn, try standby.replicationAppliedSequence());
            }
            try replication_ingress.applyRecord(&standby, record);
            try replication_ingress.applyRecord(&standby, record);
            for ([_]*db_mod.DB{ &primary, &standby }) |db| {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const progress = try @import("relational_index_jobs.zig").Progress.decode(try read.get(&@import("relational_index_jobs.zig").progressKey(binding.index)));
                try std.testing.expectEqual(build.command.next.state, progress.state);
                try std.testing.expectEqual(@min(page_count, 3), progress.rows_scanned);
                var reserved = binding;
                reserved.phase = .reserved;
                try db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&read, reserved);
                const stream = @import("relational_index_private_claims.zig").SourceStream;
                const head = (try stream.load(&read, reserved)).?;
                try std.testing.expectEqual(page_count, head.pending());
                try std.testing.expectEqual(@min(page_count, 3), head.emitted_rows);
                try std.testing.expectEqual(outcome.position.sequence, head.last_sequence);
                try std.testing.expectEqualSlices(u8, &build.claim_batch.header, try read.get(&stream.key(binding.id, page_count, false)));
                try std.testing.expectEqualSlices(u8, build.claim_batch.payload, try read.get(&stream.key(binding.id, page_count, true)));
                var batch = (try db_mod.DB.PreparedNativeIndexOwnerBuild.peekClaimSource(alloc, &read, reserved)).?;
                defer batch.deinit();
                try std.testing.expectEqual(@as(usize, 1), batch.rows().len);
                try std.testing.expectEqualStrings("items", batch.scope.table_name);
                // Even a structurally consistent consumed prefix is not
                // authorized by any source-delivery closure yet.
                var forged = head;
                forged.consumed = 1;
                forged.consumed_rows = batch.rows().len;
                forged.consumed_sequence = (try batch.header.position()).sequence;
                forged.consumed_digest = batch.header.digest();
                forged.pending_bytes -= batch.header.storedBytes();
                try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, forged.encode());
                try std.testing.expectEqual(outcome.position.sequence, (try authority.load(&read)).?.sequence);
                try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
            }
        }
        try std.testing.expectEqual(@as(u64, 4), page_count);
        try std.testing.expectEqual(@as(usize, 3), examined);
        // A valid checksum cannot substitute a different durable progress cut.
        {
            var fault_txn = try primary.core.store.beginWriteTxn();
            defer fault_txn.abort();
            var bytes = (try fault_txn.get(db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_key))[0..db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_len].*;
            bytes[72] ^= 1;
            std.crypto.hash.Blake3.hash(bytes[0..176], bytes[176..208], .{});
            try fault_txn.put(db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_key, &bytes);
            var reserved = binding;
            reserved.phase = .reserved;
            try std.testing.expectError(error.InvalidRelationalIndexProgress, db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&fault_txn, reserved));
        }
        // Bounded header recovery does not read payloads. Delivery still
        // rejects missing/corrupt bodies instead of acknowledging the batch.
        for (0..3) |fault| {
            const stream = @import("relational_index_private_claims.zig").SourceStream;
            var fault_txn = try primary.core.store.beginWriteTxn();
            defer fault_txn.abort();
            var reserved = binding;
            reserved.phase = .reserved;
            const batch_key = stream.key(binding.id, 1, true);
            if (fault == 0) {
                try fault_txn.delete(&batch_key);
                try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, stream.peek(alloc, &fault_txn, reserved));
            } else if (fault == 1) {
                const body = try alloc.dupe(u8, try fault_txn.get(&batch_key));
                defer alloc.free(body);
                body[body.len - 1] ^= 1;
                try fault_txn.put(&batch_key, body);
                try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, stream.peek(alloc, &fault_txn, reserved));
            } else {
                try fault_txn.delete(&stream.key(binding.id, 1, false));
                try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, stream.peek(alloc, &fault_txn, reserved));
                try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&fault_txn, reserved));
                continue;
            }
            try db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&fault_txn, reserved);
        }
        // Updating both checksums cannot disguise a different source progress
        // from the independently retained physical page checkpoint.
        {
            const stream = @import("relational_index_private_claims.zig").SourceStream;
            var fault_txn = try primary.core.store.beginWriteTxn();
            defer fault_txn.abort();
            var reserved = binding;
            reserved.phase = .reserved;
            var head = (try stream.load(&fault_txn, reserved)).?;
            var header = (try fault_txn.get(&stream.key(binding.id, head.emitted, false)))[0..stream.header_len].*;
            header[152] ^= 1;
            std.crypto.hash.Blake3.hash(header[0..288], header[288..320], .{});
            head.last_digest = header[288..320].*;
            try fault_txn.put(&stream.key(binding.id, head.emitted, false), &header);
            try fault_txn.put(stream.head_key, &try head.encode());
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&fault_txn, reserved));
        }
    }
    // Deliver actual claims on both replicas, then consume only the source
    // batch covered by the test metadata decision. The same native outbox and
    // transport marker must carry deletion plus the retained consumption cut.
    {
        const native = @import("relational_index_owner_control.zig");
        const Source = @import("relational_index_private_claims.zig").SourceStream;
        var reserved = binding;
        reserved.phase = .reserved;
        var read = try primary.core.store.beginReadTxn();
        var batch = (try db_mod.DB.PreparedNativeIndexOwnerBuild.peekClaimSource(alloc, &read, reserved)).?;
        read.abort();
        defer batch.deinit();
        const command: native.ClaimCommand = .{ .binding = reserved, .claims = &.{.{ .parent_table = batch.scope.table_name, .parent_key = batch.rows()[0].key, .tuple = batch.rows()[0].tuple, .schema_version = reserved.physical_candidate.schema_version }} };
        var claims = try primary.prepareNativeIndexOwnerClaims(command);
        defer claims.deinit();
        if (publisher.payload) |bytes| alloc.free(bytes);
        publisher.payload = null;
        publisher.lsn = 8;
        publisher.fail = false;
        primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
        const delivered = try primary.applyPreparedNativeIndexOwnerClaims(&claims, null);
        try delivered.validateFor(command);
        const claim_payload = try claims.finalizePayload(delivered.position);
        try replication_ingress.applyRecord(&standby, .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 8, .previous_lsn = 7, .payload = claim_payload });
        const accepted = try sourceDecision(reserved, batch.header, null, try command.digest());
        var consume = try primary.prepareNativeIndexSourceConsumption(.{ .binding = reserved, .accepted = accepted });
        defer consume.deinit();
        if (publisher.payload) |bytes| alloc.free(bytes);
        publisher.payload = null;
        publisher.lsn = 9;
        publisher.fail = true;
        const pending = try primary.applyPreparedNativeIndexSourceConsumption(&consume, null);
        try pending.validateFor(consume.command);
        try std.testing.expect(pending.replication_pending);
        try std.testing.expect(pending.position.sequence > (try batch.header.position()).sequence);
        const identity: native.Identity = .{ .id = reserved.id, .plan_digest = reserved.plan_digest, .owner_digest = reserved.owner_digest, .ordinal = reserved.ordinal, .namespace = reserved.fence.namespace, .owner_group_id = reserved.fence.owner_group_id };
        var blocked = try primary.readNativeIndexClaimSource(alloc, identity, .{});
        defer blocked.deinit();
        try std.testing.expect(blocked.replication_pending and blocked.batch == null and blocked.consumed == null);
        const payload = try consume.finalizePayload(pending.position);
        primary.local_execution.replication_async_batch_mirror = null;
        primary.close();
        primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
        publisher.fail = false;
        primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
        try primary.flushDurableReplicationOutboxes();
        const duplicate = try primary.applyPreparedNativeIndexSourceConsumption(&consume, null);
        try std.testing.expect(duplicate.duplicate and !duplicate.replication_pending);
        try std.testing.expectEqual(pending.position, duplicate.position);
        const record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 9, .previous_lsn = 8, .payload = payload };
        var decoded = try effects.decodeBatchMutationRequest(alloc, record);
        defer decoded.deinit();
        var gap = try replication_ingress.normalizeBatch(decoded.value);
        gap.index_owner_control.?.position.sequence += 1;
        gap.receipt = .{ .native = gap.index_owner_control.?.position };
        try std.testing.expectError(error.InvalidControlReceiptPosition, standby.applyReplicatedBatch(gap, 9));
        try std.testing.expectEqual(@as(u64, 8), try standby.replicationAppliedSequence());
        try replication_ingress.applyRecord(&standby, record);
        try replication_ingress.applyRecord(&standby, record);
        for ([_]*db_mod.DB{ &primary, &standby }) |db| {
            var check = try db.core.store.beginReadTxn();
            defer check.abort();
            try db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&check, reserved);
            const head = (try Source.load(&check, reserved)).?;
            try std.testing.expectEqual(@as(u64, 1), head.consumed);
            try std.testing.expectEqual(@as(u64, 3), head.pending());
            try std.testing.expectError(error.NotFound, check.get(&Source.key(binding.id, 1, true)));
            const retained = (try Source.loadConsumption(&check, reserved, head)).?;
            try std.testing.expectEqualDeep(pending.record.?, retained);
            try std.testing.expectEqual(pending.position.sequence, (try authority.load(&check)).?.sequence);
        }
        primary.local_execution.replication_async_batch_mirror = null;
    }

    // Prepare first, then let a newer ordinary transport receipt win. The
    // apply-lock recheck must suppress the old effect and retain the newer LSN.
    var stale = try standby.prepareNativeIndexOwnerControl(.{ .effect = .fenced, .binding = binding });
    defer stale.deinit();
    try standby.recordReplicationApplied(100);
    const skipped = try standby.applyPreparedNativeIndexOwnerControl(&stale, .{ .position = .{ .namespace = options.identity_namespace.?, .sequence = 2 }, .lsn = 99 });
    try std.testing.expect(skipped.duplicate);
    try std.testing.expectEqual(@as(u64, 100), try standby.replicationAppliedSequence());
    standby.close();
    standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    var txn = try standby.core.store.beginReadTxn();
    defer txn.abort();
    try std.testing.expectEqual(@as(u64, 100), try standby.replicationAppliedSequence());
    try std.testing.expectEqual(jobs.Phase.reserved, (try jobs.load(&txn)).?.phase);
    // Private reservation must not publish the candidate schema.
    try std.testing.expectEqual(@as(u32, 1), standby.core.schema.?.version);

    // An owner may receive cancellation before its first admission. The
    // durable intent must survive that jobless restart and replay atomically
    // with its native cut, source fence, transport marker and outbox.
    var cancel_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-cancel-primary");
    defer cancel_dir.cleanup();
    var follower_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-cancel-standby");
    defer follower_dir.cleanup();
    var cancel_primary = try db_mod.DB.open(alloc, cancel_dir.path(), options);
    defer cancel_primary.close();
    var follower = try db_mod.DB.open(alloc, follower_dir.path(), options);
    defer follower.close();
    for ([_]*db_mod.DB{ &cancel_primary, &follower }) |db| {
        try db.setSchemaJson(alloc, before);
        try db.core.store.putBatch(&.{.{ .key = catalog.key, .value = prior.value }}, &.{});
    }
    var cancel_barrier: Barrier = .{};
    var cancel_publisher: Publisher = .{ .barrier = &cancel_barrier, .lsn = 1 };
    defer if (cancel_publisher.payload) |bytes| alloc.free(bytes);
    defer cancel_primary.local_execution.replication_async_batch_mirror = null;
    const cancellation = @import("relational_index_cancellation_intent.zig");
    const intent = cancellation.Intent.fromBinding(binding);
    var pending_cancel = try cancel_primary.prepareNativeIndexOwnerCancellation(intent);
    defer pending_cancel.deinit();
    cancel_primary.local_execution.replication_async_batch_mirror = cancel_publisher.asMirror();
    const canceled = try cancel_primary.applyPreparedNativeIndexOwnerCancellation(&pending_cancel, null);
    try std.testing.expect(canceled.replication_pending);
    try std.testing.expect(!canceled.duplicate);
    const intent_status = try cancel_primary.nativeIndexOwnerCancellationState(receipt_identity);
    try std.testing.expectEqual(canceled.position, intent_status.intent.?.position);
    try std.testing.expect(intent_status.binding == null and intent_status.admitted == null);
    var foreign_identity = receipt_identity;
    foreign_identity.id[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, cancel_primary.nativeIndexOwnerCancellationState(foreign_identity));
    try std.testing.expectEqualStrings("TestPublicationUnavailable", canceled.error_name.?);
    {
        var read = try cancel_primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try jobs.load(&read) == null);
        try std.testing.expectEqual(canceled.position, (try cancellation.load(&read)).?.position);
        try jobs.verifyRecovery(alloc, &read, null);
    }
    const cancel_payload = try pending_cancel.finalizePayload(canceled.position);
    try std.testing.expectEqualSlices(u8, cancel_payload, cancel_publisher.payload.?);
    cancel_primary.local_execution.replication_async_batch_mirror = null;
    cancel_primary.close();
    cancel_primary = try db_mod.DB.open(alloc, cancel_dir.path(), options);
    cancel_publisher.fail = false;
    cancel_primary.local_execution.replication_async_batch_mirror = cancel_publisher.asMirror();
    try cancel_primary.flushDurableReplicationOutboxes();
    try std.testing.expectEqual(@as(usize, 2), cancel_publisher.attempts);
    try std.testing.expectEqual(@as(usize, 1), cancel_publisher.completed);
    {
        var pending = try outbox_store.readPending(alloc, cancel_primary.core.store);
        defer pending.deinit(alloc);
        try std.testing.expect(pending.isEmpty());
    }
    const retried = try cancel_primary.applyPreparedNativeIndexOwnerCancellation(&pending_cancel, null);
    try std.testing.expect(retried.duplicate);
    try std.testing.expect(!retried.replication_pending);
    try std.testing.expectEqual(canceled.position, retried.position);
    const cancel_record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = cancel_payload };
    var cancel_decoded = try effects.decodeBatchMutationRequest(alloc, cancel_record);
    defer cancel_decoded.deinit();
    var smuggled = cancel_decoded.value;
    smuggled.request.writes = &.{.{ .key = "smuggled", .value = "{}" }};
    try std.testing.expectError(error.InvalidBatchRequest, effects.validateIndexOwnerPayload(smuggled));
    smuggled = cancel_decoded.value;
    smuggled.ordinary_raft_entry = .{ .term = 1, .index = 1 };
    try std.testing.expectError(error.InvalidBatchRequest, effects.validateIndexOwnerPayload(smuggled));
    const extra_field = try std.fmt.allocPrint(alloc, "{s},\"unknown_control\":true}}", .{cancel_payload[0 .. cancel_payload.len - 1]});
    defer alloc.free(extra_field);
    var unknown_cancel = cancel_record;
    unknown_cancel.payload = extra_field;
    try std.testing.expectError(error.InvalidBatchRequest, effects.decodeBatchMutationRequest(alloc, unknown_cancel));
    const union_boundary = std.mem.indexOf(u8, cancel_payload, "},\"position\":{").?;
    var cleanup_binding = binding;
    cleanup_binding.phase = .reserved;
    const binding_json = try std.json.Stringify.valueAlloc(alloc, cleanup_binding, .{});
    defer alloc.free(binding_json);
    const mixed_effects = try std.fmt.allocPrint(alloc, "{s},\"cancel_cleanup\":{s}{s}", .{ cancel_payload[0..union_boundary], binding_json, cancel_payload[union_boundary..] });
    defer alloc.free(mixed_effects);
    // Valid JSON, but not one typed effect. Do not accidentally accept the
    // first arm and silently discard a second operation from the same frame.
    var valid_json = try std.json.parseFromSlice(std.json.Value, alloc, mixed_effects, .{});
    defer valid_json.deinit();
    unknown_cancel.payload = mixed_effects;
    if (effects.decodeBatchMutationRequest(alloc, unknown_cancel)) |accepted_value| {
        var accepted = accepted_value;
        accepted.deinit();
        return error.MixedNativeIndexEffectsAccepted;
    } else |err| if (err == error.OutOfMemory) return err;
    var cancel_gap = try replication_ingress.normalizeBatch(cancel_decoded.value);
    cancel_gap.index_owner_control.?.position.sequence += 1;
    cancel_gap.receipt = .{ .native = cancel_gap.index_owner_control.?.position };
    try std.testing.expectError(error.OnlineSourceScopeChanged, follower.applyReplicatedBatch(cancel_gap, 1));
    try std.testing.expectEqual(@as(u64, 0), try follower.replicationAppliedSequence());
    try replication_ingress.applyRecord(&follower, cancel_record);
    try replication_ingress.applyRecord(&follower, cancel_record);
    follower.close();
    follower = try db_mod.DB.open(alloc, follower_dir.path(), options);
    try std.testing.expectEqual(@as(u64, 1), try follower.replicationAppliedSequence());
    {
        var read = try follower.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try jobs.load(&read) == null);
        try jobs.verifyRecovery(alloc, &read, null);
        try std.testing.expectEqual(canceled.position, (try cancellation.load(&read)).?.position);
        try std.testing.expectEqual(canceled.position.sequence, (try authority.load(&read)).?.sequence);
    }
    var late_admission = try follower.prepareNativeIndexOwnerAdmission(intent, candidate.inputs.bytes);
    defer late_admission.deinit();
    try std.testing.expectError(error.PrivateIndexPublicationCanceled, follower.applyPreparedNativeIndexOwnerControl(&late_admission, null));

    // Drain and burn now use the real DB commit/outbox/replay pipeline too.
    // Each committed mirror failure retains its exact payload and cut; ordinary
    // controls remain canceled and none of these outcomes has a receipt field.
    for ([_]jobs.NativeEffect{ .admitted, .fenced, .reserved }, 0..) |effect, ordinal| {
        alloc.free(cancel_publisher.payload.?);
        cancel_publisher.payload = null;
        cancel_publisher.lsn = 2 + ordinal;
        cancel_publisher.fail = true;
        var canceled_control = if (effect == .admitted)
            try cancel_primary.prepareNativeIndexOwnerCancellationAdmission(intent, candidate.inputs.bytes)
        else
            try cancel_primary.prepareNativeIndexOwnerCancellationControl(.{ .effect = effect, .binding = binding });
        defer canceled_control.deinit();
        const progress_outcome = try cancel_primary.applyPreparedNativeIndexOwnerCancellationControl(&canceled_control, null);
        try std.testing.expect(progress_outcome.replication_pending);
        try std.testing.expect(!progress_outcome.duplicate);
        try std.testing.expectEqual(@as(u64, 2 + ordinal), progress_outcome.position.sequence);
        const progress_payload = try canceled_control.finalizePayload(progress_outcome.position);
        try std.testing.expectEqualSlices(u8, progress_payload, cancel_publisher.payload.?);
        cancel_publisher.fail = false;
        try cancel_primary.flushDurableReplicationOutboxes();
        const duplicate_progress = try cancel_primary.applyPreparedNativeIndexOwnerCancellationControl(&canceled_control, null);
        try std.testing.expect(duplicate_progress.duplicate);
        try std.testing.expectEqual(progress_outcome.position, duplicate_progress.position);
        var progress_record = cancel_record;
        progress_record.lsn = 2 + ordinal;
        progress_record.previous_lsn = 1 + ordinal;
        progress_record.payload = progress_payload;
        var decoded_progress = try effects.decodeBatchMutationRequest(alloc, progress_record);
        defer decoded_progress.deinit();
        try std.testing.expect(decoded_progress.value.index_owner_control.?.effect == .cancel_control);
        var progress_gap = try replication_ingress.normalizeBatch(decoded_progress.value);
        progress_gap.index_owner_control.?.position.sequence += 1;
        progress_gap.receipt = .{ .native = progress_gap.index_owner_control.?.position };
        try std.testing.expectError(error.OnlineSourceScopeChanged, follower.applyReplicatedBatch(progress_gap, progress_record.lsn));
        try std.testing.expectEqual(progress_record.previous_lsn, try follower.replicationAppliedSequence());
        try replication_ingress.applyRecord(&follower, progress_record);
        try replication_ingress.applyRecord(&follower, progress_record);
        follower.close();
        follower = try db_mod.DB.open(alloc, follower_dir.path(), options);
        try std.testing.expectEqual(progress_record.lsn, try follower.replicationAppliedSequence());
        const status = try follower.nativeIndexOwnerCancellationState(receipt_identity);
        try std.testing.expectEqual(@as(u64, 2 + ordinal), status.source.sequence);
        try std.testing.expectEqual(canceled.position, status.intent.?.position);
        try std.testing.expectEqual(@as(u64, 2), status.admitted.?.sequence);
        try std.testing.expectEqual(ordinal >= 1, status.fenced != null);
        try std.testing.expectEqual(ordinal >= 2, status.reserved != null);
        if (status.fenced) |cut| try std.testing.expectEqual(@as(u64, 3), cut.sequence);
        if (status.reserved) |cut| try std.testing.expectEqual(@as(u64, 4), cut.sequence);
        try std.testing.expect(status.cleanup == null and !status.artifacts_retired);
    }
    alloc.free(cancel_publisher.payload.?);
    cancel_publisher.payload = null;
    cancel_publisher.lsn = 5;
    cancel_publisher.fail = true;
    var cleanup = try cancel_primary.prepareNativeIndexOwnerCleanup(receipt_identity);
    defer cleanup.deinit();
    const admitted_cleanup = try cancel_primary.applyPreparedNativeIndexOwnerCleanup(&cleanup, null);
    try std.testing.expect(admitted_cleanup.replication_pending);
    try std.testing.expect(!admitted_cleanup.duplicate);
    try std.testing.expectEqual(@as(u64, 5), admitted_cleanup.position.sequence);
    const cleanup_payload = try cleanup.finalizePayload(admitted_cleanup.position);
    try std.testing.expectEqualSlices(u8, cleanup_payload, cancel_publisher.payload.?);
    cancel_primary.local_execution.replication_async_batch_mirror = null;
    cancel_primary.close();
    cancel_primary = try db_mod.DB.open(alloc, cancel_dir.path(), options);
    cancel_publisher.fail = false;
    cancel_primary.local_execution.replication_async_batch_mirror = cancel_publisher.asMirror();
    try cancel_primary.flushDurableReplicationOutboxes();
    const duplicate_cleanup = try cancel_primary.applyPreparedNativeIndexOwnerCleanup(&cleanup, null);
    try std.testing.expect(duplicate_cleanup.duplicate);
    try std.testing.expectEqual(admitted_cleanup.position, duplicate_cleanup.position);
    var cleanup_record = cancel_record;
    cleanup_record.lsn = 5;
    cleanup_record.previous_lsn = 4;
    cleanup_record.payload = cleanup_payload;
    var decoded_cleanup = try effects.decodeBatchMutationRequest(alloc, cleanup_record);
    defer decoded_cleanup.deinit();
    try std.testing.expect(decoded_cleanup.value.index_owner_control.?.effect == .cancel_cleanup);
    var cleanup_gap = try replication_ingress.normalizeBatch(decoded_cleanup.value);
    cleanup_gap.index_owner_control.?.position.sequence += 1;
    cleanup_gap.receipt = .{ .native = cleanup_gap.index_owner_control.?.position };
    try std.testing.expectError(error.OnlineSourceScopeChanged, follower.applyReplicatedBatch(cleanup_gap, 5));
    try std.testing.expectEqual(@as(u64, 4), try follower.replicationAppliedSequence());
    try replication_ingress.applyRecord(&follower, cleanup_record);
    try replication_ingress.applyRecord(&follower, cleanup_record);
    follower.close();
    follower = try db_mod.DB.open(alloc, follower_dir.path(), options);
    try std.testing.expectEqual(@as(u64, 5), try follower.replicationAppliedSequence());
    const queued = try follower.nativeIndexOwnerCancellationState(receipt_identity);
    try std.testing.expectEqual(@as(u64, 5), queued.cleanup.?.sequence);
    try std.testing.expect(queued.claim_allocation != null);
    try std.testing.expect(!queued.artifacts_retired);
    {
        var read = try follower.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(admitted_cleanup.position, (try jobs.CancellationCleanupRecord.decode(try read.get(jobs.cancellation_cleanup_key))).position);
        try std.testing.expectError(error.PrivateIndexCancellationPending, jobs.requireCancellationArtifactsRetired(&read, binding));
        try std.testing.expect(try @import("relational_integrity_topology.zig").completed(&read) == null);
    }

    // Standby-local derived cleanup must not weaken ordinary admission. An
    // unrelated queued generation is untouched by the targeted worker.
    const Denied = struct {
        fn check(_: *const anyopaque) anyerror!void {
            return error.WriteAdmissionRejected;
        }
    };
    var denied_context: u8 = 0;
    follower.local_execution.replication_write_gate = .{ .borrowed = .{
        .ptr = &denied_context,
        .check_fn = Denied.check,
        .allows_background_work = false,
    } };
    defer follower.local_execution.replication_write_gate = null;
    const gc = @import("relational_index_gc.zig");
    try std.testing.expect(binding.index.generation > 1);
    const unrelated: records.Id = .{ .generation = binding.index.generation - 1, .slot = binding.index.slot };
    try std.testing.expect(std.mem.order(u8, &gc.key(unrelated), &gc.key(binding.index)) == .lt);
    try follower.core.store.putBatch(&.{.{ .key = &gc.key(unrelated), .value = &gc.initial(unrelated) }}, &.{});
    try std.testing.expectError(error.WriteAdmissionRejected, follower.collectRelationalIndexGarbageStep());
    const first_page = try follower.collectCanceledNativeIndexGarbageStep(receipt_identity);
    try std.testing.expect(first_page.advanced);
    try std.testing.expect(!first_page.artifacts_retired);
    {
        var completion_txn = try follower.core.store.beginWriteTxn();
        defer completion_txn.abort();
        var manager = try follower.core.initTxnManager();
        defer manager.deinit();
        try std.testing.expectError(error.PrivateIndexCancellationPending, jobs.Canceled.stage(&completion_txn, &manager, cleanup_binding, null));
    }
    // Resume the admitted generation after a restart between bounded phases.
    follower.local_execution.replication_write_gate = null;
    follower.close();
    follower = try db_mod.DB.open(alloc, follower_dir.path(), options);
    follower.local_execution.replication_write_gate = .{ .borrowed = .{
        .ptr = &denied_context,
        .check_fn = Denied.check,
        .allows_background_work = false,
    } };
    const final_page = try follower.collectCanceledNativeIndexGarbageStep(receipt_identity);
    try std.testing.expect(final_page.advanced);
    try std.testing.expect(final_page.artifacts_retired);
    const duplicate_page = try follower.collectCanceledNativeIndexGarbageStep(receipt_identity);
    try std.testing.expect(!duplicate_page.advanced);
    try std.testing.expect(duplicate_page.artifacts_retired);
    const retired_status = try follower.nativeIndexOwnerCancellationState(receipt_identity);
    try std.testing.expect(retired_status.artifacts_retired);
    try std.testing.expect(retired_status.binding != null);
    try std.testing.expectEqual(@as(u64, 5), retired_status.source.sequence);
    try std.testing.expectError(error.WriteAdmissionRejected, follower.collectRelationalIndexGarbageStep());
    try std.testing.expectEqual(@as(u64, 5), try follower.replicationAppliedSequence());
    {
        var read = try follower.core.store.beginReadTxn();
        defer read.abort();
        try jobs.requireCancellationArtifactsRetired(&read, binding);
        try std.testing.expectEqualSlices(u8, &gc.initial(unrelated), try read.get(&gc.key(unrelated)));
        try std.testing.expectEqual(@as(u64, 5), (try authority.require(&read, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace))).sequence);
        try std.testing.expect(try @import("relational_integrity_topology.zig").completed(&read) == null);
    }
    follower.local_execution.replication_write_gate = null;
    try testNativeCancellationCompletion(&follower, cleanup_binding, options);
}

fn testNativeCancellationCompletion(source: *db_mod.DB, binding: @import("relational_index_owner_job.zig").Binding, options: db_mod.OpenOptions) !void {
    const jobs = @import("relational_index_owner_job.zig");
    const native = @import("relational_index_owner_control.zig");
    const topology = @import("relational_integrity_topology.zig");
    const authority = @import("../source_authority.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    var dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-cancel-complete-primary");
    defer dir.cleanup();
    var standby_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-cancel-complete-standby");
    defer standby_dir.cleanup();
    var primary = try db_mod.DB.open(alloc, dir.path(), options);
    defer primary.close();
    try seedNativeIndexFixture(source, &primary);
    var standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    defer standby.close();
    try seedNativeIndexFixture(source, &standby);
    const Fault = struct {
        txn: *docstore.DocStore.Txn,
        fail_at: usize,
        after: bool,
        writes: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.txn.get(key);
        }
        pub fn openCursor(self: *@This()) !docstore.DocStore.Txn.CursorAdapter {
            return self.txn.openCursor();
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (!self.after and self.writes + 1 == self.fail_at) return error.TestCancellationWriteFailed;
            try self.txn.put(key, value);
            self.writes += 1;
            if (self.after and self.writes == self.fail_at) return error.TestCancellationWriteFailed;
        }
    };
    for (1..3) |fail_at| for ([_]bool{ false, true }) |after| {
        {
            var txn = try primary.core.store.beginWriteTxn();
            defer txn.abort();
            var fault: Fault = .{ .txn = &txn, .fail_at = fail_at, .after = after };
            var manager = try primary.core.initTxnManager();
            defer manager.deinit();
            try std.testing.expectError(error.TestCancellationWriteFailed, jobs.Canceled.stage(&fault, &manager, binding, null));
        }
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try jobs.Canceled.load(&read) == null);
        try std.testing.expectEqual(@as(u64, 5), (try authority.load(&read)).?.sequence);
        try std.testing.expect((try topology.current(&read)).?.eql(binding.fence));
    };
    var barrier: Barrier = .{};
    var publisher: TestNativeIndexPublisher = .{ .barrier = &barrier, .lsn = 6 };
    defer if (publisher.payload) |bytes| alloc.free(bytes);
    primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
    defer primary.local_execution.replication_async_batch_mirror = null;
    var prepared: native.PreparedCancellationCompletion = .{ .alloc = alloc, .binding = binding };
    defer prepared.deinit();
    const pending = try primary.applyPreparedNativeIndexOwnerCancellationCompletion(&prepared, null);
    try pending.validate();
    try std.testing.expect(pending.replication_pending and pending.receipt == null and pending.record != null);
    try std.testing.expectEqual(@as(u64, 6), pending.position.sequence);
    // Canonical codec, truncation, checksum and semantic-cut validation.
    const encoded = try pending.record.?.encode();
    try std.testing.expectEqual(pending.record.?, try jobs.Canceled.decode(&encoded));
    for (0..encoded.len) |len| try std.testing.expectError(error.InvalidPrivateIndexCancellation, jobs.Canceled.decode(encoded[0..len]));
    var corrupt = encoded;
    corrupt[1178] = 1;
    std.crypto.hash.Blake3.hash(corrupt[0..1184], corrupt[1184..1216], .{});
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, jobs.Canceled.decode(&corrupt));
    var invalid = pending.record.?;
    invalid.position = invalid.cleanup.position;
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, invalid.encode());
    publisher.fail = false;
    primary.close();
    primary = try db_mod.DB.open(alloc, dir.path(), options);
    primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
    const retried = try primary.applyPreparedNativeIndexOwnerCancellationCompletion(&prepared, null);
    try retried.validate();
    try std.testing.expect(retried.duplicate and !retried.replication_pending);
    try std.testing.expectEqual(pending.position, retried.position);
    try std.testing.expectEqual(.canceled, retried.receipt.?.action);
    const record_packet: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 6, .previous_lsn = 5, .payload = publisher.payload.? };
    try replication_ingress.applyRecord(&standby, record_packet);
    try replication_ingress.applyRecord(&standby, record_packet);
    standby.close();
    standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    try std.testing.expectEqual(@as(u64, 6), try standby.replicationAppliedSequence());
    for ([_]*db_mod.DB{ &primary, &standby }) |db| {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var active = (try @import("relational_index_catalog.zig").load(alloc, &read)).?;
        defer active.deinit();
        try jobs.verifyRecovery(alloc, &read, &active);
        try std.testing.expectEqual(retried.record.?, (try jobs.Canceled.load(&read)).?);
        try std.testing.expect((try topology.current(&read)).?.eql(binding.fence));
        try std.testing.expect(try topology.completed(&read) == null);
        try std.testing.expectEqual(binding, (try jobs.load(&read)).?);
    }
    primary.local_execution.replication_async_batch_mirror = null;
    try testNativeCancellationRelease(&primary, &standby, retried.record.?, options, dir.path(), standby_dir.path());
}

fn testNativeCancellationRelease(primary: *db_mod.DB, standby: *db_mod.DB, canceled: @import("relational_index_owner_job.zig").Canceled, options: db_mod.OpenOptions, primary_path: []const u8, standby_path: []const u8) !void {
    const release = @import("relational_index_release.zig");
    const jobs = @import("relational_index_owner_job.zig");
    const topology = @import("relational_integrity_topology.zig");
    const authority = @import("../source_authority.zig");
    const intent: release.CancellationIntent = .{ .canceled = canceled, .metadata_decision = @splat(12) };
    const Fault = struct {
        txn: *docstore.DocStore.Txn,
        fail_at: ?usize = null,
        after: bool = false,
        writes: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.txn.get(key);
        }
        pub fn openCursor(self: *@This()) !docstore.DocStore.Txn.CursorAdapter {
            return self.txn.openCursor();
        }
        fn before(self: *@This()) !void {
            if (!self.after and self.fail_at == self.writes + 1) return error.TestCancellationReleaseWriteFailed;
        }
        fn completed(self: *@This()) !void {
            self.writes += 1;
            if (self.after and self.fail_at == self.writes) return error.TestCancellationReleaseWriteFailed;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.before();
            try self.txn.put(key, value);
            try self.completed();
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            try self.before();
            try self.txn.delete(key);
            try self.completed();
        }
    };
    const writes = count: {
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        var manager = try primary.core.initTxnManager();
        defer manager.deinit();
        var counter: Fault = .{ .txn = &txn };
        _ = try release.CancellationRecord.stage(&counter, &manager, intent, null);
        break :count counter.writes;
    };
    try std.testing.expectEqual(@as(usize, 11), writes);
    for (1..writes + 1) |fail_at| for ([_]bool{ false, true }) |after| {
        {
            var txn = try primary.core.store.beginWriteTxn();
            defer txn.abort();
            var manager = try primary.core.initTxnManager();
            defer manager.deinit();
            var fault: Fault = .{ .txn = &txn, .fail_at = fail_at, .after = after };
            try std.testing.expectError(error.TestCancellationReleaseWriteFailed, release.CancellationRecord.stage(&fault, &manager, intent, null));
        }
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try intent.requireCurrent(&read);
        try std.testing.expect(try release.CancellationRecord.load(&read, intent) == null);
        try std.testing.expectEqual(canceled.position.sequence, (try authority.load(&read)).?.sequence);
    };
    {
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        const stream = @import("relational_index_private_claims.zig").SourceStream;
        try txn.put(&stream.key(canceled.binding.id, 999, true), "orphan");
        var manager = try primary.core.initTxnManager();
        defer manager.deinit();
        try std.testing.expectError(error.PrivateIndexCancellationPending, release.CancellationRecord.stage(&txn, &manager, intent, null));
    }
    var barrier: @import("antfly_runtime_abi").mutation_barrier.MutationBarrier = .{};
    var publisher: TestNativeIndexPublisher = .{ .barrier = &barrier, .lsn = 7 };
    defer if (publisher.payload) |bytes| alloc.free(bytes);
    defer primary.local_execution.replication_async_batch_mirror = null;
    primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
    var prepared = try primary.prepareNativeIndexOwnerCancellationRelease(intent, .{});
    defer prepared.deinit();
    const pending = try primary.applyPreparedNativeIndexOwnerCancellationRelease(&prepared, null);
    try pending.validate();
    try std.testing.expect(pending.replication_pending and pending.receipt == null and !pending.duplicate);
    try std.testing.expectEqual(canceled.position.sequence + 1, pending.position.sequence);
    const encoded = try (release.CancellationRecord{ .intent = intent, .position = pending.position }).encode();
    for (0..encoded.len) |len| try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, release.CancellationRecord.decode(encoded[0..len]));
    var corrupt = encoded;
    corrupt[1289] = 1;
    std.crypto.hash.Blake3.hash(corrupt[0..1296], corrupt[1296..1328], .{});
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, release.CancellationRecord.decode(&corrupt));
    primary.local_execution.replication_async_batch_mirror = null;
    primary.close();
    primary.* = try db_mod.DB.open(alloc, primary_path, options);
    publisher.fail = false;
    primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
    // Retry itself flushes the recovered outbox before taking a mutation lease.
    var retry = try primary.prepareNativeIndexOwnerCancellationRelease(intent, .{});
    defer retry.deinit();
    const recovered = try primary.applyPreparedNativeIndexOwnerCancellationRelease(&retry, null);
    try recovered.validate();
    try std.testing.expect(recovered.duplicate and !recovered.replication_pending);
    try std.testing.expectEqualDeep(try intent.receipt(pending.position), recovered.receipt.?);
    const packet: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 7, .previous_lsn = 6, .payload = publisher.payload.? };
    try replication_ingress.applyRecord(standby, packet);
    try replication_ingress.applyRecord(standby, packet);
    standby.close();
    standby.* = try db_mod.DB.open(alloc, standby_path, options);
    try std.testing.expectEqual(@as(u64, 7), try standby.replicationAppliedSequence());
    for ([_]*db_mod.DB{ primary, standby }) |db| {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var active = (try @import("relational_index_catalog.zig").load(alloc, &read)).?;
        defer active.deinit();
        try jobs.verifyRecovery(alloc, &read, &active);
        try std.testing.expect(try jobs.load(&read) == null);
        try std.testing.expect(try topology.current(&read) == null);
        try std.testing.expect((try topology.completed(&read)).?.eql(canceled.binding.fence));
        try std.testing.expectEqual(pending.position, (try release.CancellationRecord.load(&read, intent)).?.position);
        try topology.requireUnfenced(&read);
    }
    {
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        var foreign = (try topology.completed(&txn)).?;
        foreign.admission_epoch += 1;
        foreign.namespace.range_id += 1;
        try txn.put(topology.receipt_key, &try foreign.encode());
        try std.testing.expectError(error.IntegrityTopologyChanged, release.CancellationRecord.verifyRecovery(&txn));
    }
    primary.local_execution.replication_async_batch_mirror = null;
    try primary.batch(.{ .relational_schema_version = 1, .writes = &.{.{ .key = "after-cancel-release", .value = "{\"id\":99,\"email\":\"after-cancel@example.com\"}" }} });
    const written = (try primary.get(alloc, "after-cancel-release")).?;
    defer alloc.free(written);
    try std.testing.expectEqualStrings("{\"id\":99,\"email\":\"after-cancel@example.com\"}", written);
    primary.close();
    primary.* = try db_mod.DB.open(alloc, primary_path, options);
    var after_write = try primary.prepareNativeIndexOwnerCancellationRelease(intent, .{});
    defer after_write.deinit();
    const duplicate = try primary.applyPreparedNativeIndexOwnerCancellationRelease(&after_write, null);
    try duplicate.validate();
    try std.testing.expect(duplicate.duplicate);
    try std.testing.expectEqual(pending.position, duplicate.position);
}

fn testInstalledIndexCheckpoint(database: *db_mod.DB, root_path: []const u8, intent: @import("relational_index_validation.zig").InstallIntent) !@import("relational_index_audit_checkpoint.zig").Summary {
    const native = @import("relational_index_owner_control.zig");
    const io = database.backend_runtime.io().?;
    const signer = try @import("root_signing_identity.zig").loadOrCreate(alloc, io, root_path);
    const start_compilations = database.native_index_build_cache.compilations;
    const page_budget: @import("relational_index_jobs.zig").Budget = .{ .records = 4, .time_ns = std.time.ns_per_s };
    const installation: native.InstallationRequest = .{ .intent = intent, .budget = page_budget };
    var completed: ?@import("relational_index_audit_checkpoint.zig").Summary = null;
    var partial_pages: usize = 0;
    for (0..128) |_| {
        const response = try database.stepNativeIndexOwnerInstallation("items", installation, root_path, signer.root_incarnation, .{});
        try response.validateFor(installation);
        const page = response.physicalAudit().audit;
        completed = page;
        try page.validateFor(intent.validation.command.expected, signer.root_incarnation);
        if (response == .installed) {
            try std.testing.expect(response.installed.logical.duplicate);
            try std.testing.expect(response.installed.logical.receipt != null);
            var mismatched = response;
            mismatched.installed.physical.audit.physical_root_incarnation = 0;
            try std.testing.expectError(error.InvalidRelationalIndexValidation, mismatched.validateFor(installation));
        } else partial_pages += 1;
        if (page.inventory.phase == .failed or (page.generation != null and (page.generation.?.phase == .ready or page.generation.?.phase == .failed))) break;
    }
    try std.testing.expect(partial_pages > 1);
    try std.testing.expectEqual(@import("relational_index_validation.zig").Phase.ready, completed.?.generation.?.phase);
    try std.testing.expectEqual(start_compilations + 1, database.native_index_build_cache.compilations);
    try std.testing.expect(database.native_index_build_cache.hits > 1);
    const repeated = try database.stepNativeIndexOwnerInstallation("items", installation, root_path, signer.root_incarnation, .{});
    try std.testing.expect(repeated == .installed);
    try std.testing.expectEqualDeep(completed.?, repeated.installed.physical.audit);
    var changed_intent = intent;
    changed_intent.metadata_decision[0] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, database.stepNativeIndexOwnerInstallationAudit("items", changed_intent, page_budget, root_path, signer.root_incarnation, .{}));
    {
        const fence_key = @import("relational_integrity_topology.zig").fence_key;
        const saved_fence = try database.core.store.get(alloc, fence_key);
        defer alloc.free(saved_fence);
        try database.core.store.delete(fence_key);
        defer database.core.store.put(fence_key, saved_fence) catch unreachable;
        try std.testing.expectError(error.IntegrityTopologyFenceMissing, database.stepNativeIndexOwnerInstallationAudit("items", intent, page_budget, root_path, signer.root_incarnation, .{}));
    }
    const binding = intent.validation.command.binding;
    const identity: native.Identity = .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = binding.fence.owner_group_id };
    var request = try native.BuildRequest.init(identity, binding, page_budget);
    request.target_expectation = intent.validation.command.expected;
    {
        var lease = try database.native_index_build_cache.acquireInstalledAudit(io, alloc, database.core.index_manager.resource_manager, database.core, request, intent, .{});
        defer lease.deinit();
        database.native_index_build_cache.evict(io);
        var read = try database.core.store.beginReadTxn();
        defer read.abort();
        const history = lease.installedHistory().?;
        try history.record.requireCurrent(alloc, &read);
        var historical = history.reader(&read);
        try @import("relational_index_owner_job.zig").requireReserved(&historical, binding);
    }
    const reloaded = try database.stepNativeIndexOwnerInstallationAudit("items", intent, page_budget, root_path, signer.root_incarnation, .{});
    try std.testing.expectEqualDeep(completed.?, reloaded);
    try std.testing.expectEqual(start_compilations + 2, database.native_index_build_cache.compilations);
    return reloaded;
}

/// Storage/replay qualification only, not metadata release permission or root
/// adoption. Each fixture owns separate identity files and copied logical KV.
fn testNativeIndexRelease(source: *db_mod.DB, installed: @import("relational_index_validation.zig").Installed) !void {
    const release_mod = @import("relational_index_release.zig");
    const topology = @import("relational_integrity_topology.zig");
    const jobs = @import("relational_index_owner_job.zig");
    const authority = @import("../source_authority.zig");
    const binding = installed.command.validation.command.binding;
    const intent: release_mod.Intent = .{ .installed = installed, .metadata_decision = @splat(11) };
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} }, .online_source_authority = .native, .identity_namespace = binding.fence.namespace };
    var primary_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-release-primary");
    defer primary_dir.cleanup();
    var primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
    var primary_open = true;
    defer if (primary_open) primary.close();
    try seedNativeIndexFixture(source, &primary);
    primary.close();
    primary_open = false;
    primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
    primary_open = true;
    const Fault = struct {
        txn: *docstore.DocStore.Txn,
        fail_at: ?usize = null,
        after: bool = false,
        writes: usize = 0,
        fn before(self: *@This()) !void {
            if (self.fail_at == self.writes + 1 and !self.after) return error.TestReleaseWriteFailed;
        }
        fn completed(self: *@This()) !void {
            self.writes += 1;
            if (self.fail_at == self.writes and self.after) return error.TestReleaseWriteFailed;
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.txn.get(key);
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.before();
            try self.txn.put(key, value);
            try self.completed();
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            try self.before();
            try self.txn.delete(key);
            try self.completed();
        }
    };
    const write_count = count: {
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        var counter: Fault = .{ .txn = &txn };
        _ = try release_mod.Record.stage(alloc, &counter, intent, null);
        break :count counter.writes;
    };
    try std.testing.expect(write_count > 10);
    for (1..write_count + 1) |fail_at| for ([_]bool{ false, true }) |after| {
        {
            var txn = try primary.core.store.beginWriteTxn();
            defer txn.abort();
            var fault: Fault = .{ .txn = &txn, .fail_at = fail_at, .after = after };
            try std.testing.expectError(error.TestReleaseWriteFailed, release_mod.Record.stage(alloc, &fault, intent, null));
            try std.testing.expectEqual(if (after) fail_at else fail_at - 1, fault.writes);
        }
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try intent.requireInstalled(alloc, &read);
        try std.testing.expect(try release_mod.Record.load(alloc, &read, intent) == null);
        try std.testing.expectEqual(installed.position.sequence, (try authority.load(&read)).?.sequence);
    };
    var barrier: @import("antfly_runtime_abi").mutation_barrier.MutationBarrier = .{};
    var publisher: TestNativeIndexPublisher = .{ .barrier = &barrier, .lsn = 7 };
    defer if (publisher.payload) |bytes| alloc.free(bytes);
    defer if (primary_open) {
        primary.local_execution.replication_async_batch_mirror = null;
    };
    primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
    const pending = pending: {
        var prepared = try primary.prepareNativeIndexOwnerRelease(intent, .{});
        defer prepared.deinit();
        break :pending try primary.applyPreparedNativeIndexOwnerRelease(&prepared, null);
    };
    try pending.validate();
    try std.testing.expect(!pending.duplicate and pending.replication_pending and pending.receipt == null);
    try std.testing.expectEqualStrings("TestPublicationUnavailable", pending.error_name.?);
    try std.testing.expectEqual(@as(usize, 1), publisher.attempts);
    try std.testing.expectEqual(installed.position.sequence + 1, pending.position.sequence);
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try jobs.load(&read) == null);
        try std.testing.expect(try topology.current(&read) == null);
        try std.testing.expectError(error.NotFound, read.get(@import("relational_index_inputs.zig").key));
        try std.testing.expectError(error.NotFound, read.get(@import("relational_index_validation.zig").Installed.key));
        try release_mod.Record.verifyRecovery(alloc, &read);
        try std.testing.expectEqual(pending.position, (try release_mod.Record.load(alloc, &read, intent)).?.position);
        const dirty = try @import("receipt_position.zig").Position.decode(try read.get(release_mod.Record.retirement_pending_key));
        try std.testing.expectEqual(pending.position, dirty.native);
    }
    primary.local_execution.replication_async_batch_mirror = null;
    primary.close();
    primary_open = false;
    primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
    primary_open = true;
    publisher.fail = false;
    primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
    try primary.flushDurableReplicationOutboxes();
    try std.testing.expectEqual(@as(usize, 2), publisher.attempts);
    try std.testing.expectEqual(@as(usize, 1), publisher.completed);
    var prepared_retry = try primary.prepareNativeIndexOwnerRelease(intent, .{});
    defer prepared_retry.deinit();
    const recovered = try primary.applyPreparedNativeIndexOwnerRelease(&prepared_retry, null);
    try recovered.validate();
    try std.testing.expect(recovered.duplicate and !recovered.replication_pending);
    try std.testing.expectEqualDeep(try intent.receipt(pending.position), recovered.receipt.?);
    primary.local_execution.replication_async_batch_mirror = null;
    {
        var original_inputs = original: {
            var read = try source.core.store.beginReadTxn();
            defer read.abort();
            break :original try @import("relational_index_inputs.zig").Prepared.load(alloc, &read, intent.installed.command.validation.command.binding.inputs_digest);
        };
        defer original_inputs.deinit();
        var attempted_binding = intent.installed.command.validation.command.binding;
        attempted_binding.phase = .fencing;
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.GenerationRetirementPending, jobs.stageBegin(&txn, attempted_binding, &original_inputs));
        try release_mod.Record.verifyRecovery(alloc, &txn);
    }
    try std.testing.expectError(error.ForeignKeyCoordinationRequired, primary.batch(.{ .writes = &.{.{ .key = "after-release", .value = "{\"id\":99,\"email\":\"after-release@example.com\"}" }} }));
    const catalog = @import("relational_integrity_catalog.zig");
    const integrity = @import("relational_integrity.zig");
    const catalog_bytes = (try primary.core.getStoreValue(alloc, catalog.key)).?;
    defer alloc.free(catalog_bytes);
    var active_catalog = try catalog.decode(alloc, catalog_bytes);
    defer active_catalog.deinit();
    const generation = active_catalog.find(.unique, "email_idx").?.generation;
    const generation_set = @import("relational_integrity_activation.zig").generationSet(active_catalog);
    var active_view = primary.core.acquireSchemaView().?;
    defer active_view.release();
    var tuple_plan = try @import("relational_index_keys.zig").TuplePlan.init(alloc, active_view.tableSchema().*, active_view.physicalLayout(), &.{.{ .column = "email" }});
    defer tuple_plan.deinit();
    var tuple: std.ArrayList(u8) = .empty;
    defer tuple.deinit(alloc);
    _ = try tuple_plan.appendValues(alloc, &tuple, &.{.{ .string = "after-release@example.com" }});
    const create = try primary.beginTransactionWithId(@splat(91), 100);
    try primary.writeTransaction(create, .{
        .relational_schema_version = 2,
        .relational_integrity_generation_set = generation_set,
        .writes = &.{.{ .key = "after-release", .value = "{\"id\":99,\"email\":\"after-release@example.com\"}" }},
        .integrity_commands = &.{.{ .address = try integrity.Address.init(generation, tuple.items), .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "items", .parent_key = "after-release", .schema_version = 2 } } }},
    });
    try primary.commitTransaction(create, 200);
    const written = (try primary.get(alloc, "after-release")).?;
    defer alloc.free(written);
    try std.testing.expectEqualStrings("{\"id\":99,\"email\":\"after-release@example.com\"}", written);
    tuple.clearRetainingCapacity();
    _ = try tuple_plan.appendValues(alloc, &tuple, &.{.{ .string = "0@example.com" }});
    const duplicate = try primary.beginTransactionWithId(@splat(92), 300);
    try std.testing.expectError(error.UniqueConstraintViolation, primary.writeTransaction(duplicate, .{
        .relational_schema_version = 2,
        .relational_integrity_generation_set = generation_set,
        .writes = &.{.{ .key = "duplicate-live", .value = "{\"id\":100,\"email\":\"0@example.com\"}" }},
        .integrity_commands = &.{.{ .address = try integrity.Address.init(generation, tuple.items), .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "items", .parent_key = "duplicate-live", .schema_version = 2 } } }},
    }));
    try primary.abortTransaction(duplicate, 400);
    try std.testing.expect(try primary.get(alloc, "duplicate-live") == null);
    {
        var old_layout = (try primary.core.acquireSchemaVersionView(1)).?;
        defer old_layout.release();
        try std.testing.expectEqual(@as(u32, 1), old_layout.version());
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try release_mod.Record.verifyRecovery(alloc, &read);
    }
    var standby_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-release-standby");
    defer standby_dir.cleanup();
    var standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    var standby_open = true;
    defer if (standby_open) standby.close();
    try seedNativeIndexFixture(source, &standby);
    standby.close();
    standby_open = false;
    standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    standby_open = true;
    const native = @import("relational_index_owner_control.zig");
    var packet: native.PreparedRelease = .{ .alloc = alloc, .intent = intent };
    defer packet.deinit();
    try packet.preparePayload();
    var wrong_position = pending.position;
    wrong_position.sequence += 1;
    var frame: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 7, .previous_lsn = 6, .payload = try packet.finalizePayload(wrong_position) };
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, replication_ingress.applyRecord(&standby, frame));
    try std.testing.expectEqual(@as(u64, 6), try standby.replicationAppliedSequence());
    frame.payload = publisher.payload.?;
    try replication_ingress.applyRecord(&standby, frame);
    try replication_ingress.applyRecord(&standby, frame);
    frame.lsn = 8;
    frame.previous_lsn = 7;
    try replication_ingress.applyRecord(&standby, frame);
    try std.testing.expectEqual(@as(u64, 8), try standby.replicationAppliedSequence());
    standby.close();
    standby_open = false;
    standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    standby_open = true;
    {
        var read = try standby.core.store.beginReadTxn();
        defer read.abort();
        try release_mod.Record.verifyRecovery(alloc, &read);
        try std.testing.expect(try jobs.load(&read) == null and try topology.current(&read) == null);
        try std.testing.expectEqual(pending.position.sequence, (try authority.load(&read)).?.sequence);
    }
    try testReleasedIndexCompanionGc(&primary, try integrity.Address.init(generation, tuple.items));
}

fn testReleasedIndexGcAllocations(allocator: std.mem.Allocator, database: *db_mod.DB) !void {
    var read = try database.core.store.beginReadTxn();
    defer read.abort();
    var page = (try @import("relational_index_release.zig").Gc.Page.prepare(allocator, database.backend_runtime.io(), &read)).?;
    defer page.deinit();
    var prepared: @import("relational_index_owner_control.zig").PreparedReleaseGc = .{ .alloc = allocator, .command = page.command };
    defer prepared.deinit();
    try prepared.preparePayload();
}

/// Real released LSM/claims plus extra obsolete private source companions to
/// force multiple bounded pages. Logical KV seeding is interface qualification,
/// not restore adoption or independent metadata authorization.
fn testReleasedIndexCompanionGc(source: *db_mod.DB, live_address: @import("relational_integrity.zig").Address) !void {
    const release_mod = @import("relational_index_release.zig");
    const native = @import("relational_index_owner_control.zig");
    const authority = @import("../source_authority.zig");
    var primary_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-release-gc-primary");
    defer primary_dir.cleanup();
    var standby_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-release-gc-standby");
    defer standby_dir.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} }, .online_source_authority = .native, .identity_namespace = source.core.identity_namespace };
    var primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
    var primary_open = true;
    defer if (primary_open) primary.close();
    var standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    var standby_open = true;
    defer if (standby_open) standby.close();
    try seedNativeIndexFixture(source, &primary);
    try seedNativeIndexFixture(source, &standby);
    const terminal = terminal: {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        break :terminal (try release_mod.Record.pending(alloc, &read)).?;
    };
    const owner_id = terminal.intent.installed.command.validation.command.binding.id;
    for ([_]*db_mod.DB{ &primary, &standby }) |db| {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        for (0..300) |ordinal| try txn.put(&@import("relational_index_private_claims.zig").SourceStream.key(owner_id, @intCast(1000 + ordinal), false), "obsolete source companion");
        try txn.commit();
    }
    primary.close();
    primary_open = false;
    primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
    primary_open = true;
    standby.close();
    standby_open = false;
    standby = try db_mod.DB.open(alloc, standby_dir.path(), options);
    standby_open = true;
    const live_key = live_address.claimKey();
    const live_bytes = (try source.core.getStoreValue(alloc, &live_key)).?;
    defer alloc.free(live_bytes);
    const row_bytes = (try source.get(alloc, "after-release")).?;
    defer alloc.free(row_bytes);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, testReleasedIndexGcAllocations, .{&primary});
    {
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        var wrong_cut = terminal.position;
        wrong_cut.sequence += 1;
        try txn.put(release_mod.Record.retirement_pending_key, &try (@import("receipt_position.zig").Position{ .native = wrong_cut }).encode());
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, release_mod.Record.verifyRecovery(alloc, &txn));
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, release_mod.Gc.Page.prepare(alloc, primary.backend_runtime.io(), &txn));
    }
    const Fault = struct {
        txn: *docstore.DocStore.Txn,
        fail_at: ?usize = null,
        after: bool = false,
        writes: usize = 0,
        fn before(self: *@This()) !void {
            if (self.fail_at == self.writes + 1 and !self.after) return error.TestReleaseGcWriteFailed;
        }
        fn completed(self: *@This()) !void {
            self.writes += 1;
            if (self.fail_at == self.writes and self.after) return error.TestReleaseGcWriteFailed;
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.txn.get(key);
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.before();
            try self.txn.put(key, value);
            try self.completed();
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            try self.before();
            try self.txn.delete(key);
            try self.completed();
        }
        pub fn openCursor(self: *@This()) !docstore.DocStore.Txn.CursorAdapter {
            return self.txn.openCursor();
        }
    };
    {
        var page = page: {
            var read = try primary.core.store.beginReadTxn();
            defer read.abort();
            break :page (try release_mod.Gc.Page.prepare(alloc, primary.backend_runtime.io(), &read)).?;
        };
        defer page.deinit();
        try std.testing.expect(page.command.entries.len <= release_mod.Gc.max_records);
        const writes = count: {
            var txn = try primary.core.store.beginWriteTxn();
            defer txn.abort();
            var fault: Fault = .{ .txn = &txn };
            _ = try page.command.stage(alloc, &fault, null);
            break :count fault.writes;
        };
        for (1..writes + 1) |fail_at| for ([_]bool{ false, true }) |after| {
            var txn = try primary.core.store.beginWriteTxn();
            defer txn.abort();
            var fault: Fault = .{ .txn = &txn, .fail_at = fail_at, .after = after };
            try std.testing.expectError(error.TestReleaseGcWriteFailed, page.command.stage(alloc, &fault, null));
            try std.testing.expectEqual(if (after) fail_at else fail_at - 1, fault.writes);
        };
        var encoded_key: [std.base64.standard.Encoder.calcSize(live_key.len)]u8 = undefined;
        const entry: release_mod.Gc.Entry = .{ .key_b64 = std.base64.standard.Encoder.encode(&encoded_key, &live_key), .value_digest = @import("relational_integrity_contract.zig").hash(live_bytes), .value_bytes = @intCast(live_bytes.len) };
        var txn = try primary.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, (release_mod.Gc.Command{ .record = terminal, .entries = &.{entry} }).stage(alloc, &txn, null));
    }
    const baseline_lsn = try standby.replicationAppliedSequence();
    var pages: u64 = 0;
    var deleted: usize = 0;
    var finished = false;
    while (pages < 32) {
        var barrier: @import("antfly_runtime_abi").mutation_barrier.MutationBarrier = .{};
        var publisher: TestNativeIndexPublisher = .{ .barrier = &barrier, .lsn = baseline_lsn + pages + 1 };
        defer if (publisher.payload) |bytes| alloc.free(bytes);
        defer if (primary_open) {
            primary.local_execution.replication_async_batch_mirror = null;
        };
        primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
        const outcome = (try primary.collectReleasedNativeIndexCompanionsStep()).?;
        try std.testing.expect(!outcome.duplicate and outcome.replication_pending);
        try std.testing.expectEqualStrings("TestPublicationUnavailable", outcome.error_name.?);
        try std.testing.expect(outcome.records <= release_mod.Gc.max_records and outcome.bytes <= release_mod.Gc.max_bytes);
        deleted += outcome.records;
        primary.local_execution.replication_async_batch_mirror = null;
        primary.close();
        primary_open = false;
        primary = try db_mod.DB.open(alloc, primary_dir.path(), options);
        primary_open = true;
        publisher.fail = false;
        primary.local_execution.replication_async_batch_mirror = publisher.asMirror();
        try primary.flushDurableReplicationOutboxes();
        try std.testing.expectEqual(@as(usize, 2), publisher.attempts);
        try std.testing.expectEqual(@as(usize, 1), publisher.completed);
        primary.local_execution.replication_async_batch_mirror = null;
        const frame: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = baseline_lsn + pages + 1, .previous_lsn = baseline_lsn + pages, .payload = publisher.payload.? };
        try replication_ingress.applyRecord(&standby, frame);
        try replication_ingress.applyRecord(&standby, frame);
        for ([_]*db_mod.DB{ &primary, &standby }) |db| {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualSlices(u8, live_bytes, try read.get(&live_key));
            try std.testing.expectEqual(outcome.position.sequence, (try authority.load(&read)).?.sequence);
            try std.testing.expectEqual(!outcome.retired, try release_mod.Record.pending(alloc, &read) != null);
            const row = (try db.get(alloc, "after-release")).?;
            defer alloc.free(row);
            try std.testing.expectEqualStrings(row_bytes, row);
        }
        pages += 1;
        if (outcome.retired) {
            finished = true;
            break;
        }
    }
    try std.testing.expect(finished and pages >= 3 and deleted >= 300);
    try std.testing.expect(try primary.collectReleasedNativeIndexCompanionsStep() == null);
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try release_mod.Record.requireRetirementClear(&read);
    }
    var retry: native.PreparedRelease = try primary.prepareNativeIndexOwnerRelease(terminal.intent, .{});
    defer retry.deinit();
    const outcome = try primary.applyPreparedNativeIndexOwnerRelease(&retry, null);
    try std.testing.expect(outcome.duplicate);
    try std.testing.expectEqualDeep(try terminal.intent.receipt(terminal.position), outcome.receipt.?);
    var scheduled_dir = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-release-gc-scheduled");
    defer scheduled_dir.cleanup();
    var scheduled = try db_mod.DB.open(alloc, scheduled_dir.path(), options);
    var scheduled_open = true;
    defer if (scheduled_open) scheduled.close();
    try seedNativeIndexFixture(source, &scheduled);
    scheduled.close();
    scheduled_open = false;
    scheduled = try db_mod.DB.open(alloc, scheduled_dir.path(), options);
    scheduled_open = true;
    try std.testing.expect(try scheduled.runRelationalIndexMaintenancePass());
    {
        var read = try scheduled.core.store.beginReadTxn();
        defer read.abort();
        try release_mod.Record.requireRetirementClear(&read);
        try std.testing.expectEqualSlices(u8, live_bytes, try read.get(&live_key));
    }
}

fn testInstalledIndexHistoryAllocations(allocator: std.mem.Allocator, txn: *docstore.DocStore.Txn, binding: @import("relational_index_owner_job.zig").Binding) !void {
    var history = try @import("relational_index_validation.zig").Installed.History.init(allocator, txn, binding);
    defer history.deinit();
    var view = history.reader(txn);
    try @import("relational_index_owner_job.zig").requireReserved(&view, history.binding);
}

fn testInstalledIndexPhysicalAudit(database: *db_mod.DB) !void {
    const verification = @import("relational_index_validation.zig");
    const owner = @import("relational_index_owner_job.zig");
    const claims = @import("relational_index_private_claims.zig");
    var read = try database.core.store.beginReadTxn();
    defer read.abort();
    const binding = (try owner.load(&read)).?;
    try std.testing.expectEqual(owner.Phase.installed, binding.phase);
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, owner.requireReserved(&read, binding));
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, testInstalledIndexHistoryAllocations, .{ &read, binding });
    var history = try verification.Installed.History.init(alloc, &read, binding);
    defer history.deinit();
    var view = history.reader(&read);
    const expected = history.record.command.validation.command.expected;
    var before_schema = (try database.core.acquireSchemaVersionView(binding.physical_reserved.schema_version)).?;
    defer before_schema.release();
    var after_schema = database.core.acquireSchemaView().?;
    defer after_schema.release();
    const inputs = try @import("relational_index_inputs.zig").decode(history.candidate.inputs.bytes);
    var addition = try owner.reopenPhysical(alloc, &view, before_schema, after_schema, inputs.name);
    defer addition.deinit();
    const budget: @import("relational_index_jobs.zig").Budget = .{ .records = 4, .time_ns = std.time.ns_per_s };
    var inventory: ?claims.InventoryValidation.Audit = null;
    var turns: usize = 0;
    while (inventory == null or inventory.?.fields.phase == .scanning or inventory.?.fields.phase == .claims) {
        try std.testing.expect(turns < 128);
        turns += 1;
        inventory = try claims.InventoryValidation.auditPage(alloc, &view, history.binding, expected, database.root_incarnation, inventory, budget, .{});
    }
    try std.testing.expectEqual(claims.InventoryValidation.Phase.ready, inventory.?.fields.phase);
    var generation: ?verification.Audit = null;
    defer if (generation) |*value| value.deinit();
    turns = 0;
    while (generation == null or (generation.?.progress.summary.fields.phase != .ready and generation.?.progress.summary.fields.phase != .failed)) {
        try std.testing.expect(turns < 128);
        turns += 1;
        const next = try verification.auditReadPage(alloc, database.core, &view, history.binding, expected, &addition, database.root_incarnation, inventory.?, if (generation) |*value| value else null, budget, .{});
        if (generation) |*value| value.deinit();
        generation = next;
    }
    try std.testing.expectEqual(verification.Phase.ready, generation.?.progress.summary.fields.phase);
    {
        // An aborting physical corruption is visible through the history
        // reader: it cannot substitute canonical evidence for real row reads.
        var corrupt_txn = try database.core.store.beginWriteTxn();
        defer corrupt_txn.abort();
        const primary_key = try internal.relationalRowKeyAlloc(alloc, "row000");
        defer alloc.free(primary_key);
        try corrupt_txn.put(primary_key, "corrupt");
        var corrupt_history = try verification.Installed.History.init(alloc, &corrupt_txn, binding);
        defer corrupt_history.deinit();
        var corrupt_view = corrupt_history.reader(&corrupt_txn);
        var failed = try verification.auditReadPage(alloc, database.core, &corrupt_view, history.binding, expected, &addition, database.root_incarnation, inventory.?, null, budget, .{});
        defer failed.deinit();
        try std.testing.expectEqual(verification.Phase.failed, failed.progress.summary.fields.phase);
        try std.testing.expectEqual(verification.Failure.invalid_row, failed.progress.summary.fields.failure);
    }
    try std.testing.expectEqual(owner.Phase.installed, (try owner.load(&read)).?.phase);
    try std.testing.expectEqual(history.record.position.sequence, (try @import("../source_authority.zig").load(&read)).?.sequence);
}

fn testNativeIndexActivationAllocations(allocator: std.mem.Allocator, txn: *docstore.DocStore.Txn, binding: @import("relational_index_owner_job.zig").Binding, candidate: *const @import("relational_index_candidate.zig").Candidate) !void {
    const activation = @import("relational_integrity_activation.zig");
    var publication = try @import("relational_integrity_catalog.zig").prepareIndexPublication(allocator, candidate.reservation.expected.?, candidate.reservation.value, candidate.next.value, "email_idx");
    defer publication.deinit();
    var prepared = try activation.PreparedIndexPublication.init(allocator, txn, binding, candidate.reservation.expected.?, &publication, "email_idx");
    defer prepared.deinit();
    try prepared.validateBefore(txn);
    const progress = try activation.Progress.decode(prepared.value);
    try std.testing.expectEqual(activation.State.enforced, progress.state);
    try std.testing.expectEqual(@as(u32, 2), progress.schema_version);
    try std.testing.expect(progress.matches(publication.catalog, try activation.ownership(txn)));
    // Even a real seal cannot stage activation against the reserved catalog.
    try std.testing.expectError(error.IntegrityCatalogChanged, prepared.stage(txn));
}

test "relational index build pages identity dispatch caches immutable candidates and keeps completion receipt free" {
    @setEvalBranchQuota(4000);
    const native = @import("relational_index_owner_control.zig");
    const catalog = @import("relational_integrity_catalog.zig");
    const candidates = @import("relational_index_candidate.zig");
    const authority = @import("../source_authority.zig");
    const resources = @import("../resource_manager.zig");
    const before = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"additionalProperties\":false}}}}";
    const after = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"additionalProperties\":false}}},\"relational_indexes\":[{\"name\":\"email_idx\",\"keys\":[{\"column\":\"email\",\"direction\":\"desc\"}],\"include_columns\":[\"id\"]}],\"unique_constraints\":[{\"name\":\"email_idx\",\"origin\":\"index\",\"columns\":[\"email\"]}]}";
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-build-dispatch");
    defer directory.cleanup();
    var resource_options: resources.Options = .{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 * 1024 } };
    resource_options.budgets[@backingInt(resources.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = 16 * 1024 * 1024 };
    var manager = resources.ResourceManager.init(resource_options);
    defer manager.deinit(alloc);
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} }, .online_source_authority = .native, .identity_namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .resource_manager = &manager };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try db.setSchemaJson(alloc, before);
    var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, before);
    defer parsed.deinit(alloc);
    var prior = try @import("../../schema/relational_declarations.zig").compileCatalog(alloc, parsed, 101, null);
    defer prior.deinit();
    try db.core.store.putBatch(&.{.{ .key = catalog.key, .value = prior.value }}, &.{});
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const writes = try arena.allocator().alloc(db_mod.types.BatchWrite, 32);
    for (writes, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "row{d:0>3}", .{i}), .value = if (i == 1) "{\"id\":2}" else try std.fmt.allocPrint(arena.allocator(), "{{\"id\":{d},\"email\":\"{d}@example.com\"}}", .{ i + 1, i }) };
    try db.batch(.{ .writes = writes });
    var candidate = try candidates.Candidate.prepare(alloc, 101, before, after, prior.value, "email_idx");
    defer candidate.deinit();
    var admission = try db.prepareNativeIndexOwnerAdmission(.{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = options.identity_namespace.?, .catalog_digest = @import("relational_index_inputs.zig").digest(prior.value) },
        .candidate_digest = @import("relational_index_inputs.zig").digest(candidate.next.value),
        .reservation_digest = @import("relational_index_inputs.zig").digest(candidate.reservation.value),
    }, candidate.inputs.bytes);
    defer admission.deinit();
    _ = try db.applyPreparedNativeIndexOwnerControl(&admission, null);
    for ([_]@import("relational_index_owner_job.zig").NativeEffect{ .fenced, .reserved }) |effect| {
        var command = try db.prepareNativeIndexOwnerControl(.{ .binding = admission.command.binding, .effect = effect });
        defer command.deinit();
        _ = try db.applyPreparedNativeIndexOwnerControl(&command, null);
    }
    const identity: native.Identity = .{ .id = admission.command.binding.id, .plan_digest = admission.command.binding.plan_digest, .owner_digest = admission.command.binding.owner_digest, .ordinal = 0, .namespace = options.identity_namespace.?, .owner_group_id = 11 };
    const binding = (try db.nativeIndexOwnerState(identity)).?;
    const request = try native.BuildRequest.init(identity, binding, .{ .records = 4, .time_ns = std.time.ns_per_s });
    var wrong = request;
    wrong.binding_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, db.stepNativeIndexOwnerBuild("items", wrong, .{}));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, db.stepNativeIndexOwnerBuild("items", request, .{ .cancellation = .fromAtomic(&canceled) }));
    try std.testing.expectError(error.DeadlineExceeded, db.stepNativeIndexOwnerBuild("items", request, .{ .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(u64, 0), db.native_index_build_cache.compilations);
    const slice = @backingInt(resources.Slice.relational_preparation_working_set);
    {
        var denied = try manager.reserve(.relational_preparation_working_set, 16 * 1024 * 1024);
        defer denied.release();
        try std.testing.expectError(error.ResourceBudgetExceeded, db.stepNativeIndexOwnerBuild("items", request, .{}));
        try std.testing.expect(!db.native_index_build_cache.compiling);
    }
    for ([_]usize{ 0, 1, 2, 4, 8, 16 }) |failure_index| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = failure_index });
        try std.testing.expectError(error.OutOfMemory, db.native_index_build_cache.acquire(std.testing.io, failing.allocator(), &manager, db.core, request, binding, .{}));
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().slices[slice].used_bytes);
        try std.testing.expect(!db.native_index_build_cache.compiling);
    }
    const DuringPreparation = struct {
        calls: usize = 0,
        fn check(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.calls += 1;
            return self.calls >= 8;
        }
    };
    var during: DuringPreparation = .{};
    try std.testing.expectError(error.Canceled, db.stepNativeIndexOwnerBuild("items", request, .{ .cancellation = .{ .ptr = &during, .is_cancelled_fn = DuringPreparation.check } }));
    try std.testing.expectEqual(@as(usize, 8), during.calls);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get(&@import("relational_index_jobs.zig").progressKey(binding.index)));
        try std.testing.expectEqual(@as(u64, 3), (try authority.load(&read)).?.sequence);
    }
    var pages: usize = 0;
    var last_position: ?@import("receipt_position.zig").Native = null;
    while (true) {
        const result = try db.stepNativeIndexOwnerBuild("items", request, .{});
        try result.validateFor(request);
        switch (result) {
            .inventory, .generation, .audit, .validated => return error.UnexpectedInventoryValidation,
            .advanced => |value| {
                pages += 1;
                try std.testing.expect(pages <= 9);
                try std.testing.expect(!value.duplicate and !value.replication_pending);
                last_position = value.position;
            },
            .complete => |value| {
                try std.testing.expectEqual(.ready, value.state);
                try std.testing.expectEqual(@as(u64, 32), value.rows_scanned);
                try std.testing.expectEqual(last_position.?, value.position);
                try std.testing.expectEqual(@as(u64, 9), value.claim_batches_pending);
                try std.testing.expect(value.claim_bytes_pending > 0);
                var invalid = value;
                invalid.claim_bytes_pending = 0;
                try std.testing.expectError(error.InvalidRelationalIndexProgress, invalid.validateFor(request));
                try std.testing.expect(!value.replication_pending);
                break;
            },
        }
    }
    try std.testing.expectEqual(@as(usize, 9), pages);
    try std.testing.expectEqual(@as(u64, 1), db.native_index_build_cache.compilations);
    try std.testing.expectEqual(@as(u64, 10), db.native_index_build_cache.hits);
    try std.testing.expect(manager.snapshot().slices[slice].used_bytes > 0);
    var delivered_position: ?@import("receipt_position.zig").Native = null;
    {
        var source_read = try db.readNativeIndexClaimSource(alloc, identity, .{});
        defer source_read.deinit();
        try source_read.requireIdentity(identity);
        try std.testing.expectEqual(@as(usize, 4), source_read.batch.?.rows().len);
        _ = try source_read.digest();
        try std.testing.expectEqual(@as(u64, 1), db.native_index_build_cache.compilations);
        try std.testing.expectEqual(@as(u64, 10), db.native_index_build_cache.hits);
    }
    // Eviction cannot destroy a candidate still pinned by a page preparer.
    {
        var lease = try db.native_index_build_cache.acquire(std.testing.io, alloc, &manager, db.core, request, binding, .{});
        defer lease.deinit();
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var batch = (try db_mod.DB.PreparedNativeIndexOwnerBuild.peekClaimSource(alloc, &read, binding)).?;
            defer batch.deinit();
            try std.testing.expectEqual(@as(usize, 4), batch.rows().len);
            try std.testing.expectEqualStrings("items", batch.scope.table_name);
            try std.testing.expectEqualStrings("row000", batch.rows()[0].key);
            var view = (try db.core.acquireSchemaVersionView(1)).?;
            defer view.release();
            const key = try internal.relationalRowKeyAlloc(alloc, batch.rows()[0].key);
            defer alloc.free(key);
            const row = try @import("algebraic/relational_row_codec.zig").ordinalRowView(try read.get(key), view.tableSchema().*, view.physicalLayout());
            const unique = lease.reservation().artifacts.?.unique;
            const expected = (try @import("../../api/relational_integrity.zig").encodeUnique(alloc, unique.tuple, row, unique.nulls_not_distinct, batch.rows()[0].key)).?;
            defer alloc.free(expected);
            try std.testing.expectEqualSlices(u8, expected, batch.rows()[0].tuple);
            var physical = try lease.reservation().artifacts.?.addition.plan.boundIndexes()[0].tuple.projectSource(alloc, row.table_schema, row.layout);
            defer physical.deinit();
            var encoded: std.ArrayList(u8) = .empty;
            defer encoded.deinit(alloc);
            _ = try physical.append(alloc, &encoded, row);
            try std.testing.expect(!std.mem.eql(u8, encoded.items, expected));
            const nullable_key = try internal.relationalRowKeyAlloc(alloc, batch.rows()[1].key);
            defer alloc.free(nullable_key);
            const nullable = try @import("algebraic/relational_row_codec.zig").ordinalRowView(try read.get(nullable_key), view.tableSchema().*, view.physicalLayout());
            const equality = @import("../../api/relational_integrity.zig");
            const distinct = (try equality.encodeUnique(alloc, unique.tuple, nullable, false, batch.rows()[1].key)).?;
            defer alloc.free(distinct);
            const another = (try equality.encodeUnique(alloc, unique.tuple, nullable, false, "another-row")).?;
            defer alloc.free(another);
            const not_distinct = (try equality.encodeUnique(alloc, unique.tuple, nullable, true, batch.rows()[1].key)).?;
            defer alloc.free(not_distinct);
            try std.testing.expectEqualSlices(u8, distinct, batch.rows()[1].tuple);
            try std.testing.expect(std.mem.endsWith(u8, distinct, batch.rows()[1].key));
            try std.testing.expect(!std.mem.eql(u8, distinct, another));
            try std.testing.expect(distinct.len > not_distinct.len);
            const delivery = @import("relational_index_claim_delivery.zig");
            var cohort = try delivery.Cohort.init(alloc, &.{.{ .owner = .{
                .plan_id = binding.id,
                .plan_digest = binding.plan_digest,
                .ordinal = binding.ordinal,
                .group_id = binding.fence.owner_group_id,
                .namespace = binding.fence.namespace,
                .authority = binding.authority,
                .fence_digest = binding.owner_digest,
            } }});
            defer cohort.deinit();
            const plan = try delivery.Plan.prepare(&cohort, binding, .fromBatch(&batch));
            try std.testing.expectEqual(@as(usize, 1), plan.group_count);
            try std.testing.expectEqual(plan.digest, try plan.coverage(&.{0}));
            try plan.requireTarget(&cohort, 0, binding);
            const command: native.ClaimCommand = .{ .binding = binding, .claims = try plan.groupClaims(0) };
            var prepared_claims = try db.prepareNativeIndexOwnerClaims(command);
            defer prepared_claims.deinit();
            const delivered = try db.applyPreparedNativeIndexOwnerClaims(&prepared_claims, null);
            try delivered.validateFor(command);
            try std.testing.expectEqual(@as(usize, 4), delivered.claims_added);
            delivered_position = delivered.position;
            const duplicate = try db.applyPreparedNativeIndexOwnerClaims(&prepared_claims, null);
            try std.testing.expect(duplicate.duplicate);
            try std.testing.expectEqual(delivered.position, duplicate.position);
            // Physical delivery has not authenticated a complete source
            // closure and therefore cannot retire its queued batch.
            var current = try db.core.store.beginReadTxn();
            defer current.abort();
            try std.testing.expectEqual(@as(u64, 9), (try @import("relational_index_private_claims.zig").SourceStream.load(&current, binding)).?.pending());
        }
        db.native_index_build_cache.evict(std.testing.io);
        try std.testing.expect(manager.snapshot().slices[slice].used_bytes > 0);
        try std.testing.expect((try native.PreparedBuild.prepare(alloc, std.testing.io, db.core, "items", lease.reservation(), request.budget)) == null);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().slices[slice].used_bytes);
    {
        var runtime = std.Io.Threaded.init(alloc, .{});
        defer runtime.deinit();
        const async_io = runtime.io();
        const cache_io = db.backend_runtime.io().?;
        const Context = @import("../../api/operation.zig").RequestContext;
        const Worker = struct {
            io: std.Io,
            owner: *db_mod.DB,
            request: native.BuildRequest,
            start: *std.Io.Event,
            done: std.Io.Event = .unset,
            context: Context = .{},
            err: ?anyerror = null,
            response: ?native.BuildResponse = null,
            fn run(self: *@This()) std.Io.Cancelable!void {
                defer self.done.set(self.io);
                try self.start.wait(self.io);
                self.response = self.owner.stepNativeIndexOwnerBuild("items", self.request, self.context) catch |err| {
                    self.err = err;
                    return;
                };
            }
        };
        const Control = struct {
            io: std.Io,
            reached: std.Io.Event = .unset,
            canceled: std.atomic.Value(bool) = .init(false),
            calls: usize = 0,
            fn check(ptr: *const anyopaque) bool {
                const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
                self.calls += 1;
                // The fourth checkpoint follows enqueue in the miss waiter.
                if (self.calls >= 4) self.reached.set(self.io);
                return self.canceled.load(.acquire);
            }
        };
        const timeout: std.Io.Timeout = .{ .duration = .{ .raw = std.Io.Duration.fromSeconds(5), .clock = .awake } };
        // Hold the compile-admission state, not apply exclusion, to exercise
        // semantic cancellation/deadline removal of an independently queued RPC.
        for (0..2) |mode| {
            var control: Control = .{ .io = async_io };
            var start: std.Io.Event = .unset;
            db.native_index_build_cache.mutex.lockUncancelable(cache_io);
            db.native_index_build_cache.compiling = true;
            db.native_index_build_cache.mutex.unlock(cache_io);
            var worker: Worker = .{ .io = async_io, .owner = &db, .request = request, .start = &start, .context = .{
                .cancellation = .{ .ptr = &control, .is_cancelled_fn = Control.check },
                .deadline_ns = if (mode == 1) time.monotonicNs() + 100 * std.time.ns_per_ms else null,
            } };
            var group: std.Io.Group = .init;
            defer {
                control.canceled.store(true, .release);
                group.cancel(async_io);
                db.native_index_build_cache.mutex.lockUncancelable(cache_io);
                db.native_index_build_cache.compiling = false;
                db.native_index_build_cache.mutex.unlock(cache_io);
            }
            try group.concurrent(async_io, Worker.run, .{&worker});
            start.set(async_io);
            try control.reached.waitTimeout(async_io, timeout);
            if (mode == 0) control.canceled.store(true, .release);
            try worker.done.waitTimeout(async_io, timeout);
            try group.await(async_io);
            try std.testing.expectEqual(@as(?anyerror, if (mode == 0) error.Canceled else error.DeadlineExceeded), worker.err);
            try std.testing.expect(worker.response == null);
            try std.testing.expect(db.native_index_build_cache.waiters.head == null);
            try std.testing.expectEqual(@as(u64, 1), db.native_index_build_cache.compilations);
        }
        var start: std.Io.Event = .unset;
        var workers: [8]Worker = undefined;
        var group: std.Io.Group = .init;
        defer group.cancel(async_io);
        for (&workers) |*worker| {
            worker.* = .{ .io = async_io, .owner = &db, .request = request, .start = &start };
            try group.concurrent(async_io, Worker.run, .{worker});
        }
        start.set(async_io);
        try group.await(async_io);
        for (workers) |worker| {
            if (worker.err) |err| return err;
            try std.testing.expectEqual(.ready, worker.response.?.complete.state);
            try std.testing.expectEqual(last_position.?, worker.response.?.complete.position);
        }
        try std.testing.expectEqual(@as(u64, 2), db.native_index_build_cache.compilations);
    }
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    const recovered = try db.stepNativeIndexOwnerBuild("items", request, .{});
    try recovered.validateFor(request);
    try std.testing.expectEqual(last_position.?, recovered.complete.position);
    try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    try std.testing.expectEqual(@as(u64, 1), db.native_index_build_cache.compilations);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(delivered_position.?.sequence, (try authority.load(&read)).?.sequence);
    }
    // Consume the actual nine queued pages, including empty EOF, without
    // recompiling the candidate or changing the physical build checkpoint.
    {
        const Source = @import("relational_index_private_claims.zig").SourceStream;
        var previous: ?Source.Accepted = null;
        var expected_xor: [32]u8 = @splat(0);
        var duplicate_claim: ?db_mod.DB.PreparedNativeIndexOwnerClaims = null;
        defer if (duplicate_claim) |*prepared| prepared.deinit();
        for (0..9) |i| {
            var source = try db.readNativeIndexClaimSource(alloc, identity, .{});
            defer source.deinit();
            const header = source.batch.?.header;
            var claims: [@import("relational_index_private_claims.zig").max_claims]@import("relational_integrity_contract.zig").Claim = undefined;
            for (source.batch.?.rows(), 0..) |row, index| claims[index] = .{ .parent_table = source.batch.?.scope.table_name, .parent_key = row.key, .tuple = row.tuple, .schema_version = binding.physical_candidate.schema_version };
            var logical_digest: [32]u8 = @splat(7);
            if (source.batch.?.count != 0) {
                const delivered_command: native.ClaimCommand = .{ .binding = binding, .claims = claims[0..source.batch.?.count] };
                var delivered = try db.prepareNativeIndexOwnerClaims(delivered_command);
                defer delivered.deinit();
                const result = try db.applyPreparedNativeIndexOwnerClaims(&delivered, null);
                try result.validateFor(delivered_command);
                logical_digest = try delivered_command.digest();
                for (delivered_command.claims) |claim| for (&expected_xor, claim.memberDigest()) |*byte, contribution| {
                    byte.* ^= contribution;
                };
                if (i == 0) duplicate_claim = try db.prepareNativeIndexOwnerClaims(.{ .binding = binding, .claims = claims[0..1] });
            }
            const accepted = try sourceDecision(binding, header, previous, logical_digest);
            if (i == 0) {
                const command: native.ConsumeCommand = .{ .binding = binding, .accepted = accepted };
                var consume_canceled = std.atomic.Value(bool).init(true);
                try std.testing.expectError(error.Canceled, db.prepareNativeIndexSourceConsumptionControlled(alloc, command, .{ .cancellation = .fromAtomic(&consume_canceled) }));
                try std.testing.expectError(error.DeadlineExceeded, db.prepareNativeIndexSourceConsumptionControlled(alloc, command, .{ .deadline_ns = 0 }));
                consume_canceled.store(false, .release);
                var controlled = try db.prepareNativeIndexSourceConsumptionControlled(alloc, command, .{ .cancellation = .fromAtomic(&consume_canceled) });
                defer controlled.deinit();
                consume_canceled.store(true, .release);
                try std.testing.expectError(error.Canceled, db.applyPreparedNativeIndexSourceConsumption(&controlled, null));
                var aborted = try db.core.store.beginWriteTxn();
                defer aborted.abort();
                try std.testing.expectError(error.Canceled, controlled.stage(db.core, &aborted, null));
            }
            var prepared = try db.prepareNativeIndexSourceConsumption(.{ .binding = binding, .accepted = accepted });
            defer prepared.deinit();
            {
                var aborted = try db.core.store.beginWriteTxn();
                defer aborted.abort();
                _ = try prepared.prepared.stage(&aborted, null);
            }
            const outcome = try db.applyPreparedNativeIndexSourceConsumption(&prepared, null);
            try outcome.validateFor(prepared.command);
            try std.testing.expect(!outcome.duplicate and !outcome.replication_pending);
            const duplicate = try db.applyPreparedNativeIndexSourceConsumption(&prepared, null);
            try duplicate.validateFor(prepared.command);
            try std.testing.expect(duplicate.duplicate);
            try std.testing.expectEqual(outcome.position, duplicate.position);
            var check = try db.core.store.beginReadTxn();
            defer check.abort();
            try db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&check, binding);
            const head = (try Source.load(&check, binding)).?;
            try std.testing.expectEqual(@as(u64, i + 1), head.consumed);
            try std.testing.expectEqual(@as(u64, 8 - i), head.pending());
            try std.testing.expectEqual(accepted.rows, head.consumed_rows);
            try std.testing.expectEqual((try header.position()).sequence, head.consumed_sequence);
            try std.testing.expect(outcome.position.sequence > head.consumed_sequence);
            try std.testing.expectError(error.NotFound, check.get(&Source.key(binding.id, accepted.ordinal, true)));
            try std.testing.expectEqual(outcome.position.sequence, (try authority.load(&check)).?.sequence);
            previous = accepted;
        }
        try std.testing.expectEqual(@as(u64, 32), previous.?.rows);
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        var source = try db.readNativeIndexClaimSource(alloc, identity, .{});
        defer source.deinit();
        try source.requireIdentity(identity);
        try std.testing.expect(source.batch == null and source.consumed != null);
        try std.testing.expectEqual(Source.State.ready, source.consumed.?.accepted.state);
        try std.testing.expectEqual(previous.?.decision_digest, source.consumed.?.accepted.decision_digest);
        {
            var missing = try db.core.store.beginWriteTxn();
            defer missing.abort();
            try missing.delete(Source.consumption_key);
            try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&missing, binding));
        }
        const complete = try db.stepNativeIndexOwnerBuild("items", request, .{});
        try std.testing.expectEqual(@as(u64, 0), complete.complete.claim_batches_pending);
        try std.testing.expectEqual(last_position.?, complete.complete.position);
        try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);

        // Expected logical membership comes from the delivered source, not
        // from the target scan being verified. Metadata proof signing itself
        // is covered by the publication tests, not fabricated by this fixture.
        const private = @import("relational_index_private_claims.zig");
        const Validation = private.InventoryValidation;
        const consumed = source.consumed.?;
        var validation_request = request;
        validation_request.budget.records = 3;
        validation_request.budget.time_ns = std.time.ns_per_s;
        validation_request.target_expectation = .{
            .metadata_incarnation = consumed.accepted.metadata_incarnation,
            .binding_digest = consumed.accepted.binding_digest,
            .generation = consumed.header.bytes[72..88].*,
            .completion_revision = 123,
            .source_consumption_digest = try consumed.prefix().digest(),
            .source_progress_digest = consumed.header.progressDigest(),
            .table_name_digest = @import("relational_integrity_contract.zig").hash("items"),
            .rows = 32,
            .inventory_xor = expected_xor,
            .provenance = @splat(7),
        };
        const late_command: native.ClaimCommand = .{ .binding = binding, .claims = &.{.{ .parent_table = "items", .parent_key = "late", .tuple = "late", .schema_version = binding.physical_candidate.schema_version }} };
        var late = try db.prepareNativeIndexOwnerClaims(late_command);
        defer late.deinit();
        const frozen = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
        try frozen.validateFor(validation_request);
        try std.testing.expectEqual(Validation.Phase.scanning, frozen.inventory.record.fields.phase);
        try std.testing.expectEqual(@as(u64, 0), frozen.inventory.record.fields.rows);
        try std.testing.expectError(error.PrivateIndexTargetSealed, db.applyPreparedNativeIndexOwnerClaims(&late, null));
        const duplicate = try db.applyPreparedNativeIndexOwnerClaims(&duplicate_claim.?, null);
        try std.testing.expect(duplicate.duplicate);
        try std.testing.expectError(error.Canceled, db.stepNativeIndexOwnerBuild("items", validation_request, .{ .cancellation = .fromAtomic(&canceled) }));
        try std.testing.expectError(error.DeadlineExceeded, db.stepNativeIndexOwnerBuild("items", validation_request, .{ .deadline_ns = 0 }));
        var too_small = validation_request;
        too_small.budget.bytes = 1;
        try std.testing.expectError(error.InvalidRelationalIndexBudget, db.stepNativeIndexOwnerBuild("items", too_small, .{}));
        var changed_expectation = validation_request;
        changed_expectation.target_expectation.?.rows += 1;
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, db.stepNativeIndexOwnerBuild("items", changed_expectation, .{}));
        {
            var denied = try manager.reserve(.relational_preparation_working_set, 16 * 1024 * 1024);
            defer denied.release();
            try std.testing.expectError(error.ResourceBudgetExceeded, db.stepNativeIndexOwnerBuild("items", validation_request, .{}));
        }
        {
            var corrupt = try db.core.store.beginWriteTxn();
            defer corrupt.abort();
            const address = try @import("relational_integrity_contract.zig").Address.init(validation_request.target_expectation.?.generation, duplicate_claim.?.command.claims[0].tuple);
            try corrupt.delete(&address.claimKey());
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, Validation.Prepared.prepare(alloc, &corrupt, binding, validation_request.target_expectation.?, .{ .records = 128, .time_ns = std.time.ns_per_s }, .{}));
        }
        // Exercise the actual normalized ingress and native mutation pipeline,
        // not only the page recomputation helper. The transport marker and
        // inventory cursor must commit together and exact replay is inert.
        var prior_rows: u64 = 0;
        {
            const page = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :blk (try Validation.Prepared.prepare(alloc, &read, binding, validation_request.target_expectation.?, validation_request.budget, .{})).?;
            };
            var effect: native.PreparedInventoryValidation = .{ .alloc = alloc, .batch = page };
            defer effect.deinit();
            try effect.preparePayload();
            const position: @import("receipt_position.zig").Native = .{ .namespace = binding.fence.namespace, .sequence = frozen.inventory.position.sequence + 1 };
            const payload = try effect.finalizePayload(position);
            const record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = payload };
            try replication_ingress.applyRecord(&db, record);
            try replication_ingress.applyRecord(&db, record);
            try std.testing.expectEqual(@as(u64, 1), try db.replicationAppliedSequence());
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const retained = (try Validation.load(alloc, &read, binding)).?;
            try std.testing.expectEqual(position, retained.position);
            try std.testing.expectEqualDeep(page.command.next, retained.fields);
            prior_rows = retained.fields.rows;
        }
        var validation_pages: usize = 1;
        var prior_claim_rows: u64 = 0;
        var restarted_claims = false;
        var orphan_exercised = false;
        const Integrity = @import("relational_integrity_contract.zig");
        var foreign_generation = validation_request.target_expectation.?.generation;
        foreign_generation[0] ^= 1;
        if (std.mem.allEqual(u8, &foreign_generation, 0)) foreign_generation[1] = 1;
        const foreign_address = try Integrity.Address.init(foreign_generation, "foreign-audit");
        const foreign_key = foreign_address.claimKey();
        const foreign_value = try (Integrity.Claim{ .tuple = "foreign-audit", .parent_table = "items", .parent_key = "row000", .schema_version = 1 }).encode(alloc, foreign_address);
        defer alloc.free(foreign_value);
        try db.core.store.putBatch(&.{.{ .key = &foreign_key, .value = foreign_value }}, &.{});
        var foreign_deleted = false;
        while (true) {
            var applied_override: ?native.BuildResponse = null;
            // Normalized replay must recompute the physical page rather than
            // accepting a supplied cursor or chain, even with a valid digest.
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const page = (try Validation.Prepared.prepare(alloc, &read, binding, validation_request.target_expectation.?, validation_request.budget, .{})).?;
                _ = try Validation.Prepared.replay(alloc, &read, page.command);
                var forged = page.command;
                forged.next.chain[0] ^= 1;
                try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, Validation.Prepared.replay(alloc, &read, forged));
                if (page.command.from == .claims and !foreign_deleted) {
                    const before_audit = (try Validation.load(alloc, &read, binding)).?;
                    const lower = before_audit.fields.claim_cursor;
                    const end = page.command.next.claim_cursor;
                    if ((lower == null or std.mem.order(u8, &foreign_key, &lower.?) == .gt) and end != null and std.mem.order(u8, &foreign_key, &end.?) != .gt) {
                        // Unrelated retired claims may disappear between
                        // preparation and ordered replay. Verify the bounded
                        // interval, not an exact foreign-record count.
                        try db.core.store.putBatch(&.{}, &.{&foreign_key});
                        foreign_deleted = true;
                        var fresh = try db.core.store.beginReadTxn();
                        const checked = Validation.Prepared.replay(alloc, &fresh, page.command) catch |err| {
                            fresh.abort();
                            return err;
                        };
                        const sequence = (try authority.load(&fresh)).?.sequence + 1;
                        fresh.abort();
                        var replay_effect: native.PreparedInventoryValidation = .{ .alloc = alloc, .batch = checked };
                        defer replay_effect.deinit();
                        try replay_effect.preparePayload();
                        const position: @import("receipt_position.zig").Native = .{ .namespace = binding.fence.namespace, .sequence = sequence };
                        const payload = try replay_effect.finalizePayload(position);
                        const packet: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 2, .previous_lsn = 1, .payload = payload };
                        try replication_ingress.applyRecord(&db, packet);
                        try replication_ingress.applyRecord(&db, packet);
                        var observe_replay = try db.core.store.beginReadTxn();
                        defer observe_replay.abort();
                        const retained = (try Validation.load(alloc, &observe_replay, binding)).?;
                        applied_override = .{ .inventory = .{ .binding = binding, .record = retained, .position = retained.position, .duplicate = false } };
                    }
                }
            }
            const result = applied_override orelse try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
            try result.validateFor(validation_request);
            const fields = result.inventory.record.fields;
            validation_pages += 1;
            try std.testing.expect(validation_pages <= 48);
            try std.testing.expect(fields.rows >= prior_rows and fields.rows - prior_rows <= 3);
            try std.testing.expect(fields.claim_rows >= prior_claim_rows and fields.claim_rows - prior_claim_rows <= 3);
            prior_rows = fields.rows;
            prior_claim_rows = fields.claim_rows;
            if (fields.phase == .claims and fields.claim_rows == 0 and !orphan_exercised) {
                orphan_exercised = true;
                const orphan_address = try Integrity.Address.init(validation_request.target_expectation.?.generation, late.command.claims[0].tuple);
                const orphan_value = try late.command.claims[0].encode(alloc, orphan_address);
                defer alloc.free(orphan_value);
                const saved = try result.inventory.record.encode(alloc);
                defer alloc.free(saved);
                // Privileged corruption injection: the forward inventory
                // still matches exactly, but an authoritative claim has no
                // companion and must prevent readiness.
                try db.core.store.putBatch(&.{.{ .key = &orphan_address.claimKey(), .value = orphan_value }}, &.{});
                var turns: usize = 0;
                while (true) {
                    const detected = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
                    try detected.validateFor(validation_request);
                    turns += 1;
                    try std.testing.expect(turns <= 48);
                    if (detected.inventory.record.fields.phase == .failed) {
                        try std.testing.expectEqual(Validation.Failure.claim_companion, detected.inventory.record.fields.failure);
                        try std.testing.expectEqual(@as(u64, 32), detected.inventory.record.fields.rows);
                        try std.testing.expectEqualSlices(u8, &expected_xor, &detected.inventory.record.fields.inventory_xor);
                        db.close();
                        db = try db_mod.DB.open(alloc, directory.path(), options);
                        const retained = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
                        try std.testing.expect(retained.inventory.duplicate);
                        try std.testing.expectEqualDeep(detected.inventory.record, retained.inventory.record);
                        break;
                    }
                    try std.testing.expect(detected.inventory.record.fields.phase != .ready);
                }
                try db.core.store.putBatch(&.{.{ .key = &Validation.key(binding), .value = saved }}, &.{&orphan_address.claimKey()});
            }
            const restart_claims = fields.phase == .claims and fields.claim_rows != 0 and !restarted_claims;
            if (validation_pages == 2 or restart_claims) {
                if (restart_claims) restarted_claims = true;
                db.close();
                db = try db_mod.DB.open(alloc, directory.path(), options);
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const retained = (try Validation.load(alloc, &read, binding)).?;
                try std.testing.expectEqualDeep(result.inventory.record, retained);
            }
            if (fields.phase == .ready) {
                try std.testing.expectEqual(@as(u64, 32), fields.rows);
                try std.testing.expectEqualSlices(u8, &expected_xor, &fields.inventory_xor);
                try std.testing.expectEqual(@as(u64, 32), fields.claim_rows);
                try std.testing.expectEqualSlices(u8, &expected_xor, &fields.claim_xor);
                break;
            }
            try std.testing.expect(fields.phase == .scanning or fields.phase == .claims);
        }
        try std.testing.expect(restarted_claims);
        try std.testing.expect(orphan_exercised and foreign_deleted);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const canonical = try alloc.dupe(u8, try read.get(&Validation.key(binding)));
            defer alloc.free(canonical);
            const before_sequence = (try authority.load(&read)).?.sequence;
            var audit: ?Validation.Audit = null;
            try std.testing.expectError(error.PrivateIndexAuditScopeChanged, Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, 0, null, validation_request.budget, .{}));
            var audit_canceled = std.atomic.Value(bool).init(true);
            const canceled_context: @import("../../api/operation.zig").RequestContext = .{ .cancellation = @import("../../api/operation.zig").CancellationToken.fromAtomic(&audit_canceled) };
            try std.testing.expectError(error.Canceled, Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, null, validation_request.budget, canceled_context));
            var turns: usize = 0;
            while (true) {
                audit = try Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, audit, validation_request.budget, .{});
                turns += 1;
                try std.testing.expect(turns <= 48);
                if (turns == 2) {
                    try std.testing.expectError(error.PrivateIndexAuditScopeChanged, Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation ^ 1, audit, validation_request.budget, .{}));
                }
                if (audit.?.fields.phase == .ready) break;
                try std.testing.expect(audit.?.fields.phase != .failed);
            }
            try std.testing.expectEqual(@as(u64, 32), audit.?.fields.claim_rows);
            const terminal = try Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, audit, validation_request.budget, .{});
            try std.testing.expectEqualDeep(audit.?, terminal);
            try std.testing.expectError(error.Canceled, Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, audit, validation_request.budget, canceled_context));
            try std.testing.expectEqualSlices(u8, canonical, try read.get(&Validation.key(binding)));
            try std.testing.expectEqual(before_sequence, (try authority.load(&read)).?.sequence);
        }
        {
            // A restored canonical "ready" cursor cannot substitute for a
            // new root's physical audit. Leave the canonical proof untouched,
            // inject an orphan, and recompute from the beginning read-only.
            const orphan_address = try Integrity.Address.init(validation_request.target_expectation.?.generation, late.command.claims[0].tuple);
            const orphan_value = try late.command.claims[0].encode(alloc, orphan_address);
            defer alloc.free(orphan_value);
            try db.core.store.putBatch(&.{.{ .key = &orphan_address.claimKey(), .value = orphan_value }}, &.{});
            var read = try db.core.store.beginReadTxn();
            var read_open = true;
            defer if (read_open) read.abort();
            const canonical = try alloc.dupe(u8, try read.get(&Validation.key(binding)));
            defer alloc.free(canonical);
            const before_sequence = (try authority.load(&read)).?.sequence;
            var audit: ?Validation.Audit = null;
            var turns: usize = 0;
            while (true) {
                audit = try Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, audit, validation_request.budget, .{});
                turns += 1;
                try std.testing.expect(turns <= 48);
                if (audit.?.fields.phase == .failed) break;
                try std.testing.expect(audit.?.fields.phase != .ready);
            }
            try std.testing.expectEqual(Validation.Failure.claim_companion, audit.?.fields.failure);
            try std.testing.expectEqual(Validation.Phase.ready, (try Validation.load(alloc, &read, binding)).?.fields.phase);
            try std.testing.expectEqualSlices(u8, canonical, try read.get(&Validation.key(binding)));
            try std.testing.expectEqual(before_sequence, (try authority.load(&read)).?.sequence);
            read.abort();
            read_open = false;
            try db.core.store.putBatch(&.{}, &.{&orphan_address.claimKey()});
        }
        {
            // Local progress cannot carry a scanned prefix across a change to
            // the canonical seal, even on the same physical root.
            const saved = saved: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :saved try alloc.dupe(u8, try read.get(&Validation.key(binding)));
            };
            defer alloc.free(saved);
            const first = first: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :first try Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, null, validation_request.budget, .{});
            };
            var changed = try Validation.Record.decode(alloc, saved);
            changed.page_digest[0] ^= 1;
            const altered = try changed.encode(alloc);
            defer alloc.free(altered);
            try db.core.store.putBatch(&.{.{ .key = &Validation.key(binding), .value = altered }}, &.{});
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expectError(error.PrivateIndexAuditScopeChanged, Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, first, validation_request.budget, .{}));
            }
            try db.core.store.putBatch(&.{.{ .key = &Validation.key(binding), .value = saved }}, &.{});
        }
        // Inventory readiness alone cannot publish the candidate schema.
        try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
        const Verification = @import("relational_index_validation.zig");
        const physical = @import("relational_index_records.zig");
        const Keys = struct { forward: []u8, reverse: []u8, ownership: []u8, primary: []u8 };
        const saved_keys: Keys = keys: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var cursor = try read.openCursor();
            defer cursor.close();
            const entry = (try cursor.seekAtOrAfter(&try physical.forwardPrefix(binding.index))).?;
            const forward = try alloc.dupe(u8, entry.key);
            errdefer alloc.free(forward);
            const owned = try physical.forwardOwnership(forward);
            const raw = try std.mem.concat(alloc, u8, &.{ &.{internal.user_namespace}, owned.document_component });
            defer alloc.free(raw);
            const document = (try internal.decodeDocumentComponentAlloc(alloc, raw)).?;
            defer alloc.free(document);
            var reverse: std.ArrayList(u8) = .empty;
            errdefer reverse.deinit(alloc);
            try physical.appendReverseKey(alloc, &reverse, binding.index, document);
            var ownership: std.ArrayList(u8) = .empty;
            errdefer ownership.deinit(alloc);
            try physical.appendOwnershipFromReverse(alloc, &ownership, reverse.items);
            break :keys .{ .forward = forward, .reverse = try reverse.toOwnedSlice(alloc), .ownership = try ownership.toOwnedSlice(alloc), .primary = try internal.relationalRowKeyAlloc(alloc, document) };
        };
        defer inline for (@typeInfo(Keys).@"struct".field_names) |field| alloc.free(@field(saved_keys, field));
        var saved_values: [4][]u8 = undefined;
        inline for (@typeInfo(Keys).@"struct".field_names, 0..) |field, i| {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            saved_values[i] = try alloc.dupe(u8, try read.get(@field(saved_keys, field)));
        }
        defer for (saved_values) |value| alloc.free(value);
        const Fault = enum { missing_forward, missing_reverse, missing_ownership, bad_cover, malformed_forward, orphan_reverse, orphan_ownership, removed_row_and_pairs };
        for ([_]Fault{ .missing_forward, .missing_reverse, .missing_ownership, .bad_cover, .malformed_forward, .orphan_reverse, .orphan_ownership, .removed_row_and_pairs }) |fault| {
            var extra: std.ArrayList(u8) = .empty;
            defer extra.deinit(alloc);
            if (fault == .malformed_forward) try extra.appendSlice(alloc, &(try physical.forwardPrefix(binding.index))) else {
                try physical.appendReverseKey(alloc, &extra, binding.index, "orphan");
                if (fault == .orphan_ownership) {
                    var ownership: std.ArrayList(u8) = .empty;
                    defer ownership.deinit(alloc);
                    try physical.appendOwnershipFromReverse(alloc, &ownership, extra.items);
                    extra.clearRetainingCapacity();
                    try extra.appendSlice(alloc, ownership.items);
                }
            }
            {
                var corrupt = try db.core.store.beginWriteTxn();
                errdefer corrupt.abort();
                switch (fault) {
                    .missing_forward => try corrupt.delete(saved_keys.forward),
                    .missing_reverse => try corrupt.delete(saved_keys.reverse),
                    .missing_ownership => try corrupt.delete(saved_keys.ownership),
                    .bad_cover => {
                        const value = try alloc.dupe(u8, saved_values[0]);
                        defer alloc.free(value);
                        try std.testing.expect(value.len > 4);
                        value[0] ^= 1;
                        var crc = @import("antfly_hash").Crc32c.init();
                        crc.update(saved_keys.forward);
                        crc.update(value[0 .. value.len - 4]);
                        std.mem.writeInt(u32, value[value.len - 4 ..][0..4], crc.final(), .little);
                        try corrupt.put(saved_keys.forward, value);
                    },
                    .malformed_forward, .orphan_reverse, .orphan_ownership => try corrupt.put(extra.items, ""),
                    .removed_row_and_pairs => inline for (@typeInfo(Keys).@"struct".field_names) |field| {
                        try corrupt.delete(@field(saved_keys, field));
                    },
                }
                try corrupt.commit();
            }
            var turns: usize = 0;
            while (true) {
                const outcome = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
                try outcome.validateFor(validation_request);
                try std.testing.expect(outcome == .generation);
                turns += 1;
                try std.testing.expect(turns <= 40);
                if (outcome.generation.record.fields.phase == .failed) {
                    const expected_failure: Verification.Failure = switch (fault) {
                        .missing_forward => .missing_forward,
                        .missing_reverse => .missing_reverse,
                        .missing_ownership => .invalid_ownership,
                        .bad_cover => .invalid_payload,
                        .malformed_forward => .malformed_key,
                        .orphan_reverse, .orphan_ownership => .missing_primary,
                        .removed_row_and_pairs => .count_mismatch,
                    };
                    try std.testing.expectEqual(expected_failure, outcome.generation.record.fields.failure);
                    {
                        var inspect = try db.core.store.beginReadTxn();
                        defer inspect.abort();
                        switch (fault) {
                            .missing_forward => try std.testing.expectError(error.NotFound, inspect.get(saved_keys.forward)),
                            .missing_reverse => try std.testing.expectError(error.NotFound, inspect.get(saved_keys.reverse)),
                            .missing_ownership => try std.testing.expectError(error.NotFound, inspect.get(saved_keys.ownership)),
                            .bad_cover => try std.testing.expect(!std.mem.eql(u8, saved_values[0], try inspect.get(saved_keys.forward))),
                            .malformed_forward, .orphan_reverse, .orphan_ownership => try std.testing.expectEqualStrings("", try inspect.get(extra.items)),
                            .removed_row_and_pairs => inline for (@typeInfo(Keys).@"struct".field_names) |field| {
                                try std.testing.expectError(error.NotFound, inspect.get(@field(saved_keys, field)));
                            },
                        }
                    }
                    db.close();
                    db = try db_mod.DB.open(alloc, directory.path(), options);
                    const retained = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
                    try std.testing.expect(retained.generation.duplicate);
                    try std.testing.expectEqualDeep(outcome.generation.record, retained.generation.record);
                    break;
                }
                try std.testing.expect(outcome.generation.record.fields.phase != .ready);
            }
            // Privileged fault injection/reset, not a repair endpoint: each
            // fixture starts from the exact same primary/companion bytes.
            var reset = try db.core.store.beginWriteTxn();
            errdefer reset.abort();
            inline for (@typeInfo(Keys).@"struct".field_names, 0..) |field, i| try reset.put(@field(saved_keys, field), saved_values[i]);
            try reset.delete(extra.items);
            try reset.delete(&Verification.key(binding));
            try reset.commit();
        }
        {
            var lease = try db.native_index_build_cache.acquire(std.testing.io, alloc, &manager, db.core, validation_request, binding, .{});
            defer lease.deinit();
            try std.testing.expectError(error.PrivateIndexTargetValidationRequired, db.sealNativeIndexOwnerValidation(validation_request, .{}));
            var page = (try native.PreparedGenerationValidation.prepare(alloc, db.core, lease.reservation(), validation_request.target_expectation.?, validation_request.budget, .{})).?;
            defer page.deinit();
            {
                const raw_cursor = try alloc.alloc(u8, @import("relational_index_limits.zig").max_cursor_key_bytes);
                defer alloc.free(raw_cursor);
                @memset(raw_cursor, 255);
                const b64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(raw_cursor.len));
                defer alloc.free(b64);
                _ = std.base64.standard.Encoder.encode(b64, raw_cursor);
                var wire_command = page.page.command;
                wire_command.cursor_b64 = b64;
                wire_command.next.cursor_digest = @import("relational_integrity_contract.zig").hash(raw_cursor);
                const packet = try std.json.Stringify.valueAlloc(alloc, .{ .schema_version = native.envelope_version, .request = @as(struct {}, .{}), .index_owner_control = native.Frame{ .effect = .{ .generation_validation = wire_command }, .position = frozen.inventory.position } }, .{});
                defer alloc.free(packet);
                var parsed_packet = try @import("replication_effects.zig").decodeBatchMutationRequest(alloc, .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 3, .previous_lsn = 2, .payload = packet });
                defer parsed_packet.deinit();
                try std.testing.expectEqualStrings(b64, parsed_packet.value.index_owner_control.?.effect.generation_validation.cursor_b64);
                // Codec coverage is not a continuation proof: the actual
                // replayer must reject this substituted physical cursor.
                try std.testing.expectError(error.InvalidRelationalIndexValidation, native.PreparedGenerationValidation.replay(alloc, db.core, wire_command));
            }
            var replayed_page = try native.PreparedGenerationValidation.replay(alloc, db.core, page.page.command);
            replayed_page.deinit();
            var forged = page.page.command;
            forged.next.chain[0] ^= 1;
            try std.testing.expectError(error.InvalidRelationalIndexValidation, native.PreparedGenerationValidation.replay(alloc, db.core, forged));
            try page.preparePayload();
            const sequence = current: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :current (try authority.load(&read)).?.sequence + 1;
            };
            const position: @import("receipt_position.zig").Native = .{ .namespace = binding.fence.namespace, .sequence = sequence };
            const payload = try page.finalizePayload(position);
            const record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 3, .previous_lsn = 2, .payload = payload };
            try replication_ingress.applyRecord(&db, record);
            try replication_ingress.applyRecord(&db, record);
            try std.testing.expectEqual(@as(u64, 3), try db.replicationAppliedSequence());
        }
        var turns: usize = 0;
        var prior_counts: [3]u64 = .{ 3, 0, 0 };
        while (true) {
            const outcome = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
            try outcome.validateFor(validation_request);
            const fields = outcome.generation.record.fields;
            var examined: u64 = 0;
            for (fields.counts, prior_counts) |count, prior_count| {
                try std.testing.expect(count >= prior_count);
                examined += count - prior_count;
            }
            try std.testing.expect(examined <= 3);
            prior_counts = fields.counts;
            turns += 1;
            try std.testing.expect(turns <= 40);
            if (turns == 5) {
                db.close();
                db = try db_mod.DB.open(alloc, directory.path(), options);
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                var retained = (try Verification.load(alloc, &read, binding)).?;
                defer retained.deinit();
                try std.testing.expectEqualDeep(outcome.generation.record, retained.summary);
            }
            if (fields.phase == .ready) {
                try std.testing.expectEqualDeep(@as([3]u64, @splat(32)), fields.counts);
                const observed_generation = try db.stepNativeIndexOwnerBuild("items", validation_request, .{});
                try std.testing.expect(observed_generation.generation.duplicate);
                try std.testing.expectEqual(outcome.generation.position, observed_generation.generation.position);
                break;
            }
            try std.testing.expect(fields.phase != .failed);
        }
        {
            var inventory_audit: ?Validation.Audit = null;
            var inventory_turns: usize = 0;
            while (true) {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                inventory_audit = try Validation.auditPage(alloc, &read, binding, validation_request.target_expectation.?, db.root_incarnation, inventory_audit, validation_request.budget, .{});
                inventory_turns += 1;
                try std.testing.expect(inventory_turns <= 48);
                if (inventory_audit.?.fields.phase == .ready) break;
                try std.testing.expect(inventory_audit.?.fields.phase != .failed);
            }
            var lease = try db.native_index_build_cache.acquire(std.testing.io, alloc, &manager, db.core, validation_request, binding, .{});
            defer lease.deinit();
            const addition = &lease.reservation().artifacts.?.addition;
            const canonical = canonical: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :canonical try alloc.dupe(u8, try read.get(&Verification.key(binding)));
            };
            defer alloc.free(canonical);
            const sequence = sequence: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :sequence (try authority.load(&read)).?.sequence;
            };
            var audit: ?Verification.Audit = null;
            defer if (audit) |*value| value.deinit();
            var audit_turns: usize = 0;
            while (true) {
                const next = try Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation, inventory_audit.?, if (audit) |*value| value else null, validation_request.budget, .{});
                if (audit) |*value| value.deinit();
                audit = next;
                audit_turns += 1;
                try std.testing.expect(audit_turns <= 40);
                if (audit_turns == 2) {
                    try std.testing.expectError(error.PrivateIndexAuditScopeChanged, Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation ^ 1, inventory_audit.?, &audit.?, validation_request.budget, .{}));
                    var other_inventory = inventory_audit.?;
                    other_inventory.page_digest[0] ^= 1;
                    try std.testing.expectError(error.PrivateIndexAuditScopeChanged, Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation, other_inventory, &audit.?, validation_request.budget, .{}));
                }
                if (audit.?.progress.summary.fields.phase == .ready) break;
                try std.testing.expect(audit.?.progress.summary.fields.phase != .failed);
            }
            try std.testing.expectEqualDeep(@as([3]u64, @splat(32)), audit.?.progress.summary.fields.counts);
            var generation_audit_canceled = std.atomic.Value(bool).init(true);
            try std.testing.expectError(error.Canceled, Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation, inventory_audit.?, &audit.?, validation_request.budget, .{ .cancellation = @import("../../api/operation.zig").CancellationToken.fromAtomic(&generation_audit_canceled) }));
            audit.?.progress.digest[0] ^= 1;
            try std.testing.expectError(error.InvalidRelationalIndexValidation, Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation, inventory_audit.?, &audit.?, validation_request.budget, .{}));
            audit.?.progress.digest[0] ^= 1;
            var repeated = try Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation, inventory_audit.?, &audit.?, validation_request.budget, .{});
            defer repeated.deinit();
            try std.testing.expectEqualDeep(audit.?.progress.summary, repeated.progress.summary);
            try std.testing.expectEqualSlices(u8, audit.?.progress.cursor, repeated.progress.cursor);
            // Imported canonical readiness cannot hide physical corruption.
            // This changes no logical claim; the completed inventory audit
            // remains scoped correctly, but the generation must be re-read.
            try db.core.store.putBatch(&.{}, &.{saved_keys.forward});
            var corrupt_audit: ?Verification.Audit = null;
            defer if (corrupt_audit) |*value| value.deinit();
            var corrupt_turns: usize = 0;
            while (true) {
                const next = try Verification.auditPage(alloc, db.core, binding, validation_request.target_expectation.?, addition, db.root_incarnation, inventory_audit.?, if (corrupt_audit) |*value| value else null, validation_request.budget, .{});
                if (corrupt_audit) |*value| value.deinit();
                corrupt_audit = next;
                corrupt_turns += 1;
                try std.testing.expect(corrupt_turns <= 40);
                if (corrupt_audit.?.progress.summary.fields.phase == .failed) break;
                try std.testing.expect(corrupt_audit.?.progress.summary.fields.phase != .ready);
            }
            try std.testing.expectEqual(Verification.Failure.missing_forward, corrupt_audit.?.progress.summary.fields.failure);
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                var retained = (try Verification.load(alloc, &read, binding)).?;
                defer retained.deinit();
                try std.testing.expectEqual(Verification.Phase.ready, retained.summary.fields.phase);
                try std.testing.expectEqualSlices(u8, canonical, try read.get(&Verification.key(binding)));
                try std.testing.expectEqual(sequence, (try authority.load(&read)).?.sequence);
                try std.testing.expectError(error.NotFound, read.get(saved_keys.forward));
            }
            try db.core.store.putBatch(&.{.{ .key = saved_keys.forward, .value = saved_values[0] }}, &.{});
        }
        {
            const checkpoint = @import("relational_index_audit_checkpoint.zig");
            const io = std.testing.io;
            _ = try @import("root_signing_identity.zig").loadOrCreate(alloc, io, directory.path());
            try std.testing.expectError(error.PrivateIndexAuditScopeChanged, db.stepNativeIndexOwnerAudit("items", validation_request, directory.path(), db.root_incarnation ^ 1, .{}));
            const original = original: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :original .{ try alloc.dupe(u8, try read.get(&Validation.key(binding))), try alloc.dupe(u8, try read.get(&Verification.key(binding))), (try authority.load(&read)).?.sequence };
            };
            defer alloc.free(original[0]);
            defer alloc.free(original[1]);
            var restarted_inventory = false;
            var restarted_generation = false;
            var audit_turns: usize = 0;
            var terminal: ?checkpoint.Summary = null;
            while (true) {
                const result = try db.stepNativeIndexOwnerAudit("items", validation_request, directory.path(), db.root_incarnation, .{});
                audit_turns += 1;
                try std.testing.expect(audit_turns <= 96);
                try std.testing.expect(result.inventory.phase != .failed);
                if (result.generation) |fields| {
                    try std.testing.expect(fields.phase != .failed);
                    if (fields.phase == .ready) {
                        terminal = result;
                        break;
                    }
                }
                const restart_inventory = audit_turns == 2;
                const restart_generation = result.generation != null and result.generation.?.counts[0] != 0 and !restarted_generation;
                if (restart_inventory or restart_generation) {
                    restarted_inventory = restarted_inventory or restart_inventory;
                    restarted_generation = restarted_generation or restart_generation;
                    db.close();
                    db = try db_mod.DB.open(alloc, directory.path(), options);
                    var guard = try checkpoint.open(alloc, io, directory.path(), db.root_incarnation, db.root_incarnation, binding, validation_request.target_expectation.?, .{});
                    defer guard.deinit();
                    var retained = (try guard.load()).?;
                    defer retained.deinit();
                    try std.testing.expectEqualDeep(result, try guard.summary(retained));
                }
            }
            try std.testing.expect(restarted_inventory and restarted_generation);
            const repeated = try db.stepNativeIndexOwnerAudit("items", validation_request, directory.path(), db.root_incarnation, .{});
            try std.testing.expectEqualDeep(terminal.?, repeated);
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqualSlices(u8, original[0], try read.get(&Validation.key(binding)));
                try std.testing.expectEqualSlices(u8, original[1], try read.get(&Verification.key(binding)));
                try std.testing.expectEqual(original[2], (try authority.load(&read)).?.sequence);
            }
            const raw = raw: {
                var guard = try checkpoint.open(alloc, io, directory.path(), db.root_incarnation, db.root_incarnation, binding, validation_request.target_expectation.?, .{});
                defer guard.deinit();
                const Wait = struct {
                    calls: usize = 0,
                    fn waitCanceled(ptr: *const anyopaque) bool {
                        const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
                        self.calls += 1;
                        return self.calls >= 32;
                    }
                };
                {
                    var other_binding = binding;
                    other_binding.fence.namespace.range_id += 1;
                    var other_expected = validation_request.target_expectation.?;
                    other_expected.binding_digest = Integrity.hash(&try other_binding.encode());
                    var independent = try checkpoint.open(alloc, io, directory.path(), db.root_incarnation, db.root_incarnation, other_binding, other_expected, .{});
                    defer independent.deinit();
                    try std.testing.expect(!std.mem.eql(u8, guard.path, independent.path));
                    try std.testing.expect(try independent.load() == null);
                }
                var waiting: Wait = .{};
                try std.testing.expectError(error.Canceled, checkpoint.open(alloc, io, directory.path(), db.root_incarnation, db.root_incarnation, binding, validation_request.target_expectation.?, .{ .cancellation = .{ .ptr = &waiting, .is_cancelled_fn = Wait.waitCanceled } }));
                try std.testing.expectEqual(@as(usize, 32), waiting.calls);
                const temporary = try std.fmt.allocPrint(alloc, "{s}.tmp", .{guard.path});
                defer alloc.free(temporary);
                const stale = try std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true });
                stale.close(io);
                break :raw try std.Io.Dir.cwd().readFileAlloc(io, guard.path, alloc, .limited(Verification.max_wire_bytes + 16384));
            };
            defer alloc.free(raw);
            {
                var next_binding = binding;
                next_binding.id[0] ^= 1;
                var next_expected = validation_request.target_expectation.?;
                next_expected.binding_digest = Integrity.hash(&try next_binding.encode());
                var guard = try checkpoint.open(alloc, io, directory.path(), db.root_incarnation, db.root_incarnation, next_binding, next_expected, .{});
                defer guard.deinit();
                try std.testing.expect(try guard.load() == null);
            }
            {
                var guard = try checkpoint.open(alloc, io, directory.path(), db.root_incarnation, db.root_incarnation, binding, validation_request.target_expectation.?, .{});
                defer guard.deinit();
                const temporary = try std.fmt.allocPrint(alloc, "{s}.tmp", .{guard.path});
                defer alloc.free(temporary);
                try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, temporary, .{}));
                var retained = (try guard.load()).?;
                defer retained.deinit();
                try std.testing.expectEqualDeep(terminal.?, try guard.summary(retained));
                const Write = struct {
                    fn at(path: []const u8, bytes: []const u8) !void {
                        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .write_only });
                        defer file.close(std.testing.io);
                        var buffer: [4096]u8 = undefined;
                        var writer = file.writer(std.testing.io, &buffer);
                        try writer.interface.writeAll(bytes);
                        try writer.end();
                        try file.sync(std.testing.io);
                    }
                };
                const corrupted = try alloc.dupe(u8, raw);
                defer alloc.free(corrupted);
                corrupted[corrupted.len - 1] ^= 1;
                try Write.at(guard.path, corrupted);
                try std.testing.expectError(error.InvalidRelationalIndexValidation, guard.load());
                // Controlled fault restoration, never a checkpoint repair API.
                try Write.at(guard.path, raw);
            }
            {
                // Even an intact source checkpoint cannot become local proof
                // merely by copying it into another restored physical root.
                var peer = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-audit-peer-root");
                defer peer.cleanup();
                const peer_key = try @import("root_signing_identity.zig").loadOrCreate(alloc, io, peer.path());
                var guard = try checkpoint.open(alloc, io, peer.path(), peer_key.root_incarnation, peer_key.root_incarnation, binding, validation_request.target_expectation.?, .{});
                defer guard.deinit();
                {
                    var file = try @import("antfly_runtime_fs").fs_paths.createFilePortable(io, guard.path, .{ .exclusive = true, .permissions = if (comptime @import("builtin").os.tag == .windows) .default_file else @fromBackingInt(0o600) });
                    defer file.close(io);
                    var buffer: [4096]u8 = undefined;
                    var writer = file.writer(io, &buffer);
                    try writer.interface.writeAll(raw);
                    try writer.end();
                    try file.sync(io);
                }
                try std.testing.expectError(error.InvalidRelationalIndexValidation, guard.load());
            }
        }
        try std.testing.expectEqual(@as(u64, 1), db.native_index_build_cache.compilations);
        {
            const MetadataController = @import("relational_index_catalog.zig").Controller;
            const reserved_keys = [_][]const u8{ Verification.Seal.key, &Verification.key(binding), &Validation.key(binding), @import("relational_index_owner_job.zig").key ++ ":future-receipt" };
            for (reserved_keys) |reserved_key| {
                try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, MetadataController.validateExtraMetadata(&.{.{ .key = reserved_key, .value = "forged" }}, &.{}));
                try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, MetadataController.validateExtraMetadata(&.{}, &.{reserved_key}));
            }
            try MetadataController.validateExtraMetadata(&.{.{ .key = @import("relational_index_owner_job.zig").key ++ "-unrelated", .value = "allowed" }}, &.{});
            const command = command: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :command try Verification.SealCommand.prepare(alloc, &read, binding, validation_request.target_expectation.?);
            };
            const sequence = current: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :current (try authority.load(&read)).?.sequence;
            };
            const AllocationFaults = struct {
                fn stage(task_alloc: std.mem.Allocator, database: *db_mod.DB, seal_command: @import("relational_index_validation.zig").SealCommand) !void {
                    var txn = try database.core.store.beginWriteTxn();
                    defer txn.abort();
                    _ = try @import("relational_index_validation.zig").Seal.stage(task_alloc, &txn, seal_command, null);
                }
            };
            try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, AllocationFaults.stage, .{ &db, command });
            var canceled_seal = std.atomic.Value(bool).init(true);
            try std.testing.expectError(error.Canceled, db.sealNativeIndexOwnerValidation(validation_request, .{ .cancellation = @import("../../api/operation.zig").CancellationToken.fromAtomic(&canceled_seal) }));
            // A staged receipt is not durable. Abort must remove both the seal
            // and its native cut, leaving the same retry position available.
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                const staged = try Verification.Seal.stage(alloc, &txn, command, null);
                try std.testing.expect(!staged.duplicate);
                try std.testing.expectEqual(sequence + 1, staged.seal.position.sequence);
            }
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect(try Verification.Seal.load(alloc, &read, binding) == null);
                try std.testing.expectEqual(sequence, (try authority.load(&read)).?.sequence);
            }
            var forged = command;
            forged.generation_digest[0] ^= 1;
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                try std.testing.expectError(error.InvalidRelationalIndexValidation, Verification.Seal.stage(alloc, &txn, forged, null));
                try std.testing.expectEqual(sequence, (try authority.load(&txn)).?.sequence);
            }
            var prepared: native.PreparedValidationSeal = .{ .alloc = alloc, .command = command };
            defer prepared.deinit();
            try prepared.preparePayload();
            const position: @import("receipt_position.zig").Native = .{ .namespace = binding.fence.namespace, .sequence = sequence + 1 };
            const payload = try prepared.finalizePayload(position);
            const record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 4, .previous_lsn = 3, .payload = payload };
            try replication_ingress.applyRecord(&db, record);
            try replication_ingress.applyRecord(&db, record);
            try std.testing.expectEqual(@as(u64, 4), try db.replicationAppliedSequence());
            const observed = try db.sealNativeIndexOwnerValidation(validation_request, .{});
            try std.testing.expect(observed.duplicate);
            try std.testing.expectEqual(position, observed.position);
            try std.testing.expectEqualDeep(try command.receipt(position), observed.receipt.?);
            try std.testing.expectEqual(@import("relational_index_publication_progress.zig").Action.validated, observed.receipt.?.action);
            // Reopen exercises structural seal recovery; it does not grant a
            // replacement root permission to reuse physical audit evidence.
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), options);
            const reopened = try db.sealNativeIndexOwnerValidation(validation_request, .{});
            try std.testing.expectEqualDeep(observed, reopened);
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try testNativeIndexActivationAllocations(alloc, &read, binding, &candidate);
                try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, testNativeIndexActivationAllocations, .{ &read, binding, &candidate });
            }
            {
                const api = @import("../../schema/mod.zig");
                const activation = @import("relational_integrity_activation.zig");
                var parsed_after = try api.parseValidatedTableSchema(alloc, after);
                defer parsed_after.deinit(alloc);
                const runtime = try api.deriveRuntimeTableSchema(alloc, parsed_after);
                defer @import("../schema.zig").freeSchema(alloc, runtime);
                const metadata = [_]docstore.KVPair{.{ .key = "\x00\x00__metadata__:schema_json", .value = after }};
                var schema_prepared = try db.core.prepareSchemaMetadataReservedIndex(runtime, &metadata, .{ .binding = binding, .candidate = &candidate });
                defer schema_prepared.deinit();
                const activated = &schema_prepared.reserved_activation.?;
                try std.testing.expectEqual(activation.State.enforced, (try activation.Progress.decode(activated.value)).state);
                try std.testing.expectError(error.PrivateIndexPublicationRequired, db.core.commitPreparedSchemaMetadata(&schema_prepared, &metadata, &.{}, null));
                {
                    // Transaction-component coverage, not a metadata-authorized
                    // install: the decision digest is test data and every write
                    // aborts. No staged receipt may escape as durable evidence.
                    const Digests = struct {
                        fn of(bytes: []const u8) [32]u8 {
                            var result: [32]u8 = undefined;
                            std.crypto.hash.Blake3.hash(bytes, &result, .{});
                            return result;
                        }
                    };
                    const install_command: Verification.InstallCommand = .{
                        .validation = activated.seal,
                        .metadata_decision = @splat(7),
                        .schema_digest = Digests.of(schema_prepared.encoded),
                        .activation_digest = Digests.of(activated.value),
                    };
                    const InstallAllocations = struct {
                        fn run(task_alloc: std.mem.Allocator, database: *db_mod.DB, prepared_schema: *@import("core.zig").PreparedSchemaMetadata, requested: Verification.InstallCommand) !void {
                            var install_txn = try database.core.store.beginWriteTxn();
                            defer install_txn.abort();
                            try requested.validateBefore(task_alloc, &install_txn);
                            try prepared_schema.reserved_activation.?.validateBefore(&install_txn);
                            try install_txn.put(@import("../schema.zig").schema_key, prepared_schema.encoded);
                            _ = try prepared_schema.relational_indexes.?.metadata.stage(&install_txn);
                            try prepared_schema.integrity_catalog.?.stage(&install_txn);
                            try prepared_schema.reserved_activation.?.stage(&install_txn);
                            _ = try Verification.Installed.stage(task_alloc, &install_txn, requested, prepared_schema.integrity_catalog.?.catalog, null);
                        }
                    };
                    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, InstallAllocations.run, .{ &db, &schema_prepared, install_command });
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    try install_command.validateBefore(alloc, &txn);
                    try activated.validateBefore(&txn);
                    try txn.put(@import("../schema.zig").schema_key, schema_prepared.encoded);
                    _ = try schema_prepared.relational_indexes.?.metadata.stage(&txn);
                    try schema_prepared.integrity_catalog.?.stage(&txn);
                    try activated.stage(&txn);
                    const staged = try Verification.Installed.stage(alloc, &txn, install_command, schema_prepared.integrity_catalog.?.catalog, null);
                    try std.testing.expect(!staged.duplicate);
                    try std.testing.expectEqual(sequence + 2, staged.record.position.sequence);
                    const owner_job = @import("relational_index_owner_job.zig");
                    try std.testing.expectEqual(owner_job.Phase.installed, (try owner_job.load(&txn)).?.phase);
                    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, owner_job.requireReserved(&txn, binding));
                    const retried = try Verification.Installed.stage(alloc, &txn, install_command, schema_prepared.integrity_catalog.?.catalog, staged.record.position);
                    try std.testing.expect(retried.duplicate);
                    try std.testing.expectEqualDeep(staged.record, retried.record);
                    const PhysicalCatalog = @import("relational_index_catalog.zig");
                    var installed_catalog = try PhysicalCatalog.decode(alloc, binding.physical_candidate, try txn.get(&PhysicalCatalog.blobKey(binding.physical_candidate.blob_digest)));
                    defer installed_catalog.deinit();
                    try owner_job.verifyRecovery(alloc, &txn, &installed_catalog);
                    var wrong_cut = staged.record.position;
                    wrong_cut.sequence += 1;
                    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, Verification.Installed.stage(alloc, &txn, install_command, schema_prepared.integrity_catalog.?.catalog, wrong_cut));
                    try txn.delete(Verification.Installed.key);
                    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, owner_job.verifyRecovery(alloc, &txn, &installed_catalog));
                }
                {
                    const InstallFault = struct {
                        txn: *docstore.DocStore.Txn,
                        target: []const u8,
                        after_put: bool,
                        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                            return self.txn.get(key);
                        }
                        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
                            if (!std.mem.eql(u8, key, self.target)) return self.txn.put(key, value);
                            if (self.after_put) try self.txn.put(key, value);
                            return error.TestInstalledWriteFailed;
                        }
                    };
                    const failure_keys = [_][]const u8{ authority.key, @import("relational_index_owner_job.zig").key, Verification.Installed.key };
                    for (failure_keys) |failure_key| for ([_]bool{ false, true }) |after_put| {
                        var txn = try db.core.store.beginWriteTxn();
                        defer txn.abort();
                        try activated.validateBefore(&txn);
                        try txn.put(@import("../schema.zig").schema_key, schema_prepared.encoded);
                        _ = try schema_prepared.relational_indexes.?.metadata.stage(&txn);
                        try schema_prepared.integrity_catalog.?.stage(&txn);
                        try activated.stage(&txn);
                        var schema_digest: [32]u8 = undefined;
                        std.crypto.hash.Blake3.hash(schema_prepared.encoded, &schema_digest, .{});
                        var activation_digest: [32]u8 = undefined;
                        std.crypto.hash.Blake3.hash(activated.value, &activation_digest, .{});
                        const install_command: Verification.InstallCommand = .{ .validation = activated.seal, .metadata_decision = @splat(7), .schema_digest = schema_digest, .activation_digest = activation_digest };
                        var fault: InstallFault = .{ .txn = &txn, .target = failure_key, .after_put = after_put };
                        try std.testing.expectError(error.TestInstalledWriteFailed, Verification.Installed.stage(alloc, &fault, install_command, schema_prepared.integrity_catalog.?.catalog, null));
                    };
                }
                {
                    var read = try db.core.store.beginReadTxn();
                    defer read.abort();
                    try std.testing.expect(try Verification.Installed.load(alloc, &read, binding) == null);
                    try std.testing.expectEqual(sequence + 1, (try authority.load(&read)).?.sequence);
                    try @import("relational_index_owner_job.zig").requireReserved(&read, binding);
                }
                {
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    try txn.delete(Verification.Seal.key);
                    try std.testing.expectError(error.PrivateIndexTargetValidationRequired, activated.validateBefore(&txn));
                }
                {
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    const stale: activation.Progress = .{ .generation_set = activation.generationSet(candidate.reservation.catalog), .owner = try activation.ownership(&txn), .schema_version = 1 };
                    const bytes = try stale.encode(alloc);
                    defer alloc.free(bytes);
                    try txn.put(activation.key, bytes);
                    try std.testing.expectError(error.ConstraintActivationChanged, activated.validateBefore(&txn));
                }
                {
                    // Component staging only: no metadata decision, installed
                    // job/cut or outbox is invented, and the transaction aborts.
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    try activated.validateBefore(&txn);
                    try txn.put(@import("../schema.zig").schema_key, schema_prepared.encoded);
                    _ = try schema_prepared.relational_indexes.?.metadata.stage(&txn);
                    try schema_prepared.integrity_catalog.?.stage(&txn);
                    try activated.stage(&txn);
                    try activation.requireReady(&txn, schema_prepared.integrity_catalog.?.catalog);
                    const fence = (try @import("relational_integrity_topology.zig").current(&txn)).?;
                    try std.testing.expect(fence.eql(binding.fence));
                }
                const ActivationFault = struct {
                    txn: *docstore.DocStore.Txn,
                    after_put: bool,
                    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                        return self.txn.get(key);
                    }
                    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
                        if (self.after_put) try self.txn.put(key, value);
                        return error.TestActivationWriteFailed;
                    }
                };
                for ([_]bool{ false, true }) |after_put| {
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    try activated.validateBefore(&txn);
                    try txn.put(@import("../schema.zig").schema_key, schema_prepared.encoded);
                    _ = try schema_prepared.relational_indexes.?.metadata.stage(&txn);
                    try schema_prepared.integrity_catalog.?.stage(&txn);
                    var activation_fault: ActivationFault = .{ .txn = &txn, .after_put = after_put };
                    try std.testing.expectError(error.TestActivationWriteFailed, activated.stage(&activation_fault));
                    if (after_put) {
                        try std.testing.expectEqual(activation.State.enforced, (try activation.Progress.decode(try txn.get(activation.key))).state);
                    } else try std.testing.expectError(error.NotFound, txn.get(activation.key));
                }
                {
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    try activated.validateBefore(&txn);
                    try txn.put(@import("../schema.zig").schema_key, schema_prepared.encoded);
                    _ = try schema_prepared.relational_indexes.?.metadata.stage(&txn);
                    try schema_prepared.integrity_catalog.?.stage(&txn);
                    const range = @import("range_state.zig");
                    const moved = try range.encodeRangeAlloc(alloc, .{ .start = "row001", .end = "" });
                    defer alloc.free(moved);
                    try txn.put(range.range_key, moved);
                    try std.testing.expectError(error.ConstraintActivationOwnerChanged, activated.stage(&txn));
                }
                {
                    // Isolated storage qualification only: copy logical KV
                    // state, not the filesystem signing identity/audit files.
                    // No metadata authorization or physical adoption is claimed.
                    var install_directory = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-install-commit");
                    defer install_directory.cleanup();
                    var installed_db = try db_mod.DB.open(alloc, install_directory.path(), options);
                    var installed_db_open = true;
                    defer if (installed_db_open) installed_db.close();
                    try seedNativeIndexFixture(&db, &installed_db);
                    installed_db.close();
                    installed_db_open = false;
                    installed_db = try db_mod.DB.open(alloc, install_directory.path(), options);
                    installed_db_open = true;
                    try std.testing.expect(installed_db.root_incarnation != db.root_incarnation);
                    const installed_public_key = try api.versionedSchemaKeyAlloc(alloc, binding.physical_candidate.schema_version);
                    defer alloc.free(installed_public_key);
                    const install_metadata = [_]docstore.KVPair{ metadata[0], .{ .key = installed_public_key, .value = after } };
                    var install_schema = try installed_db.core.prepareSchemaMetadataReservedIndex(runtime, &install_metadata, .{ .binding = binding, .candidate = &candidate });
                    var install_schema_owned = true;
                    defer if (install_schema_owned) install_schema.deinit();
                    var schema_digest: [32]u8 = undefined;
                    std.crypto.hash.Blake3.hash(install_schema.encoded, &schema_digest, .{});
                    var activation_digest: [32]u8 = undefined;
                    std.crypto.hash.Blake3.hash(install_schema.reserved_activation.?.value, &activation_digest, .{});
                    const install_command: Verification.InstallCommand = .{ .validation = install_schema.reserved_activation.?.seal, .metadata_decision = @splat(7), .schema_digest = schema_digest, .activation_digest = activation_digest };
                    const InstallCommitHook = struct {
                        const marker = "\x00\x00__metadata__:native-index-install-test-marker";
                        fail: bool,
                        pub fn stage(hook: @This(), txn: anytype, installed_record: Verification.Installed) !void {
                            var bytes: [8]u8 = undefined;
                            std.mem.writeInt(u64, &bytes, installed_record.position.sequence, .little);
                            try txn.put(marker, &bytes);
                            if (hook.fail) return error.TestInstallCommitFailed;
                        }
                    };
                    {
                        installed_db.core.lockApplyExclusive();
                        defer installed_db.core.unlockApplyExclusive();
                        try std.testing.expectError(error.InvalidPrivateIndexInputs, installed_db.core.commitPreparedSchemaMetadataReservedIndex(&install_schema, &metadata, install_command, null, null));
                        var altered_metadata = install_metadata;
                        altered_metadata[0].value = "{}";
                        try std.testing.expectError(error.InvalidPrivateIndexInputs, installed_db.core.commitPreparedSchemaMetadataReservedIndex(&install_schema, &altered_metadata, install_command, null, null));
                        try std.testing.expectError(error.TestInstallCommitFailed, installed_db.core.commitPreparedSchemaMetadataReservedIndex(&install_schema, &install_metadata, install_command, null, InstallCommitHook{ .fail = true }));
                    }
                    try std.testing.expectEqual(@as(u32, 1), installed_db.core.schema.?.version);
                    try std.testing.expectEqual(@as(u32, 1), installed_db.core.table_catalog.active_schema_version);
                    {
                        var failed_read = try installed_db.core.store.beginReadTxn();
                        defer failed_read.abort();
                        try std.testing.expectError(error.NotFound, failed_read.get(InstallCommitHook.marker));
                        try std.testing.expect(try Verification.Installed.load(alloc, &failed_read, binding) == null);
                        try @import("relational_index_owner_job.zig").requireReserved(&failed_read, binding);
                        try std.testing.expectEqual(sequence + 1, (try authority.load(&failed_read)).?.sequence);
                    }
                    install_schema.deinit();
                    install_schema_owned = false;
                    var install_primary_directory = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-install-primary-outbox");
                    defer install_primary_directory.cleanup();
                    var install_primary = try db_mod.DB.open(alloc, install_primary_directory.path(), options);
                    var install_primary_open = true;
                    defer if (install_primary_open) install_primary.close();
                    try seedNativeIndexFixture(&db, &install_primary);
                    install_primary.close();
                    install_primary_open = false;
                    install_primary = try db_mod.DB.open(alloc, install_primary_directory.path(), options);
                    install_primary_open = true;
                    var install_barrier: @import("antfly_runtime_abi").mutation_barrier.MutationBarrier = .{};
                    var install_publisher: TestNativeIndexPublisher = .{ .barrier = &install_barrier, .lsn = 5 };
                    defer if (install_publisher.payload) |bytes| alloc.free(bytes);
                    defer if (install_primary_open) {
                        install_primary.local_execution.replication_async_batch_mirror = null;
                    };
                    install_primary.local_execution.replication_async_batch_mirror = install_publisher.asMirror();
                    const InstallPreparationChecks = struct {
                        fn run(database: *db_mod.DB, proposed: Verification.InstallCommand) !void {
                            var owned = try database.prepareNativeIndexOwnerInstallation(proposed, .{});
                            defer owned.deinit();
                        }
                    };
                    var mismatched_command = install_command;
                    mismatched_command.schema_digest[0] ^= 1;
                    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, InstallPreparationChecks.run(&install_primary, mismatched_command));
                    mismatched_command = install_command;
                    mismatched_command.activation_digest[0] ^= 1;
                    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, InstallPreparationChecks.run(&install_primary, mismatched_command));
                    const pending_install = pending_install: {
                        const primary_signer = try @import("root_signing_identity.zig").loadOrCreate(alloc, install_primary.backend_runtime.io().?, install_primary_directory.path());
                        const installation: native.InstallationRequest = .{ .intent = install_command.intent(), .budget = .{ .records = 4, .time_ns = std.time.ns_per_s } };
                        var partial_pages: usize = 0;
                        for (0..128) |_| {
                            const response = try install_primary.stepNativeIndexOwnerInstallation("items", installation, install_primary_directory.path(), primary_signer.root_incarnation, .{});
                            try response.validateFor(installation);
                            if (response == .installed) {
                                try std.testing.expect(partial_pages > 1);
                                try std.testing.expectEqualDeep(install_command, response.installed.logical.command);
                                break :pending_install response.installed.logical;
                            }
                            partial_pages += 1;
                            try std.testing.expectEqual(@as(u32, 1), install_primary.core.schema.?.version);
                            try std.testing.expectEqual(@as(usize, 0), install_publisher.attempts);
                            var unchanged = try install_primary.core.store.beginReadTxn();
                            defer unchanged.abort();
                            try @import("relational_index_owner_job.zig").requireReserved(&unchanged, binding);
                            try std.testing.expect(try Verification.Installed.load(alloc, &unchanged, binding) == null);
                        }
                        return error.TestInstallationDidNotComplete;
                    };
                    try pending_install.validate();
                    try std.testing.expect(!pending_install.duplicate and pending_install.replication_pending);
                    try std.testing.expect(pending_install.receipt == null);
                    try std.testing.expectEqualStrings("TestPublicationUnavailable", pending_install.error_name.?);
                    try std.testing.expectEqual(@as(usize, 1), install_publisher.attempts);
                    try std.testing.expectEqual(@as(usize, 0), install_publisher.completed);
                    try std.testing.expectEqual(@as(u32, 2), install_primary.core.schema.?.version);
                    try std.testing.expectEqual(@as(u32, 2), install_primary.core.table_catalog.active_schema_version);
                    {
                        var install_pending_page = try @import("durable_outbox_store.zig").readPending(alloc, install_primary.core.store);
                        defer install_pending_page.deinit(alloc);
                        try std.testing.expectEqual(@as(usize, 1), install_pending_page.entries.len);
                        var primary_read = try install_primary.core.store.beginReadTxn();
                        defer primary_read.abort();
                        const primary_installed = (try Verification.Installed.load(alloc, &primary_read, binding)).?;
                        try primary_installed.requireCurrent(alloc, &primary_read);
                        try std.testing.expectEqual(pending_install.position, primary_installed.position);
                    }
                    install_primary.local_execution.replication_async_batch_mirror = null;
                    install_primary.close();
                    install_primary_open = false;
                    install_primary = try db_mod.DB.open(alloc, install_primary_directory.path(), options);
                    install_primary_open = true;
                    install_publisher.fail = false;
                    install_primary.local_execution.replication_async_batch_mirror = install_publisher.asMirror();
                    try install_primary.flushDurableReplicationOutboxes();
                    try std.testing.expectEqual(@as(usize, 2), install_publisher.attempts);
                    try std.testing.expectEqual(@as(usize, 1), install_publisher.completed);
                    {
                        var recovered_page = try @import("durable_outbox_store.zig").readPending(alloc, install_primary.core.store);
                        defer recovered_page.deinit(alloc);
                        try std.testing.expect(recovered_page.isEmpty());
                    }
                    const primary_catalog_generation = install_primary.core.table_catalog.generation;
                    {
                        var recovered_install = try install_primary.prepareNativeIndexOwnerInstallationIntent(install_command.intent(), .{});
                        defer recovered_install.deinit();
                        try std.testing.expect(!recovered_install.requiresSchemaCommit());
                        try std.testing.expectEqualDeep(install_command, recovered_install.command);
                        const recovered_outcome = try install_primary.applyPreparedNativeIndexOwnerInstallation(&recovered_install, null);
                        try recovered_outcome.validate();
                        try std.testing.expect(recovered_outcome.duplicate and !recovered_outcome.replication_pending);
                        try std.testing.expectEqual(pending_install.position, recovered_outcome.position);
                        try std.testing.expectEqualDeep(try install_command.receipt(pending_install.position), recovered_outcome.receipt.?);
                    }
                    try std.testing.expectEqual(@as(usize, 2), install_publisher.attempts);
                    try std.testing.expectEqual(primary_catalog_generation, install_primary.core.table_catalog.generation);
                    {
                        var recovered_read = try install_primary.core.store.beginReadTxn();
                        defer recovered_read.abort();
                        try std.testing.expectEqual(pending_install.position.sequence, (try authority.load(&recovered_read)).?.sequence);
                    }
                    var install_packet: native.PreparedInstall = .{ .alloc = alloc, .command = install_command };
                    defer install_packet.deinit();
                    try install_packet.preparePayload();
                    const installed_position: @import("receipt_position.zig").Native = .{ .namespace = binding.fence.namespace, .sequence = sequence + 2 };
                    const install_payload = try install_packet.finalizePayload(installed_position);
                    try std.testing.expectEqualSlices(u8, install_payload, install_publisher.payload.?);
                    var install_record: @import("replication_record.zig").Record = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 5, .previous_lsn = 4, .payload = install_publisher.payload.? };
                    {
                        var pinned_before = installed_db.core.schema_registry.acquire().?;
                        defer pinned_before.release();
                        try replication_ingress.applyRecord(&installed_db, install_record);
                        try replication_ingress.applyRecord(&installed_db, install_record);
                        try std.testing.expectEqual(@as(u32, 1), pinned_before.version());
                    }
                    const committed: Verification.Installed = .{ .command = install_command, .position = installed_position };
                    try std.testing.expectEqual(@as(u64, 5), try installed_db.replicationAppliedSequence());
                    try std.testing.expectEqual(@as(u32, 2), installed_db.core.schema.?.version);
                    try std.testing.expectEqual(@as(u32, 2), installed_db.core.table_catalog.active_schema_version);
                    try std.testing.expectEqual(sequence + 2, committed.position.sequence);
                    installed_db.close();
                    installed_db_open = false;
                    installed_db = try db_mod.DB.open(alloc, install_directory.path(), options);
                    installed_db_open = true;
                    try std.testing.expectEqual(@as(u32, 2), installed_db.core.schema.?.version);
                    try std.testing.expectEqual(@as(u32, 2), installed_db.core.table_catalog.active_schema_version);
                    {
                        var installed_read = try installed_db.core.store.beginReadTxn();
                        defer installed_read.abort();
                        try std.testing.expectEqualDeep(committed, (try Verification.Installed.load(alloc, &installed_read, binding)).?);
                        try std.testing.expectError(error.NotFound, installed_read.get(InstallCommitHook.marker));
                        try std.testing.expectEqualStrings(after, try installed_read.get(installed_public_key));
                        try activation.requireReady(&installed_read, candidate.next.catalog);
                        try std.testing.expect((try @import("relational_integrity_topology.zig").current(&installed_read)).?.eql(binding.fence));
                    }
                    {
                        var duplicate_install = try installed_db.prepareNativeIndexOwnerInstallation(install_command, .{});
                        defer duplicate_install.deinit();
                        try std.testing.expect(!duplicate_install.requiresSchemaCommit());
                        const installed_retry = try installed_db.applyPreparedNativeIndexOwnerInstallation(&duplicate_install, null);
                        try installed_retry.validate();
                        try std.testing.expect(installed_retry.duplicate);
                        try std.testing.expectEqualDeep(try install_command.receipt(committed.position), installed_retry.receipt.?);
                    }
                    const catalog_generation = installed_db.core.table_catalog.generation;
                    install_record.lsn = 6;
                    install_record.previous_lsn = 5;
                    try replication_ingress.applyRecord(&installed_db, install_record);
                    try replication_ingress.applyRecord(&installed_db, install_record);
                    try std.testing.expectEqual(@as(u64, 6), try installed_db.replicationAppliedSequence());
                    try std.testing.expectEqual(catalog_generation, installed_db.core.table_catalog.generation);
                    {
                        var duplicate_read = try installed_db.core.store.beginReadTxn();
                        defer duplicate_read.abort();
                        try std.testing.expectEqual(committed.position.sequence, (try authority.load(&duplicate_read)).?.sequence);
                    }
                    var wrong_position = committed.position;
                    wrong_position.sequence += 1;
                    install_record.lsn = 7;
                    install_record.previous_lsn = 6;
                    install_record.payload = try install_packet.finalizePayload(wrong_position);
                    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, replication_ingress.applyRecord(&installed_db, install_record));
                    try std.testing.expectEqual(@as(u64, 6), try installed_db.replicationAppliedSequence());
                    try testInstalledIndexPhysicalAudit(&installed_db);
                    const ready_checkpoint = try testInstalledIndexCheckpoint(&installed_db, install_directory.path(), install_command.intent());
                    installed_db.close();
                    installed_db_open = false;
                    installed_db = try db_mod.DB.open(alloc, install_directory.path(), options);
                    installed_db_open = true;
                    const reopened_signer = try @import("root_signing_identity.zig").load(alloc, installed_db.backend_runtime.io().?, install_directory.path());
                    const resumed_checkpoint = try installed_db.stepNativeIndexOwnerInstallationAudit("items", install_command.intent(), .{ .records = 4, .time_ns = std.time.ns_per_s }, install_directory.path(), reopened_signer.root_incarnation, .{});
                    try std.testing.expectEqualDeep(ready_checkpoint, resumed_checkpoint);
                    try std.testing.expectEqual(@as(u64, 1), installed_db.native_index_build_cache.compilations);
                    const installed_row = (try installed_db.get(alloc, "row000")).?;
                    defer alloc.free(installed_row);
                    const source_row = (try db.get(alloc, "row000")).?;
                    defer alloc.free(source_row);
                    try std.testing.expectEqualStrings(source_row, installed_row);
                    try testNativeIndexRelease(&installed_db, committed);
                }
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try @import("relational_index_owner_job.zig").requireReserved(&read, binding);
                try std.testing.expectEqual(sequence + 1, (try authority.load(&read)).?.sequence);
                try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
            }
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const durable = (try Verification.Seal.load(alloc, &read, binding)).?;
                try std.testing.expectEqualDeep(command, durable.command);
                try std.testing.expectEqual(sequence + 1, (try authority.load(&read)).?.sequence);
                try std.testing.expect(try @import("relational_integrity_topology.zig").completed(&read) == null);
            }
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                try txn.delete(&Verification.key(binding));
                try std.testing.expectError(error.InvalidRelationalIndexValidation, Verification.verifyRecovery(alloc, &txn, binding));
            }
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                const corrupt = try alloc.dupe(u8, try txn.get(Verification.Seal.key));
                defer alloc.free(corrupt);
                corrupt[corrupt.len - 1] ^= 1;
                try txn.put(Verification.Seal.key, corrupt);
                try std.testing.expectError(error.InvalidRelationalIndexValidation, Verification.Seal.load(alloc, &txn, binding));
            }
        }
        try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    }
    var cancellation = try db.prepareNativeIndexOwnerCancellation(native.Admission.fromBinding(binding));
    defer cancellation.deinit();
    _ = try db.applyPreparedNativeIndexOwnerCancellation(&cancellation, null);
    try std.testing.expect(db.native_index_build_cache.entry == null);
    try std.testing.expectError(error.PrivateIndexPublicationCanceled, db.stepNativeIndexOwnerBuild("items", request, .{}));
    var cleanup = try db.prepareNativeIndexOwnerCleanup(identity);
    defer cleanup.deinit();
    _ = try db.applyPreparedNativeIndexOwnerCleanup(&cleanup, null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get(&@import("relational_index_private_claims.zig").InventoryValidation.key(binding)));
        try std.testing.expectError(error.NotFound, read.get(&@import("relational_index_validation.zig").key(binding)));
        try std.testing.expectError(error.NotFound, read.get(@import("relational_index_validation.zig").Seal.key));
    }
    db.close();
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().slices[slice].used_bytes);
}

test "relational index build pages claim backlog rejects the whole page and cancellation retires exact queue keys" {
    const native = @import("relational_index_owner_control.zig");
    const catalog = @import("relational_integrity_catalog.zig");
    const candidates = @import("relational_index_candidate.zig");
    const authority = @import("../source_authority.zig");
    const resources = @import("../resource_manager.zig");
    const before = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"additionalProperties\":false}}}}";
    const after = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"additionalProperties\":false}}},\"relational_indexes\":[{\"name\":\"email_idx\",\"keys\":[{\"column\":\"email\"}]}],\"unique_constraints\":[{\"name\":\"email_idx\",\"origin\":\"index\",\"columns\":[\"email\"]}]}";
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-claim-backlog");
    defer directory.cleanup();
    var resource_options: resources.Options = .{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 * 1024 } };
    resource_options.budgets[@backingInt(resources.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = 16 * 1024 * 1024 };
    var manager = resources.ResourceManager.init(resource_options);
    defer manager.deinit(alloc);
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} }, .online_source_authority = .native, .identity_namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .resource_manager = &manager };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try db.setSchemaJson(alloc, before);
    var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, before);
    defer parsed.deinit(alloc);
    var prior = try @import("../../schema/relational_declarations.zig").compileCatalog(alloc, parsed, 101, null);
    defer prior.deinit();
    try db.core.store.putBatch(&.{.{ .key = catalog.key, .value = prior.value }}, &.{});
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const writes = try arena.allocator().alloc(db_mod.types.BatchWrite, 64);
    for (writes, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "row{d:0>3}", .{i}), .value = try std.fmt.allocPrint(arena.allocator(), "{{\"id\":{d},\"email\":\"{d}@example.com\"}}", .{ i + 1, i }) };
    try db.batch(.{ .writes = writes });
    var candidate = try candidates.Candidate.prepare(alloc, 101, before, after, prior.value, "email_idx");
    defer candidate.deinit();
    var admission = try db.prepareNativeIndexOwnerAdmission(.{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = options.identity_namespace.?, .catalog_digest = @import("relational_index_inputs.zig").digest(prior.value) },
        .candidate_digest = @import("relational_index_inputs.zig").digest(candidate.next.value),
        .reservation_digest = @import("relational_index_inputs.zig").digest(candidate.reservation.value),
    }, candidate.inputs.bytes);
    defer admission.deinit();
    _ = try db.applyPreparedNativeIndexOwnerControl(&admission, null);
    for ([_]@import("relational_index_owner_job.zig").NativeEffect{ .fenced, .reserved }) |effect| {
        var command = try db.prepareNativeIndexOwnerControl(.{ .binding = admission.command.binding, .effect = effect });
        defer command.deinit();
        _ = try db.applyPreparedNativeIndexOwnerControl(&command, null);
    }
    const identity: native.Identity = .{ .id = admission.command.binding.id, .plan_digest = admission.command.binding.plan_digest, .owner_digest = admission.command.binding.owner_digest, .ordinal = 0, .namespace = options.identity_namespace.?, .owner_group_id = 11 };
    const binding = (try db.nativeIndexOwnerState(identity)).?;
    const request = try native.BuildRequest.init(identity, binding, .{ .records = 1, .time_ns = std.time.ns_per_s });
    const stream = @import("relational_index_private_claims.zig").SourceStream;
    const page_jobs = @import("relational_index_jobs.zig");
    for (0..stream.max_pending) |i| {
        const outcome = try db.stepNativeIndexOwnerBuild("items", request, .{});
        try std.testing.expectEqual(@as(u64, 4 + i), outcome.advanced.position.sequence);
    }
    var previous_head: [stream.Head.encoded_len]u8 = undefined;
    var previous_progress: []u8 = undefined;
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        previous_head = (try read.get(stream.head_key))[0..stream.Head.encoded_len].*;
        previous_progress = try alloc.dupe(u8, try read.get(&page_jobs.progressKey(binding.index)));
        const head = (try stream.load(&read, binding)).?;
        try std.testing.expectEqual(@as(u64, 64), head.pending());
        try std.testing.expectEqual(@as(u64, 64), head.emitted_rows);
        try std.testing.expectEqual(stream.State.building, head.state);
        try db_mod.DB.PreparedNativeIndexOwnerBuild.verifyRecovery(&read, binding);
    }
    defer alloc.free(previous_progress);
    // EOF would publish a ready progress record and advance the native cut.
    // Backpressure must roll both back with the rejected source batch.
    try std.testing.expectError(error.PrivateIndexClaimBacklogFull, db.stepNativeIndexOwnerBuild("items", request, .{}));
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    try std.testing.expectError(error.PrivateIndexClaimBacklogFull, db.stepNativeIndexOwnerBuild("items", request, .{}));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualSlices(u8, &previous_head, try read.get(stream.head_key));
        try std.testing.expectEqualSlices(u8, previous_progress, try read.get(&page_jobs.progressKey(binding.index)));
        try std.testing.expectEqual(@as(u64, 67), (try authority.load(&read)).?.sequence);
        try std.testing.expectError(error.NotFound, read.get(&stream.key(binding.id, 65, false)));
        try std.testing.expectError(error.NotFound, read.get(&stream.key(binding.id, 65, true)));
    }
    var cancellation = try db.prepareNativeIndexOwnerCancellation(native.Admission.fromBinding(binding));
    defer cancellation.deinit();
    _ = try db.applyPreparedNativeIndexOwnerCancellation(&cancellation, null);
    var cleanup = try db.prepareNativeIndexOwnerCleanup(identity);
    defer cleanup.deinit();
    _ = try db.applyPreparedNativeIndexOwnerCleanup(&cleanup, null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try stream.load(&read, binding) == null);
        for (1..65) |ordinal| {
            try std.testing.expectError(error.NotFound, read.get(&stream.key(binding.id, ordinal, false)));
            try std.testing.expectError(error.NotFound, read.get(&stream.key(binding.id, ordinal, true)));
        }
        try std.testing.expectError(error.NotFound, read.get(db_mod.DB.PreparedNativeIndexOwnerBuild.checkpoint_key));
        try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    }
}

test "relational index build pages oversized target claims fail durably before source emission" {
    const native = @import("relational_index_owner_control.zig");
    const catalog = @import("relational_integrity_catalog.zig");
    const candidates = @import("relational_index_candidate.zig");
    const authority = @import("../source_authority.zig");
    const resources = @import("../resource_manager.zig");
    const before = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"additionalProperties\":false}}}}";
    const after = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"additionalProperties\":false}}},\"relational_indexes\":[{\"name\":\"email_idx\",\"keys\":[{\"column\":\"email\"}]}],\"unique_constraints\":[{\"name\":\"email_idx\",\"origin\":\"index\",\"columns\":[\"email\"]}]}";
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("native-index-undeliverable-claim");
    defer directory.cleanup();
    var resource_options: resources.Options = .{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 * 1024 } };
    resource_options.budgets[@backingInt(resources.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = 16 * 1024 * 1024 };
    var manager = resources.ResourceManager.init(resource_options);
    defer manager.deinit(alloc);
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} }, .online_source_authority = .native, .identity_namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .resource_manager = &manager };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try db.setSchemaJson(alloc, before);
    var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, before);
    defer parsed.deinit(alloc);
    var prior = try @import("../../schema/relational_declarations.zig").compileCatalog(alloc, parsed, 101, null);
    defer prior.deinit();
    try db.core.store.putBatch(&.{.{ .key = catalog.key, .value = prior.value }}, &.{});
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const email = try arena.allocator().alloc(u8, 1024 * 1024 - 100);
    @memset(email, 'x');
    const writes = [_]db_mod.types.BatchWrite{.{
        .key = "wide",
        .value = try std.fmt.allocPrint(arena.allocator(), "{{\"id\":1,\"email\":\"{s}\"}}", .{email}),
    }};
    try db.batch(.{ .writes = &writes });
    var candidate = try candidates.Candidate.prepare(alloc, 101, before, after, prior.value, "email_idx");
    defer candidate.deinit();
    var admission = try db.prepareNativeIndexOwnerAdmission(.{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = options.identity_namespace.?, .catalog_digest = @import("relational_index_inputs.zig").digest(prior.value) },
        .candidate_digest = @import("relational_index_inputs.zig").digest(candidate.next.value),
        .reservation_digest = @import("relational_index_inputs.zig").digest(candidate.reservation.value),
    }, candidate.inputs.bytes);
    defer admission.deinit();
    _ = try db.applyPreparedNativeIndexOwnerControl(&admission, null);
    for ([_]@import("relational_index_owner_job.zig").NativeEffect{ .fenced, .reserved }) |effect| {
        var command = try db.prepareNativeIndexOwnerControl(.{ .binding = admission.command.binding, .effect = effect });
        defer command.deinit();
        _ = try db.applyPreparedNativeIndexOwnerControl(&command, null);
    }
    const identity: native.Identity = .{ .id = admission.command.binding.id, .plan_digest = admission.command.binding.plan_digest, .owner_digest = admission.command.binding.owner_digest, .ordinal = 0, .namespace = options.identity_namespace.?, .owner_group_id = 11 };
    const binding = (try db.nativeIndexOwnerState(identity)).?;
    const request = try native.BuildRequest.init(identity, binding, .{ .records = 1, .time_ns = std.time.ns_per_s });
    const advanced = try db.stepNativeIndexOwnerBuild("items", request, .{});
    try std.testing.expectEqual(@as(u64, 4), advanced.advanced.position.sequence);
    const complete = try db.stepNativeIndexOwnerBuild("items", request, .{});
    try std.testing.expectEqual(.failed, complete.complete.state);
    try std.testing.expectEqual(@import("relational_index_jobs.zig").Failure.key_too_large, complete.complete.failure);
    try std.testing.expectEqual(@as(u64, 0), complete.complete.rows_scanned);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const stream = @import("relational_index_private_claims.zig").SourceStream;
        const head = (try stream.load(&read, binding)).?;
        try std.testing.expectEqual(stream.State.failed, head.state);
        try std.testing.expectEqual(@as(u64, 0), head.emitted_rows);
        var batch = (try native.PreparedBuild.peekClaimSource(alloc, &read, binding)).?;
        defer batch.deinit();
        try std.testing.expectEqual(@as(usize, 0), batch.rows().len);
        var cursor = try read.openCursor();
        defer cursor.close();
        const prefix = try records.forwardPrefix(binding.index);
        if (try cursor.seekAtOrAfter(&prefix)) |entry|
            try std.testing.expect(!std.mem.startsWith(u8, entry.key, &prefix));
    }
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    const recovered = try db.stepNativeIndexOwnerBuild("items", request, .{});
    try std.testing.expectEqual(complete.complete.position, recovered.complete.position);
    try std.testing.expectEqual(.failed, recovered.complete.state);
    try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 4), (try authority.load(&read)).?.sequence);
}

test "relational index system SQL precise schema publication upgrades catalog atomically and survives LSM reopen" {
    const storage_schema = @import("../schema.zig");
    const catalog = @import("table_catalog.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("sql-precise-type-reopen");
    defer directory.cleanup();
    {
        var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        // A deployed document owner with no schema requires only format 15.
        // Seed the durable fixture; do not introduce an upgrade write on open.
        var previous = db.core.table_catalog;
        previous.schema_format_version = 15;
        const bytes = previous.encode();
        var transaction = try db.core.store.beginWriteTxn();
        errdefer transaction.abort();
        try transaction.put(catalog.key, &bytes);
        try transaction.commit();
    }
    {
        var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try std.testing.expectEqual(@as(u32, 15), db.core.table_catalog.schema_format_version);
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 }} });
        try std.testing.expectEqual(storage_schema.storage_format_version, db.core.table_catalog.schema_format_version);
        try db.batch(.{ .writes = &.{.{ .key = "valid", .value = "{\"n\":32767}" }} });
        try std.testing.expectError(error.InvalidRelationalRow, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = "{\"n\":1}" }, .{ .key = "invalid", .value = "{\"n\":32768}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try std.testing.expect((try db.get(alloc, "invalid")) == null);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer reopened.close();
    try std.testing.expectEqual(storage_schema.storage_format_version, reopened.core.table_catalog.schema_format_version);
    try std.testing.expectEqual(@import("../../common/sql_builtin_type.zig").Type.int16, reopened.core.schema.?.relational_columns[0].sql_element_type.?);
    const bytes = (try reopened.get(alloc, "valid")).?;
    defer alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 32767), parsed.value.object.get("n").?.integer);
}

test "relational index system SQL typed arrays survive LSM reopen and portable restore" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const storage_schema = @import("../schema.zig");
    const wire = @import("../../sql/array_wire.zig");
    var directory = try TestDirectory.init("sql-stored-array-reopen");
    defer directory.cleanup();
    var target_directory = try TestDirectory.init("sql-stored-array-portable");
    defer target_directory.cleanup();
    const array_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_array","x-antfly-sql-type":"int64","nullable":true},"j":{"type":"sql_array","x-antfly-sql-type":"jsonb","nullable":true},"f":{"type":"sql_array","x-antfly-sql-type":"float32","nullable":true}},"additionalProperties":false}}}}
    ;
    const json =
        \\{"a":{"dimensions":[{"length":2,"lower_bound":-3}],"values":["9007199254740993",null],"sql_nulls":[false,true]},"j":{"dimensions":[{"length":2,"lower_bound":1}],"values":[null,null],"sql_nulls":[false,true]},"f":{"dimensions":[{"length":1,"lower_bound":1}],"values":[0.1],"sql_nulls":[false]}}
    ;
    var archive = std.ArrayListUnmanaged(u8).empty;
    defer archive.deinit(alloc);
    {
        var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
        defer db.close();
        try db.setSchemaJson(alloc, array_schema);
        try db.batch(.{ .writes = &.{ .{ .key = "row", .value = json }, .{ .key = "null", .value = "{\"a\":null,\"j\":null}" }, .{ .key = "empty", .value = "{\"a\":{\"dimensions\":[],\"values\":[],\"sql_nulls\":[]}}" } } });
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = json }, .{ .key = "invalid", .value = "{\"a\":{\"dimensions\":[{\"length\":1,\"lower_bound\":1}],\"values\":[9007199254740993],\"sql_nulls\":[false]}}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{.{ .key = "invalid-json-null", .value = "{\"j\":null}", .json_null_fields = &.{"j"} }} }));
        var changed = try std.json.parseFromSlice(std.json.Value, alloc, array_schema, .{});
        defer changed.deinit();
        changed.value.object.getPtr("version").?.* = .{ .integer = 2 };
        changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.getPtr("a").?.object.getPtr("x-antfly-sql-type").?.* = .{ .string = "int32" };
        const reinterpreted = try std.json.Stringify.valueAlloc(alloc, changed.value, .{});
        defer alloc.free(reinterpreted);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.setSchemaJson(alloc, reinterpreted));
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target_directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expectEqual(storage_schema.storage_format_version, db.core.table_catalog.schema_format_version);
        try std.testing.expectEqual(storage_schema.RelationalColumnType.sql_array, db.core.schema.?.relational_columns[0].column_type);
        try std.testing.expect(db.core.schema.?.requires_public_schema);
        const bytes = (try db.get(alloc, "row")).?;
        defer alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{ .parse_numbers = false });
        defer parsed.deinit();
        var a = try wire.decode(alloc, .int64, parsed.value.object.get("a").?, .{});
        defer a.deinit();
        try std.testing.expectEqual(@as(i64, 9007199254740993), a.value.elements[0].value.integer);
        try std.testing.expectEqual(@as(i32, -3), a.value.dimensions[0].lower);
        try std.testing.expect(a.value.elements[1].sql_null);
        var j = try wire.decode(alloc, .jsonb, parsed.value.object.get("j").?, .{});
        defer j.deinit();
        try std.testing.expect(j.value.elements[0].value == .null and !j.value.elements[0].sql_null);
        try std.testing.expect(j.value.elements[1].sql_null);
        var f = try wire.decode(alloc, .float32, parsed.value.object.get("f").?, .{});
        defer f.deinit();
        try std.testing.expectEqual(@as(f64, @as(f32, 0.1)), f.value.elements[0].value.float);
        const empty = (try db.get(alloc, "empty")).?;
        defer alloc.free(empty);
        const null_row = (try db.get(alloc, "null")).?;
        defer alloc.free(null_row);
        try std.testing.expect(!std.mem.eql(u8, empty, null_row));
    }
}

test "relational index system wide logical CHECKs survive LSM reopen and portable restore" {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("wide-checks-lsm");
    defer directory.cleanup();
    var target = try @import("../../common/test_directory.zig").TestDirectory.init("wide-checks-restore");
    defer target.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    const payload = try a.alloc(u8, 600 * 1024);
    @memset(payload, 0);
    const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(payload.len));
    _ = std.base64.standard.Encoder.encode(encoded, payload);
    const text = try a.alloc(u8, 1024 * 1024 + 1);
    @memset(text, 'x');
    const schema_json = try std.json.Stringify.valueAlloc(a, .{
        .version = 1,
        .storage_mode = "relational",
        .default_type = "row",
        .checks = .{
            .{ .name = "same", .column = "b", .op = "eq", .value = encoded },
            .{ .name = "text", .column = "s", .op = "gt", .value = "" },
        },
        .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = .{ .b = .{ .type = "blob" }, .s = .{ .type = "keyword" } }, .additionalProperties = false } } },
    }, .{});
    const valid = try std.json.Stringify.valueAlloc(a, .{ .b = encoded, .s = text }, .{});
    const invalid = try std.json.Stringify.valueAlloc(a, .{ .b = encoded, .s = "" }, .{});
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    {
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc, schema_json);
        try db.batch(.{ .writes = &.{.{ .key = "row", .value = valid }} });
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = valid }, .{ .key = "invalid", .value = invalid } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        const bytes = (try db.get(alloc, "row")).?;
        defer alloc.free(bytes);
        var document = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
        defer document.deinit();
        try std.testing.expectEqualStrings(encoded, document.value.object.get("b").?.string);
        try std.testing.expectEqualStrings(text, document.value.object.get("s").?.string);
    }
}

test "relational index system public NUMERIC modifiers normalize before indexing and survive LSM restore" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("numeric-modifier-lsm");
    defer directory.cleanup();
    var target_directory = try TestDirectory.init("numeric-modifier-lsm-restore");
    defer target_directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_n","keys":[{"column":"n"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"number","x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}},"a":{"type":"sql_array","x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}}},"additionalProperties":false}}}}
    ;
    const json =
        \\{"n":1.245,"a":{"dimensions":[{"length":2,"lower_bound":-7}],"values":["1.245",null],"sql_nulls":[false,true]}}
    ;
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    {
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc, schema_json);
        try db.batch(.{ .writes = &.{.{ .key = "row", .value = json }} });
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = json }, .{ .key = "invalid", .value = "{\"n\":99.995}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        var changed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
        defer changed.deinit();
        changed.value.object.getPtr("version").?.* = .{ .integer = 2 };
        changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.getPtr("n").?.object.getPtr("x-antfly-sql-numeric-modifier").?.object.getPtr("scale").?.* = .{ .integer = 1 };
        const reinterpreted = try std.json.Stringify.valueAlloc(alloc, changed.value, .{});
        defer alloc.free(reinterpreted);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.setSchemaJson(alloc, reinterpreted));
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target_directory.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_numeric_modifiers);
        const row = (try db.get(alloc, "row")).?;
        defer alloc.free(row);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row, .{ .parse_numbers = false });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("1.25", parsed.value.object.get("n").?.number_string);
        const array = parsed.value.object.get("a").?.object;
        try std.testing.expectEqualStrings("1.25", array.get("values").?.array.items[0].string);
        try std.testing.expect(array.get("sql_nulls").?.array.items[1].bool);
        _ = try readyIndex(db, "by_n");
        const bound = try @import("../../sql/numeric_storage.zig").encodeJsonAlloc(alloc, .{ .string = "1.25" });
        defer alloc.free(bound);
        var reader = try db.beginRelationalRows(alloc, .{
            .index = "by_n",
            .fields = &.{"n"},
            .lower = .{ .values = &.{.{ .numeric = bound }} },
            .upper = .{ .values = &.{.{ .numeric = bound }} },
        });
        defer reader.deinit();
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 10, .records = 100 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqualStrings("row", page.rows[0].key);
    }
}

test "relational index system NUMERIC public array schema survives LSM reopen and portable restore" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("numeric-public-array");
    defer directory.cleanup();
    var target_directory = try TestDirectory.init("numeric-public-array-restore");
    defer target_directory.cleanup();
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    {
        var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"sql_array","x-antfly-sql-type":"numeric","nullable":true}},"additionalProperties":false}}}}
        );
        try db.batch(.{ .writes = &.{.{ .key = "row", .value =
            \\{"n":{"dimensions":[{"length":3,"lower_bound":-7}],"values":["9007199254740993.1200","NaN",null],"sql_nulls":[false,false,true]}}
        }} });
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target_directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expectEqual(@import("../schema.zig").storage_format_version, db.core.table_catalog.schema_format_version);
        try std.testing.expectEqual(@import("../../common/sql_builtin_type.zig").Type.numeric, db.core.schema.?.relational_columns[0].sql_element_type.?);
        const bytes = (try db.get(alloc, "row")).?;
        defer alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
        defer parsed.deinit();
        var array = try @import("../../sql/array_wire.zig").decode(alloc, .numeric, parsed.value.object.get("n").?, .{});
        defer array.deinit();
        try std.testing.expectEqual(@as(i32, -7), array.value.dimensions[0].lower);
        try std.testing.expectEqual(@as(u16, 4), array.value.elements[0].numeric.?.scale);
        try std.testing.expectEqual(@import("../../sql/numeric_value.zig").Kind.nan, array.value.elements[1].numeric.?.kind);
        try std.testing.expect(array.value.elements[2].sql_null);
        var context: @import("../../sql/numeric_value.zig").Context = .{ .alloc = alloc };
        const text = try @import("../../sql/numeric_value.zig").format(&context, array.value.elements[0].numeric.?.*);
        defer alloc.free(text);
        try std.testing.expectEqualStrings("9007199254740993.1200", text);
    }
}

test "relational index system SQL public scalar schema survives LSM reopen and rejects domain reinterpretation" {
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","x-antfly-sql-type":"int16"},"f":{"type":"number","x-antfly-sql-type":"float32"}},"additionalProperties":false}}}}
    ;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("sql-public-scalar-reopen");
    defer directory.cleanup();
    {
        var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
        defer db.close();
        try db.setSchemaJson(alloc, json);
        try db.batch(.{ .writes = &.{.{ .key = "valid", .value = "{\"n\":32767,\"f\":0.1}" }} });
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = "{\"n\":1}" }, .{ .key = "overflow", .value = "{\"n\":32768}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        var changed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer changed.deinit();
        changed.value.object.getPtr("version").?.* = .{ .integer = 2 };
        const property = changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.getPtr("n").?;
        property.object.getPtr("x-antfly-sql-type").?.* = .{ .string = "int32" };
        const widened = try std.json.Stringify.valueAlloc(alloc, changed.value, .{});
        defer alloc.free(widened);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.setSchemaJson(alloc, widened));
        try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer reopened.close();
    try std.testing.expectEqual(@import("../../common/sql_builtin_type.zig").Type.int16, reopened.core.schema.?.relational_columns[0].sql_element_type.?);
    const bytes = (try reopened.get(alloc, "valid")).?;
    defer alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 32767), parsed.value.object.get("n").?.integer);
    try std.testing.expectEqual(@as(f64, @as(f32, 0.1)), parsed.value.object.get("f").?.float);
    try std.testing.expectError(error.InvalidBatchRequest, reopened.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"n\":-32769}" }} }));
}

test "relational index system statement fence never waits on partial prepared transactions" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("statement-fence");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    var first = (try db.tryStatementReadFence()).?;
    defer first.release();
    try std.testing.expect((try db.tryStatementReadFence()) == null);
    first.release();
    const transaction = try db.beginTransaction(1);
    try db.writeIntents(transaction, &.{.{ .key = "pending", .value = "{\"n\":1}" }}, &.{});
    try std.testing.expect((try db.tryStatementReadFence()) == null);
    // A busy response releases both gates, allowing this participant (and a
    // coordinator holding other participants) to finish its durable decision.
    try std.testing.expect(db.core.snapshot_admission.lock.tryLockShared());
    db.core.snapshot_admission.lock.unlockShared();
    try std.testing.expect(db.core.snapshot_replay_admission.lock.tryLockShared());
    db.core.snapshot_replay_admission.lock.unlockShared();
    try db.abortTransaction(transaction, 2);
    var after_resolution = (try db.tryStatementReadFence()).?;
    defer after_resolution.release();
}

test "relational index system range activation rejects pending writers and captures empty snapshot guards" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("sql-range-activation");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const options: db_mod.types.ScanOptions = .{ .include_range_proofs = true, .relational_query = .{ .fields = &.{"_id"} } };
    try std.testing.expectError(error.SqlRangeTrackingRequired, db.openDocumentReadSession(alloc, "a", "az", options));
    const pending = try db.beginTransaction(1);
    try db.writeIntents(pending, &.{.{ .key = "alpha", .value = "{}" }}, &.{});
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .activate_range_tracking = true }));
    try db.abortTransaction(pending, 2);
    try db.batch(.{ .activate_range_tracking = true });
    const before = try db.openDocumentReadSession(alloc, "a", "az", options);
    defer before.deinit();
    const empty_proofs = try before.rangeProofs(alloc);
    defer alloc.free(empty_proofs);
    try std.testing.expectEqual(@as(usize, 1), empty_proofs.len);
    try std.testing.expectEqual(null, empty_proofs[0].generation);
    try db.batch(.{ .writes = &.{.{ .key = "alpha", .value = "{}" }} });
    const after = try db.openDocumentReadSession(alloc, "a", "az", options);
    defer after.deinit();
    const changed = try after.rangeProofs(alloc);
    defer alloc.free(changed);
    try std.testing.expectEqual(@as(?u64, 1), changed[0].generation);
    const pinned = try before.rangeProofs(alloc);
    defer alloc.free(pinned);
    try std.testing.expectEqual(null, pinned[0].generation);
    const guarded = try db.beginTransaction(3);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(guarded, .{ .range_guards = empty_proofs }));
    try db.writeTransaction(guarded, .{ .range_guards = changed });
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "another", .value = "{}" }} }));
    try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .range_guards = changed }));
    try db.abortTransaction(guarded, 4);
    try db.batch(.{ .writes = &.{.{ .key = "another", .value = "{}" }} });
    try db.batch(.{ .activate_range_tracking = true });
}

test "relational index system range activation and counters survive owner reopen" {
    const protection = @import("../range_protection.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("sql-range-activation-reopen");
    defer directory.cleanup();
    {
        var original = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        defer original.close();
        try original.batch(.{ .activate_range_tracking = true });
        try original.batch(.{ .writes = &.{.{ .key = "alpha", .value = "{}" }} });
    }
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try protection.isActive(&read));
        try std.testing.expectEqual(@as(?u64, 1), try protection.generation(&read, protection.bucket("alpha")));
    }
    const options: db_mod.types.ScanOptions = .{ .include_range_proofs = true, .relational_query = .{ .fields = &.{"_id"} } };
    const snapshot = try db.openDocumentReadSession(alloc, "a", "az", options);
    defer snapshot.deinit();
    const proofs = try snapshot.rangeProofs(alloc);
    defer alloc.free(proofs);
    try std.testing.expectEqual(@as(usize, 1), proofs.len);
    try std.testing.expectEqual(@as(?u64, 1), proofs[0].generation);
    try db.batch(.{ .writes = &.{.{ .key = "another", .value = "{}" }} });
    const guarded = try db.beginTransaction(3);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(guarded, .{ .range_guards = proofs }));
    try db.abortTransaction(guarded, 4);
}

test "relational index system ordinary tuple replacement respects a prepared index span reader" {
    const protection = @import("../range_protection.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-span-ordinary-conflict");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"label_idx","keys":[{"column":"label"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .activate_range_tracking = true });
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"before\"}" }} });
    _ = try readyIndex(&db, "label_idx");
    const span = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const entry = (try cursor.seekAtOrAfter(records.forward_namespace)) orelse return error.MissingRelationalIndexForwardKey;
        if (!records.isForwardKey(entry.key)) return error.MissingRelationalIndexForwardKey;
        break :blk (try protection.indexSpanDigest(entry.key)).?;
    };
    const counter = protection.indexCounterKey(span);
    try std.testing.expectError(error.InvalidIntegrityOperation, db.batch(.{ .writes = &.{.{ .key = &counter, .value = "forged" }} }));
    var counter_value: [8]u8 = undefined;
    var initial_generation: u64 = undefined;
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        initial_generation = (try protection.indexGeneration(&read, span)).?;
        std.mem.writeInt(u64, &counter_value, initial_generation, .little);
    }
    const reader = try db.beginTransaction(2);
    try db.writeIntents(reader, &.{}, &.{.{ .key = &counter, .expected_version = 0, .comparison = .exact_value, .expected_value = &counter_value }});
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"after\"}" }} }));
    const unchanged = (try db.get(alloc, "row")).?;
    defer alloc.free(unchanged);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, unchanged, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("before", parsed.value.object.get("label").?.string);
    try db.abortTransaction(reader, 3);
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"after\"}" }} });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(?u64, initial_generation + 1), try protection.indexGeneration(&read, span));
}

test "relational index system full-key reads guard exact READY tuple including misses" {
    const protection = @import("../range_protection.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-exact-read-proof");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"label_idx","keys":[{"column":"label"}]},{"name":"label_tenant_idx","keys":[{"column":"label"},{"column":"tenant"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"},"tenant":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .activate_range_tracking = true });
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"before\"}" }} });
    _ = try readyIndex(&db, "label_idx");
    _ = try readyIndex(&db, "label_tenant_idx");
    const options: db_mod.types.ScanOptions = .{
        .include_range_proofs = true,
        .relational_query_json = "{\"fields\":[\"label\"],\"index\":\"label_idx\",\"schema_version\":1,\"lower\":{\"values\":[\"after\"]},\"upper\":{\"values\":[\"after\"]}}",
    };
    const snapshot = try db.openRelationalReadSession(alloc, "", "", options);
    defer snapshot.deinit();
    const proofs = try snapshot.rangeProofs(alloc);
    defer alloc.free(proofs);
    try std.testing.expectEqual(@as(usize, 1), proofs.len);
    try std.testing.expectEqual(protection.index_bucket_sentinel, proofs[0].bucket);
    try std.testing.expectEqual(@as(?u64, null), proofs[0].generation);
    const ranged = try db.openRelationalReadSession(alloc, "", "", .{
        .include_range_proofs = true,
        .relational_query_json = "{\"fields\":[\"label\"],\"index\":\"label_idx\",\"schema_version\":1,\"lower\":{\"values\":[\"after\"]}}",
    });
    defer ranged.deinit();
    const conservative = try ranged.rangeProofs(alloc);
    defer alloc.free(conservative);
    try std.testing.expectEqual(@as(usize, protection.bucket_count), conservative.len);
    try std.testing.expect(conservative[0].index == null);
    const reader = try db.beginTransaction(2);
    try db.writeTransaction(reader, .{ .range_guards = proofs });
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"label\":\"after\"}" }} }));
    try db.abortTransaction(reader, 3);
    try db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"label\":\"after\"}" }} });
    const stale = try db.beginTransaction(4);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(stale, .{ .range_guards = proofs }));
    try db.abortTransaction(stale, 5);
    const compound = try db.openRelationalReadSession(alloc, "", "", .{
        .include_range_proofs = true,
        .relational_query_json = "{\"fields\":[\"label\"],\"index\":\"label_tenant_idx\",\"schema_version\":1,\"lower\":{\"values\":[\"joint\",7]},\"upper\":{\"values\":[\"joint\",7]}}",
    });
    defer compound.deinit();
    const compound_proofs = try compound.rangeProofs(alloc);
    defer alloc.free(compound_proofs);
    try std.testing.expectEqual(@as(usize, 1), compound_proofs.len);
    try std.testing.expectEqual(protection.index_bucket_sentinel, compound_proofs[0].bucket);
    const compound_reader = try db.beginTransaction(6);
    try db.writeTransaction(compound_reader, .{ .range_guards = compound_proofs });
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "composite", .value = "{\"label\":\"joint\",\"tenant\":7}" }} }));
    try db.abortTransaction(compound_reader, 7);
}

test "relational index system automatic expression points protect phantoms without unrelated write conflicts" {
    const protection = @import("../range_protection.zig");
    const Input = db_mod.types.RelationalRowQuery;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("automatic-expression-point-proof");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    var db_open = true;
    defer if (db_open) db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"mixed","keys":[{"column":"tenant","direction":"desc"},{"expression":{"op":"lower_ascii","args":[{"op":"column","column":"email"}]},"result_type":"string","nulls":"first"}],"include_columns":["extra"],"where":[{"column":"active","op":"eq","value":true}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"email":{"type":"keyword"},"active":{"type":"boolean"},"extra":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .activate_range_tracking = true });
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"tenant\":7,\"email\":\"Owner\",\"active\":true,\"extra\":\"before\"}" },
        .{ .key = "b", .value = "{\"tenant\":8,\"email\":\"Other\",\"active\":true,\"extra\":\"before\"}" },
    } });
    const identity = blk: {
        var pinned = db.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const expression = pinned.plan.boundIndexes()[0].tuple.keys[1].expression.?;
        break :blk @import("../relational_index.zig").ExpressionKeyIdentity{ .fingerprint = expression.plan.fingerprint, .kind = .string, .builtin = expression.plan.result_sql_type };
    };
    var hints = [_]Input.ExpressionEquality{.{ .identity = identity, .value = .{ .string = "owner" } }};
    const query: Input = .{ .auto_index = true, .schema_version = 1, .fields = &.{ "email", "extra" }, .expression_equalities = &hints, .conditions = &.{
        .{ .column = "tenant", .op = .eq, .value = .{ .integer = 7 } },
        .{ .column = "active", .op = .eq, .value = .{ .bool = true } },
    } };
    const Check = struct {
        fn proofs(database: *db_mod.DB, input: Input, exact: bool) ![]protection.Proof {
            const session = try database.openRelationalReadSession(alloc, "", "", .{ .include_range_proofs = true, .relational_query = input });
            defer session.deinit();
            try std.testing.expectEqual(exact, session.reader.exact_index_tuple != null);
            const captured = try session.rangeProofs(alloc);
            errdefer alloc.free(captured);
            try std.testing.expectEqual(@as(usize, if (exact) 1 else protection.bucket_count), captured.len);
            try std.testing.expectEqual(exact, captured[0].index != null);
            return captured;
        }
    };
    const unready = try Check.proofs(&db, query, false);
    alloc.free(unready);
    _ = try readyIndex(&db, "mixed");
    const point = try Check.proofs(&db, query, true);
    defer alloc.free(point);
    var explicit = query;
    explicit.auto_index = false;
    explicit.index = "mixed";
    explicit.expression_equalities = &.{};
    explicit.lower = .{ .values = &.{ .{ .integer = 7 }, .{ .string = "owner" } } };
    explicit.upper = explicit.lower;
    const equivalent = try Check.proofs(&db, explicit, true);
    defer alloc.free(equivalent);
    try std.testing.expectEqualDeep(point, equivalent);
    // Even nextPrefix-shaped bounds are not a point without full key arity.
    explicit.lower = .{ .values = &.{.{ .integer = 7 }} };
    explicit.upper = explicit.lower;
    const prefix = try Check.proofs(&db, explicit, false);
    alloc.free(prefix);
    explicit.lower = .{ .values = &.{ .{ .integer = 7 }, .{ .string = "owner" } } };
    explicit.upper = .{ .values = &.{ .{ .integer = 7 }, .{ .string = "zulu" } } };
    const ranged = try Check.proofs(&db, explicit, false);
    alloc.free(ranged);
    var broad = query;
    broad.expression_equalities = &.{};
    const automatic_prefix = try Check.proofs(&db, broad, false);
    alloc.free(automatic_prefix);

    const reader = try db.beginTransaction(2);
    try db.writeTransaction(reader, .{ .range_guards = point });
    // The previous primary-range fallback blocked this unrelated tuple too.
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"tenant\":8,\"email\":\"Other\",\"active\":true,\"extra\":\"unrelated\"}" }} });
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"tenant\":7,\"email\":\"OWNER\",\"active\":true}" }} }));
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":7,\"email\":\"Owner\",\"active\":true,\"extra\":\"changed\"}" }} }));
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"tenant\":7,\"email\":\"Owner\",\"active\":true}" }} }));
    try db.abortTransaction(reader, 3);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":7,\"email\":\"Owner\",\"active\":true,\"extra\":\"changed\"}" }} });
    const stale = try db.beginTransaction(4);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(stale, .{ .range_guards = point }));
    try db.abortTransaction(stale, 5);

    hints[0].value = .{ .string = "absent" };
    const empty = try Check.proofs(&db, query, true);
    defer alloc.free(empty);
    try std.testing.expect(empty[0].generation == null);
    const missing_reader = try db.beginTransaction(6);
    try db.writeTransaction(missing_reader, .{ .range_guards = empty });
    try db.batch(.{ .writes = &.{.{ .key = "inactive", .value = "{\"tenant\":7,\"email\":\"ABSENT\",\"active\":false}" }} });
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .writes = &.{.{ .key = "phantom", .value = "{\"tenant\":7,\"email\":\"ABSENT\",\"active\":true}" }} }));
    try db.abortTransaction(missing_reader, 7);
    try db.batch(.{ .writes = &.{.{ .key = "phantom", .value = "{\"tenant\":7,\"email\":\"ABSENT\",\"active\":true}" }} });
    const missing_stale = try db.beginTransaction(8);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(missing_stale, .{ .range_guards = empty }));
    try db.abortTransaction(missing_stale, 9);
    const before_restart = try Check.proofs(&db, query, true);
    defer alloc.free(before_restart);
    db.close();
    db_open = false;
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    db_open = true;
    const after_restart = try Check.proofs(&db, query, true);
    defer alloc.free(after_restart);
    try std.testing.expectEqualDeep(before_restart, after_restart);
    const restart_stale = try db.beginTransaction(10);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(restart_stale, .{ .range_guards = empty }));
    try db.abortTransaction(restart_stale, 11);
    std.debug.print("automatic expression point proof: conservative_scopes={} selected_scopes=1 unrelated_write=allowed phantom=blocked cold_counter=preserved\n", .{protection.bucket_count});
}

test "relational index system exact tuple proof sees non-indexed row updates" {
    const protection = @import("../range_protection.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-exact-row-update");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"label_idx","keys":[{"column":"label"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"},"extra":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .activate_range_tracking = true });
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"same\",\"extra\":\"before\"}" }} });
    _ = try readyIndex(&db, "label_idx");
    const snapshot = try db.openRelationalReadSession(alloc, "", "", .{
        .include_range_proofs = true,
        .relational_query_json = "{\"fields\":[\"label\",\"extra\"],\"index\":\"label_idx\",\"schema_version\":1,\"lower\":{\"values\":[\"same\"]},\"upper\":{\"values\":[\"same\"]}}",
    });
    defer snapshot.deinit();
    const proofs = try snapshot.rangeProofs(alloc);
    defer alloc.free(proofs);
    try std.testing.expectEqual(@as(usize, 1), proofs.len);
    try std.testing.expectEqual(protection.index_bucket_sentinel, proofs[0].bucket);
    const prior = proofs[0].generation;
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"same\",\"extra\":\"after\"}" }} });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(?u64, prior.? + 1), try protection.indexGeneration(&read, proofs[0].index.?.digest));
    const transaction = try db.beginTransaction(2);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(transaction, .{ .range_guards = proofs }));
    try db.abortTransaction(transaction, 3);
    const current = try db.openRelationalReadSession(alloc, "", "", .{
        .include_range_proofs = true,
        .relational_query_json = "{\"fields\":[\"label\",\"extra\"],\"index\":\"label_idx\",\"schema_version\":1,\"lower\":{\"values\":[\"same\"]},\"upper\":{\"values\":[\"same\"]}}",
    });
    defer current.deinit();
    const current_proofs = try current.rangeProofs(alloc);
    defer alloc.free(current_proofs);
    const writer = try db.beginTransaction(4);
    try db.writeIntents(writer, &.{.{ .key = "row", .value = "{\"label\":\"same\",\"extra\":\"committed\"}" }}, &.{});
    try db.commitTransaction(writer, 5);
    const stale_transaction = try db.beginTransaction(6);
    try std.testing.expectError(error.VersionConflict, db.writeTransaction(stale_transaction, .{ .range_guards = current_proofs }));
    try db.abortTransaction(stale_transaction, 7);
}

test "relational index system prepared writer reserves prior and candidate tuples" {
    const protection = @import("../range_protection.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-span-prepared-writer");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"label_idx","keys":[{"column":"label"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .activate_range_tracking = true });
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"label\":\"before\"}" }} });
    _ = try readyIndex(&db, "label_idx");
    const old_span = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const entry = (try cursor.seekAtOrAfter(records.forward_namespace)) orelse return error.MissingRelationalIndexForwardKey;
        break :blk (try protection.indexSpanDigest(entry.key)).?;
    };
    try db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"label\":\"after\"}" }} });
    const new_span = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(records.forward_namespace);
        while (entry) |record| : (entry = try cursor.next()) {
            if (!records.isForwardKey(record.key)) break;
            const span = (try protection.indexSpanDigest(record.key)).?;
            if (!std.mem.eql(u8, &span, &old_span)) break :blk span;
        }
        return error.MissingRelationalIndexForwardKey;
    };
    try db.batch(.{ .deletes = &.{"seed"} });
    var old_value: [8]u8 = undefined;
    var new_value: [8]u8 = undefined;
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        std.mem.writeInt(u64, &old_value, (try protection.indexGeneration(&read, old_span)).?, .little);
        std.mem.writeInt(u64, &new_value, (try protection.indexGeneration(&read, new_span)).?, .little);
    }
    const writer = try db.beginTransaction(2);
    try db.writeIntents(writer, &.{.{ .key = "row", .value = "{\"label\":\"after\"}" }}, &.{});
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    const reader = try db.beginTransaction(3);
    const old_counter = protection.indexCounterKey(old_span);
    const new_counter = protection.indexCounterKey(new_span);
    for ([_]@import("../transactions.zig").VersionPredicate{
        .{ .key = &old_counter, .expected_version = 0, .comparison = .exact_value, .expected_value = &old_value },
        .{ .key = &new_counter, .expected_version = 0, .comparison = .exact_value, .expected_value = &new_value },
    }) |proof| try std.testing.expectError(error.IntentConflict, db.writeIntents(reader, &.{}, &.{proof}));
    try db.abortTransaction(writer, 4);
    try db.writeIntents(reader, &.{}, &.{.{ .key = &old_counter, .expected_version = 0, .comparison = .exact_value, .expected_value = &old_value }});
    try db.abortTransaction(reader, 5);
}

test "relational index system ready publication waits for pre-generation transactions" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-span-ready-drain");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"label_idx","keys":[{"column":"label"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .activate_range_tracking = true });
    const pending = try db.beginTransaction(1);
    try db.writeIntents(pending, &.{.{ .key = "row", .value = "{\"label\":\"pending\"}" }}, &.{});
    var deferred = false;
    for (0..2048) |_| {
        _ = db.runRelationalIndexMaintenancePass() catch |err| switch (err) {
            error.IntentConflict => {
                deferred = true;
                break;
            },
            else => return err,
        };
    }
    try std.testing.expect(deferred);
    try std.testing.expectEqual(@import("relational_index_jobs.zig").State.building, (try db.relationalIndexBuildStatus("label_idx")).state);
    try db.abortTransaction(pending, 2);
    const idle = try db.beginTransaction(3);
    for (0..2048) |_| {
        if ((try db.relationalIndexBuildStatus("label_idx")).state == .ready) break;
        try db.buildRelationalIndexStep("label_idx", .{});
    }
    try std.testing.expectEqual(@import("relational_index_jobs.zig").State.ready, (try db.relationalIndexBuildStatus("label_idx")).state);
    try db.abortTransaction(idle, 4);
}

test "relational index system document SQL retains snapshot and projected null semantics" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("document-sql-snapshot");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"j":{}},"additionalProperties":true}}}}
    );
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"n\":9007199254740993,\"j\":null}" },
        .{ .key = "b", .value = "{\"n\":2}" },
    } });
    const session = try db.openDocumentReadSession(alloc, "", "", .{ .limit = 1, .relational_query = .{ .fields = &.{ "n", "j" }, .schema_version = 1 } });
    defer session.deinit();
    var first = try session.next(alloc, 1);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 1), first.rows.len);
    try std.testing.expectEqualStrings("a", first.rows[0].id);
    try std.testing.expectEqual(@as(i64, 9007199254740993), first.rows[0].value.object.get("n").?.integer);
    try std.testing.expect(!first.rows[0].sql_nulls.?[1]);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"n\":999,\"j\":{}}" }} });
    var second = try session.next(alloc, 1);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.rows.len);
    try std.testing.expectEqualStrings("b", second.rows[0].id);
    try std.testing.expectEqual(@as(i64, 2), second.rows[0].value.object.get("n").?.integer);
    try std.testing.expect(second.rows[0].sql_nulls.?[1]);
}

test "relational index system common-cut views release writers before scanning and survive later publication" {
    var first_dir = try @import("../../common/test_directory.zig").TestDirectory.init("statement-cut-first");
    defer first_dir.cleanup();
    var second_dir = try @import("../../common/test_directory.zig").TestDirectory.init("statement-cut-second");
    defer second_dir.cleanup();
    var first = try db_mod.DB.open(alloc, first_dir.path(), .{ .start_optional_runtimes = false });
    defer first.close();
    var second = try db_mod.DB.open(alloc, second_dir.path(), .{ .start_optional_runtimes = false });
    defer second.close();
    const schema_json = try schema(1, false);
    defer alloc.free(schema_json);
    for ([_]*db_mod.DB{ &first, &second }) |db| {
        try db.setSchemaJson(alloc, schema_json);
        try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"id\":1}" }} });
    }
    var first_fence = (try first.tryStatementReadFence()).?;
    defer first_fence.release();
    var second_fence = (try second.tryStatementReadFence()).?;
    defer second_fence.release();
    const opts: db_mod.types.ScanOptions = .{ .relational_query = .{ .fields = &.{"id"}, .schema_version = 1 } };
    const first_view = try first.openRelationalReadSession(alloc, "", "", opts);
    defer first_view.deinit();
    const alias_view = try first.openRelationalReadSession(alloc, "", "", opts);
    defer alias_view.deinit();
    const second_view = try second.openRelationalReadSession(alloc, "", "", opts);
    defer second_view.deinit();
    second_fence.release();
    first_fence.release();
    for ([_]*db_mod.DB{ &first, &second }) |db| try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"id\":2}" }} });
    for ([_]*db_mod.DB.RelationalReadSession{ first_view, alias_view, second_view }) |view| {
        var page = try view.nextTypedPage(alloc, null, .{ .rows = 1 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqual(@as(i64, 1), page.rows[0].typed.?.object.get("id").?.integer);
    }
}

test "relational index system session normalization shares defaults generated values and immutable schema" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("sql-normalize");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const row_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"b","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"a"},{"op":"column","column":"b"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"},"total":{"type":"integer"}},"required":["a","b","total"],"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, row_schema);
    const session = try db.openRelationalReadSession(alloc, "", "", .{ .relational_query_json = "{\"fields\":[\"a\",\"b\",\"total\"],\"schema_version\":1}", .limit = 16 });
    defer session.deinit();
    const normalized = try session.normalizeRows(alloc, &.{.{ .key = "row", .value = "{\"a\":9007199254740993}" }});
    defer {
        for (normalized) |prepared_write| {
            alloc.free(prepared_write.key);
            alloc.free(prepared_write.value);
        }
        alloc.free(normalized);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, normalized[0].value, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("b").?.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740995), parsed.value.object.get("total").?.integer);
    var empty = try session.nextTypedPage(alloc, null, .{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    // Native commit uses exactly the same preparation rules; supplied,
    // server-normalized stored-generated values are revalidated consistently.
    try db.batch(.{ .writes = normalized, .relational_schema_version = 1 });
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"total"} });
    defer reader.deinit();
    var committed = try reader.nextTypedPage(alloc, null, .{});
    defer committed.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740995), committed.rows[0].typed.?.object.get("total").?.integer);
    var changed = try std.json.parseFromSlice(std.json.Value, alloc, row_schema, .{});
    defer changed.deinit();
    changed.value.object.getPtr("version").?.* = .{ .integer = 2 };
    changed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?.* = .{ .string = "7" };
    const newer = try std.json.Stringify.valueAlloc(alloc, changed.value, .{});
    defer alloc.free(newer);
    try db.setSchemaJson(alloc, newer);
    const pinned = try session.normalizeRows(alloc, &.{.{ .key = "later", .value = "{\"a\":1}" }});
    defer {
        for (pinned) |prepared_write| {
            alloc.free(prepared_write.key);
            alloc.free(prepared_write.value);
        }
        alloc.free(pinned);
    }
    var still_old = try std.json.parseFromSlice(std.json.Value, alloc, pinned[0].value, .{});
    defer still_old.deinit();
    try std.testing.expectEqual(@as(i64, 2), still_old.value.object.get("b").?.integer);
    try std.testing.expectEqual(@as(i64, 3), still_old.value.object.get("total").?.integer);
}

test "relational index system typed JSON null survives preparation normalization and transaction replay" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("typed-json-null");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"payload":{"type":"json"},"optional":{"type":["json","null"]}},"required":["payload"],"additionalProperties":false}}}}
    );
    const input_write = db_mod.types.BatchWrite{ .key = "a", .value = "{\"payload\":null,\"optional\":null}", .json_null_fields = &.{"payload"} };
    const session = try db.openRelationalReadSession(alloc, "", "", .{ .relational_query_json = "{\"fields\":[\"payload\",\"optional\"],\"schema_version\":1}", .limit = 16 });
    defer session.deinit();
    const normalized = try session.normalizeRows(alloc, &.{input_write});
    defer {
        for (normalized) |item| {
            alloc.free(item.key);
            alloc.free(item.value);
            for (item.json_null_fields) |name| alloc.free(name);
            alloc.free(item.json_null_fields);
        }
        alloc.free(normalized);
    }
    try std.testing.expectEqualStrings("payload", normalized[0].json_null_fields[0]);
    try db.batch(.{ .writes = normalized, .relational_schema_version = 1 });
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{ "payload", "optional" } });
    defer reader.deinit();
    var page = try reader.nextTypedPage(alloc, null, .{});
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqualSlices(bool, &.{ false, true }, page.rows[0].sql_nulls.?);
    const transaction = try db.beginTransaction(2);
    try db.writeIntents(transaction, &.{.{ .key = "b", .value = input_write.value, .json_null_fields = input_write.json_null_fields }}, &.{});
    try db.commitTransaction(transaction, 3);
    var committed = try db.beginRelationalRows(alloc, .{ .fields = &.{ "payload", "optional" } });
    defer committed.deinit();
    var committed_page = try committed.nextTypedPage(alloc, null, .{});
    defer committed_page.deinit();
    try std.testing.expectEqual(@as(usize, 2), committed_page.rows.len);
    for (committed_page.rows) |row| try std.testing.expectEqualSlices(bool, &.{ false, true }, row.sql_nulls.?);
    try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{.{ .key = "bad", .value = input_write.value, .json_null_fields = &.{ "payload", "payload" } }} }));
    try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{.{ .key = "bad", .value = input_write.value, .json_null_fields = &.{"missing"} }} }));
}

test "relational index system activation projection transports only present JSON null markers" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("activation-json-provenance");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"j":{"type":"json"},"s":{"type":"json","nullable":true}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"j\":null,\"s\":null}", .json_null_fields = &.{"j"} },
        .{ .key = "b", .value = "{\"j\":{},\"s\":null}" },
    } });
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{ "j", "s" }, .include_primary_digest = true });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, db.backend_runtime.io(), .{});
    defer page.deinit();
    const Row = @import("relational_integrity_activation_contract.zig").Row;
    const wire_rows = try alloc.alloc(Row, page.rows.len);
    defer alloc.free(wire_rows);
    for (wire_rows, page.rows) |*row, source| row.* = .{ .key = source.key, .json = source.json, .version = source.version, .expected_content_digest = source.expected_content_digest, .json_null_fields = source.json_null_fields };
    const Response = struct {
        rows: []const Row,
        pub fn jsonStringify(response: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
            try @import("relational_integrity_json.zig").write(response, stream);
        }
    };
    const wire = try std.json.Stringify.valueAlloc(alloc, Response{ .rows = wire_rows }, .{});
    defer alloc.free(wire);
    var parsed = try std.json.parseFromSlice(struct {
        rows: []const @import("../../api/relational_integrity_commit.zig").BackfillRow,
    }, alloc, wire, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
    const first = parsed.value.rows[0];
    const second = parsed.value.rows[1];
    try std.testing.expectEqualStrings("a", first.key);
    try std.testing.expectEqual(@as(usize, 1), first.json_null_fields.len);
    try std.testing.expectEqualStrings("j", first.json_null_fields[0]);
    try std.testing.expect(first.expected_content_digest != null);
    try std.testing.expectEqualStrings("b", second.key);
    try std.testing.expectEqual(@as(usize, 0), second.json_null_fields.len);
    try std.testing.expect(second.expected_content_digest != null);
    // Empty metadata adds no wire field; omitting it must still decode as the
    // empty marker set, never turn a SQL NULL into present JSON null.
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, wire, "\"json_null_fields\""));
}

test "relational index system typed JSON null obeys composed JSON constraints" {
    const schema_api = @import("../../schema/mod.zig");
    for ([_]struct { constraint: []const u8, accepted: bool }{
        .{ .constraint = "\"allOf\":[{\"type\":\"json\"}]", .accepted = true },
        .{ .constraint = "\"allOf\":[{\"anyOf\":[{\"type\":\"json\"},{\"type\":\"integer\"}]}]", .accepted = true },
        .{ .constraint = "\"anyOf\":[{\"type\":\"json\"},{\"type\":\"string\"}]", .accepted = true },
        .{ .constraint = "\"oneOf\":[{\"type\":\"json\"},{\"type\":\"null\"}]", .accepted = false },
        .{ .constraint = "\"not\":{\"type\":\"json\"}", .accepted = false },
        .{ .constraint = "\"if\":{\"type\":\"json\"},\"then\":{\"enum\":[1]}", .accepted = false },
        .{ .constraint = "\"if\":{\"type\":\"integer\"},\"else\":{\"const\":null}", .accepted = true },
        .{ .constraint = "\"const\":null", .accepted = true },
        .{ .constraint = "\"enum\":[1]", .accepted = false },
    }) |case| {
        const schema_json = try std.fmt.allocPrint(
            alloc,
            "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"payload\":{{\"type\":\"json\",{s}}}}}}}}}}}}}",
            .{case.constraint},
        );
        defer alloc.free(schema_json);
        var validator = try schema_api.CompiledTableValidator.init(alloc, schema_json);
        defer validator.deinit(alloc);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"payload\":null}", .{});
        defer parsed.deinit();
        const result = validator.prepareTypedValue(parsed.arena.allocator(), alloc, &parsed.value, &.{"payload"}, false);
        if (case.accepted) try result else try std.testing.expectError(error.InvalidBatchRequest, result);
        const restored = @import("../../schema/table_schema_impl.zig").validateRelationalRestoreProperty(
            alloc,
            validator.schema,
            0,
            parsed.value.object.getPtr("payload").?,
            &validator.execution,
            true,
        );
        if (case.accepted) try restored else try std.testing.expectError(error.InvalidBatchRequest, restored);
        // The same JSON representation without provenance denotes SQL NULL.
        // Admitting literal JSON null must never weaken column nullability.
        try std.testing.expectError(error.InvalidBatchRequest, validator.prepareValue(parsed.arena.allocator(), alloc, &parsed.value));
    }
}

test "relational index system typed JSON null provenance is confined to named root datums" {
    const schema_api = @import("../../schema/mod.zig");
    var validator = try schema_api.CompiledTableValidator.init(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"payload":{"type":"json"},"other":{"type":"json"},"nested":{"type":"object","properties":{"child":{"type":"integer"}}}},"additionalProperties":false}}}}
    );
    defer validator.deinit(alloc);
    for ([_][]const u8{
        "{\"payload\":null,\"other\":null}",
        "{\"payload\":null,\"nested\":{\"child\":null}}",
        "null",
    }) |input| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, input, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidBatchRequest, validator.prepareTypedValue(parsed.arena.allocator(), alloc, &parsed.value, &.{"payload"}, false));
    }
}

test "relational index system automatic bounds use compiled collation identity" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-auto-collation");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"a_ci","keys":[{"column":"label","collation":"ci"}]},{"name":"z_binary","keys":[{"column":"label"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{
        .{ .key = "upper", .value = "{\"label\":\"Z\"}" },
        .{ .key = "lower", .value = "{\"label\":\"a\"}" },
    } });
    _ = try readyIndex(&db, "a_ci");
    _ = try readyIndex(&db, "z_binary");
    var reader = try db.beginRelationalRows(alloc, .{ .auto_index = true, .fields = &.{"label"}, .conditions = &.{
        .{ .column = "label", .op = .lt, .value = .{ .string = "a" }, .collation = "binary" },
    } });
    defer reader.deinit();
    try std.testing.expectEqualStrings("z_binary", reader.index.?.name);
    var page = try reader.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqualStrings("upper", page.rows[0].key);
}

test "relational index system automatic ranges and snapshot costing bound scan work" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-auto-range-cost");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_score","keys":[{"column":"tenant"},{"column":"score","direction":"desc"}],"include_columns":["bucket","payload"]},{"name":"bucket","keys":[{"column":"bucket"}],"include_columns":["tenant","score","payload"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"score":{"type":"integer"},"bucket":{"type":"integer"},"payload":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 96);
    for (writes, 0..) |*request, i| request.* = .{
        .key = try std.fmt.allocPrint(owned, "row:{d:0>3}", .{i}),
        .value = try std.json.Stringify.valueAlloc(owned, .{ .tenant = 1, .score = i, .bucket = @as(usize, @intFromBool(i == 15)), .payload = "value" }, .{}),
    };
    try db.batch(.{ .writes = writes });
    _ = try readyIndex(&db, "tenant_score");
    _ = try readyIndex(&db, "bucket");
    const bounded = [_]rows.Condition{
        .{ .column = "tenant", .op = .eq, .value = .{ .integer = 1 } },
        .{ .column = "score", .op = .gte, .value = .{ .integer = 10 } },
        .{ .column = "score", .op = .gt, .value = .{ .integer = 12 } },
        .{ .column = "score", .op = .lte, .value = .{ .integer = 20 } },
        .{ .column = "score", .op = .lt, .value = .{ .integer = 19 } },
    };
    {
        var reader = try db.beginRelationalRows(alloc, .{ .auto_index = true, .fields = &.{"payload"}, .conditions = &bounded });
        defer reader.deinit();
        try std.testing.expectEqualStrings("tenant_score", reader.index.?.name);
        try std.testing.expectEqual(@as(usize, 0), reader.planner_records);
        var page = try reader.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 6), page.rows.len);
        try std.testing.expectEqual(@as(usize, 6), page.records_examined);
        try std.testing.expectEqualStrings("row:018", page.rows[0].key);
        try std.testing.expectEqualStrings("row:013", page.rows[5].key);
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
    }
    {
        const selective = bounded ++ [_]rows.Condition{.{ .column = "bucket", .op = .eq, .value = .{ .integer = 1 } }};
        var reader = try db.beginRelationalRows(alloc, .{ .auto_index = true, .fields = &.{"payload"}, .conditions = &selective });
        defer reader.deinit();
        // Actual cardinality wins over the longer composite prefix.
        try std.testing.expectEqualStrings("bucket", reader.index.?.name);
        try std.testing.expectEqual(@as(usize, 2), reader.planner_candidates);
        try std.testing.expectEqual(@as(usize, 7), reader.planner_records);
        var page = try reader.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqualStrings("row:015", page.rows[0].key);
        try std.testing.expectEqual(@as(usize, 1), page.records_examined);
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        std.debug.print("automatic index costing: 96 primary rows; planner={} records; selected={} records, primary_probes={}\n", .{ reader.planner_records, page.records_examined, page.primary_lookups });
    }
    {
        // A range on the first ascending key needs no equality prefix.
        var reader = try db.beginRelationalRows(alloc, .{ .auto_index = true, .fields = &.{"payload"}, .conditions = &.{
            .{ .column = "bucket", .op = .gte, .value = .{ .integer = 1 } },
            .{ .column = "bucket", .op = .lte, .value = .{ .integer = 1 } },
        } });
        defer reader.deinit();
        try std.testing.expectEqualStrings("bucket", reader.index.?.name);
        var page = try reader.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqual(@as(usize, 1), page.records_examined);
    }
}

fn mixedScanAllocations(test_alloc: std.mem.Allocator, db: *db_mod.DB) !void {
    var reader = try db.beginRelationalRows(test_alloc, .{ .fields = &.{"payload"} });
    defer reader.deinit();
    var page = try reader.nextPage(test_alloc, null, .{ .rows = 2, .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(usize, 2), reader.source_compilations);
}

test "relational index system historical scan evicts bounded snapshot bindings" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("historical-binding-eviction");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    for (1..11) |version| {
        try install(&db, @intCast(version), false);
        try write(&db, version, 1);
    }
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"} });
    defer reader.deinit();
    var count: usize = 0;
    while (true) {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 1 });
        defer page.deinit();
        count += page.rows.len;
        if (!page.more) break;
    }
    try std.testing.expectEqual(@as(usize, 10), count);
    try std.testing.expectEqual(@as(usize, 10), reader.source_compilations);
    var retained: usize = 0;
    for (reader.bindings) |binding| if (binding != null) {
        retained += 1;
    };
    try std.testing.expectEqual(@as(usize, 8), retained);
    try std.testing.expect(reader.binding_bytes <= 2 * 1024 * 1024);
}

test "relational index system mixed schema scan compiles each snapshot layout once" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("mixed-schema-bindings");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    try install(&db, 1, false);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 512);
    for (writes, 0..) |*item, i| item.* = .{ .key = try std.fmt.allocPrint(owned, "{d:0>4}", .{i}), .value = "{\"tenant\":1,\"id\":7,\"payload\":\"old\"}" };
    try db.batch(.{ .writes = writes });
    try install(&db, 2, false);
    const updates = try owned.alloc(db_mod.types.BatchWrite, writes.len / 2);
    for (updates, 0..) |*item, i| item.* = .{ .key = writes[i * 2 + 1].key, .value = "{\"tenant\":1,\"id\":7,\"payload\":\"new\"}" };
    try db.batch(.{ .writes = updates });
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"payload"}, .conditions = &.{.{ .column = "id", .op = .gte, .value = .{ .integer = 7 } }} });
    defer reader.deinit();
    // Live publication must not change either the source layouts or rows in
    // this reader; cache identity is the read snapshot, never a global epoch ID.
    try install(&db, 3, false);
    var count: usize = 0;
    const started = time.monotonicNs();
    while (true) {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 7 });
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqualStrings(if (count % 2 == 0) "{\"payload\":\"old\"}" else "{\"payload\":\"new\"}", row.json);
            count += 1;
        }
        if (!page.more) break;
    }
    try std.testing.expectEqual(@as(usize, 512), count);
    try std.testing.expectEqual(@as(usize, 2), reader.source_compilations);
    try std.testing.expect(reader.source_cache_hits >= 510);
    try std.testing.expect(reader.binding_bytes <= 2 * 1024 * 1024);
    std.debug.print("mixed schema LSM scan: rows={d} compilations={d} hits={d} elapsed_us={d}\n", .{ count, reader.source_compilations, reader.source_cache_hits, (time.monotonicNs() - started) / 1000 });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, mixedScanAllocations, .{&db});
}

fn expressionKeyAllocations(test_alloc: std.mem.Allocator) !void {
    const schema_mod = @import("../schema.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const tuples = @import("relational_index_keys.zig");
    const native = @import("../relational_index.zig");
    const table: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "price", .path = "price", .column_type = .integer, .allows_null = true },
        .{ .name = "label", .path = "label", .column_type = .string },
    } };
    var layout = try codec.PhysicalLayout.init(test_alloc, table);
    defer layout.deinit();
    const keys = [_]native.RelationalIndexKey{
        .{ .expression_json = "{\"op\":\"multiply\",\"args\":[{\"op\":\"column\",\"column\":\"price\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":\"2\"}]}", .result_type = .integer },
        .{ .column = "id", .direction = .desc },
        .{ .expression_json = "{\"op\":\"lower_ascii\",\"args\":[{\"op\":\"column\",\"column\":\"label\"}]}", .result_type = .string },
        .{ .expression_json = "{\"op\":\"literal\",\"type\":\"integer\",\"value\":null}", .result_type = .integer },
    };
    var plan = try tuples.TuplePlan.init(test_alloc, table, &layout, &keys);
    defer plan.deinit();
    var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", "{\"id\":7,\"price\":9007199254740993,\"label\":\"ALPHA\"}", null, table, &layout);
    defer prepared.deinit(test_alloc);
    var encoded = try plan.encodeAlloc(test_alloc, try prepared.typedView(table, &layout));
    defer encoded.deinit(test_alloc);
    var bounds = std.ArrayList(u8).empty;
    defer bounds.deinit(test_alloc);
    _ = try plan.appendValues(test_alloc, &bounds, &.{ .{ .integer = 18014398509481986 }, .{ .integer = 7 }, .{ .string = "alpha" }, .null });
    try std.testing.expect(encoded.has_null);
    try std.testing.expectEqualSlices(u8, bounds.items, encoded.bytes);
    const historical: schema_mod.TableSchema = .{ .version = 2, .storage_mode = .relational, .relational_columns = &.{ table.relational_columns[2], table.relational_columns[1], table.relational_columns[0] } };
    var old_layout = try codec.PhysicalLayout.init(test_alloc, historical);
    defer old_layout.deinit();
    var projected = try plan.projectSource(test_alloc, historical, &old_layout);
    defer projected.deinit();
    var old_prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", "{\"id\":7,\"price\":9007199254740993,\"label\":\"ALPHA\"}", null, historical, &old_layout);
    defer old_prepared.deinit(test_alloc);
    var old_encoded = try projected.encodeAlloc(test_alloc, try old_prepared.typedView(historical, &old_layout));
    defer old_encoded.deinit(test_alloc);
    try std.testing.expectEqualSlices(u8, encoded.bytes, old_encoded.bytes);
    var cover = try @import("relational_index_cover.zig").Plan.init(test_alloc, table, &layout, &keys, &.{"label"});
    defer cover.deinit();
    try std.testing.expectEqual(@as(usize, 2), cover.columns.len);
    try std.testing.expect(cover.contains("id") and cover.contains("label"));
    try std.testing.expect(!cover.contains("price"));
    const payload = try cover.encode(test_alloc, try prepared.typedView(table, &layout));
    defer test_alloc.free(payload);
    _ = try cover.decode(payload);
}

test "relational index system expression keys share typed bounds historical projections and allocation cleanup" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, expressionKeyAllocations, .{});
}

test "relational index system expression keys fence declarations dependency changes and aggregate expansion" {
    const schema_mod = @import("../schema.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const tuples = @import("relational_index_keys.zig");
    const native = @import("../relational_index.zig");
    const table: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "label", .path = "label", .column_type = .string }} };
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    const expression = "{\"op\":\"column\",\"column\":\"label\"}";
    const key: native.RelationalIndexKey = .{ .expression_json = expression, .result_type = .string };
    for ([_]native.RelationalIndexKey{
        .{},                                .{ .column = "label", .expression_json = expression, .result_type = .string },
        .{ .expression_json = expression }, .{ .column = "label", .result_type = .string },
    }) |invalid| try std.testing.expectError(error.InvalidRelationalIndexDefinition, tuples.TuplePlan.init(alloc, table, &layout, &.{invalid}));
    var plan = try tuples.TuplePlan.init(alloc, table, &layout, &.{ key, key, key });
    defer plan.deinit();
    const incompatible: schema_mod.TableSchema = .{ .version = 2, .storage_mode = .relational, .relational_columns = &.{.{ .name = "label", .path = "different.path", .column_type = .string }} };
    var changed = try codec.PhysicalLayout.init(alloc, incompatible);
    defer changed.deinit();
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, plan.projectSource(alloc, incompatible, &changed));
    const wide = try alloc.alloc(u8, @import("../../schema/relational_expression.zig").max_output_bytes);
    defer alloc.free(wide);
    @memset(wide, 'x');
    const json = try std.json.Stringify.valueAlloc(alloc, .{ .label = wide }, .{});
    defer alloc.free(json);
    var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(alloc, "a", json, null, table, &layout);
    defer prepared.deinit(alloc);
    var output = std.ArrayList(u8).empty;
    defer output.deinit(alloc);
    try output.appendSlice(alloc, "prefix");
    try std.testing.expectError(error.RelationalIndexKeyTooLarge, plan.append(alloc, &output, try prepared.typedView(table, &layout)));
    try std.testing.expectEqualStrings("prefix", output.items);
}

const expression_index_schema =
    \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"column":"tenant"},{"expression":{"op":"multiply","args":[{"op":"column","column":"price"},{"op":"literal","type":"integer","value":"2"}]},"result_type":"integer"},{"column":"id","direction":"desc"}],"include_columns":["price","payload"],"where":[{"column":"tenant","op":"gt","value":0}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"},"price":{"type":"integer"},"payload":{"type":"keyword"}},"additionalProperties":false}}}}
;

fn expressionIndexCount(db: *db_mod.DB, doubled_price: i64) !usize {
    var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "price", "payload" }, .lower = .{ .values = &.{ .{ .integer = 1 }, .{ .integer = doubled_price } } }, .upper = .{ .values = &.{ .{ .integer = 1 }, .{ .integer = doubled_price } } }, .conditions = &.{.{ .column = "tenant", .op = .gt, .value = .{ .integer = 0 } }} });
    defer reader.deinit();
    var count: usize = 0;
    for (0..32) |_| {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        count += page.rows.len;
        if (!page.more) return count;
    }
    return error.RowScanDidNotConverge;
}

/// Simulate a replica whose durable primary checkpoint precedes its local
/// coverage checkpoint. No replicated schema/restore receipt is changed.
fn resetRestoreIndexCoverage(db: *db_mod.DB, failed: bool) !void {
    const jobs = @import("relational_index_jobs.zig");
    var pinned = db.core.relational_indexes.acquire().?;
    defer pinned.deinit();
    const index = pinned.plan.boundIndexes()[0];
    var txn = try db.core.store.beginWriteTxn();
    errdefer txn.abort();
    var progress = try jobs.resetProgress(try jobs.status(&txn, index));
    if (failed) {
        progress.state = .failed;
        progress.failure = .invalid_row;
    }
    const bytes = try progress.encode(alloc);
    defer alloc.free(bytes);
    try txn.put(&jobs.progressKey(index.id()), bytes);
    try txn.commit();
    try db.core.store.sync(true);
}

test "relational index system restore replay projection bounds allocation for wide ordinary batches" {
    const effects = @import("antfly_server_test_sources").local_test_sources.storage_hot_standby_effects;
    const wide = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(wide);
    @memset(wide, '\\'); // escaped payload is larger than either scratch budget
    const payload = try effects.encodeBatchMutationRequestAlloc(alloc, .{ .writes = &.{.{ .key = "wide", .value = wide }} });
    defer alloc.free(payload);
    var scratch: [16 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&scratch);
    const record: @import("replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = payload };
    try std.testing.expectEqual(null, try effects.decodeRestoreFinishForReplay(bounded.allocator(), record));
}

test "relational index system restore receipts require local coverage through failure and owner restart" {
    const staging = @import("restore_staging.zig");
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var source_directory = try TestDirectory.init("restore-index-coverage-source");
    defer source_directory.cleanup();
    var target_directory = try TestDirectory.init("restore-index-coverage-target");
    defer target_directory.cleanup();
    var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_optional_runtimes = false, .start_index_workers = false };
    const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 }, .start_optional_runtimes = false, .start_index_workers = false };
    {
        var source = try db_mod.DB.open(alloc, source_directory.path(), source_options);
        defer source.close();
        try source.setSchemaJson(alloc, expression_index_schema);
        try source.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":7,\"price\":10,\"payload\":\"restored\"}" }} });
    }
    source_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_directory.path(), source_options);
    defer source.close();
    var target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
    defer target.close();
    try target.setSchemaJson(alloc, expression_index_schema);
    const schema_bytes = try @import("../schema.zig").serializeSchema(alloc, target.core.schema.?);
    defer alloc.free(schema_bytes);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = staging.digest(schema_bytes) };
    try target.reserveRestoreStagingScoped(alloc, scope);
    try target.beginRestoreStaging(alloc, scope);
    var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
    defer page.deinit();
    // Hidden import pages must not request ordinary serving-table visibility.
    // Exercise the compiled-owner JSON boundary as well as the native request.
    try std.testing.expectEqual(.write, page.batch.?.sync_level);
    const batch_api = @import("../../api/batch.zig");
    const encoded_page = try batch_api.encodeBatchRequest(alloc, page.batch.?);
    defer alloc.free(encoded_page);
    var decoded_page = try batch_api.parseInternalBatchRequest(alloc, encoded_page);
    defer decoded_page.deinit(alloc);
    try std.testing.expectEqual(.write, decoded_page.req.sync_level);
    try server_test_adapter.applyOrdered(&target, decoded_page.req, .{ .index = 1, .term = 1 });
    try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{}));
    {
        var imported = (try target.restoreStagingStatus(alloc)).?;
        defer imported.deinit();
        try std.testing.expectEqual(.imported, imported.value.phase);
        try std.testing.expectEqual(@as(u64, 1), imported.value.rows);
    }

    // A permanent local job failure must not acquire a validated receipt.
    try resetRestoreIndexCoverage(&target, true);
    try std.testing.expectError(error.RestoreProjectionCorrupt, target.finishRestoreStaging(alloc, scope.digest(), .validated));
    try std.testing.expectError(error.RestoreProjectionCorrupt, target.prepareRestoreStagingIndexesStep(alloc, scope.digest()));
    try resetRestoreIndexCoverage(&target, false);
    var standby_gate: @import("antfly_server_test_sources").local_test_sources.storage_hot_standby_public_gate_state.State = .{};
    standby_gate.role.store(@backingInt(@import("antfly_server_test_sources").local_test_sources.storage_hot_standby_public_gate_state.Role.standby), .release);
    var raft_index: u64 = 2;
    for ([_]staging.Phase{ .validated, .published }) |phase| {
        // Also exercise same-phase receipt retries: a durable validated or
        // published marker is not evidence of replica-local index readiness.
        for (0..2) |trial| {
            try resetRestoreIndexCoverage(&target, false);
            target.close();
            target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
            try std.testing.expectEqual(.building, (try target.relationalIndexBuildStatus("tenant_id")).state);
            try std.testing.expectError(error.IndexRebuilding, target.finishRestoreStaging(alloc, scope.digest(), phase));
            if (trial == 1) {
                target.local_execution.replication_write_gate = .{ .shared = .{ .state = standby_gate.storageWriteState() } };
                try std.testing.expectError(error.HAReadOnlyStandby, target.prepareRestoreStagingIndexesStep(alloc, scope.digest()));
            }
            const request: db_mod.types.BatchRequest = .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = phase } } };
            const entry_index = if (trial == 0) raft_index else raft_index - 1;
            var pending: usize = 0;
            for (0..32) |_| {
                server_test_adapter.applyOrdered(&target, request, .{ .index = entry_index, .term = 1 }) catch |err| switch (err) {
                    error.RestoreProjectionCatchUpPending => {
                        pending += 1;
                        continue;
                    },
                    else => return err,
                };
                break;
            } else return error.IndexBuildDidNotConverge;
            try std.testing.expectEqual(.ready, (try target.relationalIndexBuildStatus("tenant_id")).state);
            try std.testing.expect(pending != 0); // one bounded phase per retry
            var progress = (try target.restoreStagingStatus(alloc)).?;
            defer progress.deinit();
            try std.testing.expectEqual(phase, progress.value.phase);
            if (trial == 0) raft_index += 1;
        }
    }
    // The hot-standby LSN fast path has the same obligation as Raft: a replay
    // receipt cannot skip reconstructing this replica's missing local proof.
    const replication_payload = try @import("antfly_server_test_sources").local_test_sources.storage_hot_standby_effects.encodeBatchMutationRequestAlloc(alloc, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } });
    defer alloc.free(replication_payload);
    const record: @import("replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = replication_payload };
    try replication_ingress.applyRecord(&target, record);
    try resetRestoreIndexCoverage(&target, false);
    target.close();
    target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
    target.local_execution.replication_write_gate = .{ .shared = .{ .state = standby_gate.storageWriteState() } };
    // Superseded entries and an unrelated scope must not perform maintenance
    // against the current generation, even when its local coverage is missing.
    try server_test_adapter.applyOrdered(&target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .validated } } }, .{ .index = 2, .term = 1 });
    try std.testing.expectEqual(.building, (try target.relationalIndexBuildStatus("tenant_id")).state);
    try server_test_adapter.applyOrdered(&target, .{ .restore_staging = .{ .finish = .{ .scope = @splat(99), .phase = .published } } }, .{ .index = 3, .term = 1 });
    try std.testing.expectEqual(.building, (try target.relationalIndexBuildStatus("tenant_id")).state);
    var replication_pending: usize = 0;
    for (0..32) |_| {
        replication_ingress.applyRecord(&target, record) catch |err| switch (err) {
            error.RestoreProjectionCatchUpPending => {
                replication_pending += 1;
                continue;
            },
            else => return err,
        };
        break;
    } else return error.IndexBuildDidNotConverge;
    try std.testing.expectEqual(.ready, (try target.relationalIndexBuildStatus("tenant_id")).state);
    try std.testing.expect(replication_pending != 0);
    target.close();
    target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&target, 20));
}

test "relational index system historical expression failures persist and recover after row correction" {
    const jobs = @import("relational_index_jobs.zig");
    const expressions = @import("../../schema/relational_expression_errors.zig");
    inline for (@typeInfo(expressions.Error).error_set.error_names.?) |field| {
        const err = @field(expressions.Error, field);
        try std.testing.expectEqual(if (expressions.isInvalidInput(err)) @as(?jobs.Failure, .invalid_row) else null, jobs.classifyRowFailure(err));
    }
    try std.testing.expectEqual(null, jobs.classifyRowFailure(error.OutOfMemory));
    try std.testing.expectEqual(null, jobs.classifyRowFailure(error.Canceled));
    try std.testing.expectEqual(null, jobs.classifyRowFailure(error.Corrupted));
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("expression-index-historical-overflow");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    var initial = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{});
    defer initial.deinit();
    try initial.value.object.put(initial.arena.allocator(), "version", .{ .integer = 1 });
    _ = initial.value.object.swapRemove("relational_indexes");
    const schema_json = try std.json.Stringify.valueAlloc(alloc, initial.value, .{});
    defer alloc.free(schema_json);
    try db.setSchemaJson(alloc, schema_json);
    // A valid old row becomes unindexable only when the new expression is
    // installed. The worker must record its physical source hash and fail.
    try db.batch(.{ .writes = &.{.{ .key = "overflow", .value = "{\"tenant\":1,\"id\":7,\"price\":9223372036854775807,\"payload\":\"old\"}" }} });
    const primary_key = try internal.relationalRowKeyAlloc(alloc, "overflow");
    defer alloc.free(primary_key);
    const historical_row = try db.core.store.get(alloc, primary_key);
    defer alloc.free(historical_row);
    try db.setSchemaJson(alloc, expression_index_schema);
    try db.buildRelationalIndexStep("tenant_id", .{});
    const failed = try db.relationalIndexBuildStatus("tenant_id");
    try std.testing.expectEqual(.failed, failed.state);
    try std.testing.expectEqual(.invalid_row, failed.failure);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("tenant_id")).state);
    try db.batch(.{ .writes = &.{.{ .key = "overflow", .value = "{\"tenant\":1,\"id\":7,\"price\":10,\"payload\":\"fixed\"}" }} });
    try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", failed.generation));
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 20));
    // Model a repair discovering the same historical source behind an old
    // forward/reverse pair. Both scrub phases must use the identical durable
    // failure rule, rather than retrying deterministic arithmetic forever.
    for ([_]jobs.Phase{ .forward, .reverse }) |phase| {
        {
            var pinned = db.core.relational_indexes.acquire().?;
            defer pinned.deinit();
            const index = pinned.plan.boundIndexes()[0];
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            var progress = try jobs.resetProgress(try jobs.status(&txn, index));
            progress.phase = phase;
            const encoded = try progress.encode(alloc);
            defer alloc.free(encoded);
            try txn.put(primary_key, historical_row);
            try txn.put(&jobs.progressKey(index.id()), encoded);
            try txn.commit();
        }
        try db.buildRelationalIndexStep("tenant_id", .{});
        try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("tenant_id")).state);
        try std.testing.expectEqual(.invalid_row, (try db.relationalIndexBuildStatus("tenant_id")).failure);
        try db.batch(.{ .writes = &.{.{ .key = "overflow", .value = "{\"tenant\":1,\"id\":7,\"price\":10,\"payload\":\"fixed\"}" }} });
        try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", failed.generation));
        _ = try ready(&db);
        try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 20));
    }
}

test "relational index system direct expression lifecycle partial exclusion repair and portable restore" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-direct-expression-index");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    var initial = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{ .parse_numbers = false });
    defer initial.deinit();
    const arena = initial.arena.allocator();
    try initial.value.object.put(arena, "version", .{ .integer = 1 });
    _ = initial.value.object.swapRemove("relational_indexes");
    const first = try std.json.Stringify.valueAlloc(arena, initial.value, .{});
    try db.setSchemaJson(alloc, first);
    try db.batch(.{ .writes = &.{
        .{ .key = "old", .value = "{\"tenant\":1,\"id\":1,\"price\":10,\"payload\":\"old\"}" },
        .{ .key = "excluded", .value = "{\"tenant\":0,\"id\":2,\"price\":9223372036854775807}" },
    } });
    try db.setSchemaJson(alloc, expression_index_schema);
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 20));
    // Nonmember expressions are never evaluated, even during cold backfill.
    try std.testing.expectError(error.RelationalExpressionOverflow, db.batch(.{ .writes = &.{.{ .key = "excluded", .value = "{\"tenant\":1,\"id\":2,\"price\":9223372036854775807}" }} }));
    try db.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"tenant\":1,\"id\":1,\"price\":20,\"payload\":\"changed\"}" }} });
    try std.testing.expectEqual(@as(usize, 0), try expressionIndexCount(&db, 20));
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 40));
    const status = try db.relationalIndexBuildStatus("tenant_id");
    const id = records.Id{ .generation = status.generation, .slot = status.slot };
    var reverse = std.ArrayList(u8).empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, id, "old");
    const control = @import("relational_index_maintenance_contract.zig");
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(reverse.items, "corrupt reverse");
        const ticket = try (control.Control{ .epoch = 1, .last_request = @splat(1) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 40));
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    var restored_directory = try TestDirectory.init("relational-direct-expression-restored");
    defer restored_directory.cleanup();
    var restored = try db_mod.DB.open(alloc, restored_directory.path(), .{ .start_optional_runtimes = false });
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    _ = try ready(&restored);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&restored, 40));
    const restored_generation = (try restored.relationalIndexBuildStatus("tenant_id")).generation;
    var next_schema = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{ .parse_numbers = false });
    defer next_schema.deinit();
    const next_alloc = next_schema.arena.allocator();
    try next_schema.value.object.put(next_alloc, "version", .{ .integer = 3 });
    const unchanged = try std.json.Stringify.valueAlloc(next_alloc, next_schema.value, .{});
    try restored.setSchemaJson(alloc, unchanged);
    try std.testing.expectEqual(restored_generation, (try restored.relationalIndexBuildStatus("tenant_id")).generation);
    try next_schema.value.object.put(next_alloc, "version", .{ .integer = 4 });
    next_schema.value.object.getPtr("relational_indexes").?.array.items[0].object.getPtr("keys").?.array.items[1].object.getPtr("expression").?.object.getPtr("args").?.array.items[1].object.getPtr("value").?.* = .{ .string = "3" };
    const changed = try std.json.Stringify.valueAlloc(next_alloc, next_schema.value, .{});
    try restored.setSchemaJson(alloc, changed);
    try std.testing.expect((try restored.relationalIndexBuildStatus("tenant_id")).generation > restored_generation);
    _ = try ready(&restored);
    try std.testing.expectEqual(@as(usize, 0), try expressionIndexCount(&restored, 40));
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&restored, 60));
    try restored.batch(.{ .deletes = &.{"old"} });
    try std.testing.expectEqual(@as(usize, 0), try expressionIndexCount(&restored, 60));
}

test "relational index system expression composite cold covering work avoids wide primary probes" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-expression-key-work");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    var schema_value = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{ .parse_numbers = false });
    defer schema_value.deinit();
    const temporary = schema_value.arena.allocator();
    const includes = schema_value.value.object.getPtr("relational_indexes").?.array.items[0].object.getPtr("include_columns").?;
    includes.array.shrinkRetainingCapacity(1);
    const schema_json = try std.json.Stringify.valueAlloc(temporary, schema_value.value, .{});
    try db.setSchemaJson(alloc, schema_json);
    const payload = try temporary.alloc(u8, 4096);
    @memset(payload, 'x');
    const writes = try temporary.alloc(db_mod.types.BatchWrite, 128);
    for (writes, 0..) |*write_item, i| write_item.* = .{
        .key = try std.fmt.allocPrint(temporary, "row:{d}", .{i}),
        .value = try std.json.Stringify.valueAlloc(temporary, .{ .tenant = 1, .id = i, .price = i, .payload = payload }, .{}),
    };
    try db.batch(.{ .writes = writes });
    _ = try ready(&db);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    var probes: [2]usize = undefined;
    for ([_]bool{ false, true }, 0..) |fallback, i| {
        var reader = try db.beginRelationalRows(alloc, .{
            .index = "tenant_id",
            .fields = &.{ "id", "price" },
            .include_primary_digest = fallback,
            .lower = .{ .values = &.{.{ .integer = 1 }} },
            .upper = .{ .values = &.{.{ .integer = 1 }} },
            .conditions = &.{.{ .column = "tenant", .op = .gt, .value = .{ .integer = 0 } }},
        });
        defer reader.deinit();
        var count: usize = 0;
        probes[i] = 0;
        for (0..256) |_| {
            var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 256, .records = 256 });
            defer page.deinit();
            count += page.rows.len;
            probes[i] += page.primary_lookups;
            if (!page.more) break;
        } else return error.RowScanDidNotConverge;
        try std.testing.expectEqual(writes.len, count);
    }
    try std.testing.expectEqual(@as(usize, 0), probes[0]);
    try std.testing.expectEqual(writes.len, probes[1]);
    std.debug.print("expression-index cold work rows={d} primary_probes={d} fallback_probes={d} unselected_primary_payload_bytes={d}\n", .{ writes.len, probes[0], probes[1], writes.len * payload.len });
}

fn partialPreparationAllocations(test_alloc: std.mem.Allocator) !void {
    const registry = @import("schema_registry.zig");
    const plans = @import("relational_index_plan.zig");
    const partial = @import("relational_index_predicate.zig");
    var schemas = try registry.Registry.initCloned(test_alloc, std.testing.io, .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string, .allows_null = true },
    } });
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    const predicates = [_]@import("../relational_index.zig").UniquePredicate{
        .{ .field = "id", .op = .gt, .value_json = "9007199254740993" },
        .{ .field = "label", .op = .eq, .value_json = "\"alpha\"", .collation = "ci" },
    };
    var plan = try plans.View.init(test_alloc, view, &.{.{ .name = "partial", .generation = 1, .keys = &.{.{ .column = "id" }}, .include_columns = &.{"label"}, .where = &predicates }});
    defer plan.release();
    var equivalent = try partial.Plan.init(test_alloc, view.tableSchema().*, view.physicalLayout(), &.{
        .{ .field = "label", .op = .eq, .value_json = "\"ALPHA\"", .collation = "ci" },
        .{ .field = "id", .op = .gt, .value_json = "\"9007199254740993\"" },
        predicates[0],
    });
    defer equivalent.deinit();
    const compiled = plan.boundIndexes()[0].predicate.?;
    try std.testing.expectEqualSlices(u8, &compiled.identity, &equivalent.identity);
    try std.testing.expect(compiled.impliedBy(equivalent.conditions));
    try std.testing.expect(!compiled.impliedBy(equivalent.conditions[0..1]));
    var different = try partial.Plan.init(test_alloc, view.tableSchema().*, view.physicalLayout(), &.{
        .{ .field = "id", .op = .gt, .value_json = "9007199254740992" }, predicates[1],
    });
    defer different.deinit();
    try std.testing.expect(!std.mem.eql(u8, &compiled.identity, &different.identity));
    try std.testing.expect(!compiled.impliedBy(different.conditions));
    const query = [_]@import("relational_predicate.zig").Plan{
        equivalent.conditions[0], equivalent.conditions[1], equivalent.conditions[2], different.conditions[0],
    };
    var proven: [query.len]bool = undefined;
    try std.testing.expect(compiled.impliedByAndMark(&query, &proven));
    // Stronger membership also certifies the query's weaker numeric bound.
    try std.testing.expectEqualSlices(bool, &.{ true, true, true, true }, &proven);
    var batch = plans.Batch.init(test_alloc, plan);
    defer batch.deinit();
    for ([_][]const u8{
        "{\"id\":9007199254740993,\"label\":\"Alpha\"}",
        "{\"id\":9007199254740994,\"label\":null}",
        "{\"id\":9007199254740994,\"label\":\"Alpha\"}",
    }, 0..) |json, i| {
        var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", json, null, view.tableSchema().*, view.physicalLayout());
        defer prepared.deinit(test_alloc);
        _ = try batch.appendPrepared(&prepared);
        const key = try batch.key(i, 0);
        try std.testing.expectEqual(i == 2, key.member);
        if (i < 2) {
            try std.testing.expectEqual(@as(usize, 0), key.bytes.len);
            try std.testing.expectEqual(@as(usize, 0), key.payload.len);
            try std.testing.expectEqual(@as(usize, 0), batch.bytes.items.len);
        }
    }
}

test "relational index system partial canonical typed identities and allocation failures" {
    try std.testing.checkAllAllocationFailures(alloc, partialPreparationAllocations, .{});
}

test "relational index system partial implication hashes wide operands once with bounded merge work" {
    const partial = @import("relational_index_predicate.zig");
    const predicates = @import("relational_predicate.zig");
    var schemas = try @import("schema_registry.zig").Registry.initCloned(alloc, std.testing.io, .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "label", .path = "label", .column_type = .string },
    } });
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    const wide = try temporary.alloc(u8, 4096);
    @memset(wide, 'x');
    const definitions = try temporary.alloc(@import("../relational_index.zig").UniquePredicate, partial.max_conditions);
    for (definitions, 0..) |*definition, i| {
        const literal = try std.fmt.allocPrint(temporary, "{s}{d}", .{ wide, i });
        definition.* = .{ .field = "label", .op = .ne, .value_json = try std.json.Stringify.valueAlloc(temporary, literal, .{}) };
    }
    var plan = try partial.Plan.init(alloc, view.tableSchema().*, view.physicalLayout(), definitions);
    defer plan.deinit();
    var query: [partial.max_conditions]predicates.Plan = undefined;
    var expected_bytes: usize = 0;
    for (&query, 0..) |*condition, i| {
        condition.* = plan.conditions[query.len - 1 - i];
        expected_bytes += condition.operand.len;
    }
    var work: partial.ImplicationWork = .{};
    try std.testing.expect(plan.impliedByWithWork(&query, &work));
    try std.testing.expectEqual(query.len, work.query_hashes);
    try std.testing.expectEqual(expected_bytes, work.operand_bytes_hashed);
    try std.testing.expect(work.comparisons <= query.len + plan.conditions.len);
    try std.testing.expect(!plan.impliedBy(query[0 .. query.len - 1]));
    std.debug.print("partial-index implication work conjuncts={d} query_hashes={d} operand_bytes={d} comparisons={d}\n", .{ query.len, work.query_hashes, work.operand_bytes_hashed, work.comparisons });
    // One equality outside every excluded point proves the entire 256-term
    // index predicate without manufacturing/re-hashing 256 query predicates.
    var equality = try predicates.Plan.init(alloc, view.tableSchema().*, view.physicalLayout(), .{
        .column = "label",
        .op = .eq,
        .value = .{ .string = "a" },
    });
    defer equality.deinit();
    work = .{};
    try std.testing.expect(plan.impliedByWithWork(&.{equality}, &work));
    try std.testing.expectEqual(@as(usize, 1), work.query_hashes);
    try std.testing.expectEqual(@as(usize, 1), work.semantic_domains);
    try std.testing.expectEqual(equality.operand.len, work.operand_bytes_hashed);
    var residual = [_]bool{true};
    try std.testing.expect(plan.impliedByAndMark(&.{equality}, &residual));
    try std.testing.expect(!residual[0]);
    std.debug.print("partial-index semantic proof index_conjuncts=256 query_hashes=1 domains=1 residual_retained=true\n", .{});
}

fn installPartial(db: *db_mod.DB, version: u32, indexed: bool) !void {
    const Key = struct { column: []const u8 };
    const Predicate = struct { column: []const u8, op: []const u8, value: i64 };
    const Index = struct { name: []const u8, keys: []const Key, include_columns: []const []const u8, where: []const Predicate };
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .version = version,
        .storage_mode = "relational",
        .default_type = "row",
        .relational_indexes = @as([]const Index, if (indexed) &.{.{ .name = "tenant_id", .keys = &.{.{ .column = "id" }}, .include_columns = &.{ "tenant", "payload" }, .where = &.{.{ .column = "tenant", .op = "gt", .value = 0 }} }} else &.{}),
        .document_schemas = .{ .row = .{ .schema = .{
            .type = "object",
            .properties = .{ .tenant = .{ .type = "integer" }, .id = .{ .type = "integer" }, .payload = .{ .type = "string" } },
            .additionalProperties = false,
        } } },
    }, .{});
    defer alloc.free(json);
    try db.setSchemaJson(alloc, json);
}

test "relational index system retirement work follows generation size not unrelated table size" {
    const gc = @import("relational_index_gc.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("generation-local-retirement");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try installPartial(&db, 1, true);
    // Ten thousand genuine primary rows, none belonging to the partial index.
    for (0..40) |batch_number| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var writes: [250]@import("types.zig").BatchWrite = undefined;
        for (&writes, 0..) |*item, i| item.* = .{
            .key = try std.fmt.allocPrint(a, "doc:{d:0>8}", .{batch_number * writes.len + i}),
            .value = try std.fmt.allocPrint(a, "{{\"tenant\":0,\"id\":{d},\"payload\":\"unchanged\"}}", .{batch_number * writes.len + i}),
        };
        try db.batch(.{ .writes = &writes });
    }
    try installPartial(&db, 2, false);
    var empty_scanned: usize = 0;
    while (try gc.Page.prepare(alloc, std.testing.io, db.core)) |value| {
        var page = value;
        defer page.deinit();
        empty_scanned += page.records_scanned;
        try page.commit(db.core);
    }
    try std.testing.expectEqual(@as(usize, 0), empty_scanned);

    try installPartial(&db, 3, true);
    for (0..5) |i| try write(&db, i, 1);
    const id = blk: {
        var plan = db.core.relational_indexes.acquire().?;
        defer plan.deinit();
        break :blk plan.plan.boundIndexes()[0].id();
    };
    // Lose one forward key; the independently owned locator must still find
    // its reverse. Retirement cannot depend on decoding corrupt tuple values.
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var cursor = try txn.openCursor();
        const first = (try cursor.seekAtOrAfter(&(try records.forwardPrefix(id)))).?;
        const lost = try alloc.dupe(u8, first.key);
        defer alloc.free(lost);
        cursor.close();
        try txn.delete(lost);
        try txn.commit();
    }
    try installPartial(&db, 4, false);
    var visited: usize = 0;
    var pages: usize = 0;
    while (try gc.Page.prepare(alloc, std.testing.io, db.core)) |value| {
        {
            var page = value;
            defer page.deinit();
            visited += page.records_scanned;
            try page.commit(db.core);
        }
        pages += 1;
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
    }
    try std.testing.expectEqual(@as(usize, 9), visited);
    // The 5ms time slice may yield early on a contended runner. Work must
    // remain generation-local: at most one page per record plus the two
    // phase-exhaustion probes, regardless of unrelated table size.
    try std.testing.expect(pages >= 2 and pages <= visited + 2);
    var key = std.ArrayList(u8).empty;
    defer key.deinit(alloc);
    for (0..5) |i| {
        var name: [64]u8 = undefined;
        key.clearRetainingCapacity();
        try records.appendReverseKey(alloc, &key, id, try std.fmt.bufPrint(&name, "doc:{d:0>8}", .{i}));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, key.items));
    }
    std.debug.print("\nretirement work: 10000 unrelated rows; empty generation={} visits, 5-row damaged generation={} visits / {} pages (reopen each page)\n", .{ empty_scanned, visited, pages });
}

test "relational index system partial membership discharges uncovered predicates without primary probes" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-partial-cover-proof");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"column":"id"}],"include_columns":["label"],"where":[{"column":"active","op":"eq","value":true}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"active":{"type":"boolean"},"bucket":{"type":"integer"},"label":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 192);
    for (writes, 0..) |*write_request, i| write_request.* = .{
        .key = try std.fmt.allocPrint(owned, "row:{d:0>3}", .{i}),
        .value = try std.json.Stringify.valueAlloc(owned, .{ .id = i, .active = i < 128, .bucket = i % 2, .label = "member" }, .{}),
    };
    try db.batch(.{ .writes = writes });
    _ = try ready(&db);
    const membership: rows.Condition = .{ .column = "active", .op = .eq, .value = .{ .boolean = true } };
    const cases = [_]struct {
        conditions: []const rows.Condition,
        fields: []const []const u8 = &.{ "id", "label" },
        digest: bool = false,
        expected_rows: usize = 128,
        expected_probes: usize = 0,
    }{
        .{ .conditions = &.{membership} },
        // An extra covered conjunct remains a real residual predicate.
        .{ .conditions = &.{ membership, .{ .column = "label", .op = .eq, .value = .{ .string = "member" } } } },
        .{ .conditions = &.{ membership, .{ .column = "label", .op = .eq, .value = .{ .string = "different" } } }, .expected_rows = 0 },
        // Unproven predicates, requested fields and physical digests still
        // require authoritative primary reads, even with the membership proof.
        .{ .conditions = &.{ membership, .{ .column = "bucket", .op = .eq, .value = .{ .integer = 1 } } }, .expected_rows = 64, .expected_probes = 128 },
        .{ .conditions = &.{membership}, .fields = &.{ "id", "active" }, .expected_probes = 128 },
        .{ .conditions = &.{membership}, .digest = true, .expected_probes = 128 },
    };
    for (cases) |case| {
        var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = case.fields, .conditions = case.conditions, .include_primary_digest = case.digest });
        defer reader.deinit();
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 256, .records = 1024, .time_ns = std.time.ns_per_s });
        defer page.deinit();
        try std.testing.expect(!page.more);
        try std.testing.expectEqual(case.expected_rows, page.rows.len);
        try std.testing.expectEqual(case.expected_probes, page.primary_lookups);
        try std.testing.expectEqual(if (case.expected_probes == 0) case.expected_rows else @as(usize, 0), page.index_only_rows);
    }
    std.debug.print("partial-index uncovered membership predicate rows=128 primary_probes=0 residual_fallback_probes=128\n", .{});
}

test "relational index system SQL durable numeric functions survive LSM reopen and portable restore" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("durable-numeric-index");
    defer directory.cleanup();
    var target = try @import("../../common/test_directory.zig").TestDirectory.init("durable-numeric-restored");
    defer target.cleanup();
    var archive: std.ArrayList(u8) = .empty;
    defer archive.deinit(alloc);
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    {
        var seed = try db_mod.DB.open(alloc, directory.path(), options);
        defer seed.close();
        var create = try @import("../../sql/compiler.zig").compile(alloc, "CREATE TABLE t (id integer, n numeric, g numeric GENERATED ALWAYS AS (round(n,2)) STORED, d numeric DEFAULT trunc(9.876::numeric,2), CHECK (sqrt(n) >= 0))", .{});
        defer create.deinit();
        const schema_bytes = try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(alloc, create.statement.create_table);
        defer alloc.free(schema_bytes);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var public = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), schema_bytes, .{ .parse_numbers = false });
        var index = try @import("../../sql/compiler.zig").compile(alloc, "CREATE INDEX tenant_id ON t ((round(n,2))) INCLUDE (id,n,g,d)", .{});
        defer index.deinit();
        _ = try @import("../../sql/schema_ddl.zig").apply(arena.allocator(), &public, index.statement.catalog_ddl);
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), public, .{});
        try seed.setSchemaJson(alloc, encoded);
        try std.testing.expect(seed.core.schema.?.requires_numeric_functions);
        try seed.batch(.{ .writes = &.{
            .{ .key = "r1", .value = "{\"id\":1,\"n\":9007199254740993.255}" },
            .{ .key = "r2", .value = "{\"id\":2,\"n\":1.255}" },
        } });
        _ = try ready(&seed);
        try @import("../portable_backup.zig").exportPortable(alloc, seed.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_numeric_functions);
        _ = try ready(db);
        const generation = (try db.relationalIndexBuildStatus("tenant_id")).generation;
        var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "n", "g", "d" } });
        defer reader.deinit();
        var page = try reader.nextTypedPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 2), page.rows.len);
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        for (page.rows) |row| {
            const object = row.typed.?.object;
            const value = object.get("g").?;
            const expected: []const u8 = if (object.get("id").?.integer == 1) "9007199254740993.26" else "1.26";
            try std.testing.expectEqualStrings(expected, if (value == .number_string) value.number_string else value.string);
            const default = object.get("d").?;
            try std.testing.expectEqualStrings("9.87", if (default == .number_string) default.number_string else default.string);
        }
        try std.testing.expectError(error.SqlInvalidPowerArgument, db.batch(.{ .writes = &.{
            .{ .key = "not-committed", .value = "{\"id\":3,\"n\":2}" },
            .{ .key = "invalid", .value = "{\"id\":4,\"n\":-1}" },
        } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try db.batch(.{ .writes = &.{.{ .key = "r2", .value = "{\"id\":2,\"n\":2.345}" }} });
        try std.testing.expectEqual(generation, (try db.relationalIndexBuildStatus("tenant_id")).generation);
    }
}

test "relational index system durable text indexes and CHECKs survive LSM reopen and portable restore" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("durable-text-index");
    defer directory.cleanup();
    var target = try @import("../../common/test_directory.zig").TestDirectory.init("durable-text-restored");
    defer target.cleanup();
    var archive: std.ArrayList(u8) = .empty;
    defer archive.deinit(alloc);
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    {
        var seed = try db_mod.DB.open(alloc, directory.path(), options);
        defer seed.close();
        try seed.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"result_type":"string","expression":{"op":"md5","args":[{"op":"replace","args":[{"op":"column","column":"payload"},{"op":"literal","type":"string","value":"old"},{"op":"literal","type":"string","value":"new"}]}]}}],"include_columns":["id","payload"]}],"checks":[{"name":"payload_required","expression":{"op":"is_not_null","args":[{"op":"md5","args":[{"op":"column","column":"payload"}]}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"payload":{"type":"keyword","nullable":true}},"additionalProperties":false}}}}
        );
        try std.testing.expect(seed.core.schema.?.requires_text_expressions);
        try seed.batch(.{ .writes = &.{
            .{ .key = "r1", .value = "{\"id\":1,\"payload\":\"old one\"}" },
            .{ .key = "r2", .value = "{\"id\":2,\"payload\":\"héllo\"}" },
            .{ .key = "r3", .value = "{\"id\":3,\"payload\":\"\"}" },
        } });
        _ = try ready(&seed);
        try @import("../portable_backup.zig").exportPortable(alloc, seed.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_text_expressions);
        _ = try ready(db);
        const generation = (try db.relationalIndexBuildStatus("tenant_id")).generation;
        {
            var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "payload" } });
            defer reader.deinit();
            var page = try reader.nextTypedPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
            defer page.deinit();
            try std.testing.expect(!page.more);
            try std.testing.expectEqual(@as(usize, 3), page.rows.len);
            try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
            var seen: [3]bool = @splat(false);
            for (page.rows) |row| {
                const id = row.typed.?.object.get("id").?.integer;
                try std.testing.expect(id >= 1 and id <= 3);
                const ordinal: usize = @intCast(id - 1);
                try std.testing.expect(!seen[ordinal]);
                seen[ordinal] = true;
                try std.testing.expectEqualStrings(([_][]const u8{ "old one", "héllo", "" })[ordinal], row.typed.?.object.get("payload").?.string);
            }
        }
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{
            .{ .key = "not-committed", .value = "{\"id\":4,\"payload\":\"valid\"}" },
            .{ .key = "invalid", .value = "{\"id\":5,\"payload\":null}" },
        } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try db.batch(.{ .writes = &.{.{ .key = "r1", .value = "{\"id\":1,\"payload\":\"old changed\"}" }} });
        try std.testing.expectEqual(generation, (try db.relationalIndexBuildStatus("tenant_id")).generation);
        var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "payload" } });
        defer reader.deinit();
        var page = try reader.nextTypedPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 3), page.rows.len);
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        var updated = false;
        for (page.rows) |row| if (row.typed.?.object.get("id").?.integer == 1) {
            try std.testing.expect(!updated);
            updated = true;
            try std.testing.expectEqualStrings("old changed", row.typed.?.object.get("payload").?.string);
        };
        try std.testing.expect(updated);
    }
}

test "relational index system SQL immutable index constant membership survives LSM reopen and portable restore" {
    for ([_]bool{ true, false }) |constant| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("constant-index-membership");
        defer directory.cleanup();
        var target = try @import("../../common/test_directory.zig").TestDirectory.init("constant-index-restored");
        defer target.cleanup();
        var archive: std.ArrayList(u8) = .empty;
        defer archive.deinit(alloc);
        const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
        {
            var seed = try db_mod.DB.open(alloc, directory.path(), options);
            defer seed.close();
            const definition = try std.json.Stringify.valueAlloc(alloc, .{
                .version = 1,
                .storage_mode = "relational",
                .default_type = "row",
                .relational_indexes = &.{.{ .name = "tenant_id", .keys = &.{.{ .column = "id" }}, .include_columns = &.{"payload"}, .where = &.{.{ .constant = constant }} }},
                .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = .{ .id = .{ .type = "integer" }, .payload = .{ .type = "keyword" } }, .additionalProperties = false } } },
            }, .{});
            defer alloc.free(definition);
            try seed.setSchemaJson(alloc, definition);
            try seed.batch(.{ .writes = &.{
                .{ .key = "r1", .value = "{\"id\":1,\"payload\":\"one\"}" },
                .{ .key = "r2", .value = "{\"id\":2,\"payload\":\"two\"}" },
                .{ .key = "r3", .value = "{\"id\":3,\"payload\":\"three\"}" },
            } });
            _ = try ready(&seed);
            try @import("../portable_backup.zig").exportPortable(alloc, seed.core.store, &archive);
        }
        var reopened = try db_mod.DB.open(alloc, directory.path(), options);
        defer reopened.close();
        var restored = try db_mod.DB.open(alloc, target.path(), options);
        defer restored.close();
        try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
        for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
            _ = try ready(db);
            const progress = try db.relationalIndexBuildStatus("tenant_id");
            const prefix = try records.forwardPrefix(.{ .generation = progress.generation, .slot = progress.slot });
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var cursor = try read.openCursor();
            defer cursor.close();
            var item = try cursor.seekAtOrAfter(&prefix);
            var count: usize = 0;
            while (item) |entry| : (item = try cursor.next()) {
                if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
                _ = try records.forwardOwnership(entry.key);
                count += 1;
            }
            try std.testing.expectEqual(@as(usize, if (constant) 3 else 0), count);
            if (!constant) try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
            var reader = try db.beginRelationalRows(alloc, .{
                .index = "tenant_id",
                .fields = &.{ "id", "payload" },
                .conditions = if (constant) &.{} else &.{ .{ .column = "id", .op = .gt, .value = .{ .integer = 0 } }, .{ .column = "id", .op = .lte, .value = .{ .integer = 0 } } },
            });
            defer reader.deinit();
            var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
            defer page.deinit();
            try std.testing.expect(!page.more);
            try std.testing.expectEqual(@as(usize, if (constant) 3 else 0), page.rows.len);
            try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        }
    }
}

fn partialCount(db: *db_mod.DB) !usize {
    var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "payload" }, .conditions = &.{.{ .column = "tenant", .op = .gt, .value = .{ .integer = 0 } }} });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
    defer page.deinit();
    try std.testing.expect(!page.more);
    try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
    return page.rows.len;
}

test "relational index system partial stronger query bounds preserve residual covering semantics" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-partial-interval-proof");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"scores","keys":[{"column":"id"}],"include_columns":["score"],"where":[{"column":"score","op":"gte","value":10},{"column":"score","op":"lte","value":20}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"score":{"type":"integer"}},"additionalProperties":false}}}}
    );
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 32);
    for (writes, 0..) |*write_request, i| write_request.* = .{
        .key = try std.fmt.allocPrint(owned, "row:{d:0>3}", .{i}),
        .value = try std.json.Stringify.valueAlloc(owned, .{ .id = i, .score = i }, .{}),
    };
    try db.batch(.{ .writes = writes });
    // Query=>index admits a stronger range, but index=>query does NOT prove
    // its residual. Incorrectly sharing these two proofs would return 11 rows.
    _ = try readyIndex(&db, "scores");
    {
        var automatic = try db.beginRelationalRows(alloc, .{ .auto_index = true, .fields = &.{"id"}, .conditions = &.{
            .{ .column = "id", .op = .eq, .value = .{ .integer = 15 } },
            .{ .column = "score", .op = .gte, .value = .{ .integer = 12 } },
            .{ .column = "score", .op = .lt, .value = .{ .integer = 18 } },
        } });
        defer automatic.deinit();
        try std.testing.expectEqualStrings("scores", automatic.index.?.name);
        var selected = try automatic.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 1), selected.rows.len);
        try std.testing.expectEqualStrings("row:015", selected.rows[0].key);
        try std.testing.expectEqual(@as(usize, 0), selected.primary_lookups);
        var fallback = try db.beginRelationalRows(alloc, .{ .auto_index = true, .fields = &.{"id"}, .conditions = &.{
            .{ .column = "id", .op = .eq, .value = .{ .integer = 5 } },
        } });
        defer fallback.deinit();
        try std.testing.expect(fallback.index == null);
        var primary_page = try fallback.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
        defer primary_page.deinit();
        try std.testing.expectEqual(@as(usize, 1), primary_page.rows.len);
        try std.testing.expectEqualStrings("row:005", primary_page.rows[0].key);
    }
    var reader = try db.beginRelationalRows(alloc, .{ .index = "scores", .fields = &.{"id"}, .conditions = &.{
        .{ .column = "score", .op = .gte, .value = .{ .integer = 12 } },
        .{ .column = "score", .op = .lt, .value = .{ .integer = 18 } },
        .{ .column = "score", .op = .ne, .value = .{ .integer = 5 } },
        .{ .column = "score", .op = .is_not_null },
    } });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 64, .records = 128, .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expect(!page.more);
    try std.testing.expectEqual(@as(usize, 6), page.rows.len);
    try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
    try std.testing.expectEqual(@as(usize, 6), page.index_only_rows);
    try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "scores", .conditions = &.{
        .{ .column = "score", .op = .gte, .value = .{ .integer = 9 } },
        .{ .column = "score", .op = .lte, .value = .{ .integer = 20 } },
    } }));
}

test "relational index system partial historical membership coalescing and covering proofs" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-partial-membership");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try installPartial(&db, 1, false);
    try db.batch(.{ .writes = &.{
        .{ .key = "yes", .value = "{\"tenant\":1,\"id\":1,\"payload\":\"yes\"}" },
        .{ .key = "no", .value = "{\"tenant\":0,\"id\":2,\"payload\":\"no\"}" },
        .{ .key = "null", .value = "{\"id\":3,\"payload\":\"unknown\"}" },
    } });
    try installPartial(&db, 2, true);
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
    try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "tenant_id", .conditions = &.{.{ .column = "tenant", .op = .gte, .value = .{ .integer = 0 } }} }));
    // Historical nonmembers legitimately have no reverse entry. Their first
    // active-layout update adds membership, while old members remove it.
    try db.batch(.{ .writes = &.{
        .{ .key = "yes", .value = "{\"tenant\":0,\"id\":1}" },
        .{ .key = "no", .value = "{\"tenant\":2,\"id\":2}" },
        .{ .key = "null", .value = "{\"tenant\":3,\"id\":3}" },
    } });
    try std.testing.expectEqual(@as(usize, 2), try partialCount(&db));
    try db.batch(.{ .writes = &.{
        .{ .key = "no", .value = "{\"tenant\":9,\"id\":2}" },
        .{ .key = "no", .value = "{\"id\":2}" },
        .{ .key = "yes", .value = "{\"tenant\":9,\"id\":1}" },
        .{ .key = "yes", .value = "{\"tenant\":0,\"id\":1}" },
    } });
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    try db.batch(.{ .deletes = &.{ "no", "yes" } });
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    try db.batch(.{ .deletes = &.{"null"} });
    try std.testing.expectEqual(@as(usize, 0), try partialCount(&db));
}

test "relational index system partial missing member reverse fails closed and repair scrubs nonmembers" {
    const control = @import("relational_index_maintenance_contract.zig");
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-partial-repair");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try installPartial(&db, 1, true);
    try db.batch(.{ .writes = &.{.{ .key = "member", .value = "{\"tenant\":1,\"id\":1}" }} });
    _ = try ready(&db);
    const status = try db.relationalIndexBuildStatus("tenant_id");
    const id = records.Id{ .generation = status.generation, .slot = status.slot };
    var reverse = std.ArrayList(u8).empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, id, "member");
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(reverse.items);
        try txn.commit();
    }
    try std.testing.expectError(error.MissingRelationalIndexReverse, db.batch(.{ .writes = &.{.{ .key = "member", .value = "{\"tenant\":0,\"id\":1}" }} }));
    try std.testing.expectError(error.MissingRelationalIndexReverse, db.batch(.{ .deletes = &.{"member"} }));
    const original = (try db.get(alloc, "member")).?;
    defer alloc.free(original);
    try std.testing.expectEqualStrings("{\"tenant\":1,\"id\":1}", original);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const ticket = try (control.Control{ .epoch = 1, .last_request = @splat(1) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    _ = try ready(&db);
    const primary = try internal.relationalRowKeyAlloc(alloc, "member");
    defer alloc.free(primary);
    // Simulate a stale derived pair surviving an authoritative nonmember
    // update. Copy canonical bytes from another nonmember, not invalid AROW.
    try db.batch(.{ .writes = &.{.{ .key = "nonmember", .value = "{\"tenant\":0,\"id\":1}" }} });
    const nonmember_key = try internal.relationalRowKeyAlloc(alloc, "nonmember");
    defer alloc.free(nonmember_key);
    const nonmember = try db.core.store.get(alloc, nonmember_key);
    defer alloc.free(nonmember);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(primary, nonmember);
        try txn.put(reverse.items, "corrupt reverse");
        const ticket = try (control.Control{ .epoch = 2, .last_request = @splat(2) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 0), try partialCount(&db));
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(reverse.items));
}

test "relational index system topology clear bounds key-only pages and removes every generation" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const Destination = @import("antfly_server_test_sources").local_test_sources.data_storage_db_split_handoff.Destination;
    for ([_]bool{ false, true }) |relational| {
        var directory = try TestDirectory.init("relational-topology-clear");
        defer directory.cleanup();
        var destination = try Destination.init(alloc, .{ .root_dir = directory.path(), .db = .{ .start_optional_runtimes = false } });
        defer destination.deinit();
        const db = destination.db;
        if (relational) try install(db, 1, true);
        // More than one physical page, with wide values to catch accidental
        // value materialization. Binary primary IDs retain ordinary ordering.
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const owned = arena.allocator();
        const payload = try owned.alloc(u8, 16 * 1024);
        @memset(payload, 'x');
        const json = try std.json.Stringify.valueAlloc(owned, .{ .tenant = 1, .id = 7, .payload = payload }, .{});
        const writes = try owned.alloc(db_mod.types.BatchWrite, 300);
        for (writes, 0..) |*item, i| item.* = .{ .key = try std.fmt.allocPrint(owned, "m:{d:0>4}\x00", .{i}), .value = json };
        try db.batch(.{ .writes = writes });
        try db.batch(.{ .writes = &.{ .{ .key = "a", .value = json }, .{ .key = "z", .value = json } } });
        if (relational) {
            _ = try ready(db);
            // Keep a retired generation alongside the new active one. Cleanup
            // must use row-owned reverse entries, not only the current plan.
            try install(db, 2, false);
            try install(db, 3, true);
            _ = try ready(db);
        }
        const before = db.core.nextDerivedSequence();
        // The old all-values range scan needed >4 MiB of caller allocation.
        // Key-only pages handle this corpus with a fixed 128-KiB scratch budget.
        var scratch: [128 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&scratch);
        try destination.deleteDocsInRange(bounded.allocator(), .{ .start = "m:", .end = "n" });
        try std.testing.expect(db.core.nextDerivedSequence() > before + 1);
        try destination.deleteDocsInRange(alloc, .{ .start = "m:", .end = "n" });
        try std.testing.expectError(error.InvalidRange, destination.deleteDocsInRange(alloc, .{ .start = "z", .end = "a" }));
        // Catalog row_count is deliberately a presence bit, not cardinality.
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        try std.testing.expectEqual(@as(?u64, 2), try @import("range_cardinality.zig").load(alloc, db.core.store));
        try std.testing.expect((try db.get(alloc, writes[0].key)) == null);
        for ([_][]const u8{ "a", "z" }) |key| {
            const value = (try db.get(alloc, key)).?;
            defer alloc.free(value);
            try std.testing.expectEqualStrings(json, value);
        }
        // Verify no native forward or reverse record survives for a removed
        // document, including generations no longer in the active catalog.
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var item = try cursor.first();
        while (item) |entry| : (item = try cursor.next()) {
            if (internal.isRelationalIndexReverseKey(entry.key)) {
                const owner = try records.parseReverseKey(entry.key);
                try std.testing.expect(!std.mem.startsWith(u8, owner.document_component, "m:"));
            } else if (records.isForwardKey(entry.key)) {
                const owner = try records.forwardOwnership(entry.key);
                try std.testing.expect(!std.mem.startsWith(u8, owner.document_component, "m:"));
            }
        }
    }
}

test "relational index system replicated merge fences stale attempts and rebuilds range readiness after restart" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-merge-replay");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    try db.updateRange(.{ .start = "m", .end = "z" });
    try db.batch(.{ .writes = &.{.{ .key = "x", .value = "{\"tenant\":1,\"id\":9}" }} });
    _ = try ready(&db);
    var index: u64 = 0;
    var checkpoint: db_mod.types.MergeReplicationCheckpoint = .{
        .kind = .accept,
        .transition_id = 100,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .receiver_base_start = "m",
        .receiver_base_end = "z",
        .merged_start = "a",
        .merged_end = "z",
    };
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    // Prior coverage only certified the receiver's old range. A merge may not
    // expose that receipt as readiness for the incoming donor range.
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 50 };
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    const old_copy: db_mod.types.MergeReplicationContext = .{
        .transition_id = 100,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .identity_namespace = db.core.identity_namespace,
        .copy_attempt = checkpoint.copy_attempt,
    };
    const old_request: db_mod.types.BatchRequest = .{ .merge_replication = old_copy, .writes = &.{.{ .key = "b\x00", .value = "{\"tenant\":1,\"id\":1}" }} };
    try applyTopology(&db, &index, old_request);
    checkpoint.copy_attempt = .{ .donor_term = 2, .sequence = 1 };
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    var winning_copy = old_copy;
    winning_copy.copy_attempt = checkpoint.copy_attempt;
    const winning_request: db_mod.types.BatchRequest = .{ .merge_replication = winning_copy, .writes = &.{.{ .key = "b\x00", .value = "{\"tenant\":1,\"id\":7}" }} };
    try applyTopology(&db, &index, winning_request);
    // Exact Raft replay, then a stale owner's request at a NEW applied index:
    // neither may leave old secondary tuples or change the winning row.
    try server_test_adapter.applyOrdered(&db, winning_request, .{ .term = 7, .index = index });
    try applyTopology(&db, &index, old_request);
    try applyTopology(&db, &index, .{ .merge_replication = old_copy, .deletes = &.{"b\x00"} });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    checkpoint.kind = .bootstrap_complete;
    checkpoint.bootstrap_applied_index = 20;
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    try applyTopology(&db, &index, .{ .merge_replication = winning_copy, .deletes = &.{"b\x00"} });
    checkpoint.kind = .finalize;
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    _ = try ready(&db);
    try expectIndexRows(&db, &.{ "x", "b\x00" });
    const winner = (try db.get(alloc, "b\x00")).?;
    defer alloc.free(winner);
    try std.testing.expectEqualStrings("{\"tenant\":1,\"id\":7}", winner);
    try db.batch(.{ .deletes = &.{"b\x00"} });
    try expectIndexRows(&db, &.{"x"});
}

fn applyTopology(db: *db_mod.DB, index: *u64, request: db_mod.types.BatchRequest) !void {
    index.* += 1;
    try server_test_adapter.applyOrdered(&db, request, .{ .term = 7, .index = index.* });
}

test "relational index system replicated split checkpoints and sparse deltas preserve native companions across reopen" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-split-replay");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    _ = try ready(&db);
    const copy: db_mod.types.SplitReplicationContext = .{
        .transition_id = 7001,
        .attempt_epoch = 3,
        .source_group_id = 71,
        .destination_group_id = 72,
        .identity_namespace = db.core.identity_namespace,
        .bootstrap_sequence = 9,
    };
    var control = copy;
    control.operation = .checkpoint;
    control.sequence = 9;
    var checkpoint: db_mod.types.SplitReplicationCheckpoint = .{
        .kind = .destination_begin,
        .transition_id = 7001,
        .attempt_epoch = 3,
        .source_group_id = 71,
        .destination_group_id = 72,
        .range_start = "m",
        .range_end = "",
        .delta_sequence = 9,
    };
    var index: u64 = 0;
    try applyTopology(&db, &index, .{ .split_replication = control, .split_checkpoint = checkpoint });
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
    const request: db_mod.types.BatchRequest = .{ .split_replication = copy, .writes = &.{ .{ .key = "x\x00", .value = "{\"tenant\":1,\"id\":9}" }, .{ .key = "y", .value = "{\"tenant\":1,\"id\":8}" } } };
    try applyTopology(&db, &index, request);
    try server_test_adapter.applyOrdered(&db, request, .{ .term = 7, .index = index });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try applyTopology(&db, &index, .{ .split_replication = control, .split_checkpoint = checkpoint });
    checkpoint.kind = .destination_complete;
    try applyTopology(&db, &index, .{ .split_replication = control, .split_checkpoint = checkpoint });
    var delta = copy;
    delta.bootstrap_sequence = null;
    delta.operation = .delta;
    delta.sequence = 20;
    delta.previous_sequence = 9;
    const update: db_mod.types.BatchRequest = .{ .split_replication = delta, .writes = &.{.{ .key = "x\x00", .value = "{\"tenant\":1,\"id\":7}" }}, .deletes = &.{"y"} };
    try applyTopology(&db, &index, update);
    try server_test_adapter.applyOrdered(&db, update, .{ .term = 7, .index = index });
    try applyTopology(&db, &index, update);
    _ = try ready(&db);
    try expectIndexRows(&db, &.{"x\x00"});
    const current = (try db.get(alloc, "x\x00")).?;
    defer alloc.free(current);
    try std.testing.expectEqualStrings("{\"tenant\":1,\"id\":7}", current);
    try std.testing.expect((try db.get(alloc, "y")) == null);
    try std.testing.expectEqual(@as(u64, 20), try db.getSplitDeltaFinalSeq(alloc));
}

test "relational index system same-bound incarnation change fences readiness and maintenance across reopen" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const jobs = @import("relational_index_jobs.zig");
    const maintenance = @import("relational_index_maintenance_contract.zig");
    var directory = try TestDirectory.init("relational-index-incarnation");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .identity_namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 } });
    defer db.close();
    try install(&db, 1, true);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":9}" }} });
    _ = try ready(&db);
    const command: maintenance.Command = blk: {
        var pinned = db.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const index = pinned.plan.boundIndexes()[0];
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const proof = try jobs.statusProofWithOwnership(&read, index, try jobs.ownership(&read));
        break :blk .{
            .action = .repair,
            .table_id = db.core.identity_namespace.table_id,
            .owner_group_id = 1,
            .schema_version = 1,
            .index_name = "tenant_id",
            .generation = index.id().generation,
            .slot = index.id().slot,
            .owner = proof.progress.owner,
            .comparison = proof.progress.comparison,
            .expected_progress_digest = proof.digest,
            .expected_maintenance_epoch = proof.maintenance_epoch,
            .routing_key = "",
        };
    };
    var namespace = db.core.identity_namespace;
    namespace.range_id += 1;
    try db.reassignIdentityNamespaceForInternalTransition(namespace);
    for (0..2) |pass| {
        if (pass != 0) {
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        }
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
        const transaction = try db.beginTransaction(100 + pass * 10);
        try std.testing.expectError(error.PreparedGenerationChanged, db.writeTransaction(transaction, .{ .relational_index_maintenance = command }));
        try db.abortTransaction(transaction, 101 + pass * 10);
    }
    _ = try ready(&db);
    try expectIndexRows(&db, &.{"a"});
}

fn expectIndexRows(db: *db_mod.DB, expected: []const []const u8) !void {
    var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id" });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, null, .{});
    defer page.deinit();
    try std.testing.expectEqual(expected.len, page.rows.len);
    for (expected) |key| {
        var count: usize = 0;
        for (page.rows) |row| if (std.mem.eql(u8, key, row.key)) {
            count += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), count);
    }
    const progress = try db.relationalIndexBuildStatus("tenant_id");
    const prefix = try records.forwardPrefix(.{ .generation = progress.generation, .slot = progress.slot });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(&prefix);
    var count: usize = 0;
    while (item) |entry| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
        _ = try records.forwardOwnership(entry.key);
        count += 1;
    }
    // A query may defensively reject stale tuples. Inspect physical counts too
    // so that stale/replayed mutations cannot hide an accumulating orphan leak.
    try std.testing.expectEqual(expected.len, count);
}

test "relational index system topology coverage carries completed CHECK phase and incarnation" {
    const catalog = @import("relational_integrity_catalog.zig");
    const activation = @import("relational_integrity_activation.zig");
    const handoff = @import("relational_integrity_handoff.zig");
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    for ([_]bool{ false, true }) |checks| {
        var compiled = try catalog.prepareWithChecks(alloc, null, @splat(1), 1, @splat(2), &.{}, if (checks) @splat(3) else @splat(0));
        defer compiled.deinit();
        const raw = try handoff.coverageForRange(alloc, compiled.value, namespace, .{ .start = "m", .end = "z" });
        defer alloc.free(raw);
        const progress = try activation.Progress.decode(raw);
        try std.testing.expectEqual(.enforced, progress.state);
        try std.testing.expectEqual(if (checks) activation.Phase.check else activation.Phase.foreign_key, progress.phase);
        var replacement = namespace;
        replacement.range_id += 1;
        const changed = try handoff.coverageForRange(alloc, compiled.value, replacement, .{ .start = "m", .end = "z" });
        defer alloc.free(changed);
        const new_progress = try activation.Progress.decode(changed);
        try std.testing.expect(!std.mem.eql(u8, &progress.owner, &new_progress.owner));
    }
}

test "relational index system status pins exact epoch and retired corruption cannot block cleanup" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-index-status");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, false);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":7,\"payload\":\"data\"}" }} });
    try install(&db, 2, true);
    const request = "{\"name\":\"tenant_id\",\"schema_version\":2}";
    var pending = (try db.lookup(alloc, "", .{ .relational_index_status_json = request })).?;
    defer pending.deinit(alloc);
    var status = try std.json.parseFromSlice(@import("relational_index_status_contract.zig").Status, alloc, pending.json, .{});
    defer status.deinit();
    try std.testing.expectEqual(.building, status.value.state);
    try std.testing.expectError(error.PreparedGenerationChanged, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"name\":\"tenant_id\",\"schema_version\":1}" }));
    _ = try ready(&db);
    var covered = (try db.lookup(alloc, "", .{ .relational_index_status_json = request })).?;
    defer covered.deinit(alloc);
    var complete = try std.json.parseFromSlice(@import("relational_index_status_contract.zig").Status, alloc, covered.json, .{});
    defer complete.deinit();
    try std.testing.expectEqual(.ready, complete.value.state);
    var owner_batch = (try db.lookup(alloc, "", .{ .relational_index_status_json = "{\"schema_version\":2}" })).?;
    defer owner_batch.deinit(alloc);
    var batch_status = try std.json.parseFromSlice(@import("relational_index_status_contract.zig").Batch, alloc, owner_batch.json, .{});
    defer batch_status.deinit();
    try std.testing.expectEqual(@as(usize, 1), batch_status.value.statuses.len);
    try std.testing.expectEqualStrings("tenant_id", batch_status.value.statuses[0].name);
    try std.testing.expectEqualDeep(complete.value, batch_status.value.statuses[0].status);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"schema_version\":2}", .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled) }));
    try std.testing.expectError(error.Timeout, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"schema_version\":2}", .execution_deadline_ns = 0 }));
    const id = records.Id{ .generation = complete.value.generation, .slot = complete.value.slot };
    try install(&db, 3, true);
    const reused = try db.relationalIndexBuildStatus("tenant_id");
    try std.testing.expectEqual(.ready, reused.state);
    try std.testing.expectEqual(id.generation, reused.generation);
    try std.testing.expectEqual(id.slot, reused.slot);
    const prefix = try records.forwardPrefix(id);
    const forward = key: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const item = (try cursor.seekAtOrAfter(&prefix)).?;
        try std.testing.expect(std.mem.startsWith(u8, item.key, &prefix));
        break :key try alloc.dupe(u8, item.key);
    };
    defer alloc.free(forward);
    try install(&db, 4, false);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(forward, "damaged retired payload");
        try txn.commit();
    }
    for (0..8) |_| if (!try db.collectRelationalIndexGarbageStep()) break;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(forward));
}

test "relational index system large keys paginate and oversize backfills fail durably" {
    for ([_]bool{ false, true }) |backfill| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("large-index-key");
        defer directory.cleanup();
        var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
        defer db.close();
        const plain = "{\"version\":0,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"s\":{\"type\":\"string\"}},\"additionalProperties\":false}}}}";
        const indexed = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"relational_indexes\":[{\"name\":\"by_s\",\"keys\":[{\"column\":\"s\"}]}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"s\":{\"type\":\"string\"}},\"additionalProperties\":false}}}}";
        try db.setSchemaJson(alloc, if (backfill) plain else indexed);
        const wide = try alloc.alloc(u8, if (backfill) @import("relational_index_limits.zig").max_stored_key_bytes else 70000);
        defer alloc.free(wide);
        @memset(wide, 'x');
        const json = try std.json.Stringify.valueAlloc(alloc, .{ .s = wide }, .{});
        defer alloc.free(json);
        try db.batch(.{ .writes = &.{ .{ .key = "a", .value = json }, .{ .key = "b", .value = json } } });
        if (backfill) {
            try db.setSchemaJson(alloc, indexed);
            _ = try db.runRelationalIndexMaintenancePass();
            const status = try db.relationalIndexBuildStatus("by_s");
            try std.testing.expectEqual(.failed, status.state);
            try std.testing.expectEqual(.key_too_large, status.failure);
            _ = try db.runRelationalIndexMaintenancePass();
            try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("by_s")).state);
        } else {
            _ = try readyIndex(&db, "by_s");
            var first = try db.scan(alloc, "", "", .{ .include_documents = true, .limit = 1, .relational_query_json = "{\"schema_version\":1,\"index\":\"by_s\",\"fields\":[]}" });
            defer first.deinit(alloc);
            try std.testing.expectEqualStrings("a", first.hashes[0].id);
            const cursor = first.hashes[0].relational_cursor.?;
            try std.testing.expect(cursor.len > 128 * 1024);
            const continuation = try std.json.Stringify.valueAlloc(alloc, .{ .schema_version = 1, .index = "by_s", .fields = @as([]const []const u8, &.{}), .after = cursor }, .{});
            defer alloc.free(continuation);
            var next = try db.scan(alloc, "", "", .{ .include_documents = true, .limit = 1, .relational_query_json = continuation });
            defer next.deinit(alloc);
            try std.testing.expectEqualStrings("b", next.hashes[0].id);
            const huge = try alloc.alloc(u8, @import("relational_index_limits.zig").max_stored_key_bytes);
            defer alloc.free(huge);
            @memset(huge, 'x');
            const rejected = try std.json.Stringify.valueAlloc(alloc, .{ .s = huge }, .{});
            defer alloc.free(rejected);
            try std.testing.expectError(error.RelationalIndexKeyTooLarge, db.batch(.{ .writes = &.{.{ .key = "c", .value = rejected }} }));
            var result = try db.scan(alloc, "", "", .{ .include_documents = true, .limit = 10, .relational_query_json = "{\"schema_version\":1,\"index\":\"by_s\",\"fields\":[]}" });
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), result.hashes.len);
        }
    }
    const max = @import("relational_index_limits.zig").max_stored_key_bytes;
    try records.admitForwardKey(max - records.forward_prefix_len - 7, 3);
    try std.testing.expectError(error.RelationalIndexKeyTooLarge, records.admitForwardKey(max - records.forward_prefix_len - 6, 3));
}

test "relational index system public typed bounds cursor auth and cross-owner order" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var left_dir = try TestDirectory.init("relational-index-query-left");
    defer left_dir.cleanup();
    var right_dir = try TestDirectory.init("relational-index-query-right");
    defer right_dir.cleanup();
    var left = try db_mod.DB.open(alloc, left_dir.path(), .{ .start_optional_runtimes = false });
    defer left.close();
    var right = try db_mod.DB.open(alloc, right_dir.path(), .{ .start_optional_runtimes = false });
    defer right.close();
    for ([_]*db_mod.DB{ &left, &right }) |db| try install(db, 1, false);
    try left.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"tenant\":1,\"id\":9007199254740992,\"payload\":\"visible\"}" },
        .{ .key = "b", .value = "{\"tenant\":2,\"id\":0,\"payload\":\"hidden\"}" },
    } });
    try right.batch(.{ .writes = &.{
        .{ .key = "y", .value = "{\"tenant\":1,\"id\":9007199254740993,\"payload\":\"visible\"}" },
        .{ .key = "z", .value = "{\"tenant\":1,\"id\":9007199254740991,\"payload\":\"hidden\"}" },
    } });
    // One owner observed intermediate create/drop epochs; the other restored
    // directly into the final immutable schema. Physical generations differ,
    // but distributed ordering/continuation must use the logical identity.
    try install(&left, 2, true);
    try install(&left, 3, false);
    for ([_]*db_mod.DB{ &left, &right }) |db| try install(db, 4, true);
    const query = "{\"schema_version\":4,\"index\":\"tenant_id\",\"fields\":[\"id\"],\"lower\":{\"values\":[1]},\"upper\":{\"values\":[1]}}";
    const opts = db_mod.types.ScanOptions{ .include_documents = true, .limit = 2, .relational_query_json = query };
    try std.testing.expectError(error.RelationalIndexNotReady, left.scan(alloc, "", "", opts));
    for ([_]*db_mod.DB{ &left, &right }) |db| _ = try ready(db);
    const left_status = try left.relationalIndexBuildStatus("tenant_id");
    const right_status = try right.relationalIndexBuildStatus("tenant_id");
    try std.testing.expect(left_status.generation != right_status.generation);
    var merge = try @import("antfly_server_test_sources").local_test_sources.api_relational_row_merge.Merger.init(alloc, 2);
    defer merge.deinit();
    var after: ?[]u8 = null;
    defer if (after) |value| alloc.free(value);
    for ([_]*db_mod.DB{ &left, &right }) |db| {
        var result = try db.scan(alloc, "", "", opts);
        defer result.deinit(alloc);
        if (db == &right) {
            try std.testing.expectEqualStrings("y", result.hashes[0].id);
            try std.testing.expectEqualStrings("{\"id\":9007199254740993}", result.documents[0].json);
            after = try alloc.dupe(u8, result.hashes[0].relational_cursor.?);
        }
        const encoded = try @import("../../api/query_execution_contract.zig").encodeStorageKernelScanNdjson(alloc, result, true);
        defer alloc.free(encoded);
        merge.beginGroup();
        try merge.write(encoded);
        try merge.endGroup();
    }
    const merged = try merge.finishAlloc();
    defer alloc.free(merged);
    try std.testing.expect(std.mem.startsWith(u8, merged, "{\"_id\":\"y\""));
    try std.testing.expect(std.mem.indexOf(u8, merged, "{\"_id\":\"a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged, "{\"_id\":\"z\"") == null);
    const resumed_query = try std.fmt.allocPrint(alloc, "{{\"schema_version\":4,\"index\":\"tenant_id\",\"fields\":[],\"after\":\"{s}\"}}", .{after.?});
    defer alloc.free(resumed_query);
    var resumed = try right.scan(alloc, "", "", .{ .include_documents = true, .limit = 2, .relational_query_json = resumed_query });
    defer resumed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), resumed.hashes.len);
    try std.testing.expectEqualStrings("z", resumed.hashes[0].id);
    var resumed_other_owner = try left.scan(alloc, "", "", .{ .include_documents = true, .limit = 2, .relational_query_json = resumed_query });
    defer resumed_other_owner.deinit(alloc);
    try std.testing.expectEqualStrings("a", resumed_other_owner.hashes[0].id);
    // Authorization sees the typed full row, not the empty caller projection.
    var authorized = try right.scan(alloc, "", "", .{
        .include_documents = true,
        .limit = 2,
        .relational_query_json = "{\"schema_version\":4,\"index\":\"tenant_id\",\"fields\":[]}",
        .filter_query_json = "{\"term\":{\"payload\":\"visible\"}}",
    });
    defer authorized.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), authorized.hashes.len);
    try std.testing.expectEqualStrings("y", authorized.hashes[0].id);
    try std.testing.expectEqualStrings("{}", authorized.documents[0].json);
    try install(&right, 5, false);
    try std.testing.expectError(error.PreparedGenerationChanged, right.scan(alloc, "", "", opts));
    try install(&right, 6, true);
    _ = try ready(&right);
    const old_generation_query = try std.fmt.allocPrint(alloc, "{{\"schema_version\":6,\"index\":\"tenant_id\",\"fields\":[],\"after\":\"{s}\"}}", .{after.?});
    defer alloc.free(old_generation_query);
    try std.testing.expectError(error.PreparedGenerationChanged, right.scan(alloc, "", "", .{ .relational_query_json = old_generation_query }));
}

fn schema(version: u32, indexed: bool) ![]u8 {
    const Key = struct { column: []const u8, direction: []const u8 = "asc" };
    const Index = struct { name: []const u8, keys: []const Key };
    return std.json.Stringify.valueAlloc(alloc, .{
        .version = version,
        .storage_mode = "relational",
        .default_type = "row",
        .relational_indexes = @as([]const Index, if (indexed) &.{.{ .name = "tenant_id", .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = "desc" } } }} else &.{}),
        .document_schemas = .{ .row = .{ .schema = .{
            .type = "object",
            .properties = .{ .tenant = .{ .type = "integer" }, .id = .{ .type = "integer" }, .payload = .{ .type = "string" } },
            .additionalProperties = false,
        } } },
    }, .{});
}

test "relational index system repair is primary authoritative resumable and control fenced" {
    const jobs = @import("relational_index_jobs.zig");
    const control = @import("relational_index_maintenance_contract.zig");
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-index-repair");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    try write(&db, 1, 1);
    _ = try ready(&db);
    const status = try db.relationalIndexBuildStatus("tenant_id");
    const id = records.Id{ .generation = status.generation, .slot = status.slot };
    const prefix = try records.forwardPrefix(id);
    const forward = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        break :blk try alloc.dupe(u8, (try cursor.seekAtOrAfter(&prefix)).?.key);
    };
    defer alloc.free(forward);
    var reverse = std.ArrayList(u8).empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, id, "doc:00000001");
    var orphan = std.ArrayList(u8).empty;
    defer orphan.deinit(alloc);
    try records.appendReverseKey(alloc, &orphan, id, "absent");
    const stale = try alloc.dupe(u8, forward);
    defer alloc.free(stale);
    stale[records.forward_prefix_len + 1] ^= 1;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(forward);
        try txn.put(reverse.items, "corrupt reverse");
        try txn.put(orphan.items, "corrupt orphan");
        try txn.put(stale, "corrupt stale value");
        const ticket = try (control.Control{ .epoch = 1, .last_request = @splat(1) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    var old_page = (try jobs.Page.prepare(alloc, std.testing.io, db.core, "tenant_id", .{ .records = 1 })).?;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const ticket = try (control.Control{ .epoch = 2, .last_request = @splat(2) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    try std.testing.expectError(error.PreparedGenerationChanged, old_page.commit(db.core));
    old_page.deinit();
    try db.buildRelationalIndexStep("tenant_id", .{ .records = 1 });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    _ = try ready(&db);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualStrings("", try read.get(forward));
        _ = try records.reverseTuple(reverse.items, try read.get(reverse.items));
        try std.testing.expectError(error.NotFound, read.get(stale));
        try std.testing.expectError(error.NotFound, read.get(orphan.items));
    }
    try std.testing.expectEqual(@as(usize, 1), (try scan(&db, true)).count);
    // Corrupt authoritative rows stop the generation; derived repair must not
    // conceal bad user data or falsely publish readiness.
    const primary_key = try internal.relationalRowKeyAlloc(alloc, "doc:00000001");
    defer alloc.free(primary_key);
    const primary_value = try db.core.store.get(alloc, primary_key);
    defer alloc.free(primary_value);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const damaged = try alloc.dupe(u8, primary_value);
        defer alloc.free(damaged);
        damaged[damaged.len - 1] ^= 1;
        try txn.put(primary_key, damaged);
        const ticket = try (control.Control{ .epoch = 3, .last_request = @splat(3) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    for (0..8) |_| {
        try db.buildRelationalIndexStep("tenant_id", .{});
        if ((try db.relationalIndexBuildStatus("tenant_id")).state == .failed) break;
    }
    try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("tenant_id")).state);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(primary_key, primary_value);
        try txn.commit();
    }
    try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", id.generation));
    _ = try ready(&db);
    // A reverse-only orphan must also retire when the catalog becomes empty.
    try install(&db, 2, false);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(forward);
        try txn.commit();
    }
    for (0..16) |_| if (!try db.collectRelationalIndexGarbageStep()) break;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(reverse.items));
    try std.testing.expectError(error.NotFound, read.get(&control.controlKey(id)));
}

test "relational index system replicated maintenance tolerates divergent local progress and fences replay" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const jobs = @import("relational_index_jobs.zig");
    const control = @import("relational_index_maintenance_contract.zig");
    var first_dir = try TestDirectory.init("relational-maintenance-primary");
    defer first_dir.cleanup();
    var second_dir = try TestDirectory.init("relational-maintenance-replica");
    defer second_dir.cleanup();
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 7, .shard_id = 71, .range_id = 71 }, .start_index_workers = false, .start_optional_runtimes = false };
    var first = try db_mod.DB.open(alloc, first_dir.path(), options);
    defer first.close();
    var second = try db_mod.DB.open(alloc, second_dir.path(), options);
    defer second.close();
    for ([_]*db_mod.DB{ &first, &second }) |db| {
        try install(db, 1, true);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":7,\"payload\":\"data\"}" }} });
        _ = try ready(db);
    }
    const command = blk: {
        var pinned = first.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const index = pinned.plan.boundIndexes()[0];
        var txn = try first.core.store.beginWriteTxn();
        errdefer txn.abort();
        var failed = try jobs.status(&txn, index);
        failed.state = .failed;
        failed.failure = .invalid_row;
        const bytes = try failed.encode(alloc);
        defer alloc.free(bytes);
        try txn.put(&jobs.progressKey(index.id()), bytes);
        try txn.commit();
        break :blk control.Command{ .action = .retry, .table_id = 7, .owner_group_id = 71, .schema_version = 1, .index_name = "tenant_id", .generation = index.generation, .slot = index.slot, .owner = failed.owner, .comparison = failed.comparison, .expected_progress_digest = control.progressDigest(bytes), .expected_maintenance_epoch = 0, .routing_key = "" };
    };
    const id = records.Id{ .generation = command.generation, .slot = command.slot };
    // Different replica-local progress MUST NOT affect deterministic replicated
    // prepare: both replicas accept the same desired control and independently rebuild.
    for ([_]*db_mod.DB{ &first, &second }) |db| {
        const txn_id = try db.beginTransaction(1000);
        try db.writeTransaction(txn_id, .{ .relational_index_maintenance = command });
        try db.commitTransaction(txn_id, 2000);
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        _ = try ready(db);
        const repeated = try db.beginTransaction(3000);
        try db.writeTransaction(repeated, .{ .relational_index_maintenance = command });
        try db.commitTransaction(repeated, 4000);
        try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus("tenant_id")).state);
        var read = try db.core.store.beginProbeTxn();
        defer read.abort();
        const receipt = try control.readControl(&read, id);
        try std.testing.expectEqual(@as(u64, 1), receipt.epoch);
        try std.testing.expectEqual(command.fingerprint(), receipt.last_request);
    }
    var stale = command;
    stale.expected_progress_digest[0] ^= 1;
    const rejected = try first.beginTransaction(5000);
    try std.testing.expectError(error.PreparedGenerationChanged, first.writeTransaction(rejected, .{ .relational_index_maintenance = stale }));
    try first.abortTransaction(rejected, 6000);
    var wrong_table = command;
    wrong_table.table_id += 1;
    const wrong = try first.beginTransaction(7000);
    try std.testing.expectError(error.PreparedGenerationChanged, first.writeTransaction(wrong, .{ .relational_index_maintenance = wrong_table }));
    try first.abortTransaction(wrong, 8000);
    const value = try (control.Control{ .epoch = 2, .last_request = @splat(1) }).encode(alloc);
    defer alloc.free(value);
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, first.batch(.{ .writes = &.{.{ .key = &control.controlKey(id), .value = value }} }));
    // Local workers cannot publish through a pending replicated control intent.
    var repair_command = command;
    repair_command.action = .repair;
    repair_command.expected_maintenance_epoch = 1;
    const repair_txn = try first.beginTransaction(9000);
    try first.writeTransaction(repair_txn, .{ .relational_index_maintenance = repair_command });
    try first.commitTransaction(repair_txn, 10000);
    var pending_page = (try jobs.Page.prepare(alloc, std.testing.io, first.core, "tenant_id", .{ .records = 1 })).?;
    defer pending_page.deinit();
    var obsolete_page = (try jobs.Page.prepare(alloc, std.testing.io, first.core, "tenant_id", .{ .records = 1 })).?;
    defer obsolete_page.deinit();
    repair_command.expected_maintenance_epoch = 2;
    const next_repair = try first.beginTransaction(11000);
    try first.writeTransaction(next_repair, .{ .relational_index_maintenance = repair_command });
    try std.testing.expectError(error.IntentConflict, pending_page.commit(first.core));
    try first.commitTransaction(next_repair, 12000);
    try std.testing.expectError(error.PreparedGenerationChanged, obsolete_page.commit(first.core));
    _ = try ready(&first);
    first.close();
    first = try db_mod.DB.open(alloc, first_dir.path(), options);
    var read = try first.core.store.beginProbeTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 3), (try control.readControl(&read, id)).epoch);
}

fn install(db: *db_mod.DB, version: u32, indexed: bool) !void {
    const encoded = try schema(version, indexed);
    defer alloc.free(encoded);
    try db.setSchemaJson(alloc, encoded);
}

fn write(db: *db_mod.DB, id: usize, tenant: usize) !void {
    var key: [64]u8 = undefined;
    var value: [160]u8 = undefined;
    try db.batch(.{ .writes = &.{.{
        .key = try std.fmt.bufPrint(&key, "doc:{d:0>8}", .{id}),
        .value = try std.fmt.bufPrint(&value, "{{\"tenant\":{d},\"id\":{d},\"payload\":\"unchanged\"}}", .{ tenant, id }),
    }} });
}

fn ready(db: *db_mod.DB) !usize {
    return readyIndex(db, "tenant_id");
}

fn readyIndex(db: *db_mod.DB, name: []const u8) !usize {
    for (0..2048) |pages| {
        if ((try db.relationalIndexBuildStatus(name)).state == .ready) return pages;
        _ = try db.runRelationalIndexMaintenancePass();
    }
    return error.IndexBuildDidNotConverge;
}

const Scan = struct { count: usize = 0, examined: usize = 0, bytes: usize = 0 };
fn scan(db: *db_mod.DB, indexed: bool) !Scan {
    var reader = try db.beginRelationalRows(alloc, .{
        .index = if (indexed) "tenant_id" else null,
        .lower = if (indexed) .{ .values = &.{.{ .integer = 1 }} } else null,
        .upper = if (indexed) .{ .values = &.{.{ .integer = 1 }} } else null,
        .fields = &.{"id"},
        .conditions = if (indexed) &.{} else &.{.{ .column = "tenant", .op = .eq, .value = .{ .integer = 1 } }},
    });
    defer reader.deinit();
    var result: Scan = .{};
    for (0..4096) |_| {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 32, .records = 64, .output_bytes = 64 * 1024 });
        defer page.deinit();
        result.count += page.rows.len;
        result.examined += page.records_examined;
        result.bytes += page.output_bytes;
        if (!page.more) return result;
    }
    return error.RowScanDidNotConverge;
}

fn replay(primary: *primary_mod.Primary, replica: *db_mod.DB, next: *u64) !void {
    while (next.* <= primary.lastLsn()) : (next.* += 1) {
        var entry = (try primary.log.entryAt(alloc, next.*)) orelse return error.MissingReplicationRecord;
        defer entry.deinit(alloc);
        try replication_ingress.applyRecord(replica, entry.record);
        try replication_ingress.applyRecord(replica, entry.record);
    }
}

test "relational index system standby replays schema churn and rebuilds ready generations after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", owned);
    const source_path = try std.fmt.allocPrint(owned, "{s}/source", .{root});
    const replica_path = try std.fmt.allocPrint(owned, "{s}/replica", .{root});
    var primary = try primary_mod.Primary.open(alloc, try std.fmt.allocPrintSentinel(owned, "{s}/log", .{root}, 0), try std.fmt.allocPrintSentinel(owned, "{s}/slots", .{root}, 0), .{ .cluster_id = 71, .table_id = 7, .shard_id = 71, .timeline_id = 1, .epoch = 1 }, .{});
    defer primary.close();
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{ .flush_threshold = 32 } }, .identity_namespace = .{ .table_id = 7, .shard_id = 71, .range_id = 71 }, .start_index_workers = false, .start_optional_runtimes = false };
    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    var mirrored = options;
    mirrored.replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{ .last_lsn = &last_lsn, .failure_count = &failures });
    mirrored.replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{ .sync_policy = .{ .mode = .async } });
    var source = try db_mod.DB.open(alloc, source_path, mirrored);
    defer source.close();
    var replica = try db_mod.DB.open(alloc, replica_path, options);
    var replica_open = true;
    defer if (replica_open) replica.close();
    var next: u64 = 1;
    try install(&source, 1, false);
    for (0..16) |id| try write(&source, id, id % 4);
    try install(&source, 2, true);
    try replay(&primary, &replica, &next);
    _ = try ready(&source);
    _ = try ready(&replica);
    try std.testing.expectEqual(@as(usize, 4), (try scan(&replica, true)).count);
    const first = try replica.relationalIndexBuildStatus("tenant_id");
    // Desired maintenance is replicated through the same durable outbox as
    // ordinary mutations; replay never copies replica-local build progress.
    const control = @import("relational_index_maintenance_contract.zig");
    const jobs = @import("relational_index_jobs.zig");
    const repair_command = blk: {
        var pinned = source.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const index = pinned.plan.boundIndexes()[0];
        var read = try source.core.store.beginProbeTxn();
        defer read.abort();
        const proof = try jobs.statusProofWithOwnership(&read, index, try jobs.ownership(&read));
        break :blk control.Command{ .action = .repair, .table_id = 7, .owner_group_id = 71, .schema_version = 2, .index_name = "tenant_id", .generation = index.generation, .slot = index.slot, .owner = proof.progress.owner, .comparison = proof.progress.comparison, .expected_progress_digest = proof.digest, .expected_maintenance_epoch = proof.maintenance_epoch, .routing_key = "" };
    };
    const repair_txn = try source.beginTransaction(1000);
    try source.writeTransaction(repair_txn, .{ .relational_index_maintenance = repair_command });
    try source.commitTransaction(repair_txn, 2000);
    try replay(&primary, &replica, &next);
    try std.testing.expectEqual(.building, (try replica.relationalIndexBuildStatus("tenant_id")).state);
    {
        var read = try replica.core.store.beginProbeTxn();
        defer read.abort();
        const ticket = try control.readControl(&read, .{ .generation = repair_command.generation, .slot = repair_command.slot });
        try std.testing.expectEqual(@as(u64, 1), ticket.epoch);
        try std.testing.expectEqual(repair_command.fingerprint(), ticket.last_request);
    }
    _ = try ready(&source);
    _ = try ready(&replica);
    try write(&source, 1, 2);
    try source.batch(.{ .deletes = &.{"doc:00000005"} });
    try install(&source, 3, false);
    try install(&source, 4, true);
    try replay(&primary, &replica, &next);
    replica.close();
    replica_open = false;
    replica = try db_mod.DB.open(alloc, replica_path, options);
    replica_open = true;
    _ = try ready(&replica);
    const current = try replica.relationalIndexBuildStatus("tenant_id");
    try std.testing.expect(current.generation > first.generation);
    try std.testing.expectEqual(@as(usize, 2), (try scan(&replica, true)).count);
    try std.testing.expectEqual(next - 1, try replica.replicationAppliedSequence());
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    // The recovered replica's active plan must also serve fresh primary writes;
    // replay completion alone is insufficient evidence of a usable write plan.
    try write(&replica, 99, 1);
    try std.testing.expectEqual(@as(usize, 3), (try scan(&replica, true)).count);
}

test "relational index system primary prefix scans preserve snapshots bounds and failed pages" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("primary-prefix-scan");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    const keys = [_][]const u8{ "a", "a\x00", "a\x00\xff", "aa", "\xff", "\xff\x00" };
    for (keys) |key| try db.batch(.{ .writes = &.{.{ .key = key, .value = "{\"tenant\":1,\"id\":1}" }} });
    // A leftover companion with no primary must consume only one work unit.
    const orphan = try internal.relationalRowKeyAlloc(alloc, "0-orphan");
    defer alloc.free(orphan);
    orphan[orphan.len - 1] = internal.relational_row_kind + 1;
    try db.core.store.put(orphan, "orphan");
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"} });
    defer reader.deinit();
    var bounded = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"}, .primary_lower = .{ .key = "a", .inclusive = false }, .primary_upper = .{ .key = "\xff", .inclusive = true } });
    defer bounded.deinit();
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"tenant\":1,\"id\":2}" }, .{ .key = "z", .value = "{\"tenant\":1,\"id\":3}" } }, .deletes = &.{"aa"} });
    var none: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&none);
    try std.testing.expectError(error.OutOfMemory, reader.nextPage(fixed.allocator(), null, .{ .records = 1 }));
    try std.testing.expectEqual(@as(usize, 0), reader.after.items.len);
    {
        var empty = try reader.nextPage(alloc, null, .{ .records = 1 });
        defer empty.deinit();
        try std.testing.expect(empty.more and empty.rows.len == 0 and empty.records_examined == 1);
    }
    try std.testing.expectError(error.RelationalRowResultTooLarge, reader.nextPage(alloc, null, .{ .output_bytes = 1 }));
    var count: usize = 0;
    var examined: usize = 1;
    while (true) {
        var page = try reader.nextPage(alloc, null, .{ .records = 1 });
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqualStrings(keys[count], row.key);
            try std.testing.expectEqualStrings("{\"id\":1}", row.json);
            count += 1;
        }
        examined += page.records_examined;
        if (!page.more) break;
    }
    try std.testing.expectEqual(keys.len, count);
    try std.testing.expectEqual(keys.len + 1, examined);
    var page = try bounded.nextPage(alloc, null, .{ .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 4), page.rows.len);
    for (page.rows, keys[1..5]) |row, key| try std.testing.expectEqualStrings(key, row.key);
}

test "relational index system LSM build work is linear in rows times indexes" {
    const jobs = @import("relational_index_jobs.zig");
    const count = 64;
    for ([_]usize{ 1, 8, 32 }) |index_count| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-build-scaling");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{ .flush_threshold = 128 } }, .start_index_workers = false, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        const single = try schema(1, true);
        defer alloc.free(single);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, single, .{});
        defer parsed.deinit();
        const owned = parsed.arena.allocator();
        const indexes = parsed.value.object.getPtr("relational_indexes").?;
        const template = indexes.array.items[0];
        for (1..index_count) |i| {
            var item = template;
            item.object = try template.object.clone(owned);
            try item.object.put(owned, "name", .{ .string = try std.fmt.allocPrint(owned, "index_{d}", .{i}) });
            try indexes.array.append(item);
        }
        const checked = try std.json.parseFromSliceLeaky(std.json.Value, owned, "[{\"name\":\"nonnegative\",\"column\":\"id\",\"op\":\"gte\",\"value\":0}]", .{});
        try parsed.value.object.put(owned, "checks", checked);
        // object.put can relocate its entries; retain the array value itself.
        const definitions = parsed.value.object.get("relational_indexes").?.array.items;
        const declaration = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        defer alloc.free(declaration);
        try db.setSchemaJson(alloc, declaration);
        const edge_keys = [_][]const u8{ "a", "a\x00", "a\x00\xff", "aa" };
        for (0..count) |i| {
            if (i < edge_keys.len) {
                try db.batch(.{ .writes = &.{.{ .key = edge_keys[i], .value = "{\"tenant\":1,\"id\":1}" }} });
            } else try write(&db, i, i % 4);
        }
        // Reopen against persisted LSM data. Every unrelated generation is
        // populated before measuring even the first target's verification.
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        const started = time.monotonicNs();
        var examined: usize = 0;
        for (definitions) |item| {
            const name = item.object.get("name").?.string;
            var target_examined: usize = 0;
            for (0..256) |_| {
                var page = (try jobs.Page.prepare(alloc, std.testing.io, db.core, name, .{ .records = 7 })) orelse break;
                defer page.deinit();
                target_examined += page.records_examined;
                try page.commit(db.core);
            } else return error.IndexBuildDidNotConverge;
            try std.testing.expectEqual(@as(usize, 3 * count), target_examined);
            try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus(name)).state);
            examined += target_examined;
        }
        try std.testing.expectEqual(3 * count * index_count, examined);
        const build_elapsed = time.monotonicNs() - started;
        var primary_rows: usize = 0;
        var primary_records: usize = 0;
        {
            var primary_reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"} });
            defer primary_reader.deinit();
            while (true) {
                var page = try primary_reader.nextPage(alloc, std.testing.io, .{ .rows = 1, .records = 1 });
                defer page.deinit();
                primary_rows += page.rows.len;
                primary_records += page.records_examined;
                if (!page.more) break;
            }
        }
        try std.testing.expectEqual(count, primary_rows);
        try std.testing.expectEqual(count, primary_records);
        const checks = @import("relational_constraint_jobs.zig");
        var check_records: usize = 0;
        while (try checks.Page.prepare(alloc, std.testing.io, db.core, .{ .records = 7 })) |prepared| {
            {
                var page = prepared;
                defer page.deinit();
                check_records += page.records_examined;
                try page.commit(db.core);
            }
            // CHECK continuation is durable and resumes after the whole family.
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), options);
        }
        try std.testing.expectEqual(count, check_records);
        for (0..256) |_| {
            _ = try db.runRelationalIndexMaintenancePass();
            if (!db.local_execution.relational_index_maintenance_sweep.isPending()) break;
        } else return error.MaintenanceDidNotBecomeIdle;
        try std.testing.expect(!try db.runRelationalIndexMaintenancePass() or index_count > 16);
        std.debug.print("LSM primary/CHECK scans indexes={d} rows={d} primary_records={d} check_records={d}\n", .{ index_count, count, primary_records, check_records });
        std.debug.print("LSM generation-local build indexes={d} rows={d} examined={d} elapsed_us={d}\n", .{ index_count, count, examined, build_elapsed / 1000 });
    }
}

test "relational index system LSM write rebuild query and churn work benchmark" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/lsm", .{root});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{ .flush_threshold = 128 } }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    const count = 256;
    try install(&db, 1, false);
    const start = time.monotonicNs();
    for (0..count) |id| try write(&db, id, id % 16);
    const write_ns = time.monotonicNs() - start;
    const build_start = time.monotonicNs();
    try install(&db, 2, true);
    const build_pages = try ready(&db);
    const build_ns = time.monotonicNs() - build_start;
    const primary_scan = try scan(&db, false);
    const index_scan = try scan(&db, true);
    try std.testing.expectEqual(@as(usize, count / 16), index_scan.count);
    try std.testing.expectEqual(primary_scan.count, index_scan.count);
    try std.testing.expect(index_scan.examined < primary_scan.examined);
    const churn_start = time.monotonicNs();
    for (0..4) |cycle| {
        for (0..64) |id| try write(&db, id, (id + cycle) % 16);
        try install(&db, @intCast(3 + cycle * 2), false);
        try install(&db, @intCast(4 + cycle * 2), true);
        _ = try ready(&db);
        for (0..2048) |_| {
            if (!try db.runRelationalIndexMaintenancePass()) break;
        } else return error.IndexGarbageDidNotConverge;
    }
    const churn_ns = time.monotonicNs() - churn_start;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var forward: usize = 0;
    var reverse: usize = 0;
    var logical_bytes: usize = 0;
    var item = try cursor.seekAtOrAfter("");
    while (item) |entry| : (item = try cursor.next()) {
        if (records.isForwardKey(entry.key)) forward += 1;
        if (internal.isRelationalIndexReverseKey(entry.key)) reverse += 1;
        logical_bytes += entry.key.len + entry.value.len;
    }
    try std.testing.expectEqual(@as(usize, count), forward);
    try std.testing.expectEqual(@as(usize, count), reverse);
    const backend = db.core.primary_store_owner.lsmBackend() orelse return error.ExpectedLsmBackend;
    const usage = try backend.measurePhysicalUsage();
    const after = try scan(&db, true);
    try std.testing.expect(after.examined <= index_scan.examined + 1);
    std.debug.print("relational-index benchmark rows={} write_ns={} build_ns={} build_pages={} primary_records={} index_records={} churn_ns={} forward={} reverse={} logical_bytes={} active_sst_bytes={} obsolete_bytes={} wal_bytes={}\n", .{ count, write_ns, build_ns, build_pages, primary_scan.examined, index_scan.examined, churn_ns, forward, reverse, logical_bytes, usage.active_sst_bytes, usage.obsolete_file_bytes, usage.wal_retained_bytes });
}

test "relational index system runUntilIdle drains builds across maintenance slices without workers" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-idle-drain");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const base =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, base);
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"n\":7}" }} });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var declaration = try std.json.parseFromSliceLeaky(std.json.Value, a, base, .{});
    try declaration.object.put(a, "version", .{ .integer = 2 });
    var indexes: std.array_list.Managed(std.json.Value) = .init(a);
    for (0..20) |i| {
        const definition = try std.fmt.allocPrint(a, "{{\"name\":\"by_n_{d}\",\"keys\":[{{\"column\":\"n\"}}]}}", .{i});
        try indexes.append(try std.json.parseFromSliceLeaky(std.json.Value, a, definition, .{}));
    }
    try declaration.object.put(a, "relational_indexes", .{ .array = indexes });
    try db.setSchemaJson(alloc, try std.json.Stringify.valueAlloc(a, declaration, .{}));
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("by_n_0")).state);
    try std.testing.expectError(error.DeadlineExceeded, db.runRelationalIndexMaintenanceUntilIdle(.none, time.monotonicNs()));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, db.runRelationalIndexMaintenanceUntilIdle(db_mod.types.CancellationToken.fromAtomic(&canceled), null));
    try std.testing.expectError(error.DeadlineExceeded, db.ensureRelationalIndexesReady(&.{"by_n_19"}, .none, time.monotonicNs()));
    try std.testing.expectError(error.Canceled, db.ensureRelationalIndexesReady(&.{"by_n_19"}, db_mod.types.CancellationToken.fromAtomic(&canceled), null));
    try db.ensureRelationalIndexesReady(&.{"by_n_19"}, .none, null);
    try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus("by_n_19")).state);
    // Targeted admission neither builds unrelated indexes nor requires a
    // clean sweep. Background ownership advances one page and returns debt.
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("by_n_0")).state);
    try std.testing.expect(try db.runBackgroundMaintenanceWithCancellation(.none));
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("by_n_18")).state);
    try db.runUntilIdle();
    for (0..20) |i| {
        const name = try std.fmt.allocPrint(a, "by_n_{d}", .{i});
        try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus(name)).state);
    }
    // A clean sweep may need multiple slices even after builds are ready.
    for (0..8) |_| {
        if (!try db.runBackgroundMaintenanceWithCancellation(.none)) break;
    } else return error.IndexMaintenanceDidNotConverge;
    var reader = try db.beginRelationalRows(alloc, .{ .index = "by_n_19" });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, db.backend_runtime.io(), .{ .rows = 10 });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
}

test "relational index system query lease typed point hydration retains nulls numbers and snapshot" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("query-typed-points");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"payload":{"type":"json"},"optional":{"type":["json","null"]}},"required":["payload"],"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"payload\":null,\"optional\":null}", .json_null_fields = &.{"payload"} }} });
    var reader = captured: {
        var lease = try db.beginQueryReadLease();
        defer lease.release();
        break :captured try lease.relationalRows(alloc, &.{ "payload", "optional" }, 1);
    };
    defer reader.deinit();
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"payload\":{\"fraction\":0.123456789012345678901},\"optional\":null}" }} });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const retained = (try reader.lookupTypedRow(a, "a")).?;
    try std.testing.expectEqualSlices(bool, &.{ false, true }, retained.sql_nulls.?);
    try std.testing.expect(retained.typed.?.object.get("payload").? == .null);
    try std.testing.expect(try reader.lookupTypedRow(a, "missing") == null);
    var current = try db.beginRelationalRows(alloc, .{ .fields = &.{ "payload", "optional" } });
    defer current.deinit();
    const updated = (try current.lookupTypedRow(a, "a")).?;
    try std.testing.expectEqualStrings("0.123456789012345678901", updated.typed.?.object.get("payload").?.object.get("fraction").?.number_string);
}
