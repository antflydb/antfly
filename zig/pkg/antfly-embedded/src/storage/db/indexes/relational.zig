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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;
const Indexes = @import("mod.zig").Indexes;

/// Relational column index state owned by `DB.indexes`: column maintenance and the rewrite program and tail caches.
pub const RelationalIndexes = struct {
    rewrite_program_cache: @import("../../rewrite_program_cache.zig").Cache = .{},
    rewrite_tail_cache: @import("../../rewrite_tail_spool.zig").Cache = .{},
    columns_building: std.atomic.Value(bool) = .init(false),
    columns_rebuild_requested: std.atomic.Value(bool) = .init(false),
    column_maintenance: db_mod.relational_columns.Maintenance = .{},

    fn dbPtr(self: *RelationalIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("relational", self));
        return parent.dbPtr();
    }

    pub fn runColumnMaintenancePass(self: *RelationalIndexes) !usize {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runRelationalColumnMaintenancePass", .{db});
        const started = db.independentMaintenanceNowNs();
        // Artifact repair can keep the shared worker on its active cadence;
        // enforce columnar backoff independently of that worker's sleep.
        if (started < self.column_maintenance.retry_after_ns.load(.acquire)) return 0;
        var completed: usize = 0;
        while (completed < 8) : (completed += 1) {
            const changed = self.rebuildColumnsWithPolicy(true) catch |err| {
                // Resource pressure backs off to the idle cadence; it must not
                // create a 100 ms retry storm or masquerade as a clean table.
                self.column_maintenance.notePending(true);
                self.column_maintenance.backing_off.store(true, .release);
                self.column_maintenance.retry_after_ns.store(db.independentMaintenanceNowNs() +| DB.artifact_repair_metadata_poll_ns, .release);
                return err;
            };
            if (!changed) break;
            if (db.independentMaintenanceNowNs() -| started >= 50 * std.time.ns_per_ms) return completed + 1;
        }
        return completed;
    }

    /// Build disposable table-owned column blocks without holding the apply
    /// lock. The store's atomic primary-mutation fence governs publication.
    pub fn rebuildColumns(self: *RelationalIndexes) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("rebuildRelationalColumns", .{db});
        return self.rebuildColumnsWithPolicy(false);
    }

    fn rebuildColumnsWithPolicy(self: *RelationalIndexes, adaptive: bool) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        if (db.maintenance.independent.stopping.load(.acquire)) return error.Canceled;
        var view = db.core.acquireSchemaView() orelse return false;
        defer view.release();
        if (view.storageMode() != .relational) return false;
        if (self.columns_building.swap(true, .acq_rel)) return false;
        defer self.columns_building.store(false, .release);
        const started = db.independentMaintenanceNowNs();
        _ = self.column_maintenance.passes.fetchAdd(1, .monotonic);
        defer self.column_maintenance.last_pass_ns.store(db.independentMaintenanceNowNs() -| started, .monotonic);
        errdefer _ = self.column_maintenance.failures.fetchAdd(1, .monotonic);
        var preparation: db_mod.RequestPreparationContext = undefined;
        preparation.init(db);
        defer preparation.deinit();
        const force = self.columns_rebuild_requested.swap(false, .acq_rel);
        errdefer if (force) self.columns_rebuild_requested.store(true, .release);
        const changed = db_mod.relational_columns.rebuild(db, preparation.guard.allocator(), force, adaptive) catch |err| {
            switch (err) {
                error.InvalidColumnSegment, error.InvalidData, error.NotFound => self.columns_rebuild_requested.store(true, .release),
                else => {},
            }
            return preparation.mapError(err);
        };
        self.column_maintenance.notePending(changed or self.column_maintenance.waiting_until_ns.load(.acquire) != 0);
        self.column_maintenance.backing_off.store(false, .release);
        self.column_maintenance.retry_after_ns.store(0, .release);
        return changed;
    }
};

test "relational columnar dirty scans intersect query and shard bounds before decoding" {
    const alloc = std.testing.allocator;
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"n\":1}" },
            .{ .key = "b", .value = "{\"n\":2}" },
            .{ .key = "b\x00", .value = "{\"n\":3}" },
            .{ .key = "z", .value = "{\"n\":4}" },
        } });
        try std.testing.expect(try db.indexes.relational.rebuildColumns());
        for ([_][]const u8{ "a", "z" }) |id| {
            const key = try db_mod.internal_keys.relationalRowKeyAlloc(alloc, id);
            defer alloc.free(key);
            try db.core.store.put(key, "corrupt outside query");
        }
        var stats: db_mod.types.ColumnarScanStats = .{};
        var result = try db.scan(alloc, "b", "b", .{ .inclusive_from = true, .columnar_stats = &stats });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.hashes.len);
        try std.testing.expectEqualStrings("b", result.hashes[0].id);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        // The shard end is exclusive; b\x00 must not be confused with b.
        try db.updateRange(.{ .start = "b", .end = "b\x00" });
        stats = .{};
        var shard = try db.scan(alloc, "", "", .{ .columnar_stats = &stats });
        defer shard.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), shard.hashes.len);
        try std.testing.expectEqualStrings("b", shard.hashes[0].id);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        var empty = try db.scan(alloc, "b", "b", .{ .inclusive_from = true, .exclusive_to = true });
        defer empty.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), empty.hashes.len);
    }
}

test "relational columnar existence and null projection do not fetch payload records" {
    // Assert payload/admission semantics independently of the real-time build
    // quantum. Bounded/resumable maintenance is tested separately.
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } } });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string, .allows_null = true }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const payload = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    const json = try std.fmt.allocPrint(alloc, "{{\"payload\":\"{s}\"}}", .{payload});
    defer alloc.free(json);
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = json }, .{ .key = "b", .value = "{\"payload\":null}" }, .{ .key = "c", .value = "{}" } } });
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    const payload_key = try db_mod.relational_columns.payloadKeyForTest(&db, alloc, 1, 0x200000000, 0);
    defer alloc.free(payload_key);
    try db.core.store.delete(payload_key);
    var stats: db_mod.types.ColumnarScanStats = .{};
    var exists = try db.scan(alloc, "", "", .{ .filter_query_json = "{\"exists\":{\"field\":\"payload\"}}", .columnar_stats = &stats });
    defer exists.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), exists.hashes.len);
    try std.testing.expectEqual(@as(u64, 0), stats.payload_bytes_read);
    try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
    try std.testing.expect(stats.metadata_bytes_read < 1024);
    stats = .{};
    var null_row = try db.scan(alloc, "b", "b", .{ .inclusive_from = true, .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &stats });
    defer null_row.deinit(alloc);
    try std.testing.expectEqualStrings("{\"payload\":null}", null_row.documents[0].json);
    try std.testing.expect(stats.used);
    try std.testing.expectEqual(@as(u64, 0), stats.logical_slots_initialized);
    try std.testing.expectEqual(@as(u64, 0), stats.payload_bytes_read);
    try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
    // A consumer that actually needs the missing payload still falls back.
    var projected = try db.scan(alloc, "a", "a", .{ .inclusive_from = true, .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} });
    defer projected.deinit(alloc);
    try std.testing.expectEqualStrings(json, projected.documents[0].json);
    try std.testing.expect(db.indexes.relational.columns_rebuild_requested.load(.acquire));
}

test "relational columnar adaptive maintenance resumes after empty coverage" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    for (0..3) |cycle| {
        for (0..64) |_| {
            _ = try db.indexes.relational.runColumnMaintenancePass();
            if (!db.indexes.relational.column_maintenance.pending.load(.acquire)) break;
        }
        try std.testing.expect(!db.indexes.relational.column_maintenance.pending.load(.acquire));
        if (cycle == 1) {
            try db.batch(.{ .deletes = &.{"row"} });
        } else try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"n\":1}" }} });
    }
    for (0..64) |_| {
        _ = try db.indexes.relational.runColumnMaintenancePass();
        if (!db.indexes.relational.column_maintenance.pending.load(.acquire)) break;
    }
    try std.testing.expect(!db.indexes.relational.column_maintenance.pending.load(.acquire));
    var scan_stats: db_mod.types.ColumnarScanStats = .{};
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &scan_stats });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.documents.len);
    try std.testing.expectEqual(@as(u64, 0), scan_stats.primary_rows_read);
}

test "relational columnar range admission ignores unrelated mutation epochs" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 1024);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try db_mod.drainTestRelationalMaintenance(&db);
    for (0..4) |i| try db.batch(.{ .writes = &.{.{ .key = writes[i * 256].key, .value = "{\"n\":2}" }} });
    db_mod.relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer db_mod.relational_columns.test_now_ns = null;
    for (0..8) |_| _ = try db.indexes.relational.runColumnMaintenancePass();
    const roots = db.indexes.relational.column_maintenance.admission_root_reads.load(.monotonic);
    const deferred = db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic);
    try std.testing.expect(roots >= 4);
    for (0..8) |i| {
        try db.batch(.{ .writes = &.{.{ .key = writes[0].key, .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d}}}", .{i + 3}) }} });
        _ = try db.indexes.relational.runColumnMaintenancePass();
    }
    try std.testing.expectEqual(roots, db.indexes.relational.column_maintenance.admission_root_reads.load(.monotonic));
    try std.testing.expectEqual(deferred + 8, db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic));
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    for (0..8) |_| _ = try db.indexes.relational.runColumnMaintenancePass();
    try std.testing.expectEqual(@as(u64, 0), db.indexes.relational.column_maintenance.admission_root_reads.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic));
}

test "relational columnar skew pages and sparse delta merges bound physical work" {
    const alloc = std.testing.allocator;
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string, .allows_null = true },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const large = try scratch.alloc(u8, 512 * 1024);
        var random = std.Random.DefaultPrng.init(123);
        for (large) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        const writes = try scratch.alloc(db_mod.types.BatchWrite, 128);
        for (writes, 0..) |*write, i| write.* = .{
            .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}),
            .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d},\"payload\":\"{s}\"}}", .{ i, if (i == 1) large else "small" }),
        };
        try db.batch(.{ .writes = writes });
        try db_mod.drainTestRelationalMaintenance(&db);
        for ([_][]const u8{ "", "{\"term\":{\"payload\":\"small\"}}" }) |filter| {
            var stats: db_mod.types.ColumnarScanStats = .{};
            var one = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .limit = 1, .filter_query_json = filter, .columnar_stats = &stats });
            defer one.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), one.documents.len);
            try std.testing.expectEqualStrings("{\"payload\":\"small\"}", one.documents[0].json);
            try std.testing.expectEqual(@as(u64, 1), stats.payload_pages_read);
            try std.testing.expect(stats.payload_bytes_read < 1024);
        }
        const primary_before = db.indexes.relational.column_maintenance.primary_rows_read.load(.monotonic);
        const covered_before = db.indexes.relational.column_maintenance.covered_rows_read.load(.monotonic);
        try db.batch(.{ .writes = &.{
            .{ .key = "k0000", .value = "{\"n\":-1,\"payload\":\"updated\"}" },
            .{ .key = "k0010a", .value = "{\"n\":500,\"payload\":null}" },
        }, .deletes = &.{ "k0001", "k0127" } });
        const options: db_mod.types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "payload" }, .include_content_hashes = true };
        var before = try db.scan(alloc, "", "", options);
        defer before.deinit(alloc);
        try db_mod.drainTestRelationalMaintenance(&db);
        try std.testing.expectEqual(@as(u64, 2), db.indexes.relational.column_maintenance.primary_rows_read.load(.monotonic) - primary_before);
        try std.testing.expect(db.indexes.relational.column_maintenance.covered_rows_read.load(.monotonic) > covered_before);
        var stats: db_mod.types.ColumnarScanStats = .{};
        var measured = options;
        measured.columnar_stats = &stats;
        var after = try db.scan(alloc, "", "", measured);
        defer after.deinit(alloc);
        try std.testing.expectEqualDeep(before.documents, after.documents);
        try std.testing.expectEqualDeep(before.hashes, after.hashes);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        try std.testing.expectEqual(@as(u64, 2 * 127), stats.cell_slots_initialized);
        try std.testing.expect(stats.cell_cache_hits > 127);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        var reopened = try db.scan(alloc, "", "", options);
        defer reopened.deinit(alloc);
        try std.testing.expectEqualDeep(after.documents, reopened.documents);
    }
}

test "relational columnar skewed partial payload reuse follows bytes not row count" {
    const alloc = std.testing.allocator;
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var random = std.Random.DefaultPrng.init(905);
        var large: [15 * 1024]u8 = undefined;
        for (&large) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        var writes: [64]db_mod.types.BatchWrite = undefined;
        var deletes: [63][]const u8 = undefined;
        for (&writes, 0..) |*write, i| {
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"payload\":\"{s}\"}}", .{if (i == 0) &large else "x"}) };
            if (i != 0) deletes[i - 1] = write.key;
        }
        try db.batch(.{ .writes = &writes });
        try db_mod.drainTestRelationalMaintenance(&db);
        const encoded_before = db.indexes.relational.column_maintenance.payload_encoding_bytes.load(.monotonic);
        try db.batch(.{ .deletes = &deletes });
        try db_mod.drainTestRelationalMaintenance(&db);
        // One of 64 rows survives, but it owns almost all payload bytes.
        try std.testing.expectEqual(encoded_before, db.indexes.relational.column_maintenance.payload_encoding_bytes.load(.monotonic));
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.documents.len);
        try std.testing.expectEqualStrings(writes[0].value, result.documents[0].json);
    }
}

test "relational columnar shared pages bound alternating merges and survive reclamation" {
    const alloc = std.testing.allocator;
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const large = try scratch.alloc(u8, 512 * 1024);
        var random = std.Random.DefaultPrng.init(312);
        for (large) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        var writes: [128]db_mod.types.BatchWrite = undefined;
        var updates: [64]db_mod.types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| {
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"n\":0,\"payload\":\"{s}\"}}", .{if (i == 1) large else "small"}) };
            if (i % 2 == 0) updates[i / 2] = .{ .key = write.key, .value = "{\"n\":1,\"payload\":\"small\"}" };
        }
        try db.batch(.{ .writes = &writes });
        try db_mod.drainTestRelationalMaintenance(&db);
        const before = db.indexes.relational.column_maintenance.snapshot();
        try db.batch(.{ .writes = &updates });
        try db_mod.drainTestRelationalMaintenance(&db);
        const after = db.indexes.relational.column_maintenance.snapshot();
        try std.testing.expect(after.cell_slots_examined - before.cell_slots_examined <= 2 * 128);
        try std.testing.expect(after.payload_bytes_written - before.payload_bytes_written < 4096);
        try std.testing.expect(after.payloads_reused > before.payloads_reused);
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        // A full-row scalar update must also deduplicate the unchanged wide
        // singleton payload instead of rewriting it under a new block key.
        const wide_update = try std.fmt.allocPrint(scratch, "{{\"n\":9,\"payload\":\"{s}\"}}", .{large});
        try db.batch(.{ .writes = &.{.{ .key = "k0001", .value = wide_update }} });
        try db_mod.drainTestRelationalMaintenance(&db);
        try std.testing.expect(db.indexes.relational.column_maintenance.payload_bytes_written.load(.monotonic) - after.payload_bytes_written < 4096);
        try std.testing.expect(db.indexes.relational.column_maintenance.payload_encoding_bytes.load(.monotonic) - after.payload_encoding_bytes < 4096);
        // Canceled staging owns references until durable GC removes its column
        // metadata. Both the unpublished and drained states must balance.
        const Cancel = struct {
            fn run(_: *anyopaque) anyerror!void {
                return error.Canceled;
            }
        };
        try db.batch(.{ .writes = &.{.{ .key = "k0000", .value = "{\"n\":2,\"payload\":\"small\"}" }} });
        db_mod.relational_columns.test_before_publish = .{ .context = &db, .run = Cancel.run };
        defer db_mod.relational_columns.test_before_publish = null;
        try std.testing.expectError(error.Canceled, db.indexes.relational.rebuildColumns());
        db_mod.relational_columns.test_before_publish = null;
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        try db_mod.drainTestRelationalMaintenance(&db);
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        const options: db_mod.types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "payload" } };
        var baseline = try db.scan(alloc, "", "", options);
        defer baseline.deinit(alloc);
        const Snapshot = struct {
            db: *DB,
            documents: []const db_mod.types.ScanDocument,
            index: usize = 0,
            fn visit(ptr: ?*anyopaque, entry: db_mod.types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                if (self.index == 0) {
                    try self.db.batch(.{ .deletes = &.{"k0001"} });
                    try db_mod.drainTestRelationalMaintenance(self.db);
                }
                try std.testing.expectEqualStrings(self.documents[self.index].json, entry.document_json.?);
                self.index += 1;
            }
        };
        var snapshot = Snapshot{ .db = &db, .documents = baseline.documents };
        try db.scanVisit(alloc, "", "", options, .{ .context = &snapshot, .visit = Snapshot.visit });
        try std.testing.expectEqual(baseline.documents.len, snapshot.index);
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        var reopened = try db.scan(alloc, "", "", options);
        defer reopened.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 127), reopened.documents.len);
    }
}

test "relational columnar row cursor skips artifact fanout and preserves binary owners" {
    const alloc = std.testing.allocator;
    // Exercise both an uninterrupted bootstrap and deterministic partial
    // publication. Small published ranges may then be merged by maintenance.
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    defer db_mod.relational_columns.test_owner_limit = null;
    for ([_]?usize{ null, 2 }) |owner_limit| {
        for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
            db_mod.relational_columns.test_owner_limit = owner_limit;
            var path_tmp = try db_mod.TestDirectory.init("db");
            defer path_tmp.cleanup();
            const path = path_tmp.path().ptr;
            defer db_mod.cleanupTempDir(path);
            var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
            defer db.close();
            const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
            try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
            const owners = [_][]const u8{ "", "a", "a\x00", "a\xff", "orphan" };
            for (owners[0..4]) |owner| try db.batch(.{ .writes = &.{.{ .key = owner, .value = "{\"n\":1}" }} });
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            var batch = try db.core.store.beginWriteBatch();
            var live = true;
            defer if (live) batch.abort();
            for (owners) |owner| {
                const prefix_key = try db_mod.internal_keys.artifactRootPrefixAlloc(scratch, owner);
                for (0..2048) |i| try batch.asTxn().put(try std.fmt.allocPrint(scratch, "{s}{d:0>4}", .{ prefix_key, i }), "artifact payload is never a row");
            }
            try batch.commit();
            live = false;
            var stats: db_mod.types.ColumnarScanStats = .{};
            var primary = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true, .columnar_stats = &stats });
            defer primary.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 4), primary.documents.len);
            try std.testing.expectEqual(@as(u64, 5), stats.primary_owners_examined);
            for (primary.documents, owners[0..4]) |document, owner| try std.testing.expectEqualStrings(owner, document.id);
            stats = .{};
            var bounded = try db.scan(alloc, "a", "a\xff", .{ .include_documents = true, .include_all_fields = true, .inclusive_from = false, .exclusive_to = true, .columnar_stats = &stats });
            defer bounded.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), bounded.documents.len);
            try std.testing.expectEqualStrings("a\x00", bounded.documents[0].id);
            if (owner_limit) |limit| {
                try std.testing.expect(try db.indexes.relational.rebuildColumns());
                try std.testing.expectEqual(@as(u64, limit), db.indexes.relational.column_maintenance.owners_examined.load(.monotonic));
                // Force a bootstrap split, then restore the normal merge budget.
                db_mod.relational_columns.test_owner_limit = null;
            }
            try db_mod.drainTestRelationalMaintenance(&db);
            const maintenance = db.indexes.relational.column_maintenance.snapshot();
            try std.testing.expectEqual(@as(u64, 4), maintenance.primary_rows_read);
            // Owner visits include typed rows revisited by post-bootstrap merges;
            // only the five primary owners may be scanned, regardless of fanout.
            try std.testing.expectEqual(@as(u64, 5) + maintenance.covered_rows_read, maintenance.owners_examined);
            if (owner_limit == null) {
                try std.testing.expectEqual(@as(u64, 1), maintenance.bootstrap_quanta);
            } else {
                try std.testing.expect(maintenance.bootstrap_quanta > 1);
                try std.testing.expect(maintenance.covered_rows_read > 0);
            }
            var covered = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
            defer covered.deinit(alloc);
            try std.testing.expectEqualDeep(primary.documents, covered.documents);
        }
    }
}

test "relational columnar bootstrap yields across artifact-only owners" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |leading_row| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        try db.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":1}" }} });
        if (leading_row) try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var batch = try db.core.store.beginWriteBatch();
        var live = true;
        defer if (live) batch.abort();
        for (0..1500) |i| {
            const owner = try std.fmt.allocPrint(scratch, "o{d:0>4}", .{i});
            try batch.asTxn().put(try db_mod.internal_keys.artifactRootPrefixAlloc(scratch, owner), "orphan");
        }
        try batch.commit();
        live = false;
        try std.testing.expect(try db.indexes.relational.rebuildColumns());
        try std.testing.expect(db.indexes.relational.column_maintenance.owners_examined.load(.monotonic) <= 1024);
        try std.testing.expectEqual(@as(u64, @intFromBool(leading_row)), db.indexes.relational.column_maintenance.rows_written.load(.monotonic));
        var partial = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
        defer partial.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1) + @intFromBool(leading_row), partial.documents.len);
        try std.testing.expectEqualStrings("z", partial.documents[partial.documents.len - 1].id);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        try db_mod.drainTestRelationalMaintenance(&db);
        var scan_stats: db_mod.types.ColumnarScanStats = .{};
        var complete = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &scan_stats });
        defer complete.deinit(alloc);
        try std.testing.expectEqualDeep(partial.documents, complete.documents);
        try std.testing.expectEqual(@as(u64, 0), scan_stats.primary_owners_examined);
    }
}

test "relational columnar scheduler batches deferred discovery before ready work and persists timers" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 32 * 256);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>5}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try db_mod.drainTestRelationalMaintenance(&db);
    var updates = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    for (0..31) |i| try updates.append(scratch, .{ .key = writes[i * 256].key, .value = "{\"n\":2}" });
    for (writes[31 * 256 ..][0..64]) |write| try updates.append(scratch, .{ .key = write.key, .value = "{\"n\":2}" });
    try db.batch(.{ .writes = updates.items });
    db_mod.relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer db_mod.relational_columns.test_now_ns = null;
    const before = db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
    var turns: usize = 0;
    while (db.indexes.relational.column_maintenance.blocks_written.load(.monotonic) == before) : (turns += 1) {
        try std.testing.expect(turns < 8);
        _ = try db.indexes.relational.runColumnMaintenancePass();
    }
    try std.testing.expect(db.indexes.relational.column_maintenance.scheduler_candidates.load(.monotonic) >= 32);
    try std.testing.expect(db.indexes.relational.column_maintenance.scheduler_commits.load(.monotonic) < 16);
    // Finish discovery, but not the deferred builds. Repeated idle checks must
    // not rewrite the cursor or timer records for the unchanged backlog.
    for (0..8) |_| _ = try db.indexes.relational.runColumnMaintenancePass();
    const commits = db.indexes.relational.column_maintenance.scheduler_commits.load(.monotonic);
    for (0..4) |_| _ = try db.indexes.relational.runColumnMaintenancePass();
    try std.testing.expectEqual(commits, db.indexes.relational.column_maintenance.scheduler_commits.load(.monotonic));
    try std.testing.expect(db.indexes.relational.column_maintenance.pending.load(.acquire));
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    db_mod.relational_columns.test_now_ns = 105 * std.time.ns_per_s;
    for (0..4) |_| _ = try db.indexes.relational.runColumnMaintenancePass();
    try std.testing.expectEqual(@as(u64, 0), db.indexes.relational.column_maintenance.blocks_written.load(.monotonic));
    db_mod.relational_columns.test_now_ns = 111 * std.time.ns_per_s;
    for (0..64) |_| {
        _ = try db.indexes.relational.runColumnMaintenancePass();
        if (!db.indexes.relational.column_maintenance.pending.load(.acquire)) break;
    }
    try std.testing.expect(!db.indexes.relational.column_maintenance.pending.load(.acquire));
    try std.testing.expect(db.indexes.relational.column_maintenance.blocks_written.load(.monotonic) >= 31);
}

test "relational columnar scheduler keeps merge queue separate from due timers" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 300);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    // Bootstrap leaves an underfilled tail on the merge queue. Adaptive
    // discovery must not decode that queue record as a deferred-range timer.
    for (0..64) |_| {
        _ = try db.indexes.relational.runColumnMaintenancePass();
        if (!db.indexes.relational.column_maintenance.pending.load(.acquire)) break;
    }
    try std.testing.expect(!db.indexes.relational.column_maintenance.pending.load(.acquire));
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer result.deinit(alloc);
    try std.testing.expectEqual(writes.len, result.documents.len);
}

test "relational columnar bootstrap checkpoints coverage with fresh snapshots across restart" {
    const alloc = std.testing.allocator;
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(db_mod.types.BatchWrite, 600);
        for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        try db.batch(.{ .writes = writes });
        // Model authoritative rows predating column coverage/dirty records.
        var batch = try db.core.store.beginWriteBatch();
        var live = true;
        defer if (live) batch.abort();
        for (writes) |write| try batch.asTxn().delete(try std.mem.concat(scratch, u8, &.{ db_mod.internal_keys.relational_columnar_dirty_prefix, write.key }));
        try batch.commit();
        live = false;
        db_mod.relational_columns.test_compaction_block_limit = 1;
        defer db_mod.relational_columns.test_compaction_block_limit = null;
        const Race = struct {
            fn run(ptr: *anyopaque) !void {
                db_mod.relational_columns.test_before_publish = null;
                const owner: *DB = @ptrCast(@alignCast(ptr));
                try owner.batch(.{ .writes = &.{ .{ .key = "aa", .value = "{\"n\":9}" }, .{ .key = "k0002", .value = "{\"n\":9}" }, .{ .key = "k0599", .value = "{\"n\":9}" } } });
            }
        };
        db_mod.relational_columns.test_before_publish = .{ .context = &db, .run = Race.run };
        defer db_mod.relational_columns.test_before_publish = null;
        try std.testing.expect(try db.indexes.relational.rebuildColumns());
        const first_rows = db.indexes.relational.column_maintenance.rows_written.load(.monotonic);
        try std.testing.expect(first_rows > 0 and first_rows <= 256);
        var stats: db_mod.types.ColumnarScanStats = .{};
        var partial = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
        defer partial.deinit(alloc);
        var primary = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true });
        defer primary.deinit(alloc);
        try std.testing.expectEqualDeep(primary.documents, partial.documents);
        try std.testing.expectEqual(@as(usize, 601), partial.documents.len);
        try std.testing.expectEqual(@as(u64, 1), stats.uncovered_ranges_read);
        const Cancel = struct {
            fn run(_: *anyopaque) !void {
                db_mod.relational_columns.test_before_publish = null;
                return error.Canceled;
            }
        };
        db_mod.relational_columns.test_before_publish = .{ .context = &db, .run = Cancel.run };
        try std.testing.expectError(error.Canceled, db.indexes.relational.rebuildColumns());
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        // Reclaim canceled staging, then advance from the checkpoint, never
        // the prefix. The time budget may yield before the 256-row block cap.
        var recovery_passes: usize = 0;
        while (db.indexes.relational.column_maintenance.bootstrap_quanta.load(.monotonic) == 0) : (recovery_passes += 1) {
            try std.testing.expect(recovery_passes < 10);
            try std.testing.expect(try db.indexes.relational.rebuildColumns());
        }
        const resumed_rows = db.indexes.relational.column_maintenance.rows_written.load(.monotonic);
        try std.testing.expect(resumed_rows > 0 and resumed_rows <= 256);
        try db_mod.drainTestRelationalMaintenance(&db);
        stats = .{};
        var complete = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
        defer complete.deinit(alloc);
        try std.testing.expectEqualDeep(primary.documents, complete.documents);
        try std.testing.expectEqual(@as(u64, 0), stats.uncovered_ranges_read);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
    }
}

test "relational columnar missing bootstrap checkpoint cannot claim complete coverage" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 300);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    db_mod.relational_columns.test_compaction_block_limit = 1;
    defer db_mod.relational_columns.test_compaction_block_limit = null;
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    try db.core.store.delete("\x00\x00__columnar__:bootstrap");
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 300), result.documents.len);
    try std.testing.expect(db.indexes.relational.columns_rebuild_requested.load(.acquire));
    try db_mod.drainTestRelationalMaintenance(&db);
    try std.testing.expect(!db.indexes.relational.columns_rebuild_requested.load(.acquire));
}

test "relational columnar adaptive admission preserves age across hot updates and restart" {
    // Assert payload/admission semantics independently of the real-time build
    // quantum. Bounded/resumable maintenance is tested separately.
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    const alloc = std.testing.allocator;
    const Progress = struct {
        fn expectBuild(db: *DB, before: u64) !void {
            // Admission is time-sliced. A pass may checkpoint discovery before
            // it builds, especially under concurrent compiler/CI load. Keep
            // the synthetic age clock frozen while allowing bounded progress.
            for (0..16) |_| {
                _ = try db.indexes.relational.runColumnMaintenancePass();
                if (db.indexes.relational.column_maintenance.blocks_written.load(.monotonic) > before) return;
            }
            try std.testing.expect(db.indexes.relational.column_maintenance.blocks_written.load(.monotonic) > before);
        }
    };
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 256);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try db_mod.drainTestRelationalMaintenance(&db);
    db_mod.relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer db_mod.relational_columns.test_now_ns = null;
    const before = db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
    for (0..4) |i| {
        try db.batch(.{ .writes = &.{.{ .key = "k0000", .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d}}}", .{i + 2}) }} });
        _ = try db.indexes.relational.runColumnMaintenancePass();
    }
    try std.testing.expectEqual(before, db.indexes.relational.column_maintenance.blocks_written.load(.monotonic));
    try std.testing.expect(db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic) > 0);
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    db_mod.relational_columns.test_now_ns = 111 * std.time.ns_per_s;
    try Progress.expectBuild(&db, 0);
    // Read pressure can admit another sparse change before the age cap.
    try db.batch(.{ .writes = &.{.{ .key = "k0000", .value = "{\"n\":7}" }} });
    for (0..20) |_| {
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .limit = 1 });
        result.deinit(alloc);
    }
    const warm_before = db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
    try Progress.expectBuild(&db, warm_before);
    // High density bypasses age admission without any read-pressure hint.
    for (writes[0..64]) |*write| write.value = "{\"n\":9}";
    try db.batch(.{ .writes = writes[0..64] });
    const dense_before = db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
    try Progress.expectBuild(&db, dense_before);
    // A backwards clock correction must not strand durable deferral records.
    // Use a fresh clean range: prior read pressure and density-driven partial
    // page rewrites may legitimately admit another write without deferring it.
    var skew_tmp = try db_mod.TestDirectory.init("db");
    defer skew_tmp.cleanup();
    const skew_path = skew_tmp.path().ptr;
    defer db_mod.cleanupTempDir(skew_path);
    var skew_db = try DB.open(alloc, std.mem.span(skew_path), .{ .start_optional_runtimes = false });
    defer skew_db.close();
    try skew_db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    for (writes) |*write| write.value = "{\"n\":1}";
    try skew_db.batch(.{ .writes = writes });
    try db_mod.drainTestRelationalMaintenance(&skew_db);
    try skew_db.batch(.{ .writes = &.{.{ .key = "k0000", .value = "{\"n\":10}" }} });
    const deferred_before = skew_db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic);
    for (0..16) |_| {
        _ = try skew_db.indexes.relational.runColumnMaintenancePass();
        if (skew_db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic) > deferred_before) break;
    }
    // Do not move the clock back until a deferral exists at the old time.
    // Otherwise a yielded pass can first create it at 110s, which is not skew.
    try std.testing.expect(skew_db.indexes.relational.column_maintenance.ranges_deferred.load(.monotonic) > deferred_before);
    const skew_before = skew_db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
    db_mod.relational_columns.test_now_ns = 110 * std.time.ns_per_s;
    try Progress.expectBuild(&skew_db, skew_before);
}

test "relational columnar production LSM staging snapshot benchmark" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    defer db_mod.lsm_backend_mod.Backend.test_deep_mutable_snapshots = false;
    var clones: [2]u64 = undefined;
    var elapsed: [2]u64 = undefined;
    for (0..2) |mode| {
        db_mod.lsm_backend_mod.Backend.test_deep_mutable_snapshots = mode == 0;
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const wide = try scratch.alloc(u8, 4096);
        var random = std.Random.DefaultPrng.init(729);
        var writes: [1024]db_mod.types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| {
            for (wide) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"payload\":\"{s}\"}}", .{wide}) };
        }
        try db.batch(.{ .writes = &writes });
        const backend = db.core.primary_store_owner.lsmBackend().?;
        const before = backend.snapshotMaintenanceStats();
        const started = db_mod.platform_time.monotonicNs();
        try db_mod.drainTestRelationalMaintenance(&db);
        elapsed[mode] = db_mod.platform_time.monotonicNs() - started;
        clones[mode] = backend.snapshotMaintenanceStats().mutable_snapshot_clone_bytes_total - before.mutable_snapshot_clone_bytes_total;
        try std.testing.expect(db.indexes.relational.column_maintenance.blocks_written.load(.monotonic) >= 4);
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        var result = try db.scan(alloc, "", "", .{});
        defer result.deinit(alloc);
        try std.testing.expectEqual(writes.len, result.hashes.len);
    }
    try std.testing.expect(clones[1] < clones[0]);
    std.debug.print("\nproduction LSM staging: deep/shared snapshot ns={d}/{d}, copied bytes={d}/{d}\n", .{ elapsed[0], elapsed[1], clones[0], clones[1] });
}

test "relational columnar narrow projections load only selected metadata on wide rows" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const columns = try scratch.alloc(db_mod.schema_mod.RelationalColumn, 1024);
    var object = std.json.ObjectMap.empty;
    for (columns, 0..) |*column, i| {
        const name = try std.fmt.allocPrint(scratch, "f{d:0>4}", .{i});
        column.* = .{ .name = name, .path = name, .column_type = .integer };
        try object.put(scratch, name, .{ .integer = @intCast(i) });
    }
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = columns });
    const json = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = json }} });
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    // An unrequested metadata record is not read or decoded.
    try db.core.store.put("\x00\x00__columnar__:blocks:0000000000000001:0000000200000000:p00000001", "corrupt unrequested metadata");
    var stats: db_mod.types.ColumnarScanStats = .{};
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"f0000"}, .columnar_stats = &stats });
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("{\"f0000\":0}", result.documents[0].json);
    try std.testing.expectEqual(@as(u64, 1), stats.column_metadata_reads);
    try std.testing.expect(stats.metadata_bytes_read < 1024);
    try std.testing.expect(!db.indexes.relational.columns_rebuild_requested.load(.acquire));
    var corrupt = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"f0001"} });
    defer corrupt.deinit(alloc);
    try std.testing.expectEqualStrings("{\"f0001\":1}", corrupt.documents[0].json);
    try std.testing.expect(db.indexes.relational.columns_rebuild_requested.load(.acquire));
}

test "relational columnar coalescing progresses while a distant range stays hot" {
    const alloc = std.testing.allocator;
    // This fixture assumes four 256-row bootstrap blocks and a fixed number
    // of maintenance quanta; wall-clock scheduling must not change that shape.
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 1024);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    for (writes, 0..) |*write, i| {
        write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        if (i < 768 and i % 256 != 0) try deletes.append(scratch, write.key);
    }
    try db.batch(.{ .writes = writes });
    try db_mod.drainTestRelationalMaintenance(&db);
    try db.batch(.{ .deletes = deletes.items });
    for (0..3) |_| try std.testing.expect(try db.indexes.relational.rebuildColumns());
    const merged_before_restart = db.indexes.relational.column_maintenance.ranges_merged.load(.monotonic);
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    const Hot = struct {
        fn run(ptr: *anyopaque) !void {
            const owner: *DB = @ptrCast(@alignCast(ptr));
            const key = try db_mod.internal_keys.relationalRowKeyAlloc(owner.alloc, "k0768");
            defer owner.alloc.free(key);
            const bytes = try owner.core.store.get(owner.alloc, key);
            defer owner.alloc.free(bytes);
            // Even an identical committed rewrite receives a fresh mutation ID.
            try owner.core.store.put(key, bytes);
        }
    };
    try Hot.run(&db);
    db_mod.relational_columns.test_before_publish = .{ .context = &db, .run = Hot.run };
    defer db_mod.relational_columns.test_before_publish = null;
    // Retirement is now its own bounded quantum. Compare the same number of
    // publications while also bounding the intervening reclamation work.
    var turns: usize = 0;
    while (db.indexes.relational.column_maintenance.ranges_compacted.load(.monotonic) < 16) {
        _ = try db.indexes.relational.rebuildColumns();
        turns += 1;
        try std.testing.expect(turns < 160);
    }
    var stats: db_mod.types.ColumnarScanStats = .{};
    var cold = try db.scan(alloc, "", "k0768", .{ .exclusive_to = true, .columnar_stats = &stats });
    defer cold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), cold.hashes.len);
    try std.testing.expectEqual(@as(u64, 1), stats.blocks_read);
    try std.testing.expectEqual(@as(u64, 0), stats.overlay_rows_read);
    try std.testing.expect(merged_before_restart + db.indexes.relational.column_maintenance.ranges_merged.load(.monotonic) >= 2);
}

test "relational columnar delete waves coalesce adjacent underfilled ranges" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 1024);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    for (writes, 0..) |*write, i| {
        write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        if (i % 256 != 0) try deletes.append(scratch, write.key);
    }
    try db.batch(.{ .writes = writes });
    try db_mod.drainTestRelationalMaintenance(&db);
    // Separate waves reach idle between deletes. Occupancy metadata must
    // survive queue consumption so future neighbors can still merge.
    for (0..4) |wave| {
        try db.batch(.{ .deletes = deletes.items[wave * 255 ..][0..255] });
        try db_mod.drainTestRelationalMaintenance(&db);
        if (wave == 0) {
            db.close();
            db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        }
    }
    var stats: db_mod.types.ColumnarScanStats = .{};
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .fields = &.{"n"}, .include_all_fields = false, .columnar_stats = &stats });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), result.documents.len);
    try std.testing.expectEqual(@as(u64, 1), stats.blocks_read);
    try std.testing.expectEqual(@as(u64, 0), stats.dirty_ranges_read);
    try std.testing.expect(db.indexes.relational.column_maintenance.ranges_merged.load(.monotonic) >= 3);
    try std.testing.expect(db.indexes.relational.column_maintenance.covered_rows_read.load(.monotonic) > 0);
}

test "relational columnar clean coalescing preserves typed cells without primary reads" {
    // Preserve leak checks; allocation backtraces are opt-in for diagnostics.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.initFast("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string, .allows_null = true },
            .{ .name = "metadata", .path = "metadata", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(db_mod.types.BatchWrite, 512);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        for (writes, 0..) |*write, i| {
            const json = switch (i % 4) {
                0 => "{\"n\":0,\"payload\":\"abc\",\"metadata\":{\"a\":[1,true,\"x\"]}}",
                1 => "{\"n\":1,\"payload\":null,\"metadata\":null}",
                2 => "{\"n\":2}",
                else => "{\"n\":3,\"payload\":\"xyz\",\"metadata\":[null,2]}",
            };
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = json };
            if (i % 256 >= 4) try deletes.append(scratch, write.key);
        }
        try db.batch(.{ .writes = writes });
        try db_mod.drainTestRelationalMaintenance(&db);
        try db.batch(.{ .deletes = deletes.items });
        const fields: []const []const u8 = &.{ "n", "payload", "metadata" };
        db_mod.relational_columns.test_owner_limit = 8;
        defer db_mod.relational_columns.test_owner_limit = null;
        var baseline = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = fields, .include_content_hashes = true });
        defer baseline.deinit(alloc);
        // Stop once dirty publication is complete, leaving the clean merge
        // queue to run separately so its primary I/O can be measured exactly.
        var passes: usize = 0;
        while (true) {
            var stats: db_mod.types.ColumnarScanStats = .{};
            var current = try db.scan(alloc, "", "", .{ .columnar_stats = &stats });
            current.deinit(alloc);
            if (stats.dirty_ranges_read == 0) break;
            try std.testing.expect(try db.indexes.relational.rebuildColumns());
            passes += 1;
            try std.testing.expect(passes < 200);
        }
        const primary_before = db.indexes.relational.column_maintenance.primary_rows_read.load(.monotonic);
        try db_mod.drainTestRelationalMaintenance(&db);
        try std.testing.expectEqual(primary_before, db.indexes.relational.column_maintenance.primary_rows_read.load(.monotonic));
        try std.testing.expect(db.indexes.relational.column_maintenance.covered_rows_read.load(.monotonic) > 0);
        var after = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = fields, .include_content_hashes = true });
        defer after.deinit(alloc);
        try std.testing.expectEqualDeep(baseline.documents, after.documents);
        try std.testing.expectEqualDeep(baseline.hashes, after.hashes);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        var reopened_stats: db_mod.types.ColumnarScanStats = .{};
        var reopened = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = fields, .include_content_hashes = true, .columnar_stats = &reopened_stats });
        defer reopened.deinit(alloc);
        try std.testing.expectEqualDeep(after.documents, reopened.documents);
        try std.testing.expectEqual(@as(u64, 0), reopened_stats.primary_rows_read);
    }
}

test "relational columnar deleted insertion burst has bounded cleanup and complete coverage" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":7}" }} });
    try db_mod.drainTestRelationalMaintenance(&db);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 2050);
    const deletes = try scratch.alloc([]const u8, writes.len);
    for (writes, deletes, 0..) |*write, *key, i| {
        key.* = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i});
        write.* = .{ .key = key.*, .value = "{\"n\":1}" };
    }
    try db.batch(.{ .writes = writes });
    try db.batch(.{ .deletes = deletes });
    const before = db.indexes.relational.column_maintenance.dirty_markers_cleared.load(.monotonic);
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    try std.testing.expect(db.indexes.relational.column_maintenance.dirty_markers_cleared.load(.monotonic) - before <= 1024);
    var passes: usize = 0;
    while (true) {
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .fields = &.{"n"}, .include_all_fields = false });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.documents.len);
        try std.testing.expectEqualStrings("z", result.documents[0].id);
        try std.testing.expect(!db.indexes.relational.columns_rebuild_requested.load(.acquire));
        if (!try db.indexes.relational.rebuildColumns()) break;
        passes += 1;
        try std.testing.expect(passes < 50);
    }
}

test "relational columnar epoch boundaries respect a one block maintenance quantum" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "c", .value = "{\"n\":3}" } } });
    try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns });
    try db_mod.drainTestRelationalMaintenance(&db);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"n\":2}" }} });
    db_mod.relational_columns.test_compaction_block_limit = 1;
    defer db_mod.relational_columns.test_compaction_block_limit = null;
    const before = db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    try std.testing.expectEqual(@as(u64, 1), db.indexes.relational.column_maintenance.blocks_written.load(.monotonic) - before);
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), result.documents.len);
    try db_mod.drainTestRelationalMaintenance(&db);
    try std.testing.expect(!db.indexes.relational.columns_rebuild_requested.load(.acquire));
}

test "relational columnar obsolete generation GC is bounded and resumes after restart" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
    try db_mod.drainTestRelationalMaintenance(&db);
    const already_deleted = db.indexes.relational.column_maintenance.gc_records_deleted.load(.monotonic);
    for (0..260) |i| {
        const key = try std.fmt.allocPrint(alloc, "\x00\x00__columnar__:blocks:0000000000000000:{d:0>4}", .{i});
        defer alloc.free(key);
        try db.core.store.put(key, "obsolete");
    }
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    try std.testing.expectEqual(@as(u64, 256), db.indexes.relational.column_maintenance.gc_records_deleted.load(.monotonic) - already_deleted);
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    try db_mod.drainTestRelationalMaintenance(&db);
    try std.testing.expectEqual(@as(u64, 4), db.indexes.relational.column_maintenance.gc_records_deleted.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), db.indexes.relational.column_maintenance.blocks_written.load(.monotonic));
}

test "relational columnar wide retirement is bounded durable and backpressures publication" {
    const alloc = std.testing.allocator;
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const columns = try scratch.alloc(db_mod.schema_mod.RelationalColumn, 600);
        var object = std.json.ObjectMap.empty;
        for (columns, 0..) |*column, i| {
            const name = try std.fmt.allocPrint(scratch, "c{d:0>3}", .{i});
            column.* = .{ .name = name, .path = name, .column_type = .integer };
            try object.put(scratch, name, .{ .integer = 1 });
        }
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = columns });
        const first = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = first }} });
        try db_mod.drainTestRelationalMaintenance(&db);
        try object.put(scratch, "c000", .{ .integer = 2 });
        const second = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = second }} });
        const before = db.indexes.relational.column_maintenance.gc_records_deleted.load(.monotonic);
        try std.testing.expect(try db.indexes.relational.rebuildColumns());
        // Publication does no per-column retirement work at all.
        try std.testing.expectEqual(before, db.indexes.relational.column_maintenance.gc_records_deleted.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 1), try db_mod.relational_columns.retiredRootsForTest(&db, alloc));
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        const published = db.indexes.relational.column_maintenance.blocks_written.load(.monotonic);
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = first }} });
        try std.testing.expect(try db.indexes.relational.rebuildColumns());
        try std.testing.expect(db.indexes.relational.column_maintenance.gc_records_deleted.load(.monotonic) - before <= 256);
        try std.testing.expectEqual(published, db.indexes.relational.column_maintenance.blocks_written.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 1), try db_mod.relational_columns.retiredRootsForTest(&db, alloc));
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try db_mod.drainTestRelationalMaintenance(&db);
        try std.testing.expectEqual(@as(usize, 0), try db_mod.relational_columns.retiredRootsForTest(&db, alloc));
        try db_mod.relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"c000"} });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), result.documents.len);
        try std.testing.expectEqualStrings("{\"c000\":2}", result.documents[0].json);
        try std.testing.expectEqualStrings("{\"c000\":1}", result.documents[1].json);
    }
}

test "relational columnar maintenance advances past hot ranges across restart" {
    const alloc = std.testing.allocator;
    // Durable deferred-range ages must use one clock across setup, restart,
    // and admission; switching from realtime after setup can strand timers.
    db_mod.relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer db_mod.relational_columns.test_now_ns = null;
    // Assert scheduler fairness against fixed 256-row ranges, independently
    // of debug-build speed and the production wall-clock quantum.
    db_mod.relational_columns.test_disable_deadline = true;
    defer db_mod.relational_columns.test_disable_deadline = false;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(db_mod.types.BatchWrite, 768);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    try db_mod.drainTestRelationalMaintenance(&db);
    try db.batch(.{ .writes = &.{ .{ .key = "k0000", .value = "{\"n\":2}" }, .{ .key = "k0256", .value = "{\"n\":2}" }, .{ .key = "k0512", .value = "{\"n\":2}" } } });
    const HotWriter = struct {
        db: *DB,
        version: usize = 3,
        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var buf: [64]u8 = undefined;
            const value = try std.fmt.bufPrint(&buf, "{{\"n\":{d}}}", .{self.version});
            self.version += 1;
            try self.db.batch(.{ .writes = &.{.{ .key = "k0000", .value = value }} });
        }
    };
    var hot = HotWriter{ .db = &db };
    db_mod.relational_columns.test_before_publish = .{ .context = &hot, .run = HotWriter.run };
    defer db_mod.relational_columns.test_before_publish = null;
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    for (0..8) |_| _ = try db.indexes.relational.rebuildColumns();
    var stats: db_mod.types.ColumnarScanStats = .{};
    var cold = try db.scan(alloc, "k0256", "", .{ .inclusive_from = true, .columnar_stats = &stats });
    defer cold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 512), cold.hashes.len);
    try std.testing.expectEqual(@as(u64, 0), stats.dirty_ranges_read);
    try std.testing.expectEqual(@as(u64, 2), stats.blocks_read);
    // Remove the hot writer. Adaptive maintenance waits for the durable age cap.
    db_mod.relational_columns.test_before_publish = null;
    try std.testing.expect(try db.indexes.relational.runColumnMaintenancePass() > 0);
    db_mod.relational_columns.test_now_ns = 111 * std.time.ns_per_s;
    _ = try db.indexes.relational.runColumnMaintenancePass();
    // A bounded pass may discover a range after the clock advance and arm
    // another age timer. Drive its requested wakeup instead of polling a
    // frozen clock or assuming all cleanup fits into eight native quanta.
    for (0..200) |_| {
        if (!db.indexes.relational.column_maintenance.pending.load(.acquire)) break;
        db_mod.relational_columns.test_now_ns = @max(
            db_mod.relational_columns.test_now_ns.?,
            db.indexes.relational.column_maintenance.waiting_until_ns.load(.acquire),
        );
        _ = try db.indexes.relational.runColumnMaintenancePass();
    }
    const maintenance = db.indexes.relational.column_maintenance.snapshot();
    try std.testing.expect(!maintenance.pending);
    try std.testing.expectEqual(@as(u64, 0), maintenance.pending_age_ns);
    try std.testing.expect(maintenance.ranges_compacted >= 3);
    try std.testing.expect(maintenance.blocks_written >= 3);
    // Unrelated active artifact work cannot bypass columnar resource backoff.
    db.indexes.relational.column_maintenance.retry_after_ns.store(std.math.maxInt(u64), .release);
    const passes = db.indexes.relational.column_maintenance.passes.load(.monotonic);
    try std.testing.expectEqual(@as(usize, 0), try db.indexes.relational.runColumnMaintenancePass());
    try std.testing.expectEqual(passes, db.indexes.relational.column_maintenance.passes.load(.monotonic));
}

test "relational columnar typed masks avoid vector expansion and eliminated columns" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"embedding":{"type":"embedding"},"payload":{"type":"json"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{
        .{ .key = "", .value = "{\"n\":1,\"embedding\":[1,2,3],\"payload\":[1,2]}" },
        .{ .key = "a", .value = "{\"n\":2,\"embedding\":[4,5,6],\"payload\":[3]}" },
        .{ .key = "b", .value = "{\"n\":3}" },
    } });
    const filters = [_][]const u8{
        "{\"exists\":{\"field\":\"embedding\"}}",
        "{\"term\":{\"embedding\":5}}",
        "{\"conjuncts\":[{\"term\":{\"embedding\":5}},{\"numeric_range\":{\"field\":\"n\",\"min\":100}}]}",
        "{\"bool\":{\"must\":[{\"term\":{\"payload\":2}},{\"numeric_range\":{\"field\":\"n\",\"min\":100}}]}}",
        "{\"disjuncts\":[{\"exists\":{\"field\":\"n\"}},{\"term\":{\"payload\":2}}]}",
        "{\"bool\":{\"should\":[{\"term\":{\"embedding\":5}},{\"term\":{\"n\":2}}],\"minimum_should_match\":2}}",
        "{\"term\":{\"embedding.1\":5}}",
        "{\"term\":{\"/embedding/1\":5}}",
        "{\"term\":{\"/embedding/01\":5}}",
    };
    var baseline: [filters.len]db_mod.types.ScanResult = undefined;
    var initialized: usize = 0;
    defer for (baseline[0..initialized]) |*result| result.deinit(alloc);
    for (filters, 0..) |filter, i| {
        baseline[i] = try db.scan(alloc, "", "", .{ .filter_query_json = filter });
        initialized += 1;
    }
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    for (filters, 0..) |filter, i| {
        var stats: db_mod.types.ColumnarScanStats = .{};
        var actual = try db.scan(alloc, "", "", .{ .filter_query_json = filter, .columnar_stats = &stats });
        defer actual.deinit(alloc);
        try std.testing.expectEqualDeep(baseline[i].hashes, actual.hashes);
        try std.testing.expect(stats.used);
        try std.testing.expectEqual(@as(u64, 0), stats.values_materialized);
        if (i == 2 or i == 3) try std.testing.expectEqual(@as(u64, 0), stats.columns_read);
        if (i == 0 or i == 4) {
            try std.testing.expectEqual(@as(u64, 0), stats.columns_read);
            try std.testing.expectEqual(@as(u64, 0), stats.payload_bytes_read);
        }
        for ([_]u32{ 1, 2 }) |limit| {
            var limited = try db.scan(alloc, "", "", .{ .filter_query_json = filter, .limit = limit });
            defer limited.deinit(alloc);
            try std.testing.expectEqualDeep(baseline[i].hashes[0..@min(limit, baseline[i].hashes.len)], limited.hashes);
        }
    }
    // Projecting an indexed component and a complete JSON column must borrow
    // the pinned payloads without filling block-owned logical-value slots.
    var projection_stats: db_mod.types.ColumnarScanStats = .{};
    var projected = try db.scan(alloc, "", "", .{
        .include_documents = true,
        .include_all_fields = false,
        .fields = &.{ "embedding.0", "payload" },
        .columnar_stats = &projection_stats,
    });
    defer projected.deinit(alloc);
    try std.testing.expect(projection_stats.used);
    try std.testing.expectEqualStrings("{\"embedding\":[1],\"payload\":[1,2]}", projected.documents[0].json);
    try std.testing.expectEqual(@as(u64, 0), projection_stats.values_materialized);
    try std.testing.expectEqual(@as(u64, 0), projection_stats.logical_slots_initialized);
    const Stream = struct {
        expected: []const db_mod.types.ScanDocument,
        count: usize = 0,
        fail: bool = false,
        fn visit(ptr: ?*anyopaque, entry: db_mod.types.ScanVisitEntry) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            try std.testing.expect(entry.document_json == null);
            const borrowed = entry.document_stream orelse return error.TestUnexpectedResult;
            var writer = std.Io.Writer.Allocating.init(std.testing.allocator);
            defer writer.deinit();
            try borrowed.writeFields(&writer.writer);
            const expected = self.expected[self.count];
            try std.testing.expectEqualStrings(expected.id, entry.id);
            if (expected.json.len == 2) try std.testing.expectEqual(@as(usize, 0), writer.written().len) else {
                try std.testing.expectEqual(@as(u8, ','), writer.written()[0]);
                try std.testing.expectEqualStrings(expected.json[1 .. expected.json.len - 1], writer.written()[1..]);
            }
            self.count += 1;
            if (self.fail) return error.Canceled;
        }
    };
    var stream: Stream = .{ .expected = projected.documents };
    const streaming_opts: db_mod.types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "embedding.0", "payload" } };
    try db.scanVisit(alloc, "", "", streaming_opts, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
    try std.testing.expectEqual(projected.documents.len, stream.count);
    stream.count = 0;
    stream.fail = true;
    try std.testing.expectError(error.Canceled, db.scanVisit(alloc, "", "", streaming_opts, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true }));
    try std.testing.expectEqual(@as(usize, 1), stream.count);
    // Dirty owners must use the same borrowed contract through the primary
    // fallback, with no stale page payload and no duplicate delivery.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":2,\"embedding\":[9],\"payload\":[4]}" }} });
    var dirty = try db.scan(alloc, "", "", streaming_opts);
    defer dirty.deinit(alloc);
    stream = .{ .expected = dirty.documents };
    var dirty_stats: db_mod.types.ColumnarScanStats = .{};
    var dirty_opts = streaming_opts;
    dirty_opts.columnar_stats = &dirty_stats;
    try db.scanVisit(alloc, "", "", dirty_opts, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
    try std.testing.expectEqual(dirty.documents.len, stream.count);
    try std.testing.expect(dirty_stats.primary_rows_read > 0);
    for ([_]bool{ false, true }) |disable_columnar| {
        var whole = try db.scan(alloc, "", "", .{ .include_documents = true });
        defer whole.deinit(alloc);
        stream = .{ .expected = whole.documents };
        try db.scanVisit(alloc, "", "", .{ .include_documents = true, .disable_columnar_scan = disable_columnar }, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
        try std.testing.expectEqual(whole.documents.len, stream.count);
    }
    // A small LIMIT keeps the clean covered-row path in columnar execution;
    // its borrowed primary scope must stay alive through producer completion.
    try std.testing.expect(try db.indexes.relational.rebuildColumns());
    var first = try db.scan(alloc, "", "", .{ .include_documents = true, .limit = 1 });
    defer first.deinit(alloc);
    stream = .{ .expected = first.documents };
    var full_stats: db_mod.types.ColumnarScanStats = .{};
    try db.scanVisit(alloc, "", "", .{ .include_documents = true, .limit = 1, .columnar_stats = &full_stats }, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
    try std.testing.expectEqual(first.documents.len, stream.count);
    try std.testing.expect(full_stats.used);
    try std.testing.expectEqual(@as(u64, 1), full_stats.primary_rows_read);
}

test "lite bounded reader integration scans native relational dirty rows" {
    const a = std.testing.allocator;
    const lite = @import("../../lite/backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bounded-rows.aflite", .{tmp.sub_path});
    defer a.free(path);
    var handle = try lite.Handle.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer handle.deinit();
    var opts = db_mod.OpenOptions{ .start_index_workers = false, .start_optional_runtimes = false, .ttl_cleanup = .{ .enabled = false } };
    try handle.configureDbOpenOptions(&opts);
    var db = try DB.open(a, path, opts);
    defer db.close();
    const columns = [_]db_mod.schema_mod.RelationalColumn{
        .{ .name = "n", .path = "n", .column_type = .integer },
        .{ .name = "payload", .path = "payload", .column_type = .string },
    };
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const payload = try a.alloc(u8, 65536);
    defer a.free(payload);
    @memset(payload, 'p');
    const json = try std.fmt.allocPrint(a, "{{\"n\":1,\"payload\":\"{s}\"}}", .{payload});
    defer a.free(json);
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = json }, .{ .key = "b", .value = json }, .{ .key = "c", .value = json } } });
    var passes: usize = 0;
    while (try db.indexes.relational.rebuildColumns()) {
        passes += 1;
        try std.testing.expect(passes < 100);
    }
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"n\":2,\"payload\":\"updated\"}" }} });
    var stats: db_mod.types.ColumnarScanStats = .{};
    var result = try db.scan(a, "", "", .{ .include_documents = true, .columnar_stats = &stats });
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), result.documents.len);
    try std.testing.expect(stats.primary_rows_read > 0);
    try std.testing.expectEqualStrings("b", result.documents[1].id);
    try std.testing.expect(std.mem.indexOf(u8, result.documents[1].json, "updated") != null);
    // Both maintenance and dirty-row scan scopes must release their native pins.
    try std.testing.expectEqual(@as(u64, 0), (try handle.native_docstore.?.reclamationStatus()).retained_readers);
}
