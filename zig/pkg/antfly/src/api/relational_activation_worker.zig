// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! One bounded, restartable activation page. Source read guards, globally
//! routed claims/references, and the owner-bound continuation share ONE durable
//! transaction. Concurrent supervisors may race safely on the progress CAS.
const std = @import("std");
const reads = @import("antfly_local_sources").api_table_read_source;
const writes = @import("antfly_local_sources").api_table_write_source;
const planner = @import("antfly_local_sources").api_relational_integrity_commit;
const activation = @import("antfly_local_sources").storage_db_relational_integrity_activation_contract;
const records = @import("antfly_local_sources").common_topology_records;
const contract = @import("antfly_local_sources").api_distributed_txn_contract;
const Allocator = std.mem.Allocator;
const RequestContext = @import("antfly_local_sources").api_operation.RequestContext;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const time = @import("antfly_platform").time;

const shared = @import("antfly_local_sources").api_relational_activation_worker;
pub const runPage = shared.runPage;
const recordFailure = shared.recordFailure;
const AdaptiveBudget = shared.AdaptiveBudget;

test "distributed txn activation failure publication atomically guards child and missing parent" {
    const alloc = std.testing.allocator;
    const integrity = @import("antfly_local_sources").storage_db_relational_integrity_contract;
    const types = @import("antfly_local_sources").storage_db_types;
    const address = try integrity.Address.init(@splat(1), "parent tuple");
    const progress: activation.Progress = .{ .generation_set = @splat(2), .owner = @splat(3), .schema_version = 7, .phase = .foreign_key };
    const expected = try progress.encode(alloc);
    defer alloc.free(expected);
    const Fixture = struct {
        race: bool,
        partial: bool,
        commits: usize = 0,
        fn lookup(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            return null;
        }
        fn commit(ptr: *anyopaque, _: Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel, _: CancellationToken) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.commits += 1;
            try std.testing.expectEqual(@as(usize, if (self.partial) 1 else 2), requests.len);
            try std.testing.expectEqual(@as(usize, 1), requests[0].predicates.len);
            try std.testing.expectEqual(@as(?[32]u8, @splat(4)), requests[0].predicates[0].expected_content_digest);
            try std.testing.expectEqual(if (self.partial) activation.State.validating else activation.State.invalid, (try activation.Progress.decode(requests[0].relational_activation.?.next)).state);
            try std.testing.expectEqual(self.partial, requests[0].relational_activation.?.diagnostic);
            if (!self.partial) {
                try std.testing.expectEqual(@as(usize, 1), requests[1].integrity.len);
                try std.testing.expectEqual(.guard, requests[1].integrity[0].kind);
                try std.testing.expectEqual(null, requests[1].integrity[0].expected_value);
            }
            for (requests) |request| try std.testing.expectEqual(@as(usize, 0), request.integrity_commands.len);
            if (self.race) return .{ .conflict = .{ .table_name = "parents", .key = "claim", .message = "parent inserted after absence read" } };
            return .{ .committed = .{ .participant_count = 2 } };
        }
    };
    for (0..3) |scenario| {
        const race = scenario == 1;
        var fixture: Fixture = .{ .race = race, .partial = scenario == 2 };
        const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
        const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = undefined, .commit_batch_with_cancellation = Fixture.commit } };
        var prepared: planner.Prepared = .{ .arena = std.heap.ArenaAllocator.init(alloc), .backfill_partial = fixture.partial, .tables = &.{
            .{ .table_name = "children" },
            .{ .table_name = "parents", .integrity_commands = &.{.{ .address = address, .operation = .{ .attach = .{ .child_table = "children", .child_key = "c", .constraint_name = "fk", .constraint_generation = @splat(5) } } }} },
        } };
        defer prepared.deinit();
        const result = recordFailure(alloc, reader, writer, "children", &.{.{ .key = "c", .json = "{}", .version = 11, .expected_content_digest = @splat(4) }}, &prepared, .{ .routing_key = "", .expected = expected, .next = expected }, "ForeignKeyParentMissing", .{});
        if (race) try std.testing.expectError(error.ConstraintActivationChanged, result) else try result;
        if (fixture.partial) {
            var diagnosed = progress;
            diagnosed.failure = "ForeignKeyParentMissing";
            const unchanged = try diagnosed.encode(alloc);
            defer alloc.free(unchanged);
            try recordFailure(alloc, reader, writer, "children", &.{.{ .key = "c", .json = "{}", .version = 11, .expected_content_digest = @splat(4) }}, &prepared, .{ .routing_key = "", .expected = unchanged, .next = unchanged }, "ForeignKeyParentMissing", .{});
            try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        }
    }
}

test "distributed txn activation worker adapts pages and atomically publishes native claims and failure state" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const types = @import("antfly_local_sources").storage_db_types;
    const integrity = @import("antfly_local_sources").storage_db_relational_integrity_contract;
    const catalog = @import("antfly_local_sources").storage_db_relational_integrity_catalog;
    const tuples = @import("antfly_local_sources").storage_db_relational_index_keys;
    const read_gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    inline for (.{ 0, 1, 2, 3, 4 }) |scenario| {
        const duplicate = scenario == 1;
        const singleton_too_large = scenario == 2;
        const source_too_large = scenario == 3;
        const wide_unrelated = scenario == 4;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/activation", .{tmp.sub_path});
        var db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 300, .shard_id = 301 }, .primary_backend = .{ .lsm = .{} } });
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"padding":{"type":"keyword"}},"additionalProperties":false}}}}
        );
        // A wide unrelated payload must not make a narrow unique backfill
        // oversized; the same payload selected by a composite key must fail
        // visibly and durably rather than retrying forever.
        const padding = try alloc.alloc(u8, if (source_too_large or wide_unrelated) 1024 * 1024 + 1 else 0);
        defer alloc.free(padding);
        @memset(padding, 'x');
        const first_row = if (padding.len != 0) try std.fmt.allocPrint(alloc, "{{\"id\":1,\"padding\":\"{s}\"}}", .{padding}) else try alloc.dupe(u8, "{\"id\":1}");
        defer alloc.free(first_row);
        try db.batch(.{ .timestamp_ns = 100, .writes = &.{ .{ .key = "a", .value = first_row }, .{ .key = "b", .value = if (duplicate) "{\"id\":1}" else "{\"id\":2}" } } });
        const declaration = if (source_too_large)
            \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id","padding"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"padding":{"type":"keyword"}},"additionalProperties":false}}}}
        else
            \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"padding":{"type":"keyword"}},"additionalProperties":false}}}}
        ;
        try db.setSchemaJson(alloc, declaration);
        const Fixture = struct {
            db: *db_mod.DB,
            attempts: u8 = 0,
            reduced: bool = false,
            fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, consistency: read_gate.ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(read_gate.ReadConsistency.read_index, consistency);
                try std.testing.expect(opts.execution_deadline_ns != null);
                if (opts.relational_activation_json.len != 0) {
                    var request = try std.json.parseFromSlice(struct { mode: []const u8, max_rows: u32 = 128 }, allocator, opts.relational_activation_json, .{ .ignore_unknown_fields = true });
                    defer request.deinit();
                    if (request.value.max_rows == 1) self.reduced = true;
                }
                const result = (try self.db.lookup(allocator, key, opts)) orelse return null;
                return .{ .json = result.json, .version = result.version orelse try self.db.getTimestamp(allocator, key), .expected_content_digest = result.expected_content_digest };
            }
            fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: read_gate.ReadConsistency) !?reads.ScanResponse {
                return error.UnexpectedCall;
            }
            fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: read_gate.ReadConsistency) !?@import("antfly_local_sources").api_query_response.QueryResponse {
                return error.UnexpectedCall;
            }
            fn batch(_: *anyopaque, _: Allocator, _: []const u8, _: types.BatchRequest) !?void {
                return error.UnexpectedCall;
            }
            fn commit(ptr: *anyopaque, _: Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel, cancellation: CancellationToken) !?contract.CommitOutcome {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try cancellation.check();
                try std.testing.expectEqual(@as(usize, 1), requests.len);
                const request = requests[0];
                if (!request.relational_repair) {
                    try std.testing.expect(request.relational_activation != null);
                    try std.testing.expectEqual(@as(usize, 0), request.writes.len);
                    try std.testing.expectEqual(@as(usize, 0), request.deletes.len);
                }
                // Exercise the coordinator's real retry boundary without
                // weakening native receiver validation or checkpoint CAS.
                if (!request.relational_repair and (request.integrity_commands.len > 1 or (singleton_too_large and request.integrity_commands.len != 0))) return error.TransactionTooLarge;
                self.attempts += 1;
                const timestamp = @as(u64, self.attempts) * 1000;
                const txn = try self.db.beginTransactionWithId(@splat(self.attempts), timestamp);
                self.db.writeTransaction(txn, .{
                    .relational_schema_version = request.relational_schema_version,
                    .relational_integrity_generation_set = request.relational_integrity_generation_set,
                    .relational_repair = request.relational_repair,
                    .writes = request.writes,
                    .deletes = request.deletes,
                    .predicates = request.predicates,
                    .integrity = request.integrity,
                    .integrity_commands = request.integrity_commands,
                    .relational_activation = request.relational_activation,
                }) catch |err| {
                    try self.db.abortTransaction(txn, timestamp + 1);
                    return err;
                };
                try self.db.commitTransaction(txn, timestamp + 1);
                return .{ .committed = .{ .participant_count = 1 } };
            }
        };
        var fixture: Fixture = .{ .db = &db };
        const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
        const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch, .commit_batch_with_cancellation = Fixture.commit } };
        const tables = [_]records.TableRecord{.{ .table_id = 300, .name = "rows", .placement_role = "data", .schema_json = declaration }};
        const owners = [_]records.RangeRecord{.{ .group_id = 301, .table_id = 300, .start_key = "" }};
        for (0..8) |_| {
            if (!try runPage(alloc, reader, writer, &tables, &owners, owners[0])) break;
        } else return error.ActivationDidNotConverge;
        const raw_progress = (try db.core.getStoreValue(alloc, activation.key)).?;
        defer alloc.free(raw_progress);
        const progress = try activation.Progress.decode(raw_progress);
        try std.testing.expectEqual(if (duplicate or singleton_too_large or source_too_large) activation.State.invalid else activation.State.enforced, progress.state);
        if (duplicate) try std.testing.expectEqualStrings("UniqueConstraintViolation", progress.failure) else if (singleton_too_large) try std.testing.expectEqualStrings("TransactionTooLarge", progress.failure) else if (source_too_large) try std.testing.expectEqualStrings("RelationalRowResultTooLarge", progress.failure) else try std.testing.expectEqual(@as(u64, 2), progress.rows_scanned);
        // A native 5ms scan slice may itself return only one row on a busy
        // runner. Correctness must not depend on forcing a wall-time outcome.
        if (source_too_large) try std.testing.expect(!fixture.reduced);
        try std.testing.expect(!try runPage(alloc, reader, writer, &tables, &owners, owners[0]));
        if (singleton_too_large or source_too_large) continue;
        const raw_catalog = (try db.core.getStoreValue(alloc, catalog.key)).?;
        defer alloc.free(raw_catalog);
        var active_catalog = try catalog.decode(alloc, raw_catalog);
        defer active_catalog.deinit();
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var tuple_plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
        defer tuple_plan.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        defer tuple.deinit(alloc);
        _ = try tuple_plan.appendValues(alloc, &tuple, &.{.{ .integer = 1 }});
        const address = try integrity.Address.init(active_catalog.find(.unique, "pk").?.generation, tuple.items);
        const raw_claim = (try db.core.getStoreValue(alloc, &address.claimKey())).?;
        defer alloc.free(raw_claim);
        try std.testing.expectEqualStrings("a", (try integrity.Claim.decode(&address.claimKey(), raw_claim)).parent_key);
        if (duplicate) {
            const control: RequestContext = .{ .deadline_ns = std.math.maxInt(u64) };
            // Replacing a duplicate with itself is not a constraint bypass.
            var invalid_repair = try planner.prepareRepair(alloc, reader, &tables, &owners, .{
                .table_name = "rows",
                .relational_schema_version = 2,
                .writes = &.{.{ .key = "b", .value = "{\"id\":1}" }},
            }, control);
            defer invalid_repair.deinit();
            try std.testing.expectError(error.UniqueConstraintViolation, writer.commitBatchWithCancellation(alloc, invalid_repair.tables, .write, .none));
            var repaired = try planner.prepareRepair(alloc, reader, &tables, &owners, .{
                .table_name = "rows",
                .relational_schema_version = 2,
                .writes = &.{.{ .key = "b", .value = "{\"id\":2}" }},
            }, control);
            defer repaired.deinit();
            _ = (try writer.commitBatchWithCancellation(alloc, repaired.tables, .write, .none)).?;
            const still_owned = (try db.core.getStoreValue(alloc, &address.claimKey())).?;
            defer alloc.free(still_owned);
            try std.testing.expectEqualStrings("a", (try integrity.Claim.decode(&address.claimKey(), still_owned)).parent_key);
            try @import("relational_constraint_recovery.zig").retry(alloc, reader, writer, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 2 }, control);
            // Retrying again does not reset a healthy in-progress cursor.
            try @import("relational_constraint_recovery.zig").retry(alloc, reader, writer, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 2 }, control);
            for (0..8) |_| {
                if (!try runPage(alloc, reader, writer, &tables, &owners, owners[0])) break;
            } else return error.ActivationDidNotConverge;
            const final_progress = (try db.core.getStoreValue(alloc, activation.key)).?;
            defer alloc.free(final_progress);
            try std.testing.expectEqual(activation.State.enforced, (try activation.Progress.decode(final_progress)).state);
        }
    }
}

test "distributed txn activation admission reaches singleton in bounded reductions" {
    var budget: AdaptiveBudget = .{};
    for ([_]u32{ 64, 32, 16, 8, 4, 2, 1 }) |expected| {
        try std.testing.expect(budget.shrink(budget.rows));
        try std.testing.expectEqual(expected, budget.rows);
    }
    try std.testing.expect(!budget.shrink(1));
    budget = .{};
    try std.testing.expect(budget.shrink(3));
    try std.testing.expectEqual(@as(u32, 1), budget.rows);
}

test "distributed txn MATCH PARTIAL diagnostic admits guarded deletion and correction then resumes coverage" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const types = @import("antfly_local_sources").storage_db_types;
    const gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    const initial =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true},"x":{"type":"integer","nullable":true},"y":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const declaration =
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["a","b"]}],"relational_indexes":[{"name":"by_a","keys":[{"column":"a"}]},{"name":"by_b","keys":[{"column":"b"}]}],"foreign_keys":[{"name":"fk","child_columns":["x","y"],"parent_table":"rows","parent_columns":["a","b"],"match":"partial"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true},"x":{"type":"integer","nullable":true},"y":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    for ([_]bool{ false, true }) |remove| {
        var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("partial-diagnostic-repair");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 500, .shard_id = 501 }, .primary_backend = .{ .lsm = .{} } };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc, initial);
        try db.batch(.{ .writes = &.{.{ .key = "orphan", .value = "{\"a\":9,\"b\":9,\"x\":99,\"y\":null}" }} });
        try std.testing.expectError(error.ForeignKeyGenerationPublicationRequired, db.setSchemaJson(alloc, declaration));
        try @import("relational_fk_test_publication.zig").install(alloc, &db, initial, declaration, 1);
        inline for (.{ "by_a", "by_b" }) |index| {
            for (0..16) |_| {
                if ((try db.relationalIndexBuildStatus(index)).state == .ready) break;
                try db.buildRelationalIndexStep(index, .{});
            } else return error.IndexBuildDidNotConverge;
        }
        const Fixture = struct {
            db: *db_mod.DB,
            sequence: u8 = 0,
            fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, _: gate.ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const result = (try self.db.lookup(allocator, key, opts)) orelse return null;
                return .{ .json = result.json, .version = result.version orelse 0, .expected_content_digest = result.expected_content_digest };
            }
            fn scan(ptr: *anyopaque, allocator: Allocator, _: []const u8, from: []const u8, to: []const u8, opts: types.ScanOptions, _: gate.ReadConsistency) !?reads.ScanResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                var result = try self.db.scan(allocator, from, to, opts);
                defer result.deinit(allocator);
                return .{ .ndjson = try @import("antfly_local_sources").api_local_query_contract.encodeStorageKernelScanNdjson(allocator, result, opts.include_documents) };
            }
            fn commit(ptr: *anyopaque, _: Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel, _: CancellationToken) !?contract.CommitOutcome {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(usize, 1), requests.len);
                const request = requests[0];
                self.sequence += 1;
                const stamp = @as(u64, self.sequence) * 1000;
                const txn = try self.db.beginTransactionWithId(@splat(self.sequence), stamp);
                errdefer self.db.abortTransaction(txn, stamp + 1) catch {};
                try self.db.writeTransaction(txn, .{ .relational_schema_version = request.relational_schema_version, .relational_integrity_generation_set = request.relational_integrity_generation_set, .relational_repair = request.relational_repair, .writes = request.writes, .deletes = request.deletes, .predicates = request.predicates, .integrity = request.integrity, .integrity_commands = request.integrity_commands, .relational_activation = request.relational_activation });
                if (request.relational_repair) {
                    // A concurrent diagnostic/checkpoint publication cannot
                    // move the scan cut while this row repair is prepared.
                    const raw = (try self.db.core.getStoreValue(std.testing.allocator, activation.key)).?;
                    defer std.testing.allocator.free(raw);
                    const concurrent = try self.db.beginTransactionWithId(@splat(200 + self.sequence), stamp + 1);
                    defer self.db.abortTransaction(concurrent, stamp + 2) catch {};
                    try std.testing.expectError(error.IntentConflict, self.db.writeTransaction(concurrent, .{ .relational_schema_version = request.relational_schema_version, .relational_integrity_generation_set = request.relational_integrity_generation_set, .relational_activation = .{ .routing_key = "", .expected = raw, .next = raw, .diagnostic = true } }));
                }
                try self.db.commitTransaction(txn, stamp + 1);
                return .{ .committed = .{ .participant_count = 1 } };
            }
        };
        var fixture: Fixture = .{ .db = &db };
        const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = undefined } };
        const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = undefined, .commit_batch_with_cancellation = Fixture.commit } };
        const tables = [_]records.TableRecord{.{ .table_id = 500, .name = "rows", .placement_role = "data", .schema_json = declaration }};
        const owners = [_]records.RangeRecord{.{ .group_id = 501, .table_id = 500, .start_key = "" }};
        for (0..16) |_| {
            _ = try runPage(alloc, reader, writer, &tables, &owners, owners[0]);
            const raw = (try db.core.getStoreValue(alloc, activation.key)).?;
            defer alloc.free(raw);
            const progress = try activation.Progress.decode(raw);
            if (progress.failure.len != 0) {
                try std.testing.expectEqual(activation.State.validating, progress.state);
                try std.testing.expectEqualStrings("ForeignKeyParentMissing", progress.failure);
                break;
            }
        } else return error.DiagnosticNotPublished;
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        const request: contract.TableCommitRequest = .{ .table_name = "rows", .relational_schema_version = 2, .deletes = if (remove) &.{"orphan"} else &.{}, .writes = if (remove) &.{} else &.{.{ .key = "orphan", .value = "{\"a\":9,\"b\":9,\"x\":null,\"y\":null}" }} };
        var repair = try planner.prepareRepair(alloc, reader, &tables, &owners, request, .{});
        defer repair.deinit();
        var ordinary = repair.tables[0];
        ordinary.relational_repair = false;
        try std.testing.expectError(error.ConstraintActivationInProgress, writer.commitBatchWithCancellation(alloc, &.{ordinary}, .write, .none));
        _ = try writer.commitBatchWithCancellation(alloc, repair.tables, .write, .none);
        for (0..16) |_| {
            if (!try runPage(alloc, reader, writer, &tables, &owners, owners[0])) break;
        } else return error.ActivationDidNotConverge;
        const raw = (try db.core.getStoreValue(alloc, activation.key)).?;
        defer alloc.free(raw);
        const progress = try activation.Progress.decode(raw);
        try std.testing.expectEqual(activation.State.enforced, progress.state);
        try std.testing.expectEqualStrings("", progress.failure);
        // A stale repair cannot bypass healthy coverage after the diagnostic
        // has been cleared, even though it was planned before publication.
        try std.testing.expectError(error.InvalidConstraintActivation, writer.commitBatchWithCancellation(alloc, repair.tables, .write, .none));
    }
}

test "distributed txn CHECK activation shares durable repair retry and physical source guards" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const types = @import("antfly_local_sources").storage_db_types;
    const read_gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    const initial =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"padding":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    const checked =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"padding":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    try std.testing.expect(try planner.requiresActivation(alloc, checked));
    try std.testing.expect(!try planner.requiresCoordination(alloc, checked));
    for ([_]bool{ false, true }) |race_repair| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/check", .{tmp.sub_path});
        const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 700, .shard_id = 701 }, .primary_backend = .{ .lsm = .{} } };
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, initial);
        const padding = try alloc.alloc(u8, 1024 * 1024 + 1);
        defer alloc.free(padding);
        @memset(padding, 'x');
        const row = try std.fmt.allocPrint(alloc, "{{\"id\":-1,\"padding\":\"{s}\"}}", .{padding});
        defer alloc.free(row);
        try db.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = row }} });
        try db.setSchemaJson(alloc, checked);
        const Fixture = struct {
            db: *db_mod.DB,
            race_repair: bool,
            attempts: u8 = 0,
            fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, consistency: read_gate.ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(read_gate.ReadConsistency.read_index, consistency);
                const result = (try self.db.lookup(allocator, key, opts)) orelse return null;
                return .{ .json = result.json, .version = result.version orelse try self.db.getTimestamp(allocator, key), .expected_content_digest = result.expected_content_digest };
            }
            fn commit(ptr: *anyopaque, _: Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel, cancellation: CancellationToken) !?contract.CommitOutcome {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try cancellation.check();
                try std.testing.expectEqual(@as(usize, 1), requests.len);
                const request = requests[0];
                if (request.relational_activation) |command| if (!command.retry and (try activation.Progress.decode(command.next)).state == .invalid) {
                    try std.testing.expect(request.predicates.len != 0);
                    try std.testing.expect(request.predicates[0].expected_content_digest != null);
                    if (self.race_repair) {
                        self.race_repair = false;
                        // Same timestamp: only the physical content guard can
                        // distinguish this repair from the observed bad row.
                        try self.db.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"id\":1}" }} });
                    }
                };
                self.attempts += 1;
                const stamp = @as(u64, self.attempts) * 1000;
                const txn = try self.db.beginTransactionWithId(@splat(self.attempts), stamp);
                self.db.writeTransaction(txn, .{
                    .relational_schema_version = request.relational_schema_version,
                    .relational_integrity_generation_set = request.relational_integrity_generation_set,
                    .relational_repair = request.relational_repair,
                    .writes = request.writes,
                    .deletes = request.deletes,
                    .predicates = request.predicates,
                    .relational_activation = request.relational_activation,
                }) catch |err| {
                    try self.db.abortTransaction(txn, stamp + 1);
                    return err;
                };
                try self.db.commitTransaction(txn, stamp + 1);
                return .{ .committed = .{ .participant_count = 1 } };
            }
        };
        var fixture: Fixture = .{ .db = &db, .race_repair = race_repair };
        const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
        const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = undefined, .commit_batch_with_cancellation = Fixture.commit } };
        const tables = [_]records.TableRecord{.{ .table_id = 700, .name = "rows", .placement_role = "data", .schema_json = checked }};
        const owners = [_]records.RangeRecord{.{ .group_id = 701, .table_id = 700, .start_key = "" }};
        if (race_repair) {
            try std.testing.expectError(error.VersionConflict, runPage(alloc, reader, writer, &tables, &owners, owners[0]));
        } else {
            try std.testing.expect(try runPage(alloc, reader, writer, &tables, &owners, owners[0]));
        }
        {
            const raw = (try db.core.getStoreValue(alloc, activation.key)).?;
            defer alloc.free(raw);
            const progress = try activation.Progress.decode(raw);
            try std.testing.expectEqual(if (race_repair) activation.State.validating else activation.State.invalid, progress.state);
            try std.testing.expectEqual(activation.Phase.check, progress.phase);
            if (!race_repair) try std.testing.expectEqualStrings("CHECK positive: RelationalCheckViolation", progress.failure);
        }
        db.close();
        db = try db_mod.DB.open(alloc, path, options);
        if (!race_repair) {
            try std.testing.expectError(error.RelationalCheckViolation, planner.prepareRepair(alloc, reader, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 2, .writes = &.{.{ .key = "a", .value = "{\"id\":-1}" }} }, .{}));
            var repair = try planner.prepareRepair(alloc, reader, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 2, .writes = &.{.{ .key = "a", .value = "{\"id\":1}" }} }, .{});
            defer repair.deinit();
            _ = (try writer.commitBatchWithCancellation(alloc, repair.tables, .write, .none)).?;
            try std.testing.expectError(error.PreparedGenerationChanged, @import("relational_constraint_recovery.zig").retry(alloc, reader, writer, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 1 }, .{}));
            try @import("relational_constraint_recovery.zig").retry(alloc, reader, writer, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 2 }, .{});
            try @import("relational_constraint_recovery.zig").retry(alloc, reader, writer, &tables, &owners, .{ .table_name = "rows", .relational_schema_version = 2 }, .{});
        }
        for (0..8) |_| {
            if (!try runPage(alloc, reader, writer, &tables, &owners, owners[0])) break;
        } else return error.ActivationDidNotConverge;
        const raw = (try db.core.getStoreValue(alloc, activation.key)).?;
        defer alloc.free(raw);
        try std.testing.expectEqual(activation.State.enforced, (try activation.Progress.decode(raw)).state);
        // CHECK changes reset the same durable coverage identity without
        // routed-claim retirement; removing all CHECKs retires the proof.
        const changed_version = try std.mem.replaceOwned(u8, alloc, checked, "\"version\":2", "\"version\":3");
        defer alloc.free(changed_version);
        const changed = try std.mem.replaceOwned(u8, alloc, changed_version, "\"value\":0", "\"value\":2");
        defer alloc.free(changed);
        try db.setSchemaJson(alloc, changed);
        const changed_tables = [_]records.TableRecord{.{ .table_id = 700, .name = "rows", .placement_role = "data", .schema_json = changed }};
        for (0..16) |_| {
            try std.testing.expect(try runPage(alloc, reader, writer, &changed_tables, &owners, owners[0]));
            const progress_raw = (try db.core.getStoreValue(alloc, activation.key)).?;
            defer alloc.free(progress_raw);
            if ((try activation.Progress.decode(progress_raw)).state != .validating) break;
        } else return error.ActivationDidNotConverge;
        const changed_raw = (try db.core.getStoreValue(alloc, activation.key)).?;
        defer alloc.free(changed_raw);
        try std.testing.expectEqual(activation.State.invalid, (try activation.Progress.decode(changed_raw)).state);
        const removed = try std.mem.replaceOwned(u8, alloc, initial, "\"version\":1", "\"version\":4");
        defer alloc.free(removed);
        try db.setSchemaJson(alloc, removed);
        try std.testing.expectEqual(@as(?[]u8, null), try db.core.getStoreValue(alloc, activation.key));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":-1}" }} });
    }
}

test "distributed txn CHECK coverage identity is typed order independent and layout independent" {
    const schema_api = @import("antfly_local_sources").schema_mod;
    const alloc = std.testing.allocator;
    const first =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0},{"name":"bounded","column":"id","op":"lt","value":10}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const equivalent =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"bounded","column":"id","op":"lt","value":"10"},{"name":"positive","column":"id","op":"gt","value":"0"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"extra":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var a = try schema_api.CompiledTableValidator.init(alloc, first);
    defer a.deinit(alloc);
    var b = try schema_api.CompiledTableValidator.init(alloc, equivalent);
    defer b.deinit(alloc);
    const expected = a.execution.checks.?.fingerprint();
    try std.testing.expectEqualSlices(u8, &expected, &b.execution.checks.?.fingerprint());
    const changed = try std.mem.replaceOwned(u8, alloc, equivalent, "\"value\":\"10\"", "\"value\":\"11\"");
    defer alloc.free(changed);
    var c = try schema_api.CompiledTableValidator.init(alloc, changed);
    defer c.deinit(alloc);
    try std.testing.expect(!std.mem.eql(u8, &expected, &c.execution.checks.?.fingerprint()));
}
