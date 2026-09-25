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

//! One bounded, restartable activation page. Source read guards, globally
//! routed claims/references, and the owner-bound continuation share ONE durable
//! transaction. Concurrent supervisors may race safely on the progress CAS.
const std = @import("std");
const reads = @import("table_reads.zig");
const writes = @import("table_writes.zig");
const planner = @import("relational_integrity_commit.zig");
const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
const records = @import("../common/topology_records.zig");
const contract = @import("distributed_txn_contract.zig");
const Allocator = std.mem.Allocator;
const RequestContext = @import("operation.zig").RequestContext;
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const time = @import("antfly_platform").time;

const Attempt = enum { idle, progressed, shrink };

const AdaptiveBudget = struct {
    rows: u32 = 128,
    fn shrink(self: *AdaptiveBudget, observed_rows: usize) bool {
        if (observed_rows <= 1 or self.rows <= 1) return false;
        self.rows = @intCast(@max(@as(usize, 1), @min(self.rows / 2, observed_rows / 2)));
        return true;
    }
};

pub fn runPage(
    alloc: Allocator,
    reader: reads.TableReadSource,
    writer: writes.TableWriteSource,
    tables: []const records.TableRecord,
    ranges: []const records.RangeRecord,
    owner: records.RangeRecord,
) !bool {
    const table = for (tables) |table| {
        if (table.table_id == owner.table_id) break table;
    } else return false;
    if (owner.restore_backup_id.len != 0 or !try planner.requiresActivation(alloc, table.schema_json)) return false;
    const deadline = time.monotonicNs() +| 5 * std.time.ns_per_s;
    const control: RequestContext = .{
        .deadline_ns = deadline,
        .cancellation = .{ .ptr = &deadline, .is_cancelled_fn = struct {
            fn expired(ptr: *const anyopaque) bool {
                const value: *const u64 = @ptrCast(@alignCast(ptr));
                return time.monotonicNs() >= value.*;
            }
        }.expired },
    };
    var budget: AdaptiveBudget = .{};
    // At most seven reductions reach a one-row page. Every attempt retains
    // the same absolute deadline; no cursor advances until its complete 2PC.
    for (0..8) |_| {
        try control.ensureActive();
        switch (try runAttempt(alloc, reader, writer, tables, ranges, table.name, owner.start_key, &budget, control)) {
            .idle => return false,
            .progressed => return true,
            .shrink => continue,
        }
    }
    return error.ConstraintActivationUnavailable;
}

fn deterministicValidationFailure(err: anyerror) bool {
    return switch (err) {
        error.UniqueConstraintViolation,
        error.ForeignKeyParentMissing,
        error.ForeignKeyMatchFullViolation,
        error.RelationalCheckViolation,
        error.ForeignKeyTargetNotUnique,
        error.ForeignKeyTypeMismatch,
        error.TableNotFound,
        error.InvalidIntegrityDefinition,
        error.UnsupportedIntegrityDefinition,
        => true,
        else => false,
    };
}

fn runAttempt(alloc: Allocator, reader: reads.TableReadSource, writer: writes.TableWriteSource, tables: []const records.TableRecord, ranges: []const records.RangeRecord, table_name: []const u8, range_key: []const u8, budget: *AdaptiveBudget, control: RequestContext) !Attempt {
    const request_json = try std.json.Stringify.valueAlloc(alloc, .{ .mode = "page", .max_rows = budget.rows }, .{});
    defer alloc.free(request_json);
    var response = (try reader.lookup(alloc, table_name, range_key, .{ .relational_activation_json = request_json, .execution_deadline_ns = control.deadline_ns, .cancellation = control.cancellation }, .read_index)) orelse return .idle;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(struct {
        rows: []planner.BackfillRow,
        command: activation.Command,
        phase: activation.Phase,
    }, alloc, response.json, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const progress = try activation.Progress.decode(parsed.value.command.next);
    if (progress.state == .invalid) {
        // The native reader may diagnose an individually oversized source
        // row before projecting it. Its failure envelope retains the exact
        // original checkpoint and never advances source coverage.
        try recordFailure(alloc, reader, writer, table_name, parsed.value.rows, null, parsed.value.command, progress.failure, control);
        return .progressed;
    }
    const phase: planner.BackfillPhase = switch (parsed.value.phase) {
        .unique => .unique,
        .foreign_key => .foreign_key,
        .check => .check,
    };
    var prepared = planner.prepareBackfillWithCoverageControlled(alloc, reader, tables, ranges, table_name, parsed.value.rows, phase, control) catch |err| {
        if (err == error.TransactionTooLarge and budget.shrink(parsed.value.rows.len)) return .shrink;
        if (err == error.TransactionTooLarge or deterministicValidationFailure(err)) {
            if (budget.shrink(parsed.value.rows.len)) return .shrink;
            try recordFailure(alloc, reader, writer, table_name, parsed.value.rows, null, parsed.value.command, @errorName(err), control);
            return .progressed;
        }
        return err;
    };
    defer prepared.deinit();
    const requests = try alloc.dupe(contract.TableCommitRequest, prepared.tables);
    defer alloc.free(requests);
    const source = for (requests) |*request| {
        if (std.mem.eql(u8, request.table_name, table_name)) break request;
    } else return error.InvalidConstraintActivation;
    source.relational_activation = parsed.value.command;
    source.relational_schema_version = progress.schema_version;
    if (prepared.validation_failure) |failure| {
        // Keep all physical source observations in this same transaction.
        // Concurrent repair (even with an unchanged timestamp) cannot publish
        // a stale failed CHECK, and no rejected page advances coverage.
        var failed = try activation.Progress.decode(parsed.value.command.expected orelse return error.ConstraintActivationChanged);
        failed.state = .invalid;
        failed.failure = failure;
        source.relational_activation.?.next = try failed.encode(prepared.arena.allocator());
    }
    try control.ensureActive();
    const outcome = writer.commitBatchWithCancellation(alloc, requests, .write, control.cancellation) catch |err| {
        if (err == error.TransactionTooLarge and budget.shrink(parsed.value.rows.len)) return .shrink;
        if (err == error.TransactionTooLarge or deterministicValidationFailure(err)) {
            if (budget.shrink(parsed.value.rows.len)) return .shrink;
            try recordFailure(alloc, reader, writer, table_name, parsed.value.rows, &prepared, parsed.value.command, @errorName(err), control);
            return .progressed;
        }
        return err;
    };
    if (outcome) |value| switch (value) {
        .committed => {},
        .conflict => |conflict| {
            if (conflict.reason) |reason| switch (reason) {
                .unique_constraint_violation, .foreign_key_parent_missing => {
                    if (budget.shrink(parsed.value.rows.len)) return .shrink;
                    try recordFailure(alloc, reader, writer, table_name, parsed.value.rows, &prepared, parsed.value.command, @tagName(reason), control);
                    return .progressed;
                },
                else => {},
            };
            return error.ConstraintActivationChanged;
        },
    } else return error.ConstraintActivationUnavailable;
    return .progressed;
}

fn recordFailure(alloc: Allocator, reader: reads.TableReadSource, writer: writes.TableWriteSource, table: []const u8, rows: []const planner.BackfillRow, prepared: ?*planner.Prepared, command: activation.Command, failure: []const u8, control: RequestContext) !void {
    if (rows.len == 0) return error.ConstraintActivationChanged;
    const missing_parent = std.mem.eql(u8, failure, "ForeignKeyParentMissing") or std.mem.eql(u8, failure, "foreign_key_parent_missing");
    const diagnostic = missing_parent and (prepared == null or prepared.?.backfill_partial);
    if (!diagnostic) if (prepared) |page| if (!try planner.guardBackfillFailure(page, reader, failure, control)) return error.ConstraintActivationChanged;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    // MATCH PARTIAL absence comes from an index-range read, not an exact
    // claim. Without a durable negative predicate it is a retryable diagnostic,
    // never evidence for terminal INVALID. A later parent insert can resolve it.
    var progress = try activation.Progress.decode(command.expected orelse return error.ConstraintActivationChanged);
    // Rechecks remain bounded by the supervisor, but an unchanged diagnostic
    // must not generate another distributed transaction / Raft log record.
    if (diagnostic and std.mem.eql(u8, progress.failure, "ForeignKeyParentMissing")) return;
    progress.state = if (diagnostic) .validating else .invalid;
    progress.failure = if (diagnostic) "ForeignKeyParentMissing" else failure;
    const encoded = try progress.encode(alloc);
    defer alloc.free(encoded);
    var failed = command;
    failed.next = encoded;
    failed.diagnostic = diagnostic;
    const requests = if (prepared) |page| try owned.dupe(contract.TableCommitRequest, page.tables) else try owned.alloc(contract.TableCommitRequest, 1);
    if (prepared == null) requests[0] = .{ .table_name = table };
    for (requests) |*request| {
        request.integrity_commands = &.{};
        if (std.mem.eql(u8, request.table_name, table)) {
            const guards = try owned.alloc(@import("../storage/db/types.zig").TransactionVersionPredicate, rows.len);
            for (guards, rows) |*guard, row| guard.* = .{ .key = row.key, .expected_version = row.version, .expected_content_digest = row.expected_content_digest orelse return error.MissingPrimaryObservation };
            request.predicates = guards;
            request.relational_schema_version = progress.schema_version;
            request.relational_integrity_generation_set = progress.generation_set;
            request.relational_activation = failed;
        }
    }
    var count: usize = 0;
    for (requests) |request| {
        if (request.relational_activation == null and request.integrity.len == 0 and request.predicates.len == 0) continue;
        requests[count] = request;
        count += 1;
    }
    try control.ensureActive();
    const outcome = (try writer.commitBatchWithCancellation(alloc, requests[0..count], .write, control.cancellation)) orelse return error.ConstraintActivationUnavailable;
    switch (outcome) {
        .committed => {},
        .conflict => return error.ConstraintActivationChanged,
    }
}

test "distributed txn activation failure publication atomically guards child and missing parent" {
    const alloc = std.testing.allocator;
    const integrity = @import("../storage/db/relational_integrity_contract.zig");
    const types = @import("../storage/db/types.zig");
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
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
    const integrity = @import("../storage/db/relational_integrity_contract.zig");
    const catalog = @import("../storage/db/relational_integrity_catalog.zig");
    const tuples = @import("../storage/db/relational_index_keys.zig");
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
            fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: read_gate.ReadConsistency) !?@import("query_response.zig").QueryResponse {
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
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
    const gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    const initial =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true},"x":{"type":"integer","nullable":true},"y":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const declaration =
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["a","b"]}],"relational_indexes":[{"name":"by_a","keys":[{"column":"a"}]},{"name":"by_b","keys":[{"column":"b"}]}],"foreign_keys":[{"name":"fk","child_columns":["x","y"],"parent_table":"rows","parent_columns":["a","b"],"match":"partial"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true},"x":{"type":"integer","nullable":true},"y":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    for ([_]bool{ false, true }) |remove| {
        var directory = try @import("../common/test_directory.zig").TestDirectory.init("partial-diagnostic-repair");
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
                return .{ .ndjson = try @import("local_query_contract.zig").encodeStorageKernelScanNdjson(allocator, result, opts.include_documents) };
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
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
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
    const schema_api = @import("../schema/mod.zig");
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
