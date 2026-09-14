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
const activation = @import("../storage/db/relational_integrity_activation.zig");
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
    if (owner.restore_backup_id.len != 0 or !try planner.requiresCoordination(alloc, table.schema_json)) return false;
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
        try recordFailure(alloc, writer, table_name, parsed.value.command, progress.failure, control.cancellation);
        return .progressed;
    }
    const phase: planner.BackfillPhase = switch (parsed.value.phase) {
        .unique => .unique,
        .foreign_key => .foreign_key,
    };
    var prepared = planner.prepareBackfillWithCoverageControlled(alloc, reader, tables, ranges, table_name, parsed.value.rows, phase, control) catch |err| {
        if (err == error.TransactionTooLarge and budget.shrink(parsed.value.rows.len)) return .shrink;
        if (err == error.TransactionTooLarge or deterministicValidationFailure(err)) {
            try recordFailure(alloc, writer, table_name, parsed.value.command, @errorName(err), control.cancellation);
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
    try control.ensureActive();
    const outcome = writer.commitBatchWithCancellation(alloc, requests, .write, control.cancellation) catch |err| {
        if (err == error.TransactionTooLarge and budget.shrink(parsed.value.rows.len)) return .shrink;
        if (err == error.TransactionTooLarge or deterministicValidationFailure(err)) {
            try recordFailure(alloc, writer, table_name, parsed.value.command, @errorName(err), control.cancellation);
            return .progressed;
        }
        return err;
    };
    if (outcome) |value| switch (value) {
        .committed => {},
        .conflict => |conflict| {
            if (conflict.reason) |reason| switch (reason) {
                .unique_constraint_violation, .foreign_key_parent_missing => {
                    try recordFailure(alloc, writer, table_name, parsed.value.command, @tagName(reason), control.cancellation);
                    return .progressed;
                },
                else => {},
            };
            return error.ConstraintActivationChanged;
        },
    } else return error.ConstraintActivationUnavailable;
    return .progressed;
}

fn recordFailure(alloc: Allocator, writer: writes.TableWriteSource, table: []const u8, command: activation.Command, failure: []const u8, cancellation: CancellationToken) !void {
    // Never advance past a failed page. A stale worker cannot overwrite a
    // competing successful commit because the original expected bytes remain.
    var progress = try activation.Progress.decode(command.expected orelse return error.ConstraintActivationChanged);
    progress.state = .invalid;
    progress.failure = failure;
    const encoded = try progress.encode(alloc);
    defer alloc.free(encoded);
    var failed = command;
    failed.next = encoded;
    const outcome = (try writer.commitBatchWithCancellation(alloc, &.{.{
        .table_name = table,
        .relational_schema_version = progress.schema_version,
        .relational_activation = failed,
    }}, .write, cancellation)) orelse return error.ConstraintActivationUnavailable;
    switch (outcome) {
        .committed => {},
        .conflict => return error.ConstraintActivationChanged,
    }
}

test "distributed txn activation worker adapts pages and atomically publishes native claims and failure state" {
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
    const integrity = @import("../storage/db/relational_integrity.zig");
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
