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

//! One bounded, restartable activation page. Source read guards, globally
//! routed claims/references, and the owner-bound continuation share ONE durable
//! transaction. Concurrent supervisors may race safely on the progress CAS.
const std = @import("std");
const reads = @import("../api/table_read_source.zig");
const writes = @import("../api/table_write_source.zig");
const planner = @import("../api/relational_integrity_commit.zig");
const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
const records = @import("../common/topology_records.zig");
const contract = @import("../api/distributed_txn_contract.zig");
const Allocator = std.mem.Allocator;
const RequestContext = @import("../api/operation.zig").RequestContext;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const time = @import("antfly_platform").time;

const Attempt = enum { idle, progressed, shrink };

pub const AdaptiveBudget = struct {
    rows: u32 = 128,
    pub fn shrink(self: *AdaptiveBudget, observed_rows: usize) bool {
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
        // Parent UNIQUE coverage is a dependency, not a failed child row.
        // Yield without advancing this checkpoint so parent repair/validation
        // can run before a later supervisor tick retries this page.
        if (err == error.ConstraintActivationPending) return .idle;
        // A repeated absence diagnostic is not progress. The next supervisor
        // tick (or embedded call) rechecks it, while admitting guarded repairs.
        if (err == error.ForeignKeyParentMissing and progress.state == .validating and std.mem.eql(u8, progress.failure, "ForeignKeyParentMissing")) return .idle;
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

pub fn recordFailure(alloc: Allocator, reader: reads.TableReadSource, writer: writes.TableWriteSource, table: []const u8, rows: []const planner.BackfillRow, prepared: ?*planner.Prepared, command: activation.Command, failure: []const u8, control: RequestContext) !void {
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
