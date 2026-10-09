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

//! Local two-phase commit. The root DB owns the durable decision. Participant
//! intents are resolved under the database API fence; reconnect completes a
//! decided commit and aborts an interrupted preparation without replaying SQL.
const h = @import("handles.zig");
const std = h.std;
const dependencies = h.antfly.capi_dependencies;
const contract = dependencies.api_distributed_txn_contract;
const catalog = dependencies.sql_catalog;
const key = "\x00antfly/embedded/sql-commit/v1";
const Record = struct { id: h.db_mod.types.TxnId, timestamp: u64, tables: []const []const u8 };

fn store(handle: *h.Handle, record: ?Record) !void {
    const bytes = if (record) |value| try std.json.Stringify.valueAlloc(handle.alloc, value, .{}) else null;
    defer if (bytes) |value| handle.alloc.free(value);
    var write = try handle.db.core.store.beginWriteTxn();
    errdefer write.abort();
    if (bytes) |value| try write.put(key, value) else try write.delete(key);
    try write.commit();
}

pub fn recover(handle: *h.Handle) !void {
    if (handle.sql_decision_uncertain) return error.SqlMutationOutcomeUnknown;
    var read = try handle.db.core.store.beginReadTxn();
    defer read.abort();
    const bytes = read.get(key) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const parsed = try std.json.parseFromSlice(Record, handle.alloc, bytes, .{});
    defer parsed.deinit();
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlStatementReadUnavailable;
    const record = parsed.value;
    const status = handle.db.getTransactionStatus(record.id) catch |err| switch (err) {
        error.TxnNotFound => h.transactions_mod.TxnStatus.aborted,
        else => return err,
    };
    const committed = status == .committed;
    // An undecided transaction is abandoned only at the database fence;
    // callers never invoke recovery while a preparation is running.
    if (!committed and status == .pending) try handle.db.abortTransaction(record.id, record.timestamp +| 1);
    for (record.tables) |name| {
        const db = try @import("tables.zig").get(handle, name);
        const participant_status = db.getTransactionStatus(record.id) catch |err| switch (err) {
            error.TxnNotFound => continue,
            else => return err,
        };
        if (participant_status == .pending) try db.resolveTransactionIntents(record.id, if (committed) .committed else .aborted, record.timestamp +| 1);
        if (committed and participant_status == .aborted) return error.SqlMutationOutcomeUnknown;
    }
    try store(handle, null);
    // Participant receipts are private implementation details. Keep the
    // coordinator decision for native retention/reconciliation of a 40003
    // receipt after reopening, while acknowledging its finished participants.
    for (record.tables) |name| {
        const db = try @import("tables.zig").get(handle, name);
        if (db == &handle.db) continue;
        db.markTransactionParticipantsResolved(record.id, record.tables) catch continue;
        _ = db.cleanupTransactionMetadataIfEligible(record.id, std.math.maxInt(u64), std.math.maxInt(u64)) catch false;
    }
    handle.db.markTransactionParticipantsResolved(record.id, record.tables) catch return;
}

pub fn commit(handle: *h.Handle, requests: []const contract.TableCommitRequest, out_id: *?h.db_mod.types.TxnId) !catalog.MutationOutcome {
    if (requests.len == 0) return .committed;
    try recover(handle);
    const io = handle.db.backend_runtime.io() orelse return error.UnsupportedSqlExecution;
    const now: u64 = @intCast(@max(0, std.Io.Clock.real.now(io).nanoseconds));
    var id: h.db_mod.types.TxnId = undefined;
    std.Io.random(io, &id);
    const names = try handle.alloc.alloc([]const u8, requests.len);
    defer handle.alloc.free(names);
    for (requests, names) |request, *name| name.* = request.table_name;
    const record: Record = .{ .id = id, .timestamp = now, .tables = names };
    try store(handle, record);
    // Publish a native durable coordinator before preparing any participant.
    _ = try handle.db.beginTransactionScoped(id, now, now, names, true, true, null);
    var decision_started = false;
    errdefer if (!decision_started) {
        recover(handle) catch {};
    };
    for (requests) |request| {
        const db = try @import("tables.zig").get(handle, request.table_name);
        if (db != &handle.db) _ = try db.beginTransactionScoped(id, now, now, names, false, true, null);
        try db.writeTransaction(id, .{
            .schema_version = request.schema_version,
            .relational_schema_version = request.relational_schema_version,
            .relational_integrity_generation_set = request.relational_integrity_generation_set,
            .writes = request.writes,
            .deletes = request.deletes,
            .predicates = request.predicates,
            .integrity_commands = request.integrity_commands,
        });
    }
    out_id.* = id;
    decision_started = true;
    handle.db.commitTransaction(id, now +| 1) catch {
        const status = handle.db.getTransactionStatus(id) catch {
            handle.sql_decision_uncertain = true;
            return error.SqlMutationOutcomeUnknown;
        };
        if (status != .committed) {
            handle.sql_decision_uncertain = true;
            return error.SqlMutationOutcomeUnknown;
        }
    };
    recover(handle) catch return .committed_pending;
    out_id.* = null;
    return .committed;
}
