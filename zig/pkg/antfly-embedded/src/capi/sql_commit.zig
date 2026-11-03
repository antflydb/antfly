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
pub const key = @import("../storage/lite/replay_admission.zig").coordinator_key;
const Record = struct { id: h.db_mod.types.TxnId, timestamp: u64, tables: []const []const u8 };

/// Serializes recovery with live preparation/decision and promotion's
/// canonical read/merge. Never retain this gate during provider computation or
/// request visibility waits. Native path admission is acquired before this gate.
pub const Coordinator = struct {
    handle: *h.Handle,
    pub fn release(self: Coordinator) void {
        self.handle.sql_commit_mutex.unlock(h.handleLockIo());
    }
    pub fn recover(self: Coordinator) !void {
        return recoverLocked(self.handle);
    }
    pub fn commit(self: Coordinator, requests: []const contract.TableCommitRequest, out_id: *?h.db_mod.types.TxnId) !catalog.MutationOutcome {
        return commitLocked(self.handle, requests, out_id);
    }
};
const TestHook = struct { ptr: *anyopaque, run: *const fn (*anyopaque) anyerror!void };
var test_after_prepare: if (h.builtin.is_test) ?TestHook else void = if (h.builtin.is_test) null else {};
var test_contended_acquire: if (h.builtin.is_test) ?TestHook else void = if (h.builtin.is_test) null else {};

pub fn acquire(handle: *h.Handle) Coordinator {
    if (comptime h.builtin.is_test) {
        if (handle.sql_commit_mutex.tryLock()) return .{ .handle = handle };
        if (test_contended_acquire) |hook| hook.run(hook.ptr) catch @panic("coordinator test hook failed");
    }
    handle.sql_commit_mutex.lockUncancelable(h.handleLockIo());
    return .{ .handle = handle };
}

pub fn recover(handle: *h.Handle) !void {
    const coordinator = acquire(handle);
    defer coordinator.release();
    return coordinator.recover();
}

pub fn commit(handle: *h.Handle, requests: []const contract.TableCommitRequest, out_id: *?h.db_mod.types.TxnId) !catalog.MutationOutcome {
    const coordinator = acquire(handle);
    defer coordinator.release();
    return coordinator.commit(requests, out_id);
}

fn store(handle: *h.Handle, record: ?Record) !void {
    handle.sql_commit_recovery_needed = true;
    const bytes = if (record) |value| try std.json.Stringify.valueAlloc(handle.alloc, value, .{}) else null;
    defer if (bytes) |value| handle.alloc.free(value);
    var write = try handle.db.core.store.beginWriteTxn();
    errdefer write.abort();
    if (bytes) |value| try write.put(key, value) else try write.delete(key);
    try write.commit();
}

fn recoverLocked(handle: *h.Handle) !void {
    // Participants were admitted before preparing this decision. Replaying or
    // publishing it must not recursively start a namespace's DDL coordinator.
    const was_recovering = handle.sql_ddl_recovering;
    handle.sql_ddl_recovering = true;
    defer handle.sql_ddl_recovering = was_recovering;
    if (handle.sql_decision_uncertain) return error.SqlMutationOutcomeUnknown;
    if (!handle.sql_commit_recovery_needed) return;
    if (h.builtin.is_test) handle.sql_commit_recovery_scans +%= 1;
    var read = try handle.db.core.store.beginReadTxn();
    defer read.abort();
    const bytes = read.get(key) catch |err| switch (err) {
        error.NotFound => {
            handle.sql_commit_recovery_needed = false;
            return;
        },
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
    handle.sql_commit_recovery_needed = false;
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

fn commitLocked(handle: *h.Handle, requests: []const contract.TableCommitRequest, out_id: *?h.db_mod.types.TxnId) !catalog.MutationOutcome {
    // Participants were admitted before preparing this decision. Replaying or
    // publishing it must not recursively start a namespace's DDL coordinator.
    const was_recovering = handle.sql_ddl_recovering;
    handle.sql_ddl_recovering = true;
    defer handle.sql_ddl_recovering = was_recovering;
    if (requests.len == 0) return .committed;
    try recoverLocked(handle);
    const io = handle.db.backend_runtime.io() orelse return error.UnsupportedSqlExecution;
    const now: u64 = @intCast(@max(0, std.Io.Clock.real.now(io).nanoseconds));
    var id: h.db_mod.types.TxnId = undefined;
    std.Io.random(io, &id);
    const names = try handle.alloc.alloc([]const u8, requests.len);
    defer handle.alloc.free(names);
    for (requests, names) |request, *name| name.* = request.table_name;
    const record: Record = .{ .id = id, .timestamp = now, .tables = names };
    // Refuse a new coordinator before recording a decision when the journal
    // is full. Once admitted, its bounded transaction must remain completable
    // across every participant, even if the final replay record crosses the
    // ordinary producer high-water mark. Recovery excludes a second producer.
    if (handle.owned_lite_backend) |backend| if (backend.native_docstore) |native_store|
        try native_store.file.requireReplayProducerCapacity();
    try store(handle, record);
    // Publish a native durable coordinator before preparing any participant.
    _ = try handle.db.beginTransactionScoped(id, now, now, names, true, true, null);
    var decision_started = false;
    errdefer if (!decision_started) {
        recoverLocked(handle) catch {};
    };
    for (requests) |request| {
        const db = try @import("tables.zig").get(handle, request.table_name);
        if (db != &handle.db) _ = try db.beginTransactionScoped(id, now, now, names, false, true, null);
        try db.writeTransaction(id, .{
            .schema_version = request.schema_version,
            .relational_schema_version = request.relational_schema_version,
            .relational_integrity_generation_set = request.relational_integrity_generation_set,
            .relational_repair = request.relational_repair,
            .writes = request.writes,
            .deletes = request.deletes,
            .predicates = request.predicates,
            .integrity_commands = request.integrity_commands,
            .relational_activation = request.relational_activation,
            .relational_retirement = request.relational_retirement,
        });
    }
    if (comptime h.builtin.is_test) if (test_after_prepare) |hook| try hook.run(hook.ptr);
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
    recoverLocked(handle) catch return .committed_pending;
    out_id.* = null;
    return .committed;
}

test "capi coordinator recovery waits for a live prepared decision" {
    const api = @import("db.zig");
    const tables = @import("tables.zig");
    var tmp = try api.TestDirectory.init("coordinator-live-prepare");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    defer h.closeHandle(root);
    try tables.create(root, "entities", "{}", false);
    const State = struct {
        root: *h.Handle,
        waiting: std.Io.Event = .unset,
        finished: std.atomic.Value(bool) = .init(false),
        thread: ?std.Thread = null,
        failure: ?anyerror = null,
        fn reader(self: *@This()) void {
            recover(self.root) catch |err| {
                self.failure = err;
            };
            self.finished.store(true, .release);
        }
        fn contended(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.waiting.set(h.handleLockIo());
        }
        fn prepared(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.thread = try std.Thread.spawn(.{ .stack_size = 16 * 1024 * 1024 }, reader, .{self});
            try self.waiting.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(15), .clock = .awake } });
            try std.testing.expect(!self.finished.load(.acquire));
            var read = try self.root.db.core.store.beginReadTxn();
            defer read.abort();
            const raw = try read.get(key);
            const record = try std.json.parseFromSlice(Record, std.testing.allocator, raw, .{});
            defer record.deinit();
            try std.testing.expectEqual(h.transactions_mod.TxnStatus.pending, try self.root.db.getTransactionStatus(record.value.id));
        }
    };
    var state = State{ .root = root };
    defer if (state.thread) |thread| thread.join();
    test_after_prepare = .{ .ptr = &state, .run = State.prepared };
    test_contended_acquire = .{ .ptr = &state, .run = State.contended };
    defer {
        test_after_prepare = null;
        test_contended_acquire = null;
    }
    var transaction: ?h.db_mod.types.TxnId = null;
    const outcome = try commit(root, &.{.{ .table_name = "entities", .writes = &.{.{ .key = "entity/ada", .value = "{\"name\":\"Ada\"}" }} }}, &transaction);
    try std.testing.expectEqual(catalog.MutationOutcome.committed, outcome);
    state.thread.?.join();
    state.thread = null;
    if (state.failure) |err| return err;
    try std.testing.expect(state.finished.load(.acquire));
    var row = (try (try tables.get(root, "entities")).lookup(std.testing.allocator, "entity/ada", .{})) orelse return error.TestUnexpectedResult;
    defer row.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"name\":\"Ada\"}", row.json);
}

test "capi SQL COMMIT reports a predecision refusal without applying an increment" {
    const api = @import("db.zig");
    var tmp = try api.TestDirectory.init("sql-commit-predecision-outcome");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const id = try api.publishHandle(root);
    defer api.antfly_db_close(id);
    const Probe = struct {
        failure: anyerror = error.FileBusy,
        fn refuse(ptr: *anyopaque) !void {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).failure;
        }
        fn call(handle: *anyopaque, session: ?u64, statement: []const u8, expected: h.capi.ErrorCode) !std.json.Parsed(std.json.Value) {
            const a = std.testing.allocator;
            const request = try std.json.Stringify.valueAlloc(a, .{ .statement = statement, .session_id = session }, .{});
            defer a.free(request);
            var out: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&out);
            try std.testing.expectEqual(expected, api.antfly_db_sql_json(handle, .fromSlice(request), &out));
            return std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
        }
        fn ok(handle: *anyopaque, session: ?u64, statement: []const u8) !void {
            const result = try call(handle, session, statement, .ok);
            result.deinit();
        }
    };
    try Probe.ok(id, null, "CREATE TABLE numbers (n BIGINT)");
    try Probe.ok(id, null, "INSERT INTO numbers (_id,n) VALUES ('counter',0)");
    var session: u64 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(id, &session));
    var probe: Probe = .{};
    defer test_after_prepare = null;
    for ([_]struct { failure: anyerror, code: h.capi.ErrorCode, sqlstate: []const u8 }{
        .{ .failure = error.FileBusy, .code = .busy, .sqlstate = "55P03" },
        .{ .failure = error.OutOfMemory, .code = .internal, .sqlstate = "53200" },
        .{ .failure = error.TestInjectedCommitFailure, .code = .internal, .sqlstate = "XX000" },
    }) |case| {
        try Probe.ok(id, session, "BEGIN");
        try Probe.ok(id, session, "UPDATE numbers SET n=n+1 WHERE _id='counter'");
        probe.failure = case.failure;
        test_after_prepare = .{ .ptr = &probe, .run = Probe.refuse };
        const failed = try Probe.call(id, session, "COMMIT", case.code);
        defer failed.deinit();
        test_after_prepare = null;
        try std.testing.expectEqualStrings("not_applied", failed.value.object.get("commit_outcome").?.string);
        try std.testing.expectEqualStrings(case.sqlstate, failed.value.object.get("error").?.object.get("code").?.string);
        try std.testing.expect(failed.value.object.get("transaction_id") == null);
        try Probe.ok(id, session, "ROLLBACK");
        const row = try Probe.call(id, null, "SELECT n FROM numbers WHERE _id='counter'", .ok);
        defer row.deinit();
        const cell = row.value.object.get("rows").?.array.items[0].array.items[0];
        try std.testing.expectEqualStrings("0", cell.string);
    }
    try Probe.ok(id, session, "BEGIN");
    try Probe.ok(id, session, "UPDATE numbers SET n=n+1 WHERE _id='counter'");
    const applied = try Probe.call(id, session, "COMMIT", .ok);
    defer applied.deinit();
    try std.testing.expectEqualStrings("applied", applied.value.object.get("commit_outcome").?.string);
}

test "capi SQL COMMIT uncertain checkpoint returns a token reconciled once after reopen" {
    const api = @import("db.zig");
    const Fault = struct {
        var armed = false;
        fn arm(_: *anyopaque) !void {
            armed = true;
        }
        fn write(userdata: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
            const written = try std.testing.io.vtable.fileWritePositional(userdata, file, header, data, splat, offset);
            // The selector byte is format-owned at offset 20. Publish it,
            // then lose the acknowledgement, matching native crash probes.
            if (armed and offset == 20) {
                armed = false;
                return error.Canceled;
            }
            return written;
        }
        fn call(handle: *anyopaque, session: ?u64, statement: []const u8, expected: h.capi.ErrorCode) !std.json.Parsed(std.json.Value) {
            const request = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = statement, .session_id = session }, .{});
            defer std.testing.allocator.free(request);
            var out: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&out);
            try std.testing.expectEqual(expected, api.antfly_db_sql_json(handle, .fromSlice(request), &out));
            return std.json.parseFromSlice(std.json.Value, std.testing.allocator, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
        }
        fn ok(handle: *anyopaque, session: ?u64, statement: []const u8) !void {
            const response = try call(handle, session, statement, .ok);
            response.deinit();
        }
    };
    var tmp = try api.TestDirectory.init("sql-commit-uncertain-token");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, tmp.path());
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/database.aflite", .{tmp.path()});
    defer std.testing.allocator.free(path);
    var vtable = std.testing.io.vtable.*;
    vtable.fileWritePositional = Fault.write;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    var runtime = try h.db_mod.background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual, .borrowed_io = .{ .general = io } });
    defer runtime.deinit();
    var transaction: h.db_mod.types.TxnId = undefined;
    {
        const root = try api.openLiteHandleAllocWithRuntime(std.testing.allocator, path, .{ .no_sync = true, .profile = .hosted }, true, io, runtime.ptr());
        const id = try api.publishHandle(root);
        defer api.antfly_db_close(id);
        try Fault.ok(id, null, "CREATE TABLE numbers (n BIGINT)");
        try Fault.ok(id, null, "INSERT INTO numbers (_id,n) VALUES ('counter',0)");
        var session: u64 = 0;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(id, &session));
        try Fault.ok(id, session, "BEGIN");
        try Fault.ok(id, session, "UPDATE numbers SET n=n+1 WHERE _id='counter'");
        test_after_prepare = .{ .ptr = &Fault.armed, .run = Fault.arm };
        defer {
            test_after_prepare = null;
            Fault.armed = false;
        }
        const result = try Fault.call(id, session, "COMMIT", .outcome_unknown);
        defer result.deinit();
        try std.testing.expectEqualStrings("unknown", result.value.object.get("commit_outcome").?.string);
        try std.testing.expectEqualStrings("40003", result.value.object.get("error").?.object.get("code").?.string);
        _ = try std.fmt.hexToBytes(&transaction, result.value.object.get("transaction_id").?.string);
        try std.testing.expect(root.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain);
        // Model process loss without publishing close-time index state. Native
        // owners already retire captured runtimes this way; the direct hosted
        // fixture must explicitly abandon its in-memory views before reopen.
        root.lease_snapshot = true;
    }
    const reopened = try api.openLiteHandleAllocWithRuntime(std.testing.allocator, path, .{ .no_sync = true, .profile = .hosted }, false, std.testing.io, runtime.ptr());
    const id = try api.publishHandle(reopened);
    defer api.antfly_db_close(id);
    var status: u8 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_transaction_status(id, &transaction, &status));
    // An unknown decision can reconcile as committed or aborted. The first
    // native publication can precede the coordinator decision; never infer a
    // commit merely because its selector write reached the file.
    try std.testing.expect(status == 1 or status == 2);
    const row = try Fault.call(id, null, "SELECT n FROM numbers WHERE _id='counter'", .ok);
    defer row.deinit();
    try std.testing.expectEqualStrings(if (status == 1) "1" else "0", row.value.object.get("rows").?.array.items[0].array.items[0].string);
    if (status == 2) {
        // Retry only after the durable receipt establishes an abort. The
        // non-idempotent increment must then appear exactly once.
        try Fault.ok(id, null, "UPDATE numbers SET n=n+1 WHERE _id='counter'");
    }
    const final = try Fault.call(id, null, "SELECT n FROM numbers WHERE _id='counter'", .ok);
    defer final.deinit();
    try std.testing.expectEqualStrings("1", final.value.object.get("rows").?.array.items[0].array.items[0].string);
}
