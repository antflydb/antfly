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

pub fn hasAdmission(handle: *h.Handle, id: h.db_mod.types.TxnId) !bool {
    const coordinator = acquire(handle);
    defer coordinator.release();
    var read = try handle.db.core.store.beginReadTxn();
    defer read.abort();
    const bytes = read.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    const record = try std.json.parseFromSlice(Record, handle.alloc, bytes, .{});
    defer record.deinit();
    return std.mem.eql(u8, &record.value.id, &id);
}

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
        return commitLocked(self.handle, requests, &.{}, null, out_id, .sql);
    }
};
const TestHook = struct { ptr: *anyopaque, run: *const fn (*anyopaque) anyerror!void };
var test_after_prepare: if (h.builtin.is_test) ?TestHook else void = if (h.builtin.is_test) null else {};
/// Runs before the recovery hint is stored; an error fails the commit there.
var test_before_store: if (h.builtin.is_test) ?TestHook else void = if (h.builtin.is_test) null else {};
/// Runs after a committed decision, in place of participant resolution
/// succeeding; an error simulates resolution failing.
var test_after_decision: if (h.builtin.is_test) ?TestHook else void = if (h.builtin.is_test) null else {};
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
    // An earlier decision is unresolved. New work is refused, not unknown.
    if (handle.sql_decision_uncertain) return error.SqlReconciliationRequired;
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

pub const GraphGuard = struct {
    table_name: []const u8,
    table_id: u64,
    schema_version: u32,
    proofs: []const @typeInfo(@FieldType(h.db_mod.types.TransactionIntentRequest, "range_guards")).pointer.child,
};
/// Local capture capabilities are validated against local native owners. They
/// do not fabricate distributed route fences or data-Raft history.
pub fn commitWithGraphGuards(handle: *h.Handle, requests: []const contract.TableCommitRequest, guards: []const GraphGuard, requested_id: ?h.db_mod.types.TxnId, out_id: *?h.db_mod.types.TxnId) !catalog.MutationOutcome {
    const coordinator = acquire(handle);
    defer coordinator.release();
    return commitLocked(handle, requests, guards, requested_id, out_id, .graph);
}

/// Whether the native checkpoint publication is in its uncertain window, so a
/// pending decision may nonetheless be durable. Non-native stores answer yes.
fn publicationUncertain(handle: *h.Handle) bool {
    const root = handle.parent_handle orelse handle;
    const backend = root.owned_lite_backend orelse return true;
    const docstore = backend.native_docstore orelse return true;
    return docstore.file.checkpoint_publication_uncertain;
}

/// Outcome boundaries, in order:
/// 1. Before `commitTransaction`: nothing can commit the participants'
///    intents without a committed coordinator status, so every failure is
///    "not applied" (`notApplied`). The stored record is only a recovery hint.
/// 2. `commitTransaction` failing: the durable status decides. Committed is
///    committed (pending visibility, or repair required); aborted is a
///    conflict; pending with no uncertain publication never landed and is
///    aborted and reported not applied; anything else is unknown, with the
///    receipt id left in `out_id`.
/// 3. After a committed decision, failing to resolve participants is
///    committed-pending; recovery finishes it.
fn commitLocked(handle: *h.Handle, requests: []const contract.TableCommitRequest, guards: []const GraphGuard, requested_id: ?h.db_mod.types.TxnId, out_id: *?h.db_mod.types.TxnId, language: enum { sql, graph }) !catalog.MutationOutcome {
    // Participants were admitted before preparing this decision. Replaying or
    // publishing it must not recursively start a namespace's DDL coordinator.
    const was_recovering = handle.sql_ddl_recovering;
    handle.sql_ddl_recovering = true;
    defer handle.sql_ddl_recovering = was_recovering;
    if (requests.len == 0) return .committed;
    out_id.* = null;
    const outcomes = dependencies.sql_errors;
    const id = prepareDecision(handle, requests, guards, requested_id) catch |err| {
        // Graph diagnostics carry domain identities (including a consumed
        // request ID). Preserve them at the proven pre-decision boundary;
        // SQL maps unclassified storage failures into its SQLSTATE contract.
        if (language == .graph and !outcomes.isUnknownOutcome(err)) return err;
        return outcomes.notApplied(err);
    };
    out_id.* = id.id;
    handle.db.commitTransaction(id.id, id.now +| 1) catch |commit_err| {
        const status = handle.db.getTransactionStatus(id.id) catch {
            handle.sql_decision_uncertain = true;
            return error.SqlMutationOutcomeUnknown;
        };
        switch (status) {
            .committed => {
                recoverLocked(handle) catch {};
                return if (commit_err == error.EnrichmentWorkerFailed) .committed_repair_required else .committed_pending;
            },
            .aborted => {
                out_id.* = null;
                return error.SqlWriteConflict;
            },
            .pending => if (!publicationUncertain(handle)) {
                // The decision provably never became durable. Abort it so
                // recovery can never commit it later; a failed abort leaves
                // that to the next recovery, which aborts pending decisions.
                handle.db.abortTransaction(id.id, id.now +| 2) catch {};
                recoverLocked(handle) catch {};
                out_id.* = null;
                return outcomes.notApplied(commit_err);
            },
        }
        handle.sql_decision_uncertain = true;
        return error.SqlMutationOutcomeUnknown;
    };
    if (comptime h.builtin.is_test) if (test_after_decision) |hook| hook.run(hook.ptr) catch return .committed_pending;
    recoverLocked(handle) catch return .committed_pending;
    out_id.* = null;
    return .committed;
}

const PreparedDecision = struct { id: h.db_mod.types.TxnId, now: u64 };

/// Boundary 1 of `commitLocked`. On any error the prepared intents are
/// abandoned through recovery, and nothing was applied.
fn prepareDecision(handle: *h.Handle, requests: []const contract.TableCommitRequest, guards: []const GraphGuard, requested_id: ?h.db_mod.types.TxnId) !PreparedDecision {
    try recoverLocked(handle);
    const io = handle.db.backend_runtime.io() orelse return error.UnsupportedSqlExecution;
    const now: u64 = @intCast(@max(0, std.Io.Clock.real.now(io).nanoseconds));
    var id: h.db_mod.types.TxnId = undefined;
    if (requested_id) |value| {
        id = value;
        const existing: ?h.transactions_mod.TxnStatus = handle.db.getTransactionStatus(id) catch |err| switch (err) {
            error.TxnNotFound => null,
            else => return err,
        };
        if (existing != null) return error.CypherWriteRequestAlreadyUsed;
    } else try io.randomSecure(&id);
    const names = try handle.alloc.alloc([]const u8, requests.len);
    defer handle.alloc.free(names);
    for (requests, names) |request, *name| name.* = request.table_name;
    const record: Record = .{ .id = id, .timestamp = now, .tables = names };
    // Refuse a new coordinator before recording a decision when the journal
    // is full. Once admitted, its bounded transaction must remain completable
    // across every participant, even if the final replay record crosses the
    // ordinary producer high-water mark. Recovery excludes a second producer.
    if (handle.owned_lite_backend) |backend| if (backend.native_docstore) |native_store|
        try native_store.file.requireReplayCoordinatorCapacity();
    if (comptime h.builtin.is_test) if (test_before_store) |hook| try hook.run(hook.ptr);
    try store(handle, record);
    // From here a failure leaves a hint and possibly pending intents; abort
    // them through recovery (also retried at the next entry if this fails).
    errdefer recoverLocked(handle) catch {};
    // Publish a native durable coordinator before preparing any participant.
    _ = try handle.db.beginTransactionScoped(id, now, now, names, true, true, null);
    for (requests) |request| {
        const db = try @import("tables.zig").get(handle, request.table_name);
        const proofs = for (guards) |guard| {
            if (!std.mem.eql(u8, guard.table_name, request.table_name)) continue;
            if (guard.table_id != db.core.identity_namespace.table_id or
                guard.schema_version != (if (db.core.schema) |schema| schema.version else 0) or
                request.schema_version != (if (guard.schema_version == 0) @as(?u32, null) else guard.schema_version)) return error.CatalogGenerationChanged;
            break guard.proofs;
        } else if (guards.len != 0 and (request.normalized_graph_rows or request.graph_writes.len != 0 or request.graph_deletes.len != 0)) return error.CatalogGenerationChanged else &.{};
        if (db != &handle.db) _ = try db.beginTransactionScoped(id, now, now, names, false, true, null);
        try db.writeTransaction(id, .{
            .schema_version = request.schema_version,
            .relational_schema_version = request.relational_schema_version,
            .relational_integrity_generation_set = request.relational_integrity_generation_set,
            .relational_repair = request.relational_repair,
            .writes = request.writes,
            .deletes = request.deletes,
            .normalized_graph_rows = request.normalized_graph_rows,
            .graph_writes = request.graph_writes,
            .graph_deletes = request.graph_deletes,
            .range_guards = proofs,
            .predicates = request.predicates,
            .integrity_commands = request.integrity_commands,
            .relational_activation = request.relational_activation,
            .relational_retirement = request.relational_retirement,
        });
    }
    if (comptime h.builtin.is_test) if (test_after_prepare) |hook| try hook.run(hook.ptr);
    return .{ .id = id, .now = now };
}

test "capi coordinator recovery waits for a live prepared decision" {
    try testPreparedCoordinator(false);
}

test "capi graph commit serializes recovery with its prepared decision" {
    try testPreparedCoordinator(true);
}

fn testPreparedCoordinator(graph: bool) !void {
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
    const requests: []const contract.TableCommitRequest = &.{.{ .table_name = "entities", .writes = &.{.{ .key = "entity/ada", .value = "{\"name\":\"Ada\"}" }} }};
    var requested_id: h.db_mod.types.TxnId = undefined;
    @memset(&requested_id, 7);
    const outcome = if (graph) try commitWithGraphGuards(root, requests, &.{}, requested_id, &transaction) else try commit(root, requests, &transaction);
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
        .{ .failure = error.TestInjectedCommitFailure, .code = .internal, .sqlstate = "58000" },
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

// ---------------------------------------------------------------------------
// Outcome boundaries. Each case injects one failure at one boundary of one
// path and checks the reported outcome against what is actually stored.

const OutcomeFixture = struct {
    const api = @import("db.zig");
    directory: api.TestDirectory,
    path: [:0]u8,
    id: ?*anyopaque,
    wrapper: *h.Handle,

    fn init(comptime name: []const u8) !OutcomeFixture {
        const alloc = std.testing.allocator;
        var directory = try api.TestDirectory.init(name);
        errdefer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrintSentinel(alloc, "{s}/db.aflite", .{directory.path()}, 0);
        errdefer alloc.free(path);
        const wrapper = try api.openLiteHandleAlloc(path, .{ .no_sync = true, .profile = .hosted, .busy_timeout_ms = 5000 }, true);
        const id = try api.publishHandle(wrapper);
        // Background turns also write pages; keep injected faults for the
        // statement under test.
        const owner = wrapper.lite_connection.?.owner;
        const io = h.handleLockIo();
        owner.stopping.store(true, .release);
        owner.wake.set(io);
        if (owner.worker) |worker| worker.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        var fixture: OutcomeFixture = .{ .directory = directory, .path = path, .id = id, .wrapper = wrapper };
        try fixture.expectOk("{\"statement\":\"CREATE TABLE t (n BIGINT)\"}");
        return fixture;
    }

    fn deinit(self: *OutcomeFixture) void {
        api.antfly_db_close(self.id);
        std.testing.allocator.free(self.path);
        self.directory.cleanup();
    }

    fn reopen(self: *OutcomeFixture) !void {
        const wrapper = try api.openLiteHandleAlloc(self.path, .{ .no_sync = true, .profile = .hosted, .busy_timeout_ms = 5000 }, false);
        const id = api.publishHandle(wrapper) catch |err| {
            h.closeHandle(wrapper);
            return err;
        };
        api.antfly_db_close(self.id);
        self.id = id;
        self.wrapper = wrapper;
        const owner = wrapper.lite_connection.?.owner;
        owner.stopping.store(true, .release);
        owner.wake.set(h.handleLockIo());
        if (owner.worker) |worker| worker.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
    }

    /// The current storage root. Arm faults immediately before a call.
    fn file(self: *OutcomeFixture) *@import("../storage/lite/native.zig").NativeFile {
        const root = self.wrapper.lite_connection.?.current.?;
        return &root.owned_lite_backend.?.native_docstore.?.file;
    }

    const Reply = struct {
        code: h.capi.ErrorCode,
        parsed: ?std.json.Parsed(std.json.Value),
        fn deinit(self: *Reply) void {
            if (self.parsed) |*p| p.deinit();
        }
        fn state(self: Reply) ?[]const u8 {
            const body = self.parsed orelse return null;
            const err = body.value.object.get("error") orelse return null;
            return err.object.get("code").?.string;
        }
        fn has(self: Reply, field: []const u8) bool {
            const body = self.parsed orelse return false;
            return body.value.object.get(field) != null;
        }
        fn outcome(self: Reply) ?[]const u8 {
            const body = self.parsed orelse return null;
            const value = body.value.object.get("mutation_outcome") orelse return null;
            return if (value == .string) value.string else null;
        }
    };

    fn sql(self: *OutcomeFixture, request: []const u8) !Reply {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        const code = api.antfly_db_sql_json(self.id, .fromSlice(request), &output);
        const parsed = if (output.ptr) |ptr| try std.json.parseFromSlice(std.json.Value, std.testing.allocator, ptr[0..output.len], .{}) else null;
        return .{ .code = code, .parsed = parsed };
    }

    fn expectOk(self: *OutcomeFixture, request: []const u8) !void {
        var reply = try self.sql(request);
        defer reply.deinit();
        if (reply.code != .ok) {
            std.debug.print("unexpected {s} for {s}: {?s} {s}\n", .{ @tagName(reply.code), request, reply.state(), std.mem.span(h.capi.lastErrorDetail()) });
            return error.TestUnexpectedResult;
        }
    }

    fn rows(self: *OutcomeFixture) !usize {
        var reply = try self.sql("{\"statement\":\"SELECT n FROM t\"}");
        defer reply.deinit();
        if (reply.code != .ok) std.debug.print("SELECT failed: {s} {?s} {s}\n", .{ @tagName(reply.code), reply.state(), std.mem.span(h.capi.lastErrorDetail()) });
        try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
        return reply.parsed.?.value.object.get("rows").?.array.items.len;
    }
};

const insert_one = "{\"statement\":\"INSERT INTO t (_id,n) VALUES ('r',1)\"}";

test "capi SQL outcome native constraints join one checkpoint and rollback together" {
    var f = try OutcomeFixture.init("native-sql-constraint-publication");
    defer f.deinit();
    try f.expectOk("{\"statement\":\"CREATE TABLE native_rows (id TEXT PRIMARY KEY, n BIGINT)\"}");
    // Finish the CREATE recovery agenda before measuring the row publication.
    try f.expectOk("{\"statement\":\"SELECT id FROM native_rows\"}");
    const before_insert = f.file().activeCheckpoint().commit_sequence;
    try f.expectOk("{\"statement\":\"INSERT INTO native_rows (_id,id,n) VALUES ('a','one',1)\"}");
    std.debug.print("native SQL insert publications={d}\n", .{f.file().activeCheckpoint().commit_sequence - before_insert});
    try std.testing.expectEqual(before_insert + 1, f.file().activeCheckpoint().commit_sequence);

    const before_duplicate = f.file().activeCheckpoint().commit_sequence;
    var duplicate = try f.sql("{\"statement\":\"INSERT INTO native_rows (_id,id,n) VALUES ('b','one',9)\"}");
    defer duplicate.deinit();
    try std.testing.expectEqualStrings("23505", duplicate.state().?);
    std.debug.print("native SQL duplicate publications={d}\n", .{f.file().activeCheckpoint().commit_sequence - before_duplicate});
    try std.testing.expectEqual(before_duplicate, f.file().activeCheckpoint().commit_sequence);

    // A failed primary publication must not move the UNIQUE claim or apply
    // the increment. Retrying the SQL statement after a known failure is safe.
    var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput };
    fault.arm();
    defer PrimaryFault.disarm();
    var failed = try f.sql("{\"statement\":\"UPDATE native_rows SET id='two', n=n+1 WHERE _id='a'\"}");
    PrimaryFault.disarm();
    defer failed.deinit();
    try std.testing.expectEqualStrings("58000", failed.state().?);
    std.debug.print("native SQL failed publications={d}\n", .{f.file().activeCheckpoint().commit_sequence - before_duplicate});
    try std.testing.expectEqual(before_duplicate, f.file().activeCheckpoint().commit_sequence);
    try f.expectOk("{\"statement\":\"UPDATE native_rows SET id='two', n=n+1 WHERE _id='a'\"}");
    try std.testing.expectEqual(before_duplicate + 1, f.file().activeCheckpoint().commit_sequence);
    // The old claim disappeared with the row update, and the increment ran once.
    try f.expectOk("{\"statement\":\"INSERT INTO native_rows (_id,id,n) VALUES ('b','one',3)\"}");
    var values = try f.sql("{\"statement\":\"SELECT n FROM native_rows WHERE _id='a'\"}");
    defer values.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, values.code);
    try std.testing.expectEqualStrings("2", values.parsed.?.value.object.get("rows").?.array.items[0].array.items[0].string);
    try f.reopen();
    try f.expectOk("{\"statement\":\"SELECT id FROM native_rows WHERE id='two'\"}");
    try std.testing.expect((try f.file().check()).valid);
}

test "capi SQL native publication repairs CHECK coverage while rejecting invalid postimages" {
    var f = try OutcomeFixture.init("sql-native-check-repair");
    defer f.deinit();
    try f.expectOk("{\"statement\":\"CREATE TABLE checked (n BIGINT)\"}");
    try f.expectOk("{\"statement\":\"INSERT INTO checked (_id,n) VALUES ('a',-1)\"}");
    {
        var reply = try f.sql("{\"statement\":\"ALTER TABLE checked ADD CONSTRAINT positive CHECK (n > 0)\"}");
        defer reply.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
        try std.testing.expectEqualStrings("invalid", reply.parsed.?.value.object.get("ddl_receipt").?.object.get("state").?.string);
    }
    {
        var reply = try f.sql("{\"statement\":\"UPDATE checked SET n=-2 WHERE _id='a'\"}");
        defer reply.deinit();
        try std.testing.expectEqualStrings("23514", reply.state().?);
    }
    const before = f.file().activeCheckpoint().commit_sequence;
    try f.expectOk("{\"statement\":\"UPDATE checked SET n=1 WHERE _id='a'\"}");
    try std.testing.expectEqual(before + 1, f.file().activeCheckpoint().commit_sequence);
    {
        var reply = try f.sql("{\"statement\":\"ALTER TABLE checked VALIDATE CONSTRAINT positive\"}");
        defer reply.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
        try std.testing.expectEqualStrings("ready", reply.parsed.?.value.object.get("ddl_receipt").?.object.get("state").?.string);
    }
    try std.testing.expect((try f.file().check()).valid);
}

test "capi SQL prepared text joins primary publication for autocommit and session COMMIT" {
    const api = @import("db.zig");
    const tables = @import("tables.zig");
    const sessions = @import("sql_session.zig");
    for ([_]bool{ false, true }) |session_commit| {
        var f = try OutcomeFixture.init("sql-prepared-text-publication");
        defer f.deinit();
        try f.expectOk("{\"statement\":\"CREATE TABLE words (body TEXT DEFAULT 'alpha canary',n BIGINT DEFAULT 1)\"}");
        var table: ?*anyopaque = null;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(f.id, .fromSlice("words"), &table));
        defer api.antfly_db_close(table);
        for ([_][]const u8{
            "{\"name\":\"first\",\"type\":\"full_text\"}",
            "{\"name\":\"second\",\"type\":\"full_text\"}",
        }) |config| try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(table, .fromSlice(config)));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(f.id));
        var session: u64 = 0;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(f.id, &session));
        defer _ = sessions.antfly_db_sql_session_close(f.id, session);
        const statements = [_][]const u8{
            "INSERT INTO words (_id) VALUES ('row')",
            "UPDATE words SET body='beta canary', n=n+1 WHERE _id='row'",
            "DELETE FROM words WHERE _id='row'",
        };
        for (statements, 0..) |statement, ordinal| {
            if (session_commit) {
                const begin = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = "BEGIN", .session_id = session }, .{});
                defer std.testing.allocator.free(begin);
                try f.expectOk(begin);
            }
            const request = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = statement, .session_id = if (session_commit) @as(?u64, session) else null }, .{});
            defer std.testing.allocator.free(request);
            const before = f.file().activeCheckpoint().commit_sequence;
            try f.expectOk(request);
            if (session_commit) {
                try std.testing.expectEqual(before, f.file().activeCheckpoint().commit_sequence);
                const commit_request = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = "COMMIT", .session_id = session }, .{});
                defer std.testing.allocator.free(commit_request);
                try f.expectOk(commit_request);
            }
            try std.testing.expectEqual(before + 1, f.file().activeCheckpoint().commit_sequence);
            for ([_][]const u8{ "first", "second" }) |name| {
                const query_guard = api.enterHandle(table, .read) orelse return error.InvalidArgument;
                defer query_guard.leave();
                try std.testing.expect(query_guard.entry_error == null);
                const database = query_guard.handle.database();
                const target = database.core.store.lastReplaySequence(0);
                const snapshot = database.core.index_manager.textIndex(name).?.acquireSnapshot();
                defer snapshot.release();
                try std.testing.expectEqual(@as(u32, if (ordinal == 2) 0 else 1), snapshot.liveDocCount());
                try std.testing.expect(try database.core.loadAppliedSequence(std.testing.allocator, name) >= target);
                // SQL TEXT is a keyword field by default. Verify its complete
                // normalized value without changing the configured analyzer.
                var alpha = try api.searchTextOwned(query_guard.handle, name, .{ .match = .{ .field = "body", .text = "alpha canary" } }, 10, 0);
                defer alpha.deinit();
                var beta = try api.searchTextOwned(query_guard.handle, name, .{ .match = .{ .field = "body", .text = "beta canary" } }, 10, 0);
                defer beta.deinit();
                if (alpha.ids.len != @as(usize, if (ordinal == 0) 1 else 0) or beta.ids.len != @as(usize, if (ordinal == 1) 1 else 0)) {
                    std.debug.print("SQL_TEXT session={any} operation={d} alpha={d} beta={d} segments={d}\n", .{ session_commit, ordinal, alpha.ids.len, beta.ids.len, snapshot.segments.len });
                    for (snapshot.segments) |segment| for (segment.reader.fields) |field| {
                        std.debug.print("SQL_TEXT field={s}\n", .{field.name});
                    };
                }
                try std.testing.expectEqual(@as(usize, if (ordinal == 0) 1 else 0), alpha.ids.len);
                try std.testing.expectEqual(@as(usize, if (ordinal == 1) 1 else 0), beta.ids.len);
            }
            if (ordinal == 1) {
                var row = try f.sql("{\"statement\":\"SELECT n FROM words WHERE _id='row'\"}");
                defer row.deinit();
                try std.testing.expectEqual(h.capi.ErrorCode.ok, row.code);
                try std.testing.expectEqualStrings("2", row.parsed.?.value.object.get("rows").?.array.items[0].array.items[0].string);
            }
        }
        try std.testing.expect((try f.file().check()).valid);
    }
}

test "capi SQL outcome native constraints publish foreign namespaces atomically" {
    var f = try OutcomeFixture.init("native-sql-foreign-publication");
    defer f.deinit();
    try f.expectOk("{\"statement\":\"CREATE TABLE native_parents (id TEXT PRIMARY KEY)\"}");
    try f.expectOk("{\"statement\":\"CREATE TABLE native_children (id TEXT PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES native_parents(id))\"}");
    try f.expectOk("{\"statement\":\"INSERT INTO native_parents (_id,id) VALUES ('p','parent')\"}");
    try f.expectOk("{\"statement\":\"SELECT id FROM native_children\"}");
    const before = f.file().activeCheckpoint().commit_sequence;
    try f.expectOk("{\"statement\":\"INSERT INTO native_children (_id,id,parent_id) VALUES ('c','child','parent')\"}");
    try std.testing.expectEqual(before + 1, f.file().activeCheckpoint().commit_sequence);
    var blocked = try f.sql("{\"statement\":\"DELETE FROM native_parents WHERE _id='p'\"}");
    defer blocked.deinit();
    try std.testing.expectEqualStrings("23503", blocked.state().?);
    const before_failure = f.file().activeCheckpoint().commit_sequence;
    var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput };
    fault.arm();
    var failed = try f.sql("{\"statement\":\"DELETE FROM native_children WHERE _id='c'\"}");
    PrimaryFault.disarm();
    defer failed.deinit();
    try std.testing.expectEqualStrings("58000", failed.state().?);
    try std.testing.expectEqual(before_failure, f.file().activeCheckpoint().commit_sequence);
    var still_blocked = try f.sql("{\"statement\":\"DELETE FROM native_parents WHERE _id='p'\"}");
    defer still_blocked.deinit();
    try std.testing.expectEqualStrings("23503", still_blocked.state().?);
    try f.expectOk("{\"statement\":\"DELETE FROM native_children WHERE _id='c'\"}");
    try std.testing.expectEqual(before_failure + 1, f.file().activeCheckpoint().commit_sequence);
    try f.expectOk("{\"statement\":\"DELETE FROM native_parents WHERE _id='p'\"}");
    try f.reopen();
    try f.expectOk("{\"statement\":\"SELECT id FROM native_children\"}");
    try std.testing.expect((try f.file().check()).valid);
}

test "capi SQL outcome native constraints preserve foreign unknown receipts" {
    for ([_]anyerror{ error.InputOutput, error.Canceled }) |injected| {
        var f = try OutcomeFixture.init("native-sql-foreign-unknown");
        defer f.deinit();
        try f.expectOk("{\"statement\":\"CREATE TABLE native_parents (id TEXT PRIMARY KEY)\"}");
        try f.expectOk("{\"statement\":\"CREATE TABLE native_children (id TEXT PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES native_parents(id))\"}");
        try f.expectOk("{\"statement\":\"INSERT INTO native_parents (_id,id) VALUES ('p','parent')\"}");
        try f.expectOk("{\"statement\":\"SELECT id FROM native_children\"}");
        var fault: PrimaryFault = .{ .fixture = &f, .err = injected, .publication = true };
        fault.arm();
        var failed = try f.sql("{\"statement\":\"INSERT INTO native_children (_id,id,parent_id) VALUES ('c','child','parent')\"}");
        PrimaryFault.disarm();
        defer failed.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, failed.code);
        try std.testing.expectEqualStrings("40003", failed.state().?);
        try std.testing.expect(failed.has("outcome_receipt"));
        var fenced = try f.sql("{\"statement\":\"INSERT INTO native_children (_id,id,parent_id) VALUES ('c','child','parent')\"}");
        defer fenced.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.stalled, fenced.code);
        try f.reopen();
        var rows = try f.sql("{\"statement\":\"SELECT id FROM native_children\"}");
        defer rows.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, rows.code);
        try std.testing.expectEqual(@as(usize, 0), rows.parsed.?.value.object.get("rows").?.array.items.len);
        try f.expectOk("{\"statement\":\"INSERT INTO native_children (_id,id,parent_id) VALUES ('c','child','parent')\"}");
        try std.testing.expect((try f.file().check()).valid);
    }
}

test "capi SQL outcome native constraints session publishes all tables in one checkpoint" {
    var f = try OutcomeFixture.init("native-sql-session-publication");
    defer f.deinit();
    try f.expectOk("{\"statement\":\"CREATE TABLE session_left (id TEXT PRIMARY KEY, n BIGINT)\"}");
    try f.expectOk("{\"statement\":\"CREATE TABLE session_right (id TEXT PRIMARY KEY, n BIGINT)\"}");
    try f.expectOk("{\"statement\":\"SELECT id FROM session_left\"}");
    try f.expectOk("{\"statement\":\"SELECT id FROM session_right\"}");
    var session: u64 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(f.id, &session));
    try std.testing.expectEqual(@as(u64, 1), session);
    try f.expectOk("{\"statement\":\"BEGIN\",\"session_id\":1}");
    try f.expectOk("{\"statement\":\"INSERT INTO session_left (_id,id,n) VALUES ('a','left',1)\",\"session_id\":1}");
    try f.expectOk("{\"statement\":\"INSERT INTO session_right (_id,id,n) VALUES ('b','right',2)\",\"session_id\":1}");
    const before = f.file().activeCheckpoint().commit_sequence;
    try f.expectOk("{\"statement\":\"COMMIT\",\"session_id\":1}");
    try std.testing.expectEqual(before + 1, f.file().activeCheckpoint().commit_sequence);
    try f.reopen();
    for ([_][]const u8{ "session_left", "session_right" }) |table| {
        const request = try std.fmt.allocPrint(std.testing.allocator, "{{\"statement\":\"SELECT n FROM {s}\"}}", .{table});
        defer std.testing.allocator.free(request);
        var reply = try f.sql(request);
        defer reply.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
        try std.testing.expectEqual(@as(usize, 1), reply.parsed.?.value.object.get("rows").?.array.items.len);
    }
    try std.testing.expect((try f.file().check()).valid);
}

test "capi SQL outcome native constraints session also publishes document tables" {
    var f = try OutcomeFixture.init("native-sql-document-session");
    defer f.deinit();
    const schema = "{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}}}}}}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_create_table_json(f.id, .fromSlice("native_docs"), .fromSlice(schema)));
    try f.expectOk("{\"statement\":\"SELECT n FROM native_docs\"}");
    var session: u64 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(f.id, &session));
    try std.testing.expectEqual(@as(u64, 1), session);
    try f.expectOk("{\"statement\":\"BEGIN\",\"session_id\":1}");
    try f.expectOk("{\"statement\":\"INSERT INTO native_docs (_id,n) VALUES ('d',3)\",\"session_id\":1}");
    const before = f.file().activeCheckpoint().commit_sequence;
    try f.expectOk("{\"statement\":\"COMMIT\",\"session_id\":1}");
    try std.testing.expectEqual(before + 1, f.file().activeCheckpoint().commit_sequence);
    try f.reopen();
    var reply = try f.sql("{\"statement\":\"SELECT n FROM native_docs WHERE _id='d'\"}");
    defer reply.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
    try std.testing.expectEqualStrings("3", reply.parsed.?.value.object.get("rows").?.array.items[0].array.items[0].string);
    try std.testing.expect((try f.file().check()).valid);
}

/// Table `t` as a receipt names it: its name and current durable id.
fn tableT(id: ?*anyopaque) !@import("outcome_receipts.zig").TableRef {
    const guard = @import("db.zig").enterHandle(id, .read).?;
    defer guard.leave();
    const database = try @import("tables.zig").get(guard.handle, "t");
    return .{ .name = "t", .id = database.core.identity_namespace.table_id };
}

/// Arms one primary-commit fault: fail before the decision with `err`, or
/// (with `publication`) fail inside the decision's checkpoint publication.
const PrimaryFault = struct {
    fixture: *OutcomeFixture,
    err: anyerror,
    publication: bool = false,
    armed: std.atomic.Value(bool) = .init(true),

    fn arm(self: *PrimaryFault) void {
        self.armed.store(true, .release);
        @import("../storage/db/mutation_commit.zig").test_before_primary_commit = .{ .ptr = self, .run = run };
    }
    fn disarm() void {
        @import("../storage/db/mutation_commit.zig").test_before_primary_commit = null;
    }
    fn run(ptr: *anyopaque) anyerror!void {
        const self: *PrimaryFault = @ptrCast(@alignCast(ptr));
        if (!self.armed.swap(false, .acq_rel)) return;
        if (!self.publication) return self.err;
        self.fixture.file().test_fail_checkpoint_publication = self.err;
    }
};

test "capi SQL outcome autocommit failure before the decision is not applied" {
    var f = try OutcomeFixture.init("outcome-autocommit-pre");
    defer f.deinit();
    var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput };
    fault.arm();
    defer PrimaryFault.disarm();
    var reply = try f.sql(insert_one);
    defer reply.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.internal, reply.code);
    try std.testing.expectEqualStrings("58000", reply.state().?);
    try std.testing.expect(!reply.has("transaction_id"));
    try std.testing.expectEqual(@as(usize, 0), try f.rows());
    try f.expectOk(insert_one);
    try std.testing.expectEqual(@as(usize, 1), try f.rows());
}

test "capi SQL outcome autocommit failure inside checkpoint publication is unknown" {
    for ([_]anyerror{ error.InputOutput, error.Canceled }) |injected| {
        var f = try OutcomeFixture.init("outcome-autocommit-publication");
        defer f.deinit();
        var fault: PrimaryFault = .{ .fixture = &f, .err = injected, .publication = true };
        fault.arm();
        defer PrimaryFault.disarm();
        var reply = try f.sql(insert_one);
        defer reply.deinit();
        // Never "cancelled" or a retryable code: the commit may be durable.
        try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, reply.code);
        try std.testing.expectEqualStrings("40003", reply.state().?);
        // The uncertain owner stays fenced. Reopen reconstructs durable
        // truth: this injected failure preceded the checkpoint slot write.
        var refused = try f.sql(insert_one);
        defer refused.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.stalled, refused.code);
        try f.reopen();
        try std.testing.expectEqual(@as(usize, 0), try f.rows());
        try f.expectOk(insert_one);
    }
}

test "capi SQL outcome autocommit failure after the primary commit is committed" {
    const mutation_commit = @import("../storage/db/mutation_commit.zig");
    for ([_]struct { err: anyerror, outcome: []const u8 }{
        .{ .err = error.ConcurrencyUnavailable, .outcome = "committed_pending" },
        .{ .err = error.Busy, .outcome = "committed_pending" },
        // Later uncertainty does not override the confirmed primary write.
        .{ .err = error.OutcomeUnknown, .outcome = "committed_pending" },
        .{ .err = error.EnrichmentWorkerFailed, .outcome = "committed_repair_required" },
    }) |case| {
        var f = try OutcomeFixture.init("outcome-autocommit-post");
        defer f.deinit();
        mutation_commit.test_fail_after_primary_commit = case.err;
        defer mutation_commit.test_fail_after_primary_commit = null;
        var reply = try f.sql(insert_one);
        defer reply.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
        try std.testing.expectEqualStrings(case.outcome, reply.outcome().?);
        try std.testing.expectEqual(@as(usize, 1), try f.rows());
    }
}

const SessionStatements = struct {
    const open = "{\"statement\":\"BEGIN\",\"session_id\":1}";
    const insert = "{\"statement\":\"INSERT INTO t (_id,n) VALUES ('s',2)\",\"session_id\":1}";
    const commit = "{\"statement\":\"COMMIT\",\"session_id\":1}";
    const rollback = "{\"statement\":\"ROLLBACK\",\"session_id\":1}";

    fn stage(f: *OutcomeFixture) !void {
        var session: u64 = 0;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("sql_session.zig").antfly_db_sql_session_open(f.id, &session));
        try std.testing.expectEqual(@as(u64, 1), session);
        try f.expectOk(open);
        try f.expectOk(insert);
    }
};

test "capi SQL outcome COMMIT failures before the decision are not applied" {
    for ([_]struct { err: anyerror, code: h.capi.ErrorCode, state: []const u8 }{
        .{ .err = error.InputOutput, .code = .internal, .state = "58000" },
        .{ .err = error.Busy, .code = .busy, .state = "55P03" },
        .{ .err = error.OutOfMemory, .code = .internal, .state = "53200" },
    }) |case| {
        var f = try OutcomeFixture.init("outcome-commit-pre");
        defer f.deinit();
        try SessionStatements.stage(&f);
        var fault: PrimaryFault = .{ .fixture = &f, .err = case.err };
        fault.arm();
        var reply = try f.sql(SessionStatements.commit);
        defer reply.deinit();
        PrimaryFault.disarm();
        try std.testing.expectEqual(case.code, reply.code);
        try std.testing.expectEqualStrings(case.state, reply.state().?);
        // Not applied: no receipt, nothing stored, and the abandoned
        // decision cannot commit later.
        try std.testing.expect(!reply.has("transaction_id"));
        try f.expectOk(SessionStatements.rollback);
        try std.testing.expectEqual(@as(usize, 0), try f.rows());
        try f.expectOk(insert_one);
        try std.testing.expectEqual(@as(usize, 1), try f.rows());
    }
}

test "capi SQL outcome COMMIT decision failing before publication is not applied" {
    var f = try OutcomeFixture.init("outcome-commit-decision-pre");
    defer f.deinit();
    try SessionStatements.stage(&f);
    var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput };
    fault.arm();
    var reply = try f.sql(SessionStatements.commit);
    defer reply.deinit();
    PrimaryFault.disarm();
    // Pending with no uncertain publication: provably not applied.
    try std.testing.expect(reply.code != .outcome_unknown and reply.code != .ok);
    try std.testing.expect(!reply.has("transaction_id"));
    try f.expectOk(SessionStatements.rollback);
    try std.testing.expectEqual(@as(usize, 0), try f.rows());
    try f.expectOk(insert_one);
}

test "capi SQL outcome COMMIT decision failing inside publication is unknown with a receipt" {
    var f = try OutcomeFixture.init("outcome-commit-decision-uncertain");
    defer f.deinit();
    try SessionStatements.stage(&f);
    var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = true };
    fault.arm();
    var reply = try f.sql(SessionStatements.commit);
    defer reply.deinit();
    PrimaryFault.disarm();
    try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, reply.code);
    try std.testing.expectEqualStrings("40003", reply.state().?);
    try std.testing.expect(!reply.has("transaction_id"));
    try std.testing.expect(reply.has("outcome_receipt"));
}

test "capi SQL outcome COMMIT failure after a committed decision is committed" {
    var f = try OutcomeFixture.init("outcome-commit-post");
    defer f.deinit();
    try SessionStatements.stage(&f);
    const mutation_commit = @import("../storage/db/mutation_commit.zig");
    mutation_commit.test_fail_after_primary_commit = error.InputOutput;
    defer mutation_commit.test_fail_after_primary_commit = null;
    var reply = try f.sql(SessionStatements.commit);
    defer reply.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
    try std.testing.expectEqualStrings("committed_pending", reply.outcome().?);
    try std.testing.expectEqual(@as(usize, 1), try f.rows());
}

test "capi SQL outcome batch export classifies each boundary" {
    const api = @import("db.zig");
    const mutation_commit = @import("../storage/db/mutation_commit.zig");
    const Boundary = enum { pre, publication, post, post_unknown, enrichment };
    for ([_]struct { at: Boundary, code: h.capi.ErrorCode, applied: bool }{
        .{ .at = .pre, .code = .internal, .applied = false },
        .{ .at = .publication, .code = .outcome_unknown, .applied = false },
        .{ .at = .post, .code = .stalled, .applied = true },
        // A later publication of unknown outcome does not override the
        // confirmed primary write.
        .{ .at = .post_unknown, .code = .stalled, .applied = true },
        .{ .at = .enrichment, .code = .stalled, .applied = true },
    }) |case| {
        var f = try OutcomeFixture.init("outcome-batch");
        defer f.deinit();
        var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = case.at == .publication };
        switch (case.at) {
            .pre, .publication => fault.arm(),
            .post => mutation_commit.test_fail_after_primary_commit = error.Busy,
            .post_unknown => mutation_commit.test_fail_after_primary_commit = error.OutcomeUnknown,
            .enrichment => mutation_commit.test_fail_after_primary_commit = error.EnrichmentWorkerFailed,
        }
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&out);
        const code = api.antfly_db_batch_json(f.id, .fromSlice("{\"inserts\":{\"batch-key\":{\"v\":1}}}"), &out);
        PrimaryFault.disarm();
        mutation_commit.test_fail_after_primary_commit = null;
        try std.testing.expectEqual(case.code, code);
        var lookup: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&lookup);
        const found = api.antfly_db_lookup_json(f.id, .fromSlice("batch-key"), &lookup);
        try std.testing.expectEqual(case.applied, found == .ok);
    }
}

test "capi SQL outcome batch on a directory handle is unknown once the LSM write may have applied" {
    const api = @import("db.zig");
    const Backend = @import("../storage/lsm_backend.zig").Backend;
    for ([_]struct { at: @FieldType(Backend.TestCommitFault, "at"), code: h.capi.ErrorCode, applied: bool }{
        // Nothing written yet: an ordinary, retryable failure.
        .{ .at = .before_wal_append, .code = .internal, .applied = false },
        // The WAL record may be durable: never "not applied".
        .{ .at = .after_wal_append, .code = .outcome_unknown, .applied = false },
        // Visible and durable, then a flush step failed.
        .{ .at = .after_publish, .code = .outcome_unknown, .applied = true },
    }) |case| {
        var directory = try api.TestDirectory.init("outcome-lsm-batch");
        defer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}", .{directory.path()}, 0);
        defer std.testing.allocator.free(path);
        var id: ?*anyopaque = null;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
        defer api.antfly_db_close(id);

        Backend.test_commit_fault = .{ .at = case.at, .err = error.InputOutput };
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&out);
        const code = api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"batch-key\":{\"v\":1}}}"), &out);
        const fired = Backend.test_commit_fault == null;
        Backend.test_commit_fault = null;
        try std.testing.expect(fired);
        try std.testing.expectEqual(case.code, code);
        var lookup: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&lookup);
        const found = api.antfly_db_lookup_json(id, .fromSlice("batch-key"), &lookup);
        try std.testing.expectEqual(case.applied, found == .ok);
    }
}

test {
    _ = @import("outcome_receipts.zig");
}

fn lastReceiptAlloc() ![]u8 {
    const api = @import("db.zig");
    var buf: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&buf);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_last_error_outcome_receipt(&buf));
    return std.testing.allocator.dupe(u8, buf.ptr.?[0..buf.len]);
}

fn receiptStatus(id: ?*anyopaque, receipt: []const u8) !struct { code: h.capi.ErrorCode, status: u8 } {
    var status: u8 = 0;
    const code = @import("db.zig").antfly_db_outcome_receipt_status(id, .fromSlice(receipt), &status);
    return .{ .code = code, .status = status };
}

test "capi SQL outcome receipt resolves a Lite autocommit write of unknown outcome" {
    const api = @import("db.zig");
    var f = try OutcomeFixture.init("receipt-autocommit");
    defer f.deinit();
    const receipt = blk: {
        var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = true };
        fault.arm();
        defer PrimaryFault.disarm();
        var reply = try f.sql(insert_one);
        defer reply.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, reply.code);
        const receipt = try lastReceiptAlloc();
        errdefer std.testing.allocator.free(receipt);
        // The SQL error body carries the same receipt, naming table `t`
        // and its durable id, whose database holds the marker.
        try std.testing.expectEqualStrings(receipt, reply.parsed.?.value.object.get("outcome_receipt").?.string);
        const parsed_receipt = try @import("outcome_receipts.zig").parse(receipt);
        const target = parsed_receipt.table().?;
        try std.testing.expectEqualStrings("t", target.name);
        break :blk receipt;
    };
    defer std.testing.allocator.free(receipt);
    // The uncertain owner stays fenced. Reopen before resolving durable truth.
    try std.testing.expectEqual(h.capi.ErrorCode.stalled, (try receiptStatus(f.id, receipt)).code);
    try f.reopen();
    const first = try receiptStatus(f.id, receipt);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, first.code);
    const expected: u8 = if (try f.rows() == 1) @import("db.zig").ANTFLY_OUTCOME_RECEIPT_APPLIED else @import("db.zig").ANTFLY_OUTCOME_RECEIPT_NOT_APPLIED;
    try std.testing.expectEqual(expected, first.status);
    // Later writes use a fresh writer and never move the receipt's marker.
    try f.expectOk("{\"statement\":\"INSERT INTO t (_id,n) VALUES ('later',2)\"}");
    try std.testing.expectEqual(expected, (try receiptStatus(f.id, receipt)).status);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_outcome_receipt_forget(f.id, .fromSlice(receipt)));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, (try receiptStatus(f.id, "or1:not-a-receipt")).code);
}

test "capi SQL outcome receipt resolves against the named table that holds its marker" {
    const receipts = @import("outcome_receipts.zig");
    var f = try OutcomeFixture.init("receipt-named-table");
    defer f.deinit();
    // An applied write to table `t` commits its marker in t's database.
    try f.expectOk(insert_one);
    const writer = &f.wrapper.outcome_writer;
    const marker: receipts.Marker = .{ .writer = writer.id.?, .sequence = writer.sequence };
    var buf: [receipts.max_text_len]u8 = undefined;
    const named = receipts.Receipt.init(marker, try tableT(f.id));
    const applied = try receiptStatus(f.id, receipts.format(&named, &buf));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, applied.code);
    try std.testing.expectEqual(@import("db.zig").ANTFLY_OUTCOME_RECEIPT_APPLIED, applied.status);
    // The root database never saw it: reading there would wrongly answer
    // "not applied", which is the mistake a receipt without its table makes.
    var root_buf: [receipts.max_text_len]u8 = undefined;
    const root = receipts.Receipt.init(marker, null);
    try std.testing.expectEqual(@import("db.zig").ANTFLY_OUTCOME_RECEIPT_NOT_APPLIED, (try receiptStatus(f.id, receipts.format(&root, &root_buf))).status);
    // An unknown table is an error, not "not applied".
    var missing_buf: [receipts.max_text_len]u8 = undefined;
    const missing = receipts.Receipt.init(marker, .{ .name = "no_such_table", .id = 1 });
    try std.testing.expect((try receiptStatus(f.id, receipts.format(&missing, &missing_buf))).code != .ok);
}

test "capi SQL outcome receipt for a dropped and recreated table fails closed" {
    const receipts = @import("outcome_receipts.zig");
    var f = try OutcomeFixture.init("receipt-recreated-table");
    defer f.deinit();
    try f.expectOk(insert_one);
    const writer = &f.wrapper.outcome_writer;
    const marker: receipts.Marker = .{ .writer = writer.id.?, .sequence = writer.sequence };
    const original = try tableT(f.id);
    var buf: [receipts.max_text_len]u8 = undefined;
    const text = receipts.format(&receipts.Receipt.init(marker, original), &buf);
    try std.testing.expectEqual(@import("db.zig").ANTFLY_OUTCOME_RECEIPT_APPLIED, (try receiptStatus(f.id, text)).status);
    // The write applied to the old `t`. A new `t` holds no marker for it, so
    // reading there would wrongly answer "not applied" and invite a retry.
    try f.expectOk("{\"statement\":\"DROP TABLE t\"}");
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, (try receiptStatus(f.id, text)).code);
    try f.expectOk("{\"statement\":\"CREATE TABLE t (n BIGINT)\"}");
    try std.testing.expect((try tableT(f.id)).id != original.id);
    const replaced = try receiptStatus(f.id, text);
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, replaced.code);
    try std.testing.expectEqual(@as(u8, 0), replaced.status);
    try std.testing.expect(@import("db.zig").antfly_db_outcome_receipt_forget(f.id, .fromSlice(text)) != .ok);
}

test "capi SQL outcome receipt markers in named tables are removed at a clean close" {
    const api = @import("db.zig");
    var f = try OutcomeFixture.init("receipt-named-close");
    defer f.deinit();
    try f.expectOk(insert_one);
    const writer = f.wrapper.outcome_writer.id.?;
    {
        const guard = api.enterHandle(f.id, .read).?;
        defer guard.leave();
        const table = try @import("tables.zig").get(guard.handle, "t");
        try std.testing.expect(try table.readOutcomeMarker(writer) != null);
    }
    api.antfly_db_close(f.id);
    const reopened = try api.openLiteHandleAlloc(f.path, .{ .no_sync = true, .profile = .hosted, .busy_timeout_ms = 5000 }, false);
    f.id = try api.publishHandle(reopened);
    const guard = api.enterHandle(f.id, .read).?;
    defer guard.leave();
    const table = try @import("tables.zig").get(guard.handle, "t");
    try std.testing.expectEqual(@as(?u64, null), try table.readOutcomeMarker(writer));
}

test "capi SQL outcome receipt from a table handle batch names that table" {
    const api = @import("db.zig");
    const receipts = @import("outcome_receipts.zig");
    var f = try OutcomeFixture.init("receipt-table-handle");
    defer f.deinit();
    var table_id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_open_table(f.id, .fromSlice("t"), &table_id));
    defer api.antfly_db_close(table_id);
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(table_id, .fromSlice("{\"inserts\":{\"handle-key\":{\"n\":1}}}"), &out));
    // The table handle shares its database handle's writer.
    const writer = &f.wrapper.outcome_writer;
    const marker: receipts.Marker = .{ .writer = writer.id.?, .sequence = writer.sequence };
    var buf: [receipts.max_text_len]u8 = undefined;
    const named = receipts.Receipt.init(marker, try tableT(f.id));
    const status = try receiptStatus(f.id, receipts.format(&named, &buf));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, status.code);
    try std.testing.expectEqual(api.ANTFLY_OUTCOME_RECEIPT_APPLIED, status.status);
}

test "capi SQL outcome receipt resolves the same through a table handle as through the root" {
    const api = @import("db.zig");
    const receipts = @import("outcome_receipts.zig");
    var f = try OutcomeFixture.init("receipt-via-table-handle");
    defer f.deinit();
    const writer = &f.wrapper.outcome_writer;
    // One applied write to the root database, one to table `t`.
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(f.id, .fromSlice("{\"inserts\":{\"root-key\":{\"v\":1}}}"), &out));
    var root_buf: [receipts.max_text_len]u8 = undefined;
    const root_receipt = receipts.format(&receipts.Receipt.init(.{ .writer = writer.id.?, .sequence = writer.sequence }, null), &root_buf);
    try f.expectOk(insert_one);
    var named_buf: [receipts.max_text_len]u8 = undefined;
    const named_receipt = receipts.format(&receipts.Receipt.init(.{ .writer = writer.id.?, .sequence = writer.sequence }, try tableT(f.id)), &named_buf);

    var table_id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_open_table(f.id, .fromSlice("t"), &table_id));
    defer api.antfly_db_close(table_id);
    for ([_]?*anyopaque{ f.id, table_id }) |via| {
        for ([_][]const u8{ root_receipt, named_receipt }) |receipt| {
            const status = try receiptStatus(via, receipt);
            try std.testing.expectEqual(h.capi.ErrorCode.ok, status.code);
            try std.testing.expectEqual(api.ANTFLY_OUTCOME_RECEIPT_APPLIED, status.status);
        }
    }
    // Forget routes the same way.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_outcome_receipt_forget(table_id, .fromSlice(named_receipt)));
    try std.testing.expectEqual(api.ANTFLY_OUTCOME_RECEIPT_NOT_APPLIED, (try receiptStatus(f.id, named_receipt)).status);
}

test "capi SQL outcome receipt for a named table on a directory handle resolves after reopen through the root" {
    const api = @import("db.zig");
    const Backend = @import("../storage/lsm_backend.zig").Backend;
    var directory = try api.TestDirectory.init("receipt-lsm-named");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}", .{directory.path()}, 0);
    defer std.testing.allocator.free(path);
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
    var open = true;
    defer if (open) api.antfly_db_close(id);
    var created: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&created);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"CREATE TABLE t (n BIGINT)\"}"), &created));
    // The write is published to t's memtable, then a flush step fails.
    Backend.test_commit_fault = .{ .at = .after_publish, .err = error.InputOutput };
    var reply: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&reply);
    const code = api.antfly_db_sql_json(id, .fromSlice(insert_one), &reply);
    Backend.test_commit_fault = null;
    try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, code);
    const receipt = try lastReceiptAlloc();
    defer std.testing.allocator.free(receipt);
    const parsed_receipt = try @import("outcome_receipts.zig").parse(receipt);
    const target = parsed_receipt.table().?;
    try std.testing.expectEqualStrings("t", target.name);
    api.antfly_db_close(id);
    open = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
    open = true;
    // Queried through the root handle, it resolves in t's database.
    const resolved = try receiptStatus(id, receipt);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, resolved.code);
    try std.testing.expectEqual(api.ANTFLY_OUTCOME_RECEIPT_APPLIED, resolved.status);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_outcome_receipt_forget(id, .fromSlice(receipt)));
}

test "capi SQL outcome receipt for a Lite batch export of unknown outcome" {
    const api = @import("db.zig");
    var f = try OutcomeFixture.init("receipt-batch");
    defer f.deinit();
    var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = true };
    fault.arm();
    defer PrimaryFault.disarm();
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    const code = api.antfly_db_batch_json(f.id, .fromSlice("{\"inserts\":{\"receipt-key\":{\"v\":1}}}"), &out);
    PrimaryFault.disarm();
    try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, code);
    const receipt = try lastReceiptAlloc();
    defer std.testing.allocator.free(receipt);
    try std.testing.expectEqual(h.capi.ErrorCode.stalled, (try receiptStatus(f.id, receipt)).code);
    try f.reopen();
    const resolved = try receiptStatus(f.id, receipt);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, resolved.code);
    var lookup: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&lookup);
    const found = api.antfly_db_lookup_json(f.id, .fromSlice("receipt-key"), &lookup) == .ok;
    // The same-call variant returns a receipt in its output parameter.
    var again: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = true };
    again.arm();
    defer PrimaryFault.disarm();
    var out2: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out2);
    var receipt_out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&receipt_out);
    try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, api.antfly_db_batch_json_with_receipt(f.id, .fromSlice("{\"inserts\":{\"receipt-key-2\":{\"v\":1}}}"), &out2, &receipt_out));
    PrimaryFault.disarm();
    const second = receipt_out.ptr.?[0..receipt_out.len];
    try std.testing.expect(std.mem.startsWith(u8, second, "or1:"));
    try std.testing.expect(!std.mem.eql(u8, second, receipt));
    try std.testing.expectEqual(if (found) api.ANTFLY_OUTCOME_RECEIPT_APPLIED else api.ANTFLY_OUTCOME_RECEIPT_NOT_APPLIED, resolved.status);
    // The second uncertain write fenced this owner. A third mutation needs
    // a fresh recovery boundary, including for the struct same-call variant.
    try f.reopen();
    var third_fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = true };
    third_fault.arm();
    defer PrimaryFault.disarm();
    var struct_receipt: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&struct_receipt);
    const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("receipt-key-3"), .value = .fromSlice("{\"v\":1}") }};
    try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, api.antfly_db_batch_with_receipt(f.id, &writes, writes.len, null, 0, 0, 0, &struct_receipt));
    PrimaryFault.disarm();
    const third = struct_receipt.ptr.?[0..struct_receipt.len];
    try std.testing.expect(std.mem.startsWith(u8, third, "or1:"));
    try std.testing.expect(!std.mem.eql(u8, third, second));
}

test "capi SQL outcome receipt on a directory handle stays pending until reopen, then resolves" {
    const api = @import("db.zig");
    const Backend = @import("../storage/lsm_backend.zig").Backend;
    for ([_]struct { at: @FieldType(Backend.TestCommitFault, "at"), receipt: bool }{
        .{ .at = .before_wal_append, .receipt = false },
        .{ .at = .after_wal_append, .receipt = true },
        .{ .at = .after_publish, .receipt = true },
    }) |case| {
        var directory = try api.TestDirectory.init("receipt-lsm");
        defer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}", .{directory.path()}, 0);
        defer std.testing.allocator.free(path);
        var id: ?*anyopaque = null;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
        var open = true;
        defer if (open) api.antfly_db_close(id);

        Backend.test_commit_fault = .{ .at = case.at, .err = error.InputOutput };
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&out);
        _ = api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"receipt-key\":{\"v\":1}}}"), &out);
        Backend.test_commit_fault = null;
        if (!case.receipt) {
            var none: h.capi.Buffer = .{};
            try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_last_error_outcome_receipt(&none));
            continue;
        }
        const receipt = try lastReceiptAlloc();
        defer std.testing.allocator.free(receipt);
        // The WAL record may be durable but unpublished: not settled yet.
        try std.testing.expectEqual(h.capi.ErrorCode.stalled, (try receiptStatus(id, receipt)).code);
        api.antfly_db_close(id);
        open = false;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
        open = true;
        const resolved = try receiptStatus(id, receipt);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, resolved.code);
        var lookup: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&lookup);
        const found = api.antfly_db_lookup_json(id, .fromSlice("receipt-key"), &lookup) == .ok;
        // Whichever way it settled (a clean close checkpoints and resets the
        // WAL, dropping an unpublished record; a crash would replay it), the
        // receipt reports what is actually stored.
        try std.testing.expectEqual(if (found) api.ANTFLY_OUTCOME_RECEIPT_APPLIED else api.ANTFLY_OUTCOME_RECEIPT_NOT_APPLIED, resolved.status);
        // A write that was published before its flush failed stays applied.
        if (case.at == .after_publish) try std.testing.expect(found);
    }
}

test "capi SQL outcome receipt marker of a cleanly closed handle is removed" {
    const api = @import("db.zig");
    var directory = try api.TestDirectory.init("receipt-close");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}", .{directory.path()}, 0);
    defer std.testing.allocator.free(path);
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"k\":{\"v\":1}}}"), &out));
    const writer = blk: {
        const guard = api.enterHandle(id, .read).?;
        defer guard.leave();
        const writer = guard.lock_handle.outcome_writer.id.?;
        try std.testing.expectEqual(@as(?u64, 1), try guard.handle.database().readOutcomeMarker(writer));
        break :blk writer;
    };
    api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open(path, &id));
    defer api.antfly_db_close(id);
    const guard = api.enterHandle(id, .read).?;
    defer guard.leave();
    try std.testing.expectEqual(@as(?u64, null), try guard.handle.database().readOutcomeMarker(writer));
}

test "capi SQL outcome refusals after an unresolved decision are not unknown" {
    const errors = dependencies.sql_errors;
    for ([_]anyerror{ error.SqlReconciliationRequired, error.OutcomeReconciliationRequired }) |err| {
        try std.testing.expectEqualStrings("55000", errors.describe(err).code);
        try std.testing.expect(!errors.isUnknownOutcome(err));
    }
    // A pre-decision step that observes the sticky flag reports a refusal.
    try std.testing.expectEqual(error.SqlReconciliationRequired, errors.notApplied(error.OutcomeUnknown));
    try std.testing.expectEqual(error.SqlMutationNotAppliedBusy, errors.notApplied(error.Busy));
    try std.testing.expectEqual(error.SqlMutationNotApplied, errors.notApplied(error.InputOutput));
    // Errors with their own refusal diagnostics keep them.
    try std.testing.expectEqual(error.FileBusy, errors.notApplied(error.FileBusy));
    try std.testing.expectEqual(error.SqlWriteConflict, errors.notApplied(error.SqlWriteConflict));
    // Committed-pending visibility is not an unknown outcome.
    try std.testing.expectEqualStrings("AF001", errors.describe(error.CommitVisibilityNotSatisfied).code);
}
