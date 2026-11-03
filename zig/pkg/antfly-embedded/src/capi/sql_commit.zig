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
fn commitLocked(handle: *h.Handle, requests: []const contract.TableCommitRequest, out_id: *?h.db_mod.types.TxnId) !catalog.MutationOutcome {
    // Participants were admitted before preparing this decision. Replaying or
    // publishing it must not recursively start a namespace's DDL coordinator.
    const was_recovering = handle.sql_ddl_recovering;
    handle.sql_ddl_recovering = true;
    defer handle.sql_ddl_recovering = was_recovering;
    if (requests.len == 0) return .committed;
    out_id.* = null;
    const outcomes = dependencies.sql_errors;
    const id = prepareDecision(handle, requests) catch |err| return outcomes.notApplied(err);
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
fn prepareDecision(handle: *h.Handle, requests: []const contract.TableCommitRequest) !PreparedDecision {
    try recoverLocked(handle);
    const io = handle.db.backend_runtime.io() orelse return error.UnsupportedSqlExecution;
    const now: u64 = @intCast(@max(0, std.Io.Clock.real.now(io).nanoseconds));
    var id: h.db_mod.types.TxnId = undefined;
    std.Io.random(io, &id);
    const names = try handle.alloc.alloc([]const u8, requests.len);
    defer handle.alloc.free(names);
    for (requests, names) |request, *name| name.* = request.table_name;
    const record: Record = .{ .id = id, .timestamp = now, .tables = names };
    if (comptime h.builtin.is_test) if (test_before_store) |hook| try hook.run(hook.ptr);
    try store(handle, record);
    // From here a failure leaves a hint and possibly pending intents; abort
    // them through recovery (also retried at the next entry if this fails).
    errdefer recoverLocked(handle) catch {};
    // Publish a native durable coordinator before preparing any participant.
    _ = try handle.db.beginTransactionScoped(id, now, now, names, true, true, null);
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
    return .{ .id = id, .now = now };
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

/// Arms one primary-commit fault: fail before the decision with `err`, or
/// (with `publication`) fail inside the decision's checkpoint publication.
const PrimaryFault = struct {
    fixture: *OutcomeFixture,
    err: anyerror,
    publication: bool = false,

    fn arm(self: *PrimaryFault) void {
        @import("../storage/db/mutation_commit.zig").test_before_primary_commit = .{ .ptr = self, .run = run };
    }
    fn disarm() void {
        @import("../storage/db/mutation_commit.zig").test_before_primary_commit = null;
    }
    fn run(ptr: *anyopaque) anyerror!void {
        const self: *PrimaryFault = @ptrCast(@alignCast(ptr));
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
        // The connection reconciles on its next call: the injected failure
        // came before the slot write, so the row is durably absent.
        try std.testing.expectEqual(@as(usize, 0), try f.rows());
        try f.expectOk(insert_one);
    }
}

test "capi SQL outcome autocommit failure after the primary commit is committed" {
    const mutation_commit = @import("../storage/db/mutation_commit.zig");
    for ([_]struct { err: anyerror, outcome: []const u8 }{
        .{ .err = error.ConcurrencyUnavailable, .outcome = "committed_pending" },
        .{ .err = error.Busy, .outcome = "committed_pending" },
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

const FailWith = struct {
    err: anyerror,
    fn run(ptr: *anyopaque) anyerror!void {
        const self: *FailWith = @ptrCast(@alignCast(ptr));
        return self.err;
    }
};

test "capi SQL outcome COMMIT failures before the decision are not applied" {
    const Boundary = enum { before_store, after_prepare };
    for ([_]struct { at: Boundary, err: anyerror, code: h.capi.ErrorCode, state: []const u8 }{
        .{ .at = .before_store, .err = error.InputOutput, .code = .internal, .state = "58000" },
        .{ .at = .after_prepare, .err = error.Busy, .code = .busy, .state = "55P03" },
        .{ .at = .after_prepare, .err = error.InputOutput, .code = .internal, .state = "58000" },
    }) |case| {
        var f = try OutcomeFixture.init("outcome-commit-pre");
        defer f.deinit();
        try SessionStatements.stage(&f);
        var fail: FailWith = .{ .err = case.err };
        switch (case.at) {
            .before_store => test_before_store = .{ .ptr = &fail, .run = FailWith.run },
            .after_prepare => test_after_prepare = .{ .ptr = &fail, .run = FailWith.run },
        }
        var reply = try f.sql(SessionStatements.commit);
        defer reply.deinit();
        test_before_store = null;
        test_after_prepare = null;
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
    // The coordinator decision is the next primary batch after preparing.
    const Arm = struct {
        fault: PrimaryFault,
        fn run(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fault.arm();
        }
    };
    var arm: Arm = .{ .fault = .{ .fixture = &f, .err = error.InputOutput } };
    test_after_prepare = .{ .ptr = &arm, .run = Arm.run };
    var reply = try f.sql(SessionStatements.commit);
    defer reply.deinit();
    test_after_prepare = null;
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
    const Arm = struct {
        fault: PrimaryFault,
        fn run(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fault.arm();
        }
    };
    var arm: Arm = .{ .fault = .{ .fixture = &f, .err = error.InputOutput, .publication = true } };
    test_after_prepare = .{ .ptr = &arm, .run = Arm.run };
    var reply = try f.sql(SessionStatements.commit);
    defer reply.deinit();
    test_after_prepare = null;
    PrimaryFault.disarm();
    try std.testing.expectEqual(h.capi.ErrorCode.outcome_unknown, reply.code);
    try std.testing.expectEqualStrings("40003", reply.state().?);
    try std.testing.expect(reply.has("transaction_id"));
}

test "capi SQL outcome COMMIT failure after a committed decision is committed" {
    var f = try OutcomeFixture.init("outcome-commit-post");
    defer f.deinit();
    try SessionStatements.stage(&f);
    var fail: FailWith = .{ .err = error.InputOutput };
    test_after_decision = .{ .ptr = &fail, .run = FailWith.run };
    var reply = try f.sql(SessionStatements.commit);
    defer reply.deinit();
    test_after_decision = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, reply.code);
    try std.testing.expectEqualStrings("committed_pending", reply.outcome().?);
    try std.testing.expectEqual(@as(usize, 1), try f.rows());
}

test "capi SQL outcome batch export classifies each boundary" {
    const api = @import("db.zig");
    const mutation_commit = @import("../storage/db/mutation_commit.zig");
    const Boundary = enum { pre, publication, post, enrichment };
    for ([_]struct { at: Boundary, code: h.capi.ErrorCode, applied: bool }{
        .{ .at = .pre, .code = .internal, .applied = false },
        .{ .at = .publication, .code = .outcome_unknown, .applied = false },
        .{ .at = .post, .code = .stalled, .applied = true },
        .{ .at = .enrichment, .code = .stalled, .applied = true },
    }) |case| {
        var f = try OutcomeFixture.init("outcome-batch");
        defer f.deinit();
        var fault: PrimaryFault = .{ .fixture = &f, .err = error.InputOutput, .publication = case.at == .publication };
        switch (case.at) {
            .pre, .publication => fault.arm(),
            .post => mutation_commit.test_fail_after_primary_commit = error.Busy,
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
