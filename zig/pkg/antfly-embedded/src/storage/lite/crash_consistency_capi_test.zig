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

//! Crash-consistency coverage for Lite's write path through the full capi
//! `Handle` (`antfly_db_sql_json`) rather than through `db_mod.DB` directly.
//! `crash_consistency_test.zig`'s own `via_db` scenarios open with
//! `openDbWithIo` (a bare `db_mod.DB`, no SQL surface); this file opens the
//! same way a production SQL connection does -- a `.manual` `BackendRuntime`
//! with the sim's passthrough IO, exactly `storage/lite/
//! write_amplification_bench_test.zig`'s `Fixture.open` recipe -- so
//! `commitCoordinatedLocal`'s fused SQL autocommit path (lite-single-
//! commit-write-path.md §3.3c) is exercised by the sweep itself, not just
//! by a direct, uncrashed repro.
//!
//! Background: a prior investigation reported that, on a table with a
//! UNIQUE constraint, INSERT item-0 / INSERT item-1 / UPDATE item-0 left
//! item-1 absent at a post-run check while item-0 read back fine, through a
//! harness built like this one. Two direct repros since (`capi/
//! sql_commit.zig`'s "capi SQL outcome relational unique autocommit ..."
//! tests, one through production `openLiteHandleAlloc`, one through this
//! same `.manual`/sim-IO open path) both pass cleanly on an uncrashed run,
//! so that loss was concluded to be a bug in the harness's own checkpoint
//! code -- most likely reading the wrong (root document) namespace for a
//! relational row's key, since a relational table's rows live under an
//! encoded, table-scoped physical key, never at the raw `_id` string
//! `checkCheckpointsViaDb`'s `handle.db.lookup` expects. This file's
//! `checkViaSql` avoids that failure mode by reading back through
//! `SELECT ... WHERE _id = ...`, the same surface the write went through,
//! instead of any native/root-namespace lookup.
//!
//! Run with (Debug only; filtered):
//!   cd zig && zig build capi-contract-test -Dtest-filter="crash sweep: relational"

const std = @import("std");
const Allocator = std.mem.Allocator;

const crash = @import("crash_consistency_test.zig");
const api = @import("../../capi/db.zig");
const h = @import("../../capi/handles.zig");
const db_mod = api.db_mod;

pub const Scenario = crash.Scenario;
pub const Checkpoint = crash.Checkpoint;
pub const Expectation = crash.Expectation;
pub const runCrashSweep = crash.runCrashSweep;
pub const reportFailures = crash.reportFailures;

/// The full capi `Handle` plus the `.manual` `BackendRuntime` it needs,
/// opened with injectable `io`. `capi/db.zig`'s `LiteResolvedOpenOptions`
/// (what `openLiteHandleAlloc`/`OutcomeFixture` take) has no `io` field;
/// only `openLiteHandleWithRuntime`'s `borrowed_io`/`backend_runtime`
/// threads the sim's IO all the way down to native file IO.
const CapiHandle = struct {
    runtime: db_mod.background_runtime.BackendRuntimeHandle,
    id: ?*anyopaque,

    fn open(alloc: Allocator, path: []const u8, io: std.Io, create: bool) !CapiHandle {
        var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{
            .backend = .manual,
            .borrowed_io = .{ .general = io },
        });
        errdefer runtime.deinit();
        const id = try api.openLiteHandleWithRuntime(alloc, path, io, runtime.ptr(), .{
            .create = create,
            .hosted = false,
            .no_sync = false,
        });
        return .{ .runtime = runtime, .id = id };
    }

    fn close(self: *CapiHandle) void {
        api.closeLiteRuntimeHandle(self.id);
        self.runtime.deinit();
    }
};

fn sqlOk(id: ?*anyopaque, request: []const u8) !void {
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const code = api.antfly_db_sql_json(id, .fromSlice(request), &output);
    if (code != .ok) {
        std.debug.print("crash-consistency capi: unexpected {s} for {s}: {s}\n", .{ @tagName(code), request, std.mem.span(h.capi.lastErrorDetail()) });
        return error.CapiSqlUnexpectedResult;
    }
}

fn jsonToI64(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |v| v,
        .float => |v| @intFromFloat(v),
        .string => |v| try std.fmt.parseInt(i64, v, 10),
        .number_string => |v| try std.fmt.parseInt(i64, v, 10),
        else => error.UnexpectedJsonType,
    };
}

/// `n`'s value for the row named `id_value` in table `t`, or `null` if no
/// such row exists. Any other SQL failure -- including the shim's injected
/// crash error -- propagates, per `Scenario.run`'s contract.
fn selectN(alloc: Allocator, id: ?*anyopaque, id_value: []const u8) !?i64 {
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const request = try std.fmt.allocPrint(alloc, "{{\"statement\":\"SELECT n FROM t WHERE _id = '{s}'\"}}", .{id_value});
    defer alloc.free(request);
    const code = api.antfly_db_sql_json(id, .fromSlice(request), &output);
    if (code != .ok) {
        std.debug.print("crash-consistency capi: SELECT failed {s}: {s}\n", .{ @tagName(code), std.mem.span(h.capi.lastErrorDetail()) });
        return error.CapiSqlUnexpectedResult;
    }
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
    defer parsed.deinit();
    const rows = parsed.value.object.get("rows").?.array.items;
    if (rows.len == 0) return null;
    return try jsonToI64(rows[0].array.items[0]);
}

fn checkpointCoversKey(cp: Checkpoint, key: []const u8) bool {
    for (cp.expectations) |exp| if (std.mem.eql(u8, exp.key, key)) return true;
    return false;
}

fn parseExpected(text: ?[]const u8) !?i64 {
    return if (text) |t| try std.fmt.parseInt(i64, t, 10) else null;
}

/// `Scenario.custom_check`: every `Expectation.key` names a SQL `_id`;
/// `before`/`after` are `n`'s decimal text (or `null` for absent). Reopens
/// through the exact same capi-handle/sim-io path `run` used and compares
/// via `SELECT n FROM t WHERE _id = ...`, not `db.lookup` -- see this
/// file's header comment for why `db.lookup` cannot resolve a relational
/// row's key. Mirrors `checkCheckpointsViaNative`'s split/compare
/// semantics (every acknowledged checkpoint's `after` must hold exactly;
/// the in-flight one must resolve atomically to either `before` or
/// `after`, consistently across its own keys).
fn checkViaSql(allocator: Allocator, path: []const u8, outcome: ?crash.CrashOutcome, checkpoints: []const Checkpoint) !void {
    const acknowledged = if (outcome == null or checkpoints.len == 0) checkpoints else checkpoints[0 .. checkpoints.len - 1];
    const in_flight: ?Checkpoint = if (outcome == null or checkpoints.len == 0) null else checkpoints[checkpoints.len - 1];

    var handle = CapiHandle.open(allocator, path, std.testing.io, false) catch |err| {
        if (acknowledged.len != 0) return err;
        return;
    };
    defer handle.close();

    // Last acknowledged `after` per key (the UPDATE on item-0 keeps only
    // its most recent value).
    var required = std.StringHashMapUnmanaged(?i64).empty;
    defer required.deinit(allocator);
    for (acknowledged) |cp| for (cp.expectations) |exp| {
        try required.put(allocator, exp.key, try parseExpected(exp.after));
    };

    var it = required.iterator();
    while (it.next()) |entry| {
        // A key the in-flight checkpoint also names is covered by the
        // atomic check below instead: that checkpoint's own `before` is
        // exactly this acknowledged value.
        if (in_flight) |cp| if (checkpointCoversKey(cp, entry.key_ptr.*)) continue;
        const got = try selectN(allocator, handle.id, entry.key_ptr.*);
        if (got != entry.value_ptr.*) {
            std.debug.print("crash-consistency capi: acknowledged row not visible id={s} want={?d} got={?d}\n", .{ entry.key_ptr.*, entry.value_ptr.*, got });
            return error.CrashConsistencyAcknowledgedWriteLost;
        }
    }

    if (in_flight) |cp| {
        var resolution: ?bool = null; // true => resolved to `after`
        for (cp.expectations) |exp| {
            const got = try selectN(allocator, handle.id, exp.key);
            const want_after = try parseExpected(exp.after);
            const want_before = try parseExpected(exp.before);
            const matches_after = got == want_after;
            const matches_before = got == want_before;
            if (!matches_after and !matches_before) {
                std.debug.print("crash-consistency capi: torn row id={s} got={?d} allowed_before={?d} allowed_after={?d}\n", .{ exp.key, got, want_before, want_after });
                return error.CrashConsistencyTornWrite;
            }
            if (matches_after and matches_before) continue;
            if (resolution) |r| {
                if (r != matches_after) {
                    std.debug.print("crash-consistency capi: batch not atomic id={s}\n", .{exp.key});
                    return error.CrashConsistencyBatchNotAtomic;
                }
            } else resolution = matches_after;
        }
    }
}

/// INSERT item-0, INSERT item-1, UPDATE item-0 on a table with a UNIQUE
/// constraint, autocommit throughout -- the exact scenario the earlier
/// (unresolved, reverted) report concerned.
fn runRelationalUniqueAutocommit(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try CapiHandle.open(allocator, path, io, true);
    defer handle.close();

    try sqlOk(handle.id, "{\"statement\":\"CREATE TABLE t (n BIGINT UNIQUE)\"}");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "item-0", .before = null, .after = "100" }} });
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO t (_id,n) VALUES ('item-0',100)\"}");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "item-1", .before = null, .after = "200" }} });
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO t (_id,n) VALUES ('item-1',200)\"}");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "item-0", .before = "100", .after = "150" }} });
    try sqlOk(handle.id, "{\"statement\":\"UPDATE t SET n = 150 WHERE _id = 'item-0'\"}");
}

test "crash sweep: relational unique autocommit two inserts and an update" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "relational-unique-autocommit",
        .run = runRelationalUniqueAutocommit,
        .custom_check = checkViaSql,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

/// An explicit multi-statement session (BEGIN/INSERT/INSERT/COMMIT) on the
/// fused path (lite-single-commit-write-path.md §3.3c, task 3:
/// `sql_session.zig`'s `commit` now routes a single-table session COMMIT
/// through `commitCoordinatedLocal` too). BEGIN and the staged INSERTs do
/// no native I/O at all (purely in-memory session state), so every
/// barrier this sweep finds is inside table creation or the COMMIT call
/// itself -- exactly the fused single native commit this task adds.
/// Both rows must land together or neither must, at every sync point.
fn runRelationalSessionCommit(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try CapiHandle.open(allocator, path, io, true);
    defer handle.close();

    try sqlOk(handle.id, "{\"statement\":\"CREATE TABLE t (n BIGINT)\"}");

    var session: u64 = 0;
    const session_code = @import("../../capi/sql_session.zig").antfly_db_sql_session_open(handle.id, &session);
    if (session_code != .ok) return error.CapiSqlUnexpectedResult;

    try sqlOk(handle.id, "{\"statement\":\"BEGIN\",\"session_id\":1}");
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO t (_id,n) VALUES ('row-0',10)\",\"session_id\":1}");
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO t (_id,n) VALUES ('row-1',20)\",\"session_id\":1}");

    try checkpoints.append(allocator, .{ .expectations = &.{
        .{ .key = "row-0", .before = null, .after = "10" },
        .{ .key = "row-1", .before = null, .after = "20" },
    } });
    try sqlOk(handle.id, "{\"statement\":\"COMMIT\",\"session_id\":1}");
}

test "crash sweep: relational session COMMIT on the fused path" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "relational-session-commit",
        .run = runRelationalSessionCommit,
        .custom_check = checkViaSql,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

fn execOutcome(alloc: Allocator, id: ?*anyopaque, statement: []const u8) !h.capi.ErrorCode {
    const request = try std.fmt.allocPrint(alloc, "{{\"statement\":\"{s}\"}}", .{statement});
    defer alloc.free(request);
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    return api.antfly_db_sql_json(id, .fromSlice(request), &output);
}

/// `parent_id`'s value for the child row named `id_value` in table
/// `children`, or `null` if no such row exists.
fn selectChildParentId(alloc: Allocator, id: ?*anyopaque, id_value: []const u8) !?i64 {
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const request = try std.fmt.allocPrint(alloc, "{{\"statement\":\"SELECT parent_id FROM children WHERE _id = '{s}'\"}}", .{id_value});
    defer alloc.free(request);
    const code = api.antfly_db_sql_json(id, .fromSlice(request), &output);
    if (code != .ok) {
        std.debug.print("crash-consistency capi FK: SELECT failed {s}: {s}\n", .{ @tagName(code), std.mem.span(h.capi.lastErrorDetail()) });
        return error.CapiSqlUnexpectedResult;
    }
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
    defer parsed.deinit();
    const rows = parsed.value.object.get("rows").?.array.items;
    if (rows.len == 0) return null;
    return try jsonToI64(rows[0].array.items[0]);
}

/// `Scenario.custom_check` for the FK child-insert scenario below: the
/// same acknowledged/in-flight split and atomicity check as `checkViaSql`,
/// reading `children.parent_id` instead of `t.n`, plus the invariant
/// `commitCoordinatedFusedMultiTable` (lite-single-commit-write-path.md
/// §7.5) exists to preserve: if a child row ended up durably committed,
/// its `.attach` Reference -- written as a ride-along commit against the
/// parent's own store, swept into the child's own foreground fsync --
/// must have survived WITH it, never independently lost. That is checked
/// by attempting to DELETE the one parent every acknowledged child
/// references: RESTRICT must still refuse it, proving the Reference
/// record is durable, not just the child row by itself.
fn checkViaSqlForeignKeyChild(allocator: Allocator, path: []const u8, outcome: ?crash.CrashOutcome, checkpoints: []const Checkpoint) !void {
    const acknowledged = if (outcome == null or checkpoints.len == 0) checkpoints else checkpoints[0 .. checkpoints.len - 1];
    const in_flight: ?Checkpoint = if (outcome == null or checkpoints.len == 0) null else checkpoints[checkpoints.len - 1];

    var handle = CapiHandle.open(allocator, path, std.testing.io, false) catch |err| {
        if (acknowledged.len != 0) return err;
        return;
    };
    defer handle.close();

    var required = std.StringHashMapUnmanaged(?i64).empty;
    defer required.deinit(allocator);
    for (acknowledged) |cp| for (cp.expectations) |exp| {
        try required.put(allocator, exp.key, try parseExpected(exp.after));
    };

    var any_child_committed = false;
    var it = required.iterator();
    while (it.next()) |entry| {
        // A key the in-flight checkpoint also names is covered by the
        // atomic check below instead.
        if (in_flight) |cp| if (checkpointCoversKey(cp, entry.key_ptr.*)) continue;
        const got = try selectChildParentId(allocator, handle.id, entry.key_ptr.*);
        if (got != entry.value_ptr.*) {
            std.debug.print("crash-consistency capi FK: acknowledged child not visible id={s} want={?d} got={?d}\n", .{ entry.key_ptr.*, entry.value_ptr.*, got });
            return error.CrashConsistencyAcknowledgedWriteLost;
        }
        if (got != null) any_child_committed = true;
    }

    if (in_flight) |cp| {
        var resolution: ?bool = null; // true => resolved to `after`
        for (cp.expectations) |exp| {
            const got = try selectChildParentId(allocator, handle.id, exp.key);
            const want_after = try parseExpected(exp.after);
            const want_before = try parseExpected(exp.before);
            const matches_after = got == want_after;
            const matches_before = got == want_before;
            if (!matches_after and !matches_before) {
                std.debug.print("crash-consistency capi FK: torn child id={s} got={?d} allowed_before={?d} allowed_after={?d}\n", .{ exp.key, got, want_before, want_after });
                return error.CrashConsistencyTornWrite;
            }
            if (matches_after and matches_before) continue;
            if (resolution) |r| {
                if (r != matches_after) {
                    std.debug.print("crash-consistency capi FK: batch not atomic id={s}\n", .{exp.key});
                    return error.CrashConsistencyBatchNotAtomic;
                }
            } else resolution = matches_after;
            if (matches_after and got != null) any_child_committed = true;
        }
    }

    if (any_child_committed) {
        const code = try execOutcome(allocator, handle.id, "DELETE FROM parents WHERE id = 0");
        if (code == .ok) {
            std.debug.print("crash-consistency capi FK: parent DELETE succeeded despite a surviving child reference\n", .{});
            return error.CrashConsistencyAcknowledgedWriteLost;
        }
    } else {
        // lite-single-commit-write-path.md §7.6: the inverse of the check
        // above, and the whole point of the self-healing fix. No child
        // row is durably present, so if the crash landed in the window
        // between the follower's ride-along `.attach` commit and the
        // leader's own row commit -- the non-atomicity gap this
        // coordinator cannot fully close -- a dangling Reference record
        // may still be sitting there with no child behind it. RESTRICT's
        // own existence check (`relational_integrity.zig`'s
        // `requireEmpty` + `ChildExistenceCheck`) must recognize it as
        // stale and let the parent go, not block it forever.
        const code = try execOutcome(allocator, handle.id, "DELETE FROM parents WHERE id = 0");
        if (code != .ok) {
            std.debug.print("crash-consistency capi FK: parent DELETE blocked ({s}) with no child row durably present -- a dangling reference was not self-healed\n", .{@tagName(code)});
            return error.CrashConsistencyAcknowledgedWriteLost;
        }
    }
}

/// INSERT a parent, then INSERT two children referencing it, all
/// autocommit -- through `commitCoordinatedFusedMultiTable` (lite-single-
/// commit-write-path.md §7.5): each child INSERT is the leader (foreground)
/// commit; the parent's own `.attach` Reference rides along in the same
/// fsync. Crashed at every sync point, a child row and its Reference must
/// be all-or-nothing.
fn runRelationalForeignKeyChildInsert(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try CapiHandle.open(allocator, path, io, true);
    defer handle.close();

    try sqlOk(handle.id, "{\"statement\":\"CREATE TABLE parents (id BIGINT PRIMARY KEY)\"}");
    try sqlOk(handle.id, "{\"statement\":\"CREATE TABLE children (id BIGINT, parent_id BIGINT REFERENCES parents (id))\"}");
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO parents (_id, id) VALUES ('parent-0', 0)\"}");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "child-0", .before = null, .after = "0" }} });
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO children (_id, id, parent_id) VALUES ('child-0', 0, 0)\"}");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "child-1", .before = null, .after = "0" }} });
    try sqlOk(handle.id, "{\"statement\":\"INSERT INTO children (_id, id, parent_id) VALUES ('child-1', 1, 0)\"}");
}

test "crash sweep: relational foreign-key child insert, child row and attach record are all-or-nothing" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "relational-fk-child-insert",
        .run = runRelationalForeignKeyChildInsert,
        .custom_check = checkViaSqlForeignKeyChild,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: relational foreign-key child insert, child row and attach record are all-or-nothing" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "relational-fk-child-insert",
        .run = runRelationalForeignKeyChildInsert,
        .custom_check = checkViaSqlForeignKeyChild,
    }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: relational unique autocommit two inserts and an update" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "relational-unique-autocommit",
        .run = runRelationalUniqueAutocommit,
        .custom_check = checkViaSql,
    }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}
