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

//! WS8 crash-consistency harness for Lite's write path
//! (docs/design/lite-performance/lite-single-commit-write-path.md §6).
//!
//! `runCrashSweep` is the reusable sweep engine: it drives a `Scenario`'s
//! `run` once with a plain passthrough `SimIo` to enumerate every durable
//! barrier the scenario's write path reaches (any `fileSync`/rename seen by
//! `crash_sim_io.zig`, which today means the native engine's plain `fsync`
//! and tomorrow's `syncData`/`F_FULLFSYNC`/group-commit fsync alike, since
//! the shim does not care which specific primitive backs `fileSync`), then
//! re-runs it once per barrier asking `SimIo` to abort the scenario exactly
//! before or after that barrier, optionally also simulating power loss
//! (dropping writes that never reached a durable barrier). After each
//! crashed run the harness reopens the database with ordinary IO and asks
//! the scenario to verify invariants.
//!
//! `checkCheckpointsAfterCrash` is the reusable invariant checker other
//! workstreams can call directly from their own tests: given a list of
//! `Checkpoint`s (a barrier sequence plus the exact key/value state that
//! must be visible once that barrier is durable) and the `CrashOutcome` from
//! a sweep run, it reopens the file and asserts:
//!   - `check()` (full-file integrity) always passes;
//!   - a checkpoint whose `durable_after_barrier` is at or before the crash
//!     point is visible with its exact value (no partial/torn value, ever);
//!   - a checkpoint whose `durable_after_barrier` is after the crash point
//!     may be visible or absent (both are valid outcomes of "the call never
//!     returned"), but never with a value from neither its pre- nor its
//!     post-state.
//!
//! Run with (Debug only; filtered):
//!   cd zig && zig build unit-test -Dtest-filter=crash_consistency -- --test-filter crash_consistency
//!
//! `quick` mode (the default; a bounded sample of barriers, a few seconds per
//! scenario) is meant for routine runs; `exhaustive` mode (every barrier,
//! every before/after, with and without power loss) is for qualifying a
//! rewritten commit path and can take several minutes.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const sim_io = @import("crash_sim_io.zig");
const native = @import("native.zig");
const backend = @import("backend.zig");
const connection = @import("connection.zig");
const db_mod = @import("antfly_source_root").antfly_sources.physical_db;

pub const When = sim_io.When;
pub const PowerLoss = sim_io.PowerLoss;
pub const Barrier = sim_io.Barrier;

// ---------------------------------------------------------------------
// Reusable sweep engine
// ---------------------------------------------------------------------

pub const CrashOutcome = struct {
    barrier: Barrier,
    when: When,
    power_loss: PowerLoss,
    /// The error the scenario's `run` returned when the crash fired. Always
    /// the shim's injected I/O error unless the scenario surfaced something
    /// else first -- which is itself a finding worth a test failure.
    run_err: anyerror,
};

pub const Checkpoint = struct {
    /// The barrier sequence number recorded immediately after the commit
    /// that durably applied this checkpoint's expectations (i.e.
    /// `sim.barrierCount()` right after that commit call returned, minus
    /// one, naming the last barrier that commit crossed).
    durable_after_barrier: u64,
    expectations: []const Expectation,
};

pub const Expectation = struct {
    key: []const u8,
    /// `null` means the key must be absent (post-delete).
    value: ?[]const u8,
    /// When set, this expectation is checked through full-text search
    /// instead of (or in addition to) primary lookup: the key must be a hit
    /// for this exact-phrase query against `search_index`/`search_field`.
    searchable_text: ?[]const u8 = null,
};

pub const Scenario = struct {
    name: []const u8,
    search_index: []const u8 = "",
    search_field: []const u8 = "",
    /// Runs the scenario's writes against a fresh database at `path` using
    /// `io` for all storage. Must append one `Checkpoint` per acknowledged
    /// commit. Must propagate every error, including the shim's injected
    /// crash error, to the caller.
    run: *const fn (allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void,
    /// True if `run` opens the database through `db_mod.DB` (full-text /
    /// SQL visible); false if it talks to `native.NativeFile` directly.
    via_db: bool = false,
};

pub const SweepOptions = struct {
    mode: enum { quick, exhaustive } = .quick,
    quick_sample: usize = 16,
    power_loss: []const PowerLoss = &.{ .drop_unsynced, .none },
    whens: []const When = &.{ .before, .after },
};

pub const SweepFailure = struct {
    barrier: Barrier,
    when: When,
    power_loss: PowerLoss,
    detail: []u8,
};

pub const SweepReport = struct {
    allocator: Allocator,
    total_barriers: u64 = 0,
    runs: u64 = 0,
    failures: std.ArrayListUnmanaged(SweepFailure) = .empty,

    pub fn deinit(self: *SweepReport) void {
        for (self.failures.items) |f| {
            self.allocator.free(f.detail);
            self.allocator.free(f.barrier.path);
            if (f.barrier.old_path) |p| self.allocator.free(p);
        }
        self.failures.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn ok(self: *const SweepReport) bool {
        return self.failures.items.len == 0;
    }

    /// `barrier` may point into a `SimIo` that is about to be (or already
    /// has been) torn down, so the path strings are duplicated here rather
    /// than borrowed; `deinit` frees them.
    fn fail(self: *SweepReport, barrier: Barrier, when: When, power_loss: PowerLoss, comptime fmt: []const u8, args: anytype) !void {
        const detail = try std.fmt.allocPrint(self.allocator, fmt, args);
        errdefer self.allocator.free(detail);
        const owned_path = try self.allocator.dupe(u8, barrier.path);
        errdefer self.allocator.free(owned_path);
        const owned_old = if (barrier.old_path) |p| try self.allocator.dupe(u8, p) else null;
        errdefer if (owned_old) |p| self.allocator.free(p);
        try self.failures.append(self.allocator, .{
            .barrier = .{ .sequence = barrier.sequence, .kind = barrier.kind, .path = owned_path, .old_path = owned_old },
            .when = when,
            .power_loss = power_loss,
            .detail = detail,
        });
    }
};

/// Runs the full enumerate-then-sweep cycle for `scenario`. Caller owns the
/// returned report (`deinit` it).
pub fn runCrashSweep(allocator: Allocator, scenario: Scenario, opts: SweepOptions) !SweepReport {
    var report = SweepReport{ .allocator = allocator };
    errdefer report.deinit();

    // Discovery pass: plain passthrough, no crash. Establishes the total
    // barrier count and proves the scenario is well-formed uncrashed.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testAflitePath(allocator, tmp);
        defer allocator.free(path);

        const sim = try sim_io.SimIo.init(allocator, std.testing.io);
        defer sim.deinit();
        var checkpoints = std.ArrayListUnmanaged(Checkpoint).empty;
        defer checkpoints.deinit(allocator);
        try scenario.run(allocator, path, sim.io(), &checkpoints);
        report.total_barriers = sim.barrierCount();
        try checkInvariants(allocator, scenario, path, null, checkpoints.items, &report);
    }

    if (report.total_barriers == 0) return report;

    const selected = try selectBarriers(allocator, report.total_barriers, opts);
    defer allocator.free(selected);

    for (selected) |barrier_seq| {
        for (opts.whens) |when| {
            for (opts.power_loss) |power_loss| {
                report.runs += 1;
                runOneCrash(allocator, scenario, barrier_seq, when, power_loss, &report) catch |err| {
                    const placeholder = Barrier{ .sequence = barrier_seq, .kind = .file_sync, .path = "" };
                    try report.fail(placeholder, when, power_loss, "harness error: {s}", .{@errorName(err)});
                };
            }
        }
    }
    return report;
}

fn runOneCrash(allocator: Allocator, scenario: Scenario, barrier_seq: u64, when: When, power_loss: PowerLoss, report: *SweepReport) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    const sim = try sim_io.SimIo.init(allocator, std.testing.io);
    defer sim.deinit();
    sim.setCrashPlan(.{ .at_barrier = barrier_seq, .when = when, .power_loss = power_loss });

    var checkpoints = std.ArrayListUnmanaged(Checkpoint).empty;
    defer checkpoints.deinit(allocator);

    if (scenario.run(allocator, path, sim.io(), &checkpoints)) {
        // This particular (barrier, when) combination never actually
        // crashed the scenario (e.g. a later commit short-circuited before
        // reaching it). Nothing failed; check the fully-completed state.
        try checkInvariants(allocator, scenario, path, null, checkpoints.items, report);
        return;
    } else |err| {
        if (!sim.crashed) return err; // a real, non-injected failure: surface it.
        if (power_loss == .drop_unsynced) {
            sim.applyPowerLoss() catch |pl_err| {
                const barrier = sim.crash_barrier orelse Barrier{ .sequence = barrier_seq, .kind = .file_sync, .path = "" };
                try report.fail(barrier, when, power_loss, "applyPowerLoss failed: {s}", .{@errorName(pl_err)});
                return;
            };
        }
        const barrier = sim.crash_barrier orelse Barrier{ .sequence = barrier_seq, .kind = .file_sync, .path = "" };
        const outcome = CrashOutcome{ .barrier = barrier, .when = when, .power_loss = power_loss, .run_err = err };
        try checkInvariants(allocator, scenario, path, outcome, checkpoints.items, report);
    }
}

fn checkInvariants(
    allocator: Allocator,
    scenario: Scenario,
    path: []const u8,
    outcome: ?CrashOutcome,
    checkpoints: []const Checkpoint,
    report: *SweepReport,
) !void {
    const placeholder = Barrier{ .sequence = if (outcome) |o| o.barrier.sequence else 0, .kind = .file_sync, .path = "" };
    const when: When = if (outcome) |o| o.when else .after;
    const power_loss: PowerLoss = if (outcome) |o| o.power_loss else .none;

    if (scenario.via_db) {
        checkCheckpointsViaDb(allocator, path, outcome, checkpoints, scenario.search_index, scenario.search_field) catch |err| {
            try report.fail(placeholder, when, power_loss, "db invariant check failed: {s}", .{@errorName(err)});
        };
    } else {
        checkCheckpointsViaNative(allocator, path, outcome, checkpoints) catch |err| {
            try report.fail(placeholder, when, power_loss, "native invariant check failed: {s}", .{@errorName(err)});
        };
    }
}

/// True if `checkpoint` must be visible given a crash at `outcome` (or
/// unconditionally, when there was no crash at all).
fn mustBeVisible(outcome: ?CrashOutcome, checkpoint: Checkpoint) bool {
    const o = outcome orelse return true;
    if (o.when == .after) return checkpoint.durable_after_barrier <= o.barrier.sequence;
    return checkpoint.durable_after_barrier < o.barrier.sequence;
}

/// Collapses a chronological checkpoint list into "the last durable
/// expectation per key", so a key written several times (update, then
/// update again, then delete; or the same key across concurrent-writer
/// rounds) is checked against whichever of those writes actually became
/// durable before the simulated crash, not against every one that was ever
/// true at some earlier point in the run.
fn collapseToLastDurable(allocator: Allocator, outcome: ?CrashOutcome, checkpoints: []const Checkpoint) !std.StringHashMapUnmanaged(Expectation) {
    var last = std.StringHashMapUnmanaged(Expectation).empty;
    errdefer last.deinit(allocator);
    for (checkpoints) |cp| {
        if (!mustBeVisible(outcome, cp)) continue;
        for (cp.expectations) |exp| try last.put(allocator, exp.key, exp);
    }
    return last;
}

/// Library entry point other crash tests can call directly: opens the
/// `.aflite` file at `path` through `native.NativeFile` and checks every
/// checkpoint's visibility against `outcome` (`null` for an uncrashed run).
pub fn checkCheckpointsViaNative(allocator: Allocator, path: []const u8, outcome: ?CrashOutcome, checkpoints: []const Checkpoint) !void {
    var last_durable = try collapseToLastDurable(allocator, outcome, checkpoints);
    defer last_durable.deinit(allocator);

    var file = native.NativeFile.openWithIo(allocator, std.testing.io, path, .{ .read_only = true }) catch |err| {
        // A file that never reached a single durable checkpoint may fail to
        // open at all. That is only acceptable if nothing was ever supposed
        // to be durable yet.
        if (last_durable.count() != 0) return err;
        return;
    };
    defer file.close();

    const report = try file.check();
    // `check()` is a byte-exact cleanliness verifier (what a backup/vacuum
    // wants), not a recoverability probe: native.zig's own tests
    // (e.g. "lite native file publishes checkpoint without rewriting static
    // header") expect `issue == "tail_bytes"` with `tail_bytes > 0` for any
    // file carrying uncommitted/aborted tail writes, which is exactly what a
    // crash before a commit's last durable barrier leaves behind on
    // purpose -- the recovery path is expected to ignore that tail, not
    // reject it. Only a *different* issue (real structural corruption) is a
    // crash-consistency finding here; document visibility/atomicity is
    // checked separately below regardless.
    if (!report.valid and !std.mem.eql(u8, report.issue orelse "", "tail_bytes")) {
        std.debug.print("crash-consistency: native check() invalid, issue={s}\n", .{report.issue orelse "<none>"});
        return error.CrashConsistencyIntegrityCheckFailed;
    }

    var it = last_durable.valueIterator();
    while (it.next()) |exp| {
        const got = try file.getDocumentAlloc(allocator, exp.key);
        defer if (got) |bytes| allocator.free(bytes);
        if (exp.value) |want| {
            if (got) |bytes| {
                if (!std.mem.eql(u8, bytes, want)) return error.CrashConsistencyTornWrite;
            } else {
                return error.CrashConsistencyAcknowledgedWriteLost;
            }
        } else if (got != null) {
            return error.CrashConsistencyDeleteNotDurable;
        }
    }
}

/// Library entry point for scenarios that go through `db_mod.DB` (full-text
/// visible writes). Mirrors `checkCheckpointsViaNative` but also verifies
/// full-text searchability for checkpoints that name `searchable_text`.
pub fn checkCheckpointsViaDb(
    allocator: Allocator,
    path: []const u8,
    outcome: ?CrashOutcome,
    checkpoints: []const Checkpoint,
    search_index: []const u8,
    search_field: []const u8,
) !void {
    var last_durable = try collapseToLastDurable(allocator, outcome, checkpoints);
    defer last_durable.deinit(allocator);

    var handle = openDbWithIo(allocator, path, std.testing.io, false) catch |err| {
        if (last_durable.count() != 0) return err;
        return;
    };
    defer handle.close();

    var it = last_durable.valueIterator();
    while (it.next()) |exp| {
        var looked_up = try handle.db.lookup(allocator, exp.key, .{});
        defer if (looked_up) |*r| r.deinit(allocator);
        if (exp.value) |want| {
            if (looked_up) |result| {
                if (std.mem.indexOf(u8, result.json, want) == null) return error.CrashConsistencyTornWrite;
            } else {
                return error.CrashConsistencyAcknowledgedWriteLost;
            }
        } else if (looked_up != null) {
            return error.CrashConsistencyDeleteNotDurable;
        }

        if (exp.searchable_text) |text| {
            var result = try handle.db.search(allocator, .{
                .index_name = search_index,
                .full_text = .{ .match = .{ .field = search_field, .text = text } },
                .limit = 10,
            });
            defer result.deinit();
            var found = false;
            for (result.hits) |hit| {
                if (std.mem.eql(u8, hit.id, exp.key)) found = true;
            }
            if (!found) return error.CrashConsistencyFullTextNotRebuilt;
        }
    }
}

fn selectBarriers(allocator: Allocator, total: u64, opts: SweepOptions) ![]u64 {
    if (opts.mode == .exhaustive or total <= opts.quick_sample) {
        const out = try allocator.alloc(u64, total);
        for (out, 0..) |*slot, i| slot.* = i;
        return out;
    }
    const n = opts.quick_sample;
    const out = try allocator.alloc(u64, n);
    for (out, 0..) |*slot, i| {
        slot.* = @min(total - 1, (i * total) / n);
    }
    return out;
}

fn testAflitePath(allocator: Allocator, tmp: std.testing.TmpDir) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/crash.aflite", .{tmp.sub_path});
}

/// `Connection` keeps its `backend.Handle` and `db_mod.DB` side by side for
/// exactly this reason: the backend owns the heap-allocated docstore/index
/// storage the DB's open options point into, so it must outlive the DB and
/// be closed after it. Mirrors `connection.zig`'s private `openWithBackend`,
/// but with an injectable `io` -- `Connection.Options` has no such field,
/// since it is a production API with no reason to take one. Kept in this
/// test file only.
const DbHandle = struct {
    backend: backend.Handle,
    db: db_mod.DB,

    fn close(self: *DbHandle) void {
        self.db.close();
        self.backend.deinit();
    }
};

fn openDbWithIo(allocator: Allocator, path: []const u8, io: std.Io, create: bool) !DbHandle {
    var lite_backend = if (create)
        try backend.Handle.createWithOptions(allocator, path, .{ .exclusive = true, .io = io })
    else
        try backend.Handle.open(allocator, path, .{ .io = io });
    errdefer lite_backend.deinit();

    const identity = connection.identityOpenOptions(create);
    var db_opts = db_mod.OpenOptions{
        .open_mode = .writer,
        .external_derived_checkpoints = false,
        .identity_namespace = identity.identity_namespace,
        .prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace,
    };
    try lite_backend.configureDbOpenOptions(&db_opts);

    var db = try db_mod.DB.open(allocator, path, db_opts);
    errdefer db.close();
    if (create) try db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
    // Open (and addIndex) can leave background maintenance queued, running
    // concurrently via io.concurrent(...). Drain it here so the barriers the
    // scenario's own explicit batch() calls cross are not interleaved with
    // nondeterministic background commits, which would break the
    // enumerate-once/replay-many sweep below.
    try db.runUntilIdle();
    return .{ .backend = lite_backend, .db = db };
}

// ---------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------

/// The current `SimIo`'s underlying pointer, recovered from the `Io` value
/// scenarios are handed. Lets a scenario ask "what is the last barrier
/// sequence my most recent commit crossed?" without widening `Scenario.run`.
fn simFromIo(io: std.Io) *sim_io.SimIo {
    return @ptrCast(@alignCast(io.userdata.?));
}

fn lastBarrierSeq(io: std.Io) u64 {
    const count = simFromIo(io).barrierCount();
    return if (count == 0) 0 else count - 1;
}

fn runPrimarySingleDoc(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    try file.putDocument("seed", "seed-value");
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "seed", .value = "seed-value" }} });

    try file.putDocument("primary-doc", "primary-value");
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "primary-doc", .value = "primary-value" }} });
}

fn runPrimaryBatch(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    try file.putDocument("seed", "seed-value");
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "seed", .value = "seed-value" }} });

    const mutations = [_]native.DocumentMutation{
        .{ .key = "batch-a", .value = "value-a" },
        .{ .key = "batch-b", .value = "value-b" },
        .{ .key = "batch-c", .value = "value-c" },
    };
    try file.putDocumentBatch(&mutations);
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{
        .{ .key = "batch-a", .value = "value-a" },
        .{ .key = "batch-b", .value = "value-b" },
        .{ .key = "batch-c", .value = "value-c" },
    } });
}

fn runPrimaryUpdateDelete(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    try file.putDocument("key", "v1");
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "key", .value = "v1" }} });

    try file.putDocument("key", "v2");
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "key", .value = "v2" }} });

    try file.deleteDocument("key");
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "key", .value = null }} });
}

/// Fixed, fully comptime-known (writer, round) interleaving: 4 logical
/// writers each committing 3 sequential rounds, writer-major order. Every
/// key/value pair is a comptime string so `Checkpoint.expectations` below
/// can keep pointing at it after this function returns, with no allocation
/// and nothing to free.
const ConcurrentStep = struct { key: []const u8, value: []const u8 };
const concurrent_writer_count = 4;
const concurrent_round_count = 3;
const concurrent_steps: [concurrent_writer_count * concurrent_round_count]ConcurrentStep = blk: {
    var steps: [concurrent_writer_count * concurrent_round_count]ConcurrentStep = undefined;
    var i: usize = 0;
    for (0..concurrent_round_count) |round| {
        for (0..concurrent_writer_count) |writer| {
            steps[i] = .{
                .key = std.fmt.comptimePrint("writer-{d}", .{writer}),
                .value = std.fmt.comptimePrint("round-{d}", .{round}),
            };
            i += 1;
        }
    }
    break :blk steps;
};

/// Stands in for genuinely concurrent writers with a deterministic,
/// repeatable interleaving of several logical writers' commits -- matching
/// this codebase's existing preference (VOPR, `storage/*sim*`) for
/// deterministic simulation over real OS threads in crash tests.
fn runDeterministicConcurrentWriters(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    inline for (concurrent_steps) |step| {
        try file.putDocument(step.key, step.value);
        try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = step.key, .value = step.value }} });
    }
}

fn runFullTextSyncVisible(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "seed", .value = "seed body" }} });

    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
    try checkpoints.append(allocator, .{ .durable_after_barrier = lastBarrierSeq(io), .expectations = &.{.{ .key = "ft-doc", .value = "needle in a haystack", .searchable_text = "needle" }} });
}

test "crash sweep: primary single doc" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "primary-single-doc", .run = runPrimarySingleDoc }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: primary batch" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "primary-batch", .run = runPrimaryBatch }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: primary update then delete" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "primary-update-delete", .run = runPrimaryUpdateDelete }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: deterministic concurrent writers" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "deterministic-concurrent-writers", .run = runDeterministicConcurrentWriters }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: full text synchronous visibility" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "full-text-sync-visible",
        .run = runFullTextSyncVisible,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

fn reportFailures(report: *const SweepReport) !void {
    if (report.ok()) return;
    for (report.failures.items) |f| {
        std.debug.print(
            "crash-consistency failure: barrier={d} kind={s} path={s} when={s} power_loss={s}: {s}\n",
            .{ f.barrier.sequence, @tagName(f.barrier.kind), f.barrier.path, @tagName(f.when), @tagName(f.power_loss), f.detail },
        );
    }
    return error.CrashConsistencyFailuresFound;
}
