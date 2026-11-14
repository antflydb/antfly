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
//! Crash-consistency semantics are modeled by *API acknowledgement*, not by
//! counting syncs (counting would break the moment WS1 collapses a commit's
//! three fsyncs into one, or WS2 adds group commit): a `Scenario.run`
//! appends one `Checkpoint` immediately *before* attempting each mutating
//! API call, recording both the state that must already hold (`before`,
//! whatever the last acknowledged state was) and the state the call is
//! trying to reach (`after`). Because the checkpoint for operation N+1 is
//! only ever appended once operation N's call has returned success, a
//! crashed run's own checkpoint list structurally encodes exactly which
//! operations were acknowledged in that run: every checkpoint but the last
//! belongs to a call that returned; the last is the one that was in flight
//! when the shim's injected fault fired (or, if `scenario.run` completed
//! without error, every checkpoint is acknowledged and there is no
//! in-flight one). The checker then asserts:
//!   - `check()` (full-file integrity, tolerating the benign `tail_bytes`
//!     issue an uncommitted append always leaves) always passes;
//!   - every acknowledged checkpoint's `after` state is visible, exactly;
//!   - the in-flight checkpoint's every key resolves to *either* its
//!     `before` or its `after` value, consistently across every key in that
//!     one checkpoint (atomicity), and never anything else (a torn value).
//!
//! `checkCheckpointsViaNative`/`checkCheckpointsViaDb` are the reusable
//! library pieces other workstreams can call directly from their own tests.
//!
//! Run with (Debug only; filtered):
//!   cd zig && zig build antfly-storage-test -Dtest-filter="crash sweep" -- --test-filter "crash sweep"
//!
//! `quick` mode (the default; a bounded sample of barriers, a few seconds per
//! scenario) is meant for routine runs; `exhaustive` mode (every barrier,
//! every before/after, with and without power loss) is for qualifying a
//! rewritten commit path and can take several minutes.

const std = @import("std");
const Allocator = std.mem.Allocator;

const sim_io = @import("crash_sim_io.zig");
const native = @import("native.zig");
const backend = @import("backend.zig");
const connection = @import("connection.zig");
const docstore = @import("docstore.zig");
const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
const index_manager = @import("../db/catalog/index_manager.zig");
const graph_mod = @import("../../graph/graph.zig");

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

pub const Expectation = struct {
    key: []const u8,
    /// The state that must already have been visible before this
    /// operation was attempted (the last acknowledged state for `key`).
    /// `null` means `key` did not exist yet.
    before: ?[]const u8,
    /// The state this operation is trying to reach. `null` means `key`
    /// becomes absent (a delete).
    after: ?[]const u8,
    /// Checked only on the `after` side (required-acknowledged, or
    /// in-flight resolved to `after`): `key` must be a full-text hit for
    /// this phrase against the scenario's `search_index`/`search_field`.
    searchable_after: ?[]const u8 = null,
    /// Checked only on the `after` side, same as `searchable_after`: the
    /// edge this write declared must be readable in both directions --
    /// `source` (this checkpoint's `key`) -> `target` via `.out`, and
    /// `target` -> `source` via `.in` -- against `index_name`/`edge_type`.
    graph_edge_after: ?GraphEdgeExpectation = null,
};

pub const GraphEdgeExpectation = struct {
    index_name: []const u8,
    edge_type: []const u8,
    target: []const u8,
};

pub const Checkpoint = struct {
    expectations: []const Expectation,
    /// False for a checkpoint whose call committed with durability=false
    /// (e.g. `NativeFile.commitTransactionWithDurability(false)` /
    /// `docstore.Store.submitMutationWithDurability(..., false)`): the call
    /// still returned success -- this checkpoint is structurally
    /// "acknowledged", not in-flight -- but a crash before the *next*
    /// durable barrier may still roll it back. Such a checkpoint is
    /// checked exactly like an in-flight one (every key resolves to either
    /// `before` or `after`, consistently across the checkpoint), never
    /// required to show `after`. Acknowledged, durable checkpoints (the
    /// default) must always show `after`.
    durable: bool = true,
};

pub const Scenario = struct {
    name: []const u8,
    search_index: []const u8 = "",
    search_field: []const u8 = "",
    /// Runs the scenario's writes against a fresh database at `path` using
    /// `io` for all storage. Must append one `Checkpoint` *before*
    /// attempting each mutating call (so the checkpoint for an interrupted
    /// call is still recorded) and must propagate every error, including
    /// the shim's injected crash error, to the caller.
    run: *const fn (allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void,
    /// True if `run` opens the database through `db_mod.DB` (full-text /
    /// SQL visible); false if it talks to `native.NativeFile` directly.
    via_db: bool = false,
    /// True if `run` also exercises a journal retirement turn: after the
    /// normal document/search checks, additionally verify the replay
    /// journal itself is still structurally readable.
    retirement: bool = false,
    /// Overrides `via_db`/`checkCheckpointsViaNative` with caller-supplied
    /// lookup semantics, for a scenario whose rows cannot be read back by
    /// either of this file's two checkers -- e.g. a SQL relational table
    /// opened through the full capi `Handle` (`antfly_db_sql_json`), whose
    /// rows live under an encoded, table-scoped physical key rather than at
    /// the raw document key `checkCheckpointsViaDb`'s `handle.db.lookup`
    /// expects. `path`/`outcome`/`checkpoints` are exactly what
    /// `checkCheckpointsViaNative`/`checkCheckpointsViaDb` receive; `null`
    /// `outcome` means an uncrashed run. See
    /// `storage/lite/crash_consistency_capi_test.zig`.
    custom_check: ?*const fn (allocator: Allocator, path: []const u8, outcome: ?CrashOutcome, checkpoints: []const Checkpoint) anyerror!void = null,
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
    /// The largest number of replays `runCrashSweep` ever held open (each
    /// with its own store) at once. `1` for the sequential path (today's
    /// behavior, and whenever `ANTFLY_CRASH_SWEEP_JOBS=1`); bounded by
    /// `resolveSweepJobs()` otherwise. A rough proxy for this sweep's peak
    /// memory multiplier over one replay's own footprint, not a byte count.
    peak_concurrent_replays: u64 = 1,

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
        const failure = try makeFailure(self.allocator, barrier, when, power_loss, fmt, args);
        try self.failures.append(self.allocator, failure);
    }
};

/// Builds one owned `SweepFailure` record without appending it anywhere.
/// Pulled out of `SweepReport.fail` so a concurrent replay (which has no
/// shared `SweepReport` to append into while other replays are still in
/// flight) can build its own result the same way; the caller is always
/// the only one touching `allocator` for this one record, so no locking is
/// needed here even when several of these run at once against the same
/// (thread-safe) `std.testing.allocator`. See `fail`'s own doc comment for
/// why the strings are duplicated rather than borrowed.
fn makeFailure(allocator: Allocator, barrier: Barrier, when: When, power_loss: PowerLoss, comptime fmt: []const u8, args: anytype) !SweepFailure {
    const detail = try std.fmt.allocPrint(allocator, fmt, args);
    errdefer allocator.free(detail);
    const owned_path = try allocator.dupe(u8, barrier.path);
    errdefer allocator.free(owned_path);
    const owned_old = if (barrier.old_path) |p| try allocator.dupe(u8, p) else null;
    errdefer if (owned_old) |p| allocator.free(p);
    return .{
        .barrier = .{ .sequence = barrier.sequence, .kind = barrier.kind, .path = owned_path, .old_path = owned_old },
        .when = when,
        .power_loss = power_loss,
        .detail = detail,
    };
}

/// One selected (barrier, when, power_loss) replay, in the exact order
/// `runCrashSweep`'s nested loops would visit it sequentially. Built once,
/// up front, so both the sequential and concurrent executors below walk
/// identical, deterministic work lists -- and so failures can always be
/// reported back in this same order regardless of which executor ran them
/// or in what order they actually finished.
const Combo = struct { barrier_seq: u64, when: When, power_loss: PowerLoss };

fn buildCombos(allocator: Allocator, selected: []const u64, opts: SweepOptions) ![]Combo {
    var combos = std.ArrayListUnmanaged(Combo).empty;
    errdefer combos.deinit(allocator);
    for (selected) |barrier_seq| {
        for (opts.whens) |when| {
            for (opts.power_loss) |power_loss| {
                try combos.append(allocator, .{ .barrier_seq = barrier_seq, .when = when, .power_loss = power_loss });
            }
        }
    }
    return combos.toOwnedSlice(allocator);
}

/// Runs the full enumerate-then-sweep cycle for `scenario`. Caller owns the
/// returned report (`deinit` it).
///
/// The discovery pass always runs sequentially, once, against plain
/// `std.testing.io` -- it alone establishes `total_barriers` and must
/// finish before any replay's barrier selection can even be computed, so
/// there is nothing to parallelize there. Every selected (barrier, when,
/// power_loss) replay after that is independent (see `runOneCrash`'s own
/// doc comment) and, when `resolveSweepJobs()` allows more than one, runs
/// concurrently via `runCombosConcurrently`; `ANTFLY_CRASH_SWEEP_JOBS=1`
/// (or a single-core host) keeps today's exact sequential loop.
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
        sim.setRunLabel(scenario.name);
        var checkpoints = std.ArrayListUnmanaged(Checkpoint).empty;
        defer checkpoints.deinit(allocator);
        try scenario.run(allocator, path, sim.io(), &checkpoints);
        report.total_barriers = sim.barrierCount();
        if (try checkInvariants(allocator, scenario, path, null, checkpoints.items)) |f| try report.failures.append(allocator, f);
    }

    if (report.total_barriers == 0) return report;

    const selected = try selectBarriers(allocator, report.total_barriers, opts);
    defer allocator.free(selected);

    const combos = try buildCombos(allocator, selected, opts);
    defer allocator.free(combos);

    const jobs = resolveSweepJobs();
    if (jobs <= 1 or combos.len <= 1) {
        for (combos) |combo| {
            report.runs += 1;
            const maybe_failure = runOneCrash(allocator, scenario, std.testing.io, combo.barrier_seq, combo.when, combo.power_loss) catch |err| blk: {
                const placeholder = Barrier{ .sequence = combo.barrier_seq, .kind = .file_sync, .path = "" };
                break :blk try makeFailure(allocator, placeholder, combo.when, combo.power_loss, "scenario={s} harness error: {s}", .{ scenario.name, @errorName(err) });
            };
            if (maybe_failure) |f| try report.failures.append(allocator, f);
        }
        report.peak_concurrent_replays = 1;
        return report;
    }

    try runCombosConcurrently(allocator, scenario, combos, jobs, &report);
    return report;
}

/// Reads `ANTFLY_CRASH_SWEEP_JOBS` (any positive integer; `1` reproduces
/// today's sequential sweep exactly) or falls back to `min(4, cpu count)`.
/// `std.testing.environ` is this test's own environment, populated by the
/// test runner before any test function runs.
fn resolveSweepJobs() usize {
    if (std.process.Environ.getPosix(std.testing.environ, "ANTFLY_CRASH_SWEEP_JOBS")) |raw| {
        if (std.fmt.parseInt(usize, raw, 10)) |n| {
            if (n >= 1) return n;
        } else |_| {}
    }
    const cpu_count = std.Thread.getCpuCount() catch 1;
    return @max(@as(usize, 1), @min(@as(usize, 4), cpu_count));
}

/// Runs every one of `combos` independently and, once all have finished,
/// appends any failures to `report` in `combos`' own order -- never
/// completion order, so a sweep's failure list reads identically to the
/// sequential path regardless of scheduling. Bounds how many replays are
/// ever concurrently open (each holding its own store) to `jobs` via a
/// semaphore; `Io.Group.async` may dispatch more tasks than that onto
/// `std.testing.io`'s own thread pool, but each one blocks on the
/// semaphore before doing any real work, so actual concurrency never
/// exceeds `jobs`.
fn runCombosConcurrently(allocator: Allocator, scenario: Scenario, combos: []const Combo, jobs: usize, report: *SweepReport) !void {
    const driver_io = std.testing.io;
    const effective_jobs = @max(@as(usize, 1), @min(jobs, combos.len));

    const results = try allocator.alloc(ComboResult, combos.len);
    defer allocator.free(results);
    for (results) |*r| r.* = .{};

    const tasks = try allocator.alloc(ReplayTask, combos.len);
    defer allocator.free(tasks);

    var sem = std.Io.Semaphore{ .permits = effective_jobs };
    var inflight: std.atomic.Value(usize) = .init(0);
    var peak: std.atomic.Value(usize) = .init(0);

    for (combos, tasks, results) |combo, *task, *result| {
        task.* = .{
            .allocator = allocator,
            .scenario = scenario,
            .combo = combo,
            .result = result,
            .sem = &sem,
            .driver_io = driver_io,
            .inflight = &inflight,
            .peak = &peak,
        };
    }

    var group: std.Io.Group = .init;
    defer group.cancel(driver_io);
    for (tasks) |*task| group.async(driver_io, ReplayTask.run, .{task});
    try group.await(driver_io);

    report.peak_concurrent_replays = peak.load(.monotonic);
    report.runs += combos.len;
    for (combos, results) |combo, result| {
        if (result.harness_err) |err| {
            const placeholder = Barrier{ .sequence = combo.barrier_seq, .kind = .file_sync, .path = "" };
            const failure = try makeFailure(allocator, placeholder, combo.when, combo.power_loss, "scenario={s} harness error: {s}", .{ scenario.name, @errorName(err) });
            try report.failures.append(allocator, failure);
        } else if (result.failure) |f| {
            try report.failures.append(allocator, f);
        }
    }
    if (report.peak_concurrent_replays > 1) {
        std.debug.print("crash sweep: scenario={s} runs={d} jobs={d} peak_concurrent_replays={d}\n", .{ scenario.name, combos.len, effective_jobs, report.peak_concurrent_replays });
    }
}

const ComboResult = struct {
    failure: ?SweepFailure = null,
    harness_err: ?anyerror = null,
};

/// One concurrently-scheduled replay. Owns nothing shared with any other
/// `ReplayTask`: `allocator` is `std.testing.allocator` (thread-safe, see
/// its own doc comment), `scenario` is read-only, and `result` is this
/// task's own exclusive slot in `runCombosConcurrently`'s `results` array
/// (one array entry per task, no two tasks ever touch the same one).
const ReplayTask = struct {
    allocator: Allocator,
    scenario: Scenario,
    combo: Combo,
    result: *ComboResult,
    sem: *std.Io.Semaphore,
    driver_io: std.Io,
    inflight: *std.atomic.Value(usize),
    peak: *std.atomic.Value(usize),

    /// Must be safe to run concurrently with every other `ReplayTask.run`:
    /// see `runOneCrash`'s own doc comment for why giving each one its own
    /// `std.Io.Threaded` -- not `std.testing.io` -- is what actually makes
    /// that true (`crash_sim_io.zig`'s `SimIo` registry allows only one
    /// live instance per real `Io`'s `userdata`, and every `SimIo` wrapping
    /// `std.testing.io` shares the same one).
    fn run(self: *ReplayTask) void {
        self.sem.waitUncancelable(self.driver_io);
        defer self.sem.post(self.driver_io);

        const now = self.inflight.fetchAdd(1, .monotonic) + 1;
        defer _ = self.inflight.fetchSub(1, .monotonic);
        updatePeak(self.peak, now);

        var threaded: std.Io.Threaded = .init(self.allocator, .{});
        defer threaded.deinit();

        self.result.failure = runOneCrash(self.allocator, self.scenario, threaded.io(), self.combo.barrier_seq, self.combo.when, self.combo.power_loss) catch |err| {
            self.result.harness_err = err;
            return;
        };
    }
};

fn updatePeak(peak: *std.atomic.Value(usize), candidate: usize) void {
    var current = peak.load(.monotonic);
    while (candidate > current) {
        current = peak.cmpxchgWeak(current, candidate, .monotonic, .monotonic) orelse return;
    }
}

/// Runs one (barrier, when, power_loss) replay to completion and returns
/// the single failure it found, if any. Fully self-contained: its own
/// `std.testing.tmpDir` (a fresh directory and `.aflite` path), its own
/// `SimIo` wrapping `real_io`, and -- through `scenario.run` -- its own
/// `NativeFile`/`DB`/store opened against that path. Nothing here reaches
/// outside those, so many calls can run at once as long as each gets a
/// `real_io` with its own, distinct identity: `real_io` is only ever
/// `std.testing.io` itself (the sequential path, one live `SimIo` at a
/// time, exactly as before) or one call's own private `std.Io.Threaded`
/// (the concurrent path's `ReplayTask.run`) -- never the same `real_io`
/// shared between two calls in flight together, which `SimIo.init` would
/// reject outright (`StaleSimIoRegistration`) since its registry keys on
/// `real_io`'s own `userdata`.
fn runOneCrash(allocator: Allocator, scenario: Scenario, real_io: std.Io, barrier_seq: u64, when: When, power_loss: PowerLoss) !?SweepFailure {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    const sim = try sim_io.SimIo.init(allocator, real_io);
    const label = try std.fmt.allocPrint(allocator, "{s} barrier={d} when={s} power_loss={s}", .{ scenario.name, barrier_seq, @tagName(when), @tagName(power_loss) });
    // `label` must outlive every use of `sim.run_label`, including the one
    // inside `sim.deinit()` itself -- free it only after `deinit` runs.
    defer {
        sim.deinit();
        allocator.free(label);
    }
    sim.setRunLabel(label);
    sim.setCrashPlan(.{ .at_barrier = barrier_seq, .when = when, .power_loss = power_loss });

    var checkpoints = std.ArrayListUnmanaged(Checkpoint).empty;
    defer checkpoints.deinit(allocator);

    if (scenario.run(allocator, path, sim.io(), &checkpoints)) {
        // This particular (barrier, when) combination never actually
        // crashed the scenario (e.g. it has fewer barriers under this
        // path than the full discovery run). Nothing in flight; check the
        // fully-completed state.
        return try checkInvariants(allocator, scenario, path, null, checkpoints.items);
    } else |err| {
        if (!sim.crashed) return err; // a real, non-injected failure: surface it.
        if (power_loss == .drop_unsynced) {
            sim.applyPowerLoss() catch |pl_err| {
                const barrier = sim.crash_barrier orelse Barrier{ .sequence = barrier_seq, .kind = .file_sync, .path = "" };
                return try makeFailure(allocator, barrier, when, power_loss, "scenario={s} barrier_thread={d} applyPowerLoss failed: {s}", .{ scenario.name, barrier.thread_id, @errorName(pl_err) });
            };
        }
        const barrier = sim.crash_barrier orelse Barrier{ .sequence = barrier_seq, .kind = .file_sync, .path = "" };
        const outcome = CrashOutcome{ .barrier = barrier, .when = when, .power_loss = power_loss, .run_err = err };
        return try checkInvariants(allocator, scenario, path, outcome, checkpoints.items);
    }
}

fn checkInvariants(
    allocator: Allocator,
    scenario: Scenario,
    path: []const u8,
    outcome: ?CrashOutcome,
    checkpoints: []const Checkpoint,
) !?SweepFailure {
    // `std.math.maxInt(u64)` and `.rename`/empty-path flag "no crash fired
    // in this run" in a failure report -- distinct from a real barrier 0 --
    // so a failure found with no crash at all (the uncrashed discovery
    // pass, or a (barrier, when) pair that never actually interrupted this
    // scenario) is never confused with one found exactly at barrier 0.
    const placeholder = if (outcome) |o| o.barrier else Barrier{ .sequence = std.math.maxInt(u64), .kind = .file_sync, .path = "" };
    const when: When = if (outcome) |o| o.when else .after;
    const power_loss: PowerLoss = if (outcome) |o| o.power_loss else .none;
    const crash_note = if (outcome == null) " [uncrashed run]" else "";

    if (scenario.custom_check) |check_fn| {
        check_fn(allocator, path, outcome, checkpoints) catch |err| {
            return try makeFailure(allocator, placeholder, when, power_loss, "custom invariant check failed: {s}{s}", .{ @errorName(err), crash_note });
        };
    } else if (scenario.via_db) {
        checkCheckpointsViaDb(allocator, path, outcome, checkpoints, scenario.search_index, scenario.search_field, scenario.retirement) catch |err| {
            return try makeFailure(allocator, placeholder, when, power_loss, "scenario={s} barrier_thread={d} db invariant check failed: {s}{s}", .{ scenario.name, placeholder.thread_id, @errorName(err), crash_note });
        };
    } else {
        checkCheckpointsViaNative(allocator, path, outcome, checkpoints) catch |err| {
            return try makeFailure(allocator, placeholder, when, power_loss, "scenario={s} barrier_thread={d} native invariant check failed: {s}{s}", .{ scenario.name, placeholder.thread_id, @errorName(err), crash_note });
        };
    }
    return null;
}

/// Splits a run's own checkpoint list into the prefix that is fully
/// acknowledged (every call up to and including the one that produced it
/// returned success in this run) and, for a crashed run only, the single
/// trailing checkpoint whose call was in flight when the fault fired. This
/// needs no barrier/sync counting: the structure of the list itself proves
/// it, because `Scenario.run` appends a checkpoint before attempting its
/// call, so reaching checkpoint N+1 is only possible once call N returned.
///
/// `acknowledged` is further split by `Checkpoint.durable`:
/// `durable_acknowledged` (the default) must show `after` exactly, same as
/// today; `non_durable_acknowledged` committed with durability=false, so a
/// crash before the next durable barrier may still roll it back -- each
/// such checkpoint is checked like the in-flight one (resolves to either
/// `before` or `after`, atomically across its own keys), never required to
/// show `after`.
const Split = struct {
    durable_acknowledged: []const Checkpoint,
    non_durable_acknowledged: []const Checkpoint,
    in_flight: ?Checkpoint,
};

fn splitCheckpoints(allocator: Allocator, outcome: ?CrashOutcome, checkpoints: []const Checkpoint) !Split {
    const acknowledged = if (outcome == null or checkpoints.len == 0)
        checkpoints
    else
        checkpoints[0 .. checkpoints.len - 1];
    const in_flight = if (outcome == null or checkpoints.len == 0) null else checkpoints[checkpoints.len - 1];

    var durable_list = std.ArrayListUnmanaged(Checkpoint).empty;
    errdefer durable_list.deinit(allocator);
    var non_durable_list = std.ArrayListUnmanaged(Checkpoint).empty;
    errdefer non_durable_list.deinit(allocator);
    for (acknowledged) |cp| {
        if (cp.durable) try durable_list.append(allocator, cp) else try non_durable_list.append(allocator, cp);
    }
    const durable_acknowledged = try durable_list.toOwnedSlice(allocator);
    errdefer allocator.free(durable_acknowledged);
    return .{
        .durable_acknowledged = durable_acknowledged,
        .non_durable_acknowledged = try non_durable_list.toOwnedSlice(allocator),
        .in_flight = in_flight,
    };
}

fn deinitSplit(allocator: Allocator, split: *Split) void {
    allocator.free(split.durable_acknowledged);
    allocator.free(split.non_durable_acknowledged);
}

fn valuesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

fn formatValue(v: ?[]const u8) []const u8 {
    return v orelse "<absent>";
}

/// Last `after` per key across `acknowledged`, in chronological order (a
/// key written more than once keeps only its most recent acknowledged
/// state).
fn finalAcknowledgedState(allocator: Allocator, acknowledged: []const Checkpoint) !std.StringHashMapUnmanaged(Expectation) {
    var state = std.StringHashMapUnmanaged(Expectation).empty;
    errdefer state.deinit(allocator);
    for (acknowledged) |cp| {
        for (cp.expectations) |exp| try state.put(allocator, exp.key, exp);
    }
    return state;
}

/// True if `key` is named by the in-flight checkpoint (if any) or by any
/// non-durable-acknowledged checkpoint in `split` -- i.e. `key` is subject
/// to a checkpoint whose allowed values are `{before, after}` rather than
/// a single required value, whether because its call was interrupted or
/// because it committed with durability=false.
fn atomicResolutionCovers(split: *const Split, key: []const u8) bool {
    if (checkpointCovers(split.in_flight, key)) return true;
    for (split.non_durable_acknowledged) |cp| {
        if (checkpointCovers(cp, key)) return true;
    }
    return false;
}

fn checkpointCovers(cp: ?Checkpoint, key: []const u8) bool {
    const checkpoint = cp orelse return false;
    for (checkpoint.expectations) |exp| {
        if (std.mem.eql(u8, exp.key, key)) return true;
    }
    return false;
}

/// Checks the in-flight checkpoint (if any): every key must resolve to
/// exactly its `before` or exactly its `after` value -- never anything
/// else -- and every key in the same checkpoint must resolve the *same*
/// way (atomicity of the batch that was in flight). `lookup` reads the
/// actual current value for one key; errors from it propagate. Frees each
/// looked-up value via `freeFn` once it has been compared.
fn checkInFlightResolution(
    in_flight: Checkpoint,
    allocator: Allocator,
    comptime LookupCtx: type,
    ctx: LookupCtx,
    comptime lookupFn: fn (LookupCtx, []const u8) anyerror!?[]const u8,
) !void {
    var resolution: ?bool = null; // true => resolved to `after`
    for (in_flight.expectations) |exp| {
        const got = try lookupFn(ctx, exp.key);
        defer if (got) |bytes| allocator.free(@constCast(bytes));
        const matches_after = valuesEqual(got, exp.after);
        const matches_before = valuesEqual(got, exp.before);
        if (!matches_after and !matches_before) {
            std.debug.print(
                "crash-consistency: torn value key={s} actual={s} allowed_before={s} allowed_after={s}\n",
                .{ exp.key, formatValue(got), formatValue(exp.before), formatValue(exp.after) },
            );
            return error.CrashConsistencyTornWrite;
        }
        if (matches_after and matches_before) continue; // uninformative (before == after)
        if (resolution) |r| {
            if (r != matches_after) {
                std.debug.print(
                    "crash-consistency: batch not atomic key={s} resolved_after={} (another key in the same in-flight checkpoint resolved {})\n",
                    .{ exp.key, matches_after, r },
                );
                return error.CrashConsistencyBatchNotAtomic;
            }
        } else resolution = matches_after;
    }
}

/// Library entry point other crash tests can call directly: opens the
/// `.aflite` file at `path` through `native.NativeFile` and checks every
/// checkpoint's visibility against `outcome` (`null` for an uncrashed run).
pub fn checkCheckpointsViaNative(allocator: Allocator, path: []const u8, outcome: ?CrashOutcome, checkpoints: []const Checkpoint) !void {
    var split = try splitCheckpoints(allocator, outcome, checkpoints);
    defer deinitSplit(allocator, &split);
    var required = try finalAcknowledgedState(allocator, split.durable_acknowledged);
    defer required.deinit(allocator);

    var file = native.NativeFile.openWithIo(allocator, std.testing.io, path, .{ .read_only = true }) catch |err| {
        // Nothing was ever acknowledged yet, so any state -- including an
        // unopenable file -- is a valid outcome of "the call never
        // returned".
        if (required.count() != 0) return err;
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
    // purpose. Only a *different* issue (real structural corruption) is a
    // crash-consistency finding here.
    if (!report.valid and !std.mem.eql(u8, report.issue orelse "", "tail_bytes")) {
        std.debug.print("crash-consistency: native check() invalid, issue={s}\n", .{report.issue orelse "<none>"});
        return error.CrashConsistencyIntegrityCheckFailed;
    }

    const Lookup = struct {
        file: *native.NativeFile,
        allocator: Allocator,
        fn get(self: @This(), key: []const u8) anyerror!?[]const u8 {
            return try self.file.getDocumentAlloc(self.allocator, key);
        }
    };
    const lookup_ctx = Lookup{ .file = &file, .allocator = allocator };

    var it = required.iterator();
    while (it.next()) |entry| {
        const exp = entry.value_ptr.*;
        // A key the in-flight checkpoint, or a non-durable-acknowledged
        // one, also names is *not* independently required to equal this
        // (older) acknowledged value: that checkpoint's own before/after
        // range already covers it (`before` here is exactly that
        // acknowledged value, so nothing is lost by deferring to that
        // check instead).
        if (atomicResolutionCovers(&split, exp.key)) continue;
        const got = try lookup_ctx.get(exp.key);
        defer if (got) |bytes| allocator.free(@constCast(bytes));
        if (!valuesEqual(got, exp.after)) {
            std.debug.print(
                "crash-consistency: acknowledged write not visible key={s} want={s} got={s}\n",
                .{ exp.key, formatValue(exp.after), formatValue(got) },
            );
            return error.CrashConsistencyAcknowledgedWriteLost;
        }
    }

    if (split.in_flight) |cp| try checkInFlightResolution(cp, allocator, Lookup, lookup_ctx, Lookup.get);
    for (split.non_durable_acknowledged) |cp| try checkInFlightResolution(cp, allocator, Lookup, lookup_ctx, Lookup.get);
}

/// Diagnostic only (never fails the test itself): reads `key` directly off
/// the physical file through a brand-new read-only `NativeFile`, bypassing
/// `db_mod.DB.lookup()`'s own replay/catch-up materialization entirely.
/// Logged whenever `checkCheckpointsViaDb` finds an acknowledged write not
/// visible through the DB layer, to separate "the raw store really lost
/// it" (this also shows the stale value) from "db.lookup()'s own replay
/// path is what's wrong" (this shows the correct, acknowledged value).
fn rawNativeReadDiagnostic(allocator: Allocator, handle: *DbHandle, key: []const u8, want: ?[]const u8) !void {
    const store = handle.backend.native_docstore orelse return;
    const internal_keys = @import("../internal_keys.zig");
    const encoded = try internal_keys.documentKeyAlloc(allocator, key);
    defer allocator.free(encoded);
    var txn = try store.beginRead();
    defer txn.abort();
    const raw = txn.get(encoded) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const got = raw orelse "<missing>";
    const matches_db_layer_view = if (want) |w| std.mem.indexOf(u8, got, w) != null else raw == null;
    std.debug.print(
        "crash-consistency: raw native read key={s} active_checkpoint_commit_sequence={d} got={s} matches_acknowledged_value={}\n",
        .{ key, store.file.activeCheckpoint().commit_sequence, got, matches_db_layer_view },
    );
}

/// Library entry point for scenarios that go through `db_mod.DB` (full-text
/// visible writes). Mirrors `checkCheckpointsViaNative` but also verifies
/// full-text searchability for checkpoints that name `searchable_after`,
/// and optionally (for `retirement` scenarios) that the replay journal is
/// still structurally readable afterward.
pub fn checkCheckpointsViaDb(
    allocator: Allocator,
    path: []const u8,
    outcome: ?CrashOutcome,
    checkpoints: []const Checkpoint,
    search_index: []const u8,
    search_field: []const u8,
    retirement: bool,
) !void {
    var split = try splitCheckpoints(allocator, outcome, checkpoints);
    defer deinitSplit(allocator, &split);
    var required = try finalAcknowledgedState(allocator, split.durable_acknowledged);
    defer required.deinit(allocator);

    if (native.recovery_debug_log) std.debug.print("[native-recovery] >>> checkCheckpointsViaDb verification open path={s} crashed={}\n", .{ path, outcome != null });
    var handle = openDbWithIo(allocator, path, std.testing.io, false) catch |err| {
        if (required.count() != 0) return err;
        return;
    };
    defer handle.close();

    const Lookup = struct {
        handle: *DbHandle,
        allocator: Allocator,
        fn get(self: @This(), key: []const u8) anyerror!?[]const u8 {
            var looked_up = try self.handle.db.lookup(self.allocator, key, .{});
            defer if (looked_up) |*r| r.deinit(self.allocator);
            return if (looked_up) |result| try self.allocator.dupe(u8, result.json) else null;
        }
    };
    const lookup_ctx = Lookup{ .handle = &handle, .allocator = allocator };

    var it = required.iterator();
    while (it.next()) |entry| {
        const exp = entry.value_ptr.*;
        // See the comment at the equivalent point in
        // `checkCheckpointsViaNative`: a key the in-flight checkpoint, or a
        // non-durable-acknowledged one, also names is covered by that
        // check instead.
        if (atomicResolutionCovers(&split, exp.key)) continue;
        const got = try lookup_ctx.get(exp.key);
        defer if (got) |bytes| allocator.free(@constCast(bytes));
        const matches = if (exp.after) |want|
            got != null and std.mem.indexOf(u8, got.?, want) != null
        else
            got == null;
        if (!matches) {
            std.debug.print(
                "crash-consistency: acknowledged write not visible key={s} want_contains={s} got={s}\n",
                .{ exp.key, formatValue(exp.after), formatValue(got) },
            );
            // Hypothesis (c) vs genuine durability loss: read the SAME key
            // directly off the physical file (bypassing db_mod.DB's lookup
            // path -- any replay/catch-up materialization it does -- via a
            // brand-new read-only NativeFile against this same, already-
            // reopened path). If this agrees with `got` (the reverted
            // value), the data really is gone from the raw store. If it
            // instead shows the acknowledged value, db.lookup()'s own
            // replay/catch-up materialization is what's wrong, not the
            // commit's durability.
            rawNativeReadDiagnostic(allocator, &handle, exp.key, exp.after) catch |err| {
                std.debug.print("crash-consistency: raw native read diagnostic failed err={s}\n", .{@errorName(err)});
            };
            return error.CrashConsistencyAcknowledgedWriteLost;
        }
        if (exp.searchable_after) |text| try assertSearchable(&handle, allocator, search_index, search_field, exp.key, text);
        if (exp.graph_edge_after) |ge| try assertGraphEdgeConsistent(&handle, allocator, exp.key, ge);
    }

    if (split.in_flight) |cp| try checkAtomicCheckpointDb(cp, &handle, allocator, search_index, search_field, &lookup_ctx);
    for (split.non_durable_acknowledged) |cp| try checkAtomicCheckpointDb(cp, &handle, allocator, search_index, search_field, &lookup_ctx);

    if (retirement) {
        const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
        // `ReplayIndexUnavailable` is docstore.zig's documented signal that
        // `replay_meta_init_key` was never written (lite/docstore.zig around
        // "Both runtime adapters share traversal and ownership rules. Only
        // NotFound means exhaustion"), i.e. the journal has never been
        // durably initialized -- an expected state for a crash early enough
        // that no write ever got acknowledged, not corruption.
        const entries = store.iterateReplayFrom(allocator, 0) catch |err| switch (err) {
            error.ReplayIndexUnavailable => return,
            else => return err,
        };
        defer allocator.free(entries);
        for (entries) |*entry| entry.deinit(allocator);
    }

    // Ride-along WS5 invariant for every via_db scenario, not only
    // `retirement`-flagged ones: journal credits must never leak. The
    // durable ledger must always equal a fresh scan over every replay
    // lane, crashed or not -- a non-durable commit rolling back at a
    // crash must release exactly the credit it charged, never more or
    // less, in the same recovery that drops its document-visible effect.
    // `ReplayIndexUnavailable` (see the comment above) is tolerated here
    // too: a crash early enough that the journal was never durably
    // initialized has no ledger to check yet, not a leak.
    if (handle.backend.native_docstore) |store| {
        docstore.expectLedgerMatchesScanForTest(store) catch |err| switch (err) {
            error.ReplayIndexUnavailable => return,
            else => return err,
        };
    }
}

/// Checks one atomically-resolved checkpoint (in-flight or non-durable
/// acknowledged) against a `db_mod.DB` handle: every key resolves to
/// either `before` or `after`, consistently across the checkpoint (via
/// `checkInFlightResolutionDb`), and if a key resolved to `after` and
/// names `searchable_after`, the full-text index must have come up with
/// it atomically -- the entire point of a synchronous full-text sync
/// level, whether or not the commit that produced it was durable.
fn checkAtomicCheckpointDb(
    cp: Checkpoint,
    handle: *DbHandle,
    allocator: Allocator,
    search_index: []const u8,
    search_field: []const u8,
    lookup_ctx: anytype,
) !void {
    try checkInFlightResolutionDb(cp, handle, allocator);
    for (cp.expectations) |exp| {
        if (exp.searchable_after == null and exp.graph_edge_after == null) continue;
        const got = try lookup_ctx.get(exp.key);
        defer if (got) |bytes| allocator.free(@constCast(bytes));
        const resolved_after = exp.after != null and got != null and std.mem.indexOf(u8, got.?, exp.after.?) != null;
        if (!resolved_after) continue;
        if (exp.searchable_after) |text| try assertSearchable(handle, allocator, search_index, search_field, exp.key, text);
        if (exp.graph_edge_after) |ge| try assertGraphEdgeConsistent(handle, allocator, exp.key, ge);
    }
}

fn checkInFlightResolutionDb(in_flight: Checkpoint, handle: *DbHandle, allocator: Allocator) !void {
    const Lookup = struct {
        handle: *DbHandle,
        allocator: Allocator,
        fn get(self: @This(), key: []const u8) anyerror!?[]const u8 {
            var looked_up = try self.handle.db.lookup(self.allocator, key, .{});
            defer if (looked_up) |*r| r.deinit(self.allocator);
            return if (looked_up) |result| try self.allocator.dupe(u8, result.json) else null;
        }
    };
    const lookup_ctx = Lookup{ .handle = handle, .allocator = allocator };
    // db.zig's JSON round-trips (whitespace, field order) so this checker
    // uses substring containment (`indexOf`) rather than exact equality, to
    // match a document's `after`/`before` snippet against the stored JSON.
    var resolution: ?bool = null;
    for (in_flight.expectations) |exp| {
        const got = try lookup_ctx.get(exp.key);
        defer if (got) |bytes| allocator.free(@constCast(bytes));
        const matches_after = if (exp.after) |want| got != null and std.mem.indexOf(u8, got.?, want) != null else got == null;
        const matches_before = if (exp.before) |want| got != null and std.mem.indexOf(u8, got.?, want) != null else got == null;
        if (!matches_after and !matches_before) {
            std.debug.print(
                "crash-consistency: torn value key={s} actual={s} allowed_before_contains={s} allowed_after_contains={s}\n",
                .{ exp.key, formatValue(got), formatValue(exp.before), formatValue(exp.after) },
            );
            return error.CrashConsistencyTornWrite;
        }
        if (matches_after and matches_before) continue;
        if (resolution) |r| {
            if (r != matches_after) {
                std.debug.print(
                    "crash-consistency: batch not atomic key={s} resolved_after={} (another key in the same in-flight checkpoint resolved {})\n",
                    .{ exp.key, matches_after, r },
                );
                return error.CrashConsistencyBatchNotAtomic;
            }
        } else resolution = matches_after;
    }
}

/// "For full-text-visible sync levels, the doc is searchable after recovery
/// (OR recovery deterministically rebuilds it before serving)" -- db.zig's
/// own tests document `error.IndexRebuilding` as the expected signal for
/// the second half of that clause (search is refused, not wrong, while a
/// background catch-up runs). So this retries through that specific error,
/// draining background work between attempts, and only treats it as a
/// finding if the index never finishes catching up within a generous bound.
fn assertSearchable(handle: *DbHandle, allocator: Allocator, search_index: []const u8, search_field: []const u8, key: []const u8, text: []const u8) !void {
    const platform_time = @import("antfly_platform").time;
    // A fixed retry count conflates "stuck" with "a real host under enough
    // concurrent load that each recovery pass's own I/O keeps hitting fresh
    // transient errors" -- recovery can legitimately still be making
    // progress (a different injected-free error each attempt, not the same
    // one repeating) well past any small fixed count on a busy shared
    // machine. Bound by wall-clock time instead so a crash sweep run under
    // sustained load does not fail just because it needed more than N
    // iterations; `attempt` is kept only for diagnostics. A real stuck
    // recovery (the same unresolved condition forever) still eventually
    // exhausts this deadline and fails exactly as before.
    const deadline_ns = platform_time.monotonicNs() +| (30 * std.time.ns_per_s);
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        var result = handle.db.search(allocator, .{
            .index_name = search_index,
            .full_text = .{ .match = .{ .field = search_field, .text = text } },
            .limit = 10,
        }) catch |err| {
            if (err == error.IndexRebuilding and platform_time.monotonicNs() < deadline_ns) {
                // `runUntilIdle` alone is what a sweep barrier count needs
                // to stay deterministic (see `openDbWithIo`), but settling
                // a text index after reopen may need the fuller recipe
                // `connection.prepareDatabaseCompact` uses.
                handle.db.runUntilIdle() catch {};
                handle.db.forceCompactTextIndexes() catch {};
                handle.db.drainScheduledTextMerges() catch {};
                continue;
            }
            std.debug.print("crash-consistency: search() for key={s} text={s} never recovered from {s} after {d} retries\n", .{ key, text, @errorName(err), attempt });
            return err;
        };
        defer result.deinit();
        for (result.hits) |hit| {
            if (std.mem.eql(u8, hit.id, key)) return;
        }
        std.debug.print("crash-consistency: acknowledged document not searchable key={s} text={s}\n", .{ key, text });
        return error.CrashConsistencyFullTextNotRebuilt;
    }
}

/// An acknowledged edge must be readable in both directions: `source` ->
/// `target` via `.out`, and `target` -> `source` via `.in`. Graph's reverse
/// adjacency is maintained alongside the forward edge in the same
/// publication (see graph/graph.zig), so unlike full-text there is no
/// separate catch-up/rebuild window to retry through here.
fn assertGraphEdgeConsistent(handle: *DbHandle, allocator: Allocator, source: []const u8, exp: GraphEdgeExpectation) !void {
    const outgoing = try handle.db.getEdges(allocator, exp.index_name, source, exp.edge_type, .out);
    defer graph_mod.GraphIndex.freeEdges(allocator, outgoing);
    var found_outgoing = false;
    for (outgoing) |edge| {
        if (std.mem.eql(u8, edge.target, exp.target)) {
            found_outgoing = true;
            break;
        }
    }
    if (!found_outgoing) {
        std.debug.print("crash-consistency: acknowledged edge missing forward source={s} target={s} index={s} edge_type={s}\n", .{ source, exp.target, exp.index_name, exp.edge_type });
        return error.CrashConsistencyGraphEdgeMissingForward;
    }

    const incoming = try handle.db.getEdges(allocator, exp.index_name, exp.target, exp.edge_type, .in);
    defer graph_mod.GraphIndex.freeEdges(allocator, incoming);
    var found_incoming = false;
    for (incoming) |edge| {
        if (std.mem.eql(u8, edge.source, source)) {
            found_incoming = true;
            break;
        }
    }
    if (!found_incoming) {
        std.debug.print("crash-consistency: acknowledged edge missing reverse source={s} target={s} index={s} edge_type={s}\n", .{ source, exp.target, exp.index_name, exp.edge_type });
        return error.CrashConsistencyGraphEdgeMissingReverse;
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

/// Runs one owner-queued WS5 retirement turn: raises the foreground
/// watermark to the journal's current tail via `requestRetirement`, then
/// drains it with bounded `serviceRetirement` chunks (looping until
/// `stats.done`, since a single chunk's budget is not guaranteed to cover
/// everything requested). `is_owner = true` matches this harness's own
/// single-writer, no-Raft setup, where the caller asserting ownership is
/// exactly the Lite path-lease holder driving the scenario.
fn runRetirementTurn(allocator: Allocator, handle: *DbHandle) !void {
    _ = allocator;
    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    const up_to = store.lastReplaySequence(0);
    store.requestRetirement(up_to);
    while (true) {
        const stats = try store.serviceRetirement(true, .{});
        if (stats.done) break;
    }
}

// ---------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------

fn runPrimarySingleDoc(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed-value" }} });
    try file.putDocument("seed", "seed-value");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "primary-doc", .before = null, .after = "primary-value" }} });
    try file.putDocument("primary-doc", "primary-value");
}

fn runPrimaryBatch(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed-value" }} });
    try file.putDocument("seed", "seed-value");

    const mutations = [_]native.DocumentMutation{
        .{ .key = "batch-a", .value = "value-a" },
        .{ .key = "batch-b", .value = "value-b" },
        .{ .key = "batch-c", .value = "value-c" },
    };
    try checkpoints.append(allocator, .{ .expectations = &.{
        .{ .key = "batch-a", .before = null, .after = "value-a" },
        .{ .key = "batch-b", .before = null, .after = "value-b" },
        .{ .key = "batch-c", .before = null, .after = "value-c" },
    } });
    try file.putDocumentBatch(&mutations);
}

fn runPrimaryUpdateDelete(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithIo(allocator, io, path, .{});
    defer file.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "key", .before = null, .after = "v1" }} });
    try file.putDocument("key", "v1");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "key", .before = "v1", .after = "v2" }} });
    try file.putDocument("key", "v2");

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "key", .before = "v2", .after = null }} });
    try file.deleteDocument("key");
}

/// §9 (redo + indexed reclamation): redo-log commit and indexed page
/// reclamation combined -- the combination `NativeFile.createWithRedoLog`
/// and `docstore.Store.createWithOptions` both refused outright before
/// this design's work. Every `putDocument` here runs inside its own
/// explicit `beginTransaction`/`commitTransaction` (a *bare* commit --
/// no open transaction -- never diverts at all: `putDocumentBatch`
/// only logs logically when `stagingTransaction()` is true, matching
/// every existing redo-log test's own convention, e.g. "lite redo log
/// commit stages pages without a physical pwrite and recovers after
/// reopen"), so each one is a diverting commit (logged, not built); the
/// first `checkpointRedoLog` call is what actually builds the three
/// documents' real pages, and the second is what supersedes (and
/// retires) them when the same keys are overwritten -- so this
/// scenario, swept at every sync/write barrier inside both calls,
/// directly exercises checkpointRedoLog's own real build +
/// `persistAllocator` + `drainRetirement` + checkpoint-publish sequence,
/// the one path this design adds that the existing redo-log tests
/// (indexed_reclamation always false there) never reach -- "crash
/// mid-checkpoint" and "crash during the retirement work a checkpoint's
/// own real build does inline" (§9.7 items 4 and 7 of the design doc).
///
/// Deliberately only two checkpoints: `checkpointRedoLog`'s own
/// transaction already reaches `pageAllocatorFromFreeMapWithProof`'s
/// indexed branch exactly like any other real commit, so its inline,
/// foreground `drainRetirement` call already exercises the completion
/// path for a backlog this small -- no separate `reclaimPages` call or
/// extra round is needed to reach it.
fn runRedoIndexedCheckpointReclaim(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithRedoLog(allocator, io, path, .{ .indexed_reclamation = true }, native.NativeFile.default_redo_log_capacity_bytes);
    defer file.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "r-doc-0", .before = null, .after = "r-doc-0-v1" }} });
    try file.beginTransaction();
    try file.putDocument("r-doc-0", "r-doc-0-v1");
    try file.commitTransaction();
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "r-doc-1", .before = null, .after = "r-doc-1-v1" }} });
    try file.beginTransaction();
    try file.putDocument("r-doc-1", "r-doc-1-v1");
    try file.commitTransaction();
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "r-doc-2", .before = null, .after = "r-doc-2-v1" }} });
    try file.beginTransaction();
    try file.putDocument("r-doc-2", "r-doc-2-v1");
    try file.commitTransaction();

    // Builds real pages for the three documents above -- the first point
    // this scenario's sweep can crash mid-checkpoint.
    try file.checkpointRedoLog();

    // Diverted/logged against the checkpoint just published: overwriting
    // these keys again is what gives the *next* checkpoint something to
    // retire (the v1 pages it supersedes), rather than only ever
    // allocating fresh ones.
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "r-doc-0", .before = "r-doc-0-v1", .after = "r-doc-0-v2" }} });
    try file.beginTransaction();
    try file.putDocument("r-doc-0", "r-doc-0-v2");
    try file.commitTransaction();
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "r-doc-1", .before = "r-doc-1-v1", .after = "r-doc-1-v2" }} });
    try file.beginTransaction();
    try file.putDocument("r-doc-1", "r-doc-1-v2");
    try file.commitTransaction();
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "r-doc-2", .before = "r-doc-2-v1", .after = "r-doc-2-v2" }} });
    try file.beginTransaction();
    try file.putDocument("r-doc-2", "r-doc-2-v2");
    try file.commitTransaction();

    // Builds real pages for the overwrites and retires the v1 pages they
    // superseded -- the second (and main) point this scenario's sweep can
    // crash mid-checkpoint, mid-retire, or mid-publish.
    try file.checkpointRedoLog();
}

/// Regression coverage for the vacuum redo-log-retention fix
/// (`native.zig`'s `prepareVacuum`/`replaceWithPreparedGeneration`): a
/// vacuum used to always rebuild a plain, non-redo compacted image,
/// permanently reverting a redo-log store to the legacy commit path after
/// its first vacuum. Writes before the vacuum, the vacuum's own
/// rename+sync, and writes after it (through the redo log again) are all
/// within this sweep's crash range, so a crash landing anywhere --
/// including the moment the vacuum itself becomes durable, or mid-append
/// of the very first commit the freshly-reserved post-vacuum log region
/// ever receives -- must still recover correctly.
fn runVacuumThenRedoWrites(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var file = try native.NativeFile.createWithRedoLog(allocator, io, path, .{}, native.NativeFile.default_redo_log_capacity_bytes);
    defer file.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed-value" }} });
    try file.beginTransaction();
    try file.putDocument("seed", "seed-value");
    try file.commitTransaction();

    var image = try file.prepareVacuum(null);
    defer image.deinit();
    try file.publishVacuum(&image);

    // First commit into the vacuumed generation's own, freshly reserved
    // (and, before this fix, never-reserved-at-all) log region.
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "after-vacuum-1", .before = null, .after = "one" }} });
    try file.beginTransaction();
    try file.putDocument("after-vacuum-1", "one");
    try file.commitTransaction();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "after-vacuum-2", .before = null, .after = "two" }} });
    try file.beginTransaction();
    try file.putDocument("after-vacuum-2", "two");
    try file.commitTransaction();

    // Folds the post-vacuum log -- only reachable at all if the vacuum
    // really did leave `self.redo` populated (`checkpointRedoLog` returns
    // `error.InvalidTransactionState` otherwise, which would fail this
    // scenario's own discovery pass outright, before the sweep even
    // starts injecting crashes).
    try file.checkpointRedoLog();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "after-fold", .before = null, .after = "three" }} });
    try file.beginTransaction();
    try file.putDocument("after-fold", "three");
    try file.commitTransaction();
}

/// Fixed, fully comptime-known (writer, round) interleaving: 4 logical
/// writers each committing 3 sequential rounds, writer-major order. Every
/// key/value pair is a comptime string so `Checkpoint.expectations` below
/// can keep pointing at it after this function returns, with no allocation
/// and nothing to free.
const ConcurrentStep = struct { key: []const u8, before: ?[]const u8, after: []const u8 };
const concurrent_writer_count = 4;
const concurrent_round_count = 3;
const concurrent_steps: [concurrent_writer_count * concurrent_round_count]ConcurrentStep = blk: {
    var steps: [concurrent_writer_count * concurrent_round_count]ConcurrentStep = undefined;
    var i: usize = 0;
    for (0..concurrent_round_count) |round| {
        for (0..concurrent_writer_count) |writer| {
            steps[i] = .{
                .key = std.fmt.comptimePrint("writer-{d}", .{writer}),
                .before = if (round == 0) null else std.fmt.comptimePrint("round-{d}", .{round - 1}),
                .after = std.fmt.comptimePrint("round-{d}", .{round}),
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
        try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = step.key, .before = step.before, .after = step.after }} });
        try file.putDocument(step.key, step.after);
    }
}

fn runFullTextSyncVisible(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed body" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "ft-doc", .before = null, .after = "needle in a haystack", .searchable_after = "needle" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
}

/// Ride-along invariant scenario: a non-durable commit (WS5
/// `docstore.Store.submitMutationWithDurability(..., false)`) sits
/// between two ordinary, durable full-text writes. The call still returns
/// success -- it is "acknowledged" exactly like any other checkpoint --
/// but durability=false means a crash before the next durable barrier may
/// still roll it back; the checker (see `Checkpoint.durable`) allows
/// `maybe-doc` to resolve to either `before` or `after`, never requiring
/// `after` the way the durable writes around it are. The always-on
/// journal-credit-ledger check in `checkCheckpointsViaDb` rides along for
/// every crash point this sweep reaches, including inside and around this
/// non-durable commit: a crash must never durably charge (or release)
/// credit without the matching journal effect, or the reverse.
fn runFullTextWithNonDurableCommit(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed body" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    const NonDurableWrite = struct {
        fn apply(_: *anyopaque, file: *native.NativeFile) anyerror!void {
            try file.putDocument("maybe-doc", "maybe body");
        }
    };
    try checkpoints.append(allocator, .{
        .expectations = &.{.{ .key = "maybe-doc", .before = null, .after = "maybe body" }},
        .durable = false,
    });
    var non_durable_ctx: u8 = 0;
    try store.submitMutationWithDurability(&non_durable_ctx, NonDurableWrite.apply, false);

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "ft-doc", .before = null, .after = "needle in a haystack", .searchable_after = "needle" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
}

const RetirementDoc = struct { key: []const u8, body: []const u8 };
const retirement_docs = [_]RetirementDoc{
    .{ .key = "r-doc-0", .body = "alpha retirement payload" },
    .{ .key = "r-doc-1", .body = "bravo retirement payload" },
    .{ .key = "r-doc-2", .body = "charlie retirement payload" },
};

/// Writes a handful of full-text-indexed documents, then runs one
/// retirement turn (see `runRetirementTurn`). The crash sweep naturally
/// crosses barriers both inside the writes and inside the retirement call
/// itself, exercising "crash during retirement" without a separate
/// scenario.
fn runFullTextWithRetirement(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    inline for (retirement_docs) |doc| {
        const json = try std.fmt.allocPrint(allocator, "{{\"body\":\"{s}\"}}", .{doc.body});
        defer allocator.free(json);
        try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = doc.key, .before = null, .after = doc.body, .searchable_after = "retirement" }} });
        try handle.db.batch(.{ .writes = &.{.{ .key = doc.key, .value = json }}, .sync_level = .full_text });
    }

    // The retirement turn itself touches no document key, so it adds no
    // checkpoint: nothing it does should ever change what a document reads
    // back as. `checkCheckpointsViaDb`'s `retirement` flag separately
    // verifies the journal is still readable no matter where the sweep
    // crashes inside this call.
    try runRetirementTurn(allocator, &handle);
}

/// Regression coverage for a bug found alongside dense's native-catalog
/// storage work: a crash-sweep scenario that calls `db.addIndex()` for a
/// SECOND index mid-run (after the default `body_text` index already
/// exists) used to see cascading `OutcomeUnknown`/`InputOutput` in the
/// UNCRASHED discovery pass, including on `body_text` itself, which this
/// scenario never touches via `addIndex`. Root cause:
/// `DB.addIndexForReconciliation` funneled every post-commit activation
/// error -- including an outcome-unknown one from an injected crash/fault --
/// into `recoverCommittedIndexAdmission`, which always returned success (a
/// repair id or `null`). That violated this harness's own contract (`Scenario
/// .run` "must propagate every error, including the shim's injected crash
/// error"): `addIndex()` reporting success over state the native commit
/// protocol could not actually confirm let the discovery pass's "never
/// crashed" branch run strict checks against a reopened file whose on-disk
/// checkpoint had legitimately rolled back, and `checkpoint_publication_
/// uncertain` is file-wide, so that rollback poisoned every subsequent
/// `beginTransaction()` -- including `body_text`'s own background catch-up.
/// Fixed in `recoverCommittedIndexAdmission` (db.zig): an outcome-unknown
/// activation error is now propagated instead of swallowed (same error
/// class `batchTrackedWithMarkerAndWriterLaneHook` already refuses to
/// swallow for ordinary primary writes).
///
/// Like `runFullTextWithRetirement`'s retirement turn, `addIndex` itself
/// touches no document key, so it adds no `Checkpoint` of its own -- but the
/// sweep still crosses every barrier inside that call. A `body_text` write
/// both before and after `addIndex` proves the shared catalog/checkpoint
/// state survives the second index's admission; the vector write afterward
/// proves the new index's own admission+write path is crash-consistent too.
fn runDenseVectorAddIndexMidScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed body", .searchable_after = "seed" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .full_text });

    try handle.db.addIndex(.{
        .name = "dv_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "after-add-index", .before = null, .after = "needle in a haystack", .searchable_after = "needle" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "after-add-index", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

    // `after` checks a substring of the stored document's own JSON (see
    // `checkInFlightResolutionDb`'s round-trip note), so this checks a
    // plain marker field rather than the `_embeddings` directive itself,
    // whose own on-disk round-trip shape is not this test's concern.
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "dv-doc", .before = null, .after = "dv-doc-marker" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker\",\"_embeddings\":{\"dv_idx\":[1.0,0.0]}}" }}, .sync_level = .full_index });
}

/// Dense-one-commit task 4: an update then a delete of an already-indexed
/// dense-vector document, each acked at `sync_level: .full_index` (the
/// level that blocks until the dense index itself is visible, not just the
/// primary row -- `lite-single-commit-write-path.md` §3.3a). Covers the
/// crash invariants that scenario doesn't: `commitDenseVectorMappingsWithRollback`
/// 's own separate commit (`.hbc_metadata_commit`) and
/// `saveProjectionCheckpointMetadata`'s now-`.ride_along` commit
/// (`.hbc_checkpoint_commit`) both run on every dense write, not just a
/// first insert, so an update/delete sweep exercises them repeatedly
/// (including a ride-along commit losable at any crash point) while the
/// harness's generic `splitCheckpoints`/`checkInFlightResolution` machinery
/// still proves "every acknowledged write's effects survive a crash
/// exactly once, never partially" for each -- the document-level stand-in
/// this harness has for "every acked vector is queryable after reopen"
/// (there is no dedicated vector-query `Expectation` field the way
/// `searchable_after`/`graph_edge_after` exist for full-text/graph; the
/// marker-field substring check below is this harness's existing
/// mechanism, same as the add-index-mid scenario above).
fn runDenseVectorUpdateDeleteScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try handle.db.addIndex(.{
        .name = "dv_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "dv-doc", .before = null, .after = "dv-doc-marker-v1" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker-v1\",\"_embeddings\":{\"dv_idx\":[1.0,0.0]}}" }}, .sync_level = .full_index });

    // Update: a second write to the SAME key, with a different vector --
    // exercises `deleteDenseBatchByNameWithOptions`'s replacement-delete
    // lane (index_manager.zig) ahead of the re-insert, not just a fresh
    // insert's empty-index fast path.
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "dv-doc", .before = "dv-doc-marker-v1", .after = "dv-doc-marker-v2" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker-v2\",\"_embeddings\":{\"dv_idx\":[0.0,1.0]}}" }}, .sync_level = .full_index });

    // Delete: the dense-vector mapping and HBC posting entry must both be
    // gone after reopen, in lockstep with the primary row's own tombstone
    // -- an acknowledged delete whose dense side survives a crash would be
    // exactly the "published metadata pointing at unwritten/removed
    // postings" invariant this change's ride-along collapse must not
    // introduce (lite-single-commit-write-path.md §3.2a).
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "dv-doc", .before = "dv-doc-marker-v2", .after = null }} });
    try handle.db.batch(.{ .deletes = &.{"dv-doc"}, .sync_level = .full_index });

    // One more insert after the delete, proving the index (and its
    // ride-along checkpoint commit) is still writable/consistent --
    // nothing about the delete or the checkpoint's ride-along durability
    // left the index wedged.
    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "dv-doc-2", .before = null, .after = "dv-doc-marker-v3" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "dv-doc-2", .value = "{\"marker\":\"dv-doc-marker-v3\",\"_embeddings\":{\"dv_idx\":[0.5,0.5]}}" }}, .sync_level = .full_index });
}

/// #1216 crash-sim: deleting a `.native_catalog`-backed dense index must
/// never leave the catalog saying the index is gone while its own HBC
/// metadata (dims, root node, postings), held under a fixed name-derived
/// scope in `native_catalog_backend.Backend`, survives underneath --
/// exactly the ordering `removeWithAtomicMutation`'s dense branch depends
/// on: `HBCIndex.deleteNativeCatalogScopeForRemoval` (one durable
/// `native_catalog_backend.deleteAllForScope` batch) must complete before
/// the catalog-removal commit (`persistCatalogExcludingWithAtomicMutation`)
/// becomes durable.
///
/// Setup (admitting "img" at dims=3, writing one document, draining to
/// idle) runs on plain `std.testing.io`, fully durable, *before* the swept
/// region starts, so the harness's barrier discovery only ever sees the
/// reopen and the delete itself -- the scope-delete batch's own sync and
/// the catalog-removal commit's own sync, plus whatever reopen/reload
/// needs (reads, harmless to interrupt). A broader sweep that also crossed
/// the *initial* admission's own commit surfaced a second, separate
/// finding (a crash during first-ever admission can similarly split
/// "scope has fresh metadata" from "catalog has the config" -- a creation-
/// side atomicity question, not this delete-side one); narrowing to the
/// delete keeps this scenario pinned to exactly what #1216 and this fix
/// are about.
fn runDenseNativeCatalogDimensionDeleteScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    _ = checkpoints;
    {
        var setup = try openDbWithIo(allocator, path, std.testing.io, true);
        defer setup.close();
        _ = try setup.db.admitManagedIndex(.{
            .name = "img",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
        });
        try setup.db.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"body\":\"old document\",\"_embeddings\":{\"img\":[1,0,0]}}" }} });
        try setup.db.runUntilIdle();
    }

    var handle = try openDbWithIo(allocator, path, io, false);
    defer handle.close();
    _ = try handle.db.deleteIndex("img");
}

/// Reopens with ordinary IO (matching every other scenario's post-crash
/// check) and asserts the only two consistent outcomes: either "img" is
/// still configured -- the delete never became durable, or only its
/// scope-delete half did -- in which case the catalog's own config (still
/// dims=3) must still open and search cleanly, or "img" is gone and its
/// scope is provably clean, proven by re-admitting it at a *different*
/// dims (4) and writing/searching through it. A `DimensionMismatch` (or
/// any other error) from either branch is exactly the bug: the catalog
/// disagreeing with the native-catalog metadata a reopen or re-admission
/// actually finds.
fn checkDenseNativeCatalogDimensionDeleteInvariant(
    allocator: Allocator,
    path: []const u8,
    outcome: ?CrashOutcome,
    checkpoints: []const Checkpoint,
) anyerror!void {
    _ = outcome;
    _ = checkpoints;
    var handle = try openDbWithIo(allocator, path, std.testing.io, false);
    defer handle.close();

    if (handle.db.core.index_manager.get("img") != null) {
        var result = try handle.db.search(allocator, .{
            .index_name = "img",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 8 } },
            .limit = 8,
        });
        result.deinit();
        return;
    }

    // The delete completed (or its catalog-removal half did). Deletion
    // schedules generated-artifact cleanup on the durable repair owner;
    // join it before reusing the name, same as the #1216 regression test
    // ("lite external dense dimension replacement keeps writes
    // available"), so a nonblocking `.busy` response (lease contention)
    // is never mistaken for the stale-scope bug this scenario exists to
    // catch.
    handle.db.backend_runtime.durable_jobs.drainOwner(handle.db.repair_cleanup_owner_id);
    for (0..20) |_| {
        if ((try handle.db.advanceGeneratedArtifactCleanupPage("img")) == .idle) break;
    }

    _ = try handle.db.admitManagedIndex(.{
        .name = "img",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":4,\"external\":true}",
    });
    try handle.db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"body\":\"new document\",\"_embeddings\":{\"img\":[0,1,0,0]}}" }} });
    try handle.db.runUntilIdle();
    var result = try handle.db.search(allocator, .{
        .index_name = "img",
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0, 0 }, .k = 8 } },
        .limit = 8,
    });
    result.deinit();
}

/// #1216, both directions: the unnarrowed sweep, crossing the *initial*
/// admission's own commit as well as the delete's. Covers the creation-
/// side half of the bug (a crash between seeding fresh native-catalog
/// metadata and committing the admission's catalog config can orphan that
/// metadata) in addition to everything
/// `runDenseNativeCatalogDimensionDeleteScenario` already covers on the
/// delete side -- the admission-side fix
/// (`IndexManager.fresh_admission_name`, set by `addWithOptions` and read
/// by `openConfiguredIndexDetached`'s dense_vector branch to wipe an
/// orphan scope before seeding fresh metadata) is what makes crashing
/// during the *first* admission self-heal on retry instead of surfacing a
/// stale, mismatched generation's dims.
fn runDenseNativeCatalogDimensionAdmitAndDeleteScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    _ = checkpoints;
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    _ = try handle.db.admitManagedIndex(.{
        .name = "img",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });
    try handle.db.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"body\":\"old document\",\"_embeddings\":{\"img\":[1,0,0]}}" }} });
    try handle.db.runUntilIdle();

    _ = try handle.db.deleteIndex("img");
}

/// Reopens with ordinary IO and asserts the only two consistent outcomes,
/// now covering a crash anywhere in the whole admit-write-delete sequence
/// (including the very first admission's own commit, where "img" may not
/// durably exist in the catalog at all yet): either "img" is configured --
/// opening/searching it with whatever dims the catalog currently holds
/// must work -- or "img" is absent and admitting it fresh, at a dims that
/// deliberately differs from every dims this scenario ever used (4, never
/// 3), must succeed cleanly. A `DimensionMismatch` from the second branch
/// means a stale, orphaned generation (from an interrupted first admission
/// or an interrupted delete) survived under the scope with no catalog
/// entry to account for it.
fn checkDenseNativeCatalogDimensionAdmitAndDeleteInvariant(
    allocator: Allocator,
    path: []const u8,
    outcome: ?CrashOutcome,
    checkpoints: []const Checkpoint,
) anyerror!void {
    _ = outcome;
    _ = checkpoints;
    // Unlike the delete-only scenario, this one's swept region starts at
    // the very first `createWithOptions`, so "before barrier 0, power loss
    // drops unsynced writes" can mean the file itself never became
    // durable. Nothing was ever acknowledged in that case -- an absent
    // file is as consistent an outcome as an empty one -- so there is
    // nothing to verify.
    var handle = openDbWithIo(allocator, path, std.testing.io, false) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer handle.close();

    // `run` above only ever admits "img" at dims=3 (the re-admission below,
    // at dims=4, is reached only when the catalog has no entry at all), so
    // a live catalog entry here is always dims=3.
    if (handle.db.core.index_manager.get("img") != null) {
        var result = try handle.db.search(allocator, .{
            .index_name = "img",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 8 } },
            .limit = 8,
        });
        result.deinit();
        return;
    }

    handle.db.backend_runtime.durable_jobs.drainOwner(handle.db.repair_cleanup_owner_id);
    for (0..20) |_| {
        if ((try handle.db.advanceGeneratedArtifactCleanupPage("img")) == .idle) break;
    }

    _ = try handle.db.admitManagedIndex(.{
        .name = "img",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":4,\"external\":true}",
    });
    try handle.db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"body\":\"new document\",\"_embeddings\":{\"img\":[0,1,0,0]}}" }} });
    try handle.db.runUntilIdle();
    var result = try handle.db.search(allocator, .{
        .index_name = "img",
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0, 0 }, .k = 8 } },
        .limit = 8,
    });
    result.deinit();
}

/// Sparse analog of `runDenseVectorAddIndexMidScenario`: same bug, same
/// fix, same shape, with a `sparse_vector` index and the flat
/// `{"<token id>": <weight>}` `_embeddings` wire shape instead of a dense
/// array (see `write_amplification_bench_test.zig`'s `sparseVectorJsonAlloc`
/// for the same shape used elsewhere).
fn runSparseVectorAddIndexMidScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed body", .searchable_after = "seed" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .full_text });

    try handle.db.addIndex(.{
        .name = "sp_idx",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"embedding\",\"external\":true}",
    });

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "after-add-index", .before = null, .after = "needle in a haystack", .searchable_after = "needle" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "after-add-index", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "sp-doc", .before = null, .after = "sp-doc-marker" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "sp-doc", .value = "{\"marker\":\"sp-doc-marker\",\"_embeddings\":{\"sp_idx\":{\"5\":1.0,\"12\":0.5}}}" }}, .sync_level = .full_index });
}

/// Graph analog of runDenseVectorAddIndexMidScenario/
/// runSparseVectorAddIndexMidScenario: same shape, a `graph` index instead
/// of a vector one, and an edge write asserted consistent in both
/// directions via `Expectation.graph_edge_after` (see
/// assertGraphEdgeConsistent) once it resolves durably/acknowledged --
/// "every acked edge present after reopen, reverse consistent with
/// forward".
fn runGraphAddIndexMidScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed body", .searchable_after = "seed" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .full_text });

    try handle.db.addIndex(.{
        .name = "gr_idx",
        .kind = .graph,
        .config_json = "{}",
    });

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "after-add-index", .before = null, .after = "needle in a haystack", .searchable_after = "needle" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "after-add-index", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

    try checkpoints.append(allocator, .{ .expectations = &.{
        .{
            .key = "edge-source",
            .before = null,
            .after = "edge-source-marker",
            .graph_edge_after = .{ .index_name = "gr_idx", .edge_type = "links", .target = "edge-target" },
        },
        .{ .key = "edge-target", .before = null, .after = "edge-target-marker" },
    } });
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "edge-source", .value = "{\"marker\":\"edge-source-marker\",\"_edges\":{\"gr_idx\":{\"links\":[{\"target\":\"edge-target\",\"weight\":1.0}]}}}" },
        .{ .key = "edge-target", .value = "{\"marker\":\"edge-target-marker\"}" },
    }, .sync_level = .full_index });
}

/// Crash-sweep coverage for the graph metric-build pipeline's §3.2a
/// ride-along conversion (lease acquire/prepare/scan/reduce/publish/cleanup
/// pages: `graph/graph.zig`'s background call sites now open through
/// `beginWriteReverseBatchWithOptions(.{ .durability = .ride_along })`
/// instead of a plain `beginBatch()`).
///
/// Admits the graph index with NO metrics in its `config_json` (exactly
/// `runGraphAddIndexMidScenario`'s own proven-safe shape) and only attaches
/// `metric_configs` to the already-admitted `GraphIndex` directly,
/// reaching through `handle.db.core.graphIndex(...)`. Declaring metrics
/// *at admission time* (`config_json`'s `"metrics"` key) was tried first
/// and found to hit a pre-existing, unrelated bug: it calls
/// `notifyGraphMetricConfiguration`, which races the admission commit's own
/// follow-up native-catalog work and nondeterministically surfaces as
/// `UnexpectedNativePageKind` or `InvalidNativeAllocator` -- reproduces
/// identically with the §3.2a ride-along conversion reverted, and
/// independently of `start_index_workers`, so it is a pre-existing
/// admission/managed-admission-materialization defect, not something this
/// change introduced or is positioned to fix. See the handback's
/// graph-crash-scenario note.
///
/// `GraphIndex.runGraphMetric` then drives a build synchronously to
/// completion on the calling thread, through the very call sites this
/// change converted, interleaved with ordinary acknowledged edge writes --
/// the sweep crashes mid-build as often as mid-write. Every one of those
/// background commits may be lost at a crash -- recomputable from durable
/// progress, same as any other row in §3.2a's ride-along table -- but
/// every acknowledged edge write (`sync_level = .full_index`) must
/// survive, and the reverse index must stay consistent with forward
/// adjacency: `graph_edge_after`/`assertGraphEdgeConsistent`, same
/// contract as `runGraphAddIndexMidScenario`. One metric (`degree`) keeps
/// this scenario's own barrier count close to the other scenarios in this
/// file -- a build with two metrics and two write/rebuild rounds was tried
/// first and, past roughly 40 barriers, started tripping an unrelated,
/// pre-existing `InvalidNativeAllocator`/`UnexpectedNativePageKind`
/// flakiness that reproduces with no graph or metric involvement at all
/// (plain `openDbWithIo` + `runUntilIdle` alone, graph.zig fully
/// reverted); see the handback's graph-crash-scenario note.
fn runGraphMetricBuildWithCrashScenario(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    try checkpoints.append(allocator, .{ .expectations = &.{.{ .key = "seed", .before = null, .after = "seed body", .searchable_after = "seed" }} });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .full_text });

    try handle.db.addIndex(.{
        .name = "gr_metric_idx",
        .kind = .graph,
        .config_json = "{}",
    });

    const metrics = [_]graph_mod.GraphMetricConfig{
        .{ .name = "degree", .kind = .degree, .refresh = .manual },
    };
    const entry = handle.db.core.graphIndex("gr_metric_idx") orelse return error.CrashConsistencyMissingGraphIndex;
    entry.index.metric_configs = &metrics;

    try checkpoints.append(allocator, .{ .expectations = &.{
        .{
            .key = "node-a",
            .before = null,
            .after = "node-a-marker",
            .graph_edge_after = .{ .index_name = "gr_metric_idx", .edge_type = "links", .target = "node-b" },
        },
        .{ .key = "node-b", .before = null, .after = "node-b-marker" },
    } });
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "node-a", .value = "{\"marker\":\"node-a-marker\",\"_edges\":{\"gr_metric_idx\":{\"links\":[{\"target\":\"node-b\",\"weight\":1.0}]}}}" },
        .{ .key = "node-b", .value = "{\"marker\":\"node-b-marker\"}" },
    }, .sync_level = .full_index });

    // Drive the metric build synchronously to completion through its
    // ride-along commits, still inside the sweep's fault-injected window.
    var status = try entry.index.runGraphMetric("degree");
    status.deinit(allocator);
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

test "crash sweep: redo log checkpoint with indexed reclamation retires pages" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "redo-indexed-checkpoint-reclaim", .run = runRedoIndexedCheckpointReclaim }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: a vacuum followed by ordinary redo writes survives a crash anywhere, including right after the vacuum itself" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "vacuum-then-redo-writes", .run = runVacuumThenRedoWrites }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "WS4 phase 2: a synchronous full_text write lands in exactly one native commit and is searchable immediately" {
    // Deliberately NOT a crash-sweep scenario: this is the direct,
    // uninterrupted claim (one commit, searchable right after ack)
    // through the real DB/capi write path -- `handle.db.batch(...)` with
    // `sync_level = .full_text`, the same entry point
    // `runFullTextSyncVisible` below exercises under fault injection.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    // Not `openDbWithIo`: this scenario specifically covers the no-worker
    // configuration (background index workers never started). WS4 phase 2
    // round 2 made the inline fast path engage with workers running too --
    // see the "...with background index workers running..." test below
    // for that configuration. Force the no-worker configuration explicitly
    // here rather than relying on the shared helper's default.
    var lite_backend = try backend.Handle.createWithOptions(allocator, path, .{ .exclusive = true, .io = std.testing.io });
    var backend_owned = true;
    errdefer if (backend_owned) lite_backend.deinit();
    const identity = connection.identityOpenOptions(true);
    var db_opts = db_mod.OpenOptions{
        .open_mode = .writer,
        .identity_namespace = identity.identity_namespace,
        .prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace,
        .start_index_workers = false,
    };
    try lite_backend.configureDbOpenOptions(&db_opts);
    var db = try db_mod.DB.open(allocator, path, db_opts);
    var db_owned = true;
    errdefer if (db_owned) db.close();
    try db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
    try db.runUntilIdle();
    backend_owned = false;
    db_owned = false;
    var handle = DbHandle{ .backend = lite_backend, .db = db };
    defer handle.close();

    // A seed write lets the index's own one-time catalog admission/
    // activation commit (independent of this test's claim) happen before
    // the commit count below is sampled.
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    // Deterministic settle: force any ride-along backlog from addIndex/seed
    // (e.g. a .native_catalog backend's Backend.pending_ride_along, still
    // only staged -- see native_catalog_backend.zig's NamespaceTxn.commit)
    // durable now, synchronously, instead of leaving it to the 50 ms
    // idle-flush timer. Under load, that timer racing the sample below
    // could otherwise sweep setup-phase backlog into "commits_for_this_
    // write", inflating it independently of anything the write itself did.
    try store.sync();
    const before_sequence = store.file.activeCheckpoint().commit_sequence;

    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

    const after_sequence = store.file.activeCheckpoint().commit_sequence;
    const commits_for_this_write = after_sequence - before_sequence;
    // WS4 phase 2's inline publication itself contributes exactly one
    // commit (proven directly in isolation: see persistent.zig's and
    // lite/docstore.zig's own unit tests for this mechanism). End to end
    // through the real DB path, this still measures a handful more here
    // -- unrelated background bookkeeping (replication outbox staging,
    // opportunistic maintenance ticks) this change does not touch and
    // WS7 owns driving to zero, per the design doc's acceptance gates.
    // Before this change, the OLD multi-commit path
    // (indexSegmentOwned/publishProjectionSeal/progress, each its own
    // commit) measured roughly a dozen extra commits for the same write
    // (see docs/design/lite-performance/write-amplification-baseline.json's
    // "indexed" numbers); this regression guard catches a reversion back
    // toward that, without claiming the still-outstanding literal "one
    // commit total" end-to-end goal.
    try std.testing.expect(commits_for_this_write <= 6);

    var result = try handle.db.search(allocator, .{
        .index_name = "body_text",
        .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
}

test "WS4 phase 2 round 2: a synchronous full_text write lands in one native commit with background index workers running, and the worker never re-applies it" {
    // Default Lite configuration: background index workers running
    // (`openDbWithIo` does not override `start_index_workers`, so it keeps
    // the open-options default). Before this change, this configuration
    // took the notify-workers-and-wait path and saw none of WS4 phase 2's
    // benefit; this test is the one from directly above, but through the
    // path this round actually targets.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
    try handle.db.runUntilIdle();

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    // Deterministic settle: see the no-worker test above's identical note.
    try store.sync();
    // `committedSequence()`, not `activeCheckpoint().commit_sequence`: this
    // scenario's whole point is background index workers committing
    // concurrently with the write under measurement, and
    // `activeCheckpoint().commit_sequence` reads `self.header`'s live
    // commit_sequence field directly -- the same field `stageMutation`
    // provisionally bumps once per *staged mutation* while some other
    // transaction is still open (collapsed back to a single real advance
    // only at that transaction's own commit). Reading it from outside the
    // store's own lock, racing a concurrent worker's in-flight,
    // multi-mutation batch, can catch that provisional, not-yet-committed
    // count and wildly overstate how many commits actually happened
    // (observed directly: commits_for_this_write below, before
    // `committedSequence()` was substituted here, swinging between 5 and
    // 36 run to run with no code change at all -- traced to a concurrent
    // worker's own 20-30-mutation batch still being staged, each staged
    // mutation bumping this same field, at the exact moment this read
    // landed). `committedSequence()` -- `redo.durable_sequence` for a
    // redo-log file -- only advances when a commit actually becomes
    // durable, never provisionally.
    const before_sequence = store.file.committedSequence();

    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

    const after_ack_sequence = store.file.committedSequence();
    const commits_for_this_write = after_ack_sequence - before_sequence;
    // Unlike the no-worker scenario above, this configuration's own
    // background index worker is genuinely committing concurrently with
    // the measured write (that is the whole point of running it "with
    // background index workers running"), so this bound cannot be the
    // tight "inline publication is exactly one commit" one the no-worker
    // test asserts. 25 runs with `recovery_debug_log` on, read directly
    // (not inferred from `commits_for_this_write`), show exactly this
    // shape every time: one ride-along commit capturing a pre-chain
    // `durable_header` baseline, one durable commit consuming it, that
    // pattern repeating zero to two more times purely from the
    // background worker's own unrelated catch-up commits landing before
    // and/or after this write's own single commit happens to be staged
    // (visible directly in the trace: the write's own `Txn.commitStaged`
    // -- `mutation_count=26`, `index_catalog_mutation_count=4` in a
    // representative run -- sits in the middle of the sequence, with
    // background ride-along/durable pairs on either side), and finally
    // one trailing ride-along after this write's own commit, again the
    // worker's. Observed range across 25 runs: 5-7, never above 7 --
    // `<= 6` from before this comment is the no-worker bound, carried
    // over here by mistake rather than re-derived for this scenario; `<=
    // 8` leaves one extra background ride-along/durable pair of headroom
    // above the observed maximum. None of this is the measured write
    // paying for extra commits of its *own* -- its own inline publication
    // is still exactly the one commit the no-worker test proves -- it is
    // this test's own assertion window being wide enough to also catch
    // whatever the background worker was doing at the same moment, by
    // design (this scenario exists specifically to run with that worker
    // active).
    try std.testing.expect(commits_for_this_write <= 8);

    // Searchable immediately after ack, before the background worker has
    // had any chance to run.
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }

    // Let the background worker observe this write and decide whether to
    // catch this index up. If it is not told the write already landed
    // inline, it will re-apply the same document from the journal --
    // doubling the hit count below (two postings for the same unique term)
    // even though document storage itself is idempotent by key.
    try handle.db.runUntilIdle();

    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }

    // A second write to a different document, still synchronous full_text,
    // still with the worker running: the same claims hold repeatedly, not
    // just for a single write before the worker ever wakes.
    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc-2", .value = "{\"body\":\"another needle here\"}" }}, .sync_level = .full_text });
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    }
    try handle.db.runUntilIdle();
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    }
}

test "WS4-for-graph: a synchronous graph edge write lands in one native commit and is queryable immediately" {
    // Graph analog of the full_text round 2 test above (lite-single-
    // commit-write-path.md §3.3a's graph extension): default Lite
    // configuration (background index workers running), one edge write
    // expressed through a document's `_edges` field, `sync_level =
    // .full_index` (graph has no narrower sync level of its own -- see
    // `types.SyncLevel`'s enum order). Proves `InlineGraphPublication`
    // engages for the simple single-edge case: the edge is queryable
    // immediately after ack, and the background worker's later catch-up
    // does not re-apply it (no duplicate edge).
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    try handle.db.addIndex(.{ .name = "gr_idx", .kind = .graph, .config_json = "{}" });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"marker\":\"seed\"}" }}, .sync_level = .write });
    try handle.db.runUntilIdle();

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    // Deterministic settle: see "WS4 phase 2"'s identical note above. The
    // graph index's own .native_catalog backend can still have ride-along
    // commits (e.g. initial metric/catalog bookkeeping) staged but not yet
    // durable after addIndex/seed/runUntilIdle; force them durable now so
    // the measured write's commit count can't absorb them under load.
    try store.sync();
    const before_sequence = store.file.activeCheckpoint().commit_sequence;

    try handle.db.batch(.{ .writes = &.{
        .{ .key = "edge-source", .value = "{\"marker\":\"edge-source-marker\",\"_edges\":{\"gr_idx\":{\"links\":[{\"target\":\"edge-target\",\"weight\":1.0}]}}}" },
        .{ .key = "edge-target", .value = "{\"marker\":\"edge-target-marker\"}" },
    }, .sync_level = .full_index });

    const after_ack_sequence = store.file.activeCheckpoint().commit_sequence;
    const commits_for_this_write = after_ack_sequence - before_sequence;
    // The inline publication itself still contributes no extra commit
    // beyond the primary write's own (its forward/reverse mutations and
    // progress ride inside the SAME native transaction via
    // `commit_participant.View.putIndexCatalog`). The bound used to be much
    // wider (16) to absorb two separate sources of inflation that were
    // never this write's own cost, both fixed by perf/lwa-commit-eff2:
    // (a) a redundant progress-v2 commit for a consumer whose inline
    // publication had already written the identical durable record --
    // `apply_state.contributeProgressTxn` now skips the write (and the
    // whole commit, if every consumer in it turns out to be a no-op) when
    // the durable record already covers the target; (b) the `.full_index`
    // visibility wait's own forced-persist pass calling the persist
    // callback once per consumer needing a flush instead of noting all of
    // them and flushing once -- `io_threaded_runtime.zig`'s
    // `persistSnapshots` now prefers a batched callback
    // (`runtime_types.PersistBatchFn`, bound to `db.zig`'s
    // `persistAppliedSequencesBatchAsync`) when one is registered. What is
    // left (observed 7, stable across repeated runs, no longer "not fully
    // deterministic") is this test harness's own extra managed index
    // (`openDbWithIo` always also adds `body_text`) needing its own
    // forced-visibility pass, plus whatever maintenance/catch-up activity
    // is genuinely concurrent with this write -- not a per-write cost this
    // single edge write pays on its own. 9 is comfortably above the
    // observed 7, not tight to it.
    try std.testing.expect(commits_for_this_write <= 9);

    {
        const edges = try handle.db.getEdges(allocator, "gr_idx", "edge-source", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(allocator, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqualStrings("edge-target", edges[0].target);
    }

    // Let the background worker observe this write. If it were not told
    // the edge already landed inline (`Executor.
    // advanceAppliedSequenceIfContiguous`), it would re-apply the same
    // edge from the journal, doubling the forward adjacency list.
    try handle.db.runUntilIdle();
    {
        const edges = try handle.db.getEdges(allocator, "gr_idx", "edge-source", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(allocator, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }

    // A second edge write, still synchronous, still with the worker
    // running: the same claims hold repeatedly.
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "edge-source-2", .value = "{\"marker\":\"edge-source-2-marker\",\"_edges\":{\"gr_idx\":{\"links\":[{\"target\":\"edge-target\",\"weight\":2.0}]}}}" },
    }, .sync_level = .full_index });
    {
        const edges = try handle.db.getEdges(allocator, "gr_idx", "edge-target", "links", .in);
        defer graph_mod.GraphIndex.freeEdges(allocator, edges);
        try std.testing.expectEqual(@as(usize, 2), edges.len);
    }
    try handle.db.runUntilIdle();
    {
        const edges = try handle.db.getEdges(allocator, "gr_idx", "edge-target", "links", .in);
        defer graph_mod.GraphIndex.freeEdges(allocator, edges);
        try std.testing.expectEqual(@as(usize, 2), edges.len);
    }
}

test "WS4-for-dense: a synchronous dense vector write lands in one native commit and is queryable immediately" {
    // Dense analog of the full_text/graph tests above (lite-single-
    // commit-write-path.md §3.3a's dense extension): default Lite
    // configuration (background index workers running), one caller-
    // supplied vector write expressed through a document's `_embeddings`
    // field on an `external: true` dense index, `sync_level = .full_index`
    // (dense has no narrower sync level of its own -- see
    // `types.SyncLevel`'s enum order). Proves `InlineDensePublication`
    // engages for the simple single-vector insert case: the vector is
    // queryable immediately after ack, and the background worker's later
    // catch-up does not duplicate it.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    try handle.db.addIndex(.{
        .name = "dv_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"marker\":\"seed\"}" }}, .sync_level = .write });
    try handle.db.runUntilIdle();

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    // Deterministic settle: see "WS4-for-graph"'s identical note above.
    try store.sync();
    const before_sequence = store.file.activeCheckpoint().commit_sequence;

    try handle.db.batch(.{ .writes = &.{
        .{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker\",\"_embeddings\":{\"dv_idx\":[1.0,0.0]}}" },
    }, .sync_level = .full_index });

    const after_ack_sequence = store.file.activeCheckpoint().commit_sequence;
    const commits_for_this_write = after_ack_sequence - before_sequence;
    // The inline publication itself still contributes no extra commit
    // beyond the primary write's own (its HBC namespace mutations and the
    // durable doc_key<->vector_id mapping both ride inside the SAME native
    // transaction via `commit_participant.View.putIndexCatalog`/`.put`).
    // The bound used to be much wider (14) to absorb two separate sources
    // of inflation that were never this write's own cost, both fixed by
    // perf/lwa-commit-eff2 -- see "WS4-for-graph"'s identical note for the
    // full explanation: (a) `apply_state.contributeProgressTxn` no longer
    // writes (or commits) a progress-v2 record that an inline publication
    // already wrote identically; (b) the `.full_index` visibility wait's
    // forced-persist pass now flushes every consumer needing it in one
    // call (`persistAppliedSequencesBatchAsync`) instead of one call per
    // consumer. What is left (observed 5, stable across repeated runs) is
    // this test harness's own extra managed index (`body_text`, always
    // added by `openDbWithIo`) needing its own forced-visibility pass,
    // plus genuinely concurrent background activity -- not a per-write
    // cost this single vector insert pays on its own. 7 is comfortably
    // above the observed 5, not tight to it.
    try std.testing.expect(commits_for_this_write <= 7);

    {
        var result = try handle.db.search(allocator, .{
            .index_name = "dv_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1.0, 0.0 }, .k = 1 } },
            .limit = 1,
            .search_effort = 1.0,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("dv-doc", result.hits[0].id);
    }

    // Let the background worker observe this write. If it were not told
    // the vector already landed inline (`Executor.
    // advanceAppliedSequenceIfContiguous`), it would redundantly re-apply
    // the same write from the journal -- for an exact-member upsert that
    // would not duplicate the member, but it would mean the worker, not
    // the inline path, is actually doing the work, defeating the point.
    try handle.db.runUntilIdle();
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "dv_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1.0, 0.0 }, .k = 1 } },
            .limit = 1,
            .search_effort = 1.0,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }
}

test "WS4-for-dense round 2: an update or a delete falls back and still lands correctly" {
    // `prepareInlineDensePublication`'s own batch-level gate in
    // `mutation_commit.zig` (`overwritten_doc_keys.len == 0`/
    // `deleted_keys.len == 0`) excludes a document replacement or a
    // delete from the inline fast path -- both must fall back to the
    // existing `applyDenseEmbeddingWritesByNameWithOptions` path, which
    // this proves still produces the correct final state (the replaced
    // vector is gone, the new one is findable; a deleted vector is gone
    // too) and costs strictly more than the single-commit insert case
    // above, confirming the fallback -- not the inline path -- actually
    // ran.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    try handle.db.addIndex(.{
        .name = "dv_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;

    try handle.db.batch(.{ .writes = &.{
        .{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker-v1\",\"_embeddings\":{\"dv_idx\":[1.0,0.0]}}" },
    }, .sync_level = .full_index });

    // Update: a second write to the SAME key with a different vector.
    // `overwritten_doc_keys` is non-empty for this write, so it must NOT
    // take the one-commit inline path.
    const before_update_sequence = store.file.activeCheckpoint().commit_sequence;
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker-v2\",\"_embeddings\":{\"dv_idx\":[0.0,1.0]}}" },
    }, .sync_level = .full_index });
    const after_update_sequence = store.file.activeCheckpoint().commit_sequence;
    try std.testing.expect(after_update_sequence - before_update_sequence > 1);

    // An exhaustive search (`search_effort: 1.0`) must prove the fallback's
    // functional result too (old vector gone, new one findable): HBC's
    // `CompleteCoverageTracker` used to trip over a stale `native_catalog_
    // backend` mirror left by a PRIOR inline-published write into this same
    // fresh index (`HBCIndex.installStagedWrite` never called
    // `applyExternallyCommittedMutations`, so the mirror never learned about
    // mutations that only ever landed through the primary write's own
    // native commit) -- fixed directly in `installStagedWrite`.
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "dv_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 1.0 }, .k = 1 } },
            .limit = 1,
            .search_effort = 1.0,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("dv-doc", result.hits[0].id);
    }

    // Delete: the dense-vector mapping and HBC posting entry must both be
    // gone.
    try handle.db.batch(.{ .deletes = &.{"dv-doc"}, .sync_level = .full_index });
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "dv_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 1.0 }, .k = 1 } },
            .limit = 1,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 0), result.hits.len);
    }

    // A fresh insert after the delete must still take the (now
    // unobstructed) inline fast path and cost exactly one commit, proving
    // the update/delete fallback above left the index in a state the
    // inline path can resume from cleanly.
    try handle.db.runUntilIdle();
    // Deterministic settle: see "WS4-for-graph"'s identical note above.
    try store.sync();
    const before_final_sequence = store.file.activeCheckpoint().commit_sequence;
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "dv-doc-2", .value = "{\"marker\":\"dv-doc-marker-v3\",\"_embeddings\":{\"dv_idx\":[0.5,0.5]}}" },
    }, .sync_level = .full_index });
    const after_final_sequence = store.file.activeCheckpoint().commit_sequence;
    // See "WS4-for-dense"'s identical note on why this is wider than a
    // single-digit claim. This scenario's longer history (an update, a
    // delete, and the runUntilIdle()s around each) leaves more ride-along
    // backlog for this final write's leadership turn to sweep in than the
    // simpler single-insert "WS4-for-dense" test above, hence the higher
    // bound: 24 is comfortably above the observed 19-20 (two runs).
    try std.testing.expect(after_final_sequence - before_final_sequence <= 24);
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "dv_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 0.5, 0.5 }, .k = 1 } },
            .limit = 1,
            .search_effort = 1.0,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("dv-doc-2", result.hits[0].id);
    }
}

fn getForTest(allocator: Allocator, store: anytype, key: []const u8) !?[]u8 {
    var txn = try store.beginRead();
    defer txn.abort();
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try allocator.dupe(u8, raw);
}

fn putForTest(store: anytype, key: []const u8, value: []const u8) !void {
    var txn = try store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, value);
    try txn.commit();
}

test "WS4-for-dense repair-issue ride-along: a pre-existing embedding-artifact repair issue is cleared by a write that would otherwise take the inline fast path" {
    // lite-single-commit-write-path.md §3.3a's repair-issue follow-up:
    // `prepareInlineDensePublication` now checks
    // `inlineWriteHasPendingEmbeddingRepairIssue` before committing to the
    // fast path, so a write whose artifact_key has an outstanding
    // embedding-artifact repair issue falls back to the slow path, which
    // clears the issue correctly, exactly as it always has -- instead of
    // leaving a stale issue open forever because a later write happened to
    // qualify for the fast path.
    //
    // Scope note: this does not (yet) assert inline-vs-slow-path derived-
    // coverage COUNTER parity end to end. `InlineDensePublication`'s own
    // `coverage_plan` (`prepareInlineCoveragePlan`) is applied inside
    // `stage()`, through the SAME `commit_participant` mechanism every
    // other inline publication (text/graph/sparse) uses to ride along in
    // the primary write's transaction -- and that mechanism's outer
    // wrapper, `ArtifactInputCapture.stage()`
    // (storage/artifact_input_capture.zig:168-170), unconditionally
    // returns before invoking the attached participant whenever
    // `self.documents.count() == 0 and self.artifacts.count() == 0`, which
    // is ALWAYS true for a standalone Lite database with no "online
    // source" replication authority configured: `publication.authority`
    // (artifact_publication.zig:41-47) returns null, and `Capture.touch`
    // -- the only populator of those counts -- bails before registering
    // anything once that's null. That means NONE of the commit-
    // participant-mediated inline effects (not just `coverage_plan`, but
    // every inline publication's own `view.putIndexCatalog`/
    // `contributeProgressTxn` calls too) are ever durably committed for a
    // plain standalone Lite database today. Confirmed empirically and
    // independently of this change (reproduces identically with
    // `coverage_plan` forced to `null`): an inline-published dense vector
    // is searchable in-process (the HBC node cache and, since the
    // `installStagedWrite` mirror-sync fix above, the native_catalog_
    // backend mirror both reflect it already) but fails an exhaustive
    // search with `error.IndexRebuilding` immediately after a close+
    // reopen, which rebuilds those from what is actually durable. This is
    // a separate, pre-existing bug, not introduced here, and it blocks
    // verifying coverage-counter parity for the INLINE side until fixed --
    // flagging for the agent already tracking the related
    // commitRideAlong-vs-stageRideAlong issue.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    try handle.db.addIndex(.{
        .name = "dv_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"marker\":\"seed\"}" }}, .sync_level = .write });
    try handle.db.runUntilIdle();

    const internal_keys = @import("../internal_keys.zig");
    const repair_types = @import("../db/types.zig");
    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;

    // --- Seed a repair issue for a NOT-YET-WRITTEN doc's artifact, as a
    // prior failed write (e.g. a missing artifact) would have recorded it
    // (`recordEmbeddingArtifactRepairIssueContext`). A superseding write
    // to that same doc_key, through the inline-ELIGIBLE path otherwise,
    // must still notice the issue, fall back, and let the slow path clear
    // it -- never leave it stuck open because the write took the fast
    // path.
    const issue_doc_key = "dv-issue-doc";
    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(allocator, issue_doc_key, "dv_idx");
    defer allocator.free(artifact_key);
    // Matches `bytesToHexAlloc`'s own lowercase, unseparated encoding.
    const artifact_key_hex = try allocator.alloc(u8, artifact_key.len * 2);
    defer allocator.free(artifact_key_hex);
    for (artifact_key, 0..) |byte, idx| {
        artifact_key_hex[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        artifact_key_hex[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(allocator, "dv_idx", "embedding", artifact_key_hex);
    defer allocator.free(issue_key);
    {
        const issue = repair_types.ArtifactRepairIssue{
            .artifact_kind = .embedding,
            .index_name = "dv_idx",
            .doc_key = issue_doc_key,
            .artifact_key = artifact_key,
            .sequence = 1,
            .reason = .missing_artifact,
            .first_seen_ns = 1,
            .last_seen_ns = 1,
        };
        const issue_json = try std.json.Stringify.valueAlloc(allocator, issue, .{ .emit_null_optional_fields = false });
        defer allocator.free(issue_json);
        try putForTest(store, issue_key, issue_json);
    }

    const before_issue_sequence = store.file.activeCheckpoint().commit_sequence;
    try handle.db.batch(.{ .writes = &.{
        .{ .key = issue_doc_key, .value = "{\"marker\":\"dv-issue-doc-marker\",\"_embeddings\":{\"dv_idx\":[0.3,0.3]}}" },
    }, .sync_level = .full_index });
    const after_issue_sequence = store.file.activeCheckpoint().commit_sequence;
    // The outstanding issue makes this write ineligible for the inline
    // fast path, so it costs strictly more than the single-commit inline
    // case would -- confirming the fallback, not the inline path, ran.
    try std.testing.expect(after_issue_sequence - before_issue_sequence > 1);
    try std.testing.expectEqual(@as(?[]u8, null), try getForTest(allocator, store, issue_key));

    // The cleared issue stays cleared across a reopen: the slow path's own
    // `clearPublishedArtifactRepairIssueContext` writes directly through
    // `store.putBatchWithReplayWithOptions`, not through
    // `commit_participant`, so it is unaffected by the gap described above.
    try handle.db.runUntilIdle();
    handle.close();
    handle = try openDbWithIo(allocator, path, std.testing.io, false);
    const store2 = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    try std.testing.expectEqual(@as(?[]u8, null), try getForTest(allocator, store2, issue_key));
}

test "WS4 phase 2 round 2: a worker-backlogged index falls back correctly instead of skipping the gap" {
    // Deliberately creates the one scenario the inline fast path must
    // refuse: a background worker still owes this index an earlier
    // write's effects (one that never required `.full_text` visibility,
    // so nothing but the worker's own catch-up will ever apply it) when a
    // later, synchronous `.full_text` write to the SAME index arrives.
    //
    // `std.testing.io` schedules the background executor cooperatively:
    // nothing it drives gets a turn unless this test explicitly yields
    // (`runUntilIdle`, or a wait loop's own `io.sleep`). The `.write`-level
    // batch below is therefore guaranteed to still be unapplied backlog by
    // the time the very next statement -- the `.full_text` batch -- runs
    // its own inline-eligibility pre-check.
    //
    // If the pre-check wrongly ignored that backlog and took the fast
    // path anyway, it would durably advance this index's progress past
    // the backlogged document's sequence, and nothing would ever revisit
    // it: `old-doc` would never become searchable. Instead, the `.full_text`
    // write must fall back to the existing slow path, which forces the
    // worker to catch up the backlog (and its own write) before the ack
    // returns.
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    // A write-level batch leaves this index's own effects as backlog: no
    // caller is waiting on `.full_text`/`.full_index` visibility, so only
    // the (not yet scheduled) background worker will ever apply it.
    try handle.db.batch(.{ .writes = &.{.{ .key = "old-doc", .value = "{\"body\":\"aardvark lives here\"}" }}, .sync_level = .write });

    // Immediately -- no `runUntilIdle` in between -- a synchronous
    // full_text write to the same index. The inline pre-check must see
    // the worker still behind on "old-doc" and fall back.
    try handle.db.batch(.{ .writes = &.{.{ .key = "new-doc", .value = "{\"body\":\"zebra arrived later\"}" }}, .sync_level = .full_text });

    // The fallback must have forced the worker to catch both documents up
    // before this ack returned: both are searchable right away, with no
    // duplication.
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "aardvark" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "zebra" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }

    // Draining any remaining background activity must not change the
    // picture (no double-apply once the worker actually gets a turn).
    try handle.db.runUntilIdle();
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "aardvark" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "zebra" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    }

    // A subsequent, ordinary full_text write still gets the inline fast
    // path once the backlog has drained (self-healing, not permanently
    // disabled by the earlier fallback).
    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    // Deterministic settle: see "WS4-for-graph"'s identical note above.
    try store.sync();
    const before_sequence = store.file.activeCheckpoint().commit_sequence;
    try handle.db.batch(.{ .writes = &.{.{ .key = "new-doc-2", .value = "{\"body\":\"zebra again\"}" }}, .sync_level = .full_text });
    const after_sequence = store.file.activeCheckpoint().commit_sequence;
    try std.testing.expect(after_sequence - before_sequence <= 6);
    {
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "zebra" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    }
}

// Regression for the bug found alongside the crash sweep below: under
// redo-as-default, `sync_level: .full_text`'s own derived-index catch-up
// pass used to discover "what changed" via a cursor scan
// (`docstore.zig`'s `forEachReplayLaneRaw` -> `Txn.openCursor()`) that
// merged only a write transaction's own in-flight pending writes, never
// `NativeFile`'s redo-log overlay -- so a commit logged but not yet
// checkpointed was invisible to it, even on the very same, uncrashed
// handle, with no fault injection or reopen involved at all. Root-caused
// via exactly this reproduction: it failed roughly 35/40 iterations under
// redo-as-default before the fix (`docstore.Cursor`'s three-way merge,
// `pending_tree` over the redo overlay over the B-tree), and 0/40 under
// `use_redo_log: false` throughout -- confirming this is not actually a
// crash-recovery bug, just one that the crash sweep below also happened
// to surface.
test "lite full text synchronous visibility is searchable immediately after an uncrashed commit" {
    const allocator = std.testing.allocator;
    const iterations = 20;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testAflitePath(allocator, tmp);
        defer allocator.free(path);
        var handle = try openDbWithIo(allocator, path, std.testing.io, true);
        defer handle.close();
        try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
        try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
        try assertSearchable(&handle, allocator, "body_text", "body", "ft-doc", "needle");
    }
}

// Same shape as the full-text test above, under `use_redo_log: false`
// (legacy) -- the control. Must also pass: the bug this guards against
// is specific to redo, not something this test's own write pattern
// happens to trip regardless of storage mode.
test "lite full text synchronous visibility is searchable immediately after an uncrashed commit LEGACY control" {
    const allocator = std.testing.allocator;
    const iterations = 20;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testAflitePath(allocator, tmp);
        defer allocator.free(path);
        // Opening through `openLegacyDbWithIo` (not inlined here) is the
        // point, not incidental: an `errdefer lite_backend.deinit()` /
        // `errdefer db.close()` pair declared in *this* function would stay
        // armed for the rest of the loop body below, including past the
        // point where `handle = .{ .backend = lite_backend, .db = db }`
        // copies their values and `defer handle.close()` becomes the real
        // owner -- any later failure (`batch`/`assertSearchable`) would
        // then double-close both `db` and `lite_backend` on top of
        // `handle`'s own close. `openLegacyDbWithIo`'s errdefers are scoped
        // to its own body, which returns before any of that can happen
        // (see its doc comment, and `ioFaultThenDoubleCloseScenarioLegacy`
        // below for the shape this avoids).
        var handle = try openLegacyDbWithIo(allocator, path, std.testing.io);
        defer handle.close();
        try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
        try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
        try assertSearchable(&handle, allocator, "body_text", "body", "ft-doc", "needle");
    }
}

// Dense-vector variant of the two full-text tests above: the same
// "write, then immediately require derived-index visibility, no crash"
// shape, but through `sync_level: .full_index` and a dense-vector index
// instead -- `waitForSyncLevelWithPublicationCut`'s `.full_index` branch
// is a different code path from `.full_text`'s, so this is independent
// coverage of the same bug class (any derived-index catch-up pass that
// discovers "what changed" via a cursor scan over the primary store),
// not a restatement of the full-text case. No dedicated vector-query
// assertion exists in this harness (see `runDenseVectorAddIndexMidScenario`'s
// own doc comment); like that scenario, this checks the primary
// document's own marker field survives an immediate, uncrashed reopen.
test "lite dense vector full index visibility is readable immediately after an uncrashed commit" {
    const allocator = std.testing.allocator;
    const iterations = 20;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testAflitePath(allocator, tmp);
        defer allocator.free(path);
        var handle = try openDbWithIo(allocator, path, std.testing.io, true);
        defer handle.close();
        try handle.db.addIndex(.{
            .name = "dv_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try handle.db.batch(.{ .writes = &.{.{ .key = "dv-doc", .value = "{\"marker\":\"dv-doc-marker\",\"_embeddings\":{\"dv_idx\":[1.0,0.0]}}" }}, .sync_level = .full_index });
        const read = (try handle.db.get(allocator, "dv-doc")) orelse return error.TestExpectedDocumentFound;
        defer allocator.free(read);
        try std.testing.expect(std.mem.indexOf(u8, read, "dv-doc-marker") != null);
    }
}

// Regression for the §9.7c ReadView design doc section: a document plus
// its full-text entry, written without ever calling checkpointRedoLog,
// must be found by derived catch-up through the real catch-up API
// (sync_level: .full_text's own wait path) both on the live handle that
// wrote it and, independently, on a fresh handle after a clean close and
// reopen -- exercising "recovery replays into the overlay before any
// reader starts" (§9.5/recoverRedoLog) feeding into the exact same
// catch-up path the live-handle half of this test already covers.
test "lite derived catch-up sees a document and its full-text entry written without a checkpoint, before and after a crash and reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    // Before: write on a live handle, no checkpoint, assert immediately.
    {
        var handle = try openDbWithIo(allocator, path, std.testing.io, true);
        defer handle.close();
        try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
        try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
        try assertSearchable(&handle, allocator, "body_text", "body", "ft-doc", "needle");
    }

    // After: a fresh handle, opened from scratch. recoverRedoLog must
    // have replayed the above into the shared overlay before this handle
    // is usable at all; the first thing this does with it is run the
    // exact same catch-up path again (via assertSearchable's own
    // runUntilIdle/forceCompactTextIndexes retry loop), on a derived
    // index this new handle has never touched before.
    {
        var handle = try openDbWithIo(allocator, path, std.testing.io, false);
        defer handle.close();
        try assertSearchable(&handle, allocator, "body_text", "body", "ft-doc", "needle");
    }
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

test "crash sweep: full text with retirement turn" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "full-text-with-retirement",
        .run = runFullTextWithRetirement,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
        .retirement = true,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

// Regression coverage for the actual mechanism behind "retirement makes
// full-text documents unsearchable after reopen": a minimal, non-crash
// reproduction of `runFullTextWithRetirement` above, outside the sweep
// harness, so a failure here points straight at the bug instead of a
// crash-barrier index.
//
// Root cause (confirmed by deleting pieces of this scenario one at a time
// until the failure flipped): nothing to do with retirement at all.
// `ArtifactInputCapture.stage` (storage/artifact_input_capture.zig) used
// to skip its attached `commit_participant` whenever this transaction
// touched no replicated document/artifact ("documents.count() == 0 and
// artifacts.count() == 0" -- the ordinary case for every write on a
// standalone Lite store with no online-source replication authority).
// WS4 phase 2's inline text/dense/sparse/graph publication
// (`InlineTextPublication.stage` in mutation_materialization.zig) attaches
// itself through that exact same slot purely to run once inside the host
// transaction before commit -- it ignores the replication-position bytes
// entirely and has nothing to do with document/artifact revision
// tracking. Skipping it silently dropped every inline segment/seal/
// progress mutation a write staged: the write still acknowledged and
// stayed searchable in-session (the in-memory projection is installed
// regardless of whether the durable catalog write happened), but nothing
// durable ever reached the native transaction, so the effect vanished on
// the next reopen.
//
// A fresh index's very first write still happens to survive: index
// activation's own cold-start catch-up commits through the older,
// several-commits-per-write path, which never goes through this capture
// at all. Every write after that takes the broken inline fast path, which
// is why `runFullTextWithRetirement`'s *second* and *third* writes were
// the ones that came back unsearchable -- the retirement turn in between
// them never touched the lost data; it was already gone the moment the
// write it belonged to committed.
test "regression: a second synchronous full_text write survives close+reopen without any retirement call" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    {
        var handle = try openDbWithIo(allocator, path, std.testing.io, true);
        defer handle.close();
        inline for (retirement_docs[0..2]) |doc| {
            const json = try std.fmt.allocPrint(allocator, "{{\"body\":\"{s}\"}}", .{doc.body});
            defer allocator.free(json);
            try handle.db.batch(.{ .writes = &.{.{ .key = doc.key, .value = json }}, .sync_level = .full_text });
        }
    }
    {
        var handle = try openDbWithIo(allocator, path, std.testing.io, false);
        defer handle.close();
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "retirement" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    }
}

// The exact scenario `runFullTextWithRetirement`'s crash sweep drives
// (three small full-text writes, one owner retirement turn, close,
// reopen), run once without any crash injection. Kept alongside the
// simpler regression above because this is the shape the bug was first
// reported in, and because it exercises `runRetirementTurn` itself (which
// the simpler test above never calls) staying a true no-op end to end.
test "regression: full-text writes with an intervening retirement turn survive close+reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    {
        var handle = try openDbWithIo(allocator, path, std.testing.io, true);
        defer handle.close();
        inline for (retirement_docs) |doc| {
            const json = try std.fmt.allocPrint(allocator, "{{\"body\":\"{s}\"}}", .{doc.body});
            defer allocator.free(json);
            try handle.db.batch(.{ .writes = &.{.{ .key = doc.key, .value = json }}, .sync_level = .full_text });
        }
        try runRetirementTurn(allocator, &handle);
    }

    {
        var handle = try openDbWithIo(allocator, path, std.testing.io, false);
        defer handle.close();
        var result = try handle.db.search(allocator, .{
            .index_name = "body_text",
            .full_text = .{ .match = .{ .field = "body", .text = "retirement" } },
            .limit = 10,
        });
        defer result.deinit();
        var seen: [retirement_docs.len]bool = @splat(false);
        for (result.hits) |hit| {
            inline for (retirement_docs, 0..) |doc, i| {
                if (std.mem.eql(u8, hit.id, doc.key)) seen[i] = true;
            }
        }
        inline for (retirement_docs, 0..) |doc, i| {
            if (!seen[i]) {
                std.debug.print("regression: acknowledged document not searchable after reopen key={s}\n", .{doc.key});
                return error.CrashConsistencyFullTextNotRebuilt;
            }
        }
    }
}

test "crash sweep: full text with non-durable commit" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "full-text-with-non-durable-commit",
        .run = runFullTextWithNonDurableCommit,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: dense vector add-index mid-scenario" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "dense-vector-add-index-mid-scenario",
        .run = runDenseVectorAddIndexMidScenario,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: dense vector update and delete" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "dense-vector-update-delete",
        .run = runDenseVectorUpdateDeleteScenario,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: dense native-catalog dimension replacement delete (#1216)" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "dense-native-catalog-dimension-delete",
        .run = runDenseNativeCatalogDimensionDeleteScenario,
        .custom_check = checkDenseNativeCatalogDimensionDeleteInvariant,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: dense native-catalog dimension replacement admit and delete (#1216)" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "dense-native-catalog-dimension-admit-and-delete",
        .run = runDenseNativeCatalogDimensionAdmitAndDeleteScenario,
        .custom_check = checkDenseNativeCatalogDimensionAdmitAndDeleteInvariant,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: sparse vector add-index mid-scenario" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "sparse-vector-add-index-mid-scenario",
        .run = runSparseVectorAddIndexMidScenario,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: graph add-index mid-scenario" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "graph-add-index-mid-scenario",
        .run = runGraphAddIndexMidScenario,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep: graph metric build and compaction mid-scenario" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "graph-metric-build-mid-scenario",
        .run = runGraphMetricBuildWithCrashScenario,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: primary single doc" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "primary-single-doc", .run = runPrimarySingleDoc }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: primary batch" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "primary-batch", .run = runPrimaryBatch }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: primary update then delete" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "primary-update-delete", .run = runPrimaryUpdateDelete }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: deterministic concurrent writers" {
    var report = try runCrashSweep(std.testing.allocator, .{ .name = "deterministic-concurrent-writers", .run = runDeterministicConcurrentWriters }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: full text synchronous visibility" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "full-text-sync-visible",
        .run = runFullTextSyncVisible,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

test "crash sweep exhaustive: full text with non-durable commit" {
    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "full-text-with-non-durable-commit",
        .run = runFullTextWithNonDurableCommit,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
    }, .{ .mode = .exhaustive });
    defer report.deinit();
    try reportFailures(&report);
}

// Regression test for a bug-1 finding: `SimIo.vtFileSync` used to capture a
// full durable-rollback snapshot (an O(file-size) read, done while holding
// `SimIo.mutex` for the whole call) on *every* sync, even in a run with no
// crash plan configured at all -- including every scenario's discovery
// pass, which can never call `applyPowerLoss` and so never needed that
// snapshot. That gratuitous, lock-amplified overhead ran on every thread's
// every sync, including a full-text index's background derived-worker
// catch-up (spawned via `io.concurrent(...)` by `.full_text` sync-level
// writes), serializing and slowing it far more than the real `std.Io` ever
// would. In this repo's build that reproducibly got the "full text
// synchronous visibility" scenario's discovery pass stuck: its post-write
// reopen saw `error.IndexRebuilding` forever (never recovered even after 50
// rounds of `runUntilIdle`/`forceCompactTextIndexes`/
// `drainScheduledTextMerges`), while the identical scenario run against the
// real, unwrapped `std.Io` (no `SimIo` at all) always passed immediately --
// proving the bug lived in the shim, not the engine. Run several times
// (the race was intermittent) to guard against the overhead regressing.
test "crash sweep: full text sync visibility discovery is not flaky under the shim" {
    var attempt: usize = 0;
    while (attempt < 10) : (attempt += 1) {
        // `quick_sample = 0` runs only the discovery pass (no crash-replay
        // barriers), which is where the bug manifested and all this test
        // needs to exercise.
        var report = try runCrashSweep(std.testing.allocator, .{
            .name = "full-text-sync-visible",
            .run = runFullTextSyncVisible,
            .via_db = true,
            .search_index = "body_text",
            .search_field = "body",
        }, .{ .quick_sample = 0 });
        defer report.deinit();
        try reportFailures(&report);
    }
}

// Deterministic regression test for the bug this file's "discovery is not
// flaky under the shim" test above found by accident: a single transient
// write/sync/create error during a `.full_text` commit -- no crash at all,
// `sim.crashed` never becomes true -- must not leave `body_text`
// `IndexRebuilding` forever. Before the fix (native.zig's
// `NativeFile.reconcilePublicationUncertainty`, called from
// `beginTransaction`; `catch_up_policy.isTransientIoError`, which keeps the
// threaded derived worker retrying instead of latching `last_error_name`;
// and `db.zig`'s `runUntilIdleWithReplayDrainOptions` draining a pending
// managed-admission materialization) this reproduced exactly the trail
// real heavy-load runs showed: `index catalog admission committed with
// pending activation ... err=InputOutput`/`OutcomeUnknown`, then
// `search()` never recovering from `IndexRebuilding`.
//
// Swept over every injection kind `crash_sim_io.zig` supports and a range
// of op-indices rather than one hand-picked point, so the test does not
// depend on knowing exactly which op a real disk hiccup happens to land
// on -- matching this file's own `exhaustive` crash sweep's philosophy,
// applied to I/O faults instead of crashes.
/// Runs the "seed, then ft-doc" scenario the sweep below drives, once, with
/// an unreachable `at_op` (`maxInt`) armed for `kind` so the fault never
/// fires, then reads back exactly how many `kind` calls the `ft-doc` write
/// alone made (via `SimIo.ioFaultOpCount`, which counts from the moment
/// `setIoFaultPlan` installs the plan). This discovery pass is what lets
/// the real sweep below bound `at_op` to calls the write itself makes,
/// instead of a hand-picked constant that can overshoot past the write
/// into whatever its caller does next.
fn discoverFaultOpCount(allocator: Allocator, kind: sim_io.IoFaultKind) !u64 {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    const sim = try sim_io.SimIo.init(allocator, std.testing.io);
    defer sim.deinit();

    var handle = try openDbWithIo(allocator, path, sim.io(), true);
    errdefer handle.close();
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
    try handle.db.runUntilIdle();

    sim.setIoFaultPlan(.{ .at_op = std.math.maxInt(u64), .kind = kind, .err = error.InputOutput });
    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
    const count = sim.ioFaultOpCount(kind);
    sim.setIoFaultPlan(null);
    handle.close();
    return count;
}

test "crash sweep: full text recovers from one transient io fault with no crash" {
    const allocator = std.testing.allocator;
    const kinds = [_]sim_io.IoFaultKind{ .write, .sync, .create };
    for (kinds) |kind| {
        const op_count = try discoverFaultOpCount(allocator, kind);
        var at_op: u64 = 0;
        while (at_op < op_count) : (at_op += 1) {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try testAflitePath(allocator, tmp);
            defer allocator.free(path);

            const sim = try sim_io.SimIo.init(allocator, std.testing.io);
            defer sim.deinit();

            // Create the database and its full-text index, then commit the
            // seed document, before arming the fault: this test is about
            // recovering from a fault during the *second* write, not about
            // surviving one during setup (crash_consistency_test's ordinary
            // crash sweep for this scenario already covers every op,
            // setup included).
            var handle = try openDbWithIo(allocator, path, sim.io(), true);
            errdefer handle.close();
            try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });
            // The seed write's `sync_level = .write` returns as soon as the
            // write itself is durable, *not* once `body_text`'s background
            // catch-up has indexed it -- that indexing keeps running
            // concurrently via `io.concurrent(...)` afterwards. Drain it
            // before arming the fault (same reasoning as `openDbWithIo`'s
            // own drain after `open`/`addIndex`, just above): without this,
            // `at_op` counts whichever call happens to be in flight when the
            // plan is installed, which can be the seed's own still-running
            // catch-up rather than the `ft-doc` write this test is actually
            // targeting -- the fault then lands on unrelated setup work
            // instead of the operation under test, and the one real,
            // one-shot fault's effect surfaces somewhere this test never
            // retries.
            try handle.db.runUntilIdle();

            sim.setIoFaultPlan(.{
                .at_op = at_op,
                .kind = kind,
                .err = if (kind == .create) error.NoSpaceLeft else error.InputOutput,
            });
            handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text }) catch {
                // The one-shot fault already fired and cleared itself
                // (`IoFaultPlan.repeat` defaults to false): an ordinary
                // application retry of the exact same write must now land
                // cleanly, proving (a) the transient error surfaced as
                // something retryable rather than wedging the owner. Give
                // the owner's own handling of the failed attempt one
                // `runUntilIdle` turn to settle first, instead of racing
                // it with an immediate retry.
                handle.db.runUntilIdle() catch {};
                try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
            };
            // `at_op < op_count` guarantees the fault fired (and, being
            // one-shot, already cleared itself) somewhere within the
            // `ft-doc` write's own calls above -- but disarm explicitly
            // anyway, defense in depth: a leftover armed plan must never
            // reach `close()`'s own best-effort, unretried syncs
            // (`core.syncStore`, the index status snapshot), which this
            // test has no retry path for at all.
            sim.setIoFaultPlan(null);
            handle.close();

            // Reopen with a plain, unwrapped `std.Io` -- no `SimIo`, no
            // fault -- and confirm the owner's own background catch-up
            // resumes and the document becomes searchable without this
            // reopen being the only possible recovery path: proving (b)
            // the index never stays `IndexRebuilding` forever once the
            // underlying I/O is healthy again.
            var reopened = openDbWithIo(allocator, path, std.testing.io, false) catch |err| {
                std.debug.print("io-fault regression: reopen itself failed kind={s} at_op={d}: {s}\n", .{ @tagName(kind), at_op, @errorName(err) });
                return err;
            };
            defer reopened.close();
            assertSearchable(&reopened, allocator, "body_text", "body", "ft-doc", "needle") catch |err| {
                std.debug.print("io-fault regression failed kind={s} at_op={d}: {s}\n", .{ @tagName(kind), at_op, @errorName(err) });
                return err;
            };
        }
    }
}

// Deterministic regression test for a use-after-free this file's own
// io-fault sweep above found by accident: `backend.Handle.deinit` (and
// `bridge.ContainerStorage.deinit`, which it can reach) ended every call
// with `self.* = undefined` and had no guard against running twice. A
// caller that explicitly closes a handle mid-scope while an
// `errdefer`/`defer` registered earlier in that same scope is still armed
// -- exactly `DbHandle`'s own shape just above, and an easy one for any
// caller to fall into -- invokes `deinit` a second time the instant
// anything *later* in that scope returns an error. The second call read
// `self.engine` and every other field out of memory the first call had
// already poisoned, so the switch in `deinit` could (and did) take the
// wrong branch entirely: observed as a segmentation fault inside
// `bridge.zig`'s hashmap iterator, reached through the
// `.bridge_lsm_container` arm despite the handle actually being
// `.native_single_file` the whole time. Fixed by a `closed` flag checked
// before anything else in `deinit`, set only after `self.* = undefined`.
//
// Reproduces the exact shape deterministically -- one transient fault
// during a write, an explicit `close()`, then a *manufactured* error
// propagated out of the scope -- rather than depending on some later,
// unrelated call (the full sweep above relies on `assertSearchable`) to
// happen to fail on whichever (kind, at_op) combination this file's own
// evolution leaves exercising the bug.
fn ioFaultThenDoubleCloseScenario(allocator: Allocator, path: []const u8, sim: *sim_io.SimIo) !void {
    var handle = try openDbWithIo(allocator, path, sim.io(), true);
    errdefer handle.close();
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });

    sim.setIoFaultPlan(.{ .at_op = 0, .kind = .write, .err = error.InputOutput });
    handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text }) catch {
        try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
    };

    // Matches the crash's exact lifecycle: an explicit close while the
    // `errdefer` above is still armed...
    handle.close();
    // ...then an error, from anything at all, propagated out of this
    // scope -- deterministically forcing that still-armed `errdefer` to
    // run `handle.close()` a second time on the now-closed handle. Before
    // the `closed` guard, this alone -- no actual second failure required
    // -- was enough to segfault.
    return error.DeliberateDoubleCloseTrigger;
}

test "backend handle survives a transient io fault followed by a double close" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    const sim = try sim_io.SimIo.init(allocator, std.testing.io);
    defer sim.deinit();

    // The scenario always returns `error.DeliberateDoubleCloseTrigger` by
    // construction (see above): what this test actually checks is that
    // reaching it -- and so the double `deinit` -- does not crash or
    // corrupt memory. Any other error means the scenario itself broke
    // before it got there.
    try std.testing.expectError(error.DeliberateDoubleCloseTrigger, ioFaultThenDoubleCloseScenario(allocator, path, sim));
}

// Same shape, under `use_redo_log: false` (legacy) -- the control. Must
// also pass: `Handle.deinit`'s missing re-entry guard is a plain lifecycle
// bug in `backend.zig`, with nothing in its own crash path touching
// `self.redo` or anything else redo-specific, so nothing about this
// scenario should depend on which storage mode created the handle.
//
// Opening is factored into its own function, exactly like `openDbWithIo`,
// rather than inlined into the scenario below: an `errdefer db.close()`
// declared before `db`'s value is moved into a `DbHandle` stays armed for
// the rest of whatever function it was declared in, and this scenario
// deliberately returns an error at the end to trigger the handle's own
// stale errdefer (see `ioFaultThenDoubleCloseScenario` above) -- which
// would, in the same breath, also re-trigger *this* one on the
// independent, still-"open" `db` copy it was declared against, double
// freeing `DB`'s own internals instead of (or in addition to) the
// `backend.Handle` bug this test exists to cover. `openDbWithIo`'s own
// identical-looking `errdefer db.close()` never hits this because nothing
// after it in that function can fail.
fn openLegacyDbWithIo(allocator: Allocator, path: []const u8, io: std.Io) !DbHandle {
    var lite_backend = try backend.Handle.createWithOptions(allocator, path, .{ .exclusive = true, .io = io, .use_redo_log = false });
    errdefer lite_backend.deinit();
    const identity = connection.identityOpenOptions(true);
    var db_opts = db_mod.OpenOptions{
        .open_mode = .writer,
        .identity_namespace = identity.identity_namespace,
        .prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace,
    };
    try lite_backend.configureDbOpenOptions(&db_opts);
    var db = try db_mod.DB.open(allocator, path, db_opts);
    errdefer db.close();
    try db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
    try db.runUntilIdle();
    return .{ .backend = lite_backend, .db = db };
}

fn ioFaultThenDoubleCloseScenarioLegacy(allocator: Allocator, path: []const u8, sim: *sim_io.SimIo) !void {
    var handle = try openLegacyDbWithIo(allocator, path, sim.io());
    errdefer handle.close();
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });

    sim.setIoFaultPlan(.{ .at_op = 0, .kind = .write, .err = error.InputOutput });
    handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text }) catch {
        try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
    };

    handle.close();
    return error.DeliberateDoubleCloseTrigger;
}

test "backend handle survives a transient io fault followed by a double close LEGACY control" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    const sim = try sim_io.SimIo.init(allocator, std.testing.io);
    defer sim.deinit();

    try std.testing.expectError(error.DeliberateDoubleCloseTrigger, ioFaultThenDoubleCloseScenarioLegacy(allocator, path, sim));
}

// Deterministic regression test for a second instance of the same bug class,
// one step earlier in the lifecycle: a transient fault during `db.addIndex`'s
// own *post-commit* bookkeeping (after the catalog admission itself already
// committed durably). `installIndexForReconciliation` (db.zig) always writes
// a managed-admission outbox row as part of that same catalog transaction,
// even for an *ordinary* (non-managed) `addIndex`; when a later bookkeeping
// step (`saveAppliedSequence`/`saveIndexStatusSnapshots`/etc.) then hits a
// transient error, `recoverCommittedIndexAdmission` degrades the whole call
// into "committed, repair pending" and returns success instead of
// propagating -- so `addIndex` itself returns `void` with no error at all.
// That outbox row only ever gets turned into a durable generation-repair
// *intent* (`materializeManagedIndexAdmissionValueLocked` ->
// `createGenerationRepairIntentAtTarget`); nothing then executes that
// intent. In production it is the independent-maintenance owner's `.repair`
// work that does so, but that owner is gated to borrowed-IO runtimes under
// test (`startArtifactRepairMetadataWorkerIfNeeded`) and plenty of
// production embeddings never borrow IO either, so until
// `runUntilIdleWithReplayDrainOptions` also drove
// `repairRecoverableStartupIndexFailures` here, `body_text` stayed
// `markUnscopedRepairUnavailable`-gated (`search()` -> `IndexRebuilding`)
// forever, even though the fault was one-shot and the repair itself
// succeeds instantly once actually attempted.
test "crash sweep: full text addIndex recovers from one transient post-commit fault" {
    const allocator = std.testing.allocator;
    const kinds = [_]sim_io.IoFaultKind{ .write, .sync };
    for (kinds) |kind| {
        var at_op: u64 = 0;
        while (at_op < 40) : (at_op += 1) {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try testAflitePath(allocator, tmp);
            defer allocator.free(path);

            const sim = try sim_io.SimIo.init(allocator, std.testing.io);
            defer sim.deinit();

            // Open a fresh DB without `openDbWithIo`'s own `addIndex("body_
            // text")` call, so the fault below lands on exactly that call
            // and nothing earlier.
            var lite_backend = try backend.Handle.createWithOptions(allocator, path, .{ .exclusive = true, .io = sim.io() });
            var backend_open = true;
            defer if (backend_open) lite_backend.deinit();
            const identity = connection.identityOpenOptions(true);
            var db_opts = db_mod.OpenOptions{
                .open_mode = .writer,
                .identity_namespace = identity.identity_namespace,
                .prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace,
            };
            try lite_backend.configureDbOpenOptions(&db_opts);
            var db = try db_mod.DB.open(allocator, path, db_opts);
            var db_open = true;
            defer if (db_open) db.close();

            sim.setIoFaultPlan(.{ .at_op = at_op, .kind = kind, .err = error.InputOutput });
            // An earlier op can make `addIndex` itself surface the fault
            // (before catalog commit); that is a different, already-covered
            // contract (plain call failure, nothing committed) and not what
            // this test targets. Only the silent-success case -- the fault
            // landing on post-commit bookkeeping, which `addIndex` swallows
            // into a pending repair instead of returning -- exercises the
            // bug this test is a regression for, so skip every op where
            // `addIndex` did not itself return success.
            db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" }) catch {
                sim.setIoFaultPlan(null);
                continue;
            };
            sim.setIoFaultPlan(null);
            try db.runUntilIdle();

            try db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

            var handle = DbHandle{ .backend = lite_backend, .db = db };
            db_open = false;
            backend_open = false;
            defer handle.close();
            assertSearchable(&handle, allocator, "body_text", "body", "ft-doc", "needle") catch |err| {
                std.debug.print("addIndex io-fault regression failed kind={s} at_op={d}: {s}\n", .{ @tagName(kind), at_op, @errorName(err) });
                return err;
            };
        }
    }
}

// Bug repro: a commit that fails *after staging* (an injected, persistent
// `error.InputOutput` on the write op below -- `IoFaultPlan.repeat = true`
// never clears it) must not leak the retained/artifact_inputs/
// document_revisions captures `storage/docstore.zig`'s `DocStore.Txn.commit`
// / `Batch.commitStaged` / `StagedBatch.wait` stage into the batch before
// the native write itself runs. The real bug: `DocStore.StagedDocBatch.wait`
// (the wrapper `mutation_commit.zig`'s `batchInternalPreparedOwned` actually
// calls) unconditionally `defer`-destroyed its heap-boxed `*Batch` without
// ever calling `abort()` on it first when `inner.wait()` failed, so those
// captures' backing allocations (document_mutation_revision.zig's `touch`,
// in particular) became unreachable the moment that memory was freed --
// a leak the crash sweep's `SafeAllocator` catches directly. Separately,
// also bound `close()` on its own thread with a deadline: a short
// `test_visibility_wait_timeout_ms` combined with a persistent fault drives
// the same derived-worker recoverable-retry path `ee89ce70b5` added, and a
// regression there must fail loudly instead of hanging the whole suite.
test "crash sweep: commit failing after staging under a persistent io fault does not leak or hang close" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    const sim = try sim_io.SimIo.init(allocator, std.testing.io);
    defer sim.deinit();

    var handle = try openDbWithIo(allocator, path, sim.io(), true);
    var handle_open = true;
    defer if (handle_open) handle.close();
    try handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"body\":\"seed body\"}" }}, .sync_level = .write });

    // Never clears: every subsequent write-kind op faults, including this
    // next write's own primary commit (not only the derived worker's
    // catch-up) -- exactly "a commit fails after staging" as described.
    sim.setIoFaultPlan(.{ .at_op = 0, .kind = .write, .err = error.InputOutput, .repeat = true });
    db_mod.test_visibility_wait_timeout_ms = 1;
    defer db_mod.test_visibility_wait_timeout_ms = null;

    handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text }) catch |err| switch (err) {
        // With the redo log on, a persistent fault on this commit's own
        // write surfaces as `error.OutcomeUnknown`, not a raw I/O error:
        // `NativeFile.publishCheckpoint`'s redo branch documents that a
        // failed append or sync always reports `OutcomeUnknown` (never the
        // underlying error) so callers treat the commit exactly as
        // uncertain as the legacy two-slot path would (see that
        // function's own "A failed sync (not append...) still reports
        // `OutcomeUnknown`" comment). This test's own assertions -- no
        // leak, no hang -- do not depend on which of the three this
        // particular op/fault combination surfaces as.
        error.CommitVisibilityNotSatisfied, error.InputOutput, error.OutcomeUnknown => {},
        else => return err,
    };

    const Closer = struct {
        handle: *DbHandle,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.handle.close();
            self.done.store(true, .release);
        }
    };
    var closer: Closer = .{ .handle = &handle };
    const thread = try std.Thread.spawn(.{}, Closer.run, .{&closer});
    const platform_time = @import("antfly_platform").time;
    const deadline_ns = platform_time.monotonicNs() +| (20 * std.time.ns_per_s);
    var hang_detected = false;
    while (!closer.done.load(.acquire)) {
        if (!hang_detected and platform_time.monotonicNs() >= deadline_ns) {
            // `close()` is taking far longer than it should under a
            // persistent fault -- that is the regression this test exists
            // to catch, and it is reported below. But this thread still
            // holds a reference to this function's own stack locals
            // (`handle`, `sim`, `tmp`'s backing directory, `path`), all of
            // which this function's own `defer`s would tear down the
            // moment it returns. Previously this detached the thread and
            // returned immediately, which let it keep running against a
            // freed `SimIo` (and a deleted temp directory) for an
            // unbounded time -- exactly the cross-scenario background-task
            // leak `registerSelf` now fails loudly on in every *later*
            // scenario's `SimIo.init`, not this one. Clear the fault
            // instead: the retry loop(s) `close()` is stuck behind back
            // off and retry every `retry_ms`, so clearing it lets the very
            // next attempt succeed, and this loop keeps waiting
            // (unboundedly, but only after the actual cause is gone) so
            // this function never returns while `thread` can still touch
            // its locals.
            hang_detected = true;
            sim.setIoFaultPlan(null);
        }
        std.Thread.yield() catch {};
    }
    thread.join();
    handle_open = false;
    if (hang_detected) return error.ShutdownHangDetected;
}

// Regression for a `DB.close()` hang (perf/lwa-commit-eff2): a worker (the
// derived executor's own catch-up thread, in production) can be blocked in
// `Ticket.wait()` for a ride-along commit it staged -- e.g. via
// `native_catalog_sink.Sink.submitDeferred`/`Store.stageRideAlong` -- with
// nothing else driving the Lite store's group-commit queue to resolve it
// (no foreground write arrives, and the idle-flush timer has not fired
// yet). `deinitWrapperState` used to call `self.executor.beginShutdown()`/
// `.deinit()` -- which joins that worker thread -- before ever reaching
// `self.core.syncStore()`, so a worker stuck like this stranded close()
// behind a wait nothing was left to resolve. Fixed by force-draining the
// store (`self.core.syncStore(true)`, which reaches the Lite store's own
// `sync()` -> `forceDrainDurable()`) immediately before `beginShutdown()`,
// so every currently-queued ride-along ticket -- a worker's own included --
// is already resolved by the time any worker is asked to shut down.
//
// This test stages its own ride-along ticket directly on the DB's
// underlying `docstore.Store` (bypassing the real derived-worker machinery
// for determinism, mirroring docstore.zig's own "closing a store with only
// ride-along work" precedent) and waits on it from a second thread,
// simulating a worker still blocked on its own queued ticket, then calls
// `DB.close()` from a third. `ride_along_idle_ns` is set well beyond this
// test's own deadline -- not just "deliberately long" -- so the idle-flush
// timer firing in time can never be why this passes: only `DB.close()`
// itself resolving the ticket (whether via the force-drain this change adds
// before `beginShutdown()`, or any other barrier already inside it) can.
// Both the close and the ticket wait are bounded by the same deadline,
// mirroring this file's and docstore.zig's own convention, so a regression
// fails loudly instead of wedging the suite.
test "DB.close() does not hang on a ride-along ticket a worker is still waiting on" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    var handle_open = true;
    defer if (handle_open) handle.close();

    const store = handle.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    store.ride_along_idle_ns = 120 * std.time.ns_per_s;

    const RideAlongCtx = struct {
        fn apply(_: *anyopaque, file: *native.NativeFile) !void {
            try file.putDocument("worker-ride-along", "rode along");
        }
    };
    var ride_ctx: u8 = 0;
    const ticket = try store.stageRideAlong(.{ .context = &ride_ctx, .apply = RideAlongCtx.apply });

    // Simulates the derived worker: a thread blocked in `Ticket.wait()` for
    // its own ride-along commit, with nothing else (no foreground write,
    // no idle-timer fire within this test's deadline) yet driving the
    // queue to resolve it.
    const TicketWaiter = struct {
        ticket: docstore.Store.Ticket,
        done: std.atomic.Value(bool) = .init(false),
        result: anyerror!void = {},
        fn run(self: *@This()) void {
            self.result = self.ticket.wait();
            self.done.store(true, .release);
        }
    };
    var ticket_waiter: TicketWaiter = .{ .ticket = ticket };
    const ticket_thread = try std.Thread.spawn(.{}, TicketWaiter.run, .{&ticket_waiter});

    const Closer = struct {
        handle: *DbHandle,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.handle.close();
            self.done.store(true, .release);
        }
    };
    var closer: Closer = .{ .handle = &handle };
    const closer_thread = try std.Thread.spawn(.{}, Closer.run, .{&closer});

    const platform_time = @import("antfly_platform").time;
    const deadline_ns = platform_time.monotonicNs() +| (20 * std.time.ns_per_s);
    while (!closer.done.load(.acquire) or !ticket_waiter.done.load(.acquire)) {
        if (platform_time.monotonicNs() >= deadline_ns) {
            closer_thread.detach();
            ticket_thread.detach();
            return error.CloseHangDetected;
        }
        std.Thread.yield() catch {};
    }
    ticket_thread.join();
    try ticket_waiter.result;
    closer_thread.join();
    handle_open = false;

    // The ride-along write must have actually landed durably, not just
    // resolved its ticket without a real commit behind it.
    var reopened = try openDbWithIo(allocator, path, std.testing.io, false);
    defer reopened.close();
    const reopened_store = reopened.backend.native_docstore orelse return error.CrashConsistencyMissingDocstore;
    const value = try reopened_store.file.getDocumentAlloc(allocator, "worker-ride-along");
    defer if (value) |bytes| allocator.free(bytes);
    try std.testing.expectEqualStrings("rode along", value orelse return error.TestExpectedEqual);
}

pub fn reportFailures(report: *const SweepReport) !void {
    if (report.ok()) return;
    for (report.failures.items) |f| {
        std.debug.print(
            "crash-consistency failure: barrier={d} kind={s} path={s} when={s} power_loss={s}: {s}\n",
            .{ f.barrier.sequence, @tagName(f.barrier.kind), f.barrier.path, @tagName(f.when), @tagName(f.power_loss), f.detail },
        );
    }
    return error.CrashConsistencyFailuresFound;
}

// ---------------------------------------------------------------------
// Sustained full_index visibility regression
// (docs/design/lite-performance/throughput-profile.md §6: a sustained
// single-writer `sync_level: full_index` run eventually acknowledges a
// write whose content never becomes searchable, even after reopen +
// run_until_idle()). No crash is injected here -- this is not a crash-
// consistency gap, it is a plain correctness gap under sustained load.
// ---------------------------------------------------------------------

/// Strict synchronous-visibility check: unlike `assertSearchable` (which
/// tolerates `error.IndexRebuilding` while a background catch-up finishes
/// after a crash-sweep reopen), a `sync_level: full_index` batch() must
/// already have made `key` searchable for `marker` by the time it returns
/// -- no retry, no backoff. A failure here means the ack lied.
fn expectSearchableNow(handle: *DbHandle, allocator: Allocator, search_index: []const u8, search_field: []const u8, key: []const u8, marker: usize) !void {
    var text_buf: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&text_buf, "marker{d}", .{marker});
    var result = try handle.db.search(allocator, .{
        .index_name = search_index,
        .full_text = .{ .match = .{ .field = search_field, .text = text } },
        .limit = 10,
    });
    defer result.deinit();
    for (result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, key)) return;
    }
    std.debug.print(
        "sustained full_index visibility: acknowledged document not searchable key={s} marker={s} hits={d}\n",
        .{ key, text, result.hits.len },
    );
    return error.SustainedFullIndexWriteNotVisible;
}

// docs/design/lite-performance/throughput-profile.md §6 repro, made
// deterministic and fast: a small key set absorbs repeated overwrites (so
// segment merge/compaction and journal retirement both run many times
// over the course of the run, same as the production repro's 128-key
// wraparound did over ~7,500 writes), and a lowered merge-policy override
// (`max_segments_per_tier`) reaches that same merge/retirement machinery
// in a few thousand writes instead of ~7,500.
//
// Every write uses `sync_level: full_index`, whose contract is: the
// acknowledged write is searchable with no further catch-up needed. This
// test checks that contract at a sample of writes along the way (every
// write would make a Debug run too slow) and, more importantly, for every
// key's *final* write after a full close + reopen + run_until_idle() --
// exactly the point the production repro's verification failed at.
test "sustained full_index single writer keeps every acknowledged write searchable after reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    index_manager.setBenchmarkTextMergePolicyOverride(.{
        .max_segments_per_tier = 3,
        .max_segment_size = 5 * 1024 * 1024 * 1024,
        .floor_segment_size = 0,
    });
    defer index_manager.setBenchmarkTextMergePolicyOverride(null);

    const key_count = 4;
    // Was 4000. `openDbWithIo`'s raw open never installs an optional
    // `TextMergeRuntime` (`DB.OpenOptions.text_merge` stays at its
    // `enabled = false` default, same as any "native serving" handle), and
    // `prepareInlineTextPublication`'s inline fast path for `.full_index`
    // writes (`mutation_materialization.zig`) never itself triggers a merge
    // check either: it adds its new one-document segment directly and never
    // marks `entry.compaction_pending`, so nothing -- not even a background
    // merge worker, if one were running -- ever learns this index needs a
    // look. Confirmed directly (debug instrumentation, not guessed):
    // segment count tracked 1:1 with write count (399 segments after 400
    // writes) with zero merges ever happening. `index.IndexWriter.
    // deleteAllByIdsTracked`'s per-write delete scans every live segment to
    // find the old version of the key being overwritten, so its cost is
    // O(live segment count) per write -- O(write count) once segments pile
    // up unboundedly -- making the whole loop O(total_writes^2): per-100-
    // write wall time grew write-over-write (18.2s, 21.8s, 25.3s, 29.0s,
    // 32.7s for writes 0-500), hitting every one of the 300s/600s/900s
    // timeouts this test used to fail on.
    //
    // Driving an actual background `TextMergeRuntime` from this test (so it
    // marks `compaction_pending` and merges on its own, the way a real
    // Owner-managed connection does) was tried and reverted: it reliably
    // hung a *different*, previously-passing crash-sweep test ("crash
    // sweep: graph metric build and compaction mid-scenario") in a
    // `derived catch_up failed ... err=OutcomeUnknown` exponential-backoff
    // retry loop that never recovers -- confirmed absent on an unmodified
    // build at the same commit, so the interaction between the inline
    // fast path marking `compaction_pending` for the first time and that
    // scenario's own background derived-worker catch-up is a real,
    // previously-latent bug, out of this fix's scope (same shape as the
    // background-loop/live-redo-commit races lite-single-commit-write-
    // path.md's own section 9.7d already documents and defers). Revisit together with
    // that work rather than here.
    //
    // Instead, drive an explicit, synchronous, already-public compaction
    // pass (`DB.compactTextIndexes` -> `IndexManager.compactAllTextIndexes`,
    // the same foreground merge `finalizeTextBatchMutations` runs for the
    // non-inline write path) alongside the already-explicit retirement
    // turn below -- this harness bypasses the Owner/pool layer that would
    // normally schedule both automatically, so both need a stand-in here,
    // and this one never touches `compaction_pending`/`TextMergeScheduler`
    // at all, so it cannot trip the hang above. 500 writes (125x
    // `key_count`, ~17 `runRetirementTurn` calls and as many explicit
    // compaction passes, still several real merges given the policy
    // override's `max_segments_per_tier = 3`) keeps the same "sustained
    // writes + merges + retirement" scenario this test documents wanting,
    // just at a tractable size now that segments no longer pile up
    // unboundedly between those passes.
    const total_writes = 500;

    var handle = try openDbWithIo(allocator, path, std.testing.io, true);
    defer handle.close();

    var latest_marker: [key_count]usize = @splat(0);
    var key_buf: [32]u8 = undefined;
    var value_buf: [256]u8 = undefined;

    var i: usize = 0;
    while (i < total_writes) : (i += 1) {
        const slot = i % key_count;
        const key = try std.fmt.bufPrint(&key_buf, "doc-{d}", .{slot});
        const value = try std.fmt.bufPrint(&value_buf, "{{\"body\":\"marker{d} churn alpha beta gamma delta\"}}", .{i});
        try handle.db.batch(.{ .writes = &.{.{ .key = key, .value = value }}, .sync_level = .full_index });
        latest_marker[slot] = i;

        if (i % 97 == 0 or i + 1 == total_writes) {
            try expectSearchableNow(&handle, allocator, "body_text", "body", key, i);
        }

        // In production this runs automatically, once per maintenance tick,
        // from the Owner/pool layer this low-level DB harness bypasses
        // (capi/lite_connection.zig around `store.serviceRetirement`, demand-
        // gated on the watermark `DocStore.requestRetirement` raises on every
        // ack). Drive it explicitly here so this test still exercises WS5
        // retirement running concurrently with sustained full_index writes
        // and segment merges, the combination the production repro needed.
        if (i % 29 == 0) try runRetirementTurn(allocator, &handle);

        // Same bypass as above, for segment merges: production drives
        // these from the Owner/pool layer's `TextMergeRuntime` (see the
        // `total_writes` comment above for why this test drives an
        // explicit, synchronous pass here instead of installing that
        // runtime). Keeps segment count bounded near the policy
        // override's `max_segments_per_tier` instead of growing by one
        // every write.
        if (i % 29 == 0) try handle.db.compactTextIndexes();
    }

    handle.close();
    handle = try openDbWithIo(allocator, path, std.testing.io, false);
    try handle.db.runUntilIdle();

    for (0..key_count) |slot| {
        var reopen_key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&reopen_key_buf, "doc-{d}", .{slot});
        try expectSearchableNow(&handle, allocator, "body_text", "body", key, latest_marker[slot]);
    }
}

// docs/design/lite-performance/throughput-profile.md §6 root cause, isolated
// at the lowest level: a ride-along commit
// (`NativeFile.commitTransactionWithDurability(false)`, the primitive
// behind a background text-segment merge's catalog update
// (`persistent.zig`'s `replaceSegmentsWithReservedIdOwned`,
// `txn.setDurable(false)`), a WS5 retirement turn
// (`DocStore.serviceRetirement` -> `commitRideAlong`), and a deferred
// progress-v2 record (`apply_state.contributeProgressDeferred`)) stages and
// physically writes its pages (`flushTransactionWrites` runs regardless of
// `durable`), but deliberately withholds publishing a new checkpoint slot
// (`commitTransactionWithDurability`'s `deferred_publication` branch: "Keep
// the last durable checkpoint slots intact until an explicit durability
// barrier"). Reopening from disk always resolves to the last *published*
// checkpoint, so that write is invisible after a reopen -- not only after a
// crash, but after a perfectly ordinary close, unless something publishes
// it first: either a later durable commit on the same file (how ride-along
// normally "becomes durable for free", per `Store.commitRideAlong`'s doc
// comment), or an explicit durability barrier (`NativeFile.sync()`, which
// `Store.syncIndexContents()` exposes). `capi/lite_connection.zig`'s
// `Owner.deinit()` is the one real place that was missing that barrier: it
// closed its own still-current, still-authoritative generation through the
// `lease_snapshot`/`DB.closeImmutableSnapshot()` path meant for a
// generation already superseded by a newer one (which never needs its own
// pending work published, since a newer durable generation already exists),
// silently discarding whatever was still riding along -- the fix added
// there (`Store.syncIndexContents()` before close) is exactly the missing
// barrier this test demonstrates is necessary.
test "a ride-along commit needs an explicit durability barrier before close, or it is lost" {
    const allocator = std.testing.allocator;

    // Scenario A: close with no barrier after the ride-along commit. The
    // commit itself returned success (this is the acknowledged-but-lost
    // shape of the bug, not a crash scenario), but it never becomes
    // recoverable.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/no-barrier.aflite", .{tmp.sub_path});
        defer allocator.free(path);

        var file = try native.NativeFile.createWithIo(allocator, std.testing.io, path, .{});
        try file.putDocument("seed", "seed-value");
        try file.beginTransaction();
        try file.putDocument("ride-along-doc", "ride-along-value");
        try file.commitTransactionWithDurability(false, null);
        try std.testing.expect(file.durable_header != null);
        file.close();

        var reopened = try native.NativeFile.openWithIo(allocator, std.testing.io, path, .{});
        defer reopened.close();
        const seed = try reopened.getDocumentAlloc(allocator, "seed");
        defer if (seed) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("seed-value", seed orelse return error.TestExpectedEqual);
        const lost = try reopened.getDocumentAlloc(allocator, "ride-along-doc");
        defer if (lost) |bytes| allocator.free(bytes);
        try std.testing.expect(lost == null);
    }

    // Scenario B: the identical ride-along commit, but with an explicit
    // `file.sync()` -- what `Store.syncIndexContents()` and the
    // `Owner.deinit()` fix call -- before close. Recovered after reopen.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/with-barrier.aflite", .{tmp.sub_path});
        defer allocator.free(path);

        var file = try native.NativeFile.createWithIo(allocator, std.testing.io, path, .{});
        try file.putDocument("seed", "seed-value");
        try file.beginTransaction();
        try file.putDocument("ride-along-doc", "ride-along-value");
        try file.commitTransactionWithDurability(false, null);
        try std.testing.expect(file.durable_header != null);
        try file.sync();
        try std.testing.expect(file.durable_header == null);
        file.close();

        var reopened = try native.NativeFile.openWithIo(allocator, std.testing.io, path, .{});
        defer reopened.close();
        const recovered = try reopened.getDocumentAlloc(allocator, "ride-along-doc");
        defer if (recovered) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("ride-along-value", recovered orelse return error.TestExpectedEqual);
    }
}

// WS "other write-path surfaces" (TTL, outboxes): the same durability-
// barrier shape as the ride-along test directly above, but with the exact
// key/value encodings `durable_outbox_store.clearPublished` (background
// replication-outbox trimming) and `db.zig`'s `pruneStaleGraphTtlCandidateContext`
// (background graph-TTL due-record cleanup) now write through
// `beginWriteBatchWithOptions(.{ .durability = .ride_along })` /
// `putBatchWithReplayWithOptions(..., .{ .durability = .ride_along })`
// (lite-single-commit-write-path.md §3.2a). Both are pure bookkeeping: an
// outbox row surviving a crash is simply re-published (idempotent,
// at-least-once); a stale due-record surviving a crash is simply re-pruned
// on the next sweep. Neither may force its own fsync, and losing either in
// a crash must never corrupt the file or the other key written in the same
// native transaction (`seed`, standing in for the primary row that always
// accompanies this bookkeeping in production).
test "TTL/outbox ride-along bookkeeping needs an explicit durability barrier before close, or the clear/prune is lost" {
    const allocator = std.testing.allocator;
    const outbox_mod = @import("../db/durable_outbox.zig");
    const graph_ttl_mod = @import("../db/graph_edge_ttl_expiration.zig");
    const internal_keys = @import("../internal_keys.zig");

    const outbox_key = outbox_mod.replication_batch_outbox_key;
    const outbox_value = try outbox_mod.encodeDurableReplicationOutboxAlloc(allocator, 7, "pending-batch-payload");
    defer allocator.free(outbox_value);

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(allocator, "doc:a", "g", "links", "doc:b");
    defer allocator.free(artifact_key);
    const due_key = try graph_ttl_mod.directIndexKeyAlloc(allocator, 123, artifact_key);
    defer allocator.free(due_key);
    const due_value = try graph_ttl_mod.encodeDirectAlloc(allocator, .{
        .index_name = "g",
        .generation = 7,
        .artifact_key = artifact_key,
        .deadline_ns = 123,
        .artifact_digest = @splat(5),
    });
    defer allocator.free(due_value);

    // Scenario A: delete both bookkeeping rows with a ride-along (non-
    // durable) commit, then close with NO later durability barrier. The
    // deletes are structurally acknowledged (the call returned success)
    // but, per §3.2a, not yet durable -- exactly the shape this whole
    // conversion relies on being safe to lose.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/ttl-outbox-no-barrier.aflite", .{tmp.sub_path});
        defer allocator.free(path);

        var file = try native.NativeFile.createWithIo(allocator, std.testing.io, path, .{});
        try file.putDocument("seed", "seed-value");
        try file.putDocument(outbox_key, outbox_value);
        try file.putDocument(due_key, due_value);

        try file.beginTransaction();
        try file.deleteDocument(outbox_key);
        try file.deleteDocument(due_key);
        try file.commitTransactionWithDurability(false, null);
        try std.testing.expect(file.durable_header != null);
        file.close();

        var reopened = try native.NativeFile.openWithIo(allocator, std.testing.io, path, .{});
        defer reopened.close();
        const seed = try reopened.getDocumentAlloc(allocator, "seed");
        defer if (seed) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("seed-value", seed orelse return error.TestExpectedEqual);

        const outbox_row = try reopened.getDocumentAlloc(allocator, outbox_key);
        defer if (outbox_row) |bytes| allocator.free(bytes);
        try std.testing.expectEqualSlices(u8, outbox_value, outbox_row orelse return error.TestExpectedEqual);

        const due_row = try reopened.getDocumentAlloc(allocator, due_key);
        defer if (due_row) |bytes| allocator.free(bytes);
        try std.testing.expectEqualSlices(u8, due_value, due_row orelse return error.TestExpectedEqual);
    }

    // Scenario B: the identical ride-along deletes, but with an explicit
    // `file.sync()` barrier before close -- both rows are durably cleared.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/ttl-outbox-with-barrier.aflite", .{tmp.sub_path});
        defer allocator.free(path);

        var file = try native.NativeFile.createWithIo(allocator, std.testing.io, path, .{});
        try file.putDocument("seed", "seed-value");
        try file.putDocument(outbox_key, outbox_value);
        try file.putDocument(due_key, due_value);

        try file.beginTransaction();
        try file.deleteDocument(outbox_key);
        try file.deleteDocument(due_key);
        try file.commitTransactionWithDurability(false, null);
        try std.testing.expect(file.durable_header != null);
        try file.sync();
        try std.testing.expect(file.durable_header == null);
        file.close();

        var reopened = try native.NativeFile.openWithIo(allocator, std.testing.io, path, .{});
        defer reopened.close();
        const seed = try reopened.getDocumentAlloc(allocator, "seed");
        defer if (seed) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("seed-value", seed orelse return error.TestExpectedEqual);

        const outbox_row = try reopened.getDocumentAlloc(allocator, outbox_key);
        defer if (outbox_row) |bytes| allocator.free(bytes);
        try std.testing.expect(outbox_row == null);

        const due_row = try reopened.getDocumentAlloc(allocator, due_key);
        defer if (due_row) |bytes| allocator.free(bytes);
        try std.testing.expect(due_row == null);
    }
}

// Same durability-barrier shape again, for the two ride-along conversions in
// `lite-single-commit-write-path.md`'s "adaptive/enrichment/recovery-agenda"
// workstream: `enrichment_runtime.zig`'s `mutateMediaPassOnce` (the media-
// pass cursor checkpoint, now `beginBatchWithOptions(.{ .durability =
// .ride_along })` instead of its own commit+explicit `sync`) and
// `capi/sql_ddl.zig`'s `acknowledgeAgenda` (the recovery-agenda marker clear,
// now `submitIndexMutation(..., durable = false)`). Both are safe to lose:
// a stale cursor just means a restart re-derives a bounded amount of
// recompute from the last durable checkpoint (`media_pass.zig`'s own doc
// comment -- "restart never infers coverage from a partial artifact set"),
// and an un-acknowledged recovery marker just means the next recovery turn
// re-scans and re-acknowledges it.
test "enrichment checkpoint and recovery-agenda ride-along bookkeeping need an explicit durability barrier before close, or the advance/acknowledgement is lost" {
    const allocator = std.testing.allocator;
    const media_pass_mod = @import("../db/enrichment/media_pass.zig");
    const recovery_agenda_mod = @import("recovery_agenda.zig");

    const cursor_prefix = try media_pass_mod.scope(allocator, "doc:a", "chunker", "body");
    defer allocator.free(cursor_prefix);
    const cursor_key = try media_pass_mod.key(allocator, cursor_prefix, null);
    defer allocator.free(cursor_key);
    const cursor_value_old = try media_pass_mod.encode(allocator, media_pass_mod.Cursor{
        .input_digest = @splat(9),
        .page_count = 1,
        .covered_through_ordinal = 64,
    });
    defer allocator.free(cursor_value_old);
    const cursor_value_new = try media_pass_mod.encode(allocator, media_pass_mod.Cursor{
        .input_digest = @splat(9),
        .page_count = 2,
        .covered_through_ordinal = 128,
    });
    defer allocator.free(cursor_value_new);

    // A plain (non-table-scoped) tracked key's own marker: `prefix ++ ""`.
    // `recovery_agenda.namespace("\x00\x00__metadata__:schema")` returns
    // `""`, matching this scope exactly.
    const recovery_marker_key = recovery_agenda_mod.prefix;

    // Scenario A: advance the checkpoint to its new value and clear the
    // recovery marker, both via a ride-along (non-durable) commit, then
    // close with NO later durability barrier. Structurally acknowledged
    // (the calls returned success) but, per §3.2a, not yet durable.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/enrichment-recovery-no-barrier.aflite", .{tmp.sub_path});
        defer allocator.free(path);

        var file = try native.NativeFile.createWithIo(allocator, std.testing.io, path, .{});
        try file.putDocument("seed", "seed-value");
        try file.putDocument(cursor_key, cursor_value_old);
        try file.putDocument(recovery_marker_key, "");

        try file.beginTransaction();
        try file.putDocument(cursor_key, cursor_value_new);
        try file.deleteDocument(recovery_marker_key);
        try file.commitTransactionWithDurability(false, null);
        try std.testing.expect(file.durable_header != null);
        file.close();

        var reopened = try native.NativeFile.openWithIo(allocator, std.testing.io, path, .{});
        defer reopened.close();
        const seed = try reopened.getDocumentAlloc(allocator, "seed");
        defer if (seed) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("seed-value", seed orelse return error.TestExpectedEqual);

        // Recovery resumes from the last durable checkpoint, not the lost
        // advance -- the restart's own re-derivation redoes the bounded
        // amount of work between the two covered_through_ordinal values.
        const cursor_row = try reopened.getDocumentAlloc(allocator, cursor_key);
        defer if (cursor_row) |bytes| allocator.free(bytes);
        try std.testing.expectEqualSlices(u8, cursor_value_old, cursor_row orelse return error.TestExpectedEqual);

        const marker_row = try reopened.getDocumentAlloc(allocator, recovery_marker_key);
        defer if (marker_row) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("", marker_row orelse return error.TestExpectedEqual);
    }

    // Scenario B: the identical ride-along writes, but with an explicit
    // `file.sync()` barrier before close -- the checkpoint advance and the
    // marker clear are both durable.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/enrichment-recovery-with-barrier.aflite", .{tmp.sub_path});
        defer allocator.free(path);

        var file = try native.NativeFile.createWithIo(allocator, std.testing.io, path, .{});
        try file.putDocument("seed", "seed-value");
        try file.putDocument(cursor_key, cursor_value_old);
        try file.putDocument(recovery_marker_key, "");

        try file.beginTransaction();
        try file.putDocument(cursor_key, cursor_value_new);
        try file.deleteDocument(recovery_marker_key);
        try file.commitTransactionWithDurability(false, null);
        try std.testing.expect(file.durable_header != null);
        try file.sync();
        try std.testing.expect(file.durable_header == null);
        file.close();

        var reopened = try native.NativeFile.openWithIo(allocator, std.testing.io, path, .{});
        defer reopened.close();
        const seed = try reopened.getDocumentAlloc(allocator, "seed");
        defer if (seed) |bytes| allocator.free(bytes);
        try std.testing.expectEqualStrings("seed-value", seed orelse return error.TestExpectedEqual);

        const cursor_row = try reopened.getDocumentAlloc(allocator, cursor_key);
        defer if (cursor_row) |bytes| allocator.free(bytes);
        try std.testing.expectEqualSlices(u8, cursor_value_new, cursor_row orelse return error.TestExpectedEqual);

        const marker_row = try reopened.getDocumentAlloc(allocator, recovery_marker_key);
        defer if (marker_row) |bytes| allocator.free(bytes);
        try std.testing.expect(marker_row == null);
    }
}

// ---------------------------------------------------------------------
// Crash-during-ride-along regression (prefix-ordered durability)
//
// The close-time fix above (`Owner.deinit()` now calls
// `Store.syncIndexContents()` before closing its own generation) only
// protects a *graceful* close: a ride-along commit's pages are flushed
// (`flushTransactionWrites` runs regardless of `durable`) but its
// checkpoint publication is deliberately deferred, so a plain close with
// no later durability barrier silently drops it. That fix says nothing
// about a real crash mid-stream. The open question the fix's own author
// raised: can a ride-along segment merge, WS5 retirement turn, or
// deferred progress-v2 record make the *last durably-published*
// checkpoint itself unreachable or inconsistent after a crash -- e.g. a
// merge's page reuse overwriting pages the durable segment set still
// points at, or a retirement deleting journal entries durable progress
// still needs? That would be a real prefix-order violation: losing
// already-acknowledged, already-durable data, not merely losing the
// not-yet-durable ride-along work (which is expected and fine).
//
// This scenario reuses "sustained full_index single writer..."'s own
// recipe above (a tiny key set under repeated overwrite, a lowered
// merge-policy override, and interleaved WS5 retirement turns) kept small
// enough for `runCrashSweep` to enumerate and replay every barrier it
// crosses -- including barriers immediately following a ride-along
// commit's own staged writes -- under `power_loss = .drop_unsynced`
// (the only mode that actually drops unsynced bytes; see
// `crash_sim_io.zig`'s `PowerLoss` doc comment). No graceful `close()`
// ever runs on the crashed replay path: `scenario.run` returns the
// injected error before reaching its own `defer handle.close()`'s normal
// fall-through effects on disk (see `DbHandle.close`'s doc comment -- the
// backend/DB teardown that *does* still run is best-effort cleanup of
// in-memory state, not a new durability barrier).
const ride_along_key_count = 3;
const ride_along_round_count = 3;
const RideAlongStep = struct {
    key: []const u8,
    before: ?[]const u8,
    after: []const u8,
    retire_after: bool,
};
const ride_along_steps: [ride_along_key_count * ride_along_round_count]RideAlongStep = blk: {
    var steps: [ride_along_key_count * ride_along_round_count]RideAlongStep = undefined;
    var i: usize = 0;
    for (0..ride_along_round_count) |round| {
        for (0..ride_along_key_count) |slot| {
            steps[i] = .{
                .key = std.fmt.comptimePrint("doc-{d}", .{slot}),
                .before = if (round == 0) null else std.fmt.comptimePrint("marker{d}", .{i - ride_along_key_count}),
                .after = std.fmt.comptimePrint("marker{d}", .{i}),
                // Roughly every third write, matching the non-crash
                // regression's `i % 29 == 0` cadence scaled down to this
                // scenario's much smaller write count -- frequent enough
                // that a WS5 retirement turn's own commit is itself a
                // barrier candidate throughout the sweep, not just at the
                // very end.
                .retire_after = (i % 3) == 2,
            };
            i += 1;
        }
    }
    break :blk steps;
};

/// Regression-B permanent invariant (docs/design/lite-performance/
/// lite-single-commit-write-path.md §3.2a prefix-ordered durability): once
/// `DB.batch` returns success for a sync level that promises the primary
/// write (and, for `.full_text`/`.full_index`, the relevant index work) is
/// durable -- never `.propose`/`.enrichments`, which don't promise that --
/// the commit that produced the acknowledged state must already be durable:
/// `Store.durableSequence()` must already cover `write_commit_sequence`, the
/// sequence *this write's own* group committed at
/// (`DB.batchWithCommittedSequence`'s out-param -- ultimately
/// `MutationRequest.committed_sequence`, stamped in `applyMutationGroup`
/// while the group's commit still held `Store.mutex`). This is deterministic
/// and unconditional, unlike the crash sweep's own barrier sampling, which
/// only ever observes this gap indirectly (as a lost write after a crash
/// that happens to land in exactly the right barrier window) and was
/// missing it entirely in most runs.
///
/// Comparing against `write_commit_sequence` rather than a freshly-read
/// `activeCheckpoint()` matters: a first version of this check read
/// `activeCheckpoint()` here, immediately after `batch()` returned, and fired
/// spuriously -- a background ride-along flush (`Store.
/// rideAlongFlushLoop`/maintenance, both run via `io.concurrent`) can land a
/// legitimate, unrelated, non-durable bookkeeping commit (e.g.
/// `__metadata__:index_status:...`) in the gap between this write's own
/// (already durable) commit and this check's read, advancing
/// `activeCheckpoint()` past what `durableSequence()` -- correctly -- has not
/// caught up to yet. `write_commit_sequence` is immune to that race: it is
/// stamped once, under `Store.mutex`, for exactly the group this write's own
/// ticket belongs to, and never changes afterward no matter what commits
/// later. Call this immediately after every acknowledged write a via_db
/// scenario makes at a durability-promising sync level.
fn assertAcknowledgedWriteIsDurable(handle: *DbHandle, write_commit_sequence: u64) !void {
    const store = handle.backend.native_docstore orelse return;
    const durable_sequence = store.durableSequence();
    if (durable_sequence < write_commit_sequence) {
        std.debug.print(
            "crash-consistency: INVARIANT VIOLATED -- batch() returned success for commit_sequence={d} but Store.durableSequence()={d} (durable_header_set={})\n",
            .{ write_commit_sequence, durable_sequence, store.file.durable_header != null },
        );
        return error.AcknowledgedWriteNotYetDurable;
    }
}

/// Regression-B hypothesis (c) check: stale-base lost update. Reads `key`
/// straight off the physical B-tree at the store's *current* active
/// checkpoint through a brand-new read transaction (`Store.beginRead`),
/// bypassing `db_mod.DB`'s own document/catalog caches and any replay/
/// catch-up materialization entirely -- same live `Store`/`NativeFile`
/// (a second, independent `NativeFile.open` on the same path conflicts
/// with the live writer's own lock: confirmed via `error.WouldBlock`), but
/// a strictly lower layer than anything `db.lookup()` goes through. If
/// this ever disagrees with what was just acknowledged -- with no crash,
/// no power loss, nothing dropped -- some commit between the ack and this
/// check published a document-index root built from a base that predates
/// the acknowledged write, i.e. a lost update from committing against a
/// stale snapshot.
fn assertFreshOnDiskValueContains(allocator: Allocator, handle: *DbHandle, key: []const u8, want_contains: []const u8) !void {
    const store = handle.backend.native_docstore orelse return;
    const internal_keys = @import("../internal_keys.zig");
    const encoded = try internal_keys.documentKeyAlloc(allocator, key);
    defer allocator.free(encoded);
    var txn = try store.beginRead();
    defer txn.abort();
    const raw = txn.get(encoded) catch |err| switch (err) {
        error.NotFound => null,
        else => {
            std.debug.print("[native-recovery] fresh on-disk read: get failed key={s} err={s}\n", .{ key, @errorName(err) });
            return err;
        },
    };
    const got = raw orelse "<missing>";
    const commit_sequence = store.file.activeCheckpoint().commit_sequence;
    if (native.recovery_debug_log) std.debug.print(
        "[native-recovery] fresh on-disk read key={s} commit_sequence={d} got={s}\n",
        .{ key, commit_sequence, got },
    );
    if (std.mem.indexOf(u8, got, want_contains) == null) {
        std.debug.print(
            "crash-consistency: STALE-BASE DIVERGENCE (hypothesis c) -- fresh on-disk read (no crash, no power loss) key={s} want_contains={s} got={s} active_checkpoint_commit_sequence={d}\n",
            .{ key, want_contains, got, commit_sequence },
        );
        return error.StaleBaseLostUpdate;
    }
}

fn runSustainedFullIndexRideAlongUnderCrash(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    if (native.recovery_debug_log) std.debug.print("[native-recovery] === subrun start path={s} ===\n", .{path});
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();
    defer if (native.recovery_debug_log) std.debug.print("[native-recovery] === subrun end path={s} ===\n", .{path});

    // Reaches the same merge/retirement machinery the production repro
    // needed ~7,500 writes for in a handful of writes instead -- see the
    // non-crash regression above for the full rationale.
    index_manager.setBenchmarkTextMergePolicyOverride(.{
        .max_segments_per_tier = 2,
        .max_segment_size = 5 * 1024 * 1024 * 1024,
        .floor_segment_size = 0,
    });
    defer index_manager.setBenchmarkTextMergePolicyOverride(null);

    // `inline for`, not a regular `for`: `ride_along_steps` is comptime-known
    // and every field this loop reads off `step` (key/before/after) is a
    // comptime string literal, but the loop body is still ordinary runtime
    // code (db writes, retirement, asserts) executed once per unrolled copy
    // -- `inline for` only changes how the *source* is lowered, not when
    // anything runs. A plain `for` here was a real, confirmed bug: `&.{...}`
    // for `Checkpoint.expectations` is evaluated as a block-scoped temporary
    // whose storage is the loop body's own stack slot, reused by every
    // iteration of an ordinary (non-inline) `for` loop -- so every
    // `checkpoints.append` call's `expectations` slice ended up aliasing the
    // *same* address, and by the time a crashed run's invariant checker
    // read `checkpoints.items` back, every entry showed the last iteration's
    // own key/value instead of its own (confirmed directly: a debug dump of
    // `checkpoints[0..2]` during a failing run showed all three aliased to
    // the third write's own key, not the first and second writes' real
    // ones). This made `checkCheckpointsViaDb` demand the *wrong* key's
    // value from the acknowledged set, misreporting a perfectly durable
    // write as `CrashConsistencyAcknowledgedWriteLost` -- a false positive
    // in the harness, not a storage durability bug: nothing was actually
    // lost. `inline for` unrolls the loop at compile time (the same idiom
    // this file already uses for `concurrent_steps`/`retirement_docs`
    // immediately above), giving each iteration's `&.{...}` its own,
    // distinct, comptime-promoted storage -- no aliasing, no allocator
    // needed to own it.
    inline for (ride_along_steps) |step| {
        const json = try std.fmt.allocPrint(allocator, "{{\"body\":\"{s} churn alpha beta gamma\"}}", .{step.after});
        defer allocator.free(json);
        try checkpoints.append(allocator, .{
            .expectations = &.{.{ .key = step.key, .before = step.before, .after = step.after, .searchable_after = step.after }},
        });
        var write_commit_sequence: u64 = 0;
        try handle.db.batchWithCommittedSequence(.{ .writes = &.{.{ .key = step.key, .value = json }}, .sync_level = .full_index }, &write_commit_sequence);
        if (native.recovery_debug_log) if (handle.backend.native_docstore) |store| {
            std.debug.print("[native-recovery] acked key={s} value={s} write_commit_sequence={d} active_commit_sequence={d} durable_header_set={}\n", .{
                step.key,
                step.after,
                write_commit_sequence,
                store.file.activeCheckpoint().commit_sequence,
                store.file.durable_header != null,
            });
        };
        // Stale-base (hypothesis c) check: fresh on-disk read, bypassing
        // every live cache. Kept as the active, trusted assertion.
        try assertFreshOnDiskValueContains(allocator, &handle, step.key, step.after);
        // Durability invariant, now comparing against this write's own
        // commit sequence (see `assertAcknowledgedWriteIsDurable`'s doc
        // comment for why comparing against `activeCheckpoint()` instead
        // used to fire spuriously).
        try assertAcknowledgedWriteIsDurable(&handle, write_commit_sequence);

        // Mirrors production: a WS5 retirement turn driven explicitly here
        // (this harness bypasses the Owner/pool layer that normally drives
        // it from the ack path) interleaved with sustained full_index
        // writes and the segment merges they trigger. No checkpoint: a
        // retirement turn touches no document key.
        if (step.retire_after) {
            try runRetirementTurn(allocator, &handle);
            try assertFreshOnDiskValueContains(allocator, &handle, step.key, step.after);
        }
    }
}

test "crash sweep: sustained full_index writes survive a crash during ride-along merge and retirement" {
    // Regression B investigation (AcknowledgedWriteLost under ride-along
    // merge/retirement): scope native.zig's recovery instrumentation to
    // just this scenario, so a failing run's checkpoint-candidate/verify
    // trace and every ride-along-vs-durable commit's resulting sequence
    // are directly in the test log instead of needing a separate
    // instrumented rebuild.
    const prior_recovery_log = native.recovery_debug_log;
    native.recovery_debug_log = true;
    defer native.recovery_debug_log = prior_recovery_log;

    var report = try runCrashSweep(std.testing.allocator, .{
        .name = "sustained-full-index-ride-along",
        .run = runSustainedFullIndexRideAlongUnderCrash,
        .via_db = true,
        .search_index = "body_text",
        .search_field = "body",
        .retirement = true,
    }, .{});
    defer report.deinit();
    try reportFailures(&report);
}

// Regression for the `CrashConsistencyAcknowledgedWriteLost` false positive
// in "crash sweep: sustained full_index writes survive a crash during
// ride-along merge and retirement": a plain (non-`inline`) `for` loop
// building `Checkpoint.expectations` via `&.{...}` reused the same loop-body
// stack slot on every iteration, so by the time a crashed run's checker read
// `checkpoints.items` back, every entry had silently aliased the *last*
// iteration's own key/value -- not a storage durability bug at all (nothing
// was ever actually lost), but the harness itself misreporting an entirely
// different, perfectly durable write as missing. This exercises the root
// cause directly and fast, without any crash simulation: run the scenario's
// own write loop to completion against a plain passthrough `Io`, then assert
// every recorded checkpoint's key/after matches its own step in
// `ride_along_steps`, in order -- aliasing would collapse every entry to the
// last step's own key/value, which this catches immediately.
test "crash sweep: sustained full_index ride-along checkpoints are not aliased across loop iterations" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testAflitePath(allocator, tmp);
    defer allocator.free(path);

    var checkpoints = std.ArrayListUnmanaged(Checkpoint).empty;
    defer checkpoints.deinit(allocator);
    try runSustainedFullIndexRideAlongUnderCrash(allocator, path, std.testing.io, &checkpoints);

    try std.testing.expectEqual(ride_along_steps.len, checkpoints.items.len);
    for (checkpoints.items, ride_along_steps) |cp, want| {
        try std.testing.expectEqual(@as(usize, 1), cp.expectations.len);
        try std.testing.expectEqualStrings(want.key, cp.expectations[0].key);
        try std.testing.expectEqualStrings(want.after, cp.expectations[0].after.?);
        if (want.before) |before| {
            try std.testing.expectEqualStrings(before, cp.expectations[0].before.?);
        } else {
            try std.testing.expect(cp.expectations[0].before == null);
        }
    }
}

// Deliberately no `.exhaustive` variant: this scenario's own writes already
// cross several native commits each (primary + full-text + occasional
// merge/retirement ride-along), so an exhaustive sweep's every-barrier
// replay is combinatorially far more expensive here than for the simpler
// primary/full-text scenarios above. `quick` mode's sampled barriers
// (including, by construction, points right after a ride-along commit --
// see `RideAlongStep.retire_after`) already cover this scenario's own
// specific risk.
