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
        sim.setRunLabel(scenario.name);
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
    // `std.math.maxInt(u64)` and `.rename`/empty-path flag "no crash fired
    // in this run" in a failure report -- distinct from a real barrier 0 --
    // so a failure found with no crash at all (the uncrashed discovery
    // pass, or a (barrier, when) pair that never actually interrupted this
    // scenario) is never confused with one found exactly at barrier 0.
    const placeholder = if (outcome) |o| o.barrier else Barrier{ .sequence = std.math.maxInt(u64), .kind = .file_sync, .path = "" };
    const when: When = if (outcome) |o| o.when else .after;
    const power_loss: PowerLoss = if (outcome) |o| o.power_loss else .none;
    const crash_note = if (outcome == null) " [uncrashed run]" else "";

    if (scenario.via_db) {
        checkCheckpointsViaDb(allocator, path, outcome, checkpoints, scenario.search_index, scenario.search_field, scenario.retirement) catch |err| {
            try report.fail(placeholder, when, power_loss, "db invariant check failed: {s}{s}", .{ @errorName(err), crash_note });
        };
    } else {
        checkCheckpointsViaNative(allocator, path, outcome, checkpoints) catch |err| {
            try report.fail(placeholder, when, power_loss, "native invariant check failed: {s}{s}", .{ @errorName(err), crash_note });
        };
    }
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
    const before_sequence = store.file.activeCheckpoint().commit_sequence;

    try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });

    const after_ack_sequence = store.file.activeCheckpoint().commit_sequence;
    const commits_for_this_write = after_ack_sequence - before_sequence;
    // Same bound as the no-worker scenario above: the inline publication
    // itself is still exactly one commit: see that test's comment for why
    // a handful more land end to end (unrelated background bookkeeping WS7
    // owns driving to zero). Proves the inline fast path engaged here too
    // -- without it, this write would additionally wake and wait on a
    // worker, but would not by itself add more *commits* before the ack
    // returns, so the real claim this bound guards is the search-visible
    // assertion immediately below: that only holds if the write's own
    // index effects are already durable at ack time, which requires the
    // inline path, not the worker (a worker applies asynchronously, after
    // `batch()` has already returned).
    try std.testing.expect(commits_for_this_write <= 6);

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
test "crash sweep: full text recovers from one transient io fault with no crash" {
    const allocator = std.testing.allocator;
    const kinds = [_]sim_io.IoFaultKind{ .write, .sync, .create };
    for (kinds) |kind| {
        var at_op: u64 = 0;
        while (at_op < 12) : (at_op += 1) {
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
                // something retryable rather than wedging the owner.
                try handle.db.batch(.{ .writes = &.{.{ .key = "ft-doc", .value = "{\"body\":\"needle in a haystack\"}" }}, .sync_level = .full_text });
            };
            handle.close();

            // Reopen with a plain, unwrapped `std.Io` -- no `SimIo`, no
            // fault -- and confirm the owner's own background catch-up
            // resumes and the document becomes searchable without this
            // reopen being the only possible recovery path: proving (b)
            // the index never stays `IndexRebuilding` forever once the
            // underlying I/O is healthy again.
            var reopened = try openDbWithIo(allocator, path, std.testing.io, false);
            defer reopened.close();
            assertSearchable(&reopened, allocator, "body_text", "body", "ft-doc", "needle") catch |err| {
                std.debug.print("io-fault regression failed kind={s} at_op={d}: {s}\n", .{ @tagName(kind), at_op, @errorName(err) });
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
        error.CommitVisibilityNotSatisfied, error.InputOutput => {},
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
    while (!closer.done.load(.acquire)) {
        if (platform_time.monotonicNs() >= deadline_ns) {
            thread.detach();
            return error.ShutdownHangDetected;
        }
        std.Thread.yield() catch {};
    }
    thread.join();
    handle_open = false;
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
    const total_writes = 4000;

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
        try file.commitTransactionWithDurability(false);
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
        try file.commitTransactionWithDurability(false);
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

fn runSustainedFullIndexRideAlongUnderCrash(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(Checkpoint)) anyerror!void {
    var handle = try openDbWithIo(allocator, path, io, true);
    defer handle.close();

    // Reaches the same merge/retirement machinery the production repro
    // needed ~7,500 writes for in a handful of writes instead -- see the
    // non-crash regression above for the full rationale.
    index_manager.setBenchmarkTextMergePolicyOverride(.{
        .max_segments_per_tier = 2,
        .max_segment_size = 5 * 1024 * 1024 * 1024,
        .floor_segment_size = 0,
    });
    defer index_manager.setBenchmarkTextMergePolicyOverride(null);

    for (ride_along_steps) |step| {
        const json = try std.fmt.allocPrint(allocator, "{{\"body\":\"{s} churn alpha beta gamma\"}}", .{step.after});
        defer allocator.free(json);
        try checkpoints.append(allocator, .{
            .expectations = &.{.{ .key = step.key, .before = step.before, .after = step.after, .searchable_after = step.after }},
        });
        try handle.db.batch(.{ .writes = &.{.{ .key = step.key, .value = json }}, .sync_level = .full_index });

        // Mirrors production: a WS5 retirement turn driven explicitly here
        // (this harness bypasses the Owner/pool layer that normally drives
        // it from the ack path) interleaved with sustained full_index
        // writes and the segment merges they trigger. No checkpoint: a
        // retirement turn touches no document key.
        if (step.retire_after) try runRetirementTurn(allocator, &handle);
    }
}

test "crash sweep: sustained full_index writes survive a crash during ride-along merge and retirement" {
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

// Deliberately no `.exhaustive` variant: this scenario's own writes already
// cross several native commits each (primary + full-text + occasional
// merge/retirement ride-along), so an exhaustive sweep's every-barrier
// replay is combinatorially far more expensive here than for the simpler
// primary/full-text scenarios above. `quick` mode's sampled barriers
// (including, by construction, points right after a ride-along commit --
// see `RideAlongStep.retire_after`) already cover this scenario's own
// specific risk.
