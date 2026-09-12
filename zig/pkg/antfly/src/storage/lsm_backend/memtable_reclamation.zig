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

//! FIFO retirement, independent of storage admission and cancellation. The
//! already-owned State header supplies the queue link; starting a continuation
//! never allocates. Only the destructive cursor changes while unlocked.
const std = @import("std");
const State = @import("state.zig").State;
const Account = @import("memory_account.zig").Account;
const runtime = @import("runtime.zig");
const clock = @import("antfly_platform").time;

pub const Job = struct {
    header: *State,
    reclaimer: State.Reclaimer,
    account: ?*Account,
    fallback_bytes: u64,
    array_bytes: u64,

    pub fn init(state: *State) Job {
        return .{
            .header = state,
            .reclaimer = .init(state.*),
            .account = if (state.account) |account| account.retain() else null,
            // Flat published generations freeze their charge at publication,
            // not on last-reader release. Never inspect freed entries off-lock.
            .fallback_bytes = if (state.account == null) state.frozen_memory_bytes.? else 0,
            .array_bytes = state.entries.capacity * @sizeOf(@import("state.zig").OwnedEntry),
        };
    }

    pub fn memoryBytes(self: *const Job, pass: u64) u64 {
        return @sizeOf(State) +| if (self.account) |account| account.chargeOnce(pass) +| self.array_bytes else self.fallback_bytes;
    }

    fn finish(self: *Job, allocator: std.mem.Allocator) void {
        if (self.account) |account| account.release();
        allocator.destroy(self.header);
    }
};

pub var test_slice_hook: ?*const fn (*anyopaque) void = null;

pub fn retire(backend: anytype, state: *State) void {
    std.debug.assert(state.account != null or state.frozen_memory_bytes != null);
    state.retired_next = null;
    if (backend.retired_memory_tail) |tail| tail.retired_next = state else backend.retired_memory_head = state;
    backend.retired_memory_tail = state;
    backend.memtable_reclaim_pending +|= 1;
    backend.cached_maintenance_hint.store(1, .release);
}

pub fn pending(backend: anytype) bool {
    return !backend.memtable_reclaim_in_flight and (backend.memtable_reclaimer != null or backend.retired_memory_head != null);
}

/// Called with Backend.mu held; returns with it held. A lifecycle pin prevents
/// close from destroying the backend during the unlocked portion.
pub fn reclaimSliceLocked(backend: anytype) void {
    if (!pending(backend)) return;
    backend.memtable_reclaim_in_flight = true;
    backend.retainReaderKind(.other);
    const started = clock.monotonicNs();
    const deadline = started +| 2 * std.time.ns_per_ms;
    var credits: usize = 2048;
    while (credits != 0 and clock.monotonicNs() < deadline) {
        if (backend.memtable_reclaimer == null) {
            const state = backend.retired_memory_head orelse break;
            backend.retired_memory_head = state.retired_next;
            if (backend.retired_memory_head == null) backend.retired_memory_tail = null;
            backend.memtable_reclaimer = .init(state);
        }
        backend.mu.unlock();
        var quantum: usize = @min(credits, 64);
        const before = quantum;
        const done = backend.memtable_reclaimer.?.reclaimer.step(backend.allocator, &quantum);
        if (@import("builtin").is_test) if (test_slice_hook) |hook| hook(backend);
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
        credits -= before - quantum;
        if (done) {
            backend.memtable_reclaimer.?.finish(backend.allocator);
            backend.memtable_reclaimer = null;
            backend.memtable_reclaim_pending -= 1;
        }
    }
    backend.memtable_reclaim_slices +|= 1;
    backend.memtable_reclaim_units +|= 2048 - credits;
    backend.memtable_reclaim_max_slice_ns = @max(backend.memtable_reclaim_max_slice_ns, clock.monotonicNs() -| started);
    backend.releaseReaderKind(.other);
    backend.memtable_reclaim_in_flight = false;
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
}

/// All external owners have stopped. Drain through the same bounded primitive
/// before destroying allocator, accounting, and storage dependencies.
pub fn drain(backend: anytype) void {
    _ = runtime.lockBackend(@TypeOf(backend.*), backend);
    std.debug.assert(!backend.memtable_reclaim_in_flight);
    while (pending(backend)) {
        reclaimSliceLocked(backend);
        backend.mu.unlock();
        if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
    }
    backend.mu.unlock();
}

const Representation = enum { tree, flat, arena };

fn makeState(allocator: std.mem.Allocator, count: usize, representation: Representation) !*State {
    var source: @import("state.zig").ActiveMemTable = .{};
    defer source.deinit(allocator);
    for (0..count) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try source.upsert(allocator, .{ .name = "rows" }, &key, "value", false);
    }
    const state = try allocator.create(State);
    errdefer allocator.destroy(state);
    state.* = switch (representation) {
        .tree => try source.snapshot(allocator),
        .flat => try source.clone(allocator),
        .arena => try source.cloneArena(allocator),
    };
    state.freezeMemoryAccounting();
    return state;
}

test "memtable reclamation handles tree flat and arena ownership under allocation failure" {
    const Fixture = struct {
        fn check(allocator: std.mem.Allocator, representation: Representation) !void {
            const state = try makeState(allocator, 9, representation);
            var job = Job.init(state);
            defer job.finish(allocator);
            var credits: usize = 0;
            try std.testing.expect(!job.reclaimer.step(allocator, &credits));
            var turns: usize = 0;
            while (true) {
                credits = 1;
                turns += 1;
                if (job.reclaimer.step(allocator, &credits)) break;
            }
            try std.testing.expect(turns >= 10);
            try std.testing.expect(job.reclaimer.step(allocator, &credits));
        }
    };
    inline for (std.meta.tags(Representation)) |representation|
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{representation});
}

test "memtable reclamation preserves shared roots and finishes without allocation" {
    const allocator = std.testing.allocator;
    var source: @import("state.zig").ActiveMemTable = .{};
    defer source.deinit(allocator);
    try source.upsert(allocator, .{ .name = "rows" }, "a", "old", false);
    var old = try source.snapshot(allocator);
    defer old.deinit(allocator);
    try source.upsert(allocator, .{ .name = "rows" }, "a", "new", false);
    try source.upsert(allocator, .{ .name = "rows" }, "b", "second", false);
    var job = State.Reclaimer.init(try source.snapshot(allocator));
    source.deinit(allocator);
    var credits: usize = 1;
    while (!job.step(allocator, &credits)) credits = 1;
    try std.testing.expectEqualStrings("old", old.entryAt(0).value);
}

test "memtable reclamation bounds last-reader release and drains FIFO under fences" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const resources = @import("../resource_manager.zig");
    const Hook = struct {
        var visits: usize = 0;
        var wakes: usize = 0;
        var sleeps: usize = 0;
        var arrival: ?*State = null;
        fn visit(raw: *anyopaque) void {
            const backend: *Backend = @ptrCast(@alignCast(raw));
            std.debug.assert(backend.mu.tryLock());
            defer backend.mu.unlock();
            std.debug.assert(backend.active_readers != 0);
            const header = backend.memtable_reclaimer.?.header;
            const units = backend.memtable_reclaim_units;
            reclaimSliceLocked(backend); // a second caller cannot steal the cursor
            std.debug.assert(units == backend.memtable_reclaim_units);
            if (arrival) |state| {
                retire(backend, state);
                arrival = null;
                std.debug.assert(backend.memtable_reclaimer.?.header == header);
            }
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            std.debug.assert(backend.options.resource_manager.?.snapshot().memory.used_bytes >= @sizeOf(State));
            visits += 1;
        }
        fn wake(_: *anyopaque) void {
            wakes += 1;
        }
        fn sleep(_: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            sleeps += 1;
            return error.Canceled;
        }
    };
    Hook.visits = 0;
    Hook.wakes = 0;
    Hook.sleeps = 0;
    var manager = resources.ResourceManager.init(.{});
    var vtable = std.testing.io.vtable.*;
    vtable.sleep = Hook.sleep;
    var io = std.testing.io;
    io.vtable = &vtable;
    var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager, .read_runtime = .{ .io = io }, .maintenance_waker = .{ .ptr = &manager, .wake_fn = Hook.wake } });
    defer backend.close();
    const state = try makeState(std.testing.allocator, 10000, .tree);
    // Install this tree as a real mutable generation, then pin a read epoch.
    backend.mutable.ordered.root = state.ordered_root;
    backend.mutable.ordered.account = state.account;
    backend.mutable.logical_bytes = state.estimatedLogicalBytes();
    std.testing.allocator.destroy(state);
    var read = try backend.beginRead();
    backend.mutable.deinit(std.testing.allocator);
    backend.invalidateMutableReadSnapshot();
    Hook.arrival = try makeState(std.testing.allocator, 7, .arena);
    defer if (Hook.arrival) |left| {
        left.deinit(std.testing.allocator);
        std.testing.allocator.destroy(left);
    };
    test_slice_hook = Hook.visit;
    defer test_slice_hook = null;
    read.abort();
    try std.testing.expect(backend.memtable_reclaim_pending != 0);
    try std.testing.expect(backend.memtable_reclaim_units <= 2048);
    try std.testing.expect(Hook.visits > 0 and Hook.wakes > 0);
    backend.active_bulk_ingest_batches = 1;
    backend.manifest_recovery_required = true;
    try std.testing.expectEqual(@as(?u64, 0), backend.nextMaintenanceWakeDelayNsBestEffort());
    const before = backend.memtable_reclaim_units;
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expect(backend.memtable_reclaim_units - before <= 2048);
    backend.drainRetiredMemtables();
    try std.testing.expectEqual(@as(u64, 0), backend.memtable_reclaim_pending);
    try std.testing.expect(backend.retired_memory_head == null and backend.retired_memory_tail == null);
    try std.testing.expect(Hook.sleeps > 0);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "memtable reclamation close and abandon drain unfinished generations" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const resources = @import("../resource_manager.zig");
    for ([_]bool{ false, true }) |abandon| {
        var manager = resources.ResourceManager.init(.{});
        var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager });
        retire(&backend, try makeState(std.testing.allocator, 4096, .tree));
        try std.testing.expect(backend.mu.tryLock());
        reclaimSliceLocked(&backend);
        backend.mu.unlock();
        try std.testing.expect(backend.memtable_reclaim_pending > 0);
        if (abandon) backend.abandonAfterCrash() else backend.close();
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "memtable reclamation owned bulk and replay scans use bounded retirement" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    for ([_]usize{ 0, 5000 }) |count| {
        for ([_]bool{ false, true }) |replay| {
            var backend = Backend.init(allocator, .{ .bulk_ingest_current_scan_clone_max_bytes = 16 * 1024 * 1024 });
            defer backend.close();
            const state = try makeState(allocator, count, .tree);
            backend.mutable.ordered.root = state.ordered_root;
            backend.mutable.ordered.account = state.account;
            backend.mutable.logical_bytes = state.estimatedLogicalBytes();
            allocator.destroy(state);
            backend.active_bulk_ingest_batches = if (replay) 0 else 1;
            var scan = if (replay)
                try runtime.BoundCurrentScanTxn(Backend).openReplayLane(&backend, .{ .name = "rows" }, "", "")
            else
                try runtime.BoundCurrentScanTxn(Backend).open(&backend, .{ .name = "rows" });
            backend.mutable.deinit(allocator);
            scan.abort();
            try std.testing.expect(backend.memtable_reclaim_units <= 2048);
            try std.testing.expectEqual(@as(u64, 0), backend.bulk_ingest_current_scan_clone_active_bytes);
            if (count != 0) try std.testing.expect(backend.memtable_reclaim_pending != 0);
            backend.drainRetiredMemtables();
        }
    }
}

test "memtable reclamation read-only maintenance drains memory without storage work" {
    const Backend = @import("../lsm_backend.zig").Backend;
    var backend = Backend.init(std.testing.allocator, .{ .backend = .{ .read_only = true } });
    defer backend.close();
    retire(&backend, try makeState(std.testing.allocator, 5000, .arena));
    var turns: usize = 0;
    while (pending(&backend)) : (turns += 1) {
        try std.testing.expect(try backend.runMaintenanceStepBestEffort());
        try std.testing.expect(turns < 1000);
    }
    try std.testing.expect(turns >= 3);
    try std.testing.expect(!try backend.runMaintenanceStepBestEffort());
}

test "memtable reclamation last-reference latency benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.heap.smp_allocator;
    for ([_]usize{ 1000, 10000, 100000 }) |count| {
        const control = try makeState(allocator, count, .tree);
        const old_start = clock.monotonicNs();
        control.deinit(allocator);
        allocator.destroy(control);
        const old_ns = clock.monotonicNs() -| old_start;
        var backend = Backend.init(allocator, .{});
        defer backend.close();
        const state = try makeState(allocator, count, .tree);
        backend.mutable.ordered.root = state.ordered_root;
        backend.mutable.ordered.account = state.account;
        backend.mutable.logical_bytes = state.estimatedLogicalBytes();
        allocator.destroy(state);
        var read = try backend.beginRead();
        backend.mutable.deinit(allocator);
        backend.invalidateMutableReadSnapshot();
        const start = clock.monotonicNs();
        read.abort();
        const release_ns = clock.monotonicNs() -| start;
        try std.testing.expect(backend.memtable_reclaim_units <= 2048);
        const drain_start = clock.monotonicNs();
        backend.drainRetiredMemtables();
        const drain_ns = clock.monotonicNs() -| drain_start;
        std.debug.print("\nmemtable-retirement rows={d} old_deinit_ns={d} read_release_ns={d} remaining_drain_ns={d} slices={d} max_slice_ns={d}\n", .{ count, old_ns, release_ns, drain_ns, backend.memtable_reclaim_slices, backend.memtable_reclaim_max_slice_ns });
    }
}
