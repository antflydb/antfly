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

const std = @import("std");
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Account = @import("memory_account.zig").Account;
const runtime = @import("runtime.zig");
const clock = @import("antfly_platform").time;

/// Stack-owned continuation: registration and retirement never allocate.
/// Only these immutable fields are visible to concurrent accounting; the
/// destructive traversal remains private to the thread doing reclamation.
pub const Job = struct {
    next: ?*Job,
    account: ?*Account,
    pool_bytes: u64,

    pub fn memoryBytes(self: *const Job, pass: u64) u64 {
        return self.pool_bytes +| if (self.account) |account| account.chargeOnce(pass) else 0;
    }
};

pub var test_slice_hook: ?*const fn (*anyopaque) void = null;

/// Allocated before acquiring a snapshot. Retirement is an infallible handoff,
/// including inside administrative fences that must never unlock mid-publish.
pub const Snapshot = struct {
    value: Ledger,
    active_next: ?*Snapshot,
    active_previous: ?*Snapshot = null,
    retired_next: ?*Snapshot = null,
    reclaimer: ?Ledger.Reclaimer = null,
    account: ?*Account = null,
    pool_bytes: u64 = 0,
    retired: bool = false,

    pub fn capture(backend: anytype, source: *const Ledger) !*Snapshot {
        const self = try backend.allocator.create(Snapshot);
        self.* = .{ .value = source.fork(), .active_next = if (@hasField(@TypeOf(backend.*), "ledger_snapshots")) backend.ledger_snapshots else null };
        if (@hasField(@TypeOf(backend.*), "ledger_snapshots")) {
            if (self.active_next) |next| next.active_previous = self;
            backend.ledger_snapshots = self;
        }
        return self;
    }

    pub fn retire(self: *Snapshot, backend: anytype) void {
        if (comptime !@hasField(@TypeOf(backend.*), "ledger_snapshots")) {
            self.value.deinit(backend.allocator);
            backend.allocator.destroy(self);
            return;
        }
        std.debug.assert(self.reclaimer == null);
        const value = self.value;
        self.value = .empty;
        self.account = if (value.tree.account) |account| account.retain() else null;
        self.pool_bytes = value.spare.capacity * @sizeOf(usize) + value.tree.spare.capacity * @sizeOf(usize);
        self.reclaimer = .init(value);
        self.retired = true;
        if (backend.retired_ledger_tail) |tail| tail.retired_next = self else backend.retired_ledger_snapshots = self;
        backend.retired_ledger_tail = self;
        backend.cached_maintenance_hint.store(1, .release);
    }

    pub fn memoryBytes(self: *const Snapshot, pass: u64) u64 {
        return @sizeOf(Snapshot) +| if (self.retired)
            self.pool_bytes +| if (self.account) |account| account.chargeOnce(pass) else 0
        else
            self.value.memoryBytes(pass);
    }
};

pub fn reclaimSliceLocked(backend: anytype) void {
    if (backend.ledger_reclaim_in_flight) return;
    if (backend.retired_ledger_snapshots == null) return;
    backend.ledger_reclaim_in_flight = true;
    backend.retainReaderKind(.other);
    const deadline = clock.monotonicNs() +| 2 * std.time.ns_per_ms;
    var units: usize = 0;
    for (0..64) |_| {
        const pending = backend.retired_ledger_snapshots orelse break;
        if (units >= 2048 or clock.monotonicNs() >= deadline) break;
        backend.mu.unlock();
        const result = step(backend, &pending.reclaimer.?, 2048 - units, deadline);
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
        note(backend, result);
        units += result.units;
        if (!result.done) break;
        backend.retired_ledger_snapshots = pending.retired_next;
        if (pending.retired_next == null) backend.retired_ledger_tail = null;
        if (pending.active_previous) |previous| previous.active_next = pending.active_next else backend.ledger_snapshots = pending.active_next;
        if (pending.active_next) |next| next.active_previous = pending.active_previous;
        if (pending.account) |account| account.release();
        backend.allocator.destroy(pending);
    }
    backend.releaseReaderKind(.other);
    backend.ledger_reclaim_in_flight = false;
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
}

const Slice = struct { done: bool, units: usize, ns: u64 };

fn step(backend: anytype, reclaimer: *Ledger.Reclaimer, limit: usize, deadline: u64) Slice {
    const started = clock.monotonicNs();
    var credits = limit;
    var done = false;
    while (credits != 0 and clock.monotonicNs() < deadline) {
        var part: usize = @min(credits, 64);
        const before = part;
        done = reclaimer.step(backend.allocator, &part);
        credits -= before - part;
        if (done) break;
    }
    const elapsed = clock.monotonicNs() -| started;
    if (@import("builtin").is_test) if (test_slice_hook) |hook| hook(backend);
    if (!done) if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
    return .{ .done = done, .units = limit - credits, .ns = elapsed };
}

fn note(backend: anytype, result: Slice) void {
    backend.ledger_reclaim_slices +|= 1;
    backend.ledger_reclaim_units +|= result.units;
    backend.ledger_reclaim_max_slice_ns = @max(backend.ledger_reclaim_max_slice_ns, result.ns);
}

fn fill(allocator: std.mem.Allocator, ledger: *Ledger, count: usize) !void {
    for (0..count) |i| {
        const path = try std.fmt.allocPrint(allocator, "retired-{d:0>8}.tbl", .{i});
        errdefer allocator.free(path);
        try ledger.append(allocator, .{ .path = path, .delete_after_ns = 0 });
    }
}

test "ledger reclamation snapshot capture and retirement survive every allocation failure" {
    const Fixture = struct {
        fn check(allocator: std.mem.Allocator) !void {
            const Backend = @import("../lsm_backend.zig").Backend;
            var backend = Backend.init(allocator, .{});
            defer backend.close();
            var source: Ledger = .empty;
            defer source.deinit(allocator);
            try fill(allocator, &source, 12);
            const snapshot = try Snapshot.capture(&backend, &source);
            // Mirror capture -> concurrent root replacement -> error unwind.
            source.deinit(allocator);
            snapshot.retire(&backend);
            try std.testing.expectEqual(@as(u64, 0), backend.ledger_reclaim_slices);
            backend.drainRetiredLedgers();
            try std.testing.expect(backend.ledger_snapshots == null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "ledger reclamation preserves administrative fences and drains under cancellation and pressure" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const resources = @import("../resource_manager.zig");
    const Hook = struct {
        var visits: usize = 0;
        var sleeps: usize = 0;
        var wakes: usize = 0;
        fn visit(ptr: *anyopaque) void {
            const backend: *Backend = @ptrCast(@alignCast(ptr));
            std.debug.assert(backend.mu.tryLock());
            defer backend.mu.unlock();
            std.debug.assert(backend.active_readers != 0);
            std.debug.assert(backend.ledger_snapshots != null);
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            std.debug.assert(backend.options.resource_manager.?.snapshot().memory.used_bytes >= @sizeOf(Snapshot));
            visits += 1;
        }
        fn sleep(_: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            sleeps += 1;
            return error.Canceled;
        }
        fn wake(_: *anyopaque) void {
            wakes += 1;
        }
    };
    Hook.visits = 0;
    Hook.sleeps = 0;
    Hook.wakes = 0;
    var io_vtable = std.testing.io.vtable.*;
    io_vtable.sleep = Hook.sleep;
    var io = std.testing.io;
    io.vtable = &io_vtable;
    var manager = resources.ResourceManager.init(.{});
    var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager, .read_runtime = .{ .io = io }, .maintenance_waker = .{ .ptr = &manager, .wake_fn = Hook.wake } });
    defer backend.close();
    var source: Ledger = .empty;
    defer source.deinit(std.testing.allocator);
    try fill(std.testing.allocator, &source, 10000);
    const snapshot = try Snapshot.capture(&backend, &source);
    source.deinit(std.testing.allocator);
    test_slice_hook = Hook.visit;
    defer test_slice_hook = null;
    try std.testing.expect(backend.mu.tryLock());
    snapshot.retire(&backend);
    // Capture/retire must not unlock an administrative publication fence.
    try std.testing.expectEqual(@as(usize, 0), Hook.visits);
    backend.active_bulk_ingest_batches = 1;
    backend.manifest_recovery_required = true;
    backend.mu.unlock();
    try std.testing.expectEqual(@as(?u64, 0), backend.nextMaintenanceWakeDelayNsBestEffort());
    for (0..1000) |_| {
        if (backend.retired_ledger_snapshots == null) break;
        _ = try backend.runMaintenanceStep();
    }
    try std.testing.expect(backend.ledger_snapshots == null);
    try std.testing.expect(Hook.visits > 1 and Hook.sleeps > 0);
    try std.testing.expect(Hook.wakes > 0);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "ledger reclamation checkpoint success and failure retire churned snapshots after publication" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Memory = @import("storage_io.zig").MemoryStorage;
    const repository = @import("repository.zig");
    const Hook = struct {
        backend: *Backend,
        fail: bool,
        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.backend.mu.tryLock());
            defer self.backend.mu.unlock();
            // Simulate a concurrent ledger replacement while the checkpoint
            // owns its captured root. The captured ledger is now the last
            // owner once publication retires the previous journal root.
            self.backend.obsolete_paths.deinit(self.backend.allocator);
            self.backend.obsolete_manifest_dirty = true;
            try self.backend.persistManifestLocked();
            if (self.fail) return error.InjectedCheckpointFailure;
        }
    };
    for ([_]bool{ false, true }) |fail| {
        var memory = Memory.init(std.testing.allocator);
        defer memory.deinit();
        var backend = try Backend.open(std.testing.allocator, "/ledger-checkpoint-churn", .{ .storage = memory.storage(), .obsolete_retention_ns = std.time.ns_per_day });
        defer backend.close();
        {
            const locked = runtime.lockBackend(Backend, &backend);
            defer runtime.unlockBackend(Backend, &backend, locked);
            try fill(std.testing.allocator, &backend.obsolete_paths, 10000);
            backend.obsolete_manifest_dirty = true;
            try backend.persistManifestLocked();
        }
        backend.drainRetiredLedgers();
        var hook = Hook{ .backend = &backend, .fail = fail };
        {
            const locked = runtime.lockBackend(Backend, &backend);
            defer runtime.unlockBackend(Backend, &backend, locked);
            repository.checkpoint_test_hook = .{ .context = &hook, .run = Hook.run };
            defer repository.checkpoint_test_hook = null;
            if (fail) {
                try std.testing.expectError(error.InjectedCheckpointFailure, backend.manifest_journal.runCheckpoint(&backend));
            } else try std.testing.expect(try backend.manifest_journal.runCheckpoint(&backend));
            // The last-owner traversal is still queued at function return,
            // rather than being charged to the publication lock's defer path.
            try std.testing.expect(backend.retired_ledger_snapshots != null);
        }
        backend.drainRetiredLedgers();
        try std.testing.expect(backend.ledger_snapshots == null);
        try std.testing.expect(backend.ledger_reclaim_slices > 1);
    }
}

test "ledger reclamation services oldest snapshot while new retirements arrive off lock" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Hook = struct {
        var oldest: *Snapshot = undefined;
        var arriving: ?*Snapshot = null;
        var visited: bool = false;
        fn visit(ptr: *anyopaque) void {
            const backend: *Backend = @ptrCast(@alignCast(ptr));
            std.debug.assert(backend.mu.tryLock());
            defer backend.mu.unlock();
            if (arriving) |snapshot| {
                arriving = null;
                std.debug.assert(backend.retired_ledger_snapshots == oldest);
                snapshot.retire(backend);
                std.debug.assert(backend.retired_ledger_snapshots == oldest);
                std.debug.assert(backend.retired_ledger_tail == snapshot);
                const slices = backend.ledger_reclaim_slices;
                // Reentrant unlock/maintenance must not reclaim the same head.
                reclaimSliceLocked(backend);
                std.debug.assert(backend.ledger_reclaim_slices == slices);
                visited = true;
            }
        }
    };
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var source: Ledger = .empty;
    defer source.deinit(std.testing.allocator);
    try fill(std.testing.allocator, &source, 10000);
    const oldest = try Snapshot.capture(&backend, &source);
    oldest.retire(&backend);
    const arriving = try Snapshot.capture(&backend, &Ledger.empty);
    Hook.oldest = oldest;
    Hook.arriving = arriving;
    Hook.visited = false;
    source.deinit(std.testing.allocator);
    test_slice_hook = Hook.visit;
    defer test_slice_hook = null;
    backend.drainRetiredLedgers();
    try std.testing.expect(Hook.visited);
    try std.testing.expect(backend.retired_ledger_tail == null);
    try std.testing.expect(backend.active_readers == 0);
}

test "ledger reclamation checkpoint churn benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const Backend = @import("../lsm_backend.zig").Backend;
    // Measure production-style allocation, not debug allocator bookkeeping.
    // The correctness tests above separately retain leak/OOM instrumentation.
    const allocator = std.heap.smp_allocator;
    for ([_]usize{ 1000, 10000, 100000 }) |count| {
        var backend = Backend.init(allocator, .{});
        defer backend.close();
        var baseline: Ledger = .empty;
        defer baseline.deinit(allocator);
        try fill(allocator, &baseline, count);
        var old = baseline.fork();
        baseline.deinit(allocator);
        try std.testing.expect(backend.mu.tryLock());
        const old_start = std.Io.Clock.awake.now(std.testing.io);
        old.deinit(allocator);
        const old_ns = old_start.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds();
        backend.mu.unlock();
        try fill(allocator, &baseline, count);
        const snapshot = try Snapshot.capture(&backend, &baseline);
        baseline.deinit(allocator);
        try std.testing.expect(backend.mu.tryLock());
        const start = std.Io.Clock.awake.now(std.testing.io);
        snapshot.retire(&backend);
        const handoff_ns = start.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds();
        backend.mu.unlock();
        const cleanup = std.Io.Clock.awake.now(std.testing.io);
        backend.drainRetiredLedgers();
        const cleanup_ns = cleanup.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds();
        std.debug.print("\nledger-churn paths={d} old_locked_ns={d} handoff_ns={d} cleanup_ns={d} slices={d} max_slice_ns={d}\n", .{ count, old_ns, handoff_ns, cleanup_ns, backend.ledger_reclaim_slices, backend.ledger_reclaim_max_slice_ns });
    }
}

/// Consumes and clears the caller's header before unlocking. Error/cancellation
/// unwind has the same bounded, allocation-free path as successful publication.
pub fn drainLocked(backend: anytype, ledger: *Ledger) void {
    const owned = ledger.*;
    ledger.* = .empty;
    if (owned.tree.account == null) {
        std.debug.assert(owned.tree.root == null and owned.spare.capacity == 0 and owned.tree.spare.capacity == 0);
        return;
    }
    var job = Job{
        .next = backend.active_ledger_reclamations,
        .account = if (owned.tree.account) |account| account.retain() else null,
        .pool_bytes = owned.spare.capacity * @sizeOf(usize) + owned.tree.spare.capacity * @sizeOf(usize),
    };
    backend.active_ledger_reclamations = &job;
    backend.retainReaderKind(.other);
    var reclaimer = Ledger.Reclaimer.init(owned);
    while (true) {
        // Do not invoke general unlock reclamation here: its callbacks can
        // recursively publish while our caller still owns the manifest lane.
        backend.mu.unlock();
        const result = step(backend, &reclaimer, 2048, clock.monotonicNs() +| 2 * std.time.ns_per_ms);
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
        note(backend, result);
        if (result.done) break;
    }
    var link = &backend.active_ledger_reclamations;
    while (link.*.? != &job) link = &link.*.?.next;
    link.* = job.next;
    if (job.account) |account| account.release();
    backend.releaseReaderKind(.other);
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
}
