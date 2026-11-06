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

//! Generic `std.Io` crash-simulation shim: WS8's reusable fault-injection
//! library for Lite write-path crash-consistency tests
//! (docs/design/lite-performance/lite-single-commit-write-path.md §6).
//!
//! `SimIo` wraps any real `std.Io` by copying its vtable and overriding only
//! the handful of entry points that matter for durability: file open/create
//! (to learn the path behind a handle, since the vtable only ever hands
//! callers a bare file descriptor), `fileSync` (today's barrier -- a plain
//! `fsync`; tomorrow's `syncData` may be `fdatasync` on Linux or carry an
//! `F_FULLFSYNC` policy on macOS, but it is still just "the next call that
//! reaches `fileSync`" from this shim's point of view, so no caller change is
//! needed when that primitive changes), and `dirRename` / `dirRenamePreserve`
//! (atomic replace). Every one of those calls is logged as a `Barrier` with a
//! stable sequence number; callers enumerate a scenario's barriers once, then
//! re-run the same scenario asking `SimIo` to abort exactly before or after
//! barrier `k`, optionally following up with `applyPowerLoss` to additionally
//! drop writes that never reached a durable barrier (the stronger, optional
//! "the machine lost power" simulation; without it, a crash only stops the
//! sequence of calls, which is what an ordinary process crash -- as opposed to
//! a power cut -- already guarantees on a real filesystem).
//!
//! Plain writes, truncations, and reads are never barriers themselves and are
//! passed straight through to the real `Io`, so the scenario under test sees
//! completely ordinary behavior until the configured crash point.
//!
//! This file has no dependency on any particular Lite subsystem. It is meant
//! to be reused directly by other crash and recovery tests (native commit v8,
//! group commit, owner-queued retirement, ...), not just by
//! `crash_consistency_test.zig`.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = std.Io.Dir;
const File = std.Io.File;

/// Every kind of event this shim treats as a durable barrier. `file_sync`
/// covers `fsync`, `fdatasync`, and an `F_FULLFSYNC`-backed sync alike --
/// whatever the real `Io` implementation does inside `fileSync` is opaque to
/// this shim, which only observes the call boundary. `rename` covers both the
/// POSIX-replacing and non-replacing atomic renames. `dir_sync` is not yet
/// reachable through any call site in this codebase (nothing opens a
/// directory purely to fsync it today) but is reserved so a future call site
/// can call `Ledger.noteExternalBarrier(.dir_sync, path)` directly without a
/// shim change.
pub const BarrierKind = enum { file_sync, rename, dir_sync };

pub const Barrier = struct {
    sequence: u64,
    kind: BarrierKind,
    /// The path durability applies to. For `rename`, this is the new
    /// (destination) path; `old_path` carries the source.
    path: []const u8,
    old_path: ?[]const u8 = null,
};

pub const When = enum { before, after };

pub const PowerLoss = enum {
    /// A crash only stops the sequence of calls. Every byte already written
    /// through a real `write` syscall survives, synced or not -- matching an
    /// ordinary process crash (not a power cut) on a real filesystem.
    none,
    /// After the crash, every file is rolled back to its last durably-synced
    /// byte image (or deleted outright if it was never synced), modeling an
    /// actual power loss where the page cache's unsynced contents are gone.
    drop_unsynced,
};

pub const CrashPlan = struct {
    at_barrier: u64,
    when: When,
    power_loss: PowerLoss = .drop_unsynced,
};

/// Which overridden call a `IoFaultPlan` targets.
pub const IoFaultKind = enum(u2) { write, sync, create };

/// A single transient I/O failure, injected deterministically without any
/// crash: `at_op`-th call of `kind` (0-based, counted independently per
/// kind) returns `err` instead of reaching the real `Io`. Every call before
/// and (unless `repeat`) after that one behaves completely normally --
/// nothing here ever marks the shim `crashed`, so the scenario's own
/// recovery/retry logic is exactly what has to clear the fault, not
/// `applyPowerLoss`.
pub const IoFaultPlan = struct {
    at_op: u64,
    kind: IoFaultKind,
    /// `error.NoSpaceLeft` (a full disk) or `error.InputOutput` (a
    /// transient read/write fault) are the two this models; both are valid
    /// members of every error set the three injection sites return.
    err: anyerror,
    /// `false` (the default): fires exactly once, at `at_op`, then the
    /// plan clears itself and every later call of `kind` succeeds
    /// normally (a disk that was briefly full or glitched, then recovered).
    /// `true`: fires at `at_op` and every call after it (a disk that stays
    /// full/broken until the test explicitly clears the plan), useful for
    /// proving a caller backs off instead of busy-looping.
    repeat: bool = false,
};

const HandleEntry = struct {
    path: []u8,
};

/// `std.atomic.Mutex` (this Zig version's primitive) only exposes
/// `tryLock`/`unlock`, not a blocking `lock`; this is the same spin-wrapper
/// `storage/sim_runtime.zig`'s `SpinMutex` uses, duplicated here so this file
/// keeps zero dependencies beyond `std` and stays testable with a plain
/// `zig test` outside the full build graph.
const SpinMutex = struct {
    inner: std.atomic.Mutex = .unlocked,

    fn lock(self: *SpinMutex) void {
        while (!self.inner.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *SpinMutex) void {
        self.inner.unlock();
    }
};

/// Generic fault-injecting `std.Io` wrapper. Heap-allocate with `init` (the
/// returned pointer must stay stable for the lifetime of any `Io` value
/// handed out by `io()`) and free with `deinit`.
pub const SimIo = struct {
    allocator: Allocator,
    real: Io,
    vtable: Io.VTable,

    /// fd (File or Dir handle, both `std.posix.fd_t`) -> logical path. Paths
    /// are exactly the strings callers passed to `dirCreateFile`/`dirOpenFile`
    /// (every call site in `native.zig` opens through `Dir.cwd()` with an
    /// already-complete path, so the sub_path string alone is the identity;
    /// this shim never needs to resolve directory-relative paths itself).
    handle_paths: std.AutoHashMapUnmanaged(File.Handle, HandleEntry) = .empty,

    /// path -> last durably-synced byte image, or `null` if the path has
    /// never been synced (and should therefore not exist at all once power
    /// loss is simulated). Entries are added the first time a path is
    /// created/opened and updated on every successful `fileSync`/rename.
    durable: std.StringHashMapUnmanaged(?[]u8) = .empty,

    /// Paths known to be directories, discovered the first time a `fileSync`
    /// on them hits `error.IsDir` while this shim tries to snapshot their
    /// bytes (POSIX directory durability is commonly implemented by opening
    /// the directory itself as a read-only file purely to `fsync` its fd --
    /// `fs_paths.syncDirPortable` does exactly this). A directory has no byte
    /// content to roll back, so it is excluded from `durable`/power-loss
    /// restoration entirely; the barrier is still recorded.
    directories: std.StringHashMapUnmanaged(void) = .empty,

    barriers: std.ArrayListUnmanaged(Barrier) = .empty,
    next_sequence: u64 = 0,

    crash_plan: ?CrashPlan = null,
    crashed: bool = false,
    crash_barrier: ?Barrier = null,

    /// Latency-injection, independent of any crash plan: when non-zero,
    /// every `fileSync` sleeps this many nanoseconds around the real
    /// syscall before this shim does anything else with the result.
    /// Models an ordinary slow disk under load -- no fault, no crash, just
    /// a sync that takes longer than usual -- to make "a slow sync alone
    /// must never corrupt durability bookkeeping" deterministically
    /// testable instead of relying on incidental machine load.
    sync_delay_ns: u64 = 0,

    /// Error-injection, independent of any crash plan: unlike `crash_plan`,
    /// firing this never marks `self.crashed` and never aborts the
    /// scenario's own control flow -- the process keeps running, exactly as
    /// it would after a real, transient `EIO`/`ENOSPC` that a disk recovers
    /// from moments later. This is what makes "heavy machine load, disk
    /// near full" reproducible on demand instead of incidental: the
    /// `[uncrashed run]` crash-sweep pass that first surfaced
    /// `IndexRebuilding`-forever (see crash_consistency_test.zig's "crash
    /// sweep: full text synchronous visibility") needed exactly this -- an
    /// ordinary write/sync failing once, with no crash at all, to prove the
    /// owner reconciles and resumes instead of latching forever.
    io_fault_plan: ?IoFaultPlan = null,
    /// Per-kind count of matching calls observed so far, indexed by
    /// `@intFromEnum(IoFaultKind)`. Counts every call of that kind,
    /// including ones before the plan's `at_op` and (for a non-repeating
    /// plan) ones after it fired once.
    io_fault_op_counts: [3]u64 = @splat(0),

    /// Guards every field above. A scenario that opens a `db_mod.DB` can
    /// spawn a background maintenance task via `io.concurrent(...)`, which
    /// calls back into this shim's overridden vtable entries from a second
    /// thread; without this, two threads racing on `barriers`/`handle_paths`
    /// would corrupt them.
    mutex: SpinMutex = .{},

    pub fn init(allocator: Allocator, real: Io) !*SimIo {
        const self = try allocator.create(SimIo);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .real = real,
            .vtable = real.vtable.*,
        };
        self.vtable.dirCreateFile = vtDirCreateFile;
        self.vtable.dirOpenFile = vtDirOpenFile;
        self.vtable.fileClose = vtFileClose;
        self.vtable.fileSync = vtFileSync;
        self.vtable.dirRename = vtDirRename;
        self.vtable.dirRenamePreserve = vtDirRenamePreserve;
        self.vtable.fileWritePositional = vtFileWritePositional;
        try registerSelf(real.userdata, self);
        return self;
    }

    pub fn deinit(self: *SimIo) void {
        unregisterSelf(self.real.userdata);
        const a = self.allocator;
        var handle_it = self.handle_paths.valueIterator();
        while (handle_it.next()) |entry| a.free(entry.path);
        self.handle_paths.deinit(a);

        var durable_it = self.durable.iterator();
        while (durable_it.next()) |entry| {
            a.free(entry.key_ptr.*);
            if (entry.value_ptr.*) |bytes| a.free(bytes);
        }
        self.durable.deinit(a);

        var dir_it = self.directories.keyIterator();
        while (dir_it.next()) |key| a.free(key.*);
        self.directories.deinit(a);

        for (self.barriers.items) |barrier| {
            a.free(barrier.path);
            if (barrier.old_path) |old| a.free(old);
        }
        self.barriers.deinit(a);

        a.destroy(self);
    }

    /// The returned `Io` keeps the *real* underlying `userdata` untouched --
    /// every vtable entry this shim does not override must still receive
    /// exactly the pointer the real implementation expects (its own runtime
    /// state, e.g. a thread pool), not this `SimIo`. Overridden entries
    /// recover their `*SimIo` from a side table keyed by that same real
    /// `userdata` instead of smuggling it through `Io.userdata`.
    pub fn io(self: *SimIo) Io {
        return .{ .userdata = self.real.userdata, .vtable = &self.vtable };
    }

    pub fn setCrashPlan(self: *SimIo, plan: ?CrashPlan) void {
        self.crash_plan = plan;
        self.crashed = false;
        self.crash_barrier = null;
    }

    /// Sets (or clears, with 0) the per-sync latency injected by every
    /// `fileSync` call. Independent of `crash_plan`: this models slowness
    /// alone, never a fault.
    pub fn setSyncDelayNs(self: *SimIo, ns: u64) void {
        self.sync_delay_ns = ns;
    }

    /// Sets (or clears, with `null`) the deterministic I/O fault plan. See
    /// `IoFaultPlan`. Also resets every kind's op counter to 0, so a test
    /// that sets a plan always counts from a clean slate regardless of
    /// what ran before (e.g. a scenario's earlier enumerate-only pass).
    pub fn setIoFaultPlan(self: *SimIo, plan: ?IoFaultPlan) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.io_fault_plan = plan;
        self.io_fault_op_counts = @splat(0);
    }

    pub fn barrierCount(self: *const SimIo) u64 {
        return self.barriers.items.len;
    }

    pub fn barrierAt(self: *const SimIo, sequence: u64) ?Barrier {
        for (self.barriers.items) |barrier| {
            if (barrier.sequence == sequence) return barrier;
        }
        return null;
    }

    /// Records a barrier this shim could not observe directly (e.g. a
    /// directory fsync done through a platform-specific primitive outside
    /// `std.Io`). Forward-compatibility hook; unused by any current call
    /// site. Returns whether the configured crash plan fired.
    pub fn noteExternalBarrier(self: *SimIo, kind: BarrierKind, path: []const u8) !bool {
        return self.recordBarrier(kind, path, null);
    }

    /// Rolls every tracked path back to its last durable image, deleting
    /// paths that were never synced. Call after a run aborts with the
    /// injected crash error and before reopening the database to check
    /// post-crash invariants. No-op if no crash has fired yet.
    pub fn applyPowerLoss(self: *SimIo) !void {
        if (!self.crashed) return;
        var it = self.durable.iterator();
        while (it.next()) |entry| {
            const path = entry.key_ptr.*;
            if (entry.value_ptr.*) |snapshot| {
                var file = try Dir.cwd().createFile(self.real, path, .{ .read = true, .truncate = true });
                defer file.close(self.real);
                try file.writePositionalAll(self.real, snapshot, 0);
            } else {
                Dir.cwd().deleteFile(self.real, path) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
            }
        }
    }

    // -- internals --------------------------------------------------------

    fn recordBarrier(self: *SimIo, kind: BarrierKind, path: []const u8, old_path: ?[]const u8) !bool {
        const sequence = self.next_sequence;
        self.next_sequence += 1;
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        const owned_old = if (old_path) |p| try self.allocator.dupe(u8, p) else null;
        errdefer if (owned_old) |p| self.allocator.free(p);
        try self.barriers.append(self.allocator, .{
            .sequence = sequence,
            .kind = kind,
            .path = owned_path,
            .old_path = owned_old,
        });

        const plan = self.crash_plan orelse return false;
        if (plan.at_barrier != sequence) return false;
        if (plan.when != .before) return false;
        self.crashed = true;
        self.crash_barrier = self.barriers.items[self.barriers.items.len - 1];
        return true;
    }

    fn maybeArmAfter(self: *SimIo, sequence: u64) bool {
        const plan = self.crash_plan orelse return false;
        if (plan.at_barrier != sequence or plan.when != .after) return false;
        self.crashed = true;
        self.crash_barrier = self.barrierAt(sequence);
        return true;
    }

    fn pathOf(self: *const SimIo, handle: File.Handle) []const u8 {
        return if (self.handle_paths.get(handle)) |entry| entry.path else "<untracked>";
    }

    fn ensureDurableEntry(self: *SimIo, path: []const u8) !void {
        if (self.durable.contains(path)) return;
        const owned = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned);
        try self.durable.put(self.allocator, owned, null);
    }

    /// Reads the full current contents of `file`, with `self.mutex` *not*
    /// held: this is the potentially large, potentially slow real I/O that
    /// must never run while blocking every other thread's unrelated vtable
    /// calls (see the long comment in `vtFileSync`). Returns `null` if
    /// `file` turns out to be a directory (nothing byte-level to capture);
    /// the caller commits the result -- including the directory case --
    /// via `commitDurableSnapshot`, under the lock.
    fn readDurableSnapshot(self: *SimIo, file: File) !?[]u8 {
        const len = try self.real.vtable.fileLength(self.real.userdata, file);
        const bytes = try self.allocator.alloc(u8, len);
        errdefer self.allocator.free(bytes);
        var filled: usize = 0;
        while (filled < bytes.len) {
            const got = self.real.vtable.fileReadPositional(self.real.userdata, file, &.{bytes[filled..]}, filled) catch |err| switch (err) {
                // A directory fsync'd by opening it as a read-only file hits
                // this on the first read attempt; it is not a real fault.
                // There is nothing byte-level to snapshot.
                error.IsDir => {
                    self.allocator.free(bytes);
                    return null;
                },
                else => return err,
            };
            if (got == 0) break;
            filled += got;
        }
        return bytes[0..filled];
    }

    /// Commits a snapshot captured by `readDurableSnapshot` into
    /// `self.durable` (or, for `null`, records `path` as a directory).
    /// Caller must hold `self.mutex` and, on error, still owns `captured`.
    fn commitDurableSnapshot(self: *SimIo, path: []const u8, captured: ?[]u8) !void {
        const bytes = captured orelse {
            try self.markDirectory(path);
            return;
        };
        const gop = try self.durable.getOrPut(self.allocator, path);
        if (!gop.found_existing) gop.key_ptr.* = try self.allocator.dupe(u8, path);
        if (gop.value_ptr.*) |old| self.allocator.free(old);
        gop.value_ptr.* = bytes;
    }

    fn markDirectory(self: *SimIo, path: []const u8) !void {
        if (!self.directories.contains(path)) {
            const owned = try self.allocator.dupe(u8, path);
            errdefer self.allocator.free(owned);
            try self.directories.put(self.allocator, owned, {});
        }
        if (self.durable.fetchRemove(path)) |removed| {
            self.allocator.free(removed.key);
            if (removed.value) |bytes| self.allocator.free(bytes);
        }
    }

    fn renameDurable(self: *SimIo, old_path: []const u8, new_path: []const u8) !void {
        const removed = self.durable.fetchRemove(old_path);
        const owned_new = try self.allocator.dupe(u8, new_path);
        errdefer self.allocator.free(owned_new);
        if (removed) |entry| {
            self.allocator.free(entry.key);
            const gop = try self.durable.getOrPut(self.allocator, owned_new);
            if (gop.found_existing) {
                self.allocator.free(owned_new);
                if (gop.value_ptr.*) |old| self.allocator.free(old);
            } else {
                gop.key_ptr.* = owned_new;
            }
            gop.value_ptr.* = entry.value;
        } else {
            try self.ensureDurableEntry(new_path);
            self.allocator.free(owned_new);
        }
        var handle_it = self.handle_paths.valueIterator();
        while (handle_it.next()) |entry| {
            if (std.mem.eql(u8, entry.path, old_path)) {
                self.allocator.free(entry.path);
                entry.path = try self.allocator.dupe(u8, new_path);
            }
        }
    }
};

/// Side table from a real `Io`'s `userdata` to the `SimIo` wrapping it.
/// `std.testing.io` (and most real `Io` implementations) share one
/// process-wide `userdata` pointer, so this only needs to support one active
/// `SimIo` per distinct real `userdata` at a time -- true for every scenario
/// in this package, which runs its enumerate/replay passes sequentially.
var registry_mutex: SpinMutex = .{};
var registry: std.AutoHashMapUnmanaged(?*anyopaque, *SimIo) = .empty;
var registry_allocator: Allocator = std.heap.page_allocator;

fn registerSelf(real_userdata: ?*anyopaque, self: *SimIo) !void {
    registry_mutex.lock();
    defer registry_mutex.unlock();
    try registry.put(registry_allocator, real_userdata, self);
}

fn unregisterSelf(real_userdata: ?*anyopaque) void {
    registry_mutex.lock();
    defer registry_mutex.unlock();
    _ = registry.remove(real_userdata);
}

fn selfFrom(userdata: ?*anyopaque) *SimIo {
    registry_mutex.lock();
    defer registry_mutex.unlock();
    return registry.get(userdata).?;
}

fn simDelayNs(self: *SimIo) u64 {
    self.mutex.lock();
    defer self.mutex.unlock();
    return self.sync_delay_ns;
}

/// Returns the configured fault's error if this call is the `at_op`-th
/// (0-based) call of `kind` since the plan was set, else `null`. Never
/// touches `crash_plan`/`crashed`: this is a plain, recoverable failure the
/// real `Io` could have produced on its own, not a crash.
fn maybeInjectIoFault(self: *SimIo, kind: IoFaultKind) ?anyerror {
    self.mutex.lock();
    defer self.mutex.unlock();
    const plan = self.io_fault_plan orelse return null;
    if (plan.kind != kind) return null;
    const slot = &self.io_fault_op_counts[@backingInt(kind)];
    const count = slot.*;
    slot.* += 1;
    if (count < plan.at_op) return null;
    if (count > plan.at_op and !plan.repeat) return null;
    if (!plan.repeat) self.io_fault_plan = null;
    return plan.err;
}

fn vtDirCreateFile(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, opts: Dir.CreateFileOptions) File.OpenError!File {
    const self = selfFrom(userdata);
    if (maybeInjectIoFault(self, .create)) |err| return @errorCast(err);
    const delay_ns = simDelayNs(self);
    if (delay_ns != 0) std.Io.sleep(self.real, .fromNanoseconds(@intCast(delay_ns)), .awake) catch {};
    const file = try self.real.vtable.dirCreateFile(self.real.userdata, dir, sub_path, opts);
    self.mutex.lock();
    defer self.mutex.unlock();
    trackOpenedFile(self, file, sub_path) catch {};
    return file;
}

fn vtDirOpenFile(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, opts: Dir.OpenFileOptions) File.OpenError!File {
    const self = selfFrom(userdata);
    const delay_ns = simDelayNs(self);
    if (delay_ns != 0) std.Io.sleep(self.real, .fromNanoseconds(@intCast(delay_ns)), .awake) catch {};
    const file = try self.real.vtable.dirOpenFile(self.real.userdata, dir, sub_path, opts);
    self.mutex.lock();
    defer self.mutex.unlock();
    trackOpenedFile(self, file, sub_path) catch {};
    return file;
}

fn vtFileClose(userdata: ?*anyopaque, files: []const File) void {
    const self = selfFrom(userdata);
    {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (files) |file| {
            if (self.handle_paths.fetchRemove(file.handle)) |entry| self.allocator.free(entry.value.path);
        }
    }
    self.real.vtable.fileClose(self.real.userdata, files);
}

fn vtFileSync(userdata: ?*anyopaque, file: File) File.SyncError!void {
    const self = selfFrom(userdata);
    if (maybeInjectIoFault(self, .sync)) |err| return @errorCast(err);

    self.mutex.lock();
    const path = self.allocator.dupe(u8, self.pathOf(file.handle)) catch {
        self.mutex.unlock();
        return error.InputOutput;
    };
    const my_sequence = self.next_sequence;
    const crash_before = self.recordBarrier(.file_sync, path, null) catch {
        self.mutex.unlock();
        self.allocator.free(path);
        return error.InputOutput;
    };
    // Only a run with a crash plan can ever call `applyPowerLoss` (a no-op
    // when `!self.crashed`, which requires `crash_plan != null`), so a run
    // with none configured -- most importantly every scenario's discovery
    // pass, which exists purely to enumerate barriers and prove the
    // scenario well-formed uncrashed -- has no use for a durable snapshot
    // at all. `crash_plan` is set once, single-threaded, before any
    // concurrent activity begins, so caching it here (still under the lock,
    // alongside the barrier bookkeeping above) is safe.
    const needs_snapshot = self.crash_plan != null and !self.directories.contains(path);
    const delay_ns = self.sync_delay_ns;
    self.mutex.unlock();
    defer self.allocator.free(path);
    if (crash_before) return error.InputOutput;

    // Latency injection: models an ordinary slow disk, not a fault. This
    // must never itself produce anything other than an eventual, ordinary
    // success -- no timeout, no cancellation, nothing that could be
    // mistaken for a real I/O error.
    if (delay_ns != 0) std.Io.sleep(self.real, .fromNanoseconds(@intCast(delay_ns)), .awake) catch {};

    try self.real.vtable.fileSync(self.real.userdata, file);

    // The expensive part -- an O(file-size) read, to capture the byte image
    // a crash-configured run might later need to roll back to via
    // `applyPowerLoss` -- must run with `self.mutex` released. Every other
    // overridden vtable entry, from every thread (a db_mod.DB's own
    // background derived-worker tasks spawned via `io.concurrent(...)`
    // included), takes this same lock for its own bookkeeping; holding it
    // across a potentially large read serializes and stalls *unrelated*
    // file opens/closes/syncs/renames far more than the real, unwrapped
    // `io` ever would -- a direct violation of this file's own contract
    // ("the scenario under test sees completely ordinary behavior until
    // the configured crash point"). Doing this inline under the lock was
    // reproducibly responsible for spurious `IndexRebuilding`-forever
    // failures in the full-text sync-visibility scenario that never occur
    // against the real `std.Io` (see crash_consistency_test.zig's "crash
    // sweep: full text synchronous visibility" test).
    if (needs_snapshot) {
        const captured = self.readDurableSnapshot(file) catch return error.InputOutput;
        self.mutex.lock();
        defer self.mutex.unlock();
        self.commitDurableSnapshot(path, captured) catch {
            if (captured) |bytes| self.allocator.free(bytes);
            return error.InputOutput;
        };
    }

    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.maybeArmAfter(my_sequence)) return error.InputOutput;
}

/// Latency injection only: writes are never barriers (see this file's own
/// doc comment -- "plain writes ... are never barriers themselves"), so
/// this never records anything or touches `crash_plan`. It only sleeps
/// (when a delay is configured) around the real write, to model a slow
/// disk's write path, not just its fsync path.
fn vtFileWritePositional(userdata: ?*anyopaque, file: File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) File.WritePositionalError!usize {
    const self = selfFrom(userdata);
    if (maybeInjectIoFault(self, .write)) |err| return @errorCast(err);
    const delay_ns = simDelayNs(self);
    if (delay_ns != 0) std.Io.sleep(self.real, .fromNanoseconds(@intCast(delay_ns)), .awake) catch {};
    return self.real.vtable.fileWritePositional(self.real.userdata, file, header, data, splat, offset);
}

fn vtDirRename(userdata: ?*anyopaque, old_dir: Dir, old_sub_path: []const u8, new_dir: Dir, new_sub_path: []const u8) Dir.RenameError!void {
    const self = selfFrom(userdata);
    self.mutex.lock();
    defer self.mutex.unlock();

    const crash_before = self.recordBarrier(.rename, new_sub_path, old_sub_path) catch return error.AccessDenied;
    if (crash_before) return error.AccessDenied;

    try self.real.vtable.dirRename(self.real.userdata, old_dir, old_sub_path, new_dir, new_sub_path);
    self.renameDurable(old_sub_path, new_sub_path) catch return error.AccessDenied;

    if (self.maybeArmAfter(self.next_sequence - 1)) return error.AccessDenied;
}

fn vtDirRenamePreserve(userdata: ?*anyopaque, old_dir: Dir, old_sub_path: []const u8, new_dir: Dir, new_sub_path: []const u8) Dir.RenamePreserveError!void {
    const self = selfFrom(userdata);
    self.mutex.lock();
    defer self.mutex.unlock();

    const crash_before = self.recordBarrier(.rename, new_sub_path, old_sub_path) catch return error.AccessDenied;
    if (crash_before) return error.AccessDenied;

    try self.real.vtable.dirRenamePreserve(self.real.userdata, old_dir, old_sub_path, new_dir, new_sub_path);
    self.renameDurable(old_sub_path, new_sub_path) catch return error.AccessDenied;

    if (self.maybeArmAfter(self.next_sequence - 1)) return error.AccessDenied;
}

// Not part of the formal vtable surface; small helper shared by the two
// open/create overrides above.
fn trackOpenedFile(self: *SimIo, file: File, sub_path: []const u8) !void {
    const owned = try self.allocator.dupe(u8, sub_path);
    errdefer self.allocator.free(owned);
    if (self.handle_paths.fetchRemove(file.handle)) |old| self.allocator.free(old.value.path);
    try self.handle_paths.put(self.allocator, file.handle, .{ .path = owned });
    try self.ensureDurableEntry(sub_path);
}

test "SimIo records a file_sync barrier and reopening after a before-crash loses the unsynced write" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(std.testing.io, &buf);
    const dir_path = buf[0..dir_len];
    const path = try std.fmt.allocPrint(allocator, "{s}/sim.bin", .{dir_path});
    defer allocator.free(path);

    // Pass 1: discover the barrier count for "create, write, sync, write, sync".
    {
        const sim = try SimIo.init(allocator, std.testing.io);
        defer sim.deinit();
        var file = try Dir.cwd().createFile(sim.io(), path, .{ .read = true });
        try file.writePositionalAll(sim.io(), "first", 0);
        try file.sync(sim.io());
        try file.writePositionalAll(sim.io(), "second", 5);
        try file.sync(sim.io());
        file.close(sim.io());
        try std.testing.expectEqual(@as(u64, 2), sim.barrierCount());
    }
    try Dir.cwd().deleteFile(std.testing.io, path);

    // Pass 2: crash before the second sync, then simulate power loss. The
    // "second" bytes were written but never synced, so they must not survive.
    {
        const sim = try SimIo.init(allocator, std.testing.io);
        defer sim.deinit();
        sim.setCrashPlan(.{ .at_barrier = 1, .when = .before, .power_loss = .drop_unsynced });
        var file = try Dir.cwd().createFile(sim.io(), path, .{ .read = true });
        try file.writePositionalAll(sim.io(), "first", 0);
        try file.sync(sim.io());
        try file.writePositionalAll(sim.io(), "second", 5);
        try std.testing.expectError(error.InputOutput, file.sync(sim.io()));
        file.close(sim.io());
        try std.testing.expect(sim.crashed);
        try sim.applyPowerLoss();
    }

    var reopened = try Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_only });
    defer reopened.close(std.testing.io);
    const len = try reopened.length(std.testing.io);
    try std.testing.expectEqual(@as(u64, 5), len);
    var contents: [5]u8 = undefined;
    _ = try reopened.readPositionalAll(std.testing.io, &contents, 0);
    try std.testing.expectEqualStrings("first", &contents);

    try Dir.cwd().deleteFile(std.testing.io, path);
}

test "SimIo after-crash keeps the synced write durable" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(std.testing.io, &buf);
    const dir_path = buf[0..dir_len];
    const path = try std.fmt.allocPrint(allocator, "{s}/sim-after.bin", .{dir_path});
    defer allocator.free(path);

    const sim = try SimIo.init(allocator, std.testing.io);
    defer sim.deinit();
    sim.setCrashPlan(.{ .at_barrier = 0, .when = .after, .power_loss = .drop_unsynced });
    var file = try Dir.cwd().createFile(sim.io(), path, .{ .read = true });
    try file.writePositionalAll(sim.io(), "durable", 0);
    try std.testing.expectError(error.InputOutput, file.sync(sim.io()));
    file.close(sim.io());
    try std.testing.expect(sim.crashed);
    try sim.applyPowerLoss();

    var reopened = try Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_only });
    defer reopened.close(std.testing.io);
    var contents: [7]u8 = undefined;
    _ = try reopened.readPositionalAll(std.testing.io, &contents, 0);
    try std.testing.expectEqualStrings("durable", &contents);

    try Dir.cwd().deleteFile(std.testing.io, path);
}

test "SimIo io fault plan injects one transient error without crashing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(std.testing.io, &buf);
    const dir_path = buf[0..dir_len];
    const path = try std.fmt.allocPrint(allocator, "{s}/sim-fault.bin", .{dir_path});
    defer allocator.free(path);

    const sim = try SimIo.init(allocator, std.testing.io);
    defer sim.deinit();
    // The second fileSync (index 1) fails once with NoSpaceLeft, modeling a
    // disk that was briefly full. No crash plan is set: `sim.crashed` must
    // stay false throughout, and the very next sync -- an ordinary retry,
    // not a reopen -- must succeed.
    sim.setIoFaultPlan(.{ .at_op = 1, .kind = .sync, .err = error.NoSpaceLeft });

    var file = try Dir.cwd().createFile(sim.io(), path, .{ .read = true });
    defer file.close(sim.io());
    try file.writePositionalAll(sim.io(), "first", 0);
    try file.sync(sim.io()); // sync #0: unaffected.
    try file.writePositionalAll(sim.io(), "second", 5);
    try std.testing.expectError(error.NoSpaceLeft, file.sync(sim.io())); // sync #1: fault fires.
    try std.testing.expect(!sim.crashed);
    try file.sync(sim.io()); // sync #2: the plan already cleared itself; retry succeeds.
    try std.testing.expect(!sim.crashed);

    try Dir.cwd().deleteFile(std.testing.io, path);
}

test "SimIo io fault plan can repeat until the test clears it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(std.testing.io, &buf);
    const dir_path = buf[0..dir_len];
    const path = try std.fmt.allocPrint(allocator, "{s}/sim-fault-repeat.bin", .{dir_path});
    defer allocator.free(path);

    const sim = try SimIo.init(allocator, std.testing.io);
    defer sim.deinit();
    sim.setIoFaultPlan(.{ .at_op = 0, .kind = .write, .err = error.InputOutput, .repeat = true });

    var file = try Dir.cwd().createFile(sim.io(), path, .{ .read = true });
    defer file.close(sim.io());
    try std.testing.expectError(error.InputOutput, file.writePositionalAll(sim.io(), "a", 0));
    try std.testing.expectError(error.InputOutput, file.writePositionalAll(sim.io(), "a", 0));
    try std.testing.expect(!sim.crashed);
    sim.setIoFaultPlan(null);
    try file.writePositionalAll(sim.io(), "a", 0); // Recovers once the plan clears.
}
