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

    fn captureDurableSnapshot(self: *SimIo, file: File, path: []const u8) !void {
        if (self.directories.contains(path)) return;
        const len = try self.real.vtable.fileLength(self.real.userdata, file);
        const bytes = try self.allocator.alloc(u8, len);
        errdefer self.allocator.free(bytes);
        var filled: usize = 0;
        while (filled < bytes.len) {
            const got = self.real.vtable.fileReadPositional(self.real.userdata, file, &.{bytes[filled..]}, filled) catch |err| switch (err) {
                // A directory fsync'd by opening it as a read-only file hits
                // this on the first read attempt; it is not a real fault.
                // There is nothing byte-level to snapshot, so forget this
                // path ever looked like a regular, syncable file.
                error.IsDir => {
                    self.allocator.free(bytes);
                    try self.markDirectory(path);
                    return;
                },
                else => return err,
            };
            if (got == 0) break;
            filled += got;
        }
        const gop = try self.durable.getOrPut(self.allocator, path);
        if (!gop.found_existing) gop.key_ptr.* = try self.allocator.dupe(u8, path);
        if (gop.value_ptr.*) |old| self.allocator.free(old);
        gop.value_ptr.* = bytes[0..filled];
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

fn vtDirCreateFile(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, opts: Dir.CreateFileOptions) File.OpenError!File {
    const self = selfFrom(userdata);
    const file = try self.real.vtable.dirCreateFile(self.real.userdata, dir, sub_path, opts);
    self.mutex.lock();
    defer self.mutex.unlock();
    trackOpenedFile(self, file, sub_path) catch {};
    return file;
}

fn vtDirOpenFile(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, opts: Dir.OpenFileOptions) File.OpenError!File {
    const self = selfFrom(userdata);
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
    self.mutex.lock();
    defer self.mutex.unlock();

    const path = self.allocator.dupe(u8, self.pathOf(file.handle)) catch return error.InputOutput;
    defer self.allocator.free(path);

    const crash_before = self.recordBarrier(.file_sync, path, null) catch return error.InputOutput;
    if (crash_before) return error.InputOutput;

    try self.real.vtable.fileSync(self.real.userdata, file);
    self.captureDurableSnapshot(file, path) catch return error.InputOutput;

    if (self.maybeArmAfter(self.next_sequence - 1)) return error.InputOutput;
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
