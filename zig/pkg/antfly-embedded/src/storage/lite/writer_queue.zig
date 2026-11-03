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

//! First-come-first-served admission in front of the cross-process writer
//! `flock`. A kernel `flock` queue has no ordering: on unlock every blocked
//! waiter races, and the process that just released usually wins again. This
//! module orders writers with a CLH-style queue of ticket files.
//!
//! Layout, in `<sidecar>.q/`:
//! - `tail`: an 8-byte little-endian next-ticket counter. Its own `flock` is
//!   held only for the few syscalls that issue a ticket.
//! - `t-<ticket>`: one file per waiter. The waiter holds an exclusive `flock`
//!   on it from enqueue until it finishes, and keeps its state in byte 0.
//!
//! A waiter blocks only on the ticket directly ahead of it, so each unlock
//! wakes exactly one process. Liveness comes from the kernel: when a process
//! dies, its ticket lock is dropped and the successor reads the state it left
//! behind. Ticket numbers are never reused, so there is no ABA.
//!
//! States: `W` waiting, `G` granted (holds or is acquiring the writer lease),
//! `D` done, `A` abandoned (deadline expired before its turn). A successor
//! that acquires a predecessor's lock treats `G`/`D` as "my turn" and `W`/`A`
//! as "skip it": an unlocked `W` belongs to a waiter that died. A granted
//! waiter deletes every ticket below its own; FIFO order guarantees all of
//! them have finished, so a missing ticket file also means "my turn".
//!
//! The queue only orders admission. The sidecar `flock` stays the safety
//! authority, so binaries or code paths that bypass the queue can delay a
//! queued writer but cannot break mutual exclusion.
//!
//! Protocol rules (each has a test below):
//!
//! 1. Issue. A ticket is created and locked, and its `W` state written,
//!    before the counter advances, all under the tail lock. A walker can
//!    therefore never see a live ticket unlocked or without a state.
//! 2. Monotonic numbers. The counter is written whole under the tail lock and
//!    never decreases. Enqueue also takes the maximum with one more than the
//!    highest ticket file, so a missing, short or damaged counter can never
//!    number a ticket below a live one. A crash between creating a ticket and
//!    advancing the counter leaves an unlocked `W` file that no walker has
//!    seen; the next ticket is numbered above it and skips it as dead.
//! 3. No bypass. A walker skips a predecessor only after holding that
//!    predecessor's lock and reading `W` or `A`, which proves its owner has
//!    abandoned or died; it then continues to the next lower ticket, so it
//!    always ends up blocked on the nearest earlier live ticket. Abandoning
//!    writes `A` before unlocking.
//! 4. Grant. A walker is granted on reaching a `D` or `G` ticket (finished,
//!    or holder died), or a missing file. Files are deleted only by a granted
//!    ticket, and only below itself, so a missing file means a later grant
//!    already proved everything below it finished.
//! 5. Death. Process death releases all of its ticket and tail locks: a dead
//!    waiter reads as `W` (skipped), a dead holder as `G` (finished). Death
//!    while holding the tail lock leaves at most rule 2's unadvanced file.
//! 6. Cancellation. Any error or cancellation before the grant abandons the
//!    ticket (`A`, then unlock). After the grant the caller must `finish`
//!    (`D`, then unlock); errors after the grant release through `Turn`.
//! 7. One ticket per process per path. Callers hold the in-process path gate
//!    while queued, so a process never waits on its own ticket. This also
//!    keeps ordering correct where `flock` is emulated with per-process POSIX
//!    locks (NFS on Linux).
//! 8. Descriptors are opened close-on-exec. A child forked without exec would
//!    share a ticket lock until it exits; that only delays successors.
//! 9. Bypass by older binaries or direct `flock` users. They are not ordered,
//!    but they only make the granted ticket wait longer for the sidecar.
//!    Ordering among queued writers is unchanged.
//! 10. Like the sidecar, `<sidecar>.q/` must not be deleted while any process
//!    may have the database open.

const std = @import("std");
const path_lock_wait = @import("path_lock_wait.zig");

const State = enum(u8) {
    waiting = 'W',
    granted = 'G',
    done = 'D',
    abandoned = 'A',
    _,
};

const tail_name = "tail";
const ticket_name_len = "t-".len + 16;

fn ticketName(buf: *[ticket_name_len]u8, index: u64) []const u8 {
    return std.fmt.bufPrint(buf, "t-{x:0>16}", .{index}) catch unreachable;
}

pub const Queue = struct {
    dir: std.Io.Dir,

    /// Opens (creating if needed) the queue directory next to the sidecar.
    pub fn open(allocator: std.mem.Allocator, io: std.Io, sidecar_path: []const u8) !Queue {
        const queue_path = try std.fmt.allocPrint(allocator, "{s}.q", .{sidecar_path});
        defer allocator.free(queue_path);
        const cwd = std.Io.Dir.cwd();
        return .{ .dir = cwd.openDir(io, queue_path, .{}) catch |err| switch (err) {
            error.FileNotFound => blk: {
                cwd.createDir(io, queue_path, .default_dir) catch |create_err| switch (create_err) {
                    error.PathAlreadyExists => {},
                    else => return create_err,
                };
                break :blk try cwd.openDir(io, queue_path, .{});
            },
            else => return err,
        } };
    }

    pub fn close(self: *Queue, io: std.Io) void {
        self.dir.close(io);
        self.* = undefined;
    }

    /// Holds the tail lock. While held, no ticket can be issued.
    pub const TailGuard = struct {
        file: std.Io.File,
        next: u64,

        pub fn release(self: *TailGuard, io: std.Io) void {
            self.file.close(io);
            self.* = undefined;
        }
    };

    /// The tail critical section is a few syscalls, so a short kernel wait is
    /// fine here. The deadline still bounds it against a stopped process.
    pub fn lockTail(self: *Queue, io: std.Io, deadline_ns: i96) !TailGuard {
        const file = try self.dir.createFile(io, tail_name, .{ .read = true, .truncate = false });
        const held = blk: {
            if (file.tryLock(io, .exclusive) catch |err| {
                file.close(io);
                return err;
            }) break :blk file;
            break :blk path_lock_wait.lockUntil(file, io, .exclusive, deadline_ns) catch |err| switch (err) {
                // Without a concurrent executor, poll in short sleeps.
                error.ConcurrencyUnavailable => try pollLock(self.dir, io, tail_name, deadline_ns),
                else => return err,
            };
        };
        errdefer held.close(io);
        return .{ .file = held, .next = try self.readOrRecoverCounter(held, io) };
    }

    /// Rule 2. Only called with the tail lock held.
    fn readOrRecoverCounter(self: *Queue, file: std.Io.File, io: std.Io) !u64 {
        var raw: [8]u8 = @splat(0);
        const n = try file.readPositionalAll(io, &raw, 0);
        if (n == raw.len) return std.mem.readInt(u64, &raw, .little);
        return self.recoverCounter(io);
    }

    /// One more than the highest ticket file, or 0 with no tickets.
    fn recoverCounter(self: *Queue, io: std.Io) !u64 {
        var dir = try self.dir.openDir(io, ".", .{ .iterate = true });
        defer dir.close(io);
        var next: u64 = 0;
        var it = dir.iterate();
        while (try it.next(io)) |entry| {
            if (entry.name.len != ticket_name_len or !std.mem.startsWith(u8, entry.name, "t-")) continue;
            const index = std.fmt.parseInt(u64, entry.name[2..], 16) catch continue;
            next = @max(next, index +| 1);
        }
        return next;
    }

    /// Whether any ticket numbered `mark` or above has been issued. One open
    /// and an 8-byte read, without the tail lock: the counter only grows and
    /// is written in one 8-byte write, and a stale read only delays noticing.
    /// Errors answer "yes" so callers fall back to releasing.
    pub fn ticketsIssuedSince(self: *const Queue, io: std.Io, mark: u64) bool {
        const tail = self.dir.openFile(io, tail_name, .{}) catch return true;
        defer tail.close(io);
        const next = readCounter(tail, io) catch return true;
        return next > mark;
    }

    /// Like `lockTail` but returns `error.WouldBlock` instead of waiting.
    pub fn tryLockTail(self: *Queue, io: std.Io) !TailGuard {
        const file = try self.dir.createFile(io, tail_name, .{ .read = true, .truncate = false });
        errdefer file.close(io);
        if (!try file.tryLock(io, .exclusive)) return error.WouldBlock;
        return .{ .file = file, .next = try self.readOrRecoverCounter(file, io) };
    }

    /// True when nobody is queued or holding a queue grant. Caller holds the
    /// tail, so the answer stays valid until the tail is released.
    pub fn isIdle(self: *Queue, io: std.Io, tail: *const TailGuard) !bool {
        if (tail.next == 0) return true;
        var name_buf: [ticket_name_len]u8 = undefined;
        const last = self.dir.openFile(io, ticketName(&name_buf, tail.next - 1), .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => return true,
            else => return err,
        };
        defer last.close(io);
        // A live last waiter or holder keeps its lock.
        if (!try last.tryLock(io, .exclusive)) return false;
        defer last.unlock(io);
        // A dead or abandoned last waiter may still have live waiters ahead of
        // it. Only a finished (or dead-while-granted) last ticket proves FIFO
        // has drained.
        return switch (try readState(last, io)) {
            .done, .granted => true,
            else => false,
        };
    }

    /// Issues the next ticket while the caller holds the tail. The returned
    /// ticket is locked before the counter advances, so successors can never
    /// observe it unlocked while its owner is alive.
    pub fn enqueue(self: *Queue, io: std.Io, tail: *TailGuard) !Ticket {
        var name_buf: [ticket_name_len]u8 = undefined;
        // Rule 2: never number a ticket below an existing one, even if the
        // counter was damaged. Only contended admissions enqueue, and the
        // directory holds just live and recently finished tickets.
        tail.next = @max(tail.next, try self.recoverCounter(io));
        var recovered = false;
        const index, const name, const file = while (true) {
            const index = tail.next;
            const name = ticketName(&name_buf, index);
            // An unlocked leftover here is rule 2's unadvanced file: reuse it.
            const file = self.dir.createFile(io, name, .{
                .read = true,
                .truncate = true,
                .lock = .exclusive,
                .lock_nonblocking = true,
            }) catch |err| switch (err) {
                // A live ticket at the next number: the counter is behind.
                error.WouldBlock => {
                    if (recovered) return error.FileBusy;
                    recovered = true;
                    tail.next = @max(tail.next + 1, try self.recoverCounter(io));
                    continue;
                },
                else => return err,
            };
            break .{ index, name, file };
        };
        errdefer {
            file.close(io);
            self.dir.deleteFile(io, name) catch {};
        }
        try writeState(file, io, .waiting);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, index + 1, .little);
        try tail.file.writePositionalAll(io, &raw, 0);
        tail.next = index + 1;
        return .{ .index = index, .file = file };
    }

    /// Waits until every earlier ticket has finished. On `error.FileBusy` (the
    /// deadline expired) or any other error, the ticket is abandoned and
    /// closed; on success it is granted and must later be passed to `finish`.
    pub fn awaitTurn(self: *Queue, io: std.Io, ticket: *Ticket, deadline_ns: i96) !void {
        errdefer self.abandon(io, ticket);
        var name_buf: [ticket_name_len]u8 = undefined;
        var lowest_visited = ticket.index;
        var k = ticket.index;
        while (k > 0) {
            k -= 1;
            const name = ticketName(&name_buf, k);
            const pred = self.dir.openFile(io, name, .{ .mode = .read_write }) catch |err| switch (err) {
                error.FileNotFound => break,
                else => return err,
            };
            const held = blk: {
                if (pred.tryLock(io, .exclusive) catch |err| {
                    pred.close(io);
                    return err;
                }) break :blk pred;
                break :blk path_lock_wait.lockUntil(pred, io, .exclusive, deadline_ns) catch |err| switch (err) {
                    error.ConcurrencyUnavailable => try pollLock(self.dir, io, name, deadline_ns),
                    else => return err,
                };
            };
            const state = readState(held, io) catch |err| {
                held.close(io);
                return err;
            };
            held.close(io);
            lowest_visited = k;
            switch (state) {
                .granted, .done => break,
                // Dead waiters and abandoned tickets are skipped. The next
                // live ticket below them is what they were waiting on.
                else => continue,
            }
        }
        try writeState(ticket.file, io, .granted);
        // FIFO guarantees every ticket below ours has finished.
        var i = lowest_visited;
        while (i < ticket.index) : (i += 1) self.dir.deleteFile(io, ticketName(&name_buf, i)) catch {};
    }

    /// Passes the turn to the next ticket.
    pub fn finish(self: *Queue, io: std.Io, ticket: *Ticket) void {
        _ = self;
        writeState(ticket.file, io, .done) catch {};
        // Closing drops the lock; an unwritten state reads as "dead while
        // granted", which successors also treat as their turn.
        ticket.file.close(io);
        ticket.* = undefined;
    }

    fn abandon(self: *Queue, io: std.Io, ticket: *Ticket) void {
        _ = self;
        // A failed write leaves `W`; an unlocked `W` already reads as dead.
        writeState(ticket.file, io, .abandoned) catch {};
        ticket.file.close(io);
        ticket.* = undefined;
    }
};

pub const Ticket = struct {
    index: u64,
    file: std.Io.File,
};

/// A granted ticket plus the queue handle that issued it.
pub const Turn = struct {
    queue: Queue,
    ticket: Ticket,

    pub fn release(self: *Turn, io: std.Io) void {
        self.queue.finish(io, &self.ticket);
        self.queue.close(io);
        self.* = undefined;
    }

    /// True when a later ticket exists, i.e. another writer is waiting for
    /// this turn.
    pub fn hasSuccessor(self: *const Turn, io: std.Io) bool {
        return self.queue.ticketsIssuedSince(io, self.ticket.index + 1);
    }
};

/// Lets an uncontended (ticketless) lease notice writers queuing behind it.
pub const Watch = struct {
    queue: Queue,
    /// Tickets numbered at or above this arrived after the lease was taken.
    mark: u64,

    pub fn release(self: *Watch, io: std.Io) void {
        self.queue.close(io);
        self.* = undefined;
    }

    pub fn hasArrivals(self: *const Watch, io: std.Io) bool {
        return self.queue.ticketsIssuedSince(io, self.mark);
    }
};

fn readCounter(file: std.Io.File, io: std.Io) !u64 {
    var raw: [8]u8 = @splat(0);
    const n = try file.readPositionalAll(io, &raw, 0);
    return if (n == raw.len) std.mem.readInt(u64, &raw, .little) else 0;
}

fn readState(file: std.Io.File, io: std.Io) !State {
    var b: [1]u8 = .{0};
    const n = try file.readPositionalAll(io, &b, 0);
    // An empty file belongs to a waiter that died before writing its state.
    return if (n == 0) .waiting else @fromBackingInt(@intCast(b[0]));
}

fn writeState(file: std.Io.File, io: std.Io, state: State) !void {
    try file.writePositionalAll(io, &.{@backingInt(state)}, 0);
}

/// Fallback when the executor cannot run a concurrent kernel wait.
fn pollLock(dir: std.Io.Dir, io: std.Io, name: []const u8, deadline_ns: i96) !std.Io.File {
    const file = try dir.openFile(io, name, .{ .mode = .read_write });
    errdefer file.close(io);
    var backoff_ns: u64 = 50 * std.time.ns_per_us;
    while (true) {
        if (try file.tryLock(io, .exclusive)) return file;
        const remaining = deadline_ns - std.Io.Clock.awake.now(io).nanoseconds;
        if (remaining <= 0) return error.FileBusy;
        try io.sleep(.fromNanoseconds(@min(@as(i96, backoff_ns), remaining)), .awake);
        backoff_ns = @min(backoff_ns * 2, 2 * std.time.ns_per_ms);
    }
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

fn testSidecar(tmp: *std.testing.TmpDir, io: std.Io, buf: []u8) ![]const u8 {
    const dir_path = try tmp.dir.realPathFileAlloc(io, ".", testing.allocator);
    defer testing.allocator.free(dir_path);
    return std.fmt.bufPrint(buf, "{s}/db.aflite.lock", .{dir_path});
}

fn farDeadline(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_s;
}

fn enqueueOne(q: *Queue, io: std.Io) !Ticket {
    var tail = try q.lockTail(io, farDeadline(io));
    defer tail.release(io);
    return q.enqueue(io, &tail);
}

const Recorder = struct {
    mutex: std.Io.Mutex = .init,
    order: [8]u64 = undefined,
    len: usize = 0,

    fn record(self: *Recorder, io: std.Io, index: u64) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.order[self.len] = index;
        self.len += 1;
    }
};

fn waitAndRecord(q: *Queue, io: std.Io, ticket: Ticket, rec: *Recorder) !void {
    var t = ticket;
    try q.awaitTurn(io, &t, farDeadline(io));
    rec.record(io, t.index);
    try io.sleep(.fromMilliseconds(2), .awake);
    q.finish(io, &t);
}

test "writer queue grants in ticket order" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(16) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);

    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);
    {
        var tail = try q.lockTail(io, farDeadline(io));
        defer tail.release(io);
        try testing.expect(try q.isIdle(io, &tail));
    }
    var head = try enqueueOne(&q, io);
    try q.awaitTurn(io, &head, farDeadline(io));

    var rec: Recorder = .{};
    var futures: [4]std.Io.Future(@typeInfo(@TypeOf(waitAndRecord)).@"fn".return_type.?) = undefined;
    var expected: [4]u64 = undefined;
    var handles: [4]Queue = undefined;
    defer for (&handles) |*h| h.close(io);
    for (&futures, &expected, &handles) |*f, *e, *qq| {
        // Each waiter has its own queue handle, like a separate process.
        qq.* = try Queue.open(testing.allocator, io, sidecar);
        const t = try enqueueOne(qq, io);
        e.* = t.index;
        f.* = try std.Io.concurrent(io, waitAndRecord, .{ qq, io, t, &rec });
    }
    {
        var tail = try q.lockTail(io, farDeadline(io));
        defer tail.release(io);
        try testing.expect(!try q.isIdle(io, &tail));
    }
    try io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectEqual(@as(usize, 0), rec.len);
    q.finish(io, &head);
    for (&futures) |*f| try f.await(io);
    try testing.expectEqualSlices(u64, &expected, rec.order[0..rec.len]);
    {
        var tail = try q.lockTail(io, farDeadline(io));
        defer tail.release(io);
        try testing.expect(try q.isIdle(io, &tail));
    }
}

test "writer queue skips an abandoned waiter and keeps order" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);

    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);
    var head = try enqueueOne(&q, io);
    try q.awaitTurn(io, &head, farDeadline(io));

    var impatient = try enqueueOne(&q, io);
    var patient = try enqueueOne(&q, io);
    try testing.expectError(error.FileBusy, q.awaitTurn(io, &impatient, std.Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_ms));

    // The patient waiter must still wait for the head, not jump the queue.
    var pending = try std.Io.concurrent(io, Queue.awaitTurn, .{ &q, io, &patient, farDeadline(io) });
    try io.sleep(.fromMilliseconds(20), .awake);
    q.finish(io, &head);
    try pending.await(io);
    q.finish(io, &patient);
}

test "writer queue treats a dead waiter as skipped and a dead holder as finished" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);

    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);
    // A granted holder that dies without writing `D`.
    var holder = try enqueueOne(&q, io);
    try q.awaitTurn(io, &holder, farDeadline(io));
    // A waiter that dies while still `W`.
    const dead_waiter = try enqueueOne(&q, io);
    var next = try enqueueOne(&q, io);
    holder.file.close(io);
    dead_waiter.file.close(io);
    try q.awaitTurn(io, &next, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    q.finish(io, &next);

    var tail = try q.lockTail(io, farDeadline(io));
    defer tail.release(io);
    try testing.expect(try q.isIdle(io, &tail));
}

test "writer queue is not idle while a dead tail hides a live waiter" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);

    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);
    var live = try enqueueOne(&q, io);
    var abandoned = try enqueueOne(&q, io);
    try q.awaitTurn(io, &live, farDeadline(io));
    q.abandon(io, &abandoned);
    var tail = try q.lockTail(io, farDeadline(io));
    defer tail.release(io);
    try testing.expect(!try q.isIdle(io, &tail));
    q.finish(io, &live);
}

// ------------------------------------------------- protocol rule tests

fn writeCounterRaw(q: *Queue, io: std.Io, bytes: []const u8) !void {
    const tail = try q.dir.createFile(io, tail_name, .{ .read = true, .truncate = true });
    defer tail.close(io);
    try tail.writePositionalAll(io, bytes, 0);
}

test "writer queue rule 2: a damaged counter never numbers a ticket below a live one" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);
    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);

    var head = try enqueueOne(&q, io);
    try q.awaitTurn(io, &head, farDeadline(io));
    var live = try enqueueOne(&q, io);
    defer q.abandon(io, &live);
    for ([_][]const u8{ &.{ 0, 0, 0, 0, 0, 0, 0, 0 }, &.{ 1, 2, 3 }, "" }) |damage| {
        try writeCounterRaw(&q, io, damage);
        var next = try enqueueOne(&q, io);
        try testing.expect(next.index > live.index);
        q.abandon(io, &next);
    }
    q.finish(io, &head);
}

test "writer queue rule 2: an unadvanced leftover ticket is skipped as dead" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);
    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);
    // A crashed issuer created t-0 (state W, unlocked) but never advanced.
    {
        var name_buf: [ticket_name_len]u8 = undefined;
        const f = try q.dir.createFile(io, ticketName(&name_buf, 0), .{});
        defer f.close(io);
        try writeState(f, io, .waiting);
    }
    var t = try enqueueOne(&q, io);
    try testing.expectEqual(@as(u64, 1), t.index);
    // Granted at once: the leftover reads as a dead waiter.
    try q.awaitTurn(io, &t, std.Io.Clock.awake.now(io).nanoseconds + 50 * std.time.ns_per_ms);
    q.finish(io, &t);
}

test "writer queue rule 6: cancelling a queued waiter does not let a later one pass an earlier live one" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(16) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);
    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);

    var head = try enqueueOne(&q, io);
    try q.awaitTurn(io, &head, farDeadline(io));
    var live = try enqueueOne(&q, io); // earlier live waiter
    var cancelled = try enqueueOne(&q, io);
    var last = try enqueueOne(&q, io);
    var live_wait = try std.Io.concurrent(io, Queue.awaitTurn, .{ &q, io, &live, farDeadline(io) });
    var cancelled_wait = try std.Io.concurrent(io, Queue.awaitTurn, .{ &q, io, &cancelled, farDeadline(io) });
    var last_wait = try std.Io.concurrent(io, Queue.awaitTurn, .{ &q, io, &last, farDeadline(io) });
    try io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectError(error.Canceled, cancelled_wait.cancel(io));
    // `last` now walks past the cancelled ticket and must block on `live`,
    // which is itself still blocked on `head`.
    try io.sleep(.fromMilliseconds(30), .awake);
    q.finish(io, &head);
    try live_wait.await(io);
    // `live` holds its turn: `last` must still be waiting.
    try io.sleep(.fromMilliseconds(30), .awake);
    {
        var tail = try q.lockTail(io, farDeadline(io));
        defer tail.release(io);
        try testing.expect(!try q.isIdle(io, &tail));
    }
    q.finish(io, &live);
    try last_wait.await(io);
    q.finish(io, &last);
}

const ChildMode = enum { die_holding_tail, die_waiting, die_granted, die_before_advance };

/// Raw libc only: the child of a multithreaded parent may only make
/// async-signal-safe calls. Paths are prepared by the parent.
fn childStep(mode: ChildMode, tail_path: [*:0]const u8, ticket_path: [*:0]const u8, pred_path: [*:0]const u8, index: u64) noreturn {
    const c = std.c;
    const tail = c.open(tail_path, .{ .ACCMODE = .RDWR, .CREAT = true }, @as(c.mode_t, 0o644));
    if (tail < 0) c._exit(10);
    if (c.flock(tail, c.LOCK.EX) != 0) c._exit(11);
    if (mode == .die_holding_tail) c._exit(0);
    const t = c.open(ticket_path, .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true }, @as(c.mode_t, 0o644));
    if (t < 0) c._exit(12);
    if (c.flock(t, c.LOCK.EX) != 0) c._exit(13);
    if (c.pwrite(t, "W", 1, 0) != 1) c._exit(14);
    if (mode == .die_before_advance) c._exit(0);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, index + 1, .little);
    if (c.pwrite(tail, &raw, 8, 0) != 8) c._exit(15);
    _ = c.close(tail);
    if (mode == .die_granted) {
        if (c.pwrite(t, "G", 1, 0) != 1) c._exit(16);
        c._exit(0);
    }
    // die_waiting: block in the kernel on the predecessor until killed.
    const p = c.open(pred_path, .{ .ACCMODE = .RDWR });
    if (p < 0) c._exit(17);
    _ = c.flock(p, c.LOCK.EX);
    c._exit(18);
}

fn reapChild(pid: std.c.pid_t, kill_first: bool) !void {
    if (kill_first) _ = std.c.kill(pid, std.c.SIG.KILL);
    var status: c_int = 0;
    while (std.c.waitpid(pid, &status, 0) < 0) {}
    if (!kill_first and std.c.W.EXITSTATUS(@bitCast(status)) != 0) return error.ChildFailed;
}

test "writer queue rule 5: real process death in every state" {
    if (@import("builtin").os.tag == .windows or @import("builtin").single_threaded) return error.SkipZigTest;
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);
    var q = try Queue.open(testing.allocator, io, sidecar);
    defer q.close(io);
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const qdir = try std.fmt.bufPrint(&dir_buf, "{s}.q", .{sidecar});

    var head = try enqueueOne(&q, io); // ticket 0, granted, held by the parent
    try q.awaitTurn(io, &head, farDeadline(io));

    var tail_z: [std.fs.max_path_bytes]u8 = undefined;
    var ticket_z: [std.fs.max_path_bytes]u8 = undefined;
    var pred_z: [std.fs.max_path_bytes]u8 = undefined;
    const tail_path = try std.fmt.bufPrintSentinel(&tail_z, "{s}/tail", .{qdir}, 0);

    // A process dying while it holds the tail lock does not wedge issuing.
    {
        const pid = std.c.fork();
        if (pid == 0) childStep(.die_holding_tail, tail_path, "", "", 0);
        try reapChild(pid, false);
        var tail = try q.lockTail(io, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
        tail.release(io);
    }
    // A process killed while blocked behind the head reads as dead and is
    // skipped; the next live ticket still waits for the head.
    {
        const pid = std.c.fork();
        if (pid == 0) childStep(.die_waiting, tail_path, try std.fmt.bufPrintSentinel(&ticket_z, "{s}/t-{x:0>16}", .{ qdir, @as(u64, 1) }, 0), try std.fmt.bufPrintSentinel(&pred_z, "{s}/t-{x:0>16}", .{ qdir, @as(u64, 0) }, 0), 1);
        // Wait until the child's ticket is issued, then kill it mid-wait.
        while (true) {
            var tail = try q.lockTail(io, farDeadline(io));
            const next = tail.next;
            tail.release(io);
            if (next >= 2) break;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try io.sleep(.fromMilliseconds(10), .awake);
        try reapChild(pid, true);
    }
    var after_dead_waiter = try enqueueOne(&q, io); // ticket 2
    var waiting = try std.Io.concurrent(io, Queue.awaitTurn, .{ &q, io, &after_dead_waiter, farDeadline(io) });
    try io.sleep(.fromMilliseconds(20), .awake);
    q.finish(io, &head);
    try waiting.await(io);
    try testing.expectEqual(@as(u64, 2), after_dead_waiter.index);

    // A granted holder that dies is treated as finished by its successor.
    q.finish(io, &after_dead_waiter);
    {
        const pid = std.c.fork();
        if (pid == 0) childStep(.die_granted, tail_path, try std.fmt.bufPrintSentinel(&ticket_z, "{s}/t-{x:0>16}", .{ qdir, @as(u64, 3) }, 0), "", 3);
        try reapChild(pid, false);
    }
    var after_dead_holder = try enqueueOne(&q, io);
    try testing.expectEqual(@as(u64, 4), after_dead_holder.index);
    try q.awaitTurn(io, &after_dead_holder, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    q.finish(io, &after_dead_holder);

    // A process dying between creating its ticket and advancing the counter
    // leaves a file that is skipped as dead, never a phantom live waiter.
    {
        const pid = std.c.fork();
        if (pid == 0) childStep(.die_before_advance, tail_path, try std.fmt.bufPrintSentinel(&ticket_z, "{s}/t-{x:0>16}", .{ qdir, @as(u64, 5) }, 0), "", 5);
        try reapChild(pid, false);
    }
    var after_unadvanced = try enqueueOne(&q, io);
    try testing.expectEqual(@as(u64, 6), after_unadvanced.index);
    try q.awaitTurn(io, &after_unadvanced, std.Io.Clock.awake.now(io).nanoseconds + 50 * std.time.ns_per_ms);
    q.finish(io, &after_unadvanced);
}

const Stress = struct {
    sidecar: []const u8,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    in_critical: std.atomic.Value(bool) = .init(false),
    last_granted: ?u64 = null,
    order_violations: usize = 0,
    overlap_violations: usize = 0,
    patient_timeouts: usize = 0,
    grants: usize = 0,

    fn worker(self: *Stress, seed: u64) void {
        self.run(seed) catch {
            self.mutex.lockUncancelable(self.io);
            self.patient_timeouts += 1000;
            self.mutex.unlock(self.io);
        };
    }

    fn run(self: *Stress, seed: u64) !void {
        const io = self.io;
        var prng = std.Random.DefaultPrng.init(seed);
        const rand = prng.random();
        var q = try Queue.open(testing.allocator, io, self.sidecar);
        defer q.close(io);
        for (0..60) |_| {
            var t = try enqueueOne(&q, io);
            switch (rand.uintLessThan(u8, 10)) {
                // Dies while waiting: the lock drops with state W.
                0 => {
                    t.file.close(io);
                    continue;
                },
                // Impatient: often abandons.
                1, 2 => {
                    const deadline = std.Io.Clock.awake.now(io).nanoseconds + @as(i96, rand.uintLessThan(u32, 3000)) * std.time.ns_per_us;
                    q.awaitTurn(io, &t, deadline) catch continue;
                },
                else => q.awaitTurn(io, &t, farDeadline(io)) catch |err| {
                    if (err == error.FileBusy) {
                        self.mutex.lockUncancelable(io);
                        self.patient_timeouts += 1;
                        self.mutex.unlock(io);
                        continue;
                    }
                    return err;
                },
            }
            if (self.in_critical.swap(true, .acq_rel)) {
                self.mutex.lockUncancelable(io);
                self.overlap_violations += 1;
                self.mutex.unlock(io);
            }
            self.mutex.lockUncancelable(io);
            if (self.last_granted) |last| if (t.index <= last) {
                self.order_violations += 1;
            };
            self.last_granted = t.index;
            self.grants += 1;
            self.mutex.unlock(io);
            if (rand.boolean()) io.sleep(.fromMicroseconds(rand.uintLessThan(u32, 300)), .awake) catch {};
            self.in_critical.store(false, .release);
            // Occasionally die while granted: no D is written.
            if (rand.uintLessThan(u8, 10) == 0) {
                t.file.close(io);
            } else q.finish(io, &t);
        }
    }
};

test "writer queue stress: exclusion and strict grant order with abandonment and deaths" {
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(64) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sidecar = try testSidecar(&tmp, io, &path_buf);
    var stress: Stress = .{ .sidecar = sidecar, .io = io };
    var threads: [6]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, Stress.worker, .{ &stress, 0x1431 + i });
    for (threads) |t| t.join();
    try testing.expectEqual(@as(usize, 0), stress.overlap_violations);
    try testing.expectEqual(@as(usize, 0), stress.order_violations);
    try testing.expectEqual(@as(usize, 0), stress.patient_timeouts);
    try testing.expect(stress.grants > 100);
}
