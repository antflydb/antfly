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

//! Step 1 of `docs/design/lite-performance/lite-single-commit-write-path.md`
//! §8: the shared mmap region (§8.1) and the writer lock (§8.2). Reader
//! slots (§8.3) and liveness/recovery (§8.4) land in later steps; the
//! migration/deletion of `inode_lock.zig`, `writer_queue.zig`, and the rest
//! of §8.7's table happens only once §8.3/§8.4 exist to replace what they
//! still rely on. Until then this module is additive and unreferenced by
//! existing callers.
//!
//! One `<db>.aflite-lock` file, memory-mapped `MAP_SHARED` by every process
//! with the database open (SQLite's `-shm`, LMDB's `lock.mdb`). The steady,
//! uncontended acquire/release is a single CAS/store on the mapping -- no
//! syscall at all, not even a read. A contended waiter takes a FIFO ticket
//! (an atomic counter in the region, no ticket file) and parks on a real
//! cross-process futex derived from the region's own ticket-head word:
//! macOS `__ulock_wait`/`__ulock_wake` with the *shared* compare-and-wait
//! op (the private op `std.Io`'s own futex helpers use is scoped to one
//! process's address space and cannot wake a waiter in another process),
//! Linux `futex(2)` without `FUTEX_PRIVATE_FLAG` for the same reason. The
//! releaser wakes every waiter on a release (like
//! `antfly_platform.sync.BroadcastCondition`'s own in-process pattern, just
//! backed by a real kernel primitive instead of the evented runtime's
//! private one); each re-checks whether the now-current ticket head is its
//! own before trying the CAS again, so only the correct one proceeds and
//! everyone else re-parks -- no per-ticket futex slot is needed for that.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;

const magic: [8]u8 = "AFLLOCK1".*;
const format_version: u32 = 1;

/// Fixed for step 1 (no reader table yet); grows in step 2 without a
/// format bump by using the header's own `region_bytes` field, not this
/// constant, as the source of truth for how much of the file is mapped.
const header_bytes = 64;

/// `Header` is the first `header_bytes` of the region, native-endian (the
/// region is only ever read by the same machine that wrote it -- unlike
/// `native.zig`'s on-disk format, this file never survives a copy to a
/// different architecture).
const Header = extern struct {
    magic: [8]u8,
    format_version: u32,
    header_checksum: u32,
    region_bytes: u64,
    /// §8.2: 0 when free, else the holder's packed `(pid, generation)`.
    /// `generation` is reserved for step 3 (liveness/recovery); step 1
    /// writes 0 there and holder identity is the pid alone.
    writer_holder: std.atomic.Value(u64),
    /// §8.2: next ticket to issue (fetch-add on every contended acquirer;
    /// the uncontended fast path never touches this).
    writer_ticket_tail: std.atomic.Value(u64),
    /// §8.2: the ticket currently eligible to attempt the CAS. Also the
    /// futex word contended waiters park on.
    writer_ticket_head: std.atomic.Value(u32),
    _reserved: [8]u8 = @splat(0),
};

comptime {
    std.debug.assert(@sizeOf(Header) <= header_bytes);
}

fn headerChecksum(header: *const Header) u32 {
    const Crc32c = @import("antfly_hash").Crc32c;
    // Everything after the checksum field itself, same "exclude the
    // checksum from its own input" convention native.zig's headerChecksum
    // already uses.
    const bytes = std.mem.asBytes(header);
    return Crc32c.hash(bytes[12..]);
}

/// Packs a pid into a holder word. Step 3 ORs a nonzero generation into the
/// high 32 bits so a dead holder's stale word can be told apart from a live
/// one that happens to share a reused pid; step 1 always writes 0 there.
fn packHolder(pid: u32, generation: u32) u64 {
    return (@as(u64, generation) << 32) | @as(u64, pid);
}

fn holderPid(word: u64) u32 {
    return @truncate(word);
}

/// True when mmap or the shared-futex self-test is unavailable and callers
/// must fall back to the flock-based path this module replaces (§8.6).
/// Step 1 does not yet implement that fallback path itself -- it returns
/// `error.RegionUnavailable` instead, so `inode_lock.zig` (unreferenced by
/// this module so far) keeps serving every caller until a later step wires
/// the fallback flag through.
pub const RegionError = error{RegionUnavailable};

pub const Region = struct {
    mapping: []align(std.heap.page_size_min) u8,
    /// Kept open only so `munmap`'s backing file reference is unambiguous
    /// across platforms during `close`; step 3 reuses this same fd for the
    /// per-process liveness byte-range lock instead of opening another.
    fd: std.posix.fd_t,

    fn header(self: *const Region) *Header {
        return @ptrCast(@alignCast(self.mapping.ptr));
    }

    /// Opens (creating if absent) the region next to `db_path`, e.g.
    /// `<db_path>.aflite-lock`. Initializes a fresh or corrupt region under
    /// one `flock` -- the only `flock` this module issues, and only here,
    /// never per acquire/release. `allocator` is used only for the
    /// temporary path string; nothing from the region itself is heap
    /// allocated.
    pub fn open(allocator: std.mem.Allocator, io: Io, db_path: []const u8) !Region {
        const region_path = try std.fmt.allocPrint(allocator, "{s}.aflite-lock", .{db_path});
        defer allocator.free(region_path);

        const dir = Io.Dir.cwd();
        const setup_file = try dir.createFile(io, region_path, .{ .read = true, .truncate = false, .permissions = .fromMode(0o600) });
        defer setup_file.close(io);
        try setup_file.lock(io, .exclusive);
        defer setup_file.unlock(io);

        const size = (try setup_file.stat(io)).size;
        var need_init = size < header_bytes;
        if (!need_init) {
            var raw: [header_bytes]u8 = undefined;
            const n = try setup_file.readPositionalAll(io, &raw, 0);
            need_init = n != raw.len or !isValidHeader(&raw);
        }
        if (need_init) {
            try setup_file.setLength(io, header_bytes);
            var fresh: Header = .{
                .magic = magic,
                .format_version = format_version,
                .header_checksum = 0,
                .region_bytes = header_bytes,
                .writer_holder = .init(0),
                .writer_ticket_tail = .init(0),
                .writer_ticket_head = .init(0),
            };
            fresh.header_checksum = headerChecksum(&fresh);
            try setup_file.writePositionalAll(io, std.mem.asBytes(&fresh), 0);
            try setup_file.sync(io);
        }

        // mmap needs its own raw fd (platform `mmap` takes a POSIX fd, not
        // an `Io.File`); opened read-write, close-on-exec so a forked-and-
        // exec'd child never inherits a stale mapping-backing descriptor.
        const raw_fd = std.posix.openat(std.posix.AT.FDCWD, region_path, .{ .ACCMODE = .RDWR, .CLOEXEC = true }, 0) catch return RegionError.RegionUnavailable;
        errdefer _ = std.posix.system.close(raw_fd);
        const mapping = std.posix.mmap(null, header_bytes, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, raw_fd, 0) catch return RegionError.RegionUnavailable;
        return .{ .mapping = mapping, .fd = raw_fd };
    }

    pub fn close(self: *Region) void {
        std.posix.munmap(self.mapping);
        _ = std.posix.system.close(self.fd);
        self.* = undefined;
    }

    fn isValidHeader(raw: *const [header_bytes]u8) bool {
        const h: *const Header = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, &h.magic, &magic)) return false;
        if (h.format_version != format_version) return false;
        return h.header_checksum == headerChecksum(h);
    }

    /// Uncontended fast path: one CAS, no syscall. Returns `true` if
    /// acquired. `pid` is this process's id (`antfly_platform.process.
    /// currentId()`, never 0 -- a 0 holder word means free).
    fn tryAcquireWriter(self: *Region, pid: u32) bool {
        const want = packHolder(pid, 0);
        return self.header().writer_holder.cmpxchgStrong(0, want, .acquire, .monotonic) == null;
    }

    /// Blocks (via a real cross-process futex, not a timer or a poll loop)
    /// until the writer word is acquired for `pid`. The uncontended case
    /// never reaches the futex syscall at all.
    ///
    /// `head` means "this ticket may try the CAS now", not "this ticket
    /// has it" -- the holder of ticket N may still be the *ticketless*
    /// fast-path holder above (acquired with no ticket at all), so `head`
    /// only ever advances here, in the waiter that actually wins its CAS,
    /// never in `releaseWriter`. An earlier version advanced it in
    /// `releaseWriter` instead; a ticketless holder's release then had no
    /// ticket of its own to retire, so it either skipped the wake entirely
    /// (an off-by-one on a post-increment comparison) or walked `head`
    /// past where the *next* ticket would ever match it -- a release could
    /// land with nothing waiting at all and still advance `head`, after
    /// which no future ticket's `head == my_ticket` check could ever
    /// become true again. Both were real: the second produced the lost
    /// cross-process wakeup a two-process contention test hung on.
    pub fn acquireWriter(self: *Region, pid: u32) void {
        const hdr = self.header();
        // FIFO: only take the fast, ticketless path with nobody already
        // queued -- otherwise a new arrival could win the CAS ahead of an
        // earlier ticket already parked and waiting its turn.
        if (hdr.writer_ticket_head.load(.monotonic) == @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))) and self.tryAcquireWriter(pid)) return;
        const my_ticket: u32 = @truncate(hdr.writer_ticket_tail.fetchAdd(1, .monotonic));
        while (true) {
            const head = hdr.writer_ticket_head.load(.acquire);
            if (head == my_ticket and self.tryAcquireWriter(pid)) {
                // Won this turn. Advance head so ticket `my_ticket + 1`
                // (if any) may try, and wake whoever is parked for it --
                // including a spurious wake of a later ticket still
                // waiting on the old head value, which simply re-checks
                // and re-parks (see the module doc comment).
                _ = hdr.writer_ticket_head.fetchAdd(1, .release);
                futexWakeShared(&hdr.writer_ticket_head.raw);
                return;
            }
            // Either not our turn yet, or it was but the word was not
            // free (the holder -- possibly the ticketless fast-path
            // holder, with no ticket of its own -- has not released yet).
            // `releaseWriter` wakes this exact word whenever anyone is
            // queued, so waiting on the *current*, unchanged `head` value
            // is correct: nothing else is responsible for moving `head`
            // in that case.
            futexWaitShared(&hdr.writer_ticket_head.raw, head);
        }
    }

    /// Releases a writer hold taken by `acquireWriter`/`tryAcquireWriter`.
    /// Never touches `head` itself (see `acquireWriter`'s doc comment for
    /// why) -- only wakes, and only when someone is actually queued
    /// (`head != tail`), so the fully uncontended release stays a single
    /// store with no syscall.
    pub fn releaseWriter(self: *Region) void {
        const hdr = self.header();
        hdr.writer_holder.store(0, .release);
        if (hdr.writer_ticket_head.load(.monotonic) != @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic)))) {
            futexWakeShared(&hdr.writer_ticket_head.raw);
        }
    }
};

/// Shared (cross-process) futex wait/wake. Neither `std.Io`'s own
/// `futexWait*`/`futexWake` (always the OS's *private*, same-process-only
/// variant on every backend checked -- `Io.Threaded` passes
/// `.private = true` on Linux and the non-`_SHARED` op on macOS) nor
/// `antfly_platform.sync`'s `BroadcastCondition` (built on exactly that
/// private primitive, for same-process predicate loops) can wake a waiter
/// in a different process. These wrap the same underlying OS calls with
/// the shared variant instead, so a release in one process reliably wakes
/// a waiter parked in another.
fn futexWaitShared(ptr: *const u32, expect: u32) void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            const rc = linux.futex_4arg(ptr, .{ .cmd = .WAIT, .private = false }, expect, null);
            switch (linux.errno(rc)) {
                .SUCCESS, .INTR, .AGAIN => {},
                else => {},
            }
        },
        .macos, .ios, .tvos, .watchos, .visionos, .maccatalyst, .driverkit => {
            const c = std.c;
            const flags: c.UL = .{ .op = .COMPARE_AND_WAIT_SHARED, .NO_ERRNO = true };
            _ = c.__ulock_wait(flags, @ptrCast(@constCast(ptr)), expect, 0);
        },
        else => @compileError("shared futex not implemented for this target; see §8.6 fallback"),
    }
}

fn futexWakeShared(ptr: *const u32) void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            const rc = linux.futex_4arg(ptr, .{ .cmd = .WAKE, .private = false }, std.math.maxInt(u32), null);
            _ = linux.errno(rc);
        },
        .macos, .ios, .tvos, .watchos, .visionos, .maccatalyst, .driverkit => {
            const c = std.c;
            const flags: c.UL = .{ .op = .COMPARE_AND_WAIT_SHARED, .WAKE_ALL = true, .NO_ERRNO = true };
            _ = c.__ulock_wake(flags, @ptrCast(@constCast(ptr)), 0);
        },
        else => @compileError("shared futex not implemented for this target; see §8.6 fallback"),
    }
}

/// Reads exactly `buf.len` bytes from `fd`, but never blocks past
/// `timeout_ms`: every cross-process test in this file waits on something a
/// *correct* implementation finishes in about one commit, so a regression
/// (a lost wakeup, a wrong futex flag, an unshared mapping) must fail the
/// test case quickly instead of hanging the whole suite -- exactly the
/// failure mode the first version of the two-process test below hit.
fn readBounded(fd: std.c.fd_t, buf: []u8, timeout_ms: i32) !void {
    var filled: usize = 0;
    while (filled < buf.len) {
        var fds = [_]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        const ready = std.c.poll(&fds, 1, timeout_ms);
        if (ready <= 0) return error.Timeout;
        const n = std.c.read(fd, buf[filled..].ptr, buf.len - filled);
        if (n <= 0) return error.PipeClosed;
        filled += @intCast(n);
    }
}

/// Waits for `pid` to exit, but never past `timeout_ms`: on timeout, kills
/// it (`SIGKILL`) and reaps it so the test fails with a clear error instead
/// of leaving a hung or zombie process behind.
fn reapBounded(pid: std.c.pid_t, timeout_ms: i64) !c_int {
    var status: c_int = 0;
    var waited_ms: i64 = 0;
    const poll_ms = 5;
    while (true) {
        const got = std.c.waitpid(pid, &status, std.c.W.NOHANG);
        if (got == pid) return std.c.W.EXITSTATUS(@bitCast(status));
        if (got < 0) return error.WaitFailed;
        if (waited_ms >= timeout_ms) {
            _ = std.c.kill(pid, std.c.SIG.KILL);
            while (std.c.waitpid(pid, &status, 0) < 0) {}
            return error.ChildTimedOut;
        }
        // `poll` with no fds and a timeout is a portable sleep, avoiding a
        // separate `nanosleep` declaration for this one backoff.
        var no_fds: [0]std.c.pollfd = .{};
        _ = std.c.poll(&no_fds, 0, poll_ms);
        waited_ms += poll_ms;
    }
}

// Two real OS processes (via `std.c.fork`, the same pattern
// `writer_queue.zig`'s own multi-process tests use): the parent holds the
// writer word, the child blocks in `acquireWriter` on the real shared
// futex, and the parent measures how long after its own release the child
// actually wakes and acquires -- the whole point of §8.2 over the 1 s
// fairness quantum and the ticket-queue `pread` poll it replaces. The
// child uses no `Io` at all (fork only duplicates the calling thread, not
// `std.testing.io`'s background threads, so reusing it post-fork would be
// unsafe) -- `Region.acquireWriter`/`releaseWriter` already take no `Io`
// parameter, by construction, for exactly this reason.
test "shared lock region: two-process contention hands off in about one commit, not a timer" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-contend.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriter(own_pid));

    var pipe_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&pipe_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(pipe_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        // Blocks on the real cross-process futex until the parent's
        // release below wakes it.
        region.acquireWriter(my_pid);
        var ts: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.MONOTONIC, &ts);
        const acquired_ns: i64 = @as(i64, ts.sec) * std.time.ns_per_s + ts.nsec;
        _ = std.c.write(pipe_fds[1], @ptrCast(&acquired_ns), @sizeOf(i64));
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(pipe_fds[1]);
    defer _ = std.posix.system.close(pipe_fds[0]);

    // Hold briefly, like one commit's own work, then release -- the child
    // must already be parked on the futex by the time this runs for the
    // test to actually exercise the wait/wake path rather than racing it.
    try io.sleep(.fromMilliseconds(20), .awake);
    var release_ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &release_ts);
    const release_ns: i64 = @as(i64, release_ts.sec) * std.time.ns_per_s + release_ts.nsec;
    region.releaseWriter();

    var acquired_ns: i64 = undefined;
    // Bounded: a lost wakeup must fail this test in well under a second,
    // not hang the whole suite waiting for a child that will never write.
    try readBounded(pipe_fds[0], std.mem.asBytes(&acquired_ns), 2000);

    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pid, 2000));

    // The whole point: a handoff bounded by "about one commit" (generous:
    // well under a second, let alone the 1 s quantum this design retires),
    // not merely "eventually".
    const handoff_ns = acquired_ns - release_ns;
    try std.testing.expect(handoff_ns >= 0);
    try std.testing.expect(handoff_ns < 200 * std.time.ns_per_ms);
}

// §8.8 calls out "two- and N-process contention" with FIFO grant order as
// its own assertion, not just bounded latency -- the two-process test above
// already covers the latency bound, this one covers strict ticket order
// across more than two contenders. Each child signals the parent right
// before it enters `acquireWriter` so the parent only forks the next child
// once the previous one is about to take its ticket, making fork order the
// known, intended ticket order (the same ordering assumption the
// two-process test above makes with its 20ms margin, just enforced with a
// handshake instead of a sleep since there are more than two parties here).
// Grant order is then read back off a second pipe in arrival order, which
// is also grant order: each child only writes its report after acquiring,
// and only the current holder can be running.
test "shared lock region: N-process contention grants in strict ticket order" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-n-contend.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriter(own_pid));

    const n_children = 4;
    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));
    var order_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&order_fds));

    var child_pids: [n_children]std.c.pid_t = undefined;
    var i: u8 = 0;
    while (i < n_children) : (i += 1) {
        const my_index = i;
        const child_pid = std.c.fork();
        if (child_pid == 0) {
            _ = std.posix.system.close(ready_fds[0]);
            _ = std.posix.system.close(order_fds[0]);
            const my_pid: u32 = @bitCast(std.c.getpid());
            var sig: [1]u8 = .{my_index};
            _ = std.c.write(ready_fds[1], @ptrCast(&sig), 1);
            region.acquireWriter(my_pid);
            var report: [1]u8 = .{my_index};
            _ = std.c.write(order_fds[1], @ptrCast(&report), 1);
            region.releaseWriter();
            std.c._exit(0);
        }
        child_pids[my_index] = child_pid;
        var sig: [1]u8 = undefined;
        try readBounded(ready_fds[0], &sig, 2000);
    }
    _ = std.posix.system.close(ready_fds[1]);
    _ = std.posix.system.close(ready_fds[0]);
    _ = std.posix.system.close(order_fds[1]);
    defer _ = std.posix.system.close(order_fds[0]);

    // One more margin for the last child to actually be parked on the
    // futex, same reasoning as the two-process test's own sleep.
    try io.sleep(.fromMilliseconds(20), .awake);
    region.releaseWriter();

    var got: u8 = 0;
    while (got < n_children) : (got += 1) {
        var report: [1]u8 = undefined;
        try readBounded(order_fds[0], &report, 2000);
        try std.testing.expectEqual(got, report[0]);
        try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pids[got], 2000));
    }
}

test "shared lock region opens, initializes, and round-trips the writer lock uncontended" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-test.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const pid: u32 = 4242;
    try std.testing.expect(region.tryAcquireWriter(pid));
    try std.testing.expect(!region.tryAcquireWriter(pid + 1));
    region.releaseWriter();
    try std.testing.expect(region.tryAcquireWriter(pid + 1));
    region.releaseWriter();
}

test "shared lock region reopened by a second handle shares the same writer state" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-shared.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var first = try Region.open(a, io, db_path);
    defer first.close();
    var second = try Region.open(a, io, db_path);
    defer second.close();

    try std.testing.expect(first.tryAcquireWriter(1));
    // The second mapping must observe the first's CAS: they back the same
    // file's same pages, not independent copies.
    try std.testing.expect(!second.tryAcquireWriter(2));
    first.releaseWriter();
    try std.testing.expect(second.tryAcquireWriter(2));
    second.releaseWriter();
}

test "shared lock region rejects a corrupt header and reinitializes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-corrupt.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    const region_path = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{db_path});
    defer a.free(region_path);

    {
        var region = try Region.open(a, io, db_path);
        region.close();
    }
    // Corrupt the checksum byte directly.
    {
        const f = try Io.Dir.cwd().openFile(io, region_path, .{ .mode = .read_write });
        defer f.close(io);
        var byte: [1]u8 = .{0xFF};
        try f.writePositionalAll(io, &byte, 12);
    }
    var region = try Region.open(a, io, db_path);
    defer region.close();
    // A freshly reinitialized region is uncontended and acquirable.
    try std.testing.expect(region.tryAcquireWriter(99));
    region.releaseWriter();
}
