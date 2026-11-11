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

//! Steps 1 and 2 of `docs/design/lite-performance/lite-single-commit-write-path.md`
//! §8: the shared mmap region (§8.1), the writer lock (§8.2), and the
//! reader table (§8.3). Liveness/recovery (§8.4) lands in the next step;
//! the migration/deletion of `inode_lock.zig`, `writer_queue.zig`,
//! `reader_epochs.zig`, `ReaderCoordinator`, and the rest of §8.7's table
//! happens only once §8.4 exists to replace what they still rely on (a
//! dead reader's never-cleared pin, a dead writer's held lock). Until then
//! this module is additive and unreferenced by existing callers.
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

/// Fixed size of the header itself. The header's own `region_bytes` and
/// `max_readers` fields (not this constant) are the source of truth for
/// how much of the file is mapped and how many reader slots follow the
/// header -- but `max_readers` itself is fixed forever at a region's
/// first creation; see `max_readers`'s own doc comment for why this
/// module deliberately does not support growing it later.
const header_bytes = 64;

/// `Header` is the first `header_bytes` of the region, native-endian (the
/// region is only ever read by the same machine that wrote it -- unlike
/// `native.zig`'s on-disk format, this file never survives a copy to a
/// different architecture).
const Header = extern struct {
    // -- Immutable identity: set once at creation, never mutated again,
    // and the only part of the header `header_checksum` covers. A
    // checksum that also covered the live fields below would compare a
    // snapshot taken at creation (while they still held their initial
    // values) against a fresh recomputation after the region had seen
    // any real use at all -- `writer_holder` going 0 -> pid -> 0 again
    // returns to its original bytes, but `reader_slot_released` only
    // ever increases, so as soon as a single reader released a slot, the
    // *next* open would recompute a different checksum than the one
    // stored at creation and (wrongly) conclude the region was corrupt.
    // This boundary exists specifically so that can't happen.
    magic: [8]u8,
    format_version: u32,
    header_checksum: u32,
    region_bytes: u64,
    /// §8.3: number of `ReaderSlot`s immediately following the header
    /// (at byte offset `header_bytes`), permanently fixed at the
    /// region's first creation. An `openSized` call against an existing
    /// region always defers to this on-disk value, never mutating it --
    /// growing it in place would let two already-open processes briefly
    /// disagree about the slot count, which means a missed pin, which
    /// means a writer reclaiming a page a reader still needs. Every
    /// opener gets a generous fixed count instead (`default_max_readers`)
    /// and `claimReaderSlotWait` (not growth) is the answer to
    /// exhaustion, the same choice LMDB's own fixed `maxreaders` makes.
    max_readers: u32,
    /// Pads the immutable prefix to a multiple of 8 so the live fields
    /// below start 8-aligned, with no implicit compiler-inserted padding
    /// anywhere in the struct (see `_reserved`'s own comment for why that
    /// matters).
    _immutable_reserved: [4]u8 = @splat(0),

    // -- Live, mutable state: touched by every acquire/release/claim/pin,
    // deliberately *not* covered by `header_checksum` (see above).
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
    /// Bumped (and the matching futex woken) on every `releaseReaderSlot`
    /// so `claimReaderSlotWait` can block on exhaustion instead of
    /// spin-polling, the same shape as the writer ticket's own futex word.
    reader_slot_released: std.atomic.Value(u32),
    /// Sized so the named fields above sum to exactly `header_bytes` (64)
    /// with no implicit trailing struct padding: `std.mem.asBytes`
    /// includes any such padding, and Zig's struct-literal initialization
    /// does not guarantee it is stable (let alone zero) across separate
    /// reads of it -- the root cause of an earlier version of this header
    /// rejecting its own freshly-written, uncorrupted bytes as corrupt on
    /// every reopen. `== header_bytes` below (not `<=`) keeps that
    /// invariant enforced at compile time for any future field addition.
    _reserved: [8]u8 = @splat(0),
};

comptime {
    std.debug.assert(@sizeOf(Header) == header_bytes);
}

/// §8.3's fixed reader-slot layout: one per concurrent reader the region
/// was created to support, immediately following the header.
const ReaderSlot = extern struct {
    /// 0 = free. Claimed via CAS, the same uncontended-fast-path shape as
    /// the writer word; a short linear scan under contention is
    /// acceptable here because claiming happens once per reader lifetime,
    /// not once per commit.
    owner: std.atomic.Value(u32),
    /// Packed `(pid, open-generation)` pair, same encoding as the writer
    /// word's holder. Step 3's liveness probe (not just `owner != 0`)
    /// uses this to tell a slot a dead reader never cleared apart from
    /// one a new process with the same pid legitimately claimed; step 2
    /// writes a zero generation and nothing yet reads this field back.
    generation: u64,
    /// The `commit_sequence` this reader will not let the writer reclaim
    /// pages behind. 0 means "not currently pinned" (sequence numbers
    /// start above 0), written once per pin by the slot's own owner,
    /// cleared to 0 on unpin.
    pinned_sequence: std.atomic.Value(u64),
    /// Pads the slot to 32 bytes (the compiler already inserts 4 bytes
    /// between `owner` and `generation` for `generation`'s 8-byte
    /// alignment, so 8 explicit bytes here, not 12, reaches 32) and
    /// leaves room for a second pin (metadata vs. data, matching
    /// `minimum_reader_sequence` vs. `minimum_metadata_reader_sequence`
    /// in native.zig today) without another format bump.
    _reserved: [8]u8 = @splat(0),
};

const reader_slot_bytes = 32;

comptime {
    std.debug.assert(@sizeOf(ReaderSlot) == reader_slot_bytes);
}

/// Every opener that doesn't ask for a specific count gets this many
/// reader slots -- generous (32 KiB of slot array) specifically so this
/// module never needs to grow the count after creation; see
/// `max_readers`'s own doc comment for why growth is deliberately not
/// supported. Matches the order of magnitude LMDB's own fixed
/// `maxreaders` default uses for the same reason. Revisit the exact
/// number once a caller actually wires this region in (§8.7), but keep
/// it fixed-at-creation either way.
pub const default_max_readers: u32 = 1024;

fn headerChecksum(header: *const Header) u32 {
    const Crc32c = @import("antfly_hash").Crc32c;
    // Exactly the immutable-identity span of `Header` (see its own
    // comment): `region_bytes` through the end of `_immutable_reserved`,
    // offsets 16..32. Two things make this slice specific, not just "the
    // rest of the struct":
    //
    // 1. It excludes `magic`/`format_version`/`header_checksum` (offsets
    //    0..16): those are checked directly in `isValidHeader`, and
    //    starting the checksum's own input at its own field's offset (12)
    //    instead of past it (16) would make the checksum cover its own
    //    stored bytes -- self-referential, so a header written with the
    //    placeholder `0` there computes one value, and every later read
    //    (with the real, now-nonzero checksum in that spot) computes a
    //    different one. A header would then fail validation against
    //    itself immediately after being correctly written.
    // 2. It excludes everything from offset 32 onward (`writer_holder`
    //    and later): those fields are live and mutated by ordinary
    //    operation, so a checksum covering them would compare a
    //    creation-time snapshot against a post-use recomputation and
    //    call the mismatch "corrupt" -- exactly what happened before this
    //    comment existed, caught by this step's own close-then-reopen
    //    test (not step 1's corrupt-header test, which corrupts before
    //    reopening and so never compares a valid header's checksum
    //    against a fresh recomputation of it).
    const bytes = std.mem.asBytes(header);
    return Crc32c.hash(bytes[16..32]);
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

    /// Opens (creating if absent) the region next to `db_path` with
    /// `default_max_readers` slots. See `openSized` for a caller that
    /// needs a specific count.
    pub fn open(allocator: std.mem.Allocator, io: Io, db_path: []const u8) !Region {
        return openSized(allocator, io, db_path, default_max_readers);
    }

    /// Opens (creating if absent) the region next to `db_path`, e.g.
    /// `<db_path>.aflite-lock`, under one `flock` -- the only `flock` this
    /// module issues, and only here, never per acquire/release/claim/pin/
    /// unpin. `allocator` is used only for the temporary path string;
    /// nothing from the region itself is heap allocated.
    ///
    /// `max_readers` picks the slot count *only if this call creates the
    /// region*; an existing region keeps whatever count it was created
    /// with regardless of what this call asks for (see `max_readers`'s
    /// own doc comment for why this module deliberately never changes it
    /// afterward -- two already-open processes briefly disagreeing about
    /// the slot count would mean a missed pin). Callers that don't care
    /// should pass `default_max_readers`, which `open` does for them.
    pub fn openSized(allocator: std.mem.Allocator, io: Io, db_path: []const u8, max_readers: u32) !Region {
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
            var existing_raw: [header_bytes]u8 = undefined;
            const n = try setup_file.readPositionalAll(io, &existing_raw, 0);
            need_init = n != existing_raw.len or !isValidHeader(&existing_raw);
        }
        if (need_init) {
            const region_bytes = header_bytes + @as(u64, max_readers) * reader_slot_bytes;
            try setup_file.setLength(io, region_bytes);
            var fresh: Header = .{
                .magic = magic,
                .format_version = format_version,
                .header_checksum = 0,
                .region_bytes = region_bytes,
                .writer_holder = .init(0),
                .writer_ticket_tail = .init(0),
                .writer_ticket_head = .init(0),
                .max_readers = max_readers,
                .reader_slot_released = .init(0),
            };
            fresh.header_checksum = headerChecksum(&fresh);
            try setup_file.writePositionalAll(io, std.mem.asBytes(&fresh), 0);
            try setup_file.sync(io);
        }

        // Re-read the header fresh: `need_init` above may have just set
        // `region_bytes`, and this is the only place that decides how
        // much of the file to map. An existing region's `max_readers` (and
        // therefore `region_bytes`) is authoritative regardless of what
        // this call's own `max_readers` argument asked for.
        var final_raw: [header_bytes]u8 = undefined;
        _ = try setup_file.readPositionalAll(io, &final_raw, 0);
        const final_header: *const Header = @ptrCast(@alignCast(&final_raw));
        const map_len: usize = @intCast(final_header.region_bytes);

        // mmap needs its own raw fd (platform `mmap` takes a POSIX fd, not
        // an `Io.File`); opened read-write, close-on-exec so a forked-and-
        // exec'd child never inherits a stale mapping-backing descriptor.
        const raw_fd = std.posix.openat(std.posix.AT.FDCWD, region_path, .{ .ACCMODE = .RDWR, .CLOEXEC = true }, 0) catch return RegionError.RegionUnavailable;
        errdefer _ = std.posix.system.close(raw_fd);
        const mapping = std.posix.mmap(null, map_len, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, raw_fd, 0) catch return RegionError.RegionUnavailable;
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

    /// §8.3: the fixed reader-slot array immediately following the
    /// header, sized by the header's own `max_readers` -- fixed forever
    /// at this region's first creation (see `max_readers`'s own doc
    /// comment), so this is a read of a value that never changes across
    /// this `Region` handle's lifetime, not a dynamic resize check.
    fn readerSlots(self: *const Region) []ReaderSlot {
        const max_readers = self.header().max_readers;
        const base: [*]ReaderSlot = @ptrCast(@alignCast(self.mapping.ptr + header_bytes));
        return base[0..max_readers];
    }

    /// LMDB calls the same condition (a fixed `maxreaders` table with no
    /// free slot) exactly this; matching its name here is deliberate,
    /// since this module makes the same fixed-capacity choice LMDB does
    /// and for the same reason (§8.3's `max_readers` doc comment).
    pub const ReaderError = error{ReadersExhausted};

    /// Claims the first free slot (`owner == 0`) via CAS -- the same
    /// uncontended-fast-path shape as the writer word, acceptable to scan
    /// linearly under contention because this runs once per reader
    /// lifetime, not once per commit. Returns the slot index, or
    /// `error.ReadersExhausted` immediately if every slot is held (by a
    /// live reader, or -- until §8.4's liveness probe lands in step 3 --
    /// by a dead one that never released its slot). See
    /// `claimReaderSlotWait` for a caller that would rather block for a
    /// bounded time than fail immediately.
    pub fn claimReaderSlot(self: *Region, pid: u32) ReaderError!u32 {
        for (self.readerSlots(), 0..) |*slot, i| {
            if (slot.owner.cmpxchgStrong(0, pid, .acquire, .monotonic) == null) {
                slot.generation = packHolder(pid, 0);
                slot.pinned_sequence.store(0, .monotonic);
                return @intCast(i);
            }
        }
        return ReaderError.ReadersExhausted;
    }

    /// `claimReaderSlot`, but on exhaustion parks on the real cross-
    /// process futex `releaseReaderSlot` wakes (the same shape as
    /// `acquireWriter`'s own wait, just woken by *any* release rather
    /// than a specific ticket, since a free-slot race between waiters is
    /// resolved by the same CAS loop either way) instead of failing
    /// immediately, bounded by `deadline_ms`. Returns
    /// `error.ReadersExhausted` if no slot frees up before the deadline,
    /// never hangs past it -- the fixed-capacity table's answer to
    /// exhaustion that `max_readers`'s own doc comment points to, not
    /// growth.
    pub fn claimReaderSlotWait(self: *Region, pid: u32, deadline_ms: i64) ReaderError!u32 {
        const hdr = self.header();
        var waited_ms: i64 = 0;
        while (true) {
            if (self.claimReaderSlot(pid)) |slot| return slot else |_| {}
            if (waited_ms >= deadline_ms) return ReaderError.ReadersExhausted;
            const before = hdr.reader_slot_released.load(.acquire);
            // Re-check after reading `before`, not before: a release that
            // landed between the failed claim above and this load would
            // otherwise be missed by the wait below (the same
            // lost-wakeup shape `acquireWriter`'s own doc comment warns
            // about), since `futexWaitShared` only blocks while the word
            // still equals `before`.
            if (self.claimReaderSlot(pid)) |slot| return slot else |_| {}
            const remaining_ms = deadline_ms - waited_ms;
            const step_ms: i32 = @intCast(@min(remaining_ms, 50));
            futexWaitSharedTimed(&hdr.reader_slot_released.raw, before, step_ms);
            waited_ms += step_ms;
        }
    }

    /// Records the `commit_sequence` this reader will not let the writer
    /// reclaim pages behind. Written once per pin by the slot's own
    /// owner.
    pub fn pinReader(self: *Region, slot_index: u32, sequence: u64) void {
        self.readerSlots()[slot_index].pinned_sequence.store(sequence, .release);
    }

    /// Clears the pin recorded by `pinReader`, without releasing the slot
    /// itself -- the same `NativeFile`/pin lifetime may pin again.
    pub fn unpinReader(self: *Region, slot_index: u32) void {
        self.readerSlots()[slot_index].pinned_sequence.store(0, .release);
    }

    /// Frees a slot claimed by `claimReaderSlot` for reuse, and wakes
    /// anyone parked in `claimReaderSlotWait` on exhaustion. Callers must
    /// unpin first; this clears the pin defensively anyway so a slot
    /// released while still (incorrectly) pinned never blocks
    /// `minimumPinnedSequence` after release.
    pub fn releaseReaderSlot(self: *Region, slot_index: u32) void {
        const slot = &self.readerSlots()[slot_index];
        slot.pinned_sequence.store(0, .monotonic);
        slot.generation = 0;
        slot.owner.store(0, .release);
        const hdr = self.header();
        _ = hdr.reader_slot_released.fetchAdd(1, .release);
        futexWakeShared(&hdr.reader_slot_released.raw);
    }

    /// §8.3's writer-side reuse-frontier scan, replacing
    /// `scanReaderEpochsCached`'s namespace-directory walk: the minimum
    /// `pinned_sequence` over every slot with a live owner, or `null` if
    /// no reader is currently pinned. "Live" here is just `owner != 0`;
    /// step 3's per-slot liveness byte-range probe (§8.4) replaces that
    /// with a real death check so a crashed reader's never-cleared pin
    /// cannot block reclamation forever -- until then, a process that
    /// dies while pinned leaks its pin here, same as it would in
    /// `reader_epochs.zig` today without a sweep. Nothing calls this from
    /// a real write path yet (§8.7's migration is a later step); it is
    /// exercised only by this file's own tests.
    pub fn minimumPinnedSequence(self: *const Region) ?u64 {
        var min: ?u64 = null;
        for (self.readerSlots()) |*slot| {
            if (slot.owner.load(.acquire) == 0) continue;
            const seq = slot.pinned_sequence.load(.acquire);
            if (seq == 0) continue;
            if (min == null or seq < min.?) min = seq;
        }
        return min;
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

/// `futexWaitShared`, bounded by `timeout_ms` instead of parking forever.
/// `claimReaderSlotWait` is this file's only caller: the writer/ticket
/// wait above never needs a timeout (a correct implementation hands off
/// in about one commit, not on a deadline), but `max_readers`'s own doc
/// comment makes exhaustion a real, caller-visible outcome a reader must
/// be able to give up on.
fn futexWaitSharedTimed(ptr: *const u32, expect: u32, timeout_ms: i32) void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            const ts: linux.timespec = .{
                .sec = @intCast(@divTrunc(timeout_ms, 1000)),
                .nsec = @intCast(@mod(timeout_ms, 1000) * std.time.ns_per_ms),
            };
            const rc = linux.futex_4arg(ptr, .{ .cmd = .WAIT, .private = false }, expect, &ts);
            _ = linux.errno(rc);
        },
        .macos, .ios, .tvos, .watchos, .visionos, .maccatalyst, .driverkit => {
            const c = std.c;
            const flags: c.UL = .{ .op = .COMPARE_AND_WAIT_SHARED, .NO_ERRNO = true };
            const timeout_us: u32 = @intCast(@as(i64, timeout_ms) * 1000);
            _ = c.__ulock_wait(flags, @ptrCast(@constCast(ptr)), expect, timeout_us);
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

test "shared lock region: reader slots claim, pin, and release round-trip" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-readers.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.openSized(a, io, db_path, 4);
    defer region.close();

    // No reader pinned yet.
    try std.testing.expectEqual(@as(?u64, null), region.minimumPinnedSequence());

    const slot_a = try region.claimReaderSlot(100);
    const slot_b = try region.claimReaderSlot(200);
    try std.testing.expect(slot_a != slot_b);

    region.pinReader(slot_a, 50);
    region.pinReader(slot_b, 30);
    // The frontier is the *minimum* pin across live readers.
    try std.testing.expectEqual(@as(?u64, 30), region.minimumPinnedSequence());

    region.unpinReader(slot_b);
    try std.testing.expectEqual(@as(?u64, 50), region.minimumPinnedSequence());

    region.releaseReaderSlot(slot_a);
    try std.testing.expectEqual(@as(?u64, null), region.minimumPinnedSequence());

    // A released slot is claimable again.
    const slot_c = try region.claimReaderSlot(300);
    try std.testing.expectEqual(slot_a, slot_c);
}

test "shared lock region: reader slots exhaust and report ReadersExhausted" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-readers-full.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.openSized(a, io, db_path, 2);
    defer region.close();

    _ = try region.claimReaderSlot(1);
    _ = try region.claimReaderSlot(2);
    try std.testing.expectError(Region.ReaderError.ReadersExhausted, region.claimReaderSlot(3));
}

// `claimReaderSlotWait` must not hang the suite when nothing ever frees a
// slot -- the same "bounded, not a timer" posture `acquireWriter`'s
// cross-process tests exercise, just for the reader table's own
// exhaustion path instead of the writer word.
test "shared lock region: claimReaderSlotWait is bounded when nothing frees a slot" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-readers-wait-timeout.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.openSized(a, io, db_path, 1);
    defer region.close();

    _ = try region.claimReaderSlot(1);
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const start_ns: i64 = @as(i64, ts.sec) * std.time.ns_per_s + ts.nsec;

    try std.testing.expectError(Region.ReaderError.ReadersExhausted, region.claimReaderSlotWait(2, 100));

    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const end_ns: i64 = @as(i64, ts.sec) * std.time.ns_per_s + ts.nsec;
    // Bounded by the deadline, generously: well under a second, not
    // hung, and not wildly over the 100ms asked for either.
    try std.testing.expect(end_ns - start_ns < 1 * std.time.ns_per_s);
}

// A second process releases the only slot while this process is parked
// in `claimReaderSlotWait`; the release's futex wake must actually reach
// it well inside the deadline, not merely before the deadline expires by
// coincidence.
test "shared lock region: claimReaderSlotWait wakes when another process releases a slot" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-readers-wait-wake.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.openSized(a, io, db_path, 1);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    const held_slot = try region.claimReaderSlot(own_pid);

    var pipe_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&pipe_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(pipe_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        // Blocks on the real cross-process futex until the parent's
        // release below wakes it (deadline generous enough that only a
        // genuine lost wakeup, not scheduling noise, would exhaust it).
        const slot = region.claimReaderSlotWait(my_pid, 2000) catch 0xFFFF_FFFF;
        var report: [4]u8 = undefined;
        std.mem.writeInt(u32, &report, slot, .little);
        _ = std.c.write(pipe_fds[1], @ptrCast(&report), report.len);
        std.c._exit(0);
    }
    _ = std.posix.system.close(pipe_fds[1]);
    defer _ = std.posix.system.close(pipe_fds[0]);

    // Hold briefly, like a real reader's own pin lifetime, then release
    // -- the child must already be parked by the time this runs for the
    // test to exercise the wait/wake path rather than racing it.
    try io.sleep(.fromMilliseconds(20), .awake);
    region.releaseReaderSlot(held_slot);

    var report: [4]u8 = undefined;
    try readBounded(pipe_fds[0], &report, 2000);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pid, 2000));

    const got_slot = std.mem.readInt(u32, &report, .little);
    try std.testing.expectEqual(held_slot, got_slot);
}

test "shared lock region: reader slots are visible across a second handle" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-readers-shared.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var first = try Region.openSized(a, io, db_path, 4);
    defer first.close();
    var second = try Region.openSized(a, io, db_path, 4);
    defer second.close();

    const slot = try first.claimReaderSlot(42);
    first.pinReader(slot, 7);
    // The second mapping must observe the first's pin: same pages, not an
    // independent copy.
    try std.testing.expectEqual(@as(?u64, 7), second.minimumPinnedSequence());

    second.unpinReader(slot);
    try std.testing.expectEqual(@as(?u64, null), first.minimumPinnedSequence());
}

// `max_readers`'s own doc comment explains why this module deliberately
// never changes a region's slot count after creation: a later opener's
// own `max_readers` argument is only ever a hint for *creating* a fresh
// region, never a request to resize an existing one, no matter whether
// it asks for more or fewer slots than the region already has.
test "shared lock region: an existing region's reader slot count is authoritative, not the later opener's hint" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-fixed-capacity.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    {
        var created = try Region.openSized(a, io, db_path, 2);
        created.close();
    }

    // A later opener asking for *more* slots than the region was created
    // with still only gets the original 2 -- not 8, and not a mix.
    {
        var reopened = try Region.openSized(a, io, db_path, 8);
        defer reopened.close();
        const s1 = try reopened.claimReaderSlot(1);
        const s2 = try reopened.claimReaderSlot(2);
        try std.testing.expectError(Region.ReaderError.ReadersExhausted, reopened.claimReaderSlot(3));
        // Release before closing: the next block reopens the same region
        // and needs both original slots free again.
        reopened.releaseReaderSlot(s1);
        reopened.releaseReaderSlot(s2);
    }

    // Nor does asking for *fewer* slots shrink it or otherwise disturb
    // the on-disk state (writer lock included).
    {
        var reopened = try Region.openSized(a, io, db_path, 1);
        defer reopened.close();
        try std.testing.expect(reopened.tryAcquireWriter(1));
        reopened.releaseWriter();
        _ = try reopened.claimReaderSlot(10);
        _ = try reopened.claimReaderSlot(20);
        try std.testing.expectError(Region.ReaderError.ReadersExhausted, reopened.claimReaderSlot(30));
    }
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
