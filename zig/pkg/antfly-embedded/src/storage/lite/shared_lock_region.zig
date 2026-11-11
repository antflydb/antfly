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

/// Packs a pid with a *liveness slot index* into a holder/generation word
/// (not a counter-style "generation", despite the name kept for
/// `Header`'s own field comments written before this existed): §8.4's
/// per-process liveness byte-range lock lives at a fixed index, claimed
/// once by a process the first time it opens the region (`openSized`'s
/// `claimLivenessSlot`) and held for that `Region` handle's whole
/// lifetime -- not re-taken on every `acquireWriter`/`claimReaderSlot`
/// call, which is what keeps this design's "0 syscalls on a warm,
/// uncontended write" property intact even with liveness tracking in
/// place. `holderPid`/`holderLivenessSlot` unpack this word to answer "is
/// the process that wrote this word still alive" by probing *that*
/// index's liveness byte (`probeDead`), not by re-deriving anything from
/// the pid itself -- no PID-reuse heuristic anywhere in this design.
fn packHolder(pid: u32, liveness_slot: u32) u64 {
    return (@as(u64, liveness_slot) << 32) | @as(u64, pid);
}

fn holderPid(word: u64) u32 {
    return @truncate(word);
}

fn holderLivenessSlot(word: u64) u32 {
    return @truncate(word >> 32);
}

/// True when mmap or the shared-futex self-test is unavailable and callers
/// must fall back to the flock-based path this module replaces (§8.6).
/// Step 1 does not yet implement that fallback path itself -- it returns
/// `error.RegionUnavailable` instead, so `inode_lock.zig` (unreferenced by
/// this module so far) keeps serving every caller until a later step wires
/// the fallback flag through.
///
/// `RegionCorrupt`: the on-disk header failed validation (bad magic,
/// version, or checksum) *and* at least one liveness byte-range lock
/// (§8.4) is still held by a live process, so the region cannot be safely
/// rebuilt -- doing so anyway would silently break writer exclusion and
/// reader pins for whoever is still relying on it. Rebuilding only
/// happens once every liveness slot's own nonblocking trylock proves no
/// live process holds it; otherwise `openSized` refuses with this error
/// instead of wiping.
///
/// `LivenessSlotsExhausted`: every one of the region's `max_readers`
/// liveness slots is already held by another live process. Distinct from
/// `ReaderError.ReadersExhausted` (that's about reader *pin* slots, which
/// this error precedes logically -- a process claims a liveness slot once
/// at open, long before it ever claims or releases reader pin slots).
pub const RegionError = error{ RegionUnavailable, RegionCorrupt, LivenessSlotsExhausted };

/// §8.4: `fcntl(F_SETLK)` *byte-range* record locking -- a different
/// primitive from this module's one whole-file `flock` at setup, declared
/// here because liveness specifically needs per-byte-range granularity
/// `flock` cannot give. The kernel drops a process's `fcntl` locks
/// unconditionally on its death (`exit`, `kill -9`, OOM-kill) with no
/// cleanup-code dependency, which is the only liveness signal this module
/// trusts -- there is no PID-reuse heuristic anywhere in it.
///
/// Nonblocking (`F_SETLK`, not `F_SETLKW`): on success the byte range is
/// now locked *by this process*; on failure (`EAGAIN`/`EACCES`) some
/// other live process already holds it. Never blocks, by construction --
/// every caller in this file either already knows the range should be
/// free (claiming its own liveness slot) or is specifically testing
/// whether a *recorded* holder is still alive (`probeDead`), never
/// waiting to become one.
fn fcntlTryLock(fd: std.posix.fd_t, offset: i64, lock_type: i16) bool {
    var fl: std.c.Flock = .{ .start = offset, .len = 1, .pid = 0, .type = lock_type, .whence = std.c.SEEK.SET };
    return std.c.fcntl(fd, std.c.F.SETLK, &fl) != -1;
}

fn fcntlUnlock(fd: std.posix.fd_t, offset: i64) void {
    var fl: std.c.Flock = .{ .start = offset, .len = 1, .pid = 0, .type = std.c.F.UNLCK, .whence = std.c.SEEK.SET };
    _ = std.c.fcntl(fd, std.c.F.SETLK, &fl);
}

/// Blocking `fcntl` byte-range lock, retried across `EINTR`. Used only
/// for the one-time create/validate section's own exclusive lock (see
/// `setup_lock_offset`) -- everywhere else in this module takes the
/// nonblocking `fcntlTryLock` instead.
fn fcntlLockBlocking(fd: std.posix.fd_t, offset: i64, lock_type: i16) void {
    var fl: std.c.Flock = .{ .start = offset, .len = 1, .pid = 0, .type = lock_type, .whence = std.c.SEEK.SET };
    while (std.c.fcntl(fd, std.c.F.SETLKW, &fl) == -1) {
        if (std.c._errno().* != @backingInt(std.c.E.INTR)) return;
    }
}

/// A fixed byte offset, far beyond any real region's actual size, used
/// purely as the one-time create/validate section's own lock target --
/// never real data, never a liveness slot's own byte.
///
/// This exists specifically because the obvious alternative --
/// `Io.File.lock`'s whole-file `flock()` -- and `fcntl(F_SETLK)` byte-
/// range locks are *not* independent locking domains on every platform
/// this runs on: on this Darwin kernel, holding an exclusive `flock()`
/// on a file blocks that *same process's* own subsequent
/// `fcntl(F_SETLK)` attempt on the same file, confirmed empirically (the
/// very first liveness-slot claim on a brand-new, otherwise-uncontended
/// region failed with `EAGAIN` as long as the one-time section still
/// held `Io.File.lock`'s `flock`). Using `fcntl` end to end, including
/// for this one-time section, avoids that cross-mechanism conflict
/// entirely rather than working around it.
const setup_lock_offset: i64 = 1 << 48;

/// The byte-range liveness probe §8.4 describes: `true` means the
/// recorded holder at this byte is dead (the kernel already dropped its
/// lock, so this process's own trylock just succeeded -- immediately
/// released again, since a probe is not a claim); `false` means it is
/// genuinely still running (the trylock failed because someone else
/// holds it). This is the *only* liveness signal anywhere in this
/// module.
fn probeDead(fd: std.posix.fd_t, offset: i64) bool {
    if (!fcntlTryLock(fd, offset, std.c.F.WRLCK)) return false;
    fcntlUnlock(fd, offset);
    return true;
}

/// The third POSIX trap this design has to avoid (after flock/fcntl
/// cross-conflict and the self-probe issue): closing *any* fd a process
/// holds on a file releases *all* of that process's `fcntl` locks on
/// that file, not just the one being closed. Two `Region` handles in one
/// process that each opened their own fd on the same region file would
/// therefore have `first.close()` silently drop `second`'s liveness
/// lock too -- a forked peer would then see this process's `second`
/// handle as dead when it is not. This is the exact reason SQLite keeps
/// `unixInodeInfo` and `inode_lock.zig` keeps its own `registry`/`Entry`
/// pair in this codebase: exactly one fd per (process, inode),
/// reference-counted, closed only when the last handle referencing it
/// goes away.
const FdEntry = struct {
    inode: Io.File.INode,
    fd: std.posix.fd_t,
    refs: usize = 0,
    /// Liveness slot indices *this process* has already claimed through
    /// some `Region` handle sharing this entry. `fcntl` locks are
    /// process-scoped, not per-claim: a second same-process attempt to
    /// lock an index this process already holds would trivially
    /// "succeed" (re-locking your own lock is a no-op), so without this
    /// bookkeeping `claimLivenessSlot` could hand out the *same* index to
    /// two different handles in this process. Kept in memory here
    /// because the kernel has no notion of "claimed twice by the same
    /// process" to ask about. A slot is removed from this list in
    /// `Region.close` at the same time its real lock is released, so a
    /// later same-process claim can reuse it.
    claimed_slots: std.ArrayListUnmanaged(u32) = .empty,
};

// `Io.Mutex`, matching `inode_lock.zig`'s own registry mutex -- never a
// raw `std.Thread.Mutex`/`std.atomic.Mutex`, so holding it is always
// accounted for by whatever cooperative scheduler this process's `Io`
// implementation runs (including, correctly, one that happens to be
// single-threaded). Every critical section it guards is brief and
// bounded (an in-memory scan plus at most one *nonblocking*
// `fcntl(F_SETLK)` call, never `F_SETLKW`), so `lockUncancelable` is
// appropriate here the same way it is in `inode_lock.zig`'s own `open`.
var fd_registry_mutex: Io.Mutex = .init;
var fd_registry: std.AutoHashMapUnmanaged(Io.File.INode, *FdEntry) = .empty;

/// Returns this process's single shared `FdEntry` for `region_path`,
/// opening and registering one on first use and incrementing its
/// refcount otherwise. Every `Region` handle in this process that opens
/// the same region file shares this exact fd for `mmap` and every
/// `fcntl` call (liveness, the one-time setup lock) -- never a second,
/// independently opened fd on the same inode. Paired with
/// `releaseProcessFd`.
fn acquireProcessFd(io: Io, region_path: []const u8) RegionError!*FdEntry {
    const inode = (Io.Dir.cwd().statFile(io, region_path, .{}) catch return RegionError.RegionUnavailable).inode;
    fd_registry_mutex.lockUncancelable(io);
    defer fd_registry_mutex.unlock(io);
    if (fd_registry.get(inode)) |entry| {
        entry.refs += 1;
        return entry;
    }
    // Opened read-write, close-on-exec so a forked-and-exec'd child never
    // inherits a stale mapping/lock-backing descriptor.
    const raw_fd = std.posix.openat(std.posix.AT.FDCWD, region_path, .{ .ACCMODE = .RDWR, .CLOEXEC = true }, 0) catch return RegionError.RegionUnavailable;
    errdefer _ = std.posix.system.close(raw_fd);
    const page = std.heap.page_allocator;
    const entry = page.create(FdEntry) catch return RegionError.RegionUnavailable;
    entry.* = .{ .inode = inode, .fd = raw_fd, .refs = 1 };
    fd_registry.put(page, inode, entry) catch {
        page.destroy(entry);
        return RegionError.RegionUnavailable;
    };
    return entry;
}

/// Drops this process's reference to `entry`, actually closing its fd
/// (and releasing every `fcntl` lock this process held via it, including
/// every other still-live `Region` handle's own liveness slot -- by
/// construction, since they share this one fd) only once no `Region`
/// handle in this process references it anymore.
fn releaseProcessFd(io: Io, entry: *FdEntry) void {
    fd_registry_mutex.lockUncancelable(io);
    defer fd_registry_mutex.unlock(io);
    entry.refs -= 1;
    if (entry.refs == 0) {
        _ = std.posix.system.close(entry.fd);
        _ = fd_registry.remove(entry.inode);
        entry.claimed_slots.deinit(std.heap.page_allocator);
        std.heap.page_allocator.destroy(entry);
    }
}

pub const Region = struct {
    mapping: []align(std.heap.page_size_min) u8,
    /// This handle's own reference into `fd_registry` -- this process's
    /// *shared* `FdEntry` for the region's inode (see
    /// `acquireProcessFd`'s own doc comment). Its `fd` is reused for
    /// every `fcntl` byte-range call (§8.4) as well as `munmap`'s own
    /// backing-file reference, never a second, independently opened fd.
    /// Never close `fd_entry.fd` directly; `close` goes through
    /// `releaseProcessFd`.
    fd_entry: *FdEntry,
    /// §8.4: the liveness slot this process claimed for itself in `open`/
    /// `openSized`, held (one real `fcntl` byte-range lock, taken once)
    /// for this `Region` handle's entire lifetime, released in `close`.
    /// `tryAcquireWriter`/`claimReaderSlot` pack this into `writer_holder`/
    /// a reader slot's `generation` on every call, but that's a cheap
    /// struct-field read, not a syscall -- the actual lock was already
    /// taken once, here, which is what keeps acquire/claim/pin/unpin at
    /// zero additional syscalls even with liveness tracking in place.
    own_liveness_slot: u32,
    /// The `Io` this handle was opened with, kept so `close`,
    /// `claimReaderSlot`, `minimumPinnedSequence`, `writerHolderDead`,
    /// and `recoverDeadWriter` don't need it threaded through their own
    /// signatures again. Deliberately *not* used by `acquireWriter`/
    /// `releaseWriter`/`tryAcquireWriter`/`claimReaderSlotWait`, which
    /// stay `Io`-free by construction: a forked child inherits this
    /// `Region` value (and therefore this field) but not the `Io`
    /// instance's own background threads, so calling into `Io` from a
    /// freshly forked child before it execs would be unsafe. See the
    /// two-process contention test's own doc comment for the fuller
    /// version of this reasoning.
    io: Io,

    fn header(self: *const Region) *Header {
        return @ptrCast(@alignCast(self.mapping.ptr));
    }

    /// Byte offset of liveness slot `slot_index`'s dedicated byte,
    /// immediately after the reader slot array (§8.1's "M*1 process
    /// liveness bytes, one per slot"). Content is never read or written;
    /// the byte exists purely as an `fcntl` lock target.
    fn livenessByteOffset(max_readers: u32, slot_index: u32) i64 {
        return @as(i64, header_bytes) + @as(i64, max_readers) * reader_slot_bytes + @as(i64, slot_index);
    }

    /// `probeDead`, but for *this process's own* liveness slot index
    /// (`writer_holder`/a reader slot's `generation` can legitimately
    /// name this process's own slot -- it is this process's own writer
    /// hold, or its own reader pin), always reports alive without
    /// touching `fcntl` at all. `fcntl` locks are not reference-counted
    /// per logical claim: a trylock-then-unlock probe against a byte
    /// *this same process* already holds would trivially "succeed" (you
    /// can always re-lock your own lock) and then the probe's own
    /// cleanup unlock would release the real, still-in-use lock out from
    /// under it -- this guard exists specifically so a process never
    /// probes itself into releasing its own liveness.
    fn isSlotDead(self: *const Region, slot_index: u32) bool {
        // Not just `slot_index == self.own_liveness_slot`: `fcntl` locks
        // are process-scoped, and every `Region` handle this process has
        // open on this same file shares `self.fd_entry` (and therefore
        // its one fd) -- `slot_index` could legitimately be a *different*
        // handle's own slot, not just this one's. Probing it anyway
        // would trivially "succeed" (re-locking your own process's lock
        // is a no-op) and then release that other handle's real lock out
        // from under it, same failure shape `own_liveness_slot` alone
        // guarded against for the single-handle case.
        fd_registry_mutex.lockUncancelable(self.io);
        const mine = for (self.fd_entry.claimed_slots.items) |claimed| {
            if (claimed == slot_index) break true;
        } else false;
        fd_registry_mutex.unlock(self.io);
        if (mine) return false;
        return probeDead(self.fd_entry.fd, livenessByteOffset(self.header().max_readers, slot_index));
    }

    /// Scans liveness slots `[0, max_readers)` for the first one this
    /// process can claim, holding it for the rest of this `Region`
    /// handle's lifetime. Called once per handle, from `openSized`,
    /// never from the per-commit acquire/claim paths.
    ///
    /// Skips any index already in `entry.claimed_slots` *before* trying
    /// `fcntl` at all: `fcntl` locks are process-scoped, so a second
    /// same-process handle's trylock on an index this process already
    /// holds would otherwise trivially "succeed" (re-locking your own
    /// lock is a no-op) and hand out the exact same index twice, which
    /// `entry.claimed_slots`'s own doc comment explains is wrong. Holding
    /// `fd_registry_mutex` for the whole scan keeps two concurrent same-
    /// process claims (different threads, same `FdEntry`) from racing
    /// each other onto the same index between the check and the trylock.
    fn claimLivenessSlot(io: Io, entry: *FdEntry, max_readers: u32) RegionError!u32 {
        fd_registry_mutex.lockUncancelable(io);
        defer fd_registry_mutex.unlock(io);
        var i: u32 = 0;
        next_index: while (i < max_readers) : (i += 1) {
            for (entry.claimed_slots.items) |claimed| {
                if (claimed == i) continue :next_index;
            }
            if (fcntlTryLock(entry.fd, livenessByteOffset(max_readers, i), std.c.F.WRLCK)) {
                entry.claimed_slots.append(std.heap.page_allocator, i) catch return RegionError.LivenessSlotsExhausted;
                return i;
            }
        }
        return RegionError.LivenessSlotsExhausted;
    }

    /// A forked child that inherited this `Region` value (the module doc
    /// comment's own fork pattern, and every cross-process test below)
    /// inherits `own_liveness_slot`'s *number*, but not the real `fcntl`
    /// lock behind it: `fcntl` locks are process-scoped and, per POSIX,
    /// never inherited across `fork` as the child's own, so the real
    /// lock on that byte is still held by the *parent*, who is typically
    /// still alive and unaffected. A child that is about to act as a
    /// writer or claim a reader slot of its own must call this first to
    /// claim a liveness slot under its *own* identity -- otherwise its
    /// death would never be detected (`isSlotDead` would see the index
    /// as "mine" via the inherited-but-stale `claimed_slots` bookkeeping
    /// and never even probe it) and a live parent's own slot could be
    /// wrongly implicated instead.
    /// Uses `self.io` for `fd_registry_mutex`, same as every other
    /// liveness call in this file -- safe for the simple, synchronous
    /// `Io` every test here uses post-fork, but not yet audited for a
    /// thread-pool-backed production `Io` (whose background threads this
    /// module's own fork-safety comments already warn don't survive
    /// `fork`). Tracked as part of the broader `Io`-usage sweep the rest
    /// of this design still owes (§8.9's later steps).
    pub fn reclaimLivenessAfterFork(self: *Region) RegionError!void {
        self.own_liveness_slot = try claimLivenessSlot(self.io, self.fd_entry, self.header().max_readers);
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
    /// region*; an existing valid region keeps whatever count it was
    /// created with regardless of what this call asks for (see
    /// `max_readers`'s own doc comment for why this module deliberately
    /// never changes it afterward -- two already-open processes briefly
    /// disagreeing about the slot count would mean a missed pin). Callers
    /// that don't care should pass `default_max_readers`, which `open`
    /// does for them.
    ///
    /// **Corruption is never silently wiped out from under a live
    /// peer.** If the on-disk header fails validation (bad magic,
    /// version, or checksum) on a file that is not simply too small to
    /// have ever held one, this call does **not** rebuild until it has
    /// proven, via a nonblocking `fcntl` trylock probe on every liveness
    /// byte the (untrusted) header claims to have, that no live process
    /// holds any of them. If even one probe fails, or the claimed slot
    /// count isn't even plausible for the file's actual size, this
    /// returns `error.RegionCorrupt` instead of reinitializing --
    /// rebuilding out from under a still-running writer or reader would
    /// silently break its exclusion/pin guarantees.
    pub fn openSized(allocator: std.mem.Allocator, io: Io, db_path: []const u8, max_readers: u32) !Region {
        const region_path = try std.fmt.allocPrint(allocator, "{s}.aflite-lock", .{db_path});
        defer allocator.free(region_path);

        const dir = Io.Dir.cwd();
        // Deliberately *not* a function-scoped `defer setup_file.close`:
        // see the comment right before this file closes, a few lines
        // below, for why it must close before `claimLivenessSlot` runs,
        // not at function exit.
        const setup_file = try dir.createFile(io, region_path, .{ .read = true, .truncate = false, .permissions = .fromMode(0o600) });
        errdefer setup_file.close(io);

        // This process's one shared fd for this inode (see
        // `acquireProcessFd`'s own doc comment for why it must be shared,
        // never a second fd opened independently) -- used for the one-
        // time create/validate lock below, the corruption-liveness
        // probe, `claimLivenessSlot`, and later `mmap`; `Io.File` has no
        // byte-range lock primitive of its own.
        const fd_entry = try acquireProcessFd(io, region_path);
        const raw_fd = fd_entry.fd;
        errdefer releaseProcessFd(io, fd_entry);
        // The only blocking lock this module takes, and only here: an
        // `fcntl` byte-range lock (not `Io.File.lock`'s whole-file
        // `flock`; see `setup_lock_offset`'s own doc comment for why)
        // serializing create/validate/rebuild against every other
        // process's own `openSized` call on this same region. Released
        // explicitly below, alongside `setup_file`'s own close -- not
        // deferred to function exit, same reasoning.
        fcntlLockBlocking(raw_fd, setup_lock_offset, std.c.F.WRLCK);
        errdefer fcntlUnlock(raw_fd, setup_lock_offset);

        const size = (try setup_file.stat(io)).size;
        const fresh_file = size < header_bytes;
        var need_init = fresh_file;
        var existing_raw: [header_bytes]u8 = undefined;
        if (!need_init) {
            const n = try setup_file.readPositionalAll(io, &existing_raw, 0);
            need_init = n != existing_raw.len or !isValidHeader(&existing_raw);
        }
        if (need_init and !fresh_file) {
            // A file that already existed and is at least header-sized,
            // but fails validation: prove no live process is relying on
            // it before even considering a rebuild.
            try ensureNoLivePeer(raw_fd, &existing_raw, size);
        }
        if (need_init) {
            const region_bytes = header_bytes + @as(u64, max_readers) * (reader_slot_bytes + 1);
            // Rebuild from a clean slate: a corrupt region's leftover
            // reader-slot/liveness bytes are never trusted either, only
            // the header's own validity was checked above. `setLength(0)`
            // then back up guarantees every byte -- header, reader slots,
            // and liveness bytes alike -- is freshly zero-filled, not
            // whatever garbage survived the corruption.
            try setup_file.setLength(io, 0);
            try setup_file.setLength(io, region_bytes);
            var fresh: Header = .{
                .magic = magic,
                .format_version = format_version,
                .header_checksum = 0,
                .region_bytes = region_bytes,
                .max_readers = max_readers,
                .writer_holder = .init(0),
                .writer_ticket_tail = .init(0),
                .writer_ticket_head = .init(0),
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
        const final_max_readers = final_header.max_readers;

        // `setup_file` must close -- and the setup lock release -- here,
        // *before* `claimLivenessSlot` below, not at function exit via
        // `defer`: closing *any* fd this process holds on a file (even
        // one, like `setup_file`, that never itself took a byte-range
        // lock) releases *every* `fcntl` lock this process holds on that
        // file, confirmed empirically on this kernel (a lock taken via
        // one fd vanished the instant an entirely separate, never-locked
        // fd on the same file was closed). A `defer` here would close
        // `setup_file` only after `claimLivenessSlot` had already taken
        // the real liveness lock this handle is supposed to hold for its
        // whole lifetime, silently dropping it moments after acquiring
        // it -- exactly the failure every one of this file's cross-
        // process liveness tests hit before this reordering.
        fcntlUnlock(raw_fd, setup_lock_offset);
        setup_file.close(io);

        const mapping = std.posix.mmap(null, map_len, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, raw_fd, 0) catch return RegionError.RegionUnavailable;
        errdefer std.posix.munmap(mapping);
        const own_liveness_slot = try claimLivenessSlot(io, fd_entry, final_max_readers);
        return .{ .mapping = mapping, .fd_entry = fd_entry, .own_liveness_slot = own_liveness_slot, .io = io };
    }

    /// The pre-rebuild safety check `openSized`'s doc comment describes.
    /// `existing_raw` is the (unvalidated -- that's the whole point)
    /// header this file already had. Succeeds only if every liveness
    /// byte it plausibly could have had is provably free.
    fn ensureNoLivePeer(fd: std.posix.fd_t, existing_raw: *const [header_bytes]u8, actual_file_bytes: u64) RegionError!void {
        const h: *const Header = @ptrCast(@alignCast(existing_raw));
        const claimed_max_readers = h.max_readers;
        // A sane cap well above anything `openSized` would ever actually
        // create (`default_max_readers` is 1024): without this, garbage
        // in a corrupt header's `max_readers` field could demand scanning
        // an absurd or overflowing range instead of simply being refused.
        const max_sane_readers: u32 = 1 << 20;
        if (claimed_max_readers == 0 or claimed_max_readers > max_sane_readers) {
            return RegionError.RegionCorrupt;
        }
        const implied_bytes = header_bytes + @as(u64, claimed_max_readers) * (reader_slot_bytes + 1);
        if (implied_bytes > actual_file_bytes) {
            // The claimed slot count doesn't even fit the file's actual
            // size -- the header is corrupt enough that this count can't
            // be trusted to define a real scan range either, so there is
            // no range left to prove safe. Refuse rather than guess.
            return RegionError.RegionCorrupt;
        }
        var i: u32 = 0;
        while (i < claimed_max_readers) : (i += 1) {
            const offset = livenessByteOffset(claimed_max_readers, i);
            if (!fcntlTryLock(fd, offset, std.c.F.WRLCK)) return RegionError.RegionCorrupt;
            // Just a probe, not a claim: release immediately so this
            // scan doesn't itself hold every liveness slot.
            fcntlUnlock(fd, offset);
        }
    }

    pub fn close(self: *Region) void {
        fcntlUnlock(self.fd_entry.fd, livenessByteOffset(self.header().max_readers, self.own_liveness_slot));
        // Remove this handle's own claim bookkeeping (see
        // `FdEntry.claimed_slots`'s own doc comment) now that the real
        // lock above is actually released, so a later same-process
        // `Region.open` on this file can reclaim this exact index instead
        // of `claimLivenessSlot` skipping it forever.
        fd_registry_mutex.lockUncancelable(self.io);
        for (self.fd_entry.claimed_slots.items, 0..) |slot, i| {
            if (slot == self.own_liveness_slot) {
                _ = self.fd_entry.claimed_slots.swapRemove(i);
                break;
            }
        }
        fd_registry_mutex.unlock(self.io);
        std.posix.munmap(self.mapping);
        // Never close `self.fd_entry.fd` directly: it is this process's
        // *shared* fd for the inode (see `acquireProcessFd`), and
        // closing any fd referencing a file drops every `fcntl` lock
        // this process holds on it -- including another still-live
        // `Region` handle's own liveness slot, since they share this
        // same fd. `releaseProcessFd` only actually closes it once this
        // was the last reference.
        releaseProcessFd(self.io, self.fd_entry);
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
    /// currentId()`, never 0 -- a 0 holder word means free). Packs this
    /// process's own (already-claimed, at-open-time) liveness slot
    /// alongside `pid` -- a struct-field read, not a syscall -- so a
    /// later `writerHolderDead` probe knows exactly which byte answers
    /// "is the process that wrote this word still alive".
    fn tryAcquireWriter(self: *Region, pid: u32) bool {
        const want = packHolder(pid, self.own_liveness_slot);
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

    /// §8.4: `true` only when `writer_holder` is held (nonzero) *and* the
    /// liveness probe on the pid+slot packed into it proves that process
    /// is dead -- never true for a free word (nothing to recover) and
    /// never true for a live holder (the normal, overwhelmingly common
    /// case: a waiter should keep waiting on the futex, not spin probing
    /// `fcntl` on every iteration).
    pub fn writerHolderDead(self: *const Region) bool {
        const word = self.header().writer_holder.load(.acquire);
        if (word == 0) return false;
        return self.isSlotDead(holderLivenessSlot(word));
    }

    /// Runs `recovery` (meant to be `native.zig`'s own existing
    /// `recoverActiveCheckpoint`/slot-and-manifest validation -- this
    /// module does not reimplement what that already checks, and taking
    /// a callback instead of calling it directly keeps this module
    /// decoupled from `native.zig` until §8.7's migration step actually
    /// wires a caller through) and, only if it succeeds, clears the dead
    /// holder's stale `writer_holder` word and wakes any queued waiter --
    /// §8.4's "who clears the word and when" answer: after recovery
    /// succeeds, never before. Returns `error.WriterHolderStillAlive`
    /// without running `recovery` at all if `writerHolderDead` is false,
    /// so a caller can safely call this speculatively (e.g. after a
    /// bounded wait times out) without risking running recovery against
    /// a holder that is only slow, not dead.
    ///
    /// The caller is responsible for retrying its own
    /// `acquireWriter`/`tryAcquireWriter` afterward; this only clears the
    /// obstruction, it does not itself take the lock for the caller
    /// (the two are kept separate so a caller mid-wait on a ticket
    /// doesn't have its own turn skipped by a recovering bystander).
    pub fn recoverDeadWriter(self: *Region, recovery: *const fn () anyerror!void) !void {
        if (!self.writerHolderDead()) return error.WriterHolderStillAlive;
        try recovery();
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
                slot.generation = packHolder(pid, self.own_liveness_slot);
                slot.pinned_sequence.store(0, .monotonic);
                return @intCast(i);
            }
        }
        // Nothing free outright. Per §8.4, a dead reader's slot is as
        // good as free -- the liveness probe only runs here, in the
        // already-rare exhaustion path, not on the fast claim above.
        for (self.readerSlots(), 0..) |*slot, i| {
            const owner = slot.owner.load(.acquire);
            if (owner == 0) continue;
            const gen = slot.generation;
            if (!self.isSlotDead(holderLivenessSlot(gen))) continue;
            if (slot.owner.cmpxchgStrong(owner, pid, .acquire, .monotonic) == null) {
                slot.generation = packHolder(pid, self.own_liveness_slot);
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
    pub fn minimumPinnedSequence(self: *Region) ?u64 {
        var min: ?u64 = null;
        for (self.readerSlots()) |*slot| {
            const owner = slot.owner.load(.acquire);
            if (owner == 0) continue;
            // §8.4: a dead reader's never-cleared pin cannot block
            // reclamation forever. Probing costs a syscall per occupied
            // slot, same as the writer side's `writerHolderDead` -- paid
            // only here, in the writer's own reuse-frontier scan (which
            // already costs more than a warm write's own acquire/release
            // ever does), never on a reader's own pin/unpin path.
            if (self.isSlotDead(holderLivenessSlot(slot.generation))) {
                slot.pinned_sequence.store(0, .monotonic);
                slot.generation = 0;
                slot.owner.store(0, .release);
                const hdr = self.header();
                _ = hdr.reader_slot_released.fetchAdd(1, .release);
                futexWakeShared(&hdr.reader_slot_released.raw);
                continue;
            }
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

// The third POSIX trap `acquireProcessFd`'s own doc comment describes:
// closing *any* fd a process holds on a file drops *all* of that
// process's `fcntl` locks on it. Two `Region` handles opened in this
// same process must share one fd (via `fd_registry`) so that closing one
// of them never silently drops the other's liveness lock -- verified
// here from a genuinely different process's own point of view (a forked
// child), since that's the only way to actually observe whether a
// liveness byte is still held (the self-slot guard in `isSlotDead` means
// this same process can never usefully probe its own slots).
test "shared lock region: closing one of two same-process handles does not drop the other's liveness lock" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-shared-fd.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    const region_path = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{db_path});
    defer a.free(region_path);

    var first = try Region.open(a, io, db_path);
    var second = try Region.open(a, io, db_path);
    try std.testing.expect(first.own_liveness_slot != second.own_liveness_slot);

    // The trap, if it existed: closing `first` must not touch `second`'s
    // own liveness slot, even though (pre-fix) each would have opened
    // its own independent fd on the same file.
    first.close();

    var pipe_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&pipe_fds));
    const second_slot = second.own_liveness_slot;
    const max_readers = second.header().max_readers;

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(pipe_fds[0]);
        const raw_fd = std.posix.openat(std.posix.AT.FDCWD, region_path, .{ .ACCMODE = .RDWR }, 0) catch std.c._exit(1);
        var fl: std.c.Flock = .{
            .start = Region.livenessByteOffset(max_readers, second_slot),
            .len = 1,
            .pid = 0,
            .type = std.c.F.WRLCK,
            .whence = std.c.SEEK.SET,
        };
        // `second`'s own slot should still be held: this trylock must
        // fail. If `first.close()` wrongly dropped it, this succeeds
        // instead, and the report below catches that.
        const still_held: u8 = if (std.c.fcntl(raw_fd, std.c.F.SETLK, &fl) == -1) 1 else 0;
        var report: [1]u8 = .{still_held};
        _ = std.c.write(pipe_fds[1], @ptrCast(&report), 1);
        std.c._exit(0);
    }
    _ = std.posix.system.close(pipe_fds[1]);
    defer _ = std.posix.system.close(pipe_fds[0]);
    var report: [1]u8 = undefined;
    try readBounded(pipe_fds[0], &report, 2000);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pid, 2000));

    try std.testing.expectEqual(@as(u8, 1), report[0]);
    second.close();
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

// §8.4's corruption-safety rule, case 1: a corrupt header with *no live
// peer* (the only region handle that ever touched this file was already
// closed, which released its liveness slot) is safe to rebuild.
test "shared lock region rejects a corrupt header and reinitializes when no live peer holds it" {
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

// §8.4's corruption-safety rule, case 2: a corrupt header with a *live*
// peer must refuse rather than silently wipe state a still-running
// process is relying on. The "live peer" has to be a genuinely separate
// OS process, not a second in-process `Region` handle: `fcntl` byte-
// range locks are scoped per *process*, not per fd, so two handles this
// same process opened would never conflict with each other regardless of
// whether a real peer would -- this is exactly why `probeDead`/
// `isSlotDead` need their own self-slot guard elsewhere in this file,
// and the same reasoning rules out a single-process version of this test.
test "shared lock region refuses a corrupt header instead of rebuilding under a live peer" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-corrupt-live.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    const region_path = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{db_path});
    defer a.free(region_path);

    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        // The genuinely separate "live peer" process: opens the region
        // (claiming its own liveness slot) and parks, never releasing
        // it, for as long as the parent lets it live.
        var peer = Region.open(a, io, db_path) catch std.c._exit(1);
        _ = peer.tryAcquireWriter(@bitCast(std.c.getpid()));
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready_fds[1], @ptrCast(&sig), 1);
        while (true) {
            var no_fds: [0]std.c.pollfd = .{};
            _ = std.c.poll(&no_fds, 0, 20);
        }
    }
    _ = std.posix.system.close(ready_fds[1]);
    defer _ = std.posix.system.close(ready_fds[0]);
    var sig: [1]u8 = undefined;
    try readBounded(ready_fds[0], &sig, 2000);

    // Corrupt the checksum byte directly, exactly as the "no live peer"
    // case does, but this time with a real separate process still
    // holding its liveness slot.
    {
        const f = try Io.Dir.cwd().openFile(io, region_path, .{ .mode = .read_write });
        defer f.close(io);
        var byte: [1]u8 = .{0xFF};
        try f.writePositionalAll(io, &byte, 12);
    }

    try std.testing.expectError(RegionError.RegionCorrupt, Region.open(a, io, db_path));

    _ = std.c.kill(child_pid, std.c.SIG.KILL);
    _ = try reapBounded(child_pid, 2000);
}

// §8.4's dead-writer recovery: a real `kill -9` (not a clean exit) drops
// the kernel's fcntl lock on the dead child's liveness byte exactly as a
// genuine crash would, with no cooperation from the dying process at all.
test "shared lock region: a kill -9'd writer is detected dead and recovered" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-dead-writer.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        // Claims a liveness slot under this child's *own* identity --
        // without this, `writer_holder` would carry the inherited
        // `own_liveness_slot` number whose real lock the *parent* (not
        // this child) still holds, so the parent's own `writerHolderDead`
        // check below would never probe the right thing. See
        // `reclaimLivenessAfterFork`'s own doc comment.
        region.reclaimLivenessAfterFork() catch std.c._exit(1);
        _ = region.tryAcquireWriter(my_pid);
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready_fds[1], @ptrCast(&sig), 1);
        // Simulate a crash mid-commit: no releaseWriter, just park until
        // the parent's kill -9 below ends this process outright.
        while (true) {
            var no_fds: [0]std.c.pollfd = .{};
            _ = std.c.poll(&no_fds, 0, 20);
        }
    }
    _ = std.posix.system.close(ready_fds[1]);
    defer _ = std.posix.system.close(ready_fds[0]);
    var sig: [1]u8 = undefined;
    try readBounded(ready_fds[0], &sig, 2000);

    // The actual crash: a real SIGKILL, not a cooperative exit.
    _ = std.c.kill(child_pid, std.c.SIG.KILL);
    _ = try reapBounded(child_pid, 2000);

    try std.testing.expect(region.writerHolderDead());

    const recovery = struct {
        ran: bool = false,
        fn run() anyerror!void {}
    };
    try region.recoverDeadWriter(&recovery.run);
    try std.testing.expect(!region.writerHolderDead()); // word is clear now, nothing to probe

    // The lock is usable again after recovery.
    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriter(own_pid));
    region.releaseWriter();
}

// §8.4's dead-reader recovery: simpler than the writer case, no recovery
// callback needed -- just clear the slot, which the writer's own
// reuse-frontier scan (`minimumPinnedSequence`) already does inline.
test "shared lock region: a kill -9'd reader's pin is cleared by the frontier scan" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-dead-reader.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        // See the kill-9'd-writer test's own comment: without this, the
        // claimed reader slot's `generation` would carry the inherited
        // `own_liveness_slot` number whose real lock the *parent* still
        // holds, so the parent's own frontier scan would never probe the
        // right thing once this child dies.
        region.reclaimLivenessAfterFork() catch std.c._exit(1);
        const slot = region.claimReaderSlot(my_pid) catch 0xFFFF_FFFF;
        if (slot != 0xFFFF_FFFF) region.pinReader(slot, 77);
        var report: [4]u8 = undefined;
        std.mem.writeInt(u32, &report, slot, .little);
        _ = std.c.write(ready_fds[1], @ptrCast(&report), report.len);
        while (true) {
            var no_fds: [0]std.c.pollfd = .{};
            _ = std.c.poll(&no_fds, 0, 20);
        }
    }
    _ = std.posix.system.close(ready_fds[1]);
    defer _ = std.posix.system.close(ready_fds[0]);
    var report: [4]u8 = undefined;
    try readBounded(ready_fds[0], &report, 2000);
    const child_slot = std.mem.readInt(u32, &report, .little);
    try std.testing.expect(child_slot != 0xFFFF_FFFF);

    _ = std.c.kill(child_pid, std.c.SIG.KILL);
    _ = try reapBounded(child_pid, 2000);

    // The scan probes liveness before ever looking at `pinned_sequence`,
    // so a dead reader's pin never counts toward the minimum even on the
    // very first scan that notices it -- it's cleared and excluded in
    // the same pass, not merely ignored starting next time.
    try std.testing.expectEqual(@as(?u64, null), region.minimumPinnedSequence());
    // And it stays cleared.
    try std.testing.expectEqual(@as(?u64, null), region.minimumPinnedSequence());
    const own_pid: u32 = @bitCast(std.c.getpid());
    const reclaimed = try region.claimReaderSlot(own_pid);
    try std.testing.expectEqual(child_slot, reclaimed);
}
