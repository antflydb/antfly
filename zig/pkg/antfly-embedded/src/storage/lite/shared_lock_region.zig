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
const antfly_platform = @import("antfly_platform");

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
    /// Bumped (and the matching futex woken) unconditionally on every
    /// `releaseWriter`. `acquireWriter`'s own doc comment explains why
    /// its phase 2 needs to wait on a word `releaseWriter` itself
    /// mutates (not `head`, which it never touches). `writer_holder`
    /// itself cannot serve as that word: a futex word must be exactly 32
    /// bits, and `writer_holder` is 64 (`pid` packed with a 32-bit
    /// liveness slot). A plain monotonic counter sidesteps the size
    /// mismatch entirely and is simpler to reason about than any 32-bit
    /// slice of `writer_holder` would be -- the same shape as
    /// `reader_slot_released`, already proven correct for exactly this
    /// "wait for the next release, not a specific value" pattern.
    writer_release_epoch: std.atomic.Value(u32),
    /// Sized so the named fields above sum to exactly `header_bytes` (64)
    /// with no implicit trailing struct padding: `std.mem.asBytes`
    /// includes any such padding, and Zig's struct-literal initialization
    /// does not guarantee it is stable (let alone zero) across separate
    /// reads of it -- the root cause of an earlier version of this header
    /// rejecting its own freshly-written, uncorrupted bytes as corrupt on
    /// every reopen. `== header_bytes` below (not `<=`) keeps that
    /// invariant enforced at compile time for any future field addition.
    _reserved: [4]u8 = @splat(0),
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
const fcntlTryLock = antfly_platform.sync.fcntlTryLock;
const fcntlUnlock = antfly_platform.sync.fcntlUnlock;

/// Bounded, cancelable alternative to a raw blocking `fcntl` lock (never
/// `F_SETLKW`, which has no deadline or cancellation of its own -- see
/// `antfly_platform.sync.fcntlLockBounded`'s own doc comment). Used only
/// for the one-time create/validate section's own exclusive lock (see
/// `setup_lock_offset`) -- everywhere else in this module takes the
/// nonblocking `fcntlTryLock` instead. Generous (30s) but still bounded:
/// genuine multi-process contention on this one-time section should
/// never come close, and the alternative (an unbounded wait) is exactly
/// what this whole design exists to avoid everywhere else.
fn fcntlLockBlocking(io: Io, fd: std.posix.fd_t, offset: i64, lock_type: i16) !void {
    const deadline = Io.Clock.now(.awake, io).addDuration(.fromSeconds(30));
    return antfly_platform.sync.fcntlLockBounded(io, fd, offset, lock_type, deadline);
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

    /// Byte offset of ticket ring slot `ticket % max_readers`'s owner
    /// word (packed `pid`/liveness-slot, `packHolder`'s own encoding),
    /// immediately after the reader slot array -- 8-byte aligned for any
    /// `max_readers` value, since the reader slot array's own size is
    /// always a multiple of 8. See `ticketOwnerPids`'s own doc comment
    /// for what this is for.
    fn ticketOwnerOffset(max_readers: u32) usize {
        return header_bytes + @as(usize, max_readers) * reader_slot_bytes;
    }

    /// Byte offset of liveness slot `slot_index`'s dedicated byte,
    /// immediately after the ticket owner ring (§8.1's "M*1 process
    /// liveness bytes, one per slot"). Content is never read or written;
    /// the byte exists purely as an `fcntl` lock target.
    fn livenessByteOffset(max_readers: u32, slot_index: u32) i64 {
        return @as(i64, @intCast(ticketOwnerOffset(max_readers))) + @as(i64, max_readers) * 8 + @as(i64, slot_index);
    }

    /// Byte offset of ticket `ticket`'s abandonment flag, in a fixed ring
    /// of `max_readers` bytes immediately after the liveness bytes.
    /// `acquireWriterFairBounded`'s own doc comment explains what this
    /// is for and why a ring, keyed by `ticket % max_readers`, is safe:
    /// in short, a bounded (deadline-capable) FIFO acquire needs a way
    /// for a ticket holder that gives up to signal that without
    /// permanently wedging every ticket after it, and this codebase's
    /// current callers never have more than `max_readers` of this
    /// region's own tickets genuinely outstanding at once (every ticket-
    /// taker already needed a liveness slot to open this region at all,
    /// so `max_readers` already bounds concurrent callers more broadly;
    /// `acquireWriter`/`acquireWriterFairBounded` also actively enforce
    /// this bound -- see their own "ring full" backoff).
    fn ticketAbandonedOffset(max_readers: u32, ticket: u32) i64 {
        return @as(i64, livenessByteOffset(max_readers, 0)) + @as(i64, max_readers) + @as(i64, ticket % max_readers);
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
        try fcntlLockBlocking(io, raw_fd, setup_lock_offset, std.c.F.WRLCK);
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
            const region_bytes = header_bytes + @as(u64, max_readers) * (reader_slot_bytes + 10);
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
                .writer_release_epoch = .init(0),
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
        const implied_bytes = header_bytes + @as(u64, claimed_max_readers) * (reader_slot_bytes + 10);
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
    /// Two phases, deliberately waiting on a *different* word in each,
    /// because a futex's atomic "check the word, block only if it still
    /// equals `expect`" guarantee only protects against a lost wakeup
    /// when the word being checked is the *same* word the other side
    /// actually mutates before waking. The two things this function
    /// waits for -- "it is my turn" and "the current holder has
    /// released" -- are backed by two different words, so one futex
    /// word cannot stand in for both without a race:
    ///
    /// **Phase 1** waits on `head` until `head == my_ticket`. `head`
    /// itself transitions exactly when this should re-check (advanced
    /// only by the ticket that just won, immediately below, in the same
    /// breath as its own wake), so the atomic guarantee fully applies:
    /// no interleaving of another waiter's check-then-wait with this
    /// phase's own `head` mutation can land `head` motionless while a
    /// waiter is still parked expecting it to move.
    ///
    /// **Phase 2** starts once `head == my_ticket`, but the writer word
    /// may still be held (the previous winner's own commit may still be
    /// running) -- it is *not* released by `head` moving, only by
    /// `releaseWriter`'s own store to `writer_holder`. An earlier version
    /// kept waiting on `head` through this phase too, on the reasoning
    /// that `releaseWriter` always wakes `head`'s waiters when anyone is
    /// queued. That reasoning missed that `head` itself never changes
    /// during a release, so the futex's atomic guarantee does not cover
    /// this phase at all: if a failed CAS here and this phase's own
    /// `futexWaitShared` call happened to straddle the releaser's own
    /// `store` + wake, the wake could fire while nothing was yet
    /// listening, and the subsequent wait's own atomic check would see
    /// `head` *unchanged* and block anyway -- a real, reproducible lost
    /// wakeup under a four-process contention test, not merely
    /// theoretical. Phase 2 now waits on `writer_holder` itself instead,
    /// the word `releaseWriter` actually mutates, so the same atomic
    /// guarantee phase 1 already relies on now covers this phase too.
    pub fn acquireWriter(self: *Region, pid: u32) void {
        const hdr = self.header();
        // FIFO: only take the fast, ticketless path with nobody already
        // queued -- otherwise a new arrival could win the CAS ahead of an
        // earlier ticket already parked and waiting its turn.
        if (hdr.writer_ticket_head.load(.monotonic) == @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))) and self.tryAcquireWriter(pid)) return;
        // `rawFutexWaitShared`, not an uncancelable wait with no Io: see
        // this function's own module-level peers for why it takes none,
        // and `tryHelpDeadTicketAt`'s own doc comment for why phase 1
        // needs a *bounded* step at all now, not a true indefinite park.
        var backoff_ms: u32 = ring_full_backoff_initial_ms;
        const my_ticket: u32 = while (true) {
            if (self.ringHasRoom()) break @truncate(hdr.writer_ticket_tail.fetchAdd(1, .monotonic));
            antfly_platform.sync.rawFutexWaitShared(&hdr.writer_ticket_head.raw, hdr.writer_ticket_head.load(.monotonic), backoff_ms);
            backoff_ms = @min(backoff_ms * 2, ring_full_backoff_max_ms);
        };
        self.ticketOwnerPids()[my_ticket % hdr.max_readers].store(packHolder(pid, self.own_liveness_slot), .release);
        while (true) {
            const head = hdr.writer_ticket_head.load(.acquire);
            if (head == my_ticket) break;
            antfly_platform.sync.rawFutexWaitShared(&hdr.writer_ticket_head.raw, head, dead_ticket_check_step_ms);
            const head2 = hdr.writer_ticket_head.load(.acquire);
            if (head2 == my_ticket) break;
            if (head2 == head) self.tryHelpDeadTicketAt(head2);
        }
        while (true) {
            if (self.tryAcquireWriter(pid)) {
                // Won. Advance head (and past any already-abandoned or
                // dead-owned tickets right after it -- see
                // `advanceHeadPastAbandoned`) so ticket `my_ticket + 1`
                // (if any) leaves phase 1 and enters its own phase 2, and
                // wake whoever is parked for that -- including a
                // spurious wake of a later ticket still waiting on the
                // old head value, which simply re-checks and re-parks.
                self.advanceHeadPastAbandoned();
                return;
            }
            // Still held by the previous winner. Wait on
            // `writer_release_epoch`, not `head` and not `writer_holder`
            // itself (see this function's own doc comment, and
            // `writer_release_epoch`'s own, for why).
            const epoch = hdr.writer_release_epoch.load(.acquire);
            antfly_platform.sync.rawFutexWaitShared(&hdr.writer_release_epoch.raw, epoch, null);
        }
    }

    /// §8.2 "try" mode (the three-mode region acquisition API
    /// `fairWriterLockWithIo`/`tryFairWriterLockWithIo`/
    /// `waitPathLockWithIo` are migrating onto): nonblocking, and --
    /// unlike `tryAcquireWriter`, this method's own private building
    /// block -- never jumps the FIFO: fails immediately if anyone is
    /// already queued, rather than racing them for the CAS. Takes no
    /// ticket on failure, so there is nothing to abandon; a failed call
    /// leaves no trace.
    pub fn tryAcquireWriterFair(self: *Region, pid: u32) bool {
        const hdr = self.header();
        if (hdr.writer_ticket_head.load(.monotonic) != @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic)))) return false;
        return self.tryAcquireWriter(pid);
    }

    /// `true` when there is a ticket queued (regardless of whether it
    /// belongs to this process or a foreign one) -- §8.2's "fair
    /// blocking (FIFO ticket)" mode's own query for the "is anyone
    /// waiting at all" question `PathWriterLock.othersWaiting` needs. An
    /// atomic load, same as `acquireWriter`'s own ticketless fast-path
    /// check.
    pub fn othersWaiting(self: *const Region) bool {
        const hdr = self.header();
        return hdr.writer_ticket_head.load(.monotonic) != @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic)));
    }

    /// Like `othersWaiting`, but true only if at least one *currently
    /// live* queued ticket belongs to a different process than
    /// `own_pid` -- `PathWriterLock.othersWaitingAcrossProcesses`'s own
    /// query, for a caller (`WriterLane`'s own `checkin`) that must not
    /// treat its own same-process callers (reaching the queue through a
    /// different path) as a hostile waiter to release for, the same
    /// distinction `writer_queue.zig`'s own per-ticket pid byte made.
    /// Scans every outstanding ticket (bounded by `max_readers`, same as
    /// every other scan in this file), skipping ones already known
    /// abandoned or dead -- a ticket that will never actually show up to
    /// claim the lease should not count as "someone is waiting" either.
    pub fn othersWaitingAcrossProcesses(self: *const Region, own_pid: u32) bool {
        const hdr = self.header();
        const head = hdr.writer_ticket_head.load(.monotonic);
        const tail: u32 = @truncate(hdr.writer_ticket_tail.load(.monotonic));
        if (head == tail) return false;
        const owners = self.ticketOwnerPids();
        const flags = self.ticketAbandonedFlags();
        var t = head;
        while (t != tail) : (t +%= 1) {
            const idx = t % hdr.max_readers;
            if (flags[idx].load(.acquire) != 0) continue;
            const owner = owners[idx].load(.acquire);
            if (holderPid(owner) == own_pid) continue;
            if (self.isSlotDead(holderLivenessSlot(owner))) continue;
            return true;
        }
        return false;
    }

    /// §8.2 "fair blocking (FIFO ticket)" mode, bounded by `deadline`
    /// (construct with `Io.Clock.now(.awake, io).addDuration(...)`):
    /// `acquireWriter`'s own two-phase protocol, but able to give up
    /// and return `error.Timeout` instead of blocking forever -- what
    /// `fairWriterLockWithIo`'s own `deadline_ns` parameter needs.
    ///
    /// Giving up partway through is not simply "stop waiting": once a
    /// ticket is taken, `head` only ever advances in whoever is
    /// currently responsible for ticket number `head` (the waiter who
    /// wins it, or -- new here -- the waiter who gives up on it). If a
    /// timed-out waiter just walked away, nothing would ever move `head`
    /// past its abandoned ticket, permanently wedging every ticket taken
    /// after it. The two phases handle this differently, matching which
    /// of them currently "owns" `head`:
    ///
    /// - **Phase 1** (`head != my_ticket` yet) does not own `head` --
    ///   some earlier ticket does -- so it cannot safely move it. It
    ///   marks its own ring slot in `ticketAbandonedOffset` instead:
    ///   whichever later winner's `advanceHeadPastAbandoned` eventually
    ///   reaches this ticket number sees the flag and continues past it
    ///   on this ticket's behalf, exactly as if it had also just won and
    ///   immediately stepped aside.
    /// - **Phase 2** (`head == my_ticket`) already owns `head` -- it is
    ///   this ticket's turn, the writer word is just still held by the
    ///   previous winner -- so it advances `head` itself via the same
    ///   `advanceHeadPastAbandoned` a real win would use, a "pseudo-win"
    ///   that passes the turn along without ever touching `writer_holder`.
    ///
    /// Needs `io` (unlike `acquireWriter`) specifically to check it and
    /// the deadline; not safe to call from a freshly forked child, for
    /// the same reason `acquireWriter`'s own doc comment gives.
    pub fn acquireWriterFairBounded(self: *Region, io: Io, pid: u32, deadline: Io.Timestamp) error{ Timeout, Canceled }!void {
        const hdr = self.header();
        if (hdr.writer_ticket_head.load(.monotonic) == @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))) and self.tryAcquireWriter(pid)) return;
        const my_ticket: u32 = while (true) {
            if (self.ringHasRoom()) break @truncate(hdr.writer_ticket_tail.fetchAdd(1, .monotonic));
            if (Io.Clock.now(.awake, io).compare(.gte, deadline)) return error.Timeout;
            antfly_platform.sync.futexWaitSharedBounded(io, &hdr.writer_ticket_head.raw, hdr.writer_ticket_head.load(.monotonic), deadline) catch return error.Canceled;
        };
        self.ticketOwnerPids()[my_ticket % hdr.max_readers].store(packHolder(pid, self.own_liveness_slot), .release);
        while (true) {
            const head = hdr.writer_ticket_head.load(.acquire);
            if (head == my_ticket) break;
            if (Io.Clock.now(.awake, io).compare(.gte, deadline)) {
                self.ticketAbandonedFlags()[my_ticket % hdr.max_readers].store(1, .release);
                return error.Timeout;
            }
            antfly_platform.sync.futexWaitSharedBounded(io, &hdr.writer_ticket_head.raw, head, deadline) catch return error.Canceled;
            const head2 = hdr.writer_ticket_head.load(.acquire);
            if (head2 == head) self.tryHelpDeadTicketAt(head2);
        }
        while (true) {
            if (self.tryAcquireWriter(pid)) {
                self.advanceHeadPastAbandoned();
                return;
            }
            if (Io.Clock.now(.awake, io).compare(.gte, deadline)) {
                // Our own turn, but we're giving it up: step aside
                // ourselves, exactly like a real win's own advance, just
                // without ever touching `writer_holder`.
                self.advanceHeadPastAbandoned();
                return error.Timeout;
            }
            const epoch = hdr.writer_release_epoch.load(.acquire);
            antfly_platform.sync.futexWaitSharedBounded(io, &hdr.writer_release_epoch.raw, epoch, deadline) catch return error.Canceled;
        }
    }

    /// Bound on how many of this region's own tickets may be
    /// simultaneously outstanding (taken but not yet resolved), enforced
    /// by `ringHasRoom` before `acquireWriter`/`acquireWriterFairBounded`
    /// ever call `fetchAdd` on `writer_ticket_tail`: past this point,
    /// `ticket % max_readers` would collide with an older, still-
    /// unresolved ticket's own ring slots (owner, abandoned-flag). Tied
    /// to `max_readers` rather than a separate constant because every
    /// ticket-taker already needed a liveness slot to open this region
    /// at all (§8.3), so this bound was already implied; enforcing it
    /// explicitly, rather than only documenting it, is what turns "an
    /// assumption this step's callers happen to satisfy" into something
    /// that fails safe (backs off; never silently corrupts a ring slot)
    /// if a future caller ever doesn't.
    fn ringHasRoom(self: *const Region) bool {
        const hdr = self.header();
        const outstanding = hdr.writer_ticket_tail.load(.monotonic) -% hdr.writer_ticket_head.load(.monotonic);
        return outstanding < hdr.max_readers;
    }

    const ring_full_backoff_initial_ms: u32 = 5;
    const ring_full_backoff_max_ms: u32 = 50;
    /// How often a phase-1 waiter re-checks whether the ticket currently
    /// at `head` is abandoned or dead-owned, when nothing has woken it
    /// in the meantime. See `tryHelpDeadTicketAt`'s own doc comment.
    const dead_ticket_check_step_ms: u32 = 50;

    /// `true` (and clears the abandonment flag, if that is why) when
    /// ticket `ticket` is already resolved one way or another -- either
    /// explicitly abandoned, or its recorded owner is provably dead --
    /// without this call itself needing to own or win it.
    fn ticketResolvedWithoutOwning(self: *Region, ticket: u32) bool {
        const hdr = self.header();
        const idx = ticket % hdr.max_readers;
        const flags = self.ticketAbandonedFlags();
        if (flags[idx].load(.acquire) != 0) {
            flags[idx].store(0, .monotonic);
            return true;
        }
        const owner = self.ticketOwnerPids()[idx].load(.acquire);
        return owner != 0 and self.isSlotDead(holderLivenessSlot(owner));
    }

    /// Called by a waiter stuck making no progress on `head` (any phase,
    /// any ticket -- it only ever looks at `head`'s *current* value, not
    /// its own ticket) to check whether `head`'s current occupant is a
    /// ticket whose own recorded owner has died -- `kill -9` of a queued
    /// waiter, in either of *its* phases, same as the abandonment case
    /// `acquireWriterFairBounded`'s own doc comment covers, except this
    /// one never got the chance to mark its own flag. Nobody else is
    /// normally responsible for moving `head` past a ticket once it's
    /// stuck there, so without this, a dead queued waiter would wedge
    /// every ticket after it exactly as a leaked abandonment would.
    ///
    /// A `cmpxchg`, not an unconditional advance like
    /// `advanceHeadPastAbandoned`'s own first step: more than one other
    /// waiter can notice the same dead occupant on the same bounded
    /// step, and only the one that wins should continue the skip-chain.
    /// The others simply loop back to their own wait, see the new `head`
    /// value, and proceed normally.
    fn tryHelpDeadTicketAt(self: *Region, stuck_head: u32) void {
        if (!self.ticketResolvedWithoutOwning(stuck_head)) return;
        const hdr = self.header();
        if (hdr.writer_ticket_head.cmpxchgStrong(stuck_head, stuck_head +% 1, .release, .monotonic) != null) return;
        self.continueSkippingFrom(stuck_head +% 1);
    }

    /// After `head` has just become `current_head` -- because its own
    /// rightful ticket just won or stepped aside, or because
    /// `tryHelpDeadTicketAt` just won the right to advance past a dead
    /// occupant -- keeps advancing past any immediately following
    /// tickets that are themselves already resolved, then wakes once.
    fn continueSkippingFrom(self: *Region, current_head: u32) void {
        const hdr = self.header();
        var head = current_head;
        while (self.ticketResolvedWithoutOwning(head)) {
            head = hdr.writer_ticket_head.fetchAdd(1, .release) +% 1;
        }
        futexWakeShared(&hdr.writer_ticket_head.raw);
    }

    /// After winning ticket `my_ticket`'s CAS, or voluntarily stepping
    /// aside for it (`acquireWriterFairBounded`'s own phase-2 timeout),
    /// advances `head` past it and delegates to `continueSkippingFrom`
    /// for anything immediately following that is also already
    /// resolved.
    fn advanceHeadPastAbandoned(self: *Region) void {
        const hdr = self.header();
        const new_head = hdr.writer_ticket_head.fetchAdd(1, .release) +% 1;
        self.continueSkippingFrom(new_head);
    }

    /// Releases a writer hold taken by `acquireWriter`/`tryAcquireWriter`.
    /// Never touches `head` itself (`acquireWriter`'s own doc comment
    /// explains why `head` only ever advances in the waiter that wins
    /// its own turn) -- bumps and wakes `writer_release_epoch` instead,
    /// the word `acquireWriter`'s phase 2 actually waits on, and only
    /// when someone is actually queued (`head != tail`), so the fully
    /// uncontended release stays a single store with no syscall.
    pub fn releaseWriter(self: *Region) void {
        const hdr = self.header();
        hdr.writer_holder.store(0, .release);
        if (hdr.writer_ticket_head.load(.monotonic) != @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic)))) {
            _ = hdr.writer_release_epoch.fetchAdd(1, .release);
            futexWakeShared(&hdr.writer_release_epoch.raw);
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

    /// The abandonment-flag ring `ticketAbandonedOffset` indexes into,
    /// immediately after the liveness bytes. Plain shared memory (no
    /// `fcntl` lock involved, unlike the liveness bytes right before it):
    /// `0` means "not abandoned", `1` means "the holder of this ring
    /// slot's ticket gave up without ever winning; whoever reaches this
    /// ticket number should treat it as already resolved and continue".
    fn ticketAbandonedFlags(self: *const Region) []std.atomic.Value(u8) {
        const max_readers = self.header().max_readers;
        const base: [*]std.atomic.Value(u8) = @ptrCast(@alignCast(self.mapping.ptr + @as(usize, @intCast(ticketAbandonedOffset(max_readers, 0)))));
        return base[0..max_readers];
    }

    /// A ring of `max_readers` packed owner words (`packHolder`'s own
    /// `(pid, liveness_slot)` encoding), one per `ticket % max_readers`,
    /// written when a ticket is taken and read for two purposes:
    ///
    /// 1. `othersWaitingAcrossProcesses` -- telling a genuinely foreign
    ///    waiter apart from this same process's own other caller (its
    ///    own background maintenance worker, say) reaching the queue
    ///    through a different path, the same distinction
    ///    `writer_queue.zig`'s own per-ticket pid byte made.
    /// 2. Dead-ticket-holder detection -- a waiter stuck making no
    ///    progress on `head` probes this ring slot's liveness; see
    ///    `tryHelpDeadTicketAt`'s own doc comment for the full case this
    ///    closes (a `kill -9` of a queued waiter, in either phase, that
    ///    never gets the chance to mark its own abandonment).
    fn ticketOwnerPids(self: *const Region) []std.atomic.Value(u64) {
        const max_readers = self.header().max_readers;
        const base: [*]std.atomic.Value(u64) = @ptrCast(@alignCast(self.mapping.ptr + ticketOwnerOffset(max_readers)));
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
        const deadline = Io.Clock.now(.awake, self.io).addDuration(.fromMilliseconds(deadline_ms));
        while (true) {
            if (self.claimReaderSlot(pid)) |slot| return slot else |_| {}
            if (Io.Clock.now(.awake, self.io).compare(.gte, deadline)) return ReaderError.ReadersExhausted;
            const before = hdr.reader_slot_released.load(.acquire);
            // Re-check after reading `before`, not before: a release that
            // landed between the failed claim above and this load would
            // otherwise be missed by the wait below (the same
            // lost-wakeup shape `acquireWriter`'s own doc comment warns
            // about) -- `futexWaitSharedBounded` guards against it by
            // checking the word against `before` itself before ever
            // deciding to block.
            if (self.claimReaderSlot(pid)) |slot| return slot else |_| {}
            antfly_platform.sync.futexWaitSharedBounded(self.io, &hdr.reader_slot_released.raw, before, deadline) catch {};
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

/// Shared (cross-process) futex wait/wake, now hosted in
/// `antfly_platform.sync` (not inline here) so the rest of the §8.9
/// sweep -- the fallback path (§8.6), and anything else that eventually
/// needs a shared futex -- has one place to reuse instead of its own
/// copy of this platform switch. `futexWaitShared` is this file's own
/// name for `antfly_platform.sync.rawFutexWaitShared` with no timeout:
/// the writer/ticket wait never needs one (a correct implementation
/// hands off in about one commit, not on a deadline) or an `Io` to
/// consult (the two-process contention test's own doc comment explains
/// why a forked child can't safely use one here).
fn futexWaitShared(ptr: *const u32, expect: u32) void {
    antfly_platform.sync.rawFutexWaitShared(ptr, expect, null);
}

const futexWakeShared = antfly_platform.sync.futexWakeShared;

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

/// A forked child's "now simulate a crash: park until the parent's
/// `kill -9` ends this process outright" idiom, isolated into one
/// helper (per the module's own instruction to isolate the one fork-
/// based pattern this file's cross-process tests genuinely need --
/// `fork`, not `std.process.Child`, because the point in every one of
/// these tests is a child that *inherits* the parent's existing mmap,
/// which spawning a new program via `exec` would not). `poll` with no
/// fds and a short timeout is a portable sleep, avoiding a separate
/// `nanosleep` declaration for this one backoff; never returns, since
/// the only way out is the parent's signal.
fn parkForever() noreturn {
    while (true) {
        var no_fds: [0]std.c.pollfd = .{};
        _ = std.c.poll(&no_fds, 0, 20);
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
        parkForever();
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
        parkForever();
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
        parkForever();
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

test "shared lock region: tryAcquireWriterFair succeeds uncontended and refuses when anyone is queued" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-try-fair.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const pid: u32 = 4242;
    try std.testing.expect(region.tryAcquireWriterFair(pid));
    // Already held: a second try correctly fails (not "nobody queued yet"
    // -- the word itself is busy).
    try std.testing.expect(!region.tryAcquireWriterFair(pid + 1));
    region.releaseWriter();
    try std.testing.expect(region.tryAcquireWriterFair(pid + 1));
    region.releaseWriter();
}

test "shared lock region: acquireWriterFairBounded succeeds once free and times out when it can't" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-bounded-fair.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const pid: u32 = 4242;
    // Uncontended: succeeds well within a generous deadline.
    try region.acquireWriterFairBounded(io, pid, std.Io.Clock.now(.awake, io).addDuration(.fromSeconds(5)));
    // Still held by `pid`'s own hold above (never released): a short
    // deadline against a genuinely busy word must time out, bounded,
    // not hang.
    const short_deadline = std.Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(100));
    try std.testing.expectError(error.Timeout, region.acquireWriterFairBounded(io, pid + 1, short_deadline));
    region.releaseWriter();
    // Free again: a fresh bounded acquire succeeds, proving the timed-out
    // attempt above didn't leave the region in a bad state.
    try region.acquireWriterFairBounded(io, pid + 1, std.Io.Clock.now(.awake, io).addDuration(.fromSeconds(5)));
    region.releaseWriter();
}

// Ports writer_queue.zig's own "rule 6: cancelling a queued waiter does
// not let a later one pass an earlier live one" to the region's ticket
// ring: an earlier, still-active ticket must not be skipped or harmed by
// a later ticket's own timeout, and tickets taken after the abandoned one
// must still make progress once it's skipped -- not wedge forever, which
// is exactly what would happen if whoever eventually reaches the
// abandoned ticket's position in `head` didn't know to continue past it.
test "shared lock region: an abandoned ticket does not wedge an earlier or a later one" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-abandon.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriterFair(own_pid)); // ticketless hold

    var ready1_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready1_fds));
    var done1_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&done1_fds));

    // Child 1: takes ticket 0 (the first contender after the ticketless
    // hold above), waits patiently (unbounded) -- the "earlier live
    // ticket" rule 6 says must not be skipped.
    const child1_pid = std.c.fork();
    if (child1_pid == 0) {
        _ = std.posix.system.close(ready1_fds[0]);
        _ = std.posix.system.close(done1_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready1_fds[1], @ptrCast(&sig), 1);
        region.acquireWriter(my_pid);
        var report: [1]u8 = .{1};
        _ = std.c.write(done1_fds[1], @ptrCast(&report), 1);
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready1_fds[1]);
    defer _ = std.posix.system.close(ready1_fds[0]);
    _ = std.posix.system.close(done1_fds[1]);
    defer _ = std.posix.system.close(done1_fds[0]);
    var sig1: [1]u8 = undefined;
    try readBounded(ready1_fds[0], &sig1, 2000);

    var ready2_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready2_fds));
    var timedout2_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&timedout2_fds));

    // Child 2: takes ticket 1 (forked only after child 1's own ticket is
    // taken, so fork order is ticket order, same assumption the N-process
    // test above makes), with a short deadline it cannot possibly make --
    // this process still holds the writer word and won't release for a
    // while yet. This is the ticket that abandons.
    const child2_pid = std.c.fork();
    if (child2_pid == 0) {
        _ = std.posix.system.close(ready2_fds[0]);
        _ = std.posix.system.close(timedout2_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready2_fds[1], @ptrCast(&sig), 1);
        const deadline = std.Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(100));
        const result = region.acquireWriterFairBounded(io, my_pid, deadline);
        var report: [1]u8 = .{if (result) |_| 0 else |_| 1};
        _ = std.c.write(timedout2_fds[1], @ptrCast(&report), 1);
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready2_fds[1]);
    defer _ = std.posix.system.close(ready2_fds[0]);
    _ = std.posix.system.close(timedout2_fds[1]);
    defer _ = std.posix.system.close(timedout2_fds[0]);
    var sig2: [1]u8 = undefined;
    try readBounded(ready2_fds[0], &sig2, 2000);

    // Child 2 must have genuinely timed out (not raced into winning)
    // before this process releases -- generous margin past its own
    // 100ms deadline.
    var timedout_report: [1]u8 = undefined;
    try readBounded(timedout2_fds[0], &timedout_report, 2000);
    try std.testing.expectEqual(@as(u8, 1), timedout_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child2_pid, 2000));

    // Now release: child 1 (ticket 0, still patiently waiting) must win,
    // not be harmed by ticket 1's abandonment.
    region.releaseWriter();
    var done1_report: [1]u8 = undefined;
    try readBounded(done1_fds[0], &done1_report, 2000);
    try std.testing.expectEqual(@as(u8, 1), done1_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child1_pid, 2000));

    // Child 1's own release just advanced head past ticket 1's abandoned
    // flag. The region must be left in a sane, usable state -- not
    // wedged -- for whoever comes next.
    try region.acquireWriterFairBounded(io, own_pid, std.Io.Clock.now(.awake, io).addDuration(.fromSeconds(5)));
    region.releaseWriter();
}

// A kill -9 on an abandoning waiter (the test above) still gets to run
// its own cleanup code -- it chooses to mark its own abandonment flag
// before giving up. A kill -9 on a *queued* waiter gets no such chance:
// the signal lands while it's blocked inside `acquireWriter` with no
// opportunity to run anything at all. `writer_queue.zig`'s own "dead
// waiters" rule existed for exactly this case; this ports it to the
// region's ticket ring. The fix this test exercises is
// `tryHelpDeadTicketAt`: a later waiter, stuck on `head` with no
// progress for a full bounded step, probes `head`'s current occupant's
// liveness slot itself and performs the skip -- no winner's own cleanup
// pass is involved at all, since the dead ticket's holder never reaches
// (and never will reach) the code that would normally do that.
test "shared lock region: a dead queued waiter does not wedge a later one" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-dead-queued.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriterFair(own_pid)); // ticketless hold; nobody queued yet.

    // Child 0: takes ticket 0 -- the very first ticket, so it is
    // immediately "at head" (its own phase 2), legitimately parked
    // waiting for this process's own ticketless hold to free up. It is
    // killed before it ever resolves -- no win, no voluntary abandon --
    // so nothing *it* runs ever calls `continueSkippingFrom` on its own
    // behalf. The only way forward is a later waiter's own bounded-step
    // poll on `head`.
    var ready0_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready0_fds));
    const child0_pid = std.c.fork();
    if (child0_pid == 0) {
        _ = std.posix.system.close(ready0_fds[0]);
        // See the kill-9'd-writer test's own comment: without this,
        // ticket 0's recorded owner would carry the inherited
        // `own_liveness_slot` number whose real lock the *parent* (not
        // this child) still holds, so no later waiter's dead-check
        // would ever see it as dead.
        region.reclaimLivenessAfterFork() catch std.c._exit(1);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready0_fds[1], @ptrCast(&sig), 1);
        region.acquireWriter(my_pid); // never returns: killed first.
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready0_fds[1]);
    defer _ = std.posix.system.close(ready0_fds[0]);
    var sig0: [1]u8 = undefined;
    try readBounded(ready0_fds[0], &sig0, 2000);

    // Wait until child 0 has genuinely taken ticket 0 (not merely
    // signaled that it is about to try) before forking child 1 or
    // killing it -- observed directly off the region's own shared
    // header, visible to this process through the same mapping.
    {
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_tail.load(.monotonic) == 0) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 2000) return error.TestTimeout;
        }
    }

    // Child 1: forked *while child 0 is still alive*, so that when it
    // claims its own liveness slot below, child 0's own slot is still
    // genuinely held and gets skipped -- it ends up with a distinct
    // slot number of its own. Forking only after killing child 0 would
    // let child 1's scan reclaim child 0's now-freed slot number for
    // itself, aliasing the two and making `isSlotDead` see child 1's
    // own, very much alive, hold on that same slot index instead of
    // ever noticing child 0 is gone -- a `fcntl`-byte-range-reuse trap
    // distinct from (but adjacent to) the ones this file's own module
    // doc comment already lists. It takes ticket 1 -- genuinely behind
    // (not yet at) head when it starts waiting, real "phase 1" the same
    // way the coordinator's own wording means it.
    var ready1_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready1_fds));
    var done1_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&done1_fds));
    const child1_pid = std.c.fork();
    if (child1_pid == 0) {
        _ = std.posix.system.close(ready1_fds[0]);
        _ = std.posix.system.close(done1_fds[0]);
        region.reclaimLivenessAfterFork() catch std.c._exit(1);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready1_fds[1], @ptrCast(&sig), 1);
        region.acquireWriter(my_pid);
        var report: [1]u8 = .{1};
        _ = std.c.write(done1_fds[1], @ptrCast(&report), 1);
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready1_fds[1]);
    defer _ = std.posix.system.close(ready1_fds[0]);
    _ = std.posix.system.close(done1_fds[1]);
    defer _ = std.posix.system.close(done1_fds[0]);
    var sig1: [1]u8 = undefined;
    try readBounded(ready1_fds[0], &sig1, 2000);

    // Confirm ticket 1 is genuinely taken before killing child 0 or
    // releasing, so the eventual win below cannot be a race that
    // happened to work out some other way.
    {
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_tail.load(.monotonic) < 2) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 2000) return error.TestTimeout;
        }
    }

    // The actual crash: a real SIGKILL, not a cooperative exit, and only
    // now -- after child 1 already holds its own distinct liveness slot
    // -- so child 0's freed slot cannot be reclaimed out from under this
    // test's own assumptions.
    _ = std.c.kill(child0_pid, std.c.SIG.KILL);
    _ = try reapBounded(child0_pid, 2000);

    // Give child 1 a real chance to have already run its own dead-ticket
    // poll and skip past ticket 0 *before* this process ever releases --
    // proving the skip itself doesn't depend on this release at all,
    // only the final win does. Not load-bearing for correctness (the
    // release below would eventually unstick things either way via the
    // same poll), just makes the scenario this test means to exercise
    // unambiguous.
    try io.sleep(.fromMilliseconds(250), .awake);
    try std.testing.expectEqual(@as(u32, 1), region.header().writer_ticket_head.load(.monotonic));

    region.releaseWriter();
    var done1_report: [1]u8 = undefined;
    try readBounded(done1_fds[0], &done1_report, 2000);
    try std.testing.expectEqual(@as(u8, 1), done1_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child1_pid, 2000));

    // Left in a sane, usable state.
    try std.testing.expect(region.tryAcquireWriterFair(own_pid));
    region.releaseWriter();
}

// The owner-pid and abandoned-ticket rings are both sized by
// `max_readers`, keyed by `ticket % max_readers` -- safe only as long as
// no more than `max_readers` of this region's own tickets are ever
// genuinely outstanding at once (otherwise two live tickets would alias
// the same ring slot). `ringHasRoom` enforces that bound before either
// `acquireWriter` or `acquireWriterFairBounded` ever takes a ticket at
// all, rather than merely documenting it. This test fills a
// deliberately tiny ring (`max_readers = 2`) to its exact limit and
// confirms a bounded third arrival is refused (times out without ever
// taking a ticket, rather than aliasing an existing one), and that
// normal service resumes once room frees up.
test "shared lock region: the ticket ring refuses to grow past max_readers outstanding" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-ring-full.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.openSized(a, io, db_path, 2);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriterFair(own_pid)); // ticketless hold.

    // `max_readers = 2` sizes the ticket ring tightly enough that two
    // outstanding tickets alone already fill it -- deliberately, so
    // this test can reach "exactly full" with only two child processes.
    // Unlike the dead-queued-waiter test above, this one is purely
    // about ring-capacity bookkeeping (`ringHasRoom`), not liveness or
    // death detection, so neither child calls `reclaimLivenessAfterFork`
    // -- they ride along on this process's own already-claimed slot
    // (inherited, but never reclaimed independently) rather than each
    // needing a distinct one of their own, which `max_readers = 2`
    // alone (this process's own open already consumes one of the two)
    // could not otherwise provide to two separate children at once.
    //
    // Child 0 takes ticket 0 (immediately at head, phase 2) and parks,
    // blocked on this process's own hold.
    var ready0_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready0_fds));
    var done0_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&done0_fds));
    const child0_pid = std.c.fork();
    if (child0_pid == 0) {
        _ = std.posix.system.close(ready0_fds[0]);
        _ = std.posix.system.close(done0_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready0_fds[1], @ptrCast(&sig), 1);
        region.acquireWriter(my_pid);
        var report: [1]u8 = .{1};
        _ = std.c.write(done0_fds[1], @ptrCast(&report), 1);
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready0_fds[1]);
    defer _ = std.posix.system.close(ready0_fds[0]);
    _ = std.posix.system.close(done0_fds[1]);
    defer _ = std.posix.system.close(done0_fds[0]);
    var sig0: [1]u8 = undefined;
    try readBounded(ready0_fds[0], &sig0, 2000);
    {
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_tail.load(.monotonic) < 1) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 2000) return error.TestTimeout;
        }
    }

    // Child 1 takes ticket 1 -- still behind head (phase 1) -- filling
    // the ring to its exact `max_readers` limit.
    var ready1_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready1_fds));
    var done1_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&done1_fds));
    const child1_pid = std.c.fork();
    if (child1_pid == 0) {
        _ = std.posix.system.close(ready1_fds[0]);
        _ = std.posix.system.close(done1_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready1_fds[1], @ptrCast(&sig), 1);
        region.acquireWriter(my_pid);
        var report: [1]u8 = .{1};
        _ = std.c.write(done1_fds[1], @ptrCast(&report), 1);
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready1_fds[1]);
    defer _ = std.posix.system.close(ready1_fds[0]);
    _ = std.posix.system.close(done1_fds[1]);
    defer _ = std.posix.system.close(done1_fds[0]);
    var sig1: [1]u8 = undefined;
    try readBounded(ready1_fds[0], &sig1, 2000);
    {
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_tail.load(.monotonic) < 2) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 2000) return error.TestTimeout;
        }
    }

    // Exactly full: 2 outstanding tickets against `max_readers = 2`.
    try std.testing.expect(!region.ringHasRoom());

    // A third arrival, bounded: must refuse to take a ticket at all
    // (which would alias ticket 0's or ticket 1's own ring slot) and
    // instead time out once the deadline passes, ring still exactly as
    // full as it was.
    const third_deadline = Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(200));
    try std.testing.expectError(error.Timeout, region.acquireWriterFairBounded(io, own_pid, third_deadline));
    try std.testing.expectEqual(@as(u64, 2), region.header().writer_ticket_tail.load(.monotonic));

    // Release: child 0 wins, advances head past its own ticket, frees
    // one ring slot.
    region.releaseWriter();
    var done0_report: [1]u8 = undefined;
    try readBounded(done0_fds[0], &done0_report, 2000);
    try std.testing.expectEqual(@as(u8, 1), done0_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child0_pid, 2000));
    try std.testing.expect(region.ringHasRoom());

    // Child 1 then wins its own turn in the normal way.
    var done1_report: [1]u8 = undefined;
    try readBounded(done1_fds[0], &done1_report, 2000);
    try std.testing.expectEqual(@as(u8, 1), done1_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child1_pid, 2000));

    // Left in a sane, usable state, ring fully drained.
    try std.testing.expect(region.ringHasRoom());
    try std.testing.expect(region.tryAcquireWriterFair(own_pid));
    region.releaseWriter();
}
