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

//! Steps 1 through 3 of `docs/design/lite-performance/lite-single-commit-write-path.md`
//! §8: the shared mmap region (§8.1), the writer lock (§8.2), the reader
//! table (§8.3), and liveness/recovery (§8.4). §8.7's migration table has
//! landed on both sides: on the reader side, `reader_epochs.zig` is
//! deleted, `reader_coordinator.zig`'s `Coordinator` claims and pins a
//! slot here per reader instead of writing a file marker, and
//! `native.zig`'s `acquireReaderFence`/`acquireEpochReuseLock` read this
//! region directly; on the writer side, `PathWriterLock`'s own six
//! constructors are all migrated (one, `lockWriterPath`'s CLI path,
//! deliberately stays on the single plain-flock fallback, §8.6 -- see its
//! own doc comment) and `writer_queue.zig` is deleted. The writer-lane
//! fairness row of the same table (`capi/lite_connection.zig`'s
//! `WriterLane`/`QueuedMutex`/`Owner`) is a separate, concurrently landing
//! migration -- not this file's own scope.
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
// v2: the ticket ring's own owner word widened from 64 to 128 bits (a
// packed `(tag, owner)` pair instead of just `owner`), closing a window
// where a ticket could be taken (`writer_ticket_tail` advanced) before
// its owner was recorded -- a crash in that window left the ticket
// permanently unresolvable (its ring slot read as "never claimed"
// rather than "claimed by a now-dead process", so no liveness slot was
// ever known to probe). See `packTicketSlot`'s own doc comment for the
// new layout and `Region.tryClaimTicket`'s for the claim protocol this
// enables. A v1 region is simply rebuilt (the file is a disposable
// locking aid, not a migrated artifact), same as any other
// magic/version mismatch.
// v3: §9.3b's cross-process WAL index adds four live,
// mutable fields after `writer_release_epoch` -- `log_checkpoint_sequence`,
// `log_durable_sequence`, `log_durable_offset` (all `Value(u64)`) and
// `log_generation` (`Value(u32)`, a seqlock word guarding the first three
// as one consistent triple) -- growing `header_bytes` from 64 to 96. None
// of this is covered by `header_checksum` (see that function's own
// comment: only offsets 16..32 are), so the bump exists solely to make
// room, not because anything checksum-relevant changed. A v2 region is
// rebuilt on open the same way a v1 region was when v2 landed (see the
// comment above this constant's previous value) -- same
// magic/version-mismatch policy as always, not a migrated format.
// v4: adds `db_inode`, the data file's own inode number at
// the moment this region was created, to the immutable/checksummed span
// (`header_checksum`'s slice grows from 16..32 to 16..48). `openSized`
// compares it against the current data file's inode on every open, so a
// sidecar left behind after the data file at this path was replaced (a
// stale, or even foreign, `<path>.aflite-lock`) is caught instead of
// silently trusted -- the same "prove no live peer, then rebuild"
// liveness check a corrupt header already goes through, not a new
// mechanism. `db_inode` itself is 8 bytes, but the field grows
// `header_bytes` by 16 (96 to 112), not 8: `ticketOwnerOffset`'s own
// comment requires `header_bytes` to stay a multiple of 16 (the ticket
// ring's `u128` words need 16-byte alignment), so an explicit 8-byte
// reserved field pads back up to that, exactly like `_immutable_reserved`/
// `_reserved`/`_log_reserved` already do for 8-byte alignment elsewhere
// in this header. A v3 (or earlier) region is rebuilt the same way any
// earlier version mismatch always was.
// v5 (this bump): closes the liveness-slot *reuse* race `claimLivenessSlot`'s
// own doc comment used to describe as "a mitigation, not a closed race".
// Dead-writer recovery proves a holder dead by probing its recorded
// liveness slot's `fcntl` byte; the gap was that slot *index* alone is
// not a stable identity -- once the original holder is gone, a different
// process can claim that exact same index, and the stale `writer_holder`/
// `ReaderSlot.generation`/ticket-ring-owner word naming it then reads
// "alive" forever (the new claimant's own lock, not the original's).
// The real fix is a per-slot claim-generation counter (`claimLivenessSlot`
// bumps the matching slot of the new `liveness_generations` array -- see
// its own doc comment -- on every claim, reused slot or not): a
// `(slot, generation)` reference is provably dead the instant the slot's
// current generation has moved past the value it was recorded with,
// regardless of who -- if anyone -- has since reclaimed the index itself.
// `writer_holder` and `ReaderSlot.generation` each widen from a 64-bit
// `(pid, slot)` word to a 128-bit `(pid, slot, generation)` one
// (`packHolderGen`) with 32 bits left spare for future use; the ticket
// ring's own `(tag, owner)` word has no room left to grow (already a full
// 128 bits), so its claimant's generation lives in a new parallel
// `ticket_generations` array instead, keyed the same way as the ring
// itself (see its own doc comment for why the narrow publish-after-CAS
// window that creates is benign). Both new per-slot arrays sit between
// the ticket ring and the liveness/abandonment byte rings in the region's
// layout (see `livenessGenerationOffset`/`ticketGenerationOffset`).
// `header_bytes` grows from 112 to 128: `writer_holder` alone adds 16
// bytes (64 -> 128 bits), and `_log_reserved` widens from 4 to 12 bytes to
// keep the multiple-of-16 invariant `ticketOwnerOffset`'s own comment
// requires. A v4 (or earlier) region is rebuilt the same way any earlier
// version mismatch always was -- including this one.
const format_version: u32 = 5;

/// Fixed size of the header itself. The header's own `region_bytes` and
/// `max_readers` fields (not this constant) are the source of truth for
/// how much of the file is mapped and how many reader slots follow the
/// header -- but `max_readers` itself is fixed forever at a region's
/// first creation; see `max_readers`'s own doc comment for why this
/// module deliberately does not support growing it later.
const header_bytes = 128;

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
    /// v4: the data file's own inode number, read at this region's
    /// creation and never touched again -- part of the immutable,
    /// checksummed span like `region_bytes`/`max_readers` below.
    /// `openSized` compares this against the current data file's inode on
    /// every open, so a sidecar left behind after the data file at this
    /// path was replaced (its inode changed, or an entirely different
    /// file now lives there) is treated as stale/foreign rather than
    /// silently trusted. See `openSized`'s own db-inode comparison for
    /// the exact policy (prove no live peer, then rebuild; otherwise
    /// `RegionCorrupt`).
    db_inode: u64,
    /// Pads `db_inode` out to 16 bytes total, so `header_bytes` stays a
    /// multiple of 16 -- required for `ticketOwnerOffset`'s own 16-byte
    /// alignment of the ticket ring's `u128` words, which this field's
    /// own doc comment explains depends on `header_bytes` always being
    /// one. Part of the immutable, checksummed span, like `db_inode`
    /// itself.
    _db_inode_reserved: [8]u8 = @splat(0),
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
    /// §8.2/v5: 0 when free, else `packHolderGen(pid, liveness_slot,
    /// claim_generation)` -- 128 bits, widened from v4's 64-bit
    /// `(pid, liveness_slot)`-only word to also carry the liveness slot's
    /// own claim generation at the moment this word was written (see
    /// `format_version`'s own v5 comment for why: a slot index alone is
    /// not a stable identity once its original holder is gone). 32 bits
    /// are left spare for future use.
    writer_holder: std.atomic.Value(u128),
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
    /// Sized so the named fields above sum to exactly `header_bytes`,
    /// with no implicit trailing struct padding: `std.mem.asBytes`
    /// includes any such padding, and Zig's struct-literal initialization
    /// does not guarantee it is stable (let alone zero) across separate
    /// reads of it -- the root cause of an earlier version of this header
    /// rejecting its own freshly-written, uncorrupted bytes as corrupt on
    /// every reopen. `== header_bytes` below (not `<=`) keeps that
    /// invariant enforced at compile time for any future field addition.
    _reserved: [4]u8 = @splat(0),

    // -- §9.3b: the cross-process WAL index. Also live/mutable, also
    // excluded from `header_checksum` for the same reason as the fields
    // above -- these change on every durable log append, not just at
    // creation. Appended after `_reserved` (not interleaved with the v2
    // fields above) so every v2 byte offset is unchanged; only the
    // region's total size grows.
    //
    // Published together, as one consistent triple, by
    // `publishLogState` -- a seqlock (`log_generation`), not three
    // independent atomics a reader could observe torn across: the
    // writer bumps `log_generation` to odd, stores the three values
    // (plain stores; the surrounding release/acquire pair on
    // `log_generation` is what makes them visible together, not their
    // own ordering), then bumps `log_generation` back to even.
    // `readLogState` retries whenever it observes an odd generation or a
    // generation that changed between its two reads. No syscall either
    // way -- same "atomic stores into the mmap, nothing else" property
    // every other steady-state path in this file already has.
    /// The on-disk checkpoint sequence this log is currently relative to
    /// (`RedoCommitState.checkpoint_sequence` in native.zig): what a
    /// fresh reopen's `redo_log.replay` would use as `start_after_sequence`.
    /// Advances only when a real checkpoint lands (`checkpointRedoLog`),
    /// not on an ordinary redo commit.
    log_checkpoint_sequence: std.atomic.Value(u64),
    /// The highest `commit_sequence` durably appended to the log (synced,
    /// not just written) -- `RedoCommitState.durable_sequence`. Advances
    /// on every durable commit.
    log_durable_sequence: std.atomic.Value(u64),
    /// Byte offset, relative to the log region's own `base_offset`
    /// (`NativeFile.header.redo_log_offset`, read directly from the data
    /// file by any opener -- not duplicated here), of the durable end of
    /// the log -- `RedoCommitState.writer.tail` right after the append
    /// that made `log_durable_sequence` durable. A cross-process reader
    /// reads log bytes `[its own last-applied offset, log_durable_offset)`
    /// to catch up incrementally.
    log_durable_offset: std.atomic.Value(u64),
    /// Seqlock word guarding the triple above: even when stable, odd
    /// while a publish is in progress. 32 bits, matching every other
    /// futex-shaped word in this header, though nothing waits on this one
    /// (a reader retries instead of blocking -- the window it could ever
    /// observe odd is three plain stores long).
    log_generation: std.atomic.Value(u32),
    /// Pads the WAL-index fields out to keep `header_bytes` a multiple of
    /// 16 (`ticketOwnerOffset`'s own comment explains why that invariant
    /// matters), mirroring `_immutable_reserved`/`_reserved`'s own
    /// reasoning: no implicit compiler-inserted padding anywhere in this
    /// struct. Widened from 4 to 12 bytes by v5's `writer_holder` growing
    /// 64 -> 128 bits (see `format_version`'s own v5 comment).
    _log_reserved: [12]u8 = @splat(0),
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
    /// `packHolderGen(pid, liveness_slot, claim_generation)` -- same
    /// 128-bit encoding as the writer word's holder (v5; see
    /// `format_version`'s own comment), widened from a 64-bit
    /// `(pid, liveness_slot)`-only pair. Step 3's liveness probe (not just
    /// `owner != 0`) uses this to tell a slot a dead reader never cleared
    /// apart from one a new process with the same pid legitimately
    /// claimed -- and, since v5, apart from one a new process claimed via
    /// a *different* pid that happened to land on the same now-reused
    /// liveness slot index too. `releaseReaderSlot` writes this back to 0
    /// on release; a plain (non-atomic) field like before v5, not an
    /// `std.atomic.Value` -- this slot's own owner CAS is what readers
    /// actually synchronize on.
    generation: u128,
    /// The `commit_sequence` this reader will not let the writer reclaim
    /// pages behind. 0 means "not currently pinned" (sequence numbers
    /// start above 0), written once per pin by the slot's own owner,
    /// cleared to 0 on unpin. Every reader pins this one, regardless of
    /// whether it also retains metadata (matching `native.zig`'s
    /// `minimum_reader_sequence`, the broader of its two frontiers).
    pinned_sequence: std.atomic.Value(u64),
    /// A second, narrower pin: set in addition to `pinned_sequence` only
    /// by a reader that also retains allocator/recovery metadata pages
    /// (a full graph scan, snapshot, or indexed-reclamation reader with
    /// `metadata = true` -- matching `native.zig`'s
    /// `minimum_metadata_reader_sequence`). 0 means "no metadata
    /// retention from this slot"; a plain data-only reader never touches
    /// this field. Exactly fills what was reserved padding before this
    /// field existed, so no format bump was needed to add it.
    metadata_pinned_sequence: std.atomic.Value(u64),
};

/// v5: grows from 32 to 48 bytes -- `generation` widening from a 64-bit
/// `(pid, liveness_slot)` word to the 128-bit `(pid, liveness_slot,
/// claim_generation)` one adds 16 bytes (8 of them real, 8 of them the
/// extern struct's own implicit alignment padding before a 16-byte-aligned
/// field, same shape `owner`'s existing 4 bytes of implicit padding before
/// `generation` already had). Still a multiple of 16, keeping
/// `ticketOwnerOffset`'s own alignment invariant intact for every
/// `max_readers` value.
const reader_slot_bytes = 48;

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
    // offsets 16..48 (16..32 before v4's `db_inode` and its own padding
    // field extended it by 16 bytes). Two things make this slice
    // specific, not just "the rest of the struct":
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
    // 2. It excludes everything from offset 48 onward (`writer_holder`
    //    and later): those fields are live and mutated by ordinary
    //    operation, so a checksum covering them would compare a
    //    creation-time snapshot against a post-use recomputation and
    //    call the mismatch "corrupt" -- exactly what happened before this
    //    comment existed, caught by this step's own close-then-reopen
    //    test (not step 1's corrupt-header test, which corrupts before
    //    reopening and so never compares a valid header's checksum
    //    against a fresh recomputation of it).
    const bytes = std.mem.asBytes(header);
    return Crc32c.hash(bytes[16..48]);
}

/// Packs a pid with a *liveness slot index* into a narrow 64-bit holder
/// word, with no room for a claim generation (see `packHolderGen` for the
/// wide, generation-carrying peer `writer_holder`/`ReaderSlot.generation`
/// use since v5). Used only for the ticket ring's own `(tag, owner)` word
/// (`packTicketSlot`), which is already a full 128 bits and has no spare
/// room to also carry a generation inline -- see `ticket_generations`'s
/// own doc comment for where that ring's own claim generations live
/// instead. §8.4's per-process liveness byte-range lock lives at a fixed
/// index, claimed once by a process the first time it opens the region
/// (`openSized`'s `claimLivenessSlot`) and held for that `Region` handle's
/// whole lifetime -- not re-taken on every `acquireWriter`/
/// `claimReaderSlot` call, which is what keeps this design's "0 syscalls
/// on a warm, uncontended write" property intact even with liveness
/// tracking in place. `narrowHolderPid`/`narrowHolderLivenessSlot` unpack
/// this word to answer "is the process that wrote this word still alive"
/// by probing *that* index's liveness byte (`probeDead`, via
/// `isReferenceDead`), not by re-deriving anything from the pid itself --
/// no PID-reuse heuristic anywhere in this design.
fn packHolder(pid: u32, liveness_slot: u32) u64 {
    return (@as(u64, liveness_slot) << 32) | @as(u64, pid);
}

fn narrowHolderPid(word: u64) u32 {
    return @truncate(word);
}

fn narrowHolderLivenessSlot(word: u64) u32 {
    return @truncate(word >> 32);
}

/// The wide peer of `packHolder`, used wherever the reference's own word
/// has room for it: `writer_holder` and `ReaderSlot.generation`, both 128
/// bits wide since v5 (see `format_version`'s own v5 comment). Packs
/// `pid` (bits 0..32), `liveness_slot` (32..64), and `claim_generation`
/// (64..96) -- the value `liveness_generations()[liveness_slot]` held at
/// the moment this word was written, captured once per `Region` handle at
/// `claimLivenessSlot` time (`own_liveness_generation`), not recomputed on
/// every call -- with 32 bits (96..128) left spare for future use. A
/// `(liveness_slot, claim_generation)` pair unpacked back out of this word
/// is dead the instant `liveness_generations()[liveness_slot]` no longer
/// equals `claim_generation` -- see `isReferenceDead`'s own doc comment.
fn packHolderGen(pid: u32, liveness_slot: u32, claim_generation: u32) u128 {
    return (@as(u128, claim_generation) << 64) | (@as(u128, liveness_slot) << 32) | @as(u128, pid);
}

fn holderPid(word: u128) u32 {
    return @truncate(word);
}

fn holderLivenessSlot(word: u128) u32 {
    return @truncate(word >> 32);
}

fn holderClaimGeneration(word: u128) u32 {
    return @truncate(word >> 64);
}

/// Packs a ticket ring slot: `tag` (the claiming ticket's own absolute
/// number, offset by one -- see `ticketSlots`'s own doc comment for why
/// zero must stay reserved for "nobody has ever claimed this slot") in
/// the upper 64 bits, `owner` (`packHolder`'s own narrow pid/liveness-slot
/// encoding) in the lower 64. A 128-bit word so claiming a ticket number
/// and publishing its owner is one atomic CAS, not two separate writes
/// with a window between them -- already the maximum atomic width this
/// module uses anywhere, which is why `owner` stays the narrow encoding
/// rather than also widening to carry a claim generation inline; see
/// `ticket_generations`'s own doc comment for where that lives instead.
/// A ticket's ring word: its tag (`ticket + 1`) and its owner's pid,
/// liveness slot and claim generation, published together. A pid of zero
/// marks a ticket its owner gave up.
fn packTicketWord(ticket: u32, pid: u32, liveness_slot: u32, generation: u32) u128 {
    return (@as(u128, ticket +% 1) << 96) | (@as(u128, generation) << 64) | (@as(u128, liveness_slot) << 32) | @as(u128, pid);
}

fn ticketWordTag(word: u128) u32 {
    return @truncate(word >> 96);
}

fn ticketWordGeneration(word: u128) u32 {
    return @truncate(word >> 64);
}

fn ticketWordLivenessSlot(word: u128) u32 {
    return @truncate(word >> 32);
}

fn ticketWordPid(word: u128) u32 {
    return @truncate(word);
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

/// §8.4's dead-writer recovery callback, bundled with whatever context it
/// needs (a plain `*anyopaque`, this module's own usual pattern for a
/// caller-supplied callback -- see e.g. `visitIndexValue`'s `context`/
/// `visit` pair in `native.zig`): `run(ctx)` is meant to be `native.zig`'s
/// own existing on-disk checkpoint validation (`recoverActiveCheckpoint`
/// and friends), reused as-is, not reimplemented here. Passed to
/// `recoverDeadWriter` directly, or threaded through
/// `acquireWriterFairBounded`'s own `recovery` parameter so a stuck
/// waiter can run it the moment it proves the current holder dead,
/// instead of only once its own deadline has fully expired.
pub const DeadWriterRecovery = struct {
    ctx: ?*anyopaque = null,
    run: *const fn (ctx: ?*anyopaque) anyerror!void,
};

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
    return fcntlLockUntil(io, fd, offset, lock_type, null);
}

/// `fcntlLockBlocking`, bounded by the caller's `deadline` when it is
/// sooner than the 30 s budget.
fn fcntlLockUntil(io: Io, fd: std.posix.fd_t, offset: i64, lock_type: i16, deadline: ?Io.Timestamp) !void {
    const budget = Io.Clock.now(.awake, io).addDuration(.fromSeconds(30));
    const until = if (deadline) |d| (if (d.compare(.lt, budget)) d else budget) else budget;
    return antfly_platform.sync.fcntlLockBounded(io, fd, offset, lock_type, until);
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
    /// Serializes this process's region setup on this inode
    /// (`openSizedUntil`); the fcntl setup byte alone does not.
    setup_mutex: Io.Mutex = .init,
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

/// Resolves `db_path` to an absolute, symlink-resolved path before
/// `openSized` derives its own `<db_path>.aflite-lock` sidecar name from
/// it: two different relative or symlinked spellings of the same data
/// file -- even from two entirely separate processes, each independently
/// canonicalizing its own spelling -- must converge on the exact same
/// sidecar name, or two writers can each believe they hold the region's
/// only exclusive ticket while actually holding two unrelated ones. The
/// data-file counterpart of `inode_lock.zig`'s own `canonicalPathAlloc`
/// (duplicated here, deliberately, rather than shared across modules:
/// this module has no other dependency on `inode_lock.zig`, and the two
/// callers canonicalize for related but distinct reasons -- this one
/// purely for sidecar-name convergence at call time, that one to freeze
/// a path *before* `Entry.path` caches it, against a cwd change that
/// might happen before a later lazy reopen). An existing data file
/// resolves directly; one with no backing file yet -- or, for a handful
/// of this module's own tests, ever -- resolves its parent directory
/// through an open handle and joins the basename back on, mirroring
/// `native.zig`'s own `writerLockPathAlloc`.
fn canonicalPathAlloc(allocator: std.mem.Allocator, io: Io, path: []const u8) ![]u8 {
    if (Io.Dir.cwd().realPathFileAlloc(io, path, allocator)) |resolved| {
        // `realPathFileAlloc` returns a sentinel-terminated `[:0]u8`,
        // over-allocated by one byte for the terminator. Re-dupe into a
        // plain `[]u8` sized to exactly `.len` and free the sentinel
        // original here, under its own matching type -- a caller that
        // stored the sentinel slice itself into a plain `[]const u8`
        // field (dropping the sentinel from its *type*, not its
        // allocation) and freed it that way later would ask the
        // allocator to free one byte fewer than was actually allocated,
        // caught by `SafeAllocator` as a size mismatch.
        defer allocator.free(resolved);
        return try allocator.dupe(u8, resolved);
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    const dirname = std.fs.path.dirname(path) orelse ".";
    const basename = std.fs.path.basename(path);
    var parent = try Io.Dir.cwd().openDir(io, dirname, .{});
    defer parent.close(io);
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const parent_len = try parent.realPath(io, &buf);
    return try std.fs.path.join(allocator, &.{ buf[0..parent_len], basename });
}

/// Returns this process's single shared `FdEntry` for `region_path`,
/// opening and registering one on first use and incrementing its
/// refcount otherwise. Every `Region` handle in this process that opens
/// the same region file shares this exact fd for `mmap` and every
/// `fcntl` call (liveness, the one-time setup lock) -- never a second,
/// independently opened fd on the same inode. Paired with
/// `releaseProcessFd`.
/// NAME_MAX on every supported filesystem.
const max_name_bytes = 255;

fn acquireProcessFd(io: Io, region_path: []const u8) RegionError!*FdEntry {
    // A name past the filesystem limit cannot exist or be created. Refuse it
    // before `statFile`: Linux's statx wrapper treats ENAMETOOLONG as a
    // caller bug and panics in safe builds.
    if (std.fs.path.basename(region_path).len > max_name_bytes or region_path.len >= std.fs.max_path_bytes) return RegionError.RegionUnavailable;
    fd_registry_mutex.lockUncancelable(io);
    defer fd_registry_mutex.unlock(io);
    if (Io.Dir.cwd().statFile(io, region_path, .{})) |st| {
        if (fd_registry.get(st.inode)) |entry| {
            entry.refs += 1;
            return entry;
        }
    } else |err| if (err != error.FileNotFound) return RegionError.RegionUnavailable;
    // Opened read-write, close-on-exec so a forked-and-exec'd child never
    // inherits a stale mapping/lock-backing descriptor. Created here, not
    // by a separate open: this process holds no fd on the file yet.
    const raw_fd = std.posix.openat(std.posix.AT.FDCWD, region_path, .{ .ACCMODE = .RDWR, .CLOEXEC = true, .CREAT = true }, 0o600) catch return RegionError.RegionUnavailable;
    errdefer _ = std.posix.system.close(raw_fd);
    const setup_file: Io.File = .{ .handle = raw_fd, .flags = .{ .nonblocking = false } };
    const inode = (setup_file.stat(io) catch return RegionError.RegionUnavailable).inode;
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

/// Test-only counters for the writer word's own futex traffic (the
/// `writer_ticket_head`/`writer_release_epoch` wait/wake calls in
/// `acquireWriter`/`acquireWriterFairBounded`/`releaseWriter`/
/// `advanceHeadPastAbandoned`, not the reader table's separate
/// `reader_slot_released` futex) -- what "a reported lock-syscall count
/// per contended handoff" (§8.8) needs a number for. `void` outside tests
/// so these never cost anything in a real build; each forked test process
/// gets its own copy (counters are per-process state, like everything
/// else these calls touch), so a cross-process test simply reads them
/// back after its own loop with no shared-memory bookkeeping of its own.
pub const WriterFutexSnapshot = struct { wait: u64, wake: u64 };

const WriterFutexCounters = struct {
    wait: if (builtin.is_test) std.atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    wake: if (builtin.is_test) std.atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},

    fn countWait(self: *WriterFutexCounters) void {
        if (builtin.is_test) _ = self.wait.fetchAdd(1, .monotonic);
    }
    fn countWake(self: *WriterFutexCounters) void {
        if (builtin.is_test) _ = self.wake.fetchAdd(1, .monotonic);
    }
    fn reset(self: *WriterFutexCounters) void {
        if (builtin.is_test) {
            self.wait.store(0, .monotonic);
            self.wake.store(0, .monotonic);
        }
    }
    fn snapshot(self: *const WriterFutexCounters) WriterFutexSnapshot {
        if (!builtin.is_test) return .{ .wait = 0, .wake = 0 };
        return .{ .wait = self.wait.load(.monotonic), .wake = self.wake.load(.monotonic) };
    }
};
var writer_futex_counters: WriterFutexCounters = .{};

/// Test-only reset, called once per measuring process (after `fork`, not
/// before -- see the handoff-report test's own doc comment) so a loop's
/// own counts aren't polluted by whatever setup ran before it.
pub fn testResetWriterFutexCounters() void {
    writer_futex_counters.reset();
}

/// Test-only snapshot of `writer_futex_counters`; `{0, 0}` outside tests.
pub fn testWriterFutexCounters() WriterFutexSnapshot {
    return writer_futex_counters.snapshot();
}

/// `antfly_platform.sync.rawFutexWaitShared`, counted. Every writer-word
/// wait call in `acquireWriter`/`advanceHeadPastAbandoned`'s peers goes
/// through this (not the raw platform call directly) so the counters
/// above see every one of them.
fn countedRawFutexWaitShared(ptr: *const u32, expect: u32, timeout_ms: ?u32) void {
    writer_futex_counters.countWait();
    antfly_platform.sync.rawFutexWaitShared(ptr, expect, timeout_ms);
}

/// `antfly_platform.sync.futexWaitSharedBounded`, counted -- the bounded
/// peer `countedRawFutexWaitShared` above has for `acquireWriterFairBounded`.
fn countedFutexWaitSharedBounded(io: Io, ptr: *const u32, expect: u32, deadline: Io.Timestamp) Io.Cancelable!void {
    writer_futex_counters.countWait();
    return antfly_platform.sync.futexWaitSharedBounded(io, ptr, expect, deadline);
}

/// Counted wake for the writer word only (`writer_ticket_head`/
/// `writer_release_epoch`), wrapping this file's own `futexWakeShared`
/// alias further below rather than the raw platform call, so the reader
/// table's own `reader_slot_released` wakes (a separate mechanism, not
/// part of this handoff) are deliberately not counted here.
fn countedWriterFutexWakeShared(ptr: *const u32) void {
    writer_futex_counters.countWake();
    futexWakeShared(ptr);
}

pub const Region = struct {
    mapping: []align(std.heap.page_size_min) u8,
    /// Test support: runs in `tryClaimTicket` between a competing claim's
    /// death probe and its published check.
    test_after_claim_death_probe: ?*const fn (*Region) void = null,
    /// Test support: runs between a ticketless claim and its queue recheck.
    test_after_ticketless_claim: ?*const fn (*Region) void = null,
    /// Test support: runs in `minimumPinnedSequence` between a reader
    /// slot's death probe and its cleanup.
    test_after_reader_dead_probe: ?*const fn (*Region) void = null,
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
    /// v5: the claim generation `claimLivenessSlot` recorded for
    /// `own_liveness_slot` at the moment it claimed it -- i.e.
    /// `liveness_generations()[own_liveness_slot]`'s value right after
    /// that claim's own bump. Packed alongside `own_liveness_slot`
    /// wherever that is (`packHolderGen`), so a reference recorded through
    /// this handle is provably stale the instant some *later* claim of
    /// the same slot index bumps the shared counter past this value --
    /// see `format_version`'s own v5 comment and `isReferenceDead`.
    own_liveness_generation: u32,
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

    /// Test support: leaves `writer_holder` naming a writer that died.
    pub fn markDeadWriterForTest(self: *Region, pid: u32) void {
        self.header().writer_holder.store(packHolderGen(pid, self.own_liveness_slot, self.own_liveness_generation +% 1), .release);
    }

    /// Test support: clears the writer word, as dead-writer recovery does.
    pub fn clearWriterForTest(self: *Region) void {
        self.header().writer_holder.store(0, .release);
    }

    fn header(self: *const Region) *Header {
        return @ptrCast(@alignCast(self.mapping.ptr));
    }

    /// Byte offset of ticket ring slot `ticket % max_readers`'s own
    /// packed (tag, owner) word (`packTicketSlot`'s own encoding),
    /// immediately after the reader slot array -- 16-byte aligned for
    /// any `max_readers` value, since both `header_bytes` and
    /// `reader_slot_bytes` are themselves multiples of 16. See
    /// `ticketSlots`'s own doc comment for what this ring is for.
    fn ticketOwnerOffset(max_readers: u32) usize {
        return header_bytes + @as(usize, max_readers) * reader_slot_bytes;
    }

    /// v5: byte offset of liveness slot `slot_index`'s own claim-generation
    /// word (`std.atomic.Value(u32)`), immediately after the ticket ring --
    /// 4-byte aligned for any `max_readers` since `ticketOwnerOffset` is
    /// itself 16-byte aligned. This is the *source of truth* `isReferenceDead`
    /// compares a captured `(slot, generation)` reference against: bumped by
    /// `claimLivenessSlot` on every claim of this slot, reused index or not,
    /// which is the real fix `format_version`'s own v5 comment describes for
    /// the race `claimLivenessSlot`'s hash-seeded scan start used to only
    /// mitigate.
    fn livenessGenerationOffset(max_readers: u32, slot_index: u32) i64 {
        return @as(i64, @intCast(ticketOwnerOffset(max_readers))) + @as(i64, max_readers) * 16 + @as(i64, slot_index) * 4;
    }

    /// v5: byte offset of ticket ring slot `slot_index`'s own captured
    /// claimant liveness-slot generation (`std.atomic.Value(u32)`),
    /// immediately after `liveness_generations` -- see `ticket_generations`'s
    /// own doc comment for why this lives in its own parallel array instead
    /// of packed into the ticket ring's `(tag, owner)` word itself.
    fn ticketGenerationOffset(max_readers: u32, slot_index: u32) i64 {
        return livenessGenerationOffset(max_readers, 0) + @as(i64, max_readers) * 4 + @as(i64, slot_index) * 4;
    }

    /// Byte offset of liveness slot `slot_index`'s dedicated byte,
    /// immediately after `ticket_generations` (§8.1's "M*1 process liveness
    /// bytes, one per slot"; v5 inserts `liveness_generations` and
    /// `ticket_generations` between the ticket ring and this byte ring --
    /// see their own doc comments). Content is never read or written; the
    /// byte exists purely as an `fcntl` lock target.
    fn livenessByteOffset(max_readers: u32, slot_index: u32) i64 {
        return ticketGenerationOffset(max_readers, 0) + @as(i64, max_readers) * 4 + @as(i64, slot_index);
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

    /// Total region size for `max_readers` slots: the header plus every
    /// per-slot array that follows it (reader slots, the ticket ring,
    /// `liveness_generations`, `ticket_generations`, the liveness bytes,
    /// and the abandonment-flag ring). Factored out so `openSized`'s own
    /// rebuild-from-scratch sizing and `ensureNoLivePeer`'s own "does the
    /// claimed slot count even fit the file" sanity check can't drift
    /// apart from each other or from the real per-array offsets above.
    fn regionBytesFor(max_readers: u32) u64 {
        return @as(u64, @intCast(ticketAbandonedOffset(max_readers, 0))) + @as(u64, max_readers);
    }

    /// v5: the per-liveness-slot claim-generation array immediately after
    /// the ticket ring -- the authoritative "how many times has this
    /// index been claimed" counter `claimLivenessSlot` bumps on every
    /// claim (see its own doc comment) and `isReferenceDead` compares a
    /// captured `(slot, generation)` reference against. Takes the mapping
    /// directly (not `self`) so `claimLivenessSlot` -- called before the
    /// `Region` value it will become even exists -- can reach it too.
    fn livenessGenerationsFromMapping(mapping: []align(std.heap.page_size_min) u8, max_readers: u32) []std.atomic.Value(u32) {
        const base: [*]std.atomic.Value(u32) = @ptrCast(@alignCast(mapping.ptr + @as(usize, @intCast(livenessGenerationOffset(max_readers, 0)))));
        return base[0..max_readers];
    }

    fn livenessGenerations(self: *const Region) []std.atomic.Value(u32) {
        return livenessGenerationsFromMapping(self.mapping, self.header().max_readers);
    }

    /// v5: one claim-generation slot per ticket ring index, immediately
    /// after `liveness_generations` -- the ticket ring's own parallel peer
    /// to `writer_holder`/`ReaderSlot.generation`'s inline
    /// `packHolderGen`, kept separate because the ring's own `(tag,
    /// owner)` word (`packTicketSlot`) is already the maximum atomic width
    /// this module uses (128 bits) with no room left to also carry a
    /// generation. Written by `tryClaimTicket` right after it wins a ring
    /// slot's `(tag, owner)` CAS -- a few instructions after `owner`
    /// itself becomes visible, not atomically with it. That narrow window
    /// is benign: every reader of this array already gates on `owner !=
    /// 0` first, and the only possible outcome of racing it is a stale
    /// *previous* occupant's generation being compared against the new
    /// occupant's (live, by construction, since it just won the CAS)
    /// liveness slot -- at worst one spurious extra `fcntl` probe that a
    /// microsecond-later re-check would avoid, never a false "dead"
    /// result that persists.
    fn ticketGenerations(self: *const Region) []std.atomic.Value(u32) {
        const max_readers = self.header().max_readers;
        const base: [*]std.atomic.Value(u32) = @ptrCast(@alignCast(self.mapping.ptr + @as(usize, @intCast(ticketGenerationOffset(max_readers, 0)))));
        return base[0..max_readers];
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
        //
        // The probe stays under the registry mutex: a handle of this
        // process claiming `slot_index` after the check (`claimLivenessSlot`
        // holds the same mutex) would otherwise have its new lock released
        // by the probe's unlock.
        fd_registry_mutex.lockUncancelable(self.io);
        defer fd_registry_mutex.unlock(self.io);
        const mine = for (self.fd_entry.claimed_slots.items) |claimed| {
            if (claimed == slot_index) break true;
        } else false;
        if (mine) return false;
        return probeDead(self.fd_entry.fd, livenessByteOffset(self.header().max_readers, slot_index));
    }

    /// v5: the real liveness check every reference to a liveness slot
    /// should use (`isSlotDead` alone stays available for
    /// `ensureNoLivePeer`'s own scan, which has no captured generation to
    /// compare against -- it is asking "is *anything* live at this index
    /// right now", not "is the specific claim I recorded still the one
    /// holding it"). `claim_generation` is whatever was packed alongside
    /// `slot_index` when the reference being checked was recorded
    /// (`holderClaimGeneration`/`ticket_generations`, captured from the
    /// claimant's own `own_liveness_generation` at the time).
    ///
    /// A generation mismatch alone proves the reference dead, with no
    /// `fcntl` call at all: once `liveness_generations()[slot_index]` has
    /// moved past `claim_generation`, nothing can ever again be the claim
    /// that recorded it, regardless of whether the index itself currently
    /// sits empty, is held by a live process, or was reclaimed by a dead
    /// one in turn -- the exact slot-reuse race `format_version`'s own v5
    /// comment describes, now closed structurally rather than merely made
    /// unlikely. Only a *matching* generation falls through to the real
    /// `fcntl` probe, to tell "the original claimant is still alive" apart
    /// from "the original claimant simply hasn't been superseded yet" --
    /// both read the same generation, and only one of them is actually
    /// dead.
    fn isReferenceDead(self: *const Region, slot_index: u32, claim_generation: u32) bool {
        const current = self.livenessGenerations()[slot_index].load(.acquire);
        if (current != claim_generation) return true;
        return self.isSlotDead(slot_index);
    }

    /// Scans liveness slots `[0, max_readers)`, starting from a per-process
    /// offset (see below), for the first one this process can claim,
    /// holding it for the rest of this `Region` handle's lifetime. Called
    /// once per handle, from `openSized`, never from the per-commit
    /// acquire/claim paths.
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
    ///
    /// §8.4's liveness probe identifies a holder purely by slot *index*
    /// (`writer_holder`/a reader slot's `generation`/a ticket's own owner
    /// word all pack `(pid, liveness_slot)`), and there is no kernel
    /// primitive tying a slot index to the process that last held it once
    /// that process is gone -- so a *different* process can legitimately
    /// claim the exact index a dead holder just freed. v5's
    /// `liveness_generations` array is the real fix for the staleness that
    /// would otherwise cause (see `format_version`'s own v5 comment and
    /// `isReferenceDead`): every claim here, reused index or not, bumps
    /// the matching slot's own generation, so a reference recorded before
    /// this claim reads as dead immediately, regardless of which index it
    /// named.
    ///
    /// That leaves this scan free to optimize purely for *performance*,
    /// not correctness: starting it at a per-process hash of the pid
    /// (rather than always at index 0) spreads concurrent claims across
    /// the whole range instead of serializing every opener's first few
    /// `fcntl` attempts onto the same low indices, which matters under
    /// bursty open/close churn (many short-lived handles claiming and
    /// releasing slots in quick succession -- the common shape right
    /// after a crash: a repro, a freshly started process, a test). Unlike
    /// the pre-v5 version of this comment, there is no longer any reason
    /// to also mix in the current time: that existed solely so a caller
    /// retrying a whole fresh `Region.open()` after colliding with this
    /// *specific* race wouldn't deterministically repeat the same losing
    /// scan -- moot now that a collision on a reused index is no longer a
    /// losing outcome at all. See native.zig's "lite fair writer lock
    /// recovers promptly from a killed writer's stale lease" for the
    /// dead-writer-recovery path this all ultimately serves.
    fn claimLivenessSlot(io: Io, entry: *FdEntry, mapping: []align(std.heap.page_size_min) u8, max_readers: u32) RegionError!struct { slot: u32, generation: u32 } {
        fd_registry_mutex.lockUncancelable(io);
        defer fd_registry_mutex.unlock(io);
        const pid = antfly_platform.process.currentId() orelse 0;
        // Reserve the bookkeeping first: a byte locked without its
        // `claimed_slots` entry would stay locked for the shared fd's life
        // and read as a live claimant to every peer.
        entry.claimed_slots.ensureUnusedCapacity(std.heap.page_allocator, 1) catch return RegionError.LivenessSlotsExhausted;
        const start: u32 = @intCast((@as(u64, pid) *% 2654435761) % max_readers);
        var scanned: u32 = 0;
        next_index: while (scanned < max_readers) : (scanned += 1) {
            const i = (start + scanned) % max_readers;
            for (entry.claimed_slots.items) |claimed| {
                if (claimed == i) continue :next_index;
            }
            if (fcntlTryLock(entry.fd, livenessByteOffset(max_readers, i), std.c.F.WRLCK)) {
                entry.claimed_slots.appendAssumeCapacity(i);
                // Bump unconditionally, on *every* claim of this index --
                // including this process's own very first claim of a
                // never-before-claimed slot (0 -> 1): what matters is that
                // this claim's own generation is strictly greater than
                // whatever any *previous* claim of this same index last
                // recorded, not that it starts at any particular value.
                const new_generation = livenessGenerationsFromMapping(mapping, max_readers)[i].fetchAdd(1, .acq_rel) +% 1;
                return .{ .slot = i, .generation = new_generation };
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
        const claimed = try claimLivenessSlot(self.io, self.fd_entry, self.mapping, self.header().max_readers);
        self.own_liveness_slot = claimed.slot;
        self.own_liveness_generation = claimed.generation;
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
        return openSizedUntil(allocator, io, db_path, max_readers, null);
    }

    /// `open`, with the one-time setup lock bounded by `deadline` instead of
    /// its own 30 s budget: a writer admission must not outwait its own
    /// busy timeout setting up the region it queues on.
    pub fn openUntil(allocator: std.mem.Allocator, io: Io, db_path: []const u8, deadline: Io.Timestamp) !Region {
        return openSizedUntil(allocator, io, db_path, default_max_readers, deadline);
    }

    fn openSizedUntil(allocator: std.mem.Allocator, io: Io, db_path: []const u8, max_readers: u32, deadline: ?Io.Timestamp) !Region {
        // Canonicalize before deriving the sidecar name from it: two
        // different relative or symlinked spellings of the same data
        // file -- even from two entirely separate processes, each
        // independently canonicalizing its own spelling -- must converge
        // on the exact same `<path>.aflite-lock`, or two writers can each
        // believe they hold the region's only exclusive ticket. See
        // `canonicalPathAlloc`'s own doc comment.
        const canonical_db_path = try canonicalPathAlloc(allocator, io, db_path);
        defer allocator.free(canonical_db_path);
        const region_path = try std.fmt.allocPrint(allocator, "{s}.aflite-lock", .{canonical_db_path});
        defer allocator.free(region_path);
        // Best-effort: most callers reach this only once a real data file
        // exists at `db_path` (`inode_lock.ensureRegion`, every migrated
        // writer/reader path), and for those this is the data file's real
        // inode, checked against the header below. A handful of this
        // module's own tests exercise the sidecar alone, with no backing
        // data file ever created at `db_path` -- for those this is simply
        // 0, and the comparison below treats 0 on either side as "not
        // tied to a real file," never a mismatch.
        const db_inode: u64 = if (Io.Dir.cwd().statFile(io, canonical_db_path, .{})) |st| st.inode else |_| 0;

        // This process's one shared fd for this inode (see
        // `acquireProcessFd`'s own doc comment for why it must be shared,
        // never a second fd opened independently) -- used for the one-
        // time create/validate lock below, the corruption-liveness
        // probe, `claimLivenessSlot`, and later `mmap`; `Io.File` has no
        // byte-range lock primitive of its own. Setup reads and sizes the
        // file through it too: closing *any* other fd on this file drops
        // every `fcntl` lock this process holds on it, including the
        // liveness slots of this process's other live handles, and a peer
        // would then see their writer or tickets as dead.
        const fd_entry = try acquireProcessFd(io, region_path);
        const raw_fd = fd_entry.fd;
        errdefer releaseProcessFd(io, fd_entry);
        const setup_file: Io.File = .{ .handle = raw_fd, .flags = .{ .nonblocking = false } };
        // The only blocking lock this module takes, and only here: an
        // `fcntl` byte-range lock (not `Io.File.lock`'s whole-file
        // `flock`; see `setup_lock_offset`'s own doc comment for why)
        // serializing create/validate/rebuild against every other
        // process's own `openSized` call on this same region. Released
        // explicitly below, once this handle has claimed its liveness slot,
        // so another process's rebuild (`ensureNoLivePeer`) sees it live.
        //
        // The byte is an fcntl lock, which this process's threads all
        // share: the per-fd setup mutex serializes them as well, or two
        // could both sample a fresh file and one truncate a region the
        // other already mapped.
        // One deadline covers this wait and the fcntl one: a same-process
        // opener can hold the mutex while it waits on another process.
        if (deadline) |d| {
            while (!fd_entry.setup_mutex.tryLock()) {
                if (Io.Clock.now(.awake, io).compare(.gte, d)) return error.Timeout;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        } else fd_entry.setup_mutex.lockUncancelable(io);
        defer fd_entry.setup_mutex.unlock(io);
        try fcntlLockUntil(io, raw_fd, setup_lock_offset, std.c.F.WRLCK, deadline);
        errdefer fcntlUnlock(raw_fd, setup_lock_offset);

        const size = (try setup_file.stat(io)).size;
        const fresh_file = size < header_bytes;
        var need_init = fresh_file;
        var existing_raw: [header_bytes]u8 = undefined;
        if (!need_init) {
            const n = try setup_file.readPositionalAll(io, &existing_raw, 0);
            need_init = n != existing_raw.len or !isValidHeader(&existing_raw);
            if (!need_init) {
                // A structurally valid header for a *different* data
                // file: the data file this sidecar used to sit next to
                // was removed and replaced (its inode changed) without
                // its old sidecar going with it, or this path's sidecar
                // was never this data file's own to begin with. Treat
                // exactly like corruption -- same liveness proof before
                // rebuilding, same `RegionCorrupt` if that proof fails.
                const existing_header: *const Header = @ptrCast(@alignCast(&existing_raw));
                if (existing_header.db_inode != 0 and db_inode != 0 and existing_header.db_inode != db_inode) {
                    need_init = true;
                }
            }
        }
        if (need_init and !fresh_file) {
            // A file that already existed and is at least header-sized,
            // but fails validation (or belongs to a different data file,
            // per the inode check above): prove no live process is
            // relying on it before even considering a rebuild.
            try ensureNoLivePeer(io, fd_entry, &existing_raw, size);
        }
        if (need_init) {
            // `regionBytesFor`'s own doc comment has the per-slot layout.
            const region_bytes = regionBytesFor(max_readers);
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
                .db_inode = db_inode,
                .max_readers = max_readers,
                .writer_holder = .init(0),
                .writer_ticket_tail = .init(0),
                .writer_ticket_head = .init(0),
                .reader_slot_released = .init(0),
                .writer_release_epoch = .init(0),
                .log_checkpoint_sequence = .init(0),
                .log_durable_sequence = .init(0),
                .log_durable_offset = .init(0),
                .log_generation = .init(0),
            };
            fresh.header_checksum = headerChecksum(&fresh);
            try setup_file.writePositionalAll(io, std.mem.asBytes(&fresh), 0);
            // Deliberately no `sync(io)` here: the region is a disposable
            // coordination aid, not a durable record -- a torn or missing
            // header is caught by `isValidHeader`'s checksum check and
            // simply rebuilt (under `ensureNoLivePeer`'s liveness proof)
            // the next time anything opens it, exactly like any other
            // corruption this same function already tolerates. Durability
            // of this header buys nothing but an fsync's latency, and
            // landing that fsync inside whichever caller's first
            // page-reusing commit happens to need the region first
            // (`NativeFile.openWithRuntime`/`createWithRuntime` now call
            // `ensureRegion` eagerly at open/create time specifically to
            // avoid that) would reintroduce the exact cost this comment
            // is here to rule back out.
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

        const mapping = std.posix.mmap(null, map_len, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, raw_fd, 0) catch return RegionError.RegionUnavailable;
        errdefer std.posix.munmap(mapping);
        const claimed = try claimLivenessSlot(io, fd_entry, mapping, final_max_readers);
        fcntlUnlock(raw_fd, setup_lock_offset);
        return .{ .mapping = mapping, .fd_entry = fd_entry, .own_liveness_slot = claimed.slot, .own_liveness_generation = claimed.generation, .io = io };
    }

    /// The pre-rebuild safety check `openSized`'s doc comment describes.
    /// `existing_raw` is the (unvalidated -- that's the whole point)
    /// header this file already had. Succeeds only if every liveness
    /// byte it plausibly could have had is provably free.
    ///
    /// `fd_entry` (not just the raw fd) matters here for exactly the
    /// reason `isSlotDead` already takes one: `fcntl(F_SETLK)` byte-range
    /// locks are scoped to a *process*, not to a specific `Region` handle
    /// or claim, so a slot THIS process already claimed through some
    /// *other*, still-live `Region` handle sharing this same fd (an
    /// in-process peer, not a foreign/dead one) would otherwise look
    /// exactly like a free byte to a raw `fcntlTryLock` probe --
    /// re-locking your own process's lock trivially "succeeds". Probing
    /// it anyway, then immediately `fcntlUnlock`-ing it as "just a
    /// probe", would release that other handle's *real* liveness lock
    /// out from under it, and this function would wrongly report "no
    /// live peer" the instant before `openSized` truncates and rebuilds
    /// the very file that handle's own mapping is still backed by --
    /// exactly the bus error this guard exists to prevent (confirmed
    /// live: `Store.performVacuum`'s read snapshot/maintenance-worker
    /// pairing hits this every time a generation swap's `inode_lock.
    /// rebind` reopens a region whose sidecar still has another,
    /// genuinely live, same-process reader's slot on it). Every index
    /// already in `fd_entry.claimed_slots` is therefore treated as proof
    /// of a live peer *without ever touching its byte range* -- the same
    /// "mine" guard `isSlotDead` already applies per-slot, just run
    /// across the whole claimed range before any probe, not after.
    fn ensureNoLivePeer(io: Io, fd_entry: *FdEntry, existing_raw: *const [header_bytes]u8, actual_file_bytes: u64) RegionError!void {
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
        const implied_bytes = regionBytesFor(claimed_max_readers);
        if (implied_bytes > actual_file_bytes) {
            // The claimed slot count doesn't even fit the file's actual
            // size -- the header is corrupt enough that this count can't
            // be trusted to define a real scan range either, so there is
            // no range left to prove safe. Refuse rather than guess.
            return RegionError.RegionCorrupt;
        }
        const fd = fd_entry.fd;
        var i: u32 = 0;
        while (i < claimed_max_readers) : (i += 1) {
            fd_registry_mutex.lockUncancelable(io);
            const mine = for (fd_entry.claimed_slots.items) |claimed| {
                if (claimed == i) break true;
            } else false;
            fd_registry_mutex.unlock(io);
            if (mine) return RegionError.RegionCorrupt;
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
    ///
    /// `pub` (unlike most of this module's own private building blocks)
    /// because `native.zig`'s own §8.2 "plain" mode
    /// (`lockWriterPathWithIo`) needs exactly this -- a single nonblocking
    /// attempt that, like that function's own pre-region behavior, jumps
    /// the FIFO rather than respecting it (`tryAcquireWriterFair` below
    /// is the FIFO-respecting peer `tryFairWriterLockWithIo` migrates onto
    /// instead).
    pub fn tryAcquireWriter(self: *Region, pid: u32) bool {
        const want = packHolderGen(pid, self.own_liveness_slot, self.own_liveness_generation);
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
        if (self.tryAcquireWriterTicketless(pid)) return;
        // `rawFutexWaitShared`, not an uncancelable wait with no Io: see
        // this function's own module-level peers for why it takes none,
        // and `tryHelpDeadTicketAt`'s own doc comment for why phase 1
        // needs a *bounded* step at all now, not a true indefinite park.
        var backoff_ms: u32 = ring_full_backoff_initial_ms;
        const my_ticket: u32 = while (true) {
            const ring_head = hdr.writer_ticket_head.load(.acquire);
            if (!self.ringHasRoomAt(ring_head)) {
                countedRawFutexWaitShared(&hdr.writer_ticket_head.raw, ring_head, backoff_ms);
                backoff_ms = @min(backoff_ms * 2, ring_full_backoff_max_ms);
                self.skipResolvedHeads();
                continue;
            }
            // `tryClaimTicket` publishes the owner atomically with the
            // ticket number itself -- see its own doc comment. A `null`
            // result resolves itself quickly (a live contender publishes
            // within a couple of instructions; a dead one is cleared by
            // this same call), so retrying immediately with a freshly
            // read `tail` is deliberate, not a missing backoff.
            const t = hdr.writer_ticket_tail.load(.monotonic);
            if (self.tryClaimTicket(t, pid)) |ticket| break ticket;
        };
        while (true) {
            const head = hdr.writer_ticket_head.load(.acquire);
            if (head == my_ticket) break;
            countedRawFutexWaitShared(&hdr.writer_ticket_head.raw, head, dead_ticket_check_step_ms);
            self.skipResolvedHeads();
        }
        while (true) {
            // Sample the release epoch before trying: a release between the
            // attempt and the sample would otherwise park this waiter on a
            // free writer word.
            const epoch = hdr.writer_release_epoch.load(.acquire);
            if (self.tryAcquireWriter(pid)) {
                self.finishTurn(my_ticket);
                return;
            }
            countedRawFutexWaitShared(&hdr.writer_release_epoch.raw, epoch, null);
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
        return self.tryAcquireWriterTicketless(pid);
    }

    /// Takes the writer word without a ticket, only while nobody is queued.
    /// The queue is checked again after the claim: a ticket published
    /// between the first check and the claim came first, so the word is
    /// handed back to it rather than overtaking it.
    fn tryAcquireWriterTicketless(self: *Region, pid: u32) bool {
        const hdr = self.header();
        if (hdr.writer_ticket_head.load(.acquire) != @as(u32, @truncate(hdr.writer_ticket_tail.load(.acquire)))) return false;
        if (!self.tryAcquireWriter(pid)) return false;
        if (comptime builtin.is_test) if (self.test_after_ticketless_claim) |hook| hook(self);
        if (hdr.writer_ticket_head.load(.acquire) == @as(u32, @truncate(hdr.writer_ticket_tail.load(.acquire)))) return true;
        self.releaseWriter();
        return false;
    }

    /// Whether a writer that has died still holds the writer word, or a
    /// resolved ticket sits at the head of the queue: state only a bounded
    /// acquisition (`acquireWriterFairBounded`) repairs.
    pub fn staleWriterState(self: *const Region) bool {
        if (self.writerHolderDead()) return true;
        const hdr = self.header();
        const head = hdr.writer_ticket_head.load(.acquire);
        if (head == @as(u32, @truncate(hdr.writer_ticket_tail.load(.acquire)))) return false;
        return self.ticketResolvable(self.ticketSlots()[head % hdr.max_readers].load(.acquire), head);
    }

    /// Whether any writer holds the word, live or dead.
    pub fn writerWordHeld(self: *const Region) bool {
        return self.header().writer_holder.load(.acquire) != 0;
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
    /// `own_pid`, *or* to a different in-process opener of this same
    /// region (a distinct `Region.open` -- distinguished by
    /// `own_liveness_slot`, unique per open, same `claimLivenessSlot`
    /// identity `isSlotDead` below already keys dead-process detection
    /// on) -- `PathWriterLock.othersWaitingAcrossProcesses`'s own query,
    /// for a caller (`WriterLane`'s own `checkin`) that must not treat
    /// its own same-process, same-opener callers (reaching the queue
    /// through a different code path on the very same `Region` handle)
    /// as a hostile waiter to release for, the same distinction
    /// `writer_queue.zig`'s own per-ticket pid byte made. A second,
    /// independent `Owner` in this same process (for example a
    /// read-only connection opened with incompatible options, which
    /// gets its own `Region.open` and its own liveness slot) is not
    /// that caller: it genuinely cannot make progress until this lease
    /// lets go, and relying on `maintainParkedLease`'s idle timer alone
    /// to ever serve it starves it exactly as a cross-process waiter
    /// would be starved without this same check -- confirmed live as
    /// `TestLiteCAPI`'s "snapshot with open transaction" timing out in
    /// `fairWriterLockWithIo`. Scans every outstanding ticket (bounded
    /// by `max_readers`, same as every other scan in this file),
    /// skipping ones already known abandoned or dead -- a ticket that
    /// will never actually show up to claim the lease should not count
    /// as "someone is waiting" either.
    pub fn othersWaitingAcrossProcesses(self: *const Region, own_pid: u32) bool {
        const hdr = self.header();
        const head = hdr.writer_ticket_head.load(.monotonic);
        const tail: u32 = @truncate(hdr.writer_ticket_tail.load(.monotonic));
        if (head == tail) return false;
        const slots = self.ticketSlots();
        var t = head;
        while (t != tail) : (t +%= 1) {
            const word = slots[t % hdr.max_readers].load(.acquire);
            if (word == 0 or ticketWordTag(word) != t +% 1 or ticketWordPid(word) == 0) continue;
            const owner = packHolder(ticketWordPid(word), ticketWordLivenessSlot(word));
            // `0`: this exact ring index is mid-transition (its own
            // claimant is between `tryClaimTicket`'s own two steps, or a
            // resolver just cleared it and hasn't advanced `head` past it
            // yet) -- not a foreign waiter to report either way; whoever
            // is transitioning it will finish the job.
            if (owner == 0) continue;
            if (narrowHolderPid(owner) == own_pid and narrowHolderLivenessSlot(owner) == self.own_liveness_slot) continue;
            if (self.isReferenceDead(ticketWordLivenessSlot(word), ticketWordGeneration(word))) continue;
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
    ///
    /// Returns whether this acquisition was genuinely contended (took a
    /// real ticket) rather than winning the ticketless fast path --
    /// `native.zig`'s own `fairWriterLockWithIo` has no other way to
    /// tell a caller like `capi/lite_connection.zig`'s own admission
    /// stats whether this lease was `.queued` or `.uncontended`, the
    /// same distinction `writer_queue.zig`'s own `queue_turn` (a real
    /// turn) versus `queue_watch` (an idle-queue bypass) used to carry on
    /// the returned lease itself before this design moved off it.
    ///
    /// `recovery`, when non-null, makes this call §8.4-aware: once this
    /// ticket genuinely owns the turn (phase 2 below) but `writer_holder`
    /// is still held, each wait step is bounded by the short
    /// `dead_ticket_check_step_ms` poll (not the full `deadline`) so a
    /// *dead* holder's word gets probed (`writerHolderDead`) promptly
    /// instead of only once, at the bitter end, when this call's own
    /// deadline finally expires -- the difference between one later
    /// writer paying the full busy-timeout once and every later writer
    /// paying it forever (the dead holder never calls `releaseWriter`, so
    /// nothing else ever clears the word). On a proven-dead holder,
    /// `recoverDeadWriter(recovery)` runs the caller-supplied recovery
    /// (meant to be `native.zig`'s own on-disk checkpoint validation,
    /// §8.4's own "existing code reused, not new recovery logic") and, on
    /// success, clears the word and retries the CAS immediately -- no
    /// extra wait. `error.WriterHolderStillAlive` from a losing race
    /// against this same ticket's own earlier recovery attempt (or a
    /// holder that was merely slow, not dead, and has since released
    /// normally) is swallowed: the loop just re-checks `tryAcquireWriter`
    /// and keeps waiting if it still fails. `null` (every pre-existing
    /// caller, including every test in this file) reproduces the exact
    /// prior behavior and cost -- one bounded wait for the whole
    /// `deadline`, no extra liveness probes.
    ///
    /// Only one process can ever be probing/recovering through this path
    /// at a time: the FIFO ticket already guarantees at most one claimant
    /// has `head == my_ticket` (phase 2) at once, so there is no
    /// concurrent-recovery race to guard against here -- see this
    /// function's own module-level liveness doc (§8.4) and
    /// `recoverDeadWriter`'s own re-check of `writerHolderDead` right
    /// before running `recovery`.
    pub fn acquireWriterFairBounded(self: *Region, io: Io, pid: u32, deadline: Io.Timestamp, recovery: ?DeadWriterRecovery) !bool {
        const hdr = self.header();
        if (self.tryAcquireWriterTicketless(pid)) return false;
        const my_ticket: u32 = while (true) {
            const ring_head = hdr.writer_ticket_head.load(.acquire);
            if (!self.ringHasRoomAt(ring_head)) {
                const now = Io.Clock.now(.awake, io);
                if (now.compare(.gte, deadline)) return error.Timeout;
                // Wait on the same head sample the check used, and retire a
                // dead head ourselves on a short step: a full ring of dead
                // tickets has no live owner to advance it.
                const step_deadline = now.addDuration(.fromMilliseconds(dead_ticket_check_step_ms));
                countedFutexWaitSharedBounded(io, &hdr.writer_ticket_head.raw, ring_head, if (step_deadline.compare(.lt, deadline)) step_deadline else deadline) catch return error.Canceled;
                self.skipResolvedHeads();
                continue;
            }
            // See `acquireWriter`'s own identical claim loop:
            // `tryClaimTicket` publishes the owner atomically with the
            // ticket number, so a `null` result just means retry with a
            // freshly read `tail`, not a wait.
            const t = hdr.writer_ticket_tail.load(.monotonic);
            if (self.tryClaimTicket(t, pid)) |ticket| break ticket;
            if (Io.Clock.now(.awake, io).compare(.gte, deadline)) return error.Timeout;
        };
        while (true) {
            const head = hdr.writer_ticket_head.load(.acquire);
            if (head == my_ticket) break;
            const now = Io.Clock.now(.awake, io);
            if (now.compare(.gte, deadline)) {
                self.abandonTicket(my_ticket);
                return error.Timeout;
            }
            // A dead queued owner never moves `head`: recheck on a short
            // step rather than only when this caller's deadline expires.
            const step_deadline = now.addDuration(.fromMilliseconds(dead_ticket_check_step_ms));
            // A canceled waiter gives its ticket up like a timed-out one:
            // its owner stays alive, so nobody else would ever skip it.
            countedFutexWaitSharedBounded(io, &hdr.writer_ticket_head.raw, head, if (step_deadline.compare(.lt, deadline)) step_deadline else deadline) catch {
                self.abandonTicket(my_ticket);
                return error.Canceled;
            };
            self.skipResolvedHeads();
        }
        while (true) {
            // Sample before trying; see `acquireWriter`'s phase 2.
            const epoch = hdr.writer_release_epoch.load(.acquire);
            if (self.tryAcquireWriter(pid)) {
                self.finishTurn(my_ticket);
                return true;
            }
            if (recovery) |rec| {
                if (self.writerHolderDead()) {
                    self.recoverDeadWriter(rec) catch |err| switch (err) {
                        error.WriterHolderStillAlive => {},
                        else => {
                            // Give the turn up so the next writer can retry
                            // the recovery; a live liveness slot would
                            // otherwise keep this ticket at the head.
                            self.finishTurn(my_ticket);
                            return err;
                        },
                    };
                    continue;
                }
            }
            if (Io.Clock.now(.awake, io).compare(.gte, deadline)) {
                // Our own turn, but we're giving it up: step aside
                // ourselves, exactly like a real win's own advance, just
                // without ever touching `writer_holder`.
                self.finishTurn(my_ticket);
                return error.Timeout;
            }
            const wait_deadline = if (recovery != null) blk: {
                const step_deadline = Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(dead_ticket_check_step_ms));
                break :blk if (step_deadline.compare(.lt, deadline)) step_deadline else deadline;
            } else deadline;
            countedFutexWaitSharedBounded(io, &hdr.writer_release_epoch.raw, epoch, wait_deadline) catch {
                self.finishTurn(my_ticket);
                return error.Canceled;
            };
        }
    }

    /// Waits (bounded by `deadline`) until the writer word next looks
    /// free, without taking a ticket or attempting the CAS itself --
    /// for a caller that only needs a wake signal in place of busy-
    /// polling the real kernel flock, not a second admission decision of
    /// its own. Returns normally (not an error) on a plain deadline
    /// expiry -- exactly like `acquireWriterFairBounded`'s own futex
    /// waits, this is a wake signal, not a decision, so the caller's own
    /// next real attempt is what decides success or `FileBusy`.
    ///
    /// Not yet wired to a caller: `native.zig`'s own `waitPathLockWithIo`
    /// (nested under a ticket its own caller's `plainWriterLockUntil`
    /// loop already resolved, or called with no ticket at all) would be
    /// the natural one, but it has no `Region` of its own to call this
    /// on -- opening a fresh one per retry iteration serializes every
    /// concurrent retrier through `Region.open`'s own one-time setup
    /// lock and was measured to intermittently blow a 10 s per-attempt
    /// deadline under three-way contention. Reusing the caller's own
    /// already-open `Region` needs that reference threaded through (a
    /// signature change to this function or `plainWriterLockUntil`),
    /// deliberately left for that decision rather than guessed at here.
    ///
    /// `releaseWriter` only wakes `writer_release_epoch` when a ticket is
    /// outstanding (its own doc comment: "the fully uncontended release
    /// stays a single store with no syscall") -- a caller here may have
    /// no ticket, and nobody else may ever take one either, so this
    /// cannot rely purely on that wake arriving. It re-checks on a short,
    /// bounded step (`dead_ticket_check_step_ms`, the same cadence
    /// `acquireWriter`'s own phase-1 dead-ticket poll already uses)
    /// whenever no real wake arrives sooner.
    pub fn waitWriterFreeBounded(self: *const Region, io: Io, deadline: Io.Timestamp) error{Canceled}!void {
        const hdr = self.header();
        while (hdr.writer_holder.load(.acquire) != 0) {
            if (Io.Clock.now(.awake, io).compare(.gte, deadline)) return;
            const epoch = hdr.writer_release_epoch.load(.acquire);
            if (hdr.writer_holder.load(.acquire) == 0) return;
            const step_deadline = Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(dead_ticket_check_step_ms));
            const bounded_deadline = if (step_deadline.compare(.lt, deadline)) step_deadline else deadline;
            countedFutexWaitSharedBounded(io, &hdr.writer_release_epoch.raw, epoch, bounded_deadline) catch return error.Canceled;
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
        return self.ringHasRoomAt(self.header().writer_ticket_head.load(.monotonic));
    }

    /// `ringHasRoom` against a `head` the caller also waits on, so a head
    /// advance between the check and the wait cannot be missed. Counted in
    /// `head`'s 32-bit width: past 2^32 turns the 64-bit `tail` would
    /// otherwise read as a permanently full ring.
    fn ringHasRoomAt(self: *const Region, head: u32) bool {
        const hdr = self.header();
        return @as(u32, @truncate(hdr.writer_ticket_tail.load(.acquire))) -% head < hdr.max_readers;
    }

    const ring_full_backoff_initial_ms: u32 = 5;
    const ring_full_backoff_max_ms: u32 = 50;
    /// How often a phase-1 waiter re-checks whether the ticket currently
    /// at `head` is abandoned or dead-owned, when nothing has woken it
    /// in the meantime. See `tryHelpDeadTicketAt`'s own doc comment.
    const dead_ticket_check_step_ms: u32 = 50;

    /// Whether ticket `ticket`'s slot word `word` shows it resolved without
    /// its owner taking the turn: given up (pid cleared by `abandonTicket`)
    /// or owned by a process that has died. Only a word carrying this
    /// ticket's own tag counts; any other word belongs to a claim not yet
    /// published or to another ticket using the same ring index.
    fn ticketResolvable(self: *const Region, word: u128, ticket: u32) bool {
        if (word == 0 or ticketWordTag(word) != ticket +% 1) return false;
        return ticketWordPid(word) == 0 or self.isReferenceDead(ticketWordLivenessSlot(word), ticketWordGeneration(word));
    }

    /// Moves `head` off ticket `ticket` if it is still there, then frees
    /// the ticket's slot if it still holds `word`. The head transition is
    /// the single winner for that ticket; the tagged compare-and-swap on
    /// the slot cannot free a later ticket that reuses the ring index.
    fn advanceHeadFrom(self: *Region, ticket: u32, word: u128) bool {
        const hdr = self.header();
        if (hdr.writer_ticket_head.cmpxchgStrong(ticket, ticket +% 1, .acq_rel, .monotonic) != null) return false;
        _ = self.ticketSlots()[ticket % hdr.max_readers].cmpxchgStrong(word, 0, .acq_rel, .monotonic);
        return true;
    }

    /// Advances `head` past every resolved ticket at the front of the
    /// queue, stopping at the first live or unpublished one, then wakes
    /// the waiters. Any number of callers may run it concurrently: each
    /// step re-reads `head` and moves it only by compare-and-swap.
    fn skipResolvedHeads(self: *Region) void {
        if (self.advanceResolvedHeads()) countedWriterFutexWakeShared(&self.header().writer_ticket_head.raw);
    }

    /// `skipResolvedHeads` without the wake; true when this call moved
    /// `head`. A poller that finds a live head wakes nobody.
    fn advanceResolvedHeads(self: *Region) bool {
        const hdr = self.header();
        const slots = self.ticketSlots();
        var advanced = false;
        while (true) {
            const head = hdr.writer_ticket_head.load(.acquire);
            // Published tickets only: a claim is visible in its slot before
            // `tail` advances past it.
            if (head == @as(u32, @truncate(hdr.writer_ticket_tail.load(.acquire)))) break;
            const word = slots[head % hdr.max_readers].load(.acquire);
            if (!self.ticketResolvable(word, head)) break;
            if (self.advanceHeadFrom(head, word)) advanced = true;
        }
        return advanced;
    }

    /// Gives up ticket `ticket`, which has not had its turn. Its owner is
    /// alive, so it marks the slot itself; if the previous writer already
    /// handed the turn to it before the mark, skip it now rather than
    /// leave it at the head until a later waiter's check.
    fn abandonTicket(self: *Region, ticket: u32) void {
        const hdr = self.header();
        const slot = &self.ticketSlots()[ticket % hdr.max_readers];
        var word = slot.load(.acquire);
        while (word != 0 and ticketWordTag(word) == ticket +% 1 and ticketWordPid(word) != 0) {
            word = slot.cmpxchgWeak(word, word & ~@as(u128, 0xffff_ffff), .acq_rel, .acquire) orelse break;
        }
        self.skipResolvedHeads();
    }

    /// Ends ticket `ticket`'s turn: the owner won the writer word, or is
    /// giving the turn up (deadline or cancellation in phase 2).
    fn finishTurn(self: *Region, ticket: u32) void {
        const hdr = self.header();
        const word = self.ticketSlots()[ticket % hdr.max_readers].load(.acquire);
        const advanced = self.advanceHeadFrom(ticket, word);
        // Nobody else resolves a live owner's ticket at the head.
        std.debug.assert(advanced);
        _ = self.advanceResolvedHeads();
        // The handoff itself is progress: wake the next ticket.
        countedWriterFutexWakeShared(&self.header().writer_ticket_head.raw);
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
            countedWriterFutexWakeShared(&hdr.writer_release_epoch.raw);
        }
    }

    /// §8.4: `true` only when `writer_holder` is held (nonzero) *and* the
    /// liveness probe on the pid+slot packed into it proves that process
    /// is dead -- never true for a free word (nothing to recover) and
    /// never true for a live holder (the normal, overwhelmingly common
    /// case: a waiter should keep waiting on the futex, not spin probing
    /// `fcntl` on every iteration).
    /// True when a live process other than `own_pid` holds the writer
    /// lease, so it may have an append in flight right now.
    /// Whether another process holds the writer word, live or not: a dead
    /// holder may still have left an unsynced append behind.
    pub fn foreignWriterHeld(self: *const Region, own_pid: u32) bool {
        const word = self.header().writer_holder.load(.acquire);
        return word != 0 and holderPid(word) != own_pid;
    }

    pub fn foreignLiveWriter(self: *const Region, own_pid: u32) bool {
        const word = self.header().writer_holder.load(.acquire);
        if (word == 0 or holderPid(word) == own_pid) return false;
        return !self.isReferenceDead(holderLivenessSlot(word), holderClaimGeneration(word));
    }

    pub fn writerHolderDead(self: *const Region) bool {
        const word = self.header().writer_holder.load(.acquire);
        if (word == 0) return false;
        return self.isReferenceDead(holderLivenessSlot(word), holderClaimGeneration(word));
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
    pub fn recoverDeadWriter(self: *Region, recovery: DeadWriterRecovery) !void {
        if (!self.writerHolderDead()) return error.WriterHolderStillAlive;
        try recovery.run(recovery.ctx);
        const hdr = self.header();
        hdr.writer_holder.store(0, .release);
        if (hdr.writer_ticket_head.load(.monotonic) != @as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic)))) {
            countedWriterFutexWakeShared(&hdr.writer_ticket_head.raw);
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

    /// A ring of `max_readers` packed `(tag, owner)` words
    /// (`packTicketSlot`'s own encoding; `owner` is `packHolder`'s own
    /// `(pid, liveness_slot)` encoding), one per `ticket % max_readers`.
    /// `0` means genuinely empty -- nobody has ever claimed this
    /// generation of the slot -- which is why `tag` is stored as the
    /// claiming ticket's own number *plus one*: ticket 0 would otherwise
    /// be indistinguishable from empty.
    ///
    /// Claimed atomically with the ticket number itself
    /// (`tryClaimTicket`'s own doc comment has the full protocol) --
    /// deliberately not a plain `store` after `writer_ticket_tail`'s own
    /// `fetchAdd` the way this worked before v2 of this format: that left
    /// a window where a ticket existed (tail already advanced) but its
    /// owner wasn't recorded yet, and a crash in that exact window left
    /// the ring slot permanently unresolvable (read as "never claimed"
    /// rather than "claimed by a now-dead process", so no liveness slot
    /// was ever known to probe).
    ///
    /// Read for two purposes, both via `ticketSlotOwner`:
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
    ///
    /// Cleared back to `0` once a ticket resolves one way or another
    /// (`ticketResolvedWithoutOwning`'s own doc comment), freeing this
    /// exact ring index for a future generation to claim.
    fn ticketSlots(self: *const Region) []std.atomic.Value(u128) {
        const max_readers = self.header().max_readers;
        const base: [*]std.atomic.Value(u128) = @ptrCast(@alignCast(self.mapping.ptr + ticketOwnerOffset(max_readers)));
        return base[0..max_readers];
    }

    /// Non-blocking attempt at the two confirming steps of a ticket
    /// claim for the speculative tail value `t` this caller already
    /// read: §8.2's own fix for the ownerless-ticket window
    /// `ticketSlots`'s own doc comment describes.
    ///
    /// 1. CAS `ticketSlots()[t % max_readers]` from empty (`0`) to this
    ///    process's own claim (`packTicketSlot(t +% 1, ...)`). Losing
    ///    this means the slot is occupied -- either a live concurrent
    ///    claimant racing for this *exact* `t` (another process read the
    ///    same `writer_ticket_tail` value before either of us advanced
    ///    it) or a dead one that crashed between this step and the next.
    ///    Tells the two apart by tag: only a slot whose own tag matches
    ///    `t +% 1` is this exact ticket's own contested claim (anything
    ///    else is a different ticket number's own leftover, not this
    ///    call's concern); if the owner is dead, clears the slot so a
    ///    caller's own retry (from the top, with a fresh `t`) can win it
    ///    -- it does not retry the claim itself.
    /// 2. CAS `writer_ticket_tail` from `t` to `t +% 1`. Losing this
    ///    means some other process's own step 2 already advanced it
    ///    (this call only reaches here after *winning* step 1 for `t`,
    ///    so a concurrent claimant for this exact `t` must have lost
    ///    step 1 and cannot be the reason; this is instead a genuinely
    ///    different ticket's own tail advance racing ahead, meaning `t`
    ///    itself is now stale): undoes this call's own step 1 (stores
    ///    the slot back to empty) so a retry with a fresh `t` can use
    ///    this same ring index again once it's genuinely free.
    ///
    /// Returns the ticket number (always `t` itself, truncated to
    /// `u32` like every other ticket number in this file) on success,
    /// `null` on either failure above -- the caller should back off
    /// briefly (this is expected to resolve in at most a few retries:
    /// a live contender publishes within a couple of instructions, a
    /// dead one gets cleared on this same call) and retry the whole
    /// claim with a freshly read `t`.
    fn tryClaimTicket(self: *Region, t: u64, pid: u32) ?u32 {
        const hdr = self.header();
        const ticket: u32 = @truncate(t);
        const slot = &self.ticketSlots()[ticket % hdr.max_readers];
        // Owner and liveness generation publish in one word, so no reader
        // can pair this owner with a previous claimant's generation.
        const want = packTicketWord(ticket, pid, self.own_liveness_slot, self.own_liveness_generation);
        if (slot.cmpxchgStrong(0, want, .acq_rel, .acquire)) |existing| {
            const prior = ticketWordTag(existing) -% 1;
            const head = hdr.writer_ticket_head.load(.acquire);
            // A ticket already behind `head` whose resolver has not freed
            // (or died before freeing) its slot.
            const behind = head -% prior -% 1 < (@as(u32, 1) << 31);
            // A competing claim of this same ticket whose owner died before
            // publishing it. Once `tail` has moved past it the ticket is
            // published: only the head-advance path may retire it, so this
            // stale contender must leave it.
            // Death first: a dead owner can no longer publish, so `tail`
            // read afterwards cannot move past this ticket. Read the other
            // way round, the owner could publish and die in between.
            const dead_unpublished = prior == ticket and blk: {
                if (!self.isReferenceDead(ticketWordLivenessSlot(existing), ticketWordGeneration(existing))) break :blk false;
                if (comptime builtin.is_test) if (self.test_after_claim_death_probe) |hook| hook(self);
                break :blk hdr.writer_ticket_tail.load(.acquire) == t;
            };
            if (behind or dead_unpublished) _ = slot.cmpxchgStrong(existing, 0, .acq_rel, .monotonic);
            return null;
        }
        if (hdr.writer_ticket_tail.cmpxchgStrong(t, t +% 1, .acq_rel, .monotonic) != null) {
            _ = slot.cmpxchgStrong(want, 0, .acq_rel, .monotonic);
            return null;
        }
        return ticket;
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
        const mine = packHolderGen(pid, self.own_liveness_slot, self.own_liveness_generation);
        for (self.readerSlots(), 0..) |*slot, i| {
            if (casReaderSlotWord(slot, 0, mine)) {
                claimedReaderSlot(slot, pid);
                return @intCast(i);
            }
        }
        // Nothing free outright. Per §8.4, a dead reader's slot is as
        // good as free -- the liveness probe only runs here, in the
        // already-rare exhaustion path, not on the fast claim above.
        for (self.readerSlots(), 0..) |*slot, i| {
            const existing = readerSlotWord(slot);
            if (existing == 0 or !self.readerWordDead(existing)) continue;
            // Takeover is the one transition off this exact dead word: a
            // concurrent cleanup or takeover of the same word fails its own
            // compare-and-swap.
            if (casReaderSlotWord(slot, existing, mine)) {
                claimedReaderSlot(slot, pid);
                return @intCast(i);
            }
        }
        return ReaderError.ReadersExhausted;
    }

    /// The slot word is the ownership: `packHolderGen` of its claimant,
    /// zero when free, or `reader_slot_cleaning` plus the cleaner's own
    /// identity while a dead claimant's pins are cleared. Every transition
    /// is a compare-and-swap from the exact word observed, so cleanup and
    /// takeover of one dead claim cannot both win, and neither can touch a
    /// newer claim of the same slot.
    const reader_slot_cleaning: u128 = @as(u128, 1) << 127;

    fn readerSlotWord(slot: *const ReaderSlot) u128 {
        return @atomicLoad(u128, &slot.generation, .acquire);
    }

    fn casReaderSlotWord(slot: *ReaderSlot, expected: u128, new: u128) bool {
        return @cmpxchgStrong(u128, &slot.generation, expected, new, .acq_rel, .monotonic) == null;
    }

    fn claimedReaderSlot(slot: *ReaderSlot, pid: u32) void {
        slot.pinned_sequence.store(0, .monotonic);
        slot.metadata_pinned_sequence.store(0, .monotonic);
        slot.owner.store(pid, .release);
    }

    /// A claim (or an in-progress cleanup) whose process has died.
    fn readerWordDead(self: *const Region, word: u128) bool {
        return self.isReferenceDead(holderLivenessSlot(word), holderClaimGeneration(word));
    }

    /// A word that pins: claimed, not being cleaned, owner alive.
    fn readerWordLive(self: *const Region, word: u128) bool {
        return word != 0 and word & reader_slot_cleaning == 0 and !self.readerWordDead(word);
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
    /// Sequentially consistent, as the watermark read that follows it is:
    /// see `publishLogState`.
    pub fn pinReader(self: *Region, slot_index: u32, sequence: u64) void {
        self.readerSlots()[slot_index].pinned_sequence.store(sequence, .seq_cst);
    }

    /// Clears the pin recorded by `pinReader`, without releasing the slot
    /// itself -- the same `NativeFile`/pin lifetime may pin again.
    pub fn unpinReader(self: *Region, slot_index: u32) void {
        self.readerSlots()[slot_index].pinned_sequence.store(0, .release);
    }

    /// The narrower, metadata-only pin (`ReaderSlot.metadata_pinned_sequence`):
    /// set in addition to `pinReader`, never instead of it, by a reader
    /// that also retains allocator/recovery metadata pages (a full graph
    /// scan, snapshot, or indexed-reclamation reader opened with
    /// `metadata = true`). Matches `native.zig`'s
    /// `minimum_metadata_reader_sequence` frontier.
    pub fn pinMetadataReader(self: *Region, slot_index: u32, sequence: u64) void {
        self.readerSlots()[slot_index].metadata_pinned_sequence.store(sequence, .release);
    }

    /// Clears the pin recorded by `pinMetadataReader`, without releasing
    /// the slot or touching the data pin from `pinReader`.
    pub fn unpinMetadataReader(self: *Region, slot_index: u32) void {
        self.readerSlots()[slot_index].metadata_pinned_sequence.store(0, .release);
    }

    /// Frees a slot claimed by `claimReaderSlot` for reuse, and wakes
    /// anyone parked in `claimReaderSlotWait` on exhaustion. Callers must
    /// unpin first; this clears both pins defensively anyway so a slot
    /// released while still (incorrectly) pinned never blocks
    /// `minimumPinnedSequence`/`minimumMetadataPinnedSequence` after
    /// release.
    pub fn releaseReaderSlot(self: *Region, slot_index: u32) void {
        const slot = &self.readerSlots()[slot_index];
        slot.pinned_sequence.store(0, .monotonic);
        slot.metadata_pinned_sequence.store(0, .monotonic);
        slot.owner.store(0, .release);
        @atomicStore(u128, &slot.generation, 0, .release);
        const hdr = self.header();
        _ = hdr.reader_slot_released.fetchAdd(1, .release);
        futexWakeShared(&hdr.reader_slot_released.raw);
    }

    /// Whether `slot_index` (a `claimReaderSlot` return value, not a
    /// liveness slot) currently has a live owner: `false` for a free slot
    /// and for one whose recorded holder the byte-range liveness probe
    /// proves dead (the same check `minimumPinnedSequence`'s own sweep
    /// uses internally, exposed here for a caller -- e.g. a reader-side
    /// migration step -- that needs to ask about one specific slot
    /// without running a full frontier scan).
    pub fn isReaderSlotLive(self: *const Region, slot_index: u32) bool {
        return self.readerWordLive(readerSlotWord(&self.readerSlots()[slot_index]));
    }

    /// §8.3's writer-side reuse-frontier scan, replacing
    /// `scanReaderEpochsCached`'s namespace-directory walk: the minimum
    /// `pinned_sequence` over every slot with a live owner, or `null` if
    /// no reader is currently pinned. "Live" here is just `owner != 0`;
    /// step 3's per-slot liveness byte-range probe (§8.4) replaces that
    /// with a real death check so a crashed reader's never-cleared pin
    /// cannot block reclamation forever. This is `native.zig`'s broader
    /// frontier (`reuseFrontier`'s `minimum_reader_sequence` input,
    /// protecting every pinned reader regardless of whether it also
    /// retains metadata); see `minimumMetadataPinnedSequence` for the
    /// narrower one. Name and semantics are relied on by more than one
    /// caller (page reclamation here, log-record retention elsewhere) --
    /// keep both stable.
    /// `minimumPinnedSequence`, counting only readers in other processes.
    /// The oldest pin the redo fold must respect before resetting the log.
    /// A reader in another process catches its overlay up from the log's
    /// bytes, so its pin always counts. A reader in this process resolves
    /// through the shared in-memory overlay instead, and the overlay keeps
    /// every version visible at or above its own oldest pin
    /// (`overlay_floor`); such a reader's slot is skipped only when that
    /// floor covers it. A same-process reader that pinned only its slot
    /// (a frozen read-only handle, say) still counts. Page reuse still uses
    /// `minimumPinnedSequence`: every reader reads pages.
    pub fn minimumUncoveredPinnedSequence(self: *Region, own_pid: u32, overlay_floor: ?u64) ?u64 {
        var min: ?u64 = null;
        for (self.readerSlots()) |*slot| {
            const word = readerSlotWord(slot);
            if (!self.readerWordLive(word)) continue;
            const seq = slot.pinned_sequence.load(.seq_cst);
            if (seq == 0) continue;
            if (holderPid(word) == own_pid) if (overlay_floor) |floor| if (floor <= seq) continue;
            if (min == null or seq < min.?) min = seq;
        }
        return min;
    }

    pub fn minimumPinnedSequence(self: *Region) ?u64 {
        var min: ?u64 = null;
        for (self.readerSlots()) |*slot| {
            // Re-read the slot when a cleanup loses its compare-and-swap: a
            // takeover won, and its new pin belongs in this frontier.
            while (true) {
                const word = readerSlotWord(slot);
                if (word == 0 or word & reader_slot_cleaning != 0) break;
                // §8.4: a dead reader's never-cleared pin cannot block
                // reclamation forever. Probing costs a syscall per occupied
                // slot, same as the writer side's `writerHolderDead` -- paid
                // only here, in the writer's own reuse-frontier scan, never on
                // a reader's own pin/unpin path.
                if (self.readerWordDead(word)) {
                    if (comptime builtin.is_test) if (self.test_after_reader_dead_probe) |hook| hook(self);
                    // Own the dead claim first, so a takeover cannot slip in
                    // between clearing its pins and freeing the slot.
                    const cleaning = reader_slot_cleaning | packHolderGen(antfly_platform.process.currentId() orelse 0, self.own_liveness_slot, self.own_liveness_generation);
                    if (!casReaderSlotWord(slot, word, cleaning)) continue;
                    slot.pinned_sequence.store(0, .monotonic);
                    slot.metadata_pinned_sequence.store(0, .monotonic);
                    slot.owner.store(0, .release);
                    @atomicStore(u128, &slot.generation, 0, .release);
                    const hdr = self.header();
                    _ = hdr.reader_slot_released.fetchAdd(1, .release);
                    futexWakeShared(&hdr.reader_slot_released.raw);
                    break;
                }
                const seq = slot.pinned_sequence.load(.seq_cst);
                if (seq != 0 and (min == null or seq < min.?)) min = seq;
                break;
            }
        }
        return min;
    }

    /// The narrower companion to `minimumPinnedSequence`: the minimum
    /// `metadata_pinned_sequence` over every slot with a live owner *and*
    /// a nonzero metadata pin, or `null` if no reader currently retains
    /// metadata. Matches `native.zig`'s `metadataReuseFrontier`'s
    /// `minimum_metadata_reader_sequence` input. Dead-slot reclamation is
    /// `minimumPinnedSequence`'s job (it runs on every writer-side
    /// frontier check and this one is always called alongside it in
    /// practice); this scan only reads, so calling this alone without
    /// also calling `minimumPinnedSequence` leaves a dead reader's
    /// metadata pin uncollected until the next data-frontier scan reaps
    /// it.
    pub fn minimumMetadataPinnedSequence(self: *Region) ?u64 {
        var min: ?u64 = null;
        for (self.readerSlots()) |*slot| {
            if (!self.readerWordLive(readerSlotWord(slot))) continue;
            const seq = slot.metadata_pinned_sequence.load(.acquire);
            if (seq == 0) continue;
            if (min == null or seq < min.?) min = seq;
        }
        return min;
    }

    /// §9.3b's cross-process WAL index: `publishLogState`'s own consistent
    /// snapshot of `(log_checkpoint_sequence, log_durable_sequence,
    /// log_durable_offset)`.
    pub const LogState = struct {
        checkpoint_sequence: u64,
        durable_sequence: u64,
        durable_offset: u64,
    };

    /// Publishes the writer's current log state as one consistent triple,
    /// via a seqlock on `log_generation` -- not three independent atomic
    /// stores a reader could observe torn across. Called by the writer
    /// right after a durable log append (or after a real checkpoint lands
    /// and/or reclaims log space), never on any other path: no new
    /// syscalls, just atomic stores into the already-mapped region,
    /// exactly like every other steady-state write this file makes.
    ///
    /// Usually called under writer authority (the path-writer lease); a
    /// cold opener that found no publication at all also publishes. A
    /// tuple older than the published one (a lower durable or checkpoint
    /// sequence) is dropped: the watermark never moves backwards.
    pub fn publishLogState(self: *Region, state: LogState) void {
        const hdr = self.header();
        // Sequentially consistent throughout: the generation brackets the
        // payload in one total order, and a fold's publish followed by its
        // pin scan (`minimumUncoveredPinnedSequence`) cannot both miss a
        // reader's pin followed by its watermark read (`pinReader`).
        //
        // The even-to-odd CAS makes publishers mutually exclusive, so two
        // publishers (a writer and a recovering opener) can never
        // interleave their stores under one generation. An odd generation
        // that never moves belongs to a publisher that died mid-publish;
        // this one takes it over and repairs it with a complete tuple.
        var stuck: u32 = 0;
        var last_odd: u32 = 0;
        const publishing: u32 = while (true) {
            const generation = hdr.log_generation.load(.seq_cst);
            if (generation & 1 != 0) {
                if (generation != last_odd) {
                    last_odd = generation;
                    stuck = 0;
                } else {
                    stuck += 1;
                    if (stuck >= log_state_stuck_spins) break generation;
                }
                std.atomic.spinLoopHint();
                continue;
            }
            if (state.durable_sequence < hdr.log_durable_sequence.load(.seq_cst) or
                state.checkpoint_sequence < hdr.log_checkpoint_sequence.load(.seq_cst)) return;
            if (hdr.log_generation.cmpxchgWeak(generation, generation | 1, .seq_cst, .seq_cst) == null) break generation | 1;
        };
        hdr.log_checkpoint_sequence.store(state.checkpoint_sequence, .seq_cst);
        hdr.log_durable_sequence.store(state.durable_sequence, .seq_cst);
        hdr.log_durable_offset.store(state.durable_offset, .seq_cst);
        hdr.log_generation.store(publishing +% 1, .seq_cst);
    }

    /// Test support: a region whose log publication was never made (one
    /// reinitialized after every process exited).
    pub fn resetLogStateForTest(self: *Region) void {
        const hdr = self.header();
        const publishing = hdr.log_generation.load(.acquire) | 1;
        hdr.log_generation.store(publishing, .release);
        hdr.log_checkpoint_sequence.store(0, .monotonic);
        hdr.log_durable_sequence.store(0, .monotonic);
        hdr.log_durable_offset.store(0, .monotonic);
        // A reinitialized region's generation is zero: never published.
        hdr.log_generation.store(0, .release);
    }

    /// Whether any publication has ever been made to this region. A brand
    /// new log publishes an all-zero tuple, which this tells apart from a
    /// region nothing has published to yet.
    pub fn logStatePublished(self: *const Region) bool {
        return self.header().log_generation.load(.seq_cst) != 0;
    }

    /// Test support: a publisher that died with the generation odd.
    pub fn tearLogStateForTest(self: *Region) void {
        _ = self.header().log_generation.fetchOr(1, .release);
    }

    /// Spins `readLogState` tolerates on an odd generation that never
    /// moves: a publisher that died mid-publish.
    const log_state_stuck_spins: u32 = 1 << 16;

    /// Reads the triple `publishLogState` last published, retrying (no
    /// syscall, no blocking -- a plain spin) whenever the seqlock proves
    /// the read straddled a concurrent publish. The window a publish holds
    /// the generation odd is three plain stores long, so this never spins
    /// more than a handful of times even under real contention; VOPR-safe
    /// by construction (no `std.Io`, no real time, nothing but atomic
    /// loads).
    ///
    /// A generation stuck odd (its publisher died) reads as no publication
    /// at all, the same stale-but-safe state as a freshly initialized
    /// region, rather than spinning forever; the next lease holder's
    /// publish repairs it.
    pub fn readLogState(self: *const Region) LogState {
        return self.readLogStateChecked() orelse .{ .checkpoint_sequence = 0, .durable_sequence = 0, .durable_offset = 0 };
    }

    /// The published fields read one at a time, outside the seqlock: no
    /// consistent tuple, but usable as a bound while a publication is torn
    /// or in progress. Each field then holds either its previous or its new
    /// value, and every durable sequence ever published was durable when it
    /// was published.
    pub fn readLogStateRaw(self: *const Region) LogState {
        const hdr = self.header();
        return .{
            .checkpoint_sequence = hdr.log_checkpoint_sequence.load(.seq_cst),
            .durable_sequence = hdr.log_durable_sequence.load(.seq_cst),
            .durable_offset = hdr.log_durable_offset.load(.seq_cst),
        };
    }

    /// `readLogState`, or null when the publication is torn (a publisher
    /// died mid-publish): a writer must then treat peer progress as unknown
    /// rather than as none.
    pub fn readLogStateChecked(self: *const Region) ?LogState {
        const hdr = self.header();
        var stuck: u32 = 0;
        var last_odd: u32 = 0;
        while (true) {
            const before = hdr.log_generation.load(.seq_cst);
            if (before & 1 != 0) {
                if (before != last_odd) {
                    last_odd = before;
                    stuck = 0;
                } else {
                    stuck += 1;
                    if (stuck >= log_state_stuck_spins) return null;
                }
                std.atomic.spinLoopHint();
                continue;
            }
            const state: LogState = .{
                .checkpoint_sequence = hdr.log_checkpoint_sequence.load(.seq_cst),
                .durable_sequence = hdr.log_durable_sequence.load(.seq_cst),
                .durable_offset = hdr.log_durable_offset.load(.seq_cst),
            };
            const after = hdr.log_generation.load(.seq_cst);
            if (before == after) return state;
        }
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
        // The write above only proves the child is about to *call*
        // `acquireWriter`, not that it has actually taken ticket
        // `my_index` yet -- under a loaded host, this process can win
        // this `readBounded` and go on to fork the next child well
        // before the one that just signaled is next scheduled to run
        // its own `fetchAdd`, letting a *later* fork take an *earlier*
        // ticket number and breaking the very "fork order is ticket
        // order" assumption this test relies on (observed directly:
        // ticket 2 reporting before ticket 1). Wait on the region's own
        // shared `writer_ticket_tail` -- visible to this process through
        // the same mapping, like the dead-queued-waiter test's own
        // analogous wait -- until this child has genuinely taken ticket
        // `my_index`, not merely signaled that it is about to try.
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_tail.load(.monotonic) <= @as(u64, my_index)) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 5000) return error.TestTimeout;
        }
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
        // Generous (10s, not the usual 2s used elsewhere in this file):
        // this loop waits out up to `n_children` sequential handoffs, and
        // each one's own scheduling delay (not the lock itself) can run
        // long on a heavily loaded host -- a tight bound here produces a
        // false timeout from scheduling noise, not a real correctness
        // failure, exactly as observed under a 200-run parallel loop.
        try readBounded(order_fds[0], &report, 10000);
        try std.testing.expectEqual(got, report[0]);
        try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pids[got], 10000));
    }
}

// §8.8's own CPU gate calls out a number `lite-write-amplification-cpu`
// cannot report by itself: not "zero per warm write" (that harness's own
// job, and already zero -- see `write_amplification_cpu_bench_test.zig`),
// but the real syscall price of a *contended* handoff, the case the region
// design exists to make cheap and bounded instead of a 1 s fairness
// quantum or a ticket-file `pread` poll. Two real OS processes (the same
// `std.c.fork` pattern as the two tests above, and for the same reason:
// the child uses no `Io` at all) repeatedly trade the writer word in a
// tight loop with no artificial pacing -- the FIFO ticket protocol itself
// (`acquireWriter`'s own doc comment: a new arrival may only take the
// ticketless fast path when nobody is already queued) forces them to
// alternate through the real ticket/futex path rather than one side
// starving the other, so a tight loop is already a genuinely contended
// workload, not a race that happens to usually avoid contention.
//
// Counters reset in each child right after `fork`, not in the parent
// before it: `writer_futex_counters` is ordinary process memory, so each
// child's copy already starts at whatever the parent had already done
// against this same `region` value (its own `tryAcquireWriter` priming
// below, same as the two tests above); resetting post-fork is what makes
// each child's own report cover only its own loop.
test "shared lock region: contended handoff reports lock syscalls and latency" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-handoff-report.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const handoffs_per_child = 40;
    const HandoffReport = extern struct {
        completed: u64,
        futex_wait: u64,
        futex_wake: u64,
        // Per-iteration release-to-acquire latency samples (see
        // `last_release_ns` below for what each measures), `0` for a
        // sample with no prior release to measure against (this child's
        // own very first, uncontended iteration, before anyone has ever
        // released). The parent filters those zeros out rather than
        // trusting `valid_count` alone, so a real (non-first) sample that
        // happens to land exactly on `0` -- impossible in practice at
        // nanosecond resolution, but not a assumption worth relying on --
        // still can't corrupt the statistic.
        latencies_ns: [handoffs_per_child]u64,
    };

    // A real cross-process shared value for "when did the writer word
    // last actually get released", independent of (and *not* reusing, to
    // avoid widening it) the real `Header` this module maps -- the whole
    // point of §8.8's own ask here is release-to-acquire, not
    // request-to-acquire (the `start_ts`-based measurement the previous
    // version of this test took, which folds in this test's own
    // artificial per-iteration hold and so measures mostly that hold, not
    // the handoff). An anonymous `MAP_SHARED` mapping, not a `Region`
    // field: both children inherit the same physical page across `fork`
    // exactly like a file-backed one would, with none of the overhead or
    // version/format concerns of extending the real header for a number
    // only this test ever reads.
    const latency_scratch = try std.posix.mmap(null, std.heap.pageSize(), .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED, .ANONYMOUS = true }, -1, 0);
    defer std.posix.munmap(latency_scratch);
    const last_release_ns: *std.atomic.Value(i64) = @ptrCast(@alignCast(latency_scratch.ptr));
    last_release_ns.* = .init(0);

    var report_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&report_fds));

    const n_children = 2;
    var child_pids: [n_children]std.c.pid_t = undefined;
    var ci: u8 = 0;
    while (ci < n_children) : (ci += 1) {
        const child_pid = std.c.fork();
        if (child_pid == 0) {
            _ = std.posix.system.close(report_fds[0]);
            testResetWriterFutexCounters();
            const my_pid: u32 = @bitCast(std.c.getpid());
            var latencies_ns: [handoffs_per_child]u64 = @splat(0);
            var h: u32 = 0;
            while (h < handoffs_per_child) : (h += 1) {
                region.acquireWriter(my_pid);
                var acquired_ts: std.c.timespec = undefined;
                _ = std.c.clock_gettime(.MONOTONIC, &acquired_ts);
                const acquired_ns = @as(i64, acquired_ts.sec) * std.time.ns_per_s + acquired_ts.nsec;
                // Release-to-acquire: the gap between *this* holder's own
                // `releaseWriter` and the moment `acquireWriter` returned
                // here, not this call's own total wait (which would also
                // include the other side's artificial hold below).
                const release_ns = last_release_ns.load(.acquire);
                if (release_ns != 0 and acquired_ns > release_ns) {
                    latencies_ns[h] = @intCast(acquired_ns - release_ns);
                }
                // Hold briefly, like real commit work (raw `nanosleep`,
                // not `io.sleep`: no `Io` in a forked child, same reason
                // as the two-process test above) -- without this, both
                // children's own loop bodies are fast enough that they
                // simply never overlap, and the "contended" handoff this
                // test exists to measure never actually happens.
                var hold: std.c.timespec = .{ .sec = 0, .nsec = 300_000 };
                _ = std.c.nanosleep(&hold, null);
                var release_ts: std.c.timespec = undefined;
                _ = std.c.clock_gettime(.MONOTONIC, &release_ts);
                last_release_ns.store(@as(i64, release_ts.sec) * std.time.ns_per_s + release_ts.nsec, .release);
                region.releaseWriter();
            }
            const counters = testWriterFutexCounters();
            const report: HandoffReport = .{
                .completed = handoffs_per_child,
                .futex_wait = counters.wait,
                .futex_wake = counters.wake,
                .latencies_ns = latencies_ns,
            };
            _ = std.c.write(report_fds[1], @ptrCast(&report), @sizeOf(HandoffReport));
            std.c._exit(0);
        }
        child_pids[ci] = child_pid;
    }
    _ = std.posix.system.close(report_fds[1]);
    defer _ = std.posix.system.close(report_fds[0]);

    var total_completed: u64 = 0;
    var total_wait: u64 = 0;
    var total_wake: u64 = 0;
    // Every child's own samples, combined: percentiles over the pooled
    // set, not an average of two separately-computed percentiles (not
    // the same statistic, and would hide one child getting starved more
    // than the other). Filtered down from the fixed-size wire format
    // above (dropping each child's own first, unmeasurable sample).
    var all_latencies_ns: [n_children * handoffs_per_child]u64 = undefined;
    var valid_count: usize = 0;
    var got: u8 = 0;
    while (got < n_children) : (got += 1) {
        var report: HandoffReport = undefined;
        // Generous (5s): two children doing 40 uncontended-sized handoffs
        // each is normally well under a second even on a loaded host; this
        // bound exists only to fail fast, not to constrain the real cost
        // being measured (the latencies themselves, reported below).
        try readBounded(report_fds[0], std.mem.asBytes(&report), 5000);
        try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pids[got], 5000));
        total_completed += report.completed;
        total_wait += report.futex_wait;
        total_wake += report.futex_wake;
        for (report.latencies_ns) |ns| {
            if (ns == 0) continue;
            all_latencies_ns[valid_count] = ns;
            valid_count += 1;
        }
    }
    const samples = all_latencies_ns[0..valid_count];

    try std.testing.expect(total_completed > 0);
    try std.testing.expect(valid_count > 0);
    const wait_per_handoff = @as(f64, @floatFromInt(total_wait)) / @as(f64, @floatFromInt(total_completed));
    const wake_per_handoff = @as(f64, @floatFromInt(total_wake)) / @as(f64, @floatFromInt(total_completed));
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    const median_ns = samples[samples.len / 2];
    const p99_index = @min(samples.len - 1, (samples.len * 99) / 100);
    const p99_ns = samples[p99_index];
    // §8.8's own report: the writer word takes no `fcntl` lock at all on
    // this path (liveness is only ever probed for a *dead* holder, never
    // on the happy path a plain handoff takes) and no `flock` either (the
    // sidecar flock is a separate, still-authoritative mechanism this
    // migration step does not touch) -- both genuinely zero, not merely
    // unmeasured, so printed as constants rather than counted.
    std.debug.print(
        "shared lock region contended handoff report: handoffs={d} futex_wait/handoff={d:.2} futex_wake/handoff={d:.2} fcntl/handoff=0 flock/handoff=0 release-to-acquire latency_us(median)={d:.1} latency_us(p99)={d:.1}\n",
        .{ total_completed, wait_per_handoff, wake_per_handoff, @as(f64, @floatFromInt(median_ns)) / 1000.0, @as(f64, @floatFromInt(p99_ns)) / 1000.0 },
    );

    // A correct implementation's own handoff is a small, bounded number of
    // futex calls, not a retry storm, and the release-to-acquire latency
    // this design exists to bound: about one commit's own hold time, not
    // a fairness quantum or a lost wakeup (mirrors the two-process test
    // above's 200ms bound, loosened here for a loaded host running many
    // handoffs back to back).
    try std.testing.expect(wait_per_handoff < 10);
    try std.testing.expect(wake_per_handoff < 10);
    try std.testing.expect(p99_ns < 500 * std.time.ns_per_ms);
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

test "shared lock region: metadata pin is independent of the data pin and isReaderSlotLive reflects ownership" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-readers-metadata.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.openSized(a, io, db_path, 4);
    defer region.close();

    try std.testing.expectEqual(@as(?u64, null), region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, null), region.minimumMetadataPinnedSequence());

    // A plain reader: pins the data frontier only.
    const plain = try region.claimReaderSlot(100);
    try std.testing.expect(region.isReaderSlotLive(plain));
    region.pinReader(plain, 40);
    try std.testing.expectEqual(@as(?u64, 40), region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, null), region.minimumMetadataPinnedSequence());

    // A full/metadata reader: pins both, at a different (older) sequence.
    const full = try region.claimReaderSlot(200);
    region.pinReader(full, 25);
    region.pinMetadataReader(full, 25);
    try std.testing.expectEqual(@as(?u64, 25), region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, 25), region.minimumMetadataPinnedSequence());

    // Unpinning the metadata-only side leaves the data pin (and the
    // plain reader's own pin) untouched.
    region.unpinMetadataReader(full);
    try std.testing.expectEqual(@as(?u64, 25), region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, null), region.minimumMetadataPinnedSequence());

    region.releaseReaderSlot(full);
    try std.testing.expect(!region.isReaderSlotLive(full));
    try std.testing.expectEqual(@as(?u64, 40), region.minimumPinnedSequence());

    region.releaseReaderSlot(plain);
    try std.testing.expect(!region.isReaderSlotLive(plain));
    try std.testing.expectEqual(@as(?u64, null), region.minimumPinnedSequence());
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

test "shared lock region: opening a second same-process handle does not drop the first's liveness lock" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-second-open.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    const region_path = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{db_path});
    defer a.free(region_path);

    // Regression: `openSized` used to set up the file through a second fd
    // and close it, and closing any fd drops every `fcntl` lock this
    // process holds on the file -- `first`'s liveness slot included. A peer
    // then saw `first`'s writer as dead and recovered it while it was live.
    var first = try Region.open(a, io, db_path);
    defer first.close();
    var second = try Region.open(a, io, db_path);
    defer second.close();

    var pipe_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&pipe_fds));
    const first_slot = first.own_liveness_slot;
    const max_readers = first.header().max_readers;

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(pipe_fds[0]);
        const raw_fd = std.posix.openat(std.posix.AT.FDCWD, region_path, .{ .ACCMODE = .RDWR }, 0) catch std.c._exit(1);
        var fl: std.c.Flock = .{
            .start = Region.livenessByteOffset(max_readers, first_slot),
            .len = 1,
            .pid = 0,
            .type = std.c.F.WRLCK,
            .whence = std.c.SEEK.SET,
        };
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
// process is relying on. This test's "live peer" is a genuinely separate
// OS process; "shared lock region refuses a corrupt header instead of
// rebuilding under a live in-process peer" below covers the other live-
// peer shape -- a second `Region` handle in *this* process -- which
// needs its own, differently-synchronized test rather than a variant of
// this one: `fcntl` byte-range locks are scoped per *process*, not per
// fd, so a raw `fcntlTryLock` probe of a slot this same process already
// holds (via that other handle) trivially "succeeds", which is a
// different failure shape than a real foreign process actually holding
// the lock (this test's own case).
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

// The inode cross-check's own "no live peer" half, mirroring the
// checksum-corruption test right above it: a sidecar whose recorded
// `db_inode` no longer matches the data file actually at this path (the
// data file was deleted and replaced) is treated exactly like a
// corrupt header -- rebuilt once no live peer holds it.
test "shared lock region rebuilds a sidecar whose db_inode no longer matches the data file" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-inode-check.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    {
        const f = try Io.Dir.cwd().createFile(io, db_path, .{});
        f.close(io);
    }
    const old_inode = (try Io.Dir.cwd().statFile(io, db_path, .{})).inode;
    {
        var region = try Region.open(a, io, db_path);
        try std.testing.expectEqual(old_inode, region.header().db_inode);
        region.close();
    }

    // Replace the data file at the same path with a new one: a new
    // inode, the same sidecar name, no live peer holding the old
    // sidecar (it was just closed above).
    try Io.Dir.cwd().deleteFile(io, db_path);
    {
        const f = try Io.Dir.cwd().createFile(io, db_path, .{});
        f.close(io);
    }
    const new_inode = (try Io.Dir.cwd().statFile(io, db_path, .{})).inode;
    try std.testing.expect(new_inode != old_inode);

    var rebuilt = try Region.open(a, io, db_path);
    defer rebuilt.close();
    try std.testing.expectEqual(new_inode, rebuilt.header().db_inode);
    // A freshly rebuilt region is uncontended and acquirable, same
    // observable proof the checksum-corruption case above uses.
    try std.testing.expect(rebuilt.tryAcquireWriter(99));
    rebuilt.releaseWriter();
}

// The inode cross-check's own "live peer" half: a db_inode mismatch with
// a real separate process still holding its liveness slot on the
// current sidecar must refuse to rebuild, not silently wipe state that
// process is relying on. "shared lock region refuses a db_inode mismatch
// instead of rebuilding under a live in-process peer" below covers the
// other live-peer shape this same check must also refuse under -- a
// second `Region` handle in *this* process.
test "shared lock region refuses a db_inode mismatch instead of rebuilding under a live peer" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-inode-check-live.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    {
        const f = try Io.Dir.cwd().createFile(io, db_path, .{});
        f.close(io);
    }

    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        // The genuinely separate "live peer" process: opens the region
        // against the data file as it exists right now (claiming its
        // own liveness slot) and parks, never releasing it.
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

    // Replace the data file at the same path, exactly as the "no live
    // peer" case does, but this time with a real separate process still
    // holding its liveness slot on the sidecar the old data file's
    // inode was recorded into.
    try Io.Dir.cwd().deleteFile(io, db_path);
    {
        const f = try Io.Dir.cwd().createFile(io, db_path, .{});
        f.close(io);
    }

    try std.testing.expectError(RegionError.RegionCorrupt, Region.open(a, io, db_path));

    _ = std.c.kill(child_pid, std.c.SIG.KILL);
    _ = try reapBounded(child_pid, 2000);
}

// Regression (bus error): `ensureNoLivePeer` used a raw `fcntlTryLock`
// then `fcntlUnlock` probe on every liveness byte with no check for
// whether *this process* already held it through some other, still-live
// `Region` handle sharing the same fd (`FdEntry`, keyed by the sidecar's
// own inode in `acquireProcessFd`'s registry). `fcntl` byte-range locks
// are scoped per process, not per handle or claim, so re-locking a byte
// range this process already holds trivially "succeeds" -- the probe
// wrongly concluded the holder was dead, the matching `fcntlUnlock`
// released that other handle's *real* liveness lock, and `openSized`
// then truncated and rebuilt the sidecar (`setLength(0)` then back up)
// while that handle's `mmap` mapping was still live on it. The next
// ordinary, correct `close()` of that first handle touched its now-
// invalidated mapping and crashed with a bus error.
//
// This is exactly the shape `Store.performVacuum` hits in production:
// its own full-read snapshot ("source") opens a `Region` against the
// pre-vacuum generation and keeps it open across the whole vacuum, while
// `NativeFile.replaceWithPreparedGeneration`'s `inode_lock.rebind` lazily
// reopens a region for the newly-live (just-renamed) generation against
// the very same sidecar path -- whose on-disk header still names the
// *previous* generation's `db_inode`. Reproduced via
// `storage.lite.docstore`'s "lite reclamation automatic worker bounds
// sustained overwrite history": a sustained foreground writer plus the
// automatic maintenance worker's own vacuum cycles, intermittently (the
// bug needs the worker's `ensureRegion` reopen to land while the
// snapshot reader's slot is still live), hence this deterministic,
// single-process reproduction instead of relying on that timing.
//
// Unlike the two fork-based tests above, the live peer here is a second
// `Region` handle in *this* process (`io.concurrent`, this file's own
// convention for a real OS thread backed by `std.Io` -- never raw
// `std.Thread`), not a separate OS process: the bug is specifically
// about same-process, same-fd self-conflict, which a forked child can
// never exercise (a different process's `fcntl` locks always correctly
// conflict with this one's).
test "shared lock region refuses a db_inode mismatch instead of rebuilding under a live in-process peer" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-inode-check-inprocess.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    {
        const f = try Io.Dir.cwd().createFile(io, db_path, .{});
        f.close(io);
    }

    const Peer = struct {
        db_path: []const u8,
        ready: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        // The genuinely separate "live peer" *handle*: opens the region
        // against the data file as it exists right now (claiming its own
        // liveness slot and the writer word, mirroring the fork-based
        // test's own peer shape) and parks -- on its own `io.concurrent`
        // task, not the test's main flow of control -- until told to let
        // go.
        fn run(self: *@This(), peer_io: Io) !void {
            var region = try Region.open(std.testing.allocator, peer_io, self.db_path);
            _ = region.tryAcquireWriter(1);
            self.ready.store(true, .release);
            while (!self.release.load(.acquire)) peer_io.sleep(.fromMilliseconds(1), .awake) catch {};
            region.releaseWriter();
            region.close();
        }
    };
    var peer: Peer = .{ .db_path = db_path };
    var task = try io.concurrent(Peer.run, .{ &peer, io });
    defer {
        peer.release.store(true, .release);
        task.await(io) catch {};
    }

    const ready_deadline = Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (!peer.ready.load(.acquire)) {
        if (Io.Clock.awake.now(io).nanoseconds > ready_deadline.nanoseconds) return error.Timeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }

    // Replace the data file at the same path, exactly as the "no live
    // peer" and fork-based cases do, but this time with a real, live
    // `Region` handle in *this* process (not a separate OS process)
    // still holding its liveness slot on the sidecar the old data file's
    // inode was recorded into.
    try Io.Dir.cwd().deleteFile(io, db_path);
    {
        const f = try Io.Dir.cwd().createFile(io, db_path, .{});
        f.close(io);
    }

    // Before the fix, this call wrongly succeeded -- rebuilding the
    // sidecar under the peer's still-live mapping -- instead of refusing.
    try std.testing.expectError(RegionError.RegionCorrupt, Region.open(a, io, db_path));
}

// A symlink alias and its target must converge on the exact same
// sidecar: two different spellings of the same data file opening what
// each believes is its own exclusive writer ticket would otherwise
// silently break mutual exclusion (and, by the same mechanism, reader
// pins) between them. Checked both within this process (two `Region`
// handles sharing one mmap through `canonicalPathAlloc` resolving both
// spellings to the same string) and across a fork (a genuinely separate
// process, independently canonicalizing its own spelling).
test "shared lock region: a symlink alias and its target share one region, same process and across a fork" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const dir_path = buf[0..dir_len];

    const target_path = try std.fmt.allocPrint(a, "{s}/region-alias-target.aflite", .{dir_path});
    defer a.free(target_path);
    const alias_path = try std.fmt.allocPrint(a, "{s}/region-alias-link.aflite", .{dir_path});
    defer a.free(alias_path);
    {
        const f = try Io.Dir.cwd().createFile(io, target_path, .{});
        f.close(io);
    }
    try Io.Dir.cwd().symLink(io, target_path, alias_path, .{});

    const own_pid: u32 = @bitCast(std.c.getpid());

    // The writer ticket is taken through the *alias* spelling.
    var via_alias = try Region.open(a, io, alias_path);
    defer via_alias.close();
    try std.testing.expect(via_alias.tryAcquireWriter(own_pid));

    // Same process, the *target* spelling: must see it as already held,
    // not open an independent region of its own that would freely
    // acquire.
    var via_target = try Region.open(a, io, target_path);
    defer via_target.close();
    try std.testing.expect(!via_target.tryAcquireWriter(own_pid));

    // A genuinely different process, forked after the parent already
    // holds the ticket: must see the same thing through the target
    // spelling too -- not two independent regions that each think they
    // hold the only exclusive hold.
    var pipe_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&pipe_fds));
    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(pipe_fds[0]);
        var child_region = Region.open(std.heap.page_allocator, io, target_path) catch std.c._exit(1);
        const child_pid_val: u32 = @bitCast(std.c.getpid());
        const blocked = !child_region.tryAcquireWriter(child_pid_val);
        var report: [1]u8 = .{if (blocked) 1 else 0};
        _ = std.c.write(pipe_fds[1], @ptrCast(&report), 1);
        std.c._exit(0);
    }
    _ = std.posix.system.close(pipe_fds[1]);
    defer _ = std.posix.system.close(pipe_fds[0]);

    var report: [1]u8 = undefined;
    try readBounded(pipe_fds[0], &report, 2000);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pid, 2000));
    try std.testing.expectEqual(@as(u8, 1), report[0]);

    via_alias.releaseWriter();
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
        fn run(_: ?*anyopaque) anyerror!void {}
    };
    try region.recoverDeadWriter(.{ .run = &recovery.run });
    try std.testing.expect(!region.writerHolderDead()); // word is clear now, nothing to probe

    // The lock is usable again after recovery.
    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriter(own_pid));
    region.releaseWriter();
}

// v5's own closed race (`format_version`'s comment has the full story):
// `claimLivenessSlot`'s slot *index* alone was never a stable identity --
// once a holder dies, a different process can claim that exact index, and
// the old `isSlotDead` alone would then see *that new claimant's* own
// liveness lock and (wrongly) call the original, dead holder "alive"
// forever. `max_readers = 1` forces the collision deterministically
// (there is only one index to land on) instead of relying on the
// hash-seeded scan's own `1/max_readers` odds -- the whole point is that
// this must be detected *every* time, not merely often.
test "shared lock region: a dead writer's reused liveness slot is still detected dead" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-reused-slot.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));

    // The parent deliberately never opens the region before forking: the
    // child is the one that creates it (with exactly one liveness slot)
    // and claims that one and only slot for itself.
    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        var region = Region.openSized(std.heap.page_allocator, io, db_path, 1) catch std.c._exit(1);
        const my_pid: u32 = @bitCast(std.c.getpid());
        if (!region.tryAcquireWriter(my_pid)) std.c._exit(1);
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready_fds[1], @ptrCast(&sig), 1);
        // Simulate a crash mid-commit: no releaseWriter, no close, just
        // park until the parent's kill -9 below ends this process
        // outright, dropping its liveness lock on slot 0 with it.
        parkForever();
    }
    _ = std.posix.system.close(ready_fds[1]);
    defer _ = std.posix.system.close(ready_fds[0]);
    var sig: [1]u8 = undefined;
    try readBounded(ready_fds[0], &sig, 2000);

    _ = std.c.kill(child_pid, std.c.SIG.KILL);
    _ = try reapBounded(child_pid, 2000);

    // Opens the *same* region, still sized to exactly one liveness slot,
    // so this claim cannot land anywhere but slot 0 -- the exact index
    // the now-dead child just freed. Pre-v5, this is precisely the
    // configuration that broke `isSlotDead`: this handle's own
    // `claimed_slots` now lists index 0 as "mine", so a liveness probe of
    // it would trivially report "alive" (this process's own lock, not the
    // dead child's) regardless of what `writer_holder` actually names.
    var checker = try Region.openSized(a, io, db_path, 1);
    defer checker.close();
    try std.testing.expectEqual(@as(u32, 0), checker.own_liveness_slot);

    // The real fix: `writer_holder` still names (the dead child's pid,
    // slot 0, the *old* generation). `checker`'s own claim just bumped
    // slot 0's generation past that, so this is dead on the generation
    // check alone -- never mind who, if anyone, is sitting on the index
    // right now.
    try std.testing.expect(checker.writerHolderDead());

    const recovery = struct {
        fn run(_: ?*anyopaque) anyerror!void {}
    };
    try checker.recoverDeadWriter(.{ .run = &recovery.run });
    try std.testing.expect(!checker.writerHolderDead());

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(checker.tryAcquireWriter(own_pid));
    checker.releaseWriter();
}

// The companion to the reused-slot test above: a liveness slot that
// *hasn't* been reclaimed by anyone else must never read as dead, no
// matter how long its holder has been sitting on it. Exercises
// `isReferenceDead`'s other branch (generation matches, so it falls
// through to the real `fcntl` probe) under the ordinary, non-colliding
// case every ordinary commit actually takes.
test "shared lock region: a live writer's own liveness slot is never misdetected dead" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-live-writer.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));
    var go_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&go_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        _ = std.posix.system.close(go_fds[1]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        // Same reasoning as the kill -9'd-writer test's own comment:
        // without this, `writer_holder` would carry the inherited (and,
        // for this handle, un-held) `own_liveness_slot`, not a slot this
        // child's own liveness lock actually backs.
        region.reclaimLivenessAfterFork() catch std.c._exit(1);
        if (!region.tryAcquireWriter(my_pid)) std.c._exit(1);
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready_fds[1], @ptrCast(&sig), 1);
        // Stays alive -- genuinely holding the writer lock, not merely
        // about to be killed -- until the parent says to let go, so the
        // check below runs against a holder that really is still
        // running, not one caught mid-crash.
        var go: [1]u8 = undefined;
        readBounded(go_fds[0], &go, 2000) catch std.c._exit(1);
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready_fds[1]);
    defer _ = std.posix.system.close(ready_fds[0]);
    _ = std.posix.system.close(go_fds[0]);
    defer _ = std.posix.system.close(go_fds[1]);

    var sig: [1]u8 = undefined;
    try readBounded(ready_fds[0], &sig, 2000);

    // The child's own claim of its liveness slot is still the current
    // one -- nobody has reclaimed it -- so `isReferenceDead` falls
    // through to the real liveness probe, which correctly reports it
    // alive because it genuinely is.
    try std.testing.expect(!region.writerHolderDead());

    // Let the child release cleanly and exit, rather than leaving it
    // parked for a `kill -9` -- that crash case is the *other* test's
    // own job (both the original one above and the reused-slot one).
    var go: [1]u8 = .{1};
    _ = std.c.write(go_fds[1], @ptrCast(&go), 1);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pid, 2000));
    try std.testing.expect(!region.writerHolderDead());
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

// Ports writer_queue.zig's own "foreignTicketsIssuedSince distinguishes
// a different process's ticket from this one's own" to the region's
// ticket ring: `othersWaitingAcrossProcesses`'s whole reason to exist
// over `othersWaiting` (`WriterLane.checkin`'s own real caller -- its
// background maintenance worker reaching the region through a different
// path than the lease it's deciding whether to release). A real second
// *thread* (not a fork, which always gets its own, different pid) gives
// this same process a second, genuinely queued ticket under the same
// pid to tell apart from a truly foreign one.
test "shared lock region: othersWaitingAcrossProcesses distinguishes a foreign ticket from this process's own" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-foreign-ticket.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    const own_pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region.tryAcquireWriterFair(own_pid)); // ticketless hold; nobody queued yet.

    const SameProcessWaiter = struct {
        fn run(r: *Region, pid: u32) void {
            const thread_io = std.testing.io;
            // Bounded (generous, matching this file's own other waits),
            // not `acquireWriter`'s plain indefinite wait: this runs on
            // a real OS thread with no `reapBounded`-style external
            // force-kill available if something is ever actually wrong,
            // so the bound has to come from the call itself.
            _ = r.acquireWriterFairBounded(thread_io, pid, Io.Clock.now(.awake, thread_io).addDuration(.fromSeconds(10)), null) catch return;
            r.releaseWriter();
        }
    };
    var same_pid_thread = try std.Thread.spawn(.{}, SameProcessWaiter.run, .{ &region, own_pid });

    // Give the thread a real chance to be genuinely queued (not just
    // about to try), same margin the two-process test above uses.
    try io.sleep(.fromMilliseconds(20), .awake);
    try std.testing.expect(region.othersWaiting());
    // Not a *foreign* waiter: the only queued ticket is this same
    // process's own, reached through a different caller.
    try std.testing.expect(!region.othersWaitingAcrossProcesses(own_pid));

    // Now a genuinely different process queues behind it.
    var ready_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&ready_fds));
    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(ready_fds[0]);
        const my_pid: u32 = @bitCast(std.c.getpid());
        var sig: [1]u8 = .{1};
        _ = std.c.write(ready_fds[1], @ptrCast(&sig), 1);
        region.acquireWriter(my_pid);
        region.releaseWriter();
        std.c._exit(0);
    }
    _ = std.posix.system.close(ready_fds[1]);
    defer _ = std.posix.system.close(ready_fds[0]);
    var sig: [1]u8 = undefined;
    try readBounded(ready_fds[0], &sig, 2000);
    try io.sleep(.fromMilliseconds(20), .awake);

    try std.testing.expect(region.othersWaitingAcrossProcesses(own_pid));

    // Release the original ticketless hold: the FIFO cascades through
    // the same-pid thread's own ticket first, then the foreign child's.
    region.releaseWriter();
    same_pid_thread.join();
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child_pid, 2000));
}

// Regression for `TestLiteCAPI`'s "snapshot with open transaction" timing
// out in `fairWriterLockWithIo`: unlike the sibling test above (which
// shares one `Region` instance, so the same-pid waiter is reached "through
// a different caller" on the exact same opener), this opens a *second*,
// independent `Region.open` on the same path -- the same shape a second
// in-process `Owner` with incompatible open options (for example a
// read-only connection) gets. Same pid, but a distinct `own_liveness_slot`
// per open, so it must count as a foreign waiter: it cannot make the
// zero-cost same-process progress a true same-opener caller can, and
// relying solely on `maintainParkedLease`'s idle timer to ever serve it
// starves it exactly as an unrecognized cross-process waiter would be.
test "shared lock region: othersWaitingAcrossProcesses treats a different in-process Owner's ticket as foreign too" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-foreign-owner.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region1 = try Region.open(a, io, db_path);
    defer region1.close();
    var region2 = try Region.open(a, io, db_path);
    defer region2.close();
    try std.testing.expect(region1.own_liveness_slot != region2.own_liveness_slot);

    const pid: u32 = @bitCast(std.c.getpid());
    try std.testing.expect(region1.tryAcquireWriterFair(pid)); // ticketless hold; nobody queued yet.

    const OtherOwnerWaiter = struct {
        fn run(r: *Region, waiter_pid: u32) void {
            const thread_io = std.testing.io;
            _ = r.acquireWriterFairBounded(thread_io, waiter_pid, Io.Clock.now(.awake, thread_io).addDuration(.fromSeconds(10)), null) catch return;
            r.releaseWriter();
        }
    };
    var other_owner_thread = try std.Thread.spawn(.{}, OtherOwnerWaiter.run, .{ &region2, pid });

    // Give the thread a real chance to be genuinely queued, same margin
    // the sibling test above uses.
    try io.sleep(.fromMilliseconds(20), .awake);
    try std.testing.expect(region1.othersWaiting());
    try std.testing.expect(region1.othersWaitingAcrossProcesses(pid));

    region1.releaseWriter();
    other_owner_thread.join();
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
    // Uncontended: succeeds well within a generous deadline, and reports
    // that correctly (not contended).
    try std.testing.expectEqual(false, try region.acquireWriterFairBounded(io, pid, std.Io.Clock.now(.awake, io).addDuration(.fromSeconds(5)), null));
    // Still held by `pid`'s own hold above (never released): a short
    // deadline against a genuinely busy word must time out, bounded,
    // not hang.
    const short_deadline = std.Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(100));
    try std.testing.expectError(error.Timeout, region.acquireWriterFairBounded(io, pid + 1, short_deadline, null));
    region.releaseWriter();
    // Free again: a fresh bounded acquire succeeds, proving the timed-out
    // attempt above didn't leave the region in a bad state. Uncontended
    // again: the previous attempt's own phase-2 timeout already stepped
    // itself aside (advancing head to match tail) before returning, so
    // nothing is left queued for this call to find.
    try std.testing.expectEqual(false, try region.acquireWriterFairBounded(io, pid + 1, std.Io.Clock.now(.awake, io).addDuration(.fromSeconds(5)), null));
    region.releaseWriter();
}

// Ports writer_queue.zig's own "rule 6: cancelling a queued waiter does
// not let a later one pass an earlier live one" to the region's ticket
// ring: an earlier, still-active ticket must not be skipped or harmed by
// a later ticket's own timeout, and tickets taken after the abandoned one
// must still make progress once it's skipped -- not wedge forever, which
// is exactly what would happen if whoever eventually reaches the
// abandoned ticket's position in `head` didn't know to continue past it.
test "shared lock region: a canceled fair waiter gives up its ticket" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var threaded = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-cancel.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    var region = try Region.open(a, io, db_path);
    defer region.close();
    const pid: u32 = @bitCast(std.c.getpid());
    const Waiter = struct {
        fn run(r: *Region, wio: Io, owner: u32) !bool {
            return r.acquireWriterFairBounded(wio, owner, Io.Clock.now(.awake, wio).addDuration(.fromSeconds(30)), null);
        }
        fn queued(r: *Region, wio: Io, tickets: u32) !void {
            var waited_ms: u32 = 0;
            while (r.header().writer_ticket_tail.load(.monotonic) -% r.header().writer_ticket_head.load(.monotonic) < tickets) : (waited_ms += 5) {
                if (waited_ms > 5000) return error.TestTimeout;
                try wio.sleep(.fromMilliseconds(5), .awake);
            }
            try wio.sleep(.fromMilliseconds(20), .awake);
        }
    };
    const soon = struct {
        fn at(wio: Io) Io.Timestamp {
            return Io.Clock.now(.awake, wio).addDuration(.fromMilliseconds(500));
        }
    }.at;

    // At the head, waiting for the ticketless holder to leave.
    try std.testing.expect(region.tryAcquireWriterFair(pid));
    var head = try std.Io.concurrent(io, Waiter.run, .{ &region, io, pid });
    try Waiter.queued(&region, io, 1);
    try std.testing.expectError(error.Canceled, head.cancel(io));
    region.releaseWriter();
    _ = try region.acquireWriterFairBounded(io, pid, soon(io), null);
    region.releaseWriter();

    // Queued behind a live ticket.
    try std.testing.expect(region.tryAcquireWriterFair(pid));
    var first = try std.Io.concurrent(io, Waiter.run, .{ &region, io, pid });
    try Waiter.queued(&region, io, 1);
    var second = try std.Io.concurrent(io, Waiter.run, .{ &region, io, pid });
    try Waiter.queued(&region, io, 2);
    try std.testing.expectError(error.Canceled, second.cancel(io));
    region.releaseWriter();
    _ = try first.await(io);
    region.releaseWriter();
    _ = try region.acquireWriterFairBounded(io, pid, soon(io), null);
    region.releaseWriter();
}

test "shared lock region: a ticket abandoned after the handoff reached it is skipped at once" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-handoff.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    var region = try Region.open(a, io, db_path);
    defer region.close();
    const pid: u32 = @bitCast(std.c.getpid());
    const hdr = region.header();
    // A waiter's ticket that the previous writer's handoff already made the
    // head, before the waiter marked it abandoned.
    const ticket = region.tryClaimTicket(hdr.writer_ticket_tail.load(.monotonic), pid).?;
    try std.testing.expectEqual(ticket, hdr.writer_ticket_head.load(.monotonic));
    region.abandonTicket(ticket);
    try std.testing.expectEqual(ticket +% 1, hdr.writer_ticket_head.load(.monotonic));
    // The next bounded writer gets in at once instead of waiting it out.
    const started = Io.Clock.now(.awake, io);
    _ = try region.acquireWriterFairBounded(io, pid, started.addDuration(.fromSeconds(2)), null);
    region.releaseWriter();
    try std.testing.expect(started.durationTo(Io.Clock.now(.awake, io)).toMilliseconds() < 1000);
}

const TicketFixture = struct {
    tmp: std.testing.TmpDir,
    path: []u8,
    region: Region,
    fn init(name: []const u8) !TicketFixture {
        const a = std.testing.allocator;
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        var buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_len = try tmp.dir.realPath(std.testing.io, &buf);
        const path = try std.fmt.allocPrint(a, "{s}/{s}", .{ buf[0..dir_len], name });
        errdefer a.free(path);
        return .{ .tmp = tmp, .path = path, .region = try Region.open(a, std.testing.io, path) };
    }
    fn deinit(self: *TicketFixture) void {
        self.region.close();
        std.testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

test "shared lock region: a stale claimant leaves a published dead ticket for the head path" {
    var fixture = try TicketFixture.init("region-stale-claimant.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    // A contender read tail `t` and paused. Another process claimed `t`,
    // published it, and died before its turn.
    const t = hdr.writer_ticket_tail.load(.monotonic);
    const ticket: u32 = @truncate(t);
    const dead = packTicketWord(ticket, pid +% 1, region.own_liveness_slot, region.own_liveness_generation +% 1);
    region.ticketSlots()[ticket % hdr.max_readers].store(dead, .release);
    hdr.writer_ticket_tail.store(t +% 1, .release);
    // The contender resumes with its stale `t`.
    try std.testing.expectEqual(@as(?u32, null), region.tryClaimTicket(t, pid));
    try std.testing.expectEqual(dead, region.ticketSlots()[ticket % hdr.max_readers].load(.acquire));
    // The head path still retires the dead ticket.
    _ = try region.acquireWriterFairBounded(std.testing.io, pid, Io.Clock.now(.awake, std.testing.io).addDuration(.fromSeconds(1)), null);
    region.releaseWriter();
    try std.testing.expectEqual(@as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))), hdr.writer_ticket_head.load(.monotonic));
}

test "shared lock region: a stale claimant checks publication only after the owner's death" {
    var fixture = try TicketFixture.init("region-claim-order.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    // A competing claim of ticket `t`, not yet published. In the window
    // between the stale contender's death probe and its published check,
    // the claim is published (the owner published, then died).
    const t = hdr.writer_ticket_tail.load(.monotonic);
    const ticket: u32 = @truncate(t);
    const dead = packTicketWord(ticket, pid +% 1, region.own_liveness_slot, region.own_liveness_generation +% 1);
    region.ticketSlots()[ticket % hdr.max_readers].store(dead, .release);
    const Publish = struct {
        fn run(r: *Region) void {
            const h = r.header();
            h.writer_ticket_tail.store(h.writer_ticket_tail.load(.monotonic) +% 1, .release);
        }
    };
    region.test_after_claim_death_probe = Publish.run;
    try std.testing.expectEqual(@as(?u32, null), region.tryClaimTicket(t, pid));
    region.test_after_claim_death_probe = null;
    // Published, so the stale contender left it for the head path.
    try std.testing.expectEqual(dead, region.ticketSlots()[ticket % hdr.max_readers].load(.acquire));
    _ = try region.acquireWriterFairBounded(std.testing.io, pid, Io.Clock.now(.awake, std.testing.io).addDuration(.fromSeconds(1)), null);
    region.releaseWriter();
}

test "shared lock region: a stale claimant frees a dead claim that was never published" {
    var fixture = try TicketFixture.init("region-claim-unpublished.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    const t = hdr.writer_ticket_tail.load(.monotonic);
    const ticket: u32 = @truncate(t);
    region.ticketSlots()[ticket % hdr.max_readers].store(packTicketWord(ticket, pid +% 1, region.own_liveness_slot, region.own_liveness_generation +% 1), .release);
    try std.testing.expectEqual(@as(?u32, null), region.tryClaimTicket(t, pid));
    try std.testing.expectEqual(@as(u128, 0), region.ticketSlots()[ticket % hdr.max_readers].load(.acquire));
    try std.testing.expectEqual(ticket, region.tryClaimTicket(t, pid).?);
}

test "shared lock region: a ticketless claim yields to a ticket published before it" {
    var fixture = try TicketFixture.init("region-ticketless-yield.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    // The queue looked empty, then a waiter published its ticket before
    // this caller's claim landed.
    const Publish = struct {
        var ticket: ?u32 = null;
        fn run(r: *Region) void {
            ticket = r.tryClaimTicket(r.header().writer_ticket_tail.load(.monotonic), @bitCast(std.c.getpid()));
        }
    };
    region.test_after_ticketless_claim = Publish.run;
    try std.testing.expect(!region.tryAcquireWriterFair(pid));
    region.test_after_ticketless_claim = null;
    try std.testing.expect(!region.writerWordHeld());
    // The waiter, not the overtaker, takes the turn.
    try std.testing.expect(region.tryAcquireWriter(pid));
    region.finishTurn(Publish.ticket.?);
    region.releaseWriter();
    try std.testing.expectEqual(@as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))), hdr.writer_ticket_head.load(.monotonic));
}

test "shared lock region: dead-reader cleanup cannot erase a pin taken over in between" {
    var fixture = try TicketFixture.init("region-reader-takeover.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const pid: u32 = @bitCast(std.c.getpid());
    // Fill every slot with a dead claim (an exited reader's).
    const dead = packHolderGen(pid +% 1, region.own_liveness_slot, region.own_liveness_generation +% 1);
    for (region.readerSlots()) |*slot| {
        @atomicStore(u128, &slot.generation, dead, .release);
        slot.owner.store(pid +% 1, .release);
        slot.pinned_sequence.store(5, .release);
    }
    // While the writer's frontier scan has found slot 0's claim dead, a new
    // reader takes that slot over and pins it.
    const Takeover = struct {
        var slot: ?u32 = null;
        fn run(r: *Region) void {
            if (slot != null) return;
            slot = r.claimReaderSlot(@bitCast(std.c.getpid())) catch unreachable;
            r.pinReader(slot.?, 42);
        }
    };
    region.test_after_reader_dead_probe = Takeover.run;
    // The raced scan itself, the one a writer chooses reuse from, must
    // include the replacement's pin.
    const raced = region.minimumPinnedSequence();
    region.test_after_reader_dead_probe = null;
    try std.testing.expectEqual(@as(?u64, 42), raced);
    const taken = Takeover.slot.?;
    try std.testing.expect(region.isReaderSlotLive(taken));
    try std.testing.expectEqual(@as(u64, 42), region.readerSlots()[taken].pinned_sequence.load(.acquire));
    try std.testing.expectEqual(@as(?u64, 42), region.minimumPinnedSequence());
    region.releaseReaderSlot(taken);
}

test "shared lock region: the log watermark never moves backwards and a torn publication recovers" {
    var fixture = try TicketFixture.init("region-log-state.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    region.publishLogState(.{ .checkpoint_sequence = 7, .durable_sequence = 42, .durable_offset = 4096 });
    // A stale publisher (a scan that finished behind the writer).
    region.publishLogState(.{ .checkpoint_sequence = 7, .durable_sequence = 41, .durable_offset = 2048 });
    try std.testing.expectEqual(@as(u64, 42), region.readLogState().durable_sequence);
    // A publisher died with the generation odd: reads stop spinning and
    // see no publication, and the next lease holder's publish repairs it.
    region.tearLogStateForTest();
    try std.testing.expectEqual(@as(u64, 0), region.readLogState().durable_sequence);
    region.publishLogState(.{ .checkpoint_sequence = 8, .durable_sequence = 43, .durable_offset = 128 });
    const repaired = region.readLogState();
    try std.testing.expectEqual(@as(u64, 43), repaired.durable_sequence);
    try std.testing.expectEqual(@as(u64, 128), repaired.durable_offset);
}

test "shared lock region: concurrent watermark publishers never interleave a tuple" {
    if (builtin.single_threaded) return error.SkipZigTest;
    var fixture = try TicketFixture.init("region-log-state-concurrent.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const rounds: u64 = 4000;
    const Publisher = struct {
        fn run(r: *Region, lane: u64) void {
            var k: u64 = 1;
            while (k <= rounds) : (k += 1) {
                const n = k * 4 + lane;
                r.publishLogState(.{ .checkpoint_sequence = n, .durable_sequence = n, .durable_offset = n * 16 });
            }
        }
    };
    const Reader = struct {
        fn run(r: *Region, stop: *std.atomic.Value(bool), torn: *std.atomic.Value(bool)) void {
            while (!stop.load(.acquire)) {
                const state = r.readLogStateChecked() orelse continue;
                if (state.checkpoint_sequence != state.durable_sequence or state.durable_offset != state.durable_sequence * 16) torn.store(true, .release);
            }
        }
    };
    var stop: std.atomic.Value(bool) = .init(false);
    var torn: std.atomic.Value(bool) = .init(false);
    const reader = try std.Thread.spawn(.{}, Reader.run, .{ region, &stop, &torn });
    var publishers: [4]std.Thread = undefined;
    for (&publishers, 0..) |*thread, lane| thread.* = try std.Thread.spawn(.{}, Publisher.run, .{ region, @as(u64, lane) });
    for (publishers) |thread| thread.join();
    stop.store(true, .release);
    reader.join();
    try std.testing.expect(!torn.load(.acquire));
    const final = region.readLogStateChecked().?;
    try std.testing.expectEqual(final.durable_sequence, final.checkpoint_sequence);
    try std.testing.expectEqual(final.durable_sequence * 16, final.durable_offset);
}

test "shared lock region: concurrent same-process opens of a fresh region share one mapping" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(std.testing.io, &buf);
    const path = try std.fmt.allocPrint(a, "{s}/region-concurrent-open.aflite", .{buf[0..dir_len]});
    defer a.free(path);
    const Opener = struct {
        fn run(p: []const u8, out: *?Region, failed: *bool) void {
            out.* = Region.open(std.heap.page_allocator, std.testing.io, p) catch {
                failed.* = true;
                return;
            };
        }
    };
    var regions: [6]?Region = @splat(null);
    var failed: [6]bool = @splat(false);
    var threads: [6]std.Thread = undefined;
    for (&threads, 0..) |*thread, i| thread.* = try std.Thread.spawn(.{}, Opener.run, .{ path, &regions[i], &failed[i] });
    for (threads) |thread| thread.join();
    defer for (&regions) |*r| if (r.*) |*region| region.close();
    for (failed) |f| try std.testing.expect(!f);
    // A ticket claimed through one handle is visible through every other:
    // no opener truncated and rebuilt a region another had mapped.
    const first = &regions[0].?;
    const ticket = first.tryClaimTicket(first.header().writer_ticket_tail.load(.monotonic), @bitCast(std.c.getpid())).?;
    for (&regions) |*r| try std.testing.expectEqual(ticket +% 1, @as(u32, @truncate(r.*.?.header().writer_ticket_tail.load(.acquire))));
    first.finishTurn(ticket);
}

test "shared lock region: the queue keeps admitting contended writers across the 32-bit head wrap" {
    var fixture = try TicketFixture.init("region-head-wrap.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    const start: u64 = std.math.maxInt(u32) - 1;
    hdr.writer_ticket_head.store(@truncate(start), .release);
    hdr.writer_ticket_tail.store(start, .release);
    for (0..4) |_| {
        const ticket = region.tryClaimTicket(hdr.writer_ticket_tail.load(.monotonic), pid).?;
        region.finishTurn(ticket);
    }
    try std.testing.expect(hdr.writer_ticket_tail.load(.monotonic) > std.math.maxInt(u32));
    try std.testing.expect(region.ringHasRoom());
    // A contended writer: queued behind a ticketless hold, then admitted.
    try std.testing.expect(region.tryAcquireWriterFair(pid));
    const queued = region.tryClaimTicket(hdr.writer_ticket_tail.load(.monotonic), pid).?;
    region.releaseWriter();
    try std.testing.expect(region.tryAcquireWriter(pid));
    region.finishTurn(queued);
    region.releaseWriter();
    try std.testing.expectEqual(@as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))), hdr.writer_ticket_head.load(.monotonic));
}

test "shared lock region: resolving the same abandoned ticket twice never passes tail" {
    var fixture = try TicketFixture.init("region-double-resolve.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    const first = region.tryClaimTicket(hdr.writer_ticket_tail.load(.monotonic), pid).?;
    const second = region.tryClaimTicket(hdr.writer_ticket_tail.load(.monotonic), pid).?;
    // The later waiter gives up while the first still holds the turn;
    // then the first's handoff and the abandoning waiter's own skip both
    // try to resolve it. Exactly one may move head off it.
    region.abandonTicket(second);
    try std.testing.expectEqual(first, hdr.writer_ticket_head.load(.monotonic));
    region.finishTurn(first);
    region.skipResolvedHeads();
    region.abandonTicket(second);
    const tail: u32 = @truncate(hdr.writer_ticket_tail.load(.monotonic));
    try std.testing.expectEqual(tail, hdr.writer_ticket_head.load(.monotonic));
    try std.testing.expect(region.ringHasRoom());
    _ = try region.acquireWriterFairBounded(std.testing.io, pid, Io.Clock.now(.awake, std.testing.io).addDuration(.fromMilliseconds(500)), null);
    region.releaseWriter();
}

test "shared lock region: a dead queued ticket at the head is skipped on a short cadence" {
    var fixture = try TicketFixture.init("region-dead-head.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const hdr = region.header();
    const pid: u32 = @bitCast(std.c.getpid());
    // Publish a ticket whose owner's liveness generation is no longer
    // current: a queued writer that died.
    const t = hdr.writer_ticket_tail.load(.monotonic);
    const ticket: u32 = @truncate(t);
    region.ticketSlots()[ticket % hdr.max_readers].store(packTicketWord(ticket, pid +% 1, region.own_liveness_slot, region.own_liveness_generation +% 1), .release);
    hdr.writer_ticket_tail.store(t +% 1, .release);
    try std.testing.expectEqual(ticket, hdr.writer_ticket_head.load(.monotonic));
    const started = Io.Clock.now(.awake, std.testing.io);
    _ = try region.acquireWriterFairBounded(std.testing.io, pid, started.addDuration(.fromSeconds(5)), null);
    region.releaseWriter();
    try std.testing.expect(started.durationTo(Io.Clock.now(.awake, std.testing.io)).toMilliseconds() < 1000);
}

test "shared lock region: concurrent bounded writers with expiring deadlines keep the queue consistent" {
    if (builtin.single_threaded) return error.SkipZigTest;
    var fixture = try TicketFixture.init("region-ticket-stress.aflite");
    defer fixture.deinit();
    const region = &fixture.region;
    const pid: u32 = @bitCast(std.c.getpid());
    const Worker = struct {
        fn run(r: *Region, owner: u32, seed: u64) void {
            var prng = std.Random.DefaultPrng.init(seed);
            const random = prng.random();
            for (0..300) |_| {
                const budget_us = random.uintLessThan(u64, 400);
                const deadline = Io.Clock.now(.awake, std.testing.io).addDuration(.fromMicroseconds(@intCast(budget_us)));
                if (r.acquireWriterFairBounded(std.testing.io, owner, deadline, null)) |_| {
                    r.releaseWriter();
                } else |_| {}
            }
        }
    };
    var threads: [6]std.Thread = undefined;
    for (&threads, 0..) |*thread, i| thread.* = try std.Thread.spawn(.{}, Worker.run, .{ region, pid, i + 1 });
    for (threads) |thread| thread.join();
    const hdr = region.header();
    try std.testing.expectEqual(@as(u32, @truncate(hdr.writer_ticket_tail.load(.monotonic))), hdr.writer_ticket_head.load(.monotonic));
    try std.testing.expectEqual(@as(u128, 0), hdr.writer_holder.load(.monotonic));
    _ = try region.acquireWriterFairBounded(std.testing.io, pid, Io.Clock.now(.awake, std.testing.io).addDuration(.fromMilliseconds(500)), null);
    region.releaseWriter();
}

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
    // The signal above only proves child 1 is about to call
    // `acquireWriter`, not that it has actually taken ticket 0 yet --
    // see the N-process test's own identical fix for why that gap
    // matters under a loaded host (fork order racing ahead of ticket-
    // taking order). Wait on the region's own shared `writer_ticket_tail`
    // until child 1 has genuinely taken ticket 0 before forking child 2.
    {
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_tail.load(.monotonic) == 0) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 5000) return error.TestTimeout;
        }
    }

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
        const result = region.acquireWriterFairBounded(io, my_pid, deadline, null);
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
    // 100ms deadline, and past 2000ms per the N-process test's own
    // note on scheduling delay under a loaded host.
    var timedout_report: [1]u8 = undefined;
    try readBounded(timedout2_fds[0], &timedout_report, 10000);
    try std.testing.expectEqual(@as(u8, 1), timedout_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child2_pid, 10000));

    // Now release: child 1 (ticket 0, still patiently waiting) must win,
    // not be harmed by ticket 1's abandonment.
    region.releaseWriter();
    var done1_report: [1]u8 = undefined;
    try readBounded(done1_fds[0], &done1_report, 10000);
    try std.testing.expectEqual(@as(u8, 1), done1_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child1_pid, 10000));

    // Child 1's own release just advanced head past ticket 1's abandoned
    // flag. The region must be left in a sane, usable state -- not
    // wedged -- for whoever comes next.
    _ = try region.acquireWriterFairBounded(io, own_pid, std.Io.Clock.now(.awake, io).addDuration(.fromSeconds(5)), null);
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
    // same poll) -- a fixed, short sleep here only proves this on an
    // unloaded host, though: child 1's own bounded poll step
    // (`dead_ticket_check_step_ms`) still needs to actually get
    // scheduled to run even once, and a fixed 250ms margin isn't enough
    // for that under contention (observed directly in a 200-run loaded
    // loop: `writer_ticket_head` still 0 at the 250ms mark). Poll with a
    // generous bound instead, the same pattern this test's own earlier
    // waits already use.
    {
        var waited_ms: u32 = 0;
        while (region.header().writer_ticket_head.load(.monotonic) != 1) {
            try io.sleep(.fromMilliseconds(5), .awake);
            waited_ms += 5;
            if (waited_ms > 10000) return error.TestTimeout;
        }
    }

    region.releaseWriter();
    var done1_report: [1]u8 = undefined;
    try readBounded(done1_fds[0], &done1_report, 10000);
    try std.testing.expectEqual(@as(u8, 1), done1_report[0]);
    try std.testing.expectEqual(@as(c_int, 0), try reapBounded(child1_pid, 10000));

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
    try std.testing.expectError(error.Timeout, region.acquireWriterFairBounded(io, own_pid, third_deadline, null));
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

test "shared lock region: Header stays exactly header_bytes with the §9.3b WAL-index fields added" {
    // Mechanical sentinel: `comptime { std.debug.assert(@sizeOf(Header)
    // == header_bytes); }` above already enforces this at compile time,
    // but a regression there is a compile error, not a test failure --
    // this test exists so `zig build test`'s normal reporting surfaces
    // the intent in a test name too, matching the file's own convention
    // of also exercising every comptime invariant through a named test.
    // 128 since v5 widened `writer_holder` to 128 bits and `_log_reserved`
    // to match (`format_version`'s own v5 comment).
    try std.testing.expectEqual(@as(usize, 128), @sizeOf(Header));
}

test "shared lock region: publishLogState/readLogState round-trip a consistent triple" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-log-state.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var region = try Region.open(a, io, db_path);
    defer region.close();

    // Fresh region: everything starts at zero.
    const fresh = region.readLogState();
    try std.testing.expectEqual(@as(u64, 0), fresh.checkpoint_sequence);
    try std.testing.expectEqual(@as(u64, 0), fresh.durable_sequence);
    try std.testing.expectEqual(@as(u64, 0), fresh.durable_offset);

    region.publishLogState(.{ .checkpoint_sequence = 7, .durable_sequence = 41, .durable_offset = 4096 });
    const after = region.readLogState();
    try std.testing.expectEqual(@as(u64, 7), after.checkpoint_sequence);
    try std.testing.expectEqual(@as(u64, 41), after.durable_sequence);
    try std.testing.expectEqual(@as(u64, 4096), after.durable_offset);

    // A second publish fully overwrites the triple, not a merge.
    region.publishLogState(.{ .checkpoint_sequence = 7, .durable_sequence = 42, .durable_offset = 4224 });
    const again = region.readLogState();
    try std.testing.expectEqual(@as(u64, 7), again.checkpoint_sequence);
    try std.testing.expectEqual(@as(u64, 42), again.durable_sequence);
    try std.testing.expectEqual(@as(u64, 4224), again.durable_offset);
}

test "shared lock region: readLogState sees publishLogState across a second handle" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-log-state-shared.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);

    var writer_side = try Region.open(a, io, db_path);
    defer writer_side.close();
    var reader_side = try Region.open(a, io, db_path);
    defer reader_side.close();

    writer_side.publishLogState(.{ .checkpoint_sequence = 3, .durable_sequence = 9, .durable_offset = 512 });
    const seen = reader_side.readLogState();
    try std.testing.expectEqual(@as(u64, 3), seen.checkpoint_sequence);
    try std.testing.expectEqual(@as(u64, 9), seen.durable_sequence);
    try std.testing.expectEqual(@as(u64, 512), seen.durable_offset);
}

test "shared lock region: a v2 region is rebuilt on open by a v3 binary" {
    // Same posture as the existing corrupt-header tests: an old
    // format_version is just another kind of "fails isValidHeader",
    // rebuilt from scratch when no live peer holds it (no migration --
    // this region is a disposable coordination aid, never a migrated
    // artifact).
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const db_path = try std.fmt.allocPrint(a, "{s}/region-v2.aflite", .{buf[0..dir_len]});
    defer a.free(db_path);
    const region_path = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{db_path});
    defer a.free(region_path);

    {
        var region = try Region.open(a, io, db_path);
        region.close();
    }
    // Downgrade the on-disk `format_version` directly, as a v2 writer
    // would have left it. `isValidHeader`'s version check runs before
    // its checksum check, so this alone fails validation regardless of
    // the (otherwise still-valid) checksum.
    {
        const f = try Io.Dir.cwd().openFile(io, region_path, .{ .mode = .read_write });
        defer f.close(io);
        const old_version: u32 = 2;
        try f.writePositionalAll(io, std.mem.asBytes(&old_version), 8);
    }

    var region = try Region.open(a, io, db_path);
    defer region.close();
    // Current `format_version` (bumped past 3 since this test was
    // written; see that constant's own history), not a hardcoded literal
    // -- the point of this test is "rebuilt to whatever the binary's own
    // version is," not "rebuilt to exactly 3."
    try std.testing.expectEqual(format_version, region.header().format_version);
    const state = region.readLogState();
    try std.testing.expectEqual(@as(u64, 0), state.checkpoint_sequence);
}
