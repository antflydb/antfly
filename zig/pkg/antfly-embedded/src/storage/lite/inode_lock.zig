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

//! Process-wide registry of in-process flock coordination for native data
//! files, keyed by inode -- the role SQLite's `unixInodeInfo` plays for its
//! own POSIX advisory locks.
//!
//! Every NativeFile (and so every Store) with a given path open in this
//! process shares one `Entry`, and so one kernel file descriptor and lock
//! state, instead of each independently calling `flock()` on its own fd.
//! `flock()` is scoped to the open file description, not the process, so
//! two different fds from the *same* process do not recognize each other as
//! compatible: a reader opened immediately after this same process's own
//! writer took (and kept, to amortize the kernel call across commits) an
//! exclusive lock on its own fd would otherwise queue behind that fd
//! forever, since nothing else in the process would ever release it for
//! the reader. Funneling every in-process acquisition for a given inode
//! through the one fd its `Entry` owns makes that self-conflict
//! structurally impossible -- there is never more than one fd per inode
//! taking part in `flock()` from this process, no matter how many
//! NativeFile instances (possibly belonging to different, independently
//! opened `Store`s, as `externally_locked` explicitly allows) have the path
//! open.
//!
//! An in-process acquisition against a *parked* hold (kernel lock kept
//! warm, but nothing in this process actively relying on it right now --
//! `exclusive_count == 0`, or `shared_count == 0`) is granted for free,
//! with no kernel call: `.shared` against a parked `.exclusive`, and
//! `.exclusive` against a parked `.shared` or an existing `.exclusive`
//! (active or parked). An *actively held* lock, by contrast, still makes
//! a conflicting in-process request wait or fail -- a plain reader (no
//! `reader_coordinator` participation) genuinely relies on this
//! exclusion, not any other in-memory bookkeeping, to keep a writer from
//! reusing pages it might still read, and a writer actively reclaiming
//! pages relies on it to keep a reader from observing a half-reused one.
//! Only a genuine kernel-level transition (none -> shared/exclusive, or
//! shared -> exclusive) touches the kernel, and always through the
//! `Entry`'s own fd. The kernel lock's only remaining job is excluding
//! *other processes*; in-process coordination beyond that (the writer
//! admission and retirement-eligibility decisions themselves) is already
//! handled by other, independent, already-tested mechanisms
//! (`ReaderCoordinator`, `WriterLane`/the path lease, `Store`'s own
//! mutexes) that this registry does not duplicate or replace.
//!
//! A lock is parked, not released, once no in-process holder remains, so
//! the next acquisition (from any NativeFile on this inode) is served for
//! free -- no kernel call. Cross-process fairness is bounded by
//! `fairness_quantum_ns`: once idle for at least that long, the next
//! release attempts a nonblocking unlock-then-reacquire probe on the
//! entry's own fd; if a foreign process grabs it in the gap, this process
//! correctly cedes it (the probe's reacquire attempt simply fails) instead
//! of forcing its way back in. A fail-fast `.shared` acquirer (a plain
//! read-only open with `wait_for_reader_lock = false`) retries across one
//! such window (`shared_fail_fast_retry_ns`) instead of giving up on the
//! very first attempt, since a genuinely foreign process's own hold, or
//! this process's own parking dance, can each cost up to one quantum.
//!
//! `Entry` is reference-counted by every `open`/`unref` pair *and* every
//! `acquire`/`Handle.release` pair, through the same `refs` counter: a
//! `Handle` returned by `acquire` captures the `*Entry` it was granted
//! against and stores its own share of `refs`, independent of whatever a
//! NativeFile's own long-lived reference (from `open`) later does. This is
//! what makes `replaceWithPreparedGeneration`'s atomic generation swap safe:
//! redirecting a NativeFile's own reference to a different entry never
//! invalidates a `Handle` some in-flight commit acquired against the old
//! one before the swap -- that `Handle`'s own share of `refs` keeps the old
//! entry alive, and its `release` still finds and decrements the exact
//! entry it was granted against, never a freshly reread, possibly-swapped
//! field. The entry is destroyed only once every reference of both kinds
//! has gone -- at which point `shared_count`/`exclusive_count` are
//! necessarily both already zero too, since every path that increments
//! `refs` for a lock also increments the matching count, and decrements
//! both together.
//!
//! The registry's own bookkeeping (the `Entry` struct, the path it reopens
//! with, and the hashmap of them) always uses `std.heap.page_allocator`,
//! never a caller-supplied one: an `Entry` outlives, and is shared across,
//! whichever individual NativeFile happened to create or last reference
//! it, so it cannot safely be tied to any one of their allocators.
const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const shared_lock_region = @import("shared_lock_region.zig");
const redo_overlay = @import("redo_overlay.zig");

pub const Mode = enum { shared, exclusive };

/// Permanent (Debug-only) leak diagnostics: every successful `acquire`
/// records its call site; every `release` removes one matching record.
/// If an `Entry` is ever torn down (`unrefAndMaybeDestroy`) with
/// `shared_count`/`exclusive_count` still nonzero, or a fail-fast
/// `.shared` acquirer gives up on an actively-held exclusive, this dumps
/// every still-outstanding acquisition's call site (resolved the same way
/// the write-amplification CPU harness's allocation-site histogram
/// resolves its own captured frames) instead of leaving a bare count to
/// track down by inspection. Off in non-Debug builds: the capture and the
/// list it lives in cost nothing there.
const debug_tracking = builtin.mode == .debug;

const DebugAcquisition = struct {
    mode: Mode,
    /// A short literal naming the call site (e.g. `"acquireReaderFence"`,
    /// `"vacuumWithCancel"`), threaded in by every caller of `acquire`
    /// (see `native.zig`'s `acquireInodeLock`). Explicit labels, not an
    /// unwound stack trace: a captured trace from deep inside this
    /// module's own `acquire`/`tryExclusiveOnce`/`trySharedOnce` wrapper
    /// chain did not reliably unwind far enough to reach the real caller
    /// in practice, where a hand-written label always names it exactly.
    site: []const u8,
};

/// Caller holds (or no longer needs) `entry.mutex`; this only reads
/// `entry.debug_outstanding`, never mutates it. Prints to stderr via
/// `std.debug.print` -- a diagnostic dump, not a return value, so a
/// caller mid-returning `error.WouldBlock` or mid-assert can call this
/// unconditionally without disturbing its own control flow.
fn debugDumpOutstanding(entry: *Entry, why: []const u8) void {
    if (!debug_tracking) return;
    std.debug.print("inode_lock: {s} entry={*} inode={d} shared={d} blocking_shared={d} exclusive={d} refs={d} outstanding={d}\n", .{ why, entry, entry.inode, entry.shared_count, entry.blocking_shared_count, entry.exclusive_count, entry.refs, entry.debug_outstanding.items.len });
    for (entry.debug_outstanding.items, 0..) |acq, i| {
        std.debug.print("  [{d}] mode={s} site={s}\n", .{ i, @tagName(acq.mode), acq.site });
    }
}

/// Caller holds `entry.mutex`. Records a successful acquisition's call
/// site for `debugDumpOutstanding`.
fn debugRecordAcquire(entry: *Entry, mode: Mode, site: []const u8) void {
    if (!debug_tracking) return;
    entry.debug_outstanding.append(std.heap.page_allocator, .{ .mode = mode, .site = site }) catch {};
}

/// Caller holds `entry.mutex`. Removes one record matching `mode` (not
/// necessarily the exact acquisition this particular release corresponds
/// to -- `Handle` carries no index back to its own record -- but every
/// outstanding record for a given mode is equally informative for naming
/// a leaking call site, so an arbitrary match is enough).
fn debugRemoveAcquire(entry: *Entry, mode: Mode) void {
    if (!debug_tracking) return;
    var i = entry.debug_outstanding.items.len;
    while (i > 0) {
        i -= 1;
        if (entry.debug_outstanding.items[i].mode == mode) {
            _ = entry.debug_outstanding.swapRemove(i);
            return;
        }
    }
}

/// The kernel-level state an `Entry`'s fd currently holds. Reuses
/// `Io.File.Lock` directly (rather than a parallel enum with the same
/// variants) so `maybeReleaseAssumeLocked`'s `fd.tryLock(io, wanted)` --
/// `wanted` being a saved `entry.state` -- needs no per-call conversion.
const KernelState = Io.File.Lock;

/// Bound on how long an idle `Entry` keeps its kernel lock parked before
/// the next release attempts the cross-process fairness probe below.
///
/// Deliberately *not* the same 20ms quantum `QueuedMutex.contended_quantum_ns`
/// / `WriterLane.foreign_fairness_quantum_ns` (`capi/lite_connection.zig`)
/// use, despite sharing the same trade-off in spirit (bounded same-process
/// reuse instead of paying a kernel call on every turn): both of those gate
/// their probe on elapsed time *and* an explicit "is anyone else waiting"
/// signal from a lease file other processes register intent in
/// (`othersWaiting`/`othersWaitingAcrossProcesses`), so 20ms only matters
/// once that signal already says someone is waiting. A raw `flock()` has no
/// equivalent cheap introspection -- the unlock-then-tryLock probe below
/// *is* the only way this entry can find out, so gating purely on elapsed
/// wall-clock time (as this does) means the quantum alone controls how
/// often the probe pays for one unlock/relock pair, not how often someone
/// is actually waiting. A 20ms quantum measured this way fires on
/// essentially every write whose own latency (one `fsync`, Debug-build
/// instrumentation overhead, contention) exceeds 20ms wall-clock -- which
/// is the common case, not the exception -- making the "parked" warm-fd
/// optimization pay a real kernel call nearly every turn, the opposite of
/// its purpose. Measured directly: the single-writer primary-only
/// benchmark attributed ~0.9 of its ~4.9 flock()/write here before this
/// comment was written to explain why 20ms was wrong for this call site.
/// A full second is still imperceptible next to any plausible genuine
/// cross-process wait, while comfortably exceeding any realistic
/// single-commit wall-clock latency.
pub const fairness_quantum_ns: i128 = 1 * std.time.ns_per_s;

/// How long a fail-fast `.shared` acquirer (`acquire(..., .shared, false)`)
/// retries before giving up. Deliberately independent of
/// `fairness_quantum_ns` (not derived from it, unlike an earlier version of
/// this constant): this needs to stay short to honor "fail-fast" for a
/// caller that explicitly asked not to block, while `fairness_quantum_ns`
/// needs to stay long to keep the warm-parked kernel lock from being
/// probed away on nearly every write (see that constant's own comment).
/// Coupling them meant either one dragged the other to the wrong value.
/// Covers a genuinely different process's own active hold clearing within
/// a brief, bounded grace period; exclusive acquisitions are never retried
/// here -- every existing epoch-reuse-lock call site already treats an
/// immediate `error.WouldBlock` as "skip reuse this commit," which stays
/// correct and cheap; only a fail-fast *read* benefits from tolerating a
/// transient hold.
pub const shared_fail_fast_retry_ns: i128 = 40 * std.time.ns_per_ms;

pub const Entry = struct {
    inode: Io.File.INode,
    /// Reopened lazily to get this entry's own dedicated fd (see
    /// `ensureFd`), and the string `ensureRegion` derives the shared lock
    /// region's own sidecar name from -- the one canonical truth both
    /// use. Canonicalized once (best-effort; see `canonicalPathAlloc`),
    /// at `open`/`rebind` time, to an absolute, symlink-resolved
    /// spelling: necessary for `ensureRegion`'s lazy reopen, which can
    /// run arbitrarily later against whatever is cached here, with no
    /// opportunity to re-derive "where this file originally was" from a
    /// relative string at that later point (a cwd change in between), or
    /// to converge two spellings of the same file (a symlink alias, or
    /// two processes resolving their own relative spellings
    /// independently) on the one sidecar name both must agree on.
    /// `ensureFd` only strictly needs this entry's own inode (already
    /// its whole identity), so a canonical spelling costs it nothing
    /// either -- on a real filesystem. It is *not* safe to canonicalize
    /// this field unless `crash_sim_io.zig`'s `SimIo` normalizes its own
    /// `durable`/`handle_paths` tracking to the same canonical identity
    /// (see that file's `canonicalKeyAllowingMissing`): that shim tracks
    /// durability by path *string*, a test-only simplification, and
    /// `ensureFd` reopening under a spelling different from whatever
    /// `openDataFile`/`createDataFile` used would otherwise register as
    /// a second, independently-tracked, never-synced path for the same
    /// real file, which `applyPowerLoss` then deletes out from under the
    /// first, correctly-synced record -- the exact cause of "crash
    /// sweep: redo log checkpoint with indexed reclamation retires
    /// pages" and "crash sweep: full text synchronous visibility"
    /// failing with `FileNotFound`. Owned by `std.heap.page_allocator`.
    path: []const u8,
    /// Every live reference to this entry, from both `open`/`unref` (a
    /// NativeFile's own long-lived interest) and `acquire`/`release` (a
    /// `Handle`'s share, for as long as it is held) -- see the module doc
    /// comment for why unifying these is what makes the generation-swap
    /// case safe. The entry is destroyed, and its fd closed, once this
    /// drops to zero.
    refs: usize = 0,
    /// Guards every field below, including across the (bounded,
    /// nonblocking) kernel calls `acquireOn`/`releaseOn` make. A
    /// `.shared` acquire's *blocking* kernel wait deliberately holds this
    /// mutex for its whole duration -- see the module-level trade-off
    /// note in `acquireOn`.
    mutex: Io.Mutex = .init,
    /// This entry's single dedicated fd for every in-process flock call on
    /// this inode. Opened lazily on first use; closed only when the entry
    /// itself is torn down.
    fd: ?Io.File = null,
    /// What that fd currently holds at the kernel level.
    state: KernelState = .none,
    shared_count: usize = 0,
    exclusive_count: usize = 0,
    /// The subset of `shared_count` that should actively block a new
    /// `.exclusive` request, rather than merely keep the kernel lock at
    /// `.shared` for bookkeeping purposes. A plain reader with no
    /// `ReaderCoordinator` participation (a bare read-only open, or
    /// `acquireReaderFence` called with `self.reader_coordinator == null`)
    /// relies on exactly this kernel-level exclusion to keep a writer from
    /// reusing pages it might still read -- it counts here. A *coordinated*
    /// reader's fence does not: its protection against this same process's
    /// own writer comes from `ReaderCoordinator`'s epoch tracking instead
    /// (which the writer's own `scanReaderEpochsCached`/`epoch_window`
    /// already consults), so counting it here too would make the writer's
    /// own internal reuse decision (`acquireEpochReuseLock`, reached via
    /// `beginReaderWindow`) block on a protection mechanism that already
    /// has its own, more precise answer -- observed as a same-process
    /// writer with an active `ReaderCoordinator` never reusing a page
    /// again as long as any coordinated reader stayed open. Cross-process
    /// exclusion for a coordinated reader is unaffected either way: this
    /// field only changes what counts as "actively blocking" within this
    /// process's own in-memory bookkeeping, never the real kernel lock a
    /// foreign process's own attempt still has to contend with.
    blocking_shared_count: usize = 0,
    /// When `state` last transitioned, or was last confirmed still wanted
    /// by the fairness probe in `maybeReleaseAssumeLocked`. Read from the
    /// caller's own `io`'s clock at call time, not a stored one, since
    /// different NativeFile instances sharing this entry may use
    /// different `Io` implementations (the real threaded one, or a test's).
    last_transition_ns: i128 = 0,
    /// Debug-only leak diagnostics; see `debugDumpOutstanding`. Always
    /// present (so no other field's offset depends on the build mode),
    /// but only ever appended to or read in a Debug build.
    debug_outstanding: std.ArrayListUnmanaged(DebugAcquisition) = .empty,
    /// §8.5: the shared mmap lock region for this inode's own
    /// `<path>.aflite-lock` sidecar, opened lazily (`ensureRegion`) on
    /// first need and shared by every NativeFile/Store referencing this
    /// `Entry` -- the same "one registry entry, one resource, shared by
    /// every same-process opener" shape this registry already gives the
    /// kernel flock state above. Per §8.7's migration table, this is
    /// what the writer lane, the epoch-reuse lock, and reader fences
    /// move onto as each is migrated; until then it is additive and
    /// unreferenced by any real caller, same posture `shared_lock_region
    /// .zig` itself had through its own steps 1-3.
    ///
    /// `rebind` (a generation swap, e.g.
    /// `NativeFile.replaceWithPreparedGeneration`) closes and clears this
    /// if set, rather than carrying it across the path change: the region
    /// is keyed by the path it was opened against (its own
    /// `<path>.aflite-lock` sidecar), so a region opened against the old
    /// name must not go on serving lookups for the new one. The next
    /// `ensureRegion` call after a rebind reopens lazily against the new
    /// path, exactly like `ensureFd`'s existing lazy-reopen pattern after
    /// `entry.fd` goes back to `null`.
    region: ?shared_lock_region.Region = null,
    /// §9.6a: the redo-log's in-process versioned overlay, shared the
    /// same way `region` is -- one instance per inode, referenced by
    /// every `NativeFile` (writer or `ReadGeneration` reader) open on
    /// this path in this process, instead of each independently
    /// replaying the log into its own copy (which is what made a
    /// `ReadGeneration` blind to anything written after it opened: its
    /// own, unshared copy never saw a later commit at all). Populated
    /// once, by whichever caller reaches `ensureRedoOverlay` first (see
    /// that function); `rebind` does not clear this, unlike `region` --
    /// a generation swap keeps the same logical content, just a new
    /// path/fd, and the overlay is addressed by inode via this `Entry`,
    /// never by path directly.
    redo_overlay: ?redo_overlay.SharedRedoOverlay = null,
    /// perf/lwa-proof-fork: the pid that populated `redo_overlay` above.
    /// This registry (`registry` below) is a plain process-global `var`:
    /// `fork()` duplicates it, `Entry` included, into the child's own
    /// address space verbatim -- correct for every other field here (the
    /// child gets its own fd/region/reader_pins lazily, or inherits
    /// harmless-to-share bookkeeping), but wrong for `redo_overlay`
    /// specifically, because `ensureRedoOverlay`'s whole cheapness
    /// depends on "already populated" meaning "already reflects every
    /// commit made so far" -- true only in the process that has actually
    /// been keeping it live via every commit's own `appendVersion`/
    /// `appendPage` call. A forked child inherits a `redo_overlay` that
    /// is already populated (by its parent, as of fork time) but frozen
    /// exactly there: no write this child ever makes, and no commit its
    /// parent makes after the fork, ever updates this copy again, so a
    /// child that opens its own `NativeFile` and trusts "already
    /// populated, nothing to replay" permanently can't see anything the
    /// parent committed after the fork point -- confirmed live via "lite
    /// redo log cross-process: a forked child sees commits the parent
    /// makes after the child opens" hanging (the child's own
    /// `getDocumentAlloc` never finds the parent's post-fork commit,
    /// `crossProcessParkForever`s, and the parent's `crossProcessReadBounded`
    /// on the result pipe times out). `ensureRedoOverlay` compares this
    /// field against the caller's own pid and rebuilds (replays fresh,
    /// from `folded_through`, exactly as a genuine first population
    /// would) whenever they differ -- free in the steady state (same
    /// pid, same process, one comparison), and safe even mid-fork-race
    /// because nothing else in a freshly forked child can hold a pointer
    /// into the stale overlay yet (fork only duplicates the calling
    /// thread; the child has made no `NativeFile` calls of its own
    /// before this one).
    redo_overlay_owner_pid: ?u32 = null,
    /// The in-process half of the reuse-safety frontier: every live,
    /// same-process reader that can dereference tree pages at a fixed
    /// checkpoint registers its own pinned sequence here, guarded by
    /// `mutex` like every other field above. This exists alongside (not
    /// instead of) `region`'s cross-process reader table and docstore.zig's
    /// own `Store.reader_frontier` atomic: those two already cover their
    /// own reader categories (a coordinated reader via `region`, a
    /// `ReadGeneration` pin via `reader_frontier`), but neither one is
    /// reachable by a reader that opens its own plain, uncoordinated
    /// `NativeFile` directly (e.g. `captureFullReadSnapshot`'s reader, used
    /// by vacuum's own verification snapshot) -- that reader never claims a
    /// region slot and is a different `NativeFile` instance than whatever
    /// `Store.reader_frontier` is wired into. `minimumReaderSequence`/
    /// `minimumMetadataReaderSequence` combine this registry with
    /// `region`'s own answer so every caller has one place to ask,
    /// regardless of which mechanism a given reader happens to use.
    reader_pins: std.ArrayListUnmanaged(ReaderPin) = .empty,
    /// Published by the writer side on every commit (inode-wide, so every
    /// instance on this inode -- not just the one that committed -- can
    /// read it): the most recently committed `commit_sequence`, used to
    /// detect a writer instance operating against a stale base (an old
    /// generation/header it never refreshed). Populated under the
    /// publishing commit's own exclusion (`NativeFile.publishCheckpoint`);
    /// read lock-free via a monotonic compare, never regressed by an older
    /// commit's publish racing a newer one. See
    /// `publishCommittedSequence`/`committedSequence` below.
    last_committed_sequence: std.atomic.Value(u64) = .init(0),
    /// Highest durable redo sequence this process published to the shared
    /// lock region. A larger published sequence came from another process.
    self_published_durable_sequence: std.atomic.Value(u64) = .init(0),
    /// Defense in depth, not a lock: the `NativeFile` instance (as
    /// `@intFromPtr`, 0 for none yet) that most recently began a
    /// transaction against this inode *while marked `lease_managed`* --
    /// claimed by `NativeFile.beginTransaction`, checked back by
    /// `NativeFile.publishCheckpoint`'s own backstop assertion. Only the
    /// capi Owner/Connection layer's own live root and its detached
    /// maintenance snapshot are ever marked `lease_managed`; every other
    /// `NativeFile` on this inode (a bare test, recovery/replay at open, a
    /// borrowed index writer, a vacuum-prepared image, ...) never claims
    /// or is checked against this field at all. The actual exclusivity
    /// invariant -- at most one writer between begin and publish -- is
    /// the writer-lease admission gate in capi/lite_connection.zig
    /// (`WriterLane`/`Owner`); this field only observes whether that
    /// invariant held for the instances it covers, by recording the
    /// latest claimant and letting a later publish notice if a *different*
    /// instance has since begun its own transaction before this one
    /// reached publish. Not itself synchronizing anything: a true
    /// violation can still slip past this check in the other interleaving
    /// (the second instance's own publish lands first, observing itself as
    /// the latest claimant), so this is a backstop that fails loud in the
    /// common case, not a proof of correctness.
    active_writer: std.atomic.Value(usize) = .init(0),
};

/// Claims `file_ptr` (the caller's own `@intFromPtr(self)`) as this
/// inode's most recent transaction claimant. See `active_writer`'s own
/// doc comment; called once per `NativeFile.beginTransaction`, only when
/// `self.lease_managed`.
pub fn claimActiveWriter(entry: *Entry, file_ptr: usize) void {
    entry.active_writer.store(file_ptr, .release);
}

/// Panics if a *different*, nonzero claimant has since begun its own
/// transaction on this inode. See `active_writer`'s own doc comment for
/// why this is a backstop, not a lock.
pub fn assertActiveWriter(entry: *Entry, file_ptr: usize) void {
    const current = entry.active_writer.load(.acquire);
    if (current != 0 and current != file_ptr) {
        std.debug.panic("lite: NativeFile publishing without holding the writer lease (self=0x{x} active_writer=0x{x})", .{ file_ptr, current });
    }
}

/// Publishes `sequence` as this inode's own latest known-durable commit,
/// monotonically: a concurrent publish of an older sequence (e.g. a stale
/// writer that is only now discovering/reporting what it already knew)
/// never regresses a newer one already recorded. Called once per durable
/// checkpoint publish, from the publishing `NativeFile` itself.
pub fn publishCommittedSequence(entry: *Entry, sequence: u64) void {
    var current = entry.last_committed_sequence.load(.monotonic);
    while (sequence > current) {
        current = entry.last_committed_sequence.cmpxchgWeak(current, sequence, .monotonic, .monotonic) orelse return;
    }
}

/// This inode's latest known-durable commit sequence in this process (0 if
/// nothing has published yet). See `publishCommittedSequence`.
pub fn committedSequence(entry: *Entry) u64 {
    return entry.last_committed_sequence.load(.monotonic);
}

/// Unconditionally overwrites this inode's latest known-durable commit
/// sequence, discarding whatever was recorded before -- unlike
/// `publishCommittedSequence`'s monotonic merge, which assumes the
/// existing high-water mark is still meaningful for the *same* lineage.
/// For a generation swap (`NativeFile.replaceWithPreparedGeneration`):
/// the entry being adopted is the one the discarded `prepared` instance
/// used for its own, unrelated lineage (its own vacuum/compaction
/// attempt, possibly itself a retried or abandoned one) -- a high
/// leftover value there does not describe any commit that exists in the
/// newly adopted header's own numbering, so merging monotonically would
/// permanently strand this inode's registered sequence ahead of a header
/// that can never catch up to it. The newly adopted header's own
/// sequence is this inode's one and only truth immediately after the
/// swap; nothing else may have published to this entry since (the swap
/// runs under the writer-lease admission gate's own exclusivity).
pub fn resetCommittedSequence(entry: *Entry, sequence: u64) void {
    entry.last_committed_sequence.store(sequence, .monotonic);
}

/// One live reader's pinned sequence(s), registered in `Entry.reader_pins`.
/// Mirrors `shared_lock_region.Region`'s own data/metadata split so a
/// reader that only dereferences one domain can pin just that one; most
/// callers (a full snapshot reader) pin both to the same value.
pub const ReaderPin = struct {
    data: ?u64 = null,
    metadata: ?u64 = null,
};

/// Registers a live pin, under `entry.mutex`. Callers must do this
/// *before* reading whatever checkpoint/root they are about to dereference
/// pages through (or re-validate afterward that it didn't change) -- see
/// this function's callers in native.zig for the exact ordering each one
/// uses. `allocator` is always `std.heap.page_allocator`: like `region`/
/// `redo_overlay`, this registry's own bookkeeping outlives, and is shared
/// across, whichever individual caller populated or last referenced it.
pub fn pinReader(entry: *Entry, io: Io, pin: ReaderPin) !void {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    try entry.reader_pins.append(std.heap.page_allocator, pin);
}

/// Removes exactly one pin matching `pin` (the same values `pinReader` was
/// given), not every pin that happens to share those values.
pub fn unpinReader(entry: *Entry, io: Io, pin: ReaderPin) void {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    for (entry.reader_pins.items, 0..) |p, i| {
        if (std.meta.eql(p, pin)) {
            _ = entry.reader_pins.swapRemove(i);
            return;
        }
    }
}

/// The minimum `.data` pin currently registered in this registry alone
/// (never combined with `region`'s own answer -- callers that want the
/// full picture call both this and `region.minimumPinnedSequence()` and
/// take the smaller, exactly like every other term in `NativeFile.
/// reuseFrontier` already does). `null` if no pin carries a `.data` value.
pub fn minimumReaderSequence(entry: *Entry, io: Io) ?u64 {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    var min: ?u64 = null;
    for (entry.reader_pins.items) |p| if (p.data) |s| {
        if (min == null or s < min.?) min = s;
    };
    return min;
}

/// The metadata analogue of `minimumReaderSequence`.
pub fn minimumMetadataReaderSequence(entry: *Entry, io: Io) ?u64 {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    var min: ?u64 = null;
    for (entry.reader_pins.items) |p| if (p.metadata) |s| {
        if (min == null or s < min.?) min = s;
    };
    return min;
}

/// Lazily populates (once per `Entry`, shared by every reference to it)
/// and returns this inode's shared redo-log overlay. If it is already
/// populated, `populate` is never called -- the existing overlay already
/// reflects every commit made so far, kept current by every writer's own
/// `appendVersion` calls against the same shared instance, so a second (or
/// later) opener on this inode must not replay the log again, which would
/// either duplicate versions or race the live writer's own in-flight
/// appends. `entry.mutex` serializes concurrent first-use the same way it
/// already serializes every other field above, including `region`.
///
/// perf/lwa-proof-fork: "already populated" is only trustworthy in the
/// process that has actually been keeping it live -- see
/// `Entry.redo_overlay_owner_pid`'s own doc comment for the fork-inherits-
/// a-stale-copy bug this guards against. A pid mismatch means this
/// process never participated in whatever commits built the inherited
/// overlay, so it is rebuilt here exactly as a genuine first population
/// would be (discarded and replayed fresh via `populate`), not reused.
pub fn ensureRedoOverlay(
    entry: *Entry,
    io: Io,
    ctx: anytype,
    populate: *const fn (@TypeOf(ctx), *redo_overlay.SharedRedoOverlay, std.mem.Allocator) anyerror!void,
) !*redo_overlay.SharedRedoOverlay {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    const own_pid: u32 = @bitCast(std.c.getpid());
    if (entry.redo_overlay != null and entry.redo_overlay_owner_pid != own_pid) {
        entry.redo_overlay.?.deinit(std.heap.page_allocator);
        entry.redo_overlay = null;
    }
    if (entry.redo_overlay == null) {
        entry.redo_overlay = redo_overlay.SharedRedoOverlay{};
        errdefer {
            entry.redo_overlay.?.deinit(std.heap.page_allocator);
            entry.redo_overlay = null;
            entry.redo_overlay_owner_pid = null;
        }
        // Like `region` above: this registry's own bookkeeping always
        // uses `std.heap.page_allocator`, never a caller-supplied one --
        // the overlay outlives, and is shared across, whichever
        // individual NativeFile happened to populate or last reference
        // it, so it cannot safely be tied to any one of their allocators.
        try populate(ctx, &entry.redo_overlay.?, std.heap.page_allocator);
        entry.redo_overlay_owner_pid = own_pid;
    }
    return &entry.redo_overlay.?;
}

/// Runs `apply` against this inode's already-populated shared overlay
/// under `entry.mutex` *and* the overlay's own `mutex`. §9.3b's
/// cross-process catch-up refresh needs this:
/// `redo_overlay.SharedRedoOverlay.decodeLogicalMutationsPayload`/
/// `decodeRedoGroupPayload` are deliberately unlocked (safe only while
/// nothing else can reach the overlay concurrently -- see their own doc
/// comments), which is true at the genuine first population `populate`
/// runs under (`ensureRedoOverlay` creates the `SharedRedoOverlay` fresh
/// and holds the only pointer to it, under `entry.mutex`, before any
/// other thread can have one). It is *not* true here: by the time a
/// catch-up refresh runs, other `NativeFile`s in this process already
/// hold their own pointer to this same, long-lived overlay and call
/// `appendVersion`/`resolve`/etc. against it guarded by the overlay's
/// own `mutex`, a different lock than `entry.mutex`. Taking only
/// `entry.mutex` here (as a prior version of this function did) leaves
/// the unlocked decode calls racing every one of those `self.mutex`-
/// guarded callers on the same `maps`/`entry_pool` -- observed live as
/// both lost full-text-index writes (a racing `appendVersion` insert
/// overwritten or dropped) and `entry_pool` free-list corruption
/// (concurrent `create`/`destroy` tearing the free list, segfaulting a
/// later `create`). Taking the overlay's own `mutex` too gives the
/// decode calls the same exclusion every other accessor already gets.
/// Returns `error.InvalidTransactionState` if the overlay was never
/// populated at all (nothing to refresh).
pub fn withRedoOverlayLock(
    entry: *Entry,
    io: Io,
    ctx: anytype,
    apply: *const fn (@TypeOf(ctx), *redo_overlay.SharedRedoOverlay) anyerror!void,
) !void {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    const overlay = if (entry.redo_overlay) |*o| o else return error.InvalidTransactionState;
    overlay.mutex.lockUncancelable(io);
    defer overlay.mutex.unlock(io);
    try apply(ctx, overlay);
}

/// Lazily opens (once per `Entry`, shared by every reference to it) and
/// returns this inode's shared lock region. `entry.mutex` serializes
/// concurrent first-use the same way it already serializes every other
/// field above.
pub fn ensureRegion(entry: *Entry, io: Io) !*shared_lock_region.Region {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    if (entry.region == null) {
        entry.region = try shared_lock_region.Region.open(std.heap.page_allocator, io, entry.path);
    }
    return &entry.region.?;
}

/// True when `err` (from `shared_lock_region.Region.open`/`openSized`)
/// means the region itself can't be used at this path at all -- a
/// read-only or otherwise uncooperative filesystem (the same conditions
/// that already send `native.zig`'s pre-region flock paths to their own
/// plain fallback), a sidecar name too long for the platform, proven
/// corruption, or exhausted liveness slots -- rather than a transient or
/// resource failure. Every migrated caller, writer or reader side, shares
/// this one check so "region unusable, fall back to flock" means the same
/// thing everywhere: callers should fall back to the plain-flock path on
/// `true`, and propagate the error unchanged on `false` (notably
/// `OutOfMemory`, which a fallback attempt would likely fail the same way,
/// so swallowing it here would only hide the real failure).
pub fn isRegionUnusable(err: anyerror) bool {
    return switch (err) {
        error.AccessDenied,
        error.ReadOnlyFileSystem,
        error.PermissionDenied,
        error.NameTooLong,
        error.RegionUnavailable,
        error.RegionCorrupt,
        error.LivenessSlotsExhausted,
        => true,
        else => false,
    };
}

var registry_mutex: Io.Mutex = .init;
var registry: std.AutoHashMapUnmanaged(Io.File.INode, *Entry) = .empty;

/// Resolves `path` to an absolute, symlink-resolved path for `Entry.path`
/// (see that field's own doc comment for the full reasoning: this is
/// the one canonical truth both `ensureFd` and `ensureRegion` use, safe
/// for `ensureFd` specifically because `crash_sim_io.zig`'s `SimIo` now
/// normalizes its own tracking to the same canonical identity -- see
/// that file's `canonicalKeyAllowingMissing`). Best-effort, and never
/// fails on account of `path` not currently resolving: `open` already
/// confirms `path` exists (via its own, separate stat, before this is
/// ever called), so this call's own fast tier should succeed in
/// practice; the fallbacks below exist for the narrow remaining races
/// (a concurrent deletion between that stat and this call) and for
/// `rebind`, whose caller has already renamed the file into place but
/// might, in principle, race a concurrent remover. Each tier falls
/// through to the next on any error other than `OutOfMemory`: an
/// existing file resolves directly; failing that, its parent directory
/// resolved through an open handle plus the basename joined back on
/// (mirroring `native.zig`'s own `writerLockPathAlloc`); failing that
/// too, `path` itself, unresolved -- exactly what every caller tolerated
/// before this file canonicalized anything.
fn canonicalPathAlloc(allocator: std.mem.Allocator, io: Io, path: []const u8) std.mem.Allocator.Error![]u8 {
    if (Io.Dir.cwd().realPathFileAlloc(io, path, allocator)) |resolved| {
        // `realPathFileAlloc` returns a sentinel-terminated `[:0]u8`,
        // over-allocated by one byte for the terminator. `Entry.path`'s
        // own field type is a plain `[]const u8`, with no sentinel, so
        // storing the sentinel slice directly and freeing it later under
        // that plain type would ask the allocator to free one byte fewer
        // than was actually allocated (caught by `SafeAllocator` as a
        // size mismatch). Re-dupe into a plain `[]u8` sized to exactly
        // `.len` and free the sentinel original here, under its own
        // matching type, instead.
        defer allocator.free(resolved);
        return try allocator.dupe(u8, resolved);
    } else |err| if (err == error.OutOfMemory) return error.OutOfMemory;

    if (canonicalParentJoin(allocator, io, path)) |joined| {
        return joined;
    } else |err| if (err == error.OutOfMemory) return error.OutOfMemory;

    return try allocator.dupe(u8, path);
}

fn canonicalParentJoin(allocator: std.mem.Allocator, io: Io, path: []const u8) ![]u8 {
    const dirname = std.fs.path.dirname(path) orelse ".";
    const basename = std.fs.path.basename(path);
    var parent = try Io.Dir.cwd().openDir(io, dirname, .{});
    defer parent.close(io);
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const parent_len = try parent.realPath(io, &buf);
    return try std.fs.path.join(allocator, &.{ buf[0..parent_len], basename });
}

/// Resolves `path`'s current inode and returns this process's shared
/// `Entry` for it (creating it on first use), with one reference already
/// counted. Call once per NativeFile, at open/create time; release with
/// exactly one matching `unref`, at `NativeFile.close`. Store the returned
/// pointer directly (behind an atomic, if it can ever be reread concurrently
/// with a rebind such as `replaceWithPreparedGeneration`'s) -- there is
/// nothing else to wrap it in.
///
/// Stats `path` itself first, not a canonicalized form of it, so the one
/// check that decides whether this call succeeds or fails at all keeps
/// simple, direct timing. Canonicalization (`canonicalPathAlloc`) is
/// deferred until *after* that stat succeeds, and only for a genuinely
/// new `Entry` (a registry hit reuses the existing one's own
/// `Entry.path`, already canonical from whenever it was first created):
/// both avoid adding canonicalization's own extra real I/O to a path
/// this call already confirms exists, or to the common warm case of
/// reopening an inode this process already has open.
pub fn open(io: Io, path: []const u8) !*Entry {
    const page = std.heap.page_allocator;
    const inode = (try Io.Dir.cwd().statFile(io, path, .{})).inode;
    registry_mutex.lockUncancelable(io);
    defer registry_mutex.unlock(io);
    if (registry.get(inode)) |entry| {
        entry.mutex.lockUncancelable(io);
        entry.refs += 1;
        entry.mutex.unlock(io);
        return entry;
    }
    const canonical_path = try canonicalPathAlloc(page, io, path);
    errdefer page.free(canonical_path);
    const entry = try page.create(Entry);
    errdefer page.destroy(entry);
    entry.* = .{ .inode = inode, .path = canonical_path, .refs = 1 };
    try registry.put(page, inode, entry);
    return entry;
}

/// Releases a NativeFile's own long-lived reference to `entry` (from
/// `open`). Never force-releases a lock: every acquirer already pairs
/// `acquire` with `Handle.release`, including on error paths, and each of
/// those holds its own share of `refs` for as long as it is outstanding,
/// so this reference going away can never be the one that makes
/// `shared_count`/`exclusive_count` wrong -- it only matters for *when the
/// entry itself* gets torn down.
pub fn unref(entry: *Entry, io: Io) void {
    unrefAndMaybeDestroy(entry, io);
}

pub const Handle = struct {
    /// `null` for a `Handle` from `acquireStandalone` below, in which
    /// case `standalone_file` is what gets closed on `release`, not an
    /// `Entry`.
    entry: ?*Entry,
    mode: Mode,
    /// Only meaningful for `mode == .shared`: whether this grant counted
    /// toward `Entry.blocking_shared_count` and so must decrement it on
    /// release too. See that field's doc comment.
    blocks_exclusive: bool = false,
    standalone_file: Io.File = undefined,

    /// Releases this grant against the exact `Entry` it was acquired
    /// against -- captured in this `Handle` at `acquire` time, never
    /// reread from wherever the caller's own reference to an entry (for
    /// example `NativeFile.inode_lock_ref`) might point *now*. A rebind
    /// (`replaceWithPreparedGeneration`) redirects only future acquires;
    /// this one still resolves correctly against the entry it was
    /// actually granted on, even if that entry is no longer current.
    pub fn release(self: Handle, io: Io) void {
        if (self.entry) |entry| {
            releaseOn(entry, io, self.mode, self.blocks_exclusive);
        } else {
            self.standalone_file.close(io);
        }
    }
};

/// For a NativeFile with no registry entry reference -- an internal
/// vacuum/compaction workspace file built directly rather than through
/// `open`/`create` (some on a private temp path, some with no path at
/// all), so nothing else in this process could plausibly share its
/// inode. Falls back to the original, unshared direct acquisition this
/// registry otherwise replaces: a fresh `open` plus `lock`/`tryLock` on
/// its own fd, exactly as every call site here used to do before this
/// registry existed, with the exact same contract (`.shared` blocks when
/// `blocking`; `.exclusive` never blocks).
pub fn acquireStandalone(io: Io, path: []const u8, mode: Mode, blocking: bool) !Handle {
    const file = try Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    errdefer file.close(io);
    switch (mode) {
        .shared => if (blocking) {
            try file.lock(io, .shared);
        } else if (!try file.tryLock(io, .shared)) {
            return error.WouldBlock;
        },
        .exclusive => if (!try file.tryLock(io, .exclusive)) return error.WouldBlock,
    }
    return .{ .entry = null, .mode = mode, .standalone_file = file };
}

/// Acquires `.shared` or `.exclusive` on `entry`'s inode. `blocks_exclusive`
/// only matters for `mode == .shared` (pass whatever for `.exclusive`):
/// whether this grant should actively block a future `.exclusive` request
/// in this process, not just keep the kernel lock at `.shared` for
/// bookkeeping -- `true` for a plain reader with no `ReaderCoordinator`
/// participation (the only in-memory protection it has), `false` for a
/// coordinated one (whose protection against this same process's own
/// writer comes from `ReaderCoordinator`'s epoch tracking instead -- see
/// `Entry.blocking_shared_count`'s doc comment for why conflating the two
/// is wrong).
///
/// `.shared` is granted for free (no kernel call) whenever this process
/// already holds `.shared`, or a *parked* `.exclusive` (kernel lock
/// retained, but `exclusive_count == 0`: nothing in this process is
/// actively relying on the exclusivity right now). An *actively held*
/// `.exclusive` (`exclusive_count != 0`), by contrast, makes this shared
/// request wait (`blocking == true`, matching `acquireReaderFence`'s
/// contract) or retry across `shared_fail_fast_retry_ns` before surfacing
/// `error.WouldBlock` (`blocking == false`, a fail-fast reader-open's
/// grace period) -- same as a genuinely foreign process's hold would.
/// `entry.state == .none` (nobody, not even this process, holds anything)
/// takes a real nonblocking kernel `tryLock`, which resolves any genuine
/// foreign contention on its own.
///
/// `.exclusive` never blocks and is never retried, returning
/// `error.WouldBlock` immediately on any conflict (an actively *blocking*
/// shared hold, or a real foreign hold), matching every existing
/// epoch-reuse-lock call site's contract (every caller already treats
/// that as "skip reuse this commit"). It is granted for free against an
/// existing `.exclusive` (active or parked) or a `.shared` with no active
/// *blocking* holder, the same asymmetry as above.
pub fn acquire(entry: *Entry, io: Io, mode: Mode, blocking: bool, blocks_exclusive: bool, site: []const u8) !Handle {
    switch (mode) {
        .shared => {
            const deadline: ?i128 = if (blocking) null else Io.Clock.awake.now(io).nanoseconds + shared_fail_fast_retry_ns;
            while (true) {
                if (try trySharedOnce(entry, io, blocks_exclusive, site)) |handle| return handle;
                if (deadline) |d| if (Io.Clock.awake.now(io).nanoseconds >= d) {
                    if (debug_tracking) {
                        entry.mutex.lockUncancelable(io);
                        debugDumpOutstanding(entry, "fail-fast shared gave up");
                        entry.mutex.unlock(io);
                    }
                    return error.WouldBlock;
                };
                io.sleep(.fromMilliseconds(1), .awake) catch {};
            }
        },
        .exclusive => return (try tryExclusiveOnce(entry, io, site)) orelse error.WouldBlock,
    }
}

/// One attempt at a `.shared` grant: `null` means "not available yet, try
/// again" (an actively held exclusive, or a momentary foreign conflict);
/// any other error propagates. Takes and releases `entry.mutex` itself
/// rather than being called under it, so `acquire`'s retry loop above
/// never holds the mutex across its `io.sleep` -- that would otherwise
/// serialize every other in-process acquirer on this inode behind this
/// one's own wait.
fn trySharedOnce(entry: *Entry, io: Io, blocks_exclusive: bool, site: []const u8) !?Handle {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    if (entry.state == .shared or (entry.state == .exclusive and entry.exclusive_count == 0)) {
        entry.shared_count += 1;
        if (blocks_exclusive) entry.blocking_shared_count += 1;
        entry.refs += 1;
        debugRecordAcquire(entry, .shared, site);
        return .{ .entry = entry, .mode = .shared, .blocks_exclusive = blocks_exclusive };
    }
    if (entry.state == .exclusive) return null; // actively held: wait, no kernel call
    const fd = try ensureFd(entry, io);
    if (!try fd.tryLock(io, .shared)) return null;
    entry.state = .shared;
    entry.shared_count += 1;
    if (blocks_exclusive) entry.blocking_shared_count += 1;
    entry.refs += 1;
    entry.last_transition_ns = Io.Clock.awake.now(io).nanoseconds;
    debugRecordAcquire(entry, .shared, site);
    return .{ .entry = entry, .mode = .shared, .blocks_exclusive = blocks_exclusive };
}

/// One, only, attempt at an `.exclusive` grant: `null` means "would
/// block" (every existing call site already maps that to
/// `error.WouldBlock` and treats it as "skip reuse this commit" -- there
/// is no retry for `.exclusive`, unlike `.shared` above).
fn tryExclusiveOnce(entry: *Entry, io: Io, site: []const u8) !?Handle {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    // An *actively blocking* `.shared` (`blocking_shared_count != 0`)
    // must block an exclusive request, same as a real foreign lock
    // would -- see the doc comment above. Checked before the
    // `state == .exclusive` free-grant case too: a reader can hold an
    // active share while the kernel state is still `.exclusive` (it was
    // granted for free against what was, at the time, a merely *parked*
    // exclusive -- `trySharedOnce` above), so `state == .exclusive` alone
    // does not mean nothing is actively relying on this entry right now.
    // Only a
    // *parked* hold (blocking_shared_count == 0, whatever `state` is) may
    // be reclaimed for a new exclusive grant. A non-blocking (coordinated
    // reader) shared count does not count here: see this field's doc
    // comment.
    if (entry.blocking_shared_count != 0) return null;
    if (entry.state == .exclusive) {
        entry.exclusive_count += 1;
        entry.refs += 1;
        debugRecordAcquire(entry, .exclusive, site);
        return .{ .entry = entry, .mode = .exclusive };
    }
    const fd = try ensureFd(entry, io);
    // `lock`/`tryLock` assume the fd is unlocked; a `.shared` ->
    // `.exclusive` transition needs an explicit unlock first (there is
    // no atomic shared-to-exclusive upgrade -- only `downgradeLock` goes
    // the other, always-safe direction). The brief gap this opens is
    // harmless: at worst a foreign process races in and our own
    // `tryLock` below reports that the same way as any other conflict.
    if (entry.state == .shared) fd.unlock(io);
    entry.state = .none;
    if (!try fd.tryLock(io, .exclusive)) return null;
    entry.state = .exclusive;
    entry.exclusive_count += 1;
    entry.refs += 1;
    entry.last_transition_ns = Io.Clock.awake.now(io).nanoseconds;
    debugRecordAcquire(entry, .exclusive, site);
    return .{ .entry = entry, .mode = .exclusive };
}

fn releaseOn(entry: *Entry, io: Io, mode: Mode, blocks_exclusive: bool) void {
    {
        entry.mutex.lockUncancelable(io);
        defer entry.mutex.unlock(io);
        switch (mode) {
            .shared => {
                std.debug.assert(entry.shared_count != 0);
                entry.shared_count -= 1;
                if (blocks_exclusive) {
                    std.debug.assert(entry.blocking_shared_count != 0);
                    entry.blocking_shared_count -= 1;
                }
            },
            .exclusive => {
                std.debug.assert(entry.exclusive_count != 0);
                entry.exclusive_count -= 1;
                if (entry.exclusive_count == 0 and entry.shared_count != 0 and entry.state == .exclusive) {
                    // Atomic, no release window: lets a foreign shared
                    // reader in without the probe dance below, and
                    // without ever leaving the fd briefly unlocked.
                    if (entry.fd) |fd| fd.downgradeLock(io) catch {};
                    entry.state = .shared;
                }
            },
        }
        debugRemoveAcquire(entry, mode);
        if (entry.shared_count == 0 and entry.exclusive_count == 0) maybeReleaseAssumeLocked(entry, io);
    }
    unrefAndMaybeDestroy(entry, io);
}

/// Shared teardown path for both `unref` (a NativeFile's own long-lived
/// reference going away) and `releaseOn` (a `Handle`'s share going away):
/// decrement `refs`, and once it reaches zero -- which, since every path
/// that increments it for a lock also increments the matching count and
/// decrements both together, can only happen with
/// `shared_count == exclusive_count == 0` already -- remove the entry from
/// the registry and free it. The assert is a defensive double-check, not
/// load-bearing logic.
fn unrefAndMaybeDestroy(entry: *Entry, io: Io) void {
    const page = std.heap.page_allocator;
    registry_mutex.lockUncancelable(io);
    var destroy = false;
    var fd: ?Io.File = null;
    var region: ?shared_lock_region.Region = null;
    var overlay: ?redo_overlay.SharedRedoOverlay = null;
    {
        entry.mutex.lockUncancelable(io);
        defer entry.mutex.unlock(io);
        entry.refs -= 1;
        destroy = entry.refs == 0;
        if (destroy) {
            if (debug_tracking and (entry.shared_count != 0 or entry.exclusive_count != 0)) debugDumpOutstanding(entry, "teardown with nonzero counts");
            std.debug.assert(entry.shared_count == 0 and entry.exclusive_count == 0);
            fd = entry.fd;
            entry.fd = null;
            entry.state = .none;
            region = entry.region;
            entry.region = null;
            overlay = entry.redo_overlay;
            entry.redo_overlay = null;
        }
    }
    if (destroy) _ = registry.remove(entry.inode);
    registry_mutex.unlock(io);
    if (destroy) {
        if (fd) |held| held.close(io);
        if (region) |*r| r.close();
        if (overlay) |*o| o.deinit(page);
        page.free(entry.path);
        entry.reader_pins.deinit(page);
        entry.debug_outstanding.deinit(page);
        page.destroy(entry);
    }
}

/// Updates the path `entry` reopens its dedicated fd with if it is ever
/// closed and needed again (`ensureFd`), to `new_path`. The entry's
/// identity is its inode, not a path, and inodes survive a rename -- but a
/// *remembered path* does not, so a caller that renames the file this
/// entry's inode refers to (an atomic generation swap,
/// `replaceWithPreparedGeneration`) must call this with the new path, or a
/// later lazy reopen (only when `entry.fd` happens to have gone back to
/// `null`, e.g. after a fairness release) would try the old, now-
/// nonexistent name.
///
/// Also closes and clears `entry.region` if one is open: that mapping was
/// opened against the old path's own `<old_path>.aflite-lock` sidecar, and
/// nothing renames that sidecar alongside the data file's own rename, so a
/// region left open across this call would silently keep serving stale
/// (pre-swap) writer/reader state for what is now a different generation
/// at this path. The next `ensureRegion` call lazily reopens a fresh region
/// against `new_path`, exactly mirroring `ensureFd`'s own lazy-reopen shape.
pub fn rebind(entry: *Entry, io: Io, new_path: []const u8) !void {
    const page = std.heap.page_allocator;
    // Canonicalize here too, for the exact reason `open` does: the next
    // `ensureRegion`/`ensureFd` reopens lazily, at some later and
    // otherwise unconstrained point, against whatever is cached in
    // `entry.path` -- so it must already be an absolute, symlink-resolved
    // path by the time this call returns, not a relative spelling that
    // depended on this call's own cwd.
    const owned = try canonicalPathAlloc(page, io, new_path);
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    page.free(entry.path);
    entry.path = owned;
    if (entry.region) |*r| {
        r.close();
        entry.region = null;
    }
}

/// Caller holds `entry.mutex`. Fully idle (no in-process holder remains):
/// park the kernel lock as-is unless the fairness quantum has elapsed, in
/// which case probe for a foreign waiter by releasing and immediately
/// attempting a nonblocking reacquisition of the same mode. A foreign
/// process that was genuinely waiting wins that race (our reacquire then
/// reports `false`/fails, and we correctly cede the lock, leaving `state`
/// at `.none` for the next acquirer to re-earn properly); otherwise we get
/// it straight back, parked for the next in-process acquirer exactly as
/// before, at the cost of one extra unlock+relock pair no more often than
/// once per quantum.
fn maybeReleaseAssumeLocked(entry: *Entry, io: Io) void {
    if (entry.state == .none) return;
    const now = Io.Clock.awake.now(io).nanoseconds;
    if (now - entry.last_transition_ns < fairness_quantum_ns) return;
    const fd = entry.fd orelse return;
    const wanted = entry.state;
    fd.unlock(io);
    entry.state = .none;
    const reacquired = fd.tryLock(io, wanted) catch false;
    if (reacquired) entry.state = wanted;
    entry.last_transition_ns = now;
}

/// Drops the kernel lock an idle entry keeps parked, so another fd of this
/// process can lock the file (a reader fence restored after a writer
/// window). No-op while any in-process grant is outstanding.
pub fn cedeParked(entry: *Entry, io: Io) void {
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    if (entry.shared_count != 0 or entry.exclusive_count != 0 or entry.state == .none) return;
    const fd = entry.fd orelse return;
    fd.unlock(io);
    entry.state = .none;
    entry.last_transition_ns = Io.Clock.awake.now(io).nanoseconds;
}

fn ensureFd(entry: *Entry, io: Io) !Io.File {
    if (entry.fd) |fd| return fd;
    // No `.lock` here: every acquisition above takes the lock explicitly,
    // separately, right after, so `lock`/`tryLock`'s "assumes unlocked"
    // precondition holds for this fresh fd.
    const fd = try Io.Dir.cwd().openFile(io, entry.path, .{ .mode = .read_write });
    entry.fd = fd;
    return fd;
}

test "ensureRegion opens the shared lock region once and shares it across references" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const path = try std.fmt.allocPrint(a, "{s}/inode-region.data", .{buf[0..dir_len]});
    defer a.free(path);
    {
        const f = try Io.Dir.cwd().createFile(io, path, .{});
        f.close(io);
    }

    const entry = try open(io, path);
    defer unref(entry, io);
    // A second reference to the same inode shares the same `Entry`, and
    // so the same `region` once opened -- never a second, independent
    // one.
    const entry2 = try open(io, path);
    defer unref(entry2, io);
    try std.testing.expectEqual(entry, entry2);

    const region = try ensureRegion(entry, io);
    const region_again = try ensureRegion(entry, io);
    try std.testing.expectEqual(region, region_again);

    const own_pid: u32 = @bitCast(std.c.getpid());
    region.acquireWriter(own_pid);
    region.releaseWriter();
}

test "ensureRegion's region closes when the entry is torn down" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const path = try std.fmt.allocPrint(a, "{s}/inode-region-teardown.data", .{buf[0..dir_len]});
    defer a.free(path);
    {
        const f = try Io.Dir.cwd().createFile(io, path, .{});
        f.close(io);
    }

    {
        const entry = try open(io, path);
        _ = try ensureRegion(entry, io);
        unref(entry, io); // last reference: tears the entry (and its region) down.
    }

    // Reopening (a fresh `Entry`, since the old one was destroyed) and
    // claiming the writer lock must succeed -- if the old region's
    // liveness slot or writer hold had leaked past teardown, this would
    // hang or fail instead.
    const entry = try open(io, path);
    defer unref(entry, io);
    const region = try ensureRegion(entry, io);
    const own_pid: u32 = @bitCast(std.c.getpid());
    region.acquireWriter(own_pid);
    region.releaseWriter();
}

test "rebind closes an already-open region instead of carrying it across the path change" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const old_path = try std.fmt.allocPrint(a, "{s}/inode-rebind-old.data", .{buf[0..dir_len]});
    defer a.free(old_path);
    const new_path = try std.fmt.allocPrint(a, "{s}/inode-rebind-new.data", .{buf[0..dir_len]});
    defer a.free(new_path);
    const old_region_sidecar = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{old_path});
    defer a.free(old_region_sidecar);
    const new_region_sidecar = try std.fmt.allocPrint(a, "{s}.aflite-lock", .{new_path});
    defer a.free(new_region_sidecar);
    {
        const f = try Io.Dir.cwd().createFile(io, old_path, .{});
        f.close(io);
    }
    {
        const f = try Io.Dir.cwd().createFile(io, new_path, .{});
        f.close(io);
    }

    const entry = try open(io, old_path);
    defer unref(entry, io);

    // Open a region against the old path and use it, mirroring what a
    // real reader-fence/writer-lock caller would have done before a
    // generation swap renamed this entry's underlying file.
    {
        const region = try ensureRegion(entry, io);
        const own_pid: u32 = @bitCast(std.c.getpid());
        region.acquireWriter(own_pid);
        region.releaseWriter();
    }
    // Only the old path's sidecar exists so far -- `ensureRegion` has
    // never been asked about `new_path`.
    try Io.Dir.cwd().access(io, old_region_sidecar, .{});
    try std.testing.expectError(error.FileNotFound, Io.Dir.cwd().access(io, new_region_sidecar, .{}));

    try rebind(entry, io, new_path);
    // The gap this closes: a stale `region` must not survive the rebind,
    // so the next `ensureRegion` reopens fresh against `new_path` rather
    // than going on serving the old sidecar's mapping.
    try std.testing.expect(entry.region == null);

    const region_after_rebind = try ensureRegion(entry, io);
    try Io.Dir.cwd().access(io, new_region_sidecar, .{});
    const own_pid: u32 = @bitCast(std.c.getpid());
    region_after_rebind.acquireWriter(own_pid);
    region_after_rebind.releaseWriter();
}

// The bug this guards against: `open`/`rebind` used to cache whatever
// path string the caller gave them verbatim. A *relative* one resolved
// correctly at that moment, but `ensureRegion`'s own reopen after a
// `rebind` is lazy -- it can run arbitrarily later, against whatever the
// process's current directory happens to be *then*, not whatever it was
// when the relative spelling was still meaningful. Canonicalizing to an
// absolute path at `open`/`rebind` time (this file's own fix) closes
// that window: the sidecar this test's second `ensureRegion` reopens
// must still be the one next to the data file's real location, even
// after an intervening cwd change landed it somewhere else entirely.
test "ensureRegion's lazy reopen after rebind uses the original directory, not a later cwd" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;

    const original_cwd = try Io.Dir.cwd().realPathFileAlloc(io, ".", a);
    defer a.free(original_cwd);
    var restored = false;
    defer if (!restored) std.process.setCurrentPath(io, original_cwd) catch {};

    var data_tmp = std.testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir_len = try data_tmp.dir.realPath(io, &data_buf);
    const data_dir = try a.dupe(u8, data_buf[0..data_dir_len]);
    defer a.free(data_dir);

    var elsewhere_tmp = std.testing.tmpDir(.{});
    defer elsewhere_tmp.cleanup();
    var elsewhere_buf: [std.fs.max_path_bytes]u8 = undefined;
    const elsewhere_dir_len = try elsewhere_tmp.dir.realPath(io, &elsewhere_buf);
    const elsewhere_dir = try a.dupe(u8, elsewhere_buf[0..elsewhere_dir_len]);
    defer a.free(elsewhere_dir);

    try std.process.setCurrentPath(io, data_dir);
    {
        const f = try Io.Dir.cwd().createFile(io, "old.data", .{});
        f.close(io);
    }
    {
        const f = try Io.Dir.cwd().createFile(io, "new.data", .{});
        f.close(io);
    }

    // Opened, and `rebind` called, with relative spellings -- both while
    // cwd is still `data_dir`, the directory those spellings actually
    // mean.
    const entry = try open(io, "old.data");
    defer unref(entry, io);
    {
        const region = try ensureRegion(entry, io);
        const own_pid: u32 = @bitCast(std.c.getpid());
        region.acquireWriter(own_pid);
        region.releaseWriter();
    }
    try rebind(entry, io, "new.data");
    try std.testing.expect(entry.region == null);

    // Move cwd somewhere else entirely before the lazy reopen runs.
    try std.process.setCurrentPath(io, elsewhere_dir);

    const region_after_cwd_change = try ensureRegion(entry, io);
    const own_pid: u32 = @bitCast(std.c.getpid());
    region_after_cwd_change.acquireWriter(own_pid);
    region_after_cwd_change.releaseWriter();

    // The sidecar must have landed next to `new.data` in its own
    // directory, addressed absolutely -- never relative to whatever cwd
    // is right now.
    const expected_sidecar = try std.fmt.allocPrint(a, "{s}/new.data.aflite-lock", .{data_dir});
    defer a.free(expected_sidecar);
    try Io.Dir.accessAbsolute(io, expected_sidecar, .{});

    // And nothing leaked into `elsewhere_dir`, the cwd the reopen
    // actually ran under.
    try std.testing.expectError(error.FileNotFound, Io.Dir.cwd().access(io, "new.data.aflite-lock", .{}));

    try std.process.setCurrentPath(io, original_cwd);
    restored = true;
}

// #1650's own bug class, checked directly against `ensureFd`: `open`
// canonicalizes `path` to an absolute spelling once, at open time, so a
// *later* lazy reopen of this entry's dedicated flock fd (after `fd`
// goes back to `null`, e.g. following a fairness release) must still
// resolve to the exact same file no matter what the process's current
// directory has since become -- a relative spelling cached instead
// would silently reopen a *different* file (or none at all) after a
// cwd change, and the in-process lock would stop excluding anything
// without telling anyone.
test "ensureFd's lazy reopen targets the original file despite a relative open path and a later cwd change" {
    if (builtin.os.tag == .windows or builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;

    const original_cwd = try Io.Dir.cwd().realPathFileAlloc(io, ".", a);
    defer a.free(original_cwd);
    var restored = false;
    defer if (!restored) std.process.setCurrentPath(io, original_cwd) catch {};

    var data_tmp = std.testing.tmpDir(.{});
    defer data_tmp.cleanup();
    var data_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir_len = try data_tmp.dir.realPath(io, &data_buf);
    const data_dir = try a.dupe(u8, data_buf[0..data_dir_len]);
    defer a.free(data_dir);

    var elsewhere_tmp = std.testing.tmpDir(.{});
    defer elsewhere_tmp.cleanup();
    var elsewhere_buf: [std.fs.max_path_bytes]u8 = undefined;
    const elsewhere_dir_len = try elsewhere_tmp.dir.realPath(io, &elsewhere_buf);
    const elsewhere_dir = try a.dupe(u8, elsewhere_buf[0..elsewhere_dir_len]);
    defer a.free(elsewhere_dir);

    try std.process.setCurrentPath(io, data_dir);
    const relative_path = "ensure-fd-relative.data";
    {
        const f = try Io.Dir.cwd().createFile(io, relative_path, .{});
        f.close(io);
    }
    const expected_inode = (try Io.Dir.cwd().statFile(io, relative_path, .{})).inode;

    // Opened with a relative spelling while cwd is still `data_dir` --
    // the only moment that spelling is meaningful.
    const entry = try open(io, relative_path);
    defer unref(entry, io);

    // Move cwd somewhere else entirely before the lazy reopen runs.
    try std.process.setCurrentPath(io, elsewhere_dir);

    const fd = try ensureFd(entry, io);
    try std.testing.expectEqual(expected_inode, (try fd.stat(io)).inode);

    // Nothing leaked into `elsewhere_dir`, the cwd the reopen actually
    // ran under.
    try std.testing.expectError(error.FileNotFound, Io.Dir.cwd().access(io, relative_path, .{}));

    try std.process.setCurrentPath(io, original_cwd);
    restored = true;
}

test "withRedoOverlayLock's cross-process catch-up replay does not race a live appendVersion caller" {
    // Regression for perf/lwa-integration: `withRedoOverlayLock` used to
    // take only `entry.mutex` before running `decodeLogicalMutationsPayload`
    // against an *already-live* overlay. That decode call is deliberately
    // unlocked -- safe only while nothing else can reach the overlay
    // concurrently, true at `ensureRedoOverlay`'s genuine first population
    // but not here, since every other accessor (`appendVersion`/`reclaim`
    // included) serializes through the overlay's own `mutex`, a different
    // lock than `entry.mutex`. Without the overlay's `mutex` held too, a
    // concurrent `appendVersion`/`reclaim` pair raced the unlocked decode's
    // direct `entry_pool`/`maps` mutation -- observed live as a segfault
    // inside `MemoryPool.create` -> `SinglyLinkedList.popFirst` (reusing
    // a node `reclaim` had just freed back to the pool's shared free list)
    // and as full-text search silently missing a handful of just-indexed
    // documents (a racing `appendVersion` insert lost or overwritten).
    //
    // To actually exercise `popFirst`'s non-empty-free-list path (the one
    // that crashed), both sides reuse a small, fixed key set so repeated
    // appends chain onto (supersede) the previous version and `reclaim`
    // has something real to free back into the pool on every call --
    // `entry_pool` is shared across every root, so this churns the same
    // free list the unlocked decode calls allocate from. Every version's
    // value also carries a unique, checkable marker: if the race aliases
    // or clobbers a node, some key's final resolved value stops matching
    // what that key's own thread last wrote, which the final loop below
    // catches even on a run that happens not to crash outright.
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const path = try std.fmt.allocPrint(a, "{s}/inode-overlay-race.data", .{buf[0..dir_len]});
    defer a.free(path);
    {
        const f = try Io.Dir.cwd().createFile(io, path, .{});
        f.close(io);
    }

    const entry = try open(io, path);
    defer unref(entry, io);

    const NoopCtx = struct {};
    var noop_ctx: NoopCtx = .{};
    const overlay = try ensureRedoOverlay(entry, io, &noop_ctx, struct {
        fn populate(_: *NoopCtx, _: *redo_overlay.SharedRedoOverlay, _: std.mem.Allocator) anyerror!void {}
    }.populate);

    const key_count = 6;
    const total: u64 = 6000;

    // A pin held for the whole test, well above every sequence either
    // thread ever writes: `reclaim`'s floor (`min(durable_checkpoint_sequence,
    // min_pin)`) still advances freely (the pin's value is never the
    // binding constraint), so superseded (non-head) versions are freed
    // back to `entry_pool` throughout -- real churn on the same free list
    // the unlocked decode calls allocate from. What a live pin *does*
    // change is `reclaim`'s "min_pin == null" whole-key-drop path (see its
    // own doc comment): with a pin always held, a key's current head is
    // never wholesale-dropped from `maps` just for being old, exactly the
    // same durability guarantee a real live reader gives -- without this,
    // a key whose last write happens to fall behind the floor before
    // either thread writes it again would legitimately disappear from the
    // overlay (not a bug; the same thing a real reader's pin exists to
    // prevent), which would otherwise show up indistinguishably from the
    // race this test actually targets.
    try overlay.pin(a, io, total * 10);
    defer overlay.unpin(io, total * 10);
    const Appender = struct {
        overlay: *redo_overlay.SharedRedoOverlay,
        allocator: std.mem.Allocator,
        final_value: [key_count]u64 = @splat(0),
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.work() catch |err| {
                self.failure = err;
            };
        }
        fn work(self: *@This()) !void {
            var seq: u64 = 1;
            while (seq <= total) : (seq += 1) {
                const slot = seq % key_count;
                var key_buf: [16]u8 = undefined;
                const key = try std.fmt.bufPrint(&key_buf, "a/{d:0>4}", .{slot});
                var value_buf: [24]u8 = undefined;
                const value = try std.fmt.bufPrint(&value_buf, "a-{d}-{d}", .{ slot, seq });
                try self.overlay.appendVersion(self.allocator, std.testing.io, 0, key, seq, try redo_overlay.owned_allocator.dupe(u8, value), false, 0, 0);
                self.final_value[slot] = seq;
                if (seq % 3 == 0) self.overlay.reclaim(self.allocator, std.testing.io, seq);
            }
        }
    };
    var appender: Appender = .{ .overlay = overlay, .allocator = a };

    const Catcher = struct {
        entry: *Entry,
        allocator: std.mem.Allocator,
        final_value: [key_count]u64 = @splat(0),
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.work() catch |err| {
                self.failure = err;
            };
        }
        fn work(self: *@This()) !void {
            var seq: u64 = 1;
            while (seq <= total) : (seq += 1) {
                const slot = seq % key_count;
                var key_buf: [16]u8 = undefined;
                const key = try std.fmt.bufPrint(&key_buf, "b/{d:0>4}", .{slot});
                var value_buf: [24]u8 = undefined;
                const value = try std.fmt.bufPrint(&value_buf, "b-{d}-{d}", .{ slot, seq });

                // Exactly `RedoCommitState.appendLogicalMutation`'s own
                // encoding: [root:u8][key_len:u16 LE][key][flags:u8]
                // [value_len:u32 LE][value].
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try payload.append(self.allocator, 1); // root
                var key_len_buf: [2]u8 = undefined;
                std.mem.writeInt(u16, &key_len_buf, @intCast(key.len), .little);
                try payload.appendSlice(self.allocator, &key_len_buf);
                try payload.appendSlice(self.allocator, key);
                try payload.append(self.allocator, 0); // flags: live, inline value
                var value_len_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &value_len_buf, @intCast(value.len), .little);
                try payload.appendSlice(self.allocator, &value_len_buf);
                try payload.appendSlice(self.allocator, value);

                const ApplyCtx = struct { payload: []const u8, sequence: u64 };
                var actx: ApplyCtx = .{ .payload = payload.items, .sequence = seq };
                try withRedoOverlayLock(self.entry, std.testing.io, &actx, struct {
                    fn apply(ctx: *ApplyCtx, ovl: *redo_overlay.SharedRedoOverlay) anyerror!void {
                        try ovl.decodeLogicalMutationsPayload(std.heap.page_allocator, ctx.payload, ctx.sequence);
                    }
                }.apply);
                self.final_value[slot] = seq;
            }
        }
    };
    var catcher: Catcher = .{ .entry = entry, .allocator = a };

    const t1 = try std.Thread.spawn(.{}, Appender.run, .{&appender});
    const t2 = try std.Thread.spawn(.{}, Catcher.run, .{&catcher});
    t1.join();
    t2.join();

    try std.testing.expectEqual(@as(?anyerror, null), appender.failure);
    try std.testing.expectEqual(@as(?anyerror, null), catcher.failure);

    // Content check: each key's final resolved value must be exactly
    // what that key's own thread last wrote for it -- any aliasing or
    // lost write in the shared `entry_pool`/`maps` shows up here as a
    // mismatch, even on a run that did not outright crash.
    var slot: u64 = 0;
    while (slot < key_count) : (slot += 1) {
        var a_key_buf: [16]u8 = undefined;
        const a_key = try std.fmt.bufPrint(&a_key_buf, "a/{d:0>4}", .{slot});
        const a_seq = appender.final_value[slot];
        if (a_seq != 0) {
            var expected_buf: [24]u8 = undefined;
            const expected = try std.fmt.bufPrint(&expected_buf, "a-{d}-{d}", .{ slot, a_seq });
            const got = (try overlay.resolveAlloc(a, io, 0, a_key, a_seq)) orelse return error.MissingKeyAfterRace;
            defer a.free(got.value);
            try std.testing.expectEqualStrings(expected, got.value);
        }

        var b_key_buf: [16]u8 = undefined;
        const b_key = try std.fmt.bufPrint(&b_key_buf, "b/{d:0>4}", .{slot});
        const b_seq = catcher.final_value[slot];
        if (b_seq != 0) {
            var expected_buf: [24]u8 = undefined;
            const expected = try std.fmt.bufPrint(&expected_buf, "b-{d}-{d}", .{ slot, b_seq });
            const got = (try overlay.resolveAlloc(a, io, 1, b_key, b_seq)) orelse return error.MissingKeyAfterRace;
            defer a.free(got.value);
            try std.testing.expectEqualStrings(expected, got.value);
        }
    }
}
