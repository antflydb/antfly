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
    /// `ensureFd`). Owned by `std.heap.page_allocator`.
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
};

var registry_mutex: Io.Mutex = .init;
var registry: std.AutoHashMapUnmanaged(Io.File.INode, *Entry) = .empty;

/// Resolves `path`'s current inode and returns this process's shared
/// `Entry` for it (creating it on first use), with one reference already
/// counted. Call once per NativeFile, at open/create time; release with
/// exactly one matching `unref`, at `NativeFile.close`. Store the returned
/// pointer directly (behind an atomic, if it can ever be reread concurrently
/// with a rebind such as `replaceWithPreparedGeneration`'s) -- there is
/// nothing else to wrap it in.
pub fn open(io: Io, path: []const u8) !*Entry {
    const inode = (try Io.Dir.cwd().statFile(io, path, .{})).inode;
    registry_mutex.lockUncancelable(io);
    defer registry_mutex.unlock(io);
    if (registry.get(inode)) |entry| {
        entry.mutex.lockUncancelable(io);
        entry.refs += 1;
        entry.mutex.unlock(io);
        return entry;
    }
    const page = std.heap.page_allocator;
    const owned_path = try page.dupe(u8, path);
    errdefer page.free(owned_path);
    const entry = try page.create(Entry);
    errdefer page.destroy(entry);
    entry.* = .{ .inode = inode, .path = owned_path, .refs = 1 };
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
        }
    }
    if (destroy) _ = registry.remove(entry.inode);
    registry_mutex.unlock(io);
    if (destroy) {
        if (fd) |held| held.close(io);
        page.free(entry.path);
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
pub fn rebind(entry: *Entry, io: Io, new_path: []const u8) !void {
    const page = std.heap.page_allocator;
    const owned = try page.dupe(u8, new_path);
    entry.mutex.lockUncancelable(io);
    defer entry.mutex.unlock(io);
    page.free(entry.path);
    entry.path = owned;
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

fn ensureFd(entry: *Entry, io: Io) !Io.File {
    if (entry.fd) |fd| return fd;
    // No `.lock` here: every acquisition above takes the lock explicitly,
    // separately, right after, so `lock`/`tryLock`'s "assumes unlocked"
    // precondition holds for this fresh fd.
    const fd = try Io.Dir.cwd().openFile(io, entry.path, .{ .mode = .read_write });
    entry.fd = fd;
    return fd;
}
