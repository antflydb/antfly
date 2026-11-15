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

//! The redo-log's in-process, versioned logical overlay (docs/design/
//! lite-performance/lite-single-commit-write-path.md §9.3a/§9.6a), shared
//! per inode per process: a live writer's `NativeFile` and every
//! `ReadGeneration`'s independently-opened `NativeFile` on the same path
//! reference the *same* `SharedRedoOverlay` instance (owned by
//! `inode_lock.Entry`, next to `shared_lock_region.Region` -- the same
//! "one registry entry, one resource, shared by every same-process opener"
//! shape that field already establishes) instead of each building its own
//! independent copy from the log.
//!
//! This module is intentionally standalone, mirroring `redo_log.zig`'s own
//! posture: it knows nothing about `NativeFile`/`CatalogMutation`/any other
//! native.zig type, and `inode_lock.zig` can hold a `SharedRedoOverlay` by
//! value without importing native.zig (which already imports
//! `inode_lock.zig` -- a cycle this module's independence avoids).
//!
//! A SQLite-WAL-style read-mark model: each version carries the
//! `commit_sequence` that produced it (`VersionedOverlayEntry.sequence`),
//! chained to the version it superseded (`.older`), so a reader pinned to
//! an older checkpoint still resolves to exactly the value that existed as
//! of its own pin (`resolve`), never a later overwrite. Reclaiming a
//! version is safe once both (a) a durable checkpoint already covers it
//! (its content is also in real, physical pages by then) and (b) no live
//! pin is at or before its sequence (`reclaim`, driven by `pin`/`unpin`'s
//! own tracked minimum).
const std = @import("std");
const Allocator = std.mem.Allocator;

/// One logical mutation's value as of the `commit_sequence` that produced
/// it. See this module's own doc comment for the full read-mark model.
pub const VersionedOverlayEntry = struct {
    sequence: u64,
    /// Owned; empty when `is_delete` or when the value lives externally
    /// (`external_value_root_page != 0`).
    value: []u8 = &.{},
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
    /// The version this one superseded, or null if this is the oldest
    /// version this overlay has ever held for the key.
    older: ?*VersionedOverlayEntry = null,

    /// Frees this node and every older node it chains to. Returns the
    /// number of nodes freed, so a caller tracking how many *superseded*
    /// versions remain outstanding (`SharedRedoOverlay.pending_retirement_versions`)
    /// can keep that count accurate without a second walk.
    pub fn destroyChain(head: *VersionedOverlayEntry, allocator: Allocator) usize {
        var current: ?*VersionedOverlayEntry = head;
        var freed: usize = 0;
        while (current) |node| {
            const next = node.older;
            allocator.free(node.value);
            allocator.destroy(node);
            current = next;
            freed += 1;
        }
        return freed;
    }

    /// The newest version in this chain whose `sequence` is `<=
    /// at_or_before`, or null if even the oldest version postdates it
    /// (meaning the caller must fall through to the real tree, which is
    /// the correct source of truth for any point before this overlay held
    /// anything for this key at all).
    pub fn resolve(head: *VersionedOverlayEntry, at_or_before: u64) ?*VersionedOverlayEntry {
        var current: ?*VersionedOverlayEntry = head;
        while (current) |node| {
            if (node.sequence <= at_or_before) return node;
            current = node.older;
        }
        return null;
    }
};

const OverlayMap = std.StringHashMapUnmanaged(*VersionedOverlayEntry);

/// Ordered key index, one per root, mirroring exactly the key set each
/// `OverlayMap` holds (same owned key bytes -- a node's `.key` is the
/// identical slice the hashmap already owns, never a second copy).
/// `OverlayMap` alone answers "what is key K's current chain" in O(1);
/// it cannot answer "what is the smallest key >= K" without scanning
/// every entry, which is exactly the O(n log n) `keysWithPrefixAlloc`
/// (dupe every match, then sort) used to cost on every cursor open
/// (docs/design/lite-performance/lite-single-commit-write-path.md,
/// perf/lwa-overlay-order). A treap gives seek-at-or-after/-before and
/// prefix iteration in O(log n) (+ O(k) to walk k results), maintained
/// incrementally: `appendVersion`/`decodeLogicalMutationsPayload` insert
/// a node the first time a key appears in this window; `reclaim`'s own
/// whole-key drop and `removeLastVersion`'s abort path remove it. The
/// version chain itself (`maps[root]`'s value) is unaffected -- this
/// indexes the key *set*, not any particular version.
const OrderedTreap = std.Treap([]const u8, struct {
    fn compare(a: []const u8, b: []const u8) std.math.Order {
        return std.mem.order(u8, a, b);
    }
}.compare);
const OrderedNode = OrderedTreap.Node;

/// Smallest node at or after (or strictly after, if `strict`) `key`,
/// starting the walk from `start` (a root pointer) -- O(log n) expected,
/// mirroring `docstore.zig`'s `Cursor.overlayAtOrAfter` (same algorithm,
/// generalized with `strict` since that one call site never needed it).
fn orderedLowerBound(start: ?*OrderedNode, key: []const u8, strict: bool) ?*OrderedNode {
    var node = start;
    var result: ?*OrderedNode = null;
    while (node) |current| {
        const order = std.mem.order(u8, current.key, key);
        if (order == .gt or (!strict and order == .eq)) {
            result = current;
            node = current.children[0];
        } else node = current.children[1];
    }
    return result;
}

/// Largest node at or before (or strictly before, if `strict`) `key`.
/// Mirrors `docstore.zig`'s `Cursor.overlayAtOrBefore`.
fn orderedUpperBound(start: ?*OrderedNode, key: []const u8, strict: bool) ?*OrderedNode {
    var node = start;
    var result: ?*OrderedNode = null;
    while (node) |current| {
        const order = std.mem.order(u8, current.key, key);
        if (order == .lt or (!strict and order == .eq)) {
            result = current;
            node = current.children[1];
        } else node = current.children[0];
    }
    return result;
}

/// Smallest node whose key starts with `prefix`, or null if none --
/// the classic BST "find leftmost key matching this prefix" descent: a
/// node matching the prefix is a candidate (keep looking left for a
/// smaller match); a node sorting before the prefix can only have
/// matches (if any) to its right; a node sorting after it (and not
/// matching) can only have matches (if any) to its left.
fn orderedFirstWithPrefix(start: ?*OrderedNode, prefix: []const u8) ?*OrderedNode {
    var node = start;
    var result: ?*OrderedNode = null;
    while (node) |current| {
        if (std.mem.startsWith(u8, current.key, prefix)) {
            result = current;
            node = current.children[0];
        } else if (std.mem.order(u8, current.key, prefix) == .lt) {
            node = current.children[1];
        } else {
            node = current.children[0];
        }
    }
    return result;
}

/// Largest node whose key starts with `prefix`, or null if none --
/// mirror image of `orderedFirstWithPrefix`.
fn orderedLastWithPrefix(start: ?*OrderedNode, prefix: []const u8) ?*OrderedNode {
    var node = start;
    var result: ?*OrderedNode = null;
    while (node) |current| {
        if (std.mem.startsWith(u8, current.key, prefix)) {
            result = current;
            node = current.children[1];
        } else if (std.mem.order(u8, current.key, prefix) == .lt) {
            node = current.children[1];
        } else {
            node = current.children[0];
        }
    }
    return result;
}

/// Three roots' worth of versioned overlay, plus the bookkeeping needed to
/// reclaim superseded versions safely. Shared across every `NativeFile`
/// instance (writer or reader) open on the same inode in this process --
/// see this module's own doc comment. `mutex` guards every field: a reader
/// resolving a key can run concurrently with the writer appending a new
/// version for a different key, but never with a concurrent mutation of
/// the same map (a plain mutex, not a reader/writer lock, matching
/// `inode_lock.Entry.mutex`'s own posture -- commits are not frequent
/// enough relative to reads for this to be a real bottleneck, and
/// correctness is far easier to reason about with one lock than with a
/// read/write split here).
pub const SharedRedoOverlay = struct {
    mutex: std.Io.Mutex = .init,
    maps: [3]OverlayMap = .{ .empty, .empty, .empty },
    /// Ordered key index per root -- see this file's own doc comment on
    /// `OrderedTreap` above. Kept in lockstep with `maps`: every key
    /// `maps[root]` holds has exactly one node here, sharing the same
    /// owned key bytes (freed once, via `maps`' own cleanup).
    ordered: [3]OrderedTreap = .{ .{}, .{}, .{} },
    /// Page-shaped writes not yet reflected on disk (external value
    /// chains only -- see `RedoCommitState`'s own doc comment in
    /// native.zig for why only those, never B-tree pages, ever need this),
    /// keyed by page id. Shared for the same reason `maps` is: a reader's
    /// own, unshared copy would never see a page the live writer staged
    /// here after the reader opened. Reclaimed in whole at checkpoint
    /// time (`clearPages`), same as the pre-sharing design did -- a
    /// written-once, never-updated-in-place chain has no "older version a
    /// pinned reader might still need" to preserve individually.
    pages: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    /// The last checkpoint `checkpointRedoLog` durably published: any
    /// version with `sequence <= durable_checkpoint_sequence` is also
    /// already reflected in real, physical pages, so it survives in this
    /// overlay only for as long as some pin still needs it as its own
    /// resolved value (see `reclaim`).
    durable_checkpoint_sequence: u64 = 0,
    /// The highest floor `reclaim` has collapsed versions against. A view
    /// whose mark is below it may already have lost versions it needed
    /// (they were dropped or folded into a checkpoint it cannot see), so
    /// `pinUnlessReclaimed` refuses it rather than let it read a gap.
    reclaimed_through: u64 = 0,
    /// Every currently live reader's pinned sequence, across every
    /// `NativeFile`/`ReadGeneration` referencing this overlay -- the
    /// in-process half of `minimumPinnedSequence`, unified here so an
    /// uncoordinated reader (no `ReaderCoordinator` participation) still
    /// counts, not just one with a region-visible epoch slot. One entry
    /// per live pin (not deduplicated by value): `unpin` removes exactly
    /// the one instance `pin` added, the same shape a multiset needs.
    pins: std.ArrayListUnmanaged(u64) = .empty,
    /// §9 (redo retirement signal): the number of versions currently
    /// chained as `.older` somewhere in `maps` -- i.e. versions a newer
    /// write in this same uncheckpointed window has already superseded,
    /// across every key and root. Each is a version `checkpointRedoLog`'s
    /// next fold would discover and retire (its own external-value chain
    /// included, per that function's "page-shaped exception" doc comment)
    /// that the on-disk allocator ledger cannot see yet, because retiring
    /// a chain is something only the real B-tree fold does today -- the
    /// ledger has no independent way to learn "this chain is now garbage"
    /// before that fold runs. `appendVersion` increments this by exactly
    /// one whenever it pushes a version onto an *existing* chain (never
    /// for a key's first version in this window -- that one supersedes
    /// nothing); `reclaim` decrements it by however many superseded
    /// versions a floor advance actually frees. O(1) per commit: a plain
    /// counter, no scan, piggybacking on `appendVersion`'s own
    /// `getOrPut` and `reclaim`'s own walk -- see
    /// `NativeFile.redoCheckpointWouldRetire`, the only reader.
    pending_retirement_versions: u64 = 0,

    pub fn deinit(self: *SharedRedoOverlay, allocator: Allocator) void {
        for (&self.maps, 0..) |*map, root_index| {
            const root: u8 = @intCast(root_index);
            var it = map.iterator();
            while (it.next()) |kv| {
                self.destroyOrderedKey(allocator, root, kv.key_ptr.*);
                allocator.free(kv.key_ptr.*);
                _ = kv.value_ptr.*.destroyChain(allocator);
            }
            map.deinit(allocator);
        }
        self.ordered = .{ .{}, .{}, .{} };
        var pages_it = self.pages.valueIterator();
        while (pages_it.next()) |bytes| allocator.free(bytes.*);
        self.pages.deinit(allocator);
        self.pins.deinit(allocator);
        self.* = undefined;
    }

    /// Removes `key`'s node from `ordered[root]` and frees it -- never
    /// the key bytes themselves, which `maps[root]` owns and frees on
    /// its own schedule (`ordered`'s nodes borrow that same slice; see
    /// this file's own `OrderedTreap` doc comment). A no-op if `key`
    /// has no node (should not happen given the two structures are kept
    /// in lockstep, but a missing node is harmless to tolerate here).
    /// Callers hold `self.mutex` already (or, from `deinit`, have
    /// exclusive access with nothing left to synchronize against).
    fn destroyOrderedKey(self: *SharedRedoOverlay, allocator: Allocator, root: u8, key: []const u8) void {
        var entry = self.ordered[root].getEntryFor(key);
        if (entry.node) |node| {
            entry.set(null);
            allocator.destroy(node);
        }
    }

    /// Stages (or replaces) a page-shaped write, keyed by page id. Takes
    /// ownership of `bytes`.
    pub fn appendPage(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, page_id: u64, bytes: []u8) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (try self.pages.fetchPut(allocator, page_id, bytes)) |old| allocator.free(old.value);
    }

    /// Returns a *copy* of the staged page's bytes, or null if nothing is
    /// staged for `page_id`. A copy, not a borrowed slice, for the same
    /// reason `resolve` returns one: the caller must not retain a
    /// reference across a later `unlock`.
    pub fn readPageAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, page_id: u64) !?[]u8 {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const bytes = self.pages.get(page_id) orelse return null;
        return try allocator.dupe(u8, bytes);
    }

    /// Cheap, non-allocating presence check for a multi-page read-ahead
    /// window deciding where to split a physical read: true if `page_id`
    /// has a staged raw page right now. Never copies -- unlike
    /// `readPageAlloc`, this exists purely so a caller walking a window
    /// to find the boundary of a run of consecutive non-overlay pages
    /// can check many page ids without paying for a copy it would
    /// immediately discard for every page that turns out not to be
    /// staged.
    pub fn hasPage(self: *SharedRedoOverlay, io: std.Io, page_id: u64) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.pages.contains(page_id);
    }

    /// True when nothing is staged at all right now -- the common case
    /// once every diverted page has been folded or flushed. Lets a
    /// multi-page window skip a per-page overlay check entirely when
    /// there is provably nothing to find anywhere, preserving the
    /// single-pread cost a read-ahead window exists for in the first
    /// place (the point of the window at all, on the legacy -- non-redo
    /// -- path, and on a redo path once nothing is left diverted).
    pub fn pagesEmpty(self: *SharedRedoOverlay, io: std.Io) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.pages.count() == 0;
    }

    /// One staged page, fully owned, returned by `snapshotAllPagesAlloc`.
    pub const OwnedPage = struct {
        page_id: u64,
        bytes: []u8,
    };

    /// Returns a fully-owned *copy* of every currently staged page,
    /// without removing any of them from the overlay -- see
    /// `removePages`'s own doc comment for why a caller that is about to
    /// flush these to their real, physical locations must defer removal
    /// until after that write durably lands, instead of removing them
    /// first (what this used to do, under the name `takeAllPagesAlloc`,
    /// before a reader was found reaching `readBackingPages` for exactly
    /// such a page in the window between the old remove-then-write and
    /// the write's own completion: the overlay already said "not staged,"
    /// and the real location did not have it yet either -- neither on
    /// disk nor rebuilt from the log, for the span of that one caller's
    /// own flush loop).
    pub fn snapshotAllPagesAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io) ![]OwnedPage {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var list = try std.ArrayListUnmanaged(OwnedPage).initCapacity(allocator, self.pages.count());
        errdefer {
            for (list.items) |p| allocator.free(p.bytes);
            list.deinit(allocator);
        }
        var it = self.pages.iterator();
        while (it.next()) |kv| list.appendAssumeCapacity(.{ .page_id = kv.key_ptr.*, .bytes = try allocator.dupe(u8, kv.value_ptr.*) });
        return try list.toOwnedSlice(allocator);
    }

    /// Removes exactly the given page ids from the overlay, once their
    /// content is durably reflected at their own real, physical location
    /// (a caller that has just finished writing every page
    /// `snapshotAllPagesAlloc` returned). Unlike `maps`' pin-aware
    /// `reclaim`, a page has no "older version some reader might still
    /// need" to preserve once its real location holds the same bytes --
    /// this removes unconditionally, pins or not, the same posture
    /// `takeAllPagesAlloc` always had for pages, just moved to run after
    /// the physical write instead of before it. Every caller of this
    /// function and `snapshotAllPagesAlloc` already runs under the
    /// store's own single-writer exclusion for the whole span between
    /// them, so no other writer can have restaged a page this call is
    /// about to remove with different content in between.
    pub fn removePages(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, flushed: []const OwnedPage) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (flushed) |p| {
            if (self.pages.fetchRemove(p.page_id)) |kv| allocator.free(kv.value);
        }
    }

    /// Drops every staged page, once a checkpoint has folded them all
    /// into real, physical pages -- but only when no pin is live. Unlike
    /// `maps`' per-version `reclaim`, this is whole-overlay, not
    /// per-page: an external-value-chain page may be referenced by a
    /// logical version a pin still needs (`reclaim` keeps that version
    /// precisely for this reason), and this module does not track which
    /// pages a kept version references, so the conservative, always-safe
    /// choice is to keep every staged page as long as *any* pin is live,
    /// even one unrelated to this particular page -- a bounded, temporary
    /// memory cost while a pin outlives a checkpoint, never a correctness
    /// risk. A no-op whenever a pin exists; call it after every
    /// checkpoint regardless; it only actually clears once pins drain to
    /// none.
    pub fn maybeClearPages(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.pins.items.len != 0) return;
        var it = self.pages.valueIterator();
        while (it.next()) |bytes| allocator.free(bytes.*);
        self.pages.clearAndFree(allocator);
    }

    /// Pushes a new version onto `key`'s chain for `root` (0=metadata,
    /// 1=index, 2=documents -- `TransactionWrites.Root`'s own backing
    /// ints, kept as a plain `u8` here to avoid importing native.zig's
    /// enum). Takes ownership of `value` (already caller-allocated with
    /// the same allocator this overlay uses throughout); duplicates `key`
    /// only the first time it's seen for this root.
    pub fn appendVersion(
        self: *SharedRedoOverlay,
        allocator: Allocator,
        io: std.Io,
        root: u8,
        key: []const u8,
        sequence: u64,
        value: []u8,
        is_delete: bool,
        external_value_root_page: u64,
        external_value_len: usize,
    ) !void {
        std.debug.assert(root < 3);
        const node = try allocator.create(VersionedOverlayEntry);
        errdefer allocator.destroy(node);
        node.* = .{
            .sequence = sequence,
            .value = value,
            .is_delete = is_delete,
            .external_value_root_page = external_value_root_page,
            .external_value_len = external_value_len,
        };
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const gop = try self.maps[root].getOrPut(allocator, key);
        if (gop.found_existing) {
            node.older = gop.value_ptr.*;
            // The previous head is now superseded (chained as `.older`):
            // one more version `reclaim` will eventually free and that a
            // fold would discover as garbage. A key's *first* version in
            // this window (the `else` branch below) supersedes nothing.
            self.pending_retirement_versions += 1;
        } else {
            const owned_key = try allocator.dupe(u8, key);
            errdefer allocator.free(owned_key);
            const ordered_node = try allocator.create(OrderedNode);
            errdefer allocator.destroy(ordered_node);
            var ordered_entry = self.ordered[root].getEntryFor(owned_key);
            ordered_entry.set(ordered_node);
            gop.key_ptr.* = owned_key;
        }
        gop.value_ptr.* = node;
    }

    /// Undoes a single, still-in-flight transaction's own append onto
    /// `key`'s chain under `root` (perf/lwa-txn-overlay: transaction
    /// isolation) -- the version `appendVersion` most recently pushed for
    /// this key, restoring the chain to whatever it superseded (or
    /// removing the key outright if this was its only version). Used only
    /// to unwind a transaction's own not-yet-published mutation on abort
    /// or savepoint rollback, never a version some other, already-
    /// published commit owns: safe under the single-writer model, since
    /// nothing else can have appended for the same key while this
    /// transaction is still open, so the current head is always exactly
    /// the node this call means to remove. A no-op if nothing is staged
    /// for `key` (a savepoint opened before this key was ever touched).
    pub fn removeLastVersion(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, key: []const u8) void {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const slot = self.maps[root].getPtr(key) orelse return;
        const node = slot.*;
        if (node.older) |older| {
            slot.* = older;
            // This append superseded `older` (`appendVersion`'s own
            // `pending_retirement_versions` bump for that case); undoing
            // the append un-supersedes it.
            self.pending_retirement_versions -|= 1;
        } else if (self.maps[root].fetchRemove(key)) |kv| {
            self.destroyOrderedKey(allocator, root, kv.key);
            allocator.free(kv.key);
        }
        allocator.free(node.value);
        allocator.destroy(node);
    }

    /// Discards one page-shaped write this process itself staged and
    /// never published -- the `pages`-side analogue of `removeLastVersion`,
    /// used for the same abort/savepoint-rollback undo. A no-op if
    /// nothing is staged for `page_id`.
    pub fn discardPage(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, page_id: u64) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.pages.fetchRemove(page_id)) |kv| allocator.free(kv.value);
    }

    /// The number of superseded-but-not-yet-reclaimed versions across
    /// every key and root -- see this struct's own field doc comment.
    /// Nonzero means a fold (`checkpointRedoLog`) would find at least one
    /// chain to retire that the allocator ledger does not know about yet.
    pub fn pendingRetirementVersions(self: *SharedRedoOverlay, io: std.Io) u64 {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.pending_retirement_versions;
    }

    /// Resolves `root`/`key` as of `at_or_before` (a reader's own pinned
    /// `commit_sequence`). Returns an *owned copy* of the resolved
    /// version's value (allocated with `allocator`, which may differ from
    /// whatever this overlay's own internal nodes are allocated with --
    /// e.g. a caller's own short-lived allocator, not `std.heap.page_allocator`),
    /// not a pointer or borrowed slice into the chain: a reference
    /// borrowed across the `unlock` below would be a real use-after-free
    /// risk once this overlay is shared across threads/instances -- a
    /// concurrent `reclaim` on another thread could free the exact node
    /// this call resolved to the moment the mutex releases, even within
    /// the same statement's own evaluation order.
    pub fn resolveAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, key: []const u8, at_or_before: u64) !?VersionedOverlayEntry {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const head = self.maps[root].getPtr(key) orelse return null;
        const e = head.*.resolve(at_or_before) orelse return null;
        return .{
            .sequence = e.sequence,
            .value = try allocator.dupe(u8, e.value),
            .is_delete = e.is_delete,
            .external_value_root_page = e.external_value_root_page,
            .external_value_len = e.external_value_len,
        };
    }

    /// Registers a live pin at `sequence`, counted toward this overlay's
    /// own reclaim floor regardless of whether the pinning reader also
    /// participates in the cross-process `ReaderCoordinator`/region
    /// protocol -- unifying the in-process half of `minimumPinnedSequence`
    /// so an uncoordinated reader counts too (this overlay is never
    /// visible cross-process at all, so only the in-process half is
    /// relevant to it in the first place).
    pub fn pin(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, sequence: u64) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        try self.pins.append(allocator, sequence);
    }

    /// `pin`, but refuses a mark `reclaim` has already collapsed past:
    /// the check and the pin share one lock, so no reclaim can slip
    /// between them.
    pub fn pinUnlessReclaimed(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, sequence: u64) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (sequence < self.reclaimed_through) return error.ReadSnapshotReclaimed;
        try self.pins.append(allocator, sequence);
    }

    /// The number of distinct keys currently holding at least one version
    /// for `root` (a key count, not a version count -- matches
    /// `std.HashMap.count`'s own meaning). Used by `check()`'s
    /// `record_count` accounting; see `NativeFile.redoOverlayEntryCount`.
    pub fn count(self: *SharedRedoOverlay, io: std.Io, root: u8) usize {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.maps[root].count();
    }

    /// A sorted, owned snapshot of every key under `root` that starts
    /// with `prefix` -- a *copy* of each key's bytes, not a borrowed
    /// slice into the live map: like `resolveAlloc`, holding a reference
    /// into the shared chain past this call's own lock would be a
    /// use-after-free risk once another thread can concurrently mutate
    /// or reclaim it. The caller frees each key and the returned slice
    /// (`allocator.free` on each element, then on the slice itself).
    ///
    /// O(log n + k) (k = the number of matching keys): seeks straight to
    /// the first match via `ordered[root]` (§ this file's own `OrderedTreap`
    /// doc comment) and walks forward only through the matches, which are
    /// already in order -- no scan of the other `n - k` keys, no sort.
    /// Previously O(n log n): iterated every key in the (unordered)
    /// hashmap checking `startsWith`, then sorted whatever matched. That
    /// cost -- `SharedRedoOverlay.keysWithPrefixAlloc` plus the
    /// `std.sort.block` after it -- is what every merged-cursor open used
    /// to pay regardless of how narrow a scan's own prefix was (the
    /// unordered map gives no way to skip non-matching keys), growing
    /// with total overlay size rather than result size.
    pub fn keysWithPrefixAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, prefix: []const u8) ![][]u8 {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var list = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (list.items) |k| allocator.free(k);
            list.deinit(allocator);
        }
        var node = orderedFirstWithPrefix(self.ordered[root].root, prefix);
        while (node) |n| {
            try list.append(allocator, try allocator.dupe(u8, n.key));
            const next = n.next();
            node = if (next != null and std.mem.startsWith(u8, next.?.key, prefix)) next else null;
        }
        return try list.toOwnedSlice(allocator);
    }

    /// Smallest key under `root` that starts with `prefix`, or null if
    /// none -- O(log n), the single-result sibling of
    /// `keysWithPrefixAlloc` for a caller that only needs the first
    /// match (e.g. an existence probe) and must not pay to materialize
    /// every match.
    pub fn firstKeyWithPrefixAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, prefix: []const u8) !?[]u8 {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const node = orderedFirstWithPrefix(self.ordered[root].root, prefix) orelse return null;
        return try allocator.dupe(u8, node.key);
    }

    /// Largest key under `root` that starts with `prefix`, or null if
    /// none -- O(log n), symmetric with `firstKeyWithPrefixAlloc`.
    pub fn lastKeyWithPrefixAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, prefix: []const u8) !?[]u8 {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const node = orderedLastWithPrefix(self.ordered[root].root, prefix) orelse return null;
        return try allocator.dupe(u8, node.key);
    }

    /// Smallest key under `root` that is `>= key` (or `> key` if
    /// `strict`) and starts with `prefix`, or null if none -- O(log n).
    /// Correct whenever the caller's own `key` is itself within (at or
    /// after the start of) the `prefix` bucket, which every call site
    /// in this codebase satisfies (`prefix` itself, or a key already
    /// known to match it): the matching keys form one contiguous range
    /// in sorted order, so once the raw (prefix-unaware) lower bound for
    /// `key` stops matching `prefix`, nothing further in sorted order
    /// can match it either.
    pub fn seekKeyAtOrAfterAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, prefix: []const u8, key: []const u8, strict: bool) !?[]u8 {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const node = orderedLowerBound(self.ordered[root].root, key, strict) orelse return null;
        if (!std.mem.startsWith(u8, node.key, prefix)) return null;
        return try allocator.dupe(u8, node.key);
    }

    /// Largest key under `root` that is `<= key` (or `< key` if
    /// `strict`) and starts with `prefix`, or null if none -- O(log n).
    /// Same contiguous-range reasoning as `seekKeyAtOrAfterAlloc`,
    /// mirrored.
    pub fn seekKeyAtOrBeforeAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, prefix: []const u8, key: []const u8, strict: bool) !?[]u8 {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const node = orderedUpperBound(self.ordered[root].root, key, strict) orelse return null;
        if (!std.mem.startsWith(u8, node.key, prefix)) return null;
        return try allocator.dupe(u8, node.key);
    }

    /// One key's current (head/newest) version, fully owned -- returned
    /// by `snapshotHeadsAlloc` for `checkpointRedoLog` to fold into the
    /// real tree outside this overlay's own lock (a B-tree build is not
    /// something to do while holding a lock every reader also needs).
    pub const OwnedHead = struct {
        key: []u8,
        value: []u8,
        is_delete: bool,
        external_value_root_page: u64,
        external_value_len: usize,
    };

    /// A snapshot of every key's current head version under `root` whose
    /// own `sequence` is strictly newer than `min_sequence_exclusive`,
    /// fully owned (both key and value duped) so the caller can use it
    /// after this call's own lock releases. The caller frees each
    /// `.key`/`.value` and the returned slice.
    ///
    /// `min_sequence_exclusive` matters whenever `reclaim` has been unable
    /// to drop a key's entry after an earlier `checkpointRedoLog` fold
    /// already applied it: `reclaim`'s own floor is the *oldest live pin*,
    /// not "has this version already been folded" -- a pin older than a
    /// key's entire chain (predating that key's first write in this
    /// window entirely) leaves the chain's every node newer than the
    /// floor, so `reclaim` finds nothing `<= floor` to collapse onto and
    /// never removes the key at all, even though `durable_checkpoint_
    /// sequence` already covers it. Without this filter, a *later*
    /// `checkpointRedoLog` call (an explicit checkpoint, or the ride-along
    /// idle turn) would see that same already-folded head again and
    /// re-apply it -- for an external value chain specifically, handing
    /// its already-referenced `external_value_root_page` to a *second*,
    /// newly built real record for the same key, so two distinct document/
    /// catalog records end up referencing the same chain. `check()`'s
    /// reachability walk (`markReachablePage`'s own duplicate detection)
    /// catches the result as `invalid_page_chain`; the indexed-ledger path
    /// double-books the same page's ownership, surfacing as a reopen that
    /// cannot find a page the ledger still expects. The caller passes its
    /// own `RedoCommitState.checkpoint_sequence` as captured *before* this
    /// fold publishes a new one -- exactly the boundary every version at
    /// or below it was already batched into.
    pub fn snapshotHeadsAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, min_sequence_exclusive: u64) ![]OwnedHead {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var list = try std.ArrayListUnmanaged(OwnedHead).initCapacity(allocator, self.maps[root].count());
        errdefer {
            for (list.items) |item| {
                allocator.free(item.key);
                allocator.free(item.value);
            }
            list.deinit(allocator);
        }
        var it = self.maps[root].iterator();
        while (it.next()) |kv| {
            const head = kv.value_ptr.*;
            if (head.sequence <= min_sequence_exclusive) continue;
            list.appendAssumeCapacity(.{
                .key = try allocator.dupe(u8, kv.key_ptr.*),
                .value = try allocator.dupe(u8, head.value),
                .is_delete = head.is_delete,
                .external_value_root_page = head.external_value_root_page,
                .external_value_len = head.external_value_len,
            });
        }
        return try list.toOwnedSlice(allocator);
    }

    /// One version `NativeFile.checkpointRedoLog`'s fold finds superseded
    /// (not the current head) within its own window -- see that
    /// function's `redo_fold_retire_pending` field (native.zig) for the
    /// orphaned-page-retention bug this exists to fix: a diverting
    /// commit's page-shaped exception (an external value chain, or a
    /// catalog value's direct-written one) was retained once the next
    /// fold ran, but never associated with the key/version that actually
    /// owned it, so a page belonging to a version superseded or deleted
    /// before that fold ever ran was retained forever instead of being
    /// retired. Carries only what `NativeFile.retainUnretainedValueChainPages`
    /// needs to walk and retire the chain; the version's own key/value
    /// bytes are irrelevant here -- nothing ever applies a superseded
    /// version to the real tree.
    pub const SupersededChain = struct {
        external_value_root_page: u64,
        external_value_len: usize,
    };

    /// A snapshot of every version under `root` that this same fold's own
    /// `snapshotHeadsAlloc(root, min_sequence_exclusive)` call will *not*
    /// return as a head -- i.e. every node strictly newer than
    /// `min_sequence_exclusive` in a key's chain except the newest one --
    /// that still carries its own page-shaped external value chain
    /// (`external_value_root_page != 0`). Each one is a version a later
    /// write in this same uncheckpointed window already superseded or
    /// deleted before any fold ever applied it to the real tree, so
    /// nothing else will ever retire its chain -- see `SupersededChain`'s
    /// own doc comment and `NativeFile.checkpointRedoLog`'s "page-shaped
    /// exception" doc comment.
    ///
    /// Walking a chain stops the moment a node's own `sequence` drops to
    /// `min_sequence_exclusive` or below: everything from there down was
    /// already folded (and, if page-shaped, already retired) by an
    /// earlier `checkpointRedoLog` call -- the same boundary
    /// `snapshotHeadsAlloc` itself uses to skip an already-folded head
    /// (see its own doc comment), so a version is never discovered as
    /// "superseded" by more than one fold.
    ///
    /// The caller frees the returned slice -- no per-element allocation,
    /// since `SupersededChain` is a plain value, unlike `OwnedHead`.
    pub fn snapshotSupersededChainsAlloc(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, root: u8, min_sequence_exclusive: u64) ![]SupersededChain {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var list: std.ArrayListUnmanaged(SupersededChain) = .empty;
        errdefer list.deinit(allocator);
        var it = self.maps[root].iterator();
        while (it.next()) |kv| {
            var node: ?*VersionedOverlayEntry = kv.value_ptr.*;
            var is_head = true;
            while (node) |n| {
                if (n.sequence <= min_sequence_exclusive) break;
                if (!is_head and n.external_value_root_page != 0) {
                    try list.append(allocator, .{
                        .external_value_root_page = n.external_value_root_page,
                        .external_value_len = n.external_value_len,
                    });
                }
                is_head = false;
                node = n.older;
            }
        }
        return try list.toOwnedSlice(allocator);
    }

    /// Removes exactly one live pin at `sequence` (the one `pin` added),
    /// not every pin that happens to share that value.
    pub fn unpin(self: *SharedRedoOverlay, io: std.Io, sequence: u64) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (self.pins.items, 0..) |value, i| {
            if (value == sequence) {
                _ = self.pins.swapRemove(i);
                return;
            }
        }
    }

    /// The oldest live pin's sequence, or null if there are none (nothing
    /// currently restricts reclaim beyond `durable_checkpoint_sequence`
    /// itself).
    fn minPinLocked(self: *const SharedRedoOverlay) ?u64 {
        var min: ?u64 = null;
        for (self.pins.items) |value| {
            if (min == null or value < min.?) min = value;
        }
        return min;
    }

    /// Drops every version that is both (a) covered by `durable_sequence`
    /// (a checkpoint has already folded it into real, physical pages) and
    /// (b) strictly older than every live pin (so no pin would ever need
    /// to fall back to it -- a pin exactly *at* a version's sequence still
    /// needs that exact version, hence `<`, not `<=`, against the floor).
    /// When no pin is live at all, the key itself is dropped once its
    /// chain collapses to a single, checkpoint-covered version: nothing
    /// could ever resolve to it instead of the real tree (which, as of
    /// the checkpoint that just folded it, already has the same value),
    /// so retaining it would only grow this map forever across a long
    /// run with no readers. Call this after `checkpointRedoLog` publishes
    /// a new durable checkpoint, and again whenever a pin releases (a
    /// newly-advanced floor may make more versions reclaimable).
    pub fn reclaim(self: *SharedRedoOverlay, allocator: Allocator, io: std.Io, durable_sequence: u64) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.durable_checkpoint_sequence = @max(self.durable_checkpoint_sequence, durable_sequence);
        const min_pin = self.minPinLocked();
        const floor = @min(self.durable_checkpoint_sequence, min_pin orelse self.durable_checkpoint_sequence);
        self.reclaimed_through = @max(self.reclaimed_through, floor);
        for (&self.maps, 0..) |*map, root_index| {
            const root: u8 = @intCast(root_index);
            // Collected rather than removed mid-iteration: this map's
            // iterator does not tolerate a concurrent `remove`/
            // `fetchRemove` of the entry it is currently visiting.
            var drop_keys: std.ArrayListUnmanaged([]const u8) = .empty;
            defer drop_keys.deinit(allocator);
            var it = map.iterator();
            while (it.next()) |kv| {
                // Keep the newest version with `sequence <= floor` (some
                // pin at exactly `floor` may still need it); drop
                // everything strictly older than that one, which no live
                // pin can ever resolve to instead.
                const original_head = kv.value_ptr.*;
                var node: ?*VersionedOverlayEntry = original_head;
                var kept: ?*VersionedOverlayEntry = null;
                while (node) |n| {
                    if (n.sequence <= floor) {
                        kept = n;
                        break;
                    }
                    node = n.older;
                }
                if (kept) |keep_node| {
                    if (keep_node.older) |stale| {
                        keep_node.older = null;
                        // `stale` is exactly the superseded run
                        // `appendVersion` counted one-by-one as it grew;
                        // freeing it here is the only place that run ever
                        // shrinks, so mirror the count back out.
                        self.pending_retirement_versions -|= stale.destroyChain(allocator);
                    }
                    // Dropping the whole key (map pointer included) is only
                    // safe when `keep_node` *is* the live head -- i.e. the
                    // head itself is also `<= floor`, so the entire chain
                    // is durable. When the walk had to step past the head
                    // to find something `<= floor` (the head is still
                    // ahead of `floor`: genuinely live, uncheckpointed
                    // data no fold has written to a real page yet), the
                    // map's own pointer (`kv.value_ptr.*`) still names that
                    // live head, never `keep_node` -- dropping the key
                    // here would call `destroyChain` on that live head
                    // below (`removed.value` is always `kv.value_ptr.*`,
                    // not `keep_node`), permanently destroying a committed
                    // write no fold ever folded. Reproduced live: a
                    // background `checkpointRedoLog` fold's own commit
                    // bumps the shared `commit_sequence` counter the same
                    // way every commit does, so a *second*, unrelated
                    // write that lands after the fold started but before
                    // it finished can end up chained on top of a key the
                    // fold already folded -- its own `appendVersion` push
                    // makes it the new head, newer than the fold's own
                    // `durable_checkpoint_sequence`. A reader-pin release
                    // racing in with `min_pin == null` at that exact
                    // moment (any unpin opportunistically re-applies the
                    // existing floor) then found `kept` at the older,
                    // already-folded version and dropped the whole key --
                    // erasing the newer, never-folded write with no way to
                    // recover it (`storage.lite.conformance_test` and the
                    // externally-locked reclaim test both hit this
                    // through the full redo retirement signal starting
                    // the background maintenance loop while a sibling
                    // write was still in flight).
                    if (min_pin == null and keep_node == original_head) drop_keys.append(allocator, kv.key_ptr.*) catch continue;
                } else {
                    // Every version in this chain postdates the floor --
                    // nothing to drop (every version is still potentially
                    // the right answer for some pin, or none have been
                    // checkpointed yet).
                }
            }
            for (drop_keys.items) |key| {
                if (map.fetchRemove(key)) |removed| {
                    self.destroyOrderedKey(allocator, root, removed.key);
                    allocator.free(removed.key);
                    // `removed.value` is `keep_node` itself (its own
                    // `.older` was already severed and separately
                    // accounted for above) -- it was never counted as
                    // superseded, so its own removal here does not
                    // decrement `pending_retirement_versions`.
                    _ = removed.value.destroyChain(allocator);
                }
            }
        }
    }

    /// The number of pages currently staged (page-shaped writes not yet
    /// reflected on disk). See `pages`' own doc comment.
    pub fn pagesCount(self: *SharedRedoOverlay, io: std.Io) usize {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.pages.count();
    }

    /// True if any key under `root` other than one starting with
    /// `excluded_prefix` resolves, at `at_or_before`, to a live
    /// (non-delete) version. Used by `NativeFile.hasLiveDocumentOutsidePrefix`'s
    /// "the overlay might hold a document never yet reflected in the
    /// tree at all" check -- a plain boolean read under the lock, no
    /// allocation needed since nothing escapes it.
    pub fn hasLiveKeyOutsidePrefix(self: *SharedRedoOverlay, io: std.Io, root: u8, at_or_before: u64, excluded_prefix: []const u8) bool {
        std.debug.assert(root < 3);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var it = self.maps[root].iterator();
        while (it.next()) |kv| {
            if (std.mem.startsWith(u8, kv.key_ptr.*, excluded_prefix)) continue;
            const resolved = kv.value_ptr.*.resolve(at_or_before) orelse continue;
            if (!resolved.is_delete) return true;
        }
        return false;
    }

    /// Decodes one commit's logical-mutation section
    /// (`[root:u8][key_len:u16][key][flags:u8][...]`) back into
    /// `(root, key) -> version chain` pairs, pushing a new version (tagged
    /// with `sequence`, the record's own `commit_sequence`) onto whatever
    /// chain already exists for that `(root, key)`. Used only during
    /// initial population from the log (no concurrent reader can reach
    /// this overlay yet at that point), so it writes the maps directly
    /// rather than through `appendVersion`'s locking.
    pub fn decodeLogicalMutationsPayload(self: *SharedRedoOverlay, allocator: Allocator, payload: []const u8, sequence: u64) !void {
        var pos: usize = 0;
        while (pos < payload.len) {
            if (pos + 3 > payload.len) return error.InvalidNativeRedoRecord;
            const root = payload[pos];
            if (root >= 3) return error.InvalidNativeRedoRecord;
            const key_len = std.mem.readInt(u16, payload[pos + 1 ..][0..2], .little);
            pos += 3;
            if (pos + key_len + 1 > payload.len) return error.InvalidNativeRedoRecord;
            const key = payload[pos..][0..key_len];
            pos += key_len;
            const flags = payload[pos];
            pos += 1;
            var decoded: VersionedOverlayEntry = .{ .sequence = sequence, .is_delete = flags & 1 != 0 };
            if (flags & 2 != 0) {
                if (pos + 16 > payload.len) return error.InvalidNativeRedoRecord;
                decoded.external_value_root_page = std.mem.readInt(u64, payload[pos..][0..8], .little);
                decoded.external_value_len = std.mem.readInt(u64, payload[pos + 8 ..][0..8], .little);
                pos += 16;
            } else {
                if (pos + 4 > payload.len) return error.InvalidNativeRedoRecord;
                const value_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;
                if (pos + value_len > payload.len) return error.InvalidNativeRedoRecord;
                decoded.value = try allocator.dupe(u8, payload[pos..][0..value_len]);
                pos += value_len;
            }
            errdefer allocator.free(decoded.value);
            const node = try allocator.create(VersionedOverlayEntry);
            errdefer allocator.destroy(node);
            node.* = decoded;
            const gop = try self.maps[root].getOrPut(allocator, key);
            if (gop.found_existing) {
                node.older = gop.value_ptr.*;
                // Mirror `appendVersion`'s own accounting: chaining onto an
                // existing head means that head is now superseded, exactly
                // the same retirement debt a live commit's `appendVersion`
                // counts. Replay (this function) previously left this debt
                // invisible to `pendingRetirementVersions`/`redoCheckpointWouldRetire`
                // after a reopen, even though the chain it rebuilds is
                // byte-for-byte what a live session would have produced.
                self.pending_retirement_versions += 1;
            } else {
                const owned_key = try allocator.dupe(u8, key);
                errdefer allocator.free(owned_key);
                const ordered_node = try allocator.create(OrderedNode);
                errdefer allocator.destroy(ordered_node);
                var ordered_entry = self.ordered[root].getEntryFor(owned_key);
                ordered_entry.set(ordered_node);
                gop.key_ptr.* = owned_key;
            }
            gop.value_ptr.* = node;
        }
    }

    /// Decodes one commit's page-shaped section
    /// (`[page_id:u64][len:u32][bytes]`, `RedoCommitState.appendPending`'s
    /// own shape) back into `pages`, replacing (and freeing) any earlier
    /// entry for the same page id. Used only during initial population
    /// from the log, same posture as `decodeLogicalMutationsPayload`.
    pub fn decodeRedoGroupPayload(self: *SharedRedoOverlay, allocator: Allocator, payload: []const u8) !void {
        var pos: usize = 0;
        while (pos < payload.len) {
            if (pos + 12 > payload.len) return error.InvalidNativeRedoRecord;
            const page_id = std.mem.readInt(u64, payload[pos..][0..8], .little);
            const len = std.mem.readInt(u32, payload[pos + 8 ..][0..4], .little);
            pos += 12;
            if (pos + len > payload.len) return error.InvalidNativeRedoRecord;
            const owned = try allocator.dupe(u8, payload[pos..][0..len]);
            errdefer allocator.free(owned);
            pos += len;
            if (try self.pages.fetchPut(allocator, page_id, owned)) |old| allocator.free(old.value);
        }
    }
};

test "redo overlay versioned resolve sees the newest version at or before a pinned sequence" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    try overlay.appendVersion(a, std.testing.io, 2, "k", 1, try a.dupe(u8, "v1"), false, 0, 0);
    try overlay.appendVersion(a, std.testing.io, 2, "k", 2, try a.dupe(u8, "v2"), false, 0, 0);

    const at_1 = (try overlay.resolveAlloc(a, std.testing.io, 2, "k", 1)).?;
    defer a.free(at_1.value);
    try std.testing.expectEqualStrings("v1", at_1.value);
    const at_2 = (try overlay.resolveAlloc(a, std.testing.io, 2, "k", 2)).?;
    defer a.free(at_2.value);
    try std.testing.expectEqualStrings("v2", at_2.value);
    const at_5 = (try overlay.resolveAlloc(a, std.testing.io, 2, "k", 5)).?;
    defer a.free(at_5.value);
    try std.testing.expectEqualStrings("v2", at_5.value);
    try std.testing.expectEqual(@as(?VersionedOverlayEntry, null), try overlay.resolveAlloc(a, std.testing.io, 2, "k", 0));
    try std.testing.expectEqual(@as(?VersionedOverlayEntry, null), try overlay.resolveAlloc(a, std.testing.io, 2, "missing", 5));
}

test "redo overlay reclaim keeps versions a live pin still needs and drops the rest" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    try overlay.appendVersion(a, std.testing.io, 2, "k", 1, try a.dupe(u8, "v1"), false, 0, 0);
    try overlay.appendVersion(a, std.testing.io, 2, "k", 2, try a.dupe(u8, "v2"), false, 0, 0);
    try overlay.appendVersion(a, std.testing.io, 2, "k", 3, try a.dupe(u8, "v3"), false, 0, 0);

    // A reader pinned at sequence 2 must survive a reclaim that already
    // covers everything up to 3: v1 (strictly older than the floor, 2)
    // is reclaimable, but v2 (the floor itself) must stay, since the
    // pinned reader resolves to exactly it.
    try overlay.pin(a, std.testing.io, 2);
    overlay.reclaim(a, std.testing.io, 3);
    {
        const at_2 = (try overlay.resolveAlloc(a, std.testing.io, 2, "k", 2)).?;
        defer a.free(at_2.value);
        try std.testing.expectEqualStrings("v2", at_2.value);
    }
    {
        const at_3 = (try overlay.resolveAlloc(a, std.testing.io, 2, "k", 3)).?;
        defer a.free(at_3.value);
        try std.testing.expectEqualStrings("v3", at_3.value);
    }
    // v1 is gone, but that's only observable indirectly (resolving at 1
    // now finds nothing, since the pin at 2 was the only reason to keep
    // anything below the floor, and 1 < floor).
    try std.testing.expectEqual(@as(?VersionedOverlayEntry, null), try overlay.resolveAlloc(a, std.testing.io, 2, "k", 1));

    // Once the pin releases, with no live pin left at all, the key is
    // dropped entirely rather than retained forever: nothing could ever
    // resolve to it instead of the real tree, which (in the real system,
    // outside this isolated unit test) already has the same value as of
    // the checkpoint that covers it.
    overlay.unpin(std.testing.io, 2);
    overlay.reclaim(a, std.testing.io, 3);
    try std.testing.expectEqual(@as(?VersionedOverlayEntry, null), try overlay.resolveAlloc(a, std.testing.io, 2, "k", 3));
    try std.testing.expectEqual(@as(usize, 0), overlay.count(std.testing.io, 2));
}

// perf/lwa-overlay-order: tests for the ordered key index (`ordered`,
// `orderedLowerBound`/`orderedUpperBound`/`orderedFirstWithPrefix`/
// `orderedLastWithPrefix`, and the `*KeyWithPrefixAlloc`/`seekKey*Alloc`/
// `keysWithPrefixAlloc` methods built on them).

test "redo overlay ordered index: keysWithPrefixAlloc returns matches in sorted order regardless of insertion order" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    // Inserted out of sorted order on purpose.
    for ([_][]const u8{ "p/charlie", "p/alpha", "other/zz", "p/echo", "p/bravo", "other/aa", "p/delta" }, 1..) |key, seq| {
        try overlay.appendVersion(a, std.testing.io, 2, key, seq, try a.dupe(u8, "v"), false, 0, 0);
    }

    const keys = try overlay.keysWithPrefixAlloc(a, std.testing.io, 2, "p/");
    defer {
        for (keys) |k| a.free(k);
        a.free(keys);
    }
    const expected = [_][]const u8{ "p/alpha", "p/bravo", "p/charlie", "p/delta", "p/echo" };
    try std.testing.expectEqual(expected.len, keys.len);
    for (expected, keys) |want, got| try std.testing.expectEqualStrings(want, got);
}

test "redo overlay ordered index: first/last/seek-at-or-after/seek-at-or-before agree with a plain sort" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    for ([_][]const u8{ "m/5", "m/1", "m/9", "m/3", "m/7" }, 1..) |key, seq| {
        try overlay.appendVersion(a, std.testing.io, 0, key, seq, try a.dupe(u8, "v"), false, 0, 0);
    }

    {
        const first = (try overlay.firstKeyWithPrefixAlloc(a, std.testing.io, 0, "m/")).?;
        defer a.free(first);
        try std.testing.expectEqualStrings("m/1", first);
    }
    {
        const last = (try overlay.lastKeyWithPrefixAlloc(a, std.testing.io, 0, "m/")).?;
        defer a.free(last);
        try std.testing.expectEqualStrings("m/9", last);
    }
    // Non-strict at-or-after an existing key returns that key; strict
    // skips to the next one.
    {
        const at = (try overlay.seekKeyAtOrAfterAlloc(a, std.testing.io, 0, "m/", "m/3", false)).?;
        defer a.free(at);
        try std.testing.expectEqualStrings("m/3", at);
    }
    {
        const after = (try overlay.seekKeyAtOrAfterAlloc(a, std.testing.io, 0, "m/", "m/3", true)).?;
        defer a.free(after);
        try std.testing.expectEqualStrings("m/5", after);
    }
    // Seeking after a key between two real keys lands on the next real
    // one -- the point of an ordered index over an unordered map.
    {
        const between = (try overlay.seekKeyAtOrAfterAlloc(a, std.testing.io, 0, "m/", "m/4", false)).?;
        defer a.free(between);
        try std.testing.expectEqualStrings("m/5", between);
    }
    {
        const before = (try overlay.seekKeyAtOrBeforeAlloc(a, std.testing.io, 0, "m/", "m/3", false)).?;
        defer a.free(before);
        try std.testing.expectEqualStrings("m/3", before);
    }
    {
        const strictly_before = (try overlay.seekKeyAtOrBeforeAlloc(a, std.testing.io, 0, "m/", "m/3", true)).?;
        defer a.free(strictly_before);
        try std.testing.expectEqualStrings("m/1", strictly_before);
    }
    // Past either end: nothing.
    try std.testing.expectEqual(@as(?[]u8, null), try overlay.seekKeyAtOrAfterAlloc(a, std.testing.io, 0, "m/", "m/9", true));
    try std.testing.expectEqual(@as(?[]u8, null), try overlay.seekKeyAtOrBeforeAlloc(a, std.testing.io, 0, "m/", "m/1", true));
}

test "redo overlay ordered index: a prefix scan never crosses into a neighboring prefix's keys" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    // "n/" sorts strictly between "m/" and "o/" -- both neighbors exist
    // on either side so a lower/upper-bound walk that forgot to check
    // the prefix at all would wrongly wander into them.
    for ([_][]const u8{ "m/zzz", "n/b", "n/a", "o/aaa" }, 1..) |key, seq| {
        try overlay.appendVersion(a, std.testing.io, 1, key, seq, try a.dupe(u8, "v"), false, 0, 0);
    }

    const keys = try overlay.keysWithPrefixAlloc(a, std.testing.io, 1, "n/");
    defer {
        for (keys) |k| a.free(k);
        a.free(keys);
    }
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("n/a", keys[0]);
    try std.testing.expectEqualStrings("n/b", keys[1]);

    try std.testing.expectEqual(@as(?[]u8, null), try overlay.seekKeyAtOrAfterAlloc(a, std.testing.io, 1, "n/", "n/b", true));
    try std.testing.expectEqual(@as(?[]u8, null), try overlay.seekKeyAtOrBeforeAlloc(a, std.testing.io, 1, "n/", "n/a", true));

    const first = (try overlay.firstKeyWithPrefixAlloc(a, std.testing.io, 1, "n/")).?;
    defer a.free(first);
    try std.testing.expectEqualStrings("n/a", first);
    const last = (try overlay.lastKeyWithPrefixAlloc(a, std.testing.io, 1, "n/")).?;
    defer a.free(last);
    try std.testing.expectEqualStrings("n/b", last);

    // An empty prefix (an unscoped scan) sees everything, in order.
    const everything = try overlay.keysWithPrefixAlloc(a, std.testing.io, 1, "");
    defer {
        for (everything) |k| a.free(k);
        a.free(everything);
    }
    const expected_all = [_][]const u8{ "m/zzz", "n/a", "n/b", "o/aaa" };
    try std.testing.expectEqual(expected_all.len, everything.len);
    for (expected_all, everything) |want, got| try std.testing.expectEqualStrings(want, got);
}

test "redo overlay ordered index: removeLastVersion (abort) drops only its own key from the index" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    try overlay.appendVersion(a, std.testing.io, 2, "d/1", 1, try a.dupe(u8, "v1"), false, 0, 0);
    try overlay.appendVersion(a, std.testing.io, 2, "d/2", 2, try a.dupe(u8, "v2"), false, 0, 0);
    try overlay.appendVersion(a, std.testing.io, 2, "d/3", 3, try a.dupe(u8, "v3"), false, 0, 0);

    // Undo "d/2"'s own (only) version, as an aborted transaction would.
    overlay.removeLastVersion(a, std.testing.io, 2, "d/2");

    const keys = try overlay.keysWithPrefixAlloc(a, std.testing.io, 2, "d/");
    defer {
        for (keys) |k| a.free(k);
        a.free(keys);
    }
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("d/1", keys[0]);
    try std.testing.expectEqualStrings("d/3", keys[1]);
    try std.testing.expectEqual(@as(usize, 2), overlay.count(std.testing.io, 2));

    // Seeking at-or-after the removed key's own position now lands on
    // its successor, not on a stale entry.
    const after = (try overlay.seekKeyAtOrAfterAlloc(a, std.testing.io, 2, "d/", "d/2", false)).?;
    defer a.free(after);
    try std.testing.expectEqualStrings("d/3", after);
}

test "redo overlay ordered index: reclaim's whole-key drop removes it from the index too" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    try overlay.appendVersion(a, std.testing.io, 2, "r/one", 1, try a.dupe(u8, "v1"), false, 0, 0);
    try overlay.appendVersion(a, std.testing.io, 2, "r/two", 2, try a.dupe(u8, "v2"), false, 0, 0);
    // Overwrite "r/two" so it has a superseded version to collapse, not
    // just a single untouched one -- exercises reclaim's real drop path
    // rather than a no-op.
    try overlay.appendVersion(a, std.testing.io, 2, "r/two", 3, try a.dupe(u8, "v2-updated"), false, 0, 0);

    // No pin at all: a reclaim covering everything drops "r/one" and
    // "r/two" entirely (nothing could ever need the overlay for them
    // again, by this module's own "no live pin" rule).
    overlay.reclaim(a, std.testing.io, 3);

    try std.testing.expectEqual(@as(usize, 0), overlay.count(std.testing.io, 2));
    const keys = try overlay.keysWithPrefixAlloc(a, std.testing.io, 2, "r/");
    defer {
        for (keys) |k| a.free(k);
        a.free(keys);
    }
    try std.testing.expectEqual(@as(usize, 0), keys.len);
    try std.testing.expectEqual(@as(?[]u8, null), try overlay.firstKeyWithPrefixAlloc(a, std.testing.io, 2, "r/"));
    try std.testing.expectEqual(@as(?[]u8, null), try overlay.lastKeyWithPrefixAlloc(a, std.testing.io, 2, "r/"));

    // Interleave more inserts after the drop: the index must still be
    // internally consistent (no stale node left behind confusing a
    // later lookup/insert at the same key).
    try overlay.appendVersion(a, std.testing.io, 2, "r/one", 4, try a.dupe(u8, "v1-again"), false, 0, 0);
    const resolved = (try overlay.resolveAlloc(a, std.testing.io, 2, "r/one", 4)).?;
    defer a.free(resolved.value);
    try std.testing.expectEqualStrings("v1-again", resolved.value);
    const first = (try overlay.firstKeyWithPrefixAlloc(a, std.testing.io, 2, "r/")).?;
    defer a.free(first);
    try std.testing.expectEqualStrings("r/one", first);
}

test "redo overlay ordered index: a concurrent reader's prefix scans stay well-formed while a writer appends and reclaims" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    // A pin at sequence 0, held for the whole test: `reclaim`'s own
    // floor is `min(durable_checkpoint_sequence, min_pin)` (see its own
    // doc comment), so with a pin at 0 nothing with `sequence >= 1`
    // (every real version here) is ever collapsed or dropped --
    // `reclaim` still runs its full locked walk on every call below,
    // genuinely interleaving with the reader's own lookups, just
    // without changing the key set, so the final counts below are
    // deterministic regardless of how the two threads happened to
    // interleave.
    try overlay.pin(a, std.testing.io, 0);
    defer overlay.unpin(std.testing.io, 0);

    const total: u64 = 300;
    const Writer = struct {
        overlay: *SharedRedoOverlay,
        allocator: std.mem.Allocator,
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.work() catch |err| {
                self.failure = err;
            };
            self.done.store(true, .release);
        }

        fn work(self: *@This()) !void {
            var seq: u64 = 1;
            while (seq <= total) : (seq += 1) {
                var buf: [16]u8 = undefined;
                const key = try std.fmt.bufPrint(&buf, "c/{d:0>8}", .{seq});
                try self.overlay.appendVersion(self.allocator, std.testing.io, 2, key, seq, try self.allocator.dupe(u8, "v"), false, 0, 0);
                if (seq % 10 == 0) {
                    // No pin is ever held in this test, so this collapses
                    // every key to its single (never-superseded) live
                    // version -- exercises `reclaim`'s own tree-walk
                    // running concurrently with the reader's lookups
                    // below without ever actually dropping a key (there
                    // is nothing to drop until the writer stops touching
                    // a key further), still a real interleaving of the
                    // two structures' own locked sections.
                    self.overlay.reclaim(self.allocator, std.testing.io, seq);
                }
            }
        }
    };
    var writer: Writer = .{ .overlay = &overlay, .allocator = a };

    const Reader = struct {
        overlay: *SharedRedoOverlay,
        allocator: std.mem.Allocator,
        writer_done: *std.atomic.Value(bool),
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.work() catch |err| {
                self.failure = err;
            };
        }

        fn work(self: *@This()) !void {
            while (!self.writer_done.load(.acquire)) {
                const keys = try self.overlay.keysWithPrefixAlloc(self.allocator, std.testing.io, 2, "c/");
                defer {
                    for (keys) |k| self.allocator.free(k);
                    self.allocator.free(keys);
                }
                var previous: ?[]const u8 = null;
                for (keys) |k| {
                    try std.testing.expect(std.mem.startsWith(u8, k, "c/"));
                    if (previous) |p| try std.testing.expect(std.mem.order(u8, p, k) == .lt);
                    previous = k;
                }

                var cursor: ?[]u8 = try self.overlay.firstKeyWithPrefixAlloc(self.allocator, std.testing.io, 2, "c/");
                var count: usize = 0;
                while (cursor) |c| {
                    defer self.allocator.free(c);
                    try std.testing.expect(std.mem.startsWith(u8, c, "c/"));
                    count += 1;
                    if (count > total + 1) return error.UnboundedOverlayWalk;
                    cursor = try self.overlay.seekKeyAtOrAfterAlloc(self.allocator, std.testing.io, 2, "c/", c, true);
                }
            }
        }
    };
    var reader: Reader = .{ .overlay = &overlay, .allocator = a, .writer_done = &writer.done };

    const reader_thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    const writer_thread = try std.Thread.spawn(.{}, Writer.run, .{&writer});
    writer_thread.join();
    reader_thread.join();

    try std.testing.expectEqual(@as(?anyerror, null), writer.failure);
    try std.testing.expectEqual(@as(?anyerror, null), reader.failure);
    try std.testing.expectEqual(total, overlay.count(std.testing.io, 2));
    const final_keys = try overlay.keysWithPrefixAlloc(a, std.testing.io, 2, "c/");
    defer {
        for (final_keys) |k| a.free(k);
        a.free(final_keys);
    }
    try std.testing.expectEqual(@as(usize, total), final_keys.len);
}

test "redo overlay microbenchmark: a narrow prefix probe against 10k entries (the hasGraphEndpointCleanup/hasPrefixTxn shape)" {
    const a = std.testing.allocator;
    var overlay: SharedRedoOverlay = .{};
    defer overlay.deinit(a);

    // 10k primary-write-shaped keys diverted into the overlay, none of
    // them under the probe's own prefix below -- exactly
    // `DocStore.hasGraphEndpointCleanup`'s real-world shape: a primary-
    // only write path diverts its document keys into the documents
    // root, and the graph endpoint cleanup queue (checked on every
    // commit via `hasPrefixTxn`) has no entries at all in that same
    // root. Before perf/lwa-overlay-order, `keysWithPrefixAlloc` (and so
    // every cursor open through it) paid for all 10k regardless of the
    // probe's own prefix matching none of them.
    const total: u64 = 10_000;
    var seq: u64 = 1;
    while (seq <= total) : (seq += 1) {
        var buf: [24]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "\x02db/doc/{d:0>10}", .{seq});
        try overlay.appendVersion(a, std.testing.io, 2, key, seq, try a.dupe(u8, "v"), false, 0, 0);
    }

    const started = std.Io.Clock.awake.now(std.testing.io);
    const iterations: usize = 500;
    for (0..iterations) |_| {
        const keys = try overlay.keysWithPrefixAlloc(a, std.testing.io, 2, "\x00\x00graph_endpoint_cleanup/");
        a.free(keys);
    }
    const elapsed_ns: u64 = @intCast(@max(0, started.durationTo(std.Io.Clock.awake.now(std.testing.io)).nanoseconds));
    std.debug.print(
        "OVERLAY_PREFIX_PROBE total_entries={d} iterations={d} ns_per_probe={d}\n",
        .{ total, iterations, elapsed_ns / iterations },
    );
}
