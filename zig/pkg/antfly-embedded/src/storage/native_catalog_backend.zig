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

//! A `backend_erased.NamespaceStore`-conformant main backend whose reads are
//! served from an in-process ordered-tree mirror (the same snapshot-isolated
//! structure `mem_backend.zig` uses -- this file mirrors its `NamespaceTxn`
//! closely), but whose writes are durably committed through an injected
//! `Sink`: exactly one `Sink.submit` call per `NamespaceTxn.commit()`,
//! covering every key written during that transaction.
//!
//! This file has no Lite-specific (`lite/*`) or native-`.aflite`-specific
//! imports: it only knows about `Sink`, a small `*anyopaque` + function-
//! pointer interface. The concrete Sink that durably persists mutations as
//! native index-catalog records (`lite/native.zig`'s `putIndexCatalogRecord*`
//! family, via `lite/docstore.zig`'s group-commit queue) lives in
//! `lite/native_catalog_sink.zig`, so this file keeps compiling (and
//! staying inert) for the hosted `antfly` build, which has no `lite/` tree.
//!
//! Used as `PersistentIndex`'s `.native_catalog` `MainBackend` (see
//! `persistent.zig`). The point: one `MainTxn.commit()` -- which through the
//! `.lsm` backend costs several LSM-WAL-backed native commits via
//! `lite/index_storage.zig`'s virtual filesystem -- costs exactly one
//! durable native commit here, regardless of how many keys it touches.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const Allocator = std.mem.Allocator;
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const ordered = @import("mem_ordered.zig");
// Diagnostic-only (test-build-gated); see write_amplification_trace.zig.
// Pushed here, once, so every `.native_catalog`-backed consumer --
// `persistent.zig`'s text index today, `sparse/sparse.zig`'s sparse index
// as of WS4-for-vectors -- gets the same `.main_txn_commit` attribution
// without its own call site. Safe to nest under a caller's own
// `wal_trace.scope(.main_txn_commit)` (e.g. `persistent.zig`'s `MainTxn.
// commit`): pushing the same tag twice is a no-op past the first push.
const wal_trace = @import("write_amplification_trace.zig");

/// One key's value in a namespace, as loaded from the durable sink.
/// `namespace == null` means the unnamed/default namespace.
pub const Entry = struct {
    namespace: ?[]const u8,
    key: []const u8,
    value: []const u8,
};

/// One mutation applied during a transaction, replayed into the sink at
/// commit time, in the order `put`/`delete` were called.
pub const Mutation = struct {
    namespace: ?[]const u8,
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
};

/// An outstanding, not-yet-waited-on commit of a `.ride_along` `Sink.submit`
/// (see `Sink.submitDeferred`). Opaque: `Backend` never needs to know what
/// concrete thing `ptr` is (a `lite/docstore.zig` `Store.Ticket`'s container,
/// for `native_catalog_sink.zig`'s implementation), only that `wait()` must
/// run exactly once, eventually, to free it and observe its error (if any).
/// This is what lets several ride-along `NamespaceTxn.commit()`s -- e.g. one
/// per background pass of a multi-pass maintenance sweep -- each return
/// immediately (this backend's own in-memory mirror is already updated by
/// the time `commit()` returns, independent of this) instead of each
/// blocking on its own durability barrier; see `Backend.pending_ride_along`.
pub const Pending = struct {
    ptr: *anyopaque,
    waitFn: *const fn (ptr: *anyopaque) anyerror!void,

    pub fn wait(self: Pending) anyerror!void {
        return self.waitFn(self.ptr);
    }
};

/// Durable storage underlying this backend's in-memory mirror. Implemented
/// concretely by `lite/native_catalog_sink.zig` against a Lite `.aflite`
/// file's native index-catalog records. A single `Sink` value may be shared
/// by every `PersistentIndex` in one Lite DB (it wraps one `docstore.Store`);
/// `scope` disambiguates which index's records a call addresses, so the
/// sink's own durable keys never collide across indexes sharing one `.aflite`
/// file's native index-catalog root.
pub const Sink = struct {
    ptr: *anyopaque,
    /// Apply every mutation in `mutations`, in order, as ONE native
    /// transaction (one `syncData`/checkpoint publication if `durable`, or
    /// a visible-but-not-yet-synced ride-along commit otherwise -- see
    /// `NamespaceTxn.setDurable`), or none of them on failure. `mutations`
    /// is only valid for the duration of the call.
    submit: *const fn (ptr: *anyopaque, scope: []const u8, mutations: []const Mutation, durable: bool) anyerror!void,
    /// Loads every entry currently committed under this sink's scope, to
    /// seed the in-memory mirror on open. Called once, synchronously, from
    /// `Backend.init`. The returned slice and every entry's bytes are
    /// allocated with the given allocator and freed by the caller
    /// immediately after use.
    loadAllAlloc: *const fn (ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]Entry,
    /// Like loadAllAlloc, with empty values allocated by the same allocator.
    loadKeysAlloc: ?*const fn (ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]Entry = null,
    deleteScope: ?*const fn (ptr: *anyopaque, scope: []const u8) anyerror!void = null,
    /// Optional (`null` for a `Sink` with no deferred path, e.g. the test
    /// fakes below): like `submit`, but for `durable = false` only, and
    /// never blocks waiting for the underlying group commit to apply --
    /// `mutations` is deep-copied before this returns, so the caller's own
    /// buffers (`NamespaceTxn.pending`'s arena) may be freed immediately
    /// after. Returns a `Pending` the caller must eventually `wait()` on
    /// (directly, or via `Backend.pending_ride_along`'s batching) to avoid
    /// leaking the underlying group-commit request and to observe a late
    /// failure. See the design doc's §3.2a "group membership, not
    /// standalone commits": this is what lets `NamespaceTxn.commit()` join
    /// the next group-commit queue instead of leading (and closing) its own
    /// one-member group every time, without adding a wait the caller doesn't
    /// need yet.
    submitDeferred: ?*const fn (ptr: *anyopaque, scope: []const u8, mutations: []const Mutation) anyerror!Pending = null,
    /// perf/lwa-maint-serialize: optional (`null` for a `Sink` with no
    /// background maintenance of its own, e.g. the test fakes below).
    /// Brackets a caller-driven sequence of more than one `submit`/commit
    /// that must land as one atomic-looking unit from background
    /// maintenance's point of view -- see `HBCIndex
    /// .pauseMaintenanceAcrossRemoval`'s own doc comment for the one
    /// caller (#1216's native-catalog index delete: a durable scope-clear
    /// `submit`, then a separate catalog-removal commit through a
    /// different store entirely). `pauseMaintenance` must be called
    /// before the first of those commits and `resumeMaintenance` after
    /// the last; calls must be paired and never nested.
    pauseMaintenance: ?*const fn (ptr: *anyopaque) void = null,
    resumeMaintenance: ?*const fn (ptr: *anyopaque) void = null,
};

/// Durably removes every entry currently committed under `scope`, so a
/// later `Backend.init` for the same scope -- e.g. after the index that
/// owned it is deleted and a differently-configured index of the same name
/// is re-admitted -- starts from nothing instead of resurrecting the
/// deleted generation's stale records (dims, root node, postings, ...).
/// `durable = true`: the caller (index removal) must call this, and let it
/// complete, before committing the catalog mutation that makes the index's
/// removal visible, so a crash between the two can never leave the catalog
/// saying the index is gone while its stale scope survives underneath a
/// later re-admission. A no-op when nothing is committed under `scope`.
pub fn deleteAllForScope(allocator: Allocator, sink: Sink, scope: []const u8) !void {
    if (sink.deleteScope) |delete_scope| return delete_scope(sink.ptr, scope);
    var deletes = try ScopeDeletes.load(allocator, sink, scope);
    defer deletes.deinit();
    if (deletes.mutations.len == 0) return;
    try sink.submit(sink.ptr, scope, deletes.mutations, true);
}

/// A delete for every record committed under `scope`, for a caller that
/// commits them in its own transaction (index removal, alongside the catalog
/// mutation that makes the removal visible: neither half can land alone).
pub const ScopeDeletes = struct {
    allocator: Allocator,
    scope: []const u8,
    loaded: []Entry,
    mutations: []Mutation,

    pub fn load(allocator: Allocator, sink: Sink, scope: []const u8) !ScopeDeletes {
        const loaded = try (sink.loadKeysAlloc orelse sink.loadAllAlloc)(sink.ptr, allocator, scope);
        errdefer freeLoaded(allocator, loaded);
        const mutations = try allocator.alloc(Mutation, loaded.len);
        for (loaded, 0..) |entry, i| {
            mutations[i] = .{ .namespace = entry.namespace, .key = entry.key, .is_delete = true };
        }
        return .{ .allocator = allocator, .scope = scope, .loaded = loaded, .mutations = mutations };
    }

    pub fn deinit(self: *ScopeDeletes) void {
        self.allocator.free(self.mutations);
        freeLoaded(self.allocator, self.loaded);
        self.* = undefined;
    }

    fn freeLoaded(allocator: Allocator, loaded: []Entry) void {
        for (loaded) |entry| {
            if (entry.namespace) |n| allocator.free(n);
            allocator.free(entry.key);
            allocator.free(entry.value);
        }
        allocator.free(loaded);
    }
};

const State = struct {
    tree: ordered.Tree = .empty,

    fn deinit(self: *State, allocator: Allocator) void {
        self.tree.release(allocator);
        self.* = .{};
    }

    // O(1) snapshot: the persistent tree shares structure with the source.
    fn clone(self: *const State) State {
        return .{ .tree = self.tree.snapshot() };
    }

    fn get(self: *const State, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return self.tree.get(namespace.name, key) orelse error.NotFound;
    }

    fn put(self: *State, allocator: Allocator, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        const next = try self.tree.put(allocator, namespace.name, key, value);
        self.tree.release(allocator);
        self.tree = next;
    }

    fn delete(self: *State, allocator: Allocator, namespace: backend_types.Namespace, key: []const u8) !void {
        const next = try self.tree.remove(allocator, namespace.name, key);
        self.tree.release(allocator);
        self.tree = next;
    }
};

const RcState = struct {
    refs: usize,
    state: State,
};

fn retainState(rc: *RcState) void {
    rc.refs += 1;
}

fn releaseState(allocator: Allocator, rc: *RcState) void {
    rc.refs -= 1;
    if (rc.refs == 0) {
        rc.state.deinit(allocator);
        allocator.destroy(rc);
    }
}

pub const Options = struct {
    sink: Sink,
    /// Disambiguates this backend's records from any other `PersistentIndex`
    /// sharing the same `Sink` (and therefore the same underlying native
    /// index-catalog root). Copied; the caller's `scope` need not outlive
    /// this call.
    scope: []const u8,
    backend: backend_types.OpenOptions = .{},
};

pub const Backend = struct {
    serialized_write_mutex: std.atomic.Mutex = .unlocked,
    allocator: Allocator,
    sink: Sink,
    scope: []const u8,
    open_options: backend_types.OpenOptions,
    state: *RcState,
    mutex: std.atomic.Mutex = .unlocked,
    /// Set when an externally committed write could not be applied to the
    /// in-memory mirror (`invalidateMirror`): the mirror no longer matches
    /// the durable catalog, so every later transaction fails closed instead
    /// of reading stale state or computing writes from it. Reopening the
    /// index rebuilds the mirror from the committed catalog.
    mirror_invalid: std.atomic.Value(bool) = .init(false),
    /// Ride-along commits (`NamespaceTxn.commit()` with `durable = false`)
    /// staged via `sink.submitDeferred` but not yet waited on (design doc
    /// §3.2a: background work rides along and is crash-recomputable, but a
    /// `Pending` still owns a heap-allocated group-commit request that
    /// must eventually be freed, and a late failure must eventually
    /// surface). Drained opportunistically -- never on a count or byte
    /// threshold of its own, which would just reintroduce the per-pass
    /// durability-barrier wait this exists to avoid; see
    /// `enqueuePendingRideAlong`. Writers to this backend are already
    /// serialized by the caller's own per-index lock
    /// (`capabilities().single_writer`), so this needs no lock of its own.
    pending_ride_along: std.ArrayListUnmanaged(Pending) = .empty,

    pub fn init(allocator: Allocator, options: Options) !Backend {
        const scope = try allocator.dupe(u8, options.scope);
        errdefer allocator.free(scope);

        var tree: ordered.Tree = .empty;
        errdefer tree.release(allocator);

        // `loadAllAlloc` hands back allocator-owned entries (namespace, key
        // and value each separately allocated); `tree.put` dupes its own
        // copies, so every entry's bytes must be freed here once copied in,
        // on every path (including an error partway through the loop).
        const loaded = try options.sink.loadAllAlloc(options.sink.ptr, allocator, scope);
        defer allocator.free(loaded);
        var freed: usize = 0;
        defer for (loaded[freed..]) |entry| {
            if (entry.namespace) |n| allocator.free(n);
            allocator.free(entry.key);
            allocator.free(entry.value);
        };
        for (loaded) |entry| {
            const next = try tree.put(allocator, entry.namespace, entry.key, entry.value);
            tree.release(allocator);
            tree = next;
            if (entry.namespace) |n| allocator.free(n);
            allocator.free(entry.key);
            allocator.free(entry.value);
            freed += 1;
        }

        const rc = try allocator.create(RcState);
        rc.* = .{ .refs = 1, .state = .{ .tree = tree } };
        return .{
            .scope = scope,
            .allocator = allocator,
            .sink = options.sink,
            .open_options = options.backend,
            .state = rc,
        };
    }

    pub fn close(self: *Backend) void {
        // Close is one of §3.2a's durability barriers: never leave a
        // ride-along `Pending` un-waited (and so its group-commit request
        // heap-leaked) just because nothing else happened to drain it.
        // Any error from a ride-along commit is unrecoverable at this point
        // (the work is designed to be safe to lose) -- it was already
        // logged/surfaced, if at all, inside `wait()`'s own implementation.
        self.drainPendingRideAlong() catch {};
        self.pending_ride_along.deinit(self.allocator);
        lockBackend(self);
        releaseState(self.allocator, self.state);
        self.mutex.unlock();
        self.allocator.free(self.scope);
        self.* = undefined;
    }

    /// Stash a ride-along commit's `Pending` for later, batched `wait()`ing
    /// (design doc §3.2a). Deliberately never drains itself on a count (or
    /// any other) threshold: with no concurrent foreground group and the
    /// idle-flush timer not yet due, nothing but that timer (or a future
    /// durable commit) ever resolves a ride-along ticket, so a `wait()`
    /// here would block for however long that takes -- re-imposing, just
    /// at a coarser granularity, the exact per-pass durability-barrier
    /// wait this exists to avoid (caught by
    /// "lite graph metric-build background passes ride along" wanting
    /// zero extra syncs: an earlier version of this function drained every
    /// 8 entries and forced the idle timer to fire mid-sequence). Draining
    /// instead happens only where `wait()` is either free (a just-returned
    /// durable commit on this same backend already covers every earlier
    /// ride-along ticket still in this list -- see `NamespaceTxn.commit`)
    /// or already required (`close`).
    fn enqueuePendingRideAlong(self: *Backend, pending: Pending) !void {
        try self.pending_ride_along.append(self.allocator, pending);
    }

    /// Waits on every accumulated ride-along `Pending`, in enqueue (so
    /// also apply/queue) order, then clears the list regardless of outcome
    /// -- a `Pending` whose `wait()` already ran must never be waited on
    /// again. Returns the first error encountered (after still waiting on
    /// the rest, so nothing downstream of it leaks), matching
    /// `driveMutationCoordinator`'s own "every member equally uncertain on
    /// a shared-commit failure" semantics.
    pub fn drainPendingRideAlong(self: *Backend) !void {
        defer self.pending_ride_along.clearRetainingCapacity();
        var first_err: ?anyerror = null;
        for (self.pending_ride_along.items) |pending| {
            pending.wait() catch |err| {
                if (first_err == null) first_err = err;
            };
        }
        if (first_err) |err| return err;
    }

    pub fn capabilities(_: *Backend) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .native_namespaces = false,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn beginRead(self: *Backend) !NamespaceTxn {
        return try NamespaceTxn.open(self, true);
    }

    pub fn beginWrite(self: *Backend) !NamespaceTxn {
        if (self.open_options.read_only) return error.ReadOnly;
        return try NamespaceTxn.open(self, false);
    }

    pub fn beginBatch(self: *Backend) !NamespaceTxn {
        if (self.open_options.read_only) return error.ReadOnly;
        return try NamespaceTxn.open(self, false);
    }

    /// `options.durability == .ride_along` (see `backend_types.BatchOptions`
    /// and the design doc's §3.2a) marks the resulting transaction's
    /// eventual commit as background work: visible to this backend's own
    /// readers right away, but never forcing its own durable sync -- see
    /// `NamespaceTxn.setDurable`. Every other field of `options` is
    /// currently a no-op here, same as before this method inspected it.
    pub fn beginBatchWithOptions(self: *Backend, options: backend_types.BatchOptions) !NamespaceTxn {
        if (self.open_options.read_only) return error.ReadOnly;
        var txn = try NamespaceTxn.open(self, false);
        if (options.durability == .ride_along) txn.setDurable(false);
        return txn;
    }

    pub fn runtimeNamespaceStore(self: *Backend, allocator: Allocator) !backend_erased.NamespaceStore {
        return try backend_erased.namespaceStoreFrom(allocator, self, backend_types.Namespace, identityNamespace);
    }

    /// A single-namespace, flat (non-namespace-per-call) view of this
    /// backend, for callers that want a plain `backend_erased.Store`/
    /// `Batch` instead of the namespace-per-call `NamespaceStore` above:
    /// `sparse/sparse.zig`'s `SparseIndex` and dense's HBC backend (one
    /// fixed namespace), and `graph/graph.zig`'s `openEdgeStore` (the
    /// unnamed namespace -- forward/reverse adjacency is an entirely flat
    /// keyspace, one `Backend`/scope per store). Mirrors `mem_backend.zig`'s
    /// and `lsm_backend.zig`'s own `runtimeStore`/`BoundStore` shape; unlike
    /// `lsm_backend.zig`'s, this one needs no LSM-specific probe/current-scan/
    /// replay-lane specializations, since this backend has none of those.
    /// Composes `NamespaceTxn` rather than re-deriving its commit/clone-swap
    /// logic.
    pub fn runtimeStore(self: *Backend, allocator: Allocator, namespace: backend_types.Namespace) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, BoundStore{ .backend = self, .namespace = namespace });
    }

    /// Like `runtimeStore(...).beginWrite()`, but returns the concrete
    /// (unerased) write handle instead of `backend_erased.WriteTxn`. For a
    /// caller that needs to inspect this transaction's own staged
    /// `Mutation` list before it commits (an inline publication building a
    /// small, bounded mutation set to fold into an externally-owned native
    /// transaction -- see `graph/graph.zig`'s `prepareInlineEdgePublication`,
    /// mirroring `persistent.zig`'s `prepareInlineSegmentPublication`),
    /// `backend_erased.Batch`'s vtable has no such accessor, so this bypasses
    /// erasure entirely. The caller must `abort()` (never `commit()`) once
    /// it has harvested what it needs -- this never talks to `sink`.
    pub fn boundWrite(self: *Backend, namespace: backend_types.Namespace) !BoundTxn {
        return .{ .inner = try NamespaceTxn.open(self, false), .namespace = namespace };
    }

    const BoundStore = struct {
        backend: *Backend,
        namespace: backend_types.Namespace,

        pub fn writeSerialization(self: *@This()) backend_erased.WriteSerialization {
            return .{ .gate = &self.backend.serialized_write_mutex };
        }

        pub fn capabilities(self: *@This()) backend_types.Capabilities {
            return self.backend.capabilities();
        }

        pub fn beginRead(self: *@This()) !BoundTxn {
            return .{ .inner = try NamespaceTxn.open(self.backend, true), .namespace = self.namespace };
        }

        pub fn beginWrite(self: *@This()) !BoundTxn {
            if (self.backend.open_options.read_only) return error.ReadOnly;
            return .{ .inner = try NamespaceTxn.open(self.backend, false), .namespace = self.namespace };
        }

        pub fn beginBatch(self: *@This()) !BoundTxn {
            return try self.beginWrite();
        }

        /// WS7/§3.2a: `.ride_along` marks this batch's eventual commit as
        /// background work that must never force its own durability
        /// barrier -- see `NamespaceTxn.setDurable`'s doc comment. Mirrors
        /// `persistent.zig`'s direct `txn.setDurableHint(false)` call, but
        /// applied before this handle is erased into `backend_erased.Batch`
        /// (which has no generic durability-hint vtable slot today).
        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !BoundTxn {
            var txn = try self.beginWrite();
            if (options.durability == .ride_along) txn.setDurable(false);
            return txn;
        }
    };

    /// Namespace-bound write/read handle, composing `NamespaceTxn`. Backs
    /// `BoundStore`'s `backend_erased.Store`/`Batch` erasure.
    const BoundTxn = struct {
        inner: NamespaceTxn,
        namespace: backend_types.Namespace,

        pub fn abort(self: *@This()) void {
            self.inner.abort();
        }

        /// Lets `backend_erased.ReadTxn.forkRead` fork graph adjacency reads
        /// (Cypher edge expansion pins one read per statement and forks it
        /// per page).
        pub fn forkBorrowedRead(self: *@This()) !@This() {
            return .{ .inner = try self.inner.forkRead(), .namespace = self.namespace };
        }

        pub fn commit(self: *@This()) !void {
            try self.inner.commit();
        }

        /// See `NamespaceTxn.setDurable`'s WS2/§3.2a ride-along note.
        pub fn setDurable(self: *@This(), durable: bool) void {
            self.inner.setDurable(durable);
        }

        // `anyerror`, not an inferred set: `graph/graph.zig`'s
        // `prepareInlineEdgePublication` calls generic helpers
        // (`readU64OrZero` and friends) directly against this concrete
        // type, bypassing `backend_erased` erasure (whose own vtable
        // function pointers are always `anyerror`-typed, which is why nothing
        // else going through this file hits this). Those helpers' `catch
        // |err| switch (err) { error.NotFound => ..., else => return err }`
        // shape needs a widened set, or `NamespaceTxn.get`'s true inferred
        // set (exactly `error{NotFound}`) makes the `else` prong
        // unreachable -- a compile error, not just a style nit.
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return try self.inner.get(self.namespace, key);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) anyerror!void {
            try self.inner.put(self.namespace, key, value);
        }

        pub fn delete(self: *@This(), key: []const u8) anyerror!void {
            try self.inner.delete(self.namespace, key);
        }

        pub fn openCursor(self: *@This()) !Cursor {
            return try self.inner.openCursor(self.namespace);
        }

        /// This transaction's staged mutations so far, in call order --
        /// valid only until the next `put`/`delete`/`abort`/`commit` call
        /// (the arena backing their bytes is `self.inner`'s own). A
        /// `boundWrite` caller harvesting these to fold into an externally
        /// owned commit must deep-copy before calling `abort()`.
        pub fn pendingMutations(self: *const @This()) []const Mutation {
            return self.inner.pending.items;
        }
    };

    /// WS4 phase 2: bring this backend's in-memory read mirror in sync with
    /// `mutations` that an EXTERNAL native transaction already committed
    /// durably -- bypassing this backend's own `Sink.submit` entirely (see
    /// `lite/docstore.zig`'s `Txn.putIndexCatalogBatch`, which writes the
    /// durable index-catalog records directly, in the same commit as a
    /// primary write). Call only after that external commit has returned
    /// successfully: this never talks to `sink`, so calling it any earlier
    /// would let a reader observe state with no durability behind it yet.
    ///
    /// Not safe to call concurrently with another write against this same
    /// `Backend` (a `NamespaceTxn.commit()`, or another call to this
    /// method): the caller must already hold whatever per-index
    /// serialization lock excludes that (every writer of one
    /// `PersistentIndex`, inline or not, already serializes through one
    /// such lock -- see `IndexManager.lockManagedIndexApply`). This mirrors
    /// `NamespaceTxn.commit`'s own clone-then-swap, without re-deriving the
    /// pending-mutation bookkeeping `NamespaceTxn` needs for its own
    /// (separate) commit.
    /// See `mirror_invalid`.
    pub fn invalidateMirror(self: *Backend) void {
        self.mirror_invalid.store(true, .release);
    }

    pub fn applyExternallyCommittedMutations(self: *Backend, mutations: []const Mutation) !void {
        if (mutations.len == 0) return;
        // A failure below leaves the mirror behind a durable commit.
        errdefer self.invalidateMirror();
        lockBackend(self);
        const current = self.state;
        self.mutex.unlock();

        var cloned = current.state.clone();
        errdefer cloned.deinit(self.allocator);
        for (mutations) |m| {
            const namespace: backend_types.Namespace = .{ .name = m.namespace };
            if (m.is_delete) {
                // Matches `NamespaceTxn.delete`'s own unconditional `try`
                // below: a missing key is not an error for this mirror's
                // backing tree (unlike the real `get`, which does fail
                // with `error.NotFound`).
                try cloned.delete(self.allocator, namespace, m.key);
            } else {
                try cloned.put(self.allocator, namespace, m.key, m.value);
            }
        }

        const new_rc = try self.allocator.create(RcState);
        new_rc.* = .{ .refs = 1, .state = cloned };

        lockBackend(self);
        const old = self.state;
        self.state = new_rc;
        releaseState(self.allocator, old);
        self.mutex.unlock();
    }

    const NamespaceTxn = struct {
        allocator: Allocator,
        backend: *Backend,
        read_only: bool,
        rc: ?*RcState,
        // Durable replay log for this transaction, in call order. Built from
        // a private arena so individual mutation byte slices need no manual
        // freeing; the arena is torn down once, in `teardown`.
        pending: std.ArrayListUnmanaged(Mutation) = .empty,
        pending_arena: std.heap.ArenaAllocator,
        closed: bool = false,
        // See `setDurable`. Defaults to durable: every existing caller that
        // never calls `setDurable(false)` keeps committing durably, exactly
        // as before this field existed.
        durable: bool = true,
        // `get` hands out bytes owned by the current tree. A later `put` or
        // `delete` in this transaction replaces that tree, and releasing it
        // would free a node this transaction wrote earlier and then read
        // back. Trees replaced after such a read stay alive here until the
        // transaction ends, which is as long as a `get` result may be used.
        read_since_write: bool = false,
        borrowed_trees: std.ArrayListUnmanaged(ordered.Tree) = .empty,

        fn open(backend: *Backend, read_only: bool) !NamespaceTxn {
            if (backend.mirror_invalid.load(.acquire)) return error.IndexMirrorInvalid;
            lockBackend(backend);
            defer backend.mutex.unlock();
            const current = backend.state;
            const rc: *RcState = if (read_only) blk: {
                retainState(current);
                break :blk current;
            } else blk: {
                var cloned = current.state.clone();
                errdefer cloned.deinit(backend.allocator);
                _ = &cloned; // clone() does not allocate; keep parity with mem_backend's shape.
                const new_rc = try backend.allocator.create(RcState);
                new_rc.* = .{ .refs = 1, .state = cloned };
                break :blk new_rc;
            };
            return .{
                .allocator = backend.allocator,
                .backend = backend,
                .read_only = read_only,
                .rc = rc,
                .pending_arena = std.heap.ArenaAllocator.init(backend.allocator),
            };
        }

        fn teardown(self: *NamespaceTxn) void {
            if (self.closed) return;
            self.closed = true;
            if (self.rc) |rc| {
                lockBackend(self.backend);
                releaseState(self.allocator, rc);
                self.backend.mutex.unlock();
                self.rc = null;
            }
            self.releaseBorrowedTrees();
            self.pending_arena.deinit();
        }

        fn releaseBorrowedTrees(self: *NamespaceTxn) void {
            for (self.borrowed_trees.items) |*tree| tree.release(self.allocator);
            self.borrowed_trees.deinit(self.allocator);
            self.borrowed_trees = .empty;
        }

        /// Installs `next` as this transaction's tree, keeping the replaced
        /// tree alive when a `get` may still be borrowing from it. Capacity
        /// for that is reserved by the caller before `next` is built.
        fn replaceTree(self: *NamespaceTxn, next: ordered.Tree) void {
            const state = &self.rc.?.state;
            if (self.read_since_write) {
                self.borrowed_trees.appendAssumeCapacity(state.tree);
                self.read_since_write = false;
            } else state.tree.release(self.allocator);
            state.tree = next;
        }

        pub fn abort(self: *NamespaceTxn) void {
            self.teardown();
        }

        /// A read-only fork at the same immutable state. `RcState` is never
        /// mutated once published, so the fork only retains it; each fork
        /// keeps its own arena and closes independently of its parent.
        pub fn forkRead(self: *NamespaceTxn) !NamespaceTxn {
            if (!self.read_only or self.closed) return error.ReadSnapshotForkUnsupported;
            const rc = self.rc orelse return error.ReadSnapshotForkUnsupported;
            lockBackend(self.backend);
            retainState(rc);
            self.backend.mutex.unlock();
            return .{
                .allocator = self.allocator,
                .backend = self.backend,
                .read_only = true,
                .rc = rc,
                .pending_arena = std.heap.ArenaAllocator.init(self.allocator),
            };
        }

        /// Marks this (still-open) write transaction's eventual commit as a
        /// background/ride-along publication: it becomes visible to this
        /// backend's own readers immediately on commit, same as any other,
        /// but does not force its own durable sync -- it rides along on the
        /// next durable commit against the same underlying `.aflite` file
        /// (primary write, another index's durable commit, or an explicit
        /// idle-flush/close sync), per
        /// `native.NativeFile.commitTransactionWithDurability`'s deferred-
        /// publication path. Call before `commit()`. Never call this for a
        /// publication a caller is waiting on to be durably acknowledged --
        /// only for work that is safe to redo after a crash (segment merge/
        /// compaction, retirement cleanup, catch-up for async consumers).
        /// This is the WS2-shaped helper the design doc's "background work
        /// must never pay its own fsync" note asks for ahead of
        /// `Store.stageRideAlong`; once that lands, this flag is exactly
        /// what it should set instead.
        pub fn setDurable(self: *NamespaceTxn, durable: bool) void {
            self.durable = durable;
        }

        pub fn commit(self: *NamespaceTxn) !void {
            if (self.closed) return error.ReadOnly;
            if (self.read_only) return error.ReadOnly;
            const rc = self.rc orelse return error.ReadOnly;

            // Durable-first (when durable): readers (via the backend-state
            // swap below) must never observe a mutation this backend has not
            // yet durably committed through the sink. On failure, the
            // in-memory clone in `rc` is simply discarded by
            // `teardown`/`abort` -- the caller's errdefer pattern
            // (`errdefer txn.abort()` after `try txn.commit()`) handles that.
            // When `!self.durable`, the sink still applies the mutations (so
            // this backend's own readers see them right away) but does not
            // force a sync; see `setDurable`.
            if (self.pending.items.len != 0) {
                // `scopeDefault`, not `scope`: a caller that already pushed
                // a more specific tag (e.g. `hbc_adapter.zig`'s
                // `.hbc_vector_commit`/`.hbc_checkpoint_commit`) must keep
                // attribution for this commit's actual sync/write, instead
                // of this generic fallback shadowing it -- see
                // `write_amplification_trace.zig`'s `scopeDefault` doc
                // comment and its own "defers to an already-active caller
                // tag" regression test.
                const trace_scope = wal_trace.scopeDefault(.main_txn_commit);
                defer trace_scope.end();
                // Ride-along (`!self.durable`): stage without blocking, when
                // the sink supports it. `submitDeferred` deep-copies
                // `self.pending.items` before returning, so `pending_arena`
                // (below) stays safe to free right away even though the
                // real native commit this joins may not apply until some
                // later foreground group or idle-flush barrier drains the
                // queue -- see `Backend.pending_ride_along`'s doc comment
                // for why this call must not itself wait. Falls back to the
                // old always-blocking `submit` for a `Sink` with no deferred
                // path (every current fake/test `Sink`), which keeps this
                // exactly as correct -- just not as fast -- as before.
                if (!self.durable and self.backend.sink.submitDeferred != null) {
                    const pending = try self.backend.sink.submitDeferred.?(self.backend.sink.ptr, self.backend.scope, self.pending.items);
                    try self.backend.enqueuePendingRideAlong(pending);
                } else {
                    try self.backend.sink.submit(self.backend.sink.ptr, self.backend.scope, self.pending.items, self.durable);
                    // Opportunistic, free drain: this durable commit just
                    // led (or joined) a group-commit that -- being a
                    // foreground/durable group -- already swept up every
                    // ride-along member already staged on the same Store,
                    // this backend's own earlier ride-along commits
                    // included. Their `Pending.wait()` is therefore already
                    // resolved by the time `submit` above returns, so this
                    // never blocks. A failure here is a PAST ride-along
                    // commit's, not this (already-succeeded, already-
                    // durable) one's -- swallowed the same way `close()`
                    // does, consistent with ride-along work being safe to
                    // lose per the design doc's crash invariant.
                    if (self.durable) self.backend.drainPendingRideAlong() catch {};
                    wal_trace.recordCommit();
                }
                // A deferred submission records no commit of its own: it
                // advances no commit sequence until the group it joins
                // publishes, and that publish is what gets counted.
            }

            lockBackend(self.backend);
            const old = self.backend.state;
            self.backend.state = rc;
            releaseState(self.allocator, old);
            self.backend.mutex.unlock();
            self.rc = null;
            self.closed = true;
            self.releaseBorrowedTrees();
            self.pending_arena.deinit();
        }

        pub fn get(self: *NamespaceTxn, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            const value = try self.rc.?.state.get(namespace, key);
            self.read_since_write = true;
            return value;
        }

        pub fn put(self: *NamespaceTxn, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            if (self.read_since_write) try self.borrowed_trees.ensureUnusedCapacity(self.allocator, 1);
            self.replaceTree(try self.rc.?.state.tree.put(self.allocator, namespace.name, key, value));
            try self.pending.append(self.pending_arena.allocator(), .{
                .namespace = try dupeOpt(self.pending_arena.allocator(), namespace.name),
                .key = try self.pending_arena.allocator().dupe(u8, key),
                .value = try self.pending_arena.allocator().dupe(u8, value),
            });
        }

        pub fn delete(self: *NamespaceTxn, namespace: backend_types.Namespace, key: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            if (self.read_since_write) try self.borrowed_trees.ensureUnusedCapacity(self.allocator, 1);
            self.replaceTree(try self.rc.?.state.tree.remove(self.allocator, namespace.name, key));
            try self.pending.append(self.pending_arena.allocator(), .{
                .namespace = try dupeOpt(self.pending_arena.allocator(), namespace.name),
                .key = try self.pending_arena.allocator().dupe(u8, key),
                .is_delete = true,
            });
        }

        pub fn openCursor(self: *NamespaceTxn, namespace: backend_types.Namespace) !Cursor {
            return Cursor.init(self.allocator, self.rc.?.state.tree, namespace);
        }

        /// This transaction's staged mutations so far, in call order --
        /// valid only until the next `put`/`delete`/`abort`/`commit` call
        /// (the arena backing their bytes is this transaction's own).
        /// Mirrors `BoundTxn.pendingMutations`'s doc comment: a caller
        /// harvesting these to fold into an externally-owned commit must
        /// deep-copy before calling `abort()`.
        pub fn pendingMutations(self: *const NamespaceTxn) []const Mutation {
            return self.pending.items;
        }
    };

    // Namespace-scoped cursor over a retained tree snapshot (required by
    // `loadActiveSegmentIdsFromTxn`/`loadSegmentIdsForMetaPrefix` and friends
    // in persistent.zig, which prefix-scan the `meta` keyspace to enumerate
    // active/retired segment ids on open). Holding its own snapshot keeps
    // the nodes alive even if the transaction that opened it later commits
    // or aborts.
    const Cursor = struct {
        alloc: Allocator,
        tree: ordered.Tree,
        namespace: backend_types.Namespace,
        inner: ordered.Cursor,

        fn init(alloc: Allocator, tree: ordered.Tree, namespace: backend_types.Namespace) Cursor {
            return .{
                .alloc = alloc,
                .tree = tree.snapshot(),
                .namespace = namespace,
                .inner = ordered.Cursor.init(alloc),
            };
        }

        pub fn close(self: *@This()) void {
            self.inner.deinit();
            self.tree.release(self.alloc);
            self.* = undefined;
        }

        fn entryOf(entry: ordered.Entry) backend_adapter.Entry {
            return .{ .key = entry.key, .value = entry.value };
        }

        // `?Entry` (null at the end), not `error.NotFound`: this concrete
        // `Cursor` is reachable two ways -- through `backend_erased.
        // Cursor`'s own bridge (`backend_erased.zig`'s `cursorFrom`), which
        // already normalizes either convention into `?Entry` at ITS
        // boundary and tolerates this one unchanged (its `catch |err| if
        // (err == error.NotFound) return null` simply never triggers
        // anymore), and directly, unerased, through `Backend.boundWrite`
        // for an inline publication harvesting `pendingMutations()` before
        // ever committing (`graph/graph.zig`'s `prepareInlineEdgePublication`,
        // `sparse/sparse.zig`'s `prepareInlineSparsePublication`). The
        // latter passes this transaction into generic `txn: anytype`
        // helpers written against `backend_erased.Cursor`'s own `?Entry`
        // convention (every other caller of those helpers only ever reaches
        // them through that erased wrapper), so returning `error.NotFound`
        // here instead of `null` was a latent type mismatch, invisible
        // until a caller's cursor-using code was first instantiated
        // against this concrete, unerased type.
        fn forward(self: *@This(), found: ?ordered.Entry) !?backend_adapter.Entry {
            const entry = found orelse return null;
            if (!sameNamespace(entry.name, self.namespace.name)) return null;
            return entryOf(entry);
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            return self.forward(try self.inner.seekAtOrAfter(self.tree, self.namespace.name, ""));
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            return self.forward(try self.inner.seekAtOrAfter(self.tree, self.namespace.name, key));
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            return self.forward(try self.inner.next());
        }

        pub fn last(self: *@This()) !?backend_adapter.Entry {
            var found = try self.inner.last(self.tree);
            while (found) |entry| {
                switch (namespaceOrder(entry.name, self.namespace.name)) {
                    .eq => return entryOf(entry),
                    .gt => found = try self.inner.prev(),
                    .lt => return null,
                }
            }
            return null;
        }

        pub fn prev(self: *@This()) !?backend_adapter.Entry {
            const entry = (try self.inner.prev()) orelse return null;
            if (!sameNamespace(entry.name, self.namespace.name)) return null;
            return entryOf(entry);
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const entry = (try self.inner.seekAtOrBefore(self.tree, self.namespace.name, key)) orelse return null;
            if (!sameNamespace(entry.name, self.namespace.name)) return null;
            return entryOf(entry);
        }
    };
};

fn namespaceOrder(a: ?[]const u8, b: ?[]const u8) std.math.Order {
    if (a == null and b == null) return .eq;
    if (a == null) return .lt;
    if (b == null) return .gt;
    return std.mem.order(u8, a.?, b.?);
}

fn dupeOpt(allocator: Allocator, value: ?[]const u8) !?[]const u8 {
    const v = value orelse return null;
    return try allocator.dupe(u8, v);
}

fn lockBackend(backend: *Backend) void {
    platform_sync.lockYielding(&backend.mutex);
}

fn sameNamespace(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn identityNamespace(namespace: backend_types.Namespace) !backend_types.Namespace {
    return namespace;
}

const TestSink = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    submit_calls: usize = 0,
    durable_submit_calls: usize = 0,
    non_durable_submit_calls: usize = 0,

    fn deinit(self: *TestSink) void {
        for (self.entries.items) |e| {
            if (e.namespace) |n| self.allocator.free(n);
            self.allocator.free(e.key);
            self.allocator.free(e.value);
        }
        self.entries.deinit(self.allocator);
    }

    fn findIndex(self: *TestSink, namespace: ?[]const u8, key: []const u8) ?usize {
        for (self.entries.items, 0..) |e, i| {
            if (sameNamespace(e.namespace, namespace) and std.mem.eql(u8, e.key, key)) return i;
        }
        return null;
    }

    fn submit(ptr: *anyopaque, scope: []const u8, mutations: []const Mutation, durable: bool) anyerror!void {
        const self: *TestSink = @ptrCast(@alignCast(ptr));
        _ = scope;
        self.submit_calls += 1;
        if (durable) self.durable_submit_calls += 1 else self.non_durable_submit_calls += 1;
        for (mutations) |m| {
            if (m.is_delete) {
                if (self.findIndex(m.namespace, m.key)) |i| {
                    const removed = self.entries.swapRemove(i);
                    if (removed.namespace) |n| self.allocator.free(n);
                    self.allocator.free(removed.key);
                    self.allocator.free(removed.value);
                }
                continue;
            }
            if (self.findIndex(m.namespace, m.key)) |i| {
                self.allocator.free(self.entries.items[i].value);
                self.entries.items[i].value = try self.allocator.dupe(u8, m.value);
                continue;
            }
            try self.entries.append(self.allocator, .{
                .namespace = if (m.namespace) |n| try self.allocator.dupe(u8, n) else null,
                .key = try self.allocator.dupe(u8, m.key),
                .value = try self.allocator.dupe(u8, m.value),
            });
        }
    }

    fn loadAllAlloc(ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]Entry {
        const self: *TestSink = @ptrCast(@alignCast(ptr));
        _ = scope;
        // `Sink.loadAllAlloc`'s contract: every returned entry's bytes are
        // allocated with `allocator` and freed by the caller. A shallow
        // `allocator.dupe(Entry, ...)` only copies the struct slice --
        // each entry's `namespace`/`key`/`value` would still alias this
        // sink's own `self.entries` storage, which the caller then frees
        // out from under it (and this sink double-frees again on its own
        // `deinit`/overwrite). Deep-copy every field.
        const out = try allocator.alloc(Entry, self.entries.items.len);
        var copied: usize = 0;
        errdefer {
            for (out[0..copied]) |entry| {
                if (entry.namespace) |n| allocator.free(n);
                allocator.free(entry.key);
                allocator.free(entry.value);
            }
            allocator.free(out);
        }
        for (self.entries.items, 0..) |entry, i| {
            out[i] = .{
                .namespace = if (entry.namespace) |n| try allocator.dupe(u8, n) else null,
                .key = try allocator.dupe(u8, entry.key),
                .value = try allocator.dupe(u8, entry.value),
            };
            copied += 1;
        }
        return out;
    }

    fn sink(self: *TestSink) Sink {
        return .{ .ptr = self, .submit = submit, .loadAllAlloc = loadAllAlloc };
    }
};

// A mirror update that fails after its external commit leaves the mirror
// behind the durable catalog: every later transaction fails closed instead
// of reading it or computing writes from it.
test "native catalog backend fails closed once its mirror misses a committed write" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();
    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var read = try backend.beginRead();
    read.abort();
    backend.invalidateMirror();
    try std.testing.expectError(error.IndexMirrorInvalid, backend.beginRead());
    try std.testing.expectError(error.IndexMirrorInvalid, backend.beginWrite());
}

test "native catalog backend supports prefix cursors over a namespace" {
    // Regression: PersistentIndex.open's loadActiveSegmentIdsFromTxn /
    // loadSegmentIdsForMetaPrefix prefix-scan the `meta` keyspace via
    // MainTxn.openCursor on every open. This backend originally reported
    // `capabilities().cursors = false` and defined no `openCursor` method,
    // so `backend_erased` never wired up `open_cursor`, and any real
    // PersistentIndex.open against this backend failed with
    // error.Unsupported before a single document could be indexed.
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    try std.testing.expect(runtime.capabilities().cursors);

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "meta" }, "active-segment/\x00\x00\x00\x00\x00\x00\x00\x01", "1");
        try txn.put(.{ .name = "meta" }, "active-segment/\x00\x00\x00\x00\x00\x00\x00\x02", "1");
        try txn.put(.{ .name = "meta" }, "other-key", "unrelated");
        try txn.commit();
    }

    var txn = try runtime.beginRead();
    defer txn.abort();
    var cursor = try txn.openCursor(.{ .name = "meta" });
    defer cursor.close();

    var found = try cursor.seekAtOrAfter("active-segment/");
    var count: usize = 0;
    while (found) |entry| {
        if (!std.mem.startsWith(u8, entry.key, "active-segment/")) break;
        count += 1;
        found = try cursor.next();
    }
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "native catalog backend keeps a value read back in a write transaction alive across later writes" {
    // Regression: a write transaction that put a key, read it back, and put
    // it again freed the first value's tree node, so the borrowed read (a
    // graph metric page's worker id) pointed at freed memory and a later
    // encode copied it onto itself (`@memcpy arguments alias`).
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    var txn = try runtime.beginWrite();
    errdefer txn.abort();
    try txn.put(.{ .name = "meta" }, "page", "worker-a");
    const first = try txn.get(.{ .name = "meta" }, "page");
    try txn.put(.{ .name = "meta" }, "page", "worker-b");
    try txn.delete(.{ .name = "meta" }, "page");
    try std.testing.expectEqualStrings("worker-a", first);
    try txn.put(.{ .name = "meta" }, "page", "worker-c");
    const second = try txn.get(.{ .name = "meta" }, "page");
    try txn.put(.{ .name = "meta" }, "page", "worker-d");
    try std.testing.expectEqualStrings("worker-c", second);
    try std.testing.expectEqualStrings("worker-a", first);
    try txn.commit();
}

test "native catalog backend reads and writes through the sink, one submit per commit" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();

    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "meta:a", "1");
        try txn.put(.{ .name = "segments" }, "seg:1", "bytes-1");
        try txn.put(.{ .name = "segments" }, "seg:2", "bytes-2");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.submit_calls);
    try std.testing.expectEqual(@as(usize, 3), test_sink.entries.items.len);

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("1", try txn.get(.{}, "meta:a"));
        try std.testing.expectEqualStrings("bytes-1", try txn.get(.{ .name = "segments" }, "seg:1"));
    }

    {
        var txn = try runtime.beginWrite();
        try txn.delete(.{ .name = "segments" }, "seg:1");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 2), test_sink.submit_calls);
    try std.testing.expectEqual(@as(usize, 2), test_sink.entries.items.len);

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "segments" }, "seg:1"));
        try std.testing.expectEqualStrings("bytes-2", try txn.get(.{ .name = "segments" }, "seg:2"));
    }
}

test "native catalog backend seeds its mirror from the sink on open" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();
    try TestSink.submit(&test_sink, "idx1", &.{
        .{ .namespace = "meta", .key = "k", .value = "preexisting" },
    }, true);
    test_sink.submit_calls = 0;

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("preexisting", try txn.get(.{ .name = "meta" }, "k"));
    try std.testing.expectEqual(@as(usize, 0), test_sink.submit_calls);
}

test "native catalog backend read transactions are snapshot isolated across a commit" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "k", "before");
        try txn.commit();
    }

    var read_before = try runtime.beginRead();
    defer read_before.abort();

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "k", "after");
        try txn.commit();
    }

    try std.testing.expectEqualStrings("before", try read_before.get(.{}, "k"));

    var read_after = try runtime.beginRead();
    defer read_after.abort();
    try std.testing.expectEqualStrings("after", try read_after.get(.{}, "k"));
}

test "native catalog backend setDurable makes a ride-along commit visible without marking it durable" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        txn.setDurableHint(false);
        try txn.put(.{ .name = "meta" }, "merge-result", "ride-along");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), test_sink.durable_submit_calls);
    try std.testing.expectEqual(@as(usize, 1), test_sink.non_durable_submit_calls);

    // Ride-along commits are still immediately visible to this backend's
    // own readers; only the sink's own durability bookkeeping differs.
    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("ride-along", try txn.get(.{ .name = "meta" }, "merge-result"));
    }

    // Omitting setDurable keeps the default: durable.
    {
        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "meta" }, "ack-result", "foreground");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.durable_submit_calls);
}

test "native catalog backend sink failure aborts the transaction without publishing" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    const FailingSink = struct {
        fn submit(_: *anyopaque, _: []const u8, _: []const Mutation, _: bool) anyerror!void {
            return error.InjectedSyncFault;
        }
    };

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    // Swap in a failing sink for this one write.
    backend.sink = .{ .ptr = &test_sink, .submit = FailingSink.submit, .loadAllAlloc = TestSink.loadAllAlloc };

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "k", "v");
        try std.testing.expectError(error.InjectedSyncFault, txn.commit());
        txn.abort();
    }

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(.{}, "k"));
}

test "native catalog backend applyExternallyCommittedMutations syncs the read mirror without calling the sink" {
    // WS4 phase 2: an externally-driven native commit (lite/docstore.zig's
    // Txn.putIndexCatalogBatch) never calls this backend's Sink.submit --
    // it writes the durable index-catalog records itself, in the same
    // transaction as a primary write. This is the "install the in-memory
    // snapshot" step that must run after that commit succeeds, so this
    // backend's own readers (and later writers computing e.g. the next
    // segment id from what they can read back) see it.
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();
    var runtime = try backend.runtimeNamespaceStore(a);
    defer runtime.deinit();

    // A snapshot taken before the external mutations must stay frozen
    // (copy-on-write, exactly like a transaction's own commit).
    var read_before = try runtime.beginRead();
    defer read_before.abort();

    try backend.applyExternallyCommittedMutations(&.{
        .{ .namespace = "segments", .key = "seg:1", .value = "bytes-1" },
        .{ .namespace = "meta", .key = "active-segment/1", .value = "1" },
    });
    try std.testing.expectEqual(@as(usize, 0), test_sink.submit_calls);

    try std.testing.expectError(error.NotFound, read_before.get(.{ .name = "segments" }, "seg:1"));

    var read_after = try runtime.beginRead();
    defer read_after.abort();
    try std.testing.expectEqualStrings("bytes-1", try read_after.get(.{ .name = "segments" }, "seg:1"));
    try std.testing.expectEqualStrings("1", try read_after.get(.{ .name = "meta" }, "active-segment/1"));

    // A delete of a record this mirror never had is tolerated, not an error.
    try backend.applyExternallyCommittedMutations(&.{
        .{ .namespace = "segments", .key = "never-existed", .is_delete = true },
        .{ .namespace = "meta", .key = "active-segment/1", .is_delete = true },
    });
    var read_final = try runtime.beginRead();
    defer read_final.abort();
    try std.testing.expectError(error.NotFound, read_final.get(.{ .name = "meta" }, "active-segment/1"));
    try std.testing.expectEqualStrings("bytes-1", try read_final.get(.{ .name = "segments" }, "seg:1"));
}

test "native catalog backend runtimeStore gives a flat (non-namespaced) erased Store" {
    // graph/graph.zig's `openEdgeStore` uses this, not `runtimeNamespaceStore`:
    // forward/reverse adjacency is a flat keyspace, one `Backend`/scope per
    // store, with no namespace indirection.
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "graph:g:forward" });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();

    try std.testing.expect(runtime.capabilities().cursors);

    {
        var batch = try runtime.beginBatch();
        try batch.put("edge:a->b", "1.0");
        try batch.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.submit_calls);

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("1.0", try read.get("edge:a->b"));
}

test "native catalog backend runtimeStore reads fork at the parent's snapshot and outlive it" {
    // Cypher edge expansion forks the graph adjacency read it pinned for the
    // statement; without a fork the erased read reports
    // ReadSnapshotForkUnsupported.
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "graph:g:forward" });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();

    {
        var batch = try runtime.beginBatch();
        try batch.put("edge:a->b", "1.0");
        try batch.commit();
    }
    var read = try runtime.beginRead();
    var fork = try read.forkRead();
    defer fork.abort();
    read.abort();
    {
        var batch = try runtime.beginBatch();
        try batch.put("edge:a->b", "2.0");
        try batch.put("edge:b->c", "3.0");
        try batch.commit();
    }
    try std.testing.expectEqualStrings("1.0", try fork.get("edge:a->b"));
    try std.testing.expectError(error.NotFound, fork.get("edge:b->c"));
    var nested = try fork.forkRead();
    defer nested.abort();
    var cursor = try nested.openCursor();
    defer cursor.close();
    const first = (try cursor.first()).?;
    try std.testing.expectEqualStrings("edge:a->b", first.key);
    try std.testing.expect((try cursor.next()) == null);

    var write = try backend.boundWrite(.{});
    defer write.abort();
    try std.testing.expectError(error.ReadSnapshotForkUnsupported, write.forkBorrowedRead());
}

test "native catalog backend runtimeStore binds one namespace, isolated from other namespaces" {
    // `runtimeStore` is what a vector-index caller (sparse's SparseIndex,
    // and dense's HBC backend) uses instead of `runtimeNamespaceStore`: a
    // single namespace baked into the returned `backend_erased.Store`, same
    // shape as `mem_backend.zig`/`lsm_backend.zig`'s own `runtimeStore`.
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "sparse-idx1" });
    defer backend.close();

    var store = try backend.runtimeStore(a, .{ .name = "postings" });
    defer store.deinit();

    {
        var txn = try store.beginWrite();
        try txn.put("term:a", "doc-1");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.submit_calls);

    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("doc-1", try read.get("term:a"));
    }

    // A write through this namespace-bound store must not leak into a
    // sibling namespace's keyspace -- `NamespaceTxn` already scopes by
    // namespace, but `runtimeStore`'s whole point is to bind that scope
    // once instead of trusting every call site to pass it correctly.
    var other_namespace = try backend.runtimeNamespaceStore(a);
    defer other_namespace.deinit();
    var other_read = try other_namespace.beginRead();
    defer other_read.abort();
    try std.testing.expectError(error.NotFound, other_read.get(.{ .name = "meta" }, "term:a"));
    try std.testing.expectEqualStrings("doc-1", try other_read.get(.{ .name = "postings" }, "term:a"));
}

test "native catalog backend boundWrite exposes pending mutations before commit, for an external fold-in" {
    // The shape `graph/graph.zig`'s `prepareInlineEdgePublication` depends
    // on: build up a transaction's puts, harvest its `pendingMutations()`
    // (deep-copying before `abort()`, since the arena backing them dies
    // with the transaction), and never call `commit()` -- this `Sink` must
    // see zero submits for the harvested mutations to be foldable into a
    // different, externally-owned native transaction instead.
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "graph:g:reverse" });
    defer backend.close();

    var txn = try backend.boundWrite(.{});
    try txn.put("edge:b<-a", "1.0");
    try txn.put("meta:edge_count", "1");

    const pending = txn.pendingMutations();
    try std.testing.expectEqual(@as(usize, 2), pending.len);
    try std.testing.expectEqualStrings("edge:b<-a", pending[0].key);
    try std.testing.expectEqualStrings("1.0", pending[0].value);
    try std.testing.expectEqualStrings("meta:edge_count", pending[1].key);

    txn.abort();
    try std.testing.expectEqual(@as(usize, 0), test_sink.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), test_sink.entries.items.len);
}

test "native catalog backend runtimeStore beginBatchWithOptions rides along without its own durable submit" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "sparse-idx1" });
    defer backend.close();

    var store = try backend.runtimeStore(a, .{ .name = "postings" });
    defer store.deinit();

    {
        var batch = try store.beginBatchWithOptions(.{ .durability = .ride_along });
        try batch.put("merge-result", "ride-along");
        try batch.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), test_sink.durable_submit_calls);
    try std.testing.expectEqual(@as(usize, 1), test_sink.non_durable_submit_calls);

    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("ride-along", try read.get("merge-result"));
    }

    // The plain (default-durability) path through the same Store is
    // unaffected: omitting `.ride_along` keeps paying a durable submit.
    {
        var batch = try store.beginBatchWithOptions(.{});
        try batch.put("ack-result", "foreground");
        try batch.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), test_sink.durable_submit_calls);
}

test "native catalog backend beginBatchWithOptions on the NamespaceStore surface also rides along" {
    // Same coverage as the runtimeStore test above, but through
    // `Backend.beginBatchWithOptions` directly -- the surface
    // `runtimeNamespaceStore`'s erased `NamespaceBatch` wraps. Regression
    // guard for this method's `.ride_along` handling (it used to discard
    // `options` entirely).
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    var backend = try Backend.init(a, .{ .sink = test_sink.sink(), .scope = "idx1" });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .durability = .ride_along });
        try txn.put(.{ .name = "meta" }, "merge-result", "ride-along");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 0), test_sink.durable_submit_calls);
    try std.testing.expectEqual(@as(usize, 1), test_sink.non_durable_submit_calls);
}
