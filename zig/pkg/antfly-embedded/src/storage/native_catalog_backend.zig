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

/// Durable storage underlying this backend's in-memory mirror. Implemented
/// concretely by `lite/native_catalog_sink.zig` against a Lite `.aflite`
/// file's native index-catalog records. A single `Sink` value may be shared
/// by every `PersistentIndex` in one Lite DB (it wraps one `docstore.Store`);
/// `scope` disambiguates which index's records a call addresses, so the
/// sink's own durable keys never collide across indexes sharing one `.aflite`
/// file's native index-catalog root.
pub const Sink = struct {
    ptr: *anyopaque,
    /// Apply every mutation in `mutations`, in order, as ONE durable commit
    /// (one `syncData`/checkpoint publication at the underlying
    /// native-catalog layer), or none of them on failure. `mutations` is
    /// only valid for the duration of the call.
    submit: *const fn (ptr: *anyopaque, scope: []const u8, mutations: []const Mutation) anyerror!void,
    /// Loads every entry currently committed under this sink's scope, to
    /// seed the in-memory mirror on open. Called once, synchronously, from
    /// `Backend.init`. The returned slice and every entry's bytes are
    /// allocated with the given allocator and freed by the caller
    /// immediately after use.
    loadAllAlloc: *const fn (ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]Entry,
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
        lockBackend(self);
        releaseState(self.allocator, self.state);
        self.mutex.unlock();
        self.allocator.free(self.scope);
        self.* = undefined;
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

    pub fn beginBatchWithOptions(self: *Backend, options: backend_types.BatchOptions) !NamespaceTxn {
        _ = options;
        if (self.open_options.read_only) return error.ReadOnly;
        return try NamespaceTxn.open(self, false);
    }

    pub fn runtimeNamespaceStore(self: *Backend, allocator: Allocator) !backend_erased.NamespaceStore {
        return try backend_erased.namespaceStoreFrom(allocator, self, backend_types.Namespace, identityNamespace);
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

        fn open(backend: *Backend, read_only: bool) !NamespaceTxn {
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
            self.pending_arena.deinit();
        }

        pub fn abort(self: *NamespaceTxn) void {
            self.teardown();
        }

        pub fn commit(self: *NamespaceTxn) !void {
            if (self.closed) return error.ReadOnly;
            if (self.read_only) return error.ReadOnly;
            const rc = self.rc orelse return error.ReadOnly;

            // Durable first: readers (via the backend-state swap below) must
            // never observe a mutation this backend has not yet durably
            // committed through the sink. On failure, the in-memory clone
            // in `rc` is simply discarded by `teardown`/`abort` -- the
            // caller's errdefer pattern (`errdefer txn.abort()` after
            // `try txn.commit()`) handles that.
            if (self.pending.items.len != 0) {
                try self.backend.sink.submit(self.backend.sink.ptr, self.backend.scope, self.pending.items);
            }

            lockBackend(self.backend);
            const old = self.backend.state;
            self.backend.state = rc;
            releaseState(self.allocator, old);
            self.backend.mutex.unlock();
            self.rc = null;
            self.closed = true;
            self.pending_arena.deinit();
        }

        pub fn get(self: *NamespaceTxn, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            return try self.rc.?.state.get(namespace, key);
        }

        pub fn put(self: *NamespaceTxn, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            try self.rc.?.state.put(self.allocator, namespace, key, value);
            try self.pending.append(self.pending_arena.allocator(), .{
                .namespace = try dupeOpt(self.pending_arena.allocator(), namespace.name),
                .key = try self.pending_arena.allocator().dupe(u8, key),
                .value = try self.pending_arena.allocator().dupe(u8, value),
            });
        }

        pub fn delete(self: *NamespaceTxn, namespace: backend_types.Namespace, key: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            try self.rc.?.state.delete(self.allocator, namespace, key);
            try self.pending.append(self.pending_arena.allocator(), .{
                .namespace = try dupeOpt(self.pending_arena.allocator(), namespace.name),
                .key = try self.pending_arena.allocator().dupe(u8, key),
                .is_delete = true,
            });
        }

        pub fn openCursor(self: *NamespaceTxn, namespace: backend_types.Namespace) !Cursor {
            return Cursor.init(self.allocator, self.rc.?.state.tree, namespace);
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

        fn forward(self: *@This(), found: ?ordered.Entry) !backend_adapter.Entry {
            const entry = found orelse return error.NotFound;
            if (!sameNamespace(entry.name, self.namespace.name)) return error.NotFound;
            return entryOf(entry);
        }

        pub fn first(self: *@This()) !backend_adapter.Entry {
            return self.forward(try self.inner.seekAtOrAfter(self.tree, self.namespace.name, ""));
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !backend_adapter.Entry {
            return self.forward(try self.inner.seekAtOrAfter(self.tree, self.namespace.name, key));
        }

        pub fn next(self: *@This()) !backend_adapter.Entry {
            return self.forward(try self.inner.next());
        }

        pub fn last(self: *@This()) !backend_adapter.Entry {
            var found = try self.inner.last(self.tree);
            while (found) |entry| {
                switch (namespaceOrder(entry.name, self.namespace.name)) {
                    .eq => return entryOf(entry),
                    .gt => found = try self.inner.prev(),
                    .lt => return error.NotFound,
                }
            }
            return error.NotFound;
        }

        pub fn prev(self: *@This()) !backend_adapter.Entry {
            const entry = (try self.inner.prev()) orelse return error.NotFound;
            if (!sameNamespace(entry.name, self.namespace.name)) return error.NotFound;
            return entryOf(entry);
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !backend_adapter.Entry {
            const entry = (try self.inner.seekAtOrBefore(self.tree, self.namespace.name, key)) orelse return error.NotFound;
            if (!sameNamespace(entry.name, self.namespace.name)) return error.NotFound;
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

    fn submit(ptr: *anyopaque, scope: []const u8, mutations: []const Mutation) anyerror!void {
        const self: *TestSink = @ptrCast(@alignCast(ptr));
        _ = scope;
        self.submit_calls += 1;
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
        return try allocator.dupe(Entry, self.entries.items);
    }

    fn sink(self: *TestSink) Sink {
        return .{ .ptr = self, .submit = submit, .loadAllAlloc = loadAllAlloc };
    }
};

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
    });
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

test "native catalog backend sink failure aborts the transaction without publishing" {
    const a = std.testing.allocator;
    var test_sink = TestSink{ .allocator = a };
    defer test_sink.deinit();

    const FailingSink = struct {
        fn submit(_: *anyopaque, _: []const u8, _: []const Mutation) anyerror!void {
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
