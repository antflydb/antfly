// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const ordered = @import("mem_ordered.zig");

const State = struct {
    tree: ordered.Tree = .empty,

    fn deinit(self: *State, allocator: Allocator) void {
        self.tree.release(allocator);
        self.* = .{};
    }

    // O(1) snapshot: the persistent tree shares structure with the source, so a
    // transaction starts from the current version without copying it.
    fn clone(self: *const State, allocator: Allocator) !State {
        _ = allocator;
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

fn sameNamespace(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn namespaceOrder(a: ?[]const u8, b: ?[]const u8) std.math.Order {
    if (a == null and b == null) return .eq;
    if (a == null) return .lt;
    if (b == null) return .gt;
    return std.mem.order(u8, a.?, b.?);
}

pub const Options = struct {
    backend: backend_types.OpenOptions = .{},
};

// Reference-counted, immutable-while-shared snapshot of the store. Read
// transactions retain the current snapshot in O(1) instead of cloning the whole
// entry list; a writer clones into a fresh snapshot and publishes it on commit,
// so readers opened beforehand keep observing the snapshot they retained until
// they release it. This keeps snapshot isolation while making reads cheap.
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

pub const Backend = struct {
    allocator: Allocator,
    open_options: backend_types.OpenOptions,
    state: ?*RcState = null,
    mutex: std.atomic.Mutex = .unlocked,

    // Returns the current snapshot, creating an empty one on first use. Must be
    // called with the backend mutex held.
    fn ensureStateLocked(self: *Backend) !*RcState {
        if (self.state) |rc| return rc;
        const rc = try self.allocator.create(RcState);
        rc.* = .{ .refs = 1, .state = .{} };
        self.state = rc;
        return rc;
    }

    const BoundStore = struct {
        backend: *Backend,
        namespace: backend_types.Namespace,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
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

        pub fn beginRead(self: *@This()) !BoundTxn {
            return try BoundTxn.open(self.backend, self.namespace, true);
        }

        pub fn beginWrite(self: *@This()) !BoundTxn {
            return try BoundTxn.open(self.backend, self.namespace, false);
        }

        pub fn beginBatch(self: *@This()) !BoundTxn {
            return try BoundTxn.open(self.backend, self.namespace, false);
        }

        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !BoundTxn {
            _ = options;
            return try BoundTxn.open(self.backend, self.namespace, false);
        }
    };

    // Namespace-scoped cursor over a retained tree snapshot. Holding its own
    // snapshot keeps the nodes alive even if the transaction writes afterwards.
    const BoundCursor = struct {
        alloc: Allocator,
        tree: ordered.Tree,
        namespace: backend_types.Namespace,
        inner: ordered.Cursor,

        fn init(alloc: Allocator, tree: ordered.Tree, namespace: backend_types.Namespace) BoundCursor {
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

        // A forward step lands on the next entry in namespace order; once it
        // crosses out of this cursor's namespace the scan is exhausted.
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

    const BoundTxn = struct {
        allocator: Allocator,
        backend: *Backend,
        namespace: backend_types.Namespace,
        read_only: bool,
        rc: ?*RcState,

        fn open(backend: *Backend, namespace: backend_types.Namespace, read_only: bool) !BoundTxn {
            lockBackend(backend);
            defer backend.mutex.unlock();
            const current = try backend.ensureStateLocked();
            const rc: *RcState = if (read_only) blk: {
                retainState(current);
                break :blk current;
            } else blk: {
                var cloned = try current.state.clone(backend.allocator);
                errdefer cloned.deinit(backend.allocator);
                const new_rc = try backend.allocator.create(RcState);
                new_rc.* = .{ .refs = 1, .state = cloned };
                break :blk new_rc;
            };
            return .{
                .allocator = backend.allocator,
                .backend = backend,
                .namespace = namespace,
                .read_only = read_only,
                .rc = rc,
            };
        }

        pub fn abort(self: *@This()) void {
            const rc = self.rc orelse return;
            lockBackend(self.backend);
            releaseState(self.allocator, rc);
            self.backend.mutex.unlock();
            self.rc = null;
        }

        pub fn commit(self: *@This()) !void {
            if (self.read_only) return error.ReadOnly;
            const rc = self.rc orelse return error.ReadOnly;
            lockBackend(self.backend);
            defer self.backend.mutex.unlock();
            const old = self.backend.state.?;
            self.backend.state = rc;
            releaseState(self.allocator, old);
            self.rc = null;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return try self.rc.?.state.get(self.namespace, key);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            try self.rc.?.state.put(self.allocator, self.namespace, key, value);
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            try self.rc.?.state.delete(self.allocator, self.namespace, key);
        }

        pub fn openCursor(self: *@This()) !BoundCursor {
            return BoundCursor.init(self.allocator, self.rc.?.state.tree, self.namespace);
        }
    };

    const NamespaceTxn = struct {
        allocator: Allocator,
        backend: *Backend,
        read_only: bool,
        rc: ?*RcState,

        fn open(backend: *Backend, read_only: bool) !NamespaceTxn {
            lockBackend(backend);
            defer backend.mutex.unlock();
            const current = try backend.ensureStateLocked();
            const rc: *RcState = if (read_only) blk: {
                retainState(current);
                break :blk current;
            } else blk: {
                var cloned = try current.state.clone(backend.allocator);
                errdefer cloned.deinit(backend.allocator);
                const new_rc = try backend.allocator.create(RcState);
                new_rc.* = .{ .refs = 1, .state = cloned };
                break :blk new_rc;
            };
            return .{
                .allocator = backend.allocator,
                .backend = backend,
                .read_only = read_only,
                .rc = rc,
            };
        }

        pub fn abort(self: *@This()) void {
            const rc = self.rc orelse return;
            lockBackend(self.backend);
            releaseState(self.allocator, rc);
            self.backend.mutex.unlock();
            self.rc = null;
        }

        pub fn commit(self: *@This()) !void {
            if (self.read_only) return error.ReadOnly;
            const rc = self.rc orelse return error.ReadOnly;
            lockBackend(self.backend);
            defer self.backend.mutex.unlock();
            const old = self.backend.state.?;
            self.backend.state = rc;
            releaseState(self.allocator, old);
            self.rc = null;
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            return try self.rc.?.state.get(namespace, key);
        }

        pub fn put(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            try self.rc.?.state.put(self.allocator, namespace, key, value);
        }

        pub fn delete(self: *@This(), namespace: backend_types.Namespace, key: []const u8) !void {
            if (self.read_only) return error.ReadOnly;
            try self.rc.?.state.delete(self.allocator, namespace, key);
        }
    };

    pub fn init(allocator: Allocator, options: Options) Backend {
        return .{
            .allocator = allocator,
            .open_options = options.backend,
        };
    }

    pub fn close(self: *Backend) void {
        lockBackend(self);
        if (self.state) |rc| releaseState(self.allocator, rc);
        self.mutex.unlock();
        self.* = undefined;
    }

    pub fn capabilities(_: *Backend) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = false,
            .cursors = false,
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

    pub fn runtimeStore(
        self: *Backend,
        allocator: Allocator,
        namespace: backend_types.Namespace,
    ) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, BoundStore{
            .backend = self,
            .namespace = namespace,
        });
    }
};

fn lockBackend(backend: *Backend) void {
    while (!backend.mutex.tryLock()) std.Thread.yield() catch {};
}

fn identityNamespace(namespace: backend_types.Namespace) !backend_types.Namespace {
    return namespace;
}

test "memory backend runtime erases namespace store handles" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();
    try std.testing.expect(!runtime.capabilities().native_namespaces);
    try std.testing.expect(!runtime.capabilities().ordered_append_puts);

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "meta:lsn", "1");
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("1", try txn.get(.{}, "meta:lsn"));
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
    }
}

test "memory backend runtime erases bound store handles with cursor access" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().cursors);
    try std.testing.expect(!runtime.capabilities().native_namespaces);

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
        try std.testing.expectEqualStrings("doc:b", (try cur.seekAtOrAfter("doc:b")).?.key);
        try std.testing.expectEqualStrings("doc:a", (try cur.seekAtOrBefore("doc:a")).?.key);
    }
}

test "memory backend read transactions are snapshot isolated" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "before");
        try txn.commit();
    }

    var read_before = try runtime.beginRead();
    defer read_before.abort();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "after");
        try txn.commit();
    }

    try std.testing.expectEqualStrings("before", try read_before.get("doc:a"));

    {
        var read_after = try runtime.beginRead();
        defer read_after.abort();
        try std.testing.expectEqualStrings("after", try read_after.get("doc:a"));
    }
}
