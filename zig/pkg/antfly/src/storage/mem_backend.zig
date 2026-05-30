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
const OwnedEntry = struct {
    namespace_name: ?[]u8,
    key: []u8,
    value: []u8,

    fn deinit(self: *OwnedEntry, allocator: Allocator) void {
        if (self.namespace_name) |name| allocator.free(name);
        allocator.free(self.key);
        allocator.free(self.value);
        self.* = undefined;
    }

    fn entry(self: *const OwnedEntry) backend_adapter.Entry {
        return .{
            .key = self.key,
            .value = self.value,
        };
    }
};

const State = struct {
    arena: ?std.heap.ArenaAllocator = null,
    entries: std.ArrayListUnmanaged(OwnedEntry) = .empty,

    fn deinit(self: *State, allocator: Allocator) void {
        if (self.arena) |*arena| {
            arena.deinit();
            self.* = .{};
            return;
        }
        for (self.entries.items) |*entry| entry.deinit(allocator);
        self.entries.deinit(allocator);
        self.* = .{};
    }

    fn clone(self: *const State, allocator: Allocator) !State {
        var out: State = .{};
        errdefer out.deinit(allocator);
        out.arena = std.heap.ArenaAllocator.init(allocator);

        const arena_alloc = out.arena.?.allocator();
        try out.entries.ensureTotalCapacity(arena_alloc, self.entries.items.len);
        for (self.entries.items) |entry| {
            out.entries.appendAssumeCapacity(try cloneEntry(arena_alloc, entry));
        }
        return out;
    }

    fn get(self: *const State, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        const idx = self.findIndex(namespace, key) orelse return error.NotFound;
        return self.entries.items[idx].value;
    }

    fn put(self: *State, allocator: Allocator, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        const state_alloc = self.stateAllocator(allocator);
        if (self.findIndex(namespace, key)) |idx| {
            const replacement = try state_alloc.dupe(u8, value);
            if (!self.usesArena()) allocator.free(self.entries.items[idx].value);
            self.entries.items[idx].value = replacement;
            return;
        }

        const idx = self.lowerBound(namespace, key);
        try self.entries.insert(state_alloc, idx, try initEntry(state_alloc, namespace, key, value));
    }

    fn delete(self: *State, allocator: Allocator, namespace: backend_types.Namespace, key: []const u8) !void {
        const idx = self.findIndex(namespace, key) orelse return;
        var removed = self.entries.orderedRemove(idx);
        if (!self.usesArena()) removed.deinit(allocator);
    }

    fn lowerBound(self: *const State, namespace: backend_types.Namespace, key: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const ord = compareEntryTo(self.entries.items[mid], namespace, key);
            if (ord == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    fn findIndex(self: *const State, namespace: backend_types.Namespace, key: []const u8) ?usize {
        const idx = self.lowerBound(namespace, key);
        if (idx >= self.entries.items.len) return null;
        if (compareEntryTo(self.entries.items[idx], namespace, key) != .eq) return null;
        return idx;
    }

    fn stateAllocator(self: *State, fallback: Allocator) Allocator {
        if (self.arena) |*arena| return arena.allocator();
        return fallback;
    }

    fn usesArena(self: *const State) bool {
        return self.arena != null;
    }
};

fn cloneEntry(allocator: Allocator, entry: OwnedEntry) !OwnedEntry {
    return .{
        .namespace_name = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null,
        .key = try allocator.dupe(u8, entry.key),
        .value = try allocator.dupe(u8, entry.value),
    };
}

fn initEntry(
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    value: []const u8,
) !OwnedEntry {
    return .{
        .namespace_name = if (namespace.name) |name| try allocator.dupe(u8, name) else null,
        .key = try allocator.dupe(u8, key),
        .value = try allocator.dupe(u8, value),
    };
}

fn namespaceOf(entry: OwnedEntry) backend_types.Namespace {
    return .{ .name = entry.namespace_name };
}

fn compareNamespace(a: backend_types.Namespace, b: backend_types.Namespace) std.math.Order {
    if (a.name == null and b.name == null) return .eq;
    if (a.name == null) return .lt;
    if (b.name == null) return .gt;
    return std.mem.order(u8, a.name.?, b.name.?);
}

fn compareEntryTo(entry: OwnedEntry, namespace: backend_types.Namespace, key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(namespaceOf(entry), namespace);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, entry.key, key);
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

    const BoundCursor = struct {
        state: *const State,
        namespace: backend_types.Namespace,
        current: ?usize = null,

        pub fn close(_: *@This()) void {}

        pub fn first(self: *@This()) !backend_adapter.Entry {
            const idx = self.firstIndex() orelse return error.NotFound;
            self.current = idx;
            return self.state.entries.items[idx].entry();
        }

        pub fn last(self: *@This()) !backend_adapter.Entry {
            const idx = self.lastIndex() orelse return error.NotFound;
            self.current = idx;
            return self.state.entries.items[idx].entry();
        }

        pub fn next(self: *@This()) !backend_adapter.Entry {
            const current = self.current orelse return error.NotFound;
            var idx = current + 1;
            while (idx < self.state.entries.items.len) : (idx += 1) {
                if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) == .eq) {
                    self.current = idx;
                    return self.state.entries.items[idx].entry();
                }
            }
            return error.NotFound;
        }

        pub fn prev(self: *@This()) !backend_adapter.Entry {
            const current = self.current orelse return error.NotFound;
            if (current == 0) return error.NotFound;
            var idx = current - 1;
            while (true) {
                if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) == .eq) {
                    self.current = idx;
                    return self.state.entries.items[idx].entry();
                }
                if (idx == 0) break;
                idx -= 1;
            }
            return error.NotFound;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !backend_adapter.Entry {
            const idx = self.state.lowerBound(self.namespace, key);
            if (idx >= self.state.entries.items.len) return error.NotFound;
            if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) != .eq) return error.NotFound;
            self.current = idx;
            return self.state.entries.items[idx].entry();
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !backend_adapter.Entry {
            const idx = self.state.lowerBound(self.namespace, key);
            if (idx < self.state.entries.items.len and compareEntryTo(self.state.entries.items[idx], self.namespace, key) == .eq) {
                self.current = idx;
                return self.state.entries.items[idx].entry();
            }
            if (idx == 0) return error.NotFound;
            var probe = idx - 1;
            while (true) {
                if (compareNamespace(namespaceOf(self.state.entries.items[probe]), self.namespace) == .eq) {
                    self.current = probe;
                    return self.state.entries.items[probe].entry();
                }
                if (probe == 0) break;
                probe -= 1;
            }
            return error.NotFound;
        }

        fn firstIndex(self: *const @This()) ?usize {
            const idx = self.state.lowerBound(self.namespace, "");
            if (idx >= self.state.entries.items.len) return null;
            if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) != .eq) return null;
            return idx;
        }

        fn lastIndex(self: *const @This()) ?usize {
            if (self.state.entries.items.len == 0) return null;
            var idx = self.state.entries.items.len;
            while (idx > 0) {
                idx -= 1;
                if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) == .eq) return idx;
            }
            return null;
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
            return .{
                .state = &self.rc.?.state,
                .namespace = self.namespace,
            };
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
