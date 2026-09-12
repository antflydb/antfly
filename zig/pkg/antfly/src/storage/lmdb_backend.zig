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
const backend_lmdb_adapter = @import("backend_lmdb_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const lmdb = @import("lmdb.zig");
const platform = @import("antfly_platform");

fn identityNamespace(namespace: backend_types.Namespace) !backend_types.Namespace {
    return namespace;
}

fn openNamespaceDbi(
    allocator: Allocator,
    raw: *lmdb.Transaction,
    namespace: backend_types.Namespace,
    create: bool,
) !lmdb.Dbi {
    const db_name = namespace.name orelse return try raw.openDb(null, .{ .create = create });
    const db_name_z = try allocator.dupeZ(u8, db_name);
    defer allocator.free(db_name_z);
    return try raw.openDb(db_name_z, .{ .create = create });
}

fn openNamespaceDbiBatch(
    allocator: Allocator,
    raw: *lmdb.Batch,
    namespace: backend_types.Namespace,
    create: bool,
) !lmdb.Dbi {
    const db_name = namespace.name orelse return try raw.openDb(null, .{ .create = create });
    const db_name_z = try allocator.dupeZ(u8, db_name);
    defer allocator.free(db_name_z);
    return try raw.openDb(db_name_z, .{ .create = create });
}

pub const Options = struct {
    backend: backend_types.OpenOptions = .{},
    env: lmdb.EnvironmentOptions = .{
        .max_dbs = 32,
    },
};

pub const Backend = struct {
    allocator: Allocator,
    env: lmdb.Environment,
    open_options: backend_types.OpenOptions,
    read_fork_enabled: bool = false,

    const BoundStore = struct {
        backend: *Backend,
        namespace: backend_types.Namespace,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{
                .ordered_ranges = true,
                .reverse_ranges = true,
                .cursors = true,
                .ordered_append_puts = true,
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

        pub fn beginBatch(self: *@This()) !BoundBatch {
            return try BoundBatch.open(self.backend, self.namespace);
        }
    };

    const BoundTxn = struct {
        allocator: Allocator,
        raw: lmdb.Transaction,
        dbi: lmdb.Dbi,
        read_mutex: ?*std.atomic.Mutex = null,
        owns_raw: bool = true,
        fork_enabled: bool = false,

        // MDB_NOTLS permits sharing a read transaction, but MDB still requires
        // serialized calls. Erased parents pin raw/mutex until the last fork
        // and cursor closes; each cursor has independent positioning state.
        pub fn forkBorrowedRead(self: *@This()) !BoundTxn {
            if (!self.fork_enabled or self.read_mutex == null) return error.ReadSnapshotForkUnsupported;
            var fork = self.*;
            fork.owns_raw = false;
            return fork;
        }

        fn open(backend: *Backend, namespace: backend_types.Namespace, read_only: bool) !BoundTxn {
            var raw = try backend.env.begin(.{ .read_only = read_only });
            errdefer raw.abort();
            const dbi = try openNamespaceDbi(
                backend.allocator,
                &raw,
                namespace,
                !read_only and backend.open_options.create_if_missing,
            );
            const read_mutex = if (read_only) try backend.allocator.create(std.atomic.Mutex) else null;
            if (read_mutex) |mutex| mutex.* = .unlocked;
            return .{
                .allocator = backend.allocator,
                .raw = raw,
                .dbi = dbi,
                .read_mutex = read_mutex,
                .fork_enabled = backend.read_fork_enabled,
            };
        }

        pub fn abort(self: *@This()) void {
            if (!self.owns_raw) return;
            self.raw.abort();
            if (self.read_mutex) |mutex| self.allocator.destroy(mutex);
        }

        pub fn commit(self: *@This()) !void {
            if (self.read_mutex != null) return error.ReadOnly;
            try self.raw.commit();
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return try self.raw.get(self.dbi, key);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.read_mutex != null) return error.ReadOnly;
            try self.raw.put(self.dbi, key, value, .{});
        }

        pub fn appendPut(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.read_mutex != null) return error.ReadOnly;
            try self.raw.put(self.dbi, key, value, .{ .append = true });
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            if (self.read_mutex != null) return error.ReadOnly;
            try self.raw.delete(self.dbi, key);
        }

        pub fn openCursor(self: *@This()) !BoundCursor {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return .{ .inner = backend_lmdb_adapter.Cursor.init(try self.raw.cursor(self.dbi)), .read_mutex = self.read_mutex };
        }
    };

    const BoundCursor = struct {
        inner: backend_lmdb_adapter.Cursor,
        read_mutex: ?*std.atomic.Mutex,
        pub fn close(self: *@This()) void {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            self.inner.close();
        }
        pub fn first(self: *@This()) !?backend_erased.Entry {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return self.inner.first();
        }
        pub fn last(self: *@This()) !?backend_erased.Entry {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return self.inner.last();
        }
        pub fn next(self: *@This()) !?backend_erased.Entry {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return self.inner.next();
        }
        pub fn prev(self: *@This()) !?backend_erased.Entry {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return self.inner.prev();
        }
        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_erased.Entry {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return self.inner.seekAtOrAfter(key);
        }
        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_erased.Entry {
            if (self.read_mutex) |mutex| platform.sync.lockYielding(mutex);
            defer if (self.read_mutex) |mutex| mutex.unlock();
            return self.inner.seekAtOrBefore(key);
        }
    };

    const BoundBatch = struct {
        allocator: Allocator,
        raw: lmdb.Batch,
        dbi: lmdb.Dbi,

        fn open(backend: *Backend, namespace: backend_types.Namespace) !BoundBatch {
            var raw = try backend.env.beginBatch();
            errdefer raw.abort();
            const dbi = try openNamespaceDbiBatch(
                backend.allocator,
                &raw,
                namespace,
                backend.open_options.create_if_missing,
            );
            return .{
                .allocator = backend.allocator,
                .raw = raw,
                .dbi = dbi,
            };
        }

        pub fn abort(self: *@This()) void {
            self.raw.abort();
        }

        pub fn commit(self: *@This()) !void {
            try self.raw.commit();
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            _ = self.allocator;
            return try self.raw.get(self.dbi, key);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            _ = self.allocator;
            try self.raw.put(self.dbi, key, value, .{});
        }

        pub fn appendPut(self: *@This(), key: []const u8, value: []const u8) !void {
            _ = self.allocator;
            try self.raw.put(self.dbi, key, value, .{ .append = true });
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            _ = self.allocator;
            try self.raw.delete(self.dbi, key);
        }
    };

    const NamespaceTxn = struct {
        allocator: Allocator,
        raw: lmdb.Transaction,
        create_if_missing: bool,

        fn open(backend: *Backend, read_only: bool) !NamespaceTxn {
            return .{
                .allocator = backend.allocator,
                .raw = try backend.env.begin(.{ .read_only = read_only }),
                .create_if_missing = !read_only and backend.open_options.create_if_missing,
            };
        }

        pub fn abort(self: *@This()) void {
            self.raw.abort();
        }

        pub fn commit(self: *@This()) !void {
            try self.raw.commit();
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            const dbi = try openNamespaceDbi(self.allocator, &self.raw, namespace, false);
            return try self.raw.get(dbi, key);
        }

        pub fn put(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            const dbi = try openNamespaceDbi(self.allocator, &self.raw, namespace, self.create_if_missing);
            try self.raw.put(dbi, key, value, .{});
        }

        pub fn appendPut(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            const dbi = try openNamespaceDbi(self.allocator, &self.raw, namespace, self.create_if_missing);
            try self.raw.put(dbi, key, value, .{ .append = true });
        }

        pub fn delete(self: *@This(), namespace: backend_types.Namespace, key: []const u8) !void {
            const dbi = try openNamespaceDbi(self.allocator, &self.raw, namespace, false);
            try self.raw.delete(dbi, key);
        }

        pub fn openCursor(self: *@This(), namespace: backend_types.Namespace) !backend_lmdb_adapter.Cursor {
            const dbi = try openNamespaceDbi(self.allocator, &self.raw, namespace, false);
            return backend_lmdb_adapter.Cursor.init(try self.raw.cursor(dbi));
        }
    };

    const NamespaceBatch = struct {
        allocator: Allocator,
        raw: lmdb.Batch,
        create_if_missing: bool,

        fn open(backend: *Backend) !NamespaceBatch {
            return .{
                .allocator = backend.allocator,
                .raw = try backend.env.beginBatch(),
                .create_if_missing = backend.open_options.create_if_missing,
            };
        }

        pub fn abort(self: *@This()) void {
            self.raw.abort();
        }

        pub fn commit(self: *@This()) !void {
            try self.raw.commit();
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            const dbi = try openNamespaceDbiBatch(self.allocator, &self.raw, namespace, false);
            return try self.raw.get(dbi, key);
        }

        pub fn put(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            const dbi = try openNamespaceDbiBatch(self.allocator, &self.raw, namespace, self.create_if_missing);
            try self.raw.put(dbi, key, value, .{});
        }

        pub fn appendPut(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            const dbi = try openNamespaceDbiBatch(self.allocator, &self.raw, namespace, self.create_if_missing);
            try self.raw.put(dbi, key, value, .{ .append = true });
        }

        pub fn delete(self: *@This(), namespace: backend_types.Namespace, key: []const u8) !void {
            const dbi = try openNamespaceDbiBatch(self.allocator, &self.raw, namespace, false);
            try self.raw.delete(dbi, key);
        }

        pub fn openCursor(self: *@This(), namespace: backend_types.Namespace) !backend_lmdb_adapter.Cursor {
            const dbi = try openNamespaceDbiBatch(self.allocator, &self.raw, namespace, false);
            return backend_lmdb_adapter.Cursor.init(try self.raw.cursor(dbi));
        }
    };

    pub fn open(allocator: Allocator, path: [*:0]const u8, options: Options) !Backend {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        if (options.backend.create_if_missing) {
            try std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(path));
        }

        var env_options = options.env;
        env_options.read_only = options.backend.read_only;
        env_options.max_dbs = @max(env_options.max_dbs, 1);
        switch (options.backend.durability) {
            .none => {
                env_options.no_sync = true;
                env_options.no_meta_sync = true;
            },
            .data => {
                env_options.no_meta_sync = true;
            },
            .full => {},
        }

        const env = try lmdb.Environment.open(path, env_options);
        return .{
            .allocator = allocator,
            .env = env,
            .open_options = options.backend,
            .read_fork_enabled = options.env.no_tls,
        };
    }

    pub fn close(self: *Backend) void {
        self.env.close();
        self.* = undefined;
    }

    pub fn sync(self: *Backend, force: bool) !void {
        try self.env.sync(force);
    }

    pub fn commitStatsSnapshot(self: *Backend) ?lmdb.CommitStats {
        return self.env.commitStatsSnapshot();
    }

    pub fn capabilities(_: *Backend) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = false,
            .cursors = false,
            .ordered_append_puts = true,
            .native_namespaces = true,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn beginRead(self: *Backend) !NamespaceTxn {
        return try NamespaceTxn.open(self, true);
    }

    pub fn beginWrite(self: *Backend) !NamespaceTxn {
        return try NamespaceTxn.open(self, false);
    }

    pub fn beginBatch(self: *Backend) !NamespaceBatch {
        return try NamespaceBatch.open(self);
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

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-lmdb-backend-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "lmdb backend read forks retain snapshots and cursor lifetimes and reject TLS sharing" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |no_tls| {
        var path_buf: [256]u8 = undefined;
        const path = tmpPath(&path_buf, "forks");
        defer cleanupTmp(path);
        var backend = try Backend.open(a, path, .{ .env = .{ .max_dbs = 32, .no_tls = no_tls } });
        defer backend.close();
        var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
        defer runtime.deinit();
        var writer = try runtime.beginWrite();
        try writer.put("a", "before");
        try writer.put("b", "second");
        try writer.commit();
        var original = try runtime.beginRead();
        if (!no_tls) {
            defer original.abort();
            try std.testing.expectError(error.ReadSnapshotForkUnsupported, original.forkRead());
            continue;
        }
        var first = try original.forkRead();
        defer first.abort();
        var second = try original.forkRead();
        original.abort();
        var cursor = try second.openCursor();
        defer cursor.close();
        second.abort();
        writer = try runtime.beginWrite();
        try writer.put("a", "after");
        try writer.commit();
        try std.testing.expectEqualStrings("before", try first.get("a"));
        try std.testing.expectEqualStrings("before", (try cursor.first()).?.value);
        var other = try first.openCursor();
        defer other.close();
        try std.testing.expectEqualStrings("second", (try other.last()).?.value);
        try std.testing.expectEqualStrings("second", (try cursor.next()).?.value);
        try std.testing.expectEqualStrings("before", (try other.prev()).?.value);
    }
}

test "lmdb backend runtime erases namespace store handles" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "ns");
    defer cleanupTmp(path);

    var backend = try Backend.open(std.testing.allocator, path, .{});
    defer backend.close();

    var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().native_namespaces);
    try std.testing.expect(runtime.capabilities().ordered_append_puts);

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "meta:lsn", "1");
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("1", try txn.get(.{}, "meta:lsn"));
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("B", try txn.get(.{ .name = "docs" }, "doc:b"));
    }
}

test "lmdb backend runtime erases bound store handles with cursor access" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "bound");
    defer cleanupTmp(path);

    var backend = try Backend.open(std.testing.allocator, path, .{});
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
    }
}

test "lmdb backend bound runtime commits larger sorted write set" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "bench-write");
    defer cleanupTmp(path);

    var backend = try Backend.open(std.testing.allocator, path, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    var txn = try runtime.beginWrite();
    errdefer txn.abort();

    var key_buf: [32]u8 = undefined;
    for (0..1000) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i});
        try txn.put(key, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    }
    try txn.commit();

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings(
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        try read.get("doc:00000000"),
    );
    try std.testing.expectEqualStrings(
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        try read.get("doc:00000999"),
    );
}

test "lmdb backend bound runtime commits larger sorted write set with local gpa inputs" {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "bench-write-gpa");
    defer cleanupTmp(path);

    var backend = try Backend.open(alloc, path, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    const value = try alloc.dupe(u8, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    defer alloc.free(value);

    const keys = try alloc.alloc([]u8, 1000);
    defer {
        for (keys) |key| alloc.free(key);
        alloc.free(keys);
    }
    for (keys, 0..) |*slot, i| {
        slot.* = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i});
    }

    var txn = try runtime.beginWrite();
    errdefer txn.abort();
    for (keys) |key| {
        try txn.put(key, value);
    }
    try txn.commit();

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings(value, try read.get("doc:00000000"));
    try std.testing.expectEqualStrings(value, try read.get("doc:00000999"));
}
