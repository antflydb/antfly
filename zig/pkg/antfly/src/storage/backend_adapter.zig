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
const backend_types = @import("backend_types.zig");

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub fn Store(
    comptime Impl: type,
    comptime ReadHandle: type,
    comptime WriteHandle: type,
    comptime BatchHandle: type,
    comptime ops: anytype,
) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn capabilities(self: *@This()) backend_types.Capabilities {
            return ops.capabilities(self.impl);
        }

        pub fn beginRead(self: *@This()) !ReadHandle {
            return try ops.begin_read(self.impl);
        }

        pub fn beginWrite(self: *@This()) !WriteHandle {
            return try ops.begin_write(self.impl);
        }

        pub fn beginBatch(self: *@This()) !BatchHandle {
            return try ops.begin_batch(self.impl);
        }

        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !BatchHandle {
            if (comptime @hasField(@TypeOf(ops), "begin_batch_with_options")) {
                return try ops.begin_batch_with_options(self.impl, options);
            }
            return try ops.begin_batch(self.impl);
        }
    };
}

pub fn Cursor(comptime Impl: type, comptime ops: anytype) type {
    return struct {
        impl: Impl,

        pub fn init(impl: Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn close(self: *@This()) void {
            ops.close(&self.impl);
        }

        pub fn first(self: *@This()) !?Entry {
            return ops.first(&self.impl) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn last(self: *@This()) !?Entry {
            return ops.last(&self.impl) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn next(self: *@This()) !?Entry {
            return ops.next(&self.impl) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn prev(self: *@This()) !?Entry {
            return ops.prev(&self.impl) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
            return ops.seek_at_or_after(&self.impl, key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?Entry {
            return ops.seek_at_or_before(&self.impl, key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn start(self: *@This(), options: backend_types.CursorOptions) !?Entry {
            return switch (options.start) {
                .first => switch (options.order) {
                    .forward => try self.first(),
                    .reverse => try self.last(),
                },
                .at_or_after => |key| try self.seekAtOrAfter(key),
                .at_or_before => |key| try self.seekAtOrBefore(key),
            };
        }
    };
}

pub fn ReadTxn(comptime Impl: type, comptime CursorAdapter: type, comptime ops: anytype) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn abort(self: *@This()) void {
            ops.abort(self.impl);
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return try ops.get(self.impl, key);
        }

        pub fn openCursor(self: *@This()) !CursorAdapter {
            return try ops.open_cursor(self.impl);
        }
    };
}

pub fn WriteTxn(comptime Impl: type, comptime CursorAdapter: type, comptime ops: anytype) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn abort(self: *@This()) void {
            ops.abort(self.impl);
        }

        pub fn commit(self: *@This()) !void {
            try ops.commit(self.impl);
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return try ops.get(self.impl, key);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try ops.put(self.impl, key, value);
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            try ops.delete(self.impl, key);
        }

        pub fn openCursor(self: *@This()) !CursorAdapter {
            return try ops.open_cursor(self.impl);
        }
    };
}

pub fn NamespaceReadTxn(comptime Impl: type, comptime Namespace: type, comptime ops: anytype) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn abort(self: *@This()) void {
            ops.abort(self.impl);
        }

        pub fn get(self: *@This(), namespace: Namespace, key: []const u8) ![]const u8 {
            return try ops.get(self.impl, namespace, key);
        }

        pub fn getManySorted(self: *@This(), namespace: Namespace, keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (comptime @hasField(@TypeOf(ops), "get_many_sorted")) {
                return try ops.get_many_sorted(self.impl, namespace, keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = ops.get(self.impl, namespace, key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }
    };
}

pub fn NamespaceWriteTxn(comptime Impl: type, comptime Namespace: type, comptime ops: anytype) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn abort(self: *@This()) void {
            ops.abort(self.impl);
        }

        pub fn commit(self: *@This()) !void {
            try ops.commit(self.impl);
        }

        pub fn get(self: *@This(), namespace: Namespace, key: []const u8) ![]const u8 {
            return try ops.get(self.impl, namespace, key);
        }

        pub fn put(self: *@This(), namespace: Namespace, key: []const u8, value: []const u8) !void {
            try ops.put(self.impl, namespace, key, value);
        }

        pub fn delete(self: *@This(), namespace: Namespace, key: []const u8) !void {
            try ops.delete(self.impl, namespace, key);
        }
    };
}

pub fn NamespaceBatch(comptime Impl: type, comptime Namespace: type, comptime ops: anytype) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn abort(self: *@This()) void {
            ops.abort(self.impl);
        }

        pub fn commit(self: *@This()) !void {
            try ops.commit(self.impl);
        }

        pub fn get(self: *@This(), namespace: Namespace, key: []const u8) ![]const u8 {
            return try ops.get(self.impl, namespace, key);
        }

        pub fn put(self: *@This(), namespace: Namespace, key: []const u8, value: []const u8) !void {
            try ops.put(self.impl, namespace, key, value);
        }

        pub fn delete(self: *@This(), namespace: Namespace, key: []const u8) !void {
            try ops.delete(self.impl, namespace, key);
        }
    };
}

pub fn Batch(comptime Impl: type, comptime ops: anytype) type {
    return struct {
        impl: *Impl,

        pub fn init(impl: *Impl) @This() {
            return .{ .impl = impl };
        }

        pub fn abort(self: *@This()) void {
            ops.abort(self.impl);
        }

        pub fn commit(self: *@This()) !void {
            try ops.commit(self.impl);
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return try ops.get(self.impl, key);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try ops.put(self.impl, key, value);
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            try ops.delete(self.impl, key);
        }
    };
}

test "cursor adapter normalizes not found and start semantics" {
    const MockCursor = struct {
        items: []const Entry,
        index: usize = 0,
        initialized: bool = false,

        fn close(_: *@This()) void {}

        fn first(self: *@This()) !Entry {
            if (self.items.len == 0) return error.NotFound;
            self.index = 0;
            self.initialized = true;
            return self.items[self.index];
        }

        fn last(self: *@This()) !Entry {
            if (self.items.len == 0) return error.NotFound;
            self.index = self.items.len - 1;
            self.initialized = true;
            return self.items[self.index];
        }

        fn next(self: *@This()) !Entry {
            if (!self.initialized) return self.first();
            if (self.index + 1 >= self.items.len) return error.NotFound;
            self.index += 1;
            return self.items[self.index];
        }

        fn prev(self: *@This()) !Entry {
            if (!self.initialized) return self.last();
            if (self.index == 0) return error.NotFound;
            self.index -= 1;
            return self.items[self.index];
        }

        fn seekAtOrAfter(self: *@This(), key: []const u8) !Entry {
            for (self.items, 0..) |item, i| {
                if (std.mem.order(u8, item.key, key) != .lt) {
                    self.index = i;
                    self.initialized = true;
                    return item;
                }
            }
            return error.NotFound;
        }

        fn seekAtOrBefore(self: *@This(), key: []const u8) !Entry {
            var found: ?usize = null;
            for (self.items, 0..) |item, i| {
                if (std.mem.order(u8, item.key, key) != .gt) found = i;
            }
            const idx = found orelse return error.NotFound;
            self.index = idx;
            self.initialized = true;
            return self.items[idx];
        }
    };

    const MockCursorAdapter = Cursor(MockCursor, .{
        .close = MockCursor.close,
        .first = MockCursor.first,
        .last = MockCursor.last,
        .next = MockCursor.next,
        .prev = MockCursor.prev,
        .seek_at_or_after = MockCursor.seekAtOrAfter,
        .seek_at_or_before = MockCursor.seekAtOrBefore,
    });

    const items = [_]Entry{
        .{ .key = "a", .value = "1" },
        .{ .key = "b", .value = "2" },
        .{ .key = "d", .value = "4" },
    };

    var cursor = MockCursorAdapter.init(.{ .items = &items });
    try std.testing.expectEqualStrings("a", (try cursor.start(.{})).?.key);
    try std.testing.expectEqualStrings("d", (try cursor.start(.{ .order = .reverse })).?.key);
    try std.testing.expectEqualStrings("b", (try cursor.seekAtOrAfter("b")).?.key);
    try std.testing.expectEqualStrings("b", (try cursor.seekAtOrBefore("c")).?.key);
    try std.testing.expectEqual(@as(?Entry, null), try cursor.seekAtOrAfter("z"));
}

test "write transaction and batch adapters reuse concrete methods" {
    const MockCursor = struct {
        fn close(_: *@This()) void {}
        fn first(_: *@This()) !Entry {
            return error.NotFound;
        }
        fn last(_: *@This()) !Entry {
            return error.NotFound;
        }
        fn next(_: *@This()) !Entry {
            return error.NotFound;
        }
        fn prev(_: *@This()) !Entry {
            return error.NotFound;
        }
        fn seekAtOrAfter(_: *@This(), _: []const u8) !Entry {
            return error.NotFound;
        }
        fn seekAtOrBefore(_: *@This(), _: []const u8) !Entry {
            return error.NotFound;
        }
    };

    const MockCursorAdapter = Cursor(MockCursor, .{
        .close = MockCursor.close,
        .first = MockCursor.first,
        .last = MockCursor.last,
        .next = MockCursor.next,
        .prev = MockCursor.prev,
        .seek_at_or_after = MockCursor.seekAtOrAfter,
        .seek_at_or_before = MockCursor.seekAtOrBefore,
    });

    const MockTxn = struct {
        value: ?[]const u8 = null,
        committed: bool = false,
        aborted: bool = false,

        fn abort(self: *@This()) void {
            self.aborted = true;
        }

        fn commit(self: *@This()) !void {
            self.committed = true;
        }

        fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse return error.NotFound;
        }

        fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }

        fn delete(self: *@This(), _: []const u8) !void {
            self.value = null;
        }

        fn openCursor(_: *@This()) !MockCursorAdapter {
            return MockCursorAdapter.init(.{});
        }
    };

    const MockWriteTxn = WriteTxn(MockTxn, MockCursorAdapter, .{
        .abort = MockTxn.abort,
        .commit = MockTxn.commit,
        .get = MockTxn.get,
        .put = MockTxn.put,
        .delete = MockTxn.delete,
        .open_cursor = MockTxn.openCursor,
    });

    const MockBatch = struct {
        value: ?[]const u8 = null,
        committed: bool = false,
        aborted: bool = false,

        fn abort(self: *@This()) void {
            self.aborted = true;
        }

        fn commit(self: *@This()) !void {
            self.committed = true;
        }

        fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse return error.NotFound;
        }

        fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }

        fn delete(self: *@This(), _: []const u8) !void {
            self.value = null;
        }
    };

    const MockBatchAdapter = Batch(MockBatch, .{
        .abort = MockBatch.abort,
        .commit = MockBatch.commit,
        .get = MockBatch.get,
        .put = MockBatch.put,
        .delete = MockBatch.delete,
    });

    var txn_impl = MockTxn{};
    var txn = MockWriteTxn.init(&txn_impl);
    try txn.put("k", "v");
    try std.testing.expectEqualStrings("v", try txn.get("k"));
    try txn.commit();
    try std.testing.expect(txn_impl.committed);

    var batch_impl = MockBatch{};
    var batch = MockBatchAdapter.init(&batch_impl);
    try batch.put("k", "v2");
    try std.testing.expectEqualStrings("v2", try batch.get("k"));
    try batch.commit();
    try std.testing.expect(batch_impl.committed);
}

test "store adapter opens read write and batch handles" {
    const MockReadTxn = struct {
        value: []const u8,

        fn abort(_: *@This()) void {}
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            try std.testing.expectEqualStrings("k", key);
            return self.value;
        }
    };

    const MockWriteTxn = struct {
        value: ?[]const u8 = null,

        fn abort(_: *@This()) void {}
        fn commit(_: *@This()) !void {}
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            try std.testing.expectEqualStrings("k", key);
            return self.value orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings("k", key);
            self.value = value;
        }
        fn delete(self: *@This(), key: []const u8) !void {
            try std.testing.expectEqualStrings("k", key);
            self.value = null;
        }
    };

    const MockBatch = struct {
        value: ?[]const u8 = null,

        fn abort(_: *@This()) void {}
        fn commit(_: *@This()) !void {}
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            try std.testing.expectEqualStrings("k", key);
            return self.value orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings("k", key);
            self.value = value;
        }
        fn delete(self: *@This(), key: []const u8) !void {
            try std.testing.expectEqualStrings("k", key);
            self.value = null;
        }
    };

    const MockStoreImpl = struct {
        read_txn: MockReadTxn = .{ .value = "v" },
        write_txn: MockWriteTxn = .{},
        batch: MockBatch = .{},

        fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{ .native_namespaces = true };
        }

        fn beginRead(self: *@This()) !MockReadTxn {
            return self.read_txn;
        }

        fn beginWrite(self: *@This()) !MockWriteTxn {
            return self.write_txn;
        }

        fn beginBatch(self: *@This()) !MockBatch {
            return self.batch;
        }
    };

    const MockStore = Store(MockStoreImpl, MockReadTxn, MockWriteTxn, MockBatch, .{
        .capabilities = MockStoreImpl.capabilities,
        .begin_read = MockStoreImpl.beginRead,
        .begin_write = MockStoreImpl.beginWrite,
        .begin_batch = MockStoreImpl.beginBatch,
    });

    var impl: MockStoreImpl = .{};
    var store = MockStore.init(&impl);
    try std.testing.expect(store.capabilities().native_namespaces);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("v", try read.get("k"));

    var write = try store.beginWrite();
    errdefer write.abort();
    try write.put("k", "w");
    try std.testing.expectEqualStrings("w", try write.get("k"));
    try write.commit();

    var batch = try store.beginBatch();
    errdefer batch.abort();
    try batch.put("k", "b");
    try std.testing.expectEqualStrings("b", try batch.get("k"));
    try batch.commit();
}

test "namespace transaction adapters route operations through logical partitions" {
    const Ns = enum { a, b };

    const MockTxn = struct {
        a_value: ?[]const u8 = null,
        b_value: ?[]const u8 = null,
        committed: bool = false,
        aborted: bool = false,

        fn abort(self: *@This()) void {
            self.aborted = true;
        }

        fn commit(self: *@This()) !void {
            self.committed = true;
        }

        fn get(self: *@This(), namespace: Ns, _: []const u8) ![]const u8 {
            return switch (namespace) {
                .a => self.a_value orelse error.NotFound,
                .b => self.b_value orelse error.NotFound,
            };
        }

        fn put(self: *@This(), namespace: Ns, _: []const u8, value: []const u8) !void {
            switch (namespace) {
                .a => self.a_value = value,
                .b => self.b_value = value,
            }
        }

        fn delete(self: *@This(), namespace: Ns, _: []const u8) !void {
            switch (namespace) {
                .a => self.a_value = null,
                .b => self.b_value = null,
            }
        }
    };

    const MockReadTxn = NamespaceReadTxn(MockTxn, Ns, .{
        .abort = MockTxn.abort,
        .get = MockTxn.get,
    });
    const MockWriteTxn = NamespaceWriteTxn(MockTxn, Ns, .{
        .abort = MockTxn.abort,
        .commit = MockTxn.commit,
        .get = MockTxn.get,
        .put = MockTxn.put,
        .delete = MockTxn.delete,
    });

    var impl = MockTxn{};
    {
        var write = MockWriteTxn.init(&impl);
        try write.put(.a, "k", "va");
        try write.put(.b, "k", "vb");
        try std.testing.expectEqualStrings("va", try write.get(.a, "k"));
        try std.testing.expectEqualStrings("vb", try write.get(.b, "k"));
        try write.delete(.a, "k");
        try write.commit();
        try std.testing.expect(impl.committed);
    }
    {
        var read = MockReadTxn.init(&impl);
        try std.testing.expectError(error.NotFound, read.get(.a, "k"));
        try std.testing.expectEqualStrings("vb", try read.get(.b, "k"));
    }
}

test "namespace batch adapter routes operations through logical partitions" {
    const Ns = enum { a, b };

    const MockBatch = struct {
        a_value: ?[]const u8 = null,
        b_value: ?[]const u8 = null,
        committed: bool = false,
        aborted: bool = false,

        fn abort(self: *@This()) void {
            self.aborted = true;
        }

        fn commit(self: *@This()) !void {
            self.committed = true;
        }

        fn get(self: *@This(), namespace: Ns, _: []const u8) ![]const u8 {
            return switch (namespace) {
                .a => self.a_value orelse error.NotFound,
                .b => self.b_value orelse error.NotFound,
            };
        }

        fn put(self: *@This(), namespace: Ns, _: []const u8, value: []const u8) !void {
            switch (namespace) {
                .a => self.a_value = value,
                .b => self.b_value = value,
            }
        }

        fn delete(self: *@This(), namespace: Ns, _: []const u8) !void {
            switch (namespace) {
                .a => self.a_value = null,
                .b => self.b_value = null,
            }
        }
    };

    const MockBatchAdapter = NamespaceBatch(MockBatch, Ns, .{
        .abort = MockBatch.abort,
        .commit = MockBatch.commit,
        .get = MockBatch.get,
        .put = MockBatch.put,
        .delete = MockBatch.delete,
    });

    var impl = MockBatch{};
    var batch = MockBatchAdapter.init(&impl);
    try batch.put(.a, "k", "va");
    try batch.put(.b, "k", "vb");
    try std.testing.expectEqualStrings("va", try batch.get(.a, "k"));
    try std.testing.expectEqualStrings("vb", try batch.get(.b, "k"));
    try batch.delete(.a, "k");
    try batch.commit();
    try std.testing.expect(impl.committed);
    try std.testing.expectEqual(@as(?[]const u8, null), impl.a_value);
    try std.testing.expectEqualStrings("vb", impl.b_value.?);
}
