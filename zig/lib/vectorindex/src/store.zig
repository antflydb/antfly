// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const Namespace = enum {
    nodes,
    meta,
    quant,
    vecs,
};

pub const BatchMode = enum {
    default,
    bulk_ingest,
};

pub const BatchOptions = struct {
    mode: BatchMode = .default,
    defer_commit_flush: bool = false,
};

fn entryFrom(value: anytype) Entry {
    return .{
        .key = value.key,
        .value = value.value,
    };
}

fn Box(comptime T: type) type {
    return struct {
        handle: T,
    };
}

fn allocBox(allocator: Allocator, value: anytype) !*Box(@TypeOf(value)) {
    const T = @TypeOf(value);
    const box = try allocator.create(Box(T));
    box.* = .{ .handle = value };
    return box;
}

pub const Cursor = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        close: *const fn (Allocator, *anyopaque) void,
        first: *const fn (*anyopaque) anyerror!?Entry,
        last: *const fn (*anyopaque) anyerror!?Entry,
        next: *const fn (*anyopaque) anyerror!?Entry,
        prev: *const fn (*anyopaque) anyerror!?Entry,
        seek_at_or_after: *const fn (*anyopaque, []const u8) anyerror!?Entry,
        seek_at_or_before: *const fn (*anyopaque, []const u8) anyerror!?Entry,
    };

    pub fn close(self: *Cursor) void {
        self.vtable.close(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn first(self: *Cursor) !?Entry {
        return try self.vtable.first(self.ptr);
    }

    pub fn last(self: *Cursor) !?Entry {
        return try self.vtable.last(self.ptr);
    }

    pub fn next(self: *Cursor) !?Entry {
        return try self.vtable.next(self.ptr);
    }

    pub fn prev(self: *Cursor) !?Entry {
        return try self.vtable.prev(self.ptr);
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !?Entry {
        return try self.vtable.seek_at_or_after(self.ptr, key);
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !?Entry {
        return try self.vtable.seek_at_or_before(self.ptr, key);
    }
};

pub const NamespaceReadTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, Namespace, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        open_cursor: ?*const fn (Allocator, *anyopaque, Namespace) anyerror!Cursor = null,
    };

    pub fn abort(self: *NamespaceReadTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceReadTxn, namespace: Namespace, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, namespace, key);
    }

    pub fn getManySorted(self: *NamespaceReadTxn, namespace: Namespace, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try get_many_sorted(self.ptr, namespace, keys, values);
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(namespace, key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn openCursor(self: *NamespaceReadTxn, namespace: Namespace) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try open_cursor(self.allocator, self.ptr, namespace);
    }
};

pub const NamespaceWriteTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, Namespace, []const u8) anyerror![]const u8,
        put: *const fn (*anyopaque, Namespace, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, Namespace, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, Namespace, []const u8) anyerror!void,
        open_cursor: ?*const fn (Allocator, *anyopaque, Namespace) anyerror!Cursor = null,
    };

    pub fn abort(self: *NamespaceWriteTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *NamespaceWriteTxn) !void {
        try self.vtable.commit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceWriteTxn, namespace: Namespace, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, namespace, key);
    }

    pub fn put(self: *NamespaceWriteTxn, namespace: Namespace, key: []const u8, value: []const u8) !void {
        try self.vtable.put(self.ptr, namespace, key, value);
    }

    pub fn appendPut(self: *NamespaceWriteTxn, namespace: Namespace, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try append_put(self.ptr, namespace, key, value);
    }

    pub fn delete(self: *NamespaceWriteTxn, namespace: Namespace, key: []const u8) !void {
        try self.vtable.delete(self.ptr, namespace, key);
    }

    pub fn openCursor(self: *NamespaceWriteTxn, namespace: Namespace) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try open_cursor(self.allocator, self.ptr, namespace);
    }
};

pub const NamespaceBatch = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, Namespace, []const u8) anyerror![]const u8,
        put: *const fn (*anyopaque, Namespace, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, Namespace, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, Namespace, []const u8) anyerror!void,
    };

    pub fn abort(self: *NamespaceBatch) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *NamespaceBatch) !void {
        try self.vtable.commit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceBatch, namespace: Namespace, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, namespace, key);
    }

    pub fn put(self: *NamespaceBatch, namespace: Namespace, key: []const u8, value: []const u8) !void {
        try self.vtable.put(self.ptr, namespace, key, value);
    }

    pub fn appendPut(self: *NamespaceBatch, namespace: Namespace, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try append_put(self.ptr, namespace, key, value);
    }

    pub fn delete(self: *NamespaceBatch, namespace: Namespace, key: []const u8) !void {
        try self.vtable.delete(self.ptr, namespace, key);
    }
};

pub const NamespaceStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        begin_read: *const fn (Allocator, *anyopaque) anyerror!NamespaceReadTxn,
        begin_probe: ?*const fn (Allocator, *anyopaque) anyerror!NamespaceReadTxn = null,
        begin_write: *const fn (Allocator, *anyopaque) anyerror!NamespaceWriteTxn,
        begin_batch: *const fn (Allocator, *anyopaque) anyerror!NamespaceBatch,
        begin_batch_with_options: ?*const fn (Allocator, *anyopaque, BatchOptions) anyerror!NamespaceBatch = null,
    };

    pub fn deinit(self: *NamespaceStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn beginRead(self: *NamespaceStore) !NamespaceReadTxn {
        return try self.vtable.begin_read(self.allocator, self.ptr);
    }

    pub fn beginProbeOrRead(self: *NamespaceStore) !NamespaceReadTxn {
        if (self.vtable.begin_probe) |begin_probe| {
            return try begin_probe(self.allocator, self.ptr);
        }
        return try self.beginRead();
    }

    pub fn beginWrite(self: *NamespaceStore) !NamespaceWriteTxn {
        return try self.vtable.begin_write(self.allocator, self.ptr);
    }

    pub fn beginBatch(self: *NamespaceStore) !NamespaceBatch {
        return try self.vtable.begin_batch(self.allocator, self.ptr);
    }

    pub fn beginBatchWithOptions(self: *NamespaceStore, options: BatchOptions) !NamespaceBatch {
        if (self.vtable.begin_batch_with_options) |begin_batch_with_options| {
            return try begin_batch_with_options(self.allocator, self.ptr, options);
        }
        return try self.beginBatch();
    }
};

pub fn cursorFrom(allocator: Allocator, handle: anytype) !Cursor {
    const Handle = @TypeOf(handle);
    const box_ptr = try allocBox(allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn close(alloc: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            state.handle.close();
            alloc.destroy(state);
        }

        fn first(ptr: *anyopaque) anyerror!?Entry {
            return if ((unbox(ptr).handle.first() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            })) |entry| entryFrom(entry) else null;
        }

        fn last(ptr: *anyopaque) anyerror!?Entry {
            return if ((unbox(ptr).handle.last() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            })) |entry| entryFrom(entry) else null;
        }

        fn next(ptr: *anyopaque) anyerror!?Entry {
            return if ((unbox(ptr).handle.next() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            })) |entry| entryFrom(entry) else null;
        }

        fn prev(ptr: *anyopaque) anyerror!?Entry {
            return if ((unbox(ptr).handle.prev() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            })) |entry| entryFrom(entry) else null;
        }

        fn seekAtOrAfter(ptr: *anyopaque, key: []const u8) anyerror!?Entry {
            return if ((unbox(ptr).handle.seekAtOrAfter(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            })) |entry| entryFrom(entry) else null;
        }

        fn seekAtOrBefore(ptr: *anyopaque, key: []const u8) anyerror!?Entry {
            return if ((unbox(ptr).handle.seekAtOrBefore(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            })) |entry| entryFrom(entry) else null;
        }
    };

    return .{
        .allocator = allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .close = vt.close,
            .first = vt.first,
            .last = vt.last,
            .next = vt.next,
            .prev = vt.prev,
            .seek_at_or_after = vt.seekAtOrAfter,
            .seek_at_or_before = vt.seekAtOrBefore,
        },
    };
}

pub fn namespaceReadTxnFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (Namespace) anyerror!LocalNamespace,
) !NamespaceReadTxn {
    const Handle = @TypeOf(handle);
    const box_ptr = try allocBox(allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(alloc: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            state.handle.abort();
            alloc.destroy(state);
        }

        fn get(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        fn getManySorted(ptr: *anyopaque, namespace: Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(try mapNamespace(namespace), keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(try mapNamespace(namespace), key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque, namespace: Namespace) anyerror!Cursor {
            return try cursorFrom(alloc, try unbox(ptr).handle.openCursor(try mapNamespace(namespace)));
        }
    };

    return .{
        .allocator = allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
        },
    };
}

pub fn namespaceWriteTxnFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (Namespace) anyerror!LocalNamespace,
) !NamespaceWriteTxn {
    const Handle = @TypeOf(handle);
    const box_ptr = try allocBox(allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(alloc: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            state.handle.abort();
            alloc.destroy(state);
        }

        fn commit(alloc: Allocator, ptr: *anyopaque) anyerror!void {
            const state = unbox(ptr);
            try state.handle.commit();
            alloc.destroy(state);
        }

        fn get(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        fn put(ptr: *anyopaque, namespace: Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(try mapNamespace(namespace), key, value);
        }

        fn appendPut(ptr: *anyopaque, namespace: Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(try mapNamespace(namespace), key, value);
        }

        fn delete(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(try mapNamespace(namespace), key);
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque, namespace: Namespace) anyerror!Cursor {
            return try cursorFrom(alloc, try unbox(ptr).handle.openCursor(try mapNamespace(namespace)));
        }
    };

    return .{
        .allocator = allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
        },
    };
}

pub fn namespaceBatchFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (Namespace) anyerror!LocalNamespace,
) !NamespaceBatch {
    const Handle = @TypeOf(handle);
    const box_ptr = try allocBox(allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(alloc: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            state.handle.abort();
            alloc.destroy(state);
        }

        fn commit(alloc: Allocator, ptr: *anyopaque) anyerror!void {
            const state = unbox(ptr);
            try state.handle.commit();
            alloc.destroy(state);
        }

        fn get(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        fn put(ptr: *anyopaque, namespace: Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(try mapNamespace(namespace), key, value);
        }

        fn appendPut(ptr: *anyopaque, namespace: Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(try mapNamespace(namespace), key, value);
        }

        fn delete(ptr: *anyopaque, namespace: Namespace, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(try mapNamespace(namespace), key);
        }
    };

    return .{
        .allocator = allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
        },
    };
}

pub fn namespaceStoreFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (Namespace) anyerror!LocalNamespace,
) !NamespaceStore {
    const Handle = @TypeOf(handle);
    const box_ptr = try allocBox(allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn deinit(alloc: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            if (@hasDecl(Handle, "deinit")) {
                state.handle.deinit();
            }
            alloc.destroy(state);
        }

        fn beginRead(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceReadTxn {
            return try namespaceReadTxnFrom(alloc, try unbox(ptr).handle.beginRead(), LocalNamespace, mapNamespace);
        }

        fn beginWrite(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceWriteTxn {
            return try namespaceWriteTxnFrom(alloc, try unbox(ptr).handle.beginWrite(), LocalNamespace, mapNamespace);
        }

        fn beginProbe(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceReadTxn {
            const HandleDecl = switch (@typeInfo(Handle)) {
                .pointer => |pointer| pointer.child,
                else => Handle,
            };
            if (@hasDecl(HandleDecl, "beginProbe")) {
                return try namespaceReadTxnFrom(alloc, try unbox(ptr).handle.beginProbe(), LocalNamespace, mapNamespace);
            }
            return try beginRead(alloc, ptr);
        }

        fn beginBatch(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceBatch {
            return try namespaceBatchFrom(alloc, try unbox(ptr).handle.beginBatch(), LocalNamespace, mapNamespace);
        }

        fn beginBatchWithOptions(alloc: Allocator, ptr: *anyopaque, options: BatchOptions) anyerror!NamespaceBatch {
            const HandleDecl = switch (@typeInfo(Handle)) {
                .pointer => |pointer| pointer.child,
                else => Handle,
            };
            if (@hasDecl(HandleDecl, "beginBatchWithOptions")) {
                return try namespaceBatchFrom(
                    alloc,
                    try unbox(ptr).handle.beginBatchWithOptions(.{
                        .mode = switch (options.mode) {
                            .default => .default,
                            .bulk_ingest => .bulk_ingest,
                        },
                        .defer_commit_flush = options.defer_commit_flush,
                    }),
                    LocalNamespace,
                    mapNamespace,
                );
            }
            return try beginBatch(alloc, ptr);
        }
    };

    return .{
        .allocator = allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .deinit = vt.deinit,
            .begin_read = vt.beginRead,
            .begin_probe = vt.beginProbe,
            .begin_write = vt.beginWrite,
            .begin_batch = vt.beginBatch,
            .begin_batch_with_options = vt.beginBatchWithOptions,
        },
    };
}

fn identityNamespace(namespace: Namespace) !Namespace {
    return namespace;
}

const FailingCommitCursor = struct {
    pub fn close(_: *@This()) void {}
    pub fn first(_: *@This()) !?Entry {
        return null;
    }
    pub fn last(_: *@This()) !?Entry {
        return null;
    }
    pub fn next(_: *@This()) !?Entry {
        return null;
    }
    pub fn prev(_: *@This()) !?Entry {
        return null;
    }
    pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
        return null;
    }
    pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?Entry {
        return null;
    }
};

const FailingCommitShared = struct {
    commits: usize = 0,
    aborted: bool = false,
};

const FailingCommitHandle = struct {
    shared: *FailingCommitShared,

    pub fn abort(self: *@This()) void {
        self.shared.aborted = true;
    }
    pub fn commit(self: *@This()) !void {
        self.shared.commits += 1;
        return error.CommitFailed;
    }
    pub fn get(_: *@This(), _: Namespace, _: []const u8) ![]const u8 {
        return error.NotFound;
    }
    pub fn put(_: *@This(), _: Namespace, _: []const u8, _: []const u8) !void {}
    pub fn delete(_: *@This(), _: Namespace, _: []const u8) !void {}
    pub fn openCursor(_: *@This(), _: Namespace) !FailingCommitCursor {
        return .{};
    }
};

test "failed commit keeps vectorindex namespace write handle abortable" {
    var shared = FailingCommitShared{};
    var txn = try namespaceWriteTxnFrom(std.testing.allocator, FailingCommitHandle{ .shared = &shared }, Namespace, identityNamespace);

    try std.testing.expectError(error.CommitFailed, txn.commit());
    try std.testing.expectEqual(@as(usize, 1), shared.commits);
    txn.abort();
    try std.testing.expect(shared.aborted);
}

test "failed commit keeps vectorindex namespace batch handle abortable" {
    var shared = FailingCommitShared{};
    var batch = try namespaceBatchFrom(std.testing.allocator, FailingCommitHandle{ .shared = &shared }, Namespace, identityNamespace);

    try std.testing.expectError(error.CommitFailed, batch.commit());
    try std.testing.expectEqual(@as(usize, 1), shared.commits);
    batch.abort();
    try std.testing.expect(shared.aborted);
}

test "vectorindex namespace store forwards batch options" {
    const Shared = struct {
        saw_batch_options: bool = false,
        last_mode: BatchMode = .default,
    };

    const MockRead = struct {
        pub fn abort(_: *@This()) void {}
        pub fn get(_: *@This(), _: Namespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn openCursor(_: *@This(), _: Namespace) !FailingCommitCursor {
            return .{};
        }
    };

    const MockWrite = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: Namespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: Namespace, _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: Namespace, _: []const u8) !void {}
        pub fn openCursor(_: *@This(), _: Namespace) !FailingCommitCursor {
            return .{};
        }
    };

    const MockBatch = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: Namespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: Namespace, _: []const u8, _: []const u8) !void {}
        pub fn appendPut(_: *@This(), _: Namespace, _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: Namespace, _: []const u8) !void {}
    };

    const MockStore = struct {
        shared: *Shared,

        pub fn beginRead(_: *@This()) !MockRead {
            return .{};
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(_: *@This()) !MockBatch {
            return .{};
        }

        pub fn beginBatchWithOptions(self: *@This(), options: BatchOptions) !MockBatch {
            self.shared.saw_batch_options = true;
            self.shared.last_mode = options.mode;
            return .{};
        }
    };

    var shared = Shared{};
    const mock = MockStore{ .shared = &shared };
    var store = try namespaceStoreFrom(std.testing.allocator, mock, Namespace, identityNamespace);
    defer store.deinit();

    var batch = try store.beginBatchWithOptions(.{ .mode = .bulk_ingest });
    try batch.commit();
    try std.testing.expect(shared.saw_batch_options);
    try std.testing.expectEqual(BatchMode.bulk_ingest, shared.last_mode);
}
