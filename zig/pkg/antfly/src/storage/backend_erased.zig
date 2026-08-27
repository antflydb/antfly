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

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("backend_adapter.zig");
const backend_types = @import("backend_types.zig");
const change_journal_mod = @import("db/derived/change_journal.zig");
const internal_keys = @import("internal_keys.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");

pub const types = backend_types;

pub const Entry = backend_adapter.Entry;
pub const ReplayEntry = backend_types.ReplayEntry;

fn Box(comptime T: type) type {
    return struct {
        allocator: Allocator,
        handle: T,
    };
}

fn ParentBox(comptime T: type) type {
    return struct {
        allocator: Allocator,
        handle: T,
        mutex: std.atomic.Mutex = .unlocked,
        child_count: usize = 0,
        owner_closed: bool = false,
        finalizing: bool = false,

        fn retainChild(self: *@This()) !void {
            platform.sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.owner_closed or self.finalizing) return error.TransactionClosed;
            self.child_count += 1;
        }

        fn requestAbort(self: *@This()) void {
            platform.sync.lockYielding(&self.mutex);
            std.debug.assert(!self.owner_closed);
            self.owner_closed = true;
            const finalize = self.child_count == 0;
            if (finalize) self.finalizing = true;
            self.mutex.unlock();
            if (finalize) self.finalizeAbort();
        }

        fn releaseChild(self: *@This()) void {
            platform.sync.lockYielding(&self.mutex);
            std.debug.assert(self.child_count > 0);
            self.child_count -= 1;
            const finalize = self.owner_closed and self.child_count == 0 and !self.finalizing;
            if (finalize) self.finalizing = true;
            self.mutex.unlock();
            if (finalize) self.finalizeAbort();
        }

        fn commit(self: *@This()) !void {
            platform.sync.lockYielding(&self.mutex);
            if (self.owner_closed or self.finalizing) {
                self.mutex.unlock();
                return error.TransactionClosed;
            }
            if (self.child_count != 0) {
                self.mutex.unlock();
                return error.TransactionCursorActive;
            }
            self.finalizing = true;
            self.mutex.unlock();

            self.handle.commit() catch |err| {
                platform.sync.lockYielding(&self.mutex);
                self.finalizing = false;
                self.mutex.unlock();
                return err;
            };
            const allocator = self.allocator;
            allocator.destroy(self);
        }

        fn finalizeAbort(self: *@This()) void {
            const allocator = self.allocator;
            self.handle.abort();
            allocator.destroy(self);
        }
    };
}

const ParentRelease = struct {
    ptr: *anyopaque,
    release: *const fn (*anyopaque) void,
};

fn parentReleaseFor(parent: anytype) ParentRelease {
    const Parent = @TypeOf(parent.*);
    const release = struct {
        fn run(ptr: *anyopaque) void {
            const typed: *Parent = @ptrCast(@alignCast(ptr));
            typed.releaseChild();
        }
    }.run;
    return .{ .ptr = parent, .release = release };
}

fn allocBox(allocator: Allocator, value: anytype) !*Box(@TypeOf(value)) {
    const T = @TypeOf(value);
    const box = try allocator.create(Box(T));
    box.* = .{ .allocator = allocator, .handle = value };
    return box;
}

fn allocParentBox(allocator: Allocator, value: anytype) !*ParentBox(@TypeOf(value)) {
    const T = @TypeOf(value);
    const box = try allocator.create(ParentBox(T));
    box.* = .{ .allocator = allocator, .handle = value };
    return box;
}

fn wrapperBoxAllocator(fallback: Allocator) Allocator {
    const effective_fallback = if (!builtin.single_threaded) std.heap.smp_allocator else fallback;
    return platform.allocator.processAllocator(effective_fallback);
}

fn replayHintOrdinalFromSingleMask(mask: u8) ?u8 {
    if (mask == 0 or (mask & (mask - 1)) != 0) return null;
    inline for (std.meta.fields(change_journal_mod.TargetHint)) |field| {
        if (mask == (@as(u8, 1) << @intCast(field.value))) return @intCast(field.value);
    }
    return null;
}

pub const Cursor = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        close: *const fn (Allocator, *anyopaque) void,
        first: *const fn (*anyopaque) anyerror!?Entry,
        last: *const fn (*anyopaque) anyerror!?Entry,
        next: *const fn (*anyopaque) anyerror!?Entry,
        prev: *const fn (*anyopaque) anyerror!?Entry,
        seek_at_or_after: *const fn (*anyopaque, []const u8) anyerror!?Entry,
        seek_at_or_before: *const fn (*anyopaque, []const u8) anyerror!?Entry,
        set_upper_bound: ?*const fn (*anyopaque, ?[]const u8) void = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn close(self: *Cursor) void {
        self.vtable.close(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn first(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("first", self.boundary_dispatch, self.vtable.first, .{self.ptr});
    }

    pub fn last(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("last", self.boundary_dispatch, self.vtable.last, .{self.ptr});
    }

    pub fn next(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("next", self.boundary_dispatch, self.vtable.next, .{self.ptr});
    }

    pub fn prev(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("prev", self.boundary_dispatch, self.vtable.prev, .{self.ptr});
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !?Entry {
        return try BoundaryAbi.call("seek_at_or_after", self.boundary_dispatch, self.vtable.seek_at_or_after, .{ self.ptr, key });
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !?Entry {
        return try BoundaryAbi.call("seek_at_or_before", self.boundary_dispatch, self.vtable.seek_at_or_before, .{ self.ptr, key });
    }

    pub fn setUpperBound(self: *Cursor, upper: ?[]const u8) void {
        if (self.vtable.set_upper_bound) |set_upper_bound| set_upper_bound(self.ptr, upper);
    }

    pub fn start(self: *Cursor, options: backend_types.CursorOptions) !?Entry {
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

pub const ReadTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        open_cursor: *const fn (Allocator, *anyopaque) anyerror!Cursor,
    };

    pub fn abort(self: *ReadTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *ReadTxn, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, key);
    }

    pub fn getManySorted(self: *ReadTxn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try get_many_sorted(self.ptr, keys, values);
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn openCursor(self: *ReadTxn) !Cursor {
        return try self.vtable.open_cursor(self.allocator, self.ptr);
    }
};

pub const ProbeTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
    };

    pub fn abort(self: *ProbeTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *ProbeTxn, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, key);
    }

    pub fn getManySorted(self: *ProbeTxn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try get_many_sorted(self.ptr, keys, values);
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }
};

pub const CurrentScanTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        open_cursor: *const fn (Allocator, *anyopaque) anyerror!Cursor,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *CurrentScanTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn openCursor(self: *CurrentScanTxn) !Cursor {
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, self.vtable.open_cursor, .{ self.allocator, self.ptr });
    }
};

pub const NamespaceReadTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, backend_types.Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        open_cursor: ?*const fn (Allocator, *anyopaque, backend_types.Namespace) anyerror!Cursor = null,
    };

    pub fn abort(self: *NamespaceReadTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceReadTxn, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, namespace, key);
    }

    pub fn getManySorted(self: *NamespaceReadTxn, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
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

    pub fn openCursor(self: *NamespaceReadTxn, namespace: backend_types.Namespace) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try open_cursor(self.allocator, self.ptr, namespace);
    }
};

pub const WriteTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
        open_cursor: *const fn (Allocator, *anyopaque) anyerror!Cursor,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *WriteTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *WriteTxn) !void {
        try BoundaryAbi.call("commit", self.boundary_dispatch, self.vtable.commit, .{ self.allocator, self.ptr });
        self.* = undefined;
    }

    pub fn get(self: *WriteTxn, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, key });
    }

    pub fn getManySorted(self: *WriteTxn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, keys, values });
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn put(self: *WriteTxn, key: []const u8, value: []const u8) !void {
        try BoundaryAbi.call("put", self.boundary_dispatch, self.vtable.put, .{ self.ptr, key, value });
    }

    pub fn delete(self: *WriteTxn, key: []const u8) !void {
        try BoundaryAbi.call("delete", self.boundary_dispatch, self.vtable.delete, .{ self.ptr, key });
    }

    pub fn openCursor(self: *WriteTxn) !Cursor {
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, self.vtable.open_cursor, .{ self.allocator, self.ptr });
    }
};

pub const NamespaceWriteTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, backend_types.Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror!void,
        open_cursor: ?*const fn (Allocator, *anyopaque, backend_types.Namespace) anyerror!Cursor = null,
    };

    pub fn abort(self: *NamespaceWriteTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *NamespaceWriteTxn) !void {
        try self.vtable.commit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, namespace, key);
    }

    pub fn getManySorted(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        if (self.vtable.get_many_sorted) |get_many_sorted| return try get_many_sorted(self.ptr, namespace, keys, values);
        @memset(values, null);
        for (keys, values) |key, *value| {
            value.* = self.vtable.get(self.ptr, namespace, key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }
    }

    pub fn put(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        try self.vtable.put(self.ptr, namespace, key, value);
    }

    pub fn appendPut(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try append_put(self.ptr, namespace, key, value);
    }

    pub fn delete(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8) !void {
        try self.vtable.delete(self.ptr, namespace, key);
    }

    pub fn openCursor(self: *NamespaceWriteTxn, namespace: backend_types.Namespace) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try open_cursor(self.allocator, self.ptr, namespace);
    }
};

pub const Batch = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
        open_cursor: ?*const fn (Allocator, *anyopaque) anyerror!Cursor = null,
        set_replay_opaque: ?*const fn (*anyopaque, u64, []const u8) anyerror!void = null,
    };

    pub fn abort(self: *Batch) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *Batch) !void {
        try self.vtable.commit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *Batch, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, key);
    }

    pub fn getManySorted(self: *Batch, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try get_many_sorted(self.ptr, keys, values);
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn put(self: *Batch, key: []const u8, value: []const u8) !void {
        try self.vtable.put(self.ptr, key, value);
    }

    pub fn appendPut(self: *Batch, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try append_put(self.ptr, key, value);
    }

    pub fn delete(self: *Batch, key: []const u8) !void {
        try self.vtable.delete(self.ptr, key);
    }

    pub fn openCursor(self: *Batch) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try open_cursor(self.allocator, self.ptr);
    }

    pub fn setReplayOpaque(self: *Batch, sequence: u64, payload: []const u8) !void {
        const set_replay_opaque = self.vtable.set_replay_opaque orelse return error.Unsupported;
        try set_replay_opaque(self.ptr, sequence, payload);
    }
};

pub const NamespaceBatch = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, backend_types.Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror!void,
    };

    pub fn abort(self: *NamespaceBatch) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *NamespaceBatch) !void {
        try self.vtable.commit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return try self.vtable.get(self.ptr, namespace, key);
    }

    pub fn getManySorted(self: *NamespaceBatch, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        if (self.vtable.get_many_sorted) |get_many_sorted| return try get_many_sorted(self.ptr, namespace, keys, values);
        @memset(values, null);
        for (keys, values) |key, *value| {
            value.* = self.vtable.get(self.ptr, namespace, key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }
    }

    pub fn put(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        try self.vtable.put(self.ptr, namespace, key, value);
    }

    pub fn appendPut(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try append_put(self.ptr, namespace, key, value);
    }

    pub fn delete(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8) !void {
        try self.vtable.delete(self.ptr, namespace, key);
    }
};

pub const Store = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const ReplayCallback = *const fn (*anyopaque, u64, []const u8) anyerror!void;

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        capabilities: *const fn (*anyopaque) backend_types.Capabilities,
        begin_read: *const fn (Allocator, *anyopaque) anyerror!ReadTxn,
        begin_probe: ?*const fn (Allocator, *anyopaque) anyerror!ProbeTxn = null,
        begin_current_scan: ?*const fn (Allocator, *anyopaque) anyerror!CurrentScanTxn = null,
        begin_write: *const fn (Allocator, *anyopaque) anyerror!WriteTxn,
        begin_batch: *const fn (Allocator, *anyopaque) anyerror!Batch,
        begin_batch_with_options: ?*const fn (Allocator, *anyopaque, backend_types.BatchOptions) anyerror!Batch = null,
        sync: ?*const fn (*anyopaque, bool) anyerror!void = null,
        sync_replay_state: ?*const fn (*anyopaque) anyerror!void = null,
        begin_bulk_ingest_session: ?*const fn (*anyopaque) anyerror!void = null,
        finish_bulk_ingest_session: ?*const fn (*anyopaque, backend_types.BulkIngestFinishOptions) anyerror!void = null,
        flush_buffered_writes: ?*const fn (*anyopaque, backend_types.BulkIngestFinishOptions) anyerror!void = null,
        abort_bulk_ingest_session: ?*const fn (*anyopaque) void = null,
        last_replay_sequence: ?*const fn (*anyopaque, u64) u64 = null,
        next_replay_sequence: ?*const fn (*anyopaque, u64) u64 = null,
        append_replay_opaque: ?*const fn (Allocator, *anyopaque, u64, []const u8) anyerror!void = null,
        iterate_replay_from: ?*const fn (Allocator, *anyopaque, u64) anyerror![]ReplayEntry = null,
        for_each_replay_from: ?*const fn (*anyopaque, u64, *anyopaque, ReplayCallback) anyerror!void = null,
        for_each_replay_lane_from: ?*const fn (*anyopaque, u8, u64, usize, *anyopaque, ReplayCallback) anyerror!backend_types.ReplayLaneIterationStats = null,
        for_each_replay_from_matching_hint_mask: ?*const fn (*anyopaque, u64, u8, *anyopaque, ReplayCallback) anyerror!void = null,
        truncate_replay_up_to: ?*const fn (Allocator, *anyopaque, u64) anyerror!void = null,
        // Append-only optional ABI extensions: older stores retain their
        // original transaction layout and fall back to ordinary admission.
        begin_read_with_block_cache_admission: ?*const fn (Allocator, *anyopaque, backend_types.Namespace.BlockCacheAdmission) anyerror!ReadTxn = null,
        begin_probe_with_block_cache_admission: ?*const fn (Allocator, *anyopaque, backend_types.Namespace.BlockCacheAdmission) anyerror!ProbeTxn = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn deinit(self: *Store) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn capabilities(self: *Store) backend_types.Capabilities {
        return self.vtable.capabilities(self.ptr);
    }

    pub fn beginRead(self: *Store) !ReadTxn {
        return try BoundaryAbi.call("begin_read", self.boundary_dispatch, self.vtable.begin_read, .{ self.allocator, self.ptr });
    }

    pub fn beginReadWithBlockCacheAdmission(self: *Store, admission: backend_types.Namespace.BlockCacheAdmission) !ReadTxn {
        if (self.vtable.begin_read_with_block_cache_admission) |begin_read| {
            return try BoundaryAbi.call("begin_read_with_block_cache_admission", self.boundary_dispatch, begin_read, .{ self.allocator, self.ptr, admission });
        }
        return try self.beginRead();
    }

    pub fn beginProbe(self: *Store) !ProbeTxn {
        if (self.vtable.begin_probe) |begin_probe| {
            return try BoundaryAbi.call("begin_probe", self.boundary_dispatch, begin_probe, .{ self.allocator, self.ptr });
        }
        return try probeTxnFrom(self.allocator, try self.beginRead());
    }

    pub fn beginProbeWithBlockCacheAdmission(self: *Store, admission: backend_types.Namespace.BlockCacheAdmission) !ProbeTxn {
        if (self.vtable.begin_probe_with_block_cache_admission) |begin_probe| {
            return try BoundaryAbi.call("begin_probe_with_block_cache_admission", self.boundary_dispatch, begin_probe, .{ self.allocator, self.ptr, admission });
        }
        return try self.beginProbe();
    }

    pub fn beginCurrentScan(self: *Store) !CurrentScanTxn {
        if (self.vtable.begin_current_scan) |begin_current_scan| {
            return try BoundaryAbi.call("begin_current_scan", self.boundary_dispatch, begin_current_scan, .{ self.allocator, self.ptr });
        }
        return try currentScanTxnFrom(self.allocator, try self.beginRead());
    }

    pub fn beginWrite(self: *Store) !WriteTxn {
        return try BoundaryAbi.call("begin_write", self.boundary_dispatch, self.vtable.begin_write, .{ self.allocator, self.ptr });
    }

    pub fn beginBatch(self: *Store) !Batch {
        return try BoundaryAbi.call("begin_batch", self.boundary_dispatch, self.vtable.begin_batch, .{ self.allocator, self.ptr });
    }

    pub fn beginBatchWithOptions(self: *Store, options: backend_types.BatchOptions) !Batch {
        if (self.vtable.begin_batch_with_options) |begin_batch_with_options| {
            return try BoundaryAbi.call("begin_batch_with_options", self.boundary_dispatch, begin_batch_with_options, .{ self.allocator, self.ptr, options });
        }
        return try self.beginBatch();
    }

    pub fn sync(self: *Store, force: bool) !void {
        if (self.vtable.sync) |sync_fn| {
            try BoundaryAbi.call("sync", self.boundary_dispatch, sync_fn, .{ self.ptr, force });
        }
    }

    pub fn syncReplayState(self: *Store) !void {
        if (self.vtable.sync_replay_state) |sync_replay_state_fn| {
            try BoundaryAbi.call("sync_replay_state", self.boundary_dispatch, sync_replay_state_fn, .{self.ptr});
            return;
        }
        try self.sync(false);
    }

    pub fn beginBulkIngestSession(self: *Store) !void {
        if (self.vtable.begin_bulk_ingest_session) |begin_bulk_ingest_session| {
            try BoundaryAbi.call("begin_bulk_ingest_session", self.boundary_dispatch, begin_bulk_ingest_session, .{self.ptr});
        }
    }

    pub fn finishBulkIngestSessionWithOptions(self: *Store, options: backend_types.BulkIngestFinishOptions) !void {
        if (self.vtable.finish_bulk_ingest_session) |finish_bulk_ingest_session| {
            try BoundaryAbi.call("finish_bulk_ingest_session", self.boundary_dispatch, finish_bulk_ingest_session, .{ self.ptr, options });
        }
    }

    pub fn flushBufferedWritesWithOptions(self: *Store, options: backend_types.BulkIngestFinishOptions) !void {
        if (self.vtable.flush_buffered_writes) |flush_buffered_writes| {
            try BoundaryAbi.call("flush_buffered_writes", self.boundary_dispatch, flush_buffered_writes, .{ self.ptr, options });
        }
    }

    pub fn abortBulkIngestSession(self: *Store) void {
        if (self.vtable.abort_bulk_ingest_session) |abort_bulk_ingest_session| {
            abort_bulk_ingest_session(self.ptr);
        }
    }

    pub fn lastReplaySequence(self: *Store, fallback_last: u64) u64 {
        if (self.vtable.last_replay_sequence) |f| return f(self.ptr, fallback_last);
        return fallback_last;
    }

    pub fn nextReplaySequence(self: *Store, fallback_next: u64) u64 {
        if (self.vtable.next_replay_sequence) |f| return f(self.ptr, fallback_next);
        return fallback_next;
    }

    pub fn appendReplayOpaque(self: *Store, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        const f = self.vtable.append_replay_opaque orelse return error.Unsupported;
        return try BoundaryAbi.call("append_replay_opaque", self.boundary_dispatch, f, .{ alloc, self.ptr, sequence, payload });
    }

    pub fn iterateReplayFrom(self: *Store, alloc: Allocator, from_sequence: u64) ![]ReplayEntry {
        const f = self.vtable.iterate_replay_from orelse return error.Unsupported;
        return try BoundaryAbi.call("iterate_replay_from", self.boundary_dispatch, f, .{ alloc, self.ptr, from_sequence });
    }

    pub fn forEachReplayFrom(
        self: *Store,
        from_sequence: u64,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        if (self.vtable.for_each_replay_from) |f| {
            const Adapter = struct {
                fn call(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
                    const typed_ctx: @TypeOf(ctx) = @ptrCast(@alignCast(ptr));
                    return try callback(typed_ctx, sequence, payload);
                }
            };
            return try BoundaryAbi.call("for_each_replay_from", self.boundary_dispatch, f, .{ self.ptr, from_sequence, ctx, Adapter.call });
        }

        const entries = try self.iterateReplayFrom(self.allocator, from_sequence);
        defer {
            for (entries) |*entry| entry.deinit(self.allocator);
            self.allocator.free(entries);
        }
        for (entries) |entry| try callback(ctx, entry.sequence, entry.payload);
    }

    pub fn forEachReplayFromMatchingHintMask(
        self: *Store,
        from_sequence: u64,
        required_hint_mask: u8,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        if (self.vtable.for_each_replay_from_matching_hint_mask) |f| {
            const Adapter = struct {
                fn call(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
                    const typed_ctx: @TypeOf(ctx) = @ptrCast(@alignCast(ptr));
                    return try callback(typed_ctx, sequence, payload);
                }
            };
            return try BoundaryAbi.call("for_each_replay_from_matching_hint_mask", self.boundary_dispatch, f, .{ self.ptr, from_sequence, required_hint_mask, ctx, Adapter.call });
        }
        if (self.vtable.for_each_replay_lane_from) |f| {
            const Adapter = struct {
                fn call(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
                    const typed_ctx: @TypeOf(ctx) = @ptrCast(@alignCast(ptr));
                    return try callback(typed_ctx, sequence, payload);
                }
            };
            const lane_ordinal = if (required_hint_mask == 0)
                internal_keys.replay_all_kind
            else if (replayHintOrdinalFromSingleMask(required_hint_mask)) |ordinal|
                ordinal
            else
                return error.Unsupported;
            _ = try BoundaryAbi.call("for_each_replay_lane_from", self.boundary_dispatch, f, .{ self.ptr, lane_ordinal, from_sequence, 0, ctx, Adapter.call });
            return;
        }

        if (required_hint_mask != 0) return error.Unsupported;
        const entries = try self.iterateReplayFrom(self.allocator, from_sequence);
        defer {
            for (entries) |*entry| entry.deinit(self.allocator);
            self.allocator.free(entries);
        }
        for (entries) |entry| try callback(ctx, entry.sequence, entry.payload);
    }

    pub fn forEachReplayLaneFrom(
        self: *Store,
        lane_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !backend_types.ReplayLaneIterationStats {
        const f = self.vtable.for_each_replay_lane_from orelse return error.Unsupported;
        const Adapter = struct {
            fn call(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
                const typed_ctx: @TypeOf(ctx) = @ptrCast(@alignCast(ptr));
                return try callback(typed_ctx, sequence, payload);
            }
        };
        return try BoundaryAbi.call("for_each_replay_lane_from", self.boundary_dispatch, f, .{ self.ptr, lane_ordinal, from_sequence, max_entries, ctx, Adapter.call });
    }

    pub fn truncateReplayUpTo(self: *Store, alloc: Allocator, up_to_sequence: u64) !void {
        const f = self.vtable.truncate_replay_up_to orelse return error.Unsupported;
        return try BoundaryAbi.call("truncate_replay_up_to", self.boundary_dispatch, f, .{ alloc, self.ptr, up_to_sequence });
    }
};

pub const NamespaceStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        capabilities: *const fn (*anyopaque) backend_types.Capabilities,
        begin_read: *const fn (Allocator, *anyopaque) anyerror!NamespaceReadTxn,
        begin_probe: ?*const fn (Allocator, *anyopaque) anyerror!NamespaceReadTxn = null,
        begin_write: *const fn (Allocator, *anyopaque) anyerror!NamespaceWriteTxn,
        begin_batch: *const fn (Allocator, *anyopaque) anyerror!NamespaceBatch,
        begin_batch_with_options: ?*const fn (Allocator, *anyopaque, backend_types.BatchOptions) anyerror!NamespaceBatch = null,
    };

    pub fn deinit(self: *NamespaceStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn capabilities(self: *NamespaceStore) backend_types.Capabilities {
        return self.vtable.capabilities(self.ptr);
    }

    pub fn beginRead(self: *NamespaceStore) !NamespaceReadTxn {
        return try self.vtable.begin_read(self.allocator, self.ptr);
    }

    pub fn beginProbe(self: *NamespaceStore) !NamespaceReadTxn {
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

    pub fn beginBatchWithOptions(self: *NamespaceStore, options: backend_types.BatchOptions) !NamespaceBatch {
        if (self.vtable.begin_batch_with_options) |begin_batch_with_options| {
            return try begin_batch_with_options(self.allocator, self.ptr, options);
        }
        return try self.beginBatch();
    }
};

pub fn cursorFrom(allocator: Allocator, handle: anytype) !Cursor {
    return try cursorFromWithParent(allocator, handle, null);
}

fn cursorFromWithParent(allocator: Allocator, handle: anytype, parent: ?ParentRelease) !Cursor {
    const Handle = @TypeOf(handle);
    const cursor_box_allocator = wrapperBoxAllocator(allocator);
    const CursorBox = struct {
        allocator: Allocator,
        handle: Handle,
        parent: ?ParentRelease,
    };
    const box_ptr = try cursor_box_allocator.create(CursorBox);
    box_ptr.* = .{
        .allocator = cursor_box_allocator,
        .handle = handle,
        .parent = parent,
    };

    const vt = struct {
        fn unbox(ptr: *anyopaque) *CursorBox {
            return @ptrCast(@alignCast(ptr));
        }

        fn close(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            state.handle.close();
            if (state.parent) |retained_parent| retained_parent.release(retained_parent.ptr);
            box_allocator.destroy(state);
        }

        fn first(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.first() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn last(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.last() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn next(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.next() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn prev(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.prev() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn seekAtOrAfter(ptr: *anyopaque, key: []const u8) anyerror!?Entry {
            return unbox(ptr).handle.seekAtOrAfter(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn seekAtOrBefore(ptr: *anyopaque, key: []const u8) anyerror!?Entry {
            return unbox(ptr).handle.seekAtOrBefore(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn setUpperBound(ptr: *anyopaque, upper: ?[]const u8) void {
            unbox(ptr).handle.setUpperBound(upper);
        }
    };

    return .{
        .allocator = cursor_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .close = vt.close,
            .first = vt.first,
            .last = vt.last,
            .next = vt.next,
            .prev = vt.prev,
            .seek_at_or_after = vt.seekAtOrAfter,
            .seek_at_or_before = vt.seekAtOrBefore,
            .set_upper_bound = if (@hasDecl(Handle, "setUpperBound")) vt.setUpperBound else null,
        },
    };
}

pub fn readTxnFrom(allocator: Allocator, handle: anytype) !ReadTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .open_cursor = vt.openCursor,
        },
    };
}

pub fn probeTxnFrom(allocator: Allocator, handle: anytype) !ProbeTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            state.handle.abort();
            box_allocator.destroy(state);
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
        },
    };
}

pub fn currentScanTxnFrom(allocator: Allocator, handle: anytype) !CurrentScanTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .open_cursor = vt.openCursor,
        },
    };
}

pub fn namespaceReadTxnFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceReadTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn get(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        fn getManySorted(ptr: *anyopaque, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
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

        fn openCursor(alloc: Allocator, ptr: *anyopaque, namespace: backend_types.Namespace) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor(try mapNamespace(namespace));
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
        },
    };
}

pub fn writeTxnFrom(allocator: Allocator, handle: anytype) !WriteTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            try unbox(ptr).commit();
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(key, value);
        }

        fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(key);
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .put = vt.put,
            .delete = vt.delete,
            .open_cursor = vt.openCursor,
        },
    };
}

pub fn namespaceWriteTxnFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceWriteTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            try unbox(ptr).commit();
        }

        fn get(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        fn getManySorted(ptr: *anyopaque, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            try unbox(ptr).handle.getManySorted(try mapNamespace(namespace), keys, values);
        }

        fn put(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(try mapNamespace(namespace), key, value);
        }

        fn appendPut(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(try mapNamespace(namespace), key, value);
        }

        fn delete(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(try mapNamespace(namespace), key);
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque, namespace: backend_types.Namespace) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor(try mapNamespace(namespace));
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .get_many_sorted = if (@hasDecl(Handle, "getManySorted")) vt.getManySorted else null,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
        },
    };
}

pub fn batchFrom(allocator: Allocator, handle: anytype) !Batch {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            try unbox(ptr).commit();
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(key, value);
        }

        fn appendPut(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(key, value);
        }

        fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(key);
        }

        fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }

        fn setReplayOpaque(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            if (@hasDecl(Handle, "setReplayOpaque")) {
                return try unbox(ptr).handle.setReplayOpaque(sequence, payload);
            }
            return error.Unsupported;
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
            .set_replay_opaque = if (@hasDecl(Handle, "setReplayOpaque")) vt.setReplayOpaque else null,
        },
    };
}

pub fn namespaceBatchFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceBatch {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            state.handle.abort();
            box_allocator.destroy(state);
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            try state.handle.commit();
            box_allocator.destroy(state);
        }

        fn get(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        fn getManySorted(ptr: *anyopaque, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            try unbox(ptr).handle.getManySorted(try mapNamespace(namespace), keys, values);
        }

        fn put(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(try mapNamespace(namespace), key, value);
        }

        fn appendPut(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(try mapNamespace(namespace), key, value);
        }

        fn delete(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(try mapNamespace(namespace), key);
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .get_many_sorted = if (@hasDecl(Handle, "getManySorted")) vt.getManySorted else null,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
        },
    };
}

pub fn storeFrom(allocator: Allocator, handle: anytype) !Store {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn deinit(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            box_allocator.destroy(state);
        }

        fn capabilities(ptr: *anyopaque) backend_types.Capabilities {
            return unbox(ptr).handle.capabilities();
        }

        fn beginRead(alloc: Allocator, ptr: *anyopaque) anyerror!ReadTxn {
            return try readTxnFrom(alloc, try unbox(ptr).handle.beginRead());
        }

        fn beginReadWithBlockCacheAdmission(alloc: Allocator, ptr: *anyopaque, admission: backend_types.Namespace.BlockCacheAdmission) anyerror!ReadTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginReadWithBlockCacheAdmission(admission);
            }
            return try readTxnFrom(alloc, try unbox(ptr).handle.beginReadWithBlockCacheAdmission(admission));
        }

        fn beginProbe(alloc: Allocator, ptr: *anyopaque) anyerror!ProbeTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginProbe();
            }
            if (@hasDecl(Handle, "beginProbe")) {
                return try probeTxnFrom(alloc, try unbox(ptr).handle.beginProbe());
            }
            return try probeTxnFrom(alloc, try unbox(ptr).handle.beginRead());
        }

        fn beginProbeWithBlockCacheAdmission(alloc: Allocator, ptr: *anyopaque, admission: backend_types.Namespace.BlockCacheAdmission) anyerror!ProbeTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginProbeWithBlockCacheAdmission(admission);
            }
            return try probeTxnFrom(alloc, try unbox(ptr).handle.beginProbeWithBlockCacheAdmission(admission));
        }

        fn beginCurrentScan(alloc: Allocator, ptr: *anyopaque) anyerror!CurrentScanTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginCurrentScan();
            }
            if (@hasDecl(Handle, "beginCurrentScan")) {
                return try currentScanTxnFrom(alloc, try unbox(ptr).handle.beginCurrentScan());
            }
            return try currentScanTxnFrom(alloc, try unbox(ptr).handle.beginRead());
        }

        fn beginWrite(alloc: Allocator, ptr: *anyopaque) anyerror!WriteTxn {
            return try writeTxnFrom(alloc, try unbox(ptr).handle.beginWrite());
        }

        fn beginBatch(alloc: Allocator, ptr: *anyopaque) anyerror!Batch {
            return try batchFrom(alloc, try unbox(ptr).handle.beginBatch());
        }

        fn beginBatchWithOptions(alloc: Allocator, ptr: *anyopaque, options: backend_types.BatchOptions) anyerror!Batch {
            if (@hasDecl(Handle, "beginBatchWithOptions")) {
                return try batchFrom(alloc, try unbox(ptr).handle.beginBatchWithOptions(options));
            }
            return try batchFrom(alloc, try unbox(ptr).handle.beginBatch());
        }

        fn sync(ptr: *anyopaque, force: bool) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.sync) |sync_fn| {
                    return try sync_fn(state.handle.ptr, force);
                }
                return;
            }
            if (@hasDecl(Handle, "sync")) {
                try unbox(ptr).handle.sync(force);
            }
        }

        fn syncReplayState(ptr: *anyopaque) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.sync_replay_state) |sync_replay_state_fn| {
                    return try sync_replay_state_fn(state.handle.ptr);
                }
                if (state.handle.vtable.sync) |sync_fn| {
                    return try sync_fn(state.handle.ptr, false);
                }
                return;
            }
            if (@hasDecl(Handle, "syncReplayState")) {
                try unbox(ptr).handle.syncReplayState();
                return;
            }
            if (@hasDecl(Handle, "sync")) {
                try unbox(ptr).handle.sync(false);
            }
        }

        fn beginBulkIngestSession(ptr: *anyopaque) anyerror!void {
            if (@hasDecl(Handle, "beginBulkIngestSession")) {
                try unbox(ptr).handle.beginBulkIngestSession();
            }
        }

        fn finishBulkIngestSession(ptr: *anyopaque, options: backend_types.BulkIngestFinishOptions) anyerror!void {
            if (@hasDecl(Handle, "finishBulkIngestSessionWithOptions")) {
                try unbox(ptr).handle.finishBulkIngestSessionWithOptions(options);
            } else if (@hasDecl(Handle, "finishBulkIngestSession")) {
                try unbox(ptr).handle.finishBulkIngestSession();
            }
        }

        fn flushBufferedWrites(ptr: *anyopaque, options: backend_types.BulkIngestFinishOptions) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.flush_buffered_writes) |flush_buffered_writes| {
                    return try flush_buffered_writes(state.handle.ptr, options);
                }
                return;
            }
            if (@hasDecl(Handle, "flushBufferedWritesWithOptions")) {
                try unbox(ptr).handle.flushBufferedWritesWithOptions(options);
            }
        }

        fn abortBulkIngestSession(ptr: *anyopaque) void {
            if (@hasDecl(Handle, "abortBulkIngestSession")) {
                unbox(ptr).handle.abortBulkIngestSession();
            }
        }

        fn lastReplaySequence(ptr: *anyopaque, fallback_last: u64) u64 {
            if (@hasDecl(Handle, "lastReplaySequence")) {
                return unbox(ptr).handle.lastReplaySequence(fallback_last);
            }
            return fallback_last;
        }

        fn nextReplaySequence(ptr: *anyopaque, fallback_next: u64) u64 {
            if (@hasDecl(Handle, "nextReplaySequence")) {
                return unbox(ptr).handle.nextReplaySequence(fallback_next);
            }
            return fallback_next;
        }

        fn appendReplayOpaque(alloc: Allocator, ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            if (@hasDecl(Handle, "appendReplayOpaque")) {
                return try unbox(ptr).handle.appendReplayOpaque(alloc, sequence, payload);
            }
            return error.Unsupported;
        }

        fn iterateReplayFrom(alloc: Allocator, ptr: *anyopaque, from_sequence: u64) anyerror![]ReplayEntry {
            if (@hasDecl(Handle, "iterateReplayFrom")) {
                return try unbox(ptr).handle.iterateReplayFrom(alloc, from_sequence);
            }
            return error.Unsupported;
        }

        fn forEachReplayFrom(
            ptr: *anyopaque,
            from_sequence: u64,
            callback_ctx: *anyopaque,
            callback: Store.ReplayCallback,
        ) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.for_each_replay_from) |f| {
                    return try f(state.handle.ptr, from_sequence, callback_ctx, callback);
                }
            } else if (@hasDecl(Handle, "forEachReplayFrom")) {
                return try unbox(ptr).handle.forEachReplayFrom(from_sequence, callback_ctx, callback);
            }
            if (@hasDecl(Handle, "iterateReplayFrom")) {
                const state = unbox(ptr);
                const entries = try state.handle.iterateReplayFrom(state.allocator, from_sequence);
                defer {
                    for (entries) |*entry| entry.deinit(state.allocator);
                    state.allocator.free(entries);
                }
                for (entries) |entry| {
                    try callback(callback_ctx, entry.sequence, entry.payload);
                }
                return;
            }
            return error.Unsupported;
        }

        fn forEachReplayFromMatchingHintMask(
            ptr: *anyopaque,
            from_sequence: u64,
            required_hint_mask: u8,
            callback_ctx: *anyopaque,
            callback: Store.ReplayCallback,
        ) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.for_each_replay_from_matching_hint_mask) |f| {
                    return try f(state.handle.ptr, from_sequence, required_hint_mask, callback_ctx, callback);
                }
                if (state.handle.vtable.for_each_replay_lane_from) |f| {
                    const lane_ordinal = if (required_hint_mask == 0)
                        internal_keys.replay_all_kind
                    else if (replayHintOrdinalFromSingleMask(required_hint_mask)) |ordinal|
                        ordinal
                    else
                        return error.Unsupported;
                    _ = try f(state.handle.ptr, lane_ordinal, from_sequence, 0, callback_ctx, callback);
                    return;
                }
            } else if (@hasDecl(Handle, "forEachReplayFromMatchingHintMask")) {
                return try unbox(ptr).handle.forEachReplayFromMatchingHintMask(from_sequence, required_hint_mask, callback_ctx, callback);
            } else if (@hasDecl(Handle, "forEachReplayLaneFrom")) {
                const lane_ordinal = if (required_hint_mask == 0)
                    internal_keys.replay_all_kind
                else if (replayHintOrdinalFromSingleMask(required_hint_mask)) |ordinal|
                    ordinal
                else
                    return error.Unsupported;
                _ = try unbox(ptr).handle.forEachReplayLaneFrom(lane_ordinal, from_sequence, 0, callback_ctx, callback);
                return;
            }
            return error.Unsupported;
        }

        fn forEachReplayLaneFrom(
            ptr: *anyopaque,
            lane_ordinal: u8,
            from_sequence: u64,
            max_entries: usize,
            callback_ctx: *anyopaque,
            callback: Store.ReplayCallback,
        ) anyerror!backend_types.ReplayLaneIterationStats {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.for_each_replay_lane_from) |f| {
                    return try f(state.handle.ptr, lane_ordinal, from_sequence, max_entries, callback_ctx, callback);
                }
            } else if (@hasDecl(Handle, "forEachReplayLaneFrom")) {
                return try unbox(ptr).handle.forEachReplayLaneFrom(lane_ordinal, from_sequence, max_entries, callback_ctx, callback);
            }
            return error.Unsupported;
        }

        fn truncateReplayUpTo(alloc: Allocator, ptr: *anyopaque, up_to_sequence: u64) anyerror!void {
            if (@hasDecl(Handle, "truncateReplayUpTo")) {
                return try unbox(ptr).handle.truncateReplayUpTo(alloc, up_to_sequence);
            }
            return error.Unsupported;
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .deinit = vt.deinit,
            .capabilities = vt.capabilities,
            .begin_read = vt.beginRead,
            .begin_read_with_block_cache_admission = if (Handle == Store or @hasDecl(Handle, "beginReadWithBlockCacheAdmission")) vt.beginReadWithBlockCacheAdmission else null,
            .begin_probe = if (Handle == Store or @hasDecl(Handle, "beginProbe")) vt.beginProbe else null,
            .begin_probe_with_block_cache_admission = if (Handle == Store or @hasDecl(Handle, "beginProbeWithBlockCacheAdmission")) vt.beginProbeWithBlockCacheAdmission else null,
            .begin_current_scan = if (Handle == Store or @hasDecl(Handle, "beginCurrentScan")) vt.beginCurrentScan else null,
            .begin_write = vt.beginWrite,
            .begin_batch = vt.beginBatch,
            .begin_batch_with_options = vt.beginBatchWithOptions,
            .sync = vt.sync,
            .sync_replay_state = vt.syncReplayState,
            .begin_bulk_ingest_session = vt.beginBulkIngestSession,
            .finish_bulk_ingest_session = vt.finishBulkIngestSession,
            .flush_buffered_writes = vt.flushBufferedWrites,
            .abort_bulk_ingest_session = vt.abortBulkIngestSession,
            .last_replay_sequence = vt.lastReplaySequence,
            .next_replay_sequence = vt.nextReplaySequence,
            .append_replay_opaque = vt.appendReplayOpaque,
            .iterate_replay_from = vt.iterateReplayFrom,
            .for_each_replay_from = vt.forEachReplayFrom,
            .for_each_replay_lane_from = vt.forEachReplayLaneFrom,
            .for_each_replay_from_matching_hint_mask = vt.forEachReplayFromMatchingHintMask,
            .truncate_replay_up_to = vt.truncateReplayUpTo,
        },
    };
}

pub fn namespaceStoreFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceStore {
    const Handle = @TypeOf(handle);
    const HandleDecl = switch (@typeInfo(Handle)) {
        .pointer => |pointer| pointer.child,
        else => Handle,
    };
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn deinit(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            box_allocator.destroy(state);
        }

        fn capabilities(ptr: *anyopaque) backend_types.Capabilities {
            return unbox(ptr).handle.capabilities();
        }

        fn beginRead(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceReadTxn {
            return try namespaceReadTxnFrom(alloc, try unbox(ptr).handle.beginRead(), LocalNamespace, mapNamespace);
        }

        fn beginProbe(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceReadTxn {
            if (@hasDecl(HandleDecl, "beginProbe")) {
                return try namespaceReadTxnFrom(alloc, try unbox(ptr).handle.beginProbe(), LocalNamespace, mapNamespace);
            }
            return try beginRead(alloc, ptr);
        }

        fn beginWrite(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceWriteTxn {
            return try namespaceWriteTxnFrom(alloc, try unbox(ptr).handle.beginWrite(), LocalNamespace, mapNamespace);
        }

        fn beginBatch(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceBatch {
            return try namespaceBatchFrom(alloc, try unbox(ptr).handle.beginBatch(), LocalNamespace, mapNamespace);
        }

        fn beginBatchWithOptions(alloc: Allocator, ptr: *anyopaque, options: backend_types.BatchOptions) anyerror!NamespaceBatch {
            if (@hasDecl(HandleDecl, "beginBatchWithOptions")) {
                return try namespaceBatchFrom(alloc, try unbox(ptr).handle.beginBatchWithOptions(options), LocalNamespace, mapNamespace);
            }
            return try beginBatch(alloc, ptr);
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .deinit = vt.deinit,
            .capabilities = vt.capabilities,
            .begin_read = vt.beginRead,
            .begin_probe = if (Handle == NamespaceStore or @hasDecl(HandleDecl, "beginProbe")) vt.beginProbe else null,
            .begin_write = vt.beginWrite,
            .begin_batch = vt.beginBatch,
            .begin_batch_with_options = vt.beginBatchWithOptions,
        },
    };
}

test "runtime store erases concrete single-namespace store handles" {
    const MockCursor = struct {
        closed: bool = false,

        pub fn close(self: *@This()) void {
            self.closed = true;
        }

        pub fn first(_: *@This()) !?Entry {
            return .{ .key = "a", .value = "1" };
        }

        pub fn last(_: *@This()) !?Entry {
            return .{ .key = "z", .value = "9" };
        }

        pub fn next(_: *@This()) !?Entry {
            return null;
        }

        pub fn prev(_: *@This()) !?Entry {
            return null;
        }

        pub fn seekAtOrAfter(_: *@This(), key: []const u8) !?Entry {
            return .{ .key = key, .value = "x" };
        }

        pub fn seekAtOrBefore(_: *@This(), key: []const u8) !?Entry {
            return .{ .key = key, .value = "y" };
        }
    };

    const MockRead = struct {
        pub fn abort(_: *@This()) void {}
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return "r";
        }
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    const MockWrite = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: []const u8) !void {
            self.value = null;
        }
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    const MockBatch = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: []const u8) !void {
            self.value = null;
        }
    };

    const MockStore = struct {
        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{ .cursors = true };
        }

        pub fn beginRead(_: *@This()) !MockRead {
            return .{};
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(_: *@This()) !MockBatch {
            return .{};
        }
    };

    const mock = MockStore{};
    var store = try storeFrom(std.testing.allocator, mock);
    defer store.deinit();
    try std.testing.expect(store.capabilities().cursors);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("r", try read.get("k"));
    var cur = try read.openCursor();
    defer cur.close();
    try std.testing.expectEqualStrings("a", (try cur.first()).?.key);

    var probe = try store.beginProbe();
    defer probe.abort();
    try std.testing.expectEqualStrings("r", try probe.get("k"));

    var current_scan = try store.beginCurrentScan();
    defer current_scan.abort();
    var current_scan_cur = try current_scan.openCursor();
    defer current_scan_cur.close();
    try std.testing.expectEqualStrings("a", (try current_scan_cur.first()).?.key);

    var write = try store.beginWrite();
    try write.put("k", "w");
    try std.testing.expectEqualStrings("w", try write.get("k"));
    try write.commit();

    var batch = try store.beginBatch();
    try batch.put("k", "b");
    try std.testing.expectEqualStrings("b", try batch.get("k"));
    try batch.commit();
}

test "failed commit keeps erased write handle abortable" {
    const MockCursor = struct {
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

    const Shared = struct {
        commits: usize = 0,
        aborted: bool = false,
    };

    const MockWrite = struct {
        shared: *Shared,

        pub fn abort(self: *@This()) void {
            self.shared.aborted = true;
        }
        pub fn commit(self: *@This()) !void {
            self.shared.commits += 1;
            return error.CommitFailed;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    var shared = Shared{};
    var txn = try writeTxnFrom(std.testing.allocator, MockWrite{ .shared = &shared });
    try std.testing.expectError(error.CommitFailed, txn.commit());
    txn.abort();
    try std.testing.expectEqual(@as(usize, 1), shared.commits);
    try std.testing.expect(shared.aborted);
}

test "runtime namespace store maps logical namespaces into concrete partitions" {
    const LocalNamespace = enum { meta, docs };

    const Shared = struct {
        read_calls: usize = 0,
        probe_calls: usize = 0,
    };

    const MockRead = struct {
        probe: bool = false,

        pub fn abort(_: *@This()) void {}
        pub fn get(self: *@This(), ns: LocalNamespace, _: []const u8) ![]const u8 {
            if (self.probe) return "p";
            return switch (ns) {
                .meta => "m",
                .docs => "d",
            };
        }
    };

    const MockWrite = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: LocalNamespace, _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: LocalNamespace, _: []const u8) !void {
            self.value = null;
        }
    };

    const MockBatch = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: LocalNamespace, _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: LocalNamespace, _: []const u8) !void {
            self.value = null;
        }
    };

    const MockStore = struct {
        shared: *Shared,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{ .native_namespaces = true };
        }

        pub fn beginRead(self: *@This()) !MockRead {
            self.shared.read_calls += 1;
            return .{};
        }

        pub fn beginProbe(self: *@This()) !MockRead {
            self.shared.probe_calls += 1;
            return .{ .probe = true };
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(_: *@This()) !MockBatch {
            return .{};
        }
    };

    const mapNamespace = struct {
        fn map(ns: backend_types.Namespace) !LocalNamespace {
            if (ns.name == null) return .meta;
            if (std.mem.eql(u8, ns.name.?, "docs")) return .docs;
            return error.InvalidNamespace;
        }
    }.map;

    var shared = Shared{};
    const mock = MockStore{ .shared = &shared };
    var store = try namespaceStoreFrom(std.testing.allocator, mock, LocalNamespace, mapNamespace);
    defer store.deinit();
    try std.testing.expect(store.capabilities().native_namespaces);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("m", try read.get(.{}, "k"));
    try std.testing.expectEqualStrings("d", try read.get(.{ .name = "docs" }, "k"));

    var probe = try store.beginProbe();
    defer probe.abort();
    try std.testing.expectEqualStrings("p", try probe.get(.{}, "k"));
    try std.testing.expectEqual(@as(usize, 1), shared.read_calls);
    try std.testing.expectEqual(@as(usize, 1), shared.probe_calls);

    var write = try store.beginWrite();
    try write.put(.{ .name = "docs" }, "k", "wv");
    try std.testing.expectEqualStrings("wv", try write.get(.{ .name = "docs" }, "k"));
    try write.commit();

    var batch = try store.beginBatch();
    try batch.put(.{}, "k", "bv");
    try std.testing.expectEqualStrings("bv", try batch.get(.{}, "k"));
    try batch.commit();
}

test "runtime namespace store forwards batch options" {
    const LocalNamespace = enum { meta, docs };

    const Shared = struct {
        saw_batch_options: bool = false,
        last_mode: backend_types.BatchMode = .default,
    };

    const MockRead = struct {
        pub fn abort(_: *@This()) void {}
        pub fn get(_: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
    };

    const MockWrite = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: LocalNamespace, _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: LocalNamespace, _: []const u8) !void {}
    };

    const MockBatch = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: LocalNamespace, _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: LocalNamespace, _: []const u8) !void {}
    };

    const MockStore = struct {
        shared: *Shared,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{};
        }

        pub fn beginRead(_: *@This()) !MockRead {
            return .{};
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(_: *@This()) !MockBatch {
            return .{};
        }

        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !MockBatch {
            self.shared.saw_batch_options = true;
            self.shared.last_mode = options.mode;
            return .{};
        }
    };

    const mapNamespace = struct {
        fn map(ns: backend_types.Namespace) !LocalNamespace {
            if (ns.name == null) return .meta;
            if (std.mem.eql(u8, ns.name.?, "docs")) return .docs;
            return error.InvalidNamespace;
        }
    }.map;

    var shared = Shared{};
    const mock = MockStore{ .shared = &shared };
    var store = try namespaceStoreFrom(std.testing.allocator, mock, LocalNamespace, mapNamespace);
    defer store.deinit();

    var batch = try store.beginBatchWithOptions(.{ .mode = .bulk_ingest });
    try batch.commit();
    try std.testing.expect(shared.saw_batch_options);
    try std.testing.expectEqual(backend_types.BatchMode.bulk_ingest, shared.last_mode);
}

test "erased cursor retains read transaction until cursor close" {
    const Shared = struct {
        aborted: bool = false,
        cursor_closed: bool = false,
    };
    const MockCursor = struct {
        shared: *Shared,

        pub fn close(self: *@This()) void {
            self.shared.cursor_closed = true;
        }
        pub fn first(self: *@This()) !?Entry {
            if (self.shared.aborted) return error.TransactionClosed;
            return .{ .key = "key", .value = "value" };
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
    const MockRead = struct {
        shared: *Shared,

        pub fn abort(self: *@This()) void {
            self.shared.aborted = true;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn openCursor(self: *@This()) !MockCursor {
            return .{ .shared = self.shared };
        }
    };

    var shared = Shared{};
    var txn = try readTxnFrom(std.testing.allocator, MockRead{ .shared = &shared });
    var cursor = try txn.openCursor();
    txn.abort();

    try std.testing.expect(!shared.aborted);
    try std.testing.expectEqualStrings("key", (try cursor.first()).?.key);
    cursor.close();
    try std.testing.expect(shared.cursor_closed);
    try std.testing.expect(shared.aborted);
}

test "erased write transaction rejects commit while cursor retains snapshot" {
    const Shared = struct {
        committed: bool = false,
    };
    const MockCursor = struct {
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
    const MockWrite = struct {
        shared: *Shared,

        pub fn abort(_: *@This()) void {}
        pub fn commit(self: *@This()) !void {
            self.shared.committed = true;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    var shared = Shared{};
    var txn = try writeTxnFrom(std.testing.allocator, MockWrite{ .shared = &shared });
    var cursor = try txn.openCursor();
    try std.testing.expectError(error.TransactionCursorActive, txn.commit());
    try std.testing.expect(!shared.committed);
    cursor.close();
    try txn.commit();
    try std.testing.expect(shared.committed);
}
