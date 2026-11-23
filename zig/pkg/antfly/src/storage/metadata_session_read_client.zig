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

//! Opaque retained metadata reads across the compiled storage boundary.
const std = @import("std");
const abi = @import("kernel_owner_abi");
const identity = @import("kernel_error_identity");
const erased = @import("antfly_local_sources").storage_backend_erased;
const A = std.mem.Allocator;
const Read = struct {
    handle: *anyopaque,
    arena: std.heap.ArenaAllocator,
    refs: std.atomic.Value(usize) = .init(1),
    fn cast(ptr: *anyopaque) *Read {
        return @ptrCast(@alignCast(ptr));
    }
    fn abort(_: A, ptr: *anyopaque) void {
        cast(ptr).release();
    }
    fn release(self: *Read) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.arena.child_allocator;
        abi.antfly_metadata_session_read_close(self.handle);
        self.arena.deinit();
        a.destroy(self);
    }
    fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
        const self = cast(ptr);
        return self.readValue(self.arena.allocator(), .{ .operation = .get, .key = .fromSlice(key) });
    }
    fn readValue(self: *Read, a: A, request: abi.MetadataSessionReadRequest) ![]const u8 {
        var response: abi.OwnedBytes = .{};
        try status(abi.antfly_metadata_session_read_invoke(self.handle, &request, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (response.len > abi.metadata_session_max_value_bytes) return error.TransactionTooLarge;
        return a.dupe(u8, response.slice());
    }
    fn prefixPage(ptr: *anyopaque, alloc: A, prefix: []const u8, after: ?[]const u8, length: usize) ![]erased.Entry {
        const self = cast(ptr);
        if (length > 128) return error.InvalidArgument;
        const keys = try std.mem.concat(alloc, u8, &.{ prefix, after orelse "" });
        defer alloc.free(keys);
        const bytes = try self.readValue(alloc, .{
            .operation = .prefix_page,
            .key = .fromSlice(keys),
            .offset = prefix.len,
            .length = @intCast(length),
        });
        const rows = try alloc.alloc(erased.Entry, 64);
        var count: usize = 0;
        var pos: usize = 0;
        while (pos < bytes.len) {
            if (bytes.len - pos < 4 or count == rows.len) return error.StorageKernelFailure;
            const key_len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            const value_len = std.mem.readInt(u16, bytes[pos + 2 ..][0..2], .little);
            pos += 4;
            if (key_len == 0 or value_len > length or key_len + @as(usize, value_len) > bytes.len - pos) return error.StorageKernelFailure;
            rows[count] = .{ .key = bytes[pos..][0..key_len], .value = bytes[pos + key_len ..][0..value_len] };
            pos += @as(usize, key_len) + value_len;
            count += 1;
        }
        return rows[0..count];
    }

    fn countPrefix(ptr: *anyopaque, key: []const u8) !u64 {
        const self = cast(ptr);
        var response: abi.OwnedBytes = .{};
        try status(abi.antfly_metadata_session_read_invoke(self.handle, &.{ .operation = .count_prefix, .key = .fromSlice(key) }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (response.len != 8) return error.StorageKernelFailure;
        return std.mem.readInt(u64, response.slice()[0..8], .little);
    }
    fn scope(a: A, ptr: *anyopaque) !erased.ReadScope {
        const read = cast(ptr);
        _ = read.refs.fetchAdd(1, .monotonic);
        errdefer read.release();
        return erased.readScopeFrom(a, Scope{ .read = read, .arena = std.heap.ArenaAllocator.init(a) });
    }
    const Scope = struct {
        read: *Read,
        arena: std.heap.ArenaAllocator,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.read.readValue(self.arena.allocator(), .{ .operation = .get, .key = .fromSlice(key) });
        }
        pub fn getRange(self: *@This(), key: []const u8, offset: u64, length: usize) ![]const u8 {
            if (length > abi.metadata_session_max_value_bytes) return error.TransactionTooLarge;
            const value = try self.read.readValue(self.arena.allocator(), .{ .operation = .get_range, .key = .fromSlice(key), .offset = offset, .length = @intCast(length) });
            if (value.len != length) return error.StorageKernelFailure;
            return value;
        }
        pub fn probePrefix(self: *@This(), key: []const u8, length: usize) ![]const u8 {
            if (length > 128) return error.InvalidArgument;
            return self.read.readValue(self.arena.allocator(), .{ .operation = .probe_prefix, .key = .fromSlice(key), .length = @intCast(length) });
        }

        pub fn close(self: *@This()) void {
            self.arena.deinit();
            self.read.release();
        }
    };
    fn cursor(a: A, ptr: *anyopaque) !erased.Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        const read = cast(ptr);
        var handle: ?*anyopaque = null;
        try status(abi.antfly_metadata_session_cursor_open(read.handle, &handle));
        const native = handle orelse return error.StorageKernelFailure;
        _ = read.refs.fetchAdd(1, .monotonic);
        self.* = .{ .read = read, .handle = native, .arena = std.heap.ArenaAllocator.init(a) };
        return .{ .allocator = a, .ptr = self, .vtable = &.{
            .close = Cursor.close,
            .first = Cursor.first,
            .last = Cursor.last,
            .next = Cursor.next,
            .prev = Cursor.prev,
            .seek_at_or_after = Cursor.after,
            .seek_at_or_before = Cursor.before,
            .set_keys_only = Cursor.setKeysOnly,
        } };
    }
};
pub fn open(a: A, store: ?*anyopaque, group: u64) !erased.ReadTxn {
    var handle: ?*anyopaque = null;
    try status(abi.antfly_metadata_session_read_open(store, &.{ .group_id = group }, &handle));
    errdefer abi.antfly_metadata_session_read_close(handle);
    const native = handle orelse return error.StorageKernelFailure;
    const read = try a.create(Read);
    read.* = .{ .handle = native, .arena = std.heap.ArenaAllocator.init(a) };
    return .{ .allocator = a, .ptr = read, .vtable = &.{ .abort = Read.abort, .get = Read.get, .open_cursor = Read.cursor, .open_read_scope = Read.scope, .count_prefix = Read.countPrefix, .read_prefix_page = Read.prefixPage } };
}
fn status(value: abi.Status) !void {
    return identity.statusToError(value);
}
const Cursor = struct {
    read: *Read,
    arena: std.heap.ArenaAllocator,
    handle: *anyopaque,
    keys_only: bool = false,
    fn cast(ptr: *anyopaque) *Cursor {
        return @ptrCast(@alignCast(ptr));
    }
    fn close(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        abi.antfly_metadata_session_cursor_close(self.handle);
        self.read.release();
        self.arena.deinit();
        a.destroy(self);
    }
    fn invoke(self: *Cursor, operation: abi.MetadataSessionReadOperation, key: []const u8) !?erased.Entry {
        var response: abi.OwnedBytes = .{};
        const result = abi.antfly_metadata_session_cursor_invoke(self.handle, &.{ .operation = operation, .key = .fromSlice(key), .length = @intFromBool(self.keys_only) }, &response);
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (result == .not_found) return null;
        try status(result);
        const bytes = response.slice();
        if (bytes.len < 4 or bytes.len > abi.metadata_session_max_value_bytes + 1024) return error.StorageKernelFailure;
        const length = std.mem.readInt(u32, bytes[0..4], .little);
        if (length > bytes.len - 4) return error.StorageKernelFailure;
        _ = self.arena.reset(.retain_capacity);
        const owned = self.arena.allocator();
        return .{ .key = try owned.dupe(u8, bytes[4..][0..length]), .value = try owned.dupe(u8, bytes[4 + length ..]) };
    }
    fn setKeysOnly(ptr: *anyopaque) void {
        cast(ptr).keys_only = true;
    }
    fn first(ptr: *anyopaque) !?erased.Entry {
        return cast(ptr).invoke(.first, "");
    }
    fn last(ptr: *anyopaque) !?erased.Entry {
        return cast(ptr).invoke(.last, "");
    }
    fn next(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        return self.invoke(.next, "");
    }
    fn prev(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        return self.invoke(.prev, "");
    }
    fn after(ptr: *anyopaque, key: []const u8) !?erased.Entry {
        return cast(ptr).invoke(.seek_at_or_after, key);
    }
    fn before(ptr: *anyopaque, key: []const u8) !?erased.Entry {
        return cast(ptr).invoke(.seek_at_or_before, key);
    }
};
