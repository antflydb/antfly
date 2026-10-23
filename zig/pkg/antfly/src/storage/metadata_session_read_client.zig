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
    fn cast(ptr: *anyopaque) *Read {
        return @ptrCast(@alignCast(ptr));
    }
    fn abort(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        abi.antfly_metadata_session_read_close(self.handle);
        self.arena.deinit();
        a.destroy(self);
    }
    fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
        const self = cast(ptr);
        var response: abi.OwnedBytes = .{};
        try status(abi.antfly_metadata_session_read_invoke(self.handle, &.{ .operation = .get, .key = .fromSlice(key) }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (response.len > 32 << 20) return error.TransactionTooLarge;
        return self.arena.allocator().dupe(u8, response.slice());
    }
    fn cursor(a: A, ptr: *anyopaque) !erased.Cursor {
        const self = try a.create(Cursor);
        self.* = .{ .read = cast(ptr), .arena = std.heap.ArenaAllocator.init(a) };
        return .{ .allocator = a, .ptr = self, .vtable = &.{ .close = Cursor.close, .first = Cursor.first, .last = Cursor.last, .next = Cursor.next, .prev = Cursor.prev, .seek_at_or_after = Cursor.after, .seek_at_or_before = Cursor.before } };
    }
};
pub fn open(a: A, store: ?*anyopaque, group: u64) !erased.ReadTxn {
    var handle: ?*anyopaque = null;
    try status(abi.antfly_metadata_session_read_open(store, &.{ .group_id = group }, &handle));
    errdefer abi.antfly_metadata_session_read_close(handle);
    const native = handle orelse return error.StorageKernelFailure;
    const read = try a.create(Read);
    read.* = .{ .handle = native, .arena = std.heap.ArenaAllocator.init(a) };
    return .{ .allocator = a, .ptr = read, .vtable = &.{ .abort = Read.abort, .get = Read.get, .open_cursor = Read.cursor } };
}
fn status(value: abi.Status) !void {
    return identity.statusToError(value);
}
const Cursor = struct {
    read: *Read,
    arena: std.heap.ArenaAllocator,
    key: std.ArrayList(u8) = .empty,
    fn cast(ptr: *anyopaque) *Cursor {
        return @ptrCast(@alignCast(ptr));
    }
    fn close(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        self.key.deinit(a);
        self.arena.deinit();
        a.destroy(self);
    }
    fn invoke(self: *Cursor, operation: abi.MetadataSessionReadOperation, key: []const u8) !?erased.Entry {
        var response: abi.OwnedBytes = .{};
        const result = abi.antfly_metadata_session_read_invoke(self.read.handle, &.{ .operation = operation, .key = .fromSlice(key) }, &response);
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (result == .not_found) return null;
        try status(result);
        const bytes = response.slice();
        if (bytes.len < 4 or bytes.len > (32 << 20) + 1024) return error.StorageKernelFailure;
        const length = std.mem.readInt(u32, bytes[0..4], .little);
        if (length > bytes.len - 4) return error.StorageKernelFailure;
        _ = self.arena.reset(.retain_capacity);
        const owned = self.arena.allocator();
        self.key.clearRetainingCapacity();
        try self.key.appendSlice(self.arena.child_allocator, bytes[4..][0..length]);
        return .{ .key = self.key.items, .value = try owned.dupe(u8, bytes[4 + length ..]) };
    }
    fn first(ptr: *anyopaque) !?erased.Entry {
        return cast(ptr).invoke(.first, "");
    }
    fn last(ptr: *anyopaque) !?erased.Entry {
        return cast(ptr).invoke(.last, "");
    }
    fn next(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        return self.invoke(.next, self.key.items);
    }
    fn prev(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        return self.invoke(.prev, self.key.items);
    }
    fn after(ptr: *anyopaque, key: []const u8) !?erased.Entry {
        return cast(ptr).invoke(.seek_at_or_after, key);
    }
    fn before(ptr: *anyopaque, key: []const u8) !?erased.Entry {
        return cast(ptr).invoke(.seek_at_or_before, key);
    }
};
