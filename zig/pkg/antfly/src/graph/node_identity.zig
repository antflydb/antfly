// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Ref = struct {
    table: ?[]const u8,
    key: []const u8,
};

/// Compare complete graph identities. A missing table is semantically distinct
/// from an explicitly qualified table, even when the document keys match.
pub fn equal(left: Ref, right: Ref) bool {
    if ((left.table == null) != (right.table == null)) return false;
    if (left.table) |left_table| {
        if (!std.mem.eql(u8, left_table, right.table.?)) return false;
    }
    return std.mem.eql(u8, left.key, right.key);
}

pub const Key = struct {
    storage: []u8,
    table_len: ?usize,

    pub fn init(alloc: Allocator, ref: Ref) !Key {
        const table_len = if (ref.table) |table_name| table_name.len else 0;
        const total_len = std.math.add(usize, table_len, ref.key.len) catch
            return error.NodeIdentityTooLarge;
        const storage = try alloc.alloc(u8, total_len);
        if (ref.table) |table_name| @memcpy(storage[0..table_name.len], table_name);
        @memcpy(storage[table_len..], ref.key);
        return .{
            .storage = storage,
            .table_len = if (ref.table != null) table_len else null,
        };
    }

    pub fn table(self: Key) ?[]const u8 {
        const table_len = self.table_len orelse return null;
        return self.storage[0..table_len];
    }

    pub fn key(self: Key) []const u8 {
        return self.storage[self.table_len orelse 0 ..];
    }

    pub fn deinit(self: *Key, alloc: Allocator) void {
        alloc.free(self.storage);
        self.* = undefined;
    }
};

fn hashIdentity(table: ?[]const u8, key: []const u8) u64 {
    var hash = std.hash.Wyhash.init(0);
    var len_bytes: [@sizeOf(u64)]u8 = undefined;
    const table_tag: u8 = if (table == null) 0 else 1;
    hash.update(&.{table_tag});
    if (table) |table_name| {
        std.mem.writeInt(u64, &len_bytes, @intCast(table_name.len), .little);
        hash.update(&len_bytes);
        hash.update(table_name);
    }
    std.mem.writeInt(u64, &len_bytes, @intCast(key.len), .little);
    hash.update(&len_bytes);
    hash.update(key);
    return hash.final();
}

const StoredContext = struct {
    pub fn hash(_: StoredContext, value: Key) u64 {
        return hashIdentity(value.table(), value.key());
    }

    pub fn eql(_: StoredContext, left: Key, right: Key) bool {
        if ((left.table() == null) != (right.table() == null)) return false;
        if (left.table()) |left_table| {
            if (!std.mem.eql(u8, left_table, right.table().?)) return false;
        }
        return std.mem.eql(u8, left.key(), right.key());
    }
};

const AdaptedContext = struct {
    pub fn hash(_: AdaptedContext, value: Ref) u64 {
        return hashIdentity(value.table, value.key);
    }

    pub fn eql(_: AdaptedContext, left: Ref, right: Key) bool {
        if ((left.table == null) != (right.table() == null)) return false;
        if (left.table) |left_table| {
            if (!std.mem.eql(u8, left_table, right.table().?)) return false;
        }
        return std.mem.eql(u8, left.key, right.key());
    }
};

const RefContext = struct {
    pub fn hash(_: RefContext, value: Ref) u64 {
        return hashIdentity(value.table, value.key);
    }

    pub fn eql(_: RefContext, left: Ref, right: Ref) bool {
        return equal(left, right);
    }
};

/// A map whose identity slices are owned by another stable container. This is
/// useful when the identities must also be returned to a caller: the result
/// list remains the sole payload owner and the hash index retains only slice
/// descriptors and hash metadata.
pub fn BorrowedMap(comptime Value: type) type {
    return struct {
        const Self = @This();
        const Inner = std.HashMapUnmanaged(
            Ref,
            Value,
            RefContext,
            std.hash_map.default_max_load_percentage,
        );

        inner: Inner = .empty,

        pub fn contains(self: *const Self, ref: Ref) bool {
            return self.inner.contains(ref);
        }

        pub fn getPtr(self: *Self, ref: Ref) ?*Value {
            return self.inner.getPtr(ref);
        }

        pub fn capacity(self: *const Self) usize {
            return self.inner.capacity();
        }

        pub fn ensureTotalCapacity(self: *Self, alloc: Allocator, total_count: usize) !void {
            try self.inner.ensureTotalCapacity(alloc, @intCast(total_count));
        }

        pub fn putAssumeCapacityNoClobber(self: *Self, ref: Ref, value: Value) void {
            self.inner.putAssumeCapacityNoClobber(ref, value);
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            self.inner.deinit(alloc);
            self.* = .{};
        }
    };
}

pub fn Map(comptime Value: type) type {
    return struct {
        const Self = @This();
        const Inner = std.HashMapUnmanaged(
            Key,
            Value,
            StoredContext,
            std.hash_map.default_max_load_percentage,
        );

        inner: Inner = .empty,

        pub fn contains(self: *const Self, ref: Ref) bool {
            return self.inner.getAdapted(ref, AdaptedContext{}) != null;
        }

        pub fn get(self: *const Self, ref: Ref) ?Value {
            return self.inner.getAdapted(ref, AdaptedContext{});
        }

        pub fn getPtr(self: *Self, ref: Ref) ?*Value {
            return self.inner.getPtrAdapted(ref, AdaptedContext{});
        }

        pub fn putIfAbsent(
            self: *Self,
            alloc: Allocator,
            ref: Ref,
            value: Value,
        ) !bool {
            if (self.contains(ref)) return false;
            var owned = try Key.init(alloc, ref);
            errdefer owned.deinit(alloc);
            try self.inner.putNoClobber(alloc, owned, value);
            return true;
        }

        pub fn capacity(self: *const Self) usize {
            return self.inner.capacity();
        }

        pub fn ensureTotalCapacity(self: *Self, alloc: Allocator, total_count: usize) !void {
            try self.inner.ensureTotalCapacity(alloc, @intCast(total_count));
        }

        /// Insert an already-owned key after ensureTotalCapacity. Ownership of
        /// `key` transfers to the map and no allocation can occur here.
        pub fn putOwnedAssumeCapacityNoClobber(self: *Self, key: Key, value: Value) void {
            self.inner.putAssumeCapacityNoClobber(key, value);
        }

        pub fn count(self: *const Self) usize {
            return self.inner.count();
        }

        pub fn iterator(self: *Self) Inner.Iterator {
            return self.inner.iterator();
        }

        pub fn keyIterator(self: *Self) Inner.KeyIterator {
            return self.inner.keyIterator();
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            var it = self.inner.keyIterator();
            while (it.next()) |key| key.deinit(alloc);
            self.inner.deinit(alloc);
            self.* = .{};
        }
    };
}

test "graph node identity is table scoped and delimiter independent" {
    var set = Map(void){};
    defer set.deinit(std.testing.allocator);

    try std.testing.expect(try set.putIfAbsent(
        std.testing.allocator,
        .{ .table = "docs", .key = "same" },
        {},
    ));
    try std.testing.expect(try set.putIfAbsent(
        std.testing.allocator,
        .{ .table = "entities", .key = "same" },
        {},
    ));
    try std.testing.expect(try set.putIfAbsent(
        std.testing.allocator,
        .{ .table = null, .key = "docs:same" },
        {},
    ));
    try std.testing.expect(!try set.putIfAbsent(
        std.testing.allocator,
        .{ .table = "docs", .key = "same" },
        {},
    ));
    try std.testing.expectEqual(@as(usize, 3), set.count());
}
