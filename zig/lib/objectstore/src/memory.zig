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
const client_mod = @import("client.zig");
const types = @import("types.zig");

const StoredObject = struct {
    body: []u8,
    etag: []u8,
    content_type: ?[]u8 = null,

    fn deinit(self: *StoredObject, alloc: Allocator) void {
        alloc.free(self.body);
        alloc.free(self.etag);
        if (self.content_type) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const MemoryClient = struct {
    alloc: Allocator,
    buckets: std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(StoredObject)) = .empty,

    pub fn init(alloc: Allocator) MemoryClient {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryClient) void {
        var bucket_it = self.buckets.iterator();
        while (bucket_it.next()) |bucket_entry| {
            self.alloc.free(bucket_entry.key_ptr.*);
            var object_map = bucket_entry.value_ptr.*;
            var object_it = object_map.iterator();
            while (object_it.next()) |obj_entry| {
                self.alloc.free(obj_entry.key_ptr.*);
                obj_entry.value_ptr.deinit(self.alloc);
            }
            object_map.deinit(self.alloc);
        }
        self.buckets.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn client(self: *MemoryClient) client_mod.Client {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn bucketExists(self: *MemoryClient, bucket: []const u8) bool {
        return self.buckets.contains(bucket);
    }

    fn makeBucket(self: *MemoryClient, bucket: []const u8) !void {
        _ = try self.ensureBucket(bucket);
    }

    fn ensureBucket(self: *MemoryClient, bucket: []const u8) !*std.StringHashMapUnmanaged(StoredObject) {
        if (self.buckets.getPtr(bucket)) |existing| return existing;
        const owned_name = try self.alloc.dupe(u8, bucket);
        errdefer self.alloc.free(owned_name);
        try self.buckets.put(self.alloc, owned_name, .empty);
        return self.buckets.getPtr(bucket).?;
    }

    fn putObject(self: *MemoryClient, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        var object_map = try self.ensureBucket(bucket);

        const existing = object_map.getPtr(key);
        if (opts.if_none_match and existing != null) return error.PreconditionFailed;
        if (opts.if_match_etag) |expected| {
            if (existing == null or !std.mem.eql(u8, existing.?.etag, expected)) return error.PreconditionFailed;
        }

        const etag = try sha256HexAlloc(alloc, body);
        errdefer alloc.free(etag);

        const owned_body = try self.alloc.dupe(u8, body);
        errdefer self.alloc.free(owned_body);
        const owned_etag = try self.alloc.dupe(u8, etag);
        errdefer self.alloc.free(owned_etag);
        const owned_content_type = if (opts.content_type) |value| try self.alloc.dupe(u8, value) else null;
        errdefer if (owned_content_type) |value| self.alloc.free(value);
        const replacement = StoredObject{
            .body = owned_body,
            .etag = owned_etag,
            .content_type = owned_content_type,
        };

        // Fully construct before swapping the published value. Allocation
        // failure therefore leaves the old object intact and cannot leak a
        // partially built replacement. Reuse the existing owned map key and
        // capacity on overwrite.
        if (existing) |value| {
            var previous = value.*;
            value.* = replacement;
            previous.deinit(self.alloc);
            return .{ .etag = etag };
        }
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        try object_map.ensureUnusedCapacity(self.alloc, 1);
        object_map.putAssumeCapacity(owned_key, replacement);

        return .{ .etag = etag };
    }

    fn getObject(self: *MemoryClient, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        _ = opts.version_id;
        const object_map = self.buckets.getPtr(bucket) orelse return error.FileNotFound;
        const object = object_map.getPtr(key) orelse return error.FileNotFound;
        if (opts.if_match_etag) |expected| {
            if (!std.mem.eql(u8, object.etag, expected)) return error.PreconditionFailed;
        }
        if (opts.part_number) |part_number| {
            if (part_number != 1) return error.InvalidPartNumber;
        }

        const body = if (opts.range) |range| blk: {
            const start: usize = @intCast(range.offset);
            if (start > object.body.len) return error.InvalidRange;
            const end = if (range.length) |len| @min(object.body.len, start + @as(usize, @intCast(len))) else object.body.len;
            break :blk try alloc.dupe(u8, object.body[start..end]);
        } else try alloc.dupe(u8, object.body);

        return .{
            .body = body,
            .metadata = .{
                .bucket = try alloc.dupe(u8, bucket),
                .key = try alloc.dupe(u8, key),
                .etag = try alloc.dupe(u8, object.etag),
                .content_length = @intCast(object.body.len),
                .content_type = if (object.content_type) |value| try alloc.dupe(u8, value) else null,
            },
        };
    }

    fn getObjectAttributes(self: *MemoryClient, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        const object_map = self.buckets.getPtr(bucket) orelse return error.FileNotFound;
        const object = object_map.getPtr(key) orelse return error.FileNotFound;
        const parts = try alloc.alloc(types.ObjectPart, 1);
        errdefer alloc.free(parts);
        parts[0] = .{
            .part_number = 1,
            .size = @intCast(object.body.len),
            .etag = try alloc.dupe(u8, object.etag),
        };
        return .{
            .etag = try alloc.dupe(u8, object.etag),
            .content_length = @intCast(object.body.len),
            .content_type = if (object.content_type) |value| try alloc.dupe(u8, value) else null,
            .parts = parts,
        };
    }

    fn statObject(self: *MemoryClient, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const object_map = self.buckets.getPtr(bucket) orelse return error.FileNotFound;
        const object = object_map.getPtr(key) orelse return error.FileNotFound;
        return .{
            .bucket = try alloc.dupe(u8, bucket),
            .key = try alloc.dupe(u8, key),
            .etag = try alloc.dupe(u8, object.etag),
            .content_length = @intCast(object.body.len),
            .content_type = if (object.content_type) |value| try alloc.dupe(u8, value) else null,
        };
    }

    fn deleteObject(self: *MemoryClient, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        _ = opts.version_id;
        const object_map = self.buckets.getPtr(bucket) orelse return error.FileNotFound;
        const object = object_map.getPtr(key) orelse return error.FileNotFound;
        if (opts.if_match_etag) |expected| {
            if (!std.mem.eql(u8, object.etag, expected)) return error.PreconditionFailed;
        }
        var removed = object_map.*.fetchRemove(key) orelse return error.FileNotFound;
        self.alloc.free(removed.key);
        removed.value.deinit(self.alloc);
    }

    fn listObjects(self: *MemoryClient, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const object_map = self.buckets.getPtr(bucket) orelse {
            return .{
                .entries = try alloc.alloc(types.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            };
        };

        var keys = std.ArrayListUnmanaged([]const u8).empty;
        defer keys.deinit(alloc);
        var it = object_map.iterator();
        while (it.next()) |entry| try keys.append(alloc, entry.key_ptr.*);
        std.mem.sort([]const u8, keys.items, {}, lessKey);

        var out = std.ArrayListUnmanaged(types.ListEntry).empty;
        var prefixes = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (out.items) |*entry| entry.deinit(alloc);
            out.deinit(alloc);
            for (prefixes.items) |prefix| alloc.free(prefix);
            prefixes.deinit(alloc);
        }

        var count: u32 = 0;
        var last_cursor_key: ?[]const u8 = null;
        var truncated = false;
        const continuation = opts.continuation_token orelse opts.start_after;
        for (keys.items) |key| {
            if (!std.mem.startsWith(u8, key, opts.prefix)) continue;
            if (continuation) |token| {
                if (std.mem.order(u8, key, token) != .gt) continue;
            }
            const object = object_map.getPtr(key).?;
            if (!opts.recursive and opts.delimiter.len > 0 and key.len > opts.prefix.len) {
                if (std.mem.indexOf(u8, key[opts.prefix.len..], opts.delimiter)) |delimiter_offset| {
                    const prefix_end = opts.prefix.len + delimiter_offset + opts.delimiter.len;
                    const common_prefix = key[0..prefix_end];
                    if (containsPrefix(prefixes.items, common_prefix)) {
                        // Advance beyond every object collapsed into the last
                        // emitted common prefix before issuing a page token.
                        last_cursor_key = key;
                        continue;
                    }
                    if (count >= opts.max_keys) {
                        truncated = true;
                        break;
                    }
                    try prefixes.append(alloc, try alloc.dupe(u8, common_prefix));
                    count += 1;
                    last_cursor_key = key;
                    continue;
                }
            }
            if (count >= opts.max_keys) {
                truncated = true;
                break;
            }
            try out.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .etag = try alloc.dupe(u8, object.etag),
                .size = @intCast(object.body.len),
            });
            count += 1;
            last_cursor_key = key;
        }
        std.mem.sort(types.ListEntry, out.items, {}, lessEntry);
        return .{
            .entries = try out.toOwnedSlice(alloc),
            .common_prefixes = try prefixes.toOwnedSlice(alloc),
            .next_continuation_token = if (truncated and last_cursor_key != null)
                try alloc.dupe(u8, last_cursor_key.?)
            else
                null,
        };
    }

    const vtable: client_mod.Client.VTable = .{
        .deinit = erasedDeinit,
        .bucket_exists = erasedBucketExists,
        .make_bucket = erasedMakeBucket,
        .put_object = erasedPutObject,
        .get_object = erasedGetObject,
        .get_object_attributes = erasedGetObjectAttributes,
        .stat_object = erasedStatObject,
        .delete_object = erasedDeleteObject,
        .list_objects = erasedListObjects,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedBucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        return self.bucketExists(bucket);
    }

    fn erasedMakeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        try self.makeBucket(bucket);
    }

    fn erasedPutObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        return try self.putObject(alloc, bucket, key, body, opts);
    }

    fn erasedGetObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        return try self.getObject(alloc, bucket, key, opts);
    }

    fn erasedGetObjectAttributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        return try self.getObjectAttributes(alloc, bucket, key);
    }

    fn erasedStatObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        return try self.statObject(alloc, bucket, key);
    }

    fn erasedDeleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        try self.deleteObject(bucket, key, opts);
    }

    fn erasedListObjects(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const self: *MemoryClient = @ptrCast(@alignCast(ptr));
        return try self.listObjects(alloc, bucket, opts);
    }
};

fn sha256HexAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const out = try alloc.alloc(u8, 64);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn lessEntry(_: void, lhs: types.ListEntry, rhs: types.ListEntry) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn lessKey(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

fn containsPrefix(prefixes: []const []u8, needle: []const u8) bool {
    for (prefixes) |prefix| {
        if (std.mem.eql(u8, prefix, needle)) return true;
    }
    return false;
}

test "memory client supports put get stat list and delete" {
    const alloc = std.testing.allocator;
    var memory = MemoryClient.init(alloc);
    var client = memory.client();
    defer client.deinit();

    try std.testing.expect(!(try client.bucketExists("bucket")));
    try client.makeBucket("bucket");
    try std.testing.expect(try client.bucketExists("bucket"));

    var put = try client.putObject("bucket", "a/one", "alpha", .{ .content_type = "text/plain" });
    defer put.deinit(alloc);
    try std.testing.expect(put.etag != null);

    var got = try client.getObject("bucket", "a/one", .{});
    defer got.deinit(alloc);
    try std.testing.expectEqualStrings("alpha", got.body);

    var meta = try client.statObject("bucket", "a/one");
    defer meta.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), meta.content_length);

    var listed = try client.listObjects("bucket", .{ .prefix = "a/" });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), listed.entries.len);

    var attrs = try client.getObjectAttributes("bucket", "a/one");
    defer attrs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), attrs.parts.len);

    try client.deleteObject("bucket", "a/one", .{});
    try std.testing.expectError(error.FileNotFound, client.getObject("bucket", "a/one", .{}));
}

test "memory client supports non-recursive listing with common prefixes" {
    const alloc = std.testing.allocator;
    var memory = MemoryClient.init(alloc);
    var client = memory.client();
    defer client.deinit();

    try client.makeBucket("bucket");
    var put_a = try client.putObject("bucket", "logs/2025/a.txt", "a", .{});
    defer put_a.deinit(alloc);
    var put_b = try client.putObject("bucket", "logs/2026/b.txt", "b", .{});
    defer put_b.deinit(alloc);

    var listed = try client.listObjects("bucket", .{
        .prefix = "logs/",
        .recursive = false,
    });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), listed.entries.len);
    try std.testing.expectEqual(@as(usize, 2), listed.common_prefixes.len);
}

test "memory client continuation tokens advance across filtered recursive pages" {
    const alloc = std.testing.allocator;
    var memory = MemoryClient.init(alloc);
    var client = memory.client();
    defer client.deinit();

    inline for (&.{ "other/zero", "snap/a", "snap/b", "snap/c", "snap/d", "tail/zero" }) |key| {
        var put = try client.putObject("bucket", key, key, .{});
        put.deinit(alloc);
    }

    var token: ?[]u8 = null;
    defer if (token) |value| alloc.free(value);
    var seen = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (seen.items) |key| alloc.free(key);
        seen.deinit(alloc);
    }
    while (true) {
        var page = try client.listObjects("bucket", .{
            .prefix = "snap/",
            .max_keys = 2,
            .continuation_token = token,
        });
        defer page.deinit(alloc);
        for (page.entries) |entry| try seen.append(alloc, try alloc.dupe(u8, entry.key));
        const next = if (page.next_continuation_token) |value| try alloc.dupe(u8, value) else null;
        if (token) |value| alloc.free(value);
        token = next;
        if (token == null) break;
    }
    try std.testing.expectEqual(@as(usize, 4), seen.items.len);
    try std.testing.expectEqualStrings("snap/a", seen.items[0]);
    try std.testing.expectEqualStrings("snap/d", seen.items[3]);
}

test "memory client continuation tokens do not repeat collapsed prefixes" {
    const alloc = std.testing.allocator;
    var memory = MemoryClient.init(alloc);
    var client = memory.client();
    defer client.deinit();

    inline for (&.{ "logs/2025/a", "logs/2025/b", "logs/2026/a", "logs/2026/b" }) |key| {
        var put = try client.putObject("bucket", key, key, .{});
        put.deinit(alloc);
    }
    var first = try client.listObjects("bucket", .{ .prefix = "logs/", .recursive = false, .max_keys = 1 });
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first.common_prefixes.len);
    try std.testing.expectEqualStrings("logs/2025/", first.common_prefixes[0]);
    var second = try client.listObjects("bucket", .{
        .prefix = "logs/",
        .recursive = false,
        .max_keys = 1,
        .continuation_token = first.next_continuation_token,
    });
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second.common_prefixes.len);
    try std.testing.expectEqualStrings("logs/2026/", second.common_prefixes[0]);
    try std.testing.expect(second.next_continuation_token == null);
}
