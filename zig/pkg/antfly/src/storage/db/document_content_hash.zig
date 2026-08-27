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

pub const Digest = [std.crypto.hash.Blake3.digest_length]u8;

/// Hash the user-visible content of a JSON document. Object field order does
/// not affect the digest, while array order and JSON scalar kinds do. Antfly's
/// injected identity and timestamp fields are excluded at every object level to
/// match linear-merge comparison semantics.
pub fn hashJson(alloc: std.mem.Allocator, raw: []const u8) !Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    var hasher = std.crypto.hash.Blake3.init(.{});
    try hashValue(alloc, &hasher, parsed.value);
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashValue(
    alloc: std.mem.Allocator,
    hasher: *std.crypto.hash.Blake3,
    value: std.json.Value,
) !void {
    switch (value) {
        .null => hasher.update("n"),
        .bool => |flag| hasher.update(if (flag) "b1" else "b0"),
        .integer => |number| {
            hasher.update("i");
            hashInt(hasher, i64, number);
        },
        .float => |number| {
            hasher.update("f");
            // JSON equality treats negative and positive zero as equal.
            const bits: u64 = if (number == 0) 0 else @bitCast(number);
            hashInt(hasher, u64, bits);
        },
        .number_string => |number| {
            hasher.update("r");
            hashBytes(hasher, number);
        },
        .string => |string| {
            hasher.update("s");
            hashBytes(hasher, string);
        },
        .array => |array| {
            hasher.update("a");
            hashInt(hasher, u64, @intCast(array.items.len));
            for (array.items) |item| try hashValue(alloc, hasher, item);
        },
        .object => |object| {
            hasher.update("o");
            const keys = try alloc.alloc([]const u8, comparableObjectFieldCount(object));
            defer alloc.free(keys);

            var key_index: usize = 0;
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                if (isIgnoredSystemField(entry.key_ptr.*)) continue;
                keys[key_index] = entry.key_ptr.*;
                key_index += 1;
            }
            std.mem.sort([]const u8, keys, {}, lessThanString);
            hashInt(hasher, u64, @intCast(keys.len));
            for (keys) |key| {
                hashBytes(hasher, key);
                try hashValue(alloc, hasher, object.get(key).?);
            }
        },
    }
}

fn hashInt(hasher: *std.crypto.hash.Blake3, comptime T: type, value: T) void {
    var encoded: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &encoded, value, .little);
    hasher.update(&encoded);
}

fn hashBytes(hasher: *std.crypto.hash.Blake3, value: []const u8) void {
    hashInt(hasher, u64, @intCast(value.len));
    hasher.update(value);
}

fn comparableObjectFieldCount(object: std.json.ObjectMap) usize {
    var count: usize = 0;
    var iterator = object.iterator();
    while (iterator.next()) |entry| {
        if (!isIgnoredSystemField(entry.key_ptr.*)) count += 1;
    }
    return count;
}

fn isIgnoredSystemField(field: []const u8) bool {
    return std.mem.eql(u8, field, "_timestamp") or std.mem.eql(u8, field, "_id");
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

test "document content hash ignores field order and injected system fields" {
    const left = try hashJson(std.testing.allocator,
        \\{"title":"alpha","nested":{"count":2,"_timestamp":1},"tags":["a","b"]}
    );
    const right = try hashJson(std.testing.allocator,
        \\{"tags":["a","b"],"_id":"doc:a","nested":{"_timestamp":9,"count":2},"title":"alpha","_timestamp":1234}
    );
    try std.testing.expectEqualSlices(u8, &left, &right);
}

test "document content hash preserves meaningful JSON differences" {
    const base = try hashJson(std.testing.allocator,
        \\{"title":"alpha","tags":["a","b"]}
    );
    const changed = try hashJson(std.testing.allocator,
        \\{"title":"alpha","tags":["b","a"]}
    );
    try std.testing.expect(!std.mem.eql(u8, &base, &changed));
}
