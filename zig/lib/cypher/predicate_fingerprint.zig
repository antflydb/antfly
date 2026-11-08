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

//! Candidate indexing only: matching digests always require exact Cypher equality.
const std = @import("std");
const v = @import("value.zig");
pub const Digest = [32]u8;
fn start(tag: u8) std.crypto.hash.Blake3 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(&.{tag});
    return hash;
}
fn finish(hash: *std.crypto.hash.Blake3) Digest {
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}
fn text(bytes: []const u8) Digest {
    var hash = start(4);
    hash.update(bytes);
    return finish(&hash);
}
fn scalar(item: v.Value) Digest {
    if (item == .number) {
        const n = item.number;
        if (n >= -9223372036854775808.0 and n < 9223372036854775808.0 and @trunc(n) == n)
            return scalar(.{ .integer = @intFromFloat(n) });
    }
    var hash = start(switch (item) {
        .null => 0,
        .boolean => 1,
        .integer => 2,
        .number => 3,
        else => unreachable,
    });
    switch (item) {
        .boolean => |b| hash.update(&.{@intFromBool(b)}),
        .integer => |n| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(i64, &bytes, n, .little);
            hash.update(&bytes);
        },
        .number => |n| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, @bitCast(n), .little);
            hash.update(&bytes);
        },
        .null => {},
        else => unreachable,
    }
    return finish(&hash);
}
const Object = struct {
    sums: [4]u64 = @splat(0),
    count: u64 = 0,
    fn add(self: *@This(), key: Digest, item: Digest) void {
        var hash = start(7);
        hash.update(&key);
        hash.update(&item);
        const pair = finish(&hash);
        for (&self.sums, 0..) |*sum, i| sum.* +%= std.mem.readInt(u64, pair[i * 8 ..][0..8], .little);
        self.count += 1;
    }
    fn digest(self: @This()) Digest {
        var hash = start(6);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, self.count, .little);
        hash.update(&bytes);
        for (self.sums) |sum| {
            std.mem.writeInt(u64, &bytes, sum, .little);
            hash.update(&bytes);
        }
        return finish(&hash);
    }
};
pub fn value(item: v.Value) Digest {
    return switch (item) {
        .string => |bytes| text(bytes),
        .map => |entries| result: {
            var object: Object = .{};
            for (entries) |entry| object.add(text(entry.name), value(entry.value));
            break :result object.digest();
        },
        .list => |items| result: {
            var hash = start(5);
            for (items) |child| hash.update(&value(child));
            break :result finish(&hash);
        },
        else => scalar(item),
    };
}
pub fn raw(a: std.mem.Allocator, bytes: []const u8) !Digest {
    var scanner = std.json.Scanner.initCompleteInput(a, bytes);
    defer scanner.deinit();
    return scan(a, &scanner, 0);
}
fn string(scanner: *std.json.Scanner) !Digest {
    var hash = start(4);
    while (true) {
        const token = try scanner.next();
        switch (token) {
            .string, .partial_string => |bytes| hash.update(bytes),
            .partial_string_escaped_1 => |bytes| hash.update(&bytes),
            .partial_string_escaped_2 => |bytes| hash.update(&bytes),
            .partial_string_escaped_3 => |bytes| hash.update(&bytes),
            .partial_string_escaped_4 => |bytes| hash.update(&bytes),
            else => return error.SyntaxError,
        }
        if (token == .string) return finish(&hash);
    }
}
fn scan(a: std.mem.Allocator, scanner: *std.json.Scanner, depth: usize) anyerror!Digest {
    if (depth > 64) return error.CypherValueDepthExceeded;
    switch (try scanner.peekNextTokenType()) {
        .string => return string(scanner),
        .array_begin => {
            _ = try scanner.next();
            var hash = start(5);
            while (try scanner.peekNextTokenType() != .array_end) hash.update(&try scan(a, scanner, depth + 1));
            _ = try scanner.next();
            return finish(&hash);
        },
        .object_begin => {
            _ = try scanner.next();
            var object: Object = .{};
            while (try scanner.peekNextTokenType() != .object_end) {
                const key = try string(scanner);
                object.add(key, try scan(a, scanner, depth + 1));
            }
            _ = try scanner.next();
            return object.digest();
        },
        else => return scalar(switch (try scanner.next()) {
            .null => .null,
            .true => .{ .boolean = true },
            .false => .{ .boolean = false },
            .number => |bytes| try v.Value.fromJson(a, .{ .number_string = bytes }),
            else => return error.SyntaxError,
        }),
    }
}
