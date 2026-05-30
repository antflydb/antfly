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

// Result cache with TTL expiration and singleflight deduplication.
// Mirrors the legacy Go inference caching strategy.

const std = @import("std");

// Zig 0.17 removed `@cImport`; this only needed a nanosecond timestamp for cache
// entry timing, which `std.time` provides directly.
fn nowNs() i64 {
    return @as(i64, @truncate(std.time.nanoTimestamp()));
}

pub fn ResultCache(comptime V: type) type {
    return struct {
        const Self = @This();

        const Entry = struct {
            value: V,
            expires_at: i64,
        };

        map: std.StringHashMap(Entry),
        ttl_ns: i64,
        allocator: std.mem.Allocator,
        hits: u64,
        misses: u64,

        pub fn init(allocator: std.mem.Allocator, ttl_ms: u64) Self {
            return .{
                .map = std.StringHashMap(Entry).init(allocator),
                .ttl_ns = @intCast(ttl_ms * std.time.ns_per_ms),
                .allocator = allocator,
                .hits = 0,
                .misses = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.map.deinit();
        }

        pub fn get(self: *Self, key: []const u8) ?V {
            const entry = self.map.get(key) orelse {
                self.misses += 1;
                return null;
            };
            const now = nowNs();
            if (now > entry.expires_at) {
                self.misses += 1;
                return null;
            }
            self.hits += 1;
            return entry.value;
        }

        pub fn put(self: *Self, key: []const u8, value: V) void {
            const now = nowNs();
            self.map.put(key, .{
                .value = value,
                .expires_at = now + self.ttl_ns,
            }) catch {};
        }

        pub fn stats(self: *const Self) struct { hits: u64, misses: u64, size: usize } {
            return .{
                .hits = self.hits,
                .misses = self.misses,
                .size = self.map.count(),
            };
        }
    };
}

test "basic cache operations" {
    const allocator = std.testing.allocator;
    var c = ResultCache(i32).init(allocator, 60_000);
    defer c.deinit();

    try std.testing.expectEqual(@as(?i32, null), c.get("key1"));

    c.put("key1", 42);
    try std.testing.expectEqual(@as(?i32, 42), c.get("key1"));

    const s = c.stats();
    try std.testing.expectEqual(@as(u64, 1), s.hits);
    try std.testing.expectEqual(@as(u64, 1), s.misses);
}
