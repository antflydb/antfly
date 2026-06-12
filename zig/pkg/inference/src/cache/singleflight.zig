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

// Singleflight deduplication: concurrent-safe key-value store for
// tracking in-flight requests and caching results.

const std = @import("std");

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {
        std.atomic.spinLoopHint();
    }
}

pub fn Singleflight(comptime V: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        mu: std.atomic.Mutex,
        inflight: std.StringHashMapUnmanaged(V),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .mu = .unlocked,
                .inflight = std.StringHashMapUnmanaged(V){},
            };
        }

        pub fn deinit(self: *Self) void {
            self.inflight.deinit(self.allocator);
        }

        pub fn get(self: *Self, key: []const u8) ?V {
            spinLock(&self.mu);
            defer self.mu.unlock();
            return self.inflight.get(key);
        }

        pub fn put(self: *Self, key: []const u8, value: V) void {
            spinLock(&self.mu);
            defer self.mu.unlock();
            const owned_key = self.allocator.dupe(u8, key) catch return;
            self.inflight.put(self.allocator, owned_key, value) catch {
                self.allocator.free(owned_key);
            };
        }

        pub fn remove(self: *Self, key: []const u8) void {
            spinLock(&self.mu);
            defer self.mu.unlock();
            if (self.inflight.fetchRemove(key)) |kv| {
                self.allocator.free(kv.key);
            }
        }
    };
}

test "singleflight basic" {
    const allocator = std.testing.allocator;
    var sf = Singleflight(i32).init(allocator);
    defer sf.deinit();

    try std.testing.expectEqual(@as(?i32, null), sf.get("key1"));
    sf.put("key1", 42);
    try std.testing.expectEqual(@as(?i32, 42), sf.get("key1"));
    sf.remove("key1");
    try std.testing.expectEqual(@as(?i32, null), sf.get("key1"));
}
