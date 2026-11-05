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

//! Bounded immutable exchange bytes. Physical store ownership scopes the cache;
//! keys include the authenticated output identity and requested byte range.
const std = @import("std");
const A = std.mem.Allocator;
pub const Cache = struct {
    a: A,
    mutex: std.atomic.Mutex = .unlocked,
    entries: [64]?Entry = @splat(null),
    hand: usize = 0,
    const Entry = struct { key: [32]u8, bytes: []u8 };
    pub fn deinit(self: *Cache) void {
        for (&self.entries) |*slot| if (slot.*) |entry| {
            self.a.free(entry.bytes);
            slot.* = null;
        };
    }
    pub fn get(self: *Cache, a: A, key: [32]u8) !?[]u8 {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        for (self.entries) |slot| if (slot) |entry| {
            if (std.mem.eql(u8, &entry.key, &key)) return try a.dupe(u8, entry.bytes);
        };
        return null;
    }
    pub fn put(self: *Cache, key: [32]u8, bytes: []const u8) void {
        // Fixed entry and byte caps bound this worker to at most 8 MiB.
        if (bytes.len > 128 * 1024 or !self.mutex.tryLock()) return;
        defer self.mutex.unlock();
        for (self.entries) |slot| if (slot) |entry| {
            if (std.mem.eql(u8, &entry.key, &key)) return;
        };
        const owned = self.a.dupe(u8, bytes) catch return;
        const slot = &self.entries[self.hand];
        if (slot.*) |entry| self.a.free(entry.bytes);
        slot.* = .{ .key = key, .bytes = owned };
        self.hand = (self.hand + 1) % self.entries.len;
    }
};
