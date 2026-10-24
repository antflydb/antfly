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

//! Process-local immutable read snapshots. Handles are random bearer
//! capabilities, never content hashes shared implicitly across callers. This
//! store does not execute models, retain source credentials, or resume jobs.
const std = @import("std");
const A = std.mem.Allocator;
pub const Charge = struct {
    ptr: ?*anyopaque = null,
    release_fn: ?*const fn (?*anyopaque) void = null,
    fn release(self: Charge) void {
        if (self.release_fn) |f| f(self.ptr);
    }
};
const Entry = struct { key: [64]u8, payload: []u8, expires_ns: u64, charge: Charge };
pub const Store = struct {
    entries: std.ArrayList(Entry) = .empty,
    bytes: usize = 0,
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 128,
    mutex: std.atomic.Mutex = .unlocked,

    fn lock(self: *Store) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn remove(self: *Store, a: A, i: usize) void {
        const entry = self.entries.orderedRemove(i);
        self.bytes -= entry.payload.len;
        a.free(entry.payload);
        entry.charge.release();
    }
    /// Charge ownership transfers only when true is returned.
    pub fn put(self: *Store, a: A, key: [64]u8, payload: []const u8, expires_ns: u64, charge: Charge) !bool {
        if (payload.len > self.max_bytes or self.max_entries == 0) return false;
        const copy = try a.dupe(u8, payload);
        var retained = false;
        defer if (!retained) a.free(copy);
        self.lock();
        defer self.mutex.unlock();
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &key)) return false;
        while (self.entries.items.len > 0 and (self.entries.items.len >= self.max_entries or payload.len > self.max_bytes -| self.bytes)) self.remove(a, 0);
        try self.entries.append(a, .{ .key = key, .payload = copy, .expires_ns = expires_ns, .charge = charge });
        self.bytes += copy.len;
        retained = true;
        return true;
    }
    pub fn get(self: *Store, store_allocator: A, output: A, key: []const u8, now_ns: u64) ![]u8 {
        if (key.len != 64) return error.InvalidDocumentArtifact;
        for (key) |b| if (!std.ascii.isHex(b)) return error.InvalidDocumentArtifact;
        self.lock();
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, i| if (std.mem.eql(u8, &entry.key, key)) {
            if (now_ns >= entry.expires_ns) {
                self.remove(store_allocator, i);
                return error.DocumentArtifactExpired;
            }
            return output.dupe(u8, entry.payload);
        };
        return error.DocumentArtifactNotFound;
    }
    pub fn deinit(self: *Store, a: A) void {
        while (self.entries.items.len > 0) self.remove(a, self.entries.items.len - 1);
        self.entries.deinit(a);
    }
};

test "document artifact retention is bounded and expired handles release their charge" {
    const Fake = struct {
        released: usize = 0,
        fn release(raw: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.released += 1;
        }
    };
    const a = std.testing.allocator;
    var store = Store{ .max_entries = 1, .max_bytes = 8 };
    defer store.deinit(a);
    var fake = Fake{};
    const charge = Charge{ .ptr = &fake, .release_fn = Fake.release };
    try std.testing.expect(try store.put(a, @splat('a'), "first", 10, charge));
    try std.testing.expect(try store.put(a, @splat('b'), "second", 20, charge));
    try std.testing.expectEqual(@as(usize, 1), fake.released);
    try std.testing.expectError(error.DocumentArtifactNotFound, store.get(a, a, &@as([64]u8, @splat('a')), 0));
    const bytes = try store.get(a, a, &@as([64]u8, @splat('b')), 19);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("second", bytes);
    try std.testing.expectError(error.DocumentArtifactExpired, store.get(a, a, &@as([64]u8, @splat('b')), 20));
    try std.testing.expectEqual(@as(usize, 2), fake.released);
    try std.testing.expectEqual(@as(usize, 0), store.bytes);
}
