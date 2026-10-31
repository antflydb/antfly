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

//! Owned point working set for an exclusively sequenced serving shard.
//! A cut is supplied only by the host after owner admission and commit ordering.
//! Entries never grant authority; callers must validate their serving lifetime.
const std = @import("std");
const A = std.mem.Allocator;
const Token = @import("../visibility.zig").Token;
pub const Shard = struct {
    pub const max_bytes = 256 * 1024;
    pub const max_entries = 128;
    pub const Value = struct {
        body: ?[]u8,
        lsn: u64,
        pub fn deinit(self: Value, a: A) void {
            if (self.body) |body| a.free(body);
        }
    };
    const Entry = struct { domain: [32]u8, cut: [32]u8, id: []u8, value: Value };
    mu: std.atomic.Mutex = .unlocked,
    entries: [max_entries]?Entry = @splat(null),
    bytes: usize = 0,
    next: usize = 0,
    hits: u64 = 0,
    fn size(entry: Entry) usize {
        return @sizeOf(Entry) + entry.id.len + if (entry.value.body) |b| b.len else @as(usize, 0);
    }
    fn remove(self: *Shard, a: A, i: usize) void {
        if (self.entries[i]) |entry| {
            self.bytes -= size(entry);
            a.free(entry.id);
            entry.value.deinit(a);
            self.entries[i] = null;
        }
    }
    pub fn deinit(self: *Shard, a: A) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        for (0..max_entries) |i| self.remove(a, i);
    }
    pub fn get(self: *Shard, a: A, namespace: []const u8, cut: [32]u8, id: []const u8, required: ?Token) !?Value {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        const domain = Token.init(namespace, 0).scope;
        for (self.entries) |slot| if (slot) |entry| {
            if (std.mem.eql(u8, &entry.domain, &domain) and std.mem.eql(u8, &entry.cut, &cut) and std.mem.eql(u8, entry.id, id)) {
                if (required) |token| try token.require(namespace, entry.value.lsn);
                const value = Value{ .lsn = entry.value.lsn, .body = if (entry.value.body) |b| try a.dupe(u8, b) else null };
                self.hits +|= 1;
                return value;
            }
        };
        return null;
    }
    pub fn put(self: *Shard, a: A, namespace: []const u8, cut: [32]u8, id: []const u8, body: ?[]const u8, lsn: u64) !bool {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        if (id.len > 1024 or @sizeOf(Entry) + id.len + (if (body) |b| b.len else @as(usize, 0)) > max_bytes) return false;
        const owned_id = try a.dupe(u8, id);
        errdefer a.free(owned_id);
        const owned_body = if (body) |b| try a.dupe(u8, b) else null;
        const entry = Entry{ .domain = Token.init(namespace, 0).scope, .cut = cut, .id = owned_id, .value = .{ .body = owned_body, .lsn = lsn } };
        for (self.entries, 0..) |slot, i| if (slot) |old| {
            if (std.mem.eql(u8, &old.domain, &entry.domain) and std.mem.eql(u8, &old.cut, &cut) and std.mem.eql(u8, old.id, id)) self.remove(a, i);
        };
        self.remove(a, self.next);
        while (self.bytes + size(entry) > max_bytes) {
            self.next = (self.next + 1) % max_entries;
            self.remove(a, self.next);
        }
        self.entries[self.next] = entry;
        self.bytes += size(entry);
        self.next = (self.next + 1) % max_entries;
        return true;
    }
};

test "resident serving shard owns positive and negative results and binds exact cuts namespaces and receipts" {
    const a = std.testing.allocator;
    var shard: Shard = .{};
    defer shard.deinit(a);
    var body = [_]u8{ 'o', 'k' };
    try std.testing.expect(try shard.put(a, "a", @splat(1), "one", &body, 7));
    body[0] = 'x';
    const value = (try shard.get(a, "a", @splat(1), "one", Token.init("a", 7))).?;
    defer value.deinit(a);
    try std.testing.expectEqualStrings("ok", value.body.?);
    try std.testing.expect((try shard.get(a, "a", @splat(2), "one", null)) == null);
    try std.testing.expect((try shard.get(a, "b", @splat(1), "one", null)) == null);
    try std.testing.expectError(error.WalVisibilityUnavailable, shard.get(a, "a", @splat(1), "one", Token.init("a", 8)));
    try std.testing.expectError(error.VisibilityIncarnationMismatch, shard.get(a, "a", @splat(1), "one", Token.init("b", 1)));
    try std.testing.expect(try shard.put(a, "a", @splat(2), "one", null, 8));
    const absent = (try shard.get(a, "a", @splat(2), "one", null)).?;
    defer absent.deinit(a);
    try std.testing.expect(absent.body == null);
}
fn allocationFailure(a: A) !void {
    var shard: Shard = .{};
    defer shard.deinit(a);
    _ = try shard.put(a, "a", @splat(1), "one", "body", 1);
    const value = (try shard.get(a, "a", @splat(1), "one", null)).?;
    defer value.deinit(a);
    _ = try shard.put(a, "a", @splat(1), "one", "replacement", 2);
}
test "resident serving shard allocation failures release owned state" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{});
}
test "resident serving shard byte and entry budgets evict without changing absence semantics" {
    const a = std.testing.allocator;
    var shard: Shard = .{};
    defer shard.deinit(a);
    const body = try a.alloc(u8, 10000);
    defer a.free(body);
    @memset(body, 'x');
    for (0..256) |i| {
        var cut: [32]u8 = @splat(0);
        std.mem.writeInt(u64, cut[0..8], @intCast(i), .little);
        _ = try shard.put(a, "a", cut, "one", body, @intCast(i));
        try std.testing.expect(shard.bytes <= Shard.max_bytes);
    }
    try std.testing.expect((try shard.get(a, "a", @splat(0), "one", null)) == null);
}
