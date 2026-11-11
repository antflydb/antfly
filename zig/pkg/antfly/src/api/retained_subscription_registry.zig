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

//! Bounded server-local subscription state and authenticated resume cursors.
//! Registry locks cover only bounded memory operations. Native replay and
//! archive reads run outside the lock and commit with an output sequence CAS.
const std = @import("std");
const A = std.mem.Allocator;
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;
const encoding = std.base64.url_safe_no_pad;
pub const Id = [16]u8;
pub const Digest = [32]u8;
pub const max_state_bytes = 8 * 1024 * 1024;
pub const max_reply_bytes = 4 * 1024 * 1024;
const max_total_bytes = 64 * 1024 * 1024;
const max_sessions = 64;
const idle_ns = 2 * std.time.ns_per_min;
const payload_len = 1 + 16 + 8 + 32;
const token_bytes = payload_len + Hmac.mac_length;
pub const token_length = encoding.Encoder.calcSize(token_bytes);

pub const Position = struct {
    id: Id,
    sequence: u64,
    binding: Digest,
};

pub fn token(secret: []const u8, position: Position) [token_length]u8 {
    var bytes: [token_bytes]u8 = undefined;
    bytes[0] = 1;
    bytes[1..17].* = position.id;
    std.mem.writeInt(u64, bytes[17..25], position.sequence, .big);
    bytes[25..payload_len].* = position.binding;
    const key = signingKey(secret);
    Hmac.create(bytes[payload_len..], bytes[0..payload_len], &key);
    var encoded: [token_length]u8 = undefined;
    _ = encoding.Encoder.encode(&encoded, &bytes);
    return encoded;
}
pub fn verify(secret: []const u8, raw: []const u8) !Position {
    if (raw.len != token_length) return error.InvalidRetainedSubscriptionToken;
    var bytes: [token_bytes]u8 = undefined;
    encoding.Decoder.decode(&bytes, raw) catch return error.InvalidRetainedSubscriptionToken;
    if (bytes[0] != 1) return error.InvalidRetainedSubscriptionToken;
    const key = signingKey(secret);
    var actual: [Hmac.mac_length]u8 = undefined;
    Hmac.create(&actual, bytes[0..payload_len], &key);
    if (!std.crypto.timing_safe.eql([Hmac.mac_length]u8, actual, bytes[payload_len..].*)) return error.InvalidRetainedSubscriptionToken;
    return .{ .id = bytes[1..17].*, .sequence = std.mem.readInt(u64, bytes[17..25], .big), .binding = bytes[25..payload_len].* };
}
fn signingKey(secret: []const u8) Digest {
    var key: Digest = undefined;
    Hmac.create(&key, "antfly/retained-subscription/v1", secret);
    return key;
}

pub const Registry = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayList(Entry) = .empty,
    bytes: usize = 0,
    const Entry = struct {
        position: Position,
        principal: Digest,
        expires_ns: u64,
        state: []u8,
        reply: []u8 = &.{},
    };
    pub const Loaded = union(enum) { state: []u8, retry_reply: []u8 };

    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.Thread.yield() catch std.atomic.spinLoopHint();
    }
    fn find(self: *Registry, id: Id) ?*Entry {
        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, &entry.position.id, &id)) return entry;
        }
        return null;
    }
    fn remove(self: *Registry, alloc: A, index: usize) void {
        const entry = self.entries.swapRemove(index);
        self.bytes -= entry.state.len + entry.reply.len;
        alloc.free(entry.state);
        alloc.free(entry.reply);
    }
    fn sweep(self: *Registry, alloc: A, now: u64) void {
        var index: usize = 0;
        while (index < self.entries.items.len) {
            if (now >= self.entries.items[index].expires_ns) self.remove(alloc, index) else index += 1;
        }
    }
    pub fn expire(self: *Registry, alloc: A, now: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.sweep(alloc, now);
    }
    pub fn deinit(self: *Registry, alloc: A) void {
        self.lock();
        defer self.mutex.unlock();
        while (self.entries.items.len != 0) self.remove(alloc, self.entries.items.len - 1);
        self.entries.deinit(alloc);
    }
    fn requireBinding(entry: *const Entry, position: Position, principal: Digest) !void {
        if (!std.mem.eql(u8, &entry.position.binding, &position.binding) or !std.mem.eql(u8, &entry.principal, &principal)) return error.Forbidden;
    }
    pub fn create(self: *Registry, alloc: A, position: Position, principal: Digest, now: u64, state: []const u8) !void {
        if (position.sequence != 0 or std.mem.allEqual(u8, &position.id, 0) or state.len > max_state_bytes) return error.DatasetPageBudgetExceeded;
        self.lock();
        defer self.mutex.unlock();
        self.sweep(alloc, now);
        if (self.entries.items.len >= max_sessions or state.len > max_total_bytes - self.bytes or self.find(position.id) != null) return error.DatasetReaderBudgetExceeded;
        const owned = try alloc.dupe(u8, state);
        errdefer alloc.free(owned);
        try self.entries.append(alloc, .{ .position = position, .principal = principal, .expires_ns = now +| idle_ns, .state = owned });
        self.bytes += owned.len;
    }
    pub fn load(self: *Registry, alloc: A, output: A, position: Position, principal: Digest, now: u64) !Loaded {
        self.lock();
        defer self.mutex.unlock();
        self.sweep(alloc, now);
        const entry = self.find(position.id) orelse return error.RetainedSeriesResetRequired;
        try requireBinding(entry, position, principal);
        if (position.sequence == entry.position.sequence) return .{ .state = try output.dupe(u8, entry.state) };
        if (position.sequence +| 1 == entry.position.sequence and entry.reply.len != 0) return .{ .retry_reply = try output.dupe(u8, entry.reply) };
        return error.RetainedSeriesResetRequired;
    }
    /// A concurrent replay of the same input returns the already committed
    /// response. A failed or canceled poll leaves the prior cursor valid.
    pub fn commit(self: *Registry, alloc: A, output: A, position: Position, principal: Digest, now: u64, state: []const u8, reply: []const u8) ![]u8 {
        if (state.len > max_state_bytes or reply.len > max_reply_bytes or position.sequence == std.math.maxInt(u64)) return error.DatasetPageBudgetExceeded;
        self.lock();
        defer self.mutex.unlock();
        self.sweep(alloc, now);
        const entry = self.find(position.id) orelse return error.RetainedSeriesResetRequired;
        try requireBinding(entry, position, principal);
        if (entry.position.sequence == position.sequence + 1 and entry.reply.len != 0) return output.dupe(u8, entry.reply);
        if (entry.position.sequence != position.sequence) return error.RetainedSeriesResetRequired;
        const occupied = self.bytes - entry.state.len - entry.reply.len;
        if (state.len + reply.len > max_total_bytes - occupied) return error.DatasetReaderBudgetExceeded;
        const owned = try alloc.dupe(u8, state);
        errdefer alloc.free(owned);
        const cached = try alloc.dupe(u8, reply);
        errdefer alloc.free(cached);
        const response = try output.dupe(u8, reply);
        alloc.free(entry.state);
        alloc.free(entry.reply);
        entry.state = owned;
        entry.reply = cached;
        entry.position.sequence += 1;
        entry.expires_ns = now +| idle_ns;
        self.bytes = occupied + owned.len + cached.len;
        return response;
    }
    pub fn cancel(self: *Registry, alloc: A, position: Position, principal: Digest) !void {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |*entry, index| {
            if (!std.mem.eql(u8, &entry.position.id, &position.id)) continue;
            try requireBinding(entry, position, principal);
            self.remove(alloc, index);
            return;
        }
    }
};

test "retained subscription retries return the exact committed response and reject stale or foreign cursors" {
    const a = std.testing.allocator;
    const principal: Digest = @splat(3);
    const initial: Position = .{ .id = @splat(1), .sequence = 0, .binding = @splat(2) };
    var registry: Registry = .{};
    defer registry.deinit(a);
    const encoded = token("test-secret", initial);
    try std.testing.expectEqualDeep(initial, try verify("test-secret", &encoded));
    try std.testing.expectError(error.InvalidRetainedSubscriptionToken, verify("other-secret", &encoded));
    try registry.create(a, initial, principal, 1, "snapshot");
    const committed = try registry.commit(a, a, initial, principal, 2, "next", "first reply");
    defer a.free(committed);
    const retry = try registry.commit(a, a, initial, principal, 3, "wrong", "wrong");
    defer a.free(retry);
    try std.testing.expectEqualStrings("first reply", retry);
    try std.testing.expectError(error.Forbidden, registry.load(a, a, initial, @splat(4), 4));
    const after = try registry.load(a, a, initial, principal, 4);
    defer a.free(after.retry_reply);
    try std.testing.expectEqualStrings(committed, after.retry_reply);
    try std.testing.expectError(error.RetainedSeriesResetRequired, registry.load(a, a, initial, principal, idle_ns + 4));
}
