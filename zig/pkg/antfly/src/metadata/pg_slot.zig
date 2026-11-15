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

//! Durable logical-slot ordering and fenced feedback. This is private metadata
//! state, not a replication authorization boundary. The provider must retain or
//! durably spool the certified, complete projected transaction before issue.
//! Its byte serialization is immutable across reconnect and ownership changes.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const Id = [16]u8;
pub const max_entries = 128;
pub const max_pending_bytes: u64 = 1024 * 1024 * 1024;
pub const max_session_seconds: u64 = 60;
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn equal(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub fn validName(name: []const u8) bool {
    if (name.len == 0 or name.len > 63) return false;
    for (name) |c| if (!(c >= 'a' and c <= 'z') and !(c >= '0' and c <= '9') and c != '_') return false;
    return true;
}
pub fn feedId(database: Digest, name: []const u8) Id {
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    sha.update("antfly:pg-logical-slot:v1");
    sha.update(&database);
    sha.update(name);
    return sha.finalResult()[0..16].*;
}
pub const Mutation = union(enum) {
    configure: struct { database: Digest, name: []const u8, byte_limit: u64 = max_pending_bytes },
    acquire: struct { actor: Id, expected: u64, now: u64, expires: u64 },
    release: struct { session: Session, now: u64 },
    issue: struct { session: Session, now: u64, transaction: Prepared },
    confirm: struct { session: Session, now: u64, flushed: u64 },
    pub fn validate(self: Mutation) !void {
        switch (self) {
            .configure => |v| {
                if (!nz(v.database) or !validName(v.name) or v.byte_limit < 47 or v.byte_limit > max_pending_bytes) return error.InvalidChangefeed;
            },
            .acquire => |v| if (!nz(v.actor) or v.now == 0 or v.expires <= v.now or v.expires - v.now > max_session_seconds) return error.InvalidChangefeed,
            .issue => |v| {
                try v.transaction.validate();
                try validSession(v.session, v.now);
            },
            .release => |v| try validSession(v.session, v.now),
            .confirm => |v| try validSession(v.session, v.now),
        }
    }
};
fn validSession(session: Session, now: u64) !void {
    if (!nz(session.actor) or session.fence == 0 or now == 0) return error.InvalidChangefeed;
}
pub const Session = struct { actor: Id, fence: u64 };
pub const Prepared = struct {
    before: Digest,
    after: Digest,
    content: Digest,
    /// Trusted completion proof from the retained-history coordinator. A hash
    /// supplied by an external client never constitutes this authority.
    completion: Digest,
    bytes: u64,
    /// Offset of the C message within the immutable serialized transaction.
    commit_offset: u64,
    xid: u32,
    commit_time: i64,
    pub fn validate(self: Prepared) !void {
        // B is 21 bytes and C is 26. Relation/data messages lie between them.
        if (!nz(self.before) or !nz(self.after) or equal(self.before, self.after) or !nz(self.content) or !nz(self.completion) or self.xid == 0 or self.bytes > max_pending_bytes or self.bytes < 47 or self.commit_offset != self.bytes - 26) return error.InvalidChangefeed;
    }
};
pub const Entry = struct {
    transaction: Prepared,
    start_lsn: u64,
    commit_lsn: u64,
    end_lsn: u64,
    pub fn validate(self: Entry) !void {
        try self.transaction.validate();
        if (self.start_lsn == 0 or self.commit_lsn != (std.math.add(u64, self.start_lsn, self.transaction.commit_offset) catch return error.InvalidChangefeed) or self.end_lsn != (std.math.add(u64, self.start_lsn, self.transaction.bytes) catch return error.InvalidChangefeed)) return error.InvalidChangefeed;
    }
};
pub const State = struct {
    database: Digest,
    name: []const u8,
    /// This is an independent, slot-scoped virtual byte ordering domain.
    /// Native history positions are correlated through before/after cursors.
    consistent_lsn: u64 = 1,
    next_lsn: u64 = 1,
    confirmed_lsn: u64 = 1,
    acknowledged_cursor: Digest,
    mapped_cursor: Digest,
    byte_limit: u64 = max_pending_bytes,
    pending_bytes: u64 = 0,
    entries: []const Entry = &.{},
    session: ?Session = null,
    fence: u64 = 0,
    session_expires_at: u64 = 0,
    observed_at: u64 = 0,
    pub fn validate(self: State) !void {
        if (!nz(self.database) or !validName(self.name) or self.consistent_lsn != 1 or self.confirmed_lsn < self.consistent_lsn or self.next_lsn < self.confirmed_lsn or !nz(self.acknowledged_cursor) or !nz(self.mapped_cursor) or self.byte_limit < 47 or self.byte_limit > max_pending_bytes or self.entries.len > max_entries or self.pending_bytes > self.byte_limit) return error.InvalidChangefeed;
        if ((self.fence == 0) != (self.observed_at == 0)) return error.InvalidChangefeed;
        if (self.session) |session| {
            if (!nz(session.actor) or session.fence == 0 or session.fence != self.fence or self.session_expires_at <= self.observed_at) return error.InvalidChangefeed;
        } else if (self.session_expires_at != 0) return error.InvalidChangefeed;
        var position = self.confirmed_lsn;
        var cursor = self.acknowledged_cursor;
        var count: u64 = 0;
        for (self.entries) |entry| {
            try entry.validate();
            if (entry.start_lsn != position or !equal(entry.transaction.before, cursor)) return error.InvalidChangefeed;
            position = entry.end_lsn;
            cursor = entry.transaction.after;
            count = std.math.add(u64, count, entry.transaction.bytes) catch return error.InvalidChangefeed;
        }
        if (position != self.next_lsn or !equal(cursor, self.mapped_cursor) or count != self.pending_bytes) return error.InvalidChangefeed;
    }
    pub fn initialize(database: Digest, name: []const u8, cursor: Digest, byte_limit: u64) !State {
        const value: State = .{ .database = database, .name = name, .acknowledged_cursor = cursor, .mapped_cursor = cursor, .byte_limit = byte_limit };
        try value.validate();
        return value;
    }
    pub fn acquire(self: State, actor: Id, expected: u64, now: u64, expires: u64) !State {
        try self.validate();
        if (!nz(actor) or now == 0 or now < self.observed_at or expires <= now or expires - now > max_session_seconds) return error.InvalidChangefeed;
        var next = self;
        if (self.session) |session| {
            if (now < self.session_expires_at) {
                if (!equal(actor, session.actor)) return error.ChangefeedConflict;
                // Replay the original acquire after a lost response, or renew
                // an established session without replacing its fencing token.
                if (expected != self.fence and !(expected +| 1 == self.fence and expires == self.session_expires_at)) return error.ChangefeedConflict;
                if (expires < self.session_expires_at) return error.ChangefeedConflict;
                next.session_expires_at = expires;
                next.observed_at = now;
                return next;
            }
        }
        if (expected != self.fence) return error.ChangefeedConflict;
        next.fence = std.math.add(u64, self.fence, 1) catch return error.ChangefeedConflict;
        next.session = .{ .actor = actor, .fence = next.fence };
        next.session_expires_at = expires;
        next.observed_at = now;
        return next;
    }
    fn check(self: State, session: Session, now: u64) !void {
        const current = self.session orelse return error.ChangefeedConflict;
        if (!equal(current.actor, session.actor) or current.fence != session.fence) return error.ChangefeedConflict;
        if (now == 0 or now < self.observed_at or now >= self.session_expires_at) return error.ChangefeedLeaseExpired;
    }
    pub fn release(self: State, session: Session, now: u64) !State {
        try self.validate();
        try self.check(session, now);
        var next = self;
        next.session = null;
        next.session_expires_at = 0;
        next.observed_at = now;
        return next;
    }
    pub fn issue(self: State, a: A, session: Session, now: u64, transaction: Prepared) !State {
        try self.validate();
        try self.check(session, now);
        try transaction.validate();
        for (self.entries) |entry| if (equal(entry.transaction.after, transaction.after)) {
            if (!std.meta.eql(entry.transaction, transaction)) return error.ChangefeedConflict;
            return self;
        };
        if (!equal(transaction.before, self.mapped_cursor)) return error.ChangefeedConflict;
        if (self.entries.len == max_entries or transaction.bytes > self.byte_limit - self.pending_bytes) return error.ChangefeedBackpressure;
        const entry: Entry = .{ .transaction = transaction, .start_lsn = self.next_lsn, .commit_lsn = std.math.add(u64, self.next_lsn, transaction.commit_offset) catch return error.ChangefeedBackpressure, .end_lsn = std.math.add(u64, self.next_lsn, transaction.bytes) catch return error.ChangefeedBackpressure };
        var next = self;
        const entries = try a.alloc(Entry, self.entries.len + 1);
        @memcpy(entries[0..self.entries.len], self.entries);
        entries[self.entries.len] = entry;
        next.entries = entries;
        next.next_lsn = entry.end_lsn;
        next.mapped_cursor = transaction.after;
        next.pending_bytes += transaction.bytes;
        next.observed_at = now;
        return next;
    }
    /// Advance only to the end of a complete issued transaction. An interior
    /// flush position never releases the remainder of that transaction.
    pub fn confirm(self: State, a: A, session: Session, now: u64, flushed: u64) !State {
        try self.validate();
        try self.check(session, now);
        if (flushed > self.next_lsn) return error.ChangefeedConflict;
        var count: usize = 0;
        for (self.entries) |entry| {
            if (entry.end_lsn > flushed) break;
            count += 1;
        }
        if (count == 0) return self;
        var next = self;
        const last = self.entries[count - 1];
        next.confirmed_lsn = last.end_lsn;
        next.acknowledged_cursor = last.transaction.after;
        for (self.entries[0..count]) |entry| next.pending_bytes -= entry.transaction.bytes;
        next.entries = try a.dupe(Entry, self.entries[count..]);
        next.observed_at = now;
        return next;
    }
    /// Restart an interior requested LSN at that transaction's Begin. This is
    /// at-least-once replay; a requested position does not acknowledge history.
    pub fn restartAt(self: State, requested: u64) !u64 {
        try self.validate();
        if (requested == 0) return self.confirmed_lsn;
        if (requested < self.confirmed_lsn) return error.ChangefeedResynchronizationRequired;
        if (requested > self.next_lsn) return error.ChangefeedConflict;
        for (self.entries) |entry| if (requested < entry.end_lsn) return entry.start_lsn;
        return self.next_lsn;
    }
};

fn testTransaction(before: u8, after: u8, bytes: u64) Prepared {
    return .{ .before = @splat(before), .after = @splat(after), .content = @splat(3), .completion = @splat(4), .bytes = bytes, .commit_offset = bytes - 26, .xid = 42, .commit_time = -1 };
}
test "logical slot maps complete transactions and confirms only issued boundaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot_1", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    state = try state.issue(a, session, 101, testTransaction(1, 2, 100));
    state = try state.issue(a, session, 102, testTransaction(2, 3, 200));
    try std.testing.expectEqual(@as(u64, 301), state.next_lsn);
    try std.testing.expectEqual(@as(u64, 75), state.entries[0].commit_lsn);
    try std.testing.expectEqual(@as(u64, 1), try state.restartAt(100));
    try std.testing.expectEqual(@as(u64, 101), try state.restartAt(200));
    const partial = try state.confirm(a, session, 103, 100);
    try std.testing.expectEqual(@as(u64, 1), partial.confirmed_lsn);
    state = try state.confirm(a, session, 104, 200);
    try std.testing.expectEqual(@as(u64, 101), state.confirmed_lsn);
    try std.testing.expectEqual(@as(u64, 200), state.pending_bytes);
    try std.testing.expectEqualSlices(u8, &@as(Digest, @splat(2)), &state.acknowledged_cursor);
    try std.testing.expectError(error.ChangefeedConflict, state.confirm(a, session, 104, 302));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, state.restartAt(100));
    state = try state.confirm(a, session, 105, 301);
    try state.validate();
    try std.testing.expectEqual(@as(usize, 0), state.entries.len);
    try std.testing.expectEqual(@as(u64, 0), state.pending_bytes);
}
test "logical slot sessions fence old owners and preserve replay identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const first = state.session.?;
    const replayed = try state.acquire(@splat(2), 0, 101, 150);
    try std.testing.expectEqual(first.fence, replayed.fence);
    try std.testing.expectError(error.ChangefeedConflict, state.acquire(@splat(3), 1, 101, 151));
    state = try state.issue(a, first, 102, testTransaction(1, 2, 100));
    state = try state.acquire(@splat(3), 1, 150, 200);
    try std.testing.expectError(error.ChangefeedConflict, state.confirm(a, first, 151, 101));
    try std.testing.expectError(error.ChangefeedConflict, state.release(first, 151));
    const retry = try state.issue(a, state.session.?, 151, testTransaction(1, 2, 100));
    try std.testing.expectEqual(state.next_lsn, retry.next_lsn);
    var changed = testTransaction(1, 2, 100);
    changed.content = @splat(5);
    try std.testing.expectError(error.ChangefeedConflict, state.issue(a, state.session.?, 151, changed));
    try std.testing.expectError(error.ChangefeedLeaseExpired, state.issue(a, state.session.?, 200, testTransaction(2, 3, 47)));
    try std.testing.expectError(error.InvalidChangefeed, state.acquire(@splat(3), 2, 149, 201));
}
test "logical slot bounded lag never silently drops an unconfirmed transaction" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 100);
    state = try state.acquire(@splat(2), 0, 100, 150);
    state = try state.issue(a, state.session.?, 101, testTransaction(1, 2, 100));
    try std.testing.expectError(error.ChangefeedBackpressure, state.issue(a, state.session.?, 102, testTransaction(2, 3, 47)));
    var corrupt = state;
    corrupt.next_lsn += 1;
    try std.testing.expectError(error.InvalidChangefeed, corrupt.validate());
    try std.testing.expect(!validName("Upper"));
    try std.testing.expect(!validName("a.b"));
}

test "logical slot cursor quota allows retry and reuse after complete confirmation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 65536);
    state = try state.acquire(@splat(2), 0, 100, 150);
    for (0..max_entries) |index| state = try state.issue(a, state.session.?, 101, testTransaction(@intCast(index + 1), @intCast(index + 2), 47));
    try std.testing.expectEqual(@as(usize, max_entries), state.entries.len);
    const retry = try state.issue(a, state.session.?, 102, state.entries[max_entries - 1].transaction);
    try std.testing.expectEqual(state.next_lsn, retry.next_lsn);
    try std.testing.expectError(error.ChangefeedBackpressure, state.issue(a, state.session.?, 102, testTransaction(129, 130, 47)));
    state = try state.confirm(a, state.session.?, 103, state.entries[0].end_lsn);
    state = try state.issue(a, state.session.?, 104, testTransaction(129, 130, 47));
    try state.validate();
    try std.testing.expectEqual(@as(usize, max_entries), state.entries.len);
}
fn allocationProbe(a: A) !void {
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    state = try state.issue(a, state.session.?, 101, testTransaction(1, 2, 100));
    defer a.free(state.entries);
    const second = try state.issue(a, state.session.?, 102, testTransaction(2, 3, 100));
    defer a.free(second.entries);
    const confirmed = try second.confirm(a, second.session.?, 103, 101);
    defer a.free(confirmed.entries);
    try confirmed.validate();
}
test "logical slot allocation failures leave the original mapping owned and intact" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}
