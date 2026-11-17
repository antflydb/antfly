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
pub const spool_control = @import("pg_spool_control.zig");
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
pub const Binding = struct {
    publications: []const []const u8,
    projection: Digest,
    pub fn validate(self: Binding) !void {
        if (self.publications.len == 0 or self.publications.len > 64 or !nz(self.projection)) return error.InvalidChangefeed;
        for (self.publications, 0..) |name, index| {
            if (name.len == 0 or name.len > 63 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidChangefeed;
            if (index != 0 and std.mem.order(u8, self.publications[index - 1], name) != .lt) return error.InvalidChangefeed;
        }
    }
    pub fn identity(self: Binding) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:pg-slot-publication-binding:v1:pgoutput-text-1");
        sha.update(&self.projection);
        for (self.publications) |name| {
            const length: [1]u8 = .{@intCast(name.len)};
            sha.update(&length);
            sha.update(name);
        }
        return sha.finalResult();
    }
};
pub fn bindingAlloc(a: A, names: []const []const u8, projection: Digest) !Binding {
    if (names.len == 0 or names.len > 64) return error.InvalidChangefeed;
    const owned = try a.dupe([]const u8, names);
    errdefer a.free(owned);
    std.mem.sort([]const u8, owned, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.less);
    const value: Binding = .{ .publications = owned, .projection = projection };
    try value.validate();
    return value;
}
pub const Configuration = struct {
    database: Digest,
    name: []const u8,
    byte_limit: u64 = max_pending_bytes,
    consistent_lsn: u64 = 1,
    binding: ?Binding = null,

    pub fn validate(self: Configuration) !void {
        if (!nz(self.database) or !validName(self.name) or self.consistent_lsn == 0 or self.byte_limit < 47 or self.byte_limit > max_pending_bytes) return error.InvalidChangefeed;
        if (self.binding) |binding| try binding.validate();
    }

    pub fn matchesBinding(self: Configuration, existing: ?Binding) bool {
        const expected = self.binding orelse return true;
        const actual = existing orelse return false;
        return std.meta.eql(expected.identity(), actual.identity());
    }
};

pub const Mutation = union(enum) {
    configure: Configuration,
    drop: struct { database: Digest, name: []const u8, now: u64 },
    acquire: struct { actor: Id, expected: u64, now: u64, expires: u64 },
    release: struct { session: Session, now: u64 },
    issue: struct { session: Session, now: u64, transaction: Prepared },
    confirm: struct { session: Session, now: u64, flushed: u64 },
    bind: struct { session: Session, now: u64, binding: Binding },
    feedback: struct { session: Session, now: u64, expires: u64, flushed: u64 },
    reserve_spool: struct { session: Session, now: u64, id: u64, scope: spool_control.Scope, spec: spool_control.Spec },
    begin_spool: struct { session: Session, now: u64, id: u64, token: Id },
    publish_spool: struct { session: Session, now: u64, id: u64, manifest: Digest, content: Digest },
    abandon_spool: struct { session: Session, now: u64, id: u64 },
    reclaim_spool: struct { id: u64, storage: Digest, proof: Digest },
    checkpoint_spool_cleanup: struct { id: u64, storage: Digest, expected: u64, progress: spool_control.Cleanup },
    pub fn validate(self: Mutation) !void {
        switch (self) {
            .configure => |configuration| try configuration.validate(),
            .drop => |drop| if (!nz(drop.database) or !validName(drop.name) or drop.now == 0) return error.InvalidChangefeed,
            .acquire => |v| if (!nz(v.actor) or v.now == 0 or v.expires <= v.now or v.expires - v.now > max_session_seconds) return error.InvalidChangefeed,
            .issue => |v| {
                try v.transaction.validate();
                try validSession(v.session, v.now);
            },
            .reserve_spool => |v| {
                try validSession(v.session, v.now);
                try v.scope.validate();
                try v.spec.validate();
                if (v.id == 0) return error.InvalidChangefeed;
            },
            .begin_spool => |v| {
                try validSession(v.session, v.now);
                if (v.id == 0 or !nz(v.token)) return error.InvalidChangefeed;
            },
            .publish_spool => |v| {
                try validSession(v.session, v.now);
                if (v.id == 0 or !nz(v.manifest) or !nz(v.content)) return error.InvalidChangefeed;
            },
            .abandon_spool => |v| {
                try validSession(v.session, v.now);
                if (v.id == 0) return error.InvalidChangefeed;
            },
            .checkpoint_spool_cleanup => |v| {
                if (v.id == 0 or !nz(v.storage)) return error.InvalidChangefeed;
                try v.progress.validate();
            },
            .reclaim_spool => |v| if (v.id == 0 or !nz(v.storage) or !nz(v.proof)) return error.InvalidChangefeed,
            .bind => |v| {
                try validSession(v.session, v.now);
                try v.binding.validate();
            },
            .feedback => |v| {
                try validSession(v.session, v.now);
                if (v.expires <= v.now or v.expires - v.now > max_session_seconds) return error.InvalidChangefeed;
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
    spool_id: u64 = 0,
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
    spool: spool_control.Ledger = .{},
    spool_epoch: u64 = 0,
    spool_retired: bool = false,
    binding: ?Binding = null,
    pub fn validate(self: State) !void {
        if (!nz(self.database) or !validName(self.name) or self.consistent_lsn == 0 or self.confirmed_lsn < self.consistent_lsn or self.next_lsn < self.confirmed_lsn or !nz(self.acknowledged_cursor) or !nz(self.mapped_cursor) or self.byte_limit < 47 or self.byte_limit > max_pending_bytes or self.entries.len > max_entries or self.pending_bytes > self.byte_limit) return error.InvalidChangefeed;
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
        try self.spool.validate();
        if (self.binding) |bound| {
            try bound.validate();
            if (self.spool.projection) |projection| if (!equal(bound.projection, projection)) return error.InvalidChangefeed;
        }
        if ((self.spool.next_id == 1) != (self.spool_epoch == 0) or (self.spool_retired and self.session != null)) return error.InvalidChangefeed;
        for (self.spool.records) |record| {
            if (!equal(record.spec.feed, feedId(self.database, self.name)) or record.spec.epoch != self.spool_epoch) return error.InvalidChangefeed;
            if (self.spool_retired) {
                if (record.phase != .garbage) return error.InvalidChangefeed;
                continue;
            }
            if (record.phase == .uploading and (record.spec.start_lsn != self.next_lsn or !equal(record.spec.before, self.mapped_cursor))) return error.InvalidChangefeed;
            if (record.phase == .live) {
                var found = false;
                for (self.entries) |entry| if (entry.spool_id == record.id) {
                    found = true;
                };
                if (!found) return error.InvalidChangefeed;
            }
        }
        if (!self.spool_retired) for (self.entries) |entry| {
            if (entry.spool_id == 0) {
                if (self.spool.next_id != 1) return error.InvalidChangefeed;
            } else {
                const record = self.spool.find(entry.spool_id) orelse return error.InvalidChangefeed;
                if (record.phase != .live or record.spec.start_lsn != entry.start_lsn or !std.meta.eql(entry.transaction, prepared(record.spec, record.content))) return error.InvalidChangefeed;
            }
        };
    }
    pub fn initialize(database: Digest, name: []const u8, cursor: Digest, byte_limit: u64) !State {
        return initializeAt(database, name, cursor, byte_limit, 1);
    }

    pub fn initializeAt(database: Digest, name: []const u8, cursor: Digest, byte_limit: u64, consistent_lsn: u64) !State {
        const value: State = .{
            .database = database,
            .name = name,
            .consistent_lsn = consistent_lsn,
            .next_lsn = consistent_lsn,
            .confirmed_lsn = consistent_lsn,
            .acknowledged_cursor = cursor,
            .mapped_cursor = cursor,
            .byte_limit = byte_limit,
        };
        try value.validate();
        return value;
    }
    pub fn acquire(self: State, actor: Id, expected: u64, now: u64, expires: u64) !State {
        try self.validate();
        if (self.spool_retired) return error.ChangefeedResynchronizationRequired;
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
    pub fn verifySession(self: State, session: Session, now: u64) !void {
        try self.validate();
        try self.check(session, now);
        if (self.spool_retired) return error.ChangefeedResynchronizationRequired;
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
        if (self.spool.next_id != 1) return error.ChangefeedCapabilityUnsupported;
        return self.append(a, session, now, transaction, 0);
    }
    fn append(self: State, a: A, session: Session, now: u64, transaction: Prepared, spool_id: u64) !State {
        try self.check(session, now);
        try transaction.validate();
        for (self.entries) |entry| if (equal(entry.transaction.after, transaction.after)) {
            if (entry.spool_id != spool_id or !std.meta.eql(entry.transaction, transaction)) return error.ChangefeedConflict;
            return self;
        };
        if (!equal(transaction.before, self.mapped_cursor)) return error.ChangefeedConflict;
        if (self.entries.len == max_entries or transaction.bytes > self.byte_limit - self.pending_bytes) return error.ChangefeedBackpressure;
        const entry: Entry = .{ .transaction = transaction, .spool_id = spool_id, .start_lsn = self.next_lsn, .commit_lsn = std.math.add(u64, self.next_lsn, transaction.commit_offset) catch return error.ChangefeedBackpressure, .end_lsn = std.math.add(u64, self.next_lsn, transaction.bytes) catch return error.ChangefeedBackpressure };
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
        errdefer a.free(next.entries);
        next.spool = try self.spool.retire(a, next.confirmed_lsn, false);
        next.observed_at = now;
        return next;
    }
    fn prepared(spec: spool_control.Spec, content: Digest) Prepared {
        return .{ .before = spec.before, .after = spec.after, .content = content, .completion = spec.completion, .bytes = spec.bytes, .commit_offset = spec.commit_offset, .xid = spec.xid, .commit_time = spec.commit_time };
    }
    pub fn bind(self: State, session: Session, now: u64, value: Binding) !State {
        try self.verifySession(session, now);
        try value.validate();
        if (self.binding) |old| {
            if (!equal(old.identity(), value.identity())) return error.ChangefeedResynchronizationRequired;
            return self;
        }
        if (self.spool.next_id != 1 or self.entries.len != 0) return error.ChangefeedCapabilityUnsupported;
        var next = self;
        next.binding = value;
        next.observed_at = now;
        return next;
    }
    pub fn feedback(self: State, a: A, session: Session, now: u64, expires: u64, flushed: u64) !State {
        try self.verifySession(session, now);
        if (expires < self.session_expires_at or expires <= now or expires - now > max_session_seconds) return error.InvalidChangefeed;
        var next = try self.confirm(a, session, now, flushed);
        next.session_expires_at = expires;
        next.observed_at = now;
        return next;
    }
    pub fn reserveSpool(self: State, a: A, session: Session, now: u64, id: u64, scope: spool_control.Scope, spec: spool_control.Spec) !State {
        try self.validate();
        try self.check(session, now);
        if (nz(spec.wire) and self.binding == null) return error.ChangefeedCapabilityUnsupported;
        if (self.binding) |bound| if (!equal(bound.projection, spec.projection)) return error.ChangefeedConflict;
        if (!equal(spec.feed, feedId(self.database, self.name)) or (self.spool_epoch != 0 and self.spool_epoch != spec.epoch)) return error.ChangefeedConflict;
        if (self.spool.find(id) == null) {
            if (spec.start_lsn != self.next_lsn or !equal(spec.before, self.mapped_cursor)) return error.ChangefeedConflict;
            if (self.entries.len == max_entries or spec.bytes > self.byte_limit - self.pending_bytes) return error.ChangefeedBackpressure;
            for (self.entries) |entry| if (entry.spool_id == 0) return error.ChangefeedCapabilityUnsupported;
        }
        var next = self;
        next.spool = try self.spool.reserve(a, id, scope, spec);
        next.spool_epoch = spec.epoch;
        next.observed_at = now;
        return next;
    }
    pub fn beginSpool(self: State, a: A, session: Session, now: u64, id: u64, token: Id) !State {
        try self.verifySession(session, now);
        var next = self;
        next.spool = try self.spool.begin(a, id, token);
        next.observed_at = now;
        return next;
    }
    /// Caller verifies the sealed manifest and retained completion authority
    /// before proposing this private operation. Locator and LSN map publish in
    /// the same metadata transition; no uploaded object alone becomes visible.
    pub fn publishSpool(self: State, a: A, session: Session, now: u64, id: u64, manifest: Digest, content: Digest) !State {
        try self.validate();
        try self.check(session, now);
        const record = self.spool.find(id) orelse return error.ChangefeedConflict;
        var next = self;
        next.spool = try self.spool.publish(a, id, manifest, content);
        errdefer if (next.spool.records.ptr != self.spool.records.ptr) a.free(next.spool.records);
        next = try next.append(a, session, now, prepared(record.spec, content), id);
        return next;
    }
    pub fn abandonSpool(self: State, a: A, session: Session, now: u64, id: u64) !State {
        try self.validate();
        try self.check(session, now);
        var next = self;
        next.spool = try self.spool.abandon(a, id);
        next.observed_at = now;
        return next;
    }
    pub fn checkpointSpoolCleanup(self: State, a: A, id: u64, storage: Digest, expected: u64, progress: spool_control.Cleanup) !State {
        try self.validate();
        var next = self;
        next.spool = try self.spool.checkpoint(a, id, storage, expected, progress);
        return next;
    }
    pub fn reclaimSpool(self: State, a: A, id: u64, storage: Digest, proof: Digest) !State {
        try self.validate();
        var next = self;
        next.spool = try self.spool.reclaimed(a, id, storage, proof);
        return next;
    }
    pub fn retireSpools(self: State, a: A) !State {
        try self.validate();
        if (self.spool_retired) return self;
        var next = self;
        next.spool = try self.spool.retire(a, 0, true);
        next.spool_retired = true;
        next.session = null;
        next.session_expires_at = 0;
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

fn testCompleteCleanup(state: State, a: A, id: u64, storage: Digest) !State {
    const old = (state.spool.find(id) orelse return error.TestUnexpectedResult).cleanup;
    return state.checkpointSpoolCleanup(a, id, storage, old.revision, .{ .revision = old.revision + 1, .fenced = true, .complete = true, .proof = @splat(5) });
}
fn testSpoolScope(state: State, id: u64) spool_control.Scope {
    var scope: spool_control.Scope = .{ .namespace = @splat(9), .domain = spool_control.Scope.domainFor(feedId(state.database, state.name), 1), .attempt = @splat(7) };
    std.mem.writeInt(u64, scope.attempt[0..8], id, .big);
    return scope;
}
fn testSpoolSpec(state: State, id: u64, bytes: u64) spool_control.Spec {
    const scope = testSpoolScope(state, id);
    return .{ .feed = feedId(state.database, state.name), .epoch = 1, .definition = @splat(9), .projection = @splat(8), .storage = scope.storageIdentity(), .before = state.mapped_cursor, .after = @splat(2), .completion = @splat(3), .start_lsn = state.next_lsn, .bytes = bytes, .commit_offset = bytes - 26, .xid = 42, .commit_time = -1 };
}
test "logical slot spool reservation publication feedback and physical reclamation have separate authority" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    const spec = testSpoolSpec(state, 1, 100);
    const scope = testSpoolScope(state, 1);
    state = try state.reserveSpool(a, session, 101, 1, scope, spec);
    try state.validate();
    const charge = state.spool.charged_bytes;
    try std.testing.expect(charge > spec.bytes);
    try std.testing.expectEqual(@as(u64, 1), state.next_lsn);
    const replay = try state.reserveSpool(a, session, 102, 1, scope, spec);
    try std.testing.expectEqual(charge, replay.spool.charged_bytes);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, state.issue(a, session, 102, testTransaction(1, 2, 100)));
    try std.testing.expectError(error.ChangefeedConflict, state.confirm(a, session, 102, 101));
    try std.testing.expectError(error.ChangefeedConflict, state.reclaimSpool(a, 1, spec.storage, @splat(5)));
    state = try state.publishSpool(a, session, 103, 1, @splat(6), @splat(7));
    try state.validate();
    try std.testing.expectEqual(@as(u64, 101), state.next_lsn);
    try std.testing.expectEqual(@as(u64, 1), state.entries[0].spool_id);
    try std.testing.expectEqual(@as(Digest, @splat(6)), state.spool.records[0].manifest);
    const same_publication = try state.publishSpool(a, session, 104, 1, @splat(6), @splat(7));
    try std.testing.expectEqual(state.next_lsn, same_publication.next_lsn);
    try std.testing.expectError(error.ChangefeedConflict, state.publishSpool(a, session, 104, 1, @splat(8), @splat(7)));
    const partial = try state.confirm(a, session, 105, 100);
    try std.testing.expectEqual(.live, partial.spool.records[0].phase);
    state = try state.confirm(a, session, 106, 101);
    try state.validate();
    try std.testing.expectEqual(@as(u64, 0), state.pending_bytes);
    try std.testing.expectEqual(charge, state.spool.charged_bytes);
    try std.testing.expectEqual(.garbage, state.spool.records[0].phase);
    try std.testing.expectError(error.ChangefeedConflict, state.reclaimSpool(a, 1, @splat(1), @splat(5)));
    state = try testCompleteCleanup(state, a, 1, spec.storage);
    state = try state.reclaimSpool(a, 1, spec.storage, @splat(5));
    try state.validate();
    try std.testing.expectEqual(@as(u64, 0), state.spool.charged_bytes);
    try std.testing.expectEqual(@as(u64, 2), state.spool.next_id);
    try std.testing.expectEqual(@as(u64, 1), state.spool.collected_through);
    const cleanup_retry = try state.reclaimSpool(a, 1, spec.storage, @splat(5));
    try std.testing.expectEqual(@as(u64, 0), cleanup_retry.spool.charged_bytes);
    try std.testing.expectError(error.ChangefeedConflict, state.reserveSpool(a, session, 107, 1, scope, spec));
}
test "logical slot abandoned uploads retain quota fence old owners and freeze projection after collection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), max_pending_bytes);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const old = state.session.?;
    const spec = testSpoolSpec(state, 1, max_pending_bytes);
    state = try state.reserveSpool(a, old, 101, 1, testSpoolScope(state, 1), spec);
    state = try state.acquire(@splat(3), 1, 150, 200);
    const current = state.session.?;
    try std.testing.expectError(error.ChangefeedConflict, state.publishSpool(a, old, 151, 1, @splat(6), @splat(7)));
    state = try state.abandonSpool(a, current, 151, 1);
    try state.validate();
    try std.testing.expect(state.spool.charged_bytes > max_pending_bytes);
    const second = testSpoolSpec(state, 2, max_pending_bytes);
    try std.testing.expectError(error.ChangefeedBackpressure, state.reserveSpool(a, current, 152, 2, testSpoolScope(state, 2), second));
    try std.testing.expectError(error.ChangefeedConflict, state.publishSpool(a, current, 152, 1, @splat(6), @splat(7)));
    state = try testCompleteCleanup(state, a, 1, spec.storage);
    state = try state.reclaimSpool(a, 1, spec.storage, @splat(5));
    var changed = second;
    changed.projection = @splat(10);
    try std.testing.expectError(error.ChangefeedConflict, state.reserveSpool(a, current, 153, 2, testSpoolScope(state, 2), changed));
    state = try state.reserveSpool(a, current, 153, 2, testSpoolScope(state, 2), second);
    try state.validate();
    state = try state.retireSpools(a);
    try state.validate();
    try std.testing.expect(state.session == null);
    try std.testing.expectEqual(.garbage, state.spool.records[0].phase);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, state.acquire(@splat(3), 2, 154, 201));
    state = try testCompleteCleanup(state, a, 2, second.storage);
    state = try state.reclaimSpool(a, 2, second.storage, @splat(5));
    try state.validate();
    try std.testing.expectEqual(@as(u64, 0), state.spool.charged_bytes);
}
fn spoolAllocationProbe(a: A) !void {
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    const spec = testSpoolSpec(state, 1, 100);
    const reserved = try state.reserveSpool(a, session, 101, 1, testSpoolScope(state, 1), spec);
    defer a.free(reserved.spool.records);
    const begun = try reserved.beginSpool(a, session, 102, 1, @splat(8));
    defer a.free(begun.spool.records);
    const published = try begun.publishSpool(a, session, 102, 1, @splat(6), @splat(7));
    defer a.free(published.spool.records);
    defer a.free(published.entries);
    const confirmed = try published.confirm(a, session, 103, 101);
    defer a.free(confirmed.spool.records);
    defer a.free(confirmed.entries);
    const checkpointed = try testCompleteCleanup(confirmed, a, 1, spec.storage);
    defer a.free(checkpointed.spool.records);
    const reclaimed = try checkpointed.reclaimSpool(a, 1, spec.storage, @splat(5));
    defer a.free(reclaimed.spool.records);
    try reclaimed.validate();
}
test "logical slot spool allocation failures retain caller ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, spoolAllocationProbe, .{});
}

test "logical slot spool record bound survives abandoned uploads and rejects out of order collection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    for (1..spool_control.max_records + 1) |id| {
        state = try state.reserveSpool(a, session, 101, id, testSpoolScope(state, id), testSpoolSpec(state, id, 47));
        state = try state.abandonSpool(a, session, 101, id);
    }
    try state.validate();
    const id = state.spool.next_id;
    try std.testing.expectError(error.ChangefeedBackpressure, state.reserveSpool(a, session, 102, id, testSpoolScope(state, id), testSpoolSpec(state, id, 47)));
    const second = state.spool.records[1];
    try std.testing.expectError(error.ChangefeedConflict, state.reclaimSpool(a, second.id, second.spec.storage, @splat(5)));
    const first = state.spool.records[0];
    state = try testCompleteCleanup(state, a, first.id, first.spec.storage);
    state = try state.reclaimSpool(a, first.id, first.spec.storage, @splat(5));
    state = try state.reserveSpool(a, session, 103, id, testSpoolScope(state, id), testSpoolSpec(state, id, 47));
    try state.validate();
    try std.testing.expectEqual(@as(usize, spool_control.max_records), state.spool.records.len);
    var damaged = state;
    damaged.spool.collected_through += 1;
    try std.testing.expectError(error.InvalidChangefeed, damaged.validate());
    damaged = state;
    damaged.spool.charged_bytes -= 1;
    try std.testing.expectError(error.InvalidChangefeed, damaged.validate());
}

test "logical slot cleanup checkpoints fence opaque progress and require the recorded completion proof" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    const spec = testSpoolSpec(state, 1, 100);
    state = try state.reserveSpool(a, session, 101, 1, testSpoolScope(state, 1), spec);
    state = try state.abandonSpool(a, session, 102, 1);
    try std.testing.expectError(error.ChangefeedConflict, state.reclaimSpool(a, 1, spec.storage, @splat(5)));
    const first: spool_control.Cleanup = .{ .revision = 1, .fenced = true, .continuation = "Zmlyc3Q" };
    const charge = state.spool.charged_bytes;
    state = try state.checkpointSpoolCleanup(a, 1, spec.storage, 0, first);
    const retry = try state.checkpointSpoolCleanup(a, 1, spec.storage, 0, first);
    try std.testing.expectEqual(state.spool.records.ptr, retry.spool.records.ptr);
    try std.testing.expectEqual(charge, retry.spool.charged_bytes);
    try std.testing.expectError(error.InvalidChangefeed, state.checkpointSpoolCleanup(a, 1, spec.storage, 1, .{ .revision = 2, .fenced = true, .continuation = "Zh" }));
    try std.testing.expectError(error.InvalidChangefeed, state.checkpointSpoolCleanup(a, 1, spec.storage, 1, .{ .revision = 2, .continuation = "Zg" }));
    try std.testing.expectError(error.ChangefeedConflict, state.checkpointSpoolCleanup(a, 1, spec.storage, 0, .{ .revision = 2, .fenced = true, .continuation = "c2Vjb25k" }));
    state = try state.checkpointSpoolCleanup(a, 1, spec.storage, 1, .{ .revision = 2, .fenced = true, .complete = true, .proof = @splat(9) });
    try std.testing.expectError(error.ChangefeedConflict, state.checkpointSpoolCleanup(a, 1, spec.storage, 2, .{ .revision = 3, .fenced = true }));
    try std.testing.expectError(error.ChangefeedConflict, state.reclaimSpool(a, 1, spec.storage, @splat(5)));
    state = try state.reclaimSpool(a, 1, spec.storage, @splat(9));
    try state.validate();
    try std.testing.expectEqual(@as(u64, 0), state.spool.charged_bytes);
}

test "logical slot strict wire plans require a single immutable build grant" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    var spec = testSpoolSpec(state, 1, 100);
    spec.wire = @splat(8);
    spec.messages = 5;
    state = try state.bind(session, 100, .{ .publications = &.{"rows_pub"}, .projection = spec.projection });
    state = try state.reserveSpool(a, session, 101, 1, testSpoolScope(state, 1), spec);
    try std.testing.expectError(error.ChangefeedConflict, state.publishSpool(a, session, 102, 1, @splat(6), @splat(7)));
    state = try state.beginSpool(a, session, 102, 1, @splat(3));
    const retry = try state.beginSpool(a, session, 102, 1, @splat(3));
    try std.testing.expectEqual(state.spool.records.ptr, retry.spool.records.ptr);
    try std.testing.expectError(error.ChangefeedConflict, state.beginSpool(a, session, 102, 1, @splat(4)));
    state = try state.publishSpool(a, session, 103, 1, @splat(6), @splat(7));
    try state.validate();
    try std.testing.expectEqual(@as(Id, @splat(3)), state.spool.records[0].build.?);
}

test "logical slot publication selection is canonical immutable and bound before wire capture" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const selected = try bindingAlloc(a, &.{ "z", "a" }, @splat(8));
    const replay = try bindingAlloc(a, &.{ "a", "z" }, @splat(8));
    try std.testing.expectEqual(selected.identity(), replay.identity());
    try std.testing.expectError(error.InvalidChangefeed, bindingAlloc(a, &.{ "a", "a" }, @splat(8)));
    try std.testing.expectError(error.InvalidChangefeed, bindingAlloc(a, &.{"bad\x00name"}, @splat(8)));
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    var spec = testSpoolSpec(state, 1, 100);
    spec.wire = @splat(9);
    spec.messages = 5;
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, state.reserveSpool(a, session, 101, 1, testSpoolScope(state, 1), spec));
    state = try state.bind(session, 101, selected);
    state = try state.bind(session, 101, replay);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, state.bind(session, 102, try bindingAlloc(a, &.{"other"}, @splat(8))));
    state = try state.reserveSpool(a, session, 102, 1, testSpoolScope(state, 1), spec);
    var wrong = testSpoolSpec(state, 2, 100);
    wrong.projection = @splat(7);
    try std.testing.expectError(error.ChangefeedConflict, state.reserveSpool(a, session, 103, 2, testSpoolScope(state, 2), wrong));
}
test "logical slot feedback renews valid ownership and confirms only complete boundaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try State.initialize(@splat(1), "slot", @splat(1), 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    state = try state.issue(a, session, 101, testTransaction(1, 2, 100));
    state = try state.feedback(a, session, 110, 170, 100);
    try std.testing.expectEqual(@as(u64, 170), state.session_expires_at);
    try std.testing.expectEqual(@as(u64, 1), state.confirmed_lsn);
    state = try state.feedback(a, session, 120, 180, 101);
    try std.testing.expectEqual(@as(u64, 101), state.confirmed_lsn);
    try std.testing.expectEqual(@as(u64, 180), state.session_expires_at);
    try std.testing.expectError(error.ChangefeedLeaseExpired, state.feedback(a, session, 180, 230, 101));
}
