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

//! PostgreSQL lock conflicts and bounded per-backend grant accounting. This
//! contract grants no authority: accepting writers must independently join the
//! backend fence, catalog incarnation, resource and operation receipt.
const std = @import("std");
pub const max_modes = 8;
pub const Class = enum(u8) { table, row, advisory };
pub const TableMode = enum(u3) { access_share, row_share, row_exclusive, share_update_exclusive, share, share_row_exclusive, exclusive, access_exclusive };
pub const RowMode = enum(u2) { key_share, share, no_key_update, update };
pub const AdvisoryMode = enum(u1) { shared, exclusive };
pub const Mode = union(Class) {
    table: TableMode,
    row: RowMode,
    advisory: AdvisoryMode,
    pub fn class(self: Mode) Class {
        return std.meta.activeTag(self);
    }
    pub fn index(self: Mode) u3 {
        return switch (self) {
            inline else => |v| @intCast(@backingInt(v)),
        };
    }
    pub fn conflicts(self: Mode) u8 {
        return switch (self) {
            .table => |v| ([_]u8{ 0x80, 0xc0, 0xf0, 0xf8, 0xec, 0xfc, 0xfe, 0xff })[@backingInt(v)],
            .row => |v| ([_]u8{ 0x08, 0x0c, 0x0e, 0x0f })[@backingInt(v)],
            .advisory => |v| ([_]u8{ 0x02, 0x03 })[@backingInt(v)],
        };
    }
    pub fn conflictsWith(self: Mode, other: Mode) !bool {
        if (self.class() != other.class()) return error.InvalidLockClass;
        return self.conflicts() & (@as(u8, 1) << other.index()) != 0;
    }
};
pub fn modeCount(class: Class) usize {
    return switch (class) {
        .table => 8,
        .row => 4,
        .advisory => 2,
    };
}

pub const Backend = struct {
    incarnation: [16]u8,
    id: [16]u8,
    fence: u64,
    pub fn validate(self: Backend) !void {
        if (std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.id, 0) or self.fence == 0) return error.InvalidLockOwner;
    }
    pub fn eql(self: Backend, other: Backend) bool {
        return std.meta.eql(self, other);
    }
};
pub const Scope = union(enum) {
    session,
    transaction: struct { id: [16]u8, subtransaction: u64 },
};
/// The two PostgreSQL advisory key spaces are disjoint, including negative IDs.
pub const AdvisoryKey = union(enum) { single: i64, pair: struct { first: i32, second: i32 } };
pub fn advisoryResource(incarnation: [16]u8, database: u64, key: AdvisoryKey) ![32]u8 {
    if (database == 0 or std.mem.allEqual(u8, &incarnation, 0)) return error.InvalidLockResource;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/advisory-lock-resource/v1");
    hash.update(&incarnation);
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, database, .little);
    hash.update(&bytes);
    switch (key) {
        .single => |value| {
            hash.update(&.{0});
            std.mem.writeInt(i64, &bytes, value, .little);
        },
        .pair => |value| {
            hash.update(&.{1});
            std.mem.writeInt(i32, bytes[0..4], value.first, .little);
            std.mem.writeInt(i32, bytes[4..8], value.second, .little);
        },
    }
    hash.update(&bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

/// Counts distinct backends per mode, not reentrant acquisitions. Admission
/// subtracts only the exact current backend fence before checking conflicts.
pub const Claim = struct { backend: Backend, resource: [32]u8 };

pub const Aggregate = struct {
    class: Class,
    owners: [max_modes]u32 = @splat(0),
    pub fn blocked(self: Aggregate, requested: Mode, claim: Claim, own: ?Grant) !bool {
        try claim.backend.validate();
        if (std.mem.allEqual(u8, &claim.resource, 0)) return error.InvalidLockResource;
        if (self.class != requested.class()) return error.InvalidLockClass;
        const own_mask = if (own) |value| blk: {
            try value.validate();
            if (!value.backend.eql(claim.backend) or !std.mem.eql(u8, &value.resource, &claim.resource)) return error.LockOwnerChanged;
            if (value.class != self.class) return error.InvalidLockClass;
            break :blk value.mask();
        } else 0;
        for (self.owners, 0..) |count, index| {
            const bit = @as(u8, 1) << @as(u3, @intCast(index));
            if (index >= modeCount(self.class) and count != 0) return error.InvalidLockRecord;
            const subtract: u32 = @intFromBool(own_mask & bit != 0);
            if (count < subtract) return error.InvalidLockRecord;
            if (requested.conflicts() & bit != 0 and count != subtract) return true;
        }
        return false;
    }
    pub fn transition(self: *Aggregate, before: u8, after: u8) !void {
        var next = self.*;
        for (&next.owners, 0..) |*count, index| {
            const bit = @as(u8, 1) << @as(u3, @intCast(index));
            if (index >= modeCount(self.class) and (before | after) & bit != 0) return error.InvalidLockRecord;
            if (before & bit == 0 and after & bit != 0) count.* = std.math.add(u32, count.*, 1) catch return error.LockLimitExceeded;
            if (before & bit != 0 and after & bit == 0) count.* = std.math.sub(u32, count.*, 1) catch return error.InvalidLockRecord;
        }
        self.* = next;
    }
};

/// At most eight mode slots, independently of reentrance or savepoint depth.
/// Transaction slots remember their oldest live acquisition. Savepoint release
/// reparents them; rollback removes only acquisitions in the aborted subtree.
pub const Grant = struct {
    backend: Backend,
    resource: [32]u8,
    class: Class,
    revision: u64 = 1,
    session: [max_modes]u32 = @splat(0),
    transaction: ?[16]u8 = null,
    acquired: [max_modes]?u64 = @splat(null),
    pub fn mask(self: Grant) u8 {
        var result: u8 = 0;
        for (self.session, self.acquired, 0..) |count, slot, index| if (count != 0 or slot != null) {
            result |= @as(u8, 1) << @as(u3, @intCast(index));
        };
        return result;
    }
    pub fn validate(self: Grant) !void {
        try self.backend.validate();
        if (self.revision == 0 or std.mem.allEqual(u8, &self.resource, 0)) return error.InvalidLockRecord;
        var has_transaction = false;
        for (self.session, self.acquired, 0..) |count, slot, index| {
            if ((self.class != .advisory and count != 0) or (index >= modeCount(self.class) and (count != 0 or slot != null))) return error.InvalidLockRecord;
            has_transaction = has_transaction or slot != null;
        }
        if (has_transaction != (self.transaction != null)) return error.InvalidLockRecord;
        if (self.transaction) |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidLockRecord;
    }
    fn finish(self: *Grant, next_value: Grant, changed: bool) !bool {
        if (!changed) return false;
        var next = next_value;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.LockLimitExceeded;
        var has_transaction = false;
        for (next.acquired) |slot| has_transaction = has_transaction or slot != null;
        if (!has_transaction) next.transaction = null;
        try next.validate();
        self.* = next;
        return true;
    }
    pub fn acquire(self: *Grant, mode: Mode, scope: Scope) !bool {
        try self.validate();
        if (self.class != mode.class()) return error.InvalidLockClass;
        var next = self.*;
        const index = mode.index();
        switch (scope) {
            .session => {
                if (self.class != .advisory) return error.InvalidLockScope;
                next.session[index] = std.math.add(u32, next.session[index], 1) catch return error.LockLimitExceeded;
            },
            .transaction => |owner| {
                if (std.mem.allEqual(u8, &owner.id, 0)) return error.InvalidLockOwner;
                if (next.transaction) |id| if (!std.mem.eql(u8, &id, &owner.id)) return error.LockOwnerChanged;
                if (next.acquired[index]) |old| {
                    if (old > owner.subtransaction) return error.InvalidLockScope;
                    return false;
                }
                next.transaction = owner.id;
                next.acquired[index] = owner.subtransaction;
            },
        }
        return self.finish(next, true);
    }
    pub fn unlock(self: *Grant, mode: AdvisoryMode) !bool {
        try self.validate();
        if (self.class != .advisory) return error.InvalidLockClass;
        const index = @backingInt(mode);
        if (self.session[index] == 0) return false;
        var next = self.*;
        next.session[index] -= 1;
        return self.finish(next, true);
    }
    pub fn unlockAllSession(self: *Grant) !bool {
        try self.validate();
        if (self.class != .advisory) return error.InvalidLockClass;
        var next = self.*;
        var changed = false;
        for (&next.session) |*count| {
            changed = changed or count.* != 0;
            count.* = 0;
        }
        return self.finish(next, changed);
    }
    pub fn rollback(self: *Grant, transaction: [16]u8, first_aborted: u64) !bool {
        try self.validate();
        if (self.transaction == null) return false;
        if (!std.mem.eql(u8, &self.transaction.?, &transaction)) return error.LockOwnerChanged;
        var next = self.*;
        var changed = false;
        for (&next.acquired) |*slot| if (slot.*) |ordinal| if (ordinal >= first_aborted) {
            slot.* = null;
            changed = true;
        };
        return self.finish(next, changed);
    }
    /// PREPARE hands transaction locks to an independent durable backend.
    /// Mixed session/transaction ownership of this resource is rejected.
    /// Session grants on other resources stay on the live connection; a new
    /// transaction must conflict with its prepared predecessor normally.
    pub fn detachTransaction(self: *Grant, prepared_backend: Backend) !?Grant {
        try self.validate();
        try prepared_backend.validate();
        if (!std.mem.eql(u8, &self.backend.incarnation, &prepared_backend.incarnation) or std.mem.eql(u8, &self.backend.id, &prepared_backend.id)) return error.InvalidLockOwner;
        if (self.transaction == null) return null;
        // PostgreSQL rejects PREPARE when either session mode coexists with
        // transaction ownership of this resource, even across different modes.
        for (self.session) |count| if (count != 0) return error.MixedAdvisoryPrepareUnsupported;
        var detached = self.*;
        detached.backend = prepared_backend;
        detached.revision = 1;
        detached.session = @splat(0);
        for (&detached.acquired) |*slot| if (slot.* != null) {
            slot.* = 0;
        };
        try detached.validate();
        var next = self.*;
        next.acquired = @splat(null);
        _ = try self.finish(next, true);
        return detached;
    }

    pub const wire_bytes = 234;
    const magic = "ALCK\x01\x00\x00\x00";
    pub fn encode(self: Grant) ![wire_bytes]u8 {
        try self.validate();
        var bytes: [wire_bytes]u8 = @splat(0);
        @memcpy(bytes[0..8], magic);
        bytes[8] = @backingInt(self.class);
        @memcpy(bytes[9..25], &self.backend.incarnation);
        @memcpy(bytes[25..41], &self.backend.id);
        std.mem.writeInt(u64, bytes[41..49], self.backend.fence, .little);
        @memcpy(bytes[49..81], &self.resource);
        std.mem.writeInt(u64, bytes[81..89], self.revision, .little);
        for (self.session, 0..) |count, index| std.mem.writeInt(u32, bytes[89 + index * 4 ..][0..4], count, .little);
        if (self.transaction) |id| @memcpy(bytes[121..137], &id);
        for (self.acquired, 0..) |slot, index| if (slot) |ordinal| {
            bytes[137] |= @as(u8, 1) << @as(u3, @intCast(index));
            std.mem.writeInt(u64, bytes[138 + index * 8 ..][0..8], ordinal, .little);
        };
        std.crypto.hash.Blake3.hash(bytes[0..202], bytes[202..234], .{});
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Grant {
        if (bytes.len != wire_bytes or !std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidLockRecord;
        var hash: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..202], &hash, .{});
        if (!std.mem.eql(u8, &hash, bytes[202..234])) return error.InvalidLockRecord;
        var result: Grant = .{
            .class = std.enums.fromInt(Class, bytes[8]) orelse return error.InvalidLockRecord,
            .backend = .{ .incarnation = bytes[9..25].*, .id = bytes[25..41].*, .fence = std.mem.readInt(u64, bytes[41..49], .little) },
            .resource = bytes[49..81].*,
            .revision = std.mem.readInt(u64, bytes[81..89], .little),
        };
        for (&result.session, 0..) |*count, index| count.* = std.mem.readInt(u32, bytes[89 + index * 4 ..][0..4], .little);
        if (!std.mem.allEqual(u8, bytes[121..137], 0)) result.transaction = bytes[121..137].*;
        for (&result.acquired, 0..) |*slot, index| {
            const value = std.mem.readInt(u64, bytes[138 + index * 8 ..][0..8], .little);
            if (bytes[137] & (@as(u8, 1) << @as(u3, @intCast(index))) != 0) slot.* = value else if (value != 0) return error.InvalidLockRecord;
        }
        try result.validate();
        return result;
    }

    pub fn releaseSavepoint(self: *Grant, transaction: [16]u8, first_child: u64, parent: u64) !bool {
        try self.validate();
        if (parent >= first_child) return error.InvalidLockScope;
        if (self.transaction == null) return false;
        if (!std.mem.eql(u8, &self.transaction.?, &transaction)) return error.LockOwnerChanged;
        var next = self.*;
        var changed = false;
        for (&next.acquired) |*slot| if (slot.*) |ordinal| if (ordinal >= first_child) {
            slot.* = parent;
            changed = true;
        };
        return self.finish(next, changed);
    }
};

test "pessimistic locks PostgreSQL table row and advisory matrices are symmetric and preserve key-share updates" {
    const table = [_]u8{ 0x80, 0xc0, 0xf0, 0xf8, 0xec, 0xfc, 0xfe, 0xff };
    for (0..8) |left| for (0..8) |right| {
        const a: Mode = .{ .table = @fromBackingInt(@as(u3, @intCast(left))) };
        const b: Mode = .{ .table = @fromBackingInt(@as(u3, @intCast(right))) };
        try std.testing.expectEqual(table[left] & (@as(u8, 1) << @as(u3, @intCast(right))) != 0, try a.conflictsWith(b));
        try std.testing.expectEqual(try a.conflictsWith(b), try b.conflictsWith(a));
    };
    try std.testing.expect(!try (Mode{ .row = .key_share }).conflictsWith(.{ .row = .no_key_update }));
    try std.testing.expect(try (Mode{ .row = .share }).conflictsWith(.{ .row = .no_key_update }));
    try std.testing.expect(!try (Mode{ .advisory = .shared }).conflictsWith(.{ .advisory = .shared }));
    try std.testing.expect(try (Mode{ .advisory = .shared }).conflictsWith(.{ .advisory = .exclusive }));
    try std.testing.expectError(error.InvalidLockClass, (Mode{ .row = .update }).conflictsWith(.{ .table = .access_exclusive }));
}

test "pessimistic locks count backends once while session reentrance and transaction lifetimes stay independent" {
    var grant: Grant = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .resource = @splat(3), .class = .advisory };
    var aggregate: Aggregate = .{ .class = .advisory };
    const before = grant.mask();
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .session));
    try aggregate.transition(before, grant.mask());
    try std.testing.expect(!try aggregate.blocked(.{ .advisory = .shared }, .{ .backend = grant.backend, .resource = grant.resource }, grant));
    try std.testing.expect(try aggregate.blocked(.{ .advisory = .shared }, .{ .backend = grant.backend, .resource = grant.resource }, null));
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .session));
    try std.testing.expectEqual(@as(u32, 1), aggregate.owners[1]);
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .{ .transaction = .{ .id = @splat(4), .subtransaction = 0 } }));
    try std.testing.expect(!try grant.acquire(.{ .advisory = .exclusive }, .{ .transaction = .{ .id = @splat(4), .subtransaction = 1 } }));
    try std.testing.expect(try grant.unlock(.exclusive));
    try std.testing.expectEqual(@as(u32, 1), grant.session[1]);
    try std.testing.expect(try grant.unlockAllSession());
    try std.testing.expectEqual(@as(u8, 2), grant.mask());
    try std.testing.expect(!try grant.unlock(.exclusive));
    try std.testing.expect(try grant.rollback(@splat(4), 0));
    try aggregate.transition(2, grant.mask());
    try std.testing.expect(!try aggregate.blocked(.{ .advisory = .exclusive }, .{ .backend = grant.backend, .resource = grant.resource }, null));
}

test "pessimistic locks savepoints retain ancestor modes reparent children and never release session locks" {
    var grant: Grant = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .resource = @splat(3), .class = .advisory };
    const txn: [16]u8 = @splat(4);
    try std.testing.expect(try grant.acquire(.{ .advisory = .shared }, .{ .transaction = .{ .id = txn, .subtransaction = 0 } }));
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .{ .transaction = .{ .id = txn, .subtransaction = 10 } }));
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .session));
    try std.testing.expect(try grant.releaseSavepoint(txn, 10, 5));
    try std.testing.expect(!try grant.rollback(txn, 10));
    try std.testing.expect(try grant.rollback(txn, 5));
    try std.testing.expectEqual(@as(?u64, 0), grant.acquired[0]);
    try std.testing.expect(grant.acquired[1] == null);
    try std.testing.expectEqual(@as(u32, 1), grant.session[1]);
    try std.testing.expectEqual(@as(u8, 3), grant.mask());
    try std.testing.expect(try grant.rollback(txn, 0));
    try std.testing.expectEqual(@as(u8, 2), grant.mask());
}

test "pessimistic locks reject overflowing grants owner substitution and aggregate underflow atomically" {
    var grant: Grant = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .resource = @splat(3), .class = .advisory, .session = .{ 0, std.math.maxInt(u32), 0, 0, 0, 0, 0, 0 } };
    const before = grant;
    try std.testing.expectError(error.LockLimitExceeded, grant.acquire(.{ .advisory = .exclusive }, .session));
    try std.testing.expectEqualDeep(before, grant);
    var aggregate: Aggregate = .{ .class = .advisory };
    try std.testing.expectError(error.InvalidLockRecord, aggregate.blocked(.{ .advisory = .exclusive }, .{ .backend = grant.backend, .resource = grant.resource }, grant));
    try std.testing.expectError(error.InvalidLockRecord, aggregate.transition(2, 0));
    try std.testing.expectEqual(@as(u32, 0), aggregate.owners[1]);
    const incarnation: [16]u8 = @splat(1);
    try std.testing.expect(!std.mem.eql(u8, &try advisoryResource(incarnation, 1, .{ .single = -42 }), &try advisoryResource(incarnation, 1, .{ .pair = .{ .first = -1, .second = -42 } })));
    try std.testing.expect(!std.mem.eql(u8, &try advisoryResource(incarnation, 1, .{ .single = 42 }), &try advisoryResource(incarnation, 2, .{ .single = 42 })));
    var stale = grant.backend;
    stale.fence += 1;
    try std.testing.expect(!stale.eql(grant.backend));
}

test "pessimistic locks prepared handoff rejects mixed ownership and separates the prepared backend" {
    var grant: Grant = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .resource = @splat(3), .class = .advisory };
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .session));
    try std.testing.expect(try grant.acquire(.{ .advisory = .exclusive }, .{ .transaction = .{ .id = @splat(4), .subtransaction = 7 } }));
    var aggregate: Aggregate = .{ .class = .advisory };
    try aggregate.transition(0, grant.mask());
    const mixed = grant;
    try std.testing.expectError(error.MixedAdvisoryPrepareUnsupported, grant.detachTransaction(.{ .incarnation = @splat(1), .id = @splat(5), .fence = 1 }));
    try std.testing.expectEqualDeep(mixed, grant);
    // The contract is atomic on rejection; the SQL owner separately aborts
    // the failed transaction. Qualify transfer of a transaction-only grant.
    try std.testing.expect(try grant.unlockAllSession());
    const original_mask = grant.mask();
    var prepared = (try grant.detachTransaction(.{ .incarnation = @splat(1), .id = @splat(5), .fence = 1 })).?;
    try aggregate.transition(original_mask, grant.mask());
    try std.testing.expectEqual(@as(u32, 0), grant.session[1]);
    try std.testing.expect(grant.transaction == null);
    try std.testing.expectEqual(@as(u32, 0), prepared.session[1]);
    try std.testing.expectEqual(@as(?u64, 0), prepared.acquired[1]);
    try aggregate.transition(0, prepared.mask());
    const claim: Claim = .{ .backend = grant.backend, .resource = grant.resource };
    try std.testing.expect(try aggregate.blocked(.{ .advisory = .exclusive }, claim, grant));
    try std.testing.expectError(error.LockOwnerChanged, aggregate.blocked(.{ .advisory = .exclusive }, claim, prepared));
    const before = prepared.mask();
    try std.testing.expect(try prepared.rollback(@splat(4), 0));
    try aggregate.transition(before, prepared.mask());
    try std.testing.expect(!try aggregate.blocked(.{ .advisory = .exclusive }, claim, grant));
}

test "pessimistic locks fixed grant codec covers full-width values and rejects corruption and alternate absent slots" {
    var grant: Grant = .{ .backend = .{ .incarnation = @splat(255), .id = @splat(255), .fence = std.math.maxInt(u64) }, .resource = @splat(255), .class = .advisory, .revision = std.math.maxInt(u64), .session = .{ std.math.maxInt(u32), std.math.maxInt(u32), 0, 0, 0, 0, 0, 0 } };
    grant.transaction = @splat(255);
    grant.acquired[0] = 0;
    grant.acquired[1] = std.math.maxInt(u64);
    const encoded = try grant.encode();
    try std.testing.expectEqualDeep(grant, try Grant.decode(&encoded));
    try std.testing.expectError(error.InvalidLockRecord, Grant.decode(encoded[0 .. encoded.len - 1]));
    var corrupt = encoded;
    corrupt[50] ^= 1;
    try std.testing.expectError(error.InvalidLockRecord, Grant.decode(&corrupt));
    corrupt = encoded;
    corrupt[138 + 2 * 8] = 1;
    std.crypto.hash.Blake3.hash(corrupt[0..202], corrupt[202..234], .{});
    try std.testing.expectError(error.InvalidLockRecord, Grant.decode(&corrupt));
    corrupt = encoded;
    corrupt[8] = 255;
    std.crypto.hash.Blake3.hash(corrupt[0..202], corrupt[202..234], .{});
    try std.testing.expectError(error.InvalidLockRecord, Grant.decode(&corrupt));
}
