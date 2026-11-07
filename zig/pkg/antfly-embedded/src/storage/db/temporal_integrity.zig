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

//! Temporal uniqueness on the ordinary durable integrity transaction path.
//! The equality-group gap fence is exclusive through the commit decision.
//! Only adjacent live intervals are needed because admitted groups are disjoint.
//! Pending writes are sorted once; memoized skips keep a long deleted chain
//! from becoming quadratic when a batch replaces many neighboring intervals.
const std = @import("std");
const contract = @import("relational_integrity_contract.zig");
const temporal = @import("temporal_integrity_contract.zig");
const ranges = @import("../../sql/range_value.zig");
const A = std.mem.Allocator;
const Operation = contract.Operation;
const Claim = temporal.Claim;

fn sameOwner(left: Claim, right: Claim) bool {
    return std.mem.eql(u8, left.parent_table, right.parent_table) and std.mem.eql(u8, left.parent_key, right.parent_key);
}

fn samePeriod(left: Claim, right: Claim) !bool {
    if (left.period.kind != right.period.kind) return false;
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var context: ranges.Context = .{ .alloc = none.allocator() };
    return try (try left.view(&context)).order(&context, try right.view(&context)) == .eq;
}

/// Builder owns bounded command credits and all returned effects. This function
/// cannot publish, acquire a separate lock or bypass participant revalidation.
pub fn prepareCommand(builder: anytype, txn: anytype, command: contract.Command) !void {
    const claim = switch (command.operation) {
        .establish_interval, .check_interval => |value| value,
        .release_interval => |value| value.claim,
        else => return error.InvalidIntegrityCommand,
    };
    const key = claim.keyAlloc(builder.alloc, command.address) catch |err| return if (err == error.IntegrityRecordTooLarge) error.RelationalIndexKeyTooLarge else err;
    const current = try builder.current(txn, key);
    if (current) |raw| {
        const old = try Claim.decode(key, raw);
        if (!sameOwner(old, claim)) return error.UniqueConstraintViolation;
        if (command.operation != .establish_interval and !try samePeriod(old, claim)) return error.PreparedReadSetChanged;
        if (command.operation == .check_interval or
            (command.operation == .establish_interval and old.period.kind == claim.period.kind and std.mem.eql(u8, old.period.bytes, claim.period.bytes)))
        {
            return builder.add(txn, command.address, key, .guard, null);
        }
    } else if (command.operation == .check_interval or
        (command.operation == .release_interval and !command.operation.release_interval.repair)) return error.IntegrityMissingCompanion;
    const gap = temporal.guardKey(command.address);
    if (try builder.current(txn, &gap) != null) return error.InvalidIntegrityRecord;
    try builder.add(txn, command.address, &gap, .delete, null);
    if (command.operation == .release_interval) return builder.add(txn, command.address, key, .delete, null);
    const encoded = claim.encode(builder.alloc, command.address) catch |err| return if (err == error.IntegrityRecordTooLarge) error.RelationalIndexKeyTooLarge else err;
    try builder.add(txn, command.address, key, .put, encoded);
}

const PositionMap = std.StringHashMapUnmanaged(usize);
const SkipCache = std.AutoHashMapUnmanaged(usize, ?[]const u8);
const Direction = enum { before, after };
const Checker = struct {
    a: A,
    operations: []const Operation,
    positions: *const PositionMap,
    before: SkipCache = .empty,
    after: SkipCache = .empty,
    bytes: usize = 0,
    fn charge(self: *@This(), size: usize) !void {
        if (size > contract.max_preparation_bytes -| self.bytes) return error.TransactionTooLarge;
        self.bytes += size;
    }
    fn deinit(self: *@This()) void {
        self.before.deinit(self.a);
        self.after.deinit(self.a);
    }
    fn nearest(self: *@This(), txn: anytype, key: []const u8, address: contract.Address, direction: Direction) !?Claim {
        const group = temporal.prefix(address);
        var upper = group;
        var upper_len: usize = upper.len;
        while (upper_len != 0) {
            upper_len -= 1;
            if (upper[upper_len] != 255) {
                upper[upper_len] += 1;
                upper_len += 1;
                break;
            }
        }
        var cursor = try txn.openCursor();
        defer cursor.close();
        // Intent-aware cursors must reject an out-of-group predecessor before
        // checking its intents. Raw store cursors have no intent read guard.
        if (comptime @hasDecl(@TypeOf(cursor), "setLowerBound")) cursor.setLowerBound(&group);
        cursor.setUpperBound(upper[0..upper_len]);
        var item = if (direction == .before) try cursor.seekAtOrBefore(key) else try cursor.seekAtOrAfter(key);
        if (item) |entry| if (std.mem.eql(u8, entry.key, key)) {
            item = if (direction == .before) try cursor.prev() else try cursor.next();
        };
        const cache = if (direction == .before) &self.before else &self.after;
        var path: std.ArrayList(usize) = .empty;
        defer path.deinit(self.a);
        var result: ?Claim = null;
        var found_key: ?[]const u8 = null;
        while (item) |entry| {
            if (!std.mem.startsWith(u8, entry.key, &group)) break;
            if (self.positions.get(entry.key)) |index| if (self.operations[index].kind != .guard) {
                try path.append(self.a, index);
                if (cache.get(index)) |cached| {
                    if (cached) |target| {
                        item = if (direction == .before) try cursor.seekAtOrBefore(target) else try cursor.seekAtOrAfter(target);
                    } else break;
                } else item = if (direction == .before) try cursor.prev() else try cursor.next();
                continue;
            };
            try self.charge(entry.key.len +| entry.value.len);
            const borrowed = try Claim.decode(entry.key, entry.value);
            // Cursor close may invalidate borrowed bytes. The bounded checker
            // region owns only this adjacent claim, never the equality group.
            result = .{
                .tuple = try self.a.dupe(u8, borrowed.tuple),
                .parent_table = try self.a.dupe(u8, borrowed.parent_table),
                .parent_key = try self.a.dupe(u8, borrowed.parent_key),
                .schema_version = borrowed.schema_version,
                .period = .{ .kind = borrowed.period.kind, .bytes = try self.a.dupe(u8, borrowed.period.bytes) },
            };
            found_key = try self.a.dupe(u8, entry.key);
            break;
        }
        for (path.items) |index| try cache.put(self.a, index, found_key);
        return result;
    }
};

fn requireDisjoint(left: Claim, right: Claim) !void {
    if (!std.mem.eql(u8, left.tuple, right.tuple) or left.period.kind != right.period.kind) return error.IntegrityAddressMismatch;
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var context: ranges.Context = .{ .alloc = none.allocator() };
    if (try (try left.view(&context)).overlaps(&context, try right.view(&context))) return error.TemporalOverlapViolation;
}

/// Run during planning and again under the participant's apply fence before
/// acquiring intents. Expected bytes alone cannot prove an absent interval gap.
pub fn validateEffects(a: A, txn: anytype, operations: []const Operation, positions: *const PositionMap) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var puts: std.ArrayList(usize) = .empty;
    var checker: Checker = .{ .a = scratch, .operations = operations, .positions = positions };
    defer checker.deinit();
    for (operations, 0..) |op, index| {
        const parsed = try contract.parseKey(op.key);
        if (parsed.kind != .interval) continue;
        if (op.kind == .guard) continue;
        const guard_index = positions.get(&temporal.guardKey(parsed.address)) orelse return error.IntegrityClaimGuardRequired;
        const guard = operations[guard_index];
        if (guard.kind != .delete or guard.expected_value != null or guard.value != null) return error.IntegrityClaimGuardRequired;
        if (op.kind == .put) try puts.append(scratch, index);
    }
    std.mem.sort(usize, puts.items, operations, struct {
        fn less(ops: []const Operation, left: usize, right: usize) bool {
            return std.mem.order(u8, ops[left].key, ops[right].key) == .lt;
        }
    }.less);
    var previous: ?usize = null;
    for (puts.items) |index| {
        const op = operations[index];
        const parsed = try contract.parseKey(op.key);
        const claim = try Claim.decode(op.key, op.value.?);
        if (previous) |prior| {
            if (std.mem.eql(u8, operations[prior].key[0..contract.key_len], op.key[0..contract.key_len])) {
                try requireDisjoint(try Claim.decode(operations[prior].key, operations[prior].value.?), claim);
            }
        }
        if (try checker.nearest(txn, op.key, parsed.address, .before)) |neighbor| try requireDisjoint(neighbor, claim);
        if (try checker.nearest(txn, op.key, parsed.address, .after)) |neighbor| try requireDisjoint(claim, neighbor);
        previous = index;
    }
}

/// Verify the complete staged interval set before publication. A transferred
/// group shares one logical routing digest, so it cannot be split by physical
/// metadata ordering. This uses the same adjacent-live-interval validator as
/// mutation prepare, with page-owned input copied before further store reads.
pub fn validateTransferred(a: A, txn: anytype, key: []const u8, value: []const u8) !void {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const stable_key = try scratch.dupe(u8, key);
    const stable_value = try scratch.dupe(u8, value);
    const address = (try contract.parseKey(stable_key)).address;
    const gap = temporal.guardKey(address);
    const operations = [_]Operation{
        .{ .routing_key = &address.routing, .key = stable_key, .kind = .put, .value = stable_value },
        .{ .routing_key = &address.routing, .key = &gap, .kind = .delete },
    };
    var positions: PositionMap = .empty;
    defer positions.deinit(scratch);
    for (operations, 0..) |op, index| try positions.put(scratch, op.key, index);
    try validateEffects(scratch, txn, &operations, &positions);
}
