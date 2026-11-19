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

//! Execution-owned sequence caches. Durable snapshots contain issued values,
//! never unconsumed leases: reopening a backend burns its remaining cache.
const std = @import("std");
const sequence = @import("../system_catalog/sequences.zig");
const A = std.mem.Allocator;
pub const max_sequences = 256;

pub const Identity = struct {
    incarnation: [16]u8,
    id: u64,

    pub fn validate(self: Identity) !void {
        if (self.id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSequenceSession;
    }
};
pub const Current = struct { identity: Identity, value: i64 };
pub const Snapshot = struct {
    values: []const Current = &.{},
    last: ?Identity = null,

    pub fn clone(self: Snapshot, a: A) !Snapshot {
        try self.validate();
        return .{ .values = try a.dupe(Current, self.values), .last = self.last };
    }
    pub fn deinit(self: *Snapshot, a: A) void {
        a.free(self.values);
        self.* = .{};
    }
    pub fn validate(self: Snapshot) !void {
        if (self.values.len > max_sequences) return error.InvalidSequenceSession;
        var found = self.last == null;
        if (self.last) |id| try id.validate();
        for (self.values, 0..) |entry, i| {
            try entry.identity.validate();
            for (self.values[0..i]) |prior| if (std.meta.eql(prior.identity, entry.identity)) return error.InvalidSequenceSession;
            if (self.last) |id| if (std.meta.eql(id, entry.identity)) {
                found = true;
            };
        }
        if (!found) return error.InvalidSequenceSession;
    }
};

pub const Entry = struct {
    current: ?i64 = null,
    cache: ?struct { lease: sequence.Lease, issued: u64 = 0 } = null,

    pub fn install(self: *Entry, lease: sequence.Lease) !void {
        _ = try lease.value(0);
        _ = try lease.value(lease.count - 1);
        self.cache = .{ .lease = lease };
    }

    pub fn take(self: *Entry, generation: u64) !?i64 {
        const cache = if (self.cache) |*value| value else return null;
        if (cache.lease.generation != generation or cache.issued == cache.lease.count) {
            self.cache = null;
            return null;
        }
        const value = try cache.lease.value(cache.issued);
        cache.issued += 1;
        return value;
    }

    // SETVAL(..., false) changes allocation state, not this session's currval.
    // SETVAL(..., true) changes currval but never selects a new lastval owner.
    pub fn set(self: *Entry, value: i64, called: bool) void {
        self.cache = null;
        if (called) self.current = value;
    }
};

/// A protocol connection owns both the state and its allocation lifetime.
pub const Borrowed = struct { state: *State, allocator: A };
pub const State = struct {
    entries: std.AutoHashMapUnmanaged(Identity, Entry) = .empty,
    last: ?Identity = null,
    dirty: bool = false,

    pub fn deinit(self: *State, a: A) void {
        self.entries.deinit(a);
        self.* = .{};
    }
    pub fn init(a: A, stored: Snapshot) !State {
        try stored.validate();
        var result: State = .{ .last = stored.last };
        errdefer result.deinit(a);
        try result.entries.ensureTotalCapacity(a, @intCast(stored.values.len));
        for (stored.values) |entry| result.entries.putAssumeCapacity(entry.identity, .{ .current = entry.value });
        return result;
    }
    /// Reserve the slot before allocator admission. Publishing a committed
    /// result must not allocate, and a failed reservation cannot set currval.
    pub fn prepare(self: *State, a: A, identity: Identity) !*Entry {
        try identity.validate();
        if (self.entries.getPtr(identity)) |entry| return entry;
        if (self.entries.count() == max_sequences) return error.SqlProgramLimitExceeded;
        const slot = try self.entries.getOrPut(a, identity);
        slot.value_ptr.* = .{};
        return slot.value_ptr;
    }
    pub fn publishNext(self: *State, identity: Identity, value: i64) void {
        self.entries.getPtr(identity).?.current = value;
        self.last = identity;
        self.dirty = true;
    }
    pub fn publishSet(self: *State, identity: Identity, value: i64, called: bool) void {
        self.entries.getPtr(identity).?.set(value, called);
        self.dirty = self.dirty or called;
    }
    pub fn current(self: *const State, identity: Identity) !i64 {
        const entry = self.entries.get(identity) orelse return error.SqlSequenceValueNotDefined;
        return entry.current orelse error.SqlSequenceValueNotDefined;
    }
    pub fn lastValue(self: *const State) !Current {
        const identity = self.last orelse return error.SqlSequenceValueNotDefined;
        return .{ .identity = identity, .value = try self.current(identity) };
    }
    pub fn discard(self: *State) void {
        self.entries.clearRetainingCapacity();
        self.last = null;
        self.dirty = true;
    }
    pub fn snapshot(self: *const State, a: A) !Snapshot {
        var output: std.ArrayList(Current) = .empty;
        errdefer output.deinit(a);
        var it = self.entries.iterator();
        while (it.next()) |entry| if (entry.value_ptr.current) |value|
            try output.append(a, .{ .identity = entry.key_ptr.*, .value = value });
        std.mem.sort(Current, output.items, {}, struct {
            fn less(_: void, left: Current, right: Current) bool {
                const order = std.mem.order(u8, &left.identity.incarnation, &right.identity.incarnation);
                return if (order == .eq) left.identity.id < right.identity.id else order == .lt;
            }
        }.less);
        return .{ .values = try output.toOwnedSlice(a), .last = self.last };
    }
};

test "SQL sequence session cached leases keep currval lastval setval and restart semantics distinct" {
    const a = std.testing.allocator;
    const first: Identity = .{ .incarnation = @splat(1), .id = 1 };
    const second: Identity = .{ .incarnation = @splat(1), .id = 2 };
    var state: State = .{};
    defer state.deinit(a);
    const slot = try state.prepare(a, first);
    try slot.install(.{ .generation = 1, .counter_revision = 2, .first = 100, .increment = 10, .count = 4 });
    try std.testing.expectError(error.SqlSequenceValueNotDefined, state.current(first));
    for ([_]i64{ 100, 110 }) |expected| {
        const value = (try slot.take(1)).?;
        state.publishNext(first, value);
        try std.testing.expectEqual(expected, value);
        try std.testing.expectEqual(expected, try state.current(first));
    }
    _ = try state.prepare(a, second);
    state.publishSet(second, 500, true);
    try std.testing.expectEqual(@as(i64, 500), try state.current(second));
    try std.testing.expectEqual(first, (try state.lastValue()).identity);
    state.publishSet(first, 900, false);
    try std.testing.expectEqual(@as(i64, 110), try state.current(first));
    try std.testing.expect((try state.entries.getPtr(first).?.take(1)) == null);
    const snapshot = try state.snapshot(a);
    defer a.free(snapshot.values);
    var reopened = try State.init(a, snapshot);
    defer reopened.deinit(a);
    try std.testing.expectEqualDeep(try state.lastValue(), try reopened.lastValue());
    try std.testing.expect(reopened.entries.getPtr(first).?.cache == null);
    reopened.discard();
    try std.testing.expectError(error.SqlSequenceValueNotDefined, reopened.lastValue());
}

test "SQL sequence session arithmetic stays constant space at signed limits and invalidates only cached generations" {
    var entry: Entry = .{ .current = 9 };
    try entry.install(.{ .generation = 1, .counter_revision = 2, .first = std.math.minInt(i64), .increment = 1, .count = std.math.maxInt(i64) });
    try std.testing.expectEqual(@as(?i64, std.math.minInt(i64)), try entry.take(1));
    try std.testing.expect((try entry.take(2)) == null);
    try std.testing.expectEqual(@as(?i64, 9), entry.current);
    try entry.install(.{ .generation = 2, .counter_revision = 3, .first = 0, .increment = std.math.minInt(i64), .count = 2 });
    try std.testing.expectEqual(@as(?i64, 0), try entry.take(2));
    try std.testing.expectEqual(@as(?i64, std.math.minInt(i64)), try entry.take(2));
    try std.testing.expect((try entry.take(2)) == null);
}
