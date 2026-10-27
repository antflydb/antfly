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

//! Counter CAS and bounded retry receipts for the metadata transaction owner.
//! This is not a second coordinator, nor permission to allocate sequence IDs.
//! The caller pins/validates the definition in the SAME transaction, performs
//! the normal Raft admission, and rolls back the transaction on ANY error.
//! Definition changes and counter replacement must commit together. Merely
//! importing this contract does not activate public SQL sequence operations.
const std = @import("std");
const sequence = @import("sequences.zig");
const A = std.mem.Allocator;

pub const retained_receipts: u64 = 64;
const prefix = "\x00\x00__metadata__:sql_sequence_counters:v1:";
pub const Key = struct { group: u64, sequence_id: u64 };

pub fn allGroupsPrefix() []const u8 {
    return prefix;
}

/// Metadata snapshots/projected stores select counters through the same
/// group-prefix machinery as every other catalog-owned durable projection.
pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0 or buffer.len < prefix.len + 8) return error.InvalidSequenceRequest;
    @memcpy(buffer[0..prefix.len], prefix);
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], group, .big);
    return buffer[0 .. prefix.len + 8];
}
const CounterRecord = struct { format: u8, identity: Key, state: sequence.State };
pub const Operation = union(enum) { reserve, set: struct { value: i64, called: bool } };
pub const Request = struct {
    /// Bind the complete preimage, not just its CAS revision. This permits
    /// exact historical-result verification after the live counter advances.
    expected: sequence.State,
    /// An authenticated request identity. An ambiguous request must never be
    /// retried with a new revision/identity just because its reply was lost.
    request_id: [16]u8,
    operation: Operation,

    pub fn validate(self: Request) !void {
        if (self.expected.generation == 0 or self.expected.revision == 0 or
            std.mem.allEqual(u8, &self.request_id, 0)) return error.InvalidSequenceRequest;
    }

    fn digest(self: Request, key: Key) ![32]u8 {
        try self.validate();
        if (key.group == 0 or key.sequence_id == 0) return error.InvalidSequenceRequest;
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly.sequence.request.v2\x00");
        inline for (.{ key.group, key.sequence_id, self.expected.generation, self.expected.revision, @as(u64, @bitCast(self.expected.last_value)) }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .big);
            h.update(&bytes);
        }
        h.update(&.{@intFromBool(self.expected.is_called)});
        h.update(&self.request_id);
        switch (self.operation) {
            .reserve => h.update(&.{0}),
            .set => |value| {
                var bytes: [10]u8 = undefined;
                bytes[0] = 1;
                std.mem.writeInt(i64, bytes[1..9], value.value, .big);
                bytes[9] = @intFromBool(value.called);
                h.update(&bytes);
            },
        }
        return h.finalResult();
    }
};

pub const Receipt = struct {
    request_digest: [32]u8,
    state: sequence.State,
    lease: ?sequence.Lease,

    fn validate(self: Receipt, definition: sequence.Definition) !void {
        try self.state.validate(definition);
        if (std.mem.allEqual(u8, &self.request_digest, 0)) return error.InvalidSequenceReceipt;
        if (self.lease) |lease| {
            if (lease.generation != self.state.generation or lease.counter_revision != self.state.revision or
                lease.increment != definition.increment or !self.state.is_called or
                lease.first < definition.minimum or lease.first > definition.maximum or
                lease.count > definition.cache or lease.count == 0) return error.InvalidSequenceReceipt;
            if (try lease.value(lease.count - 1) != self.state.last_value) return error.InvalidSequenceReceipt;
        }
    }
};

pub const Decision = union(enum) { apply: Receipt, replay: Receipt };
pub const Admission = union(enum) { ready, applied: Receipt, unknown };

/// Failed/evicted retries cannot allocate twice: a missing receipt at an old
/// CAS revision is a conflict, never permission to recompute from new state.
pub fn prepare(key: Key, definition: sequence.Definition, current: sequence.State, request: Request, previous: ?Receipt) !Decision {
    const digest = try request.digest(key);
    try current.validate(definition);
    if (request.expected.generation != definition.generation) return error.SequenceCatalogChanged;
    try request.expected.validate(definition);
    const next_revision = std.math.add(u64, request.expected.revision, 1) catch return error.CatalogGenerationExhausted;
    if (previous) |receipt| {
        try receipt.validate(definition);
        if (receipt.state.revision != next_revision or receipt.state.revision > current.revision)
            return error.InvalidSequenceReceipt;
        if (receipt.state.revision == current.revision and !std.meta.eql(receipt.state, current)) return error.InvalidSequenceReceipt;
        if (!std.mem.eql(u8, &digest, &receipt.request_digest)) return error.SequenceCounterChanged;
        // Bounds and endpoints alone permit a forged boundary lease to
        // expand backward into an earlier allocation. Verify the exact pure
        // transition from the original request-bound preimage, not today's
        // counter. This remains allocation-free and needs no history scan.
        const expected = transition(definition, request, digest) catch return error.InvalidSequenceReceipt;
        if (!std.meta.eql(receipt, expected)) return error.InvalidSequenceReceipt;
        return .{ .replay = receipt };
    }
    if (!std.meta.eql(current, request.expected)) return error.SequenceCounterChanged;
    return .{ .apply = try transition(definition, request, digest) };
}

fn transition(definition: sequence.Definition, request: Request, digest: [32]u8) !Receipt {
    return switch (request.operation) {
        .reserve => blk: {
            const result = try request.expected.reserve(definition);
            break :blk .{ .request_digest = digest, .state = result.state, .lease = result.lease };
        },
        .set => |value| .{ .request_digest = digest, .state = try request.expected.set(definition, value.value, value.called), .lease = null },
    };
}

const key_bytes = prefix.len + 25;
pub const StoredKey = struct { key: Key, revision: u64 };

pub fn counterKey(key: Key) ![key_bytes]u8 {
    return storageKey(key, 0);
}

pub fn storedKey(bytes: []const u8) !?StoredKey {
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    if (bytes.len != key_bytes) return error.InvalidSequenceState;
    const key: Key = .{ .group = std.mem.readInt(u64, bytes[prefix.len..][0..8], .big), .sequence_id = std.mem.readInt(u64, bytes[prefix.len + 8 ..][0..8], .big) };
    const revision = std.mem.readInt(u64, bytes[prefix.len + 17 ..][0..8], .big);
    if (key.group == 0 or key.sequence_id == 0 or bytes[prefix.len + 16] != @as(u8, if (revision == 0) 0 else 1)) return error.InvalidSequenceState;
    return .{ .key = key, .revision = revision };
}

pub fn receiptBytes(a: A, bytes: []const u8, definition: sequence.Definition) !Receipt {
    var value = try decode(Receipt, a, bytes);
    defer value.deinit();
    try value.value.validate(definition);
    return value.value;
}

fn storageKey(key: Key, revision: u64) ![key_bytes]u8 {
    if (key.group == 0 or key.sequence_id == 0) return error.InvalidSequenceRequest;
    var result: [key_bytes]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.mem.writeInt(u64, result[prefix.len..][0..8], key.group, .big);
    std.mem.writeInt(u64, result[prefix.len + 8 ..][0..8], key.sequence_id, .big);
    result[prefix.len + 16] = if (revision == 0) 0 else 1;
    std.mem.writeInt(u64, result[prefix.len + 17 ..][0..8], revision, .big);
    return result;
}

/// All puts/deletes are provisional in the caller's one atomic transaction.
/// Counters never update the catalog schema revision. Space is bounded by one
/// counter and 64 small receipts per live sequence, irrespective of churn.
pub fn Store(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        key: Key,

        pub fn load(self: @This(), a: A, definition: sequence.Definition) !?sequence.State {
            const bytes = self.txn.get(&try storageKey(self.key, 0)) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            var value = try decode(CounterRecord, a, bytes);
            defer value.deinit();
            if (value.value.format != 1 or !std.meta.eql(self.key, value.value.identity)) return error.InvalidSequenceState;
            if (value.value.state.generation != 0 and value.value.state.generation != definition.generation) return error.SequenceCatalogChanged;
            try value.value.state.validate(definition);
            return value.value.state;
        }

        pub fn initialize(self: @This(), a: A, record: sequence.Record) !void {
            if (record.definition.generation != 1 or record.state.revision != 1 or record.state.is_called) return error.InvalidSequenceDefinition;
            try record.state.validate(record.definition);
            // Check presence without interpreting a predecessor using the new
            // definition: an existing identity must never be reinitialized.
            if (self.txn.get(&try storageKey(self.key, 0))) |_| {
                return error.SequenceCounterChanged;
            } else |err| if (err != error.NotFound) return err;
            try self.put(a, 0, record.state);
        }

        fn decision(self: @This(), a: A, definition: sequence.Definition, request: Request) !Decision {
            _ = try request.digest(self.key);
            if (definition.generation != request.expected.generation) return error.SequenceCatalogChanged;
            const current = (try self.load(a, definition)) orelse return error.SequenceNotFound;
            const previous = try self.loadReceipt(a, request);
            return prepare(self.key, definition, current, request, previous);
        }

        /// A missing old receipt cannot prove nonadmission. A retained slot
        /// owned by a different digest, however, proves this CAS did not win.
        /// Both results need only two point reads, never a mutation/history scan.
        pub fn admission(self: @This(), a: A, definition: sequence.Definition, request: Request) !Admission {
            _ = try request.digest(self.key);
            if (definition.generation != request.expected.generation) return error.SequenceCatalogChanged;
            const current = (try self.load(a, definition)) orelse return .unknown;
            const previous = try self.loadReceipt(a, request);
            if (previous == null and !std.meta.eql(current, request.expected)) return .unknown;
            return switch (try prepare(self.key, definition, current, request, previous)) {
                .apply => .ready,
                .replay => |value| .{ .applied = value },
            };
        }

        pub fn execute(self: @This(), a: A, definition: sequence.Definition, request: Request) !Receipt {
            switch (try self.decision(a, definition, request)) {
                .replay => |value| return value,
                .apply => |value| {
                    try self.put(a, 0, value.state);
                    try self.put(a, value.state.revision, value);
                    try self.prune(value.state.revision);
                    return value;
                },
            }
        }

        /// Observe one exact allocation without admitting a writer or
        /// reserving another lease. Missing/evicted results are unknown, never
        /// permission to reserve with a fresh request identity. This method
        /// requires only get(), so it can run on a pinned read-only owner.
        pub fn observe(self: @This(), a: A, definition: sequence.Definition, request: Request) !?Receipt {
            _ = try request.digest(self.key);
            if (definition.generation != request.expected.generation) return error.SequenceCatalogChanged;
            const current = (try self.load(a, definition)) orelse return null;
            const previous_receipt = (try self.loadReceipt(a, request)) orelse return null;
            return switch (try prepare(self.key, definition, current, request, previous_receipt)) {
                .replay => |value| value,
                // A present receipt must authenticate this exact operation.
                .apply => unreachable,
            };
        }

        fn loadReceipt(self: @This(), a: A, request: Request) !?Receipt {
            const next_revision = std.math.add(u64, request.expected.revision, 1) catch return error.CatalogGenerationExhausted;
            const bytes = self.txn.get(&try storageKey(self.key, next_revision)) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            var value = try decode(Receipt, a, bytes);
            defer value.deinit();
            return value.value;
        }

        /// Invoked alongside the catalog definition update. A stale ALTER or
        /// RESTART cannot overwrite a lease allocated since its admission cut.
        pub fn alter(self: @This(), a: A, prior: sequence.Record, options: sequence.Options) !sequence.Record {
            const next = try prior.alter(options);
            try prior.state.validate(prior.definition);
            try next.state.validate(next.definition);
            const current = (try self.load(a, prior.definition)) orelse return error.SequenceNotFound;
            if (!std.meta.eql(current, prior.state)) return error.SequenceCounterChanged;
            if (std.meta.eql(prior.state, next.state)) {
                if (!prior.definition.eql(next.definition)) return error.InvalidSequenceDefinition;
                return next;
            }
            if (next.definition.generation != try std.math.add(u64, prior.definition.generation, 1) or
                next.state.revision != try std.math.add(u64, current.revision, 1)) return error.InvalidSequenceDefinition;
            try self.put(a, 0, next.state);
            // Old-generation receipts can no longer authorize replay. Retire
            // them in this same bounded transaction, so restore never needs
            // an unavailable old definition to interpret obsolete leases.
            try self.clearReceipts(current.revision);
            return next;
        }

        /// DROP is bounded too; no full-catalog or unbounded receipt scan.
        pub fn remove(self: @This(), a: A, definition: sequence.Definition) !void {
            const current = (try self.load(a, definition)) orelse return error.SequenceNotFound;
            try self.clearReceipts(current.revision);
            try self.erase(0);
        }

        fn clearReceipts(self: @This(), latest: u64) !void {
            const first = if (latest > retained_receipts) latest - retained_receipts + 1 else 1;
            var revision = first;
            while (true) : (revision += 1) {
                try self.erase(revision);
                if (revision == latest) break;
            }
        }

        fn put(self: @This(), a: A, revision: u64, value: anytype) !void {
            const bytes = if (@TypeOf(value) == sequence.State)
                try std.json.Stringify.valueAlloc(a, CounterRecord{ .format = 1, .identity = self.key, .state = value }, .{})
            else
                try std.json.Stringify.valueAlloc(a, value, .{});
            defer a.free(bytes);
            try self.txn.put(&try storageKey(self.key, revision), bytes);
        }
        fn erase(self: @This(), revision: u64) !void {
            self.txn.delete(&try storageKey(self.key, revision)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        fn prune(self: @This(), revision: u64) !void {
            if (revision > retained_receipts) try self.erase(revision - retained_receipts);
        }
    };
}

fn decode(comptime T: type, a: A, bytes: []const u8) !std.json.Parsed(T) {
    if (bytes.len > 2048) return error.InvalidSequenceState;
    return std.json.parseFromSlice(T, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidSequenceState,
    };
}

test "sequence catalog journal lost replies replay exactly without allocating again" {
    const t = std.testing;
    const key: Key = .{ .group = 3, .sequence_id = 19 };
    const record = try sequence.Record.create(.{ .cache = 32 });
    const request: Request = .{ .expected = record.state, .request_id = @splat(1), .operation = .reserve };
    const committed = (try prepare(key, record.definition, record.state, request, null)).apply;
    try t.expectEqual(@as(i64, 32), committed.state.last_value);
    try t.expectEqualDeep(committed, (try prepare(key, record.definition, committed.state, request, committed)).replay);
    try t.expectError(error.SequenceCounterChanged, prepare(key, record.definition, committed.state, request, null));
    var substituted = request;
    substituted.operation = .{ .set = .{ .value = 500, .called = false } };
    try t.expectError(error.SequenceCounterChanged, prepare(key, record.definition, committed.state, substituted, committed));
    try t.expectError(error.SequenceCounterChanged, prepare(.{ .group = 4, .sequence_id = 19 }, record.definition, committed.state, request, committed));
    try t.expectError(error.SequenceCounterChanged, prepare(.{ .group = 3, .sequence_id = 20 }, record.definition, committed.state, request, committed));
    for ([_]bool{ false, true }) |called_change| {
        var wrong_preimage = request;
        if (called_change) wrong_preimage.expected.is_called = true else wrong_preimage.expected.last_value += 1;
        try t.expectError(error.SequenceCounterChanged, prepare(key, record.definition, record.state, wrong_preimage, null));
        try t.expectError(error.SequenceCounterChanged, prepare(key, record.definition, committed.state, wrong_preimage, committed));
    }
    var changed = record;
    changed.state = committed.state;
    changed = try changed.alter(.{ .restart = .{ .value = 100 } });
    try t.expectError(error.SequenceCatalogChanged, prepare(key, changed.definition, changed.state, request, committed));
}

test "sequence catalog journal rejects malformed receipts and invalid request identities" {
    const t = std.testing;
    const key: Key = .{ .group = 3, .sequence_id = 19 };
    const record = try sequence.Record.create(.{});
    var request: Request = .{ .expected = record.state, .request_id = @splat(1), .operation = .reserve };
    var receipt = (try prepare(key, record.definition, record.state, request, null)).apply;
    receipt.lease.?.first = 2;
    try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, receipt.state, request, receipt));
    receipt = (try prepare(key, record.definition, record.state, request, null)).apply;
    try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, record.state, request, receipt));
    request.request_id = @splat(0);
    try t.expectError(error.InvalidSequenceRequest, prepare(key, record.definition, record.state, request, null));
    request.request_id = @splat(1);
    request.expected.revision = std.math.maxInt(u64);
    try t.expectError(error.CatalogGenerationExhausted, prepare(key, record.definition, record.state, request, null));
}

test "sequence catalog journal binds historical replay payload to its operation" {
    const t = std.testing;
    const key: Key = .{ .group = 3, .sequence_id = 19 };
    const record = try sequence.Record.create(.{ .cache = 8 });
    const reserve: Request = .{ .expected = record.state, .request_id = @splat(1), .operation = .reserve };
    const lease = (try prepare(key, record.definition, record.state, reserve, null)).apply;
    // Advance the current counter so the current-state equality check cannot
    // mask damage to an older, otherwise valid receipt.
    const advance: Request = .{ .expected = lease.state, .request_id = @splat(2), .operation = .reserve };
    const current = (try prepare(key, record.definition, lease.state, advance, null)).apply.state;
    try t.expectEqualDeep(lease, (try prepare(key, record.definition, current, reserve, lease)).replay);
    var damaged = lease;
    damaged.lease = null;
    try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, current, reserve, damaged));

    for ([_]bool{ false, true }) |called| {
        const set: Request = .{ .expected = record.state, .request_id = @splat(3), .operation = .{ .set = .{ .value = 8, .called = called } } };
        const committed = (try prepare(key, record.definition, record.state, set, null)).apply;
        var followup = advance;
        followup.expected = committed.state;
        const later = (try prepare(key, record.definition, committed.state, followup, null)).apply.state;
        try t.expectEqualDeep(committed, (try prepare(key, record.definition, later, set, committed)).replay);
        damaged = committed;
        damaged.state.last_value = 9;
        try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, later, set, damaged));
        damaged = committed;
        damaged.state.is_called = !called;
        try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, later, set, damaged));
        if (called) {
            damaged = committed;
            damaged.lease = lease.lease;
            try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, later, set, damaged));
        }
    }
}

test "sequence catalog journal historical boundary leases cannot expand into earlier allocations" {
    const t = std.testing;
    const key: Key = .{ .group = 3, .sequence_id = 19 };
    for ([_]i64{ 1, -1 }) |direction| {
        const record = try sequence.Record.create(.{ .increment = direction, .minimum = .{ .value = if (direction > 0) 1 else -10 }, .maximum = .{ .value = if (direction > 0) 10 else -1 }, .cache = 8, .cycle = true });
        const first_request: Request = .{ .expected = record.state, .request_id = @splat(1), .operation = .reserve };
        const first = (try prepare(key, record.definition, record.state, first_request, null)).apply;
        const second_request: Request = .{ .expected = first.state, .request_id = @splat(2), .operation = .reserve };
        const second = (try prepare(key, record.definition, first.state, second_request, null)).apply;
        try t.expectEqual(direction * 9, second.lease.?.first);
        try t.expectEqual(@as(u64, 2), second.lease.?.count);
        const third_request: Request = .{ .expected = second.state, .request_id = @splat(3), .operation = .reserve };
        const third = (try prepare(key, record.definition, second.state, third_request, null)).apply;
        var damaged = second;
        damaged.lease.?.first = direction * 3;
        damaged.lease.?.count = 8;
        // Digest, bounds, endpoint, revision and call-state all still match. The
        // original preimage must reject values 3..8 from an earlier allocation.
        try t.expectError(error.InvalidSequenceReceipt, prepare(key, record.definition, third.state, second_request, damaged));
        try t.expectEqualDeep(second, (try prepare(key, record.definition, third.state, second_request, second)).replay);
    }
}
