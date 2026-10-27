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

//! Allocation-free PostgreSQL sequence transitions. Definitions belong to the
//! catalog; counters and idempotent lease receipts belong to the same metadata
//! transaction but not its schema revision. These pure values grant no write
//! authority: the existing Raft owner must validate identity and commit them.
const std = @import("std");

pub const Integer = enum {
    int16,
    int32,
    int64,

    pub fn minimum(self: Integer) i64 {
        return switch (self) {
            .int16 => std.math.minInt(i16),
            .int32 => std.math.minInt(i32),
            .int64 => std.math.minInt(i64),
        };
    }
    pub fn maximum(self: Integer) i64 {
        return switch (self) {
            .int16 => std.math.maxInt(i16),
            .int32 => std.math.maxInt(i32),
            .int64 => std.math.maxInt(i64),
        };
    }
};

pub const OwnedBy = struct {
    table_id: u64,
    /// Existing catalog attribute identity, never a column name or a layout
    /// ordinal. Rename cannot orphan ownership or bind a replacement column.
    column_id: u64,

    pub fn validate(self: OwnedBy) !void {
        if (self.table_id == 0 or self.column_id == 0) return error.InvalidSequenceDefinition;
    }
};

pub const Bound = union(enum) { unchanged, default, value: i64 };
pub const Ownership = union(enum) { unchanged, none, column: OwnedBy };
pub const Options = struct {
    integer: ?Integer = null,
    increment: ?i64 = null,
    minimum: Bound = .unchanged,
    maximum: Bound = .unchanged,
    start: ?i64 = null,
    cache: ?u64 = null,
    cycle: ?bool = null,
    owned_by: Ownership = .unchanged,
    /// Outer null leaves the running counter alone; an inner null implements
    /// RESTART without a value, using the resulting definition's START value.
    restart: ?struct { value: ?i64 = null } = null,
};

pub const Definition = struct {
    generation: u64 = 1,
    integer: Integer = .int64,
    increment: i64 = 1,
    minimum: i64 = 1,
    maximum: i64 = std.math.maxInt(i64),
    start: i64 = 1,
    cache: u64 = 1,
    cycle: bool = false,
    owned_by: ?OwnedBy = null,

    pub fn validate(self: Definition) !void {
        if (self.generation == 0 or self.increment == 0 or self.cache == 0 or self.cache > std.math.maxInt(i64) or
            self.minimum < self.integer.minimum() or self.maximum > self.integer.maximum() or
            self.minimum >= self.maximum or self.start < self.minimum or self.start > self.maximum)
            return error.InvalidSequenceDefinition;
        if (self.owned_by) |reference| try reference.validate();
    }

    fn defaults(self: Definition) struct { minimum: i64, maximum: i64 } {
        return if (self.increment > 0)
            .{ .minimum = 1, .maximum = self.integer.maximum() }
        else
            .{ .minimum = self.integer.minimum(), .maximum = -1 };
    }

    /// Creation and alteration share normalization; ALTER AS changes only an
    /// old type's default bounds, preserving explicit narrower bounds.
    pub fn prepare(prior: ?Definition, options: Options) !Definition {
        if (prior) |before| try before.validate();
        var result = prior orelse Definition{};
        if (options.integer) |kind| result.integer = kind;
        if (options.increment) |step| result.increment = step;
        if (result.increment == 0) return error.InvalidSequenceDefinition;
        const bounds = result.defaults();
        result.minimum = switch (options.minimum) {
            .value => |value| value,
            .default => bounds.minimum,
            .unchanged => if (prior) |before|
                (if (options.integer != null and before.minimum == before.integer.minimum()) bounds.minimum else before.minimum)
            else
                bounds.minimum,
        };
        result.maximum = switch (options.maximum) {
            .value => |value| value,
            .default => bounds.maximum,
            .unchanged => if (prior) |before|
                (if (options.integer != null and before.maximum == before.integer.maximum()) bounds.maximum else before.maximum)
            else
                bounds.maximum,
        };
        result.start = options.start orelse if (prior) |before| before.start else if (result.increment > 0) result.minimum else result.maximum;
        if (options.cache) |count| result.cache = count;
        if (options.cycle) |cycle| result.cycle = cycle;
        switch (options.owned_by) {
            .unchanged => {},
            .none => result.owned_by = null,
            .column => |reference| result.owned_by = reference,
        }
        try result.validate();
        return result;
    }

    pub fn eql(left: Definition, right: Definition) bool {
        if (left.generation != right.generation or left.integer != right.integer or left.increment != right.increment or
            left.minimum != right.minimum or left.maximum != right.maximum or left.start != right.start or
            left.cache != right.cache or left.cycle != right.cycle or (left.owned_by == null) != (right.owned_by == null)) return false;
        if (left.owned_by) |reference| return std.meta.eql(reference, right.owned_by.?);
        return true;
    }
};

pub const Lease = struct {
    generation: u64,
    counter_revision: u64,
    first: i64,
    increment: i64,
    count: u64,

    pub fn value(self: Lease, ordinal: u64) !i64 {
        if (self.generation == 0 or self.counter_revision == 0 or self.increment == 0 or self.count == 0 or
            self.count > std.math.maxInt(i64) or ordinal >= self.count) return error.InvalidSequenceLease;
        // The full signed-64 domain, negative increments and huge CACHE
        // values must never pass through floating point or overflow i64.
        return std.math.cast(i64, @as(i128, self.first) + @as(i128, self.increment) * ordinal) orelse error.InvalidSequenceLease;
    }
};

pub const State = struct {
    generation: u64,
    revision: u64,
    last_value: i64,
    is_called: bool,

    pub fn init(definition: Definition) !State {
        try definition.validate();
        return .{ .generation = definition.generation, .revision = 1, .last_value = definition.start, .is_called = false };
    }

    pub fn validate(self: State, definition: Definition) !void {
        try definition.validate();
        if (self.generation != definition.generation or self.revision == 0 or
            self.last_value < definition.minimum or self.last_value > definition.maximum) return error.InvalidSequenceState;
    }

    /// Reserve a compact arithmetic progression, ending at a bound rather
    /// than cycling inside a lease. A later reservation performs the wrap.
    /// Neither this input nor the catalog definition changes before commit.
    pub fn reserve(self: State, definition: Definition) !struct { state: State, lease: Lease } {
        try self.validate(definition);
        var first: i128 = self.last_value;
        if (self.is_called) first += definition.increment;
        if (first < definition.minimum or first > definition.maximum) {
            if (!definition.cycle) return error.SequenceGeneratorLimitExceeded;
            first = if (definition.increment > 0) definition.minimum else definition.maximum;
        }
        const remaining: u128 = @intCast(if (definition.increment > 0)
            @divTrunc(@as(i128, definition.maximum) - first, definition.increment) + 1
        else
            @divTrunc(first - definition.minimum, -@as(i128, definition.increment)) + 1);
        const count: u64 = @intCast(@min(@as(u128, definition.cache), remaining));
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.CatalogGenerationExhausted;
        const lease: Lease = .{ .generation = self.generation, .counter_revision = next.revision, .first = @intCast(first), .increment = definition.increment, .count = count };
        next.last_value = try lease.value(count - 1);
        next.is_called = true;
        return .{ .state = next, .lease = lease };
    }

    pub fn set(self: State, definition: Definition, value: i64, called: bool) !State {
        try self.validate(definition);
        if (value < definition.minimum or value > definition.maximum) return error.SequenceValueOutOfRange;
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.CatalogGenerationExhausted;
        next.last_value = value;
        next.is_called = called;
        return next;
    }
};

/// Catalog-only immutable definition. Every persisted field is mandatory;
/// decoding must not invent defaults which change an existing allocator.
/// Mutable counter state is stored separately in the same metadata owner.
pub const Descriptor = struct {
    format: u8,
    id: u64,
    generation: u64,
    integer: Integer,
    increment: i64,
    minimum: i64,
    maximum: i64,
    start: i64,
    cache: u64,
    cycle: bool,
    owned_by: ?OwnedBy,

    pub fn init(id: u64, value: Definition) !Descriptor {
        try value.validate();
        if (id == 0) return error.InvalidSequenceDefinition;
        return .{ .format = 1, .id = id, .generation = value.generation, .integer = value.integer, .increment = value.increment, .minimum = value.minimum, .maximum = value.maximum, .start = value.start, .cache = value.cache, .cycle = value.cycle, .owned_by = value.owned_by };
    }
    pub fn definition(self: Descriptor) !Definition {
        if (self.format != 1 or self.id == 0) return error.InvalidSequenceDefinition;
        const value: Definition = .{ .generation = self.generation, .integer = self.integer, .increment = self.increment, .minimum = self.minimum, .maximum = self.maximum, .start = self.start, .cache = self.cache, .cycle = self.cycle, .owned_by = self.owned_by };
        try value.validate();
        return value;
    }
    pub fn encode(self: Descriptor, a: std.mem.Allocator) ![]u8 {
        _ = try self.definition();
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: std.mem.Allocator, bytes: []const u8) !Descriptor {
        if (bytes.len > 2048) return error.InvalidSequenceDefinition;
        var parsed = std.json.parseFromSlice(Descriptor, a, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSequenceDefinition,
        };
        defer parsed.deinit();
        _ = try parsed.value.definition();
        return parsed.value;
    }
};

pub const Record = struct {
    definition: Definition,
    state: State,

    pub fn create(options: Options) !Record {
        const definition = try Definition.prepare(null, options);
        var state = try State.init(definition);
        // PostgreSQL permits RESTART at creation too. START remains the
        // definition's future reset value, not necessarily its initial counter.
        if (options.restart) |restart| state.last_value = restart.value orelse definition.start;
        if (state.last_value < definition.minimum or state.last_value > definition.maximum) return error.InvalidSequenceDefinition;
        return .{ .definition = definition, .state = state };
    }

    pub fn alter(self: Record, options: Options) !Record {
        try self.state.validate(self.definition);
        var definition = try Definition.prepare(self.definition, options);
        if (options.restart == null and definition.eql(self.definition)) return self;
        definition.generation = std.math.add(u64, self.definition.generation, 1) catch return error.CatalogGenerationExhausted;
        var state = self.state;
        state.generation = definition.generation;
        state.revision = std.math.add(u64, state.revision, 1) catch return error.CatalogGenerationExhausted;
        if (options.restart) |restart| {
            state.last_value = restart.value orelse definition.start;
            state.is_called = false;
        }
        // A rejected ALTER is a definition error, not evidence that the
        // predecessor counter was corrupt. Preserve that diagnostic boundary.
        if (state.last_value < definition.minimum or state.last_value > definition.maximum)
            return error.InvalidSequenceDefinition;
        try state.validate(definition);
        return .{ .definition = definition, .state = state };
    }
};

test "sequence catalog descriptors preserve exact definitions and require all persisted fields" {
    const a = std.testing.allocator;
    const value = try Descriptor.init(19, .{ .cache = 32, .owned_by = .{ .table_id = 7, .column_id = 3 } });
    const bytes = try value.encode(a);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(value, try Descriptor.decode(a, bytes));
    const fields = [_][]const u8{ "format", "id", "generation", "integer", "increment", "minimum", "maximum", "start", "cache", "cycle", "owned_by" };
    for (fields) |field| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer parsed.deinit();
        try std.testing.expect(parsed.value.object.swapRemove(field));
        const missing = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(missing);
        try std.testing.expectError(error.InvalidSequenceDefinition, Descriptor.decode(a, missing));
    }
    var malformed = value;
    malformed.generation = 0;
    try std.testing.expectError(error.InvalidSequenceDefinition, malformed.encode(a));
}

test "sequence catalog counters reserve exact full-width bounded progressions without allocations" {
    const t = std.testing;
    var record = try Record.create(.{ .cache = 32 });
    const first = try record.state.reserve(record.definition);
    try t.expectEqual(@as(i64, 1), try first.lease.value(0));
    try t.expectEqual(@as(i64, 32), first.state.last_value);
    try t.expectEqual(@as(i64, 1), record.state.last_value);
    record.state = first.state;
    const second = try record.state.reserve(record.definition);
    try t.expectEqual(@as(i64, 33), second.lease.first);
    try t.expectEqual(@as(i64, 64), second.state.last_value);
    const huge = try Record.create(.{ .increment = std.math.minInt(i64), .minimum = .{ .value = std.math.minInt(i64) }, .maximum = .{ .value = std.math.maxInt(i64) }, .start = std.math.maxInt(i64), .cache = std.math.maxInt(i64) });
    const lease = try huge.state.reserve(huge.definition);
    try t.expectEqual(@as(u64, 2), lease.lease.count);
    try t.expectEqual(@as(i64, -1), try lease.lease.value(1));
    try t.expectError(error.SequenceGeneratorLimitExceeded, lease.state.reserve(huge.definition));
    try t.expectError(error.InvalidSequenceLease, lease.lease.value(2));
}

test "sequence catalog bounds cache exhaustion and descending cycles preserve state" {
    const t = std.testing;
    for ([_]bool{ false, true }) |descending| {
        const record = try Record.create(.{ .increment = if (descending) -3 else 3, .minimum = .{ .value = 1 }, .maximum = .{ .value = 10 }, .cache = 32 });
        const lease = try record.state.reserve(record.definition);
        try t.expectEqual(@as(u64, 4), lease.lease.count);
        try t.expectEqual(@as(i64, if (descending) 10 else 1), lease.lease.first);
        try t.expectEqual(@as(i64, if (descending) 1 else 10), lease.state.last_value);
        try t.expectError(error.SequenceGeneratorLimitExceeded, lease.state.reserve(record.definition));
        var cycling = record.definition;
        cycling.cycle = true;
        const wrapped = try lease.state.reserve(cycling);
        try t.expectEqual(lease.lease.first, wrapped.lease.first);
        try t.expectEqual(lease.lease.count, wrapped.lease.count);
    }
    const negative = try Record.create(.{ .integer = .int16, .increment = -1 });
    try t.expectEqual(@as(i64, -32768), negative.definition.minimum);
    try t.expectEqual(@as(i64, -1), negative.definition.maximum);
    try t.expectEqual(@as(i64, -1), negative.state.last_value);
}

test "sequence catalog ALTER START is not RESTART and narrowing rejects allocated values" {
    const t = std.testing;
    var record = try Record.create(.{});
    record = try record.alter(.{ .start = 5 });
    try t.expectEqual(@as(i64, 1), (try record.state.reserve(record.definition)).lease.first);
    const generation = record.definition.generation;
    try t.expectEqualDeep(record, try record.alter(.{ .cache = 1 }));
    record = try record.alter(.{ .restart = .{} });
    try t.expectEqual(generation + 1, record.definition.generation);
    try t.expectEqual(@as(i64, 5), (try record.state.reserve(record.definition)).lease.first);
    record.state = try record.state.set(record.definition, 60000, true);
    try t.expectError(error.InvalidSequenceDefinition, record.alter(.{ .integer = .int16 }));
    try t.expectError(error.InvalidSequenceDefinition, record.alter(.{ .maximum = .{ .value = 10 } }));
    const restarted = try record.alter(.{ .integer = .int16, .restart = .{ .value = 10 } });
    try t.expectEqual(@as(i64, 10), restarted.state.last_value);
    try t.expectEqual(@as(i64, 32767), restarted.definition.maximum);
    try t.expectError(error.InvalidSequenceState, record.state.validate(restarted.definition));
}

test "sequence catalog rejects malformed definitions leases and exhausted revisions before mutation" {
    const t = std.testing;
    for ([_]Options{ .{ .increment = 0 }, .{ .cache = 0 }, .{ .cache = std.math.maxInt(u64) }, .{ .minimum = .{ .value = 5 }, .maximum = .{ .value = 5 } }, .{ .integer = .int16, .start = 32768 }, .{ .owned_by = .{ .column = .{ .table_id = 0, .column_id = 1 } } }, .{ .owned_by = .{ .column = .{ .table_id = 1, .column_id = 0 } } }, .{ .restart = .{ .value = 0 } } }) |options|
        try t.expectError(error.InvalidSequenceDefinition, Record.create(options));
    var record = try Record.create(.{});
    try t.expectError(error.SequenceValueOutOfRange, record.state.set(record.definition, 0, true));
    record.state.revision = std.math.maxInt(u64);
    try t.expectError(error.CatalogGenerationExhausted, record.state.reserve(record.definition));
    try t.expectError(error.CatalogGenerationExhausted, record.alter(.{ .cache = 32 }));
    var malformed: Lease = .{ .generation = 1, .counter_revision = 2, .first = std.math.maxInt(i64), .increment = 1, .count = 2 };
    try t.expectError(error.InvalidSequenceLease, malformed.value(1));
    malformed.count = 0;
    try t.expectError(error.InvalidSequenceLease, malformed.value(0));
}
