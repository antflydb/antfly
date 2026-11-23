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

//! Logical default authority against a native, pinned counter definition.
//! These checks grant neither catalog access nor permission to construct that
//! definition. The accepting writer derives counters and replacement overlays.
const std = @import("std");
const local = @import("antfly_local_sources");
const sequence = local.system_catalog_sequences;
const attributes = local.system_catalog_column_identities;
const uses = @import("sequence_default_dependencies.zig");

pub const Counter = struct {
    id: u64,
    namespace_id: u64,
    definition: sequence.Definition,

    pub fn validate(self: Counter) !void {
        if (self.id == 0 or self.namespace_id == 0) return error.InvalidSequenceState;
        self.definition.validate() catch return error.InvalidSequenceState;
    }
};

/// Immutable postimages from an already validated containing catalog plan.
/// This is a lookup view, never authority to create a counter or move ownership.
pub const Overlay = struct {
    counters: []const Counter = &.{},

    /// The caller owns this bounded region throughout table preparation/apply.
    pub fn init(owned: []Counter) !Overlay {
        if (owned.len > 1024) return error.SqlProgramLimitExceeded;
        std.mem.sort(Counter, owned, {}, struct {
            fn less(_: void, left: Counter, right: Counter) bool {
                return left.id < right.id;
            }
        }.less);
        var previous: u64 = 0;
        for (owned) |counter| {
            try counter.validate();
            if (counter.id <= previous) return error.InvalidSequenceState;
            previous = counter.id;
        }
        return .{ .counters = owned };
    }
    pub fn get(self: Overlay, id: u64) ?Counter {
        var low: usize = 0;
        var high = self.counters.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const counter = self.counters[middle];
            if (counter.id == id) return counter;
            if (counter.id < id) low = middle + 1 else high = middle;
        }
        return null;
    }
};

/// A serial/ordinary default exercises ordinary sequence privileges, even if
/// its counter is owned by another attribute or is now unowned. Only a genuine
/// internal identity dependency can exercise implicit column authority.
pub fn validateReference(table: u64, reference: uses.Reference, counter: Counter) !void {
    try counter.validate();
    try reference.validate();
    if (table == 0 or reference.sequence_id != counter.id or reference.namespace_id != counter.namespace_id)
        return error.InvalidSequenceDefaultReferences;
    const same_owner = if (counter.definition.owned_by) |owner|
        owner.table_id == table and owner.column_id == reference.column_id
    else
        false;
    switch (reference.generation) {
        .serial => if (counter.definition.internal_identity and same_owner) return error.InvalidSequenceDefaultReferences,
        .always, .by_default => if (!counter.definition.internal_identity or !same_owner) return error.InvalidSequenceDefaultReferences,
    }
}

/// Generic schema writes cannot silently replace/drop an identity dependency
/// while retaining its attribute. Dropping the actual attribute is separately
/// admitted by native ownership/dependency retirement. Typed DROP IDENTITY
/// must use its own complete counter/default retirement cohort.
pub fn validateContinuity(next_attributes: attributes.State, before: []const uses.Reference, after: []const uses.Reference) !void {
    for (before) |old| {
        if (old.generation == .serial or next_attributes.columnById(old.column_id) == null) continue;
        const next = for (after) |candidate| {
            if (candidate.column_id == old.column_id) break candidate;
        } else return error.InvalidSequenceDefaultReferences;
        if (next.generation == .serial or next.sequence_id != old.sequence_id or next.namespace_id != old.namespace_id or
            !std.mem.eql(u8, &next.incarnation, &old.incarnation)) return error.InvalidSequenceDefaultReferences;
    }
}

test "sequence catalog native identity provenance separates borrowed defaults from implicit column authority" {
    const original: uses.Reference = .{ .column_id = 3, .sequence_id = 19, .namespace_id = 2, .incarnation = @splat(1), .generation = .serial };
    const owner: sequence.OwnedBy = .{ .table_id = 7, .column_id = 3 };
    for ([_]bool{ false, true }) |internal| for ([_]?sequence.OwnedBy{ null, owner, .{ .table_id = 8, .column_id = 3 } }) |owned| {
        const counter: Counter = .{ .id = 19, .namespace_id = 2, .definition = .{ .owned_by = owned, .internal_identity = internal } };
        for ([_]local.system_catalog_column_sequences.Generation{ .serial, .always, .by_default }) |mode| {
            var reference = original;
            reference.generation = mode;
            if (internal and owned == null) {
                try std.testing.expectError(error.InvalidSequenceState, validateReference(7, reference, counter));
            } else {
                const same = if (owned) |value| std.meta.eql(owner, value) else false;
                const allowed = if (mode == .serial) !(internal and same) else internal and same;
                if (allowed) try validateReference(7, reference, counter) else try std.testing.expectError(error.InvalidSequenceDefaultReferences, validateReference(7, reference, counter));
            }
        }
    };
    const counter: Counter = .{ .id = 19, .namespace_id = 2, .definition = .{} };
    var forged = original;
    forged.namespace_id = 9;
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, validateReference(7, forged, counter));
    forged = original;
    forged.sequence_id = 20;
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, validateReference(7, forged, counter));
}

test "sequence catalog native identity continuity rejects schema grant forgery and permits exact mode changes or attribute retirement" {
    const state: attributes.State = .{ .format = 1, .table_id = 7, .epoch = .{ .version = 1, .digest = @splat(1) }, .next_id = 4, .columns = &.{.{ .id = 3, .name = "n", .signature = @splat(2) }} };
    const original: uses.Reference = .{ .column_id = 3, .sequence_id = 19, .namespace_id = 2, .incarnation = @splat(1), .generation = .always };
    var next = original;
    next.generation = .by_default;
    try validateContinuity(state, &.{original}, &.{next});
    next.generation = .serial;
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, validateContinuity(state, &.{original}, &.{next}));
    next = original;
    next.sequence_id += 1;
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, validateContinuity(state, &.{original}, &.{next}));
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, validateContinuity(state, &.{original}, &.{}));
    var retired = state;
    retired.columns = &.{};
    try validateContinuity(retired, &.{original}, &.{});
    next = original;
    next.generation = .serial;
    try validateContinuity(state, &.{next}, &.{});
}

test "sequence catalog immutable pending definitions are bounded sorted unique and exact" {
    var counters = [_]Counter{ .{ .id = 19, .namespace_id = 2, .definition = .{} }, .{ .id = 7, .namespace_id = 3, .definition = .{} } };
    const overlay = try Overlay.init(&counters);
    try std.testing.expectEqual(@as(u64, 7), overlay.counters[0].id);
    try std.testing.expectEqual(@as(u64, 2), overlay.get(19).?.namespace_id);
    for ([_]u64{ 0, 6, 8, 20, std.math.maxInt(u64) }) |id| try std.testing.expect(overlay.get(id) == null);
    counters[1] = counters[0];
    try std.testing.expectError(error.InvalidSequenceState, Overlay.init(&counters));
}

test "sequence catalog identity defaults require durable NOT NULL while ordinary borrowed defaults permit nullable columns" {
    const a = std.testing.allocator;
    const schema = local.schema_mod;
    for ([_]local.system_catalog_column_sequences.Generation{ .serial, .by_default, .always }) |mode| for ([_]bool{ false, true }) |required| for ([_]bool{ false, true }) |nullable| {
        const binding: local.system_catalog_column_sequences.Binding = .{ .generation = mode, .incarnation = @splat(1), .sequence_id = 19, .namespace_id = 2, .owner = .{ .table_id = 7, .column_id = 3 }, .target = .{ .table = "counter" } };
        const text = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .storage_mode = "relational", .default_type = "row", .column_defaults = .{.{ .column = "n", .sequence = binding }}, .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = .{ .n = .{ .type = "integer", .nullable = nullable } }, .required = if (required) @as([]const []const u8, &.{"n"}) else &.{}, .additionalProperties = false } } } }, .{});
        defer a.free(text);
        if (mode != .serial and (!required or nullable)) {
            try std.testing.expectError(error.InvalidSchemaUpdateRequest, schema.parseValidatedTableSchema(a, text));
        } else {
            var parsed = try schema.parseValidatedTableSchema(a, text);
            defer parsed.deinit(a);
        }
    };
}
