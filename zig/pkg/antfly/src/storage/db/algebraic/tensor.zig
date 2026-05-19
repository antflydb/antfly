// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebra = @import("algebra.zig");
const law = @import("law.zig");
const symbol = @import("symbol.zig");
const token = @import("token.zig");

pub const namespace_version = "v4";

pub const Coordinate = struct {
    materialization: []const u8,
    axes_canonical: []const u8,
    bucket: ?[]const u8 = null,
};

pub const ExpressionCoordinate = struct {
    expr_id: []const u8,
    axes_canonical: []const u8,
    bucket: ?[]const u8 = null,
};

pub fn keyAlloc(alloc: Allocator, index_name: []const u8, coordinate: Coordinate) ![]u8 {
    const axis_id = symbol.id(coordinate.axes_canonical);
    const axis_text = try symbol.idTextAlloc(alloc, axis_id[0..]);
    defer alloc.free(axis_text);
    if (coordinate.bucket) |bucket| {
        return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, namespace_version, "tensor", coordinate.materialization, bucket, axis_text });
    }
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, namespace_version, "tensor", coordinate.materialization, axis_text });
}

pub fn expressionKeyAlloc(alloc: Allocator, index_name: []const u8, coordinate: ExpressionCoordinate) ![]u8 {
    const axis_id = symbol.id(coordinate.axes_canonical);
    const axis_text = try symbol.idTextAlloc(alloc, axis_id[0..]);
    defer alloc.free(axis_text);
    if (coordinate.bucket) |bucket| {
        return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, namespace_version, "materialized_expr", coordinate.expr_id, bucket, axis_text });
    }
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, namespace_version, "materialized_expr", coordinate.expr_id, axis_text });
}

pub fn expressionPrefixAlloc(alloc: Allocator, index_name: []const u8, expr_id: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, namespace_version, "materialized_expr", expr_id });
}

pub const Slot = struct {
    law_id: law.Id,
    value: ?[]u8 = null,

    pub fn deinit(self: *Slot, alloc: Allocator) void {
        if (self.value) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

pub const Row = struct {
    slots: []Slot,

    pub fn deinit(self: *Row, alloc: Allocator) void {
        for (self.slots) |*slot| slot.deinit(alloc);
        if (self.slots.len > 0) alloc.free(self.slots);
        self.* = undefined;
    }
};

pub fn rowAlloc(alloc: Allocator, law_ids: []const law.Id) !Row {
    const slots = try alloc.alloc(Slot, law_ids.len);
    errdefer alloc.free(slots);
    for (law_ids, 0..) |law_id, i| slots[i] = .{ .law_id = law_id };
    return .{ .slots = slots };
}

pub fn rowFromBytesOrLayoutAlloc(alloc: Allocator, encoded: ?[]const u8, law_ids: []const law.Id) !Row {
    if (encoded) |bytes| {
        var row = try decodeRowAlloc(alloc, bytes);
        errdefer row.deinit(alloc);
        try validateRowLayout(row, law_ids);
        return row;
    }
    return try rowAlloc(alloc, law_ids);
}

pub fn validateRowLayout(row: Row, law_ids: []const law.Id) !void {
    if (row.slots.len != law_ids.len) return error.InvalidAlgebraicTensorRow;
    for (row.slots, law_ids) |slot, expected| {
        if (slot.law_id != expected) return error.InvalidAlgebraicTensorRow;
    }
}

pub fn applySlot(alloc: Allocator, slot: *Slot, delta: []const u8) !void {
    const next = try law.combineAlloc(alloc, slot.law_id, slot.value, delta);
    if (slot.value) |old| alloc.free(old);
    slot.value = next;
}

pub fn applyRowSlot(alloc: Allocator, row: *Row, slot_idx: usize, expected_law_id: law.Id, delta: []const u8) !void {
    if (slot_idx >= row.slots.len) return error.InvalidAlgebraicTensorRow;
    if (row.slots[slot_idx].law_id != expected_law_id) return error.InvalidAlgebraicTensorRow;
    try applySlot(alloc, &row.slots[slot_idx], delta);
}

pub const SlotDelta = struct {
    slot_idx: usize,
    law_id: law.Id,
    delta: []const u8,
};

pub fn replaceRowSlotOwned(alloc: Allocator, row: *Row, slot_idx: usize, expected_law_id: law.Id, next: ?[]u8) !void {
    errdefer if (next) |bytes| alloc.free(bytes);
    if (slot_idx >= row.slots.len) return error.InvalidAlgebraicTensorRow;
    if (row.slots[slot_idx].law_id != expected_law_id) return error.InvalidAlgebraicTensorRow;
    if (row.slots[slot_idx].value) |old| alloc.free(old);
    row.slots[slot_idx].value = next;
}

pub fn hasValues(row: Row) bool {
    for (row.slots) |slot| {
        if (slot.value != null) return true;
    }
    return false;
}

pub fn rowSlotValueAlloc(
    alloc: Allocator,
    existing: ?[]const u8,
    law_ids: []const law.Id,
    slot_idx: usize,
    expected_law_id: law.Id,
) !?[]u8 {
    var row = try rowFromBytesOrLayoutAlloc(alloc, existing, law_ids);
    defer row.deinit(alloc);
    if (slot_idx >= row.slots.len) return error.InvalidAlgebraicTensorRow;
    if (row.slots[slot_idx].law_id != expected_law_id) return error.InvalidAlgebraicTensorRow;
    const value = row.slots[slot_idx].value orelse return null;
    return try alloc.dupe(u8, value);
}

pub fn applyRowSlotToBytesAlloc(
    alloc: Allocator,
    existing: ?[]const u8,
    law_ids: []const law.Id,
    slot_idx: usize,
    expected_law_id: law.Id,
    delta: []const u8,
    omit_empty: bool,
) !?[]u8 {
    var row = try rowFromBytesOrLayoutAlloc(alloc, existing, law_ids);
    defer row.deinit(alloc);
    try applyRowSlot(alloc, &row, slot_idx, expected_law_id, delta);
    if (omit_empty and !hasValues(row)) return null;
    return try encodeRowAlloc(alloc, row);
}

pub fn applyRowSlotsToBytesAlloc(
    alloc: Allocator,
    existing: ?[]const u8,
    law_ids: []const law.Id,
    deltas: []const SlotDelta,
    omit_empty: bool,
) !?[]u8 {
    var row = try rowFromBytesOrLayoutAlloc(alloc, existing, law_ids);
    defer row.deinit(alloc);
    for (deltas) |delta| {
        try applyRowSlot(alloc, &row, delta.slot_idx, delta.law_id, delta.delta);
    }
    if (omit_empty and !hasValues(row)) return null;
    return try encodeRowAlloc(alloc, row);
}

pub fn replaceRowSlotToBytesAlloc(
    alloc: Allocator,
    existing: ?[]const u8,
    law_ids: []const law.Id,
    slot_idx: usize,
    expected_law_id: law.Id,
    next: ?[]u8,
    omit_empty: bool,
) !?[]u8 {
    var row = try rowFromBytesOrLayoutAlloc(alloc, existing, law_ids);
    defer row.deinit(alloc);
    try replaceRowSlotOwned(alloc, &row, slot_idx, expected_law_id, next);
    if (omit_empty and !hasValues(row)) return null;
    return try encodeRowAlloc(alloc, row);
}

pub fn mergeRowsAlloc(alloc: Allocator, left: Row, right: Row) !Row {
    if (left.slots.len != right.slots.len) return error.IncompatibleAlgebraicTensorRows;
    var out = try alloc.alloc(Slot, left.slots.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*slot| slot.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (left.slots, right.slots, 0..) |left_slot, right_slot, i| {
        if (left_slot.law_id != right_slot.law_id) return error.IncompatibleAlgebraicTensorRows;
        out[i] = .{
            .law_id = left_slot.law_id,
            .value = try law.combineAlloc(alloc, left_slot.law_id, left_slot.value, right_slot.value),
        };
        filled = i + 1;
    }
    return .{ .slots = out };
}

pub fn mergeOneSlotValuesAlloc(
    alloc: Allocator,
    law_id: law.Id,
    left: ?[]const u8,
    right: ?[]const u8,
) ![]u8 {
    var left_row = try rowAlloc(alloc, &.{law_id});
    defer left_row.deinit(alloc);
    if (left) |value| left_row.slots[0].value = try alloc.dupe(u8, value);

    var right_row = try rowAlloc(alloc, &.{law_id});
    defer right_row.deinit(alloc);
    if (right) |value| right_row.slots[0].value = try alloc.dupe(u8, value);

    var merged = try mergeRowsAlloc(alloc, left_row, right_row);
    defer merged.deinit(alloc);
    const value = merged.slots[0].value orelse return try law.identityAlloc(alloc, law_id);
    return try alloc.dupe(u8, value);
}

pub fn encodeRowAlloc(alloc: Allocator, row: Row) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, "tensor-row:v1");
    for (row.slots) |slot| {
        try parts.append(alloc, @tagName(slot.law_id));
        try parts.append(alloc, slot.value orelse "");
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn decodeRowAlloc(alloc: Allocator, encoded: []const u8) !Row {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    if (parts.len == 0 or !std.mem.eql(u8, parts[0], "tensor-row:v1")) return error.InvalidAlgebraicTensorRow;
    if ((parts.len - 1) % 2 != 0) return error.InvalidAlgebraicTensorRow;
    const slot_count = (parts.len - 1) / 2;
    const slots = try alloc.alloc(Slot, slot_count);
    errdefer alloc.free(slots);
    var pos: usize = 1;
    var slot_index: usize = 0;
    while (pos < parts.len) : ({
        pos += 2;
        slot_index += 1;
    }) {
        slots[slot_index] = .{
            .law_id = law.Id.parse(parts[pos]) orelse return error.InvalidAlgebraicTensorRow,
            .value = if (parts[pos + 1].len == 0) null else try alloc.dupe(u8, parts[pos + 1]),
        };
    }
    return .{ .slots = slots };
}

const count_support_laws = [_]law.Id{.count};

pub const CountSupportMutation = struct {
    next_count: i64,
    encoded: ?[]u8 = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.encoded) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

pub fn countSupportValueAlloc(alloc: Allocator, encoded: ?[]const u8) !i64 {
    const bytes = encoded orelse return 0;
    var row = try rowFromBytesOrLayoutAlloc(alloc, bytes, &count_support_laws);
    defer row.deinit(alloc);
    const value = row.slots[0].value orelse return 0;
    return try algebra.parseI64(value);
}

pub fn applyCountSupportDeltaAlloc(
    alloc: Allocator,
    existing: ?[]const u8,
    delta: i64,
) !CountSupportMutation {
    const delta_text = try algebra.encodeI64Alloc(alloc, delta);
    defer alloc.free(delta_text);
    return try applyCountSupportDeltaBytesAlloc(alloc, existing, delta_text);
}

pub fn applyCountSupportDeltaBytesAlloc(
    alloc: Allocator,
    existing: ?[]const u8,
    delta: []const u8,
) !CountSupportMutation {
    const current = try countSupportValueAlloc(alloc, existing);
    const parsed_delta = try algebra.parseI64(delta);
    const next = std.math.add(i64, current, parsed_delta) catch if (parsed_delta < 0) @as(i64, std.math.minInt(i64)) else @as(i64, std.math.maxInt(i64));
    if (next <= 0) return .{ .next_count = next };

    const encoded = try applyRowSlotToBytesAlloc(alloc, existing, &count_support_laws, 0, .count, delta, true);
    return .{
        .next_count = next,
        .encoded = encoded,
    };
}

test "tensor materialized expression keys are separate from named aggregate tensors" {
    const alloc = std.testing.allocator;
    const axes = try token.canonicalTupleAlloc(alloc, &.{ "customer", "alice" });
    defer alloc.free(axes);

    const aggregate_key = try keyAlloc(alloc, "orders_alg", .{
        .materialization = "sum_by_customer",
        .axes_canonical = axes,
        .bucket = "2026-05-15",
    });
    defer alloc.free(aggregate_key);
    const expr_key = try expressionKeyAlloc(alloc, "orders_alg", .{
        .expr_id = "expr:abc123",
        .axes_canonical = axes,
        .bucket = "2026-05-15",
    });
    defer alloc.free(expr_key);

    try std.testing.expect(std.mem.indexOf(u8, aggregate_key, "tensor") != null);
    try std.testing.expect(std.mem.indexOf(u8, expr_key, "materialized_expr") != null);
    try std.testing.expect(std.mem.indexOf(u8, expr_key, "expr:abc123") != null);
    try std.testing.expect(!std.mem.eql(u8, aggregate_key, expr_key));
}

test "tensor row applies law-backed slot deltas" {
    const alloc = std.testing.allocator;
    var row = try rowAlloc(alloc, &.{ .count, .sum });
    defer row.deinit(alloc);
    try applySlot(alloc, &row.slots[0], "1");
    try applySlot(alloc, &row.slots[0], "2");
    try applySlot(alloc, &row.slots[1], "3.5");
    try applySlot(alloc, &row.slots[1], "0.5");
    try std.testing.expectEqualStrings("3", row.slots[0].value.?);
    try std.testing.expectEqualStrings("4", row.slots[1].value.?);
}

test "tensor row slot mutation validates layout law" {
    const alloc = std.testing.allocator;
    var row = try rowAlloc(alloc, &.{.count});
    defer row.deinit(alloc);

    try applyRowSlot(alloc, &row, 0, .count, "2");
    try std.testing.expectEqualStrings("2", row.slots[0].value.?);
    try std.testing.expectError(error.InvalidAlgebraicTensorRow, applyRowSlot(alloc, &row, 0, .sum, "1"));
    try std.testing.expectError(error.InvalidAlgebraicTensorRow, applyRowSlot(alloc, &row, 1, .count, "1"));
}

test "tensor row byte mutation validates full layout" {
    const alloc = std.testing.allocator;
    var row = try rowAlloc(alloc, &.{ .count, .sum });
    defer row.deinit(alloc);
    const encoded = try encodeRowAlloc(alloc, row);
    defer alloc.free(encoded);

    const next = try applyRowSlotToBytesAlloc(alloc, encoded, &.{ .count, .sum }, 1, .sum, "2.5", true);
    defer if (next) |bytes| alloc.free(bytes);
    var decoded = try decodeRowAlloc(alloc, next.?);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(law.Id.sum, decoded.slots[1].law_id);
    try std.testing.expectEqualStrings("2.5", decoded.slots[1].value.?);

    try std.testing.expectError(
        error.InvalidAlgebraicTensorRow,
        applyRowSlotToBytesAlloc(alloc, encoded, &.{ .sum, .count }, 1, .count, "1", true),
    );
}

test "tensor row byte mutation applies multiple slot deltas" {
    const alloc = std.testing.allocator;
    const deltas = [_]SlotDelta{
        .{ .slot_idx = 0, .law_id = .count, .delta = "2" },
        .{ .slot_idx = 1, .law_id = .sum, .delta = "3.5" },
    };
    const encoded = try applyRowSlotsToBytesAlloc(alloc, null, &.{ .count, .sum }, &deltas, true);
    defer if (encoded) |bytes| alloc.free(bytes);
    var decoded = try decodeRowAlloc(alloc, encoded.?);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualStrings("2", decoded.slots[0].value.?);
    try std.testing.expectEqualStrings("3.5", decoded.slots[1].value.?);

    try std.testing.expectError(
        error.InvalidAlgebraicTensorRow,
        applyRowSlotsToBytesAlloc(alloc, encoded, &.{ .count, .sum }, &.{.{ .slot_idx = 1, .law_id = .count, .delta = "1" }}, true),
    );
}

test "tensor row byte replacement can omit empty rows" {
    const alloc = std.testing.allocator;
    const encoded = try replaceRowSlotToBytesAlloc(alloc, null, &.{.min}, 0, .min, try alloc.dupe(u8, "3"), true);
    defer if (encoded) |bytes| alloc.free(bytes);
    const old_value = try rowSlotValueAlloc(alloc, encoded, &.{.min}, 0, .min);
    defer if (old_value) |value| alloc.free(value);
    try std.testing.expectEqualStrings("3", old_value.?);
    try std.testing.expectError(error.InvalidAlgebraicTensorRow, rowSlotValueAlloc(alloc, encoded, &.{.max}, 0, .max));

    const empty = try replaceRowSlotToBytesAlloc(alloc, encoded.?, &.{.min}, 0, .min, null, true);
    try std.testing.expect(empty == null);
}

test "tensor row replacement validates layout law and owns value" {
    const alloc = std.testing.allocator;
    var row = try rowAlloc(alloc, &.{.min});
    defer row.deinit(alloc);

    try replaceRowSlotOwned(alloc, &row, 0, .min, try alloc.dupe(u8, "3"));
    try std.testing.expectEqualStrings("3", row.slots[0].value.?);
    try std.testing.expectError(error.InvalidAlgebraicTensorRow, replaceRowSlotOwned(alloc, &row, 0, .max, try alloc.dupe(u8, "2")));
    try replaceRowSlotOwned(alloc, &row, 0, .min, null);
    try std.testing.expect(row.slots[0].value == null);
}

test "tensor row round trips law ids and values" {
    const alloc = std.testing.allocator;
    var row = try rowAlloc(alloc, &.{ .count, .max });
    defer row.deinit(alloc);
    try applySlot(alloc, &row.slots[0], "3");
    try applySlot(alloc, &row.slots[1], "9");
    const encoded = try encodeRowAlloc(alloc, row);
    defer alloc.free(encoded);
    var decoded = try decodeRowAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(law.Id.count, decoded.slots[0].law_id);
    try std.testing.expectEqualStrings("3", decoded.slots[0].value.?);
    try std.testing.expectEqual(law.Id.max, decoded.slots[1].law_id);
    try std.testing.expectEqualStrings("9", decoded.slots[1].value.?);
}

test "tensor rows merge law-compatible slots" {
    const alloc = std.testing.allocator;
    var left = try rowAlloc(alloc, &.{ .count, .sum, .max });
    defer left.deinit(alloc);
    var right = try rowAlloc(alloc, &.{ .count, .sum, .max });
    defer right.deinit(alloc);

    try applySlot(alloc, &left.slots[0], "2");
    try applySlot(alloc, &left.slots[1], "3.5");
    try applySlot(alloc, &left.slots[2], "7");
    try applySlot(alloc, &right.slots[0], "4");
    try applySlot(alloc, &right.slots[1], "1.5");
    try applySlot(alloc, &right.slots[2], "9");

    var merged = try mergeRowsAlloc(alloc, left, right);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(law.Id.count, merged.slots[0].law_id);
    try std.testing.expectEqualStrings("6", merged.slots[0].value.?);
    try std.testing.expectEqualStrings("5", merged.slots[1].value.?);
    try std.testing.expectEqualStrings("9", merged.slots[2].value.?);
}

test "tensor rows merge one-slot partial values through row primitive" {
    const alloc = std.testing.allocator;

    const sum = try mergeOneSlotValuesAlloc(alloc, .sum, "2.5", "7.5");
    defer alloc.free(sum);
    try std.testing.expectEqualStrings("10", sum);

    const count = try mergeOneSlotValuesAlloc(alloc, .count, null, "3");
    defer alloc.free(count);
    try std.testing.expectEqualStrings("3", count);

    const zero = try mergeOneSlotValuesAlloc(alloc, .sum, null, null);
    defer alloc.free(zero);
    try std.testing.expectEqualStrings("0", zero);
}

test "tensor rows apply and merge set union lattice slots" {
    const alloc = std.testing.allocator;
    const left_delta = try token.canonicalTupleAlloc(alloc, &.{ "doc:o2", "doc:o1" });
    defer alloc.free(left_delta);
    const right_delta = try token.canonicalTupleAlloc(alloc, &.{ "doc:o3", "doc:o2" });
    defer alloc.free(right_delta);

    const left_encoded = (try applyRowSlotToBytesAlloc(alloc, null, &.{.set_union}, 0, .set_union, left_delta, true)).?;
    defer alloc.free(left_encoded);
    const right_encoded = (try applyRowSlotToBytesAlloc(alloc, null, &.{.set_union}, 0, .set_union, right_delta, true)).?;
    defer alloc.free(right_encoded);

    var left_row = try decodeRowAlloc(alloc, left_encoded);
    defer left_row.deinit(alloc);
    var right_row = try decodeRowAlloc(alloc, right_encoded);
    defer right_row.deinit(alloc);
    var merged = try mergeRowsAlloc(alloc, left_row, right_row);
    defer merged.deinit(alloc);

    const merged_value = merged.slots[0].value orelse return error.TestUnexpectedResult;
    const parts = try token.decodeTupleAlloc(alloc, merged_value);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("doc:o1", parts[0]);
    try std.testing.expectEqualStrings("doc:o2", parts[1]);
    try std.testing.expectEqualStrings("doc:o3", parts[2]);
}

test "tensor count support rows use count law layout" {
    const alloc = std.testing.allocator;
    var first = try applyCountSupportDeltaAlloc(alloc, null, 2);
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 2), first.next_count);
    try std.testing.expectEqual(@as(i64, 2), try countSupportValueAlloc(alloc, first.encoded));

    var second = try applyCountSupportDeltaAlloc(alloc, first.encoded, -1);
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), second.next_count);
    try std.testing.expectEqual(@as(i64, 1), try countSupportValueAlloc(alloc, second.encoded));

    var empty = try applyCountSupportDeltaAlloc(alloc, second.encoded, -1);
    defer empty.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 0), empty.next_count);
    try std.testing.expect(empty.encoded == null);
}

test "tensor count support rows accept law-encoded deltas" {
    const alloc = std.testing.allocator;
    const two = try algebra.encodeI64Alloc(alloc, 2);
    defer alloc.free(two);
    var first = try applyCountSupportDeltaBytesAlloc(alloc, null, two);
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 2), first.next_count);
    try std.testing.expectEqual(@as(i64, 2), try countSupportValueAlloc(alloc, first.encoded));

    const neg_two = try algebra.encodeI64Alloc(alloc, -2);
    defer alloc.free(neg_two);
    var empty = try applyCountSupportDeltaBytesAlloc(alloc, first.encoded, neg_two);
    defer empty.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 0), empty.next_count);
    try std.testing.expect(empty.encoded == null);
}

test "tensor row merge rejects incompatible layouts" {
    const alloc = std.testing.allocator;
    var left = try rowAlloc(alloc, &.{.count});
    defer left.deinit(alloc);
    var right = try rowAlloc(alloc, &.{.sum});
    defer right.deinit(alloc);
    try std.testing.expectError(error.IncompatibleAlgebraicTensorRows, mergeRowsAlloc(alloc, left, right));
}

test "tensor keys use canonical axis symbols instead of raw axis bytes" {
    const alloc = std.testing.allocator;
    const axes = try token.canonicalTupleAlloc(alloc, &.{ "tenant:t1", "product:p1" });
    defer alloc.free(axes);
    const key = try keyAlloc(alloc, "alg", .{ .materialization = "sales", .axes_canonical = axes });
    defer alloc.free(key);
    try std.testing.expect(std.mem.indexOf(u8, key, "tensor") != null);
    try std.testing.expect(std.mem.indexOf(u8, key, axes) == null);
}
