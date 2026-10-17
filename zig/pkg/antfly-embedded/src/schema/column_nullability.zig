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

//! Shared nullability transformation and exact schema-delta verification.
//! This proves declaration shape, not row validity, epoch authority or commit.
const std = @import("std");
const Value = std.json.Value;
const equal = @import("json_shape.zig").equal;

fn list(schema: *Value, alloc: std.mem.Allocator, key: []const u8) !*std.array_list.Managed(Value) {
    const entry = try schema.object.getOrPut(alloc, key);
    if (!entry.found_existing) entry.value_ptr.* = .{ .array = std.array_list.Managed(Value).init(alloc) };
    if (entry.value_ptr.* != .array) return error.InvalidSqlBackendResponse;
    return &entry.value_ptr.array;
}

pub fn setColumn(alloc: std.mem.Allocator, schema: Value, row: *Value, properties: Value, column_name: []const u8, nullable: bool) !bool {
    const property = properties.object.getPtr(column_name) orelse return error.UndefinedColumn;
    if (property.* != .object) return error.InvalidSqlBackendResponse;
    // Do not override a shared referenced definition with an ignored sibling.
    if (property.object.contains("$ref")) return error.UnsupportedSqlShape;
    if (nullable) if (schema.object.get("unique_constraints")) |constraints| {
        if (constraints != .array) return error.InvalidSqlBackendResponse;
        for (constraints.array.items) |constraint| {
            if (constraint != .object) return error.InvalidSqlBackendResponse;
            const primary = constraint.object.get("primary") orelse continue;
            if (primary != .bool) return error.InvalidSqlBackendResponse;
            if (!primary.bool) continue;
            const columns = constraint.object.get("columns") orelse return error.InvalidSqlBackendResponse;
            if (columns != .array) return error.InvalidSqlBackendResponse;
            for (columns.array.items) |column| {
                if (column != .string) return error.InvalidSqlBackendResponse;
                if (std.mem.eql(u8, column.string, column_name)) return error.SqlPrimaryKeyNotNull;
            }
        }
    };
    // Presence and explicit SQL NULL admission are independent schema bits.
    // Keep the original type/format/modifier and remove only a null union arm.
    const kind = property.object.get("type") orelse return error.UnsupportedSqlShape;
    var union_null = false;
    var nonnull_type: ?Value = null;
    switch (kind) {
        .array => |kinds| {
            for (kinds.items) |entry| {
                if (entry != .string) return error.InvalidSqlBackendResponse;
                if (std.mem.eql(u8, entry.string, "null")) {
                    union_null = true;
                } else {
                    if (nonnull_type != null) return error.UnsupportedSqlShape;
                    nonnull_type = entry;
                }
            }
            if (nonnull_type == null) return error.UnsupportedSqlShape;
        },
        .string => |name| if (std.mem.eql(u8, name, "null")) {
            return error.UnsupportedSqlShape;
        },
        else => return error.InvalidSqlBackendResponse,
    }
    const flag = property.object.get("nullable");
    if (flag != null and flag.? != .bool) return error.InvalidSqlBackendResponse;
    const allows_null = union_null or (if (flag) |v| v.bool else false);
    var required_index: ?usize = null;
    if (row.object.getPtr("required")) |required| {
        if (required.* != .array) return error.InvalidSqlBackendResponse;
        for (required.array.items, 0..) |entry, i| {
            if (entry != .string) return error.InvalidSqlBackendResponse;
            if (std.mem.eql(u8, entry.string, column_name)) {
                required_index = i;
                break;
            }
        }
    }
    if (nullable and required_index == null and allows_null) return false;
    if (!nullable and required_index != null and !allows_null) return false;
    // Adding a row-level required list can relocate row map slots; finish all
    // nested property work before that insertion, then reacquire as needed.
    if (!nullable and kind == .array) try property.object.put(alloc, "type", nonnull_type.?);
    try property.object.put(alloc, "nullable", .{ .bool = nullable });
    if (nullable) {
        if (required_index) |i| _ = row.object.getPtr("required").?.array.orderedRemove(i);
    } else if (required_index == null) {
        const required = try list(row, alloc, "required");
        try required.append(.{ .string = try alloc.dupe(u8, column_name) });
    }
    return true;
}

pub const Change = struct { column: []const u8, nullable: bool };
pub const Summary = struct { changed: usize = 0, tightened: usize = 0 };
pub const Limits = struct {
    max_changes: usize = 4096,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    checkpoint_context: ?*anyopaque = null,
    fn check(self: Limits) !void {
        if (self.checkpoint) |call| try call(self.checkpoint_context);
    }
};

const Row = struct {
    name: []const u8,
    documents: Value,
    document: Value,
    row: Value,
    properties: Value,
    fn load(schema: Value) !Row {
        if (schema != .object) return error.InvalidColumnNullabilityTransition;
        const name = schema.object.get("default_type") orelse return error.InvalidColumnNullabilityTransition;
        if (name != .string) return error.InvalidColumnNullabilityTransition;
        const documents = try objectField(schema, "document_schemas");
        const document = try objectField(documents, name.string);
        const row = try objectField(document, "schema");
        return .{ .name = name.string, .documents = documents, .document = document, .row = row, .properties = try objectField(row, "properties") };
    }
};
fn objectField(parent: Value, key: []const u8) !Value {
    const child = parent.object.get(key) orelse return error.InvalidColumnNullabilityTransition;
    if (child != .object) return error.InvalidColumnNullabilityTransition;
    return child;
}
fn shallowMap(a: std.mem.Allocator, input: Value, limits: Limits) !Value {
    if (input != .object) return error.InvalidColumnNullabilityTransition;
    var result: Value = .{ .object = .empty };
    try result.object.ensureTotalCapacity(a, input.object.count());
    for (input.object.keys(), input.object.values(), 0..) |key, value, i| {
        if (i % 256 == 0) try limits.check();
        result.object.putAssumeCapacityNoClobber(key, value);
    }
    return result;
}

/// The root, edited ancestors/properties and required vector belong to the
/// supplied arena. Untouched subtrees/strings are borrowed read-only: the
/// predecessor must outlive this candidate. Only these edited paths are writable.
pub const BorrowedCandidate = struct { schema: Value, summary: Summary };

/// Construct the requested declaration changes without schema serialization,
/// reparsing or copying unrelated expression/default payloads. This is not
/// permission to publish; epochs, owner scope and row validity remain separate.
pub fn prepareBorrowed(arena: *std.heap.ArenaAllocator, before: Value, changes: []const Change, limits: Limits) !BorrowedCandidate {
    try limits.check();
    if (changes.len == 0 or changes.len > limits.max_changes or changes.len > 4096) return error.InvalidColumnNullabilityTransition;
    const a = arena.allocator();
    const original = try Row.load(before);
    var candidate = try shallowMap(a, before, limits);
    var documents = try shallowMap(a, original.documents, limits);
    var document = try shallowMap(a, original.document, limits);
    var row = try shallowMap(a, original.row, limits);
    var properties = try shallowMap(a, original.properties, limits);
    if (row.object.getPtr("required")) |required| {
        if (required.* != .array) return error.InvalidColumnNullabilityTransition;
        var copy = std.json.Array.init(a);
        try copy.appendSlice(required.array.items);
        required.* = .{ .array = copy };
    }
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    try seen.ensureTotalCapacity(a, @intCast(changes.len));
    var summary: Summary = .{};
    for (changes) |change| {
        try limits.check();
        if (change.column.len == 0 or std.mem.indexOfScalar(u8, change.column, 0) != null)
            return error.InvalidColumnNullabilityTransition;
        if (std.mem.eql(u8, change.column, "_id")) return error.UnsupportedSqlShape;
        if (seen.contains(change.column)) return error.InvalidColumnNullabilityTransition;
        seen.putAssumeCapacityNoClobber(change.column, {});
        const property = properties.object.getPtr(change.column) orelse return error.UndefinedColumn;
        property.* = try shallowMap(a, property.*, limits);
        if (try setColumn(a, before, &row, properties, change.column, change.nullable)) {
            summary.changed += 1;
            if (!change.nullable) summary.tightened += 1;
        }
    }
    try row.object.put(a, "properties", properties);
    try document.object.put(a, "schema", row);
    try documents.object.put(a, original.name, document);
    try candidate.object.put(a, "document_schemas", documents);
    try limits.check();
    return .{ .schema = candidate, .summary = summary };
}

/// Independently verify an entire received schema delta against the shared
/// transformation. Versions have a separate owner/CAS contract; this component
/// must not manufacture that authority or certify a read-only row snapshot.
pub fn verifyTransition(allocator: std.mem.Allocator, before: Value, after: Value, changes: []const Change, limits: Limits) !Summary {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var prepared = try prepareBorrowed(&arena, before, changes, limits);
    _ = prepared.schema.object.swapRemove("version");
    var target = try shallowMap(arena.allocator(), after, limits);
    _ = target.object.swapRemove("version");
    if (!equal(prepared.schema, target, 0)) return error.InvalidColumnNullabilityTransition;
    try limits.check();
    return prepared.summary;
}

fn testValue(a: std.mem.Allocator, bytes: []const u8) !Value {
    return std.json.parseFromSliceLeaky(Value, a, bytes, .{ .parse_numbers = false });
}
fn testApply(a: std.mem.Allocator, schema: *Value, change: Change) !void {
    var parts = try Row.load(schema.*);
    _ = try setColumn(a, schema.*, &parts.row, parts.properties, change.column, change.nullable);
    try parts.document.object.put(a, "schema", parts.row);
    try parts.documents.object.put(a, parts.name, parts.document);
    try schema.object.put(a, "document_schemas", parts.documents);
}
const test_schema =
    \\{"version":7,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer","nullable":true},"n":{"type":["number","null"],"nullable":true,"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}}},"additionalProperties":false}}},"column_defaults":[{"column":"n","expression":{"op":"literal","type":"numeric","value":"1.25"}}],"generated_columns":[]}
;

fn transitionFaults(allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const before = try testValue(a, test_schema);
    var after = try @import("../common/owned_json.zig").clone(a, before);
    const change: Change = .{ .column = "n", .nullable = false };
    try testApply(a, &after, change);
    const before_bytes = try std.json.Stringify.valueAlloc(a, before, .{});
    const after_bytes = try std.json.Stringify.valueAlloc(a, after, .{});
    try std.testing.expectEqualDeep(Summary{ .changed = 1, .tightened = 1 }, try verifyTransition(a, before, after, &.{change}, .{}));
    try std.testing.expectEqualStrings(before_bytes, try std.json.Stringify.valueAlloc(a, before, .{}));
    try std.testing.expectEqualStrings(after_bytes, try std.json.Stringify.valueAlloc(a, after, .{}));
    for (0..5) |fault| {
        var bad = try @import("../common/owned_json.zig").clone(a, after);
        var parts = try Row.load(bad);
        switch (fault) {
            0 => try parts.properties.object.getPtr("n").?.object.put(a, "type", .{ .string = "integer" }),
            1 => try parts.properties.object.getPtr("n").?.object.getPtr("x-antfly-sql-numeric-modifier").?.object.put(a, "precision", .{ .integer = 5 }),
            2 => try bad.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.put(a, "value", .{ .string = "2.50" }),
            3 => {
                _ = parts.properties.object.swapRemove("id");
                try parts.row.object.put(a, "properties", parts.properties);
            },
            4 => try bad.object.put(a, "relational_indexes", .{ .array = std.json.Array.init(a) }),
            else => unreachable,
        }
        if (verifyTransition(a, before, bad, &.{change}, .{})) |_| return error.TestUnexpectedResult else |err| {
            if (err == error.OutOfMemory) return err;
            try std.testing.expectEqual(error.InvalidColumnNullabilityTransition, err);
        }
    }
    if (verifyTransition(a, before, after, &.{ change, change }, .{})) |_| return error.TestUnexpectedResult else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.InvalidColumnNullabilityTransition, err);
    }
    var relaxed = try @import("../common/owned_json.zig").clone(a, after);
    const relaxation: Change = .{ .column = "n", .nullable = true };
    try testApply(a, &relaxed, relaxation);
    try std.testing.expectEqualDeep(Summary{ .changed = 1 }, try verifyTransition(a, after, relaxed, &.{relaxation}, .{}));
    try std.testing.expectEqualDeep(Summary{}, try verifyTransition(a, relaxed, relaxed, &.{relaxation}, .{}));
}

test "column nullability transition isolates requested changes and every allocation failure" {
    try transitionFaults(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, transitionFaults, .{});
}

test "column nullability transition borrows unrelated payloads within a fixed scratch bound" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var before = try testValue(a, test_schema);
    const payload = try a.alloc(u8, 128 * 1024);
    @memset(payload, 'x');
    try before.object.put(a, "untouched_metadata", .{ .string = payload });
    var after = try @import("../common/owned_json.zig").clone(a, before);
    const change: Change = .{ .column = "n", .nullable = false };
    try testApply(a, &after, change);
    var scratch: [32 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&scratch);
    try std.testing.expectEqualDeep(Summary{ .changed = 1, .tightened = 1 }, try verifyTransition(fixed.allocator(), before, after, &.{change}, .{}));
    try std.testing.expectEqual(@as(usize, 128 * 1024), before.object.get("untouched_metadata").?.string.len);
    try std.testing.expect((try Row.load(before)).properties.object.get("n").?.object.get("nullable").?.bool);
    var candidate_fixed = std.heap.FixedBufferAllocator.init(&scratch);
    var candidate_arena = std.heap.ArenaAllocator.init(candidate_fixed.allocator());
    defer candidate_arena.deinit();
    const candidate = try prepareBorrowed(&candidate_arena, before, &.{change}, .{});
    try std.testing.expectEqualDeep(Summary{ .changed = 1, .tightened = 1 }, candidate.summary);
    try std.testing.expect(candidate.schema.object.get("untouched_metadata").?.string.ptr == payload.ptr);
    try std.testing.expect(!(try Row.load(candidate.schema)).properties.object.get("n").?.object.get("nullable").?.bool);
    try std.testing.expect((try Row.load(before)).properties.object.get("n").?.object.get("nullable").?.bool);
}

test "column nullability transition cancellation precedes schema acquisition" {
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    try std.testing.expectError(error.Canceled, verifyTransition(std.testing.allocator, .null, .null, &.{.{ .column = "n", .nullable = false }}, .{ .checkpoint = Cancel.check }));
}
