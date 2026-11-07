// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Private attachment metadata on the existing durable schema boundary. These
//! functions neither grant execution authority nor import routine bodies.
const std = @import("std");
const triggers = @import("../system_catalog/triggers.zig");
const A = std.mem.Allocator;
pub const field = "sql_triggers";
pub fn parseValue(a: A, value: std.json.Value) !std.json.Parsed(triggers.State) {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var parsed = std.json.parseFromSlice(triggers.State, a, bytes, .{ .allocate = .alloc_always }) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidSqlTriggerDefinition;
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
/// An embedded schema field is a semantic object. Normalize its validated
/// typed state before persistence; only standalone wire decoders require the
/// caller's original JSON bytes to already have canonical order/defaults.
pub fn canonicalize(a: A, root: *std.json.Value, state: triggers.State) !void {
    if (root.* != .object) return error.InvalidSchemaUpdateRequest;
    const encoded = try state.encode(a);
    try root.object.put(a, field, try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}));
}
pub fn rejectPublic(root: std.json.Value) !void {
    if (root != .object or root.object.contains(field)) return error.InvalidSchemaUpdateRequest;
}

/// Public replacements/patches preserve private attachments by immutable table
/// identity. Only the authorized SQL attachment transaction may replace them.
/// An explicit public sql_triggers key, including null, is always rejected.
pub fn preserve(a: A, previous: []const u8, input: []const u8) ![]u8 {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, if (previous.len == 0) "{}" else previous, .{});
    var next = try std.json.parseFromSliceLeaky(std.json.Value, scratch, input, .{});
    try rejectPublic(next);
    if (old != .object) return error.InvalidSchemaUpdateRequest;
    if (old.object.get(field)) |state| {
        var checked = try parseValue(scratch, state);
        defer checked.deinit();
        try canonicalize(scratch, &next, checked.value);
    }
    return std.json.Stringify.valueAlloc(a, next, .{});
}

test "public table schema replacement cannot inject or discard trigger attachments" {
    const a = std.testing.allocator;
    const state: triggers.State = .{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 2, .next_id = 3 };
    const encoded = try state.encode(a);
    defer a.free(encoded);
    const previous = try std.fmt.allocPrint(a, "{{\"version\":3,\"sql_triggers\":{s}}}", .{encoded});
    defer a.free(previous);
    const merged = try preserve(a, previous, "{\"storage_mode\":\"document\"}");
    defer a.free(merged);
    var decoded = try std.json.parseFromSlice(std.json.Value, a, merged, .{});
    defer decoded.deinit();
    try std.testing.expect(decoded.value.object.contains(field));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, preserve(a, previous, "{\"sql_triggers\":null}"));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, preserve(a, previous, "{\"sql_triggers\":{}}"));
}

/// SQL column/index evolution starts from the complete captured schema. It may
/// carry existing private metadata unchanged, but cannot create/change/drop it.
pub fn stripUnchanged(a: A, previous: []const u8, input: []const u8) ![]u8 {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, if (previous.len == 0) "{}" else previous, .{});
    var next = try std.json.parseFromSliceLeaky(std.json.Value, scratch, input, .{});
    if (old != .object or next != .object) return error.InvalidSchemaUpdateRequest;
    if (next.object.get(field)) |candidate| {
        const prior = old.object.get(field) orelse return error.InvalidSchemaUpdateRequest;
        var old_state = try parseValue(scratch, prior);
        defer old_state.deinit();
        var next_state = try parseValue(scratch, candidate);
        defer next_state.deinit();
        const old_bytes = try old_state.value.encode(scratch);
        const next_bytes = try next_state.value.encode(scratch);
        if (!std.mem.eql(u8, old_bytes, next_bytes)) return error.InvalidSchemaUpdateRequest;
        _ = next.object.orderedRemove(field);
    }
    return std.json.Stringify.valueAlloc(a, next, .{});
}

test "trigger schema objects normalize reordered keys omitted defaults and raw incarnation representations" {
    const a = std.testing.allocator;
    const unordered = "{\"entries\":[],\"generation\":2,\"table_id\":7,\"metadata_incarnation\":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],\"next_id\":3,\"format\":1}";
    var raw = try std.json.parseFromSlice(std.json.Value, a, unordered, .{ .parse_numbers = false });
    defer raw.deinit();
    var parsed = try parseValue(a, raw.value);
    defer parsed.deinit();
    const canonical = try parsed.value.encode(a);
    defer a.free(canonical);
    const expected = try (triggers.State{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 2, .next_id = 3 }).encode(a);
    defer a.free(expected);
    try std.testing.expectEqualStrings(expected, canonical);
    // The standalone wire decoder remains strict even though schema objects
    // accept harmless ordering/default/byte-array representation differences.
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, triggers.State.decode(a, unordered));
    const defaults = "{\"generation\":2,\"metadata_incarnation\":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],\"table_id\":7}";
    var default_raw = try std.json.parseFromSlice(std.json.Value, a, defaults, .{});
    defer default_raw.deinit();
    var default_state = try parseValue(a, default_raw.value);
    defer default_state.deinit();
    try std.testing.expectEqual(@as(u8, 1), default_state.value.format);
    try std.testing.expectEqual(@as(u64, 1), default_state.value.next_id);
    try std.testing.expectEqual(@as(usize, 0), default_state.value.entries.len);
    try default_raw.value.object.put(default_raw.arena.allocator(), "format", .{ .integer = 2 });
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, parseValue(a, default_raw.value));
}

test "trigger schema object decoding owns attachment strings after input release" {
    const a = std.testing.allocator;
    const state = try triggers.prepare(a, .{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 1 }, .{ .name = "touch", .timing = .before, .events = .{ .update = true }, .level = .row, .function = .{ .id = 9, .namespace_id = 2, .database = "antfly", .namespace = "public", .name = "touch_updated_at" }, .arguments = &.{"updated_at_ns"} }, false);
    defer a.free(state.entries);
    const encoded = try state.encode(a);
    var raw = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    var parsed = try parseValue(a, raw.value);
    defer parsed.deinit();
    raw.deinit();
    a.free(encoded);
    try std.testing.expectEqualStrings("touch", parsed.value.entries[0].name);
    try std.testing.expectEqualStrings("touch_updated_at", parsed.value.entries[0].function.name);
    try std.testing.expectEqualStrings("updated_at_ns", parsed.value.entries[0].arguments[0]);
}
