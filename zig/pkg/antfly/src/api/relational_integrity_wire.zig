// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Internal-only, binary-safe transaction envelope. Never use this parser on
//! unauthenticated/public batch ingress. Exact absence differs from empty bytes.
const std = @import("std");

pub fn encodeGenerationSet(alloc: std.mem.Allocator, value: [32]u8) ![]u8 {
    const NumericBytes = struct {
        value: [32]u8,
        pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
            try @import("../storage/db/relational_integrity_json.zig").write(self.value, stream);
        }
    };
    return std.json.Stringify.valueAlloc(alloc, NumericBytes{ .value = value }, .{});
}

pub fn parseGenerationSet(value: std.json.Value) ![32]u8 {
    if (value != .array or value.array.items.len != 32) return error.InvalidBatchRequest;
    var result: [32]u8 = undefined;
    for (value.array.items, &result) |item, *byte| {
        if (item != .integer) return error.InvalidBatchRequest;
        byte.* = std.math.cast(u8, item.integer) orelse return error.InvalidBatchRequest;
    }
    return result;
}
const types = @import("../storage/db/types.zig");
const Operation = types.TransactionIntegrityOperation;
pub const Command = @import("../storage/db/relational_integrity_contract.zig").Command;
pub const max_operations = 65_536;
pub const max_bytes = 16 * 1024 * 1024;

test "distributed txn HA preserves arbitrary binary coordinator generation evidence" {
    const effects = @import("../storage/hot_standby/effects.zig");
    const alloc = std.testing.allocator;
    const digest = [_]u8{0xff} ** 32;
    const encoded = try effects.encodeBatchMutationRequestAlloc(alloc, .{ .relational_schema_version = 3, .relational_integrity_generation_set = digest });
    defer alloc.free(encoded);
    var json = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer json.deinit();
    const value = json.value.object.get("request").?.object.get("relational_integrity_generation_set").?;
    try std.testing.expect(value == .array);
    try std.testing.expectEqualSlices(u8, &digest, &try parseGenerationSet(value));
    var parsed = try std.json.parseFromSlice(effects.BatchMutationPayload, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(digest, parsed.value.request.relational_integrity_generation_set.?);
}

pub fn appendCommands(alloc: std.mem.Allocator, out: *std.ArrayList(u8), commands: []const Command) !void {
    _ = try @import("../storage/db/relational_integrity_contract.zig").validateCommandAdmission(commands);
    const encoded = try std.json.Stringify.valueAlloc(alloc, commands, .{});
    defer alloc.free(encoded);
    if (encoded.len > max_bytes) return error.InvalidTxnRequest;
    try out.appendSlice(alloc, encoded);
}

pub fn parseCommands(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Parsed([]const Command) {
    if (value != .array or value.array.items.len > @import("../storage/db/relational_integrity_contract.zig").max_commands) return error.InvalidTxnRequest;
    var parsed = try std.json.parseFromValue([]const Command, alloc, value, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    _ = try @import("../storage/db/relational_integrity_contract.zig").validateCommandAdmission(parsed.value);
    return parsed;
}

fn appendBinary(alloc: std.mem.Allocator, out: *std.ArrayList(u8), bytes: ?[]const u8) !void {
    const value = bytes orelse return out.appendSlice(alloc, "null");
    const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(value.len));
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, value);
    try out.append(alloc, '"');
    try out.appendSlice(alloc, encoded);
    try out.append(alloc, '"');
}

pub fn append(alloc: std.mem.Allocator, out: *std.ArrayList(u8), operations: []const Operation) !void {
    if (operations.len > max_operations) return error.InvalidTxnRequest;
    var bytes: usize = 0;
    try out.append(alloc, '[');
    for (operations, 0..) |operation, i| {
        try validate(operation);
        bytes = std.math.add(usize, bytes, operation.routing_key.len + operation.key.len) catch return error.InvalidTxnRequest;
        bytes = std.math.add(usize, bytes, if (operation.value) |value| value.len else 0) catch return error.InvalidTxnRequest;
        bytes = std.math.add(usize, bytes, if (operation.expected_value) |value| value.len else 0) catch return error.InvalidTxnRequest;
        if (bytes > max_bytes) return error.InvalidTxnRequest;
        if (i != 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"routing_key\":");
        try appendBinary(alloc, out, operation.routing_key);
        try out.appendSlice(alloc, ",\"key\":");
        try appendBinary(alloc, out, operation.key);
        try out.appendSlice(alloc, ",\"kind\":\"");
        try out.appendSlice(alloc, @tagName(operation.kind));
        try out.appendSlice(alloc, "\",\"value\":");
        try appendBinary(alloc, out, operation.value);
        try out.appendSlice(alloc, ",\"expected_value\":");
        try appendBinary(alloc, out, operation.expected_value);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
}

fn validate(operation: Operation) !void {
    if (operation.routing_key.len == 0 or operation.key.len == 0) return error.InvalidTxnRequest;
    if ((operation.kind == .put) != (operation.value != null)) return error.InvalidTxnRequest;
}

fn parseBinary(alloc: std.mem.Allocator, value: std.json.Value, remaining: *usize) !?[]u8 {
    if (value == .null) return null;
    if (value != .string) return error.InvalidTxnRequest;
    const size = std.base64.standard.Decoder.calcSizeForSlice(value.string) catch return error.InvalidTxnRequest;
    if (size > remaining.*) return error.InvalidTxnRequest;
    const result = try alloc.alloc(u8, size);
    errdefer alloc.free(result);
    std.base64.standard.Decoder.decode(result, value.string) catch return error.InvalidTxnRequest;
    remaining.* -= size;
    return result;
}

pub fn parse(alloc: std.mem.Allocator, value: std.json.Value) ![]Operation {
    if (value != .array or value.array.items.len > max_operations) return error.InvalidTxnRequest;
    const result = try alloc.alloc(Operation, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |operation| freeOperation(alloc, operation);
        alloc.free(result);
    }
    var remaining: usize = max_bytes;
    for (value.array.items, result) |item, *operation| {
        if (item != .object or item.object.count() != 5) return error.InvalidTxnRequest;
        const object = item.object;
        const route = (try parseBinary(alloc, object.get("routing_key") orelse return error.InvalidTxnRequest, &remaining)) orelse return error.InvalidTxnRequest;
        errdefer alloc.free(route);
        const key = (try parseBinary(alloc, object.get("key") orelse return error.InvalidTxnRequest, &remaining)) orelse return error.InvalidTxnRequest;
        errdefer alloc.free(key);
        const kind = object.get("kind") orelse return error.InvalidTxnRequest;
        if (kind != .string) return error.InvalidTxnRequest;
        const parsed_kind = std.meta.stringToEnum(@FieldType(Operation, "kind"), kind.string) orelse return error.InvalidTxnRequest;
        const bytes = try parseBinary(alloc, object.get("value") orelse return error.InvalidTxnRequest, &remaining);
        errdefer if (bytes) |data| alloc.free(data);
        const expected = try parseBinary(alloc, object.get("expected_value") orelse return error.InvalidTxnRequest, &remaining);
        errdefer if (expected) |data| alloc.free(data);
        operation.* = .{ .routing_key = route, .key = key, .kind = parsed_kind, .value = bytes, .expected_value = expected };
        try validate(operation.*);
        initialized += 1;
    }
    return result;
}

fn freeOperation(alloc: std.mem.Allocator, operation: Operation) void {
    alloc.free(operation.routing_key);
    alloc.free(operation.key);
    if (operation.value) |value| alloc.free(value);
    if (operation.expected_value) |value| alloc.free(value);
}

pub fn free(alloc: std.mem.Allocator, operations: []const Operation) void {
    for (operations) |operation| freeOperation(alloc, operation);
    if (operations.len != 0) alloc.free(operations);
}

test "distributed txn integrity wire preserves binary routing and exact empty versus absent" {
    const alloc = std.testing.allocator;
    const operations = [_]Operation{
        .{ .routing_key = "\xff\x00", .key = "\x00\x00claim", .kind = .guard, .expected_value = "" },
        .{ .routing_key = "\xff\x00", .key = "\x00\x00ref", .kind = .put, .value = "\xff\xfe\x00" },
        .{ .routing_key = "b", .key = "\x00\x00other", .kind = .delete, .expected_value = "old" },
    };
    var encoded = std.ArrayList(u8).empty;
    defer encoded.deinit(alloc);
    try append(alloc, &encoded, &operations);
    var json = try std.json.parseFromSlice(std.json.Value, alloc, encoded.items, .{});
    defer json.deinit();
    const parsed = try parse(alloc, json.value);
    defer free(alloc, parsed);
    try std.testing.expectEqual(@as(usize, 3), parsed.len);
    for (operations, parsed) |expected, actual| {
        try std.testing.expectEqualSlices(u8, expected.routing_key, actual.routing_key);
        try std.testing.expectEqualSlices(u8, expected.key, actual.key);
        try std.testing.expectEqual(expected.kind, actual.kind);
        try std.testing.expectEqualDeep(expected.value, actual.value);
        try std.testing.expectEqualDeep(expected.expected_value, actual.expected_value);
    }
}

test "distributed txn integrity wire rejects ambiguous or invalid effects and releases allocations" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{
        "[{\"routing_key\":\"YQ==\",\"key\":\"Yg==\",\"kind\":\"put\",\"value\":null,\"expected_value\":null}]",
        "[{\"routing_key\":\"\",\"key\":\"Yg==\",\"kind\":\"guard\",\"value\":null,\"expected_value\":null}]",
        "[{\"routing_key\":\"YQ==\",\"key\":\"Yg==\",\"kind\":\"guard\",\"value\":\"Yw==\",\"expected_value\":null}]",
    }) |body| {
        var json = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
        defer json.deinit();
        try std.testing.expectError(error.InvalidTxnRequest, parse(alloc, json.value));
    }
}

test "distributed txn semantic commands roundtrip binary tuples for native and internal wire" {
    const alloc = std.testing.allocator;
    const integrity = @import("../storage/db/relational_integrity_contract.zig");
    const address = try integrity.Address.init(@splat(1), "\xff\x00\xfe");
    const commands = [_]Command{ .{ .address = address, .operation = .{ .establish = .{
        .tuple = "\xff\x00\xfe",
        .parent_table = "parents",
        .parent_key = "key",
        .schema_version = 1,
    } } }, .{ .address = address, .operation = .{ .attach = .{
        .child_table = "children",
        .child_key = "child",
        .constraint_name = "fk",
        .constraint_generation = @splat(2),
    } } } };
    var encoded = std.ArrayList(u8).empty;
    defer encoded.deinit(alloc);
    try appendCommands(alloc, &encoded, &commands);
    var json = try std.json.parseFromSlice(std.json.Value, alloc, encoded.items, .{});
    defer json.deinit();
    var parsed = try parseCommands(alloc, json.value);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(@as([]const Command, &commands), parsed.value);
}
