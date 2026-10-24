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

//! Schema execution remains task-owned. This layer verifies literal support
//! against exactly the text submitted and maps it back to original page spans.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}

pub fn attach(a: A, item: *V, state: []const u8, metadata: ?V, required: bool, allow_derived: bool) !bool {
    var supported = true;
    const unit_value = field(item.*, "offset_unit") orelse V{ .string = "utf8_bytes" };
    if (unit_value != .string) return error.InvalidDocumentEvidence;
    const unit = unit_value.string;
    if (item.* != .object) return error.InvalidDocumentModelResult;
    if (item.object.getPtr("structures")) |structures| {
        if (structures.* != .object) return error.InvalidDocumentModelResult;
        for (structures.object.values()) |*records| {
            if (records.* != .array) return error.InvalidDocumentModelResult;
            for (records.array.items) |*record| {
                if (record.* != .object) return error.InvalidDocumentModelResult;
                for (record.object.values()) |*value| {
                    if (value.* == .object) {
                        supported = try attachField(a, value, "value", state, metadata, required, allow_derived, unit) and supported;
                    } else if (value.* == .array) {
                        for (value.array.items) |*element| supported = try attachField(a, element, "value", state, metadata, required, allow_derived, unit) and supported;
                    } else if (required) supported = false;
                }
            }
        }
    }
    if (item.object.getPtr("entities")) |entities| {
        if (entities.* != .array) return error.InvalidDocumentModelResult;
        for (entities.array.items) |*entity| supported = try attachField(a, entity, "text", state, metadata, required, false, unit) and supported;
    }
    return supported;
}

fn attachField(a: A, item: *V, key: []const u8, state: []const u8, metadata: ?V, required: bool, allow_derived: bool, unit: []const u8) !bool {
    const value = field(item.*, key) orelse return error.InvalidDocumentModelResult;
    if (value == .null) return true;
    if (field(item.*, "source")) |source| if (source == .string and std.mem.eql(u8, source.string, "schema")) return allow_derived;
    const origin = field(item.*, "origin");
    if (origin) |o| {
        if (o != .string) return error.InvalidDocumentModelResult;
        if (std.mem.eql(u8, o.string, "derived") or std.mem.eql(u8, o.string, "estimated")) return allow_derived;
    }
    if (value == .array) {
        const spans = field(item.*, "spans") orelse return !required;
        if (spans != .array or spans.array.items.len != value.array.items.len) return error.InvalidDocumentModelResult;
        var evidence: std.array_list.Managed(V) = .init(a);
        for (value.array.items, spans.array.items) |element, span| {
            if (span == .null) {
                if (required) return false;
                continue;
            }
            if (span != .array or span.array.items.len != 2 or element != .string) return error.InvalidDocumentModelResult;
            const range = (try exactSpan(state, element.string, span.array.items[0], span.array.items[1], unit)) orelse return false;
            try mapSpan(a, &evidence, metadata, range);
        }
        try item.object.put(a, "evidence", .{ .array = evidence });
        return preserveOrigin(a, item, evidence.items, allow_derived);
    }
    const start = field(item.*, "start") orelse return !required;
    const end = field(item.*, "end") orelse return !required;
    if (start == .null or end == .null) return !required;
    if (value != .string) return false;
    const range = (try exactSpan(state, value.string, start, end, unit)) orelse return false;
    var evidence: std.array_list.Managed(V) = .init(a);
    try mapSpan(a, &evidence, metadata, range);
    try item.object.put(a, "evidence", .{ .array = evidence });
    return preserveOrigin(a, item, evidence.items, allow_derived);
}
fn preserveOrigin(a: A, item: *V, references: []const V, allow_derived: bool) !bool {
    for (references) |reference| if (field(reference, "origin")) |origin| {
        if (origin == .string and std.mem.eql(u8, origin.string, "interpretation")) {
            try item.object.put(a, "origin", .{ .string = "derived" });
            _ = item.object.swapRemove("start");
            _ = item.object.swapRemove("end");
            return allow_derived;
        }
    };
    return true;
}
fn exactSpan(state: []const u8, text: []const u8, first: V, last: V, unit: []const u8) !?[2]usize {
    const start = try byteOffset(state, try offset(first), unit);
    const end = try byteOffset(state, try offset(last), unit);
    if (start > end or end > state.len or !std.mem.eql(u8, state[start..end], text)) return null;
    return .{ start, end };
}
fn byteOffset(state: []const u8, target: usize, unit: []const u8) !usize {
    if (std.mem.eql(u8, unit, "utf8_bytes")) return target;
    const utf16 = std.mem.eql(u8, unit, "utf16_codeunits");
    if (!utf16 and !std.mem.eql(u8, unit, "unicode_codepoints")) return error.InvalidDocumentEvidence;
    var position: usize = 0;
    var units: usize = 0;
    while (position < state.len and units < target) {
        const length = std.unicode.utf8ByteSequenceLength(state[position]) catch return error.InvalidDocumentEvidence;
        if (length > state.len - position) return error.InvalidDocumentEvidence;
        const cp = std.unicode.utf8Decode(state[position..][0..length]) catch return error.InvalidDocumentEvidence;
        units += if (utf16 and cp > 0xffff) @as(usize, 2) else 1;
        position += length;
    }
    if (units != target) return error.InvalidDocumentEvidence;
    return position;
}
fn offset(value: V) !usize {
    return switch (value) {
        .integer => std.math.cast(usize, value.integer) orelse error.InvalidDocumentEvidence,
        .number_string => std.fmt.parseUnsigned(usize, value.number_string, 10) catch error.InvalidDocumentEvidence,
        else => error.InvalidDocumentEvidence,
    };
}
fn mapSpan(a: A, out: *std.array_list.Managed(V), metadata: ?V, range: [2]usize) !void {
    const source = metadata orelse return;
    const segments = field(source, "segments") orelse return;
    const id = field(source, "source_id") orelse return error.InvalidDocumentEvidence;
    if (segments != .array) return error.InvalidDocumentEvidence;
    for (segments.array.items) |segment| {
        const start = try offset(field(segment, "start") orelse return error.InvalidDocumentEvidence);
        const end = try offset(field(segment, "end") orelse return error.InvalidDocumentEvidence);
        const left = @max(start, range[0]);
        const right = @min(end, range[1]);
        if (left >= right) continue;
        var reference = V{ .object = .{} };
        try reference.object.put(a, "source_id", id);
        try reference.object.put(a, "offset_unit", .{ .string = "utf8_bytes" });
        try reference.object.put(a, "page_number", field(segment, "number") orelse return error.InvalidDocumentEvidence);
        try reference.object.put(a, "start", .{ .integer = @intCast(left - start) });
        try reference.object.put(a, "end", .{ .integer = @intCast(right - start) });
        if (field(segment, "origin")) |origin| try reference.object.put(a, "origin", origin);
        if (field(segment, "role")) |role| try reference.object.put(a, "role", role);
        try out.append(reference);
    }
}

/// Independent extraction corroboration compares requested values/records,
/// keeping confidence, source offsets and provider metadata out of the vote.
pub fn sameValues(left: V, right: V) bool {
    for ([_][]const u8{ "structures", "entities", "relations", "classifications" }) |group| {
        const l = field(left, group);
        const r = field(right, group);
        if ((l == null) != (r == null)) return false;
        if (l == null) continue;
        if (std.mem.eql(u8, group, "structures")) {
            if (l.? != .object or r.? != .object or l.?.object.count() != r.?.object.count()) return false;
            for (l.?.object.keys(), l.?.object.values()) |name, records| {
                const other = r.?.object.get(name) orelse return false;
                if (records != .array or other != .array or records.array.items.len != other.array.items.len) return false;
                for (records.array.items, other.array.items) |record, match| {
                    if (record != .object or match != .object or record.object.count() != match.object.count()) return false;
                    for (record.object.keys(), record.object.values()) |key, value| {
                        if (!sameField(value, match.object.get(key) orelse return false)) return false;
                    }
                }
            }
        } else if (!equal(l.?, r.?, 0)) return false;
    }
    return true;
}
fn sameField(left: V, right: V) bool {
    if (left == .object and right == .object) return equal(field(left, "value") orelse return false, field(right, "value") orelse return false, 0);
    if (left == .array and right == .array) {
        if (left.array.items.len != right.array.items.len) return false;
        for (left.array.items, right.array.items) |l, r| if (!sameField(l, r)) return false;
        return true;
    }
    return equal(left, right, 0);
}
fn equal(left: V, right: V, depth: usize) bool {
    if (depth > 64 or std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .null => true,
        .bool => left.bool == right.bool,
        .integer => left.integer == right.integer,
        .float => left.float == right.float,
        .string => std.mem.eql(u8, left.string, right.string),
        .number_string => std.mem.eql(u8, left.number_string, right.number_string),
        .array => blk: {
            if (left.array.items.len != right.array.items.len) break :blk false;
            for (left.array.items, right.array.items) |l, r| if (!equal(l, r, depth + 1)) break :blk false;
            break :blk true;
        },
        .object => blk: {
            if (left.object.count() != right.object.count()) break :blk false;
            for (left.object.keys(), left.object.values()) |key, value| if (!equal(value, right.object.get(key) orelse break :blk false, depth + 1)) break :blk false;
            break :blk true;
        },
    };
}

test "document extraction evidence maps exact unicode bytes across page records" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var item = try std.json.parseFromSliceLeaky(V, a, "{\"structures\":{\"receipt\":[{\"name\":{\"value\":\"Éva\",\"start\":3,\"end\":7}}]}}", .{});
    const metadata = try std.json.parseFromSliceLeaky(V, a, "{\"source_id\":\"receipt\",\"segments\":[{\"number\":2,\"start\":3,\"end\":7}]}", .{});
    try std.testing.expect(try attach(a, &item, "xx Éva", metadata, true, false));
    const value = field(field(item, "structures").?, "receipt").?.array.items[0].object.get("name").?;
    try std.testing.expectEqual(@as(i64, 0), field(value, "evidence").?.array.items[0].object.get("start").?.integer);
    try std.testing.expect(!try attach(a, &item, "xx Eva!", metadata, true, false));
}

test "document extraction corroboration compares values without accepting confidence as evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const l = try std.json.parseFromSliceLeaky(V, a, "{\"structures\":{\"r\":[{\"x\":{\"value\":\"12.00\",\"score\":0.1}}]}}", .{});
    const r = try std.json.parseFromSliceLeaky(V, a, "{\"structures\":{\"r\":[{\"x\":{\"value\":\"12.00\",\"score\":0.9}}]}}", .{});
    try std.testing.expect(sameValues(l, r));
    try std.testing.expect(!try attach(a, @constCast(&l), "12.00", null, true, false));
}

test "document extraction preserves interpretation provenance without inventing literal source spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json = "{\"structures\":{\"r\":[{\"x\":{\"value\":\"caption\",\"start\":0,\"end\":7,\"origin\":\"transcription\"}}]}}";
    var item = try std.json.parseFromSliceLeaky(V, a, json, .{});
    const metadata = try std.json.parseFromSliceLeaky(V, a, "{\"source_id\":\"picture\",\"segments\":[{\"number\":1,\"start\":0,\"end\":7,\"origin\":\"interpretation\"}]}", .{});
    try std.testing.expect(try attach(a, &item, "caption", metadata, true, true));
    const value = field(field(item, "structures").?, "r").?.array.items[0].object.get("x").?;
    try std.testing.expectEqualStrings("derived", field(value, "origin").?.string);
    try std.testing.expect(field(value, "start") == null);
    var strict = try std.json.parseFromSliceLeaky(V, a, json, .{});
    try std.testing.expect(!try attach(a, &strict, "caption", metadata, true, false));
}
