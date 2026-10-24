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
const refs = @import("entity_index.zig");
fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}

pub fn attach(a: A, item: *V, state: []const u8, metadata: ?V, required: bool, allow_derived: bool) !bool {
    var supported = true;
    const entities = try refs.Index.init(a, item.*);
    const entity_support = try a.alloc(bool, entities.entities.len);
    // Summarize effective source provenance once per input, not once per field.
    // Without offsets a value cannot disambiguate literal and interpreted regions.
    var source_references: std.array_list.Managed(V) = .init(a);
    if (!required) try mapSpan(a, &source_references, metadata, .{ 0, state.len });
    const unlocated_interpretation = interpreted(source_references.items);
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
                        supported = try attachField(a, value, "value", state, metadata, required, allow_derived, unit, unlocated_interpretation) and supported;
                    } else if (value.* == .array) {
                        for (value.array.items) |*element| supported = try attachField(a, element, "value", state, metadata, required, allow_derived, unit, unlocated_interpretation) and supported;
                    } else if (required) supported = false;
                }
            }
        }
    }
    // Validate each entity once; relation references reuse this result and
    // its verified evidence rather than rescanning source regions per edge.
    for (entities.entities, entity_support) |*entity, *valid| {
        valid.* = try attachField(a, entity, "text", state, metadata, required, false, unit, unlocated_interpretation);
        supported = valid.* and supported;
    }
    if (item.object.getPtr("relations")) |relations| {
        if (relations.* != .array) return error.InvalidDocumentModelResult;
        for (relations.array.items) |*relation| {
            if (relation.* != .object) return error.InvalidDocumentModelResult;
            _ = relation.object.swapRemove("evidence");
            for ([_][]const u8{ "source", "target" }) |key| {
                const endpoint = relation.object.getPtr(key) orelse {
                    supported = false;
                    continue;
                };
                supported = try attachEndpoint(a, endpoint, entities, entity_support, state, metadata, required, unit, unlocated_interpretation) and supported;
            }
        }
    }
    return supported;
}

fn attachEndpoint(a: A, endpoint: *V, entities: refs.Index, entity_support: []const bool, state: []const u8, metadata: ?V, required: bool, unit: []const u8, unlocated_interpretation: bool) !bool {
    if (endpoint.* != .object) return error.InvalidDocumentModelResult;
    _ = endpoint.object.swapRemove("evidence");
    switch (try entities.resolve(endpoint.*)) {
        .unresolved => return false,
        .inline_value => return attachField(a, endpoint, "text", state, metadata, required, false, unit, unlocated_interpretation),
        .entity => |i| {
            if (!entity_support[i]) return false;
            const entity = entities.entities[i];
            // A reference cannot disagree with its entity's literal location.
            for ([_][]const u8{ "start", "end" }) |key| if (field(endpoint.*, key)) |value| {
                const expected = field(entity, key) orelse return false;
                if (try offset(value) != try offset(expected)) return false;
            };
            _ = endpoint.object.swapRemove("origin");
            if (field(entity, "origin")) |origin| try endpoint.object.put(a, "origin", origin);
            if (field(entity, "evidence")) |evidence| try endpoint.object.put(a, "evidence", evidence);
            return true;
        },
    }
}

fn attachField(a: A, item: *V, key: []const u8, state: []const u8, metadata: ?V, required: bool, allow_derived: bool, unit: []const u8, unlocated_interpretation: bool) !bool {
    const value = field(item.*, key) orelse return error.InvalidDocumentModelResult;
    // Provider evidence is not Antfly-verified evidence. Rebuild it only from
    // checked spans against the exact submitted source and reading metadata.
    _ = item.object.swapRemove("evidence");
    if (value == .null) {
        stripLiteralOffsets(item);
        return true;
    }
    if (field(item.*, "source")) |source| if (source == .string and std.mem.eql(u8, source.string, "schema")) return markDerived(a, item, allow_derived);
    const origin = field(item.*, "origin");
    if (origin) |o| {
        if (o != .string) return error.InvalidDocumentModelResult;
        if (std.mem.eql(u8, o.string, "derived") or std.mem.eql(u8, o.string, "estimated")) {
            stripLiteralOffsets(item);
            return allow_derived;
        }
        if (std.mem.eql(u8, o.string, "interpretation")) return markDerived(a, item, allow_derived);
    }
    if (value == .array) {
        const spans = field(item.*, "spans") orelse return unlocated(a, item, required, allow_derived, unlocated_interpretation);
        if (spans != .array or spans.array.items.len != value.array.items.len) return error.InvalidDocumentModelResult;
        var evidence: std.array_list.Managed(V) = .init(a);
        var unlocated_items = false;
        for (value.array.items, spans.array.items) |element, span| {
            if (span == .null) {
                if (required) return false;
                unlocated_items = true;
                continue;
            }
            if (span != .array or span.array.items.len != 2 or element != .string) return error.InvalidDocumentModelResult;
            const range = (try exactSpan(state, element.string, span.array.items[0], span.array.items[1], unit)) orelse return false;
            try mapSpan(a, &evidence, metadata, range);
        }
        try item.object.put(a, "evidence", .{ .array = evidence });
        if (unlocated_items and unlocated_interpretation) return markDerived(a, item, allow_derived);
        return preserveOrigin(a, item, evidence.items, allow_derived);
    }
    const start = field(item.*, "start") orelse return unlocated(a, item, required, allow_derived, unlocated_interpretation);
    const end = field(item.*, "end") orelse return unlocated(a, item, required, allow_derived, unlocated_interpretation);
    if (start == .null or end == .null) return unlocated(a, item, required, allow_derived, unlocated_interpretation);
    if (value != .string) return false;
    const range = (try exactSpan(state, value.string, start, end, unit)) orelse return false;
    var evidence: std.array_list.Managed(V) = .init(a);
    try mapSpan(a, &evidence, metadata, range);
    try item.object.put(a, "evidence", .{ .array = evidence });
    return preserveOrigin(a, item, evidence.items, allow_derived);
}
fn interpreted(references: []const V) bool {
    for (references) |reference| if (field(reference, "origin")) |origin| {
        if (origin == .string and (std.mem.eql(u8, origin.string, "interpretation") or std.mem.eql(u8, origin.string, "derived") or std.mem.eql(u8, origin.string, "estimated"))) return true;
    };
    return false;
}
fn stripLiteralOffsets(item: *V) void {
    _ = item.object.swapRemove("start");
    _ = item.object.swapRemove("end");
    _ = item.object.swapRemove("spans");
}
fn markDerived(a: A, item: *V, allowed: bool) !bool {
    try item.object.put(a, "origin", .{ .string = "derived" });
    stripLiteralOffsets(item);
    return allowed;
}
fn unlocated(a: A, item: *V, required: bool, allowed: bool, source_interpreted: bool) !bool {
    if (required) return false;
    return if (source_interpreted) markDerived(a, item, allowed) else true;
}
fn preserveOrigin(a: A, item: *V, references: []const V, allow_derived: bool) !bool {
    return if (interpreted(references)) markDerived(a, item, allow_derived) else true;
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
        var path: [17]usize = undefined;
        var remaining: usize = 65536;
        try mapRegionSpan(a, out, id, segment, field(segment, "regions"), .{ left - start, right - start }, field(segment, "origin"), &path, 0, &remaining);
    }
}

fn mapRegionSpan(a: A, out: *std.array_list.Managed(V), id: V, segment: V, regions: ?V, range: [2]usize, inherited: ?V, path: *[17]usize, depth: usize, remaining: *usize) !void {
    if (depth > 16) return error.InvalidDocumentEvidence;
    var cursor = range[0];
    if (regions) |list| {
        if (list != .array or list.array.items.len > remaining.*) return error.InvalidDocumentEvidence;
        remaining.* -= list.array.items.len;
        for (list.array.items) |region| {
            const first = field(region, "text_start") orelse continue;
            const last = field(region, "text_end") orelse continue;
            if (first == .null or last == .null) continue;
            const start = try offset(first);
            const end = try offset(last);
            if (start > end) return error.InvalidDocumentEvidence;
            const left = @max(cursor, start);
            const right = @min(range[1], end);
            if (left >= right) continue;
            if (cursor < left) try appendReference(a, out, id, segment, .{ cursor, left }, inherited, path[0..depth]);
            path[depth] = try offset(field(region, "id") orelse return error.InvalidDocumentEvidence);
            try mapRegionSpan(a, out, id, segment, field(region, "children"), .{ left, right }, field(region, "origin") orelse inherited, path, depth + 1, remaining);
            cursor = right;
        }
    }
    if (cursor < range[1]) try appendReference(a, out, id, segment, .{ cursor, range[1] }, inherited, path[0..depth]);
}
fn appendReference(a: A, out: *std.array_list.Managed(V), id: V, segment: V, range: [2]usize, origin: ?V, path: []const usize) !void {
    var reference = V{ .object = .{} };
    try reference.object.put(a, "source_id", id);
    try reference.object.put(a, "offset_unit", .{ .string = "utf8_bytes" });
    try reference.object.put(a, "page_number", field(segment, "number") orelse return error.InvalidDocumentEvidence);
    try reference.object.put(a, "start", .{ .integer = @intCast(range[0]) });
    try reference.object.put(a, "end", .{ .integer = @intCast(range[1]) });
    if (origin) |value| try reference.object.put(a, "origin", value);
    if (field(segment, "role")) |role| try reference.object.put(a, "role", role);
    if (path.len > 0) try reference.object.put(a, "region_path", try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, path, .{}), .{}));
    try out.append(reference);
}

/// Shared semantic comparison owns bounded scratch memory for each vote.
pub const sameValues = @import("comparison.zig").sameValues;

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
    try std.testing.expect(try sameValues(a, l, r));
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

test "document unlocated extraction preserves interpreted provenance independently of span policy" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metadata = try std.json.parseFromSliceLeaky(V, a,
        \\{"source_id":"photo","segments":[{"number":1,"start":0,"end":7,"origin":"interpretation"}]}
    , .{});
    for ([_]bool{ false, true }) |allowed| {
        var item = try std.json.parseFromSliceLeaky(V, a,
            \\{"structures":{"r":[{"caption":{"value":"Caption"},"empty":{"value":null},"list":{"value":["Caption"],"spans":[null]}}]}}
        , .{});
        try std.testing.expectEqual(allowed, try attach(a, &item, "Caption", metadata, false, allowed));
        const record = item.object.get("structures").?.object.get("r").?.array.items[0];
        try std.testing.expectEqualStrings("derived", record.object.get("caption").?.object.get("origin").?.string);
        try std.testing.expectEqualStrings("derived", record.object.get("list").?.object.get("origin").?.string);
        try std.testing.expect(!record.object.get("list").?.object.contains("spans"));
    }
    const literal = try std.json.parseFromSliceLeaky(V, a,
        \\{"source_id":"photo","segments":[{"number":1,"start":0,"end":7,"origin":"interpretation","regions":[{"id":0,"text_start":0,"text_end":7,"origin":"transcription"}]}]}
    , .{});
    var item = try std.json.parseFromSliceLeaky(V, a,
        \\{"structures":{"r":[{"caption":{"value":"Caption"}}]}}
    , .{});
    try std.testing.expect(try attach(a, &item, "Caption", literal, false, false));
}

test "document nonliteral and null values cannot inherit provider literal evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var item = try std.json.parseFromSliceLeaky(V, a,
        \\{"structures":{"r":[{"derived":{"value":"12.50","origin":"derived","start":0,"end":5,"evidence":[{"source_id":"unverified"}]},"estimate":{"value":12.5,"origin":"estimated","start":0,"end":5,"spans":[[0,5]]},"schema_value":{"value":"12.50","source":"schema","start":0,"end":5},"empty":{"value":null,"start":0,"end":5,"evidence":[{"source_id":"unverified"}]}}]}}
    , .{});
    try std.testing.expect(try attach(a, &item, "12.50", null, true, true));
    const record = item.object.get("structures").?.object.get("r").?.array.items[0];
    for (record.object.values()) |field_value| {
        try std.testing.expect(!field_value.object.contains("start"));
        try std.testing.expect(!field_value.object.contains("end"));
        try std.testing.expect(!field_value.object.contains("spans"));
        try std.testing.expect(!field_value.object.contains("evidence"));
    }
    try std.testing.expectEqualStrings("estimated", record.object.get("estimate").?.object.get("origin").?.string);
    try std.testing.expectEqualStrings("derived", record.object.get("schema_value").?.object.get("origin").?.string);
}

test "document relation endpoints inherit checked entity evidence and validate inline spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json =
        \\{"offset_unit":"unicode_codepoints","entities":[{"id":"p","label":"person","text":"Éva","start":0,"end":3},{"id":"o","label":"org","text":"Antfly","start":4,"end":10}],"relations":[{"type":"works_for","source":{"id":"p","evidence":[{"source_id":"fake"}]},"target":{"entity_index":1}},{"type":"works_for","source":{"text":"Éva","start":0,"end":3},"target":{"text":"Antfly","start":4,"end":10}}]}
    ;
    var item = try std.json.parseFromSliceLeaky(V, a, json, .{});
    const metadata = try std.json.parseFromSliceLeaky(V, a,
        \\{"source_id":"slide","segments":[{"number":2,"start":0,"end":11,"origin":"native"}]}
    , .{});
    try std.testing.expect(try attach(a, &item, "Éva Antfly", metadata, true, false));
    for (item.object.get("relations").?.array.items) |relation| {
        const source = field(relation, "source").?;
        const target = field(relation, "target").?;
        const sref = field(source, "evidence").?.array.items[0];
        const tref = field(target, "evidence").?.array.items[0];
        try std.testing.expectEqualStrings("slide", field(sref, "source_id").?.string);
        try std.testing.expectEqual(@as(i64, 2), field(tref, "page_number").?.integer);
        try std.testing.expectEqual(@as(i64, 4), field(sref, "end").?.integer);
        try std.testing.expectEqual(@as(i64, 5), field(tref, "start").?.integer);
    }
    // The primary now contains Antfly evidence; the raw verifier does not.
    const check = try std.json.parseFromSliceLeaky(V, a, json, .{});
    try std.testing.expect(try sameValues(a, item, check));
}

test "document relation evidence rejects dangling contradictory unsupported and interpreted endpoints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const invalid = [_][]const u8{
        "{\"text\":\"Mallory\"}",
        "{\"text\":\"Mallory\",\"start\":0,\"end\":5}",
        "{\"entity_index\":3}",
        "{\"id\":\"missing\"}",
        "{\"entity_index\":0,\"text\":\"Mallory\"}",
        "{\"entity_index\":0,\"start\":1,\"end\":5}",
        "{\"entity_index\":0,\"id\":\"other\"}",
    };
    for (invalid) |endpoint| {
        const json = try std.fmt.allocPrint(a, "{{\"entities\":[{{\"id\":\"p\",\"label\":\"person\",\"text\":\"Alice\",\"start\":0,\"end\":5}}],\"relations\":[{{\"type\":\"works_for\",\"source\":{s},\"target\":{{\"text\":\"Antfly\",\"start\":6,\"end\":12}}}}]}}", .{endpoint});
        var item = try std.json.parseFromSliceLeaky(V, a, json, .{});
        try std.testing.expect(!try attach(a, &item, "Alice Antfly", null, true, false));
    }
    const metadata = try std.json.parseFromSliceLeaky(V, a,
        \\{"source_id":"photo","segments":[{"number":1,"start":0,"end":12,"origin":"interpretation"}]}
    , .{});
    var interpreted_item = try std.json.parseFromSliceLeaky(V, a,
        \\{"relations":[{"type":"works_for","source":{"text":"Alice","start":0,"end":5},"target":{"text":"Antfly","start":6,"end":12}}]}
    , .{});
    try std.testing.expect(!try attach(a, &interpreted_item, "Alice Antfly", metadata, true, false));
    var optional = try std.json.parseFromSliceLeaky(V, a,
        \\{"relations":[{"type":"works_for","source":{"text":"Alice","evidence":[{"source_id":"fake"}]},"target":{"text":"Antfly"}}]}
    , .{});
    try std.testing.expect(try attach(a, &optional, "Alice Antfly", null, false, false));
    try std.testing.expect(field(field(optional, "relations").?.array.items[0].object.get("source").?, "evidence") == null);
}
