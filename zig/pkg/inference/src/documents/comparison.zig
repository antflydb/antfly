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

//! Semantic projections are canonicalized once and sorted as multisets.
//! Confidence, provenance and provider bookkeeping never participate in votes.
const std = @import("std");
const refs = @import("entity_index.zig");
const field = refs.field;
const A = std.mem.Allocator;
const V = std.json.Value;

pub fn sameValues(backing: A, left: V, right: V) !bool {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    return compare(arena.allocator(), left, right) catch |err| switch (err) {
        error.InvalidDocumentModelResult => false,
        else => return err,
    };
}
fn compare(a: A, left: V, right: V) !bool {
    var identities = Identities{};
    const lidx = try Projection.init(a, left, &identities);
    const ridx = try Projection.init(a, right, &identities);
    for ([_][]const u8{ "structures", "entities", "relations", "classifications" }) |name| {
        const l = field(left, name);
        const r = field(right, name);
        if ((l == null) != (r == null)) return false;
        if (l == null) continue;
        const lp = try project(a, name, l.?, lidx);
        const rp = try project(a, name, r.?, ridx);
        if (!std.mem.eql(u8, try encode(a, lp), try encode(a, rp))) return false;
    }
    return true;
}
// Preserve complete semantic identity at relation endpoints. Canonicalize
// attributes once per entity, even when a high-degree entity has many edges.
const Identities = struct {
    keys: std.StringHashMapUnmanaged(usize) = .empty,
    fn intern(self: *Identities, a: A, value: V) !V {
        const key = try encode(a, value);
        const entry = try self.keys.getOrPut(a, key);
        if (!entry.found_existing) entry.value_ptr.* = self.keys.count() - 1;
        return .{ .integer = std.math.cast(i64, entry.value_ptr.*) orelse return error.InvalidDocumentModelResult };
    }
};
const Projection = struct {
    index: refs.Index,
    entities: []V,
    identities: *Identities,
    fn init(a: A, root: V, identities: *Identities) !Projection {
        const index = try refs.Index.init(a, root);
        const entities = try a.alloc(V, index.entities.len);
        for (index.entities, entities) |value, *identity| identity.* = try identities.intern(a, try entity(a, value));
        return .{ .index = index, .entities = entities, .identities = identities };
    }
};
fn object() V {
    return .{ .object = .{} };
}
fn put(a: A, obj: *V, key: []const u8, value: V) !void {
    try obj.object.put(a, key, value);
}
fn required(value: V, key: []const u8) !V {
    return field(value, key) orelse error.InvalidDocumentModelResult;
}
fn string(value: V, key: []const u8) !V {
    const result = try required(value, key);
    return if (result == .string) result else error.InvalidDocumentModelResult;
}
fn project(a: A, name: []const u8, value: V, projection: Projection) !V {
    if (std.mem.eql(u8, name, "structures")) {
        if (value != .object) return error.InvalidDocumentModelResult;
        var result = object();
        for (value.object.keys(), value.object.values()) |key, records| {
            if (records != .array) return error.InvalidDocumentModelResult;
            var rows: std.array_list.Managed(V) = .init(a);
            for (records.array.items) |record| {
                if (record != .object) return error.InvalidDocumentModelResult;
                var row = object();
                for (record.object.keys(), record.object.values()) |fname, wrapped| try put(a, &row, fname, try fieldValue(a, wrapped, 0));
                try rows.append(row);
            }
            try put(a, &result, key, try bag(a, rows.items));
        }
        return result;
    }
    if (value != .array) return error.InvalidDocumentModelResult;
    if (std.mem.eql(u8, name, "entities")) return bag(a, projection.entities);
    var rows: std.array_list.Managed(V) = .init(a);
    for (value.array.items) |item| {
        var row = object();
        if (std.mem.eql(u8, name, "classifications")) {
            try put(a, &row, "name", try string(item, "name"));
            try put(a, &row, "label", try string(item, "label"));
        } else {
            try put(a, &row, "type", try string(item, "type"));
            try put(a, &row, "source", try endpoint(a, try required(item, "source"), projection));
            try put(a, &row, "target", try endpoint(a, try required(item, "target"), projection));
        }
        try rows.append(row);
    }
    return bag(a, rows.items);
}
fn entity(a: A, value: V) !V {
    var result = object();
    try put(a, &result, "label", try string(value, "label"));
    try put(a, &result, "text", try string(value, "text"));
    var attributes = object();
    if (field(value, "attributes")) |groups| {
        if (groups != .object) return error.InvalidDocumentModelResult;
        for (groups.object.keys(), groups.object.values()) |key, selection| {
            var labels: std.array_list.Managed(V) = .init(a);
            if (selection == .array) {
                for (selection.array.items) |s| try labels.append(try string(s, "label"));
            } else try labels.append(try string(selection, "label"));
            try put(a, &attributes, key, try bag(a, labels.items));
        }
    }
    try put(a, &result, "attributes", attributes);
    return result;
}
fn endpoint(a: A, value: V, projection: Projection) !V {
    switch (try projection.index.resolve(value)) {
        .entity => |i| return projection.entities[i],
        .inline_value => {
            var result = object();
            try put(a, &result, "text", try string(value, "text"));
            // Inline endpoints have no referenced entity attributes. Keeping
            // that empty identity distinguishes them from attributed entities.
            if (field(value, "label")) |_| try put(a, &result, "label", try string(value, "label"));
            try put(a, &result, "attributes", object());
            return projection.identities.intern(a, result);
        },
        .unresolved => return error.InvalidDocumentModelResult,
    }
}
fn fieldValue(a: A, value: V, depth: usize) error{ OutOfMemory, InvalidDocumentModelResult }!V {
    if (depth > 64) return error.InvalidDocumentModelResult;
    if (value == .object) return required(value, "value");
    if (value == .array) {
        var values: std.array_list.Managed(V) = .init(a);
        for (value.array.items) |v| try values.append(try fieldValue(a, v, depth + 1));
        return .{ .array = values };
    }
    return value;
}
fn less(_: void, l: []const u8, r: []const u8) bool {
    return std.mem.lessThan(u8, l, r);
}
fn bag(a: A, values: []const V) !V {
    const encoded = try a.alloc([]const u8, values.len);
    for (values, encoded) |v, *out| out.* = try encode(a, v);
    return sortedTokens(a, encoded);
}
fn sortedTokens(a: A, encoded: [][]const u8) !V {
    std.mem.sort([]const u8, encoded, {}, less);
    var result: std.array_list.Managed(V) = .init(a);
    for (encoded) |v| try result.append(.{ .string = v });
    return .{ .array = result };
}
fn encode(a: A, value: V) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, try canonical(a, value, 0), .{});
}
fn canonical(a: A, value: V, depth: usize) error{ OutOfMemory, InvalidDocumentModelResult }!V {
    if (depth > 64) return error.InvalidDocumentModelResult;
    if (value == .object) {
        const keys = try a.dupe([]const u8, value.object.keys());
        std.mem.sort([]const u8, keys, {}, less);
        var result = object();
        for (keys) |key| try put(a, &result, key, try canonical(a, value.object.get(key).?, depth + 1));
        return result;
    }
    if (value == .array) {
        var result: std.array_list.Managed(V) = .init(a);
        for (value.array.items) |v| try result.append(try canonical(a, v, depth + 1));
        return .{ .array = result };
    }
    if (value == .integer or value == .float or value == .number_string) {
        if (value == .float and !std.math.isFinite(value.float)) return error.InvalidDocumentModelResult;
        const raw = if (value == .number_string) value.number_string else try std.json.Stringify.valueAlloc(a, value, .{});
        return .{ .number_string = try number(a, raw) };
    }
    return value;
}
// Normalize decimal coefficients and exponents without f64 coercion or
// expanding huge exponents. Exact integer/decimal lexemes retain precision.
fn number(a: A, raw: []const u8) ![]const u8 {
    if (raw.len == 0) return error.InvalidDocumentModelResult;
    const negative = raw[0] == '-';
    var i: usize = @intFromBool(negative);
    const begin = i;
    while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
    if (i == begin or (i - begin > 1 and raw[begin] == '0')) return error.InvalidDocumentModelResult;
    var fraction: usize = 0;
    if (i < raw.len and raw[i] == '.') {
        i += 1;
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        fraction = i - start;
        if (fraction == 0) return error.InvalidDocumentModelResult;
    }
    const mantissa_end = i;
    var exponent: i64 = 0;
    if (i < raw.len and (raw[i] == 'e' or raw[i] == 'E')) {
        i += 1;
        const start = i;
        if (i < raw.len and (raw[i] == '+' or raw[i] == '-')) i += 1;
        const digits_start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        if (i == digits_start) return error.InvalidDocumentModelResult;
        exponent = std.fmt.parseInt(i64, raw[start..i], 10) catch return error.InvalidDocumentModelResult;
    }
    if (i != raw.len) return error.InvalidDocumentModelResult;
    const digits = try a.alloc(u8, mantissa_end - begin);
    var len: usize = 0;
    for (raw[begin..mantissa_end]) |ch| if (ch != '.') {
        digits[len] = ch;
        len += 1;
    };
    var first: usize = 0;
    while (first < len and digits[first] == '0') : (first += 1) {}
    if (first == len) return "0";
    var last = len;
    while (digits[last - 1] == '0') : (last -= 1) {}
    exponent = std.math.sub(i64, exponent, std.math.cast(i64, fraction) orelse return error.InvalidDocumentModelResult) catch return error.InvalidDocumentModelResult;
    exponent = std.math.add(i64, exponent, std.math.cast(i64, len - last) orelse return error.InvalidDocumentModelResult) catch return error.InvalidDocumentModelResult;
    return std.fmt.allocPrint(a, "{s}{s}e{d}", .{ if (negative) "-" else "", digits[first..last], exponent });
}

test "document comparison ignores metadata and reindexes unordered semantic results" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try std.json.parseFromSliceLeaky(V, a,
        \\{"entities":[{"id":"p","label":"person","text":"Alice","score":0.9,"attributes":{"role":{"label":"staff","confidence":0.1}}},{"id":"o","label":"org","text":"Antfly"}],"relations":[{"type":"works_for","source":{"entity_index":0},"target":{"id":"o"},"score":0.8}],"classifications":[{"name":"kind","label":"work","score":0.9},{"name":"tone","label":"positive"}],"structures":{"record":[{"n":{"value":1}},{"n":{"value":9007199254740993}}]}}
    , .{});
    var right = try std.json.parseFromSliceLeaky(V, a,
        \\{"entities":[{"label":"org","text":"Antfly","evidence":[]},{"label":"person","text":"Alice","start":50,"end":55,"attributes":{"role":{"label":"staff","confidence":0.9}}}],"relations":[{"type":"works_for","source":{"entity_index":1},"target":{"label":"org","text":"Antfly"},"score":0.2,"derived":true}],"classifications":[{"name":"tone","label":"positive","score":0.1},{"name":"kind","label":"work","score":0.4}],"structures":{"record":[{"n":{"value":9007199254740993}},{"n":{"value":1.0,"score":0.2}}]}}
    , .{ .parse_numbers = false });
    try std.testing.expect(try sameValues(a, left, right));
    try right.object.getPtr("relations").?.array.items[0].object.put(a, "type", .{ .string = "owns" });
    try std.testing.expect(!try sameValues(a, left, right));
}

test "document comparison retains attributes multiplicity direction and ordered field data" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_][2][]const u8{
        .{ "{\"entities\":[{\"label\":\"person\",\"text\":\"Alice\",\"attributes\":{\"role\":{\"label\":\"staff\"}}}]}", "{\"entities\":[{\"label\":\"person\",\"text\":\"Alice\",\"attributes\":{\"role\":{\"label\":\"guest\"}}}]}" },
        .{ "{\"entities\":[{\"label\":\"person\",\"text\":\"Alice\"},{\"label\":\"person\",\"text\":\"Alice\"}]}", "{\"entities\":[{\"label\":\"person\",\"text\":\"Alice\"}]}" },
        .{ "{\"relations\":[{\"type\":\"knows\",\"source\":{\"text\":\"Alice\"},\"target\":{\"text\":\"Bob\"}}]}", "{\"relations\":[{\"type\":\"knows\",\"source\":{\"text\":\"Bob\"},\"target\":{\"text\":\"Alice\"}}]}" },
        .{ "{\"structures\":{\"r\":[{\"v\":{\"value\":[\"a\",\"b\"]}}]}}", "{\"structures\":{\"r\":[{\"v\":{\"value\":[\"b\",\"a\"]}}]}}" },
        .{ "{\"structures\":{\"r\":[{\"v\":{\"value\":{\"score\":1}}}]}}", "{\"structures\":{\"r\":[{\"v\":{\"value\":{\"score\":2}}}]}}" },
        .{ "{\"structures\":{\"r\":[{\"v\":{\"value\":9007199254740993}}]}}", "{\"structures\":{\"r\":[{\"v\":{\"value\":9007199254740992}}]}}" },
    };
    for (cases) |pair| {
        const left = try std.json.parseFromSliceLeaky(V, a, pair[0], .{ .parse_numbers = false });
        const right = try std.json.parseFromSliceLeaky(V, a, pair[1], .{ .parse_numbers = false });
        try std.testing.expect(!try sameValues(a, left, right));
    }
    const bad = try std.json.parseFromSliceLeaky(V, a, "{\"relations\":[{\"type\":\"x\",\"source\":{\"entity_index\":0},\"target\":{\"text\":\"y\"}}]}", .{});
    try std.testing.expect(!try sameValues(a, bad, bad));
}

test "document comparison decimal canonicalization is exact and bounded" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_][2][]const u8{ .{ "1.000", "1e0" }, .{ "-0e99", "0" }, .{ "0.00100", "1e-3" }, .{ "10e999999", "1e1000000" }, .{ "-123.40e+2", "-1234e1" } };
    for (cases) |pair| {
        const result = try number(a, pair[0]);
        try std.testing.expectEqualStrings(pair[1], result);
    }
}

test "document relation verification preserves attributed endpoint identity across reordering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try std.json.parseFromSliceLeaky(V, a,
        \\{"entities":[{"id":"buyer","label":"person","text":"Alex","attributes":{"role":{"label":"buyer","confidence":0.8}}},{"id":"seller","label":"person","text":"Alex","attributes":{"role":{"label":"seller"}}},{"id":"org","label":"org","text":"Antfly"}],"relations":[{"type":"works_for","source":{"entity_index":0},"target":{"id":"org"}}]}
    , .{});
    var right = try std.json.parseFromSliceLeaky(V, a,
        \\{"entities":[{"id":"company","label":"org","text":"Antfly"},{"id":"s","label":"person","text":"Alex","attributes":{"role":{"label":"seller"}}},{"id":"b","label":"person","text":"Alex","attributes":{"role":{"label":"buyer","confidence":0.1}}}],"relations":[{"type":"works_for","source":{"id":"b"},"target":{"entity_index":0}}]}
    , .{});
    try std.testing.expect(try sameValues(a, left, right));
    try right.object.getPtr("relations").?.array.items[0].object.getPtr("source").?.object.put(a, "id", .{ .string = "s" });
    try std.testing.expect(!try sameValues(a, left, right));
}
