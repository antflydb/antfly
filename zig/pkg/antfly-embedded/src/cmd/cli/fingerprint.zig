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

//! Versioned, response-local fingerprints. No digest asserts a database snapshot.
const std = @import("std");
const Value = std.json.Value;
const Hash = std.crypto.hash.Blake3;
pub const Ordering = enum { ordered, multiset };
pub const Record = struct {
    version: u32 = 1,
    algorithm: []const u8 = "blake3-256",
    encoding: []const u8 = "antfly-typed-values-v1",
    scope: []const u8,
    ordering: Ordering,
    snapshot: []const u8 = "single_response; no cross-request snapshot guarantee",
    units: usize = 0,
    digest: ?[]const u8 = null,
    unavailable: ?[]const u8 = null,
};
fn get(v: Value, key: []const u8) Value {
    return if (v == .object) v.object.get(key) orelse .null else .null;
}
fn items(v: Value) []const Value {
    return if (v == .array) v.array.items else &.{};
}
fn length(h: *Hash, n: usize) void {
    var encoded_length: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_length, @intCast(n), .big);
    h.update(&encoded_length);
}
fn bytes(h: *Hash, tag: []const u8, value: []const u8) void {
    h.update(tag);
    length(h, value.len);
    h.update(value);
}
fn canonical(a: std.mem.Allocator, h: *Hash, value: Value) anyerror!void {
    switch (value) {
        .null => h.update("N"),
        .bool => |v| h.update(if (v) "T" else "F"),
        .string => |v| bytes(h, "S", v),
        .number_string => |v| bytes(h, "D", v),
        .integer, .float => bytes(h, "D", try std.json.Stringify.valueAlloc(a, value, .{})),
        .array => |v| {
            h.update("A");
            length(h, v.items.len);
            for (v.items) |item| try canonical(a, h, item);
        },
        .object => |v| {
            h.update("O");
            length(h, v.count());
            const keys = try a.dupe([]const u8, v.keys());
            defer a.free(keys);
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.less);
            for (keys) |key| {
                bytes(h, "K", key);
                try canonical(a, h, v.get(key).?);
            }
        },
    }
}
fn add(a: std.mem.Allocator, hashes: *std.ArrayList([32]u8), value: Value, mask: Value) !void {
    var h = Hash.init(.{});
    h.update("antfly-unit-v1");
    try canonical(a, &h, value);
    // SQL null masks are authoritative; absent masks use legacy null semantics.
    if (value == .array) {
        const flags = items(mask);
        for (value.array.items, 0..) |cell, i| {
            const sql_null = if (i < flags.len) flags[i] == .bool and flags[i].bool else cell == .null;
            h.update(if (sql_null) "Q" else "V");
        }
    }
    var digest: [32]u8 = undefined;
    h.final(&digest);
    try hashes.append(a, digest);
}
pub fn make(a: std.mem.Allocator, ordering: Ordering, scope: []const u8, complete: bool, value: Value) !Record {
    var record = Record{ .scope = scope, .ordering = ordering };
    if (!complete or std.mem.eql(u8, scope, "operation") or std.mem.eql(u8, scope, "plan") or std.mem.eql(u8, scope, "workflow")) {
        record.unavailable = "execution_or_result_scope_not_complete";
        return record;
    }
    var hashes: std.ArrayList([32]u8) = .empty;
    defer hashes.deinit(a);
    var h = Hash.init(.{});
    bytes(&h, "C", "antfly-result-v1");
    bytes(&h, "S", scope);
    bytes(&h, "R", @tagName(ordering));
    if (std.mem.eql(u8, scope, "sql")) {
        record.scope = "complete_sql_result";
        try canonical(a, &h, get(value, "columns"));
        const masks = items(get(value, "sql_nulls"));
        for (items(get(value, "rows")), 0..) |row, i| try add(a, &hashes, row, if (i < masks.len) masks[i] else .null);
    } else if (std.mem.eql(u8, scope, "search")) {
        record.scope = "requested_top_k";
        // Keep response/table boundaries and aggregation values; only hit order
        // is ignored by multiset mode. Timing and lower-bound totals aren't rows.
        for (items(get(value, "responses"))) |response| {
            try canonical(a, &h, get(response, "table"));
            try canonical(a, &h, get(response, "aggregations"));
            var response_hashes: std.ArrayList([32]u8) = .empty;
            defer response_hashes.deinit(a);
            for (items(get(get(response, "hits"), "hits"))) |hit| try add(a, &response_hashes, hit, .null);
            fold(&h, response_hashes.items, ordering);
            record.units += response_hashes.items.len;
        }
    } else if (std.mem.eql(u8, scope, "discovery") and value == .array) {
        record.scope = "discovery_response";
        for (value.array.items) |item| try add(a, &hashes, item, .null);
    } else {
        record.scope = if (std.mem.eql(u8, scope, "lookup")) "lookup_document" else "discovery_response";
        try add(a, &hashes, value, .null);
    }
    if (!std.mem.eql(u8, scope, "search")) {
        fold(&h, hashes.items, ordering);
        record.units = hashes.items.len;
    }
    var digest: [32]u8 = undefined;
    h.final(&digest);
    record.digest = try a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    return record;
}
fn fold(h: *Hash, hashes: [][32]u8, ordering: Ordering) void {
    if (ordering == .multiset) std.mem.sort([32]u8, hashes, {}, struct {
        fn less(_: void, l: [32]u8, r: [32]u8) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.less);
    length(h, hashes.len);
    for (hashes) |digest| h.update(&digest);
}

test "fingerprints canonicalize keys and preserve duplicates order types and SQL nulls" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const inputs = [_][]const u8{
        "{\"columns\":[{\"name\":\"j\",\"type\":\"json\"}],\"rows\":[[null],[\"null\"],[null]],\"sql_nulls\":[[true],[false],[false]]}",
        "{\"rows\":[[null],[null],[\"null\"]],\"columns\":[{\"type\":\"json\",\"name\":\"j\"}],\"sql_nulls\":[[false],[true],[false]]}",
        "{\"columns\":[{\"name\":\"j\",\"type\":\"json\"}],\"rows\":[[null],[\"null\"]],\"sql_nulls\":[[true],[false]]}",
    };
    var values: [3]Value = undefined;
    for (inputs, &values) |raw, *v| v.* = (try std.json.parseFromSlice(Value, alloc, raw, .{ .parse_numbers = false })).value;
    const first = try make(alloc, .multiset, "sql", true, values[0]);
    const reordered = try make(alloc, .multiset, "sql", true, values[1]);
    try std.testing.expectEqualStrings(first.digest.?, reordered.digest.?);
    const fewer = try make(alloc, .multiset, "sql", true, values[2]);
    try std.testing.expect(!std.mem.eql(u8, first.digest.?, fewer.digest.?));
    const ordered = try make(alloc, .ordered, "sql", true, values[0]);
    const changed = try make(alloc, .ordered, "sql", true, values[1]);
    try std.testing.expect(!std.mem.eql(u8, ordered.digest.?, changed.digest.?));
    try std.testing.expect((try make(alloc, .ordered, "sql", false, values[0])).digest == null);
}
