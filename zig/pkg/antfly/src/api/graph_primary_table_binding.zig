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

//! Bind only MATCH aliases explicitly naming the already-resolved primary
//! catalog target. Foreign names and existing physical qualifiers are preserved.
const std = @import("std");

pub fn bind(alloc: std.mem.Allocator, body: []const u8, primary_names: []const []const u8, physical: []const u8) !?[]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const queries = parsed.value.object.getPtr("graph_queries") orelse return null;
    if (queries.* != .object) return null;
    var changed = false;
    var operations = queries.object.iterator();
    while (operations.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const match = entry.value_ptr.object.getPtr("match") orelse continue;
        if (match.* != .object) continue;
        if (match.object.getPtr("nodes")) |nodes| bindNodes(nodes, primary_names, physical, &changed);
        if (match.object.getPtr("optional")) |groups| {
            if (groups.* != .array) continue;
            for (groups.array.items) |*group| {
                if (group.* != .object) continue;
                if (group.object.getPtr("nodes")) |nodes| bindNodes(nodes, primary_names, physical, &changed);
            }
        }
    }
    if (!changed) return null;
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn bindNodes(nodes: *std.json.Value, primary_names: []const []const u8, physical: []const u8, changed: *bool) void {
    if (nodes.* != .object) return;
    var entries = nodes.object.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const table = entry.value_ptr.object.getPtr("table") orelse continue;
        if (table.* != .string or std.mem.eql(u8, table.string, physical)) continue;
        for (primary_names) |logical| {
            if (!std.mem.eql(u8, table.string, logical)) continue;
            table.* = .{ .string = physical };
            changed.* = true;
            break;
        }
    }
}

test "system catalog graph primary binding preserves omitted physical and foreign qualifiers" {
    const a = std.testing.allocator;
    const body =
        \\{"remote_snapshot":"captured-cut","timeout_ms":250,"graph_queries":{"q":{"match":{"nodes":{"logical":{"table":"docs"},"physical":{"table":"table:primary"},"omitted":{},"foreign":{"table":"other"}},"optional":[{"nodes":{"extra":{"table":"docs"},"foreign":{"table":"other"}}}]}}}}
    ;
    const encoded = (try bind(a, body, &.{"docs"}, "table:primary")).?;
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("captured-cut", parsed.value.object.get("remote_snapshot").?.string);
    try std.testing.expectEqual(@as(i64, 250), parsed.value.object.get("timeout_ms").?.integer);
    const match = parsed.value.object.get("graph_queries").?.object.get("q").?.object.get("match").?;
    const nodes = match.object.get("nodes").?.object;
    try std.testing.expectEqualStrings("table:primary", nodes.get("logical").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("table:primary", nodes.get("physical").?.object.get("table").?.string);
    try std.testing.expect(!nodes.get("omitted").?.object.contains("table"));
    try std.testing.expectEqualStrings("other", nodes.get("foreign").?.object.get("table").?.string);
    const optional = match.object.get("optional").?.array.items[0].object.get("nodes").?.object;
    try std.testing.expectEqualStrings("table:primary", optional.get("extra").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("other", optional.get("foreign").?.object.get("table").?.string);
}

test "system catalog graph primary binding never guesses qualification from a basename" {
    const a = std.testing.allocator;
    const body =
        \\{"graph_queries":{"q":{"match":{"nodes":{"same":{"table":"scope-one/docs"},"other_namespace":{"table":"scope-two/docs"},"bare":{"table":"docs"}}}}}}
    ;
    const encoded = (try bind(a, body, &.{"scope-one/docs"}, "table:primary")).?;
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer parsed.deinit();
    const nodes = parsed.value.object.get("graph_queries").?.object.get("q").?.object.get("match").?.object.get("nodes").?.object;
    try std.testing.expectEqualStrings("table:primary", nodes.get("same").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("scope-two/docs", nodes.get("other_namespace").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("docs", nodes.get("bare").?.object.get("table").?.string);
    try std.testing.expectEqual(@as(?[]u8, null), try bind(a, "{\"graph_queries\":{\"q\":{\"match\":{\"nodes\":{\"a\":{}}}}}}", &.{"docs"}, "table:primary"));
}

test "system catalog graph primary binding preserves exact unrelated numeric literals" {
    const a = std.testing.allocator;
    const body =
        \\{"filter_query":{"conjuncts":[{"term":{"path":"/n","value":18446744073709551615}},{"term":{"path":"/p","value":0.12345678901234567890123456789}}]},"graph_queries":{"q":{"match":{"nodes":{"a":{"table":"docs"}}}}}}
    ;
    const encoded = (try bind(a, body, &.{"docs"}, "table:primary")).?;
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "18446744073709551615") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "0.12345678901234567890123456789") != null);
}
