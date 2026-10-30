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

//! Bind structural graph identities explicitly naming the already-resolved primary
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
        if (entry.value_ptr.object.getPtr("match")) |match| {
            if (match.* == .object) {
                if (match.object.getPtr("nodes")) |nodes| bindNodes(nodes, primary_names, physical, &changed);
                if (match.object.getPtr("optional")) |groups| {
                    if (groups.* == .array) for (groups.array.items) |*group| {
                        if (group.* != .object) continue;
                        if (group.object.getPtr("nodes")) |nodes| bindNodes(nodes, primary_names, physical, &changed);
                    };
                }
            }
        }
        if (entry.value_ptr.object.getPtr("traverse")) |traverse| {
            if (traverse.* == .object) if (traverse.object.getPtr("start")) |start| {
                if (start.* == .object) if (start.object.getPtr("identities")) |identities| {
                    if (identities.* == .array) for (identities.array.items) |*identity| {
                        bindIdentity(identity, primary_names, physical, &changed);
                    };
                };
            };
        }
        inline for (.{ "shortest_path", "k_shortest_paths" }) |kind| {
            if (entry.value_ptr.object.getPtr(kind)) |path| {
                if (path.* == .object) inline for (.{ "from", "to" }) |endpoint| {
                    if (path.object.getPtr(endpoint)) |identity| bindIdentity(identity, primary_names, physical, &changed);
                };
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
        bindIdentity(entry.value_ptr, primary_names, physical, changed);
    }
}

fn bindIdentity(identity: *std.json.Value, primary_names: []const []const u8, physical: []const u8, changed: *bool) void {
    if (identity.* != .object) return;
    const table = identity.object.getPtr("table") orelse return;
    if (table.* != .string or std.mem.eql(u8, table.string, physical)) return;
    for (primary_names) |logical| {
        if (!std.mem.eql(u8, table.string, logical)) continue;
        table.* = .{ .string = physical };
        changed.* = true;
        break;
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

test "system catalog graph primary binding scopes only traversal and path selector identities" {
    const a = std.testing.allocator;
    const body =
        \\{"remote_snapshot":"cut","graph_queries":{"walk":{"traverse":{"start":{"identities":[{"key":"a","table":"docs"},{"key":"b","table":"foreign"},{"key":"c","table":"table:primary"},{"key":"d"}]},"filter":{"term":{"table":"docs","value":18446744073709551615}}}},"shortest":{"shortest_path":{"from":{"key":"a","table":"docs"},"to":{"key":"b","table":"docs"}}},"alternatives":{"k_shortest_paths":{"from":{"key":"a","table":"foreign"},"to":{"key":"b","table":"table:primary"},"k":2}}}}
    ;
    const encoded = (try bind(a, body, &.{"docs"}, "table:primary")).?;
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, encoded, .{ .parse_numbers = false });
    defer parsed.deinit();
    const queries = parsed.value.object.get("graph_queries").?.object;
    const walk = queries.get("walk").?.object.get("traverse").?.object;
    const identities = walk.get("start").?.object.get("identities").?.array.items;
    try std.testing.expectEqualStrings("table:primary", identities[0].object.get("table").?.string);
    try std.testing.expectEqualStrings("foreign", identities[1].object.get("table").?.string);
    try std.testing.expectEqualStrings("table:primary", identities[2].object.get("table").?.string);
    try std.testing.expect(!identities[3].object.contains("table"));
    try std.testing.expectEqualStrings("docs", walk.get("filter").?.object.get("term").?.object.get("table").?.string);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "18446744073709551615") != null);
    try std.testing.expectEqualStrings("cut", parsed.value.object.get("remote_snapshot").?.string);
    const shortest = queries.get("shortest").?.object.get("shortest_path").?.object;
    try std.testing.expectEqualStrings("table:primary", shortest.get("from").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("table:primary", shortest.get("to").?.object.get("table").?.string);
    const alternatives = queries.get("alternatives").?.object.get("k_shortest_paths").?.object;
    try std.testing.expectEqualStrings("foreign", alternatives.get("from").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("table:primary", alternatives.get("to").?.object.get("table").?.string);
}
