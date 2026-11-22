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

const std = @import("std");

/// Read-modify-write of public descriptors cannot round-trip write-only
/// producer JSON. Omission retains the admitted producer of the same kind;
/// explicit producer configuration still replaces it.
pub fn inheritProducer(alloc: std.mem.Allocator, root: std.json.ObjectMap, name: []const u8, replacement: *std.json.Value) !void {
    if (replacement.* != .object or replacement.object.contains("producer_json") or replacement.object.contains("producer")) return;
    const kind = replacement.object.get("kind") orelse return;
    if (kind != .string) return;
    if (root.get("enrichments")) |items| if (try inheritFrom(alloc, items, name, kind.string, replacement)) return;
    var entries = root.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const items = entry.value_ptr.object.get("enrichments") orelse continue;
        if (try inheritFrom(alloc, items, name, kind.string, replacement)) return;
    }
}

fn inheritFrom(alloc: std.mem.Allocator, items: std.json.Value, name: []const u8, kind: []const u8, replacement: *std.json.Value) !bool {
    if (items != .array) return false;
    for (items.array.items) |entry| {
        if (entry != .object) continue;
        const candidate = entry.object.get("name") orelse continue;
        const old_kind = entry.object.get("kind") orelse continue;
        if (candidate != .string or old_kind != .string or !std.mem.eql(u8, candidate.string, name) or !std.mem.eql(u8, old_kind.string, kind)) continue;
        for ([_][]const u8{ "producer_json", "producer" }) |field| if (entry.object.get(field)) |value| {
            try replacement.object.put(alloc, field, value);
            return true;
        };
    }
    return false;
}

/// Artifact names identify a table-wide producer, including declarations
/// originally installed inline with an index. Replace every inline copy before
/// validating the merged catalog, so an obsolete producer does not prevent its
/// own repair. Never walk arbitrary producer/schema objects looking for names.
pub fn replaceInline(root: std.json.ObjectMap, name: []const u8, replacement: std.json.Value) usize {
    var count: usize = 0;
    var entries = root.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const definitions = entry.value_ptr.object.getPtr("enrichments") orelse continue;
        if (definitions.* != .array) continue;
        for (definitions.array.items) |*definition| {
            if (definition.* != .object) continue;
            const candidate = definition.object.get("name") orelse continue;
            if (candidate != .string or !std.mem.eql(u8, candidate.string, name)) continue;
            definition.* = replacement;
            count += 1;
        }
    }
    return count;
}

/// Delete every declaration of the table-level producer. Consumers remain in
/// place so normal catalog validation can reject a dangling dependency.
pub fn removeInline(root: std.json.ObjectMap, name: []const u8) usize {
    var count: usize = 0;
    var entries = root.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const definitions = entry.value_ptr.object.getPtr("enrichments") orelse continue;
        if (definitions.* != .array) continue;
        var index: usize = 0;
        while (index < definitions.array.items.len) {
            const definition = definitions.array.items[index];
            const candidate = if (definition == .object) definition.object.get("name") else null;
            if (candidate != null and candidate.? == .string and std.mem.eql(u8, candidate.?.string, name)) {
                _ = definitions.array.orderedRemove(index);
                count += 1;
            } else index += 1;
        }
    }
    return count;
}

test "replacement repairs every inline producer without touching index sources or other producers" {
    const alloc = std.testing.allocator;
    var catalog = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"first":{"type":"full_text","sources":[{"artifact":"units"}],"enrichments":[{"name":"units","producer_json":"obsolete"},{"name":"other","producer_json":"keep"}]},"second":{"type":"embeddings","enrichments":[{"name":"units","producer_json":"obsolete"}]},"enrichments":[{"name":"unrelated"}]}
    , .{});
    defer catalog.deinit();
    var replacement = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"name":"units","kind":"asset","field":"url","producer_json":"canonical"}
    , .{});
    defer replacement.deinit();
    try std.testing.expectEqual(@as(usize, 2), replaceInline(catalog.value.object, "units", replacement.value));
    const encoded = try std.json.Stringify.valueAlloc(alloc, catalog.value, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "obsolete") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "keep") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"sources\":[{\"artifact\":\"units\"}]") != null);
    try std.testing.expectEqual(@as(usize, 0), replaceInline(catalog.value.object, "missing", replacement.value));
    try std.testing.expectEqual(@as(usize, 2), removeInline(catalog.value.object, "units"));
    try std.testing.expectEqual(@as(usize, 0), removeInline(catalog.value.object, "units"));
    const remaining = catalog.value.object.get("first").?.object.get("enrichments").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualStrings("other", remaining[0].object.get("name").?.string);
    try std.testing.expectEqual(@as(usize, 0), catalog.value.object.get("second").?.object.get("enrichments").?.array.items.len);
}

test "execution update retains omitted private producer from root or inline declaration" {
    const a = std.testing.allocator;
    inline for (.{ "{\"enrichments\":[{\"name\":\"units\",\"kind\":\"asset\",\"producer_json\":\"private recipe\"}]}", "{\"text\":{\"enrichments\":[{\"name\":\"units\",\"kind\":\"asset\",\"producer_json\":\"private recipe\"}]}}" }) |raw| {
        var catalog = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer catalog.deinit();
        var replacement = try std.json.parseFromSlice(std.json.Value, a, "{\"name\":\"units\",\"kind\":\"asset\",\"execution\":{\"max_output_tokens_per_item\":8192}}", .{});
        defer replacement.deinit();
        try inheritProducer(replacement.arena.allocator(), catalog.value.object, "units", &replacement.value);
        try std.testing.expectEqualStrings("private recipe", replacement.value.object.get("producer_json").?.string);
        try replacement.value.object.put(replacement.arena.allocator(), "producer_json", .{ .string = "replacement" });
        try inheritProducer(replacement.arena.allocator(), catalog.value.object, "units", &replacement.value);
        try std.testing.expectEqualStrings("replacement", replacement.value.object.get("producer_json").?.string);
    }
}
