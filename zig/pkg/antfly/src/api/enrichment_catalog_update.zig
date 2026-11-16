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
