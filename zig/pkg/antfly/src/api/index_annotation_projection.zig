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

//! Presentation-only overlay on existing index resources. Configuration and
//! status retain their existing serializers and authority; notes never create
//! resources or change their runtime identities.
const std = @import("std");
pub const max_output_bytes = 16 * 1024 * 1024;

/// Return null when no output changes. `body` and every annotation are borrowed.
/// The generic two-field note view keeps this renderer independent of storage.
pub fn encode(alloc: std.mem.Allocator, body: []const u8, notes: anytype) !?[]u8 {
    if (notes.len == 0) return null;
    if (body.len > max_output_bytes) return error.CatalogCommandTooLarge;
    var names: std.StringHashMapUnmanaged(usize) = .empty;
    defer names.deinit(alloc);
    for (notes, 0..) |note, i| {
        if (note.name.len == 0 or !std.unicode.utf8ValidateSlice(note.name) or std.mem.indexOfScalar(u8, note.name, 0) != null) return error.InvalidIndexAnnotationProjection;
        if (note.description) |text| if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidIndexAnnotationProjection;
        const entry = try names.getOrPut(alloc, note.name);
        if (entry.found_existing) return error.InvalidIndexAnnotationProjection;
        entry.value_ptr.* = i;
    }
    // Overlay text without coercing unrelated configuration/status numbers.
    // In particular, u64 counters and exact decimal metadata exceed f64/i64.
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    const Resources = struct {
        fn apply(a: std.mem.Allocator, resource: *std.json.Value, lookup: *const std.StringHashMapUnmanaged(usize), descriptions: @TypeOf(notes)) !bool {
            if (resource.* != .object) return error.InvalidIndexAnnotationProjection;
            const config = resource.object.getPtr("config") orelse return error.InvalidIndexAnnotationProjection;
            if (config.* != .object) return error.InvalidIndexAnnotationProjection;
            const name = config.object.get("name") orelse return error.InvalidIndexAnnotationProjection;
            if (name != .string) return error.InvalidIndexAnnotationProjection;
            const ordinal = lookup.get(name.string) orelse return false;
            const description = descriptions[ordinal].description;
            if (description) |text| {
                if (config.object.get("description")) |current| if (current == .string and std.mem.eql(u8, current.string, text)) return false;
                try config.object.put(a, "description", .{ .string = text });
                return true;
            }
            return config.object.swapRemove("description");
        }
    };
    var changed = false;
    switch (parsed.value) {
        .array => |*entries| for (entries.items) |*resource| {
            changed = try Resources.apply(parsed.arena.allocator(), resource, &names, notes) or changed;
        },
        .object => changed = try Resources.apply(parsed.arena.allocator(), &parsed.value, &names, notes),
        else => return error.InvalidIndexAnnotationProjection,
    }
    if (!changed) return null;
    const encoded = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(encoded);
    if (encoded.len > max_output_bytes) return error.CatalogCommandTooLarge;
    return encoded;
}

const Note = struct { name: []const u8, description: ?[]const u8 };
test "index annotation projection shares list and individual resources removes inline text and preserves configuration and status" {
    const a = std.testing.allocator;
    const body = "[{\"config\":{\"name\":\"a\",\"description\":\"inline\",\"type\":\"relational\",\"metadata\":{\"description\":\"literal\"}},\"status\":{\"pending\":3}},{\"config\":{\"name\":\"b\",\"type\":\"graph\"}}]";
    const notes = [_]Note{ .{ .name = "a", .description = null }, .{ .name = "b", .description = "current Ω" } };
    const result = (try encode(a, body, &notes)).?;
    defer a.free(result);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result, .{});
    defer parsed.deinit();
    const first = parsed.value.array.items[0];
    try std.testing.expect(!first.object.get("config").?.object.contains("description"));
    try std.testing.expectEqualStrings("literal", first.object.get("config").?.object.get("metadata").?.object.get("description").?.string);
    try std.testing.expectEqual(@as(i64, 3), first.object.get("status").?.object.get("pending").?.integer);
    try std.testing.expectEqualStrings("current Ω", parsed.value.array.items[1].object.get("config").?.object.get("description").?.string);
    const single = (try encode(a, "{\"config\":{\"name\":\"b\",\"type\":\"graph\"}}", &notes)).?;
    defer a.free(single);
    try std.testing.expectEqualStrings("{\"config\":{\"name\":\"b\",\"type\":\"graph\",\"description\":\"current Ω\"}}", single);
}

test "index annotation projection has an empty fast path never fabricates indexes and rejects duplicate metadata" {
    const a = std.testing.allocator;
    try std.testing.expect(try encode(a, "not parsed", &[_]Note{}) == null);
    const notes = [_]Note{.{ .name = "other", .description = "text" }};
    try std.testing.expect(try encode(a, "[]", &notes) == null);
    try std.testing.expect(try encode(a, "{\"config\":{\"name\":\"existing\"}}", &notes) == null);
    try std.testing.expectError(error.InvalidIndexAnnotationProjection, encode(a, "[]", &[_]Note{ notes[0], notes[0] }));
    try std.testing.expectError(error.InvalidIndexAnnotationProjection, encode(a, "null", &notes));
}

test "index annotation projection preserves exact numeric tokens throughout configuration and status" {
    const a = std.testing.allocator;
    const body = "{\"config\":{\"name\":\"a\",\"seed\":18446744073709551615,\"metadata\":{\"threshold\":1.0000000000000000000001,\"tiny\":1e-1000,\"zero\":-0}},\"status\":{\"bytes\":18446744073709551615}}";
    const notes = [_]Note{.{ .name = "a", .description = "exact" }};
    const output = (try encode(a, body, &notes)).?;
    defer a.free(output);
    try std.testing.expectEqualStrings("{\"config\":{\"name\":\"a\",\"seed\":18446744073709551615,\"metadata\":{\"threshold\":1.0000000000000000000001,\"tiny\":1e-1000,\"zero\":-0},\"description\":\"exact\"},\"status\":{\"bytes\":18446744073709551615}}", output);
}
