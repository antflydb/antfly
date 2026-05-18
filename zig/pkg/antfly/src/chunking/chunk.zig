// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const Chunk = struct {
    chunk_id: u32,
    mime_type: []const u8 = "text/plain",
    owns_mime_type: bool = false,
    text: ?[]u8 = null,
    data: ?[]u8 = null,
    start_offset: ?u32 = null,
    end_offset: ?u32 = null,
    start_time_ms: ?f32 = null,
    end_time_ms: ?f32 = null,
    frame_index: ?u32 = null,
    frame_delay_ms: ?u32 = null,

    pub fn deinit(self: *Chunk, alloc: Allocator) void {
        if (self.text) |text| alloc.free(text);
        if (self.data) |data| alloc.free(data);
        if (self.owns_mime_type) alloc.free(@constCast(self.mime_type));
        self.* = undefined;
    }

    pub fn isText(self: Chunk) bool {
        return self.text != null and std.mem.eql(u8, self.mime_type, "text/plain");
    }
};

pub fn appendArtifactFields(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    source_field: []const u8,
    chunk: Chunk,
    include_payload: bool,
) !void {
    try obj.put(alloc, try alloc.dupe(u8, "_chunk_id"), .{ .integer = chunk.chunk_id });
    try obj.put(alloc, try alloc.dupe(u8, "_mime_type"), .{ .string = try alloc.dupe(u8, chunk.mime_type) });
    if (chunk.start_offset) |value| try obj.put(alloc, try alloc.dupe(u8, "_start_offset"), .{ .integer = value });
    if (chunk.end_offset) |value| try obj.put(alloc, try alloc.dupe(u8, "_end_offset"), .{ .integer = value });
    if (chunk.start_time_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_start_time_ms"), .{ .float = value });
    if (chunk.end_time_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_end_time_ms"), .{ .float = value });
    if (chunk.frame_index) |value| try obj.put(alloc, try alloc.dupe(u8, "_frame_index"), .{ .integer = value });
    if (chunk.frame_delay_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_frame_delay_ms"), .{ .integer = value });

    if (!include_payload) return;

    if (chunk.text) |text| {
        try obj.put(alloc, try alloc.dupe(u8, source_field), .{ .string = try alloc.dupe(u8, text) });
    } else if (chunk.data) |data| {
        const encoded = try base64EncodeAlloc(alloc, data);
        errdefer alloc.free(encoded);
        try obj.put(alloc, try alloc.dupe(u8, "_data"), .{ .string = encoded });
    }
}

fn base64EncodeAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

test "append artifact fields stores text offsets and payload" {
    const alloc = std.testing.allocator;
    var obj = std.json.ObjectMap.empty;
    defer {
        var it = obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        obj.deinit(alloc);
    }

    const chunk = Chunk{
        .chunk_id = 3,
        .text = try alloc.dupe(u8, "hello"),
        .start_offset = 7,
        .end_offset = 12,
    };
    defer {
        var mutable = chunk;
        mutable.deinit(alloc);
    }

    try appendArtifactFields(alloc, &obj, "body", chunk, true);
    try std.testing.expectEqual(@as(i64, 3), obj.get("_chunk_id").?.integer);
    try std.testing.expectEqualStrings("text/plain", obj.get("_mime_type").?.string);
    try std.testing.expectEqual(@as(i64, 7), obj.get("_start_offset").?.integer);
    try std.testing.expectEqualStrings("hello", obj.get("body").?.string);
}

test "append artifact fields stores binary metadata and base64 payload" {
    const alloc = std.testing.allocator;
    var obj = std.json.ObjectMap.empty;
    defer {
        var it = obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        obj.deinit(alloc);
    }

    const chunk = Chunk{
        .chunk_id = 1,
        .mime_type = "image/png",
        .data = try alloc.dupe(u8, &.{ 1, 2, 3 }),
        .frame_index = 0,
        .frame_delay_ms = 50,
    };
    defer {
        var mutable = chunk;
        mutable.deinit(alloc);
    }

    try appendArtifactFields(alloc, &obj, "body", chunk, true);
    try std.testing.expectEqualStrings("image/png", obj.get("_mime_type").?.string);
    try std.testing.expectEqual(@as(i64, 0), obj.get("_frame_index").?.integer);
    try std.testing.expectEqualStrings("AQID", obj.get("_data").?.string);
}

fn freeJsonValue(alloc: Allocator, value: std.json.Value) void {
    switch (value) {
        .string => |s| alloc.free(s),
        .array => |arr| {
            for (arr.items) |item| freeJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |obj| {
            var mutable = obj;
            var it = mutable.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                freeJsonValue(alloc, entry.value_ptr.*);
            }
            mutable.deinit(alloc);
        },
        else => {},
    }
}
