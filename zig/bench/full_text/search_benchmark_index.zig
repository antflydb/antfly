// Copyright 2026 Antfly, Inc.
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
const common = @import("search_benchmark_common.zig");

const batch_size = 5000;

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const args = try common.parseArgs(init.minimal.args);
    var db = try common.openDb(alloc, args.db_path);
    defer db.close();
    try common.ensureIndex(&db);

    var stdin_buf: [64 * 1024]u8 = undefined;
    var reader = std.Io.File.stdin().readerStreaming(init.io, &stdin_buf);
    var stderr_buf: [4096]u8 = undefined;
    var stderr = std.Io.File.stderr().writerStreaming(init.io, &stderr_buf);
    defer stderr.interface.flush() catch {};

    var batch_arena = std.heap.ArenaAllocator.init(alloc);
    defer batch_arena.deinit();
    var writes = std.ArrayListUnmanaged(common.types.BatchWrite).empty;
    defer writes.deinit(alloc);

    var indexed: usize = 0;
    var generated_id: usize = 0;
    while (try reader.interface.takeDelimiter('\n')) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0) continue;

        const batch_alloc = batch_arena.allocator();
        const doc = try normalizedDocument(batch_alloc, line, generated_id, args.max_text_bytes);
        generated_id += 1;
        try writes.append(alloc, .{
            .key = doc.key,
            .value = doc.value,
        });

        if (writes.items.len >= batch_size) {
            try flushBatch(&db, writes.items);
            indexed += writes.items.len;
            writes.clearRetainingCapacity();
            _ = batch_arena.reset(.retain_capacity);
            if (indexed % 100_000 == 0) {
                try stderr.interface.print("{d}\n", .{indexed});
                try stderr.interface.flush();
            }
        }
    }

    if (writes.items.len > 0) {
        try flushBatch(&db, writes.items);
        indexed += writes.items.len;
    }

    try db.runUntilIdle();
    try db.drainScheduledTextMerges();
    try stderr.interface.print("indexed {d} documents\n", .{indexed});
}

fn flushBatch(db: *common.antfly.db.DB, writes: []const common.types.BatchWrite) !void {
    try db.batch(.{
        .writes = writes,
        .sync_level = .full_text,
    });
}

const IndexedDocument = struct {
    key: []const u8,
    value: []const u8,
};

fn normalizedDocument(alloc: std.mem.Allocator, line: []const u8, fallback: usize, max_text_bytes: ?usize) !IndexedDocument {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{}) catch {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{fallback});
        return .{ .key = key, .value = try std.json.Stringify.valueAlloc(alloc, .{ .id = key, .text = line, .sort_field = @as(u64, 0) }, .{}) };
    };
    defer parsed.deinit();

    var key: []const u8 = "";
    var text: []const u8 = "";
    var sort_field: u64 = 0;

    if (parsed.value == .object) {
        if (parsed.value.object.get("id")) |id_value| {
            key = switch (id_value) {
                .string => |id| try alloc.dupe(u8, id),
                .integer => |id| try std.fmt.allocPrint(alloc, "{d}", .{id}),
                else => try std.fmt.allocPrint(alloc, "doc:{d}", .{fallback}),
            };
        }
        if (parsed.value.object.get("text")) |text_value| {
            if (text_value == .string) text = text_value.string;
        }
        if (parsed.value.object.get("sort_field")) |sort_value| {
            sort_field = switch (sort_value) {
                .integer => |v| if (v >= 0) @intCast(v) else 0,
                .float => |v| if (v >= 0) @intFromFloat(v) else 0,
                else => 0,
            };
        }
    }
    if (key.len == 0) key = try std.fmt.allocPrint(alloc, "doc:{d}", .{fallback});

    const indexed_text = if (max_text_bytes) |limit| truncateUtf8(text, limit) else text;
    const value = try std.json.Stringify.valueAlloc(alloc, .{
        .id = key,
        .text = indexed_text,
        .sort_field = sort_field,
    }, .{});
    return .{ .key = key, .value = value };
}

fn truncateUtf8(text: []const u8, max_bytes: usize) []const u8 {
    if (text.len <= max_bytes) return text;
    var end = max_bytes;
    while (end > 0 and (text[end] & 0b1100_0000) == 0b1000_0000) : (end -= 1) {}
    return text[0..end];
}
