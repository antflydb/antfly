// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const reader = @import("reader.zig");

/// A reading-order block of compatible text on one geometric line. Spans are
/// page-local UTF-8 byte offsets into the emitted text, not glyph indices.
/// Font evidence is retained without guessing heading, table or chart semantics.
pub const Block = struct {
    span: [2]u32,
    bbox: [4]f64,
    reading_order: u32,
    font_size: ?f64 = null,
    bold: ?bool = null,
    italic: ?bool = null,
};

const Entry = struct {
    block: Block,
    source_index: usize,
    upright: bool,
};

fn entryLess(_: void, left: Entry, right: Entry) bool {
    if (left.block.span[0] != right.block.span[0]) return left.block.span[0] < right.block.span[0];
    return left.source_index < right.source_index;
}

fn sameSize(left: ?f64, right: ?f64) bool {
    if (left == null or right == null) return left == null and right == null;
    return @abs(left.? - right.?) <= @max(left.?, right.?) * 0.1;
}

fn canJoin(text: []const u8, left: Block, right: Block, upright: bool) bool {
    if (!upright or left.bold != right.bold or left.italic != right.italic or !sameSize(left.font_size, right.font_size)) return false;
    if (right.span[0] < left.span[1]) return false;
    if (std.mem.indexOfScalar(u8, text[left.span[1]..right.span[0]], '\n') != null) return false;
    const left_height = left.bbox[3] - left.bbox[1];
    const right_height = right.bbox[3] - right.bbox[1];
    const height = @max(left_height, right_height);
    const center_distance = @abs((left.bbox[1] + left.bbox[3] - right.bbox[1] - right.bbox[3]) * 0.5);
    if (center_distance > height * 0.45) return false;
    // Keep distant cells/columns and backward jumps separate, even when the
    // source transcript has no line break between them.
    const gap = right.bbox[0] - left.bbox[2];
    const em = left.font_size orelse height;
    return gap >= -em * 0.25 and gap <= em * 2;
}

/// Output order follows emitted spans rather than PDF paint order. Filtered or
/// unaligned runs have no span and cannot leak back into the structural output.
pub fn blocksFromRunsAlloc(alloc: std.mem.Allocator, text: []const u8, runs: []const reader.TextRun) ![]Block {
    var entries = std.ArrayList(Entry).empty;
    defer entries.deinit(alloc);
    for (runs, 0..) |run, index| {
        const span = run.output_span orelse continue;
        if (span.start >= span.end or span.end > text.len) continue;
        if (std.mem.trim(u8, text[span.start..span.end], &std.ascii.whitespace).len == 0) continue;
        const start = std.math.cast(u32, span.start) orelse return error.Overflow;
        const end = std.math.cast(u32, span.end) orelse return error.Overflow;
        const box = run.bounds() orelse continue;
        const size = @abs(run.font_size) * @sqrt(run.c * run.c + run.d * run.d);
        const axis = @sqrt(run.a * run.a + run.b * run.b);
        try entries.append(alloc, .{
            .block = .{
                .span = .{ start, end },
                .bbox = .{ box.min_x, box.min_y, box.max_x, box.max_y },
                .reading_order = 0,
                .font_size = if (std.math.isFinite(size) and size > 0) size else null,
                .bold = run.bold,
                .italic = run.italic,
            },
            .source_index = index,
            .upright = axis > 0 and run.a / axis >= 0.984807753 and @abs(run.b) / axis <= 0.173648178,
        });
    }
    std.mem.sort(Entry, entries.items, {}, entryLess);
    var blocks = std.ArrayList(Block).empty;
    errdefer blocks.deinit(alloc);
    var previous_upright = false;
    var previous_span: [2]u32 = undefined;
    for (entries.items) |entry| {
        if (blocks.items.len > 0) {
            const previous = &blocks.items[blocks.items.len - 1];
            const same_span = previous_span[0] == entry.block.span[0] and previous_span[1] == entry.block.span[1];
            if (same_span or canJoin(text, previous.*, entry.block, previous_upright and entry.upright)) {
                // Several glyph runs can belong to one ActualText replacement.
                // Preserve the union of their geometry, not repeated text blocks.
                if (same_span) {
                    if (previous.bold != entry.block.bold) previous.bold = null;
                    if (previous.italic != entry.block.italic) previous.italic = null;
                    if (!sameSize(previous.font_size, entry.block.font_size)) previous.font_size = null;
                    previous_upright = previous_upright and entry.upright;
                }
                previous.span[1] = entry.block.span[1];
                previous.bbox[0] = @min(previous.bbox[0], entry.block.bbox[0]);
                previous.bbox[1] = @min(previous.bbox[1], entry.block.bbox[1]);
                previous.bbox[2] = @max(previous.bbox[2], entry.block.bbox[2]);
                previous.bbox[3] = @max(previous.bbox[3], entry.block.bbox[3]);
                previous_span = entry.block.span;
                continue;
            }
        }
        var block = entry.block;
        block.reading_order = std.math.cast(u32, blocks.items.len) orelse return error.Overflow;
        try blocks.append(alloc, block);
        previous_upright = entry.upright;
        previous_span = entry.block.span;
    }
    return blocks.toOwnedSlice(alloc);
}

fn fixtureRun(text: []const u8, start: usize, end: usize, x: f64, y: f64) reader.TextRun {
    return .{ .text = text, .x = x, .y = y, .font_size = 10, .advance_width = @as(f64, @floatFromInt(end - start)) * 5, .ascent = 8, .descent = 2, .bold = false, .italic = false, .output_span = .{ .start = start, .end = end } };
}

test "native blocks group emitted text order and preserve line boundaries" {
    const alloc = std.testing.allocator;
    const text = "Hello world\nNext";
    const runs = [_]reader.TextRun{
        fixtureRun("world", 6, 11, 30, 100),
        fixtureRun("Next", 12, 16, 0, 85),
        fixtureRun("Hello", 0, 5, 0, 100),
    };
    const blocks = try blocksFromRunsAlloc(alloc, text, &runs);
    defer alloc.free(blocks);
    try std.testing.expectEqual(@as(usize, 2), blocks.len);
    try std.testing.expectEqualStrings("Hello world", text[blocks[0].span[0]..blocks[0].span[1]]);
    try std.testing.expectEqualStrings("Next", text[blocks[1].span[0]..blocks[1].span[1]]);
    try std.testing.expectEqual([4]f64{ 0, 98, 55, 108 }, blocks[0].bbox);
    try std.testing.expectEqual(@as(u32, 1), blocks[1].reading_order);
}

test "native blocks retain style changes and distant cells" {
    const alloc = std.testing.allocator;
    const text = "Title body cell";
    var runs = [_]reader.TextRun{
        fixtureRun("Title", 0, 5, 0, 100),
        fixtureRun("body", 6, 10, 30, 100),
        fixtureRun("cell", 11, 15, 200, 100),
    };
    runs[0].bold = true;
    runs[0].italic = true;
    const blocks = try blocksFromRunsAlloc(alloc, text, &runs);
    defer alloc.free(blocks);
    try std.testing.expectEqual(@as(usize, 3), blocks.len);
    try std.testing.expectEqual(@as(?bool, true), blocks[0].bold);
    try std.testing.expectEqual(@as(?bool, true), blocks[0].italic);
    try std.testing.expectEqual(@as(?bool, false), blocks[1].bold);
    try std.testing.expectEqualStrings("cell", text[blocks[2].span[0]..blocks[2].span[1]]);
}

test "native blocks omit filtered runs and use transformed font size" {
    const alloc = std.testing.allocator;
    const text = "\xce\xb1\xce\xb2";
    var runs = [_]reader.TextRun{ fixtureRun(text, 0, 4, 0, 100), fixtureRun("hidden", 0, 4, 0, 200) };
    runs[0].font_size = 5;
    runs[0].c = 0;
    runs[0].d = 2;
    runs[1].output_span = null;
    const blocks = try blocksFromRunsAlloc(alloc, text, &runs);
    defer alloc.free(blocks);
    try std.testing.expectEqual(@as(usize, 1), blocks.len);
    try std.testing.expectEqualStrings(text, text[blocks[0].span[0]..blocks[0].span[1]]);
    try std.testing.expectEqual(@as(?f64, 10), blocks[0].font_size);
}

test "ActualText fragments share one semantic block and retain their geometry" {
    const alloc = std.testing.allocator;
    const text = "prefix accessible";
    var runs = [_]reader.TextRun{
        fixtureRun("prefix", 0, 6, 0, 100),
        fixtureRun("accessible", 7, 17, 30, 100),
        fixtureRun("", 7, 17, 55, 100),
    };
    runs[0].advance_width = 25;
    runs[1].advance_width = 20;
    runs[2].advance_width = 15;
    runs[2].bold = true;
    const blocks = try blocksFromRunsAlloc(alloc, text, &runs);
    defer alloc.free(blocks);
    try std.testing.expectEqual(@as(usize, 1), blocks.len);
    try std.testing.expectEqualStrings(text, text[blocks[0].span[0]..blocks[0].span[1]]);
    try std.testing.expectEqual([4]f64{ 0, 98, 70, 108 }, blocks[0].bbox);
    try std.testing.expectEqual(@as(?bool, null), blocks[0].bold);
}

test "native block grouping releases partial allocations" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const runs = [_]reader.TextRun{ fixtureRun("left", 0, 4, 0, 100), fixtureRun("right", 5, 10, 100, 100) };
            const blocks = try blocksFromRunsAlloc(alloc, "left right", &runs);
            defer alloc.free(blocks);
            try std.testing.expectEqualStrings("right", "left right"[blocks[1].span[0]..blocks[1].span[1]]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
