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
const types = @import("types.zig");
const HfTokenizer = @import("termite_hf_tokenizer").HfTokenizer;

const tokenizer_json = @import("termite_fixed_tokenizer_data").tokenizer_json;
const Allocator = std.mem.Allocator;

const PositionedSection = struct {
    text: []const u8,
    start: usize,
    tokens: usize = 0,
};

pub fn chunkText(alloc: Allocator, text: []const u8, cfg: types.FixedTextConfig) ![]types.Chunk {
    if (text.len == 0) return try alloc.alloc(types.Chunk, 0);

    const target_tokens = if (cfg.target_tokens > 0) cfg.target_tokens else 500;
    const overlap_tokens = cfg.overlap_tokens;
    const max_chunks = if (cfg.max_chunks > 0) cfg.max_chunks else 50;
    const separator = if (cfg.separator.len > 0) cfg.separator else "\n\n";
    if (overlap_tokens >= target_tokens) return error.InvalidChunkOverlap;

    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();

    const initial_sections = try splitSections(alloc, text, separator);
    defer alloc.free(initial_sections);
    const sections = try flattenOversizedSections(alloc, tokenizer, initial_sections, target_tokens);
    defer alloc.free(sections);

    var chunks = std.ArrayListUnmanaged(types.Chunk).empty;
    errdefer chunks.deinit(alloc);

    var current = std.ArrayListUnmanaged(PositionedSection).empty;
    defer current.deinit(alloc);
    var current_tokens: usize = 0;
    var previous_text: []const u8 = "";
    var previous_start: usize = 0;
    var chunk_id: u32 = 0;

    for (sections) |section| {
        const section_tokens = try countTokens(alloc, tokenizer, section.text);
        if (current_tokens > 0 and current_tokens + section_tokens > target_tokens) {
            try chunks.append(alloc, buildChunk(text, current.items, chunk_id));
            previous_start = current.items[0].start;
            chunk_id += 1;
            if (chunks.items.len >= max_chunks) return try chunks.toOwnedSlice(alloc);

            previous_text = chunks.items[chunks.items.len - 1].text.?;
            current.clearRetainingCapacity();
            current_tokens = 0;

            if (overlap_tokens > 0 and previous_text.len > 0) {
                const overlap_start = computeOverlapStart(alloc, tokenizer, previous_text, overlap_tokens);
                const overlap_text = previous_text[overlap_start..];
                if (overlap_text.len > 0) {
                    try current.append(alloc, .{
                        .text = overlap_text,
                        .start = previous_start + overlap_start,
                        .tokens = try countTokens(alloc, tokenizer, overlap_text),
                    });
                    current_tokens = current.items[0].tokens;
                }
            }
        }

        try current.append(alloc, .{
            .text = section.text,
            .start = section.start,
            .tokens = section_tokens,
        });
        current_tokens += section_tokens;
    }

    if (current.items.len > 0 and chunks.items.len < max_chunks) {
        try chunks.append(alloc, buildChunk(text, current.items, chunk_id));
    }

    return try chunks.toOwnedSlice(alloc);
}

fn splitSections(alloc: Allocator, text: []const u8, separator: []const u8) ![]PositionedSection {
    const primary = if (separator.len > 0) separator else "\n\n";
    var sections = try splitBySeparator(alloc, text, primary);
    if (sections.len <= 1 and !std.mem.eql(u8, primary, "\n")) {
        alloc.free(sections);
        sections = try splitBySeparator(alloc, text, "\n");
    }
    if (sections.len <= 1) {
        alloc.free(sections);
        sections = try splitBySeparator(alloc, text, ". ");
    }
    return sections;
}

fn splitBySeparator(alloc: Allocator, text: []const u8, separator: []const u8) ![]PositionedSection {
    if (separator.len == 0) {
        const single = try alloc.alloc(PositionedSection, 1);
        single[0] = .{ .text = text, .start = 0 };
        return single;
    }

    var sections = std.ArrayListUnmanaged(PositionedSection).empty;
    errdefer sections.deinit(alloc);

    var start: usize = 0;
    while (start <= text.len) {
        const next = std.mem.indexOfPos(u8, text, start, separator) orelse text.len;
        var end = next;
        if (std.mem.eql(u8, separator, ". ") and next < text.len) end += 1;
        try sections.append(alloc, .{
            .text = text[start..end],
            .start = start,
        });
        if (next == text.len) break;
        start = next + separator.len;
    }

    return try sections.toOwnedSlice(alloc);
}

fn flattenOversizedSections(
    alloc: Allocator,
    tokenizer: *HfTokenizer,
    sections: []PositionedSection,
    target_tokens: usize,
) ![]PositionedSection {
    var out = std.ArrayListUnmanaged(PositionedSection).empty;
    errdefer out.deinit(alloc);

    for (sections) |section| {
        const tokens = try countTokens(alloc, tokenizer, section.text);
        if (tokens <= target_tokens) {
            try out.append(alloc, .{ .text = section.text, .start = section.start, .tokens = tokens });
            continue;
        }

        const splitters = [_][]const u8{ "\n", ". ", " " };
        var split = false;
        for (splitters) |separator| {
            const finer = try splitBySeparator(alloc, section.text, separator);
            defer alloc.free(finer);
            if (finer.len <= 1) continue;
            for (finer) |*child| child.start += section.start;
            const nested = try flattenOversizedSections(alloc, tokenizer, finer, target_tokens);
            defer alloc.free(nested);
            try out.appendSlice(alloc, nested);
            split = true;
            break;
        }
        if (!split) {
            try appendTokenWindowChunks(alloc, tokenizer, section, target_tokens, &out);
        }
    }

    return try out.toOwnedSlice(alloc);
}

fn appendTokenWindowChunks(
    alloc: Allocator,
    tokenizer: *HfTokenizer,
    section: PositionedSection,
    target_tokens: usize,
    out: *std.ArrayListUnmanaged(PositionedSection),
) !void {
    var encoded_with_offsets = try tokenizer.encodeWithOffsets(alloc, section.text);
    if (encoded_with_offsets) |*encoded| {
        defer encoded.deinit(alloc);
        if (encoded.ids.items.len == 0) return;
        if (try appendTokenWindowChunksWithOffsets(alloc, section, encoded, target_tokens, out)) return;
        try appendTokenWindowChunksByDecodedSearch(alloc, tokenizer, section, encoded.ids.items, target_tokens, out);
        return;
    }

    const token_ids = try tokenizer.tokenizer().encode(alloc, section.text);
    defer alloc.free(token_ids);
    if (token_ids.len == 0) return;
    try appendTokenWindowChunksByDecodedSearch(alloc, tokenizer, section, token_ids, target_tokens, out);
}

fn appendTokenWindowChunksWithOffsets(
    alloc: Allocator,
    section: PositionedSection,
    encoded: anytype,
    target_tokens: usize,
    out: *std.ArrayListUnmanaged(PositionedSection),
) !bool {
    const token_len = encoded.ids.items.len;
    if (encoded.offsets.items.len != token_len) return false;
    const offsets = encoded.offsets.items;

    var start_token: usize = 0;
    while (start_token < token_len) {
        const token_count = @min(target_tokens, token_len - start_token);
        const start_rel: usize = offsets[start_token][0];
        const end_rel: usize = offsets[start_token + token_count - 1][1];
        if (start_rel > end_rel or end_rel > section.text.len) return false;
        if (start_rel == end_rel and end_rel < section.text.len) return false;
        start_token += token_count;
    }

    start_token = 0;
    while (start_token < token_len) {
        const token_count = @min(target_tokens, token_len - start_token);
        const start_rel: usize = offsets[start_token][0];
        const end_rel: usize = offsets[start_token + token_count - 1][1];
        try out.append(alloc, .{
            .text = section.text[start_rel..end_rel],
            .start = section.start + start_rel,
            .tokens = token_count,
        });
        start_token += token_count;
    }
    return true;
}

fn appendTokenWindowChunksByDecodedSearch(
    alloc: Allocator,
    tokenizer: *HfTokenizer,
    section: PositionedSection,
    token_ids: []const i32,
    target_tokens: usize,
    out: *std.ArrayListUnmanaged(PositionedSection),
) !void {
    var start_token: usize = 0;
    var search_start: usize = 0;
    while (start_token < token_ids.len) {
        const token_count = @min(target_tokens, token_ids.len - start_token);
        const piece_text = try tokenizer.tokenizer().decode(alloc, token_ids[start_token .. start_token + token_count]);
        defer alloc.free(piece_text);
        const remaining_tokens = token_ids.len - start_token;
        const rel, const end_rel = findDecodedWindow(section.text, search_start, piece_text) orelse blk: {
            const fallback_end = fallbackWindowEnd(section.text, search_start, token_count, remaining_tokens);
            break :blk .{ search_start, fallback_end };
        };
        const start = section.start + rel;
        try out.append(alloc, .{
            .text = section.text[rel..end_rel],
            .start = start,
            .tokens = token_count,
        });
        search_start = end_rel;
        start_token += token_count;
    }
}

fn findDecodedWindow(text: []const u8, search_start: usize, piece_text: []const u8) ?struct { usize, usize } {
    if (piece_text.len == 0 or search_start > text.len) return null;
    const rel = std.mem.indexOfPos(u8, text, search_start, piece_text) orelse return null;
    const end = rel + piece_text.len;
    if (end > text.len) return null;
    return .{ rel, end };
}

fn fallbackWindowEnd(text: []const u8, start: usize, token_count: usize, remaining_tokens: usize) usize {
    if (start >= text.len) return text.len;
    if (token_count >= remaining_tokens) return text.len;

    const remaining_bytes = text.len - start;
    const proportional = @max(@as(usize, 1), remaining_bytes * token_count / remaining_tokens);
    var end = previousUtf8Boundary(text, @min(text.len, start + proportional));
    if (end <= start) end = nextUtf8Boundary(text, @min(text.len, start + proportional + 1));
    if (end <= start) return text.len;
    return end;
}

fn previousUtf8Boundary(text: []const u8, index: usize) usize {
    var i = @min(index, text.len);
    while (i > 0 and i < text.len and (text[i] & 0xc0) == 0x80) : (i -= 1) {}
    return i;
}

fn nextUtf8Boundary(text: []const u8, index: usize) usize {
    var i = @min(index, text.len);
    while (i < text.len and (text[i] & 0xc0) == 0x80) : (i += 1) {}
    return i;
}

fn buildChunk(full_text: []const u8, sections: []const PositionedSection, chunk_id: u32) types.Chunk {
    const start = sections[0].start;
    const last = sections[sections.len - 1];
    const end = last.start + last.text.len;
    return types.Chunk.initText(chunk_id, full_text[start..end], start, end);
}

fn countTokens(alloc: Allocator, tokenizer: *HfTokenizer, text: []const u8) !usize {
    const ids = try tokenizer.tokenizer().encode(alloc, text);
    defer alloc.free(ids);
    return ids.len;
}

fn computeOverlapStart(alloc: Allocator, tokenizer: *HfTokenizer, text: []const u8, overlap_tokens: usize) usize {
    const ids = tokenizer.tokenizer().encode(alloc, text) catch return 0;
    defer alloc.free(ids);
    if (ids.len <= overlap_tokens) return 0;
    const overlap_text = tokenizer.tokenizer().decode(alloc, ids[ids.len - overlap_tokens ..]) catch return 0;
    defer alloc.free(overlap_text);
    return std.mem.lastIndexOf(u8, text, overlap_text) orelse 0;
}

test "fixed text chunker splits by token target" {
    const alloc = std.testing.allocator;
    const text =
        \\alpha beta gamma delta
        \\
        \\epsilon zeta eta theta
    ;
    const chunks = try chunkText(alloc, text, .{
        .target_tokens = 4,
        .overlap_tokens = 0,
        .separator = "\n\n",
    });
    defer alloc.free(chunks);

    try std.testing.expect(chunks.len >= 2);
    try std.testing.expectEqualStrings("text/plain", chunks[0].mime_type);
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].start_char);
}

test "fixed text chunker rejects invalid overlap" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidChunkOverlap, chunkText(alloc, "alpha beta", .{
        .target_tokens = 4,
        .overlap_tokens = 4,
    }));
}

test "decoded token window search starts from current cursor" {
    const found = findDecodedWindow("repeat repeat", 7, "repeat") orelse return error.ExpectedMatch;
    try std.testing.expectEqual(@as(usize, 7), found[0]);
    try std.testing.expectEqual(@as(usize, 13), found[1]);
}

test "decoded token window fallback clamps to source bounds" {
    try std.testing.expect(findDecodedWindow("abc", 0, "[UNK]") == null);
    const end = fallbackWindowEnd("abc", 1, 1, 2);
    try std.testing.expect(end <= 3);
    try std.testing.expect(end > 1);
}
