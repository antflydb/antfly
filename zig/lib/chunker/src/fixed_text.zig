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
const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;

const tokenizer_json = @import("inference_fixed_tokenizer_data").tokenizer_json;
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
    var previous_text: []const u8 = "";
    var previous_start: usize = 0;
    var chunk_id: u32 = 0;

    for (sections) |section| {
        // Empty and tokenizer-empty sections (such as whitespace) are only
        // boundaries. They must not anchor a span or emit a chunk artifact.
        if (section.tokens == 0) continue;
        const section_tokens = section.tokens;
        // Count the actual source span, including separators between sections.
        const candidate_tokens = if (current.items.len > 0)
            try countTokens(alloc, tokenizer, text[current.items[0].start .. section.start + section.text.len])
        else
            section_tokens;
        if (current.items.len > 0 and candidate_tokens > target_tokens) {
            try chunks.append(alloc, buildChunk(text, current.items, chunk_id));
            previous_start = current.items[0].start;
            chunk_id += 1;
            if (chunks.items.len >= max_chunks) return try chunks.toOwnedSlice(alloc);

            previous_text = chunks.items[chunks.items.len - 1].text.?;
            current.clearRetainingCapacity();

            if (overlap_tokens > 0 and previous_text.len > 0) {
                // A full-size next section leaves no room for overlap.
                const overlap_budget = @min(overlap_tokens, target_tokens -| section_tokens);
                const overlap_start = try computeOverlapStart(alloc, tokenizer, previous_text, overlap_budget);
                const overlap_text = previous_text[overlap_start..];
                if (overlap_text.len > 0) {
                    const overlap_candidate_tokens = try countTokens(alloc, tokenizer, text[previous_start + overlap_start .. section.start + section.text.len]);
                    if (overlap_candidate_tokens <= target_tokens) {
                        try current.append(alloc, .{
                            .text = overlap_text,
                            .start = previous_start + overlap_start,
                        });
                    }
                }
            }
        }

        try current.append(alloc, .{
            .text = section.text,
            .start = section.start,
            .tokens = section_tokens,
        });
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
) anyerror!void {
    var encoded_with_offsets = try tokenizer.encodeWithOffsets(alloc, section.text);
    if (encoded_with_offsets) |*encoded| {
        defer encoded.deinit(alloc);
        if (encoded.ids.items.len == 0) return;
        if (try appendTokenWindowChunksWithOffsets(alloc, tokenizer, section, encoded, target_tokens, out)) return;
        try appendTokenWindowChunksBySource(alloc, tokenizer, section, encoded.ids.items.len, target_tokens, out);
        return;
    }

    const token_ids = try tokenizer.tokenizer().encode(alloc, section.text);
    defer alloc.free(token_ids);
    if (token_ids.len == 0) return;
    try appendTokenWindowChunksBySource(alloc, tokenizer, section, token_ids.len, target_tokens, out);
}

fn appendTokenWindowChunksWithOffsets(
    alloc: Allocator,
    tokenizer: *HfTokenizer,
    section: PositionedSection,
    encoded: anytype,
    target_tokens: usize,
    out: *std.ArrayListUnmanaged(PositionedSection),
) anyerror!bool {
    const token_len = encoded.ids.items.len;
    if (encoded.offsets.items.len != token_len) return false;
    const offsets = encoded.offsets.items;

    var start_token: usize = 0;
    var previous_end: usize = 0;
    while (start_token < token_len) {
        const token_count = @min(target_tokens, token_len - start_token);
        const start_rel: usize = offsets[start_token][0];
        const end_rel: usize = offsets[start_token + token_count - 1][1];
        if (start_rel > end_rel or end_rel > section.text.len) return false;
        if (start_rel >= end_rel or start_rel < previous_end) return false;
        if (previousUtf8Boundary(section.text, start_rel) != start_rel or
            previousUtf8Boundary(section.text, end_rel) != end_rel) return false;
        previous_end = end_rel;
        start_token += token_count;
    }

    start_token = 0;
    while (start_token < token_len) {
        const token_count = @min(target_tokens, token_len - start_token);
        const start_rel: usize = offsets[start_token][0];
        const end_rel: usize = offsets[start_token + token_count - 1][1];
        try appendValidatedTokenWindow(alloc, tokenizer, .{
            .text = section.text[start_rel..end_rel],
            .start = section.start + start_rel,
        }, section.text.len, target_tokens, out);
        start_token += token_count;
    }
    return true;
}

fn appendTokenWindowChunksBySource(
    alloc: Allocator,
    tokenizer: *HfTokenizer,
    section: PositionedSection,
    total_tokens: usize,
    target_tokens: usize,
    out: *std.ArrayListUnmanaged(PositionedSection),
) anyerror!void {
    // Decoded tokens may be normalized or contain [UNK], so substring search
    // cannot establish source offsets. Partition the original bytes instead;
    // proportional boundaries are only hints and every span is revalidated.
    var remaining_tokens = total_tokens;
    var start_rel: usize = 0;
    while (remaining_tokens > 0 and start_rel < section.text.len) {
        const token_count = @min(target_tokens, remaining_tokens);
        const end_rel = fallbackWindowEnd(section.text, start_rel, token_count, remaining_tokens);
        try appendValidatedTokenWindow(alloc, tokenizer, .{
            .text = section.text[start_rel..end_rel],
            .start = section.start + start_rel,
        }, section.text.len, target_tokens, out);
        start_rel = end_rel;
        remaining_tokens -= token_count;
    }
}

fn appendValidatedTokenWindow(
    alloc: Allocator,
    tokenizer: *HfTokenizer,
    window: PositionedSection,
    parent_bytes: usize,
    target_tokens: usize,
    out: *std.ArrayListUnmanaged(PositionedSection),
) anyerror!void {
    if (window.text.len == 0) return;
    // A continuation token can become several tokens when its source substring
    // is encoded alone. Store the independent count, not the original ID count.
    const tokens = try countTokens(alloc, tokenizer, window.text);
    if (tokens <= target_tokens) {
        try out.append(alloc, .{ .text = window.text, .start = window.start, .tokens = tokens });
    } else if (window.text.len < parent_bytes) {
        try appendTokenWindowChunks(alloc, tokenizer, window, target_tokens, out);
    } else {
        // Offset normalization or source-boundary fallback can retain the whole
        // parent span. Require byte progress before trying another token split.
        var split = previousUtf8Boundary(window.text, window.text.len / 2);
        if (split == 0) split = nextUtf8Boundary(window.text, 1);
        if (split >= window.text.len) return error.ChunkTokenBudgetTooSmall;
        try appendTokenWindowChunks(alloc, tokenizer, .{ .text = window.text[0..split], .start = window.start }, target_tokens, out);
        try appendTokenWindowChunks(alloc, tokenizer, .{ .text = window.text[split..], .start = window.start + split }, target_tokens, out);
    }
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

fn computeOverlapStart(alloc: Allocator, tokenizer: *HfTokenizer, text: []const u8, overlap_tokens: usize) !usize {
    if (overlap_tokens == 0) return text.len;
    // Decoded tokens are normalized text, not a searchable source substring.
    // If offsets are unavailable, omit overlap rather than retaining a prefix.
    var encoded = (try tokenizer.encodeWithOffsets(alloc, text)) orelse return text.len;
    defer encoded.deinit(alloc);
    if (encoded.ids.items.len != encoded.offsets.items.len) return text.len;
    const first = encoded.ids.items.len -| overlap_tokens;
    for (encoded.offsets.items[first..]) |offset| {
        const start: usize = offset[0];
        if (start >= text.len or start > offset[1] or offset[1] > text.len) return text.len;
        if (previousUtf8Boundary(text, start) != start) continue;
        // Starting inside a word can change its tokenization. Only keep a
        // suffix that fits the overlap budget when encoded independently.
        if (try countTokens(alloc, tokenizer, text[start..]) <= overlap_tokens) return start;
    }
    return text.len;
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

test "token window fallback clamps to source bounds" {
    const end = fallbackWindowEnd("abc", 1, 1, 2);
    try std.testing.expect(end <= 3);
    try std.testing.expect(end > 1);
}

test "fixed text overlap preserves source offsets despite normalization" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    const text = "alpha beta gamma HELLO, WORLD!";
    const start = try computeOverlapStart(alloc, tokenizer, text, 4);
    try std.testing.expectEqualStrings("HELLO, WORLD!", text[start..]);
    try std.testing.expectEqual(text.len, try computeOverlapStart(alloc, tokenizer, text, 0));
    try std.testing.expectEqual(@as(usize, 0), try computeOverlapStart(alloc, tokenizer, text, 100));
}

test "fixed text overlap advances bounded chunks through mixed source text" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    const paragraph = "Korean HISTORY: Major Events (1950–1953), Seoul! Café, 日本語. Repeated WORDS; punctuation changes.\n\n";
    const text = paragraph ** 80;
    const chunks = try chunkText(alloc, text, .{ .target_tokens = 200, .overlap_tokens = 25, .max_chunks = 200 });
    defer alloc.free(chunks);
    try std.testing.expect(chunks.len > 1 and chunks.len < 200);
    for (chunks, 0..) |chunk, i| {
        const start = chunk.start_char.?;
        const end = chunk.end_char.?;
        try std.testing.expectEqualStrings(text[start..end], chunk.text.?);
        try std.testing.expect(std.unicode.utf8ValidateSlice(chunk.text.?));
        try std.testing.expect(try countTokens(alloc, tokenizer, chunk.text.?) <= 200);
        if (i > 0) {
            try std.testing.expect(start > chunks[i - 1].start_char.?);
            try std.testing.expect(end > chunks[i - 1].end_char.?);
        }
    }
    try std.testing.expect(chunks[chunks.len - 1].end_char.? >= std.mem.trimEnd(u8, text, "\n").len);
}

test "fixed text overlap leaves room for full sections and counts separators" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    for ([_][]const u8{ "alpha beta gamma delta\n\nepsilon zeta eta theta", "alpha,beta,gamma,delta,epsilon,zeta,eta,theta" }) |text| {
        const chunks = try chunkText(alloc, text, .{ .target_tokens = 4, .overlap_tokens = 2, .separator = "," });
        defer alloc.free(chunks);
        for (chunks) |chunk| try std.testing.expect(try countTokens(alloc, tokenizer, chunk.text.?) <= 4);
        try std.testing.expectEqual(text.len, chunks[chunks.len - 1].end_char.?);
    }
}

test "fixed text chunker bounds custom separator edge sections" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    for ([_][]const u8{ ",alpha", "alpha,beta,", ",,alpha,,,beta,,", " ,alpha", ",,," }) |text| {
        const chunks = try chunkText(alloc, text, .{ .target_tokens = 1, .overlap_tokens = 0, .separator = "," });
        defer alloc.free(chunks);
        for (chunks, 0..) |chunk, i| {
            try std.testing.expect(chunk.text.?.len > 0);
            try std.testing.expect(chunk.start_char.? < chunk.end_char.?);
            try std.testing.expectEqualStrings(text[chunk.start_char.?..chunk.end_char.?], chunk.text.?);
            try std.testing.expect(try countTokens(alloc, tokenizer, chunk.text.?) <= 1);
            if (i > 0) try std.testing.expect(chunk.start_char.? >= chunks[i - 1].end_char.?);
        }
        if (std.mem.indexOf(u8, text, "alpha") != null) {
            var found = false;
            for (chunks) |chunk| if (std.mem.eql(u8, chunk.text.?, "alpha")) {
                found = true;
            };
            try std.testing.expect(found);
        } else try std.testing.expectEqual(@as(usize, 0), chunks.len);
    }
}

test "fixed text token windows are independently bounded after continuation splits" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    for ([_][]const u8{ "unaffordable", "UNAFFORDABLE", "antidisestablishmentarianism", "unbelievably", "encyclopaedia", "normalization" }) |text| {
        for ([_]usize{ 1, 2, 3 }) |target| {
            const chunks = try chunkText(alloc, text, .{ .target_tokens = target, .overlap_tokens = 0 });
            defer alloc.free(chunks);
            var end: usize = 0;
            for (chunks) |chunk| {
                try std.testing.expectEqual(end, chunk.start_char.?);
                try std.testing.expect(chunk.text.?.len > 0);
                try std.testing.expect(try countTokens(alloc, tokenizer, chunk.text.?) <= target);
                end = chunk.end_char.?;
            }
            try std.testing.expectEqual(text.len, end);
        }
    }
    var windows = std.ArrayListUnmanaged(PositionedSection).empty;
    defer windows.deinit(alloc);
    const text = "unaffordable";
    try appendValidatedTokenWindow(alloc, tokenizer, .{ .text = text, .start = 0 }, text.len, 1, &windows);
    var end: usize = 0;
    for (windows.items) |window| {
        try std.testing.expectEqual(end, window.start);
        try std.testing.expect(try countTokens(alloc, tokenizer, window.text) <= 1);
        end = window.start + window.text.len;
    }
    try std.testing.expectEqual(text.len, end);
}

test "fixed text fallback preserves normalized and unknown source spans" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    for ([_][]const u8{ "AbcabcЖ", "HELLOhelloЖ", "NORMALnormalЖ", "AabcabcЖ", "caféCAFÉ", "İstanbulistanbul", "aéæa", "unaffordableЖ", "😀abc", "abc😀def" }) |text| {
        for ([_]usize{ 1, 2, 3 }) |target| {
            const chunks = try chunkText(alloc, text, .{ .target_tokens = target, .overlap_tokens = 0, .max_chunks = 1000 });
            defer alloc.free(chunks);
            var end: usize = 0;
            for (chunks) |chunk| {
                try std.testing.expectEqual(end, chunk.start_char.?);
                try std.testing.expect(chunk.text.?.len > 0);
                try std.testing.expect(std.unicode.utf8ValidateSlice(chunk.text.?));
                try std.testing.expectEqualStrings(text[chunk.start_char.?..chunk.end_char.?], chunk.text.?);
                try std.testing.expect(try countTokens(alloc, tokenizer, chunk.text.?) <= target);
                end = chunk.end_char.?;
            }
            try std.testing.expectEqual(text.len, end);
        }
    }
}

test "fixed text chunker omits tokenizer empty sections" {
    const alloc = std.testing.allocator;
    var tokenizer = try HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer tokenizer.deinitSelf();
    for ([_][]const u8{ " ,alpha", "alpha, \t", " ,alpha, \t", " \t, \r" }) |text| {
        const chunks = try chunkText(alloc, text, .{ .target_tokens = 1, .overlap_tokens = 0, .separator = "," });
        defer alloc.free(chunks);
        try std.testing.expectEqual(@as(usize, if (std.mem.indexOf(u8, text, "alpha") != null) 1 else 0), chunks.len);
        for (chunks) |chunk| {
            try std.testing.expect(std.mem.trim(u8, chunk.text.?, " \t\r\n").len > 0);
            try std.testing.expectEqual(@as(usize, 1), try countTokens(alloc, tokenizer, chunk.text.?));
            try std.testing.expectEqualStrings(text[chunk.start_char.?..chunk.end_char.?], chunk.text.?);
        }
    }
}
