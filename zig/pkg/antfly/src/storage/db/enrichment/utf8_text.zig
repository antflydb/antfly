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
const prometheus = @import("../../../common/prometheus.zig");
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);

const invalid_utf8_warning_limit = 8;
var invalid_utf8_repairs: AtomicU64 = .init(0);

pub const BoundaryDirection = enum { backward, forward };

pub const SourceBoundary = enum { start, end };

pub const SourceSpan = struct {
    repaired_start: u32,
    repaired_end: u32,
    source_start: u32,
    source_end: u32,
};

pub const SourceOffsetMap = struct {
    spans: []SourceSpan,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.spans);
        self.* = undefined;
    }

    pub fn mapBoundary(self: SourceOffsetMap, repaired_offset: u32, boundary: SourceBoundary) u32 {
        if (self.spans.len == 0) return repaired_offset;

        var low: usize = 0;
        var high: usize = self.spans.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const span = self.spans[mid];
            if (repaired_offset < span.repaired_start) {
                high = mid;
            } else if (repaired_offset > span.repaired_end) {
                low = mid + 1;
            } else {
                return mapWithinSpan(span, repaired_offset, boundary);
            }
        }

        if (low == 0) return self.spans[0].source_start;
        return self.spans[low - 1].source_end;
    }
};

pub const SanitizedText = struct {
    text: []const u8,
    owned: ?[]u8 = null,
    source_map: ?SourceOffsetMap = null,
    repaired: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.source_map) |*source_map| source_map.deinit(alloc);
        if (self.owned) |owned| alloc.free(owned);
        self.* = undefined;
    }
};

pub fn sanitizeAlloc(alloc: Allocator, text: []const u8, context: []const u8) !SanitizedText {
    if (std.unicode.utf8ValidateSlice(text)) return .{ .text = text };
    const repaired = try replacementWithMapAlloc(alloc, text, context);
    return .{
        .text = repaired.text,
        .owned = repaired.text,
        .source_map = repaired.source_map,
        .repaired = true,
    };
}

pub fn sanitizeWithoutSourceMapAlloc(alloc: Allocator, text: []const u8, context: []const u8) !SanitizedText {
    if (std.unicode.utf8ValidateSlice(text)) return .{ .text = text };
    const repaired = try replacementAlloc(alloc, text, context);
    return .{
        .text = repaired,
        .owned = repaired,
        .repaired = true,
    };
}

pub fn replacementAlloc(alloc: Allocator, text: []const u8, context: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var i: usize = 0;
    while (i < text.len) {
        const seq_len = std.unicode.utf8ByteSequenceLength(text[i]) catch 0;
        if (seq_len > 0 and i + seq_len <= text.len and std.unicode.utf8ValidateSlice(text[i .. i + seq_len])) {
            try out.appendSlice(alloc, text[i .. i + seq_len]);
            i += seq_len;
        } else {
            try out.appendSlice(alloc, &std.unicode.replacement_character_utf8);
            i += 1;
        }
    }

    const repaired = try out.toOwnedSlice(alloc);
    noteInvalidUtf8Repair(context, text.len, repaired.len);
    return repaired;
}

pub fn invalidUtf8RepairCount() u64 {
    return invalid_utf8_repairs.load(.monotonic);
}

pub fn writePrometheus(writer: *std.Io.Writer) !void {
    try prometheus.appendPromMetric(
        writer,
        "antfly_enrichment_invalid_utf8_repairs_total",
        "counter",
        "Invalid UTF-8 inputs repaired before enrichment chunking or embedding",
        invalidUtf8RepairCount(),
    );
}

pub fn snapToBoundary(text: []const u8, index: usize, direction: BoundaryDirection) usize {
    var i = @min(index, text.len);
    switch (direction) {
        .backward => {
            while (i > 0 and i < text.len and isContinuation(text[i])) : (i -= 1) {}
        },
        .forward => {
            while (i < text.len and isContinuation(text[i])) : (i += 1) {}
        },
    }
    return i;
}

fn isContinuation(byte: u8) bool {
    return (byte & 0xc0) == 0x80;
}

const ReplacementWithMap = struct {
    text: []u8,
    source_map: SourceOffsetMap,
};

fn replacementWithMapAlloc(alloc: Allocator, text: []const u8, context: []const u8) !ReplacementWithMap {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var spans = std.ArrayListUnmanaged(SourceSpan).empty;
    errdefer spans.deinit(alloc);

    var i: usize = 0;
    while (i < text.len) {
        const seq_len = std.unicode.utf8ByteSequenceLength(text[i]) catch 0;
        if (seq_len > 0 and i + seq_len <= text.len and std.unicode.utf8ValidateSlice(text[i .. i + seq_len])) {
            const repaired_start = out.items.len;
            try out.appendSlice(alloc, text[i .. i + seq_len]);
            try appendSourceSpan(alloc, &spans, repaired_start, out.items.len, i, i + seq_len);
            i += seq_len;
        } else {
            const repaired_start = out.items.len;
            try out.appendSlice(alloc, &std.unicode.replacement_character_utf8);
            try appendSourceSpan(alloc, &spans, repaired_start, out.items.len, i, i + 1);
            i += 1;
        }
    }

    const repaired = try out.toOwnedSlice(alloc);
    errdefer alloc.free(repaired);
    const owned_spans = try spans.toOwnedSlice(alloc);
    noteInvalidUtf8Repair(context, text.len, repaired.len);
    return .{
        .text = repaired,
        .source_map = .{ .spans = owned_spans },
    };
}

fn appendSourceSpan(
    alloc: Allocator,
    spans: *std.ArrayListUnmanaged(SourceSpan),
    repaired_start: usize,
    repaired_end: usize,
    source_start: usize,
    source_end: usize,
) !void {
    const next: SourceSpan = .{
        .repaired_start = try checkedU32(repaired_start),
        .repaired_end = try checkedU32(repaired_end),
        .source_start = try checkedU32(source_start),
        .source_end = try checkedU32(source_end),
    };
    if (spans.items.len > 0) {
        const last = &spans.items[spans.items.len - 1];
        const last_is_identity = last.repaired_end - last.repaired_start == last.source_end - last.source_start;
        const next_is_identity = next.repaired_end - next.repaired_start == next.source_end - next.source_start;
        const repaired_delta = next.repaired_start - last.repaired_end;
        const source_delta = next.source_start - last.source_end;
        if (last_is_identity and next_is_identity and repaired_delta == source_delta) {
            last.repaired_end = next.repaired_end;
            last.source_end = next.source_end;
            return;
        }
    }
    try spans.append(alloc, next);
}

fn mapWithinSpan(span: SourceSpan, repaired_offset: u32, boundary: SourceBoundary) u32 {
    if (repaired_offset <= span.repaired_start) return span.source_start;
    if (repaired_offset >= span.repaired_end) return span.source_end;

    const repaired_len = span.repaired_end - span.repaired_start;
    const source_len = span.source_end - span.source_start;
    if (repaired_len == source_len) return span.source_start + (repaired_offset - span.repaired_start);

    return switch (boundary) {
        .start => span.source_start,
        .end => span.source_end,
    };
}

fn checkedU32(value: usize) !u32 {
    return std.math.cast(u32, value) orelse error.TextTooLarge;
}

fn noteInvalidUtf8Repair(context: []const u8, input_len: usize, output_len: usize) void {
    const previous = invalid_utf8_repairs.fetchAdd(1, .monotonic);
    if (previous < invalid_utf8_warning_limit) {
        std.log.warn(
            "{s}: replaced invalid utf8 before enrichment processing input_bytes={d} output_bytes={d}",
            .{ context, input_len, output_len },
        );
    } else if (previous == invalid_utf8_warning_limit) {
        std.log.warn(
            "suppressing further invalid utf8 enrichment repair warnings after {d} repairs",
            .{invalid_utf8_warning_limit},
        );
    }
}

test "enrichment utf8 sanitizer preserves valid input without allocation" {
    const alloc = std.testing.allocator;
    var sanitized = try sanitizeAlloc(alloc, "valid \xc3\xa9", "test");
    defer sanitized.deinit(alloc);

    try std.testing.expect(!sanitized.repaired);
    try std.testing.expect(sanitized.owned == null);
    try std.testing.expectEqualStrings("valid \xc3\xa9", sanitized.text);
}

test "enrichment utf8 sanitizer replaces invalid input" {
    const alloc = std.testing.allocator;
    const repairs_before = invalidUtf8RepairCount();

    var sanitized = try sanitizeAlloc(alloc, "bad\xc2 text", "test");
    defer sanitized.deinit(alloc);

    try std.testing.expect(sanitized.repaired);
    try std.testing.expect(sanitized.source_map != null);
    try std.testing.expect(std.unicode.utf8ValidateSlice(sanitized.text));
    try std.testing.expect(std.mem.indexOf(u8, sanitized.text, &std.unicode.replacement_character_utf8) != null);
    try std.testing.expect(invalidUtf8RepairCount() > repairs_before);
}

test "enrichment utf8 sanitizer can repair without source offset map" {
    const alloc = std.testing.allocator;
    const repairs_before = invalidUtf8RepairCount();

    var sanitized = try sanitizeWithoutSourceMapAlloc(alloc, "bad\xc2 text", "test");
    defer sanitized.deinit(alloc);

    try std.testing.expect(sanitized.repaired);
    try std.testing.expect(sanitized.source_map == null);
    try std.testing.expect(std.unicode.utf8ValidateSlice(sanitized.text));
    try std.testing.expect(std.mem.indexOf(u8, sanitized.text, &std.unicode.replacement_character_utf8) != null);
    try std.testing.expect(invalidUtf8RepairCount() > repairs_before);
}

test "enrichment utf8 sanitizer maps repaired offsets to original source bytes" {
    const alloc = std.testing.allocator;

    var sanitized = try sanitizeAlloc(alloc, "abc\xc2def", "test");
    defer sanitized.deinit(alloc);

    try std.testing.expectEqualStrings("abc\xef\xbf\xbddef", sanitized.text);
    const source_map = sanitized.source_map.?;
    try std.testing.expectEqual(@as(u32, 0), source_map.mapBoundary(0, .start));
    try std.testing.expectEqual(@as(u32, 3), source_map.mapBoundary(3, .start));
    try std.testing.expectEqual(@as(u32, 3), source_map.mapBoundary(3, .end));
    try std.testing.expectEqual(@as(u32, 4), source_map.mapBoundary(6, .end));
    try std.testing.expectEqual(@as(u32, 7), source_map.mapBoundary(9, .end));
}

test "enrichment utf8 repair metric writes prometheus counter" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();

    try writePrometheus(&writer.writer);
    const out = writer.writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "# TYPE antfly_enrichment_invalid_utf8_repairs_total counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "antfly_enrichment_invalid_utf8_repairs_total ") != null);
}
