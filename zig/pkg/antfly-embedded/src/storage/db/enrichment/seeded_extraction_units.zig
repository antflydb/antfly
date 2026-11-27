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

//! Seeded document extraction units (#1791) as extraction units: exactly
//! the caller's text, page facts, layout and transcript phrases, with
//! `method: "seeded"` and nothing pending.
const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_seeds = @import("../artifact_seeds.zig");
const document_extraction = @import("document_extraction.zig");

/// One seeded unit with `unit_id` (copied).
pub fn unitAlloc(alloc: Allocator, seed: artifact_seeds.ExtractionUnit, unit_id: []const u8) !document_extraction.Unit {
    const id = try alloc.dupe(u8, unit_id);
    errdefer alloc.free(id);
    const unit_type = try alloc.dupe(u8, seed.unit_type);
    errdefer alloc.free(unit_type);
    const text = try alloc.dupe(u8, seed.text);
    errdefer alloc.free(text);
    const method = try alloc.dupe(u8, "seeded");
    errdefer alloc.free(method);
    const page_label: ?[]u8 = if (seed.page_label) |label| try alloc.dupe(u8, label) else null;
    errdefer if (page_label) |label| alloc.free(label);
    const regions = try alloc.alloc(document_extraction.TextRegion, seed.text_regions.len);
    errdefer alloc.free(regions);
    for (seed.text_regions, regions) |box, *slot| slot.* = .{ .span = box.span, .bbox = box.bbox, .font_size = box.font_size, .bold = box.bold, .italic = box.italic };
    const blocks = try alloc.alloc(document_extraction.TextBlock, seed.text_blocks.len);
    errdefer alloc.free(blocks);
    for (seed.text_blocks, blocks, 0..) |box, *slot, order| slot.* = .{ .span = box.span, .bbox = box.bbox, .reading_order = @intCast(order), .font_size = box.font_size, .bold = box.bold, .italic = box.italic };
    const spans = try alloc.alloc(document_extraction.TranscriptSpan, seed.spans.len);
    for (seed.spans, spans) |span, *slot| slot.* = .{
        .char_start = span.char_start,
        .char_end = span.char_end,
        .start_ms = span.start_ms,
        .end_ms = span.end_ms,
        .speaker_index = span.speaker,
    };
    return .{
        .unit_id = id,
        .unit_type = unit_type,
        .text = text,
        .method = method,
        .page_number = seed.page_number,
        .page_label = page_label,
        .page_bbox = seed.page_bbox,
        .page_rotation = seed.page_rotation,
        .transcript_used = spans.len > 0,
        .transcript_spans = spans,
        .text_regions = regions,
        .text_blocks = blocks,
    };
}

/// Replaces the producer's own pages with seeded pages of a partial
/// extraction seed as units stream through, keeping each native unit id and
/// rebasing character offsets over the combined text. Seeded pages carry no
/// pending OCR or transcription status, so completion skips them.
pub const SeededPagesSink = struct {
    alloc: Allocator,
    /// Seeded units by page number (borrowed).
    pages: []const artifact_seeds.ExtractionUnit,
    inner: document_extraction.UnitSink,
    cursor: usize = 0,
    replaced: usize = 0,

    pub fn sink(self: *SeededPagesSink) document_extraction.UnitSink {
        return .{ .ptr = self, .on_begin = onBegin, .on_unit = onUnit, .on_end = onEnd };
    }

    fn onBegin(ptr: *anyopaque, info: document_extraction.StreamInfo) anyerror!void {
        const self: *SeededPagesSink = @ptrCast(@alignCast(ptr));
        try self.inner.on_begin(self.inner.ptr, info);
    }

    fn onUnit(ptr: *anyopaque, unit: *document_extraction.Unit) anyerror!void {
        const self: *SeededPagesSink = @ptrCast(@alignCast(ptr));
        if (unit.page_number) |page| for (self.pages) |seed| {
            if (seed.page_number != page) continue;
            var replacement = try unitAlloc(self.alloc, seed, unit.unit_id);
            errdefer replacement.deinit(self.alloc);
            unit.deinit(self.alloc);
            unit.* = replacement;
            self.replaced += 1;
            break;
        };
        unit.char_start = std.math.cast(u32, self.cursor);
        self.cursor += unit.text.len;
        unit.char_end = std.math.cast(u32, self.cursor);
        try self.inner.on_unit(self.inner.ptr, unit);
    }

    fn onEnd(ptr: *anyopaque) anyerror!void {
        const self: *SeededPagesSink = @ptrCast(@alignCast(ptr));
        try self.inner.on_end(self.inner.ptr);
    }
};
