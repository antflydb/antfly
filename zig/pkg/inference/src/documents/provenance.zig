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

//! Read-only, bounded provenance coverage for source verification. Regions are
//! located against the exact reading text; no provider offsets are trusted
//! without checking their bytes. Ambiguous coverage fails closed.
const std = @import("std");
const c = @import("contract.zig");
const ir = @import("representation.zig");
pub const View = struct { text: []const u8, origin: c.Origin, regions: []const c.Region = &.{} };
const Range = struct { start: usize, end: usize };
const Span = struct { end: usize, literal: bool };

/// Every byte the candidate presents as literal must be literal in the
/// corroborating reading. Nonliteral candidate spans require no literal vote.
pub fn supportsLiteral(candidate: View, corroboration: View) !bool {
    const left = trimmed(candidate.text);
    const right = trimmed(corroboration.text);
    if (left.end == left.start or !std.mem.eql(u8, candidate.text[left.start..left.end], corroboration.text[right.start..right.end])) return false;
    var needed = try Cursor.init(candidate, left, true);
    var supplied = try Cursor.init(corroboration, right, false);
    var l = (try needed.next()) orelse return false;
    var r = (try supplied.next()) orelse return false;
    while (true) {
        if (l.literal and !r.literal) return false;
        const end = @min(l.end, r.end);
        if (end == left.end - left.start) return true;
        if (l.end == end) l = (try needed.next()) orelse return false;
        if (r.end == end) r = (try supplied.next()) orelse return false;
    }
}
fn trimmed(text: []const u8) Range {
    const left = std.mem.trimStart(u8, text, " \t\r\n");
    const content = std.mem.trimEnd(u8, left, " \t\r\n");
    return .{ .start = text.len - left.len, .end = text.len - left.len + content.len };
}
const Frame = struct {
    range: Range,
    regions: []const c.Region,
    cursor: usize,
    next_region: usize = 0,
    literal: bool,
};
const Cursor = struct {
    view: View,
    range: Range,
    unknown_literal: bool,
    frames: [18]Frame = undefined,
    len: usize = 0,
    fn init(view: View, range: Range, unknown_literal: bool) !Cursor {
        var self = Cursor{ .view = view, .range = range, .unknown_literal = unknown_literal };
        try self.push(.{ .start = 0, .end = view.text.len }, view.regions, ir.isLiteral(view.origin));
        return self;
    }
    fn push(self: *Cursor, range: Range, regions: []const c.Region, literal: bool) !void {
        if (self.len >= self.frames.len) return error.InvalidDocumentModelResult;
        var frame = Frame{ .range = range, .regions = regions, .cursor = range.start, .literal = literal };
        if (try uniform(regions, literal, 0)) {
            // Homogeneous provenance needs no text searching or offsets.
            frame.regions = &.{};
        } else {
            // Preflight the entire sibling scope before emitting any span.
            // The first ambiguous location ends the scan, bounding failed
            // searches and preventing later metadata from retracting proof.
            var position = range.start;
            for (regions) |region| {
                const located = locate(self.view.text, range, position, region) orelse {
                    frame.regions = &.{};
                    frame.literal = self.unknown_literal;
                    break;
                };
                position = located.end;
            }
        }
        self.frames[self.len] = frame;
        self.len += 1;
    }
    fn next(self: *Cursor) error{InvalidDocumentModelResult}!?Span {
        while (self.len > 0) {
            const frame = &self.frames[self.len - 1];
            var start: usize = undefined;
            var end: usize = undefined;
            var literal: bool = undefined;
            if (frame.next_region < frame.regions.len) {
                const region = frame.regions[frame.next_region];
                const located = locate(self.view.text, frame.range, frame.cursor, region) orelse unreachable;
                if (frame.cursor < located.start) {
                    start = frame.cursor;
                    end = located.start;
                    literal = frame.literal;
                    frame.cursor = located.start;
                } else {
                    frame.next_region += 1;
                    frame.cursor = located.end;
                    try self.push(located, region.children, ir.isLiteral(region.origin));
                    continue;
                }
            } else {
                start = frame.cursor;
                end = frame.range.end;
                literal = frame.literal;
                self.len -= 1;
            }
            const clipped_start = @max(start, self.range.start);
            const clipped_end = @min(end, self.range.end);
            if (clipped_start < clipped_end) return .{ .end = clipped_end - self.range.start, .literal = literal };
        }
        return null;
    }
};
fn uniform(regions: []const c.Region, literal: bool, depth: usize) error{InvalidDocumentModelResult}!bool {
    if (regions.len == 0) return true;
    if (depth > 16 or regions.len > 65536) return error.InvalidDocumentModelResult;
    for (regions) |region| {
        if (ir.isLiteral(region.origin) != literal or !try uniform(region.children, literal, depth + 1)) return false;
    }
    return true;
}
fn locate(text: []const u8, scope: Range, cursor: usize, region: c.Region) ?Range {
    if (region.text.len == 0) return null;
    if (region.text_start) |start| if (region.text_end) |end| {
        if (start >= cursor and end >= start and end <= scope.end and std.mem.eql(u8, text[start..end], region.text)) return .{ .start = start, .end = end };
    };
    const start = std.mem.indexOfPos(u8, text[0..scope.end], cursor, region.text) orelse return null;
    return .{ .start = start, .end = start + region.text.len };
}

test "document literal corroboration follows nested provenance and preserves unrelated literal spans" {
    const header = c.Region{ .id = 1, .text = "Header", .origin = .transcription };
    for ([_]c.Origin{ .interpretation, .derived, .estimated }) |origin| {
        const children = [_]c.Region{.{ .id = 9, .text = "caption", .origin = origin }};
        const regions = [_]c.Region{.{ .id = 7, .text = "caption", .origin = .transcription, .children = &children }};
        const mixed = View{ .text = "caption", .origin = .transcription, .regions = &regions };
        try std.testing.expect(!try supportsLiteral(.{ .text = "caption", .origin = .transcription }, mixed));
        const candidate_regions = [_]c.Region{ header, .{ .id = 2, .text = "caption", .origin = .interpretation } };
        const verifier_regions = [_]c.Region{ header, .{ .id = 42, .text = "caption", .origin = origin } };
        try std.testing.expect(try supportsLiteral(.{ .text = "Header\ncaption", .origin = .transcription, .regions = &candidate_regions }, .{ .text = "Header\ncaption", .origin = .transcription, .regions = &verifier_regions }));
        const bad_header = [_]c.Region{ .{ .id = 1, .text = "Header", .origin = origin }, .{ .id = 42, .text = "caption", .origin = origin } };
        try std.testing.expect(!try supportsLiteral(.{ .text = "Header\ncaption", .origin = .transcription, .regions = &candidate_regions }, .{ .text = "Header\ncaption", .origin = .transcription, .regions = &bad_header }));
    }
    // A separately established literal child remains usable beneath a mixed
    // page origin; provenance is checked per span rather than per page.
    const repaired = [_]c.Region{header};
    try std.testing.expect(try supportsLiteral(.{ .text = "Header\ncaption", .origin = .interpretation, .regions = &repaired }, .{ .text = "Header\ncaption", .origin = .transcription }));
}

test "document provenance checks verify offsets handle trimming and fail closed on ambiguous spans" {
    const unknown = [_]c.Region{.{ .id = 1, .text = "missing", .origin = .interpretation }};
    try std.testing.expect(!try supportsLiteral(.{ .text = "caption", .origin = .transcription }, .{ .text = "caption", .origin = .transcription, .regions = &unknown }));
    const wrong_offsets = [_]c.Region{.{ .id = 1, .text = "caption", .origin = .estimated, .text_start = 0, .text_end = 1 }};
    try std.testing.expect(!try supportsLiteral(.{ .text = "caption", .origin = .transcription }, .{ .text = "  caption\n", .origin = .transcription, .regions = &wrong_offsets }));
    const correct = [_]c.Region{.{ .id = 1, .text = "caption", .origin = .transcription, .text_start = 2, .text_end = 9 }};
    try std.testing.expect(try supportsLiteral(.{ .text = "caption", .origin = .transcription }, .{ .text = "  caption\n", .origin = .interpretation, .regions = &correct }));
    const overlapping = [_]c.Region{ .{ .id = 1, .text = "ab", .origin = .transcription }, .{ .id = 2, .text = "bc", .origin = .derived } };
    try std.testing.expect(!try supportsLiteral(.{ .text = "abc", .origin = .transcription }, .{ .text = "abc", .origin = .transcription, .regions = &overlapping }));
    const empty = [_]c.Region{.{ .id = 1, .text = "", .origin = .interpretation }};
    try std.testing.expect(!try supportsLiteral(.{ .text = "caption", .origin = .transcription }, .{ .text = "caption", .origin = .transcription, .regions = &empty }));
}
