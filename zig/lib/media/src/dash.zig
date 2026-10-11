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

//! Bounded XML MPD parsing for single-representation explicit fMP4 SegmentList.
//! URI slices borrow the MPD. DTDs/entities, DRM, templates and availability-clock
//! scheduling fail explicitly; never turn XML extensions into guessed fetches.
const std = @import("std");
const hls = @import("hls.zig");
pub const Limits = hls.Limits;
pub const Playlist = hls.Playlist;
const Attribute = struct { name: []const u8, value: []const u8 };
const Tag = struct {
    name: []const u8,
    attrs: [32]Attribute = undefined,
    count: usize = 0,
    closing: bool = false,
    empty: bool = false,
    fn get(self: Tag, name: []const u8) ?[]const u8 {
        for (self.attrs[0..self.count]) |attr| if (std.mem.eql(u8, attr.name, name)) return attr.value;
        return null;
    }
};
fn whitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}
fn nameChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c == '-' or c == ':' or c == '.';
}
const Xml = struct {
    bytes: []const u8,
    cursor: usize = 0,
    fn spaces(self: *Xml) void {
        while (self.cursor < self.bytes.len and whitespace(self.bytes[self.cursor])) self.cursor += 1;
    }
    fn name(self: *Xml) ![]const u8 {
        const first = self.cursor;
        while (self.cursor < self.bytes.len and nameChar(self.bytes[self.cursor])) self.cursor += 1;
        if (self.cursor == first or !std.ascii.isAlphabetic(self.bytes[first])) return error.MalformedDash;
        return self.bytes[first..self.cursor];
    }
    fn next(self: *Xml) !?Tag {
        self.spaces();
        if (self.cursor == self.bytes.len) return null;
        if (self.bytes[self.cursor] != '<') return error.MalformedDash;
        self.cursor += 1;
        if (self.cursor >= self.bytes.len) return error.MalformedDash;
        if (self.bytes[self.cursor] == '!' or self.bytes[self.cursor] == '?') return error.UnsupportedDash;
        var tag = Tag{ .name = undefined };
        if (self.bytes[self.cursor] == '/') {
            tag.closing = true;
            self.cursor += 1;
        }
        tag.name = try self.name();
        while (true) {
            const before = self.cursor;
            self.spaces();
            if (self.cursor >= self.bytes.len) return error.MalformedDash;
            if (self.bytes[self.cursor] == '>') {
                self.cursor += 1;
                return tag;
            }
            if (!tag.closing and self.bytes[self.cursor] == '/' and self.cursor + 1 < self.bytes.len and self.bytes[self.cursor + 1] == '>') {
                self.cursor += 2;
                tag.empty = true;
                return tag;
            }
            if (tag.closing or self.cursor == before) return error.MalformedDash;
            if (tag.count == tag.attrs.len) return error.ResourceLimitExceeded;
            const key = try self.name();
            if (tag.get(key) != null) return error.MalformedDash;
            self.spaces();
            if (self.cursor >= self.bytes.len or self.bytes[self.cursor] != '=') return error.MalformedDash;
            self.cursor += 1;
            self.spaces();
            if (self.cursor >= self.bytes.len) return error.MalformedDash;
            const quote = self.bytes[self.cursor];
            if (quote != '\'' and quote != '"') return error.MalformedDash;
            self.cursor += 1;
            const start = self.cursor;
            while (self.cursor < self.bytes.len and self.bytes[self.cursor] != quote) : (self.cursor += 1) {
                if (self.bytes[self.cursor] == '&') return error.UnsupportedDash;
                if (self.bytes[self.cursor] == '<' or self.bytes[self.cursor] < 0x20) return error.MalformedDash;
            }
            if (self.cursor == self.bytes.len) return error.MalformedDash;
            tag.attrs[tag.count] = .{ .name = key, .value = self.bytes[start..self.cursor] };
            tag.count += 1;
            self.cursor += 1;
        }
    }
};
fn integer(value: []const u8) !u64 {
    if (value.len == 0) return error.MalformedDash;
    for (value) |c| if (!std.ascii.isDigit(c)) return error.MalformedDash;
    return std.fmt.parseInt(u64, value, 10) catch error.MalformedDash;
}
fn byteRange(value: ?[]const u8) !?hls.Range {
    const text = value orelse return null;
    const dash = std.mem.indexOfScalar(u8, text, '-') orelse return error.MalformedDash;
    const first = try integer(text[0..dash]);
    const last = try integer(text[dash + 1 ..]);
    if (last < first) return error.MalformedDash;
    const length = std.math.add(u64, last - first, 1) catch return error.MalformedDash;
    _ = std.math.add(u64, first, length) catch return error.MalformedDash;
    return .{ .offset = first, .length = length };
}
fn uri(value: ?[]const u8, limits: Limits) ![]const u8 {
    const text = value orelse return error.MalformedDash;
    if (text.len == 0) return error.MalformedDash;
    if (text.len > limits.max_uri_bytes) return error.ResourceLimitExceeded;
    for (text) |c| if (c <= 0x20 or c == 0x7f) return error.MalformedDash;
    return text;
}

/// Explicit static snapshots only. A single unencrypted MP4 video
/// representation avoids silently selecting the wrong adaptation/track.
pub fn parse(a: std.mem.Allocator, bytes: []const u8, limits: Limits) !Playlist {
    if (bytes.len > limits.max_bytes) return error.ResourceLimitExceeded;
    if (!std.unicode.utf8ValidateSlice(bytes)) return error.MalformedDash;
    var xml = Xml{ .bytes = bytes };
    // Optional XML declaration is allowed only at the start, with no processing
    // instructions after it. XML external entity expansion is never performed.
    if (std.mem.startsWith(u8, bytes, "<?xml ")) {
        const end = std.mem.indexOf(u8, bytes, "?>") orelse return error.MalformedDash;
        if (std.mem.indexOfScalar(u8, bytes[1..end], '<') != null or std.mem.indexOfScalar(u8, bytes[0..end], '&') != null) return error.MalformedDash;
        if (std.mem.indexOf(u8, bytes[0..end], "version=\"1.0\"") == null and std.mem.indexOf(u8, bytes[0..end], "version='1.0'") == null) return error.UnsupportedDash;
        xml.cursor = end + 2;
    }
    const expected = [_][]const u8{ "MPD", "Period", "AdaptationSet", "Representation", "SegmentList" };
    var stack: [7][]const u8 = undefined;
    var depth: usize = 0;
    var seen: [5]bool = @splat(false);
    var closed = false;
    var initialization: ?hls.Initialization = null;
    var duration: u64 = 0;
    var scale: u64 = 1;
    var sequence: u64 = 1;
    var segments: std.ArrayList(hls.Segment) = .empty;
    errdefer segments.deinit(a);
    while (try xml.next()) |tag| {
        if (closed) return error.MalformedDash;
        if (tag.closing) {
            if (depth == 0 or !std.mem.eql(u8, stack[depth - 1], tag.name)) return error.MalformedDash;
            depth -= 1;
            if (depth == 0) closed = true;
            continue;
        }
        // Attributes that affect fetches, DRM, timing or representation choice
        // must never be silently ignored in this intentionally bounded subset.
        const allowed: []const []const u8 = switch (depth) {
            0 => &.{ "xmlns", "type", "id", "profiles", "minBufferTime", "mediaPresentationDuration", "maxSegmentDuration" },
            1 => &.{ "id", "start", "duration" },
            2 => &.{ "id", "mimeType", "contentType", "codecs", "segmentAlignment", "startWithSAP" },
            3 => &.{ "id", "mimeType", "codecs", "bandwidth", "width", "height", "frameRate", "sar", "startWithSAP" },
            4 => &.{ "timescale", "duration", "startNumber" },
            5 => if (std.mem.eql(u8, tag.name, "Initialization")) &.{ "sourceURL", "range" } else &.{ "media", "mediaRange" },
            else => &.{},
        };
        for (tag.attrs[0..tag.count]) |attr| {
            var found = false;
            for (allowed) |key| if (std.mem.eql(u8, key, attr.name)) {
                found = true;
                break;
            };
            if (!found) return error.UnsupportedDash;
        }
        if (depth < expected.len) {
            if (!std.mem.eql(u8, tag.name, expected[depth])) return error.UnsupportedDash;
            if (seen[depth]) return error.UnsupportedDash;
            seen[depth] = true;
            if (tag.empty) return error.MalformedDash;
            switch (depth) {
                0 => {
                    if (!std.mem.eql(u8, tag.get("xmlns") orelse return error.MalformedDash, "urn:mpeg:dash:schema:mpd:2011")) return error.UnsupportedDash;
                    if (!std.mem.eql(u8, tag.get("type") orelse "static", "static")) return error.UnsupportedDash;
                },
                2, 3 => {
                    if (tag.get("mimeType")) |mime| if (!std.mem.eql(u8, mime, "video/mp4")) return error.UnsupportedDash;
                    if (tag.get("contentType")) |content| if (!std.mem.eql(u8, content, "video")) return error.UnsupportedDash;
                },
                4 => {
                    scale = try integer(tag.get("timescale") orelse "1");
                    duration = try integer(tag.get("duration") orelse return error.UnsupportedDash);
                    sequence = try integer(tag.get("startNumber") orelse "1");
                    if (scale == 0 or duration == 0) return error.MalformedDash;
                },
                else => {},
            }
        } else if (depth == 5 and std.mem.eql(u8, tag.name, "Initialization")) {
            if (initialization != null or segments.items.len != 0) return error.MalformedDash;
            initialization = .{ .uri = try uri(tag.get("sourceURL"), limits), .range = try byteRange(tag.get("range")) };
        } else if (depth == 5 and std.mem.eql(u8, tag.name, "SegmentURL")) {
            if (segments.items.len >= limits.max_segments) return error.ResourceLimitExceeded;
            const init = initialization orelse return error.MalformedDash;
            if (tag.get("index") != null or tag.get("indexRange") != null) return error.UnsupportedDash;
            try segments.append(a, .{ .uri = try uri(tag.get("media"), limits), .range = try byteRange(tag.get("mediaRange")), .initialization = init, .sequence = sequence, .discontinuity_sequence = 0, .duration_seconds = @as(f64, @floatFromInt(duration)) / @as(f64, @floatFromInt(scale)), .discontinuity = false });
            sequence = std.math.add(u64, sequence, 1) catch return error.MalformedDash;
        } else return error.UnsupportedDash;
        if (!tag.empty) {
            if (depth == stack.len) return error.ResourceLimitExceeded;
            stack[depth] = tag.name;
            depth += 1;
        }
    }
    if (!closed or depth != 0 or segments.items.len == 0 or !seen[4]) return error.MalformedDash;
    const target = std.math.divCeil(u64, duration, scale) catch return error.MalformedDash;
    return .{ .allocator = a, .segments = try segments.toOwnedSlice(a), .target_duration_seconds = target, .ended = true };
}

const fixture = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='static'><Period><AdaptationSet mimeType='video/mp4'><Representation id='v'><SegmentList timescale='1000' duration='1500' startNumber='7'><Initialization sourceURL='video.mp4' range='0-127'/><SegmentURL media='video.mp4' mediaRange='128-511'/><SegmentURL media='next.m4s'/></SegmentList></Representation></AdaptationSet></Period></MPD>";
test "DASH explicit SegmentList clocks ranges and sequence use fMP4 ingest contract" {
    var playlist = try parse(std.testing.allocator, fixture, .{});
    defer playlist.deinit();
    try std.testing.expect(playlist.ended);
    try std.testing.expectEqual(@as(u64, 2), playlist.target_duration_seconds);
    try std.testing.expectEqual(@as(usize, 2), playlist.segments.len);
    try std.testing.expectEqual(@as(u64, 7), playlist.segments[0].sequence);
    try std.testing.expectEqual(@as(u64, 8), playlist.segments[1].sequence);
    try std.testing.expectEqual(@as(f64, 1.5), playlist.segments[0].duration_seconds);
    try std.testing.expectEqual(hls.Range{ .offset = 128, .length = 384 }, playlist.segments[0].range.?);
    try std.testing.expectEqual(hls.Range{ .offset = 0, .length = 128 }, playlist.segments[1].initialization.range.?);
    try std.testing.expectError(error.ResourceLimitExceeded, parse(std.testing.allocator, fixture, .{ .max_segments = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, parse(std.testing.allocator, fixture, .{ .max_bytes = 10 }));
    try std.testing.expectError(error.MalformedDash, parse(std.testing.allocator, fixture[0 .. fixture.len - 6], .{}));
    try std.testing.expectError(error.UnsupportedDash, parse(std.testing.allocator, "<!DOCTYPE MPD><MPD/>", .{}));
    try std.testing.expectError(error.MalformedDash, byteRange("10-9"));
    try std.testing.expectError(error.MalformedDash, byteRange("0-18446744073709551615"));
}
fn allocationRun(a: std.mem.Allocator) !void {
    var playlist = try parse(a, fixture, .{});
    defer playlist.deinit();
}
test "DASH parser releases allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRun, .{});
}
