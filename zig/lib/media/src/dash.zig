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

//! Bounded fMP4 DASH representation selection, inherited addressing and clocks.
//! All returned references are playlist-owned. XML predefined/numeric entities
//! are supported; DTDs, custom entities and DRM fail explicitly.
const std = @import("std");
const hls = @import("hls.zig");
pub const document = @import("dash_xml.zig");
const hierarchy = @import("dash_hierarchy.zig");
const availability = @import("dash_availability.zig");
pub const Selection = hierarchy.Selection;
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
    allocator: std.mem.Allocator,
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
                if (self.bytes[self.cursor] == '<' or self.bytes[self.cursor] < 0x20) return error.MalformedDash;
            }
            if (self.cursor == self.bytes.len) return error.MalformedDash;
            tag.attrs[tag.count] = .{ .name = key, .value = try document.decodeAttribute(self.allocator, self.bytes[start..self.cursor]) };
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

pub const Clock = struct {
    context: ?*anyopaque = null,
    /// UTC Unix milliseconds from a synchronized clock, refreshed on each reload.
    now: *const fn (?*anyopaque) i64,
};
pub const ReaderOptions = struct {
    playlist: Limits = .{},
    ingest: @import("live_mp4.zig").Options = .{},
    clock: ?Clock = null,
    selection: Selection = .{},
};

// ISO 8601 elapsed durations. Calendar years/months cannot define sample clocks.
fn elapsed(text: []const u8) !u64 {
    if (text.len < 3 or text[0] != 'P') return error.MalformedDash;
    var cursor: usize = 1;
    var in_time = false;
    var previous: u8 = 0;
    var total: u64 = 0;
    var fields: usize = 0;
    while (cursor < text.len) {
        if (text[cursor] == 'T') {
            if (in_time) return error.MalformedDash;
            in_time = true;
            cursor += 1;
            if (cursor == text.len) return error.MalformedDash;
            continue;
        }
        const first = cursor;
        while (cursor < text.len and std.ascii.isDigit(text[cursor])) cursor += 1;
        if (first == cursor) return error.MalformedDash;
        const whole = try integer(text[first..cursor]);
        var fraction: u64 = 0;
        if (cursor < text.len and text[cursor] == '.') {
            cursor += 1;
            const decimal = cursor;
            while (cursor < text.len and std.ascii.isDigit(text[cursor])) cursor += 1;
            const digits = cursor - decimal;
            if (digits == 0 or digits > 3) return error.UnsupportedDash;
            fraction = try integer(text[decimal..cursor]);
            for (digits..3) |_| fraction *= 10;
        }
        if (cursor == text.len) return error.MalformedDash;
        const unit = text[cursor];
        cursor += 1;
        const rank: u8 = switch (unit) {
            'D' => if (!in_time) 1 else return error.MalformedDash,
            'H' => if (in_time) 2 else return error.MalformedDash,
            'M' => if (in_time) 3 else return error.UnsupportedDash,
            'S' => if (in_time) 4 else return error.MalformedDash,
            else => return error.UnsupportedDash,
        };
        if (rank <= previous or (fraction != 0 and unit != 'S')) return error.MalformedDash;
        previous = rank;
        const factor: u64 = switch (unit) {
            'D' => 86400000,
            'H' => 3600000,
            'M' => 60000,
            'S' => 1000,
            else => unreachable,
        };
        total = std.math.add(u64, total, std.math.mul(u64, whole, factor) catch return error.MalformedDash) catch return error.MalformedDash;
        total = std.math.add(u64, total, fraction) catch return error.MalformedDash;
        fields += 1;
    }
    if (fields == 0) return error.MalformedDash;
    return total;
}
fn dateTime(text: []const u8) !i64 {
    // UTC Z only. No locale, leap seconds or ambiguous timezone assumptions.
    if (text.len < 20 or text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':' or text[text.len - 1] != 'Z') return error.UnsupportedDash;
    const year = try integer(text[0..4]);
    const month = try integer(text[5..7]);
    const day = try integer(text[8..10]);
    const hour = try integer(text[11..13]);
    const minute = try integer(text[14..16]);
    const second = try integer(text[17..19]);
    if (year < 1970 or month == 0 or month > 12 or hour > 23 or minute > 59 or second > 59) return error.MalformedDash;
    const leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0);
    const days_in_month = [_]u64{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (day == 0 or day > days_in_month[month - 1]) return error.MalformedDash;
    var days: u64 = 0;
    for (1970..@intCast(year)) |y| days += if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) @as(u64, 366) else 365;
    for (days_in_month[0..@intCast(month - 1)]) |count| days += count;
    days += day - 1;
    const fraction = if (text.len == 20) @as(u64, 0) else blk: {
        if (text[19] != '.' or text.len > 24) return error.UnsupportedDash;
        const digits = text[20 .. text.len - 1];
        if (digits.len == 0) return error.MalformedDash;
        var value = try integer(digits);
        for (digits.len..3) |_| value *= 10;
        break :blk value;
    };
    return @intCast(((days * 24 + hour) * 60 + minute) * 60000 + second * 1000 + fraction);
}
fn attributes(tag: Tag, allowed: []const []const u8) !void {
    for (tag.attrs[0..tag.count]) |attr| {
        var found = false;
        for (allowed) |key| if (std.mem.eql(u8, key, attr.name)) {
            found = true;
            break;
        };
        if (!found) return error.UnsupportedDash;
    }
}
const Timeline = struct { start: ?u64, duration: u64, repeat: ?u64 };
const Expansion = struct {
    a: std.mem.Allocator,
    limits: Limits,
    segments: std.ArrayList(hls.Segment) = .empty,
    owned: std.ArrayList([]u8) = .empty,
    owned_bytes: usize = 0,
    target: u64 = 1,
    scale: u64 = 1,
    offset: u64 = 0,
    sequence: u64 = 1,
    dynamic: bool = false,
    now: i128 = 0, // period-relative scaled nanoseconds (ticks * 1e9)
    availability_offset: availability.Offset = .{ .finite = 0 },
    availability_started: bool = true,
    depth: ?u64 = null, // UTC milliseconds
    period_end: ?u128 = null, // sample ticks * 1e9, excluding offset
    media: ?[]const u8 = null,
    initialization: ?hls.Initialization = null,
    representation: ?[]const u8 = null,
    bandwidth: ?u64 = null,
    fn availableEnd(self: *const Expansion) ?i128 {
        return switch (self.availability_offset) {
            .finite => |ns_| self.now + @as(i128, ns_) * self.scale,
            .infinite => null,
        };
    }
    fn deinit(self: *Expansion) void {
        for (self.owned.items) |value| self.a.free(value);
        self.owned.deinit(self.a);
        self.segments.deinit(self.a);
    }
    fn template(self: *Expansion, text: []const u8, number: ?u64, time: ?u64) ![]const u8 {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(self.a);
        var cursor: usize = 0;
        var distinguishes_segments = false;
        while (cursor < text.len) {
            if (text[cursor] != '$') {
                try out.append(self.a, text[cursor]);
                cursor += 1;
            } else if (cursor + 1 < text.len and text[cursor + 1] == '$') {
                try out.append(self.a, '$');
                cursor += 2;
            } else {
                const end = std.mem.indexOfScalarPos(u8, text, cursor + 1, '$') orelse return error.MalformedDash;
                const field = text[cursor + 1 .. end];
                if (std.mem.eql(u8, field, "RepresentationID")) {
                    try out.appendSlice(self.a, self.representation orelse return error.MalformedDash);
                } else {
                    const percent = std.mem.indexOfScalar(u8, field, '%') orelse field.len;
                    const name = field[0..percent];
                    if (std.mem.eql(u8, name, "Number") or std.mem.eql(u8, name, "Time")) distinguishes_segments = true;
                    const value = if (std.mem.eql(u8, name, "Number")) number orelse return error.UnsupportedDash else if (std.mem.eql(u8, name, "Time")) time orelse return error.UnsupportedDash else if (std.mem.eql(u8, name, "Bandwidth")) self.bandwidth orelse return error.MalformedDash else return error.UnsupportedDash;
                    var width: usize = 0;
                    if (percent != field.len) {
                        const format = field[percent..];
                        if (format.len < 4 or format[1] != '0' or format[format.len - 1] != 'd') return error.UnsupportedDash;
                        width = std.math.cast(usize, try integer(format[2 .. format.len - 1])) orelse return error.ResourceLimitExceeded;
                        if (width == 0 or width > 20) return error.ResourceLimitExceeded;
                    }
                    var buffer: [20]u8 = undefined;
                    const digits = try std.fmt.bufPrint(&buffer, "{d}", .{value});
                    if (width > digits.len) try out.appendNTimes(self.a, '0', width - digits.len);
                    try out.appendSlice(self.a, digits);
                }
                cursor = end + 1;
            }
            if (out.items.len > self.limits.max_uri_bytes) return error.ResourceLimitExceeded;
        }
        if (number != null and time != null and !distinguishes_segments) return error.UnsupportedDash;
        _ = try uri(out.items, self.limits);
        self.owned_bytes = std.math.add(usize, self.owned_bytes, out.items.len) catch return error.ResourceLimitExceeded;
        if (self.owned_bytes > self.limits.max_bytes) return error.ResourceLimitExceeded;
        const owned = try out.toOwnedSlice(self.a);
        errdefer self.a.free(owned);
        try self.owned.append(self.a, owned);
        return owned;
    }
    fn span(self: *Expansion, start: u64, duration: u64, count: u64) !void {
        if (duration == 0 or count == 0) return error.MalformedDash;
        const end_sequence = std.math.add(u64, self.sequence, count) catch return error.MalformedDash;
        const finish = @as(u128, start) + @as(u128, duration) * count;
        if (finish > std.math.maxInt(u64)) return error.MalformedDash;
        self.target = @max(self.target, std.math.divCeil(u64, duration, self.scale) catch return error.MalformedDash);
        var first: u128 = 0;
        var last: u128 = count;
        const origin = (@as(i128, start) - self.offset) * 1_000_000_000;
        const stride = @as(i128, duration) * 1_000_000_000;
        if (self.period_end) |period| {
            // The final segment can overlap a static period's end.
            last = @min(last, @as(u128, @intCast(@max(0, -@divFloor(origin - @as(i128, @intCast(period)), stride)))));
        }
        if (self.dynamic) {
            // INF makes advertised segments available at availabilityStartTime,
            // including later Periods, but never before that global instant.
            if (self.availability_offset == .infinite and !self.availability_started) last = 0;
            if (self.availableEnd()) |end| last = @min(last, @as(u128, @intCast(@max(0, @divFloor(end - origin, stride)))));
            if (self.depth) |depth_ms| {
                const cutoff = self.now - @as(i128, depth_ms) * self.scale * 1_000_000;
                first = @min(last, @as(u128, @intCast(@max(0, @divFloor(cutoff - origin, stride)))));
            }
        }
        // References entirely before the Period do not belong to this reader.
        first = @min(last, @max(first, @as(u128, @intCast(@max(0, @divFloor(-origin, stride))))));
        if (last - first > self.limits.max_segments - self.segments.items.len) return error.ResourceLimitExceeded;
        var index = first;
        while (index < last) : (index += 1) {
            const number = self.sequence + @as(u64, @intCast(index));
            const time = start + duration * @as(u64, @intCast(index));
            const address = try self.template(self.media.?, number, time);
            try self.segments.append(self.a, .{ .uri = address, .range = null, .initialization = self.initialization.?, .sequence = number, .discontinuity_sequence = 0, .duration_seconds = @as(f64, @floatFromInt(duration)) / @as(f64, @floatFromInt(self.scale)), .discontinuity = false, .dash = .{ .start_ticks = time, .duration_ticks = duration, .timescale = @intCast(self.scale), .presentation_offset = self.offset } });
        }
        self.sequence = end_sequence;
    }
};

/// Select a video representation with inherited list/template addressing.
/// Dynamic manifests require a supplied synchronized UTC clock; no guessed AST.
pub fn parse(a: std.mem.Allocator, bytes: []const u8, limits: Limits) !Playlist {
    return parseAt(a, bytes, limits, null);
}
pub fn parseAt(a: std.mem.Allocator, bytes: []const u8, limits: Limits, now_unix_ms: ?i64) !Playlist {
    return parseAtWithSelection(a, bytes, limits, now_unix_ms, .{});
}
pub fn parseAtWithSelection(a: std.mem.Allocator, bytes: []const u8, limits: Limits, now_unix_ms: ?i64, selection: Selection) !Playlist {
    return hierarchy.parse(a, bytes, limits, now_unix_ms, selection, parseRepresentation, elapsed, dateTime);
}
fn parseRepresentation(a: std.mem.Allocator, scratch: std.mem.Allocator, bytes: []const u8, limits: Limits, now_unix_ms: ?i64) !Playlist {
    if (bytes.len > limits.max_bytes) return error.ResourceLimitExceeded;
    if (!std.unicode.utf8ValidateSlice(bytes)) return error.MalformedDash;
    // Decoded attributes must survive until the hierarchy planner has copied
    // explicit segment references into the returned playlist.
    var xml = Xml{ .bytes = bytes, .allocator = scratch };
    if (std.mem.startsWith(u8, bytes, "<?xml ")) {
        const end = std.mem.indexOf(u8, bytes, "?>") orelse return error.MalformedDash;
        if (std.mem.indexOfScalar(u8, bytes[1..end], '<') != null or std.mem.indexOfScalar(u8, bytes[0..end], '&') != null) return error.MalformedDash;
        if (std.mem.indexOf(u8, bytes[0..end], "version=\"1.0\"") == null and std.mem.indexOf(u8, bytes[0..end], "version='1.0'") == null) return error.UnsupportedDash;
        xml.cursor = end + 2;
    }
    const expected = [_][]const u8{ "MPD", "Period", "AdaptationSet", "Representation" };
    var stack: [7][]const u8 = undefined;
    var depth: usize = 0;
    var seen: [5]bool = @splat(false);
    var closed = false;
    var template_mode = false;
    var timeline_seen = false;
    var explicit_duration: ?u64 = null;
    var presentation_duration: ?u64 = null;
    var period_duration: ?u64 = null;
    var period_start: u64 = 0;
    var availability_start: ?i64 = null;
    var update_period: ?u64 = null;
    var timeline: std.ArrayList(Timeline) = .empty;
    defer timeline.deinit(a);
    var state = Expansion{ .a = a, .limits = limits };
    defer state.deinit();
    while (try xml.next()) |tag| {
        if (closed) return error.MalformedDash;
        if (tag.closing) {
            if (depth == 0 or !std.mem.eql(u8, stack[depth - 1], tag.name)) return error.MalformedDash;
            depth -= 1;
            if (depth == 0) closed = true;
            continue;
        }
        if (depth < 4) {
            if (!std.mem.eql(u8, tag.name, expected[depth]) or seen[depth]) return error.UnsupportedDash;
            seen[depth] = true;
            if (tag.empty) return error.MalformedDash;
            switch (depth) {
                0 => {
                    try attributes(tag, &.{ "xmlns", "type", "id", "profiles", "minBufferTime", "mediaPresentationDuration", "maxSegmentDuration", "availabilityStartTime", "publishTime", "minimumUpdatePeriod", "timeShiftBufferDepth", "suggestedPresentationDelay" });
                    if (!std.mem.eql(u8, tag.get("xmlns") orelse return error.MalformedDash, "urn:mpeg:dash:schema:mpd:2011")) return error.UnsupportedDash;
                    const kind = tag.get("type") orelse "static";
                    if (!std.mem.eql(u8, kind, "static") and !std.mem.eql(u8, kind, "dynamic")) return error.MalformedDash;
                    state.dynamic = std.mem.eql(u8, kind, "dynamic");
                    if (tag.get("mediaPresentationDuration")) |value| presentation_duration = try elapsed(value);
                    if (tag.get("availabilityStartTime")) |value| availability_start = try dateTime(value);
                    if (tag.get("timeShiftBufferDepth")) |value| state.depth = try elapsed(value);
                    if (tag.get("minimumUpdatePeriod")) |value| update_period = try elapsed(value);
                    if (tag.get("suggestedPresentationDelay")) |value| _ = try elapsed(value);
                    if (tag.get("publishTime")) |value| _ = try dateTime(value);
                },
                1 => {
                    try attributes(tag, &.{ "id", "start", "duration" });
                    if (tag.get("start")) |value| period_start = try elapsed(value);
                    if (tag.get("duration")) |value| period_duration = try elapsed(value);
                },
                2, 3 => {
                    try attributes(tag, if (depth == 2) &.{ "id", "mimeType", "contentType", "codecs", "segmentAlignment", "startWithSAP" } else &.{ "id", "mimeType", "codecs", "bandwidth", "width", "height", "frameRate", "sar", "startWithSAP" });
                    if (tag.get("mimeType")) |mime| if (!std.mem.eql(u8, mime, "video/mp4")) return error.UnsupportedDash;
                    if (tag.get("contentType")) |content| if (!std.mem.eql(u8, content, "video")) return error.UnsupportedDash;
                    if (depth == 3) {
                        state.representation = tag.get("id");
                        if (tag.get("bandwidth")) |value| state.bandwidth = try integer(value);
                    }
                },
                else => unreachable,
            }
        } else if (depth == 4) {
            if (seen[4]) return error.UnsupportedDash;
            seen[4] = true;
            template_mode = std.mem.eql(u8, tag.name, "SegmentTemplate");
            if (!template_mode and !std.mem.eql(u8, tag.name, "SegmentList")) return error.UnsupportedDash;
            try attributes(tag, if (template_mode) &.{ "timescale", "duration", "startNumber", "presentationTimeOffset", "initialization", "media", "availabilityTimeOffset", "availabilityTimeComplete" } else &.{ "timescale", "duration", "startNumber", "availabilityTimeOffset", "availabilityTimeComplete" });
            state.scale = try integer(tag.get("timescale") orelse "1");
            if (state.scale == 0) return error.MalformedDash;
            if (state.scale > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
            if (tag.get("duration")) |value| {
                explicit_duration = try integer(value);
                if (explicit_duration.? == 0) return error.MalformedDash;
            }
            state.sequence = try integer(tag.get("startNumber") orelse "1");
            state.offset = try integer(tag.get("presentationTimeOffset") orelse "0");
            if (tag.get("availabilityTimeOffset")) |value| state.availability_offset = try availability.parse(value);
            if (tag.get("availabilityTimeComplete")) |value| if (!std.mem.eql(u8, value, "true")) return error.UnsupportedDash;
            if (template_mode) {
                state.media = try uri(tag.get("media"), limits);
                // Validate even an early live snapshot with no available segments.
                _ = try state.template(state.media.?, state.sequence, state.offset);
                const validation_uri = state.owned.pop().?;
                state.owned_bytes -= validation_uri.len;
                a.free(validation_uri);
                const initialization = try state.template(try uri(tag.get("initialization"), limits), null, null);
                state.initialization = .{ .uri = initialization, .range = null };
            } else if (tag.empty or explicit_duration == null) return error.MalformedDash;
        } else if (depth == 5 and !template_mode and std.mem.eql(u8, tag.name, "Initialization")) {
            try attributes(tag, &.{ "sourceURL", "range" });
            if (!tag.empty or state.initialization != null or state.segments.items.len != 0) return error.MalformedDash;
            state.initialization = .{ .uri = try uri(tag.get("sourceURL"), limits), .range = try byteRange(tag.get("range")) };
        } else if (depth == 5 and !template_mode and std.mem.eql(u8, tag.name, "SegmentURL")) {
            try attributes(tag, &.{ "media", "mediaRange" });
            if (!tag.empty) return error.MalformedDash;
            if (state.segments.items.len >= limits.max_segments) return error.ResourceLimitExceeded;
            const start_ticks = std.math.mul(u64, @intCast(state.segments.items.len), explicit_duration.?) catch return error.MalformedDash;
            try state.segments.append(a, .{ .uri = try uri(tag.get("media"), limits), .range = try byteRange(tag.get("mediaRange")), .initialization = state.initialization orelse return error.MalformedDash, .sequence = state.sequence, .discontinuity_sequence = 0, .duration_seconds = @as(f64, @floatFromInt(explicit_duration.?)) / @as(f64, @floatFromInt(state.scale)), .discontinuity = false, .dash = .{ .start_ticks = start_ticks, .duration_ticks = explicit_duration.?, .timescale = @intCast(state.scale), .presentation_offset = state.offset } });
            state.sequence = std.math.add(u64, state.sequence, 1) catch return error.MalformedDash;
            state.target = std.math.divCeil(u64, explicit_duration.?, state.scale) catch return error.MalformedDash;
        } else if (depth == 5 and template_mode and std.mem.eql(u8, tag.name, "SegmentTimeline")) {
            try attributes(tag, &.{});
            if (tag.empty or timeline_seen or explicit_duration != null) return error.MalformedDash;
            timeline_seen = true;
        } else if (depth == 6 and template_mode and std.mem.eql(u8, tag.name, "S")) {
            try attributes(tag, &.{ "t", "d", "r" });
            if (!tag.empty) return error.MalformedDash;
            if (timeline.items.len >= limits.max_timeline_entries) return error.ResourceLimitExceeded;
            const repeat = tag.get("r") orelse "0";
            const duration = try integer(tag.get("d") orelse return error.MalformedDash);
            if (duration == 0) return error.MalformedDash;
            try timeline.append(a, .{ .start = if (tag.get("t")) |value| try integer(value) else null, .duration = duration, .repeat = if (std.mem.eql(u8, repeat, "-1")) null else try integer(repeat) });
        } else return error.UnsupportedDash;
        if (!tag.empty) {
            if (depth == stack.len) return error.ResourceLimitExceeded;
            stack[depth] = tag.name;
            depth += 1;
        }
    }
    if (!closed or depth != 0 or !seen[4]) return error.MalformedDash;
    if (state.dynamic and !template_mode) return error.UnsupportedDash;
    if (period_duration == null and presentation_duration != null) {
        if (presentation_duration.? <= period_start) return error.MalformedDash;
        period_duration = presentation_duration.? - period_start;
    }
    if (period_duration) |duration_ms| state.period_end = @as(u128, duration_ms) * state.scale * 1_000_000;
    if (!template_mode and state.period_end != null) {
        var retained: usize = 0;
        for (state.segments.items) |segment| {
            if (@as(u128, segment.dash.?.start_ticks) * 1_000_000_000 >= state.period_end.?) break;
            retained += 1;
        }
        state.segments.items.len = retained;
    }
    if (state.dynamic) {
        const wall = now_unix_ms orelse return error.DashClockRequired;
        const start = availability_start orelse return error.MalformedDash;
        state.availability_started = wall >= start;
        state.now = (@as(i128, wall) - start - period_start) * state.scale * 1_000_000;
    }
    if (template_mode) {
        if (timeline_seen) {
            if (timeline.items.len == 0) return error.MalformedDash;
            var cursor: u64 = 0;
            for (timeline.items, 0..) |entry, i| {
                const start = entry.start orelse cursor;
                if (start < cursor) return error.MalformedDash;
                const count = if (entry.repeat) |repeat| std.math.add(u64, repeat, 1) catch return error.MalformedDash else blk: {
                    const boundary: u128 = if (i + 1 < timeline.items.len) @as(u128, timeline.items[i + 1].start orelse return error.MalformedDash) * 1_000_000_000 else if (state.period_end) |period| period + @as(u128, state.offset) * 1_000_000_000 else if (state.dynamic) @as(u128, @intCast(@max(0, (state.availableEnd() orelse return error.ResourceLimitExceeded) + @as(i128, state.offset) * 1_000_000_000))) else return error.UnsupportedDash;
                    if (boundary <= @as(u128, start) * 1_000_000_000) break :blk @as(u64, 0);
                    break :blk std.math.cast(u64, std.math.divCeil(u128, boundary - @as(u128, start) * 1_000_000_000, @as(u128, entry.duration) * 1_000_000_000) catch return error.MalformedDash) orelse return error.MalformedDash;
                };
                state.target = @max(state.target, std.math.divCeil(u64, entry.duration, state.scale) catch return error.MalformedDash);
                if (count == 0) continue;
                try state.span(start, entry.duration, count);
                cursor = std.math.add(u64, start, std.math.mul(u64, entry.duration, count) catch return error.MalformedDash) catch return error.MalformedDash;
            }
        } else {
            const duration = explicit_duration orelse return error.MalformedDash;
            const boundary = state.period_end orelse if (state.dynamic) @as(u128, @intCast(@max(0, state.availableEnd() orelse return error.ResourceLimitExceeded))) else return error.UnsupportedDash;
            const count = std.math.cast(u64, std.math.divCeil(u128, boundary, @as(u128, duration) * 1_000_000_000) catch return error.MalformedDash) orelse return error.MalformedDash;
            if (count != 0) try state.span(state.offset, duration, count);
            state.target = std.math.divCeil(u64, duration, state.scale) catch return error.MalformedDash;
        }
    }
    if (!state.dynamic and state.segments.items.len == 0) return error.MalformedDash;
    // Commit each allocation independently so every failing conversion cleans up.
    const segments = try state.segments.toOwnedSlice(a);
    errdefer a.free(segments);
    const owned = try state.owned.toOwnedSlice(a);
    var identity_text: [160]u8 = undefined;
    const identity = std.fmt.bufPrint(&identity_text, "dash-timing-v1/{d}/{d}/{d}/{d}", .{ state.scale, state.offset, period_start, availability_start orelse -1 }) catch unreachable;
    var timing_identity: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(identity, &timing_identity, .{});
    return .{ .allocator = a, .segments = segments, .owned_uris = owned, .target_duration_seconds = state.target, .ended = !state.dynamic, .timing_identity = timing_identity, .reload_after_seconds = if (state.dynamic and update_period != null) @as(f64, @floatFromInt(update_period.?)) / 1000 else null, .dash_window_start_ms = if (state.dynamic and state.depth != null) @as(i128, now_unix_ms.?) - availability_start.? - state.depth.? else null };
}

const fixture = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='static'><Period><AdaptationSet mimeType='video/mp4'><Representation id='v'><SegmentList timescale='1000' duration='1500' startNumber='7'><Initialization sourceURL='video.mp4' range='0-127'/><SegmentURL media='video.mp4' mediaRange='128-511'/><SegmentURL media='next.m4s'/></SegmentList></Representation></AdaptationSet></Period></MPD>";
const availability_prefix = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z'><Period duration='PT4S'><AdaptationSet><Representation id='v'><SegmentTemplate timescale='1000' duration='1000' initialization='init' media='$Number$-$Time$.m4s' availabilityTimeOffset='";
const availability_suffix = "'/></Representation></AdaptationSet></Period></MPD>";
test "DASH availability offsets use exact eligibility while preserving clocks and expiry" {
    const a = std.testing.allocator;
    inline for (.{ .{ "0", @as(i64, 1999), @as(usize, 1) }, .{ "0.5", @as(i64, 1500), @as(usize, 2) }, .{ "0.499999999", @as(i64, 1500), @as(usize, 1) }, .{ "-0.5", @as(i64, 1500), @as(usize, 1) }, .{ "-0.500000001", @as(i64, 1500), @as(usize, 0) }, .{ "1e-3", @as(i64, 1999), @as(usize, 2) }, .{ "INF", @as(i64, 0), @as(usize, 4) }, .{ "INF", @as(i64, -1), @as(usize, 0) } }) |case| {
        var playlist = try parseAt(a, availability_prefix ++ case[0] ++ availability_suffix, .{}, case[1]);
        defer playlist.deinit();
        try std.testing.expectEqual(case[2], playlist.segments.len);
        for (playlist.segments, 0..) |segment, i| {
            try std.testing.expectEqual(@as(u64, @intCast(i + 1)), segment.sequence);
            try std.testing.expectEqual(@as(u64, @intCast(i * 1000)), segment.dash.?.start_ticks);
        }
    }
    const mpd = availability_prefix ++ "1" ++ availability_suffix;
    const expiring = try std.mem.replaceOwned(u8, a, mpd, "type='dynamic'", "type='dynamic' timeShiftBufferDepth='PT1S'");
    defer a.free(expiring);
    var playlist = try parseAt(a, expiring, .{}, 2500);
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 2), playlist.segments.len);
    try std.testing.expectEqual(@as(u64, 2), playlist.segments[0].sequence);
    try std.testing.expectEqual(@as(?i128, 1500), playlist.dash_window_start_ms);
    var ordinary = try parseAt(a, availability_prefix ++ "0" ++ availability_suffix, .{}, 3000);
    defer ordinary.deinit();
    try std.testing.expectEqual(ordinary.timing_identity, playlist.timing_identity);
    try std.testing.expectEqual(ordinary.segments[1].dash, playlist.segments[0].dash);
}
const inherited_availability_fixture = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z'><BaseURL availabilityTimeOffset='0.25'>https://media.test/</BaseURL><Period duration='PT4S'><BaseURL availabilityTimeOffset='0.125'>period/</BaseURL><SegmentTemplate timescale='1000' duration='1000' initialization='init' media='$Number$.m4s' availabilityTimeOffset='4'/><AdaptationSet><BaseURL availabilityTimeOffset='0.25'>adaptation/</BaseURL><Representation id='v'><BaseURL availabilityTimeOffset='-0.125'>representation/</BaseURL><SegmentTemplate availabilityTimeOffset='0.5'/></Representation></AdaptationSet></Period></MPD>";
test "DASH availability sums the selected BaseURL path and overrides inherited templates" {
    var playlist = try parseAt(std.testing.allocator, inherited_availability_fixture, .{}, 1000);
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 2), playlist.segments.len);
    try std.testing.expectEqualStrings("https://media.test/period/adaptation/representation/2.m4s", playlist.segments[1].uri);
}
test "DASH availability selects only the chosen BaseURL alternative" {
    const a = std.testing.allocator;
    const alternatives = try std.mem.replaceOwned(u8, a, inherited_availability_fixture, "<Period duration", "<BaseURL availabilityTimeOffset='0.5'>https://other.test/</BaseURL><Period duration");
    defer a.free(alternatives);
    var first = try parseAt(a, alternatives, .{}, 750);
    defer first.deinit();
    var second = try parseAtWithSelection(a, alternatives, .{}, 750, .{ .base_url_index = 1 });
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), first.segments.len);
    try std.testing.expectEqual(@as(usize, 2), second.segments.len);
    try std.testing.expectEqualStrings("https://other.test/period/adaptation/representation/2.m4s", second.segments[1].uri);
}
test "DASH availability expands open timelines with offset and requires finite INF extent" {
    const a = std.testing.allocator;
    const epoch = try dateTime("2026-01-01T00:00:00Z");
    const early = try std.mem.replaceOwned(u8, a, live_fixture, "startNumber='7'", "startNumber='7' availabilityTimeOffset='0.999999999'");
    defer a.free(early);
    var waiting = try parseAt(a, early, .{}, epoch + 3000);
    defer waiting.deinit();
    try std.testing.expectEqual(@as(usize, 0), waiting.segments.len);
    const boundary = try std.mem.replaceOwned(u8, a, early, "0.999999999", "1");
    defer a.free(boundary);
    var available = try parseAt(a, boundary, .{}, epoch + 3000);
    defer available.deinit();
    try std.testing.expectEqual(@as(usize, 1), available.segments.len);
    try std.testing.expectEqual(@as(u64, 7), available.segments[0].sequence);
    try std.testing.expectEqual(@as(u64, 9000), available.segments[0].dash.?.start_ticks);
    const infinite = try std.mem.replaceOwned(u8, a, boundary, "availabilityTimeOffset='1'", "availabilityTimeOffset='INF'");
    defer a.free(infinite);
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, infinite, .{}, epoch + 3000));
    const advertised = try std.mem.replaceOwned(u8, a, infinite, "r='-1'", "r='2'");
    defer a.free(advertised);
    var bounded = try parseAt(a, advertised, .{}, epoch + 2000);
    defer bounded.deinit();
    try std.testing.expectEqual(@as(usize, 3), bounded.segments.len);
}
test "DASH availability bounds unbounded INF timelines malformed precision and expansion" {
    const a = std.testing.allocator;
    const infinite = availability_prefix ++ "INF" ++ availability_suffix;
    const unbounded = try std.mem.replaceOwned(u8, a, infinite, " duration='PT4S'", "");
    defer a.free(unbounded);
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, unbounded, .{}, 0));
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, infinite, .{ .max_segments = 3 }, 0));
    try std.testing.expectError(error.MalformedDash, parseAt(a, availability_prefix ++ "NaN" ++ availability_suffix, .{}, 0));
    try std.testing.expectError(error.UnsupportedDash, parseAt(a, availability_prefix ++ "1e-10" ++ availability_suffix, .{}, 0));
    const partial = try std.mem.replaceOwned(u8, a, infinite, "availabilityTimeOffset='INF'", "availabilityTimeOffset='INF' availabilityTimeComplete='false'");
    defer a.free(partial);
    try std.testing.expectError(error.UnsupportedDash, parseAt(a, partial, .{}, 0));
}
fn availabilityAllocationRun(a: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    var playlist = try parseAt(no_resize.allocator(), inherited_availability_fixture, .{}, 1000);
    defer playlist.deinit();
}
test "DASH availability inherited addressing releases every failing allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, availabilityAllocationRun, .{});
}
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
fn allocationRun(backing: std.mem.Allocator) !void {
    // Arena resize success depends on heap placement; exercise all allocation
    // fallbacks deterministically, as in the template/inheritance campaigns.
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    var playlist = try parse(no_resize.allocator(), fixture, .{});
    defer playlist.deinit();
}
test "DASH parser releases allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRun, .{});
}

const template_fixture = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' mediaPresentationDuration='PT5S'><Period><AdaptationSet mimeType='video/mp4'><Representation id='v' bandwidth='128000'><SegmentTemplate timescale='1000' duration='2000' startNumber='7' initialization='$RepresentationID$-$Bandwidth$.mp4' media='seg-$$-$Number%05d$-$Time$.m4s'/></Representation></AdaptationSet></Period></MPD>";
const live_fixture = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='2026-01-01T00:00:00Z' timeShiftBufferDepth='PT4S' minimumUpdatePeriod='PT1S'><Period start='PT2S'><AdaptationSet><Representation id='v'><SegmentTemplate timescale='1000' presentationTimeOffset='9000' startNumber='7' initialization='init.mp4' media='seg-$Time$.m4s'><SegmentTimeline><S t='9000' d='2000' r='-1'/></SegmentTimeline></SegmentTemplate></Representation></AdaptationSet></Period></MPD>";
test "DASH templates own bounded expanded URI clocks and static last partial period" {
    var playlist = try parse(std.testing.allocator, template_fixture, .{});
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 3), playlist.segments.len);
    try std.testing.expectEqualStrings("v-128000.mp4", playlist.segments[0].initialization.uri);
    try std.testing.expectEqualStrings("seg-$-00007-0.m4s", playlist.segments[0].uri);
    try std.testing.expectEqualStrings("seg-$-00009-4000.m4s", playlist.segments[2].uri);
    try std.testing.expectEqual(@as(usize, 4), playlist.owned_uris.len);
    try std.testing.expectError(error.ResourceLimitExceeded, parse(std.testing.allocator, template_fixture, .{ .max_segments = 2 }));
    try std.testing.expectError(error.ResourceLimitExceeded, parse(std.testing.allocator, template_fixture, .{ .max_uri_bytes = 12 }));
}
test "DASH live timeline filters incomplete and expired segments with synchronized UTC" {
    const a = std.testing.allocator;
    const epoch = try dateTime("2026-01-01T00:00:00Z");
    try std.testing.expectError(error.DashClockRequired, parse(a, live_fixture, .{}));
    var first = try parseAt(a, live_fixture, .{}, epoch + 10000);
    defer first.deinit();
    try std.testing.expect(!first.ended);
    try std.testing.expectEqual(@as(f64, 1), first.reload_after_seconds.?);
    try std.testing.expectEqual(@as(usize, 2), first.segments.len);
    try std.testing.expectEqual(@as(u64, 9), first.segments[0].sequence);
    try std.testing.expectEqualStrings("seg-13000.m4s", first.segments[0].uri);
    try std.testing.expectEqualStrings("seg-15000.m4s", first.segments[1].uri);
    var next = try parseAt(a, live_fixture, .{}, epoch + 12000);
    defer next.deinit();
    try std.testing.expectEqual(@as(u64, 10), next.segments[0].sequence);
    try std.testing.expectEqualStrings(first.segments[1].uri, next.segments[0].uri);
    var early = try parseAt(a, live_fixture, .{}, epoch + 3000);
    defer early.deinit();
    try std.testing.expectEqual(@as(usize, 0), early.segments.len);
    try std.testing.expectEqual(@as(u64, 2), early.target_duration_seconds);
    // The number of expired historical segments does not allocate or loop.
    var late = try parseAt(a, live_fixture, .{ .max_segments = 2 }, epoch + 1000000000);
    defer late.deinit();
    try std.testing.expectEqual(@as(usize, 2), late.segments.len);
}
test "DASH elapsed and UTC reject invalid calendar ambiguous and overflowing clocks" {
    try std.testing.expectEqual(@as(u64, 93784250), try elapsed("P1DT2H3M4.250S"));
    try std.testing.expectEqual(@as(i64, 0), try dateTime("1970-01-01T00:00:00Z"));
    try std.testing.expectEqual(@as(i64, 951782400125), try dateTime("2000-02-29T00:00:00.125Z"));
    try std.testing.expectError(error.MalformedDash, dateTime("2100-02-29T00:00:00Z"));
    try std.testing.expectError(error.UnsupportedDash, dateTime("2026-01-01T00:00:00+01:00"));
    try std.testing.expectError(error.UnsupportedDash, elapsed("P1M"));
    try std.testing.expectError(error.MalformedDash, elapsed("PT18446744073709551615S"));
    try std.testing.expectError(error.MalformedDash, elapsed("PT1S2H"));
}
fn templateAllocationRun(backing: std.mem.Allocator) !void {
    // Arena growth may resize in place depending on heap placement. Exercise
    // deterministic allocation fallbacks at every failure position instead.
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var playlist = try parse(a, template_fixture, .{});
    defer playlist.deinit();
    var live_playlist = try parseAt(a, live_fixture, .{}, try dateTime("2026-01-01T00:00:10Z"));
    defer live_playlist.deinit();
}
test "DASH template expansion releases allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, templateAllocationRun, .{});
}

const inherited_fixture =
    "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><BaseURL>https://media.example/root/</BaseURL>" ++
    "<Period id='p' duration='PT4S'><BaseURL>period/</BaseURL>" ++
    "<SegmentTemplate timescale='1000' duration='2000' startNumber='7' initialization='init-$RepresentationID$.mp4' media='$RepresentationID$-$Number$.m4s?key=a&amp;sig=b'/>" ++
    "<AdaptationSet id='audio' contentType='audio'><Representation id='a'/></AdaptationSet>" ++
    "<AdaptationSet id='video' contentType='video'><BaseURL>video/</BaseURL><SegmentTemplate startNumber='9'/>" ++
    "<Representation id='high' bandwidth='2000'><BaseURL>hi/</BaseURL></Representation>" ++
    "<Representation id='low' bandwidth='500'><BaseURL>lo/</BaseURL></Representation>" ++
    "</AdaptationSet></Period></MPD>";
test "DASH inherited templates select representations and resolve signed BaseURL chains" {
    const a = std.testing.allocator;
    var playlist = try parseAtWithSelection(a, inherited_fixture, .{}, null, .{ .max_bandwidth = 1000 });
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 2), playlist.segments.len);
    try std.testing.expectEqual(@as(u64, 9), playlist.segments[0].sequence);
    try std.testing.expectEqualStrings("https://media.example/root/period/video/lo/low-9.m4s?key=a&sig=b", playlist.segments[0].uri);
    try std.testing.expectEqualStrings("https://media.example/root/period/video/lo/init-low.mp4", playlist.segments[0].initialization.uri);
    var high = try parseAtWithSelection(a, inherited_fixture, .{}, null, .{ .adaptation_id = "video", .representation_id = "high" });
    defer high.deinit();
    try std.testing.expectEqualStrings("https://media.example/root/period/video/hi/high-10.m4s?key=a&sig=b", high.segments[1].uri);
    try std.testing.expectError(error.DashSelectionNotFound, parseAtWithSelection(a, inherited_fixture, .{}, null, .{ .representation_id = "missing" }));
    try std.testing.expectError(error.DashSelectionNotFound, parseAtWithSelection(a, inherited_fixture, .{}, null, .{ .max_bandwidth = 100 }));
}
test "DASH explicit escaped references remain owned after parsing" {
    const a = std.testing.allocator;
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period><AdaptationSet><Representation><SegmentList duration='2'><Initialization sourceURL='init.mp4?x=1&amp;y=2'/><SegmentURL media='seg.m4s?x=3&amp;y=4'/></SegmentList></Representation></AdaptationSet></Period></MPD>";
    const input = try a.dupe(u8, mpd);
    var playlist = try parse(a, input, .{});
    defer playlist.deinit();
    @memset(input, 0);
    a.free(input);
    try std.testing.expectEqualStrings("init.mp4?x=1&y=2", playlist.segments[0].initialization.uri);
    try std.testing.expectEqualStrings("seg.m4s?x=3&y=4", playlist.segments[0].uri);
}
test "DASH BaseURL alternatives relative manifests and invalid absolute URLs" {
    const a = std.testing.allocator;
    const prefix = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><BaseURL>first/</BaseURL><BaseURL>second/</BaseURL>";
    const suffix = "<Period duration='PT2S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    const manifest = prefix ++ suffix;
    try std.testing.expectError(error.DashManifestUrlRequired, parse(a, manifest, .{}));
    var playlist = try parseAtWithSelection(a, manifest, .{}, null, .{ .manifest_url = "https://media.example/live/index.mpd", .base_url_index = 1 });
    defer playlist.deinit();
    try std.testing.expectEqualStrings("https://media.example/live/second/seg-1.m4s", playlist.segments[0].uri);
    try std.testing.expectError(error.DashSelectionNotFound, parseAtWithSelection(a, manifest, .{}, null, .{ .manifest_url = "https://media.example/live/index.mpd", .base_url_index = 2 }));
    for ([_][]const u8{ "file:///root/", "https://user@media.example/", "https://media.example/#fragment", "https://media.example/bad\\path/" }) |url| {
        const bad = try std.fmt.allocPrint(a, "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><BaseURL>{s}</BaseURL>{s}", .{ url, suffix });
        defer a.free(bad);
        try std.testing.expectError(error.InvalidMediaUrl, parse(a, bad, .{}));
    }
}
test "DASH namespace prefixes and selection identity are stable across descriptive XML edits" {
    const a = std.testing.allocator;
    const prefixed = "<d:MPD xmlns:d='urn:mpeg:dash:schema:mpd:2011' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='ignored'><d:Period duration='PT2S'><d:AdaptationSet><d:Representation id='v'><d:SegmentTemplate duration='2' initialization='init.mp4' media='seg-$Number$.m4s'/></d:Representation></d:AdaptationSet></d:Period></d:MPD>";
    var first = try parse(a, prefixed, .{});
    defer first.deinit();
    const changed = try std.mem.replaceOwned(u8, a, prefixed, "id='v'", "id='other'");
    defer a.free(changed);
    var second = try parse(a, changed, .{});
    defer second.deinit();
    try std.testing.expect(!std.meta.eql(first.dash_periods[0].configuration, second.dash_periods[0].configuration));
    const spoof = try std.mem.replaceOwned(u8, a, prefixed, "http://www.w3.org/2001/XMLSchema-instance", "urn:wrong");
    defer a.free(spoof);
    try std.testing.expectError(error.UnsupportedDash, parse(a, spoof, .{}));
}
test "DASH bandwidth ceiling chooses the largest eligible representation" {
    var playlist = try parseAtWithSelection(std.testing.allocator, inherited_fixture, .{}, null, .{ .max_bandwidth = 3000 });
    defer playlist.deinit();
    try std.testing.expectEqualStrings("https://media.example/root/period/video/hi/high-9.m4s?key=a&sig=b", playlist.segments[0].uri);
}
test "DASH inherited timeline children override nearer levels and reject critical features" {
    const a = std.testing.allocator;
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period duration='PT4S'><SegmentTemplate initialization='init.mp4' media='seg-$Time$.m4s'><SegmentTimeline><S t='0' d='2' r='1'/></SegmentTimeline></SegmentTemplate><AdaptationSet><Representation id='v'><SegmentTemplate><SegmentTimeline><S t='0' d='1' r='3'/></SegmentTimeline></SegmentTemplate></Representation></AdaptationSet></Period></MPD>";
    var playlist = try parse(a, mpd, .{});
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 4), playlist.segments.len);
    try std.testing.expectEqualStrings("seg-3.m4s", playlist.segments[3].uri);
    for ([_][]const u8{ "<ContentProtection/>", "<EssentialProperty schemeIdUri='urn:unknown'/>", "<SegmentBase/>" }) |feature| {
        const replacement = try std.fmt.allocPrint(a, "<Representation id='v'>{s}", .{feature});
        defer a.free(replacement);
        const bad = try std.mem.replaceOwned(u8, a, mpd, "<Representation id='v'>", replacement);
        defer a.free(bad);
        try std.testing.expectError(error.UnsupportedDash, parse(a, bad, .{}));
    }
}
const periods_fixture = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' mediaPresentationDuration='PT8S'><Period id='one' duration='PT4S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' startNumber='7' initialization='init.mp4' media='one-$Number$.m4s'/></Representation></AdaptationSet></Period><Period id='two'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='two-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
test "DASH multiple periods infer timing and preserve period-local sequence resets" {
    const a = std.testing.allocator;
    var playlist = try parse(a, periods_fixture, .{});
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 2), playlist.dash_periods.len);
    try std.testing.expectEqual(@as(usize, 4), playlist.segments.len);
    try std.testing.expectEqual(@as(u64, 4000), playlist.dash_periods[1].start_ms);
    try std.testing.expectEqual(@as(?u64, 4000), playlist.dash_periods[1].duration_ms);
    try std.testing.expectEqual(@as(u64, 8), playlist.segments[1].sequence);
    try std.testing.expectEqual(@as(u64, 1), playlist.segments[2].sequence);
    try std.testing.expect(playlist.segments[1].dash.?.finishesPeriod());
    try std.testing.expect(playlist.segments[2].dash.?.startsPeriod());
    var second = try parseAtWithSelection(a, periods_fixture, .{}, null, .{ .period_id = "two" });
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 2), second.segments.len);
    try std.testing.expectEqual(@as(u64, 4000), second.segments[0].dash.?.period.start_ms);
    const from_next = try std.mem.replaceOwned(u8, a, periods_fixture, "duration='PT4S'", "");
    defer a.free(from_next);
    const explicit_next = try std.mem.replaceOwned(u8, a, from_next, "id='two'", "id='two' start='PT4S'");
    defer a.free(explicit_next);
    var inferred = try parse(a, explicit_next, .{});
    defer inferred.deinit();
    try std.testing.expectEqual(@as(?u64, 4000), inferred.dash_periods[0].duration_ms);
    try std.testing.expectError(error.ResourceLimitExceeded, parse(a, periods_fixture, .{ .max_segments = 3 }));
    try std.testing.expectError(error.MalformedDash, parse(a, from_next, .{}));
    const duplicate = try std.mem.replaceOwned(u8, a, periods_fixture, "id='two'", "id='one'");
    defer a.free(duplicate);
    try std.testing.expectError(error.DashSelectionAmbiguous, parse(a, duplicate, .{}));
    const overlap = try std.mem.replaceOwned(u8, a, periods_fixture, "id='two'", "id='two' start='PT3S'");
    defer a.free(overlap);
    try std.testing.expectError(error.MalformedDash, parse(a, overlap, .{}));
}
fn periodAllocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    var playlist = try parse(no_resize.allocator(), periods_fixture, .{});
    defer playlist.deinit();
}
test "DASH combined periods release every failing allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, periodAllocationRun, .{});
}
test "DASH announced period gaps preserve exact timing and cannot hide a skipped period" {
    const a = std.testing.allocator;
    const longer = try std.mem.replaceOwned(u8, a, periods_fixture, "mediaPresentationDuration='PT8S'", "mediaPresentationDuration='PT10S'");
    defer a.free(longer);
    const gap = try std.mem.replaceOwned(u8, a, longer, "id='two'", "id='two' start='PT6S'");
    defer a.free(gap);
    var playlist = try parse(a, gap, .{});
    defer playlist.deinit();
    const cursor = @import("dash_cursor.zig");
    var position = cursor.Cursor.initial(playlist.segments[0]);
    position.consumed(playlist.segments[1]);
    try std.testing.expectEqual(@as(?usize, 2), try position.select(playlist));
    const start_time = playlist.segments[2].dash.?.presentationStart();
    try std.testing.expectEqual(@as(i128, 6000), @divExact(start_time.numerator, start_time.denominator));
    var skipped = playlist;
    var period = playlist.dash_periods[1];
    period.start_ms = 7000;
    skipped.dash_periods = &.{period};
    skipped.segments = playlist.segments[2..];
    try std.testing.expectError(error.MediaStreamGap, position.select(skipped));
    position.consumed(playlist.segments[3]);
    var rollback = playlist;
    rollback.dash_periods = playlist.dash_periods[0..1];
    rollback.segments = playlist.segments[0..2];
    try std.testing.expectError(error.SourceVersionChanged, position.select(rollback));
    var learned = playlist.dash_periods[0];
    learned.duration_ms = 1000;
    var consumed = playlist.segments[1].dash.?;
    consumed.period.duration_ms = null;
    const late_end = @import("dash_cursor.zig").Cursor{ .period = consumed.period, .sequence = 9, .last = consumed };
    var expired = playlist;
    expired.dash_periods = &.{ learned, playlist.dash_periods[1] };
    expired.segments = &.{};
    try std.testing.expectError(error.SourceVersionChanged, late_end.select(expired));
    // The namespace for anonymous fallback IDs is distinct from supplied IDs.
    const named = try std.mem.replaceOwned(u8, a, periods_fixture, "id='one'", "id='anonymous-start/0'");
    defer a.free(named);
    var supplied = try parse(a, named, .{});
    defer supplied.deinit();
    const anonymous = try std.mem.replaceOwned(u8, a, periods_fixture, "id='one'", "");
    defer a.free(anonymous);
    var inferred = try parse(a, anonymous, .{});
    defer inferred.deinit();
    try std.testing.expect(!std.meta.eql(supplied.dash_periods[0].identity, inferred.dash_periods[0].identity));
    const fractional = hls.DashTiming{ .start_ticks = 0, .duration_ticks = 2, .timescale = 3, .presentation_offset = 1, .period = .{ .duration_ms = 333 } };
    try std.testing.expect(fractional.startsPeriod());
    try std.testing.expect(fractional.finishesPeriod());
    try std.testing.expectEqual(@as(i128, -1000), fractional.presentationStart().numerator);
    try std.testing.expectEqual(@as(i128, 1000), fractional.presentationEnd().numerator);
}
test "DASH future timelines keep independent metadata and aggregate segment budgets" {
    const a = std.testing.allocator;
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z'><Period id='one' duration='PT2S'><AdaptationSet><Representation><SegmentTemplate duration='2' initialization='init.mp4' media='one-$Number$.m4s'/></Representation></AdaptationSet></Period><Period id='two' start='PT2S'><AdaptationSet><Representation><SegmentTemplate initialization='init.mp4' media='two-$Time$.m4s'><SegmentTimeline><S d='2'/></SegmentTimeline></SegmentTemplate></Representation></AdaptationSet></Period></MPD>";
    var playlist = try parseAt(a, mpd, .{ .max_segments = 1 }, 2000);
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 1), playlist.segments.len);
    try std.testing.expectEqual(@as(usize, 2), playlist.dash_periods.len);
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, mpd, .{ .max_segments = 1 }, 4000));
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, mpd, .{ .max_timeline_entries = 0 }, 2000));
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, mpd, .{ .max_periods = 1 }, 2000));
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, mpd, .{ .max_planning_bytes = 10 }, 2000));
    var early = try parseAt(a, mpd, .{ .max_segments = 0 }, 0);
    defer early.deinit();
    try std.testing.expectEqual(@as(usize, 0), early.segments.len);
}
fn inheritedAllocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    var playlist = try parseAtWithSelection(no_resize.allocator(), inherited_fixture, .{}, null, .{ .representation_id = "low" });
    defer playlist.deinit();
}
test "DASH inherited addressing releases allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, inheritedAllocationRun, .{});
}

test "DASH explicit timeline repeats start offsets and validates unavailable templates" {
    const a = std.testing.allocator;
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period duration='PT6S'><AdaptationSet><Representation id='v'><SegmentTemplate timescale='1000' presentationTimeOffset='4000' startNumber='3' initialization='init.mp4' media='$Number$-$Time%05d$.m4s'><SegmentTimeline><S t='0' d='2000' r='-1'/><S t='6000' d='2000' r='1'/></SegmentTimeline></SegmentTemplate></Representation></AdaptationSet></Period></MPD>";
    var playlist = try parse(a, mpd, .{});
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 3), playlist.segments.len);
    try std.testing.expectEqualStrings("5-04000.m4s", playlist.segments[0].uri);
    try std.testing.expectEqualStrings("7-08000.m4s", playlist.segments[2].uri);
    const literal = try std.mem.replaceOwned(u8, a, live_fixture, "seg-$Time$.m4s", "seg-$$Time$$.m4s");
    defer a.free(literal);
    try std.testing.expectError(error.UnsupportedDash, parseAt(a, literal, .{}, 0));
    const bad_width = try std.mem.replaceOwned(u8, a, live_fixture, "seg-$Time$.m4s", "seg-$Time%099d$.m4s");
    defer a.free(bad_width);
    try std.testing.expectError(error.ResourceLimitExceeded, parseAt(a, bad_width, .{}, 0));
    const huge_scale = try std.mem.replaceOwned(u8, a, mpd, "timescale='1000'", "timescale='18446744073709551615'");
    defer a.free(huge_scale);
    try std.testing.expectError(error.ResourceLimitExceeded, parse(a, huge_scale, .{}));
    const overflow = try std.mem.replaceOwned(u8, a, mpd, "r='1'", "r='18446744073709551615'");
    defer a.free(overflow);
    try std.testing.expectError(error.MalformedDash, parse(a, overflow, .{}));
}
