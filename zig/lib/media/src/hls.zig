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

//! Bounded HLS media playlists, including published partial fMP4 units. Strings borrow input.
const std = @import("std");
pub const master = @import("hls_master.zig");
pub const crypto = @import("hls_crypto.zig");
pub const Range = struct { offset: u64, length: u64 };
pub const Initialization = struct { uri: []const u8, range: ?Range, encryption: ?crypto.Key = null };
/// DASH period identity survives live-window removal and XML reordering.
pub const DashPeriod = struct {
    identity: [32]u8 = @splat(0),
    configuration: [32]u8 = @splat(0),
    start_ms: u64 = 0,
    duration_ms: ?u64 = null,
    next_start_ms: ?u64 = null,
    next_identity: ?[32]u8 = null,
};
pub const DashTiming = struct {
    period: DashPeriod = .{},
    start_ticks: u64,
    duration_ticks: u64,
    timescale: u32,
    presentation_offset: u64,
    /// Explicit static lists can finish without an MPD period duration.
    final: bool = false,
    pub const Time = struct { numerator: i128, denominator: u32 };
    /// Exact presentation milliseconds, avoiding float clock comparisons.
    pub fn presentationStart(self: DashTiming) Time {
        return .{ .numerator = @as(i128, self.period.start_ms) * self.timescale + (@as(i128, self.start_ticks) - self.presentation_offset) * 1000, .denominator = self.timescale };
    }
    pub fn presentationEnd(self: DashTiming) Time {
        const start = self.presentationStart();
        return .{ .numerator = start.numerator + @as(i128, self.duration_ticks) * 1000, .denominator = start.denominator };
    }
    pub fn finishesPeriod(self: DashTiming) bool {
        if (self.final) return true;
        const duration = self.period.duration_ms orelse return false;
        const end = (@as(i128, self.start_ticks) + self.duration_ticks - self.presentation_offset) * 1000;
        return end >= @as(i128, duration) * self.timescale;
    }
    pub fn startsPeriod(self: DashTiming) bool {
        return self.start_ticks <= self.presentation_offset and @as(u128, self.start_ticks) + self.duration_ticks > self.presentation_offset;
    }
};
pub const ServerControl = struct { can_block_reload: bool = false, can_skip_until: ?f64 = null, can_skip_dateranges: bool = false, hold_back: ?f64 = null, part_hold_back: ?f64 = null };
pub const PreloadHint = struct { uri: []const u8, kind: enum { part, map }, offset: u64 = 0, length: ?u64 = null };
pub const Part = struct { index: u32, independent: bool = false, gap: bool = false };
pub const Segment = struct {
    uri: []const u8,
    range: ?Range,
    initialization: Initialization,
    sequence: u64,
    discontinuity_sequence: u64,
    duration_seconds: f64,
    discontinuity: bool,
    part: ?Part = null,
    dash: ?DashTiming = null,
    encryption: ?crypto.Key = null,
};
pub const Limits = struct {
    max_bytes: usize = 1024 * 1024,
    max_segments: usize = 4096,
    max_variants: usize = 1024,
    max_renditions: usize = 4096,
    max_uri_bytes: usize = 4096,
    max_periods: usize = 4096,
    max_timeline_entries: usize = 4096,
    max_planning_bytes: usize = 16 * 1024 * 1024,
};
pub const Playlist = struct {
    allocator: std.mem.Allocator,
    segments: []Segment,
    target_duration_seconds: u64,
    ended: bool,
    part_target_seconds: ?f64 = null,
    server_control: ServerControl = .{},
    preload_hint: ?PreloadHint = null,
    timing_identity: ?[32]u8 = null,
    reload_after_seconds: ?f64 = null,
    dash_periods: []const DashPeriod = &.{},
    dash_window_start_ms: ?i128 = null,
    /// Playlist-owned manifest references. HLS references borrow its input;
    /// DASH copies both explicit and expanded references into this owner.
    owned_uris: []const []u8 = &.{},
    pub fn deinit(self: *Playlist) void {
        for (self.owned_uris) |owned| self.allocator.free(owned);
        self.allocator.free(self.owned_uris);
        self.allocator.free(self.segments);
        self.allocator.free(self.dash_periods);
        self.* = undefined;
    }
};

fn number(text: []const u8) !u64 {
    if (text.len == 0) return error.MalformedHls;
    for (text) |c| if (c < '0' or c > '9') return error.MalformedHls;
    return std.fmt.parseInt(u64, text, 10) catch error.MalformedHls;
}
fn uri(value: []const u8, limits: Limits) !void {
    if (value.len == 0) return error.MalformedHls;
    if (value.len > limits.max_uri_bytes) return error.ResourceLimitExceeded;
    for (value) |c| if (c <= 0x20 or c == 0x7f or c == '"') return error.MalformedHls;
}
fn positiveDecimal(text: []const u8) !f64 {
    var dots: usize = 0;
    var digits: usize = 0;
    for (text) |c| {
        if (c == '.') dots += 1 else if (c >= '0' and c <= '9') digits += 1 else return error.MalformedHls;
    }
    if (dots > 1 or digits == 0) return error.MalformedHls;
    const value = std.fmt.parseFloat(f64, text) catch return error.MalformedHls;
    if (!std.math.isFinite(value) or value <= 0) return error.MalformedHls;
    return value;
}
fn attributeDecimal(list: *const @import("hls_attributes.zig").List, name: []const u8) !?f64 {
    const value = list.get(name) orelse return null;
    if (value.quoted) return error.MalformedHls;
    return try positiveDecimal(value.value);
}
const PendingRange = struct { length: u64, offset: ?u64 };
fn byteRange(value: []const u8) !PendingRange {
    var parts = std.mem.splitScalar(u8, value, '@');
    const length = try number(parts.next().?);
    if (length == 0) return error.MalformedHls;
    const offset = if (parts.next()) |part| try number(part) else null;
    if (parts.next() != null) return error.MalformedHls;
    return .{ .length = length, .offset = offset };
}
fn map(value: []const u8, limits: Limits) !Initialization {
    var cursor: usize = 0;
    var result_uri: ?[]const u8 = null;
    var result_range: ?Range = null;
    while (cursor < value.len) {
        const equal = std.mem.indexOfScalarPos(u8, value, cursor, '=') orelse return error.MalformedHls;
        const key = value[cursor..equal];
        cursor = equal + 1;
        if (cursor >= value.len or value[cursor] != '"') return error.MalformedHls;
        cursor += 1;
        const end = std.mem.indexOfScalarPos(u8, value, cursor, '"') orelse return error.MalformedHls;
        const item = value[cursor..end];
        if (std.mem.eql(u8, key, "URI")) {
            if (result_uri != null) return error.MalformedHls;
            try uri(item, limits);
            result_uri = item;
        } else if (std.mem.eql(u8, key, "BYTERANGE")) {
            if (result_range != null) return error.MalformedHls;
            const parsed = try byteRange(item);
            // MAP ranges do not inherit a preceding media segment's offset.
            // Require an explicit initialization range in this subset.
            result_range = .{ .offset = parsed.offset orelse return error.UnsupportedHls, .length = parsed.length };
            _ = std.math.add(u64, result_range.?.offset, result_range.?.length) catch return error.MalformedHls;
        } else return error.UnsupportedHls;
        cursor = end + 1;
        if (cursor < value.len) {
            if (value[cursor] != ',') return error.MalformedHls;
            cursor += 1;
            if (cursor == value.len) return error.MalformedHls;
        }
    }
    return .{ .uri = result_uri orelse return error.MalformedHls, .range = result_range };
}

/// fMP4 media playlists and published partial units, including AES-128 identity keys.
/// Master and I-frame playlists use separate APIs or fail explicitly.
pub fn parse(a: std.mem.Allocator, bytes: []const u8, limits: Limits) !Playlist {
    if (bytes.len > limits.max_bytes) return error.ResourceLimitExceeded;
    if (!std.unicode.utf8ValidateSlice(bytes)) return error.MalformedHls;
    if (master.isMaster(bytes)) return error.UnsupportedHls;
    for (bytes) |c| if ((c < 0x20 and c != '\n' and c != '\r') or c == 0x7f) return error.MalformedHls;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    if (!std.mem.eql(u8, std.mem.trimEnd(u8, lines.next().?, "\r"), "#EXTM3U")) return error.MalformedHls;
    var segments: std.ArrayList(Segment) = .empty;
    errdefer segments.deinit(a);
    var target: ?u64 = null;
    var initialization: ?Initialization = null;
    var duration: ?f64 = null;
    var pending_range: ?PendingRange = null;
    var part_target: ?f64 = null;
    var server_control: ?ServerControl = null;
    var preload_hint: ?PreloadHint = null;
    var part_index: u32 = 0;
    var previous_full: ?Segment = null;
    var sequence: u64 = 0;
    var discontinuity_sequence: u64 = 0;
    var has_sequence = false;
    var has_discontinuity_sequence = false;
    var discontinuity = false;
    var ended = false;
    var version: ?u64 = null;
    var encryption: ?crypto.Key = null;
    var min_version: u64 = 1;
    var has_identity_key = false;
    var unsupported_only = false;
    while (lines.next()) |raw| {
        const line = std.mem.trimEnd(u8, raw, "\r");
        if (line.len == 0) continue;
        if (ended or std.mem.eql(u8, line, "#EXTM3U")) return error.MalformedHls;
        if (std.mem.startsWith(u8, line, "#EXTINF:")) {
            if (duration != null) return error.MalformedHls;
            const comma = std.mem.indexOfScalar(u8, line, ',') orelse return error.MalformedHls;
            const decimal = line[8..comma];
            var dots: usize = 0;
            var digits: usize = 0;
            for (decimal) |c| {
                if (c == '.') dots += 1 else if (c >= '0' and c <= '9') digits += 1 else return error.MalformedHls;
            }
            if (dots > 1 or digits == 0) return error.MalformedHls;
            duration = std.fmt.parseFloat(f64, decimal) catch return error.MalformedHls;
            if (!std.math.isFinite(duration.?) or duration.? <= 0) return error.MalformedHls;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-PART-INF:")) {
            if (part_target != null or segments.items.len != 0) return error.MalformedHls;
            const attrs = try @import("hls_attributes.zig").parse(line[16..]);
            part_target = (try attributeDecimal(&attrs, "PART-TARGET")) orelse return error.MalformedHls;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-SERVER-CONTROL:")) {
            if (server_control != null) return error.MalformedHls;
            const attrs = try @import("hls_attributes.zig").parse(line[22..]);
            server_control = .{ .can_block_reload = (try attrs.yesNo("CAN-BLOCK-RELOAD")) orelse false, .can_skip_until = try attributeDecimal(&attrs, "CAN-SKIP-UNTIL"), .can_skip_dateranges = (try attrs.yesNo("CAN-SKIP-DATERANGES")) orelse false, .hold_back = try attributeDecimal(&attrs, "HOLD-BACK"), .part_hold_back = try attributeDecimal(&attrs, "PART-HOLD-BACK") };
        } else if (std.mem.startsWith(u8, line, "#EXT-X-PRELOAD-HINT:")) {
            if (preload_hint != null) return error.MalformedHls;
            const attrs = try @import("hls_attributes.zig").parse(line[20..]);
            const reference = (try attrs.string("URI")) orelse return error.MalformedHls;
            try uri(reference, limits);
            const kind = attrs.get("TYPE") orelse return error.MalformedHls;
            if (kind.quoted) return error.MalformedHls;
            const hint_kind: @FieldType(PreloadHint, "kind") = if (std.mem.eql(u8, kind.value, "PART")) .part else if (std.mem.eql(u8, kind.value, "MAP")) .map else return error.UnsupportedHls;
            const offset = (try attrs.integer("BYTERANGE-START")) orelse 0;
            const length = try attrs.integer("BYTERANGE-LENGTH");
            if (length) |n| {
                if (n == 0) return error.MalformedHls;
                _ = std.math.add(u64, offset, n) catch return error.MalformedHls;
            }
            preload_hint = .{ .uri = reference, .kind = hint_kind, .offset = offset, .length = length };
        } else if (std.mem.startsWith(u8, line, "#EXT-X-PART:")) {
            if (unsupported_only) return error.UnsupportedEncryptedMedia;
            if (duration != null or pending_range != null) return error.MalformedHls;
            if (segments.items.len >= limits.max_segments) return error.ResourceLimitExceeded;
            const attrs = try @import("hls_attributes.zig").parse(line[12..]);
            const reference = (try attrs.string("URI")) orelse return error.MalformedHls;
            try uri(reference, limits);
            const seconds = (try attributeDecimal(&attrs, "DURATION")) orelse return error.MalformedHls;
            if (seconds > (part_target orelse return error.MalformedHls)) return error.MalformedHls;
            var range: ?Range = null;
            if (try attrs.string("BYTERANGE")) |text| {
                const pending = try byteRange(text);
                const offset = pending.offset orelse blk: {
                    if (part_index == 0) return error.MalformedHls;
                    const previous = segments.items[segments.items.len - 1];
                    if (previous.sequence != sequence or previous.part == null or previous.range == null) return error.MalformedHls;
                    break :blk std.math.add(u64, previous.range.?.offset, previous.range.?.length) catch return error.MalformedHls;
                };
                _ = std.math.add(u64, offset, pending.length) catch return error.MalformedHls;
                range = .{ .offset = offset, .length = pending.length };
            }
            try segments.append(a, .{ .uri = reference, .range = range, .initialization = initialization orelse .{ .uri = &.{}, .range = null }, .sequence = sequence, .discontinuity_sequence = discontinuity_sequence, .duration_seconds = seconds, .discontinuity = discontinuity and part_index == 0, .encryption = encryption, .part = .{ .index = part_index, .independent = (try attrs.yesNo("INDEPENDENT")) orelse false, .gap = (try attrs.yesNo("GAP")) orelse false } });
            part_index = std.math.add(u32, part_index, 1) catch return error.ResourceLimitExceeded;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-MAP:")) {
            if (part_index != 0) return error.MalformedHls;
            initialization = try map(line[11..], limits);
            if (unsupported_only) return error.UnsupportedEncryptedMedia;
            if (encryption) |key| if (key.iv == null) return error.MalformedHls;
            initialization.?.encryption = encryption;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-BYTERANGE:")) {
            if (pending_range != null) return error.MalformedHls;
            pending_range = try byteRange(line[17..]);
        } else if (std.mem.startsWith(u8, line, "#EXT-X-TARGETDURATION:")) {
            if (target != null or segments.items.len != 0) return error.MalformedHls;
            target = try number(line[22..]);
            if (target.? == 0) return error.MalformedHls;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-MEDIA-SEQUENCE:")) {
            if (has_sequence or segments.items.len != 0) return error.MalformedHls;
            has_sequence = true;
            sequence = try number(line[22..]);
        } else if (std.mem.startsWith(u8, line, "#EXT-X-DISCONTINUITY-SEQUENCE:")) {
            if (has_discontinuity_sequence or segments.items.len != 0 or discontinuity) return error.MalformedHls;
            has_discontinuity_sequence = true;
            discontinuity_sequence = try number(line[30..]);
        } else if (std.mem.eql(u8, line, "#EXT-X-DISCONTINUITY")) {
            if (part_index != 0) return error.MalformedHls;
            discontinuity_sequence = std.math.add(u64, discontinuity_sequence, 1) catch return error.MalformedHls;
            discontinuity = true;
        } else if (std.mem.eql(u8, line, "#EXT-X-ENDLIST")) {
            if (duration != null or pending_range != null) return error.MalformedHls;
            ended = true;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-VERSION:")) {
            if (version != null) return error.MalformedHls;
            version = try number(line[15..]);
            if (version.? == 0 or version.? > 13) return error.UnsupportedHls;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-KEY:")) {
            if (part_index != 0) return error.MalformedHls;
            const declaration = try crypto.parseKey(line[11..], limits.max_uri_bytes);
            if (declaration.supported) {
                encryption = declaration.key;
                has_identity_key = true;
                unsupported_only = false;
            } else if (!has_identity_key) unsupported_only = true;
            min_version = @max(min_version, declaration.min_version);
        } else if (std.mem.startsWith(u8, line, "#EXT-X-STREAM-INF:") or std.mem.startsWith(u8, line, "#EXT-X-MEDIA:") or std.mem.startsWith(u8, line, "#EXT-X-I-FRAME") or std.mem.startsWith(u8, line, "#EXT-X-SKIP:") or std.mem.startsWith(u8, line, "#EXT-X-DEFINE:") or std.mem.startsWith(u8, line, "#EXT-X-SESSION-KEY:")) {
            return error.UnsupportedHls;
        } else if (line[0] != '#') {
            if (unsupported_only) return error.UnsupportedEncryptedMedia;
            try uri(line, limits);
            if (segments.items.len >= limits.max_segments) return error.ResourceLimitExceeded;
            const seconds = duration orelse return error.MalformedHls;
            const maximum = target orelse return error.MalformedHls;
            if (@round(seconds) > @as(f64, @floatFromInt(maximum))) return error.MalformedHls;
            var range: ?Range = null;
            if (pending_range) |pending| {
                const offset = pending.offset orelse blk: {
                    const previous = previous_full orelse return error.MalformedHls;
                    if (!std.mem.eql(u8, previous.uri, line) or previous.range == null) return error.MalformedHls;
                    break :blk std.math.add(u64, previous.range.?.offset, previous.range.?.length) catch return error.MalformedHls;
                };
                _ = std.math.add(u64, offset, pending.length) catch return error.MalformedHls;
                range = .{ .offset = offset, .length = pending.length };
            }
            try segments.append(a, .{ .uri = line, .range = range, .initialization = initialization orelse .{ .uri = &.{}, .range = null }, .sequence = sequence, .discontinuity_sequence = discontinuity_sequence, .duration_seconds = seconds, .discontinuity = discontinuity, .encryption = encryption });
            previous_full = segments.items[segments.items.len - 1];
            part_index = 0;
            sequence = std.math.add(u64, sequence, 1) catch return error.MalformedHls;
            duration = null;
            pending_range = null;
            discontinuity = false;
        }
    }
    if (duration != null or pending_range != null or discontinuity) return error.MalformedHls;
    const target_seconds = target orelse return error.MalformedHls;
    if ((version orelse 1) < min_version or (initialization != null and (version orelse 1) < 6)) return error.UnsupportedHls;
    const control = server_control orelse ServerControl{};
    if (control.can_skip_dateranges and control.can_skip_until == null) return error.MalformedHls;
    if (control.can_skip_until) |seconds| if (seconds < 6 * @as(f64, @floatFromInt(target_seconds))) return error.MalformedHls;
    if (control.hold_back) |seconds| if (seconds < 3 * @as(f64, @floatFromInt(target_seconds))) return error.MalformedHls;
    if (control.part_hold_back) |seconds| {
        if (seconds < 2 * (part_target orelse return error.MalformedHls)) return error.MalformedHls;
    }
    if (server_control != null and part_target != null and control.part_hold_back == null) return error.MalformedHls;
    if (ended and preload_hint != null) return error.MalformedHls;
    if (part_target) |seconds| for (segments.items, 0..) |segment, index| {
        const part = segment.part orelse continue;
        if (part.independent or part.gap or segment.duration_seconds >= 0.85 * seconds or index + 1 == segments.items.len) continue;
        const following = segments.items[index + 1];
        if (following.part) |next| if (following.sequence == segment.sequence and !next.gap) return error.MalformedHls;
    };
    return .{ .allocator = a, .segments = try segments.toOwnedSlice(a), .target_duration_seconds = target_seconds, .ended = ended, .part_target_seconds = part_target, .server_control = control, .preload_hint = preload_hint };
}

const fixture = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:41\n#EXT-X-DISCONTINUITY-SEQUENCE:5\n#EXT-X-MAP:URI=\"init,1.mp4\",BYTERANGE=\"100@0\"\n#EXTINF:1.5,first\n#EXT-X-BYTERANGE:200@100\nmedia.mp4\n#EXTINF:2,second\n#EXT-X-BYTERANGE:300\nmedia.mp4\n#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"init2.mp4\"\n#EXTINF:1,last\nnext.mp4\n#EXT-X-ENDLIST\n";
test "HLS fMP4 preserves sequence ranges initialization changes and discontinuities" {
    var playlist = try parse(std.testing.allocator, fixture, .{});
    defer playlist.deinit();
    try std.testing.expect(playlist.ended);
    try std.testing.expectEqual(@as(usize, 3), playlist.segments.len);
    try std.testing.expectEqual(@as(u64, 42), playlist.segments[1].sequence);
    try std.testing.expectEqual(@as(u64, 300), playlist.segments[1].range.?.offset);
    try std.testing.expectEqual(@as(u64, 6), playlist.segments[2].discontinuity_sequence);
    try std.testing.expect(playlist.segments[2].discontinuity);
    try std.testing.expectEqualStrings("init,1.mp4", playlist.segments[0].initialization.uri);
    try std.testing.expectEqualStrings("init2.mp4", playlist.segments[2].initialization.uri);
    try std.testing.expectError(error.ResourceLimitExceeded, parse(std.testing.allocator, fixture, .{ .max_segments = 2 }));
}
fn allocations(a: std.mem.Allocator) !void {
    var playlist = try parse(a, fixture, .{});
    defer playlist.deinit();
}
test "HLS fMP4 allocation failures release parsed segment storage" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocations, .{});
}
test "HLS fMP4 rejects incomplete encrypted unsupported and overflowing playlists" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.MalformedHls, parse(a, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:1,\n", .{}));
    try std.testing.expectError(error.UnsupportedEncryptedMedia, parse(a, "#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key\"\n", .{}));
    try std.testing.expectError(error.UnsupportedHls, parse(a, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\nvariant.m3u8\n", .{}));
    try std.testing.expectError(error.MalformedHls, parse(a, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:1\na.mp4\n", .{}));
    try std.testing.expectError(error.MalformedHls, parse(a, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:2@18446744073709551615\na.mp4\n", .{}));
    try std.testing.expectError(error.UnsupportedHls, parse(a, "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"100\"\n", .{}));
    for ([_][]const u8{ "1e0", "+1", "-1", "nan", "1.0.1" }) |value| {
        const manifest = try std.fmt.allocPrint(a, "#EXTM3U\n#EXTINF:{s},\n", .{value});
        defer a.free(manifest);
        try std.testing.expectError(error.MalformedHls, parse(a, manifest, .{}));
    }
    try std.testing.expectError(error.MalformedHls, parse(a, "#EXTM3U\n#EXTM3U\n", .{}));
}

test "HLS encrypted maps freeze their declaration while media key rotates or becomes clear" {
    const text = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\",IV=0x1\n#EXT-X-MAP:URI=\"init\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\"\n#EXTINF:2,\na\n#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n#EXTINF:2,\nb\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:2,\nc\n";
    var playlist = try parse(std.testing.allocator, text, .{});
    defer playlist.deinit();
    try std.testing.expectEqual(@as(usize, 3), playlist.segments.len);
    for (playlist.segments) |segment| {
        try std.testing.expectEqualStrings("key1", segment.initialization.encryption.?.uri);
        try std.testing.expectEqual(@as(u8, 1), segment.initialization.encryption.?.iv.?[15]);
    }
    try std.testing.expectEqualStrings("key1", playlist.segments[0].encryption.?.uri);
    try std.testing.expect(playlist.segments[0].encryption.?.iv == null);
    try std.testing.expectEqualStrings("key2", playlist.segments[1].encryption.?.uri);
    try std.testing.expect(playlist.segments[2].encryption == null);
    try std.testing.expectError(error.MalformedHls, parse(std.testing.allocator, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\"\n#EXT-X-MAP:URI=\"init\"\n", .{}));
}

test "HLS identity keys coexist with ignored DRM formats and unsupported-only media fails" {
    const prefix = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n";
    const identity = "#EXT-X-KEY:METHOD=AES-128,URI=\"key\",IV=0x1\n";
    const drm = "#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"drm\",KEYFORMAT=\"example.drm\"\n";
    const suffix = "#EXT-X-MAP:URI=\"init\"\n#EXTINF:1,\nmedia\n";
    inline for (.{ prefix ++ drm ++ identity ++ suffix, prefix ++ identity ++ drm ++ suffix }) |text| {
        var playlist = try parse(std.testing.allocator, text, .{});
        defer playlist.deinit();
        try std.testing.expectEqualStrings("key", playlist.segments[0].encryption.?.uri);
        try std.testing.expectEqualStrings("key", playlist.segments[0].initialization.encryption.?.uri);
    }
    try std.testing.expectError(error.UnsupportedEncryptedMedia, parse(std.testing.allocator, prefix ++ drm ++ suffix, .{}));
}
fn aesFuzzSeed(comptime text: []const u8) []const u8 {
    return comptime blk: {
        var encoded: [4 + text.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], text.len, .little);
        @memcpy(encoded[4..], text);
        const frozen = encoded;
        break :blk &frozen;
    };
}
const aes_fuzz_seeds = .{
    aesFuzzSeed("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\",IV=0x1\n#EXT-X-MAP:URI=\"init\"\n#EXTINF:1,\nmedia\n"),
    aesFuzzSeed("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"init\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\",KEYFORMAT=\"identity\",KEYFORMATVERSIONS=\"1/2\"\n#EXT-X-MEDIA-SEQUENCE:10\n#EXTINF:1,\nmedia\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:1,\nclear\n"),
};
test "HLS AES fuzz media key declarations and map scope" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [16 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0x484c5341);
            var playlist = parse(std.testing.allocator, bytes[0..length], .{ .max_bytes = bytes.len, .max_segments = 16, .max_uri_bytes = 512 }) catch return;
            defer playlist.deinit();
            for (playlist.segments) |segment| if (segment.initialization.encryption) |key| try std.testing.expect(key.iv != null);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &aes_fuzz_seeds });
}
test "HLS AES fuzz media seeds retain positive encrypted segment declarations through Smith" {
    inline for (aes_fuzz_seeds) |seed| {
        var smith = std.testing.Smith{ .in = seed };
        var bytes: [16 * 1024]u8 = undefined;
        const length = smith.sliceWithHash(&bytes, 0x484c5341);
        try std.testing.expectEqual(seed.len - 4, length);
        var playlist = try parse(std.testing.allocator, bytes[0..length], .{});
        defer playlist.deinit();
        try std.testing.expect(playlist.segments.len > 0 and playlist.segments[0].encryption != null);
    }
}

const parts_fixture = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXT-X-PART:DURATION=1,URI=\"part.mp4\",BYTERANGE=\"10@5\",INDEPENDENT=YES\n#EXT-X-PART:DURATION=1,URI=\"part.mp4\",BYTERANGE=\"20\"\n#EXTINF:2,\nparent.mp4\n#EXT-X-PART:DURATION=0.5,URI=\"next.mp4\",GAP=YES\n";
test "HLS partial records retain parent sequence identities ranges and flags" {
    var parsed = try parse(std.testing.allocator, parts_fixture, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 4), parsed.segments.len);
    try std.testing.expectEqual(@as(f64, 1), parsed.part_target_seconds.?);
    try std.testing.expectEqual(@as(u64, 10), parsed.segments[1].sequence);
    try std.testing.expectEqual(@as(u32, 1), parsed.segments[1].part.?.index);
    try std.testing.expectEqual(@as(u64, 15), parsed.segments[1].range.?.offset);
    try std.testing.expect(parsed.segments[0].part.?.independent);
    try std.testing.expect(parsed.segments[2].part == null);
    try std.testing.expectEqual(@as(u64, 11), parsed.segments[3].sequence);
    try std.testing.expectEqual(@as(u32, 0), parsed.segments[3].part.?.index);
    try std.testing.expect(parsed.segments[3].part.?.gap);
}
test "HLS partial records reject missing durations targets ranges and mid-parent config mutation" {
    const prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n";
    inline for (.{ "#EXT-X-PART:DURATION=1,URI=\"p\"\n", "#EXT-X-PART-INF:PART-TARGET=0\n", "#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-PART:DURATION=2,URI=\"p\"\n", "#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-PART:DURATION=NaN,URI=\"p\"\n", "#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-PART:DURATION=1,URI=\"p\",BYTERANGE=\"10\"\n", "#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-PART:DURATION=1,URI=\"p\"\n#EXT-X-DISCONTINUITY\n" }) |suffix| try std.testing.expectError(error.MalformedHls, parse(std.testing.allocator, prefix ++ suffix, .{}));
}

fn partialFuzzSeed(comptime text: []const u8) []const u8 {
    return comptime blk: {
        var data: [4 + text.len]u8 = undefined;
        std.mem.writeInt(u32, data[0..4], text.len, .little);
        @memcpy(data[4..], text);
        const frozen = data;
        break :blk &frozen;
    };
}
const partial_fuzz_seeds = .{ partialFuzzSeed(parts_fixture), partialFuzzSeed("#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=2\n#EXT-X-MAP:URI=\"init\"\n#EXT-X-PART:DURATION=1,URI=\"part\",INDEPENDENT=YES\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"next\",BYTERANGE-START=10,BYTERANGE-LENGTH=20\n") };
test "HLS partial fuzz bounded attributes sequence ranges controls and hints" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [4096]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0x484c5302);
            var parsed = parse(std.testing.allocator, bytes[0..length], .{ .max_bytes = bytes.len, .max_segments = 64, .max_uri_bytes = 256 }) catch return;
            defer parsed.deinit();
            for (parsed.segments, 0..) |segment, index| if (index != 0) {
                const previous = parsed.segments[index - 1];
                try std.testing.expect(segment.sequence >= previous.sequence);
                if (segment.part) |part| if (previous.part) |old| if (segment.sequence == previous.sequence) try std.testing.expectEqual(old.index + 1, part.index);
            };
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &partial_fuzz_seeds });
}
test "HLS partial fuzz serialized seeds are positively accepted" {
    inline for (partial_fuzz_seeds) |seed| {
        var smith = std.testing.Smith{ .in = seed };
        var bytes: [4096]u8 = undefined;
        const length = smith.sliceWithHash(&bytes, 0x484c5302);
        try std.testing.expectEqual(seed.len - 4, length);
        var parsed = try parse(std.testing.allocator, bytes[0..length], .{});
        defer parsed.deinit();
        try std.testing.expect(parsed.segments.len > 0 and parsed.segments[0].part != null);
    }
}
