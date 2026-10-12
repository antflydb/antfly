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

//! RFC 8216 master discovery and explicit video selection. No hidden ABR.
const std = @import("std");
const hls = @import("hls.zig");
const attributes = @import("hls_attributes.zig");
const source = @import("source.zig");
const admission = @import("admission.zig");
pub const Resolution = struct { width: u32, height: u32 };
pub const Variant = struct {
    uri: []const u8 = &.{},
    bandwidth: u64,
    average_bandwidth: ?u64 = null,
    codecs: ?[]const u8 = null,
    resolution: ?Resolution = null,
    frame_rate: ?f64 = null,
    audio: ?[]const u8 = null,
    video: ?[]const u8 = null,
    subtitles: ?[]const u8 = null,
    closed_captions: ?[]const u8 = null,
    no_closed_captions: bool = false,
};
pub const Kind = enum { audio, video, subtitles, closed_captions };
pub const Rendition = struct {
    kind: Kind,
    group_id: []const u8,
    name: []const u8,
    uri: ?[]const u8 = null,
    language: ?[]const u8 = null,
    associated_language: ?[]const u8 = null,
    characteristics: ?[]const u8 = null,
    channels: ?[]const u8 = null,
    instream_id: ?[]const u8 = null,
    default: bool = false,
    autoselect: bool = false,
    forced: bool = false,
};
pub const Selection = struct {
    /// Exact manifest URI selector; filters still apply. Strings borrow caller.
    variant_uri: ?[]const u8 = null,
    video_rendition_name: ?[]const u8 = null,
    max_bandwidth: ?u64 = null,
    max_width: ?u32 = null,
    max_height: ?u32 = null,
    max_frame_rate: ?f64 = null,
    /// Require at least one matching declared codec (e.g. avc1 or avc3).
    /// Audio codec entries do not prevent a matching video codec selection.
    codec_prefixes: ?[]const []const u8 = null,
};
pub const Selected = struct { variant: Variant, uri: []const u8, video: ?Rendition };
pub const Master = struct {
    allocator: std.mem.Allocator,
    variants: []Variant,
    renditions: []Rendition,
    reservation: admission.Token = .{},
    pub fn deinit(self: *Master) void {
        self.allocator.free(self.variants);
        self.allocator.free(self.renditions);
        self.reservation.deinit();
        self.* = undefined;
    }
    /// Highest peak bandwidth within explicit constraints, with lexical media
    /// URI tie-breaking. DEFAULT video wins unless the caller names a rendition.
    pub fn select(self: Master, selection: Selection) !Selected {
        return self.selectControlled(selection, .{});
    }
    pub fn selectControlled(self: Master, selection: Selection, control: source.Control) !Selected {
        try control.check();
        if (selection.max_frame_rate) |rate| if (!std.math.isFinite(rate) or rate <= 0) return error.InvalidHlsSelection;
        var best: ?Selected = null;
        for (self.variants) |variant| {
            try control.check();
            if (selection.variant_uri) |wanted| if (!std.mem.eql(u8, wanted, variant.uri)) continue;
            if (selection.max_bandwidth) |maximum| if (variant.bandwidth > maximum) continue;
            if (selection.max_width) |maximum| if ((variant.resolution orelse continue).width > maximum) continue;
            if (selection.max_height) |maximum| if ((variant.resolution orelse continue).height > maximum) continue;
            if (selection.max_frame_rate) |maximum| if ((variant.frame_rate orelse continue) > maximum) continue;
            if (selection.codec_prefixes) |prefixes| {
                const codecs = variant.codecs orelse continue;
                var matched = false;
                var list = std.mem.splitScalar(u8, codecs, ',');
                while (list.next()) |raw| {
                    try control.check();
                    const codec = std.mem.trim(u8, raw, " ");
                    for (prefixes) |prefix| {
                        if (prefix.len != 0 and (std.mem.eql(u8, codec, prefix) or (std.mem.startsWith(u8, codec, prefix) and codec.len > prefix.len and codec[prefix.len] == '.'))) matched = true;
                    }
                }
                if (!matched) continue;
            }
            var video: ?Rendition = null;
            if (variant.video) |group| {
                for (self.renditions) |item| {
                    try control.check();
                    if (item.kind != .video or !std.mem.eql(u8, group, item.group_id)) continue;
                    if (selection.video_rendition_name) |name| {
                        if (std.mem.eql(u8, name, item.name)) video = item;
                    } else if (item.default) video = item;
                }
            }
            if (selection.video_rendition_name != null and video == null) continue;
            const selected = Selected{ .variant = variant, .uri = if (video) |item| item.uri orelse variant.uri else variant.uri, .video = video };
            if (best == null or variant.bandwidth > best.?.variant.bandwidth or
                (variant.bandwidth == best.?.variant.bandwidth and selectedLessThan(selected, best.?))) best = selected;
        }
        return best orelse error.NoCompatibleHlsVariant;
    }
};
fn selectedLessThan(left: Selected, right: Selected) bool {
    const uri_order = std.mem.order(u8, left.uri, right.uri);
    if (uri_order != .eq) return uri_order == .lt;
    const variant_uri_order = std.mem.order(u8, left.variant.uri, right.variant.uri);
    if (variant_uri_order != .eq) return variant_uri_order == .lt;
    inline for (.{ "codecs", "audio", "video", "subtitles", "closed_captions" }) |field| {
        const order = std.mem.order(u8, @field(left.variant, field) orelse "", @field(right.variant, field) orelse "");
        if (order != .eq) return order == .lt;
    }
    const average = std.math.order(left.variant.average_bandwidth orelse 0, right.variant.average_bandwidth orelse 0);
    if (average != .eq) return average == .lt;
    const l = left.variant.resolution orelse Resolution{ .width = 0, .height = 0 };
    const r = right.variant.resolution orelse Resolution{ .width = 0, .height = 0 };
    if (l.width != r.width) return l.width < r.width;
    if (l.height != r.height) return l.height < r.height;
    return (left.variant.frame_rate orelse 0) < (right.variant.frame_rate orelse 0);
}
const master_tags = [_][]const u8{ "#EXT-X-STREAM-INF:", "#EXT-X-MEDIA:", "#EXT-X-I-FRAME-STREAM-INF:", "#EXT-X-SESSION-DATA:", "#EXT-X-SESSION-KEY:" };
pub fn isMaster(bytes: []const u8) bool {
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| for (master_tags) |tag| if (std.mem.startsWith(u8, line, tag)) return true;
    return false;
}
fn validateUri(value: []const u8, limits: hls.Limits) !void {
    if (value.len == 0) return error.MalformedHls;
    if (value.len > limits.max_uri_bytes) return error.ResourceLimitExceeded;
    for (value) |c| if (c <= 0x20 or c == 0x7f or c == '"') return error.MalformedHls;
}
fn variantFrom(text: []const u8) !Variant {
    const list = try attributes.parse(text);
    var result = Variant{ .bandwidth = (try list.integer("BANDWIDTH")) orelse return error.MalformedHls, .average_bandwidth = try list.integer("AVERAGE-BANDWIDTH"), .codecs = try list.string("CODECS"), .audio = try list.string("AUDIO"), .video = try list.string("VIDEO"), .subtitles = try list.string("SUBTITLES") };
    if (result.bandwidth == 0 or (result.average_bandwidth != null and result.average_bandwidth.? == 0)) return error.MalformedHls;
    if (list.get("RESOLUTION")) |item| {
        if (item.quoted) return error.MalformedHls;
        var dimensions = std.mem.splitScalar(u8, item.value, 'x');
        const width = try attributes.uint(dimensions.next().?);
        const height = try attributes.uint(dimensions.next() orelse return error.MalformedHls);
        if (dimensions.next() != null or width == 0 or height == 0) return error.MalformedHls;
        result.resolution = .{ .width = std.math.cast(u32, width) orelse return error.ResourceLimitExceeded, .height = std.math.cast(u32, height) orelse return error.ResourceLimitExceeded };
    }
    if (list.get("FRAME-RATE")) |item| {
        if (item.quoted) return error.MalformedHls;
        result.frame_rate = try attributes.decimal(item.value);
    }
    if (list.get("CLOSED-CAPTIONS")) |item| {
        if (item.quoted) {
            if (item.value.len == 0) return error.MalformedHls;
            result.closed_captions = item.value;
        } else if (std.mem.eql(u8, item.value, "NONE")) result.no_closed_captions = true else return error.MalformedHls;
    }
    return result;
}
fn renditionFrom(text: []const u8, limits: hls.Limits) !Rendition {
    const list = try attributes.parse(text);
    const item = list.get("TYPE") orelse return error.MalformedHls;
    if (item.quoted) return error.MalformedHls;
    const kind: Kind = if (std.mem.eql(u8, item.value, "AUDIO")) .audio else if (std.mem.eql(u8, item.value, "VIDEO")) .video else if (std.mem.eql(u8, item.value, "SUBTITLES")) .subtitles else if (std.mem.eql(u8, item.value, "CLOSED-CAPTIONS")) .closed_captions else return error.MalformedHls;
    const result = Rendition{ .kind = kind, .group_id = (try list.string("GROUP-ID")) orelse return error.MalformedHls, .name = (try list.string("NAME")) orelse return error.MalformedHls, .uri = try list.string("URI"), .language = try list.string("LANGUAGE"), .associated_language = try list.string("ASSOC-LANGUAGE"), .characteristics = try list.string("CHARACTERISTICS"), .channels = try list.string("CHANNELS"), .instream_id = try list.string("INSTREAM-ID"), .default = (try list.yesNo("DEFAULT")) orelse false, .autoselect = (try list.yesNo("AUTOSELECT")) orelse false, .forced = (try list.yesNo("FORCED")) orelse false };
    if (result.uri) |value| try validateUri(value, limits);
    if (result.default and list.get("AUTOSELECT") != null and !result.autoselect) return error.MalformedHls;
    if (list.get("FORCED") != null and kind != .subtitles) return error.MalformedHls;
    if (result.channels != null and kind != .audio) return error.MalformedHls;
    if (kind == .subtitles and result.uri == null) return error.MalformedHls;
    if (kind == .closed_captions) {
        if (result.uri != null) return error.MalformedHls;
        const id = result.instream_id orelse return error.MalformedHls;
        if (std.mem.startsWith(u8, id, "SERVICE")) {
            const number = try attributes.uint(id[7..]);
            if (number == 0 or number > 63) return error.MalformedHls;
        } else if (!std.mem.eql(u8, id, "CC1") and !std.mem.eql(u8, id, "CC2") and !std.mem.eql(u8, id, "CC3") and !std.mem.eql(u8, id, "CC4")) return error.MalformedHls;
    } else if (result.instream_id != null) return error.MalformedHls;
    return result;
}
fn append(comptime T: type, a: std.mem.Allocator, list: *std.ArrayList(T), value: T, maximum: usize, other_capacity: usize, limits: hls.Limits, reservation: *admission.Token) !void {
    if (list.items.len >= maximum) return error.ResourceLimitExceeded;
    if (list.items.len == list.capacity) {
        const capacity = @min(maximum, @max(@as(usize, 8), list.capacity * 2));
        const bytes = std.math.add(usize, std.math.mul(usize, capacity, @sizeOf(T)) catch return error.ResourceLimitExceeded, other_capacity) catch return error.ResourceLimitExceeded;
        if (bytes > limits.max_planning_bytes / 2) return error.ResourceLimitExceeded;
        try reservation.resize(.{ .host_bytes = bytes * 2 });
        try list.ensureTotalCapacityPrecise(a, capacity);
    }
    list.appendAssumeCapacity(value);
}
pub fn parse(a: std.mem.Allocator, bytes: []const u8, limits: hls.Limits) !Master {
    return parseControlled(a, bytes, limits, .{}, null);
}
pub fn parseControlled(a: std.mem.Allocator, bytes: []const u8, limits: hls.Limits, control: source.Control, pool: ?*admission.Pool) !Master {
    if (bytes.len > limits.max_bytes) return error.ResourceLimitExceeded;
    if (!std.unicode.utf8ValidateSlice(bytes)) return error.MalformedHls;
    for (bytes) |c| if ((c < 0x20 and c != '\n' and c != '\r') or c == 0x7f) return error.MalformedHls;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    if (!std.mem.eql(u8, std.mem.trimEnd(u8, lines.next().?, "\r"), "#EXTM3U")) return error.MalformedHls;
    var variants: std.ArrayList(Variant) = .empty;
    defer variants.deinit(a);
    var renditions: std.ArrayList(Rendition) = .empty;
    defer renditions.deinit(a);
    var reservation = if (pool) |p| try p.acquire(.{}) else admission.Token{};
    errdefer reservation.deinit();
    var pending: ?Variant = null;
    var version: ?u64 = null;
    var min_version: u64 = 1;
    while (lines.next()) |raw| {
        try control.check();
        const line = std.mem.trimEnd(u8, raw, "\r");
        if (line.len == 0) continue;
        if (std.mem.eql(u8, line, "#EXTM3U") or (pending != null and std.mem.startsWith(u8, line, "#EXT"))) return error.MalformedHls;
        if (std.mem.startsWith(u8, line, "#EXT-X-STREAM-INF:")) {
            if (pending != null) return error.MalformedHls;
            pending = try variantFrom(line[18..]);
        } else if (std.mem.startsWith(u8, line, "#EXT-X-MEDIA:")) {
            const item = try renditionFrom(line[13..], limits);
            for (renditions.items) |old| {
                try control.check();
                if (old.kind == item.kind and std.mem.eql(u8, old.group_id, item.group_id) and (std.mem.eql(u8, old.name, item.name) or (old.default and item.default))) return error.MalformedHls;
            }
            try append(Rendition, a, &renditions, item, limits.max_renditions, variants.capacity * @sizeOf(Variant), limits, &reservation);
        } else if (std.mem.startsWith(u8, line, "#EXT-X-VERSION:")) {
            if (version != null) return error.MalformedHls;
            version = try attributes.uint(line[15..]);
            if (version.? == 0 or version.? > 13) return error.UnsupportedHls;
        } else if (std.mem.startsWith(u8, line, "#EXT-X-SESSION-KEY:")) {
            const declaration = try hls.crypto.parseKey(line[19..], limits.max_uri_bytes);
            if (declaration.key == null) return error.MalformedHls;
            min_version = @max(min_version, declaration.min_version);
            // Advisory preloading metadata; media EXT-X-KEY remains authoritative.
        } else if (line[0] != '#') {
            var item = pending orelse return error.MalformedHls;
            try validateUri(line, limits);
            item.uri = line;
            try append(Variant, a, &variants, item, limits.max_variants, renditions.capacity * @sizeOf(Rendition), limits, &reservation);
            pending = null;
        } else {
            inline for (.{ "#EXTINF:", "#EXT-X-TARGETDURATION:", "#EXT-X-MEDIA-SEQUENCE:", "#EXT-X-DISCONTINUITY-SEQUENCE:", "#EXT-X-DISCONTINUITY", "#EXT-X-ENDLIST", "#EXT-X-PLAYLIST-TYPE:", "#EXT-X-I-FRAMES-ONLY", "#EXT-X-MAP:", "#EXT-X-BYTERANGE:", "#EXT-X-KEY:", "#EXT-X-PROGRAM-DATE-TIME:", "#EXT-X-DATERANGE:", "#EXT-X-GAP" }) |tag| if (std.mem.startsWith(u8, line, tag)) return error.MalformedHls;
            inline for (.{ "#EXT-X-DEFINE:", "#EXT-X-PART", "#EXT-X-PRELOAD-HINT:", "#EXT-X-SKIP:" }) |tag| if (std.mem.startsWith(u8, line, tag)) return error.UnsupportedHls;
        }
    }
    if (pending != null or variants.items.len == 0) return error.MalformedHls;
    if ((version orelse 1) < min_version) return error.UnsupportedHls;
    for (variants.items) |variant| {
        inline for (.{ .{ Kind.audio, variant.audio }, .{ Kind.video, variant.video }, .{ Kind.subtitles, variant.subtitles }, .{ Kind.closed_captions, variant.closed_captions } }) |reference| {
            if (reference[1]) |group| {
                var found = false;
                for (renditions.items) |item| {
                    try control.check();
                    if (item.kind == reference[0] and std.mem.eql(u8, group, item.group_id)) found = true;
                }
                if (!found) return error.MalformedHls;
            }
        }
        if (variant.no_closed_captions) for (variants.items) |other| if (!other.no_closed_captions) return error.MalformedHls;
    }
    const owned_variants = try variants.toOwnedSlice(a);
    errdefer a.free(owned_variants);
    const owned_renditions = try renditions.toOwnedSlice(a);
    errdefer a.free(owned_renditions);
    try control.check();
    try reservation.resize(.{ .host_bytes = owned_variants.len * @sizeOf(Variant) + owned_renditions.len * @sizeOf(Rendition) });
    return .{ .allocator = a, .variants = owned_variants, .renditions = owned_renditions, .reservation = reservation };
}

const fixture =
    "#EXTM3U\n#EXT-X-VERSION:7\n" ++
    "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English, commentary\",LANGUAGE=\"en\",URI=\"sound/main.m3u8\",CHANNELS=\"2\"\n" ++
    "#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"camera\",NAME=\"front\",DEFAULT=YES,AUTOSELECT=YES,URI=\"views/front.m3u8\"\n" ++
    "#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"camera\",NAME=\"back\",URI=\"views/back.m3u8\"\n" ++
    "#EXT-X-STREAM-INF:BANDWIDTH=4000000,AVERAGE-BANDWIDTH=3200000,CODECS=\"av01.0.08M.08,mp4a.40.2\",RESOLUTION=3840x2160,FRAME-RATE=60,AUDIO=\"audio\"\n" ++
    "av1/list.m3u8\n" ++
    "#EXT-X-STREAM-INF:BANDWIDTH=2000000,CODECS=\"avc1.64001f,mp4a.40.2\",RESOLUTION=1280x720,FRAME-RATE=30,AUDIO=\"audio\",VIDEO=\"camera\"\n" ++
    "hd/list.m3u8\n" ++
    "#EXT-X-STREAM-INF:BANDWIDTH=700000,CODECS=\"avc3.4d401e,mp4a.40.2\",RESOLUTION=640x360,FRAME-RATE=24,AUDIO=\"audio\"\n" ++
    "sd/list.m3u8\n";
test "HLS master retains variants renditions and selects explicit compatible video" {
    const a = std.testing.allocator;
    var master = try parse(a, fixture, .{});
    defer master.deinit();
    try std.testing.expect(isMaster(fixture));
    try std.testing.expectEqual(@as(usize, 3), master.variants.len);
    try std.testing.expectEqual(@as(usize, 3), master.renditions.len);
    try std.testing.expectEqualStrings("English, commentary", master.renditions[0].name);
    try std.testing.expectEqual(@as(?u64, 3200000), master.variants[0].average_bandwidth);
    try std.testing.expectEqual(Resolution{ .width = 1280, .height = 720 }, master.variants[1].resolution.?);
    try std.testing.expectEqualStrings("av1/list.m3u8", (try master.select(.{})).uri);
    const avc = try master.select(.{ .codec_prefixes = &.{ "avc1", "avc3" } });
    try std.testing.expectEqualStrings("views/front.m3u8", avc.uri);
    try std.testing.expectEqualStrings("front", avc.video.?.name);
    const back = try master.select(.{ .video_rendition_name = "back", .max_width = 1920 });
    try std.testing.expectEqualStrings("views/back.m3u8", back.uri);
    const low = try master.select(.{ .max_bandwidth = 1000000, .max_width = 640, .max_height = 360, .max_frame_rate = 24 });
    try std.testing.expectEqualStrings("sd/list.m3u8", low.uri);
    try std.testing.expectEqual(@as(u64, 700000), low.variant.bandwidth);
    try std.testing.expectError(error.NoCompatibleHlsVariant, master.select(.{ .codec_prefixes = &.{"avc"} }));
    try std.testing.expectError(error.NoCompatibleHlsVariant, master.select(.{ .variant_uri = "missing.m3u8" }));
    try std.testing.expectError(error.NoCompatibleHlsVariant, master.select(.{ .max_frame_rate = 1 }));
    try std.testing.expectError(error.NoCompatibleHlsVariant, master.select(.{ .video_rendition_name = "missing" }));
    try std.testing.expectError(error.InvalidHlsSelection, master.select(.{ .max_frame_rate = std.math.nan(f64) }));
    try std.testing.expectError(error.ResourceLimitExceeded, parse(a, fixture, .{ .max_variants = 2 }));
    try std.testing.expectError(error.ResourceLimitExceeded, parse(a, fixture, .{ .max_renditions = 2 }));
    try std.testing.expectError(error.ResourceLimitExceeded, parse(a, fixture, .{ .max_planning_bytes = 1 }));
}
test "HLS master selection ties are independent of manifest variant ordering" {
    const a = std.testing.allocator;
    const first = "#EXT-X-STREAM-INF:BANDWIDTH=100\nz.m3u8\n";
    const second = "#EXT-X-STREAM-INF:BANDWIDTH=100\na.m3u8\n";
    inline for (.{ "#EXTM3U\n" ++ first ++ second, "#EXTM3U\n" ++ second ++ first }) |text| {
        var master = try parse(a, text, .{});
        defer master.deinit();
        try std.testing.expectEqualStrings("a.m3u8", (try master.select(.{})).uri);
    }
}
fn allocationRun(a: std.mem.Allocator, pool: *admission.Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), admission.Resources{}));
    var master = try parseControlled(a, fixture, .{}, .{}, pool);
    defer master.deinit();
    if (pool.snapshot().host_bytes == 0) return error.MissingMetadataAdmission;
    _ = try master.select(.{});
}
test "HLS master allocation denial cancellation and metadata admission reclaim all owners" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 16 * 1024 * 1024 } };
    try std.testing.checkAllAllocationFailures(a, allocationRun, .{&pool});
    var denied = admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, parseControlled(a, fixture, .{}, .{}, &denied));
    try std.testing.expectEqual(admission.Resources{}, denied.snapshot());
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*const anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.checks += 1;
            if (self.checks == 7) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    try std.testing.expectError(error.Cancelled, parseControlled(a, fixture, .{}, .{ .context = &cancel, .check_fn = Cancel.check }, &pool));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
}
test "HLS master rejects mixed playlists invalid attributes and broken rendition groups" {
    const a = std.testing.allocator;
    inline for (.{
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-STREAM-INF:BANDWIDTH=1\na\n",
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1,BANDWIDTH=2\na\n",
        "#EXTM3U\n#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"v\",NAME=\"bad\rname\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\na\n",
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1,RESOLUTION=1x0\na\n",
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1,FRAME-RATE=nan\na\n",
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1,CODECS=avc1\na\n",
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1,VIDEO=\"missing\"\na\n",
        "#EXTM3U\n#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"v\",NAME=\"a\",DEFAULT=YES,AUTOSELECT=NO\n#EXT-X-STREAM-INF:BANDWIDTH=1\na\n",
        "#EXTM3U\n#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"v\",NAME=\"a\"\n#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"v\",NAME=\"a\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\na\n",
        "#EXTM3U\n#EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS,GROUP-ID=\"cc\",NAME=\"captions\",INSTREAM-ID=\"SERVICE64\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\na\n",
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1,CLOSED-CAPTIONS=NONE\na\n#EXT-X-STREAM-INF:BANDWIDTH=1\nb\n",
    }) |text| try std.testing.expectError(error.MalformedHls, parse(a, text, .{}));
    try std.testing.expectError(error.UnsupportedEncryptedMedia, parse(a, "#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=SAMPLE-AES,URI=\"key\"\n", .{}));
    try std.testing.expectError(error.UnsupportedHls, parse(a, "#EXTM3U\n#EXT-X-VERSION:14\n", .{}));
}

fn fuzzSeed(comptime text: []const u8) []const u8 {
    return comptime blk: {
        var encoded: [4 + text.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], text.len, .little);
        @memcpy(encoded[4..], text);
        const frozen = encoded;
        break :blk &frozen;
    };
}
const fuzz_seeds = .{
    fuzzSeed(@embedFile("../testdata/hls/apple-bipbop-master.m3u8")),
    fuzzSeed(fixture),
    fuzzSeed("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\nvideo.m3u8\n"),
    fuzzSeed("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS,GROUP-ID=\"cc\",NAME=\"captions\",INSTREAM-ID=\"SERVICE63\"\n#EXT-X-STREAM-INF:BANDWIDTH=123456,CLOSED-CAPTIONS=\"cc\",CODECS=\"avc1.64001f\",FRAME-RATE=29.970,RESOLUTION=1920x1080\nvideo.m3u8\n"),
};
test "HLS master fuzz parsing selection and shared metadata cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [16 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0x484c5301);
            var pool = admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), admission.Resources{}));
            const Budget = struct {
                steps: usize = 0,
                fn check(raw: ?*const anyopaque) anyerror!void {
                    const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
                    self.steps += 1;
                    if (self.steps > 4096) return error.ResourceLimitExceeded;
                }
            };
            var budget = Budget{};
            var master = parseControlled(std.testing.allocator, bytes[0..length], .{ .max_bytes = bytes.len, .max_variants = 32, .max_renditions = 32, .max_planning_bytes = 64 * 1024 }, .{ .context = &budget, .check_fn = Budget.check }, &pool) catch return;
            defer master.deinit();
            _ = master.select(.{}) catch return;
            _ = master.select(.{ .max_bandwidth = 1000000, .codec_prefixes = &.{ "avc1", "avc3" } }) catch return;
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &fuzz_seeds });
}
test "HLS master fuzz seeds reach variant selection through Smith serialization" {
    inline for (fuzz_seeds) |seed| {
        var smith = std.testing.Smith{ .in = seed };
        var bytes: [16 * 1024]u8 = undefined;
        const length = smith.sliceWithHash(&bytes, 0x484c5301);
        try std.testing.expectEqual(seed.len - 4, length);
        var master = try parse(std.testing.allocator, bytes[0..length], .{});
        defer master.deinit();
        try std.testing.expect(master.variants.len > 0);
        _ = try master.select(.{});
    }
}

test "HLS master selection honors cancellation without releasing its live metadata owner" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 } };
    var master = try parseControlled(a, fixture, .{}, .{}, &pool);
    defer master.deinit();
    const held = pool.snapshot();
    const Cancel = struct {
        fn check(_: ?*const anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, master.selectControlled(.{}, .{ .check_fn = Cancel.check }));
    try std.testing.expectEqual(held, pool.snapshot());
    _ = try master.select(.{});
}

test "HLS master public Apple configuration selects capped AVC without fetching alternate media" {
    var master = try parse(std.testing.allocator, @embedFile("../testdata/hls/apple-bipbop-master.m3u8"), .{});
    defer master.deinit();
    try std.testing.expectEqual(@as(usize, 24), master.variants.len);
    try std.testing.expectEqual(@as(usize, 5), master.renditions.len);
    const selected = try master.select(.{ .max_bandwidth = 1500000, .max_width = 960, .max_height = 540, .max_frame_rate = 30, .codec_prefixes = &.{"avc1"} });
    try std.testing.expectEqualStrings("v4/prog_index.m3u8", selected.uri);
    try std.testing.expectEqual(@as(u64, 1326366), selected.variant.bandwidth);
    try std.testing.expectEqualStrings("aud3", selected.variant.audio.?);
    try std.testing.expectEqualStrings("sub1", selected.variant.subtitles.?);
    try std.testing.expectEqualStrings("cc1", selected.variant.closed_captions.?);
}

test "HLS master duplicate video URI metadata ties retain deterministic audio-group selection" {
    const groups = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"English\"\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"b\",NAME=\"English\"\n";
    const first = "#EXT-X-STREAM-INF:BANDWIDTH=100,CODECS=\"avc1,mp4a\",AUDIO=\"b\"\nvideo.m3u8\n";
    const second = "#EXT-X-STREAM-INF:BANDWIDTH=100,CODECS=\"avc1,mp4a\",AUDIO=\"a\"\nvideo.m3u8\n";
    inline for (.{ groups ++ first ++ second, groups ++ second ++ first }) |bytes| {
        var master = try parse(std.testing.allocator, bytes, .{});
        defer master.deinit();
        try std.testing.expectEqualStrings("a", (try master.select(.{})).variant.audio.?);
    }
}

test "HLS master validates AES session hints without applying them as media keys" {
    const text = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI=\"key\",KEYFORMAT=\"identity\",IV=0x1\n#EXT-X-STREAM-INF:BANDWIDTH=1\nvideo.m3u8\n";
    var master = try parse(std.testing.allocator, text, .{});
    defer master.deinit();
    try std.testing.expectEqualStrings("video.m3u8", (try master.select(.{})).uri);
    try std.testing.expectError(error.MalformedHls, parse(std.testing.allocator, "#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=NONE\n", .{}));
}

test "HLS master version nine discovers low latency media variants" {
    var parsed = try parse(std.testing.allocator, "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-STREAM-INF:BANDWIDTH=1000,CODECS=\"avc1.64001f\"\nparts.m3u8\n", .{});
    defer parsed.deinit();
    const selected = try parsed.select(.{});
    try std.testing.expectEqualStrings("parts.m3u8", selected.uri);
}
