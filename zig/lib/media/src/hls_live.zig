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

//! Native HTTP fMP4 HLS ingestion with bounded explicit reload and config epochs.
const std = @import("std");
const hls = @import("hls.zig");
const network = @import("http_stream.zig");
const live = @import("live_mp4.zig");
const dash_cursor = @import("dash_cursor.zig");
pub const Options = struct { playlist: hls.Limits = .{}, ingest: live.Options = .{}, selection: hls.master.Selection = .{} };
fn sameInitialization(left: hls.Initialization, right: hls.Initialization) bool {
    return std.mem.eql(u8, left.uri, right.uri) and std.meta.eql(left.range, right.range);
}
fn sameSegment(left: hls.Segment, right: hls.Segment) bool {
    return std.mem.eql(u8, left.uri, right.uri) and std.meta.eql(left.range, right.range) and sameInitialization(left.initialization, right.initialization) and left.discontinuity_sequence == right.discontinuity_sequence and left.duration_seconds == right.duration_seconds and (if (left.dash) |timing| right.dash != null and dash_cursor.sameTiming(timing, right.dash.?) else right.dash == null);
}

fn segmentOrder(left: hls.Segment, right: hls.Segment) std.math.Order {
    if (left.dash != null and right.dash != null) return dash_cursor.order(left, right);
    return std.math.order(left.sequence, right.sequence);
}

/// Single consumer, borrowing a native HTTP adapter. Initial master discovery
/// requests a master and selected media playlist; subsequent reloads request one
/// pinned media playlist; the caller's scheduler waits between reloads. next() returns
/// an independently owned immutable segment; epochs require a decoder reset.
pub fn Reader(comptime http: type) type {
    return ReaderWithManifest(http, hls);
}

/// Shared authenticated fMP4 transport/epoch/retry state machine.
pub fn ReaderWithManifest(comptime http: type, comptime manifest_parser: type) type {
    const ReaderOptions = if (@hasDecl(manifest_parser, "ReaderOptions")) manifest_parser.ReaderOptions else Options;
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        transport: *network.Adapter(http),
        url: []u8,
        /// Committed only after the selected media snapshot validates.
        media_url: ?[]u8 = null,
        options: ReaderOptions,
        manifest: ?network.Body = null,
        playlist: ?hls.Playlist = null,
        ingest: ?live.Ingest = null,
        next_sequence: ?u64 = null,
        dash_position: ?dash_cursor.Cursor = null,
        active_dash_period: ?[32]u8 = null,
        active_initialization: ?hls.Initialization = null,
        active_discontinuity_sequence: ?u64 = null,
        pub fn init(a: std.mem.Allocator, transport: *network.Adapter(http), url: []const u8, options: ReaderOptions) !Self {
            const owned = try network.resolve(a, transport.root_url, url, url, transport.limits.max_uri_bytes);
            return .{ .allocator = a, .transport = transport, .url = owned, .options = options };
        }
        pub fn deinit(self: *Self) void {
            if (self.ingest) |*ingest| ingest.deinit();
            if (self.playlist) |*playlist| playlist.deinit();
            if (self.manifest) |*manifest| manifest.deinit();
            if (self.active_initialization) |initialization| self.allocator.free(initialization.uri);
            if (self.media_url) |url| self.allocator.free(url);
            self.allocator.free(self.url);
            self.* = undefined;
        }
        pub fn mediaUrl(self: *const Self) []const u8 {
            return self.media_url orelse self.url;
        }
        /// Initial load starts at the oldest available segment. A live window
        /// advancing past an unconsumed sequence fails instead of dropping data.
        /// Overlapping sequence descriptors must remain immutable across reloads.
        pub fn reload(self: *Self) !void {
            try self.transport.control.check();
            var body = try self.transport.fetch(self.mediaUrl(), self.mediaUrl(), null, self.options.playlist.max_bytes);
            errdefer body.deinit();
            var selected_url: ?[]u8 = null;
            defer if (selected_url) |url| self.allocator.free(url);
            if (comptime @hasDecl(manifest_parser, "master")) {
                if (self.media_url == null and manifest_parser.master.isMaster(body.bytes)) {
                    var master = try manifest_parser.master.parseControlled(self.allocator, body.bytes, self.options.playlist, self.transport.control, self.transport.pool);
                    defer master.deinit();
                    const selected = try master.selectControlled(self.options.selection, self.transport.control);
                    selected_url = try network.resolve(self.allocator, self.transport.root_url, self.url, selected.uri, self.transport.limits.max_uri_bytes);
                    const media_body = try self.transport.fetch(selected_url.?, selected_url.?, null, self.options.playlist.max_bytes);
                    body.deinit();
                    body = media_body;
                }
            }
            const manifest_url = selected_url orelse self.mediaUrl();
            var parsed = if (@hasDecl(manifest_parser, "parseAtWithSelection")) blk: {
                var selection = self.options.selection;
                selection.manifest_url = manifest_url;
                break :blk try manifest_parser.parseAtWithSelection(self.allocator, body.bytes, self.options.playlist, if (self.options.clock) |clock| clock.now(clock.context) else null, selection);
            } else if (@hasDecl(manifest_parser, "parseAt"))
                try manifest_parser.parseAt(self.allocator, body.bytes, self.options.playlist, if (self.options.clock) |clock| clock.now(clock.context) else null)
            else
                try manifest_parser.parse(self.allocator, body.bytes, self.options.playlist);
            errdefer parsed.deinit();
            if (self.playlist) |previous| {
                if (!std.meta.eql(parsed.timing_identity, previous.timing_identity)) return error.SourceVersionChanged;
                if (!@hasDecl(manifest_parser, "parseAt") and parsed.target_duration_seconds != previous.target_duration_seconds) return error.SourceVersionChanged;
                if (parsed.dash_periods.len != 0 or previous.dash_periods.len != 0) {
                    var periods: std.AutoHashMap([32]u8, hls.DashPeriod) = .init(self.allocator);
                    defer periods.deinit();
                    for (previous.dash_periods) |period| try periods.put(period.identity, period);
                    for (parsed.dash_periods) |period| if (periods.get(period.identity)) |old| {
                        if (!dash_cursor.compatible(old, period)) return error.SourceVersionChanged;
                    };
                }
                // DASH orders by stable period start and period-local sequence;
                // HLS retains its consecutive global media sequence.
                var old_index: usize = 0;
                for (parsed.segments) |new| {
                    try self.transport.control.check();
                    while (old_index < previous.segments.len and segmentOrder(previous.segments[old_index], new) == .lt) old_index += 1;
                    if (old_index < previous.segments.len) {
                        const old = previous.segments[old_index];
                        if (segmentOrder(new, old) == .eq and !sameSegment(old, new)) return error.SourceVersionChanged;
                    }
                }
                if (previous.ended and !parsed.ended) return error.SourceVersionChanged;
                if (previous.ended and (previous.segments.len != parsed.segments.len or
                    (previous.segments.len != 0 and previous.segments[0].sequence != parsed.segments[0].sequence))) return error.SourceVersionChanged;
            }
            if (self.dash_position) |position| {
                _ = try position.select(parsed);
            } else if (self.next_sequence) |expected_sequence| {
                if (parsed.segments.len != 0) {
                    const first = parsed.segments[0].sequence;
                    const last = parsed.segments[parsed.segments.len - 1].sequence;
                    if (expected_sequence < first) return error.MediaStreamGap;
                    if (expected_sequence > last + 1) return error.SourceVersionChanged;
                } else if (self.playlist != null and self.playlist.?.segments.len != 0) return error.SourceVersionChanged;
            }
            try self.transport.control.check();
            if (self.playlist) |*previous| previous.deinit();
            if (self.manifest) |*previous| previous.deinit();
            self.playlist = parsed;
            self.manifest = body;
            if (selected_url) |url| {
                self.media_url = url;
                selected_url = null;
            }
            if (self.dash_position) |*position| position.refresh(parsed);
            if (self.next_sequence == null and parsed.segments.len != 0) {
                self.next_sequence = parsed.segments[0].sequence;
                if (parsed.segments[0].dash != null) self.dash_position = dash_cursor.Cursor.initial(parsed.segments[0]);
            }
        }
        pub fn ended(self: *const Self) bool {
            const playlist = self.playlist orelse return false;
            if (!playlist.ended) return false;
            if (playlist.segments.len == 0) return true;
            if (self.dash_position) |position| return (position.select(playlist) catch return false) == null;
            return (self.next_sequence orelse return false) > playlist.segments[playlist.segments.len - 1].sequence;
        }
        /// Caller-visible polling delay; no blocking sleeper is hidden in ingest.
        pub fn reloadAfterSeconds(self: *const Self, unchanged: bool) f64 {
            if (self.playlist) |playlist| if (playlist.reload_after_seconds) |seconds| return seconds;
            const target = if (self.playlist) |playlist| playlist.target_duration_seconds else 1;
            return @as(f64, @floatFromInt(target)) * (if (unchanged) @as(f64, 0.5) else 1.0);
        }
        fn commitCursor(self: *Self, item: hls.Segment) void {
            self.next_sequence = item.sequence + 1;
            if (self.dash_position) |*position| position.consumed(item);
        }
        pub fn next(self: *Self) !?live.Segment {
            try self.transport.control.check();
            const playlist = self.playlist orelse return error.MediaPlaylistRequired;
            const item: hls.Segment = if (self.dash_position) |position|
                playlist.segments[(try position.select(playlist)) orelse return null]
            else blk: {
                const sequence = self.next_sequence orelse return null;
                for (playlist.segments) |segment| if (segment.sequence == sequence) break :blk segment;
                return null;
            };
            // Failed snapshot allocation/validation retains pending input. Retry
            // that commit before any fetch; never append the same object twice.
            if (self.ingest) |*ingest| if (ingest.pending.items.len != 0) {
                var result = try ingest.finishSegment();
                result.dash_timing = item.dash;
                self.commitCursor(item);
                return result;
            };
            const needs_initialization = if (self.active_initialization) |active| !sameInitialization(active, item.initialization) or self.active_discontinuity_sequence.? != item.discontinuity_sequence or (if (item.dash) |timing| !std.meta.eql(self.active_dash_period, @as(?[32]u8, timing.period.identity)) else false) else true;
            if (needs_initialization) {
                var initialization = try self.transport.fetch(self.mediaUrl(), item.initialization.uri, item.initialization.range, self.options.ingest.stream.max_bytes);
                defer initialization.deinit();
                const owned_uri = try self.allocator.dupe(u8, item.initialization.uri);
                errdefer self.allocator.free(owned_uri);
                if (self.ingest) |*ingest| {
                    try ingest.reconfigure(self.mediaUrl(), initialization.bytes);
                } else {
                    var options = self.options.ingest;
                    options.stream.control = self.transport.control;
                    if (options.stream.admission_pool == null) options.stream.admission_pool = self.transport.pool;
                    self.ingest = try live.Ingest.init(self.allocator, self.mediaUrl(), initialization.bytes, options);
                }
                if (self.active_initialization) |previous| self.allocator.free(previous.uri);
                self.active_initialization = .{ .uri = owned_uri, .range = item.initialization.range };
                self.active_discontinuity_sequence = item.discontinuity_sequence;
                self.active_dash_period = if (item.dash) |timing| timing.period.identity else null;
            }
            var media = try self.transport.fetch(self.mediaUrl(), item.uri, item.range, self.options.ingest.stream.max_bytes);
            defer media.deinit();
            try self.ingest.?.push(media.bytes);
            var result = try self.ingest.?.finishSegment();
            result.dash_timing = item.dash;
            self.commitCursor(item);
            return result;
        }
    };
}

const FakeHttp = struct {
    pub const Client = struct {
        manifest: []const u8,
        calls: usize = 0,
        fail_media: bool = false,
        pub fn get(self: *@This(), url: []const u8, options: anytype) !Response {
            self.calls += 1;
            if (options.cancellation.is_cancelled_fn(options.cancellation.ptr)) return error.Cancelled;
            if (std.mem.endsWith(u8, url, "list.m3u8")) return .{ .body = self.manifest };
            const changed = std.mem.endsWith(u8, url, "init2.mp4") or std.mem.endsWith(u8, url, "seg2.mp4");
            const bytes = if (changed) @embedFile("../testdata/live-reconfigure.mp4") else @embedFile("../testdata/fragmented.mp4");
            var cursor: usize = 0;
            while (cursor < bytes.len) {
                const box = try @import("isobmff.zig").readBox(bytes, cursor);
                if (box.typ == @import("isobmff.zig").fourcc("moof")) break;
                cursor = box.end;
            }
            if (std.mem.indexOf(u8, url, "/init") != null) return .{ .body = bytes[0..cursor] };
            if (self.fail_media) return error.ConnectionReset;
            if (std.mem.endsWith(u8, url, "seg-1.m4s")) return .{ .body = bytes[cursor..11206] };
            if (std.mem.endsWith(u8, url, "seg-2.m4s")) return .{ .body = bytes[11206..20811] };
            return .{ .body = bytes[cursor..] };
        }
    };
    const Response = struct {
        body: ?[]const u8,
        status: struct { code: u16 } = .{ .code = 200 },
        pub fn header(_: *const @This(), _: []const u8) ?[]const u8 {
            return null;
        }
        pub fn deinit(_: *@This()) void {}
    };
};
const first_manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\nseg.mp4\n";
const second_manifest = first_manifest ++ "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"init2.mp4\"\n#EXTINF:2,\nseg2.mp4\n#EXT-X-ENDLIST\n";
test "live HLS HTTP reload and changed initialization preserve epochs and independent leases" {
    const a = std.testing.allocator;
    var client = FakeHttp.Client{ .manifest = first_manifest };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var lease = try first.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.epoch);
    try std.testing.expect((try reader.next()) == null);
    try std.testing.expect(!reader.ended());
    client.manifest = second_manifest;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
    try std.testing.expectEqual(@as(u16, 96), second.reader.track.width);
    try std.testing.expect(reader.ended());
    try std.testing.expect(lease.bytes.len > 0);
    try std.testing.expectEqual(@as(usize, 6), client.calls);
}
test "live HLS media transport retry does not repeat configuration and reload rejects mutation or gaps" {
    const a = std.testing.allocator;
    var client = FakeHttp.Client{ .manifest = first_manifest, .fail_media = true };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    client.fail_media = false;
    var segment = (try reader.next()).?;
    defer segment.deinit();
    try std.testing.expectEqual(@as(usize, 0), segment.epoch);
    try std.testing.expectEqual(@as(usize, 4), client.calls);
    client.manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\nseg.mp4\n";
    try std.testing.expectError(error.SourceVersionChanged, reader.reload());
    client.manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:12\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\nseg.mp4\n";
    try std.testing.expectError(error.MediaStreamGap, reader.reload());
    try std.testing.expectEqual(@as(u64, 11), reader.next_sequence.?);
}

test "live HLS allocation failures release both configuration epochs and retained packets" {
    const Harness = struct {
        fn run(a: std.mem.Allocator) !void {
            var client = FakeHttp.Client{ .manifest = first_manifest };
            var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
            var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
            defer reader.deinit();
            try reader.reload();
            var first = (try reader.next()).?;
            defer first.deinit();
            var lease = try first.reader.readPacket(0);
            defer lease.deinit();
            client.manifest = second_manifest;
            try reader.reload();
            var second = (try reader.next()).?;
            defer second.deinit();
            try std.testing.expectEqual(@as(usize, 1), second.epoch);
        }
    };
    // Disable resizing so every growth path is represented by an allocation
    // both during the successful count and at each injected failure.
    var baseline = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try Harness.run(baseline.allocator());
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index, .resize_fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, Harness.run(failing.allocator()));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}

test "live DASH template reload uses refreshed clock retry and gap detection" {
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z' timeShiftBufferDepth='PT4S'><Period><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    const a = std.testing.allocator;
    const TestClock = struct {
        time: i64 = 2000,
        fn now(raw: ?*anyopaque) i64 {
            return @as(*@This(), @ptrCast(@alignCast(raw.?))).time;
        }
    };
    var clock = TestClock{};
    var client = FakeHttp.Client{ .manifest = mpd };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now = TestClock.now } });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var lease = try first.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(!reader.ended());
    try std.testing.expect((try reader.next()) == null);
    clock.time = 4000;
    try reader.reload();
    client.fail_media = true;
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expectEqual(@as(u64, 2), reader.next_sequence.?);
    client.fail_media = false;
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.epoch);
    try std.testing.expectEqual(@as(usize, 6), client.calls);
    const changed_clock = try std.mem.replaceOwned(u8, a, mpd, "1970-01-01", "1970-01-02");
    defer a.free(changed_clock);
    client.manifest = changed_clock;
    try std.testing.expectError(error.SourceVersionChanged, reader.reload());
    client.manifest = mpd;
    clock.time = 20000;
    try std.testing.expectError(error.MediaStreamGap, reader.reload());
    try std.testing.expectEqual(@as(u64, 3), reader.next_sequence.?);
    try std.testing.expect(lease.bytes.len > 0);
}
test "live DASH inherited BaseURL keeps authenticated origin pinned and retries safely" {
    const a = std.testing.allocator;
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><BaseURL>./</BaseURL><Period duration='PT2S'><SegmentTemplate duration='2' initialization='init.mp4' media='seg-$Number$.m4s'/><AdaptationSet><Representation id='v'/></AdaptationSet></Period></MPD>";
    var client = FakeHttp.Client{ .manifest = mpd };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    const cross_origin = try std.mem.replaceOwned(u8, a, mpd, "<BaseURL>./", "<BaseURL>https://other.test/");
    defer a.free(cross_origin);
    client.manifest = cross_origin;
    try reader.reload();
    try std.testing.expectError(error.MediaOriginChanged, reader.next());
    try std.testing.expectEqual(@as(usize, 1), client.calls);
    try std.testing.expect(reader.ingest == null);
    client.manifest = mpd;
    // No media was consumed and the selected timing/configuration is unchanged.
    // The overlapping URI change requires a fresh reader rather than mutating
    // already-admitted immutable descriptors.
    try std.testing.expectError(error.SourceVersionChanged, reader.reload());
    var clean = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{});
    defer clean.deinit();
    try clean.reload();
    client.fail_media = true;
    try std.testing.expectError(error.ConnectionReset, clean.next());
    try std.testing.expectEqual(@as(u64, 1), clean.next_sequence.?);
    client.fail_media = false;
    var result = (try clean.next()).?;
    defer result.deinit();
    try std.testing.expect(clean.ended());
    try std.testing.expectEqual(@as(usize, 0), result.epoch);
}
const dash_period_one = "<Period id='one' start='PT0S' duration='PT4S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='one/seg-$Number$.m4s'/></Representation></AdaptationSet></Period>";
const dash_period_two = "<Period id='two' start='PT4S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='two/seg-$Number$.m4s'/></Representation></AdaptationSet></Period>";
const dash_dynamic_open = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z' timeShiftBufferDepth='PT4S'>";
const DashTestClock = struct {
    milliseconds: i64,
    fn now(raw: ?*anyopaque) i64 {
        return @as(*@This(), @ptrCast(@alignCast(raw.?))).milliseconds;
    }
};
test "DASH period transitions reset decoder epochs with repeated init URI and local numbering" {
    const a = std.testing.allocator;
    const manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'>" ++ dash_period_one ++ "<Period id='two' start='PT4S' duration='PT4S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='two/seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    var client = FakeHttp.Client{ .manifest = manifest };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var lease = try first.reader.readPacket(0);
    defer lease.deinit();
    const retained = try a.dupe(u8, lease.bytes);
    defer a.free(retained);
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.epoch);
    client.fail_media = true;
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expectEqual(@as(u64, 3), reader.next_sequence.?);
    client.fail_media = false;
    var third = (try reader.next()).?;
    defer third.deinit();
    try std.testing.expectEqual(@as(usize, 1), third.epoch);
    try std.testing.expectEqual(@as(u64, 4000), third.dash_timing.?.period.start_ms);
    try std.testing.expectEqual(@as(u64, 0), third.dash_timing.?.start_ticks);
    var fourth = (try reader.next()).?;
    defer fourth.deinit();
    try std.testing.expectEqual(@as(usize, 1), fourth.epoch);
    try std.testing.expect(reader.ended());
    try std.testing.expect((try reader.next()) == null);
    try std.testing.expectEqualSlices(u8, retained, lease.bytes);
    try std.testing.expectEqual(@as(usize, 8), client.calls);
}
test "live DASH retains learned period endings across removal and rejects skipped pending media" {
    const a = std.testing.allocator;
    const initial = dash_dynamic_open ++ dash_period_one ++ "</MPD>";
    const unbounded = try std.mem.replaceOwned(u8, a, initial, " duration='PT4S'", "");
    defer a.free(unbounded);
    const appended = dash_dynamic_open ++ dash_period_one ++ dash_period_two ++ "</MPD>";
    const dropped = dash_dynamic_open ++ dash_period_two ++ "</MPD>";
    var clock = DashTestClock{ .milliseconds = 4000 };
    var client = FakeHttp.Client{ .manifest = unbounded };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now = DashTestClock.now } });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expect(!reader.dash_position.?.last.?.finishesPeriod());
    client.manifest = appended;
    clock.milliseconds = 6000;
    try reader.reload();
    try std.testing.expect(reader.dash_position.?.last.?.finishesPeriod());
    client.manifest = dropped;
    try reader.reload();
    var next_period = (try reader.next()).?;
    defer next_period.deinit();
    try std.testing.expectEqual(@as(usize, 1), next_period.epoch);
    try std.testing.expectEqual(@as(u64, 4000), next_period.dash_timing.?.period.start_ms);
    try std.testing.expect((try reader.next()) == null);
    // A separate consumer still has period one's second segment pending.
    client.manifest = initial;
    clock.milliseconds = 4000;
    var pending = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, reader.options);
    defer pending.deinit();
    try pending.reload();
    var pending_first = (try pending.next()).?;
    defer pending_first.deinit();
    client.manifest = dropped;
    clock.milliseconds = 6000;
    try std.testing.expectError(error.MediaStreamGap, pending.reload());
    try std.testing.expectEqual(@as(u64, 2), pending.next_sequence.?);
    client.manifest = appended;
    const changed = try std.mem.replaceOwned(u8, a, appended, "id='v'", "id='changed'");
    defer a.free(changed);
    client.manifest = changed;
    try std.testing.expectError(error.SourceVersionChanged, pending.reload());
    client.manifest = appended;
    try pending.reload();
    var pending_second = (try pending.next()).?;
    defer pending_second.deinit();
    try std.testing.expectEqual(@as(usize, 0), pending_second.epoch);
}
test "live DASH waits for incomplete periods and rejects an empty expired pending window" {
    const a = std.testing.allocator;
    var clock = DashTestClock{ .milliseconds = 2000 };
    var client = FakeHttp.Client{ .manifest = dash_dynamic_open ++ dash_period_one ++ dash_period_two ++ "</MPD>" };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now = DashTestClock.now } });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    try std.testing.expect((try reader.next()) == null);
    try reader.reload();
    try std.testing.expect((try reader.next()) == null);
    const equivalent = try std.mem.replaceOwned(u8, a, client.manifest, "00:00:00Z", "00:00:00.000Z");
    defer a.free(equivalent);
    client.manifest = equivalent;
    try reader.reload();
    // Period one remains in the MPD, but every available reference expired.
    client.manifest = dash_dynamic_open ++ dash_period_one ++ "</MPD>";
    clock.milliseconds = 20000;
    try std.testing.expectError(error.SourceVersionChanged, reader.reload());
    // Keep the previously announced successor while all media has expired.
    client.manifest = dash_dynamic_open ++ dash_period_one ++ "<Period id='two' start='PT4S' duration='PT4S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='two/seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    try std.testing.expectError(error.MediaStreamGap, reader.reload());
    try std.testing.expectEqual(@as(u64, 2), reader.next_sequence.?);
}
fn dashPeriodAllocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var clock = DashTestClock{ .milliseconds = 4000 };
    var client = FakeHttp.Client{ .manifest = dash_dynamic_open ++ dash_period_one ++ "</MPD>" };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now = DashTestClock.now } });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var second = (try reader.next()).?;
    defer second.deinit();
    client.manifest = dash_dynamic_open ++ dash_period_one ++ dash_period_two ++ "</MPD>";
    clock.milliseconds = 6000;
    try reader.reload();
    var transition = (try reader.next()).?;
    defer transition.deinit();
    try std.testing.expectEqual(@as(usize, 1), transition.epoch);
}
test "live DASH reload and period reconfiguration release every failing allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dashPeriodAllocationRun, .{});
}

const master_manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,CODECS=\"avc1.64001f\",RESOLUTION=1280x720\nvariants/high/list.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=700000,CODECS=\"avc1.4d401e\",RESOLUTION=640x360\nvariants/low/list.m3u8\n";
const MasterHttp = struct {
    pub const Client = struct {
        master: []const u8 = master_manifest,
        media: []const u8 = first_manifest,
        calls: usize = 0,
        fail_playlist: bool = false,
        pub fn get(self: *@This(), url: []const u8, options: anytype) !FakeHttp.Response {
            self.calls += 1;
            if (std.mem.endsWith(u8, url, "/master.m3u8")) return .{ .body = self.master };
            if (!std.mem.startsWith(u8, url, "https://example.test/live/variants/low/")) return error.IncorrectHlsVariantBase;
            if (std.mem.endsWith(u8, url, "/list.m3u8")) {
                if (self.fail_playlist) return error.ConnectionReset;
                return .{ .body = self.media };
            }
            var original = FakeHttp.Client{ .manifest = self.media };
            return original.get(url, options);
        }
    };
};
test "live HLS master selects media base and pins it across reload and epoch transitions" {
    const a = std.testing.allocator;
    var client = MasterHttp.Client{};
    var transport = network.Adapter(MasterHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/master.m3u8" };
    var reader = try Reader(MasterHttp).init(a, &transport, transport.root_url, .{ .selection = .{ .max_bandwidth = 1000000, .codec_prefixes = &.{"avc1"} } });
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectEqualStrings("https://example.test/live/variants/low/list.m3u8", reader.mediaUrl());
    var first = (try reader.next()).?;
    defer first.deinit();
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    const bytes = try a.dupe(u8, held.bytes);
    defer a.free(bytes);
    client.master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhttps://other.test/changed.m3u8\n";
    client.media = second_manifest;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
    try std.testing.expectEqualSlices(u8, bytes, held.bytes);
    try std.testing.expect(reader.ended());
    try std.testing.expectEqual(@as(usize, 7), client.calls);
}
test "live HLS master discovery failures retain uncommitted URL and release admission" {
    const a = std.testing.allocator;
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    var client = MasterHttp.Client{ .fail_playlist = true };
    var transport = network.Adapter(MasterHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/master.m3u8", .pool = &pool };
    var reader = try Reader(MasterHttp).init(a, &transport, transport.root_url, .{ .selection = .{ .max_bandwidth = 1000000 } });
    defer reader.deinit();
    try std.testing.expectError(error.ConnectionReset, reader.reload());
    try std.testing.expect(reader.media_url == null and reader.playlist == null and reader.next_sequence == null);
    try std.testing.expectEqual(@import("admission.zig").Resources{}, pool.snapshot());
    client.fail_playlist = false;
    client.media = "#EXTM3U\n#EXTINF:2,\n";
    try std.testing.expectError(error.MalformedHls, reader.reload());
    try std.testing.expect(reader.media_url == null and reader.playlist == null);
    try std.testing.expectEqual(@import("admission.zig").Resources{}, pool.snapshot());
    client.media = first_manifest;
    try reader.reload();
    try std.testing.expect(reader.media_url != null);
    try std.testing.expectEqual(@as(usize, 6), client.calls);
}
test "live HLS master refuses foreign-origin variants before authenticated transport" {
    const a = std.testing.allocator;
    var client = MasterHttp.Client{ .master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhttps://other.test/media.m3u8\n" };
    var transport = network.Adapter(MasterHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/master.m3u8" };
    var reader = try Reader(MasterHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try std.testing.expectError(error.MediaOriginChanged, reader.reload());
    try std.testing.expectEqual(@as(usize, 1), client.calls);
    try std.testing.expect(reader.media_url == null and reader.playlist == null);
}
fn masterAllocations(a: std.mem.Allocator, pool: *@import("admission.zig").Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), @import("admission.zig").Resources{}));
    var client = MasterHttp.Client{};
    var transport = network.Adapter(MasterHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/master.m3u8", .pool = pool };
    var reader = try Reader(MasterHttp).init(a, &transport, transport.root_url, .{ .selection = .{ .max_bandwidth = 1000000 } });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    client.media = second_manifest;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
}
test "live HLS master selection allocation failures release snapshots epochs and packet owners" {
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, masterAllocations, .{&pool});
}
