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
pub const Options = struct { playlist: hls.Limits = .{}, ingest: live.Options = .{} };
fn sameInitialization(left: hls.Initialization, right: hls.Initialization) bool {
    return std.mem.eql(u8, left.uri, right.uri) and std.meta.eql(left.range, right.range);
}
fn sameSegment(left: hls.Segment, right: hls.Segment) bool {
    return std.mem.eql(u8, left.uri, right.uri) and std.meta.eql(left.range, right.range) and sameInitialization(left.initialization, right.initialization) and left.discontinuity_sequence == right.discontinuity_sequence and left.duration_seconds == right.duration_seconds;
}

/// Single consumer, borrowing a native HTTP adapter. reload() performs one
/// bounded request; the caller's scheduler waits between reloads. next() returns
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
        options: ReaderOptions,
        manifest: ?network.Body = null,
        playlist: ?hls.Playlist = null,
        ingest: ?live.Ingest = null,
        next_sequence: ?u64 = null,
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
            self.allocator.free(self.url);
            self.* = undefined;
        }
        /// Initial load starts at the oldest available segment. A live window
        /// advancing past an unconsumed sequence fails instead of dropping data.
        /// Overlapping sequence descriptors must remain immutable across reloads.
        pub fn reload(self: *Self) !void {
            try self.transport.control.check();
            var body = try self.transport.fetch(self.url, self.url, null, self.options.playlist.max_bytes);
            errdefer body.deinit();
            var parsed = if (@hasDecl(manifest_parser, "parseAtWithSelection")) blk: {
                var selection = self.options.selection;
                selection.manifest_url = self.url;
                break :blk try manifest_parser.parseAtWithSelection(self.allocator, body.bytes, self.options.playlist, if (self.options.clock) |clock| clock.now(clock.context) else null, selection);
            } else if (@hasDecl(manifest_parser, "parseAt"))
                try manifest_parser.parseAt(self.allocator, body.bytes, self.options.playlist, if (self.options.clock) |clock| clock.now(clock.context) else null)
            else
                try manifest_parser.parse(self.allocator, body.bytes, self.options.playlist);
            errdefer parsed.deinit();
            if (self.playlist) |previous| {
                if (!std.meta.eql(parsed.timing_identity, previous.timing_identity)) return error.SourceVersionChanged;
                if (!@hasDecl(manifest_parser, "parseAt") and parsed.target_duration_seconds != previous.target_duration_seconds) return error.SourceVersionChanged;
                // Both arrays have consecutive sequence numbers. Compare the
                // overlap in linear time, checking cancellation during reload.
                var old_index: usize = 0;
                for (parsed.segments) |new| {
                    try self.transport.control.check();
                    while (old_index < previous.segments.len and previous.segments[old_index].sequence < new.sequence) old_index += 1;
                    if (old_index < previous.segments.len) {
                        const old = previous.segments[old_index];
                        if (new.sequence == old.sequence and !sameSegment(new, old)) return error.SourceVersionChanged;
                    }
                }
                if (previous.ended and !parsed.ended) return error.SourceVersionChanged;
                if (previous.ended and (previous.segments.len != parsed.segments.len or
                    (previous.segments.len != 0 and previous.segments[0].sequence != parsed.segments[0].sequence))) return error.SourceVersionChanged;
            }
            if (self.next_sequence) |expected_sequence| {
                if (parsed.segments.len != 0) {
                    const first = parsed.segments[0].sequence;
                    const last = parsed.segments[parsed.segments.len - 1].sequence;
                    if (expected_sequence < first) return error.MediaStreamGap;
                    if (expected_sequence > last + 1) return error.SourceVersionChanged;
                } else if (self.playlist != null and self.playlist.?.segments.len != 0) return error.SourceVersionChanged;
            }
            if (self.playlist) |*previous| previous.deinit();
            if (self.manifest) |*previous| previous.deinit();
            self.playlist = parsed;
            self.manifest = body;
            if (self.next_sequence == null and parsed.segments.len != 0) self.next_sequence = parsed.segments[0].sequence;
        }
        pub fn ended(self: *const Self) bool {
            const playlist = self.playlist orelse return false;
            if (!playlist.ended) return false;
            if (playlist.segments.len == 0) return true;
            return (self.next_sequence orelse return false) > playlist.segments[playlist.segments.len - 1].sequence;
        }
        /// Caller-visible polling delay; no blocking sleeper is hidden in ingest.
        pub fn reloadAfterSeconds(self: *const Self, unchanged: bool) f64 {
            if (self.playlist) |playlist| if (playlist.reload_after_seconds) |seconds| return seconds;
            const target = if (self.playlist) |playlist| playlist.target_duration_seconds else 1;
            return @as(f64, @floatFromInt(target)) * (if (unchanged) @as(f64, 0.5) else 1.0);
        }
        pub fn next(self: *Self) !?live.Segment {
            try self.transport.control.check();
            const playlist = self.playlist orelse return error.MediaPlaylistRequired;
            const sequence = self.next_sequence orelse return null;
            const item: hls.Segment = blk: {
                for (playlist.segments) |segment| if (segment.sequence == sequence) break :blk segment;
                return null;
            };
            // Failed snapshot allocation/validation retains pending input. Retry
            // that commit before any fetch; never append the same object twice.
            if (self.ingest) |*ingest| if (ingest.pending.items.len != 0) {
                const result = try ingest.finishSegment();
                self.next_sequence = sequence + 1;
                return result;
            };
            const needs_initialization = if (self.active_initialization) |active| !sameInitialization(active, item.initialization) or self.active_discontinuity_sequence.? != item.discontinuity_sequence else true;
            if (needs_initialization) {
                var initialization = try self.transport.fetch(self.url, item.initialization.uri, item.initialization.range, self.options.ingest.stream.max_bytes);
                defer initialization.deinit();
                const owned_uri = try self.allocator.dupe(u8, item.initialization.uri);
                errdefer self.allocator.free(owned_uri);
                if (self.ingest) |*ingest| {
                    try ingest.reconfigure(self.url, initialization.bytes);
                } else {
                    var options = self.options.ingest;
                    options.stream.control = self.transport.control;
                    if (options.stream.admission_pool == null) options.stream.admission_pool = self.transport.pool;
                    self.ingest = try live.Ingest.init(self.allocator, self.url, initialization.bytes, options);
                }
                if (self.active_initialization) |previous| self.allocator.free(previous.uri);
                self.active_initialization = .{ .uri = owned_uri, .range = item.initialization.range };
                self.active_discontinuity_sequence = item.discontinuity_sequence;
            }
            var media = try self.transport.fetch(self.url, item.uri, item.range, self.options.ingest.stream.max_bytes);
            defer media.deinit();
            try self.ingest.?.push(media.bytes);
            const result = try self.ingest.?.finishSegment();
            self.next_sequence = sequence + 1;
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
