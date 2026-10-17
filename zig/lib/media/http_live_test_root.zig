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

const std = @import("std");
const media = @import("antfly_media");
const http = @import("httpx");
const a = std.testing.allocator;
const Verify = struct {
    fn auth(request: http.testing_mod.RequestInfo) !void {
        try std.testing.expectEqualStrings("Bearer fixture-token", request.header("Authorization").?);
        try std.testing.expectEqualStrings("identity", request.header("Accept-Encoding").?);
    }
    fn range(request: http.testing_mod.RequestInfo) !void {
        try auth(request);
        try std.testing.expectEqualStrings("bytes=2-5", request.header("Range").?);
    }
    fn serve(server: *http.testing_mod.TestServer, count: usize) !void {
        for (0..count) |_| try server.handleOne();
    }
};
test "production HTTP fragments publish before response EOF under authenticated chunked backpressure" {
    const bytes = @embedFile("testdata/fragmented.mp4");
    const State = struct {
        release: std.Io.Event = .unset,
        sent_tail: std.atomic.Value(bool) = .init(false),
        count: usize = 0,
        held: ?media.live_mp4.Segment = null,
        fn serve(self: *@This(), listener: *http.TcpListener) !void {
            const accepted = try listener.accept();
            var socket = accepted.socket;
            defer socket.close();
            try socket.setRecvTimeout(2000);
            try socket.setSendTimeout(2000);
            var request: [4096]u8 = undefined;
            var used: usize = 0;
            while (std.mem.indexOf(u8, request[0..used], "\r\n\r\n") == null) {
                if (used == request.len) return error.TestUnexpectedResult;
                const n = try socket.recv(request[used..]);
                if (n == 0) return error.UnexpectedEof;
                used += n;
            }
            try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "authorization: Bearer fixture-token\r\n") != null);
            try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "accept-encoding: identity\r\n") != null);
            try socket.sendAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
            const boundary = 11206 + 8; // Next moof header frames the first fragment.
            try chunk(&socket, bytes[797..boundary]);
            // The rest of the response cannot exist until a fragment callback
            // has run. A whole-response buffering regression times out.
            try self.release.wait(std.testing.io);
            self.sent_tail.store(true, .release);
            try chunk(&socket, bytes[boundary..]);
            try socket.sendAll("0\r\n\r\n");
        }
        fn chunk(socket: *http.Socket, body: []const u8) !void {
            var header: [32]u8 = undefined;
            try socket.sendAll(try std.fmt.bufPrint(&header, "{x}\r\n", .{body.len}));
            try socket.sendAll(body);
            try socket.sendAll("\r\n");
        }
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count == 0) {
                try std.testing.expect(!self.sent_tail.load(.acquire));
                self.held = try delivery.take();
                self.release.set(std.testing.io);
            }
            self.count += 1;
        }
    };
    var state = State{};
    defer if (state.held) |*segment| segment.deinit();
    var listener = try http.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, std.testing.io);
    defer listener.deinit();
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/media.m4s", .{listener.getLocalAddress().getPort()});
    defer a.free(url);
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url, .limits = .{ .timeout_ms = 2000 } };
    var ingest = try media.live_mp4.Ingest.init(a, "authenticated-stream", bytes[0..797], .{});
    var live_ingest = true;
    defer if (live_ingest) ingest.deinit();
    var serving = try std.testing.io.concurrent(State.serve, .{ &state, &listener });
    defer _ = serving.cancel(std.testing.io) catch {};
    const stats = try transport.streamFmp4(url, "media.m4s", null, bytes.len, &ingest, &state, State.accept);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 6), stats.fragments);
    try std.testing.expectEqual(stats.fragments, state.count);
    try std.testing.expect(stats.largest_pending_capacity < stats.bytes);
    ingest.deinit();
    live_ingest = false;
    var lease = try state.held.?.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len != 0);
}
test "production HTTP media client authenticates exact ranges and refuses redirects" {
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/data", .assert_request = Verify.range, .respond = .{ .status = 206, .headers = &.{.{ .name = "Content-Range", .value = "bytes 2-5/16" }}, .body = "cdef" } },
        .{ .path = "/redirect", .assert_request = Verify.auth, .respond = .{ .status = 302, .headers = &.{.{ .name = "Location", .value = "https://other.test/data" }}, .body = "redirect" } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = server.baseUrl() };
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 2 });
    defer _ = serving.cancel(std.testing.io) catch {};
    var body = try transport.fetch(server.baseUrl(), "/data", .{ .offset = 2, .length = 4 }, 4);
    defer body.deinit();
    try std.testing.expectEqualStrings("cdef", body.bytes);
    try std.testing.expectError(error.UnexpectedHttpStatus, transport.fetch(server.baseUrl(), "/redirect", null, 1024));
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 2), transport.requests);
}

test "production live HLS HTTP ingestion reloads configuration without invalidating held packets" {
    const first = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\nseg.mp4\n";
    const second = first ++ "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"init2.mp4\"\n#EXTINF:2,\nseg2.mp4\n#EXT-X-ENDLIST\n";
    const original = @embedFile("testdata/fragmented.mp4");
    const changed = @embedFile("testdata/live-reconfigure.mp4");
    const split = struct {
        fn at(bytes: []const u8) !usize {
            var cursor: usize = 0;
            while (cursor < bytes.len) {
                const box = try media.isobmff.readBox(bytes, cursor);
                if (box.typ == media.isobmff.fourcc("moof")) return cursor;
                cursor = box.end;
            }
            return error.MissingMedia;
        }
    }.at;
    const offset = try split(original);
    const changed_offset = try split(changed);
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/live/list.m3u8", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = first } },
        .{ .path = "/live/list.m3u8", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = second } },
        .{ .path = "/live/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..offset] } },
        .{ .path = "/live/seg.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[offset..] } },
        .{ .path = "/live/init2.mp4", .assert_request = Verify.auth, .respond = .{ .body = changed[0..changed_offset] } },
        .{ .path = "/live/seg2.mp4", .assert_request = Verify.auth, .respond = .{ .body = changed[changed_offset..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/live/list.m3u8", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{});
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 6 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var old = (try reader.next()).?;
    defer old.deinit();
    var lease = try old.reader.readPacket(0);
    defer lease.deinit();
    const retained = try a.dupe(u8, lease.bytes);
    defer a.free(retained);
    try reader.reload();
    var new = (try reader.next()).?;
    defer new.deinit();
    try std.testing.expectEqual(@as(usize, 1), new.epoch);
    try std.testing.expectEqual(@as(u16, 96), new.reader.track.width);
    try std.testing.expectEqualSlices(u8, retained, lease.bytes);
    try std.testing.expect(reader.ended());
    try serving.await(std.testing.io);
}

test "production DASH inherited SegmentList selects representation with authenticated BaseURL transport" {
    const manifest = "<?xml version='1.0'?><MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><BaseURL>media/</BaseURL><Period><SegmentList duration='2'><Initialization sourceURL='init.mp4'/><SegmentURL media='seg.m4s'/></SegmentList><AdaptationSet mimeType='video/mp4'><Representation id='other'/><Representation id='v'/></AdaptationSet></Period></MPD>";
    const original = @embedFile("testdata/fragmented.mp4");
    var offset: usize = 0;
    while (offset < original.len) {
        const box = try media.isobmff.readBox(original, offset);
        if (box.typ == media.isobmff.fourcc("moof")) break;
        offset = box.end;
    }
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/dash/manifest.mpd", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/dash/media/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..offset] } },
        .{ .path = "/dash/media/seg.m4s", .assert_request = Verify.auth, .respond = .{ .body = original[offset..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/dash/manifest.mpd", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.dash_live.Reader(http).init(a, &transport, url, .{ .selection = .{ .representation_id = "v" } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 3 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var segment = (try reader.next()).?;
    defer segment.deinit();
    var lease = try segment.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
    try std.testing.expect(reader.ended());
    try std.testing.expect((try reader.next()) == null);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 3), transport.requests);
}

test "production live DASH template uses synchronized clock and authenticated fMP4 transport" {
    const manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z'><Period><AdaptationSet mimeType='video/mp4'><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    const Clock = struct {
        fn now(_: ?*anyopaque) i64 {
            return 2000;
        }
    };
    const original = @embedFile("testdata/fragmented.mp4");
    var offset: usize = 0;
    while (offset < original.len) {
        const box = try media.isobmff.readBox(original, offset);
        if (box.typ == media.isobmff.fourcc("moof")) break;
        offset = box.end;
    }
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/dash/manifest.mpd", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/dash/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..offset] } },
        .{ .path = "/dash/seg-1.m4s", .assert_request = Verify.auth, .respond = .{ .body = original[offset..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/dash/manifest.mpd", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.dash_live.Reader(http).init(a, &transport, url, .{ .clock = .{ .now = Clock.now } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 3 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var segment = (try reader.next()).?;
    defer segment.deinit();
    var lease = try segment.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
    try std.testing.expect(!reader.ended());
    try std.testing.expect((try reader.next()) == null);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 3), transport.requests);
}

test "production DASH availability offsets fetch authenticated media only at the exact boundary" {
    const manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z'><BaseURL availabilityTimeOffset='0.25'>./</BaseURL><Period><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' availabilityTimeOffset='0.75' initialization='init.mp4' media='seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    const OffsetClock = struct {
        time: i64 = 999,
        fn now(raw: ?*anyopaque) i64 {
            return @as(*@This(), @ptrCast(@alignCast(raw.?))).time;
        }
    };
    var clock = OffsetClock{};
    const original = @embedFile("testdata/fragmented.mp4");
    var offset: usize = 0;
    while (offset < original.len) {
        const box = try media.isobmff.readBox(original, offset);
        if (box.typ == media.isobmff.fourcc("moof")) break;
        offset = box.end;
    }
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/dash/manifest.mpd", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/dash/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..offset] } },
        .{ .path = "/dash/seg-1.m4s", .assert_request = Verify.auth, .respond = .{ .body = original[offset..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/dash/manifest.mpd", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.dash_live.Reader(http).init(a, &transport, url, .{ .clock = .{ .context = &clock, .now = OffsetClock.now } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 4 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    try std.testing.expect((try reader.next()) == null);
    try std.testing.expectEqual(@as(usize, 1), transport.requests);
    clock.time = 1000;
    try reader.reload();
    var segment = (try reader.next()).?;
    defer segment.deinit();
    var lease = try segment.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
    try std.testing.expect((try reader.next()) == null);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 4), transport.requests);
}
test "production DASH multiple periods authenticate repeated initialization and expose exact presentation mapping" {
    const manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period id='one' duration='PT2S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='one-$Number$.m4s'/></Representation></AdaptationSet></Period><Period id='two' duration='PT2S'><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' initialization='init.mp4' media='two-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    const original = @embedFile("testdata/fragmented.mp4");
    var offset: usize = 0;
    while (offset < original.len) {
        const box = try media.isobmff.readBox(original, offset);
        if (box.typ == media.isobmff.fourcc("moof")) break;
        offset = box.end;
    }
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/dash/manifest.mpd", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/dash/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..offset] } },
        .{ .path = "/dash/one-1.m4s", .assert_request = Verify.auth, .respond = .{ .body = original[offset..11206] } },
        .{ .path = "/dash/two-1.m4s", .assert_request = Verify.auth, .respond = .{ .body = original[offset..11206] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/dash/manifest.mpd", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.dash_live.Reader(http).init(a, &transport, url, .{});
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 5 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var lease = try first.reader.readPacket(0);
    defer lease.deinit();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
    const time = second.dash_timing.?.presentationStart();
    try std.testing.expectEqual(@as(i128, 2000), @divExact(time.numerator, time.denominator));
    try std.testing.expect(lease.bytes.len != 0);
    try std.testing.expect(reader.ended());
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 5), transport.requests);
}

test "production HLS master selects authenticated alternate video and resolves relative initialization across reload" {
    const master = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"camera\",NAME=\"main\",DEFAULT=YES,AUTOSELECT=YES,URI=\"cameras/main/list.m3u8\"\n#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"camera\",NAME=\"side\",URI=\"cameras/side/list.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,CODECS=\"avc1.64001f\",VIDEO=\"camera\"\nlarge/list.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=700000,CODECS=\"avc1.4d401e\",VIDEO=\"camera\"\nsmall/list.m3u8\n";
    const first = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\nseg.mp4\n";
    const second = first ++ "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"init2.mp4\"\n#EXTINF:2,\nseg2.mp4\n#EXT-X-ENDLIST\n";
    const original = @embedFile("testdata/fragmented.mp4");
    const changed = @embedFile("testdata/live-reconfigure.mp4");
    const split = struct {
        fn at(bytes: []const u8) !usize {
            var cursor: usize = 0;
            while (cursor < bytes.len) {
                const box = try media.isobmff.readBox(bytes, cursor);
                if (box.typ == media.isobmff.fourcc("moof")) return cursor;
                cursor = box.end;
            }
            return error.MissingMedia;
        }
    }.at;
    const offset = try split(original);
    const changed_offset = try split(changed);
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/hls/master.m3u8", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = master } },
        .{ .path = "/hls/cameras/side/list.m3u8", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = first } },
        .{ .path = "/hls/cameras/side/list.m3u8", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = second } },
        .{ .path = "/hls/cameras/side/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..offset] } },
        .{ .path = "/hls/cameras/side/seg.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[offset..] } },
        .{ .path = "/hls/cameras/side/init2.mp4", .assert_request = Verify.auth, .respond = .{ .body = changed[0..changed_offset] } },
        .{ .path = "/hls/cameras/side/seg2.mp4", .assert_request = Verify.auth, .respond = .{ .body = changed[changed_offset..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/hls/master.m3u8", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{ .selection = .{ .max_bandwidth = 1000000, .video_rendition_name = "side", .codec_prefixes = &.{ "avc1", "avc3" } } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 7 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    const selected_url = try std.fmt.allocPrint(a, "{s}/hls/cameras/side/list.m3u8", .{server.baseUrl()});
    defer a.free(selected_url);
    try std.testing.expectEqualStrings(selected_url, reader.mediaUrl());
    var old = (try reader.next()).?;
    defer old.deinit();
    var lease = try old.reader.readPacket(0);
    defer lease.deinit();
    const retained = try a.dupe(u8, lease.bytes);
    defer a.free(retained);
    try reader.reload();
    var new = (try reader.next()).?;
    defer new.deinit();
    try std.testing.expectEqual(@as(usize, 1), new.epoch);
    try std.testing.expectEqual(@as(u16, 96), new.reader.track.width);
    try std.testing.expectEqualSlices(u8, retained, lease.bytes);
    try std.testing.expect(reader.ended());
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 7), transport.requests);
}

test "production HLS AES authenticates keys decrypts ranged media and rotates without losing old packets" {
    const key1 = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";
    const key2 = "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f";
    const init = @embedFile("testdata/hls/aes/init1.bin");
    const first = @embedFile("testdata/hls/aes/media10.bin");
    const second = @embedFile("testdata/hls/aes/media11.bin");
    const total = first.len + second.len;
    const manifest = std.fmt.comptimePrint("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\",IV=0x1\n#EXT-X-MAP:URI=\"init\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:{d}@0\nblob\n#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:{d}\nblob\n#EXT-X-ENDLIST\n", .{ first.len, second.len });
    const Ranges = struct {
        fn firstRange(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings(std.fmt.comptimePrint("bytes=0-{d}", .{first.len - 1}), request.header("Range").?);
        }
        fn secondRange(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings(std.fmt.comptimePrint("bytes={d}-{d}", .{ first.len, total - 1 }), request.header("Range").?);
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/aes/list.m3u8", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/aes/key1", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = key1 } },
        .{ .path = "/aes/key2", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = key2 } },
        .{ .path = "/aes/init", .assert_request = Verify.auth, .respond = .{ .body = init } },
        .{ .path = "/aes/blob", .assert_request = Ranges.firstRange, .max_uses = 1, .respond = .{ .status = 206, .headers = &.{.{ .name = "Content-Range", .value = std.fmt.comptimePrint("bytes 0-{d}/{d}", .{ first.len - 1, total }) }}, .body = first } },
        .{ .path = "/aes/blob", .assert_request = Ranges.secondRange, .max_uses = 1, .respond = .{ .status = 206, .headers = &.{.{ .name = "Content-Range", .value = std.fmt.comptimePrint("bytes {d}-{d}/{d}", .{ first.len, total - 1, total }) }}, .body = second } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/aes/list.m3u8", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{});
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 6 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var one = (try reader.next()).?;
    defer one.deinit();
    const original = @embedFile("testdata/fragmented.mp4");
    try std.testing.expectEqualSlices(u8, original[0..11206], one.backing.bytes);
    var held = try one.reader.readPacket(0);
    defer held.deinit();
    const retained = try a.dupe(u8, held.bytes);
    defer a.free(retained);
    var two = (try reader.next()).?;
    defer two.deinit();
    try std.testing.expectEqual(@as(usize, 0), two.epoch);
    try std.testing.expectEqualSlices(u8, retained, held.bytes);
    try std.testing.expect(reader.ended());
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 6), transport.requests);
}

test "production HTTP low latency HLS authenticates blocking reload and verifies parent overlap" {
    const prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=2\n#EXT-X-MAP:URI=\"init.mp4\"\n";
    const first = prefix ++ "#EXT-X-PART:DURATION=1,URI=\"p0.m4s\",INDEPENDENT=YES\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"unpublished.m4s\"\n";
    const complete = prefix ++ "#EXTINF:2,\nparent.m4s\n#EXT-X-ENDLIST\n";
    const bytes = @embedFile("testdata/fragmented.mp4");
    const Request = struct {
        fn initial(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/live/list.m3u8?token=stable%26key", request.target);
        }
        fn blocking(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/live/list.m3u8?token=stable%26key&_HLS_msn=10&_HLS_part=1", request.target);
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/live/list.m3u8", .assert_request = Request.initial, .max_uses = 1, .respond = .{ .body = first } },
        .{ .path = "/live/list.m3u8", .assert_request = Request.blocking, .max_uses = 1, .respond = .{ .body = complete } },
        .{ .path = "/live/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = bytes[0..797] } },
        .{ .path = "/live/p0.m4s", .assert_request = Verify.auth, .respond = .{ .body = bytes[797..11206] } },
        .{ .path = "/live/parent.m4s", .assert_request = Verify.auth, .respond = .{ .body = bytes[797..20811] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/live/list.m3u8?token=stable%26key", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{});
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 5 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var head = (try reader.next()).?;
    defer head.deinit();
    var retained = try head.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expectEqual(@as(u32, 0), head.hls_timing.?.part_index.?);
    try reader.reloadBlocking();
    var tail = (try reader.next()).?;
    defer tail.deinit();
    try std.testing.expectEqual(@as(u64, 10), tail.hls_timing.?.sequence);
    try std.testing.expect(tail.hls_timing.?.part_index == null);
    try std.testing.expectEqual(head.reader.packets.len, tail.reader.packets.len);
    try std.testing.expectEqual(head.reader.packets[head.reader.packets.len - 1].media_dts + head.reader.packets[head.reader.packets.len - 1].duration, tail.reader.packets[0].media_dts);
    try std.testing.expect(reader.ended());
    var again = try head.reader.readPacket(0);
    defer again.deinit();
    try std.testing.expectEqualSlices(u8, retained.bytes, again.bytes);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 5), transport.requests);
}

test "production HTTP HLS delta merges snapshots and retries missing history without duplicating media" {
    const prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-SERVER-CONTROL:CAN-SKIP-UNTIL=12\n";
    const first = prefix ++ "#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\none.m4s\n";
    const second = prefix ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=1\n#EXTINF:2,\ntwo.m4s\n";
    const missing = prefix ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=3\n#EXTINF:2,\nfour.m4s\n";
    const full = first ++ "#EXTINF:2,\ntwo.m4s\n#EXTINF:2,\nthree.m4s\n#EXTINF:2,\nfour.m4s\n#EXT-X-ENDLIST\n";
    const bytes = @embedFile("testdata/fragmented.mp4");
    const Request = struct {
        fn initial(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/live/list.m3u8?token=stable%26key", request.target);
        }
        fn delta(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/live/list.m3u8?token=stable%26key&_HLS_skip=YES", request.target);
        }
        fn now(_: ?*anyopaque) u64 {
            return std.time.ns_per_s;
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/live/list.m3u8", .assert_request = Request.initial, .max_uses = 1, .respond = .{ .body = first } },
        .{ .path = "/live/list.m3u8", .assert_request = Request.delta, .max_uses = 1, .respond = .{ .body = second } },
        .{ .path = "/live/list.m3u8", .assert_request = Request.delta, .max_uses = 1, .respond = .{ .body = missing } },
        .{ .path = "/live/list.m3u8", .assert_request = Request.initial, .max_uses = 1, .respond = .{ .body = full } },
        .{ .path = "/live/init.mp4", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = bytes[0..797] } },
        .{ .path = "/live/one.m4s", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = bytes[797..11206] } },
        .{ .path = "/live/two.m4s", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = bytes[11206..20811] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/live/list.m3u8?token=stable%26key", .{server.baseUrl()});
    defer a.free(url);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url, .pool = &pool };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{ .clock = .{ .now_ns = Request.now }, .ingest = .{ .stream = .{ .max_bytes = 65536 }, .index = .{ .max_index_bytes = 256 * 1024 } } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 7 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var one = (try reader.next()).?;
    defer one.deinit();
    var held = try one.reader.readPacket(0);
    defer held.deinit();
    const original = try a.dupe(u8, held.bytes);
    defer a.free(original);
    try reader.reloadDelta();
    var two = (try reader.next()).?;
    defer two.deinit();
    try std.testing.expect(two.reader.packets[0].media_dts > one.reader.packets[0].media_dts);
    try std.testing.expect((try reader.next()) == null);
    try reader.reloadDelta(); // Missing skipped segment 12 -> one full retry.
    try std.testing.expectEqual(@as(?u64, 12), reader.next_sequence);
    try std.testing.expectEqual(@as(usize, 4), reader.playlist.?.segments.len);
    try std.testing.expectEqualSlices(u8, original, held.bytes);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 7), transport.requests);
}

test "production HTTP HLS date-range v2 combined reload and full fallback preserve authenticated metadata" {
    const prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,CAN-SKIP-UNTIL=12,CAN-SKIP-DATERANGES=YES\n";
    const first = prefix ++ "#EXT-X-DATERANGE:ID=\"a\",START-DATE=\"2026-01-01T00:00:00Z\",X-NOTE=\"retained\"\n#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00Z\n#EXTINF:2,\none.m4s\n";
    const second = prefix ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=1,RECENTLY-REMOVED-DATERANGES=\"\"\n#EXT-X-DATERANGE:ID=\"a\",DURATION=2\n#EXTINF:2,\ntwo.m4s\n";
    const missing = prefix ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=1\n#EXTINF:2,\ntwo.m4s\n";
    const full = first ++ "#EXTINF:2,\ntwo.m4s\n#EXT-X-ENDLIST\n";
    const Request = struct {
        fn initial(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/dates/list.m3u8?token=stable%26key", request.target);
        }
        fn delta(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/dates/list.m3u8?token=stable%26key&_HLS_skip=v2", request.target);
        }
        fn combined(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings("/dates/list.m3u8?token=stable%26key&_HLS_msn=10&_HLS_skip=v2", request.target);
        }
        fn now(_: ?*anyopaque) u64 {
            return std.time.ns_per_s;
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/dates/list.m3u8", .assert_request = Request.initial, .max_uses = 1, .respond = .{ .body = first } },
        .{ .path = "/dates/list.m3u8", .assert_request = Request.delta, .max_uses = 1, .respond = .{ .body = second } },
        .{ .path = "/dates/list.m3u8", .assert_request = Request.combined, .max_uses = 1, .respond = .{ .body = missing } },
        .{ .path = "/dates/list.m3u8", .assert_request = Request.initial, .max_uses = 1, .respond = .{ .body = full } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/dates/list.m3u8?token=stable%26key", .{server.baseUrl()});
    defer a.free(url);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url, .pool = &pool };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{ .clock = .{ .now_ns = Request.now } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 4 });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    try reader.reloadDelta();
    try std.testing.expectEqualStrings("2", reader.playlist.?.date_ranges[0].duration().?);
    try reader.reloadBlockingDelta();
    try std.testing.expectEqualStrings("retained", reader.playlist.?.date_ranges[0].attributes.get("X-NOTE").?.value);
    try std.testing.expectEqualStrings("2", reader.playlist.?.date_ranges[0].duration().?);
    try std.testing.expect(reader.playlist.?.ended);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 4), transport.requests);
}

test "production HTTP fragment replay survives truncated chunked response without duplicate accepted fragments" {
    const bytes = @embedFile("testdata/fragmented.mp4");
    const State = struct {
        first_delivered: std.Io.Event = .unset,
        new_delivered: std.Io.Event = .unset,
        sent_retry_tail: std.atomic.Value(bool) = .init(false),
        count: usize = 0,
        held: [6]?media.live_mp4.Segment = @splat(null),
        fn serve(self: *@This(), listener: *http.TcpListener) !void {
            for (0..2) |attempt| {
                const accepted = try listener.accept();
                var socket = accepted.socket;
                defer socket.close();
                try socket.setRecvTimeout(5000);
                try socket.setSendTimeout(5000);
                var request: [4096]u8 = undefined;
                var used: usize = 0;
                while (std.mem.indexOf(u8, request[0..used], "\r\n\r\n") == null) {
                    if (used == request.len) return error.TestUnexpectedResult;
                    const n = try socket.recv(request[used..]);
                    if (n == 0) return error.UnexpectedEof;
                    used += n;
                }
                try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "authorization: Bearer fixture-token\r\n") != null);
                try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "accept-encoding: identity\r\n") != null);
                try socket.sendAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
                if (attempt == 0) {
                    try chunk(&socket, bytes[797 .. 11206 + 8]);
                    try self.first_delivered.wait(std.testing.io);
                    // Close without a terminating chunk: the first accepted
                    // fragment survives a genuinely incomplete HTTP response.
                } else {
                    const boundary = 20811 + 8;
                    try chunk(&socket, bytes[797..boundary]);
                    // Retry must suppress the old prefix, then deliver the next
                    // fragment before the server is allowed to send the tail.
                    try self.new_delivered.wait(std.testing.io);
                    self.sent_retry_tail.store(true, .release);
                    try chunk(&socket, bytes[boundary..]);
                    try socket.sendAll("0\r\n\r\n");
                }
            }
        }
        fn chunk(socket: *http.Socket, body: []const u8) !void {
            var header: [32]u8 = undefined;
            try socket.sendAll(try std.fmt.bufPrint(&header, "{x}\r\n", .{body.len}));
            try socket.sendAll(body);
            try socket.sendAll("\r\n");
        }
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count >= self.held.len) return error.TestUnexpectedResult;
            try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
            if (self.count == 1) try std.testing.expect(!self.sent_retry_tail.load(.acquire));
            self.held[self.count] = try delivery.take();
            self.count += 1;
            if (self.count == 1) self.first_delivered.set(std.testing.io);
            if (self.count == 2) self.new_delivered.set(std.testing.io);
        }
    };
    var state = State{};
    defer for (&state.held) |*entry| if (entry.*) |*segment| segment.deinit();
    var listener = try http.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, std.testing.io);
    defer listener.deinit();
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/media.m4s", .{listener.getLocalAddress().getPort()});
    defer a.free(url);
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url, .pool = &pool, .limits = .{ .timeout_ms = 5000 } };
    var ledger = media.http_stream.FragmentLedger{};
    var serving = try std.testing.io.concurrent(State.serve, .{ &state, &listener });
    defer _ = serving.cancel(std.testing.io) catch {};
    try std.testing.expectError(error.UnexpectedEof, transport.streamFmp4Resuming(url, "media.m4s", null, bytes.len, bytes[0..797], &ledger, .{}, &state, State.accept));
    try std.testing.expectEqual(@as(usize, 1), ledger.acknowledged);
    try std.testing.expect(!ledger.invalid and !ledger.complete);
    const result = try transport.streamFmp4Resuming(url, "media.m4s", null, bytes.len, bytes[0..797], &ledger, .{}, &state, State.accept);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), result.replayed);
    try std.testing.expectEqual(@as(usize, 5), result.acknowledged);
    try std.testing.expectEqual(@as(usize, 2), transport.requests);
    try std.testing.expect(ledger.complete);
    for (&state.held) |*entry| {
        var lease = try entry.*.?.reader.readPacket(0);
        try std.testing.expect(lease.bytes.len > 0);
        lease.deinit();
        entry.*.?.deinit();
        entry.* = null;
    }
    try std.testing.expectEqual(@as(usize, 0), pool.snapshot().host_bytes);
}

test "production partial DASH authenticates growing HTTP versions and advances only after native duration" {
    const manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period duration='PT6S'><AdaptationSet><Representation id='v'><SegmentList timescale='1000' duration='6000' availabilityTimeComplete='false'><Initialization sourceURL='init.mp4'/><SegmentURL media='seg.mp4'/></SegmentList></Representation></AdaptationSet></Period></MPD>";
    const original = @embedFile("testdata/fragmented.mp4");
    const Capture = struct {
        count: usize = 0,
        complete: usize = 0,
        held: ?media.live_mp4.Segment = null,
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
            if (delivery.segment.?.dash_timing.?.resource_complete) self.complete += 1;
            if (self.count == 0) self.held = try delivery.take();
            self.count += 1;
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/live/list.mpd", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/live/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[0..797] } },
        .{ .path = "/live/seg.mp4", .assert_request = Verify.auth, .max_uses = 2, .respond = .{ .body = original[797..20811] } },
        .{ .path = "/live/seg.mp4", .assert_request = Verify.auth, .respond = .{ .body = original[797..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/live/list.mpd", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.dash_live.Reader(http).init(a, &transport, url, .{ .selection = .{ .allow_partial_availability = true } });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 5 });
    defer _ = serving.cancel(std.testing.io) catch {};
    var capture = Capture{};
    defer if (capture.held) |*unit| unit.deinit();
    try reader.reload();
    const first = (try reader.streamNext(&capture, Capture.accept)).?;
    try std.testing.expect(!first.resource_complete);
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    try std.testing.expect(!reader.ended());
    const unchanged = (try reader.streamNext(&capture, Capture.accept)).?;
    try std.testing.expect(!unchanged.resource_complete);
    try std.testing.expectEqual(@as(usize, 2), unchanged.replayed);
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    const final = (try reader.streamNext(&capture, Capture.accept)).?;
    try std.testing.expect(final.resource_complete);
    try std.testing.expectEqual(@as(usize, 2), final.replayed);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try std.testing.expectEqual(@as(usize, 1), capture.complete);
    try std.testing.expect(reader.ended());
    try serving.await(std.testing.io);
    var packet = try capture.held.?.reader.readPacket(0);
    defer packet.deinit();
    try std.testing.expect(packet.bytes.len != 0);
}

test "production encrypted HTTP fragment replay publishes before tail and retries truncated ciphertext" {
    const bytes = @embedFile("testdata/hls/aes-stream/media.bin");
    const clear = @embedFile("testdata/fragmented.mp4");
    const key: [16]u8 = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f".*;
    var iv: [16]u8 = @splat(0);
    iv[15] = 10;
    const options = media.http_stream.ResumeOptions{ .encryption = .{ .key = &key, .iv = iv } };
    const State = struct {
        first_delivered: std.Io.Event = .unset,
        new_delivered: std.Io.Event = .unset,
        sent_retry_tail: std.atomic.Value(bool) = .init(false),
        count: usize = 0,
        held: [6]?media.live_mp4.Segment = @splat(null),
        fn serve(self: *@This(), listener: *http.TcpListener) !void {
            for (0..2) |attempt| {
                const accepted = try listener.accept();
                var socket = accepted.socket;
                defer socket.close();
                try socket.setRecvTimeout(5000);
                try socket.setSendTimeout(5000);
                var request: [4096]u8 = undefined;
                var used: usize = 0;
                while (std.mem.indexOf(u8, request[0..used], "\r\n\r\n") == null) {
                    if (used == request.len) return error.TestUnexpectedResult;
                    const n = try socket.recv(request[used..]);
                    if (n == 0) return error.UnexpectedEof;
                    used += n;
                }
                try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "authorization: Bearer fixture-token\r\n") != null);
                try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "accept-encoding: identity\r\n") != null);
                try socket.sendAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
                if (attempt == 0) {
                    try chunk(&socket, bytes[0 .. ((11206 + 8 - 797 + 16 + 15) / 16) * 16]);
                    try self.first_delivered.wait(std.testing.io);
                    // Close without a terminating chunk: the first accepted
                    // fragment survives a genuinely incomplete HTTP response.
                } else {
                    const boundary = ((20811 + 8 - 797 + 16 + 15) / 16) * 16;
                    try chunk(&socket, bytes[0..boundary]);
                    // Retry must suppress the old prefix, then deliver the next
                    // fragment before the server is allowed to send the tail.
                    try self.new_delivered.wait(std.testing.io);
                    self.sent_retry_tail.store(true, .release);
                    try chunk(&socket, bytes[boundary..]);
                    try socket.sendAll("0\r\n\r\n");
                }
            }
        }
        fn chunk(socket: *http.Socket, body: []const u8) !void {
            var header: [32]u8 = undefined;
            try socket.sendAll(try std.fmt.bufPrint(&header, "{x}\r\n", .{body.len}));
            try socket.sendAll(body);
            try socket.sendAll("\r\n");
        }
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count >= self.held.len) return error.TestUnexpectedResult;
            try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
            if (self.count == 1) try std.testing.expect(!self.sent_retry_tail.load(.acquire));
            self.held[self.count] = try delivery.take();
            self.count += 1;
            if (self.count == 1) self.first_delivered.set(std.testing.io);
            if (self.count == 2) self.new_delivered.set(std.testing.io);
        }
    };
    var state = State{};
    defer for (&state.held) |*entry| if (entry.*) |*segment| segment.deinit();
    var listener = try http.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, std.testing.io);
    defer listener.deinit();
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/media.m4s", .{listener.getLocalAddress().getPort()});
    defer a.free(url);
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url, .pool = &pool, .limits = .{ .timeout_ms = 5000 } };
    var ledger = media.http_stream.FragmentLedger{};
    var serving = try std.testing.io.concurrent(State.serve, .{ &state, &listener });
    defer _ = serving.cancel(std.testing.io) catch {};
    try std.testing.expectError(error.UnexpectedEof, transport.streamFmp4Resuming(url, "media.m4s", null, bytes.len, clear[0..797], &ledger, options, &state, State.accept));
    try std.testing.expectEqual(@as(usize, 1), ledger.acknowledged);
    try std.testing.expect(!ledger.invalid and !ledger.complete);
    const result = try transport.streamFmp4Resuming(url, "media.m4s", null, bytes.len, clear[0..797], &ledger, options, &state, State.accept);
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), result.replayed);
    try std.testing.expectEqual(@as(usize, 5), result.acknowledged);
    try std.testing.expectEqual(@as(usize, 2), transport.requests);
    try std.testing.expect(ledger.complete);
    try std.testing.expectEqual(bytes.len, result.transport.bytes);
    try std.testing.expectEqual(clear.len - 797, result.transport.plaintext_bytes);
    for (&state.held) |*entry| {
        var lease = try entry.*.?.reader.readPacket(0);
        try std.testing.expect(lease.bytes.len > 0);
        lease.deinit();
        entry.*.?.deinit();
        entry.* = null;
    }
    try std.testing.expectEqual(@as(usize, 0), pool.snapshot().host_bytes);
}

test "production HLS AES streaming authenticates keys decrypts exact ranges and rotates retained packets" {
    const key1 = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";
    const key2 = "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f";
    const init = @embedFile("testdata/hls/aes/init1.bin");
    const first = @embedFile("testdata/hls/aes/media10.bin");
    const second = @embedFile("testdata/hls/aes/media11.bin");
    const total = first.len + second.len;
    const manifest = std.fmt.comptimePrint("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\",IV=0x1\n#EXT-X-MAP:URI=\"init\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:{d}@0\nblob\n#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n#EXTINF:1,\n#EXT-X-BYTERANGE:{d}\nblob\n#EXT-X-ENDLIST\n", .{ first.len, second.len });
    const Ranges = struct {
        fn firstRange(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings(std.fmt.comptimePrint("bytes=0-{d}", .{first.len - 1}), request.header("Range").?);
        }
        fn secondRange(request: http.testing_mod.RequestInfo) !void {
            try Verify.auth(request);
            try std.testing.expectEqualStrings(std.fmt.comptimePrint("bytes={d}-{d}", .{ first.len, total - 1 }), request.header("Range").?);
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/aes/list.m3u8", .assert_request = Verify.auth, .respond = .{ .body = manifest } },
        .{ .path = "/aes/key1", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = key1 } },
        .{ .path = "/aes/key2", .assert_request = Verify.auth, .max_uses = 1, .respond = .{ .body = key2 } },
        .{ .path = "/aes/init", .assert_request = Verify.auth, .respond = .{ .body = init } },
        .{ .path = "/aes/blob", .assert_request = Ranges.firstRange, .max_uses = 1, .respond = .{ .status = 206, .headers = &.{.{ .name = "Content-Range", .value = std.fmt.comptimePrint("bytes 0-{d}/{d}", .{ first.len - 1, total }) }}, .body = first } },
        .{ .path = "/aes/blob", .assert_request = Ranges.secondRange, .max_uses = 1, .respond = .{ .status = 206, .headers = &.{.{ .name = "Content-Range", .value = std.fmt.comptimePrint("bytes {d}-{d}/{d}", .{ first.len, total - 1, total }) }}, .body = second } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/aes/list.m3u8", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{});
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{ &server, 6 });
    defer _ = serving.cancel(std.testing.io) catch {};
    const Capture = struct {
        unit: ?media.live_mp4.Segment = null,
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.unit != null) return error.TestUnexpectedResult;
            try std.testing.expect(delivery.body_complete);
            self.unit = try delivery.take();
        }
        fn take(self: *@This()) media.live_mp4.Segment {
            const value = self.unit.?;
            self.unit = null;
            return value;
        }
    };
    var capture = Capture{};
    defer if (capture.unit) |*unit| unit.deinit();
    try reader.reload();
    _ = try reader.streamNext(&capture, Capture.accept);
    var one = capture.take();
    defer one.deinit();
    const original = @embedFile("testdata/fragmented.mp4");
    try std.testing.expectEqualSlices(u8, original[0..11206], one.backing.bytes);
    var held = try one.reader.readPacket(0);
    defer held.deinit();
    const retained = try a.dupe(u8, held.bytes);
    defer a.free(retained);
    _ = try reader.streamNext(&capture, Capture.accept);
    var two = capture.take();
    defer two.deinit();
    try std.testing.expectEqual(@as(usize, 0), two.epoch);
    try std.testing.expectEqualSlices(u8, retained, held.bytes);
    try std.testing.expect(reader.ended());
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 6), transport.requests);
}

test "production LL HLS streamed parent verifies repackaged sample prefix before authenticated withheld tail" {
    const prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-PART-INF:PART-TARGET=0.5\n#EXT-X-MAP:URI=\"init.mp4\"\n";
    const first_manifest = prefix ++ "#EXT-X-PART:DURATION=0.5,URI=\"p0.m4s\",INDEPENDENT=YES\n";
    const parent_manifest = prefix ++ "#EXTINF:6,\nparent.m4s\n#EXT-X-ENDLIST\n";
    const initial = @embedFile("testdata/fragmented.mp4")[0..797];
    const part = @embedFile("testdata/hls/ll-part-half.m4s");
    const parent = @embedFile("testdata/hls/ll-parent-2s.mp4");
    const State = struct {
        release: std.Io.Event = .unset,
        sent_tail: std.atomic.Value(bool) = .init(false),
        count: usize = 0,
        samples: usize = 0,
        held: [4]?media.live_mp4.Segment = @splat(null),
        fn serve(self: *@This(), listener: *http.TcpListener) !void {
            for (0..5) |index| {
                const accepted = try listener.accept();
                var socket = accepted.socket;
                defer socket.close();
                try socket.setRecvTimeout(5000);
                try socket.setSendTimeout(5000);
                var request: [4096]u8 = undefined;
                var used: usize = 0;
                while (std.mem.indexOf(u8, request[0..used], "\r\n\r\n") == null) {
                    if (used == request.len) return error.TestUnexpectedResult;
                    const n = try socket.recv(request[used..]);
                    if (n == 0) return error.UnexpectedEof;
                    used += n;
                }
                try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "authorization: Bearer fixture-token\r\n") != null);
                try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "accept-encoding: identity\r\n") != null);
                const path = switch (index) {
                    0, 3 => "GET /live/list.m3u8 ",
                    1 => "GET /live/init.mp4 ",
                    2 => "GET /live/p0.m4s ",
                    else => "GET /live/parent.m4s ",
                };
                try std.testing.expect(std.mem.startsWith(u8, request[0..used], path));
                if (index < 4) {
                    const body = switch (index) {
                        0 => first_manifest,
                        1 => initial,
                        2 => part,
                        else => parent_manifest,
                    };
                    var header: [128]u8 = undefined;
                    try socket.sendAll(try std.fmt.bufPrint(&header, "HTTP/1.1 200 OK\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len}));
                    try socket.sendAll(body);
                } else {
                    try socket.sendAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
                    const boundary = 20775 + 8;
                    try chunk(&socket, parent[797..boundary]);
                    try self.release.wait(std.testing.io);
                    self.sent_tail.store(true, .release);
                    try chunk(&socket, parent[boundary..]);
                    try socket.sendAll("0\r\n\r\n");
                }
            }
        }
        fn chunk(socket: *http.Socket, bytes: []const u8) !void {
            var header: [32]u8 = undefined;
            try socket.sendAll(try std.fmt.bufPrint(&header, "{x}\r\n", .{bytes.len}));
            try socket.sendAll(bytes);
            try socket.sendAll("\r\n");
        }
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
            if (self.count == 0) {
                try std.testing.expectEqual(@as(usize, 5), delivery.segment.?.reader.packets.len);
                try std.testing.expectEqual(@as(?u32, 0), delivery.segment.?.hls_timing.?.part_index);
            } else if (self.count == 1) {
                try std.testing.expect(!self.sent_tail.load(.acquire));
                try std.testing.expectEqual(@as(usize, 15), delivery.segment.?.reader.packets.len);
                try std.testing.expectEqual(@as(i64, 5120), delivery.segment.?.reader.packets[0].media_dts);
                self.release.set(std.testing.io);
            }
            self.samples += delivery.segment.?.reader.packets.len;
            self.held[self.count] = try delivery.take();
            self.count += 1;
        }
    };
    var state = State{};
    defer for (&state.held) |*entry| if (entry.*) |*unit| unit.deinit();
    var listener = try http.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, std.testing.io);
    defer listener.deinit();
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/live/list.m3u8", .{listener.getLocalAddress().getPort()});
    defer a.free(url);
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url, .limits = .{ .timeout_ms = 5000 } };
    var reader = try media.hls_live.Reader(http).init(a, &transport, url, .{});
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(State.serve, .{ &state, &listener });
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    _ = try reader.streamNext(&state, State.accept);
    try reader.reload();
    const result = (try reader.streamNext(&state, State.accept)).?;
    try serving.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 5), result.prefix_samples_skipped);
    try std.testing.expectEqual(@as(usize, 3), result.acknowledged);
    try std.testing.expectEqual(@as(usize, 60), state.samples);
    try std.testing.expectEqual(@as(usize, 4), state.count);
    try std.testing.expect(reader.ended());
    var original_source = media.source.Source{ .allocator = a, .identity = "ll-http-native-parity", .storage = .{ .borrowed = @embedFile("testdata/fragmented.mp4") } };
    var reference = try media.mp4.Reader.init(a, &original_source, .{});
    defer reference.deinit();
    var cursor: usize = 0;
    for (state.held) |entry| {
        var unit = entry.?;
        for (unit.reader.packets, 0..) |packet, local| {
            try std.testing.expectEqual(reference.packets[cursor].media_dts, packet.media_dts);
            try std.testing.expectEqual(reference.packets[cursor].media_pts, packet.media_pts);
            var left = try reference.readPacket(cursor);
            defer left.deinit();
            var right = try unit.reader.readPacket(local);
            defer right.deinit();
            try std.testing.expectEqualSlices(u8, left.bytes, right.bytes);
            cursor += 1;
        }
    }
}

test "production HTTP incremental TS publishes native PES before authenticated response tail" {
    const bytes = @embedFile("testdata/transport/closed.ts");
    const Capture = struct {
        expected: *media.transport_stream.Demux,
        release: std.Io.Event = .unset,
        tail: std.atomic.Value(bool) = .init(false),
        count: usize = 0,
        fn accept(raw: *anyopaque, pes: media.transport_stream.Pes) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const expected = self.expected.packets[self.count];
            try std.testing.expectEqualDeep(expected, pes.packet);
            try std.testing.expectEqual(self.count, pes.ordinal);
            try std.testing.expectEqualSlices(u8, self.expected.bytes[expected.offset..][0..expected.size], pes.bytes);
            if (self.count == 0) {
                try std.testing.expect(!self.tail.load(.acquire));
                self.release.set(std.testing.io);
            }
            self.count += 1;
        }
        fn serve(self: *@This(), listener: *http.TcpListener) !void {
            const accepted = try listener.accept();
            var socket = accepted.socket;
            defer socket.close();
            try socket.setRecvTimeout(5000);
            try socket.setSendTimeout(5000);
            var request: [4096]u8 = undefined;
            var used: usize = 0;
            while (std.mem.indexOf(u8, request[0..used], "\r\n\r\n") == null) {
                if (used == request.len) return error.TestUnexpectedResult;
                const n = try socket.recv(request[used..]);
                if (n == 0) return error.UnexpectedEof;
                used += n;
            }
            try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "authorization: Bearer fixture-token\r\n") != null);
            try std.testing.expect(std.ascii.findIgnoreCase(request[0..used], "accept-encoding: identity\r\n") != null);
            var starts: usize = 0;
            var boundary: usize = 0;
            // Two AVC PES starts delimit the first complete PES. Only the
            // header namespace is inspected here; demux checks PAT/PMT/CRC.
            var offset: usize = 0;
            while (offset < bytes.len) : (offset += 188) {
                const pid = (@as(u16, bytes[offset + 1] & 31) << 8) | bytes[offset + 2];
                if (pid == self.expected.next_state.video_pid.? and bytes[offset + 1] & 0x40 != 0) {
                    starts += 1;
                    if (starts == 2) {
                        boundary = offset + 188;
                        break;
                    }
                }
            }
            try std.testing.expect(boundary > 0 and boundary < bytes.len);
            try socket.sendAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
            try chunk(&socket, bytes[0..boundary]);
            try self.release.wait(std.testing.io);
            self.tail.store(true, .release);
            try chunk(&socket, bytes[boundary..]);
            try socket.sendAll("0\r\n\r\n");
        }
        fn chunk(socket: *http.Socket, body: []const u8) !void {
            var header: [32]u8 = undefined;
            try socket.sendAll(try std.fmt.bufPrint(&header, "{x}\r\n", .{body.len}));
            try socket.sendAll(body);
            try socket.sendAll("\r\n");
        }
    };
    const Sink = struct {
        stream: *media.transport_stream.Stream,
        started: bool = false,
        pub fn startResponse(self: *@This(), response: anytype) !void {
            try std.testing.expectEqual(@as(u16, 200), response.status.code);
            self.started = true;
        }
        pub fn writeAll(self: *@This(), input: []const u8) !void {
            if (!self.started) return error.UnexpectedHttpStatus;
            try self.stream.push(input);
        }
    };
    var expected = try media.transport_stream.demux(a, bytes, .{}, .{}, .{}, null);
    defer expected.deinit();
    var capture = Capture{ .expected = &expected };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    var stream = try media.transport_stream.Stream.init(a, .{}, .{}, .{}, &pool, .{ .context = &capture, .accept = Capture.accept });
    defer stream.deinit();
    var sink = Sink{ .stream = &stream };
    var listener = try http.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, std.testing.io);
    defer listener.deinit();
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/stream.ts", .{listener.getLocalAddress().getPort()});
    defer a.free(url);
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    var serving = try std.testing.io.concurrent(Capture.serve, .{ &capture, &listener });
    defer _ = serving.cancel(std.testing.io) catch {};
    var response = try client.getToWriter(url, .{ .headers = &.{.{ "Accept-Encoding", "identity" }}, .max_response_size = bytes.len, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false, .timeout_ms = 5000 }, &sink, null, null);
    defer response.deinit();
    const state = try stream.finish();
    try serving.await(std.testing.io);
    try std.testing.expectEqualDeep(expected.next_state, state);
    try std.testing.expectEqual(expected.packets.len, capture.count);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}
