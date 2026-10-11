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

test "production DASH SegmentList uses authenticated fMP4 transport and owned packets" {
    const manifest = "<?xml version='1.0'?><MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period><AdaptationSet mimeType='video/mp4'><Representation id='v'><SegmentList duration='2'><Initialization sourceURL='init.mp4'/><SegmentURL media='seg.m4s'/></SegmentList></Representation></AdaptationSet></Period></MPD>";
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
        .{ .path = "/dash/seg.m4s", .assert_request = Verify.auth, .respond = .{ .body = original[offset..] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/dash/manifest.mpd", .{server.baseUrl()});
    defer a.free(url);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = url };
    var reader = try media.dash_live.Reader(http).init(a, &transport, url, .{});
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
