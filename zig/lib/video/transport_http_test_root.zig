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
const http = @import("httpx");
const media = @import("antfly_media");
const video = @import("antfly_video");
test "production HTTP split-PES TS HLS authenticates playlist and media then decodes native AVC" {
    const a = std.testing.allocator;
    const Verify = struct {
        fn auth(request: http.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer fixture-token", request.header("Authorization").?);
            try std.testing.expectEqualStrings("identity", request.header("Accept-Encoding").?);
        }
        fn serve(server: *http.testing_mod.TestServer) !void {
            try server.handleOne();
            try server.handleOne();
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/ts/list.m3u8", .assert_request = Verify.auth, .respond = .{ .body = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:8\n#EXTINF:4,\nclosed.ts\n#EXT-X-ENDLIST\n" } },
        .{ .path = "/ts/closed.ts", .assert_request = Verify.auth, .respond = .{ .body = @embedFile("testdata/transport/closed-split.ts") } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const uri = try std.fmt.allocPrint(a, "{s}/ts/list.m3u8", .{server.baseUrl()});
    defer a.free(uri);
    var codec = video.transport_avc.Hls{ .allocator = a };
    defer codec.deinit();
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = uri };
    var reader = try media.hls_live.Reader(http).init(a, &transport, uri, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{&server});
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    var segment = (try reader.next()).?;
    defer segment.deinit();
    try std.testing.expect(segment.origin == .normalized_transport);
    const session = try video.h264_dynamic.Session.init(a, &segment.reader, .{});
    defer session.deinit();
    const Expected = struct {
        fn accept(_: *anyopaque, _: usize, frame: *const video.h264.Frame) !void {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(frame.nv12, &hash, .{});
            const oracle = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("testdata/transport/closed.json"), .{});
            defer oracle.deinit();
            try std.testing.expectEqual(@as(u32, @intCast(oracle.value.object.get("width").?.integer)), frame.width);
            try std.testing.expectEqual(@as(u32, @intCast(oracle.value.object.get("height").?.integer)), frame.height);
            try std.testing.expectEqualStrings(oracle.value.object.get("decoded_sha256").?.array.items[0].string, &std.fmt.bytesToHex(hash, .lower));
        }
    };
    var unused: u8 = 0;
    _ = try session.decodeSelected(&.{0}, &unused, Expected.accept);
    try std.testing.expect(reader.ended());
    try std.testing.expectEqual(@as(usize, 2), transport.requests);
    try serving.await(std.testing.io);
}

test "production HTTP live coordinator reloads authenticated HLS and commits derived windows" {
    const a = std.testing.allocator;
    const file = @embedFile("testdata/live/fragmented.mp4");
    const initial = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\np0.m4s\n";
    const final = initial ++ "#EXTINF:1,\np1.m4s\n#EXT-X-ENDLIST\n";
    const Verify = struct {
        fn auth(request: http.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer fixture-token", request.header("Authorization").?);
            try std.testing.expectEqualStrings("identity", request.header("Accept-Encoding").?);
        }
        fn serve(server: *http.testing_mod.TestServer) !void {
            for (0..5) |_| try server.handleOne();
        }
    };
    var server = try http.testing_mod.TestServer.start(a, std.testing.io, &.{
        .{ .path = "/live/list.m3u8", .max_uses = 1, .assert_request = Verify.auth, .respond = .{ .body = initial } },
        .{ .path = "/live/init.mp4", .assert_request = Verify.auth, .respond = .{ .body = file[0..797] } },
        .{ .path = "/live/p0.m4s", .assert_request = Verify.auth, .respond = .{ .body = file[797..11206] } },
        .{ .path = "/live/list.m3u8", .max_uses = 1, .assert_request = Verify.auth, .respond = .{ .body = final } },
        .{ .path = "/live/p1.m4s", .assert_request = Verify.auth, .respond = .{ .body = file[11206..20811] } },
    });
    defer server.deinit();
    var client = http.Client.initWithConfig(a, std.testing.io, .{ .default_headers = &.{.{ "Authorization", "Bearer fixture-token" }} });
    defer client.deinit();
    const uri = try std.fmt.allocPrint(a, "{s}/live/list.m3u8", .{server.baseUrl()});
    defer a.free(uri);
    var transport = media.http_stream.Adapter(http){ .allocator = a, .client = &client, .root_url = uri };
    var reader = try media.hls_live.Reader(http).init(a, &transport, uri, .{});
    defer reader.deinit();
    var flow = try video.live_coordinator.Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s });
    defer flow.deinit();
    var serving = try std.testing.io.concurrent(Verify.serve, .{&server});
    defer _ = serving.cancel(std.testing.io) catch {};
    try reader.reload();
    try std.testing.expect((try flow.pull(&reader)) == null);
    var one = (try flow.reload(&reader, false)).?;
    defer one.deinit();
    try flow.commit(&one);
    var tail = (try flow.pull(&reader)).?;
    defer tail.deinit();
    try std.testing.expectEqual(one.source_end_ns, tail.source_start_ns);
    try std.testing.expectEqual(@as(usize, 1), tail.ordinal);
    try flow.commit(&tail);
    try std.testing.expect((try flow.pull(&reader)) == null);
    try std.testing.expectEqual(@as(usize, 5), transport.requests);
    try serving.await(std.testing.io);
}
