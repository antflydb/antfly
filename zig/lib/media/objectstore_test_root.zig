// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const store = @import("objectstore");
const Adapter = media.objectstore.Adapter(store);
const alloc = std.testing.allocator;
fn header(headers: []const [2][]const u8, name: []const u8) ![]const u8 {
    for (headers) |pair| if (std.ascii.eqlIgnoreCase(pair[0], name)) return pair[1];
    return error.MissingHeader;
}
test "S3 authenticated media ranges sign version and range with no metadata probe" {
    const Fake = struct {
        status: u16 = 206,
        version: []const u8 = "v1",
        offset: u64 = 2,
        calls: usize = 0,
        fn request(raw: ?*anyopaque, a: std.mem.Allocator, method: store.s3.HttpMethod, url: []const u8, headers: []const store.s3.HeaderPair, _: ?[]const u8, _: ?[]const u8, cap: ?usize, cancellation: ?store.CancellationToken) !store.s3.TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try cancellation.?.check();
            try std.testing.expectEqual(store.s3.HttpMethod.GET, method);
            try std.testing.expect(std.mem.endsWith(u8, url, "/bucket/folder/key?versionId=v1"));
            try std.testing.expectEqualStrings("bytes=2-5", try header(headers, "Range"));
            try std.testing.expect(std.mem.startsWith(u8, try header(headers, "Authorization"), "AWS4-HMAC-SHA256 "));
            try std.testing.expectEqual(@as(?usize, 4), cap);
            self.calls += 1;
            const body = try a.dupe(u8, "cdef");
            errdefer a.free(body);
            return .{ .status = self.status, .body = body, .version_id = try a.dupe(u8, self.version), .content_range = .{ .offset = self.offset, .length = 4, .total = 16 } };
        }
    };
    var fake = Fake{};
    var client = store.s3.Client.initWithRequestFn(alloc, .{ .credentials = .{ .endpoint = try alloc.dupe(u8, "s3.example.test"), .region = try alloc.dupe(u8, "us-east-1"), .access_key_id = try alloc.dupe(u8, "test-access"), .secret_access_key = try alloc.dupe(u8, "test-secret") }, .addressing_style = .path }, &fake, Fake.request);
    defer client.deinit();
    var adapter = Adapter.borrow(alloc, client.client(), "bucket", "folder/key", "v1", .s3_version_id, 16);
    var input = media.source.Source{ .allocator = alloc, .identity = "v1", .storage = .{ .range = try adapter.range() } };
    var lease = try input.read(2, 4);
    try std.testing.expectEqualStrings("cdef", lease.bytes);
    lease.deinit();
    fake.status = 200;
    try std.testing.expectError(error.InvalidRemoteRange, input.read(2, 4));
    fake.status = 206;
    fake.version = "v2";
    try std.testing.expectError(error.SourceVersionChanged, input.read(2, 4));
    fake.version = "v1";
    fake.offset = 3;
    try std.testing.expectError(error.InvalidRemoteRange, input.read(2, 4));
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    try std.testing.expectEqual(@as(usize, 0), input.retained_bytes);
}
test "GCS authenticated media ranges pin generation and propagate cancellation" {
    const Fake = struct {
        fn request(_: ?*anyopaque, a: std.mem.Allocator, method: store.gcs.HttpMethod, url: []const u8, headers: []const store.gcs.HeaderPair, _: ?[]const u8, _: ?[]const u8, cap: ?usize, cancellation: ?store.CancellationToken) !store.gcs.TransportResponse {
            try cancellation.?.check();
            try std.testing.expectEqual(store.gcs.HttpMethod.GET, method);
            try std.testing.expect(std.mem.endsWith(u8, url, "/b/bucket/o/folder%2Fkey?generation=42&alt=media"));
            try std.testing.expectEqualStrings("Bearer fixture-token", try header(headers, "Authorization"));
            try std.testing.expectEqualStrings("bytes=2-5", try header(headers, "Range"));
            try std.testing.expectEqual(@as(?usize, 4), cap);
            const body = try a.dupe(u8, "cdef");
            errdefer a.free(body);
            return .{ .status = 206, .body = body, .generation = try a.dupe(u8, "42"), .content_range = .{ .offset = 2, .length = 4, .total = 16 } };
        }
    };
    var client = store.gcs.JsonApiClient.initWithRequestFn(alloc, .{ .endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/storage/v1"), .upload_endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/upload/storage/v1"), .auth = .{ .bearer_token = try alloc.dupe(u8, "fixture-token") } }, null, Fake.request);
    defer client.deinit();
    var adapter = Adapter.borrow(alloc, client.client(), "bucket", "folder/key", "42", .gcs_generation, 16);
    const range = try adapter.range();
    var bytes: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try range.read_at(range.context, 2, &bytes, .{}));
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, range.read_at(range.context, 2, &bytes, .{ .check_fn = Cancel.check }));
}
test "owned native authenticated clients initialize and release without network requests" {
    const s3cfg = store.s3.Config{ .credentials = .{ .endpoint = try alloc.dupe(u8, "s3.example.test"), .region = try alloc.dupe(u8, "us-east-1"), .access_key_id = try alloc.dupe(u8, "fixture-access"), .secret_access_key = try alloc.dupe(u8, "fixture-secret") }, .io = std.testing.io };
    const s3 = try Adapter.createS3(alloc, s3cfg, "bucket", "key", "v1", .s3_version_id, 16);
    s3.destroy();
    const gcscfg = store.gcs.JsonApiConfig{ .endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/storage/v1"), .upload_endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/upload/storage/v1"), .auth = .{ .bearer_token = try alloc.dupe(u8, "fixture-token") }, .io = std.testing.io };
    const gcs = try Adapter.createGcs(alloc, gcscfg, "bucket", "key", "42", 16);
    gcs.destroy();
}

test "native HTTP media adapters validate actual range headers and credentials" {
    const httpx = @import("httpx");
    const Verify = struct {
        fn s3(request: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("versionId=v1", request.query);
            try std.testing.expectEqualStrings("bytes=2-5", request.header("Range").?);
            try std.testing.expect(std.mem.startsWith(u8, request.header("Authorization").?, "AWS4-HMAC-SHA256 "));
        }
        fn gcs(request: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("generation=42&alt=media", request.query);
            try std.testing.expectEqualStrings("bytes=2-5", request.header("Range").?);
            try std.testing.expectEqualStrings("Bearer fixture-token", request.header("Authorization").?);
        }
    };
    const headers = [_]httpx.testing_mod.HeaderPair{ .{ .name = "Content-Range", .value = "bytes 2-5/16" }, .{ .name = "x-amz-version-id", .value = "v1" }, .{ .name = "x-goog-generation", .value = "42" } };
    for ([_]bool{ false, true }) |gcs_provider| {
        var server = try httpx.testing_mod.TestServer.start(alloc, std.testing.io, &.{.{
            .path = if (gcs_provider) "/storage/v1/b/bucket/o/folder%2Fkey" else "/bucket/folder/key",
            .assert_request = if (gcs_provider) Verify.gcs else Verify.s3,
            .respond = .{ .status = 206, .headers = &headers, .body = "cdef" },
        }});
        defer server.deinit();
        const adapter = if (gcs_provider) try Adapter.createGcs(alloc, .{
            .endpoint = try std.fmt.allocPrint(alloc, "{s}/storage/v1", .{server.baseUrl()}),
            .upload_endpoint = try std.fmt.allocPrint(alloc, "{s}/upload/storage/v1", .{server.baseUrl()}),
            .auth = .{ .bearer_token = try alloc.dupe(u8, "fixture-token") },
            .io = std.testing.io,
        }, "bucket", "folder/key", "42", 16) else try Adapter.createS3(alloc, .{
            .credentials = .{ .endpoint = try alloc.dupe(u8, server.baseUrl()[7..]), .region = try alloc.dupe(u8, "us-east-1"), .access_key_id = try alloc.dupe(u8, "fixture-access"), .secret_access_key = try alloc.dupe(u8, "fixture-secret"), .use_ssl = false },
            .addressing_style = .path,
            .io = std.testing.io,
        }, "bucket", "folder/key", "v1", .s3_version_id, 16);
        defer adapter.destroy();
        var serving = try std.testing.io.concurrent(httpx.testing_mod.TestServer.handleOne, .{&server});
        defer _ = serving.cancel(std.testing.io) catch {};
        var input = media.source.Source{ .allocator = alloc, .identity = "http-test", .storage = .{ .range = try adapter.range() } };
        var lease = try input.read(2, 4);
        defer lease.deinit();
        try std.testing.expectEqualStrings("cdef", lease.bytes);
        try serving.await(std.testing.io);
    }
}

test "S3 strong ETag pinning rejects weak validators and malformed ranges" {
    const Fake = struct {
        etag: []const u8 = "\"etag-1\"",
        wire: ?store.types.ContentRange = .{ .offset = 2, .length = 4, .total = 16 },
        fn request(raw: ?*anyopaque, a: std.mem.Allocator, _: store.s3.HttpMethod, _: []const u8, headers: []const store.s3.HeaderPair, _: ?[]const u8, _: ?[]const u8, _: ?usize, _: ?store.CancellationToken) !store.s3.TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings("\"etag-1\"", try header(headers, "If-Match"));
            const body = try a.dupe(u8, "cdef");
            errdefer a.free(body);
            return .{ .status = 206, .body = body, .etag = try a.dupe(u8, self.etag), .content_range = self.wire };
        }
    };
    var fake = Fake{};
    var client = store.s3.Client.initWithRequestFn(alloc, .{ .credentials = .{ .endpoint = try alloc.dupe(u8, "s3.example.test"), .region = try alloc.dupe(u8, "us-east-1"), .access_key_id = try alloc.dupe(u8, "test-access"), .secret_access_key = try alloc.dupe(u8, "test-secret") }, .addressing_style = .path }, &fake, Fake.request);
    defer client.deinit();
    var adapter = Adapter.borrow(alloc, client.client(), "bucket", "key", "\"etag-1\"", .strong_etag, 16);
    var input = media.source.Source{ .allocator = alloc, .identity = "etag-1", .storage = .{ .range = try adapter.range() } };
    var lease = try input.read(2, 4);
    try std.testing.expectEqualStrings("cdef", lease.bytes);
    lease.deinit();
    for ([_][]const u8{ "W/\"etag-1\"", "etag-1", "\"etag-2\"" }) |etag| {
        fake.etag = etag;
        try std.testing.expectError(error.SourceVersionChanged, input.read(2, 4));
        try std.testing.expectEqual(@as(usize, 0), input.retained_bytes);
    }
    fake.etag = "\"etag-1\"";
    fake.wire = null;
    try std.testing.expectError(error.InvalidRemoteRange, input.read(2, 4));
    fake.wire = .{ .offset = 2, .length = 5, .total = 16 };
    try std.testing.expectError(error.InvalidRemoteRange, input.read(2, 4));
    fake.wire = .{ .offset = 2, .length = 4, .total = 17 };
    try std.testing.expectError(error.InvalidRemoteRange, input.read(2, 4));
    try std.testing.expectEqual(@as(usize, 0), input.retained_bytes);
}
