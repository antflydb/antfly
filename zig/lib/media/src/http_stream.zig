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

//! Bounded same-origin HTTP reads for playlists, initialization and media.
//! Instantiate with lib/httpx; runtime transport and authentication are native.
const std = @import("std");
const source = @import("source.zig");
const admission = @import("admission.zig");
const hls = @import("hls.zig");
const live = @import("live_mp4.zig");
/// Callback-scoped owner. Take transfers an independent immutable snapshot;
/// otherwise it is destroyed when the callback returns, including on failure.
pub const FragmentDelivery = struct {
    segment: ?live.Segment,
    pub fn take(self: *FragmentDelivery) !live.Segment {
        const result = self.segment orelse return error.FragmentAlreadyTaken;
        self.segment = null;
        return result;
    }
};
pub const StreamStats = struct { bytes: usize = 0, fragments: usize = 0, largest_pending_capacity: usize = 0 };
pub const Limits = struct {
    max_resource_bytes: usize = 32 * 1024 * 1024,
    max_transferred_bytes: u64 = 1024 * 1024 * 1024,
    max_requests: usize = 100_000,
    max_uri_bytes: usize = 4096,
    timeout_ms: u64 = 30_000,
};
pub const Body = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    reservation: admission.Token = .{},
    sensitive: bool = false,
    pub fn wipe(self: *Body) void {
        if (self.sensitive) std.crypto.secureZero(u8, self.bytes);
    }
    pub fn deinit(self: *Body) void {
        self.wipe();
        self.allocator.free(self.bytes);
        self.reservation.deinit();
        self.* = undefined;
    }
};
fn component(value: std.Uri.Component) []const u8 {
    return switch (value) {
        .raw => |v| v,
        .percent_encoded => |v| v,
    };
}
fn validate(uri: std.Uri) !void {
    if ((!std.ascii.eqlIgnoreCase(uri.scheme, "http") and !std.ascii.eqlIgnoreCase(uri.scheme, "https")) or uri.host == null or uri.user != null or uri.password != null or uri.fragment != null) return error.InvalidMediaUrl;
}
fn port(uri: std.Uri) u16 {
    return uri.port orelse if (std.ascii.eqlIgnoreCase(uri.scheme, "https")) @as(u16, 443) else 80;
}
fn sameOrigin(left: std.Uri, right: std.Uri) bool {
    return std.ascii.eqlIgnoreCase(left.scheme, right.scheme) and std.ascii.eqlIgnoreCase(component(left.host.?), component(right.host.?)) and port(left) == port(right);
}
/// Owned resolved URL. Root pins the credential origin; every relative or
/// absolute reference is checked, including network-path references.
pub fn resolve(a: std.mem.Allocator, root: []const u8, base: []const u8, reference: []const u8, max_bytes: usize) ![]u8 {
    if (root.len > max_bytes or base.len > max_bytes or reference.len > max_bytes) return error.ResourceLimitExceeded;
    if (reference.len == 0) return error.InvalidMediaUrl;
    for (reference) |byte| if (byte <= 0x20 or byte == 0x7f or byte == '\\') return error.InvalidMediaUrl;
    const origin = try std.Uri.parse(root);
    try validate(origin);
    const base_uri = try std.Uri.parse(base);
    try validate(base_uri);
    if (!sameOrigin(origin, base_uri)) return error.MediaOriginChanged;
    const capacity = try std.math.add(usize, try std.math.mul(usize, max_bytes, 3), 32);
    const storage = try a.alloc(u8, capacity);
    defer a.free(storage);
    @memcpy(storage[0..reference.len], reference);
    var remaining = storage;
    const resolved = try std.Uri.resolveInPlace(base_uri, reference.len, &remaining);
    try validate(resolved);
    if (!sameOrigin(origin, resolved)) return error.MediaOriginChanged;
    const text = try std.fmt.allocPrint(a, "{f}", .{resolved});
    errdefer a.free(text);
    if (text.len > max_bytes) return error.ResourceLimitExceeded;
    return text;
}
fn validateRange(value: []const u8, requested: hls.Range) !void {
    if (!std.mem.startsWith(u8, value, "bytes ")) return error.InvalidRemoteRange;
    var sections = std.mem.splitScalar(u8, value[6..], '/');
    const interval = sections.next().?;
    const total = std.fmt.parseInt(u64, sections.next() orelse return error.InvalidRemoteRange, 10) catch return error.InvalidRemoteRange;
    if (sections.next() != null) return error.InvalidRemoteRange;
    var endpoints = std.mem.splitScalar(u8, interval, '-');
    const first = std.fmt.parseInt(u64, endpoints.next().?, 10) catch return error.InvalidRemoteRange;
    const last = std.fmt.parseInt(u64, endpoints.next() orelse return error.InvalidRemoteRange, 10) catch return error.InvalidRemoteRange;
    if (endpoints.next() != null or first != requested.offset or last < first or last >= total or requested.length != last - first + 1) return error.InvalidRemoteRange;
}

/// Single consumer. Borrows a production httpx client and credential-origin URL.
/// Client default headers may supply authentication; redirects, ambient cookies
/// and automatic retries are disabled so per-adapter work limits stay explicit.
pub fn Adapter(comptime http: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        client: *http.Client,
        root_url: []const u8,
        limits: Limits = .{},
        control: source.Control = .{},
        pool: ?*admission.Pool = null,
        requests: usize = 0,
        charged_bytes: u64 = 0,
        transferred_bytes: u64 = 0,
        const Cancel = struct {
            control: source.Control,
            fn check(raw: *const anyopaque) bool {
                const self: *const @This() = @ptrCast(@alignCast(raw));
                self.control.check() catch return true;
                return false;
            }
        };
        pub fn fetch(self: *Self, base: []const u8, reference: []const u8, range: ?hls.Range, resource_limit: usize) !Body {
            try self.control.check();
            if (self.requests >= self.limits.max_requests) return error.ResourceLimitExceeded;
            const url = try resolve(self.allocator, self.root_url, base, reference, self.limits.max_uri_bytes);
            defer self.allocator.free(url);
            var cap = @min(resource_limit, self.limits.max_resource_bytes);
            if (range) |requested| {
                if (requested.length == 0 or requested.length > cap) return error.ResourceLimitExceeded;
                _ = std.math.add(u64, requested.offset, requested.length) catch return error.InvalidRemoteRange;
                cap = @intCast(requested.length);
            }
            if (cap == 0 or cap > self.limits.max_transferred_bytes -| self.charged_bytes) return error.ResourceLimitExceeded;
            var reservation = if (self.pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(u64, cap, 2) }) else admission.Token{};
            errdefer reservation.deinit();
            var headers: [2][2][]const u8 = undefined;
            headers[0] = .{ "Accept-Encoding", "identity" };
            var range_buffer: [96]u8 = undefined;
            if (range) |requested| headers[1] = .{ "Range", try std.fmt.bufPrint(&range_buffer, "bytes={d}-{d}", .{ requested.offset, requested.offset + requested.length - 1 }) };
            const cancel = Cancel{ .control = self.control };
            self.requests += 1;
            self.charged_bytes += cap; // Failed transports cannot reset retry work.
            var response = self.client.get(url, .{
                .headers = headers[0..@as(usize, if (range != null) 2 else 1)],
                .max_response_size = cap,
                .max_retries = 0,
                .follow_redirects = false,
                .cookies_enabled = false,
                .timeout_ms = self.limits.timeout_ms,
                .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.check },
            }) catch |err| {
                try self.control.check();
                return err;
            };
            var response_live = true;
            defer if (response_live) response.deinit();
            const body = response.body orelse return error.EmptyMediaResource;
            self.transferred_bytes += @min(cap, body.len);
            try self.control.check();
            if (body.len == 0 or body.len > cap) return error.ResourceLimitExceeded;
            if (range) |requested| {
                if (response.status.code != 206 or body.len != requested.length) return error.InvalidRemoteRange;
                try validateRange(response.header("Content-Range") orelse return error.InvalidRemoteRange, requested);
            } else if (response.status.code != 200) return error.UnexpectedHttpStatus;
            if (response.header("Content-Encoding")) |encoding| if (!std.ascii.eqlIgnoreCase(encoding, "identity")) return error.UnsupportedMediaEncoding;
            const owned = try self.allocator.dupe(u8, body);
            errdefer self.allocator.free(owned);
            response.deinit();
            response_live = false;
            try reservation.resize(.{ .host_bytes = owned.len });
            self.charged_bytes -= cap - owned.len;
            return .{ .allocator = self.allocator, .bytes = owned, .reservation = reservation };
        }
        /// Feed HTTP body pieces directly to the automatic fMP4 framer. Complete
        /// fragments reach the callback before response completion, with caller
        /// backpressure. No automatic retry can duplicate published fragments.
        /// Transport-entry failures make the borrowed ingest terminal; recreate it for an
        /// explicit restart. Already taken snapshots remain independently owned.
        pub fn streamFmp4(self: *Self, base: []const u8, reference: []const u8, range: ?hls.Range, resource_limit: usize, ingest: *live.Ingest, context: *anyopaque, accept: *const fn (*anyopaque, *FragmentDelivery) anyerror!void) !StreamStats {
            try self.control.check();
            if (ingest.ended or ingest.pending.items.len != 0) return error.InvalidMediaStreamState;
            if (self.requests >= self.limits.max_requests) return error.ResourceLimitExceeded;
            const url = try resolve(self.allocator, self.root_url, base, reference, self.limits.max_uri_bytes);
            defer self.allocator.free(url);
            var cap = @min(resource_limit, self.limits.max_resource_bytes);
            if (range) |requested| {
                if (requested.length == 0 or requested.length > cap) return error.ResourceLimitExceeded;
                _ = std.math.add(u64, requested.offset, requested.length) catch return error.InvalidRemoteRange;
                cap = @intCast(requested.length);
            }
            if (cap == 0 or cap > self.limits.max_transferred_bytes -| self.charged_bytes) return error.ResourceLimitExceeded;
            // Preserve the existing conservative transport envelope, including
            // httpx's buffered HTTP/2 fallback. Framer/snapshot owners separately
            // charge their actual allocations through their admission pool.
            var reservation = if (self.pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(u64, cap, 2) }) else admission.Token{};
            defer reservation.deinit();
            var headers: [2][2][]const u8 = undefined;
            headers[0] = .{ "Accept-Encoding", "identity" };
            var range_buffer: [96]u8 = undefined;
            if (range) |requested| headers[1] = .{ "Range", try std.fmt.bufPrint(&range_buffer, "bytes={d}-{d}", .{ requested.offset, requested.offset + requested.length - 1 }) };
            const Sink = struct {
                adapter: *Self,
                ingest: *live.Ingest,
                context: *anyopaque,
                accept: *const fn (*anyopaque, *FragmentDelivery) anyerror!void,
                range: ?hls.Range,
                cap: usize,
                started: bool = false,
                stats: StreamStats = .{},
                pub fn startResponse(sink: *@This(), response: anytype) !void {
                    try sink.adapter.control.check();
                    if (sink.started) return error.InvalidMediaStreamState;
                    if (sink.range) |requested| {
                        if (response.status.code != 206) return error.InvalidRemoteRange;
                        try validateRange(response.header("Content-Range") orelse return error.InvalidRemoteRange, requested);
                    } else if (response.status.code != 200) return error.UnexpectedHttpStatus;
                    if (response.header("Content-Encoding")) |encoding| if (!std.ascii.eqlIgnoreCase(encoding, "identity")) return error.UnsupportedMediaEncoding;
                    sink.started = true;
                }
                fn publish(sink: *@This(), segment: live.Segment) !void {
                    var delivery = FragmentDelivery{ .segment = segment };
                    defer if (delivery.segment) |*owner| owner.deinit();
                    try sink.adapter.control.check();
                    sink.stats.fragments += 1;
                    try sink.accept(sink.context, &delivery);
                }
                pub fn writeAll(sink: *@This(), bytes: []const u8) !void {
                    if (!sink.started) return error.InvalidMediaStreamState;
                    try sink.adapter.control.check();
                    if (bytes.len > sink.cap -| sink.stats.bytes) return error.ResourceLimitExceeded;
                    sink.stats.bytes += bytes.len;
                    sink.adapter.transferred_bytes += bytes.len;
                    // Bound pending work to one network piece plus one framing
                    // boundary; draining does not retain the complete HTTP body.
                    var offset: usize = 0;
                    while (offset < bytes.len) {
                        try sink.adapter.control.check();
                        const end = @min(bytes.len, offset + 8192);
                        try sink.ingest.push(bytes[offset..end]);
                        sink.stats.largest_pending_capacity = @max(sink.stats.largest_pending_capacity, sink.ingest.pending.capacity);
                        offset = end;
                        while (try sink.ingest.nextSegment(false)) |segment| try sink.publish(segment);
                    }
                }
            };
            var sink = Sink{ .adapter = self, .ingest = ingest, .context = context, .accept = accept, .range = range, .cap = cap };
            const cancel = Cancel{ .control = self.control };
            self.requests += 1;
            self.charged_bytes += cap;
            errdefer {
                ingest.ended = true;
                ingest.discardSegment();
            }
            var response = self.client.getToWriter(url, .{
                .headers = headers[0..@as(usize, if (range != null) 2 else 1)],
                .max_response_size = cap,
                .max_retries = 0,
                .follow_redirects = false,
                .cookies_enabled = false,
                .timeout_ms = self.limits.timeout_ms,
                .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.check },
            }, &sink, null, null) catch |err| {
                try self.control.check();
                return err;
            };
            defer response.deinit();
            try self.control.check();
            if (!sink.started) return error.UnexpectedHttpStatus;
            if (sink.stats.bytes == 0) return error.EmptyMediaResource;
            if (range) |requested| if (sink.stats.bytes != requested.length) return error.InvalidRemoteRange;
            if (ingest.pending.items.len != 0) try sink.publish(try ingest.finishSegment());
            self.charged_bytes -= cap - sink.stats.bytes;
            return sink.stats;
        }
    };
}

test "HTTP media URI resolution pins origin and resolves dot segments queries and root paths" {
    const a = std.testing.allocator;
    const root = "https://example.test/live/list.m3u8?auth=fixture";
    const first = try resolve(a, root, root, "../media/s.mp4?part=2", 4096);
    defer a.free(first);
    try std.testing.expectEqualStrings("https://example.test/media/s.mp4?part=2", first);
    const absolute = try resolve(a, root, root, "/init.mp4", 4096);
    defer a.free(absolute);
    try std.testing.expectEqualStrings("https://example.test/init.mp4", absolute);
    for ([_][]const u8{ "https://other.test/a", "//other.test/a", "http://example.test/a", "https://example.test:8443/a" }) |ref| try std.testing.expectError(error.MediaOriginChanged, resolve(a, root, root, ref, 4096));
    for ([_][]const u8{ "file:///a", "https://user@example.test/a", "a#fragment", "a\\b" }) |ref| try std.testing.expectError(error.InvalidMediaUrl, resolve(a, root, root, ref, 4096));
}
test "HTTP media ranges require exact independent Content-Range bounds" {
    try validateRange("bytes 10-19/20", .{ .offset = 10, .length = 10 });
    for ([_][]const u8{ "bytes 9-18/20", "bytes 10-20/20", "bytes 10-18/20", "bytes 10-19/*", "bytes 10-19/20/21", "items 10-19/20" }) |value| try std.testing.expectError(error.InvalidRemoteRange, validateRange(value, .{ .offset = 10, .length = 10 }));
}

test "HTTP media adapter bounds transport work preserves auth client options and releases bodies" {
    const Fake = struct {
        pub const Client = struct {
            calls: usize = 0,
            status: u16 = 206,
            content_range: []const u8 = "bytes 2-5/16",
            fail: bool = false,
            pub fn get(self: *@This(), url: []const u8, options: anytype) !Response {
                self.calls += 1;
                try std.testing.expectEqualStrings("https://example.test/live/a.mp4", url);
                try std.testing.expectEqual(@as(usize, 4), options.max_response_size);
                try std.testing.expect(!options.follow_redirects and !options.cookies_enabled);
                try std.testing.expectEqual(@as(u32, 0), options.max_retries);
                try std.testing.expect(!options.cancellation.is_cancelled_fn(options.cancellation.ptr));
                try std.testing.expectEqualStrings("bytes=2-5", options.headers[1][1]);
                if (self.fail) return error.ConnectionReset;
                return .{ .status = .{ .code = self.status }, .content_range = self.content_range };
            }
        };
        const Response = struct {
            status: struct { code: u16 },
            body: ?[]const u8 = "cdef",
            content_range: []const u8,
            pub fn deinit(_: *@This()) void {}
            pub fn header(self: *const @This(), name: []const u8) ?[]const u8 {
                return if (std.ascii.eqlIgnoreCase(name, "Content-Range")) self.content_range else null;
            }
        };
    };
    const a = std.testing.allocator;
    var client = Fake.Client{};
    var adapter = Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8", .limits = .{ .max_requests = 8, .max_transferred_bytes = 32 } };
    var body = try adapter.fetch(adapter.root_url, "a.mp4", .{ .offset = 2, .length = 4 }, 4);
    try std.testing.expectEqualStrings("cdef", body.bytes);
    body.deinit();
    try std.testing.expectEqual(@as(u64, 4), adapter.transferred_bytes);
    client.status = 200;
    try std.testing.expectError(error.InvalidRemoteRange, adapter.fetch(adapter.root_url, "a.mp4", .{ .offset = 2, .length = 4 }, 4));
    client.status = 206;
    client.content_range = "bytes 3-6/16";
    try std.testing.expectError(error.InvalidRemoteRange, adapter.fetch(adapter.root_url, "a.mp4", .{ .offset = 2, .length = 4 }, 4));
    client.fail = true;
    try std.testing.expectError(error.ConnectionReset, adapter.fetch(adapter.root_url, "a.mp4", .{ .offset = 2, .length = 4 }, 4));
    try std.testing.expectEqual(@as(u64, 16), adapter.charged_bytes);
    try std.testing.expectEqual(@as(usize, 4), client.calls);
    adapter.limits.max_requests = 4;
    try std.testing.expectError(error.ResourceLimitExceeded, adapter.fetch(adapter.root_url, "a.mp4", .{ .offset = 2, .length = 4 }, 4));
    try std.testing.expectEqual(@as(usize, 4), client.calls);
}

test "HTTP sensitive resource wipes its complete owned allocation before release" {
    var storage: [64]u8 = @splat(0);
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    const bytes = try fixed.allocator().alloc(u8, 32);
    @memset(bytes, 0x5a);
    var body = Body{ .allocator = fixed.allocator(), .bytes = bytes, .sensitive = true };
    // Observe before Allocator.free poisons the released allocation in Debug.
    body.wipe();
    for (storage[0..32]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
    body.deinit();
}
