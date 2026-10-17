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
    /// True only for the final fragment published after successful HTTP EOF.
    body_complete: bool = false,
    /// Successful callback return acknowledges automatically. On failure the
    /// consumer may acknowledge an irreversible accepted side effect explicitly.
    /// Taking ownership alone does not acknowledge queue/model acceptance.
    acknowledged: bool = false,
    pub fn acknowledge(self: *FragmentDelivery) void {
        self.acknowledged = true;
    }
    pub fn take(self: *FragmentDelivery) !live.Segment {
        const result = self.segment orelse return error.FragmentAlreadyTaken;
        self.segment = null;
        return result;
    }
};
pub const BodyConsumer = struct {
    context: *anyopaque,
    write: *const fn (*anyopaque, []const u8) anyerror!void,
    finish: ?*const fn (*anyopaque) anyerror!void = null,
    /// Called only after request/work admission; failures consume that attempt.
    entered: ?*const fn (*anyopaque) anyerror!void = null,
};
pub const StreamStats = struct { bytes: usize = 0, plaintext_bytes: usize = 0, fragments: usize = 0, largest_pending_capacity: usize = 0 };
/// Caller-owned, constant-space in-process prefix commitment. One ledger binds
/// one immutable URL/range/init/epoch. Fresh resources require fresh ledgers.
/// No automatic retry or durable/external exactly-once delivery is implied.
pub const FragmentLedger = struct {
    binding: ?[32]u8 = null,
    prefix: [32]u8 = @splat(0),
    acknowledged: usize = 0,
    attempts: usize = 0,
    complete: bool = false,
    invalid: bool = false,
    active: bool = false,
    /// Explicit polling of an append-only live resource. HTTP EOF completes a
    /// body version, not necessarily a manifest segment. Preserve prefix and
    /// attempt/work budgets while verifying that version again on the next read.
    pub fn reopen(self: *FragmentLedger) !void {
        if (self.invalid) return error.SourceVersionChanged;
        if (self.active or !self.complete) return error.InvalidMediaStreamState;
        self.complete = false;
    }
};
/// Borrowed for one call; never retained by the ledger or immutable snapshots.
pub const Encryption = struct { key: *const [16]u8, iv: [16]u8 };
pub const ResumeOptions = struct {
    encryption: ?Encryption = null,
    /// Accepted HLS parts, independent of this parent's moof boundaries.
    sample_prefix: ?live.SamplesDigest = null,
    ingest: live.Options = .{},
    epoch: usize = 0,
    max_attempts: usize = 8,
    max_fragments: usize = 8192,
    /// Stable manifest/resource identity when the same URL is reused.
    resource_context: [32]u8 = @splat(0),
};
pub const ResumeStats = struct {
    /// Reader may withhold manifest completion across a successful partial EOF.
    resource_complete: bool = true,
    transport: StreamStats = .{},
    replayed: usize = 0,
    prefix_samples_skipped: usize = 0,
    prefix_fragments_skipped: usize = 0,
    acknowledged: usize = 0,
};
fn resumeBinding(url: []const u8, range: ?hls.Range, initialization: []const u8, epoch: usize, resource_context: [32]u8, encryption: ?Encryption, sample_prefix: ?live.SamplesDigest, control: source.Control) ![32]u8 {
    var state = std.crypto.hash.sha2.Sha256.init(.{});
    defer std.crypto.secureZero(u8, std.mem.asBytes(&state));
    state.update("antfly-fmp4-replay-v4");
    state.update(&.{@intFromBool(sample_prefix != null)});
    if (sample_prefix) |prefix| {
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, prefix.count, .big);
        state.update(&count);
        state.update(&prefix.bytes());
    }
    state.update(&.{@intFromBool(encryption != null)});
    if (encryption) |cipher| {
        state.update(cipher.key);
        state.update(&cipher.iv);
    }
    state.update(&resource_context);
    var header: [33]u8 = undefined;
    std.mem.writeInt(u64, header[0..8], url.len, .big);
    std.mem.writeInt(u64, header[8..16], epoch, .big);
    header[16] = @intFromBool(range != null);
    std.mem.writeInt(u64, header[17..25], if (range) |value| value.offset else 0, .big);
    std.mem.writeInt(u64, header[25..33], if (range) |value| value.length else 0, .big);
    state.update(&header);
    state.update(url);
    var offset: usize = 0;
    while (offset < initialization.len) {
        try control.check();
        const end = @min(initialization.len, offset + 8192);
        state.update(initialization[offset..end]);
        offset = end;
    }
    var hash: [32]u8 = undefined;
    state.final(&hash);
    return hash;
}
/// Commit an owned normalized/fMP4 unit using the same ordered sample format.
pub fn hashFragment(state: std.crypto.hash.sha2.Sha256, segment: *live.Segment, control: source.Control) !std.crypto.hash.sha2.Sha256 {
    try control.check();
    var digest = live.SamplesDigest{ .state = state };
    var boundary: [16]u8 = undefined;
    std.mem.writeInt(u64, boundary[0..8], segment.ordinal, .big);
    std.mem.writeInt(u64, boundary[8..16], segment.reader.packets.len, .big);
    digest.state.update(&boundary);
    try digest.extend(&segment.reader, 0, segment.reader.packets.len);
    try control.check();
    return digest.state;
}

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
        pub fn resourceBinding(self: *Self, base: []const u8, reference: []const u8, range: ?hls.Range, epoch: usize, context: [32]u8, encryption: ?Encryption) ![32]u8 {
            try self.control.check();
            const url = try resolve(self.allocator, self.root_url, base, reference, self.limits.max_uri_bytes);
            defer self.allocator.free(url);
            return resumeBinding(url, range, "", epoch, context, encryption, null, self.control);
        }
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
            return self.streamFmp4WithEncryption(base, reference, range, resource_limit, ingest, null, context, accept);
        }
        /// Incremental whole-object AES-128 CBC. Range bytes must describe one
        /// independently padded object. Final padding validates before body EOF
        /// completion; earlier acknowledged fragments retain replay progress.
        pub fn streamFmp4WithEncryption(self: *Self, base: []const u8, reference: []const u8, range: ?hls.Range, resource_limit: usize, ingest: *live.Ingest, encryption: ?Encryption, context: *anyopaque, accept: *const fn (*anyopaque, *FragmentDelivery) anyerror!void) !StreamStats {
            try self.control.check();
            if (ingest.ended or ingest.pending.items.len != 0) return error.InvalidMediaStreamState;
            const Frames = struct {
                adapter: *Self,
                ingest: *live.Ingest,
                context: *anyopaque,
                accept: *const fn (*anyopaque, *FragmentDelivery) anyerror!void,
                count: usize = 0,
                capacity: usize = 0,
                fn publish(frame_sink: *@This(), segment: live.Segment, complete: bool) !void {
                    var delivery = FragmentDelivery{ .segment = segment, .body_complete = complete };
                    defer if (delivery.segment) |*owner| owner.deinit();
                    try frame_sink.adapter.control.check();
                    frame_sink.count += 1;
                    try frame_sink.accept(frame_sink.context, &delivery);
                }
                fn write(raw: *anyopaque, bytes: []const u8) !void {
                    const frame_sink: *@This() = @ptrCast(@alignCast(raw));
                    try frame_sink.ingest.push(bytes);
                    frame_sink.capacity = @max(frame_sink.capacity, frame_sink.ingest.pending.capacity);
                    while (try frame_sink.ingest.nextSegment(false)) |segment| try frame_sink.publish(segment, false);
                }
                fn finish(raw: *anyopaque) !void {
                    const frame_sink: *@This() = @ptrCast(@alignCast(raw));
                    if (frame_sink.ingest.pending.items.len != 0) try frame_sink.publish(try frame_sink.ingest.finishSegment(), true);
                }
            };
            var frames = Frames{ .adapter = self, .ingest = ingest, .context = context, .accept = accept };
            const before = self.requests;
            errdefer if (self.requests != before) {
                ingest.ended = true;
                ingest.discardSegment();
            };
            var stats = try self.streamBody(base, reference, range, resource_limit, encryption, .{ .context = &frames, .write = Frames.write, .finish = Frames.finish });
            stats.fragments = frames.count;
            stats.largest_pending_capacity = frames.capacity;
            return stats;
        }
        /// Authenticated bounded plaintext byte delivery, optionally decoding
        /// one whole-object AES-128 CBC resource. No retry or buffering policy is
        /// hidden in the consumer; it owns its parsing/finalization state.
        pub fn streamBody(self: *Self, base: []const u8, reference: []const u8, range: ?hls.Range, resource_limit: usize, encryption: ?Encryption, consumer: BodyConsumer) !StreamStats {
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
            // Preserve the existing conservative transport envelope, including
            // httpx's buffered HTTP/2 fallback. Framer/snapshot owners separately
            // charge their actual allocations through their admission pool.
            const workspace: u64 = if (encryption != null) hls.crypto.Stream.workspace_bytes else 0;
            const transport_bytes = try std.math.add(u64, try std.math.mul(u64, cap, 2), workspace);
            var reservation = if (self.pool) |pool| try pool.acquire(.{ .host_bytes = transport_bytes }) else admission.Token{};
            defer reservation.deinit();
            var headers: [2][2][]const u8 = undefined;
            headers[0] = .{ "Accept-Encoding", "identity" };
            var range_buffer: [96]u8 = undefined;
            if (range) |requested| headers[1] = .{ "Range", try std.fmt.bufPrint(&range_buffer, "bytes={d}-{d}", .{ requested.offset, requested.offset + requested.length - 1 }) };
            const Sink = struct {
                adapter: *Self,
                consumer: BodyConsumer,
                range: ?hls.Range,
                cap: usize,
                started: bool = false,
                decryptor: ?hls.crypto.Stream = null,
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
                        if (sink.decryptor) |*decryptor| {
                            try decryptor.push(bytes[offset..end], sink, @This().writePlain);
                        } else try sink.writePlain(bytes[offset..end]);
                        offset = end;
                    }
                }
                fn writePlain(sink: *@This(), bytes: []const u8) !void {
                    try sink.adapter.control.check();
                    sink.stats.plaintext_bytes += bytes.len;
                    try sink.consumer.write(sink.consumer.context, bytes);
                }
            };
            var sink = Sink{ .adapter = self, .consumer = consumer, .range = range, .cap = cap };
            if (encryption) |cipher| sink.decryptor = hls.crypto.Stream.init(cipher.key, cipher.iv, self.control);
            defer if (sink.decryptor) |*decryptor| decryptor.deinit();
            const cancel = Cancel{ .control = self.control };
            self.requests += 1;
            self.charged_bytes += cap;
            if (consumer.entered) |entered| try entered(consumer.context);
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
            if (sink.decryptor) |*decryptor| try decryptor.finish(&sink, Sink.writePlain);
            if (sink.stats.plaintext_bytes == 0) return error.EmptyMediaResource;
            if (consumer.finish) |finish| try finish(consumer.context);
            self.charged_bytes -= cap - sink.stats.bytes;
            return sink.stats;
        }
        /// Explicit retry reconstructs framing from the start of the resource.
        /// Already acknowledged fragments are suppressed only after hashing
        /// their complete ordered descriptors and coded bytes. No new callback
        /// runs before the old prefix matches. A changed prefix poisons the
        /// ledger; transport/cancellation/admission failures remain retryable.
        /// Caller must reconcile taken-but-unacknowledged snapshots on failure.
        pub fn streamFmp4Resuming(self: *Self, base: []const u8, reference: []const u8, range: ?hls.Range, resource_limit: usize, initialization: []const u8, ledger: *FragmentLedger, options: ResumeOptions, context: *anyopaque, accept: *const fn (*anyopaque, *FragmentDelivery) anyerror!void) !ResumeStats {
            try self.control.check();
            if (ledger.active) return error.InvalidMediaStreamState;
            if (ledger.invalid) return error.SourceVersionChanged;
            try options.ingest.stream.control.check();
            if (initialization.len > options.ingest.stream.max_bytes or initialization.len > options.ingest.max_ingested_bytes) return error.ResourceLimitExceeded;
            const url = try resolve(self.allocator, self.root_url, base, reference, self.limits.max_uri_bytes);
            defer self.allocator.free(url);
            const binding = try resumeBinding(url, range, initialization, options.epoch, options.resource_context, options.encryption, options.sample_prefix, self.control);
            if (ledger.binding) |previous| if (!std.mem.eql(u8, &previous, &binding)) {
                ledger.invalid = true;
                return error.SourceVersionChanged;
            };
            if (ledger.complete) return .{};
            if (options.max_fragments == 0 or ledger.acknowledged > options.max_fragments or ledger.attempts >= options.max_attempts) return error.ResourceLimitExceeded;
            var ingest_options = options.ingest;
            if (ingest_options.stream.admission_pool == null) ingest_options.stream.admission_pool = self.pool;
            ingest_options.max_segments = @min(ingest_options.max_segments, options.max_fragments);
            const resource_identity = std.fmt.bytesToHex(binding, .lower);
            var ingest = try live.Ingest.init(self.allocator, &resource_identity, initialization, ingest_options);
            defer ingest.deinit();
            ingest.epoch = options.epoch;
            const Replay = struct {
                adapter: *Self,
                ledger: *FragmentLedger,
                previous_count: usize,
                previous_prefix: [32]u8,
                sample_prefix: ?live.SamplesDigest,
                observed_prefix: live.SamplesDigest = .{},
                prefix_verified: bool,
                state: std.crypto.hash.sha2.Sha256 = .init(.{}),
                count: usize = 0,
                stats: ResumeStats = .{},
                context: *anyopaque,
                accept: *const fn (*anyopaque, *FragmentDelivery) anyerror!void,
                fn publish(raw: *anyopaque, delivery: *FragmentDelivery) !void {
                    const replay: *@This() = @ptrCast(@alignCast(raw));
                    try replay.adapter.control.check();
                    const segment = &delivery.segment.?;
                    var boundary: [16]u8 = undefined;
                    std.mem.writeInt(u64, boundary[0..8], segment.ordinal, .big);
                    std.mem.writeInt(u64, boundary[8..16], segment.reader.packets.len, .big);
                    var candidate = replay.state;
                    candidate.update(&boundary);
                    const skip: usize = if (replay.sample_prefix) |expected| @min(expected.count - replay.observed_prefix.count, segment.reader.packets.len) else 0;
                    var observed = replay.observed_prefix;
                    // Payload leases remain transient; the ledger retains no
                    // decoded pictures, packets, or per-fragment hash array.
                    for (segment.reader.packets, 0..) |packet, index| {
                        try replay.adapter.control.check();
                        var header: [25]u8 = undefined;
                        std.mem.writeInt(i64, header[0..8], packet.media_dts, .big);
                        std.mem.writeInt(i64, header[8..16], packet.media_pts, .big);
                        std.mem.writeInt(u32, header[16..20], packet.duration, .big);
                        std.mem.writeInt(u32, header[20..24], packet.size, .big);
                        header[24] = @intFromBool(packet.sync);
                        var payload = try segment.reader.readPacket(index);
                        defer payload.deinit();
                        candidate.update(&header);
                        if (index < skip) observed.state.update(&header);
                        var offset: usize = 0;
                        while (offset < payload.bytes.len) {
                            try replay.adapter.control.check();
                            const end = @min(payload.bytes.len, offset + 8192);
                            candidate.update(payload.bytes[offset..end]);
                            if (index < skip) observed.state.update(payload.bytes[offset..end]);
                            offset = end;
                        }
                        if (index < skip) observed.count = try std.math.add(usize, observed.count, 1);
                    }
                    var hash_state = candidate;
                    var hash: [32]u8 = undefined;
                    hash_state.final(&hash);
                    if (replay.sample_prefix) |expected| {
                        replay.observed_prefix = observed;
                        replay.stats.prefix_samples_skipped += skip;
                        if (replay.observed_prefix.count == expected.count) {
                            if (!std.mem.eql(u8, &expected.bytes(), &replay.observed_prefix.bytes())) {
                                replay.ledger.invalid = true;
                                return error.SourceVersionChanged;
                            }
                            replay.prefix_verified = true;
                        }
                    }
                    if (replay.count < replay.previous_count) {
                        replay.count += 1;
                        replay.state = candidate;
                        replay.stats.replayed += 1;
                        if (replay.count == replay.previous_count and !std.mem.eql(u8, &hash, &replay.previous_prefix)) {
                            replay.ledger.invalid = true;
                            return error.SourceVersionChanged;
                        }
                        return;
                    }
                    if (skip == segment.reader.packets.len and skip != 0) {
                        replay.count += 1;
                        replay.state = candidate;
                        replay.stats.prefix_fragments_skipped += 1;
                        // Do not commit a merely observed prefix: a truncated
                        // unverified version must not poison a healthy retry.
                        if (replay.prefix_verified) replay.commitObserved(hash);
                        return;
                    }
                    if (!replay.prefix_verified) return error.InvalidHlsReplay;
                    // Verification may finish within this moof. Previously
                    // suppressed fragments are now proven accepted parts;
                    // this fragment's new suffix still needs consumer ACK.
                    var previous_state = replay.state;
                    replay.commitObserved(previous_state.finalResult());
                    if (skip != 0) try segment.reader.discardPacketPrefix(skip);
                    replay.accept(replay.context, delivery) catch |err| {
                        if (delivery.acknowledged) replay.commit(candidate, hash);
                        return err;
                    };
                    replay.commit(candidate, hash);
                }
                fn commitObserved(replay: *@This(), hash: [32]u8) void {
                    if (replay.count <= replay.ledger.acknowledged) return;
                    replay.ledger.acknowledged = replay.count;
                    replay.ledger.prefix = hash;
                }
                fn commit(replay: *@This(), candidate: std.crypto.hash.sha2.Sha256, hash: [32]u8) void {
                    replay.state = candidate;
                    replay.count += 1;
                    replay.ledger.acknowledged = replay.count;
                    replay.ledger.prefix = hash;
                    replay.stats.acknowledged += 1;
                }
            };
            var replay = Replay{ .adapter = self, .ledger = ledger, .previous_count = ledger.acknowledged, .previous_prefix = ledger.prefix, .sample_prefix = options.sample_prefix, .prefix_verified = options.sample_prefix == null or options.sample_prefix.?.count == 0, .context = context, .accept = accept };
            ledger.active = true;
            defer ledger.active = false;
            const previous_requests = self.requests;
            defer if (self.requests != previous_requests) {
                ledger.attempts += 1;
                ledger.binding = binding;
            };
            replay.stats.transport = try self.streamFmp4WithEncryption(base, reference, range, resource_limit, &ingest, options.encryption, &replay, Replay.publish);
            if (replay.count < replay.previous_count or !replay.prefix_verified) {
                ledger.invalid = true;
                return error.SourceVersionChanged;
            }
            ledger.complete = true;
            return replay.stats;
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
