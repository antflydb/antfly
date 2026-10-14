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

//! Native HTTP fMP4 HLS, partial-unit cursors, verified parent replay and explicit bounded reload.
const std = @import("std");
const hls = @import("hls.zig");
const network = @import("http_stream.zig");
const live = @import("live_mp4.zig");
const dash_cursor = @import("dash_cursor.zig");
pub const TransportFactory = struct {
    context: *anyopaque,
    accept: *const fn (*anyopaque, []const u8, []const u8, hls.Segment, live.Options, @import("source.zig").Control, ?*@import("admission.zig").Pool) anyerror!live.Segment,
};
pub const Clock = struct { context: ?*anyopaque = null, now_ns: *const fn (?*anyopaque) u64 };
pub const Options = struct { max_resource_attempts: usize = 8, clock: ?Clock = null, playlist: hls.Limits = .{}, ingest: live.Options = .{}, selection: hls.master.Selection = .{}, transport_stream: ?TransportFactory = null };
fn sameInitialization(left: hls.Initialization, right: hls.Initialization) bool {
    return std.mem.eql(u8, left.uri, right.uri) and std.meta.eql(left.range, right.range) and hls.crypto.same(left.encryption, right.encryption);
}
fn sameSegment(left: hls.Segment, right: hls.Segment) bool {
    return std.mem.eql(u8, left.uri, right.uri) and std.meta.eql(left.range, right.range) and sameInitialization(left.initialization, right.initialization) and hls.crypto.same(left.encryption, right.encryption) and left.discontinuity_sequence == right.discontinuity_sequence and left.duration_seconds == right.duration_seconds and (left.availability_complete == right.availability_complete or (!left.availability_complete and right.availability_complete)) and (if (left.program_date_time) |date| right.program_date_time == null or hls.dateranges.Instant.order(date, right.program_date_time.?) == .eq else true) and std.meta.eql(left.part, right.part) and (if (left.dash) |timing| right.dash != null and dash_cursor.sameTiming(timing, right.dash.?) else right.dash == null);
}

fn segmentOrder(left: hls.Segment, right: hls.Segment) std.math.Order {
    if (left.dash != null and right.dash != null) return dash_cursor.order(left, right);
    const order = std.math.order(left.sequence, right.sequence);
    if (order != .eq) return order;
    return std.math.order(if (left.part) |part| @as(u64, part.index) else std.math.maxInt(u64), if (right.part) |part| @as(u64, part.index) else std.math.maxInt(u64));
}

fn directiveName(raw: []const u8, name: []const u8) bool {
    var decoded: [32]u8 = undefined;
    var used: usize = 0;
    var pos: usize = 0;
    while (pos < raw.len) {
        if (used == decoded.len) return false;
        if (raw[pos] == '%') {
            if (raw.len - pos < 3) return false;
            decoded[used] = std.fmt.parseInt(u8, raw[pos + 1 ..][0..2], 16) catch return false;
            pos += 3;
        } else {
            decoded[used] = raw[pos];
            pos += 1;
        }
        used += 1;
    }
    return std.mem.eql(u8, decoded[0..used], name);
}
/// Preserve authentication query bytes. Existing delivery directives are
/// rejected rather than creating ambiguous duplicate query parameters.
fn directiveSeparator(url: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, url, '#') != null) return error.InvalidHlsDeliveryDirective;
    const query = std.mem.indexOfScalar(u8, url, '?');
    if (query) |offset| {
        var fields = std.mem.splitScalar(u8, url[offset + 1 ..], '&');
        while (fields.next()) |field| {
            const key = field[0 .. std.mem.indexOfScalar(u8, field, '=') orelse field.len];
            if (directiveName(key, "_HLS_msn") or directiveName(key, "_HLS_part") or directiveName(key, "_HLS_skip")) return error.InvalidHlsDeliveryDirective;
        }
    }
    return if (query == null) "?" else if (std.mem.endsWith(u8, url, "?") or std.mem.endsWith(u8, url, "&")) "" else "&";
}
pub const Delivery = struct { sequence: ?u64 = null, part: ?u32 = null, skip: enum { none, segments, dateranges } = .none };
pub fn deliveryRequestUrl(a: std.mem.Allocator, url: []const u8, delivery: Delivery, limit: usize) ![]u8 {
    const separator = try directiveSeparator(url);
    if (delivery.part != null and delivery.sequence == null) return error.InvalidHlsDeliveryDirective;
    var seqbuf: [20]u8 = undefined;
    var partbuf: [20]u8 = undefined;
    const seq = if (delivery.sequence) |v| try std.fmt.bufPrint(&seqbuf, "{d}", .{v}) else "";
    const part = if (delivery.part) |v| try std.fmt.bufPrint(&partbuf, "{d}", .{v}) else "";
    const skip = switch (delivery.skip) {
        .none => "",
        .segments => "YES",
        .dateranges => "v2",
    };
    const arguments = .{ url, if (seq.len != 0 or skip.len != 0) separator else "", if (seq.len != 0) "_HLS_msn=" else "", seq, if (part.len != 0) "&_HLS_part=" else "", part, if (skip.len == 0) "" else if (seq.len != 0) "&_HLS_skip=" else "_HLS_skip=", skip };
    if (std.fmt.count("{s}{s}{s}{s}{s}{s}{s}{s}", arguments) > limit) return error.ResourceLimitExceeded;
    return std.fmt.allocPrint(a, "{s}{s}{s}{s}{s}{s}{s}{s}", arguments);
}
pub fn deltaRequestUrl(a: std.mem.Allocator, url: []const u8, limit: usize) ![]u8 {
    return deliveryRequestUrl(a, url, .{ .skip = .segments }, limit);
}
pub fn blockingRequestUrl(a: std.mem.Allocator, url: []const u8, sequence: u64, part: ?u32, limit: usize) ![]u8 {
    return deliveryRequestUrl(a, url, .{ .sequence = sequence, .part = part }, limit);
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
        snapshot_requested_ns: ?u64 = null,
        playlist: ?hls.Playlist = null,
        ingest: ?live.Ingest = null,
        stream_ledger: network.FragmentLedger = .{},
        stream_key: ?[32]u8 = null,
        stream_base_ordinal: usize = 0,
        stream_start_decode_time: ?i64 = null,
        stream_track_scale: u32 = 0,
        stream_was_partial: bool = false,
        stream_call_active: bool = false,
        next_sequence: ?u64 = null,
        next_part: u32 = 0,
        part_progress: live.SamplesDigest = .{},
        dash_position: ?dash_cursor.Cursor = null,
        active_dash_period: ?[32]u8 = null,
        active_initialization: ?hls.Initialization = null,
        cached_key: ?CachedKey = null,
        active_discontinuity_sequence: ?u64 = null,
        const CachedKey = struct {
            uri: []u8,
            bytes: [16]u8,
            reservation: @import("admission.zig").Token = .{},
            fn deinit(self: *@This(), a: std.mem.Allocator) void {
                std.crypto.secureZero(u8, &self.bytes);
                a.free(self.uri);
                self.reservation.deinit();
                self.* = undefined;
            }
        };
        const Resource = struct {
            owner: network.Body,
            bytes: []const u8,
            fn deinit(self: *@This()) void {
                self.owner.deinit();
                self.* = undefined;
            }
        };
        fn clearKey(self: *Self) void {
            if (self.cached_key) |*key| key.deinit(self.allocator);
            self.cached_key = null;
        }
        fn freeInitialization(self: *Self, value: hls.Initialization) void {
            self.allocator.free(value.uri);
            if (value.encryption) |key| self.allocator.free(key.uri);
        }
        fn cloneInitialization(self: *Self, value: hls.Initialization) !hls.Initialization {
            const uri = try self.allocator.dupe(u8, value.uri);
            errdefer self.allocator.free(uri);
            var encryption = value.encryption;
            if (encryption) |*key| key.uri = try self.allocator.dupe(u8, key.uri);
            return .{ .uri = uri, .range = value.range, .encryption = encryption };
        }
        fn loadKey(self: *Self, declaration: hls.crypto.Key) !*const [16]u8 {
            const uri = try network.resolve(self.allocator, self.transport.root_url, self.mediaUrl(), declaration.uri, self.transport.limits.max_uri_bytes);
            errdefer self.allocator.free(uri);
            if (self.cached_key) |*key| if (std.mem.eql(u8, key.uri, uri)) {
                self.allocator.free(uri);
                return &key.bytes;
            };
            var body = try self.transport.fetch(uri, uri, null, 16);
            body.sensitive = true;
            defer body.deinit();
            if (body.bytes.len != 16) return error.InvalidHlsKey;
            var reservation = if (self.transport.pool) |pool| try pool.acquire(.{ .host_bytes = uri.len + 16 }) else @import("admission.zig").Token{};
            errdefer reservation.deinit();
            try self.transport.control.check();
            self.clearKey();
            self.cached_key = .{ .uri = uri, .bytes = body.bytes[0..16].*, .reservation = reservation };
            return &self.cached_key.?.bytes;
        }
        fn fetchResource(self: *Self, uri: []const u8, range: ?hls.Range, encryption: ?hls.crypto.Key, sequence: u64) !Resource {
            const key = if (encryption) |declaration| try self.loadKey(declaration) else null;
            var body = try self.transport.fetch(self.mediaUrl(), uri, range, self.options.ingest.stream.max_bytes);
            errdefer body.deinit();
            var bytes: []const u8 = body.bytes;
            if (encryption) |declaration| {
                body.sensitive = true;
                bytes = try hls.crypto.decrypt(body.bytes, key.?, declaration.mediaIv(sequence), self.transport.control);
                if (bytes.len == 0) return error.EmptyMediaResource;
            }
            return .{ .owner = body, .bytes = bytes };
        }
        pub fn init(a: std.mem.Allocator, transport: *network.Adapter(http), url: []const u8, options: ReaderOptions) !Self {
            const owned = try network.resolve(a, transport.root_url, url, url, transport.limits.max_uri_bytes);
            return .{ .allocator = a, .transport = transport, .url = owned, .options = options };
        }
        pub fn deinit(self: *Self) void {
            if (self.ingest) |*ingest| ingest.deinit();
            if (self.playlist) |*playlist| playlist.deinit();
            if (self.manifest) |*manifest| manifest.deinit();
            if (self.active_initialization) |initialization| self.freeInitialization(initialization);
            self.clearKey();
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
            return self.reloadRequest(null, false);
        }
        pub fn canSkipReload(self: *const Self) bool {
            if (comptime !@hasDecl(manifest_parser, "parseUpdateControlled")) return false;
            if (comptime @hasDecl(manifest_parser, "parseUpdateControlled")) {
                const playlist = self.playlist orelse return false;
                if (playlist.ended) return false;
                const boundary = playlist.server_control.can_skip_until orelse return false;
                const clock = self.options.clock orelse return false;
                const requested = self.snapshot_requested_ns orelse return false;
                const current = clock.now_ns(clock.context);
                return current >= requested and @as(f64, @floatFromInt(current - requested)) <= boundary * 0.5 * std.time.ns_per_s;
            }
        }
        /// Explicit opt-in. Old/missing clock or capability uses an ordinary reload.
        pub fn reloadDelta(self: *Self) !void {
            if (!self.canSkipReload()) return self.reload();
            const v2 = self.playlist.?.server_control.can_skip_dateranges;
            const url = try deliveryRequestUrl(self.allocator, self.mediaUrl(), .{ .skip = if (v2) .dateranges else .segments }, self.transport.limits.max_uri_bytes);
            defer self.allocator.free(url);
            return self.reloadRequest(url, v2);
        }
        pub fn canBlockReload(self: *const Self) bool {
            const playlist = self.playlist orelse return false;
            return @hasDecl(manifest_parser, "master") and playlist.server_control.can_block_reload and !playlist.ended;
        }
        /// Explicit protocol wait, bounded by the adapter timeout and control.
        pub fn reloadBlocking(self: *Self) !void {
            if (!self.canBlockReload()) return error.UnsupportedHlsBlockingReload;
            const playlist = self.playlist.?;
            const url = try blockingRequestUrl(self.allocator, self.mediaUrl(), self.next_sequence orelse return error.MediaPlaylistRequired, if (playlist.part_target_seconds != null) self.next_part else null, self.transport.limits.max_uri_bytes);
            defer self.allocator.free(url);
            return self.reloadRequest(url, false);
        }
        /// Combine blocking delivery and a fresh delta capability in one request.
        pub fn reloadBlockingDelta(self: *Self) !void {
            if (!self.canBlockReload()) return error.UnsupportedHlsBlockingReload;
            const playlist = self.playlist.?;
            const skipping = self.canSkipReload();
            const v2 = skipping and playlist.server_control.can_skip_dateranges;
            const url = try deliveryRequestUrl(self.allocator, self.mediaUrl(), .{ .sequence = self.next_sequence orelse return error.MediaPlaylistRequired, .part = if (playlist.part_target_seconds != null) self.next_part else null, .skip = if (v2) .dateranges else if (skipping) .segments else .none }, self.transport.limits.max_uri_bytes);
            defer self.allocator.free(url);
            return self.reloadRequest(url, v2);
        }
        fn reloadRequest(self: *Self, request_url: ?[]const u8, expect_date_delta: bool) !void {
            try self.transport.control.check();
            if (self.stream_call_active) return error.InvalidMediaStreamState;
            const requested_ns: ?u64 = if (comptime @hasDecl(manifest_parser, "parseUpdateControlled")) if (self.options.clock) |clock| clock.now_ns(clock.context) else null else null;
            var body = try self.transport.fetch(self.mediaUrl(), request_url orelse self.mediaUrl(), null, self.options.playlist.max_bytes);
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
            else if (@hasDecl(manifest_parser, "parseUpdateControlled")) blk: {
                const merged = parse: {
                    var candidate = manifest_parser.parseUpdateControlled(self.allocator, body.bytes, self.options.playlist, self.playlist, self.transport.control, self.transport.pool) catch |err| break :parse @as(anyerror!hls.Playlist, err);
                    if (expect_date_delta and candidate.skipped_segments != null and !candidate.daterange_delta) {
                        candidate.deinit();
                        break :parse @as(anyerror!hls.Playlist, error.HlsDeltaHistoryMissing);
                    }
                    break :parse @as(anyerror!hls.Playlist, candidate);
                } catch |err| {
                    if (err != error.HlsDeltaHistoryMissing) return err;
                    // Neither cursor nor committed manifest changes on retry failure.
                    const replacement = try self.transport.fetch(self.mediaUrl(), manifest_url, null, self.options.playlist.max_bytes);
                    body.deinit();
                    body = replacement;
                    var full = try manifest_parser.parseUpdateControlled(self.allocator, body.bytes, self.options.playlist, self.playlist, self.transport.control, self.transport.pool);
                    if (full.skipped_segments != null) {
                        full.deinit();
                        return error.HlsDeltaHistoryMissing;
                    }
                    break :blk full;
                };
                break :blk merged;
            } else try manifest_parser.parse(self.allocator, body.bytes, self.options.playlist);
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
                    if (@as(u128, expected_sequence) > @as(u128, last) + 1) return error.SourceVersionChanged;
                } else if (self.playlist != null and self.playlist.?.segments.len != 0) return error.SourceVersionChanged;
            }
            if (self.stream_key) |key| {
                const item = (try self.selectedResource(parsed)) orelse return error.MediaStreamGap;
                if (!std.meta.eql(key, try self.streamDescriptor(item))) return error.SourceVersionChanged;
            }
            try self.transport.control.check();
            if (self.playlist) |*previous| previous.deinit();
            if (self.manifest) |*previous| previous.deinit();
            // Keys may rotate behind a reused URI. Never carry key bytes into
            // another successfully committed playlist snapshot.
            self.clearKey();
            self.playlist = parsed;
            self.manifest = body;
            self.snapshot_requested_ns = requested_ns;
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
            if (self.stream_key != null) return false;
            const playlist = self.playlist orelse return false;
            if (!playlist.ended) return false;
            if (playlist.segments.len == 0) return true;
            if (self.dash_position) |position| return (position.select(playlist) catch return false) == null;
            return self.selectHls(playlist) == null;
        }
        /// Caller-visible polling delay; no blocking sleeper is hidden in ingest.
        pub fn reloadAfterSeconds(self: *const Self, unchanged: bool) f64 {
            if (self.playlist) |playlist| if (playlist.reload_after_seconds) |seconds| return seconds;
            if (self.playlist) |playlist| if (playlist.part_target_seconds) |seconds| return seconds * (if (unchanged) @as(f64, 0.5) else 1.0);
            const target = if (self.playlist) |playlist| playlist.target_duration_seconds else 1;
            return @as(f64, @floatFromInt(target)) * (if (unchanged) @as(f64, 0.5) else 1.0);
        }
        fn selectHls(self: *const Self, playlist: hls.Playlist) ?hls.Segment {
            const sequence = self.next_sequence orelse return null;
            var parent: ?hls.Segment = null;
            var part: ?hls.Segment = null;
            for (playlist.segments) |item| if (item.sequence == sequence) {
                if (item.part) |info| {
                    if (info.index == self.next_part) part = item;
                } else parent = item;
            };
            if (self.next_part == 0 and parent != null) return parent;
            return part orelse parent;
        }
        fn commitCursor(self: *Self, item: hls.Segment) void {
            if (item.part) |part| {
                self.next_sequence = item.sequence;
                self.next_part = part.index + 1;
            } else {
                self.next_sequence = item.sequence + 1;
                self.next_part = 0;
                self.part_progress = .{};
            }
            if (self.dash_position) |*position| position.consumed(item);
        }
        fn prepareInitialization(self: *Self, item: hls.Segment) !void {
            const needs_initialization = if (self.active_initialization) |active| !sameInitialization(active, item.initialization) or self.active_discontinuity_sequence.? != item.discontinuity_sequence or (if (item.dash) |timing| !std.meta.eql(self.active_dash_period, @as(?[32]u8, timing.period.identity)) else false) else true;
            if (needs_initialization and self.next_part != 0) return error.SourceVersionChanged;
            if (needs_initialization) {
                var initialization = try self.fetchResource(item.initialization.uri, item.initialization.range, item.initialization.encryption, item.sequence);
                defer initialization.deinit();
                const owned_initialization = try self.cloneInitialization(item.initialization);
                errdefer self.freeInitialization(owned_initialization);
                if (self.ingest) |*ingest| {
                    try ingest.reconfigure(self.mediaUrl(), initialization.bytes);
                } else {
                    var options = self.options.ingest;
                    options.stream.control = self.transport.control;
                    if (options.stream.admission_pool == null) options.stream.admission_pool = self.transport.pool;
                    self.ingest = try live.Ingest.init(self.allocator, self.mediaUrl(), initialization.bytes, options);
                }
                if (self.active_initialization) |previous| self.freeInitialization(previous);
                self.active_initialization = owned_initialization;
                self.active_discontinuity_sequence = item.discontinuity_sequence;
                self.active_dash_period = if (item.dash) |timing| timing.period.identity else null;
            }
        }
        fn selectedResource(self: *const Self, playlist: hls.Playlist) !?hls.Segment {
            if (self.dash_position) |position| return if (try position.select(playlist)) |index| playlist.segments[index] else null;
            return self.selectHls(playlist);
        }
        fn streamDescriptor(self: *const Self, item: hls.Segment) ![32]u8 {
            try self.transport.control.check();
            var canonical = item;
            if (canonical.dash) |*timing| {
                canonical.availability_complete = true;
                // Compatible live reloads may announce Period end/next-period
                // bounds without changing this selected resource's identity.
                timing.period.duration_ms = null;
                timing.period.next_start_ms = null;
                timing.period.next_identity = null;
                timing.final = false;
            }
            var buffer: [256]u8 = undefined;
            var hash = std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256).init(&buffer);
            try std.json.Stringify.value(.{ .base = self.mediaUrl(), .item = canonical }, .{}, &hash.writer);
            try hash.writer.flush();
            return hash.hasher.finalResult();
        }
        fn completeResource(self: *const Self, item: hls.Segment, start: i64, end: i64, scale: u32) bool {
            if (!self.stream_was_partial) return true;
            const timing = item.dash orelse return false;
            return (@as(i128, end) - start) * timing.timescale >= @as(i128, timing.duration_ticks) * scale;
        }
        /// Stream one selected plaintext or whole-object AES fMP4 resource with
        /// callback backpressure.
        /// Fragment acceptance survives failures; the manifest cursor commits
        /// only after successful EOF and, for partial DASH, a continuous native
        /// decode span covering the advertised resource duration. Retry/reload
        /// cannot silently drop or mutate
        /// the selected resource. next() cannot mix buffered consumption into an
        /// unfinished streaming resource. TS/LL-HLS parts retain next().
        pub fn streamNext(self: *Self, context: *anyopaque, accept: *const fn (*anyopaque, *network.FragmentDelivery) anyerror!void) !?network.ResumeStats {
            try self.transport.control.check();
            if (self.stream_call_active) return error.InvalidMediaStreamState;
            const playlist = self.playlist orelse return error.MediaPlaylistRequired;
            const item = (try self.selectedResource(playlist)) orelse return null;
            if (item.part) |part| if (part.gap) return error.MediaStreamGap;
            if (item.initialization.uri.len == 0) return error.UnsupportedStreamingResource;
            if (item.part != null or self.next_part != 0) return error.UnsupportedHlsStreamingParts;
            if (self.ingest) |*ingest| if (ingest.pending.items.len != 0) return error.PendingMediaSegment;
            const key = try self.streamDescriptor(item);
            if (self.stream_key) |previous| {
                if (!std.meta.eql(previous, key)) return error.SourceVersionChanged;
            } else {
                try self.prepareInitialization(item);
                self.stream_key = key;
                self.stream_base_ordinal = self.ingest.?.segments;
                self.stream_start_decode_time = null;
                self.stream_track_scale = 0;
                self.stream_was_partial = !item.availability_complete;
                self.stream_ledger = .{};
            }
            const encryption: ?network.Encryption = if (item.encryption) |declaration| .{ .key = try self.loadKey(declaration), .iv = declaration.mediaIv(item.sequence) } else null;
            if (self.stream_ledger.complete) try self.stream_ledger.reopen();
            const Sink = struct {
                owner: *Self,
                item: hls.Segment,
                context: *anyopaque,
                accept: *const fn (*anyopaque, *network.FragmentDelivery) anyerror!void,
                fn publish(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
                    const sink: *@This() = @ptrCast(@alignCast(raw));
                    const ingest = &sink.owner.ingest.?;
                    const unit = &delivery.segment.?;
                    if (unit.reader.packets.len == 0) return error.EmptyMediaResource;
                    const first = unit.reader.packets[0];
                    if (ingest.next_decode_time) |expected_time| if (first.media_dts < expected_time) return error.NonMonotonicMediaSegment;
                    if (sink.owner.stream_was_partial) {
                        var expected_time = if (sink.owner.stream_start_decode_time != null) ingest.next_decode_time.? else first.media_dts;
                        for (unit.reader.packets) |packet| {
                            if (packet.media_dts != expected_time) return error.MediaStreamGap;
                            expected_time = try std.math.add(i64, packet.media_dts, packet.duration);
                        }
                    }
                    const last = unit.reader.packets[unit.reader.packets.len - 1];
                    const decode_end = try std.math.add(i64, last.media_dts, last.duration);
                    const ordinal = try std.math.add(usize, sink.owner.stream_base_ordinal, unit.ordinal);
                    if (ordinal >= ingest.options.max_segments) return error.ResourceLimitExceeded;
                    unit.ordinal = ordinal;
                    if (sink.item.dash) |timing| {
                        var mapped = timing;
                        mapped.fragment_index = ordinal - sink.owner.stream_base_ordinal;
                        const start = sink.owner.stream_start_decode_time orelse first.media_dts;
                        mapped.resource_complete = delivery.body_complete and sink.owner.completeResource(sink.item, start, decode_end, unit.reader.track.timescale);
                        unit.dash_timing = mapped;
                    }
                    if (comptime @hasDecl(manifest_parser, "master")) unit.hls_timing = .{ .sequence = sink.item.sequence, .discontinuity_sequence = sink.item.discontinuity_sequence };
                    const start_decode = sink.owner.stream_start_decode_time orelse first.media_dts;
                    const track_scale = unit.reader.track.timescale;
                    sink.accept(sink.context, delivery) catch |err| {
                        if (delivery.acknowledged) {
                            sink.owner.stream_start_decode_time = start_decode;
                            sink.owner.stream_track_scale = track_scale;
                            ingest.next_decode_time = decode_end;
                            ingest.segments = ordinal + 1;
                        }
                        return err;
                    };
                    sink.owner.stream_start_decode_time = start_decode;
                    sink.owner.stream_track_scale = track_scale;
                    ingest.next_decode_time = decode_end;
                    ingest.segments = ordinal + 1;
                }
            };
            var sink = Sink{ .owner = self, .item = item, .context = context, .accept = accept };
            self.stream_call_active = true;
            defer self.stream_call_active = false;
            var options = self.options.ingest;
            // Borrow the stable Reader transport control, never a local shim
            // whose address would escape inside the immutable snapshots.
            options.stream.control = self.transport.control;
            const transferred_before = self.transport.transferred_bytes;
            defer self.ingest.?.ingested_bytes += self.transport.transferred_bytes - transferred_before;
            const remaining = self.ingest.?.options.max_ingested_bytes -| self.ingest.?.ingested_bytes;
            const resource_limit: usize = @intCast(@min(options.stream.max_bytes, remaining));
            var result = try self.transport.streamFmp4Resuming(self.mediaUrl(), item.uri, item.range, resource_limit, self.ingest.?.initialization.bytes, &self.stream_ledger, .{ .ingest = options, .epoch = self.ingest.?.epoch, .max_attempts = self.options.max_resource_attempts, .resource_context = key, .encryption = encryption }, &sink, Sink.publish);
            result.resource_complete = if (self.stream_start_decode_time) |start| self.completeResource(item, start, self.ingest.?.next_decode_time.?, self.stream_track_scale) else false;
            if (!result.resource_complete) {
                if (item.availability_complete) return error.IncompleteMediaSegment;
                return result;
            }
            self.commitCursor(item);
            self.stream_key = null;
            self.stream_ledger = .{};
            return result;
        }
        pub fn next(self: *Self) !?live.Segment {
            if (self.stream_key != null or self.stream_call_active) return error.PendingMediaSegment;
            while (true) {
                try self.transport.control.check();
                const playlist = self.playlist orelse return error.MediaPlaylistRequired;
                const item: hls.Segment = if (self.dash_position) |position|
                    playlist.segments[(try position.select(playlist)) orelse return null]
                else
                    self.selectHls(playlist) orelse return null;
                if (item.part) |part| if (part.gap) return error.MediaStreamGap;
                if (!item.availability_complete) return error.StreamingMediaRequired;
                if (item.initialization.uri.len == 0) {
                    if (comptime !@hasDecl(manifest_parser, "master")) return error.UnsupportedContainer;
                    if (item.part != null) return error.UnsupportedContainer;
                    const factory = self.options.transport_stream orelse return error.UnsupportedContainer;
                    var resource = try self.fetchResource(item.uri, item.range, item.encryption, item.sequence);
                    defer resource.deinit();
                    const result = try factory.accept(factory.context, self.mediaUrl(), resource.bytes, item, self.options.ingest, self.transport.control, self.transport.pool);
                    self.commitCursor(item);
                    return result;
                }
                // Failed snapshot allocation/validation retains pending input. Retry
                // that commit before any fetch; never append the same object twice.
                if (self.ingest) |*ingest| if (ingest.pending.items.len != 0) {
                    var result = try ingest.finishSegment();
                    result.dash_timing = item.dash;
                    if (comptime @hasDecl(manifest_parser, "master")) result.hls_timing = .{ .sequence = item.sequence, .part_index = if (item.part) |part| part.index else null, .independent_hint = if (item.part) |part| part.independent else false, .discontinuity_sequence = item.discontinuity_sequence };
                    self.commitCursor(item);
                    return result;
                };
                try self.prepareInitialization(item);
                var media = try self.fetchResource(item.uri, item.range, item.encryption, item.sequence);
                defer media.deinit();
                try self.ingest.?.push(media.bytes);
                var result = (if (item.part != null or self.next_part != 0)
                    self.ingest.?.finishHlsSegment(self.part_progress, item.part == null)
                else
                    self.ingest.?.finishSegment()) catch |err| {
                    self.ingest.?.discardSegment();
                    return err;
                };
                result.dash_timing = item.dash;
                if (comptime @hasDecl(manifest_parser, "master")) result.hls_timing = .{ .sequence = item.sequence, .part_index = if (item.part) |part| part.index else null, .independent_hint = if (item.part) |part| part.independent else false, .discontinuity_sequence = item.discontinuity_sequence };
                if (result.samples_digest) |digest| self.part_progress = digest;
                self.commitCursor(item);
                if (result.reader.packets.len == 0) {
                    result.deinit();
                    continue;
                }
                return result;
            }
        }
    };
}

const FakeHttp = struct {
    pub const Client = struct {
        manifest: []const u8,
        calls: usize = 0,
        fail_media: bool = false,
        fail_stream_prefix: bool = false,
        stream_complete: bool = false,
        stream_prefix_bytes: ?usize = null,
        stream_body: ?[]const u8 = null,
        pub fn get(self: *@This(), url: []const u8, options: anytype) !Response {
            self.calls += 1;
            if (options.cancellation.is_cancelled_fn(options.cancellation.ptr)) return error.Cancelled;
            if (std.mem.endsWith(u8, url, "list.m3u8") or std.mem.endsWith(u8, url, "list.mpd")) return .{ .body = self.manifest };
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
        pub fn getToWriter(self: *@This(), url: []const u8, options: anytype, writer: anytype, _: anytype, _: anytype) !Response {
            self.stream_complete = false;
            const response = try self.get(url, options);
            try writer.startResponse(response);
            const available = self.stream_body orelse response.body.?;
            const body = available[0..@min(available.len, self.stream_prefix_bytes orelse available.len)];
            var offset: usize = 0;
            while (offset < body.len) {
                if (self.fail_stream_prefix and offset >= 11206 - 797 + 8) return error.ConnectionReset;
                const end = @min(body.len, offset + 128);
                try writer.writeAll(body[offset..end]);
                offset = end;
            }
            self.stream_complete = true;
            return response;
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
test "live DASH availability reload waits for exact offset boundary preserves identity and retries" {
    const a = std.testing.allocator;
    const mpd = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011' type='dynamic' availabilityStartTime='1970-01-01T00:00:00Z' timeShiftBufferDepth='PT4S'><BaseURL availabilityTimeOffset='0.25'>./</BaseURL><Period><AdaptationSet><Representation id='v'><SegmentTemplate duration='2' availabilityTimeOffset='0.75' initialization='init.mp4' media='seg-$Number$.m4s'/></Representation></AdaptationSet></Period></MPD>";
    const OffsetClock = struct {
        time: i64 = 999,
        fn now(raw: ?*anyopaque) i64 {
            return @as(*@This(), @ptrCast(@alignCast(raw.?))).time;
        }
    };
    var clock = OffsetClock{};
    var client = FakeHttp.Client{ .manifest = mpd };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now = OffsetClock.now } });
    defer reader.deinit();
    try reader.reload();
    try std.testing.expect((try reader.next()) == null);
    try std.testing.expectEqual(@as(usize, 1), client.calls);
    clock.time = 1000;
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var lease = try first.reader.readPacket(0);
    defer lease.deinit();
    clock.time = 2000;
    const changed = try std.mem.replaceOwned(u8, a, mpd, "availabilityTimeOffset='0.75'", "availabilityTimeOffset='1.75'");
    defer a.free(changed);
    client.manifest = changed;
    try reader.reload();
    client.fail_media = true;
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expectEqual(@as(u64, 2), reader.next_sequence.?);
    client.fail_media = false;
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.epoch);
    try std.testing.expectEqual(@as(u64, 3), reader.next_sequence.?);
    try std.testing.expect(lease.bytes.len > 0);
    try std.testing.expect((try reader.next()) == null);
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

const aes_first = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\",IV=0x1\n#EXT-X-MAP:URI=\"init1\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\"\n#EXTINF:1,\nmedia10\n";
const aes_rotated = aes_first ++ "#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n#EXTINF:1,\nmedia11\n#EXT-X-ENDLIST\n";
const AesHttp = struct {
    const key1 = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";
    const key2 = "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f";
    pub const Client = struct {
        manifest: []const u8 = aes_rotated,
        calls: usize = 0,
        key_calls: usize = 0,
        fail_media: bool = false,
        short_key: bool = false,
        changed_key1: bool = false,
        reused_media11: bool = false,
        stream_body: ?[]const u8 = null,
        fail_stream_prefix: bool = false,
        stream_complete: bool = false,
        pub fn get(self: *@This(), url: []const u8, _: anytype) !FakeHttp.Response {
            self.calls += 1;
            if (std.mem.endsWith(u8, url, "list.m3u8")) return .{ .body = self.manifest };
            if (std.mem.endsWith(u8, url, "key1") or std.mem.endsWith(u8, url, "key2")) {
                self.key_calls += 1;
                const bytes = if (self.changed_key1 or std.mem.endsWith(u8, url, "key2")) key2 else key1;
                return .{ .body = if (self.short_key) bytes[0..15] else bytes };
            }
            if (std.mem.endsWith(u8, url, "init1")) return .{ .body = @embedFile("../testdata/hls/aes/init1.bin") };
            if (std.mem.endsWith(u8, url, "init2")) return .{ .body = @embedFile("../testdata/hls/aes/init2.bin") };
            if (self.fail_media) return error.ConnectionReset;
            if (std.mem.endsWith(u8, url, "media10")) return .{ .body = @embedFile("../testdata/hls/aes/media10.bin") };
            if (std.mem.endsWith(u8, url, "media11")) return .{ .body = @embedFile("../testdata/hls/aes/media11.bin") };
            if (std.mem.endsWith(u8, url, "changed12")) return .{ .body = @embedFile("../testdata/hls/aes/changed12.bin") };
            return error.UnexpectedAesResource;
        }

        pub fn getToWriter(self: *@This(), url: []const u8, options: anytype, writer: anytype, _: anytype, _: anytype) !FakeHttp.Response {
            self.stream_complete = false;
            const response = try self.get(url, options);
            try writer.startResponse(response);
            const body = self.stream_body orelse response.body.?;
            var offset: usize = 0;
            while (offset < body.len) {
                if (self.fail_stream_prefix and offset >= 11206 - 797 + 64) return error.ConnectionReset;
                const end = @min(body.len, offset + 128);
                try writer.writeAll(body[offset..end]);
                offset = end;
            }
            self.stream_complete = true;
            return response;
        }
    };
};
test "live HLS AES decrypts independent init and media fixtures with key rotation and held packets" {
    const a = std.testing.allocator;
    var client = AesHttp.Client{};
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    const original = @embedFile("../testdata/fragmented.mp4");
    try std.testing.expectEqualSlices(u8, original[0..11206], first.backing.bytes);
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    const retained = try a.dupe(u8, held.bytes);
    defer a.free(retained);
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.epoch);
    try std.testing.expectEqual(@as(usize, 2), client.key_calls);
    try std.testing.expectEqual(@as(usize, 6), client.calls);
    try std.testing.expectEqualSlices(u8, retained, held.bytes);
    try std.testing.expect(reader.ended());
}
test "live HLS AES reload refreshes reused key URI and owns encrypted map declarations" {
    const a = std.testing.allocator;
    var client = AesHttp.Client{ .manifest = aes_first };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    try std.testing.expect(reader.cached_key != null);
    client.manifest = aes_first ++ "#EXTINF:1,\nmedia11\n#EXT-X-ENDLIST\n";
    client.changed_key1 = true;
    try reader.reload();
    try std.testing.expect(reader.cached_key == null);
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.epoch);
    try std.testing.expectEqual(@as(usize, 2), client.key_calls);
    try std.testing.expectEqualStrings("key1", reader.active_initialization.?.encryption.?.uri);
    try std.testing.expect(reader.ended());
}
test "live HLS AES key and media errors preserve cursor and initialized retry state" {
    const a = std.testing.allocator;
    var client = AesHttp.Client{ .short_key = true };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.InvalidHlsKey, reader.next());
    try std.testing.expect(reader.cached_key == null and reader.ingest == null);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    client.short_key = false;
    client.fail_media = true;
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expect(reader.ingest != null and reader.cached_key != null);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    const calls = client.calls;
    client.fail_media = false;
    var segment = (try reader.next()).?;
    defer segment.deinit();
    try std.testing.expectEqual(calls + 1, client.calls);
    try std.testing.expectEqual(@as(usize, 0), segment.epoch);
}
fn aesAllocationRun(backing: std.mem.Allocator, pool: *@import("admission.zig").Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), @import("admission.zig").Resources{}));
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var client = AesHttp.Client{ .manifest = aes_first };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8", .pool = pool };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    client.manifest = aes_rotated;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.epoch);
}
test "live HLS AES allocation failures release keys decrypted resources and held snapshots" {
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, aesAllocationRun, .{&pool});
}
test "live HLS AES reload rejects changed encryption metadata in an overlapping segment" {
    const a = std.testing.allocator;
    var client = AesHttp.Client{ .manifest = aes_first };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    client.manifest = try std.mem.replaceOwned(u8, a, aes_first, "IV=0x1", "IV=0x2");
    defer a.free(client.manifest);
    try std.testing.expectError(error.SourceVersionChanged, reader.reload());
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
}

test "live HLS AES reconfiguration decrypts a new map and preserves the previous packet lease" {
    const a = std.testing.allocator;
    const updated = aes_first ++ "#EXT-X-DISCONTINUITY\n#EXT-X-KEY:METHOD=AES-128,URI=\"key2\",IV=0x2\n#EXT-X-MAP:URI=\"init2\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n#EXTINF:1,\nchanged12\n#EXT-X-ENDLIST\n";
    // The second listed segment follows sequence 10, so explicitly use IV 12.
    const corrected = try std.mem.replaceOwned(u8, a, updated, "#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n#EXTINF", "#EXT-X-KEY:METHOD=AES-128,URI=\"key2\",IV=0xC\n#EXTINF");
    defer a.free(corrected);
    var client = AesHttp.Client{ .manifest = aes_first };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var old = (try reader.next()).?;
    defer old.deinit();
    var held = try old.reader.readPacket(0);
    defer held.deinit();
    const retained = try a.dupe(u8, held.bytes);
    defer a.free(retained);
    client.manifest = corrected;
    try reader.reload();
    var changed = (try reader.next()).?;
    defer changed.deinit();
    try std.testing.expectEqual(@as(usize, 1), changed.epoch);
    try std.testing.expectEqual(@as(u16, 96), changed.reader.track.width);
    try std.testing.expectEqualSlices(u8, retained, held.bytes);
    try std.testing.expect(reader.ended());
}
test "live HLS AES foreign keys fail before fetching initialization or ciphertext" {
    const a = std.testing.allocator;
    const manifest = try std.mem.replaceOwned(u8, a, aes_first, "URI=\"key1\"", "URI=\"https://other.test/key\"");
    defer a.free(manifest);
    var client = AesHttp.Client{ .manifest = manifest };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.MediaOriginChanged, reader.next());
    try std.testing.expectEqual(@as(usize, 1), client.calls);
    try std.testing.expect(reader.cached_key == null and reader.ingest == null);
}

const ll_prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-MAP:URI=\"init.mp4\"\n";
const ll_first = ll_prefix ++ "#EXT-X-PART:DURATION=1,URI=\"seg-1.m4s\",INDEPENDENT=YES\n";
const ll_second = ll_first ++ "#EXT-X-PART:DURATION=1,URI=\"seg-2.m4s\",INDEPENDENT=YES\n";
const ll_parent = ll_prefix ++ "#EXTINF:2,\nparent.m4s\n#EXT-X-ENDLIST\n";
const PartialHttp = struct {
    pub const Response = FakeHttp.Response;
    pub const Client = struct {
        manifest: []const u8 = ll_first,
        parent_bytes: ?[]const u8 = null,
        fail_media: bool = false,
        requested_blocking_part: bool = false,
        pub fn get(self: *@This(), url: []const u8, options: anytype) !Response {
            if (std.mem.indexOf(u8, url, "list.m3u8") != null) {
                self.requested_blocking_part = std.mem.endsWith(u8, url, "_HLS_msn=10&_HLS_part=1");
                return .{ .body = self.manifest };
            }
            if (std.mem.endsWith(u8, url, "parent.m4s")) {
                if (self.fail_media) return error.ConnectionReset;
                return .{ .body = self.parent_bytes orelse @embedFile("../testdata/fragmented.mp4")[797..20811] };
            }
            var original = FakeHttp.Client{ .manifest = self.manifest, .fail_media = self.fail_media };
            return original.get(url, options);
        }
    };
};
test "live HLS partial reload consumes each part and acknowledges its parent without duplicate samples" {
    const a = std.testing.allocator;
    var client = PartialHttp.Client{};
    var transport = network.Adapter(PartialHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8" };
    var reader = try Reader(PartialHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var retained = try first.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expect((try reader.next()) == null);
    client.manifest = ll_second;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expect(second.reader.packets[0].media_dts > first.reader.packets[0].media_dts);
    client.manifest = ll_parent;
    try reader.reload();
    try std.testing.expect((try reader.next()) == null);
    try std.testing.expect(reader.ended());
    var again = try first.reader.readPacket(0);
    defer again.deinit();
    try std.testing.expectEqualSlices(u8, retained.bytes, again.bytes);
}
test "live HLS completed parent recovers unlisted tail and preserves cursor across HTTP retry" {
    const a = std.testing.allocator;
    var client = PartialHttp.Client{};
    var transport = network.Adapter(PartialHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8" };
    var reader = try Reader(PartialHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    client.manifest = ll_parent;
    try reader.reload();
    client.fail_media = true;
    try std.testing.expectError(error.ConnectionReset, reader.next());
    client.fail_media = false;
    var tail = (try reader.next()).?;
    defer tail.deinit();
    try std.testing.expect(tail.reader.packets[0].media_dts == first.reader.packets[first.reader.packets.len - 1].media_dts + first.reader.packets[first.reader.packets.len - 1].duration);
    try std.testing.expectEqual(first.reader.packets.len, tail.reader.packets.len);
    try std.testing.expect(reader.ended());
}

test "live HLS parent fallback verifies encoded sample prefix and allows a clean retry after mutation" {
    const a = std.testing.allocator;
    var client = PartialHttp.Client{};
    var transport = network.Adapter(PartialHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8" };
    var reader = try Reader(PartialHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    client.manifest = ll_parent;
    try reader.reload();
    const altered = try a.dupe(u8, @embedFile("../testdata/fragmented.mp4")[797..20811]);
    defer a.free(altered);
    altered[184 + 8 + 10] ^= 1;
    client.parent_bytes = altered;
    try std.testing.expectError(error.SourceVersionChanged, reader.next());
    try std.testing.expectEqual(@as(u32, 1), reader.next_part);
    client.parent_bytes = null;
    var tail = (try reader.next()).?;
    defer tail.deinit();
    try std.testing.expectEqual(first.reader.packets.len, tail.reader.packets.len);
}
fn partialAllocationFailures(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), @import("admission.zig").Resources{}));
    var client = PartialHttp.Client{};
    var transport = network.Adapter(PartialHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8", .pool = &pool };
    var reader = try Reader(PartialHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    client.manifest = ll_parent;
    try reader.reload();
    var tail = (try reader.next()).?;
    defer tail.deinit();
    try std.testing.expectEqual(first.reader.packets.len, tail.reader.packets.len);
}
test "live HLS partial parent fallback allocation failures release snapshots indexes and leases" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, partialAllocationFailures, .{});
}

test "live HLS blocking reload preserves authentication query and rejects directive aliases" {
    const a = std.testing.allocator;
    const result = try blockingRequestUrl(a, "https://example.test/list.m3u8?token=a%26b", 10, 2, 4096);
    defer a.free(result);
    try std.testing.expectEqualStrings("https://example.test/list.m3u8?token=a%26b&_HLS_msn=10&_HLS_part=2", result);
    inline for (.{ "_HLS_msn=1", "%5FHLS_part=1", "_HLS_skip=YES", "token=x#fragment" }) |query| try std.testing.expectError(error.InvalidHlsDeliveryDirective, blockingRequestUrl(a, "https://example.test/list.m3u8?" ++ query, 10, 2, 4096));
    try std.testing.expectError(error.ResourceLimitExceeded, blockingRequestUrl(a, "https://example.test/list.m3u8", 10, 2, 10));
}
test "live HLS blocking reload requests next part and retains canonical media URL" {
    const a = std.testing.allocator;
    const controls = "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=2\n";
    var client = PartialHttp.Client{ .manifest = ll_first ++ controls };
    var transport = network.Adapter(PartialHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8" };
    var reader = try Reader(PartialHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    try std.testing.expect(reader.canBlockReload());
    var first = (try reader.next()).?;
    defer first.deinit();
    client.manifest = ll_second ++ controls;
    try reader.reloadBlocking();
    try std.testing.expect(client.requested_blocking_part);
    try std.testing.expectEqualStrings(transport.root_url, reader.mediaUrl());
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(f64, 0.5), reader.reloadAfterSeconds(true));
}

fn encryptPartialFixture(a: std.mem.Allocator, plain: []const u8, iv: [16]u8) ![]u8 {
    const padding: u8 = @intCast(16 - plain.len % 16);
    const result = try a.alloc(u8, plain.len + padding);
    @memcpy(result[0..plain.len], plain);
    @memset(result[plain.len..], padding);
    const context = std.crypto.core.aes.Aes128.initEnc(@splat(0x37));
    var previous = iv;
    var offset: usize = 0;
    while (offset < result.len) : (offset += 16) {
        var block: [16]u8 = undefined;
        for (&block, 0..) |*value, index| value.* = result[offset + index] ^ previous[index];
        context.encrypt(&previous, &block);
        @memcpy(result[offset..][0..16], &previous);
    }
    return result;
}
const EncryptedPartialHttp = struct {
    pub const Response = FakeHttp.Response;
    pub const Client = struct {
        manifest: []const u8,
        initialization: []const u8,
        part: []const u8,
        parent: []const u8,
        pub fn get(self: *@This(), url: []const u8, _: anytype) !Response {
            if (std.mem.endsWith(u8, url, "list.m3u8")) return .{ .body = self.manifest };
            if (std.mem.endsWith(u8, url, "key")) return .{ .body = &@as([16]u8, @splat(0x37)) };
            if (std.mem.endsWith(u8, url, "init.mp4")) return .{ .body = self.initialization };
            if (std.mem.endsWith(u8, url, "p0.m4s")) return .{ .body = self.part };
            if (std.mem.endsWith(u8, url, "parent.m4s")) return .{ .body = self.parent };
            return error.UnexpectedFixtureUri;
        }
    };
};
test "live HLS independent AES partial objects and parent fallback preserve sample payloads" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/fragmented.mp4");
    const key = (try hls.crypto.parseKey("METHOD=AES-128,URI=\"key\"", 4096)).key.?;
    var init_iv: [16]u8 = @splat(0);
    init_iv[15] = 1;
    const initialization = try encryptPartialFixture(a, bytes[0..797], init_iv);
    defer a.free(initialization);
    const part = try encryptPartialFixture(a, bytes[797..11206], key.mediaIv(10));
    defer a.free(part);
    const parent = try encryptPartialFixture(a, bytes[797..20811], key.mediaIv(10));
    defer a.free(parent);
    const prefix = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-PART-INF:PART-TARGET=1\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\",IV=0x1\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\"\n";
    var client = EncryptedPartialHttp.Client{ .manifest = prefix ++ "#EXT-X-PART:DURATION=1,URI=\"p0.m4s\",INDEPENDENT=YES\n", .initialization = initialization, .part = part, .parent = parent };
    var transport = network.Adapter(EncryptedPartialHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8" };
    var reader = try Reader(EncryptedPartialHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    client.manifest = prefix ++ "#EXTINF:2,\nparent.m4s\n#EXT-X-ENDLIST\n";
    try reader.reload();
    var tail = (try reader.next()).?;
    defer tail.deinit();
    try std.testing.expectEqual(first.reader.packets.len, tail.reader.packets.len);
    try std.testing.expect(reader.ended());
}

const DeltaTestClock = struct {
    tick: u64 = std.time.ns_per_s,
    fn now(raw: ?*anyopaque) u64 {
        return @as(*DeltaTestClock, @ptrCast(@alignCast(raw.?))).tick;
    }
};
const delta_header = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-SERVER-CONTROL:CAN-SKIP-UNTIL=12\n";
const delta_full = delta_header ++ "#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\none.mp4\n#EXTINF:2,\ntwo.mp4\n";
const delta_tail = "#EXTINF:2,\nthree.mp4\n";
const DeltaHttp = struct {
    pub const Client = struct {
        responses: []const []const u8,
        directives: []const bool,
        at: usize = 0,
        fail_at: ?usize = null,
        pub fn get(self: *@This(), url: []const u8, _: anytype) !FakeHttp.Response {
            if (self.at >= self.responses.len) return error.UnexpectedRequest;
            const directed = std.mem.indexOf(u8, url, "&_HLS_skip=YES") != null;
            try std.testing.expectEqual(self.directives[self.at], directed);
            try std.testing.expect(std.mem.indexOf(u8, url, "?token=a%26b") != null);
            if (self.fail_at == self.at) return error.ConnectionReset;
            const bytes = self.responses[self.at];
            self.at += 1;
            return .{ .body = bytes };
        }
    };
};
test "live HLS delta reload merges inherited state and enforces snapshot age" {
    const a = std.testing.allocator;
    var clock = DeltaTestClock{};
    var client = DeltaHttp.Client{ .responses = &.{ delta_full, delta_header ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=2\n" ++ delta_tail, delta_full ++ delta_tail }, .directives = &.{ false, true, false } };
    var transport = network.Adapter(DeltaHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8?token=a%26b" };
    var reader = try Reader(DeltaHttp).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now_ns = DeltaTestClock.now } });
    defer reader.deinit();
    try reader.reloadDelta(); // Initial load always requests complete metadata.
    try std.testing.expect(reader.canSkipReload());
    clock.tick += std.time.ns_per_s;
    try reader.reloadDelta();
    try std.testing.expectEqual(@as(usize, 3), reader.playlist.?.segments.len);
    try std.testing.expectEqualStrings("init.mp4", reader.playlist.?.segments[2].initialization.uri);
    try std.testing.expectEqualStrings(transport.root_url, reader.mediaUrl());
    clock.tick += 7 * std.time.ns_per_s;
    try std.testing.expect(!reader.canSkipReload());
    try reader.reloadDelta(); // Too old: full reload, no directive.
    clock.tick = 0;
    try std.testing.expect(!reader.canSkipReload());
    try std.testing.expectEqual(@as(usize, 3), client.at);
}
test "live HLS delta missing history retries once and failed fetch preserves snapshot" {
    const a = std.testing.allocator;
    inline for (.{ false, true }) |fail| {
        var clock = DeltaTestClock{};
        var client = DeltaHttp.Client{ .responses = &.{ delta_full, delta_header ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=3\n#EXTINF:2,\nfour.mp4\n", delta_full ++ delta_tail ++ "#EXTINF:2,\nfour.mp4\n" }, .directives = &.{ false, true, false }, .fail_at = if (fail) 2 else null };
        var transport = network.Adapter(DeltaHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8?token=a%26b" };
        var reader = try Reader(DeltaHttp).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now_ns = DeltaTestClock.now } });
        defer reader.deinit();
        try reader.reload();
        if (fail) {
            try std.testing.expectError(error.ConnectionReset, reader.reloadDelta());
            try std.testing.expectEqual(@as(usize, 2), reader.playlist.?.segments.len);
            try std.testing.expectEqual(@as(?u64, 10), reader.next_sequence);
            client.fail_at = null;
            try reader.reload();
        } else try reader.reloadDelta();
        try std.testing.expectEqual(@as(usize, 4), reader.playlist.?.segments.len);
        try std.testing.expectEqual(@as(usize, 3), client.at);
    }
}
test "live HLS delta request preserves query bytes and refuses directive aliases" {
    const a = std.testing.allocator;
    const result = try deltaRequestUrl(a, "https://example.test/list.m3u8?token=a%26b", 4096);
    defer a.free(result);
    try std.testing.expectEqualStrings("https://example.test/list.m3u8?token=a%26b&_HLS_skip=YES", result);
    inline for (.{ "_HLS_skip=v2", "%5FHLS_skip=YES", "_HLS_msn=1", "%5FHLS_part=0" }) |query| try std.testing.expectError(error.InvalidHlsDeliveryDirective, deltaRequestUrl(a, "https://example.test/list.m3u8?" ++ query, 4096));
    try std.testing.expectError(error.ResourceLimitExceeded, deltaRequestUrl(a, "https://example.test/list.m3u8", 1));
}

test "live HLS delta descriptor mutation retains committed snapshot and cursor" {
    const a = std.testing.allocator;
    var clock = DeltaTestClock{};
    var client = DeltaHttp.Client{ .responses = &.{ delta_full, delta_header ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=1\n#EXTINF:2,\nchanged-two.mp4\n" }, .directives = &.{ false, true } };
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var transport = network.Adapter(DeltaHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8?token=a%26b", .pool = &pool };
    var reader = try Reader(DeltaHttp).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now_ns = DeltaTestClock.now } });
    defer reader.deinit();
    try reader.reload();
    const before = pool.snapshot();
    try std.testing.expectError(error.SourceVersionChanged, reader.reloadDelta());
    try std.testing.expectEqual(before, pool.snapshot());
    try std.testing.expectEqual(@as(?u64, 10), reader.next_sequence);
    try std.testing.expectEqualStrings("two.mp4", reader.playlist.?.segments[1].uri);
}
fn deltaReaderAllocationRun(a: std.mem.Allocator) !void {
    var clock = DeltaTestClock{};
    var client = DeltaHttp.Client{ .responses = &.{ delta_full, delta_header ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=2\n" ++ delta_tail }, .directives = &.{ false, true } };
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var transport = network.Adapter(DeltaHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/list.m3u8?token=a%26b", .pool = &pool };
    var reader = try Reader(DeltaHttp).init(a, &transport, transport.root_url, .{ .clock = .{ .context = &clock, .now_ns = DeltaTestClock.now } });
    defer reader.deinit();
    try reader.reload();
    try reader.reloadDelta();
}
test "live HLS delta reader releases planning and transport owners on every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, deltaReaderAllocationRun, .{});
}

test "live HLS combined blocking date-range directive preserves authentication and bounds" {
    const a = std.testing.allocator;
    const url = try deliveryRequestUrl(a, "https://example.test/list?token=a%26b", .{ .sequence = 18446744073709551615, .part = 2, .skip = .dateranges }, 4096);
    defer a.free(url);
    try std.testing.expectEqualStrings("https://example.test/list?token=a%26b&_HLS_msn=18446744073709551615&_HLS_part=2&_HLS_skip=v2", url);
    try std.testing.expectError(error.InvalidHlsDeliveryDirective, deliveryRequestUrl(a, "https://example.test/list", .{ .part = 0 }, 4096));
    try std.testing.expectError(error.InvalidHlsDeliveryDirective, deliveryRequestUrl(a, "https://example.test/list?%5FHLS_skip=v2", .{ .sequence = 10, .skip = .dateranges }, 4096));
    try std.testing.expectError(error.ResourceLimitExceeded, deliveryRequestUrl(a, "https://example.test/list", .{ .sequence = 10, .skip = .dateranges }, 10));
}
const date_live_header = "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,CAN-SKIP-UNTIL=12,CAN-SKIP-DATERANGES=YES\n";
const date_live_tags = "#EXT-X-DATERANGE:ID=\"a\",START-DATE=\"2026-01-01T00:00:00Z\",X-NOTE=\"old\"\n#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00Z\n";
const date_live_full = date_live_header ++ date_live_tags ++ "#EXTINF:2,\none.mp4\n#EXTINF:2,\ntwo.mp4\n";
const DateDeltaHttp = struct {
    pub const Client = struct {
        responses: []const []const u8,
        urls: []const []const u8,
        at: usize = 0,
        fail_at: ?usize = null,
        pub fn get(self: *@This(), url: []const u8, _: anytype) !FakeHttp.Response {
            if (self.at >= self.responses.len) return error.UnexpectedRequest;
            try std.testing.expectEqualStrings(self.urls[self.at], url);
            if (self.fail_at == self.at) return error.ConnectionReset;
            const bytes = self.responses[self.at];
            self.at += 1;
            return .{ .body = bytes };
        }
    };
};
test "live HLS v2 augments metadata combines blocking and falls back atomically on incomplete date history" {
    const a = std.testing.allocator;
    const root = "https://example.test/list.m3u8?token=a%26b";
    inline for (.{ false, true }) |fail| {
        var clock = DeltaTestClock{};
        var client = DateDeltaHttp.Client{
            .responses = &.{ date_live_full, date_live_header ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=2,RECENTLY-REMOVED-DATERANGES=\"\"\n#EXT-X-DATERANGE:ID=\"a\",X-NEW=1\n" ++ delta_tail, date_live_header ++ "#EXT-X-SKIP:SKIPPED-SEGMENTS=2\n" ++ delta_tail, date_live_full ++ delta_tail },
            .urls = &.{ root, root ++ "&_HLS_skip=v2", root ++ "&_HLS_msn=10&_HLS_skip=v2", root },
            .fail_at = if (fail) 3 else null,
        };
        var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
        defer std.debug.assert(pool.snapshot().host_bytes == 0);
        var transport = network.Adapter(DateDeltaHttp){ .allocator = a, .client = &client, .root_url = root, .pool = &pool };
        var reader = try Reader(DateDeltaHttp).init(a, &transport, root, .{ .clock = .{ .context = &clock, .now_ns = DeltaTestClock.now } });
        defer reader.deinit();
        try reader.reload();
        try reader.reloadDelta();
        try std.testing.expectEqual(@as(usize, 1), reader.playlist.?.date_ranges.len);
        try std.testing.expectEqualStrings("1", reader.playlist.?.date_ranges[0].attributes.get("X-NEW").?.value);
        if (fail) {
            try std.testing.expectError(error.ConnectionReset, reader.reloadBlockingDelta());
            try std.testing.expectEqual(@as(?u64, 10), reader.next_sequence);
            try std.testing.expectEqualStrings("1", reader.playlist.?.date_ranges[0].attributes.get("X-NEW").?.value);
            client.fail_at = null;
            try reader.reload();
        } else try reader.reloadBlockingDelta();
        try std.testing.expectEqual(@as(usize, 3), reader.playlist.?.segments.len);
        try std.testing.expectEqual(@as(usize, 4), client.at);
    }
}

const StreamCapture = struct {
    reader: *Reader(FakeHttp),
    held: [8]?live.Segment = @splat(null),
    count: usize = 0,
    early: usize = 0,
    fail: bool = false,
    acknowledge_failure: bool = false,
    fn deinit(self: *@This()) void {
        for (&self.held) |*entry| if (entry.*) |*owner| owner.deinit();
    }
    fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try std.testing.expect(!self.reader.ended());
        try std.testing.expectError(error.InvalidMediaStreamState, self.reader.reload());
        try std.testing.expectError(error.PendingMediaSegment, self.reader.next());
        try std.testing.expectEqual(@as(u64, 10), delivery.segment.?.hls_timing.?.sequence);
        if (!self.reader.transport.client.stream_complete) self.early += 1;
        if (self.fail) {
            if (self.acknowledge_failure) {
                self.held[self.count] = try delivery.take();
                self.count += 1;
                delivery.acknowledge();
            }
            return error.ConsumerFailure;
        }
        self.held[self.count] = try delivery.take();
        self.count += 1;
    }
};
test "live Reader streaming retries preserve manifest cursor accepted fragments and metadata across reload" {
    const a = std.testing.allocator;
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var client = FakeHttp.Client{ .manifest = first_manifest ++ "#EXT-X-ENDLIST\n", .fail_stream_prefix = true };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8", .pool = &pool, .limits = .{ .max_resource_bytes = 128 * 1024 } };
    var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
    var reader_live = true;
    defer if (reader_live) reader.deinit();
    try reader.reload();
    var capture = StreamCapture{ .reader = &reader };
    defer capture.deinit();
    try std.testing.expectError(error.ConnectionReset, reader.streamNext(&capture, StreamCapture.accept));
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    try std.testing.expectEqual(@as(usize, 1), reader.stream_ledger.acknowledged);
    try std.testing.expectEqual(@as(usize, 1), reader.ingest.?.segments);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    try std.testing.expect(!reader.ended());
    try reader.reload();
    client.fail_stream_prefix = false;
    const result = (try reader.streamNext(&capture, StreamCapture.accept)).?;
    try std.testing.expectEqual(@as(usize, 1), result.replayed);
    try std.testing.expectEqual(@as(usize, 5), result.acknowledged);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try std.testing.expectEqual(@as(u64, 11), reader.next_sequence.?);
    try std.testing.expectEqual(@as(usize, 5), client.calls);
    try std.testing.expect(reader.ended());
    try std.testing.expect((try reader.streamNext(&capture, StreamCapture.accept)) == null);
    for (capture.held[0..capture.count], 0..) |entry, index| {
        try std.testing.expectEqual(index, entry.?.ordinal);
        try std.testing.expectEqual(@as(usize, 0), entry.?.epoch);
    }
    reader.deinit();
    reader_live = false;
    var packet = try capture.held[0].?.reader.readPacket(0);
    defer packet.deinit();
    try std.testing.expect(packet.bytes.len > 0);
}
test "live Reader streaming consumer acknowledgement updates accepted clocks before failure" {
    inline for (.{ false, true }) |acknowledge_failure| {
        const a = std.testing.allocator;
        var client = FakeHttp.Client{ .manifest = first_manifest ++ "#EXT-X-ENDLIST\n" };
        var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
        var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
        defer reader.deinit();
        try reader.reload();
        var capture = StreamCapture{ .reader = &reader, .fail = true, .acknowledge_failure = acknowledge_failure };
        defer capture.deinit();
        try std.testing.expectError(error.ConsumerFailure, reader.streamNext(&capture, StreamCapture.accept));
        try std.testing.expectEqual(@as(usize, @intFromBool(acknowledge_failure)), reader.ingest.?.segments);
        try std.testing.expectEqual(acknowledge_failure, reader.ingest.?.next_decode_time != null);
        capture.fail = false;
        const result = (try reader.streamNext(&capture, StreamCapture.accept)).?;
        try std.testing.expectEqual(@as(usize, @intFromBool(acknowledge_failure)), result.replayed);
        try std.testing.expectEqual(@as(usize, 6), capture.count);
        try std.testing.expect(reader.ended());
    }
}
test "live Reader streaming reload mutation or removal keeps the committed snapshot and retry state" {
    const a = std.testing.allocator;
    var client = FakeHttp.Client{ .manifest = first_manifest, .fail_stream_prefix = true };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var capture = StreamCapture{ .reader = &reader };
    defer capture.deinit();
    try std.testing.expectError(error.ConnectionReset, reader.streamNext(&capture, StreamCapture.accept));
    const committed = reader.manifest.?.bytes.ptr;
    client.manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\nchanged.mp4\n";
    try std.testing.expectError(error.SourceVersionChanged, reader.reload());
    try std.testing.expectEqual(committed, reader.manifest.?.bytes.ptr);
    client.manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:11\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\nseg.mp4\n";
    try std.testing.expectError(error.MediaStreamGap, reader.reload());
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    try std.testing.expectEqual(@as(usize, 1), reader.stream_ledger.acknowledged);
    client.manifest = first_manifest;
    client.fail_stream_prefix = false;
    try reader.reload();
    _ = try reader.streamNext(&capture, StreamCapture.accept);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
}
test "live DASH Reader streaming marks only successful EOF as resource and period completion" {
    const Consumer = struct {
        count: usize = 0,
        fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const timing = delivery.segment.?.dash_timing.?;
            try std.testing.expectEqual(self.count, timing.fragment_index.?);
            try std.testing.expectEqual(self.count == 0, timing.startsPeriod());
            try std.testing.expectEqual(delivery.body_complete, timing.finishesPeriod());
            try std.testing.expectEqual(delivery.body_complete, timing.resource_complete);
            self.count += 1;
        }
    };
    var client = FakeHttp.Client{ .manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period duration='PT6S'><AdaptationSet><Representation id='v'><SegmentList duration='6'><Initialization sourceURL='init.mp4'/><SegmentURL media='seg.mp4'/></SegmentList></Representation></AdaptationSet></Period></MPD>" };
    var transport = network.Adapter(FakeHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var reader = try ReaderWithManifest(FakeHttp, @import("dash.zig")).init(std.testing.allocator, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var consumer = Consumer{};
    _ = try reader.streamNext(&consumer, Consumer.accept);
    try std.testing.expectEqual(@as(usize, 6), consumer.count);
    try std.testing.expect(reader.ended());
}
fn streamingAllocationRun(a: std.mem.Allocator) !void {
    var client = FakeHttp.Client{ .manifest = first_manifest ++ "#EXT-X-ENDLIST\n", .fail_stream_prefix = true };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(FakeHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var capture = StreamCapture{ .reader = &reader };
    defer capture.deinit();
    _ = reader.streamNext(&capture, StreamCapture.accept) catch |err| {
        if (err != error.ConnectionReset) return err;
    };
    client.fail_stream_prefix = false;
    _ = try reader.streamNext(&capture, StreamCapture.accept);
}
test "live Reader streaming allocation failures release manifests initialization and accepted snapshots" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, streamingAllocationRun, .{});
}

test "live Reader streaming epochs keep global ordinals and permit buffered reads only after EOF" {
    const Capture = struct {
        count: usize = 0,
        fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
            self.count += 1;
        }
    };
    var client = FakeHttp.Client{ .manifest = second_manifest };
    var transport = network.Adapter(FakeHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(FakeHttp).init(std.testing.allocator, &transport, transport.root_url, .{});
    defer reader.deinit();
    try reader.reload();
    var capture = Capture{};
    _ = try reader.streamNext(&capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    var next_unit = (try reader.next()).?;
    defer next_unit.deinit();
    try std.testing.expectEqual(@as(usize, 6), next_unit.ordinal);
    try std.testing.expectEqual(@as(usize, 1), next_unit.epoch);
    try std.testing.expectEqual(@as(u16, 96), next_unit.reader.track.width);
    try std.testing.expect(reader.ended());
}
test "live Reader streaming lifetime byte and accepted fragment limits remain cumulative" {
    const Capture = struct {
        count: usize = 0,
        fn accept(raw: *anyopaque, _: *network.FragmentDelivery) !void {
            @as(*@This(), @ptrCast(@alignCast(raw))).count += 1;
        }
    };
    inline for (.{ true, false }) |bytes_limited| {
        var client = FakeHttp.Client{ .manifest = first_manifest };
        var transport = network.Adapter(FakeHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
        var reader = try Reader(FakeHttp).init(std.testing.allocator, &transport, transport.root_url, .{ .ingest = .{ .max_ingested_bytes = if (bytes_limited) 1000 else 1024 * 1024, .max_segments = if (bytes_limited) 100 else 3 } });
        defer reader.deinit();
        try reader.reload();
        var capture = Capture{};
        try std.testing.expectError(error.ResourceLimitExceeded, reader.streamNext(&capture, Capture.accept));
        try std.testing.expectEqual(@as(usize, if (bytes_limited) 0 else 3), capture.count);
        try std.testing.expect(reader.ingest.?.ingested_bytes <= reader.options.ingest.max_ingested_bytes);
        try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    }
}

const partial_manifest = "<MPD xmlns='urn:mpeg:dash:schema:mpd:2011'><Period duration='PT6S'><AdaptationSet><Representation id='v'><SegmentList timescale='1000' duration='6000' availabilityTimeComplete='false'><Initialization sourceURL='init.mp4'/><SegmentURL media='seg.mp4'/></SegmentList></Representation></AdaptationSet></Period></MPD>";
const PartialReader = ReaderWithManifest(FakeHttp, @import("dash.zig"));
const PartialCapture = struct {
    held: [8]?live.Segment = @splat(null),
    count: usize = 0,
    completions: usize = 0,
    fn deinit(self: *@This()) void {
        for (&self.held) |*entry| if (entry.*) |*unit| unit.deinit();
    }
    fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const timing = delivery.segment.?.dash_timing.?;
        if (timing.resource_complete) self.completions += 1;
        try std.testing.expectEqual(timing.resource_complete, timing.finishesPeriod());
        try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
        self.held[self.count] = try delivery.take();
        self.count += 1;
    }
};
test "partial DASH Reader retains sequence through clean EOF unchanged polls and append-only growth" {
    const a = std.testing.allocator;
    var client = FakeHttp.Client{ .manifest = partial_manifest, .stream_prefix_bytes = 20811 - 797 };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var reader = try PartialReader.init(a, &transport, transport.root_url, .{ .selection = .{ .allow_partial_availability = true } });
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.StreamingMediaRequired, reader.next());
    try std.testing.expectEqual(@as(usize, 1), client.calls);
    var capture = PartialCapture{};
    defer capture.deinit();
    const first = (try reader.streamNext(&capture, PartialCapture.accept)).?;
    try std.testing.expect(!first.resource_complete);
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    try std.testing.expectEqual(@as(usize, 0), capture.completions);
    try std.testing.expect(!reader.ended());
    try std.testing.expectEqual(@as(u64, 1), reader.dash_position.?.sequence);
    try std.testing.expectError(error.PendingMediaSegment, reader.next());
    const unchanged = (try reader.streamNext(&capture, PartialCapture.accept)).?;
    try std.testing.expectEqual(@as(usize, 2), unchanged.replayed);
    try std.testing.expectEqual(@as(usize, 0), unchanged.acknowledged);
    try std.testing.expect(!unchanged.resource_complete);
    try reader.reload();
    client.stream_prefix_bytes = null;
    const complete = (try reader.streamNext(&capture, PartialCapture.accept)).?;
    try std.testing.expect(complete.resource_complete);
    try std.testing.expectEqual(@as(usize, 2), complete.replayed);
    try std.testing.expectEqual(@as(usize, 4), complete.acknowledged);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try std.testing.expectEqual(@as(usize, 1), capture.completions);
    try std.testing.expect(reader.ended());
    try std.testing.expectEqual(@as(u64, 2), reader.dash_position.?.sequence);
}
test "partial DASH declaration completion still fetches and validates an unfinished body" {
    const a = std.testing.allocator;
    const final_manifest = try std.mem.replaceOwned(u8, a, partial_manifest, "availabilityTimeComplete='false'", "availabilityTimeComplete='true'");
    defer a.free(final_manifest);
    var client = FakeHttp.Client{ .manifest = partial_manifest, .stream_prefix_bytes = 20811 - 797 };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var reader = try PartialReader.init(a, &transport, transport.root_url, .{ .selection = .{ .allow_partial_availability = true } });
    defer reader.deinit();
    try reader.reload();
    var capture = PartialCapture{};
    defer capture.deinit();
    _ = try reader.streamNext(&capture, PartialCapture.accept);
    client.manifest = final_manifest;
    try reader.reload();
    try std.testing.expectError(error.IncompleteMediaSegment, reader.streamNext(&capture, PartialCapture.accept));
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    try std.testing.expectEqual(@as(u64, 1), reader.dash_position.?.sequence);
    client.stream_prefix_bytes = null;
    const complete = (try reader.streamNext(&capture, PartialCapture.accept)).?;
    try std.testing.expect(complete.resource_complete);
    try std.testing.expect(reader.ended());
}
test "partial DASH retries and body versions obey caller-configured attempt limits" {
    var client = FakeHttp.Client{ .manifest = partial_manifest, .stream_prefix_bytes = 20811 - 797 };
    var transport = network.Adapter(FakeHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var reader = try PartialReader.init(std.testing.allocator, &transport, transport.root_url, .{ .max_resource_attempts = 1, .selection = .{ .allow_partial_availability = true } });
    defer reader.deinit();
    try reader.reload();
    var capture = PartialCapture{};
    defer capture.deinit();
    _ = try reader.streamNext(&capture, PartialCapture.accept);
    try std.testing.expectError(error.ResourceLimitExceeded, reader.streamNext(&capture, PartialCapture.accept));
    try std.testing.expectEqual(@as(usize, 3), client.calls);
    try std.testing.expect(!reader.ended());
}
fn partialAllocationRun(a: std.mem.Allocator) !void {
    var client = FakeHttp.Client{ .manifest = partial_manifest, .stream_prefix_bytes = 20811 - 797 };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var reader = try PartialReader.init(a, &transport, transport.root_url, .{ .selection = .{ .allow_partial_availability = true } });
    defer reader.deinit();
    try reader.reload();
    var capture = PartialCapture{};
    defer capture.deinit();
    _ = try reader.streamNext(&capture, PartialCapture.accept);
    client.stream_prefix_bytes = null;
    _ = try reader.streamNext(&capture, PartialCapture.accept);
}
test "partial DASH append-only version allocation failures preserve owners and release prefix leases" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, partialAllocationRun, .{});
}

test "partial DASH rejects a new fragment decode gap without advancing accepted output" {
    const a = std.testing.allocator;
    const original = @embedFile("../testdata/fragmented.mp4")[797..];
    const changed = try a.dupe(u8, original);
    defer a.free(changed);
    const third = 20811 - 797;
    const tfdt = third + std.mem.indexOf(u8, changed[third..], "tfdt").?;
    if (changed[tfdt + 4] == 1) {
        const field = changed[tfdt + 8 ..][0..8];
        std.mem.writeInt(u64, field, std.mem.readInt(u64, field, .big) + 1024, .big);
    } else {
        const field = changed[tfdt + 8 ..][0..4];
        std.mem.writeInt(u32, field, std.mem.readInt(u32, field, .big) + 1024, .big);
    }
    var client = FakeHttp.Client{ .manifest = partial_manifest, .stream_prefix_bytes = third };
    var transport = network.Adapter(FakeHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var reader = try PartialReader.init(a, &transport, transport.root_url, .{ .selection = .{ .allow_partial_availability = true } });
    defer reader.deinit();
    try reader.reload();
    var capture = PartialCapture{};
    defer capture.deinit();
    _ = try reader.streamNext(&capture, PartialCapture.accept);
    client.stream_prefix_bytes = null;
    client.stream_body = changed;
    try std.testing.expectError(error.MediaStreamGap, reader.streamNext(&capture, PartialCapture.accept));
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    try std.testing.expectEqual(@as(u64, 1), reader.dash_position.?.sequence);
    client.stream_body = null;
    const result = (try reader.streamNext(&capture, PartialCapture.accept)).?;
    try std.testing.expect(result.resource_complete);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try std.testing.expect(reader.ended());
}

const aes_stream_manifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\",IV=0x1\n#EXT-X-MAP:URI=\"init1\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"key1\"\n#EXTINF:6,\nmedia10\n#EXT-X-ENDLIST\n";
const EncryptedCapture = struct {
    count: usize = 0,
    held: [8]?live.Segment = @splat(null),
    fn deinit(self: *@This()) void {
        for (&self.held) |*entry| if (entry.*) |*unit| unit.deinit();
    }
    fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
        self.held[self.count] = try delivery.take();
        self.count += 1;
    }
};
test "live HLS encrypted streaming refreshes keys on reload and preserves accepted fragments across transport failure" {
    var client = AesHttp.Client{ .manifest = aes_stream_manifest, .stream_body = @embedFile("../testdata/hls/aes-stream/media.bin"), .fail_stream_prefix = true };
    var transport = network.Adapter(AesHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(std.testing.allocator, &transport, transport.root_url, .{});
    defer reader.deinit();
    var capture = EncryptedCapture{};
    defer capture.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConnectionReset, reader.streamNext(&capture, EncryptedCapture.accept));
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    try std.testing.expectEqual(@as(usize, 1), client.key_calls);
    try reader.reload();
    client.fail_stream_prefix = false;
    const result = (try reader.streamNext(&capture, EncryptedCapture.accept)).?;
    try std.testing.expectEqual(@as(usize, 1), result.replayed);
    try std.testing.expectEqual(@as(usize, 5), result.acknowledged);
    try std.testing.expectEqual(@as(usize, 2), client.key_calls);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try std.testing.expectEqual(@as(u64, 11), reader.next_sequence.?);
    try std.testing.expect(reader.ended());
    var packet = try capture.held[0].?.reader.readPacket(0);
    defer packet.deinit();
    try std.testing.expect(packet.bytes.len != 0);
}
test "live HLS encrypted streaming reused key URI cannot change an accepted resource binding" {
    var client = AesHttp.Client{ .manifest = aes_stream_manifest, .stream_body = @embedFile("../testdata/hls/aes-stream/media.bin"), .fail_stream_prefix = true };
    var transport = network.Adapter(AesHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(std.testing.allocator, &transport, transport.root_url, .{});
    defer reader.deinit();
    var capture = EncryptedCapture{};
    defer capture.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConnectionReset, reader.streamNext(&capture, EncryptedCapture.accept));
    try reader.reload();
    client.changed_key1 = true;
    client.fail_stream_prefix = false;
    const calls = client.calls;
    try std.testing.expectError(error.SourceVersionChanged, reader.streamNext(&capture, EncryptedCapture.accept));
    try std.testing.expectEqual(calls + 1, client.calls); // Key refresh only, no media request.
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
}
test "live HLS encrypted streaming rotates keys between resources without invalidating prior packets" {
    var client = AesHttp.Client{};
    var transport = network.Adapter(AesHttp){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(std.testing.allocator, &transport, transport.root_url, .{});
    defer reader.deinit();
    var capture = EncryptedCapture{};
    defer capture.deinit();
    try reader.reload();
    _ = try reader.streamNext(&capture, EncryptedCapture.accept);
    var old = try capture.held[0].?.reader.readPacket(0);
    defer old.deinit();
    const copy = try std.testing.allocator.dupe(u8, old.bytes);
    defer std.testing.allocator.free(copy);
    _ = try reader.streamNext(&capture, EncryptedCapture.accept);
    try std.testing.expectEqual(@as(usize, 2), client.key_calls);
    try std.testing.expect(reader.ended());
    try std.testing.expectEqualSlices(u8, copy, old.bytes);
}
fn encryptedReaderAllocations(a: std.mem.Allocator) !void {
    var client = AesHttp.Client{ .manifest = aes_stream_manifest, .stream_body = @embedFile("../testdata/hls/aes-stream/media.bin"), .fail_stream_prefix = true };
    var transport = network.Adapter(AesHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try Reader(AesHttp).init(a, &transport, transport.root_url, .{});
    defer reader.deinit();
    var capture = EncryptedCapture{};
    defer capture.deinit();
    try reader.reload();
    _ = reader.streamNext(&capture, EncryptedCapture.accept) catch |err| {
        if (err != error.ConnectionReset) return err;
    };
    client.fail_stream_prefix = false;
    _ = try reader.streamNext(&capture, EncryptedCapture.accept);
}
test "live HLS encrypted streaming allocation failures drain keys initialization and independent packets" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, encryptedReaderAllocations, .{});
}
