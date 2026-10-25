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

//! Native TS AVC packet adapter. PES clocks remain in the 90 kHz track domain.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
pub const Options = struct {
    transport: media.transport_stream.Limits = .{},
    stream: media.stream.Options = .{},
    index: media.mp4.Limits = .{},
    /// Bootstrap a continuation segment whose payload omits parameter sets.
    configuration: ?[]const u8 = null,
    last_duration_ticks: ?u32 = null,
    single_access_unit_duration_ticks: ?u32 = null,
};
pub const Reader = struct {
    snapshot: *media.stream.Snapshot,
    reader: media.mp4.Reader,
    next_state: media.transport_stream.State,
    pub fn deinit(self: *@This()) void {
        self.reader.deinit();
        self.snapshot.deinit();
        self.* = undefined;
    }
    pub fn init(a: std.mem.Allocator, identity: []const u8, bytes: []const u8, state: media.transport_stream.State, options: Options) !Reader {
        var demux = try media.transport_stream.demux(a, bytes, state, options.transport, options.stream.control, options.stream.admission_pool);
        defer demux.deinit();
        return fromDemux(a, identity, demux, options);
    }
};
const Nals = struct {
    bytes: []const u8,
    cursor: usize = 0,
    last_start: usize = 0,
    control: media.source.Control = .{},
    fn start(bytes: []const u8, offset: usize, control: media.source.Control) !?struct { at: usize, payload: usize } {
        var i = offset;
        while (i + 3 <= bytes.len) : (i += 1) {
            if (i % 4096 == 0) try control.check();
            if (bytes[i] != 0 or bytes[i + 1] != 0) continue;
            if (bytes[i + 2] == 1) return .{ .at = i, .payload = i + 3 };
            if (i + 4 <= bytes.len and bytes[i + 2] == 0 and bytes[i + 3] == 1) return .{ .at = i, .payload = i + 4 };
        }
        return null;
    }
    fn next(self: *Nals) !?[]const u8 {
        if (self.cursor == self.bytes.len) return null;
        const first = (try start(self.bytes, self.cursor, self.control)) orelse return error.MalformedAnnexB;
        for (self.bytes[self.cursor..first.at]) |value| if (value != 0) return error.MalformedAnnexB;
        self.last_start = first.at;
        const following = try start(self.bytes, first.payload, self.control);
        var end = if (following) |value| value.at else self.bytes.len;
        while (end > first.payload and self.bytes[end - 1] == 0) end -= 1;
        if (end == first.payload or self.bytes[first.payload] & 128 != 0) return error.MalformedAnnexB;
        self.cursor = if (following) |value| value.at else self.bytes.len;
        return self.bytes[first.payload..end];
    }
};
fn configuration(a: std.mem.Allocator, demux: media.transport_stream.Demux, options: Options) ![]u8 {
    var sps: ?[]const u8 = null;
    var pps: ?[]const u8 = null;
    var nals = Nals{ .bytes = demux.bytes, .control = options.stream.control };
    while (try nals.next()) |nal| {
        try options.stream.control.check();
        if (nal[0] & 31 == 7 and sps == null) sps = nal;
        if (nal[0] & 31 == 8 and pps == null) pps = nal;
        if (nal[0] & 31 == 1 or nal[0] & 31 == 2 or nal[0] & 31 == 5) break;
    }
    if (sps == null or pps == null) return a.dupe(u8, options.configuration orelse return error.MissingVideoParameterSet);
    if (sps.?.len < 4 or sps.?.len > 65535 or pps.?.len > 65535) return error.MalformedVideoConfig;
    const out = try a.alloc(u8, 11 + sps.?.len + pps.?.len);
    out[0..6].* = .{ 1, sps.?[1], sps.?[2], sps.?[3], 0xff, 0xe1 };
    std.mem.writeInt(u16, out[6..8], @intCast(sps.?.len), .big);
    @memcpy(out[8..][0..sps.?.len], sps.?);
    var offset = 8 + sps.?.len;
    out[offset] = 1;
    offset += 1;
    std.mem.writeInt(u16, out[offset..][0..2], @intCast(pps.?.len), .big);
    @memcpy(out[offset + 2 ..], pps.?);
    return out;
}
const AccessUnits = struct {
    allocator: std.mem.Allocator,
    packets: []media.transport_stream.Packet,
    reservation: media.admission.Token = .{},
    fn deinit(self: *@This()) void {
        self.allocator.free(self.packets);
        self.reservation.deinit();
    }
};
fn accessUnits(a: std.mem.Allocator, demux: media.transport_stream.Demux, options: Options) !AccessUnits {
    var nals = Nals{ .bytes = demux.bytes, .control = options.stream.control };
    var count: usize = 0;
    while (try nals.next()) |nal| if (nal[0] & 31 == 9) {
        if (nal.len != 2 or nal[0] & 0x60 != 0 or nal[1] & 31 != 16) return error.MalformedVideoPacket;
        count += 1;
        if (count > options.index.max_samples) return error.ResourceLimitExceeded;
    };
    if (count == 0) count = demux.packets.len; // Legacy PES-aligned streams without AUD.
    if (count > options.index.max_samples) return error.ResourceLimitExceeded;
    const bytes = try std.math.mul(usize, count, @sizeOf(media.transport_stream.Packet));
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = bytes }) else media.admission.Token{};
    errdefer reservation.deinit();
    const packets = try a.alloc(media.transport_stream.Packet, count);
    errdefer a.free(packets);
    nals = .{ .bytes = demux.bytes, .control = options.stream.control };
    var unit: usize = 0;
    var pes: usize = 0;
    var last_clock_pes: ?usize = null;
    var has_vcl = false;
    while (try nals.next()) |nal| {
        const kind = nal[0] & 31;
        if (kind == 9) {
            if (unit > 0) {
                if (!has_vcl) return error.IncompleteVideoPicture;
                packets[unit - 1].size = nals.last_start - packets[unit - 1].offset;
            } else {
                // Annex B permits leading_zero_8bits before the first NAL.
                // Other NAL payload before an AUD is not part of this AU.
                for (demux.bytes[0..nals.last_start]) |byte| if (byte != 0) return error.MalformedAnnexB;
            }
            while (pes + 1 < demux.packets.len and demux.packets[pes + 1].offset <= nals.last_start) pes += 1;
            const clock = demux.packets[pes];
            if (!clock.has_timestamp or (last_clock_pes != null and last_clock_pes.? == pes)) return error.AmbiguousTransportPictureTiming;
            packets[unit] = .{ .offset = if (unit == 0) 0 else nals.last_start, .size = 0, .pts = clock.pts, .dts = clock.dts };
            unit += 1;
            last_clock_pes = pes;
            has_vcl = false;
        } else if (kind == 1 or kind == 2 or kind == 5) has_vcl = true;
    }
    if (unit == 0) {
        for (demux.packets, packets) |packet, *out| {
            if (!packet.has_timestamp) return error.AmbiguousTransportPictureTiming;
            out.* = packet;
        }
    } else {
        if (!has_vcl or unit != packets.len) return error.IncompleteVideoPicture;
        packets[unit - 1].size = demux.bytes.len - packets[unit - 1].offset;
        var clocks: usize = 0;
        for (demux.packets) |packet| if (packet.has_timestamp) {
            clocks += 1;
        };
        if (clocks != unit) return error.AmbiguousTransportPictureTiming;
    }
    return .{ .allocator = a, .packets = packets, .reservation = reservation };
}
pub fn fromDemux(a: std.mem.Allocator, identity: []const u8, demux: media.transport_stream.Demux, options: Options) !Reader {
    if (demux.packets.len == 0 or demux.packets.len > options.transport.max_packets) return error.ResourceLimitExceeded;
    try options.stream.control.check();
    if (options.configuration) |bytes| if (bytes.len > options.stream.max_bytes) return error.ResourceLimitExceeded;
    // Two u16-length parameter sets plus AVCC overhead. Config parsing copies
    // RBSP bytes and may retain an FMO map: at least one coded bit per map entry
    // bounds that map by eight times the parameter bytes.
    const config_bound = @max(@min(@as(usize, 131081), try std.math.add(usize, demux.bytes.len, 11)), if (options.configuration) |bytes| bytes.len else 0);
    var config_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(usize, config_bound, 12) }) else media.admission.Token{};
    defer config_reservation.deinit();
    var units = try accessUnits(a, demux, options);
    defer units.deinit();
    const avcc = try configuration(a, demux, options);
    defer a.free(avcc);
    const config = try h264.configParse(a, avcc);
    defer config.groups.deinit(a);
    if (config.width > options.index.max_dimension or config.height > options.index.max_dimension) return error.ResourceLimitExceeded;
    const normalized_bound = @min(options.stream.max_bytes, try std.math.add(usize, avcc.len, try std.math.add(usize, demux.bytes.len, demux.bytes.len / 4)));
    const packet_bytes = try std.math.mul(usize, units.packets.len, @sizeOf(media.mp4.Packet));
    // Normalization capacity is bounded by actual input, not a blanket stream
    // maximum. Snapshot storage has its own reservation during copy overlap.
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, try std.math.add(usize, try std.math.mul(usize, normalized_bound, 2), 64), packet_bytes) }) else media.admission.Token{};
    errdefer reservation.deinit();
    const packets = try a.alloc(media.mp4.Packet, units.packets.len);
    errdefer a.free(packets);
    var normalized: std.ArrayList(u8) = .empty;
    var normalized_live = true;
    defer if (normalized_live) normalized.deinit(a);
    try normalized.appendSlice(a, avcc);
    for (units.packets, 0..) |packet, i| {
        try options.stream.control.check();
        const offset = normalized.items.len;
        var nals = Nals{ .bytes = demux.bytes[packet.offset..][0..packet.size], .control = options.stream.control };
        var sync = false;
        var auds: usize = 0;
        var count: usize = 0;
        while (try nals.next()) |nal| {
            try options.stream.control.check();
            count += 1;
            if (count > 4096) return error.ResourceLimitExceeded;
            if (nal[0] & 31 == 9) auds += 1;
            // More than one independently delimited picture needs additional
            // timestamp mapping; do not assign a PES clock to every picture.
            if (auds > 1) return error.AmbiguousTransportPictureTiming;
            if (nal[0] & 31 == 5) sync = true;
            if (nal.len + 4 > options.stream.max_bytes -| normalized.items.len) return error.ResourceLimitExceeded;
            var prefix: [4]u8 = undefined;
            std.mem.writeInt(u32, &prefix, std.math.cast(u32, nal.len) orelse return error.ResourceLimitExceeded, .big);
            try normalized.appendSlice(a, &prefix);
            try normalized.appendSlice(a, nal);
        }
        const size = normalized.items.len - offset;
        if (size == 0 or size > options.index.max_packet_bytes) return error.ResourceLimitExceeded;
        const duration: u32 = if (i + 1 < units.packets.len)
            std.math.cast(u32, @as(i128, units.packets[i + 1].dts) - packet.dts) orelse return error.InvalidSamplingMetadata
        else
            options.last_duration_ticks orelse if (i != 0) packets[i - 1].duration else options.single_access_unit_duration_ticks orelse return error.MissingTransportDuration;
        if (duration == 0) return error.InvalidSamplingMetadata;
        packets[i] = .{ .offset = offset, .size = @intCast(size), .dts = packet.dts, .media_dts = packet.dts, .pts = packet.pts, .media_pts = packet.pts, .duration = duration, .sync = sync };
    }
    const snapshot = try media.stream.Snapshot.copy(a, identity, &.{normalized.items}, options.stream);
    errdefer snapshot.deinit();
    var metadata = try snapshot.input.read(0, avcc.len);
    errdefer metadata.deinit();
    normalized.deinit(a);
    normalized_live = false;
    try reservation.resize(.{ .host_bytes = packets.len * @sizeOf(media.mp4.Packet) });
    const track = media.mp4.Track{ .codec = .avc, .inband_parameter_sets = true, .id = 1, .timescale = 90000, .width = @intCast(config.width), .height = @intCast(config.height), .avcc = metadata.bytes, .nal_length_bytes = 4, .display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 } };
    return .{ .snapshot = snapshot, .reader = .{ .allocator = a, .input = &snapshot.input, .metadata = metadata, .index_reservation = reservation, .track = track, .packets = packets, .limits = options.index }, .next_state = demux.next_state };
}

pub const StreamOptions = struct {
    video: Options = .{},
    epoch: usize = 0,
    first_ordinal: usize = 0,
    max_identity_bytes: usize = 4096,
};
/// Incremental Annex B AU normalization. Borrowed PES payloads are copied only
/// into the active AU/lookahead buffer. Output segments own their packet data.
/// One consumer; entered failures are terminal, with caller-managed replay.
pub const Stream = struct {
    const Anchor = struct { offset: usize, packet: media.transport_stream.Packet, ordinal: usize };
    allocator: std.mem.Allocator,
    options: StreamOptions,
    identity: []u8,
    identity_reservation: media.admission.Token = .{},
    configuration: ?[]u8 = null,
    configuration_reservation: media.admission.Token = .{},
    fixed_reservation: media.admission.Token = .{},
    workspace: media.admission.TrackedAllocator,
    bytes: std.ArrayList(u8) = .empty,
    anchors: std.ArrayList(Anchor) = .empty,
    scan_cursor: usize = 0,
    mode: enum { unknown, aud, pes } = .unknown,
    clock: ?Anchor = null,
    next_pes: usize = 0,
    payload_bytes: usize = 0,
    published: usize = 0,
    last_duration: ?u32 = null,
    ended: bool = false,
    active: bool = false,
    context: *anyopaque,
    accept: *const fn (*anyopaque, *media.http_stream.FragmentDelivery) anyerror!void,
    pub fn init(a: std.mem.Allocator, identity: []const u8, options: StreamOptions, context: *anyopaque, accept: *const fn (*anyopaque, *media.http_stream.FragmentDelivery) anyerror!void) !Stream {
        try options.video.stream.control.check();
        if (identity.len > options.max_identity_bytes or options.video.index.max_packet_bytes == 0) return error.ResourceLimitExceeded;
        const au_limit = @min(options.video.index.max_packet_bytes, options.video.stream.max_bytes);
        // Active AU plus incoming lookahead PES and moving allocation overlap.
        const bytes_bound = try std.math.mul(usize, try std.math.add(usize, au_limit, options.video.transport.max_pes_bytes), 3);
        const anchors_bound = try std.math.mul(usize, options.video.transport.max_packets, 3 * @sizeOf(Anchor));
        const bound = try std.math.add(usize, try std.math.add(usize, bytes_bound, anchors_bound), 4096);
        const pool = options.video.stream.admission_pool;
        var fixed = if (pool) |value| try value.acquire(.{ .host_bytes = @sizeOf(Stream) }) else media.admission.Token{};
        errdefer fixed.deinit();
        var id_token = if (pool) |value| try value.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
        errdefer id_token.deinit();
        const owned = try a.dupe(u8, identity);
        errdefer a.free(owned);
        var cfg_token = media.admission.Token{};
        errdefer cfg_token.deinit();
        var cfg: ?[]u8 = null;
        errdefer if (cfg) |value| a.free(value);
        if (options.video.configuration) |value| {
            if (value.len > options.video.stream.max_bytes) return error.ResourceLimitExceeded;
            if (pool) |shared| cfg_token = try shared.acquire(.{ .host_bytes = value.len });
            cfg = try a.dupe(u8, value);
        }
        const token = if (pool) |value| try value.acquire(.{}) else media.admission.Token{};
        var saved = options;
        saved.video.configuration = null;
        return .{ .allocator = a, .options = saved, .identity = owned, .identity_reservation = id_token, .configuration = cfg, .configuration_reservation = cfg_token, .fixed_reservation = fixed, .workspace = .{ .backing = a, .limit = bound, .reservation = token }, .context = context, .accept = accept };
    }
    pub fn deinit(self: *Stream) void {
        self.bytes.deinit(self.workspace.allocator());
        self.bytes = .empty;
        self.anchors.deinit(self.workspace.allocator());
        self.anchors = .empty;
        std.debug.assert(self.workspace.live == 0);
        self.workspace.reservation.deinit();
        self.allocator.free(self.identity);
        self.identity = &.{};
        if (self.configuration) |value| self.allocator.free(value);
        self.configuration = null;
        self.configuration_reservation.deinit();
        self.identity_reservation.deinit();
        self.fixed_reservation.deinit();
        self.ended = true;
    }
    fn failure(self: *Stream, err: anyerror) anyerror {
        const result = if (err == error.OutOfMemory and self.workspace.denied != null) self.workspace.denied.? else err;
        self.deinit();
        return result;
    }
    pub fn consumer(self: *Stream) media.transport_stream.Consumer {
        return .{ .context = self, .accept = acceptPes };
    }
    fn acceptPes(raw: *anyopaque, pes: media.transport_stream.Pes) !void {
        try @as(*Stream, @ptrCast(@alignCast(raw))).push(pes);
    }
    pub fn push(self: *Stream, pes: media.transport_stream.Pes) !void {
        if (self.ended or self.active) return error.InvalidTransportState;
        self.active = true;
        defer self.active = false;
        self.pushEntered(pes) catch |err| return self.failure(err);
    }
    fn pushEntered(self: *Stream, pes: media.transport_stream.Pes) !void {
        try self.options.video.stream.control.check();
        if (pes.ordinal != self.next_pes or pes.packet.offset != self.payload_bytes or pes.packet.size != pes.bytes.len) return error.InvalidTransportState;
        if (pes.bytes.len > self.options.video.transport.max_pes_bytes or pes.bytes.len > self.options.video.transport.max_bytes -| self.payload_bytes or self.next_pes >= self.options.video.transport.max_packets) return error.ResourceLimitExceeded;
        if (self.mode == .pes and self.bytes.items.len != 0) {
            if (!pes.packet.has_timestamp) return error.AmbiguousTransportPictureTiming;
            const duration = try durationBetween(self.clock.?.packet.dts, pes.packet.dts);
            try self.publish(self.bytes.items.len, duration, false);
            self.drop(self.bytes.items.len);
            self.clock = null;
        }
        const active_limit = @min(self.options.video.index.max_packet_bytes, self.options.video.stream.max_bytes);
        const bound = try std.math.add(usize, active_limit, self.options.video.transport.max_pes_bytes);
        if (pes.bytes.len > bound -| self.bytes.items.len) return error.ResourceLimitExceeded;
        const anchor = Anchor{ .offset = self.bytes.items.len, .packet = pes.packet, .ordinal = pes.ordinal };
        try self.anchors.append(self.workspace.allocator(), anchor);
        try self.bytes.appendSlice(self.workspace.allocator(), pes.bytes);
        self.next_pes += 1;
        self.payload_bytes += pes.bytes.len;
        try self.scan();
    }
    fn durationBetween(first: i64, second: i64) !u32 {
        const value = std.math.cast(u32, @as(i128, second) - first) orelse return error.InvalidSamplingMetadata;
        if (value == 0) return error.InvalidSamplingMetadata;
        return value;
    }
    fn clockAt(self: *Stream, offset: usize) !Anchor {
        var selected: ?Anchor = null;
        for (self.anchors.items) |anchor| {
            if (anchor.offset > offset) break;
            selected = anchor;
        }
        const value = selected orelse return error.AmbiguousTransportPictureTiming;
        if (!value.packet.has_timestamp) return error.AmbiguousTransportPictureTiming;
        return value;
    }
    fn scan(self: *Stream) !void {
        while (try Nals.start(self.bytes.items, self.scan_cursor, self.options.video.stream.control)) |nal| {
            if (nal.payload >= self.bytes.items.len) {
                self.scan_cursor = nal.at;
                return;
            }
            const kind = self.bytes.items[nal.payload] & 31;
            if (kind == 9 and self.bytes.items.len - nal.payload < 2) {
                self.scan_cursor = nal.at;
                return;
            }
            if (self.mode == .unknown) self.mode = if (kind == 9) .aud else .pes;
            if (self.clock == null) self.clock = try self.clockAt(0);
            if (kind == 9) {
                if (self.mode != .aud) return error.AmbiguousTransportPictureTiming;
                if (self.bytes.items[nal.payload] & 0x60 != 0 or self.bytes.items[nal.payload + 1] & 31 != 16) return error.MalformedVideoPacket;
                if (nal.at != 0) {
                    // A first AUD may have leading_zero_8bits, but preceding
                    // nonzero payload belongs to an already timed AU.
                    var only_zero = true;
                    for (self.bytes.items[0..nal.at]) |byte| if (byte != 0) {
                        only_zero = false;
                        break;
                    };
                    if (!only_zero) {
                        const next = try self.clockAt(nal.at);
                        if (next.ordinal == self.clock.?.ordinal) return error.AmbiguousTransportPictureTiming;
                        const duration = try durationBetween(self.clock.?.packet.dts, next.packet.dts);
                        try self.publish(nal.at, duration, false);
                        const payload = nal.payload - nal.at;
                        self.drop(nal.at);
                        self.clock = next;
                        self.scan_cursor = payload;
                        continue;
                    }
                }
            }
            self.scan_cursor = nal.payload;
        }
        self.scan_cursor = @max(self.scan_cursor, self.bytes.items.len -| 3);
    }
    fn drop(self: *Stream, count: usize) void {
        std.mem.copyForwards(u8, self.bytes.items, self.bytes.items[count..]);
        self.bytes.items.len -= count;
        var keep: usize = 0;
        for (self.anchors.items, 0..) |anchor, index| {
            if (anchor.offset > count) break;
            keep = index;
        }
        std.mem.copyForwards(Anchor, self.anchors.items, self.anchors.items[keep..]);
        self.anchors.items.len -= keep;
        for (self.anchors.items) |*anchor| anchor.offset -|= count;
        self.scan_cursor = 0;
        if (self.bytes.items.len == 0) self.anchors.clearRetainingCapacity();
    }
    fn publish(self: *Stream, count: usize, duration: u32, complete: bool) !void {
        if (self.published >= self.options.video.transport.max_packets or count > self.options.video.index.max_packet_bytes) return error.ResourceLimitExceeded;
        const ordinal = try std.math.add(usize, self.options.first_ordinal, self.published);
        const length = std.fmt.count("{s}/au/{d}", .{ self.identity, ordinal });
        if (length > self.options.max_identity_bytes) return error.ResourceLimitExceeded;
        var id_token = if (self.options.video.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = length }) else media.admission.Token{};
        var owners_live = true;
        errdefer if (owners_live) id_token.deinit();
        const identity = try self.allocator.alloc(u8, length);
        errdefer if (owners_live) self.allocator.free(identity);
        _ = try std.fmt.bufPrint(identity, "{s}/au/{d}", .{ self.identity, ordinal });
        const clock = self.clock orelse return error.AmbiguousTransportPictureTiming;
        var packet = clock.packet;
        packet.offset = 0;
        packet.size = count;
        var records = [_]media.transport_stream.Packet{packet};
        const demux = media.transport_stream.Demux{ .allocator = self.allocator, .bytes = self.bytes.items[0..count], .packets = &records, .next_state = .{} };
        var options = self.options.video;
        options.configuration = self.configuration;
        options.single_access_unit_duration_ticks = duration;
        options.last_duration_ticks = duration;
        var source = try fromDemux(self.allocator, identity, demux, options);
        errdefer if (owners_live) source.deinit();
        if (self.configuration) |config| if (!std.mem.eql(u8, config, source.reader.track.avcc)) return error.SourceVersionChanged;
        var cfg: ?[]u8 = null;
        var cfg_token = media.admission.Token{};
        defer {
            if (cfg) |value| self.allocator.free(value);
            cfg_token.deinit();
        }
        if (self.configuration == null) {
            if (options.stream.admission_pool) |pool| cfg_token = try pool.acquire(.{ .host_bytes = source.reader.track.avcc.len });
            cfg = try self.allocator.dupe(u8, source.reader.track.avcc);
        }
        // Transfer all owners into callback-scoped delivery before invoking it.
        var delivery = media.http_stream.FragmentDelivery{ .body_complete = complete, .segment = .{ .origin = .normalized_transport, .backing = source.snapshot, .reader = source.reader, .ordinal = ordinal, .epoch = self.options.epoch, .identity = identity, .identity_reservation = id_token } };
        // Ownership has moved; no errdefer may free these owners again.
        owners_live = false;
        try self.deliver(&delivery);
        if (cfg) |value| {
            self.configuration = value;
            cfg = null;
            self.configuration_reservation = cfg_token;
            cfg_token = .{};
        }
        self.last_duration = duration;
    }
    fn deliver(self: *Stream, delivery: *media.http_stream.FragmentDelivery) !void {
        defer if (delivery.segment) |*owner| owner.deinit();
        self.accept(self.context, delivery) catch |err| {
            if (delivery.acknowledged) self.published += 1;
            return err;
        };
        self.published += 1;
    }
    pub fn finish(self: *Stream, final_duration_ticks: ?u32) !void {
        if (self.ended or self.active) return error.InvalidTransportState;
        self.active = true;
        defer self.active = false;
        self.finishEntered(final_duration_ticks) catch |err| return self.failure(err);
        self.deinit();
    }
    fn finishEntered(self: *Stream, final_duration_ticks: ?u32) !void {
        try self.options.video.stream.control.check();
        if (self.bytes.items.len == 0 or self.clock == null) return error.EmptyVideo;
        const duration = final_duration_ticks orelse self.options.video.last_duration_ticks orelse self.last_duration orelse self.options.video.single_access_unit_duration_ticks orelse return error.MissingTransportDuration;
        if (duration == 0) return error.InvalidSamplingMetadata;
        try self.publish(self.bytes.items.len, duration, true);
    }
};

/// Request-local state for HLS TS video. Media controls the transport and AES;
/// this adapter supplies codec configuration and the existing packet-reader ABI.
pub const Hls = struct {
    allocator: std.mem.Allocator,
    state: media.transport_stream.State = .{},
    configuration: ?[]u8 = null,
    configuration_reservation: media.admission.Token = .{},
    discontinuity: ?u64 = null,
    ordinal: usize = 0,
    epoch: usize = 0,
    ingested_bytes: u64 = 0,
    pub fn deinit(self: *Hls) void {
        if (self.configuration) |bytes| self.allocator.free(bytes);
        self.configuration_reservation.deinit();
        self.* = undefined;
    }
    pub fn factory(self: *Hls) media.hls_live.TransportFactory {
        return .{ .context = self, .accept = accept, .stream = @import("transport_avc_http.zig").begin };
    }
    fn accept(raw: *anyopaque, identity: []const u8, bytes: []const u8, item: media.hls.Segment, limits: media.live_mp4.Options, control: media.source.Control, pool: ?*media.admission.Pool) anyerror!media.live_mp4.Segment {
        const self: *Hls = @ptrCast(@alignCast(raw));
        try control.check();
        if (self.ordinal >= limits.max_segments or bytes.len > limits.max_ingested_bytes -| self.ingested_bytes) return error.ResourceLimitExceeded;
        const reset = self.discontinuity != null and self.discontinuity.? != item.discontinuity_sequence;
        const epoch = std.math.add(usize, self.epoch, @intFromBool(reset)) catch return error.ResourceLimitExceeded;
        if (epoch > limits.max_reconfigurations) return error.ResourceLimitExceeded;
        var identity_reservation = if (pool) |p| try p.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
        errdefer identity_reservation.deinit();
        const owned_identity = try self.allocator.dupe(u8, identity);
        errdefer self.allocator.free(owned_identity);
        var options = Options{ .index = limits.index, .stream = limits.stream, .configuration = if (reset) null else self.configuration };
        options.stream.control = control;
        if (options.stream.admission_pool == null) options.stream.admission_pool = pool;
        options.transport.max_bytes = @min(options.transport.max_bytes, limits.stream.max_bytes);
        // A single PES needs an explicit duration. Multi-PES durations use their
        // decode clock differences; no extra picture timestamps are fabricated.
        var demux = try media.transport_stream.demux(self.allocator, bytes, if (reset) .{} else self.state, options.transport, control, options.stream.admission_pool);
        defer demux.deinit();
        const ticks = item.duration_seconds * 90000;
        if (!std.math.isFinite(ticks) or ticks < 1 or ticks > std.math.maxInt(u32)) return error.InvalidSamplingMetadata;
        options.single_access_unit_duration_ticks = @intFromFloat(@round(ticks));
        var source = try fromDemux(self.allocator, owned_identity, demux, options);
        errdefer source.deinit();
        var configuration_reservation = if (pool) |p| try p.acquire(.{ .host_bytes = source.reader.track.avcc.len }) else media.admission.Token{};
        errdefer configuration_reservation.deinit();
        const configuration_copy = try self.allocator.dupe(u8, source.reader.track.avcc);
        errdefer self.allocator.free(configuration_copy);
        if (!reset and self.configuration != null and !std.mem.eql(u8, self.configuration.?, configuration_copy)) return error.SourceVersionChanged;
        try control.check();
        if (self.configuration) |previous| self.allocator.free(previous);
        self.configuration_reservation.deinit();
        self.configuration = configuration_copy;
        self.configuration_reservation = configuration_reservation;
        self.state = source.next_state;
        self.discontinuity = item.discontinuity_sequence;
        self.epoch = epoch;
        self.ingested_bytes += bytes.len;
        const ordinal = self.ordinal;
        self.ordinal += 1;
        return .{ .origin = .normalized_transport, .backing = source.snapshot, .reader = source.reader, .ordinal = ordinal, .epoch = epoch, .identity = owned_identity, .identity_reservation = identity_reservation };
    }
};

test "native TS AVC packet adapter decodes every selected picture against independent FFmpeg" {
    const Oracle = struct {
        packets: []const struct { offset: usize, size: usize, pts: i64, dts: i64, duration: u32, sync: bool, sha256: []const u8 },
        width: u32,
        height: u32,
        time_base: []const u8,
        decoded_sha256: []const []const u8,
    };
    const Capture = struct {
        oracle: Oracle,
        count: usize = 0,
        fn accept(raw: *anyopaque, slot: usize, frame: *const h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const packet = self.oracle.packets[slot];
            var rank: usize = 0;
            for (self.oracle.packets, 0..) |other, index| if (other.pts < packet.pts or (other.pts == packet.pts and index < slot)) {
                rank += 1;
            };
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(frame.nv12, &hash, .{});
            try std.testing.expectEqualStrings(self.oracle.decoded_sha256[rank], &std.fmt.bytesToHex(hash, .lower));
            try std.testing.expectEqual(self.oracle.width, frame.width);
            try std.testing.expectEqual(self.oracle.height, frame.height);
            try std.testing.expectEqual(packet.pts, frame.pts);
            self.count += 1;
        }
    };
    const a = std.testing.allocator;
    inline for (.{
        .{ "closed", @embedFile("../testdata/transport/closed.json") },
        .{ "bframes", @embedFile("../testdata/transport/bframes.json") },
        .{ "closed-split", @embedFile("../testdata/transport/closed.json") },
        .{ "bframes-split", @embedFile("../testdata/transport/bframes.json") },
    }) |fixture| {
        const name = fixture[0];
        const oracle = try std.json.parseFromSlice(Oracle, a, fixture[1], .{});
        defer oracle.deinit();
        var source = try Reader.init(a, "transport-qualification", @embedFile("../testdata/transport/" ++ name ++ ".ts"), .{}, .{});
        defer source.deinit();
        const indexes = try a.alloc(usize, source.reader.packets.len);
        defer a.free(indexes);
        for (indexes, 0..) |*index, i| index.* = i;
        const session = try @import("h264_dynamic.zig").Session.init(a, &source.reader, .{});
        defer session.deinit();
        var capture = Capture{ .oracle = oracle.value };
        _ = try session.decodeSelected(indexes, &capture, Capture.accept);
        try std.testing.expectEqual(oracle.value.decoded_sha256.len, capture.count);
    }
}
fn adapterAllocationRun(backing: std.mem.Allocator, pool: *media.admission.Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    var source = try Reader.init(no_resize.allocator(), "transport-allocation", @embedFile("../testdata/transport/closed.ts"), .{}, .{ .stream = .{ .admission_pool = pool } });
    defer source.deinit();
    var lease = try source.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
}
test "native TS AVC adapter allocation failures release normalized payload metadata and admission" {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    try checkTransportAllocationFailures(adapterAllocationRun, .{&pool});
}

const TestHttp = struct {
    const initial = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:8\n#EXT-X-MEDIA-SEQUENCE:10\n#EXTINF:4,\nclosed.ts\n";
    const changed = initial ++ "#EXT-X-DISCONTINUITY\n#EXTINF:2,\nbframes.ts\n#EXT-X-ENDLIST\n";
    pub const Client = struct {
        manifest: []const u8 = initial,
        calls: usize = 0,
        fail: bool = false,
        complete: bool = false,
        body_override: ?[]const u8 = null,
        pub fn get(self: *@This(), uri: []const u8, _: anytype) !Response {
            self.calls += 1;
            if (std.mem.endsWith(u8, uri, "list.m3u8")) return .{ .body = self.manifest };
            if (self.fail) return error.ConnectionReset;
            if (std.mem.endsWith(u8, uri, "closed.ts")) return .{ .body = @embedFile("../testdata/transport/closed-split.ts") };
            if (std.mem.endsWith(u8, uri, "bframes.ts")) return .{ .body = @embedFile("../testdata/transport/bframes-split.ts") };
            return error.UnexpectedTransportResource;
        }
        pub fn getToWriter(self: *@This(), uri: []const u8, options: anytype, writer: anytype, _: anytype, _: anytype) !Response {
            self.complete = false;
            const response = try self.get(uri, options);
            try writer.startResponse(response);
            const bytes = self.body_override orelse response.body.?;
            var offset: usize = 0;
            while (offset < bytes.len) {
                const end = @min(bytes.len, offset + 512);
                try writer.writeAll(bytes[offset..end]);
                offset = end;
            }
            self.complete = true;
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
test "native TS AVC HLS factory preserves packet leases through discontinuity and transport retry" {
    const a = std.testing.allocator;
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{ .fail = true };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expectEqual(@as(usize, 0), codec.ordinal);
    client.fail = false;
    var first = (try reader.next()).?;
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.epoch);
    try std.testing.expect(first.origin == .normalized_transport);
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    const original = try a.dupe(u8, held.bytes);
    defer a.free(original);
    client.manifest = TestHttp.changed;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
    try std.testing.expectEqualSlices(u8, original, held.bytes);
    try std.testing.expect(reader.ended());
    try std.testing.expectEqual(@as(usize, 2), codec.ordinal);
}
fn hlsAllocationRun(backing: std.mem.Allocator, pool: *media.admission.Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{};
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8", .pool = pool };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    client.manifest = TestHttp.changed;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
}
test "native TS AVC HLS factory allocation failures reclaim both epochs and state" {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    try checkTransportAllocationFailures(hlsAllocationRun, .{&pool});
}

test "native TS AVC rejects reused or unowned PES timestamps" {
    const a = std.testing.allocator;
    var demux = try media.transport_stream.demux(a, @embedFile("../testdata/transport/closed-split.ts"), .{}, .{}, .{}, null);
    defer demux.deinit();
    var units = try accessUnits(a, demux, .{});
    defer units.deinit();
    try std.testing.expect(units.packets.len < demux.packets.len);
    demux.packets[0].has_timestamp = false;
    try std.testing.expectError(error.AmbiguousTransportPictureTiming, accessUnits(a, demux, .{}));
    demux.packets[0].has_timestamp = true;
    demux.packets[1].has_timestamp = true;
    try std.testing.expectError(error.AmbiguousTransportPictureTiming, accessUnits(a, demux, .{}));
}

fn fuzzSeed(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        var encoded: [4 + bytes.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], bytes.len, .little);
        @memcpy(encoded[4..], bytes);
        const frozen = encoded;
        break :blk &frozen;
    };
}
const fuzz_seeds = .{
    fuzzSeed(@embedFile("../testdata/transport/closed.ts")),
    fuzzSeed(@embedFile("../testdata/transport/bframes.ts")),
    fuzzSeed(@embedFile("../testdata/transport/closed-split.ts")),
    fuzzSeed(@embedFile("../testdata/transport/bframes-split.ts")),
};
const fuzz_options = Options{ .transport = .{ .max_bytes = 256 * 1024, .max_packets = 1024, .max_pes_bytes = 128 * 1024 }, .stream = .{ .max_bytes = 512 * 1024 }, .index = .{ .max_samples = 256 } };
test "TS normalization fuzz seeds retain complete timed access units" {
    inline for (fuzz_seeds) |seed| {
        var smith = std.testing.Smith{ .in = seed };
        var bytes: [256 * 1024]u8 = undefined;
        const length = smith.sliceWithHash(&bytes, 0x54534101);
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 8 * 1024 * 1024 } };
        var options = fuzz_options;
        options.stream.admission_pool = &pool;
        var reader = try Reader.init(std.testing.allocator, "fuzz-seed", bytes[0..length], .{}, options);
        try std.testing.expect(reader.reader.packets.len > 0);
        reader.deinit();
        try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    }
}
test "TS normalization fuzz bounded NAL framing clock ownership and cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [256 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0x54534101);
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 8 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            var options = fuzz_options;
            options.stream.admission_pool = &pool;
            var reader = Reader.init(std.testing.allocator, "fuzz", bytes[0..length], .{}, options) catch return;
            defer reader.deinit();
            for (reader.reader.packets, 0..) |packet, index| {
                try std.testing.expect(packet.duration > 0);
                var lease = try reader.reader.readPacket(index);
                defer lease.deinit();
                try std.testing.expectEqual(@as(usize, packet.size), lease.bytes.len);
            }
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &fuzz_seeds });
}

test "native TS AVC access units preserve Annex B leading zero bytes" {
    const a = std.testing.allocator;
    var demux = try media.transport_stream.demux(a, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{}, null);
    defer demux.deinit();
    const prefixed = try a.alloc(u8, demux.bytes.len + 1);
    prefixed[0] = 0;
    @memcpy(prefixed[1..], demux.bytes);
    a.free(demux.bytes);
    demux.bytes = prefixed;
    demux.packets[0].size += 1;
    for (demux.packets[1..]) |*packet| packet.offset += 1;
    var units = try accessUnits(a, demux, .{});
    defer units.deinit();
    try std.testing.expectEqual(demux.packets.len, units.packets.len);
    try std.testing.expectEqual(@as(usize, 0), units.packets[0].offset);
    try std.testing.expectEqual(demux.packets[0].size, units.packets[0].size);
    var reader = try fromDemux(a, "leading-zero", demux, .{});
    defer reader.deinit();
    try std.testing.expectEqual(demux.packets.len, reader.reader.packets.len);
}

const AuCapture = struct {
    held: [128]?media.live_mp4.Segment = @splat(null),
    count: usize = 0,
    fail: bool = false,
    fn deinit(self: *@This()) void {
        for (&self.held) |*unit| if (unit.*) |*value| value.deinit();
    }
    fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.fail) return error.ConsumerFailure;
        try std.testing.expectEqual(self.count, delivery.segment.?.ordinal);
        try std.testing.expectEqual(@as(usize, 1), delivery.segment.?.reader.packets.len);
        self.held[self.count] = try delivery.take();
        self.count += 1;
    }
    fn parity(self: *@This(), expected: *Reader) !void {
        try std.testing.expectEqual(expected.reader.packets.len, self.count);
        for (self.held[0..self.count], 0..) |entry, i| {
            var unit = entry.?;
            const packet = unit.reader.packets[0];
            const reference = expected.reader.packets[i];
            try std.testing.expectEqual(reference.media_dts, packet.media_dts);
            try std.testing.expectEqual(reference.media_pts, packet.media_pts);
            try std.testing.expectEqual(reference.duration, packet.duration);
            try std.testing.expectEqual(reference.sync, packet.sync);
            var left = try expected.reader.readPacket(i);
            defer left.deinit();
            var right = try unit.reader.readPacket(0);
            defer right.deinit();
            try std.testing.expectEqualSlices(u8, left.bytes, right.bytes);
            try std.testing.expectEqualSlices(u8, expected.reader.track.avcc, unit.reader.track.avcc);
        }
    }
};
test "native TS incremental AVC normalization joins split PES and preserves buffered packet clocks durations and payloads" {
    const a = std.testing.allocator;
    inline for (.{ "closed", "bframes", "closed-split", "bframes-split" }) |name| {
        const bytes = @embedFile("../testdata/transport/" ++ name ++ ".ts");
        var expected = try Reader.init(a, "au-stream-oracle", bytes, .{}, .{});
        defer expected.deinit();
        for ([_]usize{ 1, 188, 8192, bytes.len }) |chunk| {
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
            var capture = AuCapture{};
            defer capture.deinit();
            var normal = try Stream.init(a, "au-stream", .{ .video = .{ .stream = .{ .admission_pool = &pool } } }, &capture, AuCapture.accept);
            defer normal.deinit();
            var demux = try media.transport_stream.Stream.init(a, .{}, .{}, .{}, &pool, normal.consumer());
            defer demux.deinit();
            var offset: usize = 0;
            while (offset < bytes.len) {
                const end = @min(bytes.len, offset + chunk);
                try demux.push(bytes[offset..end]);
                offset = end;
            }
            try std.testing.expect(capture.count > 0);
            _ = try demux.finish();
            try normal.finish(null);
            try capture.parity(&expected);
            normal.deinit();
            // Snapshots remain valid after both incremental producers are gone.
            try capture.parity(&expected);
            capture.deinit();
            capture = .{};
            try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
        }
    }
}
fn auStreamAllocations(backing: std.mem.Allocator) !void {
    // Moving-growth failure counts must not depend on heap placement.
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var capture = AuCapture{};
    defer capture.deinit();
    var normal = try Stream.init(a, "au-allocation", .{}, &capture, AuCapture.accept);
    defer normal.deinit();
    var demux = try media.transport_stream.Stream.init(a, .{}, .{}, .{}, null, normal.consumer());
    defer demux.deinit();
    try demux.push(@embedFile("../testdata/transport/closed-split.ts"));
    _ = try demux.finish();
    try normal.finish(null);
}
test "native TS incremental AVC allocation failures release normalization configuration and retained snapshots" {
    try checkTransportAllocationFailures(auStreamAllocations, .{});
}
test "native TS incremental AVC consumer failure terminalizes without double-freeing transferred owners" {
    var capture = AuCapture{ .fail = true };
    defer capture.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
    var normal = try Stream.init(std.testing.allocator, "au-fail", .{ .video = .{ .stream = .{ .admission_pool = &pool } } }, &capture, AuCapture.accept);
    defer normal.deinit();
    var demux = try media.transport_stream.Stream.init(std.testing.allocator, .{}, .{}, .{}, &pool, normal.consumer());
    defer demux.deinit();
    try std.testing.expectError(error.ConsumerFailure, demux.push(@embedFile("../testdata/transport/closed.ts")));
    try std.testing.expect(normal.ended and demux.ended);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "native TS incremental AVC timed PES may begin in the previous AU and clocks belong to its later AUD" {
    const a = std.testing.allocator;
    var demux = try media.transport_stream.demux(a, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{}, null);
    defer demux.deinit();
    const first = demux.packets[0];
    const second = demux.packets[1];
    const stop = second.offset + second.size;
    const split = first.size - 7;
    var records = [_]media.transport_stream.Packet{ first, second };
    records[0].size = split;
    records[1].offset = split;
    records[1].size = stop - split;
    const joined = media.transport_stream.Demux{ .allocator = a, .bytes = demux.bytes[0..stop], .packets = &records, .next_state = demux.next_state };
    var expected = try fromDemux(a, "cross-PES-oracle", joined, .{});
    defer expected.deinit();
    var capture = AuCapture{};
    defer capture.deinit();
    var normal = try Stream.init(a, "cross-PES", .{}, &capture, AuCapture.accept);
    defer normal.deinit();
    try normal.push(.{ .bytes = joined.bytes[0..split], .packet = records[0], .ordinal = 0 });
    try normal.push(.{ .bytes = joined.bytes[split..], .packet = records[1], .ordinal = 1 });
    try normal.finish(null);
    try capture.parity(&expected);
}
test "native TS incremental AVC rejects multiple AUD clocks missing duration and untimed first AU" {
    const a = std.testing.allocator;
    var demux = try media.transport_stream.demux(a, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{}, null);
    defer demux.deinit();
    var capture = AuCapture{};
    defer capture.deinit();
    var normal = try Stream.init(a, "ambiguous", .{}, &capture, AuCapture.accept);
    defer normal.deinit();
    var packet = demux.packets[0];
    packet.size += demux.packets[1].size;
    try std.testing.expectError(error.AmbiguousTransportPictureTiming, normal.push(.{ .bytes = demux.bytes[0..packet.size], .packet = packet, .ordinal = 0 }));
    try std.testing.expect(normal.ended);
    normal = try Stream.init(a, "duration", .{}, &capture, AuCapture.accept);
    packet = demux.packets[0];
    try normal.push(.{ .bytes = demux.bytes[0..packet.size], .packet = packet, .ordinal = 0 });
    try std.testing.expectError(error.MissingTransportDuration, normal.finish(null));
    normal = try Stream.init(a, "untimed", .{}, &capture, AuCapture.accept);
    packet.has_timestamp = false;
    try std.testing.expectError(error.AmbiguousTransportPictureTiming, normal.push(.{ .bytes = demux.bytes[0..packet.size], .packet = packet, .ordinal = 0 }));
    try std.testing.expectEqual(@as(usize, 0), capture.count);
}

test "native TS incremental AVC PES aligned input without AUD retains the legacy timing contract" {
    const a = std.testing.allocator;
    var input = try media.transport_stream.demux(a, @embedFile("../testdata/transport/closed.ts"), .{}, .{}, .{}, null);
    defer input.deinit();
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    var records: std.ArrayList(media.transport_stream.Packet) = .empty;
    defer records.deinit(a);
    for (input.packets) |packet| {
        const payload = input.bytes[packet.offset..][0..packet.size];
        const aud = (try Nals.start(payload, 0, .{})).?;
        try std.testing.expectEqual(@as(u8, 9), payload[aud.payload] & 31);
        const next = (try Nals.start(payload, aud.payload, .{})).?;
        var record = packet;
        record.offset = bytes.items.len;
        record.size -= next.at;
        try records.append(a, record);
        try bytes.appendSlice(a, payload[next.at..]);
    }
    const demux = media.transport_stream.Demux{ .allocator = a, .bytes = bytes.items, .packets = records.items, .next_state = input.next_state };
    var expected = try fromDemux(a, "pes-aligned-oracle", demux, .{});
    defer expected.deinit();
    var capture = AuCapture{};
    defer capture.deinit();
    var normal = try Stream.init(a, "pes-aligned", .{}, &capture, AuCapture.accept);
    defer normal.deinit();
    for (records.items, 0..) |packet, ordinal| try normal.push(.{ .bytes = bytes.items[packet.offset..][0..packet.size], .packet = packet, .ordinal = ordinal });
    try normal.finish(null);
    try capture.parity(&expected);
}

test "native TS HTTP Reader streams normalized owned units and verifies acknowledged retry before new delivery" {
    const a = std.testing.allocator;
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{ .manifest = TestHttp.initial ++ "#EXT-X-ENDLIST\n" };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    const Capture = struct {
        units: AuCapture = .{},
        fail: bool = true,
        client: *TestHttp.Client,
        early: usize = 0,
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail and self.units.count == 2) return error.ConsumerFailure;
            if (!self.client.complete) self.early += 1;
            try AuCapture.accept(&self.units, delivery);
        }
    };
    var capture = Capture{ .client = &client };
    defer capture.units.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConsumerFailure, reader.streamNext(&capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 2), codec.ordinal);
    try std.testing.expectEqual(@as(usize, 2), reader.stream_ledger.acknowledged);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
    try std.testing.expectError(error.PendingMediaSegment, reader.next());
    capture.fail = false;
    const result = (try reader.streamNext(&capture, Capture.accept)).?;
    try std.testing.expectEqual(@as(usize, 2), result.replayed);
    try std.testing.expectEqual(capture.units.count - 2, result.acknowledged);
    try std.testing.expect(capture.early > 0);
    try std.testing.expect(reader.ended());
    var expected = try Reader.init(a, "http-ts-oracle", @embedFile("../testdata/transport/closed-split.ts"), .{}, .{});
    defer expected.deinit();
    try capture.units.parity(&expected);
}
fn httpTsAllocations(a: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    const scoped = no_resize.allocator();
    var codec = Hls{ .allocator = scoped };
    defer codec.deinit();
    var client = TestHttp.Client{ .manifest = TestHttp.initial ++ "#EXT-X-ENDLIST\n" };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = scoped, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(scoped, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    var capture = AuCapture{};
    defer capture.deinit();
    try reader.reload();
    _ = try reader.streamNext(&capture, AuCapture.accept);
}
test "native TS HTTP Reader allocation failure releases session parser normalized units and cursor owners" {
    try checkTransportAllocationFailures(httpTsAllocations, .{});
}

fn checkTransportAllocationFailures(comptime run: anytype, args: anytype) !void {
    // Keep exhaustive failure injection and SafeAllocator's leak, mismatch,
    // double-free and write-after-free checks. Capturing successful allocation
    // stacks in every replay dominates these whole-stream campaigns on Darwin.
    // FailingAllocator still retains the stack of the injected failure.
    var safe: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0, .check_write_after_free = true });
    defer std.debug.assert(safe.deinit() == 0);
    const traces = try std.testing.environ.containsUnempty(std.testing.allocator, "ANTFLY_TEST_ALLOCATOR_TRACES");
    try std.testing.checkAllAllocationFailures(if (traces) std.testing.allocator else safe.allocator(), run, args);
}

test "native TS HTTP acknowledged prefix mutation rejects before any new normalized unit" {
    const a = std.testing.allocator;
    const original = @embedFile("../testdata/transport/closed-split.ts");
    const damaged = try a.dupe(u8, original);
    defer a.free(damaged);
    var demux = try media.transport_stream.demux(a, original, .{}, .{}, .{}, null);
    defer demux.deinit();
    const first = for (demux.packets) |packet| {
        if (packet.size >= 64) break packet;
    } else return error.MissingTestPayload;
    const needle = demux.bytes[first.offset + first.size - 64 ..][0..8];
    const at = std.mem.indexOf(u8, damaged, needle).?;
    damaged[at + 7] ^= 1;
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{ .manifest = TestHttp.initial ++ "#EXT-X-ENDLIST\n" };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    const Capture = struct {
        units: AuCapture = .{},
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.units.count == 2) return error.ConsumerFailure;
            try AuCapture.accept(&self.units, delivery);
        }
    };
    var capture = Capture{};
    defer capture.units.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConsumerFailure, reader.streamNext(&capture, Capture.accept));
    client.body_override = damaged;
    try std.testing.expectError(error.SourceVersionChanged, reader.streamNext(&capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 2), capture.units.count);
    try std.testing.expect(reader.stream_ledger.invalid);
    try std.testing.expectEqual(@as(u64, 10), reader.next_sequence.?);
}
test "native TS HTTP validated EOF commits state and later discontinuity advances the decoder epoch" {
    const a = std.testing.allocator;
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{};
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    var capture = AuCapture{};
    defer capture.deinit();
    try reader.reload();
    _ = try reader.streamNext(&capture, AuCapture.accept);
    try std.testing.expect(codec.state.video_pid != null and codec.configuration != null);
    const before = capture.count;
    client.manifest = TestHttp.changed;
    try reader.reload();
    _ = try reader.streamNext(&capture, AuCapture.accept);
    try std.testing.expectEqual(@as(usize, 1), codec.epoch);
    try std.testing.expectEqual(@as(usize, 1), capture.held[before].?.epoch);
    try std.testing.expect(reader.ended());
    var lease = try capture.held[0].?.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
}

test "native TS HTTP buffered and streaming reads share the Reader lifetime byte limit" {
    const a = std.testing.allocator;
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{ .manifest = TestHttp.changed };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{
        .transport_stream = codec.factory(),
        .ingest = .{ .max_ingested_bytes = @embedFile("../testdata/transport/closed-split.ts").len },
    });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    try std.testing.expectEqual(reader.options.ingest.max_ingested_bytes, reader.stream_ts_bytes);
    const before = transport.requests;
    var capture = AuCapture{};
    defer capture.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, reader.streamNext(&capture, AuCapture.accept));
    try std.testing.expectEqual(before, transport.requests);
    try std.testing.expectEqual(@as(usize, 0), capture.count);
    try std.testing.expectEqual(@as(u64, 11), reader.next_sequence.?);
}

test "native TS HTTP cancellation after acknowledged delivery releases session and verifies retry" {
    const a = std.testing.allocator;
    const Capture = struct {
        cancelled: bool = false,
        units: AuCapture = .{},
        fn check(raw: ?*const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
        fn accept(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try AuCapture.accept(&self.units, delivery);
            if (self.units.count == 1) self.cancelled = true;
        }
    };
    var capture = Capture{};
    defer capture.units.deinit();
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{ .manifest = TestHttp.initial ++ "#EXT-X-ENDLIST\n" };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8", .control = .{ .context = &capture, .check_fn = Capture.check } };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.Cancelled, reader.streamNext(&capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 1), reader.stream_ledger.acknowledged);
    try std.testing.expect(!reader.stream_ledger.active and codec.configuration == null);
    capture.cancelled = false;
    const result = (try reader.streamNext(&capture, Capture.accept)).?;
    try std.testing.expectEqual(@as(usize, 1), result.replayed);
    try std.testing.expect(reader.ended());
}
