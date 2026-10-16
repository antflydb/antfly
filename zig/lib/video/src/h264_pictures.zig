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

//! Logical AVC presentation metadata, obtained from completed decoder assemblies.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const dynamic = @import("h264_dynamic.zig");
const sampling = @import("sampling.zig");
pub const Options = struct { max_packets: usize = 8192, max_pictures: usize = 8192, decode: h264.Options = .{ .max_dependency_packets = 8192, .max_parameter_packets = 8192 } };
pub const Index = struct {
    allocator: std.mem.Allocator,
    pictures: []h264.Picture,
    timescale: u32,
    packet_count: usize = 0,
    pending_pts: ?i64 = null,
    pending_first_packet: ?usize = null,
    decoded_packets: usize = 0,
    payload_bytes: u64 = 0,
    reservation: media.admission.Token = .{},
    pub fn deinit(self: *Index) void {
        self.allocator.free(self.pictures);
        self.reservation.deinit();
        self.* = undefined;
    }
    /// Own a metadata snapshot after whole-unit preroll eviction. The source
    /// owns its original allocation; this result can outlive it independently.
    pub fn cloneRebased(self: Index, a: std.mem.Allocator, base: usize, packet_count: usize, pool: ?*media.admission.Pool, control: media.source.Control) !Index {
        try control.check();
        const end = try std.math.add(usize, base, packet_count);
        if (end != self.packet_count) return error.InvalidLivePictureIndex;
        var count: usize = 0;
        for (self.pictures) |picture| {
            if (picture.first_packet >= end) return error.InvalidLivePictureIndex;
            if (picture.first_packet >= base) count += 1;
        }
        if (self.pending_first_packet) |pending| if (pending < base or pending >= end) return error.InvalidLivePictureIndex;
        const bytes = try std.math.mul(usize, count, @sizeOf(h264.Picture));
        var reservation = if (pool) |shared| try shared.acquire(.{ .host_bytes = bytes }) else media.admission.Token{};
        errdefer reservation.deinit();
        const owned = try a.alloc(h264.Picture, count);
        errdefer a.free(owned);
        var at: usize = 0;
        for (self.pictures) |picture| {
            try control.check();
            if (picture.first_packet < base) continue;
            owned[at] = picture;
            owned[at].first_packet -= base;
            if (picture.last_packet) |last| {
                if (last < picture.first_packet or last >= end) return error.InvalidLivePictureIndex;
                owned[at].last_packet = last - base;
            }
            at += 1;
        }
        return .{ .allocator = a, .pictures = owned, .timescale = self.timescale, .packet_count = packet_count, .pending_pts = self.pending_pts, .pending_first_packet = if (self.pending_first_packet) |pending| pending - base else null, .decoded_packets = self.decoded_packets, .payload_bytes = self.payload_bytes, .reservation = reservation };
    }
    /// Return transport selectors for logical pictures in presentation order.
    /// The first member selects the complete field/partition assembly on replay.
    pub fn select(self: Index) ![]usize {
        if (self.pictures.len == 0 or self.timescale == 0) return error.EmptyVideo;
        const order = try self.allocator.alloc(usize, self.pictures.len);
        defer self.allocator.free(order);
        for (order, 0..) |*id, i| id.* = i;
        const Less = struct {
            fn less(pictures: []const h264.Picture, l: usize, r: usize) bool {
                return pictures[l].pts < pictures[r].pts or (pictures[l].pts == pictures[r].pts and pictures[l].first_packet < pictures[r].first_packet);
            }
        };
        std.mem.sort(usize, order, @as([]const h264.Picture, self.pictures), Less.less);
        const first = self.pictures[order[0]].pts;
        const last = self.pictures[order[order.len - 1]];
        const span = @as(i128, last.pts) + last.duration - first;
        if (span <= 0) return error.InvalidSamplingMetadata;
        const seconds = @as(f64, @floatFromInt(span)) / @as(f64, @floatFromInt(self.timescale));
        const selected = try sampling.embeddingGemma2(self.allocator, .{ .total_frames = order.len, .fps = @as(f64, @floatFromInt(order.len)) / seconds, .duration_seconds = seconds }, .{});
        for (selected) |*id| id.* = self.pictures[order[id.*]].first_packet;
        return selected;
    }
};
const Collector = struct {
    allocator: std.mem.Allocator,
    pictures: std.ArrayList(h264.Picture) = .empty,
    planes: std.ArrayList(u8) = .empty,
    limit: usize,
    pending_pts: ?i64 = null,
    pending_first_packet: ?usize = null,
    fn pending(raw: *anyopaque, picture: h264.PendingPicture) void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        self.pending_pts = if (self.pending_pts) |old| @min(old, picture.pts) else picture.pts;
        self.pending_first_packet = if (self.pending_first_packet) |old| @min(old, picture.first_packet) else picture.first_packet;
    }
    fn accept(raw: *anyopaque, picture: h264.Picture) !void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        if (picture.independent_plane) |plane| {
            const mask = @as(u8, 1) << @intCast(plane);
            for (self.pictures.items, self.planes.items, 0..) |existing, seen, i| {
                if (existing.first_packet != picture.first_packet) continue;
                if (seen & mask != 0 or existing.pts != picture.pts or existing.duration != picture.duration or existing.width != picture.width or existing.height != picture.height or existing.bit_depth != picture.bit_depth) return error.MixedVideoPictures;
                if (picture.last_packet) |last| self.pictures.items[i].last_packet = @max(existing.last_packet orelse last, last);
                self.planes.items[i] |= mask;
                return;
            }
        }
        if (self.pictures.items.len >= self.limit) return error.ResourceLimitExceeded;
        var result = picture;
        if (picture.independent_plane != null) result.chroma_format = 3;
        result.independent_plane = null;
        try self.pictures.append(self.allocator, result);
        try self.planes.append(self.allocator, if (picture.independent_plane) |plane| @as(u8, 1) << @intCast(plane) else 0);
    }
};
/// Persistent static-configuration logical index. Published packets are immutable;
/// normalized content bindings detect prefix/configuration mutation before reuse.
/// Geometry/configuration epochs use a fresh owner. Failed reconstruction requires
/// destroying this owner and replaying retained preroll; snapshots stay independent.
pub const Stream = struct {
    allocator: std.mem.Allocator,
    options: Options,
    core: *h264.Incremental,
    registry: ?*dynamic.Session = null,
    epoch_base: usize = 0,
    position: usize = 0,
    collector: Collector,
    reservation: media.admission.Token = .{},
    allocations: media.admission.TrackedAllocator,
    prefix: [32]u8,
    valid: bool = true,
    last_stats: h264.Statistics = .{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 },
    pub fn init(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !*Stream {
        if (options.max_pictures == 0 or reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
        const slots = try std.math.mul(usize, try std.math.add(usize, options.max_pictures, 8), 3);
        const bytes = try std.math.add(usize, @sizeOf(Stream), try std.math.mul(usize, slots, @sizeOf(h264.Picture) + @sizeOf(u8)));
        var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(Stream) }) else media.admission.Token{};
        errdefer reservation.deinit();
        const self = try a.create(Stream);
        errdefer a.destroy(self);
        const prefix = try prefixHash(reader, 0, options.decode.max_packet_bytes);
        const registry = if (reader.track.inband_parameter_sets) try dynamic.Session.init(a, reader, options.decode) else null;
        errdefer if (registry) |owner| owner.deinit();
        var view = reader.*;
        if (registry) |owner| {
            const entry = owner.entries.items[owner.epochs.items[0].entry];
            view.track.avcc = entry.avcc;
            view.track.width = @intCast(entry.config.width);
            view.track.height = @intCast(entry.config.height);
            view.track.inband_parameter_sets = false;
        }
        const core = try h264.Incremental.init(a, &view, options.decode);
        errdefer core.deinit();
        const allocation_reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{}) else media.admission.Token{};
        self.* = .{ .allocator = a, .options = options, .core = core, .registry = registry, .collector = .{ .allocator = a, .limit = options.max_pictures }, .reservation = reservation, .allocations = .{ .backing = a, .reservation = allocation_reservation, .limit = bytes - @sizeOf(Stream) }, .prefix = prefix };
        self.collector.allocator = self.allocations.allocator();
        return self;
    }
    pub fn deinit(self: *Stream) void {
        self.core.deinit();
        if (self.registry) |owner| owner.deinit();
        self.collector.pictures.deinit(self.collector.allocator);
        self.collector.planes.deinit(self.collector.allocator);
        const a = self.allocator;
        var reservation = self.reservation;
        var allocation_reservation = self.allocations.reservation;
        std.debug.assert(self.allocations.live == 0);
        a.destroy(self);
        allocation_reservation.deinit();
        reservation.deinit();
    }
    pub fn advance(self: *Stream, reader: *media.mp4.Reader) !h264.Statistics {
        if (!self.valid) return error.InvalidVideoDecoder;
        if (reader.packets.len > self.options.max_packets or reader.packets.len <= self.position) return error.ResourceLimitExceeded;
        const previous = try prefixHash(reader, self.position, self.options.decode.max_packet_bytes);
        if (!std.mem.eql(u8, &previous, &self.prefix)) return error.InvalidLivePictureIndex;
        const next = try prefixHash(reader, reader.packets.len, self.options.decode.max_packet_bytes);
        errdefer self.valid = false;
        self.collector.pending_pts = null;
        self.collector.pending_first_packet = null;
        self.allocations.denied = null;
        const observer = h264.PictureObserver{ .context = &self.collector, .accept = Collector.accept, .pending = Collector.pending };
        const stats = (if (self.registry) |owner| self.advanceDynamic(reader, owner, observer) else self.core.advance(reader, observer)) catch |err| return if (err == error.OutOfMemory) self.allocations.denied orelse err else err;
        self.prefix = next;
        self.position = reader.packets.len;
        self.last_stats = stats;
        return stats;
    }
    fn advanceDynamic(self: *Stream, reader: *media.mp4.Reader, owner: *dynamic.Session, observer: h264.PictureObserver) !h264.Statistics {
        if (owner.packet_configs.len < reader.packets.len) try owner.append(reader) else owner.reader = reader;
        var result = h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
        for (owner.epochs.items) |epoch| {
            if (epoch.end <= self.position) continue;
            const entry = owner.entries.items[epoch.entry];
            var view = reader.*;
            view.packets = reader.packets[epoch.start..epoch.end];
            view.track.avcc = entry.avcc;
            view.track.width = @intCast(entry.config.width);
            view.track.height = @intCast(entry.config.height);
            if (epoch.start != self.epoch_base or !dynamic.layoutCompatible(self.core.cfg.?, entry.config) or self.core.cfg.?.width != entry.config.width or self.core.cfg.?.height != entry.config.height) {
                if (self.core.hasPending()) return error.UnfinishedVideoPictureAtReset;
                var initial = view;
                initial.track.inband_parameter_sets = false;
                const replacement = try h264.Incremental.init(self.allocator, &initial, self.options.decode);
                self.core.deinit();
                self.core = replacement;
                self.epoch_base = epoch.start;
            }
            if (owner.budget.live >= self.options.decode.max_decode_bytes) return error.ResourceLimitExceeded;
            self.core.options.max_decode_bytes = self.options.decode.max_decode_bytes - owner.budget.live;
            self.core.budget.limit = self.core.options.max_decode_bytes;
            const stats = try self.core.advanceWithConfigurations(&view, observer, .{ .session = owner, .base = epoch.start, .independent_start = epoch.independent, .max_refs = epoch.max_refs });
            result.decoded_packets += stats.decoded_packets;
            result.payload_bytes += stats.payload_bytes;
            result.decode_high_water = @max(result.decode_high_water, stats.decode_high_water + owner.budget.live);
        }
        return result;
    }
    /// Own parameter NALs needed by a future independently decoded suffix.
    pub fn parameterBootstrap(self: *Stream, a: std.mem.Allocator, before: usize) ![]u8 {
        if (!self.valid or before > self.position) return error.InvalidLivePictureIndex;
        return if (self.registry) |owner| owner.bootstrapBefore(a, before) else a.alloc(u8, 0);
    }
    pub fn snapshot(self: *Stream, reader: *media.mp4.Reader) !Index {
        if (!self.valid or reader.packets.len != self.position) return error.InvalidLivePictureIndex;
        var count: usize = 0;
        var pending_pts = self.collector.pending_pts;
        var pending_first = self.collector.pending_first_packet;
        for (self.collector.pictures.items, self.collector.planes.items) |picture, mask| {
            if (mask == 0 or mask == 7) count += 1 else {
                pending_pts = if (pending_pts) |old| @min(old, picture.pts) else picture.pts;
                pending_first = if (pending_first) |old| @min(old, picture.first_packet) else picture.first_packet;
            }
        }
        const bytes = try std.math.mul(usize, count, @sizeOf(h264.Picture));
        var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = bytes }) else media.admission.Token{};
        errdefer reservation.deinit();
        const owned = try self.allocator.alloc(h264.Picture, count);
        errdefer self.allocator.free(owned);
        var at: usize = 0;
        for (self.collector.pictures.items, self.collector.planes.items) |picture, mask| if (mask == 0 or mask == 7) {
            try reader.input.control.check();
            owned[at] = picture;
            at += 1;
        };
        return .{ .allocator = self.allocator, .pictures = owned, .timescale = reader.track.timescale, .packet_count = reader.packets.len, .pending_pts = pending_pts, .pending_first_packet = pending_first, .decoded_packets = self.last_stats.decoded_packets, .payload_bytes = self.last_stats.payload_bytes, .reservation = reservation };
    }
    /// All probes precede mutation; caller commits source eviction only on success.
    pub fn rebase(self: *Stream, reader: *media.mp4.Reader, removed: usize) !void {
        var suffix = reader.*;
        if (removed > reader.packets.len) return error.InvalidLivePictureIndex;
        suffix.packets = reader.packets[removed..];
        return self.rebaseWithPrefix(reader, removed, &suffix);
    }
    /// Bind the canonical retained reader, including an independently owned
    /// parameter bootstrap, while rebasing the already validated native state.
    pub fn rebaseWithPrefix(self: *Stream, reader: *media.mp4.Reader, removed: usize, suffix: *media.mp4.Reader) !void {
        if (!self.valid or reader.packets.len != self.position or removed > self.position) return error.InvalidLivePictureIndex;
        if (self.collector.pending_first_packet) |pending| if (pending < removed) return error.InvalidLivePictureIndex;
        for (self.collector.pictures.items, self.collector.planes.items) |picture, mask| {
            if (mask != 0 and mask != 7 and picture.first_packet < removed) return error.InvalidLivePictureIndex;
        }
        if (suffix.packets.len != self.position - removed) return error.InvalidLivePictureIndex;
        const binding = try prefixHash(suffix, suffix.packets.len, self.options.decode.max_packet_bytes);
        const core_removed = removed -| self.epoch_base;
        if (!self.core.canRebase(core_removed)) return error.InvalidLivePictureIndex;
        if (self.registry) |owner| try owner.rebase(removed);
        var at: usize = 0;
        for (self.collector.pictures.items, self.collector.planes.items) |picture, mask| {
            if (picture.first_packet < removed) continue;
            var rebased = picture;
            rebased.first_packet -= removed;
            if (rebased.last_packet) |last| rebased.last_packet = last - removed;
            self.collector.pictures.items[at] = rebased;
            self.collector.planes.items[at] = mask;
            at += 1;
        }
        self.collector.pictures.items.len = at;
        self.collector.planes.items.len = at;
        if (self.collector.pending_first_packet) |pending| self.collector.pending_first_packet = pending - removed;
        self.core.rebase(core_removed);
        self.epoch_base -|= removed;
        self.position -= removed;
        self.prefix = binding;
    }
};
fn hashValue(hash: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .int => |info| {
            var bytes: [8]u8 = undefined;
            const word: u64 = if (info.signedness == .signed) @bitCast(@as(i64, @intCast(value))) else @intCast(value);
            std.mem.writeInt(u64, &bytes, word, .little);
            hash.update(&bytes);
        },
        .bool => hashValue(hash, @as(u8, @intFromBool(value))),
        .@"enum" => hashValue(hash, @as(u64, @backingInt(value))),
        .@"struct" => inline for (@typeInfo(T).@"struct".field_names) |name| hashValue(hash, @field(value, name)),
        .array => for (value) |item| hashValue(hash, item),
        .pointer => |pointer| switch (pointer.size) {
            .slice => {
                hashValue(hash, value.len);
                if (pointer.child == u8) hash.update(value) else for (value) |item| hashValue(hash, item);
            },
            else => @compileError("unsupported hash pointer"),
        },
        .optional => {
            hashValue(hash, value != null);
            if (value) |item| hashValue(hash, item);
        },
        else => @compileError("unsupported hash value"),
    }
}
fn prefixHash(reader: *media.mp4.Reader, count: usize, max_packet: usize) ![32]u8 {
    if (count > reader.packets.len) return error.InvalidPacketIndex;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-incremental-avc-prefix-v2");
    hashValue(&hash, reader.track);
    hashValue(&hash, count);
    for (reader.packets[0..count], 0..) |packet, i| {
        try reader.input.control.check();
        if (packet.size > max_packet) return error.ResourceLimitExceeded;
        var lease = try reader.readPacket(i);
        defer lease.deinit();
        var descriptor = packet;
        descriptor.offset = 0;
        hashValue(&hash, descriptor);
        hashValue(&hash, lease.bytes);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

fn codedEnds(reader: *media.mp4.Reader, begin: usize, end: usize, max_packet: usize) !?[2]usize {
    var first: ?usize = null;
    var last: usize = 0;
    for (begin..end) |i| {
        try reader.input.control.check();
        if (reader.packets[i].size > max_packet) return error.ResourceLimitExceeded;
        var packet = try reader.readPacket(i);
        defer packet.deinit();
        try @import("avc.zig").validatePortablePacket(packet.bytes, reader.track.nal_length_bytes);
        var at: usize = 0;
        while (at < packet.bytes.len) {
            var size: usize = 0;
            for (packet.bytes[at..][0..reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
            at += reader.track.nal_length_bytes;
            const kind = packet.bytes[at] & 31;
            if (kind == 1 or kind == 2 or kind == 5) {
                if (first == null) first = i;
                last = i;
            }
            at += size;
        }
    }
    return if (first) |packet_index| .{ packet_index, last } else null;
}
fn discard(_: *anyopaque, _: usize, _: *const h264.Frame) !void {}
/// Transport-only immutable planning index. Pixel/entropy validity is checked
/// when selected dependency spans are decoded. Live ingestion keeps the strict
/// reconstruction index because publication requires completed pictures.
pub fn planning(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options, registry: ?*dynamic.Session) !Index {
    const owned = if (registry == null and reader.track.inband_parameter_sets) try dynamic.Session.init(a, reader, options.decode) else null;
    defer if (owned) |owner| owner.deinit();
    var result = try @import("h264_headers.zig").planning(a, reader, options, registry orelse owned);
    errdefer result.deinit();
    try result.reservation.resize(.{ .host_bytes = result.pictures.len * @sizeOf(h264.Picture) });
    return result;
}
pub fn index(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !Index {
    return indexMode(a, reader, options, false);
}
/// Completed logical pictures plus a conservative pending-picture barrier.
/// Corruption, resource failures and abandoned pictures at reset remain errors.
pub fn streaming(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !Index {
    return indexMode(a, reader, options, true);
}
fn indexMode(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options, partial: bool) !Index {
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (reader.packets.len == 0) return error.EmptyVideo;
    if (options.max_pictures == 0) return error.ResourceLimitExceeded;
    if (reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    // Account both growth buffers and transient old/new realloc owners. The
    // fixed initial growth allowance also covers very small configured limits.
    const slots = try std.math.mul(usize, try std.math.add(usize, options.max_pictures, 8), 3);
    const metadata_bound = try std.math.mul(usize, slots, @sizeOf(h264.Picture) + @sizeOf(u8));
    var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = metadata_bound }) else media.admission.Token{};
    errdefer reservation.deinit();
    var collector = Collector{ .allocator = a, .limit = options.max_pictures };
    defer collector.pictures.deinit(a);
    defer collector.planes.deinit(a);
    var decode = options.decode;
    decode.picture_observer = .{ .context = &collector, .accept = Collector.accept, .pending = if (partial) Collector.pending else null };
    var stats = h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
    if (reader.track.inband_parameter_sets) {
        const session = try dynamic.Session.init(a, reader, decode);
        defer session.deinit();
        var selectors: std.ArrayList(usize) = .empty;
        defer selectors.deinit(a);
        for (session.epochs.items) |epoch| if (try codedEnds(reader, epoch.start, epoch.end, decode.max_packet_bytes)) |ends| {
            try selectors.appendSlice(a, &ends);
        };
        if (selectors.items.len == 0) return error.EmptyVideo;
        stats = session.decodeSelected(selectors.items, &collector, discard) catch |err| caught: {
            if (!partial or err != error.PendingVideoPicture or collector.pending_first_packet == null) return err;
            break :caught h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
        };
    } else {
        const ends = try codedEnds(reader, 0, reader.packets.len, decode.max_packet_bytes) orelse return error.EmptyVideo;
        stats = h264.decodeSelected(a, reader, &ends, decode, &collector, discard) catch |err| caught: {
            if (!partial or err != error.PendingVideoPicture or collector.pending_first_packet == null) return err;
            break :caught h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
        };
    }
    var kept: usize = 0;
    for (collector.pictures.items, collector.planes.items) |picture, mask| {
        if (mask != 0 and mask != 7) {
            if (!partial) return error.IncompleteVideoPicture;
            Collector.pending(&collector, .{ .first_packet = picture.first_packet, .pts = picture.pts });
        } else {
            collector.pictures.items[kept] = picture;
            kept += 1;
        }
    }
    collector.pictures.items.len = kept;
    const owned = try collector.pictures.toOwnedSlice(a);
    errdefer a.free(owned);
    collector.planes.deinit(a);
    collector.planes = .empty;
    try reservation.resize(.{ .host_bytes = owned.len * @sizeOf(h264.Picture) });
    return .{ .allocator = a, .pictures = owned, .timescale = reader.track.timescale, .packet_count = reader.packets.len, .pending_pts = collector.pending_pts, .pending_first_packet = collector.pending_first_packet, .decoded_packets = stats.decoded_packets, .payload_bytes = stats.payload_bytes, .reservation = reservation };
}

test "AVC logical picture index counts field pairs partitions and configuration epochs" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2", "h264-jm-partitions", "h264-dynamic-geometry", "h264-jm-separate" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var pictures = try index(a, &reader, .{});
        defer pictures.deinit();
        try std.testing.expect(pictures.pictures.len > 0);
        var planned = try planning(a, &reader, .{}, null);
        defer planned.deinit();
        try std.testing.expectEqual(@as(usize, 0), planned.decoded_packets);
        try std.testing.expectEqualSlices(h264.Picture, pictures.pictures, planned.pictures);
        var live = try streaming(a, &reader, .{});
        defer live.deinit();
        try std.testing.expectEqual(@as(?i64, null), live.pending_pts);
        try std.testing.expectEqualSlices(h264.Picture, pictures.pictures, live.pictures);
        if (std.mem.eql(u8, name, "h264-jm-partitions") or std.mem.eql(u8, name, "h264-jm-separate")) try std.testing.expectEqual(@as(usize, 4), pictures.pictures.len);
        if (std.mem.eql(u8, name, "h264-dynamic-geometry")) {
            try std.testing.expectEqual(@as(usize, 3), pictures.pictures.len);
            try std.testing.expectEqual(@as(u32, 64), pictures.pictures[0].width);
            try std.testing.expectEqual(@as(u32, 96), pictures.pictures[1].width);
            try std.testing.expectEqual(@as(u32, 80), pictures.pictures[2].width);
        }
        if (std.mem.startsWith(u8, name, "h264-paff")) try std.testing.expect(pictures.pictures.len < reader.packets.len);
        const selected = try pictures.select();
        defer a.free(selected);
        for (selected) |packet| try std.testing.expect(packet < reader.packets.len);
    }
}

test "AVC logical index bounds callback metadata and releases shared decode admission" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .identity = "logical-admission", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const before = pool.snapshot().host_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_pictures = 0 }));
    try std.testing.expectEqual(before, pool.snapshot().host_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_packets = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, streaming(a, &reader, .{ .max_pictures = 0 }));
    try std.testing.expectEqual(before, pool.snapshot().host_bytes);
    var pictures = try index(a, &reader, .{});
    defer pictures.deinit();
    try std.testing.expectEqual(before + pictures.pictures.len * @sizeOf(h264.Picture), pool.snapshot().host_bytes);
}

test "AVC streaming index holds incomplete field pairs until their mate arrives" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        const packets = reader.packets;
        defer reader.packets = packets;
        var saw_pending = false;
        var saw_completed = false;
        for (1..packets.len + 1) |end| {
            reader.packets = packets[0..end];
            var logical = try streaming(a, &reader, .{});
            defer logical.deinit();
            if (logical.pending_pts != null) {
                saw_pending = true;
                try std.testing.expect(logical.pending_first_packet.? < end);
                if (end == 1) try std.testing.expectError(error.IncompleteVideoPicture, index(a, &reader, .{}));
            }
            saw_completed = saw_completed or logical.pictures.len != 0;
            if (end == packets.len) try std.testing.expectEqual(@as(?i64, null), logical.pending_pts);
        }
        try std.testing.expect(saw_pending and saw_completed);
    }
}

test "AVC streaming index rejects corrupt NALs rather than manufacturing a pending tail" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4"));
    defer a.free(bytes);
    var source = media.source.Source{ .allocator = a, .identity = "corrupt-tail", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const header = @as(usize, @intCast(reader.packets[0].offset)) + reader.track.nal_length_bytes;
    bytes[header] |= 128;
    try std.testing.expectError(error.MalformedVideoPacket, streaming(a, &reader, .{}));
}

test "AVC logical snapshots rebase retained suffix and keep independent admission" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .identity = "snapshot", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-dynamic-geometry.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const before = pool.snapshot().host_bytes;
    var original = try streaming(a, &reader, .{});
    var original_live = true;
    defer if (original_live) original.deinit();
    const base = original.pictures[1].first_packet;
    var copy = try original.cloneRebased(a, base, reader.packets.len - base, &pool, .{});
    defer copy.deinit();
    try std.testing.expectEqual(@as(usize, 2), copy.pictures.len);
    try std.testing.expectEqual(@as(usize, 0), copy.pictures[0].first_packet);
    try std.testing.expectEqual(@as(u32, 96), copy.pictures[0].width);
    try std.testing.expectError(error.InvalidLivePictureIndex, original.cloneRebased(a, 0, reader.packets.len - 1, &pool, .{}));
    original.deinit();
    original_live = false;
    try std.testing.expectEqual(before + copy.pictures.len * @sizeOf(h264.Picture), pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(u32, 80), copy.pictures[1].width);
}

test "AVC incremental logical metadata matches replay across static tools and unfinished fields" {
    const a = std.testing.allocator;
    inline for (.{ "h264-baseline-p", "h264-high10-cabac", "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2", "h264-paff-split-packets-14-3", "h264-jm-separate", "h264-jm-partitions" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        if (!reader.track.inband_parameter_sets) {
            const packets = reader.packets;
            defer reader.packets = packets;
            var stream: ?*Stream = null;
            defer if (stream) |owner| owner.deinit();
            var decoded: usize = 0;
            for (1..packets.len + 1) |end| {
                reader.packets = packets[0..end];
                var oracle = streaming(a, &reader, .{}) catch |expected| {
                    if (stream == null) stream = try Stream.init(a, &reader, .{});
                    try std.testing.expectError(expected, stream.?.advance(&reader));
                    stream.?.deinit();
                    stream = null;
                    continue;
                };
                defer oracle.deinit();
                if (stream == null) stream = try Stream.init(a, &reader, .{});
                const stats = try stream.?.advance(&reader);
                decoded += stats.decoded_packets;
                var snapshot = try stream.?.snapshot(&reader);
                defer snapshot.deinit();
                try std.testing.expectEqualSlices(h264.Picture, oracle.pictures, snapshot.pictures);
                try std.testing.expectEqual(oracle.pending_pts, snapshot.pending_pts);
                try std.testing.expectEqual(oracle.pending_first_packet, snapshot.pending_first_packet);
            }
            try std.testing.expect(decoded >= packets.len);
            if (std.mem.eql(u8, name, "h264-baseline-p") or std.mem.eql(u8, name, "h264-paff-adaptive-packets-8-1")) try std.testing.expectEqual(packets.len, decoded);
        }
    }
}
fn streamAllocationRun(a: std.mem.Allocator) !void {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "incremental-failure", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    reader.packets = packets[0..1];
    const owner = try Stream.init(a, &reader, .{});
    defer owner.deinit();
    _ = try owner.advance(&reader);
    var held = try owner.snapshot(&reader);
    defer held.deinit();
    reader.packets = packets;
    _ = try owner.advance(&reader);
    var complete = try owner.snapshot(&reader);
    defer complete.deinit();
}
test "AVC incremental owner releases persistent allocation and admission failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, streamAllocationRun, .{});
}

test "AVC incremental prefix mutation and failed eviction preserve retry and held metadata" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4"));
    defer a.free(bytes);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "immutable-prefix", .admission_pool = &pool, .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    reader.packets = packets[0..1];
    const owner = try Stream.init(a, &reader, .{});
    defer owner.deinit();
    _ = try owner.advance(&reader);
    var held = try owner.snapshot(&reader);
    defer held.deinit();
    try std.testing.expect(held.pending_first_packet != null);
    const charged = pool.snapshot().host_bytes;
    try std.testing.expectError(error.InvalidLivePictureIndex, owner.rebase(&reader, 1));
    try std.testing.expectEqual(charged, pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.core.cursor());
    reader.packets = packets;
    const at: usize = @intCast(packets[0].offset + packets[0].size - 1);
    bytes[at] ^= 1;
    try std.testing.expectError(error.InvalidLivePictureIndex, owner.advance(&reader));
    try std.testing.expectEqual(@as(usize, 1), owner.core.cursor());
    try std.testing.expect(owner.valid);
    bytes[at] ^= 1;
    _ = try owner.advance(&reader);
    var oracle = try streaming(a, &reader, .{});
    defer oracle.deinit();
    var snapshot = try owner.snapshot(&reader);
    defer snapshot.deinit();
    try std.testing.expectEqualSlices(h264.Picture, oracle.pictures, snapshot.pictures);
    // The earlier output owns its metadata even while the owner advances.
    try std.testing.expect(held.pending_first_packet != null and held.pictures.len == 0);
}

test "AVC dynamic incremental registry and reconstruction match every prefix across epochs" {
    const a = std.testing.allocator;
    inline for (.{ "h264-dynamic-geometry", "h264-dynamic-pps" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        try std.testing.expect(reader.track.inband_parameter_sets);
        const packets = reader.packets;
        defer reader.packets = packets;
        reader.packets = packets[0..1];
        const owner = try Stream.init(a, &reader, .{});
        defer owner.deinit();
        var decoded: usize = 0;
        for (1..packets.len + 1) |end| {
            reader.packets = packets[0..end];
            const stats = try owner.advance(&reader);
            decoded += stats.decoded_packets;
            var actual = try owner.snapshot(&reader);
            defer actual.deinit();
            var oracle = try streaming(a, &reader, .{});
            defer oracle.deinit();
            try std.testing.expectEqualSlices(h264.Picture, oracle.pictures, actual.pictures);
            const registry = try dynamic.Session.init(a, &reader, .{});
            defer registry.deinit();
            try std.testing.expectEqualSlices(usize, registry.packet_configs, owner.registry.?.packet_configs);
            try std.testing.expectEqualSlices(dynamic.Epoch, registry.epochs.items, owner.registry.?.epochs.items);
        }
        try std.testing.expectEqual(packets.len, decoded);
        try std.testing.expectEqual(packets.len, owner.registry.?.probe_packets);
    }
}
fn dynamicStreamAllocationRun(a: std.mem.Allocator) !void {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "dynamic-owner-failure", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-dynamic-geometry.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    reader.packets = packets[0..1];
    const owner = try Stream.init(a, &reader, .{});
    defer owner.deinit();
    _ = try owner.advance(&reader);
    reader.packets = packets;
    _ = try owner.advance(&reader);
    const bootstrap = try owner.parameterBootstrap(a, 1);
    defer a.free(bootstrap);
    try owner.rebase(&reader, 1);
    reader.packets = packets[1..];
    var snapshot = try owner.snapshot(&reader);
    defer snapshot.deinit();
}
test "AVC dynamic incremental registry owners bootstrap and epoch replacement unwind allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dynamicStreamAllocationRun, .{});
}
fn bootstrapSuffixRun(fixture_bytes: []const u8, comptime independent_reset: bool) !void {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .identity = "bootstrap-source", .storage = .{ .borrowed = fixture_bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const owner = try Stream.init(a, &reader, .{});
    defer owner.deinit();
    _ = try owner.advance(&reader);
    var all = try owner.snapshot(&reader);
    defer all.deinit();
    const base: usize = 1;
    const bootstrap = try owner.parameterBootstrap(a, base);
    defer a.free(bootstrap);
    try std.testing.expect(bootstrap.len > 0);
    var size = bootstrap.len;
    for (reader.packets[base..]) |packet| size += packet.size;
    const bytes = try a.alloc(u8, size);
    defer a.free(bytes);
    @memcpy(bytes[0..bootstrap.len], bootstrap);
    const descriptors = try a.dupe(media.mp4.Packet, reader.packets[base..]);
    defer a.free(descriptors);
    var cursor = bootstrap.len;
    for (descriptors, base..) |*packet, i| {
        var lease = try reader.readPacket(i);
        defer lease.deinit();
        @memcpy(bytes[cursor..][0..lease.bytes.len], lease.bytes);
        packet.offset = if (i == base) 0 else cursor;
        if (i == base) packet.size += @intCast(bootstrap.len);
        cursor += lease.bytes.len;
    }
    var independent = media.source.Source{ .allocator = a, .identity = "bootstrap-owned", .storage = .{ .borrowed = bytes } };
    var suffix = reader;
    suffix.input = &independent;
    suffix.packets = descriptors;
    if (!independent_reset) {
        try std.testing.expectError(error.MissingVideoReference, streaming(a, &suffix, .{}));
        return;
    }
    var decoded = try streaming(a, &suffix, .{});
    defer decoded.deinit();
    var expected = try all.cloneRebased(a, base, descriptors.len, null, .{});
    defer expected.deinit();
    try std.testing.expectEqualSlices(h264.Picture, expected.pictures, decoded.pictures);
}

test "AVC parameter bootstrap reconstructs reset suffixes and preserves non-IDR reference requirements" {
    try bootstrapSuffixRun(@embedFile("../testdata/h264-dynamic-geometry.mp4"), true);
    try bootstrapSuffixRun(@embedFile("../testdata/h264-dynamic-pps.mp4"), false);
}

test "AVC header planning matches reconstructed fields planes partitions and epochs" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2", "h264-paff-adaptive-packets-14-3", "h264-paff-b-10-2-spatial", "h264-jm-partitions", "h264-jm-separate", "h264-dynamic-geometry", "h264-baseline-p" }) |name| {
        var input = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &input, .{});
        defer reader.deinit();
        var decoded = try index(a, &reader, .{});
        defer decoded.deinit();
        var headers = try planning(a, &reader, .{}, null);
        defer headers.deinit();
        try std.testing.expectEqual(@as(usize, 0), headers.decoded_packets);
        try std.testing.expectEqualSlices(h264.Picture, decoded.pictures, headers.pictures);
    }
}

test "AVC header planning releases admission across allocation and resource failures" {
    const Harness = struct {
        fn run(a: std.mem.Allocator) !void {
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            var input = media.source.Source{ .allocator = a, .identity = "header-failures", .storage = .{ .borrowed = @embedFile("../testdata/h264-paff-interleaved-fragments-10-2.mp4") }, .admission_pool = &pool };
            var reader = try media.mp4.Reader.init(a, &input, .{});
            defer reader.deinit();
            var headers = try planning(a, &reader, .{}, null);
            defer headers.deinit();
            try std.testing.expectError(error.ResourceLimitExceeded, planning(a, &reader, .{ .max_packets = 1 }, null));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}
