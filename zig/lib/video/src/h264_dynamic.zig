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

//! Owned parameter registry and packet configuration epochs for avc3 sources.
//! Reusable sessions avoid repeating the parameter scan for each selection.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const Bits = @import("h264_bits.zig").Bits;
const Budget = @import("decode_budget.zig").Budget;
const PPS = struct { bytes: []u8, sps: usize };
const Parameter = struct { at: ?usize, bytes: []u8, original: bool };
const Versions = struct {
    list: std.ArrayList(Parameter) = .empty,

    fn install(self: *Versions, a: std.mem.Allocator, nal: []const u8, at: ?usize) !?[]u8 {
        // The first in-band announcement becomes part of suffix bootstraps,
        // even when it repeats the original avcC bytes.
        if (self.list.getLastOrNull()) |last| if (last.original == (at == null) and std.mem.eql(u8, last.bytes, nal)) return null;
        const owned = try a.dupe(u8, nal);
        errdefer a.free(owned);
        try self.list.append(a, .{ .at = at, .bytes = owned, .original = at == null });
        return owned;
    }

    fn boundary(self: Versions, packet: usize) usize {
        var lo: usize = 0;
        var hi = self.list.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.list.items[mid].at) |at| if (at >= packet) {
                hi = mid;
                continue;
            };
            lo = mid + 1;
        }
        return lo;
    }

    fn before(self: Versions, packet: usize) ?Parameter {
        const end = self.boundary(packet);
        return if (end == 0) null else self.list.items[end - 1];
    }

    fn rebase(self: *Versions, a: std.mem.Allocator, removed: usize) void {
        const end = self.boundary(removed);
        const drop = end -| 1;
        for (self.list.items[0..drop]) |p| a.free(p.bytes);
        if (end > 0) self.list.items[end - 1].at = null;
        for (self.list.items[end..]) |*p| p.at = p.at.? - removed;
        self.list.replaceRangeAssumeCapacity(0, drop, &.{});
    }
    fn deinit(self: *Versions, a: std.mem.Allocator) void {
        for (self.list.items) |p| a.free(p.bytes);
        self.list.deinit(a);
    }
};
pub const Entry = struct { avcc: []u8, config: h264.Config, sps_hash: [32]u8 };
pub const Epoch = struct { start: usize, end: usize, entry: usize, independent: bool = false, max_refs: usize };
pub const Context = struct {
    session: *Session,
    base: usize,
    independent_start: bool = false,
    max_refs: usize,

    pub fn progressiveRestart(self: Context, relative_target: usize) ?usize {
        const packet = self.session.progressiveRestart(self.base, self.base + relative_target) orelse return null;
        return packet - self.base;
    }
};
/// Forward selections retain DPB/assembly state within a compatible epoch.
/// Crossing an SPS geometry/representation epoch constructs a fresh owner;
/// parameter changes inside an epoch use the same validated registry as replay.
/// The borrowed registry and immutable reader must outlive this owner.
pub const Continuation = struct {
    session: *Session,
    core: ?*h264.Incremental = null,
    epoch_start: ?usize = null,
    pub fn deinit(self: *@This()) void {
        if (self.core) |core| core.deinit();
        self.core = null;
        self.epoch_start = null;
    }
    pub fn cursor(self: *const @This()) usize {
        return if (self.core) |core| self.epoch_start.? + core.cursor() else 0;
    }
    pub fn decodeSelected(self: *@This(), indexes: []const usize, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
        return self.decodeSelectedReader(self.session.reader, indexes, context, callback);
    }
    pub fn decodeSelectedReader(self: *@This(), reader: *media.mp4.Reader, indexes: []const usize, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
        const registry = self.session;
        if (reader.packets.ptr != registry.reader.packets.ptr or reader.packets.len != registry.reader.packets.len) return error.InvalidPacketIndex;
        if (indexes.len == 0 or indexes.len > registry.options.max_selection_slots) return error.ResourceLimitExceeded;
        for (indexes) |index| if (index >= reader.packets.len or index < self.cursor()) return error.InvalidPacketIndex;
        errdefer self.deinit();
        const selected = try registry.backing.alloc(usize, indexes.len);
        defer registry.backing.free(selected);
        const slots = try registry.backing.alloc(usize, indexes.len);
        defer registry.backing.free(slots);
        var stats = h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = registry.budget.live };
        orderSlots(slots, indexes);
        var end: usize = 0;
        while (end < slots.len) {
            const begin = end;
            const epoch = try registry.epochAt(indexes[slots[begin]]);
            while (end < slots.len and indexes[slots[end]] < epoch.end) : (end += 1)
                selected[end] = indexes[slots[end]] - epoch.start;
            if (stats.decoded_packets >= registry.options.max_dependency_packets) return error.ResourceLimitExceeded;
            const entry = registry.entries.items[epoch.entry];
            var view = reader.*;
            view.packets = reader.packets[epoch.start..epoch.end];
            view.track.avcc = entry.avcc;
            view.track.width = @intCast(entry.config.width);
            view.track.height = @intCast(entry.config.height);
            if (self.epoch_start == null or self.epoch_start.? != epoch.start) {
                if (self.core) |core| if (core.hasPending()) return error.UnfinishedVideoPictureAtReset;
                self.deinit();
                var initial = view;
                initial.track.inband_parameter_sets = false;
                initial.track.multiple_avc_parameter_sets = false;
                var options = registry.options;
                if (registry.budget.live >= options.max_decode_bytes) return error.ResourceLimitExceeded;
                options.max_decode_bytes -= registry.budget.live;
                self.core = try h264.Incremental.init(registry.backing, &initial, options);
                self.epoch_start = epoch.start;
            }
            const Capture = struct {
                slots: []const usize,
                context: *anyopaque,
                callback: h264.SelectionCallback,
                fn publish(raw: *anyopaque, slot: usize, frame: *const h264.Frame) !void {
                    const capture: *@This() = @ptrCast(@alignCast(raw));
                    try capture.callback(capture.context, capture.slots[slot], frame);
                }
            };
            var capture = Capture{ .slots = slots[begin..end], .context = context, .callback = callback };
            const core = self.core.?;
            const limit = core.options.max_dependency_packets;
            defer core.options.max_dependency_packets = limit;
            core.options.max_dependency_packets = limit -| stats.decoded_packets;
            const result = try core.decodeSelectedWithConfigurations(&view, selected[begin..end], &capture, Capture.publish, .{ .session = registry, .base = epoch.start, .independent_start = epoch.independent, .max_refs = epoch.max_refs });
            stats.decoded_packets += result.decoded_packets;
            stats.payload_bytes += result.payload_bytes;
            stats.decode_high_water = @max(stats.decode_high_water, result.decode_high_water + registry.budget.live);
        }
        return stats;
    }
};
/// Arena-owned decode and suffix-bootstrap metadata. Mutable parser state and
/// provider pointers never enter the shared cache.
pub const Snapshot = struct {
    parameter_bytes: usize,
    entries: []Entry,
    epochs: []Epoch,
    packet_configs: []usize,
    progressive_idrs: []usize,
    sequence_versions: [32]Versions,
    picture_versions: [256]Versions,

    pub fn storageBytes(session: *const Session) usize {
        var bytes = std.mem.sliceAsBytes(session.entries.items).len +| std.mem.sliceAsBytes(session.epochs.items).len +| std.mem.sliceAsBytes(session.packet_configs).len +| std.mem.sliceAsBytes(session.progressive_idrs.items).len;
        for (session.entries.items) |entry| {
            bytes +|= entry.avcc.len;
            if (entry.config.groups.explicit) |map| bytes +|= map.len;
        }
        inline for (.{ session.sequence_versions[0..], session.picture_versions[0..] }) |table| for (table) |versions| {
            bytes +|= std.mem.sliceAsBytes(versions.list.items).len;
            for (versions.list.items) |version| bytes +|= version.bytes.len;
        };
        return bytes;
    }
    fn copyVersions(a: std.mem.Allocator, source: anytype) !@TypeOf(source) {
        var result: @TypeOf(source) = @splat(.{});
        for (&result, source) |*dest, versions| {
            dest.list = .fromOwnedSlice(try a.dupe(Parameter, versions.list.items));
            for (dest.list.items) |*item| item.bytes = try a.dupe(u8, item.bytes);
        }
        return result;
    }

    pub fn copy(a: std.mem.Allocator, session: *const Session) !Snapshot {
        const entries = try a.dupe(Entry, session.entries.items);
        for (entries) |*value| {
            value.avcc = try a.dupe(u8, value.avcc);
            if (value.config.groups.explicit) |map| value.config.groups.explicit = try a.dupe(u8, map);
        }
        return .{
            .parameter_bytes = session.budget.live,
            .entries = entries,
            .epochs = try a.dupe(Epoch, session.epochs.items),
            .packet_configs = try a.dupe(usize, session.packet_configs),
            .progressive_idrs = try a.dupe(usize, session.progressive_idrs.items),
            .sequence_versions = try copyVersions(a, session.sequence_versions),
            .picture_versions = try copyVersions(a, session.picture_versions),
        };
    }

    pub fn bind(self: *const Snapshot, a: std.mem.Allocator, reader: *media.mp4.Reader, options: h264.Options) !*Session {
        var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(Session) }) else media.admission.Token{};
        errdefer reservation.deinit();
        const session = try a.create(Session);
        session.* = .{
            .borrowed = true,
            .reservation = reservation,
            .backing = a,
            .reader = reader,
            .length_bytes = reader.track.nal_length_bytes,
            .options = options,
            .budget = .{ .backing = a, .limit = options.max_parameter_bytes, .live = self.parameter_bytes, .peak = self.parameter_bytes },
            .header_budget = .{ .backing = a, .limit = options.max_parameter_bytes },
            .entries = .fromOwnedSlice(self.entries),
            .epochs = .fromOwnedSlice(self.epochs),
            .packet_configs = self.packet_configs,
            .progressive_idrs = .fromOwnedSlice(self.progressive_idrs),
            .sequence_versions = self.sequence_versions,
            .picture_versions = self.picture_versions,
        };
        return session;
    }
};

fn orderSlots(slots: []usize, indexes: []const usize) void {
    for (slots, 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, slots, indexes, struct {
        fn less(ids: []const usize, left: usize, right: usize) bool {
            return ids[left] < ids[right];
        }
    }.less);
}

pub const Session = struct {
    borrowed: bool = false,
    backing: std.mem.Allocator,
    budget: Budget,
    reader: *media.mp4.Reader,
    length_bytes: u3,
    options: h264.Options,
    reservation: media.admission.Token = .{},
    sequence: [32]?[]u8 = @splat(null),
    picture: [256]?PPS = @splat(null),
    resolved_pps: [256]?usize = @splat(null),
    entries: std.ArrayList(Entry) = .empty,
    epochs: std.ArrayList(Epoch) = .empty,
    progressive_idrs: std.ArrayList(usize) = .empty,
    packet_configs: []usize = &.{},
    memory: ?media.admission.TrackedAllocator = null,
    header_budget: Budget,
    supplied_headers: ?[]const []u8 = null,
    header_packets: std.ArrayList([]u8) = .empty,
    cache_headers: bool = false,
    probe_packets: usize = 0,
    probe_bytes: u64 = 0,
    active: ?usize = null,
    valid: bool = true,
    sequence_versions: [32]Versions = @splat(.{}),
    picture_versions: [256]Versions = @splat(.{}),
    pub fn init(allocator: std.mem.Allocator, reader: *media.mp4.Reader, options: h264.Options) !*Session {
        return initCached(allocator, reader, options, false, null);
    }
    pub fn initPlanning(allocator: std.mem.Allocator, reader: *media.mp4.Reader, options: h264.Options) !*Session {
        return initCached(allocator, reader, options, true, null);
    }
    pub fn initWithHeaders(allocator: std.mem.Allocator, reader: *media.mp4.Reader, options: h264.Options, headers: []const []u8) !*Session {
        if (headers.len != reader.packets.len) return error.InvalidVideoIndex;
        return initCached(allocator, reader, options, false, headers);
    }
    pub fn planningHeaders(self: *const Session) ?[]const []u8 {
        return self.supplied_headers orelse if (self.header_packets.items.len == self.reader.packets.len) self.header_packets.items else null;
    }
    fn initCached(allocator: std.mem.Allocator, reader: *media.mp4.Reader, options: h264.Options, cache_headers: bool, headers: ?[]const []u8) !*Session {
        if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
        if (reader.packets.len > options.max_parameter_packets) return error.ResourceLimitExceeded;
        const self = try allocator.create(Session);
        self.* = .{ .backing = allocator, .budget = .{ .backing = allocator, .limit = options.max_parameter_bytes }, .header_budget = .{ .backing = allocator, .limit = options.max_parameter_bytes }, .reader = reader, .length_bytes = reader.track.nal_length_bytes, .options = options, .cache_headers = cache_headers, .supplied_headers = headers };
        errdefer self.deinit();
        self.reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(Session) }) else media.admission.Token{};
        if (reader.input.admission_pool) |pool| {
            self.memory = .{ .backing = allocator, .limit = std.math.maxInt(usize), .reservation = try pool.acquire(.{}) };
            self.budget.backing = self.memory.?.allocator();
            self.header_budget.backing = self.memory.?.allocator();
        }
        if (cache_headers) self.budget.reclaimer = .{ .context = self, .release = reclaimOwnedHeaders };
        self.scan() catch |err| return self.scanError(err);
        return self;
    }
    fn scanError(self: *const Session, err: anyerror) anyerror {
        if (err != error.OutOfMemory) return err;
        if (self.memory) |memory| if (memory.denied) |reason| return reason;
        return if (self.budget.denied or self.header_budget.denied) error.ResourceLimitExceeded else err;
    }
    pub fn deinit(self: *Session) void {
        var reservation = self.reservation;
        defer reservation.deinit();
        defer self.backing.destroy(self);
        if (self.borrowed) return;
        defer if (self.memory) |*memory| memory.reservation.deinit();
        const allocator = self.budget.allocator();
        for (self.entries.items) |value| {
            value.config.groups.deinit(allocator);
            allocator.free(value.avcc);
        }
        self.entries.deinit(allocator);
        self.epochs.deinit(allocator);
        self.progressive_idrs.deinit(allocator);
        for (&self.sequence_versions) |*versions| versions.deinit(allocator);
        for (&self.picture_versions) |*versions| versions.deinit(allocator);
        allocator.free(self.packet_configs);
        self.releasePlanningHeaders();
    }
    fn reclaimOwnedHeaders(raw: *anyopaque) bool {
        const self: *Session = @ptrCast(@alignCast(raw));
        if (self.header_packets.capacity == 0) return false;
        self.releasePlanningHeaders();
        return true;
    }
    pub fn releasePlanningHeaders(self: *Session) void {
        const a = self.header_budget.allocator();
        for (self.header_packets.items) |bytes| a.free(bytes);
        self.header_packets.deinit(a);
        self.header_packets = .empty;
        self.cache_headers = false;
        self.supplied_headers = null;
    }
    fn install(self: *Session, nal: []const u8, at: ?usize) !void {
        if (nal.len > std.math.maxInt(u16)) return error.ResourceLimitExceeded;
        const allocator = self.budget.allocator();
        var bits = try Bits.initControlled(allocator, nal, self.reader.input.control);
        defer bits.deinit();
        if (nal[0] & 31 == 7) {
            _ = try bits.read(24);
            const id = try bits.ue();
            if (id >= self.sequence.len) return error.MalformedVideoConfig;
            const owned = try self.sequence_versions[id].install(allocator, nal, at) orelse return;
            for (self.picture, &self.resolved_pps) |pps, *resolved| {
                if (pps != null and pps.?.sps == id) resolved.* = null;
            }
            self.sequence[id] = owned;
        } else if (nal[0] & 31 == 8) {
            const id = try bits.ue();
            const sps = try bits.ue();
            if (id >= self.picture.len or sps >= self.sequence.len) return error.MalformedVideoConfig;
            const owned = try self.picture_versions[id].install(allocator, nal, at) orelse return;
            self.resolved_pps[id] = null;
            self.picture[id] = .{ .bytes = owned, .sps = sps };
        } else return error.MalformedVideoConfig;
    }
    fn initial(self: *Session) !void {
        const bytes = self.reader.track.avcc;
        try @import("avc.zig").validatePortableConfig(bytes);
        var cursor: usize = 6;
        for (0..bytes[5] & 31) |_| {
            try self.install(try nalSet(bytes, &cursor), null);
        }
        const count = bytes[cursor];
        cursor += 1;
        for (0..count) |_| try self.install(try nalSet(bytes, &cursor), null);
    }
    fn entry(self: *Session, id: usize) !usize {
        if (id >= self.picture.len) return error.MalformedVideoConfig;
        if (self.resolved_pps[id]) |resolved| return resolved;
        const pps = self.picture[id] orelse return error.MissingVideoParameterSet;
        const sps = self.sequence[pps.sps] orelse return error.MissingVideoParameterSet;
        const allocator = self.budget.allocator();
        const size = 11 + sps.len + pps.bytes.len;
        const bytes = try allocator.alloc(u8, size);
        errdefer allocator.free(bytes);
        @memcpy(bytes[0..6], &[_]u8{ 1, sps[1], sps[2], sps[3], @as(u8, 0xfc) | @as(u8, self.reader.track.nal_length_bytes - 1), 0xe1 });
        std.mem.writeInt(u16, bytes[6..8], @intCast(sps.len), .big);
        @memcpy(bytes[8..][0..sps.len], sps);
        bytes[8 + sps.len] = 1;
        std.mem.writeInt(u16, bytes[9 + sps.len ..][0..2], @intCast(pps.bytes.len), .big);
        @memcpy(bytes[11 + sps.len ..], pps.bytes);
        for (self.entries.items, 0..) |candidate, i| if (std.mem.eql(u8, candidate.avcc, bytes)) {
            allocator.free(bytes);
            self.resolved_pps[id] = i;
            return i;
        };
        if (self.entries.items.len >= self.options.max_configurations) return error.ResourceLimitExceeded;
        const config = try h264.configParse(allocator, bytes);
        errdefer config.groups.deinit(allocator);
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(sps, &hash, .{});
        // Precise growth keeps the registry allocator/admission limit honest.
        try self.entries.ensureTotalCapacityPrecise(allocator, self.entries.items.len + 1);
        self.entries.appendAssumeCapacity(.{ .avcc = bytes, .config = config, .sps_hash = hash });
        self.resolved_pps[id] = self.entries.items.len - 1;
        return self.resolved_pps[id].?;
    }
    fn scan(self: *Session) !void {
        try self.initial();
        try self.scanFrom(0);
    }
    fn scanFrom(self: *Session, begin: usize) !void {
        const allocator = self.budget.allocator();
        self.packet_configs = try allocator.realloc(self.packet_configs, self.reader.packets.len);
        @memset(self.packet_configs[begin..], std.math.maxInt(usize));
        if (self.epochs.items.len != 0) self.epochs.items[self.epochs.items.len - 1].end = self.reader.packets.len;
        for (begin..self.reader.packets.len) |i| {
            const packet = self.reader.packets[i];
            if (packet.size > self.options.max_packet_bytes) return error.ResourceLimitExceeded;
            const bytes_before = self.reader.input.total_bytes;
            const header_allocator = self.header_budget.allocator();
            const prefix = if (self.supplied_headers) |headers| headers[i] else prefix: {
                // Eviction only touches previously completed packets. Disable it
                // before growing the list which owns those packets.
                self.header_budget.reclaimer = self.budget.reclaimer;
                defer self.header_budget.reclaimer = null;
                break :prefix try @import("h264_prefix.zig").packetReclaiming(header_allocator, self.reader, i, self.options.max_packet_bytes, self.budget.reclaimer);
            };
            var retained = self.supplied_headers != null;
            defer if (!retained) header_allocator.free(prefix);
            const lease = .{ .bytes = prefix };
            self.probe_packets += 1;
            self.probe_bytes += self.reader.input.total_bytes - bytes_before;
            try @import("avc.zig").validatePortablePacket(lease.bytes, self.reader.track.nal_length_bytes);
            var cursor: usize = 0;
            var selected: ?usize = null;
            var idr_start = false;
            var non_idr_slice = false;
            while (cursor < lease.bytes.len) {
                try self.reader.input.control.check();
                var size: usize = 0;
                for (lease.bytes[cursor..][0..self.reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
                cursor += self.reader.track.nal_length_bytes;
                const nal = lease.bytes[cursor..][0..size];
                cursor += size;
                const kind = nal[0] & 31;
                non_idr_slice = non_idr_slice or (kind >= 1 and kind <= 4);
                switch (kind) {
                    7, 8 => try self.install(nal, i),
                    1, 2, 5 => {
                        var bits = @import("h264_prefix.zig").Bits{ .bytes = nal };
                        const first_mb = try bits.ue();
                        if (kind == 5 and first_mb == 0) idr_start = true;
                        const slice_type = try bits.ue();
                        const index = try self.entry(try bits.ue());
                        if (selected) |previous| if (previous != index) return error.MixedVideoPictures;
                        const window_reset = selected == null and packet.avc_epoch_start;
                        selected = index;
                        const changed = self.active == null or !std.mem.eql(u8, &self.entries.items[self.active.?].sps_hash, &self.entries.items[index].sps_hash);
                        const compatible = self.active != null and layoutCompatible(self.entries.items[self.active.?].config, self.entries.items[index].config);
                        if (window_reset or (changed and (self.active == null or nal[0] & 31 == 5 or !compatible))) {
                            const independent = window_reset or (self.active != null and nal[0] & 31 != 5);
                            // Qualified non-IDR extension: incompatible storage starts
                            // with a complete I picture and an empty prediction DPB.
                            if (independent and slice_type % 5 != 2) return error.UnsupportedDynamicGeometry;
                            if (self.epochs.items.len != 0) self.epochs.items[self.epochs.items.len - 1].end = i;
                            try self.epochs.ensureTotalCapacityPrecise(allocator, self.epochs.items.len + 1);
                            self.epochs.appendAssumeCapacity(.{ .start = if (self.active == null) 0 else i, .end = self.reader.packets.len, .entry = index, .independent = independent, .max_refs = self.entries.items[index].config.max_refs });
                        }
                        const epoch = &self.epochs.items[self.epochs.items.len - 1];
                        epoch.max_refs = @max(epoch.max_refs, self.entries.items[index].config.max_refs);
                        self.active = index;
                    },
                    else => {},
                }
            }
            if (idr_start and !non_idr_slice) {
                const cfg = self.entries.items[selected.?].config;
                // Only complete progressive access units can split a run here.
                // Fields, partitions and separate planes share assembly state.
                if (cfg.frame_only and !cfg.separate_planes and cfg.profile != 88)
                    try self.progressive_idrs.append(allocator, i);
            }
            self.packet_configs[i] = selected orelse self.active orelse std.math.maxInt(usize);
            if (self.cache_headers) {
                if (self.header_packets.append(header_allocator, prefix)) |_| {
                    retained = true;
                } else |err| {
                    if (err != error.OutOfMemory) return err;
                    self.releasePlanningHeaders();
                    self.header_budget.denied = false;
                    if (self.memory) |*memory| memory.denied = null;
                }
            }
        }
        if (self.epochs.items.len == 0) return error.EmptyVideoTrack;
    }
    /// Append only to an immutable prefix. A scan failure invalidates this owner.
    /// Caller content-bindings verify the prefix before invoking this method.
    pub fn append(self: *Session, reader: *media.mp4.Reader) !void {
        if (self.borrowed) return error.InvalidVideoIndex;
        if (!self.valid) return error.InvalidVideoDecoder;
        if (reader.packets.len <= self.packet_configs.len or reader.packets.len > self.options.max_parameter_packets) return error.ResourceLimitExceeded;
        self.reader = reader;
        const begin = self.packet_configs.len;
        errdefer self.valid = false;
        self.scanFrom(begin) catch |err| return self.scanError(err);
    }
    /// Parameter NALs effective immediately before a retained packet. Initial
    /// avcC entries are already present in the reader and need no duplication.
    pub fn bootstrapBefore(self: *Session, a: std.mem.Allocator, index: usize) ![]u8 {
        return self.bootstrap(a, index, false);
    }
    pub fn bootstrap(self: *Session, a: std.mem.Allocator, index: usize, include_original: bool) ![]u8 {
        if (!self.valid or index > self.packet_configs.len) return error.InvalidPacketIndex;
        var sequence: [32]?Parameter = @splat(null);
        var picture: [256]?Parameter = @splat(null);
        for (&sequence, &self.sequence_versions) |*slot, *versions| slot.* = versions.before(index);
        for (&picture, &self.picture_versions) |*slot, *versions| slot.* = versions.before(index);
        var size: usize = 0;
        const length = self.length_bytes;
        for (sequence) |item| if (item) |parameter| if (include_original or !parameter.original) {
            if (parameter.bytes.len > (@as(u64, 1) << (@as(u6, length) * 8)) - 1) return error.ResourceLimitExceeded;
            size = try std.math.add(usize, size, length + parameter.bytes.len);
        };
        for (picture) |item| if (item) |parameter| if (include_original or !parameter.original) {
            if (parameter.bytes.len > (@as(u64, 1) << (@as(u6, length) * 8)) - 1) return error.ResourceLimitExceeded;
            size = try std.math.add(usize, size, length + parameter.bytes.len);
        };
        if (size > self.options.max_parameter_bytes) return error.ResourceLimitExceeded;
        const bytes = try a.alloc(u8, size);
        var cursor: usize = 0;
        for (sequence) |item| if (item) |parameter| if (include_original or !parameter.original) writeParameter(bytes, &cursor, length, parameter.bytes);
        for (picture) |item| if (item) |parameter| if (include_original or !parameter.original) writeParameter(bytes, &cursor, length, parameter.bytes);
        return bytes;
    }
    /// Allocate the new selector table before changing any retained state.
    pub fn rebase(self: *Session, removed: usize) !void {
        if (self.borrowed) return error.InvalidVideoIndex;
        if (!self.valid or removed > self.packet_configs.len) return error.InvalidPacketIndex;
        const allocator = self.budget.allocator();
        const selectors = try allocator.dupe(usize, self.packet_configs[removed..]);
        errdefer allocator.free(selectors);
        const remap = try allocator.alloc(usize, self.entries.items.len);
        defer allocator.free(remap);
        @memset(remap, std.math.maxInt(usize));
        // Retain packet configurations, retained epoch initial configurations
        // and the active configuration needed by later parameter-only appends.
        for (selectors) |id| {
            if (id == std.math.maxInt(usize)) continue;
            if (id >= remap.len) return error.InvalidLivePictureIndex;
            remap[id] = 0;
        }
        for (self.epochs.items) |epoch| if (epoch.end > removed) {
            if (epoch.entry >= remap.len) return error.InvalidLivePictureIndex;
            remap[epoch.entry] = 0;
        };
        if (self.active) |id| {
            if (id >= remap.len) return error.InvalidLivePictureIndex;
            remap[id] = 0;
        }
        var retained_entries: usize = 0;
        for (remap) |*id| if (id.* != std.math.maxInt(usize)) {
            id.* = retained_entries;
            retained_entries += 1;
        };

        for (&self.sequence_versions) |*versions| versions.rebase(allocator, removed);
        for (&self.picture_versions) |*versions| versions.rebase(allocator, removed);
        var keep: usize = 0;
        for (self.epochs.items) |epoch| {
            if (epoch.end <= removed) continue;
            var rebased = epoch;
            if (rebased.start < removed) rebased.independent = false;
            rebased.start -|= removed;
            rebased.end -= removed;
            self.epochs.items[keep] = rebased;
            keep += 1;
        }
        self.epochs.items.len = keep;
        if (keep == 0) if (self.active) |active| {
            // A decoder may retain references after all transport packets are
            // evicted. Preserve an empty current epoch for subsequent appends.
            self.epochs.items.len = 1;
            self.epochs.items[0] = .{ .start = 0, .end = 0, .entry = active, .max_refs = self.entries.items[active].config.max_refs };
        };
        // Everything below is infallible. Arrays compact in place so neither
        // native state nor published snapshots borrow released registry entries.
        var retained: usize = 0;
        for (self.entries.items, remap) |entry_value, id| {
            if (id == std.math.maxInt(usize)) {
                entry_value.config.groups.deinit(allocator);
                allocator.free(entry_value.avcc);
            } else {
                self.entries.items[retained] = entry_value;
                retained += 1;
            }
        }
        std.debug.assert(retained == retained_entries);
        self.entries.items.len = retained;
        self.resolved_pps = @splat(null); // Compaction assigns new entry IDs.
        for (selectors) |*id| if (id.* != std.math.maxInt(usize)) {
            id.* = remap[id.*];
        };
        for (self.epochs.items) |*epoch| epoch.entry = remap[epoch.entry];
        if (self.active) |id| self.active = remap[id];
        allocator.free(self.packet_configs);
        self.packet_configs = selectors;
        var kept_idrs: usize = 0;
        for (self.progressive_idrs.items) |packet| if (packet >= removed) {
            self.progressive_idrs.items[kept_idrs] = packet - removed;
            kept_idrs += 1;
        };
        self.progressive_idrs.items.len = kept_idrs;
    }
    pub fn configAt(self: *const Session, index: usize) !h264.Config {
        if (index >= self.packet_configs.len or self.packet_configs[index] == std.math.maxInt(usize)) return error.MissingVideoParameterSet;
        return self.entries.items[self.packet_configs[index]].config;
    }
    fn epochAt(self: *const Session, index: usize) !Epoch {
        if (index >= self.reader.packets.len) return error.InvalidPacketIndex;
        var lo: usize = 0;
        var hi = self.epochs.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (index >= self.epochs.items[mid].end) lo = mid + 1 else hi = mid;
        }
        if (lo == self.epochs.items.len or index < self.epochs.items[lo].start) return error.MissingVideoParameterSet;
        return self.epochs.items[lo];
    }
    pub fn independentEpochStart(self: *const Session, target: usize) ?usize {
        const epoch = self.epochAt(target) catch return null;
        return if (epoch.independent) epoch.start else null;
    }

    /// Last complete progressive IDR in this epoch at or before the target.
    pub fn progressiveRestart(self: *const Session, epoch_start: usize, target: usize) ?usize {
        const after = std.sort.upperBound(usize, self.progressive_idrs.items, target, packetOrder);
        if (after == 0) return null;
        const packet = self.progressive_idrs.items[after - 1];
        return if (packet >= epoch_start) packet else null;
    }

    /// Replay an immutable registry with request-local IO, cancellation counters
    /// and decoder allocation ownership. Only metadata slices are shared; this
    /// temporary view is never deinitialized as a registry owner.
    pub fn decodeSelectedBorrowed(self: *const Session, allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
        return self.decodeSelectedBorrowedWithOptions(allocator, reader, indexes, self.options, context, callback);
    }
    pub fn decodeSelectedBorrowedWithOptions(self: *const Session, allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, options: h264.Options, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
        if (reader.packets.ptr != self.reader.packets.ptr or reader.packets.len != self.reader.packets.len) return error.InvalidPacketIndex;
        var borrowed = self.*;
        borrowed.reader = reader;
        borrowed.options = options;
        borrowed.backing = allocator;
        borrowed.budget.backing = allocator;
        const live = borrowed.budget.live;
        defer std.debug.assert(borrowed.budget.live == live);
        return borrowed.decodeSelected(indexes, context, callback);
    }
    pub fn decodeSelected(self: *Session, indexes: []const usize, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
        if (indexes.len == 0 or indexes.len > self.options.max_selection_slots) return error.ResourceLimitExceeded;
        for (indexes) |index| if (index >= self.reader.packets.len) return error.InvalidPacketIndex;
        const allocator = self.budget.allocator();
        const selected = allocator.alloc(usize, indexes.len) catch |err| return if (err == error.OutOfMemory and self.budget.denied) error.ResourceLimitExceeded else err;
        defer allocator.free(selected);
        const slots = allocator.alloc(usize, indexes.len) catch |err| return if (err == error.OutOfMemory and self.budget.denied) error.ResourceLimitExceeded else err;
        defer allocator.free(slots);
        var options = self.options;
        if (self.budget.peak >= options.max_decode_bytes) return error.ResourceLimitExceeded;
        options.max_decode_bytes -= self.budget.peak;
        var stats = h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = self.budget.peak };
        orderSlots(slots, indexes);
        var first: usize = 0;
        while (first < slots.len) {
            const epoch = try self.epochAt(indexes[slots[first]]);
            const begin = first;
            while (first < slots.len and indexes[slots[first]] < epoch.end) : (first += 1) {
                selected[first] = indexes[slots[first]] - epoch.start;
            }
            var view = self.reader.*;
            view.packets = self.reader.packets[epoch.start..epoch.end];
            const value = self.entries.items[epoch.entry];
            view.track.avcc = value.avcc;
            view.track.width = @intCast(value.config.width);
            view.track.height = @intCast(value.config.height);
            const Capture = struct {
                slots: []const usize,
                context: *anyopaque,
                callback: h264.SelectionCallback,
                fn publish(ctx: *anyopaque, slot: usize, frame: *const h264.Frame) !void {
                    const capture: *@This() = @ptrCast(@alignCast(ctx));
                    try capture.callback(capture.context, capture.slots[slot], frame);
                }
            };
            const configurations = Context{ .session = self, .base = epoch.start, .independent_start = epoch.independent, .max_refs = epoch.max_refs };
            var start = begin;
            while (start < first) {
                var end = first;
                // An indexing observer needs every intervening picture. For
                // sparse output, split only when a verified IDR skips work.
                if (options.picture_observer == null) {
                    end = start + 1;
                    while (end < first) : (end += 1) {
                        if (self.progressiveRestart(epoch.start, indexes[slots[end]])) |restart|
                            if (restart > indexes[slots[end - 1]] + 1) break;
                    }
                }
                var capture = Capture{ .slots = slots[start..end], .context = context, .callback = callback };
                var remaining = options;
                remaining.max_dependency_packets -|= stats.decoded_packets;
                var frame = try h264.decodeWithConfigurations(self.backing, &view, selected[start..end], remaining, &capture, Capture.publish, configurations);
                defer frame.deinit();
                stats.decoded_packets += frame.decoded_packets;
                stats.payload_bytes += frame.payload_bytes;
                stats.decode_high_water = @max(stats.decode_high_water, frame.decode_high_water + self.budget.peak);
                start = end;
            }
        }
        return stats;
    }
    pub fn decodeFrame(self: *Session, index: usize) !h264.Frame {
        const epoch = try self.epochAt(index);
        var view = self.reader.*;
        view.packets = self.reader.packets[epoch.start..epoch.end];
        const entry_value = self.entries.items[epoch.entry];
        view.track.avcc = entry_value.avcc;
        view.track.width = @intCast(entry_value.config.width);
        view.track.height = @intCast(entry_value.config.height);
        var options = self.options;
        if (self.budget.peak >= options.max_decode_bytes) return error.ResourceLimitExceeded;
        options.max_decode_bytes -= self.budget.peak;
        var frame = try h264.decodeWithConfigurations(self.backing, &view, &.{index - epoch.start}, options, null, null, .{ .session = self, .base = epoch.start, .independent_start = epoch.independent, .max_refs = epoch.max_refs });
        frame.decode_high_water += self.budget.peak;
        return frame;
    }
};
fn packetOrder(left: usize, right: usize) std.math.Order {
    return std.math.order(left, right);
}

fn nalSet(bytes: []const u8, cursor: *usize) ![]const u8 {
    if (cursor.* > bytes.len or bytes.len - cursor.* < 2) return error.MalformedVideoConfig;
    const size = std.mem.readInt(u16, bytes[cursor.*..][0..2], .big);
    cursor.* += 2;
    if (size == 0 or size > bytes.len - cursor.*) return error.MalformedVideoConfig;
    const nal = bytes[cursor.*..][0..size];
    cursor.* += size;
    return nal;
}

/// Prediction storage/POC invariants. Other SPS tools (range, cropping, scaling,
/// reference capacity) may change between complete pictures without reallocating
/// reference samples. This is a qualified extension to CVS SPS activation rules.
pub fn layoutCompatible(a: h264.Config, b: h264.Config) bool {
    return a.coded_width == b.coded_width and a.coded_height == b.coded_height and
        a.bit_depth == b.bit_depth and a.chroma_format == b.chroma_format and
        a.separate_planes == b.separate_planes and a.frame_only == b.frame_only and
        a.mbaff == b.mbaff and a.frame_bits == b.frame_bits and a.poc_type == b.poc_type and
        a.poc_bits == b.poc_bits and a.poc_zero == b.poc_zero and a.poc_nonref == b.poc_nonref and
        a.poc_bottom == b.poc_bottom and a.poc_cycle == b.poc_cycle and
        std.mem.eql(i32, a.poc_offsets[0..a.poc_cycle], b.poc_offsets[0..b.poc_cycle]);
}

fn writeParameter(bytes: []u8, cursor: *usize, length: u3, nal: []const u8) void {
    for (0..length) |i| bytes[cursor.* + i] = @intCast((nal.len >> @intCast(8 * (length - i - 1))) & 255);
    cursor.* += length;
    @memcpy(bytes[cursor.*..][0..nal.len], nal);
    cursor.* += nal.len;
}

const TestBits = struct {
    bytes: [128]u8 = @splat(0),
    bits: usize = 0,
    fn bit(self: *@This(), value: u1) void {
        self.bytes[self.bits / 8] |= @as(u8, value) << @intCast(7 - self.bits % 8);
        self.bits += 1;
    }
    fn ue(self: *@This(), value: u32) void {
        const coded = value + 1;
        const count = 32 - @clz(coded);
        for (0..count - 1) |_| self.bit(0);
        var left: usize = count;
        while (left != 0) {
            left -= 1;
            self.bit(@intCast((coded >> @intCast(left)) & 1));
        }
    }
    fn se(self: *@This(), value: i32) void {
        self.ue(@intCast(if (value > 0) value * 2 - 1 else -value * 2));
    }
    fn nal(self: *@This(), a: std.mem.Allocator, header: u8) ![]u8 {
        self.bit(1);
        while (self.bits % 8 != 0) self.bit(0);
        var list: std.ArrayList(u8) = .empty;
        defer list.deinit(a);
        try list.append(a, header);
        var zeros: usize = 0;
        for (self.bytes[0 .. self.bits / 8]) |byte| {
            if (zeros >= 2 and byte <= 3) {
                try list.append(a, 3);
                zeros = 0;
            }
            try list.append(a, byte);
            zeros = if (byte == 0) zeros + 1 else 0;
        }
        return list.toOwnedSlice(a);
    }
};
fn parameterPacket(a: std.mem.Allocator, id: u32, qp: i32) ![]u8 {
    var pps = TestBits{};
    pps.ue(id);
    pps.ue(0);
    pps.bit(0);
    pps.bit(0);
    pps.ue(0);
    pps.ue(0);
    pps.ue(0);
    pps.bit(0);
    pps.bit(0);
    pps.bit(0);
    pps.se(qp);
    pps.se(0);
    pps.se(0);
    pps.bit(1);
    pps.bit(0);
    pps.bit(0);
    const parameters = try pps.nal(a, 0x68);
    defer a.free(parameters);
    // This is a parameter-planning corpus: only the activation prefix is
    // supplied. It does not claim valid reconstruction of macroblocks.
    var slice = TestBits{};
    slice.ue(0);
    slice.ue(2);
    slice.ue(id);
    const activation = try slice.nal(a, 0x41);
    defer a.free(activation);
    const bytes = try a.alloc(u8, 8 + parameters.len + activation.len);
    std.mem.writeInt(u32, bytes[0..4], @intCast(parameters.len), .big);
    @memcpy(bytes[4..][0..parameters.len], parameters);
    const at = 4 + parameters.len;
    std.mem.writeInt(u32, bytes[at..][0..4], @intCast(activation.len), .big);
    @memcpy(bytes[at + 4 ..], activation);
    return bytes;
}
fn configurationChurn(a: std.mem.Allocator, rounds: usize) !void {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "configuration-churn", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-dynamic-pps.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var view = reader;
    view.packets = reader.packets[0..1];
    const owner = try Session.init(a, &view, .{ .max_configurations = 2 });
    defer owner.deinit();
    try owner.rebase(1);
    for (0..rounds) |i| {
        const bytes = try parameterPacket(a, @intCast(i % 256), @intCast(i / 256));
        defer a.free(bytes);
        var input = media.source.Source{ .allocator = a, .identity = "parameter-activation", .admission_pool = &pool, .storage = .{ .borrowed = bytes } };
        var descriptor = reader.packets[0];
        descriptor.offset = 0;
        descriptor.size = @intCast(bytes.len);
        view.input = &input;
        view.packets = @as(*[1]media.mp4.Packet, @ptrCast(&descriptor));
        try owner.append(&view);
        try std.testing.expect(owner.entries.items.len <= 2);
        {
            // Repeated slices must resolve without rebuilding avcC or touching
            // an allocator. After rebase the next append exercises remapping.
            const backing = owner.budget.backing;
            owner.budget.backing = std.testing.failing_allocator;
            defer owner.budget.backing = backing;
            try std.testing.expectEqual(owner.active.?, try owner.entry(i % 256));
        }
        const live = owner.budget.live;
        try owner.rebase(1);
        try std.testing.expectEqual(@as(usize, 1), owner.entries.items.len);
        try std.testing.expectEqual(@as(?usize, 0), owner.active);
        try std.testing.expectEqual(@as(usize, 0), owner.epochs.items[0].entry);
        try std.testing.expectEqual(@as(u32, @intCast(i % 256)), owner.entries.items[0].config.pps);
        try std.testing.expect(owner.budget.live < live);
    }
    try std.testing.expectEqual(rounds + 1, owner.probe_packets);
}
test "dynamic AVC configuration compaction bounds 600 activations under two live entries" {
    try configurationChurn(std.testing.allocator, 600);
}
test "dynamic AVC configuration compaction allocation failures retain clean shared owners" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, configurationChurn, .{@as(usize, 6)});
}

test "dynamic AVC parameter versions retain effective changes across repeated headers and rebases" {
    const a = std.testing.allocator;
    var history = Versions{};
    defer history.deinit(a);
    _ = try history.install(a, "original", null);
    _ = try history.install(a, "original", 0);
    for (1..300) |i| try std.testing.expect(try history.install(a, "original", i) == null);
    _ = try history.install(a, "first change", 300);
    _ = try history.install(a, "second change", 301);
    try std.testing.expectEqual(@as(usize, 4), history.list.items.len);
    try std.testing.expect(history.before(0).?.original);
    try std.testing.expect(!history.before(300).?.original);
    try std.testing.expectEqualStrings("first change", history.before(301).?.bytes);
    history.rebase(a, 301);
    try std.testing.expectEqualStrings("first change", history.before(0).?.bytes);
    try std.testing.expectEqualStrings("second change", history.before(1).?.bytes);
}

test "dynamic AVC continuation shares its packet budget across geometry epochs" {
    const a = std.testing.allocator;
    // Select a P-picture needing the first IDR, then an IDR in a new epoch.
    const closed = @embedFile("../testdata/schedule-closed.mp4");
    const geometry = @embedFile("../testdata/h264-dynamic-geometry.mp4");
    var first = media.source.Source{ .allocator = a, .identity = "closed", .storage = .{ .borrowed = closed } };
    var second = media.source.Source{ .allocator = a, .identity = "geometry", .storage = .{ .borrowed = geometry } };
    var left = try media.mp4.Reader.init(a, &first, .{});
    defer left.deinit();
    var right = try media.mp4.Reader.init(a, &second, .{});
    defer right.deinit();
    const joined = try std.mem.concat(a, u8, &.{ closed, geometry });
    defer a.free(joined);
    var source = media.source.Source{ .allocator = a, .identity = "epochs", .storage = .{ .borrowed = joined } };
    var view = left;
    var packets = [_]media.mp4.Packet{ left.packets[0], left.packets[1], right.packets[1] };
    packets[2].offset += closed.len;
    view.input = &source;
    view.packets = &packets;
    const registry = try Session.init(a, &view, .{ .max_dependency_packets = 2 });
    defer registry.deinit();
    const Sink = struct {
        fn accept(raw: *anyopaque, _: usize, _: *const h264.Frame) !void {
            const seen: *usize = @ptrCast(@alignCast(raw));
            seen.* += 1;
        }
    };
    var seen: usize = 0;
    var continuation = Continuation{ .session = registry };
    defer continuation.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, continuation.decodeSelected(&.{ 1, 2 }, &seen, Sink.accept));
    try std.testing.expectEqual(@as(usize, 1), seen);
    try std.testing.expect(continuation.core == null);
    registry.options.max_dependency_packets = 3;
    const stats = try continuation.decodeSelected(&.{ 1, 2, 1, 2 }, &seen, Sink.accept);
    try std.testing.expectEqual(@as(usize, 3), stats.decoded_packets);
    try std.testing.expectEqual(@as(usize, 5), seen);
}
