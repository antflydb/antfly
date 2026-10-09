// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Owned parameter registry and packet configuration epochs for avc3 sources.
//! Reusable sessions avoid repeating the parameter scan for each selection.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const Bits = @import("h264_bits.zig").Bits;
const Budget = @import("decode_budget.zig").Budget;
const PPS = struct { bytes: []u8, sps: usize };
pub const Entry = struct { avcc: []u8, config: h264.Config, sps_hash: [32]u8 };
pub const Epoch = struct { start: usize, end: usize, entry: usize };
pub const Context = struct { session: *Session, base: usize };
pub const Session = struct {
    backing: std.mem.Allocator,
    budget: Budget,
    reader: *media.mp4.Reader,
    options: h264.Options,
    reservation: media.admission.Token = .{},
    sequence: [32]?[]u8 = @splat(null),
    picture: [256]?PPS = @splat(null),
    entries: std.ArrayList(Entry) = .empty,
    epochs: std.ArrayList(Epoch) = .empty,
    packet_configs: []usize = &.{},
    probe_packets: usize = 0,
    probe_bytes: u64 = 0,
    pub fn init(allocator: std.mem.Allocator, reader: *media.mp4.Reader, options: h264.Options) !*Session {
        if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
        if (reader.packets.len > options.max_parameter_packets) return error.ResourceLimitExceeded;
        const self = try allocator.create(Session);
        self.* = .{ .backing = allocator, .budget = .{ .backing = allocator, .limit = options.max_parameter_bytes }, .reader = reader, .options = options };
        errdefer self.deinit();
        self.reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, options.max_parameter_bytes, @sizeOf(Session)) }) else media.admission.Token{};
        self.scan() catch |err| return if (err == error.OutOfMemory and self.budget.denied) error.ResourceLimitExceeded else err;
        return self;
    }
    pub fn deinit(self: *Session) void {
        const allocator = self.budget.allocator();
        for (self.entries.items) |value| {
            value.config.groups.deinit(allocator);
            allocator.free(value.avcc);
        }
        self.entries.deinit(allocator);
        self.epochs.deinit(allocator);
        for (self.sequence) |set| if (set) |bytes| allocator.free(bytes);
        for (self.picture) |set| if (set) |value| allocator.free(value.bytes);
        allocator.free(self.packet_configs);
        self.reservation.deinit();
        self.backing.destroy(self);
    }
    fn install(self: *Session, nal: []const u8) !void {
        if (nal.len > std.math.maxInt(u16)) return error.ResourceLimitExceeded;
        const allocator = self.budget.allocator();
        var bits = try Bits.initControlled(allocator, nal, self.reader.input.control);
        defer bits.deinit();
        if (nal[0] & 31 == 7) {
            _ = try bits.read(24);
            const id = try bits.ue();
            if (id >= self.sequence.len) return error.MalformedVideoConfig;
            const owned = try allocator.dupe(u8, nal);
            if (self.sequence[id]) |old| allocator.free(old);
            self.sequence[id] = owned;
        } else if (nal[0] & 31 == 8) {
            const id = try bits.ue();
            const sps = try bits.ue();
            if (id >= self.picture.len or sps >= self.sequence.len) return error.MalformedVideoConfig;
            const owned = try allocator.dupe(u8, nal);
            if (self.picture[id]) |old| allocator.free(old.bytes);
            self.picture[id] = .{ .bytes = owned, .sps = sps };
        } else return error.MalformedVideoConfig;
    }
    fn initial(self: *Session) !void {
        const bytes = self.reader.track.avcc;
        try @import("avc.zig").validatePortableConfig(bytes);
        var cursor: usize = 6;
        for (0..bytes[5] & 31) |_| {
            try self.install(try nalSet(bytes, &cursor));
        }
        const count = bytes[cursor];
        cursor += 1;
        for (0..count) |_| try self.install(try nalSet(bytes, &cursor));
    }
    fn entry(self: *Session, id: usize) !usize {
        if (id >= self.picture.len) return error.MalformedVideoConfig;
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
        return self.entries.items.len - 1;
    }
    fn scan(self: *Session) !void {
        try self.initial();
        const allocator = self.budget.allocator();
        self.packet_configs = try allocator.alloc(usize, self.reader.packets.len);
        @memset(self.packet_configs, std.math.maxInt(usize));
        var active: ?usize = null;
        for (self.reader.packets, 0..) |packet, i| {
            if (packet.size > self.options.max_packet_bytes) return error.ResourceLimitExceeded;
            var lease = try self.reader.readPacket(i);
            defer lease.deinit();
            self.probe_packets += 1;
            self.probe_bytes += lease.bytes.len;
            try @import("avc.zig").validatePortablePacket(lease.bytes, self.reader.track.nal_length_bytes);
            var cursor: usize = 0;
            var selected: ?usize = null;
            while (cursor < lease.bytes.len) {
                try self.reader.input.control.check();
                var size: usize = 0;
                for (lease.bytes[cursor..][0..self.reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
                cursor += self.reader.track.nal_length_bytes;
                const nal = lease.bytes[cursor..][0..size];
                cursor += size;
                switch (nal[0] & 31) {
                    7, 8 => try self.install(nal),
                    1, 2, 5 => {
                        var bits = try Bits.initSlice(allocator, nal, self.reader.input.control, true);
                        defer bits.deinit();
                        _ = try bits.ue();
                        _ = try bits.ue();
                        const index = try self.entry(try bits.ue());
                        if (selected) |previous| if (previous != index) return error.MixedVideoPictures;
                        selected = index;
                        if (active == null or !std.mem.eql(u8, &self.entries.items[active.?].sps_hash, &self.entries.items[index].sps_hash)) {
                            if (active != null and nal[0] & 31 != 5) return error.UnsupportedDynamicVideoConfig;
                            if (self.epochs.items.len != 0) self.epochs.items[self.epochs.items.len - 1].end = i;
                            try self.epochs.ensureTotalCapacityPrecise(allocator, self.epochs.items.len + 1);
                            self.epochs.appendAssumeCapacity(.{ .start = if (active == null) 0 else i, .end = self.reader.packets.len, .entry = index });
                        }
                        active = index;
                    },
                    else => {},
                }
            }
            self.packet_configs[i] = selected orelse active orelse std.math.maxInt(usize);
        }
        if (self.epochs.items.len == 0) return error.EmptyVideoTrack;
    }
    pub fn configAt(self: *const Session, index: usize) !h264.Config {
        if (index >= self.packet_configs.len or self.packet_configs[index] == std.math.maxInt(usize)) return error.MissingVideoParameterSet;
        return self.entries.items[self.packet_configs[index]].config;
    }
    fn epochAt(self: *const Session, index: usize) !Epoch {
        if (index >= self.reader.packets.len) return error.InvalidPacketIndex;
        for (self.epochs.items) |epoch| if (index >= epoch.start and index < epoch.end) return epoch;
        return error.MissingVideoParameterSet;
    }
    pub fn decodeSelected(self: *Session, indexes: []const usize, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
        if (indexes.len == 0 or indexes.len > self.options.max_dependency_packets) return error.ResourceLimitExceeded;
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
        for (self.epochs.items) |epoch| {
            var count: usize = 0;
            for (indexes, 0..) |index, slot| if (index >= epoch.start and index < epoch.end) {
                selected[count] = index - epoch.start;
                slots[count] = slot;
                count += 1;
            };
            if (count == 0) continue;
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
            var capture = Capture{ .slots = slots[0..count], .context = context, .callback = callback };
            var frame = try h264.decodeWithConfigurations(self.backing, &view, selected[0..count], options, &capture, Capture.publish, .{ .session = self, .base = epoch.start });
            defer frame.deinit();
            stats.decoded_packets += frame.decoded_packets;
            stats.payload_bytes += frame.payload_bytes;
            stats.decode_high_water = @max(stats.decode_high_water, frame.decode_high_water + self.budget.peak);
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
        var frame = try h264.decodeWithConfigurations(self.backing, &view, &.{index - epoch.start}, options, null, null, .{ .session = self, .base = epoch.start });
        frame.decode_high_water += self.budget.peak;
        return frame;
    }
};
fn nalSet(bytes: []const u8, cursor: *usize) ![]const u8 {
    if (cursor.* > bytes.len or bytes.len - cursor.* < 2) return error.MalformedVideoConfig;
    const size = std.mem.readInt(u16, bytes[cursor.*..][0..2], .big);
    cursor.* += 2;
    if (size == 0 or size > bytes.len - cursor.*) return error.MalformedVideoConfig;
    const nal = bytes[cursor.*..][0..size];
    cursor.* += size;
    return nal;
}
