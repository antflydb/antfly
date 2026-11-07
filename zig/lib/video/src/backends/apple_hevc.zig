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

//! HEVC in MP4 through VideoToolbox. Each run opens a fresh session, so no
//! references or delayed pictures cross a restart. Frames arrive in decode order.
const std = @import("std");
const builtin = @import("builtin");
const media = @import("antfly_media");
const apple = @import("apple.zig");
const restart = @import("../hevc_restart.zig");
const config_info = @import("../hevc_config.zig");

pub const max_surface_bytes: u64 = 1024 * 1024 * 1024;

/// Selected pictures leave as soon as they decode, so only the reference pool and
/// one output stay live. That keeps ordinary 4K clips on the hardware route.
pub fn surfaceBytes(track: media.mp4.Track) !u64 {
    return surfaceBytesControlled(track, .{});
}

pub fn surfaceBytesControlled(track: media.mp4.Track, control: media.source.Control) !u64 {
    const output_format = try format(track.codec_config, false);
    const output = try apple.surfacePayloadBytesForDepth(track.width, track.height, output_format.bitDepth());
    var arrays = (try @import("../hevc_nal.zig").parseConfigurationRecord(track.codec_config)).arrays();
    var references: u64 = 0;
    var count: usize = 0;
    while (try arrays.next()) |array| {
        if (array.nal_unit_type != 33) continue;
        var units = array.nals;
        while (try units.next()) |unit| {
            try control.check();
            if (count == 256) return error.ResourceLimitExceeded;
            count += 1;
            const geometry = config_info.spsGeometry(unit, control) catch |err| switch (err) {
                error.UnsupportedVideoProfile => continue,
                else => return err,
            };
            references = @max(references, try apple.surfaceAdmissionBytesForDepth(geometry.width, geometry.height, 0, geometry.depth));
            // A resolved record puts the active SPS first, and activation has
            // checked that every picture preserves its geometry/depth policy.
            if (track.hevc_policy_validated) return std.math.add(u64, references, output);
        }
    }
    if (references == 0) return error.MissingVideoParameterSet;
    // Unresolved metadata reserves the largest supported SPS envelope. In-band
    // additions are admitted after controlled activation, before session creation.
    return std.math.add(u64, references, output);
}

pub const Sink = struct {
    context: *anyopaque,
    accept: *const fn (*anyopaque, usize, *const apple.Surface) anyerror!void,
};
pub const Options = struct {
    require_hardware: bool = false,
    max_frames: usize = 64,
    max_decode_packets: usize = 500_000,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_surface_bytes: u64 = max_surface_bytes,
    pool: ?*media.admission.Pool = null,
    device_surfaces: bool = false,
};
pub const Receipt = struct { hardware: bool, submitted_packets: usize };

/// hvcC carries the sample layout this route can decode.
pub fn format(config: []const u8, full: bool) !apple.Format {
    if (config.len < 23 or config[0] != 1 or config[16] & 3 != 1 or config[17] & 7 != config[18] & 7) return error.UnsupportedVideoProfile;
    return switch (config[17] & 7) {
        0 => if (full) .nv12_full else .nv12_video,
        2 => if (full) .p010_full else .p010_video,
        else => error.UnsupportedVideoProfile,
    };
}
pub fn hasParameterSets(config: []const u8) bool {
    return config_info.hasParameterSets(config);
}

const Capture = struct {
    selected: []const usize,
    slots: []?apple.Surface,
    seen: []bool,
    format: apple.Format,
    failure: ?anyerror = null,
    mutex: std.atomic.Mutex = .unlocked,
    fn callback(context: *anyopaque, index: usize, status: i32, image: ?*anyopaque) callconv(.c) void {
        const self: *Capture = @ptrCast(@alignCast(context));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        // A leading picture the run can't reconstruct may report failure, but
        // nothing selected depends on it.
        const slot = std.mem.indexOfScalar(usize, self.selected, index) orelse return;
        if (self.failure != null) return;
        if (status != 0 or image == null) {
            self.failure = error.VideoDecodeFailed;
            return;
        }
        if (self.seen[slot]) {
            self.failure = error.DuplicateDecodedFrame;
            return;
        }
        const surface = apple.Surface.fromBorrowed(image.?) catch |err| {
            self.failure = err;
            return;
        };
        if (surface.format != self.format) {
            var owned = surface;
            owned.deinit();
            self.failure = error.UnsupportedVideoColor;
            return;
        }
        self.seen[slot] = true;
        self.slots[slot] = surface;
    }
    fn flush(self: *Capture, sink: Sink) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        const failure = self.failure;
        self.mutex.unlock();
        if (failure) |err| return err;
        for (self.slots, self.selected) |*slot, id| {
            if (slot.*) |*surface| {
                defer {
                    surface.deinit();
                    slot.* = null;
                }
                try sink.accept(sink.context, id, surface);
            }
        }
    }
};

/// `selected` holds unique packet indexes in any order.
pub fn decodeTo(a: std.mem.Allocator, reader: *media.mp4.Reader, selected: []const usize, options: Options, sink: Sink) !Receipt {
    if (builtin.os.tag != .macos) return error.UnsupportedVideoBackend;
    const original_track = reader.track;
    const normalized = try config_info.normalizeReader(a, reader);
    defer {
        reader.track = original_track;
        if (normalized) |bytes| a.free(bytes);
    }
    try reader.input.control.check();
    if (reader.track.codec != .hevc or !hasParameterSets(reader.track.codec_config)) return error.UnsupportedVideoProfile;
    if (selected.len == 0 or selected.len > options.max_frames) return error.ResourceLimitExceeded;
    if (reader.track.timescale > std.math.maxInt(i32)) return error.UnsupportedTimeline;
    const signal = try config_info.parse(a, reader.track.codec_config);
    const surface_format = try format(reader.track.codec_config, try config_info.fullRange(signal, reader.track.color_info));
    const bytes = try surfaceBytesControlled(reader.track, reader.input.control);
    if (bytes > options.max_surface_bytes) return error.ResourceLimitExceeded;
    var reservation = if (options.pool) |pool| try pool.acquire(if (options.device_surfaces) .{ .device_bytes = bytes } else .{ .host_bytes = bytes }) else media.admission.Token{};
    defer reservation.deinit();
    const sorted = try a.dupe(usize, selected);
    defer a.free(sorted);
    std.mem.sort(usize, sorted, {}, std.sort.asc(usize));
    const runs = try restart.plan(a, reader, sorted);
    defer a.free(runs);
    const slots = try a.alloc(?apple.Surface, selected.len);
    defer a.free(slots);
    @memset(slots, null);
    defer for (slots) |*slot| if (slot.*) |*surface| surface.deinit();
    const seen = try a.alloc(bool, selected.len);
    defer a.free(seen);
    @memset(seen, false);
    var capture = Capture{ .selected = selected, .slots = slots, .seen = seen, .format = surface_format };
    var receipt = Receipt{ .hardware = false, .submitted_packets = 0 };
    for (runs) |run| {
        receipt.submitted_packets += run.last - run.first + 1;
        if (receipt.submitted_packets > options.max_decode_packets) return error.ResourceLimitExceeded;
        var session = try apple.CodedSession.init(.hevc, reader.track.codec_config, reader.track.width, reader.track.height, surface_format, options.require_hardware, Capture.callback, &capture);
        // Fences callbacks on errors too, before the capture or slots expire.
        defer session.deinit();
        for (run.first..run.last + 1) |index| {
            try reader.input.control.check();
            const packet = reader.packets[index];
            if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
            var lease = try reader.readPacket(index);
            defer lease.deinit();
            // VideoToolbox rejects samples that its leading-picture rules skip;
            // only a selected picture's status decides the outcome.
            session.submit(lease.bytes, index, packet.pts, packet.duration, reader.track.timescale) catch |err| {
                if (std.mem.indexOfScalar(usize, selected, index) != null) return err;
            };
            try capture.flush(sink);
        }
        try session.drain();
        try capture.flush(sink);
        receipt.hardware = session.hardware();
    }
    for (seen) |value| if (!value) return error.MissingDecodedFrame;
    return receipt;
}

test "HEVC format comes from hvcC and rejects layouts the route can't output" {
    var config = @as([23]u8, @splat(0));
    config[0] = 1;
    config[16] = 1;
    try std.testing.expectEqual(apple.Format.nv12_video, try format(&config, false));
    try std.testing.expectEqual(apple.Format.nv12_full, try format(&config, true));
    config[17] = 2;
    config[18] = 2;
    try std.testing.expectEqual(apple.Format.p010_video, try format(&config, false));
    config[16] = 2;
    try std.testing.expectError(error.UnsupportedVideoProfile, format(&config, false));
}

test "VideoToolbox HEVC decodes selected pictures from a sparse open timeline" {
    if (builtin.os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/hevc-mp4.mp4") }, .identity = "hevc" };
    var reader = try media.mp4.Reader.init(a, &source, .{ .codecs = std.EnumSet(media.mp4.Codec).initMany(&.{.hevc}) });
    defer reader.deinit();
    const Count = struct {
        hits: usize = 0,
        fn accept(raw: *anyopaque, _: usize, surface: *const apple.Surface) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(surface.width != 0);
            self.hits += 1;
        }
    };
    var count: Count = .{};
    // Prove restart discovery uses the bitstream even when all hints are absent.
    for (reader.packets) |*packet| packet.sync = false;
    const last = reader.packets.len - 1;
    const receipt = try decodeTo(a, &reader, &.{ last, 0, last / 2 }, .{}, .{ .context = &count, .accept = Count.accept });
    try std.testing.expectEqual(@as(usize, 3), count.hits);
    try std.testing.expect(receipt.submitted_packets > 0);
}

test "HEVC cropped pictures reserve coded references and output separately before decoding" {
    const a = std.testing.allocator;
    inline for (.{ "hevc-cropped4k-8.mp4", "hevc-cropped4k-10.mp4", "hevc-cropped4k-inband.mp4" }) |name| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/" ++ name) }, .identity = "cropped-budget" };
        var reader = try media.mp4.Reader.init(a, &source, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
        defer reader.deinit();
        const normalized = try config_info.normalizeReader(a, &reader);
        defer if (normalized) |bytes| a.free(bytes);
        const depth: u8 = if (std.mem.indexOf(u8, name, "-10.") != null) 10 else 8;
        const expected = try std.math.add(u64, try apple.surfaceAdmissionBytesForDepth(3840, 2160, 0, depth), try apple.surfacePayloadBytesForDepth(128, 128, depth));
        try std.testing.expectEqual(expected, try surfaceBytes(reader.track));
        var upscaled = reader.track;
        upscaled.width = 4096;
        upscaled.height = 4096;
        try std.testing.expect(try surfaceBytes(upscaled) > expected);
        if (builtin.os.tag != .macos) continue;
        const Count = struct {
            hits: usize = 0,
            fn accept(raw: *anyopaque, _: usize, surface: *const apple.Surface) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try std.testing.expectEqual(@as(u32, 128), surface.width);
                try std.testing.expectEqual(@as(u32, 128), surface.height);
                self.hits += 1;
            }
        };
        var count: Count = .{};
        const sink = Sink{ .context = &count, .accept = Count.accept };
        const old_visible_budget = try apple.surfaceAdmissionBytesForDepth(128, 128, 1, depth);
        try std.testing.expectError(error.ResourceLimitExceeded, decodeTo(a, &reader, &.{0}, .{ .max_surface_bytes = old_visible_budget }, sink));
        var denied = media.admission.Pool{ .limits = .{ .device_bytes = expected - 1 } };
        try std.testing.expectError(error.SharedAdmissionExceeded, decodeTo(a, &reader, &.{0}, .{ .pool = &denied, .device_surfaces = true }, sink));
        try std.testing.expectEqual(@as(usize, 0), count.hits);
        try std.testing.expectEqualDeep(media.admission.Resources{}, denied.snapshot());
        var pool = media.admission.Pool{ .limits = .{ .device_bytes = expected } };
        _ = try decodeTo(a, &reader, &.{0}, .{ .max_surface_bytes = expected, .pool = &pool, .device_surfaces = true }, sink);
        try std.testing.expectEqual(@as(usize, 1), count.hits);
        try std.testing.expectEqual(expected, pool.high_water.device_bytes);
        try std.testing.expectEqualDeep(media.admission.Resources{}, pool.snapshot());
    }
}
