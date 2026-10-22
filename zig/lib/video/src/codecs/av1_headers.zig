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

//! Metadata-only AV1 OBU/header walk. Uses the owned dav1d header grammar;
//! references contain parsed headers, never fabricated pixels or codec workers.
//! Tile framing is checked; entropy/reconstruction validity belongs to replay.
const std = @import("std");
const media = @import("antfly_media");
const runtime = @import("runtime.zig");
const grammar = @import("av1/obu.zig");
comptime {
    _ = @import("av1/getbits.zig");
    _ = @import("av1/tables.zig");
}
const Geometry = @import("vp9_header.zig").Geometry;
pub const Options = struct { max_pixels: usize, operating_point: u5 = 0, all_layers: bool = true, control: media.source.Control = .{} };
pub const Session = struct {
    allocator: std.mem.Allocator,
    options: Options,
    owner: runtime.Owner,
    context: grammar.Dav1dContext = .{},
    sequence: grammar.Dav1dSequenceHeader = .{},
    current: grammar.Dav1dFrameHeader = .{},
    references: [8]grammar.Dav1dFrameHeader = @splat(.{}),
    have_sequence: bool = false,
    pending: bool = false,
    next_tile: usize = 0,
    failed: bool = false,
    packet_sequence: ?SequenceSpan = null,
    packet_restart_key: bool = false,
    packet_restart_needs_sequence: bool = false,
    pub const SequenceSpan = struct { offset: usize, size: usize };
    pub fn create(a: std.mem.Allocator, options: Options) !*Session {
        try options.control.check();
        const self = try a.create(Session);
        self.* = .{ .allocator = a, .options = options, .owner = .{ .budget = .{ .backing = a, .limit = 1024 * 1024 }, .control = options.control } };
        self.context.seq_hdr = &self.sequence;
        self.context.frame_hdr = &self.current;
        self.context.strict_std_compliance = 1;
        self.context.frame_size_limit = @intCast(@min(options.max_pixels, std.math.maxInt(c_uint)));
        return self;
    }
    pub fn deinit(self: *Session) void {
        self.owner.sweep();
        self.allocator.destroy(self);
    }
    pub fn finish(self: *const Session) !void {
        if (self.pending or self.failed) return error.InvalidVideoData;
    }
    /// Outputs complete displayed pictures only, in coded OBU order. Reference
    /// updates happen for hidden pictures too. Owners are invalidated on failure.
    pub fn walk(self: *Session, bytes: []const u8, context: *anyopaque, accept: *const fn (*anyopaque, Geometry) anyerror!void) !void {
        if (self.failed) return error.InvalidDecoderSession;
        const scope = runtime.Scope.enter(&self.owner);
        defer scope.deinit();
        errdefer self.failed = true;
        self.packet_sequence = null;
        self.packet_restart_key = false;
        self.packet_restart_needs_sequence = false;
        var first_frame = !self.pending;
        var position: usize = 0;
        while (position < bytes.len) {
            const obu_start = position;
            try self.options.control.check();
            const tag = bytes[position];
            position += 1;
            if (tag & 0x81 != 0) return error.InvalidVideoData;
            const kind = (tag >> 3) & 15;
            var temporal: u3 = 0;
            var spatial: u2 = 0;
            if (tag & 4 != 0) {
                if (position == bytes.len) return error.InvalidVideoData;
                const extension = bytes[position];
                position += 1;
                if (extension & 7 != 0) return error.InvalidVideoData;
                temporal = @intCast(extension >> 5);
                spatial = @intCast((extension >> 3) & 3);
            }
            const size = if (tag & 2 != 0) try leb(bytes, &position) else bytes.len - position;
            if (size > bytes.len - position) return error.InvalidVideoData;
            const payload = bytes[position..][0..size];
            position += size;
            if (self.have_sequence and kind != 1 and kind != 2 and self.context.operating_point_idc != 0) {
                const idc = self.context.operating_point_idc;
                if (idc & (@as(u32, 1) << temporal) == 0 or idc & (@as(u32, 1) << (@as(u5, spatial) + 8)) == 0) continue;
            }
            switch (kind) {
                1 => {
                    try self.sequenceHeader(payload);
                    self.packet_sequence = .{ .offset = obu_start, .size = position - obu_start };
                },
                2 => {
                    if (payload.len != 0 or self.pending) return error.InvalidVideoData;
                },
                3, 6 => {
                    if (!self.have_sequence or self.pending or payload.len == 0) return error.InvalidVideoData;
                    self.current = .{};
                    self.current.temporal_id = temporal;
                    self.current.spatial_id = spatial;
                    var bits = initBits(payload);
                    if (grammar.parse_frame_hdr(&self.context, &bits) < 0 or bits.@"error" != 0) return error.InvalidVideoData;
                    try self.owner.result();
                    if (first_frame) {
                        self.packet_restart_needs_sequence = self.packet_sequence == null;
                        self.packet_restart_key = self.current.show_existing_frame == 0 and self.current.frame_type == grammar.DAV1D_FRAME_TYPE_KEY and self.current.refresh_frame_flags == 255;
                    }
                    first_frame = false;
                    if (kind == 3 and grammar.check_trailing_bits(&bits, 1) < 0) return error.InvalidVideoData;
                    if (self.current.show_existing_frame != 0) {
                        if (kind == 6) return error.InvalidVideoData;
                        const slot = self.current.existing_frame_idx;
                        if (self.context.refs[slot].p.p.frame_hdr == null or self.context.refs[slot].p.showable == 0) return error.InvalidVideoData;
                        const previous = self.references[slot];
                        const geometry = try self.pictureGeometry(previous);
                        if (self.options.all_layers or previous.spatial_id >= self.context.max_spatial_id) try accept(context, geometry);
                        if (previous.frame_type == grammar.DAV1D_FRAME_TYPE_KEY) {
                            var consumed = previous;
                            consumed.showable_frame = 0;
                            self.refresh(consumed, 255);
                        }
                    } else {
                        _ = try self.pictureGeometry(self.current);
                        self.pending = true;
                        self.next_tile = 0;
                        if (kind == 6) {
                            // Frame OBU appends a tile-group header directly to
                            // the uncompressed header's bitstream, then aligns.
                            try self.tiles(&bits, context, accept);
                        }
                    }
                },
                4 => {
                    if (!self.pending or payload.len == 0) return error.InvalidVideoData;
                    var bits = initBits(payload);
                    try self.tiles(&bits, context, accept);
                },
                7 => {
                    if (!self.pending or payload.len == 0) return error.InvalidVideoData;
                    // A redundant header must describe the same pending frame.
                    const saved = self.current;
                    self.current = .{};
                    self.current.temporal_id = temporal;
                    self.current.spatial_id = spatial;
                    var bits = initBits(payload);
                    if (grammar.parse_frame_hdr(&self.context, &bits) < 0 or bits.@"error" != 0 or grammar.check_trailing_bits(&bits, 1) < 0) return error.InvalidVideoData;
                    if (!sameFrameHeader(saved, self.current)) return error.InvalidVideoData;
                    self.current = saved;
                },
                5, 15 => {}, // Metadata/padding do not create pictures.
                else => return error.InvalidVideoData,
            }
        }
    }
    fn sequenceHeader(self: *Session, payload: []const u8) !void {
        if (self.pending or payload.len == 0) return error.InvalidVideoData;
        var sequence: grammar.Dav1dSequenceHeader = .{};
        var bits = initBits(payload);
        if (grammar.parse_seq_hdr(&sequence, &bits, 1) < 0 or bits.@"error" != 0) return error.InvalidVideoData;
        try self.owner.result();
        if (self.options.operating_point >= sequence.num_operating_points) return error.UnsupportedVideoProfile;
        var previous = self.sequence;
        previous.operating_parameter_info = sequence.operating_parameter_info;
        if (!self.have_sequence or !std.meta.eql(previous, sequence)) {
            for (&self.context.refs) |*ref| ref.* = .{};
        }
        self.sequence = sequence;
        self.have_sequence = true;
        const idc = sequence.operating_points[self.options.operating_point].idc;
        self.context.operating_point_idc = idc;
        const spatial_mask = idc >> 8;
        self.context.max_spatial_id = if (spatial_mask != 0) @intCast(std.math.log2_int(u32, spatial_mask)) else 0;
    }
    fn pictureGeometry(self: *const Session, frame: grammar.Dav1dFrameHeader) !Geometry {
        if (frame.width[1] <= 0 or frame.height <= 0) return error.InvalidVideoData;
        const width: u32 = @intCast(frame.width[1]);
        const height: u32 = @intCast(frame.height);
        if (width > 16384 or height > 16384 or @as(u64, width) * height > self.options.max_pixels) return error.ResourceLimitExceeded;
        const s = self.sequence;
        const op = s.operating_points[self.options.operating_point];
        const depth: u8 = @intCast(8 + 2 * s.hbd);
        const config = [4]u8{ 0x81, (s.profile << 5) | ((op.major_level - 2) * 4 + op.minor_level), (op.tier << 7) | (@as(u8, @intFromBool(depth > 8)) << 6) | (@as(u8, @intFromBool(depth == 12)) << 5) |
            (@as(u8, @intFromBool(s.layout == 0)) << 4) | (@as(u8, @intFromBool(s.layout != 3)) << 3) |
            (@as(u8, @intFromBool(s.layout <= 1)) << 2) | @as(u8, @intCast(s.chr)), 0 };
        return .{ .width = width, .height = height, .bit_depth = depth, .chroma_format = @intCast(s.layout), .profile = s.profile, .av1_config = config, .apple_compatible = s.num_operating_points == 1 and s.film_grain_present == 0, .color = .{ .full_range = s.color_range != 0, .primaries = @intCast(s.pri), .transfer = @intCast(s.trc), .matrix = @intCast(s.mtrx) } };
    }
    fn refresh(self: *Session, frame: grammar.Dav1dFrameHeader, flags: u8) void {
        for (0..8) |i| if (flags & (@as(u8, 1) << @intCast(i)) != 0) {
            self.references[i] = frame;
            self.context.refs[i].p.p.frame_hdr = &self.references[i];
            self.context.refs[i].p.showable = frame.showable_frame;
        };
    }
    fn tiles(self: *Session, bits: *grammar.GetBits, context: *anyopaque, accept: *const fn (*anyopaque, Geometry) anyerror!void) !void {
        const frame = self.current;
        const count = @as(usize, frame.tiling.cols) * frame.tiling.rows;
        if (count == 0 or count > 4096) return error.InvalidVideoData;
        const explicit = count > 1 and grammar.antfly_av1_dav1d_get_bit(bits) != 0;
        const n_bits: c_int = @as(c_int, frame.tiling.log2_cols) + frame.tiling.log2_rows;
        const start: usize = if (explicit) grammar.antfly_av1_dav1d_get_bits(bits, n_bits) else 0;
        const end: usize = if (explicit) grammar.antfly_av1_dav1d_get_bits(bits, n_bits) else count - 1;
        if (bits.@"error" != 0 or start != self.next_tile or start > end or end >= count) return error.InvalidVideoData;
        grammar.dav1d_bytealign_get_bits(bits);
        var position: usize = 0;
        const remaining = @intFromPtr(bits.ptr_end) - @intFromPtr(bits.ptr);
        const bytes = bits.ptr[0..remaining];
        for (start..end) |_| {
            const size_bytes = frame.tiling.n_bytes;
            if (size_bytes < 1 or size_bytes > 4 or size_bytes > bytes.len - position) return error.InvalidVideoData;
            var size: usize = 0;
            for (0..size_bytes) |i| size |= @as(usize, bytes[position + i]) << @intCast(i * 8);
            position += size_bytes;
            size = std.math.add(usize, size, 1) catch return error.ResourceLimitExceeded;
            if (size > bytes.len - position) return error.InvalidVideoData;
            position += size;
        }
        if (position == bytes.len) return error.InvalidVideoData;
        self.next_tile = end + 1;
        if (self.next_tile == count) {
            self.refresh(frame, frame.refresh_frame_flags);
            self.pending = false;
            if (frame.show_frame != 0 and (self.options.all_layers or frame.spatial_id >= self.context.max_spatial_id)) try accept(context, try self.pictureGeometry(frame));
        }
    }
};
fn initBits(bytes: []const u8) grammar.GetBits {
    var bits: grammar.GetBits = .{};
    grammar.antfly_av1_dav1d_init_get_bits(&bits, bytes.ptr, bytes.len);
    return bits;
}
fn leb(bytes: []const u8, position: *usize) !usize {
    var size: u64 = 0;
    for (0..8) |i| {
        if (position.* == bytes.len) return error.InvalidVideoData;
        const byte = bytes[position.*];
        position.* += 1;
        size |= @as(u64, byte & 127) << @intCast(i * 7);
        if (byte & 128 == 0) return std.math.cast(usize, size) orelse error.ResourceLimitExceeded;
    }
    return error.InvalidVideoData;
}

fn sameFrameHeader(a: grammar.Dav1dFrameHeader, b: grammar.Dav1dFrameHeader) bool {
    inline for (@typeInfo(grammar.Dav1dFrameHeader).@"struct".field_names) |name| {
        if (comptime std.mem.eql(u8, name, "gmv")) {
            for (a.gmv, b.gmv) |first, second| {
                if (first.type != second.type or !std.meta.eql(first.matrix, second.matrix) or !std.meta.eql(first.u.abcd, second.u.abcd)) return false;
            }
        } else if (!std.meta.eql(@field(a, name), @field(b, name))) return false;
    }
    return true;
}

test "AV1 header metadata matches reconstruction across depth chroma grain" {
    const a = std.testing.allocator;
    const decoder = @import("decoder.zig");
    const Capture = struct {
        a: std.mem.Allocator,
        values: std.ArrayList(Geometry) = .empty,
        fn accept(raw: *anyopaque, geometry: Geometry) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.values.append(self.a, geometry);
        }
    };
    inline for (.{ "../../testdata/vpx-av1/av1.ivf", "../../testdata/vpx-av1/av1-10.ivf", "../../testdata/vpx-av1/av1-grain.ivf", "../../testdata/vpx-av1/av1-odd.ivf", "../../testdata/av1-extended/av1-c0-8.ivf", "../../testdata/av1-extended/av1-c0-12.ivf", "../../testdata/av1-extended/av1-c1-12.ivf", "../../testdata/av1-extended/av1-c2-8.ivf", "../../testdata/av1-extended/av1-c2-10.ivf", "../../testdata/av1-extended/av1-c2-12.ivf", "../../testdata/av1-extended/av1-c3-8.ivf", "../../testdata/av1-extended/av1-c3-10.ivf", "../../testdata/av1-extended/av1-c3-12.ivf", "../../testdata/av1-extended/av1-svc.ivf" }) |path| {
        const bytes = @embedFile(path);
        const headers = try Session.create(a, .{ .max_pixels = 16 * 1024 * 1024 });
        defer headers.deinit();
        const pixels = try decoder.Session.create(a, .av1, .{});
        defer pixels.destroy();
        var capture = Capture{ .a = a };
        defer capture.values.deinit(a);
        var position: usize = 32;
        while (position < bytes.len) {
            try std.testing.expect(bytes.len - position >= 12);
            const size = std.mem.readInt(u32, bytes[position..][0..4], .little);
            position += 12;
            const payload = bytes[position..][0..size];
            position += size;
            capture.values.clearRetainingCapacity();
            try headers.walk(payload, &capture, Capture.accept);
            var batch = try pixels.decode(payload, 0, null);
            defer batch.deinit();
            try std.testing.expectEqual(batch.frames.len, capture.values.items.len);
            for (batch.frames, capture.values.items) |frame, metadata| {
                try std.testing.expectEqual(frame.width, metadata.width);
                try std.testing.expectEqual(frame.height, metadata.height);
            }
        }
        try headers.finish();
    }
}

test "AV1 header metadata preserves selected scalable operating-point timelines" {
    const a = std.testing.allocator;
    const decoder = @import("decoder.zig");
    const Golden = struct { cases: []const struct { operating_point: u5, all_layers: bool, frames: []const struct { width: u32, height: u32 } } };
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("../../testdata/av1-extended/layers.json"), .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    const Capture = struct {
        a: std.mem.Allocator,
        values: std.ArrayList(Geometry) = .empty,
        fn accept(raw: *anyopaque, geometry: Geometry) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.values.append(self.a, geometry);
        }
    };
    const bytes = @embedFile("../../testdata/av1-extended/av1-svc.ivf");
    for (golden.value.cases) |case| {
        const headers = try Session.create(a, .{ .max_pixels = 16 * 1024 * 1024, .operating_point = case.operating_point, .all_layers = case.all_layers });
        defer headers.deinit();
        const pixels = try decoder.Session.create(a, .av1, .{ .av1_operating_point = case.operating_point, .av1_all_layers = case.all_layers });
        defer pixels.destroy();
        var capture = Capture{ .a = a };
        defer capture.values.deinit(a);
        var position: usize = 32;
        var emitted: usize = 0;
        while (position < bytes.len) {
            const size = std.mem.readInt(u32, bytes[position..][0..4], .little);
            position += 12;
            const payload = bytes[position..][0..size];
            position += size;
            capture.values.clearRetainingCapacity();
            try headers.walk(payload, &capture, Capture.accept);
            var batch = try pixels.decode(payload, 0, null);
            defer batch.deinit();
            try std.testing.expectEqual(batch.frames.len, capture.values.items.len);
            for (batch.frames, capture.values.items) |frame, metadata| {
                try std.testing.expect(emitted < case.frames.len);
                const expected = case.frames[emitted];
                try std.testing.expectEqual(expected.width, metadata.width);
                try std.testing.expectEqual(expected.height, metadata.height);
                try std.testing.expectEqual(frame.width, metadata.width);
                try std.testing.expectEqual(frame.height, metadata.height);
                emitted += 1;
            }
        }
        try headers.finish();
        try std.testing.expectEqual(case.frames.len, emitted);
    }
}

fn fuzzSeed(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(1_000_000);
        const size = std.mem.readInt(u32, bytes[32..36], .little);
        var encoded: [4 + size]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], size, .little);
        @memcpy(encoded[4..], bytes[44..][0..size]);
        const frozen = encoded;
        break :blk &frozen;
    };
}
test "video fuzz AV1 metadata headers and reference cleanup" {
    const Harness = struct {
        fn accept(_: *anyopaque, geometry: Geometry) !void {
            try std.testing.expect(geometry.width > 0 and geometry.height > 0);
            try std.testing.expect(@as(u64, geometry.width) * geometry.height <= 512 * 512);
        }
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17FA101);
            const session = try Session.create(std.testing.allocator, .{ .max_pixels = 512 * 512 });
            defer session.deinit();
            var unused: u8 = 0;
            session.walk(bytes[0..length], &unused, accept) catch return;
            session.finish() catch return;
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{ fuzzSeed(@embedFile("../../testdata/vpx-av1/av1.ivf")), fuzzSeed(@embedFile("../../testdata/vpx-av1/av1-grain.ivf")), fuzzSeed(@embedFile("../../testdata/av1-extended/av1-c0-12.ivf")), fuzzSeed(@embedFile("../../testdata/av1-extended/av1-c3-12.ivf")), fuzzSeed(@embedFile("../../testdata/av1-extended/av1-svc.ivf")) } });
}

test "AV1 metadata fuzz seeds emit displayed pictures through Smith" {
    const Capture = struct {
        count: usize = 0,
        fn accept(raw: *anyopaque, _: Geometry) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    inline for (.{ @embedFile("../../testdata/vpx-av1/av1.ivf"), @embedFile("../../testdata/vpx-av1/av1-grain.ivf"), @embedFile("../../testdata/av1-extended/av1-c0-12.ivf"), @embedFile("../../testdata/av1-extended/av1-c3-12.ivf"), @embedFile("../../testdata/av1-extended/av1-svc.ivf") }) |fixture| {
        const encoded = fuzzSeed(fixture);
        var smith = std.testing.Smith{ .in = encoded };
        var bytes: [128 * 1024]u8 = undefined;
        const length = smith.sliceWithHash(&bytes, 0xA17FA101);
        try std.testing.expectEqual(encoded.len - 4, length);
        const headers = try Session.create(std.testing.allocator, .{ .max_pixels = 512 * 512 });
        defer headers.deinit();
        var capture = Capture{};
        try headers.walk(bytes[0..length], &capture, Capture.accept);
        try headers.finish();
        try std.testing.expect(capture.count > 0);
    }
}
