// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const g711 = @import("g711.zig");

const tone_caf_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.caf");

pub const Codec = enum {
    alac,
};

pub const DemuxedAudio = struct {
    codec: Codec,
    sample_rate: u32,
    channels: u16,
    decoder_config: []const u8,
    access_units: [][]const u8,
    valid_frames: u64,
    priming_frames: u32,
    remainder_frames: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DemuxedAudio) void {
        self.allocator.free(self.access_units);
    }
};

pub const DecodedInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DecodedInterleaved) void {
        self.allocator.free(self.samples);
    }
};

const LpcmFormat = struct {
    sample_rate: u32,
    channels: u8,
    bits_per_sample: u16,
    bytes_per_packet: u32,
    frames_per_packet: u32,
    endian: std.builtin.Endian,
    encoding: SampleEncoding,
};

const SampleEncoding = enum {
    signed_int,
    unsigned_int,
    float,
    ulaw,
    alaw,
};

const PacketTable = struct {
    packet_count: usize,
    valid_frames: u64,
    priming_frames: u32,
    remainder_frames: u32,
    packet_sizes: []u32,
};

const lpcm_flag_is_float = 1 << 0;
const lpcm_flag_is_big_endian = 1 << 1;
const lpcm_flag_is_signed_integer = 1 << 2;
const lpcm_flag_is_packed = 1 << 3;
const lpcm_flag_is_non_interleaved = 1 << 5;

pub fn decodeInterleaved(allocator: std.mem.Allocator, bytes: []const u8) !DecodedInterleaved {
    if (bytes.len < 8 or !std.mem.eql(u8, bytes[0..4], "caff")) return error.UnsupportedAudioFormat;

    var format: ?LpcmFormat = null;
    var sample_data: []const u8 = &.{};

    var cursor: usize = 8;
    while (cursor + 12 <= bytes.len) {
        const chunk_type = bytes[cursor .. cursor + 4];
        const chunk_size = readU64(bytes[cursor + 4 ..][0..8]);
        cursor += 12;
        const payload_len = if (chunk_size == std.math.maxInt(u64))
            bytes.len - cursor
        else
            std.math.cast(usize, chunk_size) orelse return error.UnsupportedAudioFormat;
        if (cursor + payload_len > bytes.len) return error.UnsupportedAudioFormat;
        const payload = bytes[cursor .. cursor + payload_len];

        if (std.mem.eql(u8, chunk_type, "desc")) {
            format = try parseDecodeDescription(payload);
        } else if (std.mem.eql(u8, chunk_type, "data")) {
            if (payload.len < 4) return error.UnsupportedAudioFormat;
            sample_data = payload[4..];
        }

        cursor += payload_len;
    }

    const resolved_format = format orelse return error.UnsupportedAudioFormat;
    if (sample_data.len == 0) return error.UnsupportedAudioFormat;
    return decodeLpcmPayload(allocator, resolved_format, sample_data);
}

pub fn demux(allocator: std.mem.Allocator, bytes: []const u8) !DemuxedAudio {
    if (bytes.len < 8 or !std.mem.eql(u8, bytes[0..4], "caff")) return error.UnsupportedAudioFormat;

    var codec: ?Codec = null;
    var sample_rate: u32 = 0;
    var channels: u16 = 0;
    var decoder_config: []const u8 = &.{};
    var data_payload: []const u8 = &.{};
    var packet_table: ?PacketTable = null;
    defer if (packet_table) |table| allocator.free(table.packet_sizes);

    var cursor: usize = 8;
    while (cursor + 12 <= bytes.len) {
        const chunk_type = bytes[cursor .. cursor + 4];
        const chunk_size = readU64(bytes[cursor + 4 ..][0..8]);
        cursor += 12;
        const payload_len = if (chunk_size == std.math.maxInt(u64))
            bytes.len - cursor
        else
            std.math.cast(usize, chunk_size) orelse return error.UnsupportedAudioFormat;
        if (cursor + payload_len > bytes.len) return error.UnsupportedAudioFormat;
        const payload = bytes[cursor .. cursor + payload_len];

        if (std.mem.eql(u8, chunk_type, "desc")) {
            if (payload.len < 32) return error.UnsupportedAudioFormat;
            const format_id = payload[8..12];
            if (!std.mem.eql(u8, format_id, "alac")) return error.UnsupportedAudioFormat;
            codec = .alac;
            sample_rate = readF64AsU32(payload[0..8]) catch return error.UnsupportedAudioFormat;
            channels = @intCast(readU32(payload[24..28]));
        } else if (std.mem.eql(u8, chunk_type, "kuki")) {
            decoder_config = findAlacCookie(payload) orelse return error.UnsupportedAudioFormat;
        } else if (std.mem.eql(u8, chunk_type, "data")) {
            if (payload.len < 4) return error.UnsupportedAudioFormat;
            data_payload = payload[4..];
        } else if (std.mem.eql(u8, chunk_type, "pakt")) {
            packet_table = try parsePacketTable(allocator, payload);
        }

        cursor += payload_len;
    }

    const resolved_codec = codec orelse return error.UnsupportedAudioFormat;
    if (sample_rate == 0 or channels == 0 or decoder_config.len == 0 or data_payload.len == 0) {
        return error.UnsupportedAudioFormat;
    }
    const table = packet_table orelse return error.UnsupportedAudioFormat;
    if (table.valid_frames == 0) return error.UnsupportedAudioFormat;

    const access_units = try allocator.alloc([]const u8, table.packet_count);
    errdefer allocator.free(access_units);

    var data_cursor: usize = 0;
    for (table.packet_sizes, 0..) |packet_size, i| {
        const packet_len = std.math.cast(usize, packet_size) orelse return error.UnsupportedAudioFormat;
        if (data_cursor + packet_len > data_payload.len) return error.UnsupportedAudioFormat;
        access_units[i] = data_payload[data_cursor .. data_cursor + packet_len];
        data_cursor += packet_len;
    }
    if (data_cursor != data_payload.len) return error.UnsupportedAudioFormat;

    return .{
        .codec = resolved_codec,
        .sample_rate = sample_rate,
        .channels = channels,
        .decoder_config = decoder_config,
        .access_units = access_units,
        .valid_frames = table.valid_frames,
        .priming_frames = table.priming_frames,
        .remainder_frames = table.remainder_frames,
        .allocator = allocator,
    };
}

fn parseDecodeDescription(payload: []const u8) !LpcmFormat {
    if (payload.len < 32) return error.UnsupportedAudioFormat;

    const sample_rate = readF64AsU32(payload[0..8]) catch return error.UnsupportedAudioFormat;
    const format_id = payload[8..12];
    const flags = readU32(payload[12..16]);
    const bytes_per_packet = readU32(payload[16..20]);
    const frames_per_packet = readU32(payload[20..24]);
    const channel_count = readU32(payload[24..28]);
    const bits_per_channel = readU32(payload[28..32]);

    if (std.mem.eql(u8, format_id, "ulaw") or std.mem.eql(u8, format_id, "alaw")) {
        if (frames_per_packet != 1) return error.UnsupportedAudioFormat;
        if (channel_count == 0 or channel_count > std.math.maxInt(u8)) return error.UnsupportedAudioFormat;
        if (bits_per_channel != 8) return error.UnsupportedAudioFormat;
        if (bytes_per_packet != channel_count) return error.UnsupportedAudioFormat;
        return .{
            .sample_rate = sample_rate,
            .channels = @intCast(channel_count),
            .bits_per_sample = 8,
            .bytes_per_packet = bytes_per_packet,
            .frames_per_packet = frames_per_packet,
            .endian = .big,
            .encoding = if (std.mem.eql(u8, format_id, "ulaw")) .ulaw else .alaw,
        };
    }

    if (!std.mem.eql(u8, format_id, "lpcm")) return error.UnsupportedAudioFormat;
    if ((flags & lpcm_flag_is_non_interleaved) != 0) return error.UnsupportedAudioFormat;
    if ((flags & lpcm_flag_is_packed) == 0) return error.UnsupportedAudioFormat;
    if (frames_per_packet != 1) return error.UnsupportedAudioFormat;
    if (channel_count == 0 or channel_count > std.math.maxInt(u8)) return error.UnsupportedAudioFormat;
    const bits_per_sample = std.math.cast(u16, bits_per_channel) orelse return error.UnsupportedAudioFormat;
    const bytes_per_sample = switch (bits_per_sample) {
        8 => @as(u32, 1),
        16 => 2,
        24 => 3,
        32 => 4,
        64 => 8,
        else => return error.UnsupportedAudioFormat,
    };
    const expected_bytes_per_packet = try std.math.mul(u32, bytes_per_sample, channel_count);
    if (bytes_per_packet != expected_bytes_per_packet) return error.UnsupportedAudioFormat;

    return .{
        .sample_rate = sample_rate,
        .channels = @intCast(channel_count),
        .bits_per_sample = bits_per_sample,
        .bytes_per_packet = bytes_per_packet,
        .frames_per_packet = frames_per_packet,
        .endian = if ((flags & lpcm_flag_is_big_endian) != 0) .big else .little,
        .encoding = if ((flags & lpcm_flag_is_float) != 0)
            .float
        else if ((flags & lpcm_flag_is_signed_integer) != 0)
            .signed_int
        else
            .unsigned_int,
    };
}

fn decodeLpcmPayload(allocator: std.mem.Allocator, format: LpcmFormat, sample_data: []const u8) !DecodedInterleaved {
    const bytes_per_sample = switch (format.bits_per_sample) {
        8 => @as(usize, 1),
        16 => 2,
        24 => 3,
        32 => 4,
        64 => 8,
        else => return error.UnsupportedAudioFormat,
    };
    const frame_bytes = try std.math.mul(usize, bytes_per_sample, format.channels);
    if (frame_bytes == 0 or sample_data.len % frame_bytes != 0) return error.UnsupportedAudioFormat;
    const sample_count = sample_data.len / bytes_per_sample;

    const samples = try allocator.alloc(f32, sample_count);
    errdefer allocator.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * bytes_per_sample;
        sample.* = switch (format.encoding) {
            .signed_int => switch (format.bits_per_sample) {
                8 => blk: {
                    const raw: i8 = @bitCast(sample_data[offset]);
                    break :blk @as(f32, @floatFromInt(raw)) / 128.0;
                },
                16 => blk: {
                    const raw = std.mem.readInt(i16, sample_data[offset..][0..2], format.endian);
                    break :blk @as(f32, @floatFromInt(raw)) / 32768.0;
                },
                24 => blk: {
                    const raw = readI24(sample_data[offset..][0..3], format.endian);
                    break :blk @as(f32, @floatFromInt(raw)) / 8388608.0;
                },
                32 => blk: {
                    const raw = std.mem.readInt(i32, sample_data[offset..][0..4], format.endian);
                    break :blk @as(f32, @floatFromInt(raw)) / 2147483648.0;
                },
                64 => blk: {
                    const raw = std.mem.readInt(i64, sample_data[offset..][0..8], format.endian);
                    break :blk @floatCast(@as(f64, @floatFromInt(raw)) / 9223372036854775808.0);
                },
                else => return error.UnsupportedAudioFormat,
            },
            .unsigned_int => switch (format.bits_per_sample) {
                8 => (@as(f32, @floatFromInt(sample_data[offset])) - 128.0) / 128.0,
                16 => blk: {
                    const raw = std.mem.readInt(u16, sample_data[offset..][0..2], format.endian);
                    break :blk (@as(f32, @floatFromInt(raw)) - 32768.0) / 32768.0;
                },
                24 => blk: {
                    const raw = readU24(sample_data[offset..][0..3], format.endian);
                    break :blk (@as(f32, @floatFromInt(raw)) - 8388608.0) / 8388608.0;
                },
                32 => blk: {
                    const raw = std.mem.readInt(u32, sample_data[offset..][0..4], format.endian);
                    break :blk @as(f32, @floatCast((@as(f64, @floatFromInt(raw)) - 2147483648.0) / 2147483648.0));
                },
                64 => blk: {
                    const raw = std.mem.readInt(u64, sample_data[offset..][0..8], format.endian);
                    break :blk @floatCast((@as(f64, @floatFromInt(raw)) - 9223372036854775808.0) / 9223372036854775808.0);
                },
                else => return error.UnsupportedAudioFormat,
            },
            .float => switch (format.bits_per_sample) {
                32 => @bitCast(std.mem.readInt(u32, sample_data[offset..][0..4], format.endian)),
                64 => @floatCast(@as(f64, @bitCast(std.mem.readInt(u64, sample_data[offset..][0..8], format.endian)))),
                else => return error.UnsupportedAudioFormat,
            },
            .ulaw => switch (format.bits_per_sample) {
                8 => @as(f32, @floatFromInt(g711.decodeMuLaw(sample_data[offset]))) / 32768.0,
                else => return error.UnsupportedAudioFormat,
            },
            .alaw => switch (format.bits_per_sample) {
                8 => @as(f32, @floatFromInt(g711.decodeALaw(sample_data[offset]))) / 32768.0,
                else => return error.UnsupportedAudioFormat,
            },
        };
    }

    return .{
        .samples = samples,
        .sample_rate = format.sample_rate,
        .channels = format.channels,
        .allocator = allocator,
    };
}

fn parsePacketTable(allocator: std.mem.Allocator, payload: []const u8) !PacketTable {
    if (payload.len < 24) return error.UnsupportedAudioFormat;
    const packet_count_u64 = readU64(payload[0..8]);
    const packet_count = std.math.cast(usize, packet_count_u64) orelse return error.UnsupportedAudioFormat;
    const valid_frames = readU64(payload[8..16]);
    const priming_frames = readU32(payload[16..20]);
    const remainder_frames = readU32(payload[20..24]);

    const packet_sizes = try allocator.alloc(u32, packet_count);
    errdefer allocator.free(packet_sizes);

    var cursor: usize = 24;
    for (packet_sizes) |*packet_size| {
        packet_size.* = try readVariableLengthInt(payload, &cursor);
    }
    if (cursor != payload.len) return error.UnsupportedAudioFormat;

    return .{
        .packet_count = packet_count,
        .valid_frames = valid_frames,
        .priming_frames = priming_frames,
        .remainder_frames = remainder_frames,
        .packet_sizes = packet_sizes,
    };
}

fn readVariableLengthInt(payload: []const u8, cursor: *usize) !u32 {
    var value: u64 = 0;
    var groups: usize = 0;
    while (cursor.* < payload.len and groups < 10) : (groups += 1) {
        const b = payload[cursor.*];
        cursor.* += 1;
        value = (value << 7) | (b & 0x7f);
        if ((b & 0x80) == 0) {
            return std.math.cast(u32, value) orelse error.UnsupportedAudioFormat;
        }
    }
    return error.UnsupportedAudioFormat;
}

fn findAlacCookie(payload: []const u8) ?[]const u8 {
    var cursor: usize = 0;
    while (cursor + 8 <= payload.len) {
        const atom_size = readU32(payload[cursor..][0..4]);
        if (atom_size < 8) return null;
        const atom_len = std.math.cast(usize, atom_size) orelse return null;
        if (cursor + atom_len > payload.len) return null;
        const atom_type = payload[cursor + 4 .. cursor + 8];
        if (std.mem.eql(u8, atom_type, "alac")) {
            return payload[cursor .. cursor + atom_len];
        }
        cursor += atom_len;
    }
    return null;
}

fn readF64AsU32(bytes: []const u8) !u32 {
    const bits = readU64(bytes);
    const value: f64 = @bitCast(bits);
    if (!std.math.isFinite(value) or value <= 0) return error.UnsupportedAudioFormat;
    return std.math.cast(u32, @as(i64, @intFromFloat(@round(value)))) orelse error.UnsupportedAudioFormat;
}

fn readU32(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .big);
}

fn readU64(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .big);
}

fn readU24(bytes: *const [3]u8, endian: std.builtin.Endian) u32 {
    return switch (endian) {
        .big => (@as(u32, bytes[0]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[2],
        .little => (@as(u32, bytes[2]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[0],
    };
}

fn readI24(bytes: *const [3]u8, endian: std.builtin.Endian) i32 {
    const unsigned = readU24(bytes, endian);
    return @as(i32, @bitCast(unsigned << 8)) >> 8;
}

test "demux extracts checked-in caf alac fixture" {
    var demuxed = try demux(std.testing.allocator, tone_caf_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(Codec.alac, demuxed.codec);
    try std.testing.expectEqual(@as(u32, 16000), demuxed.sample_rate);
    try std.testing.expectEqual(@as(u16, 2), demuxed.channels);
    try std.testing.expectEqual(@as(u64, 16000), demuxed.valid_frames);
    try std.testing.expectEqual(@as(u32, 0), demuxed.priming_frames);
    try std.testing.expectEqual(@as(u32, 384), demuxed.remainder_frames);
    try std.testing.expectEqual(@as(usize, 4), demuxed.access_units.len);
    try std.testing.expectEqual(@as(usize, 1904), demuxed.access_units[0].len);
    try std.testing.expectEqualSlices(u8, &.{
        0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
        0x00, 0x10, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x04, 0x00, 0x07, 0xd0, 0x00,
        0x00, 0x00, 0x3e, 0x80,
    }, demuxed.decoder_config);
}

test "demux caf alac fixture accepts data chunk sized to end of file" {
    const desc_payload = [_]u8{
        0x40, 0xcf, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
        'a',  'l',  'a',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x10,
    };
    const kuki_payload = [_]u8{
        0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
        0x00, 0x10, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x04, 0x00, 0x07, 0xd0, 0x00,
        0x00, 0x00, 0x3e, 0x80,
    };
    const pakt_payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x03,
    };
    const data_payload = [_]u8{
        0x00, 0x00, 0x00, 0x00,
        0x01, 0x02, 0x03,
    };
    const synthetic =
        [_]u8{ 'c', 'a', 'f', 'f', 0x00, 0x01, 0x00, 0x00 } ++
        [_]u8{ 'd', 'e', 's', 'c', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20 } ++
        desc_payload ++
        [_]u8{ 'k', 'u', 'k', 'i', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24 } ++
        kuki_payload ++
        [_]u8{ 'p', 'a', 'k', 't', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19 } ++
        pakt_payload ++
        [_]u8{ 'd', 'a', 't', 'a', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff } ++
        data_payload;

    var demuxed = try demux(std.testing.allocator, &synthetic);
    defer demuxed.deinit();

    try std.testing.expectEqual(Codec.alac, demuxed.codec);
    try std.testing.expectEqual(@as(u32, 16000), demuxed.sample_rate);
    try std.testing.expectEqual(@as(u16, 2), demuxed.channels);
    try std.testing.expectEqual(@as(u64, 1024), demuxed.valid_frames);
    try std.testing.expectEqual(@as(u32, 0), demuxed.priming_frames);
    try std.testing.expectEqual(@as(u32, 0), demuxed.remainder_frames);
    try std.testing.expectEqual(@as(usize, 1), demuxed.access_units.len);
    try std.testing.expectEqual(@as(usize, 3), demuxed.access_units[0].len);
    try std.testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03 }, demuxed.access_units[0]);
}

test "decode synthetic caf lpcm pcm16 and float32 fixtures" {
    const pcm16 = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x10, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x40,
    };
    var decoded_pcm16 = try decodeInterleaved(std.testing.allocator, &pcm16);
    defer decoded_pcm16.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_pcm16.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded_pcm16.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_pcm16.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm16.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm16.samples[1], 1e-6);

    const fl32 = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x20, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0x00, 0xbf, 0x80, 0x00, 0x00,
        0x3f, 0x00, 0x00, 0x00,
    };
    var decoded_fl32 = try decodeInterleaved(std.testing.allocator, &fl32);
    defer decoded_fl32.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_fl32.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded_fl32.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_fl32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_fl32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_fl32.samples[1], 1e-6);
}

test "decode synthetic caf wide lpcm fixtures" {
    const unsigned8 = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x08, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xc0,
    };
    var decoded_unsigned8 = try decodeInterleaved(std.testing.allocator, &unsigned8);
    defer decoded_unsigned8.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_unsigned8.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_unsigned8.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_unsigned8.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_unsigned8.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_unsigned8.samples[1], 1e-6);

    const signed24_be = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x18, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
        0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x40,
        0x00, 0x00,
    };
    var decoded_signed24 = try decodeInterleaved(std.testing.allocator, &signed24_be);
    defer decoded_signed24.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_signed24.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_signed24.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_signed24.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_signed24.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_signed24.samples[1], 1e-6);

    const unsigned32_be = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x20, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xc0, 0x00, 0x00, 0x00,
    };
    var decoded_unsigned32 = try decodeInterleaved(std.testing.allocator, &unsigned32_be);
    defer decoded_unsigned32.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_unsigned32.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_unsigned32.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_unsigned32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_unsigned32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_unsigned32.samples[1], 1e-6);

    const signed64_be = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x40, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
        0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    var decoded_signed64 = try decodeInterleaved(std.testing.allocator, &signed64_be);
    defer decoded_signed64.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_signed64.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_signed64.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_signed64.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_signed64.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_signed64.samples[1], 1e-6);

    const unsigned64_be = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x40, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    var decoded_unsigned64 = try decodeInterleaved(std.testing.allocator, &unsigned64_be);
    defer decoded_unsigned64.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_unsigned64.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_unsigned64.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_unsigned64.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_unsigned64.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_unsigned64.samples[1], 1e-6);

    const float64_be = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
        0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x40, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
        0x00, 0x00, 0x00, 0x00, 0xbf, 0xf0, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x3f, 0xe0, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    var decoded_float64 = try decodeInterleaved(std.testing.allocator, &float64_be);
    defer decoded_float64.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_float64.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_float64.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_float64.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_float64.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_float64.samples[1], 1e-6);
}

test "decode synthetic caf g711 fixtures" {
    const ulaw = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xbf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'u',  'l',  'a',  'w',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x08, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0x7f,
    };
    var decoded_ulaw = try decodeInterleaved(std.testing.allocator, &ulaw);
    defer decoded_ulaw.deinit();
    try std.testing.expectEqual(@as(u32, 8000), decoded_ulaw.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded_ulaw.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_ulaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32124.0 / 32768.0), decoded_ulaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32124.0 / 32768.0), decoded_ulaw.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_ulaw.samples[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_ulaw.samples[3], 0.001);

    const alaw = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x20, 0x40, 0xbf, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 'a',  'l',  'a',  'w',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x08, 'd',  'a',  't',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x2a, 0xaa, 0xd5, 0x55,
    };
    var decoded_alaw = try decodeInterleaved(std.testing.allocator, &alaw);
    defer decoded_alaw.deinit();
    try std.testing.expectEqual(@as(u32, 8000), decoded_alaw.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded_alaw.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_alaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32256.0 / 32768.0), decoded_alaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32256.0 / 32768.0), decoded_alaw.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[3], 0.001);
}
