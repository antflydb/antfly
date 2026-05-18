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

const tone_aiff_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.aiff");

pub const DecodedInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DecodedInterleaved) void {
        self.allocator.free(self.samples);
    }
};

const Parsed = struct {
    sample_rate: u32,
    channels: u8,
    bits_per_sample: u16,
    endian: std.builtin.Endian,
    encoding: SampleEncoding,
    sample_frames: usize,
    sound_data: []const u8,
};

const SampleEncoding = enum {
    signed_int,
    unsigned_int,
    float,
    ulaw,
    alaw,
};

pub fn decodeInterleaved(allocator: std.mem.Allocator, bytes: []const u8) !DecodedInterleaved {
    const parsed = try parse(bytes);
    const bytes_per_sample = switch (parsed.bits_per_sample) {
        8 => @as(usize, 1),
        16 => 2,
        24 => 3,
        32 => 4,
        64 => 8,
        else => return error.UnsupportedAudioFormat,
    };

    const expected_samples = try std.math.mul(usize, parsed.sample_frames, parsed.channels);
    const expected_bytes = try std.math.mul(usize, expected_samples, bytes_per_sample);
    if (parsed.sound_data.len < expected_bytes) return error.UnsupportedAudioFormat;

    const samples = try allocator.alloc(f32, expected_samples);
    errdefer allocator.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * bytes_per_sample;
        sample.* = switch (parsed.encoding) {
            .signed_int => switch (parsed.bits_per_sample) {
                8 => blk: {
                    const raw: i8 = @bitCast(parsed.sound_data[offset]);
                    break :blk @as(f32, @floatFromInt(raw)) / 128.0;
                },
                16 => blk: {
                    const raw = std.mem.readInt(i16, parsed.sound_data[offset..][0..2], parsed.endian);
                    break :blk @as(f32, @floatFromInt(raw)) / 32768.0;
                },
                24 => blk: {
                    const raw = readI24(parsed.sound_data[offset..][0..3], parsed.endian);
                    break :blk @as(f32, @floatFromInt(raw)) / 8388608.0;
                },
                32 => blk: {
                    const raw = std.mem.readInt(i32, parsed.sound_data[offset..][0..4], parsed.endian);
                    break :blk @as(f32, @floatFromInt(raw)) / 2147483648.0;
                },
                64 => blk: {
                    const raw = std.mem.readInt(i64, parsed.sound_data[offset..][0..8], parsed.endian);
                    break :blk @floatCast(@as(f64, @floatFromInt(raw)) / 9223372036854775808.0);
                },
                else => return error.UnsupportedAudioFormat,
            },
            .unsigned_int => switch (parsed.bits_per_sample) {
                8 => (@as(f32, @floatFromInt(parsed.sound_data[offset])) - 128.0) / 128.0,
                else => return error.UnsupportedAudioFormat,
            },
            .float => switch (parsed.bits_per_sample) {
                32 => @bitCast(std.mem.readInt(u32, parsed.sound_data[offset..][0..4], parsed.endian)),
                64 => @floatCast(@as(f64, @bitCast(std.mem.readInt(u64, parsed.sound_data[offset..][0..8], parsed.endian)))),
                else => return error.UnsupportedAudioFormat,
            },
            .ulaw => switch (parsed.bits_per_sample) {
                8 => @as(f32, @floatFromInt(g711.decodeMuLaw(parsed.sound_data[offset]))) / 32768.0,
                else => return error.UnsupportedAudioFormat,
            },
            .alaw => switch (parsed.bits_per_sample) {
                8 => @as(f32, @floatFromInt(g711.decodeALaw(parsed.sound_data[offset]))) / 32768.0,
                else => return error.UnsupportedAudioFormat,
            },
        };
    }

    return .{
        .samples = samples,
        .sample_rate = parsed.sample_rate,
        .channels = parsed.channels,
        .allocator = allocator,
    };
}

fn parse(bytes: []const u8) !Parsed {
    if (bytes.len < 12 or !std.mem.eql(u8, bytes[0..4], "FORM")) return error.UnsupportedAudioFormat;
    const is_aiff = std.mem.eql(u8, bytes[8..12], "AIFF");
    const is_aifc = std.mem.eql(u8, bytes[8..12], "AIFC");
    if (!is_aiff and !is_aifc) return error.UnsupportedAudioFormat;

    var channels: u8 = 0;
    var sample_frames: usize = 0;
    var bits_per_sample: u16 = 0;
    var sample_rate: u32 = 0;
    var endian: std.builtin.Endian = .big;
    var encoding: SampleEncoding = .signed_int;
    var sound_data: []const u8 = &.{};

    var cursor: usize = 12;
    while (cursor + 8 <= bytes.len) {
        const chunk_id = bytes[cursor .. cursor + 4];
        const chunk_size_u32 = std.mem.readInt(u32, bytes[cursor + 4 ..][0..4], .big);
        const chunk_size = std.math.cast(usize, chunk_size_u32) orelse return error.UnsupportedAudioFormat;
        cursor += 8;
        if (cursor + chunk_size > bytes.len) return error.UnsupportedAudioFormat;
        const payload = bytes[cursor .. cursor + chunk_size];

        if (std.mem.eql(u8, chunk_id, "COMM")) {
            if (payload.len < 18 or (is_aifc and payload.len < 22)) return error.UnsupportedAudioFormat;
            const channel_count = std.mem.readInt(u16, payload[0..2], .big);
            if (channel_count == 0 or channel_count > std.math.maxInt(u8)) return error.UnsupportedAudioFormat;
            channels = @intCast(channel_count);
            sample_frames = std.math.cast(usize, std.mem.readInt(u32, payload[2..6], .big)) orelse return error.UnsupportedAudioFormat;
            bits_per_sample = std.mem.readInt(u16, payload[6..8], .big);
            sample_rate = try readExtended80AsU32(payload[8..18]);
            if (is_aifc) {
                const compression_type = payload[18..22];
                if (std.mem.eql(u8, compression_type, "NONE") or std.mem.eql(u8, compression_type, "twos")) {
                    endian = .big;
                } else if (std.mem.eql(u8, compression_type, "in24")) {
                    endian = .big;
                    if (bits_per_sample != 24) return error.UnsupportedAudioFormat;
                } else if (std.mem.eql(u8, compression_type, "in32")) {
                    endian = .big;
                    if (bits_per_sample != 32) return error.UnsupportedAudioFormat;
                } else if (std.mem.eql(u8, compression_type, "sowt")) {
                    endian = .little;
                } else if (std.mem.eql(u8, compression_type, "raw ")) {
                    encoding = .unsigned_int;
                    if (bits_per_sample != 8) return error.UnsupportedAudioFormat;
                } else if (std.mem.eql(u8, compression_type, "fl32") or std.mem.eql(u8, compression_type, "FL32")) {
                    endian = .big;
                    encoding = .float;
                    if (bits_per_sample != 32) return error.UnsupportedAudioFormat;
                } else if (std.mem.eql(u8, compression_type, "fl64") or std.mem.eql(u8, compression_type, "FL64")) {
                    endian = .big;
                    encoding = .float;
                    if (bits_per_sample != 64) return error.UnsupportedAudioFormat;
                } else if (std.mem.eql(u8, compression_type, "ulaw") or std.mem.eql(u8, compression_type, "ULAW")) {
                    encoding = .ulaw;
                    if (bits_per_sample != 8) return error.UnsupportedAudioFormat;
                } else if (std.mem.eql(u8, compression_type, "alaw") or std.mem.eql(u8, compression_type, "ALAW")) {
                    encoding = .alaw;
                    if (bits_per_sample != 8) return error.UnsupportedAudioFormat;
                } else {
                    return error.UnsupportedAudioFormat;
                }
            }
        } else if (std.mem.eql(u8, chunk_id, "SSND")) {
            if (payload.len < 8) return error.UnsupportedAudioFormat;
            const offset = std.math.cast(usize, std.mem.readInt(u32, payload[0..4], .big)) orelse return error.UnsupportedAudioFormat;
            if (8 + offset > payload.len) return error.UnsupportedAudioFormat;
            sound_data = payload[8 + offset ..];
        }

        cursor += chunk_size;
        if ((cursor & 1) != 0) cursor += 1;
    }

    if (channels == 0 or sample_frames == 0 or bits_per_sample == 0 or sample_rate == 0 or sound_data.len == 0) {
        return error.UnsupportedAudioFormat;
    }

    return .{
        .sample_rate = sample_rate,
        .channels = channels,
        .bits_per_sample = bits_per_sample,
        .endian = endian,
        .encoding = encoding,
        .sample_frames = sample_frames,
        .sound_data = sound_data,
    };
}

fn readI24(bytes: *const [3]u8, endian: std.builtin.Endian) i32 {
    const unsigned = switch (endian) {
        .big => (@as(u32, bytes[0]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[2],
        .little => (@as(u32, bytes[2]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[0],
    };
    return @as(i32, @bitCast(unsigned << 8)) >> 8;
}

fn readExtended80AsU32(bytes: []const u8) !u32 {
    if (bytes.len != 10) return error.UnsupportedAudioFormat;
    const sign_exp = std.mem.readInt(u16, bytes[0..2], .big);
    if ((sign_exp & 0x8000) != 0) return error.UnsupportedAudioFormat;

    const exponent = sign_exp & 0x7fff;
    if (exponent == 0 or exponent == 0x7fff) return error.UnsupportedAudioFormat;

    const mantissa = std.mem.readInt(u64, bytes[2..10], .big);
    const unbiased = @as(i32, exponent) - 16383;
    if (unbiased < 0) return error.UnsupportedAudioFormat;
    if (unbiased <= 63) {
        const shift: u6 = @intCast(63 - unbiased);
        const rounded = if (shift == 0)
            mantissa
        else
            (mantissa + (@as(u64, 1) << (shift - 1))) >> shift;
        return std.math.cast(u32, rounded) orelse error.UnsupportedAudioFormat;
    }

    if (unbiased > 94) return error.UnsupportedAudioFormat;
    const shift: u5 = @intCast(unbiased - 63);
    return std.math.cast(u32, mantissa << shift) orelse error.UnsupportedAudioFormat;
}

test "decode checked-in aiff pcm16 fixture to interleaved pcm" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_aiff_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 16000 * 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), decoded.samples[2], 1e-5);
}

test "decode synthetic aiff pcm8 and pcm24 fixtures" {
    const pcm8 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x32, 'A',  'I',  'F',  'F',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x12, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x08, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'S',  'S',  'N',  'D',  0x00, 0x00, 0x00, 0x0a, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x40,
    };
    var decoded8 = try decodeInterleaved(std.testing.allocator, &pcm8);
    defer decoded8.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded8.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded8.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded8.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded8.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded8.samples[1], 1e-6);

    const pcm24 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x36, 'A',  'I',  'F',  'F',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x12, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x18, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'S',  'S',  'N',  'D',  0x00, 0x00, 0x00, 0x0e, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x40, 0x00, 0x00,
    };
    var decoded24 = try decodeInterleaved(std.testing.allocator, &pcm24);
    defer decoded24.deinit();
    try std.testing.expectEqual(@as(usize, 2), decoded24.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded24.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded24.samples[1], 1e-6);
}

test "decode synthetic aifc none twos and sowt pcm fixtures" {
    const none = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x38, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x10, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'N',  'O',  'N',  'E',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00,
        0x40, 0x00,
    };
    var decoded_none = try decodeInterleaved(std.testing.allocator, &none);
    defer decoded_none.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_none.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_none.samples[1], 1e-6);

    const sowt = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x38, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x10, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 's',  'o',  'w',  't',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
        0x00, 0x40,
    };
    var decoded_sowt = try decodeInterleaved(std.testing.allocator, &sowt);
    defer decoded_sowt.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_sowt.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_sowt.samples[1], 1e-6);

    const twos = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x38, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x20, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 't',  'w',  'o',  's',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00,
        0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
    };
    var decoded_twos = try decodeInterleaved(std.testing.allocator, &twos);
    defer decoded_twos.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_twos.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_twos.samples[1], 1e-6);
}

test "decode synthetic aifc raw unsigned pcm8 fixture" {
    const raw = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x34, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x08, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'r',  'a',  'w',  ' ',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0,
    };
    var decoded = try decodeInterleaved(std.testing.allocator, &raw);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[1], 1e-6);
}

test "decode synthetic aifc in24 and in32 pcm aliases" {
    const in24 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x3a, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x18, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'i',  'n',  '2',  '4',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00,
        0x00, 0x40, 0x00, 0x00,
    };
    var decoded_in24 = try decodeInterleaved(std.testing.allocator, &in24);
    defer decoded_in24.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_in24.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_in24.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_in24.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_in24.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_in24.samples[1], 1e-6);

    const in32 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x3c, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x20, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'i',  'n',  '3',  '2',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00,
        0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
    };
    var decoded_in32 = try decodeInterleaved(std.testing.allocator, &in32);
    defer decoded_in32.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_in32.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_in32.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_in32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_in32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_in32.samples[1], 1e-6);
}

test "decode synthetic aifc twos signed pcm64 fixture" {
    const twos64 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x44, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x40, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 't',  'w',  'o',  's',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    };
    var decoded = try decodeInterleaved(std.testing.allocator, &twos64);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[1], 1e-6);
}

test "decode synthetic aifc fl32 and fl64 fixtures" {
    const fl32 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x3a, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x20, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'f',  'l',  '3',  '2',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xbf, 0x80,
        0x00, 0x00, 0x3f, 0x00, 0x00, 0x00,
    };
    var decoded_fl32 = try decodeInterleaved(std.testing.allocator, &fl32);
    defer decoded_fl32.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_fl32.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_fl32.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_fl32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_fl32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_fl32.samples[1], 1e-6);

    const fl64 = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x42, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x40, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'f',  'l',  '6',  '4',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xbf, 0xf0,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xe0, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    };
    var decoded_fl64 = try decodeInterleaved(std.testing.allocator, &fl64);
    defer decoded_fl64.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_fl64.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_fl64.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_fl64.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_fl64.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_fl64.samples[1], 1e-6);
}

test "decode synthetic aifc ulaw and alaw fixtures" {
    const ulaw = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x34, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x08, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'u',  'l',  'a',  'w',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
    };
    var decoded_ulaw = try decodeInterleaved(std.testing.allocator, &ulaw);
    defer decoded_ulaw.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_ulaw.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_ulaw.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_ulaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32124.0 / 32768.0), decoded_ulaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32124.0 / 32768.0), decoded_ulaw.samples[1], 1e-6);

    const alaw = [_]u8{
        'F',  'O',  'R',  'M',  0x00, 0x00, 0x00, 0x34, 'A',  'I',  'F',  'C',
        'C',  'O',  'M',  'M',  0x00, 0x00, 0x00, 0x16, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x08, 0x40, 0x0c, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 'a',  'l',  'a',  'w',  'S',  'S',  'N',  'D',  0x00, 0x00,
        0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0xaa,
    };
    var decoded_alaw = try decodeInterleaved(std.testing.allocator, &alaw);
    defer decoded_alaw.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_alaw.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_alaw.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_alaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32256.0 / 32768.0), decoded_alaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32256.0 / 32768.0), decoded_alaw.samples[1], 1e-6);
}
