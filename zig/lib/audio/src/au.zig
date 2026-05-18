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
    encoding: SampleEncoding,
    data: []const u8,
};

const SampleEncoding = enum {
    signed_int,
    float,
    ulaw,
    alaw,
};

const SampleFormat = struct {
    encoding: SampleEncoding,
    bits_per_sample: u16,
};

pub fn decodeInterleaved(allocator: std.mem.Allocator, bytes: []const u8) !DecodedInterleaved {
    const parsed = try parse(bytes);
    const bytes_per_sample = switch (parsed.bits_per_sample) {
        8 => @as(usize, 1),
        16 => 2,
        24 => 3,
        32 => 4,
        64 => if (parsed.encoding == .float) @as(usize, 8) else return error.UnsupportedAudioFormat,
        else => return error.UnsupportedAudioFormat,
    };

    const frame_bytes = try std.math.mul(usize, bytes_per_sample, parsed.channels);
    if (frame_bytes == 0 or parsed.data.len % frame_bytes != 0) return error.UnsupportedAudioFormat;
    const sample_count = parsed.data.len / bytes_per_sample;

    const samples = try allocator.alloc(f32, sample_count);
    errdefer allocator.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * bytes_per_sample;
        sample.* = switch (parsed.encoding) {
            .signed_int => switch (parsed.bits_per_sample) {
                8 => blk: {
                    const raw: i8 = @bitCast(parsed.data[offset]);
                    break :blk @as(f32, @floatFromInt(raw)) / 128.0;
                },
                16 => blk: {
                    const raw = std.mem.readInt(i16, parsed.data[offset..][0..2], .big);
                    break :blk @as(f32, @floatFromInt(raw)) / 32768.0;
                },
                24 => blk: {
                    const raw = readI24(parsed.data[offset..][0..3]);
                    break :blk @as(f32, @floatFromInt(raw)) / 8388608.0;
                },
                32 => blk: {
                    const raw = std.mem.readInt(i32, parsed.data[offset..][0..4], .big);
                    break :blk @as(f32, @floatFromInt(raw)) / 2147483648.0;
                },
                else => return error.UnsupportedAudioFormat,
            },
            .float => switch (parsed.bits_per_sample) {
                32 => @bitCast(std.mem.readInt(u32, parsed.data[offset..][0..4], .big)),
                64 => @floatCast(@as(f64, @bitCast(std.mem.readInt(u64, parsed.data[offset..][0..8], .big)))),
                else => return error.UnsupportedAudioFormat,
            },
            .ulaw => switch (parsed.bits_per_sample) {
                8 => @as(f32, @floatFromInt(g711.decodeMuLaw(parsed.data[offset]))) / 32768.0,
                else => return error.UnsupportedAudioFormat,
            },
            .alaw => switch (parsed.bits_per_sample) {
                8 => @as(f32, @floatFromInt(g711.decodeALaw(parsed.data[offset]))) / 32768.0,
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
    if (bytes.len < 24 or !std.mem.eql(u8, bytes[0..4], ".snd")) return error.UnsupportedAudioFormat;

    const data_offset = std.math.cast(usize, std.mem.readInt(u32, bytes[4..8], .big)) orelse return error.UnsupportedAudioFormat;
    const data_size_raw = std.mem.readInt(u32, bytes[8..12], .big);
    const encoding_code = std.mem.readInt(u32, bytes[12..16], .big);
    const sample_rate = std.mem.readInt(u32, bytes[16..20], .big);
    const channel_count = std.mem.readInt(u32, bytes[20..24], .big);

    if (data_offset < 24 or data_offset > bytes.len) return error.UnsupportedAudioFormat;
    if (sample_rate == 0 or channel_count == 0 or channel_count > std.math.maxInt(u8)) return error.UnsupportedAudioFormat;

    const available_data = bytes[data_offset..];
    const data_size = if (data_size_raw == 0xffff_ffff)
        available_data.len
    else
        std.math.cast(usize, data_size_raw) orelse return error.UnsupportedAudioFormat;
    if (data_size > available_data.len) return error.UnsupportedAudioFormat;

    const sample_format: SampleFormat = switch (encoding_code) {
        1 => .{ .encoding = .ulaw, .bits_per_sample = 8 },
        2 => .{ .encoding = .signed_int, .bits_per_sample = 8 },
        3 => .{ .encoding = .signed_int, .bits_per_sample = 16 },
        4 => .{ .encoding = .signed_int, .bits_per_sample = 24 },
        5 => .{ .encoding = .signed_int, .bits_per_sample = 32 },
        6 => .{ .encoding = .float, .bits_per_sample = 32 },
        7 => .{ .encoding = .float, .bits_per_sample = 64 },
        27 => .{ .encoding = .alaw, .bits_per_sample = 8 },
        else => return error.UnsupportedAudioFormat,
    };

    return .{
        .sample_rate = sample_rate,
        .channels = @intCast(channel_count),
        .bits_per_sample = sample_format.bits_per_sample,
        .encoding = sample_format.encoding,
        .data = available_data[0..data_size],
    };
}

fn readI24(bytes: *const [3]u8) i32 {
    const unsigned = (@as(u32, bytes[0]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[2];
    return @as(i32, @bitCast(unsigned << 8)) >> 8;
}

test "decode synthetic au pcm and float fixtures" {
    const pcm16 = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x02,
        0x80, 0x00, 0x40, 0x00,
    };
    var decoded_pcm16 = try decodeInterleaved(std.testing.allocator, &pcm16);
    defer decoded_pcm16.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_pcm16.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded_pcm16.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_pcm16.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm16.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm16.samples[1], 1e-6);

    const fl32 = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x01,
        0xbf, 0x80, 0x00, 0x00, 0x3f, 0x00, 0x00, 0x00,
    };
    var decoded_fl32 = try decodeInterleaved(std.testing.allocator, &fl32);
    defer decoded_fl32.deinit();
    try std.testing.expectEqual(@as(u8, 1), decoded_fl32.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_fl32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_fl32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_fl32.samples[1], 1e-6);
}

test "decode synthetic au wide pcm and float fixtures" {
    const pcm8 = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x01,
        0x80, 0x40,
    };
    var decoded_pcm8 = try decodeInterleaved(std.testing.allocator, &pcm8);
    defer decoded_pcm8.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_pcm8.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_pcm8.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_pcm8.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm8.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm8.samples[1], 1e-6);

    const pcm24 = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x06,
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x01,
        0x80, 0x00, 0x00, 0x40, 0x00, 0x00,
    };
    var decoded_pcm24 = try decodeInterleaved(std.testing.allocator, &pcm24);
    defer decoded_pcm24.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_pcm24.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_pcm24.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_pcm24.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm24.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm24.samples[1], 1e-6);

    const pcm32 = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x01,
        0x80, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
    };
    var decoded_pcm32 = try decodeInterleaved(std.testing.allocator, &pcm32);
    defer decoded_pcm32.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_pcm32.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_pcm32.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_pcm32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm32.samples[1], 1e-6);

    const fl64 = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10,
        0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x01,
        0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xe0, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    var decoded_fl64 = try decodeInterleaved(std.testing.allocator, &fl64);
    defer decoded_fl64.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded_fl64.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_fl64.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_fl64.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_fl64.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_fl64.samples[1], 1e-6);
}

test "decode synthetic au g711 fixtures" {
    const ulaw = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x1f, 0x40, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x80,
    };
    var decoded_ulaw = try decodeInterleaved(std.testing.allocator, &ulaw);
    defer decoded_ulaw.deinit();
    try std.testing.expectEqual(@as(u32, 8000), decoded_ulaw.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_ulaw.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_ulaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32124.0 / 32768.0), decoded_ulaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32124.0 / 32768.0), decoded_ulaw.samples[1], 1e-6);

    const alaw = [_]u8{
        '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x00, 0x00, 0x1b, 0x00, 0x00, 0x1f, 0x40, 0x00, 0x00, 0x00, 0x01,
        0x2a, 0xaa,
    };
    var decoded_alaw = try decodeInterleaved(std.testing.allocator, &alaw);
    defer decoded_alaw.deinit();
    try std.testing.expectEqual(@as(u32, 8000), decoded_alaw.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded_alaw.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_alaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32256.0 / 32768.0), decoded_alaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32256.0 / 32768.0), decoded_alaw.samples[1], 1e-6);
}
