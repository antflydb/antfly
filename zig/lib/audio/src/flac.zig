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

const tone_flac_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.flac");
const tone_flac_24bit_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-24bit.flac");

pub const DecodedInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DecodedInterleaved) void {
        self.allocator.free(self.samples);
    }
};

pub const StreamInfo = struct {
    min_block_size: u16,
    max_block_size: u16,
    min_frame_size: u32,
    max_frame_size: u32,
    sample_rate: u32,
    channels: u8,
    bits_per_sample: u8,
    total_samples: u64,
    md5: [16]u8,
};

const ParsedStream = struct {
    stream_info: StreamInfo,
    data_offset: usize,
};

const HeaderDefaults = struct {
    sample_rate: ?u32 = null,
    channels: ?u8 = null,
    bits_per_sample: ?u8 = null,
    max_block_size: u16 = 0,
};

const FrameHeader = struct {
    block_size: u16,
    sample_rate: u32,
    bits_per_sample: u8,
    channel_assignment: ChannelAssignment,
};

const ChannelAssignment = union(enum) {
    independent: u8,
    left_side,
    side_right,
    mid_side,

    fn decodedChannels(self: ChannelAssignment) u8 {
        return switch (self) {
            .independent => |count| count,
            else => 2,
        };
    }
};

const SubframeKind = enum {
    constant,
    verbatim,
    fixed,
    lpc,
};

const FrameDecoded = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    frame_samples: usize,
    consumed: usize,
    allocator: std.mem.Allocator,

    fn deinit(self: *FrameDecoded) void {
        self.allocator.free(self.samples);
    }
};

pub fn decodeInterleaved(allocator: std.mem.Allocator, bytes: []const u8) !DecodedInterleaved {
    const parsed = try parseStream(bytes);
    var cursor = parsed.data_offset;

    var samples = std.ArrayList(f32).empty;
    defer samples.deinit(allocator);

    while (cursor < bytes.len) {
        const frame = try decodeFrameAlloc(allocator, parsed.stream_info, bytes[cursor..]);
        defer {
            var owned = frame;
            owned.deinit();
        }
        cursor += frame.consumed;
        try samples.appendSlice(allocator, frame.samples);
    }

    return .{
        .samples = try samples.toOwnedSlice(allocator),
        .sample_rate = parsed.stream_info.sample_rate,
        .channels = parsed.stream_info.channels,
        .allocator = allocator,
    };
}

pub fn parseStreamInfo(bytes: []const u8) !StreamInfo {
    return (try parseStream(bytes)).stream_info;
}

fn parseStream(bytes: []const u8) !ParsedStream {
    if (bytes.len < 4) return error.UnsupportedAudioFormat;
    if (!std.mem.eql(u8, bytes[0..4], "fLaC")) return parseRawFrameStream(bytes);

    var cursor: usize = 4;
    var found_streaminfo = false;
    var stream_info: StreamInfo = undefined;
    while (true) {
        if (cursor + 4 > bytes.len) return error.UnsupportedAudioFormat;
        const header = bytes[cursor];
        const is_last = (header & 0x80) != 0;
        const block_type = header & 0x7f;
        const block_len = (@as(usize, bytes[cursor + 1]) << 16) |
            (@as(usize, bytes[cursor + 2]) << 8) |
            @as(usize, bytes[cursor + 3]);
        cursor += 4;
        if (cursor + block_len > bytes.len) return error.UnsupportedAudioFormat;
        const payload = bytes[cursor .. cursor + block_len];
        if (block_type == 0) {
            if (found_streaminfo or payload.len != 34) return error.UnsupportedAudioFormat;
            stream_info = try parseStreamInfoBlock(payload);
            found_streaminfo = true;
        }
        cursor += block_len;
        if (is_last) break;
    }

    if (!found_streaminfo) return error.UnsupportedAudioFormat;
    return .{
        .stream_info = stream_info,
        .data_offset = cursor,
    };
}

fn parseRawFrameStream(bytes: []const u8) !ParsedStream {
    if (bytes.len < 4) return error.UnsupportedAudioFormat;

    for (0..bytes.len - 1) |offset| {
        if (bytes[offset] != 0xff or (bytes[offset + 1] & 0xfc) != 0xf8) continue;

        var reader = BitReader.init(bytes[offset..]);
        const header = parseFrameHeaderWithDefaults(&reader, .{}) catch continue;
        const header_without_crc_len = reader.bit_offset / 8;
        if (header_without_crc_len >= bytes[offset..].len) continue;

        const expected_header_crc = bytes[offset + header_without_crc_len];
        const actual_header_crc = crc8(bytes[offset .. offset + header_without_crc_len]);
        if (expected_header_crc != actual_header_crc) continue;

        return .{
            .stream_info = .{
                .min_block_size = header.block_size,
                .max_block_size = header.block_size,
                .min_frame_size = 0,
                .max_frame_size = 0,
                .sample_rate = header.sample_rate,
                .channels = header.channel_assignment.decodedChannels(),
                .bits_per_sample = header.bits_per_sample,
                .total_samples = 0,
                .md5 = @as([16]u8, @splat(0)),
            },
            .data_offset = offset,
        };
    }

    return error.UnsupportedAudioFormat;
}

fn parseStreamInfoBlock(payload: []const u8) !StreamInfo {
    if (payload.len != 34) return error.UnsupportedAudioFormat;
    const packed_info = readU64(payload[10..18]);
    var md5: [16]u8 = undefined;
    @memcpy(md5[0..], payload[18..34]);

    const sample_rate = @as(u32, @intCast((packed_info >> 44) & 0xfffff));
    const channels = @as(u8, @intCast(((packed_info >> 41) & 0x7) + 1));
    const bits_per_sample = @as(u8, @intCast(((packed_info >> 36) & 0x1f) + 1));
    const total_samples = packed_info & 0x0f_ffff_ffff;
    if (channels == 0 or channels > 8) return error.UnsupportedAudioFormat;
    if (bits_per_sample < 4 or bits_per_sample > 32) return error.UnsupportedAudioFormat;

    return .{
        .min_block_size = readU16(payload[0..2]),
        .max_block_size = readU16(payload[2..4]),
        .min_frame_size = readU24(payload[4..7]),
        .max_frame_size = readU24(payload[7..10]),
        .sample_rate = sample_rate,
        .channels = channels,
        .bits_per_sample = bits_per_sample,
        .total_samples = total_samples,
        .md5 = md5,
    };
}

fn decodeFrameAlloc(allocator: std.mem.Allocator, stream_info: StreamInfo, bytes: []const u8) !FrameDecoded {
    var reader = BitReader.init(bytes);
    const header = try parseFrameHeader(&reader, stream_info);

    const header_without_crc_len = reader.bit_offset / 8;
    if (header_without_crc_len >= bytes.len) return error.UnsupportedAudioFormat;
    const expected_header_crc = bytes[header_without_crc_len];
    const actual_header_crc = crc8(bytes[0..header_without_crc_len]);
    if (expected_header_crc != actual_header_crc) return error.UnsupportedAudioFormat;
    try reader.skipBits(8);

    const encoded_channels = header.channel_assignment.decodedChannels();
    const decoded_channels = try allocator.alloc([]i64, encoded_channels);
    defer allocator.free(decoded_channels);
    var decoded_channel_count: usize = 0;
    defer for (decoded_channels[0..decoded_channel_count]) |channel| allocator.free(channel);

    for (decoded_channels, 0..) |*channel, index| {
        channel.* = try decodeSubframeAlloc(
            allocator,
            &reader,
            header.block_size,
            subframeBitsPerSample(header.channel_assignment, header.bits_per_sample, @intCast(index)),
        );
        decoded_channel_count += 1;
    }

    try reader.alignToByteAndValidateZero();
    const footer_offset = reader.bit_offset / 8;
    if (footer_offset + 2 > bytes.len) return error.UnsupportedAudioFormat;
    const expected_footer_crc = readU16(bytes[footer_offset .. footer_offset + 2]);
    const actual_footer_crc = crc16(bytes[0..footer_offset]);
    if (expected_footer_crc != actual_footer_crc) return error.UnsupportedAudioFormat;

    const out_channels = encoded_channels;
    const sample_count = @as(usize, header.block_size) * out_channels;
    const out = try allocator.alloc(f32, sample_count);
    errdefer allocator.free(out);

    const scale = @as(f64, @floatFromInt(@as(u64, 1) << @intCast(header.bits_per_sample - 1)));
    switch (header.channel_assignment) {
        .independent => {
            for (0..header.block_size) |i| {
                for (0..out_channels) |ch| {
                    out[i * out_channels + ch] = @floatCast(@as(f64, @floatFromInt(decoded_channels[ch][i])) / scale);
                }
            }
        },
        .left_side => {
            const left = decoded_channels[0];
            const side = decoded_channels[1];
            for (0..header.block_size) |i| {
                const left_sample = left[i];
                const right_sample = left_sample - side[i];
                out[i * 2] = @floatCast(@as(f64, @floatFromInt(left_sample)) / scale);
                out[i * 2 + 1] = @floatCast(@as(f64, @floatFromInt(right_sample)) / scale);
            }
        },
        .side_right => {
            const side = decoded_channels[0];
            const right = decoded_channels[1];
            for (0..header.block_size) |i| {
                const right_sample = right[i];
                const left_sample = right_sample + side[i];
                out[i * 2] = @floatCast(@as(f64, @floatFromInt(left_sample)) / scale);
                out[i * 2 + 1] = @floatCast(@as(f64, @floatFromInt(right_sample)) / scale);
            }
        },
        .mid_side => {
            const mid = decoded_channels[0];
            const side = decoded_channels[1];
            for (0..header.block_size) |i| {
                var mid_restored = mid[i] << 1;
                if ((side[i] & 1) != 0) mid_restored += 1;
                const left_sample = @divTrunc(mid_restored + side[i], 2);
                const right_sample = @divTrunc(mid_restored - side[i], 2);
                out[i * 2] = @floatCast(@as(f64, @floatFromInt(left_sample)) / scale);
                out[i * 2 + 1] = @floatCast(@as(f64, @floatFromInt(right_sample)) / scale);
            }
        },
    }

    return .{
        .samples = out,
        .sample_rate = header.sample_rate,
        .channels = out_channels,
        .frame_samples = header.block_size,
        .consumed = footer_offset + 2,
        .allocator = allocator,
    };
}

fn parseFrameHeader(reader: *BitReader, stream_info: StreamInfo) !FrameHeader {
    return parseFrameHeaderWithDefaults(reader, .{
        .sample_rate = stream_info.sample_rate,
        .channels = stream_info.channels,
        .bits_per_sample = stream_info.bits_per_sample,
        .max_block_size = stream_info.max_block_size,
    });
}

fn parseFrameHeaderWithDefaults(reader: *BitReader, defaults: HeaderDefaults) !FrameHeader {
    const sync = try reader.readBits(u16, 14);
    if (sync != 0x3ffe) return error.UnsupportedAudioFormat;
    if ((try reader.readBits(u1, 1)) != 0) return error.UnsupportedAudioFormat;
    const blocking_strategy = try reader.readBits(u1, 1);
    const block_size_code = try reader.readBits(u8, 4);
    const sample_rate_code = try reader.readBits(u8, 4);
    const channel_code = try reader.readBits(u8, 4);
    const bits_code = try reader.readBits(u8, 3);
    if ((try reader.readBits(u1, 1)) != 0) return error.UnsupportedAudioFormat;

    _ = try readUtf8Integer(reader);

    const block_size = try resolveBlockSize(reader, block_size_code, defaults.max_block_size);
    const sample_rate = try resolveSampleRate(reader, sample_rate_code, defaults.sample_rate);
    const channel_assignment = try parseChannelAssignment(channel_code);
    const bits_per_sample = try resolveBitsPerSample(bits_code, defaults.bits_per_sample);

    if (blocking_strategy == 0 and defaults.max_block_size != 0 and block_size > defaults.max_block_size) {
        return error.UnsupportedAudioFormat;
    }
    if (defaults.sample_rate != null and sample_rate != defaults.sample_rate.?) {
        return error.UnsupportedAudioFormat;
    }
    if (defaults.bits_per_sample != null and bits_per_sample != defaults.bits_per_sample.?) {
        return error.UnsupportedAudioFormat;
    }
    if (defaults.channels != null and channel_assignment.decodedChannels() != defaults.channels.?) {
        return error.UnsupportedAudioFormat;
    }

    return .{
        .block_size = block_size,
        .sample_rate = sample_rate,
        .bits_per_sample = bits_per_sample,
        .channel_assignment = channel_assignment,
    };
}

fn resolveBlockSize(reader: *BitReader, code: u8, _: u16) !u16 {
    return switch (code) {
        0 => error.UnsupportedAudioFormat,
        1 => 192,
        2...5 => @as(u16, 576) << @intCast(code - 2),
        6 => blk: {
            const raw = try reader.readBits(u16, 8);
            break :blk raw + 1;
        },
        7 => blk: {
            const raw = try reader.readBits(u16, 16);
            if (raw == std.math.maxInt(u16)) return error.UnsupportedAudioFormat;
            break :blk raw + 1;
        },
        8...15 => @as(u16, 256) << @intCast(code - 8),
        else => unreachable,
    };
}

fn resolveSampleRate(reader: *BitReader, code: u8, stream_sample_rate: ?u32) !u32 {
    return switch (code) {
        0 => stream_sample_rate orelse error.UnsupportedAudioFormat,
        1 => 88_200,
        2 => 176_400,
        3 => 192_000,
        4 => 8_000,
        5 => 16_000,
        6 => 22_050,
        7 => 24_000,
        8 => 32_000,
        9 => 44_100,
        10 => 48_000,
        11 => 96_000,
        12 => @as(u32, try reader.readBits(u16, 8)) * 1000,
        13 => try reader.readBits(u32, 16),
        14 => @as(u32, try reader.readBits(u32, 16)) * 10,
        15 => error.UnsupportedAudioFormat,
        else => unreachable,
    };
}

fn parseChannelAssignment(code: u8) !ChannelAssignment {
    return switch (code) {
        0...7 => .{ .independent = code + 1 },
        8 => .left_side,
        9 => .side_right,
        10 => .mid_side,
        else => error.UnsupportedAudioFormat,
    };
}

fn resolveBitsPerSample(code: u8, stream_bits_per_sample: ?u8) !u8 {
    return switch (code) {
        0 => stream_bits_per_sample orelse error.UnsupportedAudioFormat,
        1 => 8,
        2 => 12,
        4 => 16,
        5 => 20,
        6 => 24,
        7 => 32,
        else => error.UnsupportedAudioFormat,
    };
}

fn subframeBitsPerSample(assignment: ChannelAssignment, bits_per_sample: u8, channel_index: u8) u8 {
    return switch (assignment) {
        .independent => bits_per_sample,
        .left_side => if (channel_index == 1) bits_per_sample + 1 else bits_per_sample,
        .side_right => if (channel_index == 0) bits_per_sample + 1 else bits_per_sample,
        .mid_side => if (channel_index == 1) bits_per_sample + 1 else bits_per_sample,
    };
}

fn decodeSubframeAlloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    block_size: u16,
    bits_per_sample: u8,
) ![]i64 {
    if ((try reader.readBits(u1, 1)) != 0) return error.UnsupportedAudioFormat;
    const subframe_type = try reader.readBits(u8, 6);
    const wasted_bits = if ((try reader.readBits(u1, 1)) != 0) blk: {
        var count: u8 = 1;
        while ((try reader.readBits(u1, 1)) == 0) {
            count += 1;
        }
        break :blk count;
    } else 0;

    if (wasted_bits >= bits_per_sample) return error.UnsupportedAudioFormat;
    const effective_bits = bits_per_sample - wasted_bits;
    if (effective_bits == 0) return error.UnsupportedAudioFormat;

    const out = try allocator.alloc(i64, block_size);
    errdefer allocator.free(out);

    const kind: SubframeKind = switch (subframe_type) {
        0 => .constant,
        1 => .verbatim,
        8...12 => .fixed,
        32...63 => .lpc,
        else => return error.UnsupportedAudioFormat,
    };
    switch (kind) {
        .constant => {
            const sample = try reader.readSigned(effective_bits);
            for (out) |*value| value.* = sample;
        },
        .verbatim => {
            for (out) |*value| {
                value.* = try reader.readSigned(effective_bits);
            }
        },
        .fixed => {
            const order = subframe_type - 8;
            try decodeFixedSubframe(reader, out, effective_bits, order);
        },
        .lpc => {
            const order = subframe_type - 31;
            try decodeLpcSubframe(reader, out, effective_bits, order);
        },
    }

    if (wasted_bits != 0) {
        const shift: u6 = @intCast(wasted_bits);
        for (out) |*value| value.* <<= shift;
    }
    return out;
}

fn decodeFixedSubframe(reader: *BitReader, out: []i64, bits_per_sample: u8, order: u8) !void {
    if (order > 4 or order > out.len) return error.UnsupportedAudioFormat;
    for (0..order) |i| {
        out[i] = try reader.readSigned(bits_per_sample);
    }
    try decodeResiduals(reader, out, order, bits_per_sample);
    restoreFixed(out, order);
}

fn decodeLpcSubframe(reader: *BitReader, out: []i64, bits_per_sample: u8, order: u8) !void {
    if (order == 0 or order > 32 or order > out.len) return error.UnsupportedAudioFormat;
    for (0..order) |i| {
        out[i] = try reader.readSigned(bits_per_sample);
    }

    const coeff_precision_minus_one = try reader.readBits(u8, 4);
    if (coeff_precision_minus_one == 0x0f) return error.UnsupportedAudioFormat;
    const coeff_precision = coeff_precision_minus_one + 1;
    const shift = try reader.readSigned(5);
    if (shift < 0) return error.UnsupportedAudioFormat;

    var coeffs = @as([32]i32, @splat(0));
    for (0..order) |i| {
        coeffs[i] = @intCast(try reader.readSigned(coeff_precision));
    }

    try decodeResiduals(reader, out, order, bits_per_sample);
    restoreLpc(out, coeffs[0..order], @intCast(shift));
}

fn decodeResiduals(reader: *BitReader, out: []i64, predictor_order: u8, bits_per_sample: u8) !void {
    _ = bits_per_sample;
    const method = try reader.readBits(u8, 2);
    if (method > 1) return error.UnsupportedAudioFormat;
    const partition_order = try reader.readBits(u8, 4);
    const partition_count = @as(usize, 1) << @intCast(partition_order);
    if (partition_count == 0 or out.len % partition_count != 0) return error.UnsupportedAudioFormat;

    const rice_bits: usize = if (method == 0) 4 else 5;
    const escape_param: u8 = if (method == 0) 0x0f else 0x1f;
    const partition_samples = out.len / partition_count;

    var out_index: usize = predictor_order;
    for (0..partition_count) |partition| {
        const parameter = try reader.readBits(u8, rice_bits);
        var sample_count = partition_samples;
        if (partition == 0) {
            if (sample_count < predictor_order) return error.UnsupportedAudioFormat;
            sample_count -= predictor_order;
        }
        if (parameter == escape_param) {
            const raw_bits = try reader.readBits(u8, 5);
            for (0..sample_count) |_| {
                if (out_index >= out.len) return error.UnsupportedAudioFormat;
                out[out_index] = if (raw_bits == 0) 0 else try reader.readSigned(raw_bits);
                out_index += 1;
            }
            continue;
        }
        for (0..sample_count) |_| {
            if (out_index >= out.len) return error.UnsupportedAudioFormat;
            const folded = try readRice(reader, parameter);
            out[out_index] = unfoldSigned(folded);
            out_index += 1;
        }
    }
    if (out_index != out.len) return error.UnsupportedAudioFormat;
}

fn restoreFixed(out: []i64, order: u8) void {
    for (@as(usize, order)..out.len) |i| {
        const residual = out[i];
        out[i] = residual + switch (order) {
            0 => 0,
            1 => out[i - 1],
            2 => 2 * out[i - 1] - out[i - 2],
            3 => 3 * out[i - 1] - 3 * out[i - 2] + out[i - 3],
            4 => 4 * out[i - 1] - 6 * out[i - 2] + 4 * out[i - 3] - out[i - 4],
            else => unreachable,
        };
    }
}

fn restoreLpc(out: []i64, coeffs: []const i32, shift: u5) void {
    const order = coeffs.len;
    for (order..out.len) |i| {
        var prediction: i64 = 0;
        for (coeffs, 0..) |coeff, coeff_index| {
            prediction += @as(i64, coeff) * out[i - coeff_index - 1];
        }
        out[i] += prediction >> shift;
    }
}

fn readRice(reader: *BitReader, parameter: u8) !u64 {
    var quotient: u64 = 0;
    while ((try reader.readBits(u1, 1)) == 0) {
        quotient += 1;
    }
    const remainder = if (parameter == 0)
        @as(u64, 0)
    else
        try reader.readBits(u64, parameter);
    return (quotient << @intCast(parameter)) | remainder;
}

fn unfoldSigned(folded: u64) i64 {
    if ((folded & 1) == 0) return @intCast(folded >> 1);
    return -@as(i64, @intCast((folded >> 1) + 1));
}

fn readUtf8Integer(reader: *BitReader) !u64 {
    const first = try reader.readBits(u8, 8);
    if ((first & 0x80) == 0) return first;

    var leading_ones: u3 = 0;
    var mask: u8 = 0x80;
    while ((first & mask) != 0) : (mask >>= 1) {
        leading_ones += 1;
    }
    if (leading_ones < 2 or leading_ones > 7) return error.UnsupportedAudioFormat;

    const prefix_bits: u3 = 7 - leading_ones;
    const prefix_mask: u8 = if (prefix_bits == 0)
        0
    else
        (@as(u8, 1) << prefix_bits) - 1;
    var value: u64 = first & prefix_mask;
    for (1..leading_ones) |_| {
        const byte = try reader.readBits(u8, 8);
        if ((byte & 0xc0) != 0x80) return error.UnsupportedAudioFormat;
        value = (value << 6) | (byte & 0x3f);
    }
    return value;
}

fn readU24(bytes: []const u8) u32 {
    return (@as(u32, bytes[0]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[2];
}

fn readU16(bytes: []const u8) u16 {
    return (@as(u16, bytes[0]) << 8) | bytes[1];
}

fn readU64(bytes: []const u8) u64 {
    var value: u64 = 0;
    for (bytes) |byte| {
        value = (value << 8) | byte;
    }
    return value;
}

fn crc8(bytes: []const u8) u8 {
    var crc: u8 = 0;
    for (bytes) |byte| {
        crc ^= byte;
        for (0..8) |_| {
            if ((crc & 0x80) != 0) {
                crc = (crc << 1) ^ 0x07;
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

fn crc16(bytes: []const u8) u16 {
    var crc: u16 = 0;
    for (bytes) |byte| {
        crc ^= @as(u16, byte) << 8;
        for (0..8) |_| {
            if ((crc & 0x8000) != 0) {
                crc = (crc << 1) ^ 0x8005;
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

const BitReader = struct {
    bytes: []const u8,
    bit_offset: usize = 0,

    fn init(bytes: []const u8) BitReader {
        return .{ .bytes = bytes };
    }

    fn readBits(self: *BitReader, comptime T: type, count: usize) !T {
        if (count > @bitSizeOf(T) or self.bit_offset + count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        var value: u64 = 0;
        for (0..count) |_| {
            const byte = self.bytes[self.bit_offset / 8];
            const shift: u3 = @intCast(7 - (self.bit_offset % 8));
            value = (value << 1) | ((byte >> shift) & 1);
            self.bit_offset += 1;
        }
        return @intCast(value);
    }

    fn readSigned(self: *BitReader, count: usize) !i64 {
        if (count == 0 or count > 63) return error.UnsupportedAudioFormat;
        const raw = try self.readBits(u64, count);
        return signExtend(raw, count);
    }

    fn skipBits(self: *BitReader, count: usize) !void {
        if (self.bit_offset + count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        self.bit_offset += count;
    }

    fn alignToByteAndValidateZero(self: *BitReader) !void {
        const remainder = self.bit_offset % 8;
        if (remainder == 0) return;
        const pad_bits = 8 - remainder;
        const padding = try self.readBits(u8, pad_bits);
        if (padding != 0) return error.UnsupportedAudioFormat;
    }
};

fn signExtend(raw: u64, bit_count: usize) i64 {
    const shift = 64 - bit_count;
    return @as(i64, @bitCast(raw << @intCast(shift))) >> @intCast(shift);
}

test "parse checked-in flac streaminfo" {
    const info = try parseStreamInfo(tone_flac_bytes);
    try std.testing.expectEqual(@as(u32, 16000), info.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), info.channels);
    try std.testing.expectEqual(@as(u8, 16), info.bits_per_sample);
    try std.testing.expectEqual(@as(u64, 16000), info.total_samples);
}

test "decode checked-in flac fixture to interleaved pcm" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_flac_bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 32000), decoded.samples.len);
}

test "decode checked-in 24bit flac fixture to interleaved pcm" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_flac_24bit_bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 32000), decoded.samples.len);
}

test "resolve uncommon 32bit flac frame header bits-per-sample code" {
    try std.testing.expectEqual(@as(u8, 32), try resolveBitsPerSample(7, 32));
}
