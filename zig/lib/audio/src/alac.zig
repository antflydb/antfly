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
const caf = @import("caf.zig");
const mp4 = @import("mp4.zig");
const tone_caf_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.caf");
const tone_caf_24bit_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac-24bit.caf");
const tone_alac_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac.m4a");
const tone_alac_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac.mp4");
const tone_alac_24bit_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac-24bit.m4a");
const tone_alac_24bit_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac-24bit.mp4");

pub const MagicCookie = struct {
    frame_length: u32,
    compatible_version: u8,
    bit_depth: u8,
    pb: u8,
    mb: u8,
    kb: u8,
    channels: u8,
    max_run: u16,
    max_frame_bytes: u32,
    avg_bit_rate: u32,
    sample_rate: u32,
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

const FrameHeader = struct {
    channels: u8,
    has_size: bool,
    wasted_bits: u8,
    is_uncompressed: bool,
    output_samples: u32,
    read_sample_size: u32,
    interlacing_shift: u8,
    interlacing_leftweight: u8,
};

const ChannelPredictor = struct {
    prediction_type: u8 = 0,
    prediction_quantization: u8 = 0,
    rice_modifier: u8 = 0,
    predictor_coef_num: u8 = 0,
    predictor_coef_table: [32]i16 = @as([32]i16, @splat(0)),
};

pub fn parseMagicCookie(bytes: []const u8) !MagicCookie {
    const payload = try normalizeMagicCookiePayload(bytes);

    if (payload.len < 24) return error.UnsupportedAudioFormat;

    return .{
        .frame_length = std.mem.readInt(u32, payload[0..4], .big),
        .compatible_version = payload[4],
        .bit_depth = payload[5],
        .pb = payload[6],
        .mb = payload[7],
        .kb = payload[8],
        .channels = payload[9],
        .max_run = std.mem.readInt(u16, payload[10..12], .big),
        .max_frame_bytes = std.mem.readInt(u32, payload[12..16], .big),
        .avg_bit_rate = std.mem.readInt(u32, payload[16..20], .big),
        .sample_rate = std.mem.readInt(u32, payload[20..24], .big),
    };
}

pub fn decodeInterleavedPacketizedAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    channels: u16,
    decoder_config: []const u8,
    access_units: []const []const u8,
) !DecodedInterleaved {
    const cookie = try parseMagicCookie(decoder_config);
    if (cookie.bit_depth == 0 or cookie.bit_depth > 24) return error.UnsupportedAudioFormat;
    if (cookie.channels == 0 or cookie.channels > 2) return error.UnsupportedAudioFormat;
    if (channels != cookie.channels) return error.UnsupportedAudioFormat;
    if (sample_rate != cookie.sample_rate) return error.UnsupportedAudioFormat;
    const scale = alacScaleForBitDepth(cookie.bit_depth);

    var samples = std.ArrayList(f32).empty;
    defer samples.deinit(allocator);

    for (access_units, 0..) |packet, packet_index| {
        _ = packet_index;
        const decoded = try decodeFrameAlloc(allocator, cookie, packet);
        defer allocator.free(decoded);
        try samples.ensureUnusedCapacity(allocator, decoded.len);
        for (decoded) |sample| {
            samples.appendAssumeCapacity(@as(f32, @floatFromInt(sample)) / scale);
        }
    }

    return .{
        .samples = try samples.toOwnedSlice(allocator),
        .sample_rate = sample_rate,
        .channels = @intCast(channels),
        .allocator = allocator,
    };
}

pub fn extractMagicCookieFromMp4DecoderConfig(bytes: []const u8) ![]const u8 {
    if (bytes.len >= 40 and
        std.mem.readInt(u32, bytes[0..4], .big) == bytes.len and
        std.mem.eql(u8, bytes[4..8], "alac"))
    {
        return findAlacAtom(bytes[8..]) orelse error.UnsupportedAudioFormat;
    }
    if (bytes.len >= 8 and
        std.mem.readInt(u32, bytes[0..4], .big) == bytes.len and
        std.mem.eql(u8, bytes[4..8], "alac"))
    {
        return bytes;
    }
    return error.UnsupportedAudioFormat;
}

pub fn extractMagicCookieFromCaf(bytes: []const u8) ![]const u8 {
    if (bytes.len < 8 or !std.mem.eql(u8, bytes[0..4], "caff")) return error.UnsupportedAudioFormat;

    var cursor: usize = 8;
    while (cursor + 12 <= bytes.len) {
        const chunk_type = bytes[cursor .. cursor + 4];
        const chunk_size = std.mem.readInt(u64, bytes[cursor + 4 ..][0..8], .big);
        cursor += 12;
        const payload_len = if (chunk_size == std.math.maxInt(u64))
            bytes.len - cursor
        else
            std.math.cast(usize, chunk_size) orelse return error.UnsupportedAudioFormat;
        if (cursor + payload_len > bytes.len) return error.UnsupportedAudioFormat;
        const payload = bytes[cursor .. cursor + payload_len];
        if (std.mem.eql(u8, chunk_type, "kuki")) {
            return findAlacCookie(payload) orelse error.UnsupportedAudioFormat;
        }
        cursor += payload_len;
    }

    return error.UnsupportedAudioFormat;
}

fn findAlacCookie(payload: []const u8) ?[]const u8 {
    var cursor: usize = 0;
    while (cursor + 8 <= payload.len) {
        const atom_size = std.mem.readInt(u32, payload[cursor..][0..4], .big);
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

fn findAlacAtom(payload: []const u8) ?[]const u8 {
    return findAlacCookie(payload);
}

fn normalizeMagicCookiePayload(bytes: []const u8) ![]const u8 {
    if (bytes.len >= 40 and
        std.mem.readInt(u32, bytes[0..4], .big) == bytes.len and
        std.mem.eql(u8, bytes[4..8], "alac"))
    {
        const inner = findAlacAtom(bytes[8..]) orelse return error.UnsupportedAudioFormat;
        return try fullAlacAtomCookiePayload(inner);
    }

    if (bytes.len >= 8 and
        std.mem.readInt(u32, bytes[0..4], .big) == bytes.len and
        std.mem.eql(u8, bytes[4..8], "alac"))
    {
        return try fullAlacAtomCookiePayload(bytes);
    }

    return bytes;
}

fn fullAlacAtomCookiePayload(bytes: []const u8) ![]const u8 {
    if (bytes.len < 12 or !std.mem.eql(u8, bytes[4..8], "alac")) return error.UnsupportedAudioFormat;
    return bytes[12..];
}

fn decodeFrameAlloc(
    allocator: std.mem.Allocator,
    cookie: MagicCookie,
    packet: []const u8,
) ![]i32 {
    var br = BitReader.init(packet);
    const header = try parseFrameHeader(&br, cookie);
    if (header.channels != cookie.channels) return error.UnsupportedAudioFormat;
    if (header.wasted_bits >= cookie.bit_depth) return error.UnsupportedAudioFormat;

    const output_samples = std.math.cast(usize, header.output_samples) orelse return error.UnsupportedAudioFormat;
    const interleaved = try allocator.alloc(i32, output_samples * cookie.channels);
    errdefer allocator.free(interleaved);

    if (header.is_uncompressed) {
        try decodeUncompressedFrame(&br, cookie, header.wasted_bits, output_samples, interleaved);
    } else {
        try decodeCompressedFrame(allocator, &br, cookie, header, output_samples, interleaved);
    }
    try restoreWastedBits(&br, interleaved, cookie.bit_depth, header.wasted_bits);

    const end_marker = try br.readBits(u8, 3);
    if (end_marker != 7 and header.wasted_bits == 0) return error.UnsupportedAudioFormat;
    return interleaved;
}

fn parseFrameHeader(br: *BitReader, cookie: MagicCookie) !FrameHeader {
    const channels = try br.readBits(u8, 3) + 1;
    _ = try br.readBits(u8, 4);
    _ = try br.readBits(u16, 12);
    const has_size = try br.readBit();
    const wasted_bits = (try br.readBits(u8, 2)) << 3;
    const is_uncompressed = try br.readBit();
    const output_samples = if (has_size)
        try br.readBits(u32, 32)
    else
        cookie.frame_length;
    if (output_samples == 0 or output_samples > cookie.frame_length) return error.UnsupportedAudioFormat;

    const read_sample_size = @as(u32, cookie.bit_depth) - wasted_bits + channels - 1;
    if (read_sample_size > 32) return error.UnsupportedAudioFormat;

    return .{
        .channels = channels,
        .has_size = has_size,
        .wasted_bits = wasted_bits,
        .is_uncompressed = is_uncompressed,
        .output_samples = output_samples,
        .read_sample_size = read_sample_size,
        .interlacing_shift = if (is_uncompressed) 0 else try br.readBits(u8, 8),
        .interlacing_leftweight = if (is_uncompressed) 0 else try br.readBits(u8, 8),
    };
}

fn decodeUncompressedFrame(
    br: *BitReader,
    cookie: MagicCookie,
    wasted_bits: u8,
    output_samples: usize,
    interleaved: []i32,
) !void {
    const effective_bits: u8 = cookie.bit_depth - wasted_bits;
    if (cookie.channels == 1) {
        for (0..output_samples) |i| {
            interleaved[i] = try br.readSignedBitsI32(effective_bits);
        }
        return;
    }

    var left: i32 = 0;
    var right: i32 = 0;
    for (0..output_samples) |i| {
        left = try br.readSignedBitsI32(effective_bits);
        right = try br.readSignedBitsI32(effective_bits);
        interleaved[i * 2] = left;
        interleaved[i * 2 + 1] = right;
    }
}

fn restoreWastedBits(br: *BitReader, interleaved: []i32, bit_depth: u8, wasted_bits: u8) !void {
    if (wasted_bits == 0) return;
    if ((wasted_bits & 7) != 0 or wasted_bits >= bit_depth) return error.UnsupportedAudioFormat;

    const bits_needed = try std.math.mul(usize, interleaved.len, wasted_bits);
    const bits_remaining = br.bytes.len * 8 - br.bit_pos;
    if (bits_remaining < bits_needed) {
        for (interleaved) |*sample| {
            sample.* = signExtend(sample.* << @intCast(wasted_bits), bit_depth);
        }
        return;
    }

    for (interleaved) |*sample| {
        const low = try br.readBits(u32, wasted_bits);
        const widened = (sample.* << @intCast(wasted_bits)) | @as(i32, @intCast(low));
        sample.* = signExtend(widened, bit_depth);
    }
}

fn decodeCompressedFrame(
    allocator: std.mem.Allocator,
    br: *BitReader,
    cookie: MagicCookie,
    header: FrameHeader,
    output_samples: usize,
    interleaved: []i32,
) !void {
    var channel_predictors: [2]ChannelPredictor = .{
        std.mem.zeroes(ChannelPredictor),
        std.mem.zeroes(ChannelPredictor),
    };
    for (0..cookie.channels) |chan| {
        channel_predictors[chan] = try parseChannelPredictor(br);
        if (channel_predictors[chan].prediction_type != 0) return error.UnsupportedAudioFormat;
    }

    var predicterror: [2][]i32 = .{ undefined, undefined };
    var output: [2][]i32 = .{ undefined, undefined };
    var chan: usize = 0;
    while (chan < cookie.channels) : (chan += 1) {
        predicterror[chan] = try allocator.alloc(i32, output_samples);
        errdefer allocator.free(predicterror[chan]);
        output[chan] = try allocator.alloc(i32, output_samples);
        errdefer allocator.free(output[chan]);
    }
    defer {
        var i: usize = 0;
        while (i < cookie.channels) : (i += 1) {
            allocator.free(predicterror[i]);
            allocator.free(output[i]);
        }
    }

    for (0..cookie.channels) |i| {
        try bastardizedRiceDecompress(
            br,
            predicterror[i],
            header.read_sample_size,
            cookie.mb,
            cookie.kb,
            @divFloor(@as(i32, channel_predictors[i].rice_modifier) * @as(i32, cookie.pb), 4),
            (@as(i32, 1) << @intCast(cookie.kb)) - 1,
        );
        try predictorDecompressFirAdapt(
            predicterror[i],
            output[i],
            header.read_sample_size,
            channel_predictors[i].predictor_coef_table[0..channel_predictors[i].predictor_coef_num],
            channel_predictors[i].prediction_quantization,
        );
    }

    if (cookie.channels == 1) {
        for (0..output_samples) |i| {
            interleaved[i] = output[0][i];
        }
        return;
    }

    reconstructStereo(
        output[0],
        output[1],
        interleaved,
        header.interlacing_shift,
        header.interlacing_leftweight,
    ) catch return error.UnsupportedAudioFormat;
}

fn parseChannelPredictor(br: *BitReader) !ChannelPredictor {
    var predictor = ChannelPredictor{
        .prediction_type = try br.readBits(u8, 4),
        .prediction_quantization = try br.readBits(u8, 4),
        .rice_modifier = try br.readBits(u8, 3),
        .predictor_coef_num = try br.readBits(u8, 5),
    };
    if (predictor.predictor_coef_num > predictor.predictor_coef_table.len) return error.UnsupportedAudioFormat;
    for (0..predictor.predictor_coef_num) |i| {
        predictor.predictor_coef_table[i] = @bitCast(try br.readBits(u16, 16));
    }
    return predictor;
}

fn bastardizedRiceDecompress(
    br: *BitReader,
    output_buffer: []i32,
    read_sample_size: u32,
    rice_initial_history: u8,
    rice_kmodifier: u8,
    rice_history_mult: i32,
    rice_kmodifier_mask: i32,
) !void {
    var history: u32 = rice_initial_history;
    var sign_modifier: i32 = 0;

    var output_count: usize = 0;
    while (output_count < output_buffer.len) : (output_count += 1) {
        const k = alacLog2((history >> 9) + 3);
        const x = try decodeScalar(br, k, rice_kmodifier, read_sample_size);

        const x_modified = sign_modifier + x;
        var final_val = @divTrunc(x_modified + 1, 2);
        if ((x_modified & 1) != 0) final_val *= -1;
        output_buffer[output_count] = final_val;

        sign_modifier = 0;

        const next_history = @as(i64, history) +
            @as(i64, x_modified) * rice_history_mult -
            @divFloor(@as(i64, history) * rice_history_mult, 512);
        history = if (x_modified > 0xffff)
            0xffff
        else
            std.math.cast(u32, @max(@as(i64, 0), next_history)) orelse return error.UnsupportedAudioFormat;

        if (history < 128 and output_count + 1 < output_buffer.len) {
            sign_modifier = 1;
            const zero_k = lead32(history) - 24 + ((history + 16) >> 6);
            const zero_m = ((@as(u32, 1) << @intCast(zero_k)) - 1) & @as(u32, @intCast(rice_kmodifier_mask));
            const block_size_i32 = try decodeScalarWithMultiplier(br, zero_m, zero_k, 16);
            if (block_size_i32 > 0) {
                var block_size = std.math.cast(usize, block_size_i32) orelse return error.UnsupportedAudioFormat;
                if (block_size >= output_buffer.len - output_count) {
                    block_size = output_buffer.len - output_count - 1;
                }
                @memset(output_buffer[output_count + 1 .. output_count + 1 + block_size], 0);
                output_count += block_size;
            }
            if (block_size_i32 > 0xffff) sign_modifier = 0;
            history = 0;
        }
    }
}

fn decodeScalar(br: *BitReader, k_in: u32, limit_in: u8, read_sample_size: u32) !i32 {
    var k = k_in;
    const limit: u32 = limit_in;
    if (k >= limit) k = limit;

    return decodeScalarWithMultiplier(br, (@as(u32, 1) << @intCast(k)) - 1, k, read_sample_size);
}

fn decodeScalarWithMultiplier(br: *BitReader, m: u32, k: u32, read_sample_size: u32) !i32 {
    var x = try br.readUnary0_9();
    if (x > 8) {
        return @bitCast(try br.readBits(u32, @intCast(read_sample_size)));
    }

    if (k != 1) {
        const extra_bits = try br.peekBits(@intCast(k));
        x *= m;
        if (extra_bits > 1) {
            x += extra_bits - 1;
            try br.skipBits(@intCast(k));
        } else {
            try br.skipBits(@intCast(k - 1));
        }
    }

    return std.math.cast(i32, x) orelse error.UnsupportedAudioFormat;
}

fn predictorDecompressFirAdapt(
    error_buffer: []const i32,
    buffer_out: []i32,
    read_sample_size: u32,
    predictor_coef_table: []i16,
    predictor_quantization: u8,
) !void {
    if (error_buffer.len != buffer_out.len or error_buffer.len == 0) return error.UnsupportedAudioFormat;
    buffer_out[0] = error_buffer[0];

    if (predictor_coef_table.len == 0) {
        if (error_buffer.len > 1) @memcpy(buffer_out[1..], error_buffer[1..]);
        return;
    }

    if (predictor_coef_table.len == 0x1f) {
        for (0..error_buffer.len - 1) |i| {
            buffer_out[i + 1] = signExtend(buffer_out[i] + error_buffer[i + 1], read_sample_size);
        }
        return;
    }

    for (0..predictor_coef_table.len) |i| {
        const val = signExtend(buffer_out[i] + error_buffer[i + 1], read_sample_size);
        buffer_out[i + 1] = val;
    }

    var coeffs: [32]i16 = @as([32]i16, @splat(0));
    @memcpy(coeffs[0..predictor_coef_table.len], predictor_coef_table);

    var base_index: usize = 0;
    var i: usize = predictor_coef_table.len + 1;
    while (i < error_buffer.len) : (i += 1) {
        var sum: i32 = 0;
        for (0..predictor_coef_table.len) |j| {
            sum += (buffer_out[base_index + predictor_coef_table.len - j] - buffer_out[base_index]) * coeffs[j];
        }

        var outval: i32 = 0;
        if (predictor_quantization > 0) {
            outval = (@as(i32, 1) << @intCast(predictor_quantization - 1)) + sum;
            outval = shiftRightSigned(outval, predictor_quantization);
        } else {
            outval = sum;
        }
        outval = signExtend(outval + buffer_out[base_index] + error_buffer[i], read_sample_size);
        buffer_out[base_index + predictor_coef_table.len + 1] = outval;

        var error_val = error_buffer[i];
        if (error_val > 0) {
            var predictor_num: i32 = @intCast(predictor_coef_table.len - 1);
            while (predictor_num >= 0 and error_val > 0) : (predictor_num -= 1) {
                const tap_index: usize = @intCast(predictor_coef_table.len - 1 - @as(usize, @intCast(predictor_num)));
                const val0 = buffer_out[base_index] - buffer_out[base_index + tap_index + 1];
                const sign = signOnly(val0);
                coeffs[@intCast(predictor_num)] -= @intCast(sign);
                const abs_val = val0 * sign;
                error_val -= shiftRightSigned(abs_val, predictor_quantization) *
                    @as(i32, @intCast(predictor_coef_table.len - @as(usize, @intCast(predictor_num))));
            }
        } else if (error_val < 0) {
            var predictor_num: i32 = @intCast(predictor_coef_table.len - 1);
            while (predictor_num >= 0 and error_val < 0) : (predictor_num -= 1) {
                const tap_index: usize = @intCast(predictor_coef_table.len - 1 - @as(usize, @intCast(predictor_num)));
                const val0 = buffer_out[base_index] - buffer_out[base_index + tap_index + 1];
                const sign = -signOnly(val0);
                coeffs[@intCast(predictor_num)] -= @intCast(sign);
                const neg_val = val0 * sign;
                error_val -= shiftRightSigned(neg_val, predictor_quantization) *
                    @as(i32, @intCast(predictor_coef_table.len - @as(usize, @intCast(predictor_num))));
            }
        }

        base_index += 1;
    }
}

fn reconstructStereo(
    left_buf: []const i32,
    right_buf: []const i32,
    out: []i32,
    interlacing_shift: u8,
    interlacing_leftweight: u8,
) !void {
    if (left_buf.len != right_buf.len or out.len != left_buf.len * 2) return error.UnsupportedAudioFormat;
    if (interlacing_leftweight != 0) {
        for (left_buf, right_buf, 0..) |a_in, b_in, i| {
            var a = a_in;
            var b = b_in;
            a -= shiftRightSigned(b * @as(i32, interlacing_leftweight), interlacing_shift);
            b += a;
            out[i * 2] = b;
            out[i * 2 + 1] = a;
        }
        return;
    }

    for (left_buf, right_buf, 0..) |left, right, i| {
        out[i * 2] = left;
        out[i * 2 + 1] = right;
    }
}

fn signExtend(value: i32, bits: u32) i32 {
    if (bits == 0 or bits >= 32) return value;
    const shift: u5 = @intCast(32 - bits);
    return (value << shift) >> shift;
}

fn alacScaleForBitDepth(bit_depth: u8) f32 {
    return @as(f32, @floatFromInt(@as(u32, 1) << @intCast(bit_depth - 1)));
}

fn signOnly(v: i32) i32 {
    return if (v > 0) 1 else if (v < 0) -1 else 0;
}

fn shiftRightSigned(v: i32, bits: u32) i32 {
    const shift: u5 = @intCast(bits);
    return v >> shift;
}

fn alacLog2(v: u32) u32 {
    if (v == 0) return 0;
    return std.math.log2_int(u32, v);
}

fn lead32(v: u32) u32 {
    return @intCast(@clz(v));
}

const BitReader = struct {
    bytes: []const u8,
    bit_pos: usize = 0,

    fn init(bytes: []const u8) BitReader {
        return .{ .bytes = bytes };
    }

    fn readBit(self: *BitReader) !bool {
        return (try self.readBits(u1, 1)) != 0;
    }

    fn readBits(self: *BitReader, comptime T: type, count: usize) !T {
        if (count == 0) return 0;
        if (self.bit_pos + count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        var out: u64 = 0;
        for (0..count) |_| {
            const byte = self.bytes[self.bit_pos / 8];
            const shift = 7 - (self.bit_pos % 8);
            out = (out << 1) | ((byte >> @intCast(shift)) & 1);
            self.bit_pos += 1;
        }
        return std.math.cast(T, out) orelse error.UnsupportedAudioFormat;
    }

    fn peekBits(self: *BitReader, count: usize) !u32 {
        const saved = self.bit_pos;
        defer self.bit_pos = saved;
        return try self.readBits(u32, count);
    }

    fn skipBits(self: *BitReader, count: usize) !void {
        if (self.bit_pos + count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        self.bit_pos += count;
    }

    fn readUnary0_9(self: *BitReader) !u32 {
        var count: u32 = 0;
        while (count < 9) : (count += 1) {
            if (!(try self.readBit())) return count;
        }
        return count;
    }

    fn readSignedBitsI32(self: *BitReader, count: u8) !i32 {
        const raw = try self.readBits(u32, count);
        return signExtend(@intCast(raw), count);
    }
};

test "extract checked-in caf alac magic cookie" {
    try expectCheckedInCafAlacCookie(tone_caf_bytes, 16);
}

test "extract checked-in 24bit caf alac magic cookie" {
    try expectCheckedInCafAlacCookie(tone_caf_24bit_bytes, 24);
}

test "extract synthetic caf alac magic cookie from eof-sized kuki chunk" {
    const kuki_payload = [_]u8{
        0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
        0x00, 0x10, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x04, 0x00, 0x07, 0xd0, 0x00,
        0x00, 0x00, 0x3e, 0x80,
    };
    const synthetic =
        [_]u8{ 'c', 'a', 'f', 'f', 0x00, 0x01, 0x00, 0x00 } ++
        [_]u8{ 'k', 'u', 'k', 'i', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff } ++
        kuki_payload;

    const cookie_bytes = try extractMagicCookieFromCaf(&synthetic);
    const cookie = try parseMagicCookie(cookie_bytes);
    try std.testing.expectEqual(@as(u32, 4096), cookie.frame_length);
    try std.testing.expectEqual(@as(u8, 16), cookie.bit_depth);
    try std.testing.expectEqual(@as(u8, 2), cookie.channels);
    try std.testing.expectEqual(@as(u32, 16000), cookie.sample_rate);
}

test "parse checked-in mp4 alac decoder config" {
    try expectCheckedInMp4AlacCookie(tone_alac_m4a_bytes, 16);
}

test "parse checked-in 24bit mp4 alac decoder config" {
    try expectCheckedInMp4AlacCookie(tone_alac_24bit_m4a_bytes, 24);
}

test "parse checked-in generic mp4 alac decoder config" {
    try expectCheckedInMp4AlacCookie(tone_alac_mp4_bytes, 16);
}

test "parse checked-in 24bit generic mp4 alac decoder config" {
    try expectCheckedInMp4AlacCookie(tone_alac_24bit_mp4_bytes, 24);
}

test "decode checked-in caf alac fixture to interleaved pcm" {
    try expectDecodeCheckedInCafAlac(tone_caf_bytes);
}

test "decode checked-in 24bit caf alac fixture to interleaved pcm" {
    try expectDecodeCheckedInCafAlac(tone_caf_24bit_bytes);
}

test "decode checked-in m4a alac fixture to interleaved pcm" {
    try expectDecodeCheckedInMp4Alac(tone_alac_m4a_bytes);
}

test "decode checked-in 24bit m4a alac fixture to interleaved pcm" {
    try expectDecodeCheckedInMp4Alac(tone_alac_24bit_m4a_bytes);
}

test "decode checked-in generic mp4 alac fixture to interleaved pcm" {
    try expectDecodeCheckedInMp4Alac(tone_alac_mp4_bytes);
}

test "decode checked-in 24bit generic mp4 alac fixture to interleaved pcm" {
    try expectDecodeCheckedInMp4Alac(tone_alac_24bit_mp4_bytes);
}

fn expectCheckedInCafAlacCookie(bytes: []const u8, expected_bit_depth: u8) !void {
    const cookie_bytes = try extractMagicCookieFromCaf(bytes);
    const cookie = try parseMagicCookie(cookie_bytes);
    try std.testing.expectEqual(@as(u32, 4096), cookie.frame_length);
    try std.testing.expectEqual(expected_bit_depth, cookie.bit_depth);
    try std.testing.expectEqual(@as(u8, 2), cookie.channels);
    try std.testing.expectEqual(@as(u32, 16000), cookie.sample_rate);
}

fn expectCheckedInMp4AlacCookie(bytes: []const u8, expected_bit_depth: u8) !void {
    var demuxed = try mp4.demux(std.testing.allocator, bytes);
    defer demuxed.deinit();
    try std.testing.expectEqual(mp4.Codec.alac, demuxed.codec);

    const cookie_bytes = try extractMagicCookieFromMp4DecoderConfig(demuxed.decoder_config);
    const cookie = try parseMagicCookie(cookie_bytes);
    try std.testing.expectEqual(@as(u32, 4096), cookie.frame_length);
    try std.testing.expectEqual(expected_bit_depth, cookie.bit_depth);
    try std.testing.expectEqual(@as(u8, 2), cookie.channels);
    try std.testing.expectEqual(@as(u32, 16000), cookie.sample_rate);
}

fn expectDecodeCheckedInCafAlac(bytes: []const u8) !void {
    var demuxed = try caf.demux(std.testing.allocator, bytes);
    defer demuxed.deinit();
    try std.testing.expectEqual(caf.Codec.alac, demuxed.codec);

    var decoded = try decodeInterleavedPacketizedAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.channels,
        demuxed.decoder_config,
        demuxed.access_units,
    );
    defer decoded.deinit();

    try expectDecodedCheckedInAlacPcm(decoded);
}

fn expectDecodeCheckedInMp4Alac(bytes: []const u8) !void {
    var demuxed = try mp4.demux(std.testing.allocator, bytes);
    defer demuxed.deinit();
    try std.testing.expectEqual(mp4.Codec.alac, demuxed.codec);

    var decoded = try decodeInterleavedPacketizedAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.channels,
        demuxed.decoder_config,
        demuxed.access_units,
    );
    defer decoded.deinit();

    try expectDecodedCheckedInAlacPcm(decoded);
}

fn expectDecodedCheckedInAlacPcm(decoded: DecodedInterleaved) !void {
    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len >= 16000 * 2);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), decoded.samples[2], 1e-5);
}
