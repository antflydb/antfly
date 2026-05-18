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

const WAVE_FORMAT_PCM = 1;
const WAVE_FORMAT_IEEE_FLOAT = 3;
const WAVE_FORMAT_ALAW = 6;
const WAVE_FORMAT_MULAW = 7;
const WAVE_FORMAT_EXTENSIBLE = 0xfffe;
const WAVE_SUBFORMAT_TAIL = [_]u8{ 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71 };

pub const Format = struct {
    audio_format: u16,
    sample_rate: u32,
    bits_per_sample: u16,
};

pub const Decoded = struct {
    samples: []f32,
    format: Format,
};

pub const DecodedInterleaved = struct {
    samples: []f32,
    format: Format,
    channels: u16,
};

const ParsedData = struct {
    format: Format,
    channels: u16,
    data: []const u8,
    endian: std.builtin.Endian,
};

const RiffKind = enum {
    riff,
    rifx,
    rf64,
    bw64,
};

const RiffInfo = struct {
    kind: RiffKind,
    endian: std.builtin.Endian,
};

pub fn parseFormat(wav_bytes: []const u8) !Format {
    if (wav_bytes.len < 44) return error.WavTooShort;
    const riff = riffInfo(wav_bytes) orelse return error.NotRiffFormat;
    const endian = riff.endian;
    if (!std.mem.eql(u8, wav_bytes[8..12], "WAVE")) return error.NotWaveFormat;

    var pos: usize = 12;
    while (pos + 8 <= wav_bytes.len) {
        const chunk_id = wav_bytes[pos..][0..4];
        const chunk_size = std.mem.readInt(u32, wav_bytes[pos + 4 ..][0..4], endian);
        const chunk_len = std.math.cast(usize, chunk_size) orelse return error.UnsupportedAudioFormat;
        if (pos + 8 + chunk_len > wav_bytes.len) return error.WavTooShort;
        if (std.mem.eql(u8, chunk_id, "fmt ")) {
            return try parseFmtPayload(wav_bytes[pos + 8 ..][0..chunk_len], endian);
        }
        pos = std.math.add(usize, pos, 8 + chunk_len) catch return error.UnsupportedAudioFormat;
        if (pos % 2 != 0) pos = std.math.add(usize, pos, 1) catch return error.UnsupportedAudioFormat;
    }

    return error.MissingWavChunks;
}

pub fn decodeMono(alloc: std.mem.Allocator, wav_bytes: []const u8) !Decoded {
    const parsed = try parseData(wav_bytes);

    if (try decodeMonoFastPath(alloc, parsed)) |decoded| {
        return decoded;
    }

    const decoded = try decodeInterleavedParsed(alloc, parsed);
    errdefer alloc.free(decoded.samples);

    const mono = if (decoded.channels == 1)
        decoded.samples
    else
        try downmixToMono(alloc, decoded.samples, decoded.channels);
    errdefer if (decoded.channels != 1) alloc.free(mono);

    if (decoded.channels != 1) alloc.free(decoded.samples);
    return .{ .samples = mono, .format = decoded.format };
}

pub fn decodeInterleaved(alloc: std.mem.Allocator, wav_bytes: []const u8) !DecodedInterleaved {
    const parsed = try parseData(wav_bytes);
    return decodeInterleavedParsed(alloc, parsed);
}

fn parseData(wav_bytes: []const u8) !ParsedData {
    if (wav_bytes.len < 44) return error.WavTooShort;
    const riff = riffInfo(wav_bytes) orelse return error.NotRiffFormat;
    const endian = riff.endian;
    if (!std.mem.eql(u8, wav_bytes[8..12], "WAVE")) return error.NotWaveFormat;

    var pos: usize = 12;
    var channels: u16 = 0;
    var format: Format = .{ .audio_format = 0, .sample_rate = 0, .bits_per_sample = 0 };
    var data_start: usize = 0;
    var data_size: usize = 0;
    var rf64_data_size: ?usize = null;

    while (pos + 8 <= wav_bytes.len) {
        const chunk_id = wav_bytes[pos..][0..4];
        const chunk_size = std.mem.readInt(u32, wav_bytes[pos + 4 ..][0..4], endian);
        const chunk_len = if (isDs64SizedRiff(riff.kind) and chunk_size == std.math.maxInt(u32))
            if (std.mem.eql(u8, chunk_id, "data"))
                rf64_data_size orelse return error.UnsupportedAudioFormat
            else
                return error.UnsupportedAudioFormat
        else
            std.math.cast(usize, chunk_size) orelse return error.UnsupportedAudioFormat;
        if (pos + 8 + chunk_len > wav_bytes.len) return error.WavTooShort;
        if (std.mem.eql(u8, chunk_id, "fmt ")) {
            const payload = wav_bytes[pos + 8 ..][0..chunk_len];
            format = try parseFmtPayload(payload, endian);
            channels = std.mem.readInt(u16, payload[2..4], endian);
        } else if (std.mem.eql(u8, chunk_id, "ds64")) {
            rf64_data_size = try parseDs64DataSize(wav_bytes[pos + 8 ..][0..chunk_len]);
        } else if (std.mem.eql(u8, chunk_id, "data")) {
            data_start = pos + 8;
            data_size = chunk_len;
        }
        pos = std.math.add(usize, pos, 8 + chunk_len) catch return error.UnsupportedAudioFormat;
        if (pos % 2 != 0) pos = std.math.add(usize, pos, 1) catch return error.UnsupportedAudioFormat;
    }

    if (data_start == 0 or channels == 0) return error.MissingWavChunks;
    if (!isDecodableFormat(format.audio_format)) return error.UnsupportedAudioFormat;

    const bytes_per_sample: usize = @as(usize, format.bits_per_sample) / 8;
    if (bytes_per_sample == 0) return error.UnsupportedBitsPerSample;
    const frame_bytes = std.math.mul(usize, bytes_per_sample, @as(usize, channels)) catch return error.UnsupportedAudioFormat;
    if (frame_bytes == 0 or data_size % frame_bytes != 0) return error.UnsupportedAudioFormat;

    return .{
        .format = format,
        .channels = channels,
        .data = wav_bytes[data_start..][0..data_size],
        .endian = endian,
    };
}

fn riffInfo(wav_bytes: []const u8) ?RiffInfo {
    if (std.mem.eql(u8, wav_bytes[0..4], "RIFF")) return .{ .kind = .riff, .endian = .little };
    if (std.mem.eql(u8, wav_bytes[0..4], "RIFX")) return .{ .kind = .rifx, .endian = .big };
    if (std.mem.eql(u8, wav_bytes[0..4], "RF64")) return .{ .kind = .rf64, .endian = .little };
    if (std.mem.eql(u8, wav_bytes[0..4], "BW64")) return .{ .kind = .bw64, .endian = .little };
    return null;
}

fn isDs64SizedRiff(kind: RiffKind) bool {
    return kind == .rf64 or kind == .bw64;
}

fn parseDs64DataSize(payload: []const u8) !usize {
    if (payload.len < 28) return error.UnsupportedAudioFormat;
    const data_size = std.mem.readInt(u64, payload[8..16], .little);
    return std.math.cast(usize, data_size) orelse error.UnsupportedAudioFormat;
}

fn parseFmtPayload(payload: []const u8, endian: std.builtin.Endian) !Format {
    if (payload.len < 16) return error.FmtChunkTooSmall;

    var audio_format = std.mem.readInt(u16, payload[0..2], endian);
    const sample_rate = std.mem.readInt(u32, payload[4..8], endian);
    const bits_per_sample = std.mem.readInt(u16, payload[14..16], endian);

    if (audio_format == WAVE_FORMAT_EXTENSIBLE) {
        if (endian != .little) return error.UnsupportedAudioFormat;
        if (payload.len < 40) return error.UnsupportedAudioFormat;
        const extension_size = std.mem.readInt(u16, payload[16..18], .little);
        if (extension_size < 22) return error.UnsupportedAudioFormat;

        const valid_bits_per_sample = std.mem.readInt(u16, payload[18..20], .little);
        if (valid_bits_per_sample != 0 and valid_bits_per_sample != bits_per_sample) {
            return error.UnsupportedAudioFormat;
        }

        const subformat = payload[24..40];
        if (!std.mem.eql(u8, subformat[4..16], &WAVE_SUBFORMAT_TAIL)) {
            return error.UnsupportedAudioFormat;
        }
        const subformat_tag = std.mem.readInt(u32, subformat[0..4], .little);
        audio_format = std.math.cast(u16, subformat_tag) orelse return error.UnsupportedAudioFormat;
    }

    if (!isDecodableFormat(audio_format)) {
        return error.UnsupportedAudioFormat;
    }
    if ((audio_format == WAVE_FORMAT_ALAW or audio_format == WAVE_FORMAT_MULAW) and bits_per_sample != 8) {
        return error.UnsupportedBitsPerSample;
    }

    return .{
        .audio_format = audio_format,
        .sample_rate = sample_rate,
        .bits_per_sample = bits_per_sample,
    };
}

fn decodeInterleavedParsed(alloc: std.mem.Allocator, parsed: ParsedData) !DecodedInterleaved {
    if (try decodeInterleavedFastPath(alloc, parsed)) |decoded| {
        return decoded;
    }

    const bytes_per_sample = sampleByteWidth(parsed.format);
    const total_samples = parsed.data.len / bytes_per_sample;
    const samples = try alloc.alloc(f32, total_samples);
    errdefer alloc.free(samples);

    for (0..total_samples) |i| {
        const offset = i * bytes_per_sample;
        if (offset + bytes_per_sample > parsed.data.len) break;
        samples[i] = switch (parsed.format.bits_per_sample) {
            8 => (@as(f32, @floatFromInt(parsed.data[offset])) - 128.0) / 128.0,
            16 => blk: {
                const raw = std.mem.readInt(i16, parsed.data[offset..][0..2], parsed.endian);
                break :blk @as(f32, @floatFromInt(raw)) / 32768.0;
            },
            24 => blk: {
                const raw = readI24(parsed.data[offset..][0..3], parsed.endian);
                break :blk @as(f32, @floatFromInt(raw)) / 8388608.0;
            },
            32 => blk: {
                if (parsed.format.audio_format == 3) {
                    break :blk @bitCast(std.mem.readInt(u32, parsed.data[offset..][0..4], parsed.endian));
                }
                const raw = std.mem.readInt(i32, parsed.data[offset..][0..4], parsed.endian);
                break :blk @as(f32, @floatFromInt(raw)) / 2147483648.0;
            },
            64 => blk: {
                if (parsed.format.audio_format == WAVE_FORMAT_IEEE_FLOAT) {
                    break :blk @floatCast(@as(f64, @bitCast(std.mem.readInt(u64, parsed.data[offset..][0..8], parsed.endian))));
                }
                const raw = std.mem.readInt(i64, parsed.data[offset..][0..8], parsed.endian);
                break :blk @floatCast(@as(f64, @floatFromInt(raw)) / 9223372036854775808.0);
            },
            else => return error.UnsupportedBitsPerSample,
        };
    }

    return .{ .samples = samples, .format = parsed.format, .channels = parsed.channels };
}

fn decodeInterleavedFastPath(alloc: std.mem.Allocator, parsed: ParsedData) !?DecodedInterleaved {
    return switch (parsed.format.bits_per_sample) {
        8 => try decodeInterleaved8(alloc, parsed.data, parsed.format, parsed.channels),
        16 => try decodeInterleaved16(alloc, parsed.data, parsed.format, parsed.channels, parsed.endian),
        24 => try decodeInterleaved24(alloc, parsed.data, parsed.format, parsed.channels, parsed.endian),
        32 => if (parsed.format.audio_format == 3)
            try decodeInterleavedF32(alloc, parsed.data, parsed.format, parsed.channels, parsed.endian)
        else
            try decodeInterleaved32(alloc, parsed.data, parsed.format, parsed.channels, parsed.endian),
        64 => if (parsed.format.audio_format == WAVE_FORMAT_IEEE_FLOAT)
            try decodeInterleavedF64(alloc, parsed.data, parsed.format, parsed.channels, parsed.endian)
        else
            try decodeInterleaved64(alloc, parsed.data, parsed.format, parsed.channels, parsed.endian),
        else => null,
    };
}

fn decodeInterleaved8(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16) !DecodedInterleaved {
    const samples = try alloc.alloc(f32, data.len);
    errdefer alloc.free(samples);

    for (data, samples) |byte, *sample| {
        sample.* = decode8(byte, format.audio_format);
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

fn decodeInterleaved16(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16, endian: std.builtin.Endian) !DecodedInterleaved {
    const sample_count = data.len / 2;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * 2;
        const raw = std.mem.readInt(i16, data[offset..][0..2], endian);
        sample.* = @as(f32, @floatFromInt(raw)) / 32768.0;
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

fn decodeInterleaved24(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16, endian: std.builtin.Endian) !DecodedInterleaved {
    const sample_count = data.len / 3;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * 3;
        const raw = readI24(data[offset..][0..3], endian);
        sample.* = @as(f32, @floatFromInt(raw)) / 8388608.0;
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

fn decodeInterleaved32(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16, endian: std.builtin.Endian) !DecodedInterleaved {
    const sample_count = data.len / 4;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * 4;
        const raw = std.mem.readInt(i32, data[offset..][0..4], endian);
        sample.* = @as(f32, @floatFromInt(raw)) / 2147483648.0;
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

fn decodeInterleavedF32(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16, endian: std.builtin.Endian) !DecodedInterleaved {
    const sample_count = data.len / 4;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * 4;
        sample.* = @bitCast(std.mem.readInt(u32, data[offset..][0..4], endian));
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

fn decodeInterleavedF64(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16, endian: std.builtin.Endian) !DecodedInterleaved {
    const sample_count = data.len / 8;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * 8;
        sample.* = @floatCast(@as(f64, @bitCast(std.mem.readInt(u64, data[offset..][0..8], endian))));
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

fn decodeInterleaved64(alloc: std.mem.Allocator, data: []const u8, format: Format, channels: u16, endian: std.builtin.Endian) !DecodedInterleaved {
    const sample_count = data.len / 8;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, i| {
        const offset = i * 8;
        const raw = std.mem.readInt(i64, data[offset..][0..8], endian);
        sample.* = @floatCast(@as(f64, @floatFromInt(raw)) / 9223372036854775808.0);
    }

    return .{ .samples = samples, .format = format, .channels = channels };
}

pub fn encodeMono(alloc: std.mem.Allocator, samples: []const f32, format: Format) ![]u8 {
    return encodeInterleaved(alloc, samples, 1, format);
}

pub fn encodeInterleaved(alloc: std.mem.Allocator, samples: []const f32, channels: u16, format: Format) ![]u8 {
    const bytes_per_sample: usize = @as(usize, format.bits_per_sample) / 8;
    if (bytes_per_sample == 0) return error.UnsupportedBitsPerSample;
    if (channels == 0) return error.UnsupportedAudioFormat;
    if (samples.len % channels != 0) return error.WavTooShort;
    if (format.audio_format != 1 and format.audio_format != 3) return error.UnsupportedAudioFormat;
    if (format.audio_format == 3 and format.bits_per_sample != 32 and format.bits_per_sample != 64) return error.UnsupportedBitsPerSample;

    const data_len = samples.len * bytes_per_sample;
    const total_len = 44 + data_len;
    const wav = try alloc.alloc(u8, total_len);
    errdefer alloc.free(wav);

    @memcpy(wav[0..4], "RIFF");
    std.mem.writeInt(u32, wav[4..8], @intCast(total_len - 8), .little);
    @memcpy(wav[8..12], "WAVE");
    @memcpy(wav[12..16], "fmt ");
    std.mem.writeInt(u32, wav[16..20], 16, .little);
    std.mem.writeInt(u16, wav[20..22], format.audio_format, .little);
    std.mem.writeInt(u16, wav[22..24], channels, .little);
    std.mem.writeInt(u32, wav[24..28], format.sample_rate, .little);
    std.mem.writeInt(u32, wav[28..32], format.sample_rate * @as(u32, @intCast(bytes_per_sample * channels)), .little);
    std.mem.writeInt(u16, wav[32..34], @intCast(bytes_per_sample * channels), .little);
    std.mem.writeInt(u16, wav[34..36], format.bits_per_sample, .little);
    @memcpy(wav[36..40], "data");
    std.mem.writeInt(u32, wav[40..44], @intCast(data_len), .little);

    const data = wav[44..];
    switch (format.bits_per_sample) {
        8 => encodeInterleaved8(samples, data),
        16 => encodeInterleaved16(samples, data),
        24 => encodeInterleaved24(samples, data),
        32 => if (format.audio_format == 3)
            encodeInterleavedF32(samples, data)
        else
            encodeInterleaved32(samples, data),
        64 => if (format.audio_format == 3)
            encodeInterleavedF64(samples, data)
        else
            encodeInterleaved64(samples, data),
        else => return error.UnsupportedBitsPerSample,
    }

    return wav;
}

fn encodeInterleaved8(samples: []const f32, out: []u8) void {
    for (samples, out) |sample, *byte| {
        const clamped = std.math.clamp(sample, -1.0, 1.0);
        byte.* = @intFromFloat((clamped + 1.0) * 127.5);
    }
}

fn encodeInterleaved16(samples: []const f32, out: []u8) void {
    for (samples, 0..) |sample, i| {
        const clamped = std.math.clamp(sample, -1.0, 1.0);
        const value: i16 = @intFromFloat(clamped * 32767.0);
        std.mem.writeInt(i16, out[i * 2 ..][0..2], value, .little);
    }
}

fn encodeInterleaved24(samples: []const f32, out: []u8) void {
    for (samples, 0..) |sample, i| {
        const clamped = std.math.clamp(sample, -1.0, 1.0);
        const value: i32 = @intFromFloat(clamped * 8388607.0);
        const unsigned: u24 = @truncate(@as(u32, @bitCast(value)));
        std.mem.writeInt(u24, out[i * 3 ..][0..3], unsigned, .little);
    }
}

fn encodeInterleaved32(samples: []const f32, out: []u8) void {
    for (samples, 0..) |sample, i| {
        const clamped = std.math.clamp(sample, -1.0, 1.0);
        const value: i32 = @intFromFloat(@as(f64, clamped) * 2147483647.0);
        std.mem.writeInt(i32, out[i * 4 ..][0..4], value, .little);
    }
}

fn encodeInterleaved64(samples: []const f32, out: []u8) void {
    for (samples, 0..) |sample, i| {
        const clamped = std.math.clamp(sample, -1.0, 1.0);
        const value: i64 = if (clamped <= -1.0)
            std.math.minInt(i64)
        else if (clamped >= 1.0)
            std.math.maxInt(i64)
        else
            @intFromFloat(@as(f64, clamped) * @as(f64, @floatFromInt(std.math.maxInt(i64))));
        std.mem.writeInt(i64, out[i * 8 ..][0..8], value, .little);
    }
}

fn encodeInterleavedF32(samples: []const f32, out: []u8) void {
    for (samples, 0..) |sample, i| {
        std.mem.writeInt(u32, out[i * 4 ..][0..4], @bitCast(sample), .little);
    }
}

fn encodeInterleavedF64(samples: []const f32, out: []u8) void {
    for (samples, 0..) |sample, i| {
        const widened: f64 = @floatCast(sample);
        std.mem.writeInt(u64, out[i * 8 ..][0..8], @bitCast(widened), .little);
    }
}

fn downmixToMono(alloc: std.mem.Allocator, samples: []const f32, channels: u16) ![]f32 {
    if (channels == 0 or samples.len % channels != 0) return error.WavTooShort;
    const frame_count = samples.len / channels;
    const mono = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(mono);

    for (0..frame_count) |i| {
        var sum: f32 = 0;
        for (0..channels) |ch| {
            sum += samples[i * channels + ch];
        }
        mono[i] = sum / @as(f32, @floatFromInt(channels));
    }

    return mono;
}

fn decodeMonoFastPath(alloc: std.mem.Allocator, parsed: ParsedData) !?Decoded {
    const bytes_per_sample = sampleByteWidth(parsed.format);
    if (parsed.channels == 1) {
        return switch (parsed.format.bits_per_sample) {
            8 => try decodeMono8(alloc, parsed.data, parsed.format),
            16 => try decodeMono16(alloc, parsed.data, parsed.format, parsed.endian),
            24 => try decodeMono24(alloc, parsed.data, parsed.format, parsed.endian),
            32 => if (parsed.format.audio_format == 3)
                try decodeMonoF32(alloc, parsed.data, parsed.format, parsed.endian)
            else
                try decodeMono32(alloc, parsed.data, parsed.format, parsed.endian),
            64 => if (parsed.format.audio_format == WAVE_FORMAT_IEEE_FLOAT)
                try decodeMonoF64(alloc, parsed.data, parsed.format, parsed.endian)
            else
                try decodeMono64(alloc, parsed.data, parsed.format, parsed.endian),
            else => null,
        };
    }

    if (parsed.channels == 2) {
        return switch (parsed.format.bits_per_sample) {
            8 => if (bytes_per_sample == 1)
                try decodeStereo8ToMono(alloc, parsed.data, parsed.format)
            else
                null,
            16 => if (bytes_per_sample == 2)
                try decodeStereo16ToMono(alloc, parsed.data, parsed.format, parsed.endian)
            else
                null,
            24 => if (bytes_per_sample == 3)
                try decodeStereo24ToMono(alloc, parsed.data, parsed.format, parsed.endian)
            else
                null,
            32 => if (parsed.format.audio_format == 3)
                try decodeStereoF32ToMono(alloc, parsed.data, parsed.format, parsed.endian)
            else
                try decodeStereo32ToMono(alloc, parsed.data, parsed.format, parsed.endian),
            64 => if (parsed.format.audio_format == WAVE_FORMAT_IEEE_FLOAT)
                try decodeStereoF64ToMono(alloc, parsed.data, parsed.format, parsed.endian)
            else
                try decodeStereo64ToMono(alloc, parsed.data, parsed.format, parsed.endian),
            else => null,
        };
    }

    return null;
}

fn decodeMono8(alloc: std.mem.Allocator, data: []const u8, format: Format) !Decoded {
    const samples = try alloc.alloc(f32, data.len);
    errdefer alloc.free(samples);

    for (data, 0..) |byte, i| {
        samples[i] = decode8(byte, format.audio_format);
    }

    return .{ .samples = samples, .format = format };
}

fn decodeMono16(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const sample_count = data.len / 2;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (0..sample_count) |i| {
        const offset = i * 2;
        const raw = std.mem.readInt(i16, data[offset..][0..2], endian);
        samples[i] = @as(f32, @floatFromInt(raw)) / 32768.0;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeMono24(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const sample_count = data.len / 3;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (0..sample_count) |i| {
        const offset = i * 3;
        const raw = readI24(data[offset..][0..3], endian);
        samples[i] = @as(f32, @floatFromInt(raw)) / 8388608.0;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeMonoF32(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const sample_count = data.len / 4;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (0..sample_count) |i| {
        const offset = i * 4;
        samples[i] = @bitCast(std.mem.readInt(u32, data[offset..][0..4], endian));
    }

    return .{ .samples = samples, .format = format };
}

fn decodeMono32(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const sample_count = data.len / 4;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (0..sample_count) |i| {
        const offset = i * 4;
        const raw = std.mem.readInt(i32, data[offset..][0..4], endian);
        samples[i] = @as(f32, @floatFromInt(raw)) / 2147483648.0;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeMonoF64(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const sample_count = data.len / 8;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (0..sample_count) |i| {
        const offset = i * 8;
        samples[i] = @floatCast(@as(f64, @bitCast(std.mem.readInt(u64, data[offset..][0..8], endian))));
    }

    return .{ .samples = samples, .format = format };
}

fn decodeMono64(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const sample_count = data.len / 8;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (0..sample_count) |i| {
        const offset = i * 8;
        const raw = std.mem.readInt(i64, data[offset..][0..8], endian);
        samples[i] = @floatCast(@as(f64, @floatFromInt(raw)) / 9223372036854775808.0);
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereo8ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format) !Decoded {
    const frame_count = data.len / 2;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 2;
        const left = decode8(data[offset], format.audio_format);
        const right = decode8(data[offset + 1], format.audio_format);
        samples[i] = (left + right) * 0.5;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereo16ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const frame_count = data.len / 4;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 4;
        const left = std.mem.readInt(i16, data[offset..][0..2], endian);
        const right = std.mem.readInt(i16, data[offset + 2 ..][0..2], endian);
        samples[i] = (@as(f32, @floatFromInt(left)) + @as(f32, @floatFromInt(right))) / 65536.0;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereo24ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const frame_count = data.len / 6;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 6;
        const left = readI24(data[offset..][0..3], endian);
        const right = readI24(data[offset + 3 ..][0..3], endian);
        samples[i] = (@as(f32, @floatFromInt(left)) + @as(f32, @floatFromInt(right))) / 16777216.0;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereoF32ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const frame_count = data.len / 8;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 8;
        const left: f32 = @bitCast(std.mem.readInt(u32, data[offset..][0..4], endian));
        const right: f32 = @bitCast(std.mem.readInt(u32, data[offset + 4 ..][0..4], endian));
        samples[i] = (left + right) * 0.5;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereo32ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const frame_count = data.len / 8;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 8;
        const left = std.mem.readInt(i32, data[offset..][0..4], endian);
        const right = std.mem.readInt(i32, data[offset + 4 ..][0..4], endian);
        samples[i] = (@as(f32, @floatFromInt(left)) + @as(f32, @floatFromInt(right))) / 4294967296.0;
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereoF64ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const frame_count = data.len / 16;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 16;
        const left: f64 = @bitCast(std.mem.readInt(u64, data[offset..][0..8], endian));
        const right: f64 = @bitCast(std.mem.readInt(u64, data[offset + 8 ..][0..8], endian));
        samples[i] = @floatCast((left + right) * 0.5);
    }

    return .{ .samples = samples, .format = format };
}

fn decodeStereo64ToMono(alloc: std.mem.Allocator, data: []const u8, format: Format, endian: std.builtin.Endian) !Decoded {
    const frame_count = data.len / 16;
    const samples = try alloc.alloc(f32, frame_count);
    errdefer alloc.free(samples);

    for (0..frame_count) |i| {
        const offset = i * 16;
        const left = std.mem.readInt(i64, data[offset..][0..8], endian);
        const right = std.mem.readInt(i64, data[offset + 8 ..][0..8], endian);
        samples[i] = @floatCast((@as(f64, @floatFromInt(left)) + @as(f64, @floatFromInt(right))) / 18446744073709551616.0);
    }

    return .{ .samples = samples, .format = format };
}

fn sampleByteWidth(format: Format) usize {
    return @as(usize, format.bits_per_sample) / 8;
}

fn isDecodableFormat(audio_format: u16) bool {
    return audio_format == WAVE_FORMAT_PCM or
        audio_format == WAVE_FORMAT_IEEE_FLOAT or
        audio_format == WAVE_FORMAT_ALAW or
        audio_format == WAVE_FORMAT_MULAW;
}

fn decode8(byte: u8, audio_format: u16) f32 {
    return switch (audio_format) {
        WAVE_FORMAT_PCM => (@as(f32, @floatFromInt(byte)) - 128.0) / 128.0,
        WAVE_FORMAT_ALAW => @as(f32, @floatFromInt(g711.decodeALaw(byte))) / 32768.0,
        WAVE_FORMAT_MULAW => @as(f32, @floatFromInt(g711.decodeMuLaw(byte))) / 32768.0,
        else => unreachable,
    };
}

fn readI24(bytes: *const [3]u8, endian: std.builtin.Endian) i32 {
    const unsigned = switch (endian) {
        .big => (@as(u32, bytes[0]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[2],
        .little => (@as(u32, bytes[2]) << 16) | (@as(u32, bytes[1]) << 8) | bytes[0],
    };
    return @as(i32, @bitCast(unsigned << 8)) >> 8;
}

test "encode mono wav round-trips header" {
    const alloc = std.testing.allocator;
    const wav = try encodeMono(alloc, &.{ 0.0, 0.5, -0.5 }, .{
        .audio_format = 1,
        .sample_rate = 1000,
        .bits_per_sample = 16,
    });
    defer alloc.free(wav);

    const fmt = try parseFormat(wav);
    try std.testing.expectEqual(@as(u32, 1000), fmt.sample_rate);
    try std.testing.expectEqual(@as(u16, 16), fmt.bits_per_sample);
}

test "decode mono wav round-trips samples" {
    const alloc = std.testing.allocator;
    const wav = try encodeMono(alloc, &.{ 0.0, 0.5, -0.5 }, .{
        .audio_format = 1,
        .sample_rate = 1000,
        .bits_per_sample = 16,
    });
    defer alloc.free(wav);

    const decoded = try decodeMono(alloc, wav);
    defer alloc.free(decoded.samples);
    try std.testing.expectEqual(@as(u32, 1000), decoded.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 3), decoded.samples.len);
}

test "decode interleaved wav preserves stereo channels" {
    const alloc = std.testing.allocator;
    const wav = try encodeInterleaved(alloc, &.{ 1.0, -1.0, 0.5, -0.5 }, 2, .{
        .audio_format = 1,
        .sample_rate = 1000,
        .bits_per_sample = 16,
    });
    defer alloc.free(wav);

    const decoded = try decodeInterleaved(alloc, wav);
    defer alloc.free(decoded.samples);

    try std.testing.expectEqual(@as(u16, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded.samples[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[1], 0.01);
}

test "decode interleaved wav fast path handles common pcm formats" {
    const alloc = std.testing.allocator;
    const cases = [_]Format{
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 8 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 16 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 24 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 32 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 64 },
        .{ .audio_format = 3, .sample_rate = 8000, .bits_per_sample = 32 },
        .{ .audio_format = 3, .sample_rate = 8000, .bits_per_sample = 64 },
    };

    for (cases) |format| {
        const wav = try encodeInterleaved(alloc, &.{ 1.0, -1.0, 0.5, -0.5 }, 2, format);
        defer alloc.free(wav);

        const decoded = try decodeInterleaved(alloc, wav);
        defer alloc.free(decoded.samples);

        try std.testing.expectEqual(@as(u16, 2), decoded.channels);
        try std.testing.expectEqual(@as(usize, 4), decoded.samples.len);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded.samples[0], 0.01);
        try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[1], 0.01);
        try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[2], 0.01);
        try std.testing.expectApproxEqAbs(@as(f32, -0.5), decoded.samples[3], 0.01);
    }
}

test "decode wav rejects partial sample frame data" {
    const partial_stereo_pcm16 = [_]u8{
        'R',  'I',  'F',  'F',  0x27, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x04, 0x00, 0x10, 0x00,
        'd',  'a',  't',  'a',  0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    try std.testing.expectError(error.UnsupportedAudioFormat, decodeInterleaved(std.testing.allocator, &partial_stereo_pcm16));
    try std.testing.expectError(error.UnsupportedAudioFormat, decodeMono(std.testing.allocator, &partial_stereo_pcm16));
}

test "decode interleaved wav handles g711 alaw and mulaw formats" {
    const alloc = std.testing.allocator;
    const mulaw = [_]u8{
        'R',  'I',  'F',  'F',  0x28, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x07, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0x7f,
    };
    const decoded_mulaw = try decodeInterleaved(alloc, &mulaw);
    defer alloc.free(decoded_mulaw.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_MULAW), decoded_mulaw.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_mulaw.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_mulaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32124.0 / 32768.0), decoded_mulaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32124.0 / 32768.0), decoded_mulaw.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_mulaw.samples[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_mulaw.samples[3], 0.001);

    const alaw = [_]u8{
        'R',  'I',  'F',  'F',  0x28, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x06, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0x2a, 0xaa, 0xd5, 0x55,
    };
    const decoded_alaw = try decodeInterleaved(alloc, &alaw);
    defer alloc.free(decoded_alaw.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_ALAW), decoded_alaw.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_alaw.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_alaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32256.0 / 32768.0), decoded_alaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32256.0 / 32768.0), decoded_alaw.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[3], 0.001);
}

test "decode interleaved wav handles extensible g711 formats" {
    const alloc = std.testing.allocator;
    const mulaw_extensible = [_]u8{
        'R',  'I',  'F',  'F',  0x40, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x28, 0x00, 0x00, 0x00, 0xfe, 0xff, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        0x16, 0x00, 0x08, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0x7f,
    };
    const decoded_mulaw = try decodeInterleaved(alloc, &mulaw_extensible);
    defer alloc.free(decoded_mulaw.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_MULAW), decoded_mulaw.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_mulaw.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_mulaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32124.0 / 32768.0), decoded_mulaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32124.0 / 32768.0), decoded_mulaw.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_mulaw.samples[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_mulaw.samples[3], 0.001);

    const alaw_extensible = [_]u8{
        'R',  'I',  'F',  'F',  0x40, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x28, 0x00, 0x00, 0x00, 0xfe, 0xff, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        0x16, 0x00, 0x08, 0x00, 0x03, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0x2a, 0xaa, 0xd5, 0x55,
    };
    const decoded_alaw = try decodeInterleaved(alloc, &alaw_extensible);
    defer alloc.free(decoded_alaw.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_ALAW), decoded_alaw.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_alaw.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_alaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -32256.0 / 32768.0), decoded_alaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32256.0 / 32768.0), decoded_alaw.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[3], 0.001);
}

test "decode interleaved wav handles extensible pcm and float formats" {
    const alloc = std.testing.allocator;
    const pcm16_extensible = [_]u8{
        'R',  'I',  'F',  'F',  0x44, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x28, 0x00, 0x00, 0x00, 0xfe, 0xff, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x04, 0x00, 0x10, 0x00,
        0x16, 0x00, 0x10, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71,
        'd',  'a',  't',  'a',  0x08, 0x00, 0x00, 0x00, 0xff, 0x7f, 0x00, 0x80,
        0x00, 0x40, 0x00, 0xc0,
    };

    const decoded_pcm16 = try decodeInterleaved(alloc, &pcm16_extensible);
    defer alloc.free(decoded_pcm16.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_PCM), decoded_pcm16.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_pcm16.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_pcm16.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded_pcm16.samples[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm16.samples[1], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm16.samples[2], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5), decoded_pcm16.samples[3], 0.01);

    const f32_extensible = [_]u8{
        'R',  'I',  'F',  'F',  0x4c, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x28, 0x00, 0x00, 0x00, 0xfe, 0xff, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0xfa, 0x00, 0x00, 0x08, 0x00, 0x20, 0x00,
        0x16, 0x00, 0x20, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71,
        'd',  'a',  't',  'a',  0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f,
        0x00, 0x00, 0x80, 0xbf, 0x00, 0x00, 0x00, 0x3f, 0x00, 0x00, 0x00, 0xbf,
    };

    const decoded_f32 = try decodeInterleaved(alloc, &f32_extensible);
    defer alloc.free(decoded_f32.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_IEEE_FLOAT), decoded_f32.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_f32.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded_f32.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded_f32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_f32.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_f32.samples[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5), decoded_f32.samples[3], 1e-6);
}

test "decode rifx wav handles big-endian pcm and float formats" {
    const alloc = std.testing.allocator;
    const pcm16_rifx = [_]u8{
        'R',  'I',  'F',  'X',  0x00, 0x00, 0x00, 0x2c, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x00, 0x02,
        0x00, 0x00, 0x1f, 0x40, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x04, 0x00, 0x10,
        'd',  'a',  't',  'a',  0x00, 0x00, 0x00, 0x08, 0x7f, 0xff, 0x80, 0x00,
        0x40, 0x00, 0xc0, 0x00,
    };

    const decoded_pcm16 = try decodeInterleaved(alloc, &pcm16_rifx);
    defer alloc.free(decoded_pcm16.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_PCM), decoded_pcm16.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_pcm16.channels);
    try std.testing.expectEqual(@as(u32, 8000), decoded_pcm16.format.sample_rate);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded_pcm16.samples[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_pcm16.samples[1], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_pcm16.samples[2], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5), decoded_pcm16.samples[3], 0.01);

    const f32_rifx = [_]u8{
        'R',  'I',  'F',  'X',  0x00, 0x00, 0x00, 0x34, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x00, 0x00, 0x00, 0x10, 0x00, 0x03, 0x00, 0x02,
        0x00, 0x00, 0x1f, 0x40, 0x00, 0x00, 0xfa, 0x00, 0x00, 0x08, 0x00, 0x20,
        'd',  'a',  't',  'a',  0x00, 0x00, 0x00, 0x10, 0x3f, 0x80, 0x00, 0x00,
        0xbf, 0x80, 0x00, 0x00, 0x3f, 0x00, 0x00, 0x00, 0xbf, 0x00, 0x00, 0x00,
    };

    const decoded_f32 = try decodeInterleaved(alloc, &f32_rifx);
    defer alloc.free(decoded_f32.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_IEEE_FLOAT), decoded_f32.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_f32.channels);
    try std.testing.expectEqual(@as(u32, 8000), decoded_f32.format.sample_rate);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded_f32.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_f32.samples[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_f32.samples[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5), decoded_f32.samples[3], 1e-6);
}

test "decode rf64 and bw64 wav use ds64 data size" {
    const alloc = std.testing.allocator;
    const rf64 = [_]u8{
        'R',  'F',  '6',  '4',  0xff, 0xff, 0xff, 0xff, 'W',  'A',  'V',  'E',
        'd',  's',  '6',  '4',  0x1c, 0x00, 0x00, 0x00, 0x4c, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x04, 0x00, 0x10, 0x00,
        'd',  'a',  't',  'a',  0xff, 0xff, 0xff, 0xff, 0x00, 0x80, 0x00, 0x40,
    };

    const decoded = try decodeInterleaved(alloc, &rf64);
    defer alloc.free(decoded.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_PCM), decoded.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded.channels);
    try std.testing.expectEqual(@as(u32, 8000), decoded.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[1], 1e-6);

    const bw64 = [_]u8{
        'B',  'W',  '6',  '4',  0xff, 0xff, 0xff, 0xff, 'W',  'A',  'V',  'E',
        'd',  's',  '6',  '4',  0x1c, 0x00, 0x00, 0x00, 0x4c, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x04, 0x00, 0x10, 0x00,
        'd',  'a',  't',  'a',  0xff, 0xff, 0xff, 0xff, 0x00, 0x80, 0x00, 0x40,
    };

    const decoded_bw64 = try decodeInterleaved(alloc, &bw64);
    defer alloc.free(decoded_bw64.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_PCM), decoded_bw64.format.audio_format);
    try std.testing.expectEqual(@as(u16, 2), decoded_bw64.channels);
    try std.testing.expectEqual(@as(u32, 8000), decoded_bw64.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded_bw64.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_bw64.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_bw64.samples[1], 1e-6);
}

test "decode mono wav fast path handles stereo float32" {
    const alloc = std.testing.allocator;
    const wav = try encodeInterleaved(alloc, &.{ 1.0, -1.0, 0.25, 0.75 }, 2, .{
        .audio_format = 3,
        .sample_rate = 16000,
        .bits_per_sample = 32,
    });
    defer alloc.free(wav);

    const decoded = try decodeMono(alloc, wav);
    defer alloc.free(decoded.samples);

    try std.testing.expectEqual(@as(u32, 16000), decoded.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[1], 1e-6);
}

test "decode mono wav fast path handles pcm32 mono and stereo" {
    const alloc = std.testing.allocator;
    const mono_wav = try encodeMono(alloc, &.{ -1.0, 0.5 }, .{
        .audio_format = WAVE_FORMAT_PCM,
        .sample_rate = 16000,
        .bits_per_sample = 32,
    });
    defer alloc.free(mono_wav);

    const decoded_mono = try decodeMono(alloc, mono_wav);
    defer alloc.free(decoded_mono.samples);
    try std.testing.expectEqual(@as(u32, 16000), decoded_mono.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded_mono.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_mono.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_mono.samples[1], 1e-6);

    const stereo_wav = try encodeInterleaved(alloc, &.{ 1.0, -1.0, 0.25, 0.75 }, 2, .{
        .audio_format = WAVE_FORMAT_PCM,
        .sample_rate = 16000,
        .bits_per_sample = 32,
    });
    defer alloc.free(stereo_wav);

    const decoded_stereo = try decodeMono(alloc, stereo_wav);
    defer alloc.free(decoded_stereo.samples);
    try std.testing.expectEqual(@as(u32, 16000), decoded_stereo.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded_stereo.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_stereo.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_stereo.samples[1], 1e-6);
}

test "decode mono wav fast path handles pcm64 mono and stereo" {
    const alloc = std.testing.allocator;
    const mono_wav = [_]u8{
        'R',  'I',  'F',  'F',  0x34, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0xfa, 0x00, 0x00, 0x08, 0x00, 0x40, 0x00,
        'd',  'a',  't',  'a',  0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    };
    const decoded_mono = try decodeMono(alloc, &mono_wav);
    defer alloc.free(decoded_mono.samples);
    try std.testing.expectEqual(@as(u32, 8000), decoded_mono.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded_mono.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_mono.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_mono.samples[1], 1e-6);

    const stereo_wav = [_]u8{
        'R',  'I',  'F',  'F',  0x44, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x00, 0xf4, 0x01, 0x00, 0x10, 0x00, 0x40, 0x00,
        'd',  'a',  't',  'a',  0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x40,
    };
    const decoded_stereo = try decodeMono(alloc, &stereo_wav);
    defer alloc.free(decoded_stereo.samples);
    try std.testing.expectEqual(@as(u32, 8000), decoded_stereo.format.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded_stereo.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_stereo.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_stereo.samples[1], 1e-6);

    const decoded_interleaved = try decodeInterleaved(alloc, &mono_wav);
    defer alloc.free(decoded_interleaved.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_PCM), decoded_interleaved.format.audio_format);
    try std.testing.expectEqual(@as(u16, 1), decoded_interleaved.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded_interleaved.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded_interleaved.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded_interleaved.samples[1], 1e-6);
}

test "decode mono wav fast path handles pcm8 and g711 stereo" {
    const alloc = std.testing.allocator;
    const pcm8 = [_]u8{
        'R',  'I',  'F',  'F',  0x28, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0xc0, 0x40, 0xff, 0x01,
    };
    const decoded_pcm8 = try decodeMono(alloc, &pcm8);
    defer alloc.free(decoded_pcm8.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_PCM), decoded_pcm8.format.audio_format);
    try std.testing.expectEqual(@as(usize, 2), decoded_pcm8.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_pcm8.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_pcm8.samples[1], 1e-6);

    const mulaw = [_]u8{
        'R',  'I',  'F',  'F',  0x28, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x07, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0x7f,
    };
    const decoded_mulaw = try decodeMono(alloc, &mulaw);
    defer alloc.free(decoded_mulaw.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_MULAW), decoded_mulaw.format.audio_format);
    try std.testing.expectEqual(@as(usize, 2), decoded_mulaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_mulaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_mulaw.samples[1], 0.001);

    const alaw = [_]u8{
        'R',  'I',  'F',  'F',  0x28, 0x00, 0x00, 0x00, 'W',  'A',  'V',  'E',
        'f',  'm',  't',  ' ',  0x10, 0x00, 0x00, 0x00, 0x06, 0x00, 0x02, 0x00,
        0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00,
        'd',  'a',  't',  'a',  0x04, 0x00, 0x00, 0x00, 0x2a, 0xaa, 0xd5, 0x55,
    };
    const decoded_alaw = try decodeMono(alloc, &alaw);
    defer alloc.free(decoded_alaw.samples);
    try std.testing.expectEqual(@as(u16, WAVE_FORMAT_ALAW), decoded_alaw.format.audio_format);
    try std.testing.expectEqual(@as(usize, 2), decoded_alaw.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded_alaw.samples[1], 0.001);
}
