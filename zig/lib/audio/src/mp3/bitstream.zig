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

pub const MpegVersion = enum {
    mpeg1,
    mpeg2,
    mpeg25,
};

pub const Layer = enum {
    layer1,
    layer2,
    layer3,
};

pub const ChannelMode = enum {
    stereo,
    joint_stereo,
    dual_channel,
    mono,
};

pub const FrameHeader = struct {
    version: MpegVersion,
    layer: Layer,
    has_crc: bool,
    free_format: bool,
    bitrate_kbps: u16,
    sample_rate: u32,
    padding: bool,
    channel_mode: ChannelMode,
    mode_extension: u2 = 0,

    pub fn channels(self: FrameHeader) u8 {
        return if (self.channel_mode == .mono) 1 else 2;
    }

    pub fn usesMsStereo(self: FrameHeader) bool {
        return self.channel_mode == .joint_stereo and (self.mode_extension & 0b10) != 0;
    }

    pub fn usesIntensityStereo(self: FrameHeader) bool {
        return self.channel_mode == .joint_stereo and (self.mode_extension & 0b01) != 0;
    }

    pub fn samplesPerFrame(self: FrameHeader) u16 {
        return switch (self.layer) {
            .layer1 => 384,
            .layer2 => 1152,
            .layer3 => switch (self.version) {
                .mpeg1 => 1152,
                .mpeg2, .mpeg25 => 576,
            },
        };
    }

    pub fn frameLengthBytes(self: FrameHeader) !u16 {
        if (self.free_format) return error.Mp3FreeFormatLengthUnknown;
        return switch (self.layer) {
            .layer1 => blk: {
                const bitrate = @as(u64, self.bitrate_kbps) * 1000;
                const frames = ((12 * bitrate) / self.sample_rate) + @as(u64, @intFromBool(self.padding));
                break :blk @intCast(frames * 4);
            },
            .layer2 => blk: {
                const bitrate = @as(u64, self.bitrate_kbps) * 1000;
                break :blk @intCast(((144 * bitrate) / self.sample_rate) + @as(u64, @intFromBool(self.padding)));
            },
            .layer3 => blk: {
                const coeff: u64 = switch (self.version) {
                    .mpeg1 => 144,
                    .mpeg2, .mpeg25 => 72,
                };
                const bitrate = @as(u64, self.bitrate_kbps) * 1000;
                break :blk @intCast(((coeff * bitrate) / self.sample_rate) + @as(u64, @intFromBool(self.padding)));
            },
        };
    }

    pub fn minFrameLengthBytes(self: FrameHeader) u16 {
        const header_len: u16 = 4;
        const crc_len: u16 = if (self.has_crc) 2 else 0;
        return header_len + crc_len + @as(u16, @intCast(self.sideInfoLengthBytes()));
    }

    pub fn sideInfoLengthBytes(self: FrameHeader) usize {
        return switch (self.layer) {
            .layer1, .layer2 => 0,
            .layer3 => switch (self.version) {
                .mpeg1 => if (self.channels() == 1) 17 else 32,
                .mpeg2, .mpeg25 => if (self.channels() == 1) 9 else 17,
            },
        };
    }
};

pub const GranuleChannelInfo = struct {
    part2_3_length: u16,
    big_values: u16,
    global_gain: u8,
    scalefac_compress: u16,
    window_switching_flag: bool,
    block_type: u2,
    mixed_block_flag: bool,
    table_select: [3]u8,
    subblock_gain: [3]u8,
    region0_count: u8,
    region1_count: u8,
    preflag: bool,
    scalefac_scale: bool,
    count1table_select: bool,
};

pub const SideInfo = struct {
    main_data_begin: u16,
    private_bits: u8,
    scfsi: [2][4]bool,
    granules: [2][2]GranuleChannelInfo,
    granule_count: u8,
    channel_count: u8,
};

pub const Frame = struct {
    offset: usize,
    header: FrameHeader,
    frame_bytes: []const u8,
    crc_bytes: []const u8,
    side_info_bytes: []const u8,
    main_data_bytes: []const u8,

    pub fn parseSideInfo(self: Frame) !SideInfo {
        return parseLayer3SideInfo(self.header, self.side_info_bytes);
    }
};

pub const FrameIterator = struct {
    bytes: []const u8,
    cursor: usize,
    free_format_header: ?FrameHeader = null,
    free_format_base_len: ?usize = null,
    stream_header: ?FrameHeader = null,

    pub fn init(bytes: []const u8) FrameIterator {
        return .{
            .bytes = bytes,
            .cursor = skipId3v2(bytes),
        };
    }

    pub fn next(self: *FrameIterator) !?Frame {
        const frame_offset = blk: {
            if (self.stream_header) |expected| {
                if (self.cursor + 4 <= self.bytes.len and looksLikeSync(self.bytes[self.cursor], self.bytes[self.cursor + 1])) {
                    const word = std.mem.readInt(u32, self.bytes[self.cursor..][0..4], .big);
                    if (parseHeader(word)) |candidate| {
                        if (headersAreCompatibleStream(expected, candidate)) {
                            if (candidate.free_format) break :blk self.cursor;
                            const candidate_len = if (candidate.free_format)
                                self.resolveFreeFormatFrameLength(self.cursor, candidate) catch 0
                            else
                                candidate.frameLengthBytes() catch 0;
                            if (candidate_len != 0 and self.cursor + candidate_len <= self.bytes.len) {
                                if (self.cursor + candidate_len + 4 > self.bytes.len or matchesFollowingFrames(self.bytes, self.cursor, candidate, candidate_len)) {
                                    break :blk self.cursor;
                                }
                            }
                        }
                    } else |_| {}
                }
            }
            break :blk findNextFrame(self.bytes, self.cursor) orelse return null;
        };
        const header_word = std.mem.readInt(u32, self.bytes[frame_offset..][0..4], .big);
        const header = try parseHeader(header_word);
        const frame_len = if (header.free_format)
            try self.resolveFreeFormatFrameLength(frame_offset, header)
        else
            try header.frameLengthBytes();
        if (frame_offset + frame_len > self.bytes.len) return error.Mp3TruncatedFrame;

        const frame_bytes = self.bytes[frame_offset .. frame_offset + frame_len];
        const crc_len: usize = if (header.has_crc) 2 else 0;
        const side_info_len = header.sideInfoLengthBytes();
        const data_start = 4 + crc_len + side_info_len;
        if (data_start > frame_bytes.len) return error.Mp3TruncatedFrame;

        self.cursor = frame_offset + frame_len;
        self.stream_header = header;
        return .{
            .offset = frame_offset,
            .header = header,
            .frame_bytes = frame_bytes,
            .crc_bytes = frame_bytes[4 .. 4 + crc_len],
            .side_info_bytes = frame_bytes[4 + crc_len .. data_start],
            .main_data_bytes = frame_bytes[data_start..],
        };
    }

    fn resolveFreeFormatFrameLength(
        self: *FrameIterator,
        frame_offset: usize,
        header: FrameHeader,
    ) !usize {
        if (self.free_format_header) |cached_header| {
            if (self.free_format_base_len) |cached_len| {
                if (headersAreCompatibleFreeFormat(cached_header, header)) {
                    return cached_len + @as(usize, @intFromBool(header.padding));
                }
            }
        }

        const inferred = try resolveFrameLength(self.bytes, frame_offset, header);
        self.free_format_header = header;
        self.free_format_base_len = inferred - @as(usize, @intFromBool(header.padding));
        return inferred;
    }
};

const default_granule_channel = GranuleChannelInfo{
    .part2_3_length = 0,
    .big_values = 0,
    .global_gain = 0,
    .scalefac_compress = 0,
    .window_switching_flag = false,
    .block_type = 0,
    .mixed_block_flag = false,
    .table_select = .{ 0, 0, 0 },
    .subblock_gain = .{ 0, 0, 0 },
    .region0_count = 0,
    .region1_count = 0,
    .preflag = false,
    .scalefac_scale = false,
    .count1table_select = false,
};

pub fn parseLayer3SideInfo(header: FrameHeader, side_info_bytes: []const u8) !SideInfo {
    if (header.layer != .layer3) return error.Mp3UnsupportedLayer;
    if (side_info_bytes.len != header.sideInfoLengthBytes()) return error.Mp3InvalidSideInfoLength;

    var reader = BitReader{ .bytes = side_info_bytes };
    var side_info = SideInfo{
        .main_data_begin = 0,
        .private_bits = 0,
        .scfsi = .{ .{ false, false, false, false }, .{ false, false, false, false } },
        .granules = .{
            .{ default_granule_channel, default_granule_channel },
            .{ default_granule_channel, default_granule_channel },
        },
        .granule_count = if (header.version == .mpeg1) 2 else 1,
        .channel_count = header.channels(),
    };

    if (header.version == .mpeg1) {
        side_info.main_data_begin = @intCast(try reader.readBits(9));
        side_info.private_bits = @intCast(try reader.readBits(if (header.channels() == 1) 5 else 3));

        for (0..header.channels()) |ch| {
            for (0..4) |band| {
                side_info.scfsi[ch][band] = (try reader.readBits(1)) != 0;
            }
        }
    } else {
        side_info.main_data_begin = @intCast(try reader.readBits(8));
        side_info.private_bits = @intCast(try reader.readBits(if (header.channels() == 1) 1 else 2));
    }

    for (0..side_info.granule_count) |gr| {
        for (0..header.channels()) |ch| {
            side_info.granules[gr][ch] = try parseGranuleChannel(&reader, header.version);
        }
    }

    return side_info;
}

pub const BitReader = struct {
    bytes: []const u8,
    bit_offset: usize = 0,

    pub fn readBits(self: *BitReader, count: u5) !u32 {
        if (count == 0) return 0;
        if (self.bit_offset + count > self.bytes.len * 8) return error.EndOfStream;

        var result: u32 = 0;
        var remaining = count;
        while (remaining > 0) : (remaining -= 1) {
            const absolute_bit = self.bit_offset;
            const byte_index = absolute_bit / 8;
            const bit_index = 7 - (absolute_bit % 8);
            result = (result << 1) | @as(u32, (self.bytes[byte_index] >> @intCast(bit_index)) & 0x1);
            self.bit_offset += 1;
        }
        return result;
    }
};

pub fn skipId3v2(bytes: []const u8) usize {
    if (bytes.len < 10 or !std.mem.eql(u8, bytes[0..3], "ID3")) return 0;

    const size = synchsafeToU32(bytes[6..10]);
    const footer_len: usize = if ((bytes[5] & 0x10) != 0) 10 else 0;
    return @min(bytes.len, 10 + size + footer_len);
}

pub fn findNextFrame(bytes: []const u8, start: usize) ?usize {
    if (start >= bytes.len) return null;
    var search = start;
    while (findCandidateSync(bytes, search)) |i| {
        const word = std.mem.readInt(u32, bytes[i..][0..4], .big);
        const header = parseHeader(word) catch {
            search = i + 1;
            continue;
        };
        const frame_len = if (header.free_format)
            resolveFrameLength(bytes, i, header) catch {
                search = i + 1;
                continue;
            }
        else
            header.frameLengthBytes() catch {
                search = i + 1;
                continue;
            };
        if (i + frame_len > bytes.len) return null;
        if (matchesFollowingFrames(bytes, i, header, frame_len)) return i;
        search = i + 1;
    }
    return null;
}

fn findCandidateSync(bytes: []const u8, start: usize) ?usize {
    if (start >= bytes.len) return null;
    var i = start;
    while (i + 4 <= bytes.len) : (i += 1) {
        if (!looksLikeSync(bytes[i], bytes[i + 1])) continue;
        const word = std.mem.readInt(u32, bytes[i..][0..4], .big);
        if (parseHeader(word)) |_| return i else |_| {}
    }
    return null;
}

pub fn parseHeader(word: u32) !FrameHeader {
    if ((word & 0xFFE0_0000) != 0xFFE0_0000) return error.Mp3InvalidSync;

    const version_bits = @as(u2, @truncate((word >> 19) & 0x3));
    const layer_bits = @as(u2, @truncate((word >> 17) & 0x3));
    const bitrate_index = @as(u4, @truncate((word >> 12) & 0xF));
    const sample_rate_index = @as(u2, @truncate((word >> 10) & 0x3));

    const version = switch (version_bits) {
        0b00 => MpegVersion.mpeg25,
        0b10 => MpegVersion.mpeg2,
        0b11 => MpegVersion.mpeg1,
        else => return error.Mp3ReservedVersion,
    };
    const layer = switch (layer_bits) {
        0b01 => Layer.layer3,
        0b10 => Layer.layer2,
        0b11 => Layer.layer1,
        else => return error.Mp3ReservedLayer,
    };
    if (bitrate_index == 0xF) return error.Mp3InvalidBitrate;
    if (sample_rate_index == 0x3) return error.Mp3InvalidSampleRate;

    const sample_rate = sampleRate(version, sample_rate_index);
    const free_format = bitrate_index == 0;
    const bitrate_kbps = if (free_format)
        0
    else
        bitrateKbps(version, layer, bitrate_index) orelse return error.Mp3UnsupportedBitrateTable;
    const channel_mode = switch (@as(u2, @truncate((word >> 6) & 0x3))) {
        0b00 => ChannelMode.stereo,
        0b01 => ChannelMode.joint_stereo,
        0b10 => ChannelMode.dual_channel,
        0b11 => ChannelMode.mono,
    };
    const mode_extension = @as(u2, @truncate((word >> 4) & 0x3));

    return .{
        .version = version,
        .layer = layer,
        .has_crc = ((word >> 16) & 0x1) == 0,
        .free_format = free_format,
        .bitrate_kbps = bitrate_kbps,
        .sample_rate = sample_rate,
        .padding = ((word >> 9) & 0x1) != 0,
        .channel_mode = channel_mode,
        .mode_extension = mode_extension,
    };
}

fn resolveFrameLength(bytes: []const u8, frame_offset: usize, header: FrameHeader) !usize {
    if (!header.free_format) return try header.frameLengthBytes();
    if (header.layer != .layer3) return error.Mp3UnsupportedLayer;

    var search_offset = frame_offset + header.minFrameLengthBytes();
    var best_len: usize = 0;
    var best_matches: usize = 0;
    while (findCandidateSync(bytes, search_offset)) |candidate_offset| {
        const candidate_word = std.mem.readInt(u32, bytes[candidate_offset..][0..4], .big);
        const candidate_header = try parseHeader(candidate_word);
        if (!headersAreCompatibleFreeFormat(header, candidate_header)) {
            search_offset = candidate_offset + 1;
            continue;
        }

        const candidate_len = candidate_offset - frame_offset;
        if (candidate_len == 0) return error.Mp3FreeFormatLengthUnknown;
        const matches = countFreeFormatMatches(bytes, candidate_offset, candidate_len, header);
        if (matches > best_matches) {
            best_matches = matches;
            best_len = candidate_len;
        }
        search_offset = candidate_offset + 1;
    }

    if (best_matches > 0) return best_len;
    return error.Mp3FreeFormatLengthUnknown;
}

fn matchesFollowingFrames(bytes: []const u8, frame_offset: usize, header: FrameHeader, frame_len: usize) bool {
    var offset = frame_offset;
    var current = header;
    var len = frame_len;
    var matches: usize = 0;

    while (matches < 2) : (matches += 1) {
        const next_offset = offset + len;
        if (next_offset + 4 > bytes.len) return matches > 0 or frame_offset == 0;
        const next_word = std.mem.readInt(u32, bytes[next_offset..][0..4], .big);
        const next_header = parseHeader(next_word) catch return false;
        if (!headersAreCompatibleStream(current, next_header)) return false;
        offset = next_offset;
        current = next_header;
        len = if (next_header.free_format)
            return verifyFreeFormatStride(bytes, next_offset, len, next_header)
        else
            next_header.frameLengthBytes() catch return false;
    }
    return true;
}

fn headersAreCompatibleFreeFormat(expected: FrameHeader, actual: FrameHeader) bool {
    return actual.free_format and
        actual.version == expected.version and
        actual.layer == expected.layer and
        actual.has_crc == expected.has_crc and
        actual.sample_rate == expected.sample_rate and
        actual.channel_mode == expected.channel_mode and
        actual.mode_extension == expected.mode_extension;
}

fn headersAreCompatibleStream(expected: FrameHeader, actual: FrameHeader) bool {
    return actual.version == expected.version and
        actual.layer == expected.layer and
        actual.free_format == expected.free_format and
        actual.sample_rate == expected.sample_rate and
        actual.has_crc == expected.has_crc;
}

fn verifyFreeFormatStride(
    bytes: []const u8,
    candidate_offset: usize,
    candidate_len: usize,
    header: FrameHeader,
) bool {
    return countFreeFormatMatches(bytes, candidate_offset, candidate_len, header) > 0;
}

fn countFreeFormatMatches(
    bytes: []const u8,
    candidate_offset: usize,
    candidate_len: usize,
    header: FrameHeader,
) usize {
    const base_len = candidate_len - @as(usize, @intFromBool(header.padding));
    const next_word = if (candidate_offset + 4 <= bytes.len)
        std.mem.readInt(u32, bytes[candidate_offset..][0..4], .big)
    else
        return 1;
    const next_header = parseHeader(next_word) catch return 0;
    if (!headersAreCompatibleFreeFormat(header, next_header)) return 0;

    var matches: usize = 1;
    var offset = candidate_offset;
    var current_header = next_header;
    while (matches < 5) : (matches += 1) {
        const next_offset = offset + base_len + @as(usize, @intFromBool(current_header.padding));
        if (next_offset + 4 > bytes.len) break;
        const following_word = std.mem.readInt(u32, bytes[next_offset..][0..4], .big);
        const following_header = parseHeader(following_word) catch break;
        if (!headersAreCompatibleFreeFormat(header, following_header)) break;
        offset = next_offset;
        current_header = following_header;
    }
    return matches;
}

pub fn looksLikeSync(first: u8, second: u8) bool {
    if (first != 0xFF) return false;
    return (second & 0xE0) == 0xE0;
}

fn sampleRate(version: MpegVersion, sample_rate_index: u2) u32 {
    const table = switch (version) {
        .mpeg1 => [_]u32{ 44100, 48000, 32000 },
        .mpeg2 => [_]u32{ 22050, 24000, 16000 },
        .mpeg25 => [_]u32{ 11025, 12000, 8000 },
    };
    return table[sample_rate_index];
}

fn bitrateKbps(version: MpegVersion, layer: Layer, index: u4) ?u16 {
    const i = index - 1;
    return switch (layer) {
        .layer1 => switch (version) {
            .mpeg1 => blk: {
                const table = [_]u16{ 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448 };
                break :blk table[i];
            },
            .mpeg2, .mpeg25 => blk: {
                const table = [_]u16{ 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256 };
                break :blk table[i];
            },
        },
        .layer2 => switch (version) {
            .mpeg1 => blk: {
                const table = [_]u16{ 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384 };
                break :blk table[i];
            },
            .mpeg2, .mpeg25 => blk: {
                const table = [_]u16{ 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160 };
                break :blk table[i];
            },
        },
        .layer3 => switch (version) {
            .mpeg1 => blk: {
                const table = [_]u16{ 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320 };
                break :blk table[i];
            },
            .mpeg2, .mpeg25 => blk: {
                const table = [_]u16{ 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160 };
                break :blk table[i];
            },
        },
    };
}

fn synchsafeToU32(bytes: []const u8) u32 {
    std.debug.assert(bytes.len == 4);
    return (@as(u32, bytes[0] & 0x7F) << 21) |
        (@as(u32, bytes[1] & 0x7F) << 14) |
        (@as(u32, bytes[2] & 0x7F) << 7) |
        @as(u32, bytes[3] & 0x7F);
}

fn parseGranuleChannel(reader: *BitReader, version: MpegVersion) !GranuleChannelInfo {
    var info = default_granule_channel;
    info.part2_3_length = @intCast(try reader.readBits(12));
    info.big_values = @intCast(try reader.readBits(9));
    info.global_gain = @intCast(try reader.readBits(8));
    info.scalefac_compress = @intCast(try reader.readBits(if (version == .mpeg1) 4 else 9));
    info.window_switching_flag = (try reader.readBits(1)) != 0;

    if (info.window_switching_flag) {
        info.block_type = @intCast(try reader.readBits(2));
        info.mixed_block_flag = (try reader.readBits(1)) != 0;
        info.table_select[0] = @intCast(try reader.readBits(5));
        info.table_select[1] = @intCast(try reader.readBits(5));
        info.table_select[2] = 0;
        for (0..3) |i| {
            info.subblock_gain[i] = @intCast(try reader.readBits(3));
        }
        if (info.block_type == 2 and !info.mixed_block_flag) {
            info.region0_count = 8;
        } else {
            info.region0_count = 7;
        }
        info.region1_count = 20 - info.region0_count;
    } else {
        info.table_select[0] = @intCast(try reader.readBits(5));
        info.table_select[1] = @intCast(try reader.readBits(5));
        info.table_select[2] = @intCast(try reader.readBits(5));
        info.region0_count = @intCast(try reader.readBits(4));
        info.region1_count = @intCast(try reader.readBits(3));
    }

    info.preflag = if (version == .mpeg1) (try reader.readBits(1)) != 0 else false;
    info.scalefac_scale = (try reader.readBits(1)) != 0;
    info.count1table_select = (try reader.readBits(1)) != 0;
    return info;
}

test "parse layer3 header" {
    const header = try parseHeader(0xFFFB9064);
    try std.testing.expectEqual(MpegVersion.mpeg1, header.version);
    try std.testing.expectEqual(Layer.layer3, header.layer);
    try std.testing.expectEqual(@as(u32, 44100), header.sample_rate);
    try std.testing.expectEqual(@as(u16, 128), header.bitrate_kbps);
    try std.testing.expect(!header.free_format);
    try std.testing.expectEqual(@as(u2, 0b10), header.mode_extension);
}

test "parse free-format layer3 header" {
    const header = try parseHeader(0xFFFB0000);
    try std.testing.expectEqual(MpegVersion.mpeg1, header.version);
    try std.testing.expectEqual(Layer.layer3, header.layer);
    try std.testing.expectEqual(@as(u32, 44100), header.sample_rate);
    try std.testing.expectEqual(@as(u16, 0), header.bitrate_kbps);
    try std.testing.expect(header.free_format);
}

test "joint stereo helpers reflect mode extension bits" {
    const ms_only = try parseHeader(0xFFFB9064);
    try std.testing.expectEqual(ChannelMode.joint_stereo, ms_only.channel_mode);
    try std.testing.expect(ms_only.usesMsStereo());
    try std.testing.expect(!ms_only.usesIntensityStereo());

    const intensity_only = try parseHeader(0xFFFB9054);
    try std.testing.expectEqual(ChannelMode.joint_stereo, intensity_only.channel_mode);
    try std.testing.expect(!intensity_only.usesMsStereo());
    try std.testing.expect(intensity_only.usesIntensityStereo());
}

test "skip id3v2 header" {
    const bytes = [_]u8{ 'I', 'D', '3', 4, 0, 0, 0, 0, 0, 16 } ++ ([_]u8{0} ** 16);
    try std.testing.expectEqual(@as(usize, 26), skipId3v2(&bytes));
}

test "frame iterator finds first fixture frame and side info" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    var it = FrameIterator.init(fixture);
    const frame = (try it.next()).?;
    try std.testing.expectEqual(MpegVersion.mpeg2, frame.header.version);
    try std.testing.expectEqual(Layer.layer3, frame.header.layer);
    try std.testing.expectEqual(@as(u32, 16000), frame.header.sample_rate);
    try std.testing.expectEqual(@as(u16, 288), try frame.header.frameLengthBytes());
    try std.testing.expectEqual(@as(usize, 9), frame.side_info_bytes.len);
    try std.testing.expect(std.mem.startsWith(u8, frame.main_data_bytes, "Info"));

    const side_info = try frame.parseSideInfo();
    try std.testing.expectEqual(@as(u8, 1), side_info.channel_count);
    try std.testing.expectEqual(@as(u8, 1), side_info.granule_count);
}

test "frame iterator walks multiple fixture frames" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    var it = FrameIterator.init(fixture);
    var count: usize = 0;
    while (try it.next()) |frame| {
        try std.testing.expect(frame.header.layer == .layer3);
        try std.testing.expectEqual(@as(u32, 16000), frame.header.sample_rate);
        count += 1;
        if (count == 3) break;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "frame iterator infers free-format layer3 frame lengths" {
    const fixture = @embedFile("../../testdata/mp3-corpus/l3-he_free.bit");
    var it = FrameIterator.init(fixture);
    const first = (try it.next()).?;
    const second = (try it.next()).?;

    try std.testing.expect(first.header.free_format);
    try std.testing.expect(second.header.free_format);
    try std.testing.expectEqual(MpegVersion.mpeg1, first.header.version);
    try std.testing.expectEqual(Layer.layer3, first.header.layer);
    try std.testing.expectEqual(@as(u32, 44100), first.header.sample_rate);
    try std.testing.expectEqual(ChannelMode.stereo, first.header.channel_mode);
    try std.testing.expect(first.frame_bytes.len > first.header.minFrameLengthBytes());
    try std.testing.expect(@abs(@as(i32, @intCast(first.frame_bytes.len)) - @as(i32, @intCast(second.frame_bytes.len))) <= 1);

    const side_info = try first.parseSideInfo();
    try std.testing.expectEqual(@as(u8, 2), side_info.channel_count);
    try std.testing.expectEqual(@as(u8, 2), side_info.granule_count);

    var count: usize = 2;
    while (try it.next()) |_| {
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 68), count);
}
