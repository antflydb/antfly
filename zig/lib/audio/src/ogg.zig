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
const flac = @import("flac.zig");

const tone_flac_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-flac.ogg");
const tone_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.ogg");
const tone_opus_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.opus");

pub const Codec = enum {
    flac,
    opus,
    vorbis,
};

pub const Packet = struct {
    bytes: []u8,
    page_granule_position: u64,
    granule_applies: bool,
    sequence: u32,
    is_bos: bool,
    is_eos: bool,
};

pub const PacketSequence = struct {
    packets: []Packet,
    serial: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *PacketSequence) void {
        for (self.packets) |packet| self.allocator.free(packet.bytes);
        self.allocator.free(self.packets);
    }
};

pub const PacketStreams = struct {
    streams: []PacketSequence,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *PacketStreams) void {
        for (self.streams) |*stream| stream.deinit();
        self.allocator.free(self.streams);
    }
};

pub fn sniffCodec(allocator: std.mem.Allocator, ogg_bytes: []const u8) !Codec {
    var packet_streams = try parsePacketStreamsAlloc(allocator, ogg_bytes);
    defer packet_streams.deinit();
    if (packet_streams.streams.len == 0) return error.UnsupportedAudioFormat;
    if (packet_streams.streams[0].packets.len == 0) return error.UnsupportedAudioFormat;
    return sniffFirstPacket(packet_streams.streams[0].packets[0].bytes);
}

pub fn reconstructFlacStreamAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) ![]u8 {
    var packets = try parsePacketsAlloc(allocator, ogg_bytes);
    defer packets.deinit();
    if (packets.packets.len == 0) return error.UnsupportedAudioFormat;

    const first_packet = packets.packets[0].bytes;
    if ((try sniffFirstPacket(first_packet)) != .flac) return error.UnsupportedAudioFormat;
    if (first_packet.len < 13 or !std.mem.eql(u8, first_packet[9..13], "fLaC")) {
        return error.UnsupportedAudioFormat;
    }

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);

    try out.appendSlice(allocator, first_packet[9..]);
    var saw_last_metadata = (first_packet[13] & 0x80) != 0;

    for (packets.packets[1..]) |packet| {
        const packet_bytes = packet.bytes;
        if (!saw_last_metadata and packet_bytes.len < 4) return error.UnsupportedAudioFormat;
        try out.appendSlice(allocator, packet_bytes);
        if (!saw_last_metadata) {
            saw_last_metadata = (packet_bytes[0] & 0x80) != 0;
        }
    }
    if (!saw_last_metadata) return error.UnsupportedAudioFormat;
    return out.toOwnedSlice(allocator);
}

pub fn decodeInterleavedFlacAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !flac.DecodedInterleaved {
    const native_flac = try reconstructFlacStreamAlloc(allocator, ogg_bytes);
    defer allocator.free(native_flac);
    return flac.decodeInterleaved(allocator, native_flac);
}

pub fn parsePacketsAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !PacketSequence {
    var packet_streams = try parsePacketStreamsAlloc(allocator, ogg_bytes);
    errdefer packet_streams.deinit();
    if (packet_streams.streams.len != 1) return error.UnsupportedAudioFormat;

    const packets = packet_streams.streams[0].packets;
    const serial = packet_streams.streams[0].serial;
    allocator.free(packet_streams.streams);
    return .{
        .packets = packets,
        .serial = serial,
        .allocator = allocator,
    };
}

pub fn parsePacketStreamsAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !PacketStreams {
    var streams = std.ArrayList(PacketSequence).empty;
    errdefer {
        for (streams.items) |*stream| stream.deinit();
        streams.deinit(allocator);
    }

    var packets = std.ArrayList(Packet).empty;
    errdefer {
        for (packets.items) |packet| allocator.free(packet.bytes);
        packets.deinit(allocator);
    }

    var current_packet = std.ArrayList(u8).empty;
    defer current_packet.deinit(allocator);

    var cursor: usize = 0;
    var expected_sequence: ?u32 = null;
    var stream_serial: ?u32 = null;
    var current_packet_page_granule: u64 = 0;
    var current_packet_sequence: u32 = 0;
    var current_packet_is_bos = false;
    var current_packet_is_eos = false;
    var saw_truncated_final_page = false;

    while (cursor < ogg_bytes.len) {
        if (cursor + 27 > ogg_bytes.len or !std.mem.eql(u8, ogg_bytes[cursor .. cursor + 4], "OggS")) {
            return error.UnsupportedAudioFormat;
        }
        if (ogg_bytes[cursor + 4] != 0) return error.UnsupportedAudioFormat;

        const header_type = ogg_bytes[cursor + 5];
        const continuation = (header_type & 0x01) != 0;
        const is_bos = (header_type & 0x02) != 0;
        const is_eos = (header_type & 0x04) != 0;
        const granule_position = readLeU64(ogg_bytes[cursor + 6 .. cursor + 14]);
        const serial = readLeU32(ogg_bytes[cursor + 14 .. cursor + 18]);
        const sequence = readLeU32(ogg_bytes[cursor + 18 .. cursor + 22]);
        const page_segments = @as(usize, ogg_bytes[cursor + 26]);
        const header_size = 27 + page_segments;
        if (cursor + header_size > ogg_bytes.len) return error.UnsupportedAudioFormat;

        if (stream_serial) |expected_serial| {
            if (expected_serial != serial) {
                if (current_packet.items.len != 0) return error.UnsupportedAudioFormat;
                if (!is_bos) return error.UnsupportedAudioFormat;
                try finishPacketStreamAlloc(allocator, &streams, &packets, expected_serial);
                stream_serial = serial;
                expected_sequence = null;
            }
        } else {
            stream_serial = serial;
        }
        if (expected_sequence) |expected| {
            if (sequence != expected) return error.UnsupportedAudioFormat;
        }
        expected_sequence = sequence + 1;

        if (current_packet.items.len != 0 and !continuation) return error.UnsupportedAudioFormat;
        if (current_packet.items.len == 0 and continuation and packets.items.len == 0) return error.UnsupportedAudioFormat;
        if (current_packet.items.len == 0) {
            current_packet_page_granule = granule_position;
            current_packet_sequence = sequence;
            current_packet_is_bos = is_bos;
            current_packet_is_eos = is_eos;
        }

        const lacing_values = ogg_bytes[cursor + 27 .. cursor + header_size];
        var page_data_len: usize = 0;
        for (lacing_values) |lace| page_data_len += lace;
        const page_data_start = cursor + header_size;
        const page_data_end = page_data_start + page_data_len;
        const truncated_page = page_data_end > ogg_bytes.len;
        if (truncated_page) {
            saw_truncated_final_page = true;
        }

        var page_data_cursor = page_data_start;
        for (lacing_values) |lace| {
            const lace_len = @as(usize, lace);
            const remaining_page_bytes = ogg_bytes.len -| page_data_cursor;
            const available_lace_len = @min(lace_len, remaining_page_bytes);
            if (available_lace_len != 0) {
                try current_packet.appendSlice(allocator, ogg_bytes[page_data_cursor .. page_data_cursor + available_lace_len]);
                page_data_cursor += available_lace_len;
            }
            current_packet_page_granule = granule_position;
            current_packet_sequence = sequence;
            current_packet_is_bos = current_packet_is_bos or is_bos;
            current_packet_is_eos = current_packet_is_eos or is_eos;
            if (available_lace_len < lace_len) break;
            if (lace < 255) {
                const granule_applies = !truncated_page and page_data_cursor == page_data_end;
                try packets.append(allocator, .{
                    .bytes = try current_packet.toOwnedSlice(allocator),
                    .page_granule_position = current_packet_page_granule,
                    .granule_applies = granule_applies,
                    .sequence = current_packet_sequence,
                    .is_bos = current_packet_is_bos,
                    .is_eos = current_packet_is_eos,
                });
                current_packet_page_granule = 0;
                current_packet_sequence = 0;
                current_packet_is_bos = false;
                current_packet_is_eos = false;
            }
        }

        if (truncated_page) {
            current_packet.clearRetainingCapacity();
            break;
        }
        cursor = page_data_end;
    }

    if (current_packet.items.len != 0) {
        if (!saw_truncated_final_page) return error.UnsupportedAudioFormat;
        current_packet.clearRetainingCapacity();
    }
    if (stream_serial) |serial| {
        try finishPacketStreamAlloc(allocator, &streams, &packets, serial);
    }
    return .{
        .streams = try streams.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

fn finishPacketStreamAlloc(
    allocator: std.mem.Allocator,
    streams: *std.ArrayList(PacketSequence),
    packets: *std.ArrayList(Packet),
    serial: u32,
) !void {
    if (packets.items.len == 0) return error.UnsupportedAudioFormat;
    try streams.append(allocator, .{
        .packets = try packets.toOwnedSlice(allocator),
        .serial = serial,
        .allocator = allocator,
    });
}

fn sniffFirstPacket(packet: []const u8) !Codec {
    if (packet.len >= 8 and std.mem.eql(u8, packet[0..8], "OpusHead")) return .opus;
    if (packet.len >= 7 and packet[0] == 0x01 and std.mem.eql(u8, packet[1..7], "vorbis")) return .vorbis;
    if (packet.len >= 9 and packet[0] == 0x7f and std.mem.eql(u8, packet[1..5], "FLAC")) return .flac;
    return error.UnsupportedAudioFormat;
}

fn readLeU32(bytes: []const u8) u32 {
    return @as(u32, bytes[0]) |
        (@as(u32, bytes[1]) << 8) |
        (@as(u32, bytes[2]) << 16) |
        (@as(u32, bytes[3]) << 24);
}

fn readLeU64(bytes: []const u8) u64 {
    return @as(u64, bytes[0]) |
        (@as(u64, bytes[1]) << 8) |
        (@as(u64, bytes[2]) << 16) |
        (@as(u64, bytes[3]) << 24) |
        (@as(u64, bytes[4]) << 32) |
        (@as(u64, bytes[5]) << 40) |
        (@as(u64, bytes[6]) << 48) |
        (@as(u64, bytes[7]) << 56);
}

test "sniff checked-in ogg codecs" {
    try std.testing.expectEqual(Codec.flac, try sniffCodec(std.testing.allocator, tone_flac_ogg_bytes));
    try std.testing.expectEqual(Codec.vorbis, try sniffCodec(std.testing.allocator, tone_ogg_bytes));
    try std.testing.expectEqual(Codec.opus, try sniffCodec(std.testing.allocator, tone_opus_bytes));
}

test "parse checked-in ogg packet metadata" {
    var packets = try parsePacketsAlloc(std.testing.allocator, tone_opus_bytes);
    defer packets.deinit();

    try std.testing.expect(packets.packets.len > 3);
    try std.testing.expect(packets.packets[0].is_bos);
    try std.testing.expectEqual(@as(u64, 0), packets.packets[0].page_granule_position);
    try std.testing.expect(std.mem.eql(u8, packets.packets[0].bytes[0..8], "OpusHead"));
    try std.testing.expect(std.mem.eql(u8, packets.packets[1].bytes[0..8], "OpusTags"));
    try std.testing.expect(!packets.packets[2].granule_applies);
    try std.testing.expect(packets.packets[packets.packets.len - 1].granule_applies);
    try std.testing.expectEqual(@as(u64, 48_312), packets.packets[packets.packets.len - 1].page_granule_position);
    try std.testing.expect(packets.packets[packets.packets.len - 1].is_eos);
}

test "reconstruct checked-in ogg flac stream" {
    const native_flac = try reconstructFlacStreamAlloc(std.testing.allocator, tone_flac_ogg_bytes);
    defer std.testing.allocator.free(native_flac);

    try std.testing.expect(std.mem.startsWith(u8, native_flac, "fLaC"));

    const info = try flac.parseStreamInfo(native_flac);
    try std.testing.expectEqual(@as(u32, 16000), info.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), info.channels);
    try std.testing.expectEqual(@as(u64, 16000), info.total_samples);
}

test "decode checked-in ogg flac fixture to interleaved pcm" {
    var decoded = try decodeInterleavedFlacAlloc(std.testing.allocator, tone_flac_ogg_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 32000), decoded.samples.len);
}
