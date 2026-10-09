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

const std = @import("std");

const tone_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.m4a");
const tone_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.mp4");

const container = @import("antfly_media").mp4_audio;
const StscEntry = container.StscEntry;
const TrackTables = container.TrackTables;
const parseTopLevel = container.parseTopLevel;
const parseMoov = container.parseMoov;
const replaceSelectedTrack = container.replaceSelectedTrack;
const parseTrak = container.parseTrak;
const parseMdia = container.parseMdia;
const parseMinf = container.parseMinf;
const parseStbl = container.parseStbl;
const parseMvhdTimescale = container.parseMvhdTimescale;
const parseMdhdTimescale = container.parseMdhdTimescale;
const parseEdts = container.parseEdts;
const parseElst = container.parseElst;
const EditListEntry = container.EditListEntry;
const parseElstEntry = container.parseElstEntry;
const parseStsd = container.parseStsd;
const parseSampleEntry = container.parseSampleEntry;
const AudioSampleEntryLayout = container.AudioSampleEntryLayout;
const parseAudioSampleEntryLayout = container.parseAudioSampleEntryLayout;
const parseStsc = container.parseStsc;
const parseStsz = container.parseStsz;
const parseStz2 = container.parseStz2;
const parseStts = container.parseStts;
const EditListTrim = container.EditListTrim;
const parseUdtaItunSmpb = container.parseUdtaItunSmpb;
const parseIlstItunSmpb = container.parseIlstItunSmpb;
const resolveEditListTrim = container.resolveEditListTrim;
const scaleDurationToMediaFrames = container.scaleDurationToMediaFrames;
const parseStco = container.parseStco;
const parseCo64 = container.parseCo64;
const buildAccessUnits = container.buildAccessUnits;
const samplesPerChunkForIndex = container.samplesPerChunkForIndex;
const parseEsdsDecoderConfig = container.parseEsdsDecoderConfig;
const parseAlacDecoderConfig = container.parseAlacDecoderConfig;
const findChildBoxRecursive = container.findChildBoxRecursive;
const findDescriptorRecursive = container.findDescriptorRecursive;
const descriptorChildren = container.descriptorChildren;
const readDescriptorSize = container.readDescriptorSize;
const isAudioHandler = container.isAudioHandler;
const Box = container.Box;
const readBox = container.readBox;
const readU16 = container.readU16;
const readU32 = container.readU32;
const readU64 = container.readU64;
const fourcc = container.fourcc;
pub const Codec = container.Codec;
pub const DemuxedAudio = container.DemuxedAudio;
pub const demux = container.demux;

test "parse edit list accepts leading empty edit before media edit" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x80, // empty edit segment_duration
        0xff, 0xff, 0xff, 0xff, // media_time = -1
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
        0x00, 0x00, 0x03, 0xe8, // media edit segment_duration
        0x00, 0x00, 0x04, 0x00, // media_time = 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
    };

    var tables = TrackTables{
        .movie_timescale = 1000,
        .media_timescale = 44100,
    };
    defer tables.deinit(std.testing.allocator);
    try parseElst(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(@as(usize, 2), tables.edit_entries.items.len);
    try std.testing.expectEqual(@as(i64, -1), tables.edit_entries.items[0].media_time);
    try std.testing.expectEqual(@as(i64, 1024), tables.edit_entries.items[1].media_time);
    try std.testing.expectEqual(@as(u64, 1000), tables.edit_entries.items[1].segment_duration);

    const trim = try resolveEditListTrim(tables);
    try std.testing.expectEqual(@as(u64, 1024), trim.trim_start_frames);
    try std.testing.expectEqual(@as(?u64, 44100), trim.playable_frames);
}

test "parse edit list accepts contiguous media edits" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x01, 0xf4, // first segment_duration
        0x00, 0x00, 0x04, 0x00, // first media_time
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
        0x00, 0x00, 0x00, 0xfa, // second segment_duration
        0x00, 0x00, 0x23, 0x40, // second media_time = 1024 + 8000
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
    };

    var tables = TrackTables{
        .movie_timescale = 1000,
        .media_timescale = 16000,
    };
    defer tables.deinit(std.testing.allocator);
    try parseElst(std.testing.allocator, &payload, &tables);

    const trim = try resolveEditListTrim(tables);
    try std.testing.expectEqual(@as(u64, 1024), trim.trim_start_frames);
    try std.testing.expectEqual(@as(?u64, 12000), trim.playable_frames);
}

test "parse edit list rejects discontiguous media edits" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x01, 0xf4, // first segment_duration
        0x00, 0x00, 0x04, 0x00, // first media_time
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
        0x00, 0x00, 0x00, 0xfa, // second segment_duration
        0x00, 0x00, 0x23, 0x41, // second media_time has a one-frame gap
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
    };

    var tables = TrackTables{
        .movie_timescale = 1000,
        .media_timescale = 16000,
    };
    defer tables.deinit(std.testing.allocator);
    try parseElst(std.testing.allocator, &payload, &tables);
    try std.testing.expectError(error.UnsupportedAudioFormat, resolveEditListTrim(tables));
}

test "parse audio sample entry accepts version 1 child boxes" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x01, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0xac, 0x44, 0x00, 0x00, // sample rate 44100, 16.16 fixed point
        0x00, 0x00, 0x00, 0x00, // samples_per_packet
        0x00, 0x00, 0x00, 0x00, // bytes_per_packet
        0x00, 0x00, 0x00, 0x00, // bytes_per_frame
        0x00, 0x00, 0x00, 0x00, // bytes_per_sample
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x12, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    try parseSampleEntry(fourcc("mp4a"), &payload, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 44100), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x10 }, tables.decoder_config);
}

test "parse audio sample entry accepts version 2 quicktime extension" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x02, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x00, // legacy channel count
        0x00, 0x10, // legacy sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x00, 0x00, 0x00, 0x00, // legacy sample rate
        0x00, 0x00, 0x00, 0x48, // size of version 2 structure
        0x40, 0xe5, 0x88, 0x80, 0x00, 0x00, 0x00, 0x00, // sample rate 44100.0
        0x00, 0x00, 0x00, 0x02, // channel count
        0x7f, 0x00, 0x00, 0x00, // always 0x7f000000
        0x00, 0x00, 0x00, 0x10, // bits per channel
        0x00, 0x00, 0x00, 0x00, // format specific flags
        0x00, 0x00, 0x00, 0x00, // bytes per audio packet
        0x00, 0x00, 0x00, 0x00, // LPCM frames per audio packet
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x12, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    try parseSampleEntry(fourcc("mp4a"), &payload, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 44100), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x10 }, tables.decoder_config);
}

test "parse audio sample entry finds esds in wave wrapper" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x18, // wave size
        'w',  'a',  'v',  'e',
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    try parseSampleEntry(fourcc("mp4a"), &payload, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 16000), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10 }, tables.decoder_config);
}

test "parse media accepts minf before audio handler" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x3e, 0x80, // timescale 16000

        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo

        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
    };

    var tables = TrackTables{};
    try parseMdia(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(@as(u32, 16000), tables.media_timescale);
    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 16000), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10 }, tables.decoder_config);
}

test "parse movie accepts movie header after audio track" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0xb4, // trak size
        't',  'r',  'a',  'k',
        0x00, 0x00, 0x00, 0x24, // edts size
        'e',  'd',  't',  's',
        0x00, 0x00, 0x00, 0x1c, // elst size
        'e',  'l',  's',  't',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x01, 0xf4, // segment_duration 500
        0x00, 0x00, 0x04, 0x00, // media_time 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0

        0x00, 0x00, 0x00, 0x88, // mdia size
        'm',  'd',  'i',  'a',
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x3e, 0x80, // timescale 16000

        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo

        0x00, 0x00, 0x00, 0x18, // mvhd size
        'm',  'v',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x03, 0xe8, // movie timescale 1000
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseMoov(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u32, 1000), tables.movie_timescale);
    try std.testing.expectEqual(@as(u32, 16000), tables.media_timescale);
    try std.testing.expectEqual(@as(usize, 1), tables.edit_entries.items.len);
    try std.testing.expectEqual(@as(i64, 1024), tables.edit_entries.items[0].media_time);
    try std.testing.expectEqual(@as(u64, 500), tables.edit_entries.items[0].segment_duration);

    const trim = try resolveEditListTrim(tables);
    try std.testing.expectEqual(@as(u64, 1024), trim.trim_start_frames);
    try std.testing.expectEqual(@as(?u64, 8000), trim.playable_frames);
}

test "parse movie skips unsupported audio track before supported track" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0xb4, // unsupported trak size
        't',  'r',  'a',  'k',
        0x00, 0x00, 0x00, 0x24, // edts size
        'e',  'd',  't',  's',
        0x00, 0x00, 0x00, 0x1c, // elst size
        'e',  'l',  's',  't',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x01, 0xf4, // segment_duration 500
        0x00, 0x00, 0x04, 0x00, // media_time 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0

        0x00, 0x00, 0x00, 0x88, // mdia size
        'm',  'd',  'i',  'a',
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x3e, 0x80, // timescale 16000
        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'x',  'x',  'x',  'x',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo

        0x00, 0x00, 0x00, 0xb4, // supported trak size
        't',  'r',  'a',  'k',
        0x00, 0x00, 0x00, 0x24, // edts size
        'e',  'd',  't',  's',
        0x00, 0x00, 0x00, 0x1c, // elst size
        'e',  'l',  's',  't',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x01, 0xf4, // segment_duration 500
        0x00, 0x00, 0x04, 0x00, // media_time 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0

        0x00, 0x00, 0x00, 0x88, // mdia size
        'm',  'd',  'i',  'a',
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x56, 0x22, // timescale 22050
        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x56, 0x22, 0x00, 0x00, // sample rate 22050, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x13, 0x90, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseMoov(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u32, 22050), tables.media_timescale);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 22050), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x13, 0x90 }, tables.decoder_config);
}

test "parse sample-to-chunk table accepts monotonic first-description entries" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x01, // first_chunk
        0x00, 0x00, 0x00, 0x02, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
        0x00, 0x00, 0x00, 0x04, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseStsc(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(@as(usize, 2), tables.stsc_entries.items.len);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[0].first_chunk);
    try std.testing.expectEqual(@as(u32, 2), tables.stsc_entries.items[0].samples_per_chunk);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[0].sample_description_index);
    try std.testing.expectEqual(@as(u32, 4), tables.stsc_entries.items[1].first_chunk);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[1].samples_per_chunk);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[1].sample_description_index);
}

test "parse sample description table uses selected entry" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x24, // unsupported sample entry size
        'x',  'x',  'x',  'x',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x34, // supported sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{ .sample_description_index = 2 };
    try parseStsd(&payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 16000), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10 }, tables.decoder_config);
}

test "parse sample-to-chunk table rejects sample-description switch" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x01, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
        0x00, 0x00, 0x00, 0x02, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x02, // sample_description_index
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseStsc(std.testing.allocator, &payload, &tables));
}

test "parse sample-to-chunk table rejects nonmonotonic chunks" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x01, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
        0x00, 0x00, 0x00, 0x01, // repeated first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseStsc(std.testing.allocator, &payload, &tables));
}

test "parse compact sample size table handles 4-bit entries" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x04, // reserved + field_size
        0x00, 0x00, 0x00, 0x03, // sample_count
        0x12, 0x30, // three packed 4-bit sample sizes
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseStz2(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, tables.sample_sizes.items);
}

test "parse compact sample size table handles 16-bit entries" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x10, // reserved + field_size
        0x00, 0x00, 0x00, 0x02, // sample_count
        0x01, 0x23, 0x45, 0x67,
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseStz2(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqualSlices(u32, &.{ 0x0123, 0x4567 }, tables.sample_sizes.items);
}

test "parse compact sample size table rejects unsupported field size" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x0c, // reserved + unsupported field_size
        0x00, 0x00, 0x00, 0x01, // sample_count
        0x00, 0x01,
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseStz2(std.testing.allocator, &payload, &tables));
}

test "demux extracts checked-in m4a fixture" {
    var demuxed = try demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(Codec.aac, demuxed.codec);
    try std.testing.expectEqual(@as(u32, 16000), demuxed.sample_rate);
    try std.testing.expectEqual(@as(u16, 2), demuxed.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10, 0x56, 0xe5, 0x00 }, demuxed.decoder_config);
    try std.testing.expect(demuxed.access_units.len > 0);
    try std.testing.expect(demuxed.access_units[0].len > 0);
}

test "demux extracts checked-in generic mp4 fixture" {
    var demuxed = try demux(std.testing.allocator, tone_mp4_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(Codec.aac, demuxed.codec);
    try std.testing.expectEqual(@as(u32, 16000), demuxed.sample_rate);
    try std.testing.expectEqual(@as(u16, 2), demuxed.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10, 0x56, 0xe5, 0x00 }, demuxed.decoder_config);
    try std.testing.expect(demuxed.access_units.len > 0);
}
