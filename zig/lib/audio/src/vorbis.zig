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
const ogg = @import("ogg.zig");
const fast_imdct = @import("imdct.zig");

const tone_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.ogg");
const tone_oga_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.oga");

pub const IdentificationHeader = struct {
    version: u32,
    channels: u8,
    sample_rate: u32,
    bitrate_maximum: i32,
    bitrate_nominal: i32,
    bitrate_minimum: i32,
    blocksize_small: u16,
    blocksize_large: u16,
};

pub const CommentHeader = struct {
    vendor: []u8,
    user_comment_count: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *CommentHeader) void {
        self.allocator.free(self.vendor);
    }
};

pub const Codebook = struct {
    pub const DecodeNode = struct {
        child: [2]i32 = .{ -1, -1 },
        symbol: i32 = -1,
    };

    pub const LookupEntry = struct {
        bits: u8 = 0,
        symbol: u32 = 0,
    };

    dimensions: u16,
    entries: u32,
    lookup_type: u4,
    ordered: bool,
    sparse: bool,
    max_codeword_len: u8,
    codeword_lengths: []u8,
    codewords: []u32,
    decode_nodes: []DecodeNode,
    lookup: []LookupEntry,
    lookup_min_value: f32,
    lookup_delta_value: f32,
    sequence_p: bool,
    multiplicands: []u32,

    pub fn deinit(self: *const Codebook, allocator: std.mem.Allocator) void {
        if (self.codeword_lengths.len != 0) allocator.free(self.codeword_lengths);
        if (self.codewords.len != 0) allocator.free(self.codewords);
        if (self.decode_nodes.len != 0) allocator.free(self.decode_nodes);
        if (self.lookup.len != 0) allocator.free(self.lookup);
        if (self.multiplicands.len != 0) allocator.free(self.multiplicands);
    }
};

const vorbis_codebook_lookup_bits: u6 = 10;
const vorbis_codebook_lookup_size = 1 << vorbis_codebook_lookup_bits;

pub const Floor = struct {
    kind: u16,
    partition_count: u8 = 0,
    classes: [32]u8 = [_]u8{0} ** 32,
    class_dimensions: [16]u8 = [_]u8{0} ** 16,
    class_subclasses: [16]u8 = [_]u8{0} ** 16,
    class_masterbooks: [16]?u8 = [_]?u8{null} ** 16,
    subclass_books: [16][8]?u8 = [_][8]?u8{[_]?u8{null} ** 8} ** 16,
    multiplier: u8 = 0,
    rangebits: u8 = 0,
    x_list: []u32 = &.{},
    x_order: []usize = &.{},
    floor0_order: u8 = 0,
    floor0_rate: u16 = 0,
    floor0_bark_map: u16 = 0,
    floor0_amp_bits: u8 = 0,
    floor0_amp_db: u8 = 0,
    floor0_book_count: u8 = 0,
    floor0_books: [16]u8 = [_]u8{0} ** 16,

    pub fn deinit(self: *const Floor, allocator: std.mem.Allocator) void {
        if (self.x_list.len != 0) allocator.free(self.x_list);
        if (self.x_order.len != 0) allocator.free(self.x_order);
    }
};

pub const Residue = struct {
    kind: u16,
    begin: u32,
    end: u32,
    partition_size: u32,
    classifications: u8,
    classbook: u8,
    cascades: [64]u8 = [_]u8{0} ** 64,
    books: [64][8]?u8 = [_][8]?u8{[_]?u8{null} ** 8} ** 64,
};

pub const Mapping = struct {
    submaps: u8,
    coupling_steps: u8,
    magnitudes: []u8,
    angles: []u8,
    mux: []u8,
    submap_floors: []u8,
    submap_residues: []u8,

    pub fn deinit(self: *const Mapping, allocator: std.mem.Allocator) void {
        if (self.magnitudes.len != 0) allocator.free(self.magnitudes);
        if (self.angles.len != 0) allocator.free(self.angles);
        if (self.mux.len != 0) allocator.free(self.mux);
        if (self.submap_floors.len != 0) allocator.free(self.submap_floors);
        if (self.submap_residues.len != 0) allocator.free(self.submap_residues);
    }
};

pub const Mode = struct {
    block_flag: bool,
    mapping: u8,
};

pub const SetupHeader = struct {
    codebooks: []Codebook,
    floors: []Floor,
    residues: []Residue,
    mappings: []Mapping,
    modes: []Mode,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SetupHeader) void {
        for (self.codebooks) |*codebook| codebook.deinit(self.allocator);
        for (self.floors) |*floor| floor.deinit(self.allocator);
        for (self.mappings) |*mapping| mapping.deinit(self.allocator);
        self.allocator.free(self.codebooks);
        self.allocator.free(self.floors);
        self.allocator.free(self.residues);
        self.allocator.free(self.mappings);
        self.allocator.free(self.modes);
    }
};

pub const Headers = struct {
    identification: IdentificationHeader,
    comment: CommentHeader,
    setup: SetupHeader,

    pub fn deinit(self: *Headers) void {
        self.comment.deinit();
        self.setup.deinit();
    }
};

pub const AudioPacketHeader = struct {
    mode_number: u8,
    blocksize: u16,
    previous_window_flag: bool,
    next_window_flag: bool,
};

pub const DemuxedAudioPacket = struct {
    bytes: []u8,
    header: AudioPacketHeader,
    sequence: u32,
    page_granule_position: ?u64,
    decoded_sample_count: u16,
};

pub const Demuxed = struct {
    headers: Headers,
    audio_packets: []DemuxedAudioPacket,
    total_decoded_samples: u64,
    playable_samples: u64,
    discard_padding_samples: u16,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Demuxed) void {
        self.headers.deinit();
        for (self.audio_packets) |packet| self.allocator.free(packet.bytes);
        self.allocator.free(self.audio_packets);
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

const DecodedFloor = struct {
    final_y: []u16 = &.{},
    step2_flags: []bool = &.{},
    lsp_coefficients: []f32 = &.{},
    amplitude_db: f32 = 0,
    no_residue: bool,
    allocator: std.mem.Allocator,
    owns_buffers: bool = true,

    fn deinit(self: *DecodedFloor) void {
        if (!self.owns_buffers) return;
        if (self.final_y.len != 0) self.allocator.free(self.final_y);
        if (self.step2_flags.len != 0) self.allocator.free(self.step2_flags);
        if (self.lsp_coefficients.len != 0) self.allocator.free(self.lsp_coefficients);
    }
};

const DecodedPacketBlock = struct {
    samples: []f32,
    blocksize: usize,
    channels: u8,
};

const VorbisDecodeScratch = struct {
    allocator: std.mem.Allocator,
    floor_decodes: []DecodedFloor = &.{},
    no_residue: []bool = &.{},
    spectra: []f32 = &.{},
    block_samples: []f32 = &.{},
    submap_channels: []u8 = &.{},
    submap_do_not_decode: []bool = &.{},
    active_channel_indices: []u8 = &.{},
    classifications: []u8 = &.{},
    vector_buf: []f32 = &.{},
    floor_curve_y: []u16 = &.{},
    floor_order: []usize = &.{},
    floor_final_y: []u16 = &.{},
    floor_step2_flags: []bool = &.{},
    floor_raw_y: []u16 = &.{},
    floor_lsp_coefficients: []f32 = &.{},
    imdct_work: []fast_imdct.Complex = &.{},

    fn deinit(self: *VorbisDecodeScratch) void {
        self.allocator.free(self.floor_decodes);
        self.allocator.free(self.no_residue);
        self.allocator.free(self.spectra);
        self.allocator.free(self.block_samples);
        self.allocator.free(self.submap_channels);
        self.allocator.free(self.submap_do_not_decode);
        self.allocator.free(self.active_channel_indices);
        self.allocator.free(self.classifications);
        self.allocator.free(self.vector_buf);
        self.allocator.free(self.floor_curve_y);
        self.allocator.free(self.floor_order);
        self.allocator.free(self.floor_final_y);
        self.allocator.free(self.floor_step2_flags);
        self.allocator.free(self.floor_raw_y);
        self.allocator.free(self.floor_lsp_coefficients);
        self.allocator.free(self.imdct_work);
        self.* = undefined;
    }

    fn ensureFloorDecodes(self: *VorbisDecodeScratch, len: usize) ![]DecodedFloor {
        if (self.floor_decodes.len < len) self.floor_decodes = try self.allocator.realloc(self.floor_decodes, len);
        return self.floor_decodes[0..len];
    }

    fn ensureNoResidue(self: *VorbisDecodeScratch, len: usize) ![]bool {
        if (self.no_residue.len < len) self.no_residue = try self.allocator.realloc(self.no_residue, len);
        return self.no_residue[0..len];
    }

    fn ensureSpectra(self: *VorbisDecodeScratch, len: usize) ![]f32 {
        if (self.spectra.len < len) self.spectra = try self.allocator.realloc(self.spectra, len);
        return self.spectra[0..len];
    }

    fn ensureBlockSamples(self: *VorbisDecodeScratch, len: usize) ![]f32 {
        if (self.block_samples.len < len) self.block_samples = try self.allocator.realloc(self.block_samples, len);
        return self.block_samples[0..len];
    }

    fn ensureSubmapChannels(self: *VorbisDecodeScratch, len: usize) ![]u8 {
        if (self.submap_channels.len < len) self.submap_channels = try self.allocator.realloc(self.submap_channels, len);
        return self.submap_channels[0..len];
    }

    fn ensureSubmapDoNotDecode(self: *VorbisDecodeScratch, len: usize) ![]bool {
        if (self.submap_do_not_decode.len < len) self.submap_do_not_decode = try self.allocator.realloc(self.submap_do_not_decode, len);
        return self.submap_do_not_decode[0..len];
    }

    fn ensureActiveChannelIndices(self: *VorbisDecodeScratch, len: usize) ![]u8 {
        if (self.active_channel_indices.len < len) self.active_channel_indices = try self.allocator.realloc(self.active_channel_indices, len);
        return self.active_channel_indices[0..len];
    }

    fn ensureClassifications(self: *VorbisDecodeScratch, len: usize) ![]u8 {
        if (self.classifications.len < len) self.classifications = try self.allocator.realloc(self.classifications, len);
        return self.classifications[0..len];
    }

    fn ensureVectorBuf(self: *VorbisDecodeScratch, len: usize) ![]f32 {
        if (self.vector_buf.len < len) self.vector_buf = try self.allocator.realloc(self.vector_buf, len);
        return self.vector_buf[0..len];
    }

    fn ensureFloorCurveY(self: *VorbisDecodeScratch, len: usize) ![]u16 {
        if (self.floor_curve_y.len < len) self.floor_curve_y = try self.allocator.realloc(self.floor_curve_y, len);
        return self.floor_curve_y[0..len];
    }

    fn ensureFloorOrder(self: *VorbisDecodeScratch, len: usize) ![]usize {
        if (self.floor_order.len < len) self.floor_order = try self.allocator.realloc(self.floor_order, len);
        return self.floor_order[0..len];
    }

    fn ensureFloorFinalY(self: *VorbisDecodeScratch, len: usize) ![]u16 {
        if (self.floor_final_y.len < len) self.floor_final_y = try self.allocator.realloc(self.floor_final_y, len);
        return self.floor_final_y[0..len];
    }

    fn ensureFloorStep2Flags(self: *VorbisDecodeScratch, len: usize) ![]bool {
        if (self.floor_step2_flags.len < len) self.floor_step2_flags = try self.allocator.realloc(self.floor_step2_flags, len);
        return self.floor_step2_flags[0..len];
    }

    fn ensureFloorRawY(self: *VorbisDecodeScratch, len: usize) ![]u16 {
        if (self.floor_raw_y.len < len) self.floor_raw_y = try self.allocator.realloc(self.floor_raw_y, len);
        return self.floor_raw_y[0..len];
    }

    fn ensureFloorLspCoefficients(self: *VorbisDecodeScratch, len: usize) ![]f32 {
        if (self.floor_lsp_coefficients.len < len) self.floor_lsp_coefficients = try self.allocator.realloc(self.floor_lsp_coefficients, len);
        return self.floor_lsp_coefficients[0..len];
    }

    fn ensureImdctWork(self: *VorbisDecodeScratch, len: usize) ![]fast_imdct.Complex {
        if (self.imdct_work.len < len) self.imdct_work = try self.allocator.realloc(self.imdct_work, len);
        return self.imdct_work[0..len];
    }
};

const ImdctPlan = fast_imdct.Plan;

const SharedVorbisImdctPlan = struct {
    blocksize: u16 = 0,
    plan: ?ImdctPlan = null,
};

const SharedVorbisWindow = struct {
    short_blocksize: u16 = 0,
    current_blocksize: u16 = 0,
    previous_window_flag: bool = false,
    next_window_flag: bool = false,
    factors: ?[]f32 = null,
};

var shared_vorbis_plan_lock: std.atomic.Mutex = .unlocked;
var shared_vorbis_plans: [8]SharedVorbisImdctPlan = [_]SharedVorbisImdctPlan{.{}} ** 8;
var shared_vorbis_window_lock: std.atomic.Mutex = .unlocked;
var shared_vorbis_windows: [16]SharedVorbisWindow = [_]SharedVorbisWindow{.{}} ** 16;

fn sharedVorbisImdctPlan(blocksize: u16) !*const ImdctPlan {
    while (!shared_vorbis_plan_lock.tryLock()) std.atomic.spinLoopHint();
    defer shared_vorbis_plan_lock.unlock();

    for (&shared_vorbis_plans) |*entry| {
        if (entry.blocksize == blocksize) {
            if (entry.plan) |*plan| return plan;
        }
    }

    for (&shared_vorbis_plans) |*entry| {
        if (entry.blocksize == 0) {
            entry.* = .{
                .blocksize = blocksize,
                .plan = try buildImdctPlanAlloc(std.heap.page_allocator, blocksize),
            };
            if (entry.plan) |*plan| return plan;
            unreachable;
        }
    }

    return error.UnsupportedAudioFormat;
}

fn sharedVorbisWindowFactors(
    short_blocksize: u16,
    current_blocksize: u16,
    previous_window_flag: bool,
    next_window_flag: bool,
) ![]const f32 {
    while (!shared_vorbis_window_lock.tryLock()) std.atomic.spinLoopHint();
    defer shared_vorbis_window_lock.unlock();

    for (&shared_vorbis_windows) |*entry| {
        if (entry.factors) |factors| {
            if (entry.short_blocksize == short_blocksize and
                entry.current_blocksize == current_blocksize and
                entry.previous_window_flag == previous_window_flag and
                entry.next_window_flag == next_window_flag)
            {
                return factors;
            }
        }
    }

    for (&shared_vorbis_windows) |*entry| {
        if (entry.factors == null) {
            const factors = try buildVorbisWindowFactorsAlloc(
                std.heap.page_allocator,
                short_blocksize,
                current_blocksize,
                previous_window_flag,
                next_window_flag,
            );
            entry.* = .{
                .short_blocksize = short_blocksize,
                .current_blocksize = current_blocksize,
                .previous_window_flag = previous_window_flag,
                .next_window_flag = next_window_flag,
                .factors = factors,
            };
            return factors;
        }
    }

    return error.UnsupportedAudioFormat;
}

pub fn parseIdentificationHeader(packet: []const u8) !IdentificationHeader {
    if (packet.len < 30 or packet[0] != 0x01 or !std.mem.eql(u8, packet[1..7], "vorbis")) {
        return error.UnsupportedAudioFormat;
    }
    const blocksize_byte = packet[28];
    const framing_flag = packet[29];
    if (framing_flag != 1) return error.UnsupportedAudioFormat;

    const blocksize_small = @as(u16, 1) << @intCast(blocksize_byte & 0x0f);
    const blocksize_large = @as(u16, 1) << @intCast(blocksize_byte >> 4);
    if (blocksize_small < 64 or blocksize_large < blocksize_small) return error.UnsupportedAudioFormat;

    return .{
        .version = readLeU32(packet[7..11]),
        .channels = packet[11],
        .sample_rate = readLeU32(packet[12..16]),
        .bitrate_maximum = readLeI32(packet[16..20]),
        .bitrate_nominal = readLeI32(packet[20..24]),
        .bitrate_minimum = readLeI32(packet[24..28]),
        .blocksize_small = blocksize_small,
        .blocksize_large = blocksize_large,
    };
}

pub fn parseCommentHeaderAlloc(allocator: std.mem.Allocator, packet: []const u8) !CommentHeader {
    if (packet.len < 11 or packet[0] != 0x03 or !std.mem.eql(u8, packet[1..7], "vorbis")) {
        return error.UnsupportedAudioFormat;
    }

    var cursor: usize = 7;
    if (cursor + 4 > packet.len) return error.UnsupportedAudioFormat;
    const vendor_len = readLeU32(packet[cursor .. cursor + 4]);
    cursor += 4;
    if (cursor + vendor_len + 4 + 1 > packet.len) return error.UnsupportedAudioFormat;

    const vendor = try allocator.dupe(u8, packet[cursor .. cursor + vendor_len]);
    errdefer allocator.free(vendor);
    cursor += vendor_len;

    const user_comment_count = readLeU32(packet[cursor .. cursor + 4]);
    cursor += 4;

    for (0..user_comment_count) |_| {
        if (cursor + 4 > packet.len) return error.UnsupportedAudioFormat;
        const comment_len = readLeU32(packet[cursor .. cursor + 4]);
        cursor += 4;
        if (cursor + comment_len > packet.len) return error.UnsupportedAudioFormat;
        cursor += comment_len;
    }

    if (cursor >= packet.len or packet[cursor] != 1) return error.UnsupportedAudioFormat;
    return .{
        .vendor = vendor,
        .user_comment_count = user_comment_count,
        .allocator = allocator,
    };
}

pub fn parseSetupHeaderAlloc(allocator: std.mem.Allocator, packet: []const u8, channels: u8) !SetupHeader {
    if (packet.len < 8 or packet[0] != 0x05 or !std.mem.eql(u8, packet[1..7], "vorbis")) {
        return error.UnsupportedAudioFormat;
    }

    var reader = BitReader.init(packet[7..]);

    const codebook_count = @as(usize, try reader.readBits(u8, 8)) + 1;
    const codebooks = try allocator.alloc(Codebook, codebook_count);
    var parsed_codebooks: usize = 0;
    errdefer {
        for (codebooks[0..parsed_codebooks]) |*codebook| codebook.deinit(allocator);
        allocator.free(codebooks);
    }
    for (codebooks) |*codebook| {
        codebook.* = try parseCodebook(allocator, &reader);
        parsed_codebooks += 1;
    }

    const time_count = @as(usize, try reader.readBits(u8, 6)) + 1;
    for (0..time_count) |_| {
        if ((try reader.readBits(u16, 16)) != 0) return error.UnsupportedAudioFormat;
    }

    const floor_count = @as(usize, try reader.readBits(u8, 6)) + 1;
    const floors = try allocator.alloc(Floor, floor_count);
    var parsed_floors: usize = 0;
    errdefer {
        for (floors[0..parsed_floors]) |*floor| floor.deinit(allocator);
        allocator.free(floors);
    }
    for (floors) |*floor| {
        floor.* = try parseFloor(allocator, &reader, codebooks);
        parsed_floors += 1;
    }

    const residue_count = @as(usize, try reader.readBits(u8, 6)) + 1;
    const residues = try allocator.alloc(Residue, residue_count);
    errdefer allocator.free(residues);
    for (residues) |*residue| {
        residue.* = try parseResidue(&reader, codebooks);
    }

    const mapping_count = @as(usize, try reader.readBits(u8, 6)) + 1;
    const mappings = try allocator.alloc(Mapping, mapping_count);
    var parsed_mappings: usize = 0;
    errdefer {
        for (mappings[0..parsed_mappings]) |*mapping| mapping.deinit(allocator);
        allocator.free(mappings);
    }
    for (mappings) |*mapping| {
        mapping.* = try parseMapping(allocator, &reader, channels, floors.len, residues.len);
        parsed_mappings += 1;
    }

    const mode_count = @as(usize, try reader.readBits(u8, 6)) + 1;
    const modes = try allocator.alloc(Mode, mode_count);
    errdefer allocator.free(modes);
    for (modes) |*mode| {
        mode.* = try parseMode(&reader, mappings.len);
    }

    if ((try reader.readBits(u1, 1)) != 1) return error.UnsupportedAudioFormat;
    return .{
        .codebooks = codebooks,
        .floors = floors,
        .residues = residues,
        .mappings = mappings,
        .modes = modes,
        .allocator = allocator,
    };
}

pub fn parseHeadersAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !Headers {
    var packets = try ogg.parsePacketsAlloc(allocator, ogg_bytes);
    defer packets.deinit();

    return parseHeadersFromPacketsAlloc(allocator, packets.packets);
}

pub fn parseHeadersFromPacketsAlloc(allocator: std.mem.Allocator, packets: []const ogg.Packet) !Headers {
    if (packets.len < 3) return error.UnsupportedAudioFormat;
    const identification = try parseIdentificationHeader(packets[0].bytes);
    const comment = try parseCommentHeaderAlloc(allocator, packets[1].bytes);
    errdefer {
        var owned = comment;
        owned.deinit();
    }
    const setup = try parseSetupHeaderAlloc(allocator, packets[2].bytes, identification.channels);
    errdefer {
        var owned = setup;
        owned.deinit();
    }

    return .{
        .identification = identification,
        .comment = comment,
        .setup = setup,
    };
}

pub fn demuxOggAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !Demuxed {
    var packets = try ogg.parsePacketsAlloc(allocator, ogg_bytes);
    defer packets.deinit();

    return demuxPacketsAlloc(allocator, packets.packets);
}

fn demuxPacketsAlloc(allocator: std.mem.Allocator, packets: []const ogg.Packet) !Demuxed {
    const headers = try parseHeadersFromPacketsAlloc(allocator, packets);
    errdefer {
        var owned = headers;
        owned.deinit();
    }
    if (packets.len < 4) return error.UnsupportedAudioFormat;

    const source_audio_packets = packets[3..];
    var audio_packets = std.ArrayList(DemuxedAudioPacket).empty;
    errdefer {
        for (audio_packets.items) |packet| allocator.free(packet.bytes);
        audio_packets.deinit(allocator);
    }

    var total_decoded_samples: u64 = 0;
    var previous_blocksize: ?u16 = null;
    var last_granule: ?u64 = null;

    for (source_audio_packets) |packet| {
        const header = parseAudioPacketHeader(packet.bytes, headers) catch continue;
        const decoded_sample_count: u16 = if (previous_blocksize) |prev_blocksize|
            @intCast((@as(u32, prev_blocksize) + header.blocksize) / 4)
        else
            @intCast(header.blocksize / 2);

        total_decoded_samples += decoded_sample_count;
        const packet_granule = if (packet.granule_applies) packet.page_granule_position else null;
        if (packet_granule) |granule| {
            last_granule = @min(granule, total_decoded_samples);
        }

        try audio_packets.append(allocator, .{
            .bytes = try allocator.dupe(u8, packet.bytes),
            .header = header,
            .sequence = packet.sequence,
            .page_granule_position = packet_granule,
            .decoded_sample_count = decoded_sample_count,
        });
        previous_blocksize = header.blocksize;
    }

    if (audio_packets.items.len == 0) return error.UnsupportedAudioFormat;

    const playable_samples = last_granule orelse total_decoded_samples;
    const discard_padding_u64 = total_decoded_samples - playable_samples;
    if (discard_padding_u64 > std.math.maxInt(u16)) return error.UnsupportedAudioFormat;

    return .{
        .headers = headers,
        .audio_packets = try audio_packets.toOwnedSlice(allocator),
        .total_decoded_samples = total_decoded_samples,
        .playable_samples = playable_samples,
        .discard_padding_samples = @intCast(discard_padding_u64),
        .allocator = allocator,
    };
}

pub fn decodeInterleavedOggAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !DecodedInterleaved {
    var packet_streams = try ogg.parsePacketStreamsAlloc(allocator, ogg_bytes);
    defer packet_streams.deinit();

    if (packet_streams.streams.len == 0) return error.UnsupportedAudioFormat;
    if (packet_streams.streams.len == 1) {
        var demuxed = try demuxPacketsAlloc(allocator, packet_streams.streams[0].packets);
        defer demuxed.deinit();
        return decodeDemuxedInterleavedAlloc(allocator, demuxed);
    }

    var concatenated = std.ArrayList(f32).empty;
    defer concatenated.deinit(allocator);

    const decoded_streams = try allocator.alloc(DecodedInterleaved, packet_streams.streams.len);
    defer allocator.free(decoded_streams);
    var decoded_count: usize = 0;
    defer {
        for (decoded_streams[0..decoded_count]) |*decoded| decoded.deinit();
    }

    var target_sample_rate: u32 = 0;
    var target_channels: u8 = 0;
    for (packet_streams.streams, 0..) |stream, i| {
        var demuxed = try demuxPacketsAlloc(allocator, stream.packets);
        defer demuxed.deinit();

        decoded_streams[i] = try decodeDemuxedInterleavedAlloc(allocator, demuxed);
        decoded_count += 1;
        target_sample_rate = @max(target_sample_rate, decoded_streams[i].sample_rate);
        target_channels = @max(target_channels, decoded_streams[i].channels);
    }

    if (target_sample_rate == 0 or target_channels == 0) return error.UnsupportedAudioFormat;

    for (decoded_streams[0..decoded_count]) |decoded| {
        try appendNormalizedDecodedAlloc(
            allocator,
            &concatenated,
            decoded,
            target_sample_rate,
            target_channels,
        );
    }

    return .{
        .samples = try concatenated.toOwnedSlice(allocator),
        .sample_rate = target_sample_rate,
        .channels = target_channels,
        .allocator = allocator,
    };
}

fn appendNormalizedDecodedAlloc(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(f32),
    decoded: DecodedInterleaved,
    target_sample_rate: u32,
    target_channels: u8,
) !void {
    const source_frames = std.math.divExact(usize, decoded.samples.len, decoded.channels) catch return error.UnsupportedAudioFormat;
    const target_frames = if (decoded.sample_rate == target_sample_rate)
        source_frames
    else
        resampledFrameCount(source_frames, decoded.sample_rate, target_sample_rate);

    try out.ensureUnusedCapacity(allocator, target_frames * target_channels);
    if (target_frames == 0) return;

    for (0..target_frames) |target_frame| {
        const source_position = if (decoded.sample_rate == target_sample_rate)
            @as(f64, @floatFromInt(target_frame))
        else
            (@as(f64, @floatFromInt(target_frame)) * @as(f64, @floatFromInt(decoded.sample_rate))) /
                @as(f64, @floatFromInt(target_sample_rate));
        const source_index_a: usize = @min(@as(usize, @intFromFloat(@floor(source_position))), source_frames - 1);
        const source_index_b: usize = @min(source_index_a + 1, source_frames - 1);
        const frac = @as(f32, @floatCast(source_position - @floor(source_position)));

        for (0..target_channels) |target_channel| {
            const sample_a = normalizedSourceSample(decoded, source_index_a, target_channel);
            const sample_b = normalizedSourceSample(decoded, source_index_b, target_channel);
            out.appendAssumeCapacity(sample_a + (sample_b - sample_a) * frac);
        }
    }
}

fn normalizedSourceSample(decoded: DecodedInterleaved, frame_index: usize, target_channel: usize) f32 {
    const source_channels = decoded.channels;
    if (source_channels == 0) return 0;
    if (target_channel < source_channels) {
        return decoded.samples[frame_index * source_channels + target_channel];
    }
    if (source_channels == 1) {
        return decoded.samples[frame_index];
    }
    return 0;
}

fn resampledFrameCount(source_frames: usize, source_sample_rate: u32, target_sample_rate: u32) usize {
    if (source_frames == 0 or source_sample_rate == 0 or target_sample_rate == 0) return 0;
    const numerator = @as(u128, source_frames) * target_sample_rate + @divFloor(source_sample_rate, 2);
    return @intCast(@max(@as(u128, 1), @divFloor(numerator, source_sample_rate)));
}

fn decodeDemuxedInterleavedAlloc(allocator: std.mem.Allocator, demuxed: Demuxed) !DecodedInterleaved {
    const channels = demuxed.headers.identification.channels;
    if (channels == 0) return error.UnsupportedAudioFormat;
    if (demuxed.audio_packets.len == 0) return error.UnsupportedAudioFormat;

    const short_imdct_plan = try sharedVorbisImdctPlan(demuxed.headers.identification.blocksize_small);
    const long_imdct_plan = try sharedVorbisImdctPlan(demuxed.headers.identification.blocksize_large);

    const packet_centers = try allocator.alloc(usize, demuxed.audio_packets.len);
    defer allocator.free(packet_centers);

    packet_centers[0] = demuxed.audio_packets[0].header.blocksize / 2;
    for (demuxed.audio_packets[1..], 1..) |packet, i| {
        const prev_blocksize = demuxed.audio_packets[i - 1].header.blocksize;
        packet_centers[i] = packet_centers[i - 1] + (prev_blocksize + packet.header.blocksize) / 4;
    }

    const last_packet = demuxed.audio_packets[demuxed.audio_packets.len - 1];
    const total_frames = packet_centers[packet_centers.len - 1] + last_packet.header.blocksize / 2;
    const timeline = try allocator.alloc(f32, total_frames * channels);
    defer allocator.free(timeline);
    @memset(timeline, 0);

    var scratch = VorbisDecodeScratch{ .allocator = allocator };
    defer scratch.deinit();

    for (demuxed.audio_packets, 0..) |packet, packet_index| {
        const block = try decodeAudioPacketBlockAlloc(
            allocator,
            &scratch,
            demuxed.headers,
            packet.bytes,
            short_imdct_plan,
            long_imdct_plan,
        );

        try addPacketBlockToTimelineWithWindow(
            timeline,
            total_frames,
            channels,
            block,
            packet_centers[packet_index],
            demuxed.headers.identification.blocksize_small,
            packet.header,
        );
    }

    const playable_frames = std.math.cast(usize, demuxed.playable_samples) orelse return error.UnsupportedAudioFormat;
    if (playable_frames > total_frames) return error.UnsupportedAudioFormat;

    const samples = try allocator.alloc(f32, playable_frames * channels);
    @memcpy(samples, timeline[0 .. playable_frames * channels]);
    return .{
        .samples = samples,
        .sample_rate = demuxed.headers.identification.sample_rate,
        .channels = channels,
        .allocator = allocator,
    };
}

pub fn parseAudioPacketHeader(packet: []const u8, headers: Headers) !AudioPacketHeader {
    if (packet.len == 0) return error.UnsupportedAudioFormat;

    var reader = BitReader.init(packet);
    if ((try reader.readBits(u1, 1)) != 0) return error.UnsupportedAudioFormat;

    const mode_bits = ilog(headers.setup.modes.len - 1);
    const mode_number = try reader.readBits(u8, mode_bits);
    if (mode_number >= headers.setup.modes.len) return error.UnsupportedAudioFormat;

    const mode = headers.setup.modes[mode_number];
    const blocksize = if (mode.block_flag)
        headers.identification.blocksize_large
    else
        headers.identification.blocksize_small;

    const previous_window_flag = if (mode.block_flag)
        (try reader.readBits(u1, 1)) != 0
    else
        false;
    const next_window_flag = if (mode.block_flag)
        (try reader.readBits(u1, 1)) != 0
    else
        false;

    return .{
        .mode_number = mode_number,
        .blocksize = blocksize,
        .previous_window_flag = previous_window_flag,
        .next_window_flag = next_window_flag,
    };
}

fn decodeAudioPacketBlockAlloc(
    allocator: std.mem.Allocator,
    scratch: *VorbisDecodeScratch,
    headers: Headers,
    packet: []const u8,
    short_imdct_plan: *const ImdctPlan,
    long_imdct_plan: *const ImdctPlan,
) !DecodedPacketBlock {
    const packet_header = try parseAudioPacketHeader(packet, headers);
    const channels = headers.identification.channels;
    const blocksize = @as(usize, packet_header.blocksize);
    if (channels == 0 or blocksize == 0) return error.UnsupportedAudioFormat;

    var reader = BitReader.init(packet);
    _ = try reader.readBits(u1, 1);
    _ = try reader.readBits(u8, ilog(headers.setup.modes.len - 1));
    if (packet_header.blocksize == headers.identification.blocksize_large) {
        _ = try reader.readBits(u1, 1);
        _ = try reader.readBits(u1, 1);
    }

    const mapping = headers.setup.mappings[headers.setup.modes[packet_header.mode_number].mapping];
    const half_block = blocksize / 2;

    const floor_decodes = try scratch.ensureFloorDecodes(channels);
    var floor_decode_count: usize = 0;
    defer {
        for (floor_decodes[0..floor_decode_count]) |*floor_decode| floor_decode.deinit();
    }

    const no_residue = try scratch.ensureNoResidue(channels);
    var max_floor1_values: usize = 0;
    var max_floor0_order: usize = 0;
    for (0..channels) |channel| {
        const mux = mapping.mux[channel];
        const floor = headers.setup.floors[mapping.submap_floors[mux]];
        switch (floor.kind) {
            0 => max_floor0_order = @max(max_floor0_order, floor.floor0_order),
            1 => max_floor1_values = @max(max_floor1_values, floor.x_list.len),
            else => return error.UnsupportedAudioFormat,
        }
    }
    for (0..channels) |channel| {
        const mux = mapping.mux[channel];
        const floor = headers.setup.floors[mapping.submap_floors[mux]];
        floor_decodes[channel] = try decodeFloorWithScratch(
            scratch,
            channel,
            max_floor1_values,
            max_floor0_order,
            &reader,
            floor,
            headers.setup.codebooks,
        );
        no_residue[channel] = floor_decodes[channel].no_residue;
        floor_decode_count += 1;
    }

    for (0..mapping.coupling_steps) |i| {
        const magnitude = mapping.magnitudes[i];
        const angle = mapping.angles[i];
        if (!no_residue[magnitude] or !no_residue[angle]) {
            no_residue[magnitude] = false;
            no_residue[angle] = false;
        }
    }

    const spectra = try scratch.ensureSpectra(channels * half_block);
    @memset(spectra, 0);

    for (0..mapping.submaps) |submap_index| {
        var submap_channel_count: usize = 0;
        for (0..channels) |channel| {
            if (mapping.mux[channel] == submap_index) submap_channel_count += 1;
        }
        if (submap_channel_count == 0) continue;

        const submap_channels = try scratch.ensureSubmapChannels(submap_channel_count);
        const submap_do_not_decode = try scratch.ensureSubmapDoNotDecode(submap_channel_count);

        var cursor: usize = 0;
        for (0..channels) |channel| {
            if (mapping.mux[channel] != submap_index) continue;
            submap_channels[cursor] = @intCast(channel);
            submap_do_not_decode[cursor] = no_residue[channel];
            cursor += 1;
        }

        try decodeResidueAlloc(
            scratch,
            &reader,
            headers.setup.residues[mapping.submap_residues[submap_index]],
            headers.setup.codebooks,
            spectra,
            half_block,
            submap_channels,
            submap_do_not_decode,
        );
    }

    var coupling_step = mapping.coupling_steps;
    while (coupling_step > 0) {
        coupling_step -= 1;
        const magnitude = mapping.magnitudes[coupling_step];
        const angle = mapping.angles[coupling_step];
        const magnitude_channel = spectra[magnitude * half_block ..][0..half_block];
        const angle_channel = spectra[angle * half_block ..][0..half_block];
        inverseCouple(magnitude_channel, angle_channel);
    }

    const samples = try scratch.ensureBlockSamples(channels * blocksize);
    @memset(samples, 0);

    for (0..channels) |channel| {
        const output = samples[channel * blocksize ..][0..blocksize];
        const spectrum = spectra[channel * half_block ..][0..half_block];
        if (no_residue[channel]) continue;

        const floor = headers.setup.floors[mapping.submap_floors[mapping.mux[channel]]];
        try applyFloorCurveInPlace(
            allocator,
            scratch,
            spectrum,
            floor,
            floor_decodes[channel],
        );

        const imdct_plan = if (packet_header.blocksize == headers.identification.blocksize_small)
            short_imdct_plan
        else
            long_imdct_plan;
        try imdctIntoWithScratch(output, spectrum, imdct_plan.*, scratch);
    }

    return .{
        .samples = samples,
        .blocksize = blocksize,
        .channels = channels,
    };
}

fn addPacketBlockToTimelineWithWindow(
    timeline: []f32,
    total_frames: usize,
    channels: u8,
    block: DecodedPacketBlock,
    packet_center: usize,
    short_blocksize: u16,
    packet_header: AudioPacketHeader,
) !void {
    if (channels == 0 or block.channels != channels) return error.UnsupportedAudioFormat;
    if (block.samples.len != block.blocksize * channels) return error.UnsupportedAudioFormat;
    if (timeline.len != total_frames * channels) return error.UnsupportedAudioFormat;

    const factors = try sharedVorbisWindowFactors(
        short_blocksize,
        packet_header.blocksize,
        packet_header.previous_window_flag,
        packet_header.next_window_flag,
    );
    if (factors.len != block.blocksize) return error.UnsupportedAudioFormat;

    const start_frame_signed: isize =
        @as(isize, @intCast(packet_center)) - @as(isize, @intCast(block.blocksize / 2));
    for (0..channels) |channel| {
        const src = block.samples[channel * block.blocksize ..][0..block.blocksize];
        for (src, factors, 0..) |sample, factor, i| {
            const dst_frame_signed = start_frame_signed + @as(isize, @intCast(i));
            if (dst_frame_signed < 0) continue;
            const dst_frame: usize = @intCast(dst_frame_signed);
            if (dst_frame >= total_frames) break;
            timeline[dst_frame * channels + channel] += sample * factor;
        }
    }
}

fn parseCodebook(allocator: std.mem.Allocator, reader: *BitReader) !Codebook {
    if ((try reader.readBits(u32, 24)) != 0x564342) return error.UnsupportedAudioFormat;
    const dimensions = try reader.readBits(u16, 16);
    const entries = try reader.readBits(u32, 24);
    const ordered = (try reader.readBits(u1, 1)) != 0;
    var sparse = false;
    const codeword_lengths = try allocator.alloc(u8, entries);
    errdefer allocator.free(codeword_lengths);
    @memset(codeword_lengths, 0);

    if (ordered) {
        var current_entry: u32 = 0;
        var current_length: u8 = @as(u8, try reader.readBits(u8, 5)) + 1;
        while (current_entry < entries) : (current_length += 1) {
            const remaining = entries - current_entry;
            const count = try reader.readBits(u32, ilog(remaining));
            if (count > remaining) return error.UnsupportedAudioFormat;
            for (0..count) |offset| {
                codeword_lengths[current_entry + offset] = current_length;
            }
            current_entry += count;
        }
    } else {
        sparse = (try reader.readBits(u1, 1)) != 0;
        for (0..entries) |i| {
            if (sparse and (try reader.readBits(u1, 1)) == 0) continue;
            codeword_lengths[i] = @as(u8, try reader.readBits(u8, 5)) + 1;
        }
    }

    const codewords = try allocator.alloc(u32, entries);
    errdefer allocator.free(codewords);
    @memset(codewords, 0);
    const max_codeword_len = try buildCanonicalCodewords(codeword_lengths, codewords);
    const decode_nodes = try buildCodebookDecodeNodesAlloc(allocator, codeword_lengths, codewords);
    errdefer allocator.free(decode_nodes);
    const lookup = try buildCodebookLookupAlloc(allocator, codeword_lengths, codewords);
    errdefer allocator.free(lookup);

    const lookup_type = try reader.readBits(u4, 4);
    if (lookup_type > 2) return error.UnsupportedAudioFormat;
    var lookup_min_value: f32 = 0;
    var lookup_delta_value: f32 = 0;
    var sequence_p = false;
    var multiplicands: []u32 = &.{};
    if (lookup_type != 0) {
        lookup_min_value = float32Unpack(try reader.readBits(u32, 32));
        lookup_delta_value = float32Unpack(try reader.readBits(u32, 32));
        const value_bits = @as(u8, try reader.readBits(u8, 4)) + 1;
        sequence_p = (try reader.readBits(u1, 1)) != 0;
        const lookup_values = if (lookup_type == 1)
            try mapType1QuantValues(entries, dimensions)
        else
            @as(u32, dimensions) * entries;
        multiplicands = try allocator.alloc(u32, lookup_values);
        errdefer allocator.free(multiplicands);
        for (0..lookup_values) |i| {
            multiplicands[i] = try reader.readBits(u32, value_bits);
        }
    }

    return .{
        .dimensions = dimensions,
        .entries = entries,
        .lookup_type = lookup_type,
        .ordered = ordered,
        .sparse = sparse,
        .max_codeword_len = max_codeword_len,
        .codeword_lengths = codeword_lengths,
        .codewords = codewords,
        .decode_nodes = decode_nodes,
        .lookup = lookup,
        .lookup_min_value = lookup_min_value,
        .lookup_delta_value = lookup_delta_value,
        .sequence_p = sequence_p,
        .multiplicands = multiplicands,
    };
}

fn parseFloor(allocator: std.mem.Allocator, reader: *BitReader, codebooks: []const Codebook) !Floor {
    const kind = try reader.readBits(u16, 16);
    switch (kind) {
        0 => {
            const order = try reader.readBits(u8, 8);
            const rate = try reader.readBits(u16, 16);
            const bark_map = try reader.readBits(u16, 16);
            const amp_bits = try reader.readBits(u8, 6);
            const amp_db = try reader.readBits(u8, 8);
            const book_count: u8 = @as(u8, try reader.readBits(u8, 4)) + 1;
            if (order < 1 or rate < 1 or bark_map < 1 or book_count < 1) return error.UnsupportedAudioFormat;
            var floor = Floor{
                .kind = kind,
                .floor0_order = order,
                .floor0_rate = rate,
                .floor0_bark_map = bark_map,
                .floor0_amp_bits = amp_bits,
                .floor0_amp_db = amp_db,
                .floor0_book_count = book_count,
            };
            for (0..book_count) |i| {
                const book = try reader.readBits(u8, 8);
                if (book >= codebooks.len) return error.UnsupportedAudioFormat;
                if (codebooks[book].lookup_type == 0 or codebooks[book].dimensions == 0) return error.UnsupportedAudioFormat;
                floor.floor0_books[i] = book;
            }
            return floor;
        },
        1 => {
            const partition_count = try reader.readBits(u8, 5);
            var floor = Floor{ .kind = kind, .partition_count = partition_count };
            var max_class: u8 = 0;
            for (0..partition_count) |i| {
                floor.classes[i] = try reader.readBits(u8, 4);
                max_class = @max(max_class, floor.classes[i]);
            }

            for (0..@as(usize, max_class) + 1) |i| {
                floor.class_dimensions[i] = @as(u8, try reader.readBits(u8, 3)) + 1;
                const subclasses = try reader.readBits(u8, 2);
                floor.class_subclasses[i] = subclasses;
                if (subclasses > 0) {
                    const masterbook = try reader.readBits(u8, 8);
                    if (masterbook >= codebooks.len) return error.UnsupportedAudioFormat;
                    floor.class_masterbooks[i] = masterbook;
                }
                const subclass_books = @as(usize, 1) << @intCast(subclasses);
                for (0..subclass_books) |j| {
                    const raw_book = try reader.readBits(u8, 8);
                    floor.subclass_books[i][j] = if (raw_book == 255) null else blk: {
                        if (raw_book >= codebooks.len) return error.UnsupportedAudioFormat;
                        break :blk raw_book;
                    };
                }
            }

            floor.multiplier = @as(u8, try reader.readBits(u2, 2)) + 1;
            floor.rangebits = try reader.readBits(u8, 4);
            var x_list_len: usize = 2;
            for (0..partition_count) |i| {
                x_list_len += floor.class_dimensions[floor.classes[i]];
            }
            floor.x_list = try allocator.alloc(u32, x_list_len);
            errdefer allocator.free(floor.x_list);
            floor.x_order = try allocator.alloc(usize, x_list_len);
            errdefer allocator.free(floor.x_order);
            floor.x_list[0] = 0;
            floor.x_list[1] = @as(u32, 1) << @intCast(floor.rangebits);
            var x_cursor: usize = 2;
            for (0..partition_count) |i| {
                const cls = floor.classes[i];
                for (0..floor.class_dimensions[cls]) |_| {
                    floor.x_list[x_cursor] = try reader.readBits(u32, floor.rangebits);
                    x_cursor += 1;
                }
            }
            for (floor.x_order, 0..) |*slot, i| slot.* = i;
            std.sort.insertion(usize, floor.x_order, floor.x_list, lessThanX);
            return floor;
        },
        else => return error.UnsupportedAudioFormat,
    }
}

fn parseResidue(reader: *BitReader, codebooks: []const Codebook) !Residue {
    const kind = try reader.readBits(u16, 16);
    if (kind > 2) return error.UnsupportedAudioFormat;
    const begin = try reader.readBits(u32, 24);
    const end = try reader.readBits(u32, 24);
    const partition_size = (try reader.readBits(u32, 24)) + 1;
    const classifications: u8 = @as(u8, try reader.readBits(u8, 6)) + 1;
    const classbook = try reader.readBits(u8, 8);
    if (classbook >= codebooks.len) return error.UnsupportedAudioFormat;

    var residue = Residue{
        .kind = kind,
        .begin = begin,
        .end = end,
        .partition_size = partition_size,
        .classifications = classifications,
        .classbook = classbook,
    };
    for (0..classifications) |i| {
        const low_bits = try reader.readBits(u8, 3);
        const has_high_bits = (try reader.readBits(u1, 1)) != 0;
        const high_bits: u8 = if (has_high_bits) try reader.readBits(u8, 5) else 0;
        residue.cascades[i] = low_bits | (high_bits << 3);
    }

    for (0..classifications) |i| {
        for (0..8) |pass| {
            if ((residue.cascades[i] & (@as(u8, 1) << @intCast(pass))) != 0) {
                const book = try reader.readBits(u8, 8);
                if (book >= codebooks.len) return error.UnsupportedAudioFormat;
                residue.books[i][pass] = book;
            }
        }
    }

    return residue;
}

fn parseMapping(allocator: std.mem.Allocator, reader: *BitReader, channels: u8, floor_count: usize, residue_count: usize) !Mapping {
    if ((try reader.readBits(u16, 16)) != 0) return error.UnsupportedAudioFormat;
    const submaps: u8 = if ((try reader.readBits(u1, 1)) != 0)
        @as(u8, try reader.readBits(u8, 4)) + 1
    else
        1;
    const coupling_steps: u8 = if ((try reader.readBits(u1, 1)) != 0)
        @as(u8, try reader.readBits(u8, 8)) + 1
    else
        0;
    var mapping = Mapping{
        .submaps = submaps,
        .coupling_steps = coupling_steps,
        .magnitudes = try allocator.alloc(u8, coupling_steps),
        .angles = try allocator.alloc(u8, coupling_steps),
        .mux = try allocator.alloc(u8, channels),
        .submap_floors = try allocator.alloc(u8, submaps),
        .submap_residues = try allocator.alloc(u8, submaps),
    };
    errdefer mapping.deinit(allocator);

    if (coupling_steps != 0) {
        const coupling_bits = ilog(channels - 1);
        for (0..coupling_steps) |i| {
            const magnitude = try reader.readBits(u8, coupling_bits);
            const angle = try reader.readBits(u8, coupling_bits);
            if (magnitude == angle or magnitude >= channels or angle >= channels) return error.UnsupportedAudioFormat;
            mapping.magnitudes[i] = magnitude;
            mapping.angles[i] = angle;
        }
    }

    if ((try reader.readBits(u2, 2)) != 0) return error.UnsupportedAudioFormat;
    if (submaps > 1) {
        for (0..channels) |i| {
            const mux = try reader.readBits(u8, 4);
            if (mux >= submaps) return error.UnsupportedAudioFormat;
            mapping.mux[i] = mux;
        }
    } else {
        @memset(mapping.mux, 0);
    }
    for (0..submaps) |i| {
        _ = try reader.readBits(u8, 8);
        const floor = try reader.readBits(u8, 8);
        const residue = try reader.readBits(u8, 8);
        if (floor >= floor_count or residue >= residue_count) return error.UnsupportedAudioFormat;
        mapping.submap_floors[i] = floor;
        mapping.submap_residues[i] = residue;
    }

    return mapping;
}

fn parseMode(reader: *BitReader, mapping_count: usize) !Mode {
    const block_flag = (try reader.readBits(u1, 1)) != 0;
    if ((try reader.readBits(u16, 16)) != 0) return error.UnsupportedAudioFormat;
    if ((try reader.readBits(u16, 16)) != 0) return error.UnsupportedAudioFormat;
    const mapping = try reader.readBits(u8, 8);
    if (mapping >= mapping_count) return error.UnsupportedAudioFormat;
    return .{
        .block_flag = block_flag,
        .mapping = mapping,
    };
}

fn mapType1QuantValues(entries: u32, dimensions: u16) !u32 {
    var value: u32 = 1;
    while (true) {
        var acc: u64 = 1;
        for (0..dimensions) |_| {
            acc *= value;
            if (acc > entries) return value - 1;
        }
        if (acc == entries) return value;
        value = std.math.add(u32, value, 1) catch return error.UnsupportedAudioFormat;
    }
}

pub fn decodeCodebookScalar(reader: *BitReader, codebook: Codebook) !u32 {
    if (codebook.max_codeword_len == 0 or codebook.decode_nodes.len == 0) return error.UnsupportedAudioFormat;

    if (codebook.lookup.len != 0 and reader.remainingBits() >= vorbis_codebook_lookup_bits) {
        const entry = codebook.lookup[try reader.peekBits(usize, vorbis_codebook_lookup_bits)];
        if (entry.bits != 0) {
            try reader.skipBits(entry.bits);
            return entry.symbol;
        }
    }

    var node_index: usize = 0;
    var bits: u8 = 0;
    while (bits < codebook.max_codeword_len) : (bits += 1) {
        const bit = @as(usize, try reader.readBits(u1, 1));
        const next = codebook.decode_nodes[node_index].child[bit];
        if (next < 0) return error.UnsupportedAudioFormat;
        node_index = @intCast(next);
        const symbol = codebook.decode_nodes[node_index].symbol;
        if (symbol >= 0) return @intCast(symbol);
    }

    return error.UnsupportedAudioFormat;
}

pub fn decodeCodebookVectorAlloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    codebook: Codebook,
) ![]f32 {
    const entry = try decodeCodebookScalar(reader, codebook);
    return codebookEntryVectorAlloc(allocator, codebook, entry);
}

fn decodeCodebookVectorInto(
    reader: *BitReader,
    codebook: Codebook,
    out: []f32,
) !void {
    const entry = try decodeCodebookScalar(reader, codebook);
    try codebookEntryVectorInto(codebook, entry, out);
}

pub fn codebookEntryVectorAlloc(
    allocator: std.mem.Allocator,
    codebook: Codebook,
    entry: u32,
) ![]f32 {
    if (entry >= codebook.entries or codebook.lookup_type == 0) return error.UnsupportedAudioFormat;

    const dimensions: usize = codebook.dimensions;
    const out = try allocator.alloc(f32, dimensions);
    errdefer allocator.free(out);

    var last: f32 = 0;
    if (codebook.lookup_type == 1) {
        const lookup_values = codebook.multiplicands.len;
        if (lookup_values == 0) return error.UnsupportedAudioFormat;
        var divisor: u32 = 1;
        for (0..dimensions) |i| {
            const multiplicand_index = @divTrunc(entry, divisor) % @as(u32, @intCast(lookup_values));
            const value = codebook.lookup_min_value +
                codebook.lookup_delta_value * @as(f32, @floatFromInt(codebook.multiplicands[multiplicand_index])) +
                last;
            out[i] = value;
            if (codebook.sequence_p) last = value;
            divisor *= @intCast(lookup_values);
        }
        return out;
    }

    const base = @as(usize, @intCast(entry)) * dimensions;
    if (base + dimensions > codebook.multiplicands.len) return error.UnsupportedAudioFormat;
    for (0..dimensions) |i| {
        const value = codebook.lookup_min_value +
            codebook.lookup_delta_value * @as(f32, @floatFromInt(codebook.multiplicands[base + i])) +
            last;
        out[i] = value;
        if (codebook.sequence_p) last = value;
    }
    return out;
}

fn codebookEntryVectorInto(codebook: Codebook, entry: u32, out: []f32) !void {
    if (entry >= codebook.entries or codebook.lookup_type == 0) return error.UnsupportedAudioFormat;
    const dimensions: usize = codebook.dimensions;
    if (out.len < dimensions) return error.UnsupportedAudioFormat;

    var last: f32 = 0;
    if (codebook.lookup_type == 1) {
        const lookup_values = codebook.multiplicands.len;
        if (lookup_values == 0) return error.UnsupportedAudioFormat;
        var divisor: u32 = 1;
        for (0..dimensions) |i| {
            const multiplicand_index = @divTrunc(entry, divisor) % @as(u32, @intCast(lookup_values));
            const value = codebook.lookup_min_value +
                codebook.lookup_delta_value * @as(f32, @floatFromInt(codebook.multiplicands[multiplicand_index])) +
                last;
            out[i] = value;
            if (codebook.sequence_p) last = value;
            divisor *= @intCast(lookup_values);
        }
        return;
    }

    const base = @as(usize, @intCast(entry)) * dimensions;
    if (base + dimensions > codebook.multiplicands.len) return error.UnsupportedAudioFormat;
    for (0..dimensions) |i| {
        const value = codebook.lookup_min_value +
            codebook.lookup_delta_value * @as(f32, @floatFromInt(codebook.multiplicands[base + i])) +
            last;
        out[i] = value;
        if (codebook.sequence_p) last = value;
    }
}

fn decodeFloorAlloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    floor: Floor,
    codebooks: []const Codebook,
) !DecodedFloor {
    return switch (floor.kind) {
        0 => decodeFloor0Alloc(allocator, reader, floor, codebooks),
        1 => decodeFloor1Alloc(allocator, reader, floor, codebooks),
        else => error.UnsupportedAudioFormat,
    };
}

fn decodeFloorWithScratch(
    scratch: *VorbisDecodeScratch,
    channel: usize,
    floor1_stride: usize,
    floor0_stride: usize,
    reader: *BitReader,
    floor: Floor,
    codebooks: []const Codebook,
) !DecodedFloor {
    return switch (floor.kind) {
        0 => decodeFloor0WithScratch(scratch, channel, floor0_stride, reader, floor, codebooks),
        1 => decodeFloor1WithScratch(scratch, channel, floor1_stride, reader, floor, codebooks),
        else => error.UnsupportedAudioFormat,
    };
}

fn decodeFloor0Alloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    floor: Floor,
    codebooks: []const Codebook,
) !DecodedFloor {
    const amp_raw = reader.readBits(u32, floor.floor0_amp_bits) catch {
        return .{
            .no_residue = true,
            .allocator = allocator,
        };
    };
    if (amp_raw == 0) {
        return .{
            .no_residue = true,
            .allocator = allocator,
        };
    }

    const max_amp_value = (@as(u32, 1) << @intCast(floor.floor0_amp_bits)) - 1;
    if (max_amp_value == 0) return error.UnsupportedAudioFormat;
    const amplitude_db =
        (@as(f32, @floatFromInt(amp_raw)) / @as(f32, @floatFromInt(max_amp_value))) *
        @as(f32, @floatFromInt(floor.floor0_amp_db));
    const book_num = reader.readBits(u8, ilog(floor.floor0_book_count)) catch {
        return .{
            .no_residue = true,
            .allocator = allocator,
        };
    };
    if (book_num >= floor.floor0_book_count) {
        return .{
            .no_residue = true,
            .allocator = allocator,
        };
    }
    const codebook = codebooks[floor.floor0_books[book_num]];
    const dimensions = @as(usize, codebook.dimensions);
    if (dimensions == 0) return error.UnsupportedAudioFormat;

    const lsp_coefficients = try allocator.alloc(f32, floor.floor0_order);
    errdefer allocator.free(lsp_coefficients);
    @memset(lsp_coefficients, 0);

    const vector_buf = try allocator.alloc(f32, dimensions);
    defer allocator.free(vector_buf);

    var last: f32 = 0;
    var coefficient_index: usize = 0;
    while (coefficient_index < floor.floor0_order) {
        decodeCodebookVectorInto(reader, codebook, vector_buf[0..dimensions]) catch {
            return .{
                .no_residue = true,
                .allocator = allocator,
            };
        };
        var dimension_index: usize = 0;
        while (coefficient_index < floor.floor0_order and dimension_index < dimensions) : ({
            coefficient_index += 1;
            dimension_index += 1;
        }) {
            lsp_coefficients[coefficient_index] = vector_buf[dimension_index] + last;
        }
        last = lsp_coefficients[coefficient_index - 1];
    }

    return .{
        .lsp_coefficients = lsp_coefficients,
        .amplitude_db = amplitude_db,
        .no_residue = false,
        .allocator = allocator,
    };
}

fn decodeFloor0WithScratch(
    scratch: *VorbisDecodeScratch,
    channel: usize,
    floor0_stride: usize,
    reader: *BitReader,
    floor: Floor,
    codebooks: []const Codebook,
) !DecodedFloor {
    const allocator = scratch.allocator;
    const amp_raw = reader.readBits(u32, floor.floor0_amp_bits) catch {
        return .{
            .no_residue = true,
            .allocator = allocator,
            .owns_buffers = false,
        };
    };
    if (amp_raw == 0) {
        return .{
            .no_residue = true,
            .allocator = allocator,
            .owns_buffers = false,
        };
    }

    const max_amp_value = (@as(u32, 1) << @intCast(floor.floor0_amp_bits)) - 1;
    if (max_amp_value == 0) return error.UnsupportedAudioFormat;
    const amplitude_db =
        (@as(f32, @floatFromInt(amp_raw)) / @as(f32, @floatFromInt(max_amp_value))) *
        @as(f32, @floatFromInt(floor.floor0_amp_db));
    const book_num = reader.readBits(u8, ilog(floor.floor0_book_count)) catch {
        return .{
            .no_residue = true,
            .allocator = allocator,
            .owns_buffers = false,
        };
    };
    if (book_num >= floor.floor0_book_count) {
        return .{
            .no_residue = true,
            .allocator = allocator,
            .owns_buffers = false,
        };
    }
    const codebook = codebooks[floor.floor0_books[book_num]];
    const dimensions = @as(usize, codebook.dimensions);
    if (dimensions == 0) return error.UnsupportedAudioFormat;
    if (floor0_stride < floor.floor0_order) return error.UnsupportedAudioFormat;

    const lsp_storage = try scratch.ensureFloorLspCoefficients((channel + 1) * floor0_stride);
    const lsp_coefficients = lsp_storage[channel * floor0_stride ..][0..floor.floor0_order];
    @memset(lsp_coefficients, 0);
    const vector_buf = try scratch.ensureVectorBuf(dimensions);

    var last: f32 = 0;
    var coefficient_index: usize = 0;
    while (coefficient_index < floor.floor0_order) {
        decodeCodebookVectorInto(reader, codebook, vector_buf[0..dimensions]) catch {
            return .{
                .no_residue = true,
                .allocator = allocator,
                .owns_buffers = false,
            };
        };
        var dimension_index: usize = 0;
        while (coefficient_index < floor.floor0_order and dimension_index < dimensions) : ({
            coefficient_index += 1;
            dimension_index += 1;
        }) {
            lsp_coefficients[coefficient_index] = vector_buf[dimension_index] + last;
        }
        last = lsp_coefficients[coefficient_index - 1];
    }

    return .{
        .lsp_coefficients = lsp_coefficients,
        .amplitude_db = amplitude_db,
        .no_residue = false,
        .allocator = allocator,
        .owns_buffers = false,
    };
}

fn decodeFloor1Alloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    floor: Floor,
    codebooks: []const Codebook,
) !DecodedFloor {
    if (floor.kind != 1) return error.UnsupportedAudioFormat;

    const values = floor.x_list.len;
    const final_y = try allocator.alloc(u16, values);
    errdefer allocator.free(final_y);
    @memset(final_y, 0);

    const step2_flags = try allocator.alloc(bool, values);
    errdefer allocator.free(step2_flags);
    @memset(step2_flags, false);

    const floor_present = reader.readBits(u1, 1) catch return .{
        .final_y = final_y,
        .step2_flags = step2_flags,
        .no_residue = true,
        .allocator = allocator,
    };
    if (floor_present == 0) {
        return .{
            .final_y = final_y,
            .step2_flags = step2_flags,
            .no_residue = true,
            .allocator = allocator,
        };
    }

    const floor_ranges = [_]u16{ 256, 128, 86, 64 };
    const range = floor_ranges[floor.multiplier - 1];
    const range_i32 = @as(i32, range);
    const raw_y = try allocator.alloc(u16, values);
    defer allocator.free(raw_y);
    @memset(raw_y, 0);

    const range_bits = ilog(range - 1);
    raw_y[0] = reader.readBits(u16, range_bits) catch return .{
        .final_y = final_y,
        .step2_flags = step2_flags,
        .no_residue = true,
        .allocator = allocator,
    };
    raw_y[1] = reader.readBits(u16, range_bits) catch return .{
        .final_y = final_y,
        .step2_flags = step2_flags,
        .no_residue = true,
        .allocator = allocator,
    };

    var cursor: usize = 2;
    for (0..floor.partition_count) |partition_index| {
        const cls = floor.classes[partition_index];
        const dimensions = floor.class_dimensions[cls];
        const subclass_bits = floor.class_subclasses[cls];
        var cval: u32 = 0;
        if (subclass_bits > 0) {
            const masterbook = floor.class_masterbooks[cls] orelse return error.UnsupportedAudioFormat;
            cval = decodeCodebookScalar(reader, codebooks[masterbook]) catch return .{
                .final_y = final_y,
                .step2_flags = step2_flags,
                .no_residue = true,
                .allocator = allocator,
            };
        }
        const subclass_mask: u32 = (@as(u32, 1) << @intCast(subclass_bits)) - 1;
        for (0..dimensions) |_| {
            if (cursor >= values) return error.UnsupportedAudioFormat;
            const subclass = cval & subclass_mask;
            cval >>= @intCast(subclass_bits);
            if (floor.subclass_books[cls][subclass]) |book| {
                raw_y[cursor] = @intCast(decodeCodebookScalar(reader, codebooks[book]) catch return .{
                    .final_y = final_y,
                    .step2_flags = step2_flags,
                    .no_residue = true,
                    .allocator = allocator,
                });
            }
            cursor += 1;
        }
    }
    if (cursor != values) return error.UnsupportedAudioFormat;

    step2_flags[0] = true;
    step2_flags[1] = true;
    final_y[0] = raw_y[0];
    final_y[1] = raw_y[1];
    for (2..values) |i| {
        const low_neighbor_offset = lowNeighbor(floor.x_list, i);
        const high_neighbor_offset = highNeighbor(floor.x_list, i);
        const predicted = renderPoint(
            floor.x_list[low_neighbor_offset],
            final_y[low_neighbor_offset],
            floor.x_list[high_neighbor_offset],
            final_y[high_neighbor_offset],
            floor.x_list[i],
        );
        const val = raw_y[i];
        const predicted_i32 = std.math.clamp(@as(i32, predicted), 0, range_i32 - 1);
        const highroom: i32 = @max(0, range_i32 - predicted_i32);
        const lowroom: i32 = @max(0, predicted_i32);
        const room: i32 = if (highroom < lowroom) highroom * 2 else lowroom * 2;
        if (val != 0) {
            step2_flags[low_neighbor_offset] = true;
            step2_flags[high_neighbor_offset] = true;
            step2_flags[i] = true;
            if (@as(i32, val) >= room) {
                const resolved_y_raw: i32 = if (highroom > lowroom)
                    @as(i32, val) - lowroom + predicted_i32
                else
                    predicted_i32 - @as(i32, val) + highroom - 1;
                const resolved_y = std.math.clamp(resolved_y_raw, 0, range_i32 - 1);
                final_y[i] = @intCast(resolved_y);
            } else {
                const resolved_y_raw: i32 = if ((val & 1) != 0)
                    predicted_i32 - @as(i32, (val + 1) / 2)
                else
                    predicted_i32 + @as(i32, val / 2);
                const resolved_y = std.math.clamp(resolved_y_raw, 0, range_i32 - 1);
                final_y[i] = @intCast(resolved_y);
            }
        } else {
            step2_flags[i] = false;
            final_y[i] = @intCast(predicted_i32);
        }
    }

    return .{
        .final_y = final_y,
        .step2_flags = step2_flags,
        .no_residue = false,
        .allocator = allocator,
    };
}

fn decodeFloor1WithScratch(
    scratch: *VorbisDecodeScratch,
    channel: usize,
    floor1_stride: usize,
    reader: *BitReader,
    floor: Floor,
    codebooks: []const Codebook,
) !DecodedFloor {
    if (floor.kind != 1) return error.UnsupportedAudioFormat;
    if (floor1_stride < floor.x_list.len) return error.UnsupportedAudioFormat;

    const allocator = scratch.allocator;
    const values = floor.x_list.len;
    const base = channel * floor1_stride;
    const final_y_storage = try scratch.ensureFloorFinalY(base + floor1_stride);
    const step2_storage = try scratch.ensureFloorStep2Flags(base + floor1_stride);
    const raw_y_storage = try scratch.ensureFloorRawY(base + floor1_stride);
    const final_y = final_y_storage[base..][0..values];
    const step2_flags = step2_storage[base..][0..values];
    const raw_y = raw_y_storage[base..][0..values];
    @memset(final_y, 0);
    @memset(step2_flags, false);
    @memset(raw_y, 0);

    const no_residue = DecodedFloor{
        .final_y = final_y,
        .step2_flags = step2_flags,
        .no_residue = true,
        .allocator = allocator,
        .owns_buffers = false,
    };

    const floor_present = reader.readBits(u1, 1) catch return no_residue;
    if (floor_present == 0) return no_residue;

    const floor_ranges = [_]u16{ 256, 128, 86, 64 };
    const range = floor_ranges[floor.multiplier - 1];
    const range_i32 = @as(i32, range);
    const range_bits = ilog(range - 1);
    raw_y[0] = reader.readBits(u16, range_bits) catch return no_residue;
    raw_y[1] = reader.readBits(u16, range_bits) catch return no_residue;

    var cursor: usize = 2;
    for (0..floor.partition_count) |partition_index| {
        const cls = floor.classes[partition_index];
        const dimensions = floor.class_dimensions[cls];
        const subclass_bits = floor.class_subclasses[cls];
        var cval: u32 = 0;
        if (subclass_bits > 0) {
            const masterbook = floor.class_masterbooks[cls] orelse return error.UnsupportedAudioFormat;
            cval = decodeCodebookScalar(reader, codebooks[masterbook]) catch return no_residue;
        }
        const subclass_mask: u32 = (@as(u32, 1) << @intCast(subclass_bits)) - 1;
        for (0..dimensions) |_| {
            if (cursor >= values) return error.UnsupportedAudioFormat;
            const subclass = cval & subclass_mask;
            cval >>= @intCast(subclass_bits);
            if (floor.subclass_books[cls][subclass]) |book| {
                raw_y[cursor] = @intCast(decodeCodebookScalar(reader, codebooks[book]) catch return no_residue);
            }
            cursor += 1;
        }
    }
    if (cursor != values) return error.UnsupportedAudioFormat;

    step2_flags[0] = true;
    step2_flags[1] = true;
    final_y[0] = raw_y[0];
    final_y[1] = raw_y[1];
    for (2..values) |i| {
        const low_neighbor_offset = lowNeighbor(floor.x_list, i);
        const high_neighbor_offset = highNeighbor(floor.x_list, i);
        const predicted = renderPoint(
            floor.x_list[low_neighbor_offset],
            final_y[low_neighbor_offset],
            floor.x_list[high_neighbor_offset],
            final_y[high_neighbor_offset],
            floor.x_list[i],
        );
        const val = raw_y[i];
        const predicted_i32 = std.math.clamp(@as(i32, predicted), 0, range_i32 - 1);
        const highroom: i32 = @max(0, range_i32 - predicted_i32);
        const lowroom: i32 = @max(0, predicted_i32);
        const room: i32 = if (highroom < lowroom) highroom * 2 else lowroom * 2;
        if (val != 0) {
            step2_flags[low_neighbor_offset] = true;
            step2_flags[high_neighbor_offset] = true;
            step2_flags[i] = true;
            if (@as(i32, val) >= room) {
                const resolved_y_raw: i32 = if (highroom > lowroom)
                    @as(i32, val) - lowroom + predicted_i32
                else
                    predicted_i32 - @as(i32, val) + highroom - 1;
                const resolved_y = std.math.clamp(resolved_y_raw, 0, range_i32 - 1);
                final_y[i] = @intCast(resolved_y);
            } else {
                const resolved_y_raw: i32 = if ((val & 1) != 0)
                    predicted_i32 - @as(i32, (val + 1) / 2)
                else
                    predicted_i32 + @as(i32, val / 2);
                const resolved_y = std.math.clamp(resolved_y_raw, 0, range_i32 - 1);
                final_y[i] = @intCast(resolved_y);
            }
        } else {
            step2_flags[i] = false;
            final_y[i] = @intCast(predicted_i32);
        }
    }

    return .{
        .final_y = final_y,
        .step2_flags = step2_flags,
        .no_residue = false,
        .allocator = allocator,
        .owns_buffers = false,
    };
}

fn buildFloorCurveAlloc(
    allocator: std.mem.Allocator,
    n: usize,
    floor: Floor,
    decoded: DecodedFloor,
) ![]f32 {
    return switch (floor.kind) {
        0 => buildFloor0CurveAlloc(allocator, n, floor, decoded),
        1 => buildFloor1CurveAlloc(allocator, n, floor, decoded),
        else => error.UnsupportedAudioFormat,
    };
}

fn applyFloorCurveInPlace(
    allocator: std.mem.Allocator,
    scratch: *VorbisDecodeScratch,
    spectrum: []f32,
    floor: Floor,
    decoded: DecodedFloor,
) !void {
    return switch (floor.kind) {
        0 => {
            const curve = try buildFloor0CurveAlloc(allocator, spectrum.len, floor, decoded);
            defer allocator.free(curve);
            for (spectrum, curve) |*value, floor_value| value.* *= floor_value;
        },
        1 => applyFloor1CurveInPlace(scratch, spectrum, floor, decoded),
        else => error.UnsupportedAudioFormat,
    };
}

fn buildFloor1CurveAlloc(
    allocator: std.mem.Allocator,
    n: usize,
    floor: Floor,
    decoded: DecodedFloor,
) ![]f32 {
    if (floor.kind != 1) return error.UnsupportedAudioFormat;
    const curve_y = try allocator.alloc(u16, n);
    defer allocator.free(curve_y);
    @memset(curve_y, 0);

    var owned_order: []usize = &.{};
    defer if (owned_order.len != 0) allocator.free(owned_order);
    const order = if (floor.x_order.len == floor.x_list.len)
        floor.x_order
    else blk: {
        owned_order = try allocator.alloc(usize, floor.x_list.len);
        for (owned_order, 0..) |*slot, i| slot.* = i;
        std.sort.insertion(usize, owned_order, floor.x_list, lessThanX);
        break :blk owned_order;
    };

    var hx: usize = 0;
    var hy: u16 = decoded.final_y[order[0]] * floor.multiplier;
    var lx: usize = 0;
    var ly: u16 = hy;
    for (order[1..]) |index| {
        if (!decoded.step2_flags[index]) continue;
        hx = floor.x_list[index];
        hy = decoded.final_y[index] * floor.multiplier;
        renderLine(lx, ly, hx, hy, curve_y);
        lx = hx;
        ly = hy;
    }
    if (hx < n) {
        renderLine(hx, hy, n, hy, curve_y);
    }

    const curve = try allocator.alloc(f32, n);
    for (curve, curve_y) |*sample, value| {
        sample.* = inverseDbApprox(value);
    }
    return curve;
}

fn applyFloor1CurveInPlace(
    scratch: *VorbisDecodeScratch,
    spectrum: []f32,
    floor: Floor,
    decoded: DecodedFloor,
) !void {
    if (floor.kind != 1) return error.UnsupportedAudioFormat;
    const curve_y = try scratch.ensureFloorCurveY(spectrum.len);
    @memset(curve_y, 0);

    const order = if (floor.x_order.len == floor.x_list.len)
        floor.x_order
    else blk: {
        const scratch_order = try scratch.ensureFloorOrder(floor.x_list.len);
        for (scratch_order, 0..) |*slot, i| slot.* = i;
        std.sort.insertion(usize, scratch_order, floor.x_list, lessThanX);
        break :blk scratch_order;
    };

    var hx: usize = 0;
    var hy: u16 = decoded.final_y[order[0]] * floor.multiplier;
    var lx: usize = 0;
    var ly: u16 = hy;
    for (order[1..]) |index| {
        if (!decoded.step2_flags[index]) continue;
        hx = floor.x_list[index];
        hy = decoded.final_y[index] * floor.multiplier;
        renderLine(lx, ly, hx, hy, curve_y);
        lx = hx;
        ly = hy;
    }
    if (hx < spectrum.len) {
        renderLine(hx, hy, spectrum.len, hy, curve_y);
    }

    for (spectrum, curve_y) |*value, floor_value| {
        value.* *= inverseDbApprox(floor_value);
    }
}

fn buildFloor0CurveAlloc(
    allocator: std.mem.Allocator,
    n: usize,
    floor: Floor,
    decoded: DecodedFloor,
) ![]f32 {
    if (floor.kind != 0) return error.UnsupportedAudioFormat;
    const curve = try allocator.alloc(f32, n);
    @memset(curve, 1);
    if (decoded.no_residue) {
        @memset(curve, 0);
        return curve;
    }

    const linear_map = try buildFloor0LinearMapAlloc(allocator, n, floor);
    defer allocator.free(linear_map);
    try lspToCurve(curve, linear_map, decoded.lsp_coefficients, decoded.amplitude_db, floor.floor0_bark_map, floor.floor0_amp_db);
    return curve;
}

fn lessThanX(context: []const u32, lhs: usize, rhs: usize) bool {
    return context[lhs] < context[rhs];
}

fn lowNeighbor(x_list: []const u32, target: usize) usize {
    var best: usize = 0;
    for (1..target) |i| {
        if (x_list[i] < x_list[target] and x_list[i] > x_list[best]) best = i;
    }
    return best;
}

fn highNeighbor(x_list: []const u32, target: usize) usize {
    var best: ?usize = null;
    for (0..target) |i| {
        if (x_list[i] > x_list[target]) {
            if (best == null or x_list[i] < x_list[best.?]) best = i;
        }
    }
    return best orelse 1;
}

fn renderPoint(x0: u32, y0: u16, x1: u32, y1: u16, x: u32) u16 {
    const dy = @as(i32, y1) - @as(i32, y0);
    const adx = x1 - x0;
    const ady = @abs(dy);
    const err = ady * (x - x0);
    const off = @divTrunc(err, adx);
    return if (dy < 0)
        @intCast(@as(i32, y0) - @as(i32, @intCast(off)))
    else
        @intCast(@as(i32, y0) + @as(i32, @intCast(off)));
}

fn renderLine(x0: usize, y0: u16, x1: usize, y1: u16, values: []u16) void {
    if (x0 >= values.len) return;
    if (x1 <= x0 + 1) {
        values[x0] = y0;
        return;
    }

    const dy = @as(i32, y1) - @as(i32, y0);
    const adx = @as(i32, @intCast(x1 - x0));
    var ady: i32 = @intCast(@abs(dy));
    const base = @divTrunc(dy, adx);
    const sy: i32 = if (dy < 0) base - 1 else base + 1;
    ady -= @as(i32, @intCast(@abs(base))) * adx;

    var x = x0;
    var y: i32 = y0;
    var err: i32 = 0;
    values[x] = @intCast(@max(y, 0));
    x += 1;
    while (x < x1 and x < values.len) : (x += 1) {
        err += ady;
        if (err >= adx) {
            err -= adx;
            y += sy;
        } else {
            y += base;
        }
        values[x] = @intCast(@max(y, 0));
    }
}

fn inverseDbApprox(value: u16) f32 {
    const clamped = @min(value, 255);
    return @exp((@as(f32, @floatFromInt(clamped)) - 255.0) * 0.063025);
}

fn buildFloor0LinearMapAlloc(allocator: std.mem.Allocator, n: usize, floor: Floor) ![]u16 {
    const linear_map = try allocator.alloc(u16, n);
    const scale = @as(f32, @floatFromInt(floor.floor0_bark_map)) / toBark(@as(f32, @floatFromInt(floor.floor0_rate)) / 2.0);
    for (0..n) |i| {
        const edge_hz = (@as(f32, @floatFromInt(floor.floor0_rate)) / 2.0 / @as(f32, @floatFromInt(n))) * @as(f32, @floatFromInt(i));
        var value = @as(i32, @intFromFloat(@floor(toBark(edge_hz) * scale)));
        if (value >= floor.floor0_bark_map) value = floor.floor0_bark_map - 1;
        linear_map[i] = @intCast(@max(value, 0));
    }
    return linear_map;
}

fn lspToCurve(
    curve: []f32,
    linear_map: []const u16,
    lsp_coefficients: []const f32,
    amplitude_db: f32,
    bark_map: u16,
    amp_db_offset: u8,
) !void {
    if (curve.len != linear_map.len) return error.UnsupportedAudioFormat;
    if (lsp_coefficients.len == 0 or bark_map == 0) return error.UnsupportedAudioFormat;

    const order = lsp_coefficients.len;
    const wdel = std.math.pi / @as(f32, @floatFromInt(bark_map));
    var lsp_cos_buf: [256]f32 = undefined;
    if (order > lsp_cos_buf.len) return error.UnsupportedAudioFormat;
    const lsp_cos = lsp_cos_buf[0..order];
    for (lsp_coefficients, 0..) |coefficient, i| {
        lsp_cos[i] = 2.0 * @cos(coefficient);
    }

    var i: usize = 0;
    while (i < curve.len) {
        const k = linear_map[i];
        const w = 2.0 * @cos(wdel * @as(f32, @floatFromInt(k)));
        var p: f32 = 0.5;
        var q: f32 = 0.5;

        var j: usize = 1;
        while (j < order) : (j += 2) {
            q *= w - lsp_cos[j - 1];
            p *= w - lsp_cos[j];
        }

        if (j == order) {
            q *= w - lsp_cos[j - 1];
            p *= p * (4.0 - w * w);
            q *= q;
        } else {
            p *= p * (2.0 - w);
            q *= q * (2.0 + w);
        }

        const floor_value = @exp(((amplitude_db / @sqrt(p + q)) - @as(f32, @floatFromInt(amp_db_offset))) * 0.11512925);
        curve[i] = floor_value;
        i += 1;
        while (i < curve.len and linear_map[i] == k) : (i += 1) {
            curve[i] = floor_value;
        }
    }
}

fn toBark(hz: f32) f32 {
    return 13.1 * std.math.atan(0.00074 * hz) + 2.24 * std.math.atan(hz * hz * 1.85e-8) + 0.0001 * hz;
}

fn decodeResidueAlloc(
    scratch: *VorbisDecodeScratch,
    reader: *BitReader,
    residue: Residue,
    codebooks: []const Codebook,
    spectra: []f32,
    channel_len: usize,
    channel_indices: []const u8,
    do_not_decode: []const bool,
) !void {
    if (channel_indices.len != do_not_decode.len) return error.UnsupportedAudioFormat;
    if (channel_indices.len == 0) return;

    var active_channels: usize = 0;
    for (do_not_decode) |flag| {
        if (!flag) active_channels += 1;
    }
    if (active_channels == 0) return;
    const active_channel_indices = try scratch.ensureActiveChannelIndices(active_channels);
    {
        var active_cursor: usize = 0;
        for (channel_indices, 0..) |channel, i| {
            if (do_not_decode[i]) continue;
            active_channel_indices[active_cursor] = channel;
            active_cursor += 1;
        }
    }

    const actual_size = channel_len;
    const limit_begin = @min(@as(usize, @intCast(residue.begin)), actual_size);
    const limit_end = @min(@as(usize, @intCast(residue.end)), actual_size);
    if (limit_end <= limit_begin) return;

    const partition_size = @as(usize, @intCast(residue.partition_size));
    const partition_value_count = partition_size;
    const n_to_read = limit_end - limit_begin;
    const partitions_to_read = n_to_read / partition_value_count;
    if (partitions_to_read == 0) return;

    const classbook = codebooks[residue.classbook];
    const classwords_per_codeword = @as(usize, classbook.dimensions);
    if (classwords_per_codeword == 0) return error.UnsupportedAudioFormat;

    const classifications = try scratch.ensureClassifications(channel_indices.len * partitions_to_read);
    @memset(classifications, 0);

    var max_dimensions: usize = 0;
    for (codebooks) |codebook| {
        max_dimensions = @max(max_dimensions, codebook.dimensions);
    }
    const vector_buf = try scratch.ensureVectorBuf(max_dimensions);

    for (0..8) |pass| {
        var partition_count: usize = 0;
        while (partition_count < partitions_to_read) {
            if (pass == 0) {
                if (residue.kind == 2) {
                    var temp = decodeCodebookScalar(reader, classbook) catch |err| {
                        if (err == error.UnsupportedAudioFormat and reader.bit_offset == reader.bytes.len * 8) return;
                        return err;
                    };
                    var i = classwords_per_codeword;
                    while (i > 0) {
                        i -= 1;
                        if (partition_count + i >= partitions_to_read) continue;
                        classifications[partition_count + i] = @intCast(temp % residue.classifications);
                        temp = @divTrunc(temp, residue.classifications);
                    }
                } else {
                    for (channel_indices, 0..) |_, j| {
                        if (do_not_decode[j]) continue;
                        var temp = decodeCodebookScalar(reader, classbook) catch |err| {
                            if (err == error.UnsupportedAudioFormat and reader.bit_offset == reader.bytes.len * 8) return;
                            return err;
                        };
                        var i = classwords_per_codeword;
                        while (i > 0) {
                            i -= 1;
                            if (partition_count + i >= partitions_to_read) continue;
                            classifications[j * partitions_to_read + partition_count + i] =
                                @intCast(temp % residue.classifications);
                            temp = @divTrunc(temp, residue.classifications);
                        }
                    }
                }
            }

            var i: usize = 0;
            while (i < classwords_per_codeword and partition_count < partitions_to_read) : (i += 1) {
                if (residue.kind == 2) {
                    const vqclass = classifications[partition_count];
                    if (residue.books[vqclass][pass]) |vqbook| {
                        const start = limit_begin + partition_count * partition_value_count;
                        decodeResiduePartition(
                            reader,
                            codebooks[vqbook],
                            vector_buf,
                            residue.kind,
                            spectra,
                            channel_len,
                            active_channel_indices,
                            active_channel_indices[0],
                            start,
                            partition_value_count,
                        ) catch |err| {
                            if (err == error.UnsupportedAudioFormat) return;
                            return err;
                        };
                    }
                } else {
                    for (channel_indices, 0..) |channel, j| {
                        if (do_not_decode[j]) continue;
                        const vqclass = classifications[j * partitions_to_read + partition_count];
                        const vqbook = residue.books[vqclass][pass] orelse continue;
                        const start = limit_begin + partition_count * partition_value_count;
                        decodeResiduePartition(
                            reader,
                            codebooks[vqbook],
                            vector_buf,
                            residue.kind,
                            spectra,
                            channel_len,
                            active_channel_indices,
                            channel,
                            start,
                            partition_size,
                        ) catch |err| {
                            if (err == error.UnsupportedAudioFormat) return;
                            return err;
                        };
                    }
                }
                partition_count += 1;
            }
        }
    }
}

fn decodeResiduePartition(
    reader: *BitReader,
    codebook: Codebook,
    vector_buf: []f32,
    residue_kind: u16,
    spectra: []f32,
    channel_len: usize,
    active_channel_indices: []const u8,
    channel: u8,
    start: usize,
    partition_size: usize,
) !void {
    const dims = @as(usize, codebook.dimensions);
    if (dims == 0) return error.UnsupportedAudioFormat;

    switch (residue_kind) {
        0 => {
            const step = partition_size / dims;
            const channel_spectrum = spectra[channel * channel_len ..][0..channel_len];
            for (0..step) |i| {
                try decodeCodebookVectorInto(reader, codebook, vector_buf[0..dims]);
                for (vector_buf[0..dims], 0..) |value, j| {
                    const index = start + i + j * step;
                    if (index < channel_spectrum.len) channel_spectrum[index] += value;
                }
            }
        },
        1 => {
            const channel_spectrum = spectra[channel * channel_len ..][0..channel_len];
            var cursor: usize = 0;
            while (cursor < partition_size) {
                try decodeCodebookVectorInto(reader, codebook, vector_buf[0..dims]);
                for (vector_buf[0..dims]) |value| {
                    if (cursor >= partition_size) break;
                    const index = start + cursor;
                    if (index < channel_spectrum.len) channel_spectrum[index] += value;
                    cursor += 1;
                }
            }
        },
        2 => {
            var cursor: usize = 0;
            while (cursor < partition_size) {
                try decodeCodebookVectorInto(reader, codebook, vector_buf[0..dims]);
                for (vector_buf[0..dims]) |value| {
                    if (cursor >= partition_size) break;
                    const flat_index = start + cursor;
                    const target_channel = active_channel_indices[flat_index % active_channel_indices.len];
                    const target_index = flat_index / active_channel_indices.len;
                    if (target_index < channel_len) {
                        spectra[target_channel * channel_len + target_index] += value;
                    }
                    cursor += 1;
                }
            }
        },
        else => return error.UnsupportedAudioFormat,
    }
}

fn inverseCouple(magnitude: []f32, angle: []f32) void {
    for (magnitude, angle) |*mag, *ang| {
        const mag_val = mag.*;
        const ang_val = ang.*;
        if (mag_val > 0) {
            if (ang_val > 0) {
                mag.* = mag_val;
                ang.* = mag_val - ang_val;
            } else {
                ang.* = mag_val;
                mag.* = mag_val + ang_val;
            }
        } else {
            if (ang_val > 0) {
                mag.* = mag_val;
                ang.* = mag_val + ang_val;
            } else {
                ang.* = mag_val;
                mag.* = mag_val - ang_val;
            }
        }
    }
}

fn buildImdctPlanAlloc(allocator: std.mem.Allocator, n_u16: u16) !ImdctPlan {
    const n = @as(usize, n_u16);
    return ImdctPlan.init(allocator, n);
}

fn imdctInto(out: []f32, coefficients: []const f32, plan: ImdctPlan) !void {
    if (out.len != coefficients.len * 2 or out.len != plan.n) return error.UnsupportedAudioFormat;
    const work = try std.heap.page_allocator.alloc(fast_imdct.Complex, plan.fft_len);
    defer std.heap.page_allocator.free(work);
    try fast_imdct.imdctInto(out, coefficients, &plan, work);
}

fn imdctIntoWithScratch(out: []f32, coefficients: []const f32, plan: ImdctPlan, scratch: *VorbisDecodeScratch) !void {
    if (out.len != coefficients.len * 2 or out.len != plan.n) return error.UnsupportedAudioFormat;
    const work = try scratch.ensureImdctWork(plan.fft_len);
    try fast_imdct.imdctInto(out, coefficients, &plan, work);
}

fn imdctIntoNaive(out: []f32, coefficients: []const f32) !void {
    if (out.len != coefficients.len * 2) return error.UnsupportedAudioFormat;

    const n = out.len;
    const scale = 2.0 / @as(f32, @floatFromInt(n));
    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f32, @floatFromInt(n_idx)) + 0.5 + @as(f32, @floatFromInt(n)) / 4.0;
        var accum: f32 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f32, @floatFromInt(k_idx)) + 0.5;
            accum += coef * @cos((std.math.pi / @as(f32, @floatFromInt(n))) * n_term * k_term);
        }
        sample.* = accum * scale;
    }
}

fn applyVorbisWindow(
    samples: []f32,
    short_blocksize: u16,
    current_blocksize: u16,
    previous_window_flag: bool,
    next_window_flag: bool,
) !void {
    const factors = try sharedVorbisWindowFactors(
        short_blocksize,
        current_blocksize,
        previous_window_flag,
        next_window_flag,
    );
    if (factors.len != samples.len) return error.UnsupportedAudioFormat;

    for (samples, factors) |*sample, factor| {
        sample.* *= factor;
    }
}

fn buildVorbisWindowFactorsAlloc(
    allocator: std.mem.Allocator,
    short_blocksize: u16,
    current_blocksize: u16,
    previous_window_flag: bool,
    next_window_flag: bool,
) ![]f32 {
    if (short_blocksize == 0 or current_blocksize == 0) return error.UnsupportedAudioFormat;
    const current = @as(usize, current_blocksize);
    const short = @as(usize, short_blocksize);
    if (current < short or current % 2 != 0 or short % 2 != 0) return error.UnsupportedAudioFormat;

    const factors = try allocator.alloc(f32, current);
    errdefer allocator.free(factors);

    const left_start: usize = if (current_blocksize == short_blocksize or previous_window_flag)
        0
    else
        current / 4 - short / 4;
    const left_end: usize = if (current_blocksize == short_blocksize or previous_window_flag)
        current / 2
    else
        current / 4 + short / 4;
    const left_n: usize = left_end - left_start;

    const right_start: usize = if (current_blocksize == short_blocksize or next_window_flag)
        current / 2
    else
        current * 3 / 4 - short / 4;
    const right_end: usize = if (current_blocksize == short_blocksize or next_window_flag)
        current
    else
        current * 3 / 4 + short / 4;
    const right_n: usize = right_end - right_start;

    for (factors, 0..) |*factor, i| {
        if (i < left_start or i >= right_end) {
            factor.* = 0;
        } else if (i < left_end) {
            factor.* = vorbisWindowValue(i - left_start, left_n);
        } else if (i < right_start) {
            factor.* = 1.0;
        } else {
            factor.* = vorbisWindowValue(right_end - i - 1, right_n);
        }
    }

    return factors;
}

fn vorbisWindowValue(index: usize, width: usize) f32 {
    const phase = (@as(f32, @floatFromInt(index)) + 0.5) / @as(f32, @floatFromInt(width));
    const inner = @sin((std.math.pi / 2.0) * phase);
    return @sin((std.math.pi / 2.0) * inner * inner);
}

fn buildCanonicalCodewords(codeword_lengths: []const u8, codewords: []u32) !u8 {
    if (codeword_lengths.len != codewords.len) return error.UnsupportedAudioFormat;

    var counts = [_]u32{0} ** 33;
    var max_len: u8 = 0;
    for (codeword_lengths) |length| {
        if (length == 0) continue;
        if (length >= counts.len) return error.UnsupportedAudioFormat;
        counts[length] += 1;
        max_len = @max(max_len, length);
    }
    if (max_len == 0) return 0;

    var next_code = [_]u32{0} ** 33;
    var code: u32 = 0;
    for (1..counts.len) |bits| {
        code = (code + counts[bits - 1]) << 1;
        next_code[bits] = code;
    }
    for (codeword_lengths, 0..) |length, i| {
        if (length == 0) continue;
        codewords[i] = bitReverse(next_code[length], length);
        next_code[length] += 1;
    }

    return max_len;
}

fn buildCodebookDecodeNodesAlloc(
    allocator: std.mem.Allocator,
    codeword_lengths: []const u8,
    codewords: []const u32,
) ![]Codebook.DecodeNode {
    if (codeword_lengths.len != codewords.len) return error.UnsupportedAudioFormat;

    var nodes = std.ArrayList(Codebook.DecodeNode).empty;
    defer nodes.deinit(allocator);

    try nodes.append(allocator, .{});
    for (codeword_lengths, codewords, 0..) |length, codeword, symbol_index| {
        if (length == 0) continue;

        var node_index: usize = 0;
        for (0..length) |bit_index| {
            const branch = @as(usize, @intCast((codeword >> @intCast(bit_index)) & 1));
            const is_leaf = bit_index + 1 == length;
            var next = nodes.items[node_index].child[branch];
            if (next < 0) {
                next = @intCast(nodes.items.len);
                nodes.items[node_index].child[branch] = next;
                try nodes.append(allocator, .{});
            }

            node_index = @intCast(next);
            if (is_leaf) {
                if (nodes.items[node_index].symbol >= 0) return error.UnsupportedAudioFormat;
                if (nodes.items[node_index].child[0] >= 0 or nodes.items[node_index].child[1] >= 0) {
                    return error.UnsupportedAudioFormat;
                }
                nodes.items[node_index].symbol = @intCast(symbol_index);
            } else if (nodes.items[node_index].symbol >= 0) {
                return error.UnsupportedAudioFormat;
            }
        }
    }

    return nodes.toOwnedSlice(allocator);
}

fn buildCodebookLookupAlloc(
    allocator: std.mem.Allocator,
    codeword_lengths: []const u8,
    codewords: []const u32,
) ![]Codebook.LookupEntry {
    if (codeword_lengths.len != codewords.len) return error.UnsupportedAudioFormat;

    const lookup = try allocator.alloc(Codebook.LookupEntry, vorbis_codebook_lookup_size);
    @memset(lookup, .{});

    for (codeword_lengths, codewords, 0..) |length, codeword, symbol_index| {
        if (length == 0 or length > vorbis_codebook_lookup_bits) continue;

        const suffix_bits = vorbis_codebook_lookup_bits - length;
        const suffix_count = @as(usize, 1) << @intCast(suffix_bits);
        for (0..suffix_count) |suffix| {
            const index = @as(usize, codeword) | (suffix << @intCast(length));
            lookup[index] = .{
                .bits = length,
                .symbol = @intCast(symbol_index),
            };
        }
    }

    return lookup;
}

fn bitReverse(value: u32, bits: u8) u32 {
    var out: u32 = 0;
    for (0..bits) |i| {
        out = (out << 1) | ((value >> @intCast(i)) & 1);
    }
    return out;
}

fn float32Unpack(value: u32) f32 {
    if (value == 0) return 0;
    const mantissa_unsigned = value & 0x1fffff;
    const exponent: i32 = @intCast((value >> 21) & 0x3ff);
    const signed_mantissa: i32 = if ((value & 0x8000_0000) != 0)
        -@as(i32, @intCast(mantissa_unsigned))
    else
        @intCast(mantissa_unsigned);
    return @as(f32, @floatFromInt(signed_mantissa)) * std.math.pow(f32, 2.0, @as(f32, @floatFromInt(exponent - 788)));
}

fn ilog(v: anytype) u6 {
    var value: u64 = @intCast(v);
    var bits: u6 = 0;
    while (value != 0) : (value >>= 1) bits += 1;
    return bits;
}

const BitReader = struct {
    bytes: []const u8,
    bit_offset: usize = 0,

    fn init(bytes: []const u8) BitReader {
        return .{ .bytes = bytes };
    }

    fn remainingBits(self: BitReader) usize {
        return self.bytes.len * 8 -| self.bit_offset;
    }

    fn readBits(self: *BitReader, comptime T: type, count: usize) !T {
        const value = try self.peekBits(T, count);
        self.bit_offset += count;
        return value;
    }

    fn peekBits(self: *BitReader, comptime T: type, count: usize) !T {
        if (count > @bitSizeOf(T) or self.bit_offset + count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        return @intCast(peekBitsLeValue(self.bytes, self.bit_offset, count));
    }

    fn skipBits(self: *BitReader, count: usize) !void {
        if (self.bit_offset + count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        self.bit_offset += count;
    }
};

fn peekBitsLeValue(bytes: []const u8, bit_offset: usize, bit_count: usize) u64 {
    if (bit_count == 0) return 0;
    if (bit_count <= 56) {
        const byte_offset = bit_offset / 8;
        const bit_in_byte = bit_offset % 8;
        const total_bits = bit_in_byte + bit_count;
        const byte_count = (total_bits + 7) / 8;
        var value: u64 = 0;
        for (0..byte_count) |i| {
            value |= @as(u64, bytes[byte_offset + i]) << @intCast(i * 8);
        }
        const shifted = value >> @intCast(bit_in_byte);
        const mask = (@as(u64, 1) << @intCast(bit_count)) - 1;
        return shifted & mask;
    }

    var value: u64 = 0;
    var cursor = bit_offset;
    for (0..bit_count) |i| {
        const byte = bytes[cursor / 8];
        const shift: u3 = @intCast(cursor % 8);
        value |= @as(u64, (byte >> shift) & 1) << @intCast(i);
        cursor += 1;
    }
    return value;
}

fn readLeU32(bytes: []const u8) u32 {
    return @as(u32, bytes[0]) |
        (@as(u32, bytes[1]) << 8) |
        (@as(u32, bytes[2]) << 16) |
        (@as(u32, bytes[3]) << 24);
}

fn readLeI32(bytes: []const u8) i32 {
    return @bitCast(readLeU32(bytes));
}

fn writeLeU32(bytes: []u8, value: u32) void {
    bytes[0] = @truncate(value);
    bytes[1] = @truncate(value >> 8);
    bytes[2] = @truncate(value >> 16);
    bytes[3] = @truncate(value >> 24);
}

fn buildSyntheticChainedVorbisOggAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, ogg_bytes.len * 2);
    @memcpy(out[0..ogg_bytes.len], ogg_bytes);
    @memcpy(out[ogg_bytes.len..], ogg_bytes);

    var cursor: usize = ogg_bytes.len;
    while (cursor < out.len) {
        if (cursor + 27 > out.len or !std.mem.eql(u8, out[cursor .. cursor + 4], "OggS")) {
            return error.UnsupportedAudioFormat;
        }
        const page_segments = @as(usize, out[cursor + 26]);
        const header_size = 27 + page_segments;
        if (cursor + header_size > out.len) return error.UnsupportedAudioFormat;

        const lacing_values = out[cursor + 27 .. cursor + header_size];
        var page_data_len: usize = 0;
        for (lacing_values) |lace| page_data_len += lace;
        if (cursor + header_size + page_data_len > out.len) return error.UnsupportedAudioFormat;

        writeLeU32(out[cursor + 14 .. cursor + 18], 0x564F_5243);
        cursor += header_size + page_data_len;
    }

    return out;
}

test "parse checked-in vorbis identification header" {
    var packets = try ogg.parsePacketsAlloc(std.testing.allocator, tone_ogg_bytes);
    defer packets.deinit();

    const ident = try parseIdentificationHeader(packets.packets[0].bytes);
    try std.testing.expectEqual(@as(u32, 0), ident.version);
    try std.testing.expectEqual(@as(u8, 2), ident.channels);
    try std.testing.expectEqual(@as(u32, 16000), ident.sample_rate);
    try std.testing.expectEqual(@as(u16, 256), ident.blocksize_small);
    try std.testing.expectEqual(@as(u16, 2048), ident.blocksize_large);
}

test "parse checked-in vorbis headers" {
    inline for ([_][]const u8{
        tone_ogg_bytes,
        tone_oga_bytes,
    }) |fixture| {
        var headers = try parseHeadersAlloc(std.testing.allocator, fixture);
        defer headers.comment.deinit();
        defer headers.setup.deinit();

        try std.testing.expectEqual(@as(u8, 2), headers.identification.channels);
        try std.testing.expectEqual(@as(u32, 16000), headers.identification.sample_rate);
        try std.testing.expect(headers.comment.vendor.len != 0);
        try std.testing.expect(headers.setup.codebooks.len > 10);
        try std.testing.expect(headers.setup.floors.len > 0);
        try std.testing.expect(headers.setup.residues.len > 0);
        try std.testing.expect(headers.setup.mappings.len > 0);
        try std.testing.expect(headers.setup.modes.len > 0);
    }
}

test "parse checked-in vorbis setup exposes codebook floor residue metadata" {
    var headers = try parseHeadersAlloc(std.testing.allocator, tone_ogg_bytes);
    defer headers.comment.deinit();
    defer headers.setup.deinit();

    try std.testing.expect(headers.setup.codebooks[0].entries > 0);
    try std.testing.expect(headers.setup.codebooks[0].dimensions > 0);

    var saw_floor1 = false;
    for (headers.setup.floors) |floor| {
        saw_floor1 = saw_floor1 or floor.kind == 1;
    }
    try std.testing.expect(saw_floor1);

    var saw_residue2 = false;
    for (headers.setup.residues) |residue| {
        saw_residue2 = saw_residue2 or residue.kind == 2;
        try std.testing.expect(residue.classifications > 0);
    }
    try std.testing.expect(saw_residue2);
}

test "parse checked-in vorbis audio packet headers" {
    var headers = try parseHeadersAlloc(std.testing.allocator, tone_ogg_bytes);
    defer headers.comment.deinit();
    defer headers.setup.deinit();

    var packets = try ogg.parsePacketsAlloc(std.testing.allocator, tone_ogg_bytes);
    defer packets.deinit();

    try std.testing.expect(packets.packets.len > 4);

    const first_audio = try parseAudioPacketHeader(packets.packets[3].bytes, headers);
    const second_audio = try parseAudioPacketHeader(packets.packets[4].bytes, headers);

    try std.testing.expect(first_audio.blocksize == headers.identification.blocksize_small or first_audio.blocksize == headers.identification.blocksize_large);
    try std.testing.expect(second_audio.blocksize == headers.identification.blocksize_small or second_audio.blocksize == headers.identification.blocksize_large);
    try std.testing.expect(first_audio.mode_number < headers.setup.modes.len);
    try std.testing.expect(second_audio.mode_number < headers.setup.modes.len);
}

test "demux checked-in vorbis fixtures exposes packet schedule and trim" {
    inline for ([_][]const u8{
        tone_ogg_bytes,
        tone_oga_bytes,
    }) |fixture| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, fixture);
        defer demuxed.deinit();

        try std.testing.expectEqual(@as(u32, 16000), demuxed.headers.identification.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), demuxed.headers.identification.channels);
        try std.testing.expectEqual(@as(usize, 17), demuxed.audio_packets.len);
        try std.testing.expectEqual(@as(u64, 16000), demuxed.playable_samples);
        try std.testing.expect(demuxed.total_decoded_samples >= demuxed.playable_samples);
        try std.testing.expect(demuxed.discard_padding_samples > 0);
        try std.testing.expectEqual(@as(?u64, null), demuxed.audio_packets[0].page_granule_position);
        try std.testing.expectEqual(@as(?u64, 15360), demuxed.audio_packets[15].page_granule_position);
        try std.testing.expectEqual(@as(?u64, 16000), demuxed.audio_packets[16].page_granule_position);
        try std.testing.expectEqual(@as(u16, 1024), demuxed.audio_packets[0].decoded_sample_count);
        try std.testing.expectEqual(@as(u16, 1024), demuxed.audio_packets[1].decoded_sample_count);
        try std.testing.expect(demuxed.audio_packets[16].decoded_sample_count > 0);
    }
}

test "decode chained vorbis logical streams with stable format" {
    const chained_bytes = try buildSyntheticChainedVorbisOggAlloc(std.testing.allocator, tone_ogg_bytes);
    defer std.testing.allocator.free(chained_bytes);

    var packet_streams = try ogg.parsePacketStreamsAlloc(std.testing.allocator, chained_bytes);
    defer packet_streams.deinit();
    try std.testing.expectEqual(@as(usize, 2), packet_streams.streams.len);

    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, chained_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 16000 * 2 * 2 * 2), decoded.samples.len);
}

test "optimized vorbis imdct stays close to naive transform" {
    const coefficients = [_]f32{ 0.25, -0.5, 0.75, -1.0 };
    var expected: [8]f32 = undefined;
    var actual: [8]f32 = undefined;

    var plan = try buildImdctPlanAlloc(std.testing.allocator, 8);
    defer plan.deinit();

    try imdctIntoNaive(&expected, &coefficients);
    try imdctInto(&actual, &coefficients, plan);

    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-4);
    }
}

test "fused vorbis timeline window overlap matches separate pass" {
    const channels: u8 = 2;
    const blocksize: usize = 8;
    const total_frames: usize = 12;
    const packet_center: usize = 4;
    const packet_header = AudioPacketHeader{
        .mode_number = 0,
        .blocksize = blocksize,
        .previous_window_flag = false,
        .next_window_flag = true,
    };

    var raw_samples: [channels * blocksize]f32 = undefined;
    for (&raw_samples, 0..) |*sample, index| {
        sample.* = @as(f32, @floatFromInt(@as(i32, @intCast(index)) - 7)) / 7.0;
    }

    var separate_samples = raw_samples;
    for (0..channels) |channel| {
        try applyVorbisWindow(
            separate_samples[channel * blocksize ..][0..blocksize],
            4,
            packet_header.blocksize,
            packet_header.previous_window_flag,
            packet_header.next_window_flag,
        );
    }

    var separate_timeline = [_]f32{0} ** (total_frames * channels);
    const start_frame_signed: isize =
        @as(isize, @intCast(packet_center)) - @as(isize, @intCast(blocksize / 2));
    for (0..channels) |channel| {
        const src = separate_samples[channel * blocksize ..][0..blocksize];
        for (src, 0..) |sample, i| {
            const dst_frame_signed = start_frame_signed + @as(isize, @intCast(i));
            if (dst_frame_signed < 0) continue;
            const dst_frame: usize = @intCast(dst_frame_signed);
            if (dst_frame >= total_frames) break;
            separate_timeline[dst_frame * channels + channel] += sample;
        }
    }

    var fused_timeline = [_]f32{0} ** (total_frames * channels);
    try addPacketBlockToTimelineWithWindow(
        &fused_timeline,
        total_frames,
        channels,
        .{
            .samples = &raw_samples,
            .blocksize = blocksize,
            .channels = channels,
        },
        packet_center,
        4,
        packet_header,
    );

    for (separate_timeline, fused_timeline) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
    }
}

test "decode checked-in vorbis fixtures to interleaved pcm" {
    inline for ([_][]const u8{
        tone_ogg_bytes,
        tone_oga_bytes,
    }) |fixture| {
        var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, fixture);
        defer decoded.deinit();

        try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), decoded.channels);
        try std.testing.expectEqual(@as(usize, 32000), decoded.samples.len);

        var sum_abs: f32 = 0;
        for (decoded.samples[0..@min(decoded.samples.len, 512)]) |sample| {
            sum_abs += @abs(sample);
        }
        try std.testing.expect(sum_abs > 0.01);
    }
}
