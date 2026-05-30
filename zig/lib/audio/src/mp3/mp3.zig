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
const mp3 = @import("../mp3.zig");
const bitstream = @import("bitstream.zig");
const huffman = @import("huffman.zig");
const imdct = @import("imdct.zig");
const requantize = @import("requantize.zig");
pub const synthesis = @import("synthesis.zig");

const inv_sqrt2: f32 = 0.707_106_77;
const mpeg1_intensity_table = [2][7]f32{
    .{ 0.0, 0.211_324_87, 0.366_025_4, 0.5, 0.633_974_6, 0.788_675_1, 1.0 },
    .{ 1.0, 0.788_675_1, 0.633_974_6, 0.5, 0.366_025_4, 0.211_324_87, 0.0 },
};
const lsf_intensity_table = buildLsfIntensityTable();

pub const GranulePayload = struct {
    frame_index: usize,
    granule_index: usize,
    channel_index: usize,
    bit_offset: usize,
    bit_length: usize,
    bytes: []u8,

    pub fn deinit(self: *GranulePayload, allocator: std.mem.Allocator) void {
        if (self.bytes.len == 0) return;
        allocator.free(self.bytes);
    }
};

pub const FramePayload = struct {
    offset: usize,
    header: bitstream.FrameHeader,
    side_info: bitstream.SideInfo,
    has_complete_main_data: bool,
    granules: []GranulePayload,

    pub fn deinit(self: *FramePayload, allocator: std.mem.Allocator) void {
        for (self.granules) |*granule| granule.deinit(allocator);
        allocator.free(self.granules);
    }
};

pub const GranuleDecodePlan = struct {
    frame_index: usize,
    granule_index: usize,
    channel_index: usize,
    payload: GranulePayload,
    info: bitstream.GranuleChannelInfo,
    part2: requantize.Part2Info,
    region_plan: huffman.RegionPlan,
};

const empty_u8_slice: []u8 = @constCast(&[_]u8{});

const default_region_plan = huffman.RegionPlan{
    .big_values = 0,
    .big_value_samples = 0,
    .region_sample_bounds = .{ 0, 0, 0 },
    .region_pair_counts = .{ 0, 0, 0 },
    .table_select = .{ 0, 0, 0 },
    .count1_table_select = false,
};

const default_granule_decode_plan = GranuleDecodePlan{
    .frame_index = 0,
    .granule_index = 0,
    .channel_index = 0,
    .payload = .{
        .frame_index = 0,
        .granule_index = 0,
        .channel_index = 0,
        .bit_offset = 0,
        .bit_length = 0,
        .bytes = empty_u8_slice,
    },
    .info = .{
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
    },
    .part2 = .{
        .bit_length = 0,
        .slen = .{ 0, 0, 0, 0 },
        .partition_scalefactor_bands = .{ 0, 0, 0, 0 },
    },
    .region_plan = default_region_plan,
};

pub const FrameDecodePlan = struct {
    offset: usize,
    header: bitstream.FrameHeader,
    side_info: bitstream.SideInfo,
    has_complete_main_data: bool,
    granules: []GranuleDecodePlan,

    pub fn deinit(self: *FrameDecodePlan, allocator: std.mem.Allocator) void {
        for (self.granules) |*granule| granule.payload.deinit(allocator);
        allocator.free(self.granules);
    }
};

pub const DecodePlanSummary = struct {
    frame_count: usize,
    granule_count: usize,
    table_usage: huffman.TableUsage,
};

pub const GranuleDecodeAttempt = struct {
    pairs_decoded: usize,
    unsupported_table: ?u8,
};

pub const GranuleRequantizeAttempt = struct {
    samples_decoded: usize,
    unsupported_table: ?u8,
    unsupported_sample_index: ?usize,
    coefficients: []f32,
    owns_coefficients: bool = true,

    pub fn deinit(self: *GranuleRequantizeAttempt, allocator: std.mem.Allocator) void {
        if (self.owns_coefficients) allocator.free(self.coefficients);
    }
};

pub const GranuleCount1Attempt = struct {
    quads_decoded: usize,
    samples_decoded: usize,
};

pub const GranuleHybridAttempt = struct {
    unsupported_table: ?u8,
    unsupported_sample_index: ?usize,
    blocks: []f32,

    pub fn deinit(self: *GranuleHybridAttempt, allocator: std.mem.Allocator) void {
        allocator.free(self.blocks);
    }
};

pub const GranuleOverlapAttempt = struct {
    unsupported_table: ?u8,
    unsupported_sample_index: ?usize,
    subband_samples: []f32,

    pub fn deinit(self: *GranuleOverlapAttempt, allocator: std.mem.Allocator) void {
        allocator.free(self.subband_samples);
    }
};

pub const GranulePcmAttempt = struct {
    unsupported_table: ?u8,
    unsupported_sample_index: ?usize,
    pcm_samples: []f32,
    owns_pcm_samples: bool = true,

    pub fn deinit(self: *GranulePcmAttempt, allocator: std.mem.Allocator) void {
        if (self.owns_pcm_samples) allocator.free(self.pcm_samples);
    }
};

pub const PartialDecodeSummary = struct {
    granule_count: usize,
    fully_supported_granules: usize,
    partially_supported_granules: usize,
    unsupported_table_usage: huffman.TableUsage,
};

const Mp3DecodeScratch = struct {
    pairs: [2][288]huffman.DecodedPair = undefined,
    quads: [2][144]huffman.DecodedQuad = undefined,
    coefficients: [2][576]f32 = undefined,
    blocks: [2][32 * 36]f32 = undefined,
    subband_samples: [2][32 * 18]f32 = undefined,
    pcm_samples: [32 * 18 * 2]f32 = undefined,
};

pub const InspectResult = struct {
    first_frame_offset: usize,
    first_header: bitstream.FrameHeader,
    frame_count: usize,
};

pub const PrefixDecodeResult = struct {
    decoded: mp3.Decoded,
    frames_decoded: usize,
    granules_decoded: usize,
    unsupported_table: ?u8,

    pub fn deinit(self: *PrefixDecodeResult, allocator: std.mem.Allocator) void {
        allocator.free(self.decoded.samples);
    }
};

pub const InterleavedPrefixDecodeResult = struct {
    decoded: mp3.DecodedInterleaved,
    frames_decoded: usize,
    granules_decoded: usize,
    unsupported_table: ?u8,

    pub fn deinit(self: *InterleavedPrefixDecodeResult, allocator: std.mem.Allocator) void {
        allocator.free(self.decoded.samples);
    }
};

pub fn enabled() bool {
    return true;
}

pub fn inspectStream(mp3_bytes: []const u8) !InspectResult {
    var frames = bitstream.FrameIterator.init(mp3_bytes);
    const first = (try frames.next()) orelse return error.Mp3SyncNotFound;
    var count: usize = 1;
    while (true) {
        const next_frame = frames.next() catch |err| switch (err) {
            error.Mp3TruncatedFrame => break,
            else => return err,
        };
        if (next_frame == null) break;
        count += 1;
    }
    return .{
        .first_frame_offset = first.offset,
        .first_header = first.header,
        .frame_count = count,
    };
}

pub fn decodeMono(allocator: std.mem.Allocator, mp3_bytes: []const u8) !mp3.Decoded {
    var prefix = try decodeMonoSupportedPrefix(allocator, mp3_bytes);
    errdefer prefix.deinit(allocator);
    if (prefix.unsupported_table != null) return error.Mp3PureZigUnimplemented;

    const decoded = prefix.decoded;
    prefix.decoded.samples = &.{};
    return decoded;
}

pub fn decodeInterleaved(allocator: std.mem.Allocator, mp3_bytes: []const u8) !mp3.DecodedInterleaved {
    var prefix = try decodeInterleavedSupportedPrefix(allocator, mp3_bytes);
    errdefer prefix.deinit(allocator);
    if (prefix.unsupported_table != null) return error.Mp3PureZigUnimplemented;

    const decoded = prefix.decoded;
    prefix.decoded.samples = &.{};
    return decoded;
}

pub fn decodeMonoSupportedPrefix(
    allocator: std.mem.Allocator,
    mp3_bytes: []const u8,
) !PrefixDecodeResult {
    var interleaved = try decodeInterleavedSupportedPrefix(allocator, mp3_bytes);
    defer interleaved.deinit(allocator);

    const mono_samples = switch (interleaved.decoded.channels) {
        1 => blk: {
            const owned = interleaved.decoded.samples;
            interleaved.decoded.samples = &.{};
            break :blk owned;
        },
        2 => try downmixInterleavedStereoToMono(allocator, interleaved.decoded.samples),
        else => return error.Mp3PureZigUnimplemented,
    };

    return .{
        .decoded = .{
            .samples = mono_samples,
            .sample_rate = interleaved.decoded.sample_rate,
        },
        .frames_decoded = interleaved.frames_decoded,
        .granules_decoded = interleaved.granules_decoded,
        .unsupported_table = interleaved.unsupported_table,
    };
}

pub fn decodeInterleavedSupportedPrefix(
    allocator: std.mem.Allocator,
    mp3_bytes: []const u8,
) !InterleavedPrefixDecodeResult {
    const plans = try collectFrameDecodePlans(allocator, mp3_bytes);
    defer {
        for (plans) |*plan| plan.deinit(allocator);
        allocator.free(plans);
    }

    if (plans.len == 0) return error.Mp3SyncNotFound;
    const output_channels = maxOutputChannels(plans);

    var hybrid_states = [_]synthesis.HybridState{ .{}, .{} };
    var qmf_state = synthesis.QmfState{};
    var scratch = Mp3DecodeScratch{};
    var samples = std.ArrayList(f32).empty;
    errdefer samples.deinit(allocator);

    var frames_decoded: usize = 0;
    var granules_decoded: usize = 0;
    var unsupported_table: ?u8 = null;

    for (plans) |plan| {
        const channel_count = @as(usize, plan.side_info.channel_count);
        const granule_count = @as(usize, plan.side_info.granule_count);
        var previous_scalefactors = [_]?requantize.BandScalefactors{ null, null };
        if (channel_count == 0 or channel_count > hybrid_states.len) return error.Mp3PureZigUnimplemented;
        if (plan.granules.len != granule_count * channel_count) return error.Mp3PureZigUnimplemented;
        if (!plan.has_complete_main_data) continue;
        if (!planHasAudioPayload(plan)) continue;

        for (0..granule_count) |gr| {
            var decoded_channels: [2]GranulePcmAttempt = undefined;
            var decoded_count: usize = 0;
            defer {
                for (decoded_channels[0..decoded_count]) |*attempt| attempt.deinit(allocator);
            }

            if (channel_count == 2 and plan.header.channel_mode == .joint_stereo) {
                var scalefactors: [2]requantize.BandScalefactors = undefined;
                var requantized: [2]GranuleRequantizeAttempt = undefined;
                var requantized_count: usize = 0;
                defer {
                    for (requantized[0..requantized_count]) |*attempt| attempt.deinit(allocator);
                }

                for (0..2) |ch| {
                    const granule = plan.granules[gr * 2 + ch];
                    scalefactors[ch] = try decodeGranuleScalefactors(granule, plan.header, previous_scalefactors[ch]);
                    previous_scalefactors[ch] = scalefactors[ch];
                    requantized[ch] = try requantizeGranuleRawPartialInto(
                        plan.header,
                        granule,
                        scalefactors[ch],
                        scratch.pairs[ch][0..],
                        scratch.quads[ch][0..],
                        scratch.coefficients[ch][0..],
                    );
                    requantized_count += 1;
                }

                try applyStereoProcessingPartial(
                    plan.header,
                    plan.granules[gr * 2 + 1].info,
                    scalefactors[1],
                    requantized[0].coefficients,
                    requantized[1].coefficients,
                );
                requantize.reorderShortCoefficients(plan.header.sample_rate, plan.granules[gr * 2].info, requantized[0].coefficients);
                requantize.reorderShortCoefficients(plan.header.sample_rate, plan.granules[gr * 2 + 1].info, requantized[1].coefficients);
                requantize.applyAliasReduction(plan.granules[gr * 2].info, requantized[0].coefficients);
                requantize.applyAliasReduction(plan.granules[gr * 2 + 1].info, requantized[1].coefficients);

                for (0..2) |ch| {
                    const attempt = requantized[ch];
                    if (attempt.unsupported_table) |table| {
                        unsupported_table = table;
                    }
                }
                decoded_channels[0] = try synthesizeStereoGranuleCoefficientsPartialInto(
                    &scratch,
                    &hybrid_states[0],
                    &hybrid_states[1],
                    &qmf_state,
                    plan.granules[gr * 2].info,
                    requantized[0].coefficients,
                    requantized[0].unsupported_table,
                    requantized[0].unsupported_sample_index,
                    plan.granules[gr * 2 + 1].info,
                    requantized[1].coefficients,
                    requantized[1].unsupported_table,
                    requantized[1].unsupported_sample_index,
                );
                decoded_count = 1;
            } else {
                if (channel_count == 2) {
                    var scalefactors: [2]requantize.BandScalefactors = undefined;
                    var requantized: [2]GranuleRequantizeAttempt = undefined;
                    var requantized_count: usize = 0;
                    defer {
                        for (requantized[0..requantized_count]) |*attempt| attempt.deinit(allocator);
                    }

                    for (0..2) |ch| {
                        const granule = plan.granules[gr * 2 + ch];
                        scalefactors[ch] = try decodeGranuleScalefactors(granule, plan.header, previous_scalefactors[ch]);
                        previous_scalefactors[ch] = scalefactors[ch];
                        requantized[ch] = try requantizeGranuleRawPartialInto(
                            plan.header,
                            granule,
                            scalefactors[ch],
                            scratch.pairs[ch][0..],
                            scratch.quads[ch][0..],
                            scratch.coefficients[ch][0..],
                        );
                        requantized_count += 1;
                        if (requantized[ch].unsupported_table) |table| {
                            unsupported_table = table;
                        }
                    }

                    requantize.reorderShortCoefficients(plan.header.sample_rate, plan.granules[gr * 2].info, requantized[0].coefficients);
                    requantize.reorderShortCoefficients(plan.header.sample_rate, plan.granules[gr * 2 + 1].info, requantized[1].coefficients);
                    requantize.applyAliasReduction(plan.granules[gr * 2].info, requantized[0].coefficients);
                    requantize.applyAliasReduction(plan.granules[gr * 2 + 1].info, requantized[1].coefficients);
                    decoded_channels[0] = try synthesizeStereoGranuleCoefficientsPartialInto(
                        &scratch,
                        &hybrid_states[0],
                        &hybrid_states[1],
                        &qmf_state,
                        plan.granules[gr * 2].info,
                        requantized[0].coefficients,
                        requantized[0].unsupported_table,
                        requantized[0].unsupported_sample_index,
                        plan.granules[gr * 2 + 1].info,
                        requantized[1].coefficients,
                        requantized[1].unsupported_table,
                        requantized[1].unsupported_sample_index,
                    );
                    decoded_count = 1;
                } else {
                    for (0..channel_count) |ch| {
                        const granule = plan.granules[gr * channel_count + ch];
                        const scalefactors = try decodeGranuleScalefactors(granule, plan.header, previous_scalefactors[ch]);
                        previous_scalefactors[ch] = scalefactors;
                        var requantized = try requantizeGranuleRawPartialInto(
                            plan.header,
                            granule,
                            scalefactors,
                            scratch.pairs[ch][0..],
                            scratch.quads[ch][0..],
                            scratch.coefficients[ch][0..],
                        );
                        defer requantized.deinit(allocator);

                        requantize.reorderShortCoefficients(plan.header.sample_rate, granule.info, requantized.coefficients);
                        requantize.applyAliasReduction(granule.info, requantized.coefficients);
                        decoded_channels[ch] = try synthesizeGranuleCoefficientsPartialInto(
                            &scratch,
                            ch,
                            &hybrid_states[ch],
                            &qmf_state,
                            granule.info,
                            requantized.coefficients,
                            requantized.unsupported_table,
                            requantized.unsupported_sample_index,
                        );
                        decoded_count += 1;
                        if (decoded_channels[ch].unsupported_table) |table| {
                            unsupported_table = table;
                            break;
                        }
                    }
                }
                if (unsupported_table != null) break;
            }

            if (decoded_count == 0) continue;

            const base_pcm = decoded_channels[0].pcm_samples;
            if (output_channels == 1) {
                try samples.appendSlice(allocator, base_pcm);
            } else if (channel_count == 1) {
                try appendGranuleDuplicatedMonoPcm(allocator, &samples, base_pcm);
            } else if (decoded_count == 1) {
                try samples.appendSlice(allocator, base_pcm);
            } else {
                const other_pcm = decoded_channels[1].pcm_samples;
                if (other_pcm.len != base_pcm.len) return error.Mp3DecodeFailed;
                try appendGranuleInterleavedPcm(allocator, &samples, base_pcm, other_pcm);
            }

            granules_decoded += 1;
            if (unsupported_table != null) break;
        }

        frames_decoded += 1;
        if (unsupported_table != null) break;
    }

    return .{
        .decoded = .{
            .samples = try samples.toOwnedSlice(allocator),
            .sample_rate = plans[0].header.sample_rate,
            .channels = output_channels,
        },
        .frames_decoded = frames_decoded,
        .granules_decoded = granules_decoded,
        .unsupported_table = unsupported_table,
    };
}

fn planHasAudioPayload(plan: FrameDecodePlan) bool {
    for (plan.granules) |granule| {
        if (granule.info.part2_3_length != 0) return true;
    }
    return false;
}

pub fn collectFramePayloads(allocator: std.mem.Allocator, mp3_bytes: []const u8) ![]FramePayload {
    var frames = bitstream.FrameIterator.init(mp3_bytes);
    var reservoir = std.ArrayList(u8).empty;
    defer reservoir.deinit(allocator);

    var payloads = std.ArrayList(FramePayload).empty;
    errdefer {
        for (payloads.items) |*payload| payload.deinit(allocator);
        payloads.deinit(allocator);
    }

    var frame_index: usize = 0;
    while (true) {
        const frame = frames.next() catch |err| switch (err) {
            error.Mp3TruncatedFrame => if (payloads.items.len > 0) break else return err,
            else => return err,
        } orelse break;

        const side_info = try frame.parseSideInfo();
        try reservoir.appendSlice(allocator, frame.main_data_bytes);

        const main_data_begin = side_info.main_data_begin;
        const frame_data_start = reservoir.items.len - frame.main_data_bytes.len;
        const side_info_end = frame_data_start;
        var owned_frame_data: ?[]u8 = null;
        defer if (owned_frame_data) |buf| allocator.free(buf);

        const frame_data = blk: {
            if (main_data_begin <= frame_data_start) {
                const main_data_start = side_info_end - main_data_begin;
                break :blk reservoir.items[main_data_start..];
            }

            const available_history = @min(@as(usize, main_data_begin), frame_data_start);
            const missing_prefix = main_data_begin - available_history;
            const history_start = frame_data_start - available_history;
            const padded = try allocator.alloc(u8, missing_prefix + reservoir.items.len);
            @memset(padded[0..missing_prefix], 0);
            @memcpy(padded[missing_prefix..], reservoir.items[history_start..]);
            owned_frame_data = padded;
            break :blk padded;
        };

        const granule_count = @as(usize, side_info.granule_count);
        const channel_count = @as(usize, side_info.channel_count);
        const granules = try allocator.alloc(GranulePayload, granule_count * channel_count);
        errdefer allocator.free(granules);

        var payload_index: usize = 0;
        var running_bit_offset: usize = 0;
        for (0..granule_count) |gr| {
            for (0..channel_count) |ch| {
                const info = side_info.granules[gr][ch];
                const bit_length = @as(usize, info.part2_3_length);
                const end_bit = running_bit_offset + bit_length;
                const required_bytes = end_bit + 7;
                if (required_bytes / 8 > frame_data.len) {
                    allocator.free(granules);
                    return error.Mp3InsufficientMainData;
                }

                const start_byte = running_bit_offset / 8;
                const end_byte = @divFloor(end_bit + 7, 8);
                const copied = try allocator.dupe(u8, frame_data[start_byte..end_byte]);
                granules[payload_index] = .{
                    .frame_index = frame_index,
                    .granule_index = gr,
                    .channel_index = ch,
                    .bit_offset = running_bit_offset % 8,
                    .bit_length = bit_length,
                    .bytes = copied,
                };
                payload_index += 1;
                running_bit_offset = end_bit;
            }
        }

        try payloads.append(allocator, .{
            .offset = frame.offset,
            .header = frame.header,
            .side_info = side_info,
            .has_complete_main_data = main_data_begin <= frame_data_start,
            .granules = granules,
        });

        const keep_from = if (main_data_begin < frame_data_start)
            frame_data_start - main_data_begin
        else
            0;
        if (keep_from > 0) {
            std.mem.copyForwards(u8, reservoir.items[0 .. reservoir.items.len - keep_from], reservoir.items[keep_from..]);
            reservoir.items.len -= keep_from;
        }

        frame_index += 1;
    }

    return payloads.toOwnedSlice(allocator);
}

pub fn collectFrameDecodePlans(allocator: std.mem.Allocator, mp3_bytes: []const u8) ![]FrameDecodePlan {
    var payloads = try collectFramePayloads(allocator, mp3_bytes);
    var moved_payloads: usize = 0;
    const plans = try allocator.alloc(FrameDecodePlan, payloads.len);
    errdefer {
        for (plans[0..payloads.len]) |*plan| {
            if (plan.granules.len != 0) plan.deinit(allocator);
        }
        allocator.free(plans);
        for (payloads[moved_payloads..]) |*payload| {
            allocator.free(payload.granules);
        }
        allocator.free(payloads);
    }

    for (plans) |*plan| {
        plan.* = .{
            .offset = 0,
            .header = undefined,
            .side_info = undefined,
            .has_complete_main_data = false,
            .granules = &.{},
        };
    }

    for (payloads, 0..) |*payload, i| {
        plans[i] = .{
            .offset = payload.offset,
            .header = payload.header,
            .side_info = payload.side_info,
            .has_complete_main_data = payload.has_complete_main_data,
            .granules = try allocator.alloc(GranuleDecodePlan, payload.granules.len),
        };
        for (plans[i].granules) |*granule| granule.* = default_granule_decode_plan;

        for (payload.granules, 0..) |granule, j| {
            const info = payload.side_info.granules[granule.granule_index][granule.channel_index];
            plans[i].granules[j] = .{
                .frame_index = granule.frame_index,
                .granule_index = granule.granule_index,
                .channel_index = granule.channel_index,
                .payload = granule,
                .info = info,
                .part2 = try requantize.computePart2Info(
                    payload.header,
                    info,
                    granule.granule_index,
                    payload.side_info.scfsi[granule.channel_index],
                ),
                .region_plan = huffman.makeRegionPlan(payload.header, info),
            };
            payload.granules[j].bytes = empty_u8_slice;
        }

        allocator.free(payload.granules);
        moved_payloads += 1;
    }
    allocator.free(payloads);

    return plans;
}

pub fn summarizeDecodePlans(plans: []const FrameDecodePlan) DecodePlanSummary {
    var summary = DecodePlanSummary{
        .frame_count = plans.len,
        .granule_count = 0,
        .table_usage = .{},
    };

    for (plans) |plan| {
        summary.granule_count += plan.granules.len;
        for (plan.granules) |granule| {
            huffman.notePlanUsage(granule.region_plan, &summary.table_usage);
        }
    }

    return summary;
}

pub fn decodeGranuleBigValuesPartial(
    allocator: std.mem.Allocator,
    granule: GranuleDecodePlan,
) !GranuleDecodeAttempt {
    if (granule.part2.bit_length > granule.payload.bit_length) return error.Mp3InvalidPart23Length;
    const pair_count = granule.region_plan.big_values;
    const pairs = try allocator.alloc(huffman.DecodedPair, pair_count);
    defer allocator.free(pairs);

    var reader = huffman.BitSliceReader.init(
        granule.payload.bytes,
        granule.payload.bit_offset + granule.part2.bit_length,
        granule.payload.bit_length -| granule.part2.bit_length,
    );
    const progress = try huffman.decodeBigValuePairsPartial(&reader, granule.region_plan, pairs);
    return .{
        .pairs_decoded = progress.pairs_decoded,
        .unsupported_table = progress.unsupported_table,
    };
}

pub fn decodeGranuleRawScalefactors(granule: GranuleDecodePlan) !requantize.RawScalefactors {
    return requantize.decodeRawScalefactors(
        granule.payload.bytes,
        granule.payload.bit_offset,
        granule.part2,
    );
}

pub fn decodeGranuleScalefactors(
    granule: GranuleDecodePlan,
    header: bitstream.FrameHeader,
    previous: ?requantize.BandScalefactors,
) !requantize.BandScalefactors {
    const raw = try decodeGranuleRawScalefactors(granule);
    const raw_intensity: ?requantize.RawIntensityPositions = if (header.channel_mode == .joint_stereo and
        granule.channel_index == 1 and header.usesIntensityStereo())
        try requantize.decodeRawIntensityPositions(
            granule.payload.bytes,
            granule.payload.bit_offset,
            granule.part2,
            header.version != .mpeg1,
        )
    else
        null;
    return requantize.expandScalefactorsWithIntensity(header, granule.info, granule.part2, raw, raw_intensity, previous);
}

pub fn decodeGranuleScalePlan(
    granule: GranuleDecodePlan,
    header: bitstream.FrameHeader,
    previous: ?requantize.BandScalefactors,
) !requantize.BandScalePlan {
    const scalefactors = try decodeGranuleScalefactors(granule, header, previous);
    return requantize.buildBandScalePlan(header, granule.info, scalefactors);
}

fn requantizeGranuleRawPartial(
    allocator: std.mem.Allocator,
    header: bitstream.FrameHeader,
    granule: GranuleDecodePlan,
    scalefactors: requantize.BandScalefactors,
) !GranuleRequantizeAttempt {
    const pair_count = granule.region_plan.big_values;
    const pairs = try allocator.alloc(huffman.DecodedPair, pair_count);
    defer allocator.free(pairs);

    const remaining_samples = 576 - @min(576, granule.region_plan.big_value_samples);
    const quad_capacity = remaining_samples / 4;
    const quads = try allocator.alloc(huffman.DecodedQuad, quad_capacity);
    defer allocator.free(quads);

    const coefficients = try allocator.alloc(f32, 576);
    errdefer allocator.free(coefficients);

    var attempt = try requantizeGranuleRawPartialInto(header, granule, scalefactors, pairs, quads, coefficients);
    attempt.owns_coefficients = true;
    return attempt;
}

fn requantizeGranuleRawPartialInto(
    header: bitstream.FrameHeader,
    granule: GranuleDecodePlan,
    scalefactors: requantize.BandScalefactors,
    pairs: []huffman.DecodedPair,
    quads: []huffman.DecodedQuad,
    coefficients: []f32,
) !GranuleRequantizeAttempt {
    if (granule.part2.bit_length > granule.payload.bit_length) return error.Mp3InvalidPart23Length;
    if (pairs.len < granule.region_plan.big_values or coefficients.len < 576) return error.Mp3DecodeFailed;

    var reader = huffman.BitSliceReader.init(
        granule.payload.bytes,
        granule.payload.bit_offset + granule.part2.bit_length,
        granule.payload.bit_length -| granule.part2.bit_length,
    );
    const decode_progress = try huffman.decodeBigValuePairsPartial(&reader, granule.region_plan, pairs);
    const remaining_samples = 576 - @min(576, granule.region_plan.big_value_samples);
    const quad_capacity = remaining_samples / 4;
    if (quads.len < quad_capacity) return error.Mp3DecodeFailed;
    const count1_progress = try huffman.decodeCount1QuadsPartial(
        &reader,
        granule.region_plan.count1_table_select,
        quads,
        remaining_samples,
    );

    const scales = try requantize.buildBandScalePlan(header, granule.info, scalefactors);
    const coefficient_frame = coefficients[0..576];

    const spectral_progress = try requantize.requantizeBigValuePairs(
        header,
        granule.info,
        pairs[0..decode_progress.pairs_decoded],
        scales,
        coefficient_frame,
    );
    const count1_spectral_progress = try requantize.requantizeCount1Quads(
        header,
        granule.info,
        scales,
        spectral_progress.samples_decoded,
        quads[0..count1_progress.quads_decoded],
        coefficient_frame,
    );

    return .{
        .samples_decoded = count1_spectral_progress.samples_decoded,
        .unsupported_table = decode_progress.unsupported_table,
        .unsupported_sample_index = spectral_progress.unsupported_sample_index orelse count1_spectral_progress.unsupported_sample_index,
        .coefficients = coefficient_frame,
        .owns_coefficients = false,
    };
}

pub fn requantizeGranuleBigValuesPartial(
    allocator: std.mem.Allocator,
    header: bitstream.FrameHeader,
    granule: GranuleDecodePlan,
    previous: ?requantize.BandScalefactors,
) !GranuleRequantizeAttempt {
    if (header.channel_mode == .joint_stereo) return error.Mp3PureZigUnimplemented;

    const scalefactors = try decodeGranuleScalefactors(granule, header, previous);
    var attempt = try requantizeGranuleRawPartial(allocator, header, granule, scalefactors);
    errdefer attempt.deinit(allocator);

    requantize.reorderShortCoefficients(header.sample_rate, granule.info, attempt.coefficients);
    requantize.applyAliasReduction(granule.info, attempt.coefficients);
    return attempt;
}

pub fn decodeGranuleCount1Partial(
    allocator: std.mem.Allocator,
    granule: GranuleDecodePlan,
) !GranuleCount1Attempt {
    if (granule.part2.bit_length > granule.payload.bit_length) return error.Mp3InvalidPart23Length;

    const pair_count = granule.region_plan.big_values;
    const pairs = try allocator.alloc(huffman.DecodedPair, pair_count);
    defer allocator.free(pairs);

    var reader = huffman.BitSliceReader.init(
        granule.payload.bytes,
        granule.payload.bit_offset + granule.part2.bit_length,
        granule.payload.bit_length -| granule.part2.bit_length,
    );
    const big_progress = try huffman.decodeBigValuePairsPartial(&reader, granule.region_plan, pairs);

    const remaining_samples = 576 - @min(576, granule.region_plan.big_value_samples);
    const quad_capacity = remaining_samples / 4;
    const quads = try allocator.alloc(huffman.DecodedQuad, quad_capacity);
    defer allocator.free(quads);

    const count1_progress = try huffman.decodeCount1QuadsPartial(
        &reader,
        granule.region_plan.count1_table_select,
        quads,
        remaining_samples,
    );
    _ = big_progress;

    return .{
        .quads_decoded = count1_progress.quads_decoded,
        .samples_decoded = count1_progress.samples_decoded,
    };
}

pub fn hybridTransformGranulePartial(
    allocator: std.mem.Allocator,
    header: bitstream.FrameHeader,
    granule: GranuleDecodePlan,
    previous: ?requantize.BandScalefactors,
) !GranuleHybridAttempt {
    var requantized = try requantizeGranuleBigValuesPartial(allocator, header, granule, previous);
    defer requantized.deinit(allocator);

    const blocks = try hybridTransformCoefficients(allocator, granule.info, requantized.coefficients);
    errdefer allocator.free(blocks);

    return .{
        .unsupported_table = requantized.unsupported_table,
        .unsupported_sample_index = requantized.unsupported_sample_index,
        .blocks = blocks,
    };
}

fn hybridTransformCoefficients(
    allocator: std.mem.Allocator,
    info: bitstream.GranuleChannelInfo,
    coefficients: []const f32,
) ![]f32 {
    const blocks = try allocator.alloc(f32, 32 * 36);
    errdefer allocator.free(blocks);
    try hybridTransformCoefficientsInto(info, coefficients, blocks);
    return blocks;
}

fn hybridTransformCoefficientsInto(
    info: bitstream.GranuleChannelInfo,
    coefficients: []const f32,
    blocks: []f32,
) !void {
    if (coefficients.len < 576 or blocks.len < 32 * 36) return error.Mp3DecodeFailed;
    @memset(blocks, 0);

    const short_blocks = info.window_switching_flag and info.block_type == 2;
    for (0..32) |subband| {
        const coeffs = coefficients[subband * 18 ..][0..18];
        const block = blocks[subband * 36 ..][0..36];

        if (short_blocks and (!info.mixed_block_flag or subband >= 2)) {
            try imdct.hybridShortBlock(coeffs, block);
        } else {
            const block_type: u2 = if (short_blocks and info.mixed_block_flag and subband < 2)
                0
            else
                info.block_type;
            try imdct.hybridLongBlock(block_type, coeffs, block);
        }
    }
}

pub fn overlapGranuleHybridPartial(
    allocator: std.mem.Allocator,
    state: *synthesis.HybridState,
    header: bitstream.FrameHeader,
    granule: GranuleDecodePlan,
    previous: ?requantize.BandScalefactors,
) !GranuleOverlapAttempt {
    var hybrid = try hybridTransformGranulePartial(allocator, header, granule, previous);
    defer hybrid.deinit(allocator);

    const subband_samples = try allocator.alloc(f32, 32 * 18);
    errdefer allocator.free(subband_samples);
    try synthesis.overlapAddGranule(state, hybrid.blocks, subband_samples);
    applyHybridFrequencyInversion(subband_samples);

    return .{
        .unsupported_table = hybrid.unsupported_table,
        .unsupported_sample_index = hybrid.unsupported_sample_index,
        .subband_samples = subband_samples,
    };
}

pub fn synthesizeGranulePcmPartial(
    allocator: std.mem.Allocator,
    hybrid_state: *synthesis.HybridState,
    qmf_state: *synthesis.QmfState,
    header: bitstream.FrameHeader,
    granule: GranuleDecodePlan,
    previous: ?requantize.BandScalefactors,
) !GranulePcmAttempt {
    var overlap = try overlapGranuleHybridPartial(allocator, hybrid_state, header, granule, previous);
    defer overlap.deinit(allocator);

    return synthesizeSubbandSamplesPartial(
        allocator,
        qmf_state,
        overlap.subband_samples,
        .{
            .unsupported_table = overlap.unsupported_table,
            .unsupported_sample_index = overlap.unsupported_sample_index,
        },
    );
}

pub fn summarizePartialBigValueDecodeSupport(
    allocator: std.mem.Allocator,
    plans: []const FrameDecodePlan,
) !PartialDecodeSummary {
    var summary = PartialDecodeSummary{
        .granule_count = 0,
        .fully_supported_granules = 0,
        .partially_supported_granules = 0,
        .unsupported_table_usage = .{},
    };

    for (plans) |plan| {
        for (plan.granules) |granule| {
            summary.granule_count += 1;
            const attempt = try decodeGranuleBigValuesPartial(allocator, granule);
            if (attempt.unsupported_table) |table| {
                summary.partially_supported_granules += 1;
                summary.unsupported_table_usage.note(table);
            } else {
                summary.fully_supported_granules += 1;
            }
        }
    }

    return summary;
}

fn synthesizeGranuleCoefficientsPartial(
    allocator: std.mem.Allocator,
    hybrid_state: *synthesis.HybridState,
    qmf_state: *synthesis.QmfState,
    info: bitstream.GranuleChannelInfo,
    coefficients: []const f32,
    unsupported_table: ?u8,
    unsupported_sample_index: ?usize,
) !GranulePcmAttempt {
    const blocks = try hybridTransformCoefficients(allocator, info, coefficients);
    defer allocator.free(blocks);

    const subband_samples = try allocator.alloc(f32, 32 * 18);
    defer allocator.free(subband_samples);
    try synthesis.overlapAddGranule(hybrid_state, blocks, subband_samples);
    applyHybridFrequencyInversion(subband_samples);

    return synthesizeSubbandSamplesPartial(
        allocator,
        qmf_state,
        subband_samples,
        .{
            .unsupported_table = unsupported_table,
            .unsupported_sample_index = unsupported_sample_index,
        },
    );
}

fn synthesizeGranuleCoefficientsPartialInto(
    scratch: *Mp3DecodeScratch,
    scratch_channel: usize,
    hybrid_state: *synthesis.HybridState,
    qmf_state: *synthesis.QmfState,
    info: bitstream.GranuleChannelInfo,
    coefficients: []const f32,
    unsupported_table: ?u8,
    unsupported_sample_index: ?usize,
) !GranulePcmAttempt {
    if (scratch_channel >= scratch.blocks.len) return error.Mp3DecodeFailed;
    const blocks = scratch.blocks[scratch_channel][0..];
    const subband_samples = scratch.subband_samples[scratch_channel][0..];
    const pcm_samples = scratch.pcm_samples[0 .. 32 * 18];

    try hybridTransformCoefficientsInto(info, coefficients, blocks);
    try synthesis.overlapAddGranule(hybrid_state, blocks, subband_samples);
    applyHybridFrequencyInversion(subband_samples);
    try synthesis.synthesizeFrameMono(qmf_state, subband_samples, pcm_samples);

    return .{
        .unsupported_table = unsupported_table,
        .unsupported_sample_index = unsupported_sample_index,
        .pcm_samples = pcm_samples,
        .owns_pcm_samples = false,
    };
}

fn synthesizeStereoGranuleCoefficientsPartial(
    allocator: std.mem.Allocator,
    left_hybrid_state: *synthesis.HybridState,
    right_hybrid_state: *synthesis.HybridState,
    qmf_state: *synthesis.QmfState,
    left_info: bitstream.GranuleChannelInfo,
    left_coefficients: []const f32,
    left_unsupported_table: ?u8,
    left_unsupported_sample_index: ?usize,
    right_info: bitstream.GranuleChannelInfo,
    right_coefficients: []const f32,
    right_unsupported_table: ?u8,
    right_unsupported_sample_index: ?usize,
) !GranulePcmAttempt {
    const left_blocks = try hybridTransformCoefficients(allocator, left_info, left_coefficients);
    defer allocator.free(left_blocks);
    const right_blocks = try hybridTransformCoefficients(allocator, right_info, right_coefficients);
    defer allocator.free(right_blocks);

    const left_subband_samples = try allocator.alloc(f32, 32 * 18);
    defer allocator.free(left_subband_samples);
    const right_subband_samples = try allocator.alloc(f32, 32 * 18);
    defer allocator.free(right_subband_samples);

    try synthesis.overlapAddGranule(left_hybrid_state, left_blocks, left_subband_samples);
    try synthesis.overlapAddGranule(right_hybrid_state, right_blocks, right_subband_samples);
    applyHybridFrequencyInversion(left_subband_samples);
    applyHybridFrequencyInversion(right_subband_samples);

    const pcm_samples = try allocator.alloc(f32, 32 * 18 * 2);
    errdefer allocator.free(pcm_samples);
    try synthesis.synthesizeFrameStereo(qmf_state, left_subband_samples, right_subband_samples, pcm_samples);

    return .{
        .unsupported_table = left_unsupported_table orelse right_unsupported_table,
        .unsupported_sample_index = left_unsupported_sample_index orelse right_unsupported_sample_index,
        .pcm_samples = pcm_samples,
    };
}

fn synthesizeStereoGranuleCoefficientsPartialInto(
    scratch: *Mp3DecodeScratch,
    left_hybrid_state: *synthesis.HybridState,
    right_hybrid_state: *synthesis.HybridState,
    qmf_state: *synthesis.QmfState,
    left_info: bitstream.GranuleChannelInfo,
    left_coefficients: []const f32,
    left_unsupported_table: ?u8,
    left_unsupported_sample_index: ?usize,
    right_info: bitstream.GranuleChannelInfo,
    right_coefficients: []const f32,
    right_unsupported_table: ?u8,
    right_unsupported_sample_index: ?usize,
) !GranulePcmAttempt {
    const left_blocks = scratch.blocks[0][0..];
    const right_blocks = scratch.blocks[1][0..];
    const left_subband_samples = scratch.subband_samples[0][0..];
    const right_subband_samples = scratch.subband_samples[1][0..];
    const pcm_samples = scratch.pcm_samples[0 .. 32 * 18 * 2];

    try hybridTransformCoefficientsInto(left_info, left_coefficients, left_blocks);
    try hybridTransformCoefficientsInto(right_info, right_coefficients, right_blocks);
    try synthesis.overlapAddGranule(left_hybrid_state, left_blocks, left_subband_samples);
    try synthesis.overlapAddGranule(right_hybrid_state, right_blocks, right_subband_samples);
    applyHybridFrequencyInversion(left_subband_samples);
    applyHybridFrequencyInversion(right_subband_samples);
    try synthesis.synthesizeFrameStereo(qmf_state, left_subband_samples, right_subband_samples, pcm_samples);

    return .{
        .unsupported_table = left_unsupported_table orelse right_unsupported_table,
        .unsupported_sample_index = left_unsupported_sample_index orelse right_unsupported_sample_index,
        .pcm_samples = pcm_samples,
        .owns_pcm_samples = false,
    };
}

fn synthesizeSubbandSamplesPartial(
    allocator: std.mem.Allocator,
    qmf_state: *synthesis.QmfState,
    subband_samples: []const f32,
    meta: struct { unsupported_table: ?u8, unsupported_sample_index: ?usize },
) !GranulePcmAttempt {
    const pcm_samples = try allocator.alloc(f32, 32 * 18);
    errdefer allocator.free(pcm_samples);

    try synthesis.synthesizeFrameMono(qmf_state, subband_samples, pcm_samples);
    return .{
        .unsupported_table = meta.unsupported_table,
        .unsupported_sample_index = meta.unsupported_sample_index,
        .pcm_samples = pcm_samples,
    };
}

fn applyHybridFrequencyInversion(subband_samples: []f32) void {
    std.debug.assert(subband_samples.len == 32 * 18);
    var subband: usize = 1;
    while (subband < 32) : (subband += 2) {
        var sample_index: usize = 1;
        while (sample_index < 18) : (sample_index += 2) {
            subband_samples[subband * 18 + sample_index] = -subband_samples[subband * 18 + sample_index];
        }
    }
}

fn applyStereoProcessingPartial(
    header: bitstream.FrameHeader,
    right_info: bitstream.GranuleChannelInfo,
    right_scalefactors: requantize.BandScalefactors,
    left_coefficients: []f32,
    right_coefficients: []f32,
) !void {
    switch (header.channel_mode) {
        .mono, .stereo, .dual_channel => {},
        .joint_stereo => {
            if (header.usesIntensityStereo()) {
                switch (header.version) {
                    .mpeg1 => try applyIntensityStereo(
                        header,
                        right_scalefactors,
                        left_coefficients,
                        right_coefficients,
                        &mpeg1_intensity_table[0],
                        &mpeg1_intensity_table[1],
                    ),
                    .mpeg2, .mpeg25 => try applyIntensityStereo(
                        header,
                        right_scalefactors,
                        left_coefficients,
                        right_coefficients,
                        &lsf_intensity_table[right_info.scalefac_compress & 1][0],
                        &lsf_intensity_table[right_info.scalefac_compress & 1][1],
                    ),
                }
            } else if (header.usesMsStereo()) {
                applyMsStereoWholeSpectrum(left_coefficients, right_coefficients);
            }
        },
    }
}

fn appendGranuleMonoPcm(
    allocator: std.mem.Allocator,
    samples: *std.ArrayList(f32),
    header: bitstream.FrameHeader,
    primary_pcm: []const f32,
    secondary_pcm: []const f32,
) !void {
    const mixed = try allocator.alloc(f32, primary_pcm.len);
    defer allocator.free(mixed);

    switch (header.channel_mode) {
        .mono => return error.Mp3DecodeFailed,
        .stereo, .dual_channel, .joint_stereo => {
            for (primary_pcm, secondary_pcm, 0..) |left, right, i| {
                mixed[i] = (left + right) * 0.5;
            }
        },
    }

    try samples.appendSlice(allocator, mixed);
}

fn appendGranuleInterleavedPcm(
    allocator: std.mem.Allocator,
    samples: *std.ArrayList(f32),
    left_pcm: []const f32,
    right_pcm: []const f32,
) !void {
    try samples.ensureUnusedCapacity(allocator, left_pcm.len * 2);
    for (left_pcm, right_pcm) |left, right| {
        samples.appendAssumeCapacity(left);
        samples.appendAssumeCapacity(right);
    }
}

fn appendGranuleDuplicatedMonoPcm(
    allocator: std.mem.Allocator,
    samples: *std.ArrayList(f32),
    mono_pcm: []const f32,
) !void {
    try samples.ensureUnusedCapacity(allocator, mono_pcm.len * 2);
    for (mono_pcm) |sample| {
        samples.appendAssumeCapacity(sample);
        samples.appendAssumeCapacity(sample);
    }
}

fn downmixInterleavedStereoToMono(
    allocator: std.mem.Allocator,
    interleaved: []const f32,
) ![]f32 {
    if ((interleaved.len & 1) != 0) return error.Mp3DecodeFailed;

    const mono = try allocator.alloc(f32, interleaved.len / 2);
    errdefer allocator.free(mono);

    for (mono, 0..) |*sample, i| {
        const left = interleaved[i * 2];
        const right = interleaved[i * 2 + 1];
        sample.* = (left + right) * 0.5;
    }

    return mono;
}

fn maxOutputChannels(plans: []const FrameDecodePlan) u8 {
    var channels: u8 = 1;
    for (plans) |plan| {
        channels = @max(channels, plan.header.channels());
    }
    return channels;
}

fn applyIntensityStereo(
    header: bitstream.FrameHeader,
    right_scalefactors: requantize.BandScalefactors,
    left_coefficients: []f32,
    right_coefficients: []f32,
    left_factors: []const f32,
    right_factors: []const f32,
) !void {
    const band_map = buildStereoBandMap(
        header,
        right_scalefactors,
        right_coefficients,
    );

    for (band_map.entries[0..band_map.count], 0..) |entry, band_index| {
        const block_index = band_index % band_map.max_blocks;
        const use_intensity = band_index > band_map.max_band[block_index] and entry.intensity_pos < band_map.max_pos;
        const intensity_ms_scale: f32 = if (header.usesMsStereo()) @sqrt(@as(f32, 2.0)) else 1.0;

        if (use_intensity) {
            if (entry.intensity_pos < left_factors.len and entry.intensity_pos < right_factors.len) {
                applyStereoBandIntensity(
                    left_coefficients,
                    right_coefficients,
                    entry.start,
                    entry.end,
                    left_factors[entry.intensity_pos] * intensity_ms_scale,
                    right_factors[entry.intensity_pos] * intensity_ms_scale,
                );
                continue;
            }
        }

        if (header.usesMsStereo()) {
            applyMsStereoLongBand(left_coefficients, right_coefficients, entry.start, entry.end);
        }
    }
}

const StereoBandEntry = struct {
    start: usize,
    end: usize,
    intensity_pos: u8,
};

const StereoBandMap = struct {
    entries: [39]StereoBandEntry,
    count: usize,
    max_band: [3]isize,
    max_blocks: usize,
    max_pos: u8,
};

fn buildStereoBandMap(
    header: bitstream.FrameHeader,
    scalefactors: requantize.BandScalefactors,
    right_coefficients: []const f32,
) StereoBandMap {
    const default_pos = switch (header.version) {
        .mpeg1 => @as(u8, 3),
        .mpeg2, .mpeg25 => @as(u8, 0),
    };
    const max_pos: u8 = if (header.version == .mpeg1) 7 else 64;
    const has_short = scalefactors.short_band_count != 0;
    const has_long = scalefactors.long_band_count != 0;
    const max_blocks: usize = if (has_short) 3 else 1;

    var map = StereoBandMap{
        .entries = undefined,
        .count = 0,
        .max_band = .{ -1, -1, -1 },
        .max_blocks = max_blocks,
        .max_pos = max_pos,
    };

    if (has_long) {
        const long_entry_count = if (has_short) scalefactors.long_band_count else scalefactors.long_band_count + 1;
        const long_bands = requantize.scalefactorBandLong(header.sample_rate);
        for (0..long_entry_count) |band| {
            map.entries[map.count] = .{
                .start = long_bands[band],
                .end = long_bands[band + 1],
                .intensity_pos = if (band < scalefactors.long_band_count) scalefactors.intensity_long[band] else default_pos,
            };
            if (sliceHasNonZero(right_coefficients[long_bands[band]..long_bands[band + 1]])) {
                map.max_band[map.count % max_blocks] = @intCast(map.count);
            }
            map.count += 1;
        }
    }

    if (has_short) {
        const short_bands = requantize.scalefactorBandShort(header.sample_rate);
        const short_start = scalefactors.short_band_start;
        const short_end = scalefactors.short_band_start + scalefactors.short_band_count;
        var source_offset: usize = if (scalefactors.long_band_count > 0) 36 else 0;
        for (short_start..short_end) |band| {
            const band_width = short_bands[band + 1] - short_bands[band];
            for (0..3) |window| {
                const band_start = source_offset;
                const band_end = band_start + band_width;
                map.entries[map.count] = .{
                    .start = band_start,
                    .end = band_end,
                    .intensity_pos = if (band + 1 == short_end) default_pos else scalefactors.intensity_short[window][band],
                };
                if (sliceHasNonZero(right_coefficients[band_start..band_end])) {
                    map.max_band[map.count % max_blocks] = @intCast(map.count);
                }
                map.count += 1;
                source_offset = band_end;
            }
        }
    }

    if (has_long and max_blocks == 3) {
        const merged = @max(map.max_band[0], @max(map.max_band[1], map.max_band[2]));
        map.max_band = .{ merged, merged, merged };
    }

    for (0..max_blocks) |block_index| {
        const top_index = map.count - max_blocks + block_index;
        const prev_index = top_index - max_blocks;
        map.entries[top_index].intensity_pos = if (map.max_band[block_index] >= @as(isize, @intCast(prev_index)))
            default_pos
        else
            map.entries[prev_index].intensity_pos;
    }

    return map;
}

fn sliceHasNonZero(values: []const f32) bool {
    for (values) |value| {
        if (value != 0) return true;
    }
    return false;
}

fn shortBandWindowHasNonZero(
    coefficients: []const f32,
    band_start: usize,
    band_end: usize,
    window: usize,
) bool {
    var line = band_start;
    while (line < band_end) : (line += 1) {
        if (coefficients[line * 3 + window] != 0) return true;
    }
    return false;
}

fn applyIntensityLongBand(
    left_coefficients: []f32,
    right_coefficients: []f32,
    band_start: usize,
    band_end: usize,
    left_factor: f32,
    right_factor: f32,
) void {
    for (band_start..band_end) |i| {
        const value = left_coefficients[i];
        left_coefficients[i] = value * left_factor;
        right_coefficients[i] = value * right_factor;
    }
}

fn applyStereoBandIntensity(
    left_coefficients: []f32,
    right_coefficients: []f32,
    band_start: usize,
    band_end: usize,
    left_factor: f32,
    right_factor: f32,
) void {
    for (band_start..band_end) |i| {
        const value = left_coefficients[i];
        left_coefficients[i] = value * left_factor;
        right_coefficients[i] = value * right_factor;
    }
}

fn applyIntensityShortBandWindow(
    left_coefficients: []f32,
    right_coefficients: []f32,
    band_start: usize,
    band_end: usize,
    window: usize,
    left_factor: f32,
    right_factor: f32,
) void {
    var line = band_start;
    while (line < band_end) : (line += 1) {
        const index = line * 3 + window;
        const value = left_coefficients[index];
        left_coefficients[index] = value * left_factor;
        right_coefficients[index] = value * right_factor;
    }
}

fn intensityLongBandLimit(
    scalefactors: requantize.BandScalefactors,
    long_bands: []const usize,
) usize {
    if (scalefactors.short_band_count == 0 and
        scalefactors.long_band_count > 0 and
        scalefactors.long_band_count + 1 < long_bands.len)
    {
        return scalefactors.long_band_count + 1;
    }
    return scalefactors.long_band_count;
}

fn intensityLongScalefactorIndex(
    scalefactors: requantize.BandScalefactors,
    band_index: usize,
) usize {
    if (scalefactors.long_band_count == 0) return 0;
    if (band_index >= scalefactors.long_band_count) return scalefactors.long_band_count - 1;
    return band_index;
}

fn buildLsfIntensityTable() [2][2][16]f32 {
    var table: [2][2][16]f32 = undefined;
    for (0..16) |i| {
        for (0..2) |j| {
            const exponent = -(@as(i32, @intCast(j)) + 1) * @as(i32, @intCast((i + 1) >> 1));
            const factor = std.math.exp2(@as(f32, @floatFromInt(exponent)) / 4.0);
            const k = i & 1;
            table[j][k ^ 1][i] = factor;
            table[j][k][i] = 1.0;
        }
    }
    return table;
}

fn applyMsStereoWholeSpectrum(left_coefficients: []f32, right_coefficients: []f32) void {
    for (left_coefficients, right_coefficients, 0..) |left, right, i| {
        left_coefficients[i] = left + right;
        right_coefficients[i] = left - right;
    }
}

fn applyMsStereoLongBand(left_coefficients: []f32, right_coefficients: []f32, band_start: usize, band_end: usize) void {
    for (band_start..band_end) |i| {
        const left = left_coefficients[i];
        const right = right_coefficients[i];
        left_coefficients[i] = left + right;
        right_coefficients[i] = left - right;
    }
}

fn applyMsStereoShortBandWindow(
    left_coefficients: []f32,
    right_coefficients: []f32,
    band_start: usize,
    band_end: usize,
    window: usize,
) void {
    var line = band_start;
    while (line < band_end) : (line += 1) {
        const index = line * 3 + window;
        const left = left_coefficients[index];
        const right = right_coefficients[index];
        left_coefficients[index] = left + right;
        right_coefficients[index] = left - right;
    }
}

const Mp3ConformanceCase = struct {
    name: []const u8,
    mp3_bytes: []const u8,
    expected_sample_rate: u32,
    min_samples: usize,
};

const checked_in_conformance_cases = [_]Mp3ConformanceCase{
    .{
        .name = "tone",
        .mp3_bytes = @embedFile("../../testdata/tone.mp3"),
        .expected_sample_rate = 16000,
        .min_samples = 16000,
    },
    .{
        .name = "l3-compl",
        .mp3_bytes = @embedFile("../../testdata/mp3-corpus/l3-compl.bit"),
        .expected_sample_rate = 48000,
        .min_samples = 249984,
    },
    .{
        .name = "l3-si",
        .mp3_bytes = @embedFile("../../testdata/mp3-corpus/l3-si.bit"),
        .expected_sample_rate = 44100,
        .min_samples = 135936,
    },
    .{
        .name = "l3-si_huff",
        .mp3_bytes = @embedFile("../../testdata/mp3-corpus/l3-si_huff.bit"),
        .expected_sample_rate = 44100,
        .min_samples = 85248,
    },
    .{
        .name = "l3-he_free",
        .mp3_bytes = @embedFile("../../testdata/mp3-corpus/l3-he_free.bit"),
        .expected_sample_rate = 44100,
        .min_samples = 78336,
    },
    .{
        .name = "l3-he_mode",
        .mp3_bytes = @embedFile("../../testdata/mp3-corpus/l3-he_mode.bit"),
        .expected_sample_rate = 44100,
        .min_samples = 147456,
    },
};

fn assertConformanceCaseWithBackend(case: Mp3ConformanceCase, backend: mp3.Backend) !void {
    const decoded = try mp3.decodeMonoWithBackend(std.testing.allocator, case.mp3_bytes, backend);
    defer std.testing.allocator.free(decoded.samples);

    try std.testing.expectEqual(case.expected_sample_rate, decoded.sample_rate);
    try std.testing.expect(decoded.samples.len >= case.min_samples);
}

fn assertConformanceCaseThroughFacade(case: Mp3ConformanceCase) !void {
    const decoded = try mp3.decodeMono(std.testing.allocator, case.mp3_bytes);
    defer std.testing.allocator.free(decoded.samples);

    try std.testing.expectEqual(case.expected_sample_rate, decoded.sample_rate);
    try std.testing.expect(decoded.samples.len >= case.min_samples);
}

test "inspect stream finds first frame in fixture" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const inspected = try inspectStream(fixture);
    try std.testing.expect(inspected.first_frame_offset > 0);
    try std.testing.expect(inspected.first_header.sample_rate != 0);
    try std.testing.expect(inspected.frame_count > 10);
}

test "collect frame payloads extracts granule main-data windows" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const payloads = try collectFramePayloads(std.testing.allocator, fixture);
    defer {
        for (payloads) |*payload| payload.deinit(std.testing.allocator);
        std.testing.allocator.free(payloads);
    }

    try std.testing.expect(payloads.len > 10);
    try std.testing.expectEqual(@as(u8, 1), payloads[0].side_info.channel_count);
    try std.testing.expectEqual(@as(u8, 1), payloads[0].side_info.granule_count);
    try std.testing.expect(payloads[0].granules.len == 1);
    try std.testing.expect(payloads[0].granules[0].bit_length > 0);

    var found_reservoir_backref = false;
    for (payloads) |payload| {
        for (payload.granules) |granule| {
            try std.testing.expect(granule.bit_length > 0);
            try std.testing.expect(granule.bytes.len > 0);
            if (payload.side_info.main_data_begin > 0) found_reservoir_backref = true;
        }
    }
    try std.testing.expect(found_reservoir_backref);
}

test "collect frame decode plans builds huffman region plans" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    try std.testing.expect(plans.len > 10);
    for (plans[0..@min(plans.len, 8)]) |plan| {
        for (plan.granules) |granule| {
            try std.testing.expect(granule.region_plan.big_value_samples >= granule.region_plan.region_sample_bounds[2]);
            try std.testing.expectEqual(granule.region_plan.big_values, granule.region_plan.totalBigValuePairs());
            try std.testing.expect(granule.part2.bit_length <= granule.payload.bit_length);

            var reader = huffman.BitSliceReader.init(
                granule.payload.bytes,
                granule.payload.bit_offset,
                granule.payload.bit_length,
            );
            const tasks = huffman.regionTasksForGranule(granule.region_plan);
            try std.testing.expectEqual(granule.region_plan.totalBigValuePairs(), tasks[0].pair_count + tasks[1].pair_count + tasks[2].pair_count);
            const preview = @min(reader.remainingBits(), 12);
            if (preview > 0) {
                _ = try reader.readBits(@intCast(preview));
            }
        }
    }

    const summary = summarizeDecodePlans(plans);
    try std.testing.expect(summary.frame_count > 10);
    try std.testing.expect(summary.granule_count > 10);
    try std.testing.expect(summary.table_usage.usedCount() > 0);
}

test "partial big value decode succeeds on zero-table frame and covered fixture table path" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    const first_attempt = try decodeGranuleBigValuesPartial(std.testing.allocator, plans[0].granules[0]);
    try std.testing.expectEqual(@as(usize, 0), first_attempt.pairs_decoded);
    try std.testing.expect(first_attempt.unsupported_table == null);

    const second_attempt = try decodeGranuleBigValuesPartial(std.testing.allocator, plans[1].granules[0]);
    try std.testing.expect(second_attempt.pairs_decoded > 0);
    try std.testing.expect(second_attempt.unsupported_table == null);
}

test "partial big value decode summary now covers the full fixture" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    const summary = try summarizePartialBigValueDecodeSupport(std.testing.allocator, plans);
    try std.testing.expect(summary.granule_count > 10);
    try std.testing.expectEqual(summary.granule_count, summary.fully_supported_granules);
    try std.testing.expectEqual(@as(usize, 0), summary.partially_supported_granules);
    try std.testing.expectEqual(@as(usize, 0), summary.unsupported_table_usage.usedCount());
}

test "fixture decode plans expose raw part2 scalefactors" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    const scalefactors = try decodeGranuleRawScalefactors(plans[1].granules[0]);
    try std.testing.expectEqual(plans[1].granules[0].part2.totalScalefactors(), scalefactors.count);
    try std.testing.expect(scalefactors.count > 0);
}

test "fixture decode plans expand scalefactors into long or short band layout" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    const expanded = try decodeGranuleScalefactors(plans[1].granules[0], plans[1].header, null);
    try std.testing.expect(expanded.long_band_count == 21 or expanded.short_band_count > 0);
}

test "fixture decode plans expose band scale plan" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    const plan = try decodeGranuleScalePlan(plans[1].granules[0], plans[1].header, null);
    try std.testing.expect(plan.long_band_count == 21 or plan.short_band_count > 0);
}

test "fixture decode plans expose count1 region progress" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    const attempt = try decodeGranuleCount1Partial(std.testing.allocator, plans[1].granules[0]);
    try std.testing.expect(attempt.quads_decoded >= 0);
    try std.testing.expect(attempt.samples_decoded % 4 == 0);
}

test "l3-si_huff requantization includes count1 region samples" {
    const fixture = @embedFile("../../testdata/mp3-corpus/l3-si_huff.bit");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    for (plans) |plan| {
        for (plan.granules) |granule| {
            const count1 = try decodeGranuleCount1Partial(std.testing.allocator, granule);
            if (count1.quads_decoded == 0) continue;

            const scalefactors = try decodeGranuleScalefactors(granule, plan.header, null);
            var requantized = try requantizeGranuleRawPartial(std.testing.allocator, plan.header, granule, scalefactors);
            defer requantized.deinit(std.testing.allocator);

            try std.testing.expect(requantized.samples_decoded > granule.region_plan.big_value_samples);
            return;
        }
    }
    return error.SkipZigTest;
}

test "fixture long-block granule can be partially requantized to spectral coefficients" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    var attempt = try requantizeGranuleBigValuesPartial(std.testing.allocator, plans[1].header, plans[1].granules[0], null);
    defer attempt.deinit(std.testing.allocator);

    try std.testing.expect(attempt.samples_decoded > 0);
    try std.testing.expect(attempt.unsupported_sample_index == null or attempt.unsupported_sample_index.? >= attempt.samples_decoded);
}

test "fixture granule can be transformed into hybrid blocks" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    var attempt = try hybridTransformGranulePartial(std.testing.allocator, plans[1].header, plans[1].granules[0], null);
    defer attempt.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 32 * 36), attempt.blocks.len);

    var energy: f32 = 0;
    for (attempt.blocks) |sample| energy += @abs(sample);
    try std.testing.expect(energy > 0);
}

test "fixture successive granules overlap-add into subband samples" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    var first_hybrid = try hybridTransformGranulePartial(std.testing.allocator, plans[1].header, plans[1].granules[0], null);
    defer first_hybrid.deinit(std.testing.allocator);
    var second_hybrid = try hybridTransformGranulePartial(std.testing.allocator, plans[2].header, plans[2].granules[0], null);
    defer second_hybrid.deinit(std.testing.allocator);

    var state = synthesis.HybridState{};

    var first_overlap = try overlapGranuleHybridPartial(std.testing.allocator, &state, plans[1].header, plans[1].granules[0], null);
    defer first_overlap.deinit(std.testing.allocator);
    var second_overlap = try overlapGranuleHybridPartial(std.testing.allocator, &state, plans[2].header, plans[2].granules[0], null);
    defer second_overlap.deinit(std.testing.allocator);

    for (0..(32 * 18)) |i| {
        try std.testing.expectApproxEqAbs(first_hybrid.blocks[i], first_overlap.subband_samples[i], 1e-5);
    }

    for (0..32) |subband| {
        for (0..18) |sample_index| {
            const first_tail = first_hybrid.blocks[subband * 36 + 18 + sample_index];
            const expected = second_hybrid.blocks[subband * 36 + sample_index] + first_tail;
            const actual = second_overlap.subband_samples[subband * 18 + sample_index];
            try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
        }
    }
}

test "fixture granule can be synthesized to partial pcm" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const plans = try collectFrameDecodePlans(std.testing.allocator, fixture);
    defer {
        for (plans) |*plan| plan.deinit(std.testing.allocator);
        std.testing.allocator.free(plans);
    }

    var hybrid_state = synthesis.HybridState{};
    var qmf_state = synthesis.QmfState{};
    var attempt = try synthesizeGranulePcmPartial(std.testing.allocator, &hybrid_state, &qmf_state, plans[1].header, plans[1].granules[0], null);
    defer attempt.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 32 * 18), attempt.pcm_samples.len);

    var energy: f32 = 0;
    for (attempt.pcm_samples) |sample| energy += @abs(sample);
    try std.testing.expect(energy > 0);
}

test "fixture fully decodes through zig mono prefix path" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    var prefix = try decodeMonoSupportedPrefix(std.testing.allocator, fixture);
    defer prefix.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 16000), prefix.decoded.sample_rate);
    try std.testing.expect(prefix.unsupported_table == null);
    try std.testing.expect(prefix.frames_decoded > 0);
    try std.testing.expect(prefix.granules_decoded > 0);
    try std.testing.expectEqual(prefix.granules_decoded * 32 * 18, prefix.decoded.samples.len);
}

test "l3-si_huff skips non-audio leading frame in zig mono prefix path" {
    const fixture = @embedFile("../../testdata/mp3-corpus/l3-si_huff.bit");
    var prefix = try decodeMonoSupportedPrefix(std.testing.allocator, fixture);
    defer prefix.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 44100), prefix.decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 85248), prefix.decoded.samples.len);
    try std.testing.expectEqual(@as(usize, 74), prefix.frames_decoded);
}

test "l3-he_free payloads expose complete main-data windows" {
    const fixture = @embedFile("../../testdata/mp3-corpus/l3-he_free.bit");
    const payloads = try collectFramePayloads(std.testing.allocator, fixture);
    defer {
        for (payloads) |*payload| payload.deinit(std.testing.allocator);
        std.testing.allocator.free(payloads);
    }

    try std.testing.expectEqual(@as(usize, 68), payloads.len);
    try std.testing.expect(payloads[0].has_complete_main_data);
    try std.testing.expect(payloads[1].has_complete_main_data);
    try std.testing.expect(payloads[2].has_complete_main_data);
}

test "fixture decodeMono succeeds through zig mono path" {
    const fixture = @embedFile("../../testdata/tone.mp3");
    const decoded = try decodeMono(std.testing.allocator, fixture);
    defer std.testing.allocator.free(decoded.samples);

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expect(decoded.samples.len > 16000);
}

test "joint stereo mono output helper averages stereo pcm" {
    var samples = std.ArrayList(f32).empty;
    defer samples.deinit(std.testing.allocator);

    try appendGranuleMonoPcm(
        std.testing.allocator,
        &samples,
        .{
            .version = .mpeg1,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 128,
            .sample_rate = 44100,
            .padding = false,
            .channel_mode = .joint_stereo,
            .mode_extension = 0,
        },
        &.{ 2.0, -2.0 },
        &.{ 0.0, 2.0 },
    );

    try std.testing.expectEqualSlices(f32, &.{ 1.0, 0.0 }, samples.items);
}

test "lsf intensity stereo long-band helper applies scalefac-compress table 0" {
    var left = @as([576]f32, @splat(0));
    var right = @as([576]f32, @splat(0));
    left[550] = 2.0;

    var scalefactors = requantize.BandScalefactors{
        .long_band_count = 21,
    };
    scalefactors.long[20] = 1;

    try applyStereoProcessingPartial(
        .{
            .version = .mpeg2,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 64,
            .sample_rate = 22050,
            .padding = false,
            .channel_mode = .joint_stereo,
            .mode_extension = 0b01,
        },
        .{
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
        },
        scalefactors,
        &left,
        &right,
    );

    try std.testing.expectApproxEqAbs(@as(f32, 2.0 * 0.840_896_4), left[550], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), right[550], 0.0001);
}

test "lsf intensity stereo long-band helper applies scalefac-compress table 1" {
    var left = @as([576]f32, @splat(0));
    var right = @as([576]f32, @splat(0));
    left[550] = 2.0;

    var scalefactors = requantize.BandScalefactors{
        .long_band_count = 21,
    };
    scalefactors.long[20] = 1;

    try applyStereoProcessingPartial(
        .{
            .version = .mpeg2,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 64,
            .sample_rate = 22050,
            .padding = false,
            .channel_mode = .joint_stereo,
            .mode_extension = 0b01,
        },
        .{
            .part2_3_length = 0,
            .big_values = 0,
            .global_gain = 0,
            .scalefac_compress = 1,
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
        },
        scalefactors,
        &left,
        &right,
    );

    try std.testing.expectApproxEqAbs(@as(f32, 2.0 * inv_sqrt2), left[550], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), right[550], 0.0001);
}

test "free-format stereo fixture decodes through zig downmix path" {
    const fixture = @embedFile("../../testdata/mp3-corpus/l3-he_free.bit");
    const decoded = try decodeMono(std.testing.allocator, fixture);
    defer std.testing.allocator.free(decoded.samples);

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 68 * 2 * 32 * 18), decoded.samples.len);
}

test "joint-stereo fixture decodes through zig interleaved stereo path" {
    const fixture = @embedFile("../../testdata/mp3-corpus/l3-he_mode.bit");
    const decoded = try decodeInterleaved(std.testing.allocator, fixture);
    defer std.testing.allocator.free(decoded.samples);

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 147456 * 2), decoded.samples.len);
}

test "checked-in mp3 conformance corpus passes through zig backend" {
    for (checked_in_conformance_cases) |case| {
        try assertConformanceCaseWithBackend(case, .zig);
    }
}

test "checked-in mp3 conformance corpus passes through facade backend selection" {
    for (checked_in_conformance_cases) |case| {
        try assertConformanceCaseThroughFacade(case);
    }
}
