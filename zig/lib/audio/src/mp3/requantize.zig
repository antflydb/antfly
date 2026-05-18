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
const bitstream = @import("bitstream.zig");
const huffman = @import("huffman.zig");

pub const GranuleChannel = struct {
    global_gain: u8 = 0,
    scalefac_compress: u16 = 0,
    block_type: u2 = 0,
    mixed_block_flag: bool = false,
};

pub const Part2Info = struct {
    bit_length: usize,
    slen: [4]u8,
    partition_scalefactor_bands: [4]u8,
    copy_partitions: [4]bool = .{ false, false, false, false },

    pub fn totalScalefactors(self: Part2Info) usize {
        var total: usize = 0;
        for (self.partition_scalefactor_bands) |count| total += count;
        return total;
    }
};

pub const RawScalefactors = struct {
    count: usize = 0,
    values: [39]u8 = [_]u8{0} ** 39,
};

pub const invalid_intensity_position: u8 = 0xFF;

pub const RawIntensityPositions = struct {
    count: usize = 0,
    values: [39]u8 = [_]u8{invalid_intensity_position} ** 39,
};

pub const BandScalefactors = struct {
    long: [21]u8 = [_]u8{0} ** 21,
    short: [3][13]u8 = [_][13]u8{[_]u8{0} ** 13} ** 3,
    intensity_long: [21]u8 = [_]u8{invalid_intensity_position} ** 21,
    intensity_short: [3][13]u8 = [_][13]u8{[_]u8{invalid_intensity_position} ** 13} ** 3,
    long_band_count: usize = 0,
    short_band_start: usize = 12,
    short_band_count: usize = 0,
};

pub const BandScalePlan = struct {
    long: [21]f32 = [_]f32{0} ** 21,
    short: [3][13]f32 = [_][13]f32{[_]f32{0} ** 13} ** 3,
    long_band_count: usize = 0,
    short_band_start: usize = 12,
    short_band_count: usize = 0,
};

pub const SpectralDecodeProgress = struct {
    samples_decoded: usize,
    unsupported_sample_index: ?usize = null,
};

const lsf_partition_count = [3][3][4]u8{
    .{
        .{ 6, 5, 5, 5 },
        .{ 9, 9, 9, 9 },
        .{ 6, 9, 9, 9 },
    },
    .{
        .{ 6, 5, 7, 3 },
        .{ 9, 9, 12, 6 },
        .{ 6, 9, 12, 6 },
    },
    .{
        .{ 11, 10, 0, 0 },
        .{ 18, 18, 0, 0 },
        .{ 15, 18, 0, 0 },
    },
};

const mpeg1_slen_table = [2][16]u8{
    .{ 0, 0, 0, 0, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4 },
    .{ 0, 1, 2, 3, 0, 1, 2, 3, 1, 2, 3, 1, 2, 3, 2, 3 },
};

const mpeg1_pretab = [21]u8{
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 2, 2, 3, 3, 3, 2,
};

pub fn computePart2Info(
    header: bitstream.FrameHeader,
    info: bitstream.GranuleChannelInfo,
    granule_index: usize,
    scfsi: [4]bool,
) !Part2Info {
    return switch (header.version) {
        .mpeg2, .mpeg25 => computePart2InfoLsf(info),
        .mpeg1 => computePart2InfoMpeg1(info, granule_index, scfsi),
    };
}

fn computePart2InfoMpeg1(
    info: bitstream.GranuleChannelInfo,
    granule_index: usize,
    scfsi: [4]bool,
) !Part2Info {
    if (info.scalefac_compress >= mpeg1_slen_table[0].len) return error.Mp3PureZigUnimplemented;

    const slen1 = mpeg1_slen_table[0][info.scalefac_compress];
    const slen2 = mpeg1_slen_table[1][info.scalefac_compress];

    if (info.window_switching_flag and info.block_type == 2) {
        const first_partition_count: usize = if (info.mixed_block_flag) 17 else 18;
        return .{
            .bit_length = first_partition_count * @as(usize, slen1) + 18 * @as(usize, slen2),
            .slen = .{ slen1, slen2, 0, 0 },
            .partition_scalefactor_bands = .{ if (info.mixed_block_flag) 17 else 18, 18, 0, 0 },
        };
    }

    const copy_partitions = if (granule_index == 0) [_]bool{ false, false, false, false } else scfsi;
    const counts = [4]u8{ 6, 5, 5, 5 };
    const slen = [4]u8{ slen1, slen1, slen2, slen2 };

    var bit_length: usize = 0;
    for (counts, slen, copy_partitions) |count, partition_slen, copy| {
        if (!copy) bit_length += @as(usize, count) * @as(usize, partition_slen);
    }

    return .{
        .bit_length = bit_length,
        .slen = slen,
        .partition_scalefactor_bands = counts,
        .copy_partitions = copy_partitions,
    };
}

fn computePart2InfoLsf(info: bitstream.GranuleChannelInfo) !Part2Info {
    var scalefac_compress = info.scalefac_compress;
    var slen: [4]u8 = .{ 0, 0, 0, 0 };
    var table_index: usize = 0;

    if (scalefac_compress < 400) {
        const high = scalefac_compress >> 4;
        slen[0] = @intCast(high / 5);
        slen[1] = @intCast(high % 5);
        slen[2] = @intCast((scalefac_compress & 0xF) >> 2);
        slen[3] = @intCast(scalefac_compress & 0x3);
        table_index = 0;
    } else if (scalefac_compress < 500) {
        scalefac_compress -= 400;
        const high = scalefac_compress >> 2;
        slen[0] = @intCast(high / 5);
        slen[1] = @intCast(high % 5);
        slen[2] = @intCast(scalefac_compress & 0x3);
        slen[3] = 0;
        table_index = 1;
    } else {
        scalefac_compress -= 500;
        slen[0] = @intCast(scalefac_compress / 3);
        slen[1] = @intCast(scalefac_compress % 3);
        slen[2] = 0;
        slen[3] = 0;
        table_index = 2;
    }

    const block_index: usize = if (info.window_switching_flag and info.block_type == 2)
        (if (info.mixed_block_flag) 2 else 1)
    else
        0;
    const partitions = lsf_partition_count[table_index][block_index];

    var bit_length: usize = 0;
    for (partitions, slen) |band_count, band_slen| {
        bit_length += @as(usize, band_count) * @as(usize, band_slen);
    }

    return .{
        .bit_length = bit_length,
        .slen = slen,
        .partition_scalefactor_bands = partitions,
    };
}

pub fn requantizeSamples(_: []f32, _: GranuleChannel) !void {
    return error.Mp3PureZigUnimplemented;
}

pub fn decodeRawScalefactors(
    payload_bytes: []const u8,
    payload_bit_offset: usize,
    part2: Part2Info,
) !RawScalefactors {
    var reader = huffman.BitSliceReader.init(payload_bytes, payload_bit_offset, part2.bit_length);
    var scalefactors = RawScalefactors{};

    for (part2.partition_scalefactor_bands, part2.slen, part2.copy_partitions) |band_count, band_slen, copy| {
        for (0..band_count) |_| {
            if (copy) continue;
            if (scalefactors.count >= scalefactors.values.len) return error.Mp3ScalefactorOverflow;
            scalefactors.values[scalefactors.count] = if (band_slen == 0)
                0
            else
                @intCast(try reader.readBits(@intCast(band_slen)));
            scalefactors.count += 1;
        }
    }

    return scalefactors;
}

pub fn decodeRawIntensityPositions(
    payload_bytes: []const u8,
    payload_bit_offset: usize,
    part2: Part2Info,
    sentinel_on_partition_max: bool,
) !RawIntensityPositions {
    var reader = huffman.BitSliceReader.init(payload_bytes, payload_bit_offset, part2.bit_length);
    var positions = RawIntensityPositions{};

    for (part2.partition_scalefactor_bands, part2.slen, part2.copy_partitions) |band_count, band_slen, copy| {
        for (0..band_count) |_| {
            if (copy) continue;
            if (positions.count >= positions.values.len) return error.Mp3ScalefactorOverflow;
            if (band_slen == 0) {
                positions.values[positions.count] = 0;
            } else {
                const value: u8 = @intCast(try reader.readBits(@intCast(band_slen)));
                if (sentinel_on_partition_max and value == ((@as(u8, 1) << @intCast(band_slen)) - 1)) {
                    positions.values[positions.count] = invalid_intensity_position;
                } else {
                    positions.values[positions.count] = value;
                }
            }
            positions.count += 1;
        }
    }

    return positions;
}

pub fn expandScalefactors(
    header: bitstream.FrameHeader,
    info: bitstream.GranuleChannelInfo,
    part2: Part2Info,
    raw: RawScalefactors,
    previous: ?BandScalefactors,
) !BandScalefactors {
    return expandScalefactorsWithIntensity(header, info, part2, raw, null, previous);
}

pub fn expandScalefactorsWithIntensity(
    header: bitstream.FrameHeader,
    info: bitstream.GranuleChannelInfo,
    part2: Part2Info,
    raw: RawScalefactors,
    raw_intensity: ?RawIntensityPositions,
    previous: ?BandScalefactors,
) !BandScalefactors {
    var expanded = BandScalefactors{};
    var raw_index: usize = 0;

    if (header.version == .mpeg1) {
        if (info.window_switching_flag and info.block_type == 2) {
            if (info.mixed_block_flag) {
                expanded.long_band_count = 8;
                for (0..expanded.long_band_count) |band| {
                    if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                    expanded.long[band] = raw.values[raw_index];
                    expanded.intensity_long[band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                    raw_index += 1;
                }

                expanded.short_band_start = 3;
                expanded.short_band_count = 10;

                for (3..6) |band| {
                    for (0..3) |window| {
                        if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                        expanded.short[window][band] = raw.values[raw_index];
                        expanded.intensity_short[window][band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                        raw_index += 1;
                    }
                }

                for (6..12) |band| {
                    for (0..3) |window| {
                        if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                        expanded.short[window][band] = raw.values[raw_index];
                        expanded.intensity_short[window][band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                        raw_index += 1;
                    }
                }
            } else {
                expanded.short_band_start = 0;
                expanded.short_band_count = 13;
                for (0..12) |band| {
                    for (0..3) |window| {
                        if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                        expanded.short[window][band] = raw.values[raw_index];
                        expanded.intensity_short[window][band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                        raw_index += 1;
                    }
                }
            }
        } else {
            const previous_scales = previous orelse BandScalefactors{};
            const part_ranges = [4][2]usize{
                .{ 0, 6 },
                .{ 6, 11 },
                .{ 11, 16 },
                .{ 16, 21 },
            };

            expanded.long_band_count = 21;
            for (part_ranges, 0..) |range, part_index| {
                const start = range[0];
                const end = range[1];
                for (start..end) |band| {
                    if (part2.copy_partitions[part_index]) {
                        expanded.long[band] = previous_scales.long[band];
                        expanded.intensity_long[band] = previous_scales.intensity_long[band];
                    } else {
                        if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                        expanded.long[band] = raw.values[raw_index];
                        expanded.intensity_long[band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                        raw_index += 1;
                    }
                }
            }
        }
    } else if (info.window_switching_flag and info.block_type == 2) {
        if (info.mixed_block_flag) {
            expanded.long_band_count = 6;
            for (0..expanded.long_band_count) |band| {
                if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                expanded.long[band] = raw.values[raw_index];
                expanded.intensity_long[band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                raw_index += 1;
            }

            expanded.short_band_start = 3;
            expanded.short_band_count = 10;
            for (expanded.short_band_start..expanded.short_band_start + expanded.short_band_count) |band| {
                if (band >= 12) break;
                for (0..3) |window| {
                    if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                    expanded.short[window][band] = raw.values[raw_index];
                    expanded.intensity_short[window][band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                    raw_index += 1;
                }
            }
        } else {
            expanded.short_band_start = 0;
            expanded.short_band_count = 13;
            for (0..12) |band| {
                for (0..3) |window| {
                    if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
                    expanded.short[window][band] = raw.values[raw_index];
                    expanded.intensity_short[window][band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
                    raw_index += 1;
                }
            }
        }
    } else {
        expanded.long_band_count = 21;
        for (0..expanded.long_band_count) |band| {
            if (raw_index >= raw.count) return error.Mp3ScalefactorUnderflow;
            expanded.long[band] = raw.values[raw_index];
            expanded.intensity_long[band] = if (raw_intensity) |intensity| intensity.values[raw_index] else raw.values[raw_index];
            raw_index += 1;
        }
    }

    if (raw_index != raw.count) return error.Mp3ScalefactorCountMismatch;
    return expanded;
}

pub fn buildBandScalePlan(
    header: bitstream.FrameHeader,
    info: bitstream.GranuleChannelInfo,
    scalefactors: BandScalefactors,
) !BandScalePlan {
    const scalefac_multiplier: i32 = if (info.scalefac_scale) 2 else 1;
    const gain_bias: i32 = 210;
    const ms_gain_bias: i32 = if (header.version == .mpeg1 and header.usesMsStereo()) 2 else 0;
    var plan = BandScalePlan{
        .long_band_count = scalefactors.long_band_count,
        .short_band_start = scalefactors.short_band_start,
        .short_band_count = scalefactors.short_band_count,
    };

    for (0..scalefactors.long_band_count) |band| {
        const pretab = if (header.version == .mpeg1 and info.preflag and band < mpeg1_pretab.len)
            mpeg1_pretab[band]
        else
            0;
        const total_shift = 2 * scalefac_multiplier * (@as(i32, scalefactors.long[band]) + @as(i32, pretab));
        plan.long[band] = gainToScale(@as(i32, info.global_gain) - gain_bias - ms_gain_bias - total_shift);
    }

    for (scalefactors.short_band_start..scalefactors.short_band_start + scalefactors.short_band_count) |band| {
        for (0..3) |window| {
            const total_shift = (8 * @as(i32, info.subblock_gain[window])) +
                (2 * scalefac_multiplier * @as(i32, scalefactors.short[window][band]));
            plan.short[window][band] = gainToScale(@as(i32, info.global_gain) - gain_bias - ms_gain_bias - total_shift);
        }
    }

    return plan;
}

pub fn requantizeBigValuePairsLong(
    sample_rate: u32,
    pairs: []const huffman.DecodedPair,
    band_scales: BandScalePlan,
    out_coefficients: []f32,
) !SpectralDecodeProgress {
    const long_bands = scalefactorBandLong(sample_rate);
    const max_samples = @min(out_coefficients.len, pairs.len * 2);

    @memset(out_coefficients, 0);

    var sample_index: usize = 0;
    var band_index: usize = 0;
    while (sample_index < max_samples) : (sample_index += 1) {
        while (band_index + 1 < long_bands.len and sample_index >= long_bands[band_index + 1]) {
            band_index += 1;
        }
        if (band_index >= band_scales.long_band_count) {
            return .{
                .samples_decoded = sample_index,
                .unsupported_sample_index = sample_index,
            };
        }

        const pair = pairs[sample_index / 2];
        const value = if ((sample_index & 1) == 0) pair.x else pair.y;
        out_coefficients[sample_index] = requantizeValue(value, band_scales.long[band_index]);
    }

    return .{
        .samples_decoded = max_samples,
        .unsupported_sample_index = null,
    };
}

pub fn requantizeBigValuePairs(
    header: bitstream.FrameHeader,
    info: bitstream.GranuleChannelInfo,
    pairs: []const huffman.DecodedPair,
    band_scales: BandScalePlan,
    out_coefficients: []f32,
) !SpectralDecodeProgress {
    if (info.window_switching_flag and info.block_type == 2) {
        return requantizeBigValuePairsShort(header.sample_rate, info, pairs, band_scales, out_coefficients);
    }
    return requantizeBigValuePairsLong(header.sample_rate, pairs, band_scales, out_coefficients);
}

pub fn requantizeCount1Quads(
    header: bitstream.FrameHeader,
    info: bitstream.GranuleChannelInfo,
    band_scales: BandScalePlan,
    start_sample: usize,
    quads: []const huffman.DecodedQuad,
    out_coefficients: []f32,
) !SpectralDecodeProgress {
    if (info.window_switching_flag and info.block_type == 2) {
        return requantizeCount1QuadsShort(header.sample_rate, info, band_scales, start_sample, quads, out_coefficients);
    }
    return requantizeCount1QuadsLong(header.sample_rate, band_scales, start_sample, quads, out_coefficients);
}

pub fn applyAliasReduction(
    info: bitstream.GranuleChannelInfo,
    coefficients: []f32,
) void {
    if (coefficients.len < 576) return;

    const pair_count: usize = if (info.window_switching_flag and info.block_type == 2)
        (if (info.mixed_block_flag) 1 else 0)
    else
        31;
    if (pair_count == 0) return;

    for (0..pair_count) |sb| {
        const lower_base = sb * 18;
        const upper_base = lower_base + 18;
        for (0..8) |i| {
            const li = lower_base + 17 - i;
            const ui = upper_base + i;
            const lower = coefficients[li];
            const upper = coefficients[ui];

            coefficients[ui] = (upper * alias_cs[i]) - (lower * alias_ca[i]);
            coefficients[li] = (upper * alias_ca[i]) + (lower * alias_cs[i]);
        }
    }
}

pub fn requantizeBigValuePairsShort(
    sample_rate: u32,
    info: bitstream.GranuleChannelInfo,
    pairs: []const huffman.DecodedPair,
    band_scales: BandScalePlan,
    out_coefficients: []f32,
) !SpectralDecodeProgress {
    const short_bands = scalefactorBandShort(sample_rate);
    const total_samples = @min(out_coefficients.len, pairs.len * 2);
    @memset(out_coefficients, 0);

    var source_sample: usize = 0;

    if (info.mixed_block_flag) {
        const long_samples = @min(total_samples, 36);
        var mixed_long_scales = BandScalePlan{
            .long_band_count = band_scales.long_band_count,
        };
        @memcpy(mixed_long_scales.long[0..band_scales.long_band_count], band_scales.long[0..band_scales.long_band_count]);
        const long_progress = try requantizeBigValuePairsLong(sample_rate, pairs[0 .. long_samples / 2], mixed_long_scales, out_coefficients[0..long_samples]);
        source_sample = long_progress.samples_decoded;
    }

    for (band_scales.short_band_start..band_scales.short_band_start + band_scales.short_band_count) |band| {
        const band_start = short_bands[band];
        const band_end = short_bands[band + 1];
        const band_width = band_end - band_start;

        for (0..3) |window| {
            for (0..band_width) |_| {
                if (source_sample >= total_samples) {
                    return .{
                        .samples_decoded = source_sample,
                        .unsupported_sample_index = null,
                    };
                }

                const pair = pairs[source_sample / 2];
                const value = if ((source_sample & 1) == 0) pair.x else pair.y;
                const dest_index = source_sample;
                if (dest_index >= out_coefficients.len) {
                    return .{
                        .samples_decoded = source_sample,
                        .unsupported_sample_index = source_sample,
                    };
                }
                out_coefficients[dest_index] = requantizeValue(value, band_scales.short[window][band]);
                source_sample += 1;
            }
        }
    }

    return .{
        .samples_decoded = source_sample,
        .unsupported_sample_index = if (source_sample < total_samples) source_sample else null,
    };
}

fn gainToScale(gain: i32) f32 {
    return std.math.pow(f32, 2.0, @as(f32, @floatFromInt(gain)) / 4.0);
}

fn requantizeCount1QuadsLong(
    sample_rate: u32,
    band_scales: BandScalePlan,
    start_sample: usize,
    quads: []const huffman.DecodedQuad,
    out_coefficients: []f32,
) !SpectralDecodeProgress {
    const long_bands = scalefactorBandLong(sample_rate);
    var sample_index = start_sample;
    var band_index: usize = 0;
    while (band_index + 1 < long_bands.len and sample_index >= long_bands[band_index + 1]) : (band_index += 1) {}

    for (quads) |quad| {
        const values = [_]i16{ quad.v, quad.w, quad.x, quad.y };
        for (values) |value| {
            if (sample_index >= out_coefficients.len) {
                return .{
                    .samples_decoded = sample_index,
                    .unsupported_sample_index = null,
                };
            }
            while (band_index + 1 < long_bands.len and sample_index >= long_bands[band_index + 1]) {
                band_index += 1;
            }
            if (band_index >= band_scales.long_band_count) {
                return .{
                    .samples_decoded = sample_index,
                    .unsupported_sample_index = sample_index,
                };
            }
            out_coefficients[sample_index] = requantizeValue(value, band_scales.long[band_index]);
            sample_index += 1;
        }
    }

    return .{
        .samples_decoded = sample_index,
        .unsupported_sample_index = null,
    };
}

fn requantizeCount1QuadsShort(
    sample_rate: u32,
    info: bitstream.GranuleChannelInfo,
    band_scales: BandScalePlan,
    start_sample: usize,
    quads: []const huffman.DecodedQuad,
    out_coefficients: []f32,
) !SpectralDecodeProgress {
    const short_bands = scalefactorBandShort(sample_rate);
    var source_sample = start_sample;
    var quad_index: usize = 0;
    var value_index: usize = 0;

    if (info.mixed_block_flag and source_sample < 36) {
        const long_progress = try requantizeCount1QuadsLong(sample_rate, band_scales, source_sample, quads, out_coefficients);
        if (long_progress.samples_decoded < 36) return long_progress;
        source_sample = long_progress.samples_decoded;
        const consumed_values = source_sample - start_sample;
        quad_index = consumedValuesToQuadIndex(consumed_values);
        value_index = consumed_values % 4;
    }

    for (band_scales.short_band_start..band_scales.short_band_start + band_scales.short_band_count) |band| {
        const band_start = short_bands[band];
        const band_end = short_bands[band + 1];
        const band_width = band_end - band_start;

        for (0..3) |window| {
            for (0..band_width) |_| {
                if (source_sample < start_sample) {
                    source_sample += 1;
                    continue;
                }
                if (quad_index >= quads.len) {
                    return .{
                        .samples_decoded = source_sample,
                        .unsupported_sample_index = null,
                    };
                }
                const dest_index = source_sample;
                if (dest_index >= out_coefficients.len) {
                    return .{
                        .samples_decoded = source_sample,
                        .unsupported_sample_index = source_sample,
                    };
                }
                const quad = quads[quad_index];
                const values = [_]i16{ quad.v, quad.w, quad.x, quad.y };
                out_coefficients[dest_index] = requantizeValue(values[value_index], band_scales.short[window][band]);
                source_sample += 1;
                value_index += 1;
                if (value_index == 4) {
                    value_index = 0;
                    quad_index += 1;
                }
            }
        }
    }

    return .{
        .samples_decoded = source_sample,
        .unsupported_sample_index = null,
    };
}

fn consumedValuesToQuadIndex(values: usize) usize {
    return values / 4;
}

pub fn reorderShortCoefficients(
    sample_rate: u32,
    info: bitstream.GranuleChannelInfo,
    coefficients: []f32,
) void {
    if (!info.window_switching_flag or info.block_type != 2) return;
    if (coefficients.len < 576) return;

    const short_bands = scalefactorBandShort(sample_rate);
    var reordered: [576]f32 = [_]f32{0} ** 576;

    var source_offset: usize = 0;
    var dest_offset: usize = 0;
    var band_start_index: usize = 0;

    if (info.mixed_block_flag) {
        @memcpy(reordered[0..36], coefficients[0..36]);
        source_offset = 36;
        dest_offset = 36;
        band_start_index = 3;
    }

    for (band_start_index..12) |band| {
        const band_width = short_bands[band + 1] - short_bands[band];
        const block = coefficients[source_offset .. source_offset + band_width * 3];
        for (0..band_width) |line| {
            reordered[dest_offset + line * 3 + 0] = block[line];
            reordered[dest_offset + line * 3 + 1] = block[band_width + line];
            reordered[dest_offset + line * 3 + 2] = block[band_width * 2 + line];
        }
        source_offset += band_width * 3;
        dest_offset += band_width * 3;
    }

    @memcpy(coefficients[0..dest_offset], reordered[0..dest_offset]);
}

const alias_ca = [_]f32{
    0.514_495_73,
    0.471_731_96,
    0.313_377_44,
    0.181_913_2,
    0.094_574_19,
    0.040_965_583,
    0.014_198_569,
    0.003_699_974_8,
};

const alias_cs = [_]f32{
    0.857_492_9,
    0.881_742,
    0.949_628_65,
    0.983_314_6,
    0.995_517_8,
    0.999_160_6,
    0.999_899_2,
    0.999_993_15,
};

fn requantizeValue(value: i16, scale: f32) f32 {
    if (value == 0) return 0;
    const magnitude = pow43(value);
    return if (value < 0) -(magnitude * scale) else magnitude * scale;
}

fn pow43(value: i16) f32 {
    const x: usize = @intCast(@abs(value));
    if (x < pow43_table.len) return pow43_table[x];

    var scaled = x;
    var mult: f32 = 1.0;
    if (scaled < 1024) {
        scaled <<= 3;
        mult = 16.0;
    } else {
        mult = 256.0;
    }

    const sign = (2 * scaled) & 64;
    const frac = @as(f32, @floatFromInt(@as(i32, @intCast(scaled & 63)) - @as(i32, @intCast(sign)))) /
        @as(f32, @floatFromInt((scaled & ~@as(usize, 63)) + sign));
    const base = pow43_table[(scaled + sign) >> 6];
    return base * (1.0 + frac * ((4.0 / 3.0) + frac * (2.0 / 9.0))) * mult;
}

const pow43_table = [_]f32{
    0.0,       1.0,       2.519842,   4.326749,  6.349604,   8.54988,   10.902724, 13.390518, 16.0,       18.720754,
    21.544348, 24.463781, 27.473143,  30.56735,  33.741993,  36.99318,  40.317474, 43.71179,  47.173344,  50.69963,
    54.288353, 57.93741,  61.644867,  65.40894,  69.22798,   73.10044,  77.024895, 81.0,      85.02449,   89.09719,
    93.21697,  97.3828,   101.593666, 105.84863, 110.146805, 114.48732, 118.86938, 123.29221, 127.755066, 132.25725,
    136.79808, 141.3769,  145.99312,  150.64612, 155.33533,  160.0602,  164.8202,  169.61482, 174.44357,  179.30598,
    184.20157, 189.12991, 194.09058,  199.08315, 204.10721,  209.16238, 214.24829, 219.36456, 224.51085,  229.68678,
    234.89206, 240.12633, 245.38928,  250.6806,  256.0,      261.34717, 266.72183, 272.12372, 277.55255,  283.00806,
    288.48996, 293.99805, 299.53207,  305.09177, 310.6769,   316.28726, 321.92258, 327.5827,  333.26736,  338.97638,
    344.70956, 350.46664, 356.2475,   362.05188, 367.8796,   373.73053, 379.60443, 385.50113, 391.4205,   397.3623,
    403.32642, 409.31268, 415.3209,   421.35092, 427.4026,   433.47574, 439.57028, 445.686,   451.82275,  457.98044,
    464.15887, 470.35797, 476.57755,  482.81744, 489.0776,   495.35788, 501.65808, 507.97815, 514.31793,  520.6773,
    527.0562,  533.4544,  539.8719,   546.3085,  552.76404,  559.2386,  565.7319,  572.2439,  578.7744,   585.3235,
    591.8909,  598.47656, 605.08044,  611.70233, 618.3422,   625.0,     631.67554, 638.3688,  645.0796,
};

pub fn scalefactorBandLong(sample_rate: u32) []const usize {
    return switch (sample_rate) {
        44100 => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 44, 52, 62, 74, 90, 110, 134, 162, 196, 238, 288, 342, 418, 576 },
        48000 => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 42, 50, 60, 72, 88, 106, 128, 156, 190, 230, 276, 330, 384, 576 },
        32000 => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 44, 54, 66, 82, 102, 126, 156, 194, 240, 296, 364, 448, 550, 576 },
        24000 => &.{ 0, 6, 12, 18, 24, 30, 36, 44, 54, 66, 80, 96, 114, 136, 162, 194, 232, 278, 330, 394, 464, 540, 576 },
        22050, 16000, 12000, 11025 => &.{ 0, 6, 12, 18, 24, 30, 36, 44, 54, 66, 80, 96, 116, 140, 168, 200, 238, 284, 336, 396, 464, 522, 576 },
        else => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 42, 50, 60, 72, 88, 106, 128, 156, 190, 230, 276, 330, 384, 576 },
    };
}

pub fn scalefactorBandShort(sample_rate: u32) []const usize {
    return switch (sample_rate) {
        44100 => &.{ 0, 4, 8, 12, 16, 22, 30, 40, 52, 66, 84, 106, 136, 192 },
        48000 => &.{ 0, 4, 8, 12, 16, 22, 28, 38, 50, 64, 80, 100, 126, 192 },
        32000 => &.{ 0, 4, 8, 12, 16, 22, 30, 42, 58, 78, 104, 138, 180, 192 },
        16000 => &.{ 0, 4, 8, 12, 18, 26, 36, 48, 62, 80, 104, 134, 174, 192 },
        else => @panic("unsupported short-band sample rate"),
    };
}

test "compute LSF part2 info for long blocks" {
    const header = bitstream.FrameHeader{
        .version = .mpeg2,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 64,
        .sample_rate = 16000,
        .padding = false,
        .channel_mode = .mono,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 137,
        .window_switching_flag = false,
        .block_type = 0,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 7 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 6,
        .region1_count = 7,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    const part2 = try computePart2Info(header, info, 0, .{ false, false, false, false });
    try std.testing.expectEqual([4]u8{ 1, 3, 2, 1 }, part2.slen);
    try std.testing.expectEqual([4]u8{ 6, 5, 5, 5 }, part2.partition_scalefactor_bands);
    try std.testing.expectEqual(@as(usize, 31), part2.bit_length);
}

test "compute LSF part2 info for short blocks" {
    const header = bitstream.FrameHeader{
        .version = .mpeg2,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 64,
        .sample_rate = 16000,
        .padding = false,
        .channel_mode = .mono,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 215,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    const part2 = try computePart2Info(header, info, 0, .{ false, false, false, false });
    try std.testing.expectEqual([4]u8{ 2, 3, 0, 3 }, part2.slen);
    try std.testing.expectEqual([4]u8{ 9, 9, 9, 9 }, part2.partition_scalefactor_bands);
    try std.testing.expectEqual(@as(usize, 72), part2.bit_length);
}

test "decode raw scalefactors consumes synthetic long-block part2 payload" {
    const part2 = Part2Info{
        .bit_length = 7,
        .slen = .{ 1, 2, 0, 1 },
        .partition_scalefactor_bands = .{ 2, 1, 2, 1 },
    };
    const scalefactors = try decodeRawScalefactors(&.{0b10010010}, 0, part2);
    try std.testing.expectEqual(@as(usize, 6), scalefactors.count);
    try std.testing.expectEqualSlices(u8, &.{ 1, 0, 2, 0, 0, 1 }, scalefactors.values[0..scalefactors.count]);
}

test "expand long-block scalefactors fills 21 long bands" {
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 137,
        .window_switching_flag = false,
        .block_type = 0,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 7 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 6,
        .region1_count = 7,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var raw = RawScalefactors{ .count = 21 };
    for (0..21) |i| raw.values[i] = @intCast(i + 1);

    const expanded = try expandScalefactors(
        .{
            .version = .mpeg2,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 64,
            .sample_rate = 16000,
            .padding = false,
            .channel_mode = .mono,
        },
        info,
        .{ .bit_length = 0, .slen = .{ 0, 0, 0, 0 }, .partition_scalefactor_bands = .{ 0, 0, 0, 0 } },
        raw,
        null,
    );
    try std.testing.expectEqual(@as(usize, 21), expanded.long_band_count);
    try std.testing.expectEqual(@as(usize, 0), expanded.short_band_count);
    try std.testing.expectEqualSlices(u8, raw.values[0..21], expanded.long[0..21]);
}

test "expand short-block scalefactors maps band-major input to per-window storage" {
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 215,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var raw = RawScalefactors{ .count = 36 };
    for (0..36) |i| raw.values[i] = @intCast(i);

    const expanded = try expandScalefactors(
        .{
            .version = .mpeg2,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 64,
            .sample_rate = 16000,
            .padding = false,
            .channel_mode = .mono,
        },
        info,
        .{ .bit_length = 0, .slen = .{ 0, 0, 0, 0 }, .partition_scalefactor_bands = .{ 0, 0, 0, 0 } },
        raw,
        null,
    );
    try std.testing.expectEqual(@as(usize, 0), expanded.long_band_count);
    try std.testing.expectEqual(@as(usize, 0), expanded.short_band_start);
    try std.testing.expectEqual(@as(usize, 13), expanded.short_band_count);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[0][0]);
    try std.testing.expectEqual(@as(u8, 1), expanded.short[1][0]);
    try std.testing.expectEqual(@as(u8, 2), expanded.short[2][0]);
    try std.testing.expectEqual(@as(u8, 33), expanded.short[0][11]);
    try std.testing.expectEqual(@as(u8, 34), expanded.short[1][11]);
    try std.testing.expectEqual(@as(u8, 35), expanded.short[2][11]);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[0][12]);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[1][12]);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[2][12]);
}

test "expand mixed short-block scalefactors preserves long prefix and short band offset" {
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 215,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = true,
        .table_select = .{ 5, 6, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var raw = RawScalefactors{ .count = 33 };
    for (0..33) |i| raw.values[i] = @intCast(i + 10);

    const expanded = try expandScalefactors(
        .{
            .version = .mpeg2,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 64,
            .sample_rate = 16000,
            .padding = false,
            .channel_mode = .mono,
        },
        info,
        .{ .bit_length = 0, .slen = .{ 0, 0, 0, 0 }, .partition_scalefactor_bands = .{ 0, 0, 0, 0 } },
        raw,
        null,
    );
    try std.testing.expectEqual(@as(usize, 6), expanded.long_band_count);
    try std.testing.expectEqual(@as(usize, 3), expanded.short_band_start);
    try std.testing.expectEqual(@as(usize, 10), expanded.short_band_count);
    try std.testing.expectEqualSlices(u8, raw.values[0..6], expanded.long[0..6]);
    try std.testing.expectEqual(@as(u8, 16), expanded.short[0][3]);
    try std.testing.expectEqual(@as(u8, 17), expanded.short[1][3]);
    try std.testing.expectEqual(@as(u8, 18), expanded.short[2][3]);
    try std.testing.expectEqual(@as(u8, 40), expanded.short[0][11]);
    try std.testing.expectEqual(@as(u8, 41), expanded.short[1][11]);
    try std.testing.expectEqual(@as(u8, 42), expanded.short[2][11]);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[0][12]);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[1][12]);
    try std.testing.expectEqual(@as(u8, 0), expanded.short[2][12]);
}

test "build long-band scale plan uses global gain and scalefactor attenuation" {
    const header = bitstream.FrameHeader{
        .version = .mpeg2,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 64,
        .sample_rate = 16000,
        .padding = false,
        .channel_mode = .mono,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 137,
        .window_switching_flag = false,
        .block_type = 0,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 7 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 6,
        .region1_count = 7,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var scalefactors = BandScalefactors{ .long_band_count = 21 };
    scalefactors.long[0] = 0;
    scalefactors.long[1] = 1;
    scalefactors.long[2] = 3;

    const plan = try buildBandScalePlan(header, info, scalefactors);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), plan.long[0], 0.0001);
    try std.testing.expect(plan.long[1] < plan.long[0]);
    try std.testing.expect(plan.long[2] < plan.long[1]);
}

test "build long-band scale plan applies mpeg1 ms stereo gain bias" {
    const mono_header = bitstream.FrameHeader{
        .version = .mpeg1,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 128,
        .sample_rate = 44100,
        .padding = false,
        .channel_mode = .stereo,
        .mode_extension = 0,
    };
    const ms_header = bitstream.FrameHeader{
        .version = .mpeg1,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 128,
        .sample_rate = 44100,
        .padding = false,
        .channel_mode = .joint_stereo,
        .mode_extension = 0b10,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 0,
        .window_switching_flag = false,
        .block_type = 0,
        .mixed_block_flag = false,
        .table_select = .{ 7, 20, 29 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 7,
        .region1_count = 13,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    const scalefactors = BandScalefactors{ .long_band_count = 21 };

    const mono_plan = try buildBandScalePlan(mono_header, info, scalefactors);
    const ms_plan = try buildBandScalePlan(ms_header, info, scalefactors);

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), mono_plan.long[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.70710677), ms_plan.long[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, mono_plan.long[0] / std.math.sqrt(@as(f32, 2.0))), ms_plan.long[0], 0.0001);
}

test "build short-band scale plan applies subblock gain per window" {
    const header = bitstream.FrameHeader{
        .version = .mpeg2,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 64,
        .sample_rate = 16000,
        .padding = false,
        .channel_mode = .mono,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 10,
        .global_gain = 210,
        .scalefac_compress = 215,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 0 },
        .subblock_gain = .{ 0, 1, 2 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = true,
        .count1table_select = false,
    };
    var scalefactors = BandScalefactors{
        .short_band_start = 0,
        .short_band_count = 13,
    };
    scalefactors.short[0][0] = 1;
    scalefactors.short[1][0] = 1;
    scalefactors.short[2][0] = 1;

    const plan = try buildBandScalePlan(header, info, scalefactors);
    try std.testing.expect(plan.short[0][0] > plan.short[1][0]);
    try std.testing.expect(plan.short[1][0] > plan.short[2][0]);
}

test "requantize long big values uses band scales and preserves sign" {
    var scales = BandScalePlan{ .long_band_count = 21 };
    scales.long[0] = 1.0;
    scales.long[1] = 0.5;

    const pairs = [_]huffman.DecodedPair{
        .{ .x = 1, .y = -8 },
        .{ .x = 0, .y = 2 },
    };
    var coeffs = [_]f32{0} ** 8;
    const progress = try requantizeBigValuePairsLong(16000, &pairs, scales, &coeffs);
    try std.testing.expectEqual(@as(usize, 4), progress.samples_decoded);
    try std.testing.expect(progress.unsupported_sample_index == null);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), coeffs[0], 0.0001);
    try std.testing.expect(coeffs[1] < 0);
    try std.testing.expectEqual(@as(f32, 0), coeffs[2]);
    try std.testing.expect(coeffs[3] > coeffs[0]);
}

test "requantize short big values reorders by band and window" {
    const header = bitstream.FrameHeader{
        .version = .mpeg2,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 64,
        .sample_rate = 16000,
        .padding = false,
        .channel_mode = .mono,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 12,
        .global_gain = 210,
        .scalefac_compress = 215,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var scales = BandScalePlan{
        .short_band_start = 0,
        .short_band_count = 13,
    };
    for (0..13) |band| {
        scales.short[0][band] = 1.0;
        scales.short[1][band] = 1.0;
        scales.short[2][band] = 1.0;
    }

    const pairs = [_]huffman.DecodedPair{
        .{ .x = 1, .y = 2 }, .{ .x = 3, .y = 4 },  .{ .x = 5, .y = 6 },
        .{ .x = 7, .y = 8 }, .{ .x = 9, .y = 10 }, .{ .x = 11, .y = 12 },
    };
    var coeffs = [_]f32{0} ** 64;
    const progress = try requantizeBigValuePairs(header, info, &pairs, scales, &coeffs);
    try std.testing.expectEqual(@as(usize, 12), progress.samples_decoded);
    try std.testing.expect(progress.unsupported_sample_index == null);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), coeffs[0], 0.0001);
    try std.testing.expect(coeffs[1] > coeffs[0]);
    try std.testing.expect(coeffs[2] > coeffs[1]);
    try std.testing.expectApproxEqAbs(@as(f32, std.math.pow(f32, 4.0, 4.0 / 3.0)), coeffs[3], 0.001);
}

test "mpeg1 short scalefactor band tables are available for 44.1/48/32 kHz" {
    try std.testing.expectEqualSlices(usize, &.{ 0, 4, 8, 12, 16, 22, 30, 40, 52, 66, 84, 106, 136, 192 }, scalefactorBandShort(44100));
    try std.testing.expectEqualSlices(usize, &.{ 0, 4, 8, 12, 16, 22, 28, 38, 50, 64, 80, 100, 126, 192 }, scalefactorBandShort(48000));
    try std.testing.expectEqualSlices(usize, &.{ 0, 4, 8, 12, 16, 22, 30, 42, 58, 78, 104, 138, 180, 192 }, scalefactorBandShort(32000));
}

test "requantize mixed big values keeps first 36 samples long and reorders remainder short" {
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 24,
        .global_gain = 210,
        .scalefac_compress = 215,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = true,
        .table_select = .{ 5, 6, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var scales = BandScalePlan{
        .long_band_count = 6,
        .short_band_start = 3,
        .short_band_count = 10,
    };
    for (0..6) |band| scales.long[band] = 1.0;
    for (3..12) |band| {
        scales.short[0][band] = 1.0;
        scales.short[1][band] = 1.0;
        scales.short[2][band] = 1.0;
    }

    var pairs: [24]huffman.DecodedPair = undefined;
    for (0..24) |i| {
        pairs[i] = .{ .x = @intCast(i * 2 + 1), .y = @intCast(i * 2 + 2) };
    }
    var coeffs = [_]f32{0} ** 128;
    const progress = try requantizeBigValuePairs(
        .{
            .version = .mpeg2,
            .layer = .layer3,
            .has_crc = false,
            .free_format = false,
            .bitrate_kbps = 64,
            .sample_rate = 16000,
            .padding = false,
            .channel_mode = .mono,
        },
        info,
        &pairs,
        scales,
        &coeffs,
    );
    try std.testing.expectEqual(@as(usize, 48), progress.samples_decoded);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), coeffs[0], 0.0001);
    try std.testing.expect(coeffs[36] != 0);
}

test "alias reduction changes long block boundary coefficients" {
    const info = bitstream.GranuleChannelInfo{
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
    var coeffs = [_]f32{0} ** 576;
    coeffs[17] = 1;
    coeffs[18] = 2;

    applyAliasReduction(info, coeffs[0..]);

    try std.testing.expect(coeffs[17] != 1);
    try std.testing.expect(coeffs[18] != 2);
}

test "alias reduction skips pure short blocks" {
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 0,
        .big_values = 0,
        .global_gain = 0,
        .scalefac_compress = 0,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = false,
        .table_select = .{ 0, 0, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 0,
        .region1_count = 0,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var coeffs = [_]f32{0} ** 576;
    coeffs[17] = 1;
    coeffs[18] = 2;

    applyAliasReduction(info, coeffs[0..]);

    try std.testing.expectEqual(@as(f32, 1), coeffs[17]);
    try std.testing.expectEqual(@as(f32, 2), coeffs[18]);
}

test "alias reduction on mixed blocks only touches first long boundary" {
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 0,
        .big_values = 0,
        .global_gain = 0,
        .scalefac_compress = 0,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = true,
        .table_select = .{ 0, 0, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 0,
        .region1_count = 0,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    var coeffs = [_]f32{0} ** 576;
    coeffs[17] = 1;
    coeffs[18] = 2;
    coeffs[35] = 3;
    coeffs[36] = 4;

    applyAliasReduction(info, coeffs[0..]);

    try std.testing.expect(coeffs[17] != 1);
    try std.testing.expect(coeffs[18] != 2);
    try std.testing.expectEqual(@as(f32, 3), coeffs[35]);
    try std.testing.expectEqual(@as(f32, 4), coeffs[36]);
}
