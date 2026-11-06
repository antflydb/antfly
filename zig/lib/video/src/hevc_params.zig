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

//! HEVC/H.265 parameter set syntax: profile_tier_level (7.3.3), VPS (7.3.2.1,
//! stored only far enough to validate layering), SPS (7.3.2.2), PPS (7.3.2.3),
//! short-term RPS (7.3.7/7.4.8) and scaling_list_data (7.3.4). Written from
//! the specification; field order/bit widths were cross-checked against this
//! repository's own fixtures using FFmpeg's `trace_headers` bitstream filter
//! as an independent oracle, never against any decoder's source.
const std = @import("std");
const Bits = @import("hevc_bits.zig").Bits;

pub const max_sub_layers = 8;
pub const max_short_term_rps = 65;
pub const max_rps_entries = 16;
pub const max_sps_count = 16;
pub const max_pps_count = 64;
/// Bounds a single delta_poc_*_minus1-derived magnitude well below any real
/// stream's needs, leaving generous headroom against i32 overflow even after
/// up to max_short_term_rps hops of inter-RPS-predicted compounding.
pub const max_delta_poc: u32 = 1 << 20;

pub const ProfileTierLevel = struct {
    general_profile_space: u2 = 0,
    general_tier_flag: bool = false,
    general_profile_idc: u5 = 0,
    general_profile_compatibility_flags: u32 = 0,
    general_progressive_source_flag: bool = false,
    general_interlaced_source_flag: bool = false,
    general_non_packed_constraint_flag: bool = false,
    general_frame_only_constraint_flag: bool = false,
    general_level_idc: u8 = 0,
};

/// clause 7.3.3. `max_sub_layers_minus1` comes from the enclosing VPS/SPS.
fn parseProfileTierLevel(bits: *Bits, max_sub_layers_minus1: u3) !ProfileTierLevel {
    const general = ProfileTierLevel{
        .general_profile_space = @intCast(try bits.read(2)),
        .general_tier_flag = try bits.flag(),
        .general_profile_idc = @intCast(try bits.read(5)),
        .general_profile_compatibility_flags = try bits.read(32),
        .general_progressive_source_flag = try bits.flag(),
        .general_interlaced_source_flag = try bits.flag(),
        .general_non_packed_constraint_flag = try bits.flag(),
        .general_frame_only_constraint_flag = try bits.flag(),
    };
    _ = try bits.read(32); // general_reserved_zero_43bits (first 32 of 44 reserved/constraint bits)
    _ = try bits.read(11); // remaining 11 of the 43 reserved bits
    _ = try bits.flag(); // general_inbld_flag / general_reserved_zero_bit
    const level_idc: u8 = @intCast(try bits.read(8));
    var sub_profile_present: [max_sub_layers]bool = @splat(false);
    var sub_level_present: [max_sub_layers]bool = @splat(false);
    for (0..max_sub_layers_minus1) |i| {
        sub_profile_present[i] = try bits.flag();
        sub_level_present[i] = try bits.flag();
    }
    if (max_sub_layers_minus1 > 0) {
        for (max_sub_layers_minus1..8) |_| _ = try bits.read(2);
    }
    for (0..max_sub_layers_minus1) |i| {
        if (sub_profile_present[i]) {
            _ = try bits.read(2);
            _ = try bits.read(1);
            _ = try bits.read(5);
            _ = try bits.read(32);
            _ = try bits.read(32);
            _ = try bits.read(11);
            _ = try bits.read(1);
        }
        if (sub_level_present[i]) _ = try bits.read(8);
    }
    var result = general;
    result.general_level_idc = level_idc;
    return result;
}

/// clause 7.3.7, derived per 7.4.8. `UsedByCurrPic{S0,S1}` are folded into the
/// delta array as the sign-preserving entries so `num_entries` stays a single
/// bound; S0 (negative, nearest first) precedes S1 (positive, nearest first)
/// in iteration order via `negative_count`.
pub const ShortTermRefPicSet = struct {
    negative_count: u8 = 0,
    positive_count: u8 = 0,
    delta_poc: [max_rps_entries]i32 = @splat(0),
    used_by_curr: [max_rps_entries]bool = @splat(false),

    pub fn numDeltaPocs(self: ShortTermRefPicSet) usize {
        return self.negative_count + self.positive_count;
    }
};

/// Parses st_ref_pic_set(stRpsIdx); `sets[0..st_rps_idx]` must already be
/// populated when inter-RPS prediction (clause 7.4.8) is used.
pub fn parseShortTermRefPicSet(bits: *Bits, sets: []const ShortTermRefPicSet, st_rps_idx: usize) !ShortTermRefPicSet {
    var inter_pred = false;
    if (st_rps_idx != 0) inter_pred = try bits.flag();
    if (inter_pred) {
        var delta_idx: usize = 1;
        if (st_rps_idx == sets.len) delta_idx = try bits.ue() + 1;
        if (delta_idx == 0 or delta_idx > st_rps_idx) return error.MalformedVideoConfig;
        const ref_idx = st_rps_idx - delta_idx;
        const sign = try bits.flag();
        const abs_delta = try bits.ue() + 1;
        if (abs_delta > max_delta_poc) return error.MalformedVideoPacket;
        const delta_rps: i32 = (if (sign) @as(i32, -1) else 1) * @as(i32, @intCast(abs_delta));
        const ref = sets[ref_idx];
        const num_delta = ref.numDeltaPocs();
        if (num_delta > max_rps_entries) return error.ResourceLimitExceeded;
        var use_delta: [max_rps_entries + 1]bool = @splat(true);
        var used: [max_rps_entries + 1]bool = @splat(false);
        for (0..num_delta + 1) |j| {
            used[j] = try bits.flag();
            use_delta[j] = if (!used[j]) try bits.flag() else true;
        }
        var result = ShortTermRefPicSet{};
        var i: usize = 0;
        var j: usize = ref.positive_count;
        while (j > 0) {
            j -= 1;
            const d = ref.delta_poc[ref.negative_count + j] + delta_rps;
            if (d < 0 and use_delta[ref.negative_count + j]) {
                if (i >= max_rps_entries) return error.ResourceLimitExceeded;
                result.delta_poc[i] = d;
                result.used_by_curr[i] = used[ref.negative_count + j];
                i += 1;
            }
        }
        if (delta_rps < 0 and use_delta[num_delta]) {
            if (i >= max_rps_entries) return error.ResourceLimitExceeded;
            result.delta_poc[i] = delta_rps;
            result.used_by_curr[i] = used[num_delta];
            i += 1;
        }
        for (0..ref.negative_count) |k| {
            const d = ref.delta_poc[k] + delta_rps;
            if (d < 0 and use_delta[k]) {
                if (i >= max_rps_entries) return error.ResourceLimitExceeded;
                result.delta_poc[i] = d;
                result.used_by_curr[i] = used[k];
                i += 1;
            }
        }
        result.negative_count = @intCast(i);
        var p: usize = ref.negative_count;
        while (p > 0) {
            p -= 1;
            const d = ref.delta_poc[p] + delta_rps;
            if (d > 0 and use_delta[p]) {
                if (i >= max_rps_entries) return error.ResourceLimitExceeded;
                result.delta_poc[i] = d;
                result.used_by_curr[i] = used[p];
                i += 1;
            }
        }
        if (delta_rps > 0 and use_delta[num_delta]) {
            if (i >= max_rps_entries) return error.ResourceLimitExceeded;
            result.delta_poc[i] = delta_rps;
            result.used_by_curr[i] = used[num_delta];
            i += 1;
        }
        for (0..ref.positive_count) |k| {
            const d = ref.delta_poc[ref.negative_count + k] + delta_rps;
            if (d > 0 and use_delta[ref.negative_count + k]) {
                if (i >= max_rps_entries) return error.ResourceLimitExceeded;
                result.delta_poc[i] = d;
                result.used_by_curr[i] = used[ref.negative_count + k];
                i += 1;
            }
        }
        result.positive_count = @intCast(i - result.negative_count);
        return result;
    }
    const num_negative = try bits.ue();
    const num_positive = try bits.ue();
    if (num_negative > max_rps_entries or num_positive > max_rps_entries - num_negative) return error.ResourceLimitExceeded;
    var result = ShortTermRefPicSet{ .negative_count = @intCast(num_negative), .positive_count = @intCast(num_positive) };
    var prev: i32 = 0;
    for (0..num_negative) |i| {
        const delta = try bits.ue() + 1;
        if (delta > max_delta_poc) return error.MalformedVideoPacket;
        prev -= @intCast(delta);
        result.delta_poc[i] = prev;
        result.used_by_curr[i] = try bits.flag();
    }
    prev = 0;
    for (0..num_positive) |i| {
        const delta = try bits.ue() + 1;
        if (delta > max_delta_poc) return error.MalformedVideoPacket;
        prev += @intCast(delta);
        result.delta_poc[num_negative + i] = prev;
        result.used_by_curr[num_negative + i] = try bits.flag();
    }
    return result;
}

/// clause 7.3.4. Explicit coefficients are retained in scan order without
/// resolving default/predicted-from matrices to final values: no scaled
/// reconstruction consumes them yet (see hevc.zig's documented scope limit).
pub const ScalingListData = struct {
    pub const Entry = union(enum) {
        default,
        predicted_from: u8,
        explicit: struct { dc_minus8: i16 = 0, coefficients: [64]u8 = @splat(0), count: u8 = 0 },
    };
    entries: [4][6]Entry = @splat(@splat(.default)),
    present: bool = false,
};

pub fn parseScalingListData(bits: *Bits) !ScalingListData {
    var data = ScalingListData{ .present = true };
    var size_id: usize = 0;
    while (size_id < 4) : (size_id += 1) {
        const step: usize = if (size_id == 3) 3 else 1;
        var matrix_id: usize = 0;
        while (matrix_id < 6) : (matrix_id += step) {
            const predicted = !try bits.flag(); // scaling_list_pred_mode_flag
            if (predicted) {
                const delta = try bits.ue();
                if (delta * step > matrix_id) return error.MalformedVideoConfig;
                data.entries[size_id][matrix_id] = if (delta == 0) .default else .{ .predicted_from = @intCast(matrix_id - delta * step) };
                continue;
            }
            var dc_minus8: i16 = 0;
            if (size_id > 1) dc_minus8 = @intCast(try bits.seBounded(-7, 247));
            const coef_num: usize = @min(64, @as(usize, 1) << @intCast(4 + size_id * 2));
            var coefficients: [64]u8 = @splat(0);
            var next_coef: i32 = 8;
            for (0..coef_num) |i| {
                const delta = try bits.seBounded(-512, 512);
                next_coef = @mod(next_coef + delta + 256, 256);
                coefficients[i] = @intCast(next_coef);
            }
            data.entries[size_id][matrix_id] = .{ .explicit = .{ .dc_minus8 = dc_minus8, .coefficients = coefficients, .count = @intCast(coef_num) } };
        }
    }
    return data;
}

pub const Vui = struct {
    sar_width: u16 = 0,
    sar_height: u16 = 0,
    video_full_range_flag: bool = false,
    colour_primaries: ?u8 = null,
    transfer_characteristics: ?u8 = null,
    matrix_coefficients: ?u8 = null,
    field_seq_flag: bool = false,
};

pub const Sps = struct {
    vps_id: u4,
    max_sub_layers_minus1: u3,
    sps_id: u5,
    chroma_format_idc: u2,
    separate_colour_plane_flag: bool,
    width: u32,
    height: u32,
    conf_win_left: u32,
    conf_win_right: u32,
    conf_win_top: u32,
    conf_win_bottom: u32,
    bit_depth_luma: u8,
    bit_depth_chroma: u8,
    log2_max_poc_lsb: u6,
    max_dec_pic_buffering: u8,
    max_num_reorder_pics: u8,
    log2_min_luma_cb: u5,
    log2_ctb_size: u5,
    log2_min_tb: u5,
    log2_max_tb: u5,
    max_transform_hierarchy_depth_inter: u5,
    max_transform_hierarchy_depth_intra: u5,
    scaling_list_enabled: bool,
    scaling_list: ScalingListData,
    amp_enabled: bool,
    sao_enabled: bool,
    pcm_enabled: bool,
    pcm_bit_depth_luma: u8,
    pcm_bit_depth_chroma: u8,
    log2_min_pcm_cb: u5,
    log2_max_pcm_cb: u5,
    pcm_loop_filter_disabled: bool,
    num_short_term_ref_pic_sets: u8,
    short_term_ref_pic_sets: [max_short_term_rps]ShortTermRefPicSet,
    long_term_ref_pics_present: bool,
    num_long_term_ref_pics: u8,
    lt_ref_pic_poc_lsb: [max_rps_entries]u32,
    used_by_curr_pic_lt: [max_rps_entries]bool,
    temporal_mvp_enabled: bool,
    strong_intra_smoothing_enabled: bool,
    vui: Vui,

    pub fn ctbSize(self: Sps) u32 {
        return @as(u32, 1) << self.log2_ctb_size;
    }
    pub fn picWidthInCtbs(self: Sps) u32 {
        return (self.width + self.ctbSize() - 1) / self.ctbSize();
    }
    pub fn picHeightInCtbs(self: Sps) u32 {
        return (self.height + self.ctbSize() - 1) / self.ctbSize();
    }
    pub fn subWidthC(self: Sps) u32 {
        return if (self.chroma_format_idc == 1 or self.chroma_format_idc == 2) 2 else 1;
    }
    pub fn subHeightC(self: Sps) u32 {
        return if (self.chroma_format_idc == 1) 2 else 1;
    }
    pub fn displayWidth(self: Sps) u32 {
        return self.width -| self.subWidthC() * (self.conf_win_left + self.conf_win_right);
    }
    pub fn displayHeight(self: Sps) u32 {
        return self.height -| self.subHeightC() * (self.conf_win_top + self.conf_win_bottom);
    }
};

/// clause 7.3.2.2. Rejects range-extension profiles/chroma formats this
/// decoder does not support (4:2:2, 4:4:4, separate colour planes), and any
/// SPS that signals field-sequence or interlaced-affecting VUI content.
pub fn parseSps(bits: *Bits) !Sps {
    const vps_id: u4 = @intCast(try bits.read(4));
    const max_sub_layers_minus1: u3 = @intCast(try bits.read(3));
    _ = try bits.flag(); // sps_temporal_id_nesting_flag
    _ = try parseProfileTierLevel(bits, max_sub_layers_minus1);
    const sps_id = try bits.ue();
    if (sps_id >= max_sps_count) return error.ResourceLimitExceeded;
    const chroma_format_idc = try bits.ue();
    var separate_colour_plane_flag = false;
    if (chroma_format_idc == 3) separate_colour_plane_flag = try bits.flag();
    if (chroma_format_idc != 1 or separate_colour_plane_flag) return error.UnsupportedVideoProfile;
    const width = try bits.ue();
    const height = try bits.ue();
    if (width == 0 or height == 0 or width > 16_384 or height > 16_384) return error.UnsupportedVideoProfile;
    var conf_win_left: u32 = 0;
    var conf_win_right: u32 = 0;
    var conf_win_top: u32 = 0;
    var conf_win_bottom: u32 = 0;
    if (try bits.flag()) {
        conf_win_left = try bits.ue();
        conf_win_right = try bits.ue();
        conf_win_top = try bits.ue();
        conf_win_bottom = try bits.ue();
    }
    const bit_depth_luma = try bits.ue() + 8;
    const bit_depth_chroma = try bits.ue() + 8;
    if ((bit_depth_luma != 8 and bit_depth_luma != 10) or bit_depth_luma != bit_depth_chroma) return error.UnsupportedVideoProfile;
    const log2_max_poc_lsb = try bits.ue() + 4;
    if (log2_max_poc_lsb > 16) return error.UnsupportedVideoProfile;
    const sub_layer_ordering_present = try bits.flag();
    var max_dec_pic_buffering: u8 = 0;
    var max_num_reorder_pics: u8 = 0;
    const first_layer: usize = if (sub_layer_ordering_present) 0 else max_sub_layers_minus1;
    for (first_layer..@as(usize, max_sub_layers_minus1) + 1) |_| {
        const buffering = try bits.ue() + 1;
        const reorder = try bits.ue();
        _ = try bits.ue(); // sps_max_latency_increase_plus1
        if (buffering > 16 or reorder > 16) return error.UnsupportedVideoProfile;
        max_dec_pic_buffering = @intCast(buffering);
        max_num_reorder_pics = @intCast(reorder);
    }
    const log2_min_luma_cb = try bits.ue() + 3;
    const log2_diff_cb = try bits.ue();
    const log2_min_tb = try bits.ue() + 2;
    const log2_diff_tb = try bits.ue();
    const max_hierarchy_inter = try bits.ue();
    const max_hierarchy_intra = try bits.ue();
    if (log2_min_luma_cb > 10 or log2_diff_cb > 10 or log2_min_tb > 10 or log2_diff_tb > 10 or max_hierarchy_inter > 4 or max_hierarchy_intra > 4) return error.UnsupportedVideoProfile;
    const log2_ctb_size = log2_min_luma_cb + log2_diff_cb;
    if (log2_ctb_size < 4 or log2_ctb_size > 6) return error.UnsupportedVideoProfile;
    const scaling_list_enabled = try bits.flag();
    var scaling_list = ScalingListData{};
    if (scaling_list_enabled) {
        if (try bits.flag()) scaling_list = try parseScalingListData(bits);
    }
    const amp_enabled = try bits.flag();
    const sao_enabled = try bits.flag();
    const pcm_enabled = try bits.flag();
    var pcm_bit_depth_luma: u8 = 0;
    var pcm_bit_depth_chroma: u8 = 0;
    var log2_min_pcm_cb: u5 = 0;
    var log2_max_pcm_cb: u5 = 0;
    var pcm_loop_filter_disabled = false;
    if (pcm_enabled) {
        pcm_bit_depth_luma = @intCast(try bits.read(4) + 1);
        pcm_bit_depth_chroma = @intCast(try bits.read(4) + 1);
        log2_min_pcm_cb = @intCast(try bits.ueBounded(31 - 3) + 3);
        log2_max_pcm_cb = @intCast(@as(u32, log2_min_pcm_cb) + try bits.ueBounded(31 - @as(u32, log2_min_pcm_cb)));
        pcm_loop_filter_disabled = try bits.flag();
    }
    const num_short_term_ref_pic_sets = try bits.ue();
    if (num_short_term_ref_pic_sets > max_short_term_rps) return error.ResourceLimitExceeded;
    var short_term_ref_pic_sets: [max_short_term_rps]ShortTermRefPicSet = @splat(.{});
    for (0..num_short_term_ref_pic_sets) |i| short_term_ref_pic_sets[i] = try parseShortTermRefPicSet(bits, short_term_ref_pic_sets[0..i], i);
    const long_term_ref_pics_present = try bits.flag();
    var num_long_term_ref_pics: u8 = 0;
    var lt_ref_pic_poc_lsb: [max_rps_entries]u32 = @splat(0);
    var used_by_curr_pic_lt: [max_rps_entries]bool = @splat(false);
    if (long_term_ref_pics_present) {
        const num = try bits.ue();
        if (num > max_rps_entries) return error.ResourceLimitExceeded;
        num_long_term_ref_pics = @intCast(num);
        for (0..num) |i| {
            lt_ref_pic_poc_lsb[i] = try bits.read(@intCast(log2_max_poc_lsb));
            used_by_curr_pic_lt[i] = try bits.flag();
        }
    }
    const temporal_mvp_enabled = try bits.flag();
    const strong_intra_smoothing_enabled = try bits.flag();
    var vui = Vui{};
    if (try bits.flag()) vui = try parseVui(bits, max_sub_layers_minus1);
    if (vui.field_seq_flag) return error.UnsupportedVideoProfile;
    // sps_extension_present_flag and range/multilayer/3d/scc extension flags:
    // reject any of them rather than silently ignoring tool bits we did not parse.
    if (try bits.flag()) {
        const range_ext = try bits.flag();
        const multilayer_ext = try bits.flag();
        const d3_ext = try bits.flag();
        const scc_ext = try bits.flag();
        _ = try bits.read(4); // sps_extension_4bits
        if (range_ext or multilayer_ext or d3_ext or scc_ext) return error.UnsupportedVideoProfile;
    }
    return .{
        .vps_id = vps_id,
        .max_sub_layers_minus1 = max_sub_layers_minus1,
        .sps_id = @intCast(sps_id),
        .chroma_format_idc = @intCast(chroma_format_idc),
        .separate_colour_plane_flag = separate_colour_plane_flag,
        .width = width,
        .height = height,
        .conf_win_left = conf_win_left,
        .conf_win_right = conf_win_right,
        .conf_win_top = conf_win_top,
        .conf_win_bottom = conf_win_bottom,
        .bit_depth_luma = @intCast(bit_depth_luma),
        .bit_depth_chroma = @intCast(bit_depth_chroma),
        .log2_max_poc_lsb = @intCast(log2_max_poc_lsb),
        .max_dec_pic_buffering = max_dec_pic_buffering,
        .max_num_reorder_pics = max_num_reorder_pics,
        .log2_min_luma_cb = @intCast(log2_min_luma_cb),
        .log2_ctb_size = @intCast(log2_ctb_size),
        .log2_min_tb = @intCast(log2_min_tb),
        .log2_max_tb = @intCast(log2_min_tb + log2_diff_tb),
        .max_transform_hierarchy_depth_inter = @intCast(max_hierarchy_inter),
        .max_transform_hierarchy_depth_intra = @intCast(max_hierarchy_intra),
        .scaling_list_enabled = scaling_list_enabled,
        .scaling_list = scaling_list,
        .amp_enabled = amp_enabled,
        .sao_enabled = sao_enabled,
        .pcm_enabled = pcm_enabled,
        .pcm_bit_depth_luma = pcm_bit_depth_luma,
        .pcm_bit_depth_chroma = pcm_bit_depth_chroma,
        .log2_min_pcm_cb = log2_min_pcm_cb,
        .log2_max_pcm_cb = log2_max_pcm_cb,
        .pcm_loop_filter_disabled = pcm_loop_filter_disabled,
        .num_short_term_ref_pic_sets = @intCast(num_short_term_ref_pic_sets),
        .short_term_ref_pic_sets = short_term_ref_pic_sets,
        .long_term_ref_pics_present = long_term_ref_pics_present,
        .num_long_term_ref_pics = num_long_term_ref_pics,
        .lt_ref_pic_poc_lsb = lt_ref_pic_poc_lsb,
        .used_by_curr_pic_lt = used_by_curr_pic_lt,
        .temporal_mvp_enabled = temporal_mvp_enabled,
        .strong_intra_smoothing_enabled = strong_intra_smoothing_enabled,
        .vui = vui,
    };
}

/// Table E-1, for aspect_ratio_idc 1 through 16.
const sar_table = [_][2]u16{ .{ 1, 1 }, .{ 12, 11 }, .{ 10, 11 }, .{ 16, 11 }, .{ 40, 33 }, .{ 24, 11 }, .{ 20, 11 }, .{ 32, 11 }, .{ 80, 33 }, .{ 18, 11 }, .{ 15, 11 }, .{ 64, 33 }, .{ 160, 99 }, .{ 4, 3 }, .{ 3, 2 }, .{ 2, 1 } };

fn parseVui(bits: *Bits, max_sub_layers_minus1: u3) !Vui {
    var vui = Vui{};
    if (try bits.flag()) { // aspect_ratio_info_present_flag
        const idc = try bits.read(8);
        if (idc == 255) {
            vui.sar_width = @intCast(try bits.read(16));
            vui.sar_height = @intCast(try bits.read(16));
        } else if (idc >= 1 and idc < sar_table.len + 1) {
            vui.sar_width = sar_table[idc - 1][0];
            vui.sar_height = sar_table[idc - 1][1];
        }
    }
    if (try bits.flag()) _ = try bits.flag(); // overscan_info_present_flag -> overscan_appropriate_flag
    if (try bits.flag()) { // video_signal_type_present_flag
        _ = try bits.read(3); // video_format
        vui.video_full_range_flag = try bits.flag();
        if (try bits.flag()) { // colour_description_present_flag
            vui.colour_primaries = @intCast(try bits.read(8));
            vui.transfer_characteristics = @intCast(try bits.read(8));
            vui.matrix_coefficients = @intCast(try bits.read(8));
        }
    }
    if (try bits.flag()) { // chroma_loc_info_present_flag
        _ = try bits.ue();
        _ = try bits.ue();
    }
    _ = try bits.flag(); // neutral_chroma_indication_flag
    vui.field_seq_flag = try bits.flag();
    _ = try bits.flag(); // frame_field_info_present_flag
    if (try bits.flag()) { // default_display_window_flag
        _ = try bits.ue();
        _ = try bits.ue();
        _ = try bits.ue();
        _ = try bits.ue();
    }
    if (try bits.flag()) { // vui_timing_info_present_flag
        _ = try bits.read(32);
        _ = try bits.read(32);
        if (try bits.flag()) _ = try bits.ue(); // vui_poc_proportional_to_timing_flag -> vui_num_ticks_poc_diff_one_minus1
        if (try bits.flag()) try skipHrdParametersFull(bits, true, max_sub_layers_minus1);
    }
    if (try bits.flag()) { // bitstream_restriction_flag
        _ = try bits.flag();
        _ = try bits.flag();
        _ = try bits.flag();
        _ = try bits.ue();
        _ = try bits.ue();
        _ = try bits.ue();
        _ = try bits.ue();
        _ = try bits.ue();
    }
    return vui;
}

/// clause E.2.2, consumed only to stay byte/bit-aligned for later syntax; no
/// HRD field is otherwise used by this decoder.
fn skipHrdParametersFull(bits: *Bits, common_inf_present: bool, max_sub_layers_minus1: u3) !void {
    var nal_hrd = false;
    var vcl_hrd = false;
    var sub_pic_hrd = false;
    if (common_inf_present) {
        nal_hrd = try bits.flag();
        vcl_hrd = try bits.flag();
        if (nal_hrd or vcl_hrd) {
            sub_pic_hrd = try bits.flag();
            if (sub_pic_hrd) {
                _ = try bits.read(8);
                _ = try bits.read(5);
                _ = try bits.flag();
                _ = try bits.read(5);
            }
            _ = try bits.read(4);
            _ = try bits.read(4);
            if (sub_pic_hrd) _ = try bits.read(4);
            _ = try bits.read(5);
            _ = try bits.read(5);
            _ = try bits.read(5);
        }
    }
    for (0..@as(usize, max_sub_layers_minus1) + 1) |_| {
        const fixed_pic_rate_general = try bits.flag();
        var fixed_pic_rate_within_cvs = fixed_pic_rate_general;
        if (!fixed_pic_rate_general) fixed_pic_rate_within_cvs = try bits.flag();
        var low_delay = false;
        if (fixed_pic_rate_within_cvs) _ = try bits.ue() else low_delay = try bits.flag();
        var cpb_cnt: usize = 1;
        if (!low_delay) cpb_cnt = try bits.ue() + 1;
        if (cpb_cnt > 32) return error.UnsupportedVideoProfile;
        if (nal_hrd) for (0..cpb_cnt) |_| {
            _ = try bits.ue();
            _ = try bits.ue();
            if (sub_pic_hrd) {
                _ = try bits.ue();
                _ = try bits.ue();
            }
            _ = try bits.flag();
        };
        if (vcl_hrd) for (0..cpb_cnt) |_| {
            _ = try bits.ue();
            _ = try bits.ue();
            if (sub_pic_hrd) {
                _ = try bits.ue();
                _ = try bits.ue();
            }
            _ = try bits.flag();
        };
    }
}

pub const TileGrid = struct {
    columns: u16 = 1,
    rows: u16 = 1,
    uniform_spacing: bool = true,
    column_width: [32]u32 = @splat(0),
    row_height: [32]u32 = @splat(0),
    loop_filter_across_tiles: bool = true,
};

pub const Pps = struct {
    pps_id: u6,
    sps_id: u5,
    dependent_slice_segments_enabled: bool,
    output_flag_present: bool,
    num_extra_slice_header_bits: u3,
    sign_data_hiding_enabled: bool,
    cabac_init_present: bool,
    num_ref_idx_l0_default_active: u8,
    num_ref_idx_l1_default_active: u8,
    init_qp: i32,
    constrained_intra_pred: bool,
    transform_skip_enabled: bool,
    cu_qp_delta_enabled: bool,
    diff_cu_qp_delta_depth: u5,
    cb_qp_offset: i8,
    cr_qp_offset: i8,
    slice_chroma_qp_offsets_present: bool,
    weighted_pred: bool,
    weighted_bipred: bool,
    transquant_bypass_enabled: bool,
    tiles_enabled: bool,
    entropy_coding_sync_enabled: bool,
    tiles: TileGrid,
    loop_filter_across_slices_enabled: bool,
    deblocking_filter_control_present: bool,
    deblocking_filter_override_enabled: bool,
    pps_deblocking_filter_disabled: bool,
    beta_offset_div2: i8,
    tc_offset_div2: i8,
    scaling_list_data_present: bool,
    scaling_list: ScalingListData,
    lists_modification_present: bool,
    log2_parallel_merge_level: u5,
    slice_segment_header_extension_present: bool,
};

/// clause 7.3.2.3.
pub fn parsePps(bits: *Bits) !Pps {
    const pps_id = try bits.ue();
    const sps_id = try bits.ue();
    if (pps_id >= max_pps_count or sps_id >= max_sps_count) return error.ResourceLimitExceeded;
    const dependent_slice_segments_enabled = try bits.flag();
    const output_flag_present = try bits.flag();
    const num_extra_slice_header_bits: u3 = @intCast(try bits.read(3));
    const sign_data_hiding_enabled = try bits.flag();
    const cabac_init_present = try bits.flag();
    const num_ref_idx_l0 = try bits.ue() + 1;
    const num_ref_idx_l1 = try bits.ue() + 1;
    if (num_ref_idx_l0 > 16 or num_ref_idx_l1 > 16) return error.UnsupportedVideoProfile;
    const init_qp = try bits.se() + 26;
    const constrained_intra_pred = try bits.flag();
    const transform_skip_enabled = try bits.flag();
    const cu_qp_delta_enabled = try bits.flag();
    var diff_cu_qp_delta_depth: u5 = 0;
    if (cu_qp_delta_enabled) diff_cu_qp_delta_depth = @intCast(try bits.ueBounded(31));
    const cb_qp_offset = try bits.se();
    const cr_qp_offset = try bits.se();
    if (cb_qp_offset < -12 or cb_qp_offset > 12 or cr_qp_offset < -12 or cr_qp_offset > 12) return error.UnsupportedVideoProfile;
    const slice_chroma_qp_offsets_present = try bits.flag();
    const weighted_pred = try bits.flag();
    const weighted_bipred = try bits.flag();
    const transquant_bypass_enabled = try bits.flag();
    const tiles_enabled = try bits.flag();
    const entropy_coding_sync_enabled = try bits.flag();
    var tiles = TileGrid{};
    if (tiles_enabled) {
        tiles.columns = @intCast(try bits.ueBounded(31) + 1);
        tiles.rows = @intCast(try bits.ueBounded(31) + 1);
        if (tiles.columns > 32 or tiles.rows > 32) return error.ResourceLimitExceeded;
        tiles.uniform_spacing = try bits.flag();
        if (!tiles.uniform_spacing) {
            for (0..tiles.columns - 1) |i| tiles.column_width[i] = try bits.ue() + 1;
            for (0..tiles.rows - 1) |i| tiles.row_height[i] = try bits.ue() + 1;
        }
        tiles.loop_filter_across_tiles = try bits.flag();
    }
    const loop_filter_across_slices_enabled = try bits.flag();
    const deblocking_filter_control_present = try bits.flag();
    var deblocking_filter_override_enabled = false;
    var pps_deblocking_filter_disabled = false;
    var beta_offset_div2: i8 = 0;
    var tc_offset_div2: i8 = 0;
    if (deblocking_filter_control_present) {
        deblocking_filter_override_enabled = try bits.flag();
        pps_deblocking_filter_disabled = try bits.flag();
        if (!pps_deblocking_filter_disabled) {
            beta_offset_div2 = @intCast(try bits.seBounded(-12, 12));
            tc_offset_div2 = @intCast(try bits.seBounded(-12, 12));
        }
    }
    const scaling_list_data_present = try bits.flag();
    var scaling_list = ScalingListData{};
    if (scaling_list_data_present) scaling_list = try parseScalingListData(bits);
    const lists_modification_present = try bits.flag();
    const log2_parallel_merge_level = try bits.ue() + 2;
    if (log2_parallel_merge_level > 10) return error.UnsupportedVideoProfile;
    const slice_segment_header_extension_present = try bits.flag();
    if (try bits.flag()) { // pps_extension_present_flag
        const range_ext = try bits.flag();
        const multilayer_ext = try bits.flag();
        const d3_ext = try bits.flag();
        const scc_ext = try bits.flag();
        _ = try bits.read(4);
        if (range_ext or multilayer_ext or d3_ext or scc_ext) return error.UnsupportedVideoProfile;
    }
    return .{
        .pps_id = @intCast(pps_id),
        .sps_id = @intCast(sps_id),
        .dependent_slice_segments_enabled = dependent_slice_segments_enabled,
        .output_flag_present = output_flag_present,
        .num_extra_slice_header_bits = num_extra_slice_header_bits,
        .sign_data_hiding_enabled = sign_data_hiding_enabled,
        .cabac_init_present = cabac_init_present,
        .num_ref_idx_l0_default_active = @intCast(num_ref_idx_l0),
        .num_ref_idx_l1_default_active = @intCast(num_ref_idx_l1),
        .init_qp = init_qp,
        .constrained_intra_pred = constrained_intra_pred,
        .transform_skip_enabled = transform_skip_enabled,
        .cu_qp_delta_enabled = cu_qp_delta_enabled,
        .diff_cu_qp_delta_depth = diff_cu_qp_delta_depth,
        .cb_qp_offset = @intCast(cb_qp_offset),
        .cr_qp_offset = @intCast(cr_qp_offset),
        .slice_chroma_qp_offsets_present = slice_chroma_qp_offsets_present,
        .weighted_pred = weighted_pred,
        .weighted_bipred = weighted_bipred,
        .transquant_bypass_enabled = transquant_bypass_enabled,
        .tiles_enabled = tiles_enabled,
        .entropy_coding_sync_enabled = entropy_coding_sync_enabled,
        .tiles = tiles,
        .loop_filter_across_slices_enabled = loop_filter_across_slices_enabled,
        .deblocking_filter_control_present = deblocking_filter_control_present,
        .deblocking_filter_override_enabled = deblocking_filter_override_enabled,
        .pps_deblocking_filter_disabled = pps_deblocking_filter_disabled,
        .beta_offset_div2 = beta_offset_div2,
        .tc_offset_div2 = tc_offset_div2,
        .scaling_list_data_present = scaling_list_data_present,
        .scaling_list = scaling_list,
        .lists_modification_present = lists_modification_present,
        .log2_parallel_merge_level = @intCast(log2_parallel_merge_level),
        .slice_segment_header_extension_present = slice_segment_header_extension_present,
    };
}

test "HEVC SPS parses the shipped Main-profile fixture bitstream" {
    // RBSP extracted from zig/pkg/inference testdata hevc-hvc1.mp4's hvcC SPS
    // NAL (header stripped), cross-checked against FFmpeg trace_headers output.
    const rbsp = [_]u8{
        0x01, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0x90, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x1e,
        0xa0, 0x18, 0x20, 0x30, 0x59, 0x65, 0x64, 0x4a, 0x4c, 0x2e, 0x6a, 0x02, 0x02, 0x02, 0x08, 0x00,
        0x00, 0x03, 0x00, 0x08, 0x00, 0x00, 0x03, 0x00, 0x20, 0x40,
    };
    var bits = try Bits.init(std.testing.allocator, &rbsp);
    defer bits.deinit();
    const sps = try parseSps(&bits);
    try std.testing.expectEqual(@as(u32, 192), sps.width);
    try std.testing.expectEqual(@as(u32, 192), sps.height);
    try std.testing.expectEqual(@as(u2, 1), sps.chroma_format_idc);
    try std.testing.expectEqual(@as(u8, 8), sps.bit_depth_luma);
    try std.testing.expectEqual(@as(u8, 8), sps.bit_depth_chroma);
    try std.testing.expectEqual(@as(u6, 8), sps.log2_max_poc_lsb);
    try std.testing.expectEqual(@as(u5, 5), sps.log2_ctb_size);
    try std.testing.expect(!sps.sao_enabled);
    try std.testing.expect(sps.temporal_mvp_enabled);
    try std.testing.expect(sps.vui.colour_primaries != null);
    try std.testing.expectEqual(@as(u8, 1), sps.vui.colour_primaries.?);
}
