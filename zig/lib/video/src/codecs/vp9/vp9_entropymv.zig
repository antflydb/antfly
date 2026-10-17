// Copyright (c) 2010, The WebM Project authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//
// Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in
//     the documentation and/or other materials provided with the
//     distribution.
//
// Neither the name of Google, nor the WebM Project, nor the names
//     of its contributors may be used to endorse or promote products
//     derived from this software without specific prior written
//     permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
// SPDX-License-Identifier: BSD-3-Clause
// /*
//  *  Copyright (c) 2010 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE = common.TX_MODE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const INTERP_FILTER = common.INTERP_FILTER;
pub const ENTROPY_CONTEXT = common.ENTROPY_CONTEXT;
pub const vp9_coeff_count_model = common.vp9_coeff_count_model;
pub const struct_tx_counts = common.struct_tx_counts;
pub const struct_FRAME_COUNTS = extern struct {
    y_mode: [4][10]c_uint = @import("std").mem.zeroes([4][10]c_uint),
    uv_mode: [10][10]c_uint = @import("std").mem.zeroes([10][10]c_uint),
    partition: [16][4]c_uint = @import("std").mem.zeroes([16][4]c_uint),
    coef: [4][2]vp9_coeff_count_model = @import("std").mem.zeroes([4][2]vp9_coeff_count_model),
    eob_branch: [4][2][2][6][6]c_uint = @import("std").mem.zeroes([4][2][2][6][6]c_uint),
    switchable_interp: [4][3]c_uint = @import("std").mem.zeroes([4][3]c_uint),
    inter_mode: [7][4]c_uint = @import("std").mem.zeroes([7][4]c_uint),
    intra_inter: [4][2]c_uint = @import("std").mem.zeroes([4][2]c_uint),
    comp_inter: [5][2]c_uint = @import("std").mem.zeroes([5][2]c_uint),
    single_ref: [5][2][2]c_uint = @import("std").mem.zeroes([5][2][2]c_uint),
    comp_ref: [5][2]c_uint = @import("std").mem.zeroes([5][2]c_uint),
    tx: struct_tx_counts = @import("std").mem.zeroes(struct_tx_counts),
    skip: [3][2]c_uint = @import("std").mem.zeroes([3][2]c_uint),
    mv: nmv_context_counts = @import("std").mem.zeroes(nmv_context_counts),
};
pub const FRAME_COUNTS = struct_FRAME_COUNTS;
pub const MV_REFERENCE_FRAME = common.MV_REFERENCE_FRAME;
pub const struct_mv = extern struct {
    row: i16 = 0,
    col: i16 = 0,
};
pub const MV = struct_mv;
pub const union_int_mv = extern union {
    as_int: u32,
    as_mv: MV,
};
pub const int_mv = union_int_mv;
pub const struct_MODE_INFO = extern struct {
    sb_type: BLOCK_SIZE = 0,
    mode: PREDICTION_MODE = 0,
    tx_size: TX_SIZE = 0,
    skip: i8 = 0,
    segment_id: i8 = 0,
    seg_id_predicted: i8 = 0,
    uv_mode: PREDICTION_MODE = 0,
    interp_filter: INTERP_FILTER = 0,
    ref_frame: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
    mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
    bmi: [4]b_mode_info = @import("std").mem.zeroes([4]b_mode_info),
};
pub const MODE_INFO = struct_MODE_INFO;
pub const vpx_prob = common.vpx_prob;
pub const vp9_coeff_probs_model = common.vp9_coeff_probs_model;
pub const struct_tx_probs = extern struct {
    p32x32: [2][3]vpx_prob = @import("std").mem.zeroes([2][3]vpx_prob),
    p16x16: [2][2]vpx_prob = @import("std").mem.zeroes([2][2]vpx_prob),
    p8x8: [2][1]vpx_prob = @import("std").mem.zeroes([2][1]vpx_prob),
};
pub const struct_frame_contexts = extern struct {
    y_mode_prob: [4][9]vpx_prob = @import("std").mem.zeroes([4][9]vpx_prob),
    uv_mode_prob: [10][9]vpx_prob = @import("std").mem.zeroes([10][9]vpx_prob),
    partition_prob: [16][3]vpx_prob = @import("std").mem.zeroes([16][3]vpx_prob),
    coef_probs: [4][2]vp9_coeff_probs_model = @import("std").mem.zeroes([4][2]vp9_coeff_probs_model),
    switchable_interp_prob: [4][2]vpx_prob = @import("std").mem.zeroes([4][2]vpx_prob),
    inter_mode_probs: [7][3]vpx_prob = @import("std").mem.zeroes([7][3]vpx_prob),
    intra_inter_prob: [4]vpx_prob = @import("std").mem.zeroes([4]vpx_prob),
    comp_inter_prob: [5]vpx_prob = @import("std").mem.zeroes([5]vpx_prob),
    single_ref_prob: [5][2]vpx_prob = @import("std").mem.zeroes([5][2]vpx_prob),
    comp_ref_prob: [5]vpx_prob = @import("std").mem.zeroes([5]vpx_prob),
    tx_probs: struct_tx_probs = @import("std").mem.zeroes(struct_tx_probs),
    skip_probs: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    nmvc: nmv_context = @import("std").mem.zeroes(nmv_context),
    initialized: c_int = 0,
};
pub const FRAME_CONTEXT = struct_frame_contexts;
pub const struct_yv12_buffer_config = extern struct {
    y_width: c_int = 0,
    y_height: c_int = 0,
    y_crop_width: c_int = 0,
    y_crop_height: c_int = 0,
    y_stride: c_int = 0,
    uv_width: c_int = 0,
    uv_height: c_int = 0,
    uv_crop_width: c_int = 0,
    uv_crop_height: c_int = 0,
    uv_stride: c_int = 0,
    alpha_width: c_int = 0,
    alpha_height: c_int = 0,
    alpha_stride: c_int = 0,
    y_buffer: [*c]u8 = null,
    u_buffer: [*c]u8 = null,
    v_buffer: [*c]u8 = null,
    alpha_buffer: [*c]u8 = null,
    buffer_alloc: [*c]u8 = null,
    buffer_alloc_sz: usize = 0,
    border: c_int = 0,
    frame_size: usize = 0,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    bit_depth: c_uint = 0,
    color_space: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    color_range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    render_width: c_int = 0,
    render_height: c_int = 0,
    corrupted: c_int = 0,
    flags: c_int = 0,
};
pub const YV12_BUFFER_CONFIG = struct_yv12_buffer_config;
pub const struct_scale_factors = common.struct_scale_factors;
pub const struct_RefBuffer = extern struct {
    idx: c_int = 0,
    buf: [*c]YV12_BUFFER_CONFIG = null,
    sf: struct_scale_factors = @import("std").mem.zeroes(struct_scale_factors),
};
pub const RefBuffer = struct_RefBuffer;
pub const struct_loopfilter = extern struct {
    filter_level: c_int = 0,
    last_filt_level: c_int = 0,
    sharpness_level: c_int = 0,
    last_sharpness_level: c_int = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    ref_deltas: [4]i8 = @import("std").mem.zeroes([4]i8),
    last_ref_deltas: [4]i8 = @import("std").mem.zeroes([4]i8),
    mode_deltas: [2]i8 = @import("std").mem.zeroes([2]i8),
    last_mode_deltas: [2]i8 = @import("std").mem.zeroes([2]i8),
    lfm: [*c]LOOP_FILTER_MASK = null,
    lfm_stride: c_int = 0,
};
pub const struct_segmentation = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    update_data: u8 = 0,
    abs_delta: u8 = 0,
    temporal_update: u8 = 0,
    tree_probs: [7]vpx_prob = @import("std").mem.zeroes([7]vpx_prob),
    pred_probs: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    feature_data: [8][4]i16 = @import("std").mem.zeroes([8][4]i16),
    feature_mask: [8]u32 = @import("std").mem.zeroes([8]u32),
    aq_av_offset: c_int = 0,
};
pub const InternalFrameBufferList = common.InternalFrameBufferList;
pub const struct_BufferPool = extern struct {
    cb_priv: ?*anyopaque = null,
    get_fb_cb: vpx_get_frame_buffer_cb_fn_t = null,
    release_fb_cb: vpx_release_frame_buffer_cb_fn_t = null,
    frame_bufs: [12]RefCntBuffer = @import("std").mem.zeroes([12]RefCntBuffer),
    int_frame_buffers: InternalFrameBufferList = @import("std").mem.zeroes(InternalFrameBufferList),
};
pub const BufferPool = struct_BufferPool;
pub const struct_VP9Common = extern struct {
    @"error": struct_vpx_internal_error_info = @import("std").mem.zeroes(struct_vpx_internal_error_info),
    color_space: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    color_range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    width: c_int = 0,
    height: c_int = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    last_width: c_int = 0,
    last_height: c_int = 0,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    use_highbitdepth: c_int = 0,
    frame_to_show: [*c]YV12_BUFFER_CONFIG = null,
    prev_frame: [*c]RefCntBuffer = null,
    cur_frame: [*c]RefCntBuffer = null,
    ref_frame_map: [8]c_int = @import("std").mem.zeroes([8]c_int),
    next_ref_frame_map: [8]c_int = @import("std").mem.zeroes([8]c_int),
    frame_refs: [3]RefBuffer = @import("std").mem.zeroes([3]RefBuffer),
    new_fb_idx: c_int = 0,
    cur_show_frame_fb_idx: c_int = 0,
    last_frame_type: FRAME_TYPE = @import("std").mem.zeroes(FRAME_TYPE),
    frame_type: FRAME_TYPE = @import("std").mem.zeroes(FRAME_TYPE),
    show_frame: c_int = 0,
    last_show_frame: c_int = 0,
    show_existing_frame: c_int = 0,
    intra_only: u8 = 0,
    last_intra_only: u8 = 0,
    allow_high_precision_mv: c_int = 0,
    reset_frame_context: c_int = 0,
    MBs: c_int = 0,
    mb_rows: c_int = 0,
    mi_rows: c_int = 0,
    mb_cols: c_int = 0,
    mi_cols: c_int = 0,
    mi_stride: c_int = 0,
    tx_mode: TX_MODE = @import("std").mem.zeroes(TX_MODE),
    base_qindex: c_int = 0,
    y_dc_delta_q: c_int = 0,
    uv_dc_delta_q: c_int = 0,
    uv_ac_delta_q: c_int = 0,
    y_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    uv_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    mi_alloc_size: c_int = 0,
    mip: [*c]MODE_INFO = null,
    mi: [*c]MODE_INFO = null,
    prev_mip: [*c]MODE_INFO = null,
    prev_mi: [*c]MODE_INFO = null,
    alloc_mi: ?*const fn (cm: [*c]struct_VP9Common, mi_size: c_int) callconv(.c) c_int = null,
    free_mi: ?*const fn (cm: [*c]struct_VP9Common) callconv(.c) void = null,
    setup_mi: ?*const fn (cm: [*c]struct_VP9Common) callconv(.c) void = null,
    mi_grid_base: [*c][*c]MODE_INFO = null,
    mi_grid_visible: [*c][*c]MODE_INFO = null,
    prev_mi_grid_base: [*c][*c]MODE_INFO = null,
    prev_mi_grid_visible: [*c][*c]MODE_INFO = null,
    use_prev_frame_mvs: c_int = 0,
    seg_map_idx: c_int = 0,
    prev_seg_map_idx: c_int = 0,
    seg_map_array: [2][*c]u8 = @import("std").mem.zeroes([2][*c]u8),
    last_frame_seg_map: [*c]u8 = null,
    current_frame_seg_map: [*c]u8 = null,
    seg_map_alloc_size: c_int = 0,
    interp_filter: INTERP_FILTER = 0,
    lf_info: loop_filter_info_n = @import("std").mem.zeroes(loop_filter_info_n),
    refresh_frame_context: c_int = 0,
    ref_frame_sign_bias: [4]c_int = @import("std").mem.zeroes([4]c_int),
    lf: struct_loopfilter = @import("std").mem.zeroes(struct_loopfilter),
    seg: struct_segmentation = @import("std").mem.zeroes(struct_segmentation),
    comp_fixed_ref: MV_REFERENCE_FRAME = 0,
    comp_var_ref: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
    reference_mode: REFERENCE_MODE = @import("std").mem.zeroes(REFERENCE_MODE),
    fc: [*c]FRAME_CONTEXT = null,
    frame_contexts: [*c]FRAME_CONTEXT = null,
    frame_context_idx: c_uint = 0,
    counts: FRAME_COUNTS = @import("std").mem.zeroes(FRAME_COUNTS),
    current_video_frame: c_uint = 0,
    current_frame_coding_index: c_int = 0,
    profile: BITSTREAM_PROFILE = @import("std").mem.zeroes(BITSTREAM_PROFILE),
    bit_depth: vpx_bit_depth_t = @import("std").mem.zeroes(vpx_bit_depth_t),
    dequant_bit_depth: vpx_bit_depth_t = @import("std").mem.zeroes(vpx_bit_depth_t),
    error_resilient_mode: c_int = 0,
    frame_parallel_decoding_mode: c_int = 0,
    log2_tile_cols: c_int = 0,
    log2_tile_rows: c_int = 0,
    byte_alignment: c_int = 0,
    skip_loop_filter: c_int = 0,
    buffer_pool: [*c]BufferPool = null,
    above_seg_context: [*c]PARTITION_CONTEXT = null,
    above_context: [*c]ENTROPY_CONTEXT = null,
    above_context_alloc_cols: c_int = 0,
    lf_row: c_int = 0,
};
pub const vpx_tree_index = common.vpx_tree_index;
pub fn get_prob(arg_num: c_uint, arg_den: c_uint) callconv(.c) vpx_prob {
    var num = arg_num;
    _ = &num;
    var den = arg_den;
    _ = &den;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const p: c_int = @bitCast(@as(c_uint, @truncate(((@as(u64, num) *% @as(u64, 256)) +% @as(u64, den >> @intCast(@as(c_uint, 1)))) / @as(u64, den))));
        _ = &p;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const clipped_prob: c_int = (p | ((@as(c_int, 255) - p) >> @intCast(@as(c_int, 23)))) | @as(c_int, @intFromBool(p == @as(c_int, 0)));
        _ = &clipped_prob;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(@as(i8, @truncate(clipped_prob)));
    }
}
pub fn weighted_prob(arg_prob1: c_int, arg_prob2: c_int, arg_factor: c_int) callconv(.c) vpx_prob {
    var prob1 = arg_prob1;
    _ = &prob1;
    var prob2 = arg_prob2;
    _ = &prob2;
    var factor = arg_factor;
    _ = &factor;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(i8, @truncate((((prob1 * (@as(c_int, 256) - factor)) + (prob2 * factor)) + (@as(c_int, 1) << @intCast(@as(c_int, 8) - @as(c_int, 1)))) >> @intCast(@as(c_int, 8)))));
}
pub const count_to_update_factor = common.count_to_update_factor;
pub fn mode_mv_merge_probs(arg_pre_prob: vpx_prob, arg_ct: [*c]const c_uint) callconv(.c) vpx_prob {
    var pre_prob = arg_pre_prob;
    _ = &pre_prob;
    var ct = arg_ct;
    _ = &ct;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const den: c_uint = ct[@as(c_int, 0)] +% ct[@as(c_int, 1)];
    _ = &den;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (den == @as(c_uint, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return pre_prob;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const count: c_uint = if (den < @as(c_uint, @bitCast(@as(c_int, @as(c_int, 20))))) den else @as(c_uint, @bitCast(@as(c_int, @as(c_int, 20))));
        _ = &count;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const factor: c_uint = @bitCast(@as(c_int, count_to_update_factor[count]));
        _ = &factor;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const prob: vpx_prob = get_prob(ct[@as(c_int, 0)], den);
        _ = &prob;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return weighted_prob(pre_prob, prob, @bitCast(@as(c_uint, @truncate(factor))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub extern fn antfly_vp9_vpx_tree_merge_probs(tree: [*c]const vpx_tree_index, pre_probs: [*c]const vpx_prob, counts: [*c]const c_uint, probs: [*c]vpx_prob) void;
pub const MV_JOINT_ZERO = common.MV_JOINT_ZERO;
pub const MV_JOINT_HNZVZ = common.MV_JOINT_HNZVZ;
pub const MV_JOINT_HZVNZ = common.MV_JOINT_HZVNZ;
pub const MV_JOINT_HNZVNZ = common.MV_JOINT_HNZVNZ;
pub const MV_JOINT_TYPE = common.MV_JOINT_TYPE;
pub fn mv_joint_vertical(arg_type: MV_JOINT_TYPE) callconv(.c) c_int {
    var @"type" = arg_type;
    _ = &@"type";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((@"type" == @as(MV_JOINT_TYPE, MV_JOINT_HZVNZ)) or (@"type" == @as(MV_JOINT_TYPE, MV_JOINT_HNZVNZ))));
}
pub fn mv_joint_horizontal(arg_type: MV_JOINT_TYPE) callconv(.c) c_int {
    var @"type" = arg_type;
    _ = &@"type";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((@"type" == @as(MV_JOINT_TYPE, MV_JOINT_HNZVZ)) or (@"type" == @as(MV_JOINT_TYPE, MV_JOINT_HNZVNZ))));
}
pub const MV_CLASS_0 = common.MV_CLASS_0;
pub const MV_CLASS_1 = common.MV_CLASS_1;
pub const MV_CLASS_2 = common.MV_CLASS_2;
pub const MV_CLASS_3 = common.MV_CLASS_3;
pub const MV_CLASS_4 = common.MV_CLASS_4;
pub const MV_CLASS_5 = common.MV_CLASS_5;
pub const MV_CLASS_6 = common.MV_CLASS_6;
pub const MV_CLASS_7 = common.MV_CLASS_7;
pub const MV_CLASS_8 = common.MV_CLASS_8;
pub const MV_CLASS_9 = common.MV_CLASS_9;
pub const MV_CLASS_10 = common.MV_CLASS_10;
pub const MV_CLASS_TYPE = common.MV_CLASS_TYPE;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub fn vp9_get_mv_joint(arg_mv_1: [*c]const MV) callconv(.c) MV_JOINT_TYPE {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(c_int, mv_1.*.row) == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(if (@as(c_int, mv_1.*.col) == @as(c_int, 0)) MV_JOINT_ZERO else MV_JOINT_HNZVZ);
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(if (@as(c_int, mv_1.*.col) == @as(c_int, 0)) MV_JOINT_HZVNZ else MV_JOINT_HNZVNZ);
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub const nmv_component_counts = common.nmv_component_counts;
pub const nmv_context_counts = common.nmv_context_counts;
pub const FRAME_TYPE = common.FRAME_TYPE;
pub const b_mode_info = extern struct {
    as_mode: PREDICTION_MODE = 0,
    as_mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
};
pub const loop_filter_thresh = common.loop_filter_thresh;
pub const loop_filter_info_n = extern struct {
    lfthr: [64]loop_filter_thresh = @import("std").mem.zeroes([64]loop_filter_thresh),
    lvl: [8][4][2]u8 = @import("std").mem.zeroes([8][4][2]u8),
};
pub const LOOP_FILTER_MASK = common.LOOP_FILTER_MASK;
pub const REFERENCE_MODE = common.REFERENCE_MODE;
pub const MV_REF = extern struct {
    mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
    ref_frame: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
};
pub const RefCntBuffer = extern struct {
    ref_count: c_int = 0,
    mvs: [*c]MV_REF = null,
    mi_rows: c_int = 0,
    mi_cols: c_int = 0,
    released: u8 = 0,
    frame_index: c_int = 0,
    frame_coding_index: c_int = 0,
    raw_frame_buffer: vpx_codec_frame_buffer_t = @import("std").mem.zeroes(vpx_codec_frame_buffer_t),
    buf: YV12_BUFFER_CONFIG = @import("std").mem.zeroes(YV12_BUFFER_CONFIG),
};
pub const VP9_COMMON = struct_VP9Common;
pub export const antfly_vp9_vp9_mv_joint_tree: [6]vpx_tree_index = [6]vpx_tree_index{
    @truncate(-MV_JOINT_ZERO),
    2,
    @truncate(-MV_JOINT_HNZVZ),
    4,
    @truncate(-MV_JOINT_HZVNZ),
    @truncate(-MV_JOINT_HNZVNZ),
};
pub export const antfly_vp9_vp9_mv_class_tree: [20]vpx_tree_index = [20]vpx_tree_index{
    @truncate(-MV_CLASS_0),
    2,
    @truncate(-MV_CLASS_1),
    4,
    6,
    8,
    @truncate(-MV_CLASS_2),
    @truncate(-MV_CLASS_3),
    10,
    12,
    @truncate(-MV_CLASS_4),
    @truncate(-MV_CLASS_5),
    @truncate(-MV_CLASS_6),
    14,
    16,
    18,
    @truncate(-MV_CLASS_7),
    @truncate(-MV_CLASS_8),
    @truncate(-MV_CLASS_9),
    @truncate(-MV_CLASS_10),
};
pub export const antfly_vp9_vp9_mv_class0_tree: [2]vpx_tree_index = [2]vpx_tree_index{
    @truncate(-@as(c_int, 0)),
    @truncate(-@as(c_int, 1)),
};
pub export const antfly_vp9_vp9_mv_fp_tree: [6]vpx_tree_index = [6]vpx_tree_index{
    @truncate(-@as(c_int, 0)),
    2,
    @truncate(-@as(c_int, 1)),
    4,
    @truncate(-@as(c_int, 2)),
    @truncate(-@as(c_int, 3)),
};
pub const default_nmv_context: nmv_context = nmv_context{
    .joints = [3]vpx_prob{
        32,
        64,
        96,
    },
    .comps = [2]nmv_component{
        nmv_component{
            .sign = 128,
            .classes = [10]vpx_prob{
                224,
                144,
                192,
                168,
                192,
                176,
                192,
                198,
                198,
                245,
            },
            .class0 = [1]vpx_prob{
                216,
            },
            .bits = [10]vpx_prob{
                136,
                140,
                148,
                160,
                176,
                192,
                224,
                234,
                234,
                240,
            },
            .class0_fp = [2][3]vpx_prob{
                [3]vpx_prob{
                    128,
                    128,
                    64,
                },
                [3]vpx_prob{
                    96,
                    112,
                    64,
                },
            },
            .fp = [3]vpx_prob{
                64,
                96,
                64,
            },
            .class0_hp = 160,
            .hp = 128,
        },
        nmv_component{
            .sign = 128,
            .classes = [10]vpx_prob{
                216,
                128,
                176,
                160,
                176,
                176,
                192,
                198,
                198,
                208,
            },
            .class0 = [1]vpx_prob{
                208,
            },
            .bits = [10]vpx_prob{
                136,
                140,
                148,
                160,
                176,
                192,
                224,
                234,
                234,
                240,
            },
            .class0_fp = [2][3]vpx_prob{
                [3]vpx_prob{
                    128,
                    128,
                    64,
                },
                [3]vpx_prob{
                    96,
                    112,
                    64,
                },
            },
            .fp = [3]vpx_prob{
                64,
                96,
                64,
            },
            .class0_hp = 160,
            .hp = 128,
        },
    },
};
pub const log_in_base_2: [1025]u8 = [1025]u8{
    0,
    0,
    1,
    1,
    2,
    2,
    2,
    2,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    10,
};
pub fn mv_class_base(arg_c: MV_CLASS_TYPE) callconv(.c) c_int {
    var c = arg_c;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (c != 0) (@as(c_int, 1) << @intCast(@as(c_int, 1))) << @intCast(@as(c_int, @bitCast(c +% @as(MV_CLASS_TYPE, 2)))) else @as(c_int, 0);
}
pub export fn antfly_vp9_vp9_get_mv_class(arg_z: c_int, arg_offset: [*c]c_int) MV_CLASS_TYPE {
    var z = arg_z;
    _ = &z;
    var offset = arg_offset;
    _ = &offset;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const c: MV_CLASS_TYPE = @bitCast(if (z >= ((@as(c_int, 1) << @intCast(@as(c_int, 1))) * @as(c_int, 4096))) MV_CLASS_10 else @as(c_int, @bitCast(@as(MV_CLASS_TYPE, log_in_base_2[@bitCast(@as(isize, @intCast(z >> @intCast(@as(c_int, 3)))))]))));
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (offset != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        offset.* = z - mv_class_base(c);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return c;
}
pub fn inc_mv_component(arg_v: c_int, arg_comp_counts: [*c]nmv_component_counts, arg_incr: c_int, arg_usehp: c_int) callconv(.c) void {
    var v = arg_v;
    _ = &v;
    var comp_counts = arg_comp_counts;
    _ = &comp_counts;
    var incr = arg_incr;
    _ = &incr;
    var usehp = arg_usehp;
    _ = &usehp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s: c_int = undefined;
    _ = &s;
    var z: c_int = undefined;
    _ = &z;
    var c: c_int = undefined;
    _ = &c;
    var o: c_int = undefined;
    _ = &o;
    var d: c_int = undefined;
    _ = &d;
    var e: c_int = undefined;
    _ = &e;
    var f: c_int = undefined;
    _ = &f;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s = @as(c_int, @intFromBool(v < @as(c_int, 0)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    comp_counts.*.sign[@bitCast(@as(isize, @intCast(s)))] +%= @bitCast(@as(c_int, incr));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    z = (if (s != 0) -v else v) - @as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    c = @bitCast(antfly_vp9_vp9_get_mv_class(z, &o));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    comp_counts.*.classes[@bitCast(@as(isize, @intCast(c)))] +%= @bitCast(@as(c_int, incr));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d = o >> @intCast(@as(c_int, 3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    f = (o >> @intCast(@as(c_int, 1))) & @as(c_int, 3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    e = o & @as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (c == MV_CLASS_0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        comp_counts.*.class0[@bitCast(@as(isize, @intCast(d)))] +%= @bitCast(@as(c_int, incr));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        comp_counts.*.class0_fp[@bitCast(@as(isize, @intCast(d)))][@bitCast(@as(isize, @intCast(f)))] +%= @bitCast(@as(c_int, incr));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        comp_counts.*.class0_hp[@bitCast(@as(isize, @intCast(e)))] +%= @bitCast(@as(c_int, usehp * incr));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var i: c_int = undefined;
        _ = &i;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var b: c_int = (c + @as(c_int, 1)) - @as(c_int, 1);
        _ = &b;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < b) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                comp_counts.*.bits[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast((d >> @intCast(i)) & @as(c_int, 1))))] +%= @bitCast(@as(c_int, incr));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        comp_counts.*.fp[@bitCast(@as(isize, @intCast(f)))] +%= @bitCast(@as(c_int, incr));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        comp_counts.*.hp[@bitCast(@as(isize, @intCast(e)))] +%= @bitCast(@as(c_int, usehp * incr));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_inc_mv(arg_mv_1: [*c]const MV, arg_counts: [*c]nmv_context_counts) void {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var counts = arg_counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(counts))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const j: MV_JOINT_TYPE = vp9_get_mv_joint(mv_1);
        _ = &j;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        counts.*.joints[j] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (mv_joint_vertical(j) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            inc_mv_component(mv_1.*.row, &counts.*.comps[@as(c_int, 0)], 1, 1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (mv_joint_horizontal(j) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            inc_mv_component(mv_1.*.col, &counts.*.comps[@as(c_int, 1)], 1, 1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_adapt_mv_probs(arg_cm: [*c]VP9_COMMON, arg_allow_hp: c_int) void {
    var cm = arg_cm;
    _ = &cm;
    var allow_hp = arg_allow_hp;
    _ = &allow_hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var fc: [*c]nmv_context = &cm.*.fc.*.nmvc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var pre_fc: [*c]const nmv_context = &cm.*.frame_contexts[cm.*.frame_context_idx].nmvc;
    _ = &pre_fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var counts: [*c]const nmv_context_counts = &cm.*.counts.mv;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_mv_joint_tree)), @ptrCast(@alignCast(&pre_fc.*.joints)), @ptrCast(@alignCast(&counts.*.joints)), @ptrCast(@alignCast(&fc.*.joints)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var comp: [*c]nmv_component = &fc.*.comps[@bitCast(@as(isize, @intCast(i)))];
            _ = &comp;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var pre_comp: [*c]const nmv_component = &pre_fc.*.comps[@bitCast(@as(isize, @intCast(i)))];
            _ = &pre_comp;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var c: [*c]const nmv_component_counts = &counts.*.comps[@bitCast(@as(isize, @intCast(i)))];
            _ = &c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            comp.*.sign = mode_mv_merge_probs(pre_comp.*.sign, @ptrCast(@alignCast(&c.*.sign)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_mv_class_tree)), @ptrCast(@alignCast(&pre_comp.*.classes)), @ptrCast(@alignCast(&c.*.classes)), @ptrCast(@alignCast(&comp.*.classes)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_mv_class0_tree)), @ptrCast(@alignCast(&pre_comp.*.class0)), @ptrCast(@alignCast(&c.*.class0)), @ptrCast(@alignCast(&comp.*.class0)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < ((@as(c_int, 11) + @as(c_int, 1)) - @as(c_int, 2))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comp.*.bits[@bitCast(@as(isize, @intCast(j)))] = mode_mv_merge_probs(pre_comp.*.bits[@bitCast(@as(isize, @intCast(j)))], @ptrCast(@alignCast(&c.*.bits[@bitCast(@as(isize, @intCast(j)))])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            {
                j = 0;
                while (j < (@as(c_int, 1) << @intCast(@as(c_int, 1)))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_mv_fp_tree)), @ptrCast(@alignCast(&pre_comp.*.class0_fp[@bitCast(@as(isize, @intCast(j)))])), @ptrCast(@alignCast(&c.*.class0_fp[@bitCast(@as(isize, @intCast(j)))])), @ptrCast(@alignCast(&comp.*.class0_fp[@bitCast(@as(isize, @intCast(j)))])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_mv_fp_tree)), @ptrCast(@alignCast(&pre_comp.*.fp)), @ptrCast(@alignCast(&c.*.fp)), @ptrCast(@alignCast(&comp.*.fp)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (allow_hp != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                comp.*.class0_hp = mode_mv_merge_probs(pre_comp.*.class0_hp, @ptrCast(@alignCast(&c.*.class0_hp)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                comp.*.hp = mode_mv_merge_probs(pre_comp.*.hp, @ptrCast(@alignCast(&c.*.hp)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_init_mv_probs(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.fc.*.nmvc = default_nmv_context;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
