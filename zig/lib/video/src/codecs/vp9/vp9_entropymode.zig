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
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_NONE = common.PARTITION_NONE;
pub const PARTITION_HORZ = common.PARTITION_HORZ;
pub const PARTITION_VERT = common.PARTITION_VERT;
pub const PARTITION_SPLIT = common.PARTITION_SPLIT;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE_SELECT = common.TX_MODE_SELECT;
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
pub extern fn antfly_vp9_vp9_default_coef_probs(cm: [*c]struct_VP9Common) void;
pub extern fn antfly_vp9_vp9_init_mv_probs(cm: [*c]struct_VP9Common) void;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
pub extern fn antfly_vp9_vp9_clearall_segfeatures(seg: [*c]struct_segmentation) void;
pub const KEY_FRAME = common.KEY_FRAME;
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
pub fn frame_is_intra_only(cm: [*c]const VP9_COMMON) callconv(.c) c_int {
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((cm.*.frame_type == @as(FRAME_TYPE, KEY_FRAME)) or (@as(c_int, cm.*.intra_only) != 0)));
}
pub export var antfly_vp9_vp9_kf_y_mode_prob: [10][10][9]vpx_prob = [10][10][9]vpx_prob{
    [10][9]vpx_prob{
        [9]vpx_prob{
            137,
            30,
            42,
            148,
            151,
            207,
            70,
            52,
            91,
        },
        [9]vpx_prob{
            92,
            45,
            102,
            136,
            116,
            180,
            74,
            90,
            100,
        },
        [9]vpx_prob{
            73,
            32,
            19,
            187,
            222,
            215,
            46,
            34,
            100,
        },
        [9]vpx_prob{
            91,
            30,
            32,
            116,
            121,
            186,
            93,
            86,
            94,
        },
        [9]vpx_prob{
            72,
            35,
            36,
            149,
            68,
            206,
            68,
            63,
            105,
        },
        [9]vpx_prob{
            73,
            31,
            28,
            138,
            57,
            124,
            55,
            122,
            151,
        },
        [9]vpx_prob{
            67,
            23,
            21,
            140,
            126,
            197,
            40,
            37,
            171,
        },
        [9]vpx_prob{
            86,
            27,
            28,
            128,
            154,
            212,
            45,
            43,
            53,
        },
        [9]vpx_prob{
            74,
            32,
            27,
            107,
            86,
            160,
            63,
            134,
            102,
        },
        [9]vpx_prob{
            59,
            67,
            44,
            140,
            161,
            202,
            78,
            67,
            119,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            63,
            36,
            126,
            146,
            123,
            158,
            60,
            90,
            96,
        },
        [9]vpx_prob{
            43,
            46,
            168,
            134,
            107,
            128,
            69,
            142,
            92,
        },
        [9]vpx_prob{
            44,
            29,
            68,
            159,
            201,
            177,
            50,
            57,
            77,
        },
        [9]vpx_prob{
            58,
            38,
            76,
            114,
            97,
            172,
            78,
            133,
            92,
        },
        [9]vpx_prob{
            46,
            41,
            76,
            140,
            63,
            184,
            69,
            112,
            57,
        },
        [9]vpx_prob{
            38,
            32,
            85,
            140,
            46,
            112,
            54,
            151,
            133,
        },
        [9]vpx_prob{
            39,
            27,
            61,
            131,
            110,
            175,
            44,
            75,
            136,
        },
        [9]vpx_prob{
            52,
            30,
            74,
            113,
            130,
            175,
            51,
            64,
            58,
        },
        [9]vpx_prob{
            47,
            35,
            80,
            100,
            74,
            143,
            64,
            163,
            74,
        },
        [9]vpx_prob{
            36,
            61,
            116,
            114,
            128,
            162,
            80,
            125,
            82,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            82,
            26,
            26,
            171,
            208,
            204,
            44,
            32,
            105,
        },
        [9]vpx_prob{
            55,
            44,
            68,
            166,
            179,
            192,
            57,
            57,
            108,
        },
        [9]vpx_prob{
            42,
            26,
            11,
            199,
            241,
            228,
            23,
            15,
            85,
        },
        [9]vpx_prob{
            68,
            42,
            19,
            131,
            160,
            199,
            55,
            52,
            83,
        },
        [9]vpx_prob{
            58,
            50,
            25,
            139,
            115,
            232,
            39,
            52,
            118,
        },
        [9]vpx_prob{
            50,
            35,
            33,
            153,
            104,
            162,
            64,
            59,
            131,
        },
        [9]vpx_prob{
            44,
            24,
            16,
            150,
            177,
            202,
            33,
            19,
            156,
        },
        [9]vpx_prob{
            55,
            27,
            12,
            153,
            203,
            218,
            26,
            27,
            49,
        },
        [9]vpx_prob{
            53,
            49,
            21,
            110,
            116,
            168,
            59,
            80,
            76,
        },
        [9]vpx_prob{
            38,
            72,
            19,
            168,
            203,
            212,
            50,
            50,
            107,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            103,
            26,
            36,
            129,
            132,
            201,
            83,
            80,
            93,
        },
        [9]vpx_prob{
            59,
            38,
            83,
            112,
            103,
            162,
            98,
            136,
            90,
        },
        [9]vpx_prob{
            62,
            30,
            23,
            158,
            200,
            207,
            59,
            57,
            50,
        },
        [9]vpx_prob{
            67,
            30,
            29,
            84,
            86,
            191,
            102,
            91,
            59,
        },
        [9]vpx_prob{
            60,
            32,
            33,
            112,
            71,
            220,
            64,
            89,
            104,
        },
        [9]vpx_prob{
            53,
            26,
            34,
            130,
            56,
            149,
            84,
            120,
            103,
        },
        [9]vpx_prob{
            53,
            21,
            23,
            133,
            109,
            210,
            56,
            77,
            172,
        },
        [9]vpx_prob{
            77,
            19,
            29,
            112,
            142,
            228,
            55,
            66,
            36,
        },
        [9]vpx_prob{
            61,
            29,
            29,
            93,
            97,
            165,
            83,
            175,
            162,
        },
        [9]vpx_prob{
            47,
            47,
            43,
            114,
            137,
            181,
            100,
            99,
            95,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            69,
            23,
            29,
            128,
            83,
            199,
            46,
            44,
            101,
        },
        [9]vpx_prob{
            53,
            40,
            55,
            139,
            69,
            183,
            61,
            80,
            110,
        },
        [9]vpx_prob{
            40,
            29,
            19,
            161,
            180,
            207,
            43,
            24,
            91,
        },
        [9]vpx_prob{
            60,
            34,
            19,
            105,
            61,
            198,
            53,
            64,
            89,
        },
        [9]vpx_prob{
            52,
            31,
            22,
            158,
            40,
            209,
            58,
            62,
            89,
        },
        [9]vpx_prob{
            44,
            31,
            29,
            147,
            46,
            158,
            56,
            102,
            198,
        },
        [9]vpx_prob{
            35,
            19,
            12,
            135,
            87,
            209,
            41,
            45,
            167,
        },
        [9]vpx_prob{
            55,
            25,
            21,
            118,
            95,
            215,
            38,
            39,
            66,
        },
        [9]vpx_prob{
            51,
            38,
            25,
            113,
            58,
            164,
            70,
            93,
            97,
        },
        [9]vpx_prob{
            47,
            54,
            34,
            146,
            108,
            203,
            72,
            103,
            151,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            64,
            19,
            37,
            156,
            66,
            138,
            49,
            95,
            133,
        },
        [9]vpx_prob{
            46,
            27,
            80,
            150,
            55,
            124,
            55,
            121,
            135,
        },
        [9]vpx_prob{
            36,
            23,
            27,
            165,
            149,
            166,
            54,
            64,
            118,
        },
        [9]vpx_prob{
            53,
            21,
            36,
            131,
            63,
            163,
            60,
            109,
            81,
        },
        [9]vpx_prob{
            40,
            26,
            35,
            154,
            40,
            185,
            51,
            97,
            123,
        },
        [9]vpx_prob{
            35,
            19,
            34,
            179,
            19,
            97,
            48,
            129,
            124,
        },
        [9]vpx_prob{
            36,
            20,
            26,
            136,
            62,
            164,
            33,
            77,
            154,
        },
        [9]vpx_prob{
            45,
            18,
            32,
            130,
            90,
            157,
            40,
            79,
            91,
        },
        [9]vpx_prob{
            45,
            26,
            28,
            129,
            45,
            129,
            49,
            147,
            123,
        },
        [9]vpx_prob{
            38,
            44,
            51,
            136,
            74,
            162,
            57,
            97,
            121,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            75,
            17,
            22,
            136,
            138,
            185,
            32,
            34,
            166,
        },
        [9]vpx_prob{
            56,
            39,
            58,
            133,
            117,
            173,
            48,
            53,
            187,
        },
        [9]vpx_prob{
            35,
            21,
            12,
            161,
            212,
            207,
            20,
            23,
            145,
        },
        [9]vpx_prob{
            56,
            29,
            19,
            117,
            109,
            181,
            55,
            68,
            112,
        },
        [9]vpx_prob{
            47,
            29,
            17,
            153,
            64,
            220,
            59,
            51,
            114,
        },
        [9]vpx_prob{
            46,
            16,
            24,
            136,
            76,
            147,
            41,
            64,
            172,
        },
        [9]vpx_prob{
            34,
            17,
            11,
            108,
            152,
            187,
            13,
            15,
            209,
        },
        [9]vpx_prob{
            51,
            24,
            14,
            115,
            133,
            209,
            32,
            26,
            104,
        },
        [9]vpx_prob{
            55,
            30,
            18,
            122,
            79,
            179,
            44,
            88,
            116,
        },
        [9]vpx_prob{
            37,
            49,
            25,
            129,
            168,
            164,
            41,
            54,
            148,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            82,
            22,
            32,
            127,
            143,
            213,
            39,
            41,
            70,
        },
        [9]vpx_prob{
            62,
            44,
            61,
            123,
            105,
            189,
            48,
            57,
            64,
        },
        [9]vpx_prob{
            47,
            25,
            17,
            175,
            222,
            220,
            24,
            30,
            86,
        },
        [9]vpx_prob{
            68,
            36,
            17,
            106,
            102,
            206,
            59,
            74,
            74,
        },
        [9]vpx_prob{
            57,
            39,
            23,
            151,
            68,
            216,
            55,
            63,
            58,
        },
        [9]vpx_prob{
            49,
            30,
            35,
            141,
            70,
            168,
            82,
            40,
            115,
        },
        [9]vpx_prob{
            51,
            25,
            15,
            136,
            129,
            202,
            38,
            35,
            139,
        },
        [9]vpx_prob{
            68,
            26,
            16,
            111,
            141,
            215,
            29,
            28,
            28,
        },
        [9]vpx_prob{
            59,
            39,
            19,
            114,
            75,
            180,
            77,
            104,
            42,
        },
        [9]vpx_prob{
            40,
            61,
            26,
            126,
            152,
            206,
            61,
            59,
            93,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            78,
            23,
            39,
            111,
            117,
            170,
            74,
            124,
            94,
        },
        [9]vpx_prob{
            48,
            34,
            86,
            101,
            92,
            146,
            78,
            179,
            134,
        },
        [9]vpx_prob{
            47,
            22,
            24,
            138,
            187,
            178,
            68,
            69,
            59,
        },
        [9]vpx_prob{
            56,
            25,
            33,
            105,
            112,
            187,
            95,
            177,
            129,
        },
        [9]vpx_prob{
            48,
            31,
            27,
            114,
            63,
            183,
            82,
            116,
            56,
        },
        [9]vpx_prob{
            43,
            28,
            37,
            121,
            63,
            123,
            61,
            192,
            169,
        },
        [9]vpx_prob{
            42,
            17,
            24,
            109,
            97,
            177,
            56,
            76,
            122,
        },
        [9]vpx_prob{
            58,
            18,
            28,
            105,
            139,
            182,
            70,
            92,
            63,
        },
        [9]vpx_prob{
            46,
            23,
            32,
            74,
            86,
            150,
            67,
            183,
            88,
        },
        [9]vpx_prob{
            36,
            38,
            48,
            92,
            122,
            165,
            88,
            137,
            91,
        },
    },
    [10][9]vpx_prob{
        [9]vpx_prob{
            65,
            70,
            60,
            155,
            159,
            199,
            61,
            60,
            81,
        },
        [9]vpx_prob{
            44,
            78,
            115,
            132,
            119,
            173,
            71,
            112,
            93,
        },
        [9]vpx_prob{
            39,
            38,
            21,
            184,
            227,
            206,
            42,
            32,
            64,
        },
        [9]vpx_prob{
            58,
            47,
            36,
            124,
            137,
            193,
            80,
            82,
            78,
        },
        [9]vpx_prob{
            49,
            50,
            35,
            144,
            95,
            205,
            63,
            78,
            59,
        },
        [9]vpx_prob{
            41,
            53,
            52,
            148,
            71,
            142,
            65,
            128,
            51,
        },
        [9]vpx_prob{
            40,
            36,
            28,
            143,
            143,
            202,
            40,
            55,
            137,
        },
        [9]vpx_prob{
            52,
            34,
            29,
            129,
            183,
            227,
            42,
            35,
            43,
        },
        [9]vpx_prob{
            42,
            44,
            44,
            104,
            105,
            164,
            64,
            130,
            80,
        },
        [9]vpx_prob{
            43,
            81,
            53,
            140,
            169,
            204,
            68,
            84,
            72,
        },
    },
};
pub export var antfly_vp9_vp9_kf_uv_mode_prob: [10][9]vpx_prob = [10][9]vpx_prob{
    [9]vpx_prob{
        144,
        11,
        54,
        157,
        195,
        130,
        46,
        58,
        108,
    },
    [9]vpx_prob{
        118,
        15,
        123,
        148,
        131,
        101,
        44,
        93,
        131,
    },
    [9]vpx_prob{
        113,
        12,
        23,
        188,
        226,
        142,
        26,
        32,
        125,
    },
    [9]vpx_prob{
        120,
        11,
        50,
        123,
        163,
        135,
        64,
        77,
        103,
    },
    [9]vpx_prob{
        113,
        9,
        36,
        155,
        111,
        157,
        32,
        44,
        161,
    },
    [9]vpx_prob{
        116,
        9,
        55,
        176,
        76,
        96,
        37,
        61,
        149,
    },
    [9]vpx_prob{
        115,
        9,
        28,
        141,
        161,
        167,
        21,
        25,
        193,
    },
    [9]vpx_prob{
        120,
        12,
        32,
        145,
        195,
        142,
        32,
        38,
        86,
    },
    [9]vpx_prob{
        116,
        12,
        64,
        120,
        140,
        125,
        49,
        115,
        121,
    },
    [9]vpx_prob{
        102,
        19,
        66,
        162,
        182,
        122,
        35,
        59,
        128,
    },
};
pub var default_if_y_probs: [4][9]vpx_prob = [4][9]vpx_prob{
    [9]vpx_prob{
        65,
        32,
        18,
        144,
        162,
        194,
        41,
        51,
        98,
    },
    [9]vpx_prob{
        132,
        68,
        18,
        165,
        217,
        196,
        45,
        40,
        78,
    },
    [9]vpx_prob{
        173,
        80,
        19,
        176,
        240,
        193,
        64,
        35,
        46,
    },
    [9]vpx_prob{
        221,
        135,
        38,
        194,
        248,
        121,
        96,
        85,
        29,
    },
};
pub var default_if_uv_probs: [10][9]vpx_prob = [10][9]vpx_prob{
    [9]vpx_prob{
        120,
        7,
        76,
        176,
        208,
        126,
        28,
        54,
        103,
    },
    [9]vpx_prob{
        48,
        12,
        154,
        155,
        139,
        90,
        34,
        117,
        119,
    },
    [9]vpx_prob{
        67,
        6,
        25,
        204,
        243,
        158,
        13,
        21,
        96,
    },
    [9]vpx_prob{
        97,
        5,
        44,
        131,
        176,
        139,
        48,
        68,
        97,
    },
    [9]vpx_prob{
        83,
        5,
        42,
        156,
        111,
        152,
        26,
        49,
        152,
    },
    [9]vpx_prob{
        80,
        5,
        58,
        178,
        74,
        83,
        33,
        62,
        145,
    },
    [9]vpx_prob{
        86,
        5,
        32,
        154,
        192,
        168,
        14,
        22,
        163,
    },
    [9]vpx_prob{
        85,
        5,
        32,
        156,
        216,
        148,
        19,
        29,
        73,
    },
    [9]vpx_prob{
        77,
        7,
        64,
        116,
        132,
        122,
        37,
        126,
        120,
    },
    [9]vpx_prob{
        101,
        21,
        107,
        181,
        192,
        103,
        19,
        67,
        125,
    },
};
pub export var antfly_vp9_vp9_kf_partition_probs: [16][3]vpx_prob = [16][3]vpx_prob{
    [3]vpx_prob{
        158,
        97,
        94,
    },
    [3]vpx_prob{
        93,
        24,
        99,
    },
    [3]vpx_prob{
        85,
        119,
        44,
    },
    [3]vpx_prob{
        62,
        59,
        67,
    },
    [3]vpx_prob{
        149,
        53,
        53,
    },
    [3]vpx_prob{
        94,
        20,
        48,
    },
    [3]vpx_prob{
        83,
        53,
        24,
    },
    [3]vpx_prob{
        52,
        18,
        18,
    },
    [3]vpx_prob{
        150,
        40,
        39,
    },
    [3]vpx_prob{
        78,
        12,
        26,
    },
    [3]vpx_prob{
        67,
        33,
        11,
    },
    [3]vpx_prob{
        24,
        7,
        5,
    },
    [3]vpx_prob{
        174,
        35,
        49,
    },
    [3]vpx_prob{
        68,
        11,
        27,
    },
    [3]vpx_prob{
        57,
        15,
        9,
    },
    [3]vpx_prob{
        12,
        3,
        3,
    },
};
pub var default_partition_probs: [16][3]vpx_prob = [16][3]vpx_prob{
    [3]vpx_prob{
        199,
        122,
        141,
    },
    [3]vpx_prob{
        147,
        63,
        159,
    },
    [3]vpx_prob{
        148,
        133,
        118,
    },
    [3]vpx_prob{
        121,
        104,
        114,
    },
    [3]vpx_prob{
        174,
        73,
        87,
    },
    [3]vpx_prob{
        92,
        41,
        83,
    },
    [3]vpx_prob{
        82,
        99,
        50,
    },
    [3]vpx_prob{
        53,
        39,
        39,
    },
    [3]vpx_prob{
        177,
        58,
        59,
    },
    [3]vpx_prob{
        68,
        26,
        63,
    },
    [3]vpx_prob{
        52,
        79,
        25,
    },
    [3]vpx_prob{
        17,
        14,
        12,
    },
    [3]vpx_prob{
        222,
        34,
        30,
    },
    [3]vpx_prob{
        72,
        16,
        44,
    },
    [3]vpx_prob{
        58,
        32,
        12,
    },
    [3]vpx_prob{
        10,
        7,
        6,
    },
};
pub var default_inter_mode_probs: [7][3]vpx_prob = [7][3]vpx_prob{
    [3]vpx_prob{
        2,
        173,
        34,
    },
    [3]vpx_prob{
        7,
        145,
        85,
    },
    [3]vpx_prob{
        7,
        166,
        63,
    },
    [3]vpx_prob{
        7,
        94,
        66,
    },
    [3]vpx_prob{
        8,
        64,
        46,
    },
    [3]vpx_prob{
        17,
        81,
        31,
    },
    [3]vpx_prob{
        25,
        29,
        30,
    },
};
pub export const antfly_vp9_vp9_intra_mode_tree: [18]vpx_tree_index = [18]vpx_tree_index{
    @truncate(-@as(c_int, 0)),
    2,
    @truncate(-@as(c_int, 9)),
    4,
    @truncate(-@as(c_int, 1)),
    6,
    8,
    12,
    @truncate(-@as(c_int, 2)),
    10,
    @truncate(-@as(c_int, 4)),
    @truncate(-@as(c_int, 5)),
    @truncate(-@as(c_int, 3)),
    14,
    @truncate(-@as(c_int, 8)),
    16,
    @truncate(-@as(c_int, 6)),
    @truncate(-@as(c_int, 7)),
};
pub export const antfly_vp9_vp9_inter_mode_tree: [6]vpx_tree_index = [6]vpx_tree_index{
    @truncate(-(@as(c_int, 12) - @as(c_int, 10))),
    2,
    @truncate(-(@as(c_int, 10) - @as(c_int, 10))),
    4,
    @truncate(-(@as(c_int, 11) - @as(c_int, 10))),
    @truncate(-(@as(c_int, 13) - @as(c_int, 10))),
};
pub export const antfly_vp9_vp9_partition_tree: [6]vpx_tree_index = [6]vpx_tree_index{
    @truncate(-PARTITION_NONE),
    2,
    @truncate(-PARTITION_HORZ),
    4,
    @truncate(-PARTITION_VERT),
    @truncate(-PARTITION_SPLIT),
};
pub const default_intra_inter_p: [4]vpx_prob = [4]vpx_prob{
    9,
    102,
    187,
    225,
};
pub const default_comp_inter_p: [5]vpx_prob = [5]vpx_prob{
    239,
    183,
    119,
    96,
    41,
};
pub const default_comp_ref_p: [5]vpx_prob = [5]vpx_prob{
    50,
    126,
    123,
    221,
    226,
};
pub var default_single_ref_p: [5][2]vpx_prob = [5][2]vpx_prob{
    [2]vpx_prob{
        33,
        16,
    },
    [2]vpx_prob{
        77,
        74,
    },
    [2]vpx_prob{
        142,
        142,
    },
    [2]vpx_prob{
        172,
        170,
    },
    [2]vpx_prob{
        238,
        247,
    },
};
pub const default_tx_probs: struct_tx_probs = struct_tx_probs{
    .p32x32 = [2][3]vpx_prob{
        [3]vpx_prob{
            3,
            136,
            37,
        },
        [3]vpx_prob{
            5,
            52,
            13,
        },
    },
    .p16x16 = [2][2]vpx_prob{
        [2]vpx_prob{
            20,
            152,
        },
        [2]vpx_prob{
            15,
            101,
        },
    },
    .p8x8 = [2][1]vpx_prob{
        [1]vpx_prob{
            100,
        },
        [1]vpx_prob{
            66,
        },
    },
};
pub export fn antfly_vp9_tx_counts_to_branch_counts_32x32(arg_tx_count_32x32p: [*c]const c_uint, arg_ct_32x32p: [*c][2]c_uint) void {
    var tx_count_32x32p = arg_tx_count_32x32p;
    _ = &tx_count_32x32p;
    var ct_32x32p = arg_ct_32x32p;
    _ = &ct_32x32p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_32x32p[@as(c_int, 0)][@as(c_int, 0)] = tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_32x32p[@as(c_int, 0)][@as(c_int, 1)] = (tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] +% tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]) +% tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_32x32p[@as(c_int, 1)][@as(c_int, 0)] = tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_32x32p[@as(c_int, 1)][@as(c_int, 1)] = tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] +% tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_32x32p[@as(c_int, 2)][@as(c_int, 0)] = tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_32x32p[@as(c_int, 2)][@as(c_int, 1)] = tx_count_32x32p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_tx_counts_to_branch_counts_16x16(arg_tx_count_16x16p: [*c]const c_uint, arg_ct_16x16p: [*c][2]c_uint) void {
    var tx_count_16x16p = arg_tx_count_16x16p;
    _ = &tx_count_16x16p;
    var ct_16x16p = arg_ct_16x16p;
    _ = &ct_16x16p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_16x16p[@as(c_int, 0)][@as(c_int, 0)] = tx_count_16x16p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_16x16p[@as(c_int, 0)][@as(c_int, 1)] = tx_count_16x16p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] +% tx_count_16x16p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_16x16p[@as(c_int, 1)][@as(c_int, 0)] = tx_count_16x16p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_16x16p[@as(c_int, 1)][@as(c_int, 1)] = tx_count_16x16p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_tx_counts_to_branch_counts_8x8(arg_tx_count_8x8p: [*c]const c_uint, arg_ct_8x8p: [*c][2]c_uint) void {
    var tx_count_8x8p = arg_tx_count_8x8p;
    _ = &tx_count_8x8p;
    var ct_8x8p = arg_ct_8x8p;
    _ = &ct_8x8p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_8x8p[@as(c_int, 0)][@as(c_int, 0)] = tx_count_8x8p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ct_8x8p[@as(c_int, 0)][@as(c_int, 1)] = tx_count_8x8p[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub const default_skip_probs: [3]vpx_prob = [3]vpx_prob{
    192,
    128,
    64,
};
pub var default_switchable_interp_prob: [4][2]vpx_prob = [4][2]vpx_prob{
    [2]vpx_prob{
        235,
        162,
    },
    [2]vpx_prob{
        36,
        255,
    },
    [2]vpx_prob{
        34,
        3,
    },
    [2]vpx_prob{
        149,
        144,
    },
};
pub fn init_mode_probs(arg_fc: [*c]FRAME_CONTEXT) callconv(.c) void {
    var fc = arg_fc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][9]vpx_prob, @ptrCast(@alignCast(&fc.*.uv_mode_prob))))), @ptrCast(@alignCast(@as([*c][9]vpx_prob, @ptrCast(@alignCast(&default_if_uv_probs))))), @sizeOf(@TypeOf(default_if_uv_probs)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][9]vpx_prob, @ptrCast(@alignCast(&fc.*.y_mode_prob))))), @ptrCast(@alignCast(@as([*c][9]vpx_prob, @ptrCast(@alignCast(&default_if_y_probs))))), @sizeOf(@TypeOf(default_if_y_probs)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][2]vpx_prob, @ptrCast(@alignCast(&fc.*.switchable_interp_prob))))), @ptrCast(@alignCast(@as([*c][2]vpx_prob, @ptrCast(@alignCast(&default_switchable_interp_prob))))), @sizeOf(@TypeOf(default_switchable_interp_prob)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][3]vpx_prob, @ptrCast(@alignCast(&fc.*.partition_prob))))), @ptrCast(@alignCast(@as([*c][3]vpx_prob, @ptrCast(@alignCast(&default_partition_probs))))), @sizeOf(@TypeOf(default_partition_probs)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]vpx_prob, @ptrCast(@alignCast(&fc.*.intra_inter_prob))))), @ptrCast(@alignCast(@as([*c]const vpx_prob, @ptrCast(@alignCast(&default_intra_inter_p))))), @sizeOf(@TypeOf(default_intra_inter_p)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]vpx_prob, @ptrCast(@alignCast(&fc.*.comp_inter_prob))))), @ptrCast(@alignCast(@as([*c]const vpx_prob, @ptrCast(@alignCast(&default_comp_inter_p))))), @sizeOf(@TypeOf(default_comp_inter_p)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]vpx_prob, @ptrCast(@alignCast(&fc.*.comp_ref_prob))))), @ptrCast(@alignCast(@as([*c]const vpx_prob, @ptrCast(@alignCast(&default_comp_ref_p))))), @sizeOf(@TypeOf(default_comp_ref_p)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][2]vpx_prob, @ptrCast(@alignCast(&fc.*.single_ref_prob))))), @ptrCast(@alignCast(@as([*c][2]vpx_prob, @ptrCast(@alignCast(&default_single_ref_p))))), @sizeOf(@TypeOf(default_single_ref_p)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    fc.*.tx_probs = default_tx_probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]vpx_prob, @ptrCast(@alignCast(&fc.*.skip_probs))))), @ptrCast(@alignCast(@as([*c]const vpx_prob, @ptrCast(@alignCast(&default_skip_probs))))), @sizeOf(@TypeOf(default_skip_probs)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][3]vpx_prob, @ptrCast(@alignCast(&fc.*.inter_mode_probs))))), @ptrCast(@alignCast(@as([*c][3]vpx_prob, @ptrCast(@alignCast(&default_inter_mode_probs))))), @sizeOf(@TypeOf(default_inter_mode_probs)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export const antfly_vp9_vp9_switchable_interp_tree: [4]vpx_tree_index = [4]vpx_tree_index{
    @truncate(-@as(c_int, 0)),
    2,
    @truncate(-@as(c_int, 1)),
    @truncate(-@as(c_int, 2)),
};
pub export fn antfly_vp9_vp9_adapt_mode_probs(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
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
    var fc: [*c]FRAME_CONTEXT = cm.*.fc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var pre_fc: [*c]const FRAME_CONTEXT = &cm.*.frame_contexts[cm.*.frame_context_idx];
    _ = &pre_fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var counts: [*c]const FRAME_COUNTS = &cm.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            fc.*.intra_inter_prob[@bitCast(@as(isize, @intCast(i)))] = mode_mv_merge_probs(pre_fc.*.intra_inter_prob[@bitCast(@as(isize, @intCast(i)))], @ptrCast(@alignCast(&counts.*.intra_inter[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 5)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            fc.*.comp_inter_prob[@bitCast(@as(isize, @intCast(i)))] = mode_mv_merge_probs(pre_fc.*.comp_inter_prob[@bitCast(@as(isize, @intCast(i)))], @ptrCast(@alignCast(&counts.*.comp_inter[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 5)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            fc.*.comp_ref_prob[@bitCast(@as(isize, @intCast(i)))] = mode_mv_merge_probs(pre_fc.*.comp_ref_prob[@bitCast(@as(isize, @intCast(i)))], @ptrCast(@alignCast(&counts.*.comp_ref[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 5)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 2)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    fc.*.single_ref_prob[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] = mode_mv_merge_probs(pre_fc.*.single_ref_prob[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))], @ptrCast(@alignCast(&counts.*.single_ref[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
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
    }
    {
        i = 0;
        while (i < @as(c_int, 7)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_inter_mode_tree)), @ptrCast(@alignCast(&pre_fc.*.inter_mode_probs[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&counts.*.inter_mode[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&fc.*.inter_mode_probs[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_intra_mode_tree)), @ptrCast(@alignCast(&pre_fc.*.y_mode_prob[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&counts.*.y_mode[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&fc.*.y_mode_prob[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < (@as(c_int, 9) + @as(c_int, 1))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_intra_mode_tree)), @ptrCast(@alignCast(&pre_fc.*.uv_mode_prob[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&counts.*.uv_mode[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&fc.*.uv_mode_prob[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < (@as(c_int, 4) * @as(c_int, 4))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_partition_tree)), @ptrCast(@alignCast(&pre_fc.*.partition_prob[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&counts.*.partition[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&fc.*.partition_prob[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (@as(c_int, cm.*.interp_filter) == @as(c_int, 4)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < (@as(c_int, 3) + @as(c_int, 1))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_tree_merge_probs(@ptrCast(@alignCast(&antfly_vp9_vp9_switchable_interp_tree)), @ptrCast(@alignCast(&pre_fc.*.switchable_interp_prob[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&counts.*.switchable_interp[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&fc.*.switchable_interp_prob[@bitCast(@as(isize, @intCast(i)))])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.tx_mode == @as(TX_MODE, TX_MODE_SELECT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var branch_ct_8x8p: [1][2]c_uint = undefined;
        _ = &branch_ct_8x8p;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var branch_ct_16x16p: [2][2]c_uint = undefined;
        _ = &branch_ct_16x16p;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var branch_ct_32x32p: [3][2]c_uint = undefined;
        _ = &branch_ct_32x32p;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 2)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_tx_counts_to_branch_counts_8x8(@ptrCast(@alignCast(&counts.*.tx.p8x8[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&branch_ct_8x8p)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 3))) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        fc.*.tx_probs.p8x8[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] = mode_mv_merge_probs(pre_fc.*.tx_probs.p8x8[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))], @ptrCast(@alignCast(&branch_ct_8x8p[@bitCast(@as(isize, @intCast(j)))])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                antfly_vp9_tx_counts_to_branch_counts_16x16(@ptrCast(@alignCast(&counts.*.tx.p16x16[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&branch_ct_16x16p)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 2))) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        fc.*.tx_probs.p16x16[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] = mode_mv_merge_probs(pre_fc.*.tx_probs.p16x16[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))], @ptrCast(@alignCast(&branch_ct_16x16p[@bitCast(@as(isize, @intCast(j)))])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                antfly_vp9_tx_counts_to_branch_counts_32x32(@ptrCast(@alignCast(&counts.*.tx.p32x32[@bitCast(@as(isize, @intCast(i)))])), @ptrCast(@alignCast(&branch_ct_32x32p)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 1))) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        fc.*.tx_probs.p32x32[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] = mode_mv_merge_probs(pre_fc.*.tx_probs.p32x32[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))], @ptrCast(@alignCast(&branch_ct_32x32p[@bitCast(@as(isize, @intCast(j)))])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            fc.*.skip_probs[@bitCast(@as(isize, @intCast(i)))] = mode_mv_merge_probs(pre_fc.*.skip_probs[@bitCast(@as(isize, @intCast(i)))], @ptrCast(@alignCast(&counts.*.skip[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn set_default_lf_deltas(arg_lf: [*c]struct_loopfilter) callconv(.c) void {
    var lf = arg_lf;
    _ = &lf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.mode_ref_delta_enabled = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.mode_ref_delta_update = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.ref_deltas[@as(c_int, 0)] = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.ref_deltas[@as(c_int, 1)] = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.ref_deltas[@as(c_int, 2)] = @truncate(-@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.ref_deltas[@as(c_int, 3)] = @truncate(-@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.mode_deltas[@as(c_int, 0)] = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.mode_deltas[@as(c_int, 1)] = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_setup_past_independence(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lf: [*c]struct_loopfilter = &cm.*.lf;
    _ = &lf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_clearall_segfeatures(&cm.*.seg);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.seg.abs_delta = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.last_frame_seg_map != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.last_frame_seg_map)), 0, @bitCast(@as(c_long, cm.*.mi_rows * cm.*.mi_cols)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (cm.*.current_frame_seg_map != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.current_frame_seg_map)), 0, @bitCast(@as(c_long, cm.*.mi_rows * cm.*.mi_cols)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&lf.*.last_ref_deltas)), 0, @sizeOf(@TypeOf(lf.*.last_ref_deltas)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&lf.*.last_mode_deltas)), 0, @sizeOf(@TypeOf(lf.*.last_mode_deltas)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    set_default_lf_deltas(lf);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.last_sharpness_level = -@as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_default_coef_probs(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    init_mode_probs(cm.*.fc);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_init_mv_probs(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.fc.*.initialized = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((cm.*.frame_type == @as(FRAME_TYPE, KEY_FRAME)) or (cm.*.error_resilient_mode != 0)) or (cm.*.reset_frame_context == @as(c_int, 3))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < (@as(c_int, 1) << @intCast(@as(c_int, 2)))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.frame_contexts[@bitCast(@as(isize, @intCast(i)))] = cm.*.fc.*;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (cm.*.reset_frame_context == @as(c_int, 2)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.frame_contexts[cm.*.frame_context_idx] = cm.*.fc.*;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if ((frame_is_intra_only(cm) != 0) and (cm.*.prev_mip != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.prev_mip)), 0, @as(c_ulong, @bitCast(@as(c_long, cm.*.mi_stride * (cm.*.mi_rows + @as(c_int, 1))))) *% @sizeOf(@TypeOf(cm.*.prev_mip.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&cm.*.ref_frame_sign_bias)), 0, @sizeOf(@TypeOf(cm.*.ref_frame_sign_bias)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.frame_context_idx = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
