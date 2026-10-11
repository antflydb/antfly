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
//
// /*
//  *  Copyright (c) 2012 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const tran_low_t = common.tran_low_t;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
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
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE = common.TX_MODE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const vpx_prob = common.vpx_prob;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const struct_scale_factors = common.struct_scale_factors;
pub const struct_RefBuffer = extern struct {
    idx: c_int = 0,
    buf: [*c]YV12_BUFFER_CONFIG = null,
    sf: struct_scale_factors = @import("std").mem.zeroes(struct_scale_factors),
};
pub const RefBuffer = struct_RefBuffer;
pub const INTERP_FILTER = common.INTERP_FILTER;
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
pub const InternalFrameBufferList = common.InternalFrameBufferList;
pub const struct_BufferPool = extern struct {
    cb_priv: ?*anyopaque = null,
    get_fb_cb: vpx_get_frame_buffer_cb_fn_t = null,
    release_fb_cb: vpx_release_frame_buffer_cb_fn_t = null,
    frame_bufs: [12]RefCntBuffer = @import("std").mem.zeroes([12]RefCntBuffer),
    int_frame_buffers: InternalFrameBufferList = @import("std").mem.zeroes(InternalFrameBufferList),
};
pub const BufferPool = struct_BufferPool;
pub const ENTROPY_CONTEXT = common.ENTROPY_CONTEXT;
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
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
pub const struct_TileInfo = extern struct {
    mi_row_start: c_int = 0,
    mi_row_end: c_int = 0,
    mi_col_start: c_int = 0,
    mi_col_end: c_int = 0,
};
pub const TileInfo = struct_TileInfo;
pub const FRAME_TYPE = common.FRAME_TYPE;
pub const b_mode_info = extern struct {
    as_mode: PREDICTION_MODE = 0,
    as_mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
};
pub fn is_inter_block(arg_mi: [*c]const MODE_INFO) callconv(.c) c_int {
    var mi = arg_mi;
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(@as(c_int, mi.*.ref_frame[@as(c_int, 0)]) > @as(c_int, 0)));
}
pub fn has_second_ref(arg_mi: [*c]const MODE_INFO) callconv(.c) c_int {
    var mi = arg_mi;
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(@as(c_int, mi.*.ref_frame[@as(c_int, 1)]) > @as(c_int, 0)));
}
pub const struct_buf_2d = extern struct {
    buf: [*c]u8 = null,
    stride: c_int = 0,
};
pub const struct_macroblockd_plane = extern struct {
    dqcoeff: [*c]tran_low_t = null,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    dst: struct_buf_2d = @import("std").mem.zeroes(struct_buf_2d),
    pre: [2]struct_buf_2d = @import("std").mem.zeroes([2]struct_buf_2d),
    above_context: [*c]ENTROPY_CONTEXT = null,
    left_context: [*c]ENTROPY_CONTEXT = null,
    seg_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    n4_w: u16 = 0,
    n4_h: u16 = 0,
    n4_wl: u8 = 0,
    n4_hl: u8 = 0,
    dequant: [*c]const i16 = null,
    eob: [*c]c_int = null,
};
pub const struct_macroblockd = extern struct {
    plane: [3]struct_macroblockd_plane = @import("std").mem.zeroes([3]struct_macroblockd_plane),
    bmode_blocks_wl: u8 = 0,
    bmode_blocks_hl: u8 = 0,
    counts: [*c]FRAME_COUNTS = null,
    tile: TileInfo = @import("std").mem.zeroes(TileInfo),
    mi_stride: c_int = 0,
    mi: [*c][*c]MODE_INFO = null,
    left_mi: [*c]MODE_INFO = null,
    above_mi: [*c]MODE_INFO = null,
    max_blocks_wide: c_uint = 0,
    max_blocks_high: c_uint = 0,
    partition_probs: [*c][3]vpx_prob = null,
    mb_to_left_edge: c_int = 0,
    mb_to_right_edge: c_int = 0,
    mb_to_top_edge: c_int = 0,
    mb_to_bottom_edge: c_int = 0,
    fc: [*c]FRAME_CONTEXT = null,
    block_refs: [2][*c]const RefBuffer = @import("std").mem.zeroes([2][*c]const RefBuffer),
    cur_buf: [*c]const YV12_BUFFER_CONFIG = null,
    above_context: [3][*c]ENTROPY_CONTEXT = @import("std").mem.zeroes([3][*c]ENTROPY_CONTEXT),
    left_context: [3][16]ENTROPY_CONTEXT = @import("std").mem.zeroes([3][16]ENTROPY_CONTEXT),
    above_seg_context: [*c]PARTITION_CONTEXT = null,
    left_seg_context: [8]PARTITION_CONTEXT = @import("std").mem.zeroes([8]PARTITION_CONTEXT),
    bd: c_int = 0,
    lossless: c_int = 0,
    corrupted: c_int = 0,
    error_info: [*c]struct_vpx_internal_error_info = null,
    partition: [*c]PARTITION_TYPE = null,
};
pub const MACROBLOCKD = struct_macroblockd;
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
pub export fn antfly_vp9_vp9_compound_reference_allowed(arg_cm: [*c]const VP9_COMMON) c_int {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        i = 1;
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (cm.*.ref_frame_sign_bias[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))] != cm.*.ref_frame_sign_bias[@as(c_int, 1)]) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return 1;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    return 0;
}
pub export fn antfly_vp9_vp9_setup_compound_reference_mode(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.ref_frame_sign_bias[@as(c_int, 1)] == cm.*.ref_frame_sign_bias[@as(c_int, 2)]) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.comp_fixed_ref = 3;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.comp_var_ref[@as(c_int, 0)] = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.comp_var_ref[@as(c_int, 1)] = 2;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (cm.*.ref_frame_sign_bias[@as(c_int, 1)] == cm.*.ref_frame_sign_bias[@as(c_int, 3)]) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.comp_fixed_ref = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.comp_var_ref[@as(c_int, 0)] = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.comp_var_ref[@as(c_int, 1)] = 3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.comp_fixed_ref = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.comp_var_ref[@as(c_int, 0)] = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.comp_var_ref[@as(c_int, 1)] = 3;
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
}
pub export fn antfly_vp9_vp9_get_reference_mode_context(arg_cm: [*c]const VP9_COMMON, arg_xd: [*c]const MACROBLOCKD) c_int {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ctx: c_int = undefined;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_mi: [*c]const MODE_INFO = xd.*.above_mi;
    _ = &above_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_mi: [*c]const MODE_INFO = xd.*.left_mi;
    _ = &left_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const has_above: c_int = @as(c_int, @intFromBool(!!(above_mi != null)));
    _ = &has_above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const has_left: c_int = @as(c_int, @intFromBool(!!(left_mi != null)));
    _ = &has_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((has_above != 0) and (has_left != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(has_second_ref(above_mi) != 0) and !(has_second_ref(left_mi) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx = @as(c_int, @intFromBool(@as(c_int, above_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, cm.*.comp_fixed_ref))) ^ @as(c_int, @intFromBool(@as(c_int, left_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, cm.*.comp_fixed_ref)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(has_second_ref(above_mi) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx = @as(c_int, 2) + @as(c_int, @intFromBool((@as(c_int, above_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, cm.*.comp_fixed_ref)) or !(is_inter_block(above_mi) != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(has_second_ref(left_mi) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    ctx = @as(c_int, 2) + @as(c_int, @intFromBool((@as(c_int, left_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, cm.*.comp_fixed_ref)) or !(is_inter_block(left_mi) != 0)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    ctx = 4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((has_above != 0) or (has_left != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var edge_mi: [*c]const MODE_INFO = if (has_above != 0) above_mi else left_mi;
            _ = &edge_mi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(has_second_ref(edge_mi) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx = @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, cm.*.comp_fixed_ref)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx = 3;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ctx;
}
pub export fn antfly_vp9_vp9_get_pred_context_comp_ref_p(arg_cm: [*c]const VP9_COMMON, arg_xd: [*c]const MACROBLOCKD) c_int {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var pred_context: c_int = undefined;
    _ = &pred_context;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_mi: [*c]const MODE_INFO = xd.*.above_mi;
    _ = &above_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_mi: [*c]const MODE_INFO = xd.*.left_mi;
    _ = &left_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_in_image: c_int = @as(c_int, @intFromBool(!!(above_mi != null)));
    _ = &above_in_image;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_in_image: c_int = @as(c_int, @intFromBool(!!(left_mi != null)));
    _ = &left_in_image;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const fix_ref_idx: c_int = cm.*.ref_frame_sign_bias[@bitCast(@as(isize, @intCast(cm.*.comp_fixed_ref)))];
    _ = &fix_ref_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const var_ref_idx: c_int = @as(c_int, @intFromBool(!(fix_ref_idx != 0)));
    _ = &var_ref_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((above_in_image != 0) and (left_in_image != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const above_intra: c_int = @as(c_int, @intFromBool(!(is_inter_block(above_mi) != 0)));
        _ = &above_intra;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const left_intra: c_int = @as(c_int, @intFromBool(!(is_inter_block(left_mi) != 0)));
        _ = &left_intra;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((above_intra != 0) and (left_intra != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pred_context = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if ((above_intra != 0) or (left_intra != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                var edge_mi: [*c]const MODE_INFO = if (above_intra != 0) left_mi else above_mi;
                _ = &edge_mi;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(has_second_ref(edge_mi) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 1) + (@as(c_int, 2) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) != @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)]))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 1) + (@as(c_int, 2) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@bitCast(@as(isize, @intCast(var_ref_idx)))]) != @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)]))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const l_sg: c_int = @as(c_int, @intFromBool(!(has_second_ref(left_mi) != 0)));
                _ = &l_sg;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const a_sg: c_int = @as(c_int, @intFromBool(!(has_second_ref(above_mi) != 0)));
                _ = &a_sg;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const vrfa: MV_REFERENCE_FRAME = @truncate(if (a_sg != 0) @as(c_int, above_mi.*.ref_frame[@as(c_int, 0)]) else @as(c_int, above_mi.*.ref_frame[@bitCast(@as(isize, @intCast(var_ref_idx)))]));
                _ = &vrfa;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const vrfl: MV_REFERENCE_FRAME = @truncate(if (l_sg != 0) @as(c_int, left_mi.*.ref_frame[@as(c_int, 0)]) else @as(c_int, left_mi.*.ref_frame[@bitCast(@as(isize, @intCast(var_ref_idx)))]));
                _ = &vrfl;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if ((@as(c_int, vrfa) == @as(c_int, vrfl)) and (@as(c_int, cm.*.comp_var_ref[@as(c_int, 1)]) == @as(c_int, vrfa))) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = 0;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if ((l_sg != 0) and (a_sg != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (((@as(c_int, vrfa) == @as(c_int, cm.*.comp_fixed_ref)) and (@as(c_int, vrfl) == @as(c_int, cm.*.comp_var_ref[@as(c_int, 0)]))) or ((@as(c_int, vrfl) == @as(c_int, cm.*.comp_fixed_ref)) and (@as(c_int, vrfa) == @as(c_int, cm.*.comp_var_ref[@as(c_int, 0)])))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            pred_context = 4;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (@as(c_int, vrfa) == @as(c_int, vrfl)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = 3;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = 1;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if ((l_sg != 0) or (a_sg != 0)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            const vrfc: MV_REFERENCE_FRAME = @truncate(if (l_sg != 0) @as(c_int, vrfa) else @as(c_int, vrfl));
                            _ = &vrfc;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            const rfs: MV_REFERENCE_FRAME = @truncate(if (a_sg != 0) @as(c_int, vrfa) else @as(c_int, vrfl));
                            _ = &rfs;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if ((@as(c_int, vrfc) == @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)])) and (@as(c_int, rfs) != @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)]))) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = 1;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                if ((@as(c_int, rfs) == @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)])) and (@as(c_int, vrfc) != @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)]))) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                    pred_context = 2;
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                    pred_context = 4;
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (@as(c_int, vrfa) == @as(c_int, vrfl)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = 4;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = 2;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((above_in_image != 0) or (left_in_image != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var edge_mi: [*c]const MODE_INFO = if (above_in_image != 0) above_mi else left_mi;
            _ = &edge_mi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(is_inter_block(edge_mi) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                pred_context = 2;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (has_second_ref(edge_mi) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 4) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@bitCast(@as(isize, @intCast(var_ref_idx)))]) != @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 3) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) != @as(c_int, cm.*.comp_var_ref[@as(c_int, 1)])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pred_context = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return pred_context;
}
pub export fn antfly_vp9_vp9_get_pred_context_single_ref_p1(arg_xd: [*c]const MACROBLOCKD) c_int {
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var pred_context: c_int = undefined;
    _ = &pred_context;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_mi: [*c]const MODE_INFO = xd.*.above_mi;
    _ = &above_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_mi: [*c]const MODE_INFO = xd.*.left_mi;
    _ = &left_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const has_above: c_int = @as(c_int, @intFromBool(!!(above_mi != null)));
    _ = &has_above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const has_left: c_int = @as(c_int, @intFromBool(!!(left_mi != null)));
    _ = &has_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((has_above != 0) and (has_left != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const above_intra: c_int = @as(c_int, @intFromBool(!(is_inter_block(above_mi) != 0)));
        _ = &above_intra;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const left_intra: c_int = @as(c_int, @intFromBool(!(is_inter_block(left_mi) != 0)));
        _ = &left_intra;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((above_intra != 0) and (left_intra != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pred_context = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if ((above_intra != 0) or (left_intra != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                var edge_mi: [*c]const MODE_INFO = if (above_intra != 0) left_mi else above_mi;
                _ = &edge_mi;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(has_second_ref(edge_mi) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 4) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 1)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 1) + @as(c_int, @intFromBool((@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 1)) or (@as(c_int, edge_mi.*.ref_frame[@as(c_int, 1)]) == @as(c_int, 1))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const above_has_second: c_int = has_second_ref(above_mi);
                _ = &above_has_second;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const left_has_second: c_int = has_second_ref(left_mi);
                _ = &left_has_second;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const above0: MV_REFERENCE_FRAME = above_mi.*.ref_frame[@as(c_int, 0)];
                _ = &above0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const above1: MV_REFERENCE_FRAME = above_mi.*.ref_frame[@as(c_int, 1)];
                _ = &above1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const left0: MV_REFERENCE_FRAME = left_mi.*.ref_frame[@as(c_int, 0)];
                _ = &left0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const left1: MV_REFERENCE_FRAME = left_mi.*.ref_frame[@as(c_int, 1)];
                _ = &left1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if ((above_has_second != 0) and (left_has_second != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 1) + @as(c_int, @intFromBool((((@as(c_int, above0) == @as(c_int, 1)) or (@as(c_int, above1) == @as(c_int, 1))) or (@as(c_int, left0) == @as(c_int, 1))) or (@as(c_int, left1) == @as(c_int, 1))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if ((above_has_second != 0) or (left_has_second != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const rfs: MV_REFERENCE_FRAME = @truncate(if (!(above_has_second != 0)) @as(c_int, above0) else @as(c_int, left0));
                        _ = &rfs;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const crf1: MV_REFERENCE_FRAME = @truncate(if (above_has_second != 0) @as(c_int, above0) else @as(c_int, left0));
                        _ = &crf1;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const crf2: MV_REFERENCE_FRAME = @truncate(if (above_has_second != 0) @as(c_int, above1) else @as(c_int, left1));
                        _ = &crf2;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (@as(c_int, rfs) == @as(c_int, 1)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            pred_context = @as(c_int, 3) + @as(c_int, @intFromBool((@as(c_int, crf1) == @as(c_int, 1)) or (@as(c_int, crf2) == @as(c_int, 1))));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            pred_context = @as(c_int, @intFromBool((@as(c_int, crf1) == @as(c_int, 1)) or (@as(c_int, crf2) == @as(c_int, 1))));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        pred_context = (@as(c_int, 2) * @as(c_int, @intFromBool(@as(c_int, above0) == @as(c_int, 1)))) + (@as(c_int, 2) * @as(c_int, @intFromBool(@as(c_int, left0) == @as(c_int, 1))));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((has_above != 0) or (has_left != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var edge_mi: [*c]const MODE_INFO = if (has_above != 0) above_mi else left_mi;
            _ = &edge_mi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(is_inter_block(edge_mi) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                pred_context = 2;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(has_second_ref(edge_mi) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 4) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 1)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 1) + @as(c_int, @intFromBool((@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 1)) or (@as(c_int, edge_mi.*.ref_frame[@as(c_int, 1)]) == @as(c_int, 1))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pred_context = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return pred_context;
}
pub export fn antfly_vp9_vp9_get_pred_context_single_ref_p2(arg_xd: [*c]const MACROBLOCKD) c_int {
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var pred_context: c_int = undefined;
    _ = &pred_context;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_mi: [*c]const MODE_INFO = xd.*.above_mi;
    _ = &above_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_mi: [*c]const MODE_INFO = xd.*.left_mi;
    _ = &left_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const has_above: c_int = @as(c_int, @intFromBool(!!(above_mi != null)));
    _ = &has_above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const has_left: c_int = @as(c_int, @intFromBool(!!(left_mi != null)));
    _ = &has_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((has_above != 0) and (has_left != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const above_intra: c_int = @as(c_int, @intFromBool(!(is_inter_block(above_mi) != 0)));
        _ = &above_intra;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const left_intra: c_int = @as(c_int, @intFromBool(!(is_inter_block(left_mi) != 0)));
        _ = &left_intra;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((above_intra != 0) and (left_intra != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pred_context = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if ((above_intra != 0) or (left_intra != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                var edge_mi: [*c]const MODE_INFO = if (above_intra != 0) left_mi else above_mi;
                _ = &edge_mi;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(has_second_ref(edge_mi) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 1)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        pred_context = 3;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        pred_context = @as(c_int, 4) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 2)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 1) + (@as(c_int, 2) * @as(c_int, @intFromBool((@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 2)) or (@as(c_int, edge_mi.*.ref_frame[@as(c_int, 1)]) == @as(c_int, 2)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const above_has_second: c_int = has_second_ref(above_mi);
                _ = &above_has_second;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const left_has_second: c_int = has_second_ref(left_mi);
                _ = &left_has_second;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const above0: MV_REFERENCE_FRAME = above_mi.*.ref_frame[@as(c_int, 0)];
                _ = &above0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const above1: MV_REFERENCE_FRAME = above_mi.*.ref_frame[@as(c_int, 1)];
                _ = &above1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const left0: MV_REFERENCE_FRAME = left_mi.*.ref_frame[@as(c_int, 0)];
                _ = &left0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const left1: MV_REFERENCE_FRAME = left_mi.*.ref_frame[@as(c_int, 1)];
                _ = &left1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if ((above_has_second != 0) and (left_has_second != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if ((@as(c_int, above0) == @as(c_int, left0)) and (@as(c_int, above1) == @as(c_int, left1))) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        pred_context = @as(c_int, 3) * @as(c_int, @intFromBool((((@as(c_int, above0) == @as(c_int, 2)) or (@as(c_int, above1) == @as(c_int, 2))) or (@as(c_int, left0) == @as(c_int, 2))) or (@as(c_int, left1) == @as(c_int, 2))));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        pred_context = 2;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if ((above_has_second != 0) or (left_has_second != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const rfs: MV_REFERENCE_FRAME = @truncate(if (!(above_has_second != 0)) @as(c_int, above0) else @as(c_int, left0));
                        _ = &rfs;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const crf1: MV_REFERENCE_FRAME = @truncate(if (above_has_second != 0) @as(c_int, above0) else @as(c_int, left0));
                        _ = &crf1;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const crf2: MV_REFERENCE_FRAME = @truncate(if (above_has_second != 0) @as(c_int, above1) else @as(c_int, left1));
                        _ = &crf2;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (@as(c_int, rfs) == @as(c_int, 2)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            pred_context = @as(c_int, 3) + @as(c_int, @intFromBool((@as(c_int, crf1) == @as(c_int, 2)) or (@as(c_int, crf2) == @as(c_int, 2))));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (@as(c_int, rfs) == @as(c_int, 3)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = @as(c_int, @intFromBool((@as(c_int, crf1) == @as(c_int, 2)) or (@as(c_int, crf2) == @as(c_int, 2))));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = @as(c_int, 1) + (@as(c_int, 2) * @as(c_int, @intFromBool((@as(c_int, crf1) == @as(c_int, 2)) or (@as(c_int, crf2) == @as(c_int, 2)))));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if ((@as(c_int, above0) == @as(c_int, 1)) and (@as(c_int, left0) == @as(c_int, 1))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            pred_context = 3;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if ((@as(c_int, above0) == @as(c_int, 1)) or (@as(c_int, left0) == @as(c_int, 1))) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                const edge0: MV_REFERENCE_FRAME = @truncate(if (@as(c_int, above0) == @as(c_int, 1)) @as(c_int, left0) else @as(c_int, above0));
                                _ = &edge0;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = @as(c_int, 4) * @as(c_int, @intFromBool(@as(c_int, edge0) == @as(c_int, 2)));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                pred_context = (@as(c_int, 2) * @as(c_int, @intFromBool(@as(c_int, above0) == @as(c_int, 2)))) + (@as(c_int, 2) * @as(c_int, @intFromBool(@as(c_int, left0) == @as(c_int, 2))));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((has_above != 0) or (has_left != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var edge_mi: [*c]const MODE_INFO = if (has_above != 0) above_mi else left_mi;
            _ = &edge_mi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(is_inter_block(edge_mi) != 0) or ((@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 1)) and !(has_second_ref(edge_mi) != 0))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                pred_context = 2;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(has_second_ref(edge_mi) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 4) * @as(c_int, @intFromBool(@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 2)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    pred_context = @as(c_int, 3) * @as(c_int, @intFromBool((@as(c_int, edge_mi.*.ref_frame[@as(c_int, 0)]) == @as(c_int, 2)) or (@as(c_int, edge_mi.*.ref_frame[@as(c_int, 1)]) == @as(c_int, 2))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pred_context = 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return pred_context;
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
