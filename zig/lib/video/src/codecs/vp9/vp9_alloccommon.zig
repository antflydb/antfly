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
pub extern fn antfly_codec_afz_trap() void;
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_vp9_vpx_calloc(num: usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
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
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const TX_SIZE = common.TX_SIZE;
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
pub const vpx_prob = common.vpx_prob;
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
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
pub const InternalFrameBufferList = common.InternalFrameBufferList;
pub const struct_BufferPool = extern struct {
    cb_priv: ?*anyopaque = null,
    get_fb_cb: vpx_get_frame_buffer_cb_fn_t = null,
    release_fb_cb: vpx_release_frame_buffer_cb_fn_t = null,
    frame_bufs: [12]RefCntBuffer = @import("std").mem.zeroes([12]RefCntBuffer),
    int_frame_buffers: InternalFrameBufferList = @import("std").mem.zeroes(InternalFrameBufferList),
};
pub const BufferPool = struct_BufferPool;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
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
pub extern fn antfly_vp9_vpx_free_frame_buffer(ybf: [*c]YV12_BUFFER_CONFIG) c_int;
pub const TX_MODE = common.TX_MODE;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
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
pub fn mi_cols_aligned_to_sb(arg_n_mis: c_int) callconv(.c) c_int {
    var n_mis = arg_n_mis;
    _ = &n_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return (n_mis + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1));
}
pub fn calc_mi_size(arg_len: c_int) callconv(.c) c_int {
    var len = arg_len;
    _ = &len;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return len + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)));
}
pub export fn antfly_vp9_vp9_set_mi_size(arg_mi_rows: [*c]c_int, arg_mi_cols: [*c]c_int, arg_mi_stride: [*c]c_int, arg_width: c_int, arg_height: c_int) void {
    var mi_rows = arg_mi_rows;
    _ = &mi_rows;
    var mi_cols = arg_mi_cols;
    _ = &mi_cols;
    var mi_stride = arg_mi_stride;
    _ = &mi_stride;
    var width = arg_width;
    _ = &width;
    var height = arg_height;
    _ = &height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const aligned_width: c_int = (width + ((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1));
    _ = &aligned_width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const aligned_height: c_int = (height + ((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1));
    _ = &aligned_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi_cols.* = aligned_width >> @intCast(@as(c_int, 3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi_rows.* = aligned_height >> @intCast(@as(c_int, 3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi_stride.* = calc_mi_size(mi_cols.*);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_set_mb_size(arg_mb_rows: [*c]c_int, arg_mb_cols: [*c]c_int, arg_mb_num: [*c]c_int, arg_mi_rows: c_int, arg_mi_cols: c_int) void {
    var mb_rows = arg_mb_rows;
    _ = &mb_rows;
    var mb_cols = arg_mb_cols;
    _ = &mb_cols;
    var mb_num = arg_mb_num;
    _ = &mb_num;
    var mi_rows = arg_mi_rows;
    _ = &mi_rows;
    var mi_cols = arg_mi_cols;
    _ = &mi_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_cols.* = (mi_cols + @as(c_int, 1)) >> @intCast(@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_rows.* = (mi_rows + @as(c_int, 1)) >> @intCast(@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_num.* = mb_rows.* * mb_cols.*;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_set_mb_mi(arg_cm: [*c]VP9_COMMON, arg_width: c_int, arg_height: c_int) void {
    var cm = arg_cm;
    _ = &cm;
    var width = arg_width;
    _ = &width;
    var height = arg_height;
    _ = &height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_set_mi_size(&cm.*.mi_rows, &cm.*.mi_cols, &cm.*.mi_stride, width, height);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_set_mb_size(&cm.*.mb_rows, &cm.*.mb_cols, &cm.*.MBs, cm.*.mi_rows, cm.*.mi_cols);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn alloc_seg_map(arg_cm: [*c]VP9_COMMON, arg_seg_map_size: c_int) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var seg_map_size = arg_seg_map_size;
    _ = &seg_map_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            cm.*.seg_map_array[@bitCast(@as(isize, @intCast(i)))] = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, seg_map_size)), 1)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (@as(?*anyopaque, @ptrCast(@alignCast(cm.*.seg_map_array[@bitCast(@as(isize, @intCast(i)))]))) == @as(?*anyopaque, null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return 1;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.seg_map_alloc_size = seg_map_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.seg_map_idx = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.prev_seg_map_idx = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.current_frame_seg_map = cm.*.seg_map_array[@bitCast(@as(isize, @intCast(cm.*.seg_map_idx)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.last_frame_seg_map = cm.*.seg_map_array[@bitCast(@as(isize, @intCast(cm.*.prev_seg_map_idx)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 0;
}
pub fn free_seg_map(arg_cm: [*c]VP9_COMMON) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.seg_map_array[@bitCast(@as(isize, @intCast(i)))])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.seg_map_array[@bitCast(@as(isize, @intCast(i)))] = null;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.seg_map_alloc_size = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.current_frame_seg_map = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.last_frame_seg_map = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_free_ref_frame_buffers(arg_pool: [*c]BufferPool) void {
    var pool = arg_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(pool != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    {
        i = 0;
        while (i < (((@as(c_int, 1) << @intCast(@as(c_int, 3))) + @as(c_int, 1)) + @as(c_int, 3))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(@as(c_int, pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].released) != 0) and (@as(?*anyopaque, @ptrCast(@alignCast(pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].raw_frame_buffer.data))) != @as(?*anyopaque, null))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = pool.*.release_fb_cb.?(pool.*.cb_priv, &pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].raw_frame_buffer);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].ref_count = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].released = 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_free(@ptrCast(@alignCast(pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].mvs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].mvs = null;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_vp9_vpx_free_frame_buffer(&pool.*.frame_bufs[@bitCast(@as(isize, @intCast(i)))].buf);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_free_context_buffers(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.free_mi != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.free_mi.?(cm);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    free_seg_map(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.above_context)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.above_context = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.above_seg_context)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.above_seg_context = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.above_context_alloc_cols = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.lf.lfm)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.lf.lfm = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_alloc_loop_filter(arg_cm: [*c]VP9_COMMON) c_int {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.lf.lfm)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.lf.lfm_stride = (cm.*.mi_cols + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))) >> @intCast(@as(c_int, 3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.lf.lfm = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, ((cm.*.mi_rows + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))) >> @intCast(@as(c_int, 3))) * cm.*.lf.lfm_stride)), @sizeOf(@TypeOf(cm.*.lf.lfm.*)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(cm.*.lf.lfm != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    }
    return 0;
}
pub export fn antfly_vp9_vp9_alloc_context_buffers(arg_cm: [*c]VP9_COMMON, arg_width: c_int, arg_height: c_int) c_int {
    var cm = arg_cm;
    _ = &cm;
    var width = arg_width;
    _ = &width;
    var height = arg_height;
    _ = &height;
    var afz_new_mi_size_0: c_int = undefined;
    _ = &afz_new_mi_size_0;
    var afz_pc: c_uint = 95;
    _ = &afz_pc;
    while (true) {
        if (afz_pc == @as(c_uint, 0)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 1)) {
            return 1;
        }
        if (afz_pc == @as(c_uint, 2)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 3)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 2) else @as(c_int, 1)));
            continue;
        }
        if (afz_pc == @as(c_uint, 4)) {
            antfly_vp9_vp9_free_context_buffers(cm);
            afz_pc = 3;
            continue;
        }
        if (afz_pc == @as(c_uint, 5)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 6)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 5) else @as(c_int, 4)));
            continue;
        }
        if (afz_pc == @as(c_uint, 7)) {
            antfly_vp9_vp9_set_mb_mi(cm, 0, 0);
            afz_pc = 6;
            continue;
        }
        if (afz_pc == @as(c_uint, 8)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 9)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 10)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 9) else @as(c_int, 8)));
            continue;
        }
        if (afz_pc == @as(c_uint, 11)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 12)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 11) else @as(c_int, 10)));
            continue;
        }
        if (afz_pc == @as(c_uint, 13)) {
            afz_pc = 7;
            continue;
        }
        if (afz_pc == @as(c_uint, 14)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 15)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 14) else @as(c_int, 13)));
            continue;
        }
        if (afz_pc == @as(c_uint, 16)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_vp9_vp9_alloc_loop_filter(cm) != 0) @as(c_int, 15) else @as(c_int, 8)));
            continue;
        }
        if (afz_pc == @as(c_uint, 17)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 18)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 17) else @as(c_int, 16)));
            continue;
        }
        if (afz_pc == @as(c_uint, 19)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 20)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 19) else @as(c_int, 18)));
            continue;
        }
        if (afz_pc == @as(c_uint, 21)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 22)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 21) else @as(c_int, 20)));
            continue;
        }
        if (afz_pc == @as(c_uint, 23)) {
            afz_pc = 7;
            continue;
        }
        if (afz_pc == @as(c_uint, 24)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 25)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 24) else @as(c_int, 23)));
            continue;
        }
        if (afz_pc == @as(c_uint, 26)) {
            afz_pc = @bitCast(@as(c_int, if (alloc_seg_map(cm, cm.*.mi_rows * cm.*.mi_cols) != 0) @as(c_int, 25) else @as(c_int, 18)));
            continue;
        }
        if (afz_pc == @as(c_uint, 27)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 28)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 27) else @as(c_int, 26)));
            continue;
        }
        if (afz_pc == @as(c_uint, 29)) {
            free_seg_map(cm);
            afz_pc = 28;
            continue;
        }
        if (afz_pc == @as(c_uint, 30)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 31)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 30) else @as(c_int, 29)));
            continue;
        }
        if (afz_pc == @as(c_uint, 32)) {
            afz_pc = @bitCast(@as(c_int, if (cm.*.seg_map_alloc_size < (cm.*.mi_rows * cm.*.mi_cols)) @as(c_int, 31) else @as(c_int, 18)));
            continue;
        }
        if (afz_pc == @as(c_uint, 33)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 34)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 33) else @as(c_int, 32)));
            continue;
        }
        if (afz_pc == @as(c_uint, 35)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 36)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 35) else @as(c_int, 34)));
            continue;
        }
        if (afz_pc == @as(c_uint, 37)) {
            cm.*.above_context_alloc_cols = cm.*.mi_cols;
            afz_pc = 36;
            continue;
        }
        if (afz_pc == @as(c_uint, 38)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 39)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 38) else @as(c_int, 37)));
            continue;
        }
        if (afz_pc == @as(c_uint, 40)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 41)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 40) else @as(c_int, 39)));
            continue;
        }
        if (afz_pc == @as(c_uint, 42)) {
            afz_pc = 7;
            continue;
        }
        if (afz_pc == @as(c_uint, 43)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 44)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 43) else @as(c_int, 42)));
            continue;
        }
        if (afz_pc == @as(c_uint, 45)) {
            afz_pc = @bitCast(@as(c_int, if (!(cm.*.above_seg_context != null)) @as(c_int, 44) else @as(c_int, 37)));
            continue;
        }
        if (afz_pc == @as(c_uint, 46)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 47)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 46) else @as(c_int, 45)));
            continue;
        }
        if (afz_pc == @as(c_uint, 48)) {
            cm.*.above_seg_context = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, mi_cols_aligned_to_sb(cm.*.mi_cols))), @sizeOf(@TypeOf(cm.*.above_seg_context.*)))));
            afz_pc = 47;
            continue;
        }
        if (afz_pc == @as(c_uint, 49)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 50)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 49) else @as(c_int, 48)));
            continue;
        }
        if (afz_pc == @as(c_uint, 51)) {
            antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.above_seg_context)));
            afz_pc = 50;
            continue;
        }
        if (afz_pc == @as(c_uint, 52)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 53)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 52) else @as(c_int, 51)));
            continue;
        }
        if (afz_pc == @as(c_uint, 54)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 55)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 54) else @as(c_int, 53)));
            continue;
        }
        if (afz_pc == @as(c_uint, 56)) {
            afz_pc = 7;
            continue;
        }
        if (afz_pc == @as(c_uint, 57)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 58)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 57) else @as(c_int, 56)));
            continue;
        }
        if (afz_pc == @as(c_uint, 59)) {
            afz_pc = @bitCast(@as(c_int, if (!(cm.*.above_context != null)) @as(c_int, 58) else @as(c_int, 51)));
            continue;
        }
        if (afz_pc == @as(c_uint, 60)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 61)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 60) else @as(c_int, 59)));
            continue;
        }
        if (afz_pc == @as(c_uint, 62)) {
            cm.*.above_context = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, (@as(c_int, 2) * mi_cols_aligned_to_sb(cm.*.mi_cols)) * @as(c_int, 3))), @sizeOf(@TypeOf(cm.*.above_context.*)))));
            afz_pc = 61;
            continue;
        }
        if (afz_pc == @as(c_uint, 63)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 64)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 63) else @as(c_int, 62)));
            continue;
        }
        if (afz_pc == @as(c_uint, 65)) {
            antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.above_context)));
            afz_pc = 64;
            continue;
        }
        if (afz_pc == @as(c_uint, 66)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 67)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 66) else @as(c_int, 65)));
            continue;
        }
        if (afz_pc == @as(c_uint, 68)) {
            afz_pc = @bitCast(@as(c_int, if (cm.*.above_context_alloc_cols < cm.*.mi_cols) @as(c_int, 67) else @as(c_int, 34)));
            continue;
        }
        if (afz_pc == @as(c_uint, 69)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 70)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 69) else @as(c_int, 68)));
            continue;
        }
        if (afz_pc == @as(c_uint, 71)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 72)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 71) else @as(c_int, 70)));
            continue;
        }
        if (afz_pc == @as(c_uint, 73)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 74)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 73) else @as(c_int, 72)));
            continue;
        }
        if (afz_pc == @as(c_uint, 75)) {
            afz_pc = 7;
            continue;
        }
        if (afz_pc == @as(c_uint, 76)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 77)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 76) else @as(c_int, 75)));
            continue;
        }
        if (afz_pc == @as(c_uint, 78)) {
            afz_pc = @bitCast(@as(c_int, if (cm.*.alloc_mi.?(cm, afz_new_mi_size_0) != 0) @as(c_int, 77) else @as(c_int, 70)));
            continue;
        }
        if (afz_pc == @as(c_uint, 79)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 80)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 79) else @as(c_int, 78)));
            continue;
        }
        if (afz_pc == @as(c_uint, 81)) {
            cm.*.free_mi.?(cm);
            afz_pc = 80;
            continue;
        }
        if (afz_pc == @as(c_uint, 82)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 83)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 82) else @as(c_int, 81)));
            continue;
        }
        if (afz_pc == @as(c_uint, 84)) {
            afz_pc = @bitCast(@as(c_int, if (cm.*.mi_alloc_size < afz_new_mi_size_0) @as(c_int, 83) else @as(c_int, 70)));
            continue;
        }
        if (afz_pc == @as(c_uint, 85)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 86)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 85) else @as(c_int, 84)));
            continue;
        }
        if (afz_pc == @as(c_uint, 87)) {
            afz_new_mi_size_0 = cm.*.mi_stride * calc_mi_size(cm.*.mi_rows);
            afz_pc = 86;
            continue;
        }
        if (afz_pc == @as(c_uint, 88)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 89)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 88) else @as(c_int, 87)));
            continue;
        }
        if (afz_pc == @as(c_uint, 90)) {
            antfly_vp9_vp9_set_mb_mi(cm, width, height);
            afz_pc = 89;
            continue;
        }
        if (afz_pc == @as(c_uint, 91)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 92)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 91) else @as(c_int, 90)));
            continue;
        }
        if (afz_pc == @as(c_uint, 93)) {
            afz_pc = 92;
            continue;
        }
        if (afz_pc == @as(c_uint, 94)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 95)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 94) else @as(c_int, 93)));
            continue;
        }
        antfly_codec_afz_trap();
    }
    return undefined;
}
pub export fn antfly_vp9_vp9_remove_common(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_free_context_buffers(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.fc)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.fc = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.frame_contexts)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.frame_contexts = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_init_context_buffers(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.setup_mi.?(cm);
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
}
pub export fn antfly_vp9_vp9_swap_current_and_last_seg_map(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tmp: c_int = cm.*.seg_map_idx;
    _ = &tmp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.seg_map_idx = cm.*.prev_seg_map_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.prev_seg_map_idx = tmp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.current_frame_seg_map = cm.*.seg_map_array[@bitCast(@as(isize, @intCast(cm.*.seg_map_idx)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.last_frame_seg_map = cm.*.seg_map_array[@bitCast(@as(isize, @intCast(cm.*.prev_seg_map_idx)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
