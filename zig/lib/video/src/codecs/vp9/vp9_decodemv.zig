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
//   Copyright (c) 2010 The WebM project authors. All Rights Reserved.
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
pub extern fn antfly_codec_afz_vp9_fail(?*anyopaque, c_int) void;
pub const tran_low_t = common.tran_low_t;
pub fn clamp(arg_value: c_int, arg_low: c_int, arg_high: c_int) callconv(.c) c_int {
    var value = arg_value;
    _ = &value;
    var low = arg_low;
    _ = &low;
    var high = arg_high;
    _ = &high;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (value < low) low else if (value > high) high else value;
}
pub extern fn antfly_codec_abs(c_int) c_int;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const vpx_prob = common.vpx_prob;
pub const vpx_tree_index = common.vpx_tree_index;
pub extern const antfly_vp9_vpx_norm: [256]u8 align(16);
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE_SELECT = common.TX_MODE_SELECT;
pub const TX_MODE = common.TX_MODE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
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
pub fn clamp_mv(arg_mv_1: [*c]MV, arg_min_col: c_int, arg_max_col: c_int, arg_min_row: c_int, arg_max_row: c_int) callconv(.c) void {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var min_col = arg_min_col;
    _ = &min_col;
    var max_col = arg_max_col;
    _ = &max_col;
    var min_row = arg_min_row;
    _ = &min_row;
    var max_row = arg_max_row;
    _ = &max_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mv_1.*.col = @truncate(clamp(mv_1.*.col, min_col, max_col));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mv_1.*.row = @truncate(clamp(mv_1.*.row, min_row, max_row));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn use_mv_hp(arg_ref: [*c]const MV) callconv(.c) c_int {
    var ref = arg_ref;
    _ = &ref;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const kMvRefThresh: c_int = 64;
    _ = &kMvRefThresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((antfly_codec_abs(ref.*.row) < kMvRefThresh) and (antfly_codec_abs(ref.*.col) < kMvRefThresh)));
}
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
pub const antfly_vp9_vp9_mv_joint_tree = common.antfly_vp9_vp9_mv_joint_tree;
pub const antfly_vp9_vp9_mv_class_tree = common.antfly_vp9_vp9_mv_class_tree;
pub const antfly_vp9_vp9_mv_fp_tree = common.antfly_vp9_vp9_mv_fp_tree;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
pub extern fn antfly_vp9_vp9_inc_mv(mv: [*c]const MV, counts: [*c]nmv_context_counts) void;
pub extern var antfly_vp9_vp9_kf_uv_mode_prob: [10][9]vpx_prob;
pub extern var antfly_vp9_vp9_kf_y_mode_prob: [10][10][9]vpx_prob;
pub extern const antfly_vp9_vp9_intra_mode_tree: [18]vpx_tree_index;
pub extern const antfly_vp9_vp9_inter_mode_tree: [6]vpx_tree_index;
pub extern const antfly_vp9_vp9_switchable_interp_tree: [4]vpx_tree_index;
pub const VPX_CODEC_UNSUP_BITSTREAM = common.VPX_CODEC_UNSUP_BITSTREAM;
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
pub const struct_TileInfo = extern struct {
    mi_row_start: c_int = 0,
    mi_row_end: c_int = 0,
    mi_col_start: c_int = 0,
    mi_col_end: c_int = 0,
};
pub const TileInfo = struct_TileInfo;
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
pub extern const antfly_vp9_size_group_lookup: [13]u8;
pub extern const antfly_vp9_max_txsize_lookup: [13]TX_SIZE;
pub extern const antfly_vp9_tx_mode_to_biggest_tx_size: [5]TX_SIZE;
pub const SEG_LVL_REF_FRAME = common.SEG_LVL_REF_FRAME;
pub const SEG_LVL_SKIP = common.SEG_LVL_SKIP;
pub const SEG_LVL_FEATURES = common.SEG_LVL_FEATURES;
pub fn segfeature_active(arg_seg: [*c]const struct_segmentation, arg_segment_id: c_int, arg_feature_id: SEG_LVL_FEATURES) callconv(.c) c_int {
    var seg = arg_seg;
    _ = &seg;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var feature_id = arg_feature_id;
    _ = &feature_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((@as(u32, @bitCast(@as(c_int, @as(c_int, seg.*.enabled)))) != 0) and ((seg.*.feature_mask[@bitCast(@as(isize, @intCast(segment_id)))] & @as(u32, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @bitCast(feature_id))))))) != 0)));
}
pub fn get_segdata(arg_seg: [*c]const struct_segmentation, arg_segment_id: c_int, arg_feature_id: SEG_LVL_FEATURES) callconv(.c) c_int {
    var seg = arg_seg;
    _ = &seg;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var feature_id = arg_feature_id;
    _ = &feature_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return seg.*.feature_data[@bitCast(@as(isize, @intCast(segment_id)))][feature_id];
}
pub extern const antfly_vp9_vp9_segment_tree: [14]vpx_tree_index;
pub const KEY_FRAME = common.KEY_FRAME;
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
pub extern fn antfly_vp9_vp9_left_block_mode(cur_mi: [*c]const MODE_INFO, left_mi: [*c]const MODE_INFO, b: c_int) PREDICTION_MODE;
pub extern fn antfly_vp9_vp9_above_block_mode(cur_mi: [*c]const MODE_INFO, above_mi: [*c]const MODE_INFO, b: c_int) PREDICTION_MODE;
pub const MACROBLOCKD = struct_macroblockd;
pub fn get_y_mode_probs(arg_mi: [*c]const MODE_INFO, arg_above_mi: [*c]const MODE_INFO, arg_left_mi: [*c]const MODE_INFO, arg_block: c_int) callconv(.c) [*c]const vpx_prob {
    var mi = arg_mi;
    _ = &mi;
    var above_mi = arg_above_mi;
    _ = &above_mi;
    var left_mi = arg_left_mi;
    _ = &left_mi;
    var block = arg_block;
    _ = &block;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const above: PREDICTION_MODE = antfly_vp9_vp9_above_block_mode(mi, above_mi, block);
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const left: PREDICTION_MODE = antfly_vp9_vp9_left_block_mode(mi, left_mi, block);
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return @ptrCast(@alignCast(&antfly_vp9_vp9_kf_y_mode_prob[above][left]));
}
pub const loop_filter_thresh = common.loop_filter_thresh;
pub const loop_filter_info_n = extern struct {
    lfthr: [64]loop_filter_thresh = @import("std").mem.zeroes([64]loop_filter_thresh),
    lvl: [8][4][2]u8 = @import("std").mem.zeroes([8][4][2]u8),
};
pub const LOOP_FILTER_MASK = common.LOOP_FILTER_MASK;
pub const struct_LoopFilterWorkerData = extern struct {
    frame_buffer: [*c]YV12_BUFFER_CONFIG = null,
    cm: [*c]struct_VP9Common = null,
    planes: [3]struct_macroblockd_plane = @import("std").mem.zeroes([3]struct_macroblockd_plane),
    start: c_int = 0,
    stop: c_int = 0,
    y_only: c_int = 0,
};
pub const LFWorkerData = struct_LoopFilterWorkerData;
pub const struct_VP9LfSyncData = extern struct {
    cur_sb_col: [*c]c_int = null,
    sync_range: c_int = 0,
    rows: c_int = 0,
    lfdata: [*c]LFWorkerData = null,
    num_workers: c_int = 0,
    num_active_workers: c_int = 0,
    num_tiles_done: [*c]c_int = null,
    corrupted: c_int = 0,
};
pub const SINGLE_REFERENCE = common.SINGLE_REFERENCE;
pub const COMPOUND_REFERENCE = common.COMPOUND_REFERENCE;
pub const REFERENCE_MODE_SELECT = common.REFERENCE_MODE_SELECT;
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
pub const struct_position = extern struct {
    row: c_int = 0,
    col: c_int = 0,
};
pub const POSITION = struct_position;
pub const BOTH_ZERO: c_int = 0;
pub const ZERO_PLUS_PREDICTED: c_int = 1;
pub const BOTH_PREDICTED: c_int = 2;
pub const NEW_PLUS_NON_INTRA: c_int = 3;
pub const BOTH_NEW: c_int = 4;
pub const INTRA_PLUS_NON_INTRA: c_int = 5;
pub const BOTH_INTRA: c_int = 6;
pub const INVALID_CASE: c_int = 9;
pub const mode_2_counter: [14]c_int = [14]c_int{
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    0,
    0,
    3,
    1,
};
pub const counter_to_context: [19]c_int = [19]c_int{
    BOTH_PREDICTED,
    NEW_PLUS_NON_INTRA,
    BOTH_NEW,
    ZERO_PLUS_PREDICTED,
    NEW_PLUS_NON_INTRA,
    INVALID_CASE,
    BOTH_ZERO,
    INVALID_CASE,
    INVALID_CASE,
    INTRA_PLUS_NON_INTRA,
    INTRA_PLUS_NON_INTRA,
    INVALID_CASE,
    INTRA_PLUS_NON_INTRA,
    INVALID_CASE,
    INVALID_CASE,
    INVALID_CASE,
    INVALID_CASE,
    INVALID_CASE,
    BOTH_INTRA,
};
pub var mv_ref_blocks: [13][8]POSITION = [13][8]POSITION{
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 2),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 2),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 2),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 2),
        },
    },
    [8]POSITION{
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 1,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = 0,
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 2),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 1,
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 2),
        },
        POSITION{
            .row = -@as(c_int, 2),
            .col = -@as(c_int, 1),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 1,
        },
        POSITION{
            .row = 1,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 3),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = -@as(c_int, 3),
        },
    },
    [8]POSITION{
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 2,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 1,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 3),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = 0,
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = -@as(c_int, 3),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 2,
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = 1,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 3),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = -@as(c_int, 3),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 1,
        },
        POSITION{
            .row = 1,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 2,
        },
        POSITION{
            .row = 2,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 3),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = -@as(c_int, 3),
        },
    },
    [8]POSITION{
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 4,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 2,
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 3),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = 0,
        },
        POSITION{
            .row = 2,
            .col = -@as(c_int, 1),
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 4,
        },
        POSITION{
            .row = 2,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 3),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 3),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 2,
        },
    },
    [8]POSITION{
        POSITION{
            .row = -@as(c_int, 1),
            .col = 3,
        },
        POSITION{
            .row = 3,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 4,
        },
        POSITION{
            .row = 4,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 0,
        },
        POSITION{
            .row = 0,
            .col = -@as(c_int, 1),
        },
        POSITION{
            .row = -@as(c_int, 1),
            .col = 6,
        },
    },
};
pub var idx_n_column_to_subblock: [4][2]c_int = [4][2]c_int{
    [2]c_int{
        1,
        2,
    },
    [2]c_int{
        1,
        3,
    },
    [2]c_int{
        3,
        2,
    },
    [2]c_int{
        3,
        3,
    },
};
pub fn clamp_mv_ref(arg_mv_1: [*c]MV, arg_xd: [*c]const MACROBLOCKD) callconv(.c) void {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    clamp_mv(mv_1, xd.*.mb_to_left_edge - (@as(c_int, 16) << @intCast(@as(c_int, 3))), xd.*.mb_to_right_edge + (@as(c_int, 16) << @intCast(@as(c_int, 3))), xd.*.mb_to_top_edge - (@as(c_int, 16) << @intCast(@as(c_int, 3))), xd.*.mb_to_bottom_edge + (@as(c_int, 16) << @intCast(@as(c_int, 3))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_sub_block_mv(arg_candidate: [*c]const MODE_INFO, arg_which_mv: c_int, arg_search_col: c_int, arg_block_idx: c_int) callconv(.c) int_mv {
    var candidate = arg_candidate;
    _ = &candidate;
    var which_mv = arg_which_mv;
    _ = &which_mv;
    var search_col = arg_search_col;
    _ = &search_col;
    var block_idx = arg_block_idx;
    _ = &block_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return int_mv{
            .as_int = 0,
        };
    }
    return if ((block_idx >= @as(c_int, 0)) and (@as(c_int, candidate.*.sb_type) < @as(c_int, 3))) candidate.*.bmi[@bitCast(@as(isize, @intCast(idx_n_column_to_subblock[@bitCast(@as(isize, @intCast(block_idx)))][@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(search_col == @as(c_int, 0))))))])))].as_mv[@bitCast(@as(isize, @intCast(which_mv)))] else candidate.*.mv[@bitCast(@as(isize, @intCast(which_mv)))];
}
pub fn scale_mv(arg_mi: [*c]const MODE_INFO, arg_ref: c_int, this_ref_frame: MV_REFERENCE_FRAME, arg_ref_sign_bias: [*c]const c_int) callconv(.c) int_mv {
    var mi = arg_mi;
    _ = &mi;
    var ref = arg_ref;
    _ = &ref;
    _ = &this_ref_frame;
    var ref_sign_bias = arg_ref_sign_bias;
    _ = &ref_sign_bias;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return int_mv{
            .as_int = 0,
        };
    }
    var mv_1: int_mv = mi.*.mv[@bitCast(@as(isize, @intCast(ref)))];
    _ = &mv_1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return int_mv{
            .as_int = 0,
        };
    }
    if (ref_sign_bias[@bitCast(@as(isize, @intCast(mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))])))] != ref_sign_bias[@bitCast(@as(isize, @intCast(this_ref_frame)))]) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return int_mv{
                .as_int = 0,
            };
        }
        {
            const ref_1 = &mv_1.as_mv.row;
            ref_1.* = @truncate(@as(c_int, ref_1.*) * -@as(c_int, 1));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return int_mv{
                .as_int = 0,
            };
        }
        {
            const ref_1 = &mv_1.as_mv.col;
            ref_1.* = @truncate(@as(c_int, ref_1.*) * -@as(c_int, 1));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return int_mv{
                .as_int = 0,
            };
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return int_mv{
            .as_int = 0,
        };
    }
    return mv_1;
}
pub fn is_inside(tile: [*c]const TileInfo, arg_mi_col: c_int, arg_mi_row: c_int, arg_mi_rows: c_int, arg_mi_pos: [*c]const POSITION) callconv(.c) c_int {
    _ = &tile;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_rows = arg_mi_rows;
    _ = &mi_rows;
    var mi_pos = arg_mi_pos;
    _ = &mi_pos;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(!(((((mi_row + mi_pos.*.row) < @as(c_int, 0)) or ((mi_col + mi_pos.*.col) < tile.*.mi_col_start)) or ((mi_row + mi_pos.*.row) >= mi_rows)) or ((mi_col + mi_pos.*.col) >= tile.*.mi_col_end))));
}
pub fn lower_mv_precision(arg_mv_1: [*c]MV, arg_allow_hp: c_int) callconv(.c) void {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var allow_hp = arg_allow_hp;
    _ = &allow_hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const use_hp: c_int = @as(c_int, @intFromBool((allow_hp != 0) and (use_mv_hp(mv_1) != 0)));
    _ = &use_hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(use_hp != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((@as(c_int, mv_1.*.row) & @as(c_int, 1)) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mv_1.*.row;
                ref.* = @truncate(@as(c_int, ref.*) + (if (@as(c_int, mv_1.*.row) > @as(c_int, 0)) -@as(c_int, 1) else @as(c_int, 1)));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if ((@as(c_int, mv_1.*.col) & @as(c_int, 1)) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mv_1.*.col;
                ref.* = @truncate(@as(c_int, ref.*) + (if (@as(c_int, mv_1.*.col) > @as(c_int, 0)) -@as(c_int, 1) else @as(c_int, 1)));
            }
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
pub fn vp9_get_pred_context_seg_id(arg_xd: [*c]const MACROBLOCKD) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
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
    const above_sip: c_int = if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(above_mi)))) != @as(?*anyopaque, null)) @as(c_int, above_mi.*.seg_id_predicted) else @as(c_int, 0);
    _ = &above_sip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_sip: c_int = if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(left_mi)))) != @as(?*anyopaque, null)) @as(c_int, left_mi.*.seg_id_predicted) else @as(c_int, 0);
    _ = &left_sip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return above_sip + left_sip;
}
pub fn vp9_get_pred_prob_seg_id(arg_seg: [*c]const struct_segmentation, arg_xd: [*c]const MACROBLOCKD) callconv(.c) vpx_prob {
    var seg = arg_seg;
    _ = &seg;
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return seg.*.pred_probs[@bitCast(@as(isize, @intCast(vp9_get_pred_context_seg_id(xd))))];
}
pub fn vp9_get_skip_context(arg_xd: [*c]const MACROBLOCKD) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
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
    const above_skip: c_int = if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(above_mi)))) != @as(?*anyopaque, null)) @as(c_int, above_mi.*.skip) else @as(c_int, 0);
    _ = &above_skip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_skip: c_int = if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(left_mi)))) != @as(?*anyopaque, null)) @as(c_int, left_mi.*.skip) else @as(c_int, 0);
    _ = &left_skip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return above_skip + left_skip;
}
pub fn get_pred_context_switchable_interp(arg_xd: [*c]const MACROBLOCKD) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_mi: [*c]const MODE_INFO = xd.*.left_mi;
    _ = &left_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_type: c_int = if (left_mi != null) @as(c_int, left_mi.*.interp_filter) else @as(c_int, 3);
    _ = &left_type;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_mi: [*c]const MODE_INFO = xd.*.above_mi;
    _ = &above_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const above_type: c_int = if (above_mi != null) @as(c_int, above_mi.*.interp_filter) else @as(c_int, 3);
    _ = &above_type;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (left_type == above_type) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return left_type;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (left_type == @as(c_int, 3)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return above_type;
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (above_type == @as(c_int, 3)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return left_type;
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return 3;
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
pub fn get_intra_inter_context(arg_xd: [*c]const MACROBLOCKD) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
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
        return if ((left_intra != 0) and (above_intra != 0)) @as(c_int, 3) else @as(c_int, @intFromBool((left_intra != 0) or (above_intra != 0)));
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((has_above != 0) or (has_left != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return @as(c_int, 2) * @as(c_int, @intFromBool(!(is_inter_block(if (has_above != 0) above_mi else left_mi) != 0)));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return 0;
}
pub extern fn antfly_vp9_vp9_get_reference_mode_context(cm: [*c]const VP9_COMMON, xd: [*c]const MACROBLOCKD) c_int;
pub extern fn antfly_vp9_vp9_get_pred_context_comp_ref_p(cm: [*c]const VP9_COMMON, xd: [*c]const MACROBLOCKD) c_int;
pub extern fn antfly_vp9_vp9_get_pred_context_single_ref_p1(xd: [*c]const MACROBLOCKD) c_int;
pub extern fn antfly_vp9_vp9_get_pred_context_single_ref_p2(xd: [*c]const MACROBLOCKD) c_int;
pub fn get_tx_size_context(arg_xd: [*c]const MACROBLOCKD) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const max_tx_size: c_int = antfly_vp9_max_txsize_lookup[xd.*.mi[@as(c_int, 0)].*.sb_type];
    _ = &max_tx_size;
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
    var above_ctx: c_int = if ((has_above != 0) and !(@as(c_int, above_mi.*.skip) != 0)) @as(c_int, above_mi.*.tx_size) else max_tx_size;
    _ = &above_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var left_ctx: c_int = if ((has_left != 0) and !(@as(c_int, left_mi.*.skip) != 0)) @as(c_int, left_mi.*.tx_size) else max_tx_size;
    _ = &left_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(has_left != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        left_ctx = above_ctx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (!(has_above != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        above_ctx = left_ctx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return @as(c_int, @intFromBool((above_ctx + left_ctx) > max_tx_size));
}
pub fn get_tx_probs(arg_max_tx_size: TX_SIZE, arg_ctx: c_int, arg_tx_probs_1: [*c]const struct_tx_probs) callconv(.c) [*c]const vpx_prob {
    var max_tx_size = arg_max_tx_size;
    _ = &max_tx_size;
    var ctx = arg_ctx;
    _ = &ctx;
    var tx_probs_1 = arg_tx_probs_1;
    _ = &tx_probs_1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        switch (@as(c_int, max_tx_size)) {
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                return @ptrCast(@alignCast(&tx_probs_1.*.p8x8[@bitCast(@as(isize, @intCast(ctx)))]));
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                return @ptrCast(@alignCast(&tx_probs_1.*.p16x16[@bitCast(@as(isize, @intCast(ctx)))]));
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                return @ptrCast(@alignCast(&tx_probs_1.*.p32x32[@bitCast(@as(isize, @intCast(ctx)))]));
            },
            else => {
                _ = @as(c_int, 0);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                return null;
            },
        }
    }
}
pub fn get_tx_counts(arg_max_tx_size: TX_SIZE, arg_ctx: c_int, arg_tx_counts_1: [*c]struct_tx_counts) callconv(.c) [*c]c_uint {
    var max_tx_size = arg_max_tx_size;
    _ = &max_tx_size;
    var ctx = arg_ctx;
    _ = &ctx;
    var tx_counts_1 = arg_tx_counts_1;
    _ = &tx_counts_1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        switch (@as(c_int, max_tx_size)) {
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                return @ptrCast(@alignCast(&tx_counts_1.*.p8x8[@bitCast(@as(isize, @intCast(ctx)))]));
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                return @ptrCast(@alignCast(&tx_counts_1.*.p16x16[@bitCast(@as(isize, @intCast(ctx)))]));
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                return @ptrCast(@alignCast(&tx_counts_1.*.p32x32[@bitCast(@as(isize, @intCast(ctx)))]));
            },
            else => {
                _ = @as(c_int, 0);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                return null;
            },
        }
    }
}
pub const vpx_decrypt_cb = common.vpx_decrypt_cb;
pub const BD_VALUE = common.BD_VALUE;
pub const vpx_reader = extern struct {
    value: BD_VALUE = 0,
    range: c_uint = 0,
    count: c_int = 0,
    buffer_end: [*c]const u8 = null,
    buffer: [*c]const u8 = null,
    decrypt_cb: vpx_decrypt_cb = null,
    decrypt_state: ?*anyopaque = null,
    clear_buffer: [9]u8 = @import("std").mem.zeroes([9]u8),
};
pub extern fn antfly_vp9_vpx_reader_fill(r: [*c]vpx_reader) void;
pub fn vpx_read(arg_r: [*c]vpx_reader, arg_prob: c_int) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var prob = arg_prob;
    _ = &prob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bit: c_uint = 0;
    _ = &bit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var value: BD_VALUE = undefined;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bigsplit: BD_VALUE = undefined;
    _ = &bigsplit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var count: c_int = undefined;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var range: c_uint = undefined;
    _ = &range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var split: c_uint = ((r.*.range *% @as(c_uint, @bitCast(@as(c_int, prob)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 256) - prob)))) >> @intCast(@as(c_uint, 8));
    _ = &split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (r.*.count < @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_reader_fill(r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    value = r.*.value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    count = r.*.count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    bigsplit = @as(BD_VALUE, split) << @intCast(@as(BD_VALUE, @bitCast(@as(c_long, (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)) - @as(c_int, 8)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    range = split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (value >= bigsplit) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range = r.*.range -% split;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value = value -% bigsplit;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        bit = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const shift: u8 = antfly_vp9_vpx_norm[@as(u8, @truncate(range))];
        _ = &shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range <<= @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, shift)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value <<= @intCast(@as(BD_VALUE, @bitCast(@as(c_long, @as(c_int, shift)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        count -= shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.value = value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.count = count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.range = range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(c_uint, @truncate(bit)));
}
pub fn vpx_read_tree(arg_r: [*c]vpx_reader, arg_tree: [*c]const vpx_tree_index, arg_probs: [*c]const vpx_prob) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var tree = arg_tree;
    _ = &tree;
    var probs = arg_probs;
    _ = &probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: vpx_tree_index = 0;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (@as(c_int, blk: {
        const tmp = tree[@bitCast(@as(isize, @intCast(@as(c_int, i) + vpx_read(r, probs[@bitCast(@as(isize, @intCast(@as(c_int, i) >> @intCast(@as(c_int, 1)))))]))))];
        i = tmp;
        break :blk tmp;
    }) > @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        continue;
    }
    return -@as(c_int, i);
}
pub const VPxWorkerStatus = common.VPxWorkerStatus;
pub const VPxWorkerHook = common.VPxWorkerHook;
pub const VPxWorkerImpl = common.VPxWorkerImpl;
pub const VPxWorker = extern struct {
    impl_: ?*VPxWorkerImpl = null,
    status_: VPxWorkerStatus = @import("std").mem.zeroes(VPxWorkerStatus),
    thread_name: [*c]const u8 = null,
    hook: VPxWorkerHook = null,
    data1: ?*anyopaque = null,
    data2: ?*anyopaque = null,
    had_error: c_int = 0,
};
pub const VP9LfSync = struct_VP9LfSyncData;
pub const JobQueueRowMt = common.JobQueueRowMt;
pub const struct_TileWorkerData = extern struct {
    data_end: [*c]const u8 align(32) = null,
    buf_start: c_int = 0,
    buf_end: c_int = 0,
    bit_reader: vpx_reader = @import("std").mem.zeroes(vpx_reader),
    counts: FRAME_COUNTS = @import("std").mem.zeroes(FRAME_COUNTS),
    lf_data: [*c]LFWorkerData = null,
    lf_sync: [*c]VP9LfSync = null,
    xd: MACROBLOCKD align(32) = @import("std").mem.zeroes(MACROBLOCKD),
    dqcoeff: [1024]tran_low_t = @import("std").mem.zeroes([1024]tran_low_t),
    extend_and_predict_buf: [25600]u16 = @import("std").mem.zeroes([25600]u16),
    error_info: struct_vpx_internal_error_info = @import("std").mem.zeroes(struct_vpx_internal_error_info),
};
pub const TileWorkerData = struct_TileWorkerData;
pub const TileBuffer = common.TileBuffer;
pub const ThreadData = struct_ThreadData;
pub const struct_RowMTWorkerData = extern struct {
    num_sbs: c_int = 0,
    eob: [3][*c]c_int = @import("std").mem.zeroes([3][*c]c_int),
    partition: [*c]PARTITION_TYPE = null,
    dqcoeff: [3][*c]tran_low_t = @import("std").mem.zeroes([3][*c]tran_low_t),
    recon_map: [*c]i8 = null,
    data_end: [*c]const u8 = null,
    jobq_buf: [*c]u8 = null,
    jobq: JobQueueRowMt = @import("std").mem.zeroes(JobQueueRowMt),
    jobq_size: usize = 0,
    num_tiles_done: c_int = 0,
    num_jobs: c_int = 0,
    thread_data: [*c]ThreadData = null,
};
pub const RowMTWorkerData = struct_RowMTWorkerData;
pub const struct_VP9Decoder = extern struct {
    mb: MACROBLOCKD = @import("std").mem.zeroes(MACROBLOCKD),
    common: VP9_COMMON = @import("std").mem.zeroes(VP9_COMMON),
    ready_for_new_data: c_int = 0,
    refresh_frame_flags: c_int = 0,
    cur_buf: [*c]RefCntBuffer = null,
    lf_worker: VPxWorker = @import("std").mem.zeroes(VPxWorker),
    tile_workers: [*c]VPxWorker = null,
    tile_worker_data: [*c]TileWorkerData = null,
    tile_buffers: [64]TileBuffer = @import("std").mem.zeroes([64]TileBuffer),
    num_tile_workers: c_int = 0,
    total_tiles: c_int = 0,
    lf_row_sync: VP9LfSync = @import("std").mem.zeroes(VP9LfSync),
    decrypt_cb: vpx_decrypt_cb = null,
    decrypt_state: ?*anyopaque = null,
    max_threads: c_int = 0,
    inv_tile_order: c_int = 0,
    need_resync: c_int = 0,
    hold_ref_buf: c_int = 0,
    row_mt: c_int = 0,
    lpf_mt_opt: c_int = 0,
    row_mt_worker_data: [*c]RowMTWorkerData = null,
};
pub const struct_ThreadData = extern struct {
    pbi: [*c]struct_VP9Decoder = null,
    lf_data: [*c]LFWorkerData = null,
    lf_sync: [*c]VP9LfSync = null,
};
pub const VP9Decoder = struct_VP9Decoder;
pub fn read_intra_mode(arg_r: [*c]vpx_reader, arg_p: [*c]const vpx_prob) callconv(.c) PREDICTION_MODE {
    var r = arg_r;
    _ = &r;
    var p = arg_p;
    _ = &p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(i8, @truncate(vpx_read_tree(r, @ptrCast(@alignCast(&antfly_vp9_vp9_intra_mode_tree)), p))));
}
pub fn read_intra_mode_y(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_r: [*c]vpx_reader, arg_size_group: c_int) callconv(.c) PREDICTION_MODE {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var r = arg_r;
    _ = &r;
    var size_group = arg_size_group;
    _ = &size_group;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const y_mode: PREDICTION_MODE = read_intra_mode(r, @ptrCast(@alignCast(&cm.*.fc.*.y_mode_prob[@bitCast(@as(isize, @intCast(size_group)))])));
    _ = &y_mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var counts: [*c]FRAME_COUNTS = xd.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        counts.*.y_mode[@bitCast(@as(isize, @intCast(size_group)))][y_mode] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return y_mode;
}
pub fn read_intra_mode_uv(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_r: [*c]vpx_reader, arg_y_mode: PREDICTION_MODE) callconv(.c) PREDICTION_MODE {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var r = arg_r;
    _ = &r;
    var y_mode = arg_y_mode;
    _ = &y_mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const uv_mode: PREDICTION_MODE = read_intra_mode(r, @ptrCast(@alignCast(&cm.*.fc.*.uv_mode_prob[y_mode])));
    _ = &uv_mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var counts: [*c]FRAME_COUNTS = xd.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        counts.*.uv_mode[y_mode][uv_mode] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return uv_mode;
}
pub fn read_inter_mode(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_r: [*c]vpx_reader, arg_ctx: c_int) callconv(.c) PREDICTION_MODE {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var r = arg_r;
    _ = &r;
    var ctx = arg_ctx;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const mode: c_int = vpx_read_tree(r, @ptrCast(@alignCast(&antfly_vp9_vp9_inter_mode_tree)), @ptrCast(@alignCast(&cm.*.fc.*.inter_mode_probs[@bitCast(@as(isize, @intCast(ctx)))])));
    _ = &mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var counts: [*c]FRAME_COUNTS = xd.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        counts.*.inter_mode[@bitCast(@as(isize, @intCast(ctx)))][@bitCast(@as(isize, @intCast(mode)))] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return @bitCast(@as(i8, @truncate(@as(c_int, 10) + mode)));
}
pub fn read_segment_id(arg_r: [*c]vpx_reader, arg_seg: [*c]const struct_segmentation) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var seg = arg_seg;
    _ = &seg;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return vpx_read_tree(r, @ptrCast(@alignCast(&antfly_vp9_vp9_segment_tree)), @ptrCast(@alignCast(&seg.*.tree_probs)));
}
pub fn read_selected_tx_size(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_max_tx_size: TX_SIZE, arg_r: [*c]vpx_reader) callconv(.c) TX_SIZE {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var max_tx_size = arg_max_tx_size;
    _ = &max_tx_size;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var counts: [*c]FRAME_COUNTS = xd.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const ctx: c_int = get_tx_size_context(xd);
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var tx_probs_1: [*c]const vpx_prob = get_tx_probs(max_tx_size, ctx, &cm.*.fc.*.tx_probs);
    _ = &tx_probs_1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var tx_size: c_int = vpx_read(r, tx_probs_1[@as(c_int, 0)]);
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((tx_size != @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))))) and (@as(c_int, max_tx_size) >= @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        tx_size += vpx_read(r, tx_probs_1[@as(c_int, 1)]);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((tx_size != @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))))) and (@as(c_int, max_tx_size) >= @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            tx_size += vpx_read(r, tx_probs_1[@as(c_int, 2)]);
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
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        get_tx_counts(max_tx_size, ctx, &counts.*.tx)[@bitCast(@as(isize, @intCast(tx_size)))] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return @bitCast(@as(i8, @truncate(tx_size)));
}
pub fn read_tx_size(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_allow_select: c_int, arg_r: [*c]vpx_reader) callconv(.c) TX_SIZE {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var allow_select = arg_allow_select;
    _ = &allow_select;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var tx_mode: TX_MODE = cm.*.tx_mode;
    _ = &tx_mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bsize: BLOCK_SIZE = xd.*.mi[@as(c_int, 0)].*.sb_type;
    _ = &bsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const max_tx_size: TX_SIZE = antfly_vp9_max_txsize_lookup[bsize];
    _ = &max_tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (((allow_select != 0) and (tx_mode == @as(TX_MODE, TX_MODE_SELECT))) and (@as(c_int, bsize) >= @as(c_int, 3))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return read_selected_tx_size(cm, xd, max_tx_size, r);
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(@as(i8, @truncate(if (@as(c_int, max_tx_size) < @as(c_int, antfly_vp9_tx_mode_to_biggest_tx_size[tx_mode])) @as(c_int, max_tx_size) else @as(c_int, antfly_vp9_tx_mode_to_biggest_tx_size[tx_mode]))));
    }
}
pub fn dec_get_segment_id(arg_cm: [*c]const VP9_COMMON, arg_segment_ids: [*c]const u8, arg_mi_offset: c_int, arg_x_mis: c_int, arg_y_mis: c_int) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var segment_ids = arg_segment_ids;
    _ = &segment_ids;
    var mi_offset = arg_mi_offset;
    _ = &mi_offset;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    var segment_id: c_int = 2147483647;
    _ = &segment_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        y = 0;
        while (y < y_mis) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            {
                x = 0;
                while (x < x_mis) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    segment_id = if (segment_id < @as(c_int, segment_ids[@bitCast(@as(isize, @intCast((mi_offset + (y * cm.*.mi_cols)) + x)))])) segment_id else @as(c_int, segment_ids[@bitCast(@as(isize, @intCast((mi_offset + (y * cm.*.mi_cols)) + x)))]);
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
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return segment_id;
}
pub fn set_segment_id(arg_cm: [*c]VP9_COMMON, arg_mi_offset: c_int, arg_x_mis: c_int, arg_y_mis: c_int, arg_segment_id: c_int) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var mi_offset = arg_mi_offset;
    _ = &mi_offset;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < y_mis) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < x_mis) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    cm.*.current_frame_seg_map[@bitCast(@as(isize, @intCast((mi_offset + (y * cm.*.mi_cols)) + x)))] = @bitCast(@as(i8, @truncate(segment_id)));
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
}
pub fn copy_segment_id(arg_cm: [*c]const VP9_COMMON, arg_last_segment_ids: [*c]const u8, arg_current_segment_ids: [*c]u8, arg_mi_offset: c_int, arg_x_mis: c_int, arg_y_mis: c_int) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var last_segment_ids = arg_last_segment_ids;
    _ = &last_segment_ids;
    var current_segment_ids = arg_current_segment_ids;
    _ = &current_segment_ids;
    var mi_offset = arg_mi_offset;
    _ = &mi_offset;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < y_mis) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < x_mis) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    current_segment_ids[@bitCast(@as(isize, @intCast((mi_offset + (y * cm.*.mi_cols)) + x)))] = @bitCast(@as(i8, @truncate(if (last_segment_ids != null) @as(c_int, last_segment_ids[@bitCast(@as(isize, @intCast((mi_offset + (y * cm.*.mi_cols)) + x)))]) else @as(c_int, 0))));
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
}
pub fn read_intra_segment_id(cm: [*c]VP9_COMMON, arg_mi_offset: c_int, arg_x_mis: c_int, arg_y_mis: c_int, arg_r: [*c]vpx_reader) callconv(.c) c_int {
    _ = &cm;
    var mi_offset = arg_mi_offset;
    _ = &mi_offset;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const seg: [*c]struct_segmentation = &cm.*.seg;
    _ = &seg;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var segment_id: c_int = undefined;
    _ = &segment_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(@as(c_int, seg.*.enabled) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (!(@as(c_int, seg.*.update_map) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        copy_segment_id(cm, cm.*.last_frame_seg_map, cm.*.current_frame_seg_map, mi_offset, x_mis, y_mis);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    segment_id = read_segment_id(r, seg);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    set_segment_id(cm, mi_offset, x_mis, y_mis, segment_id);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return segment_id;
}
pub fn read_inter_segment_id(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_mi_row: c_int, arg_mi_col: c_int, arg_r: [*c]vpx_reader, arg_x_mis: c_int, arg_y_mis: c_int) callconv(.c) c_int {
    _ = &cm;
    _ = &xd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var r = arg_r;
    _ = &r;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const seg: [*c]struct_segmentation = &cm.*.seg;
    _ = &seg;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const mi: [*c]MODE_INFO = xd.*.mi[@as(c_int, 0)];
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var predicted_segment_id: c_int = undefined;
    _ = &predicted_segment_id;
    var segment_id: c_int = undefined;
    _ = &segment_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const mi_offset: c_int = (mi_row * cm.*.mi_cols) + mi_col;
    _ = &mi_offset;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(@as(c_int, seg.*.enabled) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    predicted_segment_id = if (cm.*.last_frame_seg_map != null) dec_get_segment_id(cm, cm.*.last_frame_seg_map, mi_offset, x_mis, y_mis) else @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(@as(c_int, seg.*.update_map) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        copy_segment_id(cm, cm.*.last_frame_seg_map, cm.*.current_frame_seg_map, mi_offset, x_mis, y_mis);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return predicted_segment_id;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(c_int, seg.*.temporal_update) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const pred_prob: vpx_prob = vp9_get_pred_prob_seg_id(seg, xd);
        _ = &pred_prob;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        mi.*.seg_id_predicted = @truncate(vpx_read(r, pred_prob));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        segment_id = if (mi.*.seg_id_predicted != 0) predicted_segment_id else read_segment_id(r, seg);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        segment_id = read_segment_id(r, seg);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    set_segment_id(cm, mi_offset, x_mis, y_mis, segment_id);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return segment_id;
}
pub fn read_skip(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]const MACROBLOCKD, arg_segment_id: c_int, arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (segfeature_active(&cm.*.seg, segment_id, SEG_LVL_SKIP) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const ctx: c_int = vp9_get_skip_context(xd);
        _ = &ctx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const skip: c_int = vpx_read(r, cm.*.fc.*.skip_probs[@bitCast(@as(isize, @intCast(ctx)))]);
        _ = &skip;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var counts: [*c]FRAME_COUNTS = xd.*.counts;
        _ = &counts;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (counts != null) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            counts.*.skip[@bitCast(@as(isize, @intCast(ctx)))][@bitCast(@as(isize, @intCast(skip)))] +%= 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        return skip;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub fn read_intra_frame_mode_info(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_mi_row: c_int, arg_mi_col: c_int, arg_r: [*c]vpx_reader, arg_x_mis: c_int, arg_y_mis: c_int) callconv(.c) void {
    _ = &cm;
    _ = &xd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var r = arg_r;
    _ = &r;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi: [*c]MODE_INFO = xd.*.mi[@as(c_int, 0)];
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var above_mi: [*c]const MODE_INFO = xd.*.above_mi;
    _ = &above_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var left_mi: [*c]const MODE_INFO = xd.*.left_mi;
    _ = &left_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bsize: BLOCK_SIZE = mi.*.sb_type;
    _ = &bsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi_offset: c_int = (mi_row * cm.*.mi_cols) + mi_col;
    _ = &mi_offset;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.segment_id = @truncate(read_intra_segment_id(cm, mi_offset, x_mis, y_mis, r));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.skip = @truncate(read_skip(cm, xd, mi.*.segment_id, r));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.tx_size = read_tx_size(cm, xd, 1, r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.ref_frame[@as(c_int, 0)] = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.ref_frame[@as(c_int, 1)] = @truncate(-@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    blk: {
        switch (@as(c_int, bsize)) {
            @as(c_int, 0) => {
                {
                    i = 0;
                    while (i < @as(c_int, 4)) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        mi.*.bmi[@bitCast(@as(isize, @intCast(i)))].as_mode = read_intra_mode(r, get_y_mode_probs(mi, above_mi, left_mi, i));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                mi.*.mode = mi.*.bmi[@as(c_int, 3)].as_mode;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            @as(c_int, 1) => {
                mi.*.bmi[@as(c_int, 0)].as_mode = blk_1: {
                    const tmp = read_intra_mode(r, get_y_mode_probs(mi, above_mi, left_mi, 0));
                    mi.*.bmi[@as(c_int, 2)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mi.*.bmi[@as(c_int, 1)].as_mode = blk_1: {
                    const tmp = blk_2: {
                        const tmp_3 = read_intra_mode(r, get_y_mode_probs(mi, above_mi, left_mi, 1));
                        mi.*.mode = tmp_3;
                        break :blk_2 tmp_3;
                    };
                    mi.*.bmi[@as(c_int, 3)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            @as(c_int, 2) => {
                mi.*.bmi[@as(c_int, 0)].as_mode = blk_1: {
                    const tmp = read_intra_mode(r, get_y_mode_probs(mi, above_mi, left_mi, 0));
                    mi.*.bmi[@as(c_int, 1)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mi.*.bmi[@as(c_int, 2)].as_mode = blk_1: {
                    const tmp = blk_2: {
                        const tmp_3 = read_intra_mode(r, get_y_mode_probs(mi, above_mi, left_mi, 2));
                        mi.*.mode = tmp_3;
                        break :blk_2 tmp_3;
                    };
                    mi.*.bmi[@as(c_int, 3)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            else => {
                mi.*.mode = read_intra_mode(r, get_y_mode_probs(mi, above_mi, left_mi, 0));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            },
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.uv_mode = read_intra_mode(r, @ptrCast(@alignCast(&antfly_vp9_vp9_kf_uv_mode_prob[mi.*.mode])));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_mv_component(arg_r: [*c]vpx_reader, arg_mvcomp: [*c]const nmv_component, arg_usehp: c_int) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var mvcomp = arg_mvcomp;
    _ = &mvcomp;
    var usehp = arg_usehp;
    _ = &usehp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mag: c_int = undefined;
    _ = &mag;
    var d: c_int = undefined;
    _ = &d;
    var fr: c_int = undefined;
    _ = &fr;
    var hp: c_int = undefined;
    _ = &hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const sign: c_int = vpx_read(r, mvcomp.*.sign);
    _ = &sign;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const mv_class: c_int = vpx_read_tree(r, antfly_vp9_vp9_mv_class_tree, @ptrCast(@alignCast(&mvcomp.*.classes)));
    _ = &mv_class;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const class0: c_int = @as(c_int, @intFromBool(mv_class == MV_CLASS_0));
    _ = &class0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (class0 != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        d = vpx_read(r, mvcomp.*.class0[@as(c_int, 0)]);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        mag = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var i: c_int = undefined;
        _ = &i;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const n: c_int = (mv_class + @as(c_int, 1)) - @as(c_int, 1);
        _ = &n;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        d = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        {
            i = 0;
            while (i < n) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                d |= vpx_read(r, mvcomp.*.bits[@bitCast(@as(isize, @intCast(i)))]) << @intCast(i);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        }
        mag = (@as(c_int, 1) << @intCast(@as(c_int, 1))) << @intCast(mv_class + @as(c_int, 2));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    fr = vpx_read_tree(r, antfly_vp9_vp9_mv_fp_tree, if (class0 != 0) @as([*c]vpx_prob, @ptrCast(@alignCast(@constCast(&mvcomp.*.class0_fp[@bitCast(@as(isize, @intCast(d)))])))) else @as([*c]vpx_prob, @ptrCast(@alignCast(@constCast(&mvcomp.*.fp)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    hp = if (usehp != 0) vpx_read(r, if (class0 != 0) @as(c_int, mvcomp.*.class0_hp) else @as(c_int, mvcomp.*.hp)) else @as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    mag += (((d << @intCast(@as(c_int, 3))) | (fr << @intCast(@as(c_int, 1)))) | hp) + @as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (sign != 0) -mag else mag;
}
pub fn read_mv(arg_r: [*c]vpx_reader, arg_mv_1: [*c]MV, arg_ref: [*c]const MV, arg_ctx: [*c]const nmv_context, arg_counts: [*c]nmv_context_counts, arg_allow_hp: c_int) callconv(.c) void {
    var r = arg_r;
    _ = &r;
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var ref = arg_ref;
    _ = &ref;
    var ctx = arg_ctx;
    _ = &ctx;
    var counts = arg_counts;
    _ = &counts;
    var allow_hp = arg_allow_hp;
    _ = &allow_hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const joint_type: MV_JOINT_TYPE = @bitCast(vpx_read_tree(r, antfly_vp9_vp9_mv_joint_tree, @ptrCast(@alignCast(&ctx.*.joints))));
    _ = &joint_type;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const use_hp: c_int = @as(c_int, @intFromBool((allow_hp != 0) and (use_mv_hp(ref) != 0)));
    _ = &use_hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var diff: MV = MV{
        .row = 0,
        .col = 0,
    };
    _ = &diff;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (mv_joint_vertical(joint_type) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        diff.row = @truncate(read_mv_component(r, &ctx.*.comps[@as(c_int, 0)], use_hp));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (mv_joint_horizontal(joint_type) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        diff.col = @truncate(read_mv_component(r, &ctx.*.comps[@as(c_int, 1)], use_hp));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    antfly_vp9_vp9_inc_mv(&diff, counts);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mv_1.*.row = @truncate(@as(c_int, ref.*.row) + @as(c_int, diff.row));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mv_1.*.col = @truncate(@as(c_int, ref.*.col) + @as(c_int, diff.col));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_block_reference_mode(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]const MACROBLOCKD, arg_r: [*c]vpx_reader) callconv(.c) REFERENCE_MODE {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.reference_mode == @as(REFERENCE_MODE, REFERENCE_MODE_SELECT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const ctx: c_int = antfly_vp9_vp9_get_reference_mode_context(cm, xd);
        _ = &ctx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const mode: REFERENCE_MODE = @bitCast(vpx_read(r, cm.*.fc.*.comp_inter_prob[@bitCast(@as(isize, @intCast(ctx)))]));
        _ = &mode;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var counts: [*c]FRAME_COUNTS = xd.*.counts;
        _ = &counts;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (counts != null) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            counts.*.comp_inter[@bitCast(@as(isize, @intCast(ctx)))][mode] +%= 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        return mode;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return cm.*.reference_mode;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub fn read_ref_frames(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_r: [*c]vpx_reader, arg_segment_id: c_int, arg_ref_frame: [*c]MV_REFERENCE_FRAME) callconv(.c) void {
    _ = &cm;
    _ = &xd;
    var r = arg_r;
    _ = &r;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var ref_frame = arg_ref_frame;
    _ = &ref_frame;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const fc: [*c]FRAME_CONTEXT = cm.*.fc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var counts: [*c]FRAME_COUNTS = xd.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (segfeature_active(&cm.*.seg, segment_id, SEG_LVL_REF_FRAME) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ref_frame[@as(c_int, 0)] = @truncate(get_segdata(&cm.*.seg, segment_id, SEG_LVL_REF_FRAME));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ref_frame[@as(c_int, 1)] = @truncate(-@as(c_int, 1));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mode: REFERENCE_MODE = read_block_reference_mode(cm, xd, r);
        _ = &mode;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (mode == @as(REFERENCE_MODE, COMPOUND_REFERENCE)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const idx: c_int = cm.*.ref_frame_sign_bias[@bitCast(@as(isize, @intCast(cm.*.comp_fixed_ref)))];
            _ = &idx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const ctx: c_int = antfly_vp9_vp9_get_pred_context_comp_ref_p(cm, xd);
            _ = &ctx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const bit: c_int = vpx_read(r, fc.*.comp_ref_prob[@bitCast(@as(isize, @intCast(ctx)))]);
            _ = &bit;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (counts != null) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                counts.*.comp_ref[@bitCast(@as(isize, @intCast(ctx)))][@bitCast(@as(isize, @intCast(bit)))] +%= 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            ref_frame[@bitCast(@as(isize, @intCast(idx)))] = cm.*.comp_fixed_ref;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ref_frame[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(idx != 0))))))] = cm.*.comp_var_ref[@bitCast(@as(isize, @intCast(bit)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (mode == @as(REFERENCE_MODE, SINGLE_REFERENCE)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const ctx0: c_int = antfly_vp9_vp9_get_pred_context_single_ref_p1(xd);
                _ = &ctx0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const bit0: c_int = vpx_read(r, fc.*.single_ref_prob[@bitCast(@as(isize, @intCast(ctx0)))][@as(c_int, 0)]);
                _ = &bit0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (counts != null) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    counts.*.single_ref[@bitCast(@as(isize, @intCast(ctx0)))][@as(c_int, 0)][@bitCast(@as(isize, @intCast(bit0)))] +%= 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
                if (bit0 != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const ctx1: c_int = antfly_vp9_vp9_get_pred_context_single_ref_p2(xd);
                    _ = &ctx1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const bit1: c_int = vpx_read(r, fc.*.single_ref_prob[@bitCast(@as(isize, @intCast(ctx1)))][@as(c_int, 1)]);
                    _ = &bit1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (counts != null) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        counts.*.single_ref[@bitCast(@as(isize, @intCast(ctx1)))][@as(c_int, 1)][@bitCast(@as(isize, @intCast(bit1)))] +%= 1;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    ref_frame[@as(c_int, 0)] = @truncate(if (bit1 != 0) @as(c_int, 3) else @as(c_int, 2));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    ref_frame[@as(c_int, 0)] = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                ref_frame[@as(c_int, 1)] = @truncate(-@as(c_int, 1));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = @as(c_int, 0);
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_switchable_interp_filter(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_r: [*c]vpx_reader) callconv(.c) INTERP_FILTER {
    _ = &cm;
    _ = &xd;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const ctx: c_int = get_pred_context_switchable_interp(xd);
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const @"type": INTERP_FILTER = @bitCast(@as(i8, @truncate(vpx_read_tree(r, @ptrCast(@alignCast(&antfly_vp9_vp9_switchable_interp_tree)), @ptrCast(@alignCast(&cm.*.fc.*.switchable_interp_prob[@bitCast(@as(isize, @intCast(ctx)))]))))));
    _ = &@"type";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var counts: [*c]FRAME_COUNTS = xd.*.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        counts.*.switchable_interp[@bitCast(@as(isize, @intCast(ctx)))][@"type"] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return @"type";
}
pub fn read_intra_block_mode_info(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_mi: [*c]MODE_INFO, arg_r: [*c]vpx_reader) callconv(.c) void {
    _ = &cm;
    _ = &xd;
    var mi = arg_mi;
    _ = &mi;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bsize: BLOCK_SIZE = mi.*.sb_type;
    _ = &bsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    blk: {
        switch (@as(c_int, bsize)) {
            @as(c_int, 0) => {
                {
                    i = 0;
                    while (i < @as(c_int, 4)) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        mi.*.bmi[@bitCast(@as(isize, @intCast(i)))].as_mode = read_intra_mode_y(cm, xd, r, 0);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                mi.*.mode = mi.*.bmi[@as(c_int, 3)].as_mode;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            @as(c_int, 1) => {
                mi.*.bmi[@as(c_int, 0)].as_mode = blk_1: {
                    const tmp = read_intra_mode_y(cm, xd, r, 0);
                    mi.*.bmi[@as(c_int, 2)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mi.*.bmi[@as(c_int, 1)].as_mode = blk_1: {
                    const tmp = blk_2: {
                        const tmp_3 = read_intra_mode_y(cm, xd, r, 0);
                        mi.*.mode = tmp_3;
                        break :blk_2 tmp_3;
                    };
                    mi.*.bmi[@as(c_int, 3)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            @as(c_int, 2) => {
                mi.*.bmi[@as(c_int, 0)].as_mode = blk_1: {
                    const tmp = read_intra_mode_y(cm, xd, r, 0);
                    mi.*.bmi[@as(c_int, 1)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mi.*.bmi[@as(c_int, 2)].as_mode = blk_1: {
                    const tmp = blk_2: {
                        const tmp_3 = read_intra_mode_y(cm, xd, r, 0);
                        mi.*.mode = tmp_3;
                        break :blk_2 tmp_3;
                    };
                    mi.*.bmi[@as(c_int, 3)].as_mode = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            else => {
                mi.*.mode = read_intra_mode_y(cm, xd, r, antfly_vp9_size_group_lookup[bsize]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            },
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.uv_mode = read_intra_mode_uv(cm, xd, r, mi.*.mode);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.interp_filter = 3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.ref_frame[@as(c_int, 0)] = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.ref_frame[@as(c_int, 1)] = @truncate(-@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn is_mv_valid(arg_mv_1: [*c]const MV) callconv(.c) c_int {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((((@as(c_int, mv_1.*.row) > -(@as(c_int, 1) << @intCast(@as(c_int, 14)))) and (@as(c_int, mv_1.*.row) < ((@as(c_int, 1) << @intCast(@as(c_int, 14))) - @as(c_int, 1)))) and (@as(c_int, mv_1.*.col) > -(@as(c_int, 1) << @intCast(@as(c_int, 14))))) and (@as(c_int, mv_1.*.col) < ((@as(c_int, 1) << @intCast(@as(c_int, 14))) - @as(c_int, 1)))));
}
pub fn copy_mv_pair(arg_dst: [*c]int_mv, arg_src: [*c]const int_mv) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var src = arg_src;
    _ = &src;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(src)), @sizeOf(@TypeOf(dst.*)) *% @as(c_ulong, 2));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn zero_mv_pair(arg_dst: [*c]int_mv) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), 0, @sizeOf(@TypeOf(dst.*)) *% @as(c_ulong, 2));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn assign_mv(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_mode: PREDICTION_MODE, arg_mv_1: [*c]int_mv, arg_ref_mv: [*c]int_mv, arg_near_nearest_mv: [*c]int_mv, arg_is_compound: c_int, arg_allow_hp: c_int, arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var mode = arg_mode;
    _ = &mode;
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var ref_mv = arg_ref_mv;
    _ = &ref_mv;
    var near_nearest_mv = arg_near_nearest_mv;
    _ = &near_nearest_mv;
    var is_compound = arg_is_compound;
    _ = &is_compound;
    var allow_hp = arg_allow_hp;
    _ = &allow_hp;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ret: c_int = 1;
    _ = &ret;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    blk: {
        switch (@as(c_int, mode)) {
            @as(c_int, 13) => {
                {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    var counts: [*c]FRAME_COUNTS = xd.*.counts;
                    _ = &counts;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    const mv_counts: [*c]nmv_context_counts = @ptrCast(@alignCast(if (counts != null) @as(?*anyopaque, @ptrCast(@alignCast(&counts.*.mv))) else @as(?*anyopaque, null)));
                    _ = &mv_counts;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    {
                        i = 0;
                        while (i < (@as(c_int, 1) + is_compound)) : (i += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            read_mv(r, &mv_1[@bitCast(@as(isize, @intCast(i)))].as_mv, &ref_mv[@bitCast(@as(isize, @intCast(i)))].as_mv, &cm.*.fc.*.nmvc, mv_counts, allow_hp);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            ret = @as(c_int, @intFromBool((ret != 0) and (is_mv_valid(&mv_1[@bitCast(@as(isize, @intCast(i)))].as_mv) != 0)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    break :blk;
                }
            },
            @as(c_int, 11), @as(c_int, 10) => {
                {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    copy_mv_pair(mv_1, near_nearest_mv);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    break :blk;
                }
            },
            @as(c_int, 12) => {
                {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    zero_mv_pair(mv_1);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    break :blk;
                }
            },
            else => {
                {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    return 0;
                }
            },
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ret;
}
pub fn read_is_inter_block(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_segment_id: c_int, arg_r: [*c]vpx_reader) callconv(.c) c_int {
    _ = &cm;
    _ = &xd;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (segfeature_active(&cm.*.seg, segment_id, SEG_LVL_REF_FRAME) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @as(c_int, @intFromBool(get_segdata(&cm.*.seg, segment_id, SEG_LVL_REF_FRAME) != @as(c_int, 0)));
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const ctx: c_int = get_intra_inter_context(xd);
        _ = &ctx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const is_inter: c_int = vpx_read(r, cm.*.fc.*.intra_inter_prob[@bitCast(@as(isize, @intCast(ctx)))]);
        _ = &is_inter;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var counts: [*c]FRAME_COUNTS = xd.*.counts;
        _ = &counts;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (counts != null) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            counts.*.intra_inter[@bitCast(@as(isize, @intCast(ctx)))][@bitCast(@as(isize, @intCast(is_inter)))] +%= 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        return is_inter;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub fn dec_find_mv_refs(arg_cm: [*c]const VP9_COMMON, arg_xd: [*c]const MACROBLOCKD, arg_mode: PREDICTION_MODE, arg_ref_frame: MV_REFERENCE_FRAME, mv_ref_search: [*c]const POSITION, arg_mv_ref_list: [*c]int_mv, arg_mi_row: c_int, arg_mi_col: c_int, arg_block: c_int) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var mode = arg_mode;
    _ = &mode;
    var ref_frame = arg_ref_frame;
    _ = &ref_frame;
    _ = &mv_ref_search;
    var mv_ref_list = arg_mv_ref_list;
    _ = &mv_ref_list;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var block = arg_block;
    _ = &block;
    var afz_mv_14: int_mv = undefined;
    _ = &afz_mv_14;
    var afz_mv_13: int_mv = undefined;
    _ = &afz_mv_13;
    var afz_candidate_12: [*c]MODE_INFO = undefined;
    _ = &afz_candidate_12;
    var afz_mv_ref_11: [*c]POSITION = undefined;
    _ = &afz_mv_ref_11;
    var afz_candidate_10: [*c]MODE_INFO = undefined;
    _ = &afz_candidate_10;
    var afz_mv_ref_9: [*c]POSITION = undefined;
    _ = &afz_mv_ref_9;
    var afz_candidate_mi_8: [*c]MODE_INFO = undefined;
    _ = &afz_candidate_mi_8;
    var afz_mv_ref_7: [*c]POSITION = undefined;
    _ = &afz_mv_ref_7;
    var afz_early_break_6: c_int = undefined;
    _ = &afz_early_break_6;
    var afz_tile_5: [*c]TileInfo = undefined;
    _ = &afz_tile_5;
    var afz_prev_frame_mvs_4: [*c]MV_REF = undefined;
    _ = &afz_prev_frame_mvs_4;
    var afz_different_ref_found_3: c_int = undefined;
    _ = &afz_different_ref_found_3;
    var afz_i_1: c_int = undefined;
    _ = &afz_i_1;
    var afz_refmv_count_2: c_int = undefined;
    _ = &afz_refmv_count_2;
    var afz_ref_sign_bias_0: [*c]c_int = undefined;
    _ = &afz_ref_sign_bias_0;
    var afz_pc: c_uint = 573;
    _ = &afz_pc;
    while (true) {
        if (afz_pc == @as(c_uint, 0)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 1)) {
            return afz_refmv_count_2;
        }
        if (afz_pc == @as(c_uint, 2)) {
            afz_pc = @bitCast(@as(c_int, if (afz_i_1 < afz_refmv_count_2) @as(c_int, 10) else @as(c_int, 1)));
            continue;
        }
        if (afz_pc == @as(c_uint, 3)) {
            afz_i_1 += 1;
            afz_pc = 2;
            continue;
        }
        if (afz_pc == @as(c_uint, 4)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 5)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 4) else @as(c_int, 3)));
            continue;
        }
        if (afz_pc == @as(c_uint, 6)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 7)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 6) else @as(c_int, 5)));
            continue;
        }
        if (afz_pc == @as(c_uint, 8)) {
            clamp_mv_ref(&mv_ref_list[@bitCast(@as(isize, @intCast(afz_i_1)))].as_mv, xd);
            afz_pc = 7;
            continue;
        }
        if (afz_pc == @as(c_uint, 9)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 10)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 9) else @as(c_int, 8)));
            continue;
        }
        if (afz_pc == @as(c_uint, 11)) {
            afz_i_1 = 0;
            afz_pc = 2;
            continue;
        }
        if (afz_pc == @as(c_uint, 12)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 13)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 12) else @as(c_int, 11)));
            continue;
        }
        if (afz_pc == @as(c_uint, 14)) {
            afz_refmv_count_2 = 2;
            afz_pc = 13;
            continue;
        }
        if (afz_pc == @as(c_uint, 15)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 16)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 15) else @as(c_int, 14)));
            continue;
        }
        if (afz_pc == @as(c_uint, 17)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 18)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 17) else @as(c_int, 11)));
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
            afz_refmv_count_2 = 1;
            afz_pc = 20;
            continue;
        }
        if (afz_pc == @as(c_uint, 22)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 23)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 22) else @as(c_int, 21)));
            continue;
        }
        if (afz_pc == @as(c_uint, 24)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, mode) == @as(c_int, 11)) @as(c_int, 16) else @as(c_int, 23)));
            continue;
        }
        if (afz_pc == @as(c_uint, 25)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 26)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 25) else @as(c_int, 24)));
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
            return 0;
        }
        if (afz_pc == @as(c_uint, 30)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 29) else @as(c_int, 28)));
            continue;
        }
        if (afz_pc == @as(c_uint, 31)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 63) else @as(c_int, 30);
            continue;
        }
        if (afz_pc == @as(c_uint, 32)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 33)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 32) else @as(c_int, 31)));
            continue;
        }
        if (afz_pc == @as(c_uint, 34)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 35)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 34) else @as(c_int, 33)));
            continue;
        }
        if (afz_pc == @as(c_uint, 36)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 37)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 38)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 37) else @as(c_int, 36)));
            continue;
        }
        if (afz_pc == @as(c_uint, 39)) {
            afz_refmv_count_2 += 1;
            afz_pc = 38;
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
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = afz_mv_14;
            afz_pc = 41;
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
            afz_pc = @bitCast(@as(c_int, if (afz_mv_14.as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 44) else @as(c_int, 35)));
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
            return 0;
        }
        if (afz_pc == @as(c_uint, 49)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 48) else @as(c_int, 33)));
            continue;
        }
        if (afz_pc == @as(c_uint, 50)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 51)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 50) else @as(c_int, 49)));
            continue;
        }
        if (afz_pc == @as(c_uint, 52)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 53)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 54)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 53) else @as(c_int, 52)));
            continue;
        }
        if (afz_pc == @as(c_uint, 55)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 54) else @as(c_int, 33)));
            continue;
        }
        if (afz_pc == @as(c_uint, 56)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 57)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 56) else @as(c_int, 55)));
            continue;
        }
        if (afz_pc == @as(c_uint, 58)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = afz_mv_14;
            afz_pc = 57;
            continue;
        }
        if (afz_pc == @as(c_uint, 59)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 60)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 59) else @as(c_int, 58)));
            continue;
        }
        if (afz_pc == @as(c_uint, 61)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 47) else @as(c_int, 60)));
            continue;
        }
        if (afz_pc == @as(c_uint, 62)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 63)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 62) else @as(c_int, 61)));
            continue;
        }
        if (afz_pc == @as(c_uint, 64)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 65)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 64) else @as(c_int, 63)));
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
            {
                const ref = &afz_mv_14.as_mv.col;
                ref.* = @truncate(@as(c_int, ref.*) * -@as(c_int, 1));
            }
            afz_pc = 67;
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
            {
                const ref = &afz_mv_14.as_mv.row;
                ref.* = @truncate(@as(c_int, ref.*) * -@as(c_int, 1));
            }
            afz_pc = 70;
            continue;
        }
        if (afz_pc == @as(c_uint, 72)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 73)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 72) else @as(c_int, 71)));
            continue;
        }
        if (afz_pc == @as(c_uint, 74)) {
            afz_pc = @bitCast(@as(c_int, if (afz_ref_sign_bias_0[@bitCast(@as(isize, @intCast(afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 1)])))] != afz_ref_sign_bias_0[@bitCast(@as(isize, @intCast(ref_frame)))]) @as(c_int, 73) else @as(c_int, 65)));
            continue;
        }
        if (afz_pc == @as(c_uint, 75)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 76)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 75) else @as(c_int, 74)));
            continue;
        }
        if (afz_pc == @as(c_uint, 77)) {
            afz_mv_14 = afz_prev_frame_mvs_4.*.mv[@as(c_int, 1)];
            afz_pc = 76;
            continue;
        }
        if (afz_pc == @as(c_uint, 78)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 79)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 78) else @as(c_int, 77)));
            continue;
        }
        if (afz_pc == @as(c_uint, 80)) {
            afz_pc = @bitCast(@as(c_int, if (((@as(c_int, afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 1)]) > @as(c_int, 0)) and (@as(c_int, afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 1)]) != @as(c_int, ref_frame))) and (afz_prev_frame_mvs_4.*.mv[@as(c_int, 1)].as_int != afz_prev_frame_mvs_4.*.mv[@as(c_int, 0)].as_int)) @as(c_int, 79) else @as(c_int, 28)));
            continue;
        }
        if (afz_pc == @as(c_uint, 81)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 82)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 81) else @as(c_int, 80)));
            continue;
        }
        if (afz_pc == @as(c_uint, 83)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 84)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 83) else @as(c_int, 82)));
            continue;
        }
        if (afz_pc == @as(c_uint, 85)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 117) else @as(c_int, 84);
            continue;
        }
        if (afz_pc == @as(c_uint, 86)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 87)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 86) else @as(c_int, 85)));
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
            afz_pc = 11;
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
            afz_refmv_count_2 += 1;
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
        if (afz_pc == @as(c_uint, 96)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = afz_mv_13;
            afz_pc = 95;
            continue;
        }
        if (afz_pc == @as(c_uint, 97)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 98)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 97) else @as(c_int, 96)));
            continue;
        }
        if (afz_pc == @as(c_uint, 99)) {
            afz_pc = @bitCast(@as(c_int, if (afz_mv_13.as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 98) else @as(c_int, 89)));
            continue;
        }
        if (afz_pc == @as(c_uint, 100)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 101)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 100) else @as(c_int, 99)));
            continue;
        }
        if (afz_pc == @as(c_uint, 102)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 103)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 102) else @as(c_int, 87)));
            continue;
        }
        if (afz_pc == @as(c_uint, 104)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 105)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 104) else @as(c_int, 103)));
            continue;
        }
        if (afz_pc == @as(c_uint, 106)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 107)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 108)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 107) else @as(c_int, 106)));
            continue;
        }
        if (afz_pc == @as(c_uint, 109)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 108) else @as(c_int, 87)));
            continue;
        }
        if (afz_pc == @as(c_uint, 110)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 111)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 110) else @as(c_int, 109)));
            continue;
        }
        if (afz_pc == @as(c_uint, 112)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = afz_mv_13;
            afz_pc = 111;
            continue;
        }
        if (afz_pc == @as(c_uint, 113)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 114)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 113) else @as(c_int, 112)));
            continue;
        }
        if (afz_pc == @as(c_uint, 115)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 101) else @as(c_int, 114)));
            continue;
        }
        if (afz_pc == @as(c_uint, 116)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 117)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 116) else @as(c_int, 115)));
            continue;
        }
        if (afz_pc == @as(c_uint, 118)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 119)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 118) else @as(c_int, 117)));
            continue;
        }
        if (afz_pc == @as(c_uint, 120)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 121)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 120) else @as(c_int, 119)));
            continue;
        }
        if (afz_pc == @as(c_uint, 122)) {
            {
                const ref = &afz_mv_13.as_mv.col;
                ref.* = @truncate(@as(c_int, ref.*) * -@as(c_int, 1));
            }
            afz_pc = 121;
            continue;
        }
        if (afz_pc == @as(c_uint, 123)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 124)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 123) else @as(c_int, 122)));
            continue;
        }
        if (afz_pc == @as(c_uint, 125)) {
            {
                const ref = &afz_mv_13.as_mv.row;
                ref.* = @truncate(@as(c_int, ref.*) * -@as(c_int, 1));
            }
            afz_pc = 124;
            continue;
        }
        if (afz_pc == @as(c_uint, 126)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 127)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 126) else @as(c_int, 125)));
            continue;
        }
        if (afz_pc == @as(c_uint, 128)) {
            afz_pc = @bitCast(@as(c_int, if (afz_ref_sign_bias_0[@bitCast(@as(isize, @intCast(afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 0)])))] != afz_ref_sign_bias_0[@bitCast(@as(isize, @intCast(ref_frame)))]) @as(c_int, 127) else @as(c_int, 119)));
            continue;
        }
        if (afz_pc == @as(c_uint, 129)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 130)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 129) else @as(c_int, 128)));
            continue;
        }
        if (afz_pc == @as(c_uint, 131)) {
            afz_mv_13 = afz_prev_frame_mvs_4.*.mv[@as(c_int, 0)];
            afz_pc = 130;
            continue;
        }
        if (afz_pc == @as(c_uint, 132)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 133)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 132) else @as(c_int, 131)));
            continue;
        }
        if (afz_pc == @as(c_uint, 134)) {
            afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 0)]) != @as(c_int, ref_frame)) and (@as(c_int, afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 0)]) > @as(c_int, 0))) @as(c_int, 133) else @as(c_int, 82)));
            continue;
        }
        if (afz_pc == @as(c_uint, 135)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 136)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 135) else @as(c_int, 134)));
            continue;
        }
        if (afz_pc == @as(c_uint, 137)) {
            afz_pc = @bitCast(@as(c_int, if (afz_prev_frame_mvs_4 != null) @as(c_int, 136) else @as(c_int, 26)));
            continue;
        }
        if (afz_pc == @as(c_uint, 138)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 139)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 138) else @as(c_int, 137)));
            continue;
        }
        if (afz_pc == @as(c_uint, 140)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 141)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 140) else @as(c_int, 139)));
            continue;
        }
        if (afz_pc == @as(c_uint, 142)) {
            afz_pc = @bitCast(@as(c_int, if (afz_i_1 < @as(c_int, 8)) @as(c_int, 246) else @as(c_int, 141)));
            continue;
        }
        if (afz_pc == @as(c_uint, 143)) {
            afz_i_1 += 1;
            afz_pc = 142;
            continue;
        }
        if (afz_pc == @as(c_uint, 144)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 145)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 144) else @as(c_int, 143)));
            continue;
        }
        if (afz_pc == @as(c_uint, 146)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 147)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 146) else @as(c_int, 145)));
            continue;
        }
        if (afz_pc == @as(c_uint, 148)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 235) else @as(c_int, 147);
            continue;
        }
        if (afz_pc == @as(c_uint, 149)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 150)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 149) else @as(c_int, 148)));
            continue;
        }
        if (afz_pc == @as(c_uint, 151)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 152)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 151) else @as(c_int, 150)));
            continue;
        }
        if (afz_pc == @as(c_uint, 153)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 154)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 153) else @as(c_int, 152)));
            continue;
        }
        if (afz_pc == @as(c_uint, 155)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 187) else @as(c_int, 154);
            continue;
        }
        if (afz_pc == @as(c_uint, 156)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 157)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 156) else @as(c_int, 155)));
            continue;
        }
        if (afz_pc == @as(c_uint, 158)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 159)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 158) else @as(c_int, 157)));
            continue;
        }
        if (afz_pc == @as(c_uint, 160)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 161)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 162)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 161) else @as(c_int, 160)));
            continue;
        }
        if (afz_pc == @as(c_uint, 163)) {
            afz_refmv_count_2 += 1;
            afz_pc = 162;
            continue;
        }
        if (afz_pc == @as(c_uint, 164)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 165)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 164) else @as(c_int, 163)));
            continue;
        }
        if (afz_pc == @as(c_uint, 166)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = scale_mv(afz_candidate_12, 1, ref_frame, afz_ref_sign_bias_0);
            afz_pc = 165;
            continue;
        }
        if (afz_pc == @as(c_uint, 167)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 168)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 167) else @as(c_int, 166)));
            continue;
        }
        if (afz_pc == @as(c_uint, 169)) {
            afz_pc = @bitCast(@as(c_int, if (scale_mv(afz_candidate_12, 1, ref_frame, afz_ref_sign_bias_0).as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 168) else @as(c_int, 159)));
            continue;
        }
        if (afz_pc == @as(c_uint, 170)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 171)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 170) else @as(c_int, 169)));
            continue;
        }
        if (afz_pc == @as(c_uint, 172)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 173)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 172) else @as(c_int, 157)));
            continue;
        }
        if (afz_pc == @as(c_uint, 174)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 175)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 174) else @as(c_int, 173)));
            continue;
        }
        if (afz_pc == @as(c_uint, 176)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 177)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 178)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 177) else @as(c_int, 176)));
            continue;
        }
        if (afz_pc == @as(c_uint, 179)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 178) else @as(c_int, 157)));
            continue;
        }
        if (afz_pc == @as(c_uint, 180)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 181)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 180) else @as(c_int, 179)));
            continue;
        }
        if (afz_pc == @as(c_uint, 182)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = scale_mv(afz_candidate_12, 1, ref_frame, afz_ref_sign_bias_0);
            afz_pc = 181;
            continue;
        }
        if (afz_pc == @as(c_uint, 183)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 184)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 183) else @as(c_int, 182)));
            continue;
        }
        if (afz_pc == @as(c_uint, 185)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 171) else @as(c_int, 184)));
            continue;
        }
        if (afz_pc == @as(c_uint, 186)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 187)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 186) else @as(c_int, 185)));
            continue;
        }
        if (afz_pc == @as(c_uint, 188)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 189)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 188) else @as(c_int, 187)));
            continue;
        }
        if (afz_pc == @as(c_uint, 190)) {
            afz_pc = @bitCast(@as(c_int, if (((has_second_ref(afz_candidate_12) != 0) and (@as(c_int, afz_candidate_12.*.ref_frame[@as(c_int, 1)]) != @as(c_int, ref_frame))) and (afz_candidate_12.*.mv[@as(c_int, 1)].as_int != afz_candidate_12.*.mv[@as(c_int, 0)].as_int)) @as(c_int, 189) else @as(c_int, 150)));
            continue;
        }
        if (afz_pc == @as(c_uint, 191)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 192)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 191) else @as(c_int, 190)));
            continue;
        }
        if (afz_pc == @as(c_uint, 193)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 194)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 193) else @as(c_int, 192)));
            continue;
        }
        if (afz_pc == @as(c_uint, 195)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 227) else @as(c_int, 194);
            continue;
        }
        if (afz_pc == @as(c_uint, 196)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 197)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 196) else @as(c_int, 195)));
            continue;
        }
        if (afz_pc == @as(c_uint, 198)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 199)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 198) else @as(c_int, 197)));
            continue;
        }
        if (afz_pc == @as(c_uint, 200)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 201)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 202)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 201) else @as(c_int, 200)));
            continue;
        }
        if (afz_pc == @as(c_uint, 203)) {
            afz_refmv_count_2 += 1;
            afz_pc = 202;
            continue;
        }
        if (afz_pc == @as(c_uint, 204)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 205)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 204) else @as(c_int, 203)));
            continue;
        }
        if (afz_pc == @as(c_uint, 206)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = scale_mv(afz_candidate_12, 0, ref_frame, afz_ref_sign_bias_0);
            afz_pc = 205;
            continue;
        }
        if (afz_pc == @as(c_uint, 207)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 208)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 207) else @as(c_int, 206)));
            continue;
        }
        if (afz_pc == @as(c_uint, 209)) {
            afz_pc = @bitCast(@as(c_int, if (scale_mv(afz_candidate_12, 0, ref_frame, afz_ref_sign_bias_0).as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 208) else @as(c_int, 199)));
            continue;
        }
        if (afz_pc == @as(c_uint, 210)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 211)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 210) else @as(c_int, 209)));
            continue;
        }
        if (afz_pc == @as(c_uint, 212)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 213)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 212) else @as(c_int, 197)));
            continue;
        }
        if (afz_pc == @as(c_uint, 214)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 215)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 214) else @as(c_int, 213)));
            continue;
        }
        if (afz_pc == @as(c_uint, 216)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 217)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 218)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 217) else @as(c_int, 216)));
            continue;
        }
        if (afz_pc == @as(c_uint, 219)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 218) else @as(c_int, 197)));
            continue;
        }
        if (afz_pc == @as(c_uint, 220)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 221)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 220) else @as(c_int, 219)));
            continue;
        }
        if (afz_pc == @as(c_uint, 222)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = scale_mv(afz_candidate_12, 0, ref_frame, afz_ref_sign_bias_0);
            afz_pc = 221;
            continue;
        }
        if (afz_pc == @as(c_uint, 223)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 224)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 223) else @as(c_int, 222)));
            continue;
        }
        if (afz_pc == @as(c_uint, 225)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 211) else @as(c_int, 224)));
            continue;
        }
        if (afz_pc == @as(c_uint, 226)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 227)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 226) else @as(c_int, 225)));
            continue;
        }
        if (afz_pc == @as(c_uint, 228)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 229)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 228) else @as(c_int, 227)));
            continue;
        }
        if (afz_pc == @as(c_uint, 230)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_candidate_12.*.ref_frame[@as(c_int, 0)]) != @as(c_int, ref_frame)) @as(c_int, 229) else @as(c_int, 190)));
            continue;
        }
        if (afz_pc == @as(c_uint, 231)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 232)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 231) else @as(c_int, 230)));
            continue;
        }
        if (afz_pc == @as(c_uint, 233)) {
            afz_pc = @bitCast(@as(c_int, if (is_inter_block(afz_candidate_12) != 0) @as(c_int, 232) else @as(c_int, 150)));
            continue;
        }
        if (afz_pc == @as(c_uint, 234)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 235)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 234) else @as(c_int, 233)));
            continue;
        }
        if (afz_pc == @as(c_uint, 236)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 237)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 236) else @as(c_int, 235)));
            continue;
        }
        if (afz_pc == @as(c_uint, 238)) {
            afz_candidate_12 = xd.*.mi[@bitCast(@as(isize, @intCast(afz_mv_ref_11.*.col + (afz_mv_ref_11.*.row * xd.*.mi_stride))))];
            afz_pc = 237;
            continue;
        }
        if (afz_pc == @as(c_uint, 239)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 240)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 239) else @as(c_int, 238)));
            continue;
        }
        if (afz_pc == @as(c_uint, 241)) {
            afz_pc = @bitCast(@as(c_int, if (is_inside(afz_tile_5, mi_col, mi_row, cm.*.mi_rows, afz_mv_ref_11) != 0) @as(c_int, 240) else @as(c_int, 145)));
            continue;
        }
        if (afz_pc == @as(c_uint, 242)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 243)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 242) else @as(c_int, 241)));
            continue;
        }
        if (afz_pc == @as(c_uint, 244)) {
            afz_mv_ref_11 = @ptrCast(@alignCast(@constCast(&mv_ref_search[@bitCast(@as(isize, @intCast(afz_i_1)))])));
            afz_pc = 243;
            continue;
        }
        if (afz_pc == @as(c_uint, 245)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 246)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 245) else @as(c_int, 244)));
            continue;
        }
        if (afz_pc == @as(c_uint, 247)) {
            afz_i_1 = 0;
            afz_pc = 142;
            continue;
        }
        if (afz_pc == @as(c_uint, 248)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 249)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 248) else @as(c_int, 247)));
            continue;
        }
        if (afz_pc == @as(c_uint, 250)) {
            afz_pc = @bitCast(@as(c_int, if (afz_different_ref_found_3 != 0) @as(c_int, 249) else @as(c_int, 139)));
            continue;
        }
        if (afz_pc == @as(c_uint, 251)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 252)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 251) else @as(c_int, 250)));
            continue;
        }
        if (afz_pc == @as(c_uint, 253)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 254)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 253) else @as(c_int, 252)));
            continue;
        }
        if (afz_pc == @as(c_uint, 255)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 287) else @as(c_int, 254);
            continue;
        }
        if (afz_pc == @as(c_uint, 256)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 257)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 256) else @as(c_int, 255)));
            continue;
        }
        if (afz_pc == @as(c_uint, 258)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 259)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 258) else @as(c_int, 257)));
            continue;
        }
        if (afz_pc == @as(c_uint, 260)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 261)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 262)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 261) else @as(c_int, 260)));
            continue;
        }
        if (afz_pc == @as(c_uint, 263)) {
            afz_refmv_count_2 += 1;
            afz_pc = 262;
            continue;
        }
        if (afz_pc == @as(c_uint, 264)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 265)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 264) else @as(c_int, 263)));
            continue;
        }
        if (afz_pc == @as(c_uint, 266)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = afz_prev_frame_mvs_4.*.mv[@as(c_int, 0)];
            afz_pc = 265;
            continue;
        }
        if (afz_pc == @as(c_uint, 267)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 268)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 267) else @as(c_int, 266)));
            continue;
        }
        if (afz_pc == @as(c_uint, 269)) {
            afz_pc = @bitCast(@as(c_int, if (afz_prev_frame_mvs_4.*.mv[@as(c_int, 0)].as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 268) else @as(c_int, 259)));
            continue;
        }
        if (afz_pc == @as(c_uint, 270)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 271)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 270) else @as(c_int, 269)));
            continue;
        }
        if (afz_pc == @as(c_uint, 272)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 273)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 272) else @as(c_int, 257)));
            continue;
        }
        if (afz_pc == @as(c_uint, 274)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 275)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 274) else @as(c_int, 273)));
            continue;
        }
        if (afz_pc == @as(c_uint, 276)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 277)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 278)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 277) else @as(c_int, 276)));
            continue;
        }
        if (afz_pc == @as(c_uint, 279)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 278) else @as(c_int, 257)));
            continue;
        }
        if (afz_pc == @as(c_uint, 280)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 281)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 280) else @as(c_int, 279)));
            continue;
        }
        if (afz_pc == @as(c_uint, 282)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = afz_prev_frame_mvs_4.*.mv[@as(c_int, 0)];
            afz_pc = 281;
            continue;
        }
        if (afz_pc == @as(c_uint, 283)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 284)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 283) else @as(c_int, 282)));
            continue;
        }
        if (afz_pc == @as(c_uint, 285)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 271) else @as(c_int, 284)));
            continue;
        }
        if (afz_pc == @as(c_uint, 286)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 287)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 286) else @as(c_int, 285)));
            continue;
        }
        if (afz_pc == @as(c_uint, 288)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 289)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 288) else @as(c_int, 287)));
            continue;
        }
        if (afz_pc == @as(c_uint, 290)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 291)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 290) else @as(c_int, 252)));
            continue;
        }
        if (afz_pc == @as(c_uint, 292)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 293)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 292) else @as(c_int, 291)));
            continue;
        }
        if (afz_pc == @as(c_uint, 294)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 295)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 294) else @as(c_int, 293)));
            continue;
        }
        if (afz_pc == @as(c_uint, 296)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 328) else @as(c_int, 295);
            continue;
        }
        if (afz_pc == @as(c_uint, 297)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 298)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 297) else @as(c_int, 296)));
            continue;
        }
        if (afz_pc == @as(c_uint, 299)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 300)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 299) else @as(c_int, 298)));
            continue;
        }
        if (afz_pc == @as(c_uint, 301)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 302)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 303)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 302) else @as(c_int, 301)));
            continue;
        }
        if (afz_pc == @as(c_uint, 304)) {
            afz_refmv_count_2 += 1;
            afz_pc = 303;
            continue;
        }
        if (afz_pc == @as(c_uint, 305)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 306)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 305) else @as(c_int, 304)));
            continue;
        }
        if (afz_pc == @as(c_uint, 307)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = afz_prev_frame_mvs_4.*.mv[@as(c_int, 1)];
            afz_pc = 306;
            continue;
        }
        if (afz_pc == @as(c_uint, 308)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 309)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 308) else @as(c_int, 307)));
            continue;
        }
        if (afz_pc == @as(c_uint, 310)) {
            afz_pc = @bitCast(@as(c_int, if (afz_prev_frame_mvs_4.*.mv[@as(c_int, 1)].as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 309) else @as(c_int, 300)));
            continue;
        }
        if (afz_pc == @as(c_uint, 311)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 312)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 311) else @as(c_int, 310)));
            continue;
        }
        if (afz_pc == @as(c_uint, 313)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 314)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 313) else @as(c_int, 298)));
            continue;
        }
        if (afz_pc == @as(c_uint, 315)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 316)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 315) else @as(c_int, 314)));
            continue;
        }
        if (afz_pc == @as(c_uint, 317)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 318)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 319)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 318) else @as(c_int, 317)));
            continue;
        }
        if (afz_pc == @as(c_uint, 320)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 319) else @as(c_int, 298)));
            continue;
        }
        if (afz_pc == @as(c_uint, 321)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 322)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 321) else @as(c_int, 320)));
            continue;
        }
        if (afz_pc == @as(c_uint, 323)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = afz_prev_frame_mvs_4.*.mv[@as(c_int, 1)];
            afz_pc = 322;
            continue;
        }
        if (afz_pc == @as(c_uint, 324)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 325)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 324) else @as(c_int, 323)));
            continue;
        }
        if (afz_pc == @as(c_uint, 326)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 312) else @as(c_int, 325)));
            continue;
        }
        if (afz_pc == @as(c_uint, 327)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 328)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 327) else @as(c_int, 326)));
            continue;
        }
        if (afz_pc == @as(c_uint, 329)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 330)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 329) else @as(c_int, 328)));
            continue;
        }
        if (afz_pc == @as(c_uint, 331)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 1)]) == @as(c_int, ref_frame)) @as(c_int, 330) else @as(c_int, 293)));
            continue;
        }
        if (afz_pc == @as(c_uint, 332)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 333)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 332) else @as(c_int, 331)));
            continue;
        }
        if (afz_pc == @as(c_uint, 334)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_prev_frame_mvs_4.*.ref_frame[@as(c_int, 0)]) == @as(c_int, ref_frame)) @as(c_int, 289) else @as(c_int, 333)));
            continue;
        }
        if (afz_pc == @as(c_uint, 335)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 336)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 335) else @as(c_int, 334)));
            continue;
        }
        if (afz_pc == @as(c_uint, 337)) {
            afz_pc = @bitCast(@as(c_int, if (afz_prev_frame_mvs_4 != null) @as(c_int, 336) else @as(c_int, 252)));
            continue;
        }
        if (afz_pc == @as(c_uint, 338)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 339)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 338) else @as(c_int, 337)));
            continue;
        }
        if (afz_pc == @as(c_uint, 340)) {
            afz_pc = @bitCast(@as(c_int, if (afz_i_1 < @as(c_int, 8)) @as(c_int, 439) else @as(c_int, 339)));
            continue;
        }
        if (afz_pc == @as(c_uint, 341)) {
            afz_i_1 += 1;
            afz_pc = 340;
            continue;
        }
        if (afz_pc == @as(c_uint, 342)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 343)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 342) else @as(c_int, 341)));
            continue;
        }
        if (afz_pc == @as(c_uint, 344)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 345)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 344) else @as(c_int, 343)));
            continue;
        }
        if (afz_pc == @as(c_uint, 346)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 378) else @as(c_int, 345);
            continue;
        }
        if (afz_pc == @as(c_uint, 347)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 348)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 347) else @as(c_int, 346)));
            continue;
        }
        if (afz_pc == @as(c_uint, 349)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 350)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 349) else @as(c_int, 348)));
            continue;
        }
        if (afz_pc == @as(c_uint, 351)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 352)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 353)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 352) else @as(c_int, 351)));
            continue;
        }
        if (afz_pc == @as(c_uint, 354)) {
            afz_refmv_count_2 += 1;
            afz_pc = 353;
            continue;
        }
        if (afz_pc == @as(c_uint, 355)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 356)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 355) else @as(c_int, 354)));
            continue;
        }
        if (afz_pc == @as(c_uint, 357)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = afz_candidate_10.*.mv[@as(c_int, 0)];
            afz_pc = 356;
            continue;
        }
        if (afz_pc == @as(c_uint, 358)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 359)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 358) else @as(c_int, 357)));
            continue;
        }
        if (afz_pc == @as(c_uint, 360)) {
            afz_pc = @bitCast(@as(c_int, if (afz_candidate_10.*.mv[@as(c_int, 0)].as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 359) else @as(c_int, 350)));
            continue;
        }
        if (afz_pc == @as(c_uint, 361)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 362)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 361) else @as(c_int, 360)));
            continue;
        }
        if (afz_pc == @as(c_uint, 363)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 364)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 363) else @as(c_int, 348)));
            continue;
        }
        if (afz_pc == @as(c_uint, 365)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 366)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 365) else @as(c_int, 364)));
            continue;
        }
        if (afz_pc == @as(c_uint, 367)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 368)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 369)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 368) else @as(c_int, 367)));
            continue;
        }
        if (afz_pc == @as(c_uint, 370)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 369) else @as(c_int, 348)));
            continue;
        }
        if (afz_pc == @as(c_uint, 371)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 372)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 371) else @as(c_int, 370)));
            continue;
        }
        if (afz_pc == @as(c_uint, 373)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = afz_candidate_10.*.mv[@as(c_int, 0)];
            afz_pc = 372;
            continue;
        }
        if (afz_pc == @as(c_uint, 374)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 375)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 374) else @as(c_int, 373)));
            continue;
        }
        if (afz_pc == @as(c_uint, 376)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 362) else @as(c_int, 375)));
            continue;
        }
        if (afz_pc == @as(c_uint, 377)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 378)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 377) else @as(c_int, 376)));
            continue;
        }
        if (afz_pc == @as(c_uint, 379)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 380)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 379) else @as(c_int, 378)));
            continue;
        }
        if (afz_pc == @as(c_uint, 381)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 382)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 381) else @as(c_int, 343)));
            continue;
        }
        if (afz_pc == @as(c_uint, 383)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 384)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 383) else @as(c_int, 382)));
            continue;
        }
        if (afz_pc == @as(c_uint, 385)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 386)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 385) else @as(c_int, 384)));
            continue;
        }
        if (afz_pc == @as(c_uint, 387)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 419) else @as(c_int, 386);
            continue;
        }
        if (afz_pc == @as(c_uint, 388)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 389)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 388) else @as(c_int, 387)));
            continue;
        }
        if (afz_pc == @as(c_uint, 390)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 391)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 390) else @as(c_int, 389)));
            continue;
        }
        if (afz_pc == @as(c_uint, 392)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 393)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 394)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 393) else @as(c_int, 392)));
            continue;
        }
        if (afz_pc == @as(c_uint, 395)) {
            afz_refmv_count_2 += 1;
            afz_pc = 394;
            continue;
        }
        if (afz_pc == @as(c_uint, 396)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 397)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 396) else @as(c_int, 395)));
            continue;
        }
        if (afz_pc == @as(c_uint, 398)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = afz_candidate_10.*.mv[@as(c_int, 1)];
            afz_pc = 397;
            continue;
        }
        if (afz_pc == @as(c_uint, 399)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 400)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 399) else @as(c_int, 398)));
            continue;
        }
        if (afz_pc == @as(c_uint, 401)) {
            afz_pc = @bitCast(@as(c_int, if (afz_candidate_10.*.mv[@as(c_int, 1)].as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 400) else @as(c_int, 391)));
            continue;
        }
        if (afz_pc == @as(c_uint, 402)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 403)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 402) else @as(c_int, 401)));
            continue;
        }
        if (afz_pc == @as(c_uint, 404)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 405)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 404) else @as(c_int, 389)));
            continue;
        }
        if (afz_pc == @as(c_uint, 406)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 407)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 406) else @as(c_int, 405)));
            continue;
        }
        if (afz_pc == @as(c_uint, 408)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 409)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 410)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 409) else @as(c_int, 408)));
            continue;
        }
        if (afz_pc == @as(c_uint, 411)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 410) else @as(c_int, 389)));
            continue;
        }
        if (afz_pc == @as(c_uint, 412)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 413)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 412) else @as(c_int, 411)));
            continue;
        }
        if (afz_pc == @as(c_uint, 414)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = afz_candidate_10.*.mv[@as(c_int, 1)];
            afz_pc = 413;
            continue;
        }
        if (afz_pc == @as(c_uint, 415)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 416)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 415) else @as(c_int, 414)));
            continue;
        }
        if (afz_pc == @as(c_uint, 417)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 403) else @as(c_int, 416)));
            continue;
        }
        if (afz_pc == @as(c_uint, 418)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 419)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 418) else @as(c_int, 417)));
            continue;
        }
        if (afz_pc == @as(c_uint, 420)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 421)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 420) else @as(c_int, 419)));
            continue;
        }
        if (afz_pc == @as(c_uint, 422)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_candidate_10.*.ref_frame[@as(c_int, 1)]) == @as(c_int, ref_frame)) @as(c_int, 421) else @as(c_int, 382)));
            continue;
        }
        if (afz_pc == @as(c_uint, 423)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 424)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 423) else @as(c_int, 422)));
            continue;
        }
        if (afz_pc == @as(c_uint, 425)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_candidate_10.*.ref_frame[@as(c_int, 0)]) == @as(c_int, ref_frame)) @as(c_int, 380) else @as(c_int, 424)));
            continue;
        }
        if (afz_pc == @as(c_uint, 426)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 427)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 426) else @as(c_int, 425)));
            continue;
        }
        if (afz_pc == @as(c_uint, 428)) {
            afz_different_ref_found_3 = 1;
            afz_pc = 427;
            continue;
        }
        if (afz_pc == @as(c_uint, 429)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 430)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 429) else @as(c_int, 428)));
            continue;
        }
        if (afz_pc == @as(c_uint, 431)) {
            afz_candidate_10 = xd.*.mi[@bitCast(@as(isize, @intCast(afz_mv_ref_9.*.col + (afz_mv_ref_9.*.row * xd.*.mi_stride))))];
            afz_pc = 430;
            continue;
        }
        if (afz_pc == @as(c_uint, 432)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 433)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 432) else @as(c_int, 431)));
            continue;
        }
        if (afz_pc == @as(c_uint, 434)) {
            afz_pc = @bitCast(@as(c_int, if (is_inside(afz_tile_5, mi_col, mi_row, cm.*.mi_rows, afz_mv_ref_9) != 0) @as(c_int, 433) else @as(c_int, 343)));
            continue;
        }
        if (afz_pc == @as(c_uint, 435)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 436)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 435) else @as(c_int, 434)));
            continue;
        }
        if (afz_pc == @as(c_uint, 437)) {
            afz_mv_ref_9 = @ptrCast(@alignCast(@constCast(&mv_ref_search[@bitCast(@as(isize, @intCast(afz_i_1)))])));
            afz_pc = 436;
            continue;
        }
        if (afz_pc == @as(c_uint, 438)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 439)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 438) else @as(c_int, 437)));
            continue;
        }
        if (afz_pc == @as(c_uint, 440)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 441)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 440) else @as(c_int, 340)));
            continue;
        }
        if (afz_pc == @as(c_uint, 442)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 443)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 442) else @as(c_int, 441)));
            continue;
        }
        if (afz_pc == @as(c_uint, 444)) {
            afz_pc = @bitCast(@as(c_int, if (afz_i_1 < @as(c_int, 2)) @as(c_int, 543) else @as(c_int, 443)));
            continue;
        }
        if (afz_pc == @as(c_uint, 445)) {
            afz_i_1 += 1;
            afz_pc = 444;
            continue;
        }
        if (afz_pc == @as(c_uint, 446)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 447)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 446) else @as(c_int, 445)));
            continue;
        }
        if (afz_pc == @as(c_uint, 448)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 449)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 448) else @as(c_int, 447)));
            continue;
        }
        if (afz_pc == @as(c_uint, 450)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 482) else @as(c_int, 449);
            continue;
        }
        if (afz_pc == @as(c_uint, 451)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 452)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 451) else @as(c_int, 450)));
            continue;
        }
        if (afz_pc == @as(c_uint, 453)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 454)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 453) else @as(c_int, 452)));
            continue;
        }
        if (afz_pc == @as(c_uint, 455)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 456)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 457)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 456) else @as(c_int, 455)));
            continue;
        }
        if (afz_pc == @as(c_uint, 458)) {
            afz_refmv_count_2 += 1;
            afz_pc = 457;
            continue;
        }
        if (afz_pc == @as(c_uint, 459)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 460)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 459) else @as(c_int, 458)));
            continue;
        }
        if (afz_pc == @as(c_uint, 461)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = get_sub_block_mv(afz_candidate_mi_8, 0, afz_mv_ref_7.*.col, block);
            afz_pc = 460;
            continue;
        }
        if (afz_pc == @as(c_uint, 462)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 463)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 462) else @as(c_int, 461)));
            continue;
        }
        if (afz_pc == @as(c_uint, 464)) {
            afz_pc = @bitCast(@as(c_int, if (get_sub_block_mv(afz_candidate_mi_8, 0, afz_mv_ref_7.*.col, block).as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 463) else @as(c_int, 454)));
            continue;
        }
        if (afz_pc == @as(c_uint, 465)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 466)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 465) else @as(c_int, 464)));
            continue;
        }
        if (afz_pc == @as(c_uint, 467)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 468)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 467) else @as(c_int, 452)));
            continue;
        }
        if (afz_pc == @as(c_uint, 469)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 470)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 469) else @as(c_int, 468)));
            continue;
        }
        if (afz_pc == @as(c_uint, 471)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 472)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 473)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 472) else @as(c_int, 471)));
            continue;
        }
        if (afz_pc == @as(c_uint, 474)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 473) else @as(c_int, 452)));
            continue;
        }
        if (afz_pc == @as(c_uint, 475)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 476)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 475) else @as(c_int, 474)));
            continue;
        }
        if (afz_pc == @as(c_uint, 477)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = get_sub_block_mv(afz_candidate_mi_8, 0, afz_mv_ref_7.*.col, block);
            afz_pc = 476;
            continue;
        }
        if (afz_pc == @as(c_uint, 478)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 479)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 478) else @as(c_int, 477)));
            continue;
        }
        if (afz_pc == @as(c_uint, 480)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 466) else @as(c_int, 479)));
            continue;
        }
        if (afz_pc == @as(c_uint, 481)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 482)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 481) else @as(c_int, 480)));
            continue;
        }
        if (afz_pc == @as(c_uint, 483)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 484)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 483) else @as(c_int, 482)));
            continue;
        }
        if (afz_pc == @as(c_uint, 485)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 486)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 485) else @as(c_int, 447)));
            continue;
        }
        if (afz_pc == @as(c_uint, 487)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 488)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 487) else @as(c_int, 486)));
            continue;
        }
        if (afz_pc == @as(c_uint, 489)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 490)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 489) else @as(c_int, 488)));
            continue;
        }
        if (afz_pc == @as(c_uint, 491)) {
            afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 523) else @as(c_int, 490);
            continue;
        }
        if (afz_pc == @as(c_uint, 492)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 493)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 492) else @as(c_int, 491)));
            continue;
        }
        if (afz_pc == @as(c_uint, 494)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 495)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 494) else @as(c_int, 493)));
            continue;
        }
        if (afz_pc == @as(c_uint, 496)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 497)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 498)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 497) else @as(c_int, 496)));
            continue;
        }
        if (afz_pc == @as(c_uint, 499)) {
            afz_refmv_count_2 += 1;
            afz_pc = 498;
            continue;
        }
        if (afz_pc == @as(c_uint, 500)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 501)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 500) else @as(c_int, 499)));
            continue;
        }
        if (afz_pc == @as(c_uint, 502)) {
            mv_ref_list[@bitCast(@as(isize, @intCast(afz_refmv_count_2)))] = get_sub_block_mv(afz_candidate_mi_8, 1, afz_mv_ref_7.*.col, block);
            afz_pc = 501;
            continue;
        }
        if (afz_pc == @as(c_uint, 503)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 504)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 503) else @as(c_int, 502)));
            continue;
        }
        if (afz_pc == @as(c_uint, 505)) {
            afz_pc = @bitCast(@as(c_int, if (get_sub_block_mv(afz_candidate_mi_8, 1, afz_mv_ref_7.*.col, block).as_int != mv_ref_list[@as(c_int, 0)].as_int) @as(c_int, 504) else @as(c_int, 495)));
            continue;
        }
        if (afz_pc == @as(c_uint, 506)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 507)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 506) else @as(c_int, 505)));
            continue;
        }
        if (afz_pc == @as(c_uint, 508)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 509)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 508) else @as(c_int, 493)));
            continue;
        }
        if (afz_pc == @as(c_uint, 510)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 511)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 510) else @as(c_int, 509)));
            continue;
        }
        if (afz_pc == @as(c_uint, 512)) {
            afz_pc = 11;
            continue;
        }
        if (afz_pc == @as(c_uint, 513)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 514)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 513) else @as(c_int, 512)));
            continue;
        }
        if (afz_pc == @as(c_uint, 515)) {
            afz_pc = @bitCast(@as(c_int, if (afz_early_break_6 != 0) @as(c_int, 514) else @as(c_int, 493)));
            continue;
        }
        if (afz_pc == @as(c_uint, 516)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 517)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 516) else @as(c_int, 515)));
            continue;
        }
        if (afz_pc == @as(c_uint, 518)) {
            mv_ref_list[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &afz_refmv_count_2;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = get_sub_block_mv(afz_candidate_mi_8, 1, afz_mv_ref_7.*.col, block);
            afz_pc = 517;
            continue;
        }
        if (afz_pc == @as(c_uint, 519)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 520)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 519) else @as(c_int, 518)));
            continue;
        }
        if (afz_pc == @as(c_uint, 521)) {
            afz_pc = @bitCast(@as(c_int, if (afz_refmv_count_2 != 0) @as(c_int, 507) else @as(c_int, 520)));
            continue;
        }
        if (afz_pc == @as(c_uint, 522)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 523)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 522) else @as(c_int, 521)));
            continue;
        }
        if (afz_pc == @as(c_uint, 524)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 525)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 524) else @as(c_int, 523)));
            continue;
        }
        if (afz_pc == @as(c_uint, 526)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_candidate_mi_8.*.ref_frame[@as(c_int, 1)]) == @as(c_int, ref_frame)) @as(c_int, 525) else @as(c_int, 486)));
            continue;
        }
        if (afz_pc == @as(c_uint, 527)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 528)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 527) else @as(c_int, 526)));
            continue;
        }
        if (afz_pc == @as(c_uint, 529)) {
            afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_candidate_mi_8.*.ref_frame[@as(c_int, 0)]) == @as(c_int, ref_frame)) @as(c_int, 484) else @as(c_int, 528)));
            continue;
        }
        if (afz_pc == @as(c_uint, 530)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 531)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 530) else @as(c_int, 529)));
            continue;
        }
        if (afz_pc == @as(c_uint, 532)) {
            afz_different_ref_found_3 = 1;
            afz_pc = 531;
            continue;
        }
        if (afz_pc == @as(c_uint, 533)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 534)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 533) else @as(c_int, 532)));
            continue;
        }
        if (afz_pc == @as(c_uint, 535)) {
            afz_candidate_mi_8 = xd.*.mi[@bitCast(@as(isize, @intCast(afz_mv_ref_7.*.col + (afz_mv_ref_7.*.row * xd.*.mi_stride))))];
            afz_pc = 534;
            continue;
        }
        if (afz_pc == @as(c_uint, 536)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 537)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 536) else @as(c_int, 535)));
            continue;
        }
        if (afz_pc == @as(c_uint, 538)) {
            afz_pc = @bitCast(@as(c_int, if (is_inside(afz_tile_5, mi_col, mi_row, cm.*.mi_rows, afz_mv_ref_7) != 0) @as(c_int, 537) else @as(c_int, 447)));
            continue;
        }
        if (afz_pc == @as(c_uint, 539)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 540)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 539) else @as(c_int, 538)));
            continue;
        }
        if (afz_pc == @as(c_uint, 541)) {
            afz_mv_ref_7 = @ptrCast(@alignCast(@constCast(&mv_ref_search[@bitCast(@as(isize, @intCast(afz_i_1)))])));
            afz_pc = 540;
            continue;
        }
        if (afz_pc == @as(c_uint, 542)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 543)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 542) else @as(c_int, 541)));
            continue;
        }
        if (afz_pc == @as(c_uint, 544)) {
            afz_i_1 = 0;
            afz_pc = 444;
            continue;
        }
        if (afz_pc == @as(c_uint, 545)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 546)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 545) else @as(c_int, 544)));
            continue;
        }
        if (afz_pc == @as(c_uint, 547)) {
            afz_pc = @bitCast(@as(c_int, if (block >= @as(c_int, 0)) @as(c_int, 546) else @as(c_int, 441)));
            continue;
        }
        if (afz_pc == @as(c_uint, 548)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 549)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 548) else @as(c_int, 547)));
            continue;
        }
        if (afz_pc == @as(c_uint, 550)) {
            afz_i_1 = 0;
            afz_pc = 549;
            continue;
        }
        if (afz_pc == @as(c_uint, 551)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 552)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 551) else @as(c_int, 550)));
            continue;
        }
        if (afz_pc == @as(c_uint, 553)) {
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(mv_ref_list)), 0, @sizeOf(@TypeOf(mv_ref_list.*)) *% @as(c_ulong, 2));
            afz_pc = 552;
            continue;
        }
        if (afz_pc == @as(c_uint, 554)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 555)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 554) else @as(c_int, 553)));
            continue;
        }
        if (afz_pc == @as(c_uint, 556)) {
            afz_early_break_6 = @as(c_int, @intFromBool(@as(c_int, mode) != @as(c_int, 11)));
            afz_pc = 555;
            continue;
        }
        if (afz_pc == @as(c_uint, 557)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 558)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 557) else @as(c_int, 556)));
            continue;
        }
        if (afz_pc == @as(c_uint, 559)) {
            afz_tile_5 = @constCast(&xd.*.tile);
            afz_pc = 558;
            continue;
        }
        if (afz_pc == @as(c_uint, 560)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 561)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 560) else @as(c_int, 559)));
            continue;
        }
        if (afz_pc == @as(c_uint, 562)) {
            afz_prev_frame_mvs_4 = @ptrCast(@alignCast(if (cm.*.use_prev_frame_mvs != 0) @as(?*anyopaque, @ptrCast(@alignCast((cm.*.prev_frame.*.mvs + @as(usize, @bitCast(@as(isize, @intCast(mi_row * cm.*.mi_cols))))) + @as(usize, @bitCast(@as(isize, @intCast(mi_col))))))) else @as(?*anyopaque, null)));
            afz_pc = 561;
            continue;
        }
        if (afz_pc == @as(c_uint, 563)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 564)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 563) else @as(c_int, 562)));
            continue;
        }
        if (afz_pc == @as(c_uint, 565)) {
            afz_different_ref_found_3 = 0;
            afz_pc = 564;
            continue;
        }
        if (afz_pc == @as(c_uint, 566)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 567)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 566) else @as(c_int, 565)));
            continue;
        }
        if (afz_pc == @as(c_uint, 568)) {
            afz_refmv_count_2 = 0;
            afz_pc = 567;
            continue;
        }
        if (afz_pc == @as(c_uint, 569)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 570)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 569) else @as(c_int, 568)));
            continue;
        }
        if (afz_pc == @as(c_uint, 571)) {
            afz_ref_sign_bias_0 = @ptrCast(@alignCast(@constCast(&cm.*.ref_frame_sign_bias)));
            afz_pc = 570;
            continue;
        }
        if (afz_pc == @as(c_uint, 572)) {
            return 0;
        }
        if (afz_pc == @as(c_uint, 573)) {
            afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_vp9_failed() != 0) @as(c_int, 572) else @as(c_int, 571)));
            continue;
        }
        antfly_codec_afz_trap();
    }
    return undefined;
}
pub fn append_sub8x8_mvs_for_idx(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, mv_ref_search: [*c]const POSITION, arg_b_mode: PREDICTION_MODE, arg_block: c_int, arg_ref: c_int, arg_mi_row: c_int, arg_mi_col: c_int, arg_best_sub8x8: [*c]int_mv) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    _ = &mv_ref_search;
    var b_mode = arg_b_mode;
    _ = &b_mode;
    var block = arg_block;
    _ = &block;
    var ref = arg_ref;
    _ = &ref;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var best_sub8x8 = arg_best_sub8x8;
    _ = &best_sub8x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mv_list: [2]int_mv = undefined;
    _ = &mv_list;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi: [*c]MODE_INFO = xd.*.mi[@as(c_int, 0)];
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var bmi: [*c]b_mode_info = @ptrCast(@alignCast(&mi.*.bmi));
    _ = &bmi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var n: c_int = undefined;
    _ = &n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var refmv_count: c_int = undefined;
    _ = &refmv_count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    blk: {
        switch (block) {
            @as(c_int, 0) => {
                refmv_count = dec_find_mv_refs(cm, xd, b_mode, mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))], mv_ref_search, @ptrCast(@alignCast(&mv_list)), mi_row, mi_col, block);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                best_sub8x8.*.as_int = mv_list[@bitCast(@as(isize, @intCast(refmv_count - @as(c_int, 1))))].as_int;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                break :blk;
            },
            @as(c_int, 1), @as(c_int, 2) => {
                if (@as(c_int, b_mode) == @as(c_int, 10)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    best_sub8x8.*.as_int = bmi[@as(c_int, 0)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = dec_find_mv_refs(cm, xd, b_mode, mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))], mv_ref_search, @ptrCast(@alignCast(&mv_list)), mi_row, mi_col, block);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    best_sub8x8.*.as_int = 0;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        n = 0;
                        while (n < @as(c_int, 2)) : (n += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (bmi[@as(c_int, 0)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int != mv_list[@bitCast(@as(isize, @intCast(n)))].as_int) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                best_sub8x8.*.as_int = mv_list[@bitCast(@as(isize, @intCast(n)))].as_int;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                break;
                            }
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
                break :blk;
            },
            @as(c_int, 3) => {
                if (@as(c_int, b_mode) == @as(c_int, 10)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    best_sub8x8.*.as_int = bmi[@as(c_int, 2)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    best_sub8x8.*.as_int = 0;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (bmi[@as(c_int, 2)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int != bmi[@as(c_int, 1)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        best_sub8x8.*.as_int = bmi[@as(c_int, 1)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (bmi[@as(c_int, 2)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int != bmi[@as(c_int, 0)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        best_sub8x8.*.as_int = bmi[@as(c_int, 0)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = dec_find_mv_refs(cm, xd, b_mode, mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))], mv_ref_search, @ptrCast(@alignCast(&mv_list)), mi_row, mi_col, block);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        n = 0;
                        while (n < @as(c_int, 2)) : (n += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (bmi[@as(c_int, 2)].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_int != mv_list[@bitCast(@as(isize, @intCast(n)))].as_int) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                best_sub8x8.*.as_int = mv_list[@bitCast(@as(isize, @intCast(n)))].as_int;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                break;
                            }
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
                break :blk;
            },
            else => {
                _ = @as(c_int, 0);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            },
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_mode_context(arg_cm: [*c]const VP9_COMMON, arg_xd: [*c]const MACROBLOCKD, mv_ref_search: [*c]const POSITION, arg_mi_row: c_int, arg_mi_col: c_int) callconv(.c) u8 {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    _ = &mv_ref_search;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var context_counter: c_int = 0;
    _ = &context_counter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const tile: [*c]const TileInfo = &xd.*.tile;
    _ = &tile;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const mv_ref: [*c]const POSITION = &mv_ref_search[@bitCast(@as(isize, @intCast(i)))];
            _ = &mv_ref;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (is_inside(tile, mi_col, mi_row, cm.*.mi_rows, mv_ref) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const candidate: [*c]const MODE_INFO = xd.*.mi[@bitCast(@as(isize, @intCast(mv_ref.*.col + (mv_ref.*.row * xd.*.mi_stride))))];
                _ = &candidate;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                context_counter += mode_2_counter[candidate.*.mode];
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
    return @bitCast(@as(i8, @truncate(counter_to_context[@bitCast(@as(isize, @intCast(context_counter)))])));
}
pub fn read_inter_block_mode_info(pbi: [*c]VP9Decoder, xd: [*c]MACROBLOCKD, mi: [*c]MODE_INFO, arg_mi_row: c_int, arg_mi_col: c_int, arg_r: [*c]vpx_reader) callconv(.c) void {
    _ = &pbi;
    _ = &xd;
    _ = &mi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bsize: BLOCK_SIZE = mi.*.sb_type;
    _ = &bsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const allow_hp: c_int = cm.*.allow_high_precision_mv;
    _ = &allow_hp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var best_ref_mvs: [2]int_mv = [2]int_mv{
        int_mv{
            .as_int = 0,
        },
        int_mv{
            .as_int = 0,
        },
    };
    _ = &best_ref_mvs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref: c_int = undefined;
    _ = &ref;
    var is_compound: c_int = undefined;
    _ = &is_compound;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var inter_mode_ctx: u8 = undefined;
    _ = &inter_mode_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mv_ref_search: [*c]const POSITION = @ptrCast(@alignCast(&mv_ref_blocks[bsize]));
    _ = &mv_ref_search;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    read_ref_frames(cm, xd, r, mi.*.segment_id, @ptrCast(@alignCast(&mi.*.ref_frame)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    is_compound = has_second_ref(mi);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    inter_mode_ctx = get_mode_context(cm, xd, mv_ref_search, mi_row, mi_col);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (segfeature_active(&cm.*.seg, mi.*.segment_id, SEG_LVL_SKIP) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        mi.*.mode = 12;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, bsize) < @as(c_int, 3)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(xd.*.error_info)), VPX_CODEC_UNSUP_BITSTREAM);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, bsize) >= @as(c_int, 3)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mi.*.mode = read_inter_mode(cm, xd, r, inter_mode_ctx);
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
    mi.*.interp_filter = @bitCast(@as(i8, @truncate(if (@as(c_int, cm.*.interp_filter) == @as(c_int, 4)) @as(c_int, read_switchable_interp_filter(cm, xd, r)) else @as(c_int, cm.*.interp_filter))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, bsize) < @as(c_int, 3)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const num_4x4_w: c_int = @as(c_int, 1) << @intCast(@as(c_int, xd.*.bmode_blocks_wl));
        _ = &num_4x4_w;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const num_4x4_h: c_int = @as(c_int, 1) << @intCast(@as(c_int, xd.*.bmode_blocks_hl));
        _ = &num_4x4_h;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var idx: c_int = undefined;
        _ = &idx;
        var idy: c_int = undefined;
        _ = &idy;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var b_mode: PREDICTION_MODE = undefined;
        _ = &b_mode;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var got_mv_refs_for_new: c_int = 0;
        _ = &got_mv_refs_for_new;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var best_sub8x8: [2]int_mv = undefined;
        _ = &best_sub8x8;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const invalid_mv: u32 = 2147516416;
        _ = &invalid_mv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        best_sub8x8[@as(c_int, 1)].as_int = invalid_mv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            idy = 0;
            while (idy < @as(c_int, 2)) : (idy += num_4x4_h) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    idx = 0;
                    while (idx < @as(c_int, 2)) : (idx += num_4x4_w) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const j: c_int = (idy * @as(c_int, 2)) + idx;
                        _ = &j;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        b_mode = read_inter_mode(cm, xd, r, inter_mode_ctx);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((@as(c_int, b_mode) == @as(c_int, 10)) or (@as(c_int, b_mode) == @as(c_int, 11))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            {
                                ref = 0;
                                while (ref < (@as(c_int, 1) + is_compound)) : (ref += 1) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    append_sub8x8_mvs_for_idx(cm, xd, mv_ref_search, b_mode, j, ref, mi_row, mi_col, &best_sub8x8[@bitCast(@as(isize, @intCast(ref)))]);
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
                            if ((@as(c_int, b_mode) == @as(c_int, 13)) and !(got_mv_refs_for_new != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                {
                                    ref = 0;
                                    while (ref < (@as(c_int, 1) + is_compound)) : (ref += 1) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        var tmp_mvs: [2]int_mv = undefined;
                                        _ = &tmp_mvs;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        const frame: MV_REFERENCE_FRAME = mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))];
                                        _ = &frame;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        _ = dec_find_mv_refs(cm, xd, 13, frame, mv_ref_search, @ptrCast(@alignCast(&tmp_mvs)), mi_row, mi_col, -@as(c_int, 1));
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        lower_mv_precision(&tmp_mvs[@as(c_int, 0)].as_mv, allow_hp);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        best_ref_mvs[@bitCast(@as(isize, @intCast(ref)))] = tmp_mvs[@as(c_int, 0)];
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        got_mv_refs_for_new = 1;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
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
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (!(assign_mv(cm, xd, b_mode, @ptrCast(@alignCast(&mi.*.bmi[@bitCast(@as(isize, @intCast(j)))].as_mv)), @ptrCast(@alignCast(&best_ref_mvs)), @ptrCast(@alignCast(&best_sub8x8)), is_compound, allow_hp, r) != 0)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            xd.*.corrupted |= 1;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (num_4x4_h == @as(c_int, 2)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            mi.*.bmi[@bitCast(@as(isize, @intCast(j + @as(c_int, 2))))] = mi.*.bmi[@bitCast(@as(isize, @intCast(j)))];
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (num_4x4_w == @as(c_int, 2)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            mi.*.bmi[@bitCast(@as(isize, @intCast(j + @as(c_int, 1))))] = mi.*.bmi[@bitCast(@as(isize, @intCast(j)))];
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
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        mi.*.mode = b_mode;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        copy_mv_pair(@ptrCast(@alignCast(&mi.*.mv)), @ptrCast(@alignCast(&mi.*.bmi[@as(c_int, 3)].as_mv)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, mi.*.mode) != @as(c_int, 12)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                ref = 0;
                while (ref < (@as(c_int, 1) + is_compound)) : (ref += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var tmp_mvs: [2]int_mv = undefined;
                    _ = &tmp_mvs;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const frame: MV_REFERENCE_FRAME = mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))];
                    _ = &frame;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var refmv_count: c_int = dec_find_mv_refs(cm, xd, mi.*.mode, frame, mv_ref_search, @ptrCast(@alignCast(&tmp_mvs)), mi_row, mi_col, -@as(c_int, 1));
                    _ = &refmv_count;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    lower_mv_precision(&tmp_mvs[@bitCast(@as(isize, @intCast(refmv_count - @as(c_int, 1))))].as_mv, allow_hp);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    best_ref_mvs[@bitCast(@as(isize, @intCast(ref)))] = tmp_mvs[@bitCast(@as(isize, @intCast(refmv_count - @as(c_int, 1))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
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
        xd.*.corrupted |= @as(c_int, @intFromBool(!(assign_mv(cm, xd, mi.*.mode, @ptrCast(@alignCast(&mi.*.mv)), @ptrCast(@alignCast(&best_ref_mvs)), @ptrCast(@alignCast(&best_ref_mvs)), is_compound, allow_hp, r) != 0)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_inter_frame_mode_info(pbi: [*c]VP9Decoder, xd: [*c]MACROBLOCKD, arg_mi_row: c_int, arg_mi_col: c_int, arg_r: [*c]vpx_reader, arg_x_mis: c_int, arg_y_mis: c_int) callconv(.c) void {
    _ = &pbi;
    _ = &xd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var r = arg_r;
    _ = &r;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi: [*c]MODE_INFO = xd.*.mi[@as(c_int, 0)];
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var inter_block: c_int = undefined;
    _ = &inter_block;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.segment_id = @truncate(read_inter_segment_id(cm, xd, mi_row, mi_col, r, x_mis, y_mis));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.skip = @truncate(read_skip(cm, xd, mi.*.segment_id, r));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    inter_block = read_is_inter_block(cm, xd, mi.*.segment_id, r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi.*.tx_size = read_tx_size(cm, xd, @as(c_int, @intFromBool(!(@as(c_int, mi.*.skip) != 0) or !(inter_block != 0))), r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (inter_block != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        read_inter_block_mode_info(pbi, xd, mi, mi_row, mi_col, r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        read_intra_block_mode_info(cm, xd, mi, r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn copy_ref_frame_pair(arg_dst: [*c]MV_REFERENCE_FRAME, arg_src: [*c]const MV_REFERENCE_FRAME) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var src = arg_src;
    _ = &src;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(src)), @sizeOf(@TypeOf(dst.*)) *% @as(c_ulong, 2));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_read_mode_info(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_mi_col: c_int, arg_x_mis: c_int, arg_y_mis: c_int) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: [*c]vpx_reader = &twd.*.bit_reader;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi: [*c]MODE_INFO = xd.*.mi[@as(c_int, 0)];
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var frame_mvs: [*c]MV_REF = (cm.*.cur_frame.*.mvs + @as(usize, @bitCast(@as(isize, @intCast(mi_row * cm.*.mi_cols))))) + @as(usize, @bitCast(@as(isize, @intCast(mi_col))));
    _ = &frame_mvs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var w: c_int = undefined;
    _ = &w;
    var h: c_int = undefined;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (frame_is_intra_only(cm) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        read_intra_frame_mode_info(cm, xd, mi_row, mi_col, r, x_mis, y_mis);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var mi_ref_frame: [2]MV_REFERENCE_FRAME = undefined;
        _ = &mi_ref_frame;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var mi_mv: [2]int_mv = undefined;
        _ = &mi_mv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        read_inter_frame_mode_info(pbi, xd, mi_row, mi_col, r, x_mis, y_mis);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        copy_ref_frame_pair(@ptrCast(@alignCast(&mi_ref_frame)), @ptrCast(@alignCast(&mi.*.ref_frame)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        copy_mv_pair(@ptrCast(@alignCast(&mi_mv)), @ptrCast(@alignCast(&mi.*.mv)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            h = 0;
            while (h < y_mis) : (h += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    w = 0;
                    while (w < x_mis) : (w += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const mv_1: [*c]MV_REF = frame_mvs + @as(usize, @bitCast(@as(isize, @intCast(w))));
                        _ = &mv_1;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        copy_ref_frame_pair(@ptrCast(@alignCast(&mv_1.*.ref_frame)), @ptrCast(@alignCast(&mi_ref_frame)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        copy_mv_pair(@ptrCast(@alignCast(&mv_1.*.mv)), @ptrCast(@alignCast(&mi_mv)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                frame_mvs += @as(usize, @bitCast(@as(isize, @intCast(cm.*.mi_cols))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
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
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
