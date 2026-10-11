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
pub const tran_low_t = common.tran_low_t;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const VPX_BITS_10 = common.VPX_BITS_10;
pub const VPX_BITS_12 = common.VPX_BITS_12;
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
pub const PLANE_TYPE = common.PLANE_TYPE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const vpx_prob = common.vpx_prob;
pub extern const antfly_vp9_vpx_norm: [256]u8 align(16);
pub extern const antfly_vp9_vp9_cat1_prob: [1]u8 align(16);
pub extern const antfly_vp9_vp9_cat2_prob: [2]u8 align(16);
pub extern const antfly_vp9_vp9_cat3_prob: [3]u8 align(16);
pub extern const antfly_vp9_vp9_cat4_prob: [4]u8 align(16);
pub extern const antfly_vp9_vp9_cat5_prob: [5]u8 align(16);
pub extern const antfly_vp9_vp9_cat6_prob: [14]u8 align(16);
pub extern const antfly_vp9_vp9_cat6_prob_high12: [18]u8 align(16);
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
pub extern const antfly_vp9_vp9_coefband_trans_8x8plus: [1024]u8 align(16);
pub extern const antfly_vp9_vp9_coefband_trans_4x4: [16]u8 align(16);
pub fn get_band_translate(arg_tx_size: TX_SIZE) callconv(.c) [*c]const u8 {
    var tx_size = arg_tx_size;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return if (@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))))) @as([*c]const u8, @ptrCast(@alignCast(&antfly_vp9_vp9_coefband_trans_4x4))) else @as([*c]const u8, @ptrCast(@alignCast(&antfly_vp9_vp9_coefband_trans_8x8plus)));
}
pub extern var antfly_vp9_vp9_pareto8_full: [255][8]vpx_prob;
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
pub fn get_plane_type(arg_plane: c_int) callconv(.c) PLANE_TYPE {
    var plane = arg_plane;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @intCast(@as(c_int, @intFromBool(plane > @as(c_int, 0))));
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
pub const VP9LfSync = struct_VP9LfSyncData;
pub const struct_ScanOrder = extern struct {
    scan: [*c]const i16 = null,
    iscan: [*c]const i16 = null,
    neighbors: [*c]const i16 = null,
};
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
pub const ScanOrder = struct_ScanOrder;
pub fn get_coef_context(arg_neighbors: [*c]const i16, arg_token_cache: [*c]const u8, arg_c: c_int) callconv(.c) c_int {
    var neighbors = arg_neighbors;
    _ = &neighbors;
    var token_cache = arg_token_cache;
    _ = &token_cache;
    var c = arg_c;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ((@as(c_int, 1) + @as(c_int, token_cache[@bitCast(@as(isize, @intCast(neighbors[@bitCast(@as(isize, @intCast((@as(c_int, 2) * c) + @as(c_int, 0))))])))])) + @as(c_int, token_cache[@bitCast(@as(isize, @intCast(neighbors[@bitCast(@as(isize, @intCast((@as(c_int, 2) * c) + @as(c_int, 1))))])))])) >> @intCast(@as(c_int, 1));
}
pub fn read_bool(arg_r: [*c]vpx_reader, arg_prob: c_int, arg_value: [*c]BD_VALUE, arg_count: [*c]c_int, arg_range: [*c]c_uint) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var prob = arg_prob;
    _ = &prob;
    var value = arg_value;
    _ = &value;
    var count = arg_count;
    _ = &count;
    var range = arg_range;
    _ = &range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const split: c_uint = ((range.* *% @as(c_uint, @bitCast(@as(c_int, prob)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 256) - prob)))) >> @intCast(@as(c_uint, 8));
    _ = &split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const bigsplit: BD_VALUE = @as(BD_VALUE, split) << @intCast(@as(BD_VALUE, @bitCast(@as(c_long, (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)) - @as(c_int, 8)))));
    _ = &bigsplit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (count.* < @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.value = value.*;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.count = count.*;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_reader_fill(r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value.* = r.*.value;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        count.* = r.*.count;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (value.* >= bigsplit) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range.* = range.* -% split;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value.* = value.* -% bigsplit;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const shift: c_int = antfly_vp9_vpx_norm[range.*];
            _ = &shift;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            range.* <<= @intCast(@as(c_uint, @bitCast(@as(c_int, shift))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            value.* <<= @intCast(@as(BD_VALUE, @bitCast(@as(c_long, shift))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            count.* -= shift;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    range.* = split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const shift: c_int = antfly_vp9_vpx_norm[range.*];
        _ = &shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range.* <<= @intCast(@as(c_uint, @bitCast(@as(c_int, shift))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value.* <<= @intCast(@as(BD_VALUE, @bitCast(@as(c_long, shift))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        count.* -= shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 0;
}
pub fn read_coeff(arg_r: [*c]vpx_reader, arg_probs: [*c]const vpx_prob, arg_n: c_int, arg_value: [*c]BD_VALUE, arg_count: [*c]c_int, arg_range: [*c]c_uint) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var probs = arg_probs;
    _ = &probs;
    var n = arg_n;
    _ = &n;
    var value = arg_value;
    _ = &value;
    var count = arg_count;
    _ = &count;
    var range = arg_range;
    _ = &range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    var val: c_int = 0;
    _ = &val;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        i = 0;
        while (i < n) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            val = (val << @intCast(@as(c_int, 1))) | read_bool(r, probs[@bitCast(@as(isize, @intCast(i)))], value, count, range);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    return val;
}
pub fn decode_coefs(arg_xd: [*c]const MACROBLOCKD, arg_type: PLANE_TYPE, arg_dqcoeff: [*c]tran_low_t, arg_tx_size: TX_SIZE, arg_dq: [*c]const i16, arg_ctx: c_int, arg_scan: [*c]const i16, arg_nb: [*c]const i16, arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
    var @"type" = arg_type;
    _ = &@"type";
    var dqcoeff = arg_dqcoeff;
    _ = &dqcoeff;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    var dq = arg_dq;
    _ = &dq;
    var ctx = arg_ctx;
    _ = &ctx;
    var scan = arg_scan;
    _ = &scan;
    var nb = arg_nb;
    _ = &nb;
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
    const max_eob: c_int = @as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)));
    _ = &max_eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const fc: [*c]const FRAME_CONTEXT = xd.*.fc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const ref: c_int = is_inter_block(xd.*.mi[@as(c_int, 0)]);
    _ = &ref;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var band: c_int = undefined;
    _ = &band;
    var c: c_int = 0;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var coef_probs: [*c][6][3]vpx_prob = @ptrCast(@alignCast(@constCast(&fc.*.coef_probs[tx_size][@"type"][@bitCast(@as(isize, @intCast(ref)))])));
    _ = &coef_probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var prob: [*c]const vpx_prob = undefined;
    _ = &prob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var coef_counts: [*c][6][4]c_uint = undefined;
    _ = &coef_counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var eob_branch_count: [*c][6]c_uint = undefined;
    _ = &eob_branch_count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var token_cache: [1024]u8 = undefined;
    _ = &token_cache;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var band_translate: [*c]const u8 = get_band_translate(tx_size);
    _ = &band_translate;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const dq_shift: c_int = @as(c_int, @intFromBool(@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))));
    _ = &dq_shift;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var v: c_int = undefined;
    _ = &v;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var dqv: i16 = dq[@as(c_int, 0)];
    _ = &dqv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cat6_prob: [*c]const u8 = if (xd.*.bd == VPX_BITS_12) @as([*c]const u8, @ptrCast(@alignCast(&antfly_vp9_vp9_cat6_prob_high12))) else if (xd.*.bd == VPX_BITS_10) @as([*c]const u8, @ptrCast(@alignCast(&antfly_vp9_vp9_cat6_prob_high12))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))) else @as([*c]const u8, @ptrCast(@alignCast(&antfly_vp9_vp9_cat6_prob)));
    _ = &cat6_prob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cat6_bits: c_int = if (xd.*.bd == VPX_BITS_12) @as(c_int, 18) else if (xd.*.bd == VPX_BITS_10) @as(c_int, 16) else @as(c_int, 14);
    _ = &cat6_bits;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var value: BD_VALUE = r.*.value;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var range: c_uint = r.*.range;
    _ = &range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var count: c_int = r.*.count;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        coef_counts = @ptrCast(@alignCast(&counts.*.coef[tx_size][@"type"][@bitCast(@as(isize, @intCast(ref)))]));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        eob_branch_count = @ptrCast(@alignCast(&counts.*.eob_branch[tx_size][@"type"][@bitCast(@as(isize, @intCast(ref)))]));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (c < max_eob) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var val: c_int = -@as(c_int, 1);
        _ = &val;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        band = (blk: {
            const ref_1 = &band_translate;
            const tmp = ref_1.*;
            ref_1.* += 1;
            break :blk tmp;
        }).*;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        prob = @ptrCast(@alignCast(&coef_probs[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))]));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (counts != null) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            eob_branch_count[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))] +%= 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (!(read_bool(r, prob[@as(c_int, 0)], &value, &count, &range) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (counts != null) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    coef_counts[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))][@as(c_int, 3)] +%= 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            break;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        while (!(read_bool(r, prob[@as(c_int, 1)], &value, &count, &range) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (counts != null) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    coef_counts[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))][@as(c_int, 0)] +%= 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            dqv = dq[@as(c_int, 1)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            token_cache[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            c += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (c >= max_eob) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                r.*.value = value;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                r.*.range = range;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                r.*.count = count;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return c;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx = get_coef_context(nb, @ptrCast(@alignCast(&token_cache)), c);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            band = (blk: {
                const ref_1 = &band_translate;
                const tmp = ref_1.*;
                ref_1.* += 1;
                break :blk tmp;
            }).*;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            prob = @ptrCast(@alignCast(&coef_probs[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))]));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (read_bool(r, prob[@as(c_int, 2)], &value, &count, &range) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var p: [*c]const vpx_prob = @ptrCast(@alignCast(&antfly_vp9_vp9_pareto8_full[@bitCast(@as(isize, @intCast(@as(c_int, prob[@as(c_int, 2)]) - @as(c_int, 1))))]));
            _ = &p;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (counts != null) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    coef_counts[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))][@as(c_int, 2)] +%= 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (read_bool(r, p[@as(c_int, 0)], &value, &count, &range) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (read_bool(r, p[@as(c_int, 3)], &value, &count, &range) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    token_cache[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = 5;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (read_bool(r, p[@as(c_int, 5)], &value, &count, &range) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (read_bool(r, p[@as(c_int, 7)], &value, &count, &range) != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            val = @as(c_int, 67) + read_coeff(r, cat6_prob, cat6_bits, &value, &count, &range);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            val = @as(c_int, 35) + read_coeff(r, @ptrCast(@alignCast(&antfly_vp9_vp9_cat5_prob)), 5, &value, &count, &range);
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
                        if (read_bool(r, p[@as(c_int, 6)], &value, &count, &range) != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            val = @as(c_int, 19) + read_coeff(r, @ptrCast(@alignCast(&antfly_vp9_vp9_cat4_prob)), 4, &value, &count, &range);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            val = @as(c_int, 11) + read_coeff(r, @ptrCast(@alignCast(&antfly_vp9_vp9_cat3_prob)), 3, &value, &count, &range);
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
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    token_cache[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = 4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (read_bool(r, p[@as(c_int, 4)], &value, &count, &range) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        val = @as(c_int, 7) + read_coeff(r, @ptrCast(@alignCast(&antfly_vp9_vp9_cat2_prob)), 2, &value, &count, &range);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        val = @as(c_int, 5) + read_coeff(r, @ptrCast(@alignCast(&antfly_vp9_vp9_cat1_prob)), 1, &value, &count, &range);
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
                v = @truncate((@as(i64, val) * @as(i64, @as(c_int, dqv))) >> @intCast(@as(i64, dq_shift)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (read_bool(r, p[@as(c_int, 1)], &value, &count, &range) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    token_cache[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = 3;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    v = ((@as(c_int, 3) + read_bool(r, p[@as(c_int, 2)], &value, &count, &range)) * @as(c_int, dqv)) >> @intCast(dq_shift);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    token_cache[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = 2;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    v = (@as(c_int, 2) * @as(c_int, dqv)) >> @intCast(dq_shift);
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
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (counts != null) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    coef_counts[@bitCast(@as(isize, @intCast(band)))][@bitCast(@as(isize, @intCast(ctx)))][@as(c_int, 1)] +%= 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            token_cache[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            v = @as(c_int, dqv) >> @intCast(dq_shift);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (read_bool(r, 128, &value, &count, &range) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            dqcoeff[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = -v;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            dqcoeff[@bitCast(@as(isize, @intCast(scan[@bitCast(@as(isize, @intCast(c)))])))] = v;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        c += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx = get_coef_context(nb, @ptrCast(@alignCast(&token_cache)), c);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        dqv = dq[@as(c_int, 1)];
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
    r.*.range = range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.count = count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return c;
}
pub fn get_ctx_shift(arg_xd: [*c]MACROBLOCKD, arg_ctx_shift_a: [*c]c_int, arg_ctx_shift_l: [*c]c_int, arg_x: c_int, arg_y: c_int, arg_tx_size_in_blocks: c_uint) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var ctx_shift_a = arg_ctx_shift_a;
    _ = &ctx_shift_a;
    var ctx_shift_l = arg_ctx_shift_l;
    _ = &ctx_shift_l;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var tx_size_in_blocks = arg_tx_size_in_blocks;
    _ = &tx_size_in_blocks;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (xd.*.max_blocks_wide != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((tx_size_in_blocks +% @as(c_uint, @bitCast(@as(c_int, x)))) > xd.*.max_blocks_wide) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ctx_shift_a.* = @bitCast(@as(c_uint, @truncate((tx_size_in_blocks -% (xd.*.max_blocks_wide -% @as(c_uint, @bitCast(@as(c_int, x))))) *% @as(c_uint, 8))));
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
    if (xd.*.max_blocks_high != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((tx_size_in_blocks +% @as(c_uint, @bitCast(@as(c_int, y)))) > xd.*.max_blocks_high) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ctx_shift_l.* = @bitCast(@as(c_uint, @truncate((tx_size_in_blocks -% (xd.*.max_blocks_high -% @as(c_uint, @bitCast(@as(c_int, y))))) *% @as(c_uint, 8))));
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
pub export fn antfly_vp9_vp9_decode_block_tokens(arg_twd: [*c]TileWorkerData, arg_plane: c_int, arg_sc: [*c]const ScanOrder, arg_x: c_int, arg_y: c_int, arg_tx_size: TX_SIZE, arg_seg_id: c_int) c_int {
    var twd = arg_twd;
    _ = &twd;
    var plane = arg_plane;
    _ = &plane;
    var sc = arg_sc;
    _ = &sc;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    var seg_id = arg_seg_id;
    _ = &seg_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var r: [*c]vpx_reader = &twd.*.bit_reader;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const dequant: [*c]const i16 = @ptrCast(@alignCast(&pd.*.seg_dequant[@bitCast(@as(isize, @intCast(seg_id)))]));
    _ = &dequant;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var eob: c_int = undefined;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var a: [*c]ENTROPY_CONTEXT = pd.*.above_context + @as(usize, @bitCast(@as(isize, @intCast(x))));
    _ = &a;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var l: [*c]ENTROPY_CONTEXT = pd.*.left_context + @as(usize, @bitCast(@as(isize, @intCast(y))));
    _ = &l;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ctx: c_int = undefined;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ctx_shift_a: c_int = 0;
    _ = &ctx_shift_a;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ctx_shift_l: c_int = 0;
    _ = &ctx_shift_l;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    blk: {
        switch (@as(c_int, tx_size)) {
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))) => {
                ctx = @as(c_int, @intFromBool(@as(c_int, a[@as(c_int, 0)]) != @as(c_int, 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx += @as(c_int, @intFromBool(@as(c_int, l[@as(c_int, 0)]) != @as(c_int, 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                eob = decode_coefs(xd, get_plane_type(plane), pd.*.dqcoeff, tx_size, dequant, ctx, sc.*.scan, sc.*.neighbors, r);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                a[@as(c_int, 0)] = blk_1: {
                    const tmp = @as(ENTROPY_CONTEXT, @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(eob > @as(c_int, 0)))))));
                    l[@as(c_int, 0)] = tmp;
                    break :blk_1 tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break :blk;
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                get_ctx_shift(xd, &ctx_shift_a, &ctx_shift_l, x, y, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))))))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx = @as(c_int, @intFromBool(!!(@as(c_int, @as([*c]const u16, @ptrCast(@alignCast(a))).*) != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx += @as(c_int, @intFromBool(!!(@as(c_int, @as([*c]const u16, @ptrCast(@alignCast(l))).*) != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                eob = decode_coefs(xd, get_plane_type(plane), pd.*.dqcoeff, tx_size, dequant, ctx, sc.*.scan, sc.*.neighbors, r);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                @as([*c]u16, @ptrCast(@alignCast(a))).* = @bitCast(@as(c_short, @truncate((@as(c_int, @intFromBool(eob > @as(c_int, 0))) * @as(c_int, 257)) >> @intCast(ctx_shift_a))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                @as([*c]u16, @ptrCast(@alignCast(l))).* = @bitCast(@as(c_short, @truncate((@as(c_int, @intFromBool(eob > @as(c_int, 0))) * @as(c_int, 257)) >> @intCast(ctx_shift_l))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break :blk;
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                get_ctx_shift(xd, &ctx_shift_a, &ctx_shift_l, x, y, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))))))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx = @as(c_int, @intFromBool(!!(@as([*c]const u32, @ptrCast(@alignCast(a))).* != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx += @as(c_int, @intFromBool(!!(@as([*c]const u32, @ptrCast(@alignCast(l))).* != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                eob = decode_coefs(xd, get_plane_type(plane), pd.*.dqcoeff, tx_size, dequant, ctx, sc.*.scan, sc.*.neighbors, r);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                @as([*c]u32, @ptrCast(@alignCast(a))).* = @bitCast(@as(c_int, (@as(c_int, @intFromBool(eob > @as(c_int, 0))) * @as(c_int, 16843009)) >> @intCast(ctx_shift_a)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                @as([*c]u32, @ptrCast(@alignCast(l))).* = @bitCast(@as(c_int, (@as(c_int, @intFromBool(eob > @as(c_int, 0))) * @as(c_int, 16843009)) >> @intCast(ctx_shift_l)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break :blk;
            },
            @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                get_ctx_shift(xd, &ctx_shift_a, &ctx_shift_l, x, y, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx = @as(c_int, @intFromBool(!!(@as([*c]const u64, @ptrCast(@alignCast(a))).* != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx += @as(c_int, @intFromBool(!!(@as([*c]const u64, @ptrCast(@alignCast(l))).* != 0)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                eob = decode_coefs(xd, get_plane_type(plane), pd.*.dqcoeff, tx_size, dequant, ctx, sc.*.scan, sc.*.neighbors, r);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                @as([*c]u64, @ptrCast(@alignCast(a))).* = (@as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, @intFromBool(eob > @as(c_int, 0)))))) *% @as(c_ulonglong, 72340172838076673)) >> @intCast(@as(c_ulonglong, @bitCast(@as(c_longlong, ctx_shift_a))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                @as([*c]u64, @ptrCast(@alignCast(l))).* = (@as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, @intFromBool(eob > @as(c_int, 0)))))) *% @as(c_ulonglong, 72340172838076673)) >> @intCast(@as(c_ulonglong, @bitCast(@as(c_longlong, ctx_shift_l))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break :blk;
            },
            else => {
                _ = @as(c_int, 0);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                eob = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break :blk;
            },
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return eob;
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
