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
pub extern fn antfly_vp9_vpx_highbd_lpf_horizontal_16_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_horizontal_16_dual_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_horizontal_4_dual_c(s: [*c]u16, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_horizontal_8_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_horizontal_8_dual_c(s: [*c]u16, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_vertical_16_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_vertical_16_dual_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_vertical_4_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_vertical_4_dual_c(s: [*c]u16, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_vertical_8_c(s: [*c]u16, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_lpf_vertical_8_dual_c(s: [*c]u16, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8, bd: c_int) void;
pub extern fn antfly_vp9_vpx_lpf_horizontal_16_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_horizontal_16_dual_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_horizontal_4_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_horizontal_4_dual_c(s: [*c]u8, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_horizontal_8_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_horizontal_8_dual_c(s: [*c]u8, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_vertical_16_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_vertical_16_dual_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_vertical_4_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_vertical_4_dual_c(s: [*c]u8, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_vertical_8_c(s: [*c]u8, pitch: c_int, blimit: [*c]const u8, limit: [*c]const u8, thresh: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_lpf_vertical_8_dual_c(s: [*c]u8, pitch: c_int, blimit0: [*c]const u8, limit0: [*c]const u8, thresh0: [*c]const u8, blimit1: [*c]const u8, limit1: [*c]const u8, thresh1: [*c]const u8) void;
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
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE = common.TX_MODE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub extern const antfly_vp9_num_8x8_blocks_wide_lookup: [13]u8;
pub extern const antfly_vp9_num_8x8_blocks_high_lookup: [13]u8;
pub extern const antfly_vp9_num_4x4_blocks_high_lookup: [13]u8;
pub extern const antfly_vp9_num_4x4_blocks_wide_lookup: [13]u8;
pub extern var antfly_vp9_uv_txsize_lookup: [13][4][2][2]TX_SIZE;
pub const vpx_prob = common.vpx_prob;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
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
pub const SEG_LVL_ALT_LF = common.SEG_LVL_ALT_LF;
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
pub fn get_uv_tx_size(arg_mi: [*c]const MODE_INFO, arg_pd: [*c]const struct_macroblockd_plane) callconv(.c) TX_SIZE {
    var mi = arg_mi;
    _ = &mi;
    var pd = arg_pd;
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return antfly_vp9_uv_txsize_lookup[mi.*.sb_type][mi.*.tx_size][@bitCast(@as(isize, @intCast(pd.*.subsampling_x)))][@bitCast(@as(isize, @intCast(pd.*.subsampling_y)))];
}
pub const LF_PATH_420 = common.LF_PATH_420;
pub const LF_PATH_444 = common.LF_PATH_444;
pub const LF_PATH_SLOW = common.LF_PATH_SLOW;
pub const enum_lf_path = common.enum_lf_path;
pub const loop_filter_thresh = common.loop_filter_thresh;
pub const loop_filter_info_n = extern struct {
    lfthr: [64]loop_filter_thresh = @import("std").mem.zeroes([64]loop_filter_thresh),
    lvl: [8][4][2]u8 = @import("std").mem.zeroes([8][4][2]u8),
};
pub const LOOP_FILTER_MASK = common.LOOP_FILTER_MASK;
pub fn get_lfm(arg_lf: [*c]const struct_loopfilter, mi_row: c_int, mi_col: c_int) callconv(.c) [*c]LOOP_FILTER_MASK {
    var lf = arg_lf;
    _ = &lf;
    _ = &mi_row;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return &lf.*.lfm[@bitCast(@as(isize, @intCast((mi_col >> @intCast(@as(c_int, 3))) + ((mi_row >> @intCast(@as(c_int, 3))) * lf.*.lfm_stride))))];
}
pub const struct_LoopFilterWorkerData = extern struct {
    frame_buffer: [*c]YV12_BUFFER_CONFIG = null,
    cm: [*c]struct_VP9Common = null,
    planes: [3]struct_macroblockd_plane = @import("std").mem.zeroes([3]struct_macroblockd_plane),
    start: c_int = 0,
    stop: c_int = 0,
    y_only: c_int = 0,
};
pub const LFWorkerData = struct_LoopFilterWorkerData;
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
pub extern fn antfly_vp9_vp9_setup_dst_planes(planes: [*c]struct_macroblockd_plane, src: [*c]const YV12_BUFFER_CONFIG, mi_row: c_int, mi_col: c_int) void;
pub const left_64x64_txform_mask: [4]u64 = [4]u64{
    18446744073709551615,
    18446744073709551615,
    6148914691236517205,
    1229782938247303441,
};
pub const above_64x64_txform_mask: [4]u64 = [4]u64{
    18446744073709551615,
    18446744073709551615,
    71777214294589695,
    1095216660735,
};
pub const left_prediction_mask: [13]u64 = [13]u64{
    1,
    1,
    1,
    1,
    257,
    1,
    257,
    16843009,
    257,
    16843009,
    72340172838076673,
    16843009,
    72340172838076673,
};
pub const above_prediction_mask: [13]u64 = [13]u64{
    1,
    1,
    1,
    1,
    1,
    3,
    3,
    3,
    15,
    15,
    15,
    255,
    255,
};
pub const size_mask: [13]u64 = [13]u64{
    1,
    1,
    1,
    1,
    257,
    3,
    771,
    50529027,
    3855,
    252645135,
    1085102592571150095,
    4294967295,
    18446744073709551615,
};
pub const left_border: u64 = 1229782938247303441;
pub const above_border: u64 = 1095216660735;
pub const left_64x64_txform_mask_uv: [4]u16 = [4]u16{
    65535,
    65535,
    21845,
    4369,
};
pub const above_64x64_txform_mask_uv: [4]u16 = [4]u16{
    65535,
    65535,
    3855,
    15,
};
pub const left_prediction_mask_uv: [13]u16 = [13]u16{
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    17,
    1,
    17,
    4369,
    17,
    4369,
};
pub const above_prediction_mask_uv: [13]u16 = [13]u16{
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    3,
    3,
    3,
    15,
    15,
};
pub const size_mask_uv: [13]u16 = [13]u16{
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    17,
    3,
    51,
    13107,
    255,
    65535,
};
pub const left_border_uv: u16 = 4369;
pub const above_border_uv: u16 = 15;
pub const mode_lf_lut: [14]c_int = [14]c_int{
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    1,
    0,
    1,
};
pub fn update_sharpness(arg_lfi: [*c]loop_filter_info_n, arg_sharpness_lvl: c_int) callconv(.c) void {
    var lfi = arg_lfi;
    _ = &lfi;
    var sharpness_lvl = arg_sharpness_lvl;
    _ = &sharpness_lvl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lvl: c_int = undefined;
    _ = &lvl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        lvl = 0;
        while (lvl <= @as(c_int, 63)) : (lvl += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var block_inside_limit: c_int = lvl >> @intCast(@as(c_int, @intFromBool(sharpness_lvl > @as(c_int, 0))) + @as(c_int, @intFromBool(sharpness_lvl > @as(c_int, 4))));
            _ = &block_inside_limit;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (sharpness_lvl > @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (block_inside_limit > (@as(c_int, 9) - sharpness_lvl)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    block_inside_limit = @as(c_int, 9) - sharpness_lvl;
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
            if (block_inside_limit < @as(c_int, 1)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                block_inside_limit = 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&lfi.*.lfthr[@bitCast(@as(isize, @intCast(lvl)))].lim))))), block_inside_limit, 16);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&lfi.*.lfthr[@bitCast(@as(isize, @intCast(lvl)))].mblim))))), (@as(c_int, 2) * (lvl + @as(c_int, 2))) + block_inside_limit, 16);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_filter_level(arg_lfi_n: [*c]const loop_filter_info_n, arg_mi: [*c]const MODE_INFO) callconv(.c) u8 {
    var lfi_n = arg_lfi_n;
    _ = &lfi_n;
    var mi = arg_mi;
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return lfi_n.*.lvl[@bitCast(@as(isize, @intCast(mi.*.segment_id)))][@bitCast(@as(isize, @intCast(mi.*.ref_frame[@as(c_int, 0)])))][@bitCast(@as(isize, @intCast(mode_lf_lut[mi.*.mode])))];
}
pub export fn antfly_vp9_vp9_loop_filter_init(arg_cm: [*c]VP9_COMMON) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lfi: [*c]loop_filter_info_n = &cm.*.lf_info;
    _ = &lfi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lf: [*c]struct_loopfilter = &cm.*.lf;
    _ = &lf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lvl: c_int = undefined;
    _ = &lvl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    update_sharpness(lfi, lf.*.sharpness_level);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.last_sharpness_level = lf.*.sharpness_level;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        lvl = 0;
        while (lvl <= @as(c_int, 63)) : (lvl += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&lfi.*.lfthr[@bitCast(@as(isize, @intCast(lvl)))].hev_thr))))), lvl >> @intCast(@as(c_int, 4)), 16);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub export fn antfly_vp9_vp9_loop_filter_frame_init(arg_cm: [*c]VP9_COMMON, arg_default_filt_lvl: c_int) void {
    var cm = arg_cm;
    _ = &cm;
    var default_filt_lvl = arg_default_filt_lvl;
    _ = &default_filt_lvl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var seg_id: c_int = undefined;
    _ = &seg_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const scale: c_int = @as(c_int, 1) << @intCast(default_filt_lvl >> @intCast(@as(c_int, 5)));
    _ = &scale;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lfi: [*c]loop_filter_info_n = &cm.*.lf_info;
    _ = &lfi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lf: [*c]struct_loopfilter = &cm.*.lf;
    _ = &lf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const seg: [*c]const struct_segmentation = &cm.*.seg;
    _ = &seg;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (lf.*.last_sharpness_level != lf.*.sharpness_level) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        update_sharpness(lfi, lf.*.sharpness_level);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        lf.*.last_sharpness_level = lf.*.sharpness_level;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        seg_id = 0;
        while (seg_id < @as(c_int, 8)) : (seg_id += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var lvl_seg: c_int = default_filt_lvl;
            _ = &lvl_seg;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (segfeature_active(seg, seg_id, SEG_LVL_ALT_LF) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const data: c_int = get_segdata(seg, seg_id, SEG_LVL_ALT_LF);
                _ = &data;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lvl_seg = clamp(if (@as(c_int, seg.*.abs_delta) == @as(c_int, 1)) data else default_filt_lvl + data, 0, 63);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(@as(c_int, lf.*.mode_ref_delta_enabled) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][2]u8, @ptrCast(@alignCast(&lfi.*.lvl[@bitCast(@as(isize, @intCast(seg_id)))]))))), lvl_seg, @sizeOf(@TypeOf(lfi.*.lvl[@bitCast(@as(isize, @intCast(seg_id)))])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var ref: c_int = undefined;
                _ = &ref;
                var mode: c_int = undefined;
                _ = &mode;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const intra_lvl: c_int = lvl_seg + (@as(c_int, lf.*.ref_deltas[@as(c_int, 0)]) * scale);
                _ = &intra_lvl;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfi.*.lvl[@bitCast(@as(isize, @intCast(seg_id)))][@as(c_int, 0)][@as(c_int, 0)] = @bitCast(@as(i8, @truncate(clamp(intra_lvl, 0, 63))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    ref = 1;
                    while (ref < @as(c_int, 4)) : (ref += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        {
                            mode = 0;
                            while (mode < @as(c_int, 2)) : (mode += 1) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                const inter_lvl: c_int = (lvl_seg + (@as(c_int, lf.*.ref_deltas[@bitCast(@as(isize, @intCast(ref)))]) * scale)) + (@as(c_int, lf.*.mode_deltas[@bitCast(@as(isize, @intCast(mode)))]) * scale);
                                _ = &inter_lvl;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                lfi.*.lvl[@bitCast(@as(isize, @intCast(seg_id)))][@bitCast(@as(isize, @intCast(ref)))][@bitCast(@as(isize, @intCast(mode)))] = @bitCast(@as(i8, @truncate(clamp(inter_lvl, 0, 63))));
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
pub fn filter_selectively_vert_row2(arg_subsampling_factor: c_int, arg_s: [*c]u8, arg_pitch: c_int, arg_mask_16x16: c_uint, arg_mask_8x8: c_uint, arg_mask_4x4: c_uint, arg_mask_4x4_int: c_uint, arg_lfthr: [*c]const loop_filter_thresh, arg_lfl: [*c]const u8) callconv(.c) void {
    var subsampling_factor = arg_subsampling_factor;
    _ = &subsampling_factor;
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var mask_16x16 = arg_mask_16x16;
    _ = &mask_16x16;
    var mask_8x8 = arg_mask_8x8;
    _ = &mask_8x8;
    var mask_4x4 = arg_mask_4x4;
    _ = &mask_4x4;
    var mask_4x4_int = arg_mask_4x4_int;
    _ = &mask_4x4_int;
    var lfthr = arg_lfthr;
    _ = &lfthr;
    var lfl = arg_lfl;
    _ = &lfl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dual_mask_cutoff: c_int = if (subsampling_factor != 0) @as(c_int, 255) else @as(c_int, 65535);
    _ = &dual_mask_cutoff;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lfl_forward: c_int = if (subsampling_factor != 0) @as(c_int, 4) else @as(c_int, 8);
    _ = &lfl_forward;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dual_one: c_uint = @bitCast(@as(c_int, @as(c_int, 1) | (@as(c_int, 1) << @intCast(lfl_forward))));
    _ = &dual_one;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask: c_uint = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ss: [2][*c]u8 = undefined;
    _ = &ss;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ss[@as(c_int, 0)] = s;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = (((mask_16x16 | mask_8x8) | mask_4x4) | mask_4x4_int) & @as(c_uint, @bitCast(@as(c_int, dual_mask_cutoff)));
        while (mask != 0) : (mask = (mask & ~dual_one) >> @intCast(@as(c_uint, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & dual_one) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var lfis: [2][*c]const loop_filter_thresh = undefined;
                _ = &lfis;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfis[@as(c_int, 0)] = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, lfl.*)))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfis[@as(c_int, 1)] = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, (lfl + @as(usize, @bitCast(@as(isize, @intCast(lfl_forward))))).*)))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                ss[@as(c_int, 1)] = ss[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask_16x16 & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_16x16 & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_16_dual_c(ss[@as(c_int, 0)], pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_16x16 & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_16_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_16x16 & @as(c_uint, 1)) != 0))))))], pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                if ((mask_8x8 & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_8x8 & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_8_dual_c(ss[@as(c_int, 0)], pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_8x8 & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_8_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_8x8 & @as(c_uint, 1)) != 0))))))], pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                if ((mask_4x4 & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_4x4 & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_4_dual_c(ss[@as(c_int, 0)], pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4 & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_4_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4 & @as(c_uint, 1)) != 0))))))], pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                if ((mask_4x4_int & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_4x4_int & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_4_dual_c(ss[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4_int & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_4_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4_int & @as(c_uint, 1)) != 0))))))] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ss[@as(c_int, 0)] += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfl += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_filter_selectively_vert_row2(arg_subsampling_factor: c_int, arg_s: [*c]u16, arg_pitch: c_int, arg_mask_16x16: c_uint, arg_mask_8x8: c_uint, arg_mask_4x4: c_uint, arg_mask_4x4_int: c_uint, arg_lfthr: [*c]const loop_filter_thresh, arg_lfl: [*c]const u8, arg_bd: c_int) callconv(.c) void {
    var subsampling_factor = arg_subsampling_factor;
    _ = &subsampling_factor;
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var mask_16x16 = arg_mask_16x16;
    _ = &mask_16x16;
    var mask_8x8 = arg_mask_8x8;
    _ = &mask_8x8;
    var mask_4x4 = arg_mask_4x4;
    _ = &mask_4x4;
    var mask_4x4_int = arg_mask_4x4_int;
    _ = &mask_4x4_int;
    var lfthr = arg_lfthr;
    _ = &lfthr;
    var lfl = arg_lfl;
    _ = &lfl;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dual_mask_cutoff: c_int = if (subsampling_factor != 0) @as(c_int, 255) else @as(c_int, 65535);
    _ = &dual_mask_cutoff;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lfl_forward: c_int = if (subsampling_factor != 0) @as(c_int, 4) else @as(c_int, 8);
    _ = &lfl_forward;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dual_one: c_uint = @bitCast(@as(c_int, @as(c_int, 1) | (@as(c_int, 1) << @intCast(lfl_forward))));
    _ = &dual_one;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask: c_uint = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ss: [2][*c]u16 = undefined;
    _ = &ss;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ss[@as(c_int, 0)] = s;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = (((mask_16x16 | mask_8x8) | mask_4x4) | mask_4x4_int) & @as(c_uint, @bitCast(@as(c_int, dual_mask_cutoff)));
        while (mask != 0) : (mask = (mask & ~dual_one) >> @intCast(@as(c_uint, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & dual_one) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var lfis: [2][*c]const loop_filter_thresh = undefined;
                _ = &lfis;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfis[@as(c_int, 0)] = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, lfl.*)))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfis[@as(c_int, 1)] = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, (lfl + @as(usize, @bitCast(@as(isize, @intCast(lfl_forward))))).*)))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                ss[@as(c_int, 1)] = ss[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask_16x16 & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_16x16 & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_16_dual_c(ss[@as(c_int, 0)], pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_16x16 & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_16_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_16x16 & @as(c_uint, 1)) != 0))))))], pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                if ((mask_8x8 & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_8x8 & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_8_dual_c(ss[@as(c_int, 0)], pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_8x8 & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_8_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_8x8 & @as(c_uint, 1)) != 0))))))], pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                if ((mask_4x4 & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_4x4 & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_4_dual_c(ss[@as(c_int, 0)], pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4 & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_4_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4 & @as(c_uint, 1)) != 0))))))], pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                if ((mask_4x4_int & dual_one) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_4x4_int & dual_one) == dual_one) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_4_dual_c(ss[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), pitch, @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 0)].*.hev_thr)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.mblim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.lim)), @ptrCast(@alignCast(&lfis[@as(c_int, 1)].*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lfi: [*c]const loop_filter_thresh = lfis[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4_int & @as(c_uint, 1)) != 0))))))];
                        _ = &lfi;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_4_c(ss[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!((mask_4x4_int & @as(c_uint, 1)) != 0))))))] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ss[@as(c_int, 0)] += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfl += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn filter_selectively_horiz(arg_s: [*c]u8, arg_pitch: c_int, arg_mask_16x16: c_uint, arg_mask_8x8: c_uint, arg_mask_4x4: c_uint, arg_mask_4x4_int: c_uint, arg_lfthr: [*c]const loop_filter_thresh, arg_lfl: [*c]const u8) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var mask_16x16 = arg_mask_16x16;
    _ = &mask_16x16;
    var mask_8x8 = arg_mask_8x8;
    _ = &mask_8x8;
    var mask_4x4 = arg_mask_4x4;
    _ = &mask_4x4;
    var mask_4x4_int = arg_mask_4x4_int;
    _ = &mask_4x4_int;
    var lfthr = arg_lfthr;
    _ = &lfthr;
    var lfl = arg_lfl;
    _ = &lfl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask: c_uint = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var count: c_int = undefined;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = ((mask_16x16 | mask_8x8) | mask_4x4) | mask_4x4_int;
        while (mask != 0) : (mask >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            count = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & @as(c_uint, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var lfi: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, lfl.*)))));
                _ = &lfi;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask_16x16 & @as(c_uint, 1)) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_16x16 & @as(c_uint, 3)) == @as(c_uint, 3)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_horizontal_16_dual_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        count = 2;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_horizontal_16_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_8x8 & @as(c_uint, 1)) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((mask_8x8 & @as(c_uint, 3)) == @as(c_uint, 3)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            var lfin: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, (lfl + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))))).*)))));
                            _ = &lfin;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_lpf_horizontal_8_dual_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if ((mask_4x4_int & @as(c_uint, 3)) == @as(c_uint, 3)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_lpf_horizontal_4_dual_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    antfly_vp9_vpx_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    if ((mask_4x4_int & @as(c_uint, 2)) != 0) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        antfly_vp9_vpx_lpf_horizontal_4_c((s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)));
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
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
                            count = 2;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_lpf_horizontal_8_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((mask_4x4 & @as(c_uint, 1)) != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if ((mask_4x4 & @as(c_uint, 3)) == @as(c_uint, 3)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                var lfin: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, (lfl + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))))).*)))));
                                _ = &lfin;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_lpf_horizontal_4_dual_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((mask_4x4_int & @as(c_uint, 3)) == @as(c_uint, 3)) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    antfly_vp9_vpx_lpf_horizontal_4_dual_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        antfly_vp9_vpx_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                    } else {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        if ((mask_4x4_int & @as(c_uint, 2)) != 0) {
                                            if (antfly_codec_afz_vp9_failed() != 0) {
                                                return;
                                            }
                                            antfly_vp9_vpx_lpf_horizontal_4_c((s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)));
                                            if (antfly_codec_afz_vp9_failed() != 0) {
                                                return;
                                            }
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
                                count = 2;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_lpf_horizontal_4_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    antfly_vp9_vpx_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfl += @as(usize, @bitCast(@as(isize, @intCast(count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_filter_selectively_horiz(arg_s: [*c]u16, arg_pitch: c_int, arg_mask_16x16: c_uint, arg_mask_8x8: c_uint, arg_mask_4x4: c_uint, arg_mask_4x4_int: c_uint, arg_lfthr: [*c]const loop_filter_thresh, arg_lfl: [*c]const u8, arg_bd: c_int) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var mask_16x16 = arg_mask_16x16;
    _ = &mask_16x16;
    var mask_8x8 = arg_mask_8x8;
    _ = &mask_8x8;
    var mask_4x4 = arg_mask_4x4;
    _ = &mask_4x4;
    var mask_4x4_int = arg_mask_4x4_int;
    _ = &mask_4x4_int;
    var lfthr = arg_lfthr;
    _ = &lfthr;
    var lfl = arg_lfl;
    _ = &lfl;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask: c_uint = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var count: c_int = undefined;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = ((mask_16x16 | mask_8x8) | mask_4x4) | mask_4x4_int;
        while (mask != 0) : (mask >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            count = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & @as(c_uint, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var lfi: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, lfl.*)))));
                _ = &lfi;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask_16x16 & @as(c_uint, 1)) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_16x16 & @as(c_uint, 3)) == @as(c_uint, 3)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_horizontal_16_dual_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        count = 2;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_horizontal_16_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_8x8 & @as(c_uint, 1)) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((mask_8x8 & @as(c_uint, 3)) == @as(c_uint, 3)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            var lfin: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, (lfl + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))))).*)))));
                            _ = &lfin;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_highbd_lpf_horizontal_8_dual_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)), bd);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if ((mask_4x4_int & @as(c_uint, 3)) == @as(c_uint, 3)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_highbd_lpf_horizontal_4_dual_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)), bd);
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    if ((mask_4x4_int & @as(c_uint, 2)) != 0) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        antfly_vp9_vpx_highbd_lpf_horizontal_4_c((s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)), bd);
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
                            count = 2;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_highbd_lpf_horizontal_8_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((mask_4x4 & @as(c_uint, 1)) != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if ((mask_4x4 & @as(c_uint, 3)) == @as(c_uint, 3)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                var lfin: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, (lfl + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))))).*)))));
                                _ = &lfin;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_highbd_lpf_horizontal_4_dual_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)), bd);
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((mask_4x4_int & @as(c_uint, 3)) == @as(c_uint, 3)) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    antfly_vp9_vpx_highbd_lpf_horizontal_4_dual_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)), bd);
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                    } else {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        if ((mask_4x4_int & @as(c_uint, 2)) != 0) {
                                            if (antfly_codec_afz_vp9_failed() != 0) {
                                                return;
                                            }
                                            antfly_vp9_vpx_highbd_lpf_horizontal_4_c((s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfin.*.mblim)), @ptrCast(@alignCast(&lfin.*.lim)), @ptrCast(@alignCast(&lfin.*.hev_thr)), bd);
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
                                count = 2;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfl += @as(usize, @bitCast(@as(isize, @intCast(count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(c_uint, @bitCast(@as(c_int, count))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_adjust_mask(cm: [*c]VP9_COMMON, mi_row: c_int, mi_col: c_int, arg_lfm: [*c]LOOP_FILTER_MASK) void {
    _ = &cm;
    _ = &mi_row;
    _ = &mi_col;
    var lfm = arg_lfm;
    _ = &lfm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] |= lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] |= lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        const ref = &lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
        ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | @as(c_int, lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))]))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        const ref = &lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
        ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | @as(c_int, lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))]))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] |= lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] & left_border;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] &= ~left_border;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] |= lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] & above_border;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] &= ~above_border;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        const ref = &lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
        ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))]) & @as(c_int, left_border_uv)))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        const ref = &lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
        ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & ~@as(c_int, left_border_uv))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        const ref = &lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
        ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))]) & @as(c_int, above_border_uv)))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        const ref = &lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
        ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & ~@as(c_int, above_border_uv))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((mi_row + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) > cm.*.mi_rows) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const rows: u64 = @bitCast(@as(c_longlong, cm.*.mi_rows - mi_row));
        _ = &rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mask_y: u64 = (@as(u64, @bitCast(@as(c_longlong, @as(c_int, 1)))) << @intCast(rows << @intCast(@as(u64, 3)))) -% @as(u64, 1);
        _ = &mask_y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mask_uv: u16 = @truncate((@as(u64, @bitCast(@as(c_longlong, @as(c_int, @as(u16, @bitCast(@as(c_short, @truncate(@as(c_int, 1))))))))) << @intCast(((rows +% @as(u64, 1)) >> @intCast(@as(u64, 1))) << @intCast(@as(u64, 2)))) -% @as(u64, 1));
        _ = &mask_uv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfm.*.left_y[@bitCast(@as(isize, @intCast(i)))] &= mask_y;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfm.*.above_y[@bitCast(@as(isize, @intCast(i)))] &= mask_y;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    const ref = &lfm.*.left_uv[@bitCast(@as(isize, @intCast(i)))];
                    ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, mask_uv))));
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    const ref = &lfm.*.above_uv[@bitCast(@as(isize, @intCast(i)))];
                    ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, mask_uv))));
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        lfm.*.int_4x4_y &= mask_y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &lfm.*.int_4x4_uv;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, mask_uv))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (rows == @as(u64, 1)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | @as(c_int, lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (rows == @as(u64, 5)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]) & @as(c_int, 65280)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & ~(@as(c_int, lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]) & @as(c_int, 65280)))));
            }
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
    if ((mi_col + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) > cm.*.mi_cols) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const columns: u64 = @bitCast(@as(c_longlong, cm.*.mi_cols - mi_col));
        _ = &columns;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mask_y: u64 = ((@as(u64, 1) << @intCast(columns)) -% @as(u64, 1)) *% @as(c_ulonglong, 72340172838076673);
        _ = &mask_y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mask_uv: u16 = @truncate(((@as(u64, 1) << @intCast((columns +% @as(u64, 1)) >> @intCast(@as(u64, 1)))) -% @as(u64, 1)) *% @as(u64, 4369));
        _ = &mask_uv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mask_uv_int: u16 = @truncate(((@as(u64, 1) << @intCast(columns >> @intCast(@as(u64, 1)))) -% @as(u64, 1)) *% @as(u64, 4369));
        _ = &mask_uv_int;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfm.*.left_y[@bitCast(@as(isize, @intCast(i)))] &= mask_y;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfm.*.above_y[@bitCast(@as(isize, @intCast(i)))] &= mask_y;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    const ref = &lfm.*.left_uv[@bitCast(@as(isize, @intCast(i)))];
                    ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, mask_uv))));
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    const ref = &lfm.*.above_uv[@bitCast(@as(isize, @intCast(i)))];
                    ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, mask_uv))));
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        lfm.*.int_4x4_y &= mask_y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &lfm.*.int_4x4_uv;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, mask_uv_int))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (columns == @as(u64, 1)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | @as(c_int, lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (columns == @as(u64, 5)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]) & @as(c_int, 52428)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & ~(@as(c_int, lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))]) & @as(c_int, 52428)))));
            }
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
    if (mi_col == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                lfm.*.left_y[@bitCast(@as(isize, @intCast(i)))] &= 18374403900871474942;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    const ref = &lfm.*.left_uv[@bitCast(@as(isize, @intCast(i)))];
                    ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) & @as(c_int, 61166))));
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn filter_selectively_vert(arg_s: [*c]u8, arg_pitch: c_int, arg_mask_16x16: c_uint, arg_mask_8x8: c_uint, arg_mask_4x4: c_uint, arg_mask_4x4_int: c_uint, arg_lfthr: [*c]const loop_filter_thresh, arg_lfl: [*c]const u8) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var mask_16x16 = arg_mask_16x16;
    _ = &mask_16x16;
    var mask_8x8 = arg_mask_8x8;
    _ = &mask_8x8;
    var mask_4x4 = arg_mask_4x4;
    _ = &mask_4x4;
    var mask_4x4_int = arg_mask_4x4_int;
    _ = &mask_4x4_int;
    var lfthr = arg_lfthr;
    _ = &lfthr;
    var lfl = arg_lfl;
    _ = &lfl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask: c_uint = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = ((mask_16x16 | mask_8x8) | mask_4x4) | mask_4x4_int;
        while (mask != 0) : (mask >>= @intCast(@as(c_uint, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var lfi: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, lfl.*)))));
            _ = &lfi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & @as(c_uint, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask_16x16 & @as(c_uint, 1)) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vpx_lpf_vertical_16_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_8x8 & @as(c_uint, 1)) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_lpf_vertical_8_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((mask_4x4 & @as(c_uint, 1)) != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_lpf_vertical_4_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
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
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_lpf_vertical_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfl += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_filter_selectively_vert(arg_s: [*c]u16, arg_pitch: c_int, arg_mask_16x16: c_uint, arg_mask_8x8: c_uint, arg_mask_4x4: c_uint, arg_mask_4x4_int: c_uint, arg_lfthr: [*c]const loop_filter_thresh, arg_lfl: [*c]const u8, arg_bd: c_int) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var mask_16x16 = arg_mask_16x16;
    _ = &mask_16x16;
    var mask_8x8 = arg_mask_8x8;
    _ = &mask_8x8;
    var mask_4x4 = arg_mask_4x4;
    _ = &mask_4x4;
    var mask_4x4_int = arg_mask_4x4_int;
    _ = &mask_4x4_int;
    var lfthr = arg_lfthr;
    _ = &lfthr;
    var lfl = arg_lfl;
    _ = &lfl;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask: c_uint = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = ((mask_16x16 | mask_8x8) | mask_4x4) | mask_4x4_int;
        while (mask != 0) : (mask >>= @intCast(@as(c_uint, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var lfi: [*c]const loop_filter_thresh = lfthr + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, lfl.*)))));
            _ = &lfi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & @as(c_uint, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask_16x16 & @as(c_uint, 1)) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vpx_highbd_lpf_vertical_16_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((mask_8x8 & @as(c_uint, 1)) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vpx_highbd_lpf_vertical_8_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((mask_4x4 & @as(c_uint, 1)) != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vpx_highbd_lpf_vertical_4_c(s, pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
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
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask_4x4_int & @as(c_uint, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_highbd_lpf_vertical_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), pitch, @ptrCast(@alignCast(&lfi.*.mblim)), @ptrCast(@alignCast(&lfi.*.lim)), @ptrCast(@alignCast(&lfi.*.hev_thr)), bd);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lfl += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(c_uint, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_filter_block_plane_non420(arg_cm: [*c]VP9_COMMON, arg_plane: [*c]struct_macroblockd_plane, arg_mi_8x8: [*c][*c]MODE_INFO, arg_mi_row: c_int, arg_mi_col: c_int) void {
    var cm = arg_cm;
    _ = &cm;
    var plane = arg_plane;
    _ = &plane;
    var mi_8x8 = arg_mi_8x8;
    _ = &mi_8x8;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ss_x: c_int = plane.*.subsampling_x;
    _ = &ss_x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ss_y: c_int = plane.*.subsampling_y;
    _ = &ss_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const row_step: c_int = @as(c_int, 1) << @intCast(ss_y);
    _ = &row_step;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const col_step: c_int = @as(c_int, 1) << @intCast(ss_x);
    _ = &col_step;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const row_step_stride: c_int = cm.*.mi_stride * row_step;
    _ = &row_step_stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst: [*c]struct_buf_2d = &plane.*.dst;
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst0: [*c]u8 = dst.*.buf;
    _ = &dst0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_16x16: [8]c_uint = undefined;
    _ = &mask_16x16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_8x8: [8]c_uint = undefined;
    _ = &mask_8x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_4x4: [8]c_uint = undefined;
    _ = &mask_4x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_4x4_int: [8]c_uint = undefined;
    _ = &mask_4x4_int;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lfl: [64]u8 = undefined;
    _ = &lfl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    var c: c_int = undefined;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&mask_16x16)), 0, @sizeOf(@TypeOf(mask_16x16)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&mask_8x8)), 0, @sizeOf(@TypeOf(mask_8x8)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&mask_4x4)), 0, @sizeOf(@TypeOf(mask_4x4)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&mask_4x4_int)), 0, @sizeOf(@TypeOf(mask_4x4_int)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&lfl)), 0, @sizeOf(@TypeOf(lfl)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while ((r < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_row + r) < cm.*.mi_rows)) : (r += row_step) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_16x16_c: c_uint = 0;
            _ = &mask_16x16_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_8x8_c: c_uint = 0;
            _ = &mask_8x8_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_4x4_c: c_uint = 0;
            _ = &mask_4x4_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var border_mask: c_uint = undefined;
            _ = &border_mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while ((c < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_col + c) < cm.*.mi_cols)) : (c += col_step) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var mi: [*c]const MODE_INFO = mi_8x8[@bitCast(@as(isize, @intCast(c)))];
                    _ = &mi;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const sb_type: BLOCK_SIZE = mi[@as(c_int, 0)].sb_type;
                    _ = &sb_type;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const skip_this: c_int = @as(c_int, @intFromBool((@as(c_int, mi[@as(c_int, 0)].skip) != 0) and (is_inter_block(mi) != 0)));
                    _ = &skip_this;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const block_edge_left: c_int = if (@as(c_int, antfly_vp9_num_4x4_blocks_wide_lookup[sb_type]) > @as(c_int, 1)) @as(c_int, @intFromBool(!((c & (@as(c_int, antfly_vp9_num_8x8_blocks_wide_lookup[sb_type]) - @as(c_int, 1))) != 0))) else @as(c_int, 1);
                    _ = &block_edge_left;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const skip_this_c: c_int = @as(c_int, @intFromBool((skip_this != 0) and !(block_edge_left != 0)));
                    _ = &skip_this_c;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const block_edge_above: c_int = if (@as(c_int, antfly_vp9_num_4x4_blocks_high_lookup[sb_type]) > @as(c_int, 1)) @as(c_int, @intFromBool(!((r & (@as(c_int, antfly_vp9_num_8x8_blocks_high_lookup[sb_type]) - @as(c_int, 1))) != 0))) else @as(c_int, 1);
                    _ = &block_edge_above;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const skip_this_r: c_int = @as(c_int, @intFromBool((skip_this != 0) and !(block_edge_above != 0)));
                    _ = &skip_this_r;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const tx_size: TX_SIZE = get_uv_tx_size(mi, plane);
                    _ = &tx_size;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const skip_border_4x4_c: c_int = @as(c_int, @intFromBool((ss_x != 0) and ((mi_col + c) == (cm.*.mi_cols - @as(c_int, 1)))));
                    _ = &skip_border_4x4_c;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const skip_border_4x4_r: c_int = @as(c_int, @intFromBool((ss_y != 0) and ((mi_row + r) == (cm.*.mi_rows - @as(c_int, 1)))));
                    _ = &skip_border_4x4_r;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (!(@as(c_int, blk: {
                        const tmp = get_filter_level(&cm.*.lf_info, mi);
                        lfl[@bitCast(@as(isize, @intCast((r << @intCast(@as(c_int, 3))) + (c >> @intCast(ss_x)))))] = tmp;
                        break :blk tmp;
                    }) != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        continue;
                    }
                    if (@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (!(skip_this_c != 0) and (((c >> @intCast(ss_x)) & @as(c_int, 3)) == @as(c_int, 0))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (!(skip_border_4x4_c != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                mask_16x16_c |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                mask_8x8_c |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                        if (!(skip_this_r != 0) and (((r >> @intCast(ss_y)) & @as(c_int, 3)) == @as(c_int, 0))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (!(skip_border_4x4_r != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                mask_16x16[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                mask_8x8[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (!(skip_this_c != 0) and (((c >> @intCast(ss_x)) & @as(c_int, 1)) == @as(c_int, 0))) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if (!(skip_border_4x4_c != 0)) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_16x16_c |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_8x8_c |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                            if (!(skip_this_r != 0) and (((r >> @intCast(ss_y)) & @as(c_int, 1)) == @as(c_int, 0))) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if (!(skip_border_4x4_r != 0)) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_16x16[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_8x8[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (!(skip_this_c != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))))) or (((c >> @intCast(ss_x)) & @as(c_int, 3)) == @as(c_int, 0))) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_8x8_c |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_4x4_c |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                            if (!(skip_this_r != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if ((@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))))) or (((r >> @intCast(ss_y)) & @as(c_int, 3)) == @as(c_int, 0))) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_8x8[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                } else {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    mask_4x4[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                            if ((!(skip_this != 0) and (@as(c_int, tx_size) < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))))) and !(skip_border_4x4_c != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                mask_4x4_int[@bitCast(@as(isize, @intCast(r)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(c >> @intCast(ss_x))));
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
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            border_mask = ~(if (mi_col == @as(c_int, 0)) @as(c_uint, 1) else @as(c_uint, 0));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cm.*.use_highbitdepth != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_filter_selectively_vert(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst.*.buf))) << @intCast(@as(usize, 1)))), dst.*.stride, mask_16x16_c & border_mask, mask_8x8_c & border_mask, mask_4x4_c & border_mask, mask_4x4_int[@bitCast(@as(isize, @intCast(r)))], @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))], @bitCast(cm.*.bit_depth));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                filter_selectively_vert(dst.*.buf, dst.*.stride, mask_16x16_c & border_mask, mask_8x8_c & border_mask, mask_4x4_c & border_mask, mask_4x4_int[@bitCast(@as(isize, @intCast(r)))], @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst.*.buf += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * dst.*.stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mi_8x8 += @as(usize, @bitCast(@as(isize, @intCast(row_step_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst.*.buf = dst0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while ((r < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_row + r) < cm.*.mi_rows)) : (r += row_step) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const skip_border_4x4_r: c_int = @as(c_int, @intFromBool((ss_y != 0) and ((mi_row + r) == (cm.*.mi_rows - @as(c_int, 1)))));
            _ = &skip_border_4x4_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask_4x4_int_r: c_uint = if (skip_border_4x4_r != 0) @as(c_uint, 0) else mask_4x4_int[@bitCast(@as(isize, @intCast(r)))];
            _ = &mask_4x4_int_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_16x16_r: c_uint = undefined;
            _ = &mask_16x16_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_8x8_r: c_uint = undefined;
            _ = &mask_8x8_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_4x4_r: c_uint = undefined;
            _ = &mask_4x4_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mi_row + r) == @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_16x16_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_8x8_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_4x4_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_16x16_r = mask_16x16[@bitCast(@as(isize, @intCast(r)))];
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_8x8_r = mask_8x8[@bitCast(@as(isize, @intCast(r)))];
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_4x4_r = mask_4x4[@bitCast(@as(isize, @intCast(r)))];
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cm.*.use_highbitdepth != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_filter_selectively_horiz(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst.*.buf))) << @intCast(@as(usize, 1)))), dst.*.stride, mask_16x16_r, mask_8x8_r, mask_4x4_r, mask_4x4_int_r, @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))], @bitCast(cm.*.bit_depth));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                filter_selectively_horiz(dst.*.buf, dst.*.stride, mask_16x16_r, mask_8x8_r, mask_4x4_r, mask_4x4_int_r, @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst.*.buf += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * dst.*.stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_filter_block_plane_ss00(cm: [*c]VP9_COMMON, plane: [*c]struct_macroblockd_plane, arg_mi_row: c_int, arg_lfm: [*c]LOOP_FILTER_MASK) void {
    _ = &cm;
    _ = &plane;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var lfm = arg_lfm;
    _ = &lfm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst: [*c]struct_buf_2d = &plane.*.dst;
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst0: [*c]u8 = dst.*.buf;
    _ = &dst0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_16x16: u64 = lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    _ = &mask_16x16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_8x8: u64 = lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    _ = &mask_8x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_4x4: u64 = lfm.*.left_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    _ = &mask_4x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_4x4_int: u64 = lfm.*.int_4x4_y;
    _ = &mask_4x4_int;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while ((r < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_row + r) < cm.*.mi_rows)) : (r += 2) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cm.*.use_highbitdepth != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_filter_selectively_vert_row2(plane.*.subsampling_x, @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst.*.buf))) << @intCast(@as(usize, 1)))), dst.*.stride, @truncate(mask_16x16), @truncate(mask_8x8), @truncate(mask_4x4), @truncate(mask_4x4_int), @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfm.*.lfl_y[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))], @bitCast(cm.*.bit_depth));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                filter_selectively_vert_row2(plane.*.subsampling_x, dst.*.buf, dst.*.stride, @truncate(mask_16x16), @truncate(mask_8x8), @truncate(mask_4x4), @truncate(mask_4x4_int), @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfm.*.lfl_y[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst.*.buf += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16) * dst.*.stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(u64, 16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(u64, 16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(u64, 16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(u64, 16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst.*.buf = dst0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_16x16 = lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_8x8 = lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_4x4 = lfm.*.above_y[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_4x4_int = lfm.*.int_4x4_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while ((r < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_row + r) < cm.*.mi_rows)) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_16x16_r: c_uint = undefined;
            _ = &mask_16x16_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_8x8_r: c_uint = undefined;
            _ = &mask_8x8_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_4x4_r: c_uint = undefined;
            _ = &mask_4x4_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mi_row + r) == @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_16x16_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_8x8_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_4x4_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_16x16_r = @truncate(mask_16x16 & @as(u64, 255));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_8x8_r = @truncate(mask_8x8 & @as(u64, 255));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_4x4_r = @truncate(mask_4x4 & @as(u64, 255));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cm.*.use_highbitdepth != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_filter_selectively_horiz(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst.*.buf))) << @intCast(@as(usize, 1)))), dst.*.stride, mask_16x16_r, mask_8x8_r, mask_4x4_r, @truncate(mask_4x4_int & @as(u64, 255)), @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfm.*.lfl_y[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))], @bitCast(cm.*.bit_depth));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                filter_selectively_horiz(dst.*.buf, dst.*.stride, mask_16x16_r, mask_8x8_r, mask_4x4_r, @truncate(mask_4x4_int & @as(u64, 255)), @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfm.*.lfl_y[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 3)))))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst.*.buf += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * dst.*.stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_16x16 >>= @intCast(@as(u64, 8));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_8x8 >>= @intCast(@as(u64, 8));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4 >>= @intCast(@as(u64, 8));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mask_4x4_int >>= @intCast(@as(u64, 8));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_filter_block_plane_ss11(cm: [*c]VP9_COMMON, plane: [*c]struct_macroblockd_plane, arg_mi_row: c_int, arg_lfm: [*c]LOOP_FILTER_MASK) void {
    _ = &cm;
    _ = &plane;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var lfm = arg_lfm;
    _ = &lfm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst: [*c]struct_buf_2d = &plane.*.dst;
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst0: [*c]u8 = dst.*.buf;
    _ = &dst0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    var c: c_int = undefined;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lfl_uv: [16]u8 = undefined;
    _ = &lfl_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_16x16: u16 = lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    _ = &mask_16x16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_8x8: u16 = lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    _ = &mask_8x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_4x4: u16 = lfm.*.left_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    _ = &mask_4x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mask_4x4_int: u16 = lfm.*.int_4x4_uv;
    _ = &mask_4x4_int;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&lfl_uv)), 0, @sizeOf(@TypeOf(lfl_uv)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while ((r < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_row + r) < cm.*.mi_rows)) : (r += 4) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) >> @intCast(@as(c_int, 1)))) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    lfl_uv[@bitCast(@as(isize, @intCast((r << @intCast(@as(c_int, 1))) + c)))] = lfm.*.lfl_y[@bitCast(@as(isize, @intCast((r << @intCast(@as(c_int, 3))) + (c << @intCast(@as(c_int, 1))))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    lfl_uv[@bitCast(@as(isize, @intCast(((r + @as(c_int, 2)) << @intCast(@as(c_int, 1))) + c)))] = lfm.*.lfl_y[@bitCast(@as(isize, @intCast(((r + @as(c_int, 2)) << @intCast(@as(c_int, 3))) + (c << @intCast(@as(c_int, 1))))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cm.*.use_highbitdepth != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_filter_selectively_vert_row2(plane.*.subsampling_x, @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst.*.buf))) << @intCast(@as(usize, 1)))), dst.*.stride, mask_16x16, mask_8x8, mask_4x4, mask_4x4_int, @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl_uv[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 1)))))], @bitCast(cm.*.bit_depth));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                filter_selectively_vert_row2(plane.*.subsampling_x, dst.*.buf, dst.*.stride, mask_16x16, mask_8x8, mask_4x4, mask_4x4_int, @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl_uv[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 1)))))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst.*.buf += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16) * dst.*.stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_16x16;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 8)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_8x8;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 8)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_4x4;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 8)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_4x4_int;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 8)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst.*.buf = dst0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_16x16 = lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_8x8 = lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_4x4 = lfm.*.above_uv[@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mask_4x4_int = lfm.*.int_4x4_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while ((r < (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) and ((mi_row + r) < cm.*.mi_rows)) : (r += 2) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const skip_border_4x4_r: c_int = @as(c_int, @intFromBool((mi_row + r) == (cm.*.mi_rows - @as(c_int, 1))));
            _ = &skip_border_4x4_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask_4x4_int_r: c_uint = @bitCast(@as(c_int, if (skip_border_4x4_r != 0) @as(c_int, 0) else @as(c_int, mask_4x4_int) & @as(c_int, 15)));
            _ = &mask_4x4_int_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_16x16_r: c_uint = undefined;
            _ = &mask_16x16_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_8x8_r: c_uint = undefined;
            _ = &mask_8x8_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mask_4x4_r: c_uint = undefined;
            _ = &mask_4x4_r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mi_row + r) == @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_16x16_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_8x8_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_4x4_r = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_16x16_r = @bitCast(@as(c_int, @as(c_int, mask_16x16) & @as(c_int, 15)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_8x8_r = @bitCast(@as(c_int, @as(c_int, mask_8x8) & @as(c_int, 15)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mask_4x4_r = @bitCast(@as(c_int, @as(c_int, mask_4x4) & @as(c_int, 15)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cm.*.use_highbitdepth != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_filter_selectively_horiz(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst.*.buf))) << @intCast(@as(usize, 1)))), dst.*.stride, mask_16x16_r, mask_8x8_r, mask_4x4_r, mask_4x4_int_r, @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl_uv[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 1)))))], @bitCast(cm.*.bit_depth));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                filter_selectively_horiz(dst.*.buf, dst.*.stride, mask_16x16_r, mask_8x8_r, mask_4x4_r, mask_4x4_int_r, @ptrCast(@alignCast(&cm.*.lf_info.lfthr)), &lfl_uv[@bitCast(@as(isize, @intCast(r << @intCast(@as(c_int, 1)))))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst.*.buf += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * dst.*.stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_16x16;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 4)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_8x8;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 4)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_4x4;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 4)))));
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                const ref = &mask_4x4_int;
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) >> @intCast(@as(c_int, 4)))));
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
pub fn loop_filter_rows(arg_frame_buffer: [*c]YV12_BUFFER_CONFIG, arg_cm: [*c]VP9_COMMON, arg_planes: [*c]struct_macroblockd_plane, arg_start: c_int, arg_stop: c_int, arg_y_only: c_int) callconv(.c) void {
    var frame_buffer = arg_frame_buffer;
    _ = &frame_buffer;
    var cm = arg_cm;
    _ = &cm;
    var planes = arg_planes;
    _ = &planes;
    var start = arg_start;
    _ = &start;
    var stop = arg_stop;
    _ = &stop;
    var y_only = arg_y_only;
    _ = &y_only;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_planes: c_int = if (y_only != 0) @as(c_int, 1) else @as(c_int, 3);
    _ = &num_planes;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var path: enum_lf_path = undefined;
    _ = &path;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi_row: c_int = undefined;
    _ = &mi_row;
    var mi_col: c_int = undefined;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (y_only != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        path = LF_PATH_444;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((planes[@as(c_int, 1)].subsampling_y == @as(c_int, 1)) and (planes[@as(c_int, 1)].subsampling_x == @as(c_int, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            path = LF_PATH_420;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((planes[@as(c_int, 1)].subsampling_y == @as(c_int, 0)) and (planes[@as(c_int, 1)].subsampling_x == @as(c_int, 0))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                path = LF_PATH_444;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                path = LF_PATH_SLOW;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
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
    {
        mi_row = start;
        while (mi_row < stop) : (mi_row += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var mi: [*c][*c]MODE_INFO = cm.*.mi_grid_visible + @as(usize, @bitCast(@as(isize, @intCast(mi_row * cm.*.mi_stride))));
            _ = &mi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var lfm: [*c]LOOP_FILTER_MASK = get_lfm(&cm.*.lf, mi_row, 0);
            _ = &lfm;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                mi_col = 0;
                while (mi_col < cm.*.mi_cols) : ({
                    mi_col += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3));
                    lfm += 1;
                }) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var plane: c_int = undefined;
                    _ = &plane;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_setup_dst_planes(planes, frame_buffer, mi_row, mi_col);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_adjust_mask(cm, mi_row, mi_col, lfm);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_filter_block_plane_ss00(cm, &planes[@as(c_int, 0)], mi_row, lfm);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        plane = 1;
                        while (plane < num_planes) : (plane += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            blk: {
                                switch (path) {
                                    @as(enum_lf_path, LF_PATH_420) => {
                                        antfly_vp9_vp9_filter_block_plane_ss11(cm, &planes[@bitCast(@as(isize, @intCast(plane)))], mi_row, lfm);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        break :blk;
                                    },
                                    @as(enum_lf_path, LF_PATH_444) => {
                                        antfly_vp9_vp9_filter_block_plane_ss00(cm, &planes[@bitCast(@as(isize, @intCast(plane)))], mi_row, lfm);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        break :blk;
                                    },
                                    @as(enum_lf_path, LF_PATH_SLOW) => {
                                        antfly_vp9_vp9_filter_block_plane_non420(cm, &planes[@bitCast(@as(isize, @intCast(plane)))], mi + @as(usize, @bitCast(@as(isize, @intCast(mi_col)))), mi_row, mi_col);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        break :blk;
                                    },
                                    else => {},
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
pub var first_block_in_16x16: [8][8]u8 = [8][8]u8{
    [8]u8{
        1,
        0,
        1,
        0,
        1,
        0,
        1,
        0,
    },
    [8]u8{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    },
    [8]u8{
        1,
        0,
        1,
        0,
        1,
        0,
        1,
        0,
    },
    [8]u8{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    },
    [8]u8{
        1,
        0,
        1,
        0,
        1,
        0,
        1,
        0,
    },
    [8]u8{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    },
    [8]u8{
        1,
        0,
        1,
        0,
        1,
        0,
        1,
        0,
    },
    [8]u8{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    },
};
pub export fn antfly_vp9_vp9_build_mask(arg_cm: [*c]VP9_COMMON, arg_mi: [*c]const MODE_INFO, arg_mi_row: c_int, arg_mi_col: c_int, arg_bw: c_int, arg_bh: c_int) void {
    var cm = arg_cm;
    _ = &cm;
    var mi = arg_mi;
    _ = &mi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bw = arg_bw;
    _ = &bw;
    var bh = arg_bh;
    _ = &bh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const block_size: BLOCK_SIZE = mi.*.sb_type;
    _ = &block_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tx_size_y: TX_SIZE = mi.*.tx_size;
    _ = &tx_size_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lfi_n: [*c]const loop_filter_info_n = &cm.*.lf_info;
    _ = &lfi_n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const filter_level: c_int = get_filter_level(lfi_n, mi);
    _ = &filter_level;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tx_size_uv: TX_SIZE = antfly_vp9_uv_txsize_lookup[block_size][tx_size_y][@as(c_int, 1)][@as(c_int, 1)];
    _ = &tx_size_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const lfm: [*c]LOOP_FILTER_MASK = get_lfm(&cm.*.lf, mi_row, mi_col);
    _ = &lfm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const left_y: [*c]u64 = &lfm.*.left_y[tx_size_y];
    _ = &left_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const above_y: [*c]u64 = &lfm.*.above_y[tx_size_y];
    _ = &above_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const int_4x4_y: [*c]u64 = &lfm.*.int_4x4_y;
    _ = &int_4x4_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const left_uv: [*c]u16 = &lfm.*.left_uv[tx_size_uv];
    _ = &left_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const above_uv: [*c]u16 = &lfm.*.above_uv[tx_size_uv];
    _ = &above_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const int_4x4_uv: [*c]u16 = &lfm.*.int_4x4_uv;
    _ = &int_4x4_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const row_in_sb: c_int = mi_row & @as(c_int, 7);
    _ = &row_in_sb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const col_in_sb: c_int = mi_col & @as(c_int, 7);
    _ = &col_in_sb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const shift_y: c_int = col_in_sb + (row_in_sb << @intCast(@as(c_int, 3)));
    _ = &shift_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const shift_uv: c_int = (col_in_sb >> @intCast(@as(c_int, 1))) + ((row_in_sb >> @intCast(@as(c_int, 1))) << @intCast(@as(c_int, 2)));
    _ = &shift_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const build_uv: c_int = first_block_in_16x16[@bitCast(@as(isize, @intCast(row_in_sb)))][@bitCast(@as(isize, @intCast(col_in_sb)))];
    _ = &build_uv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(filter_level != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var index_1: c_int = shift_y;
        _ = &index_1;
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
            while (i < bh) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&lfm.*.lfl_y[@bitCast(@as(isize, @intCast(index_1)))])), filter_level, @bitCast(@as(c_long, bw)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                index_1 += 8;
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
    above_y.* |= above_prediction_mask[block_size] << @intCast(@as(u64, @bitCast(@as(c_longlong, shift_y))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    left_y.* |= left_prediction_mask[block_size] << @intCast(@as(u64, @bitCast(@as(c_longlong, shift_y))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (build_uv != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &above_uv.*;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, above_prediction_mask_uv[block_size]) << @intCast(shift_uv)))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &left_uv.*;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, left_prediction_mask_uv[block_size]) << @intCast(shift_uv)))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, mi.*.skip) != 0) and (is_inter_block(mi) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    above_y.* |= (size_mask[block_size] & above_64x64_txform_mask[tx_size_y]) << @intCast(@as(u64, @bitCast(@as(c_longlong, shift_y))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    left_y.* |= (size_mask[block_size] & left_64x64_txform_mask[tx_size_y]) << @intCast(@as(u64, @bitCast(@as(c_longlong, shift_y))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (build_uv != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &above_uv.*;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | ((@as(c_int, size_mask_uv[block_size]) & @as(c_int, above_64x64_txform_mask_uv[tx_size_uv])) << @intCast(shift_uv)))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &left_uv.*;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | ((@as(c_int, size_mask_uv[block_size]) & @as(c_int, left_64x64_txform_mask_uv[tx_size_uv])) << @intCast(shift_uv)))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, tx_size_y) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        int_4x4_y.* |= size_mask[block_size] << @intCast(@as(u64, @bitCast(@as(c_longlong, shift_y))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if ((build_uv != 0) and (@as(c_int, tx_size_uv) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &int_4x4_uv.*;
            ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | ((@as(c_int, size_mask_uv[block_size]) & @as(c_int, 65535)) << @intCast(shift_uv)))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_loop_filter_data_reset(arg_lf_data: [*c]LFWorkerData, arg_frame_buffer: [*c]YV12_BUFFER_CONFIG, arg_cm: [*c]struct_VP9Common, arg_planes: [*c]const struct_macroblockd_plane) void {
    var lf_data = arg_lf_data;
    _ = &lf_data;
    var frame_buffer = arg_frame_buffer;
    _ = &frame_buffer;
    var cm = arg_cm;
    _ = &cm;
    var planes = arg_planes;
    _ = &planes;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_data.*.frame_buffer = frame_buffer;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_data.*.cm = cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_data.*.start = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_data.*.stop = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_data.*.y_only = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]struct_macroblockd_plane, @ptrCast(@alignCast(&lf_data.*.planes))))), @ptrCast(@alignCast(planes)), @sizeOf(@TypeOf(lf_data.*.planes)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_reset_lfm(cm: [*c]VP9_COMMON) void {
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.lf.filter_level != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.lf.lfm)), 0, @as(c_ulong, @bitCast(@as(c_long, ((cm.*.mi_rows + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))) >> @intCast(@as(c_int, 3))) * cm.*.lf.lfm_stride))) *% @sizeOf(@TypeOf(cm.*.lf.lfm.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_loop_filter_worker(arg_arg1: ?*anyopaque, arg_unused: ?*anyopaque) c_int {
    var arg1 = arg_arg1;
    _ = &arg1;
    var unused = arg_unused;
    _ = &unused;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const lf_data: [*c]LFWorkerData = @ptrCast(@alignCast(arg1));
    _ = &lf_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &unused;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    loop_filter_rows(lf_data.*.frame_buffer, lf_data.*.cm, @ptrCast(@alignCast(&lf_data.*.planes)), lf_data.*.start, lf_data.*.stop, lf_data.*.y_only);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 1;
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
