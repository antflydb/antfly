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
pub extern fn antfly_codec_afz_vp9_fail(?*anyopaque, c_int) void;
pub const struct___va_list_1 = common.struct___va_list_1;
pub const __builtin_va_list = struct___va_list_1;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub const vpx_img_fmt_t = common.vpx_img_fmt_t;
pub const VPX_CS_SRGB = common.VPX_CS_SRGB;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const struct_vpx_image = extern struct {
    fmt: vpx_img_fmt_t = @import("std").mem.zeroes(vpx_img_fmt_t),
    cs: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    w: c_uint = 0,
    h: c_uint = 0,
    bit_depth: c_uint = 0,
    d_w: c_uint = 0,
    d_h: c_uint = 0,
    r_w: c_uint = 0,
    r_h: c_uint = 0,
    x_chroma_shift: c_uint = 0,
    y_chroma_shift: c_uint = 0,
    planes: [4][*c]u8 = @import("std").mem.zeroes([4][*c]u8),
    stride: [4]c_int = @import("std").mem.zeroes([4]c_int),
    bps: c_int = 0,
    user_priv: ?*anyopaque = null,
    img_data: [*c]u8 = null,
    img_data_owner: c_int = 0,
    self_allocd: c_int = 0,
    fb_priv: ?*anyopaque = null,
};
pub const vpx_image_t = struct_vpx_image;
pub const vpx_image_rect_t = common.vpx_image_rect_t;
pub const VPX_CODEC_OK = common.VPX_CODEC_OK;
pub const VPX_CODEC_ERROR = common.VPX_CODEC_ERROR;
pub const VPX_CODEC_MEM_ERROR = common.VPX_CODEC_MEM_ERROR;
pub const VPX_CODEC_INCAPABLE = common.VPX_CODEC_INCAPABLE;
pub const VPX_CODEC_UNSUP_BITSTREAM = common.VPX_CODEC_UNSUP_BITSTREAM;
pub const VPX_CODEC_CORRUPT_FRAME = common.VPX_CODEC_CORRUPT_FRAME;
pub const VPX_CODEC_INVALID_PARAM = common.VPX_CODEC_INVALID_PARAM;
pub const vpx_codec_err_t = common.vpx_codec_err_t;
pub const vpx_codec_caps_t = common.vpx_codec_caps_t;
pub const vpx_codec_flags_t = common.vpx_codec_flags_t;
pub const vpx_codec_iface_t = struct_vpx_codec_iface;
pub const struct_vpx_codec_dec_cfg = common.struct_vpx_codec_dec_cfg;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_fixed_buf_t = common.vpx_fixed_buf_t;
pub const struct_vpx_codec_enc_cfg = common.struct_vpx_codec_enc_cfg;
const union_unnamed_3 = extern union {
    dec: [*c]const struct_vpx_codec_dec_cfg,
    enc: [*c]const struct_vpx_codec_enc_cfg,
    raw: ?*const anyopaque,
};
pub const vpx_codec_put_frame_cb_fn_t = ?*const fn (user_priv: ?*anyopaque, img: [*c]const vpx_image_t) callconv(.c) void;
pub const vpx_codec_put_slice_cb_fn_t = ?*const fn (user_priv: ?*anyopaque, img: [*c]const vpx_image_t, valid: [*c]const vpx_image_rect_t, update: [*c]const vpx_image_rect_t) callconv(.c) void;
const union_unnamed_5 = extern union {
    put_frame: vpx_codec_put_frame_cb_fn_t,
    put_slice: vpx_codec_put_slice_cb_fn_t,
};
pub const struct_vpx_codec_priv_cb_pair = extern struct {
    u: union_unnamed_5 = @import("std").mem.zeroes(union_unnamed_5),
    user_priv: ?*anyopaque = null,
};
pub const vpx_codec_priv_cb_pair_t = struct_vpx_codec_priv_cb_pair;
const struct_unnamed_4 = extern struct {
    put_frame_cb: vpx_codec_priv_cb_pair_t = @import("std").mem.zeroes(vpx_codec_priv_cb_pair_t),
    put_slice_cb: vpx_codec_priv_cb_pair_t = @import("std").mem.zeroes(vpx_codec_priv_cb_pair_t),
};
pub const enum_vpx_codec_cx_pkt_kind = common.enum_vpx_codec_cx_pkt_kind;
pub const vpx_codec_pts_t = common.vpx_codec_pts_t;
pub const vpx_codec_frame_flags_t = common.vpx_codec_frame_flags_t;
const struct_unnamed_8 = extern struct {
    buf: ?*anyopaque = null,
    sz: usize = 0,
    pts: vpx_codec_pts_t = 0,
    duration: c_ulong = 0,
    flags: vpx_codec_frame_flags_t = 0,
    partition_id: c_int = 0,
    width: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    height: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    spatial_layer_encoded: [5]u8 = @import("std").mem.zeroes([5]u8),
};
pub const struct_vpx_psnr_pkt_9 = common.struct_vpx_psnr_pkt_9;
const union_unnamed_7 = extern union {
    frame: struct_unnamed_8,
    twopass_stats: vpx_fixed_buf_t,
    firstpass_mb_stats: vpx_fixed_buf_t,
    psnr: struct_vpx_psnr_pkt_9,
    raw: vpx_fixed_buf_t,
    pad: [124]u8,
};
pub const struct_vpx_codec_cx_pkt = extern struct {
    kind: enum_vpx_codec_cx_pkt_kind = @import("std").mem.zeroes(enum_vpx_codec_cx_pkt_kind),
    data: union_unnamed_7 = @import("std").mem.zeroes(union_unnamed_7),
};
pub const vpx_codec_cx_pkt_t = struct_vpx_codec_cx_pkt;
const struct_unnamed_6 = extern struct {
    cx_data_dst_buf: vpx_fixed_buf_t = @import("std").mem.zeroes(vpx_fixed_buf_t),
    cx_data_pad_before: c_uint = 0,
    cx_data_pad_after: c_uint = 0,
    cx_data_pkt: vpx_codec_cx_pkt_t = @import("std").mem.zeroes(vpx_codec_cx_pkt_t),
    total_encoders: c_uint = 0,
};
pub const struct_vpx_codec_priv = extern struct {
    err_detail: [*c]const u8 = null,
    init_flags: vpx_codec_flags_t = 0,
    dec: struct_unnamed_4 = @import("std").mem.zeroes(struct_unnamed_4),
    enc: struct_unnamed_6 = @import("std").mem.zeroes(struct_unnamed_6),
};
pub const vpx_codec_priv_t = struct_vpx_codec_priv;
pub const struct_vpx_codec_ctx = extern struct {
    name: [*c]const u8 = null,
    iface: [*c]const vpx_codec_iface_t = null,
    err: vpx_codec_err_t = @import("std").mem.zeroes(vpx_codec_err_t),
    err_detail: [*c]const u8 = null,
    init_flags: vpx_codec_flags_t = 0,
    config: union_unnamed_3 = @import("std").mem.zeroes(union_unnamed_3),
    priv: [*c]vpx_codec_priv_t = null,
};
pub const vpx_codec_ctx_t = struct_vpx_codec_ctx;
pub const vpx_codec_priv_enc_mr_cfg_t = common.vpx_codec_priv_enc_mr_cfg_t;
pub const vpx_codec_init_fn_t = ?*const fn (ctx: [*c]vpx_codec_ctx_t, data: [*c]vpx_codec_priv_enc_mr_cfg_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_dec_cfg_t = common.vpx_codec_dec_cfg_t;
pub const vpx_codec_stream_info_t = common.vpx_codec_stream_info_t;
pub const vp9_stream_info_t = vpx_codec_stream_info_t;
pub const tran_low_t = common.tran_low_t;
pub const struct_buf_2d = extern struct {
    buf: [*c]u8 = null,
    stride: c_int = 0,
};
pub const ENTROPY_CONTEXT = common.ENTROPY_CONTEXT;
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
pub const struct_TileInfo = extern struct {
    mi_row_start: c_int = 0,
    mi_row_end: c_int = 0,
    mi_col_start: c_int = 0,
    mi_col_end: c_int = 0,
};
pub const TileInfo = struct_TileInfo;
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
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
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
pub const PROFILE_0 = common.PROFILE_0;
pub const PROFILE_1 = common.PROFILE_1;
pub const PROFILE_2 = common.PROFILE_2;
pub const PROFILE_3 = common.PROFILE_3;
pub const MAX_PROFILES = common.MAX_PROFILES;
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
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
pub const VP9_COMMON = struct_VP9Common;
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
pub const vpx_decrypt_cb = common.vpx_decrypt_cb;
pub const struct_ThreadData = extern struct {
    pbi: [*c]struct_VP9Decoder = null,
    lf_data: [*c]LFWorkerData = null,
    lf_sync: [*c]VP9LfSync = null,
};
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
pub const VP9Decoder = struct_VP9Decoder;
pub const struct_vp8_postproc_cfg = extern struct {
    post_proc_flag: c_int = 0,
    deblocking_level: c_int = 0,
    noise_level: c_int = 0,
};
pub const vp8_postproc_cfg_t = struct_vp8_postproc_cfg;
pub const struct_vpx_codec_alg_priv = extern struct {
    base: vpx_codec_priv_t = @import("std").mem.zeroes(vpx_codec_priv_t),
    cfg: vpx_codec_dec_cfg_t = @import("std").mem.zeroes(vpx_codec_dec_cfg_t),
    si: vp9_stream_info_t = @import("std").mem.zeroes(vp9_stream_info_t),
    pbi: [*c]VP9Decoder = null,
    user_priv: ?*anyopaque = null,
    postproc_cfg_set: c_int = 0,
    postproc_cfg: vp8_postproc_cfg_t = @import("std").mem.zeroes(vp8_postproc_cfg_t),
    decrypt_cb: vpx_decrypt_cb = null,
    decrypt_state: ?*anyopaque = null,
    img: vpx_image_t = @import("std").mem.zeroes(vpx_image_t),
    img_avail: c_int = 0,
    flushed: c_int = 0,
    invert_tile_order: c_int = 0,
    last_show_frame: c_int = 0,
    byte_alignment: c_int = 0,
    skip_loop_filter: c_int = 0,
    need_resync: c_int = 0,
    buffer_pool: [*c]BufferPool = null,
    ext_priv: ?*anyopaque = null,
    get_ext_fb_cb: vpx_get_frame_buffer_cb_fn_t = null,
    release_ext_fb_cb: vpx_release_frame_buffer_cb_fn_t = null,
    svc_decoding: c_int = 0,
    svc_spatial_layer: c_int = 0,
    row_mt: c_int = 0,
    lpf_opt: c_int = 0,
};
pub const vpx_codec_alg_priv_t = struct_vpx_codec_alg_priv;
pub const vpx_codec_destroy_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t) callconv(.c) vpx_codec_err_t;
pub const va_list = __builtin_va_list;
pub const vpx_codec_control_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, ap: va_list) callconv(.c) vpx_codec_err_t;
pub const struct_vpx_codec_ctrl_fn_map = extern struct {
    ctrl_id: c_int = 0,
    @"fn": vpx_codec_control_fn_t = null,
};
pub const vpx_codec_ctrl_fn_map_t = struct_vpx_codec_ctrl_fn_map;
pub const vpx_codec_peek_si_fn_t = common.vpx_codec_peek_si_fn_t;
pub const vpx_codec_get_si_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, si: [*c]vpx_codec_stream_info_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_decode_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, data: [*c]const u8, data_sz: c_uint, user_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_iter_t = common.vpx_codec_iter_t;
pub const vpx_codec_get_frame_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, iter: [*c]vpx_codec_iter_t) callconv(.c) [*c]vpx_image_t;
pub const vpx_codec_set_fb_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, cb_get: vpx_get_frame_buffer_cb_fn_t, cb_release: vpx_release_frame_buffer_cb_fn_t, cb_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t;
pub const struct_vpx_codec_dec_iface_10 = extern struct {
    peek_si: vpx_codec_peek_si_fn_t = null,
    get_si: vpx_codec_get_si_fn_t = null,
    decode: vpx_codec_decode_fn_t = null,
    get_frame: vpx_codec_get_frame_fn_t = null,
    set_fb_fn: vpx_codec_set_fb_fn_t = null,
};
pub const vpx_codec_enc_cfg_t = common.vpx_codec_enc_cfg_t;
pub const vpx_codec_enc_cfg_map_t = common.vpx_codec_enc_cfg_map_t;
pub const vpx_enc_frame_flags_t = common.vpx_enc_frame_flags_t;
pub const vpx_enc_deadline_t = common.vpx_enc_deadline_t;
pub const vpx_codec_encode_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, img: [*c]const vpx_image_t, pts: vpx_codec_pts_t, duration: c_ulong, flags: vpx_enc_frame_flags_t, deadline: vpx_enc_deadline_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_get_cx_data_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, iter: [*c]vpx_codec_iter_t) callconv(.c) [*c]const vpx_codec_cx_pkt_t;
pub const vpx_codec_enc_config_set_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t, cfg: [*c]const vpx_codec_enc_cfg_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_get_global_headers_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t) callconv(.c) [*c]vpx_fixed_buf_t;
pub const vpx_codec_get_preview_frame_fn_t = ?*const fn (ctx: [*c]vpx_codec_alg_priv_t) callconv(.c) [*c]vpx_image_t;
pub const vpx_codec_enc_mr_get_mem_loc_fn_t = common.vpx_codec_enc_mr_get_mem_loc_fn_t;
pub const struct_vpx_codec_enc_iface_11 = extern struct {
    cfg_map_count: c_int = 0,
    cfg_maps: [*c]const vpx_codec_enc_cfg_map_t = null,
    encode: vpx_codec_encode_fn_t = null,
    get_cx_data: vpx_codec_get_cx_data_fn_t = null,
    cfg_set: vpx_codec_enc_config_set_fn_t = null,
    get_glob_hdrs: vpx_codec_get_global_headers_fn_t = null,
    get_preview: vpx_codec_get_preview_frame_fn_t = null,
    mr_get_mem_loc: vpx_codec_enc_mr_get_mem_loc_fn_t = null,
};
pub const struct_vpx_codec_iface = extern struct {
    name: [*c]const u8 = null,
    abi_version: c_int = 0,
    caps: vpx_codec_caps_t = 0,
    init: vpx_codec_init_fn_t = null,
    destroy: vpx_codec_destroy_fn_t = null,
    ctrl_maps: [*c]const vpx_codec_ctrl_fn_map_t = null,
    dec: struct_vpx_codec_dec_iface_10 = @import("std").mem.zeroes(struct_vpx_codec_dec_iface_10),
    enc: struct_vpx_codec_enc_iface_11 = @import("std").mem.zeroes(struct_vpx_codec_enc_iface_11),
};
pub const VP8_SET_REFERENCE = common.VP8_SET_REFERENCE;
pub const VP8_COPY_REFERENCE = common.VP8_COPY_REFERENCE;
pub const VP8_SET_POSTPROC = common.VP8_SET_POSTPROC;
pub const VP9_GET_REFERENCE = common.VP9_GET_REFERENCE;
pub const VP8_DEBLOCK = common.VP8_DEBLOCK;
pub const VP8_DEMACROBLOCK = common.VP8_DEMACROBLOCK;
pub const VP8D_GET_LAST_REF_UPDATES = common.VP8D_GET_LAST_REF_UPDATES;
pub const VP8D_GET_FRAME_CORRUPTED = common.VP8D_GET_FRAME_CORRUPTED;
pub const VPXD_SET_DECRYPTOR = common.VPXD_SET_DECRYPTOR;
pub const VP9D_GET_FRAME_SIZE = common.VP9D_GET_FRAME_SIZE;
pub const VP9D_GET_DISPLAY_SIZE = common.VP9D_GET_DISPLAY_SIZE;
pub const VP9D_GET_BIT_DEPTH = common.VP9D_GET_BIT_DEPTH;
pub const VP9_SET_BYTE_ALIGNMENT = common.VP9_SET_BYTE_ALIGNMENT;
pub const VP9_INVERT_TILE_DECODE_ORDER = common.VP9_INVERT_TILE_DECODE_ORDER;
pub const VP9_SET_SKIP_LOOP_FILTER = common.VP9_SET_SKIP_LOOP_FILTER;
pub const VP9_DECODE_SVC_SPATIAL_LAYER = common.VP9_DECODE_SVC_SPATIAL_LAYER;
pub const VPXD_GET_LAST_QUANTIZER = common.VPXD_GET_LAST_QUANTIZER;
pub const VP9D_SET_ROW_MT = common.VP9D_SET_ROW_MT;
pub const VP9D_SET_LOOP_FILTER_OPT = common.VP9D_SET_LOOP_FILTER_OPT;
pub const vpx_rb_error_handler = common.vpx_rb_error_handler;
pub const struct_vpx_read_bit_buffer = extern struct {
    bit_buffer: [*c]const u8 = null,
    bit_buffer_end: [*c]const u8 = null,
    bit_offset: usize = 0,
    error_handler_data: ?*anyopaque = null,
    error_handler: vpx_rb_error_handler = null,
};
pub extern fn antfly_vp9_vpx_rb_read_bit(rb: [*c]struct_vpx_read_bit_buffer) c_int;
pub extern fn antfly_vp9_vpx_rb_read_literal(rb: [*c]struct_vpx_read_bit_buffer, bits: c_int) c_int;
pub extern fn antfly_vp9_vp9_free_ref_frame_buffers(pool: [*c]struct_BufferPool) void;
pub extern fn antfly_vp9_vp9_alloc_internal_frame_buffers(list: [*c]InternalFrameBufferList) c_int;
pub extern fn antfly_vp9_vp9_free_internal_frame_buffers(list: [*c]InternalFrameBufferList) void;
pub extern fn antfly_vp9_vp9_get_frame_buffer(cb_priv: ?*anyopaque, min_size: usize, fb: [*c]vpx_codec_frame_buffer_t) c_int;
pub extern fn antfly_vp9_vp9_release_frame_buffer(cb_priv: ?*anyopaque, fb: [*c]vpx_codec_frame_buffer_t) c_int;
pub const TX_MODE = common.TX_MODE;
pub extern fn antfly_vp9_vp9_read_sync_code(rb: [*c]struct_vpx_read_bit_buffer) c_int;
pub extern fn antfly_vp9_vp9_read_frame_size(rb: [*c]struct_vpx_read_bit_buffer, width: [*c]c_int, height: [*c]c_int) void;
pub extern fn antfly_vp9_vp9_read_profile(rb: [*c]struct_vpx_read_bit_buffer) BITSTREAM_PROFILE;
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
pub extern fn antfly_vp9_vpx_calloc(num: usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
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
pub const vp9_ppflags_t = common.vp9_ppflags_t;
pub const JobQueueRowMt = common.JobQueueRowMt;
pub extern fn antfly_vp9_vp9_receive_compressed_data(pbi: [*c]struct_VP9Decoder, size: usize, psource: [*c][*c]const u8) c_int;
pub extern fn antfly_vp9_vp9_get_raw_frame(pbi: [*c]struct_VP9Decoder, sd: [*c]YV12_BUFFER_CONFIG, flags: [*c]vp9_ppflags_t) c_int;
pub fn read_marker(arg_decrypt_cb: vpx_decrypt_cb, arg_decrypt_state: ?*anyopaque, arg_data: [*c]const u8) callconv(.c) u8 {
    var decrypt_cb = arg_decrypt_cb;
    _ = &decrypt_cb;
    var decrypt_state = arg_decrypt_state;
    _ = &decrypt_state;
    var data = arg_data;
    _ = &data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (decrypt_cb != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var marker: u8 = undefined;
        _ = &marker;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        decrypt_cb.?(decrypt_state, data, &marker, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return marker;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return data.*;
}
pub extern fn antfly_vp9_vp9_parse_superframe_index(data: [*c]const u8, data_sz: usize, sizes: [*c]u32, count: [*c]c_int, decrypt_cb: vpx_decrypt_cb, decrypt_state: ?*anyopaque) vpx_codec_err_t;
pub extern fn antfly_vp9_vp9_decoder_create(pool: [*c]BufferPool) [*c]struct_VP9Decoder;
pub extern fn antfly_vp9_vp9_decoder_remove(pbi: [*c]struct_VP9Decoder) void;
pub extern fn antfly_vp9_yuvconfig2image(img: [*c]vpx_image_t, yv12: [*c]const YV12_BUFFER_CONFIG, user_priv: ?*anyopaque) void;
pub fn decoder_init(arg_ctx: [*c]vpx_codec_ctx_t, arg_data: [*c]vpx_codec_priv_enc_mr_cfg_t) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var data = arg_data;
    _ = &data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(ctx.*.priv != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var priv: [*c]vpx_codec_alg_priv_t = undefined;
        priv = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(1, @sizeOf(@TypeOf(priv.*)))));
        _ = &priv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(priv))) == @as(?*anyopaque, null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_MEM_ERROR;
        }
        ctx.*.priv = @ptrCast(@alignCast(priv));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.priv.*.init_flags = ctx.*.init_flags;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        priv.*.si.sz = @truncate(@sizeOf(@TypeOf(priv.*.si)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        priv.*.flushed = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (ctx.*.config.dec != null) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            priv.*.cfg = ctx.*.config.dec.*;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx.*.config.dec = &priv.*.cfg;
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
    return VPX_CODEC_OK;
}
pub fn decoder_destroy(arg_ctx: [*c]vpx_codec_alg_priv_t) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(ctx.*.pbi))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_decoder_remove(ctx.*.pbi);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (ctx.*.buffer_pool != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_free_ref_frame_buffers(ctx.*.buffer_pool);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_free_internal_frame_buffers(&ctx.*.buffer_pool.*.int_frame_buffers);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(ctx.*.buffer_pool)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(ctx)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return VPX_CODEC_OK;
}
pub fn parse_bitdepth_colorspace_sampling(arg_profile: BITSTREAM_PROFILE, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) c_int {
    var profile = arg_profile;
    _ = &profile;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var color_space: vpx_color_space_t = undefined;
    _ = &color_space;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (profile >= @as(BITSTREAM_PROFILE, PROFILE_2)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        rb.*.bit_offset +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    color_space = @bitCast(antfly_vp9_vpx_rb_read_literal(rb, 3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (color_space != @as(vpx_color_space_t, VPX_CS_SRGB)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        rb.*.bit_offset +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((profile == @as(BITSTREAM_PROFILE, PROFILE_1)) or (profile == @as(BITSTREAM_PROFILE, PROFILE_3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            rb.*.bit_offset +%= 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            rb.*.bit_offset +%= 1;
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
        if ((profile == @as(BITSTREAM_PROFILE, PROFILE_1)) or (profile == @as(BITSTREAM_PROFILE, PROFILE_3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            rb.*.bit_offset +%= 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 1;
}
pub fn decoder_peek_si_internal(arg_data: [*c]const u8, arg_data_sz: c_uint, arg_si: [*c]vpx_codec_stream_info_t, arg_is_intra_only: [*c]c_int, arg_decrypt_cb: vpx_decrypt_cb, arg_decrypt_state: ?*anyopaque) callconv(.c) vpx_codec_err_t {
    var data = arg_data;
    _ = &data;
    var data_sz = arg_data_sz;
    _ = &data_sz;
    var si = arg_si;
    _ = &si;
    var is_intra_only = arg_is_intra_only;
    _ = &is_intra_only;
    var decrypt_cb = arg_decrypt_cb;
    _ = &decrypt_cb;
    var decrypt_state = arg_decrypt_state;
    _ = &decrypt_state;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var intra_only_flag: c_int = 0;
    _ = &intra_only_flag;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var clear_buffer: [11]u8 = undefined;
    _ = &clear_buffer;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((data + data_sz) <= data) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return VPX_CODEC_INVALID_PARAM;
    }
    si.*.is_kf = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    si.*.w = blk: {
        const tmp = @as(c_uint, 0);
        si.*.h = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (decrypt_cb != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        data_sz = @truncate(if (@sizeOf(@TypeOf(clear_buffer)) < @as(c_ulong, data_sz)) @sizeOf(@TypeOf(clear_buffer)) else @as(c_ulong, data_sz));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        decrypt_cb.?(decrypt_state, data, @ptrCast(@alignCast(&clear_buffer)), @bitCast(@as(c_uint, @truncate(data_sz))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        data = @ptrCast(@alignCast(&clear_buffer));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (data_sz < @as(c_uint, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return VPX_CODEC_UNSUP_BITSTREAM;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var show_frame: c_int = undefined;
        _ = &show_frame;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var error_resilient: c_int = undefined;
        _ = &error_resilient;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var rb: struct_vpx_read_bit_buffer = struct_vpx_read_bit_buffer{
            .bit_buffer = data,
            .bit_buffer_end = data + data_sz,
            .bit_offset = 0,
            .error_handler_data = null,
            .error_handler = null,
        };
        _ = &rb;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const frame_marker: c_int = antfly_vp9_vpx_rb_read_literal(&rb, 2);
        _ = &frame_marker;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const profile: BITSTREAM_PROFILE = antfly_vp9_vp9_read_profile(&rb);
        _ = &profile;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (frame_marker != @as(c_int, 2)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_UNSUP_BITSTREAM;
        }
        if (profile >= @as(BITSTREAM_PROFILE, MAX_PROFILES)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_UNSUP_BITSTREAM;
        }
        if (antfly_vp9_vpx_rb_read_bit(&rb) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if ((profile > @as(BITSTREAM_PROFILE, 2)) and (data_sz < @as(c_uint, 2))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return VPX_CODEC_UNSUP_BITSTREAM;
            }
            _ = antfly_vp9_vpx_rb_read_literal(&rb, 3);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_OK;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (data_sz < @as(c_uint, 10)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_UNSUP_BITSTREAM;
        }
        si.*.is_kf = @bitCast(@as(c_int, @as(c_int, @intFromBool(!(antfly_vp9_vpx_rb_read_bit(&rb) != 0)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        show_frame = antfly_vp9_vpx_rb_read_bit(&rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        error_resilient = antfly_vp9_vpx_rb_read_bit(&rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (si.*.is_kf != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(antfly_vp9_vp9_read_sync_code(&rb) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return VPX_CODEC_UNSUP_BITSTREAM;
            }
            if (!(parse_bitdepth_colorspace_sampling(profile, &rb) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return VPX_CODEC_UNSUP_BITSTREAM;
            }
            antfly_vp9_vp9_read_frame_size(&rb, @ptrCast(@alignCast(&si.*.w)), @ptrCast(@alignCast(&si.*.h)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            intra_only_flag = if (show_frame != 0) @as(c_int, 0) else antfly_vp9_vpx_rb_read_bit(&rb);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            rb.bit_offset +%= @bitCast(@as(c_long, if (error_resilient != 0) @as(c_int, 0) else @as(c_int, 2)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (intra_only_flag != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(antfly_vp9_vp9_read_sync_code(&rb) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    return VPX_CODEC_UNSUP_BITSTREAM;
                }
                if (profile > @as(BITSTREAM_PROFILE, PROFILE_0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (!(parse_bitdepth_colorspace_sampling(profile, &rb) != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        return VPX_CODEC_UNSUP_BITSTREAM;
                    }
                    if (data_sz < @as(c_uint, 11)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        return VPX_CODEC_UNSUP_BITSTREAM;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                rb.bit_offset +%= @bitCast(@as(c_long, @as(c_int, 1) << @intCast(@as(c_int, 3))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                antfly_vp9_vp9_read_frame_size(&rb, @ptrCast(@alignCast(&si.*.w)), @ptrCast(@alignCast(&si.*.h)));
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(is_intra_only))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        is_intra_only.* = intra_only_flag;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return VPX_CODEC_OK;
}
pub fn decoder_peek_si(arg_data: [*c]const u8, arg_data_sz: c_uint, arg_si: [*c]vpx_codec_stream_info_t) callconv(.c) vpx_codec_err_t {
    var data = arg_data;
    _ = &data;
    var data_sz = arg_data_sz;
    _ = &data_sz;
    var si = arg_si;
    _ = &si;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return decoder_peek_si_internal(data, data_sz, si, null, null, null);
}
pub fn decoder_get_si(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_si: [*c]vpx_codec_stream_info_t) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var si = arg_si;
    _ = &si;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const sz: usize = if (@as(c_ulong, si.*.sz) >= @sizeOf(vp9_stream_info_t)) @sizeOf(vp9_stream_info_t) else @sizeOf(vpx_codec_stream_info_t);
    _ = &sz;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(si)), @ptrCast(@alignCast(&ctx.*.si)), sz);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    si.*.sz = @truncate(sz);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return VPX_CODEC_OK;
}
pub fn set_error_detail(arg_ctx: [*c]vpx_codec_alg_priv_t, @"error": [*c]const u8) callconv(.c) void {
    var ctx = arg_ctx;
    _ = &ctx;
    _ = &@"error";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ctx.*.base.err_detail = @"error";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn update_error_state(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_error: [*c]const struct_vpx_internal_error_info) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var @"error" = arg_error;
    _ = &@"error";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@"error".*.error_code != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        set_error_detail(ctx, @ptrCast(@alignCast(if (@"error".*.has_detail != 0) @as(?*anyopaque, @ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(@constCast(&@"error".*.detail))))))) else @as(?*anyopaque, null))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return @"error".*.error_code;
}
pub fn init_buffer_callbacks(arg_ctx: [*c]vpx_codec_alg_priv_t) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cm: [*c]VP9_COMMON = &ctx.*.pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.new_fb_idx = -@as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.byte_alignment = ctx.*.byte_alignment;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.skip_loop_filter = ctx.*.skip_loop_filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((@as(?*anyopaque, @ptrCast(@alignCast(@constCast(ctx.*.get_ext_fb_cb)))) != @as(?*anyopaque, null)) and (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(ctx.*.release_ext_fb_cb)))) != @as(?*anyopaque, null))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pool.*.get_fb_cb = ctx.*.get_ext_fb_cb;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pool.*.release_fb_cb = ctx.*.release_ext_fb_cb;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pool.*.cb_priv = ctx.*.ext_priv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pool.*.get_fb_cb = antfly_vp9_vp9_get_frame_buffer;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pool.*.release_fb_cb = antfly_vp9_vp9_release_frame_buffer;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_vp9_vp9_alloc_internal_frame_buffers(&pool.*.int_frame_buffers) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_MEM_ERROR;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pool.*.cb_priv = @ptrCast(@alignCast(&pool.*.int_frame_buffers));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return VPX_CODEC_OK;
}
pub fn set_default_ppflags(arg_cfg: [*c]vp8_postproc_cfg_t) callconv(.c) void {
    var cfg = arg_cfg;
    _ = &cfg;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cfg.*.post_proc_flag = VP8_DEBLOCK | VP8_DEMACROBLOCK;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cfg.*.deblocking_level = 4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cfg.*.noise_level = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn set_ppflags(arg_ctx: [*c]const vpx_codec_alg_priv_t, arg_flags: [*c]vp9_ppflags_t) callconv(.c) void {
    var ctx = arg_ctx;
    _ = &ctx;
    var flags = arg_flags;
    _ = &flags;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    flags.*.post_proc_flag = ctx.*.postproc_cfg.post_proc_flag;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    flags.*.deblocking_level = ctx.*.postproc_cfg.deblocking_level;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    flags.*.noise_level = ctx.*.postproc_cfg.noise_level;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn init_decoder(arg_ctx: [*c]vpx_codec_alg_priv_t) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var res: vpx_codec_err_t = undefined;
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.last_show_frame = -@as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.need_resync = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.flushed = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.buffer_pool = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(1, @sizeOf(BufferPool))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(ctx.*.buffer_pool))) == @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return VPX_CODEC_MEM_ERROR;
    }
    ctx.*.pbi = antfly_vp9_vp9_decoder_create(ctx.*.buffer_pool);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(ctx.*.pbi))) == @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(ctx.*.buffer_pool)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.buffer_pool = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        set_error_detail(ctx, "Failed to allocate decoder");
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return VPX_CODEC_MEM_ERROR;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.pbi.*.max_threads = @bitCast(@as(c_uint, @truncate(ctx.*.cfg.threads)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.pbi.*.inv_tile_order = ctx.*.invert_tile_order;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(((ctx.*.row_mt == @as(c_int, 0)) or (ctx.*.row_mt > @as(c_int, 0))) and (ctx.*.row_mt <= @as(c_int, 1)))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx.*.base.err_detail = "row_mt out of range [0..1]";
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return VPX_CODEC_INVALID_PARAM;
            }
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
    ctx.*.pbi.*.row_mt = ctx.*.row_mt;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(((ctx.*.lpf_opt == @as(c_int, 0)) or (ctx.*.lpf_opt > @as(c_int, 0))) and (ctx.*.lpf_opt <= @as(c_int, 1)))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ctx.*.base.err_detail = "lpf_opt out of range [0..1]";
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return VPX_CODEC_INVALID_PARAM;
            }
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
    ctx.*.pbi.*.lpf_mt_opt = ctx.*.lpf_opt;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((@as(vpx_codec_flags_t, @as(c_int, @intFromBool(!(ctx.*.postproc_cfg_set != 0)))) != 0) and ((ctx.*.base.init_flags & @as(vpx_codec_flags_t, 65536)) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        set_default_ppflags(&ctx.*.postproc_cfg);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    res = init_buffer_callbacks(ctx);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (res != @as(vpx_codec_err_t, VPX_CODEC_OK)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(ctx.*.buffer_pool)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.buffer_pool = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_decoder_remove(ctx.*.pbi);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.pbi = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return res;
}
pub fn check_resync(ctx: [*c]vpx_codec_alg_priv_t, pbi: [*c]const VP9Decoder) callconv(.c) void {
    _ = &ctx;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((ctx.*.need_resync == @as(c_int, 1)) and (pbi.*.need_resync == @as(c_int, 0))) and ((@as(c_int, pbi.*.common.intra_only) != 0) or (pbi.*.common.frame_type == @as(FRAME_TYPE, KEY_FRAME)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ctx.*.need_resync = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn decode_one(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_data: [*c][*c]const u8, arg_data_sz: c_uint, arg_user_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var data = arg_data;
    _ = &data;
    var data_sz = arg_data_sz;
    _ = &data_sz;
    var user_priv = arg_user_priv;
    _ = &user_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(ctx.*.si.h != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var is_intra_only: c_int = 0;
        _ = &is_intra_only;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const res: vpx_codec_err_t = decoder_peek_si_internal(data.*, data_sz, &ctx.*.si, &is_intra_only, ctx.*.decrypt_cb, ctx.*.decrypt_state);
        _ = &res;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (res != @as(vpx_codec_err_t, VPX_CODEC_OK)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return res;
        }
        if (!(ctx.*.si.is_kf != 0) and !(is_intra_only != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_ERROR;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.user_priv = user_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.pbi.*.decrypt_cb = ctx.*.decrypt_cb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.pbi.*.decrypt_state = ctx.*.decrypt_state;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (antfly_vp9_vp9_receive_compressed_data(ctx.*.pbi, data_sz, data) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.pbi.*.cur_buf.*.buf.corrupted = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.pbi.*.need_resync = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.need_resync = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return update_error_state(ctx, &ctx.*.pbi.*.common.@"error");
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    check_resync(ctx, ctx.*.pbi);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return VPX_CODEC_OK;
}
pub fn decoder_decode(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_data: [*c]const u8, arg_data_sz: c_uint, arg_user_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var data = arg_data;
    _ = &data;
    var data_sz = arg_data_sz;
    _ = &data_sz;
    var user_priv = arg_user_priv;
    _ = &user_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var data_start: [*c]const u8 = data;
    _ = &data_start;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var res: vpx_codec_err_t = undefined;
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var frame_sizes: [8]u32 = undefined;
    _ = &frame_sizes;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var frame_count: c_int = undefined;
    _ = &frame_count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((@as(?*anyopaque, @ptrCast(@alignCast(@constCast(data)))) == @as(?*anyopaque, null)) and (data_sz == @as(c_uint, 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ctx.*.flushed = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return VPX_CODEC_OK;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ctx.*.flushed = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(ctx.*.pbi))) == @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        res = init_decoder(ctx);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (res != @as(vpx_codec_err_t, VPX_CODEC_OK)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return res;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    res = antfly_vp9_vp9_parse_superframe_index(data, data_sz, @ptrCast(@alignCast(&frame_sizes)), &frame_count, ctx.*.decrypt_cb, ctx.*.decrypt_state);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (res != @as(vpx_codec_err_t, VPX_CODEC_OK)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return res;
    }
    if ((ctx.*.svc_decoding != 0) and (ctx.*.svc_spatial_layer < (frame_count - @as(c_int, 1)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        frame_count = ctx.*.svc_spatial_layer + @as(c_int, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (frame_count > @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const data_end: [*c]const u8 = data + data_sz;
        _ = &data_end;
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
            while (i < frame_count) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                var data_start_copy: [*c]const u8 = data_start;
                _ = &data_start_copy;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const frame_size: u32 = frame_sizes[@bitCast(@as(isize, @intCast(i)))];
                _ = &frame_size;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if ((data_start < data) or (frame_size > @as(u32, @bitCast(@as(c_int, @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(data_end) -% @intFromPtr(data_start))), @sizeOf(u8)))))))) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    set_error_detail(ctx, "Invalid frame size in index");
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    return VPX_CODEC_CORRUPT_FRAME;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                res = decode_one(ctx, &data_start_copy, frame_size, user_priv);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (res != @as(vpx_codec_err_t, VPX_CODEC_OK)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    return res;
                }
                data_start += frame_size;
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
        const data_end: [*c]const u8 = data + data_sz;
        _ = &data_end;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        while (data_start < data_end) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const frame_size: u32 = @bitCast(@as(c_int, @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(data_end) -% @intFromPtr(data_start))), @sizeOf(u8)))));
            _ = &frame_size;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            res = decode_one(ctx, &data_start, frame_size, user_priv);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (res != @as(vpx_codec_err_t, VPX_CODEC_OK)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return res;
            }
            while (data_start < data_end) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const marker: u8 = read_marker(ctx.*.decrypt_cb, ctx.*.decrypt_state, data_start);
                _ = &marker;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (@as(c_int, marker) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    break;
                }
                data_start += 1;
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return res;
}
pub fn decoder_get_frame(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_iter: [*c]vpx_codec_iter_t) callconv(.c) [*c]vpx_image_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var iter = arg_iter;
    _ = &iter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var img: [*c]vpx_image_t = null;
    _ = &img;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = &iter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(ctx.*.pbi))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        var sd: YV12_BUFFER_CONFIG = undefined;
        _ = &sd;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        var flags: vp9_ppflags_t = vp9_ppflags_t{
            .post_proc_flag = 0,
            .deblocking_level = 0,
            .noise_level = 0,
        };
        _ = &flags;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        if ((ctx.*.base.init_flags & @as(vpx_codec_flags_t, 65536)) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            set_ppflags(ctx, &flags);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
        if (antfly_vp9_vp9_get_raw_frame(ctx.*.pbi, &sd, &flags) == @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const cm: [*c]VP9_COMMON = &ctx.*.pbi.*.common;
            _ = &cm;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&cm.*.buffer_pool.*.frame_bufs));
            _ = &frame_bufs;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            ctx.*.last_show_frame = ctx.*.pbi.*.common.new_fb_idx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (ctx.*.need_resync != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                return null;
            }
            antfly_vp9_yuvconfig2image(&ctx.*.img, &sd, ctx.*.user_priv);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            ctx.*.img.fb_priv = frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].raw_frame_buffer.priv;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            img = &ctx.*.img;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            return img;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return null;
}
pub fn decoder_set_fb_fn(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_cb_get: vpx_get_frame_buffer_cb_fn_t, arg_cb_release: vpx_release_frame_buffer_cb_fn_t, arg_cb_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var cb_get = arg_cb_get;
    _ = &cb_get;
    var cb_release = arg_cb_release;
    _ = &cb_release;
    var cb_priv = arg_cb_priv;
    _ = &cb_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((@as(?*anyopaque, @ptrCast(@alignCast(@constCast(cb_get)))) == @as(?*anyopaque, null)) or (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(cb_release)))) == @as(?*anyopaque, null))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return VPX_CODEC_INVALID_PARAM;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(ctx.*.pbi))) == @as(?*anyopaque, null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx.*.get_ext_fb_cb = cb_get;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx.*.release_ext_fb_cb = cb_release;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ctx.*.ext_priv = cb_priv;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_OK;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return VPX_CODEC_ERROR;
}
pub fn ctrl_set_reference(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_copy_reference(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_reference(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_set_postproc(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_quantizer(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_last_ref_updates(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_frame_corrupted(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_frame_size(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_render_size(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_get_bit_depth(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}

pub fn ctrl_set_invert_tile_order(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_set_decryptor(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub fn ctrl_set_byte_alignment(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}

pub fn ctrl_set_skip_loop_filter(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}

pub fn ctrl_set_spatial_layer_svc(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}

pub fn ctrl_set_row_mt(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}

pub fn ctrl_enable_lpf_opt(arg_ctx: [*c]vpx_codec_alg_priv_t, arg_args: va_list) callconv(.c) vpx_codec_err_t {
    _ = arg_ctx;
    _ = arg_args;
    return VPX_CODEC_INCAPABLE;
}
pub const decoder_ctrl_maps: [18]vpx_codec_ctrl_fn_map_t = [18]vpx_codec_ctrl_fn_map_t{
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP8_COPY_REFERENCE,
        .@"fn" = ctrl_copy_reference,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP8_SET_REFERENCE,
        .@"fn" = ctrl_set_reference,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP8_SET_POSTPROC,
        .@"fn" = ctrl_set_postproc,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9_INVERT_TILE_DECODE_ORDER,
        .@"fn" = ctrl_set_invert_tile_order,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VPXD_SET_DECRYPTOR,
        .@"fn" = ctrl_set_decryptor,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9_SET_BYTE_ALIGNMENT,
        .@"fn" = ctrl_set_byte_alignment,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9_SET_SKIP_LOOP_FILTER,
        .@"fn" = ctrl_set_skip_loop_filter,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9_DECODE_SVC_SPATIAL_LAYER,
        .@"fn" = ctrl_set_spatial_layer_svc,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9D_SET_ROW_MT,
        .@"fn" = ctrl_set_row_mt,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9D_SET_LOOP_FILTER_OPT,
        .@"fn" = ctrl_enable_lpf_opt,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VPXD_GET_LAST_QUANTIZER,
        .@"fn" = ctrl_get_quantizer,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP8D_GET_LAST_REF_UPDATES,
        .@"fn" = ctrl_get_last_ref_updates,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP8D_GET_FRAME_CORRUPTED,
        .@"fn" = ctrl_get_frame_corrupted,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9_GET_REFERENCE,
        .@"fn" = ctrl_get_reference,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9D_GET_DISPLAY_SIZE,
        .@"fn" = ctrl_get_render_size,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9D_GET_BIT_DEPTH,
        .@"fn" = ctrl_get_bit_depth,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = VP9D_GET_FRAME_SIZE,
        .@"fn" = ctrl_get_frame_size,
    },
    vpx_codec_ctrl_fn_map_t{
        .ctrl_id = -@as(c_int, 1),
        .@"fn" = null,
    },
};
pub export fn antfly_vp9_vpx_codec_vp9_dx() [*c]const vpx_codec_iface_t {
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return &antfly_vp9_vpx_codec_vp9_dx_algo;
}
pub export const antfly_vp9_vpx_codec_vp9_dx_algo: vpx_codec_iface_t = vpx_codec_iface_t{
    .name = "WebM Project VP9 Decoder v1.15.2",
    .abi_version = @as(c_int, 5),
    .caps = ((@as(c_int, 4) | @as(c_int, 1)) | (if (false) @as(c_int, 262144) else @as(c_int, 0))) | @as(c_int, 4194304),
    .init = decoder_init,
    .destroy = decoder_destroy,
    .ctrl_maps = @ptrCast(@alignCast(&decoder_ctrl_maps)),
    .dec = struct_vpx_codec_dec_iface_10{
        .peek_si = decoder_peek_si,
        .get_si = decoder_get_si,
        .decode = decoder_decode,
        .get_frame = decoder_get_frame,
        .set_fb_fn = decoder_set_fb_fn,
    },
    .enc = struct_vpx_codec_enc_iface_11{
        .cfg_map_count = 0,
        .cfg_maps = null,
        .encode = null,
        .get_cx_data = null,
        .cfg_set = null,
        .get_glob_hdrs = null,
        .get_preview = null,
        .mr_get_mem_loc = null,
    },
};

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
