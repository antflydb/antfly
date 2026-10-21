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
pub const tran_low_t = common.tran_low_t;
pub extern fn antfly_vp9_vpx_memalign(@"align": usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_calloc(num: usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE = common.TX_MODE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const INTERP_FILTER = common.INTERP_FILTER;
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
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
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
pub extern fn antfly_vp9_vp9_rtcd() void;
pub extern fn antfly_vp9_vpx_dsp_rtcd() void;
pub extern fn antfly_vp9_vpx_scale_rtcd() void;
pub extern fn antfly_codec_afz_vp9_once(callback: ?*const fn () callconv(.c) void) void;
pub const VPX_CODEC_OK = common.VPX_CODEC_OK;
pub const VPX_CODEC_MEM_ERROR = common.VPX_CODEC_MEM_ERROR;
pub const VPX_CODEC_CORRUPT_FRAME = common.VPX_CODEC_CORRUPT_FRAME;
pub const vpx_codec_err_t = common.vpx_codec_err_t;
pub const VPX_BITS_8 = common.VPX_BITS_8;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
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
pub const VPxWorkerInterface = extern struct {
    init: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    reset: ?*const fn (worker: [*c]VPxWorker) callconv(.c) c_int = null,
    sync: ?*const fn (worker: [*c]VPxWorker) callconv(.c) c_int = null,
    launch: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    execute: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    end: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
};
pub extern fn antfly_vp9_vpx_get_worker_interface() [*c]const VPxWorkerInterface;
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
pub extern fn antfly_vp9_vp9_remove_common(cm: [*c]struct_VP9Common) void;
pub extern fn antfly_vp9_vp9_swap_current_and_last_seg_map(cm: [*c]struct_VP9Common) void;
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
pub const MACROBLOCKD = struct_macroblockd;
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
pub extern fn antfly_vp9_vp9_loop_filter_init(cm: [*c]struct_VP9Common) void;
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
pub fn init_frame_indexes(arg_cm: [*c]VP9_COMMON) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.current_video_frame = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.current_frame_coding_index = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_frame_new_buffer(arg_cm: [*c]VP9_COMMON) callconv(.c) [*c]YV12_BUFFER_CONFIG {
    var cm = arg_cm;
    _ = &cm;
    // Pure frame view: polling here can cancel between the caller guard and
    // its dereference. The surrounding decoder checkpoints own cancellation.
    return &cm.*.buffer_pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf;
}
pub fn get_free_fb(arg_cm: [*c]VP9_COMMON) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&cm.*.buffer_pool.*.frame_bufs));
    _ = &frame_bufs;
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
        while (i < (((@as(c_int, 1) << @intCast(@as(c_int, 3))) + @as(c_int, 1)) + @as(c_int, 3))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (frame_bufs[@bitCast(@as(isize, @intCast(i)))].ref_count == @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    if (i != (((@as(c_int, 1) << @intCast(@as(c_int, 3))) + @as(c_int, 1)) + @as(c_int, 3))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        frame_bufs[@bitCast(@as(isize, @intCast(i)))].ref_count = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        i = -@as(c_int, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return i;
}
pub extern fn antfly_vp9_vp9_init_intra_predictors() void;
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
pub extern fn antfly_vp9_vp9_decode_frame(pbi: [*c]struct_VP9Decoder, data: [*c]const u8, data_end: [*c]const u8, p_data_end: [*c][*c]const u8) void;
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
pub extern fn antfly_vp9_vp9_loop_filter_dealloc(lf_sync: [*c]VP9LfSync) void;
pub const vp9_ppflags_t = common.vp9_ppflags_t;
pub const JobQueueRowMt = common.JobQueueRowMt;
pub extern fn antfly_vp9_vp9_jobq_deinit(jobq: [*c]JobQueueRowMt) void;
pub const VP9Decoder = struct_VP9Decoder;
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
pub fn decrease_ref_count(arg_idx: c_int, frame_bufs: [*c]RefCntBuffer, pool: [*c]BufferPool) callconv(.c) void {
    var idx = arg_idx;
    _ = &idx;
    _ = &frame_bufs;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((idx >= @as(c_int, 0)) and (frame_bufs[@bitCast(@as(isize, @intCast(idx)))].ref_count > @as(c_int, 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_bufs[@bitCast(@as(isize, @intCast(idx)))].ref_count -= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((!(@as(c_int, frame_bufs[@bitCast(@as(isize, @intCast(idx)))].released) != 0) and (frame_bufs[@bitCast(@as(isize, @intCast(idx)))].ref_count == @as(c_int, 0))) and (frame_bufs[@bitCast(@as(isize, @intCast(idx)))].raw_frame_buffer.priv != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = pool.*.release_fb_cb.?(pool.*.cb_priv, &frame_bufs[@bitCast(@as(isize, @intCast(idx)))].raw_frame_buffer);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            frame_bufs[@bitCast(@as(isize, @intCast(idx)))].released = 1;
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
pub fn initialize_dec() callconv(.c) void {
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const static_local_init_done = struct {
        var init_done: c_int = 0;
    };
    _ = &static_local_init_done;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(static_local_init_done.init_done != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_rtcd();
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_dsp_rtcd();
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_scale_rtcd();
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_init_intra_predictors();
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        static_local_init_done.init_done = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn vp9_dec_setup_mi(arg_cm: [*c]VP9_COMMON) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.mi = (cm.*.mip + @as(usize, @bitCast(@as(isize, @intCast(cm.*.mi_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.mi_grid_visible = (cm.*.mi_grid_base + @as(usize, @bitCast(@as(isize, @intCast(cm.*.mi_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.mi_grid_base)), 0, @as(c_ulong, @bitCast(@as(c_long, cm.*.mi_stride * (cm.*.mi_rows + @as(c_int, 1))))) *% @sizeOf(@TypeOf(cm.*.mi_grid_base.*)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_dec_alloc_row_mt_mem(arg_row_mt_worker_data: [*c]RowMTWorkerData, arg_cm: [*c]VP9_COMMON, arg_num_sbs: c_int, arg_max_threads: c_int, arg_num_jobs: c_int) void {
    var row_mt_worker_data = arg_row_mt_worker_data;
    _ = &row_mt_worker_data;
    var cm = arg_cm;
    _ = &cm;
    var num_sbs = arg_num_sbs;
    _ = &num_sbs;
    var max_threads = arg_max_threads;
    _ = &max_threads;
    var num_jobs = arg_num_jobs;
    _ = &num_jobs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var plane: c_int = undefined;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dqcoeff_size: usize = @as(c_ulong, @bitCast(@as(c_long, num_sbs << @intCast(@as(c_int, 12))))) *% @sizeOf(@TypeOf(row_mt_worker_data.*.dqcoeff[@as(c_int, 0)].*));
    _ = &dqcoeff_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    row_mt_worker_data.*.num_jobs = num_jobs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    row_mt_worker_data.*.num_sbs = num_sbs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        plane = 0;
        while (plane < @as(c_int, 3)) : (plane += 1) {
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
                row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))] = @ptrCast(@alignCast(antfly_vp9_vpx_memalign(32, dqcoeff_size)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (!(row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))] != null)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))])), 0, dqcoeff_size);
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
                row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))] = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, num_sbs << @intCast(@as(c_int, 8)))), @sizeOf(@TypeOf(row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))].*)))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (!(row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))] != null)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
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
        row_mt_worker_data.*.partition = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, num_sbs * @as(c_int, 85))), @sizeOf(@TypeOf(row_mt_worker_data.*.partition.*)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(row_mt_worker_data.*.partition != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
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
        row_mt_worker_data.*.recon_map = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, num_sbs)), @sizeOf(@TypeOf(row_mt_worker_data.*.recon_map.*)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(row_mt_worker_data.*.recon_map != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(row_mt_worker_data.*.thread_data))) == @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const thread_size: usize = @as(c_ulong, @bitCast(@as(c_long, max_threads))) *% @sizeOf(@TypeOf(row_mt_worker_data.*.thread_data.*));
        _ = &thread_size;
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
            row_mt_worker_data.*.thread_data = @ptrCast(@alignCast(antfly_vp9_vpx_memalign(32, thread_size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(row_mt_worker_data.*.thread_data != null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (!false) break;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_dec_free_row_mt_mem(arg_row_mt_worker_data: [*c]RowMTWorkerData) void {
    var row_mt_worker_data = arg_row_mt_worker_data;
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(row_mt_worker_data))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var plane: c_int = undefined;
        _ = &plane;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            plane = 0;
            while (plane < @as(c_int, 3)) : (plane += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_free(@ptrCast(@alignCast(row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))] = null;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_free(@ptrCast(@alignCast(row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))] = null;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(row_mt_worker_data.*.partition)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        row_mt_worker_data.*.partition = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(row_mt_worker_data.*.recon_map)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        row_mt_worker_data.*.recon_map = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(row_mt_worker_data.*.thread_data)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        row_mt_worker_data.*.thread_data = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn vp9_dec_alloc_mi(arg_cm: [*c]VP9_COMMON, arg_mi_size: c_int) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var mi_size = arg_mi_size;
    _ = &mi_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.mip = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, mi_size)), @sizeOf(@TypeOf(cm.*.mip.*)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(cm.*.mip != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    }
    cm.*.mi_alloc_size = mi_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.mi_grid_base = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, mi_size)), @sizeOf([*c]MODE_INFO))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(cm.*.mi_grid_base != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    }
    return 0;
}
pub fn vp9_dec_free_mi(arg_cm: [*c]VP9_COMMON) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.mip)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.mip = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.mi_grid_base)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.mi_grid_base = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.mi_alloc_size = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_decoder_create(pool: [*c]BufferPool) [*c]VP9Decoder {
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var pbi: [*c]VP9Decoder = undefined;
    pbi = @ptrCast(@alignCast(antfly_vp9_vpx_memalign(32, @sizeOf(@TypeOf(pbi.*)))));
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const cm: [*c]VP9_COMMON = @ptrCast(@alignCast(if (pbi != null) @as(?*anyopaque, @ptrCast(@alignCast(&pbi.*.common))) else @as(?*anyopaque, null)));
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(cm != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        return null;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&pbi.*)), 0, @sizeOf(@TypeOf(pbi.*)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (false) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        cm.*.@"error".setjmp = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        antfly_vp9_vp9_decoder_remove(pbi);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        return null;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.@"error".setjmp = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        cm.*.fc = @as([*c]FRAME_CONTEXT, @ptrCast(@alignCast(antfly_vp9_vpx_calloc(1, @sizeOf(@TypeOf(cm.*.fc.*))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        if (!(cm.*.fc != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        cm.*.frame_contexts = @as([*c]FRAME_CONTEXT, @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, @as(c_int, 1) << @intCast(@as(c_int, 2)))), @sizeOf(@TypeOf(cm.*.frame_contexts.*))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        if (!(cm.*.frame_contexts != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    pbi.*.need_resync = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_codec_afz_vp9_once(initialize_dec);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&cm.*.ref_frame_map)), -@as(c_int, 1), @sizeOf(@TypeOf(cm.*.ref_frame_map)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&cm.*.next_ref_frame_map)), -@as(c_int, 1), @sizeOf(@TypeOf(cm.*.next_ref_frame_map)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    init_frame_indexes(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    pbi.*.ready_for_new_data = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    pbi.*.common.buffer_pool = pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.bit_depth = VPX_BITS_8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.dequant_bit_depth = VPX_BITS_8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.alloc_mi = vp9_dec_alloc_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.free_mi = vp9_dec_free_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.setup_mi = vp9_dec_setup_mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_vp9_vp9_loop_filter_init(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    cm.*.@"error".setjmp = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_vp9_vpx_get_worker_interface().*.init.?(&pbi.*.lf_worker);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    pbi.*.lf_worker.thread_name = "vpx lf worker";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return pbi;
}
pub export fn antfly_vp9_vp9_decoder_remove(arg_pbi: [*c]VP9Decoder) void {
    var pbi = arg_pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(pbi != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    antfly_vp9_vpx_get_worker_interface().*.end.?(&pbi.*.lf_worker);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(pbi.*.lf_worker.data1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < pbi.*.num_tile_workers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(i)))];
            _ = &worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_get_worker_interface().*.end.?(worker);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi.*.tile_worker_data)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi.*.tile_workers)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (pbi.*.num_tile_workers > @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_loop_filter_dealloc(&pbi.*.lf_row_sync);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (pbi.*.row_mt == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_dec_free_row_mt_mem(pbi.*.row_mt_worker_data);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(pbi.*.row_mt_worker_data))) != @as(?*anyopaque, null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_jobq_deinit(&pbi.*.row_mt_worker_data.*.jobq);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi.*.row_mt_worker_data.*.jobq_buf)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi.*.row_mt_worker_data)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_remove_common(&pbi.*.common);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn swap_frame_buffers(arg_pbi: [*c]VP9Decoder) callconv(.c) void {
    var pbi = arg_pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref_index: c_int = 0;
    _ = &ref_index;
    var mask: c_int = undefined;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&cm.*.buffer_pool.*.frame_bufs));
    _ = &frame_bufs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mask = pbi.*.refresh_frame_flags;
        while (mask != 0) : (mask >>= @intCast(@as(c_int, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const old_idx: c_int = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
            _ = &old_idx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            decrease_ref_count(old_idx, frame_bufs, pool);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((mask & @as(c_int, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                decrease_ref_count(old_idx, frame_bufs, pool);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] = cm.*.next_ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ref_index += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while ((ref_index < (@as(c_int, 1) << @intCast(@as(c_int, 3)))) and !(cm.*.show_existing_frame != 0)) : (ref_index += 1) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const old_idx: c_int = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
        _ = &old_idx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        decrease_ref_count(old_idx, frame_bufs, pool);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] = cm.*.next_ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pbi.*.hold_ref_buf = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.frame_to_show = get_frame_new_buffer(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].ref_count -= 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        ref_index = 0;
        while (ref_index < @as(c_int, 3)) : (ref_index += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.frame_refs[@bitCast(@as(isize, @intCast(ref_index)))].idx = -@as(c_int, 1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn release_fb_on_decoder_exit(arg_pbi: [*c]VP9Decoder) callconv(.c) void {
    var pbi = arg_pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const winterface: [*c]const VPxWorkerInterface = antfly_vp9_vpx_get_worker_interface();
    _ = &winterface;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&cm.*.buffer_pool.*.frame_bufs));
    _ = &frame_bufs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = winterface.*.sync.?(&pbi.*.lf_worker);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < pbi.*.num_tile_workers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = winterface.*.sync.?(&pbi.*.tile_workers[@bitCast(@as(isize, @intCast(i)))]);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (pbi.*.hold_ref_buf == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var ref_index: c_int = 0;
        _ = &ref_index;
        var mask: c_int = undefined;
        _ = &mask;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            mask = pbi.*.refresh_frame_flags;
            while (mask != 0) : (mask >>= @intCast(@as(c_int, 1))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const old_idx: c_int = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
                _ = &old_idx;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                decrease_ref_count(old_idx, frame_bufs, pool);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mask & @as(c_int, 1)) != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    decrease_ref_count(old_idx, frame_bufs, pool);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                ref_index += 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        while ((ref_index < (@as(c_int, 1) << @intCast(@as(c_int, 3)))) and !(cm.*.show_existing_frame != 0)) : (ref_index += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const old_idx: c_int = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
            _ = &old_idx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            decrease_ref_count(old_idx, frame_bufs, pool);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pbi.*.hold_ref_buf = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_receive_compressed_data(arg_pbi: [*c]VP9Decoder, arg_size: usize, arg_psource: [*c][*c]const u8) c_int {
    var pbi = arg_pbi;
    _ = &pbi;
    var size = arg_size;
    _ = &size;
    var psource = arg_psource;
    _ = &psource;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&cm.*.buffer_pool.*.frame_bufs));
    _ = &frame_bufs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var source: [*c]const u8 = psource.*;
    _ = &source;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var retcode: c_int = 0;
    _ = &retcode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.@"error".error_code = VPX_CODEC_OK;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (size == @as(usize, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (cm.*.frame_refs[@as(c_int, 0)].idx > @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            cm.*.frame_refs[@as(c_int, 0)].buf.*.corrupted = 1;
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
    pbi.*.ready_for_new_data = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (((cm.*.new_fb_idx >= @as(c_int, 0)) and (frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].ref_count == @as(c_int, 0))) and !(@as(c_int, frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].released) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        _ = pool.*.release_fb_cb.?(pool.*.cb_priv, &frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].raw_frame_buffer);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].released = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.new_fb_idx = get_free_fb(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.new_fb_idx == -@as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pbi.*.ready_for_new_data = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        release_fb_on_decoder_exit(pbi);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(cm.*.@"error".error_code);
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.cur_frame = &pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pbi.*.hold_ref_buf = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pbi.*.cur_buf = &frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (false) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.@"error".setjmp = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pbi.*.ready_for_new_data = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        release_fb_on_decoder_exit(pbi);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        decrease_ref_count(cm.*.new_fb_idx, frame_bufs, pool);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return -@as(c_int, 1);
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.@"error".setjmp = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    antfly_vp9_vp9_decode_frame(pbi, source, source + size, psource);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    swap_frame_buffers(pbi);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(cm.*.show_existing_frame != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.last_show_frame = cm.*.show_frame;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.prev_frame = cm.*.cur_frame;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(c_int, cm.*.seg.enabled) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_vp9_vp9_swap_current_and_last_seg_map(cm);
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
    if (cm.*.show_frame != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.cur_show_frame_fb_idx = cm.*.new_fb_idx;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    cm.*.last_width = cm.*.width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.last_height = cm.*.height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.show_frame != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.current_video_frame +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.@"error".setjmp = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return retcode;
}
pub export fn antfly_vp9_vp9_get_raw_frame(arg_pbi: [*c]VP9Decoder, arg_sd: [*c]YV12_BUFFER_CONFIG, arg_flags: [*c]vp9_ppflags_t) c_int {
    var pbi = arg_pbi;
    _ = &pbi;
    var sd = arg_sd;
    _ = &sd;
    var flags = arg_flags;
    _ = &flags;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ret: c_int = -@as(c_int, 1);
    _ = &ret;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = flags.*;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (pbi.*.ready_for_new_data == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return ret;
    }
    pbi.*.ready_for_new_data = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(cm.*.show_frame != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return ret;
    }
    pbi.*.ready_for_new_data = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    sd.* = cm.*.frame_to_show.*;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    ret = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ret;
}
pub export fn antfly_vp9_vp9_parse_superframe_index(arg_data: [*c]const u8, arg_data_sz: usize, arg_sizes: [*c]u32, arg_count: [*c]c_int, arg_decrypt_cb: vpx_decrypt_cb, arg_decrypt_state: ?*anyopaque) vpx_codec_err_t {
    var data = arg_data;
    _ = &data;
    var data_sz = arg_data_sz;
    _ = &data_sz;
    var sizes = arg_sizes;
    _ = &sizes;
    var count = arg_count;
    _ = &count;
    var decrypt_cb = arg_decrypt_cb;
    _ = &decrypt_cb;
    var decrypt_state = arg_decrypt_state;
    _ = &decrypt_state;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var marker: u8 = undefined;
    _ = &marker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    marker = read_marker(decrypt_cb, decrypt_state, (data + data_sz) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    count.* = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((@as(c_int, marker) & @as(c_int, 224)) == @as(c_int, 192)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const frames: u32 = @bitCast(@as(c_int, (@as(c_int, marker) & @as(c_int, 7)) + @as(c_int, 1)));
        _ = &frames;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const mag: u32 = @bitCast(@as(c_int, ((@as(c_int, marker) >> @intCast(@as(c_int, 3))) & @as(c_int, 3)) + @as(c_int, 1)));
        _ = &mag;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const index_sz: usize = @as(u32, 2) +% (mag *% frames);
        _ = &index_sz;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (data_sz < index_sz) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return VPX_CODEC_CORRUPT_FRAME;
        }
        {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const marker2: u8 = read_marker(decrypt_cb, decrypt_state, (data + data_sz) - index_sz);
            _ = &marker2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (@as(c_int, marker) != @as(c_int, marker2)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return VPX_CODEC_CORRUPT_FRAME;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var i: u32 = undefined;
            _ = &i;
            var j: u32 = undefined;
            _ = &j;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var x: [*c]const u8 = &data[(data_sz -% index_sz) +% @as(usize, 1)];
            _ = &x;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var clear_buffer: [32]u8 = undefined;
            _ = &clear_buffer;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (decrypt_cb != null) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                decrypt_cb.?(decrypt_state, x, @ptrCast(@alignCast(&clear_buffer)), @bitCast(@as(c_uint, @truncate(frames *% mag))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                x = @ptrCast(@alignCast(&clear_buffer));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            {
                i = 0;
                while (i < frames) : (i +%= 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    var this_sz: u32 = 0;
                    _ = &this_sz;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    {
                        j = 0;
                        while (j < mag) : (j +%= 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            this_sz |= @as(u32, (blk: {
                                const ref = &x;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            }).*) << @intCast(j *% @as(u32, 8));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    }
                    sizes[i] = this_sz;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            count.* = @bitCast(@as(c_uint, @truncate(frames)));
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

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
