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
//  *  Copyright (c) 2014 The WebM project authors. All Rights Reserved.
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
pub extern fn antfly_vp9_vpx_malloc(size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const vpx_prob = common.vpx_prob;
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_TYPES = common.PARTITION_TYPES;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const TX_MODE = common.TX_MODE;
pub const PLANE_TYPES = common.PLANE_TYPES;
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
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_component_counts = common.nmv_component_counts;
pub const nmv_context_counts = common.nmv_context_counts;
pub const VPX_CODEC_MEM_ERROR = common.VPX_CODEC_MEM_ERROR;
pub const FRAME_TYPE = common.FRAME_TYPE;
pub const b_mode_info = extern struct {
    as_mode: PREDICTION_MODE = 0,
    as_mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
};
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
pub extern fn antfly_vp9_vp9_filter_block_plane_ss00(cm: [*c]struct_VP9Common, plane: [*c]struct_macroblockd_plane, mi_row: c_int, lfm: [*c]LOOP_FILTER_MASK) void;
pub extern fn antfly_vp9_vp9_filter_block_plane_ss11(cm: [*c]struct_VP9Common, plane: [*c]struct_macroblockd_plane, mi_row: c_int, lfm: [*c]LOOP_FILTER_MASK) void;
pub extern fn antfly_vp9_vp9_filter_block_plane_non420(cm: [*c]struct_VP9Common, plane: [*c]struct_macroblockd_plane, mi_8x8: [*c][*c]MODE_INFO, mi_row: c_int, mi_col: c_int) void;
pub extern fn antfly_vp9_vp9_loop_filter_frame_init(cm: [*c]struct_VP9Common, default_filt_lvl: c_int) void;
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
pub extern fn antfly_vp9_vp9_adjust_mask(cm: [*c]struct_VP9Common, mi_row: c_int, mi_col: c_int, lfm: [*c]LOOP_FILTER_MASK) void;
pub extern fn antfly_vp9_vp9_loop_filter_data_reset(lf_data: [*c]LFWorkerData, frame_buffer: [*c]YV12_BUFFER_CONFIG, cm: [*c]struct_VP9Common, planes: [*c]const struct_macroblockd_plane) void;
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
pub const VP9LfSync = struct_VP9LfSyncData;
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
pub extern fn antfly_vp9_vp9_setup_dst_planes(planes: [*c]struct_macroblockd_plane, src: [*c]const YV12_BUFFER_CONFIG, mi_row: c_int, mi_col: c_int) void;
pub fn sync_read(lf_sync: [*c]VP9LfSync, arg_r: c_int, arg_c: c_int) callconv(.c) void {
    _ = &lf_sync;
    var r = arg_r;
    _ = &r;
    var c = arg_c;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn sync_write(lf_sync: [*c]VP9LfSync, arg_r: c_int, arg_c: c_int, sb_cols: c_int) callconv(.c) void {
    _ = &lf_sync;
    var r = arg_r;
    _ = &r;
    var c = arg_c;
    _ = &c;
    _ = &sb_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &sb_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn thread_loop_filter_rows(frame_buffer: [*c]const YV12_BUFFER_CONFIG, cm: [*c]VP9_COMMON, arg_planes: [*c]struct_macroblockd_plane, arg_start: c_int, arg_stop: c_int, arg_y_only: c_int, lf_sync: [*c]VP9LfSync) callconv(.c) void {
    _ = &frame_buffer;
    _ = &cm;
    var planes = arg_planes;
    _ = &planes;
    var start = arg_start;
    _ = &start;
    var stop = arg_stop;
    _ = &stop;
    var y_only = arg_y_only;
    _ = &y_only;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_planes: c_int = if (y_only != 0) @as(c_int, 1) else @as(c_int, 3);
    _ = &num_planes;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const sb_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols) >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_active_workers: c_int = lf_sync.*.num_active_workers;
    _ = &num_active_workers;
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
    var path: enum_lf_path = undefined;
    _ = &path;
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
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mi_row = start;
        while (mi_row < stop) : (mi_row += num_active_workers * (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mi: [*c][*c]MODE_INFO = cm.*.mi_grid_visible + @as(usize, @bitCast(@as(isize, @intCast(mi_row * cm.*.mi_stride))));
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
                    const r: c_int = mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3));
                    _ = &r;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const c: c_int = mi_col >> @intCast(@as(c_int, 6) - @as(c_int, 3));
                    _ = &c;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var plane: c_int = undefined;
                    _ = &plane;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    sync_read(lf_sync, r, c);
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
                    sync_write(lf_sync, r, c, sb_cols);
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
pub fn loop_filter_row_worker(arg_arg1: ?*anyopaque, arg_arg2: ?*anyopaque) callconv(.c) c_int {
    var arg1 = arg_arg1;
    _ = &arg1;
    var arg2 = arg_arg2;
    _ = &arg2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const lf_sync: [*c]VP9LfSync = @ptrCast(@alignCast(arg1));
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const lf_data: [*c]LFWorkerData = @ptrCast(@alignCast(arg2));
    _ = &lf_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    thread_loop_filter_rows(lf_data.*.frame_buffer, lf_data.*.cm, @ptrCast(@alignCast(&lf_data.*.planes)), lf_data.*.start, lf_data.*.stop, lf_data.*.y_only, lf_sync);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 1;
}
pub fn loop_filter_rows_mt(arg_frame: [*c]YV12_BUFFER_CONFIG, arg_cm: [*c]VP9_COMMON, arg_planes: [*c]struct_macroblockd_plane, arg_start: c_int, arg_stop: c_int, arg_y_only: c_int, arg_workers: [*c]VPxWorker, arg_nworkers: c_int, arg_lf_sync: [*c]VP9LfSync) callconv(.c) void {
    var frame = arg_frame;
    _ = &frame;
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
    var workers = arg_workers;
    _ = &workers;
    var nworkers = arg_nworkers;
    _ = &nworkers;
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const winterface: [*c]const VPxWorkerInterface = antfly_vp9_vpx_get_worker_interface();
    _ = &winterface;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const sb_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows) >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &num_tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_workers: c_int = if (nworkers < (if (num_tile_cols < sb_rows) num_tile_cols else sb_rows)) nworkers else if (num_tile_cols < sb_rows) num_tile_cols else sb_rows;
    _ = &num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((!(lf_sync.*.sync_range != 0) or (sb_rows != lf_sync.*.rows)) or (num_workers > lf_sync.*.num_workers)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_loop_filter_dealloc(lf_sync);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_loop_filter_alloc(lf_sync, cm, sb_rows, cm.*.width, num_workers);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_sync.*.num_active_workers = num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(lf_sync.*.cur_sb_col)), -@as(c_int, 1), @sizeOf(@TypeOf(lf_sync.*.cur_sb_col.*)) *% @as(c_ulong, @bitCast(@as(c_long, sb_rows))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < num_workers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const worker: [*c]VPxWorker = &workers[@bitCast(@as(isize, @intCast(i)))];
            _ = &worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const lf_data: [*c]LFWorkerData = &lf_sync.*.lfdata[@bitCast(@as(isize, @intCast(i)))];
            _ = &lf_data;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            worker.*.hook = loop_filter_row_worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            worker.*.data1 = @ptrCast(@alignCast(lf_sync));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            worker.*.data2 = @ptrCast(@alignCast(lf_data));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_loop_filter_data_reset(lf_data, frame, cm, planes);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lf_data.*.start = start + (i * (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lf_data.*.stop = stop;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            lf_data.*.y_only = y_only;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (i == (num_workers - @as(c_int, 1))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                winterface.*.execute.?(worker);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                winterface.*.launch.?(worker);
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
    {
        i = 0;
        while (i < num_workers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = winterface.*.sync.?(&workers[@bitCast(@as(isize, @intCast(i)))]);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_loop_filter_frame_mt(arg_frame: [*c]YV12_BUFFER_CONFIG, arg_cm: [*c]VP9_COMMON, arg_planes: [*c]struct_macroblockd_plane, arg_frame_filter_level: c_int, arg_y_only: c_int, arg_partial_frame: c_int, arg_workers: [*c]VPxWorker, arg_num_workers: c_int, arg_lf_sync: [*c]VP9LfSync) void {
    var frame = arg_frame;
    _ = &frame;
    var cm = arg_cm;
    _ = &cm;
    var planes = arg_planes;
    _ = &planes;
    var frame_filter_level = arg_frame_filter_level;
    _ = &frame_filter_level;
    var y_only = arg_y_only;
    _ = &y_only;
    var partial_frame = arg_partial_frame;
    _ = &partial_frame;
    var workers = arg_workers;
    _ = &workers;
    var num_workers = arg_num_workers;
    _ = &num_workers;
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var start_mi_row: c_int = undefined;
    _ = &start_mi_row;
    var end_mi_row: c_int = undefined;
    _ = &end_mi_row;
    var mi_rows_to_filter: c_int = undefined;
    _ = &mi_rows_to_filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(frame_filter_level != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    start_mi_row = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mi_rows_to_filter = cm.*.mi_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((partial_frame != 0) and (cm.*.mi_rows > @as(c_int, 8))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        start_mi_row = cm.*.mi_rows >> @intCast(@as(c_int, 1));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            const ref = &start_mi_row;
            ref.* = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, ref.*))) & @as(c_uint, 4294967288))));
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        mi_rows_to_filter = if (@divTrunc(cm.*.mi_rows, @as(c_int, 8)) > @as(c_int, 8)) @divTrunc(cm.*.mi_rows, @as(c_int, 8)) else @as(c_int, 8);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    end_mi_row = start_mi_row + mi_rows_to_filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_loop_filter_frame_init(cm, frame_filter_level);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    loop_filter_rows_mt(frame, cm, planes, start_mi_row, end_mi_row, y_only, workers, num_workers, lf_sync);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_lpf_mt_init(arg_lf_sync: [*c]VP9LfSync, arg_cm: [*c]VP9_COMMON, arg_frame_filter_level: c_int, arg_num_workers: c_int) void {
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    var cm = arg_cm;
    _ = &cm;
    var frame_filter_level = arg_frame_filter_level;
    _ = &frame_filter_level;
    var num_workers = arg_num_workers;
    _ = &num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const sb_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows) >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(frame_filter_level != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if ((!(lf_sync.*.sync_range != 0) or (sb_rows != lf_sync.*.rows)) or (num_workers > lf_sync.*.num_workers)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_loop_filter_dealloc(lf_sync);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_loop_filter_alloc(lf_sync, cm, sb_rows, cm.*.width, num_workers);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(lf_sync.*.cur_sb_col)), -@as(c_int, 1), @sizeOf(@TypeOf(lf_sync.*.cur_sb_col.*)) *% @as(c_ulong, @bitCast(@as(c_long, sb_rows))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_sync.*.corrupted = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(lf_sync.*.num_tiles_done)), 0, @sizeOf(@TypeOf(lf_sync.*.num_tiles_done.*)) *% @as(c_ulong, @bitCast(@as(c_long, sb_rows))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.lf_row = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_sync_range(arg_width: c_int) callconv(.c) c_int {
    var width = arg_width;
    _ = &width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (width < @as(c_int, 640)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (width <= @as(c_int, 1280)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return 2;
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (width <= @as(c_int, 4096)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return 4;
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return 8;
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
pub export fn antfly_vp9_vp9_loop_filter_alloc(arg_lf_sync: [*c]VP9LfSync, arg_cm: [*c]VP9_COMMON, arg_rows: c_int, arg_width: c_int, arg_num_workers: c_int) void {
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    var cm = arg_cm;
    _ = &cm;
    var rows = arg_rows;
    _ = &rows;
    var width = arg_width;
    _ = &width;
    var num_workers = arg_num_workers;
    _ = &num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_sync.*.rows = rows;
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
        lf_sync.*.lfdata = @ptrCast(@alignCast(antfly_vp9_vpx_malloc(@as(c_ulong, @bitCast(@as(c_long, num_workers))) *% @sizeOf(@TypeOf(lf_sync.*.lfdata.*)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(lf_sync.*.lfdata != null)) {
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
    lf_sync.*.num_workers = num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf_sync.*.num_active_workers = lf_sync.*.num_workers;
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
        lf_sync.*.cur_sb_col = @ptrCast(@alignCast(antfly_vp9_vpx_malloc(@sizeOf(@TypeOf(lf_sync.*.cur_sb_col.*)) *% @as(c_ulong, @bitCast(@as(c_long, rows))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(lf_sync.*.cur_sb_col != null)) {
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
        lf_sync.*.num_tiles_done = @ptrCast(@alignCast(antfly_vp9_vpx_malloc((@sizeOf(@TypeOf(lf_sync.*.num_tiles_done.*)) *% @as(c_ulong, @bitCast(@as(c_long, mi_cols_aligned_to_sb(cm.*.mi_rows))))) >> @intCast(@as(c_ulong, @bitCast(@as(c_long, @as(c_int, 6) - @as(c_int, 3))))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(lf_sync.*.num_tiles_done != null)) {
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
    lf_sync.*.sync_range = get_sync_range(width);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_loop_filter_dealloc(arg_lf_sync: [*c]VP9LfSync) void {
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(lf_sync.*.lfdata)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(lf_sync.*.cur_sb_col)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(lf_sync.*.num_tiles_done)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&lf_sync.*)), 0, @sizeOf(@TypeOf(lf_sync.*)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_next_row(arg_cm: [*c]VP9_COMMON, arg_lf_sync: [*c]VP9LfSync) callconv(.c) c_int {
    var cm = arg_cm;
    _ = &cm;
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var return_val: c_int = -@as(c_int, 1);
    _ = &return_val;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const max_rows: c_int = cm.*.mi_rows;
    _ = &max_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.lf_row < max_rows) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return_val = cm.*.lf_row;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.lf_row += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return return_val;
}
pub export fn antfly_vp9_vp9_loopfilter_rows(arg_lf_data: [*c]LFWorkerData, arg_lf_sync: [*c]VP9LfSync) void {
    var lf_data = arg_lf_data;
    _ = &lf_data;
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi_row: c_int = undefined;
    _ = &mi_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var cm: [*c]VP9_COMMON = lf_data.*.cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (((blk: {
        const tmp = get_next_row(cm, lf_sync);
        mi_row = tmp;
        break :blk tmp;
    }) != -@as(c_int, 1)) and (mi_row < cm.*.mi_rows)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        lf_data.*.start = mi_row;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        lf_data.*.stop = mi_row + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        thread_loop_filter_rows(lf_data.*.frame_buffer, lf_data.*.cm, @ptrCast(@alignCast(&lf_data.*.planes)), lf_data.*.start, lf_data.*.stop, lf_data.*.y_only, lf_sync);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_set_row(arg_lf_sync: [*c]VP9LfSync, arg_num_tiles: c_int, arg_row: c_int, arg_is_last_row: c_int, arg_corrupted: c_int) void {
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    var num_tiles = arg_num_tiles;
    _ = &num_tiles;
    var row = arg_row;
    _ = &row;
    var is_last_row = arg_is_last_row;
    _ = &is_last_row;
    var corrupted = arg_corrupted;
    _ = &corrupted;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &num_tiles;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &is_last_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &corrupted;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_loopfilter_job(arg_lf_data: [*c]LFWorkerData, arg_lf_sync: [*c]VP9LfSync) void {
    var lf_data = arg_lf_data;
    _ = &lf_data;
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    thread_loop_filter_rows(lf_data.*.frame_buffer, lf_data.*.cm, @ptrCast(@alignCast(&lf_data.*.planes)), lf_data.*.start, lf_data.*.stop, lf_data.*.y_only, lf_sync);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_accumulate_frame_counts(arg_accum: [*c]FRAME_COUNTS, arg_counts: [*c]const FRAME_COUNTS, arg_is_dec: c_int) void {
    var accum = arg_accum;
    _ = &accum;
    var counts = arg_counts;
    _ = &counts;
    var is_dec = arg_is_dec;
    _ = &is_dec;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    var k: c_int = undefined;
    _ = &k;
    var l: c_int = undefined;
    _ = &l;
    var m: c_int = undefined;
    _ = &m;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (@as(c_int, 9) + @as(c_int, 1))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.y_mode[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.y_mode[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
        while (i < (@as(c_int, 9) + @as(c_int, 1))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (@as(c_int, 9) + @as(c_int, 1))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.uv_mode[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.uv_mode[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
        while (i < (@as(c_int, 4) * @as(c_int, 4))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < PARTITION_TYPES) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.partition[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.partition[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
    if (is_dec != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var n: c_int = undefined;
        _ = &n;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4))))))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < PLANE_TYPES) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        {
                            k = 0;
                            while (k < @as(c_int, 2)) : (k += 1) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                {
                                    l = 0;
                                    while (l < @as(c_int, 6)) : (l += 1) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        {
                                            m = 0;
                                            while (m < @as(c_int, 6)) : (m += 1) {
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return;
                                                }
                                                accum.*.eob_branch[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))] +%= counts.*.eob_branch[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))];
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return;
                                                }
                                                {
                                                    n = 0;
                                                    while (n < (@as(c_int, 3) + @as(c_int, 1))) : (n += 1) {
                                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                                            return;
                                                        }
                                                        accum.*.coef[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))][@bitCast(@as(isize, @intCast(n)))] +%= counts.*.coef[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))][@bitCast(@as(isize, @intCast(n)))];
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
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4))))))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < PLANE_TYPES) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        {
                            k = 0;
                            while (k < @as(c_int, 2)) : (k += 1) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                {
                                    l = 0;
                                    while (l < @as(c_int, 6)) : (l += 1) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        {
                                            m = 0;
                                            while (m < @as(c_int, 6)) : (m += 1) {
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return;
                                                }
                                                accum.*.eob_branch[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))] +%= counts.*.eob_branch[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))];
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
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (@as(c_int, 3) + @as(c_int, 1))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 3)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.switchable_interp[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.switchable_interp[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
            {
                j = 0;
                while (j < ((@as(c_int, 1) + @as(c_int, 13)) - @as(c_int, 10))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.inter_mode[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.inter_mode[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 2)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.intra_inter[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.intra_inter[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
                    accum.*.comp_inter[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.comp_inter[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
                    {
                        k = 0;
                        while (k < @as(c_int, 2)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            accum.*.single_ref[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))] +%= counts.*.single_ref[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))];
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
                    accum.*.comp_ref[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.comp_ref[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4))))))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.tx.p32x32[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.tx.p32x32[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
                while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 1))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.tx.p16x16[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.tx.p16x16[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
                while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 2))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.tx.p8x8[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.tx.p8x8[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
    {
        i = 0;
        while (i < @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4))))))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            accum.*.tx.tx_totals[@bitCast(@as(isize, @intCast(i)))] +%= counts.*.tx.tx_totals[@bitCast(@as(isize, @intCast(i)))];
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
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 2)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    accum.*.skip[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= counts.*.skip[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            accum.*.mv.joints[@bitCast(@as(isize, @intCast(i)))] +%= counts.*.mv.joints[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        k = 0;
        while (k < @as(c_int, 2)) : (k += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const comps: [*c]nmv_component_counts = &accum.*.mv.comps[@bitCast(@as(isize, @intCast(k)))];
            _ = &comps;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const comps_t: [*c]const nmv_component_counts = &counts.*.mv.comps[@bitCast(@as(isize, @intCast(k)))];
            _ = &comps_t;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 2)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comps.*.sign[@bitCast(@as(isize, @intCast(i)))] +%= comps_t.*.sign[@bitCast(@as(isize, @intCast(i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comps.*.class0_hp[@bitCast(@as(isize, @intCast(i)))] +%= comps_t.*.class0_hp[@bitCast(@as(isize, @intCast(i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comps.*.hp[@bitCast(@as(isize, @intCast(i)))] +%= comps_t.*.hp[@bitCast(@as(isize, @intCast(i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 11)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comps.*.classes[@bitCast(@as(isize, @intCast(i)))] +%= comps_t.*.classes[@bitCast(@as(isize, @intCast(i)))];
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
                while (i < (@as(c_int, 1) << @intCast(@as(c_int, 1)))) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comps.*.class0[@bitCast(@as(isize, @intCast(i)))] +%= comps_t.*.class0[@bitCast(@as(isize, @intCast(i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        j = 0;
                        while (j < @as(c_int, 4)) : (j += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            comps.*.class0_fp[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= comps_t.*.class0_fp[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
            {
                i = 0;
                while (i < ((@as(c_int, 11) + @as(c_int, 1)) - @as(c_int, 2))) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        j = 0;
                        while (j < @as(c_int, 2)) : (j += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            comps.*.bits[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))] +%= comps_t.*.bits[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))];
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
                while (i < @as(c_int, 4)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    comps.*.fp[@bitCast(@as(isize, @intCast(i)))] +%= comps_t.*.fp[@bitCast(@as(isize, @intCast(i)))];
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

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
