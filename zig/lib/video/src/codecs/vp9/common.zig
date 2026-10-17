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
pub const ADST_ADST: c_int = 3;
pub const ADST_DCT: c_int = 1;
pub const ALLOW_32X32: c_int = 3;
pub const BD_VALUE = usize;
pub const BITSTREAM_PROFILE = enum_BITSTREAM_PROFILE;
pub const BLOCK_SIZE = u8;
pub const COMPOUND_REFERENCE: c_int = 1;
pub const DCT_ADST: c_int = 2;
pub const DCT_DCT: c_int = 0;
pub const ENTROPY_CONTEXT = u8;
pub const FRAME_TYPE = c_uint;
pub const INTERP_FILTER = u8;
pub const InternalFrameBuffer = struct_InternalFrameBuffer;
pub const InternalFrameBufferList = struct_InternalFrameBufferList;
pub const InterpKernel = [8]i16;
pub const Job = struct_Job;
pub const JobQueueRowMt = extern struct {
    buf_base: [*c]u8 = null,
    buf_wr: [*c]u8 = null,
    buf_rd: [*c]u8 = null,
    buf_end: [*c]u8 = null,
    terminate: c_int = 0,
};
pub const JobType = enum_JobType;
pub const KEY_FRAME: c_int = 0;
pub const LF_PATH_420: c_int = 0;
pub const LF_PATH_444: c_int = 1;
pub const LF_PATH_SLOW: c_int = 2;
pub const LOOP_FILTER_MASK = extern struct {
    left_y: [4]u64 = @import("std").mem.zeroes([4]u64),
    above_y: [4]u64 = @import("std").mem.zeroes([4]u64),
    int_4x4_y: u64 = 0,
    left_uv: [4]u16 = @import("std").mem.zeroes([4]u16),
    above_uv: [4]u16 = @import("std").mem.zeroes([4]u16),
    int_4x4_uv: u16 = 0,
    lfl_y: [64]u8 = @import("std").mem.zeroes([64]u8),
};
pub const LPF_JOB: c_int = 2;
pub const MAX_PROFILES: c_int = 4;
pub const MV32 = struct_mv32;
pub const MV_CLASS_0: c_int = 0;
pub const MV_CLASS_1: c_int = 1;
pub const MV_CLASS_10: c_int = 10;
pub const MV_CLASS_2: c_int = 2;
pub const MV_CLASS_3: c_int = 3;
pub const MV_CLASS_4: c_int = 4;
pub const MV_CLASS_5: c_int = 5;
pub const MV_CLASS_6: c_int = 6;
pub const MV_CLASS_7: c_int = 7;
pub const MV_CLASS_8: c_int = 8;
pub const MV_CLASS_9: c_int = 9;
pub const MV_CLASS_TYPE = c_uint;
pub const MV_JOINT_HNZVNZ: c_int = 3;
pub const MV_JOINT_HNZVZ: c_int = 1;
pub const MV_JOINT_HZVNZ: c_int = 2;
pub const MV_JOINT_TYPE = c_uint;
pub const MV_JOINT_ZERO: c_int = 0;
pub const MV_REFERENCE_FRAME = i8;
pub const ONLY_4X4: c_int = 0;
pub const PARSE: c_int = 1;
pub const PARSE_JOB: c_int = 0;
pub const PARTITION_CONTEXT = u8;
pub const PARTITION_HORZ: c_int = 1;
pub const PARTITION_NONE: c_int = 0;
pub const PARTITION_SPLIT: c_int = 3;
pub const PARTITION_TYPE = enum_PARTITION_TYPE;
pub const PARTITION_TYPES: c_int = 4;
pub const PARTITION_VERT: c_int = 2;
pub const PLANE_TYPE = c_uint;
pub const PLANE_TYPES: c_int = 2;
pub const PLANE_TYPE_Y: c_int = 0;
pub const PREDICTION_MODE = u8;
pub const PROFILE_0: c_int = 0;
pub const PROFILE_1: c_int = 1;
pub const PROFILE_2: c_int = 2;
pub const PROFILE_3: c_int = 3;
pub const RECON: c_int = 2;
pub const RECON_JOB: c_int = 1;
pub const REFERENCE_MODE = c_uint;
pub const REFERENCE_MODE_SELECT: c_int = 2;
pub const SEG_LVL_ALT_LF: c_int = 1;
pub const SEG_LVL_ALT_Q: c_int = 0;
pub const SEG_LVL_FEATURES = c_uint;
pub const SEG_LVL_MAX: c_int = 4;
pub const SEG_LVL_REF_FRAME: c_int = 2;
pub const SEG_LVL_SKIP: c_int = 3;
pub const SINGLE_REFERENCE: c_int = 0;
pub const TX_MODE = c_uint;
pub const TX_MODE_SELECT: c_int = 4;
pub const TX_SIZE = u8;
pub const TX_TYPE = c_uint;
pub const TileBuffer = struct_TileBuffer;
pub const VP8D_GET_FRAME_CORRUPTED: c_int = 257;
pub const VP8D_GET_LAST_REF_UPDATES: c_int = 256;
pub const VP8_COPY_REFERENCE: c_int = 2;
pub const VP8_DEBLOCK: c_int = 1;
pub const VP8_DEMACROBLOCK: c_int = 2;
pub const VP8_SET_POSTPROC: c_int = 3;
pub const VP8_SET_REFERENCE: c_int = 1;
pub const VP9D_GET_BIT_DEPTH: c_int = 262;
pub const VP9D_GET_DISPLAY_SIZE: c_int = 261;
pub const VP9D_GET_FRAME_SIZE: c_int = 260;
pub const VP9D_SET_LOOP_FILTER_OPT: c_int = 269;
pub const VP9D_SET_ROW_MT: c_int = 268;
pub const VP9_DECODE_SVC_SPATIAL_LAYER: c_int = 266;
pub const VP9_GET_REFERENCE: c_int = 128;
pub const VP9_INVERT_TILE_DECODE_ORDER: c_int = 264;
pub const VP9_SET_BYTE_ALIGNMENT: c_int = 263;
pub const VP9_SET_SKIP_LOOP_FILTER: c_int = 265;
pub const VPXD_GET_LAST_QUANTIZER: c_int = 267;
pub const VPXD_SET_DECRYPTOR: c_int = 259;
pub const VPX_BITS_10: c_int = 10;
pub const VPX_BITS_12: c_int = 12;
pub const VPX_BITS_8: c_int = 8;
pub const VPX_CODEC_ABI_MISMATCH: c_int = 3;
pub const VPX_CODEC_CORRUPT_FRAME: c_int = 7;
pub const VPX_CODEC_ERROR: c_int = 1;
pub const VPX_CODEC_INCAPABLE: c_int = 4;
pub const VPX_CODEC_INVALID_PARAM: c_int = 8;
pub const VPX_CODEC_MEM_ERROR: c_int = 2;
pub const VPX_CODEC_OK: c_int = 0;
pub const VPX_CODEC_UNSUP_BITSTREAM: c_int = 5;
pub const VPX_CR_FULL_RANGE: c_int = 1;
pub const VPX_CR_STUDIO_RANGE: c_int = 0;
pub const VPX_CS_BT_601: c_int = 1;
pub const VPX_CS_SRGB: c_int = 7;
pub const VPX_IMG_FMT_I420: c_int = 258;
pub const VPX_IMG_FMT_I422: c_int = 261;
pub const VPX_IMG_FMT_I440: c_int = 263;
pub const VPX_IMG_FMT_I444: c_int = 262;
pub const VPX_WORKER_STATUS_NOT_OK: c_int = 0;
pub const VPX_WORKER_STATUS_OK: c_int = 1;
pub const VPxWorkerHook = ?*const fn (?*anyopaque, ?*anyopaque) callconv(.c) c_int;
pub const VPxWorkerImpl = struct_VPxWorkerImpl;
pub const VPxWorkerStatus = c_uint;
pub const __builtin = @import("std").zig.c_translation.builtins;
pub const __helpers = @import("std").zig.c_translation.helpers;
pub const convolve_fn_t = ?*const fn (src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) callconv(.c) void;
pub const cospi_10_64: tran_coef_t = 14449;
pub const cospi_11_64: tran_coef_t = 14053;
pub const cospi_12_64: tran_coef_t = 13623;
pub const cospi_13_64: tran_coef_t = 13160;
pub const cospi_14_64: tran_coef_t = 12665;
pub const cospi_15_64: tran_coef_t = 12140;
pub const cospi_16_64: tran_coef_t = 11585;
pub const cospi_17_64: tran_coef_t = 11003;
pub const cospi_18_64: tran_coef_t = 10394;
pub const cospi_19_64: tran_coef_t = 9760;
pub const cospi_1_64: tran_coef_t = 16364;
pub const cospi_20_64: tran_coef_t = 9102;
pub const cospi_21_64: tran_coef_t = 8423;
pub const cospi_22_64: tran_coef_t = 7723;
pub const cospi_23_64: tran_coef_t = 7005;
pub const cospi_24_64: tran_coef_t = 6270;
pub const cospi_25_64: tran_coef_t = 5520;
pub const cospi_26_64: tran_coef_t = 4756;
pub const cospi_27_64: tran_coef_t = 3981;
pub const cospi_28_64: tran_coef_t = 3196;
pub const cospi_29_64: tran_coef_t = 2404;
pub const cospi_2_64: tran_coef_t = 16305;
pub const cospi_30_64: tran_coef_t = 1606;
pub const cospi_31_64: tran_coef_t = 804;
pub const cospi_3_64: tran_coef_t = 16207;
pub const cospi_4_64: tran_coef_t = 16069;
pub const cospi_5_64: tran_coef_t = 15893;
pub const cospi_6_64: tran_coef_t = 15679;
pub const cospi_7_64: tran_coef_t = 15426;
pub const cospi_8_64: tran_coef_t = 15137;
pub const cospi_9_64: tran_coef_t = 14811;
pub const count_to_update_factor: [21]c_int = [21]c_int{
    0,
    6,
    12,
    19,
    25,
    32,
    38,
    44,
    51,
    57,
    64,
    70,
    76,
    83,
    89,
    96,
    102,
    108,
    115,
    121,
    128,
};
pub const enum_BITSTREAM_PROFILE = c_uint;
pub const enum_JobType = c_uint;
pub const enum_PARTITION_TYPE = c_uint;
pub const enum_lf_path = c_uint;
pub const enum_vpx_bit_depth = c_uint;
pub const enum_vpx_codec_cx_pkt_kind = c_uint;
pub const enum_vpx_color_range = c_uint;
pub const enum_vpx_color_space = c_uint;
pub const enum_vpx_enc_pass = c_uint;
pub const enum_vpx_img_fmt = c_uint;
pub const enum_vpx_kf_mode = c_uint;
pub const enum_vpx_rc_mode = c_uint;
pub const highbd_convolve_fn_t = ?*const fn (src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) callconv(.c) void;
pub const jmp_buf = [24]c_ulong;
pub const loop_filter_thresh = extern struct {
    mblim: [16]u8 align(16) = @import("std").mem.zeroes([16]u8),
    lim: [16]u8 = @import("std").mem.zeroes([16]u8),
    hev_thr: [16]u8 = @import("std").mem.zeroes([16]u8),
};
pub const nmv_component = extern struct {
    sign: vpx_prob = 0,
    classes: [10]vpx_prob = @import("std").mem.zeroes([10]vpx_prob),
    class0: [1]vpx_prob = @import("std").mem.zeroes([1]vpx_prob),
    bits: [10]vpx_prob = @import("std").mem.zeroes([10]vpx_prob),
    class0_fp: [2][3]vpx_prob = @import("std").mem.zeroes([2][3]vpx_prob),
    fp: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    class0_hp: vpx_prob = 0,
    hp: vpx_prob = 0,
};
pub const nmv_component_counts = extern struct {
    sign: [2]c_uint = @import("std").mem.zeroes([2]c_uint),
    classes: [11]c_uint = @import("std").mem.zeroes([11]c_uint),
    class0: [2]c_uint = @import("std").mem.zeroes([2]c_uint),
    bits: [10][2]c_uint = @import("std").mem.zeroes([10][2]c_uint),
    class0_fp: [2][4]c_uint = @import("std").mem.zeroes([2][4]c_uint),
    fp: [4]c_uint = @import("std").mem.zeroes([4]c_uint),
    class0_hp: [2]c_uint = @import("std").mem.zeroes([2]c_uint),
    hp: [2]c_uint = @import("std").mem.zeroes([2]c_uint),
};
pub const nmv_context_counts = extern struct {
    joints: [4]c_uint = @import("std").mem.zeroes([4]c_uint),
    comps: [2]nmv_component_counts = @import("std").mem.zeroes([2]nmv_component_counts),
};
pub const ptrdiff_t = c_long;
pub const sinpi_1_9: tran_coef_t = 5283;
pub const sinpi_2_9: tran_coef_t = 9929;
pub const sinpi_3_9: tran_coef_t = 13377;
pub const sinpi_4_9: tran_coef_t = 15212;
pub const struct_InternalFrameBuffer = extern struct {
    data: [*c]u8 = null,
    size: usize = 0,
    in_use: c_int = 0,
};
pub const struct_InternalFrameBufferList = extern struct {
    num_internal_frame_buffers: c_int = 0,
    int_fb: [*c]InternalFrameBuffer = null,
};
pub const struct_Job = extern struct {
    row_num: c_int = 0,
    tile_col: c_int = 0,
    job_type: JobType = @import("std").mem.zeroes(JobType),
};
pub const struct_TileBuffer = extern struct {
    data: [*c]const u8 = null,
    size: usize = 0,
    col: c_int = 0,
};
pub const struct_VPxWorkerImpl = opaque {};
pub const struct___va_list_1 = extern struct {
    unnamed_0: ?*anyopaque = null,
    unnamed_1: ?*anyopaque = null,
    unnamed_2: ?*anyopaque = null,
    unnamed_3: c_int = 0,
    unnamed_4: c_int = 0,
};
pub const struct_mv32 = extern struct {
    row: i32 = 0,
    col: i32 = 0,
};
pub const struct_scale_factors = extern struct {
    x_scale_fp: c_int = 0,
    y_scale_fp: c_int = 0,
    x_step_q4: c_int = 0,
    y_step_q4: c_int = 0,
    scale_value_x: ?*const fn (val: c_int, sf: [*c]const struct_scale_factors) callconv(.c) c_int = null,
    scale_value_y: ?*const fn (val: c_int, sf: [*c]const struct_scale_factors) callconv(.c) c_int = null,
    predict: [2][2][2]convolve_fn_t = @import("std").mem.zeroes([2][2][2]convolve_fn_t),
    highbd_predict: [2][2][2]highbd_convolve_fn_t = @import("std").mem.zeroes([2][2][2]highbd_convolve_fn_t),
};
pub const struct_tx_counts = extern struct {
    p32x32: [2][4]c_uint = @import("std").mem.zeroes([2][4]c_uint),
    p16x16: [2][3]c_uint = @import("std").mem.zeroes([2][3]c_uint),
    p8x8: [2][2]c_uint = @import("std").mem.zeroes([2][2]c_uint),
    tx_totals: [4]c_uint = @import("std").mem.zeroes([4]c_uint),
};
pub const struct_vpx_codec_dec_cfg = extern struct {
    threads: c_uint = 0,
    w: c_uint = 0,
    h: c_uint = 0,
};
pub const struct_vpx_codec_enc_cfg = extern struct {
    g_usage: c_uint = 0,
    g_threads: c_uint = 0,
    g_profile: c_uint = 0,
    g_w: c_uint = 0,
    g_h: c_uint = 0,
    g_bit_depth: vpx_bit_depth_t = @import("std").mem.zeroes(vpx_bit_depth_t),
    g_input_bit_depth: c_uint = 0,
    g_timebase: struct_vpx_rational = @import("std").mem.zeroes(struct_vpx_rational),
    g_error_resilient: vpx_codec_er_flags_t = 0,
    g_pass: enum_vpx_enc_pass = @import("std").mem.zeroes(enum_vpx_enc_pass),
    g_lag_in_frames: c_uint = 0,
    rc_dropframe_thresh: c_uint = 0,
    rc_resize_allowed: c_uint = 0,
    rc_scaled_width: c_uint = 0,
    rc_scaled_height: c_uint = 0,
    rc_resize_up_thresh: c_uint = 0,
    rc_resize_down_thresh: c_uint = 0,
    rc_end_usage: enum_vpx_rc_mode = @import("std").mem.zeroes(enum_vpx_rc_mode),
    rc_twopass_stats_in: vpx_fixed_buf_t = @import("std").mem.zeroes(vpx_fixed_buf_t),
    rc_firstpass_mb_stats_in: vpx_fixed_buf_t = @import("std").mem.zeroes(vpx_fixed_buf_t),
    rc_target_bitrate: c_uint = 0,
    rc_min_quantizer: c_uint = 0,
    rc_max_quantizer: c_uint = 0,
    rc_undershoot_pct: c_uint = 0,
    rc_overshoot_pct: c_uint = 0,
    rc_buf_sz: c_uint = 0,
    rc_buf_initial_sz: c_uint = 0,
    rc_buf_optimal_sz: c_uint = 0,
    rc_2pass_vbr_bias_pct: c_uint = 0,
    rc_2pass_vbr_minsection_pct: c_uint = 0,
    rc_2pass_vbr_maxsection_pct: c_uint = 0,
    rc_2pass_vbr_corpus_complexity: c_uint = 0,
    kf_mode: enum_vpx_kf_mode = @import("std").mem.zeroes(enum_vpx_kf_mode),
    kf_min_dist: c_uint = 0,
    kf_max_dist: c_uint = 0,
    ss_number_layers: c_uint = 0,
    ss_enable_auto_alt_ref: [5]c_int = @import("std").mem.zeroes([5]c_int),
    ss_target_bitrate: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    ts_number_layers: c_uint = 0,
    ts_target_bitrate: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    ts_rate_decimator: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    ts_periodicity: c_uint = 0,
    ts_layer_id: [16]c_uint = @import("std").mem.zeroes([16]c_uint),
    layer_target_bitrate: [12]c_uint = @import("std").mem.zeroes([12]c_uint),
    temporal_layering_mode: c_int = 0,
    use_vizier_rc_params: c_int = 0,
    active_wq_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    err_per_mb_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    sr_default_decay_limit: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    sr_diff_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    kf_err_per_mb_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    kf_frame_min_boost_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    kf_frame_max_boost_first_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    kf_frame_max_boost_subs_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    kf_max_total_boost_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    gf_max_total_boost_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    gf_frame_max_boost_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    zm_factor: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    rd_mult_inter_qp_fac: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    rd_mult_arf_qp_fac: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
    rd_mult_key_qp_fac: vpx_rational_t = @import("std").mem.zeroes(vpx_rational_t),
};
pub const struct_vpx_codec_enc_cfg_map = extern struct {
    usage: c_int = 0,
    cfg: vpx_codec_enc_cfg_t = @import("std").mem.zeroes(vpx_codec_enc_cfg_t),
};
pub const struct_vpx_codec_frame_buffer = extern struct {
    data: [*c]u8 = null,
    size: usize = 0,
    priv: ?*anyopaque = null,
};
pub const struct_vpx_codec_priv_enc_mr_cfg = extern struct {
    mr_total_resolutions: c_uint = 0,
    mr_encoder_id: c_uint = 0,
    mr_down_sampling_factor: struct_vpx_rational = @import("std").mem.zeroes(struct_vpx_rational),
    mr_low_res_mode_info: ?*anyopaque = null,
};
pub const struct_vpx_codec_stream_info = extern struct {
    sz: c_uint = 0,
    w: c_uint = 0,
    h: c_uint = 0,
    is_kf: c_uint = 0,
};
pub const struct_vpx_fixed_buf = extern struct {
    buf: ?*anyopaque = null,
    sz: usize = 0,
};
pub const struct_vpx_image_rect = extern struct {
    x: c_uint = 0,
    y: c_uint = 0,
    w: c_uint = 0,
    h: c_uint = 0,
};
pub const struct_vpx_internal_error_info = extern struct {
    error_code: vpx_codec_err_t = @import("std").mem.zeroes(vpx_codec_err_t),
    has_detail: c_int = 0,
    detail: [80]u8 = @import("std").mem.zeroes([80]u8),
    setjmp: c_int = 0,
    jmp: jmp_buf = @import("std").mem.zeroes(jmp_buf),
};
pub const struct_vpx_internal_error_info_6 = opaque {};
pub const struct_vpx_psnr_pkt_8 = extern struct {
    samples: [4]c_uint = @import("std").mem.zeroes([4]c_uint),
    sse: [4]u64 = @import("std").mem.zeroes([4]u64),
    psnr: [4]f64 = @import("std").mem.zeroes([4]f64),
};
pub const struct_vpx_psnr_pkt_9 = extern struct {
    samples: [4]c_uint = @import("std").mem.zeroes([4]c_uint),
    sse: [4]u64 = @import("std").mem.zeroes([4]u64),
    psnr: [4]f64 = @import("std").mem.zeroes([4]f64),
};
pub const struct_vpx_rational = extern struct {
    num: c_int = 0,
    den: c_int = 0,
};
pub const tran_coef_t = i16;
pub const tran_high_t = i64;
pub const tran_low_t = i32;
pub const vp9_coeff_count_model = [2][6][6][4]c_uint;
pub const vp9_coeff_probs_model = [2][6][6][3]vpx_prob;
pub const antfly_vp9_vp9_mv_class_tree: [*c]const vpx_tree_index = @extern([*c]const vpx_tree_index, .{
    .name = "antfly_vp9_vp9_mv_class_tree",
});
pub const antfly_vp9_vp9_mv_fp_tree: [*c]const vpx_tree_index = @extern([*c]const vpx_tree_index, .{
    .name = "antfly_vp9_vp9_mv_fp_tree",
});
pub const antfly_vp9_vp9_mv_joint_tree: [*c]const vpx_tree_index = @extern([*c]const vpx_tree_index, .{
    .name = "antfly_vp9_vp9_mv_joint_tree",
});
pub const vp9_ppflags_t = extern struct {
    post_proc_flag: c_int = 0,
    deblocking_level: c_int = 0,
    noise_level: c_int = 0,
};
pub const vpx_bit_depth_t = enum_vpx_bit_depth;
pub const vpx_codec_caps_t = c_long;
pub const vpx_codec_dec_cfg_t = struct_vpx_codec_dec_cfg;
pub const vpx_codec_enc_cfg_map_t = struct_vpx_codec_enc_cfg_map;
pub const vpx_codec_enc_cfg_t = struct_vpx_codec_enc_cfg;
pub const vpx_codec_enc_mr_get_mem_loc_fn_t = ?*const fn (cfg: [*c]const vpx_codec_enc_cfg_t, mem_loc: [*c]?*anyopaque) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_er_flags_t = u32;
pub const vpx_codec_err_t = c_uint;
pub const vpx_codec_flags_t = c_long;
pub const vpx_codec_frame_buffer_t = struct_vpx_codec_frame_buffer;
pub const vpx_codec_frame_flags_t = u32;
pub const vpx_codec_iter_t = ?*const anyopaque;
pub const vpx_codec_peek_si_fn_t = ?*const fn (data: [*c]const u8, data_sz: c_uint, si: [*c]vpx_codec_stream_info_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_priv_enc_mr_cfg_t = struct_vpx_codec_priv_enc_mr_cfg;
pub const vpx_codec_pts_t = i64;
pub const vpx_codec_stream_info_t = struct_vpx_codec_stream_info;
pub const vpx_color_range_t = enum_vpx_color_range;
pub const vpx_color_space_t = enum_vpx_color_space;
pub const vpx_decrypt_cb = ?*const fn (decrypt_state: ?*anyopaque, input: [*c]const u8, output: [*c]u8, count: c_int) callconv(.c) void;
pub const vpx_enc_deadline_t = c_ulong;
pub const vpx_enc_frame_flags_t = c_long;
pub const vpx_fixed_buf_t = struct_vpx_fixed_buf;
pub const vpx_get_frame_buffer_cb_fn_t = ?*const fn (priv: ?*anyopaque, min_size: usize, fb: [*c]vpx_codec_frame_buffer_t) callconv(.c) c_int;
pub const vpx_image_rect_t = struct_vpx_image_rect;
pub const vpx_img_fmt_t = enum_vpx_img_fmt;
pub const vpx_prob = u8;
pub const vpx_rational_t = struct_vpx_rational;
pub const vpx_rb_error_handler = ?*const fn (data: ?*anyopaque) callconv(.c) void;
pub const vpx_release_frame_buffer_cb_fn_t = ?*const fn (priv: ?*anyopaque, fb: [*c]vpx_codec_frame_buffer_t) callconv(.c) c_int;
pub const vpx_tree_index = i8;
