// Copyright © 2018-2019, VideoLAN and dav1d authors
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
// SPDX-License-Identifier: BSD-2-Clause
// /*
//  * Copyright © 2018-2021, VideoLAN and dav1d authors
//  * Copyright © 2018, Two Orioles, LLC
//  * All rights reserved.
//  *
//  * Redistribution and use in source and binary forms, with or without
//  * modification, are permitted provided that the following conditions are met:
//  *
//  * 1. Redistributions of source code must retain the above copyright notice, this
//  *    list of conditions and the following disclaimer.
//  *
//  * 2. Redistributions in binary form must reproduce the above copyright notice,
//  *    this list of conditions and the following disclaimer in the documentation
//  *    and/or other materials provided with the distribution.
//  *
//  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//  * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
//  * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//  * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//  * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//  * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//  * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//  */
const common = @import("common.zig");
pub const __builtin = common.__builtin;
pub extern fn antfly_codec_afz_trap() void;
pub const struct___va_list_1 = common.struct___va_list_1;
pub const __builtin_va_list = struct___va_list_1;
pub const __darwin_va_list = __builtin_va_list;
pub const va_list = __darwin_va_list;
pub const ptrdiff_t = common.ptrdiff_t;
pub const atomic_int = common.atomic_int;
pub const struct_Dav1dRef = common.struct_Dav1dRef;
pub const struct_Dav1dUserData = extern struct {
    data: [*c]const u8 = null,
    ref: [*c]struct_Dav1dRef = null,
};
pub const struct_Dav1dDataProps = extern struct {
    timestamp: i64 = 0,
    duration: i64 = 0,
    offset: i64 = 0,
    size: usize = 0,
    user_data: struct_Dav1dUserData = @import("std").mem.zeroes(struct_Dav1dUserData),
};
pub const Dav1dDataProps = struct_Dav1dDataProps;
pub const struct_Dav1dData = extern struct {
    data: [*c]const u8 = null,
    sz: usize = 0,
    ref: [*c]struct_Dav1dRef = null,
    m: Dav1dDataProps = @import("std").mem.zeroes(Dav1dDataProps),
};
pub const Dav1dData = struct_Dav1dData;
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

pub fn imax(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a > b) a else b;
}
pub fn imin(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub fn iclip(v: c_int, min: c_int, max: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &min;
    _ = &max;
    return if (v < min) min else if (v > max) max else v;
}
pub fn iclip_u8(v: c_int) callconv(.c) c_int {
    _ = &v;
    return iclip(v, 0, 255);
}
pub fn ulog2(v: c_uint) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 31) ^ clz(v);
}
pub extern fn antfly_codec_afz_malloc(__size: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_realloc(__ptr: ?*anyopaque, __size: usize) ?*anyopaque;
pub const atomic_uint = common.atomic_uint;
pub extern fn antfly_codec_afz_atomic_load(p: ?*const anyopaque) c_int;
pub extern fn antfly_codec_afz_atomic_store(p: ?*anyopaque, v: c_int) void;
pub extern fn antfly_codec_afz_atomic_add(p: ?*anyopaque, v: c_int) c_int;
pub extern fn antfly_codec_afz_atomic_compare(p: ?*anyopaque, expected: ?*anyopaque, desired: c_int) c_int;
pub const Dav1dRef = struct_Dav1dRef;
pub const DAV1D_PIXEL_LAYOUT_I400 = common.DAV1D_PIXEL_LAYOUT_I400;
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
pub const DAV1D_PIXEL_LAYOUT_I422 = common.DAV1D_PIXEL_LAYOUT_I422;
pub const DAV1D_PIXEL_LAYOUT_I444 = common.DAV1D_PIXEL_LAYOUT_I444;
pub const enum_Dav1dPixelLayout = common.enum_Dav1dPixelLayout;
pub const DAV1D_COLOR_PRI_BT709 = common.DAV1D_COLOR_PRI_BT709;
pub const DAV1D_COLOR_PRI_UNKNOWN = common.DAV1D_COLOR_PRI_UNKNOWN;
pub const enum_Dav1dColorPrimaries = common.enum_Dav1dColorPrimaries;
pub const DAV1D_TRC_UNKNOWN = common.DAV1D_TRC_UNKNOWN;
pub const DAV1D_TRC_SRGB = common.DAV1D_TRC_SRGB;
pub const enum_Dav1dTransferCharacteristics = common.enum_Dav1dTransferCharacteristics;
pub const DAV1D_MC_IDENTITY = common.DAV1D_MC_IDENTITY;
pub const DAV1D_MC_UNKNOWN = common.DAV1D_MC_UNKNOWN;
pub const enum_Dav1dMatrixCoefficients = common.enum_Dav1dMatrixCoefficients;
pub const DAV1D_CHR_UNKNOWN = common.DAV1D_CHR_UNKNOWN;
pub const enum_Dav1dChromaSamplePosition = common.enum_Dav1dChromaSamplePosition;
pub const struct_Dav1dSequenceHeaderOperatingPoint_3 = common.struct_Dav1dSequenceHeaderOperatingPoint_3;
pub const DAV1D_ADAPTIVE = common.DAV1D_ADAPTIVE;
pub const enum_Dav1dAdaptiveBoolean = common.enum_Dav1dAdaptiveBoolean;
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_4 = common.struct_Dav1dSequenceHeaderOperatingParameterInfo_4;
pub const struct_Dav1dSequenceHeader = extern struct {
    profile: u8 = 0,
    max_width: c_int = 0,
    max_height: c_int = 0,
    layout: enum_Dav1dPixelLayout = @import("std").mem.zeroes(enum_Dav1dPixelLayout),
    pri: enum_Dav1dColorPrimaries = @import("std").mem.zeroes(enum_Dav1dColorPrimaries),
    trc: enum_Dav1dTransferCharacteristics = @import("std").mem.zeroes(enum_Dav1dTransferCharacteristics),
    mtrx: enum_Dav1dMatrixCoefficients = @import("std").mem.zeroes(enum_Dav1dMatrixCoefficients),
    chr: enum_Dav1dChromaSamplePosition = @import("std").mem.zeroes(enum_Dav1dChromaSamplePosition),
    hbd: u8 = 0,
    color_range: u8 = 0,
    num_operating_points: u8 = 0,
    operating_points: [32]struct_Dav1dSequenceHeaderOperatingPoint_3 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingPoint_3),
    still_picture: u8 = 0,
    reduced_still_picture_header: u8 = 0,
    timing_info_present: u8 = 0,
    num_units_in_tick: u32 = 0,
    time_scale: u32 = 0,
    equal_picture_interval: u8 = 0,
    num_ticks_per_picture: u32 = 0,
    decoder_model_info_present: u8 = 0,
    encoder_decoder_buffer_delay_length: u8 = 0,
    num_units_in_decoding_tick: u32 = 0,
    buffer_removal_delay_length: u8 = 0,
    frame_presentation_delay_length: u8 = 0,
    display_model_info_present: u8 = 0,
    width_n_bits: u8 = 0,
    height_n_bits: u8 = 0,
    frame_id_numbers_present: u8 = 0,
    delta_frame_id_n_bits: u8 = 0,
    frame_id_n_bits: u8 = 0,
    sb128: u8 = 0,
    filter_intra: u8 = 0,
    intra_edge_filter: u8 = 0,
    inter_intra: u8 = 0,
    masked_compound: u8 = 0,
    warped_motion: u8 = 0,
    dual_filter: u8 = 0,
    order_hint: u8 = 0,
    jnt_comp: u8 = 0,
    ref_frame_mvs: u8 = 0,
    screen_content_tools: enum_Dav1dAdaptiveBoolean = @import("std").mem.zeroes(enum_Dav1dAdaptiveBoolean),
    force_integer_mv: enum_Dav1dAdaptiveBoolean = @import("std").mem.zeroes(enum_Dav1dAdaptiveBoolean),
    order_hint_n_bits: u8 = 0,
    super_res: u8 = 0,
    cdef: u8 = 0,
    restoration: u8 = 0,
    ss_hor: u8 = 0,
    ss_ver: u8 = 0,
    monochrome: u8 = 0,
    color_description_present: u8 = 0,
    separate_uv_delta_q: u8 = 0,
    film_grain_present: u8 = 0,
    operating_parameter_info: [32]struct_Dav1dSequenceHeaderOperatingParameterInfo_4 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingParameterInfo_4),
};
pub const Dav1dSequenceHeader = struct_Dav1dSequenceHeader;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
const struct_unnamed_5 = extern struct {
    data: Dav1dFilmGrainData = @import("std").mem.zeroes(Dav1dFilmGrainData),
    present: u8 = 0,
    update: u8 = 0,
};
pub const DAV1D_FRAME_TYPE_KEY = common.DAV1D_FRAME_TYPE_KEY;
pub const DAV1D_FRAME_TYPE_INTER = common.DAV1D_FRAME_TYPE_INTER;
pub const DAV1D_FRAME_TYPE_INTRA = common.DAV1D_FRAME_TYPE_INTRA;
pub const DAV1D_FRAME_TYPE_SWITCH = common.DAV1D_FRAME_TYPE_SWITCH;
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const struct_Dav1dFrameHeaderOperatingPoint_6 = common.struct_Dav1dFrameHeaderOperatingPoint_6;
const struct_unnamed_7 = extern struct {
    width_scale_denominator: u8 = 0,
    enabled: u8 = 0,
};
pub const DAV1D_FILTER_SWITCHABLE = common.DAV1D_FILTER_SWITCHABLE;
pub const enum_Dav1dFilterMode = common.enum_Dav1dFilterMode;
const struct_unnamed_8 = extern struct {
    uniform: u8 = 0,
    n_bytes: u8 = 0,
    min_log2_cols: u8 = 0,
    max_log2_cols: u8 = 0,
    log2_cols: u8 = 0,
    cols: u8 = 0,
    min_log2_rows: u8 = 0,
    max_log2_rows: u8 = 0,
    log2_rows: u8 = 0,
    rows: u8 = 0,
    col_start_sb: [65]u16 = @import("std").mem.zeroes([65]u16),
    row_start_sb: [65]u16 = @import("std").mem.zeroes([65]u16),
    update: u16 = 0,
};
const struct_unnamed_9 = extern struct {
    yac: u8 = 0,
    ydc_delta: i8 = 0,
    udc_delta: i8 = 0,
    uac_delta: i8 = 0,
    vdc_delta: i8 = 0,
    vac_delta: i8 = 0,
    qm: u8 = 0,
    qm_y: u8 = 0,
    qm_u: u8 = 0,
    qm_v: u8 = 0,
};
pub const Dav1dSegmentationData = common.Dav1dSegmentationData;
pub const Dav1dSegmentationDataSet = common.Dav1dSegmentationDataSet;
const struct_unnamed_10 = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    temporal: u8 = 0,
    update_data: u8 = 0,
    seg_data: Dav1dSegmentationDataSet = @import("std").mem.zeroes(Dav1dSegmentationDataSet),
    lossless: [8]u8 = @import("std").mem.zeroes([8]u8),
    qidx: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_12 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
};
const struct_unnamed_13 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
    multi: u8 = 0,
};
const struct_unnamed_11 = extern struct {
    q: struct_unnamed_12 = @import("std").mem.zeroes(struct_unnamed_12),
    lf: struct_unnamed_13 = @import("std").mem.zeroes(struct_unnamed_13),
};
pub const Dav1dLoopfilterModeRefDeltas = common.Dav1dLoopfilterModeRefDeltas;
const struct_unnamed_14 = extern struct {
    level_y: [2]u8 = @import("std").mem.zeroes([2]u8),
    level_u: u8 = 0,
    level_v: u8 = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    mode_ref_deltas: Dav1dLoopfilterModeRefDeltas = @import("std").mem.zeroes(Dav1dLoopfilterModeRefDeltas),
    sharpness: u8 = 0,
};
const struct_unnamed_15 = extern struct {
    damping: u8 = 0,
    n_bits: u8 = 0,
    y_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
    uv_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
};
pub const DAV1D_RESTORATION_NONE = common.DAV1D_RESTORATION_NONE;
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
const struct_unnamed_16 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const DAV1D_TX_4X4_ONLY = common.DAV1D_TX_4X4_ONLY;
pub const DAV1D_TX_LARGEST = common.DAV1D_TX_LARGEST;
pub const DAV1D_TX_SWITCHABLE = common.DAV1D_TX_SWITCHABLE;
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
pub const DAV1D_WM_TYPE_IDENTITY = common.DAV1D_WM_TYPE_IDENTITY;
pub const DAV1D_WM_TYPE_TRANSLATION = common.DAV1D_WM_TYPE_TRANSLATION;
pub const DAV1D_WM_TYPE_ROT_ZOOM = common.DAV1D_WM_TYPE_ROT_ZOOM;
pub const DAV1D_WM_TYPE_AFFINE = common.DAV1D_WM_TYPE_AFFINE;
pub const enum_Dav1dWarpedMotionType = common.enum_Dav1dWarpedMotionType;
const struct_unnamed_18 = extern struct {
    alpha: i16 = 0,
    beta: i16 = 0,
    gamma: i16 = 0,
    delta: i16 = 0,
};
const union_unnamed_17 = extern union {
    p: struct_unnamed_18,
    abcd: [4]i16,
};
pub const struct_Dav1dWarpedMotionParams = extern struct {
    type: enum_Dav1dWarpedMotionType = @import("std").mem.zeroes(enum_Dav1dWarpedMotionType),
    matrix: [6]i32 = @import("std").mem.zeroes([6]i32),
    u: union_unnamed_17 = @import("std").mem.zeroes(union_unnamed_17),
};
pub const Dav1dWarpedMotionParams = struct_Dav1dWarpedMotionParams;
pub const struct_Dav1dFrameHeader = extern struct {
    film_grain: struct_unnamed_5 = @import("std").mem.zeroes(struct_unnamed_5),
    frame_type: enum_Dav1dFrameType = @import("std").mem.zeroes(enum_Dav1dFrameType),
    width: [2]c_int = @import("std").mem.zeroes([2]c_int),
    height: c_int = 0,
    frame_offset: u8 = 0,
    temporal_id: u8 = 0,
    spatial_id: u8 = 0,
    show_existing_frame: u8 = 0,
    existing_frame_idx: u8 = 0,
    frame_id: u32 = 0,
    frame_presentation_delay: u32 = 0,
    show_frame: u8 = 0,
    showable_frame: u8 = 0,
    error_resilient_mode: u8 = 0,
    disable_cdf_update: u8 = 0,
    allow_screen_content_tools: u8 = 0,
    force_integer_mv: u8 = 0,
    frame_size_override: u8 = 0,
    primary_ref_frame: u8 = 0,
    buffer_removal_time_present: u8 = 0,
    operating_points: [32]struct_Dav1dFrameHeaderOperatingPoint_6 = @import("std").mem.zeroes([32]struct_Dav1dFrameHeaderOperatingPoint_6),
    refresh_frame_flags: u8 = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    super_res: struct_unnamed_7 = @import("std").mem.zeroes(struct_unnamed_7),
    have_render_size: u8 = 0,
    allow_intrabc: u8 = 0,
    frame_ref_short_signaling: u8 = 0,
    refidx: [7]i8 = @import("std").mem.zeroes([7]i8),
    hp: u8 = 0,
    subpel_filter_mode: enum_Dav1dFilterMode = @import("std").mem.zeroes(enum_Dav1dFilterMode),
    switchable_motion_mode: u8 = 0,
    use_ref_frame_mvs: u8 = 0,
    refresh_context: u8 = 0,
    tiling: struct_unnamed_8 = @import("std").mem.zeroes(struct_unnamed_8),
    quant: struct_unnamed_9 = @import("std").mem.zeroes(struct_unnamed_9),
    segmentation: struct_unnamed_10 = @import("std").mem.zeroes(struct_unnamed_10),
    delta: struct_unnamed_11 = @import("std").mem.zeroes(struct_unnamed_11),
    all_lossless: u8 = 0,
    loopfilter: struct_unnamed_14 = @import("std").mem.zeroes(struct_unnamed_14),
    cdef: struct_unnamed_15 = @import("std").mem.zeroes(struct_unnamed_15),
    restoration: struct_unnamed_16 = @import("std").mem.zeroes(struct_unnamed_16),
    txfm_mode: enum_Dav1dTxfmMode = @import("std").mem.zeroes(enum_Dav1dTxfmMode),
    switchable_comp_refs: u8 = 0,
    skip_mode_allowed: u8 = 0,
    skip_mode_enabled: u8 = 0,
    skip_mode_refs: [2]i8 = @import("std").mem.zeroes([2]i8),
    warp_motion: u8 = 0,
    reduced_txtp_set: u8 = 0,
    gmv: [7]Dav1dWarpedMotionParams = @import("std").mem.zeroes([7]Dav1dWarpedMotionParams),
};
pub const Dav1dFrameHeader = struct_Dav1dFrameHeader;
pub const Dav1dPictureParameters = common.Dav1dPictureParameters;
pub const Dav1dContentLightLevel = common.Dav1dContentLightLevel;
pub const Dav1dMasteringDisplay = common.Dav1dMasteringDisplay;
pub const struct_Dav1dITUTT35 = common.struct_Dav1dITUTT35;
pub const Dav1dITUTT35 = common.Dav1dITUTT35;
pub const struct_Dav1dPicture = extern struct {
    seq_hdr: [*c]Dav1dSequenceHeader = null,
    frame_hdr: [*c]Dav1dFrameHeader = null,
    data: [3]?*anyopaque = @import("std").mem.zeroes([3]?*anyopaque),
    stride: [2]ptrdiff_t = @import("std").mem.zeroes([2]ptrdiff_t),
    p: Dav1dPictureParameters = @import("std").mem.zeroes(Dav1dPictureParameters),
    m: Dav1dDataProps = @import("std").mem.zeroes(Dav1dDataProps),
    content_light: [*c]Dav1dContentLightLevel = null,
    mastering_display: [*c]Dav1dMasteringDisplay = null,
    itut_t35: [*c]Dav1dITUTT35 = null,
    n_itut_t35: usize = 0,
    reserved: [4]usize = @import("std").mem.zeroes([4]usize),
    frame_hdr_ref: [*c]struct_Dav1dRef = null,
    seq_hdr_ref: [*c]struct_Dav1dRef = null,
    content_light_ref: [*c]struct_Dav1dRef = null,
    mastering_display_ref: [*c]struct_Dav1dRef = null,
    itut_t35_ref: [*c]struct_Dav1dRef = null,
    reserved_ref: [4]usize = @import("std").mem.zeroes([4]usize),
    ref: [*c]struct_Dav1dRef = null,
    allocator_data: ?*anyopaque = null,
};
pub const Dav1dPicture = struct_Dav1dPicture;
pub const PICTURE_FLAG_NEW_SEQUENCE = common.PICTURE_FLAG_NEW_SEQUENCE;
pub const PICTURE_FLAG_NEW_OP_PARAMS_INFO = common.PICTURE_FLAG_NEW_OP_PARAMS_INFO;
pub const PICTURE_FLAG_NEW_TEMPORAL_UNIT = common.PICTURE_FLAG_NEW_TEMPORAL_UNIT;
pub const enum_PictureFlags = common.enum_PictureFlags;
pub const struct_Dav1dThreadPicture = extern struct {
    p: Dav1dPicture = @import("std").mem.zeroes(Dav1dPicture),
    visible: c_int = 0,
    showable: c_int = 0,
    flags: enum_PictureFlags = @import("std").mem.zeroes(enum_PictureFlags),
    progress: [*c]atomic_uint = null,
};
pub const Dav1dThreadPicture = struct_Dav1dThreadPicture;
const struct_unnamed_19 = extern struct {
    y: i16 = 0,
    x: i16 = 0,
};
pub const union_mv = extern union {
    unnamed_0: struct_unnamed_19,
    n: u32,
};
pub const mv = union_mv;
pub const struct_refmvs_temporal_block = extern struct {
    mv: mv align(1) = @import("std").mem.zeroes(mv),
    ref: i8 = 0,
};
pub const refmvs_temporal_block = struct_refmvs_temporal_block;
pub const CdfCoefContext = common.CdfCoefContext;
pub const struct_CdfModeContext = extern struct {
    uv_mode: [2][13][16]u16 align(32) = @import("std").mem.zeroes([2][13][16]u16),
    partition: [5][4][16]u16 = @import("std").mem.zeroes([5][4][16]u16),
    cfl_alpha: [6][16]u16 = @import("std").mem.zeroes([6][16]u16),
    txtp_inter1: [2][16]u16 = @import("std").mem.zeroes([2][16]u16),
    txtp_inter2: [16]u16 = @import("std").mem.zeroes([16]u16),
    txtp_intra1: [2][13][8]u16 = @import("std").mem.zeroes([2][13][8]u16),
    txtp_intra2: [3][13][8]u16 = @import("std").mem.zeroes([3][13][8]u16),
    cfl_sign: [8]u16 = @import("std").mem.zeroes([8]u16),
    angle_delta: [8][8]u16 = @import("std").mem.zeroes([8][8]u16),
    filter_intra: [8]u16 = @import("std").mem.zeroes([8]u16),
    seg_id: [3][8]u16 = @import("std").mem.zeroes([3][8]u16),
    pal_sz: [2][7][8]u16 = @import("std").mem.zeroes([2][7][8]u16),
    color_map: [2][7][5][8]u16 = @import("std").mem.zeroes([2][7][5][8]u16),
    txsz: [4][3][4]u16 = @import("std").mem.zeroes([4][3][4]u16),
    delta_q: [4]u16 = @import("std").mem.zeroes([4]u16),
    delta_lf: [5][4]u16 = @import("std").mem.zeroes([5][4]u16),
    restore_switchable: [4]u16 = @import("std").mem.zeroes([4]u16),
    restore_wiener: [2]u16 = @import("std").mem.zeroes([2]u16),
    restore_sgrproj: [2]u16 = @import("std").mem.zeroes([2]u16),
    txtp_inter3: [4][2]u16 = @import("std").mem.zeroes([4][2]u16),
    use_filter_intra: [22][2]u16 = @import("std").mem.zeroes([22][2]u16),
    txpart: [7][3][2]u16 = @import("std").mem.zeroes([7][3][2]u16),
    skip: [3][2]u16 = @import("std").mem.zeroes([3][2]u16),
    pal_y: [7][3][2]u16 = @import("std").mem.zeroes([7][3][2]u16),
    pal_uv: [2][2]u16 = @import("std").mem.zeroes([2][2]u16),
    intrabc: [2]u16 = @import("std").mem.zeroes([2]u16),
    y_mode: [4][16]u16 align(32) = @import("std").mem.zeroes([4][16]u16),
    wedge_idx: [9][16]u16 = @import("std").mem.zeroes([9][16]u16),
    comp_inter_mode: [8][8]u16 = @import("std").mem.zeroes([8][8]u16),
    filter: [2][8][4]u16 = @import("std").mem.zeroes([2][8][4]u16),
    interintra_mode: [4][4]u16 = @import("std").mem.zeroes([4][4]u16),
    motion_mode: [22][4]u16 = @import("std").mem.zeroes([22][4]u16),
    skip_mode: [3][2]u16 = @import("std").mem.zeroes([3][2]u16),
    newmv_mode: [6][2]u16 = @import("std").mem.zeroes([6][2]u16),
    globalmv_mode: [2][2]u16 = @import("std").mem.zeroes([2][2]u16),
    refmv_mode: [6][2]u16 = @import("std").mem.zeroes([6][2]u16),
    drl_bit: [3][2]u16 = @import("std").mem.zeroes([3][2]u16),
    intra: [4][2]u16 = @import("std").mem.zeroes([4][2]u16),
    comp: [5][2]u16 = @import("std").mem.zeroes([5][2]u16),
    comp_dir: [5][2]u16 = @import("std").mem.zeroes([5][2]u16),
    jnt_comp: [6][2]u16 = @import("std").mem.zeroes([6][2]u16),
    mask_comp: [6][2]u16 = @import("std").mem.zeroes([6][2]u16),
    wedge_comp: [9][2]u16 = @import("std").mem.zeroes([9][2]u16),
    ref: [6][3][2]u16 = @import("std").mem.zeroes([6][3][2]u16),
    comp_fwd_ref: [3][3][2]u16 = @import("std").mem.zeroes([3][3][2]u16),
    comp_bwd_ref: [2][3][2]u16 = @import("std").mem.zeroes([2][3][2]u16),
    comp_uni_ref: [3][3][2]u16 = @import("std").mem.zeroes([3][3][2]u16),
    seg_pred: [3][2]u16 = @import("std").mem.zeroes([3][2]u16),
    interintra: [7][2]u16 = @import("std").mem.zeroes([7][2]u16),
    interintra_wedge: [7][2]u16 = @import("std").mem.zeroes([7][2]u16),
    obmc: [22][2]u16 = @import("std").mem.zeroes([22][2]u16),
};
pub const CdfModeContext = struct_CdfModeContext;
pub const CdfMvContext = common.CdfMvContext;
pub const struct_CdfContext = extern struct {
    coef: CdfCoefContext = @import("std").mem.zeroes(CdfCoefContext),
    m: CdfModeContext = @import("std").mem.zeroes(CdfModeContext),
    mv: CdfMvContext = @import("std").mem.zeroes(CdfMvContext),
    kfym: [5][5][16]u16 = @import("std").mem.zeroes([5][5][16]u16),
};
pub const CdfContext = struct_CdfContext;
const union_unnamed_20 = extern union {
    cdf: [*c]CdfContext,
    qcat: c_uint,
};
pub const struct_CdfThreadContext = extern struct {
    ref: [*c]Dav1dRef = null,
    data: union_unnamed_20 = @import("std").mem.zeroes(union_unnamed_20),
    progress: [*c]atomic_uint = null,
};
pub const CdfThreadContext = struct_CdfThreadContext;
pub const struct_Dav1dTileGroup = extern struct {
    data: Dav1dData = @import("std").mem.zeroes(Dav1dData),
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_ScalableMotionParams_21 = extern struct {
    scale: c_int = 0,
    step: c_int = 0,
};
pub const Dav1dFrameContext = struct_Dav1dFrameContext;
pub const ec_win = common.ec_win;
pub const struct_MsacContext = extern struct {
    buf_pos: [*c]const u8 = null,
    buf_end: [*c]const u8 = null,
    dif: ec_win = 0,
    rng: c_uint = 0,
    cnt: c_int = 0,
    allow_update_cdf: c_int = 0,
};
pub const MsacContext = struct_MsacContext;
const struct_unnamed_22 = extern struct {
    col_start: c_int = 0,
    col_end: c_int = 0,
    row_start: c_int = 0,
    row_end: c_int = 0,
    col: c_int = 0,
    row: c_int = 0,
};
pub const coef = anyopaque;
const struct_unnamed_23 = extern struct {
    pal_idx: [*c]u8 = null,
    cbi: [*c]i16 = null,
    cf: ?*coef = null,
};
const union_unnamed_24 = extern union {
    i8: [4]i8,
    u32: u32,
};
pub const Av1RestorationUnit = common.Av1RestorationUnit;
pub const struct_Dav1dTileState = extern struct {
    cdf: CdfContext = @import("std").mem.zeroes(CdfContext),
    msac: MsacContext = @import("std").mem.zeroes(MsacContext),
    tiling: struct_unnamed_22 = @import("std").mem.zeroes(struct_unnamed_22),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    frame_thread: [2]struct_unnamed_23 = @import("std").mem.zeroes([2]struct_unnamed_23),
    lowest_pixel: [*c][7][2]c_int = null,
    dqmem: [8][3][2]u16 = @import("std").mem.zeroes([8][3][2]u16),
    dq: [*c][3][2]u16 = null,
    last_qidx: c_int = 0,
    last_delta_lf: union_unnamed_24 = @import("std").mem.zeroes(union_unnamed_24),
    lflvlmem: [8][4][8][2]u8 = @import("std").mem.zeroes([8][4][8][2]u8),
    lflvl: [*c][4][8][2]u8 = null,
    lr_ref: [3][*c]Av1RestorationUnit = @import("std").mem.zeroes([3][*c]Av1RestorationUnit),
};
pub const Dav1dTileState = struct_Dav1dTileState;
pub const struct_BlockContext = extern struct {
    mode: [32]u8 align(8) = @import("std").mem.zeroes([32]u8),
    lcoef: [32]u8 = @import("std").mem.zeroes([32]u8),
    ccoef: [2][32]u8 = @import("std").mem.zeroes([2][32]u8),
    seg_pred: [32]u8 = @import("std").mem.zeroes([32]u8),
    skip: [32]u8 = @import("std").mem.zeroes([32]u8),
    skip_mode: [32]u8 = @import("std").mem.zeroes([32]u8),
    intra: [32]u8 = @import("std").mem.zeroes([32]u8),
    comp_type: [32]u8 = @import("std").mem.zeroes([32]u8),
    ref: [2][32]i8 = @import("std").mem.zeroes([2][32]i8),
    filter: [2][32]u8 = @import("std").mem.zeroes([2][32]u8),
    tx_intra: [32]i8 = @import("std").mem.zeroes([32]i8),
    tx: [32]i8 = @import("std").mem.zeroes([32]i8),
    tx_lpf_y: [32]u8 = @import("std").mem.zeroes([32]u8),
    tx_lpf_uv: [32]u8 = @import("std").mem.zeroes([32]u8),
    partition: [16]u8 = @import("std").mem.zeroes([16]u8),
    uvmode: [32]u8 = @import("std").mem.zeroes([32]u8),
    pal_sz: [32]u8 = @import("std").mem.zeroes([32]u8),
};
pub const BlockContext = struct_BlockContext;
pub const union_refmvs_mvpair = extern union {
    mv: [2]mv,
    n: u64,
};
pub const refmvs_mvpair = union_refmvs_mvpair;
pub const refmvs_refpair = common.refmvs_refpair;
pub const struct_refmvs_block = extern struct {
    mv: refmvs_mvpair align(1) = @import("std").mem.zeroes(refmvs_mvpair),
    ref: refmvs_refpair align(1) = @import("std").mem.zeroes(refmvs_refpair),
    bs: u8 = 0,
    mf: u8 = 0,
};
pub const refmvs_block = struct_refmvs_block;
pub const struct_refmvs_frame = extern struct {
    frm_hdr: [*c]const Dav1dFrameHeader = null,
    iw4: c_int = 0,
    ih4: c_int = 0,
    iw8: c_int = 0,
    ih8: c_int = 0,
    sbsz: c_int = 0,
    use_ref_frame_mvs: c_int = 0,
    sign_bias: [7]u8 = @import("std").mem.zeroes([7]u8),
    mfmv_sign: [7]u8 = @import("std").mem.zeroes([7]u8),
    pocdiff: [7]i8 = @import("std").mem.zeroes([7]i8),
    mfmv_ref: [3]u8 = @import("std").mem.zeroes([3]u8),
    mfmv_ref2cur: [3]c_int = @import("std").mem.zeroes([3]c_int),
    mfmv_ref2ref: [3][7]c_int = @import("std").mem.zeroes([3][7]c_int),
    n_mfmvs: c_int = 0,
    n_blocks: c_int = 0,
    rp: [*c]refmvs_temporal_block = null,
    rp_ref: [*c]const [*c]refmvs_temporal_block = null,
    rp_proj: [*c]refmvs_temporal_block = null,
    rp_stride: ptrdiff_t = 0,
    r: [*c]refmvs_block = null,
    n_tile_threads: c_int = 0,
    n_frame_threads: c_int = 0,
};
pub const refmvs_frame = struct_refmvs_frame;
const struct_unnamed_25 = extern struct {
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_refmvs_tile = extern struct {
    rf: [*c]const refmvs_frame = null,
    r: [37][*c]refmvs_block = @import("std").mem.zeroes([37][*c]refmvs_block),
    rp_proj: [*c]refmvs_temporal_block = null,
    tile_col: struct_unnamed_25 = @import("std").mem.zeroes(struct_unnamed_25),
    tile_row: struct_unnamed_25 = @import("std").mem.zeroes(struct_unnamed_25),
};
pub const refmvs_tile = struct_refmvs_tile;
const union_unnamed_26 = extern union {
    cf_8bpc: [1024]i16 align(64),
    cf_16bpc: [1024]i32 align(64),
};
const union_unnamed_27 = extern union {
    al_pal_8bpc: [2][32][3][8]u8,
    al_pal_16bpc: [2][32][3][8]u16,
};
const struct_unnamed_31 = extern struct {
    compinter: [2][16384]i16 = @import("std").mem.zeroes([2][16384]i16),
    seg_mask: [16384]u8 = @import("std").mem.zeroes([16384]u8),
};
const union_unnamed_30 = extern union {
    lap_8bpc: [4096]u8,
    lap_16bpc: [4096]u16,
    unnamed_0: struct_unnamed_31,
};
const union_unnamed_32 = extern union {
    emu_edge_8bpc: [84160]u8,
    emu_edge_16bpc: [84160]u16,
};
const struct_unnamed_29 = extern struct {
    unnamed_0: union_unnamed_30 = @import("std").mem.zeroes(union_unnamed_30),
    unnamed_1: union_unnamed_32 = @import("std").mem.zeroes(union_unnamed_32),
};
const struct_unnamed_35 = extern struct {
    pal_order: [64][8]u8 = @import("std").mem.zeroes([64][8]u8),
    pal_ctx: [64]u8 = @import("std").mem.zeroes([64]u8),
};
const union_unnamed_34 = extern union {
    levels: [1088]u8,
    unnamed_0: struct_unnamed_35,
};
const union_unnamed_36 = extern union {
    ac: [1024]i16,
    txtp_map: [1024]u8,
};
const struct_unnamed_38 = extern struct {
    interintra_8bpc: [4096]u8 align(8) = @import("std").mem.zeroes([4096]u8),
    edge_8bpc: [257]u8 = @import("std").mem.zeroes([257]u8),
    pal_8bpc: [3][8]u8 align(8) = @import("std").mem.zeroes([3][8]u8),
};
const struct_unnamed_39 = extern struct {
    interintra_16bpc: [4096]u16 align(16) = @import("std").mem.zeroes([4096]u16),
    edge_16bpc: [257]u16 = @import("std").mem.zeroes([257]u16),
    pal_16bpc: [3][8]u16 align(16) = @import("std").mem.zeroes([3][8]u16),
};
const union_unnamed_37 = extern union {
    unnamed_0: struct_unnamed_38,
    unnamed_1: struct_unnamed_39,
};
const struct_unnamed_33 = extern struct {
    unnamed_0: union_unnamed_34 = @import("std").mem.zeroes(union_unnamed_34),
    unnamed_1: union_unnamed_36 = @import("std").mem.zeroes(union_unnamed_36),
    pal_idx_y: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    pal_idx_uv: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    unnamed_2: union_unnamed_37 = @import("std").mem.zeroes(union_unnamed_37),
};
const union_unnamed_28 = extern union {
    unnamed_0: struct_unnamed_29 align(64),
    unnamed_1: struct_unnamed_33 align(64),
};
pub const Av1Filter = common.Av1Filter;
pub const enum_Filter2d = common.enum_Filter2d;
const struct_unnamed_40 = extern struct {
    pass: c_int = 0,
};
pub const afz_pthread_cond_t = common.afz_pthread_cond_t;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const struct_thread_data = common.struct_thread_data;
pub const enum_TaskType = common.enum_TaskType;
const struct_unnamed_45 = extern struct {
    grain_lut_8bpc: [3][74][82]i8 align(64) = @import("std").mem.zeroes([3][74][82]i8),
    scaling_8bpc: [3][256]u8 align(64) = @import("std").mem.zeroes([3][256]u8),
};
const struct_unnamed_46 = extern struct {
    grain_lut_16bpc: [3][74][82]i16 align(64) = @import("std").mem.zeroes([3][74][82]i16),
    scaling_16bpc: [3][4096]u8 align(64) = @import("std").mem.zeroes([3][4096]u8),
};
const union_unnamed_44 = extern union {
    unnamed_0: struct_unnamed_45,
    unnamed_1: struct_unnamed_46,
};
const struct_unnamed_43 = extern struct {
    exec: c_int = 0,
    finished: c_int = 0,
    cond: afz_pthread_cond_t = 0,
    in: [*c]const Dav1dPicture = null,
    out: [*c]Dav1dPicture = null,
    type: enum_TaskType = @import("std").mem.zeroes(enum_TaskType),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    unnamed_0: union_unnamed_44 = @import("std").mem.zeroes(union_unnamed_44),
};
pub const struct_TaskThreadData_42 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    first: atomic_uint = 0,
    cur: c_uint = 0,
    reset_task_cur: atomic_uint = 0,
    cond_signaled: atomic_int = 0,
    delayed_fg: struct_unnamed_43 = @import("std").mem.zeroes(struct_unnamed_43),
    inited: c_int = 0,
};
pub const struct_FrameTileThreadData_47 = extern struct {
    lowest_pixel_mem: [*c][7][2]c_int = null,
    lowest_pixel_mem_sz: c_int = 0,
};
const struct_unnamed_41 = extern struct {
    td: struct_thread_data = @import("std").mem.zeroes(struct_thread_data),
    ttd: [*c]struct_TaskThreadData_42 = null,
    fttd: [*c]struct_FrameTileThreadData_47 = null,
    flushed: c_int = 0,
    die: c_int = 0,
};
pub const struct_Dav1dTaskContext = extern struct {
    c: [*c]const Dav1dContext = null,
    f: [*c]const Dav1dFrameContext = null,
    ts: [*c]Dav1dTileState = null,
    bx: c_int = 0,
    by: c_int = 0,
    l: BlockContext = @import("std").mem.zeroes(BlockContext),
    a: [*c]BlockContext = null,
    rt: refmvs_tile = @import("std").mem.zeroes(refmvs_tile),
    unnamed_0: union_unnamed_26 = @import("std").mem.zeroes(union_unnamed_26),
    unnamed_1: union_unnamed_27 = @import("std").mem.zeroes(union_unnamed_27),
    pal_sz_uv: [2][32]u8 = @import("std").mem.zeroes([2][32]u8),
    scratch: union_unnamed_28 = @import("std").mem.zeroes(union_unnamed_28),
    warpmv: Dav1dWarpedMotionParams = @import("std").mem.zeroes(Dav1dWarpedMotionParams),
    lf_mask: [*c]Av1Filter = null,
    top_pre_cdef_toggle: c_int = 0,
    cur_sb_cdef_idx_ptr: [*c]i8 = null,
    tl_4x4_filter: enum_Filter2d = @import("std").mem.zeroes(enum_Filter2d),
    frame_thread: struct_unnamed_40 = @import("std").mem.zeroes(struct_unnamed_40),
    task_thread: struct_unnamed_41 = @import("std").mem.zeroes(struct_unnamed_41),
};
pub const Dav1dTaskContext = struct_Dav1dTaskContext;
pub const Dav1dMemPoolBuffer = common.Dav1dMemPoolBuffer;
pub const struct_Dav1dMemPool = extern struct {
    lock: afz_pthread_mutex_t = 0,
    buf: [*c]Dav1dMemPoolBuffer = null,
    ref_cnt: c_int = 0,
    end: c_int = 0,
};
pub const Dav1dMemPool = struct_Dav1dMemPool;
const struct_unnamed_48 = extern struct {
    out_delayed: [*c]Dav1dThreadPicture = null,
    next: c_uint = 0,
};
const struct_unnamed_49 = extern struct {
    p: Dav1dThreadPicture = @import("std").mem.zeroes(Dav1dThreadPicture),
    segmap: [*c]Dav1dRef = null,
    refmvs: [*c]Dav1dRef = null,
    refpoc: [7]c_uint = @import("std").mem.zeroes([7]c_uint),
};
pub const entry = i8;
pub const generate_grain_y_fn = ?*const fn (buf: [*c][82]entry, data: [*c]const Dav1dFilmGrainData) callconv(.c) void;
pub const generate_grain_uv_fn = ?*const fn (buf: [*c][82]entry, buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize) callconv(.c) void;
pub const pixel = u8;
pub const fgy_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int) callconv(.c) void;
pub const fguv_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, luma_row: [*c]const pixel, luma_stride: ptrdiff_t, uv_pl: c_int, is_id: c_int) callconv(.c) void;
pub const struct_Dav1dFilmGrainDSPContext = extern struct {
    generate_grain_y: generate_grain_y_fn = null,
    generate_grain_uv: [3]generate_grain_uv_fn = @import("std").mem.zeroes([3]generate_grain_uv_fn),
    fgy_32x32xn: fgy_32x32xn_fn = null,
    fguv_32x32xn: [3]fguv_32x32xn_fn = @import("std").mem.zeroes([3]fguv_32x32xn_fn),
};
pub const Dav1dFilmGrainDSPContext = struct_Dav1dFilmGrainDSPContext;
pub const angular_ipred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, angle: c_int, max_width: c_int, max_height: c_int) callconv(.c) void;
pub const cfl_ac_fn = ?*const fn (ac: [*c]i16, y: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, cw: c_int, ch: c_int) callconv(.c) void;
pub const cfl_pred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, ac: [*c]const i16, alpha: c_int) callconv(.c) void;
pub const pal_pred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, pal: [*c]const pixel, idx: [*c]const u8, w: c_int, h: c_int) callconv(.c) void;
pub const struct_Dav1dIntraPredDSPContext = extern struct {
    intra_pred: [14]angular_ipred_fn = @import("std").mem.zeroes([14]angular_ipred_fn),
    cfl_ac: [3]cfl_ac_fn = @import("std").mem.zeroes([3]cfl_ac_fn),
    cfl_pred: [6]cfl_pred_fn = @import("std").mem.zeroes([6]cfl_pred_fn),
    pal_pred: pal_pred_fn = null,
};
pub const Dav1dIntraPredDSPContext = struct_Dav1dIntraPredDSPContext;
pub const mc_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int) callconv(.c) void;
pub const mc_scaled_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int) callconv(.c) void;
pub const mct_fn = ?*const fn (tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int) callconv(.c) void;
pub const mct_scaled_fn = ?*const fn (tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int) callconv(.c) void;
pub const avg_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int) callconv(.c) void;
pub const w_avg_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, weight: c_int) callconv(.c) void;
pub const mask_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, mask: [*c]const u8) callconv(.c) void;
pub const w_mask_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, mask: [*c]u8, sign: c_int) callconv(.c) void;
pub const blend_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp: [*c]const pixel, w: c_int, h: c_int, mask: [*c]const u8) callconv(.c) void;
pub const blend_dir_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp: [*c]const pixel, w: c_int, h: c_int) callconv(.c) void;
pub const warp8x8_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, mx: c_int, my: c_int) callconv(.c) void;
pub const warp8x8t_fn = ?*const fn (tmp: [*c]i16, tmp_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, mx: c_int, my: c_int) callconv(.c) void;
pub const emu_edge_fn = ?*const fn (bw: isize, bh: isize, iw: isize, ih: isize, x: isize, y: isize, dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t) callconv(.c) void;
pub const resize_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, dst_w: c_int, h: c_int, src_w: c_int, dx: c_int, mx: c_int) callconv(.c) void;
pub const struct_Dav1dMCDSPContext = extern struct {
    mc: [10]mc_fn = @import("std").mem.zeroes([10]mc_fn),
    mc_scaled: [10]mc_scaled_fn = @import("std").mem.zeroes([10]mc_scaled_fn),
    mct: [10]mct_fn = @import("std").mem.zeroes([10]mct_fn),
    mct_scaled: [10]mct_scaled_fn = @import("std").mem.zeroes([10]mct_scaled_fn),
    avg: avg_fn = null,
    w_avg: w_avg_fn = null,
    mask: mask_fn = null,
    w_mask: [3]w_mask_fn = @import("std").mem.zeroes([3]w_mask_fn),
    blend: blend_fn = null,
    blend_v: blend_dir_fn = null,
    blend_h: blend_dir_fn = null,
    warp8x8: warp8x8_fn = null,
    warp8x8t: warp8x8t_fn = null,
    emu_edge: emu_edge_fn = null,
    resize: resize_fn = null,
};
pub const Dav1dMCDSPContext = struct_Dav1dMCDSPContext;
pub const itxfm_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, coeff: ?*coef, eob: c_int) callconv(.c) void;
pub const struct_Dav1dInvTxfmDSPContext = extern struct {
    itxfm_add: [19][17]itxfm_fn = @import("std").mem.zeroes([19][17]itxfm_fn),
};
pub const Dav1dInvTxfmDSPContext = struct_Dav1dInvTxfmDSPContext;
pub const Av1FilterLUT = common.Av1FilterLUT;
pub const loopfilter_sb_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, mask: [*c]const u32, lvl: [*c][4]u8, lvl_stride: ptrdiff_t, lut: [*c]const Av1FilterLUT, w: c_int) callconv(.c) void;
pub const struct_Dav1dLoopFilterDSPContext = extern struct {
    loop_filter_sb: [2][2]loopfilter_sb_fn = @import("std").mem.zeroes([2][2]loopfilter_sb_fn),
};
pub const Dav1dLoopFilterDSPContext = struct_Dav1dLoopFilterDSPContext;
pub const cdef_dir_fn = ?*const fn (dst: [*c]const pixel, dst_stride: ptrdiff_t, @"var": [*c]c_uint) callconv(.c) c_int;
pub const const_left_pixel_row_2px = ?*const anyopaque;
pub const enum_CdefEdgeFlags = common.enum_CdefEdgeFlags;
pub const cdef_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, left: const_left_pixel_row_2px, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, edges: enum_CdefEdgeFlags) callconv(.c) void;
pub const struct_Dav1dCdefDSPContext = extern struct {
    dir: cdef_dir_fn = null,
    fb: [3]cdef_fn = @import("std").mem.zeroes([3]cdef_fn),
};
pub const Dav1dCdefDSPContext = struct_Dav1dCdefDSPContext;
pub const const_left_pixel_row = ?*const anyopaque;
const struct_unnamed_50 = extern struct {
    s0: u32 = 0,
    s1: u32 = 0,
    w0: i16 = 0,
    w1: i16 = 0,
};
pub const union_LooprestorationParams = extern union {
    filter: [2][8]i16 align(16),
    sgr: struct_unnamed_50 align(16),
};
pub const LooprestorationParams = union_LooprestorationParams;
pub const enum_LrEdgeFlags = common.enum_LrEdgeFlags;
pub const looprestorationfilter_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, left: const_left_pixel_row, lpf: [*c]const pixel, w: c_int, h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags) callconv(.c) void;
pub const struct_Dav1dLoopRestorationDSPContext = extern struct {
    wiener: [2]looprestorationfilter_fn = @import("std").mem.zeroes([2]looprestorationfilter_fn),
    sgr: [3]looprestorationfilter_fn = @import("std").mem.zeroes([3]looprestorationfilter_fn),
};
pub const Dav1dLoopRestorationDSPContext = struct_Dav1dLoopRestorationDSPContext;
pub const struct_Dav1dDSPContext = extern struct {
    fg: Dav1dFilmGrainDSPContext = @import("std").mem.zeroes(Dav1dFilmGrainDSPContext),
    ipred: Dav1dIntraPredDSPContext = @import("std").mem.zeroes(Dav1dIntraPredDSPContext),
    mc: Dav1dMCDSPContext = @import("std").mem.zeroes(Dav1dMCDSPContext),
    itx: Dav1dInvTxfmDSPContext = @import("std").mem.zeroes(Dav1dInvTxfmDSPContext),
    lf: Dav1dLoopFilterDSPContext = @import("std").mem.zeroes(Dav1dLoopFilterDSPContext),
    cdef: Dav1dCdefDSPContext = @import("std").mem.zeroes(Dav1dCdefDSPContext),
    lr: Dav1dLoopRestorationDSPContext = @import("std").mem.zeroes(Dav1dLoopRestorationDSPContext),
};
pub const Dav1dDSPContext = struct_Dav1dDSPContext;
pub const Dav1dPalDSPContext = common.Dav1dPalDSPContext;
pub const load_tmvs_fn = ?*const fn (rf: [*c]const refmvs_frame, tile_row_idx: c_int, col_start8: c_int, col_end8: c_int, row_start8: c_int, row_end8: c_int) callconv(.c) void;
pub const save_tmvs_fn = ?*const fn (rp: [*c]refmvs_temporal_block, stride: ptrdiff_t, rr: [*c]const [*c]refmvs_block, ref_sign: [*c]const u8, col_end8: c_int, row_end8: c_int, col_start8: c_int, row_start8: c_int) callconv(.c) void;
pub const splat_mv_fn = ?*const fn (rr: [*c][*c]refmvs_block, rmv: [*c]const refmvs_block, bx4: c_int, bw4: c_int, bh4: c_int) callconv(.c) void;
pub const struct_Dav1dRefmvsDSPContext = extern struct {
    load_tmvs: load_tmvs_fn = null,
    save_tmvs: save_tmvs_fn = null,
    splat_mv: splat_mv_fn = null,
};
pub const Dav1dRefmvsDSPContext = struct_Dav1dRefmvsDSPContext;
pub const struct_Dav1dPicAllocator = extern struct {
    cookie: ?*anyopaque = null,
    alloc_picture_callback: ?*const fn (pic: [*c]Dav1dPicture, cookie: ?*anyopaque) callconv(.c) c_int = null,
    release_picture_callback: ?*const fn (pic: [*c]Dav1dPicture, cookie: ?*anyopaque) callconv(.c) void = null,
};
pub const Dav1dPicAllocator = struct_Dav1dPicAllocator;
pub const enum_Dav1dInloopFilterType = common.enum_Dav1dInloopFilterType;
pub const DAV1D_DECODEFRAMETYPE_REFERENCE = common.DAV1D_DECODEFRAMETYPE_REFERENCE;
pub const DAV1D_DECODEFRAMETYPE_INTRA = common.DAV1D_DECODEFRAMETYPE_INTRA;
pub const enum_Dav1dDecodeFrameType = common.enum_Dav1dDecodeFrameType;
pub const enum_Dav1dEventFlags = common.enum_Dav1dEventFlags;
pub const struct_Dav1dLogger = extern struct {
    cookie: ?*anyopaque = null,
    callback: ?*const fn (cookie: ?*anyopaque, format: [*c]const u8, ap: va_list) callconv(.c) void = null,
};
pub const Dav1dLogger = struct_Dav1dLogger;
pub const struct_Dav1dContext = extern struct {
    fc: [*c]Dav1dFrameContext = null,
    n_fc: c_uint = 0,
    tc: [*c]Dav1dTaskContext = null,
    n_tc: c_uint = 0,
    tile: [*c]struct_Dav1dTileGroup = null,
    n_tile_data_alloc: c_int = 0,
    n_tile_data: c_int = 0,
    n_tiles: c_int = 0,
    seq_hdr_pool: [*c]Dav1dMemPool = null,
    seq_hdr_ref: [*c]Dav1dRef = null,
    seq_hdr: [*c]Dav1dSequenceHeader = null,
    frame_hdr_pool: [*c]Dav1dMemPool = null,
    frame_hdr_ref: [*c]Dav1dRef = null,
    frame_hdr: [*c]Dav1dFrameHeader = null,
    content_light_ref: [*c]Dav1dRef = null,
    content_light: [*c]Dav1dContentLightLevel = null,
    mastering_display_ref: [*c]Dav1dRef = null,
    mastering_display: [*c]Dav1dMasteringDisplay = null,
    itut_t35_ref: [*c]Dav1dRef = null,
    itut_t35: [*c]Dav1dITUTT35 = null,
    n_itut_t35: c_int = 0,
    in: Dav1dData = @import("std").mem.zeroes(Dav1dData),
    out: Dav1dThreadPicture = @import("std").mem.zeroes(Dav1dThreadPicture),
    cache: Dav1dThreadPicture = @import("std").mem.zeroes(Dav1dThreadPicture),
    flush_mem: atomic_int = 0,
    flush: [*c]atomic_int = null,
    frame_thread: struct_unnamed_48 = @import("std").mem.zeroes(struct_unnamed_48),
    task_thread: struct_TaskThreadData_42 = @import("std").mem.zeroes(struct_TaskThreadData_42),
    segmap_pool: [*c]Dav1dMemPool = null,
    refmvs_pool: [*c]Dav1dMemPool = null,
    refs: [8]struct_unnamed_49 = @import("std").mem.zeroes([8]struct_unnamed_49),
    cdf_pool: [*c]Dav1dMemPool = null,
    cdf: [8]CdfThreadContext = @import("std").mem.zeroes([8]CdfThreadContext),
    dsp: [3]Dav1dDSPContext = @import("std").mem.zeroes([3]Dav1dDSPContext),
    pal_dsp: Dav1dPalDSPContext = @import("std").mem.zeroes(Dav1dPalDSPContext),
    refmvs_dsp: Dav1dRefmvsDSPContext = @import("std").mem.zeroes(Dav1dRefmvsDSPContext),
    allocator: Dav1dPicAllocator = @import("std").mem.zeroes(Dav1dPicAllocator),
    apply_grain: c_int = 0,
    operating_point: c_int = 0,
    operating_point_idc: c_uint = 0,
    all_layers: c_int = 0,
    max_spatial_id: c_int = 0,
    frame_size_limit: c_uint = 0,
    strict_std_compliance: c_int = 0,
    output_invisible_frames: c_int = 0,
    inloop_filters: enum_Dav1dInloopFilterType = @import("std").mem.zeroes(enum_Dav1dInloopFilterType),
    decode_frame_type: enum_Dav1dDecodeFrameType = @import("std").mem.zeroes(enum_Dav1dDecodeFrameType),
    drain: c_int = 0,
    frame_flags: enum_PictureFlags = @import("std").mem.zeroes(enum_PictureFlags),
    event_flags: enum_Dav1dEventFlags = @import("std").mem.zeroes(enum_Dav1dEventFlags),
    cached_error_props: Dav1dDataProps = @import("std").mem.zeroes(Dav1dDataProps),
    cached_error: c_int = 0,
    logger: Dav1dLogger = @import("std").mem.zeroes(Dav1dLogger),
    picture_pool: [*c]Dav1dMemPool = null,
    pic_ctx_pool: [*c]Dav1dMemPool = null,
};
pub const Dav1dContext = struct_Dav1dContext;
pub const enum_BlockSize = common.enum_BlockSize;
pub const enum_EdgeFlags = common.enum_EdgeFlags;
const struct_unnamed_53 = extern struct {
    y_mode: u8 = 0,
    uv_mode: u8 = 0,
    tx: u8 = 0,
    pal_sz: [2]u8 = @import("std").mem.zeroes([2]u8),
    y_angle: i8 = 0,
    uv_angle: i8 = 0,
    cfl_alpha: [2]i8 = @import("std").mem.zeroes([2]i8),
};
const struct_unnamed_56 = extern struct {
    mv: [2]union_mv = @import("std").mem.zeroes([2]union_mv),
    wedge_idx: u8 = 0,
    mask_sign: u8 = 0,
    interintra_mode: u8 = 0,
};
const struct_unnamed_57 = extern struct {
    mv2d: union_mv = @import("std").mem.zeroes(union_mv),
    matrix: [4]i16 = @import("std").mem.zeroes([4]i16),
};
const union_unnamed_55 = extern union {
    unnamed_0: struct_unnamed_56,
    unnamed_1: struct_unnamed_57,
};
const struct_unnamed_54 = extern struct {
    unnamed_0: union_unnamed_55 = @import("std").mem.zeroes(union_unnamed_55),
    comp_type: u8 = 0,
    inter_mode: u8 = 0,
    motion_mode: u8 = 0,
    drl_idx: u8 = 0,
    ref: [2]i8 = @import("std").mem.zeroes([2]i8),
    max_ytx: u8 = 0,
    filter2d: u8 = 0,
    interintra_type: u8 = 0,
    tx_split0: u8 = 0,
    tx_split1: u16 = 0,
};
const union_unnamed_52 = extern union {
    unnamed_0: struct_unnamed_53,
    unnamed_1: struct_unnamed_54,
};
pub const struct_Av1Block = extern struct {
    bl: u8 = 0,
    bs: u8 = 0,
    bp: u8 = 0,
    intra: u8 = 0,
    seg_id: u8 = 0,
    skip_mode: u8 = 0,
    skip: u8 = 0,
    uvtx: u8 = 0,
    unnamed_0: union_unnamed_52 = @import("std").mem.zeroes(union_unnamed_52),
};
pub const Av1Block = struct_Av1Block;
pub const recon_b_intra_fn = ?*const fn (t: [*c]Dav1dTaskContext, bs: enum_BlockSize, intra_edge_flags: enum_EdgeFlags, b: [*c]const Av1Block) callconv(.c) void;
pub const recon_b_inter_fn = ?*const fn (t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) callconv(.c) c_int;
pub const filter_sbrow_fn = ?*const fn (f: [*c]Dav1dFrameContext, sby: c_int) callconv(.c) void;
pub const backup_ipred_edge_fn = ?*const fn (t: [*c]Dav1dTaskContext) callconv(.c) void;
pub const read_coef_blocks_fn = ?*const fn (t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) callconv(.c) void;
pub const copy_pal_block_fn = ?*const fn (t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) callconv(.c) void;
pub const read_pal_plane_fn = ?*const fn (t: [*c]Dav1dTaskContext, b: [*c]Av1Block, pl: c_int, sz_ctx: c_int, bx4: c_int, by4: c_int) callconv(.c) void;
pub const read_pal_uv_fn = ?*const fn (t: [*c]Dav1dTaskContext, b: [*c]Av1Block, sz_ctx: c_int, bx4: c_int, by4: c_int) callconv(.c) void;
const struct_unnamed_51 = extern struct {
    recon_b_intra: recon_b_intra_fn = null,
    recon_b_inter: recon_b_inter_fn = null,
    filter_sbrow: filter_sbrow_fn = null,
    filter_sbrow_deblock_cols: filter_sbrow_fn = null,
    filter_sbrow_deblock_rows: filter_sbrow_fn = null,
    filter_sbrow_cdef: ?*const fn (tc: [*c]Dav1dTaskContext, sby: c_int) callconv(.c) void = null,
    filter_sbrow_resize: filter_sbrow_fn = null,
    filter_sbrow_lr: filter_sbrow_fn = null,
    backup_ipred_edge: backup_ipred_edge_fn = null,
    read_coef_blocks: read_coef_blocks_fn = null,
    copy_pal_block_y: copy_pal_block_fn = null,
    copy_pal_block_uv: copy_pal_block_fn = null,
    read_pal_plane: read_pal_plane_fn = null,
    read_pal_uv: read_pal_uv_fn = null,
};
const struct_unnamed_58 = extern struct {
    next_tile_row: [2]c_int = @import("std").mem.zeroes([2]c_int),
    entropy_progress: atomic_int = 0,
    deblock_progress: atomic_int = 0,
    frame_progress: [*c]atomic_uint = null,
    copy_lpf_progress: [*c]atomic_uint = null,
    b: [*c]Av1Block = null,
    cbi: [*c]i16 = null,
    pal: [*c][3][8]pixel = null,
    pal_idx: [*c]u8 = null,
    cf: ?*coef = null,
    prog_sz: c_int = 0,
    cbi_sz: c_int = 0,
    pal_sz: c_int = 0,
    pal_idx_sz: c_int = 0,
    cf_sz: c_int = 0,
    tile_start_off: [*c]c_uint = null,
};
pub const Av1Restoration = common.Av1Restoration;
const struct_unnamed_59 = extern struct {
    level: [*c][4]u8 align(16) = null,
    mask: [*c]Av1Filter = null,
    lr_mask: [*c]Av1Restoration = null,
    mask_sz: c_int = 0,
    lr_mask_sz: c_int = 0,
    cdef_buf_plane_sz: [2]c_int = @import("std").mem.zeroes([2]c_int),
    cdef_buf_sbh: c_int = 0,
    lr_buf_plane_sz: [2]c_int = @import("std").mem.zeroes([2]c_int),
    re_sz: c_int = 0,
    lim_lut: Av1FilterLUT align(16) = @import("std").mem.zeroes(Av1FilterLUT),
    lvl: [8][4][8][2]u8 = @import("std").mem.zeroes([8][4][8][2]u8),
    last_sharpness: c_int = 0,
    tx_lpf_right_edge: [2][*c]u8 = @import("std").mem.zeroes([2][*c]u8),
    cdef_line_buf: [*c]u8 = null,
    lr_line_buf: [*c]u8 = null,
    cdef_line: [2][3][*c]pixel = @import("std").mem.zeroes([2][3][*c]pixel),
    cdef_lpf_line: [3][*c]pixel = @import("std").mem.zeroes([3][*c]pixel),
    lr_lpf_line: [3][*c]pixel = @import("std").mem.zeroes([3][*c]pixel),
    start_of_tile_row: [*c]u8 = null,
    start_of_tile_row_sz: c_int = 0,
    need_cdef_lpf_copy: c_int = 0,
    p: [3][*c]pixel = @import("std").mem.zeroes([3][*c]pixel),
    sr_p: [3][*c]pixel = @import("std").mem.zeroes([3][*c]pixel),
    restore_planes: c_int = 0,
};
pub const struct_Dav1dTask = extern struct {
    frame_idx: c_uint = 0,
    type: enum_TaskType = @import("std").mem.zeroes(enum_TaskType),
    sby: c_int = 0,
    recon_progress: c_int = 0,
    deblock_progress: c_int = 0,
    deps_skip: c_int = 0,
    next: [*c]struct_Dav1dTask = null,
};
pub const Dav1dTask = struct_Dav1dTask;
const struct_unnamed_61 = extern struct {
    merge: atomic_int = 0,
    lock: afz_pthread_mutex_t = 0,
    head: [*c]Dav1dTask = null,
    tail: [*c]Dav1dTask = null,
};
const struct_unnamed_60 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    ttd: [*c]struct_TaskThreadData_42 = null,
    tasks: [*c]struct_Dav1dTask = null,
    tile_tasks: [2][*c]struct_Dav1dTask = @import("std").mem.zeroes([2][*c]struct_Dav1dTask),
    init_task: struct_Dav1dTask = @import("std").mem.zeroes(struct_Dav1dTask),
    num_tasks: c_int = 0,
    num_tile_tasks: c_int = 0,
    init_done: atomic_int = 0,
    done: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    retval: c_int = 0,
    update_set: c_int = 0,
    @"error": atomic_int = 0,
    task_counter: atomic_int = 0,
    task_head: [*c]struct_Dav1dTask = null,
    task_tail: [*c]struct_Dav1dTask = null,
    task_cur_prev: [*c]struct_Dav1dTask = null,
    pending_tasks: struct_unnamed_61 = @import("std").mem.zeroes(struct_unnamed_61),
};
pub const struct_Dav1dFrameContext = extern struct {
    seq_hdr_ref: [*c]Dav1dRef = null,
    seq_hdr: [*c]Dav1dSequenceHeader = null,
    frame_hdr_ref: [*c]Dav1dRef = null,
    frame_hdr: [*c]Dav1dFrameHeader = null,
    refp: [7]Dav1dThreadPicture = @import("std").mem.zeroes([7]Dav1dThreadPicture),
    cur: Dav1dPicture = @import("std").mem.zeroes(Dav1dPicture),
    sr_cur: Dav1dThreadPicture = @import("std").mem.zeroes(Dav1dThreadPicture),
    mvs_ref: [*c]Dav1dRef = null,
    mvs: [*c]refmvs_temporal_block = null,
    ref_mvs: [7][*c]refmvs_temporal_block = @import("std").mem.zeroes([7][*c]refmvs_temporal_block),
    ref_mvs_ref: [7][*c]Dav1dRef = @import("std").mem.zeroes([7][*c]Dav1dRef),
    cur_segmap_ref: [*c]Dav1dRef = null,
    prev_segmap_ref: [*c]Dav1dRef = null,
    cur_segmap: [*c]u8 = null,
    prev_segmap: [*c]const u8 = null,
    refpoc: [7]c_uint = @import("std").mem.zeroes([7]c_uint),
    refrefpoc: [7][7]c_uint = @import("std").mem.zeroes([7][7]c_uint),
    gmv_warp_allowed: [7]u8 = @import("std").mem.zeroes([7]u8),
    in_cdf: CdfThreadContext = @import("std").mem.zeroes(CdfThreadContext),
    out_cdf: CdfThreadContext = @import("std").mem.zeroes(CdfThreadContext),
    tile: [*c]struct_Dav1dTileGroup = null,
    n_tile_data_alloc: c_int = 0,
    n_tile_data: c_int = 0,
    svc: [7][2]struct_ScalableMotionParams_21 = @import("std").mem.zeroes([7][2]struct_ScalableMotionParams_21),
    resize_step: [2]c_int = @import("std").mem.zeroes([2]c_int),
    resize_start: [2]c_int = @import("std").mem.zeroes([2]c_int),
    c: [*c]const Dav1dContext = null,
    ts: [*c]Dav1dTileState = null,
    n_ts: c_int = 0,
    dsp: [*c]const Dav1dDSPContext = null,
    bd_fn: struct_unnamed_51 = @import("std").mem.zeroes(struct_unnamed_51),
    ipred_edge_sz: c_int = 0,
    ipred_edge: [3][*c]pixel = @import("std").mem.zeroes([3][*c]pixel),
    b4_stride: ptrdiff_t = 0,
    w4: c_int = 0,
    h4: c_int = 0,
    bw: c_int = 0,
    bh: c_int = 0,
    sb128w: c_int = 0,
    sb128h: c_int = 0,
    sbh: c_int = 0,
    sb_shift: c_int = 0,
    sb_step: c_int = 0,
    sr_sb128w: c_int = 0,
    dq: [8][3][2]u16 = @import("std").mem.zeroes([8][3][2]u16),
    qm: [19][3][*c]const u8 = @import("std").mem.zeroes([19][3][*c]const u8),
    a: [*c]BlockContext = null,
    a_sz: c_int = 0,
    rf: refmvs_frame = @import("std").mem.zeroes(refmvs_frame),
    jnt_weights: [7][7]u8 = @import("std").mem.zeroes([7][7]u8),
    bitdepth_max: c_int = 0,
    frame_thread: struct_unnamed_58 = @import("std").mem.zeroes(struct_unnamed_58),
    lf: struct_unnamed_59 = @import("std").mem.zeroes(struct_unnamed_59),
    task_thread: struct_unnamed_60 = @import("std").mem.zeroes(struct_unnamed_60),
    tile_thread: struct_FrameTileThreadData_47 = @import("std").mem.zeroes(struct_FrameTileThreadData_47),
};
pub extern fn antfly_codec_afz_memcmp(__s1: ?*const anyopaque, __s2: ?*const anyopaque, __n: usize) c_int;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const DAV1D_OBU_SEQ_HDR = common.DAV1D_OBU_SEQ_HDR;
pub const DAV1D_OBU_TD = common.DAV1D_OBU_TD;
pub const DAV1D_OBU_FRAME_HDR = common.DAV1D_OBU_FRAME_HDR;
pub const DAV1D_OBU_TILE_GRP = common.DAV1D_OBU_TILE_GRP;
pub const DAV1D_OBU_METADATA = common.DAV1D_OBU_METADATA;
pub const DAV1D_OBU_FRAME = common.DAV1D_OBU_FRAME;
pub const DAV1D_OBU_REDUNDANT_FRAME_HDR = common.DAV1D_OBU_REDUNDANT_FRAME_HDR;
pub const DAV1D_OBU_PADDING = common.DAV1D_OBU_PADDING;
pub const enum_Dav1dObuType = common.enum_Dav1dObuType;
pub const OBU_META_HDR_CLL = common.OBU_META_HDR_CLL;
pub const OBU_META_HDR_MDCV = common.OBU_META_HDR_MDCV;
pub const OBU_META_SCALABILITY = common.OBU_META_SCALABILITY;
pub const OBU_META_ITUT_T35 = common.OBU_META_ITUT_T35;
pub const OBU_META_TIMECODE = common.OBU_META_TIMECODE;
pub const enum_ObuMetaType = common.enum_ObuMetaType;
pub extern fn antfly_codec_afz_mutex_lock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_unlock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_cond_wait([*c]afz_pthread_cond_t, [*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_av1_dav1d_ref_create(size: usize) [*c]Dav1dRef;
pub extern fn antfly_av1_dav1d_ref_create_using_pool(pool: [*c]Dav1dMemPool, size: usize) [*c]Dav1dRef;
pub extern fn antfly_av1_dav1d_ref_dec(ref: [*c][*c]Dav1dRef) void;
pub fn dav1d_ref_init(ref: [*c]Dav1dRef, ptr: ?*const anyopaque, free_callback: ?*const fn (data: [*c]const u8, user_data: ?*anyopaque) callconv(.c) void, user_data: ?*anyopaque, free_ref: c_int) callconv(.c) [*c]Dav1dRef {
    _ = &ref;
    _ = &ptr;
    _ = &free_callback;
    _ = &user_data;
    _ = &free_ref;
    ref.*.data = null;
    ref.*.const_data = ptr;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&ref.*.ref_cnt)), 1);
    ref.*.free_ref = free_ref;
    ref.*.free_callback = free_callback;
    ref.*.user_data = user_data;
    return ref;
}
pub fn dav1d_ref_inc(ref: [*c]Dav1dRef) callconv(.c) void {
    _ = &ref;
    _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ref.*.ref_cnt)), 1);
}
pub extern fn antfly_av1_dav1d_cdf_thread_ref(dst: [*c]CdfThreadContext, src: [*c]CdfThreadContext) void;
pub extern fn antfly_av1_dav1d_cdf_thread_unref(cdf: [*c]CdfThreadContext) void;
pub extern fn antfly_av1_dav1d_data_ref(dst: [*c]Dav1dData, src: [*c]const Dav1dData) void;
pub extern fn antfly_av1_dav1d_data_props_copy(dst: [*c]Dav1dDataProps, src: [*c]const Dav1dDataProps) void;
pub extern fn antfly_av1_dav1d_data_unref_internal(buf: [*c]Dav1dData) void;
pub extern const antfly_av1_dav1d_default_wm_params: Dav1dWarpedMotionParams;
comptime {
    if (!(@as(c_ulong, 5) == @sizeOf(refmvs_temporal_block))) @compileError("static assertion failed \"refmvs_temporal_block\"");
}
comptime {
    if (!(@as(c_ulong, 2) == @sizeOf(refmvs_refpair))) @compileError("static assertion failed \"refmvs_refpair\"");
}
comptime {
    if (!(@as(c_ulong, 8) == @sizeOf(refmvs_mvpair))) @compileError("static assertion failed \"refmvs_mvpair\"");
}
comptime {
    if (!(@as(c_ulong, 12) == @sizeOf(refmvs_block))) @compileError("static assertion failed \"refmvs_block\"");
}
pub fn get_poc_diff(order_hint_n_bits: c_int, poc0: c_int, poc1: c_int) callconv(.c) c_int {
    _ = &order_hint_n_bits;
    _ = &poc0;
    _ = &poc1;
    if (!(order_hint_n_bits != 0)) return 0;
    const mask: c_int = @as(c_int, 1) << @intCast(order_hint_n_bits - @as(c_int, 1));
    _ = &mask;
    const diff: c_int = poc0 - poc1;
    _ = &diff;
    return (diff & (mask - @as(c_int, 1))) - (diff & mask);
}
pub extern fn antfly_av1_dav1d_thread_picture_ref(dst: [*c]Dav1dThreadPicture, src: [*c]const Dav1dThreadPicture) void;
pub extern fn antfly_av1_dav1d_thread_picture_unref(p: [*c]Dav1dThreadPicture) void;
pub const struct_itut_t35_ctx_context = extern struct {
    itut_t35: [*c]Dav1dITUTT35 = null,
    n_itut_t35: usize = 0,
    ref: Dav1dRef = @import("std").mem.zeroes(Dav1dRef),
};
pub extern fn antfly_av1_dav1d_picture_free_itut_t35(data: [*c]const u8, user_data: ?*anyopaque) void;
pub extern fn antfly_av1_dav1d_picture_copy_props(p: [*c]Dav1dPicture, content_light: [*c]Dav1dContentLightLevel, content_light_ref: [*c]Dav1dRef, mastering_display: [*c]Dav1dMasteringDisplay, mastering_display_ref: [*c]Dav1dRef, itut_t35: [*c]Dav1dITUTT35, itut_t35_ref: [*c]Dav1dRef, n_itut_t35: usize, props: [*c]const Dav1dDataProps) void;
pub extern fn antfly_av1_dav1d_picture_get_event_flags(p: [*c]const Dav1dThreadPicture) enum_Dav1dEventFlags;
pub extern fn antfly_av1_dav1d_submit_frame(c: [*c]Dav1dContext) c_int;
pub const struct_GetBits = extern struct {
    state: u64 = 0,
    bits_left: c_int = 0,
    @"error": c_int = 0,
    ptr: [*c]const u8 = null,
    ptr_start: [*c]const u8 = null,
    ptr_end: [*c]const u8 = null,
};
pub const GetBits = struct_GetBits;
pub extern fn antfly_av1_dav1d_init_get_bits(c: [*c]GetBits, data: [*c]const u8, sz: usize) void;
pub extern fn antfly_av1_dav1d_get_bit(c: [*c]GetBits) c_uint;
pub extern fn antfly_av1_dav1d_get_bits(c: [*c]GetBits, n: c_int) c_uint;
pub extern fn antfly_av1_dav1d_get_sbits(c: [*c]GetBits, n: c_int) c_int;
pub extern fn antfly_av1_dav1d_get_uleb128(c: [*c]GetBits) c_uint;
pub extern fn antfly_av1_dav1d_get_uniform(c: [*c]GetBits, max: c_uint) c_uint;
pub extern fn antfly_av1_dav1d_get_vlc(c: [*c]GetBits) c_uint;
pub extern fn antfly_av1_dav1d_get_bits_subexp(c: [*c]GetBits, ref: c_int, n: c_uint) c_int;
pub fn dav1d_bytealign_get_bits(arg_c: [*c]GetBits) callconv(.c) void {
    var c = arg_c;
    _ = &c;
    while (true) {
        if (!(c.*.bits_left <= @as(c_int, 7))) {
            unreachable;
        }
        if (!false) break;
    }
    c.*.bits_left = 0;
    c.*.state = 0;
}
pub fn check_trailing_bits(gb: [*c]GetBits, strict_std_compliance: c_int) callconv(.c) c_int {
    _ = &gb;
    _ = &strict_std_compliance;
    const trailing_one_bit: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bit(gb))));
    _ = &trailing_one_bit;
    if (gb.*.@"error" != 0) {
        return -@as(c_int, 22);
    }
    if (!(strict_std_compliance != 0)) return 0;
    if ((@as(u64, @bitCast(@as(c_longlong, @as(c_int, @intFromBool(!(trailing_one_bit != 0)))))) != 0) or (gb.*.state != 0)) {
        return -@as(c_int, 22);
    }
    var size: ptrdiff_t = @divExact(@as(c_long, @bitCast(@intFromPtr(gb.*.ptr_end) -% @intFromPtr(gb.*.ptr))), @sizeOf(u8));
    _ = &size;
    while ((size > @as(ptrdiff_t, 0)) and (@as(c_int, gb.*.ptr[@bitCast(@as(isize, @intCast(size - @as(ptrdiff_t, 1))))]) == @as(c_int, 0))) {
        size -= 1;
    }
    if (size != 0) {
        return -@as(c_int, 22);
    }
    return 0;
}
pub fn parse_seq_hdr(hdr: [*c]Dav1dSequenceHeader, gb: [*c]GetBits, strict_std_compliance: c_int) callconv(.c) c_int {
    _ = &hdr;
    _ = &gb;
    _ = &strict_std_compliance;
    var afz_opi_3: [*c]struct_Dav1dSequenceHeaderOperatingParameterInfo_4 = undefined;
    _ = &afz_opi_3;
    var afz_op_2: [*c]struct_Dav1dSequenceHeaderOperatingPoint_3 = undefined;
    _ = &afz_op_2;
    var afz_i_1: c_int = undefined;
    _ = &afz_i_1;
    var afz_num_ticks_per_picture_0: c_uint = undefined;
    _ = &afz_num_ticks_per_picture_0;
    var afz_pc: c_uint = 131;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                {
                    return -@as(c_int, 22);
                }
            },
            2 => {
                return check_trailing_bits(gb, strict_std_compliance);
            },
            3 => {
                hdr.*.film_grain_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 2;
                continue;
            },
            4 => {
                hdr.*.separate_uv_delta_q = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, hdr.*.monochrome) != 0)) @as(c_int, 4) else @as(c_int, 3)));
                continue;
            },
            6 => {
                afz_pc = 1;
                continue;
            },
            7 => {
                afz_pc = @bitCast(@as(c_int, if (((strict_std_compliance != 0) and (hdr.*.mtrx == @as(enum_Dav1dMatrixCoefficients, DAV1D_MC_IDENTITY))) and (hdr.*.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))) @as(c_int, 6) else @as(c_int, 5)));
                continue;
            },
            8 => {
                hdr.*.chr = DAV1D_CHR_UNKNOWN;
                afz_pc = 7;
                continue;
            },
            9 => {
                hdr.*.ss_hor = blk: {
                    const tmp = @as(u8, 1);
                    hdr.*.ss_ver = tmp;
                    break :blk tmp;
                };
                afz_pc = 8;
                continue;
            },
            10 => {
                hdr.*.layout = DAV1D_PIXEL_LAYOUT_I400;
                afz_pc = 9;
                continue;
            },
            11 => {
                hdr.*.color_range = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_pc = 1;
                continue;
            },
            13 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, hdr.*.profile) != @as(c_int, 1)) and !((@as(c_int, hdr.*.profile) == @as(c_int, 2)) and (@as(c_int, hdr.*.hbd) == @as(c_int, 2)))) @as(c_int, 12) else @as(c_int, 7)));
                continue;
            },
            14 => {
                hdr.*.color_range = 1;
                afz_pc = 13;
                continue;
            },
            15 => {
                hdr.*.layout = DAV1D_PIXEL_LAYOUT_I444;
                afz_pc = 14;
                continue;
            },
            16 => {
                hdr.*.chr = if ((@as(c_int, hdr.*.ss_hor) & @as(c_int, hdr.*.ss_ver)) != 0) antfly_av1_dav1d_get_bits(gb, 2) else @as(c_uint, DAV1D_CHR_UNKNOWN);
                afz_pc = 7;
                continue;
            },
            17 => {
                afz_pc = 16;
                continue;
            },
            18 => {
                hdr.*.layout = @bitCast(if (hdr.*.ss_hor != 0) if (hdr.*.ss_ver != 0) DAV1D_PIXEL_LAYOUT_I420 else DAV1D_PIXEL_LAYOUT_I422 else DAV1D_PIXEL_LAYOUT_I444);
                afz_pc = 17;
                continue;
            },
            19 => {
                hdr.*.ss_ver = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.ss_hor != 0) @as(c_int, 19) else @as(c_int, 18)));
                continue;
            },
            21 => {
                hdr.*.ss_hor = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 20;
                continue;
            },
            22 => {
                hdr.*.ss_hor = 1;
                afz_pc = 18;
                continue;
            },
            23 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, hdr.*.hbd) == @as(c_int, 2)) @as(c_int, 21) else @as(c_int, 22)));
                continue;
            },
            24 => {
                afz_pc = 16;
                continue;
            },
            25 => {
                hdr.*.layout = DAV1D_PIXEL_LAYOUT_I444;
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_pc = 16;
                continue;
            },
            27 => {
                hdr.*.ss_hor = blk: {
                    const tmp = @as(u8, 1);
                    hdr.*.ss_ver = tmp;
                    break :blk tmp;
                };
                afz_pc = 26;
                continue;
            },
            28 => {
                hdr.*.layout = DAV1D_PIXEL_LAYOUT_I420;
                afz_pc = 27;
                continue;
            },
            29 => {
                blk: {
                    switch (@as(c_int, hdr.*.profile)) {
                        @as(c_int, 2) => {
                            afz_pc = 23;
                            continue;
                        },
                        @as(c_int, 1) => {
                            afz_pc = 25;
                            continue;
                        },
                        @as(c_int, 0) => {
                            afz_pc = 28;
                            continue;
                        },
                        else => {
                            afz_pc = 16;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            30 => {
                hdr.*.color_range = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_pc = @bitCast(@as(c_int, if (((hdr.*.pri == @as(enum_Dav1dColorPrimaries, DAV1D_COLOR_PRI_BT709)) and (hdr.*.trc == @as(enum_Dav1dTransferCharacteristics, DAV1D_TRC_SRGB))) and (hdr.*.mtrx == @as(enum_Dav1dMatrixCoefficients, DAV1D_MC_IDENTITY))) @as(c_int, 15) else @as(c_int, 30)));
                continue;
            },
            32 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.monochrome != 0) @as(c_int, 11) else @as(c_int, 31)));
                continue;
            },
            33 => {
                hdr.*.mtrx = antfly_av1_dav1d_get_bits(gb, 8);
                afz_pc = 32;
                continue;
            },
            34 => {
                hdr.*.trc = antfly_av1_dav1d_get_bits(gb, 8);
                afz_pc = 33;
                continue;
            },
            35 => {
                hdr.*.pri = antfly_av1_dav1d_get_bits(gb, 8);
                afz_pc = 34;
                continue;
            },
            36 => {
                hdr.*.mtrx = DAV1D_MC_UNKNOWN;
                afz_pc = 32;
                continue;
            },
            37 => {
                hdr.*.trc = DAV1D_TRC_UNKNOWN;
                afz_pc = 36;
                continue;
            },
            38 => {
                hdr.*.pri = DAV1D_COLOR_PRI_UNKNOWN;
                afz_pc = 37;
                continue;
            },
            39 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.color_description_present != 0) @as(c_int, 35) else @as(c_int, 38)));
                continue;
            },
            40 => {
                hdr.*.color_description_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 39;
                continue;
            },
            41 => {
                hdr.*.monochrome = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 40;
                continue;
            },
            42 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, hdr.*.profile) != @as(c_int, 1)) @as(c_int, 41) else @as(c_int, 40)));
                continue;
            },
            43 => {
                {
                    const ref = &hdr.*.hbd;
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_get_bit(gb));
                }
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, hdr.*.profile) == @as(c_int, 2)) and (@as(c_int, hdr.*.hbd) != 0)) @as(c_int, 43) else @as(c_int, 42)));
                continue;
            },
            45 => {
                hdr.*.hbd = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 44;
                continue;
            },
            46 => {
                hdr.*.restoration = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 45;
                continue;
            },
            47 => {
                hdr.*.cdef = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 46;
                continue;
            },
            48 => {
                hdr.*.super_res = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 47;
                continue;
            },
            49 => {
                hdr.*.force_integer_mv = DAV1D_ADAPTIVE;
                afz_pc = 48;
                continue;
            },
            50 => {
                hdr.*.screen_content_tools = DAV1D_ADAPTIVE;
                afz_pc = 49;
                continue;
            },
            51 => {
                hdr.*.order_hint_n_bits = @truncate(antfly_av1_dav1d_get_bits(gb, 3) +% @as(c_uint, 1));
                afz_pc = 48;
                continue;
            },
            52 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.order_hint != 0) @as(c_int, 51) else @as(c_int, 48)));
                continue;
            },
            53 => {
                hdr.*.force_integer_mv = if (hdr.*.screen_content_tools != 0) if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_uint, DAV1D_ADAPTIVE) else antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 2);
                afz_pc = 52;
                continue;
            },
            54 => {
                hdr.*.screen_content_tools = if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_uint, DAV1D_ADAPTIVE) else antfly_av1_dav1d_get_bit(gb);
                afz_pc = 53;
                continue;
            },
            55 => {
                hdr.*.ref_frame_mvs = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 54;
                continue;
            },
            56 => {
                hdr.*.jnt_comp = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.order_hint != 0) @as(c_int, 56) else @as(c_int, 54)));
                continue;
            },
            58 => {
                hdr.*.order_hint = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 57;
                continue;
            },
            59 => {
                hdr.*.dual_filter = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 58;
                continue;
            },
            60 => {
                hdr.*.warped_motion = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 59;
                continue;
            },
            61 => {
                hdr.*.masked_compound = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 60;
                continue;
            },
            62 => {
                hdr.*.inter_intra = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 61;
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.reduced_still_picture_header != 0) @as(c_int, 50) else @as(c_int, 62)));
                continue;
            },
            64 => {
                hdr.*.intra_edge_filter = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 63;
                continue;
            },
            65 => {
                hdr.*.filter_intra = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 64;
                continue;
            },
            66 => {
                hdr.*.sb128 = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 65;
                continue;
            },
            67 => {
                hdr.*.frame_id_n_bits = @truncate((antfly_av1_dav1d_get_bits(gb, 3) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, hdr.*.delta_frame_id_n_bits))))) +% @as(c_uint, 1));
                afz_pc = 66;
                continue;
            },
            68 => {
                hdr.*.delta_frame_id_n_bits = @truncate(antfly_av1_dav1d_get_bits(gb, 4) +% @as(c_uint, 2));
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.frame_id_numbers_present != 0) @as(c_int, 68) else @as(c_int, 66)));
                continue;
            },
            70 => {
                hdr.*.frame_id_numbers_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 69;
                continue;
            },
            71 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, hdr.*.reduced_still_picture_header) != 0)) @as(c_int, 70) else @as(c_int, 66)));
                continue;
            },
            72 => {
                hdr.*.max_height = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, hdr.*.height_n_bits) +% @as(c_uint, 1))));
                afz_pc = 71;
                continue;
            },
            73 => {
                hdr.*.max_width = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, hdr.*.width_n_bits) +% @as(c_uint, 1))));
                afz_pc = 72;
                continue;
            },
            74 => {
                hdr.*.height_n_bits = @truncate(antfly_av1_dav1d_get_bits(gb, 4) +% @as(c_uint, 1));
                afz_pc = 73;
                continue;
            },
            75 => {
                hdr.*.width_n_bits = @truncate(antfly_av1_dav1d_get_bits(gb, 4) +% @as(c_uint, 1));
                afz_pc = 74;
                continue;
            },
            76 => {
                hdr.*.operating_points[@as(c_int, 0)].initial_display_delay = 10;
                afz_pc = 75;
                continue;
            },
            77 => {
                hdr.*.operating_points[@as(c_int, 0)].minor_level = @truncate(antfly_av1_dav1d_get_bits(gb, 2));
                afz_pc = 76;
                continue;
            },
            78 => {
                hdr.*.operating_points[@as(c_int, 0)].major_level = @truncate(antfly_av1_dav1d_get_bits(gb, 3));
                afz_pc = 77;
                continue;
            },
            79 => {
                hdr.*.num_operating_points = 1;
                afz_pc = 78;
                continue;
            },
            80 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_1 < @as(c_int, hdr.*.num_operating_points)) @as(c_int, 99) else @as(c_int, 75)));
                continue;
            },
            81 => {
                afz_i_1 += 1;
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_op_2.*.initial_display_delay = @truncate(if (afz_op_2.*.display_model_param_present != 0) antfly_av1_dav1d_get_bits(gb, 4) +% @as(c_uint, 1) else @as(c_uint, 10));
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_op_2.*.display_model_param_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.display_model_info_present != 0) @as(c_int, 83) else @as(c_int, 82)));
                continue;
            },
            85 => {
                afz_opi_3.*.low_delay_mode = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_opi_3.*.encoder_buffer_delay = antfly_av1_dav1d_get_bits(gb, hdr.*.encoder_decoder_buffer_delay_length);
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_opi_3.*.decoder_buffer_delay = antfly_av1_dav1d_get_bits(gb, hdr.*.encoder_decoder_buffer_delay_length);
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_opi_3 = &hdr.*.operating_parameter_info[@bitCast(@as(isize, @intCast(afz_i_1)))];
                afz_pc = 87;
                continue;
            },
            89 => {
                afz_pc = @bitCast(@as(c_int, if (afz_op_2.*.decoder_model_param_present != 0) @as(c_int, 88) else @as(c_int, 84)));
                continue;
            },
            90 => {
                afz_op_2.*.decoder_model_param_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.decoder_model_info_present != 0) @as(c_int, 90) else @as(c_int, 84)));
                continue;
            },
            92 => {
                afz_op_2.*.tier = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_op_2.*.major_level) > @as(c_int, 3)) @as(c_int, 92) else @as(c_int, 91)));
                continue;
            },
            94 => {
                afz_op_2.*.minor_level = @truncate(antfly_av1_dav1d_get_bits(gb, 2));
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_op_2.*.major_level = @truncate(@as(c_uint, 2) +% antfly_av1_dav1d_get_bits(gb, 3));
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_pc = 1;
                continue;
            },
            97 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_op_2.*.idc) != 0) and (!((@as(c_int, afz_op_2.*.idc) & @as(c_int, 255)) != 0) or !((@as(c_int, afz_op_2.*.idc) & @as(c_int, 3840)) != 0))) @as(c_int, 96) else @as(c_int, 95)));
                continue;
            },
            98 => {
                afz_op_2.*.idc = @truncate(antfly_av1_dav1d_get_bits(gb, 12));
                afz_pc = 97;
                continue;
            },
            99 => {
                afz_op_2 = &hdr.*.operating_points[@bitCast(@as(isize, @intCast(afz_i_1)))];
                afz_pc = 98;
                continue;
            },
            100 => {
                afz_i_1 = 0;
                afz_pc = 80;
                continue;
            },
            101 => {
                hdr.*.num_operating_points = @truncate(antfly_av1_dav1d_get_bits(gb, 5) +% @as(c_uint, 1));
                afz_pc = 100;
                continue;
            },
            102 => {
                hdr.*.display_model_info_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 101;
                continue;
            },
            103 => {
                hdr.*.frame_presentation_delay_length = @truncate(antfly_av1_dav1d_get_bits(gb, 5) +% @as(c_uint, 1));
                afz_pc = 102;
                continue;
            },
            104 => {
                hdr.*.buffer_removal_delay_length = @truncate(antfly_av1_dav1d_get_bits(gb, 5) +% @as(c_uint, 1));
                afz_pc = 103;
                continue;
            },
            105 => {
                afz_pc = 1;
                continue;
            },
            106 => {
                afz_pc = @bitCast(@as(c_int, if ((strict_std_compliance != 0) and !(hdr.*.num_units_in_decoding_tick != 0)) @as(c_int, 105) else @as(c_int, 104)));
                continue;
            },
            107 => {
                hdr.*.num_units_in_decoding_tick = antfly_av1_dav1d_get_bits(gb, 32);
                afz_pc = 106;
                continue;
            },
            108 => {
                hdr.*.encoder_decoder_buffer_delay_length = @truncate(antfly_av1_dav1d_get_bits(gb, 5) +% @as(c_uint, 1));
                afz_pc = 107;
                continue;
            },
            109 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.decoder_model_info_present != 0) @as(c_int, 108) else @as(c_int, 102)));
                continue;
            },
            110 => {
                hdr.*.decoder_model_info_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 109;
                continue;
            },
            111 => {
                hdr.*.num_ticks_per_picture = afz_num_ticks_per_picture_0 +% @as(c_uint, 1);
                afz_pc = 110;
                continue;
            },
            112 => {
                afz_pc = 1;
                continue;
            },
            113 => {
                afz_pc = @bitCast(@as(c_int, if (afz_num_ticks_per_picture_0 == @as(c_uint, 4294967295)) @as(c_int, 112) else @as(c_int, 111)));
                continue;
            },
            114 => {
                afz_num_ticks_per_picture_0 = antfly_av1_dav1d_get_vlc(gb);
                afz_pc = 113;
                continue;
            },
            115 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.equal_picture_interval != 0) @as(c_int, 114) else @as(c_int, 110)));
                continue;
            },
            116 => {
                hdr.*.equal_picture_interval = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 115;
                continue;
            },
            117 => {
                afz_pc = 1;
                continue;
            },
            118 => {
                afz_pc = @bitCast(@as(c_int, if ((strict_std_compliance != 0) and (!(hdr.*.num_units_in_tick != 0) or !(hdr.*.time_scale != 0))) @as(c_int, 117) else @as(c_int, 116)));
                continue;
            },
            119 => {
                hdr.*.time_scale = antfly_av1_dav1d_get_bits(gb, 32);
                afz_pc = 118;
                continue;
            },
            120 => {
                hdr.*.num_units_in_tick = antfly_av1_dav1d_get_bits(gb, 32);
                afz_pc = 119;
                continue;
            },
            121 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.timing_info_present != 0) @as(c_int, 120) else @as(c_int, 102)));
                continue;
            },
            122 => {
                hdr.*.timing_info_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 121;
                continue;
            },
            123 => {
                afz_pc = @bitCast(@as(c_int, if (hdr.*.reduced_still_picture_header != 0) @as(c_int, 79) else @as(c_int, 122)));
                continue;
            },
            124 => {
                afz_pc = 1;
                continue;
            },
            125 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, hdr.*.reduced_still_picture_header) != 0) and !(@as(c_int, hdr.*.still_picture) != 0)) @as(c_int, 124) else @as(c_int, 123)));
                continue;
            },
            126 => {
                hdr.*.reduced_still_picture_header = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 125;
                continue;
            },
            127 => {
                hdr.*.still_picture = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 126;
                continue;
            },
            128 => {
                afz_pc = 1;
                continue;
            },
            129 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, hdr.*.profile) > @as(c_int, 2)) @as(c_int, 128) else @as(c_int, 127)));
                continue;
            },
            130 => {
                hdr.*.profile = @truncate(antfly_av1_dav1d_get_bits(gb, 3));
                afz_pc = 129;
                continue;
            },
            131 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(hdr)), 0, @sizeOf(@TypeOf(hdr.*)));
                afz_pc = 130;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub fn read_frame_size(c: [*c]Dav1dContext, gb: [*c]GetBits, use_ref: c_int) callconv(.c) c_int {
    _ = &c;
    _ = &gb;
    _ = &use_ref;
    const seqhdr: [*c]const Dav1dSequenceHeader = c.*.seq_hdr;
    _ = &seqhdr;
    const hdr: [*c]Dav1dFrameHeader = c.*.frame_hdr;
    _ = &hdr;
    if (use_ref != 0) {
        {
            var i: c_int = 0;
            _ = &i;
            while (i < @as(c_int, 7)) : (i += 1) {
                if (antfly_av1_dav1d_get_bit(gb) != 0) {
                    const ref: [*c]const Dav1dThreadPicture = &c.*.refs[@bitCast(@as(isize, @intCast(c.*.frame_hdr.*.refidx[@bitCast(@as(isize, @intCast(i)))])))].p;
                    _ = &ref;
                    if (!(ref.*.p.frame_hdr != null)) return -@as(c_int, 1);
                    hdr.*.width[@as(c_int, 1)] = ref.*.p.frame_hdr.*.width[@as(c_int, 1)];
                    hdr.*.height = ref.*.p.frame_hdr.*.height;
                    hdr.*.render_width = ref.*.p.frame_hdr.*.render_width;
                    hdr.*.render_height = ref.*.p.frame_hdr.*.render_height;
                    hdr.*.super_res.enabled = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @as(c_int, seqhdr.*.super_res)))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                    if (@as(c_int, hdr.*.super_res.enabled) != 0) {
                        const d: c_int = blk: {
                            const tmp = @as(u8, @truncate(@as(c_uint, 9) +% antfly_av1_dav1d_get_bits(gb, 3)));
                            hdr.*.super_res.width_scale_denominator = tmp;
                            break :blk tmp;
                        };
                        _ = &d;
                        hdr.*.width[@as(c_int, 0)] = imax(@divTrunc((hdr.*.width[@as(c_int, 1)] * @as(c_int, 8)) + (d >> @intCast(@as(c_int, 1))), d), imin(16, hdr.*.width[@as(c_int, 1)]));
                    } else {
                        hdr.*.super_res.width_scale_denominator = 8;
                        hdr.*.width[@as(c_int, 0)] = hdr.*.width[@as(c_int, 1)];
                    }
                    return 0;
                }
            }
        }
    }
    if (@as(c_int, hdr.*.frame_size_override) != 0) {
        hdr.*.width[@as(c_int, 1)] = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, seqhdr.*.width_n_bits) +% @as(c_uint, 1))));
        hdr.*.height = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, seqhdr.*.height_n_bits) +% @as(c_uint, 1))));
    } else {
        hdr.*.width[@as(c_int, 1)] = seqhdr.*.max_width;
        hdr.*.height = seqhdr.*.max_height;
    }
    hdr.*.super_res.enabled = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @as(c_int, seqhdr.*.super_res)))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
    if (@as(c_int, hdr.*.super_res.enabled) != 0) {
        const d: c_int = blk: {
            const tmp = @as(u8, @truncate(@as(c_uint, 9) +% antfly_av1_dav1d_get_bits(gb, 3)));
            hdr.*.super_res.width_scale_denominator = tmp;
            break :blk tmp;
        };
        _ = &d;
        hdr.*.width[@as(c_int, 0)] = imax(@divTrunc((hdr.*.width[@as(c_int, 1)] * @as(c_int, 8)) + (d >> @intCast(@as(c_int, 1))), d), imin(16, hdr.*.width[@as(c_int, 1)]));
    } else {
        hdr.*.super_res.width_scale_denominator = 8;
        hdr.*.width[@as(c_int, 0)] = hdr.*.width[@as(c_int, 1)];
    }
    hdr.*.have_render_size = @truncate(antfly_av1_dav1d_get_bit(gb));
    if (@as(c_int, hdr.*.have_render_size) != 0) {
        hdr.*.render_width = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 16) +% @as(c_uint, 1))));
        hdr.*.render_height = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 16) +% @as(c_uint, 1))));
    } else {
        hdr.*.render_width = hdr.*.width[@as(c_int, 1)];
        hdr.*.render_height = hdr.*.height;
    }
    return 0;
}
pub fn tile_log2(sz: c_int, tgt: c_int) callconv(.c) c_int {
    _ = &sz;
    _ = &tgt;
    var k: c_int = undefined;
    _ = &k;
    {
        k = 0;
        while ((sz << @intCast(k)) < tgt) : (k += 1) {}
    }
    return k;
}
pub const default_mode_ref_deltas: Dav1dLoopfilterModeRefDeltas = Dav1dLoopfilterModeRefDeltas{
    .mode_delta = [2]i8{
        0,
        0,
    },
    .ref_delta = [8]i8{
        1,
        0,
        0,
        0,
        @truncate(-@as(c_int, 1)),
        0,
        @truncate(-@as(c_int, 1)),
        @truncate(-@as(c_int, 1)),
    },
};
pub fn parse_frame_hdr(c: [*c]Dav1dContext, gb: [*c]GetBits) callconv(.c) c_int {
    _ = &c;
    _ = &gb;
    var afz_i_87: c_int = undefined;
    _ = &afz_i_87;
    var afz_refidx_86: c_int = undefined;
    _ = &afz_refidx_86;
    var afz_pl_97: c_int = undefined;
    _ = &afz_pl_97;
    var afz_i_96: c_int = undefined;
    _ = &afz_i_96;
    var afz_num_uv_pos_95: c_int = undefined;
    _ = &afz_num_uv_pos_95;
    var afz_pl_94: c_int = undefined;
    _ = &afz_pl_94;
    var afz_i_93: c_int = undefined;
    _ = &afz_i_93;
    var afz_num_y_pos_92: c_int = undefined;
    _ = &afz_num_y_pos_92;
    var afz_i_91: c_int = undefined;
    _ = &afz_i_91;
    var afz_pl_90: c_int = undefined;
    _ = &afz_pl_90;
    var afz_i_89: c_int = undefined;
    _ = &afz_i_89;
    var afz_fgd_88: [*c]Dav1dFilmGrainData = undefined;
    _ = &afz_fgd_88;
    var afz_seed_85: c_uint = undefined;
    _ = &afz_seed_85;
    var afz_bits_83: c_int = undefined;
    _ = &afz_bits_83;
    var afz_shift_84: c_int = undefined;
    _ = &afz_shift_84;
    var afz_ref_mat_82: [*c]i32 = undefined;
    _ = &afz_ref_mat_82;
    var afz_mat_81: [*c]i32 = undefined;
    _ = &afz_mat_81;
    var afz_pri_ref_80: c_int = undefined;
    _ = &afz_pri_ref_80;
    var afz_ref_gmv_79: [*c]Dav1dWarpedMotionParams = undefined;
    _ = &afz_ref_gmv_79;
    var afz_i_78: c_int = undefined;
    _ = &afz_i_78;
    var afz_i_77: c_int = undefined;
    _ = &afz_i_77;
    var afz_refpoc_76: c_uint = undefined;
    _ = &afz_refpoc_76;
    var afz_i_75: c_int = undefined;
    _ = &afz_i_75;
    var afz_off_before2_idx_74: c_int = undefined;
    _ = &afz_off_before2_idx_74;
    var afz_off_before2_73: c_uint = undefined;
    _ = &afz_off_before2_73;
    var afz_diff_72: c_int = undefined;
    _ = &afz_diff_72;
    var afz_refpoc_71: c_uint = undefined;
    _ = &afz_refpoc_71;
    var afz_i_70: c_int = undefined;
    _ = &afz_i_70;
    var afz_off_before_idx_68: c_int = undefined;
    _ = &afz_off_before_idx_68;
    var afz_off_after_idx_69: c_int = undefined;
    _ = &afz_off_after_idx_69;
    var afz_off_after_67: c_int = undefined;
    _ = &afz_off_after_67;
    var afz_off_before_66: c_uint = undefined;
    _ = &afz_off_before_66;
    var afz_poc_65: c_uint = undefined;
    _ = &afz_poc_65;
    var afz_i_64: c_int = undefined;
    _ = &afz_i_64;
    var afz_i_63: c_int = undefined;
    _ = &afz_i_63;
    var afz_i_62: c_int = undefined;
    _ = &afz_i_62;
    var afz_ref_61: c_int = undefined;
    _ = &afz_ref_61;
    var afz_i_60: c_int = undefined;
    _ = &afz_i_60;
    var afz_delta_lossless_59: c_int = undefined;
    _ = &afz_delta_lossless_59;
    var afz_seg_56: [*c]Dav1dSegmentationData = undefined;
    _ = &afz_seg_56;
    var afz_i_55: c_int = undefined;
    _ = &afz_i_55;
    var afz_pri_ref_57: c_int = undefined;
    _ = &afz_pri_ref_57;
    var afz_i_58: c_int = undefined;
    _ = &afz_i_58;
    var afz_diff_uv_delta_54: c_int = undefined;
    _ = &afz_diff_uv_delta_54;
    var afz_sby_44: c_int = undefined;
    _ = &afz_sby_44;
    var afz_tile_h_43: c_int = undefined;
    _ = &afz_tile_h_43;
    var afz_sbx_42: c_int = undefined;
    _ = &afz_sbx_42;
    var afz_tile_w_41: c_int = undefined;
    _ = &afz_tile_w_41;
    var afz_tile_h_53: c_int = undefined;
    _ = &afz_tile_h_53;
    var afz_tile_height_sb_52: c_int = undefined;
    _ = &afz_tile_height_sb_52;
    var afz_sby_51: c_int = undefined;
    _ = &afz_sby_51;
    var afz_max_tile_height_sb_50: c_int = undefined;
    _ = &afz_max_tile_height_sb_50;
    var afz_tile_w_49: c_int = undefined;
    _ = &afz_tile_w_49;
    var afz_tile_width_sb_48: c_int = undefined;
    _ = &afz_tile_width_sb_48;
    var afz_sbx_47: c_int = undefined;
    _ = &afz_sbx_47;
    var afz_widest_tile_45: c_int = undefined;
    _ = &afz_widest_tile_45;
    var afz_max_tile_area_sb_46: c_int = undefined;
    _ = &afz_max_tile_area_sb_46;
    var afz_min_log2_tiles_40: c_int = undefined;
    _ = &afz_min_log2_tiles_40;
    var afz_max_tile_area_sb_39: c_int = undefined;
    _ = &afz_max_tile_area_sb_39;
    var afz_max_tile_width_sb_38: c_int = undefined;
    _ = &afz_max_tile_width_sb_38;
    var afz_sbh_37: c_int = undefined;
    _ = &afz_sbh_37;
    var afz_sbw_36: c_int = undefined;
    _ = &afz_sbw_36;
    var afz_sbsz_log2_35: c_int = undefined;
    _ = &afz_sbsz_log2_35;
    var afz_sbsz_min1_34: c_int = undefined;
    _ = &afz_sbsz_min1_34;
    var afz_i_8: c_int = undefined;
    _ = &afz_i_8;
    var afz_use_ref_33: c_int = undefined;
    _ = &afz_use_ref_33;
    var afz_ref_frame_hdr_32: [*c]Dav1dFrameHeader = undefined;
    _ = &afz_ref_frame_hdr_32;
    var afz_ref_frame_id_31: c_uint = undefined;
    _ = &afz_ref_frame_id_31;
    var afz_delta_ref_frame_id_30: c_uint = undefined;
    _ = &afz_delta_ref_frame_id_30;
    var afz_i_29: c_int = undefined;
    _ = &afz_i_29;
    var afz_i_28: c_int = undefined;
    _ = &afz_i_28;
    var afz_hint_27: c_int = undefined;
    _ = &afz_hint_27;
    var afz_i_26: c_int = undefined;
    _ = &afz_i_26;
    var afz_ref_25: c_int = undefined;
    _ = &afz_ref_25;
    var afz_hint_24: c_int = undefined;
    _ = &afz_hint_24;
    var afz_j_23: c_int = undefined;
    _ = &afz_j_23;
    var afz_i_22: c_int = undefined;
    _ = &afz_i_22;
    var afz_hint_21: c_int = undefined;
    _ = &afz_hint_21;
    var afz_i_20: c_int = undefined;
    _ = &afz_i_20;
    var afz_hint_19: c_int = undefined;
    _ = &afz_hint_19;
    var afz_i_18: c_int = undefined;
    _ = &afz_i_18;
    var afz_earliest_frame_offset_17: c_int = undefined;
    _ = &afz_earliest_frame_offset_17;
    var afz_hint_16: c_int = undefined;
    _ = &afz_hint_16;
    var afz_i_15: c_int = undefined;
    _ = &afz_i_15;
    var afz_latest_frame_offset_14: c_int = undefined;
    _ = &afz_latest_frame_offset_14;
    var afz_used_frame_13: [8]c_int = undefined;
    _ = &afz_used_frame_13;
    var afz_i_12: c_int = undefined;
    _ = &afz_i_12;
    var afz_current_frame_offset_11: c_int = undefined;
    _ = &afz_current_frame_offset_11;
    var afz_shifted_frame_offset_10: [8]c_int = undefined;
    _ = &afz_shifted_frame_offset_10;
    var afz_i_9: c_int = undefined;
    _ = &afz_i_9;
    var afz_in_spatial_layer_7: c_int = undefined;
    _ = &afz_in_spatial_layer_7;
    var afz_in_temporal_layer_6: c_int = undefined;
    _ = &afz_in_temporal_layer_6;
    var afz_op_5: [*c]struct_Dav1dFrameHeaderOperatingPoint_6 = undefined;
    _ = &afz_op_5;
    var afz_seqop_4: [*c]struct_Dav1dSequenceHeaderOperatingPoint_3 = undefined;
    _ = &afz_seqop_4;
    var afz_i_3: c_int = undefined;
    _ = &afz_i_3;
    var afz_ref_frame_hdr_2: [*c]Dav1dFrameHeader = undefined;
    _ = &afz_ref_frame_hdr_2;
    var afz_hdr_1: [*c]Dav1dFrameHeader = undefined;
    _ = &afz_hdr_1;
    var afz_seqhdr_0: [*c]Dav1dSequenceHeader = undefined;
    _ = &afz_seqhdr_0;
    var afz_pc: c_uint = 536;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                {
                    return -@as(c_int, 22);
                }
            },
            2 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 2) else @as(c_int, 1);
                continue;
            },
            3 => {
                return 0;
            },
            4 => {
                afz_hdr_1.*.film_grain.data.seed = afz_seed_85;
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_hdr_1.*.film_grain.data = c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_86)))].p.p.frame_hdr.*.film_grain.data;
                afz_pc = 4;
                continue;
            },
            6 => {
                afz_pc = 2;
                continue;
            },
            7 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_i_87 == @as(c_int, 7)) or !(c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_86)))].p.p.frame_hdr != null)) @as(c_int, 6) else @as(c_int, 5)));
                continue;
            },
            8 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_87 < @as(c_int, 7)) @as(c_int, 11) else @as(c_int, 7)));
                continue;
            },
            9 => {
                afz_i_87 += 1;
                afz_pc = 8;
                continue;
            },
            10 => {
                afz_pc = 7;
                continue;
            },
            11 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_87)))]) == afz_refidx_86) @as(c_int, 10) else @as(c_int, 9)));
                continue;
            },
            12 => {
                afz_i_87 = 0;
                afz_pc = 8;
                continue;
            },
            13 => {
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_refidx_86 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 3))));
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_fgd_88.*.clip_to_restricted_range = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bit(gb))));
                afz_pc = 3;
                continue;
            },
            16 => {
                afz_fgd_88.*.overlap_flag = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bit(gb))));
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_97 < @as(c_int, 2)) @as(c_int, 22) else @as(c_int, 16)));
                continue;
            },
            18 => {
                afz_pl_97 += 1;
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_fgd_88.*.uv_offset[@bitCast(@as(isize, @intCast(afz_pl_97)))] = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 9) -% @as(c_uint, 256))));
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_fgd_88.*.uv_luma_mult[@bitCast(@as(isize, @intCast(afz_pl_97)))] = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 8) -% @as(c_uint, 128))));
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_fgd_88.*.uv_mult[@bitCast(@as(isize, @intCast(afz_pl_97)))] = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 8) -% @as(c_uint, 128))));
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_pc = @bitCast(@as(c_int, if (afz_fgd_88.*.num_uv_points[@bitCast(@as(isize, @intCast(afz_pl_97)))] != 0) @as(c_int, 21) else @as(c_int, 18)));
                continue;
            },
            23 => {
                afz_pl_97 = 0;
                afz_pc = 17;
                continue;
            },
            24 => {
                afz_fgd_88.*.grain_scale_shift = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 2))));
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_fgd_88.*.ar_coeff_shift = antfly_av1_dav1d_get_bits(gb, 2) +% @as(c_uint, 6);
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_94 < @as(c_int, 2)) @as(c_int, 35) else @as(c_int, 25)));
                continue;
            },
            27 => {
                afz_pl_94 += 1;
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_fgd_88.*.ar_coeffs_uv[@bitCast(@as(isize, @intCast(afz_pl_94)))][@bitCast(@as(isize, @intCast(afz_num_uv_pos_95)))] = 0;
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_fgd_88.*.num_y_points != 0)) @as(c_int, 28) else @as(c_int, 27)));
                continue;
            },
            30 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_96 < afz_num_uv_pos_95) @as(c_int, 32) else @as(c_int, 29)));
                continue;
            },
            31 => {
                afz_i_96 += 1;
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_fgd_88.*.ar_coeffs_uv[@bitCast(@as(isize, @intCast(afz_pl_94)))][@bitCast(@as(isize, @intCast(afz_i_96)))] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_get_bits(gb, 8) -% @as(c_uint, 128))));
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_i_96 = 0;
                afz_pc = 30;
                continue;
            },
            34 => {
                afz_num_uv_pos_95 = afz_num_y_pos_92 + @as(c_int, @intFromBool(!!(afz_fgd_88.*.num_y_points != 0)));
                afz_pc = 33;
                continue;
            },
            35 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_fgd_88.*.num_uv_points[@bitCast(@as(isize, @intCast(afz_pl_94)))] != 0) or (afz_fgd_88.*.chroma_scaling_from_luma != 0)) @as(c_int, 34) else @as(c_int, 27)));
                continue;
            },
            36 => {
                afz_pl_94 = 0;
                afz_pc = 26;
                continue;
            },
            37 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_93 < afz_num_y_pos_92) @as(c_int, 39) else @as(c_int, 36)));
                continue;
            },
            38 => {
                afz_i_93 += 1;
                afz_pc = 37;
                continue;
            },
            39 => {
                afz_fgd_88.*.ar_coeffs_y[@bitCast(@as(isize, @intCast(afz_i_93)))] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_get_bits(gb, 8) -% @as(c_uint, 128))));
                afz_pc = 38;
                continue;
            },
            40 => {
                afz_i_93 = 0;
                afz_pc = 37;
                continue;
            },
            41 => {
                afz_pc = @bitCast(@as(c_int, if (afz_fgd_88.*.num_y_points != 0) @as(c_int, 40) else @as(c_int, 36)));
                continue;
            },
            42 => {
                afz_num_y_pos_92 = (@as(c_int, 2) * afz_fgd_88.*.ar_coeff_lag) * (afz_fgd_88.*.ar_coeff_lag + @as(c_int, 1));
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_fgd_88.*.ar_coeff_lag = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 2))));
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_fgd_88.*.scaling_shift = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 2) +% @as(c_uint, 8))));
                afz_pc = 43;
                continue;
            },
            45 => {
                afz_pc = 2;
                continue;
            },
            46 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(c_int, afz_seqhdr_0.*.ss_hor) == @as(c_int, 1)) and (@as(c_int, afz_seqhdr_0.*.ss_ver) == @as(c_int, 1))) and (@as(c_int, @intFromBool(!!(afz_fgd_88.*.num_uv_points[@as(c_int, 0)] != 0))) != @as(c_int, @intFromBool(!!(afz_fgd_88.*.num_uv_points[@as(c_int, 1)] != 0))))) @as(c_int, 45) else @as(c_int, 44)));
                continue;
            },
            47 => {
                afz_fgd_88.*.num_uv_points[@as(c_int, 0)] = blk: {
                    const tmp = @as(c_int, 0);
                    afz_fgd_88.*.num_uv_points[@as(c_int, 1)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 46;
                continue;
            },
            48 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_90 < @as(c_int, 2)) @as(c_int, 59) else @as(c_int, 46)));
                continue;
            },
            49 => {
                afz_pl_90 += 1;
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_91 < afz_fgd_88.*.num_uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))]) @as(c_int, 55) else @as(c_int, 49)));
                continue;
            },
            51 => {
                afz_i_91 += 1;
                afz_pc = 50;
                continue;
            },
            52 => {
                afz_fgd_88.*.uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))][@bitCast(@as(isize, @intCast(afz_i_91)))][@as(c_int, 1)] = @truncate(antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 51;
                continue;
            },
            53 => {
                afz_pc = 2;
                continue;
            },
            54 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_i_91 != 0) and (@as(c_int, afz_fgd_88.*.uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))][@bitCast(@as(isize, @intCast(afz_i_91 - @as(c_int, 1))))][@as(c_int, 0)]) >= @as(c_int, afz_fgd_88.*.uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))][@bitCast(@as(isize, @intCast(afz_i_91)))][@as(c_int, 0)]))) @as(c_int, 53) else @as(c_int, 52)));
                continue;
            },
            55 => {
                afz_fgd_88.*.uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))][@bitCast(@as(isize, @intCast(afz_i_91)))][@as(c_int, 0)] = @truncate(antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 54;
                continue;
            },
            56 => {
                afz_i_91 = 0;
                afz_pc = 50;
                continue;
            },
            57 => {
                afz_pc = 2;
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if (afz_fgd_88.*.num_uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))] > @as(c_int, 10)) @as(c_int, 57) else @as(c_int, 56)));
                continue;
            },
            59 => {
                afz_fgd_88.*.num_uv_points[@bitCast(@as(isize, @intCast(afz_pl_90)))] = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 4))));
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_pl_90 = 0;
                afz_pc = 48;
                continue;
            },
            61 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(c_int, afz_seqhdr_0.*.monochrome) != 0) or (afz_fgd_88.*.chroma_scaling_from_luma != 0)) or (((@as(c_int, afz_seqhdr_0.*.ss_ver) == @as(c_int, 1)) and (@as(c_int, afz_seqhdr_0.*.ss_hor) == @as(c_int, 1))) and !(afz_fgd_88.*.num_y_points != 0))) @as(c_int, 47) else @as(c_int, 60)));
                continue;
            },
            62 => {
                afz_fgd_88.*.chroma_scaling_from_luma = @as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool(!(@as(c_int, afz_seqhdr_0.*.monochrome) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0)));
                afz_pc = 61;
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_89 < afz_fgd_88.*.num_y_points) @as(c_int, 68) else @as(c_int, 62)));
                continue;
            },
            64 => {
                afz_i_89 += 1;
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_fgd_88.*.y_points[@bitCast(@as(isize, @intCast(afz_i_89)))][@as(c_int, 1)] = @truncate(antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_pc = 2;
                continue;
            },
            67 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_i_89 != 0) and (@as(c_int, afz_fgd_88.*.y_points[@bitCast(@as(isize, @intCast(afz_i_89 - @as(c_int, 1))))][@as(c_int, 0)]) >= @as(c_int, afz_fgd_88.*.y_points[@bitCast(@as(isize, @intCast(afz_i_89)))][@as(c_int, 0)]))) @as(c_int, 66) else @as(c_int, 65)));
                continue;
            },
            68 => {
                afz_fgd_88.*.y_points[@bitCast(@as(isize, @intCast(afz_i_89)))][@as(c_int, 0)] = @truncate(antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_i_89 = 0;
                afz_pc = 63;
                continue;
            },
            70 => {
                afz_pc = 2;
                continue;
            },
            71 => {
                afz_pc = @bitCast(@as(c_int, if (afz_fgd_88.*.num_y_points > @as(c_int, 14)) @as(c_int, 70) else @as(c_int, 69)));
                continue;
            },
            72 => {
                afz_fgd_88.*.num_y_points = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, 4))));
                afz_pc = 71;
                continue;
            },
            73 => {
                afz_fgd_88.*.seed = afz_seed_85;
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_fgd_88 = &afz_hdr_1.*.film_grain.data;
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_hdr_1.*.film_grain.update) != 0)) @as(c_int, 14) else @as(c_int, 74)));
                continue;
            },
            76 => {
                afz_hdr_1.*.film_grain.update = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool(afz_hdr_1.*.frame_type != @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_INTER))))) != 0) or (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_seed_85 = antfly_av1_dav1d_get_bits(gb, 16);
                afz_pc = 76;
                continue;
            },
            78 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&afz_hdr_1.*.film_grain.data)), 0, @sizeOf(@TypeOf(afz_hdr_1.*.film_grain.data)));
                afz_pc = 3;
                continue;
            },
            79 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.film_grain.present != 0) @as(c_int, 77) else @as(c_int, 78)));
                continue;
            },
            80 => {
                afz_hdr_1.*.film_grain.present = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool((@as(c_int, afz_seqhdr_0.*.film_grain_present) != 0) and ((@as(c_int, afz_hdr_1.*.show_frame) != 0) or (@as(c_int, afz_hdr_1.*.showable_frame) != 0)))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_78 < @as(c_int, 7)) @as(c_int, 109) else @as(c_int, 80)));
                continue;
            },
            82 => {
                afz_i_78 += 1;
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_mat_81[@as(c_int, 1)] = antfly_av1_dav1d_get_bits_subexp(gb, afz_ref_mat_82[@as(c_int, 1)] >> @intCast(afz_shift_84), @bitCast(@as(c_int, afz_bits_83))) * (@as(c_int, 1) << @intCast(afz_shift_84));
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_mat_81[@as(c_int, 0)] = antfly_av1_dav1d_get_bits_subexp(gb, afz_ref_mat_82[@as(c_int, 0)] >> @intCast(afz_shift_84), @bitCast(@as(c_int, afz_bits_83))) * (@as(c_int, 1) << @intCast(afz_shift_84));
                afz_pc = 83;
                continue;
            },
            85 => {
                afz_mat_81[@as(c_int, 5)] = (@as(c_int, 1) << @intCast(@as(c_int, 16))) + (@as(c_int, 2) * antfly_av1_dav1d_get_bits_subexp(gb, (afz_ref_mat_82[@as(c_int, 5)] - (@as(c_int, 1) << @intCast(@as(c_int, 16)))) >> @intCast(@as(c_int, 1)), 12));
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_mat_81[@as(c_int, 4)] = @as(c_int, 2) * antfly_av1_dav1d_get_bits_subexp(gb, afz_ref_mat_82[@as(c_int, 4)] >> @intCast(@as(c_int, 1)), 12);
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_mat_81[@as(c_int, 5)] = afz_mat_81[@as(c_int, 2)];
                afz_pc = 84;
                continue;
            },
            88 => {
                afz_mat_81[@as(c_int, 4)] = -afz_mat_81[@as(c_int, 3)];
                afz_pc = 87;
                continue;
            },
            89 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.gmv[@bitCast(@as(isize, @intCast(afz_i_78)))].type == @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_AFFINE)) @as(c_int, 86) else @as(c_int, 88)));
                continue;
            },
            90 => {
                afz_shift_84 = 10;
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_bits_83 = 12;
                afz_pc = 90;
                continue;
            },
            92 => {
                afz_mat_81[@as(c_int, 3)] = @as(c_int, 2) * antfly_av1_dav1d_get_bits_subexp(gb, afz_ref_mat_82[@as(c_int, 3)] >> @intCast(@as(c_int, 1)), 12);
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_mat_81[@as(c_int, 2)] = (@as(c_int, 1) << @intCast(@as(c_int, 16))) + (@as(c_int, 2) * antfly_av1_dav1d_get_bits_subexp(gb, (afz_ref_mat_82[@as(c_int, 2)] - (@as(c_int, 1) << @intCast(@as(c_int, 16)))) >> @intCast(@as(c_int, 1)), 12));
                afz_pc = 92;
                continue;
            },
            94 => {
                afz_shift_84 = @as(c_int, 13) + @as(c_int, @intFromBool(!(@as(c_int, afz_hdr_1.*.hp) != 0)));
                afz_pc = 89;
                continue;
            },
            95 => {
                afz_bits_83 = @as(c_int, 9) - @as(c_int, @intFromBool(!(@as(c_int, afz_hdr_1.*.hp) != 0)));
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.gmv[@bitCast(@as(isize, @intCast(afz_i_78)))].type >= @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_ROT_ZOOM)) @as(c_int, 93) else @as(c_int, 95)));
                continue;
            },
            97 => {
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_ref_mat_82 = @ptrCast(@alignCast(&afz_ref_gmv_79.*.matrix));
                afz_pc = 97;
                continue;
            },
            99 => {
                afz_mat_81 = @ptrCast(@alignCast(&afz_hdr_1.*.gmv[@bitCast(@as(isize, @intCast(afz_i_78)))].matrix));
                afz_pc = 98;
                continue;
            },
            100 => {
                afz_ref_gmv_79 = @ptrCast(@alignCast(@constCast(&antfly_av1_dav1d_default_wm_params)));
                afz_pc = 99;
                continue;
            },
            101 => {
                afz_ref_gmv_79 = &c.*.refs[@bitCast(@as(isize, @intCast(afz_pri_ref_80)))].p.p.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(afz_i_78)))];
                afz_pc = 99;
                continue;
            },
            102 => {
                afz_pc = 2;
                continue;
            },
            103 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_pri_ref_80)))].p.p.frame_hdr != null)) @as(c_int, 102) else @as(c_int, 101)));
                continue;
            },
            104 => {
                afz_pri_ref_80 = afz_hdr_1.*.refidx[afz_hdr_1.*.primary_ref_frame];
                afz_pc = 103;
                continue;
            },
            105 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.primary_ref_frame) == @as(c_int, 7)) @as(c_int, 100) else @as(c_int, 104)));
                continue;
            },
            106 => {
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_pc = 82;
                continue;
            },
            108 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.gmv[@bitCast(@as(isize, @intCast(afz_i_78)))].type == @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_IDENTITY)) @as(c_int, 107) else @as(c_int, 106)));
                continue;
            },
            109 => {
                afz_hdr_1.*.gmv[@bitCast(@as(isize, @intCast(afz_i_78)))].type = @bitCast(if (!(antfly_av1_dav1d_get_bit(gb) != 0)) DAV1D_WM_TYPE_IDENTITY else if (antfly_av1_dav1d_get_bit(gb) != 0) DAV1D_WM_TYPE_ROT_ZOOM else if (antfly_av1_dav1d_get_bit(gb) != 0) DAV1D_WM_TYPE_TRANSLATION else DAV1D_WM_TYPE_AFFINE);
                afz_pc = 108;
                continue;
            },
            110 => {
                afz_i_78 = 0;
                afz_pc = 81;
                continue;
            },
            111 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) @as(c_int, 110) else @as(c_int, 80)));
                continue;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_77 < @as(c_int, 7)) @as(c_int, 114) else @as(c_int, 111)));
                continue;
            },
            113 => {
                afz_i_77 += 1;
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_hdr_1.*.gmv[@bitCast(@as(isize, @intCast(afz_i_77)))] = antfly_av1_dav1d_default_wm_params;
                afz_pc = 113;
                continue;
            },
            115 => {
                afz_i_77 = 0;
                afz_pc = 112;
                continue;
            },
            116 => {
                afz_hdr_1.*.reduced_txtp_set = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 115;
                continue;
            },
            117 => {
                afz_hdr_1.*.warp_motion = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool((!(@as(c_int, afz_hdr_1.*.error_resilient_mode) != 0) and (@as(c_int, @bitCast(afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1))) != 0)) and (@as(c_int, afz_seqhdr_0.*.warped_motion) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 116;
                continue;
            },
            118 => {
                afz_hdr_1.*.skip_mode_enabled = @truncate(if (afz_hdr_1.*.skip_mode_allowed != 0) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0));
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_hdr_1.*.skip_mode_allowed = 1;
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_hdr_1.*.skip_mode_refs[@as(c_int, 1)] = @truncate(imax(afz_off_before_idx_68, afz_off_after_idx_69));
                afz_pc = 119;
                continue;
            },
            121 => {
                afz_hdr_1.*.skip_mode_refs[@as(c_int, 0)] = @truncate(imin(afz_off_before_idx_68, afz_off_after_idx_69));
                afz_pc = 120;
                continue;
            },
            122 => {
                afz_hdr_1.*.skip_mode_allowed = 1;
                afz_pc = 118;
                continue;
            },
            123 => {
                afz_hdr_1.*.skip_mode_refs[@as(c_int, 1)] = @truncate(imax(afz_off_before_idx_68, afz_off_before2_idx_74));
                afz_pc = 122;
                continue;
            },
            124 => {
                afz_hdr_1.*.skip_mode_refs[@as(c_int, 0)] = @truncate(imin(afz_off_before_idx_68, afz_off_before2_idx_74));
                afz_pc = 123;
                continue;
            },
            125 => {
                afz_pc = @bitCast(@as(c_int, if (afz_off_before2_73 != @as(c_uint, 4294967295)) @as(c_int, 124) else @as(c_int, 118)));
                continue;
            },
            126 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_75 < @as(c_int, 7)) @as(c_int, 134) else @as(c_int, 125)));
                continue;
            },
            127 => {
                afz_i_75 += 1;
                afz_pc = 126;
                continue;
            },
            128 => {
                afz_off_before2_idx_74 = afz_i_75;
                afz_pc = 127;
                continue;
            },
            129 => {
                afz_off_before2_73 = afz_refpoc_76;
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_off_before2_73 == @as(c_uint, 4294967295)) or (get_poc_diff(afz_seqhdr_0.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(afz_refpoc_76))), @bitCast(@as(c_uint, @truncate(afz_off_before2_73)))) > @as(c_int, 0))) @as(c_int, 129) else @as(c_int, 127)));
                continue;
            },
            131 => {
                afz_pc = @bitCast(@as(c_int, if (get_poc_diff(afz_seqhdr_0.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(afz_refpoc_76))), @bitCast(@as(c_uint, @truncate(afz_off_before_66)))) < @as(c_int, 0)) @as(c_int, 130) else @as(c_int, 127)));
                continue;
            },
            132 => {
                afz_refpoc_76 = c.*.refs[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_75)))])))].p.p.frame_hdr.*.frame_offset;
                afz_pc = 131;
                continue;
            },
            133 => {
                afz_pc = 2;
                continue;
            },
            134 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_75)))])))].p.p.frame_hdr != null)) @as(c_int, 133) else @as(c_int, 132)));
                continue;
            },
            135 => {
                afz_i_75 = 0;
                afz_pc = 126;
                continue;
            },
            136 => {
                afz_pc = 135;
                continue;
            },
            137 => {
                afz_off_before2_73 = 4294967295;
                afz_pc = 136;
                continue;
            },
            138 => {
                afz_pc = @bitCast(@as(c_int, if (afz_off_before_66 != @as(c_uint, 4294967295)) @as(c_int, 137) else @as(c_int, 118)));
                continue;
            },
            139 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_off_before_66 != @as(c_uint, 4294967295)) and (afz_off_after_67 != -@as(c_int, 1))) @as(c_int, 121) else @as(c_int, 138)));
                continue;
            },
            140 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_70 < @as(c_int, 7)) @as(c_int, 152) else @as(c_int, 139)));
                continue;
            },
            141 => {
                afz_i_70 += 1;
                afz_pc = 140;
                continue;
            },
            142 => {
                afz_off_after_idx_69 = afz_i_70;
                afz_pc = 141;
                continue;
            },
            143 => {
                afz_off_after_67 = @bitCast(@as(c_uint, @truncate(afz_refpoc_71)));
                afz_pc = 142;
                continue;
            },
            144 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_off_after_67 == -@as(c_int, 1)) or (get_poc_diff(afz_seqhdr_0.*.order_hint_n_bits, afz_off_after_67, @bitCast(@as(c_uint, @truncate(afz_refpoc_71)))) > @as(c_int, 0))) @as(c_int, 143) else @as(c_int, 141)));
                continue;
            },
            145 => {
                afz_off_before_idx_68 = afz_i_70;
                afz_pc = 141;
                continue;
            },
            146 => {
                afz_off_before_66 = afz_refpoc_71;
                afz_pc = 145;
                continue;
            },
            147 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_diff_72 < @as(c_int, 0)) and ((afz_off_before_66 == @as(c_uint, 4294967295)) or (get_poc_diff(afz_seqhdr_0.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(afz_refpoc_71))), @bitCast(@as(c_uint, @truncate(afz_off_before_66)))) > @as(c_int, 0)))) @as(c_int, 146) else @as(c_int, 141)));
                continue;
            },
            148 => {
                afz_pc = @bitCast(@as(c_int, if (afz_diff_72 > @as(c_int, 0)) @as(c_int, 144) else @as(c_int, 147)));
                continue;
            },
            149 => {
                afz_diff_72 = get_poc_diff(afz_seqhdr_0.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(afz_refpoc_71))), @bitCast(@as(c_uint, @truncate(afz_poc_65))));
                afz_pc = 148;
                continue;
            },
            150 => {
                afz_refpoc_71 = c.*.refs[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_70)))])))].p.p.frame_hdr.*.frame_offset;
                afz_pc = 149;
                continue;
            },
            151 => {
                afz_pc = 2;
                continue;
            },
            152 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_70)))])))].p.p.frame_hdr != null)) @as(c_int, 151) else @as(c_int, 150)));
                continue;
            },
            153 => {
                afz_i_70 = 0;
                afz_pc = 140;
                continue;
            },
            154 => {
                afz_pc = 153;
                continue;
            },
            155 => {
                afz_off_after_67 = -@as(c_int, 1);
                afz_pc = 154;
                continue;
            },
            156 => {
                afz_off_before_66 = 4294967295;
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_poc_65 = afz_hdr_1.*.frame_offset;
                afz_pc = 156;
                continue;
            },
            158 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(c_int, afz_hdr_1.*.switchable_comp_refs) != 0) and (@as(c_int, @bitCast(afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1))) != 0)) and (@as(c_int, afz_seqhdr_0.*.order_hint) != 0)) @as(c_int, 157) else @as(c_int, 118)));
                continue;
            },
            159 => {
                afz_hdr_1.*.skip_mode_allowed = 0;
                afz_pc = 158;
                continue;
            },
            160 => {
                afz_hdr_1.*.switchable_comp_refs = @truncate(if ((afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0));
                afz_pc = 159;
                continue;
            },
            161 => {
                afz_hdr_1.*.txfm_mode = @bitCast(if (afz_hdr_1.*.all_lossless != 0) DAV1D_TX_4X4_ONLY else if (antfly_av1_dav1d_get_bit(gb) != 0) DAV1D_TX_SWITCHABLE else DAV1D_TX_LARGEST);
                afz_pc = 160;
                continue;
            },
            162 => {
                {
                    const ref = &afz_hdr_1.*.restoration.unit_size[@as(c_int, 1)];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) -% antfly_av1_dav1d_get_bit(gb));
                }
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_pc = @bitCast(@as(c_int, if ((((afz_hdr_1.*.restoration.type[@as(c_int, 1)] != 0) or (afz_hdr_1.*.restoration.type[@as(c_int, 2)] != 0)) and (@as(c_int, afz_seqhdr_0.*.ss_hor) == @as(c_int, 1))) and (@as(c_int, afz_seqhdr_0.*.ss_ver) == @as(c_int, 1))) @as(c_int, 162) else @as(c_int, 161)));
                continue;
            },
            164 => {
                afz_hdr_1.*.restoration.unit_size[@as(c_int, 1)] = afz_hdr_1.*.restoration.unit_size[@as(c_int, 0)];
                afz_pc = 163;
                continue;
            },
            165 => {
                {
                    const ref = &afz_hdr_1.*.restoration.unit_size[@as(c_int, 0)];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_get_bit(gb));
                }
                afz_pc = 164;
                continue;
            },
            166 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_seqhdr_0.*.sb128) != 0)) @as(c_int, 165) else @as(c_int, 164)));
                continue;
            },
            167 => {
                afz_hdr_1.*.restoration.unit_size[@as(c_int, 0)] +%= 1;
                afz_pc = 166;
                continue;
            },
            168 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 167) else @as(c_int, 164)));
                continue;
            },
            169 => {
                afz_hdr_1.*.restoration.unit_size[@as(c_int, 0)] = @bitCast(@as(i8, @truncate(@as(c_int, 6) + @as(c_int, afz_seqhdr_0.*.sb128))));
                afz_pc = 168;
                continue;
            },
            170 => {
                afz_hdr_1.*.restoration.unit_size[@as(c_int, 0)] = 8;
                afz_pc = 161;
                continue;
            },
            171 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_hdr_1.*.restoration.type[@as(c_int, 0)] != 0) or (afz_hdr_1.*.restoration.type[@as(c_int, 1)] != 0)) or (@as(c_int, @bitCast(afz_hdr_1.*.restoration.type[@as(c_int, 2)])) != 0)) @as(c_int, 169) else @as(c_int, 170)));
                continue;
            },
            172 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 2)] = antfly_av1_dav1d_get_bits(gb, 2);
                afz_pc = 171;
                continue;
            },
            173 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 1)] = antfly_av1_dav1d_get_bits(gb, 2);
                afz_pc = 172;
                continue;
            },
            174 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 1)] = blk: {
                    const tmp = @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_NONE);
                    afz_hdr_1.*.restoration.type[@as(c_int, 2)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 171;
                continue;
            },
            175 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_seqhdr_0.*.monochrome) != 0)) @as(c_int, 173) else @as(c_int, 174)));
                continue;
            },
            176 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 0)] = antfly_av1_dav1d_get_bits(gb, 2);
                afz_pc = 175;
                continue;
            },
            177 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 2)] = DAV1D_RESTORATION_NONE;
                afz_pc = 161;
                continue;
            },
            178 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 1)] = DAV1D_RESTORATION_NONE;
                afz_pc = 177;
                continue;
            },
            179 => {
                afz_hdr_1.*.restoration.type[@as(c_int, 0)] = DAV1D_RESTORATION_NONE;
                afz_pc = 178;
                continue;
            },
            180 => {
                afz_pc = @bitCast(@as(c_int, if (((!(@as(c_int, afz_hdr_1.*.all_lossless) != 0) or (@as(c_int, afz_hdr_1.*.super_res.enabled) != 0)) and (@as(c_int, afz_seqhdr_0.*.restoration) != 0)) and !(@as(c_int, afz_hdr_1.*.allow_intrabc) != 0)) @as(c_int, 176) else @as(c_int, 179)));
                continue;
            },
            181 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_64 < (@as(c_int, 1) << @intCast(@as(c_int, afz_hdr_1.*.cdef.n_bits)))) @as(c_int, 185) else @as(c_int, 180)));
                continue;
            },
            182 => {
                afz_i_64 += 1;
                afz_pc = 181;
                continue;
            },
            183 => {
                afz_hdr_1.*.cdef.uv_strength[@bitCast(@as(isize, @intCast(afz_i_64)))] = @truncate(antfly_av1_dav1d_get_bits(gb, 6));
                afz_pc = 182;
                continue;
            },
            184 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_seqhdr_0.*.monochrome) != 0)) @as(c_int, 183) else @as(c_int, 182)));
                continue;
            },
            185 => {
                afz_hdr_1.*.cdef.y_strength[@bitCast(@as(isize, @intCast(afz_i_64)))] = @truncate(antfly_av1_dav1d_get_bits(gb, 6));
                afz_pc = 184;
                continue;
            },
            186 => {
                afz_i_64 = 0;
                afz_pc = 181;
                continue;
            },
            187 => {
                afz_hdr_1.*.cdef.n_bits = @truncate(antfly_av1_dav1d_get_bits(gb, 2));
                afz_pc = 186;
                continue;
            },
            188 => {
                afz_hdr_1.*.cdef.damping = @truncate(antfly_av1_dav1d_get_bits(gb, 2) +% @as(c_uint, 3));
                afz_pc = 187;
                continue;
            },
            189 => {
                afz_hdr_1.*.cdef.uv_strength[@as(c_int, 0)] = 0;
                afz_pc = 180;
                continue;
            },
            190 => {
                afz_hdr_1.*.cdef.y_strength[@as(c_int, 0)] = 0;
                afz_pc = 189;
                continue;
            },
            191 => {
                afz_hdr_1.*.cdef.n_bits = 0;
                afz_pc = 190;
                continue;
            },
            192 => {
                afz_pc = @bitCast(@as(c_int, if ((!(@as(c_int, afz_hdr_1.*.all_lossless) != 0) and (@as(c_int, afz_seqhdr_0.*.cdef) != 0)) and !(@as(c_int, afz_hdr_1.*.allow_intrabc) != 0)) @as(c_int, 188) else @as(c_int, 191)));
                continue;
            },
            193 => {
                afz_hdr_1.*.loopfilter.mode_ref_deltas = default_mode_ref_deltas;
                afz_pc = 192;
                continue;
            },
            194 => {
                afz_hdr_1.*.loopfilter.mode_ref_delta_update = 1;
                afz_pc = 193;
                continue;
            },
            195 => {
                afz_hdr_1.*.loopfilter.mode_ref_delta_enabled = 1;
                afz_pc = 194;
                continue;
            },
            196 => {
                afz_hdr_1.*.loopfilter.sharpness = 0;
                afz_pc = 195;
                continue;
            },
            197 => {
                afz_hdr_1.*.loopfilter.level_u = blk: {
                    const tmp = @as(u8, 0);
                    afz_hdr_1.*.loopfilter.level_v = tmp;
                    break :blk tmp;
                };
                afz_pc = 196;
                continue;
            },
            198 => {
                afz_hdr_1.*.loopfilter.level_y[@as(c_int, 0)] = blk: {
                    const tmp = @as(u8, 0);
                    afz_hdr_1.*.loopfilter.level_y[@as(c_int, 1)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 197;
                continue;
            },
            199 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_63 < @as(c_int, 2)) @as(c_int, 202) else @as(c_int, 192)));
                continue;
            },
            200 => {
                afz_i_63 += 1;
                afz_pc = 199;
                continue;
            },
            201 => {
                afz_hdr_1.*.loopfilter.mode_ref_deltas.mode_delta[@bitCast(@as(isize, @intCast(afz_i_63)))] = @truncate(antfly_av1_dav1d_get_sbits(gb, 7));
                afz_pc = 200;
                continue;
            },
            202 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 201) else @as(c_int, 200)));
                continue;
            },
            203 => {
                afz_i_63 = 0;
                afz_pc = 199;
                continue;
            },
            204 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_62 < @as(c_int, 8)) @as(c_int, 207) else @as(c_int, 203)));
                continue;
            },
            205 => {
                afz_i_62 += 1;
                afz_pc = 204;
                continue;
            },
            206 => {
                afz_hdr_1.*.loopfilter.mode_ref_deltas.ref_delta[@bitCast(@as(isize, @intCast(afz_i_62)))] = @truncate(antfly_av1_dav1d_get_sbits(gb, 7));
                afz_pc = 205;
                continue;
            },
            207 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 206) else @as(c_int, 205)));
                continue;
            },
            208 => {
                afz_i_62 = 0;
                afz_pc = 204;
                continue;
            },
            209 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.loopfilter.mode_ref_delta_update != 0) @as(c_int, 208) else @as(c_int, 192)));
                continue;
            },
            210 => {
                afz_hdr_1.*.loopfilter.mode_ref_delta_update = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 209;
                continue;
            },
            211 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.loopfilter.mode_ref_delta_enabled != 0) @as(c_int, 210) else @as(c_int, 192)));
                continue;
            },
            212 => {
                afz_hdr_1.*.loopfilter.mode_ref_delta_enabled = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_hdr_1.*.loopfilter.mode_ref_deltas = default_mode_ref_deltas;
                afz_pc = 212;
                continue;
            },
            214 => {
                afz_hdr_1.*.loopfilter.mode_ref_deltas = c.*.refs[@bitCast(@as(isize, @intCast(afz_ref_61)))].p.p.frame_hdr.*.loopfilter.mode_ref_deltas;
                afz_pc = 212;
                continue;
            },
            215 => {
                afz_pc = 2;
                continue;
            },
            216 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_ref_61)))].p.p.frame_hdr != null)) @as(c_int, 215) else @as(c_int, 214)));
                continue;
            },
            217 => {
                afz_ref_61 = afz_hdr_1.*.refidx[afz_hdr_1.*.primary_ref_frame];
                afz_pc = 216;
                continue;
            },
            218 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.primary_ref_frame) == @as(c_int, 7)) @as(c_int, 213) else @as(c_int, 217)));
                continue;
            },
            219 => {
                afz_hdr_1.*.loopfilter.sharpness = @truncate(antfly_av1_dav1d_get_bits(gb, 3));
                afz_pc = 218;
                continue;
            },
            220 => {
                afz_hdr_1.*.loopfilter.level_v = @truncate(antfly_av1_dav1d_get_bits(gb, 6));
                afz_pc = 219;
                continue;
            },
            221 => {
                afz_hdr_1.*.loopfilter.level_u = @truncate(antfly_av1_dav1d_get_bits(gb, 6));
                afz_pc = 220;
                continue;
            },
            222 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_seqhdr_0.*.monochrome) != 0) and ((@as(c_int, afz_hdr_1.*.loopfilter.level_y[@as(c_int, 0)]) != 0) or (@as(c_int, afz_hdr_1.*.loopfilter.level_y[@as(c_int, 1)]) != 0))) @as(c_int, 221) else @as(c_int, 219)));
                continue;
            },
            223 => {
                afz_hdr_1.*.loopfilter.level_y[@as(c_int, 1)] = @truncate(antfly_av1_dav1d_get_bits(gb, 6));
                afz_pc = 222;
                continue;
            },
            224 => {
                afz_hdr_1.*.loopfilter.level_y[@as(c_int, 0)] = @truncate(antfly_av1_dav1d_get_bits(gb, 6));
                afz_pc = 223;
                continue;
            },
            225 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_hdr_1.*.all_lossless) != 0) or (@as(c_int, afz_hdr_1.*.allow_intrabc) != 0)) @as(c_int, 198) else @as(c_int, 224)));
                continue;
            },
            226 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_60 < @as(c_int, 8)) @as(c_int, 230) else @as(c_int, 225)));
                continue;
            },
            227 => {
                afz_i_60 += 1;
                afz_pc = 226;
                continue;
            },
            228 => {
                {
                    const ref = &afz_hdr_1.*.all_lossless;
                    ref.* = @bitCast(@as(i8, @truncate(@as(c_int, ref.*) & @as(c_int, afz_hdr_1.*.segmentation.lossless[@bitCast(@as(isize, @intCast(afz_i_60)))]))));
                }
                afz_pc = 227;
                continue;
            },
            229 => {
                afz_hdr_1.*.segmentation.lossless[@bitCast(@as(isize, @intCast(afz_i_60)))] = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(!(@as(c_int, afz_hdr_1.*.segmentation.qidx[@bitCast(@as(isize, @intCast(afz_i_60)))]) != 0) and (afz_delta_lossless_59 != 0))))));
                afz_pc = 228;
                continue;
            },
            230 => {
                afz_hdr_1.*.segmentation.qidx[@bitCast(@as(isize, @intCast(afz_i_60)))] = @bitCast(@as(i8, @truncate(if (afz_hdr_1.*.segmentation.enabled != 0) iclip_u8(@as(c_int, afz_hdr_1.*.quant.yac) + @as(c_int, afz_hdr_1.*.segmentation.seg_data.d[@bitCast(@as(isize, @intCast(afz_i_60)))].delta_q)) else @as(c_int, afz_hdr_1.*.quant.yac))));
                afz_pc = 229;
                continue;
            },
            231 => {
                afz_i_60 = 0;
                afz_pc = 226;
                continue;
            },
            232 => {
                afz_hdr_1.*.all_lossless = 1;
                afz_pc = 231;
                continue;
            },
            233 => {
                afz_delta_lossless_59 = @as(c_int, @intFromBool((((!(@as(c_int, afz_hdr_1.*.quant.ydc_delta) != 0) and !(@as(c_int, afz_hdr_1.*.quant.udc_delta) != 0)) and !(@as(c_int, afz_hdr_1.*.quant.uac_delta) != 0)) and !(@as(c_int, afz_hdr_1.*.quant.vdc_delta) != 0)) and !(@as(c_int, afz_hdr_1.*.quant.vac_delta) != 0)));
                afz_pc = 232;
                continue;
            },
            234 => {
                afz_hdr_1.*.delta.lf.multi = @truncate(if (afz_hdr_1.*.delta.lf.present != 0) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0));
                afz_pc = 233;
                continue;
            },
            235 => {
                afz_hdr_1.*.delta.lf.res_log2 = @truncate(if (afz_hdr_1.*.delta.lf.present != 0) antfly_av1_dav1d_get_bits(gb, 2) else @as(c_uint, 0));
                afz_pc = 234;
                continue;
            },
            236 => {
                afz_hdr_1.*.delta.lf.present = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool((@as(c_int, afz_hdr_1.*.delta.q.present) != 0) and !(@as(c_int, afz_hdr_1.*.allow_intrabc) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 235;
                continue;
            },
            237 => {
                afz_hdr_1.*.delta.q.res_log2 = @truncate(if (afz_hdr_1.*.delta.q.present != 0) antfly_av1_dav1d_get_bits(gb, 2) else @as(c_uint, 0));
                afz_pc = 236;
                continue;
            },
            238 => {
                afz_hdr_1.*.delta.q.present = @truncate(if (afz_hdr_1.*.quant.yac != 0) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0));
                afz_pc = 237;
                continue;
            },
            239 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_55 < @as(c_int, 8)) @as(c_int, 272) else @as(c_int, 238)));
                continue;
            },
            240 => {
                afz_i_55 += 1;
                afz_pc = 239;
                continue;
            },
            241 => {
                afz_hdr_1.*.segmentation.seg_data.preskip = 1;
                afz_pc = 240;
                continue;
            },
            242 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 241;
                continue;
            },
            243 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const tmp = @as(u8, @truncate(antfly_av1_dav1d_get_bit(gb)));
                    afz_seg_56.*.globalmv = tmp;
                    break :blk tmp;
                }) != 0) @as(c_int, 242) else @as(c_int, 240)));
                continue;
            },
            244 => {
                afz_hdr_1.*.segmentation.seg_data.preskip = 1;
                afz_pc = 243;
                continue;
            },
            245 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 244;
                continue;
            },
            246 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const tmp = @as(u8, @truncate(antfly_av1_dav1d_get_bit(gb)));
                    afz_seg_56.*.skip = tmp;
                    break :blk tmp;
                }) != 0) @as(c_int, 245) else @as(c_int, 243)));
                continue;
            },
            247 => {
                afz_hdr_1.*.segmentation.seg_data.preskip = 1;
                afz_pc = 246;
                continue;
            },
            248 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 247;
                continue;
            },
            249 => {
                afz_seg_56.*.ref = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_get_bits(gb, 3))));
                afz_pc = 248;
                continue;
            },
            250 => {
                afz_seg_56.*.ref = @truncate(-@as(c_int, 1));
                afz_pc = 246;
                continue;
            },
            251 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 249) else @as(c_int, 250)));
                continue;
            },
            252 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 251;
                continue;
            },
            253 => {
                afz_seg_56.*.delta_lf_v = @truncate(antfly_av1_dav1d_get_sbits(gb, 7));
                afz_pc = 252;
                continue;
            },
            254 => {
                afz_seg_56.*.delta_lf_v = 0;
                afz_pc = 251;
                continue;
            },
            255 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 253) else @as(c_int, 254)));
                continue;
            },
            256 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 255;
                continue;
            },
            257 => {
                afz_seg_56.*.delta_lf_u = @truncate(antfly_av1_dav1d_get_sbits(gb, 7));
                afz_pc = 256;
                continue;
            },
            258 => {
                afz_seg_56.*.delta_lf_u = 0;
                afz_pc = 255;
                continue;
            },
            259 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 257) else @as(c_int, 258)));
                continue;
            },
            260 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 259;
                continue;
            },
            261 => {
                afz_seg_56.*.delta_lf_y_h = @truncate(antfly_av1_dav1d_get_sbits(gb, 7));
                afz_pc = 260;
                continue;
            },
            262 => {
                afz_seg_56.*.delta_lf_y_h = 0;
                afz_pc = 259;
                continue;
            },
            263 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 261) else @as(c_int, 262)));
                continue;
            },
            264 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 263;
                continue;
            },
            265 => {
                afz_seg_56.*.delta_lf_y_v = @truncate(antfly_av1_dav1d_get_sbits(gb, 7));
                afz_pc = 264;
                continue;
            },
            266 => {
                afz_seg_56.*.delta_lf_y_v = 0;
                afz_pc = 263;
                continue;
            },
            267 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 265) else @as(c_int, 266)));
                continue;
            },
            268 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(afz_i_55);
                afz_pc = 267;
                continue;
            },
            269 => {
                afz_seg_56.*.delta_q = @truncate(antfly_av1_dav1d_get_sbits(gb, 9));
                afz_pc = 268;
                continue;
            },
            270 => {
                afz_seg_56.*.delta_q = 0;
                afz_pc = 267;
                continue;
            },
            271 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_int, 269) else @as(c_int, 270)));
                continue;
            },
            272 => {
                afz_seg_56 = &afz_hdr_1.*.segmentation.seg_data.d[@bitCast(@as(isize, @intCast(afz_i_55)))];
                afz_pc = 271;
                continue;
            },
            273 => {
                afz_i_55 = 0;
                afz_pc = 239;
                continue;
            },
            274 => {
                afz_hdr_1.*.segmentation.seg_data.last_active_segid = @truncate(-@as(c_int, 1));
                afz_pc = 273;
                continue;
            },
            275 => {
                afz_hdr_1.*.segmentation.seg_data.preskip = 0;
                afz_pc = 274;
                continue;
            },
            276 => {
                afz_hdr_1.*.segmentation.seg_data = c.*.refs[@bitCast(@as(isize, @intCast(afz_pri_ref_57)))].p.p.frame_hdr.*.segmentation.seg_data;
                afz_pc = 238;
                continue;
            },
            277 => {
                afz_pc = 2;
                continue;
            },
            278 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_pri_ref_57)))].p.p.frame_hdr != null)) @as(c_int, 277) else @as(c_int, 276)));
                continue;
            },
            279 => {
                afz_pri_ref_57 = afz_hdr_1.*.refidx[afz_hdr_1.*.primary_ref_frame];
                afz_pc = 278;
                continue;
            },
            280 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 282) else @as(c_int, 279);
                continue;
            },
            281 => {
                unreachable;
            },
            282 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_hdr_1.*.primary_ref_frame) != @as(c_int, 7))) @as(c_int, 281) else @as(c_int, 280)));
                continue;
            },
            283 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.segmentation.update_data != 0) @as(c_int, 275) else @as(c_int, 282)));
                continue;
            },
            284 => {
                afz_hdr_1.*.segmentation.update_data = 1;
                afz_pc = 283;
                continue;
            },
            285 => {
                afz_hdr_1.*.segmentation.temporal = 0;
                afz_pc = 284;
                continue;
            },
            286 => {
                afz_hdr_1.*.segmentation.update_map = 1;
                afz_pc = 285;
                continue;
            },
            287 => {
                afz_hdr_1.*.segmentation.update_data = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 283;
                continue;
            },
            288 => {
                afz_hdr_1.*.segmentation.temporal = @truncate(if (afz_hdr_1.*.segmentation.update_map != 0) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0));
                afz_pc = 287;
                continue;
            },
            289 => {
                afz_hdr_1.*.segmentation.update_map = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 288;
                continue;
            },
            290 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.primary_ref_frame) == @as(c_int, 7)) @as(c_int, 286) else @as(c_int, 289)));
                continue;
            },
            291 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_58 < @as(c_int, 8)) @as(c_int, 293) else @as(c_int, 238)));
                continue;
            },
            292 => {
                afz_i_58 += 1;
                afz_pc = 291;
                continue;
            },
            293 => {
                afz_hdr_1.*.segmentation.seg_data.d[@bitCast(@as(isize, @intCast(afz_i_58)))].ref = @truncate(-@as(c_int, 1));
                afz_pc = 292;
                continue;
            },
            294 => {
                afz_i_58 = 0;
                afz_pc = 291;
                continue;
            },
            295 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&afz_hdr_1.*.segmentation.seg_data)), 0, @sizeOf(Dav1dSegmentationDataSet));
                afz_pc = 294;
                continue;
            },
            296 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.segmentation.enabled != 0) @as(c_int, 290) else @as(c_int, 295)));
                continue;
            },
            297 => {
                afz_hdr_1.*.segmentation.enabled = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 296;
                continue;
            },
            298 => {
                afz_hdr_1.*.quant.qm_v = @truncate(if (afz_seqhdr_0.*.separate_uv_delta_q != 0) antfly_av1_dav1d_get_bits(gb, 4) else @as(c_uint, @bitCast(@as(c_int, @as(c_int, afz_hdr_1.*.quant.qm_u)))));
                afz_pc = 297;
                continue;
            },
            299 => {
                afz_hdr_1.*.quant.qm_u = @truncate(antfly_av1_dav1d_get_bits(gb, 4));
                afz_pc = 298;
                continue;
            },
            300 => {
                afz_hdr_1.*.quant.qm_y = @truncate(antfly_av1_dav1d_get_bits(gb, 4));
                afz_pc = 299;
                continue;
            },
            301 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.quant.qm != 0) @as(c_int, 300) else @as(c_int, 297)));
                continue;
            },
            302 => {
                afz_hdr_1.*.quant.qm = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 301;
                continue;
            },
            303 => {
                afz_hdr_1.*.quant.vac_delta = @truncate(if (antfly_av1_dav1d_get_bit(gb) != 0) antfly_av1_dav1d_get_sbits(gb, 7) else @as(c_int, 0));
                afz_pc = 302;
                continue;
            },
            304 => {
                afz_hdr_1.*.quant.vdc_delta = @truncate(if (antfly_av1_dav1d_get_bit(gb) != 0) antfly_av1_dav1d_get_sbits(gb, 7) else @as(c_int, 0));
                afz_pc = 303;
                continue;
            },
            305 => {
                afz_hdr_1.*.quant.vac_delta = afz_hdr_1.*.quant.uac_delta;
                afz_pc = 302;
                continue;
            },
            306 => {
                afz_hdr_1.*.quant.vdc_delta = afz_hdr_1.*.quant.udc_delta;
                afz_pc = 305;
                continue;
            },
            307 => {
                afz_pc = @bitCast(@as(c_int, if (afz_diff_uv_delta_54 != 0) @as(c_int, 304) else @as(c_int, 306)));
                continue;
            },
            308 => {
                afz_hdr_1.*.quant.uac_delta = @truncate(if (antfly_av1_dav1d_get_bit(gb) != 0) antfly_av1_dav1d_get_sbits(gb, 7) else @as(c_int, 0));
                afz_pc = 307;
                continue;
            },
            309 => {
                afz_hdr_1.*.quant.udc_delta = @truncate(if (antfly_av1_dav1d_get_bit(gb) != 0) antfly_av1_dav1d_get_sbits(gb, 7) else @as(c_int, 0));
                afz_pc = 308;
                continue;
            },
            310 => {
                afz_diff_uv_delta_54 = @bitCast(@as(c_uint, @truncate(if (afz_seqhdr_0.*.separate_uv_delta_q != 0) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0))));
                afz_pc = 309;
                continue;
            },
            311 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_seqhdr_0.*.monochrome) != 0)) @as(c_int, 310) else @as(c_int, 302)));
                continue;
            },
            312 => {
                afz_hdr_1.*.quant.ydc_delta = @truncate(if (antfly_av1_dav1d_get_bit(gb) != 0) antfly_av1_dav1d_get_sbits(gb, 7) else @as(c_int, 0));
                afz_pc = 311;
                continue;
            },
            313 => {
                afz_hdr_1.*.quant.yac = @truncate(antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 312;
                continue;
            },
            314 => {
                afz_hdr_1.*.tiling.n_bytes = @truncate(antfly_av1_dav1d_get_bits(gb, 2) +% @as(c_uint, 1));
                afz_pc = 313;
                continue;
            },
            315 => {
                afz_pc = 2;
                continue;
            },
            316 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.tiling.update) >= (@as(c_int, afz_hdr_1.*.tiling.cols) * @as(c_int, afz_hdr_1.*.tiling.rows))) @as(c_int, 315) else @as(c_int, 314)));
                continue;
            },
            317 => {
                afz_hdr_1.*.tiling.update = @truncate(antfly_av1_dav1d_get_bits(gb, @as(c_int, afz_hdr_1.*.tiling.log2_cols) + @as(c_int, afz_hdr_1.*.tiling.log2_rows)));
                afz_pc = 316;
                continue;
            },
            318 => {
                afz_hdr_1.*.tiling.update = 0;
                afz_pc = 313;
                continue;
            },
            319 => {
                afz_hdr_1.*.tiling.n_bytes = 0;
                afz_pc = 318;
                continue;
            },
            320 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_hdr_1.*.tiling.log2_cols) != 0) or (@as(c_int, afz_hdr_1.*.tiling.log2_rows) != 0)) @as(c_int, 317) else @as(c_int, 319)));
                continue;
            },
            321 => {
                afz_hdr_1.*.tiling.row_start_sb[afz_hdr_1.*.tiling.rows] = @bitCast(@as(c_short, @truncate(afz_sbh_37)));
                afz_pc = 320;
                continue;
            },
            322 => {
                afz_hdr_1.*.tiling.col_start_sb[afz_hdr_1.*.tiling.cols] = @bitCast(@as(c_short, @truncate(afz_sbw_36)));
                afz_pc = 321;
                continue;
            },
            323 => {
                afz_pc = @bitCast(@as(c_int, if (afz_sby_44 < afz_sbh_37) @as(c_int, 325) else @as(c_int, 322)));
                continue;
            },
            324 => {
                afz_sby_44 += afz_tile_h_43;
                afz_hdr_1.*.tiling.rows +%= 1;
                afz_pc = 323;
                continue;
            },
            325 => {
                afz_hdr_1.*.tiling.row_start_sb[afz_hdr_1.*.tiling.rows] = @bitCast(@as(c_short, @truncate(afz_sby_44)));
                afz_pc = 324;
                continue;
            },
            326 => {
                afz_sby_44 = 0;
                afz_pc = 323;
                continue;
            },
            327 => {
                afz_hdr_1.*.tiling.rows = 0;
                afz_pc = 326;
                continue;
            },
            328 => {
                afz_tile_h_43 = @as(c_int, 1) + ((afz_sbh_37 - @as(c_int, 1)) >> @intCast(@as(c_int, afz_hdr_1.*.tiling.log2_rows)));
                afz_pc = 327;
                continue;
            },
            329 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, afz_hdr_1.*.tiling.log2_rows) < @as(c_int, afz_hdr_1.*.tiling.max_log2_rows)))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0)) @as(c_int, 330) else @as(c_int, 328)));
                continue;
            },
            330 => {
                afz_hdr_1.*.tiling.log2_rows +%= 1;
                afz_pc = 329;
                continue;
            },
            331 => {
                afz_hdr_1.*.tiling.log2_rows = afz_hdr_1.*.tiling.min_log2_rows;
                afz_pc = 329;
                continue;
            },
            332 => {
                afz_hdr_1.*.tiling.min_log2_rows = @bitCast(@as(i8, @truncate(imax(afz_min_log2_tiles_40 - @as(c_int, afz_hdr_1.*.tiling.log2_cols), 0))));
                afz_pc = 331;
                continue;
            },
            333 => {
                afz_pc = @bitCast(@as(c_int, if (afz_sbx_42 < afz_sbw_36) @as(c_int, 335) else @as(c_int, 332)));
                continue;
            },
            334 => {
                afz_sbx_42 += afz_tile_w_41;
                afz_hdr_1.*.tiling.cols +%= 1;
                afz_pc = 333;
                continue;
            },
            335 => {
                afz_hdr_1.*.tiling.col_start_sb[afz_hdr_1.*.tiling.cols] = @bitCast(@as(c_short, @truncate(afz_sbx_42)));
                afz_pc = 334;
                continue;
            },
            336 => {
                afz_sbx_42 = 0;
                afz_pc = 333;
                continue;
            },
            337 => {
                afz_hdr_1.*.tiling.cols = 0;
                afz_pc = 336;
                continue;
            },
            338 => {
                afz_tile_w_41 = @as(c_int, 1) + ((afz_sbw_36 - @as(c_int, 1)) >> @intCast(@as(c_int, afz_hdr_1.*.tiling.log2_cols)));
                afz_pc = 337;
                continue;
            },
            339 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, afz_hdr_1.*.tiling.log2_cols) < @as(c_int, afz_hdr_1.*.tiling.max_log2_cols)))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0)) @as(c_int, 340) else @as(c_int, 338)));
                continue;
            },
            340 => {
                afz_hdr_1.*.tiling.log2_cols +%= 1;
                afz_pc = 339;
                continue;
            },
            341 => {
                afz_hdr_1.*.tiling.log2_cols = afz_hdr_1.*.tiling.min_log2_cols;
                afz_pc = 339;
                continue;
            },
            342 => {
                afz_hdr_1.*.tiling.log2_rows = @bitCast(@as(i8, @truncate(tile_log2(1, afz_hdr_1.*.tiling.rows))));
                afz_pc = 322;
                continue;
            },
            343 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_sby_51 < afz_sbh_37) and (@as(c_int, afz_hdr_1.*.tiling.rows) < @as(c_int, 64))) @as(c_int, 348) else @as(c_int, 342)));
                continue;
            },
            344 => {
                afz_hdr_1.*.tiling.rows +%= 1;
                afz_pc = 343;
                continue;
            },
            345 => {
                afz_sby_51 += afz_tile_h_53;
                afz_pc = 344;
                continue;
            },
            346 => {
                afz_hdr_1.*.tiling.row_start_sb[afz_hdr_1.*.tiling.rows] = @bitCast(@as(c_short, @truncate(afz_sby_51)));
                afz_pc = 345;
                continue;
            },
            347 => {
                afz_tile_h_53 = @bitCast(@as(c_uint, @truncate(if (afz_tile_height_sb_52 > @as(c_int, 1)) @as(c_uint, 1) +% antfly_av1_dav1d_get_uniform(gb, @bitCast(@as(c_int, afz_tile_height_sb_52))) else @as(c_uint, 1))));
                afz_pc = 346;
                continue;
            },
            348 => {
                afz_tile_height_sb_52 = imin(afz_sbh_37 - afz_sby_51, afz_max_tile_height_sb_50);
                afz_pc = 347;
                continue;
            },
            349 => {
                afz_sby_51 = 0;
                afz_pc = 343;
                continue;
            },
            350 => {
                afz_hdr_1.*.tiling.rows = 0;
                afz_pc = 349;
                continue;
            },
            351 => {
                afz_max_tile_height_sb_50 = imax(@divTrunc(afz_max_tile_area_sb_46, afz_widest_tile_45), 1);
                afz_pc = 350;
                continue;
            },
            352 => {
                afz_max_tile_area_sb_46 >>= @intCast(afz_min_log2_tiles_40 + @as(c_int, 1));
                afz_pc = 351;
                continue;
            },
            353 => {
                afz_pc = @bitCast(@as(c_int, if (afz_min_log2_tiles_40 != 0) @as(c_int, 352) else @as(c_int, 351)));
                continue;
            },
            354 => {
                afz_hdr_1.*.tiling.log2_cols = @bitCast(@as(i8, @truncate(tile_log2(1, afz_hdr_1.*.tiling.cols))));
                afz_pc = 353;
                continue;
            },
            355 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_sbx_47 < afz_sbw_36) and (@as(c_int, afz_hdr_1.*.tiling.cols) < @as(c_int, 64))) @as(c_int, 361) else @as(c_int, 354)));
                continue;
            },
            356 => {
                afz_hdr_1.*.tiling.cols +%= 1;
                afz_pc = 355;
                continue;
            },
            357 => {
                afz_widest_tile_45 = imax(afz_widest_tile_45, afz_tile_w_49);
                afz_pc = 356;
                continue;
            },
            358 => {
                afz_sbx_47 += afz_tile_w_49;
                afz_pc = 357;
                continue;
            },
            359 => {
                afz_hdr_1.*.tiling.col_start_sb[afz_hdr_1.*.tiling.cols] = @bitCast(@as(c_short, @truncate(afz_sbx_47)));
                afz_pc = 358;
                continue;
            },
            360 => {
                afz_tile_w_49 = @bitCast(@as(c_uint, @truncate(if (afz_tile_width_sb_48 > @as(c_int, 1)) @as(c_uint, 1) +% antfly_av1_dav1d_get_uniform(gb, @bitCast(@as(c_int, afz_tile_width_sb_48))) else @as(c_uint, 1))));
                afz_pc = 359;
                continue;
            },
            361 => {
                afz_tile_width_sb_48 = imin(afz_sbw_36 - afz_sbx_47, afz_max_tile_width_sb_38);
                afz_pc = 360;
                continue;
            },
            362 => {
                afz_sbx_47 = 0;
                afz_pc = 355;
                continue;
            },
            363 => {
                afz_widest_tile_45 = 0;
                afz_max_tile_area_sb_46 = afz_sbw_36 * afz_sbh_37;
                afz_pc = 362;
                continue;
            },
            364 => {
                afz_hdr_1.*.tiling.cols = 0;
                afz_pc = 363;
                continue;
            },
            365 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.tiling.uniform != 0) @as(c_int, 341) else @as(c_int, 364)));
                continue;
            },
            366 => {
                afz_min_log2_tiles_40 = imax(tile_log2(afz_max_tile_area_sb_39, afz_sbw_36 * afz_sbh_37), afz_hdr_1.*.tiling.min_log2_cols);
                afz_pc = 365;
                continue;
            },
            367 => {
                afz_hdr_1.*.tiling.max_log2_rows = @bitCast(@as(i8, @truncate(tile_log2(1, imin(afz_sbh_37, 64)))));
                afz_pc = 366;
                continue;
            },
            368 => {
                afz_hdr_1.*.tiling.max_log2_cols = @bitCast(@as(i8, @truncate(tile_log2(1, imin(afz_sbw_36, 64)))));
                afz_pc = 367;
                continue;
            },
            369 => {
                afz_hdr_1.*.tiling.min_log2_cols = @bitCast(@as(i8, @truncate(tile_log2(afz_max_tile_width_sb_38, afz_sbw_36))));
                afz_pc = 368;
                continue;
            },
            370 => {
                afz_max_tile_area_sb_39 = (@as(c_int, 4096) * @as(c_int, 2304)) >> @intCast(@as(c_int, 2) * afz_sbsz_log2_35);
                afz_pc = 369;
                continue;
            },
            371 => {
                afz_max_tile_width_sb_38 = @as(c_int, 4096) >> @intCast(afz_sbsz_log2_35);
                afz_pc = 370;
                continue;
            },
            372 => {
                afz_sbh_37 = (afz_hdr_1.*.height + afz_sbsz_min1_34) >> @intCast(afz_sbsz_log2_35);
                afz_pc = 371;
                continue;
            },
            373 => {
                afz_sbw_36 = (afz_hdr_1.*.width[@as(c_int, 0)] + afz_sbsz_min1_34) >> @intCast(afz_sbsz_log2_35);
                afz_pc = 372;
                continue;
            },
            374 => {
                afz_sbsz_log2_35 = @as(c_int, 6) + @as(c_int, afz_seqhdr_0.*.sb128);
                afz_pc = 373;
                continue;
            },
            375 => {
                afz_sbsz_min1_34 = (@as(c_int, 64) << @intCast(@as(c_int, afz_seqhdr_0.*.sb128))) - @as(c_int, 1);
                afz_pc = 374;
                continue;
            },
            376 => {
                afz_hdr_1.*.tiling.uniform = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 375;
                continue;
            },
            377 => {
                afz_hdr_1.*.refresh_context = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((!(@as(c_int, afz_seqhdr_0.*.reduced_still_picture_header) != 0) and !(@as(c_int, afz_hdr_1.*.disable_cdf_update) != 0)) and !(antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 376;
                continue;
            },
            378 => {
                afz_hdr_1.*.use_ref_frame_mvs = 0;
                afz_pc = 377;
                continue;
            },
            379 => {
                afz_hdr_1.*.allow_intrabc = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool((@as(c_int, afz_hdr_1.*.allow_screen_content_tools) != 0) and !(@as(c_int, afz_hdr_1.*.super_res.enabled) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 378;
                continue;
            },
            380 => {
                afz_pc = 2;
                continue;
            },
            381 => {
                afz_pc = @bitCast(@as(c_int, if (read_frame_size(c, gb, 0) < @as(c_int, 0)) @as(c_int, 380) else @as(c_int, 379)));
                continue;
            },
            382 => {
                afz_pc = 2;
                continue;
            },
            383 => {
                afz_pc = @bitCast(@as(c_int, if (((c.*.strict_std_compliance != 0) and (afz_hdr_1.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_INTRA))) and (@as(c_int, afz_hdr_1.*.refresh_frame_flags) == @as(c_int, 255))) @as(c_int, 382) else @as(c_int, 381)));
                continue;
            },
            384 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_8 < @as(c_int, 8)) @as(c_int, 386) else @as(c_int, 383)));
                continue;
            },
            385 => {
                afz_i_8 += 1;
                afz_pc = 384;
                continue;
            },
            386 => {
                _ = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.order_hint_n_bits);
                afz_pc = 385;
                continue;
            },
            387 => {
                afz_i_8 = 0;
                afz_pc = 384;
                continue;
            },
            388 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(c_int, afz_hdr_1.*.refresh_frame_flags) != @as(c_int, 255)) and (@as(c_int, afz_hdr_1.*.error_resilient_mode) != 0)) and (@as(c_int, afz_seqhdr_0.*.order_hint) != 0)) @as(c_int, 387) else @as(c_int, 383)));
                continue;
            },
            389 => {
                afz_hdr_1.*.refresh_frame_flags = @truncate(if ((afz_hdr_1.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_KEY)) and (@as(c_int, afz_hdr_1.*.show_frame) != 0)) @as(c_uint, 255) else antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 388;
                continue;
            },
            390 => {
                afz_hdr_1.*.use_ref_frame_mvs = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool(((!(@as(c_int, afz_hdr_1.*.error_resilient_mode) != 0) and (@as(c_int, afz_seqhdr_0.*.ref_frame_mvs) != 0)) and (@as(c_int, afz_seqhdr_0.*.order_hint) != 0)) and (@as(c_int, @bitCast(afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1))) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 377;
                continue;
            },
            391 => {
                afz_hdr_1.*.switchable_motion_mode = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 390;
                continue;
            },
            392 => {
                afz_hdr_1.*.subpel_filter_mode = if (antfly_av1_dav1d_get_bit(gb) != 0) @as(c_uint, DAV1D_FILTER_SWITCHABLE) else antfly_av1_dav1d_get_bits(gb, 2);
                afz_pc = 391;
                continue;
            },
            393 => {
                afz_hdr_1.*.hp = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool(!(@as(c_int, afz_hdr_1.*.force_integer_mv) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 392;
                continue;
            },
            394 => {
                afz_pc = 2;
                continue;
            },
            395 => {
                afz_pc = @bitCast(@as(c_int, if (read_frame_size(c, gb, afz_use_ref_33) < @as(c_int, 0)) @as(c_int, 394) else @as(c_int, 393)));
                continue;
            },
            396 => {
                afz_use_ref_33 = @as(c_int, @intFromBool(!(@as(c_int, afz_hdr_1.*.error_resilient_mode) != 0) and (@as(c_int, afz_hdr_1.*.frame_size_override) != 0)));
                afz_pc = 395;
                continue;
            },
            397 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_29 < @as(c_int, 7)) @as(c_int, 406) else @as(c_int, 396)));
                continue;
            },
            398 => {
                afz_i_29 += 1;
                afz_pc = 397;
                continue;
            },
            399 => {
                afz_pc = 2;
                continue;
            },
            400 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ref_frame_hdr_32 != null) or (afz_ref_frame_hdr_32.*.frame_id != afz_ref_frame_id_31)) @as(c_int, 399) else @as(c_int, 398)));
                continue;
            },
            401 => {
                afz_ref_frame_hdr_32 = c.*.refs[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_29)))])))].p.p.frame_hdr;
                afz_pc = 400;
                continue;
            },
            402 => {
                afz_ref_frame_id_31 = ((afz_hdr_1.*.frame_id +% @as(u32, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, afz_seqhdr_0.*.frame_id_n_bits)))))) -% afz_delta_ref_frame_id_30) & @as(u32, @bitCast(@as(c_int, (@as(c_int, 1) << @intCast(@as(c_int, afz_seqhdr_0.*.frame_id_n_bits))) - @as(c_int, 1))));
                afz_pc = 401;
                continue;
            },
            403 => {
                afz_delta_ref_frame_id_30 = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.delta_frame_id_n_bits) +% @as(c_uint, 1);
                afz_pc = 402;
                continue;
            },
            404 => {
                afz_pc = @bitCast(@as(c_int, if (afz_seqhdr_0.*.frame_id_numbers_present != 0) @as(c_int, 403) else @as(c_int, 398)));
                continue;
            },
            405 => {
                afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_29)))] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_get_bits(gb, 3))));
                afz_pc = 404;
                continue;
            },
            406 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_hdr_1.*.frame_ref_short_signaling) != 0)) @as(c_int, 405) else @as(c_int, 404)));
                continue;
            },
            407 => {
                afz_i_29 = 0;
                afz_pc = 397;
                continue;
            },
            408 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_28 < @as(c_int, 7)) @as(c_int, 411) else @as(c_int, 407)));
                continue;
            },
            409 => {
                afz_i_28 += 1;
                afz_pc = 408;
                continue;
            },
            410 => {
                afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_28)))] = @truncate(afz_ref_25);
                afz_pc = 409;
                continue;
            },
            411 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_28)))]) < @as(c_int, 0)) @as(c_int, 410) else @as(c_int, 409)));
                continue;
            },
            412 => {
                afz_i_28 = 0;
                afz_pc = 408;
                continue;
            },
            413 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_26 < @as(c_int, 8)) @as(c_int, 418) else @as(c_int, 412)));
                continue;
            },
            414 => {
                afz_i_26 += 1;
                afz_pc = 413;
                continue;
            },
            415 => {
                afz_earliest_frame_offset_17 = afz_hint_27;
                afz_pc = 414;
                continue;
            },
            416 => {
                afz_ref_25 = afz_i_26;
                afz_pc = 415;
                continue;
            },
            417 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hint_27 < afz_earliest_frame_offset_17) @as(c_int, 416) else @as(c_int, 414)));
                continue;
            },
            418 => {
                afz_hint_27 = afz_shifted_frame_offset_10[@bitCast(@as(isize, @intCast(afz_i_26)))];
                afz_pc = 417;
                continue;
            },
            419 => {
                afz_i_26 = 0;
                afz_pc = 413;
                continue;
            },
            420 => {
                afz_ref_25 = -@as(c_int, 1);
                afz_pc = 419;
                continue;
            },
            421 => {
                afz_earliest_frame_offset_17 = 2147483647;
                afz_pc = 420;
                continue;
            },
            422 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_22 < @as(c_int, 7)) @as(c_int, 434) else @as(c_int, 421)));
                continue;
            },
            423 => {
                afz_i_22 += 1;
                afz_pc = 422;
                continue;
            },
            424 => {
                afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_22)))])))] = 1;
                afz_pc = 423;
                continue;
            },
            425 => {
                afz_pc = @bitCast(@as(c_int, if (afz_latest_frame_offset_14 != -@as(c_int, 1)) @as(c_int, 424) else @as(c_int, 423)));
                continue;
            },
            426 => {
                afz_pc = @bitCast(@as(c_int, if (afz_j_23 < @as(c_int, 8)) @as(c_int, 431) else @as(c_int, 425)));
                continue;
            },
            427 => {
                afz_j_23 += 1;
                afz_pc = 426;
                continue;
            },
            428 => {
                afz_latest_frame_offset_14 = afz_hint_24;
                afz_pc = 427;
                continue;
            },
            429 => {
                afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_22)))] = @truncate(afz_j_23);
                afz_pc = 428;
                continue;
            },
            430 => {
                afz_pc = @bitCast(@as(c_int, if ((!(afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_j_23)))] != 0) and (afz_hint_24 < afz_current_frame_offset_11)) and (afz_hint_24 >= afz_latest_frame_offset_14)) @as(c_int, 429) else @as(c_int, 427)));
                continue;
            },
            431 => {
                afz_hint_24 = afz_shifted_frame_offset_10[@bitCast(@as(isize, @intCast(afz_j_23)))];
                afz_pc = 430;
                continue;
            },
            432 => {
                afz_j_23 = 0;
                afz_pc = 426;
                continue;
            },
            433 => {
                afz_latest_frame_offset_14 = -@as(c_int, 1);
                afz_pc = 432;
                continue;
            },
            434 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_hdr_1.*.refidx[@bitCast(@as(isize, @intCast(afz_i_22)))]) < @as(c_int, 0)) @as(c_int, 433) else @as(c_int, 423)));
                continue;
            },
            435 => {
                afz_i_22 = 1;
                afz_pc = 422;
                continue;
            },
            436 => {
                afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@as(c_int, 5)])))] = 1;
                afz_pc = 435;
                continue;
            },
            437 => {
                afz_pc = @bitCast(@as(c_int, if (afz_earliest_frame_offset_17 != @as(c_int, 2147483647)) @as(c_int, 436) else @as(c_int, 435)));
                continue;
            },
            438 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_20 < @as(c_int, 8)) @as(c_int, 443) else @as(c_int, 437)));
                continue;
            },
            439 => {
                afz_i_20 += 1;
                afz_pc = 438;
                continue;
            },
            440 => {
                afz_earliest_frame_offset_17 = afz_hint_21;
                afz_pc = 439;
                continue;
            },
            441 => {
                afz_hdr_1.*.refidx[@as(c_int, 5)] = @truncate(afz_i_20);
                afz_pc = 440;
                continue;
            },
            442 => {
                afz_pc = @bitCast(@as(c_int, if ((!(afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_i_20)))] != 0) and (afz_hint_21 >= afz_current_frame_offset_11)) and (afz_hint_21 < afz_earliest_frame_offset_17)) @as(c_int, 441) else @as(c_int, 439)));
                continue;
            },
            443 => {
                afz_hint_21 = afz_shifted_frame_offset_10[@bitCast(@as(isize, @intCast(afz_i_20)))];
                afz_pc = 442;
                continue;
            },
            444 => {
                afz_i_20 = 0;
                afz_pc = 438;
                continue;
            },
            445 => {
                afz_earliest_frame_offset_17 = 2147483647;
                afz_pc = 444;
                continue;
            },
            446 => {
                afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@as(c_int, 4)])))] = 1;
                afz_pc = 445;
                continue;
            },
            447 => {
                afz_pc = @bitCast(@as(c_int, if (afz_earliest_frame_offset_17 != @as(c_int, 2147483647)) @as(c_int, 446) else @as(c_int, 445)));
                continue;
            },
            448 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_18 < @as(c_int, 8)) @as(c_int, 453) else @as(c_int, 447)));
                continue;
            },
            449 => {
                afz_i_18 += 1;
                afz_pc = 448;
                continue;
            },
            450 => {
                afz_earliest_frame_offset_17 = afz_hint_19;
                afz_pc = 449;
                continue;
            },
            451 => {
                afz_hdr_1.*.refidx[@as(c_int, 4)] = @truncate(afz_i_18);
                afz_pc = 450;
                continue;
            },
            452 => {
                afz_pc = @bitCast(@as(c_int, if ((!(afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_i_18)))] != 0) and (afz_hint_19 >= afz_current_frame_offset_11)) and (afz_hint_19 < afz_earliest_frame_offset_17)) @as(c_int, 451) else @as(c_int, 449)));
                continue;
            },
            453 => {
                afz_hint_19 = afz_shifted_frame_offset_10[@bitCast(@as(isize, @intCast(afz_i_18)))];
                afz_pc = 452;
                continue;
            },
            454 => {
                afz_i_18 = 0;
                afz_pc = 448;
                continue;
            },
            455 => {
                afz_earliest_frame_offset_17 = 2147483647;
                afz_pc = 454;
                continue;
            },
            456 => {
                afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@as(c_int, 6)])))] = 1;
                afz_pc = 455;
                continue;
            },
            457 => {
                afz_pc = @bitCast(@as(c_int, if (afz_latest_frame_offset_14 != -@as(c_int, 1)) @as(c_int, 456) else @as(c_int, 455)));
                continue;
            },
            458 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_15 < @as(c_int, 8)) @as(c_int, 463) else @as(c_int, 457)));
                continue;
            },
            459 => {
                afz_i_15 += 1;
                afz_pc = 458;
                continue;
            },
            460 => {
                afz_latest_frame_offset_14 = afz_hint_16;
                afz_pc = 459;
                continue;
            },
            461 => {
                afz_hdr_1.*.refidx[@as(c_int, 6)] = @truncate(afz_i_15);
                afz_pc = 460;
                continue;
            },
            462 => {
                afz_pc = @bitCast(@as(c_int, if ((!(afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_i_15)))] != 0) and (afz_hint_16 >= afz_current_frame_offset_11)) and (afz_hint_16 >= afz_latest_frame_offset_14)) @as(c_int, 461) else @as(c_int, 459)));
                continue;
            },
            463 => {
                afz_hint_16 = afz_shifted_frame_offset_10[@bitCast(@as(isize, @intCast(afz_i_15)))];
                afz_pc = 462;
                continue;
            },
            464 => {
                afz_i_15 = 0;
                afz_pc = 458;
                continue;
            },
            465 => {
                afz_latest_frame_offset_14 = -@as(c_int, 1);
                afz_pc = 464;
                continue;
            },
            466 => {
                afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@as(c_int, 3)])))] = 1;
                afz_pc = 465;
                continue;
            },
            467 => {
                afz_used_frame_13[@bitCast(@as(isize, @intCast(afz_hdr_1.*.refidx[@as(c_int, 0)])))] = 1;
                afz_pc = 466;
                continue;
            },
            468 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]c_int, @ptrCast(@alignCast(&afz_used_frame_13))))), @ptrCast(@alignCast(@as([*c]c_int, @ptrCast(@alignCast(@constCast(&@as([8]c_int, [1]c_int{
                    0,
                } ++ @as([7]c_int, @splat(0))))))))), @sizeOf(@TypeOf(afz_used_frame_13)));
                afz_pc = 467;
                continue;
            },
            469 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_12 < @as(c_int, 8)) @as(c_int, 473) else @as(c_int, 468)));
                continue;
            },
            470 => {
                afz_i_12 += 1;
                afz_pc = 469;
                continue;
            },
            471 => {
                afz_shifted_frame_offset_10[@bitCast(@as(isize, @intCast(afz_i_12)))] = afz_current_frame_offset_11 + get_poc_diff(afz_seqhdr_0.*.order_hint_n_bits, c.*.refs[@bitCast(@as(isize, @intCast(afz_i_12)))].p.p.frame_hdr.*.frame_offset, afz_hdr_1.*.frame_offset);
                afz_pc = 470;
                continue;
            },
            472 => {
                afz_pc = 2;
                continue;
            },
            473 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_i_12)))].p.p.frame_hdr != null)) @as(c_int, 472) else @as(c_int, 471)));
                continue;
            },
            474 => {
                afz_i_12 = 0;
                afz_pc = 469;
                continue;
            },
            475 => {
                afz_current_frame_offset_11 = @as(c_int, 1) << @intCast(@as(c_int, afz_seqhdr_0.*.order_hint_n_bits) - @as(c_int, 1));
                afz_pc = 474;
                continue;
            },
            476 => {
                afz_pc = 475;
                continue;
            },
            477 => {
                afz_hdr_1.*.refidx[@as(c_int, 4)] = blk: {
                    const tmp = blk_1: {
                        const tmp_2 = @as(i8, @truncate(-@as(c_int, 1)));
                        afz_hdr_1.*.refidx[@as(c_int, 6)] = tmp_2;
                        break :blk_1 tmp_2;
                    };
                    afz_hdr_1.*.refidx[@as(c_int, 5)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 476;
                continue;
            },
            478 => {
                afz_hdr_1.*.refidx[@as(c_int, 3)] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_get_bits(gb, 3))));
                afz_pc = 477;
                continue;
            },
            479 => {
                afz_hdr_1.*.refidx[@as(c_int, 1)] = blk: {
                    const tmp = @as(i8, @truncate(-@as(c_int, 1)));
                    afz_hdr_1.*.refidx[@as(c_int, 2)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 478;
                continue;
            },
            480 => {
                afz_hdr_1.*.refidx[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_get_bits(gb, 3))));
                afz_pc = 479;
                continue;
            },
            481 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.frame_ref_short_signaling != 0) @as(c_int, 480) else @as(c_int, 407)));
                continue;
            },
            482 => {
                afz_hdr_1.*.frame_ref_short_signaling = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @as(c_int, afz_seqhdr_0.*.order_hint)))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 481;
                continue;
            },
            483 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_9 < @as(c_int, 8)) @as(c_int, 485) else @as(c_int, 482)));
                continue;
            },
            484 => {
                afz_i_9 += 1;
                afz_pc = 483;
                continue;
            },
            485 => {
                _ = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.order_hint_n_bits);
                afz_pc = 484;
                continue;
            },
            486 => {
                afz_i_9 = 0;
                afz_pc = 483;
                continue;
            },
            487 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_hdr_1.*.error_resilient_mode) != 0) and (@as(c_int, afz_seqhdr_0.*.order_hint) != 0)) @as(c_int, 486) else @as(c_int, 482)));
                continue;
            },
            488 => {
                afz_hdr_1.*.refresh_frame_flags = @truncate(if (afz_hdr_1.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_SWITCH)) @as(c_uint, 255) else antfly_av1_dav1d_get_bits(gb, 8));
                afz_pc = 487;
                continue;
            },
            489 => {
                afz_hdr_1.*.allow_intrabc = 0;
                afz_pc = 488;
                continue;
            },
            490 => {
                afz_pc = @bitCast(@as(c_int, if (!((afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0)) @as(c_int, 389) else @as(c_int, 489)));
                continue;
            },
            491 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_3 < @as(c_int, c.*.seq_hdr.*.num_operating_points)) @as(c_int, 499) else @as(c_int, 490)));
                continue;
            },
            492 => {
                afz_i_3 += 1;
                afz_pc = 491;
                continue;
            },
            493 => {
                afz_op_5.*.buffer_removal_time = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.buffer_removal_delay_length);
                afz_pc = 492;
                continue;
            },
            494 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_seqop_4.*.idc) != 0) or ((afz_in_temporal_layer_6 != 0) and (afz_in_spatial_layer_7 != 0))) @as(c_int, 493) else @as(c_int, 492)));
                continue;
            },
            495 => {
                afz_in_spatial_layer_7 = (@as(c_int, afz_seqop_4.*.idc) >> @intCast(@as(c_int, afz_hdr_1.*.spatial_id) + @as(c_int, 8))) & @as(c_int, 1);
                afz_pc = 494;
                continue;
            },
            496 => {
                afz_in_temporal_layer_6 = (@as(c_int, afz_seqop_4.*.idc) >> @intCast(@as(c_int, afz_hdr_1.*.temporal_id))) & @as(c_int, 1);
                afz_pc = 495;
                continue;
            },
            497 => {
                afz_pc = @bitCast(@as(c_int, if (afz_seqop_4.*.decoder_model_param_present != 0) @as(c_int, 496) else @as(c_int, 492)));
                continue;
            },
            498 => {
                afz_op_5 = &afz_hdr_1.*.operating_points[@bitCast(@as(isize, @intCast(afz_i_3)))];
                afz_pc = 497;
                continue;
            },
            499 => {
                afz_seqop_4 = &afz_seqhdr_0.*.operating_points[@bitCast(@as(isize, @intCast(afz_i_3)))];
                afz_pc = 498;
                continue;
            },
            500 => {
                afz_i_3 = 0;
                afz_pc = 491;
                continue;
            },
            501 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.buffer_removal_time_present != 0) @as(c_int, 500) else @as(c_int, 490)));
                continue;
            },
            502 => {
                afz_hdr_1.*.buffer_removal_time_present = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 501;
                continue;
            },
            503 => {
                afz_pc = @bitCast(@as(c_int, if (afz_seqhdr_0.*.decoder_model_info_present != 0) @as(c_int, 502) else @as(c_int, 490)));
                continue;
            },
            504 => {
                afz_hdr_1.*.primary_ref_frame = @truncate(if (!(@as(c_int, afz_hdr_1.*.error_resilient_mode) != 0) and (@as(c_int, @bitCast(afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1))) != 0)) antfly_av1_dav1d_get_bits(gb, 3) else @as(c_uint, 7));
                afz_pc = 503;
                continue;
            },
            505 => {
                afz_hdr_1.*.frame_offset = @truncate(if (afz_seqhdr_0.*.order_hint != 0) antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.order_hint_n_bits) else @as(c_uint, 0));
                afz_pc = 504;
                continue;
            },
            506 => {
                afz_hdr_1.*.frame_size_override = @truncate(if (afz_seqhdr_0.*.reduced_still_picture_header != 0) @as(c_uint, 0) else if (afz_hdr_1.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_SWITCH)) @as(c_uint, 1) else antfly_av1_dav1d_get_bit(gb));
                afz_pc = 505;
                continue;
            },
            507 => {
                afz_hdr_1.*.frame_id = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.frame_id_n_bits);
                afz_pc = 506;
                continue;
            },
            508 => {
                afz_pc = @bitCast(@as(c_int, if (afz_seqhdr_0.*.frame_id_numbers_present != 0) @as(c_int, 507) else @as(c_int, 506)));
                continue;
            },
            509 => {
                afz_hdr_1.*.force_integer_mv = 1;
                afz_pc = 508;
                continue;
            },
            510 => {
                afz_pc = @bitCast(@as(c_int, if (!((afz_hdr_1.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0)) @as(c_int, 509) else @as(c_int, 508)));
                continue;
            },
            511 => {
                afz_hdr_1.*.force_integer_mv = @truncate(if (afz_seqhdr_0.*.force_integer_mv == @as(enum_Dav1dAdaptiveBoolean, DAV1D_ADAPTIVE)) antfly_av1_dav1d_get_bit(gb) else afz_seqhdr_0.*.force_integer_mv);
                afz_pc = 510;
                continue;
            },
            512 => {
                afz_hdr_1.*.force_integer_mv = 0;
                afz_pc = 510;
                continue;
            },
            513 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.allow_screen_content_tools != 0) @as(c_int, 511) else @as(c_int, 512)));
                continue;
            },
            514 => {
                afz_hdr_1.*.allow_screen_content_tools = @truncate(if (afz_seqhdr_0.*.screen_content_tools == @as(enum_Dav1dAdaptiveBoolean, DAV1D_ADAPTIVE)) antfly_av1_dav1d_get_bit(gb) else afz_seqhdr_0.*.screen_content_tools);
                afz_pc = 513;
                continue;
            },
            515 => {
                afz_hdr_1.*.disable_cdf_update = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 514;
                continue;
            },
            516 => {
                afz_hdr_1.*.error_resilient_mode = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool((((afz_hdr_1.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_KEY)) and (@as(c_int, afz_hdr_1.*.show_frame) != 0)) or (afz_hdr_1.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_SWITCH))) or (@as(c_int, afz_seqhdr_0.*.reduced_still_picture_header) != 0))))) != 0) or (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 515;
                continue;
            },
            517 => {
                afz_hdr_1.*.showable_frame = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(afz_hdr_1.*.frame_type != @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_KEY))))));
                afz_pc = 516;
                continue;
            },
            518 => {
                afz_hdr_1.*.frame_presentation_delay = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.frame_presentation_delay_length);
                afz_pc = 517;
                continue;
            },
            519 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_seqhdr_0.*.decoder_model_info_present) != 0) and !(@as(c_int, afz_seqhdr_0.*.equal_picture_interval) != 0)) @as(c_int, 518) else @as(c_int, 517)));
                continue;
            },
            520 => {
                afz_hdr_1.*.showable_frame = @truncate(antfly_av1_dav1d_get_bit(gb));
                afz_pc = 516;
                continue;
            },
            521 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.show_frame != 0) @as(c_int, 519) else @as(c_int, 520)));
                continue;
            },
            522 => {
                afz_hdr_1.*.show_frame = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @as(c_int, afz_seqhdr_0.*.reduced_still_picture_header)))) != 0) or (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 521;
                continue;
            },
            523 => {
                afz_hdr_1.*.frame_type = if (afz_seqhdr_0.*.reduced_still_picture_header != 0) @as(c_uint, DAV1D_FRAME_TYPE_KEY) else antfly_av1_dav1d_get_bits(gb, 2);
                afz_pc = 522;
                continue;
            },
            524 => {
                return 0;
            },
            525 => {
                afz_pc = 2;
                continue;
            },
            526 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ref_frame_hdr_2 != null) or (afz_ref_frame_hdr_2.*.frame_id != afz_hdr_1.*.frame_id)) @as(c_int, 525) else @as(c_int, 524)));
                continue;
            },
            527 => {
                afz_ref_frame_hdr_2 = c.*.refs[afz_hdr_1.*.existing_frame_idx].p.p.frame_hdr;
                afz_pc = 526;
                continue;
            },
            528 => {
                afz_hdr_1.*.frame_id = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.frame_id_n_bits);
                afz_pc = 527;
                continue;
            },
            529 => {
                afz_pc = @bitCast(@as(c_int, if (afz_seqhdr_0.*.frame_id_numbers_present != 0) @as(c_int, 528) else @as(c_int, 524)));
                continue;
            },
            530 => {
                afz_hdr_1.*.frame_presentation_delay = antfly_av1_dav1d_get_bits(gb, afz_seqhdr_0.*.frame_presentation_delay_length);
                afz_pc = 529;
                continue;
            },
            531 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_seqhdr_0.*.decoder_model_info_present) != 0) and !(@as(c_int, afz_seqhdr_0.*.equal_picture_interval) != 0)) @as(c_int, 530) else @as(c_int, 529)));
                continue;
            },
            532 => {
                afz_hdr_1.*.existing_frame_idx = @truncate(antfly_av1_dav1d_get_bits(gb, 3));
                afz_pc = 531;
                continue;
            },
            533 => {
                afz_pc = @bitCast(@as(c_int, if (afz_hdr_1.*.show_existing_frame != 0) @as(c_int, 532) else @as(c_int, 523)));
                continue;
            },
            534 => {
                afz_hdr_1.*.show_existing_frame = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @intFromBool(!(@as(c_int, afz_seqhdr_0.*.reduced_still_picture_header) != 0))))) != 0) and (antfly_av1_dav1d_get_bit(gb) != 0))))));
                afz_pc = 533;
                continue;
            },
            535 => {
                afz_hdr_1 = c.*.frame_hdr;
                afz_pc = 534;
                continue;
            },
            536 => {
                afz_seqhdr_0 = c.*.seq_hdr;
                afz_pc = 535;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub fn parse_tile_hdr(c: [*c]Dav1dContext, gb: [*c]GetBits) callconv(.c) void {
    _ = &c;
    _ = &gb;
    const n_tiles: c_int = @as(c_int, c.*.frame_hdr.*.tiling.cols) * @as(c_int, c.*.frame_hdr.*.tiling.rows);
    _ = &n_tiles;
    const have_tile_pos: c_int = @bitCast(@as(c_uint, @truncate(if (n_tiles > @as(c_int, 1)) antfly_av1_dav1d_get_bit(gb) else @as(c_uint, 0))));
    _ = &have_tile_pos;
    if (have_tile_pos != 0) {
        const n_bits: c_int = @as(c_int, c.*.frame_hdr.*.tiling.log2_cols) + @as(c_int, c.*.frame_hdr.*.tiling.log2_rows);
        _ = &n_bits;
        c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].start = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, n_bits))));
        c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].end = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(gb, n_bits))));
    } else {
        c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].start = 0;
        c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].end = n_tiles - @as(c_int, 1);
    }
}
pub export fn antfly_av1_dav1d_parse_obus(c: [*c]Dav1dContext, in: [*c]Dav1dData) ptrdiff_t {
    _ = &c;
    _ = &in;
    var afz_i_39: c_int = undefined;
    _ = &afz_i_39;
    var afz_i_38: c_int = undefined;
    _ = &afz_i_38;
    var afz_r_37: c_int = undefined;
    _ = &afz_r_37;
    var afz_progress_36: c_uint = undefined;
    _ = &afz_progress_36;
    var afz_error_35: c_int = undefined;
    _ = &afz_error_35;
    var afz_first_34: c_uint = undefined;
    _ = &afz_first_34;
    var afz_out_delayed_33: [*c]Dav1dThreadPicture = undefined;
    _ = &afz_out_delayed_33;
    var afz_f_32: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_32;
    var afz_next_31: c_uint = undefined;
    _ = &afz_next_31;
    var afz_itut_t35_metadata_30: [*c]Dav1dITUTT35 = undefined;
    _ = &afz_itut_t35_metadata_30;
    var afz_itut_t35_ctx_29: [*c]struct_itut_t35_ctx_context = undefined;
    _ = &afz_itut_t35_ctx_29;
    var afz_itut_t35_28: [*c]struct_Dav1dITUTT35 = undefined;
    _ = &afz_itut_t35_28;
    var afz_country_code_27: c_int = undefined;
    _ = &afz_country_code_27;
    var afz_country_code_extension_byte_26: c_int = undefined;
    _ = &afz_country_code_extension_byte_26;
    var afz_payload_size_25: ptrdiff_t = undefined;
    _ = &afz_payload_size_25;
    var afz_i_24: c_int = undefined;
    _ = &afz_i_24;
    var afz_mastering_display_23: [*c]Dav1dMasteringDisplay = undefined;
    _ = &afz_mastering_display_23;
    var afz_ref_22: [*c]Dav1dRef = undefined;
    _ = &afz_ref_22;
    var afz_content_light_21: [*c]Dav1dContentLightLevel = undefined;
    _ = &afz_content_light_21;
    var afz_ref_20: [*c]Dav1dRef = undefined;
    _ = &afz_ref_20;
    var afz_meta_type_19: enum_ObuMetaType = undefined;
    _ = &afz_meta_type_19;
    var afz_i_18: c_int = undefined;
    _ = &afz_i_18;
    var afz_tile_17: [*c]struct_Dav1dTileGroup = undefined;
    _ = &afz_tile_17;
    var afz_n_16: c_int = undefined;
    _ = &afz_n_16;
    var afz_i_15: c_int = undefined;
    _ = &afz_i_15;
    var afz_spatial_mask_14: c_uint = undefined;
    _ = &afz_spatial_mask_14;
    var afz_op_idx_13: c_int = undefined;
    _ = &afz_op_idx_13;
    var afz_seq_hdr_12: [*c]Dav1dSequenceHeader = undefined;
    _ = &afz_seq_hdr_12;
    var afz_ref_11: [*c]Dav1dRef = undefined;
    _ = &afz_ref_11;
    var afz_in_spatial_layer_10: c_int = undefined;
    _ = &afz_in_spatial_layer_10;
    var afz_in_temporal_layer_9: c_int = undefined;
    _ = &afz_in_temporal_layer_9;
    var afz_len_8: usize = undefined;
    _ = &afz_len_8;
    var afz_temporal_id_6: c_int = undefined;
    _ = &afz_temporal_id_6;
    var afz_spatial_id_7: c_int = undefined;
    _ = &afz_spatial_id_7;
    var afz_has_length_field_5: c_int = undefined;
    _ = &afz_has_length_field_5;
    var afz_has_extension_4: c_int = undefined;
    _ = &afz_has_extension_4;
    var afz_type_3: enum_Dav1dObuType = undefined;
    _ = &afz_type_3;
    var afz_obu_forbidden_bit_2: c_int = undefined;
    _ = &afz_obu_forbidden_bit_2;
    var afz_res_1: c_int = undefined;
    _ = &afz_res_1;
    var afz_gb_0: GetBits = undefined;
    _ = &afz_gb_0;
    var afz_pc: c_uint = 339;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                {
                    return -@as(c_int, 22);
                }
            },
            2 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 2) else @as(c_int, 1);
                continue;
            },
            3 => {
                antfly_av1_dav1d_data_props_copy(&c.*.cached_error_props, &in.*.m);
                afz_pc = 2;
                continue;
            },
            4 => {
                return @divExact(@as(c_long, @bitCast(@intFromPtr(afz_gb_0.ptr_end) -% @intFromPtr(afz_gb_0.ptr_start))), @sizeOf(u8));
            },
            5 => {
                c.*.n_tiles = 0;
                afz_pc = 4;
                continue;
            },
            6 => {
                c.*.frame_hdr = null;
                afz_pc = 5;
                continue;
            },
            7 => {
                antfly_av1_dav1d_ref_dec(&c.*.frame_hdr_ref);
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_39 < @as(c_int, 8)) @as(c_int, 17) else @as(c_int, 7)));
                continue;
            },
            9 => {
                afz_i_39 += 1;
                afz_pc = 8;
                continue;
            },
            10 => {
                dav1d_ref_inc(c.*.seq_hdr_ref);
                afz_pc = 9;
                continue;
            },
            11 => {
                dav1d_ref_inc(c.*.frame_hdr_ref);
                afz_pc = 10;
                continue;
            },
            12 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_39)))].p.p.seq_hdr_ref = c.*.seq_hdr_ref;
                afz_pc = 11;
                continue;
            },
            13 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_39)))].p.p.frame_hdr_ref = c.*.frame_hdr_ref;
                afz_pc = 12;
                continue;
            },
            14 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_39)))].p.p.seq_hdr = c.*.seq_hdr;
                afz_pc = 13;
                continue;
            },
            15 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_39)))].p.p.frame_hdr = c.*.frame_hdr;
                afz_pc = 14;
                continue;
            },
            16 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_39)))].p);
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, c.*.frame_hdr.*.refresh_frame_flags) & (@as(c_int, 1) << @intCast(afz_i_39))) != 0) @as(c_int, 16) else @as(c_int, 9)));
                continue;
            },
            18 => {
                afz_i_39 = 0;
                afz_pc = 8;
                continue;
            },
            19 => {
                return @divExact(@as(c_long, @bitCast(@intFromPtr(afz_gb_0.ptr_end) -% @intFromPtr(afz_gb_0.ptr_start))), @sizeOf(u8));
            },
            20 => {
                c.*.frame_hdr = null;
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_38 < @as(c_int, 8)) @as(c_int, 34) else @as(c_int, 20)));
                continue;
            },
            22 => {
                afz_i_38 += 1;
                afz_pc = 21;
                continue;
            },
            23 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_38)))].refmvs);
                afz_pc = 22;
                continue;
            },
            24 => {
                dav1d_ref_inc(c.*.refs[@bitCast(@as(isize, @intCast(afz_r_37)))].segmap);
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.refs[@bitCast(@as(isize, @intCast(afz_r_37)))].segmap != null) @as(c_int, 24) else @as(c_int, 23)));
                continue;
            },
            26 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_38)))].segmap = c.*.refs[@bitCast(@as(isize, @intCast(afz_r_37)))].segmap;
                afz_pc = 25;
                continue;
            },
            27 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_38)))].segmap);
                afz_pc = 26;
                continue;
            },
            28 => {
                antfly_av1_dav1d_cdf_thread_ref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_38)))], &c.*.cdf[@bitCast(@as(isize, @intCast(afz_r_37)))]);
                afz_pc = 27;
                continue;
            },
            29 => {
                antfly_av1_dav1d_cdf_thread_unref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_38)))]);
                afz_pc = 28;
                continue;
            },
            30 => {
                antfly_av1_dav1d_thread_picture_ref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_38)))].p, &c.*.refs[@bitCast(@as(isize, @intCast(afz_r_37)))].p);
                afz_pc = 29;
                continue;
            },
            31 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_38)))].p);
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.refs[@bitCast(@as(isize, @intCast(afz_i_38)))].p.p.frame_hdr != null) @as(c_int, 31) else @as(c_int, 30)));
                continue;
            },
            33 => {
                afz_pc = 22;
                continue;
            },
            34 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_38 == afz_r_37) @as(c_int, 33) else @as(c_int, 32)));
                continue;
            },
            35 => {
                afz_i_38 = 0;
                afz_pc = 21;
                continue;
            },
            36 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_r_37)))].p.showable = 0;
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_r_37 = c.*.frame_hdr.*.existing_frame_idx;
                afz_pc = 36;
                continue;
            },
            38 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p.p.frame_hdr.*.frame_type == @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_KEY)) @as(c_int, 37) else @as(c_int, 20)));
                continue;
            },
            39 => {
                c.*.event_flags |= antfly_av1_dav1d_picture_get_event_flags(&c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p);
                afz_pc = 38;
                continue;
            },
            40 => {
                c.*.n_itut_t35 = 0;
                afz_pc = 39;
                continue;
            },
            41 => {
                c.*.itut_t35 = null;
                afz_pc = 40;
                continue;
            },
            42 => {
                antfly_av1_dav1d_ref_dec(&c.*.itut_t35_ref);
                afz_pc = 41;
                continue;
            },
            43 => {
                antfly_av1_dav1d_picture_copy_props(&c.*.out.p, c.*.content_light, c.*.content_light_ref, c.*.mastering_display, c.*.mastering_display_ref, c.*.itut_t35, c.*.itut_t35_ref, @bitCast(@as(c_long, c.*.n_itut_t35)), &in.*.m);
                afz_pc = 42;
                continue;
            },
            44 => {
                antfly_av1_dav1d_thread_picture_ref(&c.*.out, &c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p);
                afz_pc = 43;
                continue;
            },
            45 => {
                _ = antfly_codec_afz_mutex_unlock(&c.*.task_thread.lock);
                afz_pc = 38;
                continue;
            },
            46 => {
                c.*.n_itut_t35 = 0;
                afz_pc = 45;
                continue;
            },
            47 => {
                c.*.itut_t35 = null;
                afz_pc = 46;
                continue;
            },
            48 => {
                antfly_av1_dav1d_ref_dec(&c.*.itut_t35_ref);
                afz_pc = 47;
                continue;
            },
            49 => {
                antfly_av1_dav1d_picture_copy_props(&afz_out_delayed_33.*.p, c.*.content_light, c.*.content_light_ref, c.*.mastering_display, c.*.mastering_display_ref, c.*.itut_t35, c.*.itut_t35_ref, @bitCast(@as(c_long, c.*.n_itut_t35)), &in.*.m);
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_out_delayed_33.*.visible = 1;
                afz_pc = 49;
                continue;
            },
            51 => {
                antfly_av1_dav1d_thread_picture_ref(afz_out_delayed_33, &c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p);
                afz_pc = 50;
                continue;
            },
            52 => {
                antfly_av1_dav1d_thread_picture_unref(afz_out_delayed_33);
                afz_pc = 51;
                continue;
            },
            53 => {
                antfly_av1_dav1d_data_props_copy(&c.*.cached_error_props, &afz_out_delayed_33.*.p.m);
                afz_pc = 52;
                continue;
            },
            54 => {
                afz_f_32.*.task_thread.retval = 0;
                afz_pc = 53;
                continue;
            },
            55 => {
                c.*.cached_error = afz_error_35;
                afz_pc = 54;
                continue;
            },
            56 => {
                antfly_av1_dav1d_thread_picture_unref(afz_out_delayed_33);
                afz_pc = 51;
                continue;
            },
            57 => {
                c.*.event_flags |= antfly_av1_dav1d_picture_get_event_flags(afz_out_delayed_33);
                afz_pc = 56;
                continue;
            },
            58 => {
                antfly_av1_dav1d_thread_picture_ref(&c.*.out, afz_out_delayed_33);
                afz_pc = 57;
                continue;
            },
            59 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_out_delayed_33.*.visible != 0) or (c.*.output_invisible_frames != 0)) and (afz_progress_36 != (((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))) @as(c_int, 58) else @as(c_int, 56)));
                continue;
            },
            60 => {
                afz_progress_36 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_out_delayed_33.*.progress[@as(c_int, 1)])))));
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_pc = @bitCast(@as(c_int, if (afz_out_delayed_33.*.p.data[@as(c_int, 0)] != null) @as(c_int, 60) else @as(c_int, 51)));
                continue;
            },
            62 => {
                afz_pc = @bitCast(@as(c_int, if (afz_error_35 != 0) @as(c_int, 55) else @as(c_int, 61)));
                continue;
            },
            63 => {
                afz_error_35 = afz_f_32.*.task_thread.retval;
                afz_pc = 62;
                continue;
            },
            64 => {
                c.*.task_thread.cur -%= 1;
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.task_thread.cur != 0) and (@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(c.*.task_thread.cur < c.*.n_fc))))) != 0)) @as(c_int, 64) else @as(c_int, 63)));
                continue;
            },
            66 => {
                _ = antfly_codec_afz_atomic_compare(@ptrCast(@alignCast(&c.*.task_thread.reset_task_cur)), @ptrCast(@alignCast(&afz_first_34)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))));
                afz_pc = 65;
                continue;
            },
            67 => {
                _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&c.*.task_thread.first)), 1);
                afz_pc = 66;
                continue;
            },
            68 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.task_thread.first)), 0);
                afz_pc = 66;
                continue;
            },
            69 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_first_34 +% @as(c_uint, 1)) < c.*.n_fc) @as(c_int, 67) else @as(c_int, 68)));
                continue;
            },
            70 => {
                afz_first_34 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&c.*.task_thread.first)))));
                afz_pc = 69;
                continue;
            },
            71 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_out_delayed_33.*.p.data[@as(c_int, 0)] != null) or (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_32.*.task_thread.@"error"))) != 0)) @as(c_int, 70) else @as(c_int, 63)));
                continue;
            },
            72 => {
                afz_out_delayed_33 = &c.*.frame_thread.out_delayed[afz_next_31];
                afz_pc = 71;
                continue;
            },
            73 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_32.*.n_tile_data > @as(c_int, 0)) @as(c_int, 74) else @as(c_int, 72)));
                continue;
            },
            74 => {
                _ = antfly_codec_afz_cond_wait(&afz_f_32.*.task_thread.cond, &afz_f_32.*.task_thread.ttd.*.lock);
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_f_32 = &c.*.fc[afz_next_31];
                afz_pc = 73;
                continue;
            },
            76 => {
                c.*.frame_thread.next = 0;
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.frame_thread.next == c.*.n_fc) @as(c_int, 76) else @as(c_int, 75)));
                continue;
            },
            78 => {
                afz_next_31 = blk: {
                    const ref = &c.*.frame_thread.next;
                    const tmp = ref.*;
                    ref.* +%= 1;
                    break :blk tmp;
                };
                afz_pc = 77;
                continue;
            },
            79 => {
                _ = antfly_codec_afz_mutex_lock(&c.*.task_thread.lock);
                afz_pc = 78;
                continue;
            },
            80 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_fc == @as(c_uint, 1)) @as(c_int, 44) else @as(c_int, 79)));
                continue;
            },
            81 => {
                afz_pc = 3;
                continue;
            },
            82 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.strict_std_compliance != 0) and !(c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p.showable != 0)) @as(c_int, 81) else @as(c_int, 80)));
                continue;
            },
            83 => {
                afz_pc = 3;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p.p.data[@as(c_int, 0)] != null)) @as(c_int, 83) else @as(c_int, 82)));
                continue;
            },
            85 => {
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_pc = 18;
                continue;
            },
            87 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.decode_frame_type > @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_INTRA)) @as(c_int, 86) else @as(c_int, 85)));
                continue;
            },
            88 => {
                afz_pc = 84;
                continue;
            },
            89 => {
                afz_pc = 18;
                continue;
            },
            90 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.decode_frame_type > @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_REFERENCE)) @as(c_int, 89) else @as(c_int, 88)));
                continue;
            },
            91 => {
                blk: {
                    switch (c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p.p.frame_hdr.*.frame_type) {
                        @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_INTRA) => {
                            afz_pc = 87;
                            continue;
                        },
                        @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_SWITCH) => {
                            afz_pc = 90;
                            continue;
                        },
                        @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_INTER) => {
                            afz_pc = 90;
                            continue;
                        },
                        else => {
                            afz_pc = 85;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            92 => {
                afz_pc = 3;
                continue;
            },
            93 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[c.*.frame_hdr.*.existing_frame_idx].p.p.frame_hdr != null)) @as(c_int, 92) else @as(c_int, 91)));
                continue;
            },
            94 => {
                c.*.n_tiles = 0;
                afz_pc = 19;
                continue;
            },
            95 => {
                c.*.frame_hdr = null;
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 98) else @as(c_int, 95);
                continue;
            },
            97 => {
                unreachable;
            },
            98 => {
                afz_pc = @bitCast(@as(c_int, if (!!(c.*.n_tile_data != 0)) @as(c_int, 97) else @as(c_int, 96)));
                continue;
            },
            99 => {
                return afz_res_1;
            },
            100 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const tmp = antfly_av1_dav1d_submit_frame(c);
                    afz_res_1 = tmp;
                    break :blk tmp;
                }) < @as(c_int, 0)) @as(c_int, 99) else @as(c_int, 98)));
                continue;
            },
            101 => {
                afz_pc = 3;
                continue;
            },
            102 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.n_tile_data != 0)) @as(c_int, 101) else @as(c_int, 100)));
                continue;
            },
            103 => {
                afz_pc = 102;
                continue;
            },
            104 => {
                afz_pc = 18;
                continue;
            },
            105 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.decode_frame_type > @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_INTRA)) or ((c.*.decode_frame_type == @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_REFERENCE)) and !(@as(c_int, c.*.frame_hdr.*.refresh_frame_flags) != 0))) @as(c_int, 104) else @as(c_int, 103)));
                continue;
            },
            106 => {
                afz_pc = 102;
                continue;
            },
            107 => {
                afz_pc = 18;
                continue;
            },
            108 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.decode_frame_type > @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_REFERENCE)) or ((c.*.decode_frame_type == @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_REFERENCE)) and !(@as(c_int, c.*.frame_hdr.*.refresh_frame_flags) != 0))) @as(c_int, 107) else @as(c_int, 106)));
                continue;
            },
            109 => {
                blk: {
                    switch (c.*.frame_hdr.*.frame_type) {
                        @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_INTRA) => {
                            afz_pc = 105;
                            continue;
                        },
                        @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_SWITCH) => {
                            afz_pc = 108;
                            continue;
                        },
                        @as(enum_Dav1dFrameType, DAV1D_FRAME_TYPE_INTER) => {
                            afz_pc = 108;
                            continue;
                        },
                        else => {
                            afz_pc = 103;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            110 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_tiles == (@as(c_int, c.*.frame_hdr.*.tiling.cols) * @as(c_int, c.*.frame_hdr.*.tiling.rows))) @as(c_int, 109) else @as(c_int, 19)));
                continue;
            },
            111 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.frame_hdr.*.show_existing_frame != 0) @as(c_int, 93) else @as(c_int, 110)));
                continue;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.seq_hdr != null) and (c.*.frame_hdr != null)) @as(c_int, 111) else @as(c_int, 19)));
                continue;
            },
            113 => {
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 114) else @as(c_int, 113);
                continue;
            },
            115 => {
                afz_pc = 112;
                continue;
            },
            116 => {
                afz_pc = 112;
                continue;
            },
            117 => {
                c.*.frame_flags |= PICTURE_FLAG_NEW_TEMPORAL_UNIT;
                afz_pc = 116;
                continue;
            },
            118 => {
                afz_pc = 112;
                continue;
            },
            119 => {
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 120) else @as(c_int, 119);
                continue;
            },
            121 => {
                afz_pc = @bitCast(@as(c_int, if (afz_meta_type_19 > @as(enum_ObuMetaType, 31)) @as(c_int, 120) else @as(c_int, 119)));
                continue;
            },
            122 => {
                afz_pc = 118;
                continue;
            },
            123 => {
                afz_pc = 118;
                continue;
            },
            124 => {
                c.*.n_itut_t35 += 1;
                afz_pc = 123;
                continue;
            },
            125 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(afz_itut_t35_metadata_30.*.payload)), @ptrCast(@alignCast(afz_gb_0.ptr)), @bitCast(@as(c_long, afz_payload_size_25)));
                afz_pc = 124;
                continue;
            },
            126 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 128) else @as(c_int, 125);
                continue;
            },
            127 => {
                unreachable;
            },
            128 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_gb_0.bits_left == @as(c_int, 0))) @as(c_int, 127) else @as(c_int, 126)));
                continue;
            },
            129 => {
                afz_itut_t35_metadata_30.*.payload_size = @bitCast(@as(c_long, afz_payload_size_25));
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_itut_t35_metadata_30.*.country_code_extension_byte = @bitCast(@as(i8, @truncate(afz_country_code_extension_byte_26)));
                afz_pc = 129;
                continue;
            },
            131 => {
                afz_itut_t35_metadata_30.*.country_code = @bitCast(@as(i8, @truncate(afz_country_code_27)));
                afz_pc = 130;
                continue;
            },
            132 => {
                afz_pc = 3;
                continue;
            },
            133 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_itut_t35_metadata_30.*.payload != null)) @as(c_int, 132) else @as(c_int, 131)));
                continue;
            },
            134 => {
                afz_itut_t35_metadata_30.*.payload = @ptrCast(@alignCast(antfly_codec_afz_malloc(@bitCast(@as(c_long, afz_payload_size_25)))));
                afz_pc = 133;
                continue;
            },
            135 => {
                afz_itut_t35_metadata_30 = &c.*.itut_t35[@bitCast(@as(isize, @intCast(c.*.n_itut_t35)))];
                afz_pc = 134;
                continue;
            },
            136 => {
                afz_itut_t35_ctx_29.*.n_itut_t35 = @bitCast(@as(c_long, c.*.n_itut_t35 + @as(c_int, 1)));
                afz_pc = 135;
                continue;
            },
            137 => {
                afz_itut_t35_ctx_29.*.itut_t35 = c.*.itut_t35;
                afz_pc = 136;
                continue;
            },
            138 => {
                c.*.itut_t35_ref = dav1d_ref_init(&afz_itut_t35_ctx_29.*.ref, @ptrCast(@alignCast(c.*.itut_t35)), antfly_av1_dav1d_picture_free_itut_t35, @ptrCast(@alignCast(afz_itut_t35_ctx_29)), 0);
                afz_pc = 137;
                continue;
            },
            139 => {
                afz_pc = 3;
                continue;
            },
            140 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_itut_t35_ctx_29 != null)) @as(c_int, 139) else @as(c_int, 138)));
                continue;
            },
            141 => {
                afz_itut_t35_ctx_29 = @ptrCast(@alignCast(antfly_codec_afz_malloc(@sizeOf(struct_itut_t35_ctx_context))));
                afz_pc = 140;
                continue;
            },
            142 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 144) else @as(c_int, 141);
                continue;
            },
            143 => {
                unreachable;
            },
            144 => {
                afz_pc = @bitCast(@as(c_int, if (!!(c.*.itut_t35_ref != null)) @as(c_int, 143) else @as(c_int, 142)));
                continue;
            },
            145 => {
                c.*.itut_t35_ref.*.const_data = @ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(c.*.itut_t35)))));
                afz_pc = 137;
                continue;
            },
            146 => {
                afz_itut_t35_ctx_29 = @ptrCast(@alignCast(c.*.itut_t35_ref.*.user_data));
                afz_pc = 145;
                continue;
            },
            147 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 149) else @as(c_int, 146);
                continue;
            },
            148 => {
                unreachable;
            },
            149 => {
                afz_pc = @bitCast(@as(c_int, if (!((c.*.itut_t35_ref != null) and (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&c.*.itut_t35_ref.*.ref_cnt))) == @as(c_int, 1)))) @as(c_int, 148) else @as(c_int, 147)));
                continue;
            },
            150 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.n_itut_t35 != 0)) @as(c_int, 144) else @as(c_int, 149)));
                continue;
            },
            151 => {
                afz_pc = 150;
                continue;
            },
            152 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(c.*.itut_t35 + @as(usize, @bitCast(@as(isize, @intCast(c.*.n_itut_t35)))))), 0, @sizeOf(@TypeOf(c.*.itut_t35.*)));
                afz_pc = 151;
                continue;
            },
            153 => {
                c.*.itut_t35 = afz_itut_t35_28;
                afz_pc = 152;
                continue;
            },
            154 => {
                afz_pc = 3;
                continue;
            },
            155 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_itut_t35_28 != null)) @as(c_int, 154) else @as(c_int, 153)));
                continue;
            },
            156 => {
                afz_itut_t35_28 = @ptrCast(@alignCast(antfly_codec_afz_realloc(@ptrCast(@alignCast(c.*.itut_t35)), @as(c_ulong, @bitCast(@as(c_long, c.*.n_itut_t35 + @as(c_int, 1)))) *% @sizeOf(@TypeOf(c.*.itut_t35.*)))));
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_pc = 3;
                continue;
            },
            158 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.n_itut_t35 + @as(c_int, 1)) > @divTrunc(@as(c_int, 2147483647), @as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(@TypeOf(c.*.itut_t35.*)))))))) @as(c_int, 157) else @as(c_int, 156)));
                continue;
            },
            159 => {
                afz_pc = 118;
                continue;
            },
            160 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 160) else @as(c_int, 159);
                continue;
            },
            161 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_payload_size_25 <= @as(ptrdiff_t, 0)) or (@as(c_int, afz_gb_0.ptr[@bitCast(@as(isize, @intCast(afz_payload_size_25)))]) != @as(c_int, 128))) @as(c_int, 160) else @as(c_int, 158)));
                continue;
            },
            162 => {
                afz_payload_size_25 -= 1;
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_country_code_extension_byte_26 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 8))));
                afz_pc = 162;
                continue;
            },
            164 => {
                afz_pc = @bitCast(@as(c_int, if (afz_country_code_27 == @as(c_int, 255)) @as(c_int, 163) else @as(c_int, 161)));
                continue;
            },
            165 => {
                afz_payload_size_25 -= 1;
                afz_pc = 164;
                continue;
            },
            166 => {
                afz_country_code_27 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 8))));
                afz_pc = 165;
                continue;
            },
            167 => {
                afz_country_code_extension_byte_26 = 0;
                afz_pc = 166;
                continue;
            },
            168 => {
                afz_payload_size_25 -= 1;
                afz_pc = 167;
                continue;
            },
            169 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_payload_size_25 > @as(ptrdiff_t, 0)) and !(@as(c_int, afz_gb_0.ptr[@bitCast(@as(isize, @intCast(afz_payload_size_25 - @as(ptrdiff_t, 1))))]) != 0)) @as(c_int, 170) else @as(c_int, 168)));
                continue;
            },
            170 => {
                afz_payload_size_25 -= 1;
                afz_pc = 169;
                continue;
            },
            171 => {
                afz_payload_size_25 = @divExact(@as(c_long, @bitCast(@intFromPtr(afz_gb_0.ptr_end) -% @intFromPtr(afz_gb_0.ptr))), @sizeOf(u8));
                afz_pc = 169;
                continue;
            },
            172 => {
                afz_pc = 118;
                continue;
            },
            173 => {
                c.*.mastering_display_ref = afz_ref_22;
                afz_pc = 172;
                continue;
            },
            174 => {
                c.*.mastering_display = afz_mastering_display_23;
                afz_pc = 173;
                continue;
            },
            175 => {
                antfly_av1_dav1d_ref_dec(&c.*.mastering_display_ref);
                afz_pc = 174;
                continue;
            },
            176 => {
                afz_pc = 3;
                continue;
            },
            177 => {
                antfly_av1_dav1d_ref_dec(&afz_ref_22);
                afz_pc = 176;
                continue;
            },
            178 => {
                afz_pc = @bitCast(@as(c_int, if (check_trailing_bits(&afz_gb_0, c.*.strict_std_compliance) < @as(c_int, 0)) @as(c_int, 177) else @as(c_int, 175)));
                continue;
            },
            179 => {
                afz_mastering_display_23.*.min_luminance = antfly_av1_dav1d_get_bits(&afz_gb_0, 32);
                afz_pc = 178;
                continue;
            },
            180 => {
                afz_mastering_display_23.*.max_luminance = antfly_av1_dav1d_get_bits(&afz_gb_0, 32);
                afz_pc = 179;
                continue;
            },
            181 => {
                afz_mastering_display_23.*.white_point[@as(c_int, 1)] = @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 16));
                afz_pc = 180;
                continue;
            },
            182 => {
                afz_mastering_display_23.*.white_point[@as(c_int, 0)] = @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 16));
                afz_pc = 181;
                continue;
            },
            183 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_24 < @as(c_int, 3)) @as(c_int, 186) else @as(c_int, 182)));
                continue;
            },
            184 => {
                afz_i_24 += 1;
                afz_pc = 183;
                continue;
            },
            185 => {
                afz_mastering_display_23.*.primaries[@bitCast(@as(isize, @intCast(afz_i_24)))][@as(c_int, 1)] = @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 16));
                afz_pc = 184;
                continue;
            },
            186 => {
                afz_mastering_display_23.*.primaries[@bitCast(@as(isize, @intCast(afz_i_24)))][@as(c_int, 0)] = @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 16));
                afz_pc = 185;
                continue;
            },
            187 => {
                afz_i_24 = 0;
                afz_pc = 183;
                continue;
            },
            188 => {
                afz_mastering_display_23 = @ptrCast(@alignCast(afz_ref_22.*.data));
                afz_pc = 187;
                continue;
            },
            189 => {
                return -@as(c_int, 12);
            },
            190 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ref_22 != null)) @as(c_int, 189) else @as(c_int, 188)));
                continue;
            },
            191 => {
                afz_ref_22 = antfly_av1_dav1d_ref_create(@sizeOf(Dav1dMasteringDisplay));
                afz_pc = 190;
                continue;
            },
            192 => {
                afz_pc = 118;
                continue;
            },
            193 => {
                c.*.content_light_ref = afz_ref_20;
                afz_pc = 192;
                continue;
            },
            194 => {
                c.*.content_light = afz_content_light_21;
                afz_pc = 193;
                continue;
            },
            195 => {
                antfly_av1_dav1d_ref_dec(&c.*.content_light_ref);
                afz_pc = 194;
                continue;
            },
            196 => {
                afz_pc = 3;
                continue;
            },
            197 => {
                antfly_av1_dav1d_ref_dec(&afz_ref_20);
                afz_pc = 196;
                continue;
            },
            198 => {
                afz_pc = @bitCast(@as(c_int, if (check_trailing_bits(&afz_gb_0, c.*.strict_std_compliance) < @as(c_int, 0)) @as(c_int, 197) else @as(c_int, 195)));
                continue;
            },
            199 => {
                afz_content_light_21.*.max_frame_average_light_level = @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 16));
                afz_pc = 198;
                continue;
            },
            200 => {
                afz_content_light_21.*.max_content_light_level = @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 16));
                afz_pc = 199;
                continue;
            },
            201 => {
                afz_content_light_21 = @ptrCast(@alignCast(afz_ref_20.*.data));
                afz_pc = 200;
                continue;
            },
            202 => {
                return -@as(c_int, 12);
            },
            203 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ref_20 != null)) @as(c_int, 202) else @as(c_int, 201)));
                continue;
            },
            204 => {
                afz_ref_20 = antfly_av1_dav1d_ref_create(@sizeOf(Dav1dContentLightLevel));
                afz_pc = 203;
                continue;
            },
            205 => {
                blk: {
                    switch (afz_meta_type_19) {
                        @as(enum_ObuMetaType, OBU_META_TIMECODE) => {
                            afz_pc = 122;
                            continue;
                        },
                        @as(enum_ObuMetaType, OBU_META_SCALABILITY) => {
                            afz_pc = 122;
                            continue;
                        },
                        @as(enum_ObuMetaType, OBU_META_ITUT_T35) => {
                            afz_pc = 171;
                            continue;
                        },
                        @as(enum_ObuMetaType, OBU_META_HDR_MDCV) => {
                            afz_pc = 191;
                            continue;
                        },
                        @as(enum_ObuMetaType, OBU_META_HDR_CLL) => {
                            afz_pc = 204;
                            continue;
                        },
                        else => {
                            afz_pc = 121;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            206 => {
                afz_pc = 3;
                continue;
            },
            207 => {
                afz_pc = @bitCast(@as(c_int, if (afz_gb_0.@"error" != 0) @as(c_int, 206) else @as(c_int, 205)));
                continue;
            },
            208 => {
                afz_meta_type_19 = antfly_av1_dav1d_get_uleb128(&afz_gb_0);
                afz_pc = 207;
                continue;
            },
            209 => {
                afz_pc = 112;
                continue;
            },
            210 => {
                c.*.n_tile_data += 1;
                afz_pc = 209;
                continue;
            },
            211 => {
                c.*.n_tiles += (@as(c_int, 1) + c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].end) - c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].start;
                afz_pc = 210;
                continue;
            },
            212 => {
                afz_pc = 3;
                continue;
            },
            213 => {
                c.*.n_tiles = 0;
                afz_pc = 212;
                continue;
            },
            214 => {
                c.*.n_tile_data = 0;
                afz_pc = 213;
                continue;
            },
            215 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_18 <= c.*.n_tile_data) @as(c_int, 217) else @as(c_int, 214)));
                continue;
            },
            216 => {
                afz_i_18 += 1;
                afz_pc = 215;
                continue;
            },
            217 => {
                antfly_av1_dav1d_data_unref_internal(&c.*.tile[@bitCast(@as(isize, @intCast(afz_i_18)))].data);
                afz_pc = 216;
                continue;
            },
            218 => {
                afz_i_18 = 0;
                afz_pc = 215;
                continue;
            },
            219 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].start > c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].end) or (c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].start != c.*.n_tiles)) @as(c_int, 218) else @as(c_int, 211)));
                continue;
            },
            220 => {
                c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].data.sz = @bitCast(@as(c_long, @divExact(@as(c_long, @bitCast(@intFromPtr(afz_gb_0.ptr_end) -% @intFromPtr(afz_gb_0.ptr))), @sizeOf(u8))));
                afz_pc = 219;
                continue;
            },
            221 => {
                c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].data.data = afz_gb_0.ptr;
                afz_pc = 220;
                continue;
            },
            222 => {
                antfly_av1_dav1d_data_ref(&c.*.tile[@bitCast(@as(isize, @intCast(c.*.n_tile_data)))].data, in);
                afz_pc = 221;
                continue;
            },
            223 => {
                afz_pc = 3;
                continue;
            },
            224 => {
                afz_pc = @bitCast(@as(c_int, if (afz_gb_0.@"error" != 0) @as(c_int, 223) else @as(c_int, 222)));
                continue;
            },
            225 => {
                dav1d_bytealign_get_bits(&afz_gb_0);
                afz_pc = 224;
                continue;
            },
            226 => {
                parse_tile_hdr(c, &afz_gb_0);
                afz_pc = 225;
                continue;
            },
            227 => {
                c.*.n_tile_data_alloc = c.*.n_tile_data + @as(c_int, 1);
                afz_pc = 226;
                continue;
            },
            228 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(c.*.tile + @as(usize, @bitCast(@as(isize, @intCast(c.*.n_tile_data)))))), 0, @sizeOf(@TypeOf(c.*.tile.*)));
                afz_pc = 227;
                continue;
            },
            229 => {
                c.*.tile = afz_tile_17;
                afz_pc = 228;
                continue;
            },
            230 => {
                afz_pc = 3;
                continue;
            },
            231 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_tile_17 != null)) @as(c_int, 230) else @as(c_int, 229)));
                continue;
            },
            232 => {
                afz_tile_17 = @ptrCast(@alignCast(antfly_codec_afz_realloc(@ptrCast(@alignCast(c.*.tile)), @as(c_ulong, @bitCast(@as(c_long, c.*.n_tile_data + @as(c_int, 1)))) *% @sizeOf(@TypeOf(c.*.tile.*)))));
                afz_pc = 231;
                continue;
            },
            233 => {
                afz_pc = 3;
                continue;
            },
            234 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.n_tile_data + @as(c_int, 1)) > @divTrunc(@as(c_int, 2147483647), @as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(@TypeOf(c.*.tile.*)))))))) @as(c_int, 233) else @as(c_int, 232)));
                continue;
            },
            235 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_tile_data_alloc < (c.*.n_tile_data + @as(c_int, 1))) @as(c_int, 234) else @as(c_int, 226)));
                continue;
            },
            236 => {
                afz_pc = 3;
                continue;
            },
            237 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.frame_hdr != null)) @as(c_int, 236) else @as(c_int, 235)));
                continue;
            },
            238 => {
                dav1d_bytealign_get_bits(&afz_gb_0);
                afz_pc = 237;
                continue;
            },
            239 => {
                afz_pc = 3;
                continue;
            },
            240 => {
                c.*.frame_hdr = null;
                afz_pc = 239;
                continue;
            },
            241 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.frame_hdr.*.show_existing_frame != 0) @as(c_int, 240) else @as(c_int, 238)));
                continue;
            },
            242 => {
                afz_pc = 112;
                continue;
            },
            243 => {
                afz_pc = @bitCast(@as(c_int, if (afz_type_3 != @as(enum_Dav1dObuType, DAV1D_OBU_FRAME)) @as(c_int, 242) else @as(c_int, 241)));
                continue;
            },
            244 => {
                return -@as(c_int, 34);
            },
            245 => {
                c.*.frame_hdr = null;
                afz_pc = 244;
                continue;
            },
            246 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 246) else @as(c_int, 245);
                continue;
            },
            247 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.frame_size_limit != 0) and (@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool((@as(i64, c.*.frame_hdr.*.width[@as(c_int, 1)]) * @as(i64, c.*.frame_hdr.*.height)) > @as(i64, c.*.frame_size_limit)))))) != 0)) @as(c_int, 246) else @as(c_int, 243)));
                continue;
            },
            248 => {
                afz_pc = 3;
                continue;
            },
            249 => {
                c.*.frame_hdr = null;
                afz_pc = 248;
                continue;
            },
            250 => {
                afz_pc = @bitCast(@as(c_int, if (check_trailing_bits(&afz_gb_0, c.*.strict_std_compliance) < @as(c_int, 0)) @as(c_int, 249) else @as(c_int, 247)));
                continue;
            },
            251 => {
                afz_pc = @bitCast(@as(c_int, if (afz_type_3 != @as(enum_Dav1dObuType, DAV1D_OBU_FRAME)) @as(c_int, 250) else @as(c_int, 247)));
                continue;
            },
            252 => {
                c.*.n_tiles = 0;
                afz_pc = 251;
                continue;
            },
            253 => {
                c.*.n_tile_data = 0;
                afz_pc = 252;
                continue;
            },
            254 => {
                afz_pc = @bitCast(@as(c_int, if (afz_n_16 < c.*.n_tile_data) @as(c_int, 256) else @as(c_int, 253)));
                continue;
            },
            255 => {
                afz_n_16 += 1;
                afz_pc = 254;
                continue;
            },
            256 => {
                antfly_av1_dav1d_data_unref_internal(&c.*.tile[@bitCast(@as(isize, @intCast(afz_n_16)))].data);
                afz_pc = 255;
                continue;
            },
            257 => {
                afz_n_16 = 0;
                afz_pc = 254;
                continue;
            },
            258 => {
                afz_pc = 3;
                continue;
            },
            259 => {
                c.*.frame_hdr = null;
                afz_pc = 258;
                continue;
            },
            260 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const tmp = parse_frame_hdr(c, &afz_gb_0);
                    afz_res_1 = tmp;
                    break :blk tmp;
                }) < @as(c_int, 0)) @as(c_int, 259) else @as(c_int, 257)));
                continue;
            },
            261 => {
                c.*.frame_hdr.*.spatial_id = @bitCast(@as(i8, @truncate(afz_spatial_id_7)));
                afz_pc = 260;
                continue;
            },
            262 => {
                c.*.frame_hdr.*.temporal_id = @bitCast(@as(i8, @truncate(afz_temporal_id_6)));
                afz_pc = 261;
                continue;
            },
            263 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(c.*.frame_hdr)), 0, @sizeOf(@TypeOf(c.*.frame_hdr.*)));
                afz_pc = 262;
                continue;
            },
            264 => {
                c.*.frame_hdr = @ptrCast(@alignCast(c.*.frame_hdr_ref.*.data));
                afz_pc = 263;
                continue;
            },
            265 => {
                return -@as(c_int, 12);
            },
            266 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.frame_hdr_ref != null)) @as(c_int, 265) else @as(c_int, 264)));
                continue;
            },
            267 => {
                c.*.frame_hdr_ref = antfly_av1_dav1d_ref_create_using_pool(c.*.frame_hdr_pool, @sizeOf(Dav1dFrameHeader));
                afz_pc = 266;
                continue;
            },
            268 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.frame_hdr_ref != null)) @as(c_int, 267) else @as(c_int, 264)));
                continue;
            },
            269 => {
                afz_pc = 3;
                continue;
            },
            270 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.seq_hdr != null)) @as(c_int, 269) else @as(c_int, 268)));
                continue;
            },
            271 => {
                afz_pc = 112;
                continue;
            },
            272 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.frame_hdr != null) @as(c_int, 271) else @as(c_int, 270)));
                continue;
            },
            273 => {
                afz_pc = 112;
                continue;
            },
            274 => {
                c.*.seq_hdr = afz_seq_hdr_12;
                afz_pc = 273;
                continue;
            },
            275 => {
                c.*.seq_hdr_ref = afz_ref_11;
                afz_pc = 274;
                continue;
            },
            276 => {
                antfly_av1_dav1d_ref_dec(&c.*.seq_hdr_ref);
                afz_pc = 275;
                continue;
            },
            277 => {
                c.*.frame_flags |= PICTURE_FLAG_NEW_SEQUENCE;
                afz_pc = 276;
                continue;
            },
            278 => {
                c.*.frame_hdr = null;
                afz_pc = 277;
                continue;
            },
            279 => {
                c.*.frame_flags |= PICTURE_FLAG_NEW_SEQUENCE;
                afz_pc = 276;
                continue;
            },
            280 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_15 < @as(c_int, 8)) @as(c_int, 286) else @as(c_int, 279)));
                continue;
            },
            281 => {
                afz_i_15 += 1;
                afz_pc = 280;
                continue;
            },
            282 => {
                antfly_av1_dav1d_cdf_thread_unref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_15)))]);
                afz_pc = 281;
                continue;
            },
            283 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_15)))].refmvs);
                afz_pc = 282;
                continue;
            },
            284 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_15)))].segmap);
                afz_pc = 283;
                continue;
            },
            285 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_15)))].p);
                afz_pc = 284;
                continue;
            },
            286 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.refs[@bitCast(@as(isize, @intCast(afz_i_15)))].p.p.frame_hdr != null) @as(c_int, 285) else @as(c_int, 284)));
                continue;
            },
            287 => {
                afz_i_15 = 0;
                afz_pc = 280;
                continue;
            },
            288 => {
                antfly_av1_dav1d_ref_dec(&c.*.content_light_ref);
                afz_pc = 287;
                continue;
            },
            289 => {
                antfly_av1_dav1d_ref_dec(&c.*.mastering_display_ref);
                afz_pc = 288;
                continue;
            },
            290 => {
                c.*.content_light = null;
                afz_pc = 289;
                continue;
            },
            291 => {
                c.*.mastering_display = null;
                afz_pc = 290;
                continue;
            },
            292 => {
                c.*.frame_hdr = null;
                afz_pc = 291;
                continue;
            },
            293 => {
                c.*.frame_flags |= PICTURE_FLAG_NEW_OP_PARAMS_INFO;
                afz_pc = 276;
                continue;
            },
            294 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_memcmp(@ptrCast(@alignCast(@as([*c]struct_Dav1dSequenceHeaderOperatingParameterInfo_4, @ptrCast(@alignCast(&afz_seq_hdr_12.*.operating_parameter_info))))), @ptrCast(@alignCast(@as([*c]struct_Dav1dSequenceHeaderOperatingParameterInfo_4, @ptrCast(@alignCast(&c.*.seq_hdr.*.operating_parameter_info))))), @sizeOf(@TypeOf(afz_seq_hdr_12.*.operating_parameter_info))) != 0) @as(c_int, 293) else @as(c_int, 276)));
                continue;
            },
            295 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_memcmp(@ptrCast(@alignCast(afz_seq_hdr_12)), @ptrCast(@alignCast(c.*.seq_hdr)), @intFromPtr(&@as(*allowzero Dav1dSequenceHeader, @ptrFromInt(0)).operating_parameter_info)) != 0) @as(c_int, 292) else @as(c_int, 294)));
                continue;
            },
            296 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.seq_hdr != null)) @as(c_int, 278) else @as(c_int, 295)));
                continue;
            },
            297 => {
                c.*.max_spatial_id = if (afz_spatial_mask_14 != 0) ulog2(afz_spatial_mask_14) else @as(c_int, 0);
                afz_pc = 296;
                continue;
            },
            298 => {
                afz_spatial_mask_14 = c.*.operating_point_idc >> @intCast(@as(c_uint, 8));
                afz_pc = 297;
                continue;
            },
            299 => {
                c.*.operating_point_idc = afz_seq_hdr_12.*.operating_points[@bitCast(@as(isize, @intCast(afz_op_idx_13)))].idc;
                afz_pc = 298;
                continue;
            },
            300 => {
                afz_op_idx_13 = if (c.*.operating_point < @as(c_int, afz_seq_hdr_12.*.num_operating_points)) c.*.operating_point else @as(c_int, 0);
                afz_pc = 299;
                continue;
            },
            301 => {
                afz_pc = 3;
                continue;
            },
            302 => {
                antfly_av1_dav1d_ref_dec(&afz_ref_11);
                afz_pc = 301;
                continue;
            },
            303 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 303) else @as(c_int, 302);
                continue;
            },
            304 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const tmp = parse_seq_hdr(afz_seq_hdr_12, &afz_gb_0, c.*.strict_std_compliance);
                    afz_res_1 = tmp;
                    break :blk tmp;
                }) < @as(c_int, 0)) @as(c_int, 303) else @as(c_int, 300)));
                continue;
            },
            305 => {
                afz_seq_hdr_12 = @ptrCast(@alignCast(afz_ref_11.*.data));
                afz_pc = 304;
                continue;
            },
            306 => {
                return -@as(c_int, 12);
            },
            307 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ref_11 != null)) @as(c_int, 306) else @as(c_int, 305)));
                continue;
            },
            308 => {
                afz_ref_11 = antfly_av1_dav1d_ref_create_using_pool(c.*.seq_hdr_pool, @sizeOf(Dav1dSequenceHeader));
                afz_pc = 307;
                continue;
            },
            309 => {
                blk: {
                    switch (afz_type_3) {
                        @as(enum_Dav1dObuType, DAV1D_OBU_PADDING) => {
                            afz_pc = 115;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_TD) => {
                            afz_pc = 117;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_METADATA) => {
                            afz_pc = 208;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_TILE_GRP) => {
                            afz_pc = 237;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_FRAME_HDR) => {
                            afz_pc = 270;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_FRAME) => {
                            afz_pc = 270;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_REDUNDANT_FRAME_HDR) => {
                            afz_pc = 272;
                            continue;
                        },
                        @as(enum_Dav1dObuType, DAV1D_OBU_SEQ_HDR) => {
                            afz_pc = 308;
                            continue;
                        },
                        else => {
                            afz_pc = 114;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            310 => {
                return @divExact(@as(c_long, @bitCast(@intFromPtr(afz_gb_0.ptr_end) -% @intFromPtr(afz_gb_0.ptr_start))), @sizeOf(u8));
            },
            311 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_in_temporal_layer_9 != 0) or !(afz_in_spatial_layer_10 != 0)) @as(c_int, 310) else @as(c_int, 309)));
                continue;
            },
            312 => {
                afz_in_spatial_layer_10 = @bitCast(@as(c_uint, @truncate((c.*.operating_point_idc >> @intCast(@as(c_uint, @bitCast(@as(c_int, afz_spatial_id_7 + @as(c_int, 8)))))) & @as(c_uint, 1))));
                afz_pc = 311;
                continue;
            },
            313 => {
                afz_in_temporal_layer_9 = @bitCast(@as(c_uint, @truncate((c.*.operating_point_idc >> @intCast(@as(c_uint, @bitCast(@as(c_int, afz_temporal_id_6))))) & @as(c_uint, 1))));
                afz_pc = 312;
                continue;
            },
            314 => {
                afz_pc = @bitCast(@as(c_int, if ((((afz_type_3 != @as(enum_Dav1dObuType, DAV1D_OBU_SEQ_HDR)) and (afz_type_3 != @as(enum_Dav1dObuType, DAV1D_OBU_TD))) and (afz_has_extension_4 != 0)) and (c.*.operating_point_idc != @as(c_uint, 0))) @as(c_int, 313) else @as(c_int, 309)));
                continue;
            },
            315 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 317) else @as(c_int, 314);
                continue;
            },
            316 => {
                unreachable;
            },
            317 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_gb_0.bits_left == @as(c_int, 0))) @as(c_int, 316) else @as(c_int, 315)));
                continue;
            },
            318 => {
                afz_pc = 3;
                continue;
            },
            319 => {
                afz_pc = @bitCast(@as(c_int, if (afz_gb_0.@"error" != 0) @as(c_int, 318) else @as(c_int, 317)));
                continue;
            },
            320 => {
                afz_gb_0.ptr_end = afz_gb_0.ptr + afz_len_8;
                afz_pc = 319;
                continue;
            },
            321 => {
                afz_pc = 3;
                continue;
            },
            322 => {
                afz_pc = @bitCast(@as(c_int, if (afz_len_8 > @as(usize, @bitCast(@as(c_long, @divExact(@as(c_long, @bitCast(@intFromPtr(afz_gb_0.ptr_end) -% @intFromPtr(afz_gb_0.ptr))), @sizeOf(u8)))))) @as(c_int, 321) else @as(c_int, 320)));
                continue;
            },
            323 => {
                afz_len_8 = antfly_av1_dav1d_get_uleb128(&afz_gb_0);
                afz_pc = 322;
                continue;
            },
            324 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_length_field_5 != 0) @as(c_int, 323) else @as(c_int, 319)));
                continue;
            },
            325 => {
                _ = antfly_av1_dav1d_get_bits(&afz_gb_0, 3);
                afz_pc = 324;
                continue;
            },
            326 => {
                afz_spatial_id_7 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 2))));
                afz_pc = 325;
                continue;
            },
            327 => {
                afz_temporal_id_6 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(&afz_gb_0, 3))));
                afz_pc = 326;
                continue;
            },
            328 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_extension_4 != 0) @as(c_int, 327) else @as(c_int, 324)));
                continue;
            },
            329 => {
                afz_temporal_id_6 = 0;
                afz_spatial_id_7 = 0;
                afz_pc = 328;
                continue;
            },
            330 => {
                _ = antfly_av1_dav1d_get_bit(&afz_gb_0);
                afz_pc = 329;
                continue;
            },
            331 => {
                afz_has_length_field_5 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bit(&afz_gb_0))));
                afz_pc = 330;
                continue;
            },
            332 => {
                afz_has_extension_4 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bit(&afz_gb_0))));
                afz_pc = 331;
                continue;
            },
            333 => {
                afz_type_3 = antfly_av1_dav1d_get_bits(&afz_gb_0, 4);
                afz_pc = 332;
                continue;
            },
            334 => {
                afz_pc = 3;
                continue;
            },
            335 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.strict_std_compliance != 0) and (afz_obu_forbidden_bit_2 != 0)) @as(c_int, 334) else @as(c_int, 333)));
                continue;
            },
            336 => {
                afz_obu_forbidden_bit_2 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bit(&afz_gb_0))));
                afz_pc = 335;
                continue;
            },
            337 => {
                antfly_av1_dav1d_init_get_bits(&afz_gb_0, in.*.data, in.*.sz);
                afz_pc = 336;
                continue;
            },
            338 => {
                afz_pc = 337;
                continue;
            },
            339 => {
                afz_pc = 338;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
