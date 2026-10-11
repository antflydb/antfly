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
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
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
pub fn ctz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.ctz(mask);
}
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
pub fn apply_sign(v: c_int, s: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &s;
    return if (s < @as(c_int, 0)) -v else v;
}
pub fn apply_sign64(v: c_int, s: i64) callconv(.c) c_int {
    _ = &v;
    _ = &s;
    return if (s < @as(i64, 0)) -v else v;
}
pub fn ulog2(v: c_uint) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 31) ^ clz(v);
}
pub const union_alias64 = common.union_alias64;
pub const union_alias32 = common.union_alias32;
pub const union_alias16 = common.union_alias16;
pub const union_alias8 = common.union_alias8;
pub const dav1d_memset_pow2_fn = common.dav1d_memset_pow2_fn;
pub extern const antfly_av1_dav1d_memset_pow2: [6]dav1d_memset_pow2_fn;
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
pub const enum_Dav1dColorPrimaries = common.enum_Dav1dColorPrimaries;
pub const enum_Dav1dTransferCharacteristics = common.enum_Dav1dTransferCharacteristics;
pub const enum_Dav1dMatrixCoefficients = common.enum_Dav1dMatrixCoefficients;
pub const enum_Dav1dChromaSamplePosition = common.enum_Dav1dChromaSamplePosition;
pub const struct_Dav1dSequenceHeaderOperatingPoint_2 = common.struct_Dav1dSequenceHeaderOperatingPoint_2;
pub const enum_Dav1dAdaptiveBoolean = common.enum_Dav1dAdaptiveBoolean;
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_3 = common.struct_Dav1dSequenceHeaderOperatingParameterInfo_3;
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
    operating_points: [32]struct_Dav1dSequenceHeaderOperatingPoint_2 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingPoint_2),
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
    operating_parameter_info: [32]struct_Dav1dSequenceHeaderOperatingParameterInfo_3 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingParameterInfo_3),
};
pub const Dav1dSequenceHeader = struct_Dav1dSequenceHeader;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
const struct_unnamed_4 = extern struct {
    data: Dav1dFilmGrainData = @import("std").mem.zeroes(Dav1dFilmGrainData),
    present: u8 = 0,
    update: u8 = 0,
};
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const struct_Dav1dFrameHeaderOperatingPoint_5 = common.struct_Dav1dFrameHeaderOperatingPoint_5;
const struct_unnamed_6 = extern struct {
    width_scale_denominator: u8 = 0,
    enabled: u8 = 0,
};
pub const DAV1D_FILTER_8TAP_REGULAR = common.DAV1D_FILTER_8TAP_REGULAR;
pub const DAV1D_N_SWITCHABLE_FILTERS = common.DAV1D_N_SWITCHABLE_FILTERS;
pub const DAV1D_FILTER_SWITCHABLE = common.DAV1D_FILTER_SWITCHABLE;
pub const enum_Dav1dFilterMode = common.enum_Dav1dFilterMode;
const struct_unnamed_7 = extern struct {
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
const struct_unnamed_8 = extern struct {
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
const struct_unnamed_9 = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    temporal: u8 = 0,
    update_data: u8 = 0,
    seg_data: Dav1dSegmentationDataSet = @import("std").mem.zeroes(Dav1dSegmentationDataSet),
    lossless: [8]u8 = @import("std").mem.zeroes([8]u8),
    qidx: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_11 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
};
const struct_unnamed_12 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
    multi: u8 = 0,
};
const struct_unnamed_10 = extern struct {
    q: struct_unnamed_11 = @import("std").mem.zeroes(struct_unnamed_11),
    lf: struct_unnamed_12 = @import("std").mem.zeroes(struct_unnamed_12),
};
pub const Dav1dLoopfilterModeRefDeltas = common.Dav1dLoopfilterModeRefDeltas;
const struct_unnamed_13 = extern struct {
    level_y: [2]u8 = @import("std").mem.zeroes([2]u8),
    level_u: u8 = 0,
    level_v: u8 = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    mode_ref_deltas: Dav1dLoopfilterModeRefDeltas = @import("std").mem.zeroes(Dav1dLoopfilterModeRefDeltas),
    sharpness: u8 = 0,
};
const struct_unnamed_14 = extern struct {
    damping: u8 = 0,
    n_bits: u8 = 0,
    y_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
    uv_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
};
pub const DAV1D_RESTORATION_NONE = common.DAV1D_RESTORATION_NONE;
pub const DAV1D_RESTORATION_SWITCHABLE = common.DAV1D_RESTORATION_SWITCHABLE;
pub const DAV1D_RESTORATION_WIENER = common.DAV1D_RESTORATION_WIENER;
pub const DAV1D_RESTORATION_SGRPROJ = common.DAV1D_RESTORATION_SGRPROJ;
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
const struct_unnamed_15 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const DAV1D_TX_SWITCHABLE = common.DAV1D_TX_SWITCHABLE;
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
pub const DAV1D_WM_TYPE_IDENTITY = common.DAV1D_WM_TYPE_IDENTITY;
pub const DAV1D_WM_TYPE_TRANSLATION = common.DAV1D_WM_TYPE_TRANSLATION;
pub const DAV1D_WM_TYPE_ROT_ZOOM = common.DAV1D_WM_TYPE_ROT_ZOOM;
pub const DAV1D_WM_TYPE_AFFINE = common.DAV1D_WM_TYPE_AFFINE;
pub const enum_Dav1dWarpedMotionType = common.enum_Dav1dWarpedMotionType;
const struct_unnamed_17 = extern struct {
    alpha: i16 = 0,
    beta: i16 = 0,
    gamma: i16 = 0,
    delta: i16 = 0,
};
const union_unnamed_16 = extern union {
    p: struct_unnamed_17,
    abcd: [4]i16,
};
pub const struct_Dav1dWarpedMotionParams = extern struct {
    type: enum_Dav1dWarpedMotionType = @import("std").mem.zeroes(enum_Dav1dWarpedMotionType),
    matrix: [6]i32 = @import("std").mem.zeroes([6]i32),
    u: union_unnamed_16 = @import("std").mem.zeroes(union_unnamed_16),
};
pub const Dav1dWarpedMotionParams = struct_Dav1dWarpedMotionParams;
pub const struct_Dav1dFrameHeader = extern struct {
    film_grain: struct_unnamed_4 = @import("std").mem.zeroes(struct_unnamed_4),
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
    operating_points: [32]struct_Dav1dFrameHeaderOperatingPoint_5 = @import("std").mem.zeroes([32]struct_Dav1dFrameHeaderOperatingPoint_5),
    refresh_frame_flags: u8 = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    super_res: struct_unnamed_6 = @import("std").mem.zeroes(struct_unnamed_6),
    have_render_size: u8 = 0,
    allow_intrabc: u8 = 0,
    frame_ref_short_signaling: u8 = 0,
    refidx: [7]i8 = @import("std").mem.zeroes([7]i8),
    hp: u8 = 0,
    subpel_filter_mode: enum_Dav1dFilterMode = @import("std").mem.zeroes(enum_Dav1dFilterMode),
    switchable_motion_mode: u8 = 0,
    use_ref_frame_mvs: u8 = 0,
    refresh_context: u8 = 0,
    tiling: struct_unnamed_7 = @import("std").mem.zeroes(struct_unnamed_7),
    quant: struct_unnamed_8 = @import("std").mem.zeroes(struct_unnamed_8),
    segmentation: struct_unnamed_9 = @import("std").mem.zeroes(struct_unnamed_9),
    delta: struct_unnamed_10 = @import("std").mem.zeroes(struct_unnamed_10),
    all_lossless: u8 = 0,
    loopfilter: struct_unnamed_13 = @import("std").mem.zeroes(struct_unnamed_13),
    cdef: struct_unnamed_14 = @import("std").mem.zeroes(struct_unnamed_14),
    restoration: struct_unnamed_15 = @import("std").mem.zeroes(struct_unnamed_15),
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
pub const enum_PictureFlags = common.enum_PictureFlags;
pub const struct_Dav1dThreadPicture = extern struct {
    p: Dav1dPicture = @import("std").mem.zeroes(Dav1dPicture),
    visible: c_int = 0,
    showable: c_int = 0,
    flags: enum_PictureFlags = @import("std").mem.zeroes(enum_PictureFlags),
    progress: [*c]atomic_uint = null,
};
pub const Dav1dThreadPicture = struct_Dav1dThreadPicture;
const struct_unnamed_18 = extern struct {
    y: i16 = 0,
    x: i16 = 0,
};
pub const union_mv = extern union {
    unnamed_0: struct_unnamed_18,
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
pub const CdfMvComponent = common.CdfMvComponent;
pub const CdfMvContext = common.CdfMvContext;
pub const struct_CdfContext = extern struct {
    coef: CdfCoefContext = @import("std").mem.zeroes(CdfCoefContext),
    m: CdfModeContext = @import("std").mem.zeroes(CdfModeContext),
    mv: CdfMvContext = @import("std").mem.zeroes(CdfMvContext),
    kfym: [5][5][16]u16 = @import("std").mem.zeroes([5][5][16]u16),
};
pub const CdfContext = struct_CdfContext;
const union_unnamed_19 = extern union {
    cdf: [*c]CdfContext,
    qcat: c_uint,
};
pub const struct_CdfThreadContext = extern struct {
    ref: [*c]Dav1dRef = null,
    data: union_unnamed_19 = @import("std").mem.zeroes(union_unnamed_19),
    progress: [*c]atomic_uint = null,
};
pub const CdfThreadContext = struct_CdfThreadContext;
pub const struct_Dav1dTileGroup = extern struct {
    data: Dav1dData = @import("std").mem.zeroes(Dav1dData),
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_ScalableMotionParams_20 = common.struct_ScalableMotionParams_20;
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
const struct_unnamed_21 = extern struct {
    col_start: c_int = 0,
    col_end: c_int = 0,
    row_start: c_int = 0,
    row_end: c_int = 0,
    col: c_int = 0,
    row: c_int = 0,
};
pub const coef = anyopaque;
const struct_unnamed_22 = extern struct {
    pal_idx: [*c]u8 = null,
    cbi: [*c]i16 = null,
    cf: ?*coef = null,
};
const union_unnamed_23 = extern union {
    i8: [4]i8,
    u32: u32,
};
pub const Av1RestorationUnit = common.Av1RestorationUnit;
pub const struct_Dav1dTileState = extern struct {
    cdf: CdfContext = @import("std").mem.zeroes(CdfContext),
    msac: MsacContext = @import("std").mem.zeroes(MsacContext),
    tiling: struct_unnamed_21 = @import("std").mem.zeroes(struct_unnamed_21),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    frame_thread: [2]struct_unnamed_22 = @import("std").mem.zeroes([2]struct_unnamed_22),
    lowest_pixel: [*c][7][2]c_int = null,
    dqmem: [8][3][2]u16 = @import("std").mem.zeroes([8][3][2]u16),
    dq: [*c][3][2]u16 = null,
    last_qidx: c_int = 0,
    last_delta_lf: union_unnamed_23 = @import("std").mem.zeroes(union_unnamed_23),
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
pub const union_refmvs_refpair = common.union_refmvs_refpair;
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
const struct_unnamed_24 = extern struct {
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_refmvs_tile = extern struct {
    rf: [*c]const refmvs_frame = null,
    r: [37][*c]refmvs_block = @import("std").mem.zeroes([37][*c]refmvs_block),
    rp_proj: [*c]refmvs_temporal_block = null,
    tile_col: struct_unnamed_24 = @import("std").mem.zeroes(struct_unnamed_24),
    tile_row: struct_unnamed_24 = @import("std").mem.zeroes(struct_unnamed_24),
};
pub const refmvs_tile = struct_refmvs_tile;
const union_unnamed_25 = extern union {
    cf_8bpc: [1024]i16 align(64),
    cf_16bpc: [1024]i32 align(64),
};
const union_unnamed_26 = extern union {
    al_pal_8bpc: [2][32][3][8]u8,
    al_pal_16bpc: [2][32][3][8]u16,
};
const struct_unnamed_30 = extern struct {
    compinter: [2][16384]i16 = @import("std").mem.zeroes([2][16384]i16),
    seg_mask: [16384]u8 = @import("std").mem.zeroes([16384]u8),
};
const union_unnamed_29 = extern union {
    lap_8bpc: [4096]u8,
    lap_16bpc: [4096]u16,
    unnamed_0: struct_unnamed_30,
};
const union_unnamed_31 = extern union {
    emu_edge_8bpc: [84160]u8,
    emu_edge_16bpc: [84160]u16,
};
const struct_unnamed_28 = extern struct {
    unnamed_0: union_unnamed_29 = @import("std").mem.zeroes(union_unnamed_29),
    unnamed_1: union_unnamed_31 = @import("std").mem.zeroes(union_unnamed_31),
};
const struct_unnamed_34 = extern struct {
    pal_order: [64][8]u8 = @import("std").mem.zeroes([64][8]u8),
    pal_ctx: [64]u8 = @import("std").mem.zeroes([64]u8),
};
const union_unnamed_33 = extern union {
    levels: [1088]u8,
    unnamed_0: struct_unnamed_34,
};
const union_unnamed_35 = extern union {
    ac: [1024]i16,
    txtp_map: [1024]u8,
};
const struct_unnamed_37 = extern struct {
    interintra_8bpc: [4096]u8 align(8) = @import("std").mem.zeroes([4096]u8),
    edge_8bpc: [257]u8 = @import("std").mem.zeroes([257]u8),
    pal_8bpc: [3][8]u8 align(8) = @import("std").mem.zeroes([3][8]u8),
};
const struct_unnamed_38 = extern struct {
    interintra_16bpc: [4096]u16 align(16) = @import("std").mem.zeroes([4096]u16),
    edge_16bpc: [257]u16 = @import("std").mem.zeroes([257]u16),
    pal_16bpc: [3][8]u16 align(16) = @import("std").mem.zeroes([3][8]u16),
};
const union_unnamed_36 = extern union {
    unnamed_0: struct_unnamed_37,
    unnamed_1: struct_unnamed_38,
};
const struct_unnamed_32 = extern struct {
    unnamed_0: union_unnamed_33 = @import("std").mem.zeroes(union_unnamed_33),
    unnamed_1: union_unnamed_35 = @import("std").mem.zeroes(union_unnamed_35),
    pal_idx_y: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    pal_idx_uv: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    unnamed_2: union_unnamed_36 = @import("std").mem.zeroes(union_unnamed_36),
};
const union_unnamed_27 = extern union {
    unnamed_0: struct_unnamed_28 align(64),
    unnamed_1: struct_unnamed_32 align(64),
};
pub const Av1Filter = common.Av1Filter;
pub const FILTER_2D_BILINEAR = common.FILTER_2D_BILINEAR;
pub const enum_Filter2d = common.enum_Filter2d;
const struct_unnamed_39 = extern struct {
    pass: c_int = 0,
};
pub const afz_pthread_cond_t = common.afz_pthread_cond_t;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const struct_thread_data = common.struct_thread_data;
pub const enum_TaskType = common.enum_TaskType;
const struct_unnamed_44 = extern struct {
    grain_lut_8bpc: [3][74][82]i8 align(64) = @import("std").mem.zeroes([3][74][82]i8),
    scaling_8bpc: [3][256]u8 align(64) = @import("std").mem.zeroes([3][256]u8),
};
const struct_unnamed_45 = extern struct {
    grain_lut_16bpc: [3][74][82]i16 align(64) = @import("std").mem.zeroes([3][74][82]i16),
    scaling_16bpc: [3][4096]u8 align(64) = @import("std").mem.zeroes([3][4096]u8),
};
const union_unnamed_43 = extern union {
    unnamed_0: struct_unnamed_44,
    unnamed_1: struct_unnamed_45,
};
const struct_unnamed_42 = extern struct {
    exec: c_int = 0,
    finished: c_int = 0,
    cond: afz_pthread_cond_t = 0,
    in: [*c]const Dav1dPicture = null,
    out: [*c]Dav1dPicture = null,
    type: enum_TaskType = @import("std").mem.zeroes(enum_TaskType),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    unnamed_0: union_unnamed_43 = @import("std").mem.zeroes(union_unnamed_43),
};
pub const struct_TaskThreadData_41 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    first: atomic_uint = 0,
    cur: c_uint = 0,
    reset_task_cur: atomic_uint = 0,
    cond_signaled: atomic_int = 0,
    delayed_fg: struct_unnamed_42 = @import("std").mem.zeroes(struct_unnamed_42),
    inited: c_int = 0,
};
pub const struct_FrameTileThreadData_46 = common.struct_FrameTileThreadData_46;
const struct_unnamed_40 = extern struct {
    td: struct_thread_data = @import("std").mem.zeroes(struct_thread_data),
    ttd: [*c]struct_TaskThreadData_41 = null,
    fttd: [*c]struct_FrameTileThreadData_46 = null,
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
    unnamed_0: union_unnamed_25 = @import("std").mem.zeroes(union_unnamed_25),
    unnamed_1: union_unnamed_26 = @import("std").mem.zeroes(union_unnamed_26),
    pal_sz_uv: [2][32]u8 = @import("std").mem.zeroes([2][32]u8),
    scratch: union_unnamed_27 = @import("std").mem.zeroes(union_unnamed_27),
    warpmv: Dav1dWarpedMotionParams = @import("std").mem.zeroes(Dav1dWarpedMotionParams),
    lf_mask: [*c]Av1Filter = null,
    top_pre_cdef_toggle: c_int = 0,
    cur_sb_cdef_idx_ptr: [*c]i8 = null,
    tl_4x4_filter: enum_Filter2d = @import("std").mem.zeroes(enum_Filter2d),
    frame_thread: struct_unnamed_39 = @import("std").mem.zeroes(struct_unnamed_39),
    task_thread: struct_unnamed_40 = @import("std").mem.zeroes(struct_unnamed_40),
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
const struct_unnamed_47 = extern struct {
    out_delayed: [*c]Dav1dThreadPicture = null,
    next: c_uint = 0,
};
const struct_unnamed_48 = extern struct {
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
const struct_unnamed_49 = extern struct {
    s0: u32 = 0,
    s1: u32 = 0,
    w0: i16 = 0,
    w1: i16 = 0,
};
pub const union_LooprestorationParams = extern union {
    filter: [2][8]i16 align(16),
    sgr: struct_unnamed_49 align(16),
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
    frame_thread: struct_unnamed_47 = @import("std").mem.zeroes(struct_unnamed_47),
    task_thread: struct_TaskThreadData_41 = @import("std").mem.zeroes(struct_TaskThreadData_41),
    segmap_pool: [*c]Dav1dMemPool = null,
    refmvs_pool: [*c]Dav1dMemPool = null,
    refs: [8]struct_unnamed_48 = @import("std").mem.zeroes([8]struct_unnamed_48),
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
pub const BS_128x128 = common.BS_128x128;
pub const BS_64x64 = common.BS_64x64;
pub const BS_4x4 = common.BS_4x4;
pub const enum_BlockSize = common.enum_BlockSize;
pub const EDGE_I444_TOP_HAS_RIGHT = common.EDGE_I444_TOP_HAS_RIGHT;
pub const EDGE_ALL_TOP_HAS_RIGHT = common.EDGE_ALL_TOP_HAS_RIGHT;
pub const EDGE_ALL_LEFT_HAS_BOTTOM = common.EDGE_ALL_LEFT_HAS_BOTTOM;
pub const EDGE_ALL_TR_AND_BL = common.EDGE_ALL_TR_AND_BL;
pub const enum_EdgeFlags = common.enum_EdgeFlags;
const struct_unnamed_52 = extern struct {
    y_mode: u8 = 0,
    uv_mode: u8 = 0,
    tx: u8 = 0,
    pal_sz: [2]u8 = @import("std").mem.zeroes([2]u8),
    y_angle: i8 = 0,
    uv_angle: i8 = 0,
    cfl_alpha: [2]i8 = @import("std").mem.zeroes([2]i8),
};
const struct_unnamed_55 = extern struct {
    mv: [2]union_mv = @import("std").mem.zeroes([2]union_mv),
    wedge_idx: u8 = 0,
    mask_sign: u8 = 0,
    interintra_mode: u8 = 0,
};
const struct_unnamed_56 = extern struct {
    mv2d: union_mv = @import("std").mem.zeroes(union_mv),
    matrix: [4]i16 = @import("std").mem.zeroes([4]i16),
};
const union_unnamed_54 = extern union {
    unnamed_0: struct_unnamed_55,
    unnamed_1: struct_unnamed_56,
};
const struct_unnamed_53 = extern struct {
    unnamed_0: union_unnamed_54 = @import("std").mem.zeroes(union_unnamed_54),
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
const union_unnamed_51 = extern union {
    unnamed_0: struct_unnamed_52,
    unnamed_1: struct_unnamed_53,
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
    unnamed_0: union_unnamed_51 = @import("std").mem.zeroes(union_unnamed_51),
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
const struct_unnamed_50 = extern struct {
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
const struct_unnamed_57 = extern struct {
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
const struct_unnamed_58 = extern struct {
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
const struct_unnamed_60 = extern struct {
    merge: atomic_int = 0,
    lock: afz_pthread_mutex_t = 0,
    head: [*c]Dav1dTask = null,
    tail: [*c]Dav1dTask = null,
};
const struct_unnamed_59 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    ttd: [*c]struct_TaskThreadData_41 = null,
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
    pending_tasks: struct_unnamed_60 = @import("std").mem.zeroes(struct_unnamed_60),
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
    svc: [7][2]struct_ScalableMotionParams_20 = @import("std").mem.zeroes([7][2]struct_ScalableMotionParams_20),
    resize_step: [2]c_int = @import("std").mem.zeroes([2]c_int),
    resize_start: [2]c_int = @import("std").mem.zeroes([2]c_int),
    c: [*c]const Dav1dContext = null,
    ts: [*c]Dav1dTileState = null,
    n_ts: c_int = 0,
    dsp: [*c]const Dav1dDSPContext = null,
    bd_fn: struct_unnamed_50 = @import("std").mem.zeroes(struct_unnamed_50),
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
    frame_thread: struct_unnamed_57 = @import("std").mem.zeroes(struct_unnamed_57),
    lf: struct_unnamed_58 = @import("std").mem.zeroes(struct_unnamed_58),
    task_thread: struct_unnamed_59 = @import("std").mem.zeroes(struct_unnamed_59),
    tile_thread: struct_FrameTileThreadData_46 = @import("std").mem.zeroes(struct_FrameTileThreadData_46),
};
pub extern fn antfly_av1_dav1d_cdef_dsp_init_8bpc(c: [*c]Dav1dCdefDSPContext) void;
pub extern fn antfly_av1_dav1d_cdef_dsp_init_16bpc(c: [*c]Dav1dCdefDSPContext) void;
pub const TX_4X4 = common.TX_4X4;
pub const TX_8X8 = common.TX_8X8;
pub const TX_64X64 = common.TX_64X64;
pub const BL_128X128 = common.BL_128X128;
pub const BL_64X64 = common.BL_64X64;
pub const BL_8X8 = common.BL_8X8;
pub const enum_BlockLevel = common.enum_BlockLevel;
pub const N_RECT_TX_SIZES = common.N_RECT_TX_SIZES;
pub const enum_RectTxfmSize = common.enum_RectTxfmSize;
pub const DC_PRED = common.DC_PRED;
pub const VERT_PRED = common.VERT_PRED;
pub const VERT_LEFT_PRED = common.VERT_LEFT_PRED;
pub const N_INTRA_PRED_MODES = common.N_INTRA_PRED_MODES;
pub const CFL_PRED = common.CFL_PRED;
pub const N_UV_INTRA_PRED_MODES = common.N_UV_INTRA_PRED_MODES;
pub const FILTER_PRED = common.FILTER_PRED;
pub const enum_IntraPredMode = common.enum_IntraPredMode;
pub const N_INTER_INTRA_PRED_MODES = common.N_INTER_INTRA_PRED_MODES;
pub const PARTITION_NONE = common.PARTITION_NONE;
pub const PARTITION_H = common.PARTITION_H;
pub const PARTITION_V = common.PARTITION_V;
pub const PARTITION_SPLIT = common.PARTITION_SPLIT;
pub const PARTITION_T_TOP_SPLIT = common.PARTITION_T_TOP_SPLIT;
pub const PARTITION_T_BOTTOM_SPLIT = common.PARTITION_T_BOTTOM_SPLIT;
pub const PARTITION_T_LEFT_SPLIT = common.PARTITION_T_LEFT_SPLIT;
pub const PARTITION_T_RIGHT_SPLIT = common.PARTITION_T_RIGHT_SPLIT;
pub const PARTITION_H4 = common.PARTITION_H4;
pub const PARTITION_V4 = common.PARTITION_V4;
pub const enum_BlockPartition = common.enum_BlockPartition;
pub const MV_JOINT_H = common.MV_JOINT_H;
pub const MV_JOINT_V = common.MV_JOINT_V;
pub const N_MV_JOINTS = common.N_MV_JOINTS;
pub const enum_MVJoint = common.enum_MVJoint;
pub const NEARESTMV = common.NEARESTMV;
pub const NEARMV = common.NEARMV;
pub const GLOBALMV = common.GLOBALMV;
pub const NEWMV = common.NEWMV;
pub const enum_InterPredMode = common.enum_InterPredMode;
pub const NEAREST_DRL = common.NEAREST_DRL;
pub const NEARER_DRL = common.NEARER_DRL;
pub const NEAR_DRL = common.NEAR_DRL;
pub const NEARISH_DRL = common.NEARISH_DRL;
pub const NEARESTMV_NEARESTMV = common.NEARESTMV_NEARESTMV;
pub const GLOBALMV_GLOBALMV = common.GLOBALMV_GLOBALMV;
pub const NEWMV_NEWMV = common.NEWMV_NEWMV;
pub const N_COMP_INTER_PRED_MODES = common.N_COMP_INTER_PRED_MODES;
pub const enum_CompInterPredMode = common.enum_CompInterPredMode;
pub const COMP_INTER_NONE = common.COMP_INTER_NONE;
pub const COMP_INTER_WEIGHTED_AVG = common.COMP_INTER_WEIGHTED_AVG;
pub const COMP_INTER_AVG = common.COMP_INTER_AVG;
pub const COMP_INTER_SEG = common.COMP_INTER_SEG;
pub const COMP_INTER_WEDGE = common.COMP_INTER_WEDGE;
pub const INTER_INTRA_NONE = common.INTER_INTRA_NONE;
pub const INTER_INTRA_BLEND = common.INTER_INTRA_BLEND;
pub const INTER_INTRA_WEDGE = common.INTER_INTRA_WEDGE;
pub const MM_TRANSLATION = common.MM_TRANSLATION;
pub const MM_OBMC = common.MM_OBMC;
pub const MM_WARP = common.MM_WARP;
pub const struct_Dav1dSettings = extern struct {
    n_threads: c_int = 0,
    max_frame_delay: c_int = 0,
    apply_grain: c_int = 0,
    operating_point: c_int = 0,
    all_layers: c_int = 0,
    frame_size_limit: c_uint = 0,
    allocator: Dav1dPicAllocator = @import("std").mem.zeroes(Dav1dPicAllocator),
    logger: Dav1dLogger = @import("std").mem.zeroes(Dav1dLogger),
    strict_std_compliance: c_int = 0,
    output_invisible_frames: c_int = 0,
    inloop_filters: enum_Dav1dInloopFilterType = @import("std").mem.zeroes(enum_Dav1dInloopFilterType),
    decode_frame_type: enum_Dav1dDecodeFrameType = @import("std").mem.zeroes(enum_Dav1dDecodeFrameType),
    reserved: [16]u8 = @import("std").mem.zeroes([16]u8),
};
pub const Dav1dSettings = struct_Dav1dSettings;
pub extern fn antfly_codec_afz_malloc(__size: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_free(?*anyopaque) void;
pub extern fn antfly_codec_afz_posix_memalign(__memptr: [*c]?*anyopaque, __alignment: usize, __size: usize) c_int;
pub extern fn antfly_codec_abs(c_int) c_int;
pub extern fn antfly_codec_llabs(c_longlong) c_longlong;
pub extern fn antfly_codec_afz_mutex_lock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_unlock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_cond_signal([*c]afz_pthread_cond_t) c_int;
pub extern fn antfly_codec_afz_cond_wait([*c]afz_pthread_cond_t, [*c]afz_pthread_mutex_t) c_int;
pub fn dav1d_alloc_aligned_internal(sz: usize, @"align": usize) callconv(.c) ?*anyopaque {
    _ = &sz;
    _ = &@"align";
    while (true) {
        if (!!((@"align" & (@"align" -% @as(usize, 1))) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    var ptr: ?*anyopaque = undefined;
    _ = &ptr;
    if (antfly_codec_afz_posix_memalign(&ptr, @"align", sz) != 0) return null;
    return ptr;
}
pub fn dav1d_free_aligned_internal(arg_ptr: ?*anyopaque) callconv(.c) void {
    var ptr = arg_ptr;
    _ = &ptr;
    antfly_codec_afz_free(ptr);
}
pub fn dav1d_freep_aligned(arg_ptr: ?*anyopaque) callconv(.c) void {
    var ptr = arg_ptr;
    _ = &ptr;
    var mem: [*c]?*anyopaque = @ptrCast(@alignCast(ptr));
    _ = &mem;
    if (mem.* != null) {
        dav1d_free_aligned_internal(mem.*);
        mem.* = null;
    }
}
pub extern fn antfly_av1_dav1d_ref_create_using_pool(pool: [*c]Dav1dMemPool, size: usize) [*c]Dav1dRef;
pub extern fn antfly_av1_dav1d_ref_dec(ref: [*c][*c]Dav1dRef) void;
pub fn dav1d_ref_inc(ref: [*c]Dav1dRef) callconv(.c) void {
    _ = &ref;
    _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ref.*.ref_cnt)), 1);
}
pub extern fn antfly_av1_dav1d_cdf_thread_init_static(cdf: [*c]CdfThreadContext, qidx: c_uint) void;
pub extern fn antfly_av1_dav1d_cdf_thread_alloc(c: [*c]Dav1dContext, cdf: [*c]CdfThreadContext, have_frame_mt: c_int) c_int;
pub extern fn antfly_av1_dav1d_cdf_thread_copy(dst: [*c]CdfContext, src: [*c]const CdfThreadContext) void;
pub extern fn antfly_av1_dav1d_cdf_thread_ref(dst: [*c]CdfThreadContext, src: [*c]CdfThreadContext) void;
pub extern fn antfly_av1_dav1d_cdf_thread_unref(cdf: [*c]CdfThreadContext) void;
pub extern fn antfly_av1_dav1d_cdf_thread_update(hdr: [*c]const Dav1dFrameHeader, dst: [*c]CdfContext, src: [*c]const CdfContext) void;
pub extern fn antfly_av1_dav1d_data_props_copy(dst: [*c]Dav1dDataProps, src: [*c]const Dav1dDataProps) void;
pub extern fn antfly_av1_dav1d_data_unref_internal(buf: [*c]Dav1dData) void;
pub const struct_EdgeNode = extern struct {
    o: u8 = 0,
    h: [2]u8 = @import("std").mem.zeroes([2]u8),
    v: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const EdgeNode = struct_EdgeNode;
pub const struct_EdgeTip = extern struct {
    node: EdgeNode = @import("std").mem.zeroes(EdgeNode),
    split: [3]u8 = @import("std").mem.zeroes([3]u8),
};
pub const EdgeTip = struct_EdgeTip;
pub const struct_EdgeBranch = extern struct {
    node: EdgeNode = @import("std").mem.zeroes(EdgeNode),
    h4: u8 = 0,
    v4: u8 = 0,
    split_offset: [4]u16 = @import("std").mem.zeroes([4]u16),
};
pub const EdgeBranch = struct_EdgeBranch;
pub extern var antfly_av1_dav1d_intra_edge_tree: [2][*c]const EdgeNode;
pub extern var antfly_av1_dav1d_al_part_ctx: [2][5][10]u8;
pub extern var antfly_av1_dav1d_block_sizes: [5][10][2]u8;
pub extern var antfly_av1_dav1d_block_dimensions: [22][4]u8;
pub const struct_TxfmInfo = extern struct {
    w: u8 = 0,
    h: u8 = 0,
    lw: u8 = 0,
    lh: u8 = 0,
    min: u8 = 0,
    max: u8 = 0,
    sub: u8 = 0,
    ctx: u8 = 0,
};
pub const TxfmInfo = struct_TxfmInfo;
pub extern const antfly_av1_dav1d_txfm_dimensions: [19]TxfmInfo;
pub extern var antfly_av1_dav1d_max_txfm_size_for_bs: [22][4]u8;
pub extern var antfly_av1_dav1d_comp_inter_pred_modes: [8][2]u8;
pub extern const antfly_av1_dav1d_partition_type_count: [5]u8;
pub extern const antfly_av1_dav1d_ymode_size_context: [22]u8;
pub extern var antfly_av1_dav1d_filter_2d: [4][4]u8;
pub extern var antfly_av1_dav1d_filter_dir: [10][2]u8;
pub extern const antfly_av1_dav1d_intra_mode_context: [13]u8;
pub extern const antfly_av1_dav1d_wedge_ctx_lut: [22]u8;
pub const cfl_allowed_mask = common.cfl_allowed_mask;
pub const wedge_allowed_mask = common.wedge_allowed_mask;
pub const interintra_allowed_mask = common.interintra_allowed_mask;
pub extern var antfly_av1_dav1d_sgr_params: [16][2]u16;
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
pub const struct_refmvs_candidate = extern struct {
    mv: refmvs_mvpair = @import("std").mem.zeroes(refmvs_mvpair),
    weight: c_int = 0,
};
pub const refmvs_candidate = struct_refmvs_candidate;
pub extern fn antfly_av1_dav1d_refmvs_init_frame(rf: [*c]refmvs_frame, seq_hdr: [*c]const Dav1dSequenceHeader, frm_hdr: [*c]const Dav1dFrameHeader, ref_poc: [*c]const c_uint, rp: [*c]refmvs_temporal_block, ref_ref_poc: [*c][7]c_uint, rp_ref: [*c]const [*c]refmvs_temporal_block, n_tile_threads: c_int, n_frame_threads: c_int) c_int;
pub fn dav1d_refmvs_save_tmvs(dsp: [*c]const Dav1dRefmvsDSPContext, rt: [*c]const refmvs_tile, col_start8: c_int, arg_col_end8: c_int, row_start8: c_int, arg_row_end8: c_int) callconv(.c) void {
    _ = &dsp;
    _ = &rt;
    _ = &col_start8;
    var col_end8 = arg_col_end8;
    _ = &col_end8;
    _ = &row_start8;
    var row_end8 = arg_row_end8;
    _ = &row_end8;
    const rf: [*c]const refmvs_frame = rt.*.rf;
    _ = &rf;
    while (true) {
        if (!(row_start8 >= @as(c_int, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(@as(c_uint, @bitCast(@as(c_int, row_end8 - row_start8))) <= @as(c_uint, 16))) {
            unreachable;
        }
        if (!false) break;
    }
    row_end8 = imin(row_end8, rf.*.ih8);
    col_end8 = imin(col_end8, rf.*.iw8);
    const stride: ptrdiff_t = rf.*.rp_stride;
    _ = &stride;
    const ref_sign: [*c]const u8 = @ptrCast(@alignCast(&rf.*.mfmv_sign));
    _ = &ref_sign;
    var rp: [*c]refmvs_temporal_block = &rf.*.rp[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, row_start8) * stride)))];
    _ = &rp;
    dsp.*.save_tmvs.?(rp, stride, @as([*c][*c]refmvs_block, @ptrCast(@alignCast(@constCast(&rt.*.r)))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6))))), ref_sign, col_end8, row_end8, col_start8, row_start8);
}
pub extern fn antfly_av1_dav1d_refmvs_tile_sbrow_init(rt: [*c]refmvs_tile, rf: [*c]const refmvs_frame, tile_col_start4: c_int, tile_col_end4: c_int, tile_row_start4: c_int, tile_row_end4: c_int, sby: c_int, tile_row_idx: c_int, pass: c_int) void;
pub extern fn antfly_av1_dav1d_refmvs_find(rt: [*c]const refmvs_tile, mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, ctx: [*c]c_int, ref: refmvs_refpair, bs: enum_BlockSize, edge_flags: enum_EdgeFlags, by4: c_int, bx4: c_int) void;
pub fn get_intra_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    if (have_left != 0) {
        if (have_top != 0) {
            const ctx: c_int = @as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) + @as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]);
            _ = &ctx;
            return ctx + @as(c_int, @intFromBool(ctx == @as(c_int, 2)));
        } else return @as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) * @as(c_int, 2);
    } else {
        return if (have_top != 0) @as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) * @as(c_int, 2) else @as(c_int, 0);
    }
}
pub fn get_tx_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, max_tx: [*c]const TxfmInfo, yb4: c_int, xb4: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &max_tx;
    _ = &yb4;
    _ = &xb4;
    return @as(c_int, @intFromBool(@as(c_int, l.*.tx_intra[@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, max_tx.*.lh))) + @as(c_int, @intFromBool(@as(c_int, a.*.tx_intra[@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, max_tx.*.lw)));
}
pub fn get_partition_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, bl: enum_BlockLevel, yb8: c_int, xb8: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &bl;
    _ = &yb8;
    _ = &xb8;
    return ((@as(c_int, a.*.partition[@bitCast(@as(isize, @intCast(xb8)))]) >> @intCast(@as(c_int, 4) - @as(c_int, @bitCast(bl)))) & @as(c_int, 1)) + (((@as(c_int, l.*.partition[@bitCast(@as(isize, @intCast(yb8)))]) >> @intCast(@as(c_int, 4) - @as(c_int, @bitCast(bl)))) & @as(c_int, 1)) << @intCast(@as(c_int, 1)));
}
pub fn gather_left_partition_prob(in: [*c]const u16, bl: enum_BlockLevel) callconv(.c) c_uint {
    _ = &in;
    _ = &bl;
    var out: c_uint = @bitCast(@as(c_int, @as(c_int, in[PARTITION_H - @as(c_int, 1)]) - @as(c_int, in[PARTITION_H])));
    _ = &out;
    out +%= @bitCast(@as(c_int, @as(c_int, in[PARTITION_SPLIT - @as(c_int, 1)]) - @as(c_int, in[PARTITION_T_LEFT_SPLIT])));
    if (bl != @as(enum_BlockLevel, BL_128X128)) {
        out +%= @bitCast(@as(c_int, @as(c_int, in[PARTITION_H4 - @as(c_int, 1)]) - @as(c_int, in[PARTITION_H4])));
    }
    return out;
}
pub fn gather_top_partition_prob(in: [*c]const u16, bl: enum_BlockLevel) callconv(.c) c_uint {
    _ = &in;
    _ = &bl;
    var out: c_uint = @bitCast(@as(c_int, @as(c_int, in[PARTITION_V - @as(c_int, 1)]) - @as(c_int, in[PARTITION_T_TOP_SPLIT])));
    _ = &out;
    out +%= @bitCast(@as(c_int, @as(c_int, in[PARTITION_T_LEFT_SPLIT - @as(c_int, 1)])));
    if (bl != @as(enum_BlockLevel, BL_128X128)) {
        out +%= @bitCast(@as(c_int, @as(c_int, in[PARTITION_V4 - @as(c_int, 1)]) - @as(c_int, in[PARTITION_T_RIGHT_SPLIT])));
    }
    return out;
}
pub fn get_filter_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, comp: c_int, dir: c_int, ref: c_int, yb4: c_int, xb4: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &comp;
    _ = &dir;
    _ = &ref;
    _ = &yb4;
    _ = &xb4;
    const a_filter: c_int = if ((@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) == ref) or (@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) == ref)) @as(c_int, a.*.filter[@bitCast(@as(isize, @intCast(dir)))][@bitCast(@as(isize, @intCast(xb4)))]) else DAV1D_N_SWITCHABLE_FILTERS;
    _ = &a_filter;
    const l_filter: c_int = if ((@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) == ref) or (@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) == ref)) @as(c_int, l.*.filter[@bitCast(@as(isize, @intCast(dir)))][@bitCast(@as(isize, @intCast(yb4)))]) else DAV1D_N_SWITCHABLE_FILTERS;
    _ = &l_filter;
    if (a_filter == l_filter) {
        return (comp * @as(c_int, 4)) + a_filter;
    } else if (a_filter == DAV1D_N_SWITCHABLE_FILTERS) {
        return (comp * @as(c_int, 4)) + l_filter;
    } else if (l_filter == DAV1D_N_SWITCHABLE_FILTERS) {
        return (comp * @as(c_int, 4)) + a_filter;
    } else {
        return (comp * @as(c_int, 4)) + DAV1D_N_SWITCHABLE_FILTERS;
    }
}
pub fn get_comp_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    if (have_top != 0) {
        if (have_left != 0) {
            if (@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) {
                if (@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) {
                    return 4;
                } else {
                    return @as(c_int, 2) + @as(c_int, @intFromBool(@as(c_uint, @bitCast(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]))) >= @as(c_uint, 4)));
                }
            } else if (@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) {
                return @as(c_int, 2) + @as(c_int, @intFromBool(@as(c_uint, @bitCast(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]))) >= @as(c_uint, 4)));
            } else {
                return @as(c_int, @intFromBool(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4))) ^ @as(c_int, @intFromBool(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4)));
            }
        } else {
            return if (a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))] != 0) @as(c_int, 3) else @as(c_int, @intFromBool(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4)));
        }
    } else if (have_left != 0) {
        return if (l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))] != 0) @as(c_int, 3) else @as(c_int, @intFromBool(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4)));
    } else {
        return 1;
    }
}
pub fn get_comp_dir_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    if ((have_top != 0) and (have_left != 0)) {
        const a_intra: c_int = a.*.intra[@bitCast(@as(isize, @intCast(xb4)))];
        _ = &a_intra;
        const l_intra: c_int = l.*.intra[@bitCast(@as(isize, @intCast(yb4)))];
        _ = &l_intra;
        if ((a_intra != 0) and (l_intra != 0)) return 2;
        if ((a_intra != 0) or (l_intra != 0)) {
            const edge: [*c]const BlockContext = if (a_intra != 0) l else a;
            _ = &edge;
            const off: c_int = if (a_intra != 0) yb4 else xb4;
            _ = &off;
            if (@as(c_int, edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]) == COMP_INTER_NONE) return 2;
            return @as(c_int, 1) + (@as(c_int, 2) * @as(c_int, @intFromBool(@intFromBool(@as(c_int, edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]) < @as(c_int, 4)) == @intFromBool(@as(c_int, edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]) < @as(c_int, 4)))));
        }
        const a_comp: c_int = @as(c_int, @intFromBool(@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != COMP_INTER_NONE));
        _ = &a_comp;
        const l_comp: c_int = @as(c_int, @intFromBool(@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != COMP_INTER_NONE));
        _ = &l_comp;
        const a_ref0: c_int = a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))];
        _ = &a_ref0;
        const l_ref0: c_int = l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))];
        _ = &l_ref0;
        if (!(a_comp != 0) and !(l_comp != 0)) {
            return @as(c_int, 1) + (@as(c_int, 2) * @as(c_int, @intFromBool(@intFromBool(a_ref0 >= @as(c_int, 4)) == @intFromBool(l_ref0 >= @as(c_int, 4)))));
        } else if (!(a_comp != 0) or !(l_comp != 0)) {
            const edge: [*c]const BlockContext = if (a_comp != 0) a else l;
            _ = &edge;
            const off: c_int = if (a_comp != 0) xb4 else yb4;
            _ = &off;
            if (!(@as(c_int, @intFromBool(@as(c_int, edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]) < @as(c_int, 4))) == @as(c_int, @intFromBool(@as(c_int, edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]) < @as(c_int, 4))))) return 1;
            return @as(c_int, 3) + @as(c_int, @intFromBool(@intFromBool(a_ref0 >= @as(c_int, 4)) == @intFromBool(l_ref0 >= @as(c_int, 4))));
        } else {
            const a_uni: c_int = @as(c_int, @intFromBool(@intFromBool(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) < @as(c_int, 4)) == @intFromBool(@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) < @as(c_int, 4))));
            _ = &a_uni;
            const l_uni: c_int = @as(c_int, @intFromBool(@intFromBool(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) < @as(c_int, 4)) == @intFromBool(@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) < @as(c_int, 4))));
            _ = &l_uni;
            if (!(a_uni != 0) and !(l_uni != 0)) return 0;
            if (!(a_uni != 0) or !(l_uni != 0)) return 2;
            return @as(c_int, 3) + @as(c_int, @intFromBool(@intFromBool(a_ref0 == @as(c_int, 4)) == @intFromBool(l_ref0 == @as(c_int, 4))));
        }
    } else if ((have_top != 0) or (have_left != 0)) {
        const edge: [*c]const BlockContext = if (have_left != 0) l else a;
        _ = &edge;
        const off: c_int = if (have_left != 0) yb4 else xb4;
        _ = &off;
        if (@as(c_int, edge.*.intra[@bitCast(@as(isize, @intCast(off)))]) != 0) return 2;
        if (@as(c_int, edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]) == COMP_INTER_NONE) return 2;
        return @as(c_int, 4) * @as(c_int, @intFromBool(@intFromBool(@as(c_int, edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]) < @as(c_int, 4)) == @intFromBool(@as(c_int, edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]) < @as(c_int, 4))));
    } else {
        return 2;
    }
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
pub fn get_jnt_comp_ctx(order_hint_n_bits: c_int, poc: c_uint, ref0poc: c_uint, ref1poc: c_uint, a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int) callconv(.c) c_int {
    _ = &order_hint_n_bits;
    _ = &poc;
    _ = &ref0poc;
    _ = &ref1poc;
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    const d0: c_uint = @bitCast(@as(c_int, antfly_codec_abs(get_poc_diff(order_hint_n_bits, @bitCast(@as(c_uint, @truncate(ref0poc))), @bitCast(@as(c_uint, @truncate(poc)))))));
    _ = &d0;
    const d1: c_uint = @bitCast(@as(c_int, antfly_codec_abs(get_poc_diff(order_hint_n_bits, @bitCast(@as(c_uint, @truncate(poc))), @bitCast(@as(c_uint, @truncate(ref1poc)))))));
    _ = &d1;
    const offset: c_int = @as(c_int, @intFromBool(d0 == d1));
    _ = &offset;
    const a_ctx: c_int = @as(c_int, @intFromBool((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) >= COMP_INTER_AVG) or (@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) == @as(c_int, 6))));
    _ = &a_ctx;
    const l_ctx: c_int = @as(c_int, @intFromBool((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) >= COMP_INTER_AVG) or (@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) == @as(c_int, 6))));
    _ = &l_ctx;
    return ((@as(c_int, 3) * offset) + a_ctx) + l_ctx;
}
pub fn get_mask_comp_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    const a_ctx: c_int = if (@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) >= COMP_INTER_SEG) @as(c_int, 1) else if (@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) == @as(c_int, 6)) @as(c_int, 3) else @as(c_int, 0);
    _ = &a_ctx;
    const l_ctx: c_int = if (@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) >= COMP_INTER_SEG) @as(c_int, 1) else if (@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) == @as(c_int, 6)) @as(c_int, 3) else @as(c_int, 0);
    _ = &l_ctx;
    return imin(a_ctx + l_ctx, 5);
}
pub fn av1_get_ref_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, arg_have_top: c_int, arg_have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    var have_top = arg_have_top;
    _ = &have_top;
    var have_left = arg_have_left;
    _ = &have_left;
    var cnt: [2]c_int = [1]c_int{
        0,
    } ++ @as([1]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        cnt[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4))))))] += 1;
        if (@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4))))))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        cnt[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4))))))] += 1;
        if (@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4))))))] += 1;
        }
    }
    return if (cnt[@as(c_int, 0)] == cnt[@as(c_int, 1)]) @as(c_int, 1) else if (cnt[@as(c_int, 0)] < cnt[@as(c_int, 1)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn av1_get_fwd_ref_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    var cnt: [4]c_int = [1]c_int{
        0,
    } ++ @as([3]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        if (@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) < @as(c_int, 4)) {
            cnt[@bitCast(@as(isize, @intCast(a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))])))] += 1;
        }
        if ((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) and (@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) < @as(c_int, 4))) {
            cnt[@bitCast(@as(isize, @intCast(a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))])))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        if (@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) < @as(c_int, 4)) {
            cnt[@bitCast(@as(isize, @intCast(l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))])))] += 1;
        }
        if ((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) and (@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) < @as(c_int, 4))) {
            cnt[@bitCast(@as(isize, @intCast(l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))])))] += 1;
        }
    }
    cnt[@as(c_int, 0)] += cnt[@as(c_int, 1)];
    cnt[@as(c_int, 2)] += cnt[@as(c_int, 3)];
    return if (cnt[@as(c_int, 0)] == cnt[@as(c_int, 2)]) @as(c_int, 1) else if (cnt[@as(c_int, 0)] < cnt[@as(c_int, 2)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn av1_get_fwd_ref_1_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    var cnt: [2]c_int = [1]c_int{
        0,
    } ++ @as([1]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        if (@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) < @as(c_int, 2)) {
            cnt[@bitCast(@as(isize, @intCast(a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))])))] += 1;
        }
        if ((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) and (@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) < @as(c_int, 2))) {
            cnt[@bitCast(@as(isize, @intCast(a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))])))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        if (@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) < @as(c_int, 2)) {
            cnt[@bitCast(@as(isize, @intCast(l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))])))] += 1;
        }
        if ((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) and (@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) < @as(c_int, 2))) {
            cnt[@bitCast(@as(isize, @intCast(l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))])))] += 1;
        }
    }
    return if (cnt[@as(c_int, 0)] == cnt[@as(c_int, 1)]) @as(c_int, 1) else if (cnt[@as(c_int, 0)] < cnt[@as(c_int, 1)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn av1_get_fwd_ref_2_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    var cnt: [2]c_int = [1]c_int{
        0,
    } ++ @as([1]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))])))) ^ @as(c_uint, 2)) < @as(c_uint, 2)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 2))))] += 1;
        }
        if ((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) and ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))])))) ^ @as(c_uint, 2)) < @as(c_uint, 2))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 2))))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))])))) ^ @as(c_uint, 2)) < @as(c_uint, 2)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 2))))] += 1;
        }
        if ((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) and ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))])))) ^ @as(c_uint, 2)) < @as(c_uint, 2))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 2))))] += 1;
        }
    }
    return if (cnt[@as(c_int, 0)] == cnt[@as(c_int, 1)]) @as(c_int, 1) else if (cnt[@as(c_int, 0)] < cnt[@as(c_int, 1)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn av1_get_bwd_ref_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    var cnt: [3]c_int = [1]c_int{
        0,
    } ++ @as([2]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        if (@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 4))))] += 1;
        }
        if ((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) and (@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 4))))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        if (@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 4))))] += 1;
        }
        if ((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) and (@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 4))))] += 1;
        }
    }
    cnt[@as(c_int, 1)] += cnt[@as(c_int, 0)];
    return if (cnt[@as(c_int, 2)] == cnt[@as(c_int, 1)]) @as(c_int, 1) else if (cnt[@as(c_int, 1)] < cnt[@as(c_int, 2)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn av1_get_bwd_ref_1_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    var cnt: [3]c_int = [1]c_int{
        0,
    } ++ @as([2]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        if (@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 4))))] += 1;
        }
        if ((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) and (@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) >= @as(c_int, 4))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 4))))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        if (@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 4))))] += 1;
        }
        if ((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) and (@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) >= @as(c_int, 4))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 4))))] += 1;
        }
    }
    return if (cnt[@as(c_int, 0)] == cnt[@as(c_int, 1)]) @as(c_int, 1) else if (cnt[@as(c_int, 0)] < cnt[@as(c_int, 1)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn av1_get_uni_p1_ctx(a: [*c]const BlockContext, l: [*c]const BlockContext, yb4: c_int, xb4: c_int, have_top: c_int, have_left: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &l;
    _ = &yb4;
    _ = &xb4;
    _ = &have_top;
    _ = &have_left;
    var cnt: [3]c_int = [1]c_int{
        0,
    } ++ @as([2]c_int, @splat(0));
    _ = &cnt;
    if ((have_top != 0) and !(@as(c_int, a.*.intra[@bitCast(@as(isize, @intCast(xb4)))]) != 0)) {
        if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))])))) -% @as(c_uint, 1)) < @as(c_uint, 3)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 1))))] += 1;
        }
        if ((@as(c_int, a.*.comp_type[@bitCast(@as(isize, @intCast(xb4)))]) != 0) and ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))])))) -% @as(c_uint, 1)) < @as(c_uint, 3))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, a.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(xb4)))]) - @as(c_int, 1))))] += 1;
        }
    }
    if ((have_left != 0) and !(@as(c_int, l.*.intra[@bitCast(@as(isize, @intCast(yb4)))]) != 0)) {
        if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))])))) -% @as(c_uint, 1)) < @as(c_uint, 3)) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 1))))] += 1;
        }
        if ((@as(c_int, l.*.comp_type[@bitCast(@as(isize, @intCast(yb4)))]) != 0) and ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))])))) -% @as(c_uint, 1)) < @as(c_uint, 3))) {
            cnt[@bitCast(@as(isize, @intCast(@as(c_int, l.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(yb4)))]) - @as(c_int, 1))))] += 1;
        }
    }
    cnt[@as(c_int, 1)] += cnt[@as(c_int, 2)];
    return if (cnt[@as(c_int, 0)] == cnt[@as(c_int, 1)]) @as(c_int, 1) else if (cnt[@as(c_int, 0)] < cnt[@as(c_int, 1)]) @as(c_int, 0) else @as(c_int, 2);
}
pub fn get_drl_context(ref_mv_stack: [*c]const refmvs_candidate, ref_idx: c_int) callconv(.c) c_int {
    _ = &ref_mv_stack;
    _ = &ref_idx;
    if (ref_mv_stack[@bitCast(@as(isize, @intCast(ref_idx)))].weight >= @as(c_int, 640)) return @as(c_int, @intFromBool(ref_mv_stack[@bitCast(@as(isize, @intCast(ref_idx + @as(c_int, 1))))].weight < @as(c_int, 640)));
    return if (ref_mv_stack[@bitCast(@as(isize, @intCast(ref_idx + @as(c_int, 1))))].weight < @as(c_int, 640)) @as(c_int, 2) else @as(c_int, 0);
}
pub fn get_cur_frame_segid(by: c_int, bx: c_int, have_top: c_int, have_left: c_int, seg_ctx: [*c]c_int, arg_cur_seg_map: [*c]const u8, stride: ptrdiff_t) callconv(.c) c_uint {
    _ = &by;
    _ = &bx;
    _ = &have_top;
    _ = &have_left;
    _ = &seg_ctx;
    var cur_seg_map = arg_cur_seg_map;
    _ = &cur_seg_map;
    _ = &stride;
    cur_seg_map += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, bx) + (@as(ptrdiff_t, by) * stride)))));
    if ((have_left != 0) and (have_top != 0)) {
        const l: c_int = cur_seg_map[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
        _ = &l;
        const a: c_int = cur_seg_map[@bitCast(@as(isize, @intCast(-stride)))];
        _ = &a;
        const al: c_int = cur_seg_map[@bitCast(@as(isize, @intCast(-(stride + @as(ptrdiff_t, 1)))))];
        _ = &al;
        if ((l == a) and (al == l)) {
            seg_ctx.* = 2;
        } else if (((l == a) or (al == l)) or (a == al)) {
            seg_ctx.* = 1;
        } else {
            seg_ctx.* = 0;
        }
        return @bitCast(@as(c_int, if (a == al) a else l));
    } else {
        seg_ctx.* = 0;
        return @bitCast(@as(c_int, if (have_left != 0) @as(c_int, cur_seg_map[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else if (have_top != 0) @as(c_int, cur_seg_map[@bitCast(@as(isize, @intCast(-stride)))]) else @as(c_int, 0)));
    }
}
pub fn fix_int_mv_precision(mv_1: [*c]mv) callconv(.c) void {
    _ = &mv_1;
    mv_1.*.unnamed_0.x = @bitCast(@as(c_ushort, @truncate(@as(c_uint, @bitCast(@as(c_int, (@as(c_int, mv_1.*.unnamed_0.x) - (@as(c_int, mv_1.*.unnamed_0.x) >> @intCast(@as(c_int, 15)))) + @as(c_int, 3)))) & ~@as(c_uint, 7))));
    mv_1.*.unnamed_0.y = @bitCast(@as(c_ushort, @truncate(@as(c_uint, @bitCast(@as(c_int, (@as(c_int, mv_1.*.unnamed_0.y) - (@as(c_int, mv_1.*.unnamed_0.y) >> @intCast(@as(c_int, 15)))) + @as(c_int, 3)))) & ~@as(c_uint, 7))));
}
pub fn fix_mv_precision(hdr: [*c]const Dav1dFrameHeader, mv_1: [*c]mv) callconv(.c) void {
    _ = &hdr;
    _ = &mv_1;
    if (@as(c_int, hdr.*.force_integer_mv) != 0) {
        fix_int_mv_precision(mv_1);
    } else if (!(@as(c_int, hdr.*.hp) != 0)) {
        mv_1.*.unnamed_0.x = @bitCast(@as(c_ushort, @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, mv_1.*.unnamed_0.x) - (@as(c_int, mv_1.*.unnamed_0.x) >> @intCast(@as(c_int, 15)))))) & ~@as(c_uint, 1))));
        mv_1.*.unnamed_0.y = @bitCast(@as(c_ushort, @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, mv_1.*.unnamed_0.y) - (@as(c_int, mv_1.*.unnamed_0.y) >> @intCast(@as(c_int, 15)))))) & ~@as(c_uint, 1))));
    }
}
pub fn get_gmv_2d(gmv: [*c]const Dav1dWarpedMotionParams, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int, hdr: [*c]const Dav1dFrameHeader) callconv(.c) mv {
    _ = &gmv;
    _ = &bx4;
    _ = &by4;
    _ = &bw4;
    _ = &bh4;
    _ = &hdr;
    {
        switch (gmv.*.type) {
            @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_ROT_ZOOM) => {
                while (true) {
                    if (!(gmv.*.matrix[@as(c_int, 5)] == gmv.*.matrix[@as(c_int, 2)])) {
                        unreachable;
                    }
                    if (!false) break;
                }
                while (true) {
                    if (!(gmv.*.matrix[@as(c_int, 4)] == -gmv.*.matrix[@as(c_int, 3)])) {
                        unreachable;
                    }
                    if (!false) break;
                }
                {
                    const x: c_int = ((bx4 * @as(c_int, 4)) + (bw4 * @as(c_int, 2))) - @as(c_int, 1);
                    _ = &x;
                    const y: c_int = ((by4 * @as(c_int, 4)) + (bh4 * @as(c_int, 2))) - @as(c_int, 1);
                    _ = &y;
                    const xc: c_int = (((gmv.*.matrix[@as(c_int, 2)] - (@as(c_int, 1) << @intCast(@as(c_int, 16)))) * x) + (gmv.*.matrix[@as(c_int, 3)] * y)) + gmv.*.matrix[@as(c_int, 0)];
                    _ = &xc;
                    const yc: c_int = (((gmv.*.matrix[@as(c_int, 5)] - (@as(c_int, 1) << @intCast(@as(c_int, 16)))) * y) + (gmv.*.matrix[@as(c_int, 4)] * x)) + gmv.*.matrix[@as(c_int, 1)];
                    _ = &yc;
                    const shift: c_int = @as(c_int, 16) - (@as(c_int, 3) - @as(c_int, @intFromBool(!(@as(c_int, hdr.*.hp) != 0))));
                    _ = &shift;
                    const round: c_int = (@as(c_int, 1) << @intCast(shift)) >> @intCast(@as(c_int, 1));
                    _ = &round;
                    var res: mv = mv{
                        .unnamed_0 = struct_unnamed_18{
                            .y = @truncate(apply_sign(((antfly_codec_abs(yc) + round) >> @intCast(shift)) << @intCast(@as(c_int, @intFromBool(!(@as(c_int, hdr.*.hp) != 0)))), yc)),
                            .x = @truncate(apply_sign(((antfly_codec_abs(xc) + round) >> @intCast(shift)) << @intCast(@as(c_int, @intFromBool(!(@as(c_int, hdr.*.hp) != 0)))), xc)),
                        },
                    };
                    _ = &res;
                    if (@as(c_int, hdr.*.force_integer_mv) != 0) {
                        fix_int_mv_precision(&res);
                    }
                    return res;
                }
            },
            else => {
                {
                    const x: c_int = ((bx4 * @as(c_int, 4)) + (bw4 * @as(c_int, 2))) - @as(c_int, 1);
                    _ = &x;
                    const y: c_int = ((by4 * @as(c_int, 4)) + (bh4 * @as(c_int, 2))) - @as(c_int, 1);
                    _ = &y;
                    const xc: c_int = (((gmv.*.matrix[@as(c_int, 2)] - (@as(c_int, 1) << @intCast(@as(c_int, 16)))) * x) + (gmv.*.matrix[@as(c_int, 3)] * y)) + gmv.*.matrix[@as(c_int, 0)];
                    _ = &xc;
                    const yc: c_int = (((gmv.*.matrix[@as(c_int, 5)] - (@as(c_int, 1) << @intCast(@as(c_int, 16)))) * y) + (gmv.*.matrix[@as(c_int, 4)] * x)) + gmv.*.matrix[@as(c_int, 1)];
                    _ = &yc;
                    const shift: c_int = @as(c_int, 16) - (@as(c_int, 3) - @as(c_int, @intFromBool(!(@as(c_int, hdr.*.hp) != 0))));
                    _ = &shift;
                    const round: c_int = (@as(c_int, 1) << @intCast(shift)) >> @intCast(@as(c_int, 1));
                    _ = &round;
                    var res: mv = mv{
                        .unnamed_0 = struct_unnamed_18{
                            .y = @truncate(apply_sign(((antfly_codec_abs(yc) + round) >> @intCast(shift)) << @intCast(@as(c_int, @intFromBool(!(@as(c_int, hdr.*.hp) != 0)))), yc)),
                            .x = @truncate(apply_sign(((antfly_codec_abs(xc) + round) >> @intCast(shift)) << @intCast(@as(c_int, @intFromBool(!(@as(c_int, hdr.*.hp) != 0)))), xc)),
                        },
                    };
                    _ = &res;
                    if (@as(c_int, hdr.*.force_integer_mv) != 0) {
                        fix_int_mv_precision(&res);
                    }
                    return res;
                }
            },
            @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION) => {
                {
                    var res: mv = mv{
                        .unnamed_0 = struct_unnamed_18{
                            .y = @truncate(gmv.*.matrix[@as(c_int, 0)] >> @intCast(@as(c_int, 13))),
                            .x = @truncate(gmv.*.matrix[@as(c_int, 1)] >> @intCast(@as(c_int, 13))),
                        },
                    };
                    _ = &res;
                    if (@as(c_int, hdr.*.force_integer_mv) != 0) {
                        fix_int_mv_precision(&res);
                    }
                    return res;
                }
            },
            @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_IDENTITY) => {
                return mv{
                    .unnamed_0 = struct_unnamed_18{
                        .y = 0,
                        .x = 0,
                    },
                };
            },
        }
    }
}
pub extern fn antfly_av1_dav1d_film_grain_dsp_init_8bpc(c: [*c]Dav1dFilmGrainDSPContext) void;
pub extern fn antfly_av1_dav1d_film_grain_dsp_init_16bpc(c: [*c]Dav1dFilmGrainDSPContext) void;
pub extern fn antfly_av1_dav1d_intra_pred_dsp_init_8bpc(c: [*c]Dav1dIntraPredDSPContext) void;
pub extern fn antfly_av1_dav1d_intra_pred_dsp_init_16bpc(c: [*c]Dav1dIntraPredDSPContext) void;
pub extern fn antfly_av1_dav1d_itx_dsp_init_8bpc(c: [*c]Dav1dInvTxfmDSPContext, bpc: c_int) void;
pub extern fn antfly_av1_dav1d_itx_dsp_init_16bpc(c: [*c]Dav1dInvTxfmDSPContext, bpc: c_int) void;
pub extern fn antfly_av1_dav1d_create_lf_mask_intra(lflvl: [*c]Av1Filter, level_cache: [*c][4]u8, b4_stride: ptrdiff_t, level: [*c][8][2]u8, bx: c_int, by: c_int, iw: c_int, ih: c_int, bs: enum_BlockSize, ytx: enum_RectTxfmSize, uvtx: enum_RectTxfmSize, layout: enum_Dav1dPixelLayout, ay: [*c]u8, ly: [*c]u8, auv: [*c]u8, luv: [*c]u8) void;
pub extern fn antfly_av1_dav1d_create_lf_mask_inter(lflvl: [*c]Av1Filter, level_cache: [*c][4]u8, b4_stride: ptrdiff_t, level: [*c][8][2]u8, bx: c_int, by: c_int, iw: c_int, ih: c_int, skip_inter: c_int, bs: enum_BlockSize, max_ytx: enum_RectTxfmSize, tx_mask: [*c]const u16, uvtx: enum_RectTxfmSize, layout: enum_Dav1dPixelLayout, ay: [*c]u8, ly: [*c]u8, auv: [*c]u8, luv: [*c]u8) void;
pub extern fn antfly_av1_dav1d_calc_eih(lim_lut: [*c]Av1FilterLUT, filter_sharpness: c_int) void;
pub extern fn antfly_av1_dav1d_calc_lf_values(values: [*c][4][8][2]u8, hdr: [*c]const Dav1dFrameHeader, lf_delta: [*c]const i8) void;
pub extern fn antfly_av1_dav1d_loop_filter_dsp_init_8bpc(c: [*c]Dav1dLoopFilterDSPContext) void;
pub extern fn antfly_av1_dav1d_loop_filter_dsp_init_16bpc(c: [*c]Dav1dLoopFilterDSPContext) void;
pub extern fn antfly_av1_dav1d_loop_restoration_dsp_init_8bpc(c: [*c]Dav1dLoopRestorationDSPContext, bpc: c_int) void;
pub extern fn antfly_av1_dav1d_loop_restoration_dsp_init_16bpc(c: [*c]Dav1dLoopRestorationDSPContext, bpc: c_int) void;
pub extern fn antfly_av1_dav1d_mc_dsp_init_8bpc(c: [*c]Dav1dMCDSPContext) void;
pub extern fn antfly_av1_dav1d_mc_dsp_init_16bpc(c: [*c]Dav1dMCDSPContext) void;
pub extern fn antfly_av1_dav1d_msac_init(s: [*c]MsacContext, data: [*c]const u8, sz: usize, disable_cdf_update_flag: c_int) void;
pub extern fn antfly_av1_dav1d_msac_decode_symbol_adapt_c(s: [*c]MsacContext, cdf: [*c]u16, n_symbols: usize) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_bool_adapt_c(s: [*c]MsacContext, cdf: [*c]u16) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_bool_equi_c(s: [*c]MsacContext) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_bool_c(s: [*c]MsacContext, f: c_uint) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_subexp(s: [*c]MsacContext, ref: c_int, n: c_int, k: c_uint) c_int;
pub fn dav1d_msac_decode_bools(s: [*c]MsacContext, arg_n: c_uint) callconv(.c) c_uint {
    _ = &s;
    var n = arg_n;
    _ = &n;
    var v: c_uint = 0;
    _ = &v;
    while ((blk: {
        const ref = &n;
        const tmp = ref.*;
        ref.* -%= 1;
        break :blk tmp;
    }) != 0) {
        v = (v << @intCast(@as(c_uint, 1))) | antfly_av1_dav1d_msac_decode_bool_equi_c(s);
    }
    return v;
}
pub fn dav1d_msac_decode_uniform(s: [*c]MsacContext, n: c_uint) callconv(.c) c_int {
    _ = &s;
    _ = &n;
    while (true) {
        if (!(n > @as(c_uint, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const l: c_int = ulog2(n) + @as(c_int, 1);
    _ = &l;
    while (true) {
        if (!(l > @as(c_int, 1))) {
            unreachable;
        }
        if (!false) break;
    }
    const m: c_uint = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(l)))) -% n;
    _ = &m;
    const v: c_uint = dav1d_msac_decode_bools(s, @bitCast(@as(c_int, l - @as(c_int, 1))));
    _ = &v;
    return @bitCast(@as(c_uint, @truncate(if (v < m) v else ((v << @intCast(@as(c_uint, 1))) -% m) +% antfly_av1_dav1d_msac_decode_bool_equi_c(s))));
}
pub extern fn antfly_av1_dav1d_thread_picture_alloc(c: [*c]Dav1dContext, f: [*c]Dav1dFrameContext, bpc: c_int) c_int;
pub extern fn antfly_av1_dav1d_picture_alloc_copy(c: [*c]Dav1dContext, dst: [*c]Dav1dPicture, w: c_int, src: [*c]const Dav1dPicture) c_int;
pub extern fn antfly_av1_dav1d_picture_ref(dst: [*c]Dav1dPicture, src: [*c]const Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_thread_picture_ref(dst: [*c]Dav1dThreadPicture, src: [*c]const Dav1dThreadPicture) void;
pub extern fn antfly_av1_dav1d_thread_picture_unref(p: [*c]Dav1dThreadPicture) void;
pub extern fn antfly_av1_dav1d_picture_unref_internal(p: [*c]Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_picture_get_event_flags(p: [*c]const Dav1dThreadPicture) enum_Dav1dEventFlags;
pub extern fn antfly_av1_dav1d_recon_b_intra_8bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, intra_edge_flags: enum_EdgeFlags, b: [*c]const Av1Block) void;
pub extern fn antfly_av1_dav1d_recon_b_intra_16bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, intra_edge_flags: enum_EdgeFlags, b: [*c]const Av1Block) void;
pub extern fn antfly_av1_dav1d_recon_b_inter_8bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) c_int;
pub extern fn antfly_av1_dav1d_recon_b_inter_16bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) c_int;
pub extern fn antfly_av1_dav1d_filter_sbrow_8bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_deblock_cols_8bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_deblock_cols_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_deblock_rows_8bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_deblock_rows_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_cdef_8bpc(tc: [*c]Dav1dTaskContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_cdef_16bpc(tc: [*c]Dav1dTaskContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_resize_8bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_resize_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_lr_8bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_filter_sbrow_lr_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void;
pub extern fn antfly_av1_dav1d_backup_ipred_edge_8bpc(t: [*c]Dav1dTaskContext) void;
pub extern fn antfly_av1_dav1d_backup_ipred_edge_16bpc(t: [*c]Dav1dTaskContext) void;
pub extern fn antfly_av1_dav1d_read_coef_blocks_8bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) void;
pub extern fn antfly_av1_dav1d_read_coef_blocks_16bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) void;
pub extern fn antfly_av1_dav1d_copy_pal_block_y_8bpc(t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) void;
pub extern fn antfly_av1_dav1d_copy_pal_block_y_16bpc(t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) void;
pub extern fn antfly_av1_dav1d_copy_pal_block_uv_8bpc(t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) void;
pub extern fn antfly_av1_dav1d_copy_pal_block_uv_16bpc(t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) void;
pub extern fn antfly_av1_dav1d_read_pal_plane_8bpc(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, pl: c_int, sz_ctx: c_int, bx4: c_int, by4: c_int) void;
pub extern fn antfly_av1_dav1d_read_pal_plane_16bpc(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, pl: c_int, sz_ctx: c_int, bx4: c_int, by4: c_int) void;
pub extern fn antfly_av1_dav1d_read_pal_uv_8bpc(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, sz_ctx: c_int, bx4: c_int, by4: c_int) void;
pub extern fn antfly_av1_dav1d_read_pal_uv_16bpc(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, sz_ctx: c_int, bx4: c_int, by4: c_int) void;
pub extern var antfly_av1_dav1d_dq_tbl: [3][256][2]u16;
pub extern var antfly_av1_dav1d_qm_tbl: [16][2][19][*c]const u8;
pub extern fn antfly_av1_dav1d_task_create_tile_sbrow(f: [*c]Dav1dFrameContext, pass: c_int, cond_signal: c_int) c_int;
pub extern fn antfly_av1_dav1d_task_frame_init(f: [*c]Dav1dFrameContext) void;
pub extern fn antfly_av1_dav1d_get_shear_params(wm: [*c]Dav1dWarpedMotionParams) c_int;
pub extern fn antfly_av1_dav1d_find_affine_int(pts: [*c][2][2]c_int, np: c_int, bw4: c_int, bh4: c_int, mv: mv, wm: [*c]Dav1dWarpedMotionParams, bx: c_int, by: c_int) c_int;
pub extern fn antfly_av1_dav1d_set_affine_mv2d(bw4: c_int, bh4: c_int, mv: mv, wm: [*c]Dav1dWarpedMotionParams, bx: c_int, by: c_int) void;
pub fn init_quant_tables(seq_hdr: [*c]const Dav1dSequenceHeader, frame_hdr: [*c]const Dav1dFrameHeader, qidx: c_int, arg_dq: [*c][3][2]u16) callconv(.c) void {
    _ = &seq_hdr;
    _ = &frame_hdr;
    _ = &qidx;
    var dq = arg_dq;
    _ = &dq;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < (if (frame_hdr.*.segmentation.enabled != 0) @as(c_int, 8) else @as(c_int, 1))) : (i += 1) {
            const yac: c_int = if (frame_hdr.*.segmentation.enabled != 0) iclip_u8(qidx + @as(c_int, frame_hdr.*.segmentation.seg_data.d[@bitCast(@as(isize, @intCast(i)))].delta_q)) else qidx;
            _ = &yac;
            const ydc: c_int = iclip_u8(yac + @as(c_int, frame_hdr.*.quant.ydc_delta));
            _ = &ydc;
            const uac: c_int = iclip_u8(yac + @as(c_int, frame_hdr.*.quant.uac_delta));
            _ = &uac;
            const udc: c_int = iclip_u8(yac + @as(c_int, frame_hdr.*.quant.udc_delta));
            _ = &udc;
            const vac: c_int = iclip_u8(yac + @as(c_int, frame_hdr.*.quant.vac_delta));
            _ = &vac;
            const vdc: c_int = iclip_u8(yac + @as(c_int, frame_hdr.*.quant.vdc_delta));
            _ = &vdc;
            dq[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)][@as(c_int, 0)] = antfly_av1_dav1d_dq_tbl[seq_hdr.*.hbd][@bitCast(@as(isize, @intCast(ydc)))][@as(c_int, 0)];
            dq[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)][@as(c_int, 1)] = antfly_av1_dav1d_dq_tbl[seq_hdr.*.hbd][@bitCast(@as(isize, @intCast(yac)))][@as(c_int, 1)];
            dq[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)][@as(c_int, 0)] = antfly_av1_dav1d_dq_tbl[seq_hdr.*.hbd][@bitCast(@as(isize, @intCast(udc)))][@as(c_int, 0)];
            dq[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)][@as(c_int, 1)] = antfly_av1_dav1d_dq_tbl[seq_hdr.*.hbd][@bitCast(@as(isize, @intCast(uac)))][@as(c_int, 1)];
            dq[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 2)][@as(c_int, 0)] = antfly_av1_dav1d_dq_tbl[seq_hdr.*.hbd][@bitCast(@as(isize, @intCast(vdc)))][@as(c_int, 0)];
            dq[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 2)][@as(c_int, 1)] = antfly_av1_dav1d_dq_tbl[seq_hdr.*.hbd][@bitCast(@as(isize, @intCast(vac)))][@as(c_int, 1)];
        }
    }
}
pub fn read_mv_component_diff(msac: [*c]MsacContext, mv_comp: [*c]CdfMvComponent, mv_prec: c_int) callconv(.c) c_int {
    _ = &msac;
    _ = &mv_comp;
    _ = &mv_prec;
    const sign: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.sign))))));
    _ = &sign;
    const cl: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.classes)), 10))));
    _ = &cl;
    var up: c_int = undefined;
    _ = &up;
    var fp: c_int = 3;
    _ = &fp;
    var hp: c_int = 1;
    _ = &hp;
    if (!(cl != 0)) {
        up = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.class0))))));
        if (mv_prec >= @as(c_int, 0)) {
            fp = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.class0_fp[@bitCast(@as(isize, @intCast(up)))])), 3))));
            if (mv_prec > @as(c_int, 0)) {
                hp = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.class0_hp))))));
            }
        }
    } else {
        up = @as(c_int, 1) << @intCast(cl);
        {
            var n: c_int = 0;
            _ = &n;
            while (n < cl) : (n += 1) {
                {
                    const ref = &up;
                    ref.* = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, ref.*))) | (antfly_av1_dav1d_msac_decode_bool_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.classN[@bitCast(@as(isize, @intCast(n)))]))) << @intCast(@as(c_uint, @bitCast(@as(c_int, n))))))));
                }
            }
        }
        if (mv_prec >= @as(c_int, 0)) {
            fp = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.classN_fp)), 3))));
            if (mv_prec > @as(c_int, 0)) {
                hp = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(msac, @ptrCast(@alignCast(&mv_comp.*.classN_hp))))));
            }
        }
    }
    const diff: c_int = (((up << @intCast(@as(c_int, 3))) | (fp << @intCast(@as(c_int, 1)))) | hp) + @as(c_int, 1);
    _ = &diff;
    return if (sign != 0) -diff else diff;
}
pub fn read_mv_residual(ts: [*c]Dav1dTileState, ref_mv: [*c]mv, mv_prec: c_int) callconv(.c) void {
    _ = &ts;
    _ = &ref_mv;
    _ = &mv_prec;
    const msac: [*c]MsacContext = &ts.*.msac;
    _ = &msac;
    const mv_joint: enum_MVJoint = antfly_av1_dav1d_msac_decode_symbol_adapt_c(msac, @ptrCast(@alignCast(&ts.*.cdf.mv.joint)), @bitCast(@as(c_long, N_MV_JOINTS - @as(c_int, 1))));
    _ = &mv_joint;
    if ((mv_joint & @as(enum_MVJoint, MV_JOINT_V)) != 0) {
        {
            const ref = &ref_mv.*.unnamed_0.y;
            ref.* = @truncate(@as(c_int, ref.*) + read_mv_component_diff(msac, &ts.*.cdf.mv.comp[@as(c_int, 0)], mv_prec));
        }
    }
    if ((mv_joint & @as(enum_MVJoint, MV_JOINT_H)) != 0) {
        {
            const ref = &ref_mv.*.unnamed_0.x;
            ref.* = @truncate(@as(c_int, ref.*) + read_mv_component_diff(msac, &ts.*.cdf.mv.comp[@as(c_int, 1)], mv_prec));
        }
    }
}
pub fn read_tx_tree(t: [*c]Dav1dTaskContext, from: enum_RectTxfmSize, depth: c_int, masks: [*c]u16, x_off: c_int, y_off: c_int) callconv(.c) void {
    _ = &t;
    _ = &from;
    _ = &depth;
    _ = &masks;
    _ = &x_off;
    _ = &y_off;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const bx4: c_int = t.*.bx & @as(c_int, 31);
    _ = &bx4;
    const by4: c_int = t.*.by & @as(c_int, 31);
    _ = &by4;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[from];
    _ = &t_dim;
    const txw: c_int = t_dim.*.lw;
    _ = &txw;
    const txh: c_int = t_dim.*.lh;
    _ = &txh;
    var is_split: c_int = undefined;
    _ = &is_split;
    if ((depth < @as(c_int, 2)) and (from > @as(enum_RectTxfmSize, @bitCast(TX_4X4)))) {
        const cat: c_int = (@as(c_int, 2) * (TX_64X64 - @as(c_int, t_dim.*.max))) - depth;
        _ = &cat;
        const a: c_int = @as(c_int, @intFromBool(@as(c_int, t.*.a.*.tx[@bitCast(@as(isize, @intCast(bx4)))]) < txw));
        _ = &a;
        const l: c_int = @as(c_int, @intFromBool(@as(c_int, t.*.l.tx[@bitCast(@as(isize, @intCast(by4)))]) < txh));
        _ = &l;
        is_split = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&t.*.ts.*.msac, @ptrCast(@alignCast(&t.*.ts.*.cdf.m.txpart[@bitCast(@as(isize, @intCast(cat)))][@bitCast(@as(isize, @intCast(a + l)))]))))));
        if (is_split != 0) {
            {
                const ref = &masks[@bitCast(@as(isize, @intCast(depth)))];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) | (@as(c_int, 1) << @intCast((y_off * @as(c_int, 4)) + x_off)))));
            }
        }
    } else {
        is_split = 0;
    }
    if ((is_split != 0) and (@as(c_int, t_dim.*.max) > TX_8X8)) {
        const sub: enum_RectTxfmSize = t_dim.*.sub;
        _ = &sub;
        const sub_t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[sub];
        _ = &sub_t_dim;
        const txsw: c_int = sub_t_dim.*.w;
        _ = &txsw;
        const txsh: c_int = sub_t_dim.*.h;
        _ = &txsh;
        read_tx_tree(t, sub, depth + @as(c_int, 1), masks, (x_off * @as(c_int, 2)) + @as(c_int, 0), (y_off * @as(c_int, 2)) + @as(c_int, 0));
        t.*.bx += txsw;
        if ((txw >= txh) and (t.*.bx < f.*.bw)) {
            read_tx_tree(t, sub, depth + @as(c_int, 1), masks, (x_off * @as(c_int, 2)) + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 0));
        }
        t.*.bx -= txsw;
        t.*.by += txsh;
        if ((txh >= txw) and (t.*.by < f.*.bh)) {
            read_tx_tree(t, sub, depth + @as(c_int, 1), masks, (x_off * @as(c_int, 2)) + @as(c_int, 0), (y_off * @as(c_int, 2)) + @as(c_int, 1));
            t.*.bx += txsw;
            if ((txw >= txh) and (t.*.bx < f.*.bw)) {
                read_tx_tree(t, sub, depth + @as(c_int, 1), masks, (x_off * @as(c_int, 2)) + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 1));
            }
            t.*.bx -= txsw;
        }
        t.*.by -= txsh;
    } else {
        antfly_av1_dav1d_memset_pow2[t_dim.*.lw].?(@ptrCast(@alignCast(&t.*.a.*.tx[@bitCast(@as(isize, @intCast(bx4)))])), if (is_split != 0) TX_4X4 else txw);
        antfly_av1_dav1d_memset_pow2[t_dim.*.lh].?(@ptrCast(@alignCast(&t.*.l.tx[@bitCast(@as(isize, @intCast(by4)))])), if (is_split != 0) TX_4X4 else txh);
    }
}
pub fn neg_deinterleave(arg_diff: c_int, arg_ref: c_int, arg_max: c_int) callconv(.c) c_int {
    var diff = arg_diff;
    _ = &diff;
    var ref = arg_ref;
    _ = &ref;
    var max = arg_max;
    _ = &max;
    if (!(ref != 0)) return diff;
    if (ref >= (max - @as(c_int, 1))) return (max - diff) - @as(c_int, 1);
    if ((@as(c_int, 2) * ref) < max) {
        if (diff <= (@as(c_int, 2) * ref)) {
            if ((diff & @as(c_int, 1)) != 0) return ref + ((diff + @as(c_int, 1)) >> @intCast(@as(c_int, 1))) else return ref - (diff >> @intCast(@as(c_int, 1)));
        }
        return diff;
    } else {
        if (diff <= (@as(c_int, 2) * ((max - ref) - @as(c_int, 1)))) {
            if ((diff & @as(c_int, 1)) != 0) return ref + ((diff + @as(c_int, 1)) >> @intCast(@as(c_int, 1))) else return ref - (diff >> @intCast(@as(c_int, 1)));
        }
        return max - (diff + @as(c_int, 1));
    }
}
pub fn find_matching_ref(t: [*c]const Dav1dTaskContext, intra_edge_flags: enum_EdgeFlags, bw4: c_int, bh4: c_int, w4: c_int, h4: c_int, have_left: c_int, have_top: c_int, ref: c_int, arg_masks: [*c]u64) callconv(.c) void {
    _ = &t;
    _ = &intra_edge_flags;
    _ = &bw4;
    _ = &bh4;
    _ = &w4;
    _ = &h4;
    _ = &have_left;
    _ = &have_top;
    _ = &ref;
    var masks = arg_masks;
    _ = &masks;
    var r: [*c]const [*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
    _ = &r;
    var count: c_int = 0;
    _ = &count;
    var have_topleft: c_int = @as(c_int, @intFromBool((have_top != 0) and (have_left != 0)));
    _ = &have_topleft;
    var have_topright: c_int = @as(c_int, @intFromBool((((imax(bw4, bh4) < @as(c_int, 32)) and (have_top != 0)) and ((t.*.bx + bw4) < t.*.ts.*.tiling.col_end)) and (@as(c_int, @bitCast(intra_edge_flags & @as(enum_EdgeFlags, EDGE_I444_TOP_HAS_RIGHT))) != 0)));
    _ = &have_topright;
    if (have_top != 0) {
        var r2: [*c]const refmvs_block = &r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))];
        _ = &r2;
        if ((@as(c_int, r2.*.ref.ref[@as(c_int, 0)]) == (ref + @as(c_int, 1))) and (@as(c_int, r2.*.ref.ref[@as(c_int, 1)]) == -@as(c_int, 1))) {
            masks[@as(c_int, 0)] |= 1;
            count = 1;
        }
        var aw4: c_int = antfly_av1_dav1d_block_dimensions[r2.*.bs][@as(c_int, 0)];
        _ = &aw4;
        if (aw4 >= bw4) {
            const off: c_int = t.*.bx & (aw4 - @as(c_int, 1));
            _ = &off;
            if (off != 0) {
                have_topleft = 0;
            }
            if ((aw4 - off) > bw4) {
                have_topright = 0;
            }
        } else {
            var mask: c_uint = @bitCast(@as(c_int, @as(c_int, 1) << @intCast(aw4)));
            _ = &mask;
            {
                var x: c_int = aw4;
                _ = &x;
                while (x < w4) : (x += aw4) {
                    r2 += @as(usize, @bitCast(@as(isize, @intCast(aw4))));
                    if ((@as(c_int, r2.*.ref.ref[@as(c_int, 0)]) == (ref + @as(c_int, 1))) and (@as(c_int, r2.*.ref.ref[@as(c_int, 1)]) == -@as(c_int, 1))) {
                        masks[@as(c_int, 0)] |= mask;
                        if ((blk: {
                            const ref_1 = &count;
                            ref_1.* += 1;
                            break :blk ref_1.*;
                        }) >= @as(c_int, 8)) return;
                    }
                    aw4 = antfly_av1_dav1d_block_dimensions[r2.*.bs][@as(c_int, 0)];
                    mask <<= @intCast(@as(c_uint, @bitCast(@as(c_int, aw4))));
                }
            }
        }
    }
    if (have_left != 0) {
        var r2: [*c]const [*c]refmvs_block = r;
        _ = &r2;
        if ((@as(c_int, (&r2[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.ref.ref[@as(c_int, 0)]) == (ref + @as(c_int, 1))) and (@as(c_int, (&r2[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.ref.ref[@as(c_int, 1)]) == -@as(c_int, 1))) {
            masks[@as(c_int, 1)] |= 1;
            if ((blk: {
                const ref_1 = &count;
                ref_1.* += 1;
                break :blk ref_1.*;
            }) >= @as(c_int, 8)) return;
        }
        var lh4: c_int = antfly_av1_dav1d_block_dimensions[(&r2[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 1)];
        _ = &lh4;
        if (lh4 >= bh4) {
            if ((t.*.by & (lh4 - @as(c_int, 1))) != 0) {
                have_topleft = 0;
            }
        } else {
            var mask: c_uint = @bitCast(@as(c_int, @as(c_int, 1) << @intCast(lh4)));
            _ = &mask;
            {
                var y: c_int = lh4;
                _ = &y;
                while (y < h4) : (y += lh4) {
                    r2 += @as(usize, @bitCast(@as(isize, @intCast(lh4))));
                    if ((@as(c_int, (&r2[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.ref.ref[@as(c_int, 0)]) == (ref + @as(c_int, 1))) and (@as(c_int, (&r2[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.ref.ref[@as(c_int, 1)]) == -@as(c_int, 1))) {
                        masks[@as(c_int, 1)] |= mask;
                        if ((blk: {
                            const ref_1 = &count;
                            ref_1.* += 1;
                            break :blk ref_1.*;
                        }) >= @as(c_int, 8)) return;
                    }
                    lh4 = antfly_av1_dav1d_block_dimensions[(&r2[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 1)];
                    mask <<= @intCast(@as(c_uint, @bitCast(@as(c_int, lh4))));
                }
            }
        }
    }
    if ((have_topleft != 0) and ((@as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.ref.ref[@as(c_int, 0)]) == (ref + @as(c_int, 1))) and (@as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.ref.ref[@as(c_int, 1)]) == -@as(c_int, 1)))) {
        masks[@as(c_int, 1)] |= @as(c_ulonglong, 1) << @intCast(@as(c_ulonglong, 32));
        if ((blk: {
            const ref_1 = &count;
            ref_1.* += 1;
            break :blk ref_1.*;
        }) >= @as(c_int, 8)) return;
    }
    if ((have_topright != 0) and ((@as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx + bw4)))]).*.ref.ref[@as(c_int, 0)]) == (ref + @as(c_int, 1))) and (@as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx + bw4)))]).*.ref.ref[@as(c_int, 1)]) == -@as(c_int, 1)))) {
        masks[@as(c_int, 0)] |= @as(c_ulonglong, 1) << @intCast(@as(c_ulonglong, 32));
    }
}
pub fn derive_warpmv(t: [*c]const Dav1dTaskContext, bw4: c_int, bh4: c_int, arg_masks: [*c]const u64, mv_1: union_mv, wmp: [*c]Dav1dWarpedMotionParams) callconv(.c) void {
    _ = &t;
    _ = &bw4;
    _ = &bh4;
    var masks = arg_masks;
    _ = &masks;
    _ = &mv_1;
    _ = &wmp;
    var pts: [8][2][2]c_int = undefined;
    _ = &pts;
    var np: c_int = 0;
    _ = &np;
    var r: [*c]const [*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
    _ = &r;
    if ((@as(c_uint, @truncate(masks[@as(c_int, 0)])) == @as(c_uint, 1)) and !((masks[@as(c_int, 1)] >> @intCast(@as(u64, 32))) != 0)) {
        const off: c_int = t.*.bx & (@as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))]).*.bs][@as(c_int, 0)]) - @as(c_int, 1));
        _ = &off;
        while (true) {
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] = (@as(c_int, 16) * ((@as(c_int, 2) * -off) + (@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))]).*.bs][@as(c_int, 0)])))) - @as(c_int, 8);
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))]).*.bs][@as(c_int, 1)])))) - @as(c_int, 8);
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 0)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))]).*.mv.mv[@as(c_int, 0)].unnamed_0.x);
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 1)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))]).*.mv.mv[@as(c_int, 0)].unnamed_0.y);
            np += 1;
            if (!false) break;
        }
    } else {
        var off: c_uint = 0;
        _ = &off;
        var xmask: c_uint = @truncate(masks[@as(c_int, 0)]);
        _ = &xmask;
        while ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(np < @as(c_int, 8)))))) != 0) and (xmask != 0)) {
            const tz: c_int = ctz(xmask);
            _ = &tz;
            off +%= @bitCast(@as(c_int, tz));
            xmask >>= @intCast(@as(c_uint, @bitCast(@as(c_int, tz))));
            while (true) {
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] = @bitCast(@as(c_uint, @truncate((@as(c_uint, 16) *% ((@as(c_uint, 2) *% off) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@as(c_uint, @bitCast(@as(c_int, t.*.bx))) +% off]).*.bs][@as(c_int, 0)])))))) -% @as(c_uint, 8))));
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@as(c_uint, @bitCast(@as(c_int, t.*.bx))) +% off]).*.bs][@as(c_int, 1)])))) - @as(c_int, 8);
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 0)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@as(c_uint, @bitCast(@as(c_int, t.*.bx))) +% off]).*.mv.mv[@as(c_int, 0)].unnamed_0.x);
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 1)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@as(c_uint, @bitCast(@as(c_int, t.*.bx))) +% off]).*.mv.mv[@as(c_int, 0)].unnamed_0.y);
                np += 1;
                if (!false) break;
            }
            xmask &= @bitCast(@as(c_int, ~@as(c_int, 1)));
        }
    }
    if ((np < @as(c_int, 8)) and (masks[@as(c_int, 1)] == @as(u64, 1))) {
        const off: c_int = t.*.by & (@as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 1)]) - @as(c_int, 1));
        _ = &off;
        while (true) {
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-off)))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 0)])))) - @as(c_int, 8);
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] = (@as(c_int, 16) * ((@as(c_int, 2) * -off) + (@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-off)))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 1)])))) - @as(c_int, 8);
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 0)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-off)))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.mv.mv[@as(c_int, 0)].unnamed_0.x);
            pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 1)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-off)))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.mv.mv[@as(c_int, 0)].unnamed_0.y);
            np += 1;
            if (!false) break;
        }
    } else {
        var off: c_uint = 0;
        _ = &off;
        var ymask: c_uint = @truncate(masks[@as(c_int, 1)]);
        _ = &ymask;
        while ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(np < @as(c_int, 8)))))) != 0) and (ymask != 0)) {
            const tz: c_int = ctz(ymask);
            _ = &tz;
            off +%= @bitCast(@as(c_int, tz));
            ymask >>= @intCast(@as(c_uint, @bitCast(@as(c_int, tz))));
            while (true) {
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[off][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 0)])))) - @as(c_int, 8);
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] = @bitCast(@as(c_uint, @truncate((@as(c_uint, 16) *% ((@as(c_uint, 2) *% off) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[off][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 1)])))))) -% @as(c_uint, 8))));
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 0)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] + @as(c_int, (&r[off][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.mv.mv[@as(c_int, 0)].unnamed_0.x);
                pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 1)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] + @as(c_int, (&r[off][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.mv.mv[@as(c_int, 0)].unnamed_0.y);
                np += 1;
                if (!false) break;
            }
            ymask &= @bitCast(@as(c_int, ~@as(c_int, 1)));
        }
    }
    if ((@as(u64, @bitCast(@as(c_longlong, @as(c_int, @intFromBool(np < @as(c_int, 8)))))) != 0) and ((masks[@as(c_int, 1)] >> @intCast(@as(u64, 32))) != 0)) while (true) {
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 0)])))) - @as(c_int, 8);
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.bs][@as(c_int, 1)])))) - @as(c_int, 8);
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 0)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.mv.mv[@as(c_int, 0)].unnamed_0.x);
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 1)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))]).*.mv.mv[@as(c_int, 0)].unnamed_0.y);
        np += 1;
        if (!false) break;
    };
    if ((@as(u64, @bitCast(@as(c_longlong, @as(c_int, @intFromBool(np < @as(c_int, 8)))))) != 0) and ((masks[@as(c_int, 0)] >> @intCast(@as(u64, 32))) != 0)) while (true) {
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] = (@as(c_int, 16) * ((@as(c_int, 2) * bw4) + (@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx + bw4)))]).*.bs][@as(c_int, 0)])))) - @as(c_int, 8);
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] = (@as(c_int, 16) * ((@as(c_int, 2) * @as(c_int, 0)) + (-@as(c_int, 1) * @as(c_int, antfly_av1_dav1d_block_dimensions[(&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx + bw4)))]).*.bs][@as(c_int, 1)])))) - @as(c_int, 8);
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 0)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 0)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx + bw4)))]).*.mv.mv[@as(c_int, 0)].unnamed_0.x);
        pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 1)][@as(c_int, 1)] = pts[@bitCast(@as(isize, @intCast(np)))][@as(c_int, 0)][@as(c_int, 1)] + @as(c_int, (&r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx + bw4)))]).*.mv.mv[@as(c_int, 0)].unnamed_0.y);
        np += 1;
        if (!false) break;
    };
    while (true) {
        if (!((np > @as(c_int, 0)) and (np <= @as(c_int, 8)))) {
            unreachable;
        }
        if (!false) break;
    }
    var mvd: [8]c_int = undefined;
    _ = &mvd;
    var ret: c_int = 0;
    _ = &ret;
    const thresh: c_int = @as(c_int, 4) * iclip(imax(bw4, bh4), 4, 28);
    _ = &thresh;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < np) : (i += 1) {
            mvd[@bitCast(@as(isize, @intCast(i)))] = antfly_codec_abs((pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)][@as(c_int, 0)] - pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)][@as(c_int, 0)]) - @as(c_int, mv_1.unnamed_0.x)) + antfly_codec_abs((pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)][@as(c_int, 1)] - pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)][@as(c_int, 1)]) - @as(c_int, mv_1.unnamed_0.y));
            if (mvd[@bitCast(@as(isize, @intCast(i)))] > thresh) {
                mvd[@bitCast(@as(isize, @intCast(i)))] = -@as(c_int, 1);
            } else {
                ret += 1;
            }
        }
    }
    if (!(ret != 0)) {
        ret = 1;
    } else {
        var i: c_int = 0;
        _ = &i;
        var j: c_int = np - @as(c_int, 1);
        _ = &j;
        var k: c_int = 0;
        _ = &k;
        while (k < (np - ret)) : ({
            k += 1;
            i += 1;
            j -= 1;
        }) {
            while (mvd[@bitCast(@as(isize, @intCast(i)))] != -@as(c_int, 1)) {
                i += 1;
            }
            while (mvd[@bitCast(@as(isize, @intCast(j)))] == -@as(c_int, 1)) {
                j -= 1;
            }
            while (true) {
                if (!(i != j)) {
                    unreachable;
                }
                if (!false) break;
            }
            if (i > j) break;
            mvd[@bitCast(@as(isize, @intCast(i)))] = mvd[@bitCast(@as(isize, @intCast(j)))];
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][2]c_int, @ptrCast(@alignCast(&pts[@bitCast(@as(isize, @intCast(i)))]))))), @ptrCast(@alignCast(@as([*c][2]c_int, @ptrCast(@alignCast(&pts[@bitCast(@as(isize, @intCast(j)))]))))), @sizeOf(@TypeOf(@as([*c][2][2]c_int, @ptrCast(@alignCast(&pts))).*)));
        }
    }
    if (!(antfly_av1_dav1d_find_affine_int(@ptrCast(@alignCast(&pts)), ret, bw4, bh4, mv_1, wmp, t.*.bx, t.*.by) != 0) and !(antfly_av1_dav1d_get_shear_params(wmp) != 0)) {
        wmp.*.type = DAV1D_WM_TYPE_AFFINE;
    } else {
        wmp.*.type = DAV1D_WM_TYPE_IDENTITY;
    }
}
pub fn findoddzero(arg_buf: [*c]const u8, arg_len: c_int) callconv(.c) c_int {
    var buf = arg_buf;
    _ = &buf;
    var len = arg_len;
    _ = &len;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < len) : (n += 1) if (!(@as(c_int, buf[@bitCast(@as(isize, @intCast(n * @as(c_int, 2))))]) != 0)) return 1;
    }
    return 0;
}
pub fn order_palette(arg_pal_idx: [*c]const u8, stride: ptrdiff_t, i: c_int, first: c_int, last: c_int, order: [*c][8]u8, ctx: [*c]u8) callconv(.c) void {
    var pal_idx = arg_pal_idx;
    _ = &pal_idx;
    _ = &stride;
    _ = &i;
    _ = &first;
    _ = &last;
    _ = &order;
    _ = &ctx;
    var have_top: c_int = @as(c_int, @intFromBool(i > first));
    _ = &have_top;
    while (true) {
        if (!(pal_idx != null)) {
            unreachable;
        }
        if (!false) break;
    }
    pal_idx += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, first) + (@as(ptrdiff_t, i - first) * stride)))));
    {
        var j: c_int = first;
        _ = &j;
        var n: c_int = 0;
        _ = &n;
        while (j >= last) : ({
            have_top = 1;
            j -= 1;
            n += 1;
            pal_idx += @as(usize, @bitCast(@as(isize, @intCast(stride - @as(ptrdiff_t, 1)))));
        }) {
            const have_left: c_int = @as(c_int, @intFromBool(j > @as(c_int, 0)));
            _ = &have_left;
            while (true) {
                if (!((have_left != 0) or (have_top != 0))) {
                    unreachable;
                }
                if (!false) break;
            }
            var mask: c_uint = 0;
            _ = &mask;
            var o_idx: c_int = 0;
            _ = &o_idx;
            if (!(have_left != 0)) {
                ctx[@bitCast(@as(isize, @intCast(n)))] = 0;
                while (true) {
                    const v: c_int = pal_idx[@bitCast(@as(isize, @intCast(-stride)))];
                    _ = &v;
                    while (true) {
                        if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    order[@bitCast(@as(isize, @intCast(n)))][
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref = &o_idx;
                            const tmp = ref.*;
                            ref.* += 1;
                            break :blk tmp;
                        })))
                    ] = @bitCast(@as(i8, @truncate(v)));
                    mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                    if (!false) break;
                }
            } else if (!(have_top != 0)) {
                ctx[@bitCast(@as(isize, @intCast(n)))] = 0;
                while (true) {
                    const v: c_int = pal_idx[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
                    _ = &v;
                    while (true) {
                        if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    order[@bitCast(@as(isize, @intCast(n)))][
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref = &o_idx;
                            const tmp = ref.*;
                            ref.* += 1;
                            break :blk tmp;
                        })))
                    ] = @bitCast(@as(i8, @truncate(v)));
                    mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                    if (!false) break;
                }
            } else {
                const l: c_int = pal_idx[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
                _ = &l;
                const t: c_int = pal_idx[@bitCast(@as(isize, @intCast(-stride)))];
                _ = &t;
                const tl: c_int = pal_idx[@bitCast(@as(isize, @intCast(-(stride + @as(ptrdiff_t, 1)))))];
                _ = &tl;
                const same_t_l: c_int = @as(c_int, @intFromBool(t == l));
                _ = &same_t_l;
                const same_t_tl: c_int = @as(c_int, @intFromBool(t == tl));
                _ = &same_t_tl;
                const same_l_tl: c_int = @as(c_int, @intFromBool(l == tl));
                _ = &same_l_tl;
                const same_all: c_int = (same_t_l & same_t_tl) & same_l_tl;
                _ = &same_all;
                if (same_all != 0) {
                    ctx[@bitCast(@as(isize, @intCast(n)))] = 4;
                    while (true) {
                        const v: c_int = t;
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                } else if (same_t_l != 0) {
                    ctx[@bitCast(@as(isize, @intCast(n)))] = 3;
                    while (true) {
                        const v: c_int = t;
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                    while (true) {
                        const v: c_int = tl;
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                } else if ((same_t_tl | same_l_tl) != 0) {
                    ctx[@bitCast(@as(isize, @intCast(n)))] = 2;
                    while (true) {
                        const v: c_int = tl;
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                    while (true) {
                        const v: c_int = if (same_t_tl != 0) l else t;
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                } else {
                    ctx[@bitCast(@as(isize, @intCast(n)))] = 1;
                    while (true) {
                        const v: c_int = imin(t, l);
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                    while (true) {
                        const v: c_int = imax(t, l);
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                    while (true) {
                        const v: c_int = tl;
                        _ = &v;
                        while (true) {
                            if (!(@as(c_uint, @bitCast(@as(c_int, v))) < @as(c_uint, 8))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        order[@bitCast(@as(isize, @intCast(n)))][
                            @bitCast(@as(isize, @intCast(blk: {
                                const ref = &o_idx;
                                const tmp = ref.*;
                                ref.* += 1;
                                break :blk tmp;
                            })))
                        ] = @bitCast(@as(i8, @truncate(v)));
                        mask |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(v)));
                        if (!false) break;
                    }
                }
            }
            {
                var m: c_uint = 1;
                _ = &m;
                var bit: c_uint = 0;
                _ = &bit;
                while (m < @as(c_uint, 256)) : ({
                    m <<= @intCast(@as(c_uint, 1));
                    bit +%= 1;
                }) if (!((mask & m) != 0)) {
                    order[@bitCast(@as(isize, @intCast(n)))][
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref = &o_idx;
                            const tmp = ref.*;
                            ref.* += 1;
                            break :blk tmp;
                        })))
                    ] = @truncate(bit);
                };
            }
            while (true) {
                if (!(o_idx == @as(c_int, 8))) {
                    unreachable;
                }
                if (!false) break;
            }
        }
    }
}
pub fn read_pal_indices(t: [*c]Dav1dTaskContext, pal_idx: [*c]u8, pal_sz: c_int, pl: c_int, w4: c_int, h4: c_int, bw4: c_int, bh4: c_int) callconv(.c) void {
    _ = &t;
    _ = &pal_idx;
    _ = &pal_sz;
    _ = &pl;
    _ = &w4;
    _ = &h4;
    _ = &bw4;
    _ = &bh4;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const stride: ptrdiff_t = bw4 * @as(c_int, 4);
    _ = &stride;
    while (true) {
        if (!(pal_idx != null)) {
            unreachable;
        }
        if (!false) break;
    }
    const pal_tmp: [*c]u8 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.pal_idx_uv));
    _ = &pal_tmp;
    pal_tmp[@as(c_int, 0)] = @bitCast(@as(i8, @truncate(dav1d_msac_decode_uniform(&ts.*.msac, @bitCast(@as(c_int, pal_sz))))));
    const color_map_cdf: [*c][8]u16 = @ptrCast(@alignCast(&ts.*.cdf.m.color_map[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(pal_sz - @as(c_int, 2))))]));
    _ = &color_map_cdf;
    const order: [*c][8]u8 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_0.unnamed_0.pal_order));
    _ = &order;
    const ctx: [*c]u8 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_0.unnamed_0.pal_ctx));
    _ = &ctx;
    {
        var i: c_int = 1;
        _ = &i;
        while (i < ((@as(c_int, 4) * (w4 + h4)) - @as(c_int, 1))) : (i += 1) {
            const first: c_int = imin(i, (w4 * @as(c_int, 4)) - @as(c_int, 1));
            _ = &first;
            const last: c_int = imax(0, (i - (h4 * @as(c_int, 4))) + @as(c_int, 1));
            _ = &last;
            order_palette(pal_tmp, stride, i, first, last, order, ctx);
            {
                var j: c_int = first;
                _ = &j;
                var m: c_int = 0;
                _ = &m;
                while (j >= last) : ({
                    j -= 1;
                    m += 1;
                }) {
                    const color_idx: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&color_map_cdf[ctx[@bitCast(@as(isize, @intCast(m)))]])), @bitCast(@as(c_long, pal_sz - @as(c_int, 1)))))));
                    _ = &color_idx;
                    pal_tmp[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, i - j) * stride) + @as(ptrdiff_t, j))))] = order[@bitCast(@as(isize, @intCast(m)))][@bitCast(@as(isize, @intCast(color_idx)))];
                }
            }
        }
    }
    t.*.c.*.pal_dsp.pal_idx_finish.?(pal_idx, pal_tmp, bw4 * @as(c_int, 4), bh4 * @as(c_int, 4), w4 * @as(c_int, 4), h4 * @as(c_int, 4));
}
pub fn read_vartx_tree(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, bs: enum_BlockSize, bx4: c_int, by4: c_int) callconv(.c) void {
    _ = &t;
    _ = &b;
    _ = &bs;
    _ = &bx4;
    _ = &by4;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    const bw4: c_int = b_dim[@as(c_int, 0)];
    _ = &bw4;
    const bh4: c_int = b_dim[@as(c_int, 1)];
    _ = &bh4;
    var tx_split: [2]u16 = [1]u16{
        0,
    } ++ @as([1]u16, @splat(0));
    _ = &tx_split;
    b.*.unnamed_0.unnamed_1.max_ytx = antfly_av1_dav1d_max_txfm_size_for_bs[bs][@as(c_int, 0)];
    if (!(@as(c_int, b.*.skip) != 0) and ((@as(c_int, f.*.frame_hdr.*.segmentation.lossless[b.*.seg_id]) != 0) or (@as(c_int, b.*.unnamed_0.unnamed_1.max_ytx) == TX_4X4))) {
        b.*.unnamed_0.unnamed_1.max_ytx = blk: {
            const tmp = @as(u8, TX_4X4);
            b.*.uvtx = tmp;
            break :blk tmp;
        };
        if (f.*.frame_hdr.*.txfm_mode == @as(enum_Dav1dTxfmMode, DAV1D_TX_SWITCHABLE)) {
            antfly_av1_dav1d_memset_pow2[b_dim[@as(c_int, 2)]].?(@ptrCast(@alignCast(&t.*.a.*.tx[@bitCast(@as(isize, @intCast(bx4)))])), TX_4X4);
            antfly_av1_dav1d_memset_pow2[b_dim[@as(c_int, 3)]].?(@ptrCast(@alignCast(&t.*.l.tx[@bitCast(@as(isize, @intCast(by4)))])), TX_4X4);
        }
    } else if ((f.*.frame_hdr.*.txfm_mode != @as(enum_Dav1dTxfmMode, DAV1D_TX_SWITCHABLE)) or (@as(c_int, b.*.skip) != 0)) {
        if (f.*.frame_hdr.*.txfm_mode == @as(enum_Dav1dTxfmMode, DAV1D_TX_SWITCHABLE)) {
            antfly_av1_dav1d_memset_pow2[b_dim[@as(c_int, 2)]].?(@ptrCast(@alignCast(&t.*.a.*.tx[@bitCast(@as(isize, @intCast(bx4)))])), b_dim[@as(c_int, 2) + @as(c_int, 0)]);
            antfly_av1_dav1d_memset_pow2[b_dim[@as(c_int, 3)]].?(@ptrCast(@alignCast(&t.*.l.tx[@bitCast(@as(isize, @intCast(by4)))])), b_dim[@as(c_int, 2) + @as(c_int, 1)]);
        }
        b.*.uvtx = antfly_av1_dav1d_max_txfm_size_for_bs[bs][f.*.cur.p.layout];
    } else {
        while (true) {
            if (!(((bw4 <= @as(c_int, 16)) or (bh4 <= @as(c_int, 16))) or (@as(c_int, b.*.unnamed_0.unnamed_1.max_ytx) == TX_64X64))) {
                unreachable;
            }
            if (!false) break;
        }
        var y: c_int = undefined;
        _ = &y;
        var x: c_int = undefined;
        _ = &x;
        var y_off: c_int = undefined;
        _ = &y_off;
        var x_off: c_int = undefined;
        _ = &x_off;
        const ytx: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[b.*.unnamed_0.unnamed_1.max_ytx];
        _ = &ytx;
        {
            y = 0;
            y_off = 0;
            while (y < bh4) : ({
                y += ytx.*.h;
                y_off += 1;
            }) {
                {
                    x = 0;
                    x_off = 0;
                    while (x < bw4) : ({
                        x += ytx.*.w;
                        x_off += 1;
                    }) {
                        read_tx_tree(t, b.*.unnamed_0.unnamed_1.max_ytx, 0, @ptrCast(@alignCast(&tx_split)), x_off, y_off);
                        t.*.bx += ytx.*.w;
                    }
                }
                t.*.bx -= x;
                t.*.by += ytx.*.h;
            }
        }
        t.*.by -= y;
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
        b.*.uvtx = antfly_av1_dav1d_max_txfm_size_for_bs[bs][f.*.cur.p.layout];
    }
    while (true) {
        if (!!((@as(c_int, tx_split[@as(c_int, 0)]) & ~@as(c_int, 51)) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    b.*.unnamed_0.unnamed_1.tx_split0 = @truncate(tx_split[@as(c_int, 0)]);
    b.*.unnamed_0.unnamed_1.tx_split1 = tx_split[@as(c_int, 1)];
}
pub fn get_prev_frame_segid(f: [*c]const Dav1dFrameContext, by: c_int, bx: c_int, w4: c_int, arg_h4: c_int, arg_ref_seg_map: [*c]const u8, stride: ptrdiff_t) callconv(.c) c_uint {
    _ = &f;
    _ = &by;
    _ = &bx;
    _ = &w4;
    var h4 = arg_h4;
    _ = &h4;
    var ref_seg_map = arg_ref_seg_map;
    _ = &ref_seg_map;
    _ = &stride;
    while (true) {
        if (!(@as(c_int, f.*.frame_hdr.*.primary_ref_frame) != @as(c_int, 7))) {
            unreachable;
        }
        if (!false) break;
    }
    var seg_id: c_uint = 8;
    _ = &seg_id;
    ref_seg_map += @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, by) * stride) + @as(ptrdiff_t, bx)))));
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w4) : (x += 1) {
                seg_id = @bitCast(@as(c_int, imin(@bitCast(@as(c_uint, @truncate(seg_id))), ref_seg_map[@bitCast(@as(isize, @intCast(x)))])));
            }
        }
        ref_seg_map += @as(usize, @bitCast(@as(isize, @intCast(stride))));
        if (!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool((blk: {
            const ref = &h4;
            ref.* -= 1;
            break :blk ref.*;
        }) > @as(c_int, 0)))))) != 0) and (seg_id != 0))) break;
    }
    while (true) {
        if (!(seg_id < @as(c_uint, 8))) {
            unreachable;
        }
        if (!false) break;
    }
    return seg_id;
}
pub fn splat_oneref_mv(c: [*c]const Dav1dContext, t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block, bw4: c_int, bh4: c_int) callconv(.c) void {
    _ = &c;
    _ = &t;
    _ = &bs;
    _ = &b;
    _ = &bw4;
    _ = &bh4;
    const mode: enum_InterPredMode = b.*.unnamed_0.unnamed_1.inter_mode;
    _ = &mode;
    const tmpl: refmvs_block align(16) = refmvs_block{
        .mv = refmvs_mvpair{
            .mv = [1]mv{
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)],
            } ++ @as([1]mv, @splat(@import("std").mem.zeroes(mv))),
        },
        .ref = refmvs_refpair{
            .ref = [2]i8{
                @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1)),
                @truncate(if (b.*.unnamed_0.unnamed_1.interintra_type != 0) @as(c_int, 0) else -@as(c_int, 1)),
            },
        },
        .bs = @truncate(bs),
        .mf = @bitCast(@as(i8, @truncate(@as(c_int, @intFromBool((mode == @as(enum_InterPredMode, GLOBALMV)) and (imin(bw4, bh4) >= @as(c_int, 2)))) | (@as(c_int, @intFromBool(mode == @as(enum_InterPredMode, NEWMV))) * @as(c_int, 2))))),
    };
    _ = &tmpl;
    c.*.refmvs_dsp.splat_mv.?(&t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))], &tmpl, t.*.bx, bw4, bh4);
}
pub fn splat_intrabc_mv(c: [*c]const Dav1dContext, t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block, bw4: c_int, bh4: c_int) callconv(.c) void {
    _ = &c;
    _ = &t;
    _ = &bs;
    _ = &b;
    _ = &bw4;
    _ = &bh4;
    const tmpl: refmvs_block align(16) = refmvs_block{
        .mv = refmvs_mvpair{
            .mv = [1]mv{
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)],
            } ++ @as([1]mv, @splat(@import("std").mem.zeroes(mv))),
        },
        .ref = refmvs_refpair{
            .ref = [2]i8{
                0,
                @truncate(-@as(c_int, 1)),
            },
        },
        .bs = @truncate(bs),
        .mf = 0,
    };
    _ = &tmpl;
    c.*.refmvs_dsp.splat_mv.?(&t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))], &tmpl, t.*.bx, bw4, bh4);
}
pub fn splat_tworef_mv(c: [*c]const Dav1dContext, t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block, bw4: c_int, bh4: c_int) callconv(.c) void {
    _ = &c;
    _ = &t;
    _ = &bs;
    _ = &b;
    _ = &bw4;
    _ = &bh4;
    while (true) {
        if (!((bw4 >= @as(c_int, 2)) and (bh4 >= @as(c_int, 2)))) {
            unreachable;
        }
        if (!false) break;
    }
    const mode: enum_CompInterPredMode = b.*.unnamed_0.unnamed_1.inter_mode;
    _ = &mode;
    const tmpl: refmvs_block align(16) = refmvs_block{
        .mv = refmvs_mvpair{
            .mv = [2]mv{
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)],
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)],
            },
        },
        .ref = refmvs_refpair{
            .ref = [2]i8{
                @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1)),
                @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]) + @as(c_int, 1)),
            },
        },
        .bs = @truncate(bs),
        .mf = @bitCast(@as(i8, @truncate(@as(c_int, @intFromBool(mode == @as(enum_CompInterPredMode, GLOBALMV_GLOBALMV))) | (@as(c_int, @intFromBool(!!(((@as(c_int, 1) << @intCast(@as(c_int, @bitCast(mode)))) & @as(c_int, 188)) != 0))) * @as(c_int, 2))))),
    };
    _ = &tmpl;
    c.*.refmvs_dsp.splat_mv.?(&t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))], &tmpl, t.*.bx, bw4, bh4);
}
pub fn splat_intraref(c: [*c]const Dav1dContext, t: [*c]Dav1dTaskContext, bs: enum_BlockSize, bw4: c_int, bh4: c_int) callconv(.c) void {
    _ = &c;
    _ = &t;
    _ = &bs;
    _ = &bw4;
    _ = &bh4;
    const tmpl: refmvs_block align(16) = refmvs_block{
        .mv = refmvs_mvpair{
            .mv = [1]mv{
                mv{
                    .n = 2147516416,
                },
            } ++ @as([1]mv, @splat(@import("std").mem.zeroes(mv))),
        },
        .ref = refmvs_refpair{
            .ref = [2]i8{
                0,
                @truncate(-@as(c_int, 1)),
            },
        },
        .bs = @truncate(bs),
        .mf = 0,
    };
    _ = &tmpl;
    c.*.refmvs_dsp.splat_mv.?(&t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))], &tmpl, t.*.bx, bw4, bh4);
}
pub fn mc_lowest_px(dst: [*c]c_int, by4: c_int, bh4: c_int, mvy: c_int, ss_ver: c_int, smp: [*c]const struct_ScalableMotionParams_20) callconv(.c) void {
    _ = &dst;
    _ = &by4;
    _ = &bh4;
    _ = &mvy;
    _ = &ss_ver;
    _ = &smp;
    const v_mul: c_int = @as(c_int, 4) >> @intCast(ss_ver);
    _ = &v_mul;
    if (!(smp.*.scale != 0)) {
        const my: c_int = mvy >> @intCast(@as(c_int, 3) + ss_ver);
        _ = &my;
        const dy: c_int = mvy & (@as(c_int, 15) >> @intCast(@as(c_int, @intFromBool(!(ss_ver != 0)))));
        _ = &dy;
        dst.* = imax(dst.*, (((by4 + bh4) * v_mul) + my) + (@as(c_int, 4) * @as(c_int, @intFromBool(!!(dy != 0)))));
    } else {
        var y: c_int = ((by4 * v_mul) << @intCast(@as(c_int, 4))) + (mvy * (@as(c_int, 1) << @intCast(@as(c_int, @intFromBool(!(ss_ver != 0))))));
        _ = &y;
        const tmp: i64 = (@as(i64, y) * @as(i64, smp.*.scale)) + @as(i64, (smp.*.scale - @as(c_int, 16384)) * @as(c_int, 8));
        _ = &tmp;
        y = apply_sign64(@truncate((antfly_codec_llabs(tmp) + @as(c_longlong, 128)) >> @intCast(@as(c_longlong, 8))), tmp) + @as(c_int, 32);
        const bottom: c_int = (((y + (((bh4 * v_mul) - @as(c_int, 1)) * smp.*.step)) >> @intCast(@as(c_int, 10))) + @as(c_int, 1)) + @as(c_int, 4);
        _ = &bottom;
        dst.* = imax(dst.*, bottom);
    }
}
pub inline fn affine_lowest_px(t: [*c]Dav1dTaskContext, dst: [*c]c_int, b_dim: [*c]const u8, wmp: [*c]const Dav1dWarpedMotionParams, ss_ver: c_int, ss_hor: c_int) void {
    _ = &t;
    _ = &dst;
    _ = &b_dim;
    _ = &wmp;
    _ = &ss_ver;
    _ = &ss_hor;
    const h_mul: c_int = @as(c_int, 4) >> @intCast(ss_hor);
    _ = &h_mul;
    const v_mul: c_int = @as(c_int, 4) >> @intCast(ss_ver);
    _ = &v_mul;
    while (true) {
        if (!(!(((@as(c_int, b_dim[@as(c_int, 0)]) * h_mul) & @as(c_int, 7)) != 0) and !(((@as(c_int, b_dim[@as(c_int, 1)]) * v_mul) & @as(c_int, 7)) != 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const mat: [*c]const i32 = @ptrCast(@alignCast(&wmp.*.matrix));
    _ = &mat;
    const y: c_int = (@as(c_int, b_dim[@as(c_int, 1)]) * v_mul) - @as(c_int, 8);
    _ = &y;
    const src_y: c_int = (t.*.by * @as(c_int, 4)) + ((y + @as(c_int, 4)) << @intCast(ss_ver));
    _ = &src_y;
    const mat5_y: i64 = (@as(i64, mat[@as(c_int, 5)]) * @as(i64, src_y)) + @as(i64, mat[@as(c_int, 1)]);
    _ = &mat5_y;
    {
        var x: c_int = 0;
        _ = &x;
        while (x < (@as(c_int, b_dim[@as(c_int, 0)]) * h_mul)) : (x += imax(8, (@as(c_int, b_dim[@as(c_int, 0)]) * h_mul) - @as(c_int, 8))) {
            const src_x: c_int = (t.*.bx * @as(c_int, 4)) + ((x + @as(c_int, 4)) << @intCast(ss_hor));
            _ = &src_x;
            const mvy: i64 = ((@as(i64, mat[@as(c_int, 4)]) * @as(i64, src_x)) + mat5_y) >> @intCast(@as(i64, ss_ver));
            _ = &mvy;
            const dy: c_int = @as(c_int, @truncate(mvy >> @intCast(@as(i64, 16)))) - @as(c_int, 4);
            _ = &dy;
            dst.* = imax(dst.*, (dy + @as(c_int, 4)) + @as(c_int, 8));
        }
    }
}
pub fn affine_lowest_px_luma(t: [*c]Dav1dTaskContext, dst: [*c]c_int, b_dim: [*c]const u8, wmp: [*c]const Dav1dWarpedMotionParams) callconv(.c) void {
    _ = &t;
    _ = &dst;
    _ = &b_dim;
    _ = &wmp;
    affine_lowest_px(t, dst, b_dim, wmp, 0, 0);
}
pub fn affine_lowest_px_chroma(t: [*c]Dav1dTaskContext, dst: [*c]c_int, b_dim: [*c]const u8, wmp: [*c]const Dav1dWarpedMotionParams) callconv(.c) void {
    _ = &t;
    _ = &dst;
    _ = &b_dim;
    _ = &wmp;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    while (true) {
        if (!(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400))) {
            unreachable;
        }
        if (!false) break;
    }
    if (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)) {
        affine_lowest_px_luma(t, dst, b_dim, wmp);
    } else {
        affine_lowest_px(t, dst, b_dim, wmp, @bitCast(f.*.cur.p.layout & @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)), 1);
    }
}
pub fn obmc_lowest_px(t: [*c]Dav1dTaskContext, dst: [*c][2]c_int, is_chroma: c_int, b_dim: [*c]const u8, bx4: c_int, by4: c_int, w4: c_int, h4: c_int) callconv(.c) void {
    _ = &t;
    _ = &dst;
    _ = &is_chroma;
    _ = &b_dim;
    _ = &bx4;
    _ = &by4;
    _ = &w4;
    _ = &h4;
    while (true) {
        if (!(!((t.*.bx & @as(c_int, 1)) != 0) and !((t.*.by & @as(c_int, 1)) != 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    var r: [*c][*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
    _ = &r;
    const ss_ver: c_int = @as(c_int, @intFromBool((is_chroma != 0) and (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420))));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool((is_chroma != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
    _ = &ss_hor;
    const h_mul: c_int = @as(c_int, 4) >> @intCast(ss_hor);
    _ = &h_mul;
    const v_mul: c_int = @as(c_int, 4) >> @intCast(ss_ver);
    _ = &v_mul;
    if ((t.*.by > t.*.ts.*.tiling.row_start) and (!(is_chroma != 0) or (((@as(c_int, b_dim[@as(c_int, 0)]) * h_mul) + (@as(c_int, b_dim[@as(c_int, 1)]) * v_mul)) >= @as(c_int, 16)))) {
        {
            var i: c_int = 0;
            _ = &i;
            var x: c_int = 0;
            _ = &x;
            while ((x < w4) and (i < imin(b_dim[@as(c_int, 2)], 4))) {
                const a_r: [*c]const refmvs_block = &r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast((t.*.bx + x) + @as(c_int, 1))))];
                _ = &a_r;
                const a_b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[a_r.*.bs]));
                _ = &a_b_dim;
                if (@as(c_int, a_r.*.ref.ref[@as(c_int, 0)]) > @as(c_int, 0)) {
                    const oh4: c_int = imin(b_dim[@as(c_int, 1)], 16) >> @intCast(@as(c_int, 1));
                    _ = &oh4;
                    mc_lowest_px(&dst[@bitCast(@as(isize, @intCast(@as(c_int, a_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@bitCast(@as(isize, @intCast(is_chroma)))], t.*.by, ((oh4 * @as(c_int, 3)) + @as(c_int, 3)) >> @intCast(@as(c_int, 2)), a_r.*.mv.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(@as(c_int, a_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)]);
                    i += 1;
                }
                x += imax(a_b_dim[@as(c_int, 0)], 2);
            }
        }
    }
    if (t.*.bx > t.*.ts.*.tiling.col_start) {
        var i: c_int = 0;
        _ = &i;
        var y: c_int = 0;
        _ = &y;
        while ((y < h4) and (i < imin(b_dim[@as(c_int, 3)], 4))) {
            const l_r: [*c]const refmvs_block = &r[@bitCast(@as(isize, @intCast(y + @as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))];
            _ = &l_r;
            const l_b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[l_r.*.bs]));
            _ = &l_b_dim;
            if (@as(c_int, l_r.*.ref.ref[@as(c_int, 0)]) > @as(c_int, 0)) {
                const oh4: c_int = iclip(l_b_dim[@as(c_int, 1)], 2, b_dim[@as(c_int, 1)]);
                _ = &oh4;
                mc_lowest_px(&dst[@bitCast(@as(isize, @intCast(@as(c_int, l_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@bitCast(@as(isize, @intCast(is_chroma)))], t.*.by + y, oh4, l_r.*.mv.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(@as(c_int, l_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)]);
                i += 1;
            }
            y += imax(l_b_dim[@as(c_int, 1)], 2);
        }
    }
}
pub fn decode_b(t: [*c]Dav1dTaskContext, bl: enum_BlockLevel, bs: enum_BlockSize, bp: enum_BlockPartition, intra_edge_flags: enum_EdgeFlags) callconv(.c) c_int {
    _ = &t;
    _ = &bl;
    _ = &bs;
    _ = &bp;
    _ = &intra_edge_flags;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    var b_mem: Av1Block = undefined;
    _ = &b_mem;
    const b: [*c]Av1Block = if (t.*.frame_thread.pass != 0) &f.*.frame_thread.b[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, t.*.by) * f.*.b4_stride) + @as(ptrdiff_t, t.*.bx))))] else &b_mem;
    _ = &b;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    const bx4: c_int = t.*.bx & @as(c_int, 31);
    _ = &bx4;
    const by4: c_int = t.*.by & @as(c_int, 31);
    _ = &by4;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_hor;
    const cbx4: c_int = bx4 >> @intCast(ss_hor);
    _ = &cbx4;
    const cby4: c_int = by4 >> @intCast(ss_ver);
    _ = &cby4;
    const bw4: c_int = b_dim[@as(c_int, 0)];
    _ = &bw4;
    const bh4: c_int = b_dim[@as(c_int, 1)];
    _ = &bh4;
    const w4: c_int = imin(bw4, f.*.bw - t.*.bx);
    _ = &w4;
    const h4: c_int = imin(bh4, f.*.bh - t.*.by);
    _ = &h4;
    const cbw4: c_int = (bw4 + ss_hor) >> @intCast(ss_hor);
    _ = &cbw4;
    const cbh4: c_int = (bh4 + ss_ver) >> @intCast(ss_ver);
    _ = &cbh4;
    const have_left: c_int = @as(c_int, @intFromBool(t.*.bx > ts.*.tiling.col_start));
    _ = &have_left;
    const have_top: c_int = @as(c_int, @intFromBool(t.*.by > ts.*.tiling.row_start));
    _ = &have_top;
    const has_chroma: c_int = @as(c_int, @intFromBool(((f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) and ((bw4 > ss_hor) or ((t.*.bx & @as(c_int, 1)) != 0))) and ((bh4 > ss_ver) or ((t.*.by & @as(c_int, 1)) != 0))));
    _ = &has_chroma;
    if (t.*.frame_thread.pass == @as(c_int, 2)) {
        if (@as(c_int, b.*.intra) != 0) {
            f.*.bd_fn.recon_b_intra.?(t, bs, intra_edge_flags, b);
            const y_mode_nofilt: enum_IntraPredMode = @bitCast(if (@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) == FILTER_PRED) DC_PRED else @as(c_int, b.*.unnamed_0.unnamed_0.y_mode));
            _ = &y_mode_nofilt;
            var edge: [*c]BlockContext = t.*.a;
            _ = &edge;
            {
                var i: c_int = 0;
                _ = &i;
                var off: c_int = bx4;
                _ = &off;
                while (i < @as(c_int, 2)) : ({
                    i += 1;
                    off = by4;
                    edge = &t.*.l;
                }) {
                    blk: {
                        switch (@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])) {
                            @as(c_int, 0) => {
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @truncate(y_mode_nofilt *% @as(enum_IntraPredMode, 1));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 1) * @as(c_int, 1))));
                                break :blk;
                            },
                            @as(c_int, 1) => {
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @truncate(y_mode_nofilt *% @as(enum_IntraPredMode, 257));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 1) * @as(c_int, 257))));
                                break :blk;
                            },
                            @as(c_int, 2) => {
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = y_mode_nofilt *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1)))) *% @as(c_uint, 16843009);
                                break :blk;
                            },
                            @as(c_int, 3) => {
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, y_mode_nofilt) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 1)))) *% @as(c_ulonglong, 72340172838076673);
                                break :blk;
                            },
                            @as(c_int, 4) => {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), @bitCast(y_mode_nofilt), 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 1, 16);
                                    if (!false) break;
                                }
                                break :blk;
                            },
                            @as(c_int, 5) => {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), @bitCast(y_mode_nofilt), 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 1, 32);
                                    if (!false) break;
                                }
                                break :blk;
                            },
                            else => {
                                while (true) {
                                    if (!(@as(c_int, 0) != 0)) {
                                        unreachable;
                                    }
                                    if (!false) break;
                                }
                            },
                        }
                    }
                }
            }
            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                const r: [*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((((t.*.by & @as(c_int, 31)) + @as(c_int, 5)) + bh4) - @as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))];
                _ = &r;
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < bw4) : (x += 1) {
                        r[@bitCast(@as(isize, @intCast(x)))].ref.ref[@as(c_int, 0)] = 0;
                        r[@bitCast(@as(isize, @intCast(x)))].bs = @truncate(bs);
                    }
                }
                var rr: [*c]const [*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
                _ = &rr;
                {
                    var y: c_int = 0;
                    _ = &y;
                    while (y < (bh4 - @as(c_int, 1))) : (y += 1) {
                        rr[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast((t.*.bx + bw4) - @as(c_int, 1))))].ref.ref[@as(c_int, 0)] = 0;
                        rr[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast((t.*.bx + bw4) - @as(c_int, 1))))].bs = @truncate(bs);
                    }
                }
            }
            if (has_chroma != 0) {
                var uv_mode: u8 = b.*.unnamed_0.unnamed_0.uv_mode;
                _ = &uv_mode;
                antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbw4))))))].?(@ptrCast(@alignCast(&t.*.a.*.uvmode[@bitCast(@as(isize, @intCast(cbx4)))])), uv_mode);
                antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbh4))))))].?(@ptrCast(@alignCast(&t.*.l.uvmode[@bitCast(@as(isize, @intCast(cby4)))])), uv_mode);
            }
        } else {
            if ((((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) and (@as(enum_Dav1dFrameType, @intCast(@as(c_int, @intFromBool(@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) == COMP_INTER_NONE)))) != 0)) and (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP)) {
                if (@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 0)]) == (-@as(c_int, 32767) - @as(c_int, 1))) {
                    t.*.warpmv.type = DAV1D_WM_TYPE_IDENTITY;
                } else {
                    t.*.warpmv.type = DAV1D_WM_TYPE_AFFINE;
                    t.*.warpmv.matrix[@as(c_int, 2)] = @as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 0)]) + @as(c_int, 65536);
                    t.*.warpmv.matrix[@as(c_int, 3)] = b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 1)];
                    t.*.warpmv.matrix[@as(c_int, 4)] = b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 2)];
                    t.*.warpmv.matrix[@as(c_int, 5)] = @as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 3)]) + @as(c_int, 65536);
                    antfly_av1_dav1d_set_affine_mv2d(bw4, bh4, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.mv2d, &t.*.warpmv, t.*.bx, t.*.by);
                    _ = antfly_av1_dav1d_get_shear_params(&t.*.warpmv);
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                }
            }
            if (f.*.bd_fn.recon_b_inter.?(t, bs, b) != 0) return -@as(c_int, 1);
            const filter: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_filter_dir[b.*.unnamed_0.unnamed_1.filter2d]));
            _ = &filter;
            var edge: [*c]BlockContext = t.*.a;
            _ = &edge;
            {
                var i: c_int = 0;
                _ = &i;
                var off: c_int = bx4;
                _ = &off;
                while (i < @as(c_int, 2)) : ({
                    i += 1;
                    off = by4;
                    edge = &t.*.l;
                }) {
                    blk: {
                        switch (@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])) {
                            @as(c_int, 0) => {
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, filter[@as(c_int, 0)]) * @as(c_int, 1))));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, filter[@as(c_int, 1)]) * @as(c_int, 1))));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                                break :blk;
                            },
                            @as(c_int, 1) => {
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, filter[@as(c_int, 0)]) * @as(c_int, 257))));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, filter[@as(c_int, 1)]) * @as(c_int, 257))));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                                break :blk;
                            },
                            @as(c_int, 2) => {
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, filter[@as(c_int, 0)])))) *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, filter[@as(c_int, 1)])))) *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                                break :blk;
                            },
                            @as(c_int, 3) => {
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, filter[@as(c_int, 0)])))) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, filter[@as(c_int, 1)])))) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                                break :blk;
                            },
                            @as(c_int, 4) => {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), filter[@as(c_int, 0)], 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), filter[@as(c_int, 1)], 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                    if (!false) break;
                                }
                                break :blk;
                            },
                            @as(c_int, 5) => {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), filter[@as(c_int, 0)], 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), filter[@as(c_int, 1)], 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                    if (!false) break;
                                }
                                break :blk;
                            },
                            else => {
                                while (true) {
                                    if (!(@as(c_int, 0) != 0)) {
                                        unreachable;
                                    }
                                    if (!false) break;
                                }
                            },
                        }
                    }
                }
            }
            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                const r: [*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((((t.*.by & @as(c_int, 31)) + @as(c_int, 5)) + bh4) - @as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))];
                _ = &r;
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < bw4) : (x += 1) {
                        r[@bitCast(@as(isize, @intCast(x)))].ref.ref[@as(c_int, 0)] = @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1));
                        r[@bitCast(@as(isize, @intCast(x)))].mv.mv[@as(c_int, 0)] = b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)];
                        r[@bitCast(@as(isize, @intCast(x)))].bs = @truncate(bs);
                    }
                }
                var rr: [*c]const [*c]refmvs_block = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
                _ = &rr;
                {
                    var y: c_int = 0;
                    _ = &y;
                    while (y < (bh4 - @as(c_int, 1))) : (y += 1) {
                        rr[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast((t.*.bx + bw4) - @as(c_int, 1))))].ref.ref[@as(c_int, 0)] = @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1));
                        rr[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast((t.*.bx + bw4) - @as(c_int, 1))))].mv.mv[@as(c_int, 0)] = b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)];
                        rr[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast((t.*.bx + bw4) - @as(c_int, 1))))].bs = @truncate(bs);
                    }
                }
            }
            if (has_chroma != 0) {
                antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbw4))))))].?(@ptrCast(@alignCast(&t.*.a.*.uvmode[@bitCast(@as(isize, @intCast(cbx4)))])), DC_PRED);
                antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbh4))))))].?(@ptrCast(@alignCast(&t.*.l.uvmode[@bitCast(@as(isize, @intCast(cby4)))])), DC_PRED);
            }
        }
        return 0;
    }
    const cw4: c_int = (w4 + ss_hor) >> @intCast(ss_hor);
    _ = &cw4;
    const ch4: c_int = (h4 + ss_ver) >> @intCast(ss_ver);
    _ = &ch4;
    b.*.bl = @truncate(bl);
    b.*.bp = @truncate(bp);
    b.*.bs = @truncate(bs);
    var seg: [*c]const Dav1dSegmentationData = null;
    _ = &seg;
    var seg_pred: c_int = 0;
    _ = &seg_pred;
    if (@as(c_int, f.*.frame_hdr.*.segmentation.enabled) != 0) {
        if (!(@as(c_int, f.*.frame_hdr.*.segmentation.update_map) != 0)) {
            if (f.*.prev_segmap != null) {
                var seg_id: c_uint = get_prev_frame_segid(f, t.*.by, t.*.bx, w4, h4, f.*.prev_segmap, f.*.b4_stride);
                _ = &seg_id;
                if (seg_id >= @as(c_uint, 8)) return -@as(c_int, 1);
                b.*.seg_id = @truncate(seg_id);
            } else {
                b.*.seg_id = 0;
            }
            seg = &f.*.frame_hdr.*.segmentation.seg_data.d[b.*.seg_id];
        } else if (@as(c_int, f.*.frame_hdr.*.segmentation.seg_data.preskip) != 0) {
            if ((@as(c_int, f.*.frame_hdr.*.segmentation.temporal) != 0) and ((blk: {
                const tmp = @as(c_int, @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.seg_pred[@bitCast(@as(isize, @intCast(@as(c_int, t.*.a.*.seg_pred[@bitCast(@as(isize, @intCast(bx4)))]) + @as(c_int, t.*.l.seg_pred[@bitCast(@as(isize, @intCast(by4)))]))))])))))));
                seg_pred = tmp;
                break :blk tmp;
            }) != 0)) {
                if (f.*.prev_segmap != null) {
                    var seg_id: c_uint = get_prev_frame_segid(f, t.*.by, t.*.bx, w4, h4, f.*.prev_segmap, f.*.b4_stride);
                    _ = &seg_id;
                    if (seg_id >= @as(c_uint, 8)) return -@as(c_int, 1);
                    b.*.seg_id = @truncate(seg_id);
                } else {
                    b.*.seg_id = 0;
                }
            } else {
                var seg_ctx: c_int = undefined;
                _ = &seg_ctx;
                const pred_seg_id: c_uint = get_cur_frame_segid(t.*.by, t.*.bx, have_top, have_left, &seg_ctx, f.*.cur_segmap, f.*.b4_stride);
                _ = &pred_seg_id;
                const diff: c_uint = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.seg_id[@bitCast(@as(isize, @intCast(seg_ctx)))])), @bitCast(@as(c_long, @as(c_int, 8) - @as(c_int, 1))));
                _ = &diff;
                const last_active_seg_id: c_uint = @bitCast(@as(c_int, f.*.frame_hdr.*.segmentation.seg_data.last_active_segid));
                _ = &last_active_seg_id;
                b.*.seg_id = @bitCast(@as(i8, @truncate(neg_deinterleave(@bitCast(@as(c_uint, @truncate(diff))), @bitCast(@as(c_uint, @truncate(pred_seg_id))), @bitCast(@as(c_uint, @truncate(last_active_seg_id +% @as(c_uint, 1))))))));
                if (@as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.seg_id)))) > last_active_seg_id) {
                    b.*.seg_id = 0;
                }
                if (@as(c_int, b.*.seg_id) >= @as(c_int, 8)) {
                    b.*.seg_id = 0;
                }
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            seg = &f.*.frame_hdr.*.segmentation.seg_data.d[b.*.seg_id];
        }
    } else {
        b.*.seg_id = 0;
    }
    if (((!(seg != null) or ((!(@as(c_int, seg.*.globalmv) != 0) and (@as(c_int, seg.*.ref) == -@as(c_int, 1))) and !(@as(c_int, seg.*.skip) != 0))) and (@as(c_int, f.*.frame_hdr.*.skip_mode_enabled) != 0)) and (imin(bw4, bh4) > @as(c_int, 1))) {
        const smctx: c_int = @as(c_int, t.*.a.*.skip_mode[@bitCast(@as(isize, @intCast(bx4)))]) + @as(c_int, t.*.l.skip_mode[@bitCast(@as(isize, @intCast(by4)))]);
        _ = &smctx;
        b.*.skip_mode = @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.skip_mode[@bitCast(@as(isize, @intCast(smctx)))]))));
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
    } else {
        b.*.skip_mode = 0;
    }
    if ((@as(c_int, b.*.skip_mode) != 0) or ((seg != null) and (@as(c_int, seg.*.skip) != 0))) {
        b.*.skip = 1;
    } else {
        const sctx: c_int = @as(c_int, t.*.a.*.skip[@bitCast(@as(isize, @intCast(bx4)))]) + @as(c_int, t.*.l.skip[@bitCast(@as(isize, @intCast(by4)))]);
        _ = &sctx;
        b.*.skip = @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.skip[@bitCast(@as(isize, @intCast(sctx)))]))));
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
    }
    if (((@as(c_int, f.*.frame_hdr.*.segmentation.enabled) != 0) and (@as(c_int, f.*.frame_hdr.*.segmentation.update_map) != 0)) and !(@as(c_int, f.*.frame_hdr.*.segmentation.seg_data.preskip) != 0)) {
        if ((!(@as(c_int, b.*.skip) != 0) and (@as(c_int, f.*.frame_hdr.*.segmentation.temporal) != 0)) and ((blk: {
            const tmp = @as(c_int, @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.seg_pred[@bitCast(@as(isize, @intCast(@as(c_int, t.*.a.*.seg_pred[@bitCast(@as(isize, @intCast(bx4)))]) + @as(c_int, t.*.l.seg_pred[@bitCast(@as(isize, @intCast(by4)))]))))])))))));
            seg_pred = tmp;
            break :blk tmp;
        }) != 0)) {
            if (f.*.prev_segmap != null) {
                var seg_id: c_uint = get_prev_frame_segid(f, t.*.by, t.*.bx, w4, h4, f.*.prev_segmap, f.*.b4_stride);
                _ = &seg_id;
                if (seg_id >= @as(c_uint, 8)) return -@as(c_int, 1);
                b.*.seg_id = @truncate(seg_id);
            } else {
                b.*.seg_id = 0;
            }
        } else {
            var seg_ctx: c_int = undefined;
            _ = &seg_ctx;
            const pred_seg_id: c_uint = get_cur_frame_segid(t.*.by, t.*.bx, have_top, have_left, &seg_ctx, f.*.cur_segmap, f.*.b4_stride);
            _ = &pred_seg_id;
            if (@as(c_int, b.*.skip) != 0) {
                b.*.seg_id = @truncate(pred_seg_id);
            } else {
                const diff: c_uint = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.seg_id[@bitCast(@as(isize, @intCast(seg_ctx)))])), @bitCast(@as(c_long, @as(c_int, 8) - @as(c_int, 1))));
                _ = &diff;
                const last_active_seg_id: c_uint = @bitCast(@as(c_int, f.*.frame_hdr.*.segmentation.seg_data.last_active_segid));
                _ = &last_active_seg_id;
                b.*.seg_id = @bitCast(@as(i8, @truncate(neg_deinterleave(@bitCast(@as(c_uint, @truncate(diff))), @bitCast(@as(c_uint, @truncate(pred_seg_id))), @bitCast(@as(c_uint, @truncate(last_active_seg_id +% @as(c_uint, 1))))))));
                if (@as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.seg_id)))) > last_active_seg_id) {
                    b.*.seg_id = 0;
                }
            }
            if (@as(c_int, b.*.seg_id) >= @as(c_int, 8)) {
                b.*.seg_id = 0;
            }
        }
        seg = &f.*.frame_hdr.*.segmentation.seg_data.d[b.*.seg_id];
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
    }
    if (!(@as(c_int, b.*.skip) != 0)) {
        const idx: c_int = if (f.*.seq_hdr.*.sb128 != 0) ((t.*.bx & @as(c_int, 16)) >> @intCast(@as(c_int, 4))) + ((t.*.by & @as(c_int, 16)) >> @intCast(@as(c_int, 3))) else @as(c_int, 0);
        _ = &idx;
        if (@as(c_int, t.*.cur_sb_cdef_idx_ptr[@bitCast(@as(isize, @intCast(idx)))]) == -@as(c_int, 1)) {
            const v: c_int = @bitCast(@as(c_uint, @truncate(dav1d_msac_decode_bools(&ts.*.msac, f.*.frame_hdr.*.cdef.n_bits))));
            _ = &v;
            t.*.cur_sb_cdef_idx_ptr[@bitCast(@as(isize, @intCast(idx)))] = @truncate(v);
            if (bw4 > @as(c_int, 16)) {
                t.*.cur_sb_cdef_idx_ptr[@bitCast(@as(isize, @intCast(idx + @as(c_int, 1))))] = @truncate(v);
            }
            if (bh4 > @as(c_int, 16)) {
                t.*.cur_sb_cdef_idx_ptr[@bitCast(@as(isize, @intCast(idx + @as(c_int, 2))))] = @truncate(v);
            }
            if ((bw4 == @as(c_int, 32)) and (bh4 == @as(c_int, 32))) {
                t.*.cur_sb_cdef_idx_ptr[@bitCast(@as(isize, @intCast(idx + @as(c_int, 3))))] = @truncate(v);
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
    }
    if (!((t.*.bx & (@as(c_int, 31) >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0)))))) != 0) and !((t.*.by & (@as(c_int, 31) >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0)))))) != 0)) {
        const prev_qidx: c_int = ts.*.last_qidx;
        _ = &prev_qidx;
        const have_delta_q: c_int = @as(c_int, @intFromBool((@as(c_int, f.*.frame_hdr.*.delta.q.present) != 0) and ((bs != @as(enum_BlockSize, @bitCast(if (f.*.seq_hdr.*.sb128 != 0) BS_128x128 else BS_64x64))) or !(@as(c_int, b.*.skip) != 0))));
        _ = &have_delta_q;
        var prev_delta_lf: u32 = ts.*.last_delta_lf.u32;
        _ = &prev_delta_lf;
        if (have_delta_q != 0) {
            var delta_q: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.delta_q)), 3))));
            _ = &delta_q;
            if (delta_q == @as(c_int, 3)) {
                const n_bits: c_int = @bitCast(@as(c_uint, @truncate(@as(c_uint, 1) +% dav1d_msac_decode_bools(&ts.*.msac, 3))));
                _ = &n_bits;
                delta_q = @bitCast(@as(c_uint, @truncate((dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, n_bits))) +% @as(c_uint, 1)) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(n_bits)))))));
            }
            if (delta_q != 0) {
                if (antfly_av1_dav1d_msac_decode_bool_equi_c(&ts.*.msac) != 0) {
                    delta_q = -delta_q;
                }
                delta_q *= @as(c_int, 1) << @intCast(@as(c_int, f.*.frame_hdr.*.delta.q.res_log2));
            }
            ts.*.last_qidx = iclip(ts.*.last_qidx + delta_q, 1, 255);
            if (((((((have_delta_q != 0) and false) and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            if (@as(c_int, f.*.frame_hdr.*.delta.lf.present) != 0) {
                const n_lfs: c_int = if (f.*.frame_hdr.*.delta.lf.multi != 0) if (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) @as(c_int, 4) else @as(c_int, 2) else @as(c_int, 1);
                _ = &n_lfs;
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < n_lfs) : (i += 1) {
                        var delta_lf: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.delta_lf[@bitCast(@as(isize, @intCast(i + @as(c_int, f.*.frame_hdr.*.delta.lf.multi))))])), 3))));
                        _ = &delta_lf;
                        if (delta_lf == @as(c_int, 3)) {
                            const n_bits: c_int = @bitCast(@as(c_uint, @truncate(@as(c_uint, 1) +% dav1d_msac_decode_bools(&ts.*.msac, 3))));
                            _ = &n_bits;
                            delta_lf = @bitCast(@as(c_uint, @truncate((dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, n_bits))) +% @as(c_uint, 1)) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(n_bits)))))));
                        }
                        if (delta_lf != 0) {
                            if (antfly_av1_dav1d_msac_decode_bool_equi_c(&ts.*.msac) != 0) {
                                delta_lf = -delta_lf;
                            }
                            delta_lf *= @as(c_int, 1) << @intCast(@as(c_int, f.*.frame_hdr.*.delta.lf.res_log2));
                        }
                        ts.*.last_delta_lf.i8[@bitCast(@as(isize, @intCast(i)))] = @truncate(iclip(@as(c_int, ts.*.last_delta_lf.i8[@bitCast(@as(isize, @intCast(i)))]) + delta_lf, -@as(c_int, 63), 63));
                        if (((((((have_delta_q != 0) and false) and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                            _ = @as(c_int, 0);
                        }
                    }
                }
            }
        }
        if (ts.*.last_qidx == @as(c_int, f.*.frame_hdr.*.quant.yac)) {
            ts.*.dq = @ptrCast(@alignCast(@constCast(&f.*.dq)));
        } else if (ts.*.last_qidx != prev_qidx) {
            init_quant_tables(f.*.seq_hdr, f.*.frame_hdr, ts.*.last_qidx, @ptrCast(@alignCast(&ts.*.dqmem)));
            ts.*.dq = @ptrCast(@alignCast(&ts.*.dqmem));
        }
        if (!(ts.*.last_delta_lf.u32 != 0)) {
            ts.*.lflvl = @ptrCast(@alignCast(@constCast(&f.*.lf.lvl)));
        } else if (ts.*.last_delta_lf.u32 != prev_delta_lf) {
            ts.*.lflvl = @ptrCast(@alignCast(&ts.*.lflvlmem));
            antfly_av1_dav1d_calc_lf_values(@ptrCast(@alignCast(&ts.*.lflvlmem)), f.*.frame_hdr, @ptrCast(@alignCast(&ts.*.last_delta_lf.i8)));
        }
    }
    if (@as(c_int, b.*.skip_mode) != 0) {
        b.*.intra = 0;
    } else if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
        if ((seg != null) and ((@as(c_int, seg.*.ref) >= @as(c_int, 0)) or (@as(c_int, seg.*.globalmv) != 0))) {
            b.*.intra = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(!(@as(c_int, seg.*.ref) != 0))))));
        } else {
            const ictx: c_int = get_intra_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
            _ = &ictx;
            b.*.intra = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(!(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.intra[@bitCast(@as(isize, @intCast(ictx)))]))) != 0))))));
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
    } else if (@as(c_int, f.*.frame_hdr.*.allow_intrabc) != 0) {
        b.*.intra = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(!(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.intrabc))) != 0))))));
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
    } else {
        b.*.intra = 1;
    }
    if (@as(c_int, b.*.intra) != 0) {
        const ymode_cdf: [*c]u16 = if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) @as([*c]u16, @ptrCast(@alignCast(&ts.*.cdf.m.y_mode[antfly_av1_dav1d_ymode_size_context[bs]]))) else @as([*c]u16, @ptrCast(@alignCast(&ts.*.cdf.kfym[antfly_av1_dav1d_intra_mode_context[t.*.a.*.mode[@bitCast(@as(isize, @intCast(bx4)))]]][antfly_av1_dav1d_intra_mode_context[t.*.l.mode[@bitCast(@as(isize, @intCast(by4)))]]])));
        _ = &ymode_cdf;
        b.*.unnamed_0.unnamed_0.y_mode = @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, ymode_cdf, @bitCast(@as(c_long, N_INTRA_PRED_MODES - @as(c_int, 1)))));
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
        if ((((@as(c_int, b_dim[@as(c_int, 2)]) + @as(c_int, b_dim[@as(c_int, 3)])) >= @as(c_int, 2)) and (@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) >= VERT_PRED)) and (@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) <= VERT_LEFT_PRED)) {
            const acdf: [*c]u16 = @ptrCast(@alignCast(&ts.*.cdf.m.angle_delta[@bitCast(@as(isize, @intCast(@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) - VERT_PRED)))]));
            _ = &acdf;
            const angle: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, acdf, 6))));
            _ = &angle;
            b.*.unnamed_0.unnamed_0.y_angle = @truncate(angle - @as(c_int, 3));
        } else {
            b.*.unnamed_0.unnamed_0.y_angle = 0;
        }
        if (has_chroma != 0) {
            const cfl_allowed: c_int = if (f.*.frame_hdr.*.segmentation.lossless[b.*.seg_id] != 0) @as(c_int, @intFromBool((cbw4 == @as(c_int, 1)) and (cbh4 == @as(c_int, 1)))) else @as(c_int, @intFromBool(!!((cfl_allowed_mask & @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @bitCast(bs))))))) != 0)));
            _ = &cfl_allowed;
            const uvmode_cdf: [*c]u16 = @ptrCast(@alignCast(&ts.*.cdf.m.uv_mode[@bitCast(@as(isize, @intCast(cfl_allowed)))][b.*.unnamed_0.unnamed_0.y_mode]));
            _ = &uvmode_cdf;
            b.*.unnamed_0.unnamed_0.uv_mode = @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, uvmode_cdf, @bitCast(@as(c_long, (N_UV_INTRA_PRED_MODES - @as(c_int, 1)) - @as(c_int, @intFromBool(!(cfl_allowed != 0)))))));
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            b.*.unnamed_0.unnamed_0.uv_angle = 0;
            if (@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) == CFL_PRED) {
                const sign: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.cfl_sign)), 7) +% @as(c_uint, 1))));
                _ = &sign;
                const sign_u: c_int = (sign * @as(c_int, 86)) >> @intCast(@as(c_int, 8));
                _ = &sign_u;
                const sign_v: c_int = sign - (sign_u * @as(c_int, 3));
                _ = &sign_v;
                while (true) {
                    if (!(sign_u == @divTrunc(sign, @as(c_int, 3)))) {
                        unreachable;
                    }
                    if (!false) break;
                }
                if (sign_u != 0) {
                    const ctx: c_int = (@as(c_int, @intFromBool(sign_u == @as(c_int, 2))) * @as(c_int, 3)) + sign_v;
                    _ = &ctx;
                    b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.cfl_alpha[@bitCast(@as(isize, @intCast(ctx)))])), 15) +% @as(c_uint, 1))));
                    if (sign_u == @as(c_int, 1)) {
                        b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 0)] = @truncate(-@as(c_int, b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 0)]));
                    }
                } else {
                    b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 0)] = 0;
                }
                if (sign_v != 0) {
                    const ctx: c_int = (@as(c_int, @intFromBool(sign_v == @as(c_int, 2))) * @as(c_int, 3)) + sign_u;
                    _ = &ctx;
                    b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 1)] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.cfl_alpha[@bitCast(@as(isize, @intCast(ctx)))])), 15) +% @as(c_uint, 1))));
                    if (sign_v == @as(c_int, 1)) {
                        b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 1)] = @truncate(-@as(c_int, b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 1)]));
                    }
                } else {
                    b.*.unnamed_0.unnamed_0.cfl_alpha[@as(c_int, 1)] = 0;
                }
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            } else if ((((@as(c_int, b_dim[@as(c_int, 2)]) + @as(c_int, b_dim[@as(c_int, 3)])) >= @as(c_int, 2)) and (@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) >= VERT_PRED)) and (@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) <= VERT_LEFT_PRED)) {
                const acdf: [*c]u16 = @ptrCast(@alignCast(&ts.*.cdf.m.angle_delta[@bitCast(@as(isize, @intCast(@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) - VERT_PRED)))]));
                _ = &acdf;
                const angle: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, acdf, 6))));
                _ = &angle;
                b.*.unnamed_0.unnamed_0.uv_angle = @truncate(angle - @as(c_int, 3));
            }
        }
        b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)] = blk: {
            const tmp = @as(u8, 0);
            b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)] = tmp;
            break :blk tmp;
        };
        if (((@as(c_int, f.*.frame_hdr.*.allow_screen_content_tools) != 0) and (imax(bw4, bh4) <= @as(c_int, 16))) and ((bw4 + bh4) >= @as(c_int, 4))) {
            const sz_ctx: c_int = (@as(c_int, b_dim[@as(c_int, 2)]) + @as(c_int, b_dim[@as(c_int, 3)])) - @as(c_int, 2);
            _ = &sz_ctx;
            if (@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) == DC_PRED) {
                const pal_ctx: c_int = @as(c_int, @intFromBool(@as(c_int, t.*.a.*.pal_sz[@bitCast(@as(isize, @intCast(bx4)))]) > @as(c_int, 0))) + @as(c_int, @intFromBool(@as(c_int, t.*.l.pal_sz[@bitCast(@as(isize, @intCast(by4)))]) > @as(c_int, 0)));
                _ = &pal_ctx;
                const use_y_pal: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.pal_y[@bitCast(@as(isize, @intCast(sz_ctx)))][@bitCast(@as(isize, @intCast(pal_ctx)))]))))));
                _ = &use_y_pal;
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
                if (use_y_pal != 0) {
                    f.*.bd_fn.read_pal_plane.?(t, b, 0, sz_ctx, bx4, by4);
                }
            }
            if ((has_chroma != 0) and (@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) == DC_PRED)) {
                const pal_ctx: c_int = @as(c_int, @intFromBool(@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)]) > @as(c_int, 0)));
                _ = &pal_ctx;
                const use_uv_pal: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.pal_uv[@bitCast(@as(isize, @intCast(pal_ctx)))]))))));
                _ = &use_uv_pal;
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
                if (use_uv_pal != 0) {
                    f.*.bd_fn.read_pal_uv.?(t, b, sz_ctx, bx4, by4);
                }
            }
        }
        if ((((@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) == DC_PRED) and !(@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)]) != 0)) and (imax(b_dim[@as(c_int, 2)], b_dim[@as(c_int, 3)]) <= @as(c_int, 3))) and (@as(c_int, f.*.seq_hdr.*.filter_intra) != 0)) {
            const is_filter: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.use_filter_intra[bs]))))));
            _ = &is_filter;
            if (is_filter != 0) {
                b.*.unnamed_0.unnamed_0.y_mode = FILTER_PRED;
                b.*.unnamed_0.unnamed_0.y_angle = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.filter_intra)), 4))));
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        if (@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)]) != 0) {
            var pal_idx: [*c]u8 = undefined;
            _ = &pal_idx;
            if (t.*.frame_thread.pass != 0) {
                const p: c_int = t.*.frame_thread.pass & @as(c_int, 1);
                _ = &p;
                while (true) {
                    if (!(ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx != null)) {
                        unreachable;
                    }
                    if (!false) break;
                }
                pal_idx = ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx;
                ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx += @as(usize, @bitCast(@as(isize, @intCast((bw4 * bh4) * @as(c_int, 8)))));
            } else {
                pal_idx = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.pal_idx_y));
            }
            read_pal_indices(t, pal_idx, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)], 0, w4, h4, bw4, bh4);
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        if ((has_chroma != 0) and (@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) != 0)) {
            var pal_idx: [*c]u8 = undefined;
            _ = &pal_idx;
            if (t.*.frame_thread.pass != 0) {
                const p: c_int = t.*.frame_thread.pass & @as(c_int, 1);
                _ = &p;
                while (true) {
                    if (!(ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx != null)) {
                        unreachable;
                    }
                    if (!false) break;
                }
                pal_idx = ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx;
                ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx += @as(usize, @bitCast(@as(isize, @intCast((cbw4 * cbh4) * @as(c_int, 8)))));
            } else {
                pal_idx = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.pal_idx_uv));
            }
            read_pal_indices(t, pal_idx, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)], 1, cw4, ch4, cbw4, cbh4);
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        var t_dim: [*c]const TxfmInfo = undefined;
        _ = &t_dim;
        if (@as(c_int, f.*.frame_hdr.*.segmentation.lossless[b.*.seg_id]) != 0) {
            b.*.unnamed_0.unnamed_0.tx = blk: {
                const tmp = @as(u8, @bitCast(@as(i8, @truncate(TX_4X4))));
                b.*.uvtx = tmp;
                break :blk tmp;
            };
            t_dim = &antfly_av1_dav1d_txfm_dimensions[TX_4X4];
        } else {
            b.*.unnamed_0.unnamed_0.tx = antfly_av1_dav1d_max_txfm_size_for_bs[bs][@as(c_int, 0)];
            b.*.uvtx = antfly_av1_dav1d_max_txfm_size_for_bs[bs][f.*.cur.p.layout];
            t_dim = &antfly_av1_dav1d_txfm_dimensions[b.*.unnamed_0.unnamed_0.tx];
            if ((f.*.frame_hdr.*.txfm_mode == @as(enum_Dav1dTxfmMode, DAV1D_TX_SWITCHABLE)) and (@as(c_int, t_dim.*.max) > TX_4X4)) {
                const tctx: c_int = get_tx_ctx(t.*.a, &t.*.l, t_dim, by4, bx4);
                _ = &tctx;
                const tx_cdf: [*c]u16 = @ptrCast(@alignCast(&ts.*.cdf.m.txsz[@bitCast(@as(isize, @intCast(@as(c_int, t_dim.*.max) - @as(c_int, 1))))][@bitCast(@as(isize, @intCast(tctx)))]));
                _ = &tx_cdf;
                var depth: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, tx_cdf, @bitCast(@as(c_long, imin(t_dim.*.max, 2)))))));
                _ = &depth;
                while ((blk: {
                    const ref = &depth;
                    const tmp = ref.*;
                    ref.* -= 1;
                    break :blk tmp;
                }) != 0) {
                    b.*.unnamed_0.unnamed_0.tx = t_dim.*.sub;
                    t_dim = &antfly_av1_dav1d_txfm_dimensions[b.*.unnamed_0.unnamed_0.tx];
                }
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        if (t.*.frame_thread.pass == @as(c_int, 1)) {
            f.*.bd_fn.read_coef_blocks.?(t, bs, b);
        } else {
            f.*.bd_fn.recon_b_intra.?(t, bs, intra_edge_flags, b);
        }
        if ((@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) or (@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0)) {
            antfly_av1_dav1d_create_lf_mask_intra(t.*.lf_mask, f.*.lf.level, f.*.b4_stride, @ptrCast(@alignCast(@constCast(&ts.*.lflvl[b.*.seg_id][@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)]))), t.*.bx, t.*.by, f.*.w4, f.*.h4, bs, b.*.unnamed_0.unnamed_0.tx, b.*.uvtx, f.*.cur.p.layout, &t.*.a.*.tx_lpf_y[@bitCast(@as(isize, @intCast(bx4)))], &t.*.l.tx_lpf_y[@bitCast(@as(isize, @intCast(by4)))], @ptrCast(@alignCast(if (has_chroma != 0) @as(?*anyopaque, @ptrCast(@alignCast(&t.*.a.*.tx_lpf_uv[@bitCast(@as(isize, @intCast(cbx4)))]))) else @as(?*anyopaque, null))), @ptrCast(@alignCast(if (has_chroma != 0) @as(?*anyopaque, @ptrCast(@alignCast(&t.*.l.tx_lpf_uv[@bitCast(@as(isize, @intCast(cby4)))]))) else @as(?*anyopaque, null))));
        }
        const y_mode_nofilt: enum_IntraPredMode = @bitCast(if (@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) == FILTER_PRED) DC_PRED else @as(c_int, b.*.unnamed_0.unnamed_0.y_mode));
        _ = &y_mode_nofilt;
        var edge: [*c]BlockContext = t.*.a;
        _ = &edge;
        {
            var i: c_int = 0;
            _ = &i;
            var off: c_int = bx4;
            _ = &off;
            while (i < @as(c_int, 2)) : ({
                i += 1;
                off = by4;
                edge = &t.*.l;
            }) {
                var t_lsz: c_int = @as([*]const u8, @ptrCast(&t_dim.*.lw))[@bitCast(@as(isize, @intCast(i)))];
                _ = &t_lsz;
                blk: {
                    switch (@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])) {
                        @as(c_int, 0) => {
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(t_lsz * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.tx[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(t_lsz * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @truncate(y_mode_nofilt *% @as(enum_IntraPredMode, 1));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)]) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(seg_pred * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 1) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.skip) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate((if (has_chroma != 0) @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) else @as(c_int, 0)) * @as(c_int, 1))));
                            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(COMP_INTER_NONE * @as(c_int, 1))));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1)))))) * @as(c_int, 1))));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1)))))) * @as(c_int, 1))));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(DAV1D_N_SWITCHABLE_FILTERS * @as(c_int, 1))));
                                @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(DAV1D_N_SWITCHABLE_FILTERS * @as(c_int, 1))));
                            }
                            break :blk;
                        },
                        @as(c_int, 1) => {
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(t_lsz * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.tx[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(t_lsz * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @truncate(y_mode_nofilt *% @as(enum_IntraPredMode, 257));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)]) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(seg_pred * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 1) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.skip) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate((if (has_chroma != 0) @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) else @as(c_int, 0)) * @as(c_int, 257))));
                            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(COMP_INTER_NONE * @as(c_int, 257))));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1)))))) * @as(c_int, 257))));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1)))))) * @as(c_int, 257))));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(DAV1D_N_SWITCHABLE_FILTERS * @as(c_int, 257))));
                                @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(DAV1D_N_SWITCHABLE_FILTERS * @as(c_int, 257))));
                            }
                            break :blk;
                        },
                        @as(c_int, 2) => {
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, t_lsz))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.tx[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, t_lsz))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = y_mode_nofilt *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)])))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, seg_pred))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.skip)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, if (has_chroma != 0) @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) else @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, COMP_INTER_NONE))) *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))))))) *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))))))) *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, DAV1D_N_SWITCHABLE_FILTERS))) *% @as(c_uint, 16843009);
                                @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, DAV1D_N_SWITCHABLE_FILTERS))) *% @as(c_uint, 16843009);
                            }
                            break :blk;
                        },
                        @as(c_int, 3) => {
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, t_lsz))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.tx[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, t_lsz))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, y_mode_nofilt) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)])))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, seg_pred))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 1)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.skip)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, if (has_chroma != 0) @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) else @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, COMP_INTER_NONE))) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))))))) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))))))) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, DAV1D_N_SWITCHABLE_FILTERS))) *% @as(c_ulonglong, 72340172838076673);
                                @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, DAV1D_N_SWITCHABLE_FILTERS))) *% @as(c_ulonglong, 72340172838076673);
                            }
                            break :blk;
                        },
                        @as(c_int, 4) => {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))])), t_lsz, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx[@bitCast(@as(isize, @intCast(off)))])), t_lsz, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), @bitCast(y_mode_nofilt), 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)], 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))])), seg_pred, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 1, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))])), b.*.skip, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))])), if (has_chroma != 0) @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) else @as(c_int, 0), 16);
                                if (!false) break;
                            }
                            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))])), COMP_INTER_NONE, 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))), 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))), 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), DAV1D_N_SWITCHABLE_FILTERS, 16);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), DAV1D_N_SWITCHABLE_FILTERS, 16);
                                    if (!false) break;
                                }
                            }
                            break :blk;
                        },
                        @as(c_int, 5) => {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))])), t_lsz, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx[@bitCast(@as(isize, @intCast(off)))])), t_lsz, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), @bitCast(y_mode_nofilt), 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)], 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))])), seg_pred, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 1, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))])), b.*.skip, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))])), if (has_chroma != 0) @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) else @as(c_int, 0), 32);
                                if (!false) break;
                            }
                            if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))])), COMP_INTER_NONE, 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))), 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), @as(u8, @bitCast(@as(i8, @truncate(-@as(c_int, 1))))), 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), DAV1D_N_SWITCHABLE_FILTERS, 32);
                                    if (!false) break;
                                }
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), DAV1D_N_SWITCHABLE_FILTERS, 32);
                                    if (!false) break;
                                }
                            }
                            break :blk;
                        },
                        else => {
                            while (true) {
                                if (!(@as(c_int, 0) != 0)) {
                                    unreachable;
                                }
                                if (!false) break;
                            }
                        },
                    }
                }
            }
        }
        if (@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)]) != 0) {
            f.*.bd_fn.copy_pal_block_y.?(t, bx4, by4, bw4, bh4);
        }
        if (has_chroma != 0) {
            var uv_mode: u8 = b.*.unnamed_0.unnamed_0.uv_mode;
            _ = &uv_mode;
            antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbw4))))))].?(@ptrCast(@alignCast(&t.*.a.*.uvmode[@bitCast(@as(isize, @intCast(cbx4)))])), uv_mode);
            antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbh4))))))].?(@ptrCast(@alignCast(&t.*.l.uvmode[@bitCast(@as(isize, @intCast(cby4)))])), uv_mode);
            if (@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) != 0) {
                f.*.bd_fn.copy_pal_block_uv.?(t, bx4, by4, bw4, bh4);
            }
        }
        if (((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) or (@as(enum_Dav1dFrameType, @bitCast(@as(c_int, f.*.frame_hdr.*.allow_intrabc))) != 0)) {
            splat_intraref(f.*.c, t, bs, bw4, bh4);
        }
    } else if (!((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0)) {
        var mvstack: [8]refmvs_candidate = undefined;
        _ = &mvstack;
        var n_mvs: c_int = undefined;
        _ = &n_mvs;
        var ctx: c_int = undefined;
        _ = &ctx;
        antfly_av1_dav1d_refmvs_find(&t.*.rt, @ptrCast(@alignCast(&mvstack)), &n_mvs, &ctx, union_refmvs_refpair{
            .ref = [2]i8{
                0,
                @truncate(-@as(c_int, 1)),
            },
        }, bs, intra_edge_flags, t.*.by, t.*.bx);
        if (mvstack[@as(c_int, 0)].mv.mv[@as(c_int, 0)].n != 0) {
            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[@as(c_int, 0)].mv.mv[@as(c_int, 0)];
        } else if (mvstack[@as(c_int, 1)].mv.mv[@as(c_int, 0)].n != 0) {
            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[@as(c_int, 1)].mv.mv[@as(c_int, 0)];
        } else {
            if ((t.*.by - (@as(c_int, 16) << @intCast(@as(c_int, f.*.seq_hdr.*.sb128)))) < ts.*.tiling.row_start) {
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y = 0;
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.x = @truncate(-(@as(c_int, 512) << @intCast(@as(c_int, f.*.seq_hdr.*.sb128))) - @as(c_int, 2048));
            } else {
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y = @truncate(-(@as(c_int, 512) << @intCast(@as(c_int, f.*.seq_hdr.*.sb128))));
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.x = 0;
            }
        }
        const ref: union_mv = b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)];
        _ = &ref;
        read_mv_residual(ts, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], -@as(c_int, 1));
        var border_left: c_int = ts.*.tiling.col_start * @as(c_int, 4);
        _ = &border_left;
        var border_top: c_int = ts.*.tiling.row_start * @as(c_int, 4);
        _ = &border_top;
        if (has_chroma != 0) {
            if ((bw4 < @as(c_int, 2)) and (ss_hor != 0)) {
                border_left += 4;
            }
            if ((bh4 < @as(c_int, 2)) and (ss_ver != 0)) {
                border_top += 4;
            }
        }
        var src_left: c_int = (t.*.bx * @as(c_int, 4)) + (@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.x) >> @intCast(@as(c_int, 3)));
        _ = &src_left;
        var src_top: c_int = (t.*.by * @as(c_int, 4)) + (@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y) >> @intCast(@as(c_int, 3)));
        _ = &src_top;
        var src_right: c_int = src_left + (bw4 * @as(c_int, 4));
        _ = &src_right;
        var src_bottom: c_int = src_top + (bh4 * @as(c_int, 4));
        _ = &src_bottom;
        const border_right: c_int = ((ts.*.tiling.col_end + (bw4 - @as(c_int, 1))) & ~(bw4 - @as(c_int, 1))) * @as(c_int, 4);
        _ = &border_right;
        if (src_left < border_left) {
            src_right += border_left - src_left;
            src_left += border_left - src_left;
        } else if (src_right > border_right) {
            src_left -= src_right - border_right;
            src_right -= src_right - border_right;
        }
        if (src_top < border_top) {
            src_bottom += border_top - src_top;
            src_top += border_top - src_top;
        }
        const sbx: c_int = (t.*.bx >> @intCast(@as(c_int, 4) + @as(c_int, f.*.seq_hdr.*.sb128))) << @intCast(@as(c_int, 6) + @as(c_int, f.*.seq_hdr.*.sb128));
        _ = &sbx;
        const sby: c_int = (t.*.by >> @intCast(@as(c_int, 4) + @as(c_int, f.*.seq_hdr.*.sb128))) << @intCast(@as(c_int, 6) + @as(c_int, f.*.seq_hdr.*.sb128));
        _ = &sby;
        const sb_size: c_int = @as(c_int, 1) << @intCast(@as(c_int, 6) + @as(c_int, f.*.seq_hdr.*.sb128));
        _ = &sb_size;
        if ((src_bottom > sby) and (src_right > sbx)) {
            if ((src_top - border_top) >= (src_bottom - sby)) {
                src_top -= src_bottom - sby;
                src_bottom -= src_bottom - sby;
            } else if ((src_left - border_left) >= (src_right - sbx)) {
                src_left -= src_right - sbx;
                src_right -= src_right - sbx;
            }
        }
        if (src_bottom > (sby + sb_size)) {
            src_top -= src_bottom - (sby + sb_size);
            src_bottom -= src_bottom - (sby + sb_size);
        }
        if ((src_bottom > sby) and (src_right > sbx)) return -@as(c_int, 1);
        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.x = @truncate((src_left - (t.*.bx * @as(c_int, 4))) * @as(c_int, 8));
        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y = @truncate((src_top - (t.*.by * @as(c_int, 4))) * @as(c_int, 8));
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
        read_vartx_tree(t, b, bs, bx4, by4);
        if (t.*.frame_thread.pass == @as(c_int, 1)) {
            f.*.bd_fn.read_coef_blocks.?(t, bs, b);
            b.*.unnamed_0.unnamed_1.filter2d = FILTER_2D_BILINEAR;
        } else {
            if (f.*.bd_fn.recon_b_inter.?(t, bs, b) != 0) return -@as(c_int, 1);
        }
        splat_intrabc_mv(f.*.c, t, bs, b, bw4, bh4);
        var edge: [*c]BlockContext = t.*.a;
        _ = &edge;
        {
            var i: c_int = 0;
            _ = &i;
            var off: c_int = bx4;
            _ = &off;
            while (i < @as(c_int, 2)) : ({
                i += 1;
                off = by4;
                edge = &t.*.l;
            }) {
                blk: {
                    switch (@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])) {
                        @as(c_int, 0) => {
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))]) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(DC_PRED * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(seg_pred * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.skip) * @as(c_int, 1))));
                            break :blk;
                        },
                        @as(c_int, 1) => {
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))]) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(DC_PRED * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(seg_pred * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.skip) * @as(c_int, 257))));
                            break :blk;
                        },
                        @as(c_int, 2) => {
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, DC_PRED))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, seg_pred))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.skip)))) *% @as(c_uint, 16843009);
                            break :blk;
                        },
                        @as(c_int, 3) => {
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, DC_PRED))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, seg_pred))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.skip)))) *% @as(c_ulonglong, 72340172838076673);
                            break :blk;
                        },
                        @as(c_int, 4) => {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))])), b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))], 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), DC_PRED, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))])), seg_pred, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))])), b.*.skip, 16);
                                if (!false) break;
                            }
                            break :blk;
                        },
                        @as(c_int, 5) => {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))])), b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))], 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), DC_PRED, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))])), seg_pred, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))])), b.*.skip, 32);
                                if (!false) break;
                            }
                            break :blk;
                        },
                        else => {
                            while (true) {
                                if (!(@as(c_int, 0) != 0)) {
                                    unreachable;
                                }
                                if (!false) break;
                            }
                        },
                    }
                }
            }
        }
        if (has_chroma != 0) {
            antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbw4))))))].?(@ptrCast(@alignCast(&t.*.a.*.uvmode[@bitCast(@as(isize, @intCast(cbx4)))])), DC_PRED);
            antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbh4))))))].?(@ptrCast(@alignCast(&t.*.l.uvmode[@bitCast(@as(isize, @intCast(cby4)))])), DC_PRED);
        }
    } else {
        var is_comp: c_int = undefined;
        _ = &is_comp;
        var has_subpel_filter: c_int = undefined;
        _ = &has_subpel_filter;
        if (@as(c_int, b.*.skip_mode) != 0) {
            is_comp = 1;
        } else if (((!(seg != null) or (((@as(c_int, seg.*.ref) == -@as(c_int, 1)) and !(@as(c_int, seg.*.globalmv) != 0)) and !(@as(c_int, seg.*.skip) != 0))) and (@as(c_int, f.*.frame_hdr.*.switchable_comp_refs) != 0)) and (imin(bw4, bh4) > @as(c_int, 1))) {
            const ctx: c_int = get_comp_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
            _ = &ctx;
            is_comp = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp[@bitCast(@as(isize, @intCast(ctx)))]))))));
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        } else {
            is_comp = 0;
        }
        if (@as(c_int, b.*.skip_mode) != 0) {
            b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = f.*.frame_hdr.*.skip_mode_refs[@as(c_int, 0)];
            b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)] = f.*.frame_hdr.*.skip_mode_refs[@as(c_int, 1)];
            b.*.unnamed_0.unnamed_1.comp_type = COMP_INTER_AVG;
            b.*.unnamed_0.unnamed_1.inter_mode = NEARESTMV_NEARESTMV;
            b.*.unnamed_0.unnamed_1.drl_idx = NEAREST_DRL;
            has_subpel_filter = 0;
            var mvstack: [8]refmvs_candidate = undefined;
            _ = &mvstack;
            var n_mvs: c_int = undefined;
            _ = &n_mvs;
            var ctx: c_int = undefined;
            _ = &ctx;
            antfly_av1_dav1d_refmvs_find(&t.*.rt, @ptrCast(@alignCast(&mvstack)), &n_mvs, &ctx, union_refmvs_refpair{
                .ref = [2]i8{
                    @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1)),
                    @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]) + @as(c_int, 1)),
                },
            }, bs, intra_edge_flags, t.*.by, t.*.bx);
            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[@as(c_int, 0)].mv.mv[@as(c_int, 0)];
            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)] = mvstack[@as(c_int, 0)].mv.mv[@as(c_int, 1)];
            fix_mv_precision(f.*.frame_hdr, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)]);
            fix_mv_precision(f.*.frame_hdr, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)]);
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        } else if (is_comp != 0) {
            const dir_ctx: c_int = get_comp_dir_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
            _ = &dir_ctx;
            if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_dir[@bitCast(@as(isize, @intCast(dir_ctx)))]))) != 0) {
                const ctx1: c_int = av1_get_fwd_ref_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                _ = &ctx1;
                if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_fwd_ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(ctx1)))]))) != 0) {
                    const ctx2: c_int = av1_get_fwd_ref_2_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                    _ = &ctx2;
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(@as(c_uint, 2) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_fwd_ref[@as(c_int, 2)][@bitCast(@as(isize, @intCast(ctx2)))]))))));
                } else {
                    const ctx2: c_int = av1_get_fwd_ref_1_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                    _ = &ctx2;
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_fwd_ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(ctx2)))]))))));
                }
                const ctx3: c_int = av1_get_bwd_ref_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                _ = &ctx3;
                if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_bwd_ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(ctx3)))]))) != 0) {
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)] = 6;
                } else {
                    const ctx4: c_int = av1_get_bwd_ref_1_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                    _ = &ctx4;
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)] = @bitCast(@as(u8, @truncate(@as(c_uint, 4) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_bwd_ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(ctx4)))]))))));
                }
            } else {
                const uctx_p: c_int = av1_get_ref_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                _ = &uctx_p;
                if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_uni_ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(uctx_p)))]))) != 0) {
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = 4;
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)] = 6;
                } else {
                    const uctx_p1: c_int = av1_get_uni_p1_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                    _ = &uctx_p1;
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = 0;
                    b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)] = @bitCast(@as(u8, @truncate(@as(c_uint, 1) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_uni_ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(uctx_p1)))]))))));
                    if (@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]) == @as(c_int, 2)) {
                        const uctx_p2: c_int = av1_get_fwd_ref_2_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                        _ = &uctx_p2;
                        {
                            const ref = &b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)];
                            ref.* = @bitCast(@as(u8, @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_uni_ref[@as(c_int, 2)][@bitCast(@as(isize, @intCast(uctx_p2)))]))))));
                        }
                    }
                }
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            var mvstack: [8]refmvs_candidate = undefined;
            _ = &mvstack;
            var n_mvs: c_int = undefined;
            _ = &n_mvs;
            var ctx: c_int = undefined;
            _ = &ctx;
            antfly_av1_dav1d_refmvs_find(&t.*.rt, @ptrCast(@alignCast(&mvstack)), &n_mvs, &ctx, union_refmvs_refpair{
                .ref = [2]i8{
                    @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1)),
                    @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]) + @as(c_int, 1)),
                },
            }, bs, intra_edge_flags, t.*.by, t.*.bx);
            b.*.unnamed_0.unnamed_1.inter_mode = @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.comp_inter_mode[@bitCast(@as(isize, @intCast(ctx)))])), @bitCast(@as(c_long, N_COMP_INTER_PRED_MODES - @as(c_int, 1)))));
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            const im: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_comp_inter_pred_modes[b.*.unnamed_0.unnamed_1.inter_mode]));
            _ = &im;
            b.*.unnamed_0.unnamed_1.drl_idx = NEAREST_DRL;
            if (@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == NEWMV_NEWMV) {
                if (n_mvs > @as(c_int, 1)) {
                    const drl_ctx_v1: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 0);
                    _ = &drl_ctx_v1;
                    {
                        const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v1)))]))));
                    }
                    if ((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) == NEARER_DRL) and (n_mvs > @as(c_int, 2))) {
                        const drl_ctx_v2: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 1);
                        _ = &drl_ctx_v2;
                        {
                            const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v2)))]))));
                        }
                    }
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                }
            } else if ((@as(c_int, im[@as(c_int, 0)]) == NEARMV) or (@as(c_int, im[@as(c_int, 1)]) == NEARMV)) {
                b.*.unnamed_0.unnamed_1.drl_idx = NEARER_DRL;
                if (n_mvs > @as(c_int, 2)) {
                    const drl_ctx_v2: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 1);
                    _ = &drl_ctx_v2;
                    {
                        const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v2)))]))));
                    }
                    if ((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) == NEAR_DRL) and (n_mvs > @as(c_int, 3))) {
                        const drl_ctx_v3: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 2);
                        _ = &drl_ctx_v3;
                        {
                            const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v3)))]))));
                        }
                    }
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                }
            }
            while (true) {
                if (!((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) >= NEAREST_DRL) and (@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) <= NEARISH_DRL))) {
                    unreachable;
                }
                if (!false) break;
            }
            has_subpel_filter = @as(c_int, @intFromBool((imin(bw4, bh4) == @as(c_int, 1)) or (@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) != GLOBALMV_GLOBALMV)));
            blk: {
                switch (@as(c_int, im[@as(c_int, 0)])) {
                    NEARMV, NEARESTMV => {
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[b.*.unnamed_0.unnamed_1.drl_idx].mv.mv[@as(c_int, 0)];
                        fix_mv_precision(f.*.frame_hdr, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)]);
                        break :blk;
                    },
                    GLOBALMV => {
                        has_subpel_filter |= @as(c_int, @intFromBool(f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))].type == @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION)));
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = get_gmv_2d(&f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))], t.*.bx, t.*.by, bw4, bh4, f.*.frame_hdr);
                        break :blk;
                    },
                    NEWMV => {
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[b.*.unnamed_0.unnamed_1.drl_idx].mv.mv[@as(c_int, 0)];
                        const mv_prec: c_int = @as(c_int, f.*.frame_hdr.*.hp) - @as(c_int, f.*.frame_hdr.*.force_integer_mv);
                        _ = &mv_prec;
                        read_mv_residual(ts, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], mv_prec);
                        break :blk;
                    },
                    else => {},
                }
            }
            blk: {
                switch (@as(c_int, im[@as(c_int, 1)])) {
                    NEARMV, NEARESTMV => {
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)] = mvstack[b.*.unnamed_0.unnamed_1.drl_idx].mv.mv[@as(c_int, 1)];
                        fix_mv_precision(f.*.frame_hdr, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)]);
                        break :blk;
                    },
                    GLOBALMV => {
                        has_subpel_filter |= @as(c_int, @intFromBool(f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)])))].type == @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION)));
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)] = get_gmv_2d(&f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)])))], t.*.bx, t.*.by, bw4, bh4, f.*.frame_hdr);
                        break :blk;
                    },
                    NEWMV => {
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)] = mvstack[b.*.unnamed_0.unnamed_1.drl_idx].mv.mv[@as(c_int, 1)];
                        const mv_prec: c_int = @as(c_int, f.*.frame_hdr.*.hp) - @as(c_int, f.*.frame_hdr.*.force_integer_mv);
                        _ = &mv_prec;
                        read_mv_residual(ts, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 1)], mv_prec);
                        break :blk;
                    },
                    else => {},
                }
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            var is_segwedge: c_int = 0;
            _ = &is_segwedge;
            if (@as(c_int, f.*.seq_hdr.*.masked_compound) != 0) {
                const mask_ctx: c_int = get_mask_comp_ctx(t.*.a, &t.*.l, by4, bx4);
                _ = &mask_ctx;
                is_segwedge = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.mask_comp[@bitCast(@as(isize, @intCast(mask_ctx)))]))))));
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            }
            if (!(is_segwedge != 0)) {
                if (@as(c_int, f.*.seq_hdr.*.jnt_comp) != 0) {
                    const jnt_ctx: c_int = get_jnt_comp_ctx(f.*.seq_hdr.*.order_hint_n_bits, f.*.cur.frame_hdr.*.frame_offset, f.*.refp[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))].p.frame_hdr.*.frame_offset, f.*.refp[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)])))].p.frame_hdr.*.frame_offset, t.*.a, &t.*.l, by4, bx4);
                    _ = &jnt_ctx;
                    b.*.unnamed_0.unnamed_1.comp_type = @truncate(@as(c_uint, COMP_INTER_WEIGHTED_AVG) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.jnt_comp[@bitCast(@as(isize, @intCast(jnt_ctx)))]))));
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                } else {
                    b.*.unnamed_0.unnamed_1.comp_type = COMP_INTER_AVG;
                }
            } else {
                if ((wedge_allowed_mask & @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @bitCast(bs))))))) != 0) {
                    const ctx_1: c_int = antfly_av1_dav1d_wedge_ctx_lut[bs];
                    _ = &ctx_1;
                    b.*.unnamed_0.unnamed_1.comp_type = @truncate(@as(c_uint, COMP_INTER_WEDGE) -% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.wedge_comp[@bitCast(@as(isize, @intCast(ctx_1)))]))));
                    if (@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) == COMP_INTER_WEDGE) {
                        b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.wedge_idx = @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.wedge_idx[@bitCast(@as(isize, @intCast(ctx_1)))])), 15));
                    }
                } else {
                    b.*.unnamed_0.unnamed_1.comp_type = COMP_INTER_SEG;
                }
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign = @truncate(antfly_av1_dav1d_msac_decode_bool_equi_c(&ts.*.msac));
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            }
        } else {
            b.*.unnamed_0.unnamed_1.comp_type = COMP_INTER_NONE;
            if ((seg != null) and (@as(c_int, seg.*.ref) > @as(c_int, 0))) {
                b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = @truncate(@as(c_int, seg.*.ref) - @as(c_int, 1));
            } else if ((seg != null) and ((@as(c_int, seg.*.globalmv) != 0) or (@as(c_int, seg.*.skip) != 0))) {
                b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = 0;
            } else {
                const ctx1: c_int = av1_get_ref_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                _ = &ctx1;
                if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(ctx1)))]))) != 0) {
                    const ctx2: c_int = av1_get_bwd_ref_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                    _ = &ctx2;
                    if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(ctx2)))]))) != 0) {
                        b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = 6;
                    } else {
                        const ctx3: c_int = av1_get_bwd_ref_1_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                        _ = &ctx3;
                        b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(@as(c_uint, 4) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.ref[@as(c_int, 5)][@bitCast(@as(isize, @intCast(ctx3)))]))))));
                    }
                } else {
                    const ctx2: c_int = av1_get_fwd_ref_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                    _ = &ctx2;
                    if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.ref[@as(c_int, 2)][@bitCast(@as(isize, @intCast(ctx2)))]))) != 0) {
                        const ctx3: c_int = av1_get_fwd_ref_2_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                        _ = &ctx3;
                        b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(@as(c_uint, 2) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.ref[@as(c_int, 4)][@bitCast(@as(isize, @intCast(ctx3)))]))))));
                    } else {
                        const ctx3: c_int = av1_get_fwd_ref_1_ctx(t.*.a, &t.*.l, by4, bx4, have_top, have_left);
                        _ = &ctx3;
                        b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)] = @bitCast(@as(u8, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.ref[@as(c_int, 3)][@bitCast(@as(isize, @intCast(ctx3)))]))))));
                    }
                }
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            }
            b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)] = @truncate(-@as(c_int, 1));
            var mvstack: [8]refmvs_candidate = undefined;
            _ = &mvstack;
            var n_mvs: c_int = undefined;
            _ = &n_mvs;
            var ctx: c_int = undefined;
            _ = &ctx;
            antfly_av1_dav1d_refmvs_find(&t.*.rt, @ptrCast(@alignCast(&mvstack)), &n_mvs, &ctx, union_refmvs_refpair{
                .ref = [2]i8{
                    @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1)),
                    @truncate(-@as(c_int, 1)),
                },
            }, bs, intra_edge_flags, t.*.by, t.*.bx);
            if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool((seg != null) and ((@as(c_int, seg.*.skip) != 0) or (@as(c_int, seg.*.globalmv) != 0))))))) != 0) or (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.newmv_mode[@bitCast(@as(isize, @intCast(ctx & @as(c_int, 7))))]))) != 0)) {
                if (((seg != null) and ((@as(c_int, seg.*.skip) != 0) or (@as(c_int, seg.*.globalmv) != 0))) or !(antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.globalmv_mode[@bitCast(@as(isize, @intCast((ctx >> @intCast(@as(c_int, 3))) & @as(c_int, 1))))]))) != 0)) {
                    b.*.unnamed_0.unnamed_1.inter_mode = GLOBALMV;
                    b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = get_gmv_2d(&f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))], t.*.bx, t.*.by, bw4, bh4, f.*.frame_hdr);
                    has_subpel_filter = @as(c_int, @intFromBool((imin(bw4, bh4) == @as(c_int, 1)) or (f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))].type == @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION))));
                } else {
                    has_subpel_filter = 1;
                    if (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.refmv_mode[@bitCast(@as(isize, @intCast((ctx >> @intCast(@as(c_int, 4))) & @as(c_int, 15))))]))) != 0) {
                        b.*.unnamed_0.unnamed_1.inter_mode = NEARMV;
                        b.*.unnamed_0.unnamed_1.drl_idx = NEARER_DRL;
                        if (n_mvs > @as(c_int, 2)) {
                            const drl_ctx_v2: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 1);
                            _ = &drl_ctx_v2;
                            {
                                const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v2)))]))));
                            }
                            if ((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) == NEAR_DRL) and (n_mvs > @as(c_int, 3))) {
                                const drl_ctx_v3: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 2);
                                _ = &drl_ctx_v3;
                                {
                                    const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v3)))]))));
                                }
                            }
                        }
                    } else {
                        b.*.unnamed_0.unnamed_1.inter_mode = NEARESTMV;
                        b.*.unnamed_0.unnamed_1.drl_idx = NEAREST_DRL;
                    }
                    while (true) {
                        if (!((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) >= NEAREST_DRL) and (@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) <= NEARISH_DRL))) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[b.*.unnamed_0.unnamed_1.drl_idx].mv.mv[@as(c_int, 0)];
                    if (@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) < NEAR_DRL) {
                        fix_mv_precision(f.*.frame_hdr, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)]);
                    }
                }
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            } else {
                has_subpel_filter = 1;
                b.*.unnamed_0.unnamed_1.inter_mode = NEWMV;
                b.*.unnamed_0.unnamed_1.drl_idx = NEAREST_DRL;
                if (n_mvs > @as(c_int, 1)) {
                    const drl_ctx_v1: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 0);
                    _ = &drl_ctx_v1;
                    {
                        const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v1)))]))));
                    }
                    if ((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) == NEARER_DRL) and (n_mvs > @as(c_int, 2))) {
                        const drl_ctx_v2: c_int = get_drl_context(@ptrCast(@alignCast(&mvstack)), 1);
                        _ = &drl_ctx_v2;
                        {
                            const ref = &b.*.unnamed_0.unnamed_1.drl_idx;
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.drl_bit[@bitCast(@as(isize, @intCast(drl_ctx_v2)))]))));
                        }
                    }
                }
                while (true) {
                    if (!((@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) >= NEAREST_DRL) and (@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) <= NEARISH_DRL))) {
                        unreachable;
                    }
                    if (!false) break;
                }
                if (n_mvs > @as(c_int, 1)) {
                    b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[b.*.unnamed_0.unnamed_1.drl_idx].mv.mv[@as(c_int, 0)];
                } else {
                    while (true) {
                        if (!!(@as(c_int, b.*.unnamed_0.unnamed_1.drl_idx) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)] = mvstack[@as(c_int, 0)].mv.mv[@as(c_int, 0)];
                    fix_mv_precision(f.*.frame_hdr, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)]);
                }
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
                const mv_prec: c_int = @as(c_int, f.*.frame_hdr.*.hp) - @as(c_int, f.*.frame_hdr.*.force_integer_mv);
                _ = &mv_prec;
                read_mv_residual(ts, &b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], mv_prec);
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            }
            const ii_sz_grp: c_int = antfly_av1_dav1d_ymode_size_context[bs];
            _ = &ii_sz_grp;
            if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool((@as(c_uint, @bitCast(@as(c_int, @as(c_int, f.*.seq_hdr.*.inter_intra)))) != 0) and ((interintra_allowed_mask & @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @bitCast(bs))))))) != 0)))))) != 0) and (antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.interintra[@bitCast(@as(isize, @intCast(ii_sz_grp)))]))) != 0)) {
                b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode = @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.interintra_mode[@bitCast(@as(isize, @intCast(ii_sz_grp)))])), @bitCast(@as(c_long, N_INTER_INTRA_PRED_MODES - @as(c_int, 1)))));
                const wedge_ctx: c_int = antfly_av1_dav1d_wedge_ctx_lut[bs];
                _ = &wedge_ctx;
                b.*.unnamed_0.unnamed_1.interintra_type = @truncate(@as(c_uint, INTER_INTRA_BLEND) +% antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.interintra_wedge[@bitCast(@as(isize, @intCast(wedge_ctx)))]))));
                if (@as(c_int, b.*.unnamed_0.unnamed_1.interintra_type) == INTER_INTRA_WEDGE) {
                    b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.wedge_idx = @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.wedge_idx[@bitCast(@as(isize, @intCast(wedge_ctx)))])), 15));
                }
            } else {
                b.*.unnamed_0.unnamed_1.interintra_type = INTER_INTRA_NONE;
            }
            if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool((((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and (@as(c_int, f.*.seq_hdr.*.inter_intra) != 0)))))) != 0) and ((interintra_allowed_mask & @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @bitCast(bs))))))) != 0)) {
                _ = @as(c_int, 0);
            }
            if (((((@as(c_int, f.*.frame_hdr.*.switchable_motion_mode) != 0) and (@as(c_int, b.*.unnamed_0.unnamed_1.interintra_type) == INTER_INTRA_NONE)) and (imin(bw4, bh4) >= @as(c_int, 2))) and !((!(@as(c_int, f.*.frame_hdr.*.force_integer_mv) != 0) and (@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV)) and (f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))].type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION)))) and (((have_left != 0) and (findoddzero(&t.*.l.intra[@bitCast(@as(isize, @intCast(by4 + @as(c_int, 1))))], h4 >> @intCast(@as(c_int, 1))) != 0)) or ((have_top != 0) and (findoddzero(&t.*.a.*.intra[@bitCast(@as(isize, @intCast(bx4 + @as(c_int, 1))))], w4 >> @intCast(@as(c_int, 1))) != 0)))) {
                var mask: [2]u64 = [2]u64{
                    0,
                    0,
                };
                _ = &mask;
                find_matching_ref(t, intra_edge_flags, bw4, bh4, w4, h4, have_left, have_top, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], @ptrCast(@alignCast(&mask)));
                const allow_warp: c_int = @as(c_int, @intFromBool((@as(u64, @bitCast(@as(c_longlong, @intFromBool((!(f.*.svc[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 0)].scale != 0) and !(@as(c_int, f.*.frame_hdr.*.force_integer_mv) != 0)) and (@as(c_int, f.*.frame_hdr.*.warp_motion) != 0))))) != 0) and ((mask[@as(c_int, 0)] | mask[@as(c_int, 1)]) != 0)));
                _ = &allow_warp;
                b.*.unnamed_0.unnamed_1.motion_mode = @truncate(if (allow_warp != 0) antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.motion_mode[bs])), 2) else antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.obmc[bs]))));
                if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) {
                    has_subpel_filter = 0;
                    derive_warpmv(t, bw4, bh4, @ptrCast(@alignCast(&mask)), b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], &t.*.warpmv);
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                    if (t.*.frame_thread.pass != 0) {
                        if (t.*.warpmv.type == @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_AFFINE)) {
                            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 0)] = @truncate(t.*.warpmv.matrix[@as(c_int, 2)] - @as(c_int, 65536));
                            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 1)] = @truncate(t.*.warpmv.matrix[@as(c_int, 3)]);
                            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 2)] = @truncate(t.*.warpmv.matrix[@as(c_int, 4)]);
                            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 3)] = @truncate(t.*.warpmv.matrix[@as(c_int, 5)] - @as(c_int, 65536));
                        } else {
                            b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_1.matrix[@as(c_int, 0)] = @truncate(-@as(c_int, 32767) - @as(c_int, 1));
                        }
                    }
                }
                if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                    _ = @as(c_int, 0);
                }
            } else {
                b.*.unnamed_0.unnamed_1.motion_mode = MM_TRANSLATION;
            }
        }
        var filter: [2]enum_Dav1dFilterMode = undefined;
        _ = &filter;
        if (f.*.frame_hdr.*.subpel_filter_mode == @as(enum_Dav1dFilterMode, DAV1D_FILTER_SWITCHABLE)) {
            if (has_subpel_filter != 0) {
                const comp: c_int = @as(c_int, @intFromBool(@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) != COMP_INTER_NONE));
                _ = &comp;
                const ctx1: c_int = get_filter_ctx(t.*.a, &t.*.l, comp, 0, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], by4, bx4);
                _ = &ctx1;
                filter[@as(c_int, 0)] = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(ctx1)))])), @bitCast(@as(c_long, DAV1D_N_SWITCHABLE_FILTERS - @as(c_int, 1))));
                if (@as(c_int, f.*.seq_hdr.*.dual_filter) != 0) {
                    const ctx2: c_int = get_filter_ctx(t.*.a, &t.*.l, comp, 1, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], by4, bx4);
                    _ = &ctx2;
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                    filter[@as(c_int, 1)] = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(ctx2)))])), @bitCast(@as(c_long, DAV1D_N_SWITCHABLE_FILTERS - @as(c_int, 1))));
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                } else {
                    filter[@as(c_int, 1)] = filter[@as(c_int, 0)];
                    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                        _ = @as(c_int, 0);
                    }
                }
            } else {
                filter[@as(c_int, 0)] = blk: {
                    const tmp = @as(enum_Dav1dFilterMode, DAV1D_FILTER_8TAP_REGULAR);
                    filter[@as(c_int, 1)] = tmp;
                    break :blk tmp;
                };
            }
        } else {
            filter[@as(c_int, 0)] = blk: {
                const tmp = f.*.frame_hdr.*.subpel_filter_mode;
                filter[@as(c_int, 1)] = tmp;
                break :blk tmp;
            };
        }
        b.*.unnamed_0.unnamed_1.filter2d = antfly_av1_dav1d_filter_2d[filter[@as(c_int, 1)]][filter[@as(c_int, 0)]];
        read_vartx_tree(t, b, bs, bx4, by4);
        if (t.*.frame_thread.pass == @as(c_int, 1)) {
            f.*.bd_fn.read_coef_blocks.?(t, bs, b);
        } else {
            if (f.*.bd_fn.recon_b_inter.?(t, bs, b) != 0) return -@as(c_int, 1);
        }
        if ((@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) or (@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0)) {
            const is_globalmv: c_int = @as(c_int, @intFromBool(@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == (if (is_comp != 0) GLOBALMV_GLOBALMV else GLOBALMV)));
            _ = &is_globalmv;
            const lf_lvls: [*c][8][2]u8 = @ptrCast(@alignCast(@constCast(&ts.*.lflvl[b.*.seg_id][@as(c_int, 0)][@bitCast(@as(isize, @intCast(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) + @as(c_int, 1))))][@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(is_globalmv != 0))))))])));
            _ = &lf_lvls;
            const tx_split: [2]u16 = [2]u16{
                b.*.unnamed_0.unnamed_1.tx_split0,
                b.*.unnamed_0.unnamed_1.tx_split1,
            };
            _ = &tx_split;
            var ytx: enum_RectTxfmSize = b.*.unnamed_0.unnamed_1.max_ytx;
            _ = &ytx;
            var uvtx: enum_RectTxfmSize = b.*.uvtx;
            _ = &uvtx;
            if (@as(c_int, f.*.frame_hdr.*.segmentation.lossless[b.*.seg_id]) != 0) {
                ytx = @bitCast(TX_4X4);
                uvtx = @bitCast(TX_4X4);
            }
            antfly_av1_dav1d_create_lf_mask_inter(t.*.lf_mask, f.*.lf.level, f.*.b4_stride, lf_lvls, t.*.bx, t.*.by, f.*.w4, f.*.h4, b.*.skip, bs, ytx, @ptrCast(@alignCast(&tx_split)), uvtx, f.*.cur.p.layout, &t.*.a.*.tx_lpf_y[@bitCast(@as(isize, @intCast(bx4)))], &t.*.l.tx_lpf_y[@bitCast(@as(isize, @intCast(by4)))], @ptrCast(@alignCast(if (has_chroma != 0) @as(?*anyopaque, @ptrCast(@alignCast(&t.*.a.*.tx_lpf_uv[@bitCast(@as(isize, @intCast(cbx4)))]))) else @as(?*anyopaque, null))), @ptrCast(@alignCast(if (has_chroma != 0) @as(?*anyopaque, @ptrCast(@alignCast(&t.*.l.tx_lpf_uv[@bitCast(@as(isize, @intCast(cby4)))]))) else @as(?*anyopaque, null))));
        }
        if (is_comp != 0) {
            splat_tworef_mv(f.*.c, t, bs, b, bw4, bh4);
        } else {
            splat_oneref_mv(f.*.c, t, bs, b, bw4, bh4);
        }
        var edge: [*c]BlockContext = t.*.a;
        _ = &edge;
        {
            var i: c_int = 0;
            _ = &i;
            var off: c_int = bx4;
            _ = &off;
            while (i < @as(c_int, 2)) : ({
                i += 1;
                off = by4;
                edge = &t.*.l;
            }) {
                blk: {
                    switch (@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])) {
                        @as(c_int, 0) => {
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(seg_pred * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.skip_mode) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.skip) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, 0) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))]) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @truncate(filter[@as(c_int, 0)] *% @as(enum_Dav1dFilterMode, 1));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @truncate(filter[@as(c_int, 1)] *% @as(enum_Dav1dFilterMode, 1));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, @as(u8, @bitCast(@as(i8, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)])))) * @as(c_int, 1))));
                            break :blk;
                        },
                        @as(c_int, 1) => {
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(seg_pred * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.skip_mode) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.skip) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, 0) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))]) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @truncate(filter[@as(c_int, 0)] *% @as(enum_Dav1dFilterMode, 257));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @truncate(filter[@as(c_int, 1)] *% @as(enum_Dav1dFilterMode, 257));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)]) * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, @as(u8, @bitCast(@as(i8, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)])))) * @as(c_int, 257))));
                            break :blk;
                        },
                        @as(c_int, 2) => {
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, seg_pred))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.skip_mode)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.skip)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, 0)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.unnamed_0.unnamed_1.comp_type)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = filter[@as(c_int, 0)] *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = filter[@as(c_int, 1)] *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.unnamed_0.unnamed_1.inter_mode)))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, @as(u8, @bitCast(@as(i8, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]))))))) *% @as(c_uint, 16843009);
                            break :blk;
                        },
                        @as(c_int, 3) => {
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, seg_pred))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.skip_mode)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.skip)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, 0)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))])))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.unnamed_0.unnamed_1.comp_type)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, filter[@as(c_int, 0)]) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, filter[@as(c_int, 1)]) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.unnamed_0.unnamed_1.inter_mode)))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, @as(u8, @bitCast(@as(i8, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]))))))) *% @as(c_ulonglong, 72340172838076673);
                            break :blk;
                        },
                        @as(c_int, 4) => {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))])), seg_pred, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))])), b.*.skip_mode, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))])), b.*.skip, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))])), 0, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))])), b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))], 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_1.comp_type, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), @bitCast(filter[@as(c_int, 0)]), 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), @bitCast(filter[@as(c_int, 1)]), 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_1.inter_mode, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), @as(u8, @bitCast(@as(i8, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]))), 16);
                                if (!false) break;
                            }
                            break :blk;
                        },
                        @as(c_int, 5) => {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.seg_pred[@bitCast(@as(isize, @intCast(off)))])), seg_pred, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip_mode[@bitCast(@as(isize, @intCast(off)))])), b.*.skip_mode, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.intra[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.skip[@bitCast(@as(isize, @intCast(off)))])), b.*.skip, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.pal_sz[@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.pal_sz_uv[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(off)))])), 0, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.tx_intra[@bitCast(@as(isize, @intCast(off)))])), b_dim[@bitCast(@as(isize, @intCast(@as(c_int, 2) + i)))], 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.comp_type[@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_1.comp_type, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), @bitCast(filter[@as(c_int, 0)]), 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), @bitCast(filter[@as(c_int, 1)]), 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.mode[@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_1.inter_mode, 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 0)][@bitCast(@as(isize, @intCast(off)))])), b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], 32);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&edge.*.ref[@as(c_int, 1)][@bitCast(@as(isize, @intCast(off)))])), @as(u8, @bitCast(@as(i8, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)]))), 32);
                                if (!false) break;
                            }
                            break :blk;
                        },
                        else => {
                            while (true) {
                                if (!(@as(c_int, 0) != 0)) {
                                    unreachable;
                                }
                                if (!false) break;
                            }
                        },
                    }
                }
            }
        }
        if (has_chroma != 0) {
            antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbw4))))))].?(@ptrCast(@alignCast(&t.*.a.*.uvmode[@bitCast(@as(isize, @intCast(cbx4)))])), DC_PRED);
            antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbh4))))))].?(@ptrCast(@alignCast(&t.*.l.uvmode[@bitCast(@as(isize, @intCast(cby4)))])), DC_PRED);
        }
    }
    if ((@as(c_int, f.*.frame_hdr.*.segmentation.enabled) != 0) and (@as(c_int, f.*.frame_hdr.*.segmentation.update_map) != 0)) {
        var seg_ptr: [*c]u8 = &f.*.cur_segmap[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, t.*.by) * f.*.b4_stride) + @as(ptrdiff_t, t.*.bx))))];
        _ = &seg_ptr;
        blk: {
            switch (@as(c_int, b_dim[@as(c_int, 2)])) {
                @as(c_int, 0) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < bh4) : (y += 1) {
                            @as([*c]union_alias8, @ptrCast(@alignCast(&seg_ptr[@as(c_int, 0)]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, b.*.seg_id) * @as(c_int, 1))));
                            seg_ptr += @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride))));
                        }
                    }
                    break :blk;
                },
                @as(c_int, 1) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < bh4) : (y += 1) {
                            @as([*c]union_alias16, @ptrCast(@alignCast(&seg_ptr[@as(c_int, 0)]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, b.*.seg_id) * @as(c_int, 257))));
                            seg_ptr += @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride))));
                        }
                    }
                    break :blk;
                },
                @as(c_int, 2) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < bh4) : (y += 1) {
                            @as([*c]union_alias32, @ptrCast(@alignCast(&seg_ptr[@as(c_int, 0)]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, b.*.seg_id)))) *% @as(c_uint, 16843009);
                            seg_ptr += @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride))));
                        }
                    }
                    break :blk;
                },
                @as(c_int, 3) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < bh4) : (y += 1) {
                            @as([*c]union_alias64, @ptrCast(@alignCast(&seg_ptr[@as(c_int, 0)]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, b.*.seg_id)))) *% @as(c_ulonglong, 72340172838076673);
                            seg_ptr += @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride))));
                        }
                    }
                    break :blk;
                },
                @as(c_int, 4) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < bh4) : (y += 1) {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&seg_ptr[@as(c_int, 0)])), b.*.seg_id, 16);
                                if (!false) break;
                            }
                            seg_ptr += @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride))));
                        }
                    }
                    break :blk;
                },
                @as(c_int, 5) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < bh4) : (y += 1) {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&seg_ptr[@as(c_int, 0)])), b.*.seg_id, 32);
                                if (!false) break;
                            }
                            seg_ptr += @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride))));
                        }
                    }
                    break :blk;
                },
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                },
            }
        }
    }
    if (!(@as(c_int, b.*.skip) != 0)) {
        var noskip_mask: [*c][2]u16 = &t.*.lf_mask.*.noskip_mask[@bitCast(@as(isize, @intCast(by4 >> @intCast(@as(c_int, 1)))))];
        _ = &noskip_mask;
        const mask: c_uint = (~@as(c_uint, 0) >> @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 32) - bw4))))) << @intCast(@as(c_uint, @bitCast(@as(c_int, bx4 & @as(c_int, 15)))));
        _ = &mask;
        const bx_idx: c_int = (bx4 & @as(c_int, 16)) >> @intCast(@as(c_int, 4));
        _ = &bx_idx;
        {
            var y: c_int = 0;
            _ = &y;
            while (y < bh4) : ({
                y += 2;
                noskip_mask += 1;
            }) {
                {
                    const ref = &noskip_mask.*[@bitCast(@as(isize, @intCast(bx_idx)))];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | mask);
                }
                if (bw4 == @as(c_int, 32)) {
                    {
                        const ref = &noskip_mask.*[@as(c_int, 1)];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | mask);
                    }
                }
            }
        }
    }
    if (((t.*.frame_thread.pass == @as(c_int, 1)) and !(@as(c_int, b.*.intra) != 0)) and (@as(c_int, @bitCast(f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1))) != 0)) {
        const sby: c_int = (t.*.by - ts.*.tiling.row_start) >> @intCast(f.*.sb_shift);
        _ = &sby;
        const lowest_px: [*c][2]c_int = @ptrCast(@alignCast(&ts.*.lowest_pixel[@bitCast(@as(isize, @intCast(sby)))]));
        _ = &lowest_px;
        if (@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) == COMP_INTER_NONE) {
            if ((imin(bw4, bh4) > @as(c_int, 1)) and (((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV) and (@as(c_int, f.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]) != 0)) or ((@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) and (t.*.warpmv.type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION))))) {
                affine_lowest_px_luma(t, &lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 0)], b_dim, if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) &t.*.warpmv else &f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]);
            } else {
                mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 0)], t.*.by, bh4, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y, 0, &f.*.svc[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 1)]);
                if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_OBMC) {
                    obmc_lowest_px(t, lowest_px, 0, b_dim, bx4, by4, w4, h4);
                }
            }
            if (has_chroma != 0) {
                var is_sub8x8: c_int = @as(c_int, @intFromBool((bw4 == ss_hor) or (bh4 == ss_ver)));
                _ = &is_sub8x8;
                var r: [*c]const [*c]refmvs_block = undefined;
                _ = &r;
                if (is_sub8x8 != 0) {
                    while (true) {
                        if (!(ss_hor == @as(c_int, 1))) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    r = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
                    if (bw4 == @as(c_int, 1)) {
                        is_sub8x8 &= @as(c_int, @intFromBool(@as(c_int, r[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) > @as(c_int, 0)));
                    }
                    if (bh4 == ss_ver) {
                        is_sub8x8 &= @as(c_int, @intFromBool(@as(c_int, r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))].ref.ref[@as(c_int, 0)]) > @as(c_int, 0)));
                    }
                    if ((bw4 == @as(c_int, 1)) and (bh4 == ss_ver)) {
                        is_sub8x8 &= @as(c_int, @intFromBool(@as(c_int, r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) > @as(c_int, 0)));
                    }
                }
                if (is_sub8x8 != 0) {
                    while (true) {
                        if (!(ss_hor == @as(c_int, 1))) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    if ((bw4 == @as(c_int, 1)) and (bh4 == ss_ver)) {
                        const rr: [*c]const refmvs_block = &r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))];
                        _ = &rr;
                        mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(@as(c_int, rr.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)], t.*.by - @as(c_int, 1), bh4, rr.*.mv.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(@as(c_int, rr.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)]);
                    }
                    if (bw4 == @as(c_int, 1)) {
                        const rr: [*c]const refmvs_block = &r[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))];
                        _ = &rr;
                        mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(@as(c_int, rr.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)], t.*.by, bh4, rr.*.mv.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(@as(c_int, rr.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)]);
                    }
                    if (bh4 == ss_ver) {
                        const rr: [*c]const refmvs_block = &r[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))];
                        _ = &rr;
                        mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(@as(c_int, rr.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)], t.*.by - @as(c_int, 1), bh4, rr.*.mv.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(@as(c_int, rr.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))][@as(c_int, 1)]);
                    }
                    mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 1)], t.*.by, bh4, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 1)]);
                } else {
                    if ((imin(cbw4, cbh4) > @as(c_int, 1)) and (((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV) and (@as(c_int, f.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]) != 0)) or ((@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) and (t.*.warpmv.type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION))))) {
                        affine_lowest_px_chroma(t, &lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 1)], b_dim, if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) &t.*.warpmv else &f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]);
                    } else {
                        mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 1)], t.*.by & ~ss_ver, bh4 << @intCast(@as(c_int, @intFromBool(bh4 == ss_ver))), b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@as(c_int, 1)]);
                        if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_OBMC) {
                            obmc_lowest_px(t, lowest_px, 1, b_dim, bx4, by4, w4, h4);
                        }
                    }
                }
            }
        } else {
            {
                var i: c_int = 0;
                _ = &i;
                while (i < @as(c_int, 2)) : (i += 1) {
                    if ((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV_GLOBALMV) and (@as(c_int, f.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))]) != 0)) {
                        affine_lowest_px_luma(t, &lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))][@as(c_int, 0)], b_dim, &f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))]);
                    } else {
                        mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))][@as(c_int, 0)], t.*.by, bh4, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@bitCast(@as(isize, @intCast(i)))].unnamed_0.y, 0, &f.*.svc[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))][@as(c_int, 1)]);
                    }
                }
            }
            if (has_chroma != 0) {
                var i: c_int = 0;
                _ = &i;
                while (i < @as(c_int, 2)) : (i += 1) {
                    if (((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV_GLOBALMV) and (imin(cbw4, cbh4) > @as(c_int, 1))) and (@as(c_int, f.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))]) != 0)) {
                        affine_lowest_px_chroma(t, &lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))][@as(c_int, 1)], b_dim, &f.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))]);
                    } else {
                        mc_lowest_px(&lowest_px[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))][@as(c_int, 1)], t.*.by, bh4, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@bitCast(@as(isize, @intCast(i)))].unnamed_0.y, ss_ver, &f.*.svc[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(i)))])))][@as(c_int, 1)]);
                    }
                }
            }
        }
    }
    return 0;
}
pub fn decode_sb(t: [*c]Dav1dTaskContext, bl: enum_BlockLevel, node: [*c]const EdgeNode) callconv(.c) c_int {
    _ = &t;
    _ = &bl;
    _ = &node;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const hsz: c_int = @as(c_int, 16) >> @intCast(@as(c_int, @bitCast(bl)));
    _ = &hsz;
    const have_h_split: c_int = @as(c_int, @intFromBool(f.*.bw > (t.*.bx + hsz)));
    _ = &have_h_split;
    const have_v_split: c_int = @as(c_int, @intFromBool(f.*.bh > (t.*.by + hsz)));
    _ = &have_v_split;
    if (!(have_h_split != 0) and !(have_v_split != 0)) {
        while (true) {
            if (!(bl < @as(enum_BlockLevel, BL_8X8))) {
                unreachable;
            }
            if (!false) break;
        }
        return decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 0)])))))));
    }
    var pc: [*c]u16 = undefined;
    _ = &pc;
    var bp: enum_BlockPartition = undefined;
    _ = &bp;
    var ctx: c_int = undefined;
    _ = &ctx;
    var bx8: c_int = undefined;
    _ = &bx8;
    var by8: c_int = undefined;
    _ = &by8;
    if (t.*.frame_thread.pass != @as(c_int, 2)) {
        if (false and (bl == @as(enum_BlockLevel, BL_64X64))) {
            _ = @as(c_int, 0);
        }
        bx8 = (t.*.bx & @as(c_int, 31)) >> @intCast(@as(c_int, 1));
        by8 = (t.*.by & @as(c_int, 31)) >> @intCast(@as(c_int, 1));
        ctx = get_partition_ctx(t.*.a, &t.*.l, bl, by8, bx8);
        pc = @ptrCast(@alignCast(&ts.*.cdf.m.partition[bl][@bitCast(@as(isize, @intCast(ctx)))]));
    }
    if ((have_h_split != 0) and (have_v_split != 0)) {
        if (t.*.frame_thread.pass == @as(c_int, 2)) {
            const b: [*c]const Av1Block = &f.*.frame_thread.b[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, t.*.by) * f.*.b4_stride) + @as(ptrdiff_t, t.*.bx))))];
            _ = &b;
            bp = @bitCast(if (@as(c_int, b.*.bl) == @as(c_int, @bitCast(bl))) @as(c_int, b.*.bp) else PARTITION_SPLIT);
        } else {
            bp = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, pc, antfly_av1_dav1d_partition_type_count[bl]);
            if ((f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I422)) and ((((bp == @as(enum_BlockPartition, PARTITION_V)) or (bp == @as(enum_BlockPartition, PARTITION_V4))) or (bp == @as(enum_BlockPartition, PARTITION_T_LEFT_SPLIT))) or (bp == @as(enum_BlockPartition, PARTITION_T_RIGHT_SPLIT)))) {
                return 1;
            }
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        const b: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_sizes[bl][bp]));
        _ = &b;
        blk: {
            switch (bp) {
                @as(enum_BlockPartition, PARTITION_NONE) => {
                    if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_NONE, node.*.o) != 0) return -@as(c_int, 1);
                    break :blk;
                },
                @as(enum_BlockPartition, PARTITION_H) => {
                    if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_H, node.*.h[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                    t.*.by += hsz;
                    if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_H, node.*.h[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                    t.*.by -= hsz;
                    break :blk;
                },
                @as(enum_BlockPartition, PARTITION_V) => {
                    if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_V, node.*.v[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                    t.*.bx += hsz;
                    if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_V, node.*.v[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                    t.*.bx -= hsz;
                    break :blk;
                },
                @as(enum_BlockPartition, PARTITION_SPLIT) => {
                    if (bl == @as(enum_BlockLevel, BL_8X8)) {
                        const tip: [*c]const EdgeTip = @ptrCast(@alignCast(node));
                        _ = &tip;
                        while (true) {
                            if (!(hsz == @as(c_int, 1))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        if (decode_b(t, bl, BS_4x4, PARTITION_SPLIT, EDGE_ALL_TR_AND_BL) != 0) return -@as(c_int, 1);
                        const tl_filter: enum_Filter2d = t.*.tl_4x4_filter;
                        _ = &tl_filter;
                        t.*.bx += 1;
                        if (decode_b(t, bl, BS_4x4, PARTITION_SPLIT, tip.*.split[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.bx -= 1;
                        t.*.by += 1;
                        if (decode_b(t, bl, BS_4x4, PARTITION_SPLIT, tip.*.split[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.bx += 1;
                        t.*.tl_4x4_filter = tl_filter;
                        if (decode_b(t, bl, BS_4x4, PARTITION_SPLIT, tip.*.split[@as(c_int, 2)]) != 0) return -@as(c_int, 1);
                        t.*.bx -= 1;
                        t.*.by -= 1;
                    } else {
                        if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 0)]))))))) != 0) return 1;
                        t.*.bx += hsz;
                        if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 1)]))))))) != 0) return 1;
                        t.*.bx -= hsz;
                        t.*.by += hsz;
                        if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 2)]))))))) != 0) return 1;
                        t.*.bx += hsz;
                        if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 3)]))))))) != 0) return 1;
                        t.*.bx -= hsz;
                        t.*.by -= hsz;
                    }
                    break :blk;
                },
                @as(enum_BlockPartition, PARTITION_T_TOP_SPLIT) => {
                    {
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_T_TOP_SPLIT, EDGE_ALL_TR_AND_BL) != 0) return -@as(c_int, 1);
                        t.*.bx += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_T_TOP_SPLIT, node.*.v[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.bx -= hsz;
                        t.*.by += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 1)], PARTITION_T_TOP_SPLIT, node.*.h[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.by -= hsz;
                        break :blk;
                    }
                },
                @as(enum_BlockPartition, PARTITION_T_BOTTOM_SPLIT) => {
                    {
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_T_BOTTOM_SPLIT, node.*.h[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.by += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 1)], PARTITION_T_BOTTOM_SPLIT, node.*.v[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.bx += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 1)], PARTITION_T_BOTTOM_SPLIT, 0) != 0) return -@as(c_int, 1);
                        t.*.bx -= hsz;
                        t.*.by -= hsz;
                        break :blk;
                    }
                },
                @as(enum_BlockPartition, PARTITION_T_LEFT_SPLIT) => {
                    {
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_T_LEFT_SPLIT, EDGE_ALL_TR_AND_BL) != 0) return -@as(c_int, 1);
                        t.*.by += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_T_LEFT_SPLIT, node.*.h[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.by -= hsz;
                        t.*.bx += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 1)], PARTITION_T_LEFT_SPLIT, node.*.v[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.bx -= hsz;
                        break :blk;
                    }
                },
                @as(enum_BlockPartition, PARTITION_T_RIGHT_SPLIT) => {
                    {
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_T_RIGHT_SPLIT, node.*.v[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.bx += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 1)], PARTITION_T_RIGHT_SPLIT, node.*.h[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.by += hsz;
                        if (decode_b(t, bl, b[@as(c_int, 1)], PARTITION_T_RIGHT_SPLIT, 0) != 0) return -@as(c_int, 1);
                        t.*.by -= hsz;
                        t.*.bx -= hsz;
                        break :blk;
                    }
                },
                @as(enum_BlockPartition, PARTITION_H4) => {
                    {
                        const branch: [*c]const EdgeBranch = @ptrCast(@alignCast(node));
                        _ = &branch;
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_H4, node.*.h[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.by += hsz >> @intCast(@as(c_int, 1));
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_H4, branch.*.h4) != 0) return -@as(c_int, 1);
                        t.*.by += hsz >> @intCast(@as(c_int, 1));
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_H4, EDGE_ALL_LEFT_HAS_BOTTOM) != 0) return -@as(c_int, 1);
                        t.*.by += hsz >> @intCast(@as(c_int, 1));
                        if (t.*.by < f.*.bh) if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_H4, node.*.h[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.by -= (hsz * @as(c_int, 3)) >> @intCast(@as(c_int, 1));
                        break :blk;
                    }
                },
                @as(enum_BlockPartition, PARTITION_V4) => {
                    {
                        const branch: [*c]const EdgeBranch = @ptrCast(@alignCast(node));
                        _ = &branch;
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_V4, node.*.v[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
                        t.*.bx += hsz >> @intCast(@as(c_int, 1));
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_V4, branch.*.v4) != 0) return -@as(c_int, 1);
                        t.*.bx += hsz >> @intCast(@as(c_int, 1));
                        if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_V4, EDGE_ALL_TOP_HAS_RIGHT) != 0) return -@as(c_int, 1);
                        t.*.bx += hsz >> @intCast(@as(c_int, 1));
                        if (t.*.bx < f.*.bw) if (decode_b(t, bl, b[@as(c_int, 0)], PARTITION_V4, node.*.v[@as(c_int, 1)]) != 0) return -@as(c_int, 1);
                        t.*.bx -= (hsz * @as(c_int, 3)) >> @intCast(@as(c_int, 1));
                        break :blk;
                    }
                },
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                },
            }
        }
    } else if (have_h_split != 0) {
        var is_split: c_uint = undefined;
        _ = &is_split;
        if (t.*.frame_thread.pass == @as(c_int, 2)) {
            const b: [*c]const Av1Block = &f.*.frame_thread.b[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, t.*.by) * f.*.b4_stride) + @as(ptrdiff_t, t.*.bx))))];
            _ = &b;
            is_split = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, b.*.bl) != @as(c_int, @bitCast(bl))))));
        } else {
            is_split = antfly_av1_dav1d_msac_decode_bool_c(&ts.*.msac, gather_top_partition_prob(pc, bl));
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        while (true) {
            if (!(bl < @as(enum_BlockLevel, BL_8X8))) {
                unreachable;
            }
            if (!false) break;
        }
        if (is_split != 0) {
            bp = PARTITION_SPLIT;
            if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 0)]))))))) != 0) return 1;
            t.*.bx += hsz;
            if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 1)]))))))) != 0) return 1;
            t.*.bx -= hsz;
        } else {
            bp = PARTITION_H;
            if (decode_b(t, bl, antfly_av1_dav1d_block_sizes[bl][PARTITION_H][@as(c_int, 0)], PARTITION_H, node.*.h[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
        }
    } else {
        while (true) {
            if (!(have_v_split != 0)) {
                unreachable;
            }
            if (!false) break;
        }
        var is_split: c_uint = undefined;
        _ = &is_split;
        if (t.*.frame_thread.pass == @as(c_int, 2)) {
            const b: [*c]const Av1Block = &f.*.frame_thread.b[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, t.*.by) * f.*.b4_stride) + @as(ptrdiff_t, t.*.bx))))];
            _ = &b;
            is_split = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, b.*.bl) != @as(c_int, @bitCast(bl))))));
        } else {
            is_split = antfly_av1_dav1d_msac_decode_bool_c(&ts.*.msac, gather_left_partition_prob(pc, bl));
            if ((f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I422)) and !(is_split != 0)) return 1;
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        }
        while (true) {
            if (!(bl < @as(enum_BlockLevel, BL_8X8))) {
                unreachable;
            }
            if (!false) break;
        }
        if (is_split != 0) {
            bp = PARTITION_SPLIT;
            if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 0)]))))))) != 0) return 1;
            t.*.by += hsz;
            if (decode_sb(t, bl +% @as(enum_BlockLevel, 1), @as([*c]const EdgeNode, @ptrFromInt(@as(usize, @intCast(@intFromPtr(node))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, @as([*c]const EdgeBranch, @ptrCast(@alignCast(node))).*.split_offset[@as(c_int, 2)]))))))) != 0) return 1;
            t.*.by -= hsz;
        } else {
            bp = PARTITION_V;
            if (decode_b(t, bl, antfly_av1_dav1d_block_sizes[bl][PARTITION_V][@as(c_int, 0)], PARTITION_V, node.*.v[@as(c_int, 0)]) != 0) return -@as(c_int, 1);
        }
    }
    if ((t.*.frame_thread.pass != @as(c_int, 2)) and ((bp != @as(enum_BlockPartition, PARTITION_SPLIT)) or (bl == @as(enum_BlockLevel, BL_8X8)))) {
        blk: {
            switch (ulog2(@bitCast(@as(c_int, hsz)))) {
                @as(c_int, 0) => {
                    @as([*c]union_alias8, @ptrCast(@alignCast(&t.*.a.*.partition[@bitCast(@as(isize, @intCast(bx8)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 0)][bl][bp]) * @as(c_int, 1))));
                    @as([*c]union_alias8, @ptrCast(@alignCast(&t.*.l.partition[@bitCast(@as(isize, @intCast(by8)))]))).*.u8 = @bitCast(@as(i8, @truncate(@as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 1)][bl][bp]) * @as(c_int, 1))));
                    break :blk;
                },
                @as(c_int, 1) => {
                    @as([*c]union_alias16, @ptrCast(@alignCast(&t.*.a.*.partition[@bitCast(@as(isize, @intCast(bx8)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 0)][bl][bp]) * @as(c_int, 257))));
                    @as([*c]union_alias16, @ptrCast(@alignCast(&t.*.l.partition[@bitCast(@as(isize, @intCast(by8)))]))).*.u16 = @bitCast(@as(c_short, @truncate(@as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 1)][bl][bp]) * @as(c_int, 257))));
                    break :blk;
                },
                @as(c_int, 2) => {
                    @as([*c]union_alias32, @ptrCast(@alignCast(&t.*.a.*.partition[@bitCast(@as(isize, @intCast(bx8)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 0)][bl][bp])))) *% @as(c_uint, 16843009);
                    @as([*c]union_alias32, @ptrCast(@alignCast(&t.*.l.partition[@bitCast(@as(isize, @intCast(by8)))]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 1)][bl][bp])))) *% @as(c_uint, 16843009);
                    break :blk;
                },
                @as(c_int, 3) => {
                    @as([*c]union_alias64, @ptrCast(@alignCast(&t.*.a.*.partition[@bitCast(@as(isize, @intCast(bx8)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 0)][bl][bp])))) *% @as(c_ulonglong, 72340172838076673);
                    @as([*c]union_alias64, @ptrCast(@alignCast(&t.*.l.partition[@bitCast(@as(isize, @intCast(by8)))]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, @as(c_int, antfly_av1_dav1d_al_part_ctx[@as(c_int, 1)][bl][bp])))) *% @as(c_ulonglong, 72340172838076673);
                    break :blk;
                },
                @as(c_int, 4) => {
                    while (true) {
                        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.a.*.partition[@bitCast(@as(isize, @intCast(bx8)))])), antfly_av1_dav1d_al_part_ctx[@as(c_int, 0)][bl][bp], 16);
                        if (!false) break;
                    }
                    while (true) {
                        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&t.*.l.partition[@bitCast(@as(isize, @intCast(by8)))])), antfly_av1_dav1d_al_part_ctx[@as(c_int, 1)][bl][bp], 16);
                        if (!false) break;
                    }
                    break :blk;
                },
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                },
            }
        }
    }
    return 0;
}
pub fn reset_context(ctx: [*c]BlockContext, keyframe: c_int, pass: c_int) callconv(.c) void {
    _ = &ctx;
    _ = &keyframe;
    _ = &pass;
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.intra))))), keyframe, @sizeOf(@TypeOf(ctx.*.intra)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.uvmode))))), DC_PRED, @sizeOf(@TypeOf(ctx.*.uvmode)));
    if (keyframe != 0) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.mode))))), DC_PRED, @sizeOf(@TypeOf(ctx.*.mode)));
    }
    if (pass == @as(c_int, 2)) return;
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.partition))))), 0, @sizeOf(@TypeOf(ctx.*.partition)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.skip))))), 0, @sizeOf(@TypeOf(ctx.*.skip)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.skip_mode))))), 0, @sizeOf(@TypeOf(ctx.*.skip_mode)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.tx_lpf_y))))), 2, @sizeOf(@TypeOf(ctx.*.tx_lpf_y)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.tx_lpf_uv))))), 1, @sizeOf(@TypeOf(ctx.*.tx_lpf_uv)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&ctx.*.tx_intra))))), -@as(c_int, 1), @sizeOf(@TypeOf(ctx.*.tx_intra)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&ctx.*.tx))))), TX_64X64, @sizeOf(@TypeOf(ctx.*.tx)));
    if (!(keyframe != 0)) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][32]i8, @ptrCast(@alignCast(&ctx.*.ref))))), -@as(c_int, 1), @sizeOf(@TypeOf(ctx.*.ref)));
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.comp_type))))), 0, @sizeOf(@TypeOf(ctx.*.comp_type)));
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.mode))))), NEARESTMV, @sizeOf(@TypeOf(ctx.*.mode)));
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.lcoef))))), 64, @sizeOf(@TypeOf(ctx.*.lcoef)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][32]u8, @ptrCast(@alignCast(&ctx.*.ccoef))))), 64, @sizeOf(@TypeOf(ctx.*.ccoef)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][32]u8, @ptrCast(@alignCast(&ctx.*.filter))))), DAV1D_N_SWITCHABLE_FILTERS, @sizeOf(@TypeOf(ctx.*.filter)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.seg_pred))))), 0, @sizeOf(@TypeOf(ctx.*.seg_pred)));
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&ctx.*.pal_sz))))), 0, @sizeOf(@TypeOf(ctx.*.pal_sz)));
}
pub var ss_size_mul: [4][2]u8 = [4][2]u8{
    [2]u8{
        4,
        4,
    },
    [2]u8{
        6,
        5,
    },
    [2]u8{
        8,
        6,
    },
    [2]u8{
        12,
        8,
    },
};
pub fn setup_tile(ts: [*c]Dav1dTileState, f: [*c]const Dav1dFrameContext, data: [*c]const u8, sz: usize, tile_row: c_int, tile_col: c_int, tile_start_off: c_uint) callconv(.c) void {
    _ = &ts;
    _ = &f;
    _ = &data;
    _ = &sz;
    _ = &tile_row;
    _ = &tile_col;
    _ = &tile_start_off;
    const col_sb_start: c_int = f.*.frame_hdr.*.tiling.col_start_sb[@bitCast(@as(isize, @intCast(tile_col)))];
    _ = &col_sb_start;
    const col_sb128_start: c_int = col_sb_start >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0))));
    _ = &col_sb128_start;
    const col_sb_end: c_int = f.*.frame_hdr.*.tiling.col_start_sb[@bitCast(@as(isize, @intCast(tile_col + @as(c_int, 1))))];
    _ = &col_sb_end;
    const row_sb_start: c_int = f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(tile_row)))];
    _ = &row_sb_start;
    const row_sb_end: c_int = f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(tile_row + @as(c_int, 1))))];
    _ = &row_sb_end;
    const sb_shift: c_int = f.*.sb_shift;
    _ = &sb_shift;
    const size_mul: [*c]const u8 = @ptrCast(@alignCast(&ss_size_mul[f.*.cur.p.layout]));
    _ = &size_mul;
    {
        var p: c_int = 0;
        _ = &p;
        while (p < @as(c_int, 2)) : (p += 1) {
            ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].pal_idx = @ptrCast(@alignCast(if (f.*.frame_thread.pal_idx != null) @as(?*anyopaque, @ptrCast(@alignCast(&f.*.frame_thread.pal_idx[(@as(usize, tile_start_off) *% @as(usize, @bitCast(@as(c_long, @as(c_int, size_mul[@as(c_int, 1)]))))) / @as(usize, 8)]))) else @as(?*anyopaque, null)));
            ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].cbi = @ptrCast(@alignCast(if (f.*.frame_thread.cbi != null) @as(?*anyopaque, @ptrCast(@alignCast(&f.*.frame_thread.cbi[(@as(usize, tile_start_off) *% @as(usize, @bitCast(@as(c_long, @as(c_int, size_mul[@as(c_int, 0)]))))) / @as(usize, 64)]))) else @as(?*anyopaque, null)));
            ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].cf = if (f.*.frame_thread.cf != null) @as(?*anyopaque, @ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(f.*.frame_thread.cf))) + ((@as(usize, tile_start_off) *% @as(usize, @bitCast(@as(c_long, @as(c_int, size_mul[@as(c_int, 0)]))))) >> @intCast(@as(usize, @bitCast(@as(c_long, @as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.hbd) != 0))))))))))) else @as(?*anyopaque, null);
        }
    }
    antfly_av1_dav1d_cdf_thread_copy(&ts.*.cdf, &f.*.in_cdf);
    ts.*.last_qidx = f.*.frame_hdr.*.quant.yac;
    ts.*.last_delta_lf.u32 = 0;
    antfly_av1_dav1d_msac_init(&ts.*.msac, data, sz, f.*.frame_hdr.*.disable_cdf_update);
    ts.*.tiling.row = tile_row;
    ts.*.tiling.col = tile_col;
    ts.*.tiling.col_start = col_sb_start << @intCast(sb_shift);
    ts.*.tiling.col_end = imin(col_sb_end << @intCast(sb_shift), f.*.bw);
    ts.*.tiling.row_start = row_sb_start << @intCast(sb_shift);
    ts.*.tiling.row_end = imin(row_sb_end << @intCast(sb_shift), f.*.bh);
    var sb_idx: c_int = undefined;
    _ = &sb_idx;
    var unit_idx: c_int = undefined;
    _ = &unit_idx;
    if (f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]) {
        sb_idx = (ts.*.tiling.row_start >> @intCast(@as(c_int, 5))) * f.*.sr_sb128w;
        unit_idx = (ts.*.tiling.row_start & @as(c_int, 16)) >> @intCast(@as(c_int, 3));
    } else {
        sb_idx = ((ts.*.tiling.row_start >> @intCast(@as(c_int, 5))) * f.*.sb128w) + col_sb128_start;
        unit_idx = ((ts.*.tiling.row_start & @as(c_int, 16)) >> @intCast(@as(c_int, 3))) + ((ts.*.tiling.col_start & @as(c_int, 16)) >> @intCast(@as(c_int, 4)));
    }
    {
        var p: c_int = 0;
        _ = &p;
        while (p < @as(c_int, 3)) : (p += 1) {
            if (!((@as(c_uint, @bitCast(@as(c_int, f.*.lf.restore_planes >> @intCast(p)))) & @as(c_uint, 1)) != 0)) continue;
            if (f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]) {
                const ss_hor: c_int = @as(c_int, @intFromBool((p != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
                _ = &ss_hor;
                const d: c_int = f.*.frame_hdr.*.super_res.width_scale_denominator;
                _ = &d;
                const unit_size_log2: c_int = f.*.frame_hdr.*.restoration.unit_size[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(p != 0))))))];
                _ = &unit_size_log2;
                const rnd: c_int = (@as(c_int, 8) << @intCast(unit_size_log2)) - @as(c_int, 1);
                _ = &rnd;
                const shift: c_int = unit_size_log2 + @as(c_int, 3);
                _ = &shift;
                const x: c_int = ((((@as(c_int, 4) * ts.*.tiling.col_start) * d) >> @intCast(ss_hor)) + rnd) >> @intCast(shift);
                _ = &x;
                const px_x: c_int = x << @intCast(unit_size_log2 + ss_hor);
                _ = &px_x;
                const u_idx: c_int = unit_idx + ((px_x & @as(c_int, 64)) >> @intCast(@as(c_int, 6)));
                _ = &u_idx;
                const sb128x: c_int = px_x >> @intCast(@as(c_int, 7));
                _ = &sb128x;
                if (sb128x >= f.*.sr_sb128w) continue;
                ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))] = &f.*.lf.lr_mask[@bitCast(@as(isize, @intCast(sb_idx + sb128x)))].lr[@bitCast(@as(isize, @intCast(p)))][@bitCast(@as(isize, @intCast(u_idx)))];
            } else {
                ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))] = &f.*.lf.lr_mask[@bitCast(@as(isize, @intCast(sb_idx)))].lr[@bitCast(@as(isize, @intCast(p)))][@bitCast(@as(isize, @intCast(unit_idx)))];
            }
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v[@as(c_int, 0)] = 3;
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v[@as(c_int, 1)] = @truncate(-@as(c_int, 7));
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v[@as(c_int, 2)] = 15;
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h[@as(c_int, 0)] = 3;
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h[@as(c_int, 1)] = @truncate(-@as(c_int, 7));
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h[@as(c_int, 2)] = 15;
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.sgr_weights[@as(c_int, 0)] = @truncate(-@as(c_int, 32));
            ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.sgr_weights[@as(c_int, 1)] = 31;
        }
    }
    if (f.*.c.*.n_tc > @as(c_uint, 1)) {
        {
            var p: c_int = 0;
            _ = &p;
            while (p < @as(c_int, 2)) : (p += 1) {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&ts.*.progress[@bitCast(@as(isize, @intCast(p)))])), row_sb_start);
            }
        }
    }
}
pub fn read_restoration_info(t: [*c]Dav1dTaskContext, lr: [*c]Av1RestorationUnit, p: c_int, frame_type: enum_Dav1dRestorationType) callconv(.c) void {
    _ = &t;
    _ = &lr;
    _ = &p;
    _ = &frame_type;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    if (frame_type == @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_SWITCHABLE)) {
        const filter: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.restore_switchable)), 2))));
        _ = &filter;
        lr.*.type = @bitCast(@as(i8, @truncate(filter + @as(c_int, @intFromBool(!!(filter != 0))))));
    } else {
        const @"type": c_uint = antfly_av1_dav1d_msac_decode_bool_adapt_c(&ts.*.msac, if (frame_type == @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_WIENER)) @as([*c]u16, @ptrCast(@alignCast(&ts.*.cdf.m.restore_wiener))) else @as([*c]u16, @ptrCast(@alignCast(&ts.*.cdf.m.restore_sgrproj))));
        _ = &@"type";
        lr.*.type = @truncate(if (@"type" != 0) frame_type else @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_NONE));
    }
    if (@as(c_int, lr.*.type) == DAV1D_RESTORATION_WIENER) {
        lr.*.filter_v[@as(c_int, 0)] = @truncate(if (p != 0) @as(c_int, 0) else antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v[@as(c_int, 0)]) + @as(c_int, 5), 16, 1) - @as(c_int, 5));
        lr.*.filter_v[@as(c_int, 1)] = @truncate(antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v[@as(c_int, 1)]) + @as(c_int, 23), 32, 2) - @as(c_int, 23));
        lr.*.filter_v[@as(c_int, 2)] = @truncate(antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v[@as(c_int, 2)]) + @as(c_int, 17), 64, 3) - @as(c_int, 17));
        lr.*.filter_h[@as(c_int, 0)] = @truncate(if (p != 0) @as(c_int, 0) else antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h[@as(c_int, 0)]) + @as(c_int, 5), 16, 1) - @as(c_int, 5));
        lr.*.filter_h[@as(c_int, 1)] = @truncate(antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h[@as(c_int, 1)]) + @as(c_int, 23), 32, 2) - @as(c_int, 23));
        lr.*.filter_h[@as(c_int, 2)] = @truncate(antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h[@as(c_int, 2)]) + @as(c_int, 17), 64, 3) - @as(c_int, 17));
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&lr.*.sgr_weights))))), @ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.sgr_weights))))), @sizeOf(@TypeOf(lr.*.sgr_weights)));
        ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))] = lr;
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
    } else if (@as(c_int, lr.*.type) == DAV1D_RESTORATION_SGRPROJ) {
        const idx: c_uint = dav1d_msac_decode_bools(&ts.*.msac, 4);
        _ = &idx;
        const sgr_params: [*c]const u16 = @ptrCast(@alignCast(&antfly_av1_dav1d_sgr_params[idx]));
        _ = &sgr_params;
        {
            const ref = &lr.*.type;
            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% idx);
        }
        lr.*.sgr_weights[@as(c_int, 0)] = @truncate(if (sgr_params[@as(c_int, 0)] != 0) antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.sgr_weights[@as(c_int, 0)]) + @as(c_int, 96), 128, 4) - @as(c_int, 96) else @as(c_int, 0));
        lr.*.sgr_weights[@as(c_int, 1)] = @truncate(if (sgr_params[@as(c_int, 1)] != 0) antfly_av1_dav1d_msac_decode_subexp(&ts.*.msac, @as(c_int, ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.sgr_weights[@as(c_int, 1)]) + @as(c_int, 32), 128, 4) - @as(c_int, 32) else @as(c_int, 95));
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&lr.*.filter_v))))), @ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_v))))), @sizeOf(@TypeOf(lr.*.filter_v)));
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&lr.*.filter_h))))), @ptrCast(@alignCast(@as([*c]i8, @ptrCast(@alignCast(&ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))].*.filter_h))))), @sizeOf(@TypeOf(lr.*.filter_h)));
        ts.*.lr_ref[@bitCast(@as(isize, @intCast(p)))] = lr;
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
    }
}
pub fn check_trailing_bits_after_symbol_coder(msac: [*c]const MsacContext) callconv(.c) c_int {
    _ = &msac;
    const n_bits: c_int = -(msac.*.cnt + @as(c_int, 14));
    _ = &n_bits;
    while (true) {
        if (!(n_bits <= @as(c_int, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const n_bytes: c_int = (n_bits + @as(c_int, 7)) >> @intCast(@as(c_int, 3));
    _ = &n_bytes;
    var p: [*c]const u8 = &msac.*.buf_pos[@bitCast(@as(isize, @intCast(n_bytes)))];
    _ = &p;
    const pattern: c_int = @as(c_int, 128) >> @intCast((n_bits - @as(c_int, 1)) & @as(c_int, 7));
    _ = &pattern;
    if ((@as(c_int, p[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) & ((@as(c_int, 2) * pattern) - @as(c_int, 1))) != pattern) return 1;
    while (p < msac.*.buf_end) : (p += 1) if (@as(c_int, p.*) != 0) return 1;
    return 0;
}
pub extern fn antfly_codec_poll() c_int;
pub export fn antfly_av1_dav1d_decode_tile_sbrow(t: [*c]Dav1dTaskContext) c_int {
    if (antfly_codec_poll() != 0) return 1;
    _ = &t;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const root_bl: enum_BlockLevel = @bitCast(if (f.*.seq_hdr.*.sb128 != 0) BL_128X128 else BL_64X64);
    _ = &root_bl;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const c: [*c]const Dav1dContext = f.*.c;
    _ = &c;
    const sb_step: c_int = f.*.sb_step;
    _ = &sb_step;
    const tile_row: c_int = ts.*.tiling.row;
    _ = &tile_row;
    const tile_col: c_int = ts.*.tiling.col;
    _ = &tile_col;
    const col_sb_start: c_int = f.*.frame_hdr.*.tiling.col_start_sb[@bitCast(@as(isize, @intCast(tile_col)))];
    _ = &col_sb_start;
    const col_sb128_start: c_int = col_sb_start >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0))));
    _ = &col_sb128_start;
    if (((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) or (@as(enum_Dav1dFrameType, @bitCast(@as(c_int, f.*.frame_hdr.*.allow_intrabc))) != 0)) {
        antfly_av1_dav1d_refmvs_tile_sbrow_init(&t.*.rt, &f.*.rf, ts.*.tiling.col_start, ts.*.tiling.col_end, ts.*.tiling.row_start, ts.*.tiling.row_end, t.*.by >> @intCast(f.*.sb_shift), ts.*.tiling.row, t.*.frame_thread.pass);
    }
    if (((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) and (@as(enum_Dav1dFrameType, @intCast(@as(c_int, @intFromBool(c.*.n_fc > @as(c_uint, 1))))) != 0)) {
        const sby: c_int = (t.*.by - ts.*.tiling.row_start) >> @intCast(f.*.sb_shift);
        _ = &sby;
        const lowest_px: [*c][2]c_int = @ptrCast(@alignCast(&ts.*.lowest_pixel[@bitCast(@as(isize, @intCast(sby)))]));
        _ = &lowest_px;
        {
            var n: c_int = 0;
            _ = &n;
            while (n < @as(c_int, 7)) : (n += 1) {
                var m: c_int = 0;
                _ = &m;
                while (m < @as(c_int, 2)) : (m += 1) {
                    lowest_px[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(m)))] = -@as(c_int, 2147483647) - @as(c_int, 1);
                }
            }
        }
    }
    reset_context(&t.*.l, @as(c_int, @intFromBool(!((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0))), t.*.frame_thread.pass);
    if (t.*.frame_thread.pass == @as(c_int, 2)) {
        const off_2pass: c_int = if (c.*.n_tc > @as(c_uint, 1)) f.*.sb128w * @as(c_int, f.*.frame_hdr.*.tiling.rows) else @as(c_int, 0);
        _ = &off_2pass;
        {
            t.*.bx = ts.*.tiling.col_start;
            t.*.a = ((f.*.a + @as(usize, @bitCast(@as(isize, @intCast(off_2pass))))) + @as(usize, @bitCast(@as(isize, @intCast(col_sb128_start))))) + @as(usize, @bitCast(@as(isize, @intCast(tile_row * f.*.sb128w))));
            while (t.*.bx < ts.*.tiling.col_end) : (t.*.bx += sb_step) {
                if (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(c.*.flush))) != 0) return 1;
                if (decode_sb(t, root_bl, antfly_av1_dav1d_intra_edge_tree[root_bl]) != 0) {
                    return 1;
                }
                if (((t.*.bx & @as(c_int, 16)) != 0) or (@as(c_int, f.*.seq_hdr.*.sb128) != 0)) {
                    t.*.a += 1;
                }
            }
        }
        f.*.bd_fn.backup_ipred_edge.?(t);
        return 0;
    }
    if ((f.*.c.*.n_tc > @as(c_uint, 1)) and (@as(c_int, f.*.frame_hdr.*.use_ref_frame_mvs) != 0)) {
        f.*.c.*.refmvs_dsp.load_tmvs.?(&f.*.rf, ts.*.tiling.row, ts.*.tiling.col_start >> @intCast(@as(c_int, 1)), ts.*.tiling.col_end >> @intCast(@as(c_int, 1)), t.*.by >> @intCast(@as(c_int, 1)), (t.*.by + sb_step) >> @intCast(@as(c_int, 1)));
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&t.*.pal_sz_uv[@as(c_int, 1)]))))), 0, @sizeOf(@TypeOf(@as([*c][32]u8, @ptrCast(@alignCast(&t.*.pal_sz_uv))).*)));
    const sb128y: c_int = t.*.by >> @intCast(@as(c_int, 5));
    _ = &sb128y;
    {
        t.*.bx = ts.*.tiling.col_start;
        t.*.a = (f.*.a + @as(usize, @bitCast(@as(isize, @intCast(col_sb128_start))))) + @as(usize, @bitCast(@as(isize, @intCast(tile_row * f.*.sb128w))));
        t.*.lf_mask = (f.*.lf.mask + @as(usize, @bitCast(@as(isize, @intCast(sb128y * f.*.sb128w))))) + @as(usize, @bitCast(@as(isize, @intCast(col_sb128_start))));
        while (t.*.bx < ts.*.tiling.col_end) : (t.*.bx += sb_step) {
            if (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(c.*.flush))) != 0) return 1;
            if (root_bl == @as(enum_BlockLevel, BL_128X128)) {
                t.*.cur_sb_cdef_idx_ptr = @ptrCast(@alignCast(&t.*.lf_mask.*.cdef_idx));
                t.*.cur_sb_cdef_idx_ptr[@as(c_int, 0)] = @truncate(-@as(c_int, 1));
                t.*.cur_sb_cdef_idx_ptr[@as(c_int, 1)] = @truncate(-@as(c_int, 1));
                t.*.cur_sb_cdef_idx_ptr[@as(c_int, 2)] = @truncate(-@as(c_int, 1));
                t.*.cur_sb_cdef_idx_ptr[@as(c_int, 3)] = @truncate(-@as(c_int, 1));
            } else {
                t.*.cur_sb_cdef_idx_ptr = &t.*.lf_mask.*.cdef_idx[@bitCast(@as(isize, @intCast(((t.*.bx & @as(c_int, 16)) >> @intCast(@as(c_int, 4))) + ((t.*.by & @as(c_int, 16)) >> @intCast(@as(c_int, 3))))))];
                t.*.cur_sb_cdef_idx_ptr[@as(c_int, 0)] = @truncate(-@as(c_int, 1));
            }
            {
                var p: c_int = 0;
                _ = &p;
                while (p < @as(c_int, 3)) : (p += 1) {
                    if (!((@as(c_uint, @bitCast(@as(c_int, f.*.lf.restore_planes >> @intCast(p)))) & @as(c_uint, 1)) != 0)) continue;
                    const ss_ver: c_int = @as(c_int, @intFromBool((p != 0) and (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420))));
                    _ = &ss_ver;
                    const ss_hor: c_int = @as(c_int, @intFromBool((p != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
                    _ = &ss_hor;
                    const unit_size_log2: c_int = f.*.frame_hdr.*.restoration.unit_size[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(p != 0))))))];
                    _ = &unit_size_log2;
                    const y: c_int = (t.*.by * @as(c_int, 4)) >> @intCast(ss_ver);
                    _ = &y;
                    const h: c_int = (f.*.cur.p.h + ss_ver) >> @intCast(ss_ver);
                    _ = &h;
                    const unit_size: c_int = @as(c_int, 1) << @intCast(unit_size_log2);
                    _ = &unit_size;
                    const mask: c_uint = @bitCast(@as(c_int, unit_size - @as(c_int, 1)));
                    _ = &mask;
                    if ((@as(c_uint, @bitCast(@as(c_int, y))) & mask) != 0) continue;
                    const half_unit: c_int = unit_size >> @intCast(@as(c_int, 1));
                    _ = &half_unit;
                    if ((y != 0) and ((y + half_unit) > h)) continue;
                    const frame_type: enum_Dav1dRestorationType = f.*.frame_hdr.*.restoration.type[@bitCast(@as(isize, @intCast(p)))];
                    _ = &frame_type;
                    if (f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]) {
                        const w: c_int = (f.*.sr_cur.p.p.w + ss_hor) >> @intCast(ss_hor);
                        _ = &w;
                        const n_units: c_int = imax(1, (w + half_unit) >> @intCast(unit_size_log2));
                        _ = &n_units;
                        const d: c_int = f.*.frame_hdr.*.super_res.width_scale_denominator;
                        _ = &d;
                        const rnd: c_int = (unit_size * @as(c_int, 8)) - @as(c_int, 1);
                        _ = &rnd;
                        const shift: c_int = unit_size_log2 + @as(c_int, 3);
                        _ = &shift;
                        const x0: c_int = ((((@as(c_int, 4) * t.*.bx) * d) >> @intCast(ss_hor)) + rnd) >> @intCast(shift);
                        _ = &x0;
                        const x1: c_int = ((((@as(c_int, 4) * (t.*.bx + sb_step)) * d) >> @intCast(ss_hor)) + rnd) >> @intCast(shift);
                        _ = &x1;
                        {
                            var x: c_int = x0;
                            _ = &x;
                            while (x < imin(x1, n_units)) : (x += 1) {
                                const px_x: c_int = x << @intCast(unit_size_log2 + ss_hor);
                                _ = &px_x;
                                const sb_idx: c_int = ((t.*.by >> @intCast(@as(c_int, 5))) * f.*.sr_sb128w) + (px_x >> @intCast(@as(c_int, 7)));
                                _ = &sb_idx;
                                const unit_idx: c_int = ((t.*.by & @as(c_int, 16)) >> @intCast(@as(c_int, 3))) + ((px_x & @as(c_int, 64)) >> @intCast(@as(c_int, 6)));
                                _ = &unit_idx;
                                const lr: [*c]Av1RestorationUnit = &f.*.lf.lr_mask[@bitCast(@as(isize, @intCast(sb_idx)))].lr[@bitCast(@as(isize, @intCast(p)))][@bitCast(@as(isize, @intCast(unit_idx)))];
                                _ = &lr;
                                read_restoration_info(t, lr, p, frame_type);
                            }
                        }
                    } else {
                        const x: c_int = (@as(c_int, 4) * t.*.bx) >> @intCast(ss_hor);
                        _ = &x;
                        if ((@as(c_uint, @bitCast(@as(c_int, x))) & mask) != 0) continue;
                        const w: c_int = (f.*.cur.p.w + ss_hor) >> @intCast(ss_hor);
                        _ = &w;
                        if ((x != 0) and ((x + half_unit) > w)) continue;
                        const sb_idx: c_int = ((t.*.by >> @intCast(@as(c_int, 5))) * f.*.sr_sb128w) + (t.*.bx >> @intCast(@as(c_int, 5)));
                        _ = &sb_idx;
                        const unit_idx: c_int = ((t.*.by & @as(c_int, 16)) >> @intCast(@as(c_int, 3))) + ((t.*.bx & @as(c_int, 16)) >> @intCast(@as(c_int, 4)));
                        _ = &unit_idx;
                        const lr: [*c]Av1RestorationUnit = &f.*.lf.lr_mask[@bitCast(@as(isize, @intCast(sb_idx)))].lr[@bitCast(@as(isize, @intCast(p)))][@bitCast(@as(isize, @intCast(unit_idx)))];
                        _ = &lr;
                        read_restoration_info(t, lr, p, frame_type);
                    }
                }
            }
            if (decode_sb(t, root_bl, antfly_av1_dav1d_intra_edge_tree[root_bl]) != 0) {
                return 1;
            }
            if (((t.*.bx & @as(c_int, 16)) != 0) or (@as(c_int, f.*.seq_hdr.*.sb128) != 0)) {
                t.*.a += 1;
                t.*.lf_mask += 1;
            }
        }
    }
    if (((@as(c_int, f.*.seq_hdr.*.ref_frame_mvs) != 0) and (f.*.c.*.n_tc > @as(c_uint, 1))) and (@as(c_int, @bitCast(f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1))) != 0)) {
        dav1d_refmvs_save_tmvs(&f.*.c.*.refmvs_dsp, &t.*.rt, ts.*.tiling.col_start >> @intCast(@as(c_int, 1)), ts.*.tiling.col_end >> @intCast(@as(c_int, 1)), t.*.by >> @intCast(@as(c_int, 1)), (t.*.by + sb_step) >> @intCast(@as(c_int, 1)));
    }
    if (t.*.frame_thread.pass != @as(c_int, 1)) {
        f.*.bd_fn.backup_ipred_edge.?(t);
    }
    var align_h: c_int = (f.*.bh + @as(c_int, 31)) & ~@as(c_int, 31);
    _ = &align_h;
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&f.*.lf.tx_lpf_right_edge[@as(c_int, 0)][@bitCast(@as(isize, @intCast((align_h * tile_col) + t.*.by)))])), @ptrCast(@alignCast(&t.*.l.tx_lpf_y[@bitCast(@as(isize, @intCast(t.*.by & @as(c_int, 16))))])), @bitCast(@as(c_long, sb_step)));
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    align_h >>= @intCast(ss_ver);
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&f.*.lf.tx_lpf_right_edge[@as(c_int, 1)][@bitCast(@as(isize, @intCast((align_h * tile_col) + (t.*.by >> @intCast(ss_ver)))))])), @ptrCast(@alignCast(&t.*.l.tx_lpf_uv[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 16)) >> @intCast(ss_ver))))])), @bitCast(@as(c_long, sb_step >> @intCast(ss_ver))));
    if (ts.*.msac.cnt <= -@as(c_int, 15)) {
        return 1;
    }
    return @as(c_int, @intFromBool(((c.*.strict_std_compliance != 0) and (((t.*.by >> @intCast(f.*.sb_shift)) + @as(c_int, 1)) >= @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(tile_row + @as(c_int, 1))))]))) and (check_trailing_bits_after_symbol_coder(&ts.*.msac) != 0)));
}
pub export fn antfly_av1_dav1d_decode_frame_init(f: [*c]Dav1dFrameContext) c_int {
    _ = &f;
    var afz_has_chroma_55: c_int = undefined;
    _ = &afz_has_chroma_55;
    var afz_d1_c1_54: c_int = undefined;
    _ = &afz_d1_c1_54;
    var afz_d0_c0_53: c_int = undefined;
    _ = &afz_d0_c0_53;
    var afz_c1_52: c_int = undefined;
    _ = &afz_c1_52;
    var afz_c0_51: c_int = undefined;
    _ = &afz_c0_51;
    var afz_k_50: c_int = undefined;
    _ = &afz_k_50;
    const static_local_afz_quant_dist_lookup_table_49 = struct {
        var afz_quant_dist_lookup_table_49: [4][2]u8 = [4][2]u8{
            [2]u8{
                9,
                7,
            },
            [2]u8{
                11,
                5,
            },
            [2]u8{
                12,
                4,
            },
            [2]u8{
                13,
                3,
            },
        };
    };
    _ = &static_local_afz_quant_dist_lookup_table_49;
    const static_local_afz_quant_dist_weight_48 = struct {
        var afz_quant_dist_weight_48: [3][2]u8 = [3][2]u8{
            [2]u8{
                2,
                3,
            },
            [2]u8{
                2,
                5,
            },
            [2]u8{
                2,
                7,
            },
        };
    };
    _ = &static_local_afz_quant_dist_weight_48;
    var afz_order_47: c_int = undefined;
    _ = &afz_order_47;
    var afz_d0_46: c_uint = undefined;
    _ = &afz_d0_46;
    var afz_d1_45: c_uint = undefined;
    _ = &afz_d1_45;
    var afz_ref1poc_44: c_uint = undefined;
    _ = &afz_ref1poc_44;
    var afz_j_43: c_int = undefined;
    _ = &afz_j_43;
    var afz_ref0poc_42: c_uint = undefined;
    _ = &afz_ref0poc_42;
    var afz_i_41: c_int = undefined;
    _ = &afz_i_41;
    var afz_i_40: c_int = undefined;
    _ = &afz_i_40;
    var afz_ret_39: c_int = undefined;
    _ = &afz_ret_39;
    var afz_re_sz_38: c_int = undefined;
    _ = &afz_re_sz_38;
    var afz_ptr_37: [*c]u8 = undefined;
    _ = &afz_ptr_37;
    var afz_ipred_edge_sz_36: c_int = undefined;
    _ = &afz_ipred_edge_sz_36;
    var afz_lr_mask_sz_35: c_int = undefined;
    _ = &afz_lr_mask_sz_35;
    var afz_ptr_34: [*c]u8 = undefined;
    _ = &afz_ptr_34;
    var afz_alloc_sz_33: usize = undefined;
    _ = &afz_alloc_sz_33;
    var afz_num_lines_32: c_int = undefined;
    _ = &afz_num_lines_32;
    var afz_sb128_31: c_int = undefined;
    _ = &afz_sb128_31;
    var afz_ptr_30: [*c]u8 = undefined;
    _ = &afz_ptr_30;
    var afz_alloc_sz_29: usize = undefined;
    _ = &afz_alloc_sz_29;
    var afz_need_cdef_lpf_copy_28: c_int = undefined;
    _ = &afz_need_cdef_lpf_copy_28;
    var afz_has_resize_27: c_int = undefined;
    _ = &afz_has_resize_27;
    var afz_y_stride_25: ptrdiff_t = undefined;
    _ = &afz_y_stride_25;
    var afz_uv_stride_26: ptrdiff_t = undefined;
    _ = &afz_uv_stride_26;
    var afz_pal_idx_sz_24: c_int = undefined;
    _ = &afz_pal_idx_sz_24;
    var afz_pal_sz_23: c_int = undefined;
    _ = &afz_pal_sz_23;
    var afz_cf_sz_22: c_int = undefined;
    _ = &afz_cf_sz_22;
    var afz_cbi_sz_21: c_int = undefined;
    _ = &afz_cbi_sz_21;
    var afz_tile_col_20: c_int = undefined;
    _ = &afz_tile_col_20;
    var afz_tile_row_sb_h_19: c_int = undefined;
    _ = &afz_tile_row_sb_h_19;
    var afz_tile_row_17: c_int = undefined;
    _ = &afz_tile_row_17;
    var afz_tile_row_base_18: c_int = undefined;
    _ = &afz_tile_row_base_18;
    var afz_lowest_pixel_ptr_16: [*c][7][2]c_int = undefined;
    _ = &afz_lowest_pixel_ptr_16;
    var afz_lowest_pixel_mem_sz_15: c_int = undefined;
    _ = &afz_lowest_pixel_mem_sz_15;
    var afz_tile_col_14: c_int = undefined;
    _ = &afz_tile_col_14;
    var afz_b_diff_13: c_uint = undefined;
    _ = &afz_b_diff_13;
    var afz_row_off_12: c_uint = undefined;
    _ = &afz_row_off_12;
    var afz_tile_row_11: c_int = undefined;
    _ = &afz_tile_row_11;
    var afz_tile_idx_10: c_int = undefined;
    _ = &afz_tile_idx_10;
    var afz_sb_step4_9: c_uint = undefined;
    _ = &afz_sb_step4_9;
    var afz_hbd_8: c_int = undefined;
    _ = &afz_hbd_8;
    var afz_size_mul_7: [*c]u8 = undefined;
    _ = &afz_size_mul_7;
    var afz_num_sb128_6: c_int = undefined;
    _ = &afz_num_sb128_6;
    var afz_a_sz_5: c_int = undefined;
    _ = &afz_a_sz_5;
    var afz_n_ts_4: c_int = undefined;
    _ = &afz_n_ts_4;
    var afz_tile_row_3: c_int = undefined;
    _ = &afz_tile_row_3;
    var afz_sby_2: c_int = undefined;
    _ = &afz_sby_2;
    var afz_retval_1: c_int = undefined;
    _ = &afz_retval_1;
    var afz_c_0: [*c]Dav1dContext = undefined;
    _ = &afz_c_0;
    var afz_pc: c_uint = 271;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_retval_1;
            },
            2 => {
                afz_retval_1 = 0;
                afz_pc = 1;
                continue;
            },
            3 => {
                f.*.lf.sr_p[@as(c_int, 2)] = @ptrCast(@alignCast(f.*.sr_cur.p.data[@bitCast(@as(isize, @intCast(if (afz_has_chroma_55 != 0) @as(c_int, 2) else @as(c_int, 0))))]));
                afz_pc = 2;
                continue;
            },
            4 => {
                f.*.lf.sr_p[@as(c_int, 1)] = @ptrCast(@alignCast(f.*.sr_cur.p.data[@bitCast(@as(isize, @intCast(if (afz_has_chroma_55 != 0) @as(c_int, 1) else @as(c_int, 0))))]));
                afz_pc = 3;
                continue;
            },
            5 => {
                f.*.lf.sr_p[@as(c_int, 0)] = @ptrCast(@alignCast(f.*.sr_cur.p.data[@as(c_int, 0)]));
                afz_pc = 4;
                continue;
            },
            6 => {
                f.*.lf.p[@as(c_int, 2)] = @ptrCast(@alignCast(f.*.cur.data[@bitCast(@as(isize, @intCast(if (afz_has_chroma_55 != 0) @as(c_int, 2) else @as(c_int, 0))))]));
                afz_pc = 5;
                continue;
            },
            7 => {
                f.*.lf.p[@as(c_int, 1)] = @ptrCast(@alignCast(f.*.cur.data[@bitCast(@as(isize, @intCast(if (afz_has_chroma_55 != 0) @as(c_int, 1) else @as(c_int, 0))))]));
                afz_pc = 6;
                continue;
            },
            8 => {
                f.*.lf.p[@as(c_int, 0)] = @ptrCast(@alignCast(f.*.cur.data[@as(c_int, 0)]));
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_has_chroma_55 = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)));
                afz_pc = 8;
                continue;
            },
            10 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_41 < @as(c_int, 7)) @as(c_int, 32) else @as(c_int, 9)));
                continue;
            },
            11 => {
                afz_i_41 += 1;
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_pc = @bitCast(@as(c_int, if (afz_j_43 < @as(c_int, 7)) @as(c_int, 30) else @as(c_int, 11)));
                continue;
            },
            13 => {
                afz_j_43 += 1;
                afz_pc = 12;
                continue;
            },
            14 => {
                f.*.jnt_weights[@bitCast(@as(isize, @intCast(afz_i_41)))][@bitCast(@as(isize, @intCast(afz_j_43)))] = static_local_afz_quant_dist_lookup_table_49.afz_quant_dist_lookup_table_49[@bitCast(@as(isize, @intCast(afz_k_50)))][@bitCast(@as(isize, @intCast(afz_order_47)))];
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_pc = @bitCast(@as(c_int, if (afz_k_50 < @as(c_int, 3)) @as(c_int, 22) else @as(c_int, 14)));
                continue;
            },
            16 => {
                afz_k_50 += 1;
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = 14;
                continue;
            },
            18 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_d0_46 > afz_d1_45) and (afz_d0_c0_53 < afz_d1_c1_54)) or ((afz_d0_46 <= afz_d1_45) and (afz_d0_c0_53 > afz_d1_c1_54))) @as(c_int, 17) else @as(c_int, 16)));
                continue;
            },
            19 => {
                afz_d1_c1_54 = @bitCast(@as(c_uint, @truncate(afz_d1_45 *% @as(c_uint, @bitCast(@as(c_int, afz_c1_52))))));
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_d0_c0_53 = @bitCast(@as(c_uint, @truncate(afz_d0_46 *% @as(c_uint, @bitCast(@as(c_int, afz_c0_51))))));
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_c1_52 = static_local_afz_quant_dist_weight_48.afz_quant_dist_weight_48[@bitCast(@as(isize, @intCast(afz_k_50)))][@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(afz_order_47 != 0))))))];
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_c0_51 = static_local_afz_quant_dist_weight_48.afz_quant_dist_weight_48[@bitCast(@as(isize, @intCast(afz_k_50)))][@bitCast(@as(isize, @intCast(afz_order_47)))];
                afz_pc = 21;
                continue;
            },
            23 => {
                afz_k_50 = 0;
                afz_pc = 15;
                continue;
            },
            24 => {
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_pc = 25;
                continue;
            },
            27 => {
                afz_order_47 = @as(c_int, @intFromBool(afz_d0_46 <= afz_d1_45));
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_d0_46 = @bitCast(@as(c_int, imin(antfly_codec_abs(get_poc_diff(f.*.seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(afz_ref1poc_44))), f.*.cur.frame_hdr.*.frame_offset)), 31)));
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_d1_45 = @bitCast(@as(c_int, imin(antfly_codec_abs(get_poc_diff(f.*.seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(afz_ref0poc_42))), f.*.cur.frame_hdr.*.frame_offset)), 31)));
                afz_pc = 28;
                continue;
            },
            30 => {
                afz_ref1poc_44 = f.*.refp[@bitCast(@as(isize, @intCast(afz_j_43)))].p.frame_hdr.*.frame_offset;
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_j_43 = afz_i_41 + @as(c_int, 1);
                afz_pc = 12;
                continue;
            },
            32 => {
                afz_ref0poc_42 = f.*.refp[@bitCast(@as(isize, @intCast(afz_i_41)))].p.frame_hdr.*.frame_offset;
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_i_41 = 0;
                afz_pc = 10;
                continue;
            },
            34 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.frame_hdr.*.switchable_comp_refs != 0) @as(c_int, 33) else @as(c_int, 9)));
                continue;
            },
            35 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_40 < N_RECT_TX_SIZES) @as(c_int, 39) else @as(c_int, 34)));
                continue;
            },
            36 => {
                afz_i_40 += 1;
                afz_pc = 35;
                continue;
            },
            37 => {
                f.*.qm[@bitCast(@as(isize, @intCast(afz_i_40)))][@as(c_int, 2)] = antfly_av1_dav1d_qm_tbl[f.*.frame_hdr.*.quant.qm_v][@as(c_int, 1)][@bitCast(@as(isize, @intCast(afz_i_40)))];
                afz_pc = 36;
                continue;
            },
            38 => {
                f.*.qm[@bitCast(@as(isize, @intCast(afz_i_40)))][@as(c_int, 1)] = antfly_av1_dav1d_qm_tbl[f.*.frame_hdr.*.quant.qm_u][@as(c_int, 1)][@bitCast(@as(isize, @intCast(afz_i_40)))];
                afz_pc = 37;
                continue;
            },
            39 => {
                f.*.qm[@bitCast(@as(isize, @intCast(afz_i_40)))][@as(c_int, 0)] = antfly_av1_dav1d_qm_tbl[f.*.frame_hdr.*.quant.qm_y][@as(c_int, 0)][@bitCast(@as(isize, @intCast(afz_i_40)))];
                afz_pc = 38;
                continue;
            },
            40 => {
                afz_i_40 = 0;
                afz_pc = 35;
                continue;
            },
            41 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][3][*c]const u8, @ptrCast(@alignCast(&f.*.qm))))), 0, @sizeOf(@TypeOf(f.*.qm)));
                afz_pc = 34;
                continue;
            },
            42 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.frame_hdr.*.quant.qm != 0) @as(c_int, 40) else @as(c_int, 41)));
                continue;
            },
            43 => {
                init_quant_tables(f.*.seq_hdr, f.*.frame_hdr, f.*.frame_hdr.*.quant.yac, @ptrCast(@alignCast(&f.*.dq)));
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_pc = 1;
                continue;
            },
            45 => {
                afz_pc = @bitCast(@as(c_int, if (afz_ret_39 < @as(c_int, 0)) @as(c_int, 44) else @as(c_int, 43)));
                continue;
            },
            46 => {
                afz_ret_39 = antfly_av1_dav1d_refmvs_init_frame(&f.*.rf, f.*.seq_hdr, f.*.frame_hdr, @ptrCast(@alignCast(&f.*.refpoc)), f.*.mvs, @ptrCast(@alignCast(&f.*.refrefpoc)), @ptrCast(@alignCast(&f.*.ref_mvs)), @bitCast(@as(c_uint, @truncate(f.*.c.*.n_tc))), @bitCast(@as(c_uint, @truncate(f.*.c.*.n_fc))));
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_pc = @bitCast(@as(c_int, if (((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) or (@as(enum_Dav1dFrameType, @bitCast(@as(c_int, f.*.frame_hdr.*.allow_intrabc))) != 0)) @as(c_int, 46) else @as(c_int, 43)));
                continue;
            },
            48 => {
                f.*.lf.re_sz = afz_re_sz_38;
                afz_pc = 47;
                continue;
            },
            49 => {
                f.*.lf.tx_lpf_right_edge[@as(c_int, 1)] = f.*.lf.tx_lpf_right_edge[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(afz_re_sz_38 * @as(c_int, 32)))));
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_pc = 1;
                continue;
            },
            51 => {
                f.*.lf.re_sz = 0;
                afz_pc = 50;
                continue;
            },
            52 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.lf.tx_lpf_right_edge[@as(c_int, 0)] != null)) @as(c_int, 51) else @as(c_int, 49)));
                continue;
            },
            53 => {
                f.*.lf.tx_lpf_right_edge[@as(c_int, 0)] = @ptrCast(@alignCast(antfly_codec_afz_malloc(@bitCast(@as(c_long, (afz_re_sz_38 * @as(c_int, 32)) * @as(c_int, 2))))));
                afz_pc = 52;
                continue;
            },
            54 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.tx_lpf_right_edge[@as(c_int, 0)])));
                afz_pc = 53;
                continue;
            },
            55 => {
                afz_pc = @bitCast(@as(c_int, if (afz_re_sz_38 != f.*.lf.re_sz) @as(c_int, 54) else @as(c_int, 47)));
                continue;
            },
            56 => {
                afz_re_sz_38 = f.*.sb128h * @as(c_int, f.*.frame_hdr.*.tiling.cols);
                afz_pc = 55;
                continue;
            },
            57 => {
                f.*.ipred_edge_sz = afz_ipred_edge_sz_36;
                afz_pc = 56;
                continue;
            },
            58 => {
                f.*.ipred_edge[@as(c_int, 2)] = afz_ptr_37 + @as(usize, @bitCast(@as(isize, @intCast((afz_ipred_edge_sz_36 * @as(c_int, 128)) * @as(c_int, 2)))));
                afz_pc = 57;
                continue;
            },
            59 => {
                f.*.ipred_edge[@as(c_int, 1)] = afz_ptr_37 + @as(usize, @bitCast(@as(isize, @intCast((afz_ipred_edge_sz_36 * @as(c_int, 128)) * @as(c_int, 1)))));
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_pc = 1;
                continue;
            },
            61 => {
                f.*.ipred_edge_sz = 0;
                afz_pc = 60;
                continue;
            },
            62 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ptr_37 != null)) @as(c_int, 61) else @as(c_int, 59)));
                continue;
            },
            63 => {
                afz_ptr_37 = blk: {
                    const tmp = @as([*c]pixel, @ptrCast(@alignCast(dav1d_alloc_aligned_internal(@bitCast(@as(c_long, (afz_ipred_edge_sz_36 * @as(c_int, 128)) * @as(c_int, 3))), 64))));
                    f.*.ipred_edge[@as(c_int, 0)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 62;
                continue;
            },
            64 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.ipred_edge[@as(c_int, 0)])));
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_pc = @bitCast(@as(c_int, if (afz_ipred_edge_sz_36 != f.*.ipred_edge_sz) @as(c_int, 64) else @as(c_int, 56)));
                continue;
            },
            66 => {
                afz_ipred_edge_sz_36 = (f.*.sbh * f.*.sb128w) << @intCast(afz_hbd_8);
                afz_pc = 65;
                continue;
            },
            67 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(f.*.lf.mask)), 0, @sizeOf(@TypeOf(f.*.lf.mask.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_num_sb128_6))));
                afz_pc = 66;
                continue;
            },
            68 => {
                antfly_av1_dav1d_calc_lf_values(@ptrCast(@alignCast(&f.*.lf.lvl)), f.*.frame_hdr, @ptrCast(@alignCast(&@as([4]i8, [4]i8{
                    0,
                    0,
                    0,
                    0,
                }))));
                afz_pc = 67;
                continue;
            },
            69 => {
                f.*.lf.last_sharpness = f.*.frame_hdr.*.loopfilter.sharpness;
                afz_pc = 68;
                continue;
            },
            70 => {
                antfly_av1_dav1d_calc_eih(&f.*.lf.lim_lut, f.*.frame_hdr.*.loopfilter.sharpness);
                afz_pc = 69;
                continue;
            },
            71 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, f.*.frame_hdr.*.loopfilter.sharpness) != f.*.lf.last_sharpness) @as(c_int, 70) else @as(c_int, 68)));
                continue;
            },
            72 => {
                f.*.lf.restore_planes = ((@as(c_int, @as(c_int, @intFromBool((f.*.frame_hdr.*.restoration.type[@as(c_int, 0)] != @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_NONE))))) << @intCast(@as(c_int, 0))) + (@as(c_int, @as(c_int, @intFromBool((f.*.frame_hdr.*.restoration.type[@as(c_int, 1)] != @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_NONE))))) << @intCast(@as(c_int, 1)))) + (@as(c_int, @as(c_int, @intFromBool(f.*.frame_hdr.*.restoration.type[@as(c_int, 2)] != @as(enum_Dav1dRestorationType, DAV1D_RESTORATION_NONE)))) << @intCast(@as(c_int, 2)));
                afz_pc = 71;
                continue;
            },
            73 => {
                f.*.lf.lr_mask_sz = afz_lr_mask_sz_35;
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_pc = 1;
                continue;
            },
            75 => {
                f.*.lf.lr_mask_sz = 0;
                afz_pc = 74;
                continue;
            },
            76 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.lf.lr_mask != null)) @as(c_int, 75) else @as(c_int, 73)));
                continue;
            },
            77 => {
                f.*.lf.lr_mask = @ptrCast(@alignCast(antfly_codec_afz_malloc(@sizeOf(@TypeOf(f.*.lf.lr_mask.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_lr_mask_sz_35))))));
                afz_pc = 76;
                continue;
            },
            78 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.lr_mask)));
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_pc = @bitCast(@as(c_int, if (afz_lr_mask_sz_35 != f.*.lf.lr_mask_sz) @as(c_int, 78) else @as(c_int, 72)));
                continue;
            },
            80 => {
                afz_lr_mask_sz_35 = f.*.sr_sb128w * f.*.sb128h;
                afz_pc = 79;
                continue;
            },
            81 => {
                f.*.sr_sb128w = (f.*.sr_cur.p.p.w + @as(c_int, 127)) >> @intCast(@as(c_int, 7));
                afz_pc = 80;
                continue;
            },
            82 => {
                f.*.lf.mask_sz = afz_num_sb128_6;
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_pc = 1;
                continue;
            },
            84 => {
                f.*.lf.mask_sz = 0;
                afz_pc = 83;
                continue;
            },
            85 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.frame_thread.b != null)) @as(c_int, 84) else @as(c_int, 82)));
                continue;
            },
            86 => {
                f.*.frame_thread.b = @ptrCast(@alignCast(antfly_codec_afz_malloc(((@sizeOf(@TypeOf(f.*.frame_thread.b.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_num_sb128_6)))) *% @as(c_ulong, 32)) *% @as(c_ulong, 32))));
                afz_pc = 85;
                continue;
            },
            87 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.frame_thread.b)));
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_0.*.n_fc > @as(c_uint, 1)) @as(c_int, 87) else @as(c_int, 82)));
                continue;
            },
            89 => {
                afz_pc = 1;
                continue;
            },
            90 => {
                f.*.lf.mask_sz = 0;
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.lf.mask != null) or !(f.*.lf.level != null)) @as(c_int, 90) else @as(c_int, 88)));
                continue;
            },
            92 => {
                f.*.lf.level = @ptrCast(@alignCast(antfly_codec_afz_malloc((((@sizeOf(@TypeOf(f.*.lf.level.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_num_sb128_6)))) *% @as(c_ulong, 32)) *% @as(c_ulong, 32)) +% @as(c_ulong, 3))));
                afz_pc = 91;
                continue;
            },
            93 => {
                f.*.lf.mask = @ptrCast(@alignCast(antfly_codec_afz_malloc(@sizeOf(@TypeOf(f.*.lf.mask.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_num_sb128_6))))));
                afz_pc = 92;
                continue;
            },
            94 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.level)));
                afz_pc = 93;
                continue;
            },
            95 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.mask)));
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_pc = @bitCast(@as(c_int, if (afz_num_sb128_6 != f.*.lf.mask_sz) @as(c_int, 95) else @as(c_int, 81)));
                continue;
            },
            97 => {
                f.*.lf.lr_buf_plane_sz[@as(c_int, 1)] = (@as(c_int, @truncate(afz_uv_stride_26)) * afz_num_lines_32) * @as(c_int, 2);
                afz_pc = 96;
                continue;
            },
            98 => {
                f.*.lf.lr_buf_plane_sz[@as(c_int, 0)] = @as(c_int, @truncate(afz_y_stride_25)) * afz_num_lines_32;
                afz_pc = 97;
                continue;
            },
            99 => {
                f.*.lf.lr_lpf_line[@as(c_int, 2)] = afz_ptr_34 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (afz_num_lines_32 * @as(c_int, 2)) - @as(c_int, 1))))));
                afz_pc = 98;
                continue;
            },
            100 => {
                f.*.lf.lr_lpf_line[@as(c_int, 1)] = afz_ptr_34 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (afz_num_lines_32 * @as(c_int, 1)) - @as(c_int, 1))))));
                afz_pc = 99;
                continue;
            },
            101 => {
                f.*.lf.lr_lpf_line[@as(c_int, 2)] = afz_ptr_34 + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, afz_num_lines_32)))));
                afz_pc = 98;
                continue;
            },
            102 => {
                f.*.lf.lr_lpf_line[@as(c_int, 1)] = afz_ptr_34;
                afz_pc = 101;
                continue;
            },
            103 => {
                afz_pc = @bitCast(@as(c_int, if (afz_uv_stride_26 < @as(ptrdiff_t, 0)) @as(c_int, 100) else @as(c_int, 102)));
                continue;
            },
            104 => {
                afz_ptr_34 += @as(usize, @bitCast(@as(isize, @intCast(antfly_codec_llabs(afz_y_stride_25) * @as(c_longlong, afz_num_lines_32)))));
                afz_pc = 103;
                continue;
            },
            105 => {
                f.*.lf.lr_lpf_line[@as(c_int, 0)] = afz_ptr_34 - @as(usize, @bitCast(@as(isize, @intCast(afz_y_stride_25 * @as(ptrdiff_t, afz_num_lines_32 - @as(c_int, 1))))));
                afz_pc = 104;
                continue;
            },
            106 => {
                f.*.lf.lr_lpf_line[@as(c_int, 0)] = afz_ptr_34;
                afz_pc = 104;
                continue;
            },
            107 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_stride_25 < @as(ptrdiff_t, 0)) @as(c_int, 105) else @as(c_int, 106)));
                continue;
            },
            108 => {
                afz_ptr_34 += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 64)))));
                afz_pc = 107;
                continue;
            },
            109 => {
                afz_pc = 1;
                continue;
            },
            110 => {
                f.*.lf.lr_buf_plane_sz[@as(c_int, 0)] = blk: {
                    const tmp = @as(c_int, 0);
                    f.*.lf.lr_buf_plane_sz[@as(c_int, 1)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 109;
                continue;
            },
            111 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ptr_34 != null)) @as(c_int, 110) else @as(c_int, 108)));
                continue;
            },
            112 => {
                afz_ptr_34 = blk: {
                    const tmp = @as([*c]u8, @ptrCast(@alignCast(dav1d_alloc_aligned_internal(afz_alloc_sz_33, 64))));
                    f.*.lf.lr_line_buf = tmp;
                    break :blk tmp;
                };
                afz_pc = 111;
                continue;
            },
            113 => {
                afz_alloc_sz_33 +%= (@as(usize, @bitCast(@as(c_long, @truncate(antfly_codec_llabs(afz_uv_stride_26))))) *% @as(usize, @bitCast(@as(c_long, afz_num_lines_32)))) *% @as(usize, 2);
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_alloc_sz_33 +%= @as(usize, @bitCast(@as(c_long, @truncate(antfly_codec_llabs(afz_y_stride_25))))) *% @as(usize, @bitCast(@as(c_long, afz_num_lines_32)));
                afz_pc = 113;
                continue;
            },
            115 => {
                afz_alloc_sz_33 = 128;
                afz_pc = 114;
                continue;
            },
            116 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.lf.lr_line_buf)));
                afz_pc = 115;
                continue;
            },
            117 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_y_stride_25 * @as(ptrdiff_t, afz_num_lines_32)) != @as(ptrdiff_t, f.*.lf.lr_buf_plane_sz[@as(c_int, 0)])) or (((afz_uv_stride_26 * @as(ptrdiff_t, afz_num_lines_32)) * @as(ptrdiff_t, 2)) != @as(ptrdiff_t, f.*.lf.lr_buf_plane_sz[@as(c_int, 1)]))) @as(c_int, 116) else @as(c_int, 96)));
                continue;
            },
            118 => {
                afz_y_stride_25 = f.*.sr_cur.p.stride[@as(c_int, 0)];
                afz_uv_stride_26 = f.*.sr_cur.p.stride[@as(c_int, 1)];
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_num_lines_32 = if (afz_c_0.*.n_tc > @as(c_uint, 1)) (f.*.sbh * @as(c_int, 4)) << @intCast(afz_sb128_31) else @as(c_int, 12);
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_sb128_31 = f.*.seq_hdr.*.sb128;
                afz_pc = 119;
                continue;
            },
            121 => {
                f.*.lf.cdef_buf_sbh = f.*.sbh;
                afz_pc = 120;
                continue;
            },
            122 => {
                f.*.lf.need_cdef_lpf_copy = afz_need_cdef_lpf_copy_28;
                afz_pc = 121;
                continue;
            },
            123 => {
                f.*.lf.cdef_buf_plane_sz[@as(c_int, 1)] = (@as(c_int, @truncate(afz_uv_stride_26)) * f.*.sbh) * @as(c_int, 8);
                afz_pc = 122;
                continue;
            },
            124 => {
                f.*.lf.cdef_buf_plane_sz[@as(c_int, 0)] = (@as(c_int, @truncate(afz_y_stride_25)) * f.*.sbh) * @as(c_int, 4);
                afz_pc = 123;
                continue;
            },
            125 => {
                f.*.lf.cdef_lpf_line[@as(c_int, 2)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 8)) - @as(c_int, 1))))));
                afz_pc = 124;
                continue;
            },
            126 => {
                f.*.lf.cdef_lpf_line[@as(c_int, 1)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 4)) - @as(c_int, 1))))));
                afz_pc = 125;
                continue;
            },
            127 => {
                f.*.lf.cdef_lpf_line[@as(c_int, 2)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast((afz_uv_stride_26 * @as(ptrdiff_t, f.*.sbh)) * @as(ptrdiff_t, 4)))));
                afz_pc = 124;
                continue;
            },
            128 => {
                f.*.lf.cdef_lpf_line[@as(c_int, 1)] = afz_ptr_30;
                afz_pc = 127;
                continue;
            },
            129 => {
                afz_pc = @bitCast(@as(c_int, if (afz_uv_stride_26 < @as(ptrdiff_t, 0)) @as(c_int, 126) else @as(c_int, 128)));
                continue;
            },
            130 => {
                afz_ptr_30 += @as(usize, @bitCast(@as(isize, @intCast((antfly_codec_llabs(afz_y_stride_25) * @as(c_longlong, f.*.sbh)) * @as(c_longlong, 4)))));
                afz_pc = 129;
                continue;
            },
            131 => {
                f.*.lf.cdef_lpf_line[@as(c_int, 0)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_y_stride_25 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 4)) - @as(c_int, 1))))));
                afz_pc = 130;
                continue;
            },
            132 => {
                f.*.lf.cdef_lpf_line[@as(c_int, 0)] = afz_ptr_30;
                afz_pc = 130;
                continue;
            },
            133 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_stride_25 < @as(ptrdiff_t, 0)) @as(c_int, 131) else @as(c_int, 132)));
                continue;
            },
            134 => {
                afz_ptr_30 += @as(usize, @bitCast(@as(isize, @intCast((antfly_codec_llabs(afz_uv_stride_26) * @as(c_longlong, f.*.sbh)) * @as(c_longlong, 8)))));
                afz_pc = 133;
                continue;
            },
            135 => {
                afz_pc = @bitCast(@as(c_int, if (afz_need_cdef_lpf_copy_28 != 0) @as(c_int, 134) else @as(c_int, 124)));
                continue;
            },
            136 => {
                f.*.lf.cdef_line[@as(c_int, 1)][@as(c_int, 2)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 8)) - @as(c_int, 7))))));
                afz_pc = 135;
                continue;
            },
            137 => {
                f.*.lf.cdef_line[@as(c_int, 1)][@as(c_int, 1)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 8)) - @as(c_int, 5))))));
                afz_pc = 136;
                continue;
            },
            138 => {
                f.*.lf.cdef_line[@as(c_int, 0)][@as(c_int, 2)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 8)) - @as(c_int, 3))))));
                afz_pc = 137;
                continue;
            },
            139 => {
                f.*.lf.cdef_line[@as(c_int, 0)][@as(c_int, 1)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 8)) - @as(c_int, 1))))));
                afz_pc = 138;
                continue;
            },
            140 => {
                f.*.lf.cdef_line[@as(c_int, 1)][@as(c_int, 2)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, 6)))));
                afz_pc = 135;
                continue;
            },
            141 => {
                f.*.lf.cdef_line[@as(c_int, 1)][@as(c_int, 1)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, 4)))));
                afz_pc = 140;
                continue;
            },
            142 => {
                f.*.lf.cdef_line[@as(c_int, 0)][@as(c_int, 2)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, 2)))));
                afz_pc = 141;
                continue;
            },
            143 => {
                f.*.lf.cdef_line[@as(c_int, 0)][@as(c_int, 1)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_stride_26 * @as(ptrdiff_t, 0)))));
                afz_pc = 142;
                continue;
            },
            144 => {
                afz_pc = @bitCast(@as(c_int, if (afz_uv_stride_26 < @as(ptrdiff_t, 0)) @as(c_int, 139) else @as(c_int, 143)));
                continue;
            },
            145 => {
                afz_ptr_30 += @as(usize, @bitCast(@as(isize, @intCast((antfly_codec_llabs(afz_y_stride_25) * @as(c_longlong, f.*.sbh)) * @as(c_longlong, 4)))));
                afz_pc = 144;
                continue;
            },
            146 => {
                f.*.lf.cdef_line[@as(c_int, 1)][@as(c_int, 0)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_y_stride_25 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 4)) - @as(c_int, 3))))));
                afz_pc = 145;
                continue;
            },
            147 => {
                f.*.lf.cdef_line[@as(c_int, 0)][@as(c_int, 0)] = afz_ptr_30 - @as(usize, @bitCast(@as(isize, @intCast(afz_y_stride_25 * @as(ptrdiff_t, (f.*.sbh * @as(c_int, 4)) - @as(c_int, 1))))));
                afz_pc = 146;
                continue;
            },
            148 => {
                f.*.lf.cdef_line[@as(c_int, 1)][@as(c_int, 0)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast(afz_y_stride_25 * @as(ptrdiff_t, 2)))));
                afz_pc = 145;
                continue;
            },
            149 => {
                f.*.lf.cdef_line[@as(c_int, 0)][@as(c_int, 0)] = afz_ptr_30 + @as(usize, @bitCast(@as(isize, @intCast(afz_y_stride_25 * @as(ptrdiff_t, 0)))));
                afz_pc = 148;
                continue;
            },
            150 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_stride_25 < @as(ptrdiff_t, 0)) @as(c_int, 147) else @as(c_int, 149)));
                continue;
            },
            151 => {
                afz_ptr_30 += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                afz_pc = 150;
                continue;
            },
            152 => {
                afz_pc = 1;
                continue;
            },
            153 => {
                f.*.lf.cdef_buf_plane_sz[@as(c_int, 0)] = blk: {
                    const tmp = @as(c_int, 0);
                    f.*.lf.cdef_buf_plane_sz[@as(c_int, 1)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 152;
                continue;
            },
            154 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ptr_30 != null)) @as(c_int, 153) else @as(c_int, 151)));
                continue;
            },
            155 => {
                afz_ptr_30 = blk: {
                    const tmp = @as([*c]u8, @ptrCast(@alignCast(dav1d_alloc_aligned_internal(afz_alloc_sz_29, 32))));
                    f.*.lf.cdef_line_buf = tmp;
                    break :blk tmp;
                };
                afz_pc = 154;
                continue;
            },
            156 => {
                afz_alloc_sz_29 +%= ((@as(usize, @bitCast(@as(c_long, @truncate(antfly_codec_llabs(afz_uv_stride_26))))) *% @as(usize, 8)) *% @as(usize, @bitCast(@as(c_long, f.*.sbh)))) << @intCast(@as(usize, @bitCast(@as(c_long, afz_need_cdef_lpf_copy_28))));
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_alloc_sz_29 +%= ((@as(usize, @bitCast(@as(c_long, @truncate(antfly_codec_llabs(afz_y_stride_25))))) *% @as(usize, 4)) *% @as(usize, @bitCast(@as(c_long, f.*.sbh)))) << @intCast(@as(usize, @bitCast(@as(c_long, afz_need_cdef_lpf_copy_28))));
                afz_pc = 156;
                continue;
            },
            158 => {
                afz_alloc_sz_29 = 64;
                afz_pc = 157;
                continue;
            },
            159 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.lf.cdef_line_buf)));
                afz_pc = 158;
                continue;
            },
            160 => {
                afz_pc = @bitCast(@as(c_int, if ((((((afz_y_stride_25 * @as(ptrdiff_t, f.*.sbh)) * @as(ptrdiff_t, 4)) != @as(ptrdiff_t, f.*.lf.cdef_buf_plane_sz[@as(c_int, 0)])) or (((afz_uv_stride_26 * @as(ptrdiff_t, f.*.sbh)) * @as(ptrdiff_t, 8)) != @as(ptrdiff_t, f.*.lf.cdef_buf_plane_sz[@as(c_int, 1)]))) or (afz_need_cdef_lpf_copy_28 != f.*.lf.need_cdef_lpf_copy)) or (f.*.sbh != f.*.lf.cdef_buf_sbh)) @as(c_int, 159) else @as(c_int, 120)));
                continue;
            },
            161 => {
                afz_need_cdef_lpf_copy_28 = @as(c_int, @intFromBool((afz_c_0.*.n_tc > @as(c_uint, 1)) and (afz_has_resize_27 != 0)));
                afz_pc = 160;
                continue;
            },
            162 => {
                afz_has_resize_27 = @as(c_int, @intFromBool(f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]));
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_y_stride_25 = f.*.cur.stride[@as(c_int, 0)];
                afz_uv_stride_26 = f.*.cur.stride[@as(c_int, 1)];
                afz_pc = 162;
                continue;
            },
            164 => {
                f.*.frame_thread.pal_idx_sz = afz_pal_idx_sz_24;
                afz_pc = 163;
                continue;
            },
            165 => {
                afz_pc = 1;
                continue;
            },
            166 => {
                f.*.frame_thread.pal_idx_sz = 0;
                afz_pc = 165;
                continue;
            },
            167 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.frame_thread.pal_idx != null)) @as(c_int, 166) else @as(c_int, 164)));
                continue;
            },
            168 => {
                f.*.frame_thread.pal_idx = @ptrCast(@alignCast(dav1d_alloc_aligned_internal((((@sizeOf(@TypeOf(f.*.frame_thread.pal_idx.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_pal_idx_sz_24)))) *% @as(c_ulong, 128)) *% @as(c_ulong, 128)) / @as(c_ulong, 8), 64)));
                afz_pc = 167;
                continue;
            },
            169 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.frame_thread.pal_idx)));
                afz_pc = 168;
                continue;
            },
            170 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pal_idx_sz_24 != f.*.frame_thread.pal_idx_sz) @as(c_int, 169) else @as(c_int, 163)));
                continue;
            },
            171 => {
                afz_pal_idx_sz_24 = afz_num_sb128_6 * @as(c_int, afz_size_mul_7[@as(c_int, 1)]);
                afz_pc = 170;
                continue;
            },
            172 => {
                f.*.frame_thread.pal_sz = afz_pal_sz_23;
                afz_pc = 171;
                continue;
            },
            173 => {
                afz_pc = 1;
                continue;
            },
            174 => {
                f.*.frame_thread.pal_sz = 0;
                afz_pc = 173;
                continue;
            },
            175 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.frame_thread.pal != null)) @as(c_int, 174) else @as(c_int, 172)));
                continue;
            },
            176 => {
                f.*.frame_thread.pal = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(((@sizeOf(@TypeOf(f.*.frame_thread.pal.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_pal_sz_23)))) *% @as(c_ulong, 16)) *% @as(c_ulong, 16), 64)));
                afz_pc = 175;
                continue;
            },
            177 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.frame_thread.pal)));
                afz_pc = 176;
                continue;
            },
            178 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pal_sz_23 != f.*.frame_thread.pal_sz) @as(c_int, 177) else @as(c_int, 171)));
                continue;
            },
            179 => {
                afz_pal_sz_23 = afz_num_sb128_6 << @intCast(afz_hbd_8);
                afz_pc = 178;
                continue;
            },
            180 => {
                f.*.frame_thread.pal_sz = blk: {
                    const tmp = @as(c_int, 0);
                    f.*.frame_thread.pal_idx_sz = tmp;
                    break :blk tmp;
                };
                afz_pc = 163;
                continue;
            },
            181 => {
                dav1d_freep_aligned(@ptrCast(@alignCast(&f.*.frame_thread.pal_idx)));
                afz_pc = 180;
                continue;
            },
            182 => {
                dav1d_freep_aligned(@ptrCast(@alignCast(&f.*.frame_thread.pal)));
                afz_pc = 181;
                continue;
            },
            183 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.frame_thread.pal != null) @as(c_int, 182) else @as(c_int, 163)));
                continue;
            },
            184 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.frame_hdr.*.allow_screen_content_tools != 0) @as(c_int, 179) else @as(c_int, 183)));
                continue;
            },
            185 => {
                f.*.frame_thread.cf_sz = afz_cf_sz_22;
                afz_pc = 184;
                continue;
            },
            186 => {
                _ = antfly_codec_afz_memset(f.*.frame_thread.cf, 0, ((@as(usize, @bitCast(@as(c_long, afz_cf_sz_22))) *% @as(usize, 128)) *% @as(usize, 128)) / @as(usize, 2));
                afz_pc = 185;
                continue;
            },
            187 => {
                afz_pc = 1;
                continue;
            },
            188 => {
                f.*.frame_thread.cf_sz = 0;
                afz_pc = 187;
                continue;
            },
            189 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.frame_thread.cf != null)) @as(c_int, 188) else @as(c_int, 186)));
                continue;
            },
            190 => {
                f.*.frame_thread.cf = dav1d_alloc_aligned_internal(((@as(usize, @bitCast(@as(c_long, afz_cf_sz_22))) *% @as(usize, 128)) *% @as(usize, 128)) / @as(usize, 2), 64);
                afz_pc = 189;
                continue;
            },
            191 => {
                dav1d_free_aligned_internal(f.*.frame_thread.cf);
                afz_pc = 190;
                continue;
            },
            192 => {
                afz_pc = @bitCast(@as(c_int, if (afz_cf_sz_22 != f.*.frame_thread.cf_sz) @as(c_int, 191) else @as(c_int, 184)));
                continue;
            },
            193 => {
                afz_cf_sz_22 = (afz_num_sb128_6 * @as(c_int, afz_size_mul_7[@as(c_int, 0)])) << @intCast(afz_hbd_8);
                afz_pc = 192;
                continue;
            },
            194 => {
                f.*.frame_thread.cbi_sz = afz_cbi_sz_21;
                afz_pc = 193;
                continue;
            },
            195 => {
                afz_pc = 1;
                continue;
            },
            196 => {
                f.*.frame_thread.cbi_sz = 0;
                afz_pc = 195;
                continue;
            },
            197 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.frame_thread.cbi != null)) @as(c_int, 196) else @as(c_int, 194)));
                continue;
            },
            198 => {
                f.*.frame_thread.cbi = @ptrCast(@alignCast(dav1d_alloc_aligned_internal((((@sizeOf(@TypeOf(f.*.frame_thread.cbi.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_cbi_sz_21)))) *% @as(c_ulong, 32)) *% @as(c_ulong, 32)) / @as(c_ulong, 4), 64)));
                afz_pc = 197;
                continue;
            },
            199 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.frame_thread.cbi)));
                afz_pc = 198;
                continue;
            },
            200 => {
                afz_pc = @bitCast(@as(c_int, if (afz_cbi_sz_21 != f.*.frame_thread.cbi_sz) @as(c_int, 199) else @as(c_int, 193)));
                continue;
            },
            201 => {
                afz_cbi_sz_21 = afz_num_sb128_6 * @as(c_int, afz_size_mul_7[@as(c_int, 0)]);
                afz_pc = 200;
                continue;
            },
            202 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_row_17 < @as(c_int, f.*.frame_hdr.*.tiling.rows)) @as(c_int, 209) else @as(c_int, 201)));
                continue;
            },
            203 => {
                afz_tile_row_17 += 1;
                afz_tile_row_base_18 += f.*.frame_hdr.*.tiling.cols;
                afz_pc = 202;
                continue;
            },
            204 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_col_20 < @as(c_int, f.*.frame_hdr.*.tiling.cols)) @as(c_int, 207) else @as(c_int, 203)));
                continue;
            },
            205 => {
                afz_tile_col_20 += 1;
                afz_pc = 204;
                continue;
            },
            206 => {
                afz_lowest_pixel_ptr_16 += @as(usize, @bitCast(@as(isize, @intCast(afz_tile_row_sb_h_19))));
                afz_pc = 205;
                continue;
            },
            207 => {
                f.*.ts[@bitCast(@as(isize, @intCast(afz_tile_row_base_18 + afz_tile_col_20)))].lowest_pixel = afz_lowest_pixel_ptr_16;
                afz_pc = 206;
                continue;
            },
            208 => {
                afz_tile_col_20 = 0;
                afz_pc = 204;
                continue;
            },
            209 => {
                afz_tile_row_sb_h_19 = @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_17 + @as(c_int, 1))))]) - @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_17)))]);
                afz_pc = 208;
                continue;
            },
            210 => {
                afz_tile_row_17 = 0;
                afz_tile_row_base_18 = 0;
                afz_pc = 202;
                continue;
            },
            211 => {
                afz_lowest_pixel_ptr_16 = f.*.tile_thread.lowest_pixel_mem;
                afz_pc = 210;
                continue;
            },
            212 => {
                f.*.tile_thread.lowest_pixel_mem_sz = afz_lowest_pixel_mem_sz_15;
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_pc = 1;
                continue;
            },
            214 => {
                f.*.tile_thread.lowest_pixel_mem_sz = 0;
                afz_pc = 213;
                continue;
            },
            215 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.tile_thread.lowest_pixel_mem != null)) @as(c_int, 214) else @as(c_int, 212)));
                continue;
            },
            216 => {
                f.*.tile_thread.lowest_pixel_mem = @ptrCast(@alignCast(antfly_codec_afz_malloc(@as(c_ulong, @bitCast(@as(c_long, afz_lowest_pixel_mem_sz_15))) *% @sizeOf(@TypeOf(f.*.tile_thread.lowest_pixel_mem.*)))));
                afz_pc = 215;
                continue;
            },
            217 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.tile_thread.lowest_pixel_mem)));
                afz_pc = 216;
                continue;
            },
            218 => {
                afz_pc = @bitCast(@as(c_int, if (afz_lowest_pixel_mem_sz_15 != f.*.tile_thread.lowest_pixel_mem_sz) @as(c_int, 217) else @as(c_int, 211)));
                continue;
            },
            219 => {
                afz_lowest_pixel_mem_sz_15 = @as(c_int, f.*.frame_hdr.*.tiling.cols) * f.*.sbh;
                afz_pc = 218;
                continue;
            },
            220 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_row_11 < @as(c_int, f.*.frame_hdr.*.tiling.rows)) @as(c_int, 227) else @as(c_int, 219)));
                continue;
            },
            221 => {
                afz_tile_row_11 += 1;
                afz_pc = 220;
                continue;
            },
            222 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_col_14 < @as(c_int, f.*.frame_hdr.*.tiling.cols)) @as(c_int, 224) else @as(c_int, 221)));
                continue;
            },
            223 => {
                afz_tile_col_14 += 1;
                afz_pc = 222;
                continue;
            },
            224 => {
                f.*.frame_thread.tile_start_off[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &afz_tile_idx_10;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = afz_row_off_12 +% ((afz_b_diff_13 *% @as(c_uint, @bitCast(@as(c_int, @as(c_int, f.*.frame_hdr.*.tiling.col_start_sb[@bitCast(@as(isize, @intCast(afz_tile_col_14)))]))))) *% afz_sb_step4_9);
                afz_pc = 223;
                continue;
            },
            225 => {
                afz_tile_col_14 = 0;
                afz_pc = 222;
                continue;
            },
            226 => {
                afz_b_diff_13 = @as(c_uint, @bitCast(@as(c_int, @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_11 + @as(c_int, 1))))]) - @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_11)))])))) *% afz_sb_step4_9;
                afz_pc = 225;
                continue;
            },
            227 => {
                afz_row_off_12 = ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_11)))])))) *% afz_sb_step4_9) *% @as(c_uint, @bitCast(@as(c_int, f.*.sb128w)))) *% @as(c_uint, 128);
                afz_pc = 226;
                continue;
            },
            228 => {
                afz_tile_row_11 = 0;
                afz_pc = 220;
                continue;
            },
            229 => {
                afz_tile_idx_10 = 0;
                afz_pc = 228;
                continue;
            },
            230 => {
                afz_sb_step4_9 = @bitCast(@as(c_int, f.*.sb_step * @as(c_int, 4)));
                afz_pc = 229;
                continue;
            },
            231 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_0.*.n_fc > @as(c_uint, 1)) @as(c_int, 230) else @as(c_int, 163)));
                continue;
            },
            232 => {
                afz_hbd_8 = @as(c_int, @intFromBool(!!(@as(c_int, f.*.seq_hdr.*.hbd) != 0)));
                afz_pc = 231;
                continue;
            },
            233 => {
                afz_size_mul_7 = @ptrCast(@alignCast(@constCast(@as([*c]const u8, @ptrCast(@alignCast(&ss_size_mul[f.*.cur.p.layout]))))));
                afz_pc = 232;
                continue;
            },
            234 => {
                afz_num_sb128_6 = f.*.sb128w * f.*.sb128h;
                afz_pc = 233;
                continue;
            },
            235 => {
                f.*.a_sz = afz_a_sz_5;
                afz_pc = 234;
                continue;
            },
            236 => {
                afz_pc = 1;
                continue;
            },
            237 => {
                f.*.a_sz = 0;
                afz_pc = 236;
                continue;
            },
            238 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.a != null)) @as(c_int, 237) else @as(c_int, 235)));
                continue;
            },
            239 => {
                f.*.a = @ptrCast(@alignCast(antfly_codec_afz_malloc(@sizeOf(@TypeOf(f.*.a.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_a_sz_5))))));
                afz_pc = 238;
                continue;
            },
            240 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.a)));
                afz_pc = 239;
                continue;
            },
            241 => {
                afz_pc = @bitCast(@as(c_int, if (afz_a_sz_5 != f.*.a_sz) @as(c_int, 240) else @as(c_int, 234)));
                continue;
            },
            242 => {
                afz_a_sz_5 = (f.*.sb128w * @as(c_int, f.*.frame_hdr.*.tiling.rows)) * (@as(c_int, 1) + @as(c_int, @intFromBool((afz_c_0.*.n_fc > @as(c_uint, 1)) and (afz_c_0.*.n_tc > @as(c_uint, 1)))));
                afz_pc = 241;
                continue;
            },
            243 => {
                f.*.n_ts = afz_n_ts_4;
                afz_pc = 242;
                continue;
            },
            244 => {
                afz_pc = 1;
                continue;
            },
            245 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.ts != null)) @as(c_int, 244) else @as(c_int, 243)));
                continue;
            },
            246 => {
                f.*.ts = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(@sizeOf(@TypeOf(f.*.ts.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_n_ts_4))), 32)));
                afz_pc = 245;
                continue;
            },
            247 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.ts)));
                afz_pc = 246;
                continue;
            },
            248 => {
                afz_pc = 1;
                continue;
            },
            249 => {
                f.*.n_ts = 0;
                afz_pc = 248;
                continue;
            },
            250 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.frame_thread.tile_start_off != null)) @as(c_int, 249) else @as(c_int, 247)));
                continue;
            },
            251 => {
                f.*.frame_thread.tile_start_off = @ptrCast(@alignCast(antfly_codec_afz_malloc(@sizeOf(@TypeOf(f.*.frame_thread.tile_start_off.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_n_ts_4))))));
                afz_pc = 250;
                continue;
            },
            252 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.frame_thread.tile_start_off)));
                afz_pc = 251;
                continue;
            },
            253 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_0.*.n_fc > @as(c_uint, 1)) @as(c_int, 252) else @as(c_int, 247)));
                continue;
            },
            254 => {
                afz_pc = @bitCast(@as(c_int, if (afz_n_ts_4 != f.*.n_ts) @as(c_int, 253) else @as(c_int, 242)));
                continue;
            },
            255 => {
                afz_n_ts_4 = @as(c_int, f.*.frame_hdr.*.tiling.cols) * @as(c_int, f.*.frame_hdr.*.tiling.rows);
                afz_pc = 254;
                continue;
            },
            256 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_row_3 < @as(c_int, f.*.frame_hdr.*.tiling.rows)) @as(c_int, 260) else @as(c_int, 255)));
                continue;
            },
            257 => {
                afz_tile_row_3 += 1;
                afz_pc = 256;
                continue;
            },
            258 => {
                afz_pc = @bitCast(@as(c_int, if (afz_sby_2 < @as(c_int, f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_3 + @as(c_int, 1))))])) @as(c_int, 259) else @as(c_int, 257)));
                continue;
            },
            259 => {
                f.*.lf.start_of_tile_row[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &afz_sby_2;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = 0;
                afz_pc = 258;
                continue;
            },
            260 => {
                f.*.lf.start_of_tile_row[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &afz_sby_2;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = @bitCast(@as(i8, @truncate(afz_tile_row_3)));
                afz_pc = 258;
                continue;
            },
            261 => {
                afz_tile_row_3 = 0;
                afz_pc = 256;
                continue;
            },
            262 => {
                afz_sby_2 = 0;
                afz_pc = 261;
                continue;
            },
            263 => {
                f.*.lf.start_of_tile_row_sz = f.*.sbh;
                afz_pc = 262;
                continue;
            },
            264 => {
                afz_pc = 1;
                continue;
            },
            265 => {
                f.*.lf.start_of_tile_row_sz = 0;
                afz_pc = 264;
                continue;
            },
            266 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.lf.start_of_tile_row != null)) @as(c_int, 265) else @as(c_int, 263)));
                continue;
            },
            267 => {
                f.*.lf.start_of_tile_row = @ptrCast(@alignCast(antfly_codec_afz_malloc(@as(c_ulong, @bitCast(@as(c_long, f.*.sbh))) *% @sizeOf(u8))));
                afz_pc = 266;
                continue;
            },
            268 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.start_of_tile_row)));
                afz_pc = 267;
                continue;
            },
            269 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.sbh > f.*.lf.start_of_tile_row_sz) @as(c_int, 268) else @as(c_int, 262)));
                continue;
            },
            270 => {
                afz_retval_1 = -@as(c_int, 12);
                afz_pc = 269;
                continue;
            },
            271 => {
                afz_c_0 = @ptrCast(@alignCast(@constCast(f.*.c)));
                afz_pc = 270;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub export fn antfly_av1_dav1d_decode_frame_init_cdf(f: [*c]Dav1dFrameContext) c_int {
    _ = &f;
    var afz_n_11: c_int = undefined;
    _ = &afz_n_11;
    var afz_uses_2pass_10: c_int = undefined;
    _ = &afz_uses_2pass_10;
    var afz_k_9: c_uint = undefined;
    _ = &afz_k_9;
    var afz_tile_sz_8: usize = undefined;
    _ = &afz_tile_sz_8;
    var afz_j_7: c_int = undefined;
    _ = &afz_j_7;
    var afz_size_6: usize = undefined;
    _ = &afz_size_6;
    var afz_data_5: [*c]u8 = undefined;
    _ = &afz_data_5;
    var afz_i_4: c_int = undefined;
    _ = &afz_i_4;
    var afz_tile_row_2: c_int = undefined;
    _ = &afz_tile_row_2;
    var afz_tile_col_3: c_int = undefined;
    _ = &afz_tile_col_3;
    var afz_retval_1: c_int = undefined;
    _ = &afz_retval_1;
    var afz_c_0: [*c]Dav1dContext = undefined;
    _ = &afz_c_0;
    var afz_pc: c_uint = 44;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_retval_1;
            },
            2 => {
                afz_retval_1 = 0;
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = @bitCast(@as(c_int, if (afz_n_11 < ((f.*.sb128w * @as(c_int, f.*.frame_hdr.*.tiling.rows)) * (@as(c_int, 1) + afz_uses_2pass_10))) @as(c_int, 5) else @as(c_int, 2)));
                continue;
            },
            4 => {
                afz_n_11 += 1;
                afz_pc = 3;
                continue;
            },
            5 => {
                reset_context(&f.*.a[@bitCast(@as(isize, @intCast(afz_n_11)))], @as(c_int, @intFromBool(!((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0))), if (afz_uses_2pass_10 != 0) @as(c_int, 1) + @as(c_int, @intFromBool(afz_n_11 >= (f.*.sb128w * @as(c_int, f.*.frame_hdr.*.tiling.rows)))) else @as(c_int, 0));
                afz_pc = 4;
                continue;
            },
            6 => {
                afz_n_11 = 0;
                afz_pc = 3;
                continue;
            },
            7 => {
                afz_uses_2pass_10 = @as(c_int, @intFromBool(afz_c_0.*.n_fc > @as(c_uint, 1)));
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_0.*.n_tc > @as(c_uint, 1)) @as(c_int, 7) else @as(c_int, 2)));
                continue;
            },
            9 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_4 < f.*.n_tile_data) @as(c_int, 37) else @as(c_int, 8)));
                continue;
            },
            10 => {
                afz_i_4 += 1;
                afz_pc = 9;
                continue;
            },
            11 => {
                afz_pc = @bitCast(@as(c_int, if (afz_j_7 <= f.*.tile[@bitCast(@as(isize, @intCast(afz_i_4)))].end) @as(c_int, 34) else @as(c_int, 10)));
                continue;
            },
            12 => {
                afz_j_7 += 1;
                afz_pc = 11;
                continue;
            },
            13 => {
                afz_size_6 -%= afz_tile_sz_8;
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_data_5 += afz_tile_sz_8;
                afz_pc = 13;
                continue;
            },
            15 => {
                f.*.task_thread.update_set = 1;
                afz_pc = 14;
                continue;
            },
            16 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_j_7 == @as(c_int, f.*.frame_hdr.*.tiling.update)) and (@as(c_int, f.*.frame_hdr.*.refresh_context) != 0)) @as(c_int, 15) else @as(c_int, 14)));
                continue;
            },
            17 => {
                afz_tile_row_2 += 1;
                afz_pc = 16;
                continue;
            },
            18 => {
                afz_tile_col_3 = 0;
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_col_3 == @as(c_int, f.*.frame_hdr.*.tiling.cols)) @as(c_int, 18) else @as(c_int, 16)));
                continue;
            },
            20 => {
                setup_tile(&f.*.ts[@bitCast(@as(isize, @intCast(afz_j_7)))], f, afz_data_5, afz_tile_sz_8, afz_tile_row_2, blk: {
                    const ref = &afz_tile_col_3;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }, if (afz_c_0.*.n_fc > @as(c_uint, 1)) f.*.frame_thread.tile_start_off[@bitCast(@as(isize, @intCast(afz_j_7)))] else @as(c_uint, 0));
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_tile_sz_8 = afz_size_6;
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_pc = 1;
                continue;
            },
            23 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_sz_8 > afz_size_6) @as(c_int, 22) else @as(c_int, 20)));
                continue;
            },
            24 => {
                afz_size_6 -%= @bitCast(@as(c_long, @as(c_int, f.*.frame_hdr.*.tiling.n_bytes)));
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_tile_sz_8 +%= 1;
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_pc = @bitCast(@as(c_int, if (afz_k_9 < @as(c_uint, @bitCast(@as(c_int, @as(c_int, f.*.frame_hdr.*.tiling.n_bytes))))) @as(c_int, 28) else @as(c_int, 25)));
                continue;
            },
            27 => {
                afz_k_9 +%= 1;
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_tile_sz_8 |= @as(c_uint, (blk: {
                    const ref = &afz_data_5;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }).*) << @intCast(afz_k_9 *% @as(c_uint, 8));
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_k_9 = 0;
                afz_pc = 26;
                continue;
            },
            30 => {
                afz_tile_sz_8 = 0;
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_pc = 1;
                continue;
            },
            32 => {
                afz_pc = @bitCast(@as(c_int, if (@as(usize, @bitCast(@as(c_long, @as(c_int, f.*.frame_hdr.*.tiling.n_bytes)))) > afz_size_6) @as(c_int, 31) else @as(c_int, 30)));
                continue;
            },
            33 => {
                afz_pc = @bitCast(@as(c_int, if (afz_j_7 == f.*.tile[@bitCast(@as(isize, @intCast(afz_i_4)))].end) @as(c_int, 21) else @as(c_int, 32)));
                continue;
            },
            34 => {
                afz_pc = 33;
                continue;
            },
            35 => {
                afz_j_7 = f.*.tile[@bitCast(@as(isize, @intCast(afz_i_4)))].start;
                afz_pc = 11;
                continue;
            },
            36 => {
                afz_size_6 = f.*.tile[@bitCast(@as(isize, @intCast(afz_i_4)))].data.sz;
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_data_5 = @ptrCast(@alignCast(@constCast(f.*.tile[@bitCast(@as(isize, @intCast(afz_i_4)))].data.data)));
                afz_pc = 36;
                continue;
            },
            38 => {
                afz_i_4 = 0;
                afz_pc = 9;
                continue;
            },
            39 => {
                f.*.task_thread.update_set = 0;
                afz_pc = 38;
                continue;
            },
            40 => {
                afz_tile_row_2 = 0;
                afz_tile_col_3 = 0;
                afz_pc = 39;
                continue;
            },
            41 => {
                antfly_av1_dav1d_cdf_thread_copy(f.*.out_cdf.data.cdf, &f.*.in_cdf);
                afz_pc = 40;
                continue;
            },
            42 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.frame_hdr.*.refresh_context != 0) @as(c_int, 41) else @as(c_int, 40)));
                continue;
            },
            43 => {
                afz_retval_1 = -@as(c_int, 22);
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_c_0 = @ptrCast(@alignCast(@constCast(f.*.c)));
                afz_pc = 43;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub export fn antfly_av1_dav1d_decode_frame_main(f: [*c]Dav1dFrameContext) c_int {
    _ = &f;
    var afz_tile_col_8: c_int = undefined;
    _ = &afz_tile_col_8;
    var afz_by_end_7: c_int = undefined;
    _ = &afz_by_end_7;
    var afz_sby_6: c_int = undefined;
    _ = &afz_sby_6;
    var afz_sbh_end_5: c_int = undefined;
    _ = &afz_sbh_end_5;
    var afz_tile_row_4: c_int = undefined;
    _ = &afz_tile_row_4;
    var afz_n_3: c_int = undefined;
    _ = &afz_n_3;
    var afz_t_2: [*c]Dav1dTaskContext = undefined;
    _ = &afz_t_2;
    var afz_retval_1: c_int = undefined;
    _ = &afz_retval_1;
    var afz_c_0: [*c]Dav1dContext = undefined;
    _ = &afz_c_0;
    var afz_pc: c_uint = 34;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_retval_1;
            },
            2 => {
                afz_retval_1 = 0;
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_row_4 < @as(c_int, f.*.frame_hdr.*.tiling.rows)) @as(c_int, 21) else @as(c_int, 2)));
                continue;
            },
            4 => {
                afz_tile_row_4 += 1;
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_pc = @bitCast(@as(c_int, if (afz_sby_6 < afz_sbh_end_5) @as(c_int, 19) else @as(c_int, 4)));
                continue;
            },
            6 => {
                afz_sby_6 += 1;
                afz_pc = 5;
                continue;
            },
            7 => {
                f.*.bd_fn.filter_sbrow.?(f, afz_sby_6);
                afz_pc = 6;
                continue;
            },
            8 => {
                dav1d_refmvs_save_tmvs(&f.*.c.*.refmvs_dsp, &afz_t_2.*.rt, 0, f.*.bw >> @intCast(@as(c_int, 1)), afz_t_2.*.by >> @intCast(@as(c_int, 1)), afz_by_end_7);
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_pc = @bitCast(@as(c_int, if ((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) @as(c_int, 8) else @as(c_int, 7)));
                continue;
            },
            10 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tile_col_8 < @as(c_int, f.*.frame_hdr.*.tiling.cols)) @as(c_int, 14) else @as(c_int, 9)));
                continue;
            },
            11 => {
                afz_tile_col_8 += 1;
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_pc = 1;
                continue;
            },
            13 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_decode_tile_sbrow(afz_t_2) != 0) @as(c_int, 12) else @as(c_int, 11)));
                continue;
            },
            14 => {
                afz_t_2.*.ts = &f.*.ts[@bitCast(@as(isize, @intCast((afz_tile_row_4 * @as(c_int, f.*.frame_hdr.*.tiling.cols)) + afz_tile_col_8)))];
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_tile_col_8 = 0;
                afz_pc = 10;
                continue;
            },
            16 => {
                f.*.c.*.refmvs_dsp.load_tmvs.?(&f.*.rf, afz_tile_row_4, 0, f.*.bw >> @intCast(@as(c_int, 1)), afz_t_2.*.by >> @intCast(@as(c_int, 1)), afz_by_end_7);
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = @bitCast(@as(c_int, if (f.*.frame_hdr.*.use_ref_frame_mvs != 0) @as(c_int, 16) else @as(c_int, 15)));
                continue;
            },
            18 => {
                afz_by_end_7 = (afz_t_2.*.by + f.*.sb_step) >> @intCast(@as(c_int, 1));
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_t_2.*.by = afz_sby_6 << @intCast(@as(c_int, 4) + @as(c_int, f.*.seq_hdr.*.sb128));
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_sby_6 = f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_4)))];
                afz_pc = 5;
                continue;
            },
            21 => {
                afz_sbh_end_5 = imin(f.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_tile_row_4 + @as(c_int, 1))))], f.*.sbh);
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_tile_row_4 = 0;
                afz_pc = 3;
                continue;
            },
            23 => {
                afz_pc = @bitCast(@as(c_int, if (afz_n_3 < (f.*.sb128w * @as(c_int, f.*.frame_hdr.*.tiling.rows))) @as(c_int, 25) else @as(c_int, 22)));
                continue;
            },
            24 => {
                afz_n_3 += 1;
                afz_pc = 23;
                continue;
            },
            25 => {
                reset_context(&f.*.a[@bitCast(@as(isize, @intCast(afz_n_3)))], @as(c_int, @intFromBool(!((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0))), 0);
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_n_3 = 0;
                afz_pc = 23;
                continue;
            },
            27 => {
                afz_t_2.*.frame_thread.pass = 0;
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_t_2.*.f = f;
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_t_2 = &afz_c_0.*.tc[@bitCast(@as(isize, @intCast(@divExact(@as(c_long, @bitCast(@intFromPtr(f) -% @intFromPtr(afz_c_0.*.fc))), @sizeOf(Dav1dFrameContext)))))];
                afz_pc = 28;
                continue;
            },
            30 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 32) else @as(c_int, 29);
                continue;
            },
            31 => {
                unreachable;
            },
            32 => {
                afz_pc = @bitCast(@as(c_int, if (!(f.*.c.*.n_tc == @as(c_uint, 1))) @as(c_int, 31) else @as(c_int, 30)));
                continue;
            },
            33 => {
                afz_retval_1 = -@as(c_int, 22);
                afz_pc = 32;
                continue;
            },
            34 => {
                afz_c_0 = @ptrCast(@alignCast(@constCast(f.*.c)));
                afz_pc = 33;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub export fn antfly_av1_dav1d_decode_frame_exit(f: [*c]Dav1dFrameContext, arg_retval: c_int) void {
    _ = &f;
    var retval = arg_retval;
    _ = &retval;
    const c: [*c]const Dav1dContext = f.*.c;
    _ = &c;
    if (f.*.sr_cur.p.data[@as(c_int, 0)] != null) {
        antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.@"error")), 0);
    }
    if (((c.*.n_fc > @as(c_uint, 1)) and (retval != 0)) and (f.*.frame_thread.cf != null)) {
        _ = antfly_codec_afz_memset(f.*.frame_thread.cf, 0, ((@as(usize, @bitCast(@as(c_long, f.*.frame_thread.cf_sz))) *% @as(usize, 128)) *% @as(usize, 128)) / @as(usize, 2));
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 7)) : (i += 1) {
            if (f.*.refp[@bitCast(@as(isize, @intCast(i)))].p.frame_hdr != null) {
                if (((!(retval != 0) and (c.*.n_fc > @as(c_uint, 1))) and (c.*.strict_std_compliance != 0)) and (@as(c_uint, @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&f.*.refp[@bitCast(@as(isize, @intCast(i)))].progress[@as(c_int, 1)])))))) == (((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))) {
                    retval = -@as(c_int, 22);
                    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.@"error")), 1);
                    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.sr_cur.progress[@as(c_int, 1)])), @bitCast(@as(c_uint, @truncate(((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))));
                }
                antfly_av1_dav1d_thread_picture_unref(&f.*.refp[@bitCast(@as(isize, @intCast(i)))]);
            }
            antfly_av1_dav1d_ref_dec(&f.*.ref_mvs_ref[@bitCast(@as(isize, @intCast(i)))]);
        }
    }
    antfly_av1_dav1d_picture_unref_internal(&f.*.cur);
    antfly_av1_dav1d_thread_picture_unref(&f.*.sr_cur);
    antfly_av1_dav1d_cdf_thread_unref(&f.*.in_cdf);
    if ((f.*.frame_hdr != null) and (@as(c_int, f.*.frame_hdr.*.refresh_context) != 0)) {
        if (f.*.out_cdf.progress != null) {
            antfly_codec_afz_atomic_store(@ptrCast(@alignCast(f.*.out_cdf.progress)), if (retval == @as(c_int, 0)) @as(c_int, 1) else @as(c_int, 2147483647) - @as(c_int, 1));
        }
        antfly_av1_dav1d_cdf_thread_unref(&f.*.out_cdf);
    }
    antfly_av1_dav1d_ref_dec(&f.*.cur_segmap_ref);
    antfly_av1_dav1d_ref_dec(&f.*.prev_segmap_ref);
    antfly_av1_dav1d_ref_dec(&f.*.mvs_ref);
    antfly_av1_dav1d_ref_dec(&f.*.seq_hdr_ref);
    antfly_av1_dav1d_ref_dec(&f.*.frame_hdr_ref);
    {
        var i: c_int = 0;
        _ = &i;
        while (i < f.*.n_tile_data) : (i += 1) {
            antfly_av1_dav1d_data_unref_internal(&f.*.tile[@bitCast(@as(isize, @intCast(i)))].data);
        }
    }
    f.*.task_thread.retval = retval;
}
pub export fn antfly_av1_dav1d_decode_frame(f: [*c]Dav1dFrameContext) c_int {
    _ = &f;
    while (true) {
        if (!(f.*.c.*.n_fc == @as(c_uint, 1))) {
            unreachable;
        }
        if (!false) break;
    }
    var res: c_int = antfly_av1_dav1d_decode_frame_init(f);
    _ = &res;
    if (!(res != 0)) {
        res = antfly_av1_dav1d_decode_frame_init_cdf(f);
    }
    if (!(res != 0)) {
        if (f.*.c.*.n_tc > @as(c_uint, 1)) {
            res = antfly_av1_dav1d_task_create_tile_sbrow(f, 0, 1);
            _ = antfly_codec_afz_mutex_lock(&f.*.task_thread.ttd.*.lock);
            _ = antfly_codec_afz_cond_signal(&f.*.task_thread.ttd.*.cond);
            if (!(res != 0)) {
                while (!(f.*.task_thread.done[@as(c_int, 0)] != 0) or (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&f.*.task_thread.task_counter))) > @as(c_int, 0))) {
                    _ = antfly_codec_afz_cond_wait(&f.*.task_thread.cond, &f.*.task_thread.ttd.*.lock);
                }
            }
            _ = antfly_codec_afz_mutex_unlock(&f.*.task_thread.ttd.*.lock);
            res = f.*.task_thread.retval;
        } else {
            res = antfly_av1_dav1d_decode_frame_main(f);
            if ((!(res != 0) and (@as(c_int, f.*.frame_hdr.*.refresh_context) != 0)) and (f.*.task_thread.update_set != 0)) {
                antfly_av1_dav1d_cdf_thread_update(f.*.frame_hdr, f.*.out_cdf.data.cdf, &f.*.ts[f.*.frame_hdr.*.tiling.update].cdf);
            }
        }
    }
    antfly_av1_dav1d_decode_frame_exit(f, res);
    res = f.*.task_thread.retval;
    f.*.n_tile_data = 0;
    return res;
}
pub fn get_upscale_x0(in_w: c_int, out_w: c_int, step: c_int) callconv(.c) c_int {
    _ = &in_w;
    _ = &out_w;
    _ = &step;
    const err: c_int = (out_w * step) - (in_w << @intCast(@as(c_int, 14)));
    _ = &err;
    const x0: c_int = (@divTrunc(-((out_w - in_w) << @intCast(@as(c_int, 13))) + (out_w >> @intCast(@as(c_int, 1))), out_w) + @as(c_int, 128)) - @divTrunc(err, @as(c_int, 2));
    _ = &x0;
    return x0 & @as(c_int, 16383);
}
pub export fn antfly_av1_dav1d_submit_frame(c: [*c]Dav1dContext) c_int {
    _ = &c;
    var afz_i_34: c_int = undefined;
    _ = &afz_i_34;
    var afz_i_33: c_int = undefined;
    _ = &afz_i_33;
    var afz_i_32: c_int = undefined;
    _ = &afz_i_32;
    var afz_i_31: c_int = undefined;
    _ = &afz_i_31;
    var afz_refresh_frame_flags_30: c_uint = undefined;
    _ = &afz_refresh_frame_flags_30;
    var afz_segmap_size_29: usize = undefined;
    _ = &afz_segmap_size_29;
    var afz_ref_h_28: c_int = undefined;
    _ = &afz_ref_h_28;
    var afz_ref_w_27: c_int = undefined;
    _ = &afz_ref_w_27;
    var afz_pri_ref_26: c_int = undefined;
    _ = &afz_pri_ref_26;
    var afz_ref_h_25: c_int = undefined;
    _ = &afz_ref_h_25;
    var afz_ref_w_24: c_int = undefined;
    _ = &afz_ref_w_24;
    var afz_refidx_23: c_int = undefined;
    _ = &afz_refidx_23;
    var afz_i_22: c_int = undefined;
    _ = &afz_i_22;
    var afz_i_21: c_int = undefined;
    _ = &afz_i_21;
    var afz_rows_20: c_int = undefined;
    _ = &afz_rows_20;
    var afz_cols_19: c_int = undefined;
    _ = &afz_cols_19;
    var afz_uses_2pass_18: c_int = undefined;
    _ = &afz_uses_2pass_18;
    var afz_out_cw_17: c_int = undefined;
    _ = &afz_out_cw_17;
    var afz_in_cw_16: c_int = undefined;
    _ = &afz_in_cw_16;
    var afz_ss_hor_15: c_int = undefined;
    _ = &afz_ss_hor_15;
    var afz_pri_ref_14: c_int = undefined;
    _ = &afz_pri_ref_14;
    var afz_j_13: c_int = undefined;
    _ = &afz_j_13;
    var afz_refidx_12: c_int = undefined;
    _ = &afz_refidx_12;
    var afz_i_11: c_int = undefined;
    _ = &afz_i_11;
    var afz_pri_ref_10: c_int = undefined;
    _ = &afz_pri_ref_10;
    var afz_ref_coded_width_9: [7]c_int = undefined;
    _ = &afz_ref_coded_width_9;
    var afz_dsp_8: [*c]Dav1dDSPContext = undefined;
    _ = &afz_dsp_8;
    var afz_bpc_7: c_int = undefined;
    _ = &afz_bpc_7;
    var afz_progress_6: c_uint = undefined;
    _ = &afz_progress_6;
    var afz_error_5: c_int = undefined;
    _ = &afz_error_5;
    var afz_first_4: c_uint = undefined;
    _ = &afz_first_4;
    var afz_next_3: c_uint = undefined;
    _ = &afz_next_3;
    var afz_out_delayed_2: [*c]Dav1dThreadPicture = undefined;
    _ = &afz_out_delayed_2;
    var afz_res_1: c_int = undefined;
    _ = &afz_res_1;
    var afz_f_0: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_0;
    var afz_pc: c_uint = 310;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_res_1;
            },
            2 => {
                _ = antfly_codec_afz_mutex_unlock(&c.*.task_thread.lock);
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_fc > @as(c_uint, 1)) @as(c_int, 2) else @as(c_int, 1)));
                continue;
            },
            4 => {
                afz_f_0.*.n_tile_data = 0;
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_34 < afz_f_0.*.n_tile_data) @as(c_int, 7) else @as(c_int, 4)));
                continue;
            },
            6 => {
                afz_i_34 += 1;
                afz_pc = 5;
                continue;
            },
            7 => {
                antfly_av1_dav1d_data_unref_internal(&afz_f_0.*.tile[@bitCast(@as(isize, @intCast(afz_i_34)))].data);
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_i_34 = 0;
                afz_pc = 5;
                continue;
            },
            9 => {
                antfly_av1_dav1d_data_props_copy(&c.*.cached_error_props, &c.*.in.m);
                afz_pc = 8;
                continue;
            },
            10 => {
                antfly_av1_dav1d_ref_dec(&afz_f_0.*.frame_hdr_ref);
                afz_pc = 9;
                continue;
            },
            11 => {
                antfly_av1_dav1d_ref_dec(&afz_f_0.*.seq_hdr_ref);
                afz_pc = 10;
                continue;
            },
            12 => {
                antfly_av1_dav1d_ref_dec(&afz_f_0.*.mvs_ref);
                afz_pc = 11;
                continue;
            },
            13 => {
                antfly_av1_dav1d_thread_picture_unref(&afz_f_0.*.sr_cur);
                afz_pc = 12;
                continue;
            },
            14 => {
                antfly_av1_dav1d_picture_unref_internal(&afz_f_0.*.cur);
                afz_pc = 13;
                continue;
            },
            15 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.out);
                afz_pc = 14;
                continue;
            },
            16 => {
                antfly_av1_dav1d_thread_picture_unref(afz_out_delayed_2);
                afz_pc = 14;
                continue;
            },
            17 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_fc == @as(c_uint, 1)) @as(c_int, 15) else @as(c_int, 16)));
                continue;
            },
            18 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_33 < @as(c_int, 7)) @as(c_int, 22) else @as(c_int, 17)));
                continue;
            },
            19 => {
                afz_i_33 += 1;
                afz_pc = 18;
                continue;
            },
            20 => {
                antfly_av1_dav1d_ref_dec(&afz_f_0.*.ref_mvs_ref[@bitCast(@as(isize, @intCast(afz_i_33)))]);
                afz_pc = 19;
                continue;
            },
            21 => {
                antfly_av1_dav1d_thread_picture_unref(&afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_i_33)))]);
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_i_33)))].p.frame_hdr != null) @as(c_int, 21) else @as(c_int, 20)));
                continue;
            },
            23 => {
                afz_i_33 = 0;
                afz_pc = 18;
                continue;
            },
            24 => {
                antfly_av1_dav1d_cdf_thread_unref(&afz_f_0.*.out_cdf);
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.refresh_context != 0) @as(c_int, 24) else @as(c_int, 23)));
                continue;
            },
            26 => {
                antfly_av1_dav1d_cdf_thread_unref(&afz_f_0.*.in_cdf);
                afz_pc = 25;
                continue;
            },
            27 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_0.*.task_thread.@"error")), 1);
                afz_pc = 26;
                continue;
            },
            28 => {
                return 0;
            },
            29 => {
                afz_pc = 27;
                continue;
            },
            30 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_32 < @as(c_int, 8)) @as(c_int, 37) else @as(c_int, 29)));
                continue;
            },
            31 => {
                afz_i_32 += 1;
                afz_pc = 30;
                continue;
            },
            32 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_32)))].refmvs);
                afz_pc = 31;
                continue;
            },
            33 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_32)))].segmap);
                afz_pc = 32;
                continue;
            },
            34 => {
                antfly_av1_dav1d_cdf_thread_unref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_32)))]);
                afz_pc = 33;
                continue;
            },
            35 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_32)))].p);
                afz_pc = 34;
                continue;
            },
            36 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.refs[@bitCast(@as(isize, @intCast(afz_i_32)))].p.p.frame_hdr != null) @as(c_int, 35) else @as(c_int, 34)));
                continue;
            },
            37 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_refresh_frame_flags_30 & @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(afz_i_32))))) != 0) @as(c_int, 36) else @as(c_int, 31)));
                continue;
            },
            38 => {
                afz_i_32 = 0;
                afz_pc = 30;
                continue;
            },
            39 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.out);
                afz_pc = 38;
                continue;
            },
            40 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const tmp = antfly_av1_dav1d_decode_frame(afz_f_0);
                    afz_res_1 = tmp;
                    break :blk tmp;
                }) < @as(c_int, 0)) @as(c_int, 39) else @as(c_int, 28)));
                continue;
            },
            41 => {
                _ = antfly_codec_afz_mutex_unlock(&c.*.task_thread.lock);
                afz_pc = 28;
                continue;
            },
            42 => {
                antfly_av1_dav1d_task_frame_init(afz_f_0);
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_fc == @as(c_uint, 1)) @as(c_int, 40) else @as(c_int, 42)));
                continue;
            },
            44 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_31 < @as(c_int, 8)) @as(c_int, 63) else @as(c_int, 43)));
                continue;
            },
            45 => {
                afz_i_31 += 1;
                afz_pc = 44;
                continue;
            },
            46 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]c_uint, @ptrCast(@alignCast(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].refpoc))))), @ptrCast(@alignCast(@as([*c]c_uint, @ptrCast(@alignCast(&afz_f_0.*.refpoc))))), @sizeOf(@TypeOf(afz_f_0.*.refpoc)));
                afz_pc = 45;
                continue;
            },
            47 => {
                dav1d_ref_inc(afz_f_0.*.mvs_ref);
                afz_pc = 46;
                continue;
            },
            48 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.mvs_ref != null) @as(c_int, 47) else @as(c_int, 46)));
                continue;
            },
            49 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].refmvs = afz_f_0.*.mvs_ref;
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_f_0.*.frame_hdr.*.allow_intrabc) != 0)) @as(c_int, 49) else @as(c_int, 46)));
                continue;
            },
            51 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].refmvs);
                afz_pc = 50;
                continue;
            },
            52 => {
                dav1d_ref_inc(afz_f_0.*.cur_segmap_ref);
                afz_pc = 51;
                continue;
            },
            53 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.cur_segmap_ref != null) @as(c_int, 52) else @as(c_int, 51)));
                continue;
            },
            54 => {
                c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].segmap = afz_f_0.*.cur_segmap_ref;
                afz_pc = 53;
                continue;
            },
            55 => {
                antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].segmap);
                afz_pc = 54;
                continue;
            },
            56 => {
                antfly_av1_dav1d_cdf_thread_ref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_31)))], &afz_f_0.*.out_cdf);
                afz_pc = 55;
                continue;
            },
            57 => {
                antfly_av1_dav1d_cdf_thread_ref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_31)))], &afz_f_0.*.in_cdf);
                afz_pc = 55;
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.refresh_context != 0) @as(c_int, 56) else @as(c_int, 57)));
                continue;
            },
            59 => {
                antfly_av1_dav1d_cdf_thread_unref(&c.*.cdf[@bitCast(@as(isize, @intCast(afz_i_31)))]);
                afz_pc = 58;
                continue;
            },
            60 => {
                antfly_av1_dav1d_thread_picture_ref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].p, &afz_f_0.*.sr_cur);
                afz_pc = 59;
                continue;
            },
            61 => {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].p);
                afz_pc = 60;
                continue;
            },
            62 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.refs[@bitCast(@as(isize, @intCast(afz_i_31)))].p.p.frame_hdr != null) @as(c_int, 61) else @as(c_int, 60)));
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_refresh_frame_flags_30 & @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(afz_i_31))))) != 0) @as(c_int, 62) else @as(c_int, 45)));
                continue;
            },
            64 => {
                afz_i_31 = 0;
                afz_pc = 44;
                continue;
            },
            65 => {
                afz_refresh_frame_flags_30 = afz_f_0.*.frame_hdr.*.refresh_frame_flags;
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_f_0.*.cur_segmap = @ptrCast(@alignCast(afz_f_0.*.cur_segmap_ref.*.data));
                afz_pc = 65;
                continue;
            },
            67 => {
                afz_pc = 27;
                continue;
            },
            68 => {
                afz_res_1 = -@as(c_int, 12);
                afz_pc = 67;
                continue;
            },
            69 => {
                antfly_av1_dav1d_ref_dec(&afz_f_0.*.prev_segmap_ref);
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_f_0.*.cur_segmap_ref != null)) @as(c_int, 69) else @as(c_int, 66)));
                continue;
            },
            71 => {
                afz_f_0.*.cur_segmap_ref = antfly_av1_dav1d_ref_create_using_pool(c.*.segmap_pool, ((@sizeOf(@TypeOf(afz_f_0.*.cur_segmap.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_f_0.*.b4_stride)))) *% @as(c_ulong, 32)) *% @as(c_ulong, @bitCast(@as(c_long, afz_f_0.*.sb128h))));
                afz_pc = 70;
                continue;
            },
            72 => {
                afz_f_0.*.cur_segmap = @ptrCast(@alignCast(afz_f_0.*.prev_segmap_ref.*.data));
                afz_pc = 65;
                continue;
            },
            73 => {
                dav1d_ref_inc(afz_f_0.*.cur_segmap_ref);
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_f_0.*.cur_segmap_ref = afz_f_0.*.prev_segmap_ref;
                afz_pc = 73;
                continue;
            },
            75 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_f_0.*.cur_segmap)), 0, afz_segmap_size_29);
                afz_pc = 65;
                continue;
            },
            76 => {
                afz_f_0.*.cur_segmap = @ptrCast(@alignCast(afz_f_0.*.cur_segmap_ref.*.data));
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_pc = 27;
                continue;
            },
            78 => {
                afz_res_1 = -@as(c_int, 12);
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_f_0.*.cur_segmap_ref != null)) @as(c_int, 78) else @as(c_int, 76)));
                continue;
            },
            80 => {
                afz_f_0.*.cur_segmap_ref = antfly_av1_dav1d_ref_create_using_pool(c.*.segmap_pool, afz_segmap_size_29);
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_segmap_size_29 = ((@sizeOf(@TypeOf(afz_f_0.*.cur_segmap.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_f_0.*.b4_stride)))) *% @as(c_ulong, 32)) *% @as(c_ulong, @bitCast(@as(c_long, afz_f_0.*.sb128h)));
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.prev_segmap_ref != null) @as(c_int, 74) else @as(c_int, 81)));
                continue;
            },
            83 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.segmentation.update_map != 0) @as(c_int, 71) else @as(c_int, 82)));
                continue;
            },
            84 => {
                afz_f_0.*.prev_segmap = @ptrCast(@alignCast(afz_f_0.*.prev_segmap_ref.*.data));
                afz_pc = 83;
                continue;
            },
            85 => {
                dav1d_ref_inc(afz_f_0.*.prev_segmap_ref);
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.prev_segmap_ref != null) @as(c_int, 85) else @as(c_int, 83)));
                continue;
            },
            87 => {
                afz_f_0.*.prev_segmap_ref = c.*.refs[@bitCast(@as(isize, @intCast(afz_f_0.*.frame_hdr.*.refidx[@bitCast(@as(isize, @intCast(afz_pri_ref_26)))])))].segmap;
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_ref_w_27 == afz_f_0.*.bw) and (afz_ref_h_28 == afz_f_0.*.bh)) @as(c_int, 87) else @as(c_int, 83)));
                continue;
            },
            89 => {
                afz_ref_h_28 = ((afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_pri_ref_26)))].p.p.h + @as(c_int, 7)) >> @intCast(@as(c_int, 3))) << @intCast(@as(c_int, 1));
                afz_pc = 88;
                continue;
            },
            90 => {
                afz_ref_w_27 = ((afz_ref_coded_width_9[@bitCast(@as(isize, @intCast(afz_pri_ref_26)))] + @as(c_int, 7)) >> @intCast(@as(c_int, 3))) << @intCast(@as(c_int, 1));
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 93) else @as(c_int, 90);
                continue;
            },
            92 => {
                unreachable;
            },
            93 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_pri_ref_26 != @as(c_int, 7))) @as(c_int, 92) else @as(c_int, 91)));
                continue;
            },
            94 => {
                afz_pri_ref_26 = afz_f_0.*.frame_hdr.*.primary_ref_frame;
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_0.*.frame_hdr.*.segmentation.temporal) != 0) or !(@as(c_int, afz_f_0.*.frame_hdr.*.segmentation.update_map) != 0)) @as(c_int, 94) else @as(c_int, 83)));
                continue;
            },
            96 => {
                afz_f_0.*.prev_segmap = null;
                afz_pc = 95;
                continue;
            },
            97 => {
                afz_f_0.*.prev_segmap_ref = null;
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_f_0.*.prev_segmap_ref = null;
                afz_pc = 65;
                continue;
            },
            99 => {
                afz_f_0.*.cur_segmap_ref = null;
                afz_pc = 98;
                continue;
            },
            100 => {
                afz_f_0.*.cur_segmap = null;
                afz_pc = 99;
                continue;
            },
            101 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.segmentation.enabled != 0) @as(c_int, 97) else @as(c_int, 100)));
                continue;
            },
            102 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_22 < @as(c_int, 7)) @as(c_int, 113) else @as(c_int, 101)));
                continue;
            },
            103 => {
                afz_i_22 += 1;
                afz_pc = 102;
                continue;
            },
            104 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]c_uint, @ptrCast(@alignCast(&afz_f_0.*.refrefpoc[@bitCast(@as(isize, @intCast(afz_i_22)))]))))), @ptrCast(@alignCast(@as([*c]c_uint, @ptrCast(@alignCast(&c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_23)))].refpoc))))), @sizeOf(@TypeOf(@as([*c][7]c_uint, @ptrCast(@alignCast(&afz_f_0.*.refrefpoc))).*)));
                afz_pc = 103;
                continue;
            },
            105 => {
                afz_f_0.*.ref_mvs[@bitCast(@as(isize, @intCast(afz_i_22)))] = @ptrCast(@alignCast(c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_23)))].refmvs.*.data));
                afz_pc = 104;
                continue;
            },
            106 => {
                dav1d_ref_inc(afz_f_0.*.ref_mvs_ref[@bitCast(@as(isize, @intCast(afz_i_22)))]);
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_f_0.*.ref_mvs_ref[@bitCast(@as(isize, @intCast(afz_i_22)))] = c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_23)))].refmvs;
                afz_pc = 106;
                continue;
            },
            108 => {
                afz_f_0.*.ref_mvs_ref[@bitCast(@as(isize, @intCast(afz_i_22)))] = null;
                afz_pc = 104;
                continue;
            },
            109 => {
                afz_f_0.*.ref_mvs[@bitCast(@as(isize, @intCast(afz_i_22)))] = null;
                afz_pc = 108;
                continue;
            },
            110 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(?*anyopaque, @ptrCast(@alignCast(c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_23)))].refmvs))) != @as(?*anyopaque, null)) and (afz_ref_w_24 == afz_f_0.*.bw)) and (afz_ref_h_25 == afz_f_0.*.bh)) @as(c_int, 107) else @as(c_int, 109)));
                continue;
            },
            111 => {
                afz_ref_h_25 = ((afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_i_22)))].p.p.h + @as(c_int, 7)) >> @intCast(@as(c_int, 3))) << @intCast(@as(c_int, 1));
                afz_pc = 110;
                continue;
            },
            112 => {
                afz_ref_w_24 = ((afz_ref_coded_width_9[@bitCast(@as(isize, @intCast(afz_i_22)))] + @as(c_int, 7)) >> @intCast(@as(c_int, 3))) << @intCast(@as(c_int, 1));
                afz_pc = 111;
                continue;
            },
            113 => {
                afz_refidx_23 = afz_f_0.*.frame_hdr.*.refidx[@bitCast(@as(isize, @intCast(afz_i_22)))];
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_i_22 = 0;
                afz_pc = 102;
                continue;
            },
            115 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][*c]Dav1dRef, @ptrCast(@alignCast(&afz_f_0.*.ref_mvs_ref))))), 0, @sizeOf(@TypeOf(afz_f_0.*.ref_mvs_ref)));
                afz_pc = 101;
                continue;
            },
            116 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.use_ref_frame_mvs != 0) @as(c_int, 114) else @as(c_int, 115)));
                continue;
            },
            117 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_21 < @as(c_int, 7)) @as(c_int, 119) else @as(c_int, 116)));
                continue;
            },
            118 => {
                afz_i_21 += 1;
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_f_0.*.refpoc[@bitCast(@as(isize, @intCast(afz_i_21)))] = afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_i_21)))].p.frame_hdr.*.frame_offset;
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_i_21 = 0;
                afz_pc = 117;
                continue;
            },
            121 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]c_uint, @ptrCast(@alignCast(&afz_f_0.*.refpoc))))), 0, @sizeOf(@TypeOf(afz_f_0.*.refpoc)));
                afz_pc = 116;
                continue;
            },
            122 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_f_0.*.frame_hdr.*.allow_intrabc) != 0)) @as(c_int, 120) else @as(c_int, 121)));
                continue;
            },
            123 => {
                afz_f_0.*.mvs = @ptrCast(@alignCast(afz_f_0.*.mvs_ref.*.data));
                afz_pc = 122;
                continue;
            },
            124 => {
                afz_pc = 27;
                continue;
            },
            125 => {
                afz_res_1 = -@as(c_int, 12);
                afz_pc = 124;
                continue;
            },
            126 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_f_0.*.mvs_ref != null)) @as(c_int, 125) else @as(c_int, 123)));
                continue;
            },
            127 => {
                afz_f_0.*.mvs_ref = antfly_av1_dav1d_ref_create_using_pool(c.*.refmvs_pool, ((@sizeOf(@TypeOf(afz_f_0.*.mvs.*)) *% @as(c_ulong, @bitCast(@as(c_long, afz_f_0.*.sb128h)))) *% @as(c_ulong, 16)) *% @as(c_ulong, @bitCast(@as(c_long, afz_f_0.*.b4_stride >> @intCast(@as(ptrdiff_t, 1))))));
                afz_pc = 126;
                continue;
            },
            128 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c][*c]Dav1dRef, @ptrCast(@alignCast(&afz_f_0.*.ref_mvs_ref))))), 0, @sizeOf(@TypeOf(afz_f_0.*.ref_mvs_ref)));
                afz_pc = 101;
                continue;
            },
            129 => {
                afz_f_0.*.mvs_ref = null;
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_f_0.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) or (@as(enum_Dav1dFrameType, @bitCast(@as(c_int, afz_f_0.*.frame_hdr.*.allow_intrabc))) != 0)) @as(c_int, 127) else @as(c_int, 129)));
                continue;
            },
            131 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_0.*.task_thread.task_counter)), ((afz_cols_19 * afz_rows_20) + afz_f_0.*.sbh) << @intCast(afz_uses_2pass_18));
                afz_pc = 130;
                continue;
            },
            132 => {
                afz_rows_20 = afz_f_0.*.frame_hdr.*.tiling.rows;
                afz_pc = 131;
                continue;
            },
            133 => {
                afz_cols_19 = afz_f_0.*.frame_hdr.*.tiling.cols;
                afz_pc = 132;
                continue;
            },
            134 => {
                afz_uses_2pass_18 = @as(c_int, @intFromBool(c.*.n_fc > @as(c_uint, 1)));
                afz_pc = 133;
                continue;
            },
            135 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_0.*.task_thread.@"error")), 0);
                afz_pc = 134;
                continue;
            },
            136 => {
                afz_f_0.*.bitdepth_max = (@as(c_int, 1) << @intCast(afz_f_0.*.cur.p.bpc)) - @as(c_int, 1);
                afz_pc = 135;
                continue;
            },
            137 => {
                afz_f_0.*.b4_stride = (afz_f_0.*.bw + @as(c_int, 31)) & ~@as(c_int, 31);
                afz_pc = 136;
                continue;
            },
            138 => {
                afz_f_0.*.sbh = ((afz_f_0.*.bh + afz_f_0.*.sb_step) - @as(c_int, 1)) >> @intCast(afz_f_0.*.sb_shift);
                afz_pc = 137;
                continue;
            },
            139 => {
                afz_f_0.*.sb_step = @as(c_int, 16) << @intCast(@as(c_int, afz_f_0.*.seq_hdr.*.sb128));
                afz_pc = 138;
                continue;
            },
            140 => {
                afz_f_0.*.sb_shift = @as(c_int, 4) + @as(c_int, afz_f_0.*.seq_hdr.*.sb128);
                afz_pc = 139;
                continue;
            },
            141 => {
                afz_f_0.*.sb128h = (afz_f_0.*.bh + @as(c_int, 31)) >> @intCast(@as(c_int, 5));
                afz_pc = 140;
                continue;
            },
            142 => {
                afz_f_0.*.sb128w = (afz_f_0.*.bw + @as(c_int, 31)) >> @intCast(@as(c_int, 5));
                afz_pc = 141;
                continue;
            },
            143 => {
                afz_f_0.*.bh = ((afz_f_0.*.frame_hdr.*.height + @as(c_int, 7)) >> @intCast(@as(c_int, 3))) << @intCast(@as(c_int, 1));
                afz_pc = 142;
                continue;
            },
            144 => {
                afz_f_0.*.bw = ((afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] + @as(c_int, 7)) >> @intCast(@as(c_int, 3))) << @intCast(@as(c_int, 1));
                afz_pc = 143;
                continue;
            },
            145 => {
                afz_f_0.*.h4 = (afz_f_0.*.frame_hdr.*.height + @as(c_int, 3)) >> @intCast(@as(c_int, 2));
                afz_pc = 144;
                continue;
            },
            146 => {
                afz_f_0.*.w4 = (afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] + @as(c_int, 3)) >> @intCast(@as(c_int, 2));
                afz_pc = 145;
                continue;
            },
            147 => {
                c.*.event_flags |= antfly_av1_dav1d_picture_get_event_flags(&afz_f_0.*.sr_cur);
                afz_pc = 146;
                continue;
            },
            148 => {
                antfly_av1_dav1d_thread_picture_ref(&c.*.out, &afz_f_0.*.sr_cur);
                afz_pc = 147;
                continue;
            },
            149 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_0.*.frame_hdr.*.show_frame) != 0) or (c.*.output_invisible_frames != 0)) @as(c_int, 148) else @as(c_int, 146)));
                continue;
            },
            150 => {
                antfly_av1_dav1d_thread_picture_ref(afz_out_delayed_2, &afz_f_0.*.sr_cur);
                afz_pc = 146;
                continue;
            },
            151 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_fc == @as(c_uint, 1)) @as(c_int, 149) else @as(c_int, 150)));
                continue;
            },
            152 => {
                afz_f_0.*.resize_start[@as(c_int, 1)] = get_upscale_x0(afz_in_cw_16, afz_out_cw_17, afz_f_0.*.resize_step[@as(c_int, 1)]);
                afz_pc = 151;
                continue;
            },
            153 => {
                afz_f_0.*.resize_start[@as(c_int, 0)] = get_upscale_x0(afz_f_0.*.cur.p.w, afz_f_0.*.sr_cur.p.p.w, afz_f_0.*.resize_step[@as(c_int, 0)]);
                afz_pc = 152;
                continue;
            },
            154 => {
                afz_f_0.*.resize_step[@as(c_int, 1)] = @divTrunc((afz_in_cw_16 << @intCast(@as(c_int, 14))) + (afz_out_cw_17 >> @intCast(@as(c_int, 1))), afz_out_cw_17);
                afz_pc = 153;
                continue;
            },
            155 => {
                afz_out_cw_17 = (afz_f_0.*.sr_cur.p.p.w + afz_ss_hor_15) >> @intCast(afz_ss_hor_15);
                afz_pc = 154;
                continue;
            },
            156 => {
                afz_in_cw_16 = (afz_f_0.*.cur.p.w + afz_ss_hor_15) >> @intCast(afz_ss_hor_15);
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_ss_hor_15 = @as(c_int, @intFromBool(afz_f_0.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
                afz_pc = 156;
                continue;
            },
            158 => {
                afz_f_0.*.resize_step[@as(c_int, 0)] = @divTrunc((afz_f_0.*.cur.p.w << @intCast(@as(c_int, 14))) + (afz_f_0.*.sr_cur.p.p.w >> @intCast(@as(c_int, 1))), afz_f_0.*.sr_cur.p.p.w);
                afz_pc = 157;
                continue;
            },
            159 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] != afz_f_0.*.frame_hdr.*.width[@as(c_int, 1)]) @as(c_int, 158) else @as(c_int, 151)));
                continue;
            },
            160 => {
                afz_pc = 27;
                continue;
            },
            161 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_1 < @as(c_int, 0)) @as(c_int, 160) else @as(c_int, 159)));
                continue;
            },
            162 => {
                afz_res_1 = antfly_av1_dav1d_picture_alloc_copy(c, &afz_f_0.*.cur, afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)], &afz_f_0.*.sr_cur.p);
                afz_pc = 161;
                continue;
            },
            163 => {
                antfly_av1_dav1d_picture_ref(&afz_f_0.*.cur, &afz_f_0.*.sr_cur.p);
                afz_pc = 159;
                continue;
            },
            164 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] != afz_f_0.*.frame_hdr.*.width[@as(c_int, 1)]) @as(c_int, 162) else @as(c_int, 163)));
                continue;
            },
            165 => {
                afz_pc = 27;
                continue;
            },
            166 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_1 < @as(c_int, 0)) @as(c_int, 165) else @as(c_int, 164)));
                continue;
            },
            167 => {
                afz_res_1 = antfly_av1_dav1d_thread_picture_alloc(c, afz_f_0, afz_bpc_7);
                afz_pc = 166;
                continue;
            },
            168 => {
                c.*.n_tile_data = 0;
                afz_pc = 167;
                continue;
            },
            169 => {
                afz_f_0.*.n_tile_data = c.*.n_tile_data;
                afz_pc = 168;
                continue;
            },
            170 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(c.*.tile)), 0, @as(c_ulong, @bitCast(@as(c_long, c.*.n_tile_data))) *% @sizeOf(@TypeOf(c.*.tile.*)));
                afz_pc = 169;
                continue;
            },
            171 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(afz_f_0.*.tile)), @ptrCast(@alignCast(c.*.tile)), @as(c_ulong, @bitCast(@as(c_long, c.*.n_tile_data))) *% @sizeOf(@TypeOf(afz_f_0.*.tile.*)));
                afz_pc = 170;
                continue;
            },
            172 => {
                afz_f_0.*.n_tile_data_alloc = c.*.n_tile_data;
                afz_pc = 171;
                continue;
            },
            173 => {
                afz_pc = 27;
                continue;
            },
            174 => {
                afz_res_1 = -@as(c_int, 12);
                afz_pc = 173;
                continue;
            },
            175 => {
                afz_f_0.*.n_tile_data_alloc = blk: {
                    const tmp = @as(c_int, 0);
                    afz_f_0.*.n_tile_data = tmp;
                    break :blk tmp;
                };
                afz_pc = 174;
                continue;
            },
            176 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_f_0.*.tile != null)) @as(c_int, 175) else @as(c_int, 172)));
                continue;
            },
            177 => {
                afz_f_0.*.tile = @ptrCast(@alignCast(antfly_codec_afz_malloc(@as(c_ulong, @bitCast(@as(c_long, c.*.n_tile_data))) *% @sizeOf(@TypeOf(afz_f_0.*.tile.*)))));
                afz_pc = 176;
                continue;
            },
            178 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 180) else @as(c_int, 177);
                continue;
            },
            179 => {
                unreachable;
            },
            180 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.n_tile_data < @divTrunc(@as(c_int, 2147483647), @as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(@TypeOf(afz_f_0.*.tile.*))))))))) @as(c_int, 179) else @as(c_int, 178)));
                continue;
            },
            181 => {
                antfly_codec_afz_free(@ptrCast(@alignCast(afz_f_0.*.tile)));
                afz_pc = 180;
                continue;
            },
            182 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.n_tile_data_alloc < c.*.n_tile_data) @as(c_int, 181) else @as(c_int, 171)));
                continue;
            },
            183 => {
                afz_pc = 27;
                continue;
            },
            184 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_1 < @as(c_int, 0)) @as(c_int, 183) else @as(c_int, 182)));
                continue;
            },
            185 => {
                afz_res_1 = antfly_av1_dav1d_cdf_thread_alloc(c, &afz_f_0.*.out_cdf, @as(c_int, @intFromBool(c.*.n_fc > @as(c_uint, 1))));
                afz_pc = 184;
                continue;
            },
            186 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.frame_hdr.*.refresh_context != 0) @as(c_int, 185) else @as(c_int, 182)));
                continue;
            },
            187 => {
                antfly_av1_dav1d_cdf_thread_init_static(&afz_f_0.*.in_cdf, afz_f_0.*.frame_hdr.*.quant.yac);
                afz_pc = 186;
                continue;
            },
            188 => {
                antfly_av1_dav1d_cdf_thread_ref(&afz_f_0.*.in_cdf, &c.*.cdf[@bitCast(@as(isize, @intCast(afz_pri_ref_14)))]);
                afz_pc = 186;
                continue;
            },
            189 => {
                afz_pri_ref_14 = afz_f_0.*.frame_hdr.*.refidx[afz_f_0.*.frame_hdr.*.primary_ref_frame];
                afz_pc = 188;
                continue;
            },
            190 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_f_0.*.frame_hdr.*.primary_ref_frame) == @as(c_int, 7)) @as(c_int, 187) else @as(c_int, 189)));
                continue;
            },
            191 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_11 < @as(c_int, 7)) @as(c_int, 209) else @as(c_int, 190)));
                continue;
            },
            192 => {
                afz_i_11 += 1;
                afz_pc = 191;
                continue;
            },
            193 => {
                afz_f_0.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(afz_i_11)))] = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool((((afz_f_0.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(afz_i_11)))].type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION)) and !(@as(c_int, afz_f_0.*.frame_hdr.*.force_integer_mv) != 0)) and !(antfly_av1_dav1d_get_shear_params(&afz_f_0.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(afz_i_11)))]) != 0)) and !(afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 0)].scale != 0))))));
                afz_pc = 192;
                continue;
            },
            194 => {
                afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 1)].step = (afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 1)].scale + @as(c_int, 8)) >> @intCast(@as(c_int, 4));
                afz_pc = 193;
                continue;
            },
            195 => {
                afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 0)].step = (afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 0)].scale + @as(c_int, 8)) >> @intCast(@as(c_int, 4));
                afz_pc = 194;
                continue;
            },
            196 => {
                afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 1)].scale = @divTrunc((c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.h << @intCast(@as(c_int, 14))) + (afz_f_0.*.frame_hdr.*.height >> @intCast(@as(c_int, 1))), afz_f_0.*.frame_hdr.*.height);
                afz_pc = 195;
                continue;
            },
            197 => {
                afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 0)].scale = @divTrunc((c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.w << @intCast(@as(c_int, 14))) + (afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] >> @intCast(@as(c_int, 1))), afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)]);
                afz_pc = 196;
                continue;
            },
            198 => {
                afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 0)].scale = blk: {
                    const tmp = @as(c_int, 0);
                    afz_f_0.*.svc[@bitCast(@as(isize, @intCast(afz_i_11)))][@as(c_int, 1)].scale = tmp;
                    break :blk tmp;
                };
                afz_pc = 193;
                continue;
            },
            199 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] != c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.w) or (afz_f_0.*.frame_hdr.*.height != c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.h)) @as(c_int, 197) else @as(c_int, 198)));
                continue;
            },
            200 => {
                afz_ref_coded_width_9[@bitCast(@as(isize, @intCast(afz_i_11)))] = c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.frame_hdr.*.width[@as(c_int, 0)];
                afz_pc = 199;
                continue;
            },
            201 => {
                antfly_av1_dav1d_thread_picture_ref(&afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_i_11)))], &c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p);
                afz_pc = 200;
                continue;
            },
            202 => {
                afz_pc = 27;
                continue;
            },
            203 => {
                afz_res_1 = -@as(c_int, 22);
                afz_pc = 202;
                continue;
            },
            204 => {
                afz_pc = @bitCast(@as(c_int, if (afz_j_13 < afz_i_11) @as(c_int, 206) else @as(c_int, 203)));
                continue;
            },
            205 => {
                afz_j_13 += 1;
                afz_pc = 204;
                continue;
            },
            206 => {
                antfly_av1_dav1d_thread_picture_unref(&afz_f_0.*.refp[@bitCast(@as(isize, @intCast(afz_j_13)))]);
                afz_pc = 205;
                continue;
            },
            207 => {
                afz_j_13 = 0;
                afz_pc = 204;
                continue;
            },
            208 => {
                afz_pc = @bitCast(@as(c_int, if ((((((!(c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.data[@as(c_int, 0)] != null) or ((afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] * @as(c_int, 2)) < c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.w)) or ((afz_f_0.*.frame_hdr.*.height * @as(c_int, 2)) < c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.h)) or (afz_f_0.*.frame_hdr.*.width[@as(c_int, 0)] > (c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.w * @as(c_int, 16)))) or (afz_f_0.*.frame_hdr.*.height > (c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.h * @as(c_int, 16)))) or (afz_f_0.*.seq_hdr.*.layout != c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.layout)) or (afz_bpc_7 != c.*.refs[@bitCast(@as(isize, @intCast(afz_refidx_12)))].p.p.p.bpc)) @as(c_int, 207) else @as(c_int, 201)));
                continue;
            },
            209 => {
                afz_refidx_12 = afz_f_0.*.frame_hdr.*.refidx[@bitCast(@as(isize, @intCast(afz_i_11)))];
                afz_pc = 208;
                continue;
            },
            210 => {
                afz_i_11 = 0;
                afz_pc = 191;
                continue;
            },
            211 => {
                afz_pc = 27;
                continue;
            },
            212 => {
                afz_res_1 = -@as(c_int, 22);
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.refs[@bitCast(@as(isize, @intCast(afz_pri_ref_10)))].p.p.data[@as(c_int, 0)] != null)) @as(c_int, 212) else @as(c_int, 210)));
                continue;
            },
            214 => {
                afz_pri_ref_10 = afz_f_0.*.frame_hdr.*.refidx[afz_f_0.*.frame_hdr.*.primary_ref_frame];
                afz_pc = 213;
                continue;
            },
            215 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_f_0.*.frame_hdr.*.primary_ref_frame) != @as(c_int, 7)) @as(c_int, 214) else @as(c_int, 210)));
                continue;
            },
            216 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_f_0.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0) @as(c_int, 215) else @as(c_int, 190)));
                continue;
            },
            217 => {
                afz_pc = 216;
                continue;
            },
            218 => {
                afz_f_0.*.bd_fn.read_pal_uv = antfly_av1_dav1d_read_pal_uv_8bpc;
                afz_pc = 217;
                continue;
            },
            219 => {
                afz_f_0.*.bd_fn.read_pal_plane = antfly_av1_dav1d_read_pal_plane_8bpc;
                afz_pc = 218;
                continue;
            },
            220 => {
                afz_f_0.*.bd_fn.copy_pal_block_uv = antfly_av1_dav1d_copy_pal_block_uv_8bpc;
                afz_pc = 219;
                continue;
            },
            221 => {
                afz_f_0.*.bd_fn.copy_pal_block_y = antfly_av1_dav1d_copy_pal_block_y_8bpc;
                afz_pc = 220;
                continue;
            },
            222 => {
                afz_f_0.*.bd_fn.read_coef_blocks = antfly_av1_dav1d_read_coef_blocks_8bpc;
                afz_pc = 221;
                continue;
            },
            223 => {
                afz_f_0.*.bd_fn.backup_ipred_edge = antfly_av1_dav1d_backup_ipred_edge_8bpc;
                afz_pc = 222;
                continue;
            },
            224 => {
                afz_f_0.*.bd_fn.filter_sbrow_lr = antfly_av1_dav1d_filter_sbrow_lr_8bpc;
                afz_pc = 223;
                continue;
            },
            225 => {
                afz_f_0.*.bd_fn.filter_sbrow_resize = antfly_av1_dav1d_filter_sbrow_resize_8bpc;
                afz_pc = 224;
                continue;
            },
            226 => {
                afz_f_0.*.bd_fn.filter_sbrow_cdef = antfly_av1_dav1d_filter_sbrow_cdef_8bpc;
                afz_pc = 225;
                continue;
            },
            227 => {
                afz_f_0.*.bd_fn.filter_sbrow_deblock_rows = antfly_av1_dav1d_filter_sbrow_deblock_rows_8bpc;
                afz_pc = 226;
                continue;
            },
            228 => {
                afz_f_0.*.bd_fn.filter_sbrow_deblock_cols = antfly_av1_dav1d_filter_sbrow_deblock_cols_8bpc;
                afz_pc = 227;
                continue;
            },
            229 => {
                afz_f_0.*.bd_fn.filter_sbrow = antfly_av1_dav1d_filter_sbrow_8bpc;
                afz_pc = 228;
                continue;
            },
            230 => {
                afz_f_0.*.bd_fn.recon_b_intra = antfly_av1_dav1d_recon_b_intra_8bpc;
                afz_pc = 229;
                continue;
            },
            231 => {
                afz_f_0.*.bd_fn.recon_b_inter = antfly_av1_dav1d_recon_b_inter_8bpc;
                afz_pc = 230;
                continue;
            },
            232 => {
                afz_f_0.*.bd_fn.read_pal_uv = antfly_av1_dav1d_read_pal_uv_16bpc;
                afz_pc = 217;
                continue;
            },
            233 => {
                afz_f_0.*.bd_fn.read_pal_plane = antfly_av1_dav1d_read_pal_plane_16bpc;
                afz_pc = 232;
                continue;
            },
            234 => {
                afz_f_0.*.bd_fn.copy_pal_block_uv = antfly_av1_dav1d_copy_pal_block_uv_16bpc;
                afz_pc = 233;
                continue;
            },
            235 => {
                afz_f_0.*.bd_fn.copy_pal_block_y = antfly_av1_dav1d_copy_pal_block_y_16bpc;
                afz_pc = 234;
                continue;
            },
            236 => {
                afz_f_0.*.bd_fn.read_coef_blocks = antfly_av1_dav1d_read_coef_blocks_16bpc;
                afz_pc = 235;
                continue;
            },
            237 => {
                afz_f_0.*.bd_fn.backup_ipred_edge = antfly_av1_dav1d_backup_ipred_edge_16bpc;
                afz_pc = 236;
                continue;
            },
            238 => {
                afz_f_0.*.bd_fn.filter_sbrow_lr = antfly_av1_dav1d_filter_sbrow_lr_16bpc;
                afz_pc = 237;
                continue;
            },
            239 => {
                afz_f_0.*.bd_fn.filter_sbrow_resize = antfly_av1_dav1d_filter_sbrow_resize_16bpc;
                afz_pc = 238;
                continue;
            },
            240 => {
                afz_f_0.*.bd_fn.filter_sbrow_cdef = antfly_av1_dav1d_filter_sbrow_cdef_16bpc;
                afz_pc = 239;
                continue;
            },
            241 => {
                afz_f_0.*.bd_fn.filter_sbrow_deblock_rows = antfly_av1_dav1d_filter_sbrow_deblock_rows_16bpc;
                afz_pc = 240;
                continue;
            },
            242 => {
                afz_f_0.*.bd_fn.filter_sbrow_deblock_cols = antfly_av1_dav1d_filter_sbrow_deblock_cols_16bpc;
                afz_pc = 241;
                continue;
            },
            243 => {
                afz_f_0.*.bd_fn.filter_sbrow = antfly_av1_dav1d_filter_sbrow_16bpc;
                afz_pc = 242;
                continue;
            },
            244 => {
                afz_f_0.*.bd_fn.recon_b_intra = antfly_av1_dav1d_recon_b_intra_16bpc;
                afz_pc = 243;
                continue;
            },
            245 => {
                afz_f_0.*.bd_fn.recon_b_inter = antfly_av1_dav1d_recon_b_inter_16bpc;
                afz_pc = 244;
                continue;
            },
            246 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_f_0.*.seq_hdr.*.hbd) != 0)) @as(c_int, 231) else @as(c_int, 245)));
                continue;
            },
            247 => {
                afz_pc = 27;
                continue;
            },
            248 => {
                afz_res_1 = -@as(c_int, 42);
                afz_pc = 247;
                continue;
            },
            249 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 249) else @as(c_int, 248);
                continue;
            },
            250 => {
                afz_pc = 246;
                continue;
            },
            251 => {
                antfly_av1_dav1d_film_grain_dsp_init_16bpc(&afz_dsp_8.*.fg);
                afz_pc = 250;
                continue;
            },
            252 => {
                antfly_av1_dav1d_mc_dsp_init_16bpc(&afz_dsp_8.*.mc);
                afz_pc = 251;
                continue;
            },
            253 => {
                antfly_av1_dav1d_loop_restoration_dsp_init_16bpc(&afz_dsp_8.*.lr, afz_bpc_7);
                afz_pc = 252;
                continue;
            },
            254 => {
                antfly_av1_dav1d_loop_filter_dsp_init_16bpc(&afz_dsp_8.*.lf);
                afz_pc = 253;
                continue;
            },
            255 => {
                antfly_av1_dav1d_itx_dsp_init_16bpc(&afz_dsp_8.*.itx, afz_bpc_7);
                afz_pc = 254;
                continue;
            },
            256 => {
                antfly_av1_dav1d_intra_pred_dsp_init_16bpc(&afz_dsp_8.*.ipred);
                afz_pc = 255;
                continue;
            },
            257 => {
                antfly_av1_dav1d_cdef_dsp_init_16bpc(&afz_dsp_8.*.cdef);
                afz_pc = 256;
                continue;
            },
            258 => {
                afz_pc = 246;
                continue;
            },
            259 => {
                antfly_av1_dav1d_film_grain_dsp_init_8bpc(&afz_dsp_8.*.fg);
                afz_pc = 258;
                continue;
            },
            260 => {
                antfly_av1_dav1d_mc_dsp_init_8bpc(&afz_dsp_8.*.mc);
                afz_pc = 259;
                continue;
            },
            261 => {
                antfly_av1_dav1d_loop_restoration_dsp_init_8bpc(&afz_dsp_8.*.lr, afz_bpc_7);
                afz_pc = 260;
                continue;
            },
            262 => {
                antfly_av1_dav1d_loop_filter_dsp_init_8bpc(&afz_dsp_8.*.lf);
                afz_pc = 261;
                continue;
            },
            263 => {
                antfly_av1_dav1d_itx_dsp_init_8bpc(&afz_dsp_8.*.itx, afz_bpc_7);
                afz_pc = 262;
                continue;
            },
            264 => {
                antfly_av1_dav1d_intra_pred_dsp_init_8bpc(&afz_dsp_8.*.ipred);
                afz_pc = 263;
                continue;
            },
            265 => {
                antfly_av1_dav1d_cdef_dsp_init_8bpc(&afz_dsp_8.*.cdef);
                afz_pc = 264;
                continue;
            },
            266 => {
                blk: {
                    switch (afz_bpc_7) {
                        @as(c_int, 12) => {
                            afz_pc = 257;
                            continue;
                        },
                        @as(c_int, 10) => {
                            afz_pc = 257;
                            continue;
                        },
                        @as(c_int, 8) => {
                            afz_pc = 265;
                            continue;
                        },
                        else => {
                            afz_pc = 249;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            267 => {
                afz_dsp_8 = &c.*.dsp[afz_f_0.*.seq_hdr.*.hbd];
                afz_pc = 266;
                continue;
            },
            268 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_f_0.*.dsp.*.ipred.intra_pred[DC_PRED] != null)) @as(c_int, 267) else @as(c_int, 246)));
                continue;
            },
            269 => {
                afz_bpc_7 = @as(c_int, 8) + (@as(c_int, 2) * @as(c_int, afz_f_0.*.seq_hdr.*.hbd));
                afz_pc = 268;
                continue;
            },
            270 => {
                afz_f_0.*.dsp = &c.*.dsp[afz_f_0.*.seq_hdr.*.hbd];
                afz_pc = 269;
                continue;
            },
            271 => {
                c.*.frame_hdr_ref = null;
                afz_pc = 270;
                continue;
            },
            272 => {
                c.*.frame_hdr = null;
                afz_pc = 271;
                continue;
            },
            273 => {
                afz_f_0.*.frame_hdr_ref = c.*.frame_hdr_ref;
                afz_pc = 272;
                continue;
            },
            274 => {
                afz_f_0.*.frame_hdr = c.*.frame_hdr;
                afz_pc = 273;
                continue;
            },
            275 => {
                dav1d_ref_inc(afz_f_0.*.seq_hdr_ref);
                afz_pc = 274;
                continue;
            },
            276 => {
                afz_f_0.*.seq_hdr_ref = c.*.seq_hdr_ref;
                afz_pc = 275;
                continue;
            },
            277 => {
                afz_f_0.*.seq_hdr = c.*.seq_hdr;
                afz_pc = 276;
                continue;
            },
            278 => {
                antfly_av1_dav1d_thread_picture_unref(afz_out_delayed_2);
                afz_pc = 277;
                continue;
            },
            279 => {
                antfly_av1_dav1d_data_props_copy(&c.*.cached_error_props, &afz_out_delayed_2.*.p.m);
                afz_pc = 278;
                continue;
            },
            280 => {
                c.*.cached_error = afz_error_5;
                afz_pc = 279;
                continue;
            },
            281 => {
                afz_f_0.*.task_thread.retval = 0;
                afz_pc = 280;
                continue;
            },
            282 => {
                antfly_av1_dav1d_thread_picture_unref(afz_out_delayed_2);
                afz_pc = 277;
                continue;
            },
            283 => {
                c.*.event_flags |= antfly_av1_dav1d_picture_get_event_flags(afz_out_delayed_2);
                afz_pc = 282;
                continue;
            },
            284 => {
                antfly_av1_dav1d_thread_picture_ref(&c.*.out, afz_out_delayed_2);
                afz_pc = 283;
                continue;
            },
            285 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_out_delayed_2.*.visible != 0) or (c.*.output_invisible_frames != 0)) and (afz_progress_6 != (((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))) @as(c_int, 284) else @as(c_int, 282)));
                continue;
            },
            286 => {
                afz_progress_6 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_out_delayed_2.*.progress[@as(c_int, 1)])))));
                afz_pc = 285;
                continue;
            },
            287 => {
                afz_pc = @bitCast(@as(c_int, if (afz_out_delayed_2.*.p.data[@as(c_int, 0)] != null) @as(c_int, 286) else @as(c_int, 277)));
                continue;
            },
            288 => {
                afz_pc = @bitCast(@as(c_int, if (afz_error_5 != 0) @as(c_int, 281) else @as(c_int, 287)));
                continue;
            },
            289 => {
                afz_error_5 = afz_f_0.*.task_thread.retval;
                afz_pc = 288;
                continue;
            },
            290 => {
                c.*.task_thread.cur -%= 1;
                afz_pc = 289;
                continue;
            },
            291 => {
                afz_pc = @bitCast(@as(c_int, if ((c.*.task_thread.cur != 0) and (@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(c.*.task_thread.cur < c.*.n_fc))))) != 0)) @as(c_int, 290) else @as(c_int, 289)));
                continue;
            },
            292 => {
                _ = antfly_codec_afz_atomic_compare(@ptrCast(@alignCast(&c.*.task_thread.reset_task_cur)), @ptrCast(@alignCast(&afz_first_4)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))));
                afz_pc = 291;
                continue;
            },
            293 => {
                _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&c.*.task_thread.first)), 1);
                afz_pc = 292;
                continue;
            },
            294 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.task_thread.first)), 0);
                afz_pc = 292;
                continue;
            },
            295 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_first_4 +% @as(c_uint, 1)) < c.*.n_fc) @as(c_int, 293) else @as(c_int, 294)));
                continue;
            },
            296 => {
                afz_first_4 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&c.*.task_thread.first)))));
                afz_pc = 295;
                continue;
            },
            297 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_out_delayed_2.*.p.data[@as(c_int, 0)] != null) or (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_0.*.task_thread.@"error"))) != 0)) @as(c_int, 296) else @as(c_int, 289)));
                continue;
            },
            298 => {
                afz_out_delayed_2 = &c.*.frame_thread.out_delayed[afz_next_3];
                afz_pc = 297;
                continue;
            },
            299 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_0.*.n_tile_data > @as(c_int, 0)) @as(c_int, 300) else @as(c_int, 298)));
                continue;
            },
            300 => {
                _ = antfly_codec_afz_cond_wait(&afz_f_0.*.task_thread.cond, &c.*.task_thread.lock);
                afz_pc = 299;
                continue;
            },
            301 => {
                afz_f_0 = &c.*.fc[afz_next_3];
                afz_pc = 299;
                continue;
            },
            302 => {
                c.*.frame_thread.next = 0;
                afz_pc = 301;
                continue;
            },
            303 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.frame_thread.next == c.*.n_fc) @as(c_int, 302) else @as(c_int, 301)));
                continue;
            },
            304 => {
                afz_next_3 = blk: {
                    const ref = &c.*.frame_thread.next;
                    const tmp = ref.*;
                    ref.* +%= 1;
                    break :blk tmp;
                };
                afz_pc = 303;
                continue;
            },
            305 => {
                _ = antfly_codec_afz_mutex_lock(&c.*.task_thread.lock);
                afz_pc = 304;
                continue;
            },
            306 => {
                afz_f_0 = c.*.fc;
                afz_pc = 277;
                continue;
            },
            307 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_fc > @as(c_uint, 1)) @as(c_int, 305) else @as(c_int, 306)));
                continue;
            },
            308 => {
                afz_pc = 307;
                continue;
            },
            309 => {
                afz_res_1 = -@as(c_int, 1);
                afz_pc = 308;
                continue;
            },
            310 => {
                afz_pc = 309;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
