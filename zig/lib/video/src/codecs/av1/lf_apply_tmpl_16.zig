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
//  * Copyright © 2018, VideoLAN and dav1d authors
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
pub const __builtin_va_list = [*c]u8;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub const ptrdiff_t = common.ptrdiff_t;
pub fn imin(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub const pixel = u16;
pub const coef = i32;
pub fn PXSTRIDE(x: ptrdiff_t) callconv(.c) ptrdiff_t {
    _ = &x;
    while (true) {
        if (!!((x & @as(ptrdiff_t, 1)) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    return x >> @intCast(@as(ptrdiff_t, 1));
}
pub const atomic_int = common.atomic_int;
pub const atomic_uint = common.atomic_uint;
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
pub const Dav1dRef = struct_Dav1dRef;
pub const DAV1D_PIXEL_LAYOUT_I400 = common.DAV1D_PIXEL_LAYOUT_I400;
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
pub const DAV1D_PIXEL_LAYOUT_I444 = common.DAV1D_PIXEL_LAYOUT_I444;
pub const enum_Dav1dPixelLayout = common.enum_Dav1dPixelLayout;
pub const enum_Dav1dColorPrimaries = common.enum_Dav1dColorPrimaries;
pub const enum_Dav1dTransferCharacteristics = common.enum_Dav1dTransferCharacteristics;
pub const enum_Dav1dMatrixCoefficients = common.enum_Dav1dMatrixCoefficients;
pub const enum_Dav1dChromaSamplePosition = common.enum_Dav1dChromaSamplePosition;
pub const struct_Dav1dSequenceHeaderOperatingPoint_1 = common.struct_Dav1dSequenceHeaderOperatingPoint_1;
pub const enum_Dav1dAdaptiveBoolean = common.enum_Dav1dAdaptiveBoolean;
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_2 = common.struct_Dav1dSequenceHeaderOperatingParameterInfo_2;
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
    operating_points: [32]struct_Dav1dSequenceHeaderOperatingPoint_1 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingPoint_1),
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
    operating_parameter_info: [32]struct_Dav1dSequenceHeaderOperatingParameterInfo_2 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingParameterInfo_2),
};
pub const Dav1dSequenceHeader = struct_Dav1dSequenceHeader;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
const struct_unnamed_3 = extern struct {
    data: Dav1dFilmGrainData = @import("std").mem.zeroes(Dav1dFilmGrainData),
    present: u8 = 0,
    update: u8 = 0,
};
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const struct_Dav1dFrameHeaderOperatingPoint_4 = common.struct_Dav1dFrameHeaderOperatingPoint_4;
const struct_unnamed_5 = extern struct {
    width_scale_denominator: u8 = 0,
    enabled: u8 = 0,
};
pub const enum_Dav1dFilterMode = common.enum_Dav1dFilterMode;
const struct_unnamed_6 = extern struct {
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
const struct_unnamed_7 = extern struct {
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
pub const Dav1dSegmentationDataSet = common.Dav1dSegmentationDataSet;
const struct_unnamed_8 = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    temporal: u8 = 0,
    update_data: u8 = 0,
    seg_data: Dav1dSegmentationDataSet = @import("std").mem.zeroes(Dav1dSegmentationDataSet),
    lossless: [8]u8 = @import("std").mem.zeroes([8]u8),
    qidx: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_10 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
};
const struct_unnamed_11 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
    multi: u8 = 0,
};
const struct_unnamed_9 = extern struct {
    q: struct_unnamed_10 = @import("std").mem.zeroes(struct_unnamed_10),
    lf: struct_unnamed_11 = @import("std").mem.zeroes(struct_unnamed_11),
};
pub const Dav1dLoopfilterModeRefDeltas = common.Dav1dLoopfilterModeRefDeltas;
const struct_unnamed_12 = extern struct {
    level_y: [2]u8 = @import("std").mem.zeroes([2]u8),
    level_u: u8 = 0,
    level_v: u8 = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    mode_ref_deltas: Dav1dLoopfilterModeRefDeltas = @import("std").mem.zeroes(Dav1dLoopfilterModeRefDeltas),
    sharpness: u8 = 0,
};
const struct_unnamed_13 = extern struct {
    damping: u8 = 0,
    n_bits: u8 = 0,
    y_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
    uv_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
};
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
const struct_unnamed_14 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
pub const enum_Dav1dWarpedMotionType = common.enum_Dav1dWarpedMotionType;
const struct_unnamed_16 = extern struct {
    alpha: i16 = 0,
    beta: i16 = 0,
    gamma: i16 = 0,
    delta: i16 = 0,
};
const union_unnamed_15 = extern union {
    p: struct_unnamed_16,
    abcd: [4]i16,
};
pub const struct_Dav1dWarpedMotionParams = extern struct {
    type: enum_Dav1dWarpedMotionType = @import("std").mem.zeroes(enum_Dav1dWarpedMotionType),
    matrix: [6]i32 = @import("std").mem.zeroes([6]i32),
    u: union_unnamed_15 = @import("std").mem.zeroes(union_unnamed_15),
};
pub const Dav1dWarpedMotionParams = struct_Dav1dWarpedMotionParams;
pub const struct_Dav1dFrameHeader = extern struct {
    film_grain: struct_unnamed_3 = @import("std").mem.zeroes(struct_unnamed_3),
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
    operating_points: [32]struct_Dav1dFrameHeaderOperatingPoint_4 = @import("std").mem.zeroes([32]struct_Dav1dFrameHeaderOperatingPoint_4),
    refresh_frame_flags: u8 = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    super_res: struct_unnamed_5 = @import("std").mem.zeroes(struct_unnamed_5),
    have_render_size: u8 = 0,
    allow_intrabc: u8 = 0,
    frame_ref_short_signaling: u8 = 0,
    refidx: [7]i8 = @import("std").mem.zeroes([7]i8),
    hp: u8 = 0,
    subpel_filter_mode: enum_Dav1dFilterMode = @import("std").mem.zeroes(enum_Dav1dFilterMode),
    switchable_motion_mode: u8 = 0,
    use_ref_frame_mvs: u8 = 0,
    refresh_context: u8 = 0,
    tiling: struct_unnamed_6 = @import("std").mem.zeroes(struct_unnamed_6),
    quant: struct_unnamed_7 = @import("std").mem.zeroes(struct_unnamed_7),
    segmentation: struct_unnamed_8 = @import("std").mem.zeroes(struct_unnamed_8),
    delta: struct_unnamed_9 = @import("std").mem.zeroes(struct_unnamed_9),
    all_lossless: u8 = 0,
    loopfilter: struct_unnamed_12 = @import("std").mem.zeroes(struct_unnamed_12),
    cdef: struct_unnamed_13 = @import("std").mem.zeroes(struct_unnamed_13),
    restoration: struct_unnamed_14 = @import("std").mem.zeroes(struct_unnamed_14),
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
const struct_unnamed_17 = extern struct {
    y: i16 = 0,
    x: i16 = 0,
};
pub const union_mv = extern union {
    unnamed_0: struct_unnamed_17,
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
const union_unnamed_18 = extern union {
    cdf: [*c]CdfContext,
    qcat: c_uint,
};
pub const struct_CdfThreadContext = extern struct {
    ref: [*c]Dav1dRef = null,
    data: union_unnamed_18 = @import("std").mem.zeroes(union_unnamed_18),
    progress: [*c]atomic_uint = null,
};
pub const CdfThreadContext = struct_CdfThreadContext;
pub const struct_Dav1dTileGroup = extern struct {
    data: Dav1dData = @import("std").mem.zeroes(Dav1dData),
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_ScalableMotionParams_19 = common.struct_ScalableMotionParams_19;
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
const struct_unnamed_20 = extern struct {
    col_start: c_int = 0,
    col_end: c_int = 0,
    row_start: c_int = 0,
    row_end: c_int = 0,
    col: c_int = 0,
    row: c_int = 0,
};
const struct_unnamed_21 = extern struct {
    pal_idx: [*c]u8 = null,
    cbi: [*c]i16 = null,
    cf: [*c]coef = null,
};
const union_unnamed_22 = extern union {
    i8: [4]i8,
    u32: u32,
};
pub const Av1RestorationUnit = common.Av1RestorationUnit;
pub const struct_Dav1dTileState = extern struct {
    cdf: CdfContext = @import("std").mem.zeroes(CdfContext),
    msac: MsacContext = @import("std").mem.zeroes(MsacContext),
    tiling: struct_unnamed_20 = @import("std").mem.zeroes(struct_unnamed_20),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    frame_thread: [2]struct_unnamed_21 = @import("std").mem.zeroes([2]struct_unnamed_21),
    lowest_pixel: [*c][7][2]c_int = null,
    dqmem: [8][3][2]u16 = @import("std").mem.zeroes([8][3][2]u16),
    dq: [*c][3][2]u16 = null,
    last_qidx: c_int = 0,
    last_delta_lf: union_unnamed_22 = @import("std").mem.zeroes(union_unnamed_22),
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
const struct_unnamed_23 = extern struct {
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_refmvs_tile = extern struct {
    rf: [*c]const refmvs_frame = null,
    r: [37][*c]refmvs_block = @import("std").mem.zeroes([37][*c]refmvs_block),
    rp_proj: [*c]refmvs_temporal_block = null,
    tile_col: struct_unnamed_23 = @import("std").mem.zeroes(struct_unnamed_23),
    tile_row: struct_unnamed_23 = @import("std").mem.zeroes(struct_unnamed_23),
};
pub const refmvs_tile = struct_refmvs_tile;
const union_unnamed_24 = extern union {
    cf_8bpc: [1024]i16 align(64),
    cf_16bpc: [1024]i32 align(64),
};
const union_unnamed_25 = extern union {
    al_pal_8bpc: [2][32][3][8]u8,
    al_pal_16bpc: [2][32][3][8]u16,
};
const struct_unnamed_29 = extern struct {
    compinter: [2][16384]i16 = @import("std").mem.zeroes([2][16384]i16),
    seg_mask: [16384]u8 = @import("std").mem.zeroes([16384]u8),
};
const union_unnamed_28 = extern union {
    lap_8bpc: [4096]u8,
    lap_16bpc: [4096]u16,
    unnamed_0: struct_unnamed_29,
};
const union_unnamed_30 = extern union {
    emu_edge_8bpc: [84160]u8,
    emu_edge_16bpc: [84160]u16,
};
const struct_unnamed_27 = extern struct {
    unnamed_0: union_unnamed_28 = @import("std").mem.zeroes(union_unnamed_28),
    unnamed_1: union_unnamed_30 = @import("std").mem.zeroes(union_unnamed_30),
};
const struct_unnamed_33 = extern struct {
    pal_order: [64][8]u8 = @import("std").mem.zeroes([64][8]u8),
    pal_ctx: [64]u8 = @import("std").mem.zeroes([64]u8),
};
const union_unnamed_32 = extern union {
    levels: [1088]u8,
    unnamed_0: struct_unnamed_33,
};
const union_unnamed_34 = extern union {
    ac: [1024]i16,
    txtp_map: [1024]u8,
};
const struct_unnamed_36 = extern struct {
    interintra_8bpc: [4096]u8 align(8) = @import("std").mem.zeroes([4096]u8),
    edge_8bpc: [257]u8 = @import("std").mem.zeroes([257]u8),
    pal_8bpc: [3][8]u8 align(8) = @import("std").mem.zeroes([3][8]u8),
};
const struct_unnamed_37 = extern struct {
    interintra_16bpc: [4096]u16 align(16) = @import("std").mem.zeroes([4096]u16),
    edge_16bpc: [257]u16 = @import("std").mem.zeroes([257]u16),
    pal_16bpc: [3][8]u16 align(16) = @import("std").mem.zeroes([3][8]u16),
};
const union_unnamed_35 = extern union {
    unnamed_0: struct_unnamed_36,
    unnamed_1: struct_unnamed_37,
};
const struct_unnamed_31 = extern struct {
    unnamed_0: union_unnamed_32 = @import("std").mem.zeroes(union_unnamed_32),
    unnamed_1: union_unnamed_34 = @import("std").mem.zeroes(union_unnamed_34),
    pal_idx_y: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    pal_idx_uv: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    unnamed_2: union_unnamed_35 = @import("std").mem.zeroes(union_unnamed_35),
};
const union_unnamed_26 = extern union {
    unnamed_0: struct_unnamed_27 align(64),
    unnamed_1: struct_unnamed_31 align(64),
};
pub const Av1Filter = common.Av1Filter;
pub const enum_Filter2d = common.enum_Filter2d;
const struct_unnamed_38 = extern struct {
    pass: c_int = 0,
};
pub const afz_pthread_cond_t = common.afz_pthread_cond_t;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const struct_thread_data = common.struct_thread_data;
pub const enum_TaskType = common.enum_TaskType;
const struct_unnamed_43 = extern struct {
    grain_lut_8bpc: [3][74][82]i8 align(64) = @import("std").mem.zeroes([3][74][82]i8),
    scaling_8bpc: [3][256]u8 align(64) = @import("std").mem.zeroes([3][256]u8),
};
const struct_unnamed_44 = extern struct {
    grain_lut_16bpc: [3][74][82]i16 align(64) = @import("std").mem.zeroes([3][74][82]i16),
    scaling_16bpc: [3][4096]u8 align(64) = @import("std").mem.zeroes([3][4096]u8),
};
const union_unnamed_42 = extern union {
    unnamed_0: struct_unnamed_43,
    unnamed_1: struct_unnamed_44,
};
const struct_unnamed_41 = extern struct {
    exec: c_int = 0,
    finished: c_int = 0,
    cond: afz_pthread_cond_t = 0,
    in: [*c]const Dav1dPicture = null,
    out: [*c]Dav1dPicture = null,
    type: enum_TaskType = @import("std").mem.zeroes(enum_TaskType),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    unnamed_0: union_unnamed_42 = @import("std").mem.zeroes(union_unnamed_42),
};
pub const struct_TaskThreadData_40 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    first: atomic_uint = 0,
    cur: c_uint = 0,
    reset_task_cur: atomic_uint = 0,
    cond_signaled: atomic_int = 0,
    delayed_fg: struct_unnamed_41 = @import("std").mem.zeroes(struct_unnamed_41),
    inited: c_int = 0,
};
pub const struct_FrameTileThreadData_45 = common.struct_FrameTileThreadData_45;
const struct_unnamed_39 = extern struct {
    td: struct_thread_data = @import("std").mem.zeroes(struct_thread_data),
    ttd: [*c]struct_TaskThreadData_40 = null,
    fttd: [*c]struct_FrameTileThreadData_45 = null,
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
    unnamed_0: union_unnamed_24 = @import("std").mem.zeroes(union_unnamed_24),
    unnamed_1: union_unnamed_25 = @import("std").mem.zeroes(union_unnamed_25),
    pal_sz_uv: [2][32]u8 = @import("std").mem.zeroes([2][32]u8),
    scratch: union_unnamed_26 = @import("std").mem.zeroes(union_unnamed_26),
    warpmv: Dav1dWarpedMotionParams = @import("std").mem.zeroes(Dav1dWarpedMotionParams),
    lf_mask: [*c]Av1Filter = null,
    top_pre_cdef_toggle: c_int = 0,
    cur_sb_cdef_idx_ptr: [*c]i8 = null,
    tl_4x4_filter: enum_Filter2d = @import("std").mem.zeroes(enum_Filter2d),
    frame_thread: struct_unnamed_38 = @import("std").mem.zeroes(struct_unnamed_38),
    task_thread: struct_unnamed_39 = @import("std").mem.zeroes(struct_unnamed_39),
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
const struct_unnamed_46 = extern struct {
    out_delayed: [*c]Dav1dThreadPicture = null,
    next: c_uint = 0,
};
const struct_unnamed_47 = extern struct {
    p: Dav1dThreadPicture = @import("std").mem.zeroes(Dav1dThreadPicture),
    segmap: [*c]Dav1dRef = null,
    refmvs: [*c]Dav1dRef = null,
    refpoc: [7]c_uint = @import("std").mem.zeroes([7]c_uint),
};
pub const entry = i16;
pub const generate_grain_y_fn = ?*const fn (buf: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, bitdepth_max: c_int) callconv(.c) void;
pub const generate_grain_uv_fn = ?*const fn (buf: [*c][82]entry, buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize, bitdepth_max: c_int) callconv(.c) void;
pub const fgy_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const fguv_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, luma_row: [*c]const pixel, luma_stride: ptrdiff_t, uv_pl: c_int, is_id: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const struct_Dav1dFilmGrainDSPContext = extern struct {
    generate_grain_y: generate_grain_y_fn = null,
    generate_grain_uv: [3]generate_grain_uv_fn = @import("std").mem.zeroes([3]generate_grain_uv_fn),
    fgy_32x32xn: fgy_32x32xn_fn = null,
    fguv_32x32xn: [3]fguv_32x32xn_fn = @import("std").mem.zeroes([3]fguv_32x32xn_fn),
};
pub const Dav1dFilmGrainDSPContext = struct_Dav1dFilmGrainDSPContext;
pub const angular_ipred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, angle: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const cfl_ac_fn = ?*const fn (ac: [*c]i16, y: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, cw: c_int, ch: c_int) callconv(.c) void;
pub const cfl_pred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const pal_pred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, pal: [*c]const pixel, idx: [*c]const u8, w: c_int, h: c_int) callconv(.c) void;
pub const struct_Dav1dIntraPredDSPContext = extern struct {
    intra_pred: [14]angular_ipred_fn = @import("std").mem.zeroes([14]angular_ipred_fn),
    cfl_ac: [3]cfl_ac_fn = @import("std").mem.zeroes([3]cfl_ac_fn),
    cfl_pred: [6]cfl_pred_fn = @import("std").mem.zeroes([6]cfl_pred_fn),
    pal_pred: pal_pred_fn = null,
};
pub const Dav1dIntraPredDSPContext = struct_Dav1dIntraPredDSPContext;
pub const mc_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mc_scaled_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mct_fn = ?*const fn (tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mct_scaled_fn = ?*const fn (tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const avg_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const w_avg_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, weight: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mask_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, mask: [*c]const u8, bitdepth_max: c_int) callconv(.c) void;
pub const w_mask_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, mask: [*c]u8, sign: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const blend_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp: [*c]const pixel, w: c_int, h: c_int, mask: [*c]const u8) callconv(.c) void;
pub const blend_dir_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp: [*c]const pixel, w: c_int, h: c_int) callconv(.c) void;
pub const warp8x8_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const warp8x8t_fn = ?*const fn (tmp: [*c]i16, tmp_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const emu_edge_fn = ?*const fn (bw: isize, bh: isize, iw: isize, ih: isize, x: isize, y: isize, dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t) callconv(.c) void;
pub const resize_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, dst_w: c_int, h: c_int, src_w: c_int, dx: c_int, mx: c_int, bitdepth_max: c_int) callconv(.c) void;
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
pub const itxfm_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, coeff: [*c]coef, eob: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const struct_Dav1dInvTxfmDSPContext = extern struct {
    itxfm_add: [19][17]itxfm_fn = @import("std").mem.zeroes([19][17]itxfm_fn),
};
pub const Dav1dInvTxfmDSPContext = struct_Dav1dInvTxfmDSPContext;
pub const Av1FilterLUT = common.Av1FilterLUT;
pub const loopfilter_sb_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, mask: [*c]const u32, lvl: [*c][4]u8, lvl_stride: ptrdiff_t, lut: [*c]const Av1FilterLUT, w: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const struct_Dav1dLoopFilterDSPContext = extern struct {
    loop_filter_sb: [2][2]loopfilter_sb_fn = @import("std").mem.zeroes([2][2]loopfilter_sb_fn),
};
pub const Dav1dLoopFilterDSPContext = struct_Dav1dLoopFilterDSPContext;
pub const cdef_dir_fn = ?*const fn (dst: [*c]const pixel, dst_stride: ptrdiff_t, @"var": [*c]c_uint, bitdepth_max: c_int) callconv(.c) c_int;
pub const const_left_pixel_row_2px = [*c][2]pixel;
pub const enum_CdefEdgeFlags = common.enum_CdefEdgeFlags;
pub const cdef_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, left: const_left_pixel_row_2px, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, edges: enum_CdefEdgeFlags, bitdepth_max: c_int) callconv(.c) void;
pub const struct_Dav1dCdefDSPContext = extern struct {
    dir: cdef_dir_fn = null,
    fb: [3]cdef_fn = @import("std").mem.zeroes([3]cdef_fn),
};
pub const Dav1dCdefDSPContext = struct_Dav1dCdefDSPContext;
pub const const_left_pixel_row = [*c][4]pixel;
const struct_unnamed_48 = extern struct {
    s0: u32 = 0,
    s1: u32 = 0,
    w0: i16 = 0,
    w1: i16 = 0,
};
pub const union_LooprestorationParams = extern union {
    filter: [2][8]i16 align(16),
    sgr: struct_unnamed_48 align(16),
};
pub const LooprestorationParams = union_LooprestorationParams;
pub const enum_LrEdgeFlags = common.enum_LrEdgeFlags;
pub const looprestorationfilter_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, left: const_left_pixel_row, lpf: [*c]const pixel, w: c_int, h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags, bitdepth_max: c_int) callconv(.c) void;
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
pub const va_list = __builtin_va_list;
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
    frame_thread: struct_unnamed_46 = @import("std").mem.zeroes(struct_unnamed_46),
    task_thread: struct_TaskThreadData_40 = @import("std").mem.zeroes(struct_TaskThreadData_40),
    segmap_pool: [*c]Dav1dMemPool = null,
    refmvs_pool: [*c]Dav1dMemPool = null,
    refs: [8]struct_unnamed_47 = @import("std").mem.zeroes([8]struct_unnamed_47),
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
const struct_unnamed_51 = extern struct {
    y_mode: u8 = 0,
    uv_mode: u8 = 0,
    tx: u8 = 0,
    pal_sz: [2]u8 = @import("std").mem.zeroes([2]u8),
    y_angle: i8 = 0,
    uv_angle: i8 = 0,
    cfl_alpha: [2]i8 = @import("std").mem.zeroes([2]i8),
};
const struct_unnamed_54 = extern struct {
    mv: [2]union_mv = @import("std").mem.zeroes([2]union_mv),
    wedge_idx: u8 = 0,
    mask_sign: u8 = 0,
    interintra_mode: u8 = 0,
};
const struct_unnamed_55 = extern struct {
    mv2d: union_mv = @import("std").mem.zeroes(union_mv),
    matrix: [4]i16 = @import("std").mem.zeroes([4]i16),
};
const union_unnamed_53 = extern union {
    unnamed_0: struct_unnamed_54,
    unnamed_1: struct_unnamed_55,
};
const struct_unnamed_52 = extern struct {
    unnamed_0: union_unnamed_53 = @import("std").mem.zeroes(union_unnamed_53),
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
const union_unnamed_50 = extern union {
    unnamed_0: struct_unnamed_51,
    unnamed_1: struct_unnamed_52,
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
    unnamed_0: union_unnamed_50 = @import("std").mem.zeroes(union_unnamed_50),
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
const struct_unnamed_49 = extern struct {
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
const struct_unnamed_56 = extern struct {
    next_tile_row: [2]c_int = @import("std").mem.zeroes([2]c_int),
    entropy_progress: atomic_int = 0,
    deblock_progress: atomic_int = 0,
    frame_progress: [*c]atomic_uint = null,
    copy_lpf_progress: [*c]atomic_uint = null,
    b: [*c]Av1Block = null,
    cbi: [*c]i16 = null,
    pal: [*c][3][8]pixel = null,
    pal_idx: [*c]u8 = null,
    cf: [*c]coef = null,
    prog_sz: c_int = 0,
    cbi_sz: c_int = 0,
    pal_sz: c_int = 0,
    pal_idx_sz: c_int = 0,
    cf_sz: c_int = 0,
    tile_start_off: [*c]c_uint = null,
};
pub const Av1Restoration = common.Av1Restoration;
const struct_unnamed_57 = extern struct {
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
const struct_unnamed_59 = extern struct {
    merge: atomic_int = 0,
    lock: afz_pthread_mutex_t = 0,
    head: [*c]Dav1dTask = null,
    tail: [*c]Dav1dTask = null,
};
const struct_unnamed_58 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    ttd: [*c]struct_TaskThreadData_40 = null,
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
    pending_tasks: struct_unnamed_59 = @import("std").mem.zeroes(struct_unnamed_59),
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
    svc: [7][2]struct_ScalableMotionParams_19 = @import("std").mem.zeroes([7][2]struct_ScalableMotionParams_19),
    resize_step: [2]c_int = @import("std").mem.zeroes([2]c_int),
    resize_start: [2]c_int = @import("std").mem.zeroes([2]c_int),
    c: [*c]const Dav1dContext = null,
    ts: [*c]Dav1dTileState = null,
    n_ts: c_int = 0,
    dsp: [*c]const Dav1dDSPContext = null,
    bd_fn: struct_unnamed_49 = @import("std").mem.zeroes(struct_unnamed_49),
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
    frame_thread: struct_unnamed_56 = @import("std").mem.zeroes(struct_unnamed_56),
    lf: struct_unnamed_57 = @import("std").mem.zeroes(struct_unnamed_57),
    task_thread: struct_unnamed_58 = @import("std").mem.zeroes(struct_unnamed_58),
    tile_thread: struct_FrameTileThreadData_45 = @import("std").mem.zeroes(struct_FrameTileThreadData_45),
};
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
pub const LR_RESTORE_Y = common.LR_RESTORE_Y;
pub const LR_RESTORE_U = common.LR_RESTORE_U;
pub const LR_RESTORE_V = common.LR_RESTORE_V;
pub fn backup_lpf(f: [*c]const Dav1dFrameContext, arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_src: [*c]const pixel, src_stride: ptrdiff_t, ss_ver: c_int, sb128: c_int, arg_row: c_int, row_h: c_int, src_w: c_int, h: c_int, ss_hor: c_int, lr_backup: c_int) callconv(.c) void {
    _ = &f;
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    _ = &ss_ver;
    _ = &sb128;
    var row = arg_row;
    _ = &row;
    _ = &row_h;
    _ = &src_w;
    _ = &h;
    _ = &ss_hor;
    _ = &lr_backup;
    const cdef_backup: c_int = @as(c_int, @intFromBool(!(lr_backup != 0)));
    _ = &cdef_backup;
    const dst_w: c_int = if (f.*.frame_hdr.*.super_res.enabled != 0) (f.*.frame_hdr.*.width[@as(c_int, 1)] + ss_hor) >> @intCast(ss_hor) else src_w;
    _ = &dst_w;
    var stripe_h: c_int = ((@as(c_int, 64) << @intCast(cdef_backup & sb128)) - (@as(c_int, 8) * @as(c_int, @intFromBool(!(row != 0))))) >> @intCast(ss_ver);
    _ = &stripe_h;
    src += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, stripe_h - @as(c_int, 2)) * PXSTRIDE(src_stride)))));
    if (f.*.c.*.n_tc == @as(c_uint, 1)) {
        if (row != 0) {
            const top: c_int = @as(c_int, 4) << @intCast(sb128);
            _ = &top;
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, 0))))])), @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, top))))])), @bitCast(@as(c_long, dst_w << @intCast(@as(c_int, 1)))));
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, 1))))])), @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, top + @as(c_int, 1)))))])), @bitCast(@as(c_long, dst_w << @intCast(@as(c_int, 1)))));
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, 2))))])), @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, top + @as(c_int, 2)))))])), @bitCast(@as(c_long, dst_w << @intCast(@as(c_int, 1)))));
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, 3))))])), @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride) * @as(ptrdiff_t, top + @as(c_int, 3)))))])), @bitCast(@as(c_long, dst_w << @intCast(@as(c_int, 1)))));
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * PXSTRIDE(dst_stride)))));
    }
    if ((lr_backup != 0) and (f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)])) {
        while ((row + stripe_h) <= row_h) {
            const n_lines: c_int = @as(c_int, 4) - @as(c_int, @intFromBool(((row + stripe_h) + @as(c_int, 1)) == h));
            _ = &n_lines;
            f.*.dsp.*.mc.resize.?(dst, dst_stride, src, src_stride, dst_w, n_lines, src_w, f.*.resize_step[@bitCast(@as(isize, @intCast(ss_hor)))], f.*.resize_start[@bitCast(@as(isize, @intCast(ss_hor)))], f.*.bitdepth_max);
            row += stripe_h;
            stripe_h = @as(c_int, 64) >> @intCast(ss_ver);
            src += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, stripe_h) * PXSTRIDE(src_stride)))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, n_lines) * PXSTRIDE(dst_stride)))));
            if (n_lines == @as(c_int, 3)) {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(-PXSTRIDE(dst_stride))))])), @bitCast(@as(c_long, dst_w << @intCast(@as(c_int, 1)))));
                dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
            }
        }
    } else {
        while ((row + stripe_h) <= row_h) {
            const n_lines: c_int = @as(c_int, 4) - @as(c_int, @intFromBool(((row + stripe_h) + @as(c_int, 1)) == h));
            _ = &n_lines;
            {
                var i: c_int = 0;
                _ = &i;
                while (i < @as(c_int, 4)) : (i += 1) {
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(if (i == n_lines) @as([*c]const pixel, @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(-PXSTRIDE(dst_stride))))]))) else src)), @bitCast(@as(c_long, src_w << @intCast(@as(c_int, 1)))));
                    dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
                    src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
                }
            }
            row += stripe_h;
            stripe_h = @as(c_int, 64) >> @intCast(ss_ver);
            src += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, stripe_h - @as(c_int, 4)) * PXSTRIDE(src_stride)))));
        }
    }
}
pub export fn antfly_av1_dav1d_copy_lpf_16bpc(f: [*c]Dav1dFrameContext, arg_src: [*c]const [*c]pixel, sby: c_int) void {
    _ = &f;
    var src = arg_src;
    _ = &src;
    _ = &sby;
    const have_tt: c_int = @as(c_int, @intFromBool(f.*.c.*.n_tc > @as(c_uint, 1)));
    _ = &have_tt;
    const resize: c_int = @as(c_int, @intFromBool(f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]));
    _ = &resize;
    const offset: c_int = @as(c_int, 8) * @as(c_int, @intFromBool(!!(sby != 0)));
    _ = &offset;
    const src_stride: [*c]const ptrdiff_t = @ptrCast(@alignCast(&f.*.cur.stride));
    _ = &src_stride;
    const lr_stride: [*c]const ptrdiff_t = @ptrCast(@alignCast(&f.*.sr_cur.p.stride));
    _ = &lr_stride;
    const tt_off: c_int = (have_tt * sby) * (@as(c_int, 4) << @intCast(@as(c_int, f.*.seq_hdr.*.sb128)));
    _ = &tt_off;
    const dst: [3][*c]pixel = [3][*c]pixel{
        f.*.lf.lr_lpf_line[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, tt_off) * PXSTRIDE(lr_stride[@as(c_int, 0)]))))),
        f.*.lf.lr_lpf_line[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, tt_off) * PXSTRIDE(lr_stride[@as(c_int, 1)]))))),
        f.*.lf.lr_lpf_line[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, tt_off) * PXSTRIDE(lr_stride[@as(c_int, 1)]))))),
    };
    _ = &dst;
    const restore_planes: c_int = f.*.lf.restore_planes;
    _ = &restore_planes;
    if ((@as(c_int, f.*.seq_hdr.*.cdef) != 0) or ((restore_planes & LR_RESTORE_Y) != 0)) {
        const h: c_int = f.*.cur.p.h;
        _ = &h;
        const w: c_int = f.*.bw << @intCast(@as(c_int, 2));
        _ = &w;
        const row_h: c_int = imin((sby + @as(c_int, 1)) << @intCast(@as(c_int, 6) + @as(c_int, f.*.seq_hdr.*.sb128)), h - @as(c_int, 1));
        _ = &row_h;
        const y_stripe: c_int = (sby << @intCast(@as(c_int, 6) + @as(c_int, f.*.seq_hdr.*.sb128))) - offset;
        _ = &y_stripe;
        if (((restore_planes & LR_RESTORE_Y) != 0) or !(resize != 0)) {
            backup_lpf(f, dst[@as(c_int, 0)], lr_stride[@as(c_int, 0)], src[@as(c_int, 0)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, offset) * PXSTRIDE(src_stride[@as(c_int, 0)]))))), src_stride[@as(c_int, 0)], 0, f.*.seq_hdr.*.sb128, y_stripe, row_h, w, h, 0, 1);
        }
        if ((have_tt != 0) and (resize != 0)) {
            const cdef_off_y: ptrdiff_t = @as(ptrdiff_t, sby * @as(c_int, 4)) * PXSTRIDE(src_stride[@as(c_int, 0)]);
            _ = &cdef_off_y;
            backup_lpf(f, f.*.lf.cdef_lpf_line[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(cdef_off_y)))), src_stride[@as(c_int, 0)], src[@as(c_int, 0)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, offset) * PXSTRIDE(src_stride[@as(c_int, 0)]))))), src_stride[@as(c_int, 0)], 0, f.*.seq_hdr.*.sb128, y_stripe, row_h, w, h, 0, 0);
        }
    }
    if (((@as(c_int, f.*.seq_hdr.*.cdef) != 0) or ((restore_planes & (LR_RESTORE_U | LR_RESTORE_V)) != 0)) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400))) {
        const ss_ver: c_int = @as(c_int, @intFromBool(f.*.sr_cur.p.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
        _ = &ss_ver;
        const ss_hor: c_int = @as(c_int, @intFromBool(f.*.sr_cur.p.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
        _ = &ss_hor;
        const h: c_int = (f.*.cur.p.h + ss_ver) >> @intCast(ss_ver);
        _ = &h;
        const w: c_int = f.*.bw << @intCast(@as(c_int, 2) - ss_hor);
        _ = &w;
        const row_h: c_int = imin((sby + @as(c_int, 1)) << @intCast((@as(c_int, 6) - ss_ver) + @as(c_int, f.*.seq_hdr.*.sb128)), h - @as(c_int, 1));
        _ = &row_h;
        const offset_uv: c_int = offset >> @intCast(ss_ver);
        _ = &offset_uv;
        const y_stripe: c_int = (sby << @intCast((@as(c_int, 6) - ss_ver) + @as(c_int, f.*.seq_hdr.*.sb128))) - offset_uv;
        _ = &y_stripe;
        const cdef_off_uv: ptrdiff_t = @as(ptrdiff_t, sby * @as(c_int, 4)) * PXSTRIDE(src_stride[@as(c_int, 1)]);
        _ = &cdef_off_uv;
        if ((@as(c_int, f.*.seq_hdr.*.cdef) != 0) or ((restore_planes & LR_RESTORE_U) != 0)) {
            if (((restore_planes & LR_RESTORE_U) != 0) or !(resize != 0)) {
                backup_lpf(f, dst[@as(c_int, 1)], lr_stride[@as(c_int, 1)], src[@as(c_int, 1)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, offset_uv) * PXSTRIDE(src_stride[@as(c_int, 1)]))))), src_stride[@as(c_int, 1)], ss_ver, f.*.seq_hdr.*.sb128, y_stripe, row_h, w, h, ss_hor, 1);
            }
            if ((have_tt != 0) and (resize != 0)) {
                backup_lpf(f, f.*.lf.cdef_lpf_line[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast(cdef_off_uv)))), src_stride[@as(c_int, 1)], src[@as(c_int, 1)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, offset_uv) * PXSTRIDE(src_stride[@as(c_int, 1)]))))), src_stride[@as(c_int, 1)], ss_ver, f.*.seq_hdr.*.sb128, y_stripe, row_h, w, h, ss_hor, 0);
            }
        }
        if ((@as(c_int, f.*.seq_hdr.*.cdef) != 0) or ((restore_planes & LR_RESTORE_V) != 0)) {
            if (((restore_planes & LR_RESTORE_V) != 0) or !(resize != 0)) {
                backup_lpf(f, dst[@as(c_int, 2)], lr_stride[@as(c_int, 1)], src[@as(c_int, 2)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, offset_uv) * PXSTRIDE(src_stride[@as(c_int, 1)]))))), src_stride[@as(c_int, 1)], ss_ver, f.*.seq_hdr.*.sb128, y_stripe, row_h, w, h, ss_hor, 1);
            }
            if ((have_tt != 0) and (resize != 0)) {
                backup_lpf(f, f.*.lf.cdef_lpf_line[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast(cdef_off_uv)))), src_stride[@as(c_int, 1)], src[@as(c_int, 2)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, offset_uv) * PXSTRIDE(src_stride[@as(c_int, 1)]))))), src_stride[@as(c_int, 1)], ss_ver, f.*.seq_hdr.*.sb128, y_stripe, row_h, w, h, ss_hor, 0);
            }
        }
    }
}
pub fn filter_plane_cols_y(f: [*c]const Dav1dFrameContext, have_left: c_int, arg_lvl: [*c][4]u8, b4_stride: ptrdiff_t, mask: [*c][3][2]u16, arg_dst: [*c]pixel, ls: ptrdiff_t, w: c_int, starty4: c_int, endy4: c_int) callconv(.c) void {
    _ = &f;
    _ = &have_left;
    var lvl = arg_lvl;
    _ = &lvl;
    _ = &b4_stride;
    _ = &mask;
    var dst = arg_dst;
    _ = &dst;
    _ = &ls;
    _ = &w;
    _ = &starty4;
    _ = &endy4;
    const dsp: [*c]const Dav1dDSPContext = f.*.dsp;
    _ = &dsp;
    {
        var x: c_int = 0;
        _ = &x;
        while (x < w) : (x += 1) {
            if (!(have_left != 0) and !(x != 0)) continue;
            var hmask: [4]u32 = undefined;
            _ = &hmask;
            if (!(starty4 != 0)) {
                hmask[@as(c_int, 0)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)][@as(c_int, 0)];
                hmask[@as(c_int, 1)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)][@as(c_int, 0)];
                hmask[@as(c_int, 2)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 2)][@as(c_int, 0)];
                if (endy4 > @as(c_int, 16)) {
                    hmask[@as(c_int, 0)] |= @as(c_uint, mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)][@as(c_int, 1)]) << @intCast(@as(c_uint, 16));
                    hmask[@as(c_int, 1)] |= @as(c_uint, mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)][@as(c_int, 1)]) << @intCast(@as(c_uint, 16));
                    hmask[@as(c_int, 2)] |= @as(c_uint, mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 2)][@as(c_int, 1)]) << @intCast(@as(c_uint, 16));
                }
            } else {
                hmask[@as(c_int, 0)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)][@as(c_int, 1)];
                hmask[@as(c_int, 1)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)][@as(c_int, 1)];
                hmask[@as(c_int, 2)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 2)][@as(c_int, 1)];
            }
            hmask[@as(c_int, 3)] = 0;
            dsp.*.lf.loop_filter_sb[@as(c_int, 0)][@as(c_int, 0)].?(&dst[@bitCast(@as(isize, @intCast(x * @as(c_int, 4))))], ls, @ptrCast(@alignCast(&hmask)), @ptrCast(@alignCast(@constCast(&lvl[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]))), b4_stride, &f.*.lf.lim_lut, endy4 - starty4, f.*.bitdepth_max);
        }
    }
}
pub fn filter_plane_rows_y(f: [*c]const Dav1dFrameContext, have_top: c_int, arg_lvl: [*c][4]u8, b4_stride: ptrdiff_t, mask: [*c][3][2]u16, arg_dst: [*c]pixel, ls: ptrdiff_t, w: c_int, starty4: c_int, endy4: c_int) callconv(.c) void {
    _ = &f;
    _ = &have_top;
    var lvl = arg_lvl;
    _ = &lvl;
    _ = &b4_stride;
    _ = &mask;
    var dst = arg_dst;
    _ = &dst;
    _ = &ls;
    _ = &w;
    _ = &starty4;
    _ = &endy4;
    const dsp: [*c]const Dav1dDSPContext = f.*.dsp;
    _ = &dsp;
    {
        var y: c_int = starty4;
        _ = &y;
        while (y < endy4) : ({
            y += 1;
            dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * PXSTRIDE(ls)))));
            lvl += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
        }) {
            if (!(have_top != 0) and !(y != 0)) continue;
            const vmask: [4]u32 = [4]u32{
                @as(c_uint, @bitCast(@as(c_int, @as(c_int, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)][@as(c_int, 0)])))) | (@as(c_uint, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)][@as(c_int, 1)]) << @intCast(@as(c_uint, 16))),
                @as(c_uint, @bitCast(@as(c_int, @as(c_int, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)][@as(c_int, 0)])))) | (@as(c_uint, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)][@as(c_int, 1)]) << @intCast(@as(c_uint, 16))),
                @as(c_uint, @bitCast(@as(c_int, @as(c_int, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 2)][@as(c_int, 0)])))) | (@as(c_uint, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 2)][@as(c_int, 1)]) << @intCast(@as(c_uint, 16))),
                0,
            };
            _ = &vmask;
            dsp.*.lf.loop_filter_sb[@as(c_int, 0)][@as(c_int, 1)].?(dst, ls, @ptrCast(@alignCast(&vmask)), @ptrCast(@alignCast(@constCast(&lvl[@as(c_int, 0)][@as(c_int, 1)]))), b4_stride, &f.*.lf.lim_lut, w, f.*.bitdepth_max);
        }
    }
}
pub fn filter_plane_cols_uv(f: [*c]const Dav1dFrameContext, have_left: c_int, arg_lvl: [*c][4]u8, b4_stride: ptrdiff_t, mask: [*c][2][2]u16, u: [*c]pixel, v: [*c]pixel, ls: ptrdiff_t, w: c_int, starty4: c_int, endy4: c_int, ss_ver: c_int) callconv(.c) void {
    _ = &f;
    _ = &have_left;
    var lvl = arg_lvl;
    _ = &lvl;
    _ = &b4_stride;
    _ = &mask;
    _ = &u;
    _ = &v;
    _ = &ls;
    _ = &w;
    _ = &starty4;
    _ = &endy4;
    _ = &ss_ver;
    const dsp: [*c]const Dav1dDSPContext = f.*.dsp;
    _ = &dsp;
    {
        var x: c_int = 0;
        _ = &x;
        while (x < w) : (x += 1) {
            if (!(have_left != 0) and !(x != 0)) continue;
            var hmask: [3]u32 = undefined;
            _ = &hmask;
            if (!(starty4 != 0)) {
                hmask[@as(c_int, 0)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)][@as(c_int, 0)];
                hmask[@as(c_int, 1)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)][@as(c_int, 0)];
                if (endy4 > (@as(c_int, 16) >> @intCast(ss_ver))) {
                    hmask[@as(c_int, 0)] |= @as(c_uint, mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)][@as(c_int, 1)]) << @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 16) >> @intCast(ss_ver)))));
                    hmask[@as(c_int, 1)] |= @as(c_uint, mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)][@as(c_int, 1)]) << @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 16) >> @intCast(ss_ver)))));
                }
            } else {
                hmask[@as(c_int, 0)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)][@as(c_int, 1)];
                hmask[@as(c_int, 1)] = mask[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)][@as(c_int, 1)];
            }
            hmask[@as(c_int, 2)] = 0;
            dsp.*.lf.loop_filter_sb[@as(c_int, 1)][@as(c_int, 0)].?(&u[@bitCast(@as(isize, @intCast(x * @as(c_int, 4))))], ls, @ptrCast(@alignCast(&hmask)), @ptrCast(@alignCast(@constCast(&lvl[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 2)]))), b4_stride, &f.*.lf.lim_lut, endy4 - starty4, f.*.bitdepth_max);
            dsp.*.lf.loop_filter_sb[@as(c_int, 1)][@as(c_int, 0)].?(&v[@bitCast(@as(isize, @intCast(x * @as(c_int, 4))))], ls, @ptrCast(@alignCast(&hmask)), @ptrCast(@alignCast(@constCast(&lvl[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 3)]))), b4_stride, &f.*.lf.lim_lut, endy4 - starty4, f.*.bitdepth_max);
        }
    }
}
pub fn filter_plane_rows_uv(f: [*c]const Dav1dFrameContext, have_top: c_int, arg_lvl: [*c][4]u8, b4_stride: ptrdiff_t, mask: [*c][2][2]u16, u: [*c]pixel, v: [*c]pixel, ls: ptrdiff_t, w: c_int, starty4: c_int, endy4: c_int, ss_hor: c_int) callconv(.c) void {
    _ = &f;
    _ = &have_top;
    var lvl = arg_lvl;
    _ = &lvl;
    _ = &b4_stride;
    _ = &mask;
    _ = &u;
    _ = &v;
    _ = &ls;
    _ = &w;
    _ = &starty4;
    _ = &endy4;
    _ = &ss_hor;
    const dsp: [*c]const Dav1dDSPContext = f.*.dsp;
    _ = &dsp;
    var off_l: ptrdiff_t = 0;
    _ = &off_l;
    {
        var y: c_int = starty4;
        _ = &y;
        while (y < endy4) : ({
            y += 1;
            off_l += @as(ptrdiff_t, 4) * PXSTRIDE(ls);
            lvl += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
        }) {
            if (!(have_top != 0) and !(y != 0)) continue;
            const vmask: [3]u32 = [3]u32{
                @as(c_uint, @bitCast(@as(c_int, @as(c_int, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)][@as(c_int, 0)])))) | (@as(c_uint, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)][@as(c_int, 1)]) << @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 16) >> @intCast(ss_hor)))))),
                @as(c_uint, @bitCast(@as(c_int, @as(c_int, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)][@as(c_int, 0)])))) | (@as(c_uint, mask[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)][@as(c_int, 1)]) << @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 16) >> @intCast(ss_hor)))))),
                0,
            };
            _ = &vmask;
            dsp.*.lf.loop_filter_sb[@as(c_int, 1)][@as(c_int, 1)].?(&u[@bitCast(@as(isize, @intCast(off_l)))], ls, @ptrCast(@alignCast(&vmask)), @ptrCast(@alignCast(@constCast(&lvl[@as(c_int, 0)][@as(c_int, 2)]))), b4_stride, &f.*.lf.lim_lut, w, f.*.bitdepth_max);
            dsp.*.lf.loop_filter_sb[@as(c_int, 1)][@as(c_int, 1)].?(&v[@bitCast(@as(isize, @intCast(off_l)))], ls, @ptrCast(@alignCast(&vmask)), @ptrCast(@alignCast(@constCast(&lvl[@as(c_int, 0)][@as(c_int, 3)]))), b4_stride, &f.*.lf.lim_lut, w, f.*.bitdepth_max);
        }
    }
}
pub export fn antfly_av1_dav1d_loopfilter_sbrow_cols_16bpc(f: [*c]const Dav1dFrameContext, arg_p: [*c]const [*c]pixel, lflvl: [*c]Av1Filter, arg_sby: c_int, start_of_tile_row: c_int) void {
    _ = &f;
    var p = arg_p;
    _ = &p;
    _ = &lflvl;
    var sby = arg_sby;
    _ = &sby;
    _ = &start_of_tile_row;
    var x: c_int = undefined;
    _ = &x;
    var have_left: c_int = undefined;
    _ = &have_left;
    const is_sb64: c_int = @as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0)));
    _ = &is_sb64;
    const starty4: c_int = (sby & is_sb64) << @intCast(@as(c_int, 4));
    _ = &starty4;
    const sbsz: c_int = @as(c_int, 32) >> @intCast(is_sb64);
    _ = &sbsz;
    const sbl2: c_int = @as(c_int, 5) - is_sb64;
    _ = &sbl2;
    const halign: c_int = (f.*.bh + @as(c_int, 31)) & ~@as(c_int, 31);
    _ = &halign;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_hor;
    const vmask: c_int = @as(c_int, 16) >> @intCast(ss_ver);
    _ = &vmask;
    const hmask: c_int = @as(c_int, 16) >> @intCast(ss_hor);
    _ = &hmask;
    const vmax: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, vmask))));
    _ = &vmax;
    const hmax: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, hmask))));
    _ = &hmax;
    const endy4: c_uint = @bitCast(@as(c_int, starty4 + imin(f.*.h4 - (sby * sbsz), sbsz)));
    _ = &endy4;
    const uv_endy4: c_uint = (endy4 +% @as(c_uint, @bitCast(@as(c_int, ss_ver)))) >> @intCast(@as(c_uint, @bitCast(@as(c_int, ss_ver))));
    _ = &uv_endy4;
    var lpf_y: [*c]const u8 = &f.*.lf.tx_lpf_right_edge[@as(c_int, 0)][@bitCast(@as(isize, @intCast(sby << @intCast(sbl2))))];
    _ = &lpf_y;
    var lpf_uv: [*c]const u8 = &f.*.lf.tx_lpf_right_edge[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sby << @intCast(sbl2 - ss_ver))))];
    _ = &lpf_uv;
    {
        var tile_col: c_int = 1;
        _ = &tile_col;
        while (true) : (tile_col += 1) {
            x = f.*.frame_hdr.*.tiling.col_start_sb[@bitCast(@as(isize, @intCast(tile_col)))];
            if ((x << @intCast(sbl2)) >= f.*.bw) break;
            const bx4: c_int = if ((x & is_sb64) != 0) @as(c_int, 16) else @as(c_int, 0);
            _ = &bx4;
            const cbx4: c_int = bx4 >> @intCast(ss_hor);
            _ = &cbx4;
            x >>= @intCast(is_sb64);
            const y_hmask: [*c][2]u16 = @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_y[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4)))]));
            _ = &y_hmask;
            {
                var y: c_uint = @bitCast(@as(c_int, starty4));
                _ = &y;
                var mask: c_uint = @as(c_uint, 1) << @intCast(y);
                _ = &mask;
                while (y < endy4) : ({
                    y +%= 1;
                    mask <<= @intCast(@as(c_uint, 1));
                }) {
                    const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
                    _ = &sidx;
                    const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
                    _ = &smask;
                    const idx: c_int = (@as(c_int, 2) * @as(c_int, @intFromBool(!!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, y_hmask[@as(c_int, 2)][@bitCast(@as(isize, @intCast(sidx)))])))) & smask) != 0)))) + @as(c_int, @intFromBool(!!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, y_hmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))])))) & smask) != 0)));
                    _ = &idx;
                    {
                        const ref = &y_hmask[@as(c_int, 2)][@bitCast(@as(isize, @intCast(sidx)))];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                    }
                    {
                        const ref = &y_hmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                    }
                    {
                        const ref = &y_hmask[@as(c_int, 0)][@bitCast(@as(isize, @intCast(sidx)))];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                    }
                    {
                        const ref = &y_hmask[@bitCast(@as(isize, @intCast(imin(idx, lpf_y[y -% @as(c_uint, @bitCast(@as(c_int, starty4)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
                    }
                }
            }
            if (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) {
                const uv_hmask: [*c][2]u16 = @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_uv[@as(c_int, 0)][@bitCast(@as(isize, @intCast(cbx4)))]));
                _ = &uv_hmask;
                {
                    var y: c_uint = @bitCast(@as(c_int, starty4 >> @intCast(ss_ver)));
                    _ = &y;
                    var uv_mask: c_uint = @as(c_uint, 1) << @intCast(y);
                    _ = &uv_mask;
                    while (y < uv_endy4) : ({
                        y +%= 1;
                        uv_mask <<= @intCast(@as(c_uint, 1));
                    }) {
                        const sidx: c_int = @as(c_int, @intFromBool(uv_mask >= vmax));
                        _ = &sidx;
                        const smask: c_uint = uv_mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4) - ss_ver)))));
                        _ = &smask;
                        const idx: c_int = @as(c_int, @intFromBool(!!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, uv_hmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))])))) & smask) != 0)));
                        _ = &idx;
                        {
                            const ref = &uv_hmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                        }
                        {
                            const ref = &uv_hmask[@as(c_int, 0)][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                        }
                        {
                            const ref = &uv_hmask[@bitCast(@as(isize, @intCast(imin(idx, lpf_uv[y -% @as(c_uint, @bitCast(@as(c_int, starty4 >> @intCast(ss_ver))))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
                        }
                    }
                }
            }
            lpf_y += @as(usize, @bitCast(@as(isize, @intCast(halign))));
            lpf_uv += @as(usize, @bitCast(@as(isize, @intCast(halign >> @intCast(ss_ver)))));
        }
    }
    if (start_of_tile_row != 0) {
        var a: [*c]const BlockContext = undefined;
        _ = &a;
        {
            x = 0;
            a = &f.*.a[@bitCast(@as(isize, @intCast(f.*.sb128w * (start_of_tile_row - @as(c_int, 1)))))];
            while (x < f.*.sb128w) : ({
                x += 1;
                a += 1;
            }) {
                const y_vmask: [*c][2]u16 = @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_y[@as(c_int, 1)][@bitCast(@as(isize, @intCast(starty4)))]));
                _ = &y_vmask;
                const w: c_uint = @bitCast(@as(c_int, imin(32, f.*.w4 - (x << @intCast(@as(c_int, 5))))));
                _ = &w;
                {
                    var mask: c_uint = 1;
                    _ = &mask;
                    var i: c_uint = 0;
                    _ = &i;
                    while (i < w) : ({
                        mask <<= @intCast(@as(c_uint, 1));
                        i +%= 1;
                    }) {
                        const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
                        _ = &sidx;
                        const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
                        _ = &smask;
                        const idx: c_int = (@as(c_int, 2) * @as(c_int, @intFromBool(!!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, y_vmask[@as(c_int, 2)][@bitCast(@as(isize, @intCast(sidx)))])))) & smask) != 0)))) + @as(c_int, @intFromBool(!!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, y_vmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))])))) & smask) != 0)));
                        _ = &idx;
                        {
                            const ref = &y_vmask[@as(c_int, 2)][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                        }
                        {
                            const ref = &y_vmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                        }
                        {
                            const ref = &y_vmask[@as(c_int, 0)][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                        }
                        {
                            const ref = &y_vmask[@bitCast(@as(isize, @intCast(imin(idx, a.*.tx_lpf_y[i]))))][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
                        }
                    }
                }
                if (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) {
                    const cw: c_uint = (w +% @as(c_uint, @bitCast(@as(c_int, ss_hor)))) >> @intCast(@as(c_uint, @bitCast(@as(c_int, ss_hor))));
                    _ = &cw;
                    const uv_vmask: [*c][2]u16 = @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_uv[@as(c_int, 1)][@bitCast(@as(isize, @intCast(starty4 >> @intCast(ss_ver))))]));
                    _ = &uv_vmask;
                    {
                        var uv_mask: c_uint = 1;
                        _ = &uv_mask;
                        var i: c_uint = 0;
                        _ = &i;
                        while (i < cw) : ({
                            uv_mask <<= @intCast(@as(c_uint, 1));
                            i +%= 1;
                        }) {
                            const sidx: c_int = @as(c_int, @intFromBool(uv_mask >= hmax));
                            _ = &sidx;
                            const smask: c_uint = uv_mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4) - ss_hor)))));
                            _ = &smask;
                            const idx: c_int = @as(c_int, @intFromBool(!!((@as(c_uint, @bitCast(@as(c_int, @as(c_int, uv_vmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))])))) & smask) != 0)));
                            _ = &idx;
                            {
                                const ref = &uv_vmask[@as(c_int, 1)][@bitCast(@as(isize, @intCast(sidx)))];
                                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                            }
                            {
                                const ref = &uv_vmask[@as(c_int, 0)][@bitCast(@as(isize, @intCast(sidx)))];
                                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) & ~smask);
                            }
                            {
                                const ref = &uv_vmask[@bitCast(@as(isize, @intCast(imin(idx, a.*.tx_lpf_uv[i]))))][@bitCast(@as(isize, @intCast(sidx)))];
                                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
                            }
                        }
                    }
                }
            }
        }
    }
    var ptr: [*c]pixel = undefined;
    _ = &ptr;
    var level_ptr: [*c][4]u8 = f.*.lf.level + @as(usize, @bitCast(@as(isize, @intCast((f.*.b4_stride * @as(ptrdiff_t, sby)) * @as(ptrdiff_t, sbsz)))));
    _ = &level_ptr;
    {
        ptr = p[@as(c_int, 0)];
        have_left = 0;
        x = 0;
        while (x < f.*.sb128w) : ({
            x += 1;
            have_left = 1;
            ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
            level_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
        }) {
            filter_plane_cols_y(f, have_left, level_ptr, f.*.b4_stride, @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_y[@as(c_int, 0)])), ptr, f.*.cur.stride[@as(c_int, 0)], imin(32, f.*.w4 - (x * @as(c_int, 32))), starty4, @bitCast(@as(c_uint, @truncate(endy4))));
        }
    }
    if (!(@as(c_int, f.*.frame_hdr.*.loopfilter.level_u) != 0) and !(@as(c_int, f.*.frame_hdr.*.loopfilter.level_v) != 0)) return;
    var uv_off: ptrdiff_t = undefined;
    _ = &uv_off;
    level_ptr = f.*.lf.level + @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride * @as(ptrdiff_t, (sby * sbsz) >> @intCast(ss_ver))))));
    {
        uv_off = 0;
        have_left = 0;
        x = 0;
        while (x < f.*.sb128w) : ({
            x += 1;
            have_left = 1;
            uv_off += @as(c_int, 128) >> @intCast(ss_hor);
            level_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32) >> @intCast(ss_hor)))));
        }) {
            filter_plane_cols_uv(f, have_left, level_ptr, f.*.b4_stride, @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_uv[@as(c_int, 0)])), &p[@as(c_int, 1)][@bitCast(@as(isize, @intCast(uv_off)))], &p[@as(c_int, 2)][@bitCast(@as(isize, @intCast(uv_off)))], f.*.cur.stride[@as(c_int, 1)], (imin(32, f.*.w4 - (x * @as(c_int, 32))) + ss_hor) >> @intCast(ss_hor), starty4 >> @intCast(ss_ver), @bitCast(@as(c_uint, @truncate(uv_endy4))), ss_ver);
        }
    }
}
pub export fn antfly_av1_dav1d_loopfilter_sbrow_rows_16bpc(f: [*c]const Dav1dFrameContext, arg_p: [*c]const [*c]pixel, lflvl: [*c]Av1Filter, arg_sby: c_int) void {
    _ = &f;
    var p = arg_p;
    _ = &p;
    _ = &lflvl;
    var sby = arg_sby;
    _ = &sby;
    var x: c_int = undefined;
    _ = &x;
    const have_top: c_int = @as(c_int, @intFromBool(sby > @as(c_int, 0)));
    _ = &have_top;
    const is_sb64: c_int = @as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0)));
    _ = &is_sb64;
    const starty4: c_int = (sby & is_sb64) << @intCast(@as(c_int, 4));
    _ = &starty4;
    const sbsz: c_int = @as(c_int, 32) >> @intCast(is_sb64);
    _ = &sbsz;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_hor;
    const endy4: c_uint = @bitCast(@as(c_int, starty4 + imin(f.*.h4 - (sby * sbsz), sbsz)));
    _ = &endy4;
    const uv_endy4: c_uint = (endy4 +% @as(c_uint, @bitCast(@as(c_int, ss_ver)))) >> @intCast(@as(c_uint, @bitCast(@as(c_int, ss_ver))));
    _ = &uv_endy4;
    var ptr: [*c]pixel = undefined;
    _ = &ptr;
    var level_ptr: [*c][4]u8 = f.*.lf.level + @as(usize, @bitCast(@as(isize, @intCast((f.*.b4_stride * @as(ptrdiff_t, sby)) * @as(ptrdiff_t, sbsz)))));
    _ = &level_ptr;
    {
        ptr = p[@as(c_int, 0)];
        x = 0;
        while (x < f.*.sb128w) : ({
            x += 1;
            ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
            level_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
        }) {
            filter_plane_rows_y(f, have_top, level_ptr, f.*.b4_stride, @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_y[@as(c_int, 1)])), ptr, f.*.cur.stride[@as(c_int, 0)], imin(32, f.*.w4 - (x * @as(c_int, 32))), starty4, @bitCast(@as(c_uint, @truncate(endy4))));
        }
    }
    if (!(@as(c_int, f.*.frame_hdr.*.loopfilter.level_u) != 0) and !(@as(c_int, f.*.frame_hdr.*.loopfilter.level_v) != 0)) return;
    var uv_off: ptrdiff_t = undefined;
    _ = &uv_off;
    level_ptr = f.*.lf.level + @as(usize, @bitCast(@as(isize, @intCast(f.*.b4_stride * @as(ptrdiff_t, (sby * sbsz) >> @intCast(ss_ver))))));
    {
        uv_off = 0;
        x = 0;
        while (x < f.*.sb128w) : ({
            x += 1;
            uv_off += @as(c_int, 128) >> @intCast(ss_hor);
            level_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32) >> @intCast(ss_hor)))));
        }) {
            filter_plane_rows_uv(f, have_top, level_ptr, f.*.b4_stride, @ptrCast(@alignCast(&lflvl[@bitCast(@as(isize, @intCast(x)))].filter_uv[@as(c_int, 1)])), &p[@as(c_int, 1)][@bitCast(@as(isize, @intCast(uv_off)))], &p[@as(c_int, 2)][@bitCast(@as(isize, @intCast(uv_off)))], f.*.cur.stride[@as(c_int, 1)], (imin(32, f.*.w4 - (x * @as(c_int, 32))) + ss_hor) >> @intCast(ss_hor), starty4 >> @intCast(ss_ver), @bitCast(@as(c_uint, @truncate(uv_endy4))), ss_hor);
        }
    }
}
