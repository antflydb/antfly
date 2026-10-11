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
pub extern fn antfly_codec_afz_trap() void;
pub const struct___va_list_1 = common.struct___va_list_1;
pub const __builtin_va_list = struct___va_list_1;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const va_list = __builtin_va_list;
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
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
pub const enum_Dav1dFilterMode = common.enum_Dav1dFilterMode;
pub const enum_Dav1dAdaptiveBoolean = common.enum_Dav1dAdaptiveBoolean;
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
pub const enum_Dav1dWarpedMotionType = common.enum_Dav1dWarpedMotionType;
const struct_unnamed_3 = extern struct {
    alpha: i16 = 0,
    beta: i16 = 0,
    gamma: i16 = 0,
    delta: i16 = 0,
};
const union_unnamed_2 = extern union {
    p: struct_unnamed_3,
    abcd: [4]i16,
};
pub const struct_Dav1dWarpedMotionParams = extern struct {
    type: enum_Dav1dWarpedMotionType = @import("std").mem.zeroes(enum_Dav1dWarpedMotionType),
    matrix: [6]i32 = @import("std").mem.zeroes([6]i32),
    u: union_unnamed_2 = @import("std").mem.zeroes(union_unnamed_2),
};
pub const Dav1dWarpedMotionParams = struct_Dav1dWarpedMotionParams;
pub const enum_Dav1dPixelLayout = common.enum_Dav1dPixelLayout;
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const enum_Dav1dColorPrimaries = common.enum_Dav1dColorPrimaries;
pub const enum_Dav1dTransferCharacteristics = common.enum_Dav1dTransferCharacteristics;
pub const enum_Dav1dMatrixCoefficients = common.enum_Dav1dMatrixCoefficients;
pub const enum_Dav1dChromaSamplePosition = common.enum_Dav1dChromaSamplePosition;
pub const Dav1dContentLightLevel = common.Dav1dContentLightLevel;
pub const Dav1dMasteringDisplay = common.Dav1dMasteringDisplay;
pub const Dav1dITUTT35 = common.Dav1dITUTT35;
pub const struct_Dav1dSequenceHeaderOperatingPoint_4 = common.struct_Dav1dSequenceHeaderOperatingPoint_4;
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_5 = common.struct_Dav1dSequenceHeaderOperatingParameterInfo_5;
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
    operating_points: [32]struct_Dav1dSequenceHeaderOperatingPoint_4 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingPoint_4),
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
    operating_parameter_info: [32]struct_Dav1dSequenceHeaderOperatingParameterInfo_5 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingParameterInfo_5),
};
pub const Dav1dSequenceHeader = struct_Dav1dSequenceHeader;
pub const Dav1dSegmentationDataSet = common.Dav1dSegmentationDataSet;
pub const Dav1dLoopfilterModeRefDeltas = common.Dav1dLoopfilterModeRefDeltas;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
const struct_unnamed_6 = extern struct {
    data: Dav1dFilmGrainData = @import("std").mem.zeroes(Dav1dFilmGrainData),
    present: u8 = 0,
    update: u8 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_7 = common.struct_Dav1dFrameHeaderOperatingPoint_7;
const struct_unnamed_8 = extern struct {
    width_scale_denominator: u8 = 0,
    enabled: u8 = 0,
};
const struct_unnamed_9 = extern struct {
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
const struct_unnamed_10 = extern struct {
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
const struct_unnamed_11 = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    temporal: u8 = 0,
    update_data: u8 = 0,
    seg_data: Dav1dSegmentationDataSet = @import("std").mem.zeroes(Dav1dSegmentationDataSet),
    lossless: [8]u8 = @import("std").mem.zeroes([8]u8),
    qidx: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_13 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
};
const struct_unnamed_14 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
    multi: u8 = 0,
};
const struct_unnamed_12 = extern struct {
    q: struct_unnamed_13 = @import("std").mem.zeroes(struct_unnamed_13),
    lf: struct_unnamed_14 = @import("std").mem.zeroes(struct_unnamed_14),
};
const struct_unnamed_15 = extern struct {
    level_y: [2]u8 = @import("std").mem.zeroes([2]u8),
    level_u: u8 = 0,
    level_v: u8 = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    mode_ref_deltas: Dav1dLoopfilterModeRefDeltas = @import("std").mem.zeroes(Dav1dLoopfilterModeRefDeltas),
    sharpness: u8 = 0,
};
const struct_unnamed_16 = extern struct {
    damping: u8 = 0,
    n_bits: u8 = 0,
    y_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
    uv_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_17 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const struct_Dav1dFrameHeader = extern struct {
    film_grain: struct_unnamed_6 = @import("std").mem.zeroes(struct_unnamed_6),
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
    operating_points: [32]struct_Dav1dFrameHeaderOperatingPoint_7 = @import("std").mem.zeroes([32]struct_Dav1dFrameHeaderOperatingPoint_7),
    refresh_frame_flags: u8 = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    super_res: struct_unnamed_8 = @import("std").mem.zeroes(struct_unnamed_8),
    have_render_size: u8 = 0,
    allow_intrabc: u8 = 0,
    frame_ref_short_signaling: u8 = 0,
    refidx: [7]i8 = @import("std").mem.zeroes([7]i8),
    hp: u8 = 0,
    subpel_filter_mode: enum_Dav1dFilterMode = @import("std").mem.zeroes(enum_Dav1dFilterMode),
    switchable_motion_mode: u8 = 0,
    use_ref_frame_mvs: u8 = 0,
    refresh_context: u8 = 0,
    tiling: struct_unnamed_9 = @import("std").mem.zeroes(struct_unnamed_9),
    quant: struct_unnamed_10 = @import("std").mem.zeroes(struct_unnamed_10),
    segmentation: struct_unnamed_11 = @import("std").mem.zeroes(struct_unnamed_11),
    delta: struct_unnamed_12 = @import("std").mem.zeroes(struct_unnamed_12),
    all_lossless: u8 = 0,
    loopfilter: struct_unnamed_15 = @import("std").mem.zeroes(struct_unnamed_15),
    cdef: struct_unnamed_16 = @import("std").mem.zeroes(struct_unnamed_16),
    restoration: struct_unnamed_17 = @import("std").mem.zeroes(struct_unnamed_17),
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
pub const struct_Dav1dPicAllocator = extern struct {
    cookie: ?*anyopaque = null,
    alloc_picture_callback: ?*const fn (pic: [*c]Dav1dPicture, cookie: ?*anyopaque) callconv(.c) c_int = null,
    release_picture_callback: ?*const fn (pic: [*c]Dav1dPicture, cookie: ?*anyopaque) callconv(.c) void = null,
};
pub const Dav1dPicAllocator = struct_Dav1dPicAllocator;
pub const struct_Dav1dData = extern struct {
    data: [*c]const u8 = null,
    sz: usize = 0,
    ref: [*c]struct_Dav1dRef = null,
    m: Dav1dDataProps = @import("std").mem.zeroes(Dav1dDataProps),
};
pub const Dav1dData = struct_Dav1dData;
pub const Dav1dRef = struct_Dav1dRef;
pub const PICTURE_FLAG_NEW_TEMPORAL_UNIT = common.PICTURE_FLAG_NEW_TEMPORAL_UNIT;
pub const enum_PictureFlags = common.enum_PictureFlags;
pub const atomic_uint = common.atomic_uint;
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
pub const Dav1dContext = struct_Dav1dContext;
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
const struct_unnamed_24 = extern struct {
    s0: u32 = 0,
    s1: u32 = 0,
    w0: i16 = 0,
    w1: i16 = 0,
};
pub const union_LooprestorationParams = extern union {
    filter: [2][8]i16 align(16),
    sgr: struct_unnamed_24 align(16),
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
const struct_unnamed_26 = extern struct {
    start: c_int = 0,
    end: c_int = 0,
};
pub const struct_refmvs_tile = extern struct {
    rf: [*c]const refmvs_frame = null,
    r: [37][*c]refmvs_block = @import("std").mem.zeroes([37][*c]refmvs_block),
    rp_proj: [*c]refmvs_temporal_block = null,
    tile_col: struct_unnamed_26 = @import("std").mem.zeroes(struct_unnamed_26),
    tile_row: struct_unnamed_26 = @import("std").mem.zeroes(struct_unnamed_26),
};
pub const refmvs_tile = struct_refmvs_tile;
const union_unnamed_27 = extern union {
    cf_8bpc: [1024]i16 align(64),
    cf_16bpc: [1024]i32 align(64),
};
const union_unnamed_28 = extern union {
    al_pal_8bpc: [2][32][3][8]u8,
    al_pal_16bpc: [2][32][3][8]u16,
};
const struct_unnamed_32 = extern struct {
    compinter: [2][16384]i16 = @import("std").mem.zeroes([2][16384]i16),
    seg_mask: [16384]u8 = @import("std").mem.zeroes([16384]u8),
};
const union_unnamed_31 = extern union {
    lap_8bpc: [4096]u8,
    lap_16bpc: [4096]u16,
    unnamed_0: struct_unnamed_32,
};
const union_unnamed_33 = extern union {
    emu_edge_8bpc: [84160]u8,
    emu_edge_16bpc: [84160]u16,
};
const struct_unnamed_30 = extern struct {
    unnamed_0: union_unnamed_31 = @import("std").mem.zeroes(union_unnamed_31),
    unnamed_1: union_unnamed_33 = @import("std").mem.zeroes(union_unnamed_33),
};
const struct_unnamed_36 = extern struct {
    pal_order: [64][8]u8 = @import("std").mem.zeroes([64][8]u8),
    pal_ctx: [64]u8 = @import("std").mem.zeroes([64]u8),
};
const union_unnamed_35 = extern union {
    levels: [1088]u8,
    unnamed_0: struct_unnamed_36,
};
const union_unnamed_37 = extern union {
    ac: [1024]i16,
    txtp_map: [1024]u8,
};
const struct_unnamed_39 = extern struct {
    interintra_8bpc: [4096]u8 align(8) = @import("std").mem.zeroes([4096]u8),
    edge_8bpc: [257]u8 = @import("std").mem.zeroes([257]u8),
    pal_8bpc: [3][8]u8 align(8) = @import("std").mem.zeroes([3][8]u8),
};
const struct_unnamed_40 = extern struct {
    interintra_16bpc: [4096]u16 align(16) = @import("std").mem.zeroes([4096]u16),
    edge_16bpc: [257]u16 = @import("std").mem.zeroes([257]u16),
    pal_16bpc: [3][8]u16 align(16) = @import("std").mem.zeroes([3][8]u16),
};
const union_unnamed_38 = extern union {
    unnamed_0: struct_unnamed_39,
    unnamed_1: struct_unnamed_40,
};
const struct_unnamed_34 = extern struct {
    unnamed_0: union_unnamed_35 = @import("std").mem.zeroes(union_unnamed_35),
    unnamed_1: union_unnamed_37 = @import("std").mem.zeroes(union_unnamed_37),
    pal_idx_y: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    pal_idx_uv: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    unnamed_2: union_unnamed_38 = @import("std").mem.zeroes(union_unnamed_38),
};
const union_unnamed_29 = extern union {
    unnamed_0: struct_unnamed_30 align(64),
    unnamed_1: struct_unnamed_34 align(64),
};
pub const Av1Filter = common.Av1Filter;
pub const enum_Filter2d = common.enum_Filter2d;
const struct_unnamed_41 = extern struct {
    pass: c_int = 0,
};
pub const afz_pthread_t = common.afz_pthread_t;
pub const afz_pthread_cond_t = common.afz_pthread_cond_t;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const struct_thread_data = common.struct_thread_data;
pub const enum_TaskType = common.enum_TaskType;
const struct_unnamed_46 = extern struct {
    grain_lut_8bpc: [3][74][82]i8 align(64) = @import("std").mem.zeroes([3][74][82]i8),
    scaling_8bpc: [3][256]u8 align(64) = @import("std").mem.zeroes([3][256]u8),
};
const struct_unnamed_47 = extern struct {
    grain_lut_16bpc: [3][74][82]i16 align(64) = @import("std").mem.zeroes([3][74][82]i16),
    scaling_16bpc: [3][4096]u8 align(64) = @import("std").mem.zeroes([3][4096]u8),
};
const union_unnamed_45 = extern union {
    unnamed_0: struct_unnamed_46,
    unnamed_1: struct_unnamed_47,
};
const struct_unnamed_44 = extern struct {
    exec: c_int = 0,
    finished: c_int = 0,
    cond: afz_pthread_cond_t = 0,
    in: [*c]const Dav1dPicture = null,
    out: [*c]Dav1dPicture = null,
    type: enum_TaskType = @import("std").mem.zeroes(enum_TaskType),
    progress: [2]atomic_int = @import("std").mem.zeroes([2]atomic_int),
    unnamed_0: union_unnamed_45 = @import("std").mem.zeroes(union_unnamed_45),
};
pub const struct_TaskThreadData_43 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    first: atomic_uint = 0,
    cur: c_uint = 0,
    reset_task_cur: atomic_uint = 0,
    cond_signaled: atomic_int = 0,
    delayed_fg: struct_unnamed_44 = @import("std").mem.zeroes(struct_unnamed_44),
    inited: c_int = 0,
};
pub const struct_FrameTileThreadData_48 = extern struct {
    lowest_pixel_mem: [*c][7][2]c_int = null,
    lowest_pixel_mem_sz: c_int = 0,
};
const struct_unnamed_42 = extern struct {
    td: struct_thread_data = @import("std").mem.zeroes(struct_thread_data),
    ttd: [*c]struct_TaskThreadData_43 = null,
    fttd: [*c]struct_FrameTileThreadData_48 = null,
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
    unnamed_0: union_unnamed_27 = @import("std").mem.zeroes(union_unnamed_27),
    unnamed_1: union_unnamed_28 = @import("std").mem.zeroes(union_unnamed_28),
    pal_sz_uv: [2][32]u8 = @import("std").mem.zeroes([2][32]u8),
    scratch: union_unnamed_29 = @import("std").mem.zeroes(union_unnamed_29),
    warpmv: Dav1dWarpedMotionParams = @import("std").mem.zeroes(Dav1dWarpedMotionParams),
    lf_mask: [*c]Av1Filter = null,
    top_pre_cdef_toggle: c_int = 0,
    cur_sb_cdef_idx_ptr: [*c]i8 = null,
    tl_4x4_filter: enum_Filter2d = @import("std").mem.zeroes(enum_Filter2d),
    frame_thread: struct_unnamed_41 = @import("std").mem.zeroes(struct_unnamed_41),
    task_thread: struct_unnamed_42 = @import("std").mem.zeroes(struct_unnamed_42),
};
pub const Dav1dTaskContext = struct_Dav1dTaskContext;
pub const enum_BlockSize = common.enum_BlockSize;
pub const enum_EdgeFlags = common.enum_EdgeFlags;
const struct_unnamed_50 = extern struct {
    y_mode: u8 = 0,
    uv_mode: u8 = 0,
    tx: u8 = 0,
    pal_sz: [2]u8 = @import("std").mem.zeroes([2]u8),
    y_angle: i8 = 0,
    uv_angle: i8 = 0,
    cfl_alpha: [2]i8 = @import("std").mem.zeroes([2]i8),
};
const struct_unnamed_53 = extern struct {
    mv: [2]union_mv = @import("std").mem.zeroes([2]union_mv),
    wedge_idx: u8 = 0,
    mask_sign: u8 = 0,
    interintra_mode: u8 = 0,
};
const struct_unnamed_54 = extern struct {
    mv2d: union_mv = @import("std").mem.zeroes(union_mv),
    matrix: [4]i16 = @import("std").mem.zeroes([4]i16),
};
const union_unnamed_52 = extern union {
    unnamed_0: struct_unnamed_53,
    unnamed_1: struct_unnamed_54,
};
const struct_unnamed_51 = extern struct {
    unnamed_0: union_unnamed_52 = @import("std").mem.zeroes(union_unnamed_52),
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
const union_unnamed_49 = extern union {
    unnamed_0: struct_unnamed_50,
    unnamed_1: struct_unnamed_51,
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
    unnamed_0: union_unnamed_49 = @import("std").mem.zeroes(union_unnamed_49),
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
const struct_unnamed_25 = extern struct {
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
const struct_unnamed_55 = extern struct {
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
const struct_unnamed_56 = extern struct {
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
const struct_unnamed_58 = extern struct {
    merge: atomic_int = 0,
    lock: afz_pthread_mutex_t = 0,
    head: [*c]Dav1dTask = null,
    tail: [*c]Dav1dTask = null,
};
const struct_unnamed_57 = extern struct {
    lock: afz_pthread_mutex_t = 0,
    cond: afz_pthread_cond_t = 0,
    ttd: [*c]struct_TaskThreadData_43 = null,
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
    pending_tasks: struct_unnamed_58 = @import("std").mem.zeroes(struct_unnamed_58),
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
    bd_fn: struct_unnamed_25 = @import("std").mem.zeroes(struct_unnamed_25),
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
    frame_thread: struct_unnamed_55 = @import("std").mem.zeroes(struct_unnamed_55),
    lf: struct_unnamed_56 = @import("std").mem.zeroes(struct_unnamed_56),
    task_thread: struct_unnamed_57 = @import("std").mem.zeroes(struct_unnamed_57),
    tile_thread: struct_FrameTileThreadData_48 = @import("std").mem.zeroes(struct_FrameTileThreadData_48),
};
pub const Dav1dFrameContext = struct_Dav1dFrameContext;
pub const Dav1dMemPoolBuffer = common.Dav1dMemPoolBuffer;
pub const struct_Dav1dMemPool = extern struct {
    lock: afz_pthread_mutex_t = 0,
    buf: [*c]Dav1dMemPoolBuffer = null,
    ref_cnt: c_int = 0,
    end: c_int = 0,
};
pub const Dav1dMemPool = struct_Dav1dMemPool;
const struct_unnamed_59 = extern struct {
    out_delayed: [*c]Dav1dThreadPicture = null,
    next: c_uint = 0,
};
const struct_unnamed_60 = extern struct {
    p: Dav1dThreadPicture = @import("std").mem.zeroes(Dav1dThreadPicture),
    segmap: [*c]Dav1dRef = null,
    refmvs: [*c]Dav1dRef = null,
    refpoc: [7]c_uint = @import("std").mem.zeroes([7]c_uint),
};
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
pub const DAV1D_INLOOPFILTER_ALL = common.DAV1D_INLOOPFILTER_ALL;
pub const enum_Dav1dInloopFilterType = common.enum_Dav1dInloopFilterType;
pub const DAV1D_DECODEFRAMETYPE_ALL = common.DAV1D_DECODEFRAMETYPE_ALL;
pub const DAV1D_DECODEFRAMETYPE_KEY = common.DAV1D_DECODEFRAMETYPE_KEY;
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
    frame_thread: struct_unnamed_59 = @import("std").mem.zeroes(struct_unnamed_59),
    task_thread: struct_TaskThreadData_43 = @import("std").mem.zeroes(struct_TaskThreadData_43),
    segmap_pool: [*c]Dav1dMemPool = null,
    refmvs_pool: [*c]Dav1dMemPool = null,
    refs: [8]struct_unnamed_60 = @import("std").mem.zeroes([8]struct_unnamed_60),
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
pub extern fn antfly_codec_abort() noreturn;
pub extern fn antfly_av1_dav1d_init_cpu() void;
pub extern fn antfly_av1_dav1d_num_logical_processors(c: [*c]Dav1dContext) c_int;
pub extern fn antfly_av1_dav1d_apply_grain_8bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_apply_grain_16bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture) void;
pub extern fn antfly_codec_afz_atomic_load(p: ?*const anyopaque) c_int;
pub extern fn antfly_codec_afz_atomic_store(p: ?*anyopaque, v: c_int) void;
pub extern fn antfly_codec_afz_atomic_add(p: ?*anyopaque, v: c_int) c_int;
pub extern fn antfly_codec_afz_atomic_compare(p: ?*anyopaque, expected: ?*anyopaque, desired: c_int) c_int;
pub const afz_pthread_once_t = common.afz_pthread_once_t;
pub const afz_pthread_attr_t = common.afz_pthread_attr_t;
pub extern fn antfly_codec_afz_mutex_init([*c]afz_pthread_mutex_t, ?*const anyopaque) c_int;
pub extern fn antfly_codec_afz_mutex_destroy([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_lock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_unlock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_cond_init([*c]afz_pthread_cond_t, ?*const anyopaque) c_int;
pub extern fn antfly_codec_afz_cond_destroy([*c]afz_pthread_cond_t) c_int;
pub extern fn antfly_codec_afz_cond_broadcast([*c]afz_pthread_cond_t) c_int;
pub extern fn antfly_codec_afz_cond_wait([*c]afz_pthread_cond_t, [*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_pthread_once([*c]afz_pthread_once_t, ?*const fn () callconv(.c) void) c_int;
pub extern fn antfly_codec_afz_pthread_create([*c]afz_pthread_t, [*c]const afz_pthread_attr_t, ?*const fn (?*anyopaque) callconv(.c) ?*anyopaque, ?*anyopaque) c_int;
pub extern fn antfly_codec_afz_pthread_join(afz_pthread_t, [*c]?*anyopaque) c_int;
pub extern fn antfly_codec_afz_attr_init([*c]afz_pthread_attr_t) c_int;
pub extern fn antfly_codec_afz_attr_destroy([*c]afz_pthread_attr_t) c_int;
pub extern fn antfly_codec_afz_attr_setstacksize([*c]afz_pthread_attr_t, usize) c_int;
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
pub extern fn antfly_av1_dav1d_mem_pool_init(pool: [*c][*c]Dav1dMemPool) c_int;
pub extern fn antfly_av1_dav1d_mem_pool_end(pool: [*c]Dav1dMemPool) void;
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
pub extern fn antfly_av1_dav1d_ref_dec(ref: [*c][*c]Dav1dRef) void;
pub extern fn antfly_av1_dav1d_cdf_thread_unref(cdf: [*c]CdfThreadContext) void;
pub extern fn antfly_av1_dav1d_data_ref(dst: [*c]Dav1dData, src: [*c]const Dav1dData) void;
pub extern fn antfly_av1_dav1d_data_props_copy(dst: [*c]Dav1dDataProps, src: [*c]const Dav1dDataProps) void;
pub extern fn antfly_av1_dav1d_data_props_set_defaults(props: [*c]Dav1dDataProps) void;
pub extern fn antfly_av1_dav1d_data_create_internal(buf: [*c]Dav1dData, sz: usize) [*c]u8;
pub extern fn antfly_av1_dav1d_data_unref_internal(buf: [*c]Dav1dData) void;
pub extern fn antfly_av1_dav1d_data_props_unref_internal(props: [*c]Dav1dDataProps) void;
pub fn umin(a: c_uint, b: c_uint) callconv(.c) c_uint {
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
pub extern fn antfly_av1_dav1d_init_intra_edge_tree() void;
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
pub extern fn antfly_av1_dav1d_refmvs_dsp_init(dsp: [*c]Dav1dRefmvsDSPContext) void;
pub extern fn antfly_av1_dav1d_pal_dsp_init(dsp: [*c]Dav1dPalDSPContext) void;
pub extern fn antfly_av1_dav1d_picture_alloc_copy(c: [*c]Dav1dContext, dst: [*c]Dav1dPicture, w: c_int, src: [*c]const Dav1dPicture) c_int;
pub extern fn antfly_av1_dav1d_picture_ref(dst: [*c]Dav1dPicture, src: [*c]const Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_thread_picture_ref(dst: [*c]Dav1dThreadPicture, src: [*c]const Dav1dThreadPicture) void;
pub extern fn antfly_av1_dav1d_thread_picture_move_ref(dst: [*c]Dav1dThreadPicture, src: [*c]Dav1dThreadPicture) void;
pub extern fn antfly_av1_dav1d_thread_picture_unref(p: [*c]Dav1dThreadPicture) void;
pub extern fn antfly_av1_dav1d_picture_move_ref(dst: [*c]Dav1dPicture, src: [*c]Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_default_picture_alloc(p: [*c]Dav1dPicture, cookie: ?*anyopaque) c_int;
pub extern fn antfly_av1_dav1d_default_picture_release(p: [*c]Dav1dPicture, cookie: ?*anyopaque) void;
pub extern fn antfly_av1_dav1d_picture_unref_internal(p: [*c]Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_picture_get_event_flags(p: [*c]const Dav1dThreadPicture) enum_Dav1dEventFlags;
pub extern fn antfly_av1_dav1d_parse_obus(c: [*c]Dav1dContext, in: [*c]Dav1dData) ptrdiff_t;
pub extern fn antfly_av1_dav1d_init_qm_tables() void;
pub extern fn antfly_av1_dav1d_task_delayed_fg(c: [*c]Dav1dContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture) void;
pub extern fn antfly_av1_dav1d_worker_task(data: ?*anyopaque) ?*anyopaque;
pub extern fn antfly_av1_dav1d_decode_frame_exit(f: [*c]Dav1dFrameContext, retval: c_int) void;
pub extern fn antfly_av1_dav1d_init_ii_wedge_masks() void;
pub fn init_internal() callconv(.c) void {
    antfly_av1_dav1d_init_cpu();
    antfly_av1_dav1d_init_ii_wedge_masks();
    antfly_av1_dav1d_init_intra_edge_tree();
    antfly_av1_dav1d_init_qm_tables();
    while (true) {
        if (!false) break;
    }
}
pub export fn antfly_av1_dav1d_default_settings(s: [*c]Dav1dSettings) void {
    _ = &s;
    s.*.n_threads = 0;
    s.*.max_frame_delay = 0;
    s.*.apply_grain = 1;
    s.*.allocator.cookie = null;
    s.*.allocator.alloc_picture_callback = antfly_av1_dav1d_default_picture_alloc;
    s.*.allocator.release_picture_callback = antfly_av1_dav1d_default_picture_release;
    s.*.logger.cookie = null;
    s.*.logger.callback = null;
    s.*.operating_point = 0;
    s.*.all_layers = 1;
    s.*.frame_size_limit = 0;
    s.*.strict_std_compliance = 0;
    s.*.output_invisible_frames = 0;
    s.*.inloop_filters = DAV1D_INLOOPFILTER_ALL;
    s.*.decode_frame_type = DAV1D_DECODEFRAMETYPE_ALL;
}
pub fn get_stack_size_internal(thread_attr: [*c]const afz_pthread_attr_t) callconv(.c) usize {
    _ = &thread_attr;
    return 0;
}
pub fn get_num_threads(c: [*c]Dav1dContext, s: [*c]const Dav1dSettings, arg_n_tc: [*c]c_uint, arg_n_fc: [*c]c_uint) callconv(.c) void {
    _ = &c;
    _ = &s;
    var n_tc = arg_n_tc;
    _ = &n_tc;
    var n_fc = arg_n_fc;
    _ = &n_fc;
    const static_local_fc_lut = struct {
        const fc_lut: [49]u8 = [49]u8{
            1,
            2,
            2,
            2,
            3,
            3,
            3,
            3,
            3,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            5,
            5,
            5,
            5,
            5,
            5,
            5,
            5,
            5,
            6,
            6,
            6,
            6,
            6,
            6,
            6,
            6,
            6,
            6,
            6,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
            7,
        };
    };
    _ = &static_local_fc_lut;
    n_tc.* = @bitCast(@as(c_int, if (s.*.n_threads != 0) s.*.n_threads else iclip(antfly_av1_dav1d_num_logical_processors(c), 1, 256)));
    n_fc.* = if (s.*.max_frame_delay != 0) umin(@bitCast(@as(c_int, s.*.max_frame_delay)), n_tc.*) else @as(c_uint, @bitCast(@as(c_int, if (n_tc.* < @as(c_uint, 50)) @as(c_int, static_local_fc_lut.fc_lut[n_tc.* -% @as(c_uint, 1)]) else @as(c_int, 8))));
}
pub export fn antfly_av1_dav1d_open(c_out: [*c][*c]Dav1dContext, s: [*c]const Dav1dSettings) c_int {
    _ = &c_out;
    _ = &s;
    var afz_t_8: [*c]Dav1dTaskContext = undefined;
    _ = &afz_t_8;
    var afz_m_7: c_uint = undefined;
    _ = &afz_m_7;
    var afz_f_6: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_6;
    var afz_n_5: c_uint = undefined;
    _ = &afz_n_5;
    var afz_out_delayed_sz_4: usize = undefined;
    _ = &afz_out_delayed_sz_4;
    var afz_c_3: [*c]Dav1dContext = undefined;
    _ = &afz_c_3;
    var afz_stack_size_2: usize = undefined;
    _ = &afz_stack_size_2;
    var afz_thread_attr_1: afz_pthread_attr_t = undefined;
    _ = &afz_thread_attr_1;
    const static_local_afz_initted_0 = struct {
        var afz_initted_0: afz_pthread_once_t = 0;
    };
    _ = &static_local_afz_initted_0;
    var afz_pc: c_uint = 143;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return -@as(c_int, 12);
            },
            2 => {
                _ = antfly_codec_afz_attr_destroy(&afz_thread_attr_1);
                afz_pc = 1;
                continue;
            },
            3 => {
                close_internal(c_out, 0);
                afz_pc = 2;
                continue;
            },
            4 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_3 != null) @as(c_int, 3) else @as(c_int, 2)));
                continue;
            },
            5 => {
                return 0;
            },
            6 => {
                _ = antfly_codec_afz_attr_destroy(&afz_thread_attr_1);
                afz_pc = 5;
                continue;
            },
            7 => {
                antfly_av1_dav1d_refmvs_dsp_init(&afz_c_3.*.refmvs_dsp);
                afz_pc = 6;
                continue;
            },
            8 => {
                antfly_av1_dav1d_pal_dsp_init(&afz_c_3.*.pal_dsp);
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_pc = @bitCast(@as(c_int, if (afz_m_7 < afz_c_3.*.n_tc) @as(c_int, 26) else @as(c_int, 8)));
                continue;
            },
            10 => {
                afz_m_7 +%= 1;
                afz_pc = 9;
                continue;
            },
            11 => {
                afz_t_8.*.task_thread.td.inited = 1;
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_pc = 4;
                continue;
            },
            13 => {
                _ = antfly_codec_afz_mutex_destroy(&afz_t_8.*.task_thread.td.lock);
                afz_pc = 12;
                continue;
            },
            14 => {
                _ = antfly_codec_afz_cond_destroy(&afz_t_8.*.task_thread.td.cond);
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_pthread_create(&afz_t_8.*.task_thread.td.thread, &afz_thread_attr_1, antfly_av1_dav1d_worker_task, @ptrCast(@alignCast(afz_t_8))) != 0) @as(c_int, 14) else @as(c_int, 11)));
                continue;
            },
            16 => {
                afz_pc = 4;
                continue;
            },
            17 => {
                _ = antfly_codec_afz_mutex_destroy(&afz_t_8.*.task_thread.td.lock);
                afz_pc = 16;
                continue;
            },
            18 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_cond_init(&afz_t_8.*.task_thread.td.cond, null) != 0) @as(c_int, 17) else @as(c_int, 15)));
                continue;
            },
            19 => {
                afz_pc = 4;
                continue;
            },
            20 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_mutex_init(&afz_t_8.*.task_thread.td.lock, null) != 0) @as(c_int, 19) else @as(c_int, 18)));
                continue;
            },
            21 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_3.*.n_tc > @as(c_uint, 1)) @as(c_int, 20) else @as(c_int, 10)));
                continue;
            },
            22 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]i32, @ptrCast(@alignCast(&afz_t_8.*.unnamed_0.cf_16bpc))))), 0, @sizeOf(@TypeOf(afz_t_8.*.unnamed_0.cf_16bpc)));
                afz_pc = 21;
                continue;
            },
            23 => {
                afz_t_8.*.c = afz_c_3;
                afz_pc = 22;
                continue;
            },
            24 => {
                afz_t_8.*.task_thread.ttd = &afz_c_3.*.task_thread;
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_t_8.*.f = &afz_c_3.*.fc[@as(c_int, 0)];
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_t_8 = &afz_c_3.*.tc[afz_m_7];
                afz_pc = 25;
                continue;
            },
            27 => {
                afz_m_7 = 0;
                afz_pc = 9;
                continue;
            },
            28 => {
                afz_pc = @bitCast(@as(c_int, if (afz_n_5 < afz_c_3.*.n_fc) @as(c_int, 43) else @as(c_int, 27)));
                continue;
            },
            29 => {
                afz_n_5 +%= 1;
                afz_pc = 28;
                continue;
            },
            30 => {
                afz_f_6.*.lf.last_sharpness = -@as(c_int, 1);
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_f_6.*.task_thread.ttd = &afz_c_3.*.task_thread;
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_f_6.*.c = afz_c_3;
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_pc = 4;
                continue;
            },
            34 => {
                _ = antfly_codec_afz_mutex_destroy(&afz_f_6.*.task_thread.lock);
                afz_pc = 33;
                continue;
            },
            35 => {
                _ = antfly_codec_afz_cond_destroy(&afz_f_6.*.task_thread.cond);
                afz_pc = 34;
                continue;
            },
            36 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_mutex_init(&afz_f_6.*.task_thread.pending_tasks.lock, null) != 0) @as(c_int, 35) else @as(c_int, 32)));
                continue;
            },
            37 => {
                afz_pc = 4;
                continue;
            },
            38 => {
                _ = antfly_codec_afz_mutex_destroy(&afz_f_6.*.task_thread.lock);
                afz_pc = 37;
                continue;
            },
            39 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_cond_init(&afz_f_6.*.task_thread.cond, null) != 0) @as(c_int, 38) else @as(c_int, 36)));
                continue;
            },
            40 => {
                afz_pc = 4;
                continue;
            },
            41 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_mutex_init(&afz_f_6.*.task_thread.lock, null) != 0) @as(c_int, 40) else @as(c_int, 39)));
                continue;
            },
            42 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_3.*.n_tc > @as(c_uint, 1)) @as(c_int, 41) else @as(c_int, 32)));
                continue;
            },
            43 => {
                afz_f_6 = &afz_c_3.*.fc[afz_n_5];
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_n_5 = 0;
                afz_pc = 28;
                continue;
            },
            45 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_c_3.*.frame_thread.out_delayed)), 0, afz_out_delayed_sz_4);
                afz_pc = 44;
                continue;
            },
            46 => {
                afz_pc = 4;
                continue;
            },
            47 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_3.*.frame_thread.out_delayed != null)) @as(c_int, 46) else @as(c_int, 45)));
                continue;
            },
            48 => {
                afz_c_3.*.frame_thread.out_delayed = @ptrCast(@alignCast(antfly_codec_afz_malloc(afz_out_delayed_sz_4)));
                afz_pc = 47;
                continue;
            },
            49 => {
                afz_out_delayed_sz_4 = @sizeOf(@TypeOf(afz_c_3.*.frame_thread.out_delayed.*)) *% @as(c_ulong, afz_c_3.*.n_fc);
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_3.*.n_fc > @as(c_uint, 1)) @as(c_int, 49) else @as(c_int, 44)));
                continue;
            },
            51 => {
                afz_c_3.*.task_thread.inited = 1;
                afz_pc = 50;
                continue;
            },
            52 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_c_3.*.task_thread.cond_signaled)), 0);
                afz_pc = 51;
                continue;
            },
            53 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_c_3.*.task_thread.reset_task_cur)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))));
                afz_pc = 52;
                continue;
            },
            54 => {
                afz_c_3.*.task_thread.cur = afz_c_3.*.n_fc;
                afz_pc = 53;
                continue;
            },
            55 => {
                afz_pc = 4;
                continue;
            },
            56 => {
                _ = antfly_codec_afz_mutex_destroy(&afz_c_3.*.task_thread.lock);
                afz_pc = 55;
                continue;
            },
            57 => {
                _ = antfly_codec_afz_cond_destroy(&afz_c_3.*.task_thread.cond);
                afz_pc = 56;
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_cond_init(&afz_c_3.*.task_thread.delayed_fg.cond, null) != 0) @as(c_int, 57) else @as(c_int, 54)));
                continue;
            },
            59 => {
                afz_pc = 4;
                continue;
            },
            60 => {
                _ = antfly_codec_afz_mutex_destroy(&afz_c_3.*.task_thread.lock);
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_cond_init(&afz_c_3.*.task_thread.cond, null) != 0) @as(c_int, 60) else @as(c_int, 58)));
                continue;
            },
            62 => {
                afz_pc = 4;
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_mutex_init(&afz_c_3.*.task_thread.lock, null) != 0) @as(c_int, 62) else @as(c_int, 61)));
                continue;
            },
            64 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_3.*.n_tc > @as(c_uint, 1)) @as(c_int, 63) else @as(c_int, 50)));
                continue;
            },
            65 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_c_3.*.tc)), 0, @sizeOf(@TypeOf(afz_c_3.*.tc.*)) *% @as(c_ulong, afz_c_3.*.n_tc));
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_pc = 4;
                continue;
            },
            67 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_3.*.tc != null)) @as(c_int, 66) else @as(c_int, 65)));
                continue;
            },
            68 => {
                afz_c_3.*.tc = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(@sizeOf(@TypeOf(afz_c_3.*.tc.*)) *% @as(c_ulong, afz_c_3.*.n_tc), 64)));
                afz_pc = 67;
                continue;
            },
            69 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_c_3.*.fc)), 0, @sizeOf(@TypeOf(afz_c_3.*.fc.*)) *% @as(c_ulong, afz_c_3.*.n_fc));
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_pc = 4;
                continue;
            },
            71 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_3.*.fc != null)) @as(c_int, 70) else @as(c_int, 69)));
                continue;
            },
            72 => {
                afz_c_3.*.fc = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(@sizeOf(@TypeOf(afz_c_3.*.fc.*)) *% @as(c_ulong, afz_c_3.*.n_fc), 32)));
                afz_pc = 71;
                continue;
            },
            73 => {
                get_num_threads(afz_c_3, s, &afz_c_3.*.n_tc, &afz_c_3.*.n_fc);
                afz_pc = 72;
                continue;
            },
            74 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(afz_c_3.*.flush)), 0);
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_c_3.*.flush = &afz_c_3.*.flush_mem;
                afz_pc = 74;
                continue;
            },
            76 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 76) else @as(c_int, 75);
                continue;
            },
            77 => {
                afz_pc = @bitCast(@as(c_int, if (s.*.frame_size_limit != 0) @as(c_int, 76) else @as(c_int, 75)));
                continue;
            },
            78 => {
                afz_c_3.*.frame_size_limit = @bitCast(@as(c_int, @as(c_int, 8192) * @as(c_int, 8192)));
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_pc = @bitCast(@as(c_int, if ((@sizeOf(usize) < @as(c_ulong, 8)) and ((s.*.frame_size_limit -% @as(c_uint, 1)) >= @as(c_uint, @bitCast(@as(c_int, @as(c_int, 8192) * @as(c_int, 8192)))))) @as(c_int, 78) else @as(c_int, 75)));
                continue;
            },
            80 => {
                afz_c_3.*.allocator.cookie = @ptrCast(@alignCast(afz_c_3.*.picture_pool));
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_pc = 4;
                continue;
            },
            82 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.picture_pool) != 0) @as(c_int, 81) else @as(c_int, 80)));
                continue;
            },
            83 => {
                afz_pc = 4;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_3.*.allocator.cookie != null) @as(c_int, 83) else @as(c_int, 82)));
                continue;
            },
            85 => {
                afz_pc = 4;
                continue;
            },
            86 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_c_3.*.allocator.alloc_picture_callback == antfly_av1_dav1d_default_picture_alloc) or (afz_c_3.*.allocator.release_picture_callback == antfly_av1_dav1d_default_picture_release)) @as(c_int, 85) else @as(c_int, 79)));
                continue;
            },
            87 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_c_3.*.allocator.alloc_picture_callback == antfly_av1_dav1d_default_picture_alloc) and (afz_c_3.*.allocator.release_picture_callback == antfly_av1_dav1d_default_picture_release)) @as(c_int, 84) else @as(c_int, 86)));
                continue;
            },
            88 => {
                afz_pc = 4;
                continue;
            },
            89 => {
                afz_pc = @bitCast(@as(c_int, if ((((((antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.seq_hdr_pool) != 0) or (antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.frame_hdr_pool) != 0)) or (antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.segmap_pool) != 0)) or (antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.refmvs_pool) != 0)) or (antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.pic_ctx_pool) != 0)) or (antfly_av1_dav1d_mem_pool_init(&afz_c_3.*.cdf_pool) != 0)) @as(c_int, 88) else @as(c_int, 87)));
                continue;
            },
            90 => {
                antfly_av1_dav1d_data_props_set_defaults(&afz_c_3.*.cached_error_props);
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_c_3.*.decode_frame_type = s.*.decode_frame_type;
                afz_pc = 90;
                continue;
            },
            92 => {
                afz_c_3.*.inloop_filters = s.*.inloop_filters;
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_c_3.*.output_invisible_frames = s.*.output_invisible_frames;
                afz_pc = 92;
                continue;
            },
            94 => {
                afz_c_3.*.strict_std_compliance = s.*.strict_std_compliance;
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_c_3.*.frame_size_limit = s.*.frame_size_limit;
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_c_3.*.all_layers = s.*.all_layers;
                afz_pc = 95;
                continue;
            },
            97 => {
                afz_c_3.*.operating_point = s.*.operating_point;
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_c_3.*.apply_grain = s.*.apply_grain;
                afz_pc = 97;
                continue;
            },
            99 => {
                afz_c_3.*.logger = s.*.logger;
                afz_pc = 98;
                continue;
            },
            100 => {
                afz_c_3.*.allocator = s.*.allocator;
                afz_pc = 99;
                continue;
            },
            101 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_c_3)), 0, @sizeOf(@TypeOf(afz_c_3.*)));
                afz_pc = 100;
                continue;
            },
            102 => {
                afz_pc = 4;
                continue;
            },
            103 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_3 != null)) @as(c_int, 102) else @as(c_int, 101)));
                continue;
            },
            104 => {
                afz_c_3 = blk: {
                    const tmp = @as([*c]Dav1dContext, @ptrCast(@alignCast(dav1d_alloc_aligned_internal(@sizeOf(@TypeOf(afz_c_3.*)), 64))));
                    c_out.* = tmp;
                    break :blk tmp;
                };
                afz_pc = 103;
                continue;
            },
            105 => {
                _ = antfly_codec_afz_attr_setstacksize(&afz_thread_attr_1, afz_stack_size_2);
                afz_pc = 104;
                continue;
            },
            106 => {
                afz_stack_size_2 = @as(usize, @bitCast(@as(c_long, @as(c_int, 1024) * @as(c_int, 1024)))) +% get_stack_size_internal(&afz_thread_attr_1);
                afz_pc = 105;
                continue;
            },
            107 => {
                return -@as(c_int, 12);
            },
            108 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_attr_init(&afz_thread_attr_1) != 0) @as(c_int, 107) else @as(c_int, 106)));
                continue;
            },
            109 => {
                afz_pc = 108;
                continue;
            },
            110 => {
                {
                    return -@as(c_int, 22);
                }
            },
            111 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 111) else @as(c_int, 110);
                continue;
            },
            112 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 112) else @as(c_int, 111);
                continue;
            },
            113 => {
                afz_pc = @bitCast(@as(c_int, if (!((s.*.decode_frame_type >= @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_ALL)) and (s.*.decode_frame_type <= @as(enum_Dav1dDecodeFrameType, DAV1D_DECODEFRAMETYPE_KEY)))) @as(c_int, 112) else @as(c_int, 109)));
                continue;
            },
            114 => {
                {
                    return -@as(c_int, 22);
                }
            },
            115 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 115) else @as(c_int, 114);
                continue;
            },
            116 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 116) else @as(c_int, 115);
                continue;
            },
            117 => {
                afz_pc = @bitCast(@as(c_int, if (!((s.*.operating_point >= @as(c_int, 0)) and (s.*.operating_point <= @as(c_int, 31)))) @as(c_int, 116) else @as(c_int, 113)));
                continue;
            },
            118 => {
                {
                    return -@as(c_int, 22);
                }
            },
            119 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 119) else @as(c_int, 118);
                continue;
            },
            120 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 120) else @as(c_int, 119);
                continue;
            },
            121 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(s.*.allocator.release_picture_callback)))) != @as(?*anyopaque, null))) @as(c_int, 120) else @as(c_int, 117)));
                continue;
            },
            122 => {
                {
                    return -@as(c_int, 22);
                }
            },
            123 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 123) else @as(c_int, 122);
                continue;
            },
            124 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 124) else @as(c_int, 123);
                continue;
            },
            125 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(s.*.allocator.alloc_picture_callback)))) != @as(?*anyopaque, null))) @as(c_int, 124) else @as(c_int, 121)));
                continue;
            },
            126 => {
                {
                    return -@as(c_int, 22);
                }
            },
            127 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 127) else @as(c_int, 126);
                continue;
            },
            128 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 128) else @as(c_int, 127);
                continue;
            },
            129 => {
                afz_pc = @bitCast(@as(c_int, if (!((s.*.max_frame_delay >= @as(c_int, 0)) and (s.*.max_frame_delay <= @as(c_int, 256)))) @as(c_int, 128) else @as(c_int, 125)));
                continue;
            },
            130 => {
                {
                    return -@as(c_int, 22);
                }
            },
            131 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 131) else @as(c_int, 130);
                continue;
            },
            132 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 132) else @as(c_int, 131);
                continue;
            },
            133 => {
                afz_pc = @bitCast(@as(c_int, if (!((s.*.n_threads >= @as(c_int, 0)) and (s.*.n_threads <= @as(c_int, 256)))) @as(c_int, 132) else @as(c_int, 129)));
                continue;
            },
            134 => {
                {
                    return -@as(c_int, 22);
                }
            },
            135 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 135) else @as(c_int, 134);
                continue;
            },
            136 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 136) else @as(c_int, 135);
                continue;
            },
            137 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(s)))) != @as(?*anyopaque, null))) @as(c_int, 136) else @as(c_int, 133)));
                continue;
            },
            138 => {
                {
                    return -@as(c_int, 22);
                }
            },
            139 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 139) else @as(c_int, 138);
                continue;
            },
            140 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 140) else @as(c_int, 139);
                continue;
            },
            141 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(c_out))) != @as(?*anyopaque, null))) @as(c_int, 140) else @as(c_int, 137)));
                continue;
            },
            142 => {
                _ = antfly_codec_afz_pthread_once(&static_local_afz_initted_0.afz_initted_0, init_internal);
                afz_pc = 141;
                continue;
            },
            143 => {
                afz_pc = 142;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub fn has_grain(pic: [*c]const Dav1dPicture) callconv(.c) c_int {
    _ = &pic;
    var fgdata: [*c]const Dav1dFilmGrainData = &pic.*.frame_hdr.*.film_grain.data;
    _ = &fgdata;
    return @as(c_int, @intFromBool((((fgdata.*.num_y_points != 0) or (fgdata.*.num_uv_points[@as(c_int, 0)] != 0)) or (fgdata.*.num_uv_points[@as(c_int, 1)] != 0)) or ((fgdata.*.clip_to_restricted_range != 0) and (fgdata.*.chroma_scaling_from_luma != 0))));
}
pub fn output_image(c: [*c]Dav1dContext, out: [*c]Dav1dPicture) callconv(.c) c_int {
    _ = &c;
    _ = &out;
    var afz_in_1: [*c]Dav1dThreadPicture = undefined;
    _ = &afz_in_1;
    var afz_res_0: c_int = undefined;
    _ = &afz_res_0;
    var afz_pc: c_uint = 11;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_res_0;
            },
            2 => {
                antfly_av1_dav1d_thread_picture_move_ref(afz_in_1, &c.*.out);
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = @bitCast(@as(c_int, if ((!(c.*.all_layers != 0) and (c.*.max_spatial_id != 0)) and (c.*.out.p.data[@as(c_int, 0)] != null)) @as(c_int, 2) else @as(c_int, 1)));
                continue;
            },
            4 => {
                antfly_av1_dav1d_thread_picture_unref(afz_in_1);
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_res_0 = antfly_av1_dav1d_apply_grain(c, out, &afz_in_1.*.p);
                afz_pc = 4;
                continue;
            },
            6 => {
                afz_pc = 3;
                continue;
            },
            7 => {
                antfly_av1_dav1d_thread_picture_unref(afz_in_1);
                afz_pc = 6;
                continue;
            },
            8 => {
                antfly_av1_dav1d_picture_move_ref(out, &afz_in_1.*.p);
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_pc = @bitCast(@as(c_int, if (!(c.*.apply_grain != 0) or !(has_grain(&afz_in_1.*.p) != 0)) @as(c_int, 8) else @as(c_int, 5)));
                continue;
            },
            10 => {
                afz_in_1 = if ((c.*.all_layers != 0) or !(c.*.max_spatial_id != 0)) &c.*.out else &c.*.cache;
                afz_pc = 9;
                continue;
            },
            11 => {
                afz_res_0 = 0;
                afz_pc = 10;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub fn output_picture_ready(c: [*c]Dav1dContext, drain: c_int) callconv(.c) c_int {
    _ = &c;
    _ = &drain;
    if (c.*.cached_error != 0) return 1;
    if (!(c.*.all_layers != 0) and (c.*.max_spatial_id != 0)) {
        if ((c.*.out.p.data[@as(c_int, 0)] != null) and (c.*.cache.p.data[@as(c_int, 0)] != null)) {
            if ((c.*.max_spatial_id == @as(c_int, c.*.cache.p.frame_hdr.*.spatial_id)) or (@as(c_int, @bitCast(c.*.out.flags & @as(enum_PictureFlags, PICTURE_FLAG_NEW_TEMPORAL_UNIT))) != 0)) return 1;
            antfly_av1_dav1d_thread_picture_unref(&c.*.cache);
            antfly_av1_dav1d_thread_picture_move_ref(&c.*.cache, &c.*.out);
            return 0;
        } else if ((c.*.cache.p.data[@as(c_int, 0)] != null) and (drain != 0)) {
            return 1;
        } else if (c.*.out.p.data[@as(c_int, 0)] != null) {
            antfly_av1_dav1d_thread_picture_move_ref(&c.*.cache, &c.*.out);
            return 0;
        }
    }
    return @as(c_int, @intFromBool(!!(c.*.out.p.data[@as(c_int, 0)] != null)));
}
pub fn drain_picture(c: [*c]Dav1dContext, out: [*c]Dav1dPicture) callconv(.c) c_int {
    _ = &c;
    _ = &out;
    var drain_count: c_uint = 0;
    _ = &drain_count;
    var drained: c_int = 0;
    _ = &drained;
    while (true) {
        const next: c_uint = c.*.frame_thread.next;
        _ = &next;
        const f: [*c]Dav1dFrameContext = &c.*.fc[next];
        _ = &f;
        _ = antfly_codec_afz_mutex_lock(&c.*.task_thread.lock);
        while (f.*.n_tile_data > @as(c_int, 0)) {
            _ = antfly_codec_afz_cond_wait(&f.*.task_thread.cond, &f.*.task_thread.ttd.*.lock);
        }
        const out_delayed: [*c]Dav1dThreadPicture = &c.*.frame_thread.out_delayed[next];
        _ = &out_delayed;
        if ((out_delayed.*.p.data[@as(c_int, 0)] != null) or (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&f.*.task_thread.@"error"))) != 0)) {
            var first: c_uint = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&c.*.task_thread.first)))));
            _ = &first;
            if ((first +% @as(c_uint, 1)) < c.*.n_fc) {
                _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&c.*.task_thread.first)), 1);
            } else {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.task_thread.first)), 0);
            }
            _ = antfly_codec_afz_atomic_compare(@ptrCast(@alignCast(&c.*.task_thread.reset_task_cur)), @ptrCast(@alignCast(&first)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))));
            if ((c.*.task_thread.cur != 0) and (@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(c.*.task_thread.cur < c.*.n_fc))))) != 0)) {
                c.*.task_thread.cur -%= 1;
            }
            drained = 1;
        } else if (drained != 0) {
            _ = antfly_codec_afz_mutex_unlock(&c.*.task_thread.lock);
            break;
        }
        if ((blk: {
            const ref = &c.*.frame_thread.next;
            ref.* +%= 1;
            break :blk ref.*;
        }) == c.*.n_fc) {
            c.*.frame_thread.next = 0;
        }
        _ = antfly_codec_afz_mutex_unlock(&c.*.task_thread.lock);
        const @"error": c_int = f.*.task_thread.retval;
        _ = &@"error";
        if (@"error" != 0) {
            f.*.task_thread.retval = 0;
            antfly_av1_dav1d_data_props_copy(&c.*.cached_error_props, &out_delayed.*.p.m);
            antfly_av1_dav1d_thread_picture_unref(out_delayed);
            return @"error";
        }
        if (out_delayed.*.p.data[@as(c_int, 0)] != null) {
            const progress: c_uint = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&out_delayed.*.progress[@as(c_int, 1)])))));
            _ = &progress;
            if (((out_delayed.*.visible != 0) or (c.*.output_invisible_frames != 0)) and (progress != (((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))) {
                antfly_av1_dav1d_thread_picture_ref(&c.*.out, out_delayed);
                c.*.event_flags |= antfly_av1_dav1d_picture_get_event_flags(out_delayed);
            }
            antfly_av1_dav1d_thread_picture_unref(out_delayed);
            if (output_picture_ready(c, 0) != 0) return output_image(c, out);
        }
        if (!((blk: {
            const ref = &drain_count;
            ref.* +%= 1;
            break :blk ref.*;
        }) < c.*.n_fc)) break;
    }
    if (output_picture_ready(c, 1) != 0) return output_image(c, out);
    return -@as(c_int, 35);
}
pub fn gen_picture(c: [*c]Dav1dContext) callconv(.c) c_int {
    _ = &c;
    const in: [*c]Dav1dData = &c.*.in;
    _ = &in;
    if (output_picture_ready(c, 0) != 0) return 0;
    while (in.*.sz > @as(usize, 0)) {
        const res: ptrdiff_t = antfly_av1_dav1d_parse_obus(c, in);
        _ = &res;
        if (res < @as(ptrdiff_t, 0)) {
            antfly_av1_dav1d_data_unref_internal(in);
        } else {
            while (true) {
                if (!(@as(usize, @bitCast(@as(c_long, res))) <= in.*.sz)) {
                    unreachable;
                }
                if (!false) break;
            }
            in.*.sz -%= @bitCast(@as(c_long, res));
            in.*.data += @as(usize, @bitCast(@as(isize, @intCast(res))));
            if (!(in.*.sz != 0)) {
                antfly_av1_dav1d_data_unref_internal(in);
            }
        }
        if (output_picture_ready(c, 0) != 0) break;
        if (res < @as(ptrdiff_t, 0)) return @truncate(res);
    }
    return 0;
}
pub export fn antfly_av1_dav1d_send_data(c: [*c]Dav1dContext, in: [*c]Dav1dData) c_int {
    _ = &c;
    _ = &in;
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(c))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        {
            return -@as(c_int, 22);
        }
    }
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(in))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        {
            return -@as(c_int, 22);
        }
    }
    if (in.*.data != null) {
        if (!((in.*.sz > @as(usize, 0)) and (in.*.sz <= (@as(c_ulong, 18446744073709551615) / @as(c_ulong, 2))))) {
            while (true) {
                if (!false) break;
            }
            while (true) {
                if (!false) break;
            }
            {
                return -@as(c_int, 22);
            }
        }
        c.*.drain = 0;
    }
    if (c.*.in.data != null) return -@as(c_int, 35);
    antfly_av1_dav1d_data_ref(&c.*.in, in);
    var res: c_int = gen_picture(c);
    _ = &res;
    if (!(res != 0)) {
        antfly_av1_dav1d_data_unref_internal(in);
    }
    return res;
}
pub export fn antfly_av1_dav1d_get_picture(c: [*c]Dav1dContext, out: [*c]Dav1dPicture) c_int {
    _ = &c;
    _ = &out;
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(c))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        {
            return -@as(c_int, 22);
        }
    }
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(out))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        {
            return -@as(c_int, 22);
        }
    }
    const drain: c_int = c.*.drain;
    _ = &drain;
    c.*.drain = 1;
    var res: c_int = gen_picture(c);
    _ = &res;
    if (res < @as(c_int, 0)) return res;
    if (c.*.cached_error != 0) {
        const res_1: c_int = c.*.cached_error;
        _ = &res_1;
        c.*.cached_error = 0;
        return res_1;
    }
    if (output_picture_ready(c, @as(c_int, @intFromBool(c.*.n_fc == @as(c_uint, 1)))) != 0) return output_image(c, out);
    if ((c.*.n_fc > @as(c_uint, 1)) and (drain != 0)) return drain_picture(c, out);
    return -@as(c_int, 35);
}
pub export fn antfly_av1_dav1d_apply_grain(c: [*c]Dav1dContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture) c_int {
    _ = &c;
    _ = &out;
    _ = &in;
    var afz_res_0: c_int = undefined;
    _ = &afz_res_0;
    var afz_pc: c_uint = 29;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_res_0;
            },
            2 => {
                antfly_av1_dav1d_picture_unref_internal(out);
                afz_pc = 1;
                continue;
            },
            3 => {
                return 0;
            },
            4 => {
                antfly_av1_dav1d_task_delayed_fg(c, out, in);
                afz_pc = 3;
                continue;
            },
            5 => {
                antfly_codec_abort();
                afz_pc = 3;
                continue;
            },
            6 => {
                afz_pc = 3;
                continue;
            },
            7 => {
                antfly_av1_dav1d_apply_grain_16bpc(&c.*.dsp[@bitCast(@as(isize, @intCast((out.*.p.bpc >> @intCast(@as(c_int, 1))) - @as(c_int, 4))))].fg, out, in);
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_pc = 3;
                continue;
            },
            9 => {
                antfly_av1_dav1d_apply_grain_8bpc(&c.*.dsp[@as(c_int, 0)].fg, out, in);
                afz_pc = 8;
                continue;
            },
            10 => {
                blk: {
                    switch (out.*.p.bpc) {
                        @as(c_int, 12) => {
                            afz_pc = 7;
                            continue;
                        },
                        @as(c_int, 10) => {
                            afz_pc = 7;
                            continue;
                        },
                        @as(c_int, 8) => {
                            afz_pc = 9;
                            continue;
                        },
                        else => {
                            afz_pc = 5;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            11 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.n_tc > @as(c_uint, 1)) @as(c_int, 4) else @as(c_int, 10)));
                continue;
            },
            12 => {
                afz_pc = 2;
                continue;
            },
            13 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_0 < @as(c_int, 0)) @as(c_int, 12) else @as(c_int, 11)));
                continue;
            },
            14 => {
                afz_res_0 = antfly_av1_dav1d_picture_alloc_copy(c, out, in.*.p.w, in);
                afz_pc = 13;
                continue;
            },
            15 => {
                return 0;
            },
            16 => {
                antfly_av1_dav1d_picture_ref(out, in);
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = @bitCast(@as(c_int, if (!(has_grain(in) != 0)) @as(c_int, 16) else @as(c_int, 14)));
                continue;
            },
            18 => {
                {
                    return -@as(c_int, 22);
                }
            },
            19 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 19) else @as(c_int, 18);
                continue;
            },
            20 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 20) else @as(c_int, 19);
                continue;
            },
            21 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(in)))) != @as(?*anyopaque, null))) @as(c_int, 20) else @as(c_int, 17)));
                continue;
            },
            22 => {
                {
                    return -@as(c_int, 22);
                }
            },
            23 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 23) else @as(c_int, 22);
                continue;
            },
            24 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 24) else @as(c_int, 23);
                continue;
            },
            25 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(out))) != @as(?*anyopaque, null))) @as(c_int, 24) else @as(c_int, 21)));
                continue;
            },
            26 => {
                {
                    return -@as(c_int, 22);
                }
            },
            27 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 27) else @as(c_int, 26);
                continue;
            },
            28 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 28) else @as(c_int, 27);
                continue;
            },
            29 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(?*anyopaque, @ptrCast(@alignCast(c))) != @as(?*anyopaque, null))) @as(c_int, 28) else @as(c_int, 25)));
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub export fn antfly_av1_dav1d_flush(c: [*c]Dav1dContext) void {
    _ = &c;
    antfly_av1_dav1d_data_unref_internal(&c.*.in);
    if (c.*.out.p.frame_hdr != null) {
        antfly_av1_dav1d_thread_picture_unref(&c.*.out);
    }
    if (c.*.cache.p.frame_hdr != null) {
        antfly_av1_dav1d_thread_picture_unref(&c.*.cache);
    }
    c.*.drain = 0;
    c.*.cached_error = 0;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (c.*.refs[@bitCast(@as(isize, @intCast(i)))].p.p.frame_hdr != null) {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(i)))].p);
            }
            antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(i)))].segmap);
            antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(i)))].refmvs);
            antfly_av1_dav1d_cdf_thread_unref(&c.*.cdf[@bitCast(@as(isize, @intCast(i)))]);
        }
    }
    c.*.frame_hdr = null;
    c.*.seq_hdr = null;
    antfly_av1_dav1d_ref_dec(&c.*.seq_hdr_ref);
    c.*.mastering_display = null;
    c.*.content_light = null;
    c.*.itut_t35 = null;
    c.*.n_itut_t35 = 0;
    antfly_av1_dav1d_ref_dec(&c.*.mastering_display_ref);
    antfly_av1_dav1d_ref_dec(&c.*.content_light_ref);
    antfly_av1_dav1d_ref_dec(&c.*.itut_t35_ref);
    antfly_av1_dav1d_data_props_unref_internal(&c.*.cached_error_props);
    if ((c.*.n_fc == @as(c_uint, 1)) and (c.*.n_tc == @as(c_uint, 1))) return;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(c.*.flush)), 1);
    if (c.*.n_tc > @as(c_uint, 1)) {
        _ = antfly_codec_afz_mutex_lock(&c.*.task_thread.lock);
        {
            var i: c_uint = 0;
            _ = &i;
            while (i < c.*.n_tc) : (i +%= 1) {
                const tc: [*c]Dav1dTaskContext = &c.*.tc[i];
                _ = &tc;
                while (!(tc.*.task_thread.flushed != 0)) {
                    _ = antfly_codec_afz_cond_wait(&tc.*.task_thread.td.cond, &c.*.task_thread.lock);
                }
            }
        }
        {
            var i: c_uint = 0;
            _ = &i;
            while (i < c.*.n_fc) : (i +%= 1) {
                c.*.fc[i].task_thread.task_head = null;
                c.*.fc[i].task_thread.task_tail = null;
                c.*.fc[i].task_thread.task_cur_prev = null;
                c.*.fc[i].task_thread.pending_tasks.head = null;
                c.*.fc[i].task_thread.pending_tasks.tail = null;
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.fc[i].task_thread.pending_tasks.merge)), 0);
            }
        }
        antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.task_thread.first)), 0);
        c.*.task_thread.cur = c.*.n_fc;
        antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.task_thread.reset_task_cur)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))));
        antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&c.*.task_thread.cond_signaled)), 0);
        _ = antfly_codec_afz_mutex_unlock(&c.*.task_thread.lock);
    }
    if (c.*.n_fc > @as(c_uint, 1)) {
        {
            var n: c_uint = 0;
            _ = &n;
            var next: c_uint = c.*.frame_thread.next;
            _ = &next;
            while (n < c.*.n_fc) : ({
                n +%= 1;
                next +%= 1;
            }) {
                if (next == c.*.n_fc) {
                    next = 0;
                }
                const f: [*c]Dav1dFrameContext = &c.*.fc[next];
                _ = &f;
                antfly_av1_dav1d_decode_frame_exit(f, -@as(c_int, 1));
                f.*.n_tile_data = 0;
                f.*.task_thread.retval = 0;
                f.*.task_thread.@"error" = 0;
                var out_delayed: [*c]Dav1dThreadPicture = &c.*.frame_thread.out_delayed[next];
                _ = &out_delayed;
                if (out_delayed.*.p.frame_hdr != null) {
                    antfly_av1_dav1d_thread_picture_unref(out_delayed);
                }
            }
        }
        c.*.frame_thread.next = 0;
    }
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(c.*.flush)), 0);
}
pub fn close_internal(c_out: [*c][*c]Dav1dContext, arg_flush: c_int) callconv(.c) void {
    _ = &c_out;
    var flush = arg_flush;
    _ = &flush;
    const c: [*c]Dav1dContext = c_out.*;
    _ = &c;
    if (!(c != null)) return;
    if (flush != 0) {
        antfly_av1_dav1d_flush(c);
    }
    if (c.*.tc != null) {
        var ttd: [*c]struct_TaskThreadData_43 = &c.*.task_thread;
        _ = &ttd;
        if (ttd.*.inited != 0) {
            _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
            {
                var n: c_uint = 0;
                _ = &n;
                while ((n < c.*.n_tc) and (c.*.tc[n].task_thread.td.inited != 0)) : (n +%= 1) {
                    c.*.tc[n].task_thread.die = 1;
                }
            }
            _ = antfly_codec_afz_cond_broadcast(&ttd.*.cond);
            _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
            {
                var n: c_uint = 0;
                _ = &n;
                while (n < c.*.n_tc) : (n +%= 1) {
                    const pf: [*c]Dav1dTaskContext = &c.*.tc[n];
                    _ = &pf;
                    if (!(pf.*.task_thread.td.inited != 0)) break;
                    _ = antfly_codec_afz_pthread_join(pf.*.task_thread.td.thread, null);
                    _ = antfly_codec_afz_cond_destroy(&pf.*.task_thread.td.cond);
                    _ = antfly_codec_afz_mutex_destroy(&pf.*.task_thread.td.lock);
                }
            }
            _ = antfly_codec_afz_cond_destroy(&ttd.*.delayed_fg.cond);
            _ = antfly_codec_afz_cond_destroy(&ttd.*.cond);
            _ = antfly_codec_afz_mutex_destroy(&ttd.*.lock);
        }
        dav1d_free_aligned_internal(@ptrCast(@alignCast(c.*.tc)));
    }
    {
        var n: c_uint = 0;
        _ = &n;
        while ((c.*.fc != null) and (n < c.*.n_fc)) : (n +%= 1) {
            const f: [*c]Dav1dFrameContext = &c.*.fc[n];
            _ = &f;
            if (c.*.n_fc > @as(c_uint, 1)) {
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.tile_thread.lowest_pixel_mem)));
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.frame_thread.b)));
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.frame_thread.cbi)));
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.frame_thread.pal_idx)));
                dav1d_free_aligned_internal(f.*.frame_thread.cf);
                antfly_codec_afz_free(@ptrCast(@alignCast(f.*.frame_thread.tile_start_off)));
                dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.frame_thread.pal)));
            }
            if (c.*.n_tc > @as(c_uint, 1)) {
                _ = antfly_codec_afz_mutex_destroy(&f.*.task_thread.pending_tasks.lock);
                _ = antfly_codec_afz_cond_destroy(&f.*.task_thread.cond);
                _ = antfly_codec_afz_mutex_destroy(&f.*.task_thread.lock);
            }
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.frame_thread.frame_progress)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.task_thread.tasks)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.task_thread.tile_tasks[@as(c_int, 0)])));
            dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.ts)));
            dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.ipred_edge[@as(c_int, 0)])));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.a)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.tile)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.mask)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.level)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.lr_mask)));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.tx_lpf_right_edge[@as(c_int, 0)])));
            antfly_codec_afz_free(@ptrCast(@alignCast(f.*.lf.start_of_tile_row)));
            dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.rf.r)));
            dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.lf.cdef_line_buf)));
            dav1d_free_aligned_internal(@ptrCast(@alignCast(f.*.lf.lr_line_buf)));
        }
    }
    dav1d_free_aligned_internal(@ptrCast(@alignCast(c.*.fc)));
    if ((c.*.n_fc > @as(c_uint, 1)) and (c.*.frame_thread.out_delayed != null)) {
        {
            var n: c_uint = 0;
            _ = &n;
            while (n < c.*.n_fc) : (n +%= 1) if (c.*.frame_thread.out_delayed[n].p.frame_hdr != null) {
                antfly_av1_dav1d_thread_picture_unref(&c.*.frame_thread.out_delayed[n]);
            };
        }
        antfly_codec_afz_free(@ptrCast(@alignCast(c.*.frame_thread.out_delayed)));
    }
    {
        var n: c_int = 0;
        _ = &n;
        while (n < c.*.n_tile_data) : (n += 1) {
            antfly_av1_dav1d_data_unref_internal(&c.*.tile[@bitCast(@as(isize, @intCast(n)))].data);
        }
    }
    antfly_codec_afz_free(@ptrCast(@alignCast(c.*.tile)));
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 8)) : (n += 1) {
            antfly_av1_dav1d_cdf_thread_unref(&c.*.cdf[@bitCast(@as(isize, @intCast(n)))]);
            if (c.*.refs[@bitCast(@as(isize, @intCast(n)))].p.p.frame_hdr != null) {
                antfly_av1_dav1d_thread_picture_unref(&c.*.refs[@bitCast(@as(isize, @intCast(n)))].p);
            }
            antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(n)))].refmvs);
            antfly_av1_dav1d_ref_dec(&c.*.refs[@bitCast(@as(isize, @intCast(n)))].segmap);
        }
    }
    antfly_av1_dav1d_ref_dec(&c.*.seq_hdr_ref);
    antfly_av1_dav1d_ref_dec(&c.*.frame_hdr_ref);
    antfly_av1_dav1d_ref_dec(&c.*.mastering_display_ref);
    antfly_av1_dav1d_ref_dec(&c.*.content_light_ref);
    antfly_av1_dav1d_ref_dec(&c.*.itut_t35_ref);
    antfly_av1_dav1d_mem_pool_end(c.*.seq_hdr_pool);
    antfly_av1_dav1d_mem_pool_end(c.*.frame_hdr_pool);
    antfly_av1_dav1d_mem_pool_end(c.*.segmap_pool);
    antfly_av1_dav1d_mem_pool_end(c.*.refmvs_pool);
    antfly_av1_dav1d_mem_pool_end(c.*.cdf_pool);
    antfly_av1_dav1d_mem_pool_end(c.*.picture_pool);
    antfly_av1_dav1d_mem_pool_end(c.*.pic_ctx_pool);
    dav1d_freep_aligned(@ptrCast(@alignCast(c_out)));
}
pub export fn antfly_av1_dav1d_picture_unref(p: [*c]Dav1dPicture) void {
    _ = &p;
    antfly_av1_dav1d_picture_unref_internal(p);
}
pub export fn antfly_av1_dav1d_data_create(buf: [*c]Dav1dData, sz: usize) [*c]u8 {
    _ = &buf;
    _ = &sz;
    return antfly_av1_dav1d_data_create_internal(buf, sz);
}
pub export fn antfly_av1_dav1d_data_unref(buf: [*c]Dav1dData) void {
    _ = &buf;
    antfly_av1_dav1d_data_unref_internal(buf);
}
