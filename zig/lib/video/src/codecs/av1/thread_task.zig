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
pub const __builtin = common.__builtin;
pub extern fn antfly_codec_afz_trap() void;
pub const struct___va_list_1 = common.struct___va_list_1;
pub const __builtin_va_list = struct___va_list_1;
pub const atomic_int = common.atomic_int;
pub const atomic_uint = common.atomic_uint;
pub extern fn antfly_codec_afz_atomic_load(p: ?*const anyopaque) c_int;
pub extern fn antfly_codec_afz_atomic_store(p: ?*anyopaque, v: c_int) void;
pub extern fn antfly_codec_afz_atomic_add(p: ?*anyopaque, v: c_int) c_int;
pub extern fn antfly_codec_afz_atomic_exchange(p: ?*anyopaque, v: c_int) c_int;
pub extern fn antfly_codec_afz_atomic_or(p: ?*anyopaque, v: c_int) c_int;
pub extern fn antfly_codec_afz_atomic_compare(p: ?*anyopaque, expected: ?*anyopaque, desired: c_int) c_int;
pub const ptrdiff_t = common.ptrdiff_t;
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
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
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
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
const struct_unnamed_15 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
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
pub const enum_Filter2d = common.enum_Filter2d;
const struct_unnamed_39 = extern struct {
    pass: c_int = 0,
};
pub const afz_pthread_cond_t = common.afz_pthread_cond_t;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const struct_thread_data = common.struct_thread_data;
pub const DAV1D_TASK_TYPE_INIT = common.DAV1D_TASK_TYPE_INIT;
pub const DAV1D_TASK_TYPE_INIT_CDF = common.DAV1D_TASK_TYPE_INIT_CDF;
pub const DAV1D_TASK_TYPE_TILE_ENTROPY = common.DAV1D_TASK_TYPE_TILE_ENTROPY;
pub const DAV1D_TASK_TYPE_ENTROPY_PROGRESS = common.DAV1D_TASK_TYPE_ENTROPY_PROGRESS;
pub const DAV1D_TASK_TYPE_TILE_RECONSTRUCTION = common.DAV1D_TASK_TYPE_TILE_RECONSTRUCTION;
pub const DAV1D_TASK_TYPE_DEBLOCK_COLS = common.DAV1D_TASK_TYPE_DEBLOCK_COLS;
pub const DAV1D_TASK_TYPE_DEBLOCK_ROWS = common.DAV1D_TASK_TYPE_DEBLOCK_ROWS;
pub const DAV1D_TASK_TYPE_CDEF = common.DAV1D_TASK_TYPE_CDEF;
pub const DAV1D_TASK_TYPE_SUPER_RESOLUTION = common.DAV1D_TASK_TYPE_SUPER_RESOLUTION;
pub const DAV1D_TASK_TYPE_LOOP_RESTORATION = common.DAV1D_TASK_TYPE_LOOP_RESTORATION;
pub const DAV1D_TASK_TYPE_RECONSTRUCTION_PROGRESS = common.DAV1D_TASK_TYPE_RECONSTRUCTION_PROGRESS;
pub const DAV1D_TASK_TYPE_FG_PREP = common.DAV1D_TASK_TYPE_FG_PREP;
pub const DAV1D_TASK_TYPE_FG_APPLY = common.DAV1D_TASK_TYPE_FG_APPLY;
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
pub const enum_BlockSize = common.enum_BlockSize;
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
pub fn ctz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.ctz(mask);
}
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_realloc(__ptr: ?*anyopaque, __size: usize) ?*anyopaque;
pub extern fn antfly_codec_abort() noreturn;
pub extern fn antfly_codec_afz_mutex_lock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_unlock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_cond_signal([*c]afz_pthread_cond_t) c_int;
pub extern fn antfly_codec_afz_cond_wait([*c]afz_pthread_cond_t, [*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_av1_dav1d_cdf_thread_update(hdr: [*c]const Dav1dFrameHeader, dst: [*c]CdfContext, src: [*c]const CdfContext) void;
pub fn imax(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a > b) a else b;
}
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
pub extern fn antfly_av1_dav1d_decode_frame_init(f: [*c]Dav1dFrameContext) c_int;
pub extern fn antfly_av1_dav1d_decode_frame_init_cdf(f: [*c]Dav1dFrameContext) c_int;
pub extern fn antfly_av1_dav1d_decode_frame_exit(f: [*c]Dav1dFrameContext, retval: c_int) void;
pub extern fn antfly_av1_dav1d_decode_tile_sbrow(t: [*c]Dav1dTaskContext) c_int;
pub extern fn antfly_av1_dav1d_prep_grain_8bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture, scaling: ?*anyopaque, grain_lut: ?*anyopaque) void;
pub extern fn antfly_av1_dav1d_prep_grain_16bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture, scaling: ?*anyopaque, grain_lut: ?*anyopaque) void;
pub extern fn antfly_av1_dav1d_apply_grain_row_8bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture, scaling: ?*anyopaque, grain_lut: ?*anyopaque, row: c_int) void;
pub extern fn antfly_av1_dav1d_apply_grain_row_16bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture, scaling: ?*anyopaque, grain_lut: ?*anyopaque, row: c_int) void;
pub fn reset_task_cur(c: [*c]const Dav1dContext, ttd: [*c]struct_TaskThreadData_41, arg_frame_idx: c_uint) callconv(.c) c_int {
    _ = &c;
    _ = &ttd;
    var frame_idx = arg_frame_idx;
    _ = &frame_idx;
    var afz_i_4: c_uint = undefined;
    _ = &afz_i_4;
    var afz_cur_frame_idx_3: c_uint = undefined;
    _ = &afz_cur_frame_idx_3;
    var afz_min_frame_idx_2: c_uint = undefined;
    _ = &afz_min_frame_idx_2;
    var afz_reset_frame_idx_1: c_uint = undefined;
    _ = &afz_reset_frame_idx_1;
    var afz_first_0: c_uint = undefined;
    _ = &afz_first_0;
    var afz_pc: c_uint = 32;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return 1;
            },
            2 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_4 < c.*.n_fc) @as(c_int, 4) else @as(c_int, 1)));
                continue;
            },
            3 => {
                afz_i_4 +%= 1;
                afz_pc = 2;
                continue;
            },
            4 => {
                c.*.fc[(afz_first_0 +% afz_i_4) % c.*.n_fc].task_thread.task_cur_prev = null;
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_i_4 = ttd.*.cur;
                afz_pc = 2;
                continue;
            },
            6 => {
                afz_pc = @bitCast(@as(c_int, if (ttd.*.cur < c.*.n_fc) @as(c_int, 9) else @as(c_int, 5)));
                continue;
            },
            7 => {
                ttd.*.cur +%= 1;
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_pc = 5;
                continue;
            },
            9 => {
                afz_pc = @bitCast(@as(c_int, if (c.*.fc[(afz_first_0 +% ttd.*.cur) % c.*.n_fc].task_thread.task_head != null) @as(c_int, 8) else @as(c_int, 7)));
                continue;
            },
            10 => {
                ttd.*.cur = afz_min_frame_idx_2 -% afz_first_0;
                afz_pc = 6;
                continue;
            },
            11 => {
                return 0;
            },
            12 => {
                afz_pc = @bitCast(@as(c_int, if ((ttd.*.cur < c.*.n_fc) and (afz_cur_frame_idx_3 < afz_min_frame_idx_2)) @as(c_int, 11) else @as(c_int, 10)));
                continue;
            },
            13 => {
                afz_cur_frame_idx_3 = afz_first_0 +% ttd.*.cur;
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_min_frame_idx_2 = umin(afz_reset_frame_idx_1, frame_idx);
                afz_pc = 13;
                continue;
            },
            15 => {
                frame_idx +%= c.*.n_fc;
                afz_pc = 14;
                continue;
            },
            16 => {
                afz_pc = @bitCast(@as(c_int, if (frame_idx < afz_first_0) @as(c_int, 15) else @as(c_int, 14)));
                continue;
            },
            17 => {
                afz_pc = 5;
                continue;
            },
            18 => {
                ttd.*.cur = afz_reset_frame_idx_1 -% afz_first_0;
                afz_pc = 17;
                continue;
            },
            19 => {
                return 0;
            },
            20 => {
                afz_pc = @bitCast(@as(c_int, if (afz_reset_frame_idx_1 > (afz_first_0 +% ttd.*.cur)) @as(c_int, 19) else @as(c_int, 18)));
                continue;
            },
            21 => {
                afz_pc = @bitCast(@as(c_int, if (frame_idx == ((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1))) @as(c_int, 20) else @as(c_int, 16)));
                continue;
            },
            22 => {
                return 0;
            },
            23 => {
                afz_pc = @bitCast(@as(c_int, if (frame_idx == ((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1))) @as(c_int, 22) else @as(c_int, 16)));
                continue;
            },
            24 => {
                afz_pc = @bitCast(@as(c_int, if (afz_reset_frame_idx_1 != ((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1))) @as(c_int, 21) else @as(c_int, 23)));
                continue;
            },
            25 => {
                return 0;
            },
            26 => {
                afz_pc = @bitCast(@as(c_int, if (!(ttd.*.cur != 0) and (@as(?*anyopaque, @ptrCast(@alignCast(c.*.fc[afz_first_0].task_thread.task_cur_prev))) == @as(?*anyopaque, null))) @as(c_int, 25) else @as(c_int, 24)));
                continue;
            },
            27 => {
                afz_reset_frame_idx_1 = (@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1);
                afz_pc = 26;
                continue;
            },
            28 => {
                return 0;
            },
            29 => {
                afz_pc = @bitCast(@as(c_int, if (frame_idx == ((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1))) @as(c_int, 28) else @as(c_int, 27)));
                continue;
            },
            30 => {
                afz_pc = @bitCast(@as(c_int, if (afz_reset_frame_idx_1 < afz_first_0) @as(c_int, 29) else @as(c_int, 26)));
                continue;
            },
            31 => {
                afz_reset_frame_idx_1 = @bitCast(@as(c_int, antfly_codec_afz_atomic_exchange(@ptrCast(@alignCast(&ttd.*.reset_task_cur)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))))));
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_first_0 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ttd.*.first)))));
                afz_pc = 31;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub fn reset_task_cur_async(ttd: [*c]struct_TaskThreadData_41, arg_frame_idx: c_uint, arg_n_frames: c_uint) callconv(.c) void {
    _ = &ttd;
    var frame_idx = arg_frame_idx;
    _ = &frame_idx;
    var n_frames = arg_n_frames;
    _ = &n_frames;
    const first: c_uint = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ttd.*.first)))));
    _ = &first;
    if (frame_idx < first) {
        frame_idx +%= n_frames;
    }
    var last_idx: c_uint = frame_idx;
    _ = &last_idx;
    while (true) {
        frame_idx = last_idx;
        last_idx = @bitCast(@as(c_int, antfly_codec_afz_atomic_exchange(@ptrCast(@alignCast(&ttd.*.reset_task_cur)), @bitCast(@as(c_uint, @truncate(frame_idx))))));
        if (!(last_idx < frame_idx)) break;
    }
    if ((frame_idx == first) and (@as(c_uint, @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ttd.*.first)))))) != first)) {
        var expected: c_uint = frame_idx;
        _ = &expected;
        _ = antfly_codec_afz_atomic_compare(@ptrCast(@alignCast(&ttd.*.reset_task_cur)), @ptrCast(@alignCast(&expected)), @bitCast(@as(c_uint, @truncate((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)))));
    }
}
pub fn insert_tasks_between(f: [*c]Dav1dFrameContext, first: [*c]Dav1dTask, last: [*c]Dav1dTask, a: [*c]Dav1dTask, b: [*c]Dav1dTask, cond_signal: c_int) callconv(.c) void {
    _ = &f;
    _ = &first;
    _ = &last;
    _ = &a;
    _ = &b;
    _ = &cond_signal;
    const ttd: [*c]struct_TaskThreadData_41 = f.*.task_thread.ttd;
    _ = &ttd;
    if (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(f.*.c.*.flush))) != 0) return;
    while (true) {
        if (!(!(a != null) or (a.*.next == b))) {
            unreachable;
        }
        if (!false) break;
    }
    if (!(a != null)) {
        f.*.task_thread.task_head = first;
    } else {
        a.*.next = first;
    }
    if (!(b != null)) {
        f.*.task_thread.task_tail = last;
    }
    last.*.next = b;
    _ = reset_task_cur(f.*.c, ttd, first.*.frame_idx);
    if ((cond_signal != 0) and !(antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&ttd.*.cond_signaled)), 1) != 0)) {
        _ = antfly_codec_afz_cond_signal(&ttd.*.cond);
    }
}
pub fn insert_tasks(f: [*c]Dav1dFrameContext, first: [*c]Dav1dTask, last: [*c]Dav1dTask, cond_signal: c_int) callconv(.c) void {
    _ = &f;
    _ = &first;
    _ = &last;
    _ = &cond_signal;
    var t_ptr: [*c]Dav1dTask = undefined;
    _ = &t_ptr;
    var prev_t: [*c]Dav1dTask = null;
    _ = &prev_t;
    {
        t_ptr = f.*.task_thread.task_head;
        while (t_ptr != null) : ({
            prev_t = t_ptr;
            t_ptr = t_ptr.*.next;
        }) {
            if (t_ptr.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)) {
                if (first.*.type > @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)) continue;
                if (first.*.sby > t_ptr.*.sby) continue;
                if (first.*.sby < t_ptr.*.sby) {
                    insert_tasks_between(f, first, last, prev_t, t_ptr, cond_signal);
                    return;
                }
            } else {
                if (first.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)) {
                    insert_tasks_between(f, first, last, prev_t, t_ptr, cond_signal);
                    return;
                }
                if (first.*.sby > t_ptr.*.sby) continue;
                if (first.*.sby < t_ptr.*.sby) {
                    insert_tasks_between(f, first, last, prev_t, t_ptr, cond_signal);
                    return;
                }
                if (first.*.type > t_ptr.*.type) continue;
                if (first.*.type < t_ptr.*.type) {
                    insert_tasks_between(f, first, last, prev_t, t_ptr, cond_signal);
                    return;
                }
            }
            while (true) {
                if (!((first.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_RECONSTRUCTION)) or (first.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)))) {
                    unreachable;
                }
                if (!false) break;
            }
            while (true) {
                if (!(first.*.type == t_ptr.*.type)) {
                    unreachable;
                }
                if (!false) break;
            }
            while (true) {
                if (!(t_ptr.*.sby == first.*.sby)) {
                    unreachable;
                }
                if (!false) break;
            }
            const p: c_int = @as(c_int, @intFromBool(first.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)));
            _ = &p;
            const t_tile_idx: c_int = @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(first) -% @intFromPtr(f.*.task_thread.tile_tasks[@bitCast(@as(isize, @intCast(p)))]))), @sizeOf(Dav1dTask)));
            _ = &t_tile_idx;
            const p_tile_idx: c_int = @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(t_ptr) -% @intFromPtr(f.*.task_thread.tile_tasks[@bitCast(@as(isize, @intCast(p)))]))), @sizeOf(Dav1dTask)));
            _ = &p_tile_idx;
            while (true) {
                if (!(t_tile_idx != p_tile_idx)) {
                    unreachable;
                }
                if (!false) break;
            }
            if (t_tile_idx > p_tile_idx) continue;
            insert_tasks_between(f, first, last, prev_t, t_ptr, cond_signal);
            return;
        }
    }
    insert_tasks_between(f, first, last, prev_t, null, cond_signal);
}
pub fn insert_task(f: [*c]Dav1dFrameContext, t: [*c]Dav1dTask, cond_signal: c_int) callconv(.c) void {
    _ = &f;
    _ = &t;
    _ = &cond_signal;
    insert_tasks(f, t, t, cond_signal);
}
pub fn add_pending(f: [*c]Dav1dFrameContext, t: [*c]Dav1dTask) callconv(.c) void {
    _ = &f;
    _ = &t;
    _ = antfly_codec_afz_mutex_lock(&f.*.task_thread.pending_tasks.lock);
    t.*.next = null;
    if (!(f.*.task_thread.pending_tasks.head != null)) {
        f.*.task_thread.pending_tasks.head = t;
    } else {
        f.*.task_thread.pending_tasks.tail.*.next = t;
    }
    f.*.task_thread.pending_tasks.tail = t;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.pending_tasks.merge)), 1);
    _ = antfly_codec_afz_mutex_unlock(&f.*.task_thread.pending_tasks.lock);
}
pub fn merge_pending_frame(f: [*c]Dav1dFrameContext) callconv(.c) c_int {
    _ = &f;
    const merge: c_int = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&f.*.task_thread.pending_tasks.merge)));
    _ = &merge;
    if (merge != 0) {
        _ = antfly_codec_afz_mutex_lock(&f.*.task_thread.pending_tasks.lock);
        var t: [*c]Dav1dTask = f.*.task_thread.pending_tasks.head;
        _ = &t;
        f.*.task_thread.pending_tasks.head = null;
        f.*.task_thread.pending_tasks.tail = null;
        antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.pending_tasks.merge)), 0);
        _ = antfly_codec_afz_mutex_unlock(&f.*.task_thread.pending_tasks.lock);
        while (t != null) {
            const tmp: [*c]Dav1dTask = t.*.next;
            _ = &tmp;
            insert_task(f, t, 0);
            t = tmp;
        }
    }
    return merge;
}
pub fn merge_pending(c: [*c]const Dav1dContext) callconv(.c) c_int {
    _ = &c;
    var res: c_int = 0;
    _ = &res;
    {
        var i: c_uint = 0;
        _ = &i;
        while (i < c.*.n_fc) : (i +%= 1) {
            res |= merge_pending_frame(&c.*.fc[i]);
        }
    }
    return res;
}
pub fn create_filter_sbrow(f: [*c]Dav1dFrameContext, pass: c_int, arg_res_t: [*c][*c]Dav1dTask) callconv(.c) c_int {
    _ = &f;
    _ = &pass;
    var res_t = arg_res_t;
    _ = &res_t;
    const has_deblock: c_int = @as(c_int, @intFromBool((@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) or (@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0)));
    _ = &has_deblock;
    const has_cdef: c_int = f.*.seq_hdr.*.cdef;
    _ = &has_cdef;
    const has_resize: c_int = @as(c_int, @intFromBool(f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]));
    _ = &has_resize;
    const has_lr: c_int = f.*.lf.restore_planes;
    _ = &has_lr;
    var tasks: [*c]Dav1dTask = f.*.task_thread.tasks;
    _ = &tasks;
    const uses_2pass: c_int = @as(c_int, @intFromBool(f.*.c.*.n_fc > @as(c_uint, 1)));
    _ = &uses_2pass;
    var num_tasks: c_int = f.*.sbh * (@as(c_int, 1) + uses_2pass);
    _ = &num_tasks;
    if (num_tasks > f.*.task_thread.num_tasks) {
        const size: usize = @sizeOf(Dav1dTask) *% @as(c_ulong, @bitCast(@as(c_long, num_tasks)));
        _ = &size;
        tasks = @ptrCast(@alignCast(antfly_codec_afz_realloc(@ptrCast(@alignCast(f.*.task_thread.tasks)), size)));
        if (!(tasks != null)) return -@as(c_int, 1);
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(tasks)), 0, size);
        f.*.task_thread.tasks = tasks;
        f.*.task_thread.num_tasks = num_tasks;
    }
    tasks += @as(usize, @bitCast(@as(isize, @intCast(f.*.sbh * (pass & @as(c_int, 1))))));
    if ((pass & @as(c_int, 1)) != 0) {
        f.*.frame_thread.entropy_progress = 0;
    } else {
        const prog_sz: c_int = ((f.*.sbh + @as(c_int, 31)) & ~@as(c_int, 31)) >> @intCast(@as(c_int, 5));
        _ = &prog_sz;
        if (prog_sz > f.*.frame_thread.prog_sz) {
            var prog: [*c]atomic_uint = undefined;
            prog = @ptrCast(@alignCast(antfly_codec_afz_realloc(@ptrCast(@alignCast(f.*.frame_thread.frame_progress)), @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 2) * prog_sz))) *% @sizeOf(@TypeOf(prog.*)))));
            _ = &prog;
            if (!(prog != null)) return -@as(c_int, 1);
            f.*.frame_thread.frame_progress = prog;
            f.*.frame_thread.copy_lpf_progress = prog + @as(usize, @bitCast(@as(isize, @intCast(prog_sz))));
        }
        f.*.frame_thread.prog_sz = prog_sz;
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(f.*.frame_thread.frame_progress)), 0, @as(c_ulong, @bitCast(@as(c_long, prog_sz))) *% @sizeOf(atomic_uint));
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(f.*.frame_thread.copy_lpf_progress)), 0, @as(c_ulong, @bitCast(@as(c_long, prog_sz))) *% @sizeOf(atomic_uint));
        antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.frame_thread.deblock_progress)), 0);
    }
    f.*.frame_thread.next_tile_row[@bitCast(@as(isize, @intCast(pass & @as(c_int, 1))))] = 0;
    var t: [*c]Dav1dTask = &tasks[@as(c_int, 0)];
    _ = &t;
    t.*.sby = 0;
    t.*.recon_progress = 1;
    t.*.deblock_progress = 0;
    t.*.type = @bitCast(if (pass == @as(c_int, 1)) DAV1D_TASK_TYPE_ENTROPY_PROGRESS else if (has_deblock != 0) DAV1D_TASK_TYPE_DEBLOCK_COLS else if ((has_cdef != 0) or (has_lr != 0)) DAV1D_TASK_TYPE_DEBLOCK_ROWS else if (has_resize != 0) DAV1D_TASK_TYPE_SUPER_RESOLUTION else DAV1D_TASK_TYPE_RECONSTRUCTION_PROGRESS);
    t.*.frame_idx = @bitCast(@as(c_int, @as(c_int, @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(f) -% @intFromPtr(f.*.c.*.fc))), @sizeOf(Dav1dFrameContext))))));
    res_t.* = t;
    return 0;
}
pub export fn antfly_av1_dav1d_task_create_tile_sbrow(f: [*c]Dav1dFrameContext, pass: c_int, cond_signal: c_int) c_int {
    _ = &f;
    _ = &pass;
    _ = &cond_signal;
    var tasks: [*c]Dav1dTask = f.*.task_thread.tile_tasks[@as(c_int, 0)];
    _ = &tasks;
    const uses_2pass: c_int = @as(c_int, @intFromBool(f.*.c.*.n_fc > @as(c_uint, 1)));
    _ = &uses_2pass;
    const num_tasks: c_int = @as(c_int, f.*.frame_hdr.*.tiling.cols) * @as(c_int, f.*.frame_hdr.*.tiling.rows);
    _ = &num_tasks;
    if (pass < @as(c_int, 2)) {
        var alloc_num_tasks: c_int = num_tasks * (@as(c_int, 1) + uses_2pass);
        _ = &alloc_num_tasks;
        if (alloc_num_tasks > f.*.task_thread.num_tile_tasks) {
            const size: usize = @sizeOf(Dav1dTask) *% @as(c_ulong, @bitCast(@as(c_long, alloc_num_tasks)));
            _ = &size;
            tasks = @ptrCast(@alignCast(antfly_codec_afz_realloc(@ptrCast(@alignCast(f.*.task_thread.tile_tasks[@as(c_int, 0)])), size)));
            if (!(tasks != null)) return -@as(c_int, 1);
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(tasks)), 0, size);
            f.*.task_thread.tile_tasks[@as(c_int, 0)] = tasks;
            f.*.task_thread.num_tile_tasks = alloc_num_tasks;
        }
        f.*.task_thread.tile_tasks[@as(c_int, 1)] = tasks + @as(usize, @bitCast(@as(isize, @intCast(num_tasks))));
    }
    tasks += @as(usize, @bitCast(@as(isize, @intCast(num_tasks * (pass & @as(c_int, 1))))));
    var pf_t: [*c]Dav1dTask = undefined;
    _ = &pf_t;
    if (create_filter_sbrow(f, pass, &pf_t) != 0) return -@as(c_int, 1);
    var prev_t: [*c]Dav1dTask = null;
    _ = &prev_t;
    {
        var tile_idx: c_int = 0;
        _ = &tile_idx;
        while (tile_idx < num_tasks) : (tile_idx += 1) {
            const ts: [*c]Dav1dTileState = &f.*.ts[@bitCast(@as(isize, @intCast(tile_idx)))];
            _ = &ts;
            var t: [*c]Dav1dTask = &tasks[@bitCast(@as(isize, @intCast(tile_idx)))];
            _ = &t;
            t.*.sby = ts.*.tiling.row_start >> @intCast(f.*.sb_shift);
            if ((pf_t != null) and (t.*.sby != 0)) {
                prev_t.*.next = pf_t;
                prev_t = pf_t;
                pf_t = null;
            }
            t.*.recon_progress = 0;
            t.*.deblock_progress = 0;
            t.*.deps_skip = 0;
            t.*.type = @bitCast(if (pass != @as(c_int, 1)) DAV1D_TASK_TYPE_TILE_RECONSTRUCTION else DAV1D_TASK_TYPE_TILE_ENTROPY);
            t.*.frame_idx = @bitCast(@as(c_int, @as(c_int, @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(f) -% @intFromPtr(f.*.c.*.fc))), @sizeOf(Dav1dFrameContext))))));
            if (prev_t != null) {
                prev_t.*.next = t;
            }
            prev_t = t;
        }
    }
    if (pf_t != null) {
        prev_t.*.next = pf_t;
        prev_t = pf_t;
    }
    prev_t.*.next = null;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.done[@bitCast(@as(isize, @intCast(pass & @as(c_int, 1))))])), 0);
    _ = antfly_codec_afz_mutex_lock(&f.*.task_thread.pending_tasks.lock);
    while (true) {
        if (!((@as(?*anyopaque, @ptrCast(@alignCast(f.*.task_thread.pending_tasks.head))) == @as(?*anyopaque, null)) or (pass == @as(c_int, 2)))) {
            unreachable;
        }
        if (!false) break;
    }
    if (!(f.*.task_thread.pending_tasks.head != null)) {
        f.*.task_thread.pending_tasks.head = &tasks[@as(c_int, 0)];
    } else {
        f.*.task_thread.pending_tasks.tail.*.next = &tasks[@as(c_int, 0)];
    }
    f.*.task_thread.pending_tasks.tail = prev_t;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.pending_tasks.merge)), 1);
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.init_done)), 1);
    _ = antfly_codec_afz_mutex_unlock(&f.*.task_thread.pending_tasks.lock);
    return 0;
}
pub export fn antfly_av1_dav1d_task_frame_init(f: [*c]Dav1dFrameContext) void {
    _ = &f;
    const c: [*c]const Dav1dContext = f.*.c;
    _ = &c;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.init_done)), 0);
    const t: [*c]Dav1dTask = &f.*.task_thread.init_task;
    _ = &t;
    t.*.type = DAV1D_TASK_TYPE_INIT;
    t.*.frame_idx = @bitCast(@as(c_int, @as(c_int, @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(f) -% @intFromPtr(c.*.fc))), @sizeOf(Dav1dFrameContext))))));
    t.*.sby = 0;
    t.*.recon_progress = blk: {
        const tmp = @as(c_int, 0);
        t.*.deblock_progress = tmp;
        break :blk tmp;
    };
    insert_task(f, t, 1);
}
pub export fn antfly_av1_dav1d_task_delayed_fg(c: [*c]Dav1dContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture) void {
    _ = &c;
    _ = &out;
    _ = &in;
    const ttd: [*c]struct_TaskThreadData_41 = &c.*.task_thread;
    _ = &ttd;
    ttd.*.delayed_fg.in = in;
    ttd.*.delayed_fg.out = out;
    ttd.*.delayed_fg.type = DAV1D_TASK_TYPE_FG_PREP;
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])), 0);
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 1)])), 0);
    _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
    ttd.*.delayed_fg.exec = 1;
    ttd.*.delayed_fg.finished = 0;
    _ = antfly_codec_afz_cond_signal(&ttd.*.cond);
    while (true) {
        _ = antfly_codec_afz_cond_wait(&ttd.*.delayed_fg.cond, &ttd.*.lock);
        if (!!(ttd.*.delayed_fg.finished != 0)) break;
    }
    _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
}
pub fn ensure_progress(ttd: [*c]struct_TaskThreadData_41, f: [*c]Dav1dFrameContext, t: [*c]Dav1dTask, @"type": enum_TaskType, state: [*c]atomic_int, target: [*c]c_int) callconv(.c) c_int {
    _ = &ttd;
    _ = &f;
    _ = &t;
    _ = &@"type";
    _ = &state;
    _ = &target;
    var p1: c_int = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(state)));
    _ = &p1;
    if (p1 < t.*.sby) {
        t.*.type = @"type";
        t.*.recon_progress = blk: {
            const tmp = @as(c_int, 0);
            t.*.deblock_progress = tmp;
            break :blk tmp;
        };
        target.* = t.*.sby;
        add_pending(f, t);
        _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
        return 1;
    }
    return 0;
}
pub fn check_tile(t: [*c]Dav1dTask, f: [*c]Dav1dFrameContext, frame_mt: c_int) callconv(.c) c_int {
    _ = &t;
    _ = &f;
    _ = &frame_mt;
    const tp: c_int = @as(c_int, @intFromBool(t.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)));
    _ = &tp;
    const tile_idx: c_int = @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(t) -% @intFromPtr(f.*.task_thread.tile_tasks[@bitCast(@as(isize, @intCast(tp)))]))), @sizeOf(Dav1dTask)));
    _ = &tile_idx;
    const ts: [*c]Dav1dTileState = &f.*.ts[@bitCast(@as(isize, @intCast(tile_idx)))];
    _ = &ts;
    const p1: c_int = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ts.*.progress[@bitCast(@as(isize, @intCast(tp)))])));
    _ = &p1;
    if (p1 < t.*.sby) return 1;
    var @"error": c_int = @as(c_int, @intFromBool(p1 == (@as(c_int, 2147483647) - @as(c_int, 1))));
    _ = &@"error";
    @"error" |= antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&f.*.task_thread.@"error")), @"error");
    if ((!(@"error" != 0) and (frame_mt != 0)) and !(tp != 0)) {
        const p2: c_int = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ts.*.progress[@as(c_int, 1)])));
        _ = &p2;
        if (p2 <= t.*.sby) return 1;
        @"error" = @as(c_int, @intFromBool(p2 == (@as(c_int, 2147483647) - @as(c_int, 1))));
        @"error" |= antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&f.*.task_thread.@"error")), @"error");
    }
    if ((!(@"error" != 0) and (frame_mt != 0)) and !!((f.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0)) {
        var p: [*c]const Dav1dThreadPicture = &f.*.sr_cur;
        _ = &p;
        const ss_ver: c_int = @as(c_int, @intFromBool(p.*.p.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
        _ = &ss_ver;
        const p_b: c_uint = @bitCast(@as(c_int, (t.*.sby + @as(c_int, 1)) << @intCast(f.*.sb_shift + @as(c_int, 2))));
        _ = &p_b;
        const tile_sby: c_int = t.*.sby - (ts.*.tiling.row_start >> @intCast(f.*.sb_shift));
        _ = &tile_sby;
        const lowest_px: [*c][2]c_int = @ptrCast(@alignCast(&ts.*.lowest_pixel[@bitCast(@as(isize, @intCast(tile_sby)))]));
        _ = &lowest_px;
        {
            var n: c_int = t.*.deps_skip;
            _ = &n;
            while (n < @as(c_int, 7)) : ({
                n += 1;
                t.*.deps_skip += 1;
            }) {
                var lowest: c_uint = undefined;
                _ = &lowest;
                if (tp != 0) {
                    lowest = p_b;
                } else {
                    const y: c_int = if (lowest_px[@bitCast(@as(isize, @intCast(n)))][@as(c_int, 0)] == (-@as(c_int, 2147483647) - @as(c_int, 1))) -@as(c_int, 2147483647) - @as(c_int, 1) else lowest_px[@bitCast(@as(isize, @intCast(n)))][@as(c_int, 0)] + @as(c_int, 8);
                    _ = &y;
                    const uv: c_int = if (lowest_px[@bitCast(@as(isize, @intCast(n)))][@as(c_int, 1)] == (-@as(c_int, 2147483647) - @as(c_int, 1))) -@as(c_int, 2147483647) - @as(c_int, 1) else (lowest_px[@bitCast(@as(isize, @intCast(n)))][@as(c_int, 1)] * (@as(c_int, 1) << @intCast(ss_ver))) + @as(c_int, 8);
                    _ = &uv;
                    const max: c_int = imax(y, uv);
                    _ = &max;
                    if (max == (-@as(c_int, 2147483647) - @as(c_int, 1))) continue;
                    lowest = @bitCast(@as(c_int, iclip(max, 1, f.*.refp[@bitCast(@as(isize, @intCast(n)))].p.p.h)));
                }
                const p3: c_uint = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&f.*.refp[@bitCast(@as(isize, @intCast(n)))].progress[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(tp != 0))))))])))));
                _ = &p3;
                if (p3 < lowest) return 1;
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&f.*.task_thread.@"error")), @as(c_int, @intFromBool(p3 == (((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))));
            }
        }
    }
    return 0;
}
pub fn get_frame_progress(c: [*c]const Dav1dContext, f: [*c]const Dav1dFrameContext) callconv(.c) c_int {
    _ = &c;
    _ = &f;
    var frame_prog: c_uint = @bitCast(@as(c_int, if (c.*.n_fc > @as(c_uint, 1)) antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&f.*.sr_cur.progress[@as(c_int, 1)]))) else @as(c_int, 0)));
    _ = &frame_prog;
    if (frame_prog >= (((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1))) return f.*.sbh - @as(c_int, 1);
    var idx: c_int = @bitCast(@as(c_uint, @truncate(frame_prog >> @intCast(@as(c_uint, @bitCast(@as(c_int, f.*.sb_shift + @as(c_int, 7))))))));
    _ = &idx;
    var prog: c_int = undefined;
    _ = &prog;
    while (true) {
        var state: [*c]atomic_uint = &f.*.frame_thread.frame_progress[@bitCast(@as(isize, @intCast(idx)))];
        _ = &state;
        const val: c_uint = @bitCast(@as(c_int, ~antfly_codec_afz_atomic_load(@ptrCast(@alignCast(state)))));
        _ = &val;
        prog = if (val != 0) ctz(val) else @as(c_int, 32);
        if (prog != @as(c_int, 32)) break;
        prog = 0;
        if (!((blk: {
            const ref = &idx;
            ref.* += 1;
            break :blk ref.*;
        }) < f.*.frame_thread.prog_sz)) break;
    }
    return ((idx << @intCast(@as(c_int, 5))) | prog) - @as(c_int, 1);
}
pub fn abort_frame(f: [*c]Dav1dFrameContext, @"error": c_int) callconv(.c) void {
    _ = &f;
    _ = &@"error";
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.@"error")), if (@"error" == -@as(c_int, 22)) @as(c_int, 1) else -@as(c_int, 1));
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.task_counter)), 0);
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.done[@as(c_int, 0)])), 1);
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.task_thread.done[@as(c_int, 1)])), 1);
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.sr_cur.progress[@as(c_int, 0)])), @bitCast(@as(c_uint, @truncate(((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))));
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&f.*.sr_cur.progress[@as(c_int, 1)])), @bitCast(@as(c_uint, @truncate(((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))));
    antfly_av1_dav1d_decode_frame_exit(f, @"error");
    f.*.n_tile_data = 0;
    _ = antfly_codec_afz_cond_signal(&f.*.task_thread.cond);
}
pub fn delayed_fg_task(c: [*c]const Dav1dContext, ttd: [*c]struct_TaskThreadData_41) callconv(.c) void {
    _ = &c;
    _ = &ttd;
    const in: [*c]const Dav1dPicture = ttd.*.delayed_fg.in;
    _ = &in;
    const out: [*c]Dav1dPicture = ttd.*.delayed_fg.out;
    _ = &out;
    var off: c_int = undefined;
    _ = &off;
    if (out.*.p.bpc != @as(c_int, 8)) {
        off = (out.*.p.bpc >> @intCast(@as(c_int, 1))) - @as(c_int, 4);
    }
    blk: {
        switch (ttd.*.delayed_fg.type) {
            @as(enum_TaskType, DAV1D_TASK_TYPE_FG_PREP) => {
                ttd.*.delayed_fg.exec = 0;
                if (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ttd.*.cond_signaled))) != 0) {
                    _ = antfly_codec_afz_cond_signal(&ttd.*.cond);
                }
                _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
                blk_1: {
                    switch (out.*.p.bpc) {
                        @as(c_int, 8) => {
                            antfly_av1_dav1d_prep_grain_8bpc(&c.*.dsp[@as(c_int, 0)].fg, out, in, @ptrCast(@alignCast(@as([*c][256]u8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_0.scaling_8bpc))))), @ptrCast(@alignCast(@as([*c][74][82]i8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_0.grain_lut_8bpc))))));
                            break :blk_1;
                        },
                        @as(c_int, 10), @as(c_int, 12) => {
                            antfly_av1_dav1d_prep_grain_16bpc(&c.*.dsp[@bitCast(@as(isize, @intCast(off)))].fg, out, in, @ptrCast(@alignCast(@as([*c][4096]u8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_1.scaling_16bpc))))), @ptrCast(@alignCast(@as([*c][74][82]i16, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_1.grain_lut_16bpc))))));
                            break :blk_1;
                        },
                        else => {
                            antfly_codec_abort();
                        },
                    }
                }
                ttd.*.delayed_fg.type = DAV1D_TASK_TYPE_FG_APPLY;
                _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
                ttd.*.delayed_fg.exec = 1;
                {}
                var row: c_int = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])), 1);
                _ = &row;
                _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
                var progmax: c_int = @divTrunc((out.*.p.h + @as(c_int, 32)) - @as(c_int, 1), @as(c_int, 32));
                _ = &progmax;
                while (row < progmax) {
                    if ((row + @as(c_int, 1)) < progmax) {
                        _ = antfly_codec_afz_cond_signal(&ttd.*.cond);
                    } else {
                        _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
                        ttd.*.delayed_fg.exec = 0;
                        _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
                    }
                    blk_1: {
                        switch (out.*.p.bpc) {
                            @as(c_int, 8) => {
                                antfly_av1_dav1d_apply_grain_row_8bpc(&c.*.dsp[@as(c_int, 0)].fg, out, in, @ptrCast(@alignCast(@as([*c][256]u8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_0.scaling_8bpc))))), @ptrCast(@alignCast(@as([*c][74][82]i8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_0.grain_lut_8bpc))))), row);
                                break :blk_1;
                            },
                            @as(c_int, 10), @as(c_int, 12) => {
                                antfly_av1_dav1d_apply_grain_row_16bpc(&c.*.dsp[@bitCast(@as(isize, @intCast(off)))].fg, out, in, @ptrCast(@alignCast(@as([*c][4096]u8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_1.scaling_16bpc))))), @ptrCast(@alignCast(@as([*c][74][82]i16, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_1.grain_lut_16bpc))))), row);
                                break :blk_1;
                            },
                            else => {
                                antfly_codec_abort();
                            },
                        }
                    }
                    row = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])), 1);
                    _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 1)])), 1);
                }
                _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
                ttd.*.delayed_fg.exec = 0;
                var done: c_int = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 1)])), 1) + @as(c_int, 1);
                _ = &done;
                progmax = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])));
                if (done >= progmax) {
                    ttd.*.delayed_fg.finished = 1;
                    _ = antfly_codec_afz_cond_signal(&ttd.*.delayed_fg.cond);
                }
                break :blk;
            },
            @as(enum_TaskType, DAV1D_TASK_TYPE_FG_APPLY) => {
                {}
                var row: c_int = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])), 1);
                _ = &row;
                _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
                var progmax: c_int = @divTrunc((out.*.p.h + @as(c_int, 32)) - @as(c_int, 1), @as(c_int, 32));
                _ = &progmax;
                while (row < progmax) {
                    if ((row + @as(c_int, 1)) < progmax) {
                        _ = antfly_codec_afz_cond_signal(&ttd.*.cond);
                    } else {
                        _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
                        ttd.*.delayed_fg.exec = 0;
                        _ = antfly_codec_afz_mutex_unlock(&ttd.*.lock);
                    }
                    blk_1: {
                        switch (out.*.p.bpc) {
                            @as(c_int, 8) => {
                                antfly_av1_dav1d_apply_grain_row_8bpc(&c.*.dsp[@as(c_int, 0)].fg, out, in, @ptrCast(@alignCast(@as([*c][256]u8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_0.scaling_8bpc))))), @ptrCast(@alignCast(@as([*c][74][82]i8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_0.grain_lut_8bpc))))), row);
                                break :blk_1;
                            },
                            @as(c_int, 10), @as(c_int, 12) => {
                                antfly_av1_dav1d_apply_grain_row_16bpc(&c.*.dsp[@bitCast(@as(isize, @intCast(off)))].fg, out, in, @ptrCast(@alignCast(@as([*c][4096]u8, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_1.scaling_16bpc))))), @ptrCast(@alignCast(@as([*c][74][82]i16, @ptrCast(@alignCast(&ttd.*.delayed_fg.unnamed_0.unnamed_1.grain_lut_16bpc))))), row);
                                break :blk_1;
                            },
                            else => {
                                antfly_codec_abort();
                            },
                        }
                    }
                    row = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])), 1);
                    _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 1)])), 1);
                }
                _ = antfly_codec_afz_mutex_lock(&ttd.*.lock);
                ttd.*.delayed_fg.exec = 0;
                var done: c_int = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 1)])), 1) + @as(c_int, 1);
                _ = &done;
                progmax = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&ttd.*.delayed_fg.progress[@as(c_int, 0)])));
                if (done >= progmax) {
                    ttd.*.delayed_fg.finished = 1;
                    _ = antfly_codec_afz_cond_signal(&ttd.*.delayed_fg.cond);
                }
                break :blk;
            },
            else => {
                antfly_codec_abort();
            },
        }
    }
}
pub export fn antfly_av1_dav1d_worker_task(arg_data: ?*anyopaque) ?*anyopaque {
    var data = arg_data;
    _ = &data;
    var afz_num_tasks_44: c_int = undefined;
    _ = &afz_num_tasks_44;
    var afz_y_43: c_uint = undefined;
    _ = &afz_y_43;
    var afz_num_tasks_42: c_int = undefined;
    _ = &afz_num_tasks_42;
    var afz_y_41: c_uint = undefined;
    _ = &afz_y_41;
    var afz_sbsz_40: c_int = undefined;
    _ = &afz_sbsz_40;
    var afz_sbh_39: c_int = undefined;
    _ = &afz_sbh_39;
    var afz_uses_2pass_38: c_int = undefined;
    _ = &afz_uses_2pass_38;
    var afz_prog_37: c_int = undefined;
    _ = &afz_prog_37;
    var afz_progress_36: c_int = undefined;
    _ = &afz_progress_36;
    var afz_uses_2pass_35: c_int = undefined;
    _ = &afz_uses_2pass_35;
    var afz_ts_34: [*c]Dav1dTileState = undefined;
    _ = &afz_ts_34;
    var afz_tile_idx_33: c_int = undefined;
    _ = &afz_tile_idx_33;
    var afz_p_32: c_int = undefined;
    _ = &afz_p_32;
    var afz_res_31: c_int = undefined;
    _ = &afz_res_31;
    var afz_p_30: c_int = undefined;
    _ = &afz_p_30;
    var afz_res_29: c_int = undefined;
    _ = &afz_res_29;
    var afz_p1_28: c_int = undefined;
    _ = &afz_p1_28;
    var afz_res_27: c_int = undefined;
    _ = &afz_res_27;
    var afz_sby_26: c_int = undefined;
    _ = &afz_sby_26;
    var afz_error_25: c_int = undefined;
    _ = &afz_error_25;
    var afz_flush_24: c_int = undefined;
    _ = &afz_flush_24;
    var afz_start_20: c_int = undefined;
    _ = &afz_start_20;
    var afz_ntr_19: c_int = undefined;
    _ = &afz_ntr_19;
    var afz_next_t_18: [*c]Dav1dTask = undefined;
    _ = &afz_next_t_18;
    var afz_p2_17: c_int = undefined;
    _ = &afz_p2_17;
    var afz_ts_16: [*c]Dav1dTileState = undefined;
    _ = &afz_ts_16;
    var afz_tc_15: c_int = undefined;
    _ = &afz_tc_15;
    var afz_p1_14: c_int = undefined;
    _ = &afz_p1_14;
    var afz_prog_13: [*c]atomic_int = undefined;
    _ = &afz_prog_13;
    var afz_tile_row_base_12: c_int = undefined;
    _ = &afz_tile_row_base_12;
    var afz_error_11: c_int = undefined;
    _ = &afz_error_11;
    var afz_p_10: c_int = undefined;
    _ = &afz_p_10;
    var afz_p1_22: c_int = undefined;
    _ = &afz_p1_22;
    var afz_prog_21: [*c]atomic_uint = undefined;
    _ = &afz_prog_21;
    var afz_p1_23: c_int = undefined;
    _ = &afz_p1_23;
    var afz_first_9: c_uint = undefined;
    _ = &afz_first_9;
    var afz_p1_8: c_int = undefined;
    _ = &afz_p1_8;
    var afz_first_7: c_uint = undefined;
    _ = &afz_first_7;
    var afz_i_6: c_uint = undefined;
    _ = &afz_i_6;
    var afz_t_4: [*c]Dav1dTask = undefined;
    _ = &afz_t_4;
    var afz_prev_t_5: [*c]Dav1dTask = undefined;
    _ = &afz_prev_t_5;
    var afz_f_3: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_3;
    var afz_ttd_2: [*c]struct_TaskThreadData_41 = undefined;
    _ = &afz_ttd_2;
    var afz_c_1: [*c]Dav1dContext = undefined;
    _ = &afz_c_1;
    var afz_tc_0: [*c]Dav1dTaskContext = undefined;
    _ = &afz_tc_0;
    var afz_pc: c_uint = 300;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return null;
            },
            1 => {
                return null;
            },
            2 => {
                _ = antfly_codec_afz_mutex_unlock(&afz_ttd_2.*.lock);
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = 295;
                continue;
            },
            4 => {
                afz_pc = 3;
                continue;
            },
            5 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 4;
                continue;
            },
            6 => {
                _ = antfly_codec_afz_cond_signal(&afz_f_3.*.task_thread.cond);
                afz_pc = 5;
                continue;
            },
            7 => {
                afz_f_3.*.n_tile_data = 0;
                afz_pc = 6;
                continue;
            },
            8 => {
                antfly_av1_dav1d_decode_frame_exit(afz_f_3, if (afz_error_25 == @as(c_int, 1)) -@as(c_int, 22) else if (afz_error_25 != 0) -@as(c_int, 12) else @as(c_int, 0));
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 8;
                continue;
            },
            10 => {
                afz_pc = @bitCast(@as(c_int, if ((!(afz_num_tasks_44 != 0) and (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 0)]))) != 0)) and (!(afz_uses_2pass_38 != 0) or (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 1)]))) != 0))) @as(c_int, 9) else @as(c_int, 5)));
                continue;
            },
            11 => {
                afz_pc = 4;
                continue;
            },
            12 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 11;
                continue;
            },
            13 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_sby_26 + @as(c_int, 1)) < afz_sbh_39) and (afz_num_tasks_44 != 0)) @as(c_int, 12) else @as(c_int, 10)));
                continue;
            },
            14 => {
                afz_num_tasks_44 = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&afz_f_3.*.task_thread.task_counter)), -@as(c_int, 1)) - @as(c_int, 1);
                afz_pc = 13;
                continue;
            },
            15 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 14;
                continue;
            },
            16 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 0)])), 1);
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_sby_26 + @as(c_int, 1)) == afz_sbh_39) @as(c_int, 16) else @as(c_int, 15)));
                continue;
            },
            18 => {
                _ = antfly_codec_afz_mutex_unlock(&afz_f_3.*.task_thread.lock);
                afz_pc = 17;
                continue;
            },
            19 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.sr_cur.progress[@as(c_int, 1)])), @bitCast(@as(c_uint, @truncate(if (afz_error_25 != 0) ((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1) else afz_y_43))));
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_c_1.*.n_fc > @as(c_uint, 1)) and (afz_f_3.*.sr_cur.p.data[@as(c_int, 0)] != null)) @as(c_int, 19) else @as(c_int, 18)));
                continue;
            },
            21 => {
                afz_y_43 = if ((afz_sby_26 + @as(c_int, 1)) == afz_sbh_39) (@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1) else @as(c_uint, @bitCast(@as(c_int, afz_sby_26 + @as(c_int, 1)))) *% @as(c_uint, @bitCast(@as(c_int, afz_sbsz_40)));
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 21;
                continue;
            },
            23 => {
                afz_sby_26 = get_frame_progress(afz_c_1, afz_f_3);
                afz_pc = 22;
                continue;
            },
            24 => {
                _ = antfly_codec_afz_mutex_lock(&afz_f_3.*.task_thread.lock);
                afz_pc = 23;
                continue;
            },
            25 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.frame_thread.frame_progress[@bitCast(@as(isize, @intCast(afz_sby_26 >> @intCast(@as(c_int, 5)))))])), @bitCast(@as(c_uint, @truncate(@as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, afz_sby_26 & @as(c_int, 31)))))))));
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_pc = 4;
                continue;
            },
            27 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 26;
                continue;
            },
            28 => {
                _ = antfly_codec_afz_cond_signal(&afz_f_3.*.task_thread.cond);
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_f_3.*.n_tile_data = 0;
                afz_pc = 28;
                continue;
            },
            30 => {
                antfly_av1_dav1d_decode_frame_exit(afz_f_3, if (afz_error_25 == @as(c_int, 1)) -@as(c_int, 22) else if (afz_error_25 != 0) -@as(c_int, 12) else @as(c_int, 0));
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_pc = @bitCast(@as(c_int, if ((!(afz_num_tasks_42 != 0) and (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 0)]))) != 0)) and (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 1)]))) != 0)) @as(c_int, 31) else @as(c_int, 27)));
                continue;
            },
            33 => {
                afz_pc = 4;
                continue;
            },
            34 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 33;
                continue;
            },
            35 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_sby_26 + @as(c_int, 1)) < afz_sbh_39) and (afz_num_tasks_42 != 0)) @as(c_int, 34) else @as(c_int, 32)));
                continue;
            },
            36 => {
                afz_num_tasks_42 = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&afz_f_3.*.task_thread.task_counter)), -@as(c_int, 1)) - @as(c_int, 1);
                afz_pc = 35;
                continue;
            },
            37 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 36;
                continue;
            },
            38 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 1)])), 1);
                afz_pc = 37;
                continue;
            },
            39 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_sby_26 + @as(c_int, 1)) == afz_sbh_39) @as(c_int, 38) else @as(c_int, 37)));
                continue;
            },
            40 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.frame_thread.entropy_progress)), if (afz_error_25 != 0) @as(c_int, 2147483647) - @as(c_int, 1) else afz_sby_26 + @as(c_int, 1));
                afz_pc = 39;
                continue;
            },
            41 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.sr_cur.progress[@as(c_int, 0)])), @bitCast(@as(c_uint, @truncate(if (afz_error_25 != 0) ((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1) else afz_y_41))));
                afz_pc = 40;
                continue;
            },
            42 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_3.*.sr_cur.p.data[@as(c_int, 0)] != null) @as(c_int, 41) else @as(c_int, 40)));
                continue;
            },
            43 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 45) else @as(c_int, 42);
                continue;
            },
            44 => {
                unreachable;
            },
            45 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_1.*.n_fc > @as(c_uint, 1))) @as(c_int, 44) else @as(c_int, 43)));
                continue;
            },
            46 => {
                afz_y_41 = if ((afz_sby_26 + @as(c_int, 1)) == afz_sbh_39) (@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1) else @as(c_uint, @bitCast(@as(c_int, afz_sby_26 + @as(c_int, 1)))) *% @as(c_uint, @bitCast(@as(c_int, afz_sbsz_40)));
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 46;
                continue;
            },
            48 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_ENTROPY_PROGRESS)) @as(c_int, 47) else @as(c_int, 25)));
                continue;
            },
            49 => {
                afz_sbsz_40 = afz_f_3.*.sb_step * @as(c_int, 4);
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_sbh_39 = afz_f_3.*.sbh;
                afz_pc = 49;
                continue;
            },
            51 => {
                afz_uses_2pass_38 = @as(c_int, @intFromBool(afz_c_1.*.n_fc > @as(c_uint, 1)));
                afz_pc = 50;
                continue;
            },
            52 => {
                antfly_codec_abort();
                afz_pc = 51;
                continue;
            },
            53 => {
                afz_pc = 51;
                continue;
            },
            54 => {
                afz_f_3.*.bd_fn.filter_sbrow_lr.?(afz_f_3, afz_sby_26);
                afz_pc = 53;
                continue;
            },
            55 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error"))) != 0) and (afz_f_3.*.lf.restore_planes != 0)) @as(c_int, 54) else @as(c_int, 53)));
                continue;
            },
            56 => {
                afz_f_3.*.bd_fn.filter_sbrow_resize.?(afz_f_3, afz_sby_26);
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error"))) != 0)) @as(c_int, 56) else @as(c_int, 55)));
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_3.*.frame_hdr.*.width[@as(c_int, 0)] != afz_f_3.*.frame_hdr.*.width[@as(c_int, 1)]) @as(c_int, 57) else @as(c_int, 55)));
                continue;
            },
            59 => {
                _ = antfly_codec_afz_cond_signal(&afz_ttd_2.*.cond);
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_ttd_2.*.cond_signaled)), 1) != 0)) @as(c_int, 59) else @as(c_int, 58)));
                continue;
            },
            61 => {
                reset_task_cur_async(afz_ttd_2, afz_t_4.*.frame_idx, afz_c_1.*.n_fc);
                afz_pc = 60;
                continue;
            },
            62 => {
                afz_f_3.*.bd_fn.filter_sbrow_cdef.?(afz_tc_0, afz_sby_26);
                afz_pc = 61;
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error"))) != 0)) @as(c_int, 62) else @as(c_int, 61)));
                continue;
            },
            64 => {
                afz_pc = @bitCast(@as(c_int, if (afz_f_3.*.seq_hdr.*.cdef != 0) @as(c_int, 63) else @as(c_int, 58)));
                continue;
            },
            65 => {
                _ = antfly_codec_afz_cond_signal(&afz_ttd_2.*.cond);
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_ttd_2.*.cond_signaled)), 1) != 0)) @as(c_int, 65) else @as(c_int, 64)));
                continue;
            },
            67 => {
                reset_task_cur_async(afz_ttd_2, afz_t_4.*.frame_idx, afz_c_1.*.n_fc);
                afz_pc = 66;
                continue;
            },
            68 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.frame_thread.deblock_progress)), if (afz_error_25 != 0) @as(c_int, 2147483647) - @as(c_int, 1) else afz_sby_26 + @as(c_int, 1));
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_pc = 4;
                continue;
            },
            71 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 70;
                continue;
            },
            72 => {
                add_pending(afz_f_3, afz_t_4);
                afz_pc = 71;
                continue;
            },
            73 => {
                afz_t_4.*.recon_progress = blk: {
                    const tmp = @as(c_int, 0);
                    afz_t_4.*.deblock_progress = tmp;
                    break :blk tmp;
                };
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_t_4.*.type = DAV1D_TASK_TYPE_CDEF;
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_uint, @bitCast(@as(c_int, ~afz_prog_37))) & (@as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, (afz_sby_26 - @as(c_int, 1)) & @as(c_int, 31))))))) != 0) @as(c_int, 74) else @as(c_int, 64)));
                continue;
            },
            76 => {
                afz_prog_37 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.frame_thread.copy_lpf_progress[@bitCast(@as(isize, @intCast((afz_sby_26 - @as(c_int, 1)) >> @intCast(@as(c_int, 5)))))])));
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_pc = @bitCast(@as(c_int, if (afz_sby_26 != 0) @as(c_int, 76) else @as(c_int, 64)));
                continue;
            },
            78 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.frame_thread.copy_lpf_progress[@bitCast(@as(isize, @intCast(afz_sby_26 >> @intCast(@as(c_int, 5)))))])), @bitCast(@as(c_uint, @truncate(@as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, afz_sby_26 & @as(c_int, 31)))))))));
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_3.*.seq_hdr.*.cdef) != 0) or (afz_f_3.*.lf.restore_planes != 0)) @as(c_int, 78) else @as(c_int, 64)));
                continue;
            },
            80 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_3.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) or (@as(c_int, afz_f_3.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0)) @as(c_int, 69) else @as(c_int, 79)));
                continue;
            },
            81 => {
                afz_f_3.*.bd_fn.filter_sbrow_deblock_rows.?(afz_f_3, afz_sby_26);
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error"))) != 0)) @as(c_int, 81) else @as(c_int, 80)));
                continue;
            },
            83 => {
                afz_pc = 4;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if (ensure_progress(afz_ttd_2, afz_f_3, afz_t_4, DAV1D_TASK_TYPE_DEBLOCK_ROWS, &afz_f_3.*.frame_thread.deblock_progress, &afz_t_4.*.deblock_progress) != 0) @as(c_int, 83) else @as(c_int, 82)));
                continue;
            },
            85 => {
                afz_f_3.*.bd_fn.filter_sbrow_deblock_cols.?(afz_f_3, afz_sby_26);
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error"))) != 0)) @as(c_int, 85) else @as(c_int, 84)));
                continue;
            },
            87 => {
                afz_pc = 4;
                continue;
            },
            88 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 87;
                continue;
            },
            89 => {
                add_pending(afz_f_3, afz_t_4);
                afz_pc = 88;
                continue;
            },
            90 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_ts_34.*.progress[@bitCast(@as(isize, @intCast(afz_p_32)))])), afz_progress_36);
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_pc = 185;
                continue;
            },
            92 => {
                _ = antfly_codec_afz_cond_signal(&afz_ttd_2.*.cond);
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_ttd_2.*.cond_signaled)), 1) != 0)) @as(c_int, 92) else @as(c_int, 91)));
                continue;
            },
            94 => {
                reset_task_cur_async(afz_ttd_2, afz_t_4.*.frame_idx, afz_c_1.*.n_fc);
                afz_pc = 93;
                continue;
            },
            95 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_ts_34.*.progress[@bitCast(@as(isize, @intCast(afz_p_32)))])), afz_progress_36);
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_pc = @bitCast(@as(c_int, if (!(check_tile(afz_t_4, afz_f_3, afz_uses_2pass_35) != 0)) @as(c_int, 95) else @as(c_int, 90)));
                continue;
            },
            97 => {
                afz_t_4.*.deps_skip = 0;
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_t_4.*.sby += 1;
                afz_pc = 97;
                continue;
            },
            99 => {
                _ = antfly_codec_afz_cond_signal(&afz_ttd_2.*.cond);
                afz_pc = 87;
                continue;
            },
            100 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_ttd_2.*.cond_signaled)), 1) != 0)) @as(c_int, 99) else @as(c_int, 87)));
                continue;
            },
            101 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 103) else @as(c_int, 100);
                continue;
            },
            102 => {
                unreachable;
            },
            103 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.task_counter))) >= @as(c_int, 0))) @as(c_int, 102) else @as(c_int, 101)));
                continue;
            },
            104 => {
                _ = antfly_codec_afz_cond_signal(&afz_f_3.*.task_thread.cond);
                afz_pc = 103;
                continue;
            },
            105 => {
                afz_f_3.*.n_tile_data = 0;
                afz_pc = 104;
                continue;
            },
            106 => {
                antfly_av1_dav1d_decode_frame_exit(afz_f_3, if (afz_error_25 == @as(c_int, 1)) -@as(c_int, 22) else if (afz_error_25 != 0) -@as(c_int, 12) else @as(c_int, 0));
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 106;
                continue;
            },
            108 => {
                afz_pc = @bitCast(@as(c_int, if ((((antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&afz_f_3.*.task_thread.task_counter)), -@as(c_int, 1)) - @as(c_int, 1)) == @as(c_int, 0)) and (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 0)]))) != 0)) and (!(afz_uses_2pass_35 != 0) or (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 1)]))) != 0))) @as(c_int, 107) else @as(c_int, 103)));
                continue;
            },
            109 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(afz_f_3.*.out_cdf.progress)), if (afz_error_25 != 0) @as(c_int, 2147483647) - @as(c_int, 1) else @as(c_int, 1));
                afz_pc = 108;
                continue;
            },
            110 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_1.*.n_fc > @as(c_uint, 1)) @as(c_int, 109) else @as(c_int, 108)));
                continue;
            },
            111 => {
                antfly_av1_dav1d_cdf_thread_update(afz_f_3.*.frame_hdr, afz_f_3.*.out_cdf.data.cdf, &afz_f_3.*.ts[afz_f_3.*.frame_hdr.*.tiling.update].cdf);
                afz_pc = 110;
                continue;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_error_25 != 0)) @as(c_int, 111) else @as(c_int, 110)));
                continue;
            },
            113 => {
                afz_pc = @bitCast(@as(c_int, if ((((@as(c_int, afz_f_3.*.frame_hdr.*.refresh_context) != 0) and (afz_tc_0.*.frame_thread.pass <= @as(c_int, 1))) and (afz_f_3.*.task_thread.update_set != 0)) and (@as(c_int, afz_f_3.*.frame_hdr.*.tiling.update) == afz_tile_idx_33)) @as(c_int, 112) else @as(c_int, 108)));
                continue;
            },
            114 => {
                afz_error_25 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 113;
                continue;
            },
            115 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 114;
                continue;
            },
            116 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_ts_34.*.progress[@bitCast(@as(isize, @intCast(afz_p_32)))])), afz_progress_36);
                afz_pc = 115;
                continue;
            },
            117 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 116;
                continue;
            },
            118 => {
                afz_pc = @bitCast(@as(c_int, if (((afz_sby_26 + @as(c_int, 1)) << @intCast(afz_f_3.*.sb_shift)) < afz_ts_34.*.tiling.row_end) @as(c_int, 98) else @as(c_int, 117)));
                continue;
            },
            119 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), afz_error_25);
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_progress_36 = if (afz_error_25 != 0) @as(c_int, 2147483647) - @as(c_int, 1) else @as(c_int, 1) + afz_sby_26;
                afz_pc = 119;
                continue;
            },
            121 => {
                afz_error_25 = antfly_av1_dav1d_decode_tile_sbrow(afz_tc_0);
                afz_pc = 120;
                continue;
            },
            122 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_error_25 != 0)) @as(c_int, 121) else @as(c_int, 120)));
                continue;
            },
            123 => {
                afz_tc_0.*.frame_thread.pass = if (!(afz_uses_2pass_35 != 0)) @as(c_int, 0) else @as(c_int, 1) + @as(c_int, @intFromBool(afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_RECONSTRUCTION)));
                afz_pc = 122;
                continue;
            },
            124 => {
                afz_uses_2pass_35 = @as(c_int, @intFromBool(afz_c_1.*.n_fc > @as(c_uint, 1)));
                afz_pc = 123;
                continue;
            },
            125 => {
                afz_tc_0.*.by = afz_sby_26 << @intCast(afz_f_3.*.sb_shift);
                afz_pc = 124;
                continue;
            },
            126 => {
                afz_tc_0.*.ts = afz_ts_34;
                afz_pc = 125;
                continue;
            },
            127 => {
                afz_ts_34 = &afz_f_3.*.ts[@bitCast(@as(isize, @intCast(afz_tile_idx_33)))];
                afz_pc = 126;
                continue;
            },
            128 => {
                afz_tile_idx_33 = @truncate(@divExact(@as(c_long, @bitCast(@intFromPtr(afz_t_4) -% @intFromPtr(afz_f_3.*.task_thread.tile_tasks[@bitCast(@as(isize, @intCast(afz_p_32)))]))), @sizeOf(Dav1dTask)));
                afz_pc = 127;
                continue;
            },
            129 => {
                afz_p_32 = @as(c_int, @intFromBool(afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)));
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_pc = 4;
                continue;
            },
            131 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 130;
                continue;
            },
            132 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p_30 <= @as(c_int, 2)) @as(c_int, 148) else @as(c_int, 131)));
                continue;
            },
            133 => {
                afz_p_30 += 1;
                afz_pc = 132;
                continue;
            },
            134 => {
                _ = antfly_codec_afz_cond_signal(&afz_f_3.*.task_thread.cond);
                afz_pc = 133;
                continue;
            },
            135 => {
                afz_f_3.*.n_tile_data = 0;
                afz_pc = 134;
                continue;
            },
            136 => {
                antfly_av1_dav1d_decode_frame_exit(afz_f_3, -@as(c_int, 12));
                afz_pc = 135;
                continue;
            },
            137 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 139) else @as(c_int, 136);
                continue;
            },
            138 => {
                unreachable;
            },
            139 => {
                afz_pc = @bitCast(@as(c_int, if (!!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.task_counter))) != 0)) @as(c_int, 138) else @as(c_int, 137)));
                continue;
            },
            140 => {
                _ = antfly_codec_afz_mutex_unlock(&afz_ttd_2.*.lock);
                afz_pc = 133;
                continue;
            },
            141 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_p_30 == @as(c_int, 2)) and (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@as(c_int, 1)]))) != 0)) @as(c_int, 139) else @as(c_int, 140)));
                continue;
            },
            142 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.sr_cur.progress[@bitCast(@as(isize, @intCast(afz_p_30 - @as(c_int, 1))))])), @bitCast(@as(c_uint, @truncate(((@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) -% @as(c_uint, 1)))));
                afz_pc = 141;
                continue;
            },
            143 => {
                _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&afz_f_3.*.task_thread.task_counter)), -((@as(c_int, afz_f_3.*.frame_hdr.*.tiling.cols) * @as(c_int, afz_f_3.*.frame_hdr.*.tiling.rows)) + afz_f_3.*.sbh));
                afz_pc = 142;
                continue;
            },
            144 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), -@as(c_int, 1));
                afz_pc = 143;
                continue;
            },
            145 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@bitCast(@as(isize, @intCast(@as(c_int, 2) - afz_p_30)))])), 1);
                afz_pc = 144;
                continue;
            },
            146 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 145;
                continue;
            },
            147 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_31 != 0) @as(c_int, 146) else @as(c_int, 133)));
                continue;
            },
            148 => {
                afz_res_31 = antfly_av1_dav1d_task_create_tile_sbrow(afz_f_3, afz_p_30, 0);
                afz_pc = 147;
                continue;
            },
            149 => {
                afz_p_30 = 1;
                afz_pc = 132;
                continue;
            },
            150 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 152) else @as(c_int, 149);
                continue;
            },
            151 => {
                unreachable;
            },
            152 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_1.*.n_fc > @as(c_uint, 1))) @as(c_int, 151) else @as(c_int, 150)));
                continue;
            },
            153 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_f_3.*.task_thread.init_done)), 1);
                afz_pc = 130;
                continue;
            },
            154 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 153;
                continue;
            },
            155 => {
                abort_frame(afz_f_3, afz_res_29);
                afz_pc = 154;
                continue;
            },
            156 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_res_29 != 0)) @as(c_int, 152) else @as(c_int, 156)));
                continue;
            },
            158 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(afz_f_3.*.out_cdf.progress)), if (afz_res_29 < @as(c_int, 0)) @as(c_int, 2147483647) - @as(c_int, 1) else @as(c_int, 1));
                afz_pc = 157;
                continue;
            },
            159 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_3.*.frame_hdr.*.refresh_context) != 0) and !(afz_f_3.*.task_thread.update_set != 0)) @as(c_int, 158) else @as(c_int, 157)));
                continue;
            },
            160 => {
                afz_res_29 = antfly_av1_dav1d_decode_frame_init_cdf(afz_f_3);
                afz_pc = 159;
                continue;
            },
            161 => {
                afz_pc = @bitCast(@as(c_int, if (!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error"))) != 0)) @as(c_int, 160) else @as(c_int, 159)));
                continue;
            },
            162 => {
                afz_res_29 = -@as(c_int, 22);
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 165) else @as(c_int, 162);
                continue;
            },
            164 => {
                unreachable;
            },
            165 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_1.*.n_fc > @as(c_uint, 1))) @as(c_int, 164) else @as(c_int, 163)));
                continue;
            },
            166 => {
                afz_pc = 4;
                continue;
            },
            167 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, afz_t_4.*.frame_idx);
                afz_pc = 166;
                continue;
            },
            168 => {
                abort_frame(afz_f_3, if (afz_res_27 != 0) afz_res_27 else -@as(c_int, 22));
                afz_pc = 167;
                continue;
            },
            169 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 168;
                continue;
            },
            170 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 166;
                continue;
            },
            171 => {
                add_pending(afz_f_3, afz_t_4);
                afz_pc = 170;
                continue;
            },
            172 => {
                afz_pc = 185;
                continue;
            },
            173 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p1_28 != 0) @as(c_int, 172) else @as(c_int, 171)));
                continue;
            },
            174 => {
                afz_t_4.*.type = DAV1D_TASK_TYPE_INIT_CDF;
                afz_pc = 173;
                continue;
            },
            175 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_res_27 != 0) or (afz_p1_28 == (@as(c_int, 2147483647) - @as(c_int, 1)))) @as(c_int, 169) else @as(c_int, 174)));
                continue;
            },
            176 => {
                afz_p1_28 = if (afz_f_3.*.in_cdf.progress != null) antfly_codec_afz_atomic_load(@ptrCast(@alignCast(afz_f_3.*.in_cdf.progress))) else @as(c_int, 1);
                afz_pc = 175;
                continue;
            },
            177 => {
                afz_res_27 = antfly_av1_dav1d_decode_frame_init(afz_f_3);
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
                afz_pc = @bitCast(@as(c_int, if (!(afz_c_1.*.n_fc > @as(c_uint, 1))) @as(c_int, 179) else @as(c_int, 178)));
                continue;
            },
            181 => {
                blk: {
                    switch (afz_t_4.*.type) {
                        @as(enum_TaskType, DAV1D_TASK_TYPE_ENTROPY_PROGRESS) => {
                            afz_pc = 53;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_RECONSTRUCTION_PROGRESS) => {
                            afz_pc = 53;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_LOOP_RESTORATION) => {
                            afz_pc = 55;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_SUPER_RESOLUTION) => {
                            afz_pc = 58;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_CDEF) => {
                            afz_pc = 64;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_DEBLOCK_ROWS) => {
                            afz_pc = 82;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_DEBLOCK_COLS) => {
                            afz_pc = 86;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_RECONSTRUCTION) => {
                            afz_pc = 129;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY) => {
                            afz_pc = 129;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_INIT_CDF) => {
                            afz_pc = 165;
                            continue;
                        },
                        @as(enum_TaskType, DAV1D_TASK_TYPE_INIT) => {
                            afz_pc = 180;
                            continue;
                        },
                        else => {
                            afz_pc = 52;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            182 => {
                afz_sby_26 = afz_t_4.*.sby;
                afz_pc = 181;
                continue;
            },
            183 => {
                afz_tc_0.*.f = afz_f_3;
                afz_pc = 182;
                continue;
            },
            184 => {
                afz_error_25 = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), afz_flush_24) | afz_flush_24;
                afz_pc = 183;
                continue;
            },
            185 => {
                afz_flush_24 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(afz_c_1.*.flush)));
                afz_pc = 184;
                continue;
            },
            186 => {
                _ = antfly_codec_afz_mutex_unlock(&afz_ttd_2.*.lock);
                afz_pc = 185;
                continue;
            },
            187 => {
                _ = antfly_codec_afz_cond_signal(&afz_ttd_2.*.cond);
                afz_pc = 186;
                continue;
            },
            188 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_ttd_2.*.cond_signaled)), 1);
                afz_pc = 187;
                continue;
            },
            189 => {
                afz_t_4.*.next = null;
                afz_pc = 188;
                continue;
            },
            190 => {
                afz_ttd_2.*.cur +%= 1;
                afz_pc = 189;
                continue;
            },
            191 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_t_4.*.type > @as(enum_TaskType, DAV1D_TASK_TYPE_INIT_CDF)) and !(afz_f_3.*.task_thread.task_head != null)) @as(c_int, 190) else @as(c_int, 189)));
                continue;
            },
            192 => {
                afz_f_3.*.task_thread.task_tail = afz_prev_t_5;
                afz_pc = 191;
                continue;
            },
            193 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_t_4.*.next != null)) @as(c_int, 192) else @as(c_int, 191)));
                continue;
            },
            194 => {
                afz_prev_t_5.*.next = afz_t_4.*.next;
                afz_pc = 193;
                continue;
            },
            195 => {
                afz_f_3.*.task_thread.task_head = afz_t_4.*.next;
                afz_pc = 193;
                continue;
            },
            196 => {
                afz_pc = @bitCast(@as(c_int, if (afz_prev_t_5 != null) @as(c_int, 194) else @as(c_int, 195)));
                continue;
            },
            197 => {
                afz_pc = 4;
                continue;
            },
            198 => {
                _ = reset_task_cur(afz_c_1, afz_ttd_2, (@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1));
                afz_pc = 197;
                continue;
            },
            199 => {
                afz_tc_0.*.task_thread.flushed = 0;
                afz_pc = 198;
                continue;
            },
            200 => {
                _ = antfly_codec_afz_cond_wait(&afz_ttd_2.*.cond, &afz_ttd_2.*.lock);
                afz_pc = 199;
                continue;
            },
            201 => {
                antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&afz_ttd_2.*.cond_signaled)), 0);
                afz_pc = 200;
                continue;
            },
            202 => {
                _ = antfly_codec_afz_cond_signal(&afz_tc_0.*.task_thread.td.cond);
                afz_pc = 201;
                continue;
            },
            203 => {
                afz_tc_0.*.task_thread.flushed = 1;
                afz_pc = 202;
                continue;
            },
            204 => {
                afz_pc = 4;
                continue;
            },
            205 => {
                afz_pc = @bitCast(@as(c_int, if (merge_pending(afz_c_1) != 0) @as(c_int, 204) else @as(c_int, 203)));
                continue;
            },
            206 => {
                afz_pc = 4;
                continue;
            },
            207 => {
                afz_pc = @bitCast(@as(c_int, if (reset_task_cur(afz_c_1, afz_ttd_2, (@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1)) != 0) @as(c_int, 206) else @as(c_int, 205)));
                continue;
            },
            208 => {
                afz_pc = @bitCast(@as(c_int, if (afz_ttd_2.*.cur < afz_c_1.*.n_fc) @as(c_int, 267) else @as(c_int, 207)));
                continue;
            },
            209 => {
                afz_ttd_2.*.cur +%= 1;
                afz_pc = 208;
                continue;
            },
            210 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4 != null) @as(c_int, 262) else @as(c_int, 209)));
                continue;
            },
            211 => {
                afz_f_3.*.task_thread.task_cur_prev = afz_prev_t_5;
                afz_pc = 210;
                continue;
            },
            212 => {
                afz_t_4 = afz_t_4.*.next;
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_prev_t_5 = afz_t_4;
                afz_pc = 212;
                continue;
            },
            214 => {
                afz_pc = 213;
                continue;
            },
            215 => {
                afz_pc = 196;
                continue;
            },
            216 => {
                afz_pc = @bitCast(@as(c_int, if (!(check_tile(afz_t_4, afz_f_3, @as(c_int, @intFromBool(afz_c_1.*.n_fc > @as(c_uint, 1)))) != 0)) @as(c_int, 215) else @as(c_int, 213)));
                continue;
            },
            217 => {
                afz_pc = 196;
                continue;
            },
            218 => {
                insert_task(afz_f_3, afz_next_t_18, 0);
                afz_pc = 217;
                continue;
            },
            219 => {
                afz_next_t_18.*.recon_progress = afz_next_t_18.*.sby + @as(c_int, 1);
                afz_pc = 218;
                continue;
            },
            220 => {
                afz_f_3.*.frame_thread.next_tile_row[@bitCast(@as(isize, @intCast(afz_p_10)))] = afz_ntr_19;
                afz_pc = 219;
                continue;
            },
            221 => {
                afz_pc = @bitCast(@as(c_int, if (afz_next_t_18.*.sby == afz_start_20) @as(c_int, 220) else @as(c_int, 219)));
                continue;
            },
            222 => {
                afz_start_20 = afz_f_3.*.frame_hdr.*.tiling.row_start_sb[@bitCast(@as(isize, @intCast(afz_ntr_19)))];
                afz_pc = 221;
                continue;
            },
            223 => {
                afz_ntr_19 = afz_f_3.*.frame_thread.next_tile_row[@bitCast(@as(isize, @intCast(afz_p_10)))] + @as(c_int, 1);
                afz_pc = 222;
                continue;
            },
            224 => {
                afz_next_t_18.*.sby += 1;
                afz_pc = 223;
                continue;
            },
            225 => {
                afz_next_t_18.* = afz_t_4.*;
                afz_pc = 224;
                continue;
            },
            226 => {
                afz_next_t_18 = &afz_t_4[@as(c_int, 1)];
                afz_pc = 225;
                continue;
            },
            227 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_t_4.*.sby + @as(c_int, 1)) < afz_f_3.*.sbh) @as(c_int, 226) else @as(c_int, 217)));
                continue;
            },
            228 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tc_15 < @as(c_int, afz_f_3.*.frame_hdr.*.tiling.cols)) @as(c_int, 234) else @as(c_int, 227)));
                continue;
            },
            229 => {
                afz_tc_15 += 1;
                afz_pc = 228;
                continue;
            },
            230 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), @as(c_int, @intFromBool(afz_p2_17 == (@as(c_int, 2147483647) - @as(c_int, 1)))));
                afz_pc = 229;
                continue;
            },
            231 => {
                afz_pc = 213;
                continue;
            },
            232 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p2_17 < afz_t_4.*.recon_progress) @as(c_int, 231) else @as(c_int, 230)));
                continue;
            },
            233 => {
                afz_p2_17 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_ts_16.*.progress[@bitCast(@as(isize, @intCast(afz_p_10)))])));
                afz_pc = 232;
                continue;
            },
            234 => {
                afz_ts_16 = &afz_f_3.*.ts[@bitCast(@as(isize, @intCast(afz_tile_row_base_12 + afz_tc_15)))];
                afz_pc = 233;
                continue;
            },
            235 => {
                afz_tc_15 = 0;
                afz_pc = 228;
                continue;
            },
            236 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), @as(c_int, @intFromBool(afz_p1_14 == (@as(c_int, 2147483647) - @as(c_int, 1)))));
                afz_pc = 235;
                continue;
            },
            237 => {
                afz_pc = 213;
                continue;
            },
            238 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p1_14 < afz_t_4.*.sby) @as(c_int, 237) else @as(c_int, 236)));
                continue;
            },
            239 => {
                afz_p1_14 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(afz_prog_13)));
                afz_pc = 238;
                continue;
            },
            240 => {
                afz_prog_13 = &afz_f_3.*.frame_thread.entropy_progress;
                afz_pc = 239;
                continue;
            },
            241 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p_10 != 0) @as(c_int, 240) else @as(c_int, 235)));
                continue;
            },
            242 => {
                afz_tile_row_base_12 = @as(c_int, afz_f_3.*.frame_hdr.*.tiling.cols) * afz_f_3.*.frame_thread.next_tile_row[@bitCast(@as(isize, @intCast(afz_p_10)))];
                afz_pc = 241;
                continue;
            },
            243 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 245) else @as(c_int, 242);
                continue;
            },
            244 => {
                unreachable;
            },
            245 => {
                afz_pc = @bitCast(@as(c_int, if (!(!(antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.done[@bitCast(@as(isize, @intCast(afz_p_10)))]))) != 0) or (afz_error_11 != 0))) @as(c_int, 244) else @as(c_int, 243)));
                continue;
            },
            246 => {
                afz_error_11 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")));
                afz_pc = 245;
                continue;
            },
            247 => {
                afz_p_10 = @as(c_int, @intFromBool(afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_ENTROPY_PROGRESS)));
                afz_pc = 246;
                continue;
            },
            248 => {
                afz_pc = 196;
                continue;
            },
            249 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_uint, @bitCast(@as(c_int, afz_p1_22))) & (@as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, (afz_t_4.*.sby - @as(c_int, 1)) & @as(c_int, 31))))))) != 0) @as(c_int, 248) else @as(c_int, 213)));
                continue;
            },
            250 => {
                afz_p1_22 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_prog_21[@bitCast(@as(isize, @intCast((afz_t_4.*.sby - @as(c_int, 1)) >> @intCast(@as(c_int, 5)))))])));
                afz_pc = 249;
                continue;
            },
            251 => {
                afz_prog_21 = afz_f_3.*.frame_thread.copy_lpf_progress;
                afz_pc = 250;
                continue;
            },
            252 => {
                afz_pc = 196;
                continue;
            },
            253 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), @as(c_int, @intFromBool(afz_p1_23 == (@as(c_int, 2147483647) - @as(c_int, 1)))));
                afz_pc = 252;
                continue;
            },
            254 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p1_23 >= afz_t_4.*.deblock_progress) @as(c_int, 253) else @as(c_int, 213)));
                continue;
            },
            255 => {
                afz_p1_23 = antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.frame_thread.deblock_progress)));
                afz_pc = 254;
                continue;
            },
            256 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 258) else @as(c_int, 255);
                continue;
            },
            257 => {
                unreachable;
            },
            258 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_t_4.*.deblock_progress != 0)) @as(c_int, 257) else @as(c_int, 256)));
                continue;
            },
            259 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_CDEF)) @as(c_int, 251) else @as(c_int, 258)));
                continue;
            },
            260 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4.*.recon_progress != 0) @as(c_int, 247) else @as(c_int, 259)));
                continue;
            },
            261 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_ENTROPY)) or (afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_TILE_RECONSTRUCTION))) @as(c_int, 216) else @as(c_int, 260)));
                continue;
            },
            262 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_INIT_CDF)) @as(c_int, 214) else @as(c_int, 261)));
                continue;
            },
            263 => {
                afz_t_4 = if (afz_prev_t_5 != null) afz_prev_t_5.*.next else afz_f_3.*.task_thread.task_head;
                afz_pc = 210;
                continue;
            },
            264 => {
                afz_prev_t_5 = afz_f_3.*.task_thread.task_cur_prev;
                afz_pc = 263;
                continue;
            },
            265 => {
                _ = merge_pending_frame(afz_f_3);
                afz_pc = 264;
                continue;
            },
            266 => {
                afz_f_3 = &afz_c_1.*.fc[(afz_first_9 +% afz_ttd_2.*.cur) % afz_c_1.*.n_fc];
                afz_pc = 265;
                continue;
            },
            267 => {
                afz_first_9 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_ttd_2.*.first)))));
                afz_pc = 266;
                continue;
            },
            268 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_6 < afz_c_1.*.n_fc) @as(c_int, 283) else @as(c_int, 208)));
                continue;
            },
            269 => {
                afz_i_6 +%= 1;
                afz_pc = 268;
                continue;
            },
            270 => {
                afz_pc = 196;
                continue;
            },
            271 => {
                _ = antfly_codec_afz_atomic_or(@ptrCast(@alignCast(&afz_f_3.*.task_thread.@"error")), @as(c_int, @intFromBool(afz_p1_8 == (@as(c_int, 2147483647) - @as(c_int, 1)))));
                afz_pc = 270;
                continue;
            },
            272 => {
                afz_pc = @bitCast(@as(c_int, if (afz_p1_8 != 0) @as(c_int, 271) else @as(c_int, 269)));
                continue;
            },
            273 => {
                afz_p1_8 = if (afz_f_3.*.in_cdf.progress != null) antfly_codec_afz_atomic_load(@ptrCast(@alignCast(afz_f_3.*.in_cdf.progress))) else @as(c_int, 1);
                afz_pc = 272;
                continue;
            },
            274 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_INIT_CDF)) @as(c_int, 273) else @as(c_int, 269)));
                continue;
            },
            275 => {
                afz_pc = 196;
                continue;
            },
            276 => {
                afz_pc = @bitCast(@as(c_int, if (afz_t_4.*.type == @as(enum_TaskType, DAV1D_TASK_TYPE_INIT)) @as(c_int, 275) else @as(c_int, 274)));
                continue;
            },
            277 => {
                afz_pc = 269;
                continue;
            },
            278 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_t_4 != null)) @as(c_int, 277) else @as(c_int, 276)));
                continue;
            },
            279 => {
                afz_t_4 = afz_f_3.*.task_thread.task_head;
                afz_pc = 278;
                continue;
            },
            280 => {
                afz_pc = 269;
                continue;
            },
            281 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_f_3.*.task_thread.init_done))) != 0) @as(c_int, 280) else @as(c_int, 279)));
                continue;
            },
            282 => {
                afz_f_3 = &afz_c_1.*.fc[(afz_first_7 +% afz_i_6) % afz_c_1.*.n_fc];
                afz_pc = 281;
                continue;
            },
            283 => {
                afz_first_7 = @bitCast(@as(c_int, antfly_codec_afz_atomic_load(@ptrCast(@alignCast(&afz_ttd_2.*.first)))));
                afz_pc = 282;
                continue;
            },
            284 => {
                afz_i_6 = 0;
                afz_pc = 268;
                continue;
            },
            285 => {
                afz_pc = @bitCast(@as(c_int, if (afz_c_1.*.n_fc > @as(c_uint, 1)) @as(c_int, 284) else @as(c_int, 208)));
                continue;
            },
            286 => {
                afz_prev_t_5 = null;
                afz_pc = 285;
                continue;
            },
            287 => {
                afz_pc = 286;
                continue;
            },
            288 => {
                afz_pc = 4;
                continue;
            },
            289 => {
                delayed_fg_task(afz_c_1, afz_ttd_2);
                afz_pc = 288;
                continue;
            },
            290 => {
                afz_pc = @bitCast(@as(c_int, if (afz_ttd_2.*.delayed_fg.exec != 0) @as(c_int, 289) else @as(c_int, 287)));
                continue;
            },
            291 => {
                _ = merge_pending(afz_c_1);
                afz_pc = 290;
                continue;
            },
            292 => {
                afz_pc = 203;
                continue;
            },
            293 => {
                afz_pc = @bitCast(@as(c_int, if (antfly_codec_afz_atomic_load(@ptrCast(@alignCast(afz_c_1.*.flush))) != 0) @as(c_int, 292) else @as(c_int, 291)));
                continue;
            },
            294 => {
                afz_pc = 2;
                continue;
            },
            295 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tc_0.*.task_thread.die != 0) @as(c_int, 294) else @as(c_int, 293)));
                continue;
            },
            296 => {
                _ = antfly_codec_afz_mutex_lock(&afz_ttd_2.*.lock);
                afz_pc = 3;
                continue;
            },
            297 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 297) else @as(c_int, 296);
                continue;
            },
            298 => {
                afz_ttd_2 = afz_tc_0.*.task_thread.ttd;
                afz_pc = 297;
                continue;
            },
            299 => {
                afz_c_1 = @ptrCast(@alignCast(@constCast(afz_tc_0.*.c)));
                afz_pc = 298;
                continue;
            },
            300 => {
                afz_tc_0 = @ptrCast(@alignCast(data));
                afz_pc = 299;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
